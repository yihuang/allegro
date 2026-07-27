//! Allegro — Commonware simplex consensus node with optional reth execution layer.
//!
//! Two execution modes:
//! - `stub`:    standalone consensus node using empty-block stub builder (for dev/testing)
//! - `reth`:    embedded reth node with real EVM, transaction pool, and JSON-RPC
//!
//! # Reth mode architecture
//!
//! Two OS threads, each running its own tokio runtime:
//!   1. **Main thread**: reth tokio runtime (via `reth_cli_runner::CliRunner`)
//!      launches a full `EthereumNode` and sends handles to the consensus thread.
//!   2. **Consensus thread**: commonware tokio runtime
//!      runs the simplex engine with a `PayloadBuilder` backed by reth's engine API.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use allegro_consensus::{
    config::{ConsensusConfig, ForwardingPolicy},
    start_simplex_engine, ConsensusMetrics, EngineConfig, ValidatorSet,
};
use clap::{Parser, ValueEnum};
use commonware_cryptography::{ed25519::PrivateKey, Signer as _};
use commonware_p2p::authenticated::lookup;
use commonware_p2p::AddressableManager;
use commonware_runtime::{Clock, Metrics, Runner};
use commonware_utils::{ordered::Map, NZUsize};
use reth_chainspec::ChainSpec;
use tracing::{error, info, warn};
use tracing_subscriber::filter::EnvFilter;

// ── CLI ─────────────────────────────────────────────────────

/// Execution mode for the consensus node.
#[derive(Debug, Clone, ValueEnum)]
pub enum ExecutionMode {
    /// Use the stub (empty-block) payload builder.
    Stub,
    /// Embed a real reth node with EVM execution and JSON-RPC.
    Reth,
}

/// Allegro consensus node.
#[derive(Debug, Clone, Parser)]
#[command(name = "allegro", version, about = "Commonware simplex consensus node")]
pub struct Cli {
    /// Execution mode.
    #[arg(long = "execution", default_value = "reth", env = "ALLEGRO_EXECUTION")]
    pub execution: ExecutionMode,
    /// This validator's index (0-based).
    #[arg(long = "node", default_value = "0", env = "ALLEGRO_NODE")]
    pub node: u8,
    /// P2P listen address (consensus layer).
    #[arg(
        long = "listen",
        short = 'l',
        default_value = "0.0.0.0:3000",
        env = "ALLEGRO_LISTEN"
    )]
    pub listen: SocketAddr,
    /// Peer addresses for the consensus P2P network.
    #[arg(short = 'p', long = "peer", env = "ALLEGRO_PEER")]
    pub peers: Vec<SocketAddr>,

    // ── Consensus timing ──
    #[arg(
        long = "leader-timeout",
        default_value = "2000",
        env = "ALLEGRO_LEADER_TIMEOUT"
    )]
    pub leader_timeout_ms: u64,
    #[arg(
        long = "cert-timeout",
        default_value = "4000",
        env = "ALLEGRO_CERT_TIMEOUT"
    )]
    pub cert_timeout_ms: u64,
    #[arg(
        long = "timeout-retry",
        default_value = "1000",
        env = "ALLEGRO_TIMEOUT_RETRY"
    )]
    pub timeout_retry_ms: u64,
    #[arg(
        long = "fetch-timeout",
        default_value = "2000",
        env = "ALLEGRO_FETCH_TIMEOUT"
    )]
    pub fetch_timeout_ms: u64,

    // ── P2P ──
    #[arg(
        long = "max-msg-size",
        default_value = "1048576",
        env = "ALLEGRO_MAX_MSG_SIZE"
    )]
    pub max_msg_size: u32,
    #[arg(
        long = "mailbox-size",
        default_value = "1024",
        env = "ALLEGRO_MAILBOX_SIZE"
    )]
    pub mailbox_size: usize,
    #[arg(
        long = "activity-timeout",
        default_value = "10",
        env = "ALLEGRO_ACTIVITY_TIMEOUT"
    )]
    pub activity_timeout: u64,
    #[arg(
        long = "skip-timeout",
        default_value = "5",
        env = "ALLEGRO_SKIP_TIMEOUT"
    )]
    pub skip_timeout: u64,
    #[arg(long = "synchrony", default_value = "2000", env = "ALLEGRO_SYNCHRONY")]
    pub synchrony_ms: u64,

    // ── Logging / metrics ──
    #[arg(long = "log-level", default_value = "info", env = "ALLEGRO_LOG_LEVEL")]
    pub log_level: String,
    #[arg(long = "metrics", default_value_t = false, env = "ALLEGRO_METRICS")]
    pub metrics: bool,

    // ── Reth execution node ──
    #[arg(long = "datadir", env = "ALLEGRO_DATADIR")]
    pub datadir: Option<PathBuf>,
    #[arg(long = "rpc-port", default_value = "8545", env = "ALLEGRO_RPC_PORT")]
    pub rpc_port: u16,
    #[arg(
        long = "authrpc-port",
        default_value = "8551",
        env = "ALLEGRO_AUTHRPC_PORT"
    )]
    pub authrpc_port: u16,
    #[arg(
        long = "reth-p2p-port",
        default_value = "30303",
        env = "ALLEGRO_RETH_P2P_PORT"
    )]
    pub reth_p2p_port: u16,
    #[arg(long = "genesis", env = "ALLEGRO_GENESIS")]
    pub genesis: Option<PathBuf>,
}

// ── Entry point ────────────────────────────────────────────

fn main() -> eyre::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli);

    match cli.execution {
        ExecutionMode::Stub => run_stub(cli),
        ExecutionMode::Reth => run_reth(cli),
    }
}

fn init_tracing(cli: &Cli) {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(format!("allegro={},commonware=warn", cli.log_level)))
        .unwrap_or_else(|_| EnvFilter::new("allegro=info,commonware=warn"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

// ════════════════════════════════════════════════════════════
//  SHARED HELPERS
// ════════════════════════════════════════════════════════════

/// Load genesis and embedded validators.
///
/// Returns `(ChainSpec, ValidatorSet)`.
/// If `--genesis` is not set, returns the dev chainspec with an empty validator set.
fn load_genesis(cli: &Cli) -> eyre::Result<(Arc<ChainSpec>, ValidatorSet)> {
    match &cli.genesis {
        Some(path) => allegro_node::chainspec::load_chain_with_validators(path),
        None => Ok((allegro_node::chainspec::dev_chainspec(), ValidatorSet::new())),
    }
}

fn build_consensus_config(cli: &Cli) -> ConsensusConfig {
    ConsensusConfig {
        mailbox_size: cli.mailbox_size,
        leader_timeout: Duration::from_millis(cli.leader_timeout_ms),
        certification_timeout: Duration::from_millis(cli.cert_timeout_ms),
        timeout_retry: Duration::from_millis(cli.timeout_retry_ms),
        fetch_timeout: Duration::from_millis(cli.fetch_timeout_ms),
        fetch_concurrent: 4,
        activity_timeout: cli.activity_timeout,
        skip_timeout: cli.skip_timeout,
        forwarding_policy: ForwardingPolicy::SilentVoters,
        replay_buffer_size: 8 * 1024 * 1024,
        write_buffer_size: 1024 * 1024,
        page_cache_pages: 4096,
        page_cache_capacity: 8192,
        partition: format!("allegro_{}", cli.node),
        strict_startup: false,
    }
}

/// Default P2P lookup config for a devnet node.
fn dev_lookup_config(cli: &Cli, crypto: PrivateKey) -> lookup::Config<PrivateKey> {
    lookup::Config {
        namespace: commonware_utils::union_unique(b"allegro_p2p", b"_P2P"),
        crypto,
        listen: cli.listen,
        max_message_size: cli.max_msg_size,
        mailbox_size: cli.mailbox_size,
        send_batch_size: NZUsize!(8),
        bypass_ip_check: false,
        allow_private_ips: true,
        allow_dns: false,
        tracked_peer_sets: NZUsize!(3),
        synchrony_bound: Duration::from_millis(cli.synchrony_ms),
        dial_frequency: Duration::from_millis(200),
        max_handshake_age: Duration::from_secs(300),
        handshake_timeout: Duration::from_secs(5),
        max_concurrent_handshakes: std::num::NonZeroU32::new(128).expect("nz"),
        block_duration: Duration::from_secs(60),
        ping_frequency: Duration::from_secs(10),
        peer_connection_cooldown: Duration::from_secs(5),
        allowed_handshake_rate_per_ip: commonware_runtime::Quota::per_second(
            std::num::NonZeroU32::new(10).expect("nz"),
        ),
        allowed_handshake_rate_per_subnet: commonware_runtime::Quota::per_second(
            std::num::NonZeroU32::new(10).expect("nz"),
        ),
    }
}

async fn track_peers(
    oracle: &mut lookup::Oracle<commonware_cryptography::ed25519::PublicKey>,
    my_pk: &commonware_cryptography::ed25519::PublicKey,
    validators: &ValidatorSet,
) {
    let pairs: Vec<(
        commonware_cryptography::ed25519::PublicKey,
        commonware_p2p::Address,
    )> = validators
        .keys()
        .into_iter()
        .filter_map(|k| {
            if k == *my_pk {
                None
            } else {
                validators.lookup(&k).map(|a| (k, a))
            }
        })
        .collect();
    if !pairs.is_empty() {
        match Map::try_from(pairs) {
            Ok(m) => oracle.track(0, m).await,
            Err(e) => warn!(%e, "skipping peer tracking: duplicate validator keys"),
        }
    }
}

// ════════════════════════════════════════════════════════════
//  STUB MODE (existing behaviour, unchanged)
// ════════════════════════════════════════════════════════════

fn run_stub(cli: Cli) -> eyre::Result<()> {
    let sk = PrivateKey::from_seed(cli.node as u64);
    let pk = sk.public_key();
    info!(node = cli.node, listen = %cli.listen, peers = ?cli.peers, "starting allegro node (stub)");

    let (_chain_spec, validators) = load_genesis(&cli)?;
    let consensus_config = build_consensus_config(&cli);

    let runner = commonware_runtime::tokio::Runner::new(commonware_runtime::tokio::Config::new());

    runner.start(|context| async move {
        let (mut network, mut oracle) = lookup::Network::new(
            context.with_label("p2p"),
            dev_lookup_config(&cli, sk.clone()),
        );
        let q = |n| {
            commonware_runtime::Quota::per_second(
                std::num::NonZeroU32::new(n).expect("nz"),
            )
        };
        let votes = network.register(0, q(128), cli.mailbox_size);
        let certs = network.register(1, q(128), cli.mailbox_size);
        let resolver = network.register(2, q(128), cli.mailbox_size);
        let blocks = network.register(3, q(128), cli.mailbox_size);

        track_peers(&mut oracle, &pk, &validators).await;
        network.start();

        let metrics = if cli.metrics {
            Some(ConsensusMetrics::new())
        } else {
            None
        };

        match start_simplex_engine(
            context.with_label("engine"),
            EngineConfig {
                signing_key: sk,
                validators,
                consensus_config,
                proposals: Arc::new(std::sync::Mutex::new(Vec::new())),
                partition: format!("allegro_{}", cli.node),
                payload_builder: None,
                metrics,
                genesis_hash: Default::default(),
                genesis_timestamp: 0,
                finalized_tx: None,
            },
            (votes, certs, resolver),
            blocks.0,
            blocks.1,
            oracle,
        ) {
            Ok(started) => {
                let _started = started;
                info!("allegro (stub) is running");
                loop {
                    context.sleep(Duration::from_secs(3600)).await;
                }
            }
            Err(e) => {
                error!(%e, "failed to start engine");
            }
        }
    });

    Ok(())
}

// ════════════════════════════════════════════════════════════
//  RETH MODE (embedded execution node)
// ════════════════════════════════════════════════════════════

fn run_reth(cli: Cli) -> eyre::Result<()> {
    let sk = PrivateKey::from_seed(cli.node as u64);
    let pk = sk.public_key();
    info!(node = cli.node, listen = %cli.listen, peers = ?cli.peers, "starting allegro node (reth)");

    // Load genesis + validators early (before spawning threads)
    let (chain_spec, validators) = load_genesis(&cli)?;
    let consensus_config = build_consensus_config(&cli);

    // ── Channel: reth thread → consensus thread ──
    // Only cloneable handles are sent (exit future stays on the main thread).
    use alloy_primitives::B256;
    use reth_engine_primitives::ConsensusEngineHandle;
    use reth_ethereum_engine_primitives::EthEngineTypes;
    use reth_payload_builder::PayloadBuilderHandle;
    type RethHandles = (
        ConsensusEngineHandle<EthEngineTypes>,
        PayloadBuilderHandle<EthEngineTypes>,
        B256,
        u64,
    );
    let (node_tx, node_rx) = std::sync::mpsc::channel::<RethHandles>();

    // ── Consensus thread (commonware tokio runtime) ──
    let c_sk = sk.clone();
    let c_pk = pk;
    let c_validators = validators.clone();
    let c_cli = cli.clone();
    let c_consensus_config = consensus_config.clone();

    let consensus_thread = std::thread::Builder::new()
        .name("allegro-consensus".into())
        .spawn(move || -> eyre::Result<()> {
            let (_engine_h, _payload_h, g_hash, g_ts) = node_rx
                .recv()
                .map_err(|_| eyre::eyre!("reth node channel closed before sending"))?;

            let rt_cfg = commonware_runtime::tokio::Config::default()
                .with_tcp_nodelay(Some(true))
                .with_worker_threads(2)
                .with_catch_panics(true);

            let runner = commonware_runtime::tokio::Runner::new(rt_cfg);
            runner.start(|context| async move {
                // Setup P2P
                let (mut network, mut oracle) = lookup::Network::new(
                    context.with_label("p2p"),
                    dev_lookup_config(&c_cli, c_sk.clone()),
                );
                let q = |n| {
                    commonware_runtime::Quota::per_second(
                        std::num::NonZeroU32::new(n).expect("nz"),
                    )
                };
                let votes = network.register(0, q(128), c_cli.mailbox_size);
                let certs = network.register(1, q(128), c_cli.mailbox_size);
                let resolver = network.register(2, q(128), c_cli.mailbox_size);
                let blocks = network.register(3, q(128), c_cli.mailbox_size);

                track_peers(&mut oracle, &c_pk, &c_validators).await;
                network.start();

                let metrics = if c_cli.metrics {
                    Some(ConsensusMetrics::new())
                } else {
                    None
                };

                // ── Payload builder (real reth engine API) ──
                let tracker = allegro_node::builder::ForkchoiceTracker::new(g_hash);
                let engine_builder = allegro_node::builder::create_engine_payload_builder(
                    _engine_h.clone(),
                    _payload_h.clone(),
                    tracker.clone(),
                );

                // ── Finalization channel ──
                let (finalized_tx, finalized_rx) = futures::channel::mpsc::channel(32);

                // ── Start simplex engine ──
                match start_simplex_engine(
                    context.with_label("engine"),
                    EngineConfig {
                        signing_key: c_sk,
                        validators: c_validators,
                        consensus_config: c_consensus_config,
                        proposals: Arc::new(std::sync::Mutex::new(Vec::new())),
                        partition: format!("allegro_{}", c_cli.node),
                        payload_builder: Some(Arc::new(engine_builder)),
                        metrics,
                        genesis_hash: g_hash,
                        genesis_timestamp: g_ts,
                        finalized_tx: Some(finalized_tx),
                    },
                    (votes, certs, resolver),
                    blocks.0,
                    blocks.1,
                    oracle,
                ) {
                    Ok(started) => {
                        // ── Finalizer task ──
                        allegro_node::finalizer::spawn_finalizer(
                            context.with_label("finalizer"),
                            finalized_rx,
                            started.block_info,
                            _engine_h.clone(),
                            tracker,
                        );

                        info!("allegro (reth) is running");
                        loop {
                            context.sleep(Duration::from_secs(3600)).await;
                        }
                    }
                    Err(e) => {
                        error!(%e, "failed to start engine");
                    }
                }
            });

            Ok(())
        })
        .map_err(|e| eyre::eyre!("failed to spawn consensus thread: {e}"))?;

    // ── Main thread (reth tokio runtime) ──
    let runner = reth_cli_runner::CliRunner::try_default_runtime()
        .map_err(|e| eyre::eyre!("failed to build reth runtime: {e}"))?;
    // Clone the runtime handle before passing it to the block_on closure,
    // since `runner` is borrowed by `block_on`.
    let task_executor = runner.runtime();

    runner.block_on(async move {
        let datadir = cli
            .datadir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join(format!("allegro-reth-{}", cli.node)));

        let cfg = allegro_node::launch::RethNodeConfig {
            datadir,
            http_port: cli.rpc_port,
            authrpc_port: cli.authrpc_port,
            p2p_port: cli.reth_p2p_port,
            chain: chain_spec,
        };

        let launched = allegro_node::launch::launch(cfg, task_executor)
            .await
            .map_err(|e| eyre::eyre!("failed to launch reth node: {e}"))?;

        node_tx
            .send((
                launched.engine_handle,
                launched.payload_builder_handle,
                launched.genesis_hash,
                launched.genesis_timestamp,
            ))
            .map_err(|_| eyre::eyre!("consensus thread exited before receiving handles"))?;

        // Wait for node exit or ctrl-c
        tokio::select! {
            _ = &mut std::pin::pin!(tokio::signal::ctrl_c()) => {
                info!("received shutdown signal");
            }
            _ = launched.exit => {
                info!("reth node exited");
            }
        }

        Ok::<(), eyre::ErrReport>(())
    })?;

    // Join consensus thread
    match consensus_thread.join() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(panic) => std::panic::resume_unwind(panic),
    }

    Ok(())
}
