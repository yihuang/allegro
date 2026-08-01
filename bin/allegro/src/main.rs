//! Allegro — Commonware simplex consensus node embedded in a reth node.
//!
//! The CLI is reth's own (`allegro node --chain … --datadir … --http.port …`),
//! extended with `--consensus.*` flags for the simplex layer, so tooling built
//! for reth-style nodes (tempo-py, tempo-e2e) can drive allegro unchanged.
//!
//! # Architecture
//!
//! Two OS threads, each running its own tokio runtime:
//!   1. **Main thread**: reth tokio runtime (via the reth CLI) launches a full
//!      `EthereumNode` and hands engine/payload handles to the consensus thread.
//!   2. **Consensus thread**: commonware tokio runtime runs the simplex engine
//!      with a `PayloadBuilder` backed by reth's engine API.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use allegro_consensus::{
    config::{ConsensusConfig, ForwardingPolicy},
    start_simplex_engine, ConsensusMetrics, EngineConfig, ValidatorEntry, ValidatorSet,
};
use alloy_primitives::B256;
use clap::{Args, Parser};
use commonware_cryptography::{ed25519::PrivateKey, Signer as _};
use commonware_p2p::authenticated::lookup;
use commonware_p2p::AddressableManager;
use commonware_runtime::{Clock, Metrics, Runner};
use commonware_utils::{ordered::Map, NZUsize};
use reth_chainspec::ChainSpec;
use reth_cli::chainspec::ChainSpecParser;
use reth_engine_primitives::ConsensusEngineHandle;
use reth_ethereum_cli::interface::Cli;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_payload_builder::PayloadBuilderHandle;
use tracing::{error, info, warn};

// ── Consensus CLI extension (`--consensus.*`) ───────────────

/// Simplex consensus options, namespaced like tempo's fork-specific flags.
#[derive(Debug, Clone, Args)]
#[command(next_help_heading = "Consensus")]
pub struct ConsensusArgs {
    /// This validator's index (0-based).
    #[arg(
        long = "consensus.node-index",
        default_value = "0",
        env = "ALLEGRO_NODE"
    )]
    pub node: u8,

    /// P2P listen address of the consensus layer.
    /// Defaults to 0.0.0.0:3000, or an ephemeral localhost port in dev mode.
    #[arg(long = "consensus.listen-address", env = "ALLEGRO_LISTEN")]
    pub listen: Option<SocketAddr>,

    /// Peer address for the consensus P2P network (repeatable).
    #[arg(long = "consensus.peer", env = "ALLEGRO_PEER")]
    pub peers: Vec<SocketAddr>,

    /// Leader timeout (ms).
    #[arg(
        long = "consensus.leader-timeout",
        default_value = "2000",
        env = "ALLEGRO_LEADER_TIMEOUT"
    )]
    pub leader_timeout_ms: u64,

    /// Certification timeout (ms).
    #[arg(
        long = "consensus.cert-timeout",
        default_value = "4000",
        env = "ALLEGRO_CERT_TIMEOUT"
    )]
    pub cert_timeout_ms: u64,

    /// Timeout retry interval (ms).
    #[arg(
        long = "consensus.timeout-retry",
        default_value = "1000",
        env = "ALLEGRO_TIMEOUT_RETRY"
    )]
    pub timeout_retry_ms: u64,

    /// Block fetch timeout (ms).
    #[arg(
        long = "consensus.fetch-timeout",
        default_value = "2000",
        env = "ALLEGRO_FETCH_TIMEOUT"
    )]
    pub fetch_timeout_ms: u64,

    /// Maximum consensus P2P message size (bytes).
    #[arg(
        long = "consensus.max-msg-size",
        default_value = "1048576",
        env = "ALLEGRO_MAX_MSG_SIZE"
    )]
    pub max_msg_size: u32,

    /// Actor mailbox size.
    #[arg(
        long = "consensus.mailbox-size",
        default_value = "1024",
        env = "ALLEGRO_MAILBOX_SIZE"
    )]
    pub mailbox_size: usize,

    /// Activity timeout (views).
    #[arg(
        long = "consensus.activity-timeout",
        default_value = "10",
        env = "ALLEGRO_ACTIVITY_TIMEOUT"
    )]
    pub activity_timeout: u64,

    /// Skip timeout (views).
    #[arg(
        long = "consensus.skip-timeout",
        default_value = "5",
        env = "ALLEGRO_SKIP_TIMEOUT"
    )]
    pub skip_timeout: u64,

    /// P2P synchrony bound (ms).
    #[arg(
        long = "consensus.synchrony",
        default_value = "2000",
        env = "ALLEGRO_SYNCHRONY"
    )]
    pub synchrony_ms: u64,

    /// Enable consensus metrics.
    #[arg(
        long = "consensus.metrics",
        default_value_t = false,
        env = "ALLEGRO_METRICS"
    )]
    pub metrics: bool,
}

impl ConsensusArgs {
    /// Fill in the listen address if not set explicitly: any free localhost
    /// port in dev mode (nothing dials a solo validator), 0.0.0.0:3000 else.
    fn resolve_listen(&mut self, dev: bool) {
        if self.listen.is_none() {
            let default = if dev { "127.0.0.1:0" } else { "0.0.0.0:3000" };
            self.listen = Some(default.parse().expect("valid default listen address"));
        }
    }

    /// The resolved listen address; [`Self::resolve_listen`] must run first.
    fn listen(&self) -> SocketAddr {
        self.listen.expect("listen address resolved at startup")
    }
}

// ── Chain spec parser (genesis + embedded validators) ───────

/// Source path of the `--chain` genesis file.
///
/// reth's CLI parses `--chain` into an `Arc<ChainSpec>` and drops the source
/// path, so the parser stashes it here; the launcher re-reads the file for the
/// embedded validators once tracing is up (validator loading logs each entry).
static GENESIS_PATH: Mutex<Option<PathBuf>> = Mutex::new(None);

/// Like reth's parser but accepts allegro genesis files with a top-level
/// `validators` array, and defaults to the dev chain.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct AllegroChainSpecParser;

impl ChainSpecParser for AllegroChainSpecParser {
    type ChainSpec = ChainSpec;

    const SUPPORTED_CHAINS: &'static [&'static str] = &["dev"];

    fn parse(s: &str) -> eyre::Result<Arc<ChainSpec>> {
        if s == "dev" {
            *GENESIS_PATH.lock().unwrap() = None;
            return Ok(allegro_node::chainspec::dev_chainspec());
        }
        let (spec, _validators) =
            allegro_node::chainspec::load_chain_with_validators(Path::new(s))?;
        *GENESIS_PATH.lock().unwrap() = Some(PathBuf::from(s));
        Ok(spec)
    }
}

// ── Entry point ────────────────────────────────────────────

fn main() -> eyre::Result<()> {
    let mut cli = Cli::<AllegroChainSpecParser, ConsensusArgs>::parse();

    // `--dev` (tempo/reth compatible) means a solo-validator devnet here:
    // consensus drives the engine API, so reth's dev auto-miner must stay off.
    // The `--chain` default already resolved to the dev spec via clap.
    let mut dev = false;
    cli.apply_node_command(|cmd| {
        dev = cmd.dev.dev;
        cmd.dev.dev = false;
    });

    cli.run(|builder, mut consensus| async move {
        consensus.resolve_listen(dev);
        let launched = allegro_node::launch::launch_with_builder(builder).await?;

        let genesis_validators = match GENESIS_PATH.lock().unwrap().take() {
            Some(path) => allegro_node::chainspec::load_chain_with_validators(&path)?.1,
            None => ValidatorSet::new(),
        };
        let validators = build_validator_set(&consensus, genesis_validators);

        spawn_consensus_thread(
            consensus,
            validators,
            RethWiring {
                engine_handle: launched.engine_handle.clone(),
                payload_handle: launched.payload_builder_handle.clone(),
                genesis_hash: launched.genesis_hash,
                genesis_timestamp: launched.genesis_timestamp,
                genesis_timestamp_millis: launched.genesis_timestamp_millis,
            },
        )?;

        launched.exit.await
    })
}

// ════════════════════════════════════════════════════════════
//  SHARED HELPERS
// ════════════════════════════════════════════════════════════

/// Use genesis validators if present, otherwise derive a devnet set from
/// `--consensus.node-index`/`--consensus.peer` (correct only when all peers
/// are provided).
fn build_validator_set(args: &ConsensusArgs, genesis_validators: ValidatorSet) -> ValidatorSet {
    if !genesis_validators.is_empty() {
        info!("using {} validators from genesis", genesis_validators.len());
        return genesis_validators;
    }

    let num_validators = 1 + args.peers.len() as u8;
    let my_index = args.node as usize;
    let entries: Vec<ValidatorEntry> = (0..num_validators)
        .map(|i| {
            let addr = if i as usize == my_index {
                args.listen()
            } else if (i as usize) < my_index {
                args.peers[i as usize]
            } else {
                args.peers[(i as usize) - 1]
            };
            ValidatorEntry {
                public_key: PrivateKey::from_seed(i as u64).public_key(),
                ingress: addr,
                egress: addr.ip(),
            }
        })
        .collect();
    let vs = ValidatorSet::from_entries(&entries);
    info!(
        "derived {} validators from --consensus.node-index/--consensus.peer (no genesis validators)",
        vs.len()
    );
    vs
}

fn build_consensus_config(args: &ConsensusArgs) -> ConsensusConfig {
    ConsensusConfig {
        mailbox_size: args.mailbox_size,
        leader_timeout: Duration::from_millis(args.leader_timeout_ms),
        certification_timeout: Duration::from_millis(args.cert_timeout_ms),
        timeout_retry: Duration::from_millis(args.timeout_retry_ms),
        fetch_timeout: Duration::from_millis(args.fetch_timeout_ms),
        fetch_concurrent: 4,
        activity_timeout: args.activity_timeout,
        skip_timeout: args.skip_timeout,
        forwarding_policy: ForwardingPolicy::SilentVoters,
        replay_buffer_size: 8 * 1024 * 1024,
        write_buffer_size: 1024 * 1024,
        page_cache_pages: 4096,
        page_cache_capacity: 8192,
        partition: format!("allegro_{}", args.node),
        strict_startup: false,
    }
}

/// Default P2P lookup config for a devnet node.
fn dev_lookup_config(args: &ConsensusArgs, crypto: PrivateKey) -> lookup::Config<PrivateKey> {
    lookup::Config {
        namespace: commonware_utils::union_unique(b"allegro_p2p", b"_P2P"),
        crypto,
        listen: args.listen(),
        max_message_size: args.max_msg_size,
        mailbox_size: args.mailbox_size,
        send_batch_size: NZUsize!(8),
        bypass_ip_check: false,
        allow_private_ips: true,
        allow_dns: false,
        tracked_peer_sets: NZUsize!(3),
        synchrony_bound: Duration::from_millis(args.synchrony_ms),
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
//  CONSENSUS RUNTIME
// ════════════════════════════════════════════════════════════

/// Handles wiring consensus to an embedded reth node.
struct RethWiring {
    engine_handle: ConsensusEngineHandle<EthEngineTypes>,
    payload_handle: PayloadBuilderHandle<EthEngineTypes>,
    genesis_hash: B256,
    genesis_timestamp: u64,
    genesis_timestamp_millis: u64,
}

/// Spawn [`run_consensus`] on its own OS thread, wired to reth.
fn spawn_consensus_thread(
    args: ConsensusArgs,
    validators: ValidatorSet,
    reth: RethWiring,
) -> eyre::Result<()> {
    std::thread::Builder::new()
        .name("allegro-consensus".into())
        .spawn(move || run_consensus(args, validators, reth))
        .map(|_| ())
        .map_err(|e| eyre::eyre!("failed to spawn consensus thread: {e}"))
}

/// Run the simplex engine on a commonware runtime (blocking): consensus p2p,
/// the engine actor, payload building over reth's engine API, and block
/// finalization.
fn run_consensus(args: ConsensusArgs, validators: ValidatorSet, reth: RethWiring) {
    let sk = PrivateKey::from_seed(args.node as u64);
    let pk = sk.public_key();
    info!(node = args.node, listen = %args.listen(), peers = ?args.peers, "starting allegro consensus");

    let consensus_config = build_consensus_config(&args);
    let rt_cfg = commonware_runtime::tokio::Config::default()
        .with_tcp_nodelay(Some(true))
        .with_worker_threads(2)
        .with_catch_panics(true);
    let runner = commonware_runtime::tokio::Runner::new(rt_cfg);

    runner.start(|context| async move {
        // ── Consensus P2P ──
        let (mut network, mut oracle) = lookup::Network::new(
            context.with_label("p2p"),
            dev_lookup_config(&args, sk.clone()),
        );
        let q =
            |n| commonware_runtime::Quota::per_second(std::num::NonZeroU32::new(n).expect("nz"));
        let votes = network.register(0, q(128), args.mailbox_size);
        let certs = network.register(1, q(128), args.mailbox_size);
        let resolver = network.register(2, q(128), args.mailbox_size);
        let blocks = network.register(3, q(128), args.mailbox_size);

        track_peers(&mut oracle, &pk, &validators).await;
        network.start();

        let metrics = args.metrics.then(ConsensusMetrics::new);

        // ── Reth wiring: engine-API payload builder + finalization ──
        let tracker = allegro_node::builder::ForkchoiceTracker::new(reth.genesis_hash);
        let payload_builder = Arc::new(allegro_node::builder::create_engine_payload_builder(
            reth.engine_handle.clone(),
            reth.payload_handle,
            tracker.clone(),
            metrics.clone(),
        ));
        let (finalized_tx, finalized_rx) = futures::channel::mpsc::channel(32);

        // ── Start simplex engine ──
        match start_simplex_engine(
            context.with_label("engine"),
            EngineConfig {
                signing_key: sk,
                validators,
                consensus_config,
                proposals: Arc::new(Mutex::new(Vec::new())),
                partition: format!("allegro_{}", args.node),
                payload_builder,
                metrics,
                genesis_hash: reth.genesis_hash,
                genesis_timestamp: reth.genesis_timestamp,
                genesis_timestamp_millis: reth.genesis_timestamp_millis,
                finalized_tx: Some(finalized_tx),
            },
            (votes, certs, resolver),
            blocks.0,
            blocks.1,
            oracle,
        ) {
            Ok(started) => {
                allegro_node::finalizer::spawn_finalizer(
                    context.with_label("finalizer"),
                    finalized_rx,
                    started.block_info,
                    reth.engine_handle,
                    tracker,
                );
                info!("allegro consensus is running");
                loop {
                    context.sleep(Duration::from_secs(3600)).await;
                }
            }
            Err(e) => error!(%e, "failed to start engine"),
        }
    });
}
