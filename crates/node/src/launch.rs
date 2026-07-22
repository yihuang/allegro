//! Reth node programmatic launch for Allegro.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use alloy_primitives::B256;
use eyre::WrapErr;
use reth_chainspec::ChainSpec;
use reth_db::init_db;
use reth_engine_primitives::ConsensusEngineHandle;
use reth_ethereum::node::{
    EthereumNode,
    builder::{NodeBuilder, NodeHandle},
    core::{
        args::RpcServerArgs,
        node_config::NodeConfig,
        args::DatadirArgs,
        args::NetworkArgs,
        dirs::{MaybePlatformPath, DataDirPath},
    },
};
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_payload_builder::PayloadBuilderHandle;
use reth_tasks::TaskExecutor;

/// Type aliases for the launched full node (standard Ethereum components).
pub type AllegroFullNodeTypes = reth_node_builder::RethFullAdapter<reth_db::DatabaseEnv, EthereumNode>;
pub type AllegroNodeAdapter = reth_node_builder::NodeAdapter<AllegroFullNodeTypes>;
pub type AllegroFullNode = reth_node_builder::node::FullNode<
    AllegroNodeAdapter,
    reth_node_ethereum::EthereumAddOns<
        AllegroNodeAdapter,
        reth_node_ethereum::EthereumEthApiBuilder,
        reth_node_ethereum::EthereumEngineValidatorBuilder,
    >,
>;

/// Configuration for launching a reth execution node.
#[derive(Debug, Clone)]
pub struct RethNodeConfig {
    /// Data directory for reth (mdbx database).
    pub datadir: PathBuf,
    /// HTTP JSON-RPC port (default 8545).
    pub http_port: u16,
    /// Auth RPC port for engine API (default 8551).
    pub authrpc_port: u16,
    /// P2P networking port (default 30303).
    pub p2p_port: u16,
    /// Chain specification.
    pub chain: Arc<ChainSpec>,
}

impl Default for RethNodeConfig {
    fn default() -> Self {
        Self {
            datadir: std::env::temp_dir().join("allegro-reth"),
            http_port: 8545,
            authrpc_port: 8551,
            p2p_port: 30303,
            chain: crate::chainspec::dev_chainspec(),
        }
    }
}

/// Handle returned after launching a reth execution node.
///
/// Contains the engine API handles, genesis block info, and an exit future.
pub struct LaunchedRethNode {
    /// Consensus engine handle for FCU and new_payload calls.
    pub engine_handle: ConsensusEngineHandle<EthEngineTypes>,
    /// Payload builder handle for resolving built payloads.
    pub payload_builder_handle: PayloadBuilderHandle<EthEngineTypes>,
    /// Genesis block hash (from chainspec).
    pub genesis_hash: B256,
    /// Genesis block timestamp.
    pub genesis_timestamp: u64,
    /// The full node.
    ///
    /// **Must be kept alive**: the JSON-RPC servers (HTTP/auth) are bound to
    /// their server handles stored inside the node — dropping the node shuts
    /// them down (engine/p2p tasks survive, which made this subtle).
    pub node: AllegroFullNode,
    /// Future that resolves when the reth node exits.
    pub exit: Pin<Box<dyn Future<Output = eyre::Result<()>> + Send>>,
}

impl LaunchedRethNode {
    /// Get the consensus engine handle.
    pub fn engine_handle(&self) -> ConsensusEngineHandle<EthEngineTypes> {
        self.engine_handle.clone()
    }

    /// Get the payload builder handle.
    pub fn payload_builder_handle(&self) -> PayloadBuilderHandle<EthEngineTypes> {
        self.payload_builder_handle.clone()
    }
}

/// Launch a reth execution node with the given configuration.
///
/// The node is configured with:
/// - No dev-mode auto-mining (blocks only via engine API FCU)
/// - Standard Ethereum components (EthereumNode default)
/// - HTTP JSON-RPC on the configured port
/// - Engine API (auth RPC) on the configured port
/// - P2P networking on the configured port
pub async fn launch(
    cfg: RethNodeConfig,
    task_executor: TaskExecutor,
) -> eyre::Result<LaunchedRethNode> {
    let db = init_db(
        cfg.datadir.join("db"),
        reth_db::mdbx::DatabaseArguments::default(),
    )
    .wrap_err("failed to open database")?;

    let node_config = NodeConfig::new(cfg.chain.clone())
        .with_datadir_args(DatadirArgs {
            datadir: MaybePlatformPath::<DataDirPath>::from(cfg.datadir),
            ..Default::default()
        })
        .with_rpc(RpcServerArgs {
            http: true,
            http_port: cfg.http_port,
            auth_port: cfg.authrpc_port,
            // IPC uses a global default path (/tmp/reth.ipc) — multiple nodes
            // on one machine would collide; allegro doesn't need it.
            ipcdisable: true,
            ..Default::default()
        })
        .with_network(NetworkArgs {
            port: cfg.p2p_port,
            discovery: reth_ethereum::node::core::args::DiscoveryArgs {
                // Devnet mode: allegro's embedded reth nodes do not discover
                // each other (blocks flow over the consensus p2p network).
                // Disabling discovery avoids discv4/discv5 UDP port collisions
                // between co-located nodes.
                disable_discovery: true,
                ..Default::default()
            },
            ..Default::default()
        });

    let NodeHandle { node, node_exit_future } = NodeBuilder::new(node_config)
        .with_database(db)
        .with_launch_context(task_executor)
        .node(EthereumNode::default())
        .launch()
        .await
        .wrap_err("failed to launch reth node")?;

    // Extract handles — cheap channel-sender clones; the `node` itself is
    // moved into `LaunchedRethNode` to keep the RPC servers alive.
    let engine_handle = node.add_ons_handle.beacon_engine_handle.clone();
    let payload_builder_handle = node.payload_builder_handle.clone();

    let chain_spec = node.chain_spec();
    let genesis_hash = chain_spec.genesis_hash();
    let genesis_timestamp = chain_spec.genesis_timestamp();

    Ok(LaunchedRethNode {
        engine_handle,
        payload_builder_handle,
        genesis_hash,
        genesis_timestamp,
        node,
        exit: Box::pin(node_exit_future),
    })
}
