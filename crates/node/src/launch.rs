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
    builder::{NodeBuilder, NodeHandle},
    core::{
        args::DatadirArgs,
        args::NetworkArgs,
        args::RpcServerArgs,
        dirs::{DataDirPath, MaybePlatformPath},
        node_config::NodeConfig,
    },
    EthereumNode,
};

use reth_node_builder::rpc::RpcAddOns;
use reth_node_ethereum::EthereumEthApiBuilder;

use reth_rpc_builder::Identity;

use crate::allegro_consensus::{AllegroConsensusBuilder, AllegroEngineValidatorBuilder};
use reth_ethereum_engine_primitives::EthEngineTypes;

use reth_payload_builder::PayloadBuilderHandle;
use reth_tasks::TaskExecutor;

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
    /// Genesis block timestamp in milliseconds.
    pub genesis_timestamp_millis: u64,
    /// Opaque handle keeping the reth node alive (owns JSON-RPC server handles).
    _keep_alive: Arc<dyn std::any::Any + Send + Sync>,
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
/// - Standard Ethereum components but with our relaxed-timestamp consensus
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
            ipcdisable: true,
            ..Default::default()
        })
        .with_network(NetworkArgs {
            port: cfg.p2p_port,
            discovery: reth_ethereum::node::core::args::DiscoveryArgs {
                disable_discovery: true,
                ..Default::default()
            },
            ..Default::default()
        });

    // Build standard Ethereum components but with our relaxed-timestamp consensus.
    let components = EthereumNode::components()
        .consensus(AllegroConsensusBuilder);

    // Build add-ons with our custom engine validator (relaxed timestamp check).
    // Use the standard RpcAddOns with our custom PayloadValidatorBuilder.
    let add_ons = reth_node_ethereum::EthereumAddOns::<
        _,
        reth_node_ethereum::EthereumEthApiBuilder,
        AllegroEngineValidatorBuilder,
    >::new(
        RpcAddOns::new(
            EthereumEthApiBuilder::default(),
            AllegroEngineValidatorBuilder,
            reth_node_builder::rpc::BasicEngineApiBuilder::default(),
            reth_node_builder::rpc::BasicEngineValidatorBuilder::default(),
            Identity::new(),
            Identity::new(),
        )
    );

    let NodeHandle {
        node,
        node_exit_future,
    } = NodeBuilder::new(node_config)
        .with_database(db)
        .with_launch_context(task_executor)
        .with_types()
        .with_components(components)
        .with_add_ons(add_ons)
        .launch()
        .await
        .wrap_err("failed to launch reth node")?;

    // Extract handles before type-erasing the node.
    let engine_handle = node.add_ons_handle.beacon_engine_handle.clone();
    let payload_builder_handle = node.payload_builder_handle.clone();
    let genesis_hash = node.chain_spec().genesis_hash();
    let genesis_timestamp = node.chain_spec().genesis_timestamp();
    let genesis_timestamp_millis = genesis_timestamp * 1000;

    // Keep the node alive (dropping it would shut down JSON-RPC servers).
    let keep_alive = Arc::new(node) as Arc<dyn std::any::Any + Send + Sync>;

    Ok(LaunchedRethNode {
        engine_handle,
        payload_builder_handle,
        genesis_hash,
        genesis_timestamp,
        genesis_timestamp_millis,
        _keep_alive: keep_alive,
        exit: Box::pin(node_exit_future),
    })
}
