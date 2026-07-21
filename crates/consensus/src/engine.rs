//! Assembles and runs the consensus stack.
//!
//! Initializes the commonware consensus engine, p2p network, and
//! all supporting actors, then runs them in a `tokio::select!` loop.

use std::sync::Arc;
use std::time::Duration;

use commonware_consensus::{
    Reporters, marshal,
    simplex::{self, scheme::bls12381_threshold::vrf::Scheme},
    types::{FixedEpocher, ViewDelta},
};
use commonware_cryptography::{
    Digestible,
    ed25519::{PrivateKey, PublicKey},
};
use commonware_p2p::authenticated::lookup;
use commonware_runtime::{BufferPooler, Clock, Handle, Metrics, Network, Pacer, Spawner, Storage};
use commonware_utils::NZUsize;
use eyre::{OptionExt, WrapErr};
use futures::future::try_join_all;
use rand_08::{CryptoRng, Rng};
use tracing::info;

use allegro_primitives::Digest;

use crate::{
    application,
    block::Block,
    config,
    executor,
    validators::ValidatorSet,
};

/// Settings for the consensus engine.
#[derive(Clone)]
pub struct EngineConfig {
    /// Ed25519 signing key for this node.
    pub signing_key: PrivateKey,

    /// Optional BLS12-381 threshold share (for threshold signing).
    /// Set to `None` for non-validating nodes or simple Ed25519 mode.
    pub share: Option<commonware_cryptography::bls12381::primitives::group::Share>,

    /// Validator set for this node.
    pub validators: ValidatorSet,

    /// P2P listen address.
    pub listen_address: std::net::SocketAddr,

    /// Maximum P2P message size.
    pub max_message_size: usize,

    /// Mailbox capacity.
    pub mailbox_size: usize,

    /// Deque capacity.
    pub deque_size: usize,

    /// Target block time (e.g. 1s).
    pub target_block_time: Duration,

    /// Network budget for block propagation.
    pub network_budget: Duration,

    /// Timeout for waiting for a proposal.
    pub time_to_propose: Duration,

    /// Timeout for collecting notarizations.
    pub time_to_collect_notarizations: Duration,

    /// Number of views to track.
    pub views_to_track: u64,

    /// Epoch length in blocks.
    pub epoch_length: u64,

    /// Synchrony bound for p2p.
    pub synchrony_bound: Duration,

    /// Handshake age limit.
    pub handshake_stale_after: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            signing_key: PrivateKey::from_seed([0u8; 32]),
            share: None,
            validators: ValidatorSet::new(),
            listen_address: "0.0.0.0:3000".parse().unwrap(),
            max_message_size: 1024 * 1024,
            mailbox_size: 1024,
            deque_size: 256,
            target_block_time: Duration::from_secs(1),
            network_budget: Duration::from_millis(500),
            time_to_propose: Duration::from_secs(1),
            time_to_collect_notarizations: Duration::from_secs(4),
            views_to_track: 10,
            epoch_length: 100,
            synchrony_bound: Duration::from_secs(2),
            handshake_stale_after: Duration::from_secs(300),
        }
    }
}

/// Start the full consensus stack.
///
/// This initializes the p2p network, the commonware consensus engine,
/// the executor, and runs them all concurrently.
pub async fn run_consensus_stack(
    context: commonware_runtime::tokio::Context,
    config: EngineConfig,
) -> eyre::Result<()> {
    let epoch_strategy = FixedEpocher::new(std::num::NonZeroU64::new(config.epoch_length)
        .ok_or_eyre("epoch_length must be >= 1")?);

    let proposal_return_budget = config
        .target_block_time
        .saturating_sub(config.network_budget);

    // ── P2P network ──
    let namespace = commonware_utils::union_unique(crate::config::NAMESPACE, b"_P2P");

    let p2p_cfg = lookup::Config {
        namespace,
        crypto: config.signing_key.clone(),
        listen: config.listen_address,
        max_message_size: config.max_message_size,
        mailbox_size: config.mailbox_size,
        send_batch_size: NZUsize!(8),
        bypass_ip_check: false,
        allow_private_ips: true,
        allow_dns: false,
        tracked_peer_sets: config::PEERSETS_TO_TRACK,
        synchrony_bound: config.synchrony_bound,
        max_handshake_age: config.handshake_stale_after,
        handshake_timeout: Duration::from_secs(5),
        max_concurrent_handshakes: 128,
        block_duration: Duration::from_secs(60),
        dial_frequency: Duration::from_secs(30),
        ping_frequency: Duration::from_secs(10),
        peer_connection_cooldown: Duration::from_secs(10),
        allowed_handshake_rate_per_ip: commonware_runtime::Quota::with_period(
            Duration::from_millis(100),
        )
        .ok_or_eyre("non-zero handshake period required")?,
        allowed_handshake_rate_per_subnet: commonware_runtime::Quota::with_period(
            Duration::from_millis(100),
        )
        .ok_or_eyre("non-zero handshake period required")?,
    };

    let (mut network, oracle) =
        lookup::Network::new(context.with_label("network"), p2p_cfg);

    let message_backlog = config.mailbox_size;

    let votes = network.register(
        config::VOTES_CHANNEL_IDENT,
        config::VOTES_LIMIT,
        message_backlog,
    );
    let certificates = network.register(
        config::CERTIFICATES_CHANNEL_IDENT,
        config::CERTIFICATES_LIMIT,
        message_backlog,
    );
    let resolver = network.register(
        config::RESOLVER_CHANNEL_IDENT,
        config::RESOLVER_LIMIT,
        message_backlog,
    );
    let broadcaster = network.register(
        config::BROADCASTER_CHANNEL_IDENT,
        config::BROADCASTER_LIMIT,
        message_backlog,
    );
    let marshal_ch = network.register(
        config::MARSHAL_CHANNEL_IDENT,
        config::MARSHAL_LIMIT,
        message_backlog,
    );

    // ── Storage ──
    let page_cache = commonware_runtime::buffer::paged::CacheRef::from_pooler(
        &context,
        config::BUFFER_POOL_PAGE_SIZE,
        config::BUFFER_POOL_CAPACITY,
    );

    let finalizations_by_height = commonware_storage::archive::immutable::Archive::init(
        context.with_label("finalizations"),
        commonware_storage::archive::immutable::Config {
            partition: format!("{}/{}", config::PARTITION_PREFIX, config::FINALIZATIONS_BY_HEIGHT),
            items_per_section: config::IMMUTABLE_ITEMS_PER_SECTION,
            mailbox_size: config.mailbox_size,
            page_cache: page_cache.clone(),
            replay_buffer: config::REPLAY_BUFFER,
            key_write_buffer: config::WRITE_BUFFER,
            max_repair: config::MAX_REPAIR,
            codec_config: (),
        },
    )
    .await
    .wrap_err("failed to init finalizations archive")?;

    // ── Marshal ──
    // For now, use a placeholder finalized blocks store.
    // In production this would be a Hybrid store backed by reth.

    // ── Executor ──
    let (mut executor, executor_mailbox) = executor::Executor::init(
        context.with_label("executor"),
        executor::Config {
            fcu_heartbeat_interval: Duration::from_secs(10),
        },
    )
    .wrap_err("failed to init executor")?;

    // ── Application ──
    let app = application::Application::new(
        context.with_label("application"),
        application::Config {
            public_key: config.signing_key.public_key(),
            signing_key: config.signing_key.clone(),
            mailbox_size: config.mailbox_size,
            executor_mailbox,
            validators: config.validators,
        },
    );

    // ── Consensus Engine ──
    // For the minimal integration, we set up the simplex engine with
    // placeholder storage until the full marshal integration is wired.

    info!(
        identity = %config.signing_key.public_key(),
        "allegro consensus engine configured",
    );

    // Start the network
    let network_fut = network.start();

    // For the initial minimal version, we just keep the executor running
    // and the network active. The full simplex engine integration requires
    // the marshal actor which depends on reth's block storage.

    // ── Run ──
    tokio::select! {
        ret = network_fut => {
            ret.map_err(eyre::Report::from)
                .and_then(|()| Err(eyre!("network exited unexpectedly")))
                .wrap_err("network task failed")
        }
        ret = executor.run() => {
            ret.wrap_err("executor task failed")
        }
    }
}
