//! Simplex consensus engine with block propagation over p2p.
//!
//! Wires the simplex engine with:
//! - A [`BlockRelay`] that broadcasts proposed blocks on a dedicated p2p channel
//! - A block receiver task that caches incoming blocks
//! - A tracing reporter that logs consensus activity
//! - Persistence via a marshal-compatible archive
//! - Full metrics collection
//! - Configurable parameters with sensible defaults

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_primitives::B256;
use bytes::Buf;
use commonware_consensus::{
    simplex::{
        elector::RoundRobin, scheme::ed25519, types::Activity, Engine, ForwardingPolicy, Plan,
    },
    types::{Epoch, ViewDelta},
    Relay, Reporter,
};
use commonware_cryptography::{
    ed25519::{PrivateKey, PublicKey},
    Digest, Sha256, Signer as _,
};
use commonware_p2p::{Receiver, Recipients, Sender};
use commonware_runtime::{
    buffer::paged::CacheRef, BufferPooler, Clock, Handle, Metrics, Network, Pacer, Spawner, Storage,
};
use commonware_utils::{ordered::Set, NZUsize, NZU16};
use rand_08::{CryptoRng, Rng};
use tracing::{debug, error, info, warn};

use allegro_primitives::Digest as AllegroDigest;

use crate::application::{self, BlockInfoMap, PendingBlocks, ReceivedBlocks};
use crate::config::ConsensusConfig;
use crate::error::ConsensusError;
use crate::executor::StubPayloadBuilder;
use crate::metrics::ConsensusMetrics;
use crate::validators::ValidatorSet;

// ── Block relay ────────────────────────────────────────────

/// Relay that broadcasts proposed blocks over the p2p block channel.
///
/// Uses a dedicated channel to avoid blocking consensus on block distribution.
/// Wire format: 32-byte digest followed by RLP-encoded block bytes.
#[derive(Clone)]
pub struct BlockRelay<S> {
    pending: PendingBlocks,
    sender: S,
    metrics: Option<ConsensusMetrics>,
}

impl<S: Sender<PublicKey = PublicKey> + Send + 'static> BlockRelay<S> {
    /// Create a new block relay.
    ///
    /// `pending` is the store of blocks we've proposed (shared with the actor).
    /// `sender` is the p2p sender for the block channel.
    pub fn new(pending: PendingBlocks, sender: S, metrics: Option<ConsensusMetrics>) -> Self {
        Self {
            pending,
            sender,
            metrics,
        }
    }
}

impl<S: Sender<PublicKey = PublicKey> + Send + 'static> Relay for BlockRelay<S> {
    type Digest = AllegroDigest;
    type PublicKey = PublicKey;
    type Plan = Plan<PublicKey>;

    async fn broadcast(&mut self, digest: Self::Digest, _plan: Self::Plan) {
        // Look up the block bytes from our pending store
        let block_bytes = {
            let pending = self.pending.lock().expect("pending lock poisoned");
            pending.get(&digest).cloned()
        };

        let Some(block_bytes) = block_bytes else {
            debug!(%digest, "broadcast called but block not in pending store");
            return;
        };

        // Wire format: 32-byte digest + block bytes
        let mut msg = Vec::with_capacity(32 + block_bytes.len());
        msg.extend_from_slice(digest.as_ref());
        msg.extend_from_slice(&block_bytes);

        match self.sender.send(Recipients::All, msg, false).await {
            Ok(sent) => {
                info!(%digest, count = sent.len(), "broadcast block to peers");
                if let Some(ref m) = self.metrics {
                    m.inc_blocks_broadcast();
                }
            }
            Err(e) => {
                warn!(?e, "failed to broadcast block");
                if let Some(ref m) = self.metrics {
                    m.inc_errors();
                }
            }
        }
    }
}

// ── Block receiver task ────────────────────────────────────

/// Spawn a background task that receives blocks from the p2p block channel
/// and stores them in the shared received_blocks store.
pub fn spawn_block_receiver<R: Receiver<PublicKey = PublicKey> + Send + 'static>(
    context: impl Spawner + 'static,
    mut rx: R,
    received: ReceivedBlocks,
    _block_info: BlockInfoMap,
    metrics: Option<ConsensusMetrics>,
) {
    context.spawn(|_| async move {
        info!("block receiver started");
        loop {
            match rx.recv().await {
                Ok((sender, data)) => {
                    if data.remaining() < 32 {
                        warn!("received malformed block message (too short)");
                        continue;
                    }
                    // Wire format: 32-byte digest + block bytes
                    let bytes: &[u8] = data.as_ref();
                    let mut digest_arr = [0u8; 32];
                    digest_arr.copy_from_slice(&bytes[..32]);
                    let digest = AllegroDigest(B256::from(digest_arr));

                    let block_vec = bytes[32..].to_vec();

                    match received.write() {
                        Ok(mut guard) => {
                            guard.insert(digest, block_vec);
                            debug!(%digest, sender = %sender, "received block from peer");
                            if let Some(ref m) = metrics {
                                m.inc_blocks_received();
                            }
                        }
                        Err(e) => {
                            error!(%digest, error = %e, "received_blocks lock poisoned");
                            if let Some(ref m) = metrics {
                                m.inc_errors();
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(?e, "block receiver error, restarting");
                    if let Some(ref m) = metrics {
                        m.inc_errors();
                    }
                    // Brief pause before retry to avoid busy loop
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });
}

// ── Metrics Reporter ─────────────────────────────────────

/// Reporter that logs all consensus activity via tracing, records metrics,
/// and optionally forwards finalization digests for reth FCU updates.
#[derive(Clone)]
pub struct MetricsReporter<S, D> {
    inner: TracingReporter<S, D>,
    metrics: Option<ConsensusMetrics>,
    finalized_tx: Option<futures::channel::mpsc::Sender<AllegroDigest>>,
}

impl<S: commonware_cryptography::certificate::Scheme> Reporter
    for MetricsReporter<S, AllegroDigest>
{
    type Activity = Activity<S, AllegroDigest>;

    async fn report(&mut self, activity: Self::Activity) {
        // Forward finalization to the reth finalizer task.
        if let Activity::Finalization(cert) = &activity {
            if let Some(ref mut tx) = self.finalized_tx {
                if let Err(e) = tx.try_send(cert.proposal.payload) {
                    if e.is_full() {
                        warn!("finalization sink full; dropping digest");
                    }
                }
            }
            if let Some(ref m) = self.metrics {
                m.inc_blocks_finalized();
            }
        }
        self.inner.report(activity).await;
    }
}

impl<S, D> MetricsReporter<S, D> {
    pub fn new(
        metrics: Option<ConsensusMetrics>,
        finalized_tx: Option<futures::channel::mpsc::Sender<AllegroDigest>>,
    ) -> Self {
        Self {
            inner: TracingReporter::new(),
            metrics,
            finalized_tx,
        }
    }
}

// ── Tracing Reporter ───────────────────────────────────────

/// Reporter that logs all consensus activity via tracing.
#[derive(Clone)]
pub struct TracingReporter<S, D>(PhantomData<(S, D)>);

impl<S: commonware_cryptography::certificate::Scheme, D: Digest> Reporter
    for TracingReporter<S, D>
{
    type Activity = Activity<S, D>;

    async fn report(&mut self, activity: Self::Activity) {
        match activity {
            Activity::Notarize(vote) => {
                info!(view = %vote.round().view(), "notarize vote");
            }
            Activity::Notarization(cert) => {
                info!(
                    view = %cert.proposal.round.view(),
                    digest = %cert.proposal.payload,
                    "notarization certificate"
                );
            }
            Activity::Certification(cert) => {
                info!(
                    view = %cert.proposal.round.view(),
                    digest = %cert.proposal.payload,
                    "local certification"
                );
            }
            Activity::Nullify(vote) => {
                info!(view = %vote.round().view(), "nullify vote");
            }
            Activity::Nullification(cert) => {
                info!(view = %cert.round.view(), "nullification certificate");
            }
            Activity::Finalize(vote) => {
                info!(view = %vote.round().view(), "finalize vote");
            }
            Activity::Finalization(cert) => {
                info!(
                    view = %cert.proposal.round.view(),
                    digest = %cert.proposal.payload,
                    "FINALIZATION certificate"
                );
            }
            Activity::ConflictingNotarize(_) => {
                warn!("conflicting notarize evidence");
            }
            Activity::ConflictingFinalize(_) => {
                warn!("conflicting finalize evidence");
            }
            Activity::NullifyFinalize(_) => {
                warn!("nullify+finalize evidence");
            }
        }
    }
}

impl<S, D> TracingReporter<S, D> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<S, D> Default for TracingReporter<S, D> {
    fn default() -> Self {
        Self::new()
    }
}

// ── Config ─────────────────────────────────────────────────

/// Engine configuration for bootstrapping the simplex engine.
///
/// Follows Tempo's approach: takes a [`ConsensusConfig`] for tunable parameters
/// and adds runtime-only fields (signing key, validator set, payload builder).
#[derive(Clone)]
pub struct EngineConfig {
    /// The ed25519 signing key for this validator.
    pub signing_key: PrivateKey,
    /// The set of validators participating in consensus.
    pub validators: ValidatorSet,
    /// Consensus parameters with sensible defaults.
    pub consensus_config: ConsensusConfig,
    /// Collection point for proposed block digests (for testing).
    pub proposals: Arc<Mutex<Vec<AllegroDigest>>>,
    /// Namespace partition for on-disk storage isolation.
    pub partition: String,
    /// Optional payload builder. If `None`, a [`StubPayloadBuilder`] is used.
    pub payload_builder: Option<std::sync::Arc<dyn crate::executor::PayloadBuilder>>,
    /// Optional metrics collector.
    pub metrics: Option<ConsensusMetrics>,
    /// Genesis block hash (B256::ZERO for stub mode, real chainspec hash for reth).
    pub genesis_hash: B256,
    /// Genesis block timestamp.
    pub genesis_timestamp: u64,
    /// Optional channel for forwarding finalized block digests (reth finalization).
    pub finalized_tx: Option<futures::channel::mpsc::Sender<AllegroDigest>>,
}

impl EngineConfig {
    /// Create a new engine configuration with the given signing key and validators.
    pub fn new(signing_key: PrivateKey, validators: ValidatorSet) -> Self {
        Self {
            signing_key,
            validators,
            consensus_config: ConsensusConfig::default(),
            proposals: Arc::new(Mutex::new(Vec::new())),
            partition: ConsensusConfig::default().partition,
            payload_builder: None,
            metrics: None,
            genesis_hash: B256::ZERO,
            genesis_timestamp: 0,
            finalized_tx: None,
        }
    }

    /// Set the consensus configuration.
    pub fn with_consensus_config(mut self, config: ConsensusConfig) -> Self {
        self.consensus_config = config;
        self
    }

    /// Set the payload builder.
    pub fn with_payload_builder(
        mut self,
        builder: Arc<dyn crate::executor::PayloadBuilder>,
    ) -> Self {
        self.payload_builder = Some(builder);
        self
    }

    /// Set the metrics collector.
    pub fn with_metrics(mut self, metrics: ConsensusMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        let cfg = ConsensusConfig::default();
        Self {
            signing_key: PrivateKey::from_seed(0),
            validators: ValidatorSet::new(),
            consensus_config: cfg.clone(),
            proposals: Arc::new(Mutex::new(Vec::new())),
            partition: cfg.partition,
            payload_builder: None,
            metrics: None,
            genesis_hash: B256::ZERO,
            genesis_timestamp: 0,
            finalized_tx: None,
        }
    }
}

// ── Entry point ────────────────────────────────────────────

/// Handle returned from [`start_simplex_engine`].
pub struct StartedEngine {
    /// Handle that keeps the engine alive.
    pub task: Handle<()>,
    /// Shared block info map for digest → hash lookups (used by finalizer).
    pub block_info: BlockInfoMap,
}

/// Start a simplex engine with block propagation.
///
/// Takes p2p channels for consensus messages (votes, certs, resolver) plus a
/// dedicated channel for block propagation. The engine broadcasts newly proposed
/// blocks on the block channel and caches blocks received from peers for
/// verification.
///
/// # Panics
///
/// Panics if the validator set is empty.
pub fn start_simplex_engine<TContext, BS, BR, BL>(
    context: TContext,
    config: EngineConfig,
    votes: (
        impl Sender<PublicKey = PublicKey> + 'static,
        impl Receiver<PublicKey = PublicKey> + 'static,
    ),
    certs: (
        impl Sender<PublicKey = PublicKey> + 'static,
        impl Receiver<PublicKey = PublicKey> + 'static,
    ),
    resolver: (
        impl Sender<PublicKey = PublicKey> + 'static,
        impl Receiver<PublicKey = PublicKey> + 'static,
    ),
    block_sender: BS,
    block_receiver: BR,
    blocker: BL,
) -> Result<StartedEngine, ConsensusError>
where
    TContext: BufferPooler
        + Clock
        + governor::clock::Clock
        + Rng
        + CryptoRng
        + Metrics
        + Network
        + Pacer
        + Spawner
        + Storage
        + 'static,
    BS: Sender<PublicKey = PublicKey> + Clone + Send + 'static,
    BR: Receiver<PublicKey = PublicKey> + Send + 'static,
    BL: commonware_p2p::Blocker<PublicKey = PublicKey> + Clone + Send + 'static,
{
    let public_key = config.signing_key.public_key();
    info!(%public_key, "starting simplex engine");

    let pks: Vec<PublicKey> = config.validators.keys();
    if pks.is_empty() {
        return Err(ConsensusError::InvalidValidatorConfig(
            "validator set is empty".into(),
        ));
    }

    let participants = Set::try_from(pks).map_err(|e| {
        ConsensusError::InvalidValidatorConfig(format!("duplicate validators: {e}"))
    })?;

    let namespace = b"allegro";
    let scheme = ed25519::Scheme::signer(namespace, participants.clone(), config.signing_key)
        .unwrap_or_else(|| ed25519::Scheme::verifier(namespace, participants.clone()));

    let elector = RoundRobin::<Sha256>::default();

    // Create block stores shared between actor, relay, and receiver
    let (pending_blocks, received_blocks, block_info) = application::new_block_stores();

    let metrics = config.metrics;

    // Create the relay that broadcasts blocks on the p2p block channel
    let relay = BlockRelay::new(pending_blocks.clone(), block_sender, metrics.clone());

    // Spawn the block receiver task
    spawn_block_receiver(
        context.with_label("block_rx"),
        block_receiver,
        received_blocks.clone(),
        block_info.clone(),
        metrics.clone(),
    );

    // Use the provided payload builder or fall back to the stub (empty blocks)
    let payload_builder: std::sync::Arc<dyn crate::executor::PayloadBuilder> = config
        .payload_builder
        .unwrap_or_else(|| std::sync::Arc::new(StubPayloadBuilder::new()));

    // Create the application actor (registers genesis block info internally)
    let (mut actor, mailbox) = application::Actor::new(
        config.validators,
        config.consensus_config.mailbox_size,
        Some(config.proposals.clone()),
        pending_blocks,
        received_blocks,
        block_info.clone(),
        payload_builder,
        metrics.clone(),
        config.genesis_hash,
        config.genesis_timestamp,
    );

    let page_cache = CacheRef::from_pooler(
        &context,
        NZU16!(config.consensus_config.page_cache_pages),
        NZUsize!(config.consensus_config.page_cache_capacity),
    );

    // Map our forwarding policy to commonware's
    let forwarding = match config.consensus_config.forwarding_policy {
        crate::config::ForwardingPolicy::SilentVoters => ForwardingPolicy::SilentVoters,
        crate::config::ForwardingPolicy::All => ForwardingPolicy::SilentVoters,
    };

    // Build the simplex engine
    let engine = Engine::new(
        context.with_label("simplex"),
        commonware_consensus::simplex::Config {
            scheme,
            elector,
            blocker,
            automaton: mailbox,
            relay,
            reporter: MetricsReporter::<ed25519::Scheme, AllegroDigest>::new(
                metrics.clone(),
                config.finalized_tx,
            ),
            strategy: commonware_parallel::Sequential,
            partition: config.partition.clone(),
            mailbox_size: config.consensus_config.mailbox_size,
            epoch: Epoch::new(0),
            leader_timeout: config.consensus_config.leader_timeout,
            certification_timeout: config.consensus_config.certification_timeout,
            timeout_retry: config.consensus_config.timeout_retry,
            activity_timeout: ViewDelta::new(config.consensus_config.activity_timeout),
            skip_timeout: ViewDelta::new(config.consensus_config.skip_timeout),
            fetch_timeout: config.consensus_config.fetch_timeout,
            fetch_concurrent: config.consensus_config.fetch_concurrent,
            forwarding,
            replay_buffer: NZUsize!(config.consensus_config.replay_buffer_size),
            write_buffer: NZUsize!(config.consensus_config.write_buffer_size),
            page_cache,
        },
    );

    // Spawn the actor
    context.spawn(|_| async move {
        actor.run().await;
        warn!("application actor exited");
    });

    // Start the engine — returns a Handle that keeps it alive
    let task = engine.start(votes, certs, resolver);
    Ok(StartedEngine { task, block_info })
}
