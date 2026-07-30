//! Application actor — implements [`commonware_consensus::Automaton`] and [`Relay`].
//!
//! Delegates block building and validation to a [`PayloadBuilder`] (executor).
//! Follows Tempo's architecture: the application actor is the glue between
//! consensus and execution layers.
//!
//! # Architecture
//!
//! The actor communicates with the simplex engine via a [`Mailbox`] that
//! implements [`Automaton`]. Messages are sent over an mpsc channel from the
//! engine to the actor, which processes them sequentially:
//!
//! 1. `Genesis` — returns the genesis block digest
//! 2. `Propose` — builds a new block via the payload builder
//! 3. `Verify` — validates a block from another proposer
//! 4. `Broadcast` — engine asks to broadcast a block (handled by the relay)

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;

use alloy_primitives::B256;
use commonware_consensus::{
    simplex::types::Context,
    simplex::Plan,
    types::{Epoch, Round, View},
    Automaton, CertifiableAutomaton,
};
use commonware_cryptography::{ed25519::PublicKey, Signer as _};
use commonware_utils::channel::oneshot;
use futures::{channel::mpsc, SinkExt, StreamExt};
use tracing::{debug, error, info, warn};

use allegro_primitives::Digest as AllegroDigest;

use crate::executor::{BuildPayloadRequest, PayloadBuilder, ValidationResult};
use crate::metrics::ConsensusMetrics;
use crate::validators::ValidatorSet;

// ── Block info tracking ────────────────────────────────────

/// Info about a block tracked by digest.
#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub number: u64,
    pub hash: B256,
    pub view: u64,
    pub proposer: PublicKey,
    /// Block timestamp (seconds since epoch).
    pub timestamp: u64,
    /// Block timestamp (milliseconds since epoch), non-decreasing per chain.
    pub timestamp_millis: u64,
}

// ── Shared block stores ─────────────────────────────────────

/// Blocks we have proposed (shared with the relay for broadcasting).
pub type PendingBlocks = Arc<Mutex<HashMap<AllegroDigest, Vec<u8>>>>;

/// Blocks received from peers (shared between receiver task and actor for verification).
pub type ReceivedBlocks = Arc<RwLock<HashMap<AllegroDigest, Vec<u8>>>>;

/// Per-validator block info tracking (height, parent hash, etc.).
pub type BlockInfoMap = Arc<RwLock<HashMap<AllegroDigest, BlockInfo>>>;

/// Create shared block stores for the actor and relay.
pub fn new_block_stores() -> (PendingBlocks, ReceivedBlocks, BlockInfoMap) {
    (
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(RwLock::new(HashMap::new())),
        Arc::new(RwLock::new(HashMap::new())),
    )
}

// ── Messages ─────────────────────────────────────────────────

/// Messages from the consensus engine to the application actor.
pub enum Message {
    Genesis(Genesis),
    Propose(Box<Propose>),
    Verify(Box<Verify>),
    Broadcast(Box<Broadcast>),
}

pub struct Genesis {
    pub epoch: Epoch,
    pub response: oneshot::Sender<AllegroDigest>,
}

impl From<Genesis> for Message {
    fn from(v: Genesis) -> Self {
        Self::Genesis(v)
    }
}

pub struct Propose {
    pub parent: (View, AllegroDigest),
    pub response: oneshot::Sender<AllegroDigest>,
    pub round: Round,
    pub leader: PublicKey,
}

impl From<Propose> for Message {
    fn from(v: Propose) -> Self {
        Self::Propose(Box::new(v))
    }
}

pub struct Verify {
    pub parent: (View, AllegroDigest),
    pub payload: AllegroDigest,
    pub proposer: PublicKey,
    pub response: oneshot::Sender<bool>,
    pub round: Round,
}

impl From<Verify> for Message {
    fn from(v: Verify) -> Self {
        Self::Verify(Box::new(v))
    }
}

pub struct Broadcast {
    pub digest: AllegroDigest,
    pub plan: Plan<PublicKey>,
}

impl From<Broadcast> for Message {
    fn from(v: Broadcast) -> Self {
        Self::Broadcast(Box::new(v))
    }
}

// ── Mailbox ─────────────────────────────────────────────────

/// Mailbox that implements [`Automaton`] and forwards requests to the actor.
#[derive(Clone)]
pub struct Mailbox {
    sender: mpsc::Sender<Message>,
}

impl Mailbox {
    pub fn new(sender: mpsc::Sender<Message>) -> Self {
        Self { sender }
    }
}

impl Automaton for Mailbox {
    type Context = Context<AllegroDigest, PublicKey>;
    type Digest = AllegroDigest;

    async fn genesis(&mut self, epoch: Epoch) -> Self::Digest {
        let (tx, rx) = oneshot::channel();
        if self
            .sender
            .send(
                Genesis {
                    epoch,
                    response: tx,
                }
                .into(),
            )
            .await
            .is_err()
        {
            warn!("application actor dropped, returning empty digest for genesis");
            return commonware_cryptography::Digest::EMPTY;
        }
        rx.await.unwrap_or(commonware_cryptography::Digest::EMPTY)
    }

    async fn propose(&mut self, context: Self::Context) -> oneshot::Receiver<Self::Digest> {
        let (tx, rx) = oneshot::channel();
        if self
            .sender
            .send(
                Propose {
                    parent: context.parent,
                    response: tx,
                    round: context.round,
                    leader: context.leader,
                }
                .into(),
            )
            .await
            .is_err()
        {
            warn!("application actor dropped in propose");
        }
        rx
    }

    async fn verify(
        &mut self,
        context: Self::Context,
        payload: Self::Digest,
    ) -> oneshot::Receiver<bool> {
        let (tx, rx) = oneshot::channel();
        if self
            .sender
            .send(
                Verify {
                    parent: context.parent,
                    payload,
                    proposer: context.leader,
                    response: tx,
                    round: context.round,
                }
                .into(),
            )
            .await
            .is_err()
        {
            warn!("application actor dropped in verify");
        }
        rx
    }
}

impl CertifiableAutomaton for Mailbox {}

// ── Actor ───────────────────────────────────────────────────

/// The application actor processes consensus events and manages block production.
///
/// This is the core glue between the consensus engine and the execution layer.
/// It receives messages from the engine via its mailbox and delegates to the
/// [`PayloadBuilder`] for actual block construction and validation.
pub struct Actor {
    receiver: mpsc::Receiver<Message>,
    validators: ValidatorSet,
    proposal_count: u64,
    /// All proposed digests, for test verification.
    pub proposals: Arc<Mutex<Vec<AllegroDigest>>>,
    /// Blocks we proposed (relay reads from here to broadcast).
    pending_blocks: PendingBlocks,
    /// Blocks received from peers.
    received_blocks: ReceivedBlocks,
    /// Block info indexed by digest.
    block_info: BlockInfoMap,
    /// Payload builder (delegates to stub or reth engine API).
    payload_builder: Arc<dyn PayloadBuilder>,
    /// Consensus metrics.
    metrics: Option<ConsensusMetrics>,
}

impl Actor {
    /// Create a new application actor.
    ///
    /// Returns the actor and its mailbox. The caller must call [`run()`](Self::run)
    /// to process messages.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        validators: ValidatorSet,
        mailbox_size: usize,
        proposals: Option<Arc<Mutex<Vec<AllegroDigest>>>>,
        pending_blocks: PendingBlocks,
        received_blocks: ReceivedBlocks,
        block_info: BlockInfoMap,
        payload_builder: Arc<dyn PayloadBuilder>,
        metrics: Option<ConsensusMetrics>,
        genesis_hash: B256,
        genesis_timestamp: u64,
        genesis_timestamp_millis: u64,
    ) -> (Self, Mailbox) {
        // Register genesis block info
        let genesis_digest = commonware_cryptography::Digest::EMPTY;
        let genesis_sk = commonware_cryptography::ed25519::PrivateKey::from_seed(0);
        match block_info.write() {
            Ok(mut guard) => {
                guard.insert(
                    genesis_digest,
                    BlockInfo {
                        number: 0,
                        hash: genesis_hash,
                        view: 0,
                        proposer: genesis_sk.public_key(),
                        timestamp: genesis_timestamp,
                        timestamp_millis: genesis_timestamp_millis,
                    },
                );
            }
            Err(e) => {
                error!(error = %e, "block_info write lock poisoned during actor init");
            }
        }

        let (sender, receiver) = mpsc::channel(mailbox_size);
        let mailbox = Mailbox::new(sender);
        let actor = Self {
            receiver,
            validators,
            proposal_count: 0,
            proposals: proposals.unwrap_or_else(|| Arc::new(Mutex::new(Vec::new()))),
            pending_blocks,
            received_blocks,
            block_info,
            payload_builder,
            metrics,
        };
        (actor, mailbox)
    }

    /// Run the actor's event loop, processing messages from the engine.
    pub async fn run(&mut self) {
        info!("application actor started");
        while let Some(msg) = self.receiver.next().await {
            match msg {
                Message::Genesis(g) => self.handle_genesis(g).await,
                Message::Propose(p) => self.handle_propose(*p).await,
                Message::Verify(v) => self.handle_verify(*v).await,
                Message::Broadcast(b) => self.handle_broadcast(*b).await,
            }
        }
        warn!("application actor stopped");
    }

    async fn handle_genesis(&mut self, msg: Genesis) {
        info!(epoch = %msg.epoch.get(), "genesis");
        let digest = commonware_cryptography::Digest::EMPTY;
        // Genesis block info was already registered in `new()`. Just respond.
        let _ = msg.response.send(digest);
    }

    async fn handle_propose(&mut self, msg: Propose) {
        self.proposal_count += 1;
        let parent_digest = msg.parent.1;
        let parent_view = msg.parent.0;

        info!(
            round = %msg.round,
            leader = %msg.leader,
            parent_view = %parent_view.get(),
            parent_digest = %parent_digest,
            count = self.proposal_count,
            "handle_propose"
        );

        if let Some(ref m) = self.metrics {
            m.inc_blocks_proposed();
        }

        // Look up parent block info from our tracking
        let (parent_number, parent_hash, parent_timestamp, parent_timestamp_millis) =
            match self.block_info.read() {
                Ok(guard) => guard
                    .get(&parent_digest)
                    .map(|info| {
                        (
                            info.number,
                            info.hash,
                            info.timestamp,
                            info.timestamp_millis,
                        )
                    })
                    .unwrap_or((0, B256::ZERO, 0, 0)),
                Err(e) => {
                    error!(error = %e, "block_info read lock poisoned");
                    // Drop the responder (see the payload-failure path below).
                    return;
                }
            };

        let proposer_bytes = {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(msg.leader.as_ref());
            bytes
        };

        // Timestamp (seconds) is non-decreasing: equal neighbours are allowed
        // at sub-second block rates (validation is relaxed to `>=`).
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let timestamp = std::cmp::max(now, parent_timestamp);

        // Millisecond timestamp is non-decreasing relative to the parent.
        let now_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let timestamp_millis = std::cmp::max(now_millis, parent_timestamp_millis);

        // Delegate block building to the payload builder
        let request = BuildPayloadRequest {
            parent_hash,
            parent_number,
            parent_view: parent_view.get(),
            parent_digest,
            epoch: msg.round.epoch().get(),
            view: msg.round.view().get(),
            proposer: proposer_bytes,
            timestamp,
            timestamp_millis,
            parent_timestamp_millis,
        };
        let built = self.payload_builder.build_payload(&request).await;

        let (block_bytes, block_hash, block_number, built_timestamp_millis) = match built {
            Ok(payload) => (
                payload.block_bytes,
                payload.block_hash,
                payload.block_number,
                payload.timestamp_millis,
            ),
            Err(e) => {
                error!(error = %e, "payload builder failed");
                if let Some(ref m) = self.metrics {
                    m.inc_errors();
                }
                // Drop the responder without answering: proposing the EMPTY
                // digest would get a genesis-parented "block" notarized and
                // permanently poison parent tracking (every later view would
                // build on genesis while finalization has moved past it).
                // With no proposal the view times out and the next one
                // retries on the same healthy parent.
                return;
            }
        };

        let digest = AllegroDigest(block_hash);

        // Track block info (including timestamp for parent lookups). Use the
        // builder-reported millis so this record matches what verifiers derive.
        match self.block_info.write() {
            Ok(mut guard) => {
                guard.insert(
                    digest,
                    BlockInfo {
                        number: block_number,
                        hash: block_hash,
                        view: msg.round.view().get(),
                        proposer: msg.leader.clone(),
                        timestamp,
                        timestamp_millis: built_timestamp_millis,
                    },
                );
            }
            Err(e) => {
                error!(error = %e, "block_info write lock poisoned in propose");
            }
        }

        // Store in pending_blocks so the relay can broadcast it
        match self.pending_blocks.lock() {
            Ok(mut guard) => {
                guard.insert(digest, block_bytes);
            }
            Err(e) => {
                error!(error = %e, "pending_blocks lock poisoned");
            }
        }

        match self.proposals.lock() {
            Ok(mut guard) => guard.push(digest),
            Err(e) => error!(error = %e, "proposals lock poisoned"),
        }

        info!(%digest, number = block_number, "proposed block");
        let _ = msg.response.send(digest);
    }

    async fn handle_verify(&mut self, msg: Verify) {
        let proposer_valid = self.validators.lookup(&msg.proposer).is_some();
        if !proposer_valid {
            warn!(proposer = %msg.proposer, "unknown proposer");
            if let Some(ref m) = self.metrics {
                m.inc_failed_validations();
            }
            let _ = msg.response.send(false);
            return;
        }

        // Look up block bytes (ours or received from peer)
        let block_bytes = match self.pending_blocks.lock() {
            Ok(guard) => guard.get(&msg.payload).cloned(),
            Err(e) => {
                error!(error = %e, "pending_blocks lock poisoned during verify");
                let _ = msg.response.send(false);
                return;
            }
        }
        .or_else(|| match self.received_blocks.read() {
            Ok(guard) => guard.get(&msg.payload).cloned(),
            Err(e) => {
                error!(error = %e, "received_blocks lock poisoned during verify");
                None
            }
        });

        let Some(block_bytes) = block_bytes else {
            warn!(payload = %msg.payload, "block not found for verification");
            if let Some(ref m) = self.metrics {
                m.inc_failed_validations();
            }
            let _ = msg.response.send(false);
            return;
        };

        // Delegate validation to the payload builder
        let expected_parent_hash = msg.parent.1 .0;
        debug!(
            payload = %msg.payload,
            round = %msg.round,
            "verifying block"
        );

        match self
            .payload_builder
            .validate_block(block_bytes, expected_parent_hash)
            .await
        {
            Ok(ValidationResult::Valid(meta)) => {
                debug!(payload = %msg.payload, "verify succeeded");
                // Record block info for future parent lookups (critical for reth mode)
                match self.block_info.write() {
                    Ok(mut guard) => {
                        guard.insert(
                            msg.payload,
                            BlockInfo {
                                number: meta.number,
                                hash: meta.hash,
                                view: msg.round.view().get(),
                                proposer: msg.proposer.clone(),
                                timestamp: meta.timestamp,
                                timestamp_millis: meta.timestamp_millis,
                            },
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "block_info lock poisoned during verify record");
                    }
                }
                if let Some(ref m) = self.metrics {
                    m.inc_blocks_verified();
                }
                let _ = msg.response.send(true);
            }
            Ok(ValidationResult::Invalid(reason)) => {
                warn!(payload = %msg.payload, reason = %reason, "verify failed");
                if let Some(ref m) = self.metrics {
                    m.inc_failed_validations();
                }
                let _ = msg.response.send(false);
            }
            Err(e) => {
                error!(payload = %msg.payload, error = %e, "verify error");
                if let Some(ref m) = self.metrics {
                    m.inc_errors();
                    m.inc_failed_validations();
                }
                let _ = msg.response.send(false);
            }
        }
    }

    async fn handle_broadcast(&mut self, msg: Broadcast) {
        debug!(digest = %msg.digest, "broadcast request from engine");
    }
}
