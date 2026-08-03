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
//! engine to the actor, which handles them concurrently:
//!
//! 1. `Genesis` — returns the genesis block digest
//! 2. `Propose` — builds a new block via the payload builder
//! 3. `Verify` — validates a block from another proposer
//! 4. `Broadcast` — engine asks to broadcast a block (handled by the relay)
//!
//! Accepting a block also starts the next view's payload, when this node is
//! the one that will propose it. The forkchoice update that registers the job
//! moves the execution layer's head to the just-verified block *before* any
//! quorum exists: RPC `latest` may briefly name a block that never notarizes,
//! rewound by the next view's forkchoice update. `safe` and `finalized` only
//! ever move on finalization.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;

use alloy_primitives::B256;
use commonware_consensus::{
    simplex::elector::{Config as ElectorConfig, Elector, RoundRobin, RoundRobinElector},
    simplex::scheme::ed25519,
    simplex::types::Context,
    simplex::Plan,
    types::{Epoch, Participant, Round, View},
    Automaton, CertifiableAutomaton,
};
use commonware_cryptography::{ed25519::PublicKey, Sha256, Signer as _};
use commonware_utils::{channel::oneshot, ordered::Set};
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
    /// Block timestamp (milliseconds since epoch), monotonically increasing.
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

// ── Leader schedule ─────────────────────────────────────────

/// Answers "do I lead this round?" for rounds that have not happened yet.
///
/// Takes the elector value the engine votes with, so swapping electors fails
/// to compile here rather than silently predicting the wrong rotation.
/// Round-robin ignores the previous view's certificate, which is what makes a
/// future leader knowable at all; a VRF elector would not be predictable and
/// this type would have to answer "unknown".
#[derive(Clone)]
pub struct LeaderSchedule {
    elector: RoundRobinElector<ed25519::Scheme>,
    me: PublicKey,
    /// `None` if this node is not a validator, and so leads nothing.
    me_index: Option<Participant>,
}

impl LeaderSchedule {
    /// `elector` and `participants` must be the ones the engine was configured
    /// with.
    pub fn new(elector: RoundRobin<Sha256>, participants: &Set<PublicKey>, me: PublicKey) -> Self {
        Self {
            elector: ElectorConfig::<ed25519::Scheme>::build(elector, participants),
            me_index: participants.position(&me).map(Participant::from_usize),
            me,
        }
    }

    /// This node's public key.
    pub fn me(&self) -> &PublicKey {
        &self.me
    }

    /// Whether this node leads `round`.
    pub fn leads(&self, round: Round) -> bool {
        self.me_index == Some(self.elector.elect(round, None))
    }
}

// ── Actor ───────────────────────────────────────────────────

/// Everything [`Actor::new`] needs to run.
pub struct ActorConfig {
    /// Validator set, used to reject proposals from unknown proposers.
    pub validators: ValidatorSet,
    /// Mailbox capacity (message backlog from the engine).
    pub mailbox_size: usize,
    /// Cap on handlers in flight; zero disables it.
    pub max_concurrent_handlers: usize,
    /// Collection point for proposed digests (for testing).
    pub proposals: Option<Arc<Mutex<Vec<AllegroDigest>>>>,
    /// Blocks we proposed (relay reads from here to broadcast).
    pub pending_blocks: PendingBlocks,
    /// Blocks received from peers.
    pub received_blocks: ReceivedBlocks,
    /// Block info indexed by digest.
    pub block_info: BlockInfoMap,
    /// Payload builder (delegates to the execution layer).
    pub payload_builder: Arc<dyn PayloadBuilder>,
    /// Optional metrics collector.
    pub metrics: Option<ConsensusMetrics>,
    /// Genesis block hash (from the chainspec).
    pub genesis_hash: B256,
    /// Genesis block timestamp (seconds).
    pub genesis_timestamp: u64,
    /// Genesis block timestamp (milliseconds).
    pub genesis_timestamp_millis: u64,
    /// Leader prediction, used to start the next view's payload early.
    /// Without one the actor never prepares and every proposal builds cold.
    pub leader_schedule: Option<LeaderSchedule>,
}

/// The application actor processes consensus events and manages block production.
///
/// This is the core glue between the consensus engine and the execution layer.
/// It receives messages from the engine via its mailbox and delegates to the
/// [`PayloadBuilder`] for actual block construction and validation.
pub struct Actor {
    receiver: mpsc::Receiver<Message>,
    inner: Inner,
    /// `None` disables the cap, matching `for_each_concurrent`.
    max_concurrent_handlers: Option<usize>,
}

/// The actor's state, borrowed by every in-flight message handler. Fields
/// mirror [`ActorConfig`], which documents them.
struct Inner {
    validators: ValidatorSet,
    proposal_count: AtomicU64,
    proposals: Arc<Mutex<Vec<AllegroDigest>>>,
    pending_blocks: PendingBlocks,
    received_blocks: ReceivedBlocks,
    block_info: BlockInfoMap,
    payload_builder: Arc<dyn PayloadBuilder>,
    metrics: Option<ConsensusMetrics>,
    leader_schedule: Option<LeaderSchedule>,
}

impl Actor {
    /// Create a new application actor.
    ///
    /// Returns the actor and its mailbox. The caller must call [`run()`](Self::run)
    /// to process messages.
    pub fn new(config: ActorConfig) -> (Self, Mailbox) {
        let ActorConfig {
            validators,
            mailbox_size,
            max_concurrent_handlers,
            proposals,
            pending_blocks,
            received_blocks,
            block_info,
            payload_builder,
            metrics,
            genesis_hash,
            genesis_timestamp,
            genesis_timestamp_millis,
            leader_schedule,
        } = config;

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
            max_concurrent_handlers: (max_concurrent_handlers > 0)
                .then_some(max_concurrent_handlers),
            inner: Inner {
                validators,
                proposal_count: AtomicU64::new(0),
                proposals: proposals.unwrap_or_else(|| Arc::new(Mutex::new(Vec::new()))),
                pending_blocks,
                received_blocks,
                block_info,
                payload_builder,
                metrics,
                leader_schedule,
            },
        };
        (actor, mailbox)
    }

    /// Run the actor's event loop, processing messages from the engine.
    ///
    /// Handlers run concurrently: awaiting a payload build in the receive loop
    /// would stall `Verify` behind it, so this node would stop voting while it
    /// proposes. Ordering that matters is the engine's — a parent is notarized,
    /// and so verified, before any view asks us to build on it.
    pub async fn run(self) {
        info!("application actor started");
        let Self {
            receiver,
            inner,
            max_concurrent_handlers,
        } = self;
        receiver
            .for_each_concurrent(max_concurrent_handlers, |msg| inner.handle(msg))
            .await;
        warn!("application actor stopped");
    }
}

impl Inner {
    async fn handle(&self, msg: Message) {
        match msg {
            Message::Genesis(g) => self.handle_genesis(g).await,
            Message::Propose(p) => self.handle_propose(*p).await,
            Message::Verify(v) => self.handle_verify(*v).await,
            Message::Broadcast(b) => self.handle_broadcast(*b).await,
        }
    }

    async fn handle_genesis(&self, msg: Genesis) {
        info!(epoch = %msg.epoch.get(), "genesis");
        let digest = commonware_cryptography::Digest::EMPTY;
        // Genesis block info was already registered in `new()`. Just respond.
        let _ = msg.response.send(digest);
    }

    async fn handle_propose(&self, msg: Propose) {
        let count = self.proposal_count.fetch_add(1, Ordering::Relaxed) + 1;
        let parent_digest = msg.parent.1;
        let parent_view = msg.parent.0;

        info!(
            round = %msg.round,
            leader = %msg.leader,
            parent_view = %parent_view.get(),
            parent_digest = %parent_digest,
            count,
            "handle_propose"
        );

        if let Some(ref m) = self.metrics {
            m.inc_blocks_proposed();
        }

        // A parent we have never seen cannot be built on: substituting genesis
        // here would get a genesis-parented block notarized (see the
        // payload-failure path below), so skip the view instead — dropping the
        // responder, not answering.
        let parent = self
            .lookup_block_info(&parent_digest)
            .map(|info| Parent::new(parent_digest, &info));
        let Some(parent) = parent else {
            warn!(%parent_digest, "no block info for parent; skipping view");
            if let Some(ref m) = self.metrics {
                m.inc_errors();
            }
            return;
        };

        // Delegate block building to the payload builder. If a payload was
        // prepared for this parent the builder reuses it, and the timestamps
        // in the request are the ones it already froze — which is why the
        // block's own timestamps, not the request's, go into the block info
        // record below.
        let request = child_request(msg.round, parent_view.get(), parent, &msg.leader);
        let built = self.payload_builder.build_payload(&request).await;

        let payload = match built {
            Ok(payload) => payload,
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

        let digest = AllegroDigest(payload.block_hash);
        self.record_block_info(
            digest,
            BlockInfo {
                number: payload.block_number,
                hash: payload.block_hash,
                view: msg.round.view().get(),
                proposer: msg.leader.clone(),
                // Builder-reported, so this record matches what verifiers
                // derive from the block itself.
                timestamp: payload.timestamp,
                timestamp_millis: payload.timestamp_millis,
            },
        );
        let proposed = Parent {
            digest,
            hash: payload.block_hash,
            number: payload.block_number,
            timestamp_millis: payload.timestamp_millis,
        };

        // Store in pending_blocks so the relay can broadcast it
        match self.pending_blocks.lock() {
            Ok(mut guard) => {
                guard.insert(digest, payload.block_bytes);
            }
            Err(e) => {
                error!(error = %e, "pending_blocks lock poisoned");
            }
        }

        match self.proposals.lock() {
            Ok(mut guard) => guard.push(digest),
            Err(e) => error!(error = %e, "proposals lock poisoned"),
        }

        info!(%digest, number = payload.block_number, "proposed block");
        let _ = msg.response.send(digest);

        // Only reached when this node leads consecutive views, which
        // round-robin allows only in a single-validator network.
        self.prepare_next_view(msg.round, proposed).await;
    }

    async fn handle_verify(&self, msg: Verify) {
        let proposer_valid = self.validators.lookup(&msg.proposer).is_some();
        if !proposer_valid {
            warn!(proposer = %msg.proposer, "unknown proposer");
            if let Some(ref m) = self.metrics {
                m.inc_failed_validations();
            }
            let _ = msg.response.send(false);
            return;
        }

        let expected_parent_hash = self.parent_hash(&msg.parent.1);

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

        debug!(
            payload = %msg.payload,
            round = %msg.round,
            "verifying block"
        );

        match self.payload_builder.validate_block(block_bytes).await {
            Ok(ValidationResult::Valid(meta)) => {
                // The execution layer proves the block valid on its own
                // header's parent; only consensus knows which parent it
                // chose, so the linkage is enforced here — once, for every
                // builder.
                if meta.parent_hash != expected_parent_hash {
                    warn!(
                        payload = %msg.payload,
                        block_parent = %meta.parent_hash,
                        consensus_parent = %expected_parent_hash,
                        "verify failed: block does not extend the consensus parent"
                    );
                    if let Some(ref m) = self.metrics {
                        m.inc_failed_validations();
                    }
                    let _ = msg.response.send(false);
                    return;
                }

                // The seconds field is the execution layer's to validate;
                // the invariant only consensus can check is that the
                // millisecond field agrees with it. Without this, a proposer
                // could pin timestamp_millis arbitrarily far ahead and
                // max(now_millis, parent) would ratchet it into every
                // descendant block.
                if meta.timestamp_millis / 1000 != meta.timestamp {
                    warn!(
                        payload = %msg.payload,
                        timestamp = meta.timestamp,
                        timestamp_millis = meta.timestamp_millis,
                        "verify failed: timestamp_millis don't match timestamp"
                    );
                    if let Some(ref m) = self.metrics {
                        m.inc_failed_validations();
                    }
                    let _ = msg.response.send(false);
                    return;
                }

                debug!(payload = %msg.payload, "verify succeeded");
                // Record block info for future parent lookups (critical for reth mode)
                self.record_block_info(
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
                if let Some(ref m) = self.metrics {
                    m.inc_blocks_verified();
                }
                let _ = msg.response.send(true);

                // Vote first, then speculate: this block is the parent the
                // next view will most likely build on, and if we lead that
                // view we would rather have its payload already building.
                let verified = Parent {
                    digest: msg.payload,
                    hash: meta.hash,
                    number: meta.number,
                    timestamp_millis: meta.timestamp_millis,
                };
                self.prepare_next_view(msg.round, verified).await;
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

    async fn handle_broadcast(&self, msg: Broadcast) {
        debug!(digest = %msg.digest, "broadcast request from engine");
    }

    /// The execution-layer hash of the block `digest` names.
    ///
    /// Every block we produce takes its own hash as its digest, so the two are
    /// the same value — except at genesis, whose digest is `EMPTY` while its
    /// hash comes from the chainspec. The record answers that one case; for
    /// anything else the digest is the hash, which is what lets a node that
    /// restarted with an empty map still verify what its peers propose.
    fn parent_hash(&self, digest: &AllegroDigest) -> B256 {
        self.lookup_block_info(digest)
            .map_or(digest.0, |info| info.hash)
    }

    /// Read a block's record; a poisoned lock is logged and reads as absent.
    fn lookup_block_info(&self, digest: &AllegroDigest) -> Option<BlockInfo> {
        match self.block_info.read() {
            Ok(guard) => guard.get(digest).cloned(),
            Err(e) => {
                error!(error = %e, "block_info read lock poisoned");
                None
            }
        }
    }

    /// Record a block so later views can look it up as a parent.
    fn record_block_info(&self, digest: AllegroDigest, info: BlockInfo) {
        match self.block_info.write() {
            Ok(mut guard) => {
                guard.insert(digest, info);
            }
            Err(e) => error!(error = %e, %digest, "block_info write lock poisoned"),
        }
    }

    /// Start the next view's payload if this node leads it.
    ///
    /// `parent` is the block just accepted in `round`, which the next view
    /// builds on unless it nullifies. Nothing here is load bearing: the builder
    /// only reuses a prepared payload whose parent consensus actually asked
    /// for, and builds cold otherwise.
    async fn prepare_next_view(&self, round: Round, parent: Parent) {
        let Some(schedule) = self.leader_schedule.as_ref() else {
            return;
        };
        let next = Round::new(round.epoch(), round.view().next());
        if !schedule.leads(next) {
            return;
        }

        let request = child_request(next, round.view().get(), parent, schedule.me());
        debug!(round = %next, parent = %parent.digest, "preparing payload for next view");
        self.payload_builder.prepare_payload(&request).await;
        if let Some(ref m) = self.metrics {
            m.inc_payloads_prepared();
        }
    }
}

/// What a [`BuildPayloadRequest`] needs from its parent — less than a whole
/// [`BlockInfo`], which neither path then has to clone.
#[derive(Debug, Clone, Copy)]
struct Parent {
    digest: AllegroDigest,
    hash: B256,
    number: u64,
    timestamp_millis: u64,
}

impl Parent {
    fn new(digest: AllegroDigest, info: &BlockInfo) -> Self {
        Self {
            digest,
            hash: info.hash,
            number: info.number,
            timestamp_millis: info.timestamp_millis,
        }
    }
}

/// The request for the block `round` should build on top of `parent`.
///
/// Proposing and preparing derive it identically, so a prepared payload always
/// matches what the proposal later asks for.
///
/// Milliseconds strictly increase past the parent; seconds are derived from
/// them, so `timestamp_millis / 1000 == timestamp` holds by construction
/// (verify enforces it) and seconds stay non-decreasing.
fn child_request(
    round: Round,
    parent_view: u64,
    parent: Parent,
    proposer: &PublicKey,
) -> BuildPayloadRequest {
    let now_millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let timestamp_millis = now_millis.max(parent.timestamp_millis.saturating_add(1));

    BuildPayloadRequest {
        parent_hash: parent.hash,
        parent_number: parent.number,
        parent_view,
        parent_digest: parent.digest,
        epoch: round.epoch().get(),
        view: round.view().get(),
        proposer: proposer.into(),
        timestamp: timestamp_millis / 1000,
        timestamp_millis,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_cryptography::ed25519::PrivateKey;

    fn participants(n: u64) -> Set<PublicKey> {
        Set::try_from(
            (0..n)
                .map(|seed| PrivateKey::from_seed(seed).public_key())
                .collect::<Vec<_>>(),
        )
        .expect("distinct keys")
    }

    fn round(epoch: u64, view: u64) -> Round {
        Round::new(Epoch::new(epoch), View::new(view))
    }

    fn schedule(set: &Set<PublicKey>, me: &PublicKey) -> LeaderSchedule {
        LeaderSchedule::new(RoundRobin::default(), set, me.clone())
    }

    #[test]
    fn exactly_one_validator_leads_each_round() {
        let set = participants(4);
        for view in 1..20 {
            let leaders = set
                .iter()
                .filter(|pk| schedule(&set, pk).leads(round(0, view)))
                .count();
            assert_eq!(leaders, 1, "view {view}");
        }
    }

    #[test]
    fn leader_rotates_with_epoch_and_view() {
        // Mirrors `RoundRobinElector`: index is `(epoch + view) % n` over the
        // ordered participant set. If this ever diverges, preparation would be
        // aimed at views this node does not lead.
        let set = participants(3);
        for (epoch, view) in [(0, 1), (0, 2), (0, 3), (1, 1), (2, 5)] {
            let expected = set.get(((epoch + view) % 3) as usize).expect("in range");
            assert!(
                schedule(&set, expected).leads(round(epoch, view)),
                "epoch {epoch} view {view}"
            );
        }
    }

    #[test]
    fn single_validator_leads_every_round() {
        let set = participants(1);
        let me = set.get(0).expect("one participant").clone();
        for view in 1..10 {
            assert!(schedule(&set, &me).leads(round(0, view)));
        }
    }

    #[test]
    fn a_non_validator_leads_nothing() {
        let set = participants(3);
        let outsider = PrivateKey::from_seed(99).public_key();
        for view in 1..10 {
            assert!(!schedule(&set, &outsider).leads(round(0, view)));
        }
    }

    #[test]
    fn timestamps_stay_ahead_of_the_parent() {
        // A parent stamped far in the future still yields a strictly greater
        // millisecond timestamp, with seconds derived from it.
        let parent = Parent {
            digest: AllegroDigest(B256::ZERO),
            hash: B256::ZERO,
            number: 7,
            timestamp_millis: u64::MAX - 10,
        };
        let me = PrivateKey::from_seed(0).public_key();
        let request = child_request(round(0, 2), 1, parent, &me);
        assert!(request.timestamp_millis > parent.timestamp_millis);
        assert_eq!(request.timestamp, request.timestamp_millis / 1000);
        assert_eq!(request.parent_number, 7);
    }
}
