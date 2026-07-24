//! Consensus metrics for block production, voting, and finalization.
//!
//! Uses atomic counters for lock-free metrics tracking.
//! Wrapped in `Arc` for shared ownership across components.

use commonware_runtime::Metrics;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Shared consensus metrics handle.
///
/// Multiple components (actor, relay, receiver) share the same
/// [`ConsensusMetrics`] instance via `Arc` to report events.
#[derive(Debug, Clone)]
pub struct ConsensusMetrics(Arc<ConsensusMetricsInner>);

#[derive(Debug)]
struct ConsensusMetricsInner {
    /// Total blocks proposed.
    pub blocks_proposed: AtomicU64,
    /// Total blocks verified.
    pub blocks_verified: AtomicU64,
    /// Total blocks finalized.
    pub blocks_finalized: AtomicU64,
    /// Total blocks notarized.
    pub blocks_notarized: AtomicU64,
    /// Total nullifications.
    pub nullifications: AtomicU64,
    /// Total view timeouts.
    pub view_timeouts: AtomicU64,
    /// Total conflicting notarizations detected.
    pub conflicting_notarizations: AtomicU64,
    /// Total conflicting finalizations detected.
    pub conflicting_finalizations: AtomicU64,
    /// Total blocks broadcast over p2p.
    pub blocks_broadcast: AtomicU64,
    /// Total blocks received from peers.
    pub blocks_received: AtomicU64,
    /// Current view number.
    pub current_view: AtomicU64,
    /// Total errors encountered.
    pub errors_total: AtomicU64,
    /// Total blocks relayed.
    pub blocks_relayed: AtomicU64,
    /// Total failed block validations.
    pub failed_validations: AtomicU64,
}

impl ConsensusMetrics {
    /// Create a new metrics instance with all counters initialized to zero.
    pub fn new() -> Self {
        Self(Arc::new(ConsensusMetricsInner {
            blocks_proposed: AtomicU64::new(0),
            blocks_verified: AtomicU64::new(0),
            blocks_finalized: AtomicU64::new(0),
            blocks_notarized: AtomicU64::new(0),
            nullifications: AtomicU64::new(0),
            view_timeouts: AtomicU64::new(0),
            conflicting_notarizations: AtomicU64::new(0),
            conflicting_finalizations: AtomicU64::new(0),
            blocks_broadcast: AtomicU64::new(0),
            blocks_received: AtomicU64::new(0),
            current_view: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            blocks_relayed: AtomicU64::new(0),
            failed_validations: AtomicU64::new(0),
        }))
    }

    /// Record a proposed block.
    pub fn inc_blocks_proposed(&self) {
        self.0.blocks_proposed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a verified block.
    pub fn inc_blocks_verified(&self) {
        self.0.blocks_verified.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a finalized block.
    pub fn inc_blocks_finalized(&self) {
        self.0.blocks_finalized.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a notarization.
    pub fn inc_blocks_notarized(&self) {
        self.0.blocks_notarized.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a nullification.
    pub fn inc_nullifications(&self) {
        self.0.nullifications.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a view timeout.
    pub fn inc_view_timeouts(&self) {
        self.0.view_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a conflicting notarization.
    pub fn inc_conflicting_notarizations(&self) {
        self.0
            .conflicting_notarizations
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a conflicting finalization.
    pub fn inc_conflicting_finalizations(&self) {
        self.0
            .conflicting_finalizations
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a block broadcast.
    pub fn inc_blocks_broadcast(&self) {
        self.0.blocks_broadcast.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a block received from a peer.
    pub fn inc_blocks_received(&self) {
        self.0.blocks_received.fetch_add(1, Ordering::Relaxed);
    }

    /// Set the current view number.
    pub fn set_current_view(&self, view: u64) {
        self.0.current_view.store(view, Ordering::Relaxed);
    }

    /// Record an error.
    pub fn inc_errors(&self) {
        self.0.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a block relay.
    pub fn inc_blocks_relayed(&self) {
        self.0.blocks_relayed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed validation.
    pub fn inc_failed_validations(&self) {
        self.0.failed_validations.fetch_add(1, Ordering::Relaxed);
    }

    // ── Accessors for test assertions ──

    /// Total blocks proposed.
    pub fn blocks_proposed(&self) -> u64 {
        self.0.blocks_proposed.load(Ordering::Relaxed)
    }

    /// Total blocks verified.
    pub fn blocks_verified(&self) -> u64 {
        self.0.blocks_verified.load(Ordering::Relaxed)
    }

    /// Total blocks finalized.
    pub fn blocks_finalized(&self) -> u64 {
        self.0.blocks_finalized.load(Ordering::Relaxed)
    }

    /// Total errors encountered.
    pub fn errors_total(&self) -> u64 {
        self.0.errors_total.load(Ordering::Relaxed)
    }

    /// Total failed validations.
    pub fn failed_validations(&self) -> u64 {
        self.0.failed_validations.load(Ordering::Relaxed)
    }
}

impl Default for ConsensusMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Metrics> From<&M> for ConsensusMetrics {
    fn from(_metrics: &M) -> Self {
        Self::new()
    }
}
