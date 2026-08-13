//! Consensus engine configuration with sensible production defaults.
//!
//! Follows Tempo's approach: all consensus parameters have sensible defaults,
//! are configurable via CLI args, and are validated at startup.

use crate::error::ConsensusError;
use std::time::Duration;

/// Consensus engine configuration.
#[derive(Debug, Clone)]
pub struct ConsensusConfig {
    /// P2P mailbox size (message backlog per channel).
    pub mailbox_size: usize,

    /// Maximum time to wait for the leader's proposal before timing out.
    pub leader_timeout: Duration,

    /// Maximum time to wait for a certification quorum.
    pub certification_timeout: Duration,

    /// Timeout retry interval for view timeouts.
    pub timeout_retry: Duration,

    /// Timeout for fetching blocks from peers.
    pub fetch_timeout: Duration,

    /// Number of views behind the finalized tip to retain, in memory and in
    /// the journal, for recent activity.
    pub view_retention: u64,

    /// How long the selected leader may stay inactive, while a quorum of
    /// participants is active, before we nullify the view. Must be greater
    /// than both `certification_timeout` and `timeout_retry`.
    pub skip_timeout: Duration,

    /// Number of consecutive views a single leader serves (a *term*).
    ///
    /// `1` elects a new leader every view (the classic rotation). Anything
    /// greater enables stable leaders, which is **consensus-critical**: every
    /// validator must configure the same value, and mismatches produce silent
    /// disagreement on view transitions with no fault evidence. Only change it
    /// with the whole validator set, at an epoch boundary.
    pub term_length: u32,

    /// How long an entered view may stay unfinalized before this node abandons
    /// the term and nullifies, evicting a leader that keeps every per-view
    /// timer satisfied without producing finality.
    ///
    /// Local policy — only read when `term_length > 1`. Must be greater than
    /// `certification_timeout`.
    pub stall_timeout: Duration,

    /// How many views ahead of certified ancestry a participant may verify
    /// proposals and broadcast notarize votes, within a term.
    ///
    /// Trades memory (the voter tracks a round per optimistic view) for view
    /// latency that follows proposal propagation instead of certification.
    /// Local policy — mismatches degrade the optimization but never safety.
    /// `0` disables it, as does `term_length == 1`.
    pub optimistic_views: u64,

    /// Retain each individual vote until its round is pruned, rather than
    /// releasing evidence once the certificate is built. Makes conflict
    /// reporting and peer blocking reliable at the cost of memory.
    pub track_historical_votes: bool,

    /// Forwarding policy for block proposals.
    pub forwarding_policy: ForwardingPolicy,

    /// Replay buffer size for consensus messages (bytes).
    pub replay_buffer_size: usize,

    /// Write buffer size for storage (bytes).
    pub write_buffer_size: usize,

    /// Buffer pool page size.
    pub page_cache_pages: u16,

    /// Buffer pool capacity.
    pub page_cache_capacity: usize,

    /// Partition name for on-disk isolation.
    pub partition: String,

    /// Whether to enable strict startup (require finalization archive).
    pub strict_startup: bool,
}

/// How the engine forwards block proposals to validators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ForwardingPolicy {
    /// Only forward to silent voters who haven't acknowledged the proposal.
    #[default]
    SilentVoters,
    /// Forward to all validators (maps to SilentVoters on commonware).
    All,
}

impl ConsensusConfig {
    /// Check the invariants the simplex engine relies on.
    ///
    /// Commonware asserts these itself, but only inside `Engine::new`, which
    /// runs on the consensus thread under a panic-catching runtime — a bad
    /// value there takes down consensus while the node keeps running. Checking
    /// here turns that into a startup error on the main thread.
    pub fn validate(&self) -> Result<(), ConsensusError> {
        let bad = |msg: String| Err(ConsensusError::Config(msg));

        if self.mailbox_size == 0 {
            return bad("mailbox size must be greater than zero".into());
        }
        if self.leader_timeout.is_zero() {
            return bad("leader timeout must be greater than zero".into());
        }
        if self.timeout_retry.is_zero() {
            return bad("timeout retry must be greater than zero".into());
        }
        if self.fetch_timeout.is_zero() {
            return bad("fetch timeout must be greater than zero".into());
        }
        if self.view_retention == 0 {
            return bad("view retention must be greater than zero".into());
        }
        if self.term_length == 0 {
            return bad("term length must be at least 1".into());
        }
        if self.certification_timeout <= self.leader_timeout {
            return bad(format!(
                "certification timeout ({:?}) must exceed leader timeout ({:?})",
                self.certification_timeout, self.leader_timeout,
            ));
        }
        if self.skip_timeout <= self.certification_timeout {
            return bad(format!(
                "skip timeout ({:?}) must exceed certification timeout ({:?})",
                self.skip_timeout, self.certification_timeout,
            ));
        }
        if self.skip_timeout <= self.timeout_retry {
            return bad(format!(
                "skip timeout ({:?}) must exceed timeout retry ({:?})",
                self.skip_timeout, self.timeout_retry,
            ));
        }
        // Only read when the elector hands out multi-view terms.
        if self.term_length > 1 && self.stall_timeout <= self.certification_timeout {
            return bad(format!(
                "stall timeout ({:?}) must exceed certification timeout ({:?})",
                self.stall_timeout, self.certification_timeout,
            ));
        }
        Ok(())
    }
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            mailbox_size: 1024,
            leader_timeout: Duration::from_secs(2),
            certification_timeout: Duration::from_secs(4),
            timeout_retry: Duration::from_secs(1),
            fetch_timeout: Duration::from_secs(2),
            view_retention: 10,
            skip_timeout: Duration::from_secs(5),
            term_length: 1,
            stall_timeout: Duration::from_secs(8),
            optimistic_views: 0,
            track_historical_votes: false,
            forwarding_policy: ForwardingPolicy::SilentVoters,
            replay_buffer_size: 8 * 1024 * 1024, // 8 MB
            write_buffer_size: 1024 * 1024,      // 1 MB
            page_cache_pages: 4096,
            page_cache_capacity: 8192,
            partition: "allegro".into(),
            strict_startup: false,
        }
    }
}

/// P2P network configuration.
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Maximum message size in bytes.
    pub max_message_size: u32,

    /// P2P mailbox size.
    pub mailbox_size: usize,

    /// Number of tracked peer sets.
    pub tracked_peer_sets: usize,

    /// Synchrony bound for network assumptions.
    pub synchrony_bound: Duration,

    /// Peer dial frequency.
    pub dial_frequency: Duration,

    /// Maximum handshake age before peer is considered stale.
    pub max_handshake_age: Duration,

    /// Handshake timeout.
    pub handshake_timeout: Duration,

    /// Maximum concurrent handshakes.
    pub max_concurrent_handshakes: u32,

    /// Duration to block a byzantine peer.
    pub block_duration: Duration,

    /// Peer ping frequency.
    pub ping_frequency: Duration,

    /// Cooldown between connection attempts to the same peer.
    pub peer_connection_cooldown: Duration,

    /// Rate limit for handshakes per IP.
    pub handshake_rate_per_ip: u32,

    /// Rate limit for handshakes per subnet.
    pub handshake_rate_per_subnet: u32,

    /// Whether to bypass IP checks (dev/test only).
    pub bypass_ip_check: bool,

    /// Whether to allow private IPs.
    pub allow_private_ips: bool,

    /// Whether to allow DNS peer addresses.
    pub allow_dns: bool,

    /// Send batch size for p2p messages.
    pub send_batch_size: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            max_message_size: 1024 * 1024,
            mailbox_size: 1024,
            tracked_peer_sets: 3,
            synchrony_bound: Duration::from_secs(2),
            dial_frequency: Duration::from_millis(200),
            max_handshake_age: Duration::from_secs(300),
            handshake_timeout: Duration::from_secs(5),
            max_concurrent_handshakes: 128,
            block_duration: Duration::from_secs(60),
            ping_frequency: Duration::from_secs(10),
            peer_connection_cooldown: Duration::from_secs(5),
            handshake_rate_per_ip: 10,
            handshake_rate_per_subnet: 10,
            bypass_ip_check: false,
            allow_private_ips: true,
            allow_dns: false,
            send_batch_size: 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        assert!(ConsensusConfig::default().validate().is_ok());
    }

    #[test]
    fn rejects_ordering_violations() {
        // A stale `ALLEGRO_SKIP_TIMEOUT=5` from when the option counted views
        // now parses as 5ms, well under the certification timeout.
        let cfg = ConsensusConfig {
            skip_timeout: Duration::from_millis(5),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());

        let cfg = ConsensusConfig {
            certification_timeout: Duration::from_secs(2),
            leader_timeout: Duration::from_secs(2),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());

        let cfg = ConsensusConfig {
            timeout_retry: Duration::from_secs(9),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn stall_timeout_only_checked_for_multi_view_terms() {
        let short_stall = ConsensusConfig {
            stall_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        assert!(short_stall.validate().is_ok(), "unused at term_length 1");

        let cfg = ConsensusConfig {
            term_length: 4,
            ..short_stall
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn rejects_zero_values() {
        for cfg in [
            ConsensusConfig {
                mailbox_size: 0,
                ..Default::default()
            },
            ConsensusConfig {
                view_retention: 0,
                ..Default::default()
            },
            ConsensusConfig {
                fetch_timeout: Duration::ZERO,
                ..Default::default()
            },
            ConsensusConfig {
                term_length: 0,
                ..Default::default()
            },
        ] {
            assert!(cfg.validate().is_err());
        }
    }
}
