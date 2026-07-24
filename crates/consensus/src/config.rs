//! Consensus engine configuration with sensible production defaults.
//!
//! Follows Tempo's approach: all consensus parameters have sensible defaults,
//! are configurable via CLI args, and are validated at startup.

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

    /// Number of concurrent block fetches.
    pub fetch_concurrent: usize,

    /// Activity timeout in views — how many views without activity before
    /// we consider the chain stalled.
    pub activity_timeout: u64,

    /// Number of views to skip before switching leaders on inactivity.
    pub skip_timeout: u64,

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
pub enum ForwardingPolicy {
    /// Only forward to silent voters who haven't acknowledged the proposal.
    #[default]
    SilentVoters,
    /// Forward to all validators (maps to SilentVoters on commonware).
    All,
}


impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            mailbox_size: 1024,
            leader_timeout: Duration::from_secs(2),
            certification_timeout: Duration::from_secs(4),
            timeout_retry: Duration::from_secs(1),
            fetch_timeout: Duration::from_secs(2),
            fetch_concurrent: 4,
            activity_timeout: 10,
            skip_timeout: 5,
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
