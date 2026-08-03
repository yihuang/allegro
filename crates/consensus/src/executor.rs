//! Payload builder abstraction for block construction and validation.
//!
//! Follows Tempo's architecture where block building is delegated to the
//! execution layer: [`PayloadBuilder`] is implemented over reth's engine API.

use std::future::Future;
use std::pin::Pin;

use alloy_primitives::B256;
use alloy_rpc_types_engine::PayloadAttributes;

use allegro_primitives::{Digest as AllegroDigest, ProposerKey};

// ── Types ───────────────────────────────────────────────────

/// The millisecond timestamp consensus records for a block carrying `secs`.
///
/// Proposer and verifiers must derive the same value from the same block, so
/// there is one definition.
pub const fn millis_from_secs(secs: u64) -> u64 {
    secs.saturating_mul(1000)
}

/// The seconds field a block carries for a given millisecond timestamp — the
/// inverse rounding of [`millis_from_secs`], shared for the same reason.
pub const fn secs_from_millis(millis: u64) -> u64 {
    millis / 1000
}

/// A block built by the payload builder.
#[derive(Debug, Clone)]
pub struct BuiltPayload {
    /// RLP-encoded full block (header + body).
    pub block_bytes: Vec<u8>,
    /// Block hash (keccak256 of RLP-encoded header).
    pub block_hash: B256,
    /// Block number.
    pub block_number: u64,
    /// Seconds timestamp the block actually carries, which a prepared payload
    /// froze when its job started — so not necessarily the requested one.
    pub timestamp: u64,
    /// Millisecond timestamp recorded in consensus bookkeeping. Deterministic:
    /// every node derives the same value for the same block.
    pub timestamp_millis: u64,
}

/// Metadata about a valid block returned from validation.
#[derive(Debug, Clone, Copy)]
pub struct BlockMeta {
    /// Block hash (keccak256 of RLP-encoded header).
    pub hash: B256,
    /// Parent hash the block's header names. Consensus compares it against
    /// the parent it chose — reported rather than checked here, so no
    /// implementation can forget a check it never owned.
    pub parent_hash: B256,
    /// Block number.
    pub number: u64,
    /// Block timestamp (seconds since UNIX epoch).
    pub timestamp: u64,
    /// Millisecond-precision timestamp, deterministic per block.
    pub timestamp_millis: u64,
}

/// Result of block validation.
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// Block is valid and carries metadata.
    Valid(BlockMeta),
    /// Block is invalid with a reason.
    Invalid(String),
}

// ── PayloadBuilder trait ────────────────────────────────────

/// Abstract interface for building and validating blocks.
///
/// Backed by reth's engine API in production; the consensus integration tests
/// supply their own empty-block implementation.
pub trait PayloadBuilder: Send + Sync {
    /// Build a block payload.
    ///
    /// Called when this node is the leader and needs to propose a block.
    /// Should construct a block from the mempool and return the RLP-encoded bytes.
    fn build_payload(
        &self,
        request: &BuildPayloadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<BuiltPayload, String>> + Send>>;

    /// Validate a block.
    ///
    /// Called when this node receives a proposed block from another validator.
    /// Should execute the block and verify the state root. Consensus-level
    /// invariants — the parent linkage, the millisecond timestamp — are
    /// checked by the caller against the reported [`BlockMeta`].
    fn validate_block(
        &self,
        block_bytes: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult, String>> + Send>>;

    /// Start building a payload before consensus asks for it, on the block
    /// this node is predicted to build on next.
    ///
    /// A later [`build_payload`](Self::build_payload) for the same parent may
    /// reuse the work; one for a different parent must not. Best-effort: the
    /// default does nothing, and failing only costs the head start.
    fn prepare_payload(
        &self,
        request: &BuildPayloadRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let _ = request;
        Box::pin(async {})
    }
}

// ── EngineApiPayloadBuilder ────────────────────────────────

/// Build payload request parameters.
#[derive(Debug, Clone)]
pub struct BuildPayloadRequest {
    pub parent_hash: B256,
    pub parent_number: u64,
    pub parent_view: u64,
    pub parent_digest: AllegroDigest,
    pub epoch: u64,
    pub view: u64,
    pub proposer: ProposerKey,
    /// Block timestamp (seconds since UNIX epoch) — Ethereum standard field.
    pub timestamp: u64,
    /// Millisecond-precision timestamp — the proposer guarantees it is
    /// > the parent's `timestamp_millis`.
    pub timestamp_millis: u64,
}

/// Build payload attributes for a new block.
///
/// Returns attributes valid for Cancun+ (DEV chainspec):
/// - `withdrawals: Some(vec![])` for Shanghai+
/// - `parent_beacon_block_root: Some(ZERO)` for Cancun+
/// - `slot_number: None` / `target_gas_limit: None` since Amsterdam not activated.
pub fn build_payload_attributes(timestamp: u64) -> PayloadAttributes {
    PayloadAttributes {
        timestamp,
        prev_randao: B256::ZERO,
        suggested_fee_recipient: alloy_primitives::Address::ZERO,
        withdrawals: Some(vec![]),
        parent_beacon_block_root: Some(B256::ZERO),
        slot_number: None,
        target_gas_limit: None,
    }
}

#[cfg(test)]
mod executor_tests {
    use super::*;

    #[test]
    fn attrs_are_valid_for_cancun() {
        let attrs = build_payload_attributes(1_000_000);
        assert!(attrs.withdrawals.is_some());
        assert!(attrs.withdrawals.unwrap().is_empty());
        assert!(attrs.parent_beacon_block_root.is_some());
        assert!(attrs.slot_number.is_none());
        assert!(attrs.target_gas_limit.is_none());
        assert_eq!(attrs.timestamp, 1_000_000);
        assert_eq!(attrs.prev_randao, B256::ZERO);
        assert_eq!(
            attrs.suggested_fee_recipient,
            alloy_primitives::Address::ZERO
        );
    }
}
