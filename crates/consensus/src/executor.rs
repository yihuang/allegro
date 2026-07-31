//! Payload builder abstraction for block construction and validation.
//!
//! Follows Tempo's architecture where block building is delegated to the
//! execution layer: [`PayloadBuilder`] is implemented over reth's engine API.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use alloy_primitives::B256;
use alloy_rpc_types_engine::{ForkchoiceState, PayloadAttributes};

use allegro_primitives::Digest as AllegroDigest;

// ── Types ───────────────────────────────────────────────────

/// A block built by the payload builder.
#[derive(Debug, Clone)]
pub struct BuiltPayload {
    /// RLP-encoded full block (header + body).
    pub block_bytes: Vec<u8>,
    /// Block hash (keccak256 of RLP-encoded header).
    pub block_hash: B256,
    /// Block number.
    pub block_number: u64,
    /// Millisecond timestamp recorded in consensus bookkeeping. Deterministic:
    /// every node derives the same value for the same block.
    pub timestamp_millis: u64,
}

/// Metadata about a valid block returned from validation.
#[derive(Debug, Clone, Copy)]
pub struct BlockMeta {
    /// Block hash (keccak256 of RLP-encoded header).
    pub hash: B256,
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
    /// Should execute the block and verify the state root.
    fn validate_block(
        &self,
        block_bytes: Vec<u8>,
        parent_hash: B256,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult, String>> + Send>>;
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
    pub proposer: [u8; 32],
    /// Block timestamp (seconds since UNIX epoch) — Ethereum standard field.
    pub timestamp: u64,
    /// Millisecond-precision timestamp — the proposer guarantees it is
    /// > the parent's `timestamp_millis`.
    pub timestamp_millis: u64,
}

/// Validate block request parameters.
#[derive(Debug, Clone)]
pub struct ValidateBlockRequest {
    pub block_bytes: Vec<u8>,
    pub parent_hash: B256,
}

// ── Closure type aliases for EngineApiPayloadBuilder ───────

/// Async closure that builds a payload.
type BuildPayloadFn = Arc<
    dyn Fn(
            BuildPayloadRequest,
        ) -> Pin<Box<dyn Future<Output = Result<BuiltPayload, String>> + Send>>
        + Send
        + Sync,
>;

/// Async closure that validates a block.
type ValidateBlockFn = Arc<
    dyn Fn(
            ValidateBlockRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ValidationResult, String>> + Send>>
        + Send
        + Sync,
>;

/// A payload builder backed by boxed async closures.
///
/// This allows the binary to wire in reth's engine API without the consensus
/// crate depending on reth directly. The binary creates the closures that
/// call `fork_choice_updated` / `new_payload` on the engine handle.
///
/// # Example (in the binary)
///
/// ```ignore
/// use allegro_consensus::executor::{EngineApiPayloadBuilder, BuildPayloadRequest, ValidateBlockRequest, BuiltPayload, ValidationResult};
///
/// let engine_handle = node.add_ons_handle.beacon_engine_handle.clone();
/// let chain_spec = node.chain_spec();
///
/// let builder = EngineApiPayloadBuilder::new(
///     Arc::new(move |req: BuildPayloadRequest| {
///         let handle = engine_handle.clone();
///         Box::pin(async move {
///             // 1. Build payload attributes from request
///             // 2. Call fork_choice_updated with attributes
///             // 3. Resolve the built payload
///             // 4. Return BuiltPayload
///             todo!()
///         })
///     }),
///     Arc::new(move |req: ValidateBlockRequest| {
///         let handle = engine_handle.clone();
///         Box::pin(async move {
///             // 1. Decode block from req.block_bytes
///             // 2. Call new_payload on engine handle
///             // 3. Return Valid / Invalid
///             todo!()
///         })
///     }),
/// );
/// ```
pub struct EngineApiPayloadBuilder {
    build_fn: BuildPayloadFn,
    validate_fn: ValidateBlockFn,
}

impl EngineApiPayloadBuilder {
    /// Create a new engine API payload builder with the given async closures.
    pub fn new(build_fn: BuildPayloadFn, validate_fn: ValidateBlockFn) -> Self {
        Self {
            build_fn,
            validate_fn,
        }
    }
}

impl PayloadBuilder for EngineApiPayloadBuilder {
    fn build_payload(
        &self,
        request: &BuildPayloadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<BuiltPayload, String>> + Send>> {
        (self.build_fn)(request.clone())
    }

    fn validate_block(
        &self,
        block_bytes: Vec<u8>,
        parent_hash: B256,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult, String>> + Send>> {
        let req = ValidateBlockRequest {
            block_bytes,
            parent_hash,
        };
        (self.validate_fn)(req)
    }
}

// ── Engine API helpers ─────────────────────────────────────

/// Create an [`EngineApiPayloadBuilder`] from async closures.
pub fn create_reth_payload_builder<BuildFn, ValidateFn>(
    build_fn: BuildFn,
    validate_fn: ValidateFn,
) -> EngineApiPayloadBuilder
where
    BuildFn: Fn(
            BuildPayloadRequest,
        ) -> Pin<Box<dyn Future<Output = Result<BuiltPayload, String>> + Send>>
        + Send
        + Sync
        + 'static,
    ValidateFn: Fn(
            ValidateBlockRequest,
        ) -> Pin<Box<dyn Future<Output = Result<ValidationResult, String>> + Send>>
        + Send
        + Sync
        + 'static,
{
    EngineApiPayloadBuilder::new(Arc::new(build_fn), Arc::new(validate_fn))
}

/// Build forkchoice state and payload attributes from a consensus request.
pub fn build_payload_attributes_from_request(
    req: &BuildPayloadRequest,
) -> (ForkchoiceState, PayloadAttributes) {
    let forkchoice_state = ForkchoiceState {
        head_block_hash: req.parent_hash,
        safe_block_hash: req.parent_hash,
        finalized_block_hash: req.parent_hash,
    };
    let pay_attrs = build_payload_attributes(req.parent_hash, req.timestamp);
    (forkchoice_state, pay_attrs)
}

/// Build payload attributes for a new block.
///
/// Returns attributes valid for Cancun+ (DEV chainspec):
/// - `withdrawals: Some(vec![])` for Shanghai+
/// - `parent_beacon_block_root: Some(ZERO)` for Cancun+
/// - `slot_number: None` / `target_gas_limit: None` since Amsterdam not activated.
pub fn build_payload_attributes(_parent_hash: B256, timestamp: u64) -> PayloadAttributes {
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
        let attrs = build_payload_attributes(B256::ZERO, 1_000_000);
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
