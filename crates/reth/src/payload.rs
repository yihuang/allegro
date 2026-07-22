//! Reth engine API payload builder integration.
//!
//! Provides helper functions for building a production [`EngineApiPayloadBuilder`]
//! that connects to reth's engine API.
//!
//! # Architecture
//!
//! The binary creates closures that call reth's `fork_choice_updated` /
//! `new_payload` on the engine handle, then passes those closures to
//! [`create_reth_payload_builder`]. This avoids tight coupling between
//! the consensus crate and reth's complex type system.
//!
//! # Usage (in the binary)
//!
//! ```ignore
//! use allegro_reth::{create_reth_payload_builder, build_payload_attributes_from_request};
//!
//! // Get the engine handle from reth's node builder
//! let engine_handle = node.add_ons_handle.beacon_engine_handle.clone();
//!
//! let builder = create_reth_payload_builder(
//!     move |req: BuildPayloadRequest| {
//!         let handle = engine_handle.clone();
//!         Box::pin(async move {
//!             // Build forkchoice state and attributes from the request
//!             let (fcs, attrs) = build_payload_attributes_from_request(&req);
//!
//!             // Call fork_choice_updated with attributes
//!             match handle.fork_choice_updated(fcs, Some(attrs)).await {
//!                 Ok(response) => { /* resolve payload */ },
//!                 Err(e) => Err(format!("engine error: {e}")),
//!             }
//!         })
//!     },
//!     move |req: ValidateBlockRequest| {
//!         let handle = engine_handle.clone();
//!         Box::pin(async move {
//!             // Decode block bytes and call new_payload
//!             // Return Valid or Invalid
//!         })
//!     },
//! );
//!
//! let payload_builder: Arc<dyn PayloadBuilder> = Arc::new(builder);
//! ```

use alloy_primitives::B256;
use alloy_rpc_types_engine::{
    ForkchoiceState, PayloadAttributes,
};

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Address;

    #[test]
    fn attrs_are_valid_for_cancun() {
        let attrs = build_payload_attributes(B256::ZERO, 1_000_000);

        // Shanghai+: withdrawals must be Some
        assert!(attrs.withdrawals.is_some());
        assert!(attrs.withdrawals.unwrap().is_empty());

        // Cancun+: parent_beacon_block_root must be Some
        assert!(attrs.parent_beacon_block_root.is_some());

        // Amsterdam (not activated in DEV): slot_number must be None
        assert!(attrs.slot_number.is_none());
        assert!(attrs.target_gas_limit.is_none());

        // Basic field sanity
        assert_eq!(attrs.timestamp, 1_000_000);
        assert_eq!(attrs.prev_randao, B256::ZERO);
        assert_eq!(attrs.suggested_fee_recipient, Address::ZERO);
    }
}

use allegro_consensus::executor::{
    BuildPayloadRequest, EngineApiPayloadBuilder, ValidateBlockRequest,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Create an [`EngineApiPayloadBuilder`] from async closures.
///
/// This is the production payload builder for use in a real Allegro node
/// running alongside reth. The closures are expected to call reth's engine API
/// (`fork_choice_updated` / `new_payload`).
///
/// # Generic Parameters
///
/// - `BuildFn`: Async closure that builds a block from [`BuildPayloadRequest`].
/// - `ValidateFn`: Async closure that validates a block from [`ValidateBlockRequest`].
pub fn create_reth_payload_builder<BuildFn, ValidateFn>(
    build_fn: BuildFn,
    validate_fn: ValidateFn,
) -> EngineApiPayloadBuilder
where
    BuildFn: Fn(
            BuildPayloadRequest,
        ) -> Pin<
            Box<dyn Future<Output = Result<allegro_consensus::executor::BuiltPayload, String>> + Send>,
        > + Send
        + Sync
        + 'static,
    ValidateFn: Fn(
            ValidateBlockRequest,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<allegro_consensus::executor::ValidationResult, String>>
                    + Send,
            >,
        > + Send
        + Sync
        + 'static,
{
    EngineApiPayloadBuilder::new(Arc::new(build_fn), Arc::new(validate_fn))
}

/// Build forkchoice state and payload attributes from a consensus request.
///
/// This is a convenience function for use inside the build closure.
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
pub fn build_payload_attributes(
    _parent_hash: B256,
    timestamp: u64,
) -> PayloadAttributes {
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


