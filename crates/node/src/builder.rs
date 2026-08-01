//! Production payload builder backed by reth's engine API.
//!
//! Implements [`PayloadBuilder`] over reth's [`ConsensusEngineHandle`] and
//! [`PayloadBuilderHandle`], calling `fork_choice_updated` / `resolve_kind` /
//! `new_payload`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use allegro_consensus::executor::{
    build_payload_attributes, millis_from_secs, BlockMeta, BuildPayloadRequest, BuiltPayload,
    PayloadBuilder, ValidationResult,
};
use allegro_consensus::ConsensusMetrics;
use alloy_primitives::B256;
use alloy_rlp::Decodable;
use alloy_rpc_types_engine::{ForkchoiceState, PayloadId, PayloadStatusEnum};
use parking_lot::{Mutex, RwLock};
use reth_engine_primitives::ConsensusEngineHandle;
use reth_ethereum_engine_primitives::{EthEngineTypes, EthPayloadTypes};
use reth_payload_builder::PayloadBuilderHandle;
use reth_payload_primitives::{PayloadKind, PayloadTypes};
use reth_primitives_traits::SealedBlock;
use tracing::debug;

/// Tracks the last finalized block hash and number.
///
/// Shared between the payload builder (read) and the finalizer task (write).
#[derive(Clone)]
pub struct ForkchoiceTracker(Arc<RwLock<(B256, u64)>>);

impl ForkchoiceTracker {
    /// Create a tracker with an initial finalized block (typically genesis).
    pub fn new(genesis_hash: B256) -> Self {
        Self(Arc::new(RwLock::new((genesis_hash, 0))))
    }

    /// Get the current last-finalized (hash, number).
    pub fn finalized(&self) -> (B256, u64) {
        *self.0.read()
    }

    /// Update the last-finalized block.
    pub fn set_finalized(&self, hash: B256, number: u64) {
        *self.0.write() = (hash, number);
    }
}

/// A payload job started before consensus asked for a proposal. The parent is
/// the bet: only a proposal for that same parent may use the job.
#[derive(Clone, Copy)]
struct PreparedPayload {
    parent_hash: B256,
    payload_id: PayloadId,
}

/// Holds at most one prepared job — one view is ever predicted at a time, and a
/// newer prediction supersedes an older one.
#[derive(Default)]
struct PreparedSlot(Mutex<Option<PreparedPayload>>);

impl PreparedSlot {
    fn store(&self, prepared: PreparedPayload) {
        *self.0.lock() = Some(prepared);
    }

    /// Remove the prepared job if it was built on `parent_hash`.
    ///
    /// Removed either way: on a mismatch the bet lost, and reth expires the
    /// abandoned job on its own deadline.
    fn take_for(&self, parent_hash: B256) -> Option<PreparedPayload> {
        self.0
            .lock()
            .take()
            .filter(|p| p.parent_hash == parent_hash)
    }
}

/// A [`PayloadBuilder`] that drives reth's engine API.
///
/// - **prepare**: FCU(parent, attrs) → remember payload_id (no waiting)
/// - **build**: reuse the prepared job if it matches, else FCU → resolve → new_payload (self-import)
/// - **validate**: decode → block_to_payload → new_payload
///
/// One [`Arc`] so each call clones a single refcount into its future.
pub struct EnginePayloadBuilder(Arc<Engine>);

struct Engine {
    engine: ConsensusEngineHandle<EthEngineTypes>,
    payloads: PayloadBuilderHandle<EthEngineTypes>,
    tracker: ForkchoiceTracker,
    prepared: PreparedSlot,
    metrics: Option<ConsensusMetrics>,
}

/// Create a production payload builder backed by reth's engine API.
pub fn create_engine_payload_builder(
    engine: ConsensusEngineHandle<EthEngineTypes>,
    payloads: PayloadBuilderHandle<EthEngineTypes>,
    tracker: ForkchoiceTracker,
    metrics: Option<ConsensusMetrics>,
) -> EnginePayloadBuilder {
    EnginePayloadBuilder(Arc::new(Engine {
        engine,
        payloads,
        tracker,
        prepared: PreparedSlot::default(),
        metrics,
    }))
}

impl Engine {
    /// Register a payload job on the execution layer and return its id.
    ///
    /// A parent self-imported milliseconds ago may not be visible to reth's
    /// payload-job generator yet (surfacing as "invalid payload attributes"),
    /// so retry briefly. Preparing ahead takes this retry off the critical
    /// path.
    async fn start_payload_job(&self, req: &BuildPayloadRequest) -> Result<PayloadId, String> {
        let attrs = build_payload_attributes(req.timestamp);
        let (finalized_hash, _finalized_num) = self.tracker.finalized();
        let fcs = ForkchoiceState {
            head_block_hash: req.parent_hash,
            safe_block_hash: finalized_hash,
            finalized_block_hash: finalized_hash,
        };

        let mut last_err = String::new();
        for attempt in 0..5 {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            match self
                .engine
                .fork_choice_updated(fcs, Some(attrs.clone()))
                .await
            {
                Ok(fcu) => match fcu.payload_id {
                    Some(id) => return Ok(id),
                    None => last_err = "engine returned no payload id".to_string(),
                },
                Err(e) => last_err = format!("fork_choice_updated error: {e}"),
            }
        }
        Err(last_err)
    }

    /// Collect a registered payload job and self-import the resulting block.
    ///
    /// Returns `Ok(None)` when the execution layer no longer knows the job,
    /// which is recoverable by starting a fresh one.
    async fn resolve_and_import(
        &self,
        payload_id: PayloadId,
    ) -> Result<Option<BuiltPayload>, String> {
        // 1. Resolve the built payload (wait for at least one completed build)
        //    Todo: add a timeout and fall back to Earliest on timeout
        let Some(built) = self
            .payloads
            .resolve_kind(payload_id, PayloadKind::WaitForPending)
            .await
        else {
            return Ok(None);
        };
        let built = built.map_err(|e| format!("payload build error: {e}"))?;

        // 2. Self-import the block (Ethereum CL pattern: proposer's EL also
        //    needs the block in its tree before FCU finalizes it)
        let sealed = built.block().clone();
        let exec_data = EthPayloadTypes::block_to_payload(sealed.clone(), None);
        match self
            .engine
            .new_payload(exec_data)
            .await
            .map_err(|e| format!("self-import new_payload: {e}"))?
            .status
        {
            PayloadStatusEnum::Valid | PayloadStatusEnum::Accepted => {}
            other => return Err(format!("self-import rejected ({other:?})")),
        }

        // 3. RLP-encode the block for the consensus wire format
        let block = sealed.into_block();
        Ok(Some(BuiltPayload {
            block_hash: block.header.hash_slow(),
            block_number: block.header.number,
            // Read off the block, not echoed from the request: a prepared job
            // froze its timestamp, and bookkeeping must match what verifiers
            // see.
            timestamp: block.header.timestamp,
            timestamp_millis: millis_from_secs(block.header.timestamp),
            block_bytes: alloy_rlp::encode(&block),
        }))
    }
}

impl PayloadBuilder for EnginePayloadBuilder {
    fn build_payload(
        &self,
        request: &BuildPayloadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<BuiltPayload, String>> + Send>> {
        let this = self.0.clone();
        let req = request.clone();
        Box::pin(async move {
            // A job prepared for this parent has been filling since the
            // previous view resolved: skip the forkchoice update entirely.
            if let Some(p) = this.prepared.take_for(req.parent_hash) {
                match this.resolve_and_import(p.payload_id).await {
                    Ok(Some(payload)) => {
                        debug!(
                            payload_id = %p.payload_id,
                            parent = %req.parent_hash,
                            "proposing prepared payload"
                        );
                        if let Some(ref m) = this.metrics {
                            m.inc_prepared_payload_hits();
                        }
                        return Ok(payload);
                    }
                    // The job outlived its deadline and reth dropped it.
                    // Nothing is lost: build cold below.
                    Ok(None) => debug!(
                        payload_id = %p.payload_id,
                        "prepared payload job expired; building from cold"
                    ),
                    Err(e) => debug!(
                        payload_id = %p.payload_id,
                        error = %e,
                        "prepared payload failed; building from cold"
                    ),
                }
            }

            if let Some(ref m) = this.metrics {
                m.inc_prepared_payload_misses();
            }
            let payload_id = this.start_payload_job(&req).await?;
            this.resolve_and_import(payload_id)
                .await?
                .ok_or_else(|| "payload job not found".to_string())
        })
    }

    fn validate_block(
        &self,
        block_bytes: Vec<u8>,
        _parent_hash: B256,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult, String>> + Send>> {
        let this = self.0.clone();
        Box::pin(async move {
            // 1. Decode the standard Ethereum block from the wire
            use reth_ethereum_primitives::Block as EthBlock;
            let block: EthBlock =
                Decodable::decode(&mut &block_bytes[..]).map_err(|e| format!("rlp decode: {e}"))?;

            // 2. Hash, seal, and convert to engine execution data
            let hash = block.header.hash_slow();
            let number = block.header.number;
            let timestamp = block.header.timestamp;
            let sealed = SealedBlock::seal_slow(block);
            let exec_data = EthPayloadTypes::block_to_payload(sealed, None);

            // 3. Submit to the engine for validation
            let payload_status = this
                .engine
                .new_payload(exec_data)
                .await
                .map_err(|e| format!("engine new_payload error: {e}"))?;

            match payload_status.status {
                PayloadStatusEnum::Valid => {
                    // In reth mode the on-chain block is a standard Ethereum
                    // block (EthBlock) with no AllegroHeader timestamp_millis,
                    // so derive a deterministic value from the seconds
                    // timestamp. This keeps the BlockInfo bookkeeping identical
                    // on every node — the proposer must not record a wall-clock
                    // value its verifiers can't reproduce.
                    Ok(ValidationResult::Valid(BlockMeta {
                        hash,
                        number,
                        timestamp,
                        timestamp_millis: millis_from_secs(timestamp),
                    }))
                }
                PayloadStatusEnum::Invalid { validation_error } => {
                    Ok(ValidationResult::Invalid(validation_error))
                }
                PayloadStatusEnum::Syncing | PayloadStatusEnum::Accepted => {
                    Err("parent unknown to execution layer (syncing)".to_string())
                }
            }
        })
    }

    fn prepare_payload(
        &self,
        request: &BuildPayloadRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let this = self.0.clone();
        let req = request.clone();
        Box::pin(async move {
            // Best effort: a failure here only costs the head start, and
            // `build_payload` still has the full cold path.
            match this.start_payload_job(&req).await {
                Ok(payload_id) => {
                    debug!(%payload_id, parent = %req.parent_hash, "prepared payload job");
                    this.prepared.store(PreparedPayload {
                        parent_hash: req.parent_hash,
                        payload_id,
                    });
                }
                Err(e) => {
                    debug!(error = %e, parent = %req.parent_hash, "failed to prepare payload")
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared(parent: u8) -> PreparedPayload {
        PreparedPayload {
            parent_hash: B256::repeat_byte(parent),
            payload_id: PayloadId::new([parent; 8]),
        }
    }

    #[test]
    fn prepared_payload_is_reused_for_its_own_parent() {
        let slot = PreparedSlot::default();
        slot.store(prepared(1));
        assert!(slot.take_for(B256::repeat_byte(1)).is_some());
    }

    #[test]
    fn prepared_payload_for_another_parent_is_discarded() {
        // The bet lost: the proposal names a parent the job was not built on,
        // so the job must not be proposed — and must not linger to be picked
        // up by a later proposal either.
        let slot = PreparedSlot::default();
        slot.store(prepared(1));
        assert!(slot.take_for(B256::repeat_byte(2)).is_none());
        assert!(slot.take_for(B256::repeat_byte(1)).is_none());
    }

    #[test]
    fn a_prepared_payload_is_used_at_most_once() {
        let slot = PreparedSlot::default();
        slot.store(prepared(1));
        assert!(slot.take_for(B256::repeat_byte(1)).is_some());
        assert!(slot.take_for(B256::repeat_byte(1)).is_none());
    }

    #[test]
    fn a_newer_prediction_supersedes_an_older_one() {
        let slot = PreparedSlot::default();
        slot.store(prepared(1));
        slot.store(prepared(2));
        assert!(slot.take_for(B256::repeat_byte(1)).is_none());
    }
}
