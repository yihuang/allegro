//! Production payload builder backed by reth's engine API.
//!
//! Constructs [`EngineApiPayloadBuilder`] closures that call
//! `fork_choice_updated` / `resolve_kind` / `new_payload` on reth's
//! [`ConsensusEngineHandle`] and [`PayloadBuilderHandle`].

use std::sync::Arc;

use allegro_consensus::executor::{
    build_payload_attributes_from_request, BlockMeta, BuildPayloadRequest, BuiltPayload,
    EngineApiPayloadBuilder, ValidateBlockRequest, ValidationResult,
};
use alloy_primitives::B256;
use alloy_rlp::Decodable;
use alloy_rpc_types_engine::ForkchoiceState;
use parking_lot::RwLock;
use reth_engine_primitives::ConsensusEngineHandle;
use reth_ethereum_engine_primitives::{EthEngineTypes, EthPayloadTypes};
use reth_payload_builder::PayloadBuilderHandle;
use reth_payload_primitives::{PayloadKind, PayloadTypes};
use reth_primitives_traits::SealedBlock;

/// Tracks the last finalized block hash and number.
///
/// Shared between the payload-builder closures (read) and the finalizer task (write).
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

/// Create a production [`PayloadBuilder`] backed by reth's engine API.
///
/// Two closures are wired:
/// - **build**:  FCU(parent, attrs) → payload_id → resolve → new_payload (self-import)
/// - **validate**: decode → block_to_payload → new_payload
pub fn create_engine_payload_builder(
    engine: ConsensusEngineHandle<EthEngineTypes>,
    payloads: PayloadBuilderHandle<EthEngineTypes>,
    tracker: ForkchoiceTracker,
) -> EngineApiPayloadBuilder {
    let engine = Arc::new(engine);
    let payloads = Arc::new(payloads);

    let engine_b = engine.clone();
    let payloads_b = payloads.clone();

    EngineApiPayloadBuilder::new(
        // ═══════════════════════════════════════════════════════
        //  Build closure (called when this node is the leader)
        // ═══════════════════════════════════════════════════════
        Arc::new(move |req: BuildPayloadRequest| {
            let engine = engine_b.clone();
            let payloads = payloads_b.clone();
            let tracker = tracker.clone();

            Box::pin(async move {
                let (_fcs, attrs) = build_payload_attributes_from_request(&req);
                let (finalized_hash, _finalized_num) = tracker.finalized();

                let fcs = ForkchoiceState {
                    head_block_hash: req.parent_hash,
                    safe_block_hash: finalized_hash,
                    finalized_block_hash: finalized_hash,
                };

                // 1. Forkchoice update with payload attributes.
                //
                // The parent may have been self-imported milliseconds ago and
                // not yet be visible to reth's payload-job generator (it then
                // fails to start the job, surfacing as "invalid payload
                // attributes"), so retry briefly before giving up.
                let mut payload_id = None;
                let mut last_err = String::new();
                for attempt in 0..5 {
                    if attempt > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    match engine.fork_choice_updated(fcs, Some(attrs.clone())).await {
                        Ok(fcu) => match fcu.payload_id {
                            Some(id) => {
                                payload_id = Some(id);
                                break;
                            }
                            None => last_err = "engine returned no payload id".to_string(),
                        },
                        Err(e) => last_err = format!("fork_choice_updated error: {e}"),
                    }
                }
                let payload_id = payload_id.ok_or(last_err)?;

                // 2. Resolve the built payload (wait for at least one completed build)
                //    Todo: add a timeout and fall back to Earliest on timeout
                let built = payloads
                    .resolve_kind(payload_id, PayloadKind::WaitForPending)
                    .await
                    .ok_or_else(|| "payload job not found".to_string())?
                    .map_err(|e| format!("payload build error: {e}"))?;

                // 3. Self-import the block (Ethereum CL pattern: proposer's EL
                //    also needs the block in its tree before FCU finalizes it)
                let sealed = built.block().clone();
                let exec_data = EthPayloadTypes::block_to_payload(sealed.clone(), None);
                match engine
                    .new_payload(exec_data)
                    .await
                    .map_err(|e| format!("self-import new_payload: {e}"))?
                    .status
                {
                    alloy_rpc_types_engine::PayloadStatusEnum::Valid
                    | alloy_rpc_types_engine::PayloadStatusEnum::Accepted => {}
                    other => {
                        return Err(format!("self-import rejected ({:?})", other));
                    }
                }

                // 4. RLP-encode the block for the consensus wire format
                let block = sealed.into_block();
                let block_hash = block.header.hash_slow();
                let block_number = block.header.number;
                let block_bytes = alloy_rlp::encode(&block);

                Ok(BuiltPayload {
                    block_bytes,
                    block_hash,
                    block_number,
                })
            })
        }),
        // ═══════════════════════════════════════════════════════
        //  Validate closure (called for every received proposal)
        // ═══════════════════════════════════════════════════════
        Arc::new(move |req: ValidateBlockRequest| {
            let engine = engine.clone();
            Box::pin(async move {
                // 1. Decode the standard Ethereum block from the wire
                use reth_ethereum_primitives::Block as EthBlock;
                let block: EthBlock = Decodable::decode(&mut &req.block_bytes[..])
                    .map_err(|e| format!("rlp decode: {e}"))?;

                // 2. Hash, seal, and convert to engine execution data
                let hash = block.header.hash_slow();
                let number = block.header.number;
                let timestamp = block.header.timestamp;
                let sealed = SealedBlock::seal_slow(block);
                let exec_data = EthPayloadTypes::block_to_payload(sealed, None);

                // 3. Submit to the engine for validation
                let payload_status = engine
                    .new_payload(exec_data)
                    .await
                    .map_err(|e| format!("engine new_payload error: {e}"))?;

                match payload_status.status {
                    alloy_rpc_types_engine::PayloadStatusEnum::Valid => {
                        Ok(ValidationResult::Valid(BlockMeta {
                            hash,
                            number,
                            timestamp,
                        }))
                    }
                    alloy_rpc_types_engine::PayloadStatusEnum::Invalid { validation_error } => {
                        Ok(ValidationResult::Invalid(validation_error))
                    }
                    alloy_rpc_types_engine::PayloadStatusEnum::Syncing
                    | alloy_rpc_types_engine::PayloadStatusEnum::Accepted => {
                        Err("parent unknown to execution layer (syncing)".to_string())
                    }
                }
            })
        }),
    )
}
