//! Finalization forwarder: routes consensus finalization events to reth FCU.

use futures::{channel::mpsc, StreamExt};
use reth_engine_primitives::ConsensusEngineHandle;
use reth_ethereum_engine_primitives::EthEngineTypes;
use tracing::{info, warn};

use allegro_consensus::application::{with_block_info, BlockInfoMap};
use allegro_primitives::Digest as AllegroDigest;

use crate::builder::ForkchoiceTracker;

/// Spawn a background task that receives finalized block digests from the
/// consensus reporter and forwards them to reth via `fork_choice_updated`.
///
/// This is the minimal finalization bridge: for each finalized digest, look up
/// the block hash from `block_info` and submit an FCU with head=safe=finalized=hash.
///
/// **Known limitation**: if the node never called `new_payload` on the finalized
/// block (e.g. it was partitioned during verification), the FCU returns SYNCING
/// and the digest is skipped. A full block-sync backfill is out of scope for v1.
pub fn spawn_finalizer<Ctx>(
    ctx: Ctx,
    mut rx: mpsc::Receiver<AllegroDigest>,
    block_info: BlockInfoMap,
    engine: ConsensusEngineHandle<EthEngineTypes>,
    tracker: ForkchoiceTracker,
) where
    Ctx: commonware_runtime::Spawner + 'static,
{
    ctx.spawn(|_| async move {
        info!("finalizer started");
        loop {
            match rx.next().await {
                Some(digest) => {
                    // The digest names the block's execution hash directly;
                    // the record only supplies the height for the log and
                    // tracker, so a missing record cannot block finalization.
                    let hash = digest.block_hash();
                    let number =
                        with_block_info(&block_info, &digest, |info| info.number).unwrap_or_default();

                    use alloy_rpc_types_engine::ForkchoiceState;
                    let state = ForkchoiceState {
                        head_block_hash: hash,
                        safe_block_hash: hash,
                        finalized_block_hash: hash,
                    };

                    match engine.fork_choice_updated(state, None).await {
                        Ok(fcu) => {
                            if fcu.payload_status.is_valid() {
                                info!(%hash, number, "finalization FCU succeeded");
                                tracker.set_finalized(hash, number);
                            } else {
                                warn!(%hash, status = %fcu.payload_status.status, "finalization FCU returned non-valid status");
                            }
                        }
                        Err(e) => {
                            warn!(%hash, error = %e, "finalization FCU failed");
                        }
                    }
                }
                None => {
                    info!("finalizer channel closed, exiting");
                    return;
                }
            }
        }
    });
}
