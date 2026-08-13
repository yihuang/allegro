//! The Execution Extension that keeps [`Store`] in step with the canonical chain.
//!
//! Three rules, each a place a hand-rolled ExEx goes wrong:
//!
//! 1. Read reorgs through `reverted_chain()`/`committed_chain()`, never by matching the
//!    three variants. `ChainReorged` carries *both* chains, and code that matches by hand
//!    tends to log it -- keeping the orphans and missing their replacements.
//! 2. Send `FinishedHeight` only after the write commits. It lets reth prune its WAL, so
//!    sending it early turns a retryable error into silent, permanent loss.
//! 3. Resume with a head ([`resume_head`]), or reth never backfills what was missed.

// `Transaction` for `to()`, `Typed2718` for `ty()`.
use alloy_consensus::{BlockHeader as _, Transaction as _, Typed2718 as _};
use futures::StreamExt;
use reth_ethereum::exex::{ExExContext, ExExEvent, ExExHead, ExExNotification};
use reth_ethereum::provider::BlockHashReader;
use reth_ethereum::EthPrimitives;
use reth_execution_types::Chain;
use reth_node_api::{FullNodeComponents, NodeTypes};
use tracing::{debug, info};

use crate::store::{IndexedTx, Position, Store, Tip};

/// Flatten a chain's blocks into index rows.
///
/// Concrete in [`EthPrimitives`] rather than generic over `NodePrimitives`: the generic
/// form needs a pile of bounds to say "transactions have a hash and a type", and allegro
/// is only ever an Ethereum-primitives node.
fn rows_of(chain: &Chain<EthPrimitives>) -> Vec<IndexedTx> {
    let mut rows = Vec::new();
    for block in chain.blocks_iter() {
        let block_num = block.num_hash().number;
        // Senders were recovered during execution, so pairing them off the block here
        // never re-derives a signature.
        for (tx_index, (sender, tx)) in block.transactions_with_sender().enumerate() {
            rows.push(IndexedTx {
                position: Position::new(block_num, tx_index as u32),
                hash: *tx.tx_hash(),
                from: *sender,
                to: tx.to(),
                tx_type: tx.ty(),
            });
        }
    }
    rows
}

/// Everything one notification means for the index.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Plan {
    /// Drop this block and everything above it.
    pub revert_from: Option<u64>,
    /// Rows to insert, in chain order.
    pub rows: Vec<IndexedTx>,
    /// Where the index stands afterwards -- the resume head after a restart.
    pub tip: Option<Tip>,
    /// The committed tip, set only when blocks were committed. `None` for a pure revert,
    /// which is what stops one from acknowledging a height (rule 2).
    pub committed: Option<Tip>,
}

fn tip_of(chain: &Chain<EthPrimitives>) -> Tip {
    let num_hash = chain.tip().num_hash();
    Tip::new(num_hash.number, num_hash.hash)
}

/// Translate one notification into a single atomic [`Store::apply`].
///
/// The only place a notification is read: [`run`] consumes this and never looks again,
/// so the reorg semantics stay testable without a node and cannot drift between readings.
pub fn plan(notification: &ExExNotification<EthPrimitives>) -> Plan {
    // Revert from the first reverted block, so the whole orphaned range goes at once.
    let reverted = notification.reverted_chain();
    let committed = notification.committed_chain();

    let committed_tip = committed.as_deref().map(tip_of);

    // A commit ends at its own tip; a revert with nothing to put back ends at the
    // *parent* of what it dropped. Recording that parent is what keeps the stored tip
    // canonical -- and reth errors on a resume head whose hash it cannot find.
    let tip = committed_tip.or_else(|| {
        let first = reverted.as_deref()?.first();
        // Genesis is never reverted; `checked_sub` rather than assert so a malformed
        // notification cannot panic the node.
        Some(Tip::new(
            first.number().checked_sub(1)?,
            first.parent_hash(),
        ))
    });

    Plan {
        revert_from: reverted.as_deref().map(|old| *old.range().start()),
        rows: committed.as_deref().map(rows_of).unwrap_or_default(),
        tip,
        committed: committed_tip,
    }
}

/// The head to resume from: the indexed tip, or genesis when the index is empty.
///
/// Genesis rather than no head at all. Head-less, reth delivers only future blocks and
/// never backfills, so an empty or lagging index stays that way -- silently, since a
/// short answer looks exactly like a correct one.
fn resume_head<Node>(ctx: &ExExContext<Node>, store: &Store) -> eyre::Result<ExExHead>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives>>,
{
    let block = match store.indexed_tip()? {
        Some(tip) => (tip.block_num, tip.hash).into(),
        None => {
            let genesis = ctx
                .provider()
                .block_hash(0)?
                .ok_or_else(|| eyre::eyre!("genesis block hash is missing from the database"))?;
            (0u64, genesis).into()
        }
    };
    Ok(ExExHead { block })
}

/// Run the indexer ExEx until the node shuts down.
pub async fn run<Node>(mut ctx: ExExContext<Node>, mut store: Store) -> eyre::Result<()>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives>>,
{
    // A head is what makes reth backfill the gap between the index and the node: the
    // difference between an index that recovers from being deleted, lagging, or switched
    // on mid-chain, and one that silently never does.
    let head = resume_head(&ctx, &store)?;
    info!(
        target: "allegro::indexer",
        number = head.block.number,
        hash = ?head.block.hash,
        "indexer ExEx started; resuming from",
    );
    ctx.set_notifications_with_head(head);

    while let Some(notification) = ctx.notifications.next().await {
        let plan = plan(&notification?);

        // Write first; only then acknowledge. See rule 2 in the module docs.
        store.apply(plan.revert_from, &plan.rows, plan.tip)?;

        if let Some(from) = plan.revert_from {
            debug!(target: "allegro::indexer", from, "reverted index");
        }
        if let Some(committed) = plan.committed {
            debug!(
                target: "allegro::indexer",
                block = committed.block_num,
                transactions = plan.rows.len(),
                "indexed committed chain",
            );
            ctx.events.send(ExExEvent::FinishedHeight(
                (committed.block_num, committed.hash).into(),
            ))?;
        }
    }

    info!(target: "allegro::indexer", "indexer ExEx stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy_consensus::{Header, TxEip1559};
    use alloy_primitives::{Address, Signature, TxKind, U256};
    // The concrete Ethereum aliases; `reth_ethereum::primitives` re-exports the traits
    // of the same names, which is what makes this import easy to get wrong.
    use reth_ethereum_primitives::{Block, BlockBody, TransactionSigned};
    use reth_execution_types::Chain;
    use reth_primitives_traits::RecoveredBlock;

    use super::*;
    use crate::store::{Filter, Order, Store};

    fn signed_tx(nonce: u64) -> TransactionSigned {
        let tx = TxEip1559 {
            chain_id: 1337,
            nonce,
            to: TxKind::Call(Address::from([0xbb; 20])),
            value: U256::ZERO,
            ..Default::default()
        };
        TransactionSigned::new_unhashed(tx.into(), Signature::test_signature())
    }

    /// A one-transaction block at `number`, sent by `sender`.
    fn block(number: u64, sender: u8) -> RecoveredBlock<Block> {
        let body = BlockBody {
            transactions: vec![signed_tx(number)],
            ..Default::default()
        };
        let block = Block {
            header: Header {
                number,
                ..Default::default()
            },
            body,
        };
        RecoveredBlock::new_unhashed(block, vec![Address::from([sender; 20])])
    }

    fn chain(blocks: Vec<RecoveredBlock<Block>>) -> Arc<Chain<EthPrimitives>> {
        Arc::new(Chain::new(blocks, Default::default(), Default::default()))
    }

    /// Apply a plan the way [`run`] does, so the tests exercise that same shuffle.
    fn apply(store: &mut Store, plan: Plan) {
        store.apply(plan.revert_from, &plan.rows, plan.tip).unwrap();
    }

    /// A throwaway on-disk store; RocksDB has no in-memory mode.
    fn temp_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("indexer")).unwrap();
        (dir, store)
    }

    #[test]
    fn commit_indexes_every_transaction() {
        let notification = ExExNotification::ChainCommitted {
            new: chain(vec![block(1, 0xaa), block(2, 0xaa)]),
        };
        let plan = plan(&notification);
        assert_eq!(plan.revert_from, None);
        assert_eq!(plan.rows.len(), 2);
        assert_eq!(plan.tip.unwrap().block_num, 2);
        assert_eq!(
            plan.committed, plan.tip,
            "a commit acknowledges its own tip"
        );
        assert_eq!(plan.rows[0].position, Position::new(1, 0));
        assert_eq!(plan.rows[0].from, Address::from([0xaa; 20]));
    }

    #[test]
    fn revert_plans_a_delete_and_no_insert() {
        let old = chain(vec![block(2, 0xaa), block(3, 0xaa)]);
        let parent_of_dropped = old.first().parent_hash();
        let notification = ExExNotification::ChainReverted { old: old.clone() };

        let plan = plan(&notification);
        assert_eq!(
            plan.revert_from,
            Some(2),
            "must drop from the first reverted block"
        );
        assert!(plan.rows.is_empty());
        assert_eq!(
            plan.committed, None,
            "a pure revert must acknowledge no height: FinishedHeight lets reth prune",
        );

        // The tip moves *back* to the parent of the dropped range rather than staying
        // where it was. Leaving it at 3 would name a block that is no longer canonical,
        // and reth resolves a resume head by hash -- it would not find it, and errors.
        let tip = plan
            .tip
            .expect("a revert must still record where the index now stands");
        assert_eq!(
            tip.block_num, 1,
            "the tip is the parent of the first reverted block",
        );
        assert_eq!(
            tip.hash, parent_of_dropped,
            "and carries that parent's hash"
        );
    }

    #[test]
    fn reorg_both_reverts_and_commits() {
        // The Rethix bug this exists to prevent: handling ChainReorged as a log line
        // leaves the orphaned block indexed and never records the block replacing it.
        let notification = ExExNotification::ChainReorged {
            old: chain(vec![block(2, 0xaa)]),
            new: chain(vec![block(2, 0xdd), block(3, 0xdd)]),
        };
        let plan = plan(&notification);
        assert_eq!(
            plan.revert_from,
            Some(2),
            "the orphaned block must be dropped"
        );
        assert_eq!(plan.rows.len(), 2, "the replacement blocks must be indexed");
        assert_eq!(
            plan.tip.unwrap().block_num,
            3,
            "a reorg ends at the new chain's tip, not the reverted one's parent",
        );
        assert_eq!(plan.committed, plan.tip, "and acknowledges that same tip");
        assert!(plan
            .rows
            .iter()
            .all(|r| r.from == Address::from([0xdd; 20])));
    }

    #[test]
    fn a_revert_applied_to_the_store_leaves_a_resumable_tip() {
        // The restart contract end to end: whatever the store holds after a revert has to
        // be a block reth can still find, because that value is handed straight back as
        // the resume head. Before the tip carried a hash there was nothing to check here.
        let (_dir, mut store) = temp_store();

        apply(
            &mut store,
            plan(&ExExNotification::ChainCommitted {
                new: chain(vec![block(1, 0xaa), block(2, 0xaa), block(3, 0xaa)]),
            }),
        );
        assert_eq!(store.indexed_tip().unwrap().unwrap().block_num, 3);

        let old = chain(vec![block(2, 0xaa), block(3, 0xaa)]);
        let parent_of_dropped = old.first().parent_hash();
        apply(
            &mut store,
            plan(&ExExNotification::ChainReverted { old: old.clone() }),
        );

        let resumed = store.indexed_tip().unwrap().expect("a tip to resume from");
        assert_eq!(resumed.block_num, 1, "resume below the reverted range");
        assert_eq!(resumed.hash, parent_of_dropped);
    }

    #[test]
    fn a_reorg_applied_to_the_store_leaves_no_orphans() {
        let (_dir, mut store) = temp_store();

        apply(
            &mut store,
            plan(&ExExNotification::ChainCommitted {
                new: chain(vec![block(1, 0xaa), block(2, 0xaa)]),
            }),
        );
        apply(
            &mut store,
            plan(&ExExNotification::ChainReorged {
                old: chain(vec![block(2, 0xaa)]),
                new: chain(vec![block(2, 0xdd)]),
            }),
        );

        let orphaned = store
            .reader()
            .query(
                &Filter {
                    from: Some(Address::from([0xaa; 20])),
                    ..Default::default()
                },
                None,
                Order::Ascending,
                10,
            )
            .unwrap();
        assert_eq!(
            orphaned.len(),
            1,
            "only the un-reorged block 1 should remain"
        );
        assert_eq!(orphaned[0].position.block_num, 1);

        let canonical = store
            .reader()
            .query(
                &Filter {
                    from: Some(Address::from([0xdd; 20])),
                    ..Default::default()
                },
                None,
                Order::Ascending,
                10,
            )
            .unwrap();
        assert_eq!(canonical.len(), 1);
        assert_eq!(canonical[0].position.block_num, 2);
    }
}
