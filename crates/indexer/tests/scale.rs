//! Scale probe for the store, ignored by default — numbers, not assertions.
//! `cargo test -p allegro-indexer --release --test scale -- --ignored --nocapture`
//!
//! The workload matches the SQLite/RocksDB comparison run for PR #17, so numbers
//! stay comparable across engine work: 200k txs over 20k blocks, 5k senders, 97
//! recipients, 3 types.

use std::time::Instant;

use allegro_indexer::store::{Filter, IndexedTx, Order, Plan, Position, Store, Tip};
use alloy_primitives::{Address, B256};

const BLOCKS: u64 = 20_000;
const TXS_PER_BLOCK: u32 = 10;
const SENDERS: u64 = 5_000;
const RECIPIENTS: u64 = 97;
const REPS: u32 = 100;

fn addr(n: u64) -> Address {
    let mut b = [0u8; 20];
    b[..8].copy_from_slice(&n.to_be_bytes());
    Address::from(b)
}

/// One block's worth of rows.
fn rows_of(block: u64) -> Vec<IndexedTx> {
    (0..TXS_PER_BLOCK)
        .map(|i| {
            let n = block * u64::from(TXS_PER_BLOCK) + u64::from(i);
            IndexedTx {
                position: Position::new(block, i),
                hash: B256::from([n as u8; 32]),
                from: addr(n % SENDERS),
                to: Some(addr(n % RECIPIENTS)),
                tx_type: (n % 3) as u8,
            }
        })
        .collect()
}

/// What [`Plan::merge`] buys the ExEx: the same blocks folded `batch` at a time
/// into one `apply`, against one `apply` per block.
///
/// Run alongside `scale` to compare; separate so the numbers above stay the
/// per-block baseline they have always been.
#[test]
#[ignore = "prints timings; run explicitly with --ignored --nocapture"]
fn batched_write() {
    println!("\n{BLOCKS} blocks x {TXS_PER_BLOCK} txs");
    for batch in [1_u64, 8, 64, 256] {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path().join("indexer")).unwrap();

        let t = Instant::now();
        let mut block = 0;
        while block < BLOCKS {
            let last = (block + batch - 1).min(BLOCKS - 1);
            // Exactly what `run` does with a queue `batch` deep.
            let mut folded = Plan::default();
            for b in block..=last {
                folded.merge(Plan {
                    rows: rows_of(b),
                    tip: Some(Tip::new(b, B256::ZERO)),
                    committed: Some(Tip::new(b, B256::ZERO)),
                    ..Default::default()
                });
            }
            store.apply(&folded).unwrap();
            block = last + 1;
        }
        let elapsed = t.elapsed();
        println!(
            "batch={batch:<4}{:>12.1?}   ({:.0}k blocks/s)",
            elapsed,
            BLOCKS as f64 / elapsed.as_secs_f64() / 1000.0
        );
    }
}

#[test]
#[ignore = "prints timings; run explicitly with --ignored --nocapture"]
fn scale() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().join("indexer")).unwrap();

    let t = Instant::now();
    for block in 0..BLOCKS {
        let rows: Vec<_> = (0..TXS_PER_BLOCK)
            .map(|i| {
                let n = block * u64::from(TXS_PER_BLOCK) + u64::from(i);
                IndexedTx {
                    position: Position::new(block, i),
                    hash: B256::from([n as u8; 32]),
                    from: addr(n % SENDERS),
                    to: Some(addr(n % RECIPIENTS)),
                    tx_type: (n % 3) as u8,
                }
            })
            .collect();
        store
            .apply(&Plan {
                rows,
                tip: Some(Tip::new(block, B256::ZERO)),
                ..Default::default()
            })
            .unwrap();
    }
    let write = t.elapsed();

    let time = |name: &str, filter: &Filter, after: Option<Position>| {
        let mut rows = 0;
        let t = Instant::now();
        for _ in 0..REPS {
            rows = store
                .reader()
                .query(filter, after, Order::Descending, 100)
                .unwrap()
                .len();
        }
        println!("{name:<26}{:>12.1?}   ({rows} rows)", t.elapsed() / REPS);
    };

    let total = BLOCKS * u64::from(TXS_PER_BLOCK);
    println!("\n{total} txs, {BLOCKS} blocks, {SENDERS} senders; page=100, reps={REPS}");
    println!(
        "write                     {:>12.2?}   ({:.0}k tx/s)",
        write,
        total as f64 / write.as_secs_f64() / 1000.0
    );
    time(
        "page by sender",
        &Filter {
            from: Some(addr(42)),
            ..Default::default()
        },
        None,
    );
    time(
        "from+to intersect",
        &Filter {
            from: Some(addr(42)),
            to: Some(addr(7)),
            ..Default::default()
        },
        None,
    );
    time(
        "deep cursor (10k blocks)",
        &Filter {
            from: Some(addr(42)),
            ..Default::default()
        },
        Some(Position::new(BLOCKS / 2, 0)),
    );
    time(
        "page by type",
        &Filter {
            tx_type: Some(1),
            ..Default::default()
        },
        None,
    );

    // Destructive, so last: drop the top 100 blocks in one apply.
    let t = Instant::now();
    store
        .apply(&Plan {
            revert_from: Some(BLOCKS - 100),
            tip: Some(Tip::new(BLOCKS - 101, B256::ZERO)),
            ..Default::default()
        })
        .unwrap();
    println!("revert 100 blocks         {:>12.1?}", t.elapsed());

    // Close cleanly first so the size is the database, not a moment mid-compaction.
    drop(store);
    let bytes: u64 = walk(dir.path());
    println!(
        "on disk (lz4)             {:>9.1} MiB",
        bytes as f64 / 1048576.0
    );
}

fn walk(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| {
            let m = e.metadata().unwrap();
            if m.is_dir() {
                walk(&e.path())
            } else {
                m.len()
            }
        })
        .sum()
}
