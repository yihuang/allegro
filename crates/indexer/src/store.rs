//! The transaction index: a SQLite table mapping senders/recipients to chain positions.
//!
//! Rows hold positions and filter keys, never transaction bodies -- reth already stores
//! those. `(block_num, tx_index)` is both primary key and sort key, because chain
//! position is the only total order transactions have; that is what makes a cursor
//! stable, since a page boundary cannot drift as new blocks arrive.

use std::path::Path;

use alloy_primitives::{Address, TxHash, B256};
use rusqlite::types::Value;
use rusqlite::{params, params_from_iter, Connection, OpenFlags, OptionalExtension};

/// Ordering direction for a query, mirroring the RPC's `sort.order`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Order {
    Ascending,
    #[default]
    Descending,
}

/// A transaction's position in the chain. Doubles as the pagination cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Position {
    pub block_num: u64,
    pub tx_index: u64,
}

impl Position {
    pub const fn new(block_num: u64, tx_index: u64) -> Self {
        Self {
            block_num,
            tx_index,
        }
    }

    /// Zero-padded so cursors sort lexicographically too, keeping them opaque strings
    /// that clients never have to parse.
    pub fn encode(&self) -> String {
        format!("{:020}:{:010}", self.block_num, self.tx_index)
    }

    /// Parse a cursor from [`Self::encode`]; `None` on anything else, so a malformed
    /// cursor is an error rather than a silently different page.
    pub fn decode(cursor: &str) -> Option<Self> {
        let (block, index) = cursor.split_once(':')?;
        Some(Self {
            block_num: block.parse().ok()?,
            tx_index: index.parse().ok()?,
        })
    }
}

/// One indexed transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedTx {
    pub position: Position,
    pub hash: TxHash,
    pub from: Address,
    /// `None` for contract creation.
    pub to: Option<Address>,
    pub tx_type: u8,
}

/// Which transactions a query selects. Every field is optional and they intersect.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Filter {
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub tx_type: Option<u8>,
}

/// The canonical block the index is caught up to — where a restart resumes from.
///
/// The hash is load-bearing: reth resolves a resume head by hash and *errors* when it
/// finds neither the hash nor a WAL record for it. A height alone will not do, since
/// after a reorg the block at that height is a different one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tip {
    pub block_num: u64,
    pub hash: B256,
}

impl Tip {
    pub const fn new(block_num: u64, hash: B256) -> Self {
        Self { block_num, hash }
    }
}

/// Reinterpret a blob column as a fixed-width array.
///
/// An error, never a panic: `B256::from_slice` would panic on a wrong-length blob, and
/// that panic unwinds through rusqlite's callback into C. The schema should make this
/// impossible, so it only fires on corruption or a hand-edited file -- exactly when the
/// node should report rather than abort.
fn fixed<const N: usize>(raw: Vec<u8>, column: usize) -> rusqlite::Result<[u8; N]> {
    let found = raw.len();
    raw.try_into().map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Blob,
            format!("expected {N} bytes, found {found}").into(),
        )
    })
}

/// Read a column SQLite stores as `i64` back as the `u64` it was written from.
fn unsigned(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<u64> {
    let raw: i64 = row.get(column)?;
    u64::try_from(raw).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Integer,
            format!("negative value {raw}").into(),
        )
    })
}

/// How many applied notifications between stat refreshes. See [`Store::apply`].
const OPTIMIZE_EVERY: u64 = 1024;

/// The SQLite-backed index.
#[derive(Debug)]
pub struct Store {
    conn: Connection,
    applied: u64,
}

impl Store {
    /// Open (creating if absent) the index at `path`, applying the schema.
    pub fn open(path: impl AsRef<Path>) -> eyre::Result<Self> {
        let conn = Connection::open(path)?;
        Self::init(conn)
    }

    /// Open the index at `path` read-only -- the RPC's half of the pair.
    ///
    /// No schema work: the writer opens first and creates it. Read-only at the OS
    /// level, so a handler bug cannot write, and on its own connection, so under WAL
    /// it reads while [`Store::apply`] writes instead of queueing behind it.
    pub fn open_read_only(path: impl AsRef<Path>) -> eyre::Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        // A reader can still hit SQLITE_BUSY across a checkpoint reset; wait it out.
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.set_prepared_statement_cache_capacity(64);
        Ok(Self { conn, applied: 0 })
    }

    /// An in-memory index, for tests.
    pub fn in_memory() -> eyre::Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> eyre::Result<Self> {
        // WAL keeps RPC reads from blocking the ExEx's writes. `synchronous = NORMAL` is
        // safe because the index is derived: a crash that loses the last commits is
        // repaired by backfilling from the recorded tip, not by fsync.
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             -- Bounds what `PRAGMA optimize` will scan, so refreshing stats stays a
             -- sub-millisecond job on a table of any size.
             PRAGMA analysis_limit = 400;
             CREATE TABLE IF NOT EXISTS txs (
                 block_num INTEGER NOT NULL,
                 tx_index  INTEGER NOT NULL,
                 hash      BLOB    NOT NULL,
                 from_addr BLOB    NOT NULL,
                 to_addr   BLOB,
                 tx_type   INTEGER NOT NULL,
                 PRIMARY KEY (block_num, tx_index)
             );
             CREATE INDEX IF NOT EXISTS idx_txs_from ON txs (from_addr, block_num, tx_index);
             CREATE INDEX IF NOT EXISTS idx_txs_to   ON txs (to_addr,   block_num, tx_index);
             -- No index on hash: nothing selects by it -- bodies resolve through reth --
             -- so it only taxed every insert. The DROP cleans files that predate this.
             DROP INDEX IF EXISTS idx_txs_hash;
             -- The resume point: exactly one row, replaced on every applied notification.
             CREATE TABLE IF NOT EXISTS tip (
                 id        INTEGER PRIMARY KEY CHECK (id = 0),
                 block_num INTEGER NOT NULL,
                 hash      BLOB    NOT NULL
             );",
        )?;
        // `apply` reuses three statements and `query` one of a few dozen shapes, so
        // cache them (default capacity 16 is too small for query's variants).
        conn.set_prepared_statement_cache_capacity(64);
        Ok(Self { conn, applied: 0 })
    }

    /// Apply one notification atomically: drop `revert_from` and above, insert `commit`,
    /// record the new tip.
    ///
    /// One SQLite transaction on purpose. A reorg is a revert *and* a commit, and a crash
    /// between them would leave orphaned rows under a tip claiming they are current --
    /// a corruption no later notification repairs.
    pub fn apply(
        &mut self,
        revert_from: Option<u64>,
        commit: &[IndexedTx],
        tip: Option<Tip>,
    ) -> eyre::Result<()> {
        let tx = self.conn.transaction()?;
        if let Some(from) = revert_from {
            tx.prepare_cached("DELETE FROM txs WHERE block_num >= ?1")?
                .execute(params![from])?;
        }
        {
            let mut insert = tx.prepare_cached(
                "INSERT OR REPLACE INTO txs (block_num, tx_index, hash, from_addr, to_addr, tx_type)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            for entry in commit {
                insert.execute(params![
                    entry.position.block_num,
                    entry.position.tx_index,
                    entry.hash.as_slice(),
                    entry.from.as_slice(),
                    entry.to.as_ref().map(|a| a.as_slice()),
                    entry.tx_type,
                ])?;
            }
        }
        if let Some(tip) = tip {
            tx.prepare_cached(
                "INSERT OR REPLACE INTO tip (id, block_num, hash) VALUES (0, ?1, ?2)",
            )?
            .execute(params![tip.block_num, tip.hash.as_slice()])?;
        }
        tx.commit()?;

        // Refresh the query planner's statistics now and then. Without them SQLite has
        // no cardinalities to choose between `idx_txs_from` and `idx_txs_to`, takes the
        // first, and a `from`+`to` query scans the wrong side: measured at 2.9ms against
        // 33us once analysed, on 200k rows. `analysis_limit` keeps each run cheap.
        self.applied += 1;
        if self.applied.is_multiple_of(OPTIMIZE_EVERY) {
            self.conn.execute_batch("PRAGMA optimize;")?;
        }
        Ok(())
    }

    /// The block the index is caught up to, or `None` before the first notification.
    ///
    /// Always canonical -- a revert records the parent of what it dropped -- so it can go
    /// straight back to reth as a resume head.
    pub fn indexed_tip(&self) -> eyre::Result<Option<Tip>> {
        Ok(self
            .conn
            .query_row("SELECT block_num, hash FROM tip WHERE id = 0", [], |row| {
                Ok(Tip::new(
                    unsigned(row, 0)?,
                    B256::from(fixed::<32>(row.get(1)?, 1)?),
                ))
            })
            .optional()?)
    }

    /// Page through the index. `after` is exclusive and read in `order`'s direction, so
    /// paging forward never repeats or skips a row. Returns at most `limit` rows.
    pub fn query(
        &self,
        filter: &Filter,
        after: Option<Position>,
        order: Order,
        limit: usize,
    ) -> eyre::Result<Vec<IndexedTx>> {
        let mut sql = String::from(
            "SELECT block_num, tx_index, hash, from_addr, to_addr, tx_type FROM txs WHERE 1=1",
        );
        let mut binds: Vec<Value> = Vec::new();

        if let Some(from) = filter.from {
            sql.push_str(" AND from_addr = ?");
            binds.push(from.to_vec().into());
        }
        if let Some(to) = filter.to {
            sql.push_str(" AND to_addr = ?");
            binds.push(to.to_vec().into());
        }
        if let Some(tx_type) = filter.tx_type {
            sql.push_str(" AND tx_type = ?");
            binds.push(i64::from(tx_type).into());
        }
        if let Some(pos) = after {
            // The (block, index) pair compared by hand: not every SQLite build we might
            // link has row-value comparison.
            let cmp = match order {
                Order::Ascending => '>',
                Order::Descending => '<',
            };
            sql.push_str(&format!(
                " AND (block_num {cmp} ? OR (block_num = ? AND tx_index {cmp} ?))"
            ));
            binds.push((pos.block_num as i64).into());
            binds.push((pos.block_num as i64).into());
            binds.push((pos.tx_index as i64).into());
        }
        sql.push_str(match order {
            Order::Ascending => " ORDER BY block_num ASC, tx_index ASC",
            Order::Descending => " ORDER BY block_num DESC, tx_index DESC",
        });
        sql.push_str(" LIMIT ?");
        binds.push((limit as i64).into());

        let mut stmt = self.conn.prepare_cached(&sql)?;
        let rows = stmt.query_map(params_from_iter(binds), |row| {
            Ok(IndexedTx {
                position: Position::new(unsigned(row, 0)?, unsigned(row, 1)?),
                hash: B256::from(fixed::<32>(row.get(2)?, 2)?),
                from: Address::from(fixed::<20>(row.get(3)?, 3)?),
                to: row
                    .get::<_, Option<Vec<u8>>>(4)?
                    .map(|raw| fixed::<20>(raw, 4))
                    .transpose()?
                    .map(Address::from),
                tx_type: row.get(5)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    /// A deterministic tip for `block`, so a test can name one without inventing a hash.
    fn tip(block: u64) -> Option<Tip> {
        Some(Tip::new(block, B256::from([block as u8; 32])))
    }

    fn tx(block: u64, index: u64, from: u8, to: Option<u8>, tx_type: u8) -> IndexedTx {
        IndexedTx {
            position: Position::new(block, index),
            hash: B256::from([(block * 10 + index) as u8; 32]),
            from: addr(from),
            to: to.map(addr),
            tx_type,
        }
    }

    fn seeded() -> Store {
        let mut store = Store::in_memory().unwrap();
        let entries = vec![
            tx(1, 0, 0xaa, Some(0xbb), 2),
            tx(1, 1, 0xaa, Some(0xcc), 2),
            tx(2, 0, 0xbb, Some(0xaa), 0),
            tx(3, 0, 0xaa, None, 2),
        ];
        store.apply(None, &entries, tip(3)).unwrap();
        store
    }

    #[test]
    fn cursor_round_trips() {
        let pos = Position::new(12345, 7);
        assert_eq!(Position::decode(&pos.encode()), Some(pos));
        assert_eq!(Position::decode("nonsense"), None, "no separator");
        assert_eq!(Position::decode("abc:def"), None, "non-numeric parts");
        assert_eq!(Position::decode("1:-1"), None, "negative index");
    }

    #[test]
    fn cursors_sort_lexicographically() {
        assert!(Position::new(2, 0).encode() > Position::new(1, 9).encode());
        assert!(Position::new(1, 2).encode() > Position::new(1, 1).encode());
    }

    #[test]
    fn zero_position_round_trips() {
        // The all-zeros cursor is the genesis-position edge case that a naive
        // trim_start_matches('0') would turn into an empty string.
        let pos = Position::new(0, 0);
        assert_eq!(Position::decode(&pos.encode()), Some(pos));
    }

    #[test]
    fn filters_by_sender() {
        let store = seeded();
        let filter = Filter {
            from: Some(addr(0xaa)),
            ..Default::default()
        };
        let got = store.query(&filter, None, Order::Ascending, 10).unwrap();
        assert_eq!(got.len(), 3);
        assert!(got.iter().all(|t| t.from == addr(0xaa)));
    }

    #[test]
    fn filters_intersect() {
        let store = seeded();
        let filter = Filter {
            from: Some(addr(0xaa)),
            to: Some(addr(0xcc)),
            ..Default::default()
        };
        let got = store.query(&filter, None, Order::Ascending, 10).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].position, Position::new(1, 1));
    }

    #[test]
    fn contract_creation_has_no_recipient() {
        let store = seeded();
        let got = store
            .query(&Filter::default(), None, Order::Ascending, 10)
            .unwrap();
        let creation = got.iter().find(|t| t.position.block_num == 3).unwrap();
        assert_eq!(creation.to, None);
    }

    #[test]
    fn descending_is_the_reverse_of_ascending() {
        let store = seeded();
        let asc = store
            .query(&Filter::default(), None, Order::Ascending, 10)
            .unwrap();
        let mut desc = store
            .query(&Filter::default(), None, Order::Descending, 10)
            .unwrap();
        desc.reverse();
        assert_eq!(asc, desc);
    }

    #[test]
    fn paging_visits_every_row_exactly_once() {
        let store = seeded();
        for order in [Order::Ascending, Order::Descending] {
            let all = store.query(&Filter::default(), None, order, 100).unwrap();
            let mut seen = Vec::new();
            let mut cursor = None;
            loop {
                let page = store.query(&Filter::default(), cursor, order, 1).unwrap();
                let Some(entry) = page.into_iter().next() else {
                    break;
                };
                cursor = Some(entry.position);
                seen.push(entry);
            }
            assert_eq!(
                seen, all,
                "paged result diverged from the single-page result"
            );
        }
    }

    #[test]
    fn revert_drops_the_block_and_everything_above() {
        let mut store = seeded();
        store.apply(Some(2), &[], tip(1)).unwrap();
        let left = store
            .query(&Filter::default(), None, Order::Ascending, 10)
            .unwrap();
        assert_eq!(left.len(), 2);
        assert!(left.iter().all(|t| t.position.block_num == 1));
        assert_eq!(store.indexed_tip().unwrap(), tip(1));
    }

    #[test]
    fn reorg_replaces_rather_than_merges() {
        // The failure this guards: reverting and committing as two steps can leave the
        // orphaned block's transactions behind, so a reorged-out tx keeps being served.
        let mut store = seeded();
        let replacement = vec![tx(2, 0, 0xdd, Some(0xee), 2)];
        store.apply(Some(2), &replacement, tip(3)).unwrap();

        let orphaned = store
            .query(
                &Filter {
                    from: Some(addr(0xbb)),
                    ..Default::default()
                },
                None,
                Order::Ascending,
                10,
            )
            .unwrap();
        assert!(orphaned.is_empty(), "orphaned transaction survived a reorg");

        let canonical = store
            .query(
                &Filter {
                    from: Some(addr(0xdd)),
                    ..Default::default()
                },
                None,
                Order::Ascending,
                10,
            )
            .unwrap();
        assert_eq!(canonical.len(), 1);
    }

    #[test]
    fn reapplying_a_block_does_not_duplicate_it() {
        let mut store = seeded();
        store
            .apply(None, &[tx(1, 0, 0xaa, Some(0xbb), 2)], tip(3))
            .unwrap();
        let got = store
            .query(&Filter::default(), None, Order::Ascending, 10)
            .unwrap();
        assert_eq!(got.len(), 4);
    }

    #[test]
    fn reader_sees_writes_and_cannot_write() {
        // The wiring `open_store` sets up: writer first (creates schema and the WAL
        // sidecars), reader second. Covers the WAL read-only edge case in one place.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("indexer.sqlite");
        let mut writer = Store::open(&path).unwrap();
        let mut reader = Store::open_read_only(&path).unwrap();

        writer
            .apply(None, &[tx(1, 0, 0xaa, Some(0xbb), 2)], tip(1))
            .unwrap();
        assert_eq!(reader.indexed_tip().unwrap(), tip(1));
        let got = reader
            .query(&Filter::default(), None, Order::Ascending, 10)
            .unwrap();
        assert_eq!(got.len(), 1);
        assert!(
            reader.apply(None, &[], tip(2)).is_err(),
            "the read-only connection accepted a write"
        );
    }

    #[test]
    fn a_corrupt_row_errors_rather_than_panicking() {
        // Only reachable through corruption or a hand-edited file, but the blow-up would
        // be a panic unwinding out of a rusqlite callback -- i.e. the node, not the query.
        let store = seeded();
        store
            .conn
            .execute("UPDATE txs SET hash = X'DEAD' WHERE block_num = 1", [])
            .unwrap();
        store
            .conn
            .execute("UPDATE tip SET block_num = -1 WHERE id = 0", [])
            .unwrap();

        assert!(store
            .query(&Filter::default(), None, Order::Ascending, 10)
            .is_err());
        assert!(store.indexed_tip().is_err());
    }

    #[test]
    fn tip_is_absent_before_the_first_notification() {
        // The resume path reads this to decide between "catch up from here" and "backfill
        // from genesis", so absent has to mean absent rather than block zero.
        assert_eq!(Store::in_memory().unwrap().indexed_tip().unwrap(), None);
    }

    #[test]
    fn tip_round_trips_with_its_hash() {
        // The hash is the half that matters on restart: reth resolves the resume head by
        // hash, so a store that persisted only the height could not produce a usable one.
        let mut store = Store::in_memory().unwrap();
        let want = Tip::new(7, B256::from([0x7c; 32]));
        store.apply(None, &[], Some(want)).unwrap();
        assert_eq!(store.indexed_tip().unwrap(), Some(want));
    }

    #[test]
    fn tip_is_replaced_not_appended() {
        // One row, enforced by the schema. A second row would make "where am I" ambiguous
        // and the resume head order-dependent.
        let mut store = Store::in_memory().unwrap();
        store.apply(None, &[], tip(1)).unwrap();
        store.apply(None, &[], tip(2)).unwrap();
        assert_eq!(store.indexed_tip().unwrap(), tip(2));
        let rows: i64 = store
            .conn
            .query_row("SELECT COUNT(*) FROM tip", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn a_revert_moves_the_tip_back() {
        // Applying a revert must leave the tip naming a block that still exists. Left at
        // 3, a restart would hand reth a head hash it cannot find and the ExEx would fail
        // rather than catch up.
        let mut store = seeded();
        assert_eq!(store.indexed_tip().unwrap(), tip(3));
        store.apply(Some(2), &[], tip(1)).unwrap();
        assert_eq!(store.indexed_tip().unwrap(), tip(1));
    }
}
