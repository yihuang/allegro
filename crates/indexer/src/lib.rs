//! A transaction index for allegro, maintained by a reth Execution Extension.
//!
//! The node stores every transaction but cannot cheaply answer "which ones did this
//! address send". This crate adds that secondary index ([`store`]), keeps it in step with
//! the chain ([`exex`]), and serves it as `eth_getTransactions` ([`rpc`]) on the wire
//! contract tempo declares.
//!
//! It holds positions and filter keys only, resolving hashes back through reth, so disk
//! grows with transaction *count* rather than size and the index can be deleted and
//! rebuilt without touching consensus state.

pub mod exex;
pub mod rpc;
pub mod store;

use std::sync::Arc;

use parking_lot::Mutex;

pub use rpc::{IndexerApiServer, IndexerRpc};
pub use store::Store;

/// The read-only connection, shared among concurrently running RPC handlers.
pub type SharedStore = Arc<Mutex<Store>>;

/// File name of the index inside the node's datadir.
pub const INDEX_FILE: &str = "indexer.sqlite";

/// Open the index under `datadir` twice: a writing connection the ExEx owns
/// outright, and a read-only one the RPC handlers share.
///
/// Separate connections are what let WAL serve a query mid-backfill; on one shared
/// connection every read queues behind the writer. The writer opens (and creates)
/// first, so the read-only open always finds the schema and WAL sidecars in place.
pub fn open_store(datadir: &std::path::Path) -> eyre::Result<(Store, SharedStore)> {
    let path = datadir.join(INDEX_FILE);
    let writer = Store::open(&path)?;
    let reader = Store::open_read_only(&path)?;
    Ok((writer, Arc::new(Mutex::new(reader))))
}
