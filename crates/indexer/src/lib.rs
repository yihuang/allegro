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

pub use rpc::{IndexerApiServer, IndexerRpc};
pub use store::{Reader, Store};

/// Directory name of the index inside the node's datadir.
pub const INDEX_DIR: &str = "indexer";

/// Open the index under `datadir`: a writing handle the ExEx owns outright, and a
/// lock-free read handle the RPC handlers share.
pub fn open_store(datadir: &std::path::Path) -> eyre::Result<(Store, Reader)> {
    // reth creates the datadir before either launch path reaches here; this is one
    // syscall to not depend on that.
    std::fs::create_dir_all(datadir)?;
    let store = Store::open(datadir.join(INDEX_DIR))?;
    let reader = store.reader();
    Ok((store, reader))
}
