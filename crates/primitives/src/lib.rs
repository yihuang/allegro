//! Allegro primitive types.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod digest;
mod header;

pub use digest::Digest;
pub use header::{AllegroConsensusContext, AllegroHeader, ProposerKey};

use alloy_consensus::Sealable;
use alloy_primitives::B256;

/// Allegro block — standard Ethereum block with AllegroHeader.
pub type Block = alloy_consensus::Block<alloy_consensus::TxEnvelope, AllegroHeader>;
/// Allegro block body.
pub type BlockBody = alloy_consensus::BlockBody<alloy_consensus::TxEnvelope, AllegroHeader>;
/// Allegro receipt.
pub type AllegroReceipt = alloy_consensus::EthereumReceipt;

/// Marker type for Allegro node primitives.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct AllegroPrimitives;

/// Compute the block hash.
pub fn block_hash(header: &AllegroHeader) -> B256 {
    header.hash_slow()
}
