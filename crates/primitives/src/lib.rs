//! Allegro primitive types.
//!
//! Minimal block header and digest types for integrating
//! Reth (execution layer) with Commonware consensus.

#![cfg_attr(not(test), warn(unused_crate_dependencies))]

mod header;
mod digest;

pub use header::{AllegroConsensusContext, AllegroHeader, ProposerKey};
pub use digest::Digest;

use alloy_consensus::Sealable;
use alloy_primitives::B256;

/// Allegro block — a standard Ethereum block body with an [`AllegroHeader`].
pub type Block = alloy_consensus::Block<alloy_consensus::TxEnvelope, AllegroHeader>;

/// Allegro block body.
pub type BlockBody = alloy_consensus::BlockBody<alloy_consensus::TxEnvelope, AllegroHeader>;

/// Receipt type.
pub type AllegroReceipt = alloy_consensus::EthereumReceipt;

/// Marker type for Allegro node primitives.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct AllegroPrimitives;

/// Compute the block hash (Keccak of the RLP-encoded header).
pub fn block_hash(header: &AllegroHeader) -> B256 {
    header.hash_slow()
}
