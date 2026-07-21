//! Block type stub for Commonware consensus.
//!
//! In the full implementation this wraps an execution-layer sealed block
//! and implements [`Digestible`], [`Committable`], [`Heightable`], and codec
//! traits so commonware can propagate and finalize it.

use alloy_primitives::B256;
use commonware_codec::{EncodeSize, Read, Write};
use commonware_consensus::Heightable;
use commonware_consensus::types::{Epoch, Height, View};
use commonware_cryptography::{Digestible, Committable};
use tracing::warn;

use allegro_primitives::Digest;

/// The block type used by Allegro consensus.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Block {
    pub(crate) inner: Vec<u8>,
    pub(crate) hash: B256,
}

impl Block {
    pub(crate) fn hash(&self) -> B256 {
        self.hash
    }
}

// ── Digestible ──

impl Digestible for Block {
    type Digest = Digest;

    fn digest(&self) -> Self::Digest {
        Digest(self.hash)
    }
}

// ── Committable ──

impl Committable for Block {
    type Commitment = Digest;

    fn commitment(&self) -> Self::Commitment {
        Digest(self.hash)
    }
}

// ── Heightable ──

impl Heightable for Block {
    fn height(&self) -> Height {
        Height::new(0)
    }
}

// ── Codec stubs ──

impl EncodeSize for Block {
    fn encode_size(&self) -> usize {
        self.inner.len()
    }
}

impl Read for Block {
    type Cfg = ();

    fn read_cfg(
        _buf: &mut impl bytes::Buf,
        _cfg: &Self::Cfg,
    ) -> Result<Self, commonware_codec::Error> {
        Err(commonware_codec::Error::Invalid(
            "Block",
            "deserialization not yet implemented",
        ))
    }
}

impl Write for Block {
    fn write(&self, _buf: &mut impl bytes::BufMut) {
        // TODO: implement
    }
}
