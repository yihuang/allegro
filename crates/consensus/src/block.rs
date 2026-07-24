//! Block type for Commonware consensus.
//!
//! Wraps a sealed execution-layer block and implements the codec traits
//! ([`Digestible`], [`Committable`], [`Heightable`], [`EncodeSize`],
//! [`Read`], [`Write`]) needed by commonware's marshal and p2p layers.

use alloy_primitives::B256;
use bytes::{Buf, BufMut};
use commonware_codec::{EncodeSize, Read, Write};
use commonware_consensus::{Heightable, types::Height};
use commonware_cryptography::{Digestible, Committable};

use allegro_primitives::Digest;

/// The block type used by Allegro consensus.
///
/// Wraps a sealed execution block for propagation through commonware.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Block {
    pub(crate) inner: Vec<u8>, // RLP-encoded sealed block
    pub(crate) hash: B256,
}

impl Block {
    pub fn new(inner: Vec<u8>, hash: B256) -> Self {
        Self { inner, hash }
    }

    pub fn hash(&self) -> B256 {
        self.hash
    }

    pub fn raw(&self) -> &[u8] {
        &self.inner
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
        // When we don't have parsed height info, return zero.
        // The marshal layer handles height tracking via finalization certificates.
        Height::new(0)
    }
}

// ── Codec ──
//
// Wire format: 32-byte hash followed by variable-length RLP bytes.

impl EncodeSize for Block {
    fn encode_size(&self) -> usize {
        32 + self.inner.len()
    }
}

impl Read for Block {
    type Cfg = ();

    fn read_cfg(buf: &mut impl Buf, _cfg: &Self::Cfg) -> Result<Self, commonware_codec::Error> {
        if buf.remaining() < 32 {
            return Err(commonware_codec::Error::Invalid(
                "Block",
                "buffer too short for hash",
            ));
        }
        let mut hash_bytes = [0u8; 32];
        buf.copy_to_slice(&mut hash_bytes);
        let hash = B256::from(hash_bytes);

        let remaining = buf.remaining();
        let mut inner = vec![0u8; remaining];
        buf.copy_to_slice(&mut inner);

        Ok(Self { inner, hash })
    }
}

impl Write for Block {
    fn write(&self, buf: &mut impl BufMut) {
        buf.put_slice(self.hash.as_ref());
        buf.put_slice(&self.inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_codec::{DecodeExt, Encode};

    #[test]
    fn block_codec_roundtrip() {
        let hash = B256::from([0x42u8; 32]);
        let data = vec![1, 2, 3, 4, 5];
        let block = Block::new(data.clone(), hash);

        let encoded = block.encode();
        let decoded = Block::decode(encoded).unwrap();

        assert_eq!(block.hash(), decoded.hash());
        assert_eq!(block.raw(), decoded.raw());
    }
}
