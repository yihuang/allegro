//! [`Digest`] wrapper around `B256` for use with Commonware's simplex consensus.

use alloy_primitives::B256;
use bytes::BufMut;
use commonware_codec::{FixedSize, Read, ReadExt, Write};
use commonware_utils::{Array, Span};
use core::ops::Deref;
use rand_core::CryptoRngCore;

/// Wrapper around [`B256`] for use in places requiring
/// [`commonware_cryptography::Digest`].
///
/// A block's digest is its execution-layer block hash: the proposer mints
/// digests as `Digest(block_hash)`, and genesis is registered under its
/// chainspec hash. Consumers rely on the identity through
/// [`block_hash`](Self::block_hash).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Digest(pub B256);

impl Digest {
    /// Return the inner `B256`.
    pub fn get(self) -> B256 {
        self.0
    }

    /// The execution-layer block hash this digest names.
    pub const fn block_hash(self) -> B256 {
        self.0
    }
}

impl Array for Digest {}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for Digest {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl commonware_math::algebra::Random for Digest {
    fn random(mut rng: impl CryptoRngCore) -> Self {
        let mut array = B256::ZERO;
        rng.fill_bytes(&mut *array);
        Self(array)
    }
}

impl commonware_cryptography::Digest for Digest {
    const EMPTY: Self = Self(B256::ZERO);
}

impl FixedSize for Digest {
    const SIZE: usize = 32;
}

impl core::fmt::Display for Digest {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(f)
    }
}

impl Read for Digest {
    type Cfg = ();

    fn read_cfg(
        buf: &mut impl bytes::Buf,
        _cfg: &Self::Cfg,
    ) -> Result<Self, commonware_codec::Error> {
        let array = <[u8; 32]>::read(buf)?;
        Ok(Self(B256::new(array)))
    }
}

impl Span for Digest {}

impl Write for Digest {
    fn write(&self, buf: &mut impl BufMut) {
        self.0.write(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commonware_cryptography::Digest as _;

    #[test]
    fn digest_size() {
        assert_eq!(Digest::SIZE, 32);
    }

    #[test]
    fn digest_empty() {
        assert_eq!(Digest::EMPTY, Digest(B256::ZERO));
    }
}
