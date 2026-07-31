//! Allegro block header, extending the Ethereum header with consensus metadata.

use alloy_consensus::{BlockHeader, Header, Sealable};
use alloy_primitives::{keccak256, Address, BlockNumber, Bloom, Bytes, B256, B64, U256};
use alloy_rlp::{Decodable, Encodable, Header as RlpHeader, RlpDecodable, RlpEncodable};
use bytes::BufMut;

/// A raw Ed25519 public key (32 bytes) stored in the block header.
///
/// This is a wire-format type for RLP encoding. Convert to/from
/// [`commonware_cryptography::ed25519::PublicKey`] when interacting
/// with the consensus layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProposerKey(pub [u8; 32]);

impl ProposerKey {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Encodable for ProposerKey {
    fn encode(&self, out: &mut dyn BufMut) {
        self.0.encode(out)
    }
    fn length(&self) -> usize {
        32
    }
}

impl Decodable for ProposerKey {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let arr: [u8; 32] = Decodable::decode(buf)?;
        Ok(Self(arr))
    }
}

impl From<[u8; 32]> for ProposerKey {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// Consensus metadata attached to every Allegro block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AllegroConsensusContext {
    pub epoch: u64,
    pub view: u64,
    pub parent_view: u64,
    /// Ed25519 public key of the block proposer (32 bytes).
    pub proposer: ProposerKey,
}

impl AllegroConsensusContext {
    /// Convert the proposer key to a Commonware `ed25519::PublicKey`.
    /// Panics if the raw bytes are not a valid Ed25519 point (should never
    /// happen for keys that were originally created from a valid public key).
    pub fn proposer_commonware(&self) -> commonware_cryptography::ed25519::PublicKey {
        use ed25519_consensus::VerificationKey;
        let vkb = ed25519_consensus::VerificationKeyBytes::from(self.proposer.0);
        let vk = VerificationKey::try_from(vkb)
            .expect("ProposerKey bytes should be a valid Ed25519 point");
        commonware_cryptography::ed25519::PublicKey::from(vk)
    }

    /// Build from a Commonware `ed25519::PublicKey`.
    pub fn from_commonware_proposer(
        epoch: u64,
        view: u64,
        parent_view: u64,
        proposer: &commonware_cryptography::ed25519::PublicKey,
    ) -> Self {
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(proposer.as_ref());
        Self {
            epoch,
            view,
            parent_view,
            proposer: ProposerKey(bytes),
        }
    }
}

impl Encodable for AllegroConsensusContext {
    fn encode(&self, out: &mut dyn BufMut) {
        // Buffer all fields to a temp Vec to compute the accurate payload
        // length (u64 RLP encoding strips leading zeros, so we can't
        // hardcode sizes).
        let mut payload = Vec::new();
        self.epoch.encode(&mut payload);
        self.view.encode(&mut payload);
        self.parent_view.encode(&mut payload);
        // Proposer as RLP byte string (0xa0 + 32 raw bytes)
        payload.extend_from_slice(&alloy_rlp::encode(&self.proposer.0[..]));

        RlpHeader {
            list: true,
            payload_length: payload.len(),
        }
        .encode(out);
        out.put_slice(&payload);
    }

    fn length(&self) -> usize {
        let mut payload = Vec::new();
        self.epoch.encode(&mut payload);
        self.view.encode(&mut payload);
        self.parent_view.encode(&mut payload);
        payload.extend_from_slice(&alloy_rlp::encode(&self.proposer.0[..]));
        let pl = payload.len();
        let hdr = if pl <= 55 {
            1
        } else if pl <= 0xff {
            2
        } else if pl <= 0xffff {
            3
        } else {
            4
        };
        hdr + pl
    }
}

impl Decodable for AllegroConsensusContext {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let header = RlpHeader::decode(buf)?;
        if !header.list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }
        let epoch = u64::decode(buf)?;
        let view = u64::decode(buf)?;
        let parent_view = u64::decode(buf)?;
        let proposer = ProposerKey::decode(buf)?;
        Ok(Self {
            epoch,
            view,
            parent_view,
            proposer,
        })
    }
}

/// Allegro block header.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, RlpEncodable, RlpDecodable)]
#[rlp(trailing(no_gaps))]
pub struct AllegroHeader {
    /// Inner Ethereum [`Header`].
    pub inner: Header,

    /// Millisecond-precision timestamp (since UNIX epoch), monotonically increasing:
    /// always > the parent's `timestamp_millis`.
    pub timestamp_millis: u64,

    /// Consensus metadata. `None` for pre-consensus / genesis blocks.
    pub consensus_context: Option<AllegroConsensusContext>,
}

impl AllegroHeader {
    pub fn new(
        inner: Header,
        timestamp_millis: u64,
        consensus_context: Option<AllegroConsensusContext>,
    ) -> Self {
        Self {
            inner,
            timestamp_millis,
            consensus_context,
        }
    }

    pub fn epoch(&self) -> Option<u64> {
        self.consensus_context.map(|ctx| ctx.epoch)
    }

    pub fn view(&self) -> Option<u64> {
        self.consensus_context.map(|ctx| ctx.view)
    }

    pub fn proposer(&self) -> Option<&ProposerKey> {
        self.consensus_context.as_ref().map(|ctx| &ctx.proposer)
    }
}

// ── BlockHeader trait delegation ──

impl AsRef<Self> for AllegroHeader {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl BlockHeader for AllegroHeader {
    fn parent_hash(&self) -> B256 {
        self.inner.parent_hash()
    }
    fn ommers_hash(&self) -> B256 {
        self.inner.ommers_hash()
    }
    fn beneficiary(&self) -> Address {
        self.inner.beneficiary()
    }
    fn state_root(&self) -> B256 {
        self.inner.state_root()
    }
    fn transactions_root(&self) -> B256 {
        self.inner.transactions_root()
    }
    fn receipts_root(&self) -> B256 {
        self.inner.receipts_root()
    }
    fn withdrawals_root(&self) -> Option<B256> {
        self.inner.withdrawals_root()
    }
    fn logs_bloom(&self) -> Bloom {
        self.inner.logs_bloom()
    }
    fn difficulty(&self) -> U256 {
        self.inner.difficulty()
    }
    fn number(&self) -> BlockNumber {
        self.inner.number()
    }
    fn gas_limit(&self) -> u64 {
        self.inner.gas_limit()
    }
    fn gas_used(&self) -> u64 {
        self.inner.gas_used()
    }
    fn timestamp(&self) -> u64 {
        self.inner.timestamp()
    }
    fn mix_hash(&self) -> Option<B256> {
        self.inner.mix_hash()
    }
    fn nonce(&self) -> Option<B64> {
        self.inner.nonce()
    }
    fn base_fee_per_gas(&self) -> Option<u64> {
        self.inner.base_fee_per_gas()
    }
    fn blob_gas_used(&self) -> Option<u64> {
        self.inner.blob_gas_used()
    }
    fn excess_blob_gas(&self) -> Option<u64> {
        self.inner.excess_blob_gas()
    }
    fn parent_beacon_block_root(&self) -> Option<B256> {
        self.inner.parent_beacon_block_root()
    }
    fn requests_hash(&self) -> Option<B256> {
        self.inner.requests_hash()
    }
    fn block_access_list_hash(&self) -> Option<B256> {
        self.inner.block_access_list_hash()
    }
    fn slot_number(&self) -> Option<u64> {
        self.inner.slot_number()
    }
    fn extra_data(&self) -> &Bytes {
        self.inner.extra_data()
    }
}

impl Sealable for AllegroHeader {
    fn hash_slow(&self) -> B256 {
        keccak256(alloy_rlp::encode(self))
    }
}

impl From<Header> for AllegroHeader {
    fn from(inner: Header) -> Self {
        Self {
            inner,
            timestamp_millis: 0,
            consensus_context: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rlp::Decodable;

    #[test]
    fn header_rlp_roundtrip_with_context() {
        let header = AllegroHeader {
            inner: Header {
                number: 42,
                timestamp: 1_700_000_000,
                ..Default::default()
            },
            timestamp_millis: 1_700_000_000_123,
            consensus_context: Some(AllegroConsensusContext {
                epoch: 1,
                view: 5,
                parent_view: 4,
                proposer: ProposerKey([0xab; 32]),
            }),
        };
        let encoded = alloy_rlp::encode(&header);
        let decoded = AllegroHeader::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn header_rlp_roundtrip_without_context() {
        let header = AllegroHeader {
            inner: Header {
                number: 0,
                ..Default::default()
            },
            timestamp_millis: 0,
            consensus_context: None,
        };
        let encoded = alloy_rlp::encode(&header);
        let decoded = AllegroHeader::decode(&mut encoded.as_slice()).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn header_hash_deterministic() {
        let header = AllegroHeader {
            inner: Header {
                number: 42,
                ..Default::default()
            },
            timestamp_millis: 0,
            consensus_context: None,
        };
        let h1 = header.hash_slow();
        let h2 = header.hash_slow();
        assert_eq!(h1, h2);
        assert_ne!(h1, B256::ZERO);
    }
}
