//! Shared test support: an empty-block payload builder and actor harness.
//!
//! Production always builds through reth's engine API, but the deterministic
//! runtime can't host a reth node, so these tests drive consensus with blocks
//! that carry no transactions.

#![allow(dead_code)] // each integration test binary uses a different subset

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use allegro_consensus::application::{
    self as app_actor, Actor, ActorConfig, BlockInfoMap, LeaderSchedule, Mailbox, ReceivedBlocks,
};
use allegro_consensus::{
    BlockMeta, BuildPayloadRequest, BuiltPayload, PayloadBuilder, ValidationResult, ValidatorEntry,
    ValidatorSet,
};
use allegro_primitives::{AllegroConsensusContext, AllegroHeader};
use alloy_consensus::{Block as AlloyBlock, BlockBody, Sealable, TxEnvelope};
use alloy_primitives::{keccak256, Address, Bloom, B256, B64, U256};
use commonware_consensus::{
    simplex::types::Context,
    types::{Round, View},
};
use commonware_cryptography::{
    ed25519::{PrivateKey, PublicKey},
    Signer as _,
};
use commonware_utils::ordered::Set;

// ── Clock ───────────────────────────────────────────────────

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn now_secs() -> u64 {
    now_millis() / 1000
}

// ── Validators ──────────────────────────────────────────────

/// `n` validators on `127.0.0.1`, plus the ordered participant set the engine
/// elects over — index `i` in that set is what the leader schedule resolves to.
pub fn build_validators(n: u64, base_port: u16) -> (ValidatorSet, Set<PublicKey>) {
    let entries: Vec<ValidatorEntry> = (0..n)
        .map(|seed| ValidatorEntry {
            public_key: PrivateKey::from_seed(seed).public_key(),
            ingress: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), base_port + seed as u16),
            egress: IpAddr::V4(Ipv4Addr::LOCALHOST),
        })
        .collect();
    let participants = Set::try_from(
        entries
            .iter()
            .map(|e| e.public_key.clone())
            .collect::<Vec<_>>(),
    )
    .expect("distinct keys");
    (ValidatorSet::from_entries(&entries), participants)
}

pub fn context(
    round: Round,
    leader: PublicKey,
    parent: (View, allegro_primitives::Digest),
) -> Context<allegro_primitives::Digest, PublicKey> {
    Context {
        round,
        leader,
        parent,
    }
}

// ── Actor harness ───────────────────────────────────────────

/// A running actor plus the stores tests inspect.
pub struct Harness {
    pub mailbox: Mailbox,
    pub block_info: BlockInfoMap,
    pub received: ReceivedBlocks,
}

/// Spawn an actor on the current tokio runtime, genesis-rooted at zero.
///
/// The task is aborted when the returned `Harness` and its mailbox are dropped
/// at the end of the test.
pub fn spawn_actor(
    validators: ValidatorSet,
    builder: Arc<dyn PayloadBuilder>,
    leader_schedule: Option<LeaderSchedule>,
) -> Harness {
    spawn_actor_limited(validators, builder, leader_schedule, 64)
}

/// [`spawn_actor`] with an explicit cap on handlers in flight.
pub fn spawn_actor_limited(
    validators: ValidatorSet,
    builder: Arc<dyn PayloadBuilder>,
    leader_schedule: Option<LeaderSchedule>,
    max_concurrent_handlers: usize,
) -> Harness {
    let (pending_blocks, received, block_info) = app_actor::new_block_stores();
    let (actor, mailbox) = Actor::new(ActorConfig {
        validators,
        mailbox_size: 1024,
        max_concurrent_handlers,
        proposals: None,
        pending_blocks,
        received_blocks: received.clone(),
        block_info: block_info.clone(),
        payload_builder: builder,
        metrics: None,
        genesis_hash: B256::ZERO,
        genesis_timestamp: 0,
        genesis_timestamp_millis: 0,
        leader_schedule,
    });
    tokio::spawn(actor.run());
    Harness {
        mailbox,
        block_info,
        received,
    }
}

/// Builds empty blocks so tests can drive consensus without an execution layer.
#[derive(Clone, Default)]
pub struct EmptyBlockBuilder;

impl PayloadBuilder for EmptyBlockBuilder {
    fn build_payload(
        &self,
        request: &BuildPayloadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<BuiltPayload, String>> + Send>> {
        let result = build_empty_block(request);
        Box::pin(async move { result })
    }

    fn validate_block(
        &self,
        block_bytes: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult, String>> + Send>> {
        let result = validate_empty_block(&block_bytes);
        Box::pin(async move { result })
    }
}

/// Build a transaction-less block for `request`.
pub fn build_empty_block(request: &BuildPayloadRequest) -> Result<BuiltPayload, String> {
    let number = request.parent_number + 1;
    let inner = alloy_consensus::Header {
        parent_hash: request.parent_hash,
        ommers_hash: B256::ZERO,
        beneficiary: Address::ZERO,
        state_root: B256::ZERO,
        // keccak256 of an empty RLP list.
        transactions_root: keccak256([0xc0]),
        receipts_root: B256::ZERO,
        logs_bloom: Bloom::ZERO,
        difficulty: U256::ZERO,
        number,
        gas_limit: 30_000_000,
        gas_used: 0,
        timestamp: request.timestamp,
        extra_data: Default::default(),
        mix_hash: B256::ZERO,
        nonce: B64::ZERO,
        base_fee_per_gas: Some(0),
        withdrawals_root: None,
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None,
        requests_hash: None,
        block_access_list_hash: None,
        slot_number: None,
    };

    let header = AllegroHeader {
        inner,
        timestamp_millis: request.timestamp_millis,
        consensus_context: Some(AllegroConsensusContext {
            epoch: request.epoch,
            view: request.view,
            parent_view: request.parent_view,
            proposer: request.proposer,
        }),
    };
    let block_hash = header.hash_slow();

    let block = AlloyBlock {
        header,
        body: BlockBody {
            transactions: Vec::<TxEnvelope>::new(),
            ommers: Vec::new(),
            withdrawals: None,
        },
    };

    Ok(BuiltPayload {
        block_bytes: alloy_rlp::encode(&block),
        block_hash,
        block_number: number,
        timestamp: request.timestamp,
        timestamp_millis: request.timestamp_millis,
    })
}

/// Accept a block that decodes and carries consensus metadata.
fn validate_empty_block(block_bytes: &[u8]) -> Result<ValidationResult, String> {
    let block: AlloyBlock<TxEnvelope, AllegroHeader> =
        alloy_rlp::Decodable::decode(&mut &block_bytes[..])
            .map_err(|e| format!("rlp decode: {e}"))?;

    let header = &block.header;
    if header.consensus_context.is_none() {
        return Ok(ValidationResult::Invalid(
            "missing consensus context".into(),
        ));
    }

    // reth's engine API rejects far-future timestamps natively; mirror it so
    // tests exercise the same behaviour.
    const ALLOWED_TIMESTAMP_DRIFT_SECS: u64 = 15;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if header.inner.timestamp > now + ALLOWED_TIMESTAMP_DRIFT_SECS {
        return Ok(ValidationResult::Invalid(format!(
            "timestamp {} is too far in the future (now: {}, max drift: {}s)",
            header.inner.timestamp, now, ALLOWED_TIMESTAMP_DRIFT_SECS
        )));
    }

    Ok(ValidationResult::Valid(BlockMeta {
        hash: header.hash_slow(),
        parent_hash: header.inner.parent_hash,
        number: header.inner.number,
        timestamp: header.inner.timestamp,
        timestamp_millis: header.timestamp_millis,
    }))
}
