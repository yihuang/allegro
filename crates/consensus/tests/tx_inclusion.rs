//! E2E test: transaction broadcast and block inclusion.
//!
//! Uses the real [`allegro_consensus::create_reth_payload_builder`] integration
//! point with closures that build blocks from a shared transaction pool.
//! This tests the actual production code path through `EngineApiPayloadBuilder`.

use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use allegro_consensus::{
    config::ConsensusConfig,
    create_reth_payload_builder,
    executor::{BuildPayloadRequest, ValidateBlockRequest},
    start_simplex_engine, BlockMeta, BuiltPayload, EngineConfig, ValidationResult, ValidatorEntry,
    ValidatorSet,
};
use alloy_consensus::{BlockBody, Sealable, Signed, TxEnvelope, TxLegacy};
use alloy_primitives::{b256, keccak256, Address, Bytes, Signature, B256, U256};
use alloy_rlp::Decodable;
use commonware_cryptography::{ed25519::PrivateKey, Signer as _};
use commonware_p2p::simulated::{Config as SimConfig, Link, Network as SimNetwork};
use commonware_runtime::{deterministic, Clock, Metrics, Runner};
use tracing::debug;

use allegro_primitives::{AllegroConsensusContext, AllegroHeader, Digest, ProposerKey};

// ── Transaction pool ───────────────────────────────────────

#[derive(Clone, Default)]
struct TxPool {
    inner: Arc<Mutex<Vec<Vec<u8>>>>,
}
impl TxPool {
    fn new() -> Self {
        Self::default()
    }
    fn submit(&self, tx: Vec<u8>) {
        self.inner.lock().expect("lock").push(tx);
    }
    fn drain(&self) -> Vec<Vec<u8>> {
        std::mem::take(&mut *self.inner.lock().expect("lock"))
    }
}

// ── Block building / validation ────────────────────────────

fn dummy_sig() -> Signature {
    Signature::from_scalars_and_parity(
        b256!("0000000000000000000000000000000000000000000000000000000000000001"),
        b256!("0000000000000000000000000000000000000000000000000000000000000001"),
        false,
    )
}

fn tx_envelope(data: Vec<u8>) -> TxEnvelope {
    TxEnvelope::Legacy(Signed::new_unhashed(
        TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: alloy_primitives::TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Bytes::from(data),
        },
        dummy_sig(),
    ))
}

fn encode_txs_list(txs: &[TxEnvelope]) -> Vec<u8> {
    let mut buf = Vec::new();
    alloy_rlp::encode_list::<TxEnvelope, TxEnvelope>(txs, &mut buf);
    buf
}

fn compute_txs_root(txs: &[TxEnvelope]) -> B256 {
    keccak256(encode_txs_list(txs))
}

#[allow(clippy::too_many_arguments)]
fn build_block(
    parent_hash: B256,
    parent_number: u64,
    parent_view: u64,
    epoch: u64,
    view: u64,
    proposer: [u8; 32],
    timestamp: u64,
    txs: Vec<TxEnvelope>,
) -> Result<BuiltPayload, String> {
    let root = compute_txs_root(&txs);
    let inner = alloy_consensus::Header {
        parent_hash,
        ommers_hash: B256::ZERO,
        beneficiary: Address::ZERO,
        state_root: B256::ZERO,
        transactions_root: root,
        receipts_root: B256::ZERO,
        logs_bloom: alloy_primitives::Bloom::ZERO,
        difficulty: U256::ZERO,
        number: parent_number + 1,
        gas_limit: 30_000_000,
        gas_used: 0,
        timestamp,
        extra_data: Default::default(),
        mix_hash: B256::ZERO,
        nonce: alloy_primitives::B64::ZERO,
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
        timestamp_millis: timestamp * 1000,
        consensus_context: Some(AllegroConsensusContext {
            epoch,
            view,
            parent_view,
            proposer: ProposerKey(proposer),
        }),
    };
    let block_hash = header.hash_slow();
    let block = alloy_consensus::Block {
        header,
        body: BlockBody {
            transactions: txs,
            ommers: Vec::new(),
            withdrawals: None,
        },
    };
    Ok(BuiltPayload {
        block_bytes: alloy_rlp::encode(&block),
        block_hash,
        block_number: parent_number + 1,
        timestamp_millis: block.header.timestamp_millis,
    })
}

fn validate_block(block_bytes: &[u8]) -> Result<ValidationResult, String> {
    let block: alloy_consensus::Block<TxEnvelope, AllegroHeader> =
        Decodable::decode(&mut &block_bytes[..]).map_err(|e| format!("rlp: {e}"))?;
    if block.header.consensus_context.is_none() {
        return Ok(ValidationResult::Invalid("no consensus context".into()));
    }
    let actual = compute_txs_root(&block.body.transactions);
    if block.header.inner.transactions_root != actual {
        return Ok(ValidationResult::Invalid(format!(
            "txs_root mismatch: header={}, actual={}",
            block.header.inner.transactions_root, actual
        )));
    }
    let hash = block.header.hash_slow();
    Ok(ValidationResult::Valid(BlockMeta {
        hash,
        number: block.header.inner.number,
        timestamp: block.header.inner.timestamp,
        timestamp_millis: block.header.timestamp_millis,
    }))
}

// ── Recording wrapper (test-only, layers on real EngineApiPayloadBuilder) ──

struct RecordingBuilder {
    inner: allegro_consensus::executor::EngineApiPayloadBuilder,
    recorded: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl allegro_consensus::PayloadBuilder for RecordingBuilder {
    fn build_payload(
        &self,
        req: &BuildPayloadRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<BuiltPayload, String>> + Send>>
    {
        let rec = self.recorded.clone();
        let fut = self.inner.build_payload(req);
        Box::pin(async move {
            let r = fut.await;
            if let Ok(ref p) = r {
                rec.lock().expect("lock").push(p.block_bytes.clone());
            }
            r
        })
    }
    fn validate_block(
        &self,
        b: Vec<u8>,
        h: B256,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ValidationResult, String>> + Send>>
    {
        self.inner.validate_block(b, h)
    }
}

// ── Inspection ─────────────────────────────────────────────

fn decode_txs(b: &[u8]) -> Result<Vec<TxEnvelope>, String> {
    let block: alloy_consensus::Block<TxEnvelope, AllegroHeader> =
        Decodable::decode(&mut &b[..]).map_err(|e| format!("rlp: {e}"))?;
    Ok(block.body.transactions)
}

fn extract_data(tx: &TxEnvelope) -> &[u8] {
    match tx {
        TxEnvelope::Legacy(s) => s.tx().input.as_ref(),
        TxEnvelope::Eip2930(s) => s.tx().input.as_ref(),
        TxEnvelope::Eip1559(s) => s.tx().input.as_ref(),
        _ => &[],
    }
}

// ── Network helpers ────────────────────────────────────────

const UQ: commonware_runtime::Quota = commonware_runtime::Quota::per_second(NonZeroU32::MAX);

fn link() -> Link {
    Link {
        latency: Duration::ZERO,
        jitter: Duration::from_millis(1),
        success_rate: 1.0,
    }
}

async fn link_all(
    o: &commonware_p2p::simulated::Oracle<
        commonware_cryptography::ed25519::PublicKey,
        deterministic::Context,
    >,
    ps: &[commonware_cryptography::ed25519::PublicKey],
) {
    for a in ps {
        for b in ps {
            if a != b {
                o.add_link(a.clone(), b.clone(), link()).await.unwrap();
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════
//  Test: Transactions are included in consensus blocks
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_tx_inclusion_via_reth_payload_builder() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::new("allegro=warn"))
        .try_init();

    let cfg = ConsensusConfig {
        mailbox_size: 4096,
        leader_timeout: Duration::from_millis(1000),
        certification_timeout: Duration::from_millis(2000),
        timeout_retry: Duration::from_millis(500),
        ..ConsensusConfig::default()
    };

    deterministic::Runner::default().start(|context| async move {
        // ── 3 validators ──
        let n = 3;
        let keys: Vec<PrivateKey> = (0..n).map(|i| PrivateKey::from_seed(i as u64)).collect();
        let pks: Vec<_> = keys.iter().map(|sk| sk.public_key()).collect();
        let vs = ValidatorSet::from_entries(
            &(0..n)
                .map(|i| ValidatorEntry {
                    public_key: pks[i].clone(),
                    ingress: format!("127.0.0.1:{}", 3000 + i).parse().unwrap(),
                    egress: "127.0.0.1".parse().unwrap(),
                })
                .collect::<Vec<_>>(),
        );

        // ── Network ──
        let (net, oracle) = SimNetwork::new_with_peers(
            context.with_label("net"),
            SimConfig {
                max_size: 10 << 20,
                disconnect_on_block: true,
                tracked_peer_sets: std::num::NonZeroUsize::new(3).unwrap(),
            },
            pks.clone(),
        )
        .await;
        net.start();
        {
            let mut m = oracle.manager();
            commonware_p2p::Manager::track(
                &mut m,
                0,
                commonware_utils::ordered::Set::try_from(pks.clone()).unwrap(),
            )
            .await;
        }
        link_all(&oracle, &pks).await;

        // ── Shared pool + per-validator recorded blocks ──
        let pool = TxPool::new();
        let recorded: Vec<Arc<Mutex<Vec<Vec<u8>>>>> =
            (0..n).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let proposals: Vec<Arc<Mutex<Vec<Digest>>>> =
            (0..n).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();

        let mut _h = Vec::with_capacity(n);
        for i in 0..n {
            let ctrl = oracle.control(pks[i].clone());
            let (v_tx, v_rx) = ctrl.register(0, UQ).await.unwrap();
            let (c_tx, c_rx) = ctrl.register(1, UQ).await.unwrap();
            let (r_tx, r_rx) = ctrl.register(2, UQ).await.unwrap();
            let (b_tx, b_rx) = ctrl.register(3, UQ).await.unwrap();
            let blk = ctrl.clone();

            // Real integration: create_reth_payload_builder with closures
            let p = pool.clone();
            let build = move |req: BuildPayloadRequest| {
                let pool = p.clone();
                Box::pin(async move {
                    let txs = pool.drain().into_iter().map(tx_envelope).collect();
                    build_block(
                        req.parent_hash,
                        req.parent_number,
                        req.parent_view,
                        req.epoch,
                        req.view,
                        req.proposer,
                        req.timestamp,
                        txs,
                    )
                })
                    as std::pin::Pin<
                        Box<dyn std::future::Future<Output = Result<BuiltPayload, String>> + Send>,
                    >
            };
            let validate = move |req: ValidateBlockRequest| {
                Box::pin(async move { validate_block(&req.block_bytes) })
                    as std::pin::Pin<
                        Box<
                            dyn std::future::Future<Output = Result<ValidationResult, String>>
                                + Send,
                        >,
                    >
            };

            let recording = Arc::new(RecordingBuilder {
                inner: create_reth_payload_builder(build, validate),
                recorded: recorded[i].clone(),
            });

            _h.push(
                start_simplex_engine(
                    context.with_label(&format!("e{i}")),
                    EngineConfig {
                        signing_key: keys[i].clone(),
                        validators: vs.clone(),
                        consensus_config: cfg.clone(),
                        proposals: proposals[i].clone(),
                        partition: format!("a{i}"),
                        payload_builder: Some(
                            recording as Arc<dyn allegro_consensus::PayloadBuilder>,
                        ),
                        metrics: None,
                        genesis_hash: B256::ZERO,
                        genesis_timestamp: 0,
                        genesis_timestamp_millis: 0,
                        finalized_tx: None,
                    },
                    ((v_tx, v_rx), (c_tx, c_rx), (r_tx, r_rx)),
                    b_tx,
                    b_rx,
                    blk,
                )
                .expect("engine start"),
            );
        }

        // ── Submit txs in batches ──
        let txs: Vec<Vec<u8>> = vec![b"tx_a".to_vec(), b"tx_b".to_vec(), b"tx_c".to_vec()];

        context.sleep(Duration::from_secs(3)).await; // let consensus start
        pool.submit(txs[0].clone());
        pool.submit(txs[1].clone());
        context.sleep(Duration::from_secs(5)).await; // leader picks them up
        pool.submit(txs[2].clone());
        context.sleep(Duration::from_secs(6)).await; // tx3 gets included

        // ── Verify ──
        for (i, p) in proposals.iter().enumerate() {
            assert!(!p.lock().unwrap().is_empty(), "val{i}: 0 proposals");
        }

        let mut found = [false, false, false];
        let mut total = 0usize;
        for (i, b) in recorded.iter().enumerate() {
            let list = b.lock().unwrap();
            total += list.len();
            debug!("val{i} built {} blocks", list.len());
            for (j, bytes) in list.iter().enumerate() {
                let txs_in_block = decode_txs(bytes).unwrap_or_default();
                if !txs_in_block.is_empty() {
                    debug!("  block{j}: {} tx(s)", txs_in_block.len());
                }
                for (k, tx_data) in txs.iter().enumerate() {
                    if txs_in_block
                        .iter()
                        .any(|e| extract_data(e) == tx_data.as_slice())
                    {
                        found[k] = true;
                        debug!("  block{j} has tx{k}");
                    }
                }
            }
        }

        assert!(total >= 3, ">=3 blocks, got {total}");
        assert!(found[0], "tx_a not found");
        assert!(found[1], "tx_b not found");
        assert!(found[2], "tx_c not found");
        debug!("all 3 txs found in consensus blocks");
    });
}

// ═══════════════════════════════════════════════════════════════
//  Test: Empty blocks are valid
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_empty_block_is_valid() {
    let block = build_block(B256::ZERO, 0, 0, 0, 0, [0u8; 32], 100, vec![]).expect("build");
    assert!(matches!(
        validate_block(&block.block_bytes),
        Ok(ValidationResult::Valid(_))
    ));
}

// ═══════════════════════════════════════════════════════════════
//  Test: Block with a single transaction
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_block_with_one_tx_is_valid() {
    let txs = vec![tx_envelope(b"hello".to_vec())];
    let block = build_block(B256::ZERO, 0, 0, 0, 0, [0u8; 32], 100, txs).expect("build");
    assert!(matches!(
        validate_block(&block.block_bytes),
        Ok(ValidationResult::Valid(_))
    ));
    let decoded = decode_txs(&block.block_bytes).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(extract_data(&decoded[0]), b"hello");
}

// ═══════════════════════════════════════════════════════════════
//  Test: Multiple blocks each carry only their own txs
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_blocks_contain_only_drained_txs() {
    // Block A: 1 tx
    let txs_a = vec![tx_envelope(b"only_in_a".to_vec())];
    let block_a = build_block(B256::ZERO, 0, 0, 0, 0, [0u8; 32], 100, txs_a).expect("build_a");
    assert_eq!(decode_txs(&block_a.block_bytes).unwrap().len(), 1);

    // Block B: different tx (only_in_a must not appear)
    let txs_b = vec![tx_envelope(b"only_in_b".to_vec())];
    let block_b =
        build_block(block_a.block_hash, 1, 0, 0, 1, [1u8; 32], 200, txs_b).expect("build_b");
    let decoded_b = decode_txs(&block_b.block_bytes).unwrap();
    assert_eq!(decoded_b.len(), 1);
    assert_eq!(extract_data(&decoded_b[0]), b"only_in_b");
}

// ═══════════════════════════════════════════════════════════════
//  Test: Truncated block fails validation
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_truncated_block_fails_validation() {
    let payload = build_block(B256::ZERO, 0, 0, 0, 0, [0u8; 32], 100, vec![]).expect("build");
    // Truncate to corrupt
    let truncated = payload.block_bytes[..payload.block_bytes.len().saturating_sub(10)].to_vec();
    assert!(validate_block(&truncated).is_err());
}
