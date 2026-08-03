//! End-to-end tests: consensus block production.
//!
//! Tests 1-4: application actor lifecycle + types (tokio runtime)
//! Test 5:   simplex engine with simulated network (deterministic runtime)

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use allegro_consensus::application::{self as app_actor, Mailbox};
use allegro_consensus::{
    config::ConsensusConfig, start_simplex_engine, Block, BuildPayloadRequest, EngineConfig,
    ValidatorEntry, ValidatorSet,
};

use allegro_primitives::{AllegroConsensusContext, AllegroHeader, Digest};

mod common;
use alloy_consensus::Header;
use alloy_primitives::B256;
use alloy_rlp::Decodable;
use common::EmptyBlockBuilder;
use commonware_codec::{DecodeExt, Encode};
use commonware_consensus::{
    simplex::types::Context,
    types::{Epoch, Round, View},
    Automaton,
};
use commonware_cryptography::{
    ed25519::{PrivateKey, PublicKey},
    Signer as _,
};
use commonware_runtime::{deterministic, Clock, Metrics, Runner};

// ── Helpers ─────────────────────────────────────────────────

/// Simple no-op blocker for tests that don't use a real p2p network.
#[derive(Clone)]
struct NoopBlocker;
impl commonware_p2p::Blocker for NoopBlocker {
    type PublicKey = commonware_cryptography::ed25519::PublicKey;
    async fn block(&mut self, _peer: Self::PublicKey) {}
}

fn make_validator(seed: u8, port: u16) -> ValidatorEntry {
    let sk = PrivateKey::from_seed(seed as u64);
    let pk = sk.public_key();
    ValidatorEntry {
        public_key: pk,
        ingress: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        egress: IpAddr::V4(Ipv4Addr::LOCALHOST),
    }
}

fn spawn_actor(validators: ValidatorSet) -> (Mailbox, app_actor::ReceivedBlocks) {
    let h = common::spawn_actor(validators, Arc::new(EmptyBlockBuilder), None);
    (h.mailbox, h.received)
}

// ═══════════════════════════════════════════════════════════
//  Test 1: Block production via application actor
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn test_consensus_block_production() {
    let entries: Vec<ValidatorEntry> = (0..4).map(|i| make_validator(i, 3000 + i as u16)).collect();
    let validators = ValidatorSet::from_entries(&entries);
    let (mut mailbox, _received) = spawn_actor(validators.clone());

    let genesis = mailbox.genesis(Epoch::new(0)).await;
    assert_eq!(genesis, Digest(B256::ZERO));

    let ctx1 = Context {
        round: Round::new(Epoch::new(0), View::new(1)),
        leader: entries[0].public_key.clone(),
        parent: (View::new(0), genesis),
    };
    let rx = mailbox.propose(ctx1.clone()).await;
    let b1 = rx.await.expect("block 1");
    assert_ne!(b1, genesis);

    // Known proposer → accepted
    let rx = mailbox.verify(ctx1.clone(), b1).await;
    assert!(rx.await.unwrap());

    // Unknown proposer → rejected
    let bad_ctx = Context {
        round: Round::new(Epoch::new(0), View::new(2)),
        leader: PrivateKey::from_seed(999).public_key(),
        parent: (View::new(1), b1),
    };
    let rx = mailbox
        .verify(bad_ctx, Digest(B256::from([0xff; 32])))
        .await;
    assert!(!rx.await.unwrap());

    // Second proposal from a different known validator
    let ctx2 = Context {
        round: Round::new(Epoch::new(0), View::new(3)),
        leader: entries[1].public_key.clone(),
        parent: (View::new(1), b1),
    };
    let rx = mailbox.propose(ctx2.clone()).await;
    let b2 = rx.await.expect("block 2");
    assert_ne!(b2, b1);

    let rx = mailbox.verify(ctx2.clone(), b2).await;
    assert!(rx.await.unwrap());
}

// ═══════════════════════════════════════════════════════════
//  Crafted-block verification
// ═══════════════════════════════════════════════════════════

/// Build an empty block for `req`, hand it to the actor as a received block,
/// and ask it to verify against the consensus parent `ctx_parent`.
async fn verify_crafted(
    mailbox: &Mailbox,
    received: &app_actor::ReceivedBlocks,
    leader: PublicKey,
    req: BuildPayloadRequest,
    ctx_parent: (View, Digest),
) -> bool {
    let view = req.view;
    let payload = common::build_empty_block(&req).expect("build crafted block");
    let digest = Digest(payload.block_hash);
    received
        .write()
        .unwrap()
        .insert(digest, payload.block_bytes);
    let ctx = Context {
        round: Round::new(Epoch::new(0), View::new(view)),
        leader,
        parent: ctx_parent,
    };
    let mut mb = mailbox.clone();
    mb.verify(ctx, digest).await.await.unwrap()
}

/// timestamp_millis must agree with the seconds timestamp.
#[tokio::test]
async fn test_verify_rejects_inconsistent_timestamp_millis() {
    let entries: Vec<ValidatorEntry> = (0..4).map(|i| make_validator(i, 3200 + i as u16)).collect();
    let validators = ValidatorSet::from_entries(&entries);
    let (mailbox, received) = spawn_actor(validators.clone());

    let genesis = mailbox.clone().genesis(Epoch::new(0)).await;
    let now = common::now_secs();
    let proposer = allegro_primitives::ProposerKey::from(&entries[1].public_key);

    let verify_ts = |timestamp: u64, timestamp_millis: u64, view: u64| {
        verify_crafted(
            &mailbox,
            &received,
            entries[1].public_key.clone(),
            BuildPayloadRequest {
                parent_hash: genesis.0,
                parent_number: 0,
                parent_view: 1,
                parent_digest: genesis,
                epoch: 0,
                view,
                proposer,
                timestamp,
                timestamp_millis,
            },
            (View::new(0), genesis),
        )
    };

    // Millis pinned an hour past the seconds field → rejected
    assert!(
        !verify_ts(now, (now + 3600) * 1000, 1).await,
        "timestamp_millis disagreeing with timestamp must be rejected"
    );

    // Millis regressed below the seconds field → rejected
    assert!(
        !verify_ts(now, (now - 5) * 1000, 2).await,
        "lagging timestamp_millis must be rejected"
    );

    // Consistent, including sub-second precision → accepted
    assert!(
        verify_ts(now, now * 1000 + 999, 3).await,
        "consistent timestamp_millis must be accepted"
    );
}

/// The block must extend the parent consensus chose.
#[tokio::test]
async fn test_verify_rejects_wrong_parent() {
    let entries: Vec<ValidatorEntry> = (0..4).map(|i| make_validator(i, 3300 + i as u16)).collect();
    let validators = ValidatorSet::from_entries(&entries);
    let (mailbox, received) = spawn_actor(validators.clone());

    let genesis = mailbox.clone().genesis(Epoch::new(0)).await;
    let now = common::now_secs();

    let crafted = |parent_hash: B256, view: u64| BuildPayloadRequest {
        parent_hash,
        parent_number: 0,
        parent_view: 0,
        parent_digest: genesis,
        epoch: 0,
        view,
        proposer: allegro_primitives::ProposerKey::from(&entries[1].public_key),
        timestamp: now,
        timestamp_millis: allegro_consensus::millis_from_secs(now),
    };
    let leader = entries[1].public_key.clone();

    // Internally valid, but extends a parent other than the one consensus
    // names in the verify context.
    assert!(
        !verify_crafted(
            &mailbox,
            &received,
            leader.clone(),
            crafted(B256::from([0xaa; 32]), 1),
            (View::new(0), genesis),
        )
        .await,
        "a block extending a different parent must be rejected"
    );

    // A parent we hold no record of is still checkable: every block takes its
    // own hash as its digest, so the digest is the hash. Rejected here because
    // the crafted block extends genesis, not the named parent.
    let unrecorded = Digest(B256::from([0xbb; 32]));
    assert!(
        !verify_crafted(
            &mailbox,
            &received,
            leader.clone(),
            crafted(genesis.0, 2),
            (View::new(1), unrecorded),
        )
        .await,
        "a block extending a different parent must be rejected"
    );

    // ...and accepted when it does extend it. A node that restarted with an
    // empty block-info map sees every parent this way; refusing would leave it
    // unable to ever vote again.
    assert!(
        verify_crafted(
            &mailbox,
            &received,
            leader,
            crafted(unrecorded.0, 3),
            (View::new(1), unrecorded),
        )
        .await,
        "a block extending an unrecorded parent must still be verifiable"
    );
}

// ═══════════════════════════════════════════════════════════
//  Test 2: Proposer key conversion
// ═══════════════════════════════════════════════════════════

#[tokio::test]
async fn test_proposer_key_conversion() {
    let sk = PrivateKey::from_seed(42);
    let pk = sk.public_key();
    let ctx = AllegroConsensusContext::from_commonware_proposer(1, 7, 6, &pk);
    let header = AllegroHeader {
        inner: Header {
            number: 100,
            timestamp: 1_700_000_000,
            ..Default::default()
        },
        timestamp_millis: 1_700_000_000_000,
        consensus_context: Some(ctx),
    };
    let encoded = alloy_rlp::encode(&header);
    let decoded = AllegroHeader::decode(&mut encoded.as_slice()).unwrap();
    let recovered = decoded.consensus_context.unwrap().proposer_commonware();
    assert_eq!(recovered, pk);
}

// ═══════════════════════════════════════════════════════════
//  Test 3: Block codec roundtrip
// ═══════════════════════════════════════════════════════════

#[test]
fn test_block_codec() {
    let hash = B256::from([0x42; 32]);
    let block = Block::new(vec![10, 20, 30], hash);
    let encoded = block.encode();
    let decoded = Block::decode(encoded).unwrap();
    assert_eq!(block.hash(), decoded.hash());
    assert_eq!(block.raw(), decoded.raw());
}

// ═══════════════════════════════════════════════════════════
//  Test 4: Validator set
// ═══════════════════════════════════════════════════════════

#[test]
fn test_validator_set() {
    let entries: Vec<ValidatorEntry> = (0..4).map(|i| make_validator(i, 3000 + i as u16)).collect();
    let set = ValidatorSet::from_entries(&entries);
    assert_eq!(set.len(), 4);
    assert!(set.lookup(&entries[0].public_key).is_some());
    assert!(set
        .lookup(&PrivateKey::from_seed(999).public_key())
        .is_none());
}

// ═══════════════════════════════════════════════════════════
//  Test 5: Simplex engine runs on deterministic runtime
// ═══════════════════════════════════════════════════════════
//
// Uses inert channels to verify the engine builds and starts
// without panicking.

#[test]
fn test_simplex_engine_initializes() {
    use commonware_p2p::utils::mocks::inert_channel;

    let executor = deterministic::Runner::default();
    executor.start(|context| async move {
        let sk = PrivateKey::from_seed(0);
        let pk = sk.public_key();
        let entry = ValidatorEntry {
            public_key: pk,
            ingress: "127.0.0.1:3000".parse().unwrap(),
            egress: IpAddr::V4(Ipv4Addr::LOCALHOST),
        };
        let validators = ValidatorSet::from_entries(std::slice::from_ref(&entry));

        let participants: Vec<_> = validators.keys();
        let (v_tx, v_rx) = inert_channel(&participants);
        let (c_tx, c_rx) = inert_channel(&participants);
        let (r_tx, r_rx) = inert_channel(&participants);

        let cfg = EngineConfig {
            signing_key: sk,
            validators,
            consensus_config: ConsensusConfig::default(),
            proposals: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            partition: "allegro".into(),
            payload_builder: Arc::new(EmptyBlockBuilder),
            metrics: None,
            genesis_hash: B256::ZERO,
            genesis_timestamp: 0,
            genesis_timestamp_millis: 0,
            finalized_tx: None,
        };

        let (b_tx, b_rx) = inert_channel(&participants);
        let blocker = NoopBlocker;
        let started = start_simplex_engine(
            context.with_label("engine"),
            cfg,
            ((v_tx, v_rx), (c_tx, c_rx), (r_tx, r_rx)),
            b_tx,
            b_rx,
            blocker,
        )
        .expect("engine should start");
        let handle = started.task;
        context.sleep(Duration::from_millis(100)).await;
        drop(handle);
    });
}

// ═══════════════════════════════════════════════════════════
//  Test 6: Simplex engine with loopback channels (tokio)
// ═══════════════════════════════════════════════════════════
//
// Uses in-process Sender/Receiver that deliver messages locally.
// A single validator should be able to reach consensus.

#[test]
fn test_simplex_engine_loopback() {
    use allegro_consensus::loopback::loopback_channel;

    let _ = tracing_subscriber::fmt::try_init();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::new(
            "allegro=warn,commonware=warn",
        ))
        .try_init();

    let runner = commonware_runtime::tokio::Runner::new(commonware_runtime::tokio::Config::new());

    runner.start(|context| async move {
        let sk = PrivateKey::from_seed(0);
        let pk = sk.public_key();
        let pk_for_channel = pk.clone();
        let entry = ValidatorEntry {
            public_key: pk,
            ingress: "127.0.0.1:3000".parse().unwrap(),
            egress: IpAddr::V4(Ipv4Addr::LOCALHOST),
        };
        let validators = ValidatorSet::from_entries(std::slice::from_ref(&entry));

        let (vote_tx, vote_rx) = loopback_channel(pk_for_channel.clone(), 1024);
        let (cert_tx, cert_rx) = loopback_channel(pk_for_channel.clone(), 1024);
        let (res_tx, res_rx) = loopback_channel(pk_for_channel.clone(), 1024);

        let cfg = EngineConfig {
            signing_key: sk,
            validators,
            consensus_config: ConsensusConfig {
                mailbox_size: 1024,
                leader_timeout: Duration::from_millis(500),
                certification_timeout: Duration::from_secs(1),
                timeout_retry: Duration::from_millis(200),
                fetch_timeout: Duration::from_secs(1),
                ..ConsensusConfig::default()
            },
            proposals: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            partition: "allegro".into(),
            payload_builder: Arc::new(EmptyBlockBuilder),
            metrics: None,
            genesis_hash: B256::ZERO,
            genesis_timestamp: 0,
            genesis_timestamp_millis: 0,
            finalized_tx: None,
        };

        let proposals_test = cfg.proposals.clone();

        let (b_tx, b_rx) = loopback_channel(pk_for_channel.clone(), 1024);
        let blocker = NoopBlocker;
        let started = start_simplex_engine(
            context.with_label("engine"),
            cfg,
            ((vote_tx, vote_rx), (cert_tx, cert_rx), (res_tx, res_rx)),
            b_tx,
            b_rx,
            blocker,
        )
        .expect("engine should start");
        let handle = started.task;

        context.sleep(Duration::from_secs(3)).await;
        drop(handle);

        let proposed = proposals_test.lock().unwrap();
        eprintln!("loopback proposals: {}", proposed.len());
    });
}

// ═══════════════════════════════════════════════════════════
//  Test 7: Two validators on simulated network
// ═══════════════════════════════════════════════════════════
//
// Each validator gets its own Sender/Receiver from the
// simulated p2p network. Messages flow between validators
// through the simulated network.

#[test]
fn test_two_validators_simulated() {
    use commonware_p2p::simulated::{Config as SimConfig, Network as SimNetwork};

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::new("allegro=warn"))
        .try_init();

    let runner = deterministic::Runner::default();
    runner.start(|context| async move {
        // 2 validator keys
        let keys: Vec<PrivateKey> = (0..2).map(|i| PrivateKey::from_seed(i as u64)).collect();
        let pks: Vec<_> = keys.iter().map(|sk| sk.public_key()).collect();

        // Simulated network with all peers
        let (network, oracle) = SimNetwork::new_with_peers(
            context.with_label("sim_net"),
            SimConfig {
                max_size: 1024 * 1024,
                disconnect_on_block: true,
                tracked_peer_sets: std::num::NonZeroUsize::new(3).unwrap(),
            },
            pks.clone(),
        )
        .await;

        network.start();

        // Track all peers
        let mut mgr = oracle.manager();
        let peer_set = commonware_utils::ordered::Set::try_from(pks.clone()).unwrap();
        commonware_p2p::Manager::track(&mut mgr, 0, peer_set).await;
        drop(mgr);

        // Validator entries and set
        let entries: Vec<ValidatorEntry> = keys
            .iter()
            .enumerate()
            .map(|(i, sk)| ValidatorEntry {
                public_key: sk.public_key(),
                ingress: format!("127.0.0.1:{}", 3000 + i).parse().unwrap(),
                egress: IpAddr::V4(Ipv4Addr::LOCALHOST),
            })
            .collect();
        let vs = ValidatorSet::from_entries(&entries);

        let quota = commonware_runtime::Quota::per_second(std::num::NonZeroU32::new(1024).unwrap());

        // Shared proposal log across all validators
        let all_proposals = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        // Register channels + start engine for each validator
        let mut _handles = Vec::new();
        for i in 0..2 {
            let control = oracle.control(pks[i].clone());
            let (v_tx, v_rx) = control.register(0, quota).await.unwrap();
            let (c_tx, c_rx) = control.register(1, quota).await.unwrap();
            let (r_tx, r_rx) = control.register(2, quota).await.unwrap();
            let (b_tx, b_rx) = control.register(3, quota).await.unwrap();
            // The simulated control implements Blocker
            let blocker = control.clone();

            let cfg = EngineConfig {
                signing_key: keys[i].clone(),
                validators: vs.clone(),
                consensus_config: ConsensusConfig {
                    leader_timeout: Duration::from_secs(1),
                    certification_timeout: Duration::from_secs(2),
                    timeout_retry: Duration::from_millis(500),
                    ..ConsensusConfig::default()
                },
                proposals: all_proposals.clone(),
                partition: format!("allegro_{i}"),
                payload_builder: Arc::new(EmptyBlockBuilder),
                metrics: None,
                genesis_hash: B256::ZERO,
                genesis_timestamp: 0,
                genesis_timestamp_millis: 0,
                finalized_tx: None,
            };

            let started = start_simplex_engine(
                context.with_label(&format!("engine_{i}")),
                cfg,
                ((v_tx, v_rx), (c_tx, c_rx), (r_tx, r_rx)),
                b_tx,
                b_rx,
                blocker,
            )
            .expect("engine should start");
            _handles.push(started);
        }

        context.sleep(Duration::from_secs(5)).await;

        // Verify blocks were produced
        let proposed = all_proposals.lock().unwrap();
        eprintln!(
            "allegro: two validators, total proposals: {}",
            proposed.len()
        );
        assert!(!proposed.is_empty(), "engine should have proposed blocks");
        for (i, d) in proposed.iter().enumerate() {
            eprintln!("  proposal {}: {}", i, d);
        }
    });
}

// ═══════════════════════════════════════════════════════════
//  Test 8: Empty validator set rejected
// ═══════════════════════════════════════════════════════════
//
// Verify that an empty validator set is rejected with an error.

#[test]
fn test_empty_validator_set_rejected() {
    use commonware_p2p::utils::mocks::inert_channel;

    let executor = deterministic::Runner::default();
    executor.start(|context| async move {
        let sk = PrivateKey::from_seed(0);
        let validators = ValidatorSet::new();

        let participants: Vec<_> = validators.keys();
        let (v_tx, v_rx) = inert_channel(&participants);
        let (c_tx, c_rx) = inert_channel(&participants);
        let (r_tx, r_rx) = inert_channel(&participants);
        let (b_tx, b_rx) = inert_channel(&participants);
        let blocker = NoopBlocker;

        let cfg = EngineConfig {
            signing_key: sk,
            validators,
            consensus_config: ConsensusConfig::default(),
            proposals: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            partition: "allegro".into(),
            payload_builder: Arc::new(EmptyBlockBuilder),
            metrics: None,
            genesis_hash: B256::ZERO,
            genesis_timestamp: 0,
            genesis_timestamp_millis: 0,
            finalized_tx: None,
        };

        let result = start_simplex_engine(
            context.with_label("engine"),
            cfg,
            ((v_tx, v_rx), (c_tx, c_rx), (r_tx, r_rx)),
            b_tx,
            b_rx,
            blocker,
        );

        assert!(result.is_err(), "empty validator set should be rejected");
    });
}
