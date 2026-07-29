//! Multi-node consensus e2e tests.
//!
//! Uses commonware's simulated p2p network to run multiple validators
//! through consensus rounds on the deterministic runtime and verify
//! block production.
//!
//! # Deterministic Runtime
//!
//! All tests use `deterministic::Runner` which advances time only when
//! `context.sleep()` is called. Tests advance time in small increments
//! to allow the engine to process messages between views.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::B256;

use allegro_consensus::{
    config::ConsensusConfig, start_simplex_engine, ConsensusMetrics, EngineConfig, ValidatorEntry,
    ValidatorSet,
};
use allegro_primitives::Digest;
use commonware_cryptography::{ed25519::PrivateKey, Signer as _};
use commonware_p2p::simulated::{Config as SimConfig, Link, Network as SimNetwork};
use commonware_runtime::{deterministic, Clock, Metrics, Runner};
use tracing::debug;

/// Unlimited quota for simulated network (matching commonware's test pattern).
const UNLIMITED_QUOTA: commonware_runtime::Quota =
    commonware_runtime::Quota::per_second(NonZeroU32::MAX);

/// Create a perfect link config: zero latency, minimal jitter, 100% delivery.
fn perfect_link() -> Link {
    Link {
        latency: Duration::from_millis(0),
        jitter: Duration::from_millis(1),
        success_rate: 1.0,
    }
}

/// Advance the deterministic runtime in small steps so the engine
/// can process messages between increments.
async fn advance_time(context: &deterministic::Context, total: Duration, step: Duration) {
    let steps = total.as_millis() / step.as_millis();
    for _ in 0..steps {
        context.sleep(step).await;
    }
    let remainder = total.as_millis() % step.as_millis();
    if remainder > 0 {
        context.sleep(Duration::from_millis(remainder as u64)).await;
    }
}

/// Build N validator keys and set.
fn build_validators(n: usize) -> (Vec<PrivateKey>, ValidatorSet) {
    let keys: Vec<PrivateKey> = (0..n).map(|i| PrivateKey::from_seed(i as u64)).collect();
    let entries: Vec<ValidatorEntry> = (0..n)
        .map(|i| ValidatorEntry {
            public_key: keys[i].public_key(),
            ingress: format!("127.0.0.1:{}", 3000 + i).parse().unwrap(),
            egress: "127.0.0.1".parse().unwrap(),
        })
        .collect();
    let set = ValidatorSet::from_entries(&entries);
    (keys, set)
}

/// Create bidirectional perfect links between all pairs of participants.
async fn link_all_participants(
    oracle: &commonware_p2p::simulated::Oracle<
        commonware_cryptography::ed25519::PublicKey,
        deterministic::Context,
    >,
    participants: &[commonware_cryptography::ed25519::PublicKey],
) {
    for v1 in participants.iter() {
        for v2 in participants.iter() {
            if v2 == v1 {
                continue;
            }
            oracle
                .add_link(v1.clone(), v2.clone(), perfect_link())
                .await
                .expect("add_link");
        }
    }
}

/// Start N simplex engines on a simulated network and return proposal logs.
async fn start_engines(
    context: &deterministic::Context,
    keys: &[PrivateKey],
    validator_set: &ValidatorSet,
    cfg: &ConsensusConfig,
) -> Vec<Arc<std::sync::Mutex<Vec<Digest>>>> {
    let n = keys.len();
    let pks: Vec<_> = keys.iter().map(|sk| sk.public_key()).collect();

    // Create simulated network
    let (network, oracle) = SimNetwork::new_with_peers(
        context.with_label("sim_net"),
        SimConfig {
            max_size: 10 * 1024 * 1024,
            disconnect_on_block: true,
            tracked_peer_sets: std::num::NonZeroUsize::new(3).unwrap(),
        },
        pks.clone(),
    )
    .await;
    network.start();

    // Track peers
    {
        let mut mgr = oracle.manager();
        let peer_set = commonware_utils::ordered::Set::try_from(pks.clone()).unwrap();
        commonware_p2p::Manager::track(&mut mgr, 0, peer_set).await;
    }

    // Add bidirectional links between all peers (required for message delivery)
    link_all_participants(&oracle, &pks).await;

    let proposal_logs: Vec<_> = (0..n)
        .map(|_| Arc::new(std::sync::Mutex::new(Vec::new())))
        .collect();

    let mut _handles = Vec::with_capacity(n);
    for i in 0..n {
        let control = oracle.control(pks[i].clone());

        let (v_tx, v_rx) = control.register(0, UNLIMITED_QUOTA).await.unwrap();
        let (c_tx, c_rx) = control.register(1, UNLIMITED_QUOTA).await.unwrap();
        let (r_tx, r_rx) = control.register(2, UNLIMITED_QUOTA).await.unwrap();
        let (b_tx, b_rx) = control.register(3, UNLIMITED_QUOTA).await.unwrap();
        let blocker = control.clone();

        let engine_cfg = EngineConfig {
            signing_key: keys[i].clone(),
            validators: validator_set.clone(),
            consensus_config: cfg.clone(),
            proposals: proposal_logs[i].clone(),
            partition: format!("allegro_{i}"),
            payload_builder: None,
            metrics: None,
            genesis_hash: B256::ZERO,
            genesis_timestamp: 0,
            genesis_timestamp_millis: 0,
            finalized_tx: None,
        };

        let started = start_simplex_engine(
            context.with_label(&format!("engine_{i}")),
            engine_cfg,
            ((v_tx, v_rx), (c_tx, c_rx), (r_tx, r_rx)),
            b_tx,
            b_rx,
            blocker,
        )
        .expect("engine should start");
        _handles.push(started);
    }

    // Keep engines alive by leaking the handles (they live for the test duration)
    // Convert StartedEngine → Handle<()> for leaking
    let _leak = Box::leak(Box::new(
        _handles.into_iter().map(|se| se.task).collect::<Vec<_>>(),
    ));

    proposal_logs
}

/// Default test config tuned for fast deterministic execution.
fn test_config() -> ConsensusConfig {
    ConsensusConfig {
        mailbox_size: 4096,
        leader_timeout: Duration::from_millis(1000),
        certification_timeout: Duration::from_millis(2000),
        timeout_retry: Duration::from_millis(500),
        fetch_timeout: Duration::from_millis(1000),
        ..ConsensusConfig::default()
    }
}

// ═══════════════════════════════════════════════════════════════
//  MN1: 3 validators produce blocks
// ═══════════════════════════════════════════════════════════════

/// Three validators (N=3, f=1) with round-robin leader election.
#[test]
fn test_three_validators_produce_blocks() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::new("allegro=warn"))
        .try_init();

    let n = 3;
    let cfg = test_config();

    deterministic::Runner::default().start(|context| async move {
        let (keys, validator_set) = build_validators(n);
        let logs = start_engines(&context, &keys, &validator_set, &cfg).await;
        advance_time(
            &context,
            Duration::from_secs(12),
            Duration::from_millis(100),
        )
        .await;

        let total: usize = logs.iter().map(|l| l.lock().unwrap().len()).sum();
        debug!("3v: total proposals = {total}");

        assert!(total >= n, "expected >= {n} proposals, got {total}");

        for (i, log) in logs.iter().enumerate() {
            let count = log.lock().unwrap().len();
            assert!(count >= 1, "validator {i}: 0 proposals (total={total})");
        }
    });
}

// ═══════════════════════════════════════════════════════════════
//  MN2: 4 validators produce blocks
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_four_validators_produce_blocks() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::new("allegro=warn"))
        .try_init();

    let n = 4;
    let cfg = ConsensusConfig {
        leader_timeout: Duration::from_secs(1),
        certification_timeout: Duration::from_secs(2),
        ..test_config()
    };

    deterministic::Runner::default().start(|context| async move {
        let (keys, validator_set) = build_validators(n);
        let logs = start_engines(&context, &keys, &validator_set, &cfg).await;
        advance_time(
            &context,
            Duration::from_secs(15),
            Duration::from_millis(100),
        )
        .await;

        let total: usize = logs.iter().map(|l| l.lock().unwrap().len()).sum();
        debug!("4v: total proposals = {total}");

        assert!(total >= n, "expected >= {n} proposals, got {total}");

        for (i, log) in logs.iter().enumerate() {
            let count = log.lock().unwrap().len();
            assert!(count >= 1, "validator {i}: 0 proposals (total={total})");
        }
    });
}

// ═══════════════════════════════════════════════════════════════
//  MN3: 2 validators reach consensus
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_two_validators_reach_consensus() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::new("allegro=warn"))
        .try_init();

    let cfg = ConsensusConfig {
        mailbox_size: 4096,
        leader_timeout: Duration::from_millis(800),
        certification_timeout: Duration::from_millis(1600),
        ..test_config()
    };

    deterministic::Runner::default().start(|context| async move {
        let (keys, validator_set) = build_validators(2);
        let logs = start_engines(&context, &keys, &validator_set, &cfg).await;
        advance_time(&context, Duration::from_secs(8), Duration::from_millis(100)).await;

        let total: usize = logs.iter().map(|l| l.lock().unwrap().len()).sum();
        debug!("2v: total proposals = {total}");

        assert!(
            total >= 2,
            "expected >= 2 proposals across 2 validators, got {total}"
        );
    });
}

// ═══════════════════════════════════════════════════════════════
//  MN4: Proposals are unique per-validator
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_proposals_are_unique_per_validator() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::new("allegro=warn"))
        .try_init();

    deterministic::Runner::default().start(|context| async move {
        let (keys, validator_set) = build_validators(3);
        let logs = start_engines(&context, &keys, &validator_set, &test_config()).await;
        advance_time(
            &context,
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await;

        for (i, log) in logs.iter().enumerate() {
            let proposals = log.lock().unwrap();
            let mut seen = std::collections::HashSet::new();
            for d in proposals.iter() {
                assert!(seen.insert(*d), "validator {i} duplicate {d}");
            }
        }
    });
}

// ═══════════════════════════════════════════════════════════════
//  MN5: Metrics track proposals correctly
// ═══════════════════════════════════════════════════════════════

#[test]
fn test_metrics_track_proposals() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::new("allegro=warn"))
        .try_init();

    let cfg = ConsensusConfig {
        mailbox_size: 4096,
        leader_timeout: Duration::from_millis(800),
        certification_timeout: Duration::from_millis(1600),
        ..test_config()
    };

    deterministic::Runner::default().start(|context| async move {
        let (keys, validator_set) = build_validators(2);
        let pks: Vec<_> = keys.iter().map(|sk| sk.public_key()).collect();

        let (network, oracle) = SimNetwork::new_with_peers(
            context.with_label("sim_net"),
            SimConfig {
                max_size: 10 * 1024 * 1024,
                disconnect_on_block: true,
                tracked_peer_sets: std::num::NonZeroUsize::new(3).unwrap(),
            },
            pks.clone(),
        )
        .await;
        network.start();

        {
            let mut mgr = oracle.manager();
            let peer_set = commonware_utils::ordered::Set::try_from(pks.clone()).unwrap();
            commonware_p2p::Manager::track(&mut mgr, 0, peer_set).await;
        }

        // Add bidirectional links
        link_all_participants(&oracle, &pks).await;

        let per_validator_proposals: Vec<_> = (0..2)
            .map(|_| Arc::new(std::sync::Mutex::new(Vec::new())))
            .collect();

        let metrics_v0 = ConsensusMetrics::new();
        let metrics_v1 = ConsensusMetrics::new();

        let mut _handles = Vec::with_capacity(2);
        for i in 0..2 {
            let control = oracle.control(pks[i].clone());
            let (v_tx, v_rx) = control.register(0, UNLIMITED_QUOTA).await.unwrap();
            let (c_tx, c_rx) = control.register(1, UNLIMITED_QUOTA).await.unwrap();
            let (r_tx, r_rx) = control.register(2, UNLIMITED_QUOTA).await.unwrap();
            let (b_tx, b_rx) = control.register(3, UNLIMITED_QUOTA).await.unwrap();
            let blocker = control.clone();

            let my_metrics = if i == 0 {
                Some(metrics_v0.clone())
            } else {
                Some(metrics_v1.clone())
            };

            let engine_cfg = EngineConfig {
                signing_key: keys[i].clone(),
                validators: validator_set.clone(),
                consensus_config: cfg.clone(),
                proposals: per_validator_proposals[i].clone(),
                partition: format!("allegro_{i}"),
                payload_builder: None,
                metrics: my_metrics,
                genesis_hash: B256::ZERO,
                genesis_timestamp: 0,
                genesis_timestamp_millis: 0,
                finalized_tx: None,
            };

            let started = start_simplex_engine(
                context.with_label(&format!("engine_{i}")),
                engine_cfg,
                ((v_tx, v_rx), (c_tx, c_rx), (r_tx, r_rx)),
                b_tx,
                b_rx,
                blocker,
            )
            .expect("engine should start");
            _handles.push(started);
        }

        advance_time(
            &context,
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await;

        let proposed_v0 = metrics_v0.blocks_proposed();
        let proposed_v1 = metrics_v1.blocks_proposed();

        debug!("metrics: val0={proposed_v0}, val1={proposed_v1}");

        // If this fails, the engine instances may not have been kept alive.
        assert!(
            proposed_v0 + proposed_v1 > 0,
            "no proposals in metrics (logs: v0={}, v1={})",
            per_validator_proposals[0].lock().unwrap().len(),
            per_validator_proposals[1].lock().unwrap().len(),
        );

        let log_v0 = per_validator_proposals[0].lock().unwrap().len() as u64;
        let log_v1 = per_validator_proposals[1].lock().unwrap().len() as u64;

        assert!(
            proposed_v0 <= log_v0,
            "val0: metrics {proposed_v0} > log {log_v0}"
        );
        assert!(
            proposed_v1 <= log_v1,
            "val1: metrics {proposed_v1} > log {log_v1}"
        );
    });
}
