//! Multi-node consensus tests: several validators run through consensus
//! rounds over commonware's simulated p2p network.
//!
//! All tests use `deterministic::Runner`, which advances time only when
//! `context.sleep()` is called, so they step time in small increments to let
//! the engine process messages between views.

mod common;

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::B256;

use allegro_consensus::{
    config::ConsensusConfig, start_simplex_engine, ConsensusMetrics, EngineConfig, ValidatorEntry,
    ValidatorSet,
};
use allegro_primitives::Digest;
use commonware_cryptography::{
    ed25519::{PrivateKey, PublicKey},
    Signer as _,
};
use commonware_p2p::simulated::{Config as SimConfig, Link, Network as SimNetwork};
use commonware_runtime::{deterministic, Clock, Runner, Supervisor};
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
    start_engines_full(context, keys, validator_set, cfg)
        .await
        .0
}

/// Like [`start_engines`], but also hands back each node's block-info map so a
/// test can inspect the `(view, proposer)` pairs the node observed.
async fn start_engines_full(
    context: &deterministic::Context,
    keys: &[PrivateKey],
    validator_set: &ValidatorSet,
    cfg: &ConsensusConfig,
) -> (
    Vec<Arc<std::sync::Mutex<Vec<Digest>>>>,
    Vec<allegro_consensus::application::BlockInfoMap>,
) {
    let n = keys.len();
    let pks: Vec<_> = keys.iter().map(|sk| sk.public_key()).collect();

    // Create simulated network
    let (network, oracle) = SimNetwork::new_with_peers(
        context.child("sim_net"),
        SimConfig {
            max_size: 10 * 1024 * 1024,
            disconnect_on_block: true,
            max_peers_per_set: std::num::NonZeroUsize::new(64).unwrap(),
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
        commonware_p2p::Manager::track(&mut mgr, 0, peer_set);
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
            payload_builder: Arc::new(common::EmptyBlockBuilder),
            metrics: None,
            genesis_hash: B256::ZERO,
            genesis_timestamp: 0,
            genesis_timestamp_millis: 0,
            finalized_tx: None,
        };

        let started = start_simplex_engine(
            context.child("engine").with_attribute("index", i),
            engine_cfg,
            ((v_tx, v_rx), (c_tx, c_rx), (r_tx, r_rx)),
            b_tx,
            b_rx,
            blocker,
        )
        .expect("engine should start");
        _handles.push(started);
    }

    let block_infos: Vec<_> = _handles.iter().map(|se| se.block_info.clone()).collect();

    // Keep engines alive by leaking the handles (they live for the test duration)
    // Convert StartedEngine → Handle<()> for leaking
    let _leak = Box::leak(Box::new(
        _handles.into_iter().map(|se| se.task).collect::<Vec<_>>(),
    ));

    (proposal_logs, block_infos)
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
//  MN6: Pipelined simplex (stable leaders + optimistic validation)
// ═══════════════════════════════════════════════════════════════

/// Stable-leader terms with optimistic validation (the pipelined simplex
/// variant) produce blocks end-to-end.
#[test]
fn test_pipelined_stable_leader_produces_blocks() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::new("allegro=warn"))
        .try_init();

    let n = 3;
    let cfg = ConsensusConfig {
        term_length: 8,
        stall_timeout: Duration::from_secs(60),
        optimistic_views: 8,
        leader_timeout: Duration::from_millis(500),
        certification_timeout: Duration::from_millis(1000),
        timeout_retry: Duration::from_millis(200),
        fetch_timeout: Duration::from_millis(1000),
        ..ConsensusConfig::default()
    };

    deterministic::Runner::default().start(|context| async move {
        let (keys, validator_set) = build_validators(n);
        let logs = start_engines(&context, &keys, &validator_set, &cfg).await;
        advance_time(
            &context,
            Duration::from_secs(10),
            Duration::from_millis(100),
        )
        .await;

        let total: usize = logs.iter().map(|l| l.lock().unwrap().len()).sum();
        eprintln!("pipelined: total proposals = {total}");

        assert!(total >= n, "expected >= {n} proposals, got {total}");
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
            context.child("sim_net"),
            SimConfig {
                max_size: 10 * 1024 * 1024,
                disconnect_on_block: true,
                max_peers_per_set: std::num::NonZeroUsize::new(64).unwrap(),
                tracked_peer_sets: std::num::NonZeroUsize::new(3).unwrap(),
            },
            pks.clone(),
        )
        .await;
        network.start();

        {
            let mut mgr = oracle.manager();
            let peer_set = commonware_utils::ordered::Set::try_from(pks.clone()).unwrap();
            commonware_p2p::Manager::track(&mut mgr, 0, peer_set);
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
                payload_builder: Arc::new(common::EmptyBlockBuilder),
                metrics: my_metrics,
                genesis_hash: B256::ZERO,
                genesis_timestamp: 0,
                genesis_timestamp_millis: 0,
                finalized_tx: None,
            };

            let started = start_simplex_engine(
                context.child("engine").with_attribute("index", i),
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

// ═══════════════════════════════════════════════════════════════
//  MN6: Stable leaders serve a term of consecutive views
//
//  Covers commonwarexyz/monorepo#3352 (stable leaders) and #3416
//  (optimistic proposal and validation).
// ═══════════════════════════════════════════════════════════════

/// Fraction of adjacent view pairs `(v, v+1)` that share a proposer.
/// Round-robin gives ~0; terms of length `T` give roughly `(T - 1) / T`.
fn same_proposer_run_ratio(block_info: &allegro_consensus::application::BlockInfoMap) -> f64 {
    let mut by_view: Vec<(u64, PublicKey)> = block_info
        .read()
        .unwrap()
        .values()
        // View 0 is the genesis record the actor seeds at startup, not a proposal.
        .filter(|info| info.view > 0)
        .map(|info| (info.view, info.proposer.clone()))
        .collect();
    by_view.sort_by_key(|(view, _)| *view);
    by_view.dedup_by_key(|(view, _)| *view);

    let adjacent: Vec<bool> = by_view
        .windows(2)
        .filter(|w| w[1].0 == w[0].0 + 1)
        .map(|w| w[1].1 == w[0].1)
        .collect();
    assert!(
        adjacent.len() >= 8,
        "need consecutive views to measure leader runs, got {} pairs",
        adjacent.len()
    );
    adjacent.iter().filter(|same| **same).count() as f64 / adjacent.len() as f64
}

/// `term_length > 1` keeps one leader across a run of views, and
/// `optimistic_views` lets participants notarize ahead of certified ancestry
/// within that run. Both default to off, so this is the test that covers them.
///
/// Proposal totals even out across terms, so the check compares the
/// adjacent-view run ratio against a round-robin baseline instead.
#[test]
fn test_stable_leader_serves_consecutive_views() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::new("allegro=warn"))
        .try_init();

    let n = 4;
    let run_for = Duration::from_secs(10);
    let stable = ConsensusConfig {
        term_length: 8,
        // Must exceed the certification timeout; generous here so a term is
        // never abandoned mid-run.
        stall_timeout: Duration::from_secs(30),
        optimistic_views: 4,
        ..test_config()
    };
    let rotating = test_config();

    let measure = |cfg: ConsensusConfig| {
        deterministic::Runner::default().start(|context| async move {
            let (keys, validator_set) = build_validators(n);
            let (_logs, block_infos) =
                start_engines_full(&context, &keys, &validator_set, &cfg).await;
            advance_time(&context, run_for, Duration::from_millis(100)).await;
            same_proposer_run_ratio(&block_infos[0])
        })
    };

    let stable_ratio = measure(stable);
    let rotating_ratio = measure(rotating);
    debug!("same-proposer run ratio: stable={stable_ratio:.3} rotating={rotating_ratio:.3}");

    // Steady state is 7/8; the slack covers term boundaries and nullified views.
    assert!(
        stable_ratio > 0.5,
        "8-view terms should keep the same proposer across most adjacent views, got {stable_ratio:.3}"
    );
    // Rotation only repeats a proposer when a view is skipped, rare on perfect links.
    assert!(
        rotating_ratio < 0.2,
        "rotation should change proposer nearly every view, got {rotating_ratio:.3}"
    );
}
