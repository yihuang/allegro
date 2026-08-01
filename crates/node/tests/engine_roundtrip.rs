//! Integration test: single-node reth engine roundtrip.
//!
//! Launches a real reth node in-process and drives the full consensus-side
//! engine flow through the production payload builder closures:
//!
//! 1. **build**   — FCU(genesis, attrs) → payload id → resolve → self-import
//! 2. **validate** — decode wire bytes → `new_payload` → Valid + block meta
//! 3. **finalize** — FCU(head=safe=finalized=block)
//! 4. **build#2** — proves the canonical head actually advanced

use std::time::{Duration, SystemTime};

use allegro_consensus::{
    executor::{BuildPayloadRequest, ValidationResult},
    PayloadBuilder,
};
use allegro_node::{
    builder::{create_engine_payload_builder, ForkchoiceTracker},
    chainspec::dev_chainspec,
    launch::{launch, RethNodeConfig},
};
use allegro_primitives::Digest as AllegroDigest;
use alloy_rpc_types_engine::ForkchoiceState;
use commonware_cryptography::Digest as _;

/// Pick a free TCP port by binding to port 0 temporarily.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.port()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn build_validate_finalize_first_block() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("allegro=debug,reth::=info")
        .with_test_writer()
        .try_init();

    // ── 1. Launch a real reth node in-process ──
    let datadir = tempfile::tempdir().expect("tempdir");
    let cfg = RethNodeConfig {
        datadir: datadir.path().to_path_buf(),
        http_port: free_port(),
        authrpc_port: free_port(),
        p2p_port: free_port(),
        chain: dev_chainspec(),
    };
    let runtime = reth_tasks::Runtime::test();

    let launched = tokio::time::timeout(Duration::from_secs(120), launch(cfg, runtime))
        .await
        .expect("reth node launch timed out")
        .expect("reth node launch failed");
    eprintln!("node launched, genesis = {}", launched.genesis_hash);

    let tracker = ForkchoiceTracker::new(launched.genesis_hash);
    let builder = create_engine_payload_builder(
        launched.engine_handle(),
        launched.payload_builder_handle(),
        tracker.clone(),
        None,
    );

    // ── 2. Build block 1 on top of genesis ──
    let timestamp = now_secs().max(launched.genesis_timestamp + 1);
    let timestamp_millis = timestamp * 1000;
    let built = tokio::time::timeout(
        Duration::from_secs(30),
        builder.build_payload(&BuildPayloadRequest {
            parent_hash: launched.genesis_hash,
            parent_number: 0,
            parent_view: 0,
            parent_digest: AllegroDigest::EMPTY,
            epoch: 0,
            view: 1,
            proposer: [0u8; 32].into(),
            timestamp,
            timestamp_millis,
        }),
    )
    .await
    .expect("build_payload timed out")
    .expect("build_payload failed");
    eprintln!(
        "built block 1: {} ({})",
        built.block_number, built.block_hash
    );
    assert_eq!(built.block_number, 1);
    assert_ne!(built.block_hash, alloy_primitives::B256::ZERO);

    // ── 3. Validate block 1 (as a peer would) ──
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        builder.validate_block(built.block_bytes.clone(), launched.genesis_hash),
    )
    .await
    .expect("validate_block timed out")
    .expect("validate_block failed");
    match result {
        ValidationResult::Valid(meta) => {
            assert_eq!(meta.hash, built.block_hash);
            assert_eq!(meta.number, 1);
            eprintln!("validated block 1: ts = {}", meta.timestamp);
        }
        ValidationResult::Invalid(reason) => panic!("block 1 invalid: {reason}"),
    }

    // ── 4. Finalize block 1 ──
    let state = ForkchoiceState {
        head_block_hash: built.block_hash,
        safe_block_hash: built.block_hash,
        finalized_block_hash: built.block_hash,
    };
    let fcu = tokio::time::timeout(
        Duration::from_secs(30),
        launched.engine_handle().fork_choice_updated(state, None),
    )
    .await
    .expect("finalization FCU timed out")
    .expect("finalization FCU failed");
    assert!(
        fcu.payload_status.is_valid(),
        "finalization FCU not valid: {}",
        fcu.payload_status
    );
    tracker.set_finalized(built.block_hash, built.block_number);
    eprintln!("finalized block 1");

    // ── 5. Build block 2 on top of block 1 — proves the head advanced ──
    let timestamp2 = now_secs().max(timestamp + 1);
    let timestamp2_millis = timestamp2 * 1000;
    let built2 = tokio::time::timeout(
        Duration::from_secs(30),
        builder.build_payload(&BuildPayloadRequest {
            parent_hash: built.block_hash,
            parent_number: 1,
            parent_view: 1,
            parent_digest: AllegroDigest(built.block_hash),
            epoch: 0,
            view: 2,
            proposer: [0u8; 32].into(),
            timestamp: timestamp2,
            timestamp_millis: timestamp2_millis,
        }),
    )
    .await
    .expect("build_payload #2 timed out")
    .expect("build_payload #2 failed");
    eprintln!(
        "built block 2: {} ({})",
        built2.block_number, built2.block_hash
    );
    assert_eq!(built2.block_number, 2);

    // ── 6. Prepare block 3 ahead of the proposal, then propose it ──
    let prepared_ts = now_secs().max(timestamp2 + 1);
    let prepare_request = BuildPayloadRequest {
        parent_hash: built2.block_hash,
        parent_number: 2,
        parent_view: 2,
        parent_digest: AllegroDigest(built2.block_hash),
        epoch: 0,
        view: 3,
        proposer: [0u8; 32].into(),
        timestamp: prepared_ts,
        timestamp_millis: prepared_ts * 1000,
    };
    tokio::time::timeout(
        Duration::from_secs(30),
        builder.prepare_payload(&prepare_request),
    )
    .await
    .expect("prepare_payload timed out");

    // Ask for a far later timestamp than the prepared job froze. Reusing the
    // job means the block carries the prepared timestamp, so the two cannot be
    // confused for one another.
    let built3 = tokio::time::timeout(
        Duration::from_secs(30),
        builder.build_payload(&BuildPayloadRequest {
            timestamp: prepared_ts + 100,
            timestamp_millis: (prepared_ts + 100) * 1000,
            ..prepare_request
        }),
    )
    .await
    .expect("build_payload #3 timed out")
    .expect("build_payload #3 failed");
    eprintln!(
        "built block 3: {} ({}) ts = {}",
        built3.block_number, built3.block_hash, built3.timestamp
    );
    assert_eq!(built3.block_number, 3);
    assert_eq!(
        built3.timestamp, prepared_ts,
        "proposal rebuilt from cold instead of reusing the prepared job"
    );
    assert_eq!(built3.timestamp_millis, prepared_ts * 1000);
}
