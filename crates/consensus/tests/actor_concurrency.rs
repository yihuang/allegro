//! The application actor handles messages concurrently.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use allegro_consensus::{BuildPayloadRequest, BuiltPayload, PayloadBuilder, ValidationResult};
use allegro_primitives::Digest;
use alloy_primitives::B256;
use commonware_consensus::{
    types::{Epoch, Round, View},
    Automaton,
};

mod common;
use common::{
    build_empty_block, build_validators, context, now_millis, EmptyBlockBuilder, Harness,
};

/// Parks the first build until released, so a test can observe what the actor
/// does while a proposal is outstanding.
struct GatedBuilder {
    gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl PayloadBuilder for GatedBuilder {
    fn build_payload(
        &self,
        request: &BuildPayloadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<BuiltPayload, String>> + Send>> {
        let gate = self.gate.lock().unwrap().take();
        let started = self.started.lock().unwrap().take();
        let result = build_empty_block(request);
        Box::pin(async move {
            if let Some(started) = started {
                let _ = started.send(());
            }
            if let Some(gate) = gate {
                let _ = gate.await;
            }
            result
        })
    }

    fn validate_block(
        &self,
        block_bytes: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult, String>> + Send>> {
        EmptyBlockBuilder.validate_block(block_bytes)
    }
}

/// A block from a peer, ready to be verified.
fn insert_peer_block(h: &Harness, genesis: Digest) -> Digest {
    let block = build_empty_block(&BuildPayloadRequest {
        parent_hash: B256::ZERO,
        parent_number: 0,
        parent_view: 0,
        parent_digest: genesis,
        epoch: 0,
        view: 1,
        proposer: [7u8; 32].into(),
        timestamp: now_millis() / 1000,
        timestamp_millis: now_millis(),
    })
    .expect("built");
    let digest = Digest(block.block_hash);
    h.received
        .write()
        .unwrap()
        .insert(digest, block.block_bytes);
    digest
}

/// An outstanding build does not stall verification of another block. Under a
/// sequential actor loop the verify below would never be answered.
#[tokio::test]
async fn verify_is_answered_while_a_build_is_outstanding() {
    let (validators, participants) = build_validators(2, 4100);
    let leader = participants.get(0).unwrap().clone();

    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let mut h = common::spawn_actor(
        validators,
        Arc::new(GatedBuilder {
            gate: Mutex::new(Some(gate_rx)),
            started: Mutex::new(Some(started_tx)),
        }),
        None,
    );

    let genesis = h.mailbox.genesis(Epoch::new(0)).await;
    let peer_digest = insert_peer_block(&h, genesis);

    // Park a proposal inside the payload builder.
    let round = |view| Round::new(Epoch::new(0), View::new(view));
    let propose_rx = h
        .mailbox
        .propose(context(round(2), leader.clone(), (View::new(0), genesis)))
        .await;
    started_rx.await.expect("build started");

    // The build is still parked; verification must not be queued behind it.
    let verify_rx = h
        .mailbox
        .verify(
            context(round(1), leader, (View::new(0), genesis)),
            peer_digest,
        )
        .await;
    let verified = tokio::time::timeout(Duration::from_secs(5), verify_rx)
        .await
        .expect("verify blocked behind the outstanding build")
        .expect("verify responded");
    assert!(verified);

    gate_tx.send(()).expect("release build");
    propose_rx.await.expect("proposed");
}

/// The cap is honoured: with room for one handler, the parked build does hold
/// verification up — which is what makes it backpressure rather than a knob
/// that reads well and does nothing.
#[tokio::test]
async fn a_cap_of_one_serializes_handlers() {
    let (validators, participants) = build_validators(2, 4200);
    let leader = participants.get(0).unwrap().clone();

    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let mut h = common::spawn_actor_limited(
        validators,
        Arc::new(GatedBuilder {
            gate: Mutex::new(Some(gate_rx)),
            started: Mutex::new(Some(started_tx)),
        }),
        None,
        1,
    );

    let genesis = h.mailbox.genesis(Epoch::new(0)).await;
    let peer_digest = insert_peer_block(&h, genesis);

    let round = |view| Round::new(Epoch::new(0), View::new(view));
    let propose_rx = h
        .mailbox
        .propose(context(round(2), leader.clone(), (View::new(0), genesis)))
        .await;
    started_rx.await.expect("build started");

    let verify_rx = h
        .mailbox
        .verify(
            context(round(1), leader, (View::new(0), genesis)),
            peer_digest,
        )
        .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), verify_rx)
            .await
            .is_err(),
        "cap of one still let a second handler run"
    );

    gate_tx.send(()).expect("release build");
    propose_rx.await.expect("proposed");
}
