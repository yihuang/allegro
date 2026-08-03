//! Preparing the next view's payload ahead of the proposal request.
//!
//! Drives the application actor directly through its mailbox, so these tests
//! observe what the actor asks the payload builder for and when, without
//! standing up a consensus engine.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use allegro_consensus::application::LeaderSchedule;
use allegro_consensus::{BuildPayloadRequest, BuiltPayload, PayloadBuilder, ValidationResult};
use allegro_primitives::Digest;
use alloy_primitives::B256;
use commonware_consensus::{
    simplex::elector::RoundRobin,
    types::{Epoch, Round, View},
    Automaton,
};
use commonware_cryptography::ed25519::PublicKey;
use commonware_utils::ordered::Set;

mod common;
use common::{
    build_empty_block, build_validators, context, now_millis, EmptyBlockBuilder, Harness,
};

fn validators() -> (allegro_consensus::ValidatorSet, Set<PublicKey>) {
    build_validators(2, 4000)
}

fn schedule_for(participants: &Set<PublicKey>, me: &PublicKey) -> Option<LeaderSchedule> {
    Some(LeaderSchedule::new(
        RoundRobin::default(),
        participants,
        me.clone(),
    ))
}

/// The participant the engine's elector puts in `view`'s leader slot —
/// derived through the schedule itself, so these tests survive an elector
/// change; the rotation formula is pinned once, in the unit tests.
fn leader_of(participants: &Set<PublicKey>, view: u64) -> PublicKey {
    participants
        .iter()
        .find(|p| {
            schedule_for(participants, p)
                .expect("schedule")
                .leads(round(view))
        })
        .expect("some participant leads every view")
        .clone()
}

fn round(view: u64) -> Round {
    Round::new(Epoch::new(0), View::new(view))
}

// ── Recording builder ───────────────────────────────────────

/// Stands in for the reth builder: remembers the request it was asked to
/// prepare and reuses it only when a later proposal names the same parent.
#[derive(Clone, Default)]
struct RecordingBuilder {
    prepared: Arc<Mutex<Option<BuildPayloadRequest>>>,
    /// Every request ever prepared, in order.
    prepares: Arc<Mutex<Vec<BuildPayloadRequest>>>,
    hits: Arc<AtomicUsize>,
    misses: Arc<AtomicUsize>,
}

impl RecordingBuilder {
    fn prepares(&self) -> Vec<BuildPayloadRequest> {
        self.prepares.lock().unwrap().clone()
    }

    fn last_prepare(&self) -> BuildPayloadRequest {
        self.prepares().pop().expect("a payload was prepared")
    }

    fn hits(&self) -> usize {
        self.hits.load(Ordering::Relaxed)
    }

    fn misses(&self) -> usize {
        self.misses.load(Ordering::Relaxed)
    }
}

impl PayloadBuilder for RecordingBuilder {
    fn build_payload(
        &self,
        request: &BuildPayloadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<BuiltPayload, String>> + Send>> {
        // Mirrors the reth builder: a prepared job is consumed either way, and
        // only reused when it was built on the parent consensus asked for.
        let prepared = self
            .prepared
            .lock()
            .unwrap()
            .take()
            .filter(|p| p.parent_hash == request.parent_hash);

        let effective = match prepared {
            Some(prepared) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                prepared
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                request.clone()
            }
        };

        // The block carries the timestamps of whichever request actually built
        // it — frozen at prepare time on a hit.
        let result = build_empty_block(&effective);
        Box::pin(async move { result })
    }

    fn validate_block(
        &self,
        block_bytes: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<ValidationResult, String>> + Send>> {
        EmptyBlockBuilder.validate_block(block_bytes)
    }

    fn prepare_payload(
        &self,
        request: &BuildPayloadRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        *self.prepared.lock().unwrap() = Some(request.clone());
        self.prepares.lock().unwrap().push(request.clone());
        Box::pin(async {})
    }
}

/// Drive view 1 to a verified block, which is what triggers preparation of
/// view 2. Returns the builder, the harness, and the view-1 block.
async fn through_view_1(
    leader_schedule: impl Fn(&Set<PublicKey>) -> Option<LeaderSchedule>,
) -> ViewOne {
    let (validators, participants) = validators();
    let builder = RecordingBuilder::default();
    let mut harness = common::spawn_actor(
        validators,
        Arc::new(builder.clone()),
        leader_schedule(&participants),
    );

    let genesis = harness.mailbox.genesis(Epoch::new(0)).await;
    let ctx = context(
        round(1),
        leader_of(&participants, 1),
        (View::new(0), genesis),
    );
    let block = harness
        .mailbox
        .propose(ctx.clone())
        .await
        .await
        .expect("proposed");
    assert!(harness
        .mailbox
        .verify(ctx, block)
        .await
        .await
        .expect("verified"));
    ViewOne {
        builder,
        harness,
        view_2_leader: leader_of(&participants, 2),
        block,
    }
}

struct ViewOne {
    builder: RecordingBuilder,
    harness: Harness,
    /// The participant that will lead view 2.
    view_2_leader: PublicKey,
    /// The block verified in view 1 — the parent view 2 is expected to use.
    block: Digest,
}

// ═══════════════════════════════════════════════════════════
//  Preparation
// ═══════════════════════════════════════════════════════════

/// Verifying a block prepares the next view's payload when this node leads it,
/// against the block just verified.
#[tokio::test]
async fn verify_prepares_the_next_view_for_its_leader() {
    let v = through_view_1(|p| schedule_for(p, &leader_of(p, 2))).await;

    let last = v.builder.last_prepare();
    assert_eq!(last.view, 2, "prepared the wrong view");
    assert_eq!(last.parent_hash, v.block.0, "prepared on the wrong parent");
}

/// A node that does not lead the next view prepares nothing.
#[tokio::test]
async fn non_leader_of_the_next_view_prepares_nothing() {
    let v = through_view_1(|p| {
        let bystander = p
            .iter()
            .find(|k| **k != leader_of(p, 2))
            .expect("someone does not lead view 2");
        schedule_for(p, bystander)
    })
    .await;
    assert!(
        v.builder.prepares().is_empty(),
        "prepared a view it does not lead"
    );
}

/// Without a leader schedule the actor never speculates.
#[tokio::test]
async fn no_schedule_means_no_preparation() {
    let v = through_view_1(|_| None).await;
    assert!(v.builder.prepares().is_empty());
    assert_eq!(v.builder.hits(), 0);
}

// ═══════════════════════════════════════════════════════════
//  Reuse and fallback
// ═══════════════════════════════════════════════════════════

/// The proposal reuses the prepared payload, and records the timestamps the
/// block actually carries rather than the ones it asked for.
#[tokio::test]
async fn proposal_reuses_the_prepared_payload() {
    let mut v = through_view_1(|p| schedule_for(p, &leader_of(p, 2))).await;
    let me = v.view_2_leader.clone();
    let prepared_millis = v.builder.last_prepare().timestamp_millis;

    // Let the clock move on, so a cold build would stamp a later time and the
    // assertion below can tell the two apart.
    tokio::time::sleep(Duration::from_millis(30)).await;
    let before_propose = now_millis();
    assert!(before_propose > prepared_millis);

    let block = v
        .harness
        .mailbox
        .propose(context(round(2), me, (View::new(1), v.block)))
        .await
        .await
        .expect("proposed");

    assert_eq!(v.builder.hits(), 1, "prepared payload was not reused");

    let info = v
        .harness
        .block_info
        .read()
        .unwrap()
        .get(&block)
        .cloned()
        .expect("recorded");
    assert_eq!(
        info.timestamp_millis, prepared_millis,
        "block info kept the requested timestamp instead of the block's own"
    );
    assert!(
        info.timestamp_millis < before_propose,
        "payload was rebuilt from cold"
    );
    assert_eq!(info.timestamp, prepared_millis / 1000);
}

/// A payload prepared on one parent is never proposed on another.
#[tokio::test]
async fn prepared_payload_for_another_parent_is_discarded() {
    let mut v = through_view_1(|p| schedule_for(p, &leader_of(p, 2))).await;
    let me = v.view_2_leader.clone();
    assert_eq!(v.builder.last_prepare().parent_hash, v.block.0);

    // The view-1 block never made it: view 2 builds on genesis instead.
    let hits_before = v.builder.hits();
    let genesis = Digest(B256::ZERO);
    let block = v
        .harness
        .mailbox
        .propose(context(round(2), me, (View::new(0), genesis)))
        .await
        .await
        .expect("proposed");

    assert_eq!(
        v.builder.hits(),
        hits_before,
        "reused a payload for the wrong parent"
    );
    assert!(v.builder.misses() >= 1, "no cold build happened");

    let info = v
        .harness
        .block_info
        .read()
        .unwrap()
        .get(&block)
        .cloned()
        .expect("recorded");
    assert_eq!(info.number, 1, "did not build on genesis");
}
