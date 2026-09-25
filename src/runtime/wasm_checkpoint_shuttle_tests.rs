//! The WASM guest-state checkpoint protocol, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The ordering invariants one branch's checkpoint is held to while replicas fetch and
//!   confirm it, while an inspection reads its progress, and while the acknowledgements it holds
//!   race the deliveries of the same inputs.
//! - **Depends on.** The production checkpoint state, checkpoint holds, acknowledgement sets and
//!   the server Shuttle runner.
//! - **Must not know.** Guest execution, stable storage, or how a replica reaches the owner.

// The standard library's atomics are not Shuttle scheduling points, so each record below changes in
// the same scheduling step as the operation it records.
use std::sync::{
    Arc as StdArc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClusterNodeName, DomainName, FieldName, ModelKind, ModelName, SchemaFingerprint,
    WasmCheckpointStage, WasmStateGeneration,
};

use super::*;
use crate::{
    runtime::{BranchKey, RuntimeState, wasm_state::WasmCheckpointReplicas},
    runtime_ack::AckOutcome,
    runtime_schema::RuntimeValue,
    shuttle_test::check_interleavings,
};

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";

/// Replicas the modeled checkpoint is captured for.
const REPLICAS: [&str; 2] = ["node-2", "node-3"];
/// Reads the inspecting task takes of the branch's checkpoint progress.
const INSPECTIONS: usize = 3;

fn placement() -> RuntimeStatePlacement {
    RuntimeStatePlacement {
        domain: DomainName::parse("shuttle").assured("a literal domain name is valid"),
        state: RuntimeState::WasmProcessor {
            schema: SchemaFingerprint::from_digest([7; 32]),
            generation: WasmStateGeneration::FIRST,
        },
        kind: ModelKind::WasmProcessor,
        identifier: ModelName::parse("guest").assured("a literal processor name is valid"),
        branch_key: BranchKey::from_fields([(
            FieldName::parse("tenant").assured("a literal field name is valid"),
            RuntimeValue::String("alpha".to_string()),
        )])
        .assured("a branch key with one field is non-empty")
        .into(),
    }
}

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("a literal node name is valid")
}

fn replicas() -> WasmCheckpointReplicas {
    let assigned = REPLICAS.iter().map(|name| node(name)).collect();
    let WasmCheckpointBoundary::Replicas(replicas) = WasmCheckpointBoundary::assigned(assigned)
    else {
        panic!("a non-empty assignment names replicas");
    };
    replicas
}

/// What the tasks of one confirmation model execution record of each other.
#[derive(Default)]
struct ConfirmationRecord {
    /// The revision the branch recorded as locally durable, set just before it publishes it.
    durable_revision: AtomicU64,
    /// One flag per replica, set just before that replica reports holding the revision.
    reported: [AtomicBool; REPLICAS.len()],
    /// Offers a locally durable checkpoint to the replicas, as the owner notifies them once the
    /// checkpoint is on its storage.
    offered: tokio::sync::Notify,
}

impl ConfirmationRecord {
    fn every_replica_reported(&self) -> bool {
        self.reported
            .iter()
            .all(|reported| reported.load(Ordering::SeqCst))
    }
}

/// Capture a checkpoint for two replicas, write it, and wait for both replicas exactly as the
/// owner's confirmation does: register for the next replica report, then read what is still
/// awaited, and sleep only on that registration.
async fn checkpoint_and_confirm(
    state: StdArc<ReplicatedWasmProcessorState>,
    record: StdArc<ConfirmationRecord>,
) {
    let captured = state.capture(vec![1, 2, 3], WasmCheckpointBoundary::Replicas(replicas()));
    let revision = captured.revision();
    record.durable_revision.store(revision, Ordering::SeqCst);
    let durable = state.record_locally_durable(captured);
    record.offered.notify_waiters();
    let WasmCheckpointBoundary::Replicas(replicas) = durable.boundary().clone() else {
        panic!("the checkpoint was captured for replicas");
    };
    loop {
        let progressed = state.replica_progress_signal().notified();
        tokio::pin!(progressed);
        progressed.as_mut().enable();
        if state.replicas_awaiting(&replicas, revision).is_empty() {
            break;
        }
        progressed.await;
    }
    assert!(
        record.every_replica_reported(),
        "a checkpoint completed before every replica it names reported holding it"
    );
    state.commit(durable.completed());
}

/// Fetch the branch's published checkpoint the way a replica synchronizes it once it is offered,
/// and report holding it.
async fn replicate(
    state: StdArc<ReplicatedWasmProcessorState>,
    record: StdArc<ConfirmationRecord>,
    replica: usize,
) {
    let name = node(
        REPLICAS
            .get(replica)
            .assured("the model spawns one replica task per named replica"),
    );
    let held = loop {
        let offered = record.offered.notified();
        tokio::pin!(offered);
        offered.as_mut().enable();
        if let Some(snapshot) = state.snapshot_after(Some(0)) {
            break snapshot.lsm;
        }
        offered.await;
    };
    assert!(
        held <= record.durable_revision.load(Ordering::SeqCst),
        "a replica fetched revision {held} before the owner recorded it as locally durable"
    );
    record
        .reported
        .get(replica)
        .assured("the record holds a flag for every named replica")
        .store(true, Ordering::SeqCst);
    state.mark_replica_progress(&name, held);
}

/// Read the branch's checkpoint progress while it is being captured, written and confirmed.
async fn inspect(state: StdArc<ReplicatedWasmProcessorState>, record: StdArc<ConfirmationRecord>) {
    let mut last_committed = None;
    for _ in 0..INSPECTIONS {
        let inspection = state.inspection();
        assert!(
            inspection.committed_revision >= last_committed,
            "the committed revision went back from {last_committed:?} to {:?}",
            inspection.committed_revision
        );
        last_committed = inspection.committed_revision;
        if let (Some(latest), Some(committed)) =
            (inspection.latest_revision, inspection.committed_revision)
        {
            assert!(
                latest >= committed,
                "the latest checkpoint {latest} is older than the committed one {committed}"
            );
        }
        if inspection.stage == WasmCheckpointStage::ReplicaConfirmed {
            assert!(
                record.every_replica_reported(),
                "an inspection reported a replica-confirmed checkpoint before every replica \
                 reported holding it"
            );
            assert_eq!(
                inspection.confirmed_replicas, inspection.required_replicas,
                "a replica-confirmed checkpoint must count every required replica as confirmed"
            );
        }
        tokio::task::yield_now().await;
    }
}

/// A checkpoint waiting for its replicas completes once both report, whatever the order of their
/// fetches, reports and the owner's registration for the next report: a report landing between
/// the owner's read and its sleep is never missed, which Shuttle would otherwise report as a
/// deadlock. No replica fetches a revision before it is on the owner's storage, and no inspection
/// reports confirmation before every replica reported or moves the committed revision back.
fn a_checkpoint_waiting_for_its_replicas_misses_no_confirmation() {
    shuttle::future::block_on(async {
        let state = StdArc::new(ReplicatedWasmProcessorState::new(placement(), None));
        let record = StdArc::new(ConfirmationRecord::default());
        let mut replicating = Vec::with_capacity(REPLICAS.len());
        for replica in 0..REPLICAS.len() {
            replicating.push(tokio::spawn(replicate(
                state.clone(),
                record.clone(),
                replica,
            )));
        }
        let inspecting = tokio::spawn(inspect(state.clone(), record.clone()));
        let owner = tokio::spawn(checkpoint_and_confirm(state.clone(), record.clone()));
        owner.await.assured(MODEL_TASK_JOINS);
        for replica in replicating {
            replica.await.assured(MODEL_TASK_JOINS);
        }
        inspecting.await.assured(MODEL_TASK_JOINS);
        assert_eq!(
            state.inspection().stage,
            WasmCheckpointStage::ReplicaConfirmed,
            "the checkpoint every replica confirmed must be committed"
        );
    });
}

#[test]
fn shuttle_a_checkpoint_waiting_for_its_replicas_misses_no_confirmation() {
    check_interleavings(a_checkpoint_waiting_for_its_replicas_misses_no_confirmation);
}

/// How the checkpoint that covers one callback ends in a held-acknowledgement model.
#[derive(Clone, Copy)]
enum CheckpointEnd {
    /// The checkpoint reached its boundary and releases its holds.
    Completed,
    /// The checkpoint failed and withholds every input it held.
    Failed,
}

/// How the downstream delivery of the callback's output ends.
#[derive(Clone, Copy)]
enum DeliveryEnd {
    Succeeded,
    Failed,
}

/// One input a callback carried into an output row: the processor's own share, the downstream
/// delivery of the output, and the hold its checkpoint keeps. The delivery resolves concurrently
/// with the branch finishing the callback and ending its checkpoint, and the input resolves exactly
/// once: successfully only when the checkpoint completed and the delivery succeeded, and never
/// before the checkpoint released its hold.
fn held_input_resolves_once_after_its_checkpoint(checkpoint: CheckpointEnd, delivery: DeliveryEnd) {
    shuttle::future::block_on(async {
        let (processing, completion) = AckSet::root();
        let mut holds = WasmCheckpointHolds::default();
        holds.hold(&processing);
        let delivered = processing.attached();
        let released = StdArc::new(AtomicBool::new(false));

        let observed_release = released.clone();
        let observer = tokio::spawn(async move {
            let outcome = completion.wait().await;
            if outcome == AckOutcome::Ack {
                assert!(
                    observed_release.load(Ordering::SeqCst),
                    "an input was acknowledged before the checkpoint that covers it released its \
                     hold"
                );
            }
            outcome
        });
        let delivering = tokio::spawn(async move {
            match delivery {
                DeliveryEnd::Succeeded => delivered.ack_success(),
                DeliveryEnd::Failed => delivered.no_ack("delivery failed"),
            }
        });
        let branch = tokio::spawn(async move {
            processing.ack_success();
            tokio::task::yield_now().await;
            match checkpoint {
                CheckpointEnd::Completed => {
                    released.store(true, Ordering::SeqCst);
                    holds.release();
                }
                CheckpointEnd::Failed => {
                    for held in holds.acks() {
                        held.no_ack("checkpoint failed");
                    }
                }
            }
        });
        branch.await.assured(MODEL_TASK_JOINS);
        delivering.await.assured(MODEL_TASK_JOINS);
        let outcome = observer.await.assured(MODEL_TASK_JOINS);
        let succeeded = matches!(
            (checkpoint, delivery),
            (CheckpointEnd::Completed, DeliveryEnd::Succeeded)
        );
        assert_eq!(
            outcome == AckOutcome::Ack,
            succeeded,
            "the input resolved {outcome:?}, but it succeeds only when both its checkpoint and \
             its delivery do"
        );
    });
}

fn a_held_input_resolves_once_after_its_completed_checkpoint() {
    held_input_resolves_once_after_its_checkpoint(CheckpointEnd::Completed, DeliveryEnd::Succeeded);
}

fn a_held_input_whose_checkpoint_failed_is_negatively_acknowledged() {
    held_input_resolves_once_after_its_checkpoint(CheckpointEnd::Failed, DeliveryEnd::Succeeded);
}

fn a_held_input_whose_delivery_failed_is_negatively_acknowledged() {
    held_input_resolves_once_after_its_checkpoint(CheckpointEnd::Completed, DeliveryEnd::Failed);
}

fn a_held_input_whose_checkpoint_and_delivery_failed_resolves_once() {
    held_input_resolves_once_after_its_checkpoint(CheckpointEnd::Failed, DeliveryEnd::Failed);
}

#[test]
fn shuttle_a_held_input_resolves_once_after_its_completed_checkpoint() {
    check_interleavings(a_held_input_resolves_once_after_its_completed_checkpoint);
}

#[test]
fn shuttle_a_held_input_whose_checkpoint_failed_is_negatively_acknowledged() {
    check_interleavings(a_held_input_whose_checkpoint_failed_is_negatively_acknowledged);
}

#[test]
fn shuttle_a_held_input_whose_delivery_failed_is_negatively_acknowledged() {
    check_interleavings(a_held_input_whose_delivery_failed_is_negatively_acknowledged);
}

#[test]
fn shuttle_a_held_input_whose_checkpoint_and_delivery_failed_resolves_once() {
    check_interleavings(a_held_input_whose_checkpoint_and_delivery_failed_resolves_once);
}
