//! Deduplicator and window publication racing a backup capture, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The ordering invariant between a branch task's forced publication and the capture
//!   that reads it: once the branch task answers the cut's checkpoint request, the keyspace or
//!   window the capture takes holds every change the task acknowledged before the cut asked.
//! - **Depends on.** The production keyspace and window publications, the capture readers, the
//!   boundary's channels and the server Shuttle runner.
//! - **Must not know.** Archive encodings, stable storage or the interconnect.

use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::RuntimeState;
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::{DomainName, ModelKind, ModelName, ParseAsType, SchemaFingerprint, Timestamp};
use nervix_primitives::sync::{
    Arc, StdArc,
    atomic::{AtomicUsize, Ordering},
    mpsc, oneshot,
};

use crate::{
    runtime::{
        CapturedDeduplicatorKeyspace, CapturedWindow, DeduplicatorKey, OptionalTestField,
        ReorderKeyPart, ReplicatedDeduplicatorState, ReplicatedWindowProcessorState,
        RuntimeStatePlacement, TestWindow, WindowAccumulatorSnapshot, WindowArgumentColumns,
        WindowEntrySnapshot, WindowProcessorState, WindowProcessorStateSnapshot,
        window_state::WindowPublishedSnapshot,
    },
    runtime_schema::{RuntimeRecordBatch, RuntimeRecordMetadata, RuntimeRow},
};

const MODEL_TASK_JOINS: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";
const MAX_TIME: Duration = Duration::from_secs(600);
/// The changes the source applies, one at a time, while the cut may ask for a publication.
const CHANGES: usize = 2;

fn placement(kind: ModelKind, state: RuntimeState) -> RuntimeStatePlacement {
    RuntimeStatePlacement {
        domain: DomainName::parse("backup").assured("the domain name is well formed"),
        state,
        kind,
        identifier: ModelName::parse("branch_state").assured("the identifier is well formed"),
        branch_key: None,
    }
}

/// A source that sends every change after the previous one was acknowledged, and counts the
/// changes the branch task acknowledged.
async fn source(changes: mpsc::Sender<oneshot::Sender<()>>, acknowledged: Arc<AtomicUsize>) {
    for _ in 0..CHANGES {
        let (applied, application) = oneshot::channel();
        changes
            .send(applied)
            .await
            .assured("the branch task receives changes until the source ends");
        application
            .await
            .assured("the branch task acknowledges every change it receives");
        acknowledged.fetch_add(1, Ordering::SeqCst);
    }
}

/// The cut: it reads how many changes were acknowledged, asks the branch task to publish, and
/// returns that count once the task answered, as a quiesced capture reads only after its
/// lifecycle checkpoint returned.
async fn cut(checkpoints: &mpsc::Sender<oneshot::Sender<()>>, acknowledged: &AtomicUsize) -> usize {
    let before_cut = acknowledged.load(Ordering::SeqCst);
    let (published, publication) = oneshot::channel();
    checkpoints
        .send(published)
        .await
        .assured("the branch task receives checkpoint requests until the cut ends");
    publication
        .await
        .assured("the branch task answers every checkpoint request");
    before_cut
}

/// A deduplicator branch task that reserves one key per change and publishes its keyspace when
/// the cut asks, preferring the cut's request as the production branch task does.
async fn deduplicator_branch(
    state: Arc<ReplicatedDeduplicatorState>,
    mut changes: mpsc::Receiver<oneshot::Sender<()>>,
    mut checkpoints: mpsc::Receiver<oneshot::Sender<()>>,
) {
    let mut keyspace = ReplicatedDeduplicatorState::keyspace(&state);
    let mut next_key = 0_u64;
    let mut changing = true;
    let mut checkpointing = true;
    while changing || checkpointing {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::select! {
            biased;
            checkpoint = checkpoints.recv(), if checkpointing => {
                let Some(published) = checkpoint else {
                    checkpointing = false;
                    continue;
                };
                keyspace.publish();
                published.send(()).assured("the cut waits for its answer");
            }
            change = changes.recv(), if changing => {
                let Some(applied) = change else {
                    changing = false;
                    continue;
                };
                let key = DeduplicatorKey::new(vec![ReorderKeyPart::UInt64(next_key)]);
                let reserved =
                    keyspace.reserve_new_key(key, Timestamp::from_unix_nanos(1), MAX_TIME);
                assert!(reserved, "every change reserves a key the keyspace did not hold");
                next_key = next_key
                    .checked_add(1)
                    .verified("a model applies far fewer changes than u64::MAX");
                applied.send(()).assured("the source waits for its acknowledgement");
            }
        }
    }
}

/// Keys reserved while the cut asks for a publication: the keyspace the capture reads after the
/// answer holds every key acknowledged before the cut asked.
fn a_forced_deduplicator_publication_racing_reservations() {
    shuttle::future::block_on(async {
        let state = Arc::new(
            ReplicatedDeduplicatorState::new(
                placement(
                    ModelKind::Deduplicator,
                    RuntimeState::Deduplicator {
                        schema: SchemaFingerprint::from_digest([7; 32]),
                    },
                ),
                None,
            )
            .assured("an empty keyspace needs no checkpoint"),
        );
        let (changes, change_requests) = mpsc::channel(1);
        let (checkpoints, checkpoint_requests) = mpsc::channel(1);
        let acknowledged = Arc::new(AtomicUsize::new(0));
        let branch = nervix_primitives::task::spawn(deduplicator_branch(
            state.clone(),
            change_requests,
            checkpoint_requests,
        ));
        let changing = nervix_primitives::task::spawn(source(changes, acknowledged.clone()));
        let before_cut = cut(&checkpoints, &acknowledged).await;
        let captured = CapturedDeduplicatorKeyspace::published(&state);
        assert!(
            captured.key_count() >= before_cut,
            "the capture missed a key its branch task acknowledged before the cut asked it to \
             publish"
        );
        drop(checkpoints);
        changing.await.assured(MODEL_TASK_JOINS);
        branch.await.assured(MODEL_TASK_JOINS);
    });
}

#[test]
fn shuttle_a_forced_deduplicator_publication_holds_every_key_acknowledged_before_the_cut() {
    check_interleavings(a_forced_deduplicator_publication_racing_reservations);
}

/// A window whose branch task starts with `CHANGES` retained rows, restored as a branch task
/// restores its checkpoint.
fn retained_window() -> (TestWindow, WindowProcessorState) {
    let window = TestWindow::new(
        "SET total = SUM(input.latency)",
        &[OptionalTestField {
            name: "latency",
            ty: ParseAsType::I64,
            optional: false,
        }],
        &[OptionalTestField {
            name: "total",
            ty: ParseAsType::I64,
            optional: true,
        }],
    );
    let latencies: ArrayRef = StdArc::new(Int64Array::from(vec![10, 20]));
    let input_schema = window.input_schema.arrow_schema();
    let input = RecordBatch::try_new(
        StdArc::clone(&input_schema),
        vec![StdArc::clone(&latencies)],
    )
    .assured("the latency column matches the input schema");
    let input = Arc::new(
        RuntimeRecordBatch::from_record_batch(input_schema, input)
            .assured("the input rows are a valid batch"),
    );
    let argument_schema = WindowArgumentColumns::snapshot_schema(&window.plan);
    let arguments = RecordBatch::try_new(StdArc::clone(&argument_schema), vec![latencies])
        .assured("the latency argument matches the argument schema");
    let arguments = Arc::new(
        RuntimeRecordBatch::from_record_batch(argument_schema, arguments)
            .assured("the argument rows are a valid batch"),
    );
    let mut entries = Vec::with_capacity(CHANGES);
    for row in 0..CHANGES {
        let sequence = u64::try_from(row).verified("a model retains a few rows");
        let nanos = i64::try_from(row).verified("a model retains a few rows");
        let at = Timestamp::from_unix_nanos(nanos);
        let metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(at, at);
        entries.push(WindowEntrySnapshot {
            sequence,
            timestamp: at,
            key: None,
            record: RuntimeRow::new(input.clone(), row, metadata.clone())
                .assured("the row is inside its batch"),
            arguments: RuntimeRow::new(arguments.clone(), row, metadata)
                .assured("the row is inside its batch"),
        });
    }
    let snapshot = WindowProcessorStateSnapshot {
        entries,
        next_sequence: u64::try_from(CHANGES).verified("a model retains a few rows"),
        incarnation: Some(1),
        accumulators: vec![WindowAccumulatorSnapshot::Retained],
    };
    let state =
        WindowProcessorState::from_snapshot(&window.plan, window.input_schema.as_ref(), &snapshot)
            .assured("the window restores the rows it retained");
    (window, state)
}

/// A window branch task that steps past its oldest row for every change and publishes its
/// window when the cut asks, preferring the cut's request as the production branch task does.
async fn window_branch(
    replicated: Arc<ReplicatedWindowProcessorState>,
    mut state: WindowProcessorState,
    mut changes: mpsc::Receiver<oneshot::Sender<()>>,
    mut checkpoints: mpsc::Receiver<oneshot::Sender<()>>,
) {
    let mut changing = true;
    let mut checkpointing = true;
    while changing || checkpointing {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::select! {
            biased;
            checkpoint = checkpoints.recv(), if checkpointing => {
                let Some(published) = checkpoint else {
                    checkpointing = false;
                    continue;
                };
                replicated
                    .replace_state(&state)
                    .assured("a window of in-memory rows publishes");
                published.send(()).assured("the cut waits for its answer");
            }
            change = changes.recv(), if changing => {
                let Some(applied) = change else {
                    changing = false;
                    continue;
                };
                let stepped = state.retract_oldest(1, Timestamp::from_unix_nanos(10));
                assert_eq!(stepped.len(), 1, "every change steps past one retained row");
                applied.send(()).assured("the source waits for its acknowledgement");
            }
        }
    }
}

/// Rows stepped past while the cut asks for a publication: the window the capture reads after the
/// answer no longer retains any row stepped past before the cut asked.
fn a_forced_window_publication_racing_steps() {
    shuttle::future::block_on(async {
        let (_window, state) = retained_window();
        let replicated = Arc::new(
            ReplicatedWindowProcessorState::new(
                placement(
                    ModelKind::WindowProcessor,
                    RuntimeState::WindowProcessor {
                        schema: SchemaFingerprint::from_digest([8; 32]),
                    },
                ),
                None,
            )
            .assured("an unpublished window needs no checkpoint"),
        );
        let (changes, change_requests) = mpsc::channel(1);
        let (checkpoints, checkpoint_requests) = mpsc::channel(1);
        let acknowledged = Arc::new(AtomicUsize::new(0));
        let branch = nervix_primitives::task::spawn(window_branch(
            replicated.clone(),
            state,
            change_requests,
            checkpoint_requests,
        ));
        let changing = nervix_primitives::task::spawn(source(changes, acknowledged.clone()));
        let before_cut = cut(&checkpoints, &acknowledged).await;
        let generation = replicated.generations.load();
        let captured = CapturedWindow::published(&generation);
        assert!(
            captured.is_some(),
            "the capture reads the window the task published"
        );
        let Some(WindowPublishedSnapshot::Live(published)) = &generation.value else {
            panic!("a branch task publishes its live window");
        };
        let at_most = CHANGES
            .checked_sub(before_cut)
            .verified("the source acknowledges at most the changes it sends");
        assert!(
            published.entries.len() <= at_most,
            "the capture kept a row its branch task stepped past before the cut asked it to \
             publish"
        );
        drop(checkpoints);
        changing.await.assured(MODEL_TASK_JOINS);
        branch.await.assured(MODEL_TASK_JOINS);
    });
}

#[test]
fn shuttle_a_forced_window_publication_holds_every_step_acknowledged_before_the_cut() {
    check_interleavings(a_forced_window_publication_racing_steps);
}
