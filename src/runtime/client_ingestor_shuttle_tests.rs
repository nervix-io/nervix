//! Client ingestor credit, admission fence and terminal-result checks under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants the node's producer budget, the admission fence against a quiesce,
//!   and an endpoint's answers are held to while reservations, admissions, acknowledgements,
//!   closes and endings race.
//! - **Depends on.** The client ingestor endpoint and budget, the ingestor quiesce control, ACK
//!   roots and their trackers, and the model harness's Shuttle runner.
//! - **Must not know.** Sessions, the interconnect, or the graph behind the admission worker.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
};

use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::{
    AckWindow, CLIENT_PRODUCER_NODE_BYTES, ClientEndpointContract, ClientProcessingFailure,
    ClientProducerEndReason, ClientProducerLimits, ClientProducerPolicy, ClientSubmissionOutcome,
    FieldName, IngestQuiesceMode, ParseAsType, SchemaField,
};
use nervix_primitives::sync::{StdArc, mpsc, oneshot};
// Real atomics are not Shuttle scheduling points, so each record below changes in the same
// scheduling step as the operation it records.
use nervix_primitives::unmodeled::sync::atomic::{
    AtomicBool, AtomicUsize, Ordering as RecordOrdering,
};

use super::*;

const CHECK_TASK_JOINS: &str =
    "a check task that panics fails the execution before its join returns";

/// Producers racing for the node budget, of which only one fits at a time.
const RACING_PRODUCERS: usize = 3;

fn fields() -> Vec<SchemaField> {
    vec![SchemaField {
        name: FieldName::parse("id").assured("a literal field name"),
        ty: ParseAsType::String,
        optional: false,
        sensitive: false,
    }]
}

fn submission(id: u64) -> ClientSubmissionId {
    ClientSubmissionId::new(NonZeroU64::new(id).assured("a literal non-zero identity"))
}

/// Reserves more than half the node budget, holds it across a scheduling point, and returns it.
async fn reserve_more_than_half(budget: ClientProducerBudget, live: StdArc<AtomicUsize>) {
    let bytes = NonZeroU64::new(
        CLIENT_PRODUCER_NODE_BYTES
            .checked_div(2)
            .assured("the divisor is a non-zero literal")
            .checked_add(1)
            .assured("half the node budget has a successor"),
    )
    .assured("a successor is non-zero");
    let Some(reservation) = budget.try_reserve(bytes) else {
        return;
    };
    let holders = live
        .fetch_add(1, RecordOrdering::SeqCst)
        .checked_add(1)
        .assured("the holders are bounded by the racing producers");
    assert_eq!(
        holders, 1,
        "two reservations of more than half the node budget were live at once"
    );
    assert!(
        budget.reserved() <= CLIENT_PRODUCER_NODE_BYTES,
        "the node budget holds more than its bound"
    );
    nervix_primitives::task::yield_now().await;
    live.fetch_sub(1, RecordOrdering::SeqCst);
    drop(reservation);
}

fn racing_reservations_stay_within_the_node_budget() {
    shuttle::future::block_on(async {
        let budget = ClientProducerBudget::default();
        let live = StdArc::new(AtomicUsize::new(0));
        let mut producers = Vec::with_capacity(RACING_PRODUCERS);
        for _ in 0..RACING_PRODUCERS {
            producers.push(nervix_primitives::task::spawn(reserve_more_than_half(
                budget.clone(),
                live.clone(),
            )));
        }
        for producer in producers {
            producer.await.assured(CHECK_TASK_JOINS);
        }
        assert_eq!(budget.reserved(), 0, "every reservation returned its bytes");
    });
}

/// Racing opens never hold more than the node's producer budget together, and every reservation
/// returns exactly the bytes it took.
#[test]
fn shuttle_racing_reservations_never_exceed_the_node_budget_and_return_every_byte() {
    check_interleavings(racing_reservations_stay_within_the_node_budget);
}

/// Admits one validated batch through the fence, recording whether a drain had already concluded
/// when the batch was dispatched.
async fn admit_one_batch(
    control: Arc<IngestorQuiesceControl>,
    trackers: Arc<IngestorAckRootTrackers>,
    drained: StdArc<AtomicBool>,
) {
    let Ok((root, _completion)) = control.track_client_batch(&trackers) else {
        return;
    };
    assert!(
        !drained.load(RecordOrdering::SeqCst),
        "a batch was dispatched after the drain that followed the quiesce concluded"
    );
    nervix_primitives::task::yield_now().await;
    root.ack_success();
}

/// Engages a quiesce and drains the ingestor the way an entity hold does: it waits until no root
/// it tracks is outstanding.
async fn quiesce_and_drain(
    control: Arc<IngestorQuiesceControl>,
    trackers: Arc<IngestorAckRootTrackers>,
    drained: StdArc<AtomicBool>,
) {
    control.engage(IngestorQuiesceCause::EntityHold);
    loop {
        nervix_primitives::task::consume_budget().await;
        if trackers.ingestor_outstanding() == 0 {
            break;
        }
        nervix_primitives::task::yield_now().await;
    }
    drained.store(true, RecordOrdering::SeqCst);
}

fn a_batch_racing_a_quiesce_is_counted_or_refused() {
    shuttle::future::block_on(async {
        // A whole runtime is far heavier than one execution of this model needs, so the control
        // takes the metrics it records into directly.
        let metrics = RuntimeMetrics::default();
        let domain = DomainName::parse("tenant").assured("a literal domain name");
        let ingestor = IngestorName::parse("orders_in").assured("a literal ingestor name");
        let metric_labels = metrics.register_ingestor_quiesce(&domain, &ingestor, None);
        let control = Arc::new(IngestorQuiesceControl::new(
            IngestQuiesceMode::Suspend,
            metrics,
            metric_labels,
        ));
        let trackers = Arc::new(IngestorAckRootTrackers::detached());
        let drained = StdArc::new(AtomicBool::new(false));
        // The drain spins until the admitted root resolves, so it is spawned after the admission
        // it waits for.
        let admission = nervix_primitives::task::spawn(admit_one_batch(
            control.clone(),
            trackers.clone(),
            drained.clone(),
        ));
        let drain =
            nervix_primitives::task::spawn(quiesce_and_drain(control, trackers.clone(), drained));
        admission.await.assured(CHECK_TASK_JOINS);
        drain.await.assured(CHECK_TASK_JOINS);
        assert_eq!(
            trackers.ingestor_outstanding(),
            0,
            "every tracked root resolved"
        );
    });
}

/// A batch validated while a quiesce engages is either counted by the drain that follows, which
/// then waits for it, or refused with its root resolved and nothing dispatched.
#[test]
fn shuttle_a_batch_racing_a_quiesce_is_either_counted_by_its_drain_or_refused_undispatched() {
    check_interleavings(a_batch_racing_a_quiesce_is_counted_or_refused);
}

/// How the model ends its producer.
#[derive(Debug, Clone, Copy)]
enum ProducerEnding {
    /// The producer closes, so the endpoint releases it once every admitted batch is answered.
    Close,
    /// The endpoint ends, as a node that shuts down ends it.
    EndpointEnd,
}

/// An endpoint with one installed execution admitting two batches at once, and one attached
/// producer that submitted two batches the worker took.
struct EndpointModel {
    commands: mpsc::UnboundedSender<EndpointCommand>,
    endpoint: nervix_primitives::task::JoinHandle<()>,
    jobs: mpsc::Receiver<AdmissionJob>,
    handle: ClientProducerHandle,
    events: ClientProducerEvents,
}

/// An endpoint task, spawned with one installed execution admitting `window` batches at once and
/// admission open, and the admission worker's side of that execution, which a model plays.
struct StartedEndpoint {
    commands: mpsc::UnboundedSender<EndpointCommand>,
    endpoint: nervix_primitives::task::JoinHandle<()>,
    jobs: mpsc::Receiver<AdmissionJob>,
}

fn start_endpoint(window: usize) -> StartedEndpoint {
    let (commands, receiver) = mpsc::unbounded_channel();
    let domain = DomainName::parse("tenant").assured("a literal domain name");
    let ingestor = IngestorName::parse("orders_in").assured("a literal ingestor name");
    let metrics = RuntimeMetrics::default();
    let endpoint = Endpoint {
        series: metrics.client_ingestor_series(&domain, &ingestor),
        domain,
        ingestor,
        commands: receiver,
        acknowledgements: FuturesUnordered::new(),
        execution: None,
        intake: ClientIntakeState::Suspended,
        attachments: IndexMap::with_hasher(RandomState::default()),
        cursor: 0,
        window_used: 0,
        in_worker: None,
        gauges: Arc::new(PublishedClientGauges::default()),
        published: ClientIngestorGauges::default(),
    };
    let endpoint = nervix_primitives::task::spawn(endpoint.run());
    let (jobs, received) = mpsc::channel(1);
    let window_size = NonZeroUsize::new(window).assured("a model admits at least one batch");
    let window_max: u64 = window.arch_into();
    let window_max = NonZeroU64::new(window_max).assured("a model admits at least one batch");
    let execution = Arc::new(ClientExecution {
        contract: ClientEndpointContract::from_digest([1; 32]),
        generation: 1,
        fields: fields(),
        window: window_size,
        policy: ClientProducerPolicy {
            window: AckWindow::Parallel { max: window_max },
            ack_timeout: Duration::from_secs(3_600),
            retry_backoff: Duration::from_millis(10),
            retry_max_backoff: Duration::from_millis(100),
        },
        jobs,
    });
    commands
        .send(EndpointCommand::Install(execution))
        .assured("the endpoint runs until the model ends it");
    commands
        .send(EndpointCommand::Intake(ClientIntakeState::Open))
        .assured("the endpoint runs until the model ends it");
    StartedEndpoint {
        commands,
        endpoint,
        jobs: received,
    }
}

/// Attaches one producer, served the way `serving` says, to a started endpoint.
async fn attach_producer(
    commands: &mpsc::UnboundedSender<EndpointCommand>,
    serving: ProducerServing,
) -> (ClientProducerHandle, ClientProducerEvents) {
    let (reply, attached) = oneshot::channel();
    commands
        .send(EndpointCommand::Attach(AttachCommand {
            expected_fields: fields(),
            limits: ClientProducerLimits {
                batches: NonZeroU32::new(4).assured("a literal non-zero count"),
                bytes: NonZeroU64::new(1_024).assured("a literal non-zero size"),
            },
            max_batch_bytes: NonZeroU64::new(1_024).assured("a literal non-zero size"),
            serving,
            reply,
        }))
        .assured("the endpoint runs until the model ends it");
    let AttachedProducer {
        description,
        events,
    } = attached
        .await
        .assured("the endpoint answers every open")
        .assured("an open expecting the execution's fields attaches");
    let handle = ClientProducerHandle {
        commands: commands.clone(),
        attachment: description.attachment,
        detached: false,
    };
    (handle, events)
}

impl EndpointModel {
    async fn start() -> Self {
        let StartedEndpoint {
            commands,
            endpoint,
            jobs: received,
        } = start_endpoint(2);
        let (handle, events) = attach_producer(&commands, ProducerServing::Local).await;
        handle.submit(submission(1), Bytes::from_static(b"one"));
        handle.submit(submission(2), Bytes::from_static(b"two"));
        Self {
            commands,
            endpoint,
            jobs: received,
            handle,
            events,
        }
    }

    /// Takes the next batch the endpoint hands its worker.
    async fn next_job(&mut self) -> AdmissionJob {
        self.jobs
            .recv()
            .await
            .assured("the endpoint hands the worker every batch the window admits")
    }
}

/// Reports one batch admitted under a fresh root, which the returned set resolves. An endpoint
/// that already ended takes no report, as the worker of an ended execution finds.
fn admitted(commands: &mpsc::UnboundedSender<EndpointCommand>, job: &AdmissionJob) -> AckSet {
    let (root, completion) = AckSet::root();
    let report = EndpointCommand::Admission(AdmissionReport {
        attachment: job.attachment,
        submission: job.submission,
        result: AdmissionResult::Admitted {
            completion,
            ack_timeout: Duration::from_secs(3_600),
        },
    });
    commands
        .send(report)
        .discarded("an endpoint that ended already answered every batch it held");
    root
}

/// Ends the producer the way `ending` names, while the batches' acknowledgements resolve.
async fn end_producer(
    ending: ProducerEnding,
    handle: ClientProducerHandle,
    commands: mpsc::UnboundedSender<EndpointCommand>,
) {
    match ending {
        ProducerEnding::Close => handle.close(),
        ProducerEnding::EndpointEnd => {
            let (done, ended) = oneshot::channel();
            commands
                .send(EndpointCommand::End {
                    reason: ClientProducerEndReason::ShuttingDown,
                    done,
                })
                .assured("the endpoint runs until this command ends it");
            ended
                .await
                .assured("the endpoint confirms the end it was asked for");
            drop(handle);
        }
    }
}

/// The outcomes a producer's events carried, and whether its end or its release came last.
struct AnsweredEvents {
    outcomes: BTreeMap<u64, ClientSubmissionOutcome>,
    ended: Option<ClientProducerEndReason>,
}

async fn read_every_event(mut events: ClientProducerEvents) -> AnsweredEvents {
    let mut answered = AnsweredEvents {
        outcomes: BTreeMap::new(),
        ended: None,
    };
    while let Some(event) = events.outcomes.recv().await {
        assert!(
            answered.ended.is_none(),
            "an event followed the producer's end: {event:?}"
        );
        match event {
            ClientProducerEvent::Outcome {
                submission,
                outcome,
                ..
            } => {
                let previous = answered.outcomes.insert(submission.get().get(), outcome);
                assert!(previous.is_none(), "a batch was answered twice");
            }
            ClientProducerEvent::Ended(reason) => answered.ended = Some(reason),
        }
    }
    answered
}

fn an_ending_producer_answers_every_batch_once(ending: ProducerEnding) {
    shuttle::future::block_on(async move {
        let mut model = EndpointModel::start().await;
        let first = model.next_job().await;
        let first_root = admitted(&model.commands, &first);
        // The second batch reaches the worker once the first is reported; its admission report
        // races the end and the first batch's acknowledgement.
        let second = model.next_job().await;
        let reporter_commands = model.commands.clone();
        let reporter = nervix_primitives::task::spawn(async move {
            let second_root = admitted(&reporter_commands, &second);
            nervix_primitives::task::yield_now().await;
            second_root.no_ack("a route rejected it");
        });
        let acknowledger = nervix_primitives::task::spawn(async move {
            first_root.ack_success();
        });
        let ender = nervix_primitives::task::spawn(end_producer(
            ending,
            model.handle,
            model.commands.clone(),
        ));
        let answered = read_every_event(model.events).await;
        reporter.await.assured(CHECK_TASK_JOINS);
        acknowledger.await.assured(CHECK_TASK_JOINS);
        ender.await.assured(CHECK_TASK_JOINS);
        drop(model.commands);
        model.endpoint.await.assured(CHECK_TASK_JOINS);

        assert_eq!(
            answered.outcomes.len(),
            2,
            "every batch the producer submitted is answered before its end or release"
        );
        match ending {
            ProducerEnding::Close => {
                assert_eq!(
                    answered.ended, None,
                    "a closed producer is released, not ended"
                );
                assert_eq!(
                    answered.outcomes.get(&1),
                    Some(&ClientSubmissionOutcome::Completed),
                    "a close waits for the acknowledgement of every admitted batch"
                );
                assert_eq!(
                    answered.outcomes.get(&2),
                    Some(&ClientSubmissionOutcome::ProcessingFailed(
                        ClientProcessingFailure::Rejected
                    )),
                    "a close waits for the acknowledgement of every admitted batch"
                );
            }
            ProducerEnding::EndpointEnd => {
                assert_eq!(
                    answered.ended,
                    Some(ClientProducerEndReason::ShuttingDown),
                    "the endpoint's end is the producer's last event"
                );
                for outcome in answered.outcomes.values() {
                    assert!(
                        !matches!(outcome, ClientSubmissionOutcome::NotAdmitted(_)),
                        "an admitted batch was reported as not admitted: {outcome:?}"
                    );
                }
            }
        }
    });
}

/// A close racing the acknowledgements and the worker's admission reports answers every admitted
/// batch exactly once, with its real outcome, before the producer is released.
#[test]
fn shuttle_a_closing_producer_answers_every_admitted_batch_once_before_its_release() {
    check_interleavings(|| an_ending_producer_answers_every_batch_once(ProducerEnding::Close));
}

/// An endpoint ending while acknowledgements resolve answers every batch exactly once, never as
/// not admitted once it was admitted, and its end is the producer's last event.
#[test]
fn shuttle_an_ending_endpoint_answers_every_batch_once_and_ends_its_producer_last() {
    check_interleavings(|| {
        an_ending_producer_answers_every_batch_once(ProducerEnding::EndpointEnd)
    });
}

/// Attaches a producer that another node forwards, with the requests the endpoint sends that node
/// to clear its batches.
async fn attach_forwarded_producer(
    commands: &mpsc::UnboundedSender<EndpointCommand>,
) -> (
    ClientProducerHandle,
    ClientProducerEvents,
    mpsc::UnboundedReceiver<ClientSubmissionId>,
) {
    let (clearance_requests, clearances) = mpsc::unbounded_channel();
    let reservation = ClientProducerBudget::default()
        .try_reserve(NonZeroU64::new(1_024).assured("a literal non-zero size"))
        .assured("an empty node budget holds one producer's bytes");
    let serving = ProducerServing::Forwarded {
        _reservation: reservation,
        clearance_requests,
    };
    let (handle, events) = attach_producer(commands, serving).await;
    (handle, events, clearances)
}

/// The bit a batch sets in a record of the batches cleared so far.
fn cleared_bit(submission: ClientSubmissionId) -> usize {
    let shift = u32::try_from(submission.get().get())
        .assured("a model's submission identities are small literals");
    1_usize
        .checked_shl(shift)
        .assured("a model's submission identities are below the word size")
}

/// Plays the node that forwards a producer: it clears every batch the endpoint asks about until
/// the producer is gone, and records each clearance before it sends it, as a serving link records
/// a batch as possibly admitted before its `Clear` leaves.
async fn clear_requested_batches(
    mut clearances: mpsc::UnboundedReceiver<ClientSubmissionId>,
    commands: mpsc::UnboundedSender<EndpointCommand>,
    attachment: ClientAttachmentId,
    cleared: StdArc<AtomicUsize>,
) {
    while let Some(submission) = clearances.recv().await {
        cleared.fetch_or(cleared_bit(submission), RecordOrdering::SeqCst);
        commands
            .send(EndpointCommand::Clear {
                attachment,
                submission,
            })
            .discarded("an endpoint that ended already answered the batch it would have cleared");
    }
}

/// Asserts that the worker was handed a batch only after its serving node cleared it.
fn assert_cleared_before_the_worker(cleared: &AtomicUsize, job: &AdmissionJob) {
    let recorded = cleared.load(RecordOrdering::SeqCst) & cleared_bit(job.submission);
    assert_ne!(
        recorded, 0,
        "a forwarded batch reached the worker before its serving node cleared it"
    );
}

fn a_detach_racing_a_clearance_admits_only_a_cleared_batch_and_returns_its_slot() {
    shuttle::future::block_on(async {
        let StartedEndpoint {
            commands,
            endpoint,
            mut jobs,
        } = start_endpoint(1);
        let (forwarded, _forwarded_events, clearances) = attach_forwarded_producer(&commands).await;
        let (local, mut local_events) = attach_producer(&commands, ProducerServing::Local).await;
        // The forwarded batch takes the window's one slot while it is cleared, and the local batch
        // waits for that slot.
        forwarded.submit(submission(1), Bytes::from_static(b"forwarded"));
        local.submit(submission(2), Bytes::from_static(b"local"));
        let cleared = StdArc::new(AtomicUsize::new(0));
        let serving = nervix_primitives::task::spawn(clear_requested_batches(
            clearances,
            commands.clone(),
            forwarded.attachment,
            cleared.clone(),
        ));
        // The forwarding node is lost, which detaches its producer.
        let detacher = nervix_primitives::task::spawn(async move {
            drop(forwarded);
        });
        loop {
            let job = jobs
                .recv()
                .await
                .assured("the endpoint hands the worker every batch the window admits");
            if job.submission == submission(1) {
                assert_cleared_before_the_worker(&cleared, &job);
            }
            admitted(&commands, &job).ack_success();
            if job.submission == submission(2) {
                break;
            }
        }
        let answered = local_events.outcomes.recv().await;
        assert_eq!(
            answered,
            Some(ClientProducerEvent::Outcome {
                submission: submission(2),
                outcome: ClientSubmissionOutcome::Completed,
                detail: None,
            }),
            "the local batch took the slot the forwarded batch returned or finished with"
        );
        serving.await.assured(CHECK_TASK_JOINS);
        detacher.await.assured(CHECK_TASK_JOINS);
        drop(local);
        drop(commands);
        endpoint.await.assured(CHECK_TASK_JOINS);
    });
}

/// A forwarded batch reaches the worker only after its serving node cleared it, and a detach that
/// races the clearance either leaves the cleared batch admitted or drops the uncleared one and
/// returns its slot of the window, so another producer's batch is admitted either way.
#[test]
fn shuttle_a_detach_racing_a_clearance_admits_only_a_cleared_batch_and_returns_its_slot() {
    check_interleavings(
        a_detach_racing_a_clearance_admits_only_a_cleared_batch_and_returns_its_slot,
    );
}

fn an_end_racing_clearances_reports_a_batch_not_admitted_exactly_when_the_worker_never_took_it() {
    shuttle::future::block_on(async {
        let StartedEndpoint {
            commands,
            endpoint,
            mut jobs,
        } = start_endpoint(2);
        let (forwarded, events, clearances) = attach_forwarded_producer(&commands).await;
        forwarded.submit(submission(1), Bytes::from_static(b"first"));
        forwarded.submit(submission(2), Bytes::from_static(b"second"));
        let cleared = StdArc::new(AtomicUsize::new(0));
        let serving = nervix_primitives::task::spawn(clear_requested_batches(
            clearances,
            commands.clone(),
            forwarded.attachment,
            cleared.clone(),
        ));
        // The worker takes what it is handed and never reports it, like a worker that the node's
        // end stops, so a second cleared batch waits for it. Its channel closes once the ended
        // endpoint drops its execution.
        let worker_cleared = cleared.clone();
        let worker = nervix_primitives::task::spawn(async move {
            let mut taken = BTreeSet::new();
            while let Some(job) = jobs.recv().await {
                assert_cleared_before_the_worker(&worker_cleared, &job);
                taken.insert(job.submission.get().get());
            }
            taken
        });
        let ender = nervix_primitives::task::spawn(end_producer(
            ProducerEnding::EndpointEnd,
            forwarded,
            commands.clone(),
        ));
        let answered = read_every_event(events).await;
        serving.await.assured(CHECK_TASK_JOINS);
        ender.await.assured(CHECK_TASK_JOINS);
        drop(commands);
        endpoint.await.assured(CHECK_TASK_JOINS);
        let taken = worker.await.assured(CHECK_TASK_JOINS);

        assert_eq!(
            answered.ended,
            Some(ClientProducerEndReason::ShuttingDown),
            "the endpoint's end is the producer's last event"
        );
        for id in [1, 2] {
            let outcome = answered
                .outcomes
                .get(&id)
                .assured("an ending endpoint answers every batch its producer submitted");
            let expected = if taken.contains(&id) {
                ClientSubmissionOutcome::OutcomeUnknown(ClientOutcomeUncertainty::Interrupted)
            } else {
                ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::ProducerEnded)
            };
            assert_eq!(
                outcome, &expected,
                "batch {id} is not admitted exactly when the worker never took it"
            );
        }
    });
}

/// An endpoint ending while a forwarded producer's batches are cleared answers each exactly once:
/// as not admitted when the worker never took it, whether it was still being cleared or cleared and
/// waiting for the worker, and as of unknown outcome when the worker took it.
#[test]
fn shuttle_an_end_racing_clearances_reports_a_batch_not_admitted_exactly_when_the_worker_never_took_it()
 {
    check_interleavings(
        an_end_racing_clearances_reports_a_batch_not_admitted_exactly_when_the_worker_never_took_it,
    );
}
