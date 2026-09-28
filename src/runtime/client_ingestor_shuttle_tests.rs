//! Client ingestor credit, admission fence and terminal-result checks under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants the node's producer budget, the admission fence against a quiesce,
//!   and an endpoint's answers are held to while reservations, admissions, acknowledgements,
//!   closes and endings race.
//! - **Depends on.** The client ingestor endpoint and budget, the ingestor quiesce control, ACK
//!   roots and their trackers, and the server Shuttle runner.
//! - **Must not know.** Sessions, the interconnect, or the graph behind the admission worker.

// The standard library's atomics are not Shuttle scheduling points, so each record below changes in
// the same scheduling step as the operation it records.
use std::{
    collections::BTreeMap,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, AtomicUsize, Ordering as RecordOrdering},
    },
};

use nervix_models::{
    AckWindow, CLIENT_PRODUCER_NODE_BYTES, ClientEndpointContract, ClientProcessingFailure,
    ClientProducerEndReason, ClientProducerLimits, ClientProducerPolicy, ClientSubmissionOutcome,
    FieldName, IngestQuiesceMode, ParseAsType, SchemaField,
};
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::shuttle_test::check_interleavings;

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
    tokio::task::yield_now().await;
    live.fetch_sub(1, RecordOrdering::SeqCst);
    drop(reservation);
}

fn racing_reservations_stay_within_the_node_budget() {
    shuttle::future::block_on(async {
        let budget = ClientProducerBudget::default();
        let live = StdArc::new(AtomicUsize::new(0));
        let mut producers = Vec::with_capacity(RACING_PRODUCERS);
        for _ in 0..RACING_PRODUCERS {
            producers.push(tokio::spawn(reserve_more_than_half(
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
    tokio::task::yield_now().await;
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
        tokio::task::consume_budget().await;
        if trackers.ingestor_outstanding() == 0 {
            break;
        }
        tokio::task::yield_now().await;
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
        let admission = tokio::spawn(admit_one_batch(
            control.clone(),
            trackers.clone(),
            drained.clone(),
        ));
        let drain = tokio::spawn(quiesce_and_drain(control, trackers.clone(), drained));
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
    endpoint: tokio::task::JoinHandle<()>,
    jobs: mpsc::Receiver<AdmissionJob>,
    handle: ClientProducerHandle,
    events: ClientProducerEvents,
}

impl EndpointModel {
    async fn start() -> Self {
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
        let endpoint = tokio::spawn(endpoint.run());
        let (jobs, received) = mpsc::channel(1);
        let execution = Arc::new(ClientExecution {
            contract: ClientEndpointContract::from_digest([1; 32]),
            generation: 1,
            fields: fields(),
            window: NonZeroUsize::new(2).assured("a literal non-zero window"),
            policy: ClientProducerPolicy {
                window: AckWindow::Parallel {
                    max: NonZeroU64::new(2).assured("a literal non-zero window"),
                },
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
        let (reply, attached) = oneshot::channel();
        commands
            .send(EndpointCommand::Attach(AttachCommand {
                expected_fields: fields(),
                limits: ClientProducerLimits {
                    batches: NonZeroU32::new(4).assured("a literal non-zero count"),
                    bytes: NonZeroU64::new(1_024).assured("a literal non-zero size"),
                },
                max_batch_bytes: NonZeroU64::new(1_024).assured("a literal non-zero size"),
                reservation: None,
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
        let reporter = tokio::spawn(async move {
            let second_root = admitted(&reporter_commands, &second);
            tokio::task::yield_now().await;
            second_root.no_ack("a route rejected it");
        });
        let acknowledger = tokio::spawn(async move {
            first_root.ack_success();
        });
        let ender = tokio::spawn(end_producer(ending, model.handle, model.commands.clone()));
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
