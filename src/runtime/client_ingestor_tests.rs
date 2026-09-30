//! Client ingestor endpoint tests.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions that opens are checked against the installed execution, that every
//!   producer shares the execution's one acknowledgement window in turn, that every batch has
//!   exactly one outcome, and that closes, lifecycle changes and credit violations end producers
//!   with the outcome and reason that apply.
//! - **Depends on.** The endpoint task, its admission-worker protocol, and ACK roots.
//! - **Must not know.** Sessions, the interconnect, or the graph behind the admission worker.

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use nervix_models::{
    AckWindow, ClientEndpointContract, ClientProducerAdmission, ClientProducerEndReason,
    ClientProducerLimits, ClientProducerPolicy, ClientProducerRefusal, ClientSubmissionOutcome,
    ClientSubmissionRefusal, FieldName, ParseAsType, SchemaField,
};
use nervix_primitives::sync::mpsc;
use tokio::time::timeout;

use super::*;
use crate::runtime_ack::{AckCompletion, AckSet};

/// Generously longer than any step here takes, so only a hang reaches it.
const WAIT: Duration = Duration::from_secs(30);

fn fields(names: &[&str]) -> Vec<SchemaField> {
    names
        .iter()
        .map(|name| SchemaField {
            name: FieldName::parse(name).assured("a literal field name"),
            ty: ParseAsType::U64,
            optional: false,
            sensitive: false,
        })
        .collect()
}

fn policy(ack_timeout: Duration) -> ClientProducerPolicy {
    ClientProducerPolicy {
        window: AckWindow::Sequential,
        ack_timeout,
        retry_backoff: Duration::from_millis(10),
        retry_max_backoff: Duration::from_millis(100),
    }
}

fn limits(batches: u32, bytes: u64) -> ClientProducerLimits {
    ClientProducerLimits {
        batches: NonZeroU32::new(batches).assured("a literal non-zero count"),
        bytes: NonZeroU64::new(bytes).assured("a literal non-zero size"),
    }
}

fn submission(id: u64) -> ClientSubmissionId {
    ClientSubmissionId::new(NonZeroU64::new(id).assured("a literal non-zero identity"))
}

/// An endpoint task and the admission worker's side of its installed execution, which the test
/// plays.
struct Fixture {
    commands: mpsc::UnboundedSender<EndpointCommand>,
    jobs: mpsc::Receiver<AdmissionJob>,
    gauges: Arc<PublishedClientGauges>,
}

impl Fixture {
    fn start() -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let domain = DomainName::parse("tenant").assured("a literal domain name");
        let ingestor = IngestorName::parse("orders_in").assured("a literal ingestor name");
        let metrics = RuntimeMetrics::default();
        let gauges = Arc::new(PublishedClientGauges::default());
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
            gauges: gauges.clone(),
            published: ClientIngestorGauges::default(),
        };
        nervix_primitives::task::spawn(endpoint.run());
        let (_, jobs) = mpsc::channel(1);
        Self {
            commands,
            jobs,
            gauges,
        }
    }

    /// The counts the endpoint published once every command sent so far was handled.
    async fn settled_gauges(&self) -> ClientIngestorGauges {
        // An attach is answered only after every command sent before it was handled, and the
        // gauges are published after each command.
        let (reply, attached) = oneshot::channel();
        self.send(EndpointCommand::Attach(AttachCommand {
            expected_fields: fields(&["unmatched"]),
            limits: limits(1, 1),
            max_batch_bytes: NonZeroU64::MIN,
            serving: ProducerServing::Local,
            reply,
        }));
        let refused = timeout(WAIT, attached)
            .await
            .assured("the endpoint answers an open within the test's wait")
            .assured("the endpoint answers every open it receives");
        assert!(
            refused.is_err(),
            "an open expecting other fields is refused"
        );
        self.gauges.snapshot()
    }

    /// Installs an execution admitting `window` batches at once, and opens its intake.
    fn install(&mut self, window: usize, contract: u8, generation: u64, ack_timeout: Duration) {
        let (jobs, received) = mpsc::channel(1);
        self.jobs = received;
        let execution = Arc::new(ClientExecution {
            contract: ClientEndpointContract::from_digest([contract; 32]),
            generation,
            fields: fields(&["id"]),
            window: NonZeroUsize::new(window).assured("a literal non-zero window"),
            policy: policy(ack_timeout),
            jobs,
        });
        self.send(EndpointCommand::Install(execution));
        self.send(EndpointCommand::Intake(ClientIntakeState::Open));
    }

    fn send(&self, command: EndpointCommand) {
        self.commands
            .send(command)
            .assured("the endpoint task runs for the whole test");
    }

    async fn attach(
        &self,
        expected: Vec<SchemaField>,
        limits: ClientProducerLimits,
    ) -> Result<Producer, ClientProducerRefusal> {
        self.attach_serving(expected, limits, ProducerServing::Local)
            .await
    }

    /// Opens a producer that another node forwards, with the requests the endpoint sends that
    /// node to clear its batches.
    async fn attach_forwarded(
        &self,
        limits: ClientProducerLimits,
    ) -> (Producer, mpsc::UnboundedReceiver<ClientSubmissionId>) {
        let (clearance_requests, clearances) = mpsc::unbounded_channel();
        let reservation = ClientProducerBudget::default()
            .try_reserve(limits.bytes)
            .assured("an empty node budget holds one producer's bytes");
        let serving = ProducerServing::Forwarded {
            _reservation: reservation,
            clearance_requests,
        };
        let producer = self
            .attach_serving(fields(&["id"]), limits, serving)
            .await
            .assured("an open with the right schema attaches");
        (producer, clearances)
    }

    async fn attach_serving(
        &self,
        expected: Vec<SchemaField>,
        limits: ClientProducerLimits,
        serving: ProducerServing,
    ) -> Result<Producer, ClientProducerRefusal> {
        let (reply, answer) = oneshot::channel();
        self.send(EndpointCommand::Attach(AttachCommand {
            expected_fields: expected,
            limits,
            max_batch_bytes: NonZeroU64::new(1024).assured("a literal non-zero size"),
            serving,
            reply,
        }));
        let AttachedProducer {
            description,
            events,
        } = timeout(WAIT, answer)
            .await
            .assured("the endpoint answers an open")
            .assured("the endpoint keeps its reply sender")?;
        let handle = ClientProducerHandle {
            commands: self.commands.clone(),
            attachment: description.attachment,
            detached: false,
        };
        Ok(Producer {
            description,
            handle: Some(handle),
            events,
        })
    }

    /// The next batch the endpoint hands its worker.
    async fn next_job(&mut self) -> AdmissionJob {
        timeout(WAIT, self.jobs.recv())
            .await
            .assured("the endpoint hands the worker a batch")
            .assured("the execution keeps its job sender")
    }

    /// Asserts that the worker is handed nothing while the endpoint handles what was sent to it.
    async fn assert_no_job(&mut self, because: &str) {
        let handed = timeout(Duration::from_millis(200), self.jobs.recv()).await;
        assert!(handed.is_err(), "{because}");
    }

    /// Admits `job` under a fresh root, which the returned set resolves.
    fn admit(&self, job: &AdmissionJob, ack_timeout: Duration) -> AckSet {
        let (root, completion): (AckSet, AckCompletion) = AckSet::root();
        self.send(EndpointCommand::Admission(AdmissionReport {
            attachment: job.attachment,
            submission: job.submission,
            result: AdmissionResult::Admitted {
                completion,
                ack_timeout,
            },
        }));
        root
    }
}

/// One attached producer as a test drives it.
struct Producer {
    description: ClientProducerDescription,
    handle: Option<ClientProducerHandle>,
    events: ClientProducerEvents,
}

impl Producer {
    fn submit(&self, id: u64) {
        self.handle
            .as_ref()
            .assured("the test submits only while the producer is open")
            .submit(submission(id), Bytes::from_static(b"batch"));
    }

    /// Clears a batch the way the node that forwards this producer does.
    fn clear(&self, id: u64) {
        self.handle
            .as_ref()
            .assured("the test clears only while the producer is open")
            .clear(submission(id));
    }

    async fn next_event(&mut self) -> Option<ClientProducerEvent> {
        timeout(WAIT, self.events.outcomes.recv())
            .await
            .assured("the endpoint answers within the deadline")
    }

    async fn outcome(&mut self) -> (u64, ClientSubmissionOutcome) {
        match self.next_event().await {
            Some(ClientProducerEvent::Outcome {
                submission,
                outcome,
                ..
            }) => (submission.get().get(), outcome),
            other => panic!("expected an outcome, got {other:?}"),
        }
    }
}

#[nervix_primitives::test]
async fn an_open_needs_an_installed_execution_and_exactly_its_schema() {
    let mut fixture = Fixture::start();
    assert!(matches!(
        fixture.attach(fields(&["id"]), limits(4, 512)).await,
        Err(ClientProducerRefusal::EndpointUnavailable)
    ));
    fixture.install(1, 7, 3, WAIT);
    assert!(matches!(
        fixture.attach(fields(&["other"]), limits(4, 512)).await,
        Err(ClientProducerRefusal::SchemaMismatch)
    ));
    let producer = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("the expected schema is the execution's");
    let description = producer.description;
    assert_eq!(description.fields, fields(&["id"]));
    assert_eq!(description.generation, 3);
    assert_eq!(
        description.contract,
        ClientEndpointContract::from_digest([7; 32])
    );
    assert_eq!(description.grant.batches.get(), 4);
    assert_eq!(
        description.grant.max_batch_bytes.get(),
        1024,
        "one submission carries at most what the session's frame does"
    );
    assert_eq!(description.admission, ClientProducerAdmission::Open);
}

#[nervix_primitives::test]
async fn producers_take_the_one_window_in_turn() {
    let mut fixture = Fixture::start();
    fixture.install(1, 1, 1, WAIT);
    let mut first = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    let mut second = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    first.submit(1);
    first.submit(2);
    second.submit(3);

    let job = fixture.next_job().await;
    assert_eq!(job.submission, submission(1));
    let root = fixture.admit(&job, WAIT);
    // The window holds one admitted batch, so nothing else reaches the worker until it resolves.
    assert!(
        timeout(Duration::from_millis(200), fixture.jobs.recv())
            .await
            .is_err(),
        "a second batch waits for the window"
    );
    // Two producers hold three batches between them, and one slot of the window is all they use.
    assert_eq!(
        fixture.settled_gauges().await,
        ClientIngestorGauges {
            producers: 2,
            forwarded_producers: 0,
            outstanding_batches: 3,
            outstanding_bytes: 15,
            admitted_batches: 1,
        }
    );
    root.ack_success();
    assert_eq!(
        first.outcome().await,
        (1, ClientSubmissionOutcome::Completed)
    );

    // The other producer's batch goes next, ahead of the first producer's second one.
    let job = fixture.next_job().await;
    assert_eq!(job.submission, submission(3));
    fixture.admit(&job, WAIT).ack_success();
    assert_eq!(
        second.outcome().await,
        (3, ClientSubmissionOutcome::Completed)
    );
    let job = fixture.next_job().await;
    assert_eq!(job.submission, submission(2));
    fixture.admit(&job, WAIT).no_ack("a route rejected it");
    let (id, outcome) = first.outcome().await;
    assert_eq!(id, 2);
    assert_eq!(
        outcome,
        ClientSubmissionOutcome::ProcessingFailed(ClientProcessingFailure::Rejected)
    );
    assert_eq!(
        fixture.settled_gauges().await,
        ClientIngestorGauges {
            producers: 2,
            ..ClientIngestorGauges::default()
        },
        "every answered batch leaves the counts"
    );
}

#[nervix_primitives::test]
async fn a_batch_beyond_the_credit_ends_its_producer() {
    let mut fixture = Fixture::start();
    fixture.install(1, 1, 1, WAIT);
    let mut producer = fixture
        .attach(fields(&["id"]), limits(1, 4096))
        .await
        .assured("an open with the right schema attaches");
    producer.submit(1);
    let job = fixture.next_job().await;
    producer.submit(2);
    assert_eq!(
        producer.outcome().await,
        (
            2,
            ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::CreditExceeded)
        )
    );
    assert_eq!(
        producer.outcome().await,
        (
            1,
            ClientSubmissionOutcome::OutcomeUnknown(ClientOutcomeUncertainty::Interrupted)
        )
    );
    assert_eq!(
        producer.next_event().await,
        Some(ClientProducerEvent::Ended(
            ClientProducerEndReason::ProtocolViolated
        ))
    );
    // The batch the worker held still resolves; the ended producer is told nothing more.
    fixture.admit(&job, WAIT).ack_success();
    assert_eq!(producer.next_event().await, None);
}

#[nervix_primitives::test]
async fn a_suspension_refuses_queued_batches_and_reopening_admits_again() {
    let mut fixture = Fixture::start();
    fixture.install(1, 1, 1, WAIT);
    let mut producer = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    producer.submit(1);
    let job = fixture.next_job().await;
    producer.submit(2);
    fixture.send(EndpointCommand::Intake(ClientIntakeState::Suspended));
    assert_eq!(
        producer.outcome().await,
        (
            2,
            ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::Suspended)
        )
    );
    producer
        .events
        .admission
        .wait_for(|admission| *admission == ClientProducerAdmission::Suspended)
        .await
        .assured("the endpoint keeps the admission sender while attached");
    // The batch the worker held was admitted before the suspension and completes normally.
    fixture.admit(&job, WAIT).ack_success();
    assert_eq!(
        producer.outcome().await,
        (1, ClientSubmissionOutcome::Completed)
    );

    producer.submit(3);
    assert_eq!(
        producer.outcome().await,
        (
            3,
            ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::Suspended)
        )
    );
    fixture.send(EndpointCommand::Intake(ClientIntakeState::Open));
    producer
        .events
        .admission
        .wait_for(|admission| *admission == ClientProducerAdmission::Open)
        .await
        .assured("the endpoint keeps the admission sender while attached");
    producer.submit(4);
    let job = fixture.next_job().await;
    assert_eq!(job.submission, submission(4));
}

#[nervix_primitives::test]
async fn a_close_refuses_queued_batches_and_releases_after_the_admitted_ones() {
    let mut fixture = Fixture::start();
    fixture.install(1, 1, 1, WAIT);
    let mut producer = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    producer.submit(1);
    let job = fixture.next_job().await;
    producer.submit(2);
    producer
        .handle
        .take()
        .assured("the producer is open")
        .close();
    assert_eq!(
        producer.outcome().await,
        (
            2,
            ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::ProducerEnded)
        )
    );
    fixture.admit(&job, WAIT).ack_success();
    assert_eq!(
        producer.outcome().await,
        (1, ClientSubmissionOutcome::Completed)
    );
    assert_eq!(
        producer.next_event().await,
        None,
        "the release closes the events behind the last outcome"
    );
}

#[nervix_primitives::test]
async fn a_changed_contract_or_generation_ends_producers_and_an_unchanged_one_keeps_them() {
    let mut fixture = Fixture::start();
    fixture.install(1, 1, 1, WAIT);
    let mut kept = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    fixture.send(EndpointCommand::Uninstall);
    fixture.install(1, 1, 1, WAIT);
    kept.submit(1);
    let job = fixture.next_job().await;
    assert_eq!(
        job.submission,
        submission(1),
        "the producer survived the restart"
    );
    fixture.admit(&job, WAIT).ack_success();
    assert_eq!(
        kept.outcome().await,
        (1, ClientSubmissionOutcome::Completed)
    );

    fixture.install(1, 2, 1, WAIT);
    assert_eq!(
        kept.next_event().await,
        Some(ClientProducerEvent::Ended(
            ClientProducerEndReason::EndpointChanged
        ))
    );
    let mut restarted = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    fixture.install(1, 2, 2, WAIT);
    assert_eq!(
        restarted.next_event().await,
        Some(ClientProducerEvent::Ended(
            ClientProducerEndReason::DomainStopped
        ))
    );
}

#[nervix_primitives::test]
async fn an_uninstalled_execution_leaves_the_batch_its_aborted_worker_took_unknown() {
    let mut fixture = Fixture::start();
    fixture.install(1, 1, 1, WAIT);
    let mut producer = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    producer.submit(1);
    // The worker takes the batch and is aborted before it reports it, possibly mid-dispatch.
    let _job = fixture.next_job().await;
    fixture.send(EndpointCommand::Uninstall);
    assert_eq!(
        producer.outcome().await,
        (
            1,
            ClientSubmissionOutcome::OutcomeUnknown(ClientOutcomeUncertainty::Interrupted)
        )
    );
}

#[nervix_primitives::test]
async fn a_batch_a_stopped_worker_never_took_is_refused_and_so_is_every_later_one() {
    let mut fixture = Fixture::start();
    fixture.install(1, 1, 1, WAIT);
    let mut producer = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    producer.submit(1);
    producer.submit(2);
    // The worker stops with the first batch handed over and never taken: it refuses that batch,
    // and the endpoint refuses the next one the closed channel no longer takes.
    let (stop, shutdown) = watch::channel(false);
    let mut mailbox = WorkerMailbox {
        jobs: std::mem::replace(&mut fixture.jobs, mpsc::channel(1).1),
        reports: fixture.commands.clone(),
        shutdown,
    };
    stop.send_replace(true);
    let taken = timeout(WAIT, mailbox.next_job())
        .await
        .assured("a stopped worker takes nothing more within the test's wait");
    assert!(taken.is_none(), "a stopped worker takes no batch");
    assert_eq!(
        producer.outcome().await,
        (
            1,
            ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::Suspended)
        )
    );
    assert_eq!(
        producer.outcome().await,
        (
            2,
            ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::Suspended)
        )
    );
}

#[nervix_primitives::test]
async fn ending_the_endpoint_leaves_admitted_batches_unknown() {
    let mut fixture = Fixture::start();
    fixture.install(1, 1, 1, WAIT);
    let mut producer = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    producer.submit(1);
    let job = fixture.next_job().await;
    let _root = fixture.admit(&job, WAIT);
    producer.submit(2);
    let (done, ended) = oneshot::channel();
    fixture.send(EndpointCommand::End {
        reason: ClientProducerEndReason::ShuttingDown,
        done,
    });
    timeout(WAIT, ended)
        .await
        .assured("the endpoint ends within the deadline")
        .assured("the endpoint answers the end");
    let mut outcomes = vec![producer.outcome().await, producer.outcome().await];
    outcomes.sort_by_key(|(id, _)| *id);
    assert_eq!(
        outcomes,
        vec![
            (
                1,
                ClientSubmissionOutcome::OutcomeUnknown(ClientOutcomeUncertainty::Interrupted)
            ),
            (
                2,
                ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::ProducerEnded)
            ),
        ]
    );
    assert_eq!(
        producer.next_event().await,
        Some(ClientProducerEvent::Ended(
            ClientProducerEndReason::ShuttingDown
        ))
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn an_acknowledgement_without_progress_times_out() {
    let mut fixture = Fixture::start();
    fixture.install(1, 1, 1, Duration::from_millis(50));
    let mut producer = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    producer.submit(1);
    let job = fixture.next_job().await;
    let _root = fixture.admit(&job, Duration::from_millis(50));
    let (id, outcome) = producer.outcome().await;
    assert_eq!(id, 1);
    assert_eq!(
        outcome,
        ClientSubmissionOutcome::ProcessingFailed(ClientProcessingFailure::AckTimedOut)
    );
}

#[test]
fn the_node_budget_refuses_what_it_cannot_hold_and_takes_back_what_it_gave() {
    let budget = ClientProducerBudget::default();
    let half = NonZeroU64::new(CLIENT_PRODUCER_NODE_BYTES / 2).assured("a non-zero half");
    let first = budget.try_reserve(half).assured("half the budget fits");
    let second = budget.try_reserve(half).assured("the other half fits");
    assert!(budget.try_reserve(NonZeroU64::MIN).is_none());
    drop(first);
    assert_eq!(budget.reserved(), half.get());
    drop(second);
    assert_eq!(budget.reserved(), 0);
}

#[test]
fn a_detail_is_cut_to_its_bound_at_a_character_boundary() {
    let detail = "é".repeat(MAX_OUTCOME_DETAIL_BYTES);
    let bounded = bounded_detail(detail);
    assert!(bounded.len() <= MAX_OUTCOME_DETAIL_BYTES);
    assert!(bounded.chars().all(|character| character == 'é'));
    assert_eq!(bounded_detail("short".to_string()), "short");
}

#[test]
fn a_batch_validated_under_a_hold_is_refused_with_its_root_resolved() {
    let metrics = RuntimeMetrics::default();
    let domain = DomainName::parse("tenant").assured("a literal domain name");
    let ingestor = IngestorName::parse("orders_in").assured("a literal ingestor name");
    let metric_labels = metrics.register_ingestor_quiesce(&domain, &ingestor, None);
    let control = IngestorQuiesceControl::new(IngestQuiesceMode::Suspend, metrics, metric_labels);
    let trackers = IngestorAckRootTrackers::detached();

    let (root, _completion) = control
        .track_client_batch(&trackers)
        .assured("an open control admits the batch");
    assert_eq!(
        trackers.ingestor_outstanding(),
        1,
        "an admitted batch's root is tracked before it is dispatched"
    );
    root.ack_success();
    assert_eq!(trackers.ingestor_outstanding(), 0);

    control.engage(IngestorQuiesceCause::EntityHold);
    let refused = control
        .track_client_batch(&trackers)
        .err()
        .assured("an engaged hold refuses the batch");
    assert_eq!(refused, ClientSubmissionRefusal::Suspended);
    assert_eq!(
        trackers.ingestor_outstanding(),
        0,
        "a refused batch leaves no root for a drain to wait for"
    );

    control.release(IngestorQuiesceCause::EntityHold);
    control.engage(IngestorQuiesceCause::OwnershipHandoff);
    let refused = control
        .track_client_batch(&trackers)
        .err()
        .assured("an ownership handoff refuses the batch for good");
    assert_eq!(refused, ClientSubmissionRefusal::Draining);
}

/// The next batch the endpoint asks the forwarding node to clear.
async fn next_clearance(clearances: &mut mpsc::UnboundedReceiver<ClientSubmissionId>) -> u64 {
    let requested = timeout(WAIT, clearances.recv())
        .await
        .assured("the endpoint asks for the clearance within the deadline")
        .assured("the attachment keeps its clearance requests");
    requested.get().get()
}

#[nervix_primitives::test]
async fn a_forwarded_batch_reaches_the_worker_only_after_its_serving_node_cleared_it() {
    let mut fixture = Fixture::start();
    fixture.install(2, 1, 1, WAIT);
    let (mut producer, mut clearances) = fixture.attach_forwarded(limits(4, 4096)).await;
    producer.submit(1);
    producer.submit(2);
    producer.submit(3);
    // Each of the two turns the window has room for asks for a clearance and holds a slot, and
    // the third batch stays queued.
    assert_eq!(next_clearance(&mut clearances).await, 1);
    assert_eq!(next_clearance(&mut clearances).await, 2);
    assert_eq!(
        fixture.settled_gauges().await,
        ClientIngestorGauges {
            producers: 1,
            forwarded_producers: 1,
            outstanding_batches: 3,
            outstanding_bytes: 15,
            admitted_batches: 2,
        }
    );
    fixture
        .assert_no_job("no batch reaches the worker before its serving node cleared it")
        .await;

    // Clearances come back in the order they were asked for, so one naming a batch the endpoint
    // is not waiting on first changes nothing.
    producer.clear(3);
    producer.clear(2);
    fixture
        .assert_no_job("a clearance out of order clears nothing")
        .await;
    producer.clear(1);
    let job = fixture.next_job().await;
    assert_eq!(job.submission, submission(1));
    fixture.admit(&job, WAIT).ack_success();
    assert_eq!(
        producer.outcome().await,
        (1, ClientSubmissionOutcome::Completed)
    );
    // The slot the first batch freed goes to the third, which is asked about in turn.
    assert_eq!(next_clearance(&mut clearances).await, 3);
    producer.clear(2);
    let job = fixture.next_job().await;
    assert_eq!(job.submission, submission(2));
    fixture.admit(&job, WAIT).ack_success();
    producer.clear(3);
    let job = fixture.next_job().await;
    assert_eq!(job.submission, submission(3));
    fixture.admit(&job, WAIT).ack_success();
    let mut outcomes = vec![producer.outcome().await, producer.outcome().await];
    outcomes.sort_by_key(|(id, _)| *id);
    assert_eq!(
        outcomes,
        vec![
            (2, ClientSubmissionOutcome::Completed),
            (3, ClientSubmissionOutcome::Completed),
        ]
    );
}

#[nervix_primitives::test]
async fn detaching_a_forwarded_producer_returns_the_slots_its_uncleared_batches_held() {
    let mut fixture = Fixture::start();
    fixture.install(1, 1, 1, WAIT);
    let (mut forwarded, mut clearances) = fixture.attach_forwarded(limits(4, 4096)).await;
    let local = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    forwarded.submit(1);
    assert_eq!(next_clearance(&mut clearances).await, 1);
    local.submit(2);
    fixture
        .assert_no_job("the window's one slot is held by the batch being cleared")
        .await;
    // The forwarding node is gone, and the batch it never cleared leaves with its producer.
    drop(forwarded.handle.take());
    let job = fixture.next_job().await;
    assert_eq!(
        job.submission,
        submission(2),
        "the detach returned the slot to the window"
    );
}

#[nervix_primitives::test]
async fn a_suspension_refuses_batches_awaiting_clearance_and_ignores_their_late_clearance() {
    let mut fixture = Fixture::start();
    fixture.install(1, 1, 1, WAIT);
    let (mut producer, mut clearances) = fixture.attach_forwarded(limits(4, 4096)).await;
    producer.submit(1);
    assert_eq!(next_clearance(&mut clearances).await, 1);
    fixture.send(EndpointCommand::Intake(ClientIntakeState::Suspended));
    assert_eq!(
        producer.outcome().await,
        (
            1,
            ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::Suspended)
        )
    );
    // The serving node cleared the batch before it learnt of the refusal.
    producer.clear(1);
    fixture.send(EndpointCommand::Intake(ClientIntakeState::Open));
    producer.submit(2);
    assert_eq!(
        next_clearance(&mut clearances).await,
        2,
        "the refused batch returned its slot"
    );
    producer.clear(2);
    let job = fixture.next_job().await;
    assert_eq!(
        job.submission,
        submission(2),
        "a late clearance of a refused batch admits nothing"
    );
}

#[nervix_primitives::test]
async fn ending_the_endpoint_refuses_a_cleared_batch_that_never_reached_the_worker() {
    let mut fixture = Fixture::start();
    fixture.install(2, 1, 1, WAIT);
    let (mut producer, mut clearances) = fixture.attach_forwarded(limits(4, 4096)).await;
    producer.submit(1);
    producer.submit(2);
    assert_eq!(next_clearance(&mut clearances).await, 1);
    assert_eq!(next_clearance(&mut clearances).await, 2);
    producer.clear(1);
    producer.clear(2);
    // The worker takes the first cleared batch; the second waits for it.
    let job = fixture.next_job().await;
    assert_eq!(job.submission, submission(1));
    let (done, ended) = oneshot::channel();
    fixture.send(EndpointCommand::End {
        reason: ClientProducerEndReason::ShuttingDown,
        done,
    });
    timeout(WAIT, ended)
        .await
        .assured("the endpoint ends within the deadline")
        .assured("the endpoint answers the end");
    let mut outcomes = vec![producer.outcome().await, producer.outcome().await];
    outcomes.sort_by_key(|(id, _)| *id);
    assert_eq!(
        outcomes,
        vec![
            (
                1,
                ClientSubmissionOutcome::OutcomeUnknown(ClientOutcomeUncertainty::Interrupted)
            ),
            (
                2,
                ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::ProducerEnded)
            ),
        ]
    );
    assert_eq!(
        producer.next_event().await,
        Some(ClientProducerEvent::Ended(
            ClientProducerEndReason::ShuttingDown
        ))
    );
}

#[nervix_primitives::test]
async fn a_local_producer_takes_its_turn_while_the_worker_is_busy_with_a_forwarded_batch() {
    let mut fixture = Fixture::start();
    fixture.install(2, 1, 1, WAIT);
    let local = fixture
        .attach(fields(&["id"]), limits(4, 4096))
        .await
        .assured("an open with the right schema attaches");
    let (forwarded, mut clearances) = fixture.attach_forwarded(limits(4, 4096)).await;
    local.submit(1);
    let first = fixture.next_job().await;
    assert_eq!(first.submission, submission(1));
    forwarded.submit(2);
    assert_eq!(next_clearance(&mut clearances).await, 2);
    // The window is full, so both producers queue their next batch.
    local.submit(3);
    forwarded.submit(4);
    forwarded.clear(2);
    let first_root = fixture.admit(&first, WAIT);
    let second = fixture.next_job().await;
    assert_eq!(second.submission, submission(2));
    // The first batch's slot frees while the worker holds the second: the local producer's turn
    // comes next, and a busy worker does not pass it over for the forwarded producer.
    first_root.ack_success();
    let refused_turn = timeout(Duration::from_millis(200), clearances.recv()).await;
    assert!(
        refused_turn.is_err(),
        "the freed slot went to the forwarded producer out of turn"
    );
    fixture.admit(&second, WAIT).ack_success();
    let third = fixture.next_job().await;
    assert_eq!(
        third.submission,
        submission(3),
        "the local batch took its turn while the worker was busy"
    );
    assert_eq!(next_clearance(&mut clearances).await, 4);
}
