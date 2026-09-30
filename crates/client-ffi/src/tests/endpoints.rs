//! Producers, consumers and deliveries through the C ABI, as a host calls it, against an
//! in-process session server the test answers by hand.
//!
//! Every outcome class, settlement and refusal a host reads is a real frame the Rust client
//! decoded. The server ends an exchange under a settlement to show an unknown confirmation, and
//! under a producer and a consumer to show what a host reads across a reconnect.

use std::{
    num::{NonZeroU32, NonZeroU64},
    ptr,
    time::Duration,
};

use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::{
    AckWindow, ClientAttachmentId, ClientEndpointContract, ClientProducerAdmission,
    ClientProducerDescription, ClientProducerGrant, ClientProducerPolicy, ClientProducerRefusal,
    ConsumerConnection, ConsumerReopenReason, DomainName, EmitterName, IngestorName,
    ProducerConnection, ProducerReopenReason, Timestamp,
    wire::{
        ClientRequest, CloseEmitterOutcome, CloseIngestorDisposition, CloseIngestorOutcome,
        EmitterBatchDecision, EmitterBatchReceived, EmitterCloseDisposition, EmitterOpenRefusal,
        EmitterOpened, EmitterSettlement, OpenEmitterDisposition, OpenEmitterOutcome,
        OpenIngestorDisposition, OpenIngestorOutcome, ProducerOpened, ReadEmitterBatchOutcome,
        ReadEmitterDisposition, Reply, ReplyBody, RequestId, SettleEmitterBatchOutcome,
        SubmissionOutcome as WireSubmissionOutcome,
    },
};
use nervix_models::{
    ClientBatchDefect, ClientConsumerLimits, ClientOutcomeUncertainty, ClientProcessingFailure,
    ClientSubmissionOutcome, ClientSubmissionRefusal, ParseAsType, RelayName, SchemaField,
};
use nervix_primitives::thread;
use uuid::Uuid;

use super::{
    batches::{HostColumn, HostList, HostValues, SharedBatch, build, read, stream, varlen},
    clock_events::{ServerExchange, SharedSession, TestServer},
    failure_kind, field, succeeded,
};
use crate::{
    Admission, BatchDefect, Cancel, Consumer, Delivery, EndpointState, FailureKind, Fields,
    OpenRefusal, Part, ProcessingFailure, Producer, Reopen, ReopenReason, Schema, Settlement,
    SubmissionOutcome, SubmissionRefusal, SubmissionResult, Uncertainty, WindowKind,
    nx_cancel_free, nx_cancel_with_deadline, nx_consumer_close, nx_consumer_contract,
    nx_consumer_free, nx_consumer_generation, nx_consumer_grant, nx_consumer_next,
    nx_consumer_policy, nx_consumer_reopen_reason, nx_consumer_schema, nx_consumer_state,
    nx_delivery_ack, nx_delivery_batch, nx_delivery_branch_fingerprint, nx_delivery_execution_now,
    nx_delivery_identity, nx_delivery_ipc, nx_delivery_members, nx_delivery_reference,
    nx_delivery_reject, nx_delivery_release, nx_delivery_retain, nx_delivery_retry,
    nx_delivery_source_relay, nx_error_free, nx_error_kind_of, nx_error_open_refusal,
    nx_fields_add, nx_fields_element, nx_fields_free, nx_fields_new, nx_producer_admission,
    nx_producer_close, nx_producer_contract, nx_producer_free, nx_producer_generation,
    nx_producer_grant, nx_producer_pending, nx_producer_policy, nx_producer_rejoin,
    nx_producer_release, nx_producer_reopen_reason, nx_producer_schema, nx_producer_state,
    nx_producer_submit, nx_producer_submit_ipc, nx_schema_free, nx_session_open_ingestor,
    nx_session_subscribe_emitter, nx_submission_outcome_defect, nx_submission_outcome_failure,
    nx_submission_outcome_free, nx_submission_outcome_message, nx_submission_outcome_refusal,
    nx_submission_outcome_result, nx_submission_outcome_uncertainty,
};

/// The type codes the header gives the types these tests name.
const U32: i32 = 5;
const STRING: i32 = 12;
const LIST: i32 = 16;

fn domain() -> DomainName {
    DomainName::parse("sim").assured("the test domain name is valid")
}

fn ingestor() -> IngestorName {
    IngestorName::parse("orders_in").assured("the test ingestor name is valid")
}

fn emitter() -> EmitterName {
    EmitterName::parse("orders_out").assured("the test emitter name is valid")
}

/// The fields of every endpoint these tests open: an id and a list of tags.
fn endpoint_fields() -> Vec<SchemaField> {
    vec![
        field("id", ParseAsType::U32),
        field(
            "tags",
            ParseAsType::Vec {
                element: Box::new(ParseAsType::String),
            },
        ),
    ]
}

/// The same fields, as a host builds them.
struct HostFields(*mut Fields);

impl HostFields {
    fn endpoint() -> Self {
        let fields = Self(nx_fields_new());
        // SAFETY: the field list is live and the names address their lengths.
        unsafe {
            succeeded(nx_fields_add(
                fields.0,
                "id".as_ptr(),
                2,
                U32,
                0,
                false,
                false,
            ));
            succeeded(nx_fields_add(
                fields.0,
                "tags".as_ptr(),
                4,
                LIST,
                0,
                false,
                false,
            ));
            succeeded(nx_fields_element(fields.0, STRING, 0));
        }
        fields
    }
}

// SAFETY: a field list is read, and never changed, by an open on another thread.
unsafe impl Send for HostFields {}

impl Drop for HostFields {
    fn drop(&mut self) {
        // SAFETY: the field list is live and freed once, here.
        unsafe { nx_fields_free(self.0) };
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).assured("a test duration fits u64 nanoseconds")
}

fn producer_description() -> ClientProducerDescription {
    ClientProducerDescription {
        attachment: ClientAttachmentId::from_u128(7),
        fields: endpoint_fields(),
        generation: 3,
        contract: ClientEndpointContract::from_digest([5; 32]),
        policy: ClientProducerPolicy {
            window: AckWindow::Parallel {
                max: NonZeroU64::new(4).assured("four is not zero"),
            },
            ack_timeout: Duration::from_secs(30),
            retry_backoff: Duration::from_millis(100),
            retry_max_backoff: Duration::from_secs(1),
        },
        grant: ClientProducerGrant {
            batches: NonZeroU32::new(2).assured("two is not zero"),
            bytes: NonZeroU64::new(4096).assured("a literal size is not zero"),
            max_batch_bytes: NonZeroU64::new(4096).assured("a literal size is not zero"),
            max_batch_rows: NonZeroU32::new(16).assured("a literal count is not zero"),
        },
        admission: ClientProducerAdmission::Open,
    }
}

fn consumer_description() -> EmitterOpened {
    EmitterOpened {
        domain: domain(),
        emitter: emitter(),
        fields: endpoint_fields(),
        generation: 3,
        contract: ClientEndpointContract::from_digest([6; 32]),
        window: AckWindow::Sequential,
        ack_timeout: Duration::from_secs(20),
        retry_backoff: Duration::from_millis(50),
        retry_max_backoff: Duration::from_secs(2),
        granted: ClientConsumerLimits {
            batches: NonZeroU32::new(2).assured("two is not zero"),
            bytes: NonZeroU64::new(8192).assured("a literal size is not zero"),
        },
        max_batch_bytes: 8192,
        max_batch_rows: 32,
    }
}

impl ServerExchange {
    fn answer(&self, runtime: &TestServer, request_id: RequestId, body: ReplyBody) {
        runtime
            .runtime
            .block_on(self.reply(Reply { request_id, body }));
    }
}

/// The next request the session sends on `exchange`.
fn next(server: &TestServer, exchange: &mut ServerExchange) -> (RequestId, ClientRequest) {
    let message = server.runtime.block_on(exchange.next_request());
    (message.request_id, message.request)
}

/// A producer shared with the threads that block in it, as the binding allows.
#[derive(Clone, Copy)]
struct SharedProducer(*mut Producer);

// SAFETY: the binding allows a producer to be used from several threads at once.
unsafe impl Send for SharedProducer {}

/// A consumer shared with the threads that block in it, as the binding allows.
#[derive(Clone, Copy)]
struct SharedConsumer(*mut Consumer);

// SAFETY: the binding allows a consumer to be used from several threads at once.
unsafe impl Send for SharedConsumer {}

/// One reference to a delivery, released when dropped.
struct SharedDelivery(*mut Delivery);

// SAFETY: a delivery's references are counted atomically, and it may be settled and released
// from any thread.
unsafe impl Send for SharedDelivery {}

impl Drop for SharedDelivery {
    fn drop(&mut self) {
        // SAFETY: the reference is live and released once, here.
        unsafe { nx_delivery_release(self.0) };
    }
}

/// A submission outcome, freed when dropped.
struct Taken(*mut SubmissionOutcome);

impl Drop for Taken {
    fn drop(&mut self) {
        // SAFETY: the outcome is live and freed once, here.
        unsafe { nx_submission_outcome_free(self.0) };
    }
}

/// A token that expires after `millis`, freed when dropped.
struct Deadline(*mut Cancel);

// SAFETY: a token may be waited on from any thread.
unsafe impl Send for Deadline {}

impl Deadline {
    fn after(millis: u64) -> Self {
        let mut token = ptr::null_mut();
        // SAFETY: `token` is writable.
        succeeded(unsafe { nx_cancel_with_deadline(millis, &mut token) });
        Self(token)
    }
}

impl Drop for Deadline {
    fn drop(&mut self) {
        // SAFETY: no call waits on the token any more, and it is freed once, here.
        unsafe { nx_cancel_free(self.0) };
    }
}

/// How long a test lets a blocking call run before it has to have returned.
const WAIT_MILLIS: u64 = 30_000;

impl SharedSession {
    fn open_ingestor(self, fields: &HostFields) -> Result<SharedProducer, FailureKind> {
        let mut producer = ptr::null_mut();
        let name = "orders_in";
        let domain = "sim";
        // SAFETY: the session and the fields are live, the names address their lengths, and
        // `producer` is writable.
        let failure = unsafe {
            nx_session_open_ingestor(
                self.0,
                domain.as_ptr(),
                domain.len(),
                name.as_ptr(),
                name.len(),
                fields.0,
                2,
                4096,
                ptr::null(),
                &mut producer,
            )
        };
        if failure.is_null() {
            return Ok(SharedProducer(producer));
        }
        Err(failure_kind(failure))
    }

    fn subscribe_emitter(self, fields: &HostFields) -> Result<SharedConsumer, FailureKind> {
        let mut consumer = ptr::null_mut();
        let name = "orders_out";
        let domain = "sim";
        // SAFETY: the session and the fields are live, the names address their lengths, and
        // `consumer` is writable.
        let failure = unsafe {
            nx_session_subscribe_emitter(
                self.0,
                domain.as_ptr(),
                domain.len(),
                name.as_ptr(),
                name.len(),
                fields.0,
                2,
                8192,
                ptr::null(),
                &mut consumer,
            )
        };
        if failure.is_null() {
            return Ok(SharedConsumer(consumer));
        }
        Err(failure_kind(failure))
    }
}

/// Opens a producer on `session`, answering its open on `exchange` with the test's description.
fn open_producer(
    server: &TestServer,
    exchange: &mut ServerExchange,
    session: SharedSession,
) -> SharedProducer {
    let opening = thread::spawn(move || session.open_ingestor(&HostFields::endpoint()));
    let (request_id, request) = next(server, exchange);
    let ClientRequest::OpenIngestor(open) = request else {
        panic!("an open is an OpenIngestor request");
    };
    assert_eq!(open.domain, domain());
    assert_eq!(open.ingestor, ingestor());
    assert_eq!(open.expected_fields, endpoint_fields());
    assert_eq!(open.limits.batches.get(), 2);
    assert_eq!(open.limits.bytes.get(), 4096);
    exchange.answer(server, request_id, opened_producer());
    opening
        .join()
        .assured("the opening thread returns")
        .assured("an answered open attaches a producer")
}

fn opened_producer() -> ReplyBody {
    ReplyBody::OpenIngestor(OpenIngestorOutcome {
        disposition: OpenIngestorDisposition::Opened(Box::new(ProducerOpened {
            domain: domain(),
            ingestor: ingestor(),
            description: producer_description(),
        })),
        message: "producer attached".to_string(),
    })
}

/// Opens a consumer on `session`, answering its open on `exchange` with the test's description.
fn open_consumer(
    server: &TestServer,
    exchange: &mut ServerExchange,
    session: SharedSession,
) -> SharedConsumer {
    let opening = thread::spawn(move || session.subscribe_emitter(&HostFields::endpoint()));
    let (request_id, request) = next(server, exchange);
    let ClientRequest::OpenEmitter(open) = request else {
        panic!("an open is an OpenEmitter request");
    };
    assert_eq!(open.expected_fields, endpoint_fields());
    exchange.answer(
        server,
        request_id,
        ReplyBody::OpenEmitter(OpenEmitterOutcome {
            disposition: OpenEmitterDisposition::Opened(Box::new(consumer_description())),
            message: String::new(),
        }),
    );
    opening
        .join()
        .assured("the opening thread returns")
        .assured("an answered open attaches a consumer")
}

/// The columns of a batch of `ids`, each with one tag.
fn endpoint_columns(ids: &[u32]) -> Vec<HostColumn> {
    let tags: Vec<String> = ids.iter().map(|id| format!("tag-{id}")).collect();
    vec![
        HostColumn {
            states: None,
            lists: Vec::new(),
            values: HostValues::Fixed(ids.iter().flat_map(|id| id.to_ne_bytes()).collect()),
        },
        HostColumn {
            states: None,
            lists: vec![HostList::Variable(
                (0..=ids.len())
                    .map(|end| u64::try_from(end).assured("a test batch is small"))
                    .collect(),
            )],
            values: varlen(tags.iter().map(String::as_bytes)),
        },
    ]
}

fn endpoint_schema() -> Schema {
    Schema::of_fields(endpoint_fields())
}

impl SharedProducer {
    fn submit(self, batch: &SharedBatch) -> Result<u64, FailureKind> {
        let mut submission = 0;
        // SAFETY: the producer and the batch are live, and `submission` is writable.
        let failure = unsafe { nx_producer_submit(self.0, batch.0, ptr::null(), &mut submission) };
        if failure.is_null() {
            return Ok(submission);
        }
        Err(failure_kind(failure))
    }

    fn rejoin_within(self, submission: u64, millis: u64) -> Result<Taken, FailureKind> {
        let deadline = Deadline::after(millis);
        let mut outcome = ptr::null_mut();
        // SAFETY: the producer and the token are live, and `outcome` is writable.
        let failure = unsafe { nx_producer_rejoin(self.0, submission, deadline.0, &mut outcome) };
        if failure.is_null() {
            return Ok(Taken(outcome));
        }
        Err(failure_kind(failure))
    }

    fn rejoin(self, submission: u64) -> Taken {
        self.rejoin_within(submission, WAIT_MILLIS)
            .assured("the submission's outcome arrives within the test's wait")
    }

    fn pending(self) -> Vec<(u64, bool)> {
        let mut count = 0;
        let mut submissions = [0_u64; 4];
        let mut resolved = [false; 4];
        // SAFETY: the producer is live and both buffers hold four entries.
        succeeded(unsafe {
            nx_producer_pending(
                self.0,
                submissions.as_mut_ptr(),
                resolved.as_mut_ptr(),
                4,
                &mut count,
            )
        });
        submissions.into_iter().zip(resolved).take(count).collect()
    }

    fn state(self) -> EndpointState {
        // SAFETY: the producer is live.
        unsafe { nx_producer_state(self.0) }
    }

    fn free(self) {
        // SAFETY: the producer is live, no thread uses it any more, and it is freed once, here.
        unsafe { nx_producer_free(self.0) };
    }
}

impl Taken {
    fn result(&self) -> SubmissionResult {
        // SAFETY: the outcome is live.
        unsafe { nx_submission_outcome_result(self.0) }
    }

    fn message(&self) -> String {
        let mut message = ptr::null();
        let mut message_len = 0;
        // SAFETY: the outcome is live and the message is copied before it is freed.
        unsafe {
            nx_submission_outcome_message(self.0, &mut message, &mut message_len);
            String::from_utf8_lossy(std::slice::from_raw_parts(message, message_len)).into_owned()
        }
    }
}

impl SharedConsumer {
    fn next_within(self, millis: u64) -> Result<SharedDelivery, FailureKind> {
        let deadline = Deadline::after(millis);
        let mut delivery = ptr::null_mut();
        // SAFETY: the consumer and the token are live, and `delivery` is writable.
        let failure = unsafe { nx_consumer_next(self.0, deadline.0, &mut delivery) };
        if failure.is_null() {
            return Ok(SharedDelivery(delivery));
        }
        Err(failure_kind(failure))
    }

    fn state(self) -> EndpointState {
        // SAFETY: the consumer is live.
        unsafe { nx_consumer_state(self.0) }
    }

    fn free(self) {
        // SAFETY: the consumer is live, no thread uses it any more, and it is freed once, here.
        unsafe { nx_consumer_free(self.0) };
    }
}

impl SharedDelivery {
    fn settle(
        &self,
        settle: unsafe extern "C" fn(
            *const Delivery,
            *const Cancel,
            *mut Settlement,
        ) -> *mut crate::Failure,
    ) -> Result<Settlement, FailureKind> {
        let deadline = Deadline::after(WAIT_MILLIS);
        let mut settlement = Settlement::ConsumerEnded;
        // SAFETY: the delivery and the token are live, and `settlement` is writable.
        let failure = unsafe { settle(self.0, deadline.0, &mut settlement) };
        if failure.is_null() {
            return Ok(settlement);
        }
        Err(failure_kind(failure))
    }

    fn bytes(
        &self,
        read: unsafe extern "C" fn(*const Delivery, *mut *const u8, *mut usize),
    ) -> Vec<u8> {
        let mut data = ptr::null();
        let mut data_len = 0;
        // SAFETY: the delivery is live and the bytes are copied before it can be released.
        unsafe {
            read(self.0, &mut data, &mut data_len);
            std::slice::from_raw_parts(data, data_len).to_vec()
        }
    }
}

fn batch_received(identity: u128, reference: u128, ipc: Vec<u8>, members: u32) -> ReplyBody {
    ReplyBody::ReadEmitterBatch(ReadEmitterBatchOutcome {
        disposition: ReadEmitterDisposition::Batch(EmitterBatchReceived {
            identity: Uuid::from_u128(identity),
            reference: Uuid::from_u128(reference),
            source_relay: RelayName::parse("orders").assured("the test relay name is valid"),
            branch_fingerprint: Some([9; 32]),
            batch: Bytes::from(ipc),
            members,
            execution_now: Timestamp::from_unix_nanos(1_700_000_000_000_000_007),
        }),
        message: String::new(),
    })
}

fn settled(disposition: EmitterSettlement) -> ReplyBody {
    ReplyBody::SettleEmitterBatch(SettleEmitterBatchOutcome {
        disposition,
        message: String::new(),
    })
}

#[test]
fn a_producer_reports_its_description_and_takes_every_outcome_class() {
    let mut server = TestServer::start();
    let session = SharedSession::connect(server.address);
    let mut exchange = server.next_exchange();
    let producer = open_producer(&server, &mut exchange, session);

    let mut schema = ptr::null_mut();
    let mut digest = ptr::null();
    let mut digest_len = 0;
    let mut grant = (0_u32, 0_u64, 0_u32, 0_u64);
    let mut window = WindowKind::Sequential;
    let mut policy = (0_u64, 0_u64, 0_u64, 0_u64);
    // SAFETY: the producer is live and every out-parameter is writable.
    unsafe {
        succeeded(nx_producer_schema(producer.0, &mut schema));
        assert_eq!((*schema).fields(Part::Rows), endpoint_fields().as_slice());
        assert_eq!((*schema).branch(), None);
        nx_schema_free(schema);
        assert_eq!(nx_producer_generation(producer.0), 3);
        nx_producer_contract(producer.0, &mut digest, &mut digest_len);
        assert_eq!(std::slice::from_raw_parts(digest, digest_len), &[5; 32]);
        nx_producer_grant(
            producer.0,
            &mut grant.0,
            &mut grant.1,
            &mut grant.2,
            &mut grant.3,
        );
        assert_eq!(grant, (2, 4096, 16, 4096));
        nx_producer_policy(
            producer.0,
            &mut window,
            &mut policy.0,
            &mut policy.1,
            &mut policy.2,
            &mut policy.3,
        );
        assert_eq!(window, WindowKind::Parallel);
        assert_eq!(
            policy,
            (
                4,
                nanos(Duration::from_secs(30)),
                nanos(Duration::from_millis(100)),
                nanos(Duration::from_secs(1))
            )
        );
        assert_eq!(nx_producer_admission(producer.0), Admission::Open);
        assert!(!nx_producer_reopen_reason(
            producer.0,
            ptr::null_mut(),
            ptr::null_mut()
        ));
    }
    assert_eq!(producer.state(), EndpointState::Active);

    // A built batch is sent as the canonical stream the batch reads as.
    let batch = build(&endpoint_schema(), 2, &endpoint_columns(&[1, 2]));
    let submitting = thread::spawn({
        let batch = SharedBatch(unsafe { crate::nx_batch_retain(batch.0) });
        move || producer.submit(&batch)
    });
    let (request_id, request) = next(&server, &mut exchange);
    let ClientRequest::SubmitBatch(submit) = request else {
        panic!("a batch is a SubmitBatch request");
    };
    assert_eq!(submit.batch.as_ref(), stream(&batch).as_slice());
    let first = submitting
        .join()
        .assured("the submitting thread returns")
        .assured("a batch within the credit is submitted");
    // Until the server answers, the submission has no outcome, and a wait for it expires.
    assert_eq!(producer.pending(), vec![(first, false)]);
    assert_eq!(
        producer.rejoin_within(first, 50).err(),
        Some(FailureKind::Deadline)
    );
    exchange.answer(
        &server,
        request_id,
        ReplyBody::Submission(WireSubmissionOutcome {
            outcome: ClientSubmissionOutcome::Completed,
            message: String::new(),
        }),
    );
    let completed = producer.rejoin(first);
    assert_eq!(completed.result(), SubmissionResult::Completed);
    assert_eq!(completed.message(), "");
    let mut refusal = SubmissionRefusal::Busy;
    // SAFETY: the outcome is live and `refusal` is writable.
    assert_eq!(
        failure_kind(unsafe { nx_submission_outcome_refusal(completed.0, &mut refusal) }),
        FailureKind::Type
    );
    assert!(producer.pending().is_empty());

    // A stream the host wrote itself is copied and sent as it is; the server judges it.
    let mut written = stream(&batch);
    let raw = thread::spawn({
        let bytes = written.clone();
        move || {
            let producer = producer;
            let mut submission = 0;
            // SAFETY: the producer is live, the stream addresses its length, and `submission`
            // is writable.
            let failure = unsafe {
                nx_producer_submit_ipc(
                    producer.0,
                    bytes.as_ptr(),
                    bytes.len(),
                    ptr::null(),
                    &mut submission,
                )
            };
            succeeded(failure);
            submission
        }
    });
    let (request_id, request) = next(&server, &mut exchange);
    let ClientRequest::SubmitBatch(submit) = request else {
        panic!("a stream is a SubmitBatch request");
    };
    assert_eq!(submit.batch.as_ref(), written.as_slice());
    written.fill(0);
    let second = raw.join().assured("the submitting thread returns");
    exchange.answer(
        &server,
        request_id,
        ReplyBody::Submission(WireSubmissionOutcome {
            outcome: ClientSubmissionOutcome::NotAdmitted(ClientSubmissionRefusal::InvalidBatch(
                ClientBatchDefect::SchemaMismatch,
            )),
            message: "another schema".to_string(),
        }),
    );
    let refused = producer.rejoin(second);
    let mut defect = BatchDefect::Malformed;
    let mut failure = ProcessingFailure::AckTimedOut;
    // SAFETY: the outcome is live and the out-parameters are writable.
    unsafe {
        assert_eq!(refused.result(), SubmissionResult::NotAdmitted);
        succeeded(nx_submission_outcome_refusal(refused.0, &mut refusal));
        assert_eq!(refusal, SubmissionRefusal::InvalidBatch);
        succeeded(nx_submission_outcome_defect(refused.0, &mut defect));
        assert_eq!(defect, BatchDefect::SchemaMismatch);
        assert_eq!(refused.message(), "another schema");
        assert_eq!(
            failure_kind(nx_submission_outcome_failure(refused.0, &mut failure)),
            FailureKind::Type
        );
    }

    // A processing failure and an unknown outcome carry their typed causes.
    for (outcome, expected) in [
        (
            ClientSubmissionOutcome::ProcessingFailed(ClientProcessingFailure::Rejected),
            SubmissionResult::ProcessingFailed,
        ),
        (
            ClientSubmissionOutcome::OutcomeUnknown(ClientOutcomeUncertainty::OwnerLost),
            SubmissionResult::OutcomeUnknown,
        ),
    ] {
        let submitting = thread::spawn({
            let batch = SharedBatch(unsafe { crate::nx_batch_retain(batch.0) });
            move || producer.submit(&batch)
        });
        let (request_id, _) = next(&server, &mut exchange);
        let submission = submitting
            .join()
            .assured("the submitting thread returns")
            .assured("a batch within the credit is submitted");
        exchange.answer(
            &server,
            request_id,
            ReplyBody::Submission(WireSubmissionOutcome {
                outcome,
                message: String::new(),
            }),
        );
        let taken = producer.rejoin(submission);
        assert_eq!(taken.result(), expected);
        let mut uncertainty = Uncertainty::Interrupted;
        // SAFETY: the outcome is live and the out-parameters are writable.
        unsafe {
            match expected {
                SubmissionResult::ProcessingFailed => {
                    succeeded(nx_submission_outcome_failure(taken.0, &mut failure));
                    assert_eq!(failure, ProcessingFailure::Rejected);
                }
                _ => {
                    succeeded(nx_submission_outcome_uncertainty(taken.0, &mut uncertainty));
                    assert_eq!(uncertainty, Uncertainty::OwnerLost);
                    assert_eq!(
                        failure_kind(nx_submission_outcome_defect(taken.0, &mut defect)),
                        FailureKind::Type
                    );
                }
            }
        }
    }

    // A released submission gives its outcome up; the producer holds nothing about it after.
    let submitting = thread::spawn({
        let batch = SharedBatch(unsafe { crate::nx_batch_retain(batch.0) });
        move || producer.submit(&batch)
    });
    let (request_id, _) = next(&server, &mut exchange);
    let released = submitting
        .join()
        .assured("the submitting thread returns")
        .assured("a batch within the credit is submitted");
    let mut outcome = ptr::null_mut();
    // SAFETY: the producer is live and `outcome` is writable.
    succeeded(unsafe { nx_producer_release(producer.0, released, &mut outcome) });
    assert!(
        outcome.is_null(),
        "an unresolved submission has no outcome to give"
    );
    exchange.answer(
        &server,
        request_id,
        ReplyBody::Submission(WireSubmissionOutcome {
            outcome: ClientSubmissionOutcome::Completed,
            message: String::new(),
        }),
    );
    assert!(producer.pending().is_empty());
    assert_eq!(
        producer.rejoin_within(released, 50).err(),
        Some(FailureKind::InvalidArgument)
    );
    assert_eq!(
        producer.rejoin_within(0, 50).err(),
        Some(FailureKind::InvalidArgument)
    );

    // Closing waits for the server's release, and a closed producer submits nothing.
    let closing = thread::spawn(move || {
        let producer = producer;
        // SAFETY: the producer is live and no token bounds the close.
        failure_kind_or_success(unsafe { nx_producer_close(producer.0, ptr::null()) })
    });
    let (request_id, request) = next(&server, &mut exchange);
    assert!(matches!(request, ClientRequest::CloseIngestor(_)));
    exchange.answer(
        &server,
        request_id,
        ReplyBody::CloseIngestor(CloseIngestorOutcome {
            disposition: CloseIngestorDisposition::Closed,
            message: String::new(),
        }),
    );
    assert_eq!(closing.join().assured("the closing thread returns"), None);
    assert_eq!(producer.state(), EndpointState::Closed);
    assert_eq!(producer.submit(&batch).err(), Some(FailureKind::Closed));
    // SAFETY: a second close finds nothing attached and returns at once.
    assert_eq!(
        failure_kind_or_success(unsafe { nx_producer_close(producer.0, ptr::null()) }),
        None
    );
    producer.free();
    session.free();
    // SAFETY: freeing null does nothing.
    unsafe { nx_producer_free(ptr::null_mut()) };
}

/// The kind of a returned failure, or `None` on success.
fn failure_kind_or_success(failure: *mut crate::Failure) -> Option<FailureKind> {
    if failure.is_null() {
        return None;
    }
    Some(failure_kind(failure))
}

#[test]
fn a_refused_open_names_its_refusal_and_a_host_argument_is_checked_first() {
    let mut server = TestServer::start();
    let session = SharedSession::connect(server.address);
    let mut exchange = server.next_exchange();

    let opening = thread::spawn(move || {
        let session = session;
        let mut producer = ptr::null_mut();
        let fields = HostFields::endpoint();
        // SAFETY: the session and the fields are live, the names address their lengths, and
        // `producer` is writable.
        unsafe {
            nx_session_open_ingestor(
                session.0,
                "sim".as_ptr(),
                3,
                "orders_in".as_ptr(),
                9,
                fields.0,
                2,
                4096,
                ptr::null(),
                &mut producer,
            )
        }
        .addr()
    });
    let (request_id, _) = next(&server, &mut exchange);
    exchange.answer(
        &server,
        request_id,
        ReplyBody::OpenIngestor(OpenIngestorOutcome {
            disposition: OpenIngestorDisposition::Refused(ClientProducerRefusal::SchemaMismatch),
            message: "the fields differ".to_string(),
        }),
    );
    let failure = std::ptr::with_exposed_provenance_mut::<crate::Failure>(
        opening.join().assured("the opening thread returns"),
    );
    let mut refusal = OpenRefusal::DomainNotFound;
    // SAFETY: the failure is live and freed once, here.
    unsafe {
        assert_eq!(nx_error_kind_of(failure), FailureKind::Rejected);
        assert!(nx_error_open_refusal(failure, &mut refusal));
        assert_eq!(refusal, OpenRefusal::SchemaMismatch);
        nx_error_free(failure);
    }

    let subscribing = thread::spawn(move || session.subscribe_emitter(&HostFields::endpoint()));
    let (request_id, _) = next(&server, &mut exchange);
    exchange.answer(
        &server,
        request_id,
        ReplyBody::OpenEmitter(OpenEmitterOutcome {
            disposition: OpenEmitterDisposition::Refused(EmitterOpenRefusal::TooManyConsumers),
            message: String::new(),
        }),
    );
    assert_eq!(
        subscribing
            .join()
            .assured("the subscribing thread returns")
            .err(),
        Some(FailureKind::Rejected)
    );

    // Arguments the host passed wrong are refused before anything is sent.
    let incomplete = HostFields(nx_fields_new());
    // SAFETY: the field lists are live and every name addresses its length.
    unsafe {
        succeeded(nx_fields_add(
            incomplete.0,
            "tags".as_ptr(),
            4,
            LIST,
            0,
            false,
            false,
        ));
        assert_eq!(
            failure_kind(nx_fields_add(
                incomplete.0,
                "id".as_ptr(),
                2,
                U32,
                0,
                false,
                false
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_fields_element(incomplete.0, 99, 0)),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_fields_element(incomplete.0, 15, 0)),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_fields_element(incomplete.0, STRING, 3)),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_fields_add(
                incomplete.0,
                "Bad Name".as_ptr(),
                8,
                U32,
                0,
                false,
                false
            )),
            FailureKind::InvalidArgument
        );
    }
    assert_eq!(
        session.open_ingestor(&incomplete).err(),
        Some(FailureKind::InvalidArgument)
    );
    let ended = HostFields(nx_fields_new());
    // SAFETY: the field list is live and the name addresses its length.
    unsafe {
        succeeded(nx_fields_add(
            ended.0,
            "id".as_ptr(),
            2,
            U32,
            0,
            false,
            false,
        ));
        assert_eq!(
            failure_kind(nx_fields_element(ended.0, STRING, 0)),
            FailureKind::InvalidArgument
        );
        let mut producer = ptr::null_mut();
        assert_eq!(
            failure_kind(nx_session_open_ingestor(
                session.0,
                "sim".as_ptr(),
                3,
                "orders_in".as_ptr(),
                9,
                ended.0,
                0,
                4096,
                ptr::null(),
                &mut producer,
            )),
            FailureKind::InvalidArgument
        );
    }
    session.free();
}

#[test]
fn a_consumer_reads_and_settles_a_delivery_and_a_lost_confirmation_is_uncertain() {
    let mut server = TestServer::start();
    let session = SharedSession::connect(server.address);
    let mut exchange = server.next_exchange();
    let consumer = open_consumer(&server, &mut exchange, session);

    let mut schema = ptr::null_mut();
    let mut digest = ptr::null();
    let mut digest_len = 0;
    let mut grant = (0_u32, 0_u64, 0_u32, 0_u64);
    let mut window = WindowKind::Parallel;
    let mut policy = (0_u64, 0_u64, 0_u64, 0_u64);
    // SAFETY: the consumer is live and every out-parameter is writable.
    unsafe {
        succeeded(nx_consumer_schema(consumer.0, &mut schema));
        assert_eq!((*schema).fields(Part::Rows), endpoint_fields().as_slice());
        assert_eq!((*schema).branch(), None);
        nx_schema_free(schema);
        assert_eq!(nx_consumer_generation(consumer.0), 3);
        nx_consumer_contract(consumer.0, &mut digest, &mut digest_len);
        assert_eq!(std::slice::from_raw_parts(digest, digest_len), &[6; 32]);
        nx_consumer_grant(
            consumer.0,
            &mut grant.0,
            &mut grant.1,
            &mut grant.2,
            &mut grant.3,
        );
        assert_eq!(grant, (2, 8192, 32, 8192));
        nx_consumer_policy(
            consumer.0,
            &mut window,
            &mut policy.0,
            &mut policy.1,
            &mut policy.2,
            &mut policy.3,
        );
        assert_eq!(window, WindowKind::Sequential);
        assert_eq!(
            policy,
            (
                1,
                nanos(Duration::from_secs(20)),
                nanos(Duration::from_millis(50)),
                nanos(Duration::from_secs(2))
            )
        );
        assert!(!nx_consumer_reopen_reason(
            consumer.0,
            ptr::null_mut(),
            ptr::null_mut()
        ));
    }
    assert_eq!(consumer.state(), EndpointState::Active);

    // A wait that expires leaves its read with the consumer: the next read receives the reply
    // to it without asking the server again.
    assert_eq!(consumer.next_within(50).err(), Some(FailureKind::Deadline));
    let (abandoned, request) = next(&server, &mut exchange);
    assert!(matches!(request, ClientRequest::ReadEmitterBatch(_)));
    let columns = endpoint_columns(&[7, 8, 9]);
    let written = stream(&build(&endpoint_schema(), 3, &columns));
    exchange.answer(&server, abandoned, batch_received(1, 2, written.clone(), 3));
    let delivery = consumer
        .next_within(WAIT_MILLIS)
        .assured("the parked read's delivery arrives");
    assert!(
        server
            .runtime
            .block_on(exchange.quiet(Duration::from_millis(100))),
        "the read that took over sent no read of its own"
    );

    // The delivery's identity, reference, metadata and stream, and its batch column by column.
    assert_eq!(
        delivery.bytes(nx_delivery_identity),
        Uuid::from_u128(1).as_bytes()
    );
    assert_eq!(
        delivery.bytes(nx_delivery_reference),
        Uuid::from_u128(2).as_bytes()
    );
    assert_eq!(delivery.bytes(nx_delivery_source_relay), b"orders");
    assert_eq!(delivery.bytes(nx_delivery_ipc), written);
    let mut fingerprint = ptr::null();
    let mut fingerprint_len = 0;
    let mut batch = ptr::null_mut();
    // SAFETY: the delivery is live and every out-parameter is writable.
    unsafe {
        assert!(nx_delivery_branch_fingerprint(
            delivery.0,
            &mut fingerprint,
            &mut fingerprint_len
        ));
        assert_eq!(
            std::slice::from_raw_parts(fingerprint, fingerprint_len),
            &[9; 32]
        );
        assert_eq!(nx_delivery_members(delivery.0), 3);
        assert_eq!(
            nx_delivery_execution_now(delivery.0),
            1_700_000_000_000_000_007
        );
        succeeded(nx_delivery_batch(delivery.0, &mut batch));
    }
    let batch = SharedBatch(batch);
    for (index, column) in columns.iter().enumerate() {
        assert_eq!(&read(&batch, index, column), column);
    }
    assert_eq!(stream(&batch), written);

    // A reference retained here settles the attempt after the first is released on another
    // thread. Releasing a delivery settles nothing.
    // SAFETY: the reference is live, and the binding returns a new one.
    let retained = SharedDelivery(unsafe { nx_delivery_retain(delivery.0) });
    thread::spawn(move || drop(delivery))
        .join()
        .assured("releasing on another thread does not panic");
    assert!(
        server
            .runtime
            .block_on(exchange.quiet(Duration::from_millis(100))),
        "releasing a delivery sends nothing"
    );
    let acking = thread::spawn(move || {
        retained
            .settle(nx_delivery_ack)
            .map(|settled| (settled, retained))
    });
    let (request_id, request) = next(&server, &mut exchange);
    let ClientRequest::SettleEmitterBatch(settle) = request else {
        panic!("an acknowledgement is a SettleEmitterBatch request");
    };
    assert_eq!(settle.reference, Uuid::from_u128(2));
    assert_eq!(settle.decision, EmitterBatchDecision::Ack);
    exchange.answer(&server, request_id, settled(EmitterSettlement::Confirmed));
    let (confirmed, retained) = acking
        .join()
        .assured("the settling thread returns")
        .assured("an answered acknowledgement is settled");
    assert_eq!(confirmed, Settlement::Confirmed);

    // A retry and a rejection are answered with what the server did; a rejection's reason is
    // checked before anything is sent.
    let retrying = thread::spawn(move || {
        retained
            .settle(nx_delivery_retry)
            .map(|settled| (settled, retained))
    });
    let (request_id, request) = next(&server, &mut exchange);
    let ClientRequest::SettleEmitterBatch(settle) = request else {
        panic!("a retry is a SettleEmitterBatch request");
    };
    assert_eq!(settle.decision, EmitterBatchDecision::Retry);
    exchange.answer(
        &server,
        request_id,
        settled(EmitterSettlement::StaleReference),
    );
    let (stale, retained) = retrying
        .join()
        .assured("the settling thread returns")
        .assured("an answered retry is settled");
    assert_eq!(stale, Settlement::StaleReference);
    let mut settlement = Settlement::Confirmed;
    // SAFETY: the delivery is live and the reason addresses its length.
    unsafe {
        assert_eq!(
            failure_kind(nx_delivery_reject(
                retained.0,
                "".as_ptr(),
                0,
                ptr::null(),
                &mut settlement
            )),
            FailureKind::InvalidArgument
        );
    }
    let rejecting = thread::spawn(move || {
        let retained = retained;
        let deadline = Deadline::after(WAIT_MILLIS);
        let mut settlement = Settlement::ConsumerEnded;
        let reason = "refused by the application";
        // SAFETY: the delivery and the token are live, the reason addresses its length, and
        // `settlement` is writable.
        let failure = unsafe {
            nx_delivery_reject(
                retained.0,
                reason.as_ptr(),
                reason.len(),
                deadline.0,
                &mut settlement,
            )
        };
        succeeded(failure);
        (settlement, retained)
    });
    let (request_id, request) = next(&server, &mut exchange);
    let ClientRequest::SettleEmitterBatch(settle) = request else {
        panic!("a rejection is a SettleEmitterBatch request");
    };
    assert_eq!(
        settle.decision,
        EmitterBatchDecision::Reject("refused by the application".to_string())
    );
    exchange.answer(&server, request_id, settled(EmitterSettlement::Confirmed));
    let (rejected, retained) = rejecting.join().assured("the settling thread returns");
    assert_eq!(rejected, Settlement::Confirmed);

    // An acknowledgement whose answer the session loses is uncertain: the server may have
    // settled the attempt.
    let acking = thread::spawn(move || {
        retained
            .settle(nx_delivery_ack)
            .map(|settled| (settled, retained))
    });
    let (_, request) = next(&server, &mut exchange);
    assert!(matches!(request, ClientRequest::SettleEmitterBatch(_)));
    drop(exchange);
    assert_eq!(
        acking.join().assured("the settling thread returns").err(),
        Some(FailureKind::Uncertain)
    );
    consumer.free();
    session.free();
}

#[test]
fn a_reconnect_interrupts_a_consumer_expires_its_deliveries_and_restores_only_open_handles() {
    let mut server = TestServer::start();
    let session = SharedSession::connect(server.address);
    let mut exchange = server.next_exchange();
    let consumer = open_consumer(&server, &mut exchange, session);
    let kept = open_producer(&server, &mut exchange, session);
    let closed = open_producer(&server, &mut exchange, session);

    let reading = thread::spawn(move || consumer.next_within(WAIT_MILLIS));
    let (request_id, _) = next(&server, &mut exchange);
    let columns = endpoint_columns(&[1]);
    let written = stream(&build(&endpoint_schema(), 1, &columns));
    exchange.answer(&server, request_id, batch_received(3, 4, written, 1));
    let delivery = reading
        .join()
        .assured("the reading thread returns")
        .assured("a delivery arrives");
    let batch = build(&endpoint_schema(), 1, &columns);
    let submitting = thread::spawn({
        let batch = SharedBatch(unsafe { crate::nx_batch_retain(batch.0) });
        move || kept.submit(&batch)
    });
    let (_, request) = next(&server, &mut exchange);
    assert!(matches!(request, ClientRequest::SubmitBatch(_)));
    let unresolved = submitting
        .join()
        .assured("the submitting thread returns")
        .assured("a batch within the credit is submitted");

    // The session ends: the consumer reports the gap once, a delivery it read before cannot be
    // settled any more, and the batch sent before is of unknown outcome.
    drop(exchange);
    assert_eq!(
        consumer.next_within(WAIT_MILLIS).err(),
        Some(FailureKind::Interrupted)
    );
    assert_eq!(
        delivery.settle(nx_delivery_ack).err(),
        Some(FailureKind::Rejected)
    );
    let unknown = kept.rejoin(unresolved);
    assert_eq!(unknown.result(), SubmissionResult::OutcomeUnknown);
    let mut uncertainty = Uncertainty::OwnerLost;
    // SAFETY: the outcome is live and `uncertainty` is writable.
    succeeded(unsafe { nx_submission_outcome_uncertainty(unknown.0, &mut uncertainty) });
    assert_eq!(uncertainty, Uncertainty::SessionLost);

    // A handle closed while it waits to be restored returns at once and is never restored.
    wait_until(|| closed.state() != EndpointState::Active);
    // SAFETY: the producer is live and no token bounds the close.
    assert_eq!(
        failure_kind_or_success(unsafe { nx_producer_close(closed.0, ptr::null()) }),
        None
    );
    assert_eq!(closed.state(), EndpointState::Closed);

    // The next submission reopens the session, which restores the consumer and the producer
    // still open, and nothing else.
    let submitting = thread::spawn({
        let batch = SharedBatch(unsafe { crate::nx_batch_retain(batch.0) });
        move || kept.submit(&batch)
    });
    let mut restored = server.next_exchange();
    let mut opens = Vec::new();
    for _ in 0..2 {
        let (request_id, request) = next(&server, &mut restored);
        match request {
            ClientRequest::OpenIngestor(_) => {
                restored.answer(&server, request_id, opened_producer());
                opens.push("producer");
            }
            ClientRequest::OpenEmitter(_) => {
                restored.answer(
                    &server,
                    request_id,
                    ReplyBody::OpenEmitter(OpenEmitterOutcome {
                        disposition: OpenEmitterDisposition::Opened(Box::new(
                            consumer_description(),
                        )),
                        message: String::new(),
                    }),
                );
                opens.push("consumer");
            }
            other => panic!("a restoration sent {other:?} before its opens"),
        }
    }
    opens.sort_unstable();
    assert_eq!(opens, ["consumer", "producer"]);
    let (request_id, request) = next(&server, &mut restored);
    assert!(matches!(request, ClientRequest::SubmitBatch(_)));
    let after = submitting
        .join()
        .assured("the submitting thread returns")
        .assured("the restored producer submits");
    restored.answer(
        &server,
        request_id,
        ReplyBody::Submission(WireSubmissionOutcome {
            outcome: ClientSubmissionOutcome::Completed,
            message: String::new(),
        }),
    );
    assert_eq!(kept.rejoin(after).result(), SubmissionResult::Completed);
    assert_eq!(kept.state(), EndpointState::Active);

    // A consumer closed with its attachment restored releases it, and so does a freed producer.
    let closing = thread::spawn(move || {
        let consumer = consumer;
        // SAFETY: the consumer is live and no token bounds the close.
        failure_kind_or_success(unsafe { nx_consumer_close(consumer.0, ptr::null()) })
    });
    let (request_id, request) = next(&server, &mut restored);
    assert!(matches!(request, ClientRequest::CloseEmitter(_)));
    restored.answer(
        &server,
        request_id,
        ReplyBody::CloseEmitter(CloseEmitterOutcome {
            disposition: EmitterCloseDisposition::Closed,
            message: String::new(),
        }),
    );
    assert_eq!(closing.join().assured("the closing thread returns"), None);
    assert_eq!(consumer.state(), EndpointState::Closed);
    assert_eq!(consumer.next_within(50).err(), Some(FailureKind::Closed));
    consumer.free();
    closed.free();
    kept.free();
    let (_, request) = next(&server, &mut restored);
    assert!(
        matches!(request, ClientRequest::CloseIngestor(_)),
        "freeing an open producer releases its attachment"
    );
    session.free();
}

#[test]
fn a_session_freed_before_its_producer_keeps_serving_the_producer() {
    let mut server = TestServer::start();
    let session = SharedSession::connect(server.address);
    let mut exchange = server.next_exchange();
    let producer = open_producer(&server, &mut exchange, session);
    session.free();
    let batch = build(&endpoint_schema(), 1, &endpoint_columns(&[5]));
    let submitting = thread::spawn(move || producer.submit(&batch));
    let (request_id, request) = next(&server, &mut exchange);
    assert!(matches!(request, ClientRequest::SubmitBatch(_)));
    let submission = submitting
        .join()
        .assured("the submitting thread returns")
        .assured("the producer still submits");
    exchange.answer(
        &server,
        request_id,
        ReplyBody::Submission(WireSubmissionOutcome {
            outcome: ClientSubmissionOutcome::Completed,
            message: String::new(),
        }),
    );
    assert_eq!(
        producer.rejoin(submission).result(),
        SubmissionResult::Completed
    );
    producer.free();
}

/// Waits until `condition` holds, within the test's wait.
fn wait_until(condition: impl Fn() -> bool) {
    let deadline = nervix_primitives::time::Instant::now() + Duration::from_millis(WAIT_MILLIS);
    while !condition() {
        assert!(
            nervix_primitives::time::Instant::now() < deadline,
            "the condition held within the test's wait"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

/// Every state, refusal and reopen reason of a producer and of a consumer reads as the binding
/// value the header names for it, and a producer's and a consumer's variant of one reason read
/// alike.
#[test]
fn every_endpoint_state_refusal_and_reopen_reason_reads_as_its_binding_value() {
    struct States {
        producer: ProducerConnection,
        consumer: ConsumerConnection,
        state: EndpointState,
    }
    let states = [
        States {
            producer: ProducerConnection::Active,
            consumer: ConsumerConnection::Active,
            state: EndpointState::Active,
        },
        States {
            producer: ProducerConnection::Interrupted,
            consumer: ConsumerConnection::Interrupted,
            state: EndpointState::Interrupted,
        },
        States {
            producer: ProducerConnection::Restoring,
            consumer: ConsumerConnection::Restoring,
            state: EndpointState::Restoring,
        },
        States {
            producer: ProducerConnection::ReopenRequired,
            consumer: ConsumerConnection::ReopenRequired,
            state: EndpointState::ReopenRequired,
        },
        States {
            producer: ProducerConnection::Closed,
            consumer: ConsumerConnection::Closed,
            state: EndpointState::Closed,
        },
    ];
    for expected in states {
        assert_eq!(EndpointState::from(expected.producer), expected.state);
        assert_eq!(EndpointState::from(expected.consumer), expected.state);
    }

    struct Refusals {
        producer: ClientProducerRefusal,
        consumer: EmitterOpenRefusal,
        refusal: OpenRefusal,
    }
    let refusals = [
        Refusals {
            producer: ClientProducerRefusal::DomainNotFound,
            consumer: EmitterOpenRefusal::DomainNotFound,
            refusal: OpenRefusal::DomainNotFound,
        },
        Refusals {
            producer: ClientProducerRefusal::DomainStopped,
            consumer: EmitterOpenRefusal::DomainStopped,
            refusal: OpenRefusal::DomainStopped,
        },
        Refusals {
            producer: ClientProducerRefusal::IngestorNotFound,
            consumer: EmitterOpenRefusal::EmitterNotFound,
            refusal: OpenRefusal::EndpointNotFound,
        },
        Refusals {
            producer: ClientProducerRefusal::NotClientIngestor,
            consumer: EmitterOpenRefusal::NotClientEmitter,
            refusal: OpenRefusal::NotClientEndpoint,
        },
        Refusals {
            producer: ClientProducerRefusal::EndpointUnavailable,
            consumer: EmitterOpenRefusal::EndpointUnavailable,
            refusal: OpenRefusal::EndpointUnavailable,
        },
        Refusals {
            producer: ClientProducerRefusal::SchemaMismatch,
            consumer: EmitterOpenRefusal::SchemaMismatch,
            refusal: OpenRefusal::SchemaMismatch,
        },
        Refusals {
            producer: ClientProducerRefusal::TooManyProducers,
            consumer: EmitterOpenRefusal::TooManyConsumers,
            refusal: OpenRefusal::TooManyEndpoints,
        },
        Refusals {
            producer: ClientProducerRefusal::SessionCapacityExhausted,
            consumer: EmitterOpenRefusal::SessionCapacityExhausted,
            refusal: OpenRefusal::SessionCapacityExhausted,
        },
        Refusals {
            producer: ClientProducerRefusal::NodeCapacityExhausted,
            consumer: EmitterOpenRefusal::NodeCapacityExhausted,
            refusal: OpenRefusal::NodeCapacityExhausted,
        },
        Refusals {
            producer: ClientProducerRefusal::InvalidLimits,
            consumer: EmitterOpenRefusal::InvalidLimits,
            refusal: OpenRefusal::InvalidLimits,
        },
        Refusals {
            producer: ClientProducerRefusal::InTransaction,
            consumer: EmitterOpenRefusal::InTransaction,
            refusal: OpenRefusal::InTransaction,
        },
    ];
    for expected in refusals {
        assert_eq!(OpenRefusal::from(expected.producer), expected.refusal);
        assert_eq!(OpenRefusal::from(expected.consumer), expected.refusal);
        // A restoration the server refused for good needs a new open, and names the refusal.
        let producer = Reopen::from(&ProducerReopenReason::Refused(expected.producer));
        assert_eq!(producer.reason(), ReopenReason::Refused);
        assert_eq!(producer.refusal(), Some(expected.refusal));
        let consumer = Reopen::from(&ConsumerReopenReason::Refused(expected.consumer));
        assert_eq!(consumer.reason(), ReopenReason::Refused);
        assert_eq!(consumer.refusal(), Some(expected.refusal));
    }

    struct Reasons {
        producer: ProducerReopenReason,
        consumer: ConsumerReopenReason,
        reason: ReopenReason,
    }
    let reasons = [
        Reasons {
            producer: ProducerReopenReason::DomainStopped,
            consumer: ConsumerReopenReason::DomainStopped,
            reason: ReopenReason::DomainStopped,
        },
        Reasons {
            producer: ProducerReopenReason::EndpointRemoved,
            consumer: ConsumerReopenReason::EndpointRemoved,
            reason: ReopenReason::EndpointRemoved,
        },
        Reasons {
            producer: ProducerReopenReason::SchemaChanged,
            consumer: ConsumerReopenReason::SchemaChanged,
            reason: ReopenReason::SchemaChanged,
        },
        Reasons {
            producer: ProducerReopenReason::ContractChanged,
            consumer: ConsumerReopenReason::ContractChanged,
            reason: ReopenReason::ContractChanged,
        },
        Reasons {
            producer: ProducerReopenReason::GenerationChanged,
            consumer: ConsumerReopenReason::GenerationChanged,
            reason: ReopenReason::GenerationChanged,
        },
        Reasons {
            producer: ProducerReopenReason::ProtocolViolated,
            consumer: ConsumerReopenReason::ProtocolViolated,
            reason: ReopenReason::ProtocolViolated,
        },
    ];
    for expected in reasons {
        let producer = Reopen::from(&expected.producer);
        assert_eq!(producer, Reopen::Changed(expected.reason));
        assert_eq!(producer.reason(), expected.reason);
        assert_eq!(producer.refusal(), None);
        assert_eq!(
            Reopen::from(&expected.consumer),
            Reopen::Changed(expected.reason)
        );
    }
}
