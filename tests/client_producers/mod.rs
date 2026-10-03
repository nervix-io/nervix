//! Steps that open producers on client ingestors, submit typed batches through them, and read
//! their outcomes.
//!
//! Layer: test harness.
//! - **Owns.** The producers a scenario opened by name, the batches they submitted with the
//!   outcome and the instant each was answered with, the raw WebSocket sessions producers are
//!   opened on, and the Arrow batches built from step tables.
//! - **Depends on.** The Rust client's producers, the harness's raw producer session, the NSPL
//!   parser for the fields a producer expects, and the scenario cluster.
//! - **Must not know.** How the server admits, forwards or acknowledges a batch.
//!
//! A producer opened by a `client` speaks through the Rust client over the native gRPC exchange,
//! which retries temporary refusals itself. A producer opened by a `WebSocket session` speaks raw
//! frames over the console WebSocket, as a browser client does, and sees every refusal.

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_ipc::writer::StreamWriter;
use bytes::Bytes;
use cucumber::{given, then, when};
use nervix_client_core::{
    ClientError, Producer, ProducerBatch, ProducerConnection, ProducerEnd, ProducerOutcome,
    ProducerReopenReason, SubmissionId, SubmissionUncertainty,
};
use nervix_client_wire::{
    ClientRequest, CloseIngestorDisposition, CloseIngestorRequest, OpenIngestorDisposition,
    OpenIngestorRequest, ProducerId, ReplyBody, RequestId, SubmitBatchRequest,
};
use nervix_models::{
    ClientProducerAdmission, ClientProducerDescription, ClientProducerLimits,
    ClientProducerRefusal, ClientSubmissionOutcome, ClientSubmissionRefusal, DomainName,
    IngestorName, ParseAsType, SchemaField, parse_duration_text,
};
use nervix_primitives::sync::{StdArc, watch};

use super::*;
use crate::common::producer_session::{ProducerFrame, RawProducerSession};

/// How long a step waits for an open, an outcome or a producer event it expects.
const PRODUCER_EXPECTATION_TIMEOUT: Duration = Duration::from_secs(60);
/// How often a step re-reads a state the Rust client keeps for a producer.
const PRODUCER_POLL_INTERVAL: Duration = Duration::from_millis(50);
/// The credit a producer asks for unless a step names its own.
const DEFAULT_PRODUCER_BATCHES: u32 = 16;
const DEFAULT_PRODUCER_BYTES: u64 = 8 * 1024 * 1024;

/// Everything the producer steps of one scenario hold.
#[derive(Default)]
pub(crate) struct ScenarioProducers {
    /// Raw WebSocket sessions by the name a scenario gave them.
    pub(crate) sessions: BTreeMap<String, RawProducerSession>,
    producers: BTreeMap<String, ScenarioProducer>,
    submissions: BTreeMap<String, ScenarioSubmission>,
}

/// The session a step names.
#[derive(Clone)]
enum SessionRef {
    /// A Rust client connected by an earlier step.
    Client(String),
    /// A raw console WebSocket session.
    WebSocket(String),
}

/// A producer a scenario opened.
enum ScenarioProducer {
    Native(StdArc<Producer>),
    Raw {
        session: String,
        id: ProducerId,
        description: Box<ClientProducerDescription>,
        /// How many of the producer's frames earlier steps already accounted for.
        frames_seen: usize,
    },
}

impl ScenarioProducer {
    fn description(&self) -> &ClientProducerDescription {
        match self {
            Self::Native(producer) => producer.description(),
            Self::Raw { description, .. } => description,
        }
    }
}

/// A batch a producer submitted.
struct ScenarioSubmission {
    producer: String,
    state: SubmissionState,
}

enum SubmissionState {
    Native(NativeSubmission),
    Raw { session: String, request: RequestId },
}

/// A batch the Rust client submits in a task of its own, so a step can stop waiting for it
/// without withdrawing it.
struct NativeSubmission {
    /// The identity the producer gave the batch once it held it.
    id: watch::Receiver<Option<SubmissionId>>,
    wait: Option<AbortOnDropHandle<Result<Answered, String>>>,
    answered: Option<Answered>,
}

/// A batch's terminal outcome and when the client observed it.
#[derive(Debug, Clone)]
struct Answered {
    outcome: ObservedOutcome,
    at: Instant,
}

/// A terminal outcome in the words scenarios use for it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedOutcome {
    class: &'static str,
    cause: String,
    message: String,
}

impl ObservedOutcome {
    fn from_native(outcome: ProducerOutcome) -> Self {
        match outcome {
            ProducerOutcome::Completed => Self {
                class: "completed",
                cause: String::new(),
                message: String::new(),
            },
            ProducerOutcome::NotAdmitted { refusal, message } => Self {
                class: "not admitted",
                cause: refusal_label(refusal),
                message,
            },
            ProducerOutcome::ProcessingFailed { failure, message } => Self {
                class: "processing failed",
                cause: failure.as_ref().to_string(),
                message,
            },
            ProducerOutcome::OutcomeUnknown { cause, message } => {
                let cause = match cause {
                    SubmissionUncertainty::Interrupted => "interrupted",
                    SubmissionUncertainty::OwnerLost => "owner_lost",
                    SubmissionUncertainty::SessionLost => "session_lost",
                };
                Self {
                    class: "unknown outcome",
                    cause: cause.to_string(),
                    message,
                }
            }
        }
    }

    fn from_wire(outcome: ClientSubmissionOutcome, message: String) -> Self {
        match outcome {
            ClientSubmissionOutcome::Completed => Self {
                class: "completed",
                cause: String::new(),
                message,
            },
            ClientSubmissionOutcome::NotAdmitted(refusal) => Self {
                class: "not admitted",
                cause: refusal_label(refusal),
                message,
            },
            ClientSubmissionOutcome::ProcessingFailed(failure) => Self {
                class: "processing failed",
                cause: failure.as_ref().to_string(),
                message,
            },
            ClientSubmissionOutcome::OutcomeUnknown(cause) => Self {
                class: "unknown outcome",
                cause: cause.as_ref().to_string(),
                message,
            },
        }
    }
}

/// A refusal as scenarios name it; an invalid batch names its defect.
fn refusal_label(refusal: ClientSubmissionRefusal) -> String {
    match refusal {
        ClientSubmissionRefusal::InvalidBatch(defect) => {
            format!("invalid batch: {}", defect.as_ref())
        }
        ClientSubmissionRefusal::Suspended => "suspended".to_string(),
        ClientSubmissionRefusal::Busy => "busy".to_string(),
        ClientSubmissionRefusal::Draining => "draining".to_string(),
        ClientSubmissionRefusal::ProducerEnded => "producer ended".to_string(),
        ClientSubmissionRefusal::CreditExceeded => "credit exceeded".to_string(),
    }
}

/// Why an open was refused.
#[derive(Debug)]
struct OpenRefused {
    refusal: ClientProducerRefusal,
    message: String,
}

pub(crate) fn scenario_domain(world: &ScenarioWorld) -> DomainName {
    DomainName::parse(&world.domain).expect("scenario domains are valid domain names")
}

/// The fields a producer expects, written as the column list of `CREATE SCHEMA`.
pub(crate) fn expected_fields(text: &str) -> Vec<SchemaField> {
    let source = format!("CREATE SCHEMA expected_by_producer ({text});");
    let parsed = nervix_nspl::client_statement::parse_client_statement_sources(&source)
        .unwrap_or_else(|error| panic!("'{text}' is not a schema column list: {error:?}"));
    for statement in parsed {
        let nervix_nspl::client_statement::ClientStatement::Server(
            nervix_models::Statement::Create(create),
        ) = statement.statement
        else {
            continue;
        };
        if let nervix_models::Model::Schema(schema) = *create.body {
            return schema.fields;
        }
    }
    panic!("'{text}' did not parse into a schema");
}

/// The credit a producer asks for unless a step names its own.
fn default_limits() -> ClientProducerLimits {
    ClientProducerLimits {
        batches: std::num::NonZeroU32::new(DEFAULT_PRODUCER_BATCHES)
            .expect("the default batch credit is non-zero"),
        bytes: std::num::NonZeroU64::new(DEFAULT_PRODUCER_BYTES)
            .expect("the default byte credit is non-zero"),
    }
}

fn producer_limits(batches: u32, bytes: &str) -> ClientProducerLimits {
    let bytes = parse_byte_size(bytes);
    ClientProducerLimits {
        batches: std::num::NonZeroU32::new(batches).expect("a producer asks for at least a batch"),
        bytes: std::num::NonZeroU64::new(bytes).expect("a producer asks for at least a byte"),
    }
}

/// A size such as `4KiB`, `2MiB` or `512`.
fn parse_byte_size(text: &str) -> u64 {
    let text = text.trim();
    let units: [(&str, u64); 3] = [
        ("GiB", 1024 * 1024 * 1024),
        ("MiB", 1024 * 1024),
        ("KiB", 1024),
    ];
    for (suffix, multiplier) in units {
        if let Some(number) = text.strip_suffix(suffix) {
            let number: u64 = number
                .trim()
                .parse()
                .expect("a byte size is a whole number");
            return number
                .checked_mul(multiplier)
                .expect("a scenario byte size fits in u64");
        }
    }
    text.parse()
        .expect("a byte size is a whole number of bytes")
}

/// One column of a step table as the Arrow array of `field`. An empty cell of an optional field
/// is null.
fn column(field: &SchemaField, cells: &[&str]) -> ArrayRef {
    let optional_cell = |cell: &str| -> Option<String> {
        if cell.is_empty() && field.optional {
            None
        } else {
            Some(cell.to_string())
        }
    };
    match &field.ty {
        ParseAsType::String => {
            let values = cells
                .iter()
                .map(|cell| optional_cell(cell))
                .collect::<Vec<_>>();
            StdArc::new(StringArray::from(values))
        }
        ParseAsType::I64 => {
            let mut values = Vec::with_capacity(cells.len());
            for cell in cells {
                let value = optional_cell(cell)
                    .map(|text| text.parse::<i64>().expect("an I64 cell holds an integer"));
                values.push(value);
            }
            StdArc::new(Int64Array::from(values))
        }
        ParseAsType::I32 => {
            let mut values = Vec::with_capacity(cells.len());
            for cell in cells {
                let value = optional_cell(cell)
                    .map(|text| text.parse::<i32>().expect("an I32 cell holds an integer"));
                values.push(value);
            }
            StdArc::new(Int32Array::from(values))
        }
        ParseAsType::F64 => {
            let mut values = Vec::with_capacity(cells.len());
            for cell in cells {
                let value = optional_cell(cell)
                    .map(|text| text.parse::<f64>().expect("an F64 cell holds a number"));
                values.push(value);
            }
            StdArc::new(Float64Array::from(values))
        }
        ParseAsType::Bool => {
            let mut values = Vec::with_capacity(cells.len());
            for cell in cells {
                let value = optional_cell(cell).map(|text| {
                    text.parse::<bool>()
                        .expect("a BOOL cell holds true or false")
                });
                values.push(value);
            }
            StdArc::new(BooleanArray::from(values))
        }
        other => panic!("the harness builds no column of type {other:?}"),
    }
}

/// The rows of a step table, whose first row names the columns, as one batch of `fields`.
fn table_batch(fields: &[SchemaField], step: &Step) -> RecordBatch {
    let table = step
        .table
        .as_ref()
        .expect("the step lists the batch's rows under a header naming its fields");
    let Some((header, rows)) = table.rows.split_first() else {
        panic!("the batch table has no header row");
    };
    let mut columns = Vec::with_capacity(fields.len());
    for field in fields {
        let Some(index) = header.iter().position(|name| name == field.name.as_str()) else {
            panic!("the batch table has no column for field '{}'", field.name);
        };
        let cells = rows
            .iter()
            .map(|row| row[index].as_str())
            .collect::<Vec<_>>();
        columns.push(column(field, &cells));
    }
    let schema = StdArc::new(SchemaField::arrow_schema(fields));
    RecordBatch::try_new(schema, columns).expect("the table's columns make one batch")
}

/// `batches` written as one Arrow IPC stream under their shared schema.
fn arrow_stream(batches: &[RecordBatch]) -> Bytes {
    let first = batches
        .first()
        .expect("a stream carries at least one batch");
    let mut writer =
        StreamWriter::try_new(Vec::new(), first.schema_ref()).expect("the stream writer opens");
    for batch in batches {
        writer
            .write(batch)
            .expect("the stream writer takes the batch");
    }
    writer.finish().expect("the stream writer finishes");
    Bytes::from(
        writer
            .into_inner()
            .expect("the stream writer yields its bytes"),
    )
}

/// A body a scenario submits that is not the canonical stream of the producer's schema.
fn defective_body(fields: &[SchemaField], kind: &str) -> Bytes {
    match kind {
        "not an Arrow stream" => Bytes::from_static(b"these bytes are not an Arrow IPC stream"),
        "of another schema" => {
            let field = SchemaField {
                name: "unexpected".parse().expect("a literal field name"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            };
            let schema = StdArc::new(SchemaField::arrow_schema(std::slice::from_ref(&field)));
            let values: ArrayRef = StdArc::new(StringArray::from(vec!["value"]));
            let batch =
                RecordBatch::try_new(schema, vec![values]).expect("one column makes one batch");
            arrow_stream(&[batch])
        }
        "two record batches" => {
            let batch = RecordBatch::new_empty(StdArc::new(SchemaField::arrow_schema(fields)));
            arrow_stream(&[batch.clone(), batch])
        }
        other => panic!("unknown defective batch kind '{other}'"),
    }
}

impl ScenarioWorld {
    fn scenario_producer(&self, name: &str) -> &ScenarioProducer {
        self.producers
            .producers
            .get(name)
            .unwrap_or_else(|| panic!("producer '{name}' was never opened"))
    }

    fn raw_session(&self, name: &str) -> &RawProducerSession {
        self.producers
            .sessions
            .get(name)
            .unwrap_or_else(|| panic!("WebSocket session '{name}' is not connected"))
    }
}

/// Opens a producer on `session`, or says why the server refused it.
async fn open_producer(
    world: &mut ScenarioWorld,
    session: SessionRef,
    ingestor: &str,
    fields: &str,
    limits: ClientProducerLimits,
) -> Result<ScenarioProducer, OpenRefused> {
    let domain = scenario_domain(world);
    let ingestor = IngestorName::parse(&expand_placeholders(world, ingestor))
        .expect("scenario ingestors are valid names");
    let fields = expected_fields(&expand_placeholders(world, fields));
    match session {
        SessionRef::Client(name) => {
            let client = world
                .transaction_clients
                .get(&name)
                .unwrap_or_else(|| panic!("client '{name}' must be connected"))
                .clone();
            let opened = nervix_primitives::time::timeout(
                PRODUCER_EXPECTATION_TIMEOUT,
                client.open_ingestor(domain, ingestor, fields, limits),
            )
            .await
            .unwrap_or_else(|_| panic!("client '{name}' was not answered its open in time"));
            match opened {
                Ok(producer) => Ok(ScenarioProducer::Native(StdArc::new(producer))),
                Err(report) => match report.current_context() {
                    ClientError::ProducerRefused { refusal, message } => Err(OpenRefused {
                        refusal: *refusal,
                        message: message.clone(),
                    }),
                    other => panic!("client '{name}' failed to open a producer: {other}"),
                },
            }
        }
        SessionRef::WebSocket(name) => {
            let session = world
                .producers
                .sessions
                .get_mut(&name)
                .unwrap_or_else(|| panic!("WebSocket session '{name}' is not connected"));
            let request = ClientRequest::OpenIngestor(OpenIngestorRequest {
                domain,
                ingestor,
                expected_fields: fields,
                limits,
            });
            let request_id = session
                .send(request)
                .await
                .unwrap_or_else(|error| panic!("session '{name}' could not send an open: {error}"));
            let reply = session
                .reply(request_id, PRODUCER_EXPECTATION_TIMEOUT)
                .await
                .unwrap_or_else(|error| panic!("session '{name}' open: {error}"));
            let ReplyBody::OpenIngestor(outcome) = reply.body else {
                panic!("session '{name}' open was answered with {:?}", reply.body);
            };
            match outcome.disposition {
                OpenIngestorDisposition::Opened(opened) => Ok(ScenarioProducer::Raw {
                    session: name,
                    id: ProducerId::opened_by(request_id),
                    description: Box::new(opened.description),
                    frames_seen: 0,
                }),
                OpenIngestorDisposition::Refused(refusal) => Err(OpenRefused {
                    refusal,
                    message: outcome.message,
                }),
            }
        }
    }
}

/// Opens a producer, opening again while the ingestor is not running on its scheduled node yet:
/// right after `START`, while a relocation completes, or after a failover. Any other refusal fails
/// the step at once.
async fn open_named_producer(
    world: &mut ScenarioWorld,
    within: Duration,
    session: SessionRef,
    producer: String,
    ingestor: String,
    fields: String,
    limits: ClientProducerLimits,
) {
    let deadline = Instant::now() + within;
    let producer = expand_placeholders(world, &producer);
    loop {
        nervix_primitives::task::consume_budget().await;
        let opened = open_producer(world, session.clone(), &ingestor, &fields, limits).await;
        let refused = match opened {
            Ok(opened) => {
                assert!(
                    world
                        .producers
                        .producers
                        .insert(producer.clone(), opened)
                        .is_none(),
                    "producer '{producer}' is already open"
                );
                return;
            }
            Err(refused) => refused,
        };
        assert_eq!(
            refused.refusal,
            ClientProducerRefusal::EndpointUnavailable,
            "producer '{producer}' was refused for good: {refused:?}"
        );
        assert!(
            Instant::now() < deadline,
            "producer '{producer}' was still refused after {within:?}: {refused:?}"
        );
        nervix_primitives::time::sleep(PRODUCER_POLL_INTERVAL).await;
    }
}

async fn refused_open(
    world: &mut ScenarioWorld,
    session: SessionRef,
    ingestor: String,
    fields: String,
    expected: String,
) {
    let limits = default_limits();
    refused_open_with(world, session, ingestor, fields, limits, expected).await;
}

/// Opens a producer that must be refused for `expected`. An ingestor that is not running on its
/// scheduled node yet refuses every open as unavailable before its endpoint can check the open, so
/// the step opens again until the endpoint decides.
async fn refused_open_with(
    world: &mut ScenarioWorld,
    session: SessionRef,
    ingestor: String,
    fields: String,
    limits: ClientProducerLimits,
    expected: String,
) {
    let deadline = Instant::now() + PRODUCER_EXPECTATION_TIMEOUT;
    loop {
        nervix_primitives::task::consume_budget().await;
        let opened = open_producer(world, session.clone(), &ingestor, &fields, limits).await;
        let Err(refused) = opened else {
            panic!("a producer on ingestor '{ingestor}' opened although it must be refused");
        };
        let endpoint_starting = refused.refusal == ClientProducerRefusal::EndpointUnavailable
            && expected != ClientProducerRefusal::EndpointUnavailable.as_ref();
        if endpoint_starting && Instant::now() < deadline {
            nervix_primitives::time::sleep(PRODUCER_POLL_INTERVAL).await;
            continue;
        }
        assert_eq!(
            refused.refusal.as_ref(),
            expected,
            "the open was refused for another reason: {refused:?}"
        );
        assert!(
            !refused.message.is_empty(),
            "a refused open says why: {refused:?}"
        );
        return;
    }
}

#[given(expr = "WebSocket session {string} is connected to node {string}")]
async fn given_websocket_session_is_connected(
    world: &mut ScenarioWorld,
    name: String,
    node_id: String,
) {
    let name = expand_placeholders(world, &name);
    let node_id = expand_placeholders(world, &node_id);
    let console = world
        .cluster()
        .web_console_url(&node_id)
        .unwrap_or_else(|error| panic!("node '{node_id}' has no console address: {error}"));
    let mut url = url::Url::parse(&console).expect("the console address is a URL");
    url.set_scheme("ws")
        .unwrap_or_else(|()| panic!("a console address can use the ws scheme"));
    url.set_path("/console/ws");
    let session = RawProducerSession::connect_websocket(url.as_str())
        .await
        .unwrap_or_else(|error| panic!("session '{name}' could not connect: {error}"));
    assert!(
        world
            .producers
            .sessions
            .insert(name.clone(), session)
            .is_none(),
        "WebSocket session '{name}' is already connected"
    );
}

/// Connects a console WebSocket session to the node that leads, the only node that serves one.
#[given(expr = "WebSocket session {string} is connected to the leader node")]
async fn given_websocket_session_is_connected_to_the_leader(
    world: &mut ScenarioWorld,
    name: String,
) {
    let leader = current_leader_node(world).await;
    given_websocket_session_is_connected(world, name, leader).await;
}

#[when(
    expr = "client {string} opens producer {string} on ingestor {string} expecting fields {string}"
)]
async fn when_client_opens_producer(
    world: &mut ScenarioWorld,
    client: String,
    producer: String,
    ingestor: String,
    fields: String,
) {
    let client = expand_placeholders(world, &client);
    let limits = default_limits();
    let session = SessionRef::Client(client);
    open_named_producer(
        world,
        PRODUCER_EXPECTATION_TIMEOUT,
        session,
        producer,
        ingestor,
        fields,
        limits,
    )
    .await;
}

#[when(
    expr = "WebSocket session {string} opens producer {string} on ingestor {string} expecting \
            fields {string}"
)]
async fn when_websocket_session_opens_producer(
    world: &mut ScenarioWorld,
    session: String,
    producer: String,
    ingestor: String,
    fields: String,
) {
    let session = expand_placeholders(world, &session);
    let limits = default_limits();
    let session = SessionRef::WebSocket(session);
    open_named_producer(
        world,
        PRODUCER_EXPECTATION_TIMEOUT,
        session,
        producer,
        ingestor,
        fields,
        limits,
    )
    .await;
}

#[when(
    expr = "client {string} opens producer {string} on ingestor {string} expecting fields \
            {string} with {int} batch(es) and {string} of credit"
)]
async fn when_client_opens_producer_with_credit(
    world: &mut ScenarioWorld,
    client: String,
    producer: String,
    ingestor: String,
    fields: String,
    batches: u32,
    bytes: String,
) {
    let client = expand_placeholders(world, &client);
    let limits = producer_limits(batches, &bytes);
    let session = SessionRef::Client(client);
    open_named_producer(
        world,
        PRODUCER_EXPECTATION_TIMEOUT,
        session,
        producer,
        ingestor,
        fields,
        limits,
    )
    .await;
}

#[when(
    expr = "WebSocket session {string} opens producer {string} on ingestor {string} expecting \
            fields {string} with {int} batch(es) and {string} of credit"
)]
async fn when_websocket_session_opens_producer_with_credit(
    world: &mut ScenarioWorld,
    session: String,
    producer: String,
    ingestor: String,
    fields: String,
    batches: u32,
    bytes: String,
) {
    let session = expand_placeholders(world, &session);
    let limits = producer_limits(batches, &bytes);
    let session = SessionRef::WebSocket(session);
    open_named_producer(
        world,
        PRODUCER_EXPECTATION_TIMEOUT,
        session,
        producer,
        ingestor,
        fields,
        limits,
    )
    .await;
}

#[then(
    expr = "client {string} cannot open a producer on ingestor {string} expecting fields {string} \
            because {string}"
)]
async fn then_client_cannot_open_producer(
    world: &mut ScenarioWorld,
    client: String,
    ingestor: String,
    fields: String,
    expected: String,
) {
    let client = expand_placeholders(world, &client);
    refused_open(
        world,
        SessionRef::Client(client),
        ingestor,
        fields,
        expected,
    )
    .await;
}

#[then(
    expr = "WebSocket session {string} cannot open a producer on ingestor {string} expecting \
            fields {string} because {string}"
)]
async fn then_websocket_session_cannot_open_producer(
    world: &mut ScenarioWorld,
    session: String,
    ingestor: String,
    fields: String,
    expected: String,
) {
    let session = expand_placeholders(world, &session);
    let session = SessionRef::WebSocket(session);
    refused_open(world, session, ingestor, fields, expected).await;
}

#[then(
    expr = "client {string} cannot open a producer on ingestor {string} expecting fields {string} \
            with {int} batch(es) and {string} of credit because {string}"
)]
async fn then_client_cannot_open_producer_with_credit(
    world: &mut ScenarioWorld,
    client: String,
    ingestor: String,
    fields: String,
    batches: u32,
    bytes: String,
    expected: String,
) {
    let client = expand_placeholders(world, &client);
    let limits = producer_limits(batches, &bytes);
    let session = SessionRef::Client(client);
    refused_open_with(world, session, ingestor, fields, limits, expected).await;
}

#[then(
    expr = "WebSocket session {string} cannot open a producer on ingestor {string} expecting \
            fields {string} with {int} batch(es) and {string} of credit because {string}"
)]
async fn then_websocket_session_cannot_open_producer_with_credit(
    world: &mut ScenarioWorld,
    session: String,
    ingestor: String,
    fields: String,
    batches: u32,
    bytes: String,
    expected: String,
) {
    let session = expand_placeholders(world, &session);
    let limits = producer_limits(batches, &bytes);
    let session = SessionRef::WebSocket(session);
    refused_open_with(world, session, ingestor, fields, limits, expected).await;
}

#[when(
    expr = "within {string} client {string} opens producer {string} on ingestor {string} \
            expecting fields {string}"
)]
async fn when_client_eventually_opens_producer(
    world: &mut ScenarioWorld,
    within: String,
    client: String,
    producer: String,
    ingestor: String,
    fields: String,
) {
    let client = expand_placeholders(world, &client);
    let session = SessionRef::Client(client);
    let within = parse_duration_text(&within).expect("the step names a valid duration");
    let limits = default_limits();
    open_named_producer(world, within, session, producer, ingestor, fields, limits).await;
}

#[when(
    expr = "within {string} WebSocket session {string} opens producer {string} on ingestor \
            {string} expecting fields {string}"
)]
async fn when_websocket_session_eventually_opens_producer(
    world: &mut ScenarioWorld,
    within: String,
    session: String,
    producer: String,
    ingestor: String,
    fields: String,
) {
    let session = expand_placeholders(world, &session);
    let session = SessionRef::WebSocket(session);
    let within = parse_duration_text(&within).expect("the step names a valid duration");
    let limits = default_limits();
    open_named_producer(world, within, session, producer, ingestor, fields, limits).await;
}

/// What a step submits: rows, which the Rust client checks against its producer's schema and
/// encodes itself, or a body the scenario wrote.
enum SubmittedBody {
    Rows(RecordBatch),
    Written(Bytes),
}

/// Submits `body` through `producer` under the name `batch`.
async fn submit(world: &mut ScenarioWorld, producer: String, batch: String, body: SubmittedBody) {
    let submission = match world.scenario_producer(&producer) {
        ScenarioProducer::Native(native) => {
            let native = native.clone();
            let body = match body {
                SubmittedBody::Rows(rows) => native.batch(&rows).unwrap_or_else(|error| {
                    panic!("the Rust client did not encode batch '{batch}': {error:?}")
                }),
                SubmittedBody::Written(body) => ProducerBatch::from_arrow_ipc(body),
            };
            let (identified, id) = watch::channel(None);
            let wait = nervix_primitives::task::spawn(async move {
                let submitted = native.submit(body).await;
                let id = submitted.map_err(|error| error.to_string())?;
                identified.send_replace(Some(id));
                let outcome = native.rejoin(id).await.map_err(|error| error.to_string())?;
                Ok(Answered {
                    outcome: ObservedOutcome::from_native(outcome),
                    at: Instant::now(),
                })
            });
            SubmissionState::Native(NativeSubmission {
                id,
                wait: Some(AbortOnDropHandle::new(wait)),
                answered: None,
            })
        }
        ScenarioProducer::Raw { session, id, .. } => {
            let session = session.clone();
            let producer_id = *id;
            let raw = world
                .producers
                .sessions
                .get_mut(&session)
                .unwrap_or_else(|| panic!("WebSocket session '{session}' is not connected"));
            let body = match body {
                SubmittedBody::Rows(rows) => arrow_stream(&[rows]),
                SubmittedBody::Written(body) => body,
            };
            let request = ClientRequest::SubmitBatch(SubmitBatchRequest {
                producer: producer_id,
                batch: body,
            });
            let request = raw
                .send(request)
                .await
                .unwrap_or_else(|error| panic!("batch '{batch}' could not be sent: {error}"));
            SubmissionState::Raw { session, request }
        }
    };
    let submission = ScenarioSubmission {
        producer,
        state: submission,
    };
    assert!(
        world
            .producers
            .submissions
            .insert(batch.clone(), submission)
            .is_none(),
        "batch '{batch}' was already submitted"
    );
}

#[when(expr = "producer {string} submits batch {string} with rows")]
async fn when_producer_submits_rows(
    world: &mut ScenarioWorld,
    producer: String,
    batch: String,
    #[step] step: &Step,
) {
    let fields = world
        .scenario_producer(&producer)
        .description()
        .fields
        .clone();
    let rows = table_batch(&fields, step);
    submit(world, producer, batch, SubmittedBody::Rows(rows)).await;
}

#[when(expr = "producer {string} submits batch {string} with one {int}-byte id")]
async fn when_producer_submits_long_id(
    world: &mut ScenarioWorld,
    producer: String,
    batch: String,
    length: usize,
) {
    let fields = world
        .scenario_producer(&producer)
        .description()
        .fields
        .clone();
    assert_eq!(
        fields.len(),
        2,
        "long-id scenario uses id and amount fields"
    );
    let columns: Vec<ArrayRef> = vec![
        StdArc::new(StringArray::from(vec!["x".repeat(length)])),
        StdArc::new(Int64Array::from(vec![1])),
    ];
    let rows = RecordBatch::try_new(StdArc::new(SchemaField::arrow_schema(&fields)), columns)
        .expect("long id and amount match the producer schema");
    submit(world, producer, batch, SubmittedBody::Rows(rows)).await;
}

#[then(expr = "batch {string} remains pending for {string}")]
async fn then_batch_remains_pending(world: &mut ScenarioWorld, batch: String, duration: String) {
    let duration = parse_duration_text(&duration).expect("a literal duration");
    let submission = world
        .producers
        .submissions
        .get(&batch)
        .unwrap_or_else(|| panic!("batch '{batch}' was not submitted"));
    match &submission.state {
        SubmissionState::Native(native) => {
            nervix_primitives::time::sleep(duration).await;
            assert!(
                native.answered.is_none()
                    && native.wait.as_ref().is_some_and(|wait| !wait.is_finished()),
                "batch '{batch}' completed before application ACK"
            );
        }
        SubmissionState::Raw { session, request } => {
            let raw = world
                .producers
                .sessions
                .get(session)
                .unwrap_or_else(|| panic!("WebSocket session '{session}' is not connected"));
            assert!(
                raw.reply(*request, duration).await.is_err(),
                "batch '{batch}' completed before application ACK"
            );
        }
    }
}

#[when(expr = "producer {string} submits batch {string} that is {string}")]
async fn when_producer_submits_defective_batch(
    world: &mut ScenarioWorld,
    producer: String,
    batch: String,
    kind: String,
) {
    let fields = world
        .scenario_producer(&producer)
        .description()
        .fields
        .clone();
    let body = defective_body(&fields, &kind);
    submit(world, producer, batch, SubmittedBody::Written(body)).await;
}

/// Waits for the terminal outcome of `batch`.
async fn answered(world: &mut ScenarioWorld, batch: &str) -> Answered {
    let submission = world
        .producers
        .submissions
        .get_mut(batch)
        .unwrap_or_else(|| panic!("batch '{batch}' was never submitted"));
    match &mut submission.state {
        SubmissionState::Native(native) => {
            if let Some(answered) = &native.answered {
                return answered.clone();
            }
            let Some(wait) = native.wait.as_mut() else {
                panic!("nobody waits for batch '{batch}' any more");
            };
            let joined = nervix_primitives::time::timeout(PRODUCER_EXPECTATION_TIMEOUT, wait)
                .await
                .unwrap_or_else(|_| panic!("batch '{batch}' was not answered in time"));
            let result = joined.unwrap_or_else(|error| panic!("batch '{batch}' task: {error}"));
            let answered = result.unwrap_or_else(|error| panic!("batch '{batch}': {error}"));
            native.wait = None;
            native.answered = Some(answered.clone());
            answered
        }
        SubmissionState::Raw { session, request } => {
            let session = session.clone();
            let request = *request;
            let reply = world
                .raw_session(&session)
                .reply(request, PRODUCER_EXPECTATION_TIMEOUT)
                .await
                .unwrap_or_else(|error| panic!("batch '{batch}': {error}"));
            let ReplyBody::Submission(outcome) = reply.body else {
                panic!("batch '{batch}' was answered with {:?}", reply.body);
            };
            Answered {
                outcome: ObservedOutcome::from_wire(outcome.outcome, outcome.message),
                at: reply.arrived_at,
            }
        }
    }
}

async fn assert_outcome(world: &mut ScenarioWorld, batch: String, class: &str, cause: &str) {
    let batch = expand_placeholders(world, &batch);
    let answered = answered(world, &batch).await;
    assert_eq!(
        (answered.outcome.class, answered.outcome.cause.as_str()),
        (class, cause),
        "batch '{batch}' was answered otherwise: {:?}",
        answered.outcome
    );
}

#[then(expr = "batch {string} completes")]
async fn then_batch_completes(world: &mut ScenarioWorld, batch: String) {
    assert_outcome(world, batch, "completed", "").await;
}

#[then(expr = "batch {string} is not admitted because {string}")]
async fn then_batch_is_not_admitted(world: &mut ScenarioWorld, batch: String, cause: String) {
    assert_outcome(world, batch, "not admitted", &cause).await;
}

#[then(expr = "batch {string} fails processing because {string}")]
async fn then_batch_fails_processing(world: &mut ScenarioWorld, batch: String, cause: String) {
    assert_outcome(world, batch, "processing failed", &cause).await;
}

#[then(expr = "batch {string} has an unknown outcome because {string}")]
async fn then_batch_outcome_is_unknown(world: &mut ScenarioWorld, batch: String, cause: String) {
    assert_outcome(world, batch, "unknown outcome", &cause).await;
}

/// The receiver records a request before its scripted delay begins, and the client records the
/// outcome when it arrives, so load moves both instants the same way and only lengthens the gap.
#[then(
    expr = "batch {string} completed at least {string} after HTTP receiver {string} request {int} \
            arrived"
)]
async fn then_batch_completed_after_receiver_request(
    world: &mut ScenarioWorld,
    batch: String,
    minimum_gap: String,
    receiver: String,
    position: usize,
) {
    let minimum_gap =
        parse_duration_text(&minimum_gap).expect("the step names a valid minimum gap");
    let answered = answered(world, &batch).await;
    assert_eq!(
        answered.outcome.class, "completed",
        "batch '{batch}' did not complete: {:?}",
        answered.outcome
    );
    let request = captured_http_request(world, &receiver, position);
    let Some(gap) = answered.at.checked_duration_since(request.received_at) else {
        panic!("batch '{batch}' completed before HTTP receiver '{receiver}' request {position}");
    };
    assert!(
        gap >= minimum_gap,
        "batch '{batch}' completed {gap:?} after HTTP receiver '{receiver}' request {position}, \
         below the {minimum_gap:?} minimum"
    );
}

/// Stops waiting for a batch the Rust client submits. A batch still waiting for credit is never
/// sent; one the producer already holds stays with it.
#[when(expr = "the wait for batch {string} is cancelled")]
async fn when_wait_for_batch_is_cancelled(world: &mut ScenarioWorld, batch: String) {
    let submission = world
        .producers
        .submissions
        .get_mut(&batch)
        .unwrap_or_else(|| panic!("batch '{batch}' was never submitted"));
    let SubmissionState::Native(native) = &mut submission.state else {
        panic!("only the Rust client's waits can be cancelled");
    };
    let Some(wait) = native.wait.take() else {
        panic!("nobody waits for batch '{batch}'");
    };
    drop(wait);
}

/// Waits until the producer holds `batch`, which a batch waiting for credit never does.
async fn submission_id(world: &ScenarioWorld, batch: &str) -> SubmissionId {
    let submission = world
        .producers
        .submissions
        .get(batch)
        .unwrap_or_else(|| panic!("batch '{batch}' was never submitted"));
    let SubmissionState::Native(native) = &submission.state else {
        panic!("only the Rust client's batches carry a submission identity");
    };
    let mut id = native.id.clone();
    let identified = nervix_primitives::time::timeout(
        PRODUCER_EXPECTATION_TIMEOUT,
        id.wait_for(|id| id.is_some()),
    )
    .await
    .unwrap_or_else(|_| panic!("the producer did not take batch '{batch}' in time"));
    let identified = identified.unwrap_or_else(|_| panic!("batch '{batch}' was never taken"));
    identified.expect("the wait ended on an identity")
}

#[then(expr = "producer {string} holds batch(es) {string}")]
async fn then_producer_holds_batches(world: &mut ScenarioWorld, producer: String, batches: String) {
    let mut expected = Vec::new();
    for batch in batches.split(',').map(str::trim) {
        expected.push(submission_id(world, batch).await);
    }
    let ScenarioProducer::Native(native) = world.scenario_producer(&producer) else {
        panic!("only the Rust client lists the submissions it holds");
    };
    let held = native
        .pending_submissions()
        .into_iter()
        .map(|pending| pending.id)
        .collect::<Vec<_>>();
    assert_eq!(
        held, expected,
        "producer '{producer}' holds other submissions than batches {batches}"
    );
}

#[then(expr = "producer {string} holds no batches")]
async fn then_producer_holds_no_batches(world: &mut ScenarioWorld, producer: String) {
    let ScenarioProducer::Native(native) = world.scenario_producer(&producer) else {
        panic!("only the Rust client lists the submissions it holds");
    };
    let held = native.pending_submissions();
    assert!(
        held.is_empty(),
        "producer '{producer}' still holds submissions: {held:?}"
    );
}

#[when(expr = "producer {string} rejoins batch {string}")]
async fn when_producer_rejoins_batch(world: &mut ScenarioWorld, producer: String, batch: String) {
    let id = submission_id(world, &batch).await;
    let ScenarioProducer::Native(native) = world.scenario_producer(&producer) else {
        panic!("only the Rust client rejoins a submission");
    };
    let native = native.clone();
    let wait = nervix_primitives::task::spawn(async move {
        let outcome = native.rejoin(id).await.map_err(|error| error.to_string())?;
        Ok(Answered {
            outcome: ObservedOutcome::from_native(outcome),
            at: Instant::now(),
        })
    });
    let submission = world
        .producers
        .submissions
        .get_mut(&batch)
        .unwrap_or_else(|| panic!("batch '{batch}' was never submitted"));
    let SubmissionState::Native(state) = &mut submission.state else {
        panic!("only the Rust client rejoins a submission");
    };
    state.wait = Some(AbortOnDropHandle::new(wait));
}

#[then(expr = "producer {string} eventually reports admission {string}")]
async fn then_producer_reports_admission(
    world: &mut ScenarioWorld,
    producer: String,
    expected: String,
) {
    let expected = match expected.as_str() {
        "open" => ClientProducerAdmission::Open,
        "suspended" => ClientProducerAdmission::Suspended,
        other => panic!("unknown admission '{other}'"),
    };
    let deadline = Instant::now() + PRODUCER_EXPECTATION_TIMEOUT;
    let raw = match world.scenario_producer(&producer) {
        ScenarioProducer::Native(native) => loop {
            nervix_primitives::task::consume_budget().await;
            if native.admission() == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "producer '{producer}' reports admission {:?}, not {expected:?}",
                native.admission()
            );
            nervix_primitives::time::sleep(PRODUCER_POLL_INTERVAL).await;
        },
        ScenarioProducer::Raw {
            session,
            id,
            frames_seen,
            ..
        } => (session.clone(), *id, *frames_seen),
    };
    let (session, id, seen) = raw;
    let found = world
        .raw_session(&session)
        .producer_frame(
            id,
            seen,
            PRODUCER_EXPECTATION_TIMEOUT,
            "an admission change",
            |frame| {
                let ProducerFrame::Admission(changed) = frame else {
                    return false;
                };
                changed.admission == expected
            },
        )
        .await
        .unwrap_or_else(|error| panic!("producer '{producer}': {error}"));
    if let Some(ScenarioProducer::Raw { frames_seen, .. }) =
        world.producers.producers.get_mut(&producer)
    {
        *frames_seen = found.seen;
    }
}

#[then(expr = "producer {string} eventually ends because {string}")]
async fn then_producer_ends(world: &mut ScenarioWorld, producer: String, expected: String) {
    let deadline = Instant::now() + PRODUCER_EXPECTATION_TIMEOUT;
    let raw = match world.scenario_producer(&producer) {
        ScenarioProducer::Native(native) => loop {
            nervix_primitives::task::consume_budget().await;
            match native.end() {
                Some(ProducerEnd::Ended { reason, message }) => {
                    assert_eq!(
                        reason.as_ref(),
                        expected,
                        "producer '{producer}' ended otherwise: {message}"
                    );
                    return;
                }
                Some(ProducerEnd::SessionLost) => {
                    assert_eq!(
                        expected, "session lost",
                        "producer '{producer}' ended with its session"
                    );
                    return;
                }
                Some(ProducerEnd::ReopenRequired(reason)) => {
                    let actual = match reason {
                        ProducerReopenReason::DomainStopped => "domain stopped",
                        ProducerReopenReason::EndpointRemoved => "endpoint removed",
                        ProducerReopenReason::SchemaChanged => "schema changed",
                        ProducerReopenReason::ContractChanged => "endpoint changed",
                        ProducerReopenReason::GenerationChanged => "generation changed",
                        ProducerReopenReason::ProtocolViolated => "protocol violated",
                        ProducerReopenReason::Refused(_) => "open refused",
                    };
                    assert_eq!(actual, expected, "producer '{producer}' needs a new open");
                    return;
                }
                Some(ProducerEnd::Closed) => panic!("producer '{producer}' was closed"),
                None => {
                    assert!(
                        Instant::now() < deadline,
                        "producer '{producer}' did not end in time"
                    );
                    nervix_primitives::time::sleep(PRODUCER_POLL_INTERVAL).await;
                }
            }
        },
        ScenarioProducer::Raw { session, id, .. } => (session.clone(), *id),
    };
    let (session, id) = raw;
    let found = world
        .raw_session(&session)
        .producer_frame(
            id,
            0,
            PRODUCER_EXPECTATION_TIMEOUT,
            "the producer's end",
            |frame| matches!(frame, ProducerFrame::Ended(_)),
        )
        .await
        .unwrap_or_else(|error| panic!("producer '{producer}': {error}"));
    let ProducerFrame::Ended(ended) = found.frame else {
        panic!("the wait accepts only an end");
    };
    assert_eq!(
        ended.reason.as_ref(),
        expected,
        "producer '{producer}' ended otherwise: {}",
        ended.message
    );
}

#[then(expr = "producer {string} is interrupted because {string}")]
async fn then_producer_is_interrupted(
    world: &mut ScenarioWorld,
    producer: String,
    expected: String,
) {
    let native = match world.scenario_producer(&producer) {
        ScenarioProducer::Native(native) => Some(native.clone()),
        ScenarioProducer::Raw { .. } => None,
    };
    let Some(native) = native else {
        then_producer_ends(world, producer, expected).await;
        return;
    };
    assert!(matches!(
        expected.as_str(),
        "relocated" | "shutting down" | "session lost"
    ));
    let deadline = Instant::now() + PRODUCER_EXPECTATION_TIMEOUT;
    loop {
        nervix_primitives::task::consume_budget().await;
        if matches!(
            native.connection(),
            ProducerConnection::Interrupted | ProducerConnection::Restoring
        ) {
            assert!(
                native.end().is_none(),
                "a transient interruption keeps the desired producer"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "producer '{producer}' was not interrupted in time"
        );
        nervix_primitives::time::sleep(PRODUCER_POLL_INTERVAL).await;
    }
}

#[when(expr = "producer {string} is closed")]
async fn when_producer_is_closed(world: &mut ScenarioWorld, producer: String) {
    let Some(opened) = world.producers.producers.remove(&producer) else {
        panic!("producer '{producer}' was never opened");
    };
    match opened {
        ScenarioProducer::Native(native) => {
            // A batch's wait holds the producer; every batch is answered before a close is.
            for submission in world.producers.submissions.values_mut() {
                if submission.producer != producer {
                    continue;
                }
                let SubmissionState::Native(state) = &mut submission.state else {
                    continue;
                };
                if let Some(wait) = state.wait.take() {
                    let joined =
                        nervix_primitives::time::timeout(PRODUCER_EXPECTATION_TIMEOUT, wait)
                            .await
                            .unwrap_or_else(|_| panic!("a batch of '{producer}' was not answered"));
                    let result = joined.unwrap_or_else(|error| panic!("a batch task: {error}"));
                    let answered =
                        result.unwrap_or_else(|error| panic!("a batch of '{producer}': {error}"));
                    state.answered = Some(answered);
                }
            }
            let native = StdArc::try_unwrap(native)
                .unwrap_or_else(|_| panic!("producer '{producer}' is still shared"));
            nervix_primitives::time::timeout(PRODUCER_EXPECTATION_TIMEOUT, native.close())
                .await
                .unwrap_or_else(|_| panic!("producer '{producer}' was not closed in time"))
                .unwrap_or_else(|error| panic!("producer '{producer}' close failed: {error:?}"));
        }
        ScenarioProducer::Raw { session, id, .. } => {
            let raw = world
                .producers
                .sessions
                .get_mut(&session)
                .unwrap_or_else(|| panic!("WebSocket session '{session}' is not connected"));
            let request = raw
                .send(ClientRequest::CloseIngestor(CloseIngestorRequest {
                    producer: id,
                }))
                .await
                .unwrap_or_else(|error| panic!("producer '{producer}' close: {error}"));
            let reply = raw
                .reply(request, PRODUCER_EXPECTATION_TIMEOUT)
                .await
                .unwrap_or_else(|error| panic!("producer '{producer}' close: {error}"));
            let ReplyBody::CloseIngestor(outcome) = reply.body else {
                panic!(
                    "producer '{producer}' close was answered with {:?}",
                    reply.body
                );
            };
            assert_eq!(
                outcome.disposition,
                CloseIngestorDisposition::Closed,
                "producer '{producer}' was not open: {}",
                outcome.message
            );
        }
    }
}

/// Runs `DESCRIBE INGESTOR` on the leader until its output contains every line of the docstring.
/// The leader is the one the running nodes agree on, so the step also holds after a scenario
/// stopped a node.
#[then(expr = "within {string} the leader node describes ingestor {string} with")]
async fn then_leader_describes_ingestor_with(
    world: &mut ScenarioWorld,
    within: String,
    ingestor: String,
    #[step] step: &Step,
) {
    let within = parse_duration_text(&within).expect("the step names a valid duration");
    let expected = expand_placeholders(world, docstring(step));
    let ingestor = expand_placeholders(world, &ingestor);
    let deadline = Instant::now() + within;
    loop {
        nervix_primitives::task::consume_budget().await;
        let leader = running_leader_node(world).await;
        let described = world
            .cluster()
            .run_command(
                &leader,
                &world.domain,
                &format!("DESCRIBE INGESTOR {ingestor};"),
            )
            .await;
        let output = match described {
            Ok(output) => output,
            Err(error) => format!("DESCRIBE failed: {error}"),
        };
        let mut missing = Vec::new();
        for line in expected
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            if !output.contains(line) {
                missing.push(line.to_string());
            }
        }
        if missing.is_empty() {
            world.last_command_output = Some(output);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "DESCRIBE INGESTOR {ingestor} never reported {missing:?}; last output:\n{output}"
        );
        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
    }
}
