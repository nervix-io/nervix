//! The package at its binding boundary, with CPython embedded in the test process: blocking waits
//! that release the GIL and are cancelled from another Python thread, objects whose lifetime is
//! Python's, the exception every failure kind raises, and the Arrow PyCapsule exchange.
//!
//! Endpoint tests run Python in steps against an in-process session server: a Python thread
//! waits in the package with the GIL released while the test answers the request it sent.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

mod server;

use std::{
    ffi::CStr,
    num::{NonZeroU32, NonZeroU64},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::{
    AckWindow, ClientAttachmentId, ClientEndpointContract, ClientProducerAdmission,
    ClientProducerDescription, ClientProducerGrant, ClientProducerPolicy, DomainName, IngestorName,
    RowSchema, SchemaField, SubscriptionEvent, SubscriptionHandle, SubscriptionRowsEvent,
    wire::{
        ClientRequest, CloseIngestorDisposition, CloseIngestorOutcome, OpenIngestorDisposition,
        OpenIngestorOutcome, ProducerOpened, ReplyBody, ServerEvent, ServerMessage, SessionLimits,
        SubmissionOutcome, SubscriptionRowsEncoder,
    },
};
use nervix_client_host as host;
use nervix_models::{ClientSubmissionOutcome, ParseAsType, RelayName};
use nervix_primitives::sync::Arc;
use pyo3::{prelude::*, types::PyDict};

use self::server::TestServer;

/// Python globals a test keeps across the steps it runs, with the package imported as `nx`.
struct Script {
    globals: Py<PyDict>,
}

impl Script {
    fn new() -> Self {
        Python::initialize();
        Python::attach(|py| {
            let globals = globals(py).assured("the package imports into fresh globals");
            Self {
                globals: globals.unbind(),
            }
        })
    }

    /// Sets the global `name` to what `value` makes.
    fn set(&self, name: &str, value: impl for<'py> FnOnce(Python<'py>) -> PyResult<Py<PyAny>>) {
        Python::attach(|py| {
            let globals = self.globals.bind(py);
            let set = value(py).and_then(|value| globals.set_item(name, value));
            if let Err(error) = set {
                error.print(py);
                panic!("setting {name} raised {error}");
            }
        });
    }

    /// Runs one step of the script, failing the test with Python's traceback when it raises. The
    /// GIL is released when the step returns, so the threads it started run on.
    fn run(&self, code: &CStr) {
        Python::attach(|py| {
            let globals = self.globals.bind(py);
            if let Err(error) = py.run(code, Some(globals), None) {
                error.print(py);
                panic!("the Python code raised {error}");
            }
        });
    }
}

/// Runs `code` with the package imported as `nx`, failing the test with Python's traceback when it
/// raises.
fn run(code: &CStr) {
    run_with(code, |_, _| Ok(()));
}

/// Runs `code` with the package imported as `nx` and the globals `prepare` sets.
fn run_with(code: &CStr, prepare: impl FnOnce(Python<'_>, &Bound<'_, PyDict>) -> PyResult<()>) {
    Python::initialize();
    Python::attach(|py| {
        let outcome = globals(py).and_then(|globals| {
            prepare(py, &globals)?;
            py.run(code, Some(&globals), None)
        });
        if let Err(error) = outcome {
            error.print(py);
            panic!("the Python code raised {error}");
        }
    });
}

/// Globals with the package imported as `nx`, registered under its own name so threads and
/// `import` find it.
fn globals(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    let module = pyo3::wrap_pymodule!(crate::nervix_client)(py);
    let modules = py.import("sys")?.getattr("modules")?;
    modules.set_item("nervix_client", &module)?;
    let globals = PyDict::new(py);
    globals.set_item("nx", module)?;
    Ok(globals)
}

fn name<N>(raw: &str) -> N
where
    N: for<'a> TryFrom<&'a str, Error = nervix_models::NameError>,
{
    N::try_from(raw).assured("the test passes a valid name")
}

fn field(raw: &str, ty: ParseAsType) -> SchemaField {
    SchemaField {
        name: name(raw),
        ty,
        optional: false,
        sensitive: false,
    }
}

fn list(element: ParseAsType) -> ParseAsType {
    ParseAsType::Vec {
        element: Box::new(element),
    }
}

#[test]
fn a_wait_releases_the_gil_and_another_python_thread_cancels_it() {
    run(c"
import socket, threading, time

# A server that accepts every connection and never answers, so opening a session waits until its
# token ends the wait.
listener = socket.create_server(('127.0.0.1', 0))
held = []
def accept():
    while True:
        try:
            held.append(listener.accept()[0])
        except OSError:
            return
threading.Thread(target=accept, daemon=True).start()
port = listener.getsockname()[1]

cancel = nx.Cancel()
ticks = []
def tick_then_cancel():
    for _ in range(5):
        ticks.append(time.monotonic())
        time.sleep(0.02)
    cancel.trigger()
canceller = threading.Thread(target=tick_then_cancel)
canceller.start()
try:
    nx.Session(f'http://127.0.0.1:{port}', domain='sim', cancel=cancel)
except nx.CancelledError as failure:
    assert failure.kind == nx.FailureKind.CANCELLED, failure.kind
    assert isinstance(failure, nx.ClientError)
    assert failure.execution_reference is None
else:
    raise AssertionError('a wait nothing answers returned without its token')
canceller.join()
# The other thread ran five times while the main thread waited, so the wait released the GIL.
assert len(ticks) == 5, ticks

# A deadline ends the same wait on its own.
started = time.monotonic()
try:
    nx.Session(f'http://127.0.0.1:{port}', cancel=nx.Cancel(deadline=0.05))
except nx.DeadlineError as failure:
    assert failure.kind == nx.FailureKind.DEADLINE
else:
    raise AssertionError('an expired wait returned without failing')
assert time.monotonic() - started < 30
listener.close()
for connection in held:
    connection.close()
");
}

#[test]
fn every_failure_kind_raises_its_exception() {
    use nervix_client_core::{ClientError, CommandExecutionReference};
    use nervix_client_host::{Failure, FailureKind};

    let kinds = [
        (
            FailureKind::InvalidArgument,
            "InvalidArgumentError",
            "INVALID_ARGUMENT",
        ),
        (FailureKind::Connect, "ConnectError", "CONNECT"),
        (FailureKind::Transport, "TransportError", "TRANSPORT"),
        (FailureKind::Uncertain, "UncertainOutcomeError", "UNCERTAIN"),
        (FailureKind::Rejected, "RejectedError", "REJECTED"),
        (FailureKind::Deadline, "DeadlineError", "DEADLINE"),
        (FailureKind::Cancelled, "CancelledError", "CANCELLED"),
        (FailureKind::Overflow, "EventOverflowError", "OVERFLOW"),
        (FailureKind::Protocol, "ProtocolError", "PROTOCOL"),
        (FailureKind::Type, "TypeMismatchError", "TYPE"),
        (FailureKind::Closed, "ClosedError", "CLOSED"),
        (
            FailureKind::Interrupted,
            "ConsumerInterruptedError",
            "INTERRUPTED",
        ),
        (
            FailureKind::ReopenRequired,
            "ReopenRequiredError",
            "REOPEN_REQUIRED",
        ),
    ];
    for (kind, class, member) in kinds {
        let reference = CommandExecutionReference::parse("cer-test-reference")
            .assured("the test reference is valid");
        let failure = Failure::new(kind, format!("the {member} failure")).concerning(&reference);
        run_with(
            c"
assert isinstance(raised, getattr(nx, expected_class)), type(raised)
assert isinstance(raised, nx.ClientError)
assert raised.kind == getattr(nx.FailureKind, expected_member), raised.kind
assert raised.message == f'the {expected_member} failure', raised.message
assert str(raised) == raised.message
assert raised.execution_reference == 'cer-test-reference'
assert raised.open_refusal is None
",
            |py, globals| {
                let error = crate::errors::raise(py, failure.clone());
                globals.set_item("raised", error.value(py))?;
                globals.set_item("expected_class", class)?;
                globals.set_item("expected_member", member)
            },
        );
    }

    // A refused open carries its refusal, classified by the report's context.
    let refused = Failure::from(ClientError::SessionClosed);
    run_with(
        c"
assert isinstance(raised, nx.ClosedError)
assert raised.open_refusal is None
assert raised.execution_reference is None
assert 'session' in raised.message
",
        |py, globals| globals.set_item("raised", crate::errors::raise(py, refused).value(py)),
    );
}

/// A rows event of an unbranched relay with an id, an optional name and a sensitive secret, whose
/// frame the wire contract encoded: one row with a name and one without.
fn rows_event() -> host::Event {
    let schema = RowSchema {
        fields: vec![
            field("id", ParseAsType::U32),
            SchemaField {
                optional: true,
                ..field("name", ParseAsType::String)
            },
            SchemaField {
                sensitive: true,
                ..field("secret", ParseAsType::String)
            },
        ],
        branch: None,
    };
    let handle = SubscriptionHandle {
        name: name("watch"),
        generation: NonZeroU64::new(7).assured("seven is not zero"),
    };
    let limits = SessionLimits::DEFAULT;
    let mut rows = SubscriptionRowsEncoder::unbranched(handle, &limits)
        .assured("the subscription fits the limits");
    rows.push_row(|cells| {
        cells.push_u32(7)?;
        cells.push_string("seven")?;
        cells.push_redacted()
    })
    .assured("the row fits the limits");
    rows.push_row(|cells| {
        cells.push_u32(u32::MAX)?;
        cells.push_null()?;
        cells.push_redacted()
    })
    .assured("the row fits the limits");
    let frame = rows
        .finish()
        .assured("the batch fits the limits")
        .verify(&limits)
        .assured("an encoded frame verifies");
    let ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) =
        ServerMessage::decode(&frame).assured("an encoded frame decodes")
    else {
        panic!("a rows frame decodes as rows");
    };
    let event = SubscriptionEvent::Rows(SubscriptionRowsEvent {
        relay: name::<RelayName>("orders"),
        schema: Arc::new(schema),
        rows,
    });
    host::Event::new(event).assured("the rows conform to their schema")
}

#[test]
fn a_frame_view_keeps_its_event_alive() {
    let event = rows_event();
    run_with(
        c"
import ctypes, gc, sys

assert event.kind == nx.EventKind.ROWS
assert event.row_count == 2
frame = bytes(event.frame)
assert len(frame) > 0
assert event.frame.readonly

# A view holds a reference to the event, and releasing it drops that reference.
held = sys.getrefcount(event)
view = memoryview(event)
assert sys.getrefcount(event) == held + 1, (held, sys.getrefcount(event))
view.release()
assert sys.getrefcount(event) == held

# The frame is read only.
try:
    ctypes.c_char.from_buffer(event)
except (BufferError, TypeError):
    pass
else:
    raise AssertionError('a writable view of a frame was granted')

# A withheld value reads as REDACTED from the event, and as a null of a nullable field from its
# rows, whose states tell the two apart.
assert event.column(0) == [7, 2**32 - 1]
assert event.column(1) == ['seven', None]
assert event.column(2) == [nx.REDACTED, nx.REDACTED]
assert event.states(1) == [nx.CellState.VALUE, nx.CellState.NULL]
assert event.states(2) == [nx.CellState.REDACTED, nx.CellState.REDACTED]
rows = event.rows()
assert [field.nullable for field in rows.schema.fields] == [False, True, True]
assert rows.column(2) == [None, None]
assert rows.column(1) == ['seven', None]

# Once the views are the only references left, they still read what they borrowed.
view = memoryview(event)
stream = rows.ipc
streamed = bytes(stream)
del event, rows
gc.collect()
assert bytes(view) == frame
assert bytes(stream) == streamed
view.release()
stream.release()
",
        |py, globals| {
            let event = Bound::new(py, crate::event::Event::new(event))?;
            globals.set_item("event", event)
        },
    );
}

/// The fields of the producer the endpoint tests open: an id and a list of tags.
fn endpoint_fields() -> Vec<SchemaField> {
    vec![
        field("id", ParseAsType::U32),
        field("tags", list(ParseAsType::String)),
    ]
}

fn opened_producer() -> ReplyBody {
    let description = ClientProducerDescription {
        attachment: ClientAttachmentId::from_u128(7),
        fields: endpoint_fields(),
        generation: 3,
        contract: ClientEndpointContract::from_digest([5; 32]),
        policy: ClientProducerPolicy {
            window: AckWindow::Parallel {
                max: NonZeroU64::new(4).assured("four is not zero"),
            },
            ack_timeout: std::time::Duration::from_secs(30),
            retry_backoff: std::time::Duration::from_millis(100),
            retry_max_backoff: std::time::Duration::from_secs(1),
        },
        grant: ClientProducerGrant {
            batches: NonZeroU32::new(2).assured("two is not zero"),
            bytes: NonZeroU64::new(4096).assured("a literal size is not zero"),
            max_batch_bytes: NonZeroU64::new(4096).assured("a literal size is not zero"),
            max_batch_rows: NonZeroU32::new(16).assured("a literal count is not zero"),
        },
        admission: ClientProducerAdmission::Open,
    };
    ReplyBody::OpenIngestor(OpenIngestorOutcome {
        disposition: OpenIngestorDisposition::Opened(Box::new(ProducerOpened {
            domain: DomainName::parse("sim").assured("the test domain name is valid"),
            ingestor: IngestorName::parse("orders_in").assured("the test ingestor name is valid"),
            description,
        })),
        message: "producer attached".to_owned(),
    })
}

#[test]
fn a_producer_outlives_its_session() {
    let mut server = TestServer::start();
    let script = Script::new();
    let address = server.address.to_string();
    script.set("address", |py| {
        Ok(address.into_pyobject(py)?.into_any().unbind())
    });
    script.run(
        c"
import gc, sys, threading

session = nx.Session(f'http://{address}', domain='sim')
references = sys.getrefcount(session)
fields = nx.Fields()
fields.add('id', nx.FieldType.U32)
fields.add('tags', nx.FieldType.LIST)
fields.element(nx.FieldType.STRING)
opened = []
def open_producer():
    opened.append(session.open_ingestor('sim', 'orders_in', fields, batches=2, bytes=4096))
opening = threading.Thread(target=open_producer)
opening.start()
",
    );
    let mut exchange = server.next_exchange();
    let (request_id, request) = server.next_request(&mut exchange);
    let ClientRequest::OpenIngestor(open) = request else {
        panic!("an open is an OpenIngestor request");
    };
    assert_eq!(open.expected_fields, endpoint_fields());
    assert_eq!(open.limits.batches.get(), 2);
    assert_eq!(open.limits.bytes.get(), 4096);
    server.answer(&exchange, request_id, opened_producer());

    // The producer holds no reference to the session object, so deleting it collects it.
    script.run(
        c"
opening.join()
producer = opened.pop()
assert producer.state == nx.EndpointState.ACTIVE
assert sys.getrefcount(session) == references, (references, sys.getrefcount(session))
del session
gc.collect()
assert producer.state == nx.EndpointState.ACTIVE

builder = nx.BatchBuilder(producer.schema, 1)
builder.fixed(0, 0, (5).to_bytes(4, sys.byteorder))
builder.offsets(1, 0, [0, 1])
builder.varlen(1, 1, [0, 5], b'tag-5')
batch = builder.finish()
outcomes = []
def submit():
    outcomes.append(producer.rejoin(producer.submit(batch)))
submitting = threading.Thread(target=submit)
submitting.start()
",
    );
    let (request_id, request) = server.next_request(&mut exchange);
    assert!(matches!(request, ClientRequest::SubmitBatch(_)));
    server.answer(
        &exchange,
        request_id,
        ReplyBody::Submission(SubmissionOutcome {
            outcome: ClientSubmissionOutcome::Completed,
            message: String::new(),
        }),
    );

    script.run(
        c"
submitting.join()
assert outcomes.pop().result == nx.SubmissionResult.COMPLETED
closing = threading.Thread(target=producer.close)
closing.start()
",
    );
    let (request_id, request) = server.next_request(&mut exchange);
    assert!(matches!(request, ClientRequest::CloseIngestor(_)));
    server.answer(
        &exchange,
        request_id,
        ReplyBody::CloseIngestor(CloseIngestorOutcome {
            disposition: CloseIngestorDisposition::Closed,
            message: String::new(),
        }),
    );

    // Collecting the producer releases the last share of the session, which ends its exchange.
    script.run(
        c"
closing.join()
assert producer.state == nx.EndpointState.CLOSED
del producer, closing
gc.collect()
",
    );
    server.ends(&mut exchange);
}

#[test]
fn a_pycapsule_round_trip_keeps_every_column_kind_at_its_bounds() {
    let pair = ParseAsType::Array {
        element: Box::new(ParseAsType::I64),
        len: NonZeroU32::new(2).assured("two is not zero"),
    };
    let fields = vec![
        field("u8", ParseAsType::U8),
        field("i8", ParseAsType::I8),
        field("u16", ParseAsType::U16),
        field("i16", ParseAsType::I16),
        field("u32", ParseAsType::U32),
        field("i32", ParseAsType::I32),
        field("u64", ParseAsType::U64),
        field("i64", ParseAsType::I64),
        field("f32", ParseAsType::F32),
        field("f64", ParseAsType::F64),
        field("flag", ParseAsType::Bool),
        field("at", ParseAsType::Datetime),
        SchemaField {
            optional: true,
            ..field("text", ParseAsType::String)
        },
        field("raw", ParseAsType::Bytes),
        field("tags", list(ParseAsType::String)),
        field("pair", pair),
        field("nested", list(list(ParseAsType::U8))),
    ];
    let mut retyped = fields.clone();
    retyped[0].ty = ParseAsType::U16;
    let schema = host::Schema::of_fields(fields);
    let retyped = host::Schema::of_fields(retyped);
    run_with(
        c"
import ctypes, struct

builder = nx.BatchBuilder(schema, 2)
fixed = [
    ('B', [2**8 - 1, 0]),
    ('b', [-2**7, 2**7 - 1]),
    ('H', [2**16 - 1, 0]),
    ('h', [-2**15, 2**15 - 1]),
    ('I', [2**32 - 1, 0]),
    ('i', [-2**31, 2**31 - 1]),
    ('Q', [2**64 - 1, 0]),
    ('q', [-2**63, 2**63 - 1]),
    ('f', [-0.0, float('inf')]),
    ('d', [5e-324, float('-inf')]),
    ('?', [True, False]),
    ('q', [2**63 - 1, -2**63]),
]
for column, (code, values) in enumerate(fixed):
    builder.fixed(column, 0, struct.pack(f'={len(values)}{code}', *values))
text = 'h\\u00e9\\u0000'.encode()
builder.states(12, [True, False])
builder.varlen(12, 0, [0, len(text), len(text)], text)
builder.varlen(13, 0, [0, 3, 3], bytes([0, 255, 0x80]))
builder.offsets(14, 0, [0, 2, 2])
builder.varlen(14, 1, [0, 1, 3], b'abc')
builder.fixed(15, 1, struct.pack('=4q', -2**63, 2**63 - 1, 0, -1))
builder.offsets(16, 0, [0, 2, 3])
builder.offsets(16, 1, [0, 1, 1, 3])
builder.fixed(16, 2, bytes([255, 0, 7]))
batch = builder.finish()
columns = len(schema.fields)
expected = [batch.column(index) for index in range(columns)]
assert expected[12] == ['h\\u00e9\\u0000', None], expected[12]
assert expected[14] == [['a', 'bc'], []], expected[14]
assert expected[15] == [[-2**63, 2**63 - 1], [0, -1]], expected[15]
assert expected[16] == [[[255], []], [[0, 7]]], expected[16]

# Every capsule carries the name the interface gives what it holds.
is_valid = ctypes.pythonapi.PyCapsule_IsValid
is_valid.restype = ctypes.c_int
is_valid.argtypes = [ctypes.py_object, ctypes.c_char_p]
assert is_valid(batch.__arrow_c_schema__(), b'arrow_schema')
exported_schema, exported_array = batch.__arrow_c_array__()
assert is_valid(exported_schema, b'arrow_schema')
assert is_valid(exported_array, b'arrow_array')
assert is_valid(batch.__arrow_c_stream__(), b'arrow_array_stream')
del exported_schema, exported_array

class ArrayOnly:
    def __init__(self, data):
        self.data = data
    def __arrow_c_array__(self, requested_schema=None):
        return self.data.__arrow_c_array__(requested_schema)

# Through the stream and through the struct array, the copy is the same batch, down to its
# canonical stream, which keeps the bits of -0.0 apart from 0.0.
for copy in (nx.Batch.from_arrow(schema, batch), nx.Batch.from_arrow(schema, ArrayOnly(batch))):
    assert copy.row_count == 2
    assert [copy.column(index) for index in range(columns)] == expected
    assert bytes(copy.ipc) == bytes(batch.ipc)

# Arrow data of other types is refused before anything reads it as the schema's.
try:
    nx.Batch.from_arrow(retyped, batch)
except nx.InvalidArgumentError:
    pass
else:
    raise AssertionError('a batch of another type was imported')

# A capsule of another name is refused, never read as the structure it does not hold.
class WrongCapsule:
    def __arrow_c_stream__(self, requested_schema=None):
        return batch.__arrow_c_schema__()
try:
    nx.Batch.from_arrow(schema, WrongCapsule())
except ValueError:
    pass
else:
    raise AssertionError('a schema capsule was read as a stream')
",
        |py, globals| {
            globals.set_item(
                "schema",
                Bound::new(py, crate::schema::Schema::new(schema))?,
            )?;
            globals.set_item(
                "retyped",
                Bound::new(py, crate::schema::Schema::new(retyped))?,
            )
        },
    );
}

#[test]
fn an_ended_restoration_and_a_refused_one_say_why() {
    let handle = SubscriptionHandle {
        name: name("live"),
        generation: NonZeroU64::new(3).assured("three is non-zero"),
    };
    let ended = SubscriptionEvent::Ended(nervix_client_core::SubscriptionEnded {
        subscription: handle.clone(),
        reason: nervix_client_core::wire::SubscriptionEndReason::RelayChanged,
        message: "relay 'orders' was redefined".to_string(),
    });
    let refused =
        SubscriptionEvent::RestorationFailed(nervix_client_core::SubscriptionRestorationFailure {
            subscription: handle,
            cause: nervix_client_core::SubscriptionRestorationCause::InTransaction,
            message: "the session holds a transaction".to_string(),
            retry_after: std::time::Duration::from_secs(2),
        });
    let clock_refused = nervix_client_core::DomainClockEvent::RestorationFailed(
        nervix_client_core::DomainClockRestorationFailure {
            domain: name("simulation"),
            cause: nervix_client_core::DomainClockRestorationCause::Rejected(
                nervix_client_core::wire::RequestRejection::TooManyRequestsInFlight,
            ),
            message: "the session already has 64 requests in flight".to_string(),
            retry_after: std::time::Duration::from_secs(1),
        },
    );
    run_with(
        c"
assert ended.kind == nx.EventKind.ENDED
assert ended.end_reason == nx.EndReason.RELAY_CHANGED
assert ended.restoration_failure is None

assert refused.kind == nx.EventKind.RESTORATION_FAILED
failure = refused.restoration_failure
assert failure.cause == nx.RestorationCause.IN_TRANSACTION
assert failure.message == 'the session holds a transaction'
assert failure.retry_after_nanos == 2_000_000_000
try:
    refused.end_reason
except nx.TypeMismatchError:
    pass
else:
    raise AssertionError('a restoration failure carries no end reason')

assert clock_refused.kind == nx.ClockEventKind.RESTORATION_FAILED
assert clock_refused.restoration_failure.cause == nx.RestorationCause.REJECTED
",
        |py, globals| {
            let ended = host::Event::new(ended).assured("an end carries no rows");
            let refused = host::Event::new(refused).assured("a refusal carries no rows");
            globals.set_item("ended", Bound::new(py, crate::event::Event::new(ended))?)?;
            globals.set_item(
                "refused",
                Bound::new(py, crate::event::Event::new(refused))?,
            )?;
            globals.set_item(
                "clock_refused",
                Bound::new(
                    py,
                    crate::clock::ClockEvent::new(host::ClockEvent::new(clock_refused)),
                )?,
            )
        },
    );
}
