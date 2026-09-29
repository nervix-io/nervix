//! Domain clock events through the C ABI, as a host calls it.
//!
//! Events the Rust client delivers are read kind by kind, so every accessor is held to the kinds
//! that carry its field. A session connected to an in-process session server reads its state, tick
//! and end events from real frames, through an interruption and the attachment the client restores,
//! and bounds a wait that nothing ends by cancellation and by deadline.

use std::{
    convert::Infallible,
    net::SocketAddr,
    ptr, slice,
    task::{Context, Poll},
    thread,
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::{
    DomainClockAttachDisposition, DomainClockAttachOutcome, DomainClockAttachmentEndReason,
    DomainClockAttachmentEnded, DomainClockEvent, DomainClockInterruption, DomainClockObservation,
    DomainClockObserved, DomainClockObservedState, DomainClockRestorationFailure,
    DomainClockTickObservation, DomainClockTicked, DomainName, PacedDomainClock, Timestamp,
    wire::{
        ClientFrame, ClientMessage, ClientRequest, EncodedFrame, Reply, ReplyBody, ReplyDelivery,
        ServerFrame, SessionLimits, VerifiedFrame,
        grpc::{EXCHANGE_PATH, SERVICE_NAME, ServerExchangeCodec},
    },
};
use nervix_models::{DomainClockPeriod, DomainClockSkew, DomainClockState, DomainTimeRate};
use tokio::{net::TcpListener, runtime::Runtime, sync::mpsc};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{
    Request, Response, Status, Streaming,
    body::Body,
    codegen::{BoxFuture, Service, http},
    server::{Grpc, NamedService, StreamingService},
    transport::Server,
};

use super::{failure_kind, succeeded};
use crate::{
    Cancel, ClockEndReason, ClockEvent, ClockEventKind, ClockState, Disposition, FailureKind,
    Session, nx_cancel_free, nx_cancel_new, nx_cancel_trigger, nx_cancel_with_deadline,
    nx_clock_event_domain, nx_clock_event_end_reason, nx_clock_event_generation,
    nx_clock_event_kind_of, nx_clock_event_paced, nx_clock_event_release, nx_clock_event_retain,
    nx_clock_event_state, nx_clock_event_tick, nx_execution_free, nx_outcome_disposition,
    nx_outcome_free, nx_session_connect, nx_session_execute, nx_session_free,
    nx_session_next_clock_event, nx_session_prepare,
};

/// Every wait in these tests ends when its condition holds, so the bound only has to be generous.
const DEADLINE: Duration = Duration::from_secs(30);

/// The same bound, as the milliseconds a binding token takes.
const DEADLINE_MILLIS: u64 = 30_000;

fn domain(name: &str) -> DomainName {
    DomainName::parse(name).assured("the test domain name is valid")
}

/// A paced clock whose every field differs from the others, so a swapped field shows.
fn paced() -> PacedDomainClock {
    PacedDomainClock {
        period: DomainClockPeriod::try_from(Duration::from_millis(100))
            .assured("one hundred milliseconds is a valid period"),
        skew: DomainClockSkew::try_from(Duration::from_millis(10))
            .assured("ten milliseconds is a valid skew"),
        mapping: DomainClockState::new(
            Timestamp::from_unix_nanos(1_500_000_001),
            Timestamp::from_unix_nanos(-2_000_000_003),
            DomainTimeRate::try_from(2.5).assured("the fixture rate is positive and finite"),
        ),
    }
}

fn observed(generation: u64, state: DomainClockObservedState) -> DomainClockEvent {
    DomainClockEvent::Observed(DomainClockObserved {
        domain: domain("sim"),
        clock: DomainClockObservation { generation, state },
    })
}

/// The third tick of the paced clock's generation, whose every instant differs from the others.
fn third_tick(generation: u64) -> DomainClockTicked {
    DomainClockTicked {
        domain: domain("sim"),
        tick: DomainClockTickObservation {
            generation,
            tick_id: 3,
            logical_boundary: Timestamp::from_unix_nanos(-1_800_000_003),
            authority_utc: Timestamp::from_unix_nanos(1_540_000_001),
            serving_logical: Timestamp::from_unix_nanos(-1_799_999_000),
        },
    }
}

/// A clock event handed out as a host's first reference, released when dropped.
struct SharedClock(*mut ClockEvent);

impl SharedClock {
    fn new(event: DomainClockEvent) -> Self {
        Self(ClockEvent::new(event).into_shared())
    }

    fn kind(&self) -> ClockEventKind {
        // SAFETY: the reference is live.
        unsafe { nx_clock_event_kind_of(self.0) }
    }

    fn domain(&self) -> String {
        let mut name = ptr::null();
        let mut name_len = 0;
        // SAFETY: the reference is live, the out-parameters are writable, and the name is copied
        // before the reference can be released.
        unsafe {
            nx_clock_event_domain(self.0, &mut name, &mut name_len);
            String::from_utf8_lossy(slice::from_raw_parts(name, name_len)).into_owned()
        }
    }

    fn generation(&self) -> u64 {
        let mut generation = 0;
        // SAFETY: the reference is live and `generation` is writable.
        succeeded(unsafe { nx_clock_event_generation(self.0, &mut generation) });
        generation
    }

    fn state(&self) -> ClockState {
        let mut state = ClockState::Stopped;
        // SAFETY: the reference is live and `state` is writable.
        succeeded(unsafe { nx_clock_event_state(self.0, &mut state) });
        state
    }

    fn end_reason(&self) -> ClockEndReason {
        let mut reason = ClockEndReason::DomainRemoved;
        // SAFETY: the reference is live and `reason` is writable.
        succeeded(unsafe { nx_clock_event_end_reason(self.0, &mut reason) });
        reason
    }

    /// The failures of reading every field of `absent` from an event that does not carry it.
    fn refuses(&self, absent: &[Field]) {
        for field in absent {
            let mut generation = 0;
            let mut state = ClockState::Stopped;
            let mut period = 0;
            let mut tick_id = 0;
            let mut reason = ClockEndReason::DomainRemoved;
            // SAFETY: the reference is live and every out-parameter is writable.
            let failure = unsafe {
                match field {
                    Field::Generation => nx_clock_event_generation(self.0, &mut generation),
                    Field::State => nx_clock_event_state(self.0, &mut state),
                    Field::Paced => nx_clock_event_paced(
                        self.0,
                        &mut period,
                        ptr::null_mut(),
                        ptr::null_mut(),
                        ptr::null_mut(),
                        ptr::null_mut(),
                    ),
                    Field::Tick => nx_clock_event_tick(
                        self.0,
                        &mut tick_id,
                        ptr::null_mut(),
                        ptr::null_mut(),
                        ptr::null_mut(),
                    ),
                    Field::EndReason => nx_clock_event_end_reason(self.0, &mut reason),
                }
            };
            assert_eq!(
                failure_kind(failure),
                FailureKind::Type,
                "a {:?} event carries no {field:?}",
                self.kind()
            );
        }
    }
}

/// A field an event of some kinds carries.
#[derive(Debug, Clone, Copy)]
enum Field {
    Generation,
    State,
    Paced,
    Tick,
    EndReason,
}

// SAFETY: a clock event is immutable and its references are counted atomically, so a reference may
// be released on any thread, which is what the header promises.
unsafe impl Send for SharedClock {}

impl Drop for SharedClock {
    fn drop(&mut self) {
        // SAFETY: the reference is live and released once, here.
        unsafe { nx_clock_event_release(self.0) };
    }
}

/// The committed mapping a paced state event reports, as the header's out-parameters carry it.
#[derive(Debug, PartialEq)]
struct PacedFields {
    period_nanos: u64,
    skew_nanos: u64,
    logical_origin: i64,
    utc_anchor: i64,
    time_rate: f64,
}

/// The progress a tick event reports, as the header's out-parameters carry it.
#[derive(Debug, PartialEq)]
struct TickFields {
    tick_id: u64,
    logical_boundary: i64,
    authority_utc: i64,
    serving_logical: i64,
}

fn tick_fields(event: &SharedClock) -> TickFields {
    let mut fields = TickFields {
        tick_id: 0,
        logical_boundary: 0,
        authority_utc: 0,
        serving_logical: 0,
    };
    // SAFETY: the reference is live and every out-parameter is writable.
    succeeded(unsafe {
        nx_clock_event_tick(
            event.0,
            &mut fields.tick_id,
            &mut fields.logical_boundary,
            &mut fields.authority_utc,
            &mut fields.serving_logical,
        )
    });
    fields
}

fn paced_fields(event: &SharedClock) -> PacedFields {
    let mut fields = PacedFields {
        period_nanos: 0,
        skew_nanos: 0,
        logical_origin: 0,
        utc_anchor: 0,
        time_rate: 0.0,
    };
    // SAFETY: the reference is live and every out-parameter is writable.
    succeeded(unsafe {
        nx_clock_event_paced(
            event.0,
            &mut fields.period_nanos,
            &mut fields.skew_nanos,
            &mut fields.logical_origin,
            &mut fields.utc_anchor,
            &mut fields.time_rate,
        )
    });
    fields
}

#[test]
fn a_state_event_reports_its_generation_state_and_committed_mapping() {
    let event = SharedClock::new(observed(3, DomainClockObservedState::Paced(paced())));
    assert_eq!(event.kind(), ClockEventKind::State);
    assert_eq!(event.domain(), "sim");
    assert_eq!(event.generation(), 3);
    assert_eq!(event.state(), ClockState::Paced);
    assert_eq!(
        paced_fields(&event),
        PacedFields {
            period_nanos: 100_000_000,
            skew_nanos: 10_000_000,
            logical_origin: -2_000_000_003,
            utc_anchor: 1_500_000_001,
            time_rate: 2.5,
        }
    );
    let mut rate = 0.0;
    // SAFETY: the reference is live, and a host may pass null for the fields it does not read.
    succeeded(unsafe {
        nx_clock_event_paced(
            event.0,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut rate,
        )
    });
    assert_eq!(rate, 2.5);
    event.refuses(&[Field::Tick, Field::EndReason]);

    let unpaced_states = [
        (DomainClockObservedState::Stopped, ClockState::Stopped),
        (
            DomainClockObservedState::Uninstalled,
            ClockState::Uninstalled,
        ),
        (DomainClockObservedState::Unpaced, ClockState::Unpaced),
    ];
    for (state, expected) in unpaced_states {
        let event = SharedClock::new(observed(1, state));
        assert_eq!(event.kind(), ClockEventKind::State);
        assert_eq!(event.generation(), 1);
        assert_eq!(event.state(), expected);
        event.refuses(&[Field::Paced, Field::Tick, Field::EndReason]);
    }
}

#[test]
fn a_tick_event_reports_its_generation_and_progress() {
    let event = SharedClock::new(DomainClockEvent::Ticked(third_tick(4)));
    assert_eq!(event.kind(), ClockEventKind::Tick);
    assert_eq!(event.domain(), "sim");
    assert_eq!(event.generation(), 4);
    assert_eq!(
        tick_fields(&event),
        TickFields {
            tick_id: 3,
            logical_boundary: -1_800_000_003,
            authority_utc: 1_540_000_001,
            serving_logical: -1_799_999_000,
        }
    );
    let mut serving_logical = 0;
    // SAFETY: the reference is live, and a host may pass null for the fields it does not read.
    succeeded(unsafe {
        nx_clock_event_tick(
            event.0,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut serving_logical,
        )
    });
    assert_eq!(serving_logical, -1_799_999_000);
    event.refuses(&[Field::State, Field::Paced, Field::EndReason]);
}

#[test]
fn end_and_interruption_events_name_their_domain_and_carry_no_clock() {
    let ended = SharedClock::new(DomainClockEvent::Ended(DomainClockAttachmentEnded {
        domain: domain("gone"),
        reason: DomainClockAttachmentEndReason::DomainRemoved,
    }));
    assert_eq!(ended.kind(), ClockEventKind::Ended);
    assert_eq!(ended.domain(), "gone");
    assert_eq!(ended.end_reason(), ClockEndReason::DomainRemoved);
    ended.refuses(&[Field::Generation, Field::State, Field::Paced, Field::Tick]);

    let interrupted = SharedClock::new(DomainClockEvent::Interrupted(DomainClockInterruption {
        domain: domain("sim"),
    }));
    assert_eq!(interrupted.kind(), ClockEventKind::Interrupted);
    assert_eq!(interrupted.domain(), "sim");
    interrupted.refuses(&[
        Field::Generation,
        Field::State,
        Field::Paced,
        Field::Tick,
        Field::EndReason,
    ]);
}

#[test]
fn a_restoration_failure_names_its_domain_and_carries_no_clock() {
    let failed = SharedClock::new(DomainClockEvent::RestorationFailed(
        DomainClockRestorationFailure {
            domain: domain("sim"),
            message: "the session holds a transaction".to_string(),
            retry_after: Duration::from_secs(2),
        },
    ));
    assert_eq!(failed.kind(), ClockEventKind::RestorationFailed);
    assert_eq!(failed.domain(), "sim");
    failed.refuses(&[
        Field::Generation,
        Field::State,
        Field::Paced,
        Field::Tick,
        Field::EndReason,
    ]);
}

#[test]
fn a_clock_accessor_refuses_a_missing_event_or_result() {
    let event = SharedClock::new(observed(1, DomainClockObservedState::Unpaced));
    let mut generation = 0;
    let mut state = ClockState::Stopped;
    let mut reason = ClockEndReason::DomainRemoved;
    // SAFETY: every non-null pointer is live or writable; the null ones are what is refused.
    unsafe {
        assert_eq!(
            failure_kind(nx_clock_event_generation(event.0, ptr::null_mut())),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_clock_event_state(event.0, ptr::null_mut())),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_clock_event_end_reason(event.0, ptr::null_mut())),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_clock_event_generation(ptr::null(), &mut generation)),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_clock_event_state(ptr::null(), &mut state)),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_clock_event_end_reason(ptr::null(), &mut reason)),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_clock_event_paced(
                ptr::null(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_clock_event_tick(
                ptr::null(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
            )),
            FailureKind::InvalidArgument
        );
    }
}

#[test]
fn a_retained_clock_event_outlives_a_reference_released_on_another_thread() {
    let event = SharedClock::new(observed(2, DomainClockObservedState::Paced(paced())));
    // SAFETY: the reference is live, and the binding returns a new one.
    let second = SharedClock(unsafe { nx_clock_event_retain(event.0) });
    assert_eq!(
        second.0, event.0,
        "a retained reference addresses the same event"
    );
    thread::spawn(move || drop(second))
        .join()
        .assured("releasing on another thread does not panic");
    assert_eq!(event.domain(), "sim");
    assert_eq!(event.generation(), 2);
    assert_eq!(paced_fields(&event).time_rate, 2.5);
    // SAFETY: releasing null is allowed and does nothing.
    unsafe { nx_clock_event_release(ptr::null_mut()) };
}

/// The server's side of one exchange a session opened.
struct ServerExchange {
    requests: Streaming<VerifiedFrame<ClientFrame>>,
    frames: mpsc::Sender<Result<EncodedFrame<ServerFrame>, Status>>,
}

impl ServerExchange {
    async fn send(&self, frame: EncodedFrame<ServerFrame>) {
        self.frames
            .send(Ok(frame))
            .await
            .assured("the session keeps reading its exchange");
    }

    /// Reads the request attaching the session to the clock of `sim` and attaches it to `clock`.
    async fn attach(&mut self, clock: DomainClockObservation) {
        let frame = tokio::time::timeout(DEADLINE, self.requests.message())
            .await
            .assured("the session sends its request within the test deadline")
            .assured("the exchange stays open")
            .assured("the session sends a request");
        let request = ClientMessage::decode(&frame).assured("a request the client encoded decodes");
        let ClientRequest::AttachDomainClock(attach) = request.request else {
            panic!("the session sends an attach request");
        };
        assert_eq!(attach.domain, domain("sim"));
        let reply = Reply {
            request_id: request.request_id,
            body: ReplyBody::DomainClockAttach(DomainClockAttachOutcome {
                disposition: DomainClockAttachDisposition::Attached {
                    domain: attach.domain,
                    clock,
                },
                message: String::new(),
            }),
        };
        let delivery = reply
            .encode(&SessionLimits::DEFAULT)
            .assured("an attach reply fits the default limits");
        let ReplyDelivery::Frame(frame) = delivery else {
            panic!("an attach reply fits one frame");
        };
        self.send(frame).await;
    }
}

/// Serves the session exchange and hands every exchange a session opens to the test.
#[derive(Clone)]
struct SessionService {
    exchanges: mpsc::Sender<ServerExchange>,
}

impl NamedService for SessionService {
    const NAME: &'static str = SERVICE_NAME;
}

impl Service<http::Request<Body>> for SessionService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let exchanges = self.exchanges.clone();
        Box::pin(async move {
            if request.uri().path() != EXCHANGE_PATH {
                return Ok(Status::unimplemented("the test serves the exchange only").into_http());
            }
            let mut grpc = Grpc::new(ServerExchangeCodec::new(SessionLimits::DEFAULT));
            Ok(grpc.streaming(OpenExchange { exchanges }, request).await)
        })
    }
}

struct OpenExchange {
    exchanges: mpsc::Sender<ServerExchange>,
}

impl StreamingService<VerifiedFrame<ClientFrame>> for OpenExchange {
    type Response = EncodedFrame<ServerFrame>;
    type ResponseStream = ReceiverStream<Result<EncodedFrame<ServerFrame>, Status>>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

    fn call(&mut self, request: Request<Streaming<VerifiedFrame<ClientFrame>>>) -> Self::Future {
        let exchanges = self.exchanges.clone();
        Box::pin(async move {
            let (frames, outbound) = mpsc::channel(8);
            let exchange = ServerExchange {
                requests: request.into_inner(),
                frames,
            };
            if exchanges.send(exchange).await.is_err() {
                return Err(Status::unavailable("the test takes no more exchanges"));
            }
            Ok(Response::new(ReceiverStream::new(outbound)))
        })
    }
}

/// An in-process session server, run by its own runtime while the test thread blocks in the
/// binding.
struct TestServer {
    runtime: Runtime,
    address: SocketAddr,
    exchanges: mpsc::Receiver<ServerExchange>,
}

impl TestServer {
    fn start() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .assured("a test runtime starts");
        let (sender, exchanges) = mpsc::channel(4);
        let address = runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .assured("the loopback interface accepts a listener");
            let address = listener
                .local_addr()
                .assured("a bound listener has an address");
            let service = SessionService { exchanges: sender };
            tokio::spawn(async move {
                Server::builder()
                    .add_service(service)
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .assured("the test server serves until its runtime shuts down");
            });
            address
        });
        Self {
            runtime,
            address,
            exchanges,
        }
    }

    fn next_exchange(&mut self) -> ServerExchange {
        let exchanges = &mut self.exchanges;
        self.runtime.block_on(async {
            tokio::time::timeout(DEADLINE, exchanges.recv())
                .await
                .assured("the session opens an exchange within the test deadline")
                .assured("the server keeps handing exchanges to the test")
        })
    }
}

/// A session shared with the threads that block in it, as the binding allows.
#[derive(Clone, Copy)]
struct SharedSession(*mut Session);

// SAFETY: the binding allows a session to be used from several threads at once.
unsafe impl Send for SharedSession {}

impl SharedSession {
    fn connect(address: SocketAddr) -> Self {
        let server = format!("http://{address}");
        let domain = "sim";
        let mut session = ptr::null_mut();
        // SAFETY: every text argument addresses its length in readable bytes, and `session` is
        // writable.
        succeeded(unsafe {
            nx_session_connect(
                server.as_ptr(),
                server.len(),
                domain.as_ptr(),
                domain.len(),
                ptr::null(),
                0,
                ptr::null(),
                0,
                ptr::null(),
                &mut session,
            )
        });
        Self(session)
    }

    /// Runs one statement and returns its disposition.
    fn execute(self, query: &str) -> Disposition {
        let mut execution = ptr::null_mut();
        let mut outcome = ptr::null_mut();
        // SAFETY: the session is live, the query addresses its length, the out-parameters are
        // writable, and the execution and the outcome are released once each.
        unsafe {
            succeeded(nx_session_prepare(
                self.0,
                query.as_ptr(),
                query.len(),
                ptr::null(),
                &mut execution,
            ));
            succeeded(nx_session_execute(
                self.0,
                execution,
                ptr::null(),
                &mut outcome,
            ));
            nx_execution_free(execution);
            let disposition = nx_outcome_disposition(outcome);
            nx_outcome_free(outcome);
            disposition
        }
    }

    /// The next clock event, which must arrive within the test deadline.
    fn next_clock_event(self) -> SharedClock {
        let mut deadline = ptr::null_mut();
        let mut event = ptr::null_mut();
        // SAFETY: the session and the token are live, the out-parameters are writable, and the
        // token is freed once no call waits on it.
        unsafe {
            succeeded(nx_cancel_with_deadline(DEADLINE_MILLIS, &mut deadline));
            let failure = nx_session_next_clock_event(self.0, deadline, &mut event);
            nx_cancel_free(deadline);
            succeeded(failure);
        }
        SharedClock(event)
    }

    /// Waits for a clock event that `token` has to end, returning the failure's kind.
    fn wait_with(self, token: SharedToken) -> FailureKind {
        let mut event = ptr::null_mut();
        // SAFETY: the session and the token outlive the wait, and `event` is writable.
        failure_kind(unsafe { nx_session_next_clock_event(self.0, token.0, &mut event) })
    }

    /// Waits with a token that ends the wait after `millis`, returning the failure's kind.
    fn wait_expiring_after(self, millis: u64) -> FailureKind {
        let mut deadline = ptr::null_mut();
        // SAFETY: `deadline` is writable.
        succeeded(unsafe { nx_cancel_with_deadline(millis, &mut deadline) });
        let kind = self.wait_with(SharedToken(deadline));
        // SAFETY: the token is live and no call waits on it any more.
        unsafe { nx_cancel_free(deadline) };
        kind
    }

    fn free(self) {
        // SAFETY: the session is live, no thread uses it any more, and it is freed once, here.
        unsafe { nx_session_free(self.0) };
    }
}

/// A token shared with the thread that waits on it.
#[derive(Clone, Copy)]
struct SharedToken(*mut Cancel);

// SAFETY: a token may be triggered and waited on from any thread, which is what the header
// promises.
unsafe impl Send for SharedToken {}

#[test]
fn a_session_reads_every_clock_event_kind_and_bounds_its_wait() {
    let mut server = TestServer::start();
    let session = SharedSession::connect(server.address);
    let mut first = server.next_exchange();

    // A session that follows no clock waits until it attaches to one, so only its token ends the
    // wait: here from another thread, and then by its deadline.
    let token = SharedToken(nx_cancel_new());
    let waiter = thread::spawn(move || session.wait_with(token));
    thread::sleep(Duration::from_millis(100));
    // SAFETY: the token is live; it is freed once the waiter has returned.
    unsafe { nx_cancel_trigger(token.0) };
    let cancelled = waiter.join().assured("the waiting thread returns");
    // SAFETY: no call waits on the token any more.
    unsafe { nx_cancel_free(token.0) };
    assert_eq!(cancelled, FailureKind::Cancelled);
    assert_eq!(session.wait_expiring_after(50), FailureKind::Deadline);

    let attaching = thread::spawn(move || session.execute("ATTACH DOMAIN CLOCK;"));
    server
        .runtime
        .block_on(first.attach(DomainClockObservation {
            generation: 0,
            state: DomainClockObservedState::Stopped,
        }));
    assert_eq!(
        attaching.join().assured("the attaching thread returns"),
        Disposition::Completed
    );

    let started = DomainClockObserved {
        domain: domain("sim"),
        clock: DomainClockObservation {
            generation: 1,
            state: DomainClockObservedState::Paced(paced()),
        },
    }
    .encode(&SessionLimits::DEFAULT)
    .assured("a clock frame fits the default limits");
    server.runtime.block_on(first.send(started));
    let state = session.next_clock_event();
    assert_eq!(state.kind(), ClockEventKind::State);
    assert_eq!(state.domain(), "sim");
    assert_eq!(state.generation(), 1);
    assert_eq!(state.state(), ClockState::Paced);
    assert_eq!(paced_fields(&state).period_nanos, 100_000_000);

    let ticked = third_tick(1)
        .encode(&SessionLimits::DEFAULT)
        .assured("a tick frame fits the default limits");
    server.runtime.block_on(first.send(ticked));
    let tick = session.next_clock_event();
    assert_eq!(tick.kind(), ClockEventKind::Tick);
    assert_eq!(tick.domain(), "sim");
    assert_eq!(tick.generation(), 1);
    assert_eq!(tick_fields(&tick).tick_id, 3);

    // The exchange ends: the session reports the gap, then reopens and attaches the clock again.
    drop(first);
    let interrupted = session.next_clock_event();
    assert_eq!(interrupted.kind(), ClockEventKind::Interrupted);
    assert_eq!(interrupted.domain(), "sim");
    let reading = thread::spawn(move || session.next_clock_event());
    let mut second = server.next_exchange();
    server
        .runtime
        .block_on(second.attach(DomainClockObservation {
            generation: 2,
            state: DomainClockObservedState::Unpaced,
        }));
    let restored = reading.join().assured("the reading thread returns");
    assert_eq!(restored.kind(), ClockEventKind::State);
    assert_eq!(restored.generation(), 2);
    assert_eq!(restored.state(), ClockState::Unpaced);

    let ended = DomainClockAttachmentEnded {
        domain: domain("sim"),
        reason: DomainClockAttachmentEndReason::DomainRemoved,
    }
    .encode(&SessionLimits::DEFAULT)
    .assured("an end frame fits the default limits");
    server.runtime.block_on(second.send(ended));
    let ended = session.next_clock_event();
    assert_eq!(ended.kind(), ClockEventKind::Ended);
    assert_eq!(ended.end_reason(), ClockEndReason::DomainRemoved);

    // The end released the attachment, so nothing is left to wait for.
    assert_eq!(session.wait_expiring_after(50), FailureKind::Deadline);
    // Events read from a session stay valid after it ends.
    session.free();
    assert_eq!(state.domain(), "sim");
    assert_eq!(tick_fields(&tick).serving_logical, -1_799_999_000);
    assert_eq!(ended.domain(), "sim");
}
