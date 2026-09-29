//! The clock of a followed domain through the C ABI, as a host calls it.
//!
//! A session connected to an in-process session server attaches to clocks in every state and reads
//! them back through `nx_session_domain_clock`: the clock an attach reported before any tick, the
//! clock every later frame reports once its event has been read, a refusal that leaves the session
//! following what it followed, several domains, a detach, an end and an interruption. The
//! projections are held to the arithmetic the ingestor admits by, with exact values.

use std::{ptr, slice};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_core::{
    DomainClockAttachDisposition, DomainClockAttachmentEndReason, DomainClockAttachmentEnded,
    DomainClockObservation, DomainClockObserved, DomainClockObservedState, wire::SessionLimits,
};
use nervix_primitives::thread;

use super::{
    clock_events::{
        PacedFields, ServerExchange, SharedSession, TestServer, TickFields, domain, paced,
        third_tick, tick_fields,
    },
    failure_kind, succeeded,
};
use crate::{
    Cancel, ClockEventKind, ClockState, Disposition, DomainClock, Execution, FailureKind,
    nx_cancel_free, nx_cancel_new, nx_cancel_trigger, nx_domain_clock_admission_window,
    nx_domain_clock_admits, nx_domain_clock_domain, nx_domain_clock_generation,
    nx_domain_clock_logical_time_at, nx_domain_clock_paced, nx_domain_clock_release,
    nx_domain_clock_retain, nx_domain_clock_state, nx_domain_clock_tick,
    nx_domain_clock_wall_duration_until, nx_execution_free, nx_outcome_disposition,
    nx_outcome_free, nx_session_domain_clock, nx_session_execute, nx_session_prepare,
};

/// The UTC anchor of the paced fixture clock.
const ANCHOR: i64 = 1_500_000_001;

/// The logical origin of the paced fixture clock.
const ORIGIN: i64 = -2_000_000_003;

/// One reference to a domain clock a host read, released when dropped.
struct SharedDomainClock(*mut DomainClock);

// SAFETY: a domain clock is immutable and its references are counted atomically, so a reference
// may be released on any thread, which is what the header promises.
unsafe impl Send for SharedDomainClock {}

impl Drop for SharedDomainClock {
    fn drop(&mut self) {
        // SAFETY: the reference is live and released once, here.
        unsafe { nx_domain_clock_release(self.0) };
    }
}

/// The admission window a projection reports.
#[derive(Debug, PartialEq)]
struct Window {
    earliest_center: i64,
    latest_center: i64,
}

impl SharedDomainClock {
    fn retain(&self) -> Self {
        // SAFETY: the reference is live, and the binding returns a new one.
        Self(unsafe { nx_domain_clock_retain(self.0) })
    }

    fn domain(&self) -> String {
        let mut name = ptr::null();
        let mut name_len = 0;
        // SAFETY: the reference is live, the out-parameters are writable, and the name is copied
        // before the reference can be released.
        unsafe {
            nx_domain_clock_domain(self.0, &mut name, &mut name_len);
            String::from_utf8_lossy(slice::from_raw_parts(name, name_len)).into_owned()
        }
    }

    fn generation(&self) -> u64 {
        // SAFETY: the reference is live.
        unsafe { nx_domain_clock_generation(self.0) }
    }

    fn state(&self) -> ClockState {
        // SAFETY: the reference is live.
        unsafe { nx_domain_clock_state(self.0) }
    }

    fn paced(&self) -> Result<PacedFields, FailureKind> {
        let mut fields = PacedFields {
            period_nanos: 0,
            skew_nanos: 0,
            logical_origin: 0,
            utc_anchor: 0,
            time_rate: 0.0,
        };
        // SAFETY: the reference is live and every out-parameter is writable.
        let failure = unsafe {
            nx_domain_clock_paced(
                self.0,
                &mut fields.period_nanos,
                &mut fields.skew_nanos,
                &mut fields.logical_origin,
                &mut fields.utc_anchor,
                &mut fields.time_rate,
            )
        };
        if failure.is_null() {
            return Ok(fields);
        }
        Err(failure_kind(failure))
    }

    fn tick(&self) -> Option<TickFields> {
        let mut fields = TickFields {
            tick_id: 0,
            logical_boundary: 0,
            authority_utc: 0,
            serving_logical: 0,
        };
        // SAFETY: the reference is live and every out-parameter is writable.
        let held = unsafe {
            nx_domain_clock_tick(
                self.0,
                &mut fields.tick_id,
                &mut fields.logical_boundary,
                &mut fields.authority_utc,
                &mut fields.serving_logical,
            )
        };
        if !held {
            return None;
        }
        Some(fields)
    }

    fn logical_time_at(&self, utc: i64) -> Result<i64, FailureKind> {
        let mut logical = 0;
        // SAFETY: the reference is live and `logical` is writable.
        let failure = unsafe { nx_domain_clock_logical_time_at(self.0, utc, &mut logical) };
        if failure.is_null() {
            return Ok(logical);
        }
        Err(failure_kind(failure))
    }

    fn wall_duration_until(&self, utc: i64, target: i64) -> Result<u64, FailureKind> {
        let mut wait = 0;
        // SAFETY: the reference is live and `wait` is writable.
        let failure =
            unsafe { nx_domain_clock_wall_duration_until(self.0, utc, target, &mut wait) };
        if failure.is_null() {
            return Ok(wait);
        }
        Err(failure_kind(failure))
    }

    fn admission_window(&self, utc: i64) -> Result<Option<Window>, FailureKind> {
        let mut has_window = false;
        let mut window = Window {
            earliest_center: 0,
            latest_center: 0,
        };
        // SAFETY: the reference is live and every out-parameter is writable.
        let failure = unsafe {
            nx_domain_clock_admission_window(
                self.0,
                utc,
                &mut has_window,
                &mut window.earliest_center,
                &mut window.latest_center,
            )
        };
        if !failure.is_null() {
            return Err(failure_kind(failure));
        }
        if !has_window {
            return Ok(None);
        }
        Ok(Some(window))
    }

    fn admits(&self, utc: i64, event: i64) -> Result<bool, FailureKind> {
        let mut admitted = false;
        // SAFETY: the reference is live and `admitted` is writable.
        let failure = unsafe { nx_domain_clock_admits(self.0, utc, event, &mut admitted) };
        if failure.is_null() {
            return Ok(admitted);
        }
        Err(failure_kind(failure))
    }

    /// Holds every projection of a clock that has no logical time to its refusal.
    fn refuses_every_projection(&self) {
        assert_eq!(self.logical_time_at(0), Err(FailureKind::Type));
        assert_eq!(self.wall_duration_until(0, 1), Err(FailureKind::Type));
        assert_eq!(self.admission_window(0), Err(FailureKind::Type));
        assert_eq!(self.admits(0, 0), Err(FailureKind::Type));
    }
}

impl SharedSession {
    /// The clock the session holds for `name`, or `None` when it follows none.
    fn domain_clock(self, name: &str) -> Option<SharedDomainClock> {
        let mut clock = ptr::null_mut();
        // SAFETY: the session is live, the name addresses its length, and `clock` is writable.
        succeeded(unsafe {
            nx_session_domain_clock(self.0, name.as_ptr(), name.len(), &mut clock)
        });
        if clock.is_null() {
            return None;
        }
        Some(SharedDomainClock(clock))
    }

    /// The clock the session holds for `name`, which it must follow.
    fn followed_clock(self, name: &str) -> SharedDomainClock {
        self.domain_clock(name)
            .assured("the session follows the clock the test attached it to")
    }
}

/// One prepared command and a token that can cancel it, shared with the threads that run it, as the
/// binding allows.
#[derive(Clone, Copy)]
struct PreparedCommand {
    session: SharedSession,
    execution: *mut Execution,
    cancel: *mut Cancel,
}

// SAFETY: a session may be used, an execution run and a token triggered from any thread, which is
// what the header promises.
unsafe impl Send for PreparedCommand {}

impl PreparedCommand {
    fn prepare(session: SharedSession, query: &str) -> Self {
        let mut execution = ptr::null_mut();
        // SAFETY: the session is live, the query addresses its length, and `execution` is writable.
        succeeded(unsafe {
            nx_session_prepare(
                session.0,
                query.as_ptr(),
                query.len(),
                ptr::null(),
                &mut execution,
            )
        });
        Self {
            session,
            execution,
            cancel: nx_cancel_new(),
        }
    }

    /// Runs the command bounded by its token, returning its disposition or its failure's kind.
    fn run_cancellable(self) -> Result<Disposition, FailureKind> {
        self.run(self.cancel.cast_const())
    }

    /// Runs the command without a token.
    fn run_to_completion(self) -> Result<Disposition, FailureKind> {
        self.run(ptr::null())
    }

    fn run(self, cancel: *const Cancel) -> Result<Disposition, FailureKind> {
        let mut outcome = ptr::null_mut();
        // SAFETY: the session and the execution are live, the token is live or null, and `outcome`
        // is writable.
        let failure =
            unsafe { nx_session_execute(self.session.0, self.execution, cancel, &mut outcome) };
        if !failure.is_null() {
            return Err(failure_kind(failure));
        }
        // SAFETY: the outcome is live and released once, here.
        unsafe {
            let disposition = nx_outcome_disposition(outcome);
            nx_outcome_free(outcome);
            Ok(disposition)
        }
    }

    fn trigger(self) {
        // SAFETY: the token is live.
        unsafe { nx_cancel_trigger(self.cancel) };
    }

    fn free(self) {
        // SAFETY: the execution and the token are live, no call uses them any more, and each is
        // freed once, here.
        unsafe {
            nx_execution_free(self.execution);
            nx_cancel_free(self.cancel);
        }
    }
}

fn observation(generation: u64, state: DomainClockObservedState) -> DomainClockObservation {
    DomainClockObservation { generation, state }
}

/// A connected session and the exchange it opened.
fn connected(server: &mut TestServer) -> (SharedSession, ServerExchange) {
    let session = SharedSession::connect(server.address);
    let exchange = server.next_exchange();
    (session, exchange)
}

/// Runs `query` on another thread while `answer` serves the requests it sends, and returns the
/// command's disposition.
fn execute_answered(
    server: &TestServer,
    session: SharedSession,
    query: &'static str,
    answer: impl std::future::Future<Output = ()>,
) -> Disposition {
    let executing = thread::spawn(move || session.execute(query));
    server.runtime.block_on(answer);
    executing.join().assured("the executing thread returns")
}

/// Sends a clock frame reporting `clock` for the domain `name`.
fn observe(
    server: &TestServer,
    exchange: &ServerExchange,
    name: &str,
    clock: DomainClockObservation,
) {
    let frame = DomainClockObserved {
        domain: domain(name),
        clock,
    }
    .encode(&SessionLimits::DEFAULT)
    .assured("a clock frame fits the default limits");
    server.runtime.block_on(exchange.send(frame));
}

#[test]
fn a_clock_attached_after_start_is_read_before_its_first_tick() {
    let mut server = TestServer::start();
    let (session, mut exchange) = connected(&mut server);
    assert!(
        session.domain_clock("sim").is_none(),
        "a session that has not attached follows no clock"
    );

    let attach = exchange.attach(observation(3, DomainClockObservedState::Paced(paced())));
    let attached = execute_answered(&server, session, "ATTACH DOMAIN CLOCK;", attach);
    assert_eq!(attached, Disposition::Completed);

    // The attach reply carried the running generation and its committed mapping, so the host reads
    // them before the tick the serving node sends next.
    let clock = session.followed_clock("sim");
    assert_eq!(clock.domain(), "sim");
    assert_eq!(clock.generation(), 3);
    assert_eq!(clock.state(), ClockState::Paced);
    assert_eq!(
        clock.paced(),
        Ok(PacedFields {
            period_nanos: 100_000_000,
            skew_nanos: 10_000_000,
            logical_origin: ORIGIN,
            utc_anchor: ANCHOR,
            time_rate: 2.5,
        })
    );
    assert_eq!(clock.tick(), None, "no tick has arrived yet");

    let ticked = third_tick(3)
        .encode(&SessionLimits::DEFAULT)
        .assured("a tick frame fits the default limits");
    server.runtime.block_on(exchange.send(ticked));
    let tick = session.next_clock_event();
    assert_eq!(tick.kind(), ClockEventKind::Tick);
    assert_eq!(tick.generation(), clock.generation());
    let fields = tick_fields(&tick);
    let period = i64::try_from(clock.paced().assured("the clock is paced").period_nanos)
        .assured("the fixture period fits i64");
    assert_eq!(
        fields.logical_boundary,
        ORIGIN + 2 * period,
        "the tick is read against the mapping the attach reported"
    );

    // A read after the tick event holds that tick, and the first read is unchanged.
    let after = session.followed_clock("sim");
    assert_eq!(after.generation(), 3);
    assert_eq!(after.tick(), Some(fields));
    assert_eq!(clock.tick(), None, "a read never changes");
    session.free();
}

#[test]
fn every_state_reports_only_the_fields_and_projections_it_carries() {
    let mut server = TestServer::start();
    let (session, mut exchange) = connected(&mut server);
    let attach = exchange.attach(observation(0, DomainClockObservedState::Stopped));
    assert_eq!(
        execute_answered(&server, session, "ATTACH DOMAIN CLOCK;", attach),
        Disposition::Completed
    );

    let stopped = session.followed_clock("sim");
    assert_eq!(stopped.generation(), 0);
    assert_eq!(stopped.state(), ClockState::Stopped);
    assert_eq!(stopped.paced(), Err(FailureKind::Type));
    assert_eq!(stopped.tick(), None);
    stopped.refuses_every_projection();

    observe(
        &server,
        &exchange,
        "sim",
        observation(1, DomainClockObservedState::Uninstalled),
    );
    assert_eq!(session.next_clock_event().state(), ClockState::Uninstalled);
    let uninstalled = session.followed_clock("sim");
    assert_eq!(uninstalled.generation(), 1);
    assert_eq!(uninstalled.state(), ClockState::Uninstalled);
    assert_eq!(uninstalled.paced(), Err(FailureKind::Type));
    uninstalled.refuses_every_projection();

    observe(
        &server,
        &exchange,
        "sim",
        observation(2, DomainClockObservedState::Unpaced),
    );
    assert_eq!(session.next_clock_event().state(), ClockState::Unpaced);
    let unpaced = session.followed_clock("sim");
    assert_eq!(unpaced.generation(), 2);
    assert_eq!(unpaced.state(), ClockState::Unpaced);
    assert_eq!(unpaced.paced(), Err(FailureKind::Type));
    assert_eq!(
        unpaced.logical_time_at(42),
        Ok(42),
        "an unpaced clock reads UTC"
    );
    assert_eq!(unpaced.wall_duration_until(42, 50), Ok(8));
    assert_eq!(unpaced.wall_duration_until(42, 40), Ok(0));
    assert_eq!(
        unpaced.wall_duration_until(i64::MIN, i64::MAX),
        Ok(u64::MAX)
    );
    assert_eq!(
        unpaced.admission_window(42),
        Ok(None),
        "an unpaced clock's ingestors admit every timestamp"
    );
    assert_eq!(unpaced.admits(42, i64::MIN), Ok(true));

    observe(
        &server,
        &exchange,
        "sim",
        observation(3, DomainClockObservedState::Paced(paced())),
    );
    assert_eq!(session.next_clock_event().state(), ClockState::Paced);
    let clock = session.followed_clock("sim");
    assert_eq!(clock.generation(), 3);
    assert_eq!(clock.paced().assured("the clock is paced").time_rate, 2.5);
    let mut rate = 0.0;
    // SAFETY: the reference is live, and a host may pass null for the fields it does not read.
    succeeded(unsafe {
        nx_domain_clock_paced(
            clock.0,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut rate,
        )
    });
    assert_eq!(rate, 2.5);

    // Before its anchor the domain sits at its origin, and 1000 ns after it at rate 2.5 the domain
    // is 2500 logical nanoseconds past it.
    assert_eq!(clock.logical_time_at(ANCHOR - 5), Ok(ORIGIN));
    assert_eq!(clock.logical_time_at(ANCHOR), Ok(ORIGIN));
    assert_eq!(clock.logical_time_at(ANCHOR + 1_000), Ok(ORIGIN + 2_500));
    // One period at rate 2.5 is 40 ms, and a single logical nanosecond still waits one.
    assert_eq!(
        clock.wall_duration_until(ANCHOR, ORIGIN + 100_000_000),
        Ok(40_000_000)
    );
    assert_eq!(clock.wall_duration_until(ANCHOR, ORIGIN + 1), Ok(1));
    assert_eq!(clock.wall_duration_until(ANCHOR, ORIGIN), Ok(0));
    // 100 ms of UTC after the anchor the domain has reached two periods past its origin.
    let reached = ANCHOR + 100_000_000;
    assert_eq!(
        clock.admission_window(reached),
        Ok(Some(Window {
            earliest_center: ORIGIN,
            latest_center: ORIGIN + 200_000_000,
        }))
    );
    assert_eq!(clock.admits(reached, ORIGIN + 210_000_000), Ok(true));
    assert_eq!(clock.admits(reached, ORIGIN + 210_000_001), Ok(false));
    assert_eq!(clock.admits(reached, ORIGIN + 90_000_000), Ok(true));
    assert_eq!(clock.admits(reached, ORIGIN + 150_000_000), Ok(false));
    assert_eq!(
        clock.logical_time_at(i64::MAX),
        Err(FailureKind::InvalidArgument),
        "a projection past the timestamp range is an argument out of range"
    );
    assert_eq!(
        clock.wall_duration_until(i64::MAX, 0),
        Err(FailureKind::InvalidArgument)
    );
    session.free();
}

#[test]
fn a_refused_attach_leaves_the_session_following_what_it_followed() {
    let mut server = TestServer::start();
    let (session, mut exchange) = connected(&mut server);
    let sim = domain("sim");

    let not_found = exchange.answer_attach(
        &sim,
        DomainClockAttachDisposition::DomainNotFound(sim.clone()),
    );
    assert_eq!(
        execute_answered(&server, session, "ATTACH DOMAIN CLOCK;", not_found),
        Disposition::Failed
    );
    assert!(session.domain_clock("sim").is_none());

    let refused = exchange.answer_attach(&sim, DomainClockAttachDisposition::Failed);
    assert_eq!(
        execute_answered(&server, session, "ATTACH DOMAIN CLOCK;", refused),
        Disposition::Failed
    );
    assert!(session.domain_clock("sim").is_none());

    let attach = exchange.attach(observation(4, DomainClockObservedState::Unpaced));
    assert_eq!(
        execute_answered(&server, session, "ATTACH DOMAIN CLOCK;", attach),
        Disposition::Completed
    );
    let already = exchange.answer_attach(
        &sim,
        DomainClockAttachDisposition::AlreadyAttached(sim.clone()),
    );
    assert_eq!(
        execute_answered(&server, session, "ATTACH DOMAIN CLOCK;", already),
        Disposition::Failed,
        "a second attach is refused as already attached"
    );
    let followed = session.followed_clock("sim");
    assert_eq!(followed.generation(), 4, "the first attachment still holds");
    assert_eq!(followed.state(), ClockState::Unpaced);
    session.free();
}

#[test]
fn several_domains_are_read_apart_until_a_detach_or_an_end_withdraws_one() {
    let mut server = TestServer::start();
    let (session, mut exchange) = connected(&mut server);
    let attach = exchange.attach(observation(1, DomainClockObservedState::Paced(paced())));
    assert_eq!(
        execute_answered(&server, session, "ATTACH DOMAIN CLOCK;", attach),
        Disposition::Completed
    );
    assert_eq!(session.execute("USE live;"), Disposition::Completed);
    let live = domain("live");
    let attach_live = exchange.answer_attach(
        &live,
        DomainClockAttachDisposition::Attached {
            domain: live.clone(),
            clock: observation(7, DomainClockObservedState::Unpaced),
        },
    );
    assert_eq!(
        execute_answered(&server, session, "ATTACH DOMAIN CLOCK;", attach_live),
        Disposition::Completed
    );

    let sim_clock = session.followed_clock("sim");
    let live_clock = session.followed_clock("live");
    assert_eq!(
        (
            sim_clock.domain(),
            sim_clock.generation(),
            sim_clock.state()
        ),
        ("sim".to_string(), 1, ClockState::Paced)
    );
    assert_eq!(
        (
            live_clock.domain(),
            live_clock.generation(),
            live_clock.state()
        ),
        ("live".to_string(), 7, ClockState::Unpaced)
    );
    assert!(session.domain_clock("other").is_none());

    // A detach withdraws the domain's clock and leaves the other one.
    let detach = exchange.detach(&live);
    assert_eq!(
        execute_answered(&server, session, "DETACH DOMAIN CLOCK;", detach),
        Disposition::Completed
    );
    assert!(session.domain_clock("live").is_none());
    assert_eq!(session.followed_clock("sim").generation(), 1);
    assert_eq!(
        live_clock.generation(),
        7,
        "a read stays valid after its domain is detached"
    );

    // Once the end of an attachment is read, the session follows that clock no more.
    let ended = DomainClockAttachmentEnded {
        domain: domain("sim"),
        reason: DomainClockAttachmentEndReason::DomainRemoved,
    }
    .encode(&SessionLimits::DEFAULT)
    .assured("an end frame fits the default limits");
    server.runtime.block_on(exchange.send(ended));
    assert_eq!(session.next_clock_event().kind(), ClockEventKind::Ended);
    assert!(session.domain_clock("sim").is_none());
    session.free();
}

#[test]
fn an_interrupted_attachment_reads_its_last_clock_without_a_tick_until_it_is_restored() {
    let mut server = TestServer::start();
    let (session, mut first) = connected(&mut server);
    let attach = first.attach(observation(1, DomainClockObservedState::Paced(paced())));
    assert_eq!(
        execute_answered(&server, session, "ATTACH DOMAIN CLOCK;", attach),
        Disposition::Completed
    );
    let ticked = third_tick(1)
        .encode(&SessionLimits::DEFAULT)
        .assured("a tick frame fits the default limits");
    server.runtime.block_on(first.send(ticked));
    assert_eq!(session.next_clock_event().kind(), ClockEventKind::Tick);
    assert!(session.followed_clock("sim").tick().is_some());

    drop(first);
    assert_eq!(
        session.next_clock_event().kind(),
        ClockEventKind::Interrupted
    );
    let interrupted = session.followed_clock("sim");
    assert_eq!(
        interrupted.generation(),
        1,
        "the ended session's clock is kept"
    );
    assert_eq!(interrupted.state(), ClockState::Paced);
    assert_eq!(
        interrupted.tick(),
        None,
        "the ended session's tick is withdrawn"
    );

    let reading = thread::spawn(move || session.next_clock_event());
    let mut second = server.next_exchange();
    server
        .runtime
        .block_on(second.attach(observation(2, DomainClockObservedState::Unpaced)));
    let restored = reading.join().assured("the reading thread returns");
    assert_eq!(restored.kind(), ClockEventKind::State);
    assert_eq!(restored.generation(), 2);
    let clock = session.followed_clock("sim");
    assert_eq!(
        (clock.generation(), clock.state()),
        (restored.generation(), restored.state()),
        "a read after the restored state is that state"
    );
    session.free();
}

#[test]
fn an_attach_cancelled_after_it_was_sent_is_resolved_by_executing_it_again() {
    let mut server = TestServer::start();
    let (session, mut exchange) = connected(&mut server);
    let sim = domain("sim");
    let attach = PreparedCommand::prepare(session, "ATTACH DOMAIN CLOCK;");

    // The server holds the attach it received, so only the token ends the host's wait.
    let waiting = thread::spawn(move || attach.run_cancellable());
    let request = server.runtime.block_on(exchange.read_attach(&sim));
    attach.trigger();
    assert_eq!(
        waiting.join().assured("the waiting thread returns"),
        Err(FailureKind::Cancelled)
    );

    // The server admitted the attach after all, and its reply attaches the session.
    let attached = DomainClockAttachDisposition::Attached {
        domain: sim.clone(),
        clock: observation(5, DomainClockObservedState::Paced(paced())),
    };
    server
        .runtime
        .block_on(exchange.reply_to_attach(request, attached));

    // Executing the same execution again is answered after the earlier attempt: refused as
    // already attached, after which the read reports the clock the earlier reply carried.
    let again = thread::spawn(move || attach.run_to_completion());
    let already = exchange.answer_attach(
        &sim,
        DomainClockAttachDisposition::AlreadyAttached(sim.clone()),
    );
    server.runtime.block_on(already);
    assert_eq!(
        again.join().assured("the executing thread returns"),
        Ok(Disposition::Failed)
    );
    let clock = session.followed_clock("sim");
    assert_eq!(clock.generation(), 5);
    assert_eq!(clock.state(), ClockState::Paced);
    attach.free();
    session.free();
}

#[test]
fn a_domain_clock_outlives_references_released_on_another_thread_and_its_session() {
    let mut server = TestServer::start();
    let (session, mut exchange) = connected(&mut server);
    let attach = exchange.attach(observation(2, DomainClockObservedState::Paced(paced())));
    assert_eq!(
        execute_answered(&server, session, "ATTACH DOMAIN CLOCK;", attach),
        Disposition::Completed
    );
    let clock = session.followed_clock("sim");
    let second = clock.retain();
    assert_eq!(
        second.0, clock.0,
        "a retained reference addresses the same clock"
    );
    thread::spawn(move || drop(clock))
        .join()
        .assured("releasing on another thread does not panic");
    session.free();
    assert_eq!(second.domain(), "sim");
    assert_eq!(second.generation(), 2);
    assert_eq!(second.paced().assured("the clock is paced").time_rate, 2.5);
    assert_eq!(second.logical_time_at(ANCHOR), Ok(ORIGIN));
    // SAFETY: releasing null is allowed and does nothing.
    unsafe { nx_domain_clock_release(ptr::null_mut()) };
}

#[test]
fn reading_a_domain_clock_refuses_missing_and_invalid_arguments() {
    let mut server = TestServer::start();
    let (session, mut exchange) = connected(&mut server);
    let attach = exchange.attach(observation(1, DomainClockObservedState::Paced(paced())));
    assert_eq!(
        execute_answered(&server, session, "ATTACH DOMAIN CLOCK;", attach),
        Disposition::Completed
    );
    let name = "sim";
    let invalid = "not a domain";
    let not_utf8 = [0xff_u8, 0xfe];
    let mut clock: *mut DomainClock = ptr::null_mut();
    // SAFETY: every non-null pointer is live, readable for its length or writable; the null and
    // invalid ones are what is refused.
    unsafe {
        assert_eq!(
            failure_kind(nx_session_domain_clock(
                ptr::null(),
                name.as_ptr(),
                name.len(),
                &mut clock
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_session_domain_clock(
                session.0,
                name.as_ptr(),
                name.len(),
                ptr::null_mut()
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_session_domain_clock(
                session.0,
                ptr::null(),
                0,
                &mut clock
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_session_domain_clock(
                session.0,
                not_utf8.as_ptr(),
                not_utf8.len(),
                &mut clock
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_session_domain_clock(
                session.0,
                invalid.as_ptr(),
                invalid.len(),
                &mut clock
            )),
            FailureKind::InvalidArgument
        );
    }
    assert!(clock.is_null(), "a refused read writes nothing");

    let followed = session.followed_clock(name);
    let mut has_window = false;
    let mut period = 0;
    // SAFETY: every non-null pointer is live or writable; the null ones are what is refused.
    unsafe {
        assert_eq!(
            failure_kind(nx_domain_clock_logical_time_at(
                followed.0,
                0,
                ptr::null_mut()
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_domain_clock_wall_duration_until(
                followed.0,
                0,
                0,
                ptr::null_mut()
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_domain_clock_admission_window(
                followed.0,
                0,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut()
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_domain_clock_admits(followed.0, 0, 0, ptr::null_mut())),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_domain_clock_paced(
                ptr::null(),
                &mut period,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut()
            )),
            FailureKind::InvalidArgument
        );
        let mut logical = 0;
        assert_eq!(
            failure_kind(nx_domain_clock_logical_time_at(
                ptr::null(),
                0,
                &mut logical
            )),
            FailureKind::InvalidArgument
        );
        let mut wait = 0;
        assert_eq!(
            failure_kind(nx_domain_clock_wall_duration_until(
                ptr::null(),
                0,
                0,
                &mut wait
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_domain_clock_admission_window(
                ptr::null(),
                0,
                &mut has_window,
                ptr::null_mut(),
                ptr::null_mut()
            )),
            FailureKind::InvalidArgument
        );
        let mut admitted = false;
        assert_eq!(
            failure_kind(nx_domain_clock_admits(ptr::null(), 0, 0, &mut admitted)),
            FailureKind::InvalidArgument
        );
    }
    // A host may omit either center of a window.
    // SAFETY: the reference is live and `has_window` is writable.
    succeeded(unsafe {
        nx_domain_clock_admission_window(
            followed.0,
            ANCHOR,
            &mut has_window,
            ptr::null_mut(),
            ptr::null_mut(),
        )
    });
    assert!(has_window);
    session.free();
}
