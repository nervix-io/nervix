//! What the server process does when it is asked to terminate.
//!
//! Layer: edges.
//!
//! - **Owns.** SIGINT and SIGTERM for the whole life of the process: registering them, turning the
//!   first one into a graceful stop request, and turning every later one into an immediate forced
//!   exit whose status names the signal that forced it. It also owns the forced exit that ends a
//!   graceful shutdown still running when its deadline passes.
//! - **Depends on.** `signal-hook` for registration and delivery, and the shutdown coordinator's
//!   stop request, deadline, and phase.
//! - **Must not know.** Which services observe a shutdown phase, how long a phase may take, or what
//!   work a forced exit abandons.
//!
//! Signal delivery and the deadline each run on a dedicated operating-system thread rather than a
//! Tokio task. A forced exit is for a process whose graceful shutdown has stopped making progress,
//! and such a process may have no runtime worker free to poll a task; a thread blocked reading the
//! signal pipe or parked until the deadline still wakes.
//!
//! Nothing unregisters the signals. Removing the handler would not restore the default action: the
//! signal registry keeps its own handler installed, so a later signal would simply be ignored.
//! Supervision therefore lasts until the process exits, and the first signal does not end it.

use std::{ffi::c_int, time::Duration};
#[cfg(not(feature = "shuttle"))]
use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
#[cfg(feature = "shuttle")]
use shuttle::{
    sync::atomic::{AtomicBool, Ordering},
    thread,
};
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
    low_level,
};
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime};
use tracing::{info, warn};
use triomphe::Arc;

use super::{
    error::AppError,
    shutdown::{ShutdownCoordinator, ShutdownPhase, ShutdownRequestOutcome},
};

/// A shell reports a process that a signal terminated as exiting with this base plus the signal
/// number, and a forced exit reports itself the same way.
const SIGNALLED_EXIT_STATUS_BASE: c_int = 128;
const INTERRUPT_EXIT_STATUS: c_int = SIGNALLED_EXIT_STATUS_BASE + SIGINT;
const TERMINATE_EXIT_STATUS: c_int = SIGNALLED_EXIT_STATUS_BASE + SIGTERM;
/// A shutdown that its deadline cut short did not finish, so the process exits with the status it
/// reports when a shutdown step fails.
const DEADLINE_EXPIRED_EXIT_STATUS: c_int = 1;

/// How long a forced exit waits for its log record before exiting without it. Writing the record
/// normally takes microseconds; the bound matters only when standard output has stopped draining.
const FORCED_EXIT_REPORT_BUDGET: Duration = Duration::from_secs(1);

const SIGNAL_SUPERVISOR_THREAD: &str = "nervix-termination-signals";
const DEADLINE_SUPERVISOR_THREAD: &str = "nervix-shutdown-deadline";
const FORCED_EXIT_WATCHDOG_THREAD: &str = "nervix-forced-exit";

/// A signal that asks the process to terminate.
#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::Display, strum::FromRepr)]
#[repr(i32)]
enum TerminationSignal {
    #[strum(serialize = "SIGINT")]
    Interrupt = SIGINT,
    #[strum(serialize = "SIGTERM")]
    Terminate = SIGTERM,
}

impl TerminationSignal {
    const ALL: [Self; 2] = [Self::Interrupt, Self::Terminate];

    const fn number(self) -> c_int {
        match self {
            Self::Interrupt => SIGINT,
            Self::Terminate => SIGTERM,
        }
    }

    /// The status a shell reports for a process this signal terminated.
    const fn forced_exit_status(self) -> c_int {
        match self {
            Self::Interrupt => INTERRUPT_EXIT_STATUS,
            Self::Terminate => TERMINATE_EXIT_STATUS,
        }
    }
}

/// SIGINT and SIGTERM, registered for the rest of the process and waiting to be supervised.
///
/// Registration and supervision are separate steps because the process registers before the
/// coordinator a signal stops exists. A signal that arrives in between is held rather than lost,
/// and it is delivered as soon as supervision starts.
pub struct TerminationSignals {
    signals: Signals,
}

impl TerminationSignals {
    /// Registers both signals. From here on neither can end the process by its default action.
    pub fn register() -> Result<Self, Report<AppError>> {
        let numbers = TerminationSignal::ALL.map(TerminationSignal::number);
        let signals = Signals::new(numbers).change_context(AppError::RegisterTerminationSignals)?;
        Ok(Self { signals })
    }

    /// Delivers every registered signal to `shutdown` and enforces the deadline of the shutdown
    /// it starts, each on a dedicated thread until the process exits.
    pub(in crate::application) fn supervise(
        self,
        shutdown: ShutdownCoordinator,
    ) -> Result<(), Report<AppError>> {
        let forced_exit = ForcedExitClaim::new(ServerProcess);
        let deadline = DeadlineSupervision::new(shutdown.clone(), forced_exit.clone())?;
        let signals = SignalSupervision::new(shutdown, forced_exit);
        // The signal supervisor must outlive every other part of the process, because a stopped
        // supervisor would leave both signals ignored.
        spawn_until_process_exit(SIGNAL_SUPERVISOR_THREAD, move || self.deliver_to(signals))
            .change_context(AppError::SuperviseTerminationSignals)?;
        // The deadline supervisor parks until the deadline, and a process that finishes shutting
        // down before then exits without waiting for it.
        spawn_until_process_exit(DEADLINE_SUPERVISOR_THREAD, move || deadline.enforce())
            .change_context(AppError::SuperviseShutdownDeadline)?;
        Ok(())
    }

    fn deliver_to<P: ProcessExit>(mut self, mut supervision: SignalSupervision<P>) {
        for number in self.signals.forever() {
            let signal = TerminationSignal::from_repr(number)
                .assured("signal-hook delivers only the signals this registration named");
            supervision.deliver(signal);
        }
        None::<()>.assured(
            "the delivery iterator ends only when its handle is closed, and nothing takes that \
             handle",
        );
    }
}

/// Starts `body` on a thread that nothing joins, so it runs until it returns or the process exits.
#[cfg(not(feature = "shuttle"))]
fn spawn_until_process_exit<F>(name: &str, body: F) -> std::io::Result<()>
where
    F: FnOnce() + Send + 'static,
{
    let handle = thread::Builder::new().name(name.to_string()).spawn(body)?;
    drop(handle);
    Ok(())
}

/// Starts `body` as a detached Shuttle task. A model ends when its main thread returns and abandons
/// a detached task wherever it is parked, as a process that exits abandons a thread nothing joins.
#[cfg(feature = "shuttle")]
fn spawn_until_process_exit<F>(_name: &str, body: F) -> std::io::Result<()>
where
    F: FnOnce() + Send + 'static,
{
    let detached = shuttle::future::spawn(async move { body() });
    drop(detached);
    Ok(())
}

/// How a forced exit ends the process it runs in.
///
/// Supervision reaches the process only through this boundary, and the server names
/// [`ServerProcess`] once, where supervision starts. A deterministic model supervises a process
/// that records how it ended instead, so the forced exit it checks is the one the server runs.
trait ProcessExit: Send + Sync + 'static {
    /// Ends the process at once with `status`. No destructor, exit handler, or other thread runs
    /// afterwards.
    fn exit(&self, status: c_int) -> !;

    /// Parks the calling thread for the rest of the process, while the forced exit that claimed
    /// the process ends it.
    fn park_until_exit(&self) -> !;
}

/// The operating-system process the server runs as.
struct ServerProcess;

impl ProcessExit for ServerProcess {
    fn exit(&self, status: c_int) -> ! {
        low_level::exit(status)
    }

    fn park_until_exit(&self) -> ! {
        loop {
            thread::park();
        }
    }
}

/// The termination signals one process has received so far, and the coordinator they stop.
struct SignalSupervision<P> {
    shutdown: ShutdownCoordinator,
    first_signal: Option<TerminationSignal>,
    forced_exit: ForcedExitClaim<P>,
}

impl<P: ProcessExit> SignalSupervision<P> {
    fn new(shutdown: ShutdownCoordinator, forced_exit: ForcedExitClaim<P>) -> Self {
        Self {
            shutdown,
            first_signal: None,
            forced_exit,
        }
    }

    /// Carries out what one received signal asks for. A signal that forces an exit does not
    /// return.
    fn deliver(&mut self, signal: TerminationSignal) {
        let disposition = self.receive(signal);
        self.carry_out(disposition);
    }

    /// Decides what one received signal asks for: the first starts graceful shutdown, and every
    /// later one forces the process to exit.
    fn receive(&mut self, signal: TerminationSignal) -> SignalDisposition {
        let Some(first) = self.first_signal else {
            self.first_signal = Some(signal);
            let request = self.shutdown.request_stop();
            return SignalDisposition::GracefulShutdown { signal, request };
        };
        SignalDisposition::ForcedExit(ForcedExit {
            cause: ForcedExitCause::RepeatedSignal {
                first,
                repeated: signal,
            },
            phase: self.shutdown.phase(),
        })
    }

    /// Carries a disposition out. A forced exit does not return.
    fn carry_out(&self, disposition: SignalDisposition) {
        match disposition {
            SignalDisposition::GracefulShutdown {
                signal,
                request: ShutdownRequestOutcome::Accepted(_),
            } => {
                info!(
                    %signal,
                    "termination signal received; requesting graceful shutdown"
                );
            }
            SignalDisposition::GracefulShutdown {
                signal,
                request: ShutdownRequestOutcome::AlreadyRequested(_),
            } => {
                info!(
                    %signal,
                    "termination signal received while graceful shutdown is already in progress"
                );
            }
            SignalDisposition::ForcedExit(forced_exit) => forced_exit.exit(&self.forced_exit),
        }
    }
}

/// What receiving one termination signal asks of the process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignalDisposition {
    /// The first termination signal, and what the coordinator did with its stop request.
    GracefulShutdown {
        signal: TerminationSignal,
        request: ShutdownRequestOutcome,
    },
    /// A later termination signal, which abandons graceful shutdown.
    ForcedExit(ForcedExit),
}

/// Ends a process whose graceful shutdown is still running when that shutdown's deadline passes,
/// whatever made the stop request.
struct DeadlineSupervision<P> {
    shutdown: ShutdownCoordinator,
    forced_exit: ForcedExitClaim<P>,
    /// Waits for the stop request on the supervision thread, which runs no Tokio runtime. It drives
    /// no I/O or timers, only the coordinator's notification that a stop was requested.
    request_waiter: TokioRuntime,
}

impl<P: ProcessExit> DeadlineSupervision<P> {
    fn new(
        shutdown: ShutdownCoordinator,
        forced_exit: ForcedExitClaim<P>,
    ) -> Result<Self, Report<AppError>> {
        let request_waiter = TokioRuntimeBuilder::new_current_thread()
            .build()
            .change_context(AppError::SuperviseShutdownDeadline)?;
        Ok(Self {
            shutdown,
            forced_exit,
            request_waiter,
        })
    }

    /// Parks until the deadline of the first stop request, then forces the process to exit. A
    /// process that finishes shutting down first has already exited by then.
    fn enforce(self) {
        let request = self.request_waiter.block_on(self.shutdown.requested());
        let deadline = request.deadline();
        loop {
            let remaining = deadline.remaining();
            if remaining.is_zero() {
                break;
            }
            // Nothing unparks this thread, so it wakes once the time left has passed or spuriously,
            // and measures the time left again either way. Shuttle does not model time and keeps a
            // parked thread blocked, where a sleeping one would spin.
            thread::park_timeout(remaining);
        }
        let forced_exit = ForcedExit {
            cause: ForcedExitCause::DeadlineExpired,
            phase: self.shutdown.phase(),
        };
        forced_exit.exit(&self.forced_exit)
    }
}

/// Lets one forced exit end the process.
///
/// The signal and deadline supervisors can both decide to force an exit. Only the first one
/// reports and exits, so the log names the status the process actually exits with.
struct ForcedExitClaim<P> {
    inner: Arc<ForcedExitClaimInner<P>>,
}

/// Whether a forced exit has claimed the process yet, and the process it ends.
struct ForcedExitClaimInner<P> {
    claimed: AtomicBool,
    process: P,
}

impl<P> Clone for ForcedExitClaim<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<P: ProcessExit> ForcedExitClaim<P> {
    fn new(process: P) -> Self {
        Self {
            inner: Arc::new(ForcedExitClaimInner {
                claimed: AtomicBool::new(false),
                process,
            }),
        }
    }

    /// Claims the exit for the caller, and returns false when another forced exit already has it.
    fn claim(&self) -> bool {
        let already_claimed = self.inner.claimed.swap(true, Ordering::AcqRel);
        !already_claimed
    }

    fn process(&self) -> &P {
        &self.inner.process
    }
}

/// Why graceful shutdown is abandoned and the process ends at once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ForcedExitCause {
    /// A termination signal arrived after the one that started graceful shutdown.
    RepeatedSignal {
        first: TerminationSignal,
        repeated: TerminationSignal,
    },
    /// Graceful shutdown was still running when its deadline passed.
    DeadlineExpired,
}

/// A forced end of the process, and the graceful shutdown it abandons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ForcedExit {
    cause: ForcedExitCause,
    /// The phase graceful shutdown had reached when it was abandoned.
    phase: ShutdownPhase,
}

impl ForcedExit {
    fn status(self) -> c_int {
        match self.cause {
            ForcedExitCause::RepeatedSignal { repeated, .. } => repeated.forced_exit_status(),
            ForcedExitCause::DeadlineExpired => DEADLINE_EXPIRED_EXIT_STATUS,
        }
    }

    /// Ends the process at once. No destructor, exit handler, or remaining shutdown phase runs, so
    /// work still in progress is abandoned exactly as a crash would abandon it.
    ///
    /// The exit is logged first, within a bound: a watchdog thread exits with the same status once
    /// the budget passes, so standard output that has stopped draining cannot keep the process
    /// alive. When another forced exit has already claimed the process, this one parks until that
    /// exit instead of reporting a status the process will not exit with.
    fn exit<P: ProcessExit>(self, claim: &ForcedExitClaim<P>) -> ! {
        if !claim.claim() {
            claim.process().park_until_exit()
        }
        let status = self.status();
        let watchdog_claim = claim.clone();
        let watchdog = spawn_until_process_exit(FORCED_EXIT_WATCHDOG_THREAD, move || {
            Self::exit_after_report_budget(status, &watchdog_claim);
        });
        let Ok(()) = watchdog else {
            // Nothing could bound the log record, and the exit itself must stay bounded.
            claim.process().exit(status)
        };
        self.report(status);
        claim.process().exit(status)
    }

    fn report(self, status: c_int) {
        match self.cause {
            ForcedExitCause::RepeatedSignal { first, repeated } => {
                warn!(
                    signal = %repeated,
                    first_signal = %first,
                    phase = ?self.phase,
                    exit_status = status,
                    "repeated termination signal received; abandoning graceful shutdown"
                );
            }
            ForcedExitCause::DeadlineExpired => {
                warn!(
                    phase = ?self.phase,
                    exit_status = status,
                    "shutdown deadline expired; abandoning graceful shutdown"
                );
            }
        }
    }

    fn exit_after_report_budget<P: ProcessExit>(status: c_int, claim: &ForcedExitClaim<P>) {
        thread::sleep(FORCED_EXIT_REPORT_BUDGET);
        claim.process().exit(status);
    }
}

#[cfg(test)]
mod tests {
    use signal_hook::consts::SIGHUP;

    use super::*;

    #[test]
    fn the_first_signal_requests_graceful_shutdown() {
        let shutdown = ShutdownCoordinator::default();
        let mut supervision =
            SignalSupervision::new(shutdown.clone(), ForcedExitClaim::new(ServerProcess));

        let disposition = supervision.receive(TerminationSignal::Terminate);

        let request = shutdown
            .request()
            .expect("the first signal must have requested shutdown");
        assert_eq!(
            disposition,
            SignalDisposition::GracefulShutdown {
                signal: TerminationSignal::Terminate,
                request: ShutdownRequestOutcome::Accepted(request),
            }
        );
        assert_eq!(shutdown.phase(), ShutdownPhase::StopRequested);
    }

    #[test]
    fn every_signal_after_the_first_forces_an_exit_named_by_that_signal() {
        let shutdown = ShutdownCoordinator::default();
        let mut supervision = SignalSupervision::new(shutdown, ForcedExitClaim::new(ServerProcess));
        let first = supervision.receive(TerminationSignal::Interrupt);
        assert!(
            matches!(first, SignalDisposition::GracefulShutdown { .. }),
            "the first signal must not force an exit, got {first:?}"
        );

        let second = supervision.receive(TerminationSignal::Terminate);
        let third = supervision.receive(TerminationSignal::Interrupt);

        let expected_second = ForcedExit {
            cause: ForcedExitCause::RepeatedSignal {
                first: TerminationSignal::Interrupt,
                repeated: TerminationSignal::Terminate,
            },
            phase: ShutdownPhase::StopRequested,
        };
        let expected_third = ForcedExit {
            cause: ForcedExitCause::RepeatedSignal {
                first: TerminationSignal::Interrupt,
                repeated: TerminationSignal::Interrupt,
            },
            phase: ShutdownPhase::StopRequested,
        };
        assert_eq!(second, SignalDisposition::ForcedExit(expected_second));
        assert_eq!(third, SignalDisposition::ForcedExit(expected_third));
        assert_eq!(expected_second.status(), 143);
        assert_eq!(expected_third.status(), 130);
    }

    #[test]
    fn a_first_signal_joins_a_shutdown_already_in_progress() {
        let shutdown = ShutdownCoordinator::default();
        shutdown.request_stop();
        let request = shutdown
            .request()
            .expect("the stop request above must be recorded");
        let mut supervision = SignalSupervision::new(shutdown, ForcedExitClaim::new(ServerProcess));

        let disposition = supervision.receive(TerminationSignal::Interrupt);

        assert_eq!(
            disposition,
            SignalDisposition::GracefulShutdown {
                signal: TerminationSignal::Interrupt,
                request: ShutdownRequestOutcome::AlreadyRequested(request),
            }
        );
    }

    #[test]
    fn a_forced_exit_status_is_what_a_shell_reports_for_its_signal() {
        assert_eq!(TerminationSignal::Interrupt.forced_exit_status(), 130);
        assert_eq!(TerminationSignal::Terminate.forced_exit_status(), 143);
    }

    #[test]
    fn a_shutdown_that_outlives_its_deadline_exits_as_a_failed_shutdown() {
        let forced_exit = ForcedExit {
            cause: ForcedExitCause::DeadlineExpired,
            phase: ShutdownPhase::DrainSupport,
        };

        assert_eq!(forced_exit.status(), 1);
    }

    #[test]
    fn only_the_first_forced_exit_claims_the_process() {
        let signal_supervisor = ForcedExitClaim::new(ServerProcess);
        let deadline_supervisor = signal_supervisor.clone();

        assert!(deadline_supervisor.claim());
        assert!(!signal_supervisor.claim());
        assert!(!deadline_supervisor.claim());
    }

    #[test]
    fn only_the_registered_signals_are_termination_signals() {
        for signal in TerminationSignal::ALL {
            assert_eq!(TerminationSignal::from_repr(signal.number()), Some(signal));
        }
        assert_eq!(TerminationSignal::from_repr(SIGHUP), None);
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod shuttle_tests {
    use meticulous::ResultExt as _;
    use shuttle::future::block_on;
    use tokio::sync::watch;

    use super::*;
    use crate::{
        application::{
            shutdown::{ShutdownOutcome, ShutdownRequest},
            test_fixtures::{FAR_FUTURE_SHUTDOWN_TIMEOUT, shut_down_in_phase_order},
        },
        shuttle_test::{check_pct, check_random},
    };

    const MODEL_THREAD_JOINS: &str =
        "Shuttle fails the whole execution when a model thread panics, so no join observes one";
    const DETACHED_TASK_SPAWNS: &str =
        "a detached Shuttle task needs no operating-system thread, so starting one cannot fail";
    const PCT_DEPTH: usize = 3;
    const PCT_ITERATIONS: usize = 1_000;
    const RANDOM_ITERATIONS: usize = 1_000;

    /// How a modelled process ended, as its forced exits recorded it.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    struct RecordedEnding {
        /// The status the process exited with, once a forced exit ended it.
        exit_status: Option<c_int>,
        /// Forced exits that found the process already claimed and parked behind that exit.
        parked_claimants: usize,
    }

    impl RecordedEnding {
        /// Records an exit with `status`. Exiting does not stop a model, so the forced exit that
        /// claimed the process and its watchdog can both reach the exit; both name the same status,
        /// and any other status means a second forced exit proceeded.
        fn record_exit(&mut self, status: c_int) {
            let Some(recorded) = self.exit_status else {
                self.exit_status = Some(status);
                return;
            };
            assert_eq!(
                recorded, status,
                "a second forced exit ended the process with a different status"
            );
        }

        fn record_parked_claimant(&mut self) {
            self.parked_claimants = self
                .parked_claimants
                .checked_add(1)
                .assured("a model starts two forced-exit claimants");
        }
    }

    /// A process whose forced exits are recorded instead of ending it. A task that exits or parks
    /// through it parks for the rest of the model, as its thread would stop with a process that
    /// ended.
    struct RecordedProcess {
        ending: watch::Sender<RecordedEnding>,
    }

    impl ProcessExit for RecordedProcess {
        fn exit(&self, status: c_int) -> ! {
            self.ending.send_modify(|ending| ending.record_exit(status));
            loop {
                thread::park();
            }
        }

        fn park_until_exit(&self) -> ! {
            self.ending
                .send_modify(RecordedEnding::record_parked_claimant);
            loop {
                thread::park();
            }
        }
    }

    /// A server process under supervision. Its signal and deadline supervisors run on tasks that
    /// end only with the process, the composition root moves shutdown through its phases, a public
    /// listener that stopped requests shutdown as well, and a task waits for the shutdown outcome.
    struct SupervisedProcess {
        shutdown: ShutdownCoordinator,
        ending: watch::Receiver<RecordedEnding>,
        composition_root: thread::JoinHandle<ShutdownRequest>,
        stopped_listener: thread::JoinHandle<ShutdownRequestOutcome>,
        completion: thread::JoinHandle<ShutdownOutcome>,
    }

    impl SupervisedProcess {
        /// Supervises a shutdown whose deadline passes `timeout` after its first stop request, and
        /// delivers SIGINT and then SIGTERM to the process.
        fn start(timeout: Duration) -> Self {
            let shutdown = ShutdownCoordinator::new(timeout);
            let (recorder, ending) = watch::channel(RecordedEnding::default());
            let forced_exit = ForcedExitClaim::new(RecordedProcess { ending: recorder });
            let deadline = DeadlineSupervision::new(shutdown.clone(), forced_exit.clone()).assured(
                "Shuttle's runtime builder allocates no driver, so building it cannot fail",
            );
            let mut signals = SignalSupervision::new(shutdown.clone(), forced_exit.clone());
            // A forced exit runs no destructor, and the supervision tasks it leaves parked are
            // unwound by Shuttle only after the model has ended, when dropping the last coordinator
            // or claim handle would take a Shuttle lock outside any execution. The model keeps one
            // handle to each for the rest of the test process, as an exited process leaves its
            // memory to the operating system.
            std::mem::forget(shutdown.clone());
            std::mem::forget(forced_exit);

            let completing = shutdown.clone();
            let completion = thread::spawn(move || block_on(completing.completion()));
            let composing = shutdown.clone();
            let composition_root =
                thread::spawn(move || block_on(shut_down_in_phase_order(composing)));
            spawn_until_process_exit(DEADLINE_SUPERVISOR_THREAD, move || deadline.enforce())
                .assured(DETACHED_TASK_SPAWNS);
            spawn_until_process_exit(SIGNAL_SUPERVISOR_THREAD, move || {
                signals.deliver(TerminationSignal::Interrupt);
                // A repeated signal forces an exit, which parks this task instead of returning.
                signals.deliver(TerminationSignal::Terminate);
            })
            .assured(DETACHED_TASK_SPAWNS);
            let listening = shutdown.clone();
            let stopped_listener = thread::spawn(move || listening.request_stop());

            Self {
                shutdown,
                ending,
                composition_root,
                stopped_listener,
                completion,
            }
        }

        /// Waits until the recorded ending satisfies `ended`, and returns it.
        fn wait_until_ended(
            &mut self,
            ended: impl FnMut(&RecordedEnding) -> bool,
        ) -> RecordedEnding {
            *block_on(self.ending.wait_for(ended))
                .assured("the supervisors keep the recording process for the rest of the model")
        }

        /// Joins the tasks that shut down gracefully, checks that each observed the accepted stop
        /// request and the one outcome, and returns that outcome.
        fn join_graceful_shutdown(self) -> ShutdownOutcome {
            let composed_request = self.composition_root.join().assured(MODEL_THREAD_JOINS);
            let listener_request = self.stopped_listener.join().assured(MODEL_THREAD_JOINS);
            let completed = self.completion.join().assured(MODEL_THREAD_JOINS);

            let accepted = self
                .shutdown
                .request()
                .verified("the composition root returned the stop request it waited for");
            assert_eq!(
                composed_request, accepted,
                "the composition root must shut down under the accepted request and its deadline"
            );
            let listener_observed = match listener_request {
                ShutdownRequestOutcome::Accepted(request)
                | ShutdownRequestOutcome::AlreadyRequested(request) => request,
            };
            assert_eq!(
                listener_observed, accepted,
                "a racing stop request must observe the accepted request and its deadline"
            );
            let outcome = self
                .shutdown
                .outcome()
                .verified("the composition root finished shutdown before it returned");
            assert_eq!(
                completed, outcome,
                "completion must observe the one outcome shutdown finished with"
            );
            outcome
        }
    }

    /// A deadline that passed the moment shutdown was requested and a repeated SIGTERM both claim
    /// the forced exit while the composition root moves shutdown through its phases.
    fn an_expired_deadline_racing_a_repeated_signal() {
        let mut process = SupervisedProcess::start(Duration::ZERO);

        let ending = process.wait_until_ended(|ending| {
            ending.exit_status.is_some() && ending.parked_claimants == 1
        });

        let status = ending
            .exit_status
            .verified("the ending was awaited until it recorded an exit status");
        assert!(
            status == DEADLINE_EXPIRED_EXIT_STATUS || status == TERMINATE_EXIT_STATUS,
            "the process must exit with the status of the claimant that won, the expired deadline \
             or the repeated SIGTERM, not {status}"
        );
        let outcome = process.join_graceful_shutdown();
        assert!(
            outcome.deadline_expired(),
            "a shutdown whose deadline had passed must report its phases forced, got {outcome:?}"
        );
    }

    /// A repeated SIGTERM claims the forced exit while the deadline supervisor parks until a
    /// deadline no model reaches and the composition root moves shutdown through its phases.
    fn a_repeated_signal_racing_a_far_future_deadline() {
        let mut process = SupervisedProcess::start(FAR_FUTURE_SHUTDOWN_TIMEOUT);

        let ending = process.wait_until_ended(|ending| ending.exit_status.is_some());

        assert_eq!(
            ending,
            RecordedEnding {
                exit_status: Some(TERMINATE_EXIT_STATUS),
                parked_claimants: 0,
            },
            "only the repeated SIGTERM may claim the forced exit before the deadline, and the \
             process must exit with its status"
        );
        let outcome = process.join_graceful_shutdown();
        assert!(
            !outcome.deadline_expired(),
            "no phase may be forced before the deadline, got {outcome:?}"
        );
    }

    #[test]
    fn shuttle_an_expired_deadline_and_a_repeated_signal_let_exactly_one_forced_exit_end_the_process()
     {
        check_random(
            an_expired_deadline_racing_a_repeated_signal,
            RANDOM_ITERATIONS,
        );
        check_pct(
            an_expired_deadline_racing_a_repeated_signal,
            PCT_ITERATIONS,
            PCT_DEPTH,
        );
    }

    #[test]
    fn shuttle_a_repeated_signal_before_the_deadline_ends_the_process_with_the_status_of_that_signal()
     {
        check_random(
            a_repeated_signal_racing_a_far_future_deadline,
            RANDOM_ITERATIONS,
        );
        check_pct(
            a_repeated_signal_racing_a_far_future_deadline,
            PCT_ITERATIONS,
            PCT_DEPTH,
        );
    }
}
