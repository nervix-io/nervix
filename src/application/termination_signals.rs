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
//! signal pipe or sleeping until the deadline still wakes.
//!
//! Nothing unregisters the signals. Removing the handler would not restore the default action: the
//! signal registry keeps its own handler installed, so a later signal would simply be ignored.
//! Supervision therefore lasts until the process exits, and the first signal does not end it.

use std::{
    ffi::c_int,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
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
        let forced_exit = ForcedExitClaim::default();
        let deadline = DeadlineSupervision::new(shutdown.clone(), forced_exit.clone())?;
        let signals = SignalSupervision::new(shutdown, forced_exit);
        let signal_supervisor = thread::Builder::new()
            .name("nervix-termination-signals".to_string())
            .spawn(move || self.deliver_to(signals))
            .change_context(AppError::SuperviseTerminationSignals)?;
        // Never joined: the supervisor must outlive every other part of the process, because a
        // stopped supervisor would leave both signals ignored.
        drop(signal_supervisor);
        let deadline_supervisor = thread::Builder::new()
            .name("nervix-shutdown-deadline".to_string())
            .spawn(move || deadline.enforce())
            .change_context(AppError::SuperviseShutdownDeadline)?;
        // Never joined either: it sleeps until the deadline, and a process that finishes shutting
        // down before then exits without waiting for it.
        drop(deadline_supervisor);
        Ok(())
    }

    fn deliver_to(mut self, mut supervision: SignalSupervision) {
        for number in self.signals.forever() {
            let signal = TerminationSignal::from_repr(number)
                .assured("signal-hook delivers only the signals this registration named");
            let disposition = supervision.receive(signal);
            supervision.carry_out(disposition);
        }
        None::<()>.assured(
            "the delivery iterator ends only when its handle is closed, and nothing takes that \
             handle",
        );
    }
}

/// The termination signals one process has received so far, and the coordinator they stop.
struct SignalSupervision {
    shutdown: ShutdownCoordinator,
    first_signal: Option<TerminationSignal>,
    forced_exit: ForcedExitClaim,
}

impl SignalSupervision {
    fn new(shutdown: ShutdownCoordinator, forced_exit: ForcedExitClaim) -> Self {
        Self {
            shutdown,
            first_signal: None,
            forced_exit,
        }
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
struct DeadlineSupervision {
    shutdown: ShutdownCoordinator,
    forced_exit: ForcedExitClaim,
    /// Waits for the stop request on the supervision thread, which runs no Tokio runtime. It drives
    /// no I/O or timers, only the coordinator's notification that a stop was requested.
    request_waiter: TokioRuntime,
}

impl DeadlineSupervision {
    fn new(
        shutdown: ShutdownCoordinator,
        forced_exit: ForcedExitClaim,
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

    /// Sleeps until the deadline of the first stop request, then forces the process to exit. A
    /// process that finishes shutting down first has already exited by then.
    fn enforce(self) {
        let request = self.request_waiter.block_on(self.shutdown.requested());
        let deadline = request.deadline();
        loop {
            let remaining = deadline.remaining();
            if remaining.is_zero() {
                break;
            }
            thread::sleep(remaining);
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
#[derive(Clone)]
struct ForcedExitClaim {
    claimed: Arc<AtomicBool>,
}

impl Default for ForcedExitClaim {
    fn default() -> Self {
        Self {
            claimed: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl ForcedExitClaim {
    /// Claims the exit for the caller, and returns false when another forced exit already has it.
    fn claim(&self) -> bool {
        let already_claimed = self.claimed.swap(true, Ordering::AcqRel);
        !already_claimed
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
    /// alive. When another forced exit has already claimed the process, this one waits for that
    /// exit instead of reporting a status the process will not exit with.
    fn exit(self, claim: &ForcedExitClaim) -> ! {
        if !claim.claim() {
            loop {
                thread::park();
            }
        }
        let status = self.status();
        let watchdog = thread::Builder::new()
            .name("nervix-forced-exit".to_string())
            .spawn(move || Self::exit_after_report_budget(status));
        let Ok(_detached_watchdog) = watchdog else {
            // Nothing could bound the log record, and the exit itself must stay bounded.
            low_level::exit(status)
        };
        self.report(status);
        low_level::exit(status)
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

    fn exit_after_report_budget(status: c_int) {
        thread::sleep(FORCED_EXIT_REPORT_BUDGET);
        low_level::exit(status);
    }
}

#[cfg(test)]
mod tests {
    use signal_hook::consts::SIGHUP;

    use super::*;

    #[test]
    fn the_first_signal_requests_graceful_shutdown() {
        let shutdown = ShutdownCoordinator::default();
        let mut supervision = SignalSupervision::new(shutdown.clone(), ForcedExitClaim::default());

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
        let mut supervision = SignalSupervision::new(shutdown, ForcedExitClaim::default());
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
        let mut supervision = SignalSupervision::new(shutdown, ForcedExitClaim::default());

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
        let signal_supervisor = ForcedExitClaim::default();
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
