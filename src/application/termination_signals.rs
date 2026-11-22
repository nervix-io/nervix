//! What the server process does when it is asked to terminate.
//!
//! Layer: edges.
//!
//! - **Owns.** SIGINT and SIGTERM for the whole life of the process: registering them, turning the
//!   first one into a graceful stop request, and turning every later one into an immediate forced
//!   exit whose status names the signal that forced it.
//! - **Depends on.** `signal-hook` for registration and delivery, and the shutdown coordinator's
//!   stop request and phase.
//! - **Must not know.** Which services observe a shutdown phase, how long a phase may take, or what
//!   work a forced exit abandons.
//!
//! Signal delivery runs on a dedicated operating-system thread rather than a Tokio task. A forced
//! exit is for a process whose graceful shutdown has stopped making progress, and such a process
//! may have no runtime worker free to poll a task; a thread blocked reading the signal pipe still
//! wakes.
//!
//! Nothing unregisters the signals. Removing the handler would not restore the default action: the
//! signal registry keeps its own handler installed, so a later signal would simply be ignored.
//! Supervision therefore lasts until the process exits, and the first signal does not end it.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{ffi::c_int, time::Duration};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_primitives::thread;
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
    low_level,
};
use tracing::{info, warn};

use super::{
    error::AppError,
    shutdown::{ShutdownCoordinator, ShutdownPhase, ShutdownRequestOutcome},
};

/// A shell reports a process that a signal terminated as exiting with this base plus the signal
/// number, and a forced exit reports itself the same way.
const SIGNALLED_EXIT_STATUS_BASE: c_int = 128;
const INTERRUPT_EXIT_STATUS: c_int = SIGNALLED_EXIT_STATUS_BASE + SIGINT;
const TERMINATE_EXIT_STATUS: c_int = SIGNALLED_EXIT_STATUS_BASE + SIGTERM;

/// How long a forced exit waits for its log record before exiting without it. Writing the record
/// normally takes microseconds; the bound matters only when standard output has stopped draining.
const FORCED_EXIT_REPORT_BUDGET: Duration = Duration::from_secs(1);

const SIGNAL_SUPERVISOR_THREAD: &str = "nervix-termination-signals";
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

    /// Delivers every registered signal to `shutdown` on a dedicated thread until the process
    /// exits.
    pub(in crate::application) fn supervise(
        self,
        shutdown: ShutdownCoordinator,
    ) -> Result<(), Report<AppError>> {
        let signals = SignalSupervision::new(shutdown, ServerProcess);
        // The signal supervisor must outlive every other part of the process, because a stopped
        // supervisor would leave both signals ignored.
        thread::spawn_detached(SIGNAL_SUPERVISOR_THREAD, move || self.deliver_to(signals))
            .change_context(AppError::SuperviseTerminationSignals)?;
        Ok(())
    }

    fn deliver_to<P: ProcessExit + Clone>(mut self, mut supervision: SignalSupervision<P>) {
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

/// How a forced exit ends the process it runs in.
///
/// Supervision reaches the process only through this boundary, and the server names
/// [`ServerProcess`] once, where supervision starts. A test supervises a process that records how
/// it ended instead, so the forced exit it checks is the one the server runs.
trait ProcessExit: Send + Sync + 'static {
    /// Ends the process at once with `status`. No destructor, exit handler, or other thread runs
    /// afterwards.
    fn exit(&self, status: c_int) -> !;
}

/// The operating-system process the server runs as.
#[derive(Clone, Copy)]
struct ServerProcess;

impl ProcessExit for ServerProcess {
    fn exit(&self, status: c_int) -> ! {
        low_level::exit(status)
    }
}

/// The termination signals one process has received so far, and the coordinator they stop.
struct SignalSupervision<P> {
    shutdown: ShutdownCoordinator,
    first_signal: Option<TerminationSignal>,
    process: P,
}

impl<P: ProcessExit + Clone> SignalSupervision<P> {
    fn new(shutdown: ShutdownCoordinator, process: P) -> Self {
        Self {
            shutdown,
            first_signal: None,
            process,
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
            first,
            repeated: signal,
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
            SignalDisposition::ForcedExit(forced_exit) => forced_exit.exit(&self.process),
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

/// A forced end of the process by a termination signal after the one that started graceful
/// shutdown, and the graceful shutdown it abandons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ForcedExit {
    /// The signal that started graceful shutdown.
    first: TerminationSignal,
    /// The signal that forces the exit.
    repeated: TerminationSignal,
    /// The phase graceful shutdown had reached when it was abandoned.
    phase: ShutdownPhase,
}

impl ForcedExit {
    fn status(self) -> c_int {
        self.repeated.forced_exit_status()
    }

    /// Ends the process at once. No destructor, exit handler, or remaining shutdown phase runs, so
    /// work still in progress is abandoned exactly as a crash would abandon it.
    ///
    /// The exit is logged first, within a bound: a watchdog thread exits with the same status once
    /// the budget passes, so standard output that has stopped draining cannot keep the process
    /// alive.
    fn exit<P: ProcessExit + Clone>(self, process: &P) -> ! {
        let status = self.status();
        let watchdog_process = process.clone();
        let watchdog = thread::spawn_detached(FORCED_EXIT_WATCHDOG_THREAD, move || {
            Self::exit_after_report_budget(status, &watchdog_process);
        });
        let Ok(()) = watchdog else {
            // Nothing could bound the log record, and the exit itself must stay bounded.
            process.exit(status)
        };
        self.report(status);
        process.exit(status)
    }

    fn report(self, status: c_int) {
        warn!(
            signal = %self.repeated,
            first_signal = %self.first,
            phase = ?self.phase,
            exit_status = status,
            "repeated termination signal received; abandoning graceful shutdown"
        );
    }

    fn exit_after_report_budget<P: ProcessExit>(status: c_int, process: &P) {
        thread::sleep(FORCED_EXIT_REPORT_BUDGET);
        process.exit(status);
    }
}

#[cfg(test)]
mod tests {
    use nervix_primitives::{sync::blocking::mpsc, time::Instant};
    use nervix_recovery::NoReceiver as _;
    use signal_hook::consts::SIGHUP;

    use super::*;

    /// How long a test waits for a forced exit to report how its thread stopped. Only a stalled
    /// test waits it out, because every wait ends as soon as the report arrives.
    const FORCED_EXIT_WAIT: Duration = Duration::from_secs(60);

    /// How one thread that went through a forced exit stopped: it ended the process with this
    /// status.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct Exited(c_int);

    /// A process whose exits stop only the calling thread: each reports how the thread stopped,
    /// then unwinds it, so a forced exit runs to its end without ending the test binary.
    #[derive(Clone)]
    struct UnwindingProcess {
        endings: mpsc::Sender<Exited>,
    }

    impl UnwindingProcess {
        fn with_endings() -> (Self, mpsc::Receiver<Exited>) {
            let (endings, reported) = mpsc::channel();
            (Self { endings }, reported)
        }
    }

    impl ProcessExit for UnwindingProcess {
        fn exit(&self, status: c_int) -> ! {
            let ending = Exited(status);
            // A watchdog can fire after the test that started its forced exit has returned.
            self.endings
                .send(ending)
                .means_peer_left("the test that already observed the endings it asserts on");
            std::panic::resume_unwind(Box::new(ending))
        }
    }

    /// Runs `forced_exit` on a thread of its own, which the forced exit stops, and returns how that
    /// thread stopped.
    fn ending_of(forced_exit: impl FnOnce() + Send + 'static) -> Exited {
        let joined = nervix_primitives::thread::spawn(forced_exit).join();
        let Err(unwound) = joined else {
            panic!("a forced exit must not return to its thread");
        };
        *unwound
            .downcast::<Exited>()
            .expect("an unwinding process stops a thread with how it stopped")
    }

    #[test]
    fn the_first_signal_requests_graceful_shutdown() {
        let shutdown = ShutdownCoordinator::default();
        let mut supervision = SignalSupervision::new(shutdown.clone(), ServerProcess);

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
        let mut supervision = SignalSupervision::new(shutdown, ServerProcess);
        let first = supervision.receive(TerminationSignal::Interrupt);
        assert!(
            matches!(first, SignalDisposition::GracefulShutdown { .. }),
            "the first signal must not force an exit, got {first:?}"
        );

        let second = supervision.receive(TerminationSignal::Terminate);
        let third = supervision.receive(TerminationSignal::Interrupt);

        let expected_second = ForcedExit {
            first: TerminationSignal::Interrupt,
            repeated: TerminationSignal::Terminate,
            phase: ShutdownPhase::StopRequested,
        };
        let expected_third = ForcedExit {
            first: TerminationSignal::Interrupt,
            repeated: TerminationSignal::Interrupt,
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
        let mut supervision = SignalSupervision::new(shutdown, ServerProcess);

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
    fn only_the_registered_signals_are_termination_signals() {
        for signal in TerminationSignal::ALL {
            assert_eq!(TerminationSignal::from_repr(signal.number()), Some(signal));
        }
        assert_eq!(TerminationSignal::from_repr(SIGHUP), None);
    }

    #[test]
    fn the_forced_exit_watchdog_exits_with_the_same_status_once_the_report_budget_passes() {
        let (process, endings) = UnwindingProcess::with_endings();
        let forced_exit = ForcedExit {
            first: TerminationSignal::Terminate,
            repeated: TerminationSignal::Interrupt,
            phase: ShutdownPhase::StopRequested,
        };
        let started = Instant::now();

        let exiting = ending_of(move || {
            forced_exit.exit(&process);
        });
        let first_report = endings
            .recv_timeout(FORCED_EXIT_WAIT)
            .expect("the forced exit and its watchdog both report their exit");
        let second_report = endings
            .recv_timeout(FORCED_EXIT_WAIT)
            .expect("the forced exit and its watchdog both report their exit");

        assert_eq!(exiting, Exited(INTERRUPT_EXIT_STATUS));
        assert_eq!(first_report, Exited(INTERRUPT_EXIT_STATUS));
        assert_eq!(second_report, Exited(INTERRUPT_EXIT_STATUS));
        assert!(
            started.elapsed() >= FORCED_EXIT_REPORT_BUDGET,
            "the watchdog must wait out the report budget before it exits"
        );
    }

    #[test]
    fn a_repeated_signal_ends_the_process_with_the_status_of_the_repeated_signal() {
        let (process, _endings) = UnwindingProcess::with_endings();
        let shutdown = ShutdownCoordinator::default();
        let mut supervision = SignalSupervision::new(shutdown.clone(), process);

        let ending = ending_of(move || {
            supervision.deliver(TerminationSignal::Terminate);
            supervision.deliver(TerminationSignal::Interrupt);
        });

        assert_eq!(ending, Exited(INTERRUPT_EXIT_STATUS));
        assert_eq!(shutdown.phase(), ShutdownPhase::StopRequested);
    }
}
