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
//! Delivery runs on a dedicated operating-system thread rather than a Tokio task. A forced exit is
//! for a process whose graceful shutdown has stopped making progress, and such a process may have
//! no runtime worker free to poll a task; a thread blocked reading the signal pipe still wakes.
//!
//! Nothing unregisters the signals. Removing the handler would not restore the default action: the
//! signal registry keeps its own handler installed, so a later signal would simply be ignored.
//! Supervision therefore lasts until the process exits, and the first signal does not end it.

use std::{ffi::c_int, thread, time::Duration};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
    low_level,
};
use tracing::{info, warn};

use super::{
    error::AppError,
    shutdown::{ShutdownCoordinator, ShutdownDeadline, ShutdownPhase, ShutdownRequestOutcome},
};

/// A shell reports a process that a signal terminated as exiting with this base plus the signal
/// number, and a forced exit reports itself the same way.
const SIGNALLED_EXIT_STATUS_BASE: c_int = 128;
const INTERRUPT_EXIT_STATUS: c_int = SIGNALLED_EXIT_STATUS_BASE + SIGINT;
const TERMINATE_EXIT_STATUS: c_int = SIGNALLED_EXIT_STATUS_BASE + SIGTERM;

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

    /// Delivers every registered signal to `shutdown` on a dedicated thread until the process exits.
    pub(in crate::application) fn supervise(
        self,
        shutdown: ShutdownCoordinator,
    ) -> Result<(), Report<AppError>> {
        let supervision = SignalSupervision::new(shutdown);
        let supervisor = thread::Builder::new()
            .name("nervix-termination-signals".to_string())
            .spawn(move || self.deliver_to(supervision))
            .change_context(AppError::SuperviseTerminationSignals)?;
        // Never joined: the supervisor must outlive every other part of the process, because a
        // stopped supervisor would leave both signals ignored.
        drop(supervisor);
        Ok(())
    }

    fn deliver_to(mut self, mut supervision: SignalSupervision) {
        for number in self.signals.forever() {
            let signal = TerminationSignal::from_repr(number)
                .assured("signal-hook delivers only the signals this registration named");
            supervision.receive(signal).carry_out();
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
}

impl SignalSupervision {
    fn new(shutdown: ShutdownCoordinator) -> Self {
        Self {
            shutdown,
            first_signal: None,
        }
    }

    /// Decides what one received signal asks for: the first starts graceful shutdown, and every
    /// later one forces the process to exit.
    fn receive(&mut self, signal: TerminationSignal) -> SignalDisposition {
        let Some(first) = self.first_signal else {
            self.first_signal = Some(signal);
            let request = self.shutdown.request_stop(ShutdownDeadline::Unbounded);
            return SignalDisposition::GracefulShutdown { signal, request };
        };
        SignalDisposition::ForcedExit(ForcedExit {
            first,
            repeated: signal,
            phase: self.shutdown.phase(),
        })
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

impl SignalDisposition {
    /// Carries the disposition out. A forced exit does not return.
    fn carry_out(self) {
        match self {
            Self::GracefulShutdown {
                signal,
                request: ShutdownRequestOutcome::Accepted(_),
            } => {
                info!(
                    %signal,
                    "termination signal received; requesting graceful shutdown"
                );
            }
            Self::GracefulShutdown {
                signal,
                request: ShutdownRequestOutcome::AlreadyRequested(_),
            } => {
                info!(
                    %signal,
                    "termination signal received while graceful shutdown is already in progress"
                );
            }
            Self::ForcedExit(forced_exit) => forced_exit.exit(),
        }
    }
}

/// A repeated termination signal, and the graceful shutdown it abandons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ForcedExit {
    first: TerminationSignal,
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
    fn exit(self) -> ! {
        let status = self.status();
        let watchdog = thread::Builder::new()
            .name("nervix-forced-exit".to_string())
            .spawn(move || Self::exit_after_report_budget(status));
        let Ok(_detached_watchdog) = watchdog else {
            // Nothing could bound the log record, and the exit itself must stay bounded.
            low_level::exit(status)
        };
        warn!(
            signal = %self.repeated,
            first_signal = %self.first,
            phase = ?self.phase,
            exit_status = status,
            "repeated termination signal received; abandoning graceful shutdown"
        );
        low_level::exit(status)
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
        let mut supervision = SignalSupervision::new(shutdown.clone());

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
        let mut supervision = SignalSupervision::new(shutdown);
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
        shutdown.request_stop(ShutdownDeadline::Unbounded);
        let request = shutdown
            .request()
            .expect("the stop request above must be recorded");
        let mut supervision = SignalSupervision::new(shutdown);

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
}
