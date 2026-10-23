//! The diagnostic run of a process built for the `deloxide` mode.
//!
//! A run installs the deadlock detector with a recorder as its sink, then records the process's
//! evidence without findings. Potential cycles accumulate with bounded deduplication and leave the
//! workload running; an active cycle or diagnostic loss ends the process. Each delivery writes a
//! bounded description and atomically replaces the cumulative local evidence. Standard error uses
//! its descriptor directly. Terminal outcomes bypass exit handlers, and a deadline thread exits
//! without writing if a blocked output or filesystem outlives the recording budget.

use std::{
    io,
    num::NonZeroU64,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    deadlock::{self, BoundedText, DiagnosticSelection, Finding},
    sync::blocking::{OnceLock, mpsc},
    thread,
};

use crate::{
    ACTIVE_DEADLOCK_EXIT_STATUS, DIAGNOSTIC_FAILURE_EXIT_STATUS, EvidenceLossSource, MAX_FINDINGS,
    RecordedFinding,
    directory::EvidenceDirectory,
    error::DiagnosticError,
    evidence::{DeadlockEvidence, ProcessRecord},
    render::render_finding,
};

/// How long recording a finding may take before the process ends without it. Rendering a bounded
/// description and writing one bounded file normally take milliseconds; the budget matters only
/// when standard error or the evidence directory has stopped accepting writes.
const RECORDING_BUDGET: Duration = Duration::from_secs(10);
/// Repeated source changes must also fit an explicit process output budget.
const CONSOLE_BYTE_CAPACITY: usize = 128 * 1024 * 1024;

const RECORDING_DEADLINE_THREAD: &str = "nervix-deadlock-recording-deadline";

/// This process's run, once it started.
static CURRENT: OnceLock<DiagnosticRun> = OnceLock::new();

/// The diagnostic run of this process, with a selected detector and cumulative local evidence.
#[derive(Debug)]
pub struct DiagnosticRun {
    process: ProcessRecord,
    evidence_file: Option<PathBuf>,
}

/// Whether the run recorded its starting evidence, which the recorder waits for before it records a
/// finding over it.
enum Started {
    Recorded,
    Failed,
}

impl DiagnosticRun {
    /// Start this process's diagnostic run, recording evidence in `directory` when one is given.
    ///
    /// Start once, after the process has registered the signals it must not lose, and before it
    /// constructs a tracked lock or starts a runtime worker. A process whose run fails to start
    /// must end: its tracked locks refuse to run without the detector.
    pub fn start(
        directory: Option<EvidenceDirectory>,
    ) -> Result<&'static Self, Report<DiagnosticError>> {
        Self::start_selected(directory, DiagnosticSelection::for_build(false))
    }

    pub fn start_selected(
        directory: Option<EvidenceDirectory>,
        selection: DiagnosticSelection,
    ) -> Result<&'static Self, Report<DiagnosticError>> {
        let process = ProcessRecord {
            id: std::process::id(),
            program: program_name(),
            started_at: SystemTime::now(),
            selection,
        };
        let evidence = DeadlockEvidence::started(process.clone());
        let (started_sender, started) = mpsc::channel();
        let mut recorder = Recorder {
            evidence: Some(evidence.clone()),
            directory: directory.clone(),
            started: Some(started),
            printed_bytes: 0,
        };
        deadlock::install_selected(selection, move |finding| recorder.record(finding))
            .change_context(DiagnosticError::Install)?;
        let evidence_file = match &directory {
            Some(directory) => match directory.record(&evidence) {
                Ok(file) => Some(file),
                Err(error) => {
                    started_sender
                        .send(Started::Failed)
                        .assured("the recorder keeps its receiver for the life of the process");
                    return Err(error.change_context(DiagnosticError::RecordStart));
                }
            },
            None => None,
        };
        started_sender
            .send(Started::Recorded)
            .assured("the recorder keeps its receiver for the life of the process");
        let run = Self {
            process,
            evidence_file,
        };
        CURRENT
            .set(run)
            .assured("the detector installs once per process, so a run starts once");
        Ok(CURRENT
            .get()
            .verified("the run was stored in the line above"))
    }

    /// This process's run, once it started.
    pub fn current() -> Option<&'static Self> {
        CURRENT.get()
    }

    pub fn process(&self) -> &ProcessRecord {
        &self.process
    }

    /// The file the run records its evidence in, when it has an evidence directory.
    pub fn evidence_file(&self) -> Option<&Path> {
        self.evidence_file.as_deref()
    }
}

/// The detector's sink.
struct Recorder {
    /// Cumulative bounded evidence, present after the run starts.
    evidence: Option<DeadlockEvidence>,
    directory: Option<EvidenceDirectory>,
    started: Option<mpsc::Receiver<Started>>,
    printed_bytes: usize,
}

impl Recorder {
    /// Record `finding`; only active cycles, loss or recording failure end the process.
    fn record(&mut self, finding: Finding) {
        let deadline = match RecordingDeadline::start() {
            Ok(deadline) => deadline,
            Err(error) => {
                write_stderr(&format!(
                    "nervix deadlock detector: the recording deadline could not start: {error}; \
                     ending the diagnostic process\n"
                ));
                signal_hook::low_level::exit(DIAGNOSTIC_FAILURE_EXIT_STATUS);
            }
        };
        let mut finding = RecordedFinding::from(finding);
        // At most MAX_FINDINGS records are searched; repeated callbacks update cumulative
        // evidence, and only the first description consumes console output.
        let repeated = match &self.evidence {
            Some(evidence) => evidence
                .findings()
                .iter()
                .find(|recorded| recorded.same_potential(&finding))
                .is_some_and(|recorded| {
                    let mut combined = recorded.clone();
                    match combined.merge(&finding) {
                        Ok(changed) => !changed,
                        Err(_) => false,
                    }
                }),
            None => false,
        };
        let mut console_ok = true;
        if !repeated {
            let description = render_finding(&finding);
            match self.printed_bytes.checked_add(description.len()) {
                Some(total) if total <= CONSOLE_BYTE_CAPACITY => {
                    self.printed_bytes = total;
                    console_ok = write_stderr(&description);
                }
                _ => {
                    finding = RecordedFinding::Overflow {
                        lost: NonZeroU64::MIN,
                        source: EvidenceLossSource::Retention,
                    };
                    console_ok = write_stderr(&render_finding(&finding));
                }
            }
        }
        let status = match &finding {
            RecordedFinding::ActiveCycle(_) => Some(ACTIVE_DEADLOCK_EXIT_STATUS),
            RecordedFinding::Potential { .. } => None,
            RecordedFinding::Overflow { .. } => Some(DIAGNOSTIC_FAILURE_EXIT_STATUS),
        };
        let status = match self.record_evidence(finding) {
            Recorded::InEvidence(file) => {
                if !repeated {
                    console_ok &= write_stderr(&format!(
                        "nervix deadlock detector: evidence recorded at {}\n",
                        file.display()
                    ));
                }
                status
            }
            Recorded::OnStandardError => status,
            Recorded::Failed(reason) => {
                write_stderr(&format!(
                    "nervix deadlock detector: the evidence could not be recorded: {reason}\n"
                ));
                Some(DIAGNOSTIC_FAILURE_EXIT_STATUS)
            }
        };
        deadline.cancel();
        let status = if console_ok {
            status
        } else {
            Some(DIAGNOSTIC_FAILURE_EXIT_STATUS)
        };
        if let Some(status) = status {
            signal_hook::low_level::exit(status);
        }
    }

    fn record_evidence(&mut self, finding: RecordedFinding) -> Recorded {
        if let Some(started) = self.started.take() {
            match started.recv() {
                Ok(Started::Recorded) => {}
                Ok(Started::Failed) | Err(_) => {
                    return Recorded::Failed("the run did not record its starting evidence".into());
                }
            }
        }
        let Some(evidence) = self.evidence.take() else {
            return Recorded::Failed("the recorder has no current evidence".into());
        };
        let repeated = evidence
            .findings()
            .iter()
            .any(|recorded| recorded.same_potential(&finding));
        // Reserve one record for explicit retention overload. An overload artifact must never
        // remain the preceding clean or reviewed evidence after the process exits with failure.
        let retention_full = !repeated
            && matches!(finding, RecordedFinding::Potential { .. })
            && evidence.findings().len() >= MAX_FINDINGS - 1;
        let recorded = if retention_full {
            Err(crate::EvidenceOutOfBounds::TooManyFindings {
                findings: evidence.findings().len(),
            })
        } else {
            evidence.clone().with_finding(finding)
        };
        let evidence = match recorded {
            Ok(evidence) => evidence,
            Err(bounds) => {
                let loss = RecordedFinding::Overflow {
                    lost: NonZeroU64::MIN,
                    source: EvidenceLossSource::Retention,
                };
                write_stderr(&render_finding(&loss));
                let overloaded = evidence
                    .with_finding(loss)
                    .assured("retention always reserves one record for overload");
                self.evidence = Some(overloaded.clone());
                if let Some(directory) = &self.directory
                    && let Err(error) = directory.record(&overloaded)
                {
                    return Recorded::Failed(format!("{bounds}; {error:#}"));
                }
                return Recorded::Failed(bounds.to_string());
            }
        };
        self.evidence = Some(evidence.clone());
        let Some(directory) = &self.directory else {
            return Recorded::OnStandardError;
        };
        match directory.record(&evidence) {
            Ok(file) => Recorded::InEvidence(file),
            Err(error) => Recorded::Failed(format!("{error:#}")),
        }
    }
}

/// Where a finding ended up.
enum Recorded {
    InEvidence(PathBuf),
    /// The run has no evidence directory: the description on standard error is the record.
    OnStandardError,
    Failed(String),
}

/// End the process if recording a finding outlives its budget.
struct RecordingDeadline {
    cancelled: mpsc::Sender<()>,
    worker: thread::JoinHandle<()>,
}

impl RecordingDeadline {
    fn start() -> Result<Self, io::Error> {
        let (cancelled, cancellation) = mpsc::channel();
        let worker = thread::Builder::new()
            .name(RECORDING_DEADLINE_THREAD.into())
            .spawn(move || {
                match cancellation.recv_timeout(RECORDING_BUDGET) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {}
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        // The recorder may be blocked writing this very descriptor. The deadline
                        // cannot depend on another write completing before it ends the process.
                        signal_hook::low_level::exit(DIAGNOSTIC_FAILURE_EXIT_STATUS);
                    }
                }
            })?;
        Ok(Self { cancelled, worker })
    }

    fn cancel(self) {
        self.cancelled
            .send(())
            .assured("the watchdog retains its receiver until cancellation or process exit");
        self.worker
            .join()
            .assured("the watchdog only receives cancellation or exits the process");
    }
}

/// Write `text` to the standard error descriptor directly, without the lock the standard library
/// takes around it: a blocked thread could hold that lock.
fn write_stderr(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut written = 0;
    while written < bytes.len() {
        match nix::unistd::write(io::stderr(), &bytes[written..]) {
            Ok(0) => return false,
            Err(nix::errno::Errno::EINTR) => continue,
            Err(_) => return false,
            Ok(count) => {
                written = written
                    .checked_add(count)
                    .assured("a write reports at most the bytes it was given");
            }
        }
    }
    true
}

/// The name of the program this process runs, as its first argument names it.
fn program_name() -> Option<BoundedText> {
    let first = std::env::args_os().next()?;
    let name = Path::new(&first).file_name()?;
    Some(BoundedText::new(&name.to_string_lossy()))
}

#[cfg(test)]
mod tests {
    use nervix_primitives::deadlock::Access;

    use super::*;
    use crate::{PotentialTriage, order_tests::cycle};

    #[test]
    fn retention_overload_replaces_the_artifact_with_explicit_loss() {
        let root = tempfile::tempdir().assured("disposable evidence");
        let directory = EvidenceDirectory::new(root.path());
        let process = ProcessRecord {
            selection: DiagnosticSelection::OrderAnalysis,
            ..crate::tests::process()
        };
        let mut recorder = Recorder {
            evidence: Some(DeadlockEvidence::started(process)),
            directory: Some(directory.clone()),
            started: None,
            printed_bytes: 0,
        };
        for index in 0..MAX_FINDINGS - 1 {
            let finding = RecordedFinding::Potential {
                cycle: cycle(
                    u64::try_from(index * 2).assured("the bound is small"),
                    Access::Exclusive,
                ),
                repetitions: NonZeroU64::MIN,
                triage: PotentialTriage::Unreviewed,
            };
            assert!(matches!(
                recorder.record_evidence(finding),
                Recorded::InEvidence(_)
            ));
        }
        let refused = RecordedFinding::Potential {
            cycle: cycle(100, Access::Exclusive),
            repetitions: NonZeroU64::MIN,
            triage: PotentialTriage::Unreviewed,
        };
        assert!(matches!(
            recorder.record_evidence(refused),
            Recorded::Failed(_)
        ));
        let recorded = directory
            .read_all()
            .assured("the current artifact is readable");
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].findings().len(), MAX_FINDINGS);
        assert!(matches!(
            recorded[0].findings().last(),
            Some(RecordedFinding::Overflow {
                source: EvidenceLossSource::Retention,
                ..
            })
        ));
        assert!(!recorded[0].qualifies());
    }
}
