//! The diagnostic run of a process built for the `deloxide` mode.
//!
//! A run installs the deadlock detector with a recorder as its sink, then records the process's
//! evidence without findings. The recorder handles the first finding the detector delivers and ends
//! the process: it writes the finding's description to standard error, waits until the run has
//! recorded its starting evidence or failed to, records the finding in the evidence directory, and
//! exits. It writes through the standard error descriptor directly and exits without running exit
//! handlers, so nothing on the way waits for a lock a blocked thread could hold, and a deadline
//! thread ends the process if recording outlives its budget.

use std::{
    io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    deadlock::{self, BoundedText, Finding},
    sync::blocking::{OnceLock, mpsc},
    thread,
};

use crate::{
    ACTIVE_DEADLOCK_EXIT_STATUS, DIAGNOSTIC_FAILURE_EXIT_STATUS,
    directory::EvidenceDirectory,
    error::DiagnosticError,
    evidence::{DeadlockEvidence, ProcessRecord},
    render::render_finding,
};

/// How long recording a finding may take before the process ends without it. Rendering a bounded
/// description and writing one bounded file normally take milliseconds; the budget matters only
/// when standard error or the evidence directory has stopped accepting writes.
const RECORDING_BUDGET: Duration = Duration::from_secs(10);

const RECORDING_DEADLINE_THREAD: &str = "nervix-deadlock-recording-deadline";

/// This process's run, once it started.
static CURRENT: OnceLock<DiagnosticRun> = OnceLock::new();

/// The diagnostic run of this process: its detector is installed, and its first finding ends it.
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
        let process = ProcessRecord {
            id: std::process::id(),
            program: program_name(),
            started_at: SystemTime::now(),
        };
        let evidence = DeadlockEvidence::started(process.clone());
        let (started_sender, started) = mpsc::channel();
        let mut recorder = Recorder {
            evidence: Some(evidence.clone()),
            directory: directory.clone(),
            started,
        };
        deadlock::install(move |finding| recorder.record(finding))
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
    /// The evidence a finding is recorded into; taken by the first finding.
    evidence: Option<DeadlockEvidence>,
    directory: Option<EvidenceDirectory>,
    started: mpsc::Receiver<Started>,
}

impl Recorder {
    /// Record `finding` and end the process.
    fn record(&mut self, finding: Finding) {
        start_recording_deadline();
        write_stderr(&render_finding(&finding));
        let status = match &finding {
            Finding::ActiveCycle(_) => ACTIVE_DEADLOCK_EXIT_STATUS,
            Finding::Overflow { .. } => DIAGNOSTIC_FAILURE_EXIT_STATUS,
        };
        let status = match self.record_evidence(finding) {
            Recorded::InEvidence(file) => {
                write_stderr(&format!(
                    "nervix deadlock detector: evidence recorded at {}\n",
                    file.display()
                ));
                status
            }
            Recorded::OnStandardError => status,
            Recorded::Failed(reason) => {
                write_stderr(&format!(
                    "nervix deadlock detector: the evidence could not be recorded: {reason}\n"
                ));
                DIAGNOSTIC_FAILURE_EXIT_STATUS
            }
        };
        signal_hook::low_level::exit(status);
    }

    fn record_evidence(&mut self, finding: Finding) -> Recorded {
        match self.started.recv() {
            Ok(Started::Recorded) => {}
            Ok(Started::Failed) | Err(_) => {
                return Recorded::Failed("the run did not record its starting evidence".into());
            }
        }
        let Some(directory) = &self.directory else {
            return Recorded::OnStandardError;
        };
        let Some(evidence) = self.evidence.take() else {
            return Recorded::Failed("the recorder handles one finding".into());
        };
        let evidence = match evidence.with_finding(finding) {
            Ok(evidence) => evidence,
            Err(bounds) => return Recorded::Failed(bounds.to_string()),
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
fn start_recording_deadline() {
    let started = thread::spawn_detached(RECORDING_DEADLINE_THREAD, || {
        thread::sleep(RECORDING_BUDGET);
        write_stderr(
            "nervix deadlock detector: recording the finding outlived its budget; ending the \
             process\n",
        );
        signal_hook::low_level::exit(DIAGNOSTIC_FAILURE_EXIT_STATUS);
    });
    match started {
        Ok(()) => {}
        Err(error) => write_stderr(&format!(
            "nervix deadlock detector: the recording deadline could not start ({error}); \
             recording without it\n"
        )),
    }
}

/// Write `text` to the standard error descriptor directly, without the lock the standard library
/// takes around it: a blocked thread could hold that lock.
fn write_stderr(text: &str) {
    let bytes = text.as_bytes();
    let mut written = 0;
    while written < bytes.len() {
        match nix::unistd::write(io::stderr(), &bytes[written..]) {
            Ok(0) | Err(_) => return,
            Ok(count) => {
                written = written
                    .checked_add(count)
                    .assured("a write reports at most the bytes it was given");
            }
        }
    }
}

/// The name of the program this process runs, as its first argument names it.
fn program_name() -> Option<BoundedText> {
    let first = std::env::args_os().next()?;
    let name = Path::new(&first).file_name()?;
    Some(BoundedText::new(&name.to_string_lossy()))
}
