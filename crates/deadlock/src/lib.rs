//! Deadlock diagnostics for a diagnostic Nervix process.
//!
//! A build that selects the `deloxide` mode tracks every thread-blocking lock in one process-wide
//! wait-for graph. A process built that way starts a [`DiagnosticRun`] before it uses a tracked lock:
//! the run installs the deadlock detector, and on the first active deadlock the detector reports it
//! writes a bounded description to standard error, records the finding as [`DeadlockEvidence`] in
//! its evidence directory, and ends the process with [`ACTIVE_DEADLOCK_EXIT_STATUS`]. A deadlocked
//! process cannot make progress on the work its blocked threads hold, so a diagnostic process ends
//! rather than run on half stopped. A run whose recording fails ends with
//! [`DIAGNOSTIC_FAILURE_EXIT_STATUS`] instead: a diagnostic execution that failed is never taken for
//! one that found nothing.
//!
//! An evidence file is written when the run starts, describing the process and no findings, and
//! replaced when a finding is recorded, so the directory proves the detector ran even in a process
//! that ends cleanly. The file is rkyv behind a header of its own: a magic, a record kind and a
//! format version, checked before bytecheck validates a single field. Every build reads and
//! describes evidence; only a `deloxide` build records it.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The evidence format and its bounds, the evidence directory's file names and their
//!   atomic replacement, the bounded description a finding renders as, the diagnostic run's
//!   lifecycle, and the exit statuses that end a diagnostic process.
//! - **Depends on.** The deadlock findings and detector of `nervix-primitives`, `rkyv`, and the
//!   operating system's process exit.
//! - **Must not know.** Which locks or owners deadlocked, the graph, the node's lifecycle or its
//!   signals: the process that starts a run decides where its evidence goes, and nothing here waits
//!   for a lock a blocked thread could hold, writes a log, or reads application state.

mod directory;
mod error;
mod evidence;
mod render;
#[cfg(feature = "deloxide")]
mod run;
mod wire;

pub use directory::EvidenceDirectory;
pub use error::{DiagnosticError, EvidenceError};
pub use evidence::{DeadlockEvidence, EvidenceOutOfBounds, MAX_FINDINGS, ProcessRecord};
pub use render::render_finding;
#[cfg(feature = "deloxide")]
pub use run::DiagnosticRun;

/// The status a diagnostic process ends with once it has reported an active deadlock: its
/// description is on standard error, and in the evidence directory when the run has one.
pub const ACTIVE_DEADLOCK_EXIT_STATUS: i32 = 3;

/// The status a diagnostic process ends with when its diagnostic execution failed: a finding could
/// not be recorded, findings were lost, or recording outlived its budget. Whether the process
/// deadlocked is then unknown, or known and not recorded; it is never taken for a clean run.
pub const DIAGNOSTIC_FAILURE_EXIT_STATUS: i32 = 4;

#[cfg(test)]
mod tests;
