//! The deadlock diagnostic options of a diagnostic server build.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of where a diagnostic node records its deadlock evidence and
//!   whether it checks active cycles only.
//! - **Depends on.** Nothing beyond the command line.
//! - **Must not know.** How the detector records or checks anything.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::path::PathBuf;

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Deadlock diagnostics")]
pub struct DiagnosticArgs {
    /// Where a diagnostic node records its deadlock evidence. Without it, a deadlock the detector
    /// reports is described on standard error only.
    #[arg(
        long,
        env = "NERVIX_DEADLOCK_EVIDENCE",
        help = "Existing directory a diagnostic node records its deadlock evidence in"
    )]
    pub deadlock_evidence: Option<PathBuf>,

    /// Disable runtime order checking in an order-instrumented diagnostic artifact.
    #[clap(
        long,
        help = "Check active cycles only; compiled order instrumentation retains its cost"
    )]
    pub deadlock_active_only: bool,
}
