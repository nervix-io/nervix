//! The command line of the paced simulation's Rust driver.
//!
//! Layer: edges.
//!
//! - **Owns.** Reading the command line, running the simulation it describes, starting the
//!   diagnostic detector when selected, and the exit status of how the run ended.
//! - **Depends on.** The driver library beside it and the selected deadlock diagnostics.
//! - **Must not know.** Anything the library owns.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "an example client application owns its session, its loops and its files"
    )
)]

use std::process::ExitCode;

use clap::Parser as _;
use nervix_paced_simulation::{Options, run};

nervix_primitives::product_binary!("nervix-paced-simulation", diagnostic);

fn main() -> ExitCode {
    #[cfg(feature = "deloxide")]
    {
        let evidence = std::env::var_os("NERVIX_DEADLOCK_EVIDENCE")
            .map(nervix_deadlock::EvidenceDirectory::new);
        if let Err(error) = nervix_deadlock::DiagnosticRun::start(evidence) {
            eprintln!("cannot start the paced driver's deadlock diagnostics: {error:?}");
            std::process::exit(nervix_deadlock::DIAGNOSTIC_FAILURE_EXIT_STATUS);
        }
    }
    run_driver()
}

#[nervix_primitives::main]
async fn run_driver() -> ExitCode {
    let options = Options::parse();
    let finish = run(options).await;
    finish.exit_code()
}
