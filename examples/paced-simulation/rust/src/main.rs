//! The command line of the paced simulation's Rust driver.
//!
//! Layer: edges.
//!
//! - **Owns.** Reading the command line, running the simulation it describes, and the exit status
//!   of how the run ended.
//! - **Depends on.** The driver library beside it.
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

nervix_primitives::product_binary!("nervix-paced-simulation");

#[nervix_primitives::main]
async fn main() -> ExitCode {
    let options = Options::parse();
    let finish = run(options).await;
    finish.exit_code()
}
