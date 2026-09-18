#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use clap::Parser;
use error_stack::{Report, ResultExt as _};
use nervix_server::application::{AppError, Args, TerminationSignals, init_tracing, run_cli};
use tokio::runtime::Builder;

fn main() -> Result<(), Report<AppError>> {
    let args = Args::parse();
    // Registered before any other thread exists, so neither SIGINT nor SIGTERM can end the process
    // by its default action at any later point in its life.
    let termination_signals = TerminationSignals::register()?;
    let runtime = Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024) // Set custom stack size here
        .enable_all()
        .build()
        .change_context(AppError::BuildRuntime)?;

    let _tracing_guard = init_tracing(&args)?;
    runtime.block_on(run_cli(args, termination_signals))
}
