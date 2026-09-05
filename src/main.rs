use clap::Parser;
use error_stack::{Report, ResultExt as _};
use nervix_server::application::{AppError, Args, init_tracing, run_cli};
use tokio::runtime::Builder;

fn main() -> Result<(), Report<AppError>> {
    let runtime = Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024) // Set custom stack size here
        .enable_all()
        .build()
        .change_context(AppError::BuildRuntime)?;

    let args = Args::parse();
    let _tracing_guard = init_tracing(&args)?;
    runtime.block_on(run_cli(args))
}
