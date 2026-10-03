use clap::Parser;
use error_stack::{Report, ResultExt as _};
use nervix_primitives::runtime::Builder;
use nervix_server::application::{AppError, Args, TerminationSignals, init_tracing, run_cli};

nervix_primitives::product_binary!("nervix-server", diagnostic);

fn main() -> Result<(), Report<AppError>> {
    let args = Args::parse();
    // Registered before any other thread exists, so neither SIGINT nor SIGTERM can end the process
    // by its default action at any later point in its life.
    let termination_signals = TerminationSignals::register()?;
    // A diagnostic node installs its deadlock detector before any tracked lock or runtime worker
    // exists, and after the signals, so the detector's threads never see one by its default action.
    #[cfg(feature = "deloxide")]
    nervix_deadlock::DiagnosticRun::start(
        args.deadlock_evidence
            .clone()
            .map(nervix_deadlock::EvidenceDirectory::new),
    )
    .change_context(AppError::StartDeadlockDiagnostics)?;
    let runtime = Builder::new_multi_thread()
        .thread_stack_size(8 * 1024 * 1024) // Set custom stack size here
        .enable_all()
        .build()
        .change_context(AppError::BuildRuntime)?;

    let tracing_guard = {
        let _entered = runtime.enter();
        init_tracing(&args)?
    };
    runtime.block_on(run_cli(args, termination_signals, &tracing_guard))
}
