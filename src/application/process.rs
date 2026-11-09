//! Process and caller-owned runtime entry points at the server edge.
//!
//! Layer: edges.
//! - **Owns.** Command-line startup, native runtime construction and application supervision.
//! - **Depends on.** Parsed arguments, resolved runtime settings, tracing and termination signals.
//! - **Must not know.** Graph planning, branch routing, connector internals or stored shapes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns process startup at the server edge")
)]

#[cfg(not(feature = "shuttle"))]
use clap::Parser as _;
use error_stack::Report;
#[cfg(not(feature = "shuttle"))]
use error_stack::ResultExt as _;

use super::{
    AppError, Application, Args, Command, TerminationSignals, TracingGuard,
    cli_completions::print_completions,
};
#[cfg(not(feature = "shuttle"))]
use super::{RuntimeThreadOverrides, RuntimeThreadSettings, init_tracing};

/// Runs this process as the server, from its own command line to its exit. The `nervix-server`
/// executable is this call, and a test process that re-executes itself under the server's name
/// makes the same call, so a real process in a test starts the way a deployed one does. No Shuttle
/// model runs a whole process and Shuttle's runtime has no stack size to set, so a Shuttle build has
/// no such entry point.
#[cfg(not(feature = "shuttle"))]
pub fn run_server_process() -> Result<(), Report<AppError>> {
    let args = Args::parse();
    // Registered before any other thread exists, so neither SIGINT nor SIGTERM can end the process
    // by its default action at any later point in its life.
    let termination_signals = TerminationSignals::register()?;
    // A diagnostic node installs its deadlock detector before any tracked lock or runtime worker
    // exists, and after the signals, so the detector's threads never see one by its default action.
    #[cfg(feature = "deloxide")]
    nervix_deadlock::DiagnosticRun::start_selected(
        args.diagnostics
            .deadlock_evidence
            .clone()
            .map(nervix_deadlock::EvidenceDirectory::new),
        nervix_primitives::deadlock::DiagnosticSelection::for_build(
            args.diagnostics.deadlock_active_only,
        ),
    )
    .change_context(AppError::StartDeadlockDiagnostics)?;
    if let Some(Command::Completions { shell }) = args.subcommand.clone() {
        let cpus = crate::capacity::CpuCount::detect();
        let workers = nervix_execution::WorkerSettings::resolve(
            &nervix_execution::ExecutionOverrides::default(),
            cpus,
        )
        .change_context(AppError::InvalidExecutionSettings)?
        .counts();
        let settings =
            RuntimeThreadSettings::resolve(RuntimeThreadOverrides::default(), cpus, workers, None)
                .change_context(AppError::InvalidRuntimeCapacity)?;
        let runtime = settings.build().change_context(AppError::BuildRuntime)?;
        let tracing_guard = {
            let _entered = runtime.enter();
            init_tracing(&args.tracing)?
        };
        runtime.block_on(async {
            print_completions(shell);
        });
        drop(tracing_guard);
        return Ok(());
    }
    let tracing_options = args.tracing.clone();
    let mut application = Application::try_from(args)?;
    let cpus = application
        .settings
        .capacity
        .effective_cpu_count(crate::capacity::CpuCount::detect());
    application.cpu_detection = crate::capacity::CpuDetection::Resolved(cpus);
    let workers = nervix_execution::WorkerSettings::resolve(&application.settings.execution, cpus)
        .change_context(AppError::InvalidExecutionSettings)?
        .counts();
    let memory = application
        .settings
        .capacity
        .memory_limit()
        .and_then(|memory| std::num::NonZeroU64::new(memory.as_u64()));
    let runtime_settings =
        RuntimeThreadSettings::resolve(application.settings.runtime_threads, cpus, workers, memory)
            .change_context(AppError::InvalidRuntimeCapacity)?;
    let runtime = runtime_settings
        .build()
        .change_context(AppError::BuildRuntime)?;
    let tracing_guard = {
        let _entered = runtime.enter();
        init_tracing(&tracing_options)?
    };
    runtime.block_on(async {
        tracing::info!(
            target: "nervix_server::application",
            worker_threads = runtime_settings.worker_threads().get(),
            max_blocking_threads = runtime_settings.max_blocking_threads().get(),
            thread_stack_size = runtime_settings.thread_stack_size().as_u64(),
            "the native runtime capacity was resolved"
        );
        termination_signals.supervise(application.shutdown.clone())?;
        application.run_with_tracing(Some(&tracing_guard)).await
    })
}

/// Runs the command line on a runtime the caller owns, with termination signals registered
/// before that runtime started. The native process entry point sizes its runtime before entering it.
pub async fn run_cli(
    args: Args,
    termination_signals: TerminationSignals,
    tracing: &TracingGuard,
) -> Result<(), Report<AppError>> {
    if let Some(Command::Completions { shell }) = args.subcommand.clone() {
        print_completions(shell);
        return Ok(());
    }
    let application = Application::try_from(args)?;
    termination_signals.supervise(application.shutdown.clone())?;
    application.run_with_tracing(Some(tracing)).await
}
