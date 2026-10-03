//! Loom models of production owners.
//!
//! [`explore`] runs a model once for every schedule and every reordering Loom can distinguish, with
//! no preemption bound, and only a completed search passes. The model runs under the same
//! primitives the production owner uses, because the `loom` feature selects Loom's atomics and
//! threads for the whole graph; a model built from copied logic or real atomics proves nothing
//! about the owner. Loom's own limits still apply: it does not model every relaxed behavior, and an
//! operation inside a third-party dependency stays invisible to it.
//!
//! Every run prints one record for its runner. A completed search prints
//! `nervix-model-harness: loom invariant <name> explored to exhaustion in <n> executions` followed by
//! the bounds it ran under; `just test-loom` accepts nothing else as a completed model. When
//! `LOOM_CHECKPOINT_FILE` names a checkpoint that already exists, Loom resumes the search from that
//! execution, which is how a failure is replayed, and the record says the search was resumed.
//!
//! Loom runs a model's threads as coroutines of one operating-system thread, each on a stack of a
//! few kilobytes unless it asks for more, and a production owner called from a debug build needs
//! more. [`explore`] runs each model on a thread with [`MODEL_THREAD_STACK`], and a model spawns its
//! own threads through [`spawn`], which gives them the same. A stack size decides nothing a model
//! explores.

use std::env;

use loom::{MAX_THREADS, model::Builder};
use meticulous::ResultExt as _;
use nervix_primitives::{
    sync::Arc,
    thread::{self, JoinHandle},
    unmodeled::sync::atomic::{AtomicUsize, Ordering},
};
use tracing_subscriber::{EnvFilter, fmt};

use crate::exploration::{InvariantId, LOOM_BRANCH_LIMIT, refused_loom_settings};

/// The stack of the thread each model runs on, and of every thread it spawns through [`spawn`].
pub const MODEL_THREAD_STACK: usize = 8 * 1024 * 1024;

/// Spawns a thread of a model, with a stack a production owner's calls fit in.
pub fn spawn<F, T>(task: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    thread::Builder::new()
        .stack_size(MODEL_THREAD_STACK)
        .spawn(task)
        .assured("a Loom model spawns its threads inside the model, below Loom's thread limit")
}

/// Explore `model`, which checks `invariant` against a production owner, to exhaustion.
///
/// The search has no preemption bound, stops at nothing but its end, and allows each execution
/// [`LOOM_BRANCH_LIMIT`] thread switches and Loom's full thread count. A setting in the environment
/// that would change that search is refused rather than honored. `LOOM_CHECKPOINT_FILE`,
/// `LOOM_CHECKPOINT_INTERVAL`, `LOOM_LOG` and `LOOM_LOCATION` only record and describe the search,
/// and are honored.
pub fn explore<F>(invariant: InvariantId, model: F)
where
    F: Fn() + Sync + Send + 'static,
{
    let refused = refused_loom_settings(|setting| env::var_os(setting).is_some());
    assert!(
        refused.is_empty(),
        "loom invariant {invariant} is explored to exhaustion under the bounds its harness \
         declares; unset {} to run it",
        refused.join(", ")
    );

    let mut builder = Builder::new();
    builder.preemption_bound = None;
    builder.max_permutations = None;
    builder.max_duration = None;
    builder.max_branches = LOOM_BRANCH_LIMIT;
    builder.max_threads = MAX_THREADS;
    let resumed_from = match &builder.checkpoint_file {
        Some(checkpoint) if checkpoint.exists() => Some(checkpoint.clone()),
        Some(_) | None => None,
    };
    let bounds = format!(
        "preemption bound: none, branch limit: {LOOM_BRANCH_LIMIT}, thread limit: {MAX_THREADS}"
    );
    eprintln!(
        "nervix-model-harness: exploring loom invariant {invariant} to exhaustion ({bounds})"
    );

    // A statistic of the runner, counted across executions: no execution may own it, so it is a
    // real atomic outside the model.
    let executions = Arc::new(AtomicUsize::new(0));
    let counted_executions = Arc::clone(&executions);
    let model = Arc::new(model);
    let counted_model = move || {
        counted_executions.fetch_add(1, Ordering::Relaxed);
        let execution = Arc::clone(&model);
        spawn(move || {
            let model: &F = &execution;
            model();
        })
        .join()
        .assured("a model that panics fails its execution before this join returns");
    };
    let subscriber = fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_env("LOOM_LOG"))
        .with_test_writer()
        .without_time()
        .finish();
    tracing::subscriber::with_default(subscriber, || builder.check(counted_model));

    let explored = executions.load(Ordering::Relaxed);
    match resumed_from {
        Some(checkpoint) => eprintln!(
            "nervix-model-harness: loom invariant {invariant} resumed from checkpoint {} and \
             explored {explored} executions ({bounds}); a resumed search replays a failure and \
             does not complete the invariant",
            checkpoint.display()
        ),
        None => eprintln!(
            "nervix-model-harness: loom invariant {invariant} explored to exhaustion in \
             {explored} executions ({bounds})"
        ),
    }
}
