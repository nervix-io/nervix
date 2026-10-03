//! Loom models of production owners.
//!
//! [`explore`] runs a model once for every schedule and every reordering Loom can distinguish, with
//! no preemption bound, and only a completed search passes. The model runs under the same
//! primitives the production owner uses, because the `loom` feature selects Loom's atomics and
//! threads for the whole graph; a model built from copied logic or real atomics proves nothing
//! about the owner. Loom's own limits still apply: it does not model every relaxed behavior, and an
//! operation inside a third-party dependency stays invisible to it.
//!
//! The default coordinator only starts and joins the model body. The body and every participant
//! started by [`spawn`] request larger coroutine stacks, so allocator instrumentation and debug
//! frames execute on those stacks. The coordinator retains Loom's fixed default stack, so the
//! runner uses the workspace's `loom` build profile to reduce its initial allocation frames while
//! preserving debug assertions, overflow checks and allocation instrumentation. It carries no
//! protocol state and occupies one of Loom's five thread slots, alongside the body and actors.
//!
//! Every run prints one record for its runner. A completed search prints
//! `nervix-model-harness: loom invariant <name> explored to exhaustion in <n> executions` followed by
//! the bounds it ran under; `just test-loom` accepts nothing else as a completed model. When
//! `LOOM_CHECKPOINT_FILE` names a checkpoint that already exists, Loom resumes the search from that
//! execution, which is how a failure is replayed, and the record says the search was resumed.

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

/// The stack request for the model body and each participant. Loom forwards this to its coroutine
/// implementation, whose allocation units need not be bytes. Debug frames and allocator
/// instrumentation can exhaust the default stack before a model reaches its first atomic.
const MODEL_STACK_REQUEST: usize = 1_048_576;

/// Start a participant with the same coroutine stack request as the model body.
///
/// Call this from inside [`explore`]. It selects a modeled thread through the primitive boundary;
/// its operations and joins retain Loom's scheduling and memory-ordering semantics.
pub fn spawn<F, T>(body: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    thread::Builder::new()
        .stack_size(MODEL_STACK_REQUEST)
        .spawn(body)
        .assured("a modeled thread with a declared stack request can be started")
}

/// Explore `model`, which checks `invariant` against a production owner, to exhaustion.
///
/// The search has no preemption bound, stops at nothing but its end, and allows each execution
/// [`LOOM_BRANCH_LIMIT`] thread switches and Loom's full thread count. A setting in the environment
/// that would change that search is refused rather than honored. `LOOM_CHECKPOINT_FILE`,
/// `LOOM_CHECKPOINT_INTERVAL`, `LOOM_LOG` and `LOOM_LOCATION` only record and describe the search,
/// and are honored.
/// The five thread slots include the coordinator, model body and at most three other participants.
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
        let model = Arc::clone(&model);
        spawn(move || {
            model();
            drop(model);
        })
        .join()
        .assured("the model body completes before its coordinator returns");
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

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::{InvariantId, explore, spawn};

    const STACK: InvariantId = InvariantId::new("harness.loom.coroutine-stack");

    #[inline(never)]
    fn debug_frame(seed: u8) -> usize {
        let mut bytes = [0_u8; 65_536];
        bytes.fill(seed);
        std::hint::black_box(&mut bytes);
        bytes.iter().map(|byte| usize::from(*byte)).sum()
    }

    #[test]
    fn loom_model_coroutines_support_instrumented_debug_frames() {
        explore(STACK, || {
            assert_eq!(debug_frame(3), 3 * 65_536);
            let participant = spawn(|| debug_frame(7));
            assert_eq!(
                participant
                    .join()
                    .assured("the modeled participant completes its debug frame"),
                7 * 65_536
            );
        });
    }
}
