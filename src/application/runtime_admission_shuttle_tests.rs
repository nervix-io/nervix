//! Installations whose callers go away, explored under Shuttle.
//!
//! Layer: test harness.
//!
//! - **Owns.** The invariant that an installation section that took the node's installation turn
//!   runs to its end whatever happens to the caller that began it, that sections run one at a
//!   time, and that the runtime's stop follows the last of them.
//! - **Depends on.** The production runtime admission's installation turn and the model harness's
//!   Shuttle runner.
//! - **Must not know.** What a section installs, or which caller begins it.
//!
//! A caller that goes away is a select that another task's cancellation wins, which drops the
//! caller's future wherever it then waits, as the interconnect server drops a request handler whose
//! requester reset the stream. Terminal teardown is the task that ends drain support and then takes
//! the turn for good before it stops the runtime. A caller can check the shutdown just before drain
//! support ends and ask for the turn only after teardown took it, so the turn is never given back.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_model_harness::shuttle::{check_pct, check_random};
use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName};
use nervix_primitives::{
    sync::{
        Arc, CancellationToken,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task,
};
use shuttle::future::block_on;

use super::RuntimeAdmission;

const MODEL_TASKS_JOIN: &str =
    "Shuttle fails the whole execution when a model task panics, so no join observes one";
const PCT_DEPTH: usize = 3;
const PCT_ITERATIONS: usize = 1_000;
const RANDOM_ITERATIONS: usize = 1_000;

/// What the sections and the runtime's stop did, as the model ran them.
#[derive(Default)]
struct Installations {
    /// A section is under way.
    running: AtomicBool,
    began: AtomicUsize,
    ended: AtomicUsize,
    /// Terminal teardown took the installation turn and stopped the runtime.
    runtime_stopped: AtomicBool,
}

/// One installation section: it starts, lets every other task run, and ends.
async fn section(installations: Arc<Installations>) {
    assert!(
        !installations.runtime_stopped.load(Ordering::SeqCst),
        "an installation began after the runtime stopped"
    );
    assert!(
        !installations.running.swap(true, Ordering::SeqCst),
        "two installations ran at once"
    );
    installations.began.fetch_add(1, Ordering::SeqCst);
    task::yield_now().await;
    installations.ended.fetch_add(1, Ordering::SeqCst);
    installations.running.store(false, Ordering::SeqCst);
}

/// Takes the installation turn unless drain support has ended, and runs one section under it.
/// Once `gives_up` is cancelled the caller goes away, wherever it then waits.
async fn caller(
    admission: Arc<RuntimeAdmission>,
    drain_support: CancellationToken,
    gives_up: CancellationToken,
    installations: Arc<Installations>,
) {
    let installing = async {
        let Some(turn) = admission.begin_installation(&drain_support).await else {
            return;
        };
        let outcome = turn.run(section(installations.clone())).await;
        assert!(
            outcome.is_some(),
            "the model's executor keeps every section's task until it ends"
        );
    };
    nervix_primitives::select! {
        () = gives_up.cancelled() => {}
        () = installing => {}
    }
}

/// Two callers, one of which goes away at any moment, race terminal teardown.
fn callers_that_go_away_racing_the_runtime_stop() {
    block_on(async {
        let identity = ClusterNodeIdentity::new(
            ClusterNodeName::parse("node-1").assured("the model node name is a valid literal"),
            ClusterNodeIncarnation::new(1),
        );
        let admission = Arc::new(RuntimeAdmission::new(identity, Duration::from_millis(500)));
        let drain_support = CancellationToken::new();
        let installations = Arc::new(Installations::default());
        let gives_up = CancellationToken::new();

        let leaving = task::spawn(caller(
            admission.clone(),
            drain_support.clone(),
            gives_up.clone(),
            installations.clone(),
        ));
        let staying = task::spawn(caller(
            admission.clone(),
            drain_support.clone(),
            CancellationToken::new(),
            installations.clone(),
        ));
        let requester = task::spawn(async move { gives_up.cancel() });
        let teardown = task::spawn({
            let admission = admission.clone();
            let installations = installations.clone();
            async move {
                drain_support.cancel();
                admission.finish_installations().await;
                assert!(
                    !installations.running.load(Ordering::SeqCst),
                    "the runtime stopped while an installation was under way"
                );
                assert_eq!(
                    installations.began.load(Ordering::SeqCst),
                    installations.ended.load(Ordering::SeqCst),
                    "an installation that began did not end before the runtime stopped"
                );
                installations.runtime_stopped.store(true, Ordering::SeqCst);
            }
        });

        leaving.await.assured(MODEL_TASKS_JOIN);
        staying.await.assured(MODEL_TASKS_JOIN);
        requester.await.assured(MODEL_TASKS_JOIN);
        teardown.await.assured(MODEL_TASKS_JOIN);
        assert_eq!(
            installations.began.load(Ordering::SeqCst),
            installations.ended.load(Ordering::SeqCst),
            "an installation whose caller went away was cut short"
        );
    });
}

#[test]
fn shuttle_an_installation_whose_caller_goes_away_ends_before_the_next_turn_and_the_runtime_stop() {
    check_random(
        callers_that_go_away_racing_the_runtime_stop,
        RANDOM_ITERATIONS,
    );
    check_pct(
        callers_that_go_away_racing_the_runtime_stop,
        PCT_ITERATIONS,
        PCT_DEPTH,
    );
}
