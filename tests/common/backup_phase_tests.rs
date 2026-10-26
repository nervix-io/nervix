//! Controlled deadline regressions for the backup fixture's actual phase owner.
//!
//! Layer: test harness.
//! - **Owns.** Timely progress with an unpolled caller and refusal at the original phase deadline.
//! - **Depends on.** The backup phase owner, paused runtime time and controlled completion signals.
//! - **Must not know.** Scenario state or product execution.

use std::{panic::AssertUnwindSafe, time::Duration};

use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    sync::oneshot,
    time::{Instant, advance, sleep, timeout},
};

use crate::backup_phase::BackupPhase;

#[nervix_primitives::test(start_paused = true)]
async fn a_backup_phase_progresses_while_its_runner_is_unpolled() {
    let (completed, completion) = oneshot::channel();
    let phase = BackupPhase::new(
        "input domain=fixture round=1".to_string(),
        Duration::from_secs(120),
    );
    let observation = phase.observe(async move {
        sleep(Duration::from_secs(10)).await;
        completed
            .send(())
            .assured("the test retains its completion receiver");
        42
    });
    // Only the operation is polled: the scenario's result future remains unpolled until after
    // its original deadline. No tolerance or host scheduling assumption decides this check.
    timeout(Duration::from_secs(20), completion)
        .await
        .assured("the input or barrier keeps progressing without a poll of its runner")
        .assured("the phase confirms its completion");
    advance(Duration::from_secs(120)).await;
    assert_eq!(observation.await, 42);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_stalled_backup_phase_fails_at_its_original_deadline() {
    let started = Instant::now();
    let phase = BackupPhase::new(
        "publication domain=fixture reference=restore-1".to_string(),
        Duration::from_secs(120),
    );
    let deadline = phase.deadline();
    advance(Duration::from_secs(30)).await;
    assert_eq!(deadline.remaining(), Duration::from_secs(90));
    let observation = phase.observe(std::future::pending::<()>());
    let Err(failure) = AssertUnwindSafe(observation).catch_unwind().await else {
        panic!("an unfinished publication must fail");
    };
    assert_eq!(started.elapsed(), Duration::from_secs(120));
    let message = failure
        .downcast_ref::<String>()
        .assured("the phase failure has its formatted identity");
    assert!(
        message.contains("publication domain=fixture reference=restore-1"),
        "{message}"
    );
}

#[nervix_primitives::test(start_paused = true)]
async fn an_expired_backup_phase_does_not_start_another_budget() {
    let phase = BackupPhase::new(
        "input domain=fixture round=2".to_string(),
        Duration::from_secs(120),
    );
    advance(Duration::from_secs(121)).await;
    let observation = phase.observe(async { 42 });
    assert!(AssertUnwindSafe(observation).catch_unwind().await.is_err());
}
