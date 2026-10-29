//! Supervision of committed runtime installation after node startup.
//!
//! Layer: control plane.
//!
//! - **Owns.** One background installation attempt, its committed-update wakeups, failure retry,
//!   and cancellation of the wait between attempts.
//! - **Depends on.** Governed watch channels, time and cancellation, and typed runtime reports.
//! - **Must not know.** Graph construction, connectors, scheduling policy, or persisted Models.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::future::Future;

use nervix_primitives::sync::{CancellationToken, watch};
use tracing::warn;

use super::runtime_admission::RUNTIME_STATE_RETRY_INTERVAL;
use crate::runtime::RuntimeError;

/// The single node-local supervisor of committed runtime installation.
pub(in crate::application) struct RuntimeReconciliation {
    schedules: watch::Receiver<u64>,
    domains: watch::Receiver<u64>,
    shutdown: CancellationToken,
}

impl RuntimeReconciliation {
    pub(in crate::application) fn new(
        schedules: watch::Receiver<u64>,
        domains: watch::Receiver<u64>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            schedules,
            domains,
            shutdown,
        }
    }

    /// Applies current admitted state initially and after either committed input changes. The
    /// caller reads committed inputs afresh and owns cancellation during an installation. Each
    /// attempt takes and returns its local observation state, including after a refusal.
    pub(in crate::application) async fn run<State, Apply, Applying>(
        mut self,
        mut state: State,
        mut apply: Apply,
    ) where
        Apply: FnMut(State) -> Applying,
        Applying: Future<Output = (State, error_stack::Result<(), RuntimeError>)>,
    {
        loop {
            nervix_primitives::task::consume_budget().await;
            if self.shutdown.is_cancelled() {
                return;
            }
            // Consume only updates already visible before this attempt. An update arriving while
            // application is pending must still wake the next attempt, even if this one succeeds.
            drop(self.schedules.borrow_and_update());
            drop(self.domains.borrow_and_update());
            let (updated_state, outcome) = apply(state).await;
            state = updated_state;
            let failed = match outcome {
                Ok(()) => false,
                Err(error) => {
                    warn!(
                        error = format!("{error:#}"),
                        "failed to install committed runtime state; retrying"
                    );
                    true
                }
            };
            nervix_primitives::select! {
                biased;
                _ = self.shutdown.cancelled() => return,
                changed = self.schedules.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                changed = self.domains.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                _ = nervix_primitives::time::sleep(RUNTIME_STATE_RETRY_INTERVAL), if failed => {}
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::time::Duration;

    use error_stack::Report;
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_primitives::{
        sync::{mpsc, oneshot},
        time::{Instant, advance},
    };

    use super::*;

    fn refused() -> error_stack::Result<(), RuntimeError> {
        Err(Report::new(RuntimeError::ApplyRevision { revision: 1 }))
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn failure_retries_on_the_admission_cadence_and_success_waits_for_an_update() {
        let (_schedules, schedule_rx) = watch::channel(0);
        let (_domains, domain_rx) = watch::channel(0);
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let (attempts, mut observed) = mpsc::channel(4);
        let task = nervix_primitives::task::spawn(async move {
            RuntimeReconciliation::new(schedule_rx, domain_rx, task_shutdown)
                .run(0_u8, move |count| {
                    let attempts = attempts.clone();
                    async move {
                        let count = count.checked_add(1).assured(
                            "the four-slot observer and bounded test reads keep the counter below \
                             u8::MAX",
                        );
                        attempts.send(count).await.assured(
                            "the test retains the observation receiver until the supervisor joins",
                        );
                        (count, if count == 1 { refused() } else { Ok(()) })
                    }
                })
                .await;
        });

        assert_eq!(observed.recv().await, Some(1));
        let before_retry = RUNTIME_STATE_RETRY_INTERVAL
            .checked_sub(Duration::from_millis(1))
            .assured("the 500 ms retry cadence exceeds the 1 ms test boundary");
        advance(before_retry).await;
        nervix_primitives::task::yield_now().await;
        assert!(observed.try_recv().is_err());
        advance(Duration::from_millis(1)).await;
        assert_eq!(observed.recv().await, Some(2));
        advance(Duration::from_secs(10)).await;
        nervix_primitives::task::yield_now().await;
        assert!(observed.try_recv().is_err());
        shutdown.cancel();
        task.await
            .assured("the controlled callbacks only use channel endpoints retained by this test");
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn schedule_and_domain_updates_preempt_the_retry_with_current_inputs() {
        let (schedules, schedule_rx) = watch::channel(0);
        let (domains, domain_rx) = watch::channel(0);
        let current_schedule = schedule_rx.clone();
        let current_domain = domain_rx.clone();
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let (attempts, mut observed) = mpsc::channel(4);
        let started = Instant::now();
        let task = nervix_primitives::task::spawn(async move {
            RuntimeReconciliation::new(schedule_rx, domain_rx, task_shutdown)
                .run((), move |()| {
                    let inputs = (*current_schedule.borrow(), *current_domain.borrow());
                    let attempts = attempts.clone();
                    async move {
                        attempts.send(inputs).await.assured(
                            "the test retains the observation receiver until the supervisor joins",
                        );
                        ((), refused())
                    }
                })
                .await;
        });

        assert_eq!(observed.recv().await, Some((0, 0)));
        schedules
            .send(1)
            .assured("the supervisor and callback retain schedule receivers until shutdown");
        assert_eq!(observed.recv().await, Some((1, 0)));
        domains
            .send(2)
            .assured("the supervisor and callback retain domain receivers until shutdown");
        assert_eq!(observed.recv().await, Some((1, 2)));
        assert_eq!(started.elapsed(), Duration::ZERO);
        shutdown.cancel();
        task.await
            .assured("the controlled callbacks only use channel endpoints retained by this test");
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn updates_during_an_installation_are_not_lost_when_it_succeeds() {
        let (schedules, schedule_rx) = watch::channel(0);
        let (domains, domain_rx) = watch::channel(0);
        let current_schedule = schedule_rx.clone();
        let current_domain = domain_rx.clone();
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let (attempts, mut observed) = mpsc::channel(4);
        let (release, released) = oneshot::channel();
        let started = Instant::now();
        let task = nervix_primitives::task::spawn(async move {
            let mut released = Some(released);
            RuntimeReconciliation::new(schedule_rx, domain_rx, task_shutdown)
                .run((), move |()| {
                    let inputs = (*current_schedule.borrow(), *current_domain.borrow());
                    let attempts = attempts.clone();
                    let released = released.take();
                    async move {
                        attempts.send(inputs).await.assured(
                            "the test retains the observation receiver until the supervisor joins",
                        );
                        if let Some(released) = released {
                            released
                                .await
                                .assured("the test sends the release before dropping its sender");
                        }
                        ((), Ok(()))
                    }
                })
                .await;
        });

        assert_eq!(observed.recv().await, Some((0, 0)));
        schedules
            .send(1)
            .assured("the supervisor and callback retain schedule receivers until shutdown");
        domains
            .send(2)
            .assured("the supervisor and callback retain domain receivers until shutdown");
        release
            .send(())
            .assured("the first installation retains its release receiver until this send");
        assert_eq!(observed.recv().await, Some((1, 2)));
        assert_eq!(started.elapsed(), Duration::ZERO);
        nervix_primitives::task::yield_now().await;
        assert!(observed.try_recv().is_err());
        shutdown.cancel();
        task.await
            .assured("the controlled callbacks only use channel endpoints retained by this test");
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn shutdown_ends_a_failed_attempts_wait_without_another_attempt() {
        let (_schedules, schedule_rx) = watch::channel(0);
        let (_domains, domain_rx) = watch::channel(0);
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let (attempts, mut observed) = mpsc::channel(4);
        let started = Instant::now();
        let task = nervix_primitives::task::spawn(async move {
            RuntimeReconciliation::new(schedule_rx, domain_rx, task_shutdown)
                .run((), move |()| {
                    let attempts = attempts.clone();
                    async move {
                        attempts.send(()).await.assured(
                            "the test retains the observation receiver until the supervisor joins",
                        );
                        ((), refused())
                    }
                })
                .await;
        });

        assert_eq!(observed.recv().await, Some(()));
        shutdown.cancel();
        task.await
            .assured("the controlled callbacks only use channel endpoints retained by this test");
        assert_eq!(observed.recv().await, None);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn closed_committed_inputs_end_the_supervisor() {
        for close_schedule in [true, false] {
            nervix_primitives::task::consume_budget().await;
            let (schedules, schedule_rx) = watch::channel(0);
            let (domains, domain_rx) = watch::channel(0);
            let shutdown = CancellationToken::new();
            let task = nervix_primitives::task::spawn(async move {
                RuntimeReconciliation::new(schedule_rx, domain_rx, shutdown)
                    .run((), |()| async { ((), Ok(())) })
                    .await;
            });
            if close_schedule {
                drop(schedules);
            } else {
                drop(domains);
            }
            task.await.assured(
                "the controlled callbacks only use channel endpoints retained by this test",
            );
        }
    }
}
