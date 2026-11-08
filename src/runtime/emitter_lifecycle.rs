//! The host protocol for one retained emitter instance.
//!
//! Layer: data plane.
//! - **Owns.** Suspension before recovery, resumption without replacement and terminal close.
//! - **Depends on.** The connector lifecycle and its borrowed task-scoped readiness service.
//! - **Must not know.** Drivers, graph Models, buffers, retry policy or scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the retained emitter protocol")
)]

use nervix_connector::{
    EmitterFinishResult, EmitterLifecycle, EmitterLifecycleResult, EmitterReadiness, EmitterResume,
};
use nervix_primitives::time::Instant;

/// A completed suspension is retained when cancellation interrupts its following resume.
pub(super) struct EmitterLifecycleState {
    ready: bool,
    suspend: bool,
    finish_received: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("emitter close requires its finish outcome or an explicitly unfinished ending")]
struct EmitterFinishNotReceived;

impl EmitterLifecycleState {
    pub(super) fn new(ready: bool) -> Self {
        Self {
            ready,
            suspend: false,
            finish_received: false,
        }
    }

    /// A failed stop keeps publishing ownership and cannot authorize terminal close.
    pub(super) fn retain_after_stop_failure(&mut self) {
        self.finish_received = false;
    }

    /// The caller reported an ending for which no finish deadline was supplied.
    pub(super) fn record_unfinished_ending(&mut self) {
        self.finish_received = true;
    }

    pub(super) fn recover(&mut self, host: &dyn EmitterReadiness) {
        self.suspend |= self.ready;
        self.ready = false;
        host.mark_unready();
    }

    pub(super) fn needs_resume(&mut self, connector: &mut dyn EmitterLifecycle) -> bool {
        !self.ready || connector.needs_resume()
    }

    pub(super) async fn resume(
        &mut self,
        connector: &mut dyn EmitterLifecycle,
        host: &dyn EmitterReadiness,
    ) -> EmitterLifecycleResult<EmitterResume> {
        if self.ready && connector.needs_resume() {
            self.recover(host);
        }
        if self.ready {
            return Ok(EmitterResume::Ready);
        }
        host.mark_unready();
        if self.suspend {
            connector.suspend().await?;
            self.suspend = false;
        }
        let resumed = connector.resume().await?;
        if resumed == EmitterResume::Ready {
            self.ready = true;
            host.mark_ready();
        }
        Ok(resumed)
    }

    pub(super) async fn close(
        &mut self,
        connector: &mut dyn EmitterLifecycle,
        host: &dyn EmitterReadiness,
    ) -> EmitterLifecycleResult<()> {
        if !self.finish_received {
            return Err(
                error_stack::Report::new(EmitterFinishNotReceived).change_context(
                    nervix_connector::EmitterLifecycleError::Close {
                        connector: connector.kind().connector().keyword(),
                    },
                ),
            );
        }
        self.finish_received = false;
        self.ready = false;
        host.mark_unready();
        connector.close().await
    }

    pub(super) async fn finish(
        &mut self,
        connector: &mut dyn EmitterLifecycle,
        deadline: Instant,
    ) -> EmitterFinishResult {
        let finished = connector.finish(deadline).await;
        self.finish_received = true;
        finished
    }
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns deterministic lifecycle evidence")
)]
mod tests {
    use std::time::Duration;

    use error_stack::Report;
    use futures_util::FutureExt as _;
    use meticulous::ResultExt as _;
    use nervix_connector::{EmitterConnector as _, EmitterLifecycleError, EmitterResume};
    use nervix_connector_fake::{
        EmitterLifecycleOperation as Op, FakeEmitter, FakeEmitterHost, PublishAnswer, Records,
    };
    use nervix_primitives::sync::{Arc, Notify};

    use super::*;

    // Each diagnostic workload runs in a fresh process and installs the detector before the
    // ordinary async test entry point constructs its runtime or any fixture creates a mutex.
    #[cfg(feature = "deloxide")]
    #[test]
    fn diagnostic_recovery_retains_one_owner() {
        let directory = std::env::var_os("NERVIX_DEADLOCK_EVIDENCE")
            .map(nervix_deadlock::EvidenceDirectory::new);
        nervix_deadlock::DiagnosticRun::start(directory)
            .assured("the retained recovery diagnostic process starts its detector once");
        recovery_retains_one_open_through_failure_waiting_finish_and_close();
    }

    #[cfg(feature = "deloxide")]
    #[test]
    fn diagnostic_cancellation_retains_one_owner() {
        let directory = std::env::var_os("NERVIX_DEADLOCK_EVIDENCE")
            .map(nervix_deadlock::EvidenceDirectory::new);
        nervix_deadlock::DiagnosticRun::start(directory)
            .assured("the retained cancellation diagnostic process starts its detector once");
        cancellation_during_resume_keeps_the_suspension_completed_and_owner_retained();
    }

    #[nervix_primitives::test]
    async fn recovery_retains_one_open_through_failure_waiting_finish_and_close() {
        let fixture = FakeEmitterHost::default();
        let host = fixture.host();
        let plan = FakeEmitter::<Records>::new([PublishAnswer::Fail]);
        let observations = plan.observations();
        let mut connector = FakeEmitter::open(plan, host.clone())
            .await
            .assured("the fixture opens");
        let mut state = EmitterLifecycleState::new(false);
        assert_eq!(
            state
                .resume(&mut connector, &host)
                .await
                .assured("initial resume"),
            EmitterResume::Ready
        );
        state.recover(&host);
        connector.resume_results.extend([
            Err(Report::new(EmitterLifecycleError::Resume {
                connector: "scripted",
            })),
            Ok(EmitterResume::Waiting {
                retry_after: Duration::from_secs(2),
            }),
            Ok(EmitterResume::Ready),
        ]);
        assert!(state.resume(&mut connector, &host).await.is_err());
        assert_eq!(
            state
                .resume(&mut connector, &host)
                .await
                .assured("waiting resume"),
            EmitterResume::Waiting {
                retry_after: Duration::from_secs(2)
            }
        );
        assert!(state.needs_resume(&mut connector));
        assert_eq!(
            state
                .resume(&mut connector, &host)
                .await
                .assured("recovered"),
            EmitterResume::Ready
        );
        let deadline = Instant::now() + Duration::from_secs(1);
        state
            .finish(&mut connector, deadline)
            .await
            .assured("finished");
        state.close(&mut connector, &host).await.assured("closed");
        let observed = observations.lock();
        assert_eq!(
            observed.lifecycle,
            [
                Op::Open,
                Op::Resume,
                Op::Suspend,
                Op::Resume,
                Op::Resume,
                Op::Resume,
                Op::Finish,
                Op::Close
            ]
        );
        assert_eq!(observed.finishes, [deadline]);
        assert_eq!(
            fixture.observations().readiness,
            [false, true, false, false, false, false, true, false]
        );
    }

    #[nervix_primitives::test]
    async fn cancellation_during_resume_keeps_the_suspension_completed_and_owner_retained() {
        let host = FakeEmitterHost::default().host();
        let plan = FakeEmitter::<Records>::new([PublishAnswer::ConfirmAll]);
        let observations = plan.observations();
        let mut connector = FakeEmitter::open(plan, host.clone())
            .await
            .assured("the fixture opens");
        let mut state = EmitterLifecycleState::new(true);
        state.recover(&host);
        connector.resume_gate = Some(Arc::new(Notify::new()));
        assert!(state.resume(&mut connector, &host).now_or_never().is_none());
        connector.resume_gate = None;
        state
            .resume(&mut connector, &host)
            .await
            .assured("same owner resumes after cancellation");
        state
            .finish(&mut connector, Instant::now())
            .await
            .assured("finished");
        state.close(&mut connector, &host).await.assured("closed");
        assert_eq!(
            observations.lock().lifecycle,
            [
                Op::Open,
                Op::Suspend,
                Op::Resume,
                Op::Resume,
                Op::Finish,
                Op::Close
            ]
        );
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "explores the production lifecycle owner")
)]
mod shuttle_tests {
    use meticulous::ResultExt as _;
    use nervix_connector::EmitterConnector as _;
    use nervix_connector_fake::{
        EmitterLifecycleOperation as Op, FakeEmitter, FakeEmitterHost, PublishAnswer, Records,
    };
    use nervix_model_harness::shuttle::check_interleavings;
    use nervix_primitives::{
        sync::{Arc, Notify, oneshot},
        task::{spawn, yield_now},
    };

    use super::*;

    #[test]
    fn shuttle_cancelling_resume_retains_one_owner_until_finish_then_close() {
        check_interleavings(|| {
            shuttle::future::block_on(async {
                let host = FakeEmitterHost::default().host();
                let plan = FakeEmitter::<Records>::new([PublishAnswer::ConfirmAll]);
                let observations = plan.observations();
                let mut connector = FakeEmitter::open(plan, host.clone())
                    .await
                    .assured("fixture opens");
                let mut state = EmitterLifecycleState::new(true);
                state.recover(&host);
                let started = Arc::new(Notify::new());
                let gate = Arc::new(Notify::new());
                connector.resume_started = Some(started.clone());
                connector.resume_gate = Some(gate.clone());
                let (cancel, cancelled) = oneshot::channel();
                let controller = spawn(async move {
                    started.notified().await;
                    cancel.send(()).assured("the retained host is waiting");
                    yield_now().await;
                    gate.notify_one();
                });
                nervix_primitives::select! {
                    // The scheduler explores event order; DFS requires deterministic polling.
                    biased;
                    resumed = state.resume(&mut connector, &host) => { resumed.assured("resume completes"); },
                    cancelled = cancelled => { cancelled.assured("cancellation arrives"); },
                }
                controller.await.assured("controller completes");
                connector.resume_gate = None;
                state
                    .resume(&mut connector, &host)
                    .await
                    .assured("retained owner is ready");
                assert!(
                    state.close(&mut connector, &host).await.is_err(),
                    "a cancelled resume cannot authorize close before its finish outcome"
                );
                state
                    .finish(&mut connector, Instant::now())
                    .await
                    .assured("finish precedes close");
                state
                    .close(&mut connector, &host)
                    .await
                    .assured("exact instance closes");
                let observed = observations.lock();
                assert_eq!(
                    observed
                        .lifecycle
                        .iter()
                        .filter(|op| **op == Op::Open)
                        .count(),
                    1
                );
                assert_eq!(
                    observed
                        .lifecycle
                        .iter()
                        .filter(|op| **op == Op::Suspend)
                        .count(),
                    1
                );
                assert_eq!(
                    &observed.lifecycle[observed.lifecycle.len() - 2..],
                    [Op::Finish, Op::Close]
                );
                assert!(!observed.lifecycle[1..observed.lifecycle.len() - 2].contains(&Op::Close));
            })
        });
    }
}
