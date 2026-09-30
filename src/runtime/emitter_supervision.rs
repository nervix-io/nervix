//! Emitter task commands, supervision and startup against its typed execution plan.
//!
//! Layer: data plane.
//! - **Owns.** The task's stop and reconfigure commands, lifecycle guards, and node-local plan
//!   binding before a task starts.
//! - **Depends on.** Typed emitter plans, relay fan-in, resolved resource mounts, and runtime
//!   services.
//! - **Must not know.** Semantic emitter or client Models, connector-specific sink construction,
//!   or placement decisions.

use super::*;

pub(super) type EmitterReconfigureResult<T> = Result<T, Report<EmitterReconfigureError>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(super) enum EmitterReconfigureError {
    #[error("scheduled emitter task timed out accepting reconfiguration")]
    AcceptTimeout,
    #[error("scheduled emitter task is unavailable for reconfiguration")]
    Unavailable,
    #[error("scheduled emitter task timed out reconfiguring")]
    ResponseTimeout,
    #[error("scheduled emitter task dropped its reconfiguration response")]
    ResponseDropped,
}

/// What an emitter is retrying: the publish path itself, or the commit that publishes what its
/// sink staged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EmitterRetryKind {
    Infrastructure,
    Commit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EmitterRetryStatus {
    pub(super) kind: EmitterRetryKind,
    pub(super) reconnect: RuntimeReconnectStatus,
}

pub(super) struct EmitterConfirmationWaitGuard {
    pub(super) active_waits: Arc<AtomicUsize>,
}

impl EmitterConfirmationWaitGuard {
    pub(super) fn begin(active_waits: &Arc<AtomicUsize>) -> Self {
        active_waits.fetch_add(1, Ordering::AcqRel);
        Self {
            active_waits: active_waits.clone(),
        }
    }
}

impl Drop for EmitterConfirmationWaitGuard {
    fn drop(&mut self) {
        self.active_waits.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) enum EmitterTaskCommand {
    Reconfigure {
        flush_policy: FlushPolicy,
        response: oneshot::Sender<()>,
    },
    Stop {
        deadline: Instant,
        response: oneshot::Sender<emitter_task::EmitterRuntimeResult<()>>,
    },
}

impl RelayInteractionCommand for EmitterTaskCommand {
    fn drain_inputs_before_handling(&self) -> bool {
        matches!(self, Self::Stop { .. })
    }

    fn cancels_external_waits_while_draining(&self) -> bool {
        matches!(self, Self::Stop { .. })
    }
}

#[derive(Debug)]
pub(super) struct ScheduledEmitterTask {
    pub(super) commands: mpsc::Sender<EmitterTaskCommand>,
    pub(super) stop_signal: watch::Sender<Option<Instant>>,
    pub(super) task: JoinHandle<()>,
}

#[derive(Debug)]
pub(super) struct ScheduledEmitterStopError {
    pub(super) reason: String,
    pub(super) task: Option<ScheduledEmitterTask>,
}

pub(super) fn clear_emitter_stop_signal(
    stop_signal: &watch::Sender<Option<Instant>>,
    deadline: Instant,
) {
    stop_signal.send_if_modified(|pending| {
        if *pending == Some(deadline) {
            *pending = None;
            true
        } else {
            false
        }
    });
}

impl ScheduledEmitterStopError {
    pub(super) fn recoverable(reason: impl Into<String>, task: ScheduledEmitterTask) -> Self {
        Self {
            reason: reason.into(),
            task: Some(task),
        }
    }

    pub(super) fn reason(&self) -> &str {
        &self.reason
    }

    pub(super) fn into_task(self) -> Option<ScheduledEmitterTask> {
        self.task
    }
}

impl ScheduledEmitterTask {
    pub(super) async fn reconfigure_via(
        commands: &mpsc::Sender<EmitterTaskCommand>,
        flush_policy: FlushPolicy,
    ) -> EmitterReconfigureResult<()> {
        let (response, receiver) = oneshot::channel();
        nervix_primitives::time::timeout(
            PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE,
            commands.send(EmitterTaskCommand::Reconfigure {
                flush_policy,
                response,
            }),
        )
        .await
        .map_err(|_| Report::new(EmitterReconfigureError::AcceptTimeout))?
        .map_err(|_| Report::new(EmitterReconfigureError::Unavailable))?;
        nervix_primitives::time::timeout(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE, receiver)
            .await
            .map_err(|_| Report::new(EmitterReconfigureError::ResponseTimeout))?
            .map_err(|_| Report::new(EmitterReconfigureError::ResponseDropped))
    }

    pub(super) async fn stop(
        mut self,
        drain_timeout: Duration,
    ) -> Result<(), ScheduledEmitterStopError> {
        let (response, receiver) = oneshot::channel();
        let deadline = Instant::now() + drain_timeout;
        let command = EmitterTaskCommand::Stop { deadline, response };
        match nervix_primitives::time::timeout_at(deadline, self.commands.send(command)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(ScheduledEmitterStopError::recoverable(
                    "scheduled emitter task is unavailable for stopping",
                    self,
                ));
            }
            Err(_) => {
                return Err(ScheduledEmitterStopError::recoverable(
                    "scheduled emitter task timed out accepting its stop command",
                    self,
                ));
            }
        }
        self.stop_signal.send_replace(Some(deadline));
        let response_deadline = deadline + PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE;
        let response = match nervix_primitives::time::timeout_at(response_deadline, receiver).await
        {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                clear_emitter_stop_signal(&self.stop_signal, deadline);
                return Err(ScheduledEmitterStopError::recoverable(
                    "scheduled emitter task dropped its stop response",
                    self,
                ));
            }
            Err(_) => {
                clear_emitter_stop_signal(&self.stop_signal, deadline);
                return Err(ScheduledEmitterStopError::recoverable(
                    "scheduled emitter task timed out draining",
                    self,
                ));
            }
        };
        if let Err(error) = response {
            clear_emitter_stop_signal(&self.stop_signal, deadline);
            return Err(ScheduledEmitterStopError::recoverable(
                emitter_task::emitter_error_message(&error),
                self,
            ));
        }
        match nervix_primitives::time::timeout(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE, &mut self.task)
            .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                // A successful stop response means the buffered work and transport drain already
                // completed. The task has no work left to preserve even if its final join failed.
                warn!(error = %error, "scheduled emitter task join failed after a successful drain");
                Ok(())
            }
            Err(_) => {
                self.task.abort();
                self.task.join_after_shutdown("scheduled emitter").await;
                Ok(())
            }
        }
    }
}

impl Runtime {
    pub(in crate::runtime) fn emitter_task_deps(
        &self,
        deps: ExecutionBuildDeps<'_>,
        emitter: &EmitterExecutionPlan,
    ) -> Result<EmitterTaskDeps, RuntimeError> {
        let Some(input_relay) = emitter.inputs.first().map(|input| &input.relay) else {
            return Err(RuntimeError::BuildDomainExecution {
                domain: deps.domain.as_str().to_string(),
                reason: format!("emitter '{}' has no input relay", emitter.name.as_str()),
            });
        };
        let Some(input_schema) = deps.relay_schemas.get(input_relay).cloned() else {
            return Err(RuntimeError::BuildDomainExecution {
                domain: deps.domain.as_str().to_string(),
                reason: format!(
                    "missing emitter input relay schema '{}'",
                    input_relay.as_str()
                ),
            });
        };
        let Some(input_branching) = deps.relay_branchings.get(input_relay).cloned() else {
            return Err(RuntimeError::BuildDomainExecution {
                domain: deps.domain.as_str().to_string(),
                reason: format!(
                    "missing emitter input relay branching '{}'",
                    input_relay.as_str()
                ),
            });
        };
        Ok(EmitterTaskDeps {
            input_schema,
            input_branching,
            materialized_relay_specs: deps.materialized_relay_specs.clone(),
            lookups: deps.lookups.clone(),
        })
    }

    pub(super) fn emitter_status(
        &self,
        key: &DomainNodeRef,
    ) -> Arc<task_status::TaskStatus<EmitterRetryStatus>> {
        if let Some(status) = self.inner.emitter_statuses.get(key) {
            return status.clone();
        }
        // Execution preparation serializes this entity's first registration before instances start.
        let status = Arc::new(task_status::TaskStatus::<EmitterRetryStatus>::default());
        self.inner
            .emitter_statuses
            .insert(key.clone(), status.clone());
        status
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_emitter_transient_error_with_backoff(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
        error: impl Into<String>,
        backoff: Duration,
    ) {
        self.record_emitter_retry_with_backoff(
            domain,
            emitter,
            error,
            backoff,
            EmitterRetryKind::Infrastructure,
        );
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_commit_failure_with_backoff(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
        error: impl Into<String>,
        backoff: Duration,
    ) {
        self.record_emitter_retry_with_backoff(
            domain,
            emitter,
            error,
            backoff,
            EmitterRetryKind::Commit,
        );
    }

    #[cfg(test)]
    pub(super) fn record_emitter_retry_with_backoff(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
        error: impl Into<String>,
        backoff: Duration,
        kind: EmitterRetryKind,
    ) {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Emitter, emitter.clone());
        self.emitter_status(&key).fail(
            error.into(),
            Some(EmitterRetryStatus {
                kind,
                reconnect: RuntimeReconnectStatus {
                    backoff,
                    retry_at: Instant::now() + backoff,
                },
            }),
        );
    }

    pub(super) fn emitter_confirmation_counter(&self, key: &DomainNodeRef) -> Arc<AtomicUsize> {
        self.inner
            .emitter_confirmation_waits
            .entry(key.clone())
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone()
    }

    #[cfg(test)]
    pub(in crate::runtime) fn begin_emitter_confirmation_wait(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
    ) -> EmitterConfirmationWaitGuard {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Emitter, emitter.clone());
        EmitterConfirmationWaitGuard::begin(&self.emitter_confirmation_counter(&key))
    }

    /// Resolves the mounts in an already decided emitter plan, then starts its task.
    pub(in crate::runtime) fn spawn_emitter_task(
        &self,
        build: EmitterTaskBuildDeps<'_>,
        emitter: EmitterExecutionPlan,
        inputs: Vec<(RelayName, RelayRuntimeFanIn)>,
    ) -> Result<ScheduledEmitterTask, RuntimeError> {
        let domain = build.domain;
        let plan = emitter.sink.clone().resolve_clients(|client| {
            self.resolve_client_config(domain, client.config.mount.as_ref(), &client.config.entries)
                .map_err(|error| RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: format!(
                        "failed to resolve client '{}' for emitter '{}': {error}",
                        client.name.as_str(),
                        emitter.name.as_str()
                    ),
                })
        })?;
        emitter_task::EmitterTask::spawn(self, build, emitter, plan, inputs)
    }
}

/// Registration ends with its emitter task. A replacement task owns different retained handles.
pub(super) struct EmitterTaskRegistration {
    runtime: Runtime,
    key: DomainNodeRef,
    status: Arc<task_status::TaskStatus<EmitterRetryStatus>>,
    confirmations: Arc<AtomicUsize>,
}

impl EmitterTaskRegistration {
    pub(super) fn new(context: &EmitterSinkContext) -> Self {
        Self {
            runtime: context.runtime.clone(),
            key: DomainNodeRef::node_in(
                context.domain.clone(),
                ModelKind::Emitter,
                context.emitter.clone(),
            ),
            status: context.status.clone(),
            confirmations: context.confirmation_waits.clone(),
        }
    }
}

impl Drop for EmitterTaskRegistration {
    fn drop(&mut self) {
        self.runtime
            .inner
            .emitter_statuses
            .remove_if(&self.key, |_, current| Arc::ptr_eq(current, &self.status));
        self.runtime
            .inner
            .emitter_confirmation_waits
            .remove_if(&self.key, |_, current| {
                Arc::ptr_eq(current, &self.confirmations)
            });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nervix_primitives::{
        sync::{mpsc, watch},
        time::Instant,
    };

    use super::*;

    #[nervix_primitives::test]
    async fn scheduled_emitter_stop_keeps_a_failed_drain_task_available_for_retry() {
        let grace = Duration::from_millis(50);
        let started = Instant::now();
        let (commands, mut command_rx) = mpsc::channel(2);
        let (stop_signal, _stop_rx) = watch::channel(None);
        let task = nervix_primitives::task::spawn(async move {
            let Some(EmitterTaskCommand::Stop { deadline, response }) = command_rx.recv().await
            else {
                panic!("expected the first emitter stop command");
            };
            // `stop` reads the clock before it sends, so its deadline cannot be later than the
            // grace measured from the instant this receiver observed the command. Anchoring the
            // bound to an observation taken after the deadline was computed keeps it exact under
            // any scheduling delay, where a fixed tolerance would only hold on an idle machine.
            let observed = Instant::now();
            assert!(deadline > started);
            assert!(deadline <= observed + grace);
            let _ = response.send(Err(Report::new(
                emitter_task::EmitterRuntimeError::FinalFlush,
            )
            .attach_printable("transport drain failed")));

            let Some(EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
                panic!("expected the retried emitter stop command");
            };
            let _ = response.send(Ok(()));
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        };

        let failed = scheduled
            .stop(grace)
            .await
            .expect_err("a failed transport drain must fail the emitter stop");
        assert_eq!(failed.reason(), "transport drain failed");
        let scheduled = failed
            .into_task()
            .expect("a failed drain must leave the old emitter task available");

        scheduled
            .stop(grace)
            .await
            .expect("the retained emitter task must accept a later successful stop");
    }

    #[nervix_primitives::test]
    async fn scheduled_emitter_stop_clears_signal_when_the_response_is_dropped() {
        let (commands, mut command_rx) = mpsc::channel(1);
        let (stop_signal, _stop_rx) = watch::channel(None);
        let task = nervix_primitives::task::spawn(async move {
            let Some(EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
                panic!("expected an emitter stop command");
            };
            drop(response);
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        };

        let failed = scheduled
            .stop(Duration::from_millis(50))
            .await
            .expect_err("a dropped drain response must retain the scheduled task");
        assert_eq!(
            failed.reason(),
            "scheduled emitter task dropped its stop response"
        );
        assert!(
            failed
                .into_task()
                .expect("the failed stop must return its task")
                .stop_signal
                .borrow()
                .is_none(),
            "a dropped response must not leave the retained emitter interrupted"
        );
    }

    #[nervix_primitives::test]
    async fn scheduled_emitter_stop_returns_final_flush_failure_as_recoverable() {
        let (commands, mut command_rx) = mpsc::channel(1);
        let (stop_signal, _stop_rx) = watch::channel(None);
        let finished = Arc::new(AtomicBool::new(false));
        let task_finished = finished.clone();
        let task = nervix_primitives::task::spawn(async move {
            let Some(EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
                panic!("scheduled emitter must receive its stop command")
            };
            let _ = response.send(Err(Report::new(EmitterRuntimeError::FinalFlush)
                .attach_printable("emitter final flush failed: broker unavailable")));
            task_finished.store(true, Ordering::Release);
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        };

        let error = scheduled
            .stop(Duration::from_secs(1))
            .await
            .expect_err("final flush failure must reach the stopping caller");

        assert_eq!(
            error.reason(),
            "emitter final flush failed: broker unavailable"
        );
        let mut retained = error
            .into_task()
            .expect("a failed drain must retain the scheduled task");
        let _ = (&mut retained.task).await;
        assert!(finished.load(Ordering::Acquire));
    }

    #[nervix_primitives::test]
    async fn scheduled_emitter_stop_retains_a_task_that_drops_its_response() {
        struct Dropped(Arc<AtomicBool>);

        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let (commands, mut command_rx) = mpsc::channel(1);
        let (stop_signal, _) = watch::channel(None);
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let task = nervix_primitives::task::spawn(async move {
            let _dropped = Dropped(task_dropped);
            let Some(EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
                panic!("scheduled emitter must receive its stop command")
            };
            drop(response);
            std::future::pending::<()>().await;
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        };

        let error = scheduled
            .stop(Duration::from_secs(1))
            .await
            .expect_err("a dropped response must fail stopping");

        assert_eq!(
            error.reason(),
            "scheduled emitter task dropped its stop response"
        );
        let mut retained = error
            .into_task()
            .expect("a dropped response must leave the task recoverable");
        assert!(!dropped.load(Ordering::Acquire));
        retained.task.abort();
        let _ = (&mut retained.task).await;
        assert!(dropped.load(Ordering::Acquire));
    }

    #[nervix_primitives::test]
    async fn scheduled_emitter_stop_timeout_retains_the_task() {
        struct Dropped(Arc<AtomicBool>);

        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let (commands, mut command_rx) = mpsc::channel(1);
        let (stop_signal, _) = watch::channel(None);
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let task = nervix_primitives::task::spawn(async move {
            let _dropped = Dropped(task_dropped);
            let Some(EmitterTaskCommand::Stop { response, .. }) = command_rx.recv().await else {
                panic!("scheduled emitter must receive its stop command")
            };
            let _response = response;
            std::future::pending::<()>().await;
        });
        let scheduled = ScheduledEmitterTask {
            commands,
            stop_signal,
            task,
        };

        let error = scheduled
            .stop(Duration::from_millis(5))
            .await
            .expect_err("a missing stop response must time out");

        assert_eq!(error.reason(), "scheduled emitter task timed out draining");
        let mut retained = error
            .into_task()
            .expect("a timed-out drain must leave the task recoverable");
        assert!(!dropped.load(Ordering::Acquire));
        retained.task.abort();
        let _ = (&mut retained.task).await;
        assert!(dropped.load(Ordering::Acquire));
    }
}
