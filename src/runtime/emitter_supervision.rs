use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EmitterRetryKind {
    Infrastructure,
    IcebergCommit,
}

#[derive(Debug, Clone)]
pub(super) struct EmitterRetryStatus {
    pub(super) kind: EmitterRetryKind,
    pub(super) reconnect: RuntimeReconnectStatus,
}

pub(super) struct EmitterConfirmationWaitGuard {
    pub(super) active_waits: Arc<AtomicUsize>,
}

impl Drop for EmitterConfirmationWaitGuard {
    fn drop(&mut self) {
        self.active_waits.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) enum EmitterTaskCommand {
    Reconfigure {
        config: Box<CreateEmitter>,
        response: oneshot::Sender<()>,
    },
    Stop {
        deadline: Instant,
        response: oneshot::Sender<Result<(), String>>,
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
        config: Box<CreateEmitter>,
    ) -> Result<(), String> {
        let (response, receiver) = oneshot::channel();
        tokio::time::timeout(
            PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE,
            commands.send(EmitterTaskCommand::Reconfigure { config, response }),
        )
        .await
        .map_err(|_| "scheduled emitter task timed out accepting reconfiguration".to_string())?
        .map_err(|_| "scheduled emitter task is unavailable for reconfiguration".to_string())?;
        tokio::time::timeout(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE, receiver)
            .await
            .map_err(|_| "scheduled emitter task timed out reconfiguring".to_string())?
            .map_err(|_| "scheduled emitter task dropped its reconfiguration response".to_string())
    }

    pub(super) async fn stop(
        mut self,
        drain_timeout: Duration,
    ) -> Result<(), ScheduledEmitterStopError> {
        let (response, receiver) = oneshot::channel();
        let deadline = Instant::now() + drain_timeout;
        let command = EmitterTaskCommand::Stop { deadline, response };
        match tokio::time::timeout_at(deadline, self.commands.send(command)).await {
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
        let response = match tokio::time::timeout_at(response_deadline, receiver).await {
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
        if let Err(reason) = response {
            clear_emitter_stop_signal(&self.stop_signal, deadline);
            return Err(ScheduledEmitterStopError::recoverable(reason, self));
        }
        match tokio::time::timeout(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE, &mut self.task).await {
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
        emitter: &CreateEmitter,
    ) -> Result<EmitterTaskDeps, RuntimeError> {
        let Some(input_relay) = emitter.from.first() else {
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

    pub(in crate::runtime) fn record_emitter_transient_error(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
        error: impl Into<String>,
    ) {
        self.inner.emitter_transient_errors.insert(
            DomainNodeRef::node_in(domain.clone(), ModelKind::Emitter, emitter.clone()),
            error.into(),
        );
    }

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

    pub(in crate::runtime) fn record_iceberg_commit_failure_with_backoff(
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
            EmitterRetryKind::IcebergCommit,
        );
    }

    pub(super) fn record_emitter_retry_with_backoff(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
        error: impl Into<String>,
        backoff: Duration,
        kind: EmitterRetryKind,
    ) {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Emitter, emitter.clone());
        self.inner
            .emitter_transient_errors
            .insert(key.clone(), error.into());
        self.inner.emitter_retry_statuses.insert(
            key,
            EmitterRetryStatus {
                kind,
                reconnect: RuntimeReconnectStatus {
                    backoff,
                    retry_at: Instant::now() + backoff,
                },
            },
        );
    }

    pub(in crate::runtime) fn begin_emitter_confirmation_wait(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
    ) -> EmitterConfirmationWaitGuard {
        let active_waits = self
            .inner
            .emitter_confirmation_waits
            .entry(DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Emitter,
                emitter.clone(),
            ))
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone();
        active_waits.fetch_add(1, Ordering::AcqRel);
        EmitterConfirmationWaitGuard { active_waits }
    }

    pub(in crate::runtime) fn clear_emitter_transient_error(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
    ) {
        self.inner
            .emitter_transient_errors
            .remove(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Emitter,
                emitter.clone(),
            ));
        self.inner
            .emitter_retry_statuses
            .remove(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Emitter,
                emitter.clone(),
            ));
    }

    pub(super) fn emitter_transient_error(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
    ) -> Option<String> {
        self.inner
            .emitter_transient_errors
            .get(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Emitter,
                emitter.clone(),
            ))
            .map(|error| error.value().clone())
    }

    pub fn emitter_reconnect_backoff(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
    ) -> Option<String> {
        self.inner
            .emitter_retry_statuses
            .get(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Emitter,
                emitter.clone(),
            ))
            .map(|status| humantime::format_duration(status.value().reconnect.backoff).to_string())
    }

    pub(super) fn emitter_reconnect_wait_millis(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
    ) -> Option<u64> {
        self.inner
            .emitter_retry_statuses
            .get(&DomainNodeRef::node_in(
                domain.clone(),
                ModelKind::Emitter,
                emitter.clone(),
            ))
            .map(|status| {
                u64::try_from(
                    status
                        .value()
                        .reconnect
                        .retry_at
                        .saturating_duration_since(Instant::now())
                        .as_millis(),
                )
                .unwrap_or(u64::MAX)
            })
    }

    pub(in crate::runtime) fn spawn_emitter_task(
        &self,
        build: EmitterTaskBuildDeps<'_>,
        emitter: CreateEmitter,
        inputs: Vec<(RelayName, RelayRuntimeFanIn)>,
    ) -> Result<ScheduledEmitterTask, RuntimeError> {
        emitters::EmitterTask::spawn(self, build, emitter, inputs)
    }
}

#[cfg(test)]
mod tests {
    use tokio::{
        sync::{mpsc, watch},
        time::{Duration, Instant},
    };

    use super::*;

    #[tokio::test]
    async fn scheduled_emitter_stop_keeps_a_failed_drain_task_available_for_retry() {
        let grace = Duration::from_millis(50);
        let started = Instant::now();
        let (commands, mut command_rx) = mpsc::channel(2);
        let (stop_signal, _stop_rx) = watch::channel(None);
        let task = tokio::spawn(async move {
            let Some(EmitterTaskCommand::Stop { deadline, response }) = command_rx.recv().await
            else {
                panic!("expected the first emitter stop command");
            };
            assert!(deadline > started);
            assert!(deadline <= started + grace + Duration::from_millis(10));
            let _ = response.send(Err("transport drain failed".to_string()));

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

    #[tokio::test]
    async fn scheduled_emitter_stop_clears_signal_when_the_response_is_dropped() {
        let (commands, mut command_rx) = mpsc::channel(1);
        let (stop_signal, _stop_rx) = watch::channel(None);
        let task = tokio::spawn(async move {
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
}
