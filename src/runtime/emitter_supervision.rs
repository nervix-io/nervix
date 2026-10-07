//! Emitter task commands, supervision and startup against its typed execution plan.
//!
//! Layer: data plane.
//! - **Owns.** The task's stop and reconfigure commands, lifecycle guards, and node-local plan
//!   binding before a task starts.
//! - **Depends on.** Typed emitter plans, relay fan-in, resolved resource mounts, and runtime
//!   services.
//! - **Must not know.** Semantic emitter or client Models, connector-specific sink construction,
//!   or placement decisions.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "sink supervision replaces and retires concrete sink task lifetimes"
    )
)]

use error_stack::ResultExt as _;

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

/// Why a scheduled emitter task did not stop. A drain the task itself failed keeps the task's own
/// report beneath [`ScheduledEmitterStopError::Drain`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(super) enum ScheduledEmitterStopError {
    #[error("scheduled emitter task is unavailable for stopping")]
    Unavailable,
    #[error("scheduled emitter task timed out accepting its stop command")]
    AcceptTimeout,
    #[error("scheduled emitter task dropped its stop response")]
    ResponseDropped,
    #[error("scheduled emitter task timed out draining")]
    DrainTimeout,
    #[error("scheduled emitter task failed to drain")]
    Drain,
}

/// A stop that failed, together with the task it leaves running, which a later stop can still end.
#[derive(Debug)]
pub(super) struct ScheduledEmitterStopFailure {
    error: Report<ScheduledEmitterStopError>,
    task: ScheduledEmitterTask,
}

/// Every way starting an emitter on this node fails, from binding its plan to the node's relays,
/// codecs and clients to compiling the programs its task runs. Each step reports its own variant
/// beneath [`EmitterStartError::Start`], which names the emitter and its domain.
#[derive(Debug, Error)]
pub(crate) enum EmitterStartError {
    #[error("failed to start emitter '{emitter}' in domain '{domain}'")]
    Start {
        domain: DomainName,
        emitter: EmitterName,
    },
    #[error("the emitter has no input relay")]
    NoInputRelay,
    #[error("input relay '{relay}' has no schema on this node")]
    MissingInputSchema { relay: RelayName },
    #[error("input relay '{relay}' has no resolved branching on this node")]
    MissingInputBranching { relay: RelayName },
    #[error("failed to resolve client '{client}'")]
    ResolveClient { client: ClientName },
    #[error("codec '{codec}' is not instantiated on this node")]
    MissingCodec { codec: CodecName },
    #[error("cannot publish batches through codec '{codec}'")]
    BatchCodec { codec: CodecName },
    #[error("the route program did not compile")]
    Route,
    #[error("the HTTP request fields did not compile")]
    HttpRequests,
    #[error("the ordering group did not compile")]
    OrderingGroup,
    #[error("the FROM WHERE of input relay '{relay}' did not compile")]
    SourceFilter { relay: RelayName },
    #[error("the sink's client configuration is invalid")]
    ClientConfig,
    #[error("the input collection policy is invalid")]
    CollectPolicy,
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

impl ScheduledEmitterStopFailure {
    fn new(error: Report<ScheduledEmitterStopError>, task: ScheduledEmitterTask) -> Self {
        Self { error, task }
    }

    /// The task's own description of a drain it failed, which the stop report holds as a printable
    /// attachment that a rendered chain does not show; absent when the stop failed before the
    /// task answered, or the task described nothing.
    pub(super) fn drain_description(&self) -> Option<String> {
        emitter_task::emitter_attached_message(&self.error)
    }

    /// The report of why the stop failed, and the task it leaves running.
    pub(super) fn into_parts(self) -> (Report<ScheduledEmitterStopError>, ScheduledEmitterTask) {
        (self.error, self.task)
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
    ) -> Result<(), ScheduledEmitterStopFailure> {
        let (response, receiver) = oneshot::channel();
        let deadline = Instant::now() + drain_timeout;
        let command = EmitterTaskCommand::Stop { deadline, response };
        match nervix_primitives::time::timeout_at(deadline, self.commands.send(command)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(ScheduledEmitterStopFailure::new(
                    Report::new(ScheduledEmitterStopError::Unavailable),
                    self,
                ));
            }
            Err(_) => {
                return Err(ScheduledEmitterStopFailure::new(
                    Report::new(ScheduledEmitterStopError::AcceptTimeout),
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
                return Err(ScheduledEmitterStopFailure::new(
                    Report::new(ScheduledEmitterStopError::ResponseDropped),
                    self,
                ));
            }
            Err(_) => {
                clear_emitter_stop_signal(&self.stop_signal, deadline);
                return Err(ScheduledEmitterStopFailure::new(
                    Report::new(ScheduledEmitterStopError::DrainTimeout),
                    self,
                ));
            }
        };
        if let Err(error) = response {
            clear_emitter_stop_signal(&self.stop_signal, deadline);
            return Err(ScheduledEmitterStopFailure::new(
                error.change_context(ScheduledEmitterStopError::Drain),
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
    ) -> error_stack::Result<EmitterTaskDeps, EmitterStartError> {
        let failure = |error: EmitterStartError| {
            Report::new(error).change_context(EmitterStartError::Start {
                domain: deps.domain.clone(),
                emitter: emitter.name.clone(),
            })
        };
        let Some(input_relay) = emitter.inputs.first().map(|input| &input.relay) else {
            return Err(failure(EmitterStartError::NoInputRelay));
        };
        let Some(input_schema) = deps.relay_schemas.get(input_relay).cloned() else {
            return Err(failure(EmitterStartError::MissingInputSchema {
                relay: input_relay.clone(),
            }));
        };
        let Some(input_branching) = deps.relay_branchings.get(input_relay).cloned() else {
            return Err(failure(EmitterStartError::MissingInputBranching {
                relay: input_relay.clone(),
            }));
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
    ) -> error_stack::Result<ScheduledEmitterTask, EmitterStartError> {
        let domain = build.domain;
        let start = EmitterStartError::Start {
            domain: domain.clone(),
            emitter: emitter.name.clone(),
        };
        let resolved = emitter.sink.clone().resolve_clients(|client| {
            self.resolve_client_config(domain, client.config.mount.as_ref(), &client.config.entries)
                .change_context_lazy(|| EmitterStartError::ResolveClient {
                    client: client.name.clone(),
                })
        });
        let plan = match resolved {
            Ok(plan) => plan,
            Err(error) => return Err(error.change_context(start)),
        };
        emitter_task::EmitterTask::spawn(self, build, emitter, plan, inputs).change_context(start)
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

    use nervix_models::{
        AckMode, CreateEmitter, CreateRelay, DomainSchedule, EmitSink, EmitterPublishingMode,
        ErrorPolicies, ProcessorInputs, RelayBranching, RetryPolicy,
    };
    use nervix_primitives::{
        sync::{mpsc, oneshot, watch},
        time::Instant,
    };
    use nonzero_ext::nonzero;

    use super::*;
    use crate::emitter_execution_plan::EmitterExecutionPlans;

    /// The execution plan of one ZeroMQ emitter that reads relay `events` through a Syslog codec,
    /// as a domain's installed schedule decides it.
    fn zero_mq_emitter_plan() -> EmitterExecutionPlan {
        let emitter = CreateEmitter {
            name: named("audit"),
            from: ProcessorInputs::new(vec![named("events")], Vec::new()),
            body: nervix_models::EmitterBody::Codec {
                codec: named("event_codec"),
            },
            sink: Box::new(EmitSink::ZeroMq {
                client: named("sink"),
            }),
            batch: None,
            flush_policy: FlushPolicy::Immediate,
            error_policies: ErrorPolicies::handled_by_log(),
            publishing_mode: EmitterPublishingMode::NoAck {
                retry_policy: RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            mode: AckMode::Attached,
            construction: nervix_models::RouteConstruction::default(),
            materialized_state: Vec::new(),
        };
        let schedule = DomainSchedule::new(
            domain("edge"),
            vec![
                scheduled_model(nervix_models::Model::Schema(nervix_models::CreateSchema {
                    name: named("event"),
                    fields: vec![nervix_models::SchemaField {
                        name: named("seq"),
                        ty: nervix_models::ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                })),
                scheduled_model(nervix_models::Model::Codec(nervix_models::CreateCodec {
                    name: named("event_codec"),
                    wire_format: nervix_models::CodecWireFormat::Syslog,
                    schema: named("event"),
                    encoding_rules: Vec::new(),
                })),
                scheduled_model(nervix_models::Model::ClientZeroMq(
                    nervix_models::CreateClientZeroMq {
                        name: named("sink"),
                        mount: None,
                        config: Vec::new(),
                    },
                )),
                scheduled_model(nervix_models::Model::Relay(CreateRelay {
                    name: named("events"),
                    schema: named("event"),
                    buffer: nonzero!(2usize),
                    branching: RelayBranching::unbranched(),
                    materialized_state: None,
                })),
                scheduled_model(nervix_models::Model::Emitter(emitter)),
            ],
            Vec::new(),
        );
        let activation =
            DomainActivationPlan::from_scheduled_nodes(&domain("edge"), &schedule.nodes)
                .assured("the fixture schedule declares its schema, codec and relay");
        let plans = EmitterExecutionPlans::from_scheduled_nodes(&schedule.nodes, &activation)
            .assured("the fixture emitter has its client, codec and input relay");
        plans
            .emitter(&named("audit"))
            .assured("the fixture schedule holds the emitter")
            .as_ref()
            .clone()
    }

    #[test]
    fn an_emitter_that_cannot_bind_its_input_names_itself_its_domain_and_the_relay() {
        let runtime = Runtime::default();
        let plan = zero_mq_emitter_plan();
        let relay_schemas = HashMap::default();
        let relay_branchings = HashMap::default();
        let materialized_relay_specs = HashMap::default();
        let lookups = HashMap::default();

        let error = runtime
            .emitter_task_deps(
                ExecutionBuildDeps {
                    domain: &domain("edge"),
                    relay_schemas: &relay_schemas,
                    relay_branchings: &relay_branchings,
                    materialized_relay_specs: &materialized_relay_specs,
                    lookups: &lookups,
                    udfs: None,
                },
                &plan,
            )
            .err()
            .assured("an input relay this node has no schema for cannot start the emitter");

        assert!(matches!(
            error.downcast_ref::<EmitterStartError>(),
            Some(EmitterStartError::Start { emitter, .. }) if emitter.as_str() == "audit"
        ));
        let names_the_failed_step = error.frames().any(|frame| {
            matches!(
                frame.downcast_ref::<EmitterStartError>(),
                Some(EmitterStartError::MissingInputSchema { relay }) if relay.as_str() == "events"
            )
        });
        assert!(
            names_the_failed_step,
            "the failed step must stay beneath the start context: {error:#}"
        );
        assert_eq!(
            format!("{error:#}"),
            "failed to start emitter 'audit' in domain 'edge': input relay 'events' has no schema \
             on this node"
        );
    }

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
        assert_eq!(
            failed.drain_description().as_deref(),
            Some("transport drain failed")
        );
        let (_, scheduled) = failed.into_parts();

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
        assert_eq!(failed.drain_description(), None);
        let (error, retained) = failed.into_parts();
        assert!(matches!(
            error.current_context(),
            ScheduledEmitterStopError::ResponseDropped
        ));
        assert!(
            retained.stop_signal.borrow().is_none(),
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
            error.drain_description().as_deref(),
            Some("emitter final flush failed: broker unavailable")
        );
        let (_, mut retained) = error.into_parts();
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

        assert_eq!(error.drain_description(), None);
        let (error, mut retained) = error.into_parts();
        assert!(matches!(
            error.current_context(),
            ScheduledEmitterStopError::ResponseDropped
        ));
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

        assert_eq!(error.drain_description(), None);
        let (error, mut retained) = error.into_parts();
        assert!(matches!(
            error.current_context(),
            ScheduledEmitterStopError::DrainTimeout
        ));
        assert!(!dropped.load(Ordering::Acquire));
        retained.task.abort();
        let _ = (&mut retained.task).await;
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn an_emitter_without_an_input_relay_or_its_branching_names_the_step_that_failed() {
        let runtime = Runtime::default();
        let edge = domain("edge");
        let relay_schemas = HashMap::from_iter([(
            named::<RelayName>("events"),
            test_schema(&[("value", ParseAsType::I64)]),
        )]);
        let relay_branchings = HashMap::default();
        let materialized_relay_specs = HashMap::default();
        let lookups = HashMap::default();
        let mut without_inputs = zero_mq_emitter_plan();
        without_inputs.inputs.clear();

        let no_input = runtime
            .emitter_task_deps(
                ExecutionBuildDeps {
                    domain: &edge,
                    relay_schemas: &relay_schemas,
                    relay_branchings: &relay_branchings,
                    materialized_relay_specs: &materialized_relay_specs,
                    lookups: &lookups,
                    udfs: None,
                },
                &without_inputs,
            )
            .err()
            .assured("an emitter without an input relay cannot start");
        assert_eq!(
            format!("{no_input:#}"),
            "failed to start emitter 'audit' in domain 'edge': the emitter has no input relay"
        );

        let no_branching = runtime
            .emitter_task_deps(
                ExecutionBuildDeps {
                    domain: &edge,
                    relay_schemas: &relay_schemas,
                    relay_branchings: &relay_branchings,
                    materialized_relay_specs: &materialized_relay_specs,
                    lookups: &lookups,
                    udfs: None,
                },
                &zero_mq_emitter_plan(),
            )
            .err()
            .assured("an input relay without resolved branching cannot start the emitter");
        assert_eq!(
            format!("{no_branching:#}"),
            "failed to start emitter 'audit' in domain 'edge': input relay 'events' has no \
             resolved branching on this node"
        );
    }

    #[nervix_primitives::test]
    async fn a_stop_its_task_cannot_take_retains_the_task_and_says_why() {
        let (commands, command_rx) = mpsc::channel(1);
        drop(command_rx);
        let (stop_signal, _) = watch::channel(None);
        let closed = ScheduledEmitterTask {
            commands,
            stop_signal,
            task: nervix_primitives::task::spawn(std::future::pending::<()>()),
        };
        let unavailable = closed
            .stop(Duration::from_secs(1))
            .await
            .expect_err("a task whose commands closed cannot take its stop");
        let (error, retained) = unavailable.into_parts();
        assert!(matches!(
            error.current_context(),
            ScheduledEmitterStopError::Unavailable
        ));
        retained.task.abort();

        let (commands, _command_rx) = mpsc::channel(1);
        let (queued_response, _queued_receiver) = oneshot::channel();
        commands
            .send(EmitterTaskCommand::Stop {
                deadline: Instant::now(),
                response: queued_response,
            })
            .await
            .expect("the command queue holds one command");
        let (stop_signal, _) = watch::channel(None);
        let busy = ScheduledEmitterTask {
            commands,
            stop_signal,
            task: nervix_primitives::task::spawn(std::future::pending::<()>()),
        };
        let unaccepted = busy
            .stop(Duration::from_millis(5))
            .await
            .expect_err("a task whose command queue stays full cannot take its stop");
        let (error, retained) = unaccepted.into_parts();
        assert!(matches!(
            error.current_context(),
            ScheduledEmitterStopError::AcceptTimeout
        ));
        retained.task.abort();
    }
}
