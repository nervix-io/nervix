use super::*;

#[derive(Debug, Error, PartialEq, Eq)]
pub(super) enum ScheduledNodeHandoffError {
    #[error("scheduled node task is unavailable for handoff")]
    CommandUnavailable,
    #[error("scheduled node task timed out accepting handoff")]
    CommandTimeout,
    #[error("scheduled node task dropped its handoff response")]
    ResponseDropped,
    #[error("scheduled node task timed out producing handoff residue")]
    ResponseTimeout,
    #[error("scheduled node task failed while stopping for handoff")]
    TaskJoin,
    #[error("scheduled node task timed out stopping for handoff")]
    TaskStopTimeout,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExecutionBuildDeps<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) relay_schemas: &'a HashMap<RelayName, Arc<CompiledSchema>>,
    pub(super) relay_branchings: &'a HashMap<RelayName, ResolvedBranching>,
    pub(super) materialized_relay_specs: &'a HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    pub(super) lookups: &'a HashMap<LookupName, Arc<LookupRuntime>>,
}

#[derive(Debug, Clone)]
pub(super) struct EmitterTaskDeps {
    pub(super) input_schema: Arc<CompiledSchema>,
    pub(super) input_branching: ResolvedBranching,
    pub(super) materialized_relay_specs: HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    pub(super) lookups: HashMap<LookupName, Arc<LookupRuntime>>,
}

#[derive(Debug, Clone)]
pub(super) struct EmitterTaskBuildDeps<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) shutdown_tx: &'a watch::Sender<bool>,
    pub(super) codecs: &'a HashMap<CodecName, Arc<CompiledCodec>>,
    pub(super) clients: &'a HashMap<ClientName, Arc<Model>>,
    pub(super) deps: EmitterTaskDeps,
}

/// Everything a scheduled node's assignment gives it on one cluster node: the replicated states it
/// owns or replicates and the background tasks that maintain them. Reassignment replaces this set
/// for the moved node without touching the rest of the domain.
#[derive(Default)]
pub(super) struct ScheduledNodePlacement {
    pub(super) tasks: Vec<JoinHandle<()>>,
    pub(super) kafka_offset_state: Option<KafkaOffsetStateOriginator>,
    pub(super) materialized_state: Option<MaterializedRelayStateOriginator>,
}

pub(super) struct ScheduledNodeTask {
    pub(super) commands: mpsc::Sender<ProcessorNodeCommand>,
    pub(super) task: JoinHandle<()>,
}

impl ScheduledNodeTask {
    pub(super) async fn abort_and_join(&mut self) {
        self.task.abort();
        (&mut self.task).join_after_shutdown("scheduled node").await;
    }

    pub(super) async fn handoff(
        self,
    ) -> error_stack::Result<Vec<ProcessorBranchHandoff>, ScheduledNodeHandoffError> {
        self.handoff_within(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE)
            .await
    }

    pub(super) async fn checkpoint_via(
        commands: &mpsc::Sender<ProcessorNodeCommand>,
    ) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
        let (response, receiver) = oneshot::channel();
        commands
            .send(ProcessorNodeCommand::Checkpoint { response })
            .await
            .map_err(|_| {
                OwnershipHandoffError::checkpoint(
                    "scheduled node task is unavailable for checkpoint",
                )
            })?;
        receiver.await.map_err(|_| {
            OwnershipHandoffError::checkpoint("scheduled node task dropped its checkpoint response")
        })?
    }

    pub(super) async fn handoff_within(
        mut self,
        grace_period: Duration,
    ) -> error_stack::Result<Vec<ProcessorBranchHandoff>, ScheduledNodeHandoffError> {
        let (response, receiver) = oneshot::channel();
        match tokio::time::timeout(
            grace_period,
            self.commands
                .send(ProcessorNodeCommand::Handoff { response }),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                self.abort_and_join().await;
                return Err(Report::new(ScheduledNodeHandoffError::CommandUnavailable));
            }
            Err(_) => {
                self.abort_and_join().await;
                return Err(Report::new(ScheduledNodeHandoffError::CommandTimeout));
            }
        }
        let handoffs = match tokio::time::timeout(grace_period, receiver).await {
            Ok(Ok(handoffs)) => handoffs,
            Ok(Err(_)) => {
                self.abort_and_join().await;
                return Err(Report::new(ScheduledNodeHandoffError::ResponseDropped));
            }
            Err(_) => {
                self.abort_and_join().await;
                return Err(Report::new(ScheduledNodeHandoffError::ResponseTimeout));
            }
        };
        match tokio::time::timeout(grace_period, &mut self.task).await {
            Ok(Ok(())) => Ok(handoffs),
            Ok(Err(error)) => {
                Err(Report::new(ScheduledNodeHandoffError::TaskJoin).attach_printable(error))
            }
            Err(_) => {
                self.task.abort();
                self.task.join_after_shutdown("scheduled node").await;
                Err(Report::new(ScheduledNodeHandoffError::TaskStopTimeout))
            }
        }
    }
}
