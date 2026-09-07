use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct ExecutionBuildDeps<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) relay_schemas: &'a HashMap<RelayName, Arc<CompiledSchema>>,
    pub(super) relay_branchings: &'a HashMap<RelayName, Vec<FieldName>>,
    pub(super) materialized_relay_specs: &'a HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    pub(super) lookups: &'a HashMap<LookupName, Arc<LookupRuntime>>,
}

#[derive(Debug, Clone)]
pub(super) struct EmitterTaskDeps {
    pub(super) input_schema: Arc<CompiledSchema>,
    pub(super) input_branching: Vec<FieldName>,
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
    pub(super) kafka_offset_state: Option<Arc<ReplicatedKafkaOffsetState>>,
    pub(super) materialized_state: Option<Arc<ReplicatedMaterializedRelayState>>,
}

pub(super) struct ScheduledNodeTask {
    pub(super) commands: mpsc::Sender<ProcessorNodeCommand>,
    pub(super) task: JoinHandle<()>,
}

impl ScheduledNodeTask {
    pub(super) async fn abort_and_join(&mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
    }

    pub(super) async fn handoff(self) -> Result<Vec<ProcessorBranchHandoff>, String> {
        self.handoff_within(PROCESSOR_BRANCH_TASK_SHUTDOWN_GRACE)
            .await
    }

    pub(super) async fn handoff_within(
        mut self,
        grace_period: Duration,
    ) -> Result<Vec<ProcessorBranchHandoff>, String> {
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
                return Err("scheduled node task is unavailable for handoff".to_string());
            }
            Err(_) => {
                self.abort_and_join().await;
                return Err("scheduled node task timed out accepting handoff".to_string());
            }
        }
        let handoffs = match tokio::time::timeout(grace_period, receiver).await {
            Ok(Ok(handoffs)) => handoffs,
            Ok(Err(_)) => {
                self.abort_and_join().await;
                return Err("scheduled node task dropped its handoff response".to_string());
            }
            Err(_) => {
                self.abort_and_join().await;
                return Err("scheduled node task timed out producing handoff residue".to_string());
            }
        };
        match tokio::time::timeout(grace_period, &mut self.task).await {
            Ok(Ok(())) => Ok(handoffs),
            Ok(Err(error)) => Err(format!("scheduled node task join failed: {error}")),
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                Err("scheduled node task timed out stopping for handoff".to_string())
            }
        }
    }
}
