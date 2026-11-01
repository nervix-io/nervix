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

/// The node-local surfaces of one domain execution that the programs of its tasks bind against.
#[derive(Clone, Copy)]
pub(super) struct ExecutionBuildDeps<'a> {
    pub(super) domain: &'a DomainName,
    pub(super) relay_schemas: &'a HashMap<RelayName, Arc<CompiledSchema>>,
    pub(super) relay_branchings: &'a HashMap<RelayName, ResolvedBranching>,
    pub(super) materialized_relay_specs: &'a HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    pub(super) lookups: &'a HashMap<LookupName, Arc<LookupRuntime>>,
    pub(super) udfs: Option<&'a UdfExecutor>,
}

impl<'a> ExecutionBuildDeps<'a> {
    /// The surfaces an installed domain routing snapshot publishes.
    pub(super) fn from_routing(domain: &'a DomainName, routing: &'a DomainRoutingSnapshot) -> Self {
        Self {
            domain,
            relay_schemas: &routing.relay_schemas,
            relay_branchings: &routing.relay_branchings,
            materialized_relay_specs: &routing.materialized_stream_specs,
            lookups: &routing.lookups,
            udfs: Some(&routing.udfs),
        }
    }
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
    pub(super) deps: EmitterTaskDeps,
}

/// The node-owned state a scheduled node's placement replicates, as the domain's plans decided it.
#[derive(Debug, Clone)]
pub(super) enum PlacedNodeState {
    /// A materialized relay's rows, which its snapshots encode with this schema.
    MaterializedRelay(StdArc<arrow_schema::Schema>),
    /// A Kafka ingestor's domain offsets.
    KafkaDomainOffsets,
}

impl PlacedNodeState {
    /// The state the placement of `node` replicates, as the plans decided from its schedule
    /// describe it.
    pub(super) fn of(node: &ExecutionNode, revision: &ExecutionRevision) -> Option<Self> {
        match node.kind() {
            ModelKind::Relay => {
                let relay = revision
                    .activation
                    .relays
                    .get(&RelayName::from(&node.identifier))
                    .assured("the activation plan holds every relay of the schedule it came from");
                if relay.materialized {
                    Some(Self::MaterializedRelay(relay.schema.arrow_schema()))
                } else {
                    None
                }
            }
            ModelKind::Ingestor => {
                let plan = revision
                    .entrypoints
                    .ingestor(&IngestorName::from(&node.identifier))
                    .assured(
                        "the entrypoint plans hold every ingestor of the schedule they came from",
                    );
                if plan.keeps_domain_offsets() {
                    Some(Self::KafkaDomainOffsets)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Everything a scheduled node's assignment gives it on one cluster node: the replicated states it
/// owns or replicates and the background tasks that maintain them. Reassignment replaces this set
/// for the moved node without touching the rest of the domain.
#[derive(Default)]
pub(super) struct ScheduledNodePlacement {
    pub(super) tasks: PlacementTasks,
    pub(super) kafka_offset_state: Option<KafkaOffsetStateOriginator>,
    /// The originator of the materialized state of each unit of a materialized relay this node
    /// owns, handed to that unit's state task.
    pub(super) materialized_states: BTreeMap<Unit, MaterializedRelayStateOriginator>,
}

/// The background tasks a scheduled node's assignment runs on one cluster node.
#[derive(Default)]
pub(super) struct PlacementTasks {
    /// The tasks that replicate the node's state or poll its owner for it. A domain's stop ends
    /// each of them within its stop grace.
    pub(super) replication: Vec<JoinHandle<()>>,
    /// The writers of the checkpoints of the node's state. The runtime owns them, so a domain's
    /// stop tells each to make its last write once every task that changes that state has ended,
    /// and waits for it with no grace.
    pub(super) checkpoints: Vec<CheckpointWriter>,
}

impl PlacementTasks {
    pub(super) fn is_empty(&self) -> bool {
        self.replication.is_empty() && self.checkpoints.is_empty()
    }

    /// Ends every task without a last write, as a reassignment that replaces the placement's
    /// runtime does.
    pub(super) async fn abort(self) {
        for task in self.replication {
            nervix_primitives::task::consume_budget().await;
            task.abort();
            task.join_after_shutdown("placement").await;
        }
        for writer in self.checkpoints {
            nervix_primitives::task::consume_budget().await;
            writer.abort().await;
        }
    }
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
        purpose: ProcessorCheckpointPurpose,
    ) -> OwnershipHandoffResult<PersistedRuntimeStateEntry> {
        let (response, receiver) = oneshot::channel();
        commands
            .send(ProcessorNodeCommand::Checkpoint { purpose, response })
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
        match nervix_primitives::time::timeout(
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
        let handoffs = match nervix_primitives::time::timeout(grace_period, receiver).await {
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
        match nervix_primitives::time::timeout(grace_period, &mut self.task).await {
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

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_models::{
        CreateClientKafka, CreateIngestor, FlushPolicy, GeneralErrorPolicy, IngestQuiesceMode,
        IngestorEntity, KafkaConsumerGroupName, KafkaIngestMode, KafkaIngestor, KafkaOffsetMode,
        KafkaTopicIngestor, Model, OutputBranch, ParseAsType, ProcessorOutputs,
    };
    use nonzero_ext::nonzero;

    use super::*;

    fn kafka_ingestor(name: &str, offset_mode: KafkaOffsetMode) -> Model {
        Model::Ingestor(CreateIngestor {
            name: named(name),
            output_routes: with_inherit_all(ProcessorOutputs::single(named("events")))
                .with_flush_policy(FlushPolicy::Immediate)
                .with_branch(OutputBranch::Unbranched),
            input: nervix_models::IngestorInput::Transport(nervix_models::TransportIngestorInput {
                source: IngestorEntity::Kafka(KafkaIngestor::Topic(KafkaTopicIngestor {
                    client: named("kafka"),
                    topic: named("events"),
                    offset_mode,
                    instances: nonzero!(1u64),
                    mode: KafkaIngestMode::NoAck,
                    quiesce: IngestQuiesceMode::Suspend,
                })),
                codec: named("payload_codec"),
            }),
            timestamp_source: None,
            general_error_policy: GeneralErrorPolicy::Log,
            filter_where: None,
        })
    }

    #[test]
    fn a_placement_replicates_the_state_its_node_plan_keeps() {
        let domain = domain("default");
        let fixture = EntrypointTestDomain {
            relays: &["events", "state"],
            fields: &[("value", ParseAsType::I64)],
            branch_fields: &[],
        };
        let kafka_client = || {
            Model::ClientKafka(CreateClientKafka {
                name: named("kafka"),
                mount: None,
                config: Vec::new(),
            })
        };
        let mut plans = fixture.plans(
            &domain,
            vec![
                kafka_client(),
                kafka_ingestor("domain_offsets", KafkaOffsetMode::Domain),
                kafka_ingestor(
                    "group_offsets",
                    KafkaOffsetMode::ConsumerGroup(named::<KafkaConsumerGroupName>("group")),
                ),
            ],
        );
        let state_relay = plans
            .nodes
            .get_mut(&NodeRef::new(ModelKind::Relay, named::<ModelName>("state")))
            .assured("the fixture schedules its state relay");
        let Model::Relay(state_relay) = state_relay.config.as_mut() else {
            panic!("the fixture state node is a relay");
        };
        state_relay.materialized_state =
            Some(nervix_models::MaterializedRelayState::LastByTimestamp);
        let revision = ExecutionRevision::from_schedule(&DomainSchedule::new(
            domain,
            plans.nodes.into_values().collect::<Vec<_>>(),
            Vec::new(),
        ))
        .assured("the fixture schedule produces a complete typed revision");
        let placed = |kind, name: &str| {
            revision
                .nodes
                .get(&NodeRef::new(kind, named::<ModelName>(name)))
                .assured("the fixture schedules each node under test")
        };

        assert!(matches!(
            PlacedNodeState::of(placed(ModelKind::Ingestor, "domain_offsets"), &revision),
            Some(PlacedNodeState::KafkaDomainOffsets)
        ));
        assert!(
            PlacedNodeState::of(placed(ModelKind::Ingestor, "group_offsets"), &revision).is_none()
        );
        assert!(matches!(
            PlacedNodeState::of(placed(ModelKind::Relay, "state"), &revision),
            Some(PlacedNodeState::MaterializedRelay(schema))
                if schema == fixture.relay_schema().arrow_schema()
        ));
        assert!(PlacedNodeState::of(placed(ModelKind::Relay, "events"), &revision).is_none());
        assert!(PlacedNodeState::of(placed(ModelKind::Client, "kafka"), &revision).is_none());
    }
}
