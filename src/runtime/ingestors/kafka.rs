//! Kafka ingestor runtime composition and domain-offset bridge.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Kafka connector plan with host-owned intake, replicated domain
//!   offsets and the committed external partition assignment.
//! - **Depends on.** The connector ingestor contract, typed Kafka plans, and pre-resolved runtime
//!   handles.
//! - **Must not know.** The Kafka driver, Plexus parsing, registry validation, or placement
//!   computation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{IngestorConnector, IngestorPlan};
use nervix_connector_kafka::{
    KafkaDomainOffsetError, KafkaDomainOffsetHost, KafkaDomainOffsetInitialization,
    KafkaDomainOffsetResult, KafkaDomainOffsetServices, KafkaDomainOffsetStart, KafkaGroupCommit,
    KafkaOffsetPosition, KafkaTopicIngestor, KafkaTopicIngestorOffsetMode, KafkaTopicIngestorPlan,
};

use super::{
    super::*,
    IngestorConnectorStartError, IngestorStartError,
    connector::{BrokerIngestorInstance, BrokerIngestorPolicy, IngestorStart},
};

struct RuntimeKafkaDomainOffsets {
    runtime: Runtime,
    lifecycle: domain_clock::DomainClockLifecycle,
    domain: DomainName,
    ingestor: IngestorName,
    topic: String,
    /// Fixed for this ingestor lifetime; a count change joins this ingestor before installing its
    /// replacement. One unit initializes directly through its native owner.
    instances: NonZeroU64,
    /// The originator this ingestor instance acts under, which schedule application replaces when
    /// a reassignment keeps the instance running.
    state: StdArc<KafkaOffsetOriginatorSlot>,
}

const _: () = assert!(matches!(
    KafkaTopicIngestor::KIND,
    nervix_models::IngestorEntityKind::Kafka(nervix_models::KafkaIngestorEntity::Topic)
));

#[async_trait]
impl KafkaDomainOffsetServices for RuntimeKafkaDomainOffsets {
    fn generation(&self) -> Option<u64> {
        self.lifecycle.generation()
    }

    async fn initialization(
        &self,
        partitions: &[i32],
    ) -> KafkaDomainOffsetResult<KafkaDomainOffsetInitialization> {
        let Some(domain_state) = self.lifecycle.task_state() else {
            return Err(
                Report::new(KafkaDomainOffsetError::Read).attach_printable(format!(
                    "domain '{}' is not installed",
                    self.domain.as_str()
                )),
            );
        };
        let generation = domain_state.generation;
        let last_start = domain_state.last_start.clone();
        let mut assignment = None;
        if let Some(execution) = self.runtime.inner.executions.get(&self.domain)
            && !execution
                .revision
                .partition_topology
                .suspended
                .contains(&self.ingestor)
            && let Some(observation) = execution
                .revision
                .partition_topology
                .sources
                .get(&self.ingestor)
            && let Some(partitions) = observation.partitions
        {
            assignment = Some(nervix_models::ExternalPartitionAssignment {
                partitions,
                readers: self.instances,
                epoch: observation.epoch,
            });
        }
        let state = self.state.current();
        if self.instances.get() > 1 {
            self.runtime
                .synchronize_kafka_start(&state, generation)
                .await
                .change_context(KafkaDomainOffsetError::Read)?;
        }
        let missing_partition_timestamp = match last_start {
            nervix_models::DomainStartPoint::Resume => self
                .runtime
                .current_paced_domain_time(&self.domain)
                .map_err(|error| {
                    Report::new(KafkaDomainOffsetError::Read).attach_printable(error.to_string())
                })?,
            nervix_models::DomainStartPoint::At { timestamp, .. } => {
                if state.read().applied_start_generation() != Some(generation) {
                    return Ok(KafkaDomainOffsetInitialization {
                        generation,
                        start: KafkaDomainOffsetStart::At(timestamp),
                        assignment,
                    });
                }
                Some(timestamp)
            }
            nervix_models::DomainStartPoint::Now { .. } => {
                return Err(
                    Report::new(KafkaDomainOffsetError::Read).attach_printable(format!(
                        "domain '{}' has an unresolved START AT NOW",
                        self.domain.as_str(),
                    )),
                );
            }
        };
        let mut positions = Vec::new();
        for partition in partitions {
            if let Some(offset) = state.read().next_offset(&self.topic, *partition) {
                positions.push(KafkaOffsetPosition {
                    topic: self.topic.clone(),
                    partition: *partition,
                    offset,
                });
            }
        }
        Ok(KafkaDomainOffsetInitialization {
            generation,
            start: KafkaDomainOffsetStart::Resume {
                positions,
                missing_partition_timestamp,
            },
            assignment,
        })
    }

    async fn reset(
        &self,
        generation: u64,
        positions: Vec<KafkaOffsetPosition>,
    ) -> KafkaDomainOffsetResult<()> {
        if self.lifecycle.generation() != Some(generation) {
            return Err(Report::new(KafkaDomainOffsetError::Reset).attach_printable(
                "the domain start generation changed before Kafka initialization",
            ));
        }
        let state = self.state.current();
        if self.instances.get() == 1 {
            return self
                .runtime
                .start_single_kafka_unit(&state, generation, positions)
                .await
                .change_context(KafkaDomainOffsetError::Reset);
        }
        self.runtime
            .propose_kafka_start(&state, generation, positions)
            .await
            .change_context(KafkaDomainOffsetError::Reset)
    }

    async fn commit(&self, position: KafkaOffsetPosition) -> KafkaDomainOffsetResult<()> {
        let state = self.state.current();
        self.runtime
            .commit_domain_kafka_offset(&state, position)
            .await
            .change_context(KafkaDomainOffsetError::Commit)
    }
}

impl Runtime {
    /// The domain offsets this node originates for a Kafka ingestor, while it is the primary its
    /// offset state was placed on.
    fn kafka_offset_originator(
        &self,
        ingestor: &IngestorSpec,
        placement: &KafkaDomainOffsetPlacement,
    ) -> Option<KafkaOffsetStateOriginator> {
        let dispatcher = self.inner.remote_dispatcher.load();
        let local_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id)?;
        if placement.primary_node.as_ref() != Some(local_node_id) {
            return None;
        }
        let state = self
            .inner
            .replicated_kafka_offset_states
            .get(&ingestor.kafka_offset_state_placement())?;
        ReplicatedKafkaOffsetState::current_originator(state.value())
    }

    /// Hands a Kafka ingestor that keeps running here across a reassignment the originator its
    /// offsets' new binding issued, since the rebind superseded the one the ingestor holds. While
    /// the ingestor is not running there is nothing to hand it to: its next start takes the
    /// binding in force.
    pub(in crate::runtime) fn reissue_kafka_offset_originator(
        &self,
        domain: &DomainName,
        ingestor: &NodeRef,
        unit: Unit,
        originator: KafkaOffsetStateOriginator,
    ) {
        let key = DomainNodeUnitRef::new(
            DomainNodeRef {
                domain: domain.clone(),
                node: ingestor.clone(),
            },
            unit,
        );
        let Some(running) = self.inner.ingestors.get(&key) else {
            return;
        };
        if let Some(slot) = running.domain_offsets.as_ref() {
            slot.replace(originator);
        }
    }
}

impl IngestorSpec {
    /// Where this ingestor's domain offsets live as node-owned state.
    pub(in crate::runtime) fn kafka_offset_state_placement(&self) -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: self.domain.clone(),
            state: RuntimeState::KafkaOffset,
            kind: ModelKind::Ingestor,
            identifier: ModelName::from(&self.name),
            unit: self.unit,
            branch_key: None,
        }
    }
}

impl KafkaTopicIngestorStartPlan {
    /// Composes each scheduled Kafka instance with its committed partition and offset owners.
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let KafkaTopicIngestorStartPlan {
            committed_partitions,
            client,
            topic,
            offsets,
            instances,
            mode,
        } = self;
        let domain = &ingestor.domain;
        let acknowledgement =
            Runtime::parse_ingest_acknowledgement(domain, &ingestor.name, mode.acknowledgement())?;
        let resolved_client = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(|| ingestor.initialize_failure())?;

        let mut domain_offsets = None;
        let connector_offset_mode = match offsets {
            KafkaOffsetPlan::ConsumerGroup { group, start_at } => {
                KafkaTopicIngestorOffsetMode::ConsumerGroup {
                    group_id: group.as_str().to_string(),
                    commit: KafkaGroupCommit::for_acknowledgement(acknowledgement),
                    start_at,
                    committed_partitions: committed_partitions.map(|partitions| {
                        Box::new(nervix_connector_kafka::KafkaGroupPartitionRead::new(
                            partitions,
                        ))
                    }),
                }
            }
            KafkaOffsetPlan::Domain(placement) => {
                let Some(originator) = runtime.kafka_offset_originator(ingestor, &placement) else {
                    return Err(ingestor.connector_start_failure(
                        IngestorConnectorStartError::KafkaDomainOffsetsNotAuthoritative,
                    ));
                };
                let state = StdArc::new(KafkaOffsetOriginatorSlot::new(originator));
                domain_offsets = Some(state.clone());
                let offsets = KafkaDomainOffsetHost::new(RuntimeKafkaDomainOffsets {
                    lifecycle: runtime
                        .domain_clock_lifecycle(domain)
                        .change_context_lazy(|| ingestor.initialize_failure())?,
                    runtime: runtime.clone(),
                    domain: domain.clone(),
                    ingestor: ingestor.name.clone(),
                    topic: topic.as_str().to_string(),
                    instances,
                    state,
                });
                KafkaTopicIngestorOffsetMode::Domain {
                    group_id: format!(
                        "nervix_domain_{}_{}",
                        domain.as_str(),
                        ingestor.name.as_str(),
                    ),
                    offsets,
                }
            }
        };
        let connector_plan = IngestorPlan {
            connector: KafkaTopicIngestorPlan {
                settings: runtime.inner.connector_host.kafka,
                executor: runtime.executor().clone(),
                config: resolved_client.entries,
                topic,
                offset_mode: connector_offset_mode,
            },
            capabilities: ingestor.ingestor_capabilities(instances, acknowledgement.support()),
            acknowledgement,
        };

        let policy = BrokerIngestorPolicy::new(
            acknowledgement,
            connector_plan.capabilities.position_class(),
        );
        let connector =
            KafkaTopicIngestor::open(&connector_plan.connector, ingestor.instance_index())
                .await
                .change_context_lazy(|| ingestor.initialize_failure())?;
        let instance = Box::new(BrokerIngestorInstance { connector, policy });
        Ok(IngestorStart {
            instance,
            buffered_intake: false,
            flush_each_intake: false,
            // A consumer-group member that stops polling for `max.poll.interval.ms` leaves its group,
            // which rebalances, and an instance reading domain offsets would keep its partitions
            // assigned through an ownership handoff.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts: resolved_client.mounts.into_iter().collect(),
            connector_label: KafkaTopicIngestor::KIND
                .connector()
                .keyword()
                .to_ascii_lowercase(),
            domain_offsets,
        })
    }
}
