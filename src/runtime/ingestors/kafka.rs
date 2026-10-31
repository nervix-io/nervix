//! Kafka ingestor runtime composition and domain-offset bridge.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Kafka connector plan with host-owned intake, replicated domain
//!   offsets, and the partition watch a domain-offset ingestor rebalances on.
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
    KafkaOffsetPosition, KafkaTopicIngestor, KafkaTopicIngestorError, KafkaTopicIngestorOffsetMode,
    KafkaTopicIngestorPlan, TopicPartitionInspector,
};

use super::{
    super::*,
    IngestorConnectorStartError, IngestorStartError,
    connector::{
        BrokerIngestorInstance, BrokerIngestorPolicy, IngestorCompanion, IngestorInstance,
        IngestorStart,
    },
};

struct RuntimeKafkaDomainOffsets {
    runtime: Runtime,
    lifecycle: domain_clock::DomainClockLifecycle,
    domain: DomainName,
    ingestor: IngestorName,
    topic: String,
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
        let schedule = if let Some(execution) = self.runtime.inner.executions.get(&self.domain)
            && let Some(node) = execution.revision.nodes.get(&NodeRef::new(
                ModelKind::Ingestor,
                ModelName::from(&self.ingestor),
            )) {
            node.kafka_partition_schedule.clone()
        } else {
            None
        };
        let state = self.state.current();
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
                        schedule,
                    });
                }
                // A failed first initialization may have published the start in memory before
                // its persistence or replica wait failed. Retry that durability boundary before
                // any instance resumes, including when another instance applied the start.
                let checkpoint = state
                    .read()
                    .latest_snapshot()
                    .change_context(KafkaDomainOffsetError::Read)?;
                self.runtime
                    .persist_kafka_offset_snapshot(
                        &state.persistence(),
                        checkpoint.lsm,
                        &checkpoint.payload,
                    )
                    .await
                    .change_context(KafkaDomainOffsetError::Read)?;
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
            schedule,
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
        self.runtime
            .reset_domain_kafka_offsets(&state, generation, positions)
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
        originator: KafkaOffsetStateOriginator,
    ) {
        let key = DomainNodeRef {
            domain: domain.clone(),
            node: ingestor.clone(),
        };
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
            unit: nervix_models::Unit::Single,
            branch_key: None,
        }
    }
}

/// Watches a domain-offset Kafka topic and tells the ingestor's instances when its partitions
/// change, so they rebalance.
struct KafkaPartitionWatch {
    inspector: TopicPartitionInspector,
    domain: DomainName,
    ingestor: IngestorName,
    topic: nervix_models::KafkaTopicName,
    events: RuntimeEvents,
    rebalance: watch::Sender<u64>,
}

impl IngestorCompanion for KafkaPartitionWatch {
    fn start(self: Box<Self>, shutdown: watch::Receiver<bool>) -> BoxFuture<'static, ()> {
        Box::pin(self.run(shutdown))
    }
}

impl KafkaPartitionWatch {
    fn report_failure(&self, error: &Report<KafkaTopicIngestorError>) {
        self.events.report_error(format!(
            "failed to inspect Kafka partitions for ingestor '{}' in domain '{}': {error:#}",
            self.ingestor.as_str(),
            self.domain.as_str(),
        ));
    }

    /// The topic's partitions in order, or `None` once a failed inspection is reported. The source
    /// reports a topic the broker does not have itself, so the watch observes it as a topic without
    /// partitions, and the partitions of the topic once provisioned read as a topology change.
    async fn observe(&self) -> Option<Vec<i32>> {
        let mut partitions = match self.inspector.partitions(self.topic.as_str()).await {
            Ok(partitions) => partitions,
            Err(error) if error.current_context().is_missing_topic() => Vec::new(),
            Err(error) => {
                self.report_failure(&error);
                return None;
            }
        };
        partitions.sort_unstable();
        Some(partitions)
    }

    async fn run(self: Box<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut observed = self.observe().await.unwrap_or_default();
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = sleep(DEFAULT_KAFKA_PARTITION_WATCH_INTERVAL) => {}
            }
            let Some(current) = self.observe().await else {
                continue;
            };
            if current != observed {
                observed = current.clone();
                let epoch = self
                    .rebalance
                    .borrow()
                    .checked_add(1)
                    .assured("an ingestor cannot observe 2^64 partition rebalances");
                self.rebalance.send_replace(epoch);
                info!(
                    domain = self.domain.as_str(),
                    ingestor = self.ingestor.as_str(),
                    topic = self.topic.as_str(),
                    partitions = ?current,
                    rebalance_epoch = epoch,
                    "detected Kafka partition topology change"
                );
            }
        }
    }
}

impl KafkaTopicIngestorStartPlan {
    /// Composes the Kafka ingestor, whose offset mode and partition watch shape its instances, so
    /// it opens them itself rather than through the broker launcher.
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<IngestorStart, IngestorStartError> {
        let KafkaTopicIngestorStartPlan {
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

        let rebalance_tx = match &offsets {
            KafkaOffsetPlan::Domain(_) => Some(watch::channel(0_u64).0),
            KafkaOffsetPlan::ConsumerGroup(_) => None,
        };
        let mut companions: Vec<Box<dyn IngestorCompanion>> = Vec::new();
        if let Some(rebalance_tx) = rebalance_tx.as_ref() {
            let inspector = TopicPartitionInspector::new(
                &resolved_client.entries,
                format!(
                    "nervix_domain_watch_{}_{}",
                    domain.as_str(),
                    ingestor.name.as_str(),
                ),
                runtime.executor().clone(),
            )
            .change_context_lazy(|| ingestor.initialize_failure())?;
            companions.push(Box::new(KafkaPartitionWatch {
                inspector,
                domain: domain.clone(),
                ingestor: ingestor.name.clone(),
                topic: topic.clone(),
                events: runtime.events().clone(),
                rebalance: rebalance_tx.clone(),
            }));
        }

        let mut domain_offsets = None;
        let source_offset_mode = match offsets {
            KafkaOffsetPlan::ConsumerGroup(group) => KafkaTopicIngestorOffsetMode::ConsumerGroup {
                group_id: group.as_str().to_string(),
                commit: KafkaGroupCommit::for_acknowledgement(acknowledgement),
            },
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
                    state,
                });
                KafkaTopicIngestorOffsetMode::Domain {
                    group_id: format!(
                        "nervix_domain_{}_{}",
                        domain.as_str(),
                        ingestor.name.as_str(),
                    ),
                    offsets,
                    rebalance: rebalance_tx
                        .as_ref()
                        .verified("DOMAIN offset mode creates the rebalance publisher above")
                        .subscribe(),
                }
            }
        };
        let source_plan = IngestorPlan {
            connector: KafkaTopicIngestorPlan {
                executor: runtime.executor().clone(),
                config: resolved_client.entries,
                topic,
                offset_mode: source_offset_mode,
            },
            capabilities: ingestor.ingestor_capabilities(instances, acknowledgement.support()),
            acknowledgement,
        };

        let policy =
            BrokerIngestorPolicy::new(acknowledgement, source_plan.capabilities.position_class());
        let mut opened: Vec<Box<dyn IngestorInstance>> =
            Vec::with_capacity(source_plan.capabilities.instances().get().arch_into());
        for instance_index in 0..source_plan.capabilities.instances().get() {
            nervix_primitives::task::consume_budget().await;
            let source = KafkaTopicIngestor::open(&source_plan.connector, instance_index)
                .await
                .change_context_lazy(|| ingestor.initialize_failure())?;
            opened.push(Box::new(BrokerIngestorInstance { source, policy }));
        }
        Ok(IngestorStart {
            instances: opened,
            companions,
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

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_primitives::time::timeout;

    use super::*;

    #[nervix_primitives::test]
    async fn runtime_report_chain_kafka_partition_watch_event() {
        let runtime = Runtime::default();
        let mut events = runtime.subscribe_events();
        let (rebalance, _changes) = watch::channel(0);
        let watch = KafkaPartitionWatch {
            inspector: TopicPartitionInspector::new(
                &[],
                "report-chain-test".into(),
                runtime.executor().clone(),
            )
            .assured("an inspector without brokers can be initialized without a lookup"),
            domain: domain("orders"),
            ingestor: named("source"),
            topic: named("orders"),
            events: runtime.events().clone(),
            rebalance,
        };
        let report = Report::new(KafkaTopicIngestorError::MissingMetadata {
            topic: "orders".into(),
        })
        .change_context(KafkaTopicIngestorError::InspectPartitions);
        watch.report_failure(&report);
        let RuntimeEvent::Error(event) = timeout(Duration::from_secs(1), events.recv())
            .await
            .assured("the partition inspection failure is queued before its observation deadline")
            .assured("a partition inspection failure publishes its report");
        assert_eq!(
            event,
            "failed to inspect Kafka partitions for ingestor 'source' in domain 'orders': Kafka \
             partition inspection task failed: Kafka returned no metadata for topic 'orders'"
        );
    }
}
