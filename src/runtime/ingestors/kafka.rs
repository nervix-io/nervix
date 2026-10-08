//! Kafka source runtime composition and domain-offset bridge.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Kafka connector plan with host-owned intake, replicated domain
//!   offsets, and the partition watch a domain-offset source rebalances on.
//! - **Depends on.** The connector source contract, typed Kafka plans, and pre-resolved runtime
//!   handles.
//! - **Must not know.** The Kafka driver, NSPL parsing, registry validation, or placement
//!   computation.

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{SourceAckPolicy, SourceConnector, SourcePlan};
use nervix_connector_kafka::{
    KafkaDomainOffsetError, KafkaDomainOffsetHost, KafkaDomainOffsetInitialization,
    KafkaDomainOffsetResult, KafkaDomainOffsetServices, KafkaDomainOffsetStart,
    KafkaOffsetPosition, KafkaSource, KafkaSourceError, KafkaSourceOffsetMode, KafkaSourcePlan,
    TopicPartitionInspector,
};

use super::{
    super::*,
    IngestorStartError, SourceStartError,
    source::{BrokerSourceInstance, SourceCompanion, SourceInstance, SourceStart},
};

struct RuntimeKafkaDomainOffsets {
    runtime: Runtime,
    lifecycle: domain_clock::DomainClockLifecycle,
    domain: DomainName,
    ingestor: IngestorName,
    topic: String,
    state: KafkaOffsetStateOriginator,
}

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
        let start = match last_start {
            nervix_models::DomainStartPoint::Resume => {
                let missing_partition_timestamp = self
                    .runtime
                    .current_paced_domain_time(&self.domain)
                    .map_err(|error| {
                        Report::new(KafkaDomainOffsetError::Read)
                            .attach_printable(error.to_string())
                    })?;
                let mut positions = Vec::new();
                for partition in partitions {
                    if let Some(offset) = self.state.read().next_offset(&self.topic, *partition) {
                        positions.push(KafkaOffsetPosition {
                            topic: self.topic.clone(),
                            partition: *partition,
                            offset,
                        });
                    }
                }
                KafkaDomainOffsetStart::Resume {
                    positions,
                    missing_partition_timestamp,
                }
            }
            nervix_models::DomainStartPoint::At { timestamp, .. } => {
                KafkaDomainOffsetStart::At(timestamp)
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
        Ok(KafkaDomainOffsetInitialization {
            generation,
            start,
            schedule,
        })
    }

    async fn reset(&self, positions: Vec<KafkaOffsetPosition>) -> KafkaDomainOffsetResult<()> {
        self.runtime
            .reset_domain_kafka_offsets(&self.state, positions)
            .await
            .change_context(KafkaDomainOffsetError::Reset)
    }

    async fn commit(&self, position: KafkaOffsetPosition) -> KafkaDomainOffsetResult<()> {
        self.runtime
            .commit_domain_kafka_offset(&self.state, position)
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
}

impl IngestorSpec {
    /// Where this ingestor's domain offsets live as node-owned state.
    pub(in crate::runtime) fn kafka_offset_state_placement(&self) -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: self.domain.clone(),
            state: RuntimeState::KafkaOffset,
            kind: ModelKind::Ingestor,
            identifier: ModelName::from(&self.name),
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
    topic: nervix_models::TopicName,
    events: RuntimeEvents,
    rebalance: watch::Sender<u64>,
}

impl SourceCompanion for KafkaPartitionWatch {
    fn start(self: Box<Self>, shutdown: watch::Receiver<bool>) -> BoxFuture<'static, ()> {
        Box::pin(self.run(shutdown))
    }
}

impl KafkaPartitionWatch {
    fn report_failure(&self, error: &Report<KafkaSourceError>) {
        self.events.report_error(format!(
            "failed to inspect Kafka partitions for ingestor '{}' in domain '{}': {error:#}",
            self.ingestor.as_str(),
            self.domain.as_str(),
        ));
    }

    async fn run(self: Box<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut observed = match self.inspector.partitions(self.topic.as_str()).await {
            Ok(mut partitions) => {
                partitions.sort_unstable();
                partitions
            }
            Err(error) => {
                self.report_failure(&error);
                Vec::new()
            }
        };
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
            let mut current = match self.inspector.partitions(self.topic.as_str()).await {
                Ok(partitions) => partitions,
                Err(error) => {
                    self.report_failure(&error);
                    continue;
                }
            };
            current.sort_unstable();
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

impl KafkaIngestorStartPlan {
    /// Composes the Kafka source, whose offset mode and partition watch shape its instances, so
    /// it opens them itself rather than through the broker launcher.
    pub(super) async fn compose(
        self,
        runtime: &Runtime,
        ingestor: &IngestorSpec,
    ) -> error_stack::Result<SourceStart, IngestorStartError> {
        let KafkaIngestorStartPlan {
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
        let mut companions: Vec<Box<dyn SourceCompanion>> = Vec::new();
        if let Some(rebalance_tx) = rebalance_tx.as_ref() {
            let inspector = TopicPartitionInspector::new(
                &resolved_client.entries,
                format!(
                    "nervix_domain_watch_{}_{}",
                    domain.as_str(),
                    ingestor.name.as_str(),
                ),
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

        let enable_auto_commit = acknowledgement == SourceAckPolicy::None
            && matches!(offsets, KafkaOffsetPlan::ConsumerGroup(_));
        let source_offset_mode = match offsets {
            KafkaOffsetPlan::ConsumerGroup(group) => KafkaSourceOffsetMode::ConsumerGroup {
                group_id: group.as_str().to_string(),
            },
            KafkaOffsetPlan::Domain(placement) => {
                let Some(state) = runtime.kafka_offset_originator(ingestor, &placement) else {
                    return Err(ingestor.source_start_failure(
                        SourceStartError::KafkaDomainOffsetsNotAuthoritative,
                    ));
                };
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
                KafkaSourceOffsetMode::Domain {
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
        let source_plan = SourcePlan {
            connector: KafkaSourcePlan {
                config: resolved_client.entries,
                topic,
                offset_mode: source_offset_mode,
                enable_auto_commit,
            },
            capabilities: ingestor.source_capabilities(instances, acknowledgement.support()),
            acknowledgement,
        };

        // An unacknowledged Kafka consumer reopens after the host's fixed source error delay
        // rather than backing off, which the unacknowledged policy's zero retry expresses.
        let retry = acknowledgement.retry();
        let mut opened: Vec<Box<dyn SourceInstance>> =
            Vec::with_capacity(source_plan.capabilities.instances().get().arch_into());
        for instance_index in 0..source_plan.capabilities.instances().get() {
            nervix_primitives::task::consume_budget().await;
            let source = KafkaSource::open(&source_plan.connector, instance_index)
                .await
                .change_context_lazy(|| ingestor.initialize_failure())?;
            opened.push(Box::new(BrokerSourceInstance {
                source,
                acknowledgement,
                retry,
            }));
        }
        Ok(SourceStart {
            instances: opened,
            companions,
            buffered_intake: false,
            flush_each_intake: false,
            // A consumer-group member that stops polling for `max.poll.interval.ms` leaves its group,
            // which rebalances, and an instance reading domain offsets would keep its partitions
            // assigned through an ownership handoff.
            unacknowledged_admission: QueueAdmission::RefuseWhenFull,
            client_mounts: resolved_client.mounts.into_iter().collect(),
            connector_label: "kafka",
        })
    }
}

#[cfg(test)]
mod tests {
    use nervix_primitives::time::timeout;

    use super::*;

    #[nervix_primitives::test]
    async fn runtime_report_chain_kafka_partition_watch_event() {
        let runtime = Runtime::default();
        let mut events = runtime.subscribe_events();
        let (rebalance, _changes) = watch::channel(0);
        let watch = KafkaPartitionWatch {
            inspector: TopicPartitionInspector::new(&[], "report-chain-test".into())
                .assured("an inspector without brokers can be initialized without a lookup"),
            domain: domain("orders"),
            ingestor: named("source"),
            topic: named("orders"),
            events: runtime.events().clone(),
            rebalance,
        };
        let report = Report::new(KafkaSourceError::MissingMetadata {
            topic: "orders".into(),
        })
        .change_context(KafkaSourceError::InspectPartitions);
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
