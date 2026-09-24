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
    KafkaOffsetPosition, KafkaSource, KafkaSourceOffsetMode, KafkaSourcePlan,
    TopicPartitionInspector,
};

use super::{
    super::*,
    source::{BrokerSourceInstance, SourceCompanion, SourceInstance, SourceStart},
};

struct RuntimeKafkaDomainOffsets {
    runtime: Runtime,
    domain: DomainName,
    ingestor: IngestorName,
    topic: String,
    state: KafkaOffsetStateOriginator,
}

#[async_trait]
impl KafkaDomainOffsetServices for RuntimeKafkaDomainOffsets {
    fn generation(&self) -> Option<u64> {
        self.runtime
            .inner
            .domains
            .get(&self.domain)
            .map(|state| state.start_version)
    }

    async fn initialization(
        &self,
        partitions: &[i32],
    ) -> KafkaDomainOffsetResult<KafkaDomainOffsetInitialization> {
        let Some(domain_state) = self.runtime.inner.domains.get(&self.domain) else {
            return Err(
                Report::new(KafkaDomainOffsetError::Read).attach_printable(format!(
                    "domain '{}' is not installed",
                    self.domain.as_str()
                )),
            );
        };
        let generation = domain_state.start_version;
        let last_start = domain_state.last_start.clone();
        drop(domain_state);
        let schedule = if let Some(execution) = self.runtime.inner.executions.get(&self.domain)
            && let Some(node) = execution.schedule.nodes.get(&NodeRef::new(
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
        placement: Option<&KafkaOffsetStatePlacement>,
    ) -> Option<KafkaOffsetStateOriginator> {
        let placement = placement?;
        let dispatcher = self.inner.remote_dispatcher.load();
        let local_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id)?;
        if placement.primary_node.as_ref() != Some(local_node_id) {
            return None;
        }
        let state = self
            .inner
            .replicated_kafka_offset_states
            .get(&placement.placement)?;
        ReplicatedKafkaOffsetState::current_originator(state.value())
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
    async fn run(self: Box<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut observed = match self.inspector.partitions(self.topic.as_str()).await {
            Ok(mut partitions) => {
                partitions.sort_unstable();
                partitions
            }
            Err(error) => {
                self.events.report_error(format!(
                    "failed to inspect Kafka partitions for ingestor '{}' in domain '{}': {error}",
                    self.ingestor.as_str(),
                    self.domain.as_str(),
                ));
                Vec::new()
            }
        };
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
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
                    self.events.report_error(format!(
                        "failed to inspect Kafka partitions for ingestor '{}' in domain '{}': \
                         {error}",
                        self.ingestor.as_str(),
                        self.domain.as_str(),
                    ));
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
    ) -> Result<SourceStart, RuntimeError> {
        let KafkaIngestorStartPlan {
            client,
            topic,
            offset_mode,
            instances,
            mode,
            offset_state_placement,
        } = self;
        let domain = &ingestor.domain;
        let acknowledgement =
            Runtime::parse_ingest_acknowledgement(domain, &ingestor.name, mode.acknowledgement())?;
        let kafka_offset_state = runtime.kafka_offset_originator(offset_state_placement.as_ref());
        let resolved_client = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .map_err(|error| ingestor.start_failure(error.to_string()))?;

        let rebalance_tx = if offset_mode == KafkaOffsetMode::Domain {
            Some(watch::channel(0_u64).0)
        } else {
            None
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
            .map_err(|error| ingestor.start_failure(error.to_string()))?;
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
            && matches!(offset_mode, KafkaOffsetMode::ConsumerGroup(_));
        let source_offset_mode = match offset_mode {
            KafkaOffsetMode::ConsumerGroup(group) => KafkaSourceOffsetMode::ConsumerGroup {
                group_id: group.as_str().to_string(),
            },
            KafkaOffsetMode::Domain => {
                let Some(state) = kafka_offset_state else {
                    return Err(ingestor
                        .start_failure("Kafka DOMAIN offsets are not authoritative on this node"));
                };
                let offsets = KafkaDomainOffsetHost::new(RuntimeKafkaDomainOffsets {
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
            tokio::task::consume_budget().await;
            let source = KafkaSource::open(&source_plan.connector, instance_index)
                .await
                .map_err(|error| ingestor.start_failure(error.to_string()))?;
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
            client_mounts: resolved_client.mounts.into_iter().collect(),
            connector_label: "kafka",
        })
    }
}
