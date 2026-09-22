//! Kafka source runtime composition and domain-offset bridge.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Kafka connector plan with host-owned intake and replicated domain
//!   offsets.
//! - **Depends on.** The connector source contract, typed Kafka plans, and pre-resolved runtime
//!   handles.
//! - **Must not know.** The Kafka driver, NSPL parsing, registry validation, or placement
//!   computation.

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{SourceAckPolicy, SourceCapabilities, SourceConnector, SourcePlan};
use nervix_connector_kafka::{
    KafkaDomainOffsetError, KafkaDomainOffsetHost, KafkaDomainOffsetInitialization,
    KafkaDomainOffsetResult, KafkaDomainOffsetServices, KafkaDomainOffsetStart,
    KafkaOffsetPosition, KafkaSource, KafkaSourceOffsetMode, KafkaSourcePlan,
    TopicPartitionInspector,
};

use super::{
    super::*,
    source::{BrokerSourceHost, BrokerSourceHostSpec, run_source_instance},
};

pub(crate) struct KafkaIngestor;

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

impl KafkaIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: KafkaIngestorStartPlan,
        kafka_offset_state: Option<KafkaOffsetStateOriginator>,
    ) -> Result<(), RuntimeError> {
        let KafkaIngestorStartPlan {
            ingestor,
            client,
            topic,
            offset_mode,
            instances,
            mode,
            offset_state_placement: _,
        } = plan;
        let domain = &ingestor.domain;
        let key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.name.clone());
        if runtime.inner.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let acknowledgement = match &mode {
            KafkaIngestMode::AckParallel {
                max,
                batch_timeout,
                timeout,
                retry_policy,
            } => SourceAckPolicy::Parallel {
                max_in_flight: addressable_count(*max),
                batch_timeout: Runtime::parse_duration_setting(
                    domain,
                    &ingestor.name,
                    "batch timeout",
                    batch_timeout,
                )?,
                timeout: Runtime::parse_ack_timeout(domain, &ingestor.name, timeout)?,
                retry: Runtime::parse_retry_policy(domain, &ingestor.name, retry_policy)?,
            },
            KafkaIngestMode::AckSequential {
                timeout,
                retry_policy,
            } => SourceAckPolicy::Sequential {
                timeout: Runtime::parse_ack_timeout(domain, &ingestor.name, timeout)?,
                retry: Runtime::parse_retry_policy(domain, &ingestor.name, retry_policy)?,
            },
            KafkaIngestMode::NoAckParallel => SourceAckPolicy::None,
        };
        let capabilities = SourceCapabilities::new(
            ingestor.allow_header_reads,
            ingestor.metadata_kind.source_scope(),
            ingestor.quiesce.supports(ingestor.quiesce.mode()),
            instances,
            acknowledgement.support(),
        );
        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
        let branched_runtime = runtime.start_branched_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.branched_templates,
        );
        let quiesce = runtime
            .ingestor_quiesce_control(domain, &ingestor.name)
            .verified(
                "the runtime registers quiesce control for an ingestor before it starts the task",
            );
        let resolved_client = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;

        let (shutdown_tx, _) = watch::channel(false);
        let rebalance_tx = if offset_mode == KafkaOffsetMode::Domain {
            Some(watch::channel(0_u64).0)
        } else {
            None
        };
        let mut tasks = Vec::with_capacity(instances.get().arch_into());
        if let Some(rebalance_tx) = rebalance_tx.as_ref() {
            let inspector = TopicPartitionInspector::new(
                &resolved_client.entries,
                format!(
                    "nervix_domain_watch_{}_{}",
                    domain.as_str(),
                    ingestor.name.as_str(),
                ),
            )
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_topic = topic.clone();
            let task_events = runtime.events().clone();
            let mut shutdown = shutdown_tx.subscribe();
            let rebalance_tx = rebalance_tx.clone();
            tasks.push(tokio::spawn(async move {
                let mut observed = match inspector.partitions(task_topic.as_str()).await {
                    Ok(mut partitions) => {
                        partitions.sort_unstable();
                        partitions
                    }
                    Err(error) => {
                        task_events.report_error(format!(
                            "failed to inspect Kafka partitions for ingestor '{}' in domain '{}': \
                             {error}",
                            task_ingestor.as_str(),
                            task_domain.as_str(),
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
                    let mut current = match inspector.partitions(task_topic.as_str()).await {
                        Ok(partitions) => partitions,
                        Err(error) => {
                            task_events.report_error(format!(
                                "failed to inspect Kafka partitions for ingestor '{}' in domain \
                                 '{}': {error}",
                                task_ingestor.as_str(),
                                task_domain.as_str(),
                            ));
                            continue;
                        }
                    };
                    current.sort_unstable();
                    if current != observed {
                        observed = current.clone();
                        let epoch = rebalance_tx
                            .borrow()
                            .checked_add(1)
                            .assured("an ingestor cannot observe 2^64 partition rebalances");
                        rebalance_tx.send_replace(epoch);
                        info!(
                            domain = task_domain.as_str(),
                            ingestor = task_ingestor.as_str(),
                            topic = task_topic.as_str(),
                            partitions = ?current,
                            rebalance_epoch = epoch,
                            "detected Kafka partition topology change"
                        );
                    }
                }
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
                    return Err(RuntimeError::StartIngestor {
                        domain: domain.as_str().to_string(),
                        ingestor: ingestor.name.as_str().to_string(),
                        reason: "Kafka DOMAIN offsets are not authoritative on this node"
                            .to_string(),
                    });
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
                topic: topic.clone(),
                offset_mode: source_offset_mode,
                enable_auto_commit,
            },
            capabilities,
            acknowledgement,
        };
        runtime.prepare_ingestor_readiness(
            domain,
            &ingestor.name,
            source_plan.capabilities.instances(),
        );

        for instance_index in 0..source_plan.capabilities.instances().get() {
            let source = KafkaSource::open(&source_plan.connector, instance_index)
                .await
                .map_err(|error| RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: error.to_string(),
                })?;
            let host = BrokerSourceHost::build(BrokerSourceHostSpec {
                runtime: runtime.clone(),
                domain: domain.clone(),
                ingestor: ingestor.name.clone(),
                timestamp_source: ingestor.timestamp_source.clone(),
                output_routes: dependencies.output_routes.clone(),
                filter_where: dependencies.filter_where.clone(),
                codec: dependencies.codec.clone(),
                metrics: dependencies.metrics.clone(),
                branched_senders: branched_runtime.senders.clone(),
                quiesce: quiesce.clone(),
                shutdown: shutdown_tx.subscribe(),
                instance_index,
                metadata_kind: ingestor.metadata_kind,
                buffered_intake: false,
                flush_each_intake: false,
            });
            let shutdown = shutdown_tx.subscribe();
            let task_domain = domain.clone();
            let task_ingestor = ingestor.name.clone();
            let task_topic = topic.clone();
            let acknowledgement = source_plan.acknowledgement;
            let client_mounts = resolved_client.mounts.clone();
            tasks.push(tokio::spawn(async move {
                let _client_mounts = client_mounts;
                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    topic = task_topic.as_str(),
                    instance = instance_index,
                    "started Kafka ingestor"
                );
                run_source_instance(source, host, acknowledgement, shutdown).await;
                info!(
                    domain = task_domain.as_str(),
                    ingestor = task_ingestor.as_str(),
                    instance = instance_index,
                    "stopped Kafka ingestor"
                );
            }));
        }

        runtime.inner.ingestors.insert(
            key,
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
                tasks,
            },
        );
        Ok(())
    }
}
