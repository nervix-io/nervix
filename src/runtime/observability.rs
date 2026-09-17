//! Runtime-state observation projections.
//!
//! Layer: data plane.
//! - **Owns.** Read-only descriptions and metrics derived from installed runtime state.
//! - **Depends on.** Execution plans, branch-local state and runtime metric collectors.
//! - **Must not know.** NSPL parsing, persistence mutations or external telemetry transport.

use super::*;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RuntimeObservationError {
    #[error("{reason}")]
    DomainInstantiation { domain: DomainName, reason: String },
    #[error("domain '{}' is not instantiated", .domain.as_str())]
    DomainNotInstantiated { domain: DomainName },
    #[error(
        "lookup '{}' is not instantiated in domain '{}'",
        .lookup.as_str(),
        .domain.as_str()
    )]
    LookupNotInstantiated {
        domain: DomainName,
        lookup: LookupName,
    },
    #[error(
        "failed to read lookup '{}' in domain '{}': {error}",
        .lookup.as_str(),
        .domain.as_str()
    )]
    LookupRecord {
        domain: DomainName,
        lookup: LookupName,
        error: error_stack::Report<crate::runtime_schema::RuntimeSchemaError>,
    },
}

pub(super) fn kafka_domain_offset_describe_from_schedule(
    topic: &str,
    instances: NonZeroU64,
    schedule: &KafkaPartitionSchedule,
) -> KafkaDomainOffsetDescribe {
    let mut instance_assignments = schedule.instance_assignments.clone();
    let expected_instances = instances.get().arch_into();
    if instance_assignments.len() < expected_instances {
        instance_assignments.resize(expected_instances, Vec::new());
    }
    KafkaDomainOffsetDescribe {
        topic: topic.to_string(),
        instances: instances.get(),
        observed_partitions: schedule.observed_partitions.clone(),
        rebalance_epoch: schedule.rebalance_epoch,
        instance_assignments,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KafkaDomainOffsetDescribe {
    pub(crate) topic: String,
    pub(crate) instances: u64,
    pub(crate) observed_partitions: Vec<i32>,
    pub(crate) rebalance_epoch: u64,
    pub(crate) instance_assignments: Vec<Vec<i32>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IngestorDescribe {
    pub(crate) running: bool,
    pub(crate) ready: bool,
    pub(crate) quiesce_state: Option<String>,
    pub(crate) quiesce_counters: IngestorQuiesceCounters,
    pub(crate) memory_backpressure_paused: bool,
    pub(crate) transient_error: Option<String>,
    pub(crate) reconnect_backoff: Option<String>,
    pub(crate) reconnect_wait_millis: Option<u64>,
    pub(crate) kafka_domain_offsets: Option<KafkaDomainOffsetDescribe>,
}

impl Runtime {
    pub(super) fn register_branch_lifecycle_metrics(
        &self,
        domain: &DomainName,
        branch: Option<&BranchName>,
    ) {
        if let Some(branch) = branch {
            let dispatcher = self.inner.remote_dispatcher.load();
            self.inner.metrics.register_branch(
                domain,
                branch,
                dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
            );
        }
    }

    pub(super) fn observe_branch_instance_created(
        &self,
        domain: &DomainName,
        branch: Option<&BranchName>,
        key: &Option<BranchKey>,
    ) {
        if let Some(branch) = branch {
            let dispatcher = self.inner.remote_dispatcher.load();
            self.inner.metrics.observe_branch_instance_created(
                domain,
                branch,
                dispatcher.as_deref().map(RemoteDispatcher::local_node_id),
                branch_key_display(key),
            );
        }
    }

    pub(super) fn observe_branch_instance_removed(
        &self,
        domain: &DomainName,
        branch: Option<&BranchName>,
        key: &Option<BranchKey>,
        reason: Option<BranchEvictionReason>,
    ) {
        let Some(branch) = branch else {
            return;
        };
        let dispatcher = self.inner.remote_dispatcher.load();
        let physical_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
        if let Some(reason) = reason {
            self.inner.metrics.observe_branch_instance_removed(
                domain,
                branch,
                physical_node_id,
                branch_key_display(key),
                reason,
            );
        } else {
            self.inner.metrics.observe_branch_instance_detached(
                domain,
                branch,
                physical_node_id,
                branch_key_display(key),
            );
        }
    }
}

/// One instantiated lookup as this node sees it: the model it was built from, the resource
/// version it loaded, and how many entries that version produced.
pub(crate) struct LocalLookupDescription {
    pub(crate) model: CreateLookup,
    pub(crate) resource_version: u64,
    pub(crate) entry_count: usize,
}

/// A connector's reconnect state, reported alongside its dataflow node status.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DataflowNodeTransientState {
    pub(crate) error: Option<String>,
    pub(crate) reconnect_backoff: Option<String>,
    pub(crate) reconnect_wait_millis: Option<u64>,
}

impl Runtime {
    pub(in crate::runtime) fn mark_branch_aggregated_metrics_updated(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) {
        if kind == ModelKind::Relay {
            return;
        }
        let identifier = identifier.into();
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::BranchAggregated,
            kind,
            identifier,
            None,
        );
        if let Some(state) = self
            .inner
            .replicated_branch_aggregated_states
            .get(&placement)
        {
            state.mark_metrics_updated();
        }
    }

    pub(crate) fn describe_local_stream_exists(
        &self,
        domain: &DomainName,
        relay: &RelayName,
        key: &Option<BranchKey>,
    ) -> Result<bool, RuntimeError> {
        let Some(execution) = self.inner.executions.get(domain) else {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        };
        if !execution.relay_registries.contains_key(relay) {
            return Err(RuntimeError::RelayNotInstantiated {
                domain: domain.as_str().to_string(),
                relay: relay.as_str().to_string(),
            });
        }
        let relay_registry = execution
            .relay_registries
            .get(relay)
            .verified("the missing-relay branch above already returned");
        Ok(relay_registry.contains_key(key))
    }

    pub(crate) fn describe_metrics_for(
        &self,
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
    ) -> Vec<String> {
        let identifier = identifier.into();
        if let Err(error) =
            self.refresh_branch_aggregated_metrics_for_target(domain, kind, identifier.clone())
        {
            warn!(
                domain = domain.as_str(),
                kind,
                identifier = identifier.as_str(),
                error = %error,
                "failed to refresh branch-aggregated metrics before describe"
            );
        }
        self.inner
            .metrics
            .describe_global_target(domain, kind, identifier)
    }

    pub(crate) fn describe_wasm_processor_state_for(
        &self,
        domain: &DomainName,
        processor: impl Into<ModelName>,
    ) -> Vec<String> {
        let processor = processor.into();
        let mut branch_count = 0_usize;
        let mut dirty_count = 0_usize;
        let mut pending_replica_count = 0_usize;
        for state in self.inner.replicated_wasm_processor_states.iter() {
            let placement = &state.placement;
            if &placement.domain != domain
                || placement.kind != ModelKind::WasmProcessor
                || placement.identifier != processor
            {
                continue;
            }
            branch_count += 1;
            if state.is_dirty() {
                dirty_count += 1;
            }
            if !state.replica_quorum_satisfied(state.saved_revision()) {
                pending_replica_count += 1;
            }
        }
        vec![
            format!("state structures: {branch_count}"),
            format!("dirty state structures: {dirty_count}"),
            format!("replica pending state structures: {pending_replica_count}"),
        ]
    }

    pub(crate) fn describe_domain_statistics(&self, domain: &DomainName) -> Vec<String> {
        self.inner.metrics.describe_domain_statistics(domain)
    }

    pub(crate) fn dataflow_domain_statistics(
        &self,
        domain: &DomainName,
    ) -> nervix_dataflow_graph::DataflowStatistics {
        self.inner.metrics.dataflow_domain_statistics(domain)
    }

    pub(crate) fn dataflow_edge_statistics(
        &self,
        domain: &DomainName,
        metric: &nervix_dataflow_graph::DataflowMetricRef,
    ) -> nervix_dataflow_graph::DataflowStatistics {
        self.inner.metrics.dataflow_edge_statistics(domain, metric)
    }

    pub(crate) fn dataflow_relay_buffer_statistics(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> nervix_dataflow_graph::DataflowStatistics {
        self.inner
            .metrics
            .dataflow_relay_buffer_statistics(domain, relay)
    }

    pub(crate) fn dataflow_edge_branch_statistics(
        &self,
        domain: &DomainName,
        metric: &nervix_dataflow_graph::DataflowMetricRef,
    ) -> Vec<nervix_dataflow_graph::DataflowBranchStatistics> {
        self.inner
            .metrics
            .dataflow_edge_branch_statistics(domain, metric)
    }

    pub(crate) fn dataflow_relay_branch_statistics(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Vec<nervix_dataflow_graph::DataflowBranchStatistics> {
        let Some(execution) = self.inner.executions.get(domain) else {
            return Vec::new();
        };
        let Some(registry) = execution.relay_registries.get(relay) else {
            return Vec::new();
        };
        registry
            .keys()
            .into_iter()
            .map(|branch| nervix_dataflow_graph::DataflowBranchStatistics {
                branch,
                statistics: Default::default(),
            })
            .collect()
    }

    pub(crate) fn dataflow_node_status(
        &self,
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
    ) -> nervix_dataflow_graph::DataflowNodeHealth {
        let identifier = identifier.into();
        let reconnect_wait_millis = if kind.eq_ignore_ascii_case("INGESTOR") {
            self.ingestor_reconnect_wait_millis(domain, &IngestorName::from(&identifier))
        } else if kind.eq_ignore_ascii_case("EMITTER") {
            self.emitter_reconnect_wait_millis(domain, &EmitterName::from(&identifier))
        } else {
            None
        };
        let detail = if kind.eq_ignore_ascii_case("INGESTOR") {
            if let Some(error) =
                self.ingestor_transient_error(domain, &IngestorName::from(&identifier))
            {
                if let Some(backoff) =
                    self.ingestor_reconnect_backoff(domain, &IngestorName::from(&identifier))
                {
                    Some(format!("{error}; reconnect backoff: {backoff}"))
                } else {
                    Some(error)
                }
            } else if self
                .inner
                .fault_injection
                .ingestor_is_failed(&IngestorName::from(&identifier))
            {
                Some("ingestor fault injector failed source".to_string())
            } else {
                None
            }
        } else if kind.eq_ignore_ascii_case("EMITTER") {
            if let Some(error) =
                self.emitter_transient_error(domain, &EmitterName::from(&identifier))
            {
                if let Some(backoff) =
                    self.emitter_reconnect_backoff(domain, &EmitterName::from(&identifier))
                {
                    Some(format!("{error}; reconnect backoff: {backoff}"))
                } else {
                    Some(error)
                }
            } else if self
                .inner
                .fault_injection
                .emitter_should_fail(&EmitterName::from(&identifier))
                || self
                    .inner
                    .fault_injection
                    .emitter_should_stall(&EmitterName::from(&identifier))
            {
                Some("emitter fault injector failed publish".to_string())
            } else {
                None
            }
        } else {
            None
        };
        if let Some(detail) = detail {
            return nervix_dataflow_graph::DataflowNodeHealth {
                status: nervix_dataflow_graph::DataflowNodeStatus::Error,
                detail: Some(detail),
                reconnect_wait_millis,
            };
        }
        // A full pool is a waiting state rather than a failure, so the node stays healthy and says
        // what it is waiting for. Without this it would read as idle while holding no connection.
        let Ok(model_kind) = kind.to_ascii_lowercase().parse::<ModelKind>() else {
            return nervix_dataflow_graph::DataflowNodeHealth::default();
        };
        match self.pool_wait(&DomainNodeRef::node_in(
            domain.clone(),
            model_kind,
            identifier,
        )) {
            Some(wait) => nervix_dataflow_graph::DataflowNodeHealth {
                status: nervix_dataflow_graph::DataflowNodeStatus::Waiting,
                detail: Some(wait.describe()),
                reconnect_wait_millis,
            },
            None => nervix_dataflow_graph::DataflowNodeHealth::default(),
        }
    }

    /// A connector's reconnect state: the transient error it last hit, the backoff it is waiting
    /// out, and how much of that wait is left. A node that is neither an ingestor nor an emitter
    /// has none of the three.
    pub(crate) fn dataflow_node_transient_state(
        &self,
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
    ) -> DataflowNodeTransientState {
        let identifier = identifier.into();
        if kind.eq_ignore_ascii_case("INGESTOR") {
            DataflowNodeTransientState {
                error: self.ingestor_transient_error(domain, &IngestorName::from(&identifier)),
                reconnect_backoff: self
                    .ingestor_reconnect_backoff(domain, &IngestorName::from(&identifier)),
                reconnect_wait_millis: self
                    .ingestor_reconnect_wait_millis(domain, &IngestorName::from(&identifier)),
            }
        } else if kind.eq_ignore_ascii_case("EMITTER") {
            DataflowNodeTransientState {
                error: self.emitter_transient_error(domain, &EmitterName::from(&identifier)),
                reconnect_backoff: self
                    .emitter_reconnect_backoff(domain, &EmitterName::from(&identifier)),
                reconnect_wait_millis: self
                    .emitter_reconnect_wait_millis(domain, &EmitterName::from(&identifier)),
            }
        } else {
            DataflowNodeTransientState::default()
        }
    }

    pub(in crate::runtime) fn refresh_branch_aggregated_metrics_for_target(
        &self,
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
    ) -> Result<(), RuntimePersistenceError> {
        let identifier = identifier.into();
        let Ok(kind) = kind.to_ascii_lowercase().parse::<ModelKind>() else {
            return Ok(());
        };
        if kind == ModelKind::Relay {
            return Ok(());
        }
        let Some(store) = &self.inner.state_store else {
            return Ok(());
        };
        let mut placements = Vec::new();
        for entry in self.inner.replicated_branch_aggregated_states.iter() {
            let placement = entry.key();
            if &placement.domain == domain
                && placement.kind == kind
                && placement.identifier == identifier
            {
                placements.push(placement.clone());
            }
        }
        for placement in placements {
            let Some(state) = self
                .inner
                .replicated_branch_aggregated_states
                .get(&placement)
            else {
                continue;
            };
            if let Some(snapshot) = store.latest_snapshot(&placement)? {
                state.restore_persisted_snapshot(&self.inner.metrics, snapshot)?;
            }
        }
        let placement = self.state_placement(
            domain,
            RuntimeStateKind::BranchAggregated,
            kind,
            identifier.clone(),
            None,
        );
        if !self
            .inner
            .metrics
            .has_global_target_measurements(domain, kind, identifier)
            && let Some(snapshot) = store.latest_snapshot(&placement)?
        {
            let decoded = decode_branch_aggregated_snapshot(&snapshot.payload)?;
            self.inner.metrics.apply_global_snapshot(decoded.metrics);
        }
        Ok(())
    }

    pub(crate) fn describe_local_ingestor(
        &self,
        domain: &DomainName,
        ingestor: &IngestorName,
    ) -> error_stack::Result<IngestorDescribe, RuntimeObservationError> {
        let memory_backpressure_paused = self.ingestors_paused_for_memory_pressure();
        let quiesce_control = self.ingestor_quiesce_control(domain, ingestor);
        let quiesce_state = match quiesce_control.as_ref() {
            Some(control) => control.cause().map(|cause| cause.as_str().to_string()),
            None => None,
        };
        let quiesce_counters = match quiesce_control.as_ref() {
            Some(control) => control.counters(),
            None => IngestorQuiesceCounters::default(),
        };
        if !self.inner.executions.contains_key(domain) {
            let transient_error = match self.inner.domain_instantiation_errors.get(domain) {
                Some(error) => Some(error.value().clone()),
                None => self.ingestor_transient_error(domain, ingestor),
            };
            return Ok(IngestorDescribe {
                running: false,
                ready: false,
                quiesce_state: quiesce_state.clone(),
                quiesce_counters,
                memory_backpressure_paused,
                transient_error,
                reconnect_backoff: self.ingestor_reconnect_backoff(domain, ingestor),
                reconnect_wait_millis: self.ingestor_reconnect_wait_millis(domain, ingestor),
                kafka_domain_offsets: None,
            });
        }

        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.clone());
        let Some(runtime) = self.inner.ingestors.get(&key) else {
            let transient_error =
                if let Some(error) = self.ingestor_transient_error(domain, ingestor) {
                    Some(error)
                } else {
                    self.inner
                        .domain_instantiation_errors
                        .get(domain)
                        .map(|error| error.value().clone())
                };
            return Ok(IngestorDescribe {
                running: false,
                ready: false,
                quiesce_state: quiesce_state.clone(),
                quiesce_counters,
                memory_backpressure_paused,
                transient_error,
                reconnect_backoff: self.ingestor_reconnect_backoff(domain, ingestor),
                reconnect_wait_millis: self.ingestor_reconnect_wait_millis(domain, ingestor),
                kafka_domain_offsets: None,
            });
        };
        let Some(execution) = self.inner.executions.get(domain) else {
            return Ok(IngestorDescribe {
                running: true,
                ready: self.ingestor_ready(domain, ingestor),
                quiesce_state: quiesce_state.clone(),
                quiesce_counters,
                memory_backpressure_paused,
                transient_error: self.ingestor_transient_error(domain, ingestor),
                reconnect_backoff: self.ingestor_reconnect_backoff(domain, ingestor),
                reconnect_wait_millis: self.ingestor_reconnect_wait_millis(domain, ingestor),
                kafka_domain_offsets: None,
            });
        };
        let scheduled_ingestor = execution
            .schedule
            .nodes
            .get(&NodeRef::new(
                ModelKind::Ingestor,
                ModelName::from(ingestor),
            ))
            .and_then(|node| match node.config.as_ref() {
                Model::Ingestor(ingestor) => Some((node, ingestor.clone())),
                _ => None,
            });
        let kafka_domain_offsets = match runtime.value() {
            IngestorRuntime::Background { .. } => {
                if let Some((node, ingestor)) = scheduled_ingestor
                    && let IngestSource::Kafka {
                        topic,
                        offset_mode: KafkaOffsetMode::Domain,
                        instances,
                        ..
                    } = &ingestor.source
                    && let Some(schedule) = node.kafka_partition_schedule.as_ref()
                {
                    Some(kafka_domain_offset_describe_from_schedule(
                        topic.as_str(),
                        *instances,
                        schedule,
                    ))
                } else {
                    None
                }
            }
            IngestorRuntime::Endpoint { .. } => None,
        };
        Ok(IngestorDescribe {
            running: true,
            ready: self.ingestor_ready(domain, ingestor),
            quiesce_state,
            quiesce_counters,
            memory_backpressure_paused,
            transient_error: self.ingestor_transient_error(domain, ingestor),
            reconnect_backoff: self.ingestor_reconnect_backoff(domain, ingestor),
            reconnect_wait_millis: self.ingestor_reconnect_wait_millis(domain, ingestor),
            kafka_domain_offsets,
        })
    }

    pub(crate) fn describe_local_lookup(
        &self,
        domain: &DomainName,
        name: &LookupName,
    ) -> error_stack::Result<LocalLookupDescription, RuntimeObservationError> {
        let Some(execution) = self.inner.executions.get(domain) else {
            if let Some(error) = self.inner.domain_instantiation_errors.get(domain) {
                return Err(error_stack::Report::new(
                    RuntimeObservationError::DomainInstantiation {
                        domain: domain.clone(),
                        reason: error.value().clone(),
                    },
                ));
            }
            return Err(error_stack::Report::new(
                RuntimeObservationError::DomainNotInstantiated {
                    domain: domain.clone(),
                },
            ));
        };
        let Some(lookup) = execution.lookups.get(name) else {
            return Err(error_stack::Report::new(
                RuntimeObservationError::LookupNotInstantiated {
                    domain: domain.clone(),
                    lookup: name.clone(),
                },
            ));
        };
        Ok(LocalLookupDescription {
            model: lookup.model.clone(),
            resource_version: lookup.resource_version,
            entry_count: lookup.entries.len(),
        })
    }

    pub(crate) fn query_local_lookup(
        &self,
        domain: &DomainName,
        name: &LookupName,
        key: &str,
    ) -> error_stack::Result<Option<RuntimeRecordBatch>, RuntimeObservationError> {
        let Some(execution) = self.inner.executions.get(domain) else {
            if let Some(error) = self.inner.domain_instantiation_errors.get(domain) {
                return Err(error_stack::Report::new(
                    RuntimeObservationError::DomainInstantiation {
                        domain: domain.clone(),
                        reason: error.value().clone(),
                    },
                ));
            }
            return Err(error_stack::Report::new(
                RuntimeObservationError::DomainNotInstantiated {
                    domain: domain.clone(),
                },
            ));
        };
        let Some(lookup) = execution.lookups.get(name) else {
            return Err(error_stack::Report::new(
                RuntimeObservationError::LookupNotInstantiated {
                    domain: domain.clone(),
                    lookup: name.clone(),
                },
            ));
        };
        lookup.metrics.observe(1, key.len().arch_into(), None);
        self.mark_branch_aggregated_metrics_updated(domain, ModelKind::Lookup, name);
        lookup
            .entries
            .get(key)
            .map(|row| lookup.batch.slice(*row, 1))
            .transpose()
            .map_err(|error| {
                error_stack::Report::new(RuntimeObservationError::LookupRecord {
                    domain: domain.clone(),
                    lookup: name.clone(),
                    error,
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use fjall::Database;
    use futures_util::FutureExt as _;
    use nervix_models::{
        ClusterNodeName, CreateLookup, IngestorName, ModelKind, ModelName, ParseAsType,
    };
    use tempfile::tempdir;
    use tokio::time::Duration;
    use triomphe::Arc;

    use super::*;
    use crate::metrics::RuntimeMetrics;

    #[tokio::test]
    async fn a_reported_runtime_error_reaches_every_attached_observer() {
        let events = RuntimeEvents::new();
        let mut first = events.subscribe();
        let mut second = events.subscribe();

        events.report_error("ingestor 'orders' failed to decode a message");

        for observer in [&mut first, &mut second] {
            tokio::task::consume_budget().await;
            let RuntimeEvent::Error(message) = observer
                .recv()
                .await
                .expect("an attached observer must receive the report");
            assert_eq!(message, "ingestor 'orders' failed to decode a message");
        }
    }

    #[test]
    fn reporting_a_runtime_error_with_no_observer_attached_still_returns() {
        // The node reports failures before its fan-out task subscribes and after shutdown drops
        // it. Neither window may take the reporting path down with it.
        let events = RuntimeEvents::new();

        events.report_error("emitter 'ledger' failed to publish");
    }

    #[tokio::test]
    async fn an_observer_that_attaches_later_sees_only_what_follows_it() {
        let events = RuntimeEvents::new();
        events.report_error("reported before anyone was listening");

        let mut observer = events.subscribe();
        events.report_error("reported after the observer attached");

        let RuntimeEvent::Error(message) = observer
            .recv()
            .await
            .expect("the observer must receive the later report");
        assert_eq!(message, "reported after the observer attached");
        assert!(
            observer.recv().now_or_never().is_none(),
            "the bus must not replay reports that predate the observer"
        );
    }
    #[test]
    fn describe_restores_branch_aggregated_metrics_from_store_without_materialized_state() {
        let dir = tempdir().expect("temp dir should open");
        let domain = domain("default");
        let ingestor = named::<IngestorName>("redis_notifications");
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeStateKind::BranchAggregated,
            kind: ModelKind::Ingestor,
            identifier: ModelName::from(&ingestor.clone()),
            schema_fingerprint: [0; 32],
            branch_key: None,
        };
        {
            let db = Database::builder(dir.path())
                .open()
                .expect("db should open");
            let store = RuntimeStateStore::from_database(db).expect("state store should open");
            let metrics = RuntimeMetrics::default();
            metrics
                .resolve_node_batch_metrics(NodeBatchMetricsSpec {
                    domain: &domain,
                    kind: ModelKind::Ingestor,
                    node: &ModelName::from(&ingestor),
                    relay: &named("notifications"),
                    physical_node_id: Some(&ClusterNodeName::parse("node-3").expect("valid name")),
                    direction: "sent",
                    branch_key: None,
                })
                .observe(19, 1900, None);
            let snapshot = BranchAggregatedRuntimeStateSnapshot {
                metrics: metrics.snapshot_global_target(
                    &domain,
                    ModelKind::Ingestor,
                    &ModelName::from(&ingestor),
                    &ClusterNodeName::parse("node-3").expect("valid name"),
                ),
            };
            let payload = encode_branch_aggregated_snapshot(&snapshot)
                .expect("branch-aggregated snapshot should encode");
            store
                .persist_latest_snapshot(&placement, 7, &payload)
                .expect("snapshot should persist");
        }

        let db = Database::builder(dir.path())
            .open()
            .expect("db should reopen");
        let runtime = Runtime::with_persistence(Some(db), Duration::from_millis(100))
            .expect("runtime should open persisted state");
        runtime.inner.metrics.register_global_node(
            &domain,
            ModelKind::Ingestor,
            &ModelName::from(&ingestor),
            Some(&ClusterNodeName::parse("node-3").expect("valid name")),
        );

        let rendered = runtime.describe_metrics_for(&domain, "INGESTOR", &ingestor);
        assert!(
            rendered.iter().any(|line| line
                .contains("messages_total sent relay=notifications physical_node=node-3")
                && line.contains("total=19")),
            "expected persisted branch-aggregated metrics before START retry in {rendered:?}"
        );
    }

    #[test]
    fn describe_restores_branch_aggregated_metrics_when_state_lsm_is_current_but_metrics_missing() {
        let dir = tempdir().expect("temp dir should open");
        let domain = domain("default");
        let ingestor = named::<IngestorName>("redis_notifications");
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeStateKind::BranchAggregated,
            kind: ModelKind::Ingestor,
            identifier: ModelName::from(&ingestor.clone()),
            schema_fingerprint: [0; 32],
            branch_key: None,
        };
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let store = RuntimeStateStore::from_database(db.clone()).expect("state store should open");
        let persisted_metrics = RuntimeMetrics::default();
        persisted_metrics
            .resolve_node_batch_metrics(NodeBatchMetricsSpec {
                domain: &domain,
                kind: ModelKind::Ingestor,
                node: &ModelName::from(&ingestor),
                relay: &named("notifications"),
                physical_node_id: Some(&ClusterNodeName::parse("node-3").expect("valid name")),
                direction: "sent",
                branch_key: None,
            })
            .observe(19, 1900, None);
        let snapshot = BranchAggregatedRuntimeStateSnapshot {
            metrics: persisted_metrics.snapshot_global_target(
                &domain,
                ModelKind::Ingestor,
                &ModelName::from(&ingestor),
                &ClusterNodeName::parse("node-3").expect("valid name"),
            ),
        };
        let payload = encode_branch_aggregated_snapshot(&snapshot)
            .expect("branch-aggregated snapshot should encode");
        store
            .persist_latest_snapshot(&placement, 7, &payload)
            .expect("snapshot should persist");

        let runtime = Runtime::with_persistence(Some(db), Duration::from_millis(100))
            .expect("runtime should open persisted state");
        let stale_state = Arc::new(
            ReplicatedBranchAggregatedState::new(
                placement.clone(),
                Some(ClusterNodeName::parse("node-3").expect("valid name")),
                ClusterNodeName::parse("node-3").expect("valid name"),
                Vec::new(),
                0,
                &RuntimeMetrics::default(),
                store
                    .latest_snapshot(&placement)
                    .expect("snapshot should load"),
            )
            .expect("stale state should initialize"),
        );
        stale_state.mark_metrics_updated();
        runtime
            .inner
            .replicated_branch_aggregated_states
            .insert(placement, stale_state);
        runtime.inner.metrics.register_global_node(
            &domain,
            ModelKind::Ingestor,
            &ModelName::from(&ingestor),
            Some(&ClusterNodeName::parse("node-3").expect("valid name")),
        );

        let rendered = runtime.describe_metrics_for(&domain, "INGESTOR", &ingestor);
        assert!(
            rendered.iter().any(|line| line
                .contains("messages_total sent relay=notifications physical_node=node-3")
                && line.contains("total=19")),
            "expected persisted branch-aggregated metrics despite current stale LSM in \
             {rendered:?}"
        );
    }

    #[test]
    fn describe_does_not_reapply_equal_lsm_snapshot_over_active_metrics() {
        let dir = tempdir().expect("temp dir should open");
        let domain = domain("default");
        let ingestor = named::<IngestorName>("redis_notifications");
        let placement = RuntimeStatePlacement {
            domain: domain.clone(),
            state: RuntimeStateKind::BranchAggregated,
            kind: ModelKind::Ingestor,
            identifier: ModelName::from(&ingestor.clone()),
            schema_fingerprint: [0; 32],
            branch_key: None,
        };
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let store = RuntimeStateStore::from_database(db.clone()).expect("state store should open");
        let persisted_metrics = RuntimeMetrics::default();
        persisted_metrics
            .resolve_node_batch_metrics(NodeBatchMetricsSpec {
                domain: &domain,
                kind: ModelKind::Ingestor,
                node: &ModelName::from(&ingestor),
                relay: &named("notifications"),
                physical_node_id: Some(&ClusterNodeName::parse("node-3").expect("valid name")),
                direction: "sent",
                branch_key: None,
            })
            .observe(19, 1900, None);
        let snapshot = BranchAggregatedRuntimeStateSnapshot {
            metrics: persisted_metrics.snapshot_global_target(
                &domain,
                ModelKind::Ingestor,
                &ModelName::from(&ingestor),
                &ClusterNodeName::parse("node-3").expect("valid name"),
            ),
        };
        let payload = encode_branch_aggregated_snapshot(&snapshot)
            .expect("branch-aggregated snapshot should encode");
        store
            .persist_latest_snapshot(&placement, 7, &payload)
            .expect("snapshot should persist");

        let runtime = Runtime::with_persistence(Some(db), Duration::from_millis(100))
            .expect("runtime should open persisted state");
        let state = Arc::new(
            ReplicatedBranchAggregatedState::new(
                placement.clone(),
                Some(ClusterNodeName::parse("node-3").expect("valid name")),
                ClusterNodeName::parse("node-3").expect("valid name"),
                Vec::new(),
                0,
                &RuntimeMetrics::default(),
                store
                    .latest_snapshot(&placement)
                    .expect("snapshot should load"),
            )
            .expect("state should initialize"),
        );
        runtime
            .inner
            .replicated_branch_aggregated_states
            .insert(placement, state);
        runtime
            .inner
            .metrics
            .resolve_node_batch_metrics(NodeBatchMetricsSpec {
                domain: &domain,
                kind: ModelKind::Ingestor,
                node: &ModelName::from(&ingestor),
                relay: &named("notifications"),
                physical_node_id: Some(&ClusterNodeName::parse("node-3").expect("valid name")),
                direction: "sent",
                branch_key: None,
            })
            .observe(1, 100, None);

        let rendered = runtime.describe_metrics_for(&domain, "INGESTOR", &ingestor);
        assert!(
            rendered.iter().any(|line| line
                .contains("messages_total sent relay=notifications physical_node=node-3")
                && line.contains(" total=1 ")),
            "expected active metrics to remain authoritative for equal LSM in {rendered:?}"
        );
    }

    #[test]
    fn lookup_queries_surface_recorded_domain_instantiation_errors() {
        let runtime = Runtime::new();
        runtime.inner.domain_instantiation_errors.insert(
            domain("default"),
            "failed to build domain execution for 'default': lookup load failed".to_string(),
        );

        let error = runtime
            .query_local_lookup(&domain("default"), &named("zip_codes"), "99926")
            .expect_err("lookup should surface stored instantiation errors");
        let error = error.to_string();

        assert!(error.contains("failed to build domain execution for 'default'"));
        assert!(error.contains("lookup load failed"));
    }

    #[test]
    fn lookup_descriptions_classify_missing_runtime_state() {
        let runtime = Runtime::new();
        let domain = domain("default");
        let lookup = named("zip_codes");
        runtime.inner.domain_instantiation_errors.insert(
            domain.clone(),
            "lookup resource could not be decoded".to_string(),
        );

        let Err(error) = runtime.describe_local_lookup(&domain, &lookup) else {
            panic!("a recorded domain build failure must be preserved");
        };
        assert!(matches!(
            error.current_context(),
            RuntimeObservationError::DomainInstantiation { domain: failed, reason }
                if failed == &domain && reason == "lookup resource could not be decoded"
        ));

        runtime.inner.domain_instantiation_errors.remove(&domain);
        let Err(error) = runtime.describe_local_lookup(&domain, &lookup) else {
            panic!("an absent domain execution must be distinct");
        };
        assert!(matches!(
            error.current_context(),
            RuntimeObservationError::DomainNotInstantiated { domain: failed }
                if failed == &domain
        ));

        install_test_domain_execution(
            &runtime,
            &domain,
            Vec::new(),
            DomainRoutingSnapshot::default(),
        );
        let Err(error) = runtime.describe_local_lookup(&domain, &lookup) else {
            panic!("an absent lookup runtime must be distinct");
        };
        assert!(matches!(
            error.current_context(),
            RuntimeObservationError::LookupNotInstantiated {
                domain: failed_domain,
                lookup: failed_lookup,
            } if failed_domain == &domain && failed_lookup == &lookup
        ));
    }

    #[test]
    fn lookup_queries_classify_missing_runtime_state_and_invalid_rows() {
        let runtime = Runtime::new();
        let domain = domain("default");
        let lookup = named::<LookupName>("zip_codes");

        let error = runtime
            .query_local_lookup(&domain, &lookup, "99926")
            .expect_err("an absent domain execution must fail");
        assert!(matches!(
            error.current_context(),
            RuntimeObservationError::DomainNotInstantiated { domain: failed }
                if failed == &domain
        ));

        install_test_domain_execution(
            &runtime,
            &domain,
            Vec::new(),
            DomainRoutingSnapshot::default(),
        );
        let error = runtime
            .query_local_lookup(&domain, &lookup, "99926")
            .expect_err("an absent lookup runtime must fail");
        assert!(matches!(
            error.current_context(),
            RuntimeObservationError::LookupNotInstantiated {
                domain: failed_domain,
                lookup: failed_lookup,
            } if failed_domain == &domain && failed_lookup == &lookup
        ));

        let schema = test_schema(&[("postal_code", ParseAsType::String)]);
        let batch = schema
            .batch_from_test_rows([[(
                "postal_code".to_string(),
                RuntimeValue::String("99926".to_string()),
            )]])
            .expect("lookup test batch should build");
        let lookup_runtime = Arc::new(LookupRuntime {
            model: CreateLookup {
                name: lookup.clone(),
                key_field: named("postal_code"),
                resource: named("postal_codes"),
                path: "postal_codes.jsonl".to_string(),
                decode_using_codec: named("postal_code_codec"),
            },
            resource_version: 7,
            schema,
            batch: Arc::new(batch),
            entries: Arc::new(HashMap::from_iter([("99926".to_string(), 1)])),
            metrics: RuntimeMetrics::default().resolve_global_node_message_metrics(
                &domain,
                ModelKind::Lookup,
                &ModelName::from(&lookup),
                None,
                "received",
            ),
        });
        let mut routing = DomainRoutingSnapshot::default();
        routing.lookups.insert(lookup.clone(), lookup_runtime);
        install_test_domain_execution(&runtime, &domain, Vec::new(), routing);

        let error = runtime
            .query_local_lookup(&domain, &lookup, "99926")
            .expect_err("a lookup index outside the Arrow batch must fail");
        assert!(matches!(
            error.current_context(),
            RuntimeObservationError::LookupRecord {
                domain: failed_domain,
                lookup: failed_lookup,
                ..
            } if failed_domain == &domain && failed_lookup == &lookup
        ));
    }

    #[tokio::test]
    async fn describe_ingestor_surfaces_instantiation_error_when_runtime_is_missing() {
        let runtime = Runtime::new();
        let domain = domain("default");
        let ingestor = named("mqtt_notifications");
        runtime.inner.domain_instantiation_errors.insert(
            domain.clone(),
            "failed to build domain execution for 'default': ingestor start failed".to_string(),
        );
        install_test_domain_execution(
            &runtime,
            &domain,
            Vec::new(),
            DomainRoutingSnapshot::default(),
        );

        let describe = runtime
            .describe_local_ingestor(&domain, &ingestor)
            .expect("describe should succeed");

        assert!(!describe.running);
        assert!(
            describe
                .transient_error
                .as_deref()
                .is_some_and(|error| error.contains("ingestor start failed")),
            "describe should expose domain instantiation error, got {:?}",
            describe.transient_error
        );
    }
}
