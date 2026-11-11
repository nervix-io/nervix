//! Leader-owned observation and atomic publication of external partition topology.
//!
//! Layer: control plane.
//! - **Owns.** Branch observation lifetimes, captured-input publication and leader-tenure fencing.
//! - **Depends on.** Pure source/grouping plans, connector metadata inspectors and consensus.
//! - **Must not know.** Record routing, broker offset ownership, branch actors or key state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the observation and publication boundary"
    )
)]

use std::{collections::BTreeMap, num::NonZeroU32};

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_connector::ClientResourceMounts;
use nervix_connector_kafka::TopicPartitionInspector;
use nervix_connector_pulsar::PulsarTopicPartitionInspector;
use nervix_consensus::{DomainPlanningInputs, LeaderTenure};
use nervix_interconnect::EntityGatePurpose;
use nervix_models::{
    BranchName, ClusterSchedule, DomainName, DomainSchedule, ExternalPartitionSet,
    GeneralErrorPolicy, IngestorName, SourcePartitionObservation,
};
use nervix_primitives::{
    sync::{Arc, CancellationToken},
    time::sleep,
};
use thiserror::Error;

use super::{
    background_task::BackgroundTask,
    entity_gate::{ClusterEntityGate, ClusterEntityGateScope, EntityGateParticipants},
    ownership_handoff::mark_complete_ownership_transitions,
    schedule_planning::DomainSchedulePlanningSnapshot,
    session_service::SessionServiceImpl,
};
use crate::registry::{
    ActiveGraph, EntityGatePlan, KafkaPartitionSourcePlan, PartitionSourcePlan,
    PartitionSourceSpec, PulsarPartitionSourcePlan, ScheduleDelta, decide_partition_topology,
    partition_source_specs,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ObservationSubject {
    Branch(BranchName),
    DomainOffsets(IngestorName),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WatcherKey {
    domain: DomainName,
    subject: ObservationSubject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WatcherSpec {
    key: WatcherKey,
    sources: Vec<PartitionSourceSpec>,
}

struct Watcher {
    spec: WatcherSpec,
    task: BackgroundTask,
}

enum Observer {
    Kafka {
        inspector: TopicPartitionInspector,
        topic: String,
    },
    Pulsar(PulsarTopicPartitionInspector),
}

struct ConnectedSource {
    observer: Observer,
    _mounts: Vec<Arc<ClientResourceMounts>>,
}

#[derive(Debug, Error)]
enum ObservationError {
    #[error("external partition observation failed for ingestor '{ingestor}' in domain '{domain}'")]
    Source {
        domain: DomainName,
        ingestor: IngestorName,
    },
    #[error("{connector} metadata did not enumerate consecutive nonnegative partition coordinates", connector = nervix_models::ConnectorKind::Kafka)]
    KafkaCoordinates,
    #[error("partition topology could not be published for domain '{domain}'")]
    Publication { domain: DomainName },
}

fn watcher_specs(
    schedule: &ClusterSchedule,
) -> Result<BTreeMap<WatcherKey, WatcherSpec>, Report<crate::registry::RegistryError>> {
    let mut desired = BTreeMap::<WatcherKey, WatcherSpec>::new();
    for domain in schedule.domains.values() {
        for source in partition_source_specs(&domain.domain, domain)?.into_values() {
            let subjects = if source.branches.is_empty() {
                vec![ObservationSubject::DomainOffsets(source.ingestor.clone())]
            } else {
                source
                    .branches
                    .iter()
                    .cloned()
                    .map(ObservationSubject::Branch)
                    .collect()
            };
            for subject in subjects {
                let key = WatcherKey {
                    domain: domain.domain.clone(),
                    subject,
                };
                desired
                    .entry(key.clone())
                    .or_insert_with(|| WatcherSpec {
                        key,
                        sources: Vec::new(),
                    })
                    .sources
                    .push(source.clone());
            }
        }
    }
    Ok(desired)
}

impl ConnectedSource {
    async fn connect(
        service: &SessionServiceImpl,
        domain: &DomainName,
        spec: &PartitionSourceSpec,
    ) -> Result<Self, Report<ObservationError>> {
        let context = || ObservationError::Source {
            domain: domain.clone(),
            ingestor: spec.ingestor.clone(),
        };
        let client = match &spec.source {
            PartitionSourcePlan::Kafka(KafkaPartitionSourcePlan::Topic { client, .. })
            | PartitionSourcePlan::Pulsar(PulsarPartitionSourcePlan::Topic { client, .. }) => {
                client
            }
        };
        let resolved = service
            .inner
            .runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .change_context_lazy(context)?;
        let observer = match &spec.source {
            PartitionSourcePlan::Kafka(KafkaPartitionSourcePlan::Topic { topic, .. }) => {
                Observer::Kafka {
                    inspector: TopicPartitionInspector::new(
                        &resolved.entries,
                        format!(
                            "nervix_partition_observer_{}_{}",
                            domain.as_str(),
                            spec.ingestor.as_str()
                        ),
                        service.inner.runtime.executor().clone(),
                    )
                    .change_context_lazy(context)?,
                    topic: topic.as_str().to_string(),
                }
            }
            PartitionSourcePlan::Pulsar(PulsarPartitionSourcePlan::Topic { topic, .. }) => {
                Observer::Pulsar(
                    PulsarTopicPartitionInspector::connect(
                        &resolved.entries,
                        topic,
                        service.inner.runtime.outward_tls(),
                    )
                    .await
                    .change_context_lazy(context)?,
                )
            }
        };
        Ok(Self {
            observer,
            _mounts: resolved.mounts.into_iter().collect(),
        })
    }

    async fn observe(
        &self,
        domain: &DomainName,
        spec: &PartitionSourceSpec,
    ) -> Result<Option<ExternalPartitionSet>, Report<ObservationError>> {
        let context = || ObservationError::Source {
            domain: domain.clone(),
            ingestor: spec.ingestor.clone(),
        };
        match &self.observer {
            Observer::Pulsar(inspector) => inspector
                .partitions()
                .await
                .map(Some)
                .change_context_lazy(context),
            Observer::Kafka { inspector, topic } => {
                let mut partitions = match inspector.partitions(topic).await {
                    Ok(partitions) => partitions,
                    Err(error) if error.current_context().is_missing_topic() => return Ok(None),
                    Err(error) => return Err(error.change_context(context())),
                };
                partitions.sort_unstable();
                if partitions
                    .iter()
                    .enumerate()
                    .any(|(index, partition)| usize::try_from(*partition).ok() != Some(index))
                {
                    return Err(
                        Report::new(ObservationError::KafkaCoordinates).change_context(context())
                    );
                }
                let count = u32::try_from(partitions.len()).change_context_lazy(context)?;
                Ok(NonZeroU32::new(count).map(ExternalPartitionSet::Partitioned))
            }
        }
    }
}

impl SessionServiceImpl {
    async fn drain_observed_partition_update(
        &self,
        domain: &DomainName,
        existing: &DomainSchedule,
        next: &DomainSchedule,
    ) -> Result<Option<ClusterEntityGate>, Report<ObservationError>> {
        let entities = ScheduleDelta::classify(existing, next).entity_gate_entities();
        if entities.is_empty() {
            return Ok(None);
        }
        let plan = EntityGatePlan::for_model_change(Some(existing), entities.clone(), entities);
        let purpose = if ScheduleDelta::changes_a_partition_count(existing, next) {
            EntityGatePurpose::PartitionChange
        } else {
            EntityGatePurpose::ModelAlteration
        };
        let context = || ObservationError::Publication {
            domain: domain.clone(),
        };
        let deadline =
            nervix_primitives::time::Instant::now() + self.inner.runtime.entity_gate_deadline();
        let gate = self
            .engage_cluster_entity_gates(
                ClusterEntityGateScope {
                    domain,
                    relays: plan.relays(),
                    targets: plan.targets(),
                    purpose,
                    participants: EntityGateParticipants::EveryAvailableNode,
                },
                deadline,
                None,
            )
            .await
            .change_context_lazy(context)?;
        #[cfg(feature = "testing")]
        self.inner.runtime.pause_entity_gate_if_armed(domain).await;
        self.wait_for_cluster_entity_drain(
            &gate,
            plan.relays(),
            plan.targets(),
            purpose,
            &[],
            deadline,
        )
        .await
        .change_context_lazy(context)?;
        Ok(Some(gate))
    }

    async fn publish_observed_partition_update(
        &self,
        tenure: LeaderTenure,
        inputs: &DomainPlanningInputs,
        planning: &DomainSchedulePlanningSnapshot,
        mut next: DomainSchedule,
    ) -> Result<bool, Report<ObservationError>> {
        let existing = inputs
            .schedule()
            .verified("the observer captured an active schedule");
        if &next == existing {
            return Ok(false);
        }
        let domain = next.domain.clone();
        let context = || ObservationError::Publication {
            domain: domain.clone(),
        };
        mark_complete_ownership_transitions(Some(existing), &mut next);
        let gate = self
            .drain_observed_partition_update(&domain, existing, &next)
            .await?;
        let mut handoff = self
            .begin_planned_ownership_handoff(&domain, Some(existing), Some(&next))
            .await
            .change_context_lazy(context)?;
        let committed = async {
            self.validate_domain_planning_inputs(inputs)
                .await
                .change_context_lazy(context)?;
            planning
                .validate_eligibility(self)
                .await
                .change_context_lazy(context)?;
            if self.inner.consensus.current_leader_tenure().as_ref() != Some(&tenure) {
                return Ok(false);
            }
            self.inner
                .consensus
                .update_partition_topology(tenure, inputs.clone(), next)
                .await
                .change_context_lazy(context)?;
            Ok(true)
        }
        .await;
        match committed {
            Ok(true) => {}
            result => {
                if let Some(handoff) = handoff.take() {
                    self.abort_planned_ownership_handoff(&domain, handoff, None)
                        .await;
                }
                return result;
            }
        }
        if let Err(error) = self.apply_current_cluster_state().await {
            if let Some(handoff) = handoff.take() {
                self.defer_planned_ownership_handoff_release(&domain, handoff, &error, None);
            }
            return Err(error.change_context(context()));
        }
        if let Some(handoff) = handoff {
            self.finish_planned_ownership_handoff(&domain, handoff, None)
                .await
                .change_context_lazy(context)?;
        }
        if let Some(gate) = gate {
            self.release_cluster_entity_gates(gate).await;
        }
        Ok(true)
    }
}

impl WatcherSpec {
    async fn run(self, service: SessionServiceImpl, cancel: CancellationToken) {
        let domain = &self.key.domain;
        let interval = if self
            .sources
            .iter()
            .any(|source| matches!(source.source, PartitionSourcePlan::Kafka(_)))
        {
            service.inner.runtime.kafka_partition_watch_interval()
        } else {
            service.inner.runtime.partition_topology_poll_interval()
        };
        let mut connected = BTreeMap::<IngestorName, ConnectedSource>::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(tenure) = service.inner.consensus.current_leader_tenure() else {
                break;
            };
            if tenure.leader_id() != service.inner.consensus.local_node_id() {
                break;
            }
            // Capture before connecting or polling. Every external result belongs to this one
            // configuration, schedule, placement topology and leader tenure.
            let inputs = service.inner.consensus.domain_planning_inputs(domain).await;
            let Some(existing) = inputs.schedule() else {
                break;
            };
            let specs = match partition_source_specs(domain, existing) {
                Ok(specs) => specs,
                Err(error) => {
                    service.broadcast_error(format!("{error:#}"));
                    break;
                }
            };
            if self
                .sources
                .iter()
                .any(|spec| specs.get(&spec.ingestor) != Some(spec))
            {
                break;
            }
            let mut observations = existing.partition_topology.sources.clone();
            for spec in &self.sources {
                let operation = async {
                    if !connected.contains_key(&spec.ingestor) {
                        let source = ConnectedSource::connect(&service, domain, spec).await?;
                        connected.insert(spec.ingestor.clone(), source);
                    }
                    connected
                        .get(&spec.ingestor)
                        .verified("the connection was just installed")
                        .observe(domain, spec)
                        .await
                };
                let observation = nervix_primitives::select! {
                    _ = cancel.cancelled() => return,
                    _ = service.inner.drain_support_shutdown.cancelled() => return,
                    observation = operation => observation,
                };
                match observation {
                    Ok(partitions) => {
                        let epoch = match existing.partition_topology.sources.get(&spec.ingestor) {
                            Some(source) => source.epoch,
                            None => 0,
                        };
                        observations.insert(
                            spec.ingestor.clone(),
                            SourcePartitionObservation {
                                configuration: spec.fingerprint,
                                partitions,
                                epoch,
                            },
                        );
                    }
                    Err(error) => {
                        if spec.general_error_policy == GeneralErrorPolicy::Log {
                            service.broadcast_error(format!(
                                "ingestor '{}' general error in domain '{}': {error:#}",
                                spec.ingestor, domain
                            ));
                        }
                        connected.remove(&spec.ingestor);
                    }
                }
            }
            let decision = decide_partition_topology(
                existing,
                &specs,
                &existing.partition_topology,
                &observations,
            );
            if decision.topology == existing.partition_topology {
                nervix_primitives::select! {
                    _ = cancel.cancelled() => break,
                    _ = service.inner.drain_support_shutdown.cancelled() => break,
                    _ = sleep(interval) => {},
                }
                continue;
            }
            let publish = async {
                let graph = ActiveGraph::from_scheduled_models(existing).change_context(
                    ObservationError::Publication {
                        domain: domain.clone(),
                    },
                )?;
                let planning = service
                    .capture_domain_schedule_planning_snapshot(&inputs)
                    .await;
                let prepared = planning
                    .prepare_observed(
                        &inputs,
                        domain,
                        Some(graph),
                        Some(existing),
                        Some(&decision.topology),
                    )
                    .change_context(ObservationError::Publication {
                        domain: domain.clone(),
                    })?;
                let next = prepared
                    .schedule
                    .verified("a supplied active graph produces a schedule");
                if !service
                    .publish_observed_partition_update(tenure, &inputs, &planning, next)
                    .await?
                {
                    return Ok(());
                }
                for (name, error) in &decision.errors {
                    if specs
                        .get(name)
                        .is_some_and(|spec| spec.general_error_policy == GeneralErrorPolicy::Log)
                    {
                        service.broadcast_error(format!(
                            "ingestor '{name}' general error in domain '{domain}': {error}"
                        ));
                    }
                }
                Ok::<(), Report<ObservationError>>(())
            };
            let result = nervix_primitives::select! {
                _ = cancel.cancelled() => return,
                _ = service.inner.drain_support_shutdown.cancelled() => return,
                result = publish => result,
            };
            if let Err(error) = result {
                service.broadcast_error(format!("{error:#}"));
            }
            nervix_primitives::select! {
                _ = cancel.cancelled() => break,
                _ = service.inner.drain_support_shutdown.cancelled() => break,
                _ = sleep(interval) => {},
            }
        }
    }
}

pub(in crate::application) async fn run_partition_topology_watchers(
    service: SessionServiceImpl,
    shutdown: CancellationToken,
) {
    let mut schedule_rx = service.inner.consensus.subscribe_schedule();
    let mut watchers = BTreeMap::<WatcherKey, Watcher>::new();
    loop {
        let schedule = service.inner.consensus.current_schedule().await;
        let leading = service
            .inner
            .consensus
            .current_leader_tenure()
            .is_some_and(|tenure| tenure.leader_id() == service.inner.consensus.local_node_id());
        let desired = if leading {
            watcher_specs(&schedule)
        } else {
            Ok(BTreeMap::new())
        };
        match desired {
            Ok(desired) => {
                let stale = watchers
                    .iter()
                    .filter(|(key, watcher)| {
                        desired.get(*key) != Some(&watcher.spec)
                            || watcher.task.handle.is_finished()
                    })
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                for key in stale {
                    if let Some(watcher) = watchers.remove(&key) {
                        watcher.task.stop().await;
                    }
                }
                for (key, spec) in desired {
                    if watchers.contains_key(&key) {
                        continue;
                    }
                    let cancel = CancellationToken::new();
                    let handle = nervix_primitives::task::spawn(
                        spec.clone().run(service.clone(), cancel.clone()),
                    );
                    watchers.insert(
                        key,
                        Watcher {
                            spec,
                            task: BackgroundTask { cancel, handle },
                        },
                    );
                }
            }
            Err(error) => service.broadcast_error(format!("{error:#}")),
        }
        nervix_primitives::select! {
            _ = shutdown.cancelled() => break,
            changed = schedule_rx.changed() => if changed.is_err() { break; },
            _ = sleep(service.inner.runtime.partition_topology_poll_interval()) => {},
        }
    }
    for watcher in watchers.into_values() {
        watcher.task.stop().await;
    }
}
