//! The cluster-wide hold that stops an entity before its configuration changes.
//!
//! Layer: control plane.
//!
//! - **Owns.** Engaging and releasing entity gates on every node, the drain each gate waits for,
//!   and the report a caller reads while it waits.
//! - **Depends on.** The interconnect to reach every node and the runtime for the local gate.
//! - **Must not know.** Why the caller is altering the entity.

use std::{collections::BTreeSet, sync::atomic::Ordering};

use arch_into::ArchInto;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_interconnect::{
    DomainDrainStatusEnvelope, DomainDrainStatusRequest as RemoteDomainDrainStatusRequest,
    EmitterPublishingDrainStateEnvelope, EmitterPublishingDrainStatusEnvelope,
    EntityDrainStatusEnvelope, EntityDrainStatusRequest as RemoteEntityDrainStatusRequest,
    EntityGatePurpose, EntityGateReleaseRequest as RemoteEntityGateReleaseRequest,
    EntityGateRequest as RemoteEntityGateRequest,
};
use nervix_models::{ClusterNodeName, DomainName, NodeRef, RelayName};
use tokio::time::{Duration, interval, sleep};
use tracing::{debug, warn};

use super::{domain_lifecycle::DomainAlterError, session_service::SessionServiceImpl};
use crate::runtime::EntityGateLease;

pub(in crate::application) const ENTITY_GATE_RELEASE_RETRY_INTERVAL: Duration =
    Duration::from_millis(100);

#[derive(Debug, Clone)]
pub(in crate::application) struct DrainOutstanding {
    pub(in crate::application) domain: DomainName,
    pub(in crate::application) node: Option<ClusterNodeName>,
    pub(in crate::application) active_ingestors: u64,
    pub(in crate::application) active_generators: u64,
    pub(in crate::application) outstanding_acks: u64,
    pub(in crate::application) buffered_emitter_messages: u64,
    pub(in crate::application) emitter_publishing: Vec<EmitterPublishingDrainStatusEnvelope>,
    pub(in crate::application) status_error: Option<String>,
}

impl DrainOutstanding {
    fn write_emitter_publishing(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        if self.emitter_publishing.is_empty() {
            return Ok(());
        }
        formatter.write_str(&emitter_publishing_drain_summary(&self.emitter_publishing))
    }
}

fn emitter_publishing_drain_summary(statuses: &[EmitterPublishingDrainStatusEnvelope]) -> String {
    if statuses.is_empty() {
        return String::new();
    }
    let statuses = statuses
        .iter()
        .map(|status| {
            let mut detail = format!(
                "{}:{}(pending={}",
                status.emitter.as_str(),
                status.state.as_str(),
                status.pending_messages,
            );
            if let Some(backoff) = status.retry_backoff_millis {
                detail.push_str(&format!(", retry_backoff_ms={backoff}"));
            }
            if let Some(wait) = status.retry_wait_millis {
                detail.push_str(&format!(", retry_wait_ms={wait}"));
            }
            detail.push(')');
            detail
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(", publishing=[{statuses}]")
}

fn emitter_publishing_drain_status_envelope(
    status: crate::runtime::EmitterPublishingDrainStatus,
) -> EmitterPublishingDrainStatusEnvelope {
    EmitterPublishingDrainStatusEnvelope {
        emitter: status.emitter,
        state: match status.state {
            crate::runtime::EmitterPublishingDrainState::AwaitingConfirmation => {
                EmitterPublishingDrainStateEnvelope::AwaitingConfirmation
            }
            crate::runtime::EmitterPublishingDrainState::RetryingInfrastructure => {
                EmitterPublishingDrainStateEnvelope::RetryingInfrastructure
            }
            crate::runtime::EmitterPublishingDrainState::RetryingIcebergCommit => {
                EmitterPublishingDrainStateEnvelope::RetryingIcebergCommit
            }
        },
        pending_messages: status.pending_messages.arch_into(),
        retry_backoff_millis: status
            .retry_backoff
            .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)),
        retry_wait_millis: status
            .retry_wait
            .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)),
    }
}

impl std::fmt::Display for DrainOutstanding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let total = [
            self.active_ingestors,
            self.active_generators,
            self.outstanding_acks,
            self.buffered_emitter_messages,
        ]
        .into_iter()
        .try_fold(0_u64, u64::checked_add)
        .assured("every count totals work items this cluster already holds in memory");
        if let Some(node) = &self.node {
            write!(
                formatter,
                "timed out draining domain '{}' on node '{}': {} outstanding work item(s) \
                 (ingestors={}, generators={}, acknowledgements={}, emitter_buffers={}",
                self.domain.as_str(),
                node,
                total,
                self.active_ingestors,
                self.active_generators,
                self.outstanding_acks,
                self.buffered_emitter_messages,
            )?;
            self.write_emitter_publishing(formatter)?;
            formatter.write_str(")")
        } else if let Some(status_error) = &self.status_error {
            write!(
                formatter,
                "timed out draining domain '{}': {status_error}",
                self.domain.as_str()
            )
        } else {
            write!(
                formatter,
                "timed out draining domain '{}' because no node reported drain status",
                self.domain.as_str()
            )
        }
    }
}

pub(in crate::application) struct ClusterEntityGate {
    operation_id: u64,
    pub(in crate::application) domain: DomainName,
    /// Nodes whose gate engagement was attempted and not yet released. Membership decides both
    /// what still needs releasing and what a repeated attempt must not duplicate, so this is a set.
    nodes: BTreeSet<ClusterNodeName>,
    release_owner: Option<SessionServiceImpl>,
}

struct PendingClusterEntityGateRelease {
    operation_id: u64,
    domain: DomainName,
    nodes: BTreeSet<ClusterNodeName>,
}

/// One entity-gate engagement the leader asks a node to perform: the operation it belongs to, the
/// domain relays and entities it freezes, how long the node may take, and why. The local and remote
/// paths consume the same value, so the two cannot describe different gates.
#[derive(Clone, Copy)]
struct EntityGateEngagement<'a> {
    operation_id: u64,
    domain: &'a DomainName,
    relays: &'a [RelayName],
    affected_entities: &'a [NodeRef],
    purpose: EntityGatePurpose,
    deadline: tokio::time::Instant,
    reason: &'a str,
}

impl ClusterEntityGate {
    fn new(service: &SessionServiceImpl, operation_id: u64, domain: &DomainName) -> Self {
        Self {
            operation_id,
            domain: domain.clone(),
            nodes: BTreeSet::new(),
            release_owner: Some(service.clone()),
        }
    }

    /// Records a node before sending its engagement request. A response timeout is ambiguous: the
    /// remote node may already own the durable lease, so cleanup must include every attempted node
    /// and rely on idempotent release.
    fn record_attempt(&mut self, node: ClusterNodeName) {
        self.nodes.insert(node);
    }

    fn mark_released(&mut self, node: &ClusterNodeName) {
        self.nodes.remove(node);
    }

    fn schedule_remaining_releases(&mut self) {
        let Some(owner) = self.release_owner.take() else {
            return;
        };
        if self.nodes.is_empty() {
            return;
        }
        owner.schedule_cluster_entity_gate_release(PendingClusterEntityGateRelease {
            operation_id: self.operation_id,
            domain: self.domain.clone(),
            nodes: std::mem::take(&mut self.nodes),
        });
    }

    pub(in crate::application) fn defer_release_to_lease_deadline(mut self) {
        self.release_owner = None;
    }
}

impl Drop for ClusterEntityGate {
    fn drop(&mut self) {
        self.schedule_remaining_releases();
    }
}

impl SessionServiceImpl {
    fn next_entity_gate_operation_id(&self) -> u64 {
        self.inner
            .next_entity_gate_operation_id
            .fetch_add(1, Ordering::Relaxed)
    }

    pub(in crate::application) fn local_domain_drain_status(
        &self,
        domain: &DomainName,
    ) -> DomainDrainStatusEnvelope {
        let status = self.inner.runtime.domain_drain_status(domain);
        let emitter_publishing = status
            .emitter_publishing
            .into_iter()
            .map(emitter_publishing_drain_status_envelope)
            .collect();
        DomainDrainStatusEnvelope {
            active_ingestors: status.active_ingestors.arch_into(),
            active_generators: status.active_generators.arch_into(),
            outstanding_acks: status.outstanding_acks.arch_into(),
            buffered_emitter_messages: status.buffered_emitter_messages.arch_into(),
            emitter_publishing,
        }
    }

    pub(in crate::application) async fn domain_drain_status_on_node(
        &self,
        node_id: &ClusterNodeName,
        domain: &DomainName,
    ) -> Result<DomainDrainStatusEnvelope, String> {
        if node_id == self.inner.consensus.local_node_id() {
            self.inner.runtime.force_flush_domain_if_idle(domain);
            return Ok(self.local_domain_drain_status(domain));
        }
        self.inner
            .interconnect
            .request_with_timeout(
                node_id,
                RemoteDomainDrainStatusRequest {
                    domain: domain.clone(),
                },
                Duration::from_secs(2),
            )
            .await
            .map_err(|error| error.to_string())?
            .result
    }

    pub(in crate::application) fn local_entity_drain_status(
        &self,
        domain: &DomainName,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
    ) -> EntityDrainStatusEnvelope {
        let status =
            self.inner
                .runtime
                .entity_drain_status(domain, relays, affected_entities, purpose);
        let emitter_publishing = status
            .emitter_publishing
            .into_iter()
            .map(emitter_publishing_drain_status_envelope)
            .collect();
        EntityDrainStatusEnvelope {
            buffered_relay_batches: status.buffered_relay_batches.arch_into(),
            node_work_items: status.node_work_items.arch_into(),
            outstanding_acks: status.outstanding_acks.arch_into(),
            emitter_publishing,
        }
    }

    async fn engage_entity_gate_on_node(
        &self,
        node_id: &ClusterNodeName,
        engagement: EntityGateEngagement<'_>,
    ) -> Result<(), String> {
        let EntityGateEngagement {
            operation_id,
            domain,
            relays,
            affected_entities,
            purpose,
            deadline,
            reason,
        } = engagement;
        if node_id == self.inner.consensus.local_node_id() {
            return self
                .inner
                .runtime
                .engage_entity_gate_operation(
                    operation_id,
                    domain,
                    relays,
                    affected_entities,
                    purpose,
                    EntityGateLease { deadline, reason },
                )
                .await;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let deadline_millis = u64::try_from(remaining.as_millis().max(1)).unwrap_or(u64::MAX);
        self.inner
            .interconnect
            .request_with_timeout(
                node_id,
                RemoteEntityGateRequest {
                    operation_id,
                    domain: domain.clone(),
                    relays: relays.to_vec(),
                    affected_entities: affected_entities.to_vec(),
                    purpose,
                    deadline_millis,
                    reason: reason.to_string(),
                },
                remaining.min(Duration::from_secs(2)),
            )
            .await
            .map_err(|error| error.to_string())?
            .result
    }

    async fn entity_drain_status_on_node(
        &self,
        node_id: &ClusterNodeName,
        domain: &DomainName,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
        deadline: tokio::time::Instant,
    ) -> Result<EntityDrainStatusEnvelope, String> {
        if node_id == self.inner.consensus.local_node_id() {
            let status = self.local_entity_drain_status(domain, relays, affected_entities, purpose);
            if status.buffered_relay_batches != 0
                || status.node_work_items != 0
                || status.outstanding_acks != 0
            {
                self.inner.runtime.force_flush_domain_if_idle(domain);
            }
            return Ok(status);
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        self.inner
            .interconnect
            .request_with_timeout(
                node_id,
                RemoteEntityDrainStatusRequest {
                    domain: domain.clone(),
                    relays: relays.to_vec(),
                    affected_entities: affected_entities.to_vec(),
                    purpose,
                },
                remaining.min(Duration::from_secs(2)),
            )
            .await
            .map_err(|error| error.to_string())?
            .result
    }

    async fn release_entity_gate_on_node(
        &self,
        node_id: &ClusterNodeName,
        operation_id: u64,
        domain: &DomainName,
    ) -> Result<(), String> {
        if node_id == self.inner.consensus.local_node_id() {
            return self
                .inner
                .runtime
                .release_entity_gate_operation(operation_id, domain)
                .await;
        }
        self.inner
            .interconnect
            .request(
                node_id,
                RemoteEntityGateReleaseRequest {
                    operation_id,
                    domain: domain.clone(),
                },
            )
            .await
            .map_err(|error| error.to_string())?
            .result
    }

    fn schedule_cluster_entity_gate_release(&self, release: PendingClusterEntityGateRelease) {
        let service = self.clone();
        self.inner.service_tasks.spawn(async move {
            service.retry_cluster_entity_gate_release(release).await;
        });
    }

    async fn retry_cluster_entity_gate_release(
        &self,
        mut release: PendingClusterEntityGateRelease,
    ) {
        while !release.nodes.is_empty() {
            tokio::task::consume_budget().await;
            let nodes = release.nodes.clone();
            for node in nodes {
                tokio::task::consume_budget().await;
                let result = tokio::select! {
                    _ = self.inner.shutdown.cancelled() => return,
                    result = self.release_entity_gate_on_node(
                        &node,
                        release.operation_id,
                        &release.domain,
                    ) => result,
                };
                match result {
                    Ok(()) => {
                        release.nodes.remove(&node);
                    }
                    Err(error) => {
                        debug!(
                            domain = release.domain.as_str(),
                            operation_id = release.operation_id,
                            %node,
                            error,
                            "entity gate release retry remains pending"
                        );
                    }
                }
            }
            if release.nodes.is_empty() {
                return;
            }
            tokio::select! {
                _ = self.inner.shutdown.cancelled() => return,
                _ = sleep(ENTITY_GATE_RELEASE_RETRY_INTERVAL) => {}
            }
        }
    }

    pub(in crate::application) async fn engage_cluster_entity_gates(
        &self,
        domain: &DomainName,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
        deadline: tokio::time::Instant,
    ) -> Result<ClusterEntityGate, Report<DomainAlterError>> {
        let mut nodes = self.available_node_ids().await;
        if !nodes
            .iter()
            .any(|node| node == self.inner.consensus.local_node_id())
        {
            nodes.push(self.inner.consensus.local_node_id().clone());
        }
        nodes.sort();
        nodes.dedup();
        let operation_id = self.next_entity_gate_operation_id();
        let mut gate = ClusterEntityGate::new(self, operation_id, domain);
        let reason = match purpose {
            EntityGatePurpose::ModelAlteration => "leader-orchestrated entity alteration",
            EntityGatePurpose::OwnershipHandoff => "leader-orchestrated ownership handoff",
        };
        for node in &nodes {
            tokio::task::consume_budget().await;
            gate.record_attempt(node.clone());
            if let Err(error) = self
                .engage_entity_gate_on_node(
                    node,
                    EntityGateEngagement {
                        operation_id,
                        domain,
                        relays,
                        affected_entities,
                        purpose,
                        deadline,
                        reason,
                    },
                )
                .await
            {
                self.release_cluster_entity_gates(gate).await;
                return Err(Report::new(DomainAlterError::EntityGate {
                    domain: domain.clone(),
                    operation: purpose.operation_name(),
                    reason: format!("failed to engage entity gates on node '{node}': {error}"),
                }));
            }
        }
        Ok(gate)
    }

    pub(in crate::application) async fn wait_for_cluster_entity_drain(
        &self,
        gate: &ClusterEntityGate,
        relays: &[RelayName],
        affected_entities: &[NodeRef],
        purpose: EntityGatePurpose,
        required_live_nodes: &[ClusterNodeName],
        deadline: tokio::time::Instant,
    ) -> Result<(), Report<DomainAlterError>> {
        let domain = &gate.domain;
        let mut polling = interval(Duration::from_millis(25));
        polling.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_status = None::<(ClusterNodeName, EntityDrainStatusEnvelope)>;
        loop {
            tokio::task::consume_budget().await;
            polling.tick().await;
            if !required_live_nodes.is_empty() {
                let live_nodes = self
                    .available_node_ids()
                    .await
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                if let Some(node) = required_live_nodes
                    .iter()
                    .find(|node| !live_nodes.contains(*node))
                {
                    return Err(Report::new(DomainAlterError::EntityGate {
                        domain: domain.clone(),
                        operation: purpose.operation_name(),
                        reason: format!(
                            "former owner '{node}' became unavailable during ownership handoff"
                        ),
                    }));
                }
            }
            let mut all_drained = true;
            for node in &gate.nodes {
                tokio::task::consume_budget().await;
                match self
                    .entity_drain_status_on_node(
                        node,
                        domain,
                        relays,
                        affected_entities,
                        purpose,
                        deadline,
                    )
                    .await
                {
                    Ok(status)
                        if status.buffered_relay_batches == 0
                            && status.node_work_items == 0
                            && status.outstanding_acks == 0 => {}
                    Ok(status) => {
                        all_drained = false;
                        last_status = Some((node.clone(), status));
                    }
                    Err(error) => {
                        if required_live_nodes.iter().any(|required| required == node) {
                            return Err(Report::new(DomainAlterError::EntityGate {
                                domain: domain.clone(),
                                operation: purpose.operation_name(),
                                reason: format!(
                                    "former owner '{node}' became unavailable during ownership \
                                     handoff: {error}"
                                ),
                            }));
                        }
                        all_drained = false;
                        if tokio::time::Instant::now() >= deadline {
                            warn!(
                                domain = domain.as_str(),
                                %node, error, "failed to retrieve entity drain status"
                            );
                        }
                    }
                }
            }
            if all_drained {
                return Ok(());
            }
            #[cfg(feature = "testing")]
            let force_timeout = if last_status.is_some() {
                self.inner.runtime.take_forced_entity_drain_timeout(domain)
            } else {
                false
            };
            #[cfg(not(feature = "testing"))]
            let force_timeout = false;
            if force_timeout || tokio::time::Instant::now() >= deadline {
                let Some((pending_node, last_status)) = last_status.or_else(|| {
                    // A gate holding no nodes has nothing left to drain, so there is no pending
                    // node to name and no timeout to report.
                    Some((
                        gate.nodes.first().cloned()?,
                        EntityDrainStatusEnvelope {
                            buffered_relay_batches: 0,
                            node_work_items: 0,
                            outstanding_acks: 0,
                            emitter_publishing: Vec::new(),
                        },
                    ))
                }) else {
                    return Ok(());
                };
                return Err(Report::new(DomainAlterError::EntityQuiesceTimeout {
                    domain: domain.clone(),
                    operation: purpose.operation_name(),
                    pending_node,
                    buffered_relay_batches: last_status.buffered_relay_batches.arch_into(),
                    node_work_items: last_status.node_work_items.arch_into(),
                    outstanding_acks: last_status.outstanding_acks.arch_into(),
                    emitter_publishing: emitter_publishing_drain_summary(
                        &last_status.emitter_publishing,
                    ),
                }));
            }
        }
    }

    pub(in crate::application) async fn release_cluster_entity_gates(
        &self,
        mut gate: ClusterEntityGate,
    ) {
        let domain = gate.domain.clone();
        for node in gate.nodes.clone() {
            tokio::task::consume_budget().await;
            match self
                .release_entity_gate_on_node(&node, gate.operation_id, &domain)
                .await
            {
                Ok(()) => gate.mark_released(&node),
                Err(error) => {
                    warn!(
                        domain = domain.as_str(),
                        %node, error, "failed to release entity gates; scheduling retry"
                    );
                    self.broadcast_error(format!(
                        "failed to release entity gates on node '{node}' in domain '{}': {error}; \
                         release will retry in the background",
                        domain.as_str()
                    ));
                }
            }
        }
        gate.schedule_remaining_releases();
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::Duration;

    use super::{
        super::test_fixtures::{TestService, build_test_service},
        *,
    };

    #[tokio::test]
    async fn dropping_cluster_gate_owner_releases_local_durable_hold() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(false).await;
        let domain = DomainName::parse("default").expect("valid domain");
        let operation_id = 41;
        service
            .inner
            .runtime
            .engage_entity_gate_operation(
                operation_id,
                &domain,
                &[],
                &[],
                EntityGatePurpose::ModelAlteration,
                EntityGateLease {
                    deadline: tokio::time::Instant::now() + Duration::from_secs(30),
                    reason: "canceled coordinator test",
                },
            )
            .await
            .expect("local gate hold should engage");
        assert!(
            service
                .inner
                .runtime
                .entity_gate_operation_is_held(operation_id, &domain)
        );

        let mut gate = ClusterEntityGate::new(&service, operation_id, &domain);
        gate.record_attempt(service.inner.consensus.local_node_id().clone());
        drop(gate);

        tokio::time::timeout(Duration::from_secs(2), async {
            while service
                .inner
                .runtime
                .entity_gate_operation_is_held(operation_id, &domain)
            {
                tokio::task::consume_budget().await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped coordinator guard should release its local durable hold");

        service.inner.shutdown.cancel();
        service.inner.service_tasks.close();
        service.inner.service_tasks.wait().await;
        let _ = std::fs::remove_dir_all(path);
    }
}
