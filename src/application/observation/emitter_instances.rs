//! The instances of an emitter that runs several, as `DESCRIBE EMITTER` reports them.
//!
//! Layer: control plane.
//!
//! - **Owns.** Each instance's scheduled owner, the input partitions it serves and whether its task
//!   runs there, asked of every owner concurrently.
//! - **Depends on.** The schedule's instance units, the inputs' shared branch, the runtime's local
//!   task registry and the dataflow node status request its owners answer.
//! - **Must not know.** How an instance's task publishes or how its inputs are delivered.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::num::NonZeroU64;

use arch_into::ArchInto;
use futures_util::future::join_all;
use nervix_interconnect::{
    DataflowNodeStatusRequest as RemoteDataflowNodeStatusRequest,
    DataflowNodeStatusResponse as RemoteDataflowNodeStatusResponse, RemoteOperationFailure,
};
use nervix_models::{
    ClusterNodeName, CreateEmitter, DomainName, DomainSchedule, EmitterName, ModelKind, ModelName,
    NodeRef, ResolvedBranchPartitions, ScheduledNode, Unit, UnitKind,
};

use super::super::{
    describe_output::{EmitterInstanceStatus, EmitterInstanceSummary},
    session_service::SessionServiceImpl,
};

/// An emitter instance's unit and the index its unit names.
#[derive(Debug, Clone, Copy)]
struct EmitterInstanceUnit {
    unit: Unit,
    index: u64,
}

impl SessionServiceImpl {
    /// One summary per instance of `emitter`, in instance order: the node it is scheduled on,
    /// whether its task runs there, and the input partitions it serves. Owners are asked
    /// concurrently, so an owner that does not answer costs one request deadline, not one per
    /// instance.
    pub(super) async fn emitter_instance_summaries(
        &self,
        domain: &DomainName,
        emitter: &CreateEmitter,
        scheduled_domain: &DomainSchedule,
        node: &ScheduledNode,
    ) -> Vec<EmitterInstanceSummary> {
        let partitions = Self::emitter_input_partitions(emitter, scheduled_domain);
        let unit_count: u64 = node.units.len().arch_into();
        let Some(count) = NonZeroU64::new(unit_count) else {
            return Vec::new();
        };
        let mut summaries = Vec::with_capacity(node.units.len());
        for unit in node.units.keys() {
            nervix_primitives::task::consume_budget().await;
            let UnitKind::Instance(index) = unit.kind() else {
                continue;
            };
            let owner = node.unit_execution_node(*unit).cloned();
            let served = match partitions {
                Some(partitions) => partitions.units_served_by(index, count),
                None => Vec::new(),
            };
            summaries.push(self.emitter_instance_summary(
                domain,
                &emitter.name,
                EmitterInstanceUnit { unit: *unit, index },
                owner,
                served,
            ));
        }
        join_all(summaries).await
    }

    /// The partitions of an instanced emitter's inputs. Validation admits instances only over
    /// inputs that share one branch, so the first input's partitions are every input's.
    fn emitter_input_partitions(
        emitter: &CreateEmitter,
        scheduled_domain: &DomainSchedule,
    ) -> Option<ResolvedBranchPartitions> {
        let Some(relay) = emitter.from.relays().first() else {
            return Some(ResolvedBranchPartitions::ONE);
        };
        let Some(relay) = scheduled_domain
            .nodes
            .get(&NodeRef::new(ModelKind::Relay, relay))
        else {
            return Some(ResolvedBranchPartitions::ONE);
        };
        let Some(branching) = &relay.resolved_branching else {
            return Some(ResolvedBranchPartitions::ONE);
        };
        if branching.is_awaiting_topology() {
            None
        } else {
            Some(
                branching
                    .partitions()
                    .unwrap_or(ResolvedBranchPartitions::ONE),
            )
        }
    }

    async fn emitter_instance_summary(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
        instance: EmitterInstanceUnit,
        owner: Option<ClusterNodeName>,
        partitions: Vec<Unit>,
    ) -> EmitterInstanceSummary {
        let status = if partitions.is_empty() {
            EmitterInstanceStatus::Idle
        } else {
            self.emitter_instance_status(domain, emitter, instance.unit, owner.as_ref())
                .await
        };
        EmitterInstanceSummary {
            index: instance.index,
            owner,
            status,
            partitions,
        }
    }

    /// Whether the task of an emitter instance that serves partitions runs on `owner`, as the
    /// owner itself reports it.
    async fn emitter_instance_status(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
        unit: Unit,
        owner: Option<&ClusterNodeName>,
    ) -> EmitterInstanceStatus {
        let Some(owner) = owner else {
            return EmitterInstanceStatus::Stopped;
        };
        if owner == self.inner.consensus.local_node_id() {
            if self
                .inner
                .runtime
                .emitter_unit_is_running(domain, emitter, unit)
            {
                return EmitterInstanceStatus::Running;
            }
            return EmitterInstanceStatus::Stopped;
        }
        let response = self
            .inner
            .interconnect
            .request_with_timeout(
                owner,
                RemoteDataflowNodeStatusRequest {
                    domain: domain.clone(),
                    kind: ModelKind::Emitter,
                    name: ModelName::from(emitter),
                    unit,
                },
                super::DATAFLOW_NODE_STATUS_TIMEOUT,
            )
            .await;
        match response {
            Ok(RemoteDataflowNodeStatusResponse { result: Ok(_) }) => {
                EmitterInstanceStatus::Running
            }
            Ok(RemoteDataflowNodeStatusResponse {
                result: Err(RemoteOperationFailure::Unavailable { .. }),
            }) => EmitterInstanceStatus::Stopped,
            Ok(RemoteDataflowNodeStatusResponse { result: Err(_) }) | Err(_) => {
                EmitterInstanceStatus::Unknown
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nervix_recovery::Discarded as _;

    use super::*;
    use crate::application::test_fixtures::{TestService, build_test_service, named};

    #[nervix_primitives::test(start_paused = true)]
    async fn an_instance_is_stopped_where_no_task_runs_and_unknown_where_its_owner_cannot_answer() {
        let TestService { service, path, .. } = build_test_service(false).await;
        let domain: DomainName = named("instances");
        let emitter: EmitterName = named("events_out");
        let unit = Unit::FIRST_INSTANCE;

        assert_eq!(
            service
                .emitter_instance_status(&domain, &emitter, unit, None)
                .await,
            EmitterInstanceStatus::Stopped,
            "an instance no node is scheduled to run runs nowhere"
        );
        let local = service.inner.consensus.local_node_id().clone();
        assert_eq!(
            service
                .emitter_instance_status(&domain, &emitter, unit, Some(&local))
                .await,
            EmitterInstanceStatus::Stopped,
            "this node runs no task for the instance"
        );
        let unreachable: ClusterNodeName = named("unreachable_peer");
        assert_eq!(
            service
                .emitter_instance_status(&domain, &emitter, unit, Some(&unreachable))
                .await,
            EmitterInstanceStatus::Unknown,
            "an owner the interconnect cannot reach does not report the instance"
        );

        let idle = service
            .emitter_instance_summary(
                &domain,
                &emitter,
                EmitterInstanceUnit { unit, index: 0 },
                Some(unreachable),
                Vec::new(),
            )
            .await;
        assert_eq!(idle.status, EmitterInstanceStatus::Idle);

        std::fs::remove_dir_all(path).discarded("the throwaway test database may already be gone");
    }
}
