//! Replacing a relay's boundary services when its branch's partition count changes.
//!
//! Layer: data plane.
//!
//! - **Owns.** Deciding which installed relays a revision repartitions, ending their owner tasks,
//!   building their services for the new unit set on the same fan-out with the same subscribers,
//!   installing them in the domain's routing and starting the owner task of each unit of the new
//!   count this node owns.
//! - **Depends on.** The installed execution and its relay services, the revision's activation
//!   plan, branch presences and the relay owner task.
//! - **Must not know.** Plexus text, transaction planning, consensus, or how keys move between
//!   units after the activation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a gated entity swap replaces a repartitioned relay's services once per \
                  activation"
    )
)]

use std::collections::BTreeSet;

use error_stack::ResultExt as _;
use meticulous::OptionExt as _;
use nervix_models::{ClusterNodeName, DomainName, ModelKind, ModelName, NodeRef, RelayName, Unit};
use nervix_primitives::sync::Arc;

use super::{
    ExecutionBuildError, ExecutionRevision, ExecutionStep, RelayPartitioning,
    RelaySubscriptionDefinition, RelayUnitRef, Runtime, RuntimeError,
    domain_rebuild::branch_relays_from_plans,
};

/// How the previous partition count placed the keys of every relay of `revision` whose branch is in
/// a partition transition.
pub(super) fn relay_transitions(
    revision: &ExecutionRevision,
) -> super::HashMap<RelayName, RelayPartitioning> {
    let mut transitions = super::HashMap::default();
    for node in revision.nodes.values() {
        if node.kind() != ModelKind::Relay {
            continue;
        }
        if let Some(transition) = &node.partition_transition {
            transitions.insert(
                RelayName::from(&node.identifier),
                RelayPartitioning::of_partitions(transition.previous),
            );
        }
    }
    transitions
}

/// The relay consumers the installed execution registered, as a swap withdraws them from the
/// services they count in before their replacements register.
pub(super) struct InstalledRegistrations<'a> {
    installed: Option<&'a ExecutionRevision>,
    repartitioned: &'a BTreeSet<RelayName>,
}

impl<'a> InstalledRegistrations<'a> {
    pub(super) fn new(
        installed: Option<&'a ExecutionRevision>,
        repartitioned: &'a BTreeSet<RelayName>,
    ) -> Self {
        Self {
            installed,
            repartitioned,
        }
    }

    /// Whether the installed consumer of `relay` registered for `unit` of `entity`, or for the
    /// whole relay when `unit` is absent, still counts in the relay's current services. A
    /// repartitioned relay's services replaced the ones it counted in, and a unit holding only
    /// keys the previous count placed there read a dormant audience counted by none.
    pub(super) fn counts(&self, relay: &RelayName, entity: &NodeRef, unit: Option<Unit>) -> bool {
        if self.repartitioned.contains(relay) {
            return false;
        }
        let Some(unit) = unit else {
            return true;
        };
        match self
            .installed
            .and_then(|installed| installed.nodes.get(entity))
        {
            Some(node) => !node.is_holding_unit(unit),
            None => true,
        }
    }
}

impl Runtime {
    /// Gives every relay whose branch's partition count `revision` changes new boundary services
    /// for the new count, and returns those relays. The swap's gate holds every producer and
    /// consumer of them; the swap replaces those entities next, and they register with the new
    /// services.
    pub(super) async fn repartition_relays(
        &self,
        domain: &DomainName,
        revision: &ExecutionRevision,
        local_node_id: Option<&ClusterNodeName>,
    ) -> error_stack::Result<BTreeSet<RelayName>, RuntimeError> {
        let build = || RuntimeError::build_domain_execution(domain);
        let execution_unavailable = || {
            error_stack::Report::new(ExecutionBuildError::ExecutionUnavailable {
                step: ExecutionStep::RelayTransition,
            })
            .change_context(build())
        };
        let mut repartitioned = Vec::new();
        {
            // Without an installed execution no relay has services to replace; the swap's later
            // steps report what each of them could not find.
            let Some(execution) = self.inner.executions.get(domain) else {
                return Ok(BTreeSet::new());
            };
            for relay in revision.activation.relays.values() {
                let desired = RelayPartitioning::of(Some(&relay.branching));
                let Some(services) = execution.relay_services.get(&relay.name) else {
                    continue;
                };
                if services.partitioning != desired {
                    repartitioned.push((relay.clone(), services.clone()));
                }
            }
        }
        let branch_relays = branch_relays_from_plans(&revision.processors, &revision.entrypoints);
        let mut replaced = BTreeSet::new();
        for (relay, previous) in repartitioned {
            nervix_primitives::task::consume_budget().await;
            let owner_tasks = {
                let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                    return Err(execution_unavailable());
                };
                let units = execution
                    .relay_owner_tasks
                    .keys()
                    .filter(|owned| owned.relay == relay.name)
                    .cloned()
                    .collect::<Vec<_>>();
                let mut tasks = Vec::with_capacity(units.len());
                for owned in units {
                    if let Some(task) = execution.relay_owner_tasks.remove(&owned) {
                        tasks.push(task);
                    }
                }
                tasks
            };
            for task in owner_tasks {
                nervix_primitives::task::consume_budget().await;
                task.stop(self.branch_task_stop_timeout())
                    .await
                    .change_context_lazy(|| ExecutionBuildError::DrainRelay {
                        relay: relay.name.clone(),
                    })
                    .change_context_lazy(build)?;
            }
            let node = revision
                .nodes
                .get(&NodeRef::new(
                    ModelKind::Relay,
                    ModelName::from(&relay.name),
                ))
                .assured("the domain plan contains exactly the scheduled relays");
            // The fan-out takes the definition of the new count; its subscribers keep reading,
            // since the relay's rows are the same under either count.
            Box::pin(self.relay_boundary_fanout_with_capacity(
                domain,
                &relay.name,
                relay.capacity,
                RelaySubscriptionDefinition::new(relay.schema.clone(), relay.branching.clone()),
            ))
            .await;
            let partitioning = RelayPartitioning::of(Some(&relay.branching));
            let branch_relay = branch_relays.contains(&relay.name);
            let presences = self
                .relay_branch_presences(domain, &relay.name, partitioning, |unit| {
                    // An unplaced graph owns every unit wherever it runs.
                    let local = match local_node_id {
                        Some(local) => node.unit_executes_on(unit, local),
                        None => true,
                    };
                    branch_relay && local
                })
                .change_context_lazy(|| ExecutionBuildError::RelayBranchPresence {
                    relay: relay.name.clone(),
                })
                .change_context_lazy(build)?;
            let services = Arc::new(previous.repartitioned(presences));
            {
                let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                    return Err(execution_unavailable());
                };
                execution
                    .relay_services
                    .insert(relay.name.clone(), services.clone());
                execution
                    .relay_branchings
                    .insert(relay.name.clone(), relay.branching.clone());
            }
            let local_units = match local_node_id {
                Some(local) => node.units_executing_on(local),
                None => node.units.keys().copied().collect(),
            };
            for unit in local_units {
                nervix_primitives::task::consume_budget().await;
                if !services.declares_unit(unit) {
                    continue;
                }
                let task = self
                    .spawn_relay_owner_task(
                        domain,
                        &relay.name,
                        unit,
                        services.clone(),
                        relay.retention,
                    )
                    .change_context_lazy(build)?;
                let Some(mut execution) = self.inner.executions.get_mut(domain) else {
                    return Err(execution_unavailable());
                };
                execution
                    .relay_owner_tasks
                    .insert(RelayUnitRef::new(relay.name.clone(), unit), task);
            }
            replaced.insert(relay.name.clone());
        }
        Ok(replaced)
    }
}
