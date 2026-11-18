//! Ending a branch's retired partition transition where the branch's units run.
//!
//! Layer: data plane.
//!
//! - **Owns.** Telling every local unit of an entity that keeps state per key that its branch's
//!   transition retired, and ending each local unit the retired schedule no longer holds, with
//!   its placement tasks and its materialized relay owner route.
//! - **Depends on.** The installed execution's processor supervisors, materialized relay state
//!   tasks and placement tasks, and the revision the retirement installs.
//! - **Must not know.** How the leader decides that a transition retired, how a unit parks input or
//!   moves keys, or Plexus text.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "a branch's partition transition retires once per partition epoch"
    )
)]

use std::collections::BTreeSet;

use nervix_models::{DomainName, ModelKind, NodeRef, NodeUnitRef, RelayName, Unit};
use nervix_primitives::sync::mpsc;
use nervix_recovery::NoReceiver as _;
use tracing::warn;

use super::{
    ExecutionRevision, PlacementTasks, ProcessorNodeCommand, RelayOwnerRoutes, RelayStateTask,
    RelayUnitRef, Runtime, ScheduledNodeTask, materialized_partition_moves::RelayStateCommand,
};

/// What a retirement does to one local unit's task.
enum RetiringUnit {
    /// A processor unit the retired schedule keeps: its supervisor stops claiming keys.
    KeptProcessor(mpsc::Sender<ProcessorNodeCommand>),
    /// A materialized relay unit the retired schedule keeps: its state task stops claiming rows.
    KeptRelay(mpsc::Sender<RelayStateCommand>),
    /// A processor unit that held only keys the previous count placed in it.
    EndedProcessor { unit: Unit, task: ScheduledNodeTask },
    /// A materialized relay unit that held only rows the previous count placed in it.
    EndedRelay(RelayStateTask),
}

/// The local tasks of one entity a retirement changes.
struct RetiringEntity {
    units: Vec<RetiringUnit>,
    placements: Vec<PlacementTasks>,
}

impl Runtime {
    /// Retires the partition transition of `node`'s branch on this cluster node, as `desired`
    /// schedules it after the retirement. Every local unit `desired` keeps stops claiming keys
    /// and runs the input it parked, and every local unit it no longer holds ends.
    ///
    /// The leader retires a transition only once no unit holds or claims a key outside its
    /// partition, and no unit gains such a key afterwards: a unit that ends holds nothing, and a
    /// claim still in flight finds no state, so the input it parked begins its key where it waits.
    /// No task that keeps its unit stops, so no input is held back or lost while the transition
    /// ends, and nothing gates the unit's relays.
    pub(super) async fn retire_partition_transition(
        &self,
        domain: &DomainName,
        node: &NodeRef,
        desired: &ExecutionRevision,
    ) {
        let Some(retiring) = self.take_retiring_entity(domain, node, desired) else {
            return;
        };
        let RetiringEntity { units, placements } = retiring;
        for unit in units {
            nervix_primitives::task::consume_budget().await;
            match unit {
                RetiringUnit::KeptProcessor(commands) => {
                    commands
                        .send(ProcessorNodeCommand::PartitionTransitionRetired)
                        .await
                        .means_shutdown("processor unit whose partition transition retired");
                }
                RetiringUnit::KeptRelay(commands) => {
                    commands
                        .send(RelayStateCommand::TransitionRetired)
                        .await
                        .means_shutdown(
                            "materialized relay unit whose partition transition retired",
                        );
                }
                RetiringUnit::EndedProcessor { unit, task } => {
                    self.end_holding_processor_unit(domain, node, unit, task)
                        .await;
                }
                RetiringUnit::EndedRelay(task) => {
                    let unit = task.unit;
                    if let Err(error) = task.stop(self.task_stop_grace()).await {
                        warn!(
                            domain = domain.as_str(),
                            relay = node.identifier.as_str(),
                            ?unit,
                            error = %format_args!("{error:#}"),
                            "a retired transition's holding unit did not stop within its grace"
                        );
                    }
                }
            }
        }
        for tasks in placements {
            nervix_primitives::task::consume_budget().await;
            tasks.abort().await;
        }
    }

    /// Takes from the installed execution what retiring `node`'s transition changes: the command
    /// channels of the units `desired` keeps, and the tasks and placement tasks of the units it no
    /// longer holds. Returns nothing when the domain or the node is not installed here.
    fn take_retiring_entity(
        &self,
        domain: &DomainName,
        node: &NodeRef,
        desired: &ExecutionRevision,
    ) -> Option<RetiringEntity> {
        let desired_node = desired.nodes.get(node);
        let kept = match desired_node {
            Some(desired_node) => desired_node.units.keys().copied().collect::<BTreeSet<_>>(),
            None => BTreeSet::new(),
        };
        let mut execution = self.inner.executions.get_mut(domain)?;
        let installed = execution.revision.nodes.get(node)?;
        let units = installed.units.keys().copied().collect::<Vec<_>>();
        let relay = match node.kind {
            ModelKind::Relay => Some(RelayName::from(&node.identifier)),
            _ => None,
        };
        let mut retiring = RetiringEntity {
            units: Vec::new(),
            placements: Vec::new(),
        };
        for unit in units {
            let keeps = kept.contains(&unit);
            if !keeps
                && let Some(tasks) = execution
                    .placement_tasks
                    .remove(&NodeUnitRef::new(node.clone(), unit))
            {
                retiring.placements.push(tasks);
            }
            if let Some(relay) = relay.as_ref() {
                let key = RelayUnitRef::new(relay.clone(), unit);
                if keeps {
                    if let Some(task) = execution.relay_state_tasks.get(&key) {
                        retiring
                            .units
                            .push(RetiringUnit::KeptRelay(task.commands.clone()));
                    }
                } else if let Some(task) = execution.relay_state_tasks.remove(&key) {
                    retiring.units.push(RetiringUnit::EndedRelay(task));
                }
                continue;
            }
            let owner = NodeUnitRef::new(node.clone(), unit);
            if keeps {
                if let Some(task) = execution.node_tasks.get(&owner) {
                    retiring
                        .units
                        .push(RetiringUnit::KeptProcessor(task.commands.clone()));
                }
            } else if let Some(task) = execution.node_tasks.remove(&owner) {
                retiring
                    .units
                    .push(RetiringUnit::EndedProcessor { unit, task });
            }
        }
        // Reads of a key go to the unit its partition count places it in; the units that end no
        // longer answer for any key.
        if let (Some(relay), Some(desired_node)) = (relay, desired_node)
            && execution
                .materialized_stream_owner_nodes
                .contains_key(&relay)
        {
            execution.materialized_stream_owner_nodes.insert(
                relay,
                RelayOwnerRoutes::new(desired_node.unit_execution_nodes()),
            );
        }
        Some(retiring)
    }

    /// Ends a processor unit that held only keys the previous count placed in it. Its supervisor
    /// hands over what it holds, which the retirement guarantees is nothing.
    async fn end_holding_processor_unit(
        &self,
        domain: &DomainName,
        node: &NodeRef,
        unit: Unit,
        task: ScheduledNodeTask,
    ) {
        match task.handoff(self.task_stop_grace()).await {
            Ok(handoffs) => {
                if !handoffs.is_empty() {
                    warn!(
                        domain = domain.as_str(),
                        entity = node.identifier.as_str(),
                        ?unit,
                        branches = handoffs.len(),
                        "a retired transition's holding unit still held branches when it ended"
                    );
                }
            }
            Err(error) => {
                warn!(
                    domain = domain.as_str(),
                    entity = node.identifier.as_str(),
                    ?unit,
                    error = %format_args!("{error:#}"),
                    "a retired transition's holding unit did not stop within its grace"
                );
            }
        }
    }
}
