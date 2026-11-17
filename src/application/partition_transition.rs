//! The control-plane side of a branch's partition transitions: answering a peer's claim, catalog
//! and holdings requests, describing a branch's epoch, and retiring a transition once no unit holds
//! a key outside its partition.
//!
//! Layer: control plane.
//!
//! - **Owns.** The interconnect handler for partition-transition requests, the gathering of a
//!   branch's holdings from every node owning a unit of its keyed entities, `DESCRIBE BRANCH`, and
//!   the leader's retirement of a transition whose holdings are all empty.
//! - **Depends on.** The committed schedule and its partition epochs, the interconnect, the
//!   runtime's published holdings and claim answers, and consensus's automatic schedule commit.
//! - **Must not know.** How a unit parks input, moves state or decides which keys it holds.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::BTreeSet;

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_consensus::TransactionState;
use nervix_interconnect::{
    PartitionTransitionAnswer, PartitionTransitionRequest, PartitionTransitionResponse,
};
use nervix_models::{
    BranchName, ClusterNodeName, CreateBranch, DescribeBranch, DomainName, DomainSchedule, NodeRef,
    NodeUnitRef, PartitionEpochNumber, Unit,
};
use tracing::{debug, info, warn};

use super::{
    AppError,
    command_result::CommandResult,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
};

/// Why a branch's holdings could not be read completely.
#[derive(Debug, thiserror::Error)]
pub(in crate::application) enum TransitionHoldingsError {
    #[error("node '{node}' did not answer for the units of branch '{branch}' it owns")]
    Unanswered {
        node: ClusterNodeName,
        branch: BranchName,
    },
    #[error("unit {unit:?} of {entity:?} has no owner in the committed schedule")]
    Unowned { entity: NodeRef, unit: Unit },
    #[error("unit {unit:?} of {entity:?} published no holdings in epoch {epoch}")]
    Unpublished {
        entity: NodeRef,
        unit: Unit,
        epoch: PartitionEpochNumber,
    },
}

/// The holdings of every unit of a branch's keyed entities, summed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(in crate::application) struct BranchTransitionHoldings {
    pub(in crate::application) pending: u64,
    pub(in crate::application) claimed: u64,
}

impl SessionServiceImpl {
    /// Reads the holdings of every unit of `branch`'s keyed entities in `schedule`, from the node
    /// owning each, and sums them. Every unit must answer for the sum to count.
    pub(in crate::application) async fn branch_transition_holdings(
        &self,
        schedule: &DomainSchedule,
        branch: &BranchName,
        epoch: PartitionEpochNumber,
    ) -> error_stack::Result<BranchTransitionHoldings, TransitionHoldingsError> {
        let mut expected = BTreeSet::new();
        let mut owners = BTreeSet::new();
        for node in schedule.nodes.values() {
            if node.partition_branch() != Some(branch) || !node.keeps_state_per_key() {
                continue;
            }
            for (unit, placement) in &node.units {
                let Some(owner) = placement.primary_node.clone() else {
                    return Err(Report::new(TransitionHoldingsError::Unowned {
                        entity: node.identity(),
                        unit: *unit,
                    }));
                };
                owners.insert(owner);
                expected.insert(NodeUnitRef::new(node.identity(), *unit));
            }
        }
        let local = self.inner.consensus.local_node_id().clone();
        let mut answered = Vec::new();
        for owner in owners {
            nervix_primitives::task::consume_budget().await;
            if owner == local {
                answered.extend(self.inner.runtime.partition_transition_holdings(
                    &schedule.domain,
                    branch,
                    epoch,
                ));
                continue;
            }
            let response = self
                .inner
                .interconnect
                .request(
                    &owner,
                    PartitionTransitionRequest::Holdings {
                        domain: schedule.domain.clone(),
                        branch: branch.clone(),
                        epoch,
                    },
                )
                .await
                .change_context_lazy(|| TransitionHoldingsError::Unanswered {
                    node: owner.clone(),
                    branch: branch.clone(),
                })?;
            match response.result {
                Ok(PartitionTransitionAnswer::Holdings(holdings)) => answered.extend(holdings),
                Ok(_) | Err(_) => {
                    return Err(Report::new(TransitionHoldingsError::Unanswered {
                        node: owner,
                        branch: branch.clone(),
                    }));
                }
            }
        }
        let mut sum = BranchTransitionHoldings::default();
        for holdings in answered {
            let unit = NodeUnitRef::new(holdings.entity.clone(), holdings.unit);
            if !expected.remove(&unit) {
                continue;
            }
            sum.pending = sum
                .pending
                .checked_add(holdings.pending)
                .assured("a branch's units hold fewer than 2^64 keys in all");
            sum.claimed = sum
                .claimed
                .checked_add(holdings.claimed)
                .assured("a branch's units claim fewer than 2^64 keys in one epoch");
        }
        if let Some(unit) = expected.into_iter().next() {
            return Err(Report::new(TransitionHoldingsError::Unpublished {
                entity: unit.node,
                unit: unit.unit,
                epoch,
            }));
        }
        Ok(sum)
    }
}

impl SessionServiceImpl {
    /// `DESCRIBE BRANCH`: the branch's declaration, the partition epoch it executes in and, while
    /// keys placed under its previous count may lie outside their partition, that count and how
    /// far moving them has come.
    pub(in crate::application) async fn describe_branch(
        &self,
        domain: &DomainName,
        describe: DescribeBranch,
    ) -> CommandResult {
        let declaration = match self
            .inner
            .registry
            .get::<CreateBranch>(domain, &describe.branch)
        {
            Ok(Some(declaration)) => declaration,
            Ok(None) => {
                return command_error(format!(
                    "branch '{}' does not exist in domain '{}'",
                    describe.branch.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read branch '{}' in domain '{}': {error:#}",
                    describe.branch.as_str(),
                    domain.as_str()
                ));
            }
        };
        let schedule = self.inner.consensus.current_schedule().await;
        let domain_schedule = schedule.domain(domain);
        let mut lines = vec![
            format!("branch: {}", declaration.name.as_str()),
            format!("schema: {}", declaration.schema.as_str()),
            format!("ttl: {}", declaration.ttl),
        ];
        if let Some(nervix_models::BranchEviction::Lru { max_instances }) = &declaration.eviction {
            lines.push(format!("max instances: {max_instances}"));
        }
        if declaration.partitions.is_inherited() {
            lines.push(format!("partition declaration: {}", declaration.partitions));
            if let Some(layout) = domain_schedule
                .and_then(|schedule| schedule.partition_topology.branches.get(&describe.branch))
            {
                lines.push(format!("partitions: {}", layout.partitions().count()));
                lines.push(format!("external partitions: {}", layout.source_count()));
                let idle = layout
                    .idle_partitions()
                    .map(|index| index.to_string())
                    .collect::<Vec<_>>();
                let idle = if idle.is_empty() {
                    "none".to_owned()
                } else {
                    idle.join(", ")
                };
                lines.push(format!("idle partitions: {idle}"));
            } else {
                lines.push("partitions: awaiting topology".to_owned());
            }
        } else {
            lines.push(format!("partitions: {}", declaration.partitions));
        }
        let Some(domain_schedule) = domain_schedule else {
            lines.push(format!("partition epoch: {}", PartitionEpochNumber::FIRST));
            return command_ok(lines.join("\n"));
        };
        let epoch = domain_schedule.partition_epoch_of(&describe.branch);
        lines.push(format!("partition epoch: {epoch}"));
        if let Some(transition) = domain_schedule.partition_transition_of(&describe.branch) {
            lines.push(format!(
                "previous partitions: {}",
                transition.previous.count()
            ));
            match self
                .branch_transition_holdings(domain_schedule, &describe.branch, epoch)
                .await
            {
                Ok(holdings) => {
                    lines.push(format!("keys pending: {}", holdings.pending));
                    lines.push(format!("keys claimed: {}", holdings.claimed));
                }
                Err(error) => {
                    lines.push(format!("keys pending: unavailable ({error:#})"));
                }
            }
        }
        command_ok(lines.join("\n"))
    }
}

impl SessionServiceImpl {
    /// Answers a peer's partition-transition requests from this node's runtime: a holder's
    /// release or forget of one key, a catalog page of the keys one unit holds for a claimant, and
    /// the holdings of every unit of a branch this node owns.
    pub(super) fn register_partition_transition_handler(
        &self,
    ) -> error_stack::Result<(), AppError> {
        let service = self.clone();
        self.inner
            .interconnect
            .register_handler::<PartitionTransitionRequest, _, _>(move |_context, request| {
                let service = service.clone();
                async move {
                    PartitionTransitionResponse {
                        result: service
                            .inner
                            .runtime
                            .answer_partition_transition(request)
                            .await,
                    }
                }
            })
            .change_context(AppError::RegisterInterconnectRequestHandler)
    }
}

impl SessionServiceImpl {
    /// Starts the task through which the leader retires each partition transition once no unit of
    /// its branch holds or claims a key outside its partition. It checks at the node's state
    /// snapshot interval, the cadence at which a moved key's state settles where it moved.
    pub(super) fn register_partition_transition_retirement(
        &self,
        shutdown: nervix_primitives::sync::CancellationToken,
    ) {
        let service = self.clone();
        self.inner.service_tasks.spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                let interval = service.inner.runtime.state_snapshot_interval();
                nervix_primitives::select! {
                    _ = shutdown.cancelled() => break,
                    _ = nervix_primitives::time::sleep(interval) => {}
                }
                service.retire_partition_transitions().await;
            }
        });
    }

    /// On the leader, commits the retirement of the first transition whose holdings are all empty.
    /// One retirement commits per pass, so the next pass reads the schedule it produced. A domain
    /// that a transaction is committing or an alteration holds is left to a later pass, as every
    /// automatic schedule change leaves it.
    async fn retire_partition_transitions(&self) {
        let Ok(input) = self.inner.consensus.automatic_schedule_input().await else {
            // Only the leader retires a transition.
            return;
        };
        let mut committing = BTreeSet::new();
        for transaction in self
            .inner
            .consensus
            .current_transactions()
            .await
            .into_values()
        {
            if let TransactionState::Committing(_) = transaction.state {
                committing.insert(transaction.domain);
            }
        }
        for (domain, schedule) in &input.runtime_state().schedule.domains {
            if committing.contains(domain) || self.inner.runtime.domain_alter_is_active(domain) {
                continue;
            }
            for (branch, epoch) in &schedule.partition_epochs {
                nervix_primitives::task::consume_budget().await;
                if epoch.transition.is_none() {
                    continue;
                }
                let holdings = match self
                    .branch_transition_holdings(schedule, branch, epoch.number)
                    .await
                {
                    Ok(holdings) => holdings,
                    Err(error) => {
                        debug!(
                            domain = domain.as_str(),
                            branch = branch.as_str(),
                            error = %format_args!("{error:#}"),
                            "a partition transition's holdings are not all known yet"
                        );
                        continue;
                    }
                };
                if holdings.pending != 0 {
                    continue;
                }
                let Some(inputs) = input.planning_inputs(domain).cloned() else {
                    continue;
                };
                let retired = schedule.with_partition_transition_retired(branch);
                match self
                    .inner
                    .consensus
                    .apply_automatic_domain_schedule(input.fence(), inputs, Some(retired))
                    .await
                {
                    Ok(()) => info!(
                        domain = domain.as_str(),
                        branch = branch.as_str(),
                        epoch = epoch.number.get(),
                        claimed = holdings.claimed,
                        "retired a partition transition: every key lies in its partition"
                    ),
                    Err(error) => warn!(
                        domain = domain.as_str(),
                        branch = branch.as_str(),
                        error = %format_args!("{error:#}"),
                        "failed to retire a partition transition; the next pass tries again"
                    ),
                }
                return;
            }
        }
    }
}
