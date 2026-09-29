//! Planning a restored domain's model run before the domain exists.
//!
//! Layer: control plane.
//!
//! - **Owns.** The captured inputs the transaction planner reads for a domain a restore is about to
//!   create: the domain as the restore creates it, stopped, with no models, the resources it
//!   declares, and exactly the resource versions the restore imports as completed.
//! - **Depends on.** The transaction planner and the planning basis every transaction plan
//!   carries, the current cluster topology, and schedule preparation.
//! - **Must not know.** How the restore read its archive, or how it later applies the plan.
//!
//! The run is planned against what the domain will be once the restore created it and imported
//! its versions, so a plan that succeeds here is the one the restore's models step applies.

use std::collections::BTreeSet;

use error_stack::Report;
use nervix_models::{
    CanonicalImpactSet, DomainState, ImpactNodeCoverage, ModelIndex, OwnershipMoveImpact,
    ResourceName, ResourceUploads, Statement,
};

use super::{TransactionPlanningBasisSource, transaction_planning_basis};
use crate::{
    application::{
        ownership_handoff::planned_ownership_moves, session_service::SessionServiceImpl,
    },
    registry::{
        PlannedTransaction, Registry, TransactionPlanningError, TransactionPlanningSnapshot,
        TransactionScheduleDecision,
    },
};

/// A domain as a restore will create it, which the planner plans a model run against.
pub(in crate::application) struct RestoredDomainInputs<'a> {
    /// The domain as the restore creates it: stopped, with its archived configuration.
    pub(in crate::application) state: &'a DomainState,
    /// Every resource the domain declares.
    pub(in crate::application) resources: &'a BTreeSet<ResourceName>,
    /// Exactly the versions the restore imports, each completed.
    pub(in crate::application) completed: ResourceUploads,
}

impl SessionServiceImpl {
    /// Plans creating `statements`, in order, in the domain `inputs` describes, with the
    /// transaction planner, and applies nothing.
    pub(in crate::application) async fn plan_restored_model_run(
        &self,
        inputs: RestoredDomainInputs<'_>,
        statements: &[Statement],
    ) -> Result<PlannedTransaction, Report<TransactionPlanningError>> {
        let domain = inputs.state.id.clone();
        let control = self
            .inner
            .consensus
            .transaction_control_snapshot(&domain)
            .await;
        let planning_inputs = control
            .planning_inputs
            .after_domain_update(inputs.state.clone(), None);
        let schedule_inputs = self
            .capture_domain_schedule_planning_snapshot(&planning_inputs)
            .await;
        let models = ModelIndex::new();
        let basis = transaction_planning_basis(TransactionPlanningBasisSource {
            domain: inputs.state,
            models: &models,
            resources: inputs.resources,
            resource_uploads: &inputs.completed,
            schedule: None,
            authoritative_inputs: &planning_inputs,
            schedule_inputs: &schedule_inputs,
        })?;
        let snapshot = TransactionPlanningSnapshot {
            domain: inputs.state.clone(),
            models,
            resources: inputs.resources.clone(),
            resource_uploads: inputs.completed,
            schedule: None,
            basis,
            operation_references: Vec::new(),
        };
        Registry::plan_transaction(
            snapshot,
            statements,
            0,
            false,
            |graph, placement, current, attribution| {
                let prepared =
                    schedule_inputs.prepare(&planning_inputs, &domain, graph, placement, current);
                let ownership_moves = planned_ownership_moves(current, prepared.schedule.as_ref())
                    .into_iter()
                    .map(|moved| OwnershipMoveImpact {
                        node: ImpactNodeCoverage::all_executions(moved.entity),
                        source: moved.former_owner,
                        destination: moved.destination,
                        attribution: attribution.clone(),
                    });
                TransactionScheduleDecision {
                    schedule: prepared.schedule,
                    ownership_moves: CanonicalImpactSet::new(ownership_moves),
                }
            },
        )
    }
}
