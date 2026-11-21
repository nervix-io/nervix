//! Coherent metadata reads from the applied immutable publication.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Capturing entity labels, applied revision and committed policy under one read.
//! - **Depends on.** The existing state publication, domain identities and metadata vocabulary.
//! - **Must not know.** Plexus parsing, candidate planning, client framing or running execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns coherent applied metadata snapshots")
)]

use error_stack::{Report, ResultExt as _};
use nervix_models::{
    AdministrativeLabelTarget, CommandExecutionReference, DomainName, DomainState,
    EntityLabelError, EntityLabelLimits, EntityLabelMutation, EntityLabelOwner, EntityLabelPolicy,
    EntityLabelTarget, EntityLabelView, EntityLabels, EntityLabelsInspection, MutateEntityLabels,
    ResourceCatalogKey, ResourceName, ResourceVersionStatus, Statement,
};
use nervix_primitives::sync::{Arc, blocking::RwLock};
use thiserror::Error;

use crate::{
    AppliedConsensusCommand, CommandExecutionEffect, CommandExecutionState, ConsensusCommand,
    ConsensusConflict, ConsensusError, DomainPlanningInputs, Observer, Proposer,
    ReplicatedTransaction, StateMachineChanges, StateMachineData, validate_domain_mutation,
};

#[derive(Debug, Error)]
pub enum EntityLabelReadError {
    #[error("cluster parameters have not been committed")]
    ParametersUnseeded,
    #[error("captured entity labels exceed their committed policy")]
    InvalidLabels,
    #[error("entity {entity} does not exist")]
    UnknownEntity { entity: EntityLabelOwner },
    #[error("transaction {id:?} is not attached to domain {domain}")]
    UnknownTransaction { id: String, domain: DomainName },
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Error,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub enum EntityLabelWriteError {
    #[error("cluster parameters have not been committed")]
    ParametersUnseeded,
    #[error("entity {target} does not exist")]
    UnknownEntity { target: AdministrativeLabelTarget },
    #[error("the candidate label map was refused")]
    InvalidLabels { issue: EntityLabelError },
    #[error("the label edit has no admitted execution")]
    ExecutionMissing,
}

impl EntityLabelWriteError {
    fn from_invalid_labels(report: Report<EntityLabelError>) -> Report<Self> {
        let issue = report.current_context().clone();
        report.change_context(Self::InvalidLabels { issue })
    }

    pub(super) fn rejection_reason(&self) -> &dyn std::fmt::Display {
        match self {
            Self::InvalidLabels { issue } => issue,
            other => other,
        }
    }
}

impl StateMachineData {
    pub(crate) fn entity_label_sets(&self) -> impl Iterator<Item = &EntityLabels> {
        let models = self
            .schedule
            .domains
            .values()
            .flat_map(|schedule| schedule.nodes.values())
            .map(|node| node.config.labels());
        let domains = self.domains.values().map(|record| &record.labels);
        let users = self.users.values().map(|record| &record.labels);
        let resources = self.resources.catalog.values().map(|record| &record.labels);
        models.chain(domains).chain(users).chain(resources)
    }

    pub(crate) fn validate_label_set(
        &self,
        labels: &EntityLabels,
    ) -> error_stack::Result<(), EntityLabelWriteError> {
        if labels.is_empty() {
            return Ok(());
        }
        let limits = self.entity_label_limits()?;
        labels
            .validate(&limits)
            .map_err(EntityLabelWriteError::from_invalid_labels)
    }

    fn entity_label_limits(&self) -> error_stack::Result<EntityLabelLimits, EntityLabelWriteError> {
        let parameters = self
            .parameters
            .as_ref()
            .ok_or_else(|| Report::new(EntityLabelWriteError::ParametersUnseeded))?;
        Ok(EntityLabelLimits::from(&parameters.values))
    }

    fn named_labels_mut(
        &mut self,
        target: &AdministrativeLabelTarget,
    ) -> Option<&mut EntityLabels> {
        match target {
            AdministrativeLabelTarget::Domain(name) => {
                self.domains.get_mut(name).map(|record| &mut record.labels)
            }
            AdministrativeLabelTarget::User(name) => {
                self.users.get_mut(name).map(|record| &mut record.labels)
            }
        }
    }

    fn edit_administrative_labels(
        &mut self,
        target: &AdministrativeLabelTarget,
        mutation: &EntityLabelMutation,
    ) -> error_stack::Result<(), EntityLabelWriteError> {
        let limits = self.entity_label_limits()?;
        let labels = self.named_labels_mut(target).ok_or_else(|| {
            Report::new(EntityLabelWriteError::UnknownEntity {
                target: target.clone(),
            })
        })?;
        let updated = labels
            .updated(mutation, &limits)
            .map_err(EntityLabelWriteError::from_invalid_labels)?;
        *labels = updated;
        Ok(())
    }
}

pub(super) fn apply_administrative_labels(
    state: &mut StateMachineData,
    reference: &CommandExecutionReference,
) -> AppliedConsensusCommand {
    let Some(mut execution) = state.command_executions.get(reference).cloned() else {
        return write_conflict(EntityLabelWriteError::ExecutionMissing);
    };
    if let Some(CommandExecutionEffect::EditAdministrativeLabels {
        target: AdministrativeLabelTarget::Domain(domain),
        outcome: None,
        ..
    }) = execution.effect()
    {
        let lease = execution.domain_mutation(domain);
        if let Err(reason) = validate_domain_mutation(state, domain, lease) {
            return AppliedConsensusCommand::conflict(ConsensusConflict::DomainMutation {
                cause: Box::new(reason.current_context().clone()),
            });
        }
    }
    let CommandExecutionState::Applying { effect, .. } = &mut execution.state else {
        return write_conflict(EntityLabelWriteError::ExecutionMissing);
    };
    let CommandExecutionEffect::EditAdministrativeLabels {
        target,
        mutation,
        outcome,
    } = effect.as_mut()
    else {
        return write_conflict(EntityLabelWriteError::ExecutionMissing);
    };
    if let Some(result) = outcome {
        return match result {
            Ok(()) => AppliedConsensusCommand::applied(StateMachineChanges::default()),
            Err(error) => write_conflict(error.clone()),
        };
    }
    let result = state
        .edit_administrative_labels(target, mutation)
        .map_err(|report| report.current_context().clone());
    // Apply the metadata and record the exact outcome in the same state-machine write. An
    // earlier request replay must leave any later edit in place.
    *outcome = Some(result.clone());
    state.command_executions.replace(execution);
    match result {
        Ok(()) => AppliedConsensusCommand::applied(StateMachineChanges::default()),
        Err(error) => write_conflict(error),
    }
}

fn write_conflict(issue: EntityLabelWriteError) -> AppliedConsensusCommand {
    AppliedConsensusCommand::conflict(ConsensusConflict::EntityLabels {
        issue: Box::new(issue),
    })
}

pub(super) fn matches_resource_edit(
    statement: &Statement,
    inputs: &DomainPlanningInputs,
    identifier: &ResourceName,
    labels: &EntityLabels,
) -> bool {
    let Statement::MutateEntityLabels(MutateEntityLabels {
        entity: EntityLabelTarget::Resource(resource),
        mutation,
    }) = statement
    else {
        return false;
    };
    if resource != identifier {
        return false;
    }
    let Some(previous) = inputs.resources().catalog().get(resource) else {
        return false;
    };
    let Some(parameters) = inputs.parameters() else {
        return false;
    };
    let limits = EntityLabelLimits::from(&parameters.values);
    match previous.updated(mutation, &limits) {
        Ok(expected) => &expected == labels,
        Err(_) => false,
    }
}

/// One domain's schedule and accepted transaction from the same policy publication.
#[derive(Debug, Clone)]
pub struct EntityLabelTransactionSnapshot {
    pub control: crate::TransactionControlSnapshot,
    pub transaction: Arc<ReplicatedTransaction>,
    pub policy: EntityLabelPolicy,
    pub revision: u64,
}

fn policy(state: &StateMachineData) -> Result<EntityLabelPolicy, Report<EntityLabelReadError>> {
    let record = state
        .parameters
        .as_ref()
        .ok_or_else(|| Report::new(EntityLabelReadError::ParametersUnseeded))?;
    Ok(EntityLabelPolicy {
        revision: record.revision,
        limits: EntityLabelLimits::from(&record.values),
    })
}

fn revision(state: &StateMachineData) -> u64 {
    match &state.last_applied_log_id {
        Some(log_id) => log_id.index,
        None => 0,
    }
}

fn capture_labels(
    applied: &RwLock<StateMachineData>,
    entity: &EntityLabelOwner,
) -> Result<EntityLabelsInspection, Report<EntityLabelReadError>> {
    inspect_labels(&applied.read(), entity)
}

/// Only the requested map is copied while the state publication is held.
fn inspect_labels(
    state: &StateMachineData,
    entity: &EntityLabelOwner,
) -> Result<EntityLabelsInspection, Report<EntityLabelReadError>> {
    let policy = policy(state)?;
    let labels = match entity {
        EntityLabelOwner::Model(entity) => {
            let node = state
                .schedule
                .domain(&entity.domain)
                .and_then(|schedule| schedule.nodes.get(&entity.node));
            node.map(|node| node.config.labels())
        }
        EntityLabelOwner::Domain { name } => state.domains.get(name).map(|record| &record.labels),
        EntityLabelOwner::Resource(resource) => state
            .resources
            .catalog
            .get(resource)
            .map(|record| &record.labels),
        EntityLabelOwner::User { name } => state.users.get(name).map(|record| &record.labels),
    }
    .ok_or_else(|| {
        Report::new(EntityLabelReadError::UnknownEntity {
            entity: entity.clone(),
        })
    })?;
    EntityLabelsInspection::new(
        entity.clone(),
        labels.clone(),
        EntityLabelView::Committed {
            revision: revision(state),
        },
        policy,
    )
    .change_context(EntityLabelReadError::InvalidLabels)
}

fn capture_transaction(
    applied: &RwLock<StateMachineData>,
    domain: &DomainName,
    id: &str,
) -> Result<EntityLabelTransactionSnapshot, Report<EntityLabelReadError>> {
    let state = applied.read().clone();
    let policy = policy(&state)?;
    let transaction = match state.transactions.get_shared(id) {
        Some(transaction) if transaction.domain == *domain => transaction,
        _ => {
            return Err(Report::new(EntityLabelReadError::UnknownTransaction {
                id: id.to_owned(),
                domain: domain.clone(),
            }));
        }
    };
    Ok(EntityLabelTransactionSnapshot {
        control: state.transaction_control(domain),
        transaction,
        policy,
        revision: revision(&state),
    })
}

impl Observer {
    /// Describes a domain and its label policy at the same applied revision.
    pub fn domain_with_labels(
        &self,
        domain: &DomainName,
    ) -> Result<(DomainState, EntityLabelsInspection), Report<EntityLabelReadError>> {
        let state = self.inner.store.inner.state();
        let owner = EntityLabelOwner::Domain {
            name: domain.clone(),
        };
        let record = state.domains.get(domain).ok_or_else(|| {
            Report::new(EntityLabelReadError::UnknownEntity {
                entity: owner.clone(),
            })
        })?;
        Ok((record.clone(), inspect_labels(&state, &owner)?))
    }

    /// Captures the resource catalog and logical resource labels from one applied publication.
    pub fn resources_with_labels(
        &self,
        domain: &DomainName,
        identifier: &ResourceName,
    ) -> Result<(ResourceVersionStatus, EntityLabelsInspection), Report<EntityLabelReadError>> {
        let state = self.inner.store.inner.state();
        let owner = EntityLabelOwner::Resource(ResourceCatalogKey::new(domain, identifier));
        let labels = inspect_labels(&state, &owner)?;
        Ok(((&state.resources).into(), labels))
    }

    pub fn entity_labels(
        &self,
        entity: &EntityLabelOwner,
    ) -> Result<EntityLabelsInspection, Report<EntityLabelReadError>> {
        capture_labels(&self.inner.store.inner.state_machine, entity)
    }
    pub fn entity_label_transaction(
        &self,
        domain: &DomainName,
        id: &str,
    ) -> Result<EntityLabelTransactionSnapshot, Report<EntityLabelReadError>> {
        capture_transaction(&self.inner.store.inner.state_machine, domain, id)
    }
}

impl Proposer {
    pub async fn edit_administrative_labels(
        &self,
        reference: CommandExecutionReference,
    ) -> error_stack::Result<(), ConsensusError> {
        let response = self
            .inner
            .client_write(ConsensusCommand::EditAdministrativeLabels { reference })
            .await?;
        Self::applied_response(response.data)
    }
}

#[cfg(test)]
mod tests;
