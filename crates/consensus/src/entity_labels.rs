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
    DomainName, DomainNodeRef, EntityLabelLimits, EntityLabelPolicy, EntityLabelView,
    EntityLabelsInspection,
};
use nervix_primitives::sync::{Arc, blocking::RwLock};
use thiserror::Error;

use crate::{Observer, ReplicatedTransaction, StateMachineData};

#[derive(Debug, Error)]
pub enum EntityLabelReadError {
    #[error("cluster parameters have not been committed")]
    ParametersUnseeded,
    #[error("captured entity labels exceed their committed policy")]
    InvalidLabels,
    #[error("entity {entity:?} does not exist")]
    UnknownEntity { entity: DomainNodeRef },
    #[error("transaction {id:?} is not attached to domain {domain}")]
    UnknownTransaction { id: String, domain: DomainName },
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

/// Only the requested map is copied while the state publication is held.
fn capture_labels(
    applied: &RwLock<StateMachineData>,
    entity: &DomainNodeRef,
) -> Result<EntityLabelsInspection, Report<EntityLabelReadError>> {
    let state = applied.read();
    let policy = policy(&state)?;
    let node = match state.schedule.domain(&entity.domain) {
        Some(schedule) => schedule.nodes.get(&entity.node),
        None => None,
    };
    let node = node.ok_or_else(|| {
        Report::new(EntityLabelReadError::UnknownEntity {
            entity: entity.clone(),
        })
    })?;
    let labels = node.config.labels().clone();
    EntityLabelsInspection::new(
        entity.clone(),
        labels,
        EntityLabelView::Committed {
            revision: revision(&state),
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
        control: crate::TransactionControlSnapshot {
            planning_inputs: state.domain_planning_inputs(domain),
            resources: (&state.resources).into(),
            restore_installation: state
                .domain_restore_installations
                .get(domain)
                .map(|installation| installation.execution().clone()),
        },
        transaction,
        policy,
        revision: revision(&state),
    })
}

impl Observer {
    pub fn entity_labels(
        &self,
        entity: &DomainNodeRef,
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

#[cfg(test)]
mod tests;
