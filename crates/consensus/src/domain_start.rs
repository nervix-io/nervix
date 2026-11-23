//! The start of a stopped domain, which one `START` command commits on its own.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The `StartDomain` proposal and its apply: the `START` execution and the mutation
//!   lease it holds, the restore start gate, the licensing stop gate and the planning inputs the
//!   start was placed against, all checked in the one applied revision that starts the domain and
//!   records that revision in the execution; and the read a `START` checks before it places.
//! - **Depends on.** The applied consensus records, the command execution records and the domain
//!   lifecycle record.
//! - **Must not know.** Sessions, node-rule placement, clock authority selection or runtime
//!   activation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the atomic start of one domain for its START command"
    )
)]

use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_models::{
    ClusterNodeIdentity, CommandExecutionReference, DomainClockState, DomainName, DomainStartPoint,
    DomainStatus, LicensingStopReason,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    AppliedConsensusCommand, CommandExecutionEffect, ConsensusCommand, ConsensusConflict,
    ConsensusError, DomainPlanningInputs, Observer, Proposer, StateMachineChanges,
    StateMachineData, restore, validate_domain_mutation, validate_domain_planning_inputs,
};

/// The start a `START` command resolved for its stopped domain.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct DomainStart {
    /// Where the domain starts, with a requested `NOW` resolved to the instant it began at.
    pub start: DomainStartPoint,
    pub clock: DomainStartClock,
}

/// The clock a started domain runs on, which its pace decides.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum DomainStartClock {
    /// An unpaced domain reads actual UTC; it has no mapping and no authority.
    Unpaced,
    /// A paced domain begins with `mapping`, and the node process `authority` produces its
    /// progress.
    Paced {
        mapping: DomainClockState,
        authority: ClusterNodeIdentity,
    },
}

impl DomainStartClock {
    fn is_paced(&self) -> bool {
        matches!(self, Self::Paced { .. })
    }
}

/// Why consensus refused to start a domain for a `START` command. A refusal of the gates, the
/// mutation lease or the planning inputs is its own [`ConsensusConflict`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    Error,
)]
pub enum DomainStartRefusal {
    #[error("command execution reference '{reference}' starts no domain '{domain}'")]
    NotAStart {
        reference: CommandExecutionReference,
        domain: DomainName,
    },
    #[error("the START of domain '{domain}' holds no mutation lease of the domain")]
    WithoutMutationLease { domain: DomainName },
    #[error("domain '{domain}' does not exist")]
    MissingDomain { domain: DomainName },
    #[error("domain '{domain}' is not stopped")]
    NotStopped { domain: DomainName },
    #[error("the start of domain '{domain}' does not match the domain's pace")]
    PaceMismatch { domain: DomainName },
}

/// What a `START` of one domain checks before it places the domain's units, read from one applied
/// revision.
#[derive(Debug, Clone)]
pub struct DomainStartControl {
    pub planning_inputs: DomainPlanningInputs,
    /// The restore whose incomplete state installation prevents this domain from starting.
    pub restore_installation: Option<CommandExecutionReference>,
    /// The closed licensing stop gate, which prevents every domain from starting.
    pub licensing_stop: Option<LicensingStopReason>,
}

impl Observer {
    /// What a `START` of `domain` checks before it places the domain's units.
    pub async fn domain_start_control(&self, domain: &DomainName) -> DomainStartControl {
        self.inner.store.inner.state().domain_start_control(domain)
    }
}

impl Proposer {
    /// Starts the stopped domain of the applying `START` command `reference` with `start`, which
    /// that command resolved against `inputs`, and records the revision of the start in the
    /// command's execution. A repeated proposal for a command that already recorded its start
    /// applies nothing.
    pub async fn start_domain(
        &self,
        reference: CommandExecutionReference,
        inputs: DomainPlanningInputs,
        start: DomainStart,
    ) -> Result<(), Report<ConsensusError>> {
        let response = self
            .inner
            .client_write(ConsensusCommand::StartDomain {
                reference,
                inputs: Box::new(inputs),
                start: Box::new(start),
            })
            .await?;
        Self::applied_response(response.data)
    }
}

impl StateMachineData {
    fn domain_start_control(&self, domain: &DomainName) -> DomainStartControl {
        DomainStartControl {
            planning_inputs: self.domain_planning_inputs(domain),
            restore_installation: self
                .domain_restore_installations
                .get(domain)
                .map(|installation| installation.execution().clone()),
            licensing_stop: self.licensing_stop(),
        }
    }

    /// Applies the start of `inputs`' domain for the `START` command `reference`, at the revision
    /// of the entry that carries it.
    pub(super) fn start_domain(
        &mut self,
        reference: &CommandExecutionReference,
        inputs: &DomainPlanningInputs,
        start: &DomainStart,
    ) -> AppliedConsensusCommand {
        let domain = inputs.domain();
        let not_a_start = || {
            refused(DomainStartRefusal::NotAStart {
                reference: reference.clone(),
                domain: domain.clone(),
            })
        };
        let Some(mut execution) = self.command_executions.get(reference).cloned() else {
            return not_a_start();
        };
        if execution.domain() != Some(domain) {
            return not_a_start();
        }
        let Some(CommandExecutionEffect::StartDomain { started_at, .. }) = execution.effect()
        else {
            return not_a_start();
        };
        if started_at.is_some() {
            // An earlier proposal of this same start applied and recorded its revision.
            return AppliedConsensusCommand::applied(StateMachineChanges::default());
        }
        let Some(lease) = execution.domain_mutation(domain) else {
            return refused(DomainStartRefusal::WithoutMutationLease {
                domain: domain.clone(),
            });
        };
        if let Err(error) = validate_domain_mutation(self, domain, Some(lease)) {
            return AppliedConsensusCommand::conflict(ConsensusConflict::DomainMutation {
                cause: Box::new(error.current_context().clone()),
            });
        }
        if let Err(error) = restore::validate_restored_domain_start(self, domain) {
            return AppliedConsensusCommand::conflict(ConsensusConflict::RestoreInstallation {
                cause: Box::new(error.current_context().clone()),
            });
        }
        if let Some(reason) = self.licensing_stop() {
            return AppliedConsensusCommand::conflict(ConsensusConflict::LicensingStop { reason });
        }
        if let Err(error) = validate_domain_planning_inputs(self, inputs) {
            return AppliedConsensusCommand::conflict(ConsensusConflict::DomainPlanningInputs {
                cause: error.current_context().clone(),
            });
        }
        // The planning inputs are the current ones, so the captured state is the domain's own.
        let Some(state) = inputs.state() else {
            return refused(DomainStartRefusal::MissingDomain {
                domain: domain.clone(),
            });
        };
        if state.status != DomainStatus::Stopped {
            return refused(DomainStartRefusal::NotStopped {
                domain: domain.clone(),
            });
        }
        if state.config.pace.is_paced() != start.clock.is_paced() {
            return refused(DomainStartRefusal::PaceMismatch {
                domain: domain.clone(),
            });
        }
        let revision = match &self.last_applied_log_id {
            Some(log_id) => log_id.index,
            None => 0,
        };
        let changes = StateMachineChanges {
            domains_changed: self.commit_domain_start(domain, start),
            ..StateMachineChanges::default()
        };
        execution.record_domain_start(revision);
        self.command_executions.replace(execution);
        AppliedConsensusCommand::applied(changes)
    }

    /// Marks `domain_id` running at its next start version with `start`, clearing its stop, and
    /// assigns a paced domain's clock authority. Returns whether the domain changed.
    fn commit_domain_start(&mut self, domain_id: &DomainName, start: &DomainStart) -> bool {
        let Some(domain) = self.domains.get_mut(domain_id) else {
            return false;
        };
        domain.status = DomainStatus::Running;
        domain.clear_stop();
        domain.start_version = domain
            .start_version
            .checked_add(1)
            .assured("a domain cannot be started 2^64 times in the lifetime of a cluster");
        domain.last_start = start.start.clone();
        match &start.clock {
            DomainStartClock::Unpaced => {
                domain.clock = None;
                self.domain_clock_authorities.remove(domain_id);
            }
            DomainStartClock::Paced { mapping, authority } => {
                domain.clock = Some(mapping.clone());
                self.advance_domain_clock_authority(domain_id, Some(authority.clone()));
            }
        }
        true
    }
}

fn refused(refusal: DomainStartRefusal) -> AppliedConsensusCommand {
    AppliedConsensusCommand::conflict(ConsensusConflict::DomainStart { refusal })
}

#[cfg(test)]
mod tests;
