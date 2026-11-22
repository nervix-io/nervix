//! A domain's `START` on the control plane: an admitted `START` command's checks, node-rule
//! placement, resolution and commit, its wait until every live node runs the domain, and its
//! answer.
//!
//! Layer: control plane.
//! - **Owns.** Executing an admitted `START`: refusing a domain that cannot start, placing its
//!   units under their node rules, resolving its start point, paced clock and clock authority,
//!   committing the start in one consensus write, waiting until every live node runs the domain,
//!   and resuming that execution after a leader change from the start revision it recorded.
//! - **Depends on.** The command execution record and its mutation lease, consensus for the domain
//!   record and the start commit, the node-rule placement, the domain clock, the orphaned-pause
//!   recovery and the runtime readiness barrier.
//! - **Must not know.** Transactions, how a client renders the answer, or how a node runs the
//!   domain's work.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use error_stack::Report;
use nervix_consensus::{
    CommandExecution, ConsensusConflict, ConsensusError, DomainMutationLease, DomainStart,
    DomainStartClock, DomainStartControl,
};
use nervix_models::{
    CommandExecutionReference, DomainName, DomainStatus, LicensingStopReason, StartDomain,
};
use nervix_primitives::time::sleep;
use thiserror::Error;

use super::{
    command_result::CommandResult,
    domain_lifecycle::PausedDomainRecovery,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
};
use crate::runtime::RuntimeError;

/// Why an admitted `START` refused to start its domain, or did not see it run.
#[derive(Debug, Error)]
pub(in crate::application) enum DomainStartError {
    #[error("domain '{domain}' does not exist")]
    DomainNotFound { domain: DomainName },
    #[error("cluster parameters have not been seeded")]
    ClusterParametersUnseeded,
    #[error("domain '{domain}' is already running")]
    AlreadyRunning { domain: DomainName },
    #[error("domain '{domain}' is paused by a model alteration")]
    Paused { domain: DomainName },
    #[error("domain '{domain}' is stopping")]
    Stopping { domain: DomainName },
    #[error(
        "domain '{domain}' restore state installation is incomplete for execution '{execution}'"
    )]
    RestoreInstallation {
        domain: DomainName,
        execution: CommandExecutionReference,
    },
    #[error("domain '{domain}' cannot start: licensing stopped processing: {reason}")]
    LicensingStop {
        domain: DomainName,
        reason: LicensingStopReason,
    },
    #[error("domain '{domain}' start generation overflowed")]
    StartGenerationOverflow { domain: DomainName },
    #[error("paced domain '{domain}' cannot start without a live clock voter")]
    ClockAuthority { domain: DomainName },
    /// The timestamp report beneath says why the start point could not be resolved.
    #[error("failed to resolve the start of domain '{domain}'")]
    ResolveStart { domain: DomainName },
    #[error("the START of domain '{domain}' holds no mutation lease of the domain")]
    MutationLost { domain: DomainName },
    #[error("the START of domain '{domain}' committed without recording its revision")]
    StartUnrecorded { domain: DomainName },
    /// The runtime's report beneath says why the domain did not become usable everywhere.
    #[error("started domain '{domain}', but runtime activation did not complete")]
    Activation { domain: DomainName },
}

impl DomainStartError {
    /// The definitive failure an admitted `START` answers with.
    fn answer(report: &Report<Self>) -> CommandResult {
        command_error(format!("{report:#}"))
    }

    /// The definitive failure of an admitted `START` this refusal ends.
    fn refusal<T>(self) -> Result<T, Box<CommandResult>> {
        Err(Box::new(Self::answer(&Report::new(self))))
    }

    /// The answer of a `START` whose domain is committed running but did not become usable
    /// everywhere for a reason other than a node that is not ready yet.
    fn activation_failure(domain: &DomainName, error: Report<RuntimeError>) -> CommandResult {
        let report = error.change_context(Self::Activation {
            domain: domain.clone(),
        });
        Self::answer(&report)
    }
}

/// What the checks before placement found.
enum StartCheck {
    /// The domain is stopped and may start: place its units.
    Startable,
    /// A pause the domain held was recovered, or the domain changed meanwhile: check again.
    Recheck,
}

impl SessionServiceImpl {
    /// Executes the admitted `START` `execution` of its domain with `start`. The execution holds
    /// the domain's mutation lease throughout. It resumes from the start revision its execution
    /// recorded, when it recorded one, and otherwise checks, places and commits the start before it
    /// waits until every live node runs the domain.
    pub(in crate::application) async fn execute_start(
        &self,
        execution: &CommandExecution,
        start: StartDomain,
    ) -> CommandResult {
        let Some(domain) = execution.domain().cloned() else {
            return command_error("no active domain selected".to_string());
        };
        let Some(mutation) = execution.domain_mutation(&domain).cloned() else {
            return DomainStartError::answer(&Report::new(DomainStartError::MutationLost {
                domain,
            }));
        };
        let recorded = self
            .inner
            .consensus
            .current_command_execution(&execution.reference)
            .await
            .and_then(|current| current.domain_start_revision());
        let revision = match recorded {
            Some(revision) => revision,
            None => {
                let committed =
                    Box::pin(self.commit_start(&execution.reference, &domain, &mutation, &start))
                        .await;
                match committed {
                    Ok(revision) => revision,
                    Err(result) => return *result,
                }
            }
        };
        Box::pin(self.wait_for_started_domain(&domain, revision)).await
    }

    /// Checks, places and commits the start of `domain` for the `START` command `reference`, and
    /// returns the revision of that start. A start whose planning inputs changed before it
    /// committed goes back to the checks and places again.
    async fn commit_start(
        &self,
        reference: &CommandExecutionReference,
        domain: &DomainName,
        mutation: &DomainMutationLease,
        start: &StartDomain,
    ) -> Result<u64, Box<CommandResult>> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let control = self.inner.consensus.domain_start_control(domain).await;
            match self.check_start(domain, mutation, &control).await? {
                StartCheck::Startable => {}
                StartCheck::Recheck => continue,
            }
            if let Err(error) = self.place_units_for_start(domain).await {
                return Err(Box::new(self.start_placement_refusal(error).await));
            }
            // Placement may have moved units; the start commits against what it left.
            let inputs = self.inner.consensus.domain_planning_inputs(domain).await;
            let Some(previous) = inputs.state().cloned() else {
                return DomainStartError::DomainNotFound {
                    domain: domain.clone(),
                }
                .refusal();
            };
            if previous.status != DomainStatus::Stopped {
                continue;
            }
            // A paced domain's clock needs an owner before its start is resolved.
            let authority = if previous.config.pace.is_paced() {
                let Some(authority) = self.selected_domain_clock_authority(domain).await else {
                    return DomainStartError::ClockAuthority {
                        domain: domain.clone(),
                    }
                    .refusal();
                };
                Some(authority)
            } else {
                None
            };
            let resolved = match self
                .resolve_domain_start(domain, &previous, &start.start)
                .await
            {
                Ok(resolved) => resolved,
                Err(error) => {
                    let report = error.change_context(DomainStartError::ResolveStart {
                        domain: domain.clone(),
                    });
                    return Err(Box::new(DomainStartError::answer(&report)));
                }
            };
            let clock = match authority {
                Some(authority) => DomainStartClock::Paced {
                    mapping: resolved.clock,
                    authority,
                },
                None => DomainStartClock::Unpaced,
            };
            let committed = self
                .inner
                .consensus
                .start_domain(
                    reference.clone(),
                    inputs,
                    DomainStart {
                        start: resolved.concrete_start,
                        clock,
                    },
                )
                .await;
            if let Err(error) = committed {
                if let ConsensusError::Conflict(ConsensusConflict::DomainPlanningInputs { cause }) =
                    error.current_context()
                {
                    tracing::debug!(
                        domain = domain.as_str(),
                        %cause,
                        "START planning inputs changed before its commit; placing again"
                    );
                    continue;
                }
                return Err(Box::new(
                    self.consensus_report_response(
                        &error,
                        format!("failed to start domain '{}'", domain.as_str()),
                    )
                    .await,
                ));
            }
            let recorded = self
                .inner
                .consensus
                .current_command_execution(reference)
                .await
                .and_then(|current| current.domain_start_revision());
            let Some(revision) = recorded else {
                return DomainStartError::StartUnrecorded {
                    domain: domain.clone(),
                }
                .refusal();
            };
            return Ok(revision);
        }
    }

    /// Refuses a domain that cannot start, in the order the checks have always run: the restore
    /// gate, the licensing gate, the domain's existence and the seeded cluster parameters, then
    /// the domain's own status and start generation. A domain an orphaned alteration left paused
    /// is resumed through the orphaned-pause recovery under the command's lease, and is then
    /// checked again.
    async fn check_start(
        &self,
        domain: &DomainName,
        mutation: &DomainMutationLease,
        control: &DomainStartControl,
    ) -> Result<StartCheck, Box<CommandResult>> {
        if let Some(execution) = &control.restore_installation {
            return DomainStartError::RestoreInstallation {
                domain: domain.clone(),
                execution: execution.clone(),
            }
            .refusal();
        }
        if let Some(reason) = control.licensing_stop {
            return DomainStartError::LicensingStop {
                domain: domain.clone(),
                reason,
            }
            .refusal();
        }
        let Some(state) = control.planning_inputs.state() else {
            return DomainStartError::DomainNotFound {
                domain: domain.clone(),
            }
            .refusal();
        };
        if control.planning_inputs.parameters().is_none() {
            return DomainStartError::ClusterParametersUnseeded.refusal();
        }
        match state.status {
            DomainStatus::Stopped => {}
            DomainStatus::Running => {
                return DomainStartError::AlreadyRunning {
                    domain: domain.clone(),
                }
                .refusal();
            }
            DomainStatus::Stopping => {
                return DomainStartError::Stopping {
                    domain: domain.clone(),
                }
                .refusal();
            }
            DomainStatus::Paused => {
                let recovery = self.recover_paused_domain(domain, Some(mutation)).await?;
                return match recovery {
                    PausedDomainRecovery::Replan => Ok(StartCheck::Recheck),
                    PausedDomainRecovery::RetainRefusal => DomainStartError::Paused {
                        domain: domain.clone(),
                    }
                    .refusal(),
                };
            }
        }
        if state.start_version.checked_add(1).is_none() {
            return DomainStartError::StartGenerationOverflow {
                domain: domain.clone(),
            }
            .refusal();
        }
        Ok(StartCheck::Startable)
    }

    /// Waits until every live node runs `domain` at `revision`, the revision its start committed.
    /// Every failure to reconcile the runtime right after the start is reported to the attached
    /// sessions. A node that is not ready keeps `START` waiting past the readiness deadline: the
    /// revision is applied again and the wait goes on, at the control plane's application retry
    /// interval, while the domain runs on every node that started it. Nothing undoes the start;
    /// any other activation failure answers failed with the domain left running.
    async fn wait_for_started_domain(&self, domain: &DomainName, revision: u64) -> CommandResult {
        if let Err(error) = self.apply_current_cluster_state().await {
            self.broadcast_error(format!(
                "failed to reconcile runtime after starting domain '{}': {error:#}",
                domain.as_str()
            ));
            if !error.current_context().waits_for_readiness() {
                return DomainStartError::activation_failure(domain, error);
            }
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            let leader = self.inner.consensus.current_leader().await;
            if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
                // The start is committed; the next leader resumes this execution and waits for it.
                return self.not_leader_response("", leader.into()).await;
            }
            let Err(error) = self.wait_for_runtime_revision(revision).await else {
                return command_ok(format!("starting domain '{}'", domain.as_str()));
            };
            if !error.current_context().waits_for_readiness() {
                return DomainStartError::activation_failure(domain, error);
            }
            match self.apply_current_cluster_state().await {
                Ok(()) => {
                    if self.wait_for_runtime_revision(revision).await.is_ok() {
                        return command_ok(format!("starting domain '{}'", domain.as_str()));
                    }
                }
                Err(error) if error.current_context().waits_for_readiness() => {}
                Err(error) => return DomainStartError::activation_failure(domain, error),
            }
            sleep(
                self.inner
                    .operational
                    .control_plane
                    .transaction_application_retry_interval(),
            )
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;

    /// A `START` whose domain is committed running but did not become usable everywhere answers
    /// failed, naming the started domain and, beneath it, why activation did not complete.
    #[test]
    fn an_activation_failure_names_the_started_domain_and_its_cause() {
        let domain = DomainName::parse("tenant").assured("the test domain name is valid");
        let answer = DomainStartError::activation_failure(
            &domain,
            Report::new(RuntimeError::ApplyRevision { revision: 3 }),
        );
        assert!(!answer.succeeded());
        assert_eq!(
            answer.message,
            "started domain 'tenant', but runtime activation did not complete: failed to apply \
             runtime revision 3"
        );
    }

    /// A refused `START` answers failed with its reason alone.
    #[test]
    fn a_refused_start_answers_failed_with_its_reason() {
        let domain = DomainName::parse("tenant").assured("the test domain name is valid");
        let Err(answer) = DomainStartError::AlreadyRunning { domain }.refusal::<()>() else {
            panic!("a refusal never answers success");
        };
        assert!(!answer.succeeded());
        assert_eq!(answer.message, "domain 'tenant' is already running");
    }
}
