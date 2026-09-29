//! Applying one step of a restore.
//!
//! Layer: control plane.
//!
//! - **Owns.** Applying each step of a planned restore and recording it: importing the users,
//!   creating a domain stopped with its declared resources and making it usable on every node,
//!   importing each completed resource version under its archived number, and applying the domain's
//!   models as one batch under the lease the restore holds on the domain.
//! - **Depends on.** Consensus restore steps, resource version imports, the direct model batch, and
//!   the planned restore.
//! - **Must not know.** The archive format, the transport, or how the restore was planned.
//!
//! A step whose consensus record exists is never applied again; a step's own effects are either
//! recorded with it in one consensus command, or idempotent under the identity the restore gives
//! them, so a step an earlier attempt applied without recording is completed rather than repeated.

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_consensus::{CommandExecution, ConsensusError, RestoreStepEffect};
use nervix_models::{
    DomainName, ResourceId, ResourceUploadIdentity, ResourceUploadKey, RestoreStep, UserName,
};

use super::{
    prepare::{VerifiedArchive, create_statements},
    runner::{RestoreSteps, StepFailure},
};
use crate::{
    application::{resource::ImportedResourceVersion, session_service::SessionServiceImpl},
    registry::{PlannedDomain, RestorePlan},
};

/// The steps of one admitted restore, taking effect in consensus and the stores this leader
/// writes.
pub(super) struct ServerRestoreSteps<'a> {
    pub(super) service: &'a SessionServiceImpl,
    pub(super) execution: &'a CommandExecution,
    pub(super) plan: &'a RestorePlan,
    pub(super) archive: &'a VerifiedArchive,
}

impl RestoreSteps for ServerRestoreSteps<'_> {
    async fn apply(&self, step: &RestoreStep) -> Result<(), StepFailure> {
        #[cfg(feature = "testing")]
        self.service
            .inner
            .runtime
            .pause_restore_step_if_armed(self.service.inner.consensus.local_node_id(), step)
            .await;
        self.service
            .apply_restore_step(self.execution, self.plan, step, self.archive)
            .await
    }
}

/// The upload identity a restore imports `id` under for `owner`. It is derived from the version's
/// number alone, so every attempt of one restore imports a version under the same identity.
pub(in crate::application) fn restored_upload_key(
    owner: &UserName,
    id: &ResourceId,
) -> ResourceUploadKey {
    let identity = ResourceUploadIdentity::parse(format!("restore-v{}", id.version))
        .assured("'restore-v' and decimal digits satisfy the upload identity grammar");
    ResourceUploadKey::new(
        owner.clone(),
        id.domain.clone(),
        id.identifier.clone(),
        identity,
    )
}

/// The planned domain restored under `target`.
fn planned_domain<'plan>(
    plan: &'plan RestorePlan,
    target: &DomainName,
) -> Result<&'plan PlannedDomain, StepFailure> {
    let Some(domain) = plan.domains.get(target) else {
        return Err(StepFailure::Failed(format!(
            "the restore's plan holds no domain '{target}'"
        )));
    };
    Ok(domain)
}

impl SessionServiceImpl {
    /// Applies `step` of the restore `execution` records, and records it.
    async fn apply_restore_step(
        &self,
        execution: &CommandExecution,
        plan: &RestorePlan,
        step: &RestoreStep,
        archive: &VerifiedArchive,
    ) -> Result<(), StepFailure> {
        match step {
            RestoreStep::Users => {
                let Some(users) = &plan.users else {
                    return Err(StepFailure::Failed(
                        "the restore's plan imports no users".to_string(),
                    ));
                };
                let effect = RestoreStepEffect::Users {
                    users: users.users.clone(),
                    policy: users.policy,
                };
                self.record_restore_step(execution, step, effect).await
            }
            RestoreStep::CreateDomain(target) => {
                let domain = planned_domain(plan, target)?;
                let effect = RestoreStepEffect::Domain {
                    state: Box::new(domain.state.clone()),
                    resources: domain.resources.clone(),
                };
                self.record_restore_step(execution, step, effect).await?;
                self.make_restored_domain_usable(target).await
            }
            RestoreStep::ImportResources(target) => {
                let domain = planned_domain(plan, target)?;
                self.import_restored_versions(execution, domain, archive)
                    .await?;
                self.record_restore_step(execution, step, RestoreStepEffect::Completion)
                    .await
            }
            RestoreStep::ApplyModels(target) => {
                let domain = planned_domain(plan, target)?;
                self.apply_restored_models(execution, domain, archive)
                    .await?;
                self.record_restore_step(execution, step, RestoreStepEffect::Completion)
                    .await
            }
        }
    }

    /// Records `step` with `effect` in consensus, which applies the effect unless the step is
    /// already recorded.
    async fn record_restore_step(
        &self,
        execution: &CommandExecution,
        step: &RestoreStep,
        effect: RestoreStepEffect,
    ) -> Result<(), StepFailure> {
        match self
            .inner
            .consensus
            .apply_restore_step(execution.reference.clone(), step.clone(), effect)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => Err(self.consensus_step_failure(&error).await),
        }
    }

    /// The failure a consensus refusal of a restore step stands for.
    async fn consensus_step_failure(&self, error: &Report<ConsensusError>) -> StepFailure {
        if let ConsensusError::LeadershipLost { leader_id } = error.current_context() {
            let redirect = self.not_leader_response("", leader_id.clone()).await;
            return StepFailure::LeadershipLost(Box::new(redirect));
        }
        StepFailure::Failed(format!("{error:#}"))
    }

    /// Makes a domain the restore created usable in its stopped state on every live node.
    async fn make_restored_domain_usable(&self, domain: &DomainName) -> Result<(), StepFailure> {
        if let Err(error) = self.apply_current_cluster_state().await {
            return Err(StepFailure::Failed(format!(
                "domain '{domain}' was created, but its stopped state did not become usable: \
                 {error}"
            )));
        }
        if let Err(error) = self.wait_for_authoritative_visibility().await {
            return Err(StepFailure::Failed(format!(
                "domain '{domain}' was created, but authoritative visibility did not complete: \
                 {error}"
            )));
        }
        Ok(())
    }

    /// Imports every completed version of `domain` under its archived number, each installed from
    /// its section of the staged archive and completed on every live node.
    async fn import_restored_versions(
        &self,
        execution: &CommandExecution,
        domain: &PlannedDomain,
        archive: &VerifiedArchive,
    ) -> Result<(), StepFailure> {
        let owner = execution
            .owner()
            .verified("an applying execution retains its owner");
        for version in &domain.versions {
            tokio::task::consume_budget().await;
            let id = &version.resource.id;
            let import = ImportedResourceVersion {
                restore: &execution.reference,
                key: restored_upload_key(owner, id),
                resource: version.resource.clone(),
                archive_path: archive.path(),
                offset: version.archive.offset,
            };
            if let Err(error) = self.install_imported_resource_version(import).await {
                if let Some(ConsensusError::LeadershipLost { leader_id }) =
                    error.downcast_ref::<ConsensusError>()
                {
                    let redirect = self.not_leader_response("", leader_id.clone()).await;
                    return Err(StepFailure::LeadershipLost(Box::new(redirect)));
                }
                return Err(StepFailure::Failed(format!(
                    "version {} of resource '{}' was not imported: {error:#}",
                    id.version, id.identifier
                )));
            }
        }
        Ok(())
    }

    /// Applies `domain`'s models as one batch, unless an earlier attempt of the restore already
    /// applied exactly them. The restore holds the domain's mutation lease from its admission, so
    /// no other command changed the domain's models since.
    async fn apply_restored_models(
        &self,
        execution: &CommandExecution,
        domain: &PlannedDomain,
        archive: &VerifiedArchive,
    ) -> Result<(), StepFailure> {
        let current = self
            .inner
            .registry
            .transaction_planning_models(&domain.target);
        if !current.is_empty() {
            let mut applied = current.len() == domain.models.len();
            for model in &domain.models {
                if current.get(&model.node_ref()) != Some(model) {
                    applied = false;
                }
            }
            if applied {
                return Ok(());
            }
            return Err(StepFailure::Failed(format!(
                "domain '{}' holds models the restore did not apply",
                domain.target
            )));
        }
        if domain.models.is_empty() {
            return Ok(());
        }
        let Some(lease) = execution.domain_mutation(&domain.target) else {
            return Err(StepFailure::Failed(format!(
                "the restore holds no mutation lease for domain '{}'",
                domain.target
            )));
        };
        let result = self
            .process_restored_model_batch(
                create_statements(domain),
                archive.models_text(&domain.source),
                &domain.target,
                lease,
            )
            .await;
        if result.is_not_leader() {
            return Err(StepFailure::LeadershipLost(Box::new(result)));
        }
        if !result.succeeded() {
            return Err(StepFailure::Failed(result.message));
        }
        Ok(())
    }
}
