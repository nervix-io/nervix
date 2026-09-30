//! Applying one step of a restore.
//!
//! Layer: control plane.
//!
//! - **Owns.** Applying each step of a planned restore and recording it: importing the users,
//!   creating a domain stopped with its declared resources and making it usable on every node,
//!   importing each completed resource version under its archived number, and applying the domain's
//!   models as one batch under the lease the restore holds on the domain, then installing compatible
//!   archived runtime checkpoints under the restored schedule.
//! - **Depends on.** Consensus restore steps, resource version imports, the direct model batch,
//!   the verified archive description, and the planned restore.
//! - **Must not know.** Archive record encoding, client transport, or how the restore was planned.
//!
//! A step whose consensus record exists is never applied again; a step's own effects are either
//! recorded with it in one consensus command, or idempotent under the identity the restore gives
//! them, so a step an earlier attempt applied without recording is completed rather than repeated.

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_backup::DescribedRuntimeState;
use nervix_consensus::{CommandExecution, ConsensusError, RestoreStepEffect};
use nervix_interconnect::{RuntimeState, StatePlacementEnvelope};
use nervix_models::{
    ClusterNodeName, CoordinationIdentity, DomainName, ModelKind, NodeRef, ResourceId,
    ResourceUploadIdentity, ResourceUploadKey, RestoreState, RestoreStep, UserName,
};

use super::{
    prepare::{VerifiedArchive, create_statements},
    runner::{RestoreSteps, StepFailure},
};
use crate::{
    application::{
        backup::interconnect::{InstallRestoredStateAction, InstallRestoredStateRequest},
        resource::ImportedResourceVersion,
        session_service::SessionServiceImpl,
    },
    registry::{PlannedDomain, RestorePlan},
    runtime::{
        BackupBranchLifecycleEntry, CapturedRuntimeState, encode_restored_branch_lifecycle,
        encode_restored_kafka_offsets,
    },
};

/// The steps of one admitted restore, taking effect in consensus and the stores this leader
/// writes.
pub(super) struct ServerRestoreSteps<'a> {
    pub(super) service: &'a SessionServiceImpl,
    pub(super) execution: &'a CommandExecution,
    pub(super) plan: &'a RestorePlan,
    pub(super) archive: &'a VerifiedArchive,
    pub(super) state: RestoreState,
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
            .apply_restore_step(self.execution, self.plan, step, self.archive, self.state)
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
        state: RestoreState,
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
                self.install_restored_state(domain, archive, state).await?;
                self.record_restore_step(execution, step, RestoreStepEffect::Completion)
                    .await
            }
        }
    }

    /// Installs state only after models have published their new stopped-domain schedule. A
    /// retry of the unrecorded model step repeats this idempotently after purging the target.
    async fn install_restored_state(
        &self,
        domain: &PlannedDomain,
        archive: &VerifiedArchive,
        state: RestoreState,
    ) -> Result<(), StepFailure> {
        let coordination = self
            .inner
            .interconnect
            .next_coordination_identity()
            .map_err(|error| {
                StepFailure::Failed(format!(
                    "failed to identify restore state installation: {error}"
                ))
            })?;
        self.purge_restored_state_everywhere(&domain.target, &coordination)
            .await?;
        if state == RestoreState::ConfigurationOnly {
            return Ok(());
        }
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(scheduled) = schedule.domain(&domain.target) else {
            return Err(StepFailure::Failed(format!(
                "restored domain '{}' has no published schedule",
                domain.target
            )));
        };
        // Lifecycle is installed first: a WASM branch save cannot be admitted into a branch
        // incarnation the owner has not learned exists.
        for first_lifecycle in [true, false] {
            for archived in archive.states_for(&domain.source) {
                let is_lifecycle =
                    matches!(archived, DescribedRuntimeState::BranchLifecycle { .. });
                if is_lifecycle != first_lifecycle {
                    continue;
                }
                let (
                    kind,
                    entity,
                    schema,
                    branch_fingerprint,
                    branch_key,
                    runtime_state,
                    revision,
                    payload,
                ) = match archived {
                    DescribedRuntimeState::BranchLifecycle { lifecycle, .. } => {
                        let payload = encode_restored_branch_lifecycle(
                            lifecycle
                                .branches
                                .iter()
                                .map(|entry| BackupBranchLifecycleEntry {
                                    key: entry.key.clone().map(|fields| {
                                        fields
                                            .into_iter()
                                            .map(|field| field.into_remote())
                                            .collect()
                                    }),
                                    last_ingestion: entry.last_ingestion,
                                    incarnation: entry.incarnation,
                                })
                                .collect(),
                            &lifecycle.entity,
                        )
                        .map_err(|error| StepFailure::Failed(error.to_string()))?;
                        (
                            lifecycle.owner_kind,
                            lifecycle.entity.clone(),
                            Some(lifecycle.schema),
                            None,
                            None,
                            RuntimeState::BranchLru {
                                schema: lifecycle.schema,
                            },
                            lifecycle.revision,
                            payload,
                        )
                    }
                    DescribedRuntimeState::KafkaOffsets { offsets, .. } => {
                        if state == RestoreState::WithoutSourceOffsets {
                            continue;
                        }
                        let payload = encode_restored_kafka_offsets(
                            offsets
                                .offsets
                                .iter()
                                .map(|entry| {
                                    (entry.topic.clone(), entry.partition, entry.next_offset)
                                })
                                .collect(),
                        )
                        .map_err(|error| StepFailure::Failed(error.to_string()))?;
                        (
                            ModelKind::Ingestor,
                            offsets.entity.clone(),
                            Some(offsets.schema),
                            None,
                            None,
                            RuntimeState::KafkaOffset,
                            offsets.revision,
                            payload,
                        )
                    }
                    DescribedRuntimeState::Wasm {
                        descriptor, guest, ..
                    } => {
                        let payload = archive
                            .read_guest_blob(&self.inner.runtime, guest)
                            .await
                            .map_err(|error| StepFailure::Failed(error.to_string()))?;
                        (
                            ModelKind::WasmProcessor,
                            descriptor.entity.clone(),
                            Some(descriptor.schema),
                            descriptor.branch_fingerprint,
                            descriptor.branch.clone().map(|fields| {
                                fields
                                    .into_iter()
                                    .map(|field| field.into_remote())
                                    .collect()
                            }),
                            RuntimeState::WasmProcessor {
                                schema: descriptor.schema,
                                generation: descriptor.generation,
                            },
                            descriptor.revision,
                            payload,
                        )
                    }
                };
                let Some(node) = scheduled.nodes.get(&NodeRef::new(kind, entity.clone())) else {
                    tracing::warn!(domain = %domain.target, entity = %entity, "skipped state for an entity absent from the restored schedule");
                    continue;
                };
                if schema.is_some_and(|schema| schema != node.schema_fingerprint) {
                    tracing::warn!(domain = %domain.target, entity = %entity, "skipped state with a mismatched schema fingerprint");
                    continue;
                }
                let runtime_state = match runtime_state {
                    RuntimeState::WasmProcessor { schema, .. } => {
                        let Some(generations) = node.wasm_state_generations() else {
                            return Err(StepFailure::Failed(format!(
                                "restored WASM processor '{entity}' has no published state \
                                 generation"
                            )));
                        };
                        RuntimeState::WasmProcessor {
                            schema,
                            generation: generations.of_branch(branch_fingerprint.as_ref()),
                        }
                    }
                    state => state,
                };
                let checkpoint = CapturedRuntimeState {
                    placement: StatePlacementEnvelope {
                        domain: domain.target.clone(),
                        state: runtime_state,
                        kind,
                        identifier: entity.clone(),
                        branch_key,
                    },
                    branch_fingerprint,
                    revision,
                    payload,
                };
                for owner_or_replica in &node.assigned_nodes {
                    self.install_restored_checkpoint_on(owner_or_replica, checkpoint.clone())
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn purge_restored_state_everywhere(
        &self,
        domain: &DomainName,
        coordination: &CoordinationIdentity,
    ) -> Result<(), StepFailure> {
        let mut nodes = self.inner.cluster.live_node_ids().await;
        if !nodes.contains(self.inner.consensus.local_node_id()) {
            nodes.push(self.inner.consensus.local_node_id().clone());
        }
        nodes.sort();
        nodes.dedup();
        for node in nodes {
            if &node == self.inner.consensus.local_node_id() {
                self.inner
                    .runtime
                    .purge_restored_domain_state(domain)
                    .map_err(|error| {
                        StepFailure::Failed(format!(
                            "failed to purge state of '{domain}' on '{node}': {error}"
                        ))
                    })?;
            } else {
                let result = self
                    .inner
                    .interconnect
                    .request(
                        &node,
                        InstallRestoredStateRequest {
                            coordination: coordination.clone(),
                            domain: domain.clone(),
                            action: InstallRestoredStateAction::PurgeDomain,
                        },
                    )
                    .await
                    .map_err(|error| {
                        StepFailure::Failed(format!(
                            "failed to purge state of '{domain}' on '{node}': {error}"
                        ))
                    })?;
                result.map_err(|failure| {
                    StepFailure::Failed(format!(
                        "failed to purge state of '{domain}' on '{node}': {failure}"
                    ))
                })?;
            }
        }
        Ok(())
    }

    async fn install_restored_checkpoint_on(
        &self,
        node: &ClusterNodeName,
        checkpoint: CapturedRuntimeState,
    ) -> Result<(), StepFailure> {
        if node == self.inner.consensus.local_node_id() {
            return self
                .inner
                .runtime
                .install_restored_domain_state(checkpoint)
                .map_err(|error| {
                    StepFailure::Failed(format!(
                        "failed to install restored state on '{node}': {error}"
                    ))
                });
        }
        let coordination = self
            .inner
            .interconnect
            .next_coordination_identity()
            .map_err(|error| {
                StepFailure::Failed(format!(
                    "failed to identify restored state transfer: {error}"
                ))
            })?;
        let domain = checkpoint.placement.domain.clone();
        let length = u64::try_from(checkpoint.payload.len()).map_err(|_| {
            StepFailure::Failed("restored state length exceeds address space".to_string())
        })?;
        let digest = *blake3::hash(&checkpoint.payload).as_bytes();
        self.send_restored_state_action(
            node,
            &coordination,
            &domain,
            InstallRestoredStateAction::Begin {
                placement: checkpoint.placement,
                branch_fingerprint: checkpoint
                    .branch_fingerprint
                    .map(|fingerprint| *fingerprint.fingerprint()),
                revision: checkpoint.revision,
                length,
                digest,
            },
        )
        .await?;
        for (index, chunk) in checkpoint.payload.chunks(64 * 1024).enumerate() {
            let offset = u64::try_from(index)
                .map_err(|_| {
                    StepFailure::Failed(
                        "restored state chunk index exceeds address space".to_string(),
                    )
                })?
                .checked_mul(64 * 1024)
                .ok_or_else(|| {
                    StepFailure::Failed("restored state offset exceeds address space".to_string())
                })?;
            self.send_restored_state_action(
                node,
                &coordination,
                &domain,
                InstallRestoredStateAction::Chunk {
                    offset,
                    payload: chunk.to_vec(),
                },
            )
            .await?;
        }
        self.send_restored_state_action(
            node,
            &coordination,
            &domain,
            InstallRestoredStateAction::Finish,
        )
        .await
    }

    async fn send_restored_state_action(
        &self,
        node: &ClusterNodeName,
        coordination: &CoordinationIdentity,
        domain: &DomainName,
        action: InstallRestoredStateAction,
    ) -> Result<(), StepFailure> {
        let result = self
            .inner
            .interconnect
            .request(
                node,
                InstallRestoredStateRequest {
                    coordination: coordination.clone(),
                    domain: domain.clone(),
                    action,
                },
            )
            .await
            .map_err(|error| {
                StepFailure::Failed(format!(
                    "failed to send restored state to '{node}': {error}"
                ))
            })?;
        result.map_err(|failure| {
            StepFailure::Failed(format!(
                "failed to install restored state on '{node}': {failure}"
            ))
        })
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
            nervix_primitives::task::consume_budget().await;
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
