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
use nervix_backup::{DescribedRuntimeState, DescribedSection};
use nervix_consensus::{CommandExecution, ConsensusError, RestoreStepEffect};
use nervix_interconnect::{RuntimeState, StatePlacementEnvelope, backup::RestoreStateInventory};
use nervix_models::{
    BranchKeyFingerprint, ClusterNodeName, CoordinationIdentity, DomainName, ModelKind, NodeRef,
    RemoteRuntimeField, ResourceId, ResourceUploadIdentity, ResourceUploadKey, RestoreState,
    RestoreStateAuthority, RestoreStep, SchemaFingerprint, UserName,
};

use super::{
    prepare::{VerifiedArchive, create_statements},
    runner::{RestoreSteps, StepFailure},
};
use crate::{
    application::{
        backup::{
            interconnect::{InstallRestoredStateAction, InstallRestoredStateRequest},
            restore_storage::RestoredStateSource,
        },
        resource::ImportedResourceVersion,
        session_service::SessionServiceImpl,
    },
    registry::{PlannedDomain, RestorePlan},
    runtime::{
        BackupBranchLifecycleEntry, RESTORE_STATE_CHUNK_BYTES, RestoredRuntimeState,
        encode_restored_branch_lifecycle, encode_restored_kafka_offsets,
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
                let authority = self
                    .install_restored_state(execution, domain, archive, state)
                    .await?;
                self.record_restore_step(
                    execution,
                    step,
                    RestoreStepEffect::InstalledState(authority),
                )
                .await
            }
        }
    }

    /// Installs state only after models have published their new stopped-domain schedule. A
    /// retry stages a new fenced generation and replaces the complete stopped-domain set.
    async fn install_restored_state(
        &self,
        execution: &CommandExecution,
        domain: &PlannedDomain,
        archive: &VerifiedArchive,
        state: RestoreState,
    ) -> Result<RestoreStateAuthority, StepFailure> {
        enum RestorePayload<'a> {
            Encoded(Vec<u8>),
            Guest(&'a DescribedSection),
        }
        struct RestoredStateSection<'a> {
            reference: NodeRef,
            schema: SchemaFingerprint,
            branch_fingerprint: Option<BranchKeyFingerprint>,
            branch_key: Option<Vec<RemoteRuntimeField>>,
            runtime_state: RuntimeState,
            revision: u64,
            payload: RestorePayload<'a>,
        }

        let authority = match self
            .inner
            .consensus
            .begin_restore_state_installation(execution.reference.clone(), domain.target.clone())
            .await
        {
            Ok(authority) => authority,
            Err(error) => return Err(self.consensus_step_failure(&error).await),
        };
        let mut nodes = self.inner.cluster.live_node_ids().await;
        nodes.push(self.inner.consensus.local_node_id().clone());
        let mut inventories = nodes
            .into_iter()
            .map(|node| (node, RestoreStateInventory::default()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let schedule = self.inner.consensus.current_schedule().await;
        let scheduled = schedule.domain(&domain.target);
        // Stage lifecycle before guest saves; the published generation contains both at once.
        for first_lifecycle in [true, false] {
            nervix_primitives::task::consume_budget().await;
            if state == RestoreState::ConfigurationOnly {
                break;
            }
            for archived in archive.states_for(&domain.source) {
                nervix_primitives::task::consume_budget().await;
                let is_lifecycle =
                    matches!(archived, DescribedRuntimeState::BranchLifecycle { .. });
                if is_lifecycle != first_lifecycle {
                    continue;
                }
                let RestoredStateSection {
                    reference,
                    schema,
                    branch_fingerprint,
                    branch_key,
                    runtime_state,
                    revision,
                    payload,
                } = match archived {
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
                        RestoredStateSection {
                            reference: NodeRef::new(lifecycle.owner_kind, lifecycle.entity.clone()),
                            schema: lifecycle.schema,
                            branch_fingerprint: None,
                            branch_key: None,
                            runtime_state: RuntimeState::BranchLru {
                                schema: lifecycle.schema,
                            },
                            revision: lifecycle.revision,
                            payload: RestorePayload::Encoded(payload),
                        }
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
                        RestoredStateSection {
                            reference: NodeRef::new(ModelKind::Ingestor, offsets.entity.clone()),
                            schema: offsets.schema,
                            branch_fingerprint: None,
                            branch_key: None,
                            runtime_state: RuntimeState::KafkaOffset,
                            revision: offsets.revision,
                            payload: RestorePayload::Encoded(payload),
                        }
                    }
                    DescribedRuntimeState::Wasm {
                        descriptor, guest, ..
                    } => RestoredStateSection {
                        reference: NodeRef::new(
                            ModelKind::WasmProcessor,
                            descriptor.entity.clone(),
                        ),
                        schema: descriptor.schema,
                        branch_fingerprint: descriptor.branch_fingerprint,
                        branch_key: descriptor.branch.clone().map(|fields| {
                            fields
                                .into_iter()
                                .map(|field| field.into_remote())
                                .collect()
                        }),
                        runtime_state: RuntimeState::WasmProcessor {
                            schema: descriptor.schema,
                            generation: descriptor.generation,
                        },
                        revision: descriptor.revision,
                        payload: RestorePayload::Guest(guest),
                    },
                };
                let Some(node) = scheduled.and_then(|scheduled| scheduled.nodes.get(&reference))
                else {
                    tracing::warn!(domain = %domain.target, entity = %reference.identifier, "skipped state for an entity absent from the restored schedule");
                    continue;
                };
                if schema != node.schema_fingerprint {
                    tracing::warn!(domain = %domain.target, entity = %reference.identifier, "skipped state with a mismatched schema fingerprint");
                    continue;
                }
                let runtime_state = match runtime_state {
                    RuntimeState::WasmProcessor { schema, .. } => {
                        let Some(generations) = node.wasm_state_generations() else {
                            return Err(StepFailure::Failed(format!(
                                "restored WASM processor '{}' has no published state generation",
                                reference.identifier
                            )));
                        };
                        RuntimeState::WasmProcessor {
                            schema,
                            generation: generations.of_branch(branch_fingerprint.as_ref()),
                        }
                    }
                    state => state,
                };
                let (length, digest) = match &payload {
                    RestorePayload::Encoded(bytes) => (
                        u64::try_from(bytes.len()).map_err(|_| {
                            StepFailure::Failed(
                                "restored metadata length exceeds address space".to_string(),
                            )
                        })?,
                        *blake3::hash(bytes).as_bytes(),
                    ),
                    RestorePayload::Guest(section) => (section.length, *section.digest.as_bytes()),
                };
                let checkpoint = RestoredRuntimeState {
                    placement: StatePlacementEnvelope {
                        domain: domain.target.clone(),
                        state: runtime_state,
                        kind: reference.kind,
                        identifier: reference.identifier,
                        branch_key,
                    },
                    branch_fingerprint,
                    revision,
                    length,
                    digest,
                };
                for owner_or_replica in &node.assigned_nodes {
                    nervix_primitives::task::consume_budget().await;
                    let inventory = inventories.entry(owner_or_replica.clone()).or_default();
                    inventory.checkpoints =
                        inventory.checkpoints.checked_add(1).ok_or_else(|| {
                            StepFailure::Failed(
                                "restore checkpoint count exceeds address space".to_string(),
                            )
                        })?;
                    inventory.payload_bytes =
                        inventory.payload_bytes.checked_add(length).ok_or_else(|| {
                            StepFailure::Failed(
                                "restore state length exceeds address space".to_string(),
                            )
                        })?;
                    match &payload {
                        RestorePayload::Encoded(bytes) => {
                            self.install_restored_checkpoint_on(
                                owner_or_replica,
                                &authority,
                                checkpoint.clone(),
                                bytes,
                            )
                            .await?
                        }
                        RestorePayload::Guest(section) => {
                            self.install_restored_guest_on(
                                owner_or_replica,
                                &authority,
                                checkpoint.clone(),
                                archive,
                                section,
                            )
                            .await?
                        }
                    }
                }
            }
        }
        // No published checkpoint changes until every compatible section has staged successfully.
        let local = self.inner.consensus.local_node_id().clone();
        let local_inventory = inventories
            .remove(&local)
            .verified("the coordinator is an installation target");
        for (node, inventory) in std::iter::once((local, local_inventory)).chain(inventories) {
            nervix_primitives::task::consume_budget().await;
            if &node == self.inner.consensus.local_node_id() {
                self.publish_restored_state_generation(&domain.target, &authority, inventory)
                    .await
                    .map_err(|failure| StepFailure::Failed(failure.to_string()))?;
            } else {
                let coordination = self
                    .inner
                    .interconnect
                    .next_coordination_identity()
                    .map_err(|error| StepFailure::Failed(error.to_string()))?;
                self.send_restored_state_action(
                    &node,
                    &coordination,
                    &domain.target,
                    &authority,
                    InstallRestoredStateAction::Publish { inventory },
                )
                .await?;
            }
        }
        Ok(authority)
    }

    async fn install_restored_checkpoint_on(
        &self,
        node: &ClusterNodeName,
        authority: &RestoreStateAuthority,
        checkpoint: RestoredRuntimeState,
        payload: &[u8],
    ) -> Result<(), StepFailure> {
        if node == self.inner.consensus.local_node_id() {
            return self
                .stage_restored_state_checkpoint(
                    authority,
                    checkpoint,
                    RestoredStateSource::Encoded(payload.to_vec()),
                )
                .await
                .map_err(|error| StepFailure::Failed(error.to_string()));
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
        self.send_restored_state_action(
            node,
            &coordination,
            &domain,
            authority,
            InstallRestoredStateAction::Begin {
                placement: checkpoint.placement,
                branch_fingerprint: checkpoint
                    .branch_fingerprint
                    .map(|fingerprint| *fingerprint.fingerprint()),
                revision: checkpoint.revision,
                length: checkpoint.length,
                digest: checkpoint.digest,
            },
        )
        .await?;
        for (index, chunk) in payload.chunks(RESTORE_STATE_CHUNK_BYTES).enumerate() {
            nervix_primitives::task::consume_budget().await;
            let offset = u64::try_from(index)
                .map_err(|_| {
                    StepFailure::Failed(
                        "restored state chunk index exceeds address space".to_string(),
                    )
                })?
                .checked_mul(u64::try_from(RESTORE_STATE_CHUNK_BYTES).verified("chunk size fits"))
                .ok_or_else(|| {
                    StepFailure::Failed("restored state offset exceeds address space".to_string())
                })?;
            self.send_restored_state_action(
                node,
                &coordination,
                &domain,
                authority,
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
            authority,
            InstallRestoredStateAction::Finish,
        )
        .await
    }

    async fn install_restored_guest_on(
        &self,
        node: &ClusterNodeName,
        authority: &RestoreStateAuthority,
        checkpoint: RestoredRuntimeState,
        archive: &VerifiedArchive,
        section: &DescribedSection,
    ) -> Result<(), StepFailure> {
        if node == self.inner.consensus.local_node_id() {
            return self
                .stage_restored_state_checkpoint(
                    authority,
                    checkpoint,
                    RestoredStateSource::Archive {
                        artifact: archive.artifact(),
                        offset: section.offset,
                    },
                )
                .await
                .map_err(|error| StepFailure::Failed(error.to_string()));
        }
        let coordination = self
            .inner
            .interconnect
            .next_coordination_identity()
            .map_err(|error| StepFailure::Failed(error.to_string()))?;
        let domain = checkpoint.placement.domain.clone();
        self.send_restored_state_action(
            node,
            &coordination,
            &domain,
            authority,
            InstallRestoredStateAction::Begin {
                placement: checkpoint.placement,
                branch_fingerprint: checkpoint
                    .branch_fingerprint
                    .map(|fingerprint| *fingerprint.fingerprint()),
                revision: checkpoint.revision,
                length: checkpoint.length,
                digest: checkpoint.digest,
            },
        )
        .await?;
        let mut offset = 0_u64;
        while offset < section.length {
            nervix_primitives::task::consume_budget().await;
            let chunk = archive
                .read_guest_chunk(&self.inner.runtime, section, offset)
                .await
                .map_err(|error| StepFailure::Failed(error.to_string()))?;
            let length = u64::try_from(chunk.len()).verified("a bounded chunk fits");
            self.send_restored_state_action(
                node,
                &coordination,
                &domain,
                authority,
                InstallRestoredStateAction::Chunk {
                    offset,
                    payload: chunk.to_vec(),
                },
            )
            .await?;
            offset = offset.checked_add(length).ok_or_else(|| {
                StepFailure::Failed("restore guest offset exceeds address space".to_string())
            })?;
        }
        self.send_restored_state_action(
            node,
            &coordination,
            &domain,
            authority,
            InstallRestoredStateAction::Finish,
        )
        .await
    }

    async fn send_restored_state_action(
        &self,
        node: &ClusterNodeName,
        coordination: &CoordinationIdentity,
        domain: &DomainName,
        authority: &RestoreStateAuthority,
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
                    authority: authority.clone(),
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
