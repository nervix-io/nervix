//! Archive sections from one stopped relay's immutable checkpoint view.
//!
//! Layer: control plane.
//! - **Owns.** Scalar identity conversion and quota-owned staging of each preserved Arrow group.
//! - **Depends on.** Typed checkpoint readers, current archive contracts and committed placements.
//! - **Must not know.** Database keys, native framing, row decoding or runtime publication.

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_backup::{
    ArchiveRecord, MATERIALIZED_COLUMNS_BYTES, MATERIALIZED_IDENTITIES_BYTES,
    MaterializedIdentitiesRecord, MaterializedRecordIdentity, MaterializedRelayDescriptor,
    SectionContent, SectionPath, StateField,
};
use nervix_execution::{ChargedBytes, CpuClass, MemoryClass};
use nervix_interconnect::{RemoteOperationFailure, RuntimeState};
use nervix_models::{DomainName, DomainSchedule, NodeRef};

use super::{
    CaptureSectionKey,
    interconnect::{CaptureDomainStateRequest, failed},
};
use crate::{
    application::session_service::SessionServiceImpl,
    runtime::{CapturedStoredMaterializedRelay, StagedArtifact},
};

impl SessionServiceImpl {
    pub(super) async fn stage_stored_materialized_sections(
        &self,
        captured: Vec<CapturedStoredMaterializedRelay>,
        schedule: Option<&DomainSchedule>,
        request: &CaptureDomainStateRequest,
    ) -> Result<Vec<(CaptureSectionKey, SectionContent, StagedArtifact)>, RemoteOperationFailure>
    {
        let mut staged = Vec::new();
        let executor = self.inner.runtime.executor();
        for captured in captured {
            nervix_primitives::task::consume_budget().await;
            let placement = captured.placement;
            let Some(node) = schedule.and_then(|schedule| {
                schedule
                    .nodes
                    .get(&NodeRef::new(placement.kind, placement.identifier.clone()))
            }) else {
                continue;
            };
            if node.primary_node.as_ref() != Some(self.inner.consensus.local_node_id()) {
                continue;
            }
            let RuntimeState::MaterializedRelay { .. } = placement.state else {
                continue;
            };
            // The runtime selected only the current schema and START lifetime's stored identity.
            let mut reader = captured
                .checkpoint
                .open(executor)
                .await
                .map_err(|error| failed(&request.domain, &error.to_string()))?;
            let header = reader.summary();
            let descriptor = MaterializedRelayDescriptor {
                domain: request.domain.clone(),
                entity: placement.identifier.clone(),
                schema: node.schema_fingerprint,
                revision: header.revision,
                fence: header.fence,
                branch_generation: header.branch_generation,
                record_count: header.records,
                groups: header.groups,
            };
            let key = |path: SectionPath| CaptureSectionKey {
                coordination: request.coordination.clone(),
                domain: request.domain.clone(),
                path: path.to_string(),
            };
            let bytes = descriptor
                .encode()
                .map_err(|error| failed(&request.domain, &error.to_string()))?;
            let artifact = self.stage_captured_section(bytes, &request.domain).await?;
            staged.push((
                key(SectionPath::materialized_descriptor(
                    &request.domain,
                    &placement.identifier,
                )),
                SectionContent::Record(MaterializedRelayDescriptor::KIND),
                artifact,
            ));
            let mut index = 0;
            while let Some(group) = reader
                .next_group(
                    executor,
                    MATERIALIZED_IDENTITIES_BYTES,
                    MATERIALIZED_COLUMNS_BYTES,
                )
                .await
                .map_err(|error| failed(&request.domain, &error.to_string()))?
            {
                let charge = executor
                    .reserve(MemoryClass::Bulk, 4 * MATERIALIZED_IDENTITIES_BYTES)
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                let domain = request.domain.clone();
                let entity = placement.identifier.clone();
                let bytes = executor
                    .run_cpu(CpuClass::Bulk, charge, move |charge, cancellation| {
                        let mut identities = Vec::with_capacity(group.identities.len());
                        for (branch, watermarks) in group.identities {
                            cancellation.check().change_context(
                                nervix_backup::ArchiveWriteError::Encode {
                                    kind: MaterializedIdentitiesRecord::KIND,
                                },
                            )?;
                            identities.push(MaterializedRecordIdentity {
                                branch: branch.map(|fields| {
                                    fields.into_iter().map(StateField::from_remote).collect()
                                }),
                                watermarks,
                            });
                        }
                        let bytes = MaterializedIdentitiesRecord {
                            domain,
                            entity,
                            group: index,
                            identities,
                        }
                        .encode()?;
                        Ok::<_, Report<nervix_backup::ArchiveWriteError>>(ChargedBytes::from_owned(
                            bytes, charge,
                        ))
                    })
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?
                    .map_err(|error: Report<nervix_backup::ArchiveWriteError>| {
                        failed(&request.domain, &error.to_string())
                    })?;
                let artifact = self
                    .stage_materialized_section(bytes, &request.domain)
                    .await?;
                staged.push((
                    key(SectionPath::materialized_identities(
                        &request.domain,
                        &placement.identifier,
                        index,
                    )),
                    SectionContent::Record(MaterializedIdentitiesRecord::KIND),
                    artifact,
                ));
                let artifact = self
                    .stage_materialized_section(group.columns, &request.domain)
                    .await?;
                staged.push((
                    key(SectionPath::materialized_columns(
                        &request.domain,
                        &placement.identifier,
                        index,
                    )),
                    SectionContent::MaterializedColumns,
                    artifact,
                ));
                index += 1;
            }
        }
        Ok(staged)
    }

    async fn stage_materialized_section(
        &self,
        bytes: ChargedBytes,
        domain: &DomainName,
    ) -> Result<StagedArtifact, RemoteOperationFailure> {
        let mut writer = self
            .inner
            .runtime
            .try_stage_artifact(u64::try_from(bytes.len()).verified("bounded group bytes fit"))
            .await
            .map_err(|error| failed(domain, &error.to_string()))?;
        writer
            .write_chunk(bytes)
            .await
            .map_err(|error| failed(domain, &error.to_string()))?;
        writer
            .finish_artifact()
            .await
            .map_err(|error| failed(domain, &error.to_string()))
    }
}
