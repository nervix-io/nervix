//! Bounded archive conversion of freshly captured materialized generations.
//!
//! Layer: control plane.
//! - **Owns.** Converting one generation into an archive descriptor, bounded scalar identity
//!   records and exact-schema Arrow IPC, staging each section before converting the next.
//! - **Depends on.** Runtime column views, the bounded executor and archive-owned contracts.
//! - **Must not know.** Native database keys, consensus activation or client framing.

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_backup::{
    ArchiveRecord, MATERIALIZED_COLUMNS_BYTES, MATERIALIZED_IDENTITIES_BYTES,
    MaterializedIdentitiesRecord, MaterializedRecordIdentity, MaterializedRelayDescriptor,
    SectionContent, SectionPath, StateField,
};
use nervix_execution::{ChargedBytes, CpuClass, MemoryClass};
use nervix_interconnect::{RemoteOperationFailure, RuntimeState};
use nervix_models::{DomainSchedule, NodeRef};

use super::{
    CaptureSectionKey, CapturedSection,
    interconnect::{CaptureDomainStateRequest, failed},
};
use crate::{
    application::session_service::SessionServiceImpl,
    runtime::{BranchKey, CapturedMaterializedRelay},
};

impl SessionServiceImpl {
    pub(super) async fn stage_materialized_sections(
        &self,
        captured: Vec<CapturedMaterializedRelay>,
        schedule: Option<&DomainSchedule>,
        request: &CaptureDomainStateRequest,
    ) -> Result<Vec<CapturedSection>, RemoteOperationFailure> {
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
            let generation = captured.generation;
            let _capture_charge = captured.charge;
            // Leave room for IPC framing and rkyv pointers beside the measured retained values.
            let groups = generation.bounded_groups(
                MATERIALIZED_IDENTITIES_BYTES / 2,
                MATERIALIZED_COLUMNS_BYTES / 2,
            );
            let descriptor = MaterializedRelayDescriptor {
                domain: request.domain.clone(),
                entity: placement.identifier.clone(),
                schema: node.schema_fingerprint,
                revision: generation.revision(),
                fence: generation.fence(),
                branch_generation: generation.branch_generation(),
                record_count: u64::try_from(generation.records().len())
                    .verified("record count fits"),
                groups: u32::try_from(groups.len())
                    .map_err(|_| failed(&request.domain, "too many materialized groups"))?,
            };
            let bytes = descriptor
                .encode()
                .map_err(|error| failed(&request.domain, &error.to_string()))?;
            let artifact = self.stage_captured_section(bytes, &request.domain).await?;
            let key = |path: SectionPath| CaptureSectionKey {
                coordination: request.coordination.clone(),
                domain: request.domain.clone(),
                path: path.to_string(),
            };
            staged.push(CapturedSection {
                key: key(SectionPath::materialized_descriptor(
                    &request.domain,
                    &placement.identifier,
                )),
                content: SectionContent::Record(MaterializedRelayDescriptor::KIND),
                artifact,
            });
            for (index, group) in groups.into_iter().enumerate() {
                nervix_primitives::task::consume_budget().await;
                let index = u32::try_from(index).verified("group count was checked");
                let reservation = executor
                    .reserve(MemoryClass::Bulk, 4 * MATERIALIZED_IDENTITIES_BYTES)
                    .await
                    .map_err(|_| {
                        failed(&request.domain, "materialized identities admission failed")
                    })?;
                let captured = generation.clone();
                let range = group.clone();
                let domain = request.domain.clone();
                let entity = placement.identifier.clone();
                let bytes = executor
                    .run_cpu(CpuClass::Bulk, reservation, move |charge, cancellation| {
                        cancellation.check().change_context(
                            nervix_backup::ArchiveWriteError::Encode {
                                kind: MaterializedIdentitiesRecord::KIND,
                            },
                        )?;
                        let identities = captured.records()[range]
                            .iter()
                            .map(|record| MaterializedRecordIdentity {
                                branch: BranchKey::to_remote_key(&record.branch).map(|fields| {
                                    fields.into_iter().map(StateField::from_remote).collect()
                                }),
                                watermarks: record.row.metadata().to_remote(),
                            })
                            .collect();
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
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                let mut writer = self
                    .inner
                    .runtime
                    .try_stage_artifact(
                        u64::try_from(bytes.len()).verified("bounded identities fit"),
                    )
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                writer
                    .write_chunk(bytes)
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                let artifact = writer
                    .finish_artifact()
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                staged.push(CapturedSection {
                    key: key(SectionPath::materialized_identities(
                        &request.domain,
                        &placement.identifier,
                        index,
                    )),
                    content: SectionContent::Record(MaterializedIdentitiesRecord::KIND),
                    artifact,
                });
                let columns = generation
                    .encode_columns(executor, &group)
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                let length = u64::try_from(columns.len()).verified("bounded Arrow columns fit");
                if length > MATERIALIZED_COLUMNS_BYTES {
                    return Err(failed(
                        &request.domain,
                        "materialized Arrow section exceeds its limit",
                    ));
                }
                let mut writer = self
                    .inner
                    .runtime
                    .try_stage_artifact(length)
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                writer
                    .write_chunk(columns)
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                let artifact = writer
                    .finish_artifact()
                    .await
                    .map_err(|error| failed(&request.domain, &error.to_string()))?;
                staged.push(CapturedSection {
                    key: key(SectionPath::materialized_columns(
                        &request.domain,
                        &placement.identifier,
                        index,
                    )),
                    content: SectionContent::MaterializedColumns,
                    artifact,
                });
            }
        }
        Ok(staged)
    }
}
