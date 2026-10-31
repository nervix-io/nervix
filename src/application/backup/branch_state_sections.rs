//! Bounded archive conversion of captured deduplicator keyspaces and windows.
//!
//! Layer: control plane.
//! - **Owns.** Converting each active branch's keyspace or window into its archive descriptor and
//!   bounded Arrow groups under the shapes its models give it, staging each section before
//!   converting the next.
//! - **Depends on.** Captured runtime branch states, the decision layer's archive shapes, the
//!   bounded executor and archive-owned contracts.
//! - **Must not know.** Native checkpoint encodings, consensus activation or client framing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_backup::{
    ArchiveRecord, BRANCH_STATE_GROUP_BYTES, DeduplicatorStateDescriptor, DelayedHistogramRemoval,
    SectionContent, SectionPath, StateField, WindowAccumulatorRecord, WindowStateDescriptor,
};
use nervix_execution::ChargedBytes;
use nervix_interconnect::{RemoteOperationFailure, StateSchema};
use nervix_models::{DomainName, DomainSchedule, NodeRef, SchemaFingerprint};

use super::{
    CaptureSectionKey, CapturedSection,
    interconnect::{CaptureDomainStateRequest, failed, failure_context},
};
use crate::{
    application::session_service::SessionServiceImpl,
    registry::BranchStateSchemas,
    runtime::{
        CapturedBranchState, CapturedBranchStateKind, StagedArtifact, WindowAccumulatorState,
    },
};

impl SessionServiceImpl {
    pub(super) async fn stage_branch_state_sections(
        &self,
        captured: Vec<CapturedBranchState>,
        schedule: Option<&DomainSchedule>,
        request: &CaptureDomainStateRequest,
    ) -> Result<Vec<CapturedSection>, Report<RemoteOperationFailure>> {
        let mut staged = Vec::new();
        if captured.is_empty() {
            return Ok(staged);
        }
        let models = self
            .inner
            .registry
            .transaction_planning_models(&request.domain);
        let schemas =
            BranchStateSchemas::resolve(&request.domain, &models).change_context_lazy(|| {
                failure_context(&request.domain, "backup state section operation failed")
            })?;
        for captured in captured {
            nervix_primitives::task::consume_budget().await;
            let placement = &captured.placement;
            let Some(node) = schedule.and_then(|schedule| {
                schedule
                    .nodes
                    .get(&NodeRef::new(placement.kind, placement.identifier.clone()))
            }) else {
                continue;
            };
            if node
                .units
                .get(&placement.unit)
                .and_then(|unit| unit.primary_node.as_ref())
                != Some(self.inner.consensus.local_node_id())
            {
                continue;
            }
            if placement.state.schema() != StateSchema::Fingerprinted(node.schema_fingerprint) {
                continue;
            }
            let schema = node.schema_fingerprint;
            let sections = match captured.state {
                CapturedBranchStateKind::Deduplicator(_)
                | CapturedBranchStateKind::StoredDeduplicator(_) => {
                    self.stage_deduplicator_sections(captured, schema, &schemas, request)
                        .await?
                }
                CapturedBranchStateKind::Window(_) | CapturedBranchStateKind::StoredWindow(_) => {
                    self.stage_window_sections(captured, schema, &schemas, request)
                        .await?
                }
            };
            staged.extend(sections);
        }
        Ok(staged)
    }

    async fn stage_deduplicator_sections(
        &self,
        captured: CapturedBranchState,
        schema: SchemaFingerprint,
        schemas: &BranchStateSchemas,
        request: &CaptureDomainStateRequest,
    ) -> Result<Vec<CapturedSection>, Report<RemoteOperationFailure>> {
        let domain = &request.domain;
        let entity = captured.placement.identifier.clone();
        let Some(key_schema) = schemas.deduplicator(&entity) else {
            return Err(failed(
                domain,
                &format!("deduplicator '{entity}' has no archive key shape"),
            ));
        };
        let keyspace = match captured.state {
            CapturedBranchStateKind::Deduplicator(keyspace) => keyspace,
            CapturedBranchStateKind::StoredDeduplicator(checkpoint) => checkpoint
                .open_keyspace(&self.inner.runtime)
                .await
                .map_err(|error| {
                    error.change_context(failure_context(
                        domain,
                        &format!("stored keyspace of '{entity}' could not be read"),
                    ))
                })?,
            _ => return Ok(Vec::new()),
        };
        let branch = captured.branch_fingerprint;
        let groups = keyspace.groups(BRANCH_STATE_GROUP_BYTES / 2);
        let descriptor = DeduplicatorStateDescriptor {
            domain: domain.clone(),
            entity: entity.clone(),
            unit: captured.placement.unit,
            schema,
            branch_fingerprint: branch,
            branch: archive_branch(captured.placement.branch_key),
            revision: keyspace.revision(),
            keys: u64::try_from(keyspace.key_count()).verified("an addressable key count fits"),
            groups: u32::try_from(groups.len())
                .map_err(|_| failed(domain, "too many deduplicator key groups"))?,
        };
        let bytes = descriptor.encode().change_context_lazy(|| {
            failure_context(domain, "backup state section operation failed")
        })?;
        let mut staged = vec![CapturedSection {
            key: capture_key(
                request,
                SectionPath::deduplicator_descriptor(
                    domain,
                    &entity,
                    captured.placement.unit,
                    branch.as_ref(),
                ),
            ),
            content: SectionContent::Record(DeduplicatorStateDescriptor::KIND),
            artifact: self.stage_captured_section(bytes, domain).await?,
        }];
        let executor = self.inner.runtime.executor();
        for (index, group) in groups.into_iter().enumerate() {
            nervix_primitives::task::consume_budget().await;
            let index = u32::try_from(index).verified("the group count was checked above");
            let keys = keyspace
                .encode_group(executor, key_schema, group)
                .await
                .change_context_lazy(|| {
                    failure_context(domain, "backup state section operation failed")
                })?;
            staged.push(CapturedSection {
                key: capture_key(
                    request,
                    SectionPath::deduplicator_keys(
                        domain,
                        &entity,
                        captured.placement.unit,
                        branch.as_ref(),
                        index,
                    ),
                ),
                content: SectionContent::DeduplicatorKeys,
                artifact: self.stage_branch_state_group(keys, domain).await?,
            });
        }
        Ok(staged)
    }

    async fn stage_window_sections(
        &self,
        captured: CapturedBranchState,
        schema: SchemaFingerprint,
        schemas: &BranchStateSchemas,
        request: &CaptureDomainStateRequest,
    ) -> Result<Vec<CapturedSection>, Report<RemoteOperationFailure>> {
        let domain = &request.domain;
        let entity = captured.placement.identifier.clone();
        let Some(window_schemas) = schemas.window(&entity) else {
            return Err(failed(
                domain,
                &format!("window processor '{entity}' has no archive shape"),
            ));
        };
        let executor = self.inner.runtime.executor();
        let window = match captured.state {
            CapturedBranchStateKind::Window(window) => window.open(),
            CapturedBranchStateKind::StoredWindow(checkpoint) => checkpoint
                .open_window(
                    &self.inner.runtime,
                    &window_schemas.input,
                    &window_schemas.arguments,
                )
                .await
                .map_err(|error| {
                    error.change_context(failure_context(
                        domain,
                        &format!("stored window of '{entity}' could not be read"),
                    ))
                })?,
            _ => return Ok(Vec::new()),
        };
        // A window without a branch lifetime has never retained a row; it restores empty.
        let Some(window) = window else {
            return Ok(Vec::new());
        };
        let accumulators = window.accumulators().change_context_lazy(|| {
            failure_context(domain, "backup state section operation failed")
        })?;
        let mut records = Vec::with_capacity(accumulators.len());
        for accumulator in accumulators {
            records.push(archive_accumulator(accumulator));
        }
        let branch = captured.branch_fingerprint;
        let groups = window.groups(BRANCH_STATE_GROUP_BYTES / 2);
        let descriptor = WindowStateDescriptor {
            domain: domain.clone(),
            entity: entity.clone(),
            unit: captured.placement.unit,
            schema,
            model: window_schemas.model,
            branch_fingerprint: branch,
            branch: archive_branch(captured.placement.branch_key),
            revision: window.revision(),
            incarnation: window.incarnation(),
            first_sequence: window.first_sequence(),
            next_sequence: window.next_sequence(),
            rows: window.row_watermarks(),
            groups: u32::try_from(groups.len())
                .map_err(|_| failed(domain, "too many window row groups"))?,
            accumulators: records,
        };
        let bytes = descriptor.encode().change_context_lazy(|| {
            failure_context(domain, "backup state section operation failed")
        })?;
        let mut staged = vec![CapturedSection {
            key: capture_key(
                request,
                SectionPath::window_descriptor(
                    domain,
                    &entity,
                    captured.placement.unit,
                    branch.as_ref(),
                ),
            ),
            content: SectionContent::Record(WindowStateDescriptor::KIND),
            artifact: self.stage_captured_section(bytes, domain).await?,
        }];
        for (index, group) in groups.into_iter().enumerate() {
            nervix_primitives::task::consume_budget().await;
            let index = u32::try_from(index).verified("the group count was checked above");
            let input = window
                .encode_input_group(executor, &group)
                .await
                .change_context_lazy(|| {
                    failure_context(domain, "backup state section operation failed")
                })?;
            staged.push(CapturedSection {
                key: capture_key(
                    request,
                    SectionPath::window_input_rows(
                        domain,
                        &entity,
                        captured.placement.unit,
                        branch.as_ref(),
                        index,
                    ),
                ),
                content: SectionContent::WindowInputRows,
                artifact: self.stage_branch_state_group(input, domain).await?,
            });
            let arguments = window
                .encode_argument_group(executor, &group)
                .await
                .change_context_lazy(|| {
                    failure_context(domain, "backup state section operation failed")
                })?;
            staged.push(CapturedSection {
                key: capture_key(
                    request,
                    SectionPath::window_argument_columns(
                        domain,
                        &entity,
                        captured.placement.unit,
                        branch.as_ref(),
                        index,
                    ),
                ),
                content: SectionContent::WindowArgumentColumns,
                artifact: self.stage_branch_state_group(arguments, domain).await?,
            });
        }
        Ok(staged)
    }

    /// Stages one encoded Arrow group, refusing one the archive's group bound would refuse.
    async fn stage_branch_state_group(
        &self,
        bytes: ChargedBytes,
        domain: &DomainName,
    ) -> Result<StagedArtifact, Report<RemoteOperationFailure>> {
        let length = u64::try_from(bytes.len()).verified("an encoded group fits 64 bits");
        if length > BRANCH_STATE_GROUP_BYTES {
            return Err(failed(
                domain,
                "a deduplicator or window Arrow group exceeds its archive limit",
            ));
        }
        let mut writer = self
            .inner
            .runtime
            .try_stage_artifact(length)
            .await
            .change_context_lazy(|| {
                failure_context(domain, "backup state section operation failed")
            })?;
        writer.write_chunk(bytes).await.change_context_lazy(|| {
            failure_context(domain, "backup state section operation failed")
        })?;
        writer.finish_artifact().await.change_context_lazy(|| {
            failure_context(domain, "backup state section operation failed")
        })
    }
}

fn capture_key(request: &CaptureDomainStateRequest, path: SectionPath) -> CaptureSectionKey {
    CaptureSectionKey {
        coordination: request.coordination.clone(),
        domain: request.domain.clone(),
        path: path.to_string(),
    }
}

/// A typed runtime branch key as the archive's independent field values.
fn archive_branch(key: Option<Vec<nervix_models::RemoteRuntimeField>>) -> Option<Vec<StateField>> {
    let fields = key?;
    Some(fields.into_iter().map(StateField::from_remote).collect())
}

fn archive_accumulator(accumulator: WindowAccumulatorState) -> WindowAccumulatorRecord {
    match accumulator {
        WindowAccumulatorState::Retained => WindowAccumulatorRecord::Retained,
        WindowAccumulatorState::LinearHistogram { delayed_removals } => {
            WindowAccumulatorRecord::LinearHistogram {
                delayed_removals: delayed_removals
                    .into_iter()
                    .map(|removal| DelayedHistogramRemoval {
                        expires_at: removal.expires_at,
                        bucket: removal.bucket,
                    })
                    .collect(),
            }
        }
    }
}
