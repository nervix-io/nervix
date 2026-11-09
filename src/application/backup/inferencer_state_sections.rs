//! Layer: control plane.
//! Owns: checking a cut's inference identity and staging its descriptor and exact raw values.
//! Depends on: typed capture handles, the archive plan, quota-owned staging and archive records.
//! Must not know: native snapshot encodings, model execution or client framing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "stages one archived inference branch")
)]

use error_stack::{Report, ResultExt as _};
use nervix_backup::{
    ArchiveRecord, InferencerStateDescriptor, SectionContent, SectionDigest, SectionPath,
    StateField,
};
use nervix_interconnect::{RemoteOperationFailure, RuntimeState};
use nervix_models::ScheduledNode;

use super::{
    CaptureSectionKey, CapturedSection,
    interconnect::{CaptureDomainStateRequest, failed, failure_context},
};
use crate::{
    application::session_service::SessionServiceImpl,
    registry::InferencerArchivePlan,
    runtime::{CapturedBranchState, CapturedBranchStateKind, CapturedInferencer},
};

impl SessionServiceImpl {
    pub(super) async fn stage_inferencer_sections(
        &self,
        captured: CapturedBranchState,
        node: &ScheduledNode,
        request: &CaptureDomainStateRequest,
    ) -> Result<Vec<CapturedSection>, Report<RemoteOperationFailure>> {
        let domain = &request.domain;
        let placement = captured.placement;
        let Some(plan) = InferencerArchivePlan::of_scheduled(node) else {
            return Err(failed(
                domain,
                "the inferencer has no validated archive plan",
            ));
        };
        if placement.state
            != (RuntimeState::Inferencer {
                schema: node.schema_fingerprint,
                model: plan.model,
            })
        {
            return Err(failed(
                domain,
                "the captured inferencer model differs from its scheduled identity",
            ));
        }
        let capture = match captured.state {
            CapturedBranchStateKind::Inferencer(capture) => capture,
            CapturedBranchStateKind::StoredInferencer(checkpoint) => {
                let limit = plan.snapshot_bytes;
                let capture = CapturedInferencer::stored(checkpoint, &self.inner.runtime, limit)
                    .await
                    .change_context_lazy(|| {
                        failure_context(domain, "the stored inferencer snapshot could not be read")
                    })?;
                let Some(capture) = capture else {
                    return Ok(Vec::new());
                };
                capture
            }
            _ => {
                return Err(failed(
                    domain,
                    "the captured branch is not inferencer state",
                ));
            }
        };
        let snapshot_bytes = plan.snapshot_bytes;
        let (values, metadata) = capture
            .stage(&self.inner.runtime, plan.bytes_per_branch, snapshot_bytes)
            .await
            .change_context_lazy(|| {
                failure_context(domain, "the inferencer tensors could not be staged")
            })?;
        if metadata.shapes.as_slice() != plan.shapes() || values.length() != plan.bytes_per_branch {
            return Err(failed(
                domain,
                "the captured inferencer tensors differ from their declared shapes",
            ));
        }
        let descriptor = InferencerStateDescriptor {
            domain: domain.clone(),
            entity: placement.identifier,
            unit: placement.unit,
            schema: node.schema_fingerprint,
            model: plan.model,
            branch_fingerprint: captured.branch_fingerprint,
            branch: placement
                .branch_key
                .map(|fields| fields.into_iter().map(StateField::from_remote).collect()),
            incarnation: metadata.incarnation,
            revision: metadata.revision,
            shapes: metadata.shapes,
            byte_length: values.length(),
            digest: SectionDigest::from_bytes(values.digest()),
        };
        let record_path = SectionPath::inferencer_descriptor(
            domain,
            &descriptor.entity,
            descriptor.unit,
            descriptor.branch_fingerprint.as_ref(),
        );
        descriptor
            .validate(record_path.as_str())
            .change_context_lazy(|| {
                failure_context(domain, "the captured inferencer descriptor is invalid")
            })?;
        let values_path = SectionPath::inferencer_tensor_values(
            domain,
            &descriptor.entity,
            descriptor.unit,
            descriptor.branch_fingerprint.as_ref(),
        );
        let bytes = descriptor.encode().change_context_lazy(|| {
            failure_context(domain, "the inferencer descriptor could not be encoded")
        })?;
        let record = self.stage_captured_section(bytes, domain).await?;
        let key = |path: SectionPath| CaptureSectionKey {
            coordination: request.coordination.clone(),
            domain: domain.clone(),
            path: path.to_string(),
        };
        Ok(vec![
            CapturedSection {
                key: key(record_path),
                content: SectionContent::Record(InferencerStateDescriptor::KIND),
                artifact: record,
            },
            CapturedSection {
                key: key(values_path),
                content: SectionContent::InferencerTensorValues,
                artifact: values,
            },
        ])
    }
}
