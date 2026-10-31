//! The bijection between captured state kinds and archive content and paths.
//!
//! Layer: test harness.
//! - **Owns.** Complete section mapping and collision oracles over bounded current identities.
//! - **Depends on.** The production capture mapping and the archive's typed path constructors.
//! - **Must not know.** Capture scheduling, archive payloads or restore execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeSet;

use meticulous::ResultExt as _;
use nervix_backup::SectionPath;
use nervix_models::{BranchKeyFingerprint, ModelKind, ModelName};

use super::*;

#[derive(Debug, bolero::TypeGenerator)]
struct SectionMappingCase {
    domain: u16,
    entity: u16,
    fingerprint: [u8; 32],
    group: u32,
}

impl SectionMappingCase {
    fn path(
        &self,
        kind: CapturedStateSectionKind,
        branch: Option<&BranchKeyFingerprint>,
    ) -> SectionPath {
        let domain = DomainName::parse(&format!("backup_{}", self.domain))
            .assured("a generated decimal suffix makes a valid domain name");
        let entity = ModelName::parse(&format!("state_{}", self.entity))
            .assured("a generated decimal suffix makes a valid model name");
        match kind {
            CapturedStateSectionKind::WasmDescriptor => SectionPath::wasm_state_descriptor(
                &domain,
                &entity,
                nervix_models::Unit::Single,
                branch,
            ),
            CapturedStateSectionKind::KafkaOffsets => {
                SectionPath::kafka_offsets(&domain, &entity, nervix_models::Unit::Single)
            }
            CapturedStateSectionKind::BranchLifecycle => SectionPath::branch_lifecycle(
                &domain,
                ModelKind::WasmProcessor,
                &entity,
                nervix_models::Unit::Single,
            ),
            CapturedStateSectionKind::WasmGuestBlob => {
                SectionPath::wasm_guest_blob(&domain, &entity, nervix_models::Unit::Single, branch)
            }
            CapturedStateSectionKind::MaterializedDescriptor => {
                SectionPath::materialized_descriptor(&domain, &entity, nervix_models::Unit::Single)
            }
            CapturedStateSectionKind::MaterializedIdentities => {
                SectionPath::materialized_identities(
                    &domain,
                    &entity,
                    nervix_models::Unit::Single,
                    self.group,
                )
            }
            CapturedStateSectionKind::MaterializedColumns => SectionPath::materialized_columns(
                &domain,
                &entity,
                nervix_models::Unit::Single,
                self.group,
            ),
            CapturedStateSectionKind::DeduplicatorDescriptor => {
                SectionPath::deduplicator_descriptor(
                    &domain,
                    &entity,
                    nervix_models::Unit::Single,
                    branch,
                )
            }
            CapturedStateSectionKind::DeduplicatorKeys => SectionPath::deduplicator_keys(
                &domain,
                &entity,
                nervix_models::Unit::Single,
                branch,
                self.group,
            ),
            CapturedStateSectionKind::WindowDescriptor => SectionPath::window_descriptor(
                &domain,
                &entity,
                nervix_models::Unit::Single,
                branch,
            ),
            CapturedStateSectionKind::WindowInputRows => SectionPath::window_input_rows(
                &domain,
                &entity,
                nervix_models::Unit::Single,
                branch,
                self.group,
            ),
            CapturedStateSectionKind::WindowArgumentColumns => {
                SectionPath::window_argument_columns(
                    &domain,
                    &entity,
                    nervix_models::Unit::Single,
                    branch,
                    self.group,
                )
            }
        }
    }
}

#[test]
fn bolero_captured_section_mapping_is_total_and_injective() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(128)
        .with_type::<SectionMappingCase>()
        .for_each(|case| {
            let fingerprint = BranchKeyFingerprint::new(case.fingerprint);
            let kinds = [
                CapturedStateSectionKind::WasmDescriptor,
                CapturedStateSectionKind::KafkaOffsets,
                CapturedStateSectionKind::BranchLifecycle,
                CapturedStateSectionKind::WasmGuestBlob,
                CapturedStateSectionKind::MaterializedDescriptor,
                CapturedStateSectionKind::MaterializedIdentities,
                CapturedStateSectionKind::MaterializedColumns,
                CapturedStateSectionKind::DeduplicatorDescriptor,
                CapturedStateSectionKind::DeduplicatorKeys,
                CapturedStateSectionKind::WindowDescriptor,
                CapturedStateSectionKind::WindowInputRows,
                CapturedStateSectionKind::WindowArgumentColumns,
            ];
            for branch in [None, Some(&fingerprint)] {
                let mut contents = Vec::new();
                let mut paths = BTreeSet::new();
                for kind in kinds {
                    let content = section_content(kind);
                    assert_eq!(section_kind(content), Some(kind));
                    assert!(
                        !contents.contains(&content),
                        "distinct kinds name distinct content"
                    );
                    contents.push(content);
                    let path = case.path(kind, branch);
                    assert_eq!(
                        SectionPath::parse(path.as_str()).assured("a production path is valid"),
                        path
                    );
                    assert!(
                        paths.insert(path),
                        "distinct kinds name distinct archive paths"
                    );
                }
                assert_eq!(contents.len(), kinds.len());
                assert_eq!(paths.len(), kinds.len());
            }
        });
}
