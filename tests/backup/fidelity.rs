//! Complete public restore/re-export fidelity with the documented restore transformations.
//!
//! Layer: test harness.
//! - **Owns.** Archive membership, complete record and raw-byte comparisons across a public restore.
//! - **Depends on.** Public archive records/readers and the scenario's CLI-produced archive files.
//! - **Must not know.** Restore staging, private stores or graph installation internals.

use std::os::unix::fs::PermissionsExt as _;

use nervix_backup::{
    ArchiveRecord, BranchLifecycleRecord, DomainRecord, KafkaOffsetsRecord,
    MaterializedIdentitiesRecord, MaterializedRelayDescriptor, RecordKind, ResourceVersionRecord,
    SectionPath, WasmStateDescriptor, read_archive_contents,
};
use nervix_models::{DomainStatus, WasmStateGeneration};

use super::*;

#[derive(Debug, PartialEq, Eq)]
struct SectionValue {
    content: SectionContent,
    bytes: Vec<u8>,
}

#[derive(Default)]
struct ArchiveValues {
    sections: BTreeMap<SectionPath, SectionValue>,
}

impl SectionVisitor for ArchiveValues {
    fn manifest(
        &mut self,
        _manifest: &nervix_backup::BackupManifest,
    ) -> Result<(), error_stack::Report<nervix_backup::ArchiveReadError>> {
        Ok(())
    }

    fn section(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), error_stack::Report<nervix_backup::ArchiveReadError>> {
        let bytes = content.read_all(entry, MODELS_LIMIT)?;
        assert_eq!(
            u64::try_from(bytes.len()).assured("scenario sections fit 64 bits"),
            entry.length
        );
        assert_eq!(
            nervix_backup::SectionDigester::digest_of(&bytes),
            entry.digest
        );
        assert!(
            self.sections
                .insert(
                    entry.path.clone(),
                    SectionValue {
                        content: entry.content,
                        bytes
                    }
                )
                .is_none()
        );
        Ok(())
    }
}

impl ArchiveValues {
    fn read(path: &Path) -> Self {
        let mode = std::fs::metadata(path)
            .assured("the CLI wrote its archive")
            .permissions()
            .mode();
        assert_eq!(
            mode & 0o777,
            0o600,
            "the complete archive remains sensitive"
        );
        let mut values = Self::default();
        let file = std::fs::File::open(path).assured("the CLI archive exists");
        read_archive(file, &mut values).assured("every CLI archive section verifies");
        values
    }

    fn restored(self, target: &DomainName) -> Self {
        let mut expected = Self::default();
        let mut payload_paths = BTreeMap::new();
        // Match record kinds before raw payloads; their descriptors own the target path.
        for (path, section) in &self.sections {
            let SectionContent::Record(kind) = section.content else {
                continue;
            };
            let (target_path, bytes) = match kind {
                RecordKind::Domain => {
                    let mut record = DomainRecord::decode(path.as_str(), &section.bytes)
                        .assured("the original domain record validates");
                    record.domain = target.clone();
                    record.status = DomainStatus::Stopped;
                    record.clock = None;
                    record.logical_frontier = None;
                    (
                        SectionPath::domain_record(target),
                        record
                            .encode()
                            .assured("the stopped-domain expectation encodes"),
                    )
                }
                RecordKind::ResourceVersion => {
                    let mut record = ResourceVersionRecord::decode(path.as_str(), &section.bytes)
                        .assured("the original resource record validates");
                    let source_path = SectionPath::resource_archive(
                        &record.domain,
                        &record.resource,
                        record.version,
                    );
                    let target_path =
                        SectionPath::resource_archive(target, &record.resource, record.version);
                    payload_paths.insert(source_path, target_path);
                    record.domain = target.clone();
                    (
                        SectionPath::resource_version_record(
                            target,
                            &record.resource,
                            record.version,
                        ),
                        record
                            .encode()
                            .assured("the imported-resource expectation encodes"),
                    )
                }
                RecordKind::WasmStateDescriptor => {
                    let mut record = WasmStateDescriptor::decode(path.as_str(), &section.bytes)
                        .assured("the original WASM descriptor validates");
                    payload_paths.insert(
                        SectionPath::wasm_guest_blob(
                            &record.domain,
                            &record.entity,
                            record.branch_fingerprint.as_ref(),
                        ),
                        SectionPath::wasm_guest_blob(
                            target,
                            &record.entity,
                            record.branch_fingerprint.as_ref(),
                        ),
                    );
                    record.domain = target.clone();
                    // RESTORE creates the guest lifetime owned by the newly published schedule.
                    record.generation = WasmStateGeneration::FIRST;
                    (
                        SectionPath::wasm_state_descriptor(
                            target,
                            &record.entity,
                            record.branch_fingerprint.as_ref(),
                        ),
                        record
                            .encode()
                            .assured("the restored-descriptor expectation encodes"),
                    )
                }
                RecordKind::KafkaOffsets => {
                    let mut record = KafkaOffsetsRecord::decode(path.as_str(), &section.bytes)
                        .assured("the original offsets validate");
                    record.domain = target.clone();
                    (
                        SectionPath::kafka_offsets(target, &record.entity),
                        record
                            .encode()
                            .assured("the restored-offset expectation encodes"),
                    )
                }
                RecordKind::BranchLifecycle => {
                    let mut record = BranchLifecycleRecord::decode(path.as_str(), &section.bytes)
                        .assured("the original branch lifecycle validates");
                    record.domain = target.clone();
                    (
                        SectionPath::branch_lifecycle(target, record.owner_kind, &record.entity),
                        record
                            .encode()
                            .assured("the restored-lifecycle expectation encodes"),
                    )
                }
                RecordKind::MaterializedRelayDescriptor => {
                    let mut record =
                        MaterializedRelayDescriptor::decode(path.as_str(), &section.bytes)
                            .assured("the original materialized descriptor validates");
                    for group in 0..record.groups {
                        payload_paths.insert(
                            SectionPath::materialized_columns(
                                &record.domain,
                                &record.entity,
                                group,
                            ),
                            SectionPath::materialized_columns(target, &record.entity, group),
                        );
                    }
                    let target_path = SectionPath::materialized_descriptor(target, &record.entity);
                    // RESTORE clears source-process authority. A stopped domain re-exports
                    // the stored checkpoint, whose fence is explicitly reset to zero.
                    record.fence = 0;
                    record.domain = target.clone();
                    (
                        target_path,
                        record
                            .encode()
                            .assured("the restored generation expectation encodes"),
                    )
                }
                RecordKind::MaterializedIdentities => {
                    let mut record =
                        MaterializedIdentitiesRecord::decode(path.as_str(), &section.bytes)
                            .assured("the original materialized identities validate");
                    record.domain = target.clone();
                    (
                        SectionPath::materialized_identities(target, &record.entity, record.group),
                        record
                            .encode()
                            .assured("the restored identity expectation encodes"),
                    )
                }
                RecordKind::Manifest | RecordKind::Users => {
                    panic!("a domain archive has domain-owned sections")
                }
            };
            assert!(
                expected
                    .sections
                    .insert(
                        target_path,
                        SectionValue {
                            content: section.content,
                            bytes
                        }
                    )
                    .is_none()
            );
        }
        for (path, section) in self.sections {
            let target_path = match section.content {
                SectionContent::Record(_) => continue,
                SectionContent::Nspl => SectionPath::domain_models(target),
                SectionContent::ResourceArchive
                | SectionContent::WasmGuestBlob
                | SectionContent::MaterializedColumns => payload_paths
                    .remove(&path)
                    .assured("the raw payload has its original owning descriptor"),
            };
            assert!(expected.sections.insert(target_path, section).is_none());
        }
        expected
    }
}

#[then(
    expr = "backup archives {string} and {string} preserve complete domain {string} restored as \
            stopped domain {string}"
)]
fn then_complete_restored_archive_matches(
    world: &mut ScenarioWorld,
    source_file: String,
    restored_file: String,
    source: String,
    target: String,
) {
    let source = scenario_domain(world, &source);
    let target = scenario_domain(world, &target);
    let source_path = archive_path(world, &source_file);
    let restored_path = archive_path(world, &restored_file);
    let original = read_archive_contents(
        std::fs::File::open(&source_path).assured("the source archive exists"),
    )
    .assured("the source archive validates");
    let restored = read_archive_contents(
        std::fs::File::open(&restored_path).assured("the restored archive exists"),
    )
    .assured("the re-exported archive validates");
    assert_eq!(
        original.description.manifest.scope,
        nervix_backup::ArchiveScope::Domain(source)
    );
    assert_eq!(
        restored.description.manifest.scope,
        nervix_backup::ArchiveScope::Domain(target.clone())
    );
    assert_eq!(
        restored.description.manifest.language_version,
        original.description.manifest.language_version
    );
    assert_eq!(
        restored.description.manifest.resources,
        original.description.manifest.resources
    );
    assert_eq!(restored.description.manifest.domains.len(), 1);
    assert_eq!(
        restored.description.manifest.domains[0].cut,
        nervix_models::BackupCut::Stopped
    );
    assert!(restored.description.domains[0].skipped_state.is_empty());
    let actual = ArchiveValues::read(&restored_path);
    let expected = ArchiveValues::read(&source_path).restored(&target);
    assert_eq!(
        actual.sections, expected.sections,
        "restore preserves every promised field, ordered branch entry, catalog value and raw \
         payload under its explicit domain/lifecycle transformations"
    );
}
