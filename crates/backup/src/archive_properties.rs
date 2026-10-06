//! Complete archive fidelity through production export, streaming extraction and restore reads.
//!
//! Layer: test harness.
//! - **Owns.** Complete original-value oracles, section order/bytes and deterministic re-export.
//! - **Depends on.** Current public archive encoders/readers and bounded synthetic archive values.
//! - **Must not know.** Runtime ownership assignment, installation generations or cut synchronization.

use std::{collections::BTreeMap, fmt::Debug, io::Read as _};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};

use crate::{
    ArchiveContents, ArchiveDescription, ArchiveLayout, ArchiveReadError, ArchiveRecord,
    BackupManifest, DescribedDomain, DescribedMaterializedGroup, DescribedResourceVersion,
    DescribedRuntimeState, DescribedSection, DescribedWindowGroup, SectionEntry, SectionPath,
    SectionReader, SectionVisitor,
    archive_values::{Case, Section, State, Values},
    describe_archive, read_archive, read_archive_contents,
};

pub(super) fn assert_record<R: ArchiveRecord + PartialEq + Debug>(record: &R) {
    let bytes = record.encode().assured("a bounded current record encodes");
    let decoded = R::decode("synthetic.rkyv", &bytes)
        .assured("the owning validator accepts a generated current record");
    assert_eq!(&decoded, record);
    assert_eq!(decoded.encode().assured("a decoded record encodes"), bytes);
}

#[derive(Default)]
struct Collector {
    manifest: Option<BackupManifest>,
    sections: Vec<Section>,
}

impl SectionVisitor for Collector {
    fn manifest(&mut self, manifest: &BackupManifest) -> Result<(), Report<ArchiveReadError>> {
        self.manifest = Some(manifest.clone());
        Ok(())
    }

    fn section(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        self.sections.push(Section {
            entry: entry.clone(),
            bytes: content.read_all(entry, 64 * 1024 * 1024)?,
        });
        Ok(())
    }
}

impl Case {
    pub(super) fn export(&self) -> Vec<u8> {
        let layout =
            ArchiveLayout::new(self.manifest.clone()).assured("the generated manifest lays out");
        let mut archive = Vec::new();
        let mut sections = self.sections.iter();
        layout
            .write_to(&mut archive, |entry, sink| {
                let section = sections
                    .next()
                    .assured("each generated manifest entry has one section");
                assert_eq!(entry, &section.entry);
                std::io::Write::write_all(sink, &section.bytes)
            })
            .assured("the generated archive exports");
        assert!(sections.next().is_none());
        assert_eq!(
            u64::try_from(archive.len()).assured("bounded archives fit 64 bits"),
            layout.total_bytes()
        );
        archive
    }

    pub(super) fn assert_records(&self) {
        assert_record(&self.manifest);
        if let Some(users) = &self.users {
            assert_record(users);
        }
        for domain in &self.domains {
            assert_record(&domain.record);
            for resource in &domain.resources {
                assert_record(&resource.record);
            }
            for state in &domain.state {
                match state {
                    State::Wasm { descriptor, .. } => assert_record(descriptor),
                    State::Kafka(record) => assert_record(record),
                    State::Lifecycle(record) => assert_record(record),
                    State::Materialized(value) => {
                        assert_record(&value.descriptor);
                        for group in &value.groups {
                            assert_record(&group.identities);
                        }
                    }
                    State::Deduplicator(value) => assert_record(&value.descriptor),
                    State::Window(value) => assert_record(&value.descriptor),
                }
            }
        }
    }

    fn section_locations(&self, bytes: &[u8]) -> BTreeMap<SectionPath, DescribedSection> {
        let mut archive = tar::Archive::new(bytes);
        let mut entries = archive.entries().assured("the generated archive is tar");
        let first = entries
            .next()
            .assured("the archive begins with a manifest")
            .assured("the manifest entry reads");
        assert_eq!(first.path_bytes(), b"manifest.rkyv".as_slice());
        let mut locations = BTreeMap::new();
        for original in &self.sections {
            let mut entry = entries
                .next()
                .assured("tar retains every section")
                .assured("the section reads");
            assert_eq!(entry.path_bytes(), original.entry.path.as_str().as_bytes());
            let header = entry.header();
            assert_eq!(header.entry_type(), tar::EntryType::Regular);
            assert_eq!(header.mode().assured("the entry mode reads"), 0o600);
            assert_eq!(header.uid().assured("the entry owner reads"), 0);
            assert_eq!(header.gid().assured("the entry group reads"), 0);
            assert_eq!(header.mtime().assured("the entry timestamp reads"), 0);
            assert_eq!(entry.size(), original.entry.length);
            let offset = entry.raw_file_position();
            let mut extracted = Vec::new();
            entry
                .read_to_end(&mut extracted)
                .assured("a bounded synthetic section reads");
            assert_eq!(extracted, original.bytes);
            let start =
                usize::try_from(offset).assured("a generated archive fits the address space");
            let end = start
                .checked_add(original.bytes.len())
                .assured("a generated section fits inside its archive");
            assert_eq!(&bytes[start..end], original.bytes);
            locations.insert(
                original.entry.path.clone(),
                DescribedSection {
                    path: original.entry.path.clone(),
                    offset,
                    length: original.entry.length,
                    digest: original.entry.digest,
                },
            );
        }
        assert!(entries.next().is_none());
        locations
    }

    fn expected_contents(
        &self,
        locations: &BTreeMap<SectionPath, DescribedSection>,
    ) -> ArchiveContents {
        let location = |path: SectionPath| {
            locations
                .get(&path)
                .assured("the original section has a tar entry")
                .clone()
        };
        let mut domains = Vec::new();
        let mut models = BTreeMap::new();
        for original in &self.domains {
            let mut resource_versions = Vec::new();
            for resource in &original.resources {
                let record = &resource.record;
                let archive = if resource.bytes.is_some() {
                    Some(location(SectionPath::resource_archive(
                        &record.domain,
                        &record.resource,
                        record.version,
                    )))
                } else {
                    None
                };
                resource_versions.push(DescribedResourceVersion {
                    record: record.clone(),
                    archive,
                });
            }
            let mut state = Vec::new();
            for original_state in &original.state {
                state.push(match original_state {
                    State::Wasm { descriptor, .. } => DescribedRuntimeState::Wasm {
                        descriptor: descriptor.clone(),
                        record: location(SectionPath::wasm_state_descriptor(
                            &descriptor.domain,
                            &descriptor.entity,
                            descriptor.branch_fingerprint.as_ref(),
                        )),
                        guest: location(SectionPath::wasm_guest_blob(
                            &descriptor.domain,
                            &descriptor.entity,
                            descriptor.branch_fingerprint.as_ref(),
                        )),
                    },
                    State::Kafka(offsets) => DescribedRuntimeState::KafkaOffsets {
                        offsets: offsets.clone(),
                        record: location(SectionPath::kafka_offsets(
                            &offsets.domain,
                            &offsets.entity,
                        )),
                    },
                    State::Lifecycle(lifecycle) => DescribedRuntimeState::BranchLifecycle {
                        lifecycle: lifecycle.clone(),
                        record: location(SectionPath::branch_lifecycle(
                            &lifecycle.domain,
                            lifecycle.owner_kind,
                            &lifecycle.entity,
                        )),
                    },
                    State::Materialized(value) => DescribedRuntimeState::Materialized {
                        descriptor: value.descriptor.clone(),
                        record: location(SectionPath::materialized_descriptor(
                            &value.descriptor.domain,
                            &value.descriptor.entity,
                        )),
                        groups: value
                            .groups
                            .iter()
                            .map(|group| {
                                let identities = &group.identities;
                                DescribedMaterializedGroup {
                                    identities: location(SectionPath::materialized_identities(
                                        &identities.domain,
                                        &identities.entity,
                                        identities.group,
                                    )),
                                    columns: location(SectionPath::materialized_columns(
                                        &identities.domain,
                                        &identities.entity,
                                        identities.group,
                                    )),
                                    record_count: u64::try_from(identities.identities.len())
                                        .assured("bounded identity counts fit"),
                                }
                            })
                            .collect(),
                    },
                    State::Deduplicator(value) => {
                        let descriptor = &value.descriptor;
                        let branch = descriptor.branch_fingerprint.as_ref();
                        let mut groups = Vec::with_capacity(value.groups.len());
                        for index in 0..value.groups.len() {
                            let group = u32::try_from(index).assured("bounded group counts fit");
                            groups.push(location(SectionPath::deduplicator_keys(
                                &descriptor.domain,
                                &descriptor.entity,
                                branch,
                                group,
                            )));
                        }
                        DescribedRuntimeState::Deduplicator {
                            descriptor: descriptor.clone(),
                            record: location(SectionPath::deduplicator_descriptor(
                                &descriptor.domain,
                                &descriptor.entity,
                                branch,
                            )),
                            groups,
                        }
                    }
                    State::Window(value) => {
                        let descriptor = &value.descriptor;
                        let branch = descriptor.branch_fingerprint.as_ref();
                        let mut groups = Vec::with_capacity(value.groups.len());
                        for index in 0..value.groups.len() {
                            let group = u32::try_from(index).assured("bounded group counts fit");
                            groups.push(DescribedWindowGroup {
                                input: location(SectionPath::window_input_rows(
                                    &descriptor.domain,
                                    &descriptor.entity,
                                    branch,
                                    group,
                                )),
                                arguments: location(SectionPath::window_argument_columns(
                                    &descriptor.domain,
                                    &descriptor.entity,
                                    branch,
                                    group,
                                )),
                            });
                        }
                        DescribedRuntimeState::Window {
                            descriptor: descriptor.clone(),
                            record: location(SectionPath::window_descriptor(
                                &descriptor.domain,
                                &descriptor.entity,
                                branch,
                            )),
                            groups,
                        }
                    }
                });
            }
            domains.push(DescribedDomain {
                capture: original.capture.clone(),
                record: original.record.clone(),
                models: location(SectionPath::domain_models(&original.record.domain)),
                resource_versions,
                state,
                skipped_state: Vec::new(),
            });
            models.insert(original.record.domain.clone(), original.models.clone());
        }
        ArchiveContents {
            description: ArchiveDescription {
                manifest: self.manifest.clone(),
                users: self.users.clone(),
                domains,
            },
            models,
        }
    }

    fn assert_archive(&self) {
        self.assert_records();
        let bytes = self.export();
        let locations = self.section_locations(&bytes);
        let mut extracted = Collector::default();
        let manifest = read_archive(bytes.as_slice(), &mut extracted)
            .assured("the entire current archive verifies");
        assert_eq!(manifest, self.manifest);
        assert_eq!(extracted.manifest, Some(self.manifest.clone()));
        assert_eq!(extracted.sections, self.sections);
        let expected = self.expected_contents(&locations);
        assert_eq!(
            read_archive_contents(bytes.as_slice())
                .assured("the restore reader accepts the entire archive"),
            expected
        );
        assert_eq!(
            describe_archive(bytes.as_slice())
                .assured("the description accepts the entire archive"),
            expected.description
        );

        // Re-export verified extracted values. Runtime restore assigns ownership and installation
        // generations separately; those transformations are exercised through public scenarios.
        let layout = ArchiveLayout::new(manifest).assured("the extracted manifest lays out");
        let mut sections = extracted.sections.iter();
        let mut reexported = Vec::new();
        layout
            .write_to(&mut reexported, |entry, sink| {
                let section = sections
                    .next()
                    .assured("the extracted section sequence is complete");
                assert_eq!(entry, &section.entry);
                std::io::Write::write_all(sink, &section.bytes)
            })
            .assured("the extracted archive re-exports");
        assert_eq!(reexported, bytes);
    }
}

#[test]
fn bolero_complete_archives_preserve_every_record_section_and_payload() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes| {
            let mut values = Values::new(bytes);
            for cluster in [false, true] {
                for included in [false, true] {
                    let cut = values.0.entropy().byte();
                    values.case(cluster, included, cut).assert_archive();
                }
            }
        });
}

#[test]
fn every_current_section_and_cut_has_a_complete_archive_oracle() {
    for byte in [0, 1, 2, 127, 255] {
        let bytes = [byte; 4096];
        for cut in 0..4 {
            for cluster in [false, true] {
                for resources in [false, true] {
                    Values::new(&bytes)
                        .case(cluster, resources, cut)
                        .assert_archive();
                }
            }
        }
    }
}
