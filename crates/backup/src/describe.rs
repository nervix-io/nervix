//! What an archive holds, read and verified from a stream.
//!
//! A description decodes supported records, verifies every section, and keeps the place, size and digest
//! of the NSPL and resource archive sections without their bytes, so describing an archive of any
//! size holds only its records in memory. The contents a restore reads add each domain's NSPL,
//! which configuration size keeps small, and leave resource archives where they are, to be read
//! again by their place.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    io::Read,
};

use error_stack::Report;
use nervix_models::DomainName;

use crate::{
    error::ArchiveReadError,
    manifest::{
        ArchiveScope, BackupManifest, DomainCapture, SectionContent, SectionDigest, SectionEntry,
    },
    path::{MANIFEST_PATH, SectionPath},
    reader::{SectionReader, SectionVisitor, read_archive},
    records::{DomainRecord, ResourceVersionRecord, UsersRecord},
    section::{ArchiveRecord, MAX_RECORD_BYTES, RecordKind},
    state::{BranchLifecycleRecord, KafkaOffsetsRecord, WasmStateDescriptor},
};

/// Everything an archive holds, verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveDescription {
    pub manifest: BackupManifest,
    /// The users of a cluster archive. Absent for a domain archive, which holds none.
    pub users: Option<UsersRecord>,
    /// Every domain, in manifest order.
    pub domains: Vec<DescribedDomain>,
}

/// One domain of an archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedDomain {
    pub capture: DomainCapture,
    pub record: DomainRecord,
    /// The domain's `models.nspl`.
    pub models: DescribedSection,
    /// Every resource version, in archive order.
    pub resource_versions: Vec<DescribedResourceVersion>,
    /// State sections in archive order, verified and decoded without retaining guest blobs.
    pub state: Vec<DescribedRuntimeState>,
    /// Verified state sections whose record tag or version this reader cannot install.
    pub skipped_state: Vec<SkippedStateSection>,
}

/// A state section whose payload stays opaque because its record contract is unsupported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedStateSection {
    pub path: SectionPath,
    pub reason: SkippedStateReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SkippedStateReason {
    #[error("unknown record kind tag {found}")]
    UnknownKind { found: u16 },
    #[error("unsupported record version {found}; this reader supports {supported}")]
    UnsupportedVersion { found: u16, supported: u16 },
}

/// One verified runtime-state section or WASM descriptor and its guest blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DescribedRuntimeState {
    Wasm {
        descriptor: WasmStateDescriptor,
        record: DescribedSection,
        guest: DescribedSection,
    },
    KafkaOffsets {
        offsets: KafkaOffsetsRecord,
        record: DescribedSection,
    },
    BranchLifecycle {
        lifecycle: BranchLifecycleRecord,
        record: DescribedSection,
    },
}

/// One resource version of an archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedResourceVersion {
    pub record: ResourceVersionRecord,
    /// The version's original archive. Absent when the archive was taken `WITHOUT RESOURCES`, or
    /// the version never published one.
    pub archive: Option<DescribedSection>,
}

/// A section described by its place, size and digest, without its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedSection {
    pub path: SectionPath,
    /// Where the section's first byte sits in the archive, counted from the archive's first byte.
    pub offset: u64,
    pub length: u64,
    pub digest: SectionDigest,
}

impl DescribedSection {
    fn read_at(entry: &SectionEntry, content: &SectionReader<'_>) -> Self {
        Self {
            path: entry.path.clone(),
            offset: content.archive_offset(),
            length: entry.length,
            digest: entry.digest,
        }
    }
}

/// Everything a restore reads from an archive before it changes anything, verified: the
/// description, and the text of every domain's `models.nspl`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveContents {
    pub description: ArchiveDescription,
    /// Each domain's models as NSPL, keyed by the domain's name in the archive.
    pub models: BTreeMap<DomainName, String>,
}

/// The largest `models.nspl` a reader holds in memory.
const MAX_MODELS_BYTES: u64 = 64 * 1024 * 1024;

/// Reads and verifies the archive `reader` streams, and describes what it holds.
pub fn describe_archive<R: Read>(
    reader: R,
) -> Result<ArchiveDescription, Report<ArchiveReadError>> {
    let mut describer = Describer::default();
    let manifest = read_archive(reader, &mut describer)?;
    describer.finish(manifest)
}

/// Reads and verifies the archive `reader` streams, and returns its description with the text of
/// every domain's models.
pub fn read_archive_contents<R: Read>(
    reader: R,
) -> Result<ArchiveContents, Report<ArchiveReadError>> {
    let mut reader_state = ContentsReader {
        describer: Describer::default(),
        models: BTreeMap::new(),
    };
    let manifest = read_archive(reader, &mut reader_state)?;
    let description = reader_state.describer.finish(manifest)?;
    Ok(ArchiveContents {
        description,
        models: reader_state.models,
    })
}

/// A description being assembled, and the NSPL text of the domains read so far.
struct ContentsReader {
    describer: Describer,
    models: BTreeMap<DomainName, String>,
}

impl SectionVisitor for ContentsReader {
    fn manifest(&mut self, manifest: &BackupManifest) -> Result<(), Report<ArchiveReadError>> {
        self.describer.manifest(manifest)
    }

    fn section(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        let SectionContent::Nspl = entry.content else {
            return self.describer.section(entry, content);
        };
        let bytes = content.read_all(entry, MAX_MODELS_BYTES)?;
        let Ok(text) = String::from_utf8(bytes) else {
            return Err(Report::new(ArchiveReadError::InvalidText {
                path: entry.path.to_string(),
            }));
        };
        self.describer.section(entry, content)?;
        let Some(domain) = self.describer.models_paths.get(&entry.path) else {
            return Err(misplaced(entry));
        };
        self.models.insert(domain.clone(), text);
        Ok(())
    }
}

/// The domains a description is assembling, keyed by name.
#[derive(Default)]
struct Describer {
    users: Option<UsersRecord>,
    domains: BTreeMap<DomainName, AssembledDomain>,
    /// Where each domain's `models.nspl` belongs, from the domains the manifest names.
    models_paths: BTreeMap<SectionPath, DomainName>,
    /// Where the archive of each resource version already read belongs.
    archive_paths: BTreeMap<SectionPath, VersionSlot>,
    wasm_blob_paths: BTreeMap<SectionPath, (DomainName, usize)>,
    skipped_wasm_blobs: BTreeSet<SectionPath>,
}

/// A domain whose sections are still arriving.
struct AssembledDomain {
    capture: DomainCapture,
    record: Option<DomainRecord>,
    models: Option<DescribedSection>,
    resource_versions: Vec<DescribedResourceVersion>,
    state: Vec<AssembledRuntimeState>,
    skipped_state: Vec<SkippedStateSection>,
}

enum AssembledRuntimeState {
    Wasm {
        descriptor: WasmStateDescriptor,
        record: DescribedSection,
        guest: Option<DescribedSection>,
    },
    KafkaOffsets {
        offsets: KafkaOffsetsRecord,
        record: DescribedSection,
    },
    BranchLifecycle {
        lifecycle: BranchLifecycleRecord,
        record: DescribedSection,
    },
}

/// The resource version a resource archive section belongs to.
#[derive(Debug, Clone)]
struct VersionSlot {
    domain: DomainName,
    index: usize,
}

impl SectionVisitor for Describer {
    fn manifest(&mut self, manifest: &BackupManifest) -> Result<(), Report<ArchiveReadError>> {
        for capture in &manifest.domains {
            let assembled = AssembledDomain {
                capture: capture.clone(),
                record: None,
                models: None,
                resource_versions: Vec::new(),
                state: Vec::new(),
                skipped_state: Vec::new(),
            };
            match self.domains.entry(capture.domain.clone()) {
                Entry::Vacant(vacant) => {
                    vacant.insert(assembled);
                }
                Entry::Occupied(_) => {
                    return Err(Report::new(ArchiveReadError::InvalidValue {
                        path: MANIFEST_PATH.to_string(),
                        field: "domain list",
                    }));
                }
            }
            self.models_paths.insert(
                SectionPath::domain_models(&capture.domain),
                capture.domain.clone(),
            );
        }
        Ok(())
    }

    fn section(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        match entry.content {
            SectionContent::Record(RecordKind::Users) => self.users(entry, content),
            SectionContent::Record(RecordKind::Domain) => self.domain_record(entry, content),
            SectionContent::Record(RecordKind::ResourceVersion) => {
                self.resource_version(entry, content)
            }
            SectionContent::Record(RecordKind::WasmStateDescriptor) => {
                let decoded = self.wasm_descriptor(entry, content);
                self.state_record_or_skip(entry, decoded)
            }
            SectionContent::Record(RecordKind::KafkaOffsets) => {
                let decoded = self.kafka_offsets(entry, content);
                self.state_record_or_skip(entry, decoded)
            }
            SectionContent::Record(RecordKind::BranchLifecycle) => {
                let decoded = self.branch_lifecycle(entry, content);
                self.state_record_or_skip(entry, decoded)
            }
            SectionContent::Record(RecordKind::Manifest) => Err(misplaced(entry)),
            SectionContent::Nspl => self.models(entry, content),
            SectionContent::ResourceArchive => self.resource_archive(entry, content),
            SectionContent::WasmGuestBlob => self.wasm_guest_blob(entry, content),
        }
    }
}

impl Describer {
    fn state_record_or_skip(
        &mut self,
        entry: &SectionEntry,
        decoded: Result<(), Report<ArchiveReadError>>,
    ) -> Result<(), Report<ArchiveReadError>> {
        let Err(error) = decoded else {
            return Ok(());
        };
        let reason = match error.current_context() {
            ArchiveReadError::ForeignRecordKind { found, .. } => {
                SkippedStateReason::UnknownKind { found: *found }
            }
            ArchiveReadError::UnsupportedRecordVersion {
                found, supported, ..
            } => SkippedStateReason::UnsupportedVersion {
                found: *found,
                supported: *supported,
            },
            _ => return Err(error),
        };
        let domain = self
            .domains
            .keys()
            .find(|domain| {
                entry
                    .path
                    .as_str()
                    .starts_with(&format!("domains/{}/state/", domain.as_str()))
            })
            .cloned()
            .ok_or_else(|| misplaced(entry))?;
        if entry.content == SectionContent::Record(RecordKind::WasmStateDescriptor) {
            let prefix = entry
                .path
                .as_str()
                .strip_suffix("/descriptor.rkyv")
                .ok_or_else(|| misplaced(entry))?;
            let blob = SectionPath::parse(&format!("{prefix}/guest.bin"))?;
            self.skipped_wasm_blobs.insert(blob);
        }
        self.domains
            .get_mut(&domain)
            .ok_or_else(|| misplaced(entry))?
            .skipped_state
            .push(SkippedStateSection {
                path: entry.path.clone(),
                reason,
            });
        Ok(())
    }

    fn wasm_descriptor(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        let bytes = content.read_all(entry, MAX_RECORD_BYTES)?;
        let descriptor = WasmStateDescriptor::decode(entry.path.as_str(), &bytes)?;
        let expected = SectionPath::wasm_state_descriptor(
            &descriptor.domain,
            &descriptor.entity,
            descriptor.branch_fingerprint.as_ref(),
        );
        if entry.path != expected {
            return Err(misplaced(entry));
        }
        let Some(domain) = self.domains.get_mut(&descriptor.domain) else {
            return Err(misplaced(entry));
        };
        let index = domain.state.len();
        let blob_path = SectionPath::wasm_guest_blob(
            &descriptor.domain,
            &descriptor.entity,
            descriptor.branch_fingerprint.as_ref(),
        );
        if self
            .wasm_blob_paths
            .insert(blob_path, (descriptor.domain.clone(), index))
            .is_some()
        {
            return Err(misplaced(entry));
        }
        domain.state.push(AssembledRuntimeState::Wasm {
            descriptor,
            record: DescribedSection::read_at(entry, content),
            guest: None,
        });
        Ok(())
    }

    fn kafka_offsets(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        let bytes = content.read_all(entry, MAX_RECORD_BYTES)?;
        let offsets = KafkaOffsetsRecord::decode(entry.path.as_str(), &bytes)?;
        if entry.path != SectionPath::kafka_offsets(&offsets.domain, &offsets.entity) {
            return Err(misplaced(entry));
        }
        let Some(domain) = self.domains.get_mut(&offsets.domain) else {
            return Err(misplaced(entry));
        };
        domain.state.push(AssembledRuntimeState::KafkaOffsets {
            offsets,
            record: DescribedSection::read_at(entry, content),
        });
        Ok(())
    }

    fn branch_lifecycle(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        let bytes = content.read_all(entry, MAX_RECORD_BYTES)?;
        let lifecycle = BranchLifecycleRecord::decode(entry.path.as_str(), &bytes)?;
        if entry.path
            != SectionPath::branch_lifecycle(
                &lifecycle.domain,
                lifecycle.owner_kind,
                &lifecycle.entity,
            )
        {
            return Err(misplaced(entry));
        }
        let Some(domain) = self.domains.get_mut(&lifecycle.domain) else {
            return Err(misplaced(entry));
        };
        domain.state.push(AssembledRuntimeState::BranchLifecycle {
            lifecycle,
            record: DescribedSection::read_at(entry, content),
        });
        Ok(())
    }

    fn wasm_guest_blob(
        &mut self,
        entry: &SectionEntry,
        content: &SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        if self.skipped_wasm_blobs.remove(&entry.path) {
            return Ok(());
        }
        let Some((domain, index)) = self.wasm_blob_paths.get(&entry.path) else {
            return Err(misplaced(entry));
        };
        let Some(state) = self
            .domains
            .get_mut(domain)
            .and_then(|domain| domain.state.get_mut(*index))
        else {
            return Err(misplaced(entry));
        };
        let AssembledRuntimeState::Wasm { guest, .. } = state else {
            return Err(misplaced(entry));
        };
        if guest.is_some() {
            return Err(misplaced(entry));
        }
        *guest = Some(DescribedSection::read_at(entry, content));
        Ok(())
    }

    fn users(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        if entry.path != SectionPath::users() || self.users.is_some() {
            return Err(misplaced(entry));
        }
        let bytes = content.read_all(entry, MAX_RECORD_BYTES)?;
        self.users = Some(UsersRecord::decode(entry.path.as_str(), &bytes)?);
        Ok(())
    }

    fn domain_record(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        let bytes = content.read_all(entry, MAX_RECORD_BYTES)?;
        let record = DomainRecord::decode(entry.path.as_str(), &bytes)?;
        if entry.path != SectionPath::domain_record(&record.domain) {
            return Err(misplaced(entry));
        }
        let Some(assembled) = self.domains.get_mut(&record.domain) else {
            return Err(misplaced(entry));
        };
        if assembled.record.is_some() {
            return Err(misplaced(entry));
        }
        assembled.record = Some(record);
        Ok(())
    }

    fn models(
        &mut self,
        entry: &SectionEntry,
        content: &SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        let Some(domain) = self.models_paths.get(&entry.path) else {
            return Err(misplaced(entry));
        };
        let Some(assembled) = self.domains.get_mut(domain) else {
            return Err(misplaced(entry));
        };
        if assembled.models.is_some() {
            return Err(misplaced(entry));
        }
        assembled.models = Some(DescribedSection::read_at(entry, content));
        Ok(())
    }

    fn resource_version(
        &mut self,
        entry: &SectionEntry,
        content: &mut SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        let bytes = content.read_all(entry, MAX_RECORD_BYTES)?;
        let record = ResourceVersionRecord::decode(entry.path.as_str(), &bytes)?;
        let expected =
            SectionPath::resource_version_record(&record.domain, &record.resource, record.version);
        if entry.path != expected {
            return Err(misplaced(entry));
        }
        let archive_path =
            SectionPath::resource_archive(&record.domain, &record.resource, record.version);
        let domain = record.domain.clone();
        let Some(assembled) = self.domains.get_mut(&domain) else {
            return Err(misplaced(entry));
        };
        let index = assembled.resource_versions.len();
        assembled.resource_versions.push(DescribedResourceVersion {
            record,
            archive: None,
        });
        self.archive_paths
            .insert(archive_path, VersionSlot { domain, index });
        Ok(())
    }

    fn resource_archive(
        &mut self,
        entry: &SectionEntry,
        content: &SectionReader<'_>,
    ) -> Result<(), Report<ArchiveReadError>> {
        let Some(slot) = self.archive_paths.get(&entry.path) else {
            return Err(misplaced(entry));
        };
        let Some(assembled) = self.domains.get_mut(&slot.domain) else {
            return Err(misplaced(entry));
        };
        let Some(version) = assembled.resource_versions.get_mut(slot.index) else {
            return Err(misplaced(entry));
        };
        if version.archive.is_some() {
            return Err(misplaced(entry));
        }
        version.archive = Some(DescribedSection::read_at(entry, content));
        Ok(())
    }

    /// Assembles the description once every section has been read, and refuses an archive whose
    /// sections do not cover what its manifest names.
    fn finish(
        mut self,
        manifest: BackupManifest,
    ) -> Result<ArchiveDescription, Report<ArchiveReadError>> {
        match (&manifest.scope, &self.users) {
            (ArchiveScope::Cluster, None) => {
                return Err(Report::new(ArchiveReadError::MissingUsers));
            }
            (ArchiveScope::Domain(_), Some(_)) => {
                return Err(Report::new(ArchiveReadError::UnexpectedSection {
                    path: SectionPath::users().to_string(),
                }));
            }
            (ArchiveScope::Cluster, Some(_)) | (ArchiveScope::Domain(_), None) => {}
        }
        let mut domains = Vec::with_capacity(manifest.domains.len());
        for capture in &manifest.domains {
            let assembled = self
                .domains
                .remove(&capture.domain)
                .ok_or_else(|| incomplete(&capture.domain, "domain entry"))?;
            let Some(record) = assembled.record else {
                return Err(incomplete(&capture.domain, "domain record"));
            };
            let Some(models) = assembled.models else {
                return Err(incomplete(&capture.domain, "models.nspl"));
            };
            domains.push(DescribedDomain {
                capture: assembled.capture,
                record,
                models,
                resource_versions: assembled.resource_versions,
                state: assembled
                    .state
                    .into_iter()
                    .map(|state| match state {
                        AssembledRuntimeState::Wasm {
                            descriptor,
                            record,
                            guest: Some(guest),
                        } => Ok(DescribedRuntimeState::Wasm {
                            descriptor,
                            record,
                            guest,
                        }),
                        AssembledRuntimeState::Wasm { guest: None, .. } => {
                            Err(incomplete(&capture.domain, "WASM guest blob"))
                        }
                        AssembledRuntimeState::KafkaOffsets { offsets, record } => {
                            Ok(DescribedRuntimeState::KafkaOffsets { offsets, record })
                        }
                        AssembledRuntimeState::BranchLifecycle { lifecycle, record } => {
                            Ok(DescribedRuntimeState::BranchLifecycle { lifecycle, record })
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                skipped_state: assembled.skipped_state,
            });
        }
        Ok(ArchiveDescription {
            manifest,
            users: self.users,
            domains,
        })
    }
}

fn misplaced(entry: &SectionEntry) -> Report<ArchiveReadError> {
    Report::new(ArchiveReadError::MisplacedSection {
        path: entry.path.to_string(),
    })
}

fn incomplete(domain: &DomainName, missing: &'static str) -> Report<ArchiveReadError> {
    Report::new(ArchiveReadError::IncompleteDomain {
        domain: domain.to_string(),
        missing,
    })
}
