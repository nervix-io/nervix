//! What an archive holds, read and verified from a stream.
//!
//! A description decodes every record, verifies every section, and keeps the place, size and digest
//! of the NSPL and resource archive sections without their bytes, so describing an archive of any
//! size holds only its records in memory. The contents a restore reads add each domain's NSPL,
//! which configuration size keeps small, and leave resource archives where they are, to be read
//! again by their place.

use std::{
    collections::{BTreeMap, btree_map::Entry},
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
}

/// A domain whose sections are still arriving.
struct AssembledDomain {
    capture: DomainCapture,
    record: Option<DomainRecord>,
    models: Option<DescribedSection>,
    resource_versions: Vec<DescribedResourceVersion>,
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
            SectionContent::Record(RecordKind::Manifest) => Err(misplaced(entry)),
            SectionContent::Nspl => self.models(entry, content),
            SectionContent::ResourceArchive => self.resource_archive(entry, content),
        }
    }
}

impl Describer {
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
