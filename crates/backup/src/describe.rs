//! What an archive holds, read and verified from a stream.
//!
//! A description decodes every record, verifies every section, and keeps the size and digest of
//! the NSPL and resource archive sections without their bytes, so describing an archive of any size
//! holds only its records in memory.

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
    pub length: u64,
    pub digest: SectionDigest,
}

impl From<&SectionEntry> for DescribedSection {
    fn from(entry: &SectionEntry) -> Self {
        Self {
            path: entry.path.clone(),
            length: entry.length,
            digest: entry.digest,
        }
    }
}

/// Reads and verifies the archive `reader` streams, and describes what it holds.
pub fn describe_archive<R: Read>(
    reader: R,
) -> Result<ArchiveDescription, Report<ArchiveReadError>> {
    let mut describer = Describer::default();
    let manifest = read_archive(reader, &mut describer)?;
    describer.finish(manifest)
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
            SectionContent::Nspl => self.models(entry),
            SectionContent::ResourceArchive => self.resource_archive(entry),
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

    fn models(&mut self, entry: &SectionEntry) -> Result<(), Report<ArchiveReadError>> {
        let Some(domain) = self.models_paths.get(&entry.path) else {
            return Err(misplaced(entry));
        };
        let Some(assembled) = self.domains.get_mut(domain) else {
            return Err(misplaced(entry));
        };
        if assembled.models.is_some() {
            return Err(misplaced(entry));
        }
        assembled.models = Some(DescribedSection::from(entry));
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

    fn resource_archive(&mut self, entry: &SectionEntry) -> Result<(), Report<ArchiveReadError>> {
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
        version.archive = Some(DescribedSection::from(entry));
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
