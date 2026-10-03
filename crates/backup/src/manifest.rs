//! The manifest: what an archive is, who wrote it, and every section it holds.

use std::{collections::BTreeSet, fmt};

use error_stack::Report;
use nervix_models::{BackupCut, BackupQuiesceCounters, BackupResources, DomainName, Timestamp};

use crate::{
    error::{ArchiveReadError, ArchiveWriteError},
    path::{MANIFEST_PATH, SectionPath},
    section::{ArchiveRecord, RecordKind, decode_record, encode_record},
    wire::{
        CutWire, DomainCaptureWire, ManifestWire, ResourcesWire, ScopeWire, SectionContentWire,
        SectionEntryWire,
    },
};

/// The archive format major version this crate writes and reads. A reader refuses any other: a
/// new major is a format this reader cannot interpret.
pub const ARCHIVE_FORMAT_MAJOR: u16 = 1;

/// The manifest record's format version.
const MANIFEST_VERSION: u16 = 2;

/// What an archive is and every section it holds, in the order the archive holds them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupManifest {
    /// The Nervix release that wrote the archive.
    pub producer_version: String,
    /// The NSPL release its `models.nspl` sections are written in.
    pub language_version: String,
    /// The cluster the archive was taken from.
    pub cluster_id: String,
    /// When the archive's contents were read.
    pub captured_at: Timestamp,
    pub scope: ArchiveScope,
    /// Whether resource versions carry their original archives.
    pub resources: BackupResources,
    /// Every domain the archive holds, with the revision it was read at, in name order.
    pub domains: Vec<DomainCapture>,
    /// Every section after the manifest, in archive order.
    pub sections: Vec<SectionEntry>,
}

/// What an archive covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArchiveScope {
    /// Every user and every domain.
    Cluster,
    /// One domain.
    Domain(DomainName),
}

/// The configuration of one domain as a backup read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainCapture {
    pub domain: DomainName,
    /// The applied consensus revision the domain was read at.
    pub revision: u64,
    /// The Raft log entry that revision is.
    pub raft_log: RaftLogPosition,
    /// The consistency boundary of the domain state in this archive.
    pub cut: BackupCut,
}

/// One entry of the Raft log, by the term that wrote it and its index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftLogPosition {
    pub term: u64,
    pub index: u64,
}

/// One section of an archive, as its manifest describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionEntry {
    pub path: SectionPath,
    pub content: SectionContent,
    pub length: u64,
    pub digest: SectionDigest,
}

/// What a section's bytes are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectionContent {
    /// A record this crate owns, behind its header.
    Record(RecordKind),
    /// Canonical NSPL text.
    Nspl,
    /// The original archive of a resource version, byte for byte.
    ResourceArchive,
    /// An opaque guest save, whose descriptor is an archive-owned record.
    WasmGuestBlob,
}

impl SectionContent {
    /// The largest section of this content a reader accepts, when its kind has a bound of its own.
    pub(crate) fn length_limit(self) -> Option<u64> {
        match self {
            Self::Record(_) => Some(crate::section::MAX_RECORD_BYTES),
            Self::Nspl | Self::ResourceArchive | Self::WasmGuestBlob => None,
        }
    }
}

/// The BLAKE3 digest of one section's bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SectionDigest([u8; 32]);

impl SectionDigest {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A digest reads as lowercase hexadecimal, the form `b3sum` prints.
impl fmt::Display for SectionDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for SectionDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "SectionDigest({self})")
    }
}

impl BackupManifest {
    /// Refuses a manifest that names one section twice.
    pub(crate) fn ensure_unique_sections(&self) -> Result<(), Report<ArchiveWriteError>> {
        let mut seen = BTreeSet::new();
        for entry in &self.sections {
            if !seen.insert(&entry.path) {
                return Err(Report::new(ArchiveWriteError::DuplicateSection {
                    path: entry.path.to_string(),
                }));
            }
        }
        Ok(())
    }
}

impl ArchiveRecord for BackupManifest {
    const KIND: RecordKind = RecordKind::Manifest;
    const VERSION: u16 = MANIFEST_VERSION;

    fn encode(&self) -> Result<Vec<u8>, Report<ArchiveWriteError>> {
        encode_record(Self::KIND, Self::VERSION, &ManifestWire::from(self))
    }

    fn decode(path: &str, bytes: &[u8]) -> Result<Self, Report<ArchiveReadError>> {
        let wire: ManifestWire = decode_record(path, Self::KIND, Self::VERSION, bytes)?;
        Self::from_wire(wire)
    }
}

impl From<&BackupManifest> for ManifestWire {
    fn from(manifest: &BackupManifest) -> Self {
        let scope = match &manifest.scope {
            ArchiveScope::Cluster => ScopeWire::Cluster,
            ArchiveScope::Domain(domain) => ScopeWire::Domain(domain.as_str().to_string()),
        };
        let domains = manifest
            .domains
            .iter()
            .map(|capture| DomainCaptureWire {
                domain: capture.domain.as_str().to_string(),
                revision: capture.revision,
                raft_term: capture.raft_log.term,
                raft_index: capture.raft_log.index,
                cut: match capture.cut {
                    BackupCut::Quiesced {
                        engaged_at,
                        released_at,
                        quiesce,
                    } => CutWire::Quiesced {
                        engaged_at_unix_nanos: engaged_at.unix_nanos(),
                        released_at_unix_nanos: released_at.unix_nanos(),
                        buffered_records: quiesce.buffered_records,
                        buffered_bytes: quiesce.buffered_bytes,
                        dropped_records: quiesce.dropped_records,
                        rejected_records: quiesce.rejected_records,
                    },
                    BackupCut::Live => CutWire::Live,
                    BackupCut::Stopped => CutWire::Stopped,
                    BackupCut::ConfigurationOnly => CutWire::ConfigurationOnly,
                },
            })
            .collect();
        let sections = manifest
            .sections
            .iter()
            .map(|entry| SectionEntryWire {
                path: entry.path.as_str().to_string(),
                content: SectionContentWire::from(entry.content),
                length: entry.length,
                digest: *entry.digest.as_bytes(),
            })
            .collect();
        Self {
            format_major: ARCHIVE_FORMAT_MAJOR,
            producer_version: manifest.producer_version.clone(),
            language_version: manifest.language_version.clone(),
            cluster_id: manifest.cluster_id.clone(),
            captured_at_unix_nanos: manifest.captured_at.unix_nanos(),
            scope,
            resources: ResourcesWire::from(manifest.resources),
            domains,
            sections,
        }
    }
}

impl BackupManifest {
    fn from_wire(wire: ManifestWire) -> Result<Self, Report<ArchiveReadError>> {
        if wire.format_major != ARCHIVE_FORMAT_MAJOR {
            return Err(Report::new(ArchiveReadError::UnsupportedArchiveFormat {
                found: wire.format_major,
                supported: ARCHIVE_FORMAT_MAJOR,
            }));
        }
        let scope = match wire.scope {
            ScopeWire::Cluster => ArchiveScope::Cluster,
            ScopeWire::Domain(domain) => ArchiveScope::Domain(manifest_domain(&domain)?),
        };
        let mut domains = Vec::with_capacity(wire.domains.len());
        for capture in wire.domains {
            domains.push(DomainCapture {
                domain: manifest_domain(&capture.domain)?,
                revision: capture.revision,
                raft_log: RaftLogPosition {
                    term: capture.raft_term,
                    index: capture.raft_index,
                },
                cut: match capture.cut {
                    CutWire::Quiesced {
                        engaged_at_unix_nanos,
                        released_at_unix_nanos,
                        buffered_records,
                        buffered_bytes,
                        dropped_records,
                        rejected_records,
                    } => {
                        if released_at_unix_nanos < engaged_at_unix_nanos {
                            return Err(Report::new(ArchiveReadError::InvalidValue {
                                path: MANIFEST_PATH.to_string(),
                                field: "domain cut interval",
                            }));
                        }
                        BackupCut::Quiesced {
                            engaged_at: Timestamp::from_unix_nanos(engaged_at_unix_nanos),
                            released_at: Timestamp::from_unix_nanos(released_at_unix_nanos),
                            quiesce: BackupQuiesceCounters {
                                buffered_records,
                                buffered_bytes,
                                dropped_records,
                                rejected_records,
                            },
                        }
                    }
                    CutWire::Live => BackupCut::Live,
                    CutWire::Stopped => BackupCut::Stopped,
                    CutWire::ConfigurationOnly => BackupCut::ConfigurationOnly,
                },
            });
        }
        let mut sections = Vec::with_capacity(wire.sections.len());
        let mut seen = BTreeSet::new();
        for entry in wire.sections {
            let path = SectionPath::parse(&entry.path)?;
            if !seen.insert(path.clone()) {
                return Err(Report::new(ArchiveReadError::DuplicateSection {
                    path: path.to_string(),
                }));
            }
            let content = match entry.content {
                SectionContentWire::Record { kind } => match RecordKind::from_repr(kind) {
                    Some(kind) => SectionContent::Record(kind),
                    None => {
                        return Err(Report::new(ArchiveReadError::InvalidValue {
                            path: MANIFEST_PATH.to_string(),
                            field: "section record kind",
                        }));
                    }
                },
                SectionContentWire::Nspl => SectionContent::Nspl,
                SectionContentWire::ResourceArchive => SectionContent::ResourceArchive,
                SectionContentWire::WasmGuestBlob => SectionContent::WasmGuestBlob,
            };
            sections.push(SectionEntry {
                path,
                content,
                length: entry.length,
                digest: SectionDigest::from_bytes(entry.digest),
            });
        }
        Ok(Self {
            producer_version: wire.producer_version,
            language_version: wire.language_version,
            cluster_id: wire.cluster_id,
            captured_at: Timestamp::from_unix_nanos(wire.captured_at_unix_nanos),
            scope,
            resources: BackupResources::from(wire.resources),
            domains,
            sections,
        })
    }
}

fn manifest_domain(raw: &str) -> Result<DomainName, Report<ArchiveReadError>> {
    DomainName::parse(raw).map_err(|error| {
        error.change_context(ArchiveReadError::InvalidValue {
            path: MANIFEST_PATH.to_string(),
            field: "domain name",
        })
    })
}

impl From<SectionContent> for SectionContentWire {
    fn from(content: SectionContent) -> Self {
        match content {
            SectionContent::Record(kind) => Self::Record { kind: kind.tag() },
            SectionContent::Nspl => Self::Nspl,
            SectionContent::ResourceArchive => Self::ResourceArchive,
            SectionContent::WasmGuestBlob => Self::WasmGuestBlob,
        }
    }
}

impl From<BackupResources> for ResourcesWire {
    fn from(resources: BackupResources) -> Self {
        match resources {
            BackupResources::Included => Self::Included,
            BackupResources::Omitted => Self::Omitted,
        }
    }
}

impl From<ResourcesWire> for BackupResources {
    fn from(resources: ResourcesWire) -> Self {
        match resources {
            ResourcesWire::Included => Self::Included,
            ResourcesWire::Omitted => Self::Omitted,
        }
    }
}
