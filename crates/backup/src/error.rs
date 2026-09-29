//! Why an archive could not be written or read.
//!
//! Every variant names where the problem is — a section path, a record kind, a field — and never
//! the bytes it found there, because an archive holds secrets, password hashes and payload data.

use thiserror::Error;

use crate::section::RecordKind;

/// Why an archive could not be laid out or a record encoded.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ArchiveWriteError {
    #[error("the {kind} record could not be encoded")]
    Encode { kind: RecordKind },
    #[error("the {kind} record encodes to {length} bytes, above the {limit}-byte record limit")]
    RecordTooLarge {
        kind: RecordKind,
        length: u64,
        limit: u64,
    },
    #[error("section '{path}' cannot be named in a tar header")]
    Header { path: String },
    #[error("section '{path}' appears more than once in the manifest")]
    DuplicateSection { path: String },
    #[error("the archive is larger than a tar stream can be addressed in")]
    ArchiveTooLarge,
    #[error("the archive could not be written")]
    Write,
    #[error("the bytes supplied for section '{path}' differ from its manifest entry")]
    SectionMismatch { path: String },
}

/// Why an archive was refused while it was read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ArchiveReadError {
    #[error("the archive could not be read")]
    Read,
    #[error("the archive is not a tar stream")]
    NotAnArchive,
    #[error("the archive holds no manifest")]
    MissingManifest,
    #[error("the archive begins with '{path}' rather than its manifest")]
    UnexpectedFirstEntry { path: String },
    #[error("section '{path}' does not begin with the archive record magic")]
    ForeignMagic { path: String },
    #[error("section '{path}' holds a record of kind {found} where a {expected} record belongs")]
    ForeignRecordKind {
        path: String,
        expected: RecordKind,
        found: u16,
    },
    #[error(
        "section '{path}' holds a {kind} record of format version {found}; this reader supports \
         version {supported}"
    )]
    UnsupportedRecordVersion {
        path: String,
        kind: RecordKind,
        found: u16,
        supported: u16,
    },
    #[error(
        "the archive uses format major version {found}; this reader supports major version \
         {supported}"
    )]
    UnsupportedArchiveFormat { found: u16, supported: u16 },
    #[error("section '{path}' is not a valid {kind} record")]
    InvalidRecord { path: String, kind: RecordKind },
    #[error("section '{path}' holds an invalid {field}")]
    InvalidValue { path: String, field: &'static str },
    #[error("section '{path}' is {length} bytes, above the {limit}-byte limit for its kind")]
    SectionTooLarge {
        path: String,
        length: u64,
        limit: u64,
    },
    #[error("the manifest names an invalid section path")]
    InvalidPath,
    #[error("the manifest names section '{path}' more than once")]
    DuplicateSection { path: String },
    #[error("the archive holds '{found}' where the manifest places '{expected}'")]
    SectionOutOfOrder { expected: String, found: String },
    #[error("the archive ends before section '{path}' the manifest names")]
    MissingSection { path: String },
    #[error("the archive holds '{path}', which its manifest does not name")]
    UnexpectedSection { path: String },
    #[error("section '{path}' is {actual} bytes where the manifest declares {declared}")]
    LengthMismatch {
        path: String,
        declared: u64,
        actual: u64,
    },
    #[error("section '{path}' does not match the digest its manifest declares")]
    DigestMismatch { path: String },
    #[error("section '{path}' is not UTF-8 NSPL text")]
    InvalidText { path: String },
    #[error("the archive's sections do not describe domain '{domain}' completely: {missing}")]
    IncompleteDomain {
        domain: String,
        missing: &'static str,
    },
    #[error("the cluster archive holds no users section")]
    MissingUsers,
    #[error("section '{path}' does not belong where the manifest places it")]
    MisplacedSection { path: String },
}
