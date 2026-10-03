//! What can go wrong reading, writing or recording deadlock evidence.

use std::path::PathBuf;

use crate::evidence::EvidenceOutOfBounds;

/// Why evidence could not be encoded, decoded, written or read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EvidenceError {
    #[error("the evidence could not be encoded")]
    Encode,
    #[error(
        "an evidence file of {length} bytes is larger than the {limit} bytes evidence may take"
    )]
    TooLarge { length: u64, limit: u64 },
    #[error("the bytes are not Nervix deadlock evidence: they do not begin with its magic")]
    ForeignMagic,
    #[error("the evidence holds record kind {found}, not deadlock evidence")]
    ForeignKind { found: u16 },
    #[error("the evidence is format version {found}; this reader reads version {supported}")]
    UnsupportedVersion { found: u16, supported: u16 },
    #[error("the evidence payload is not a valid encoding")]
    InvalidPayload,
    #[error("the evidence describes a value outside its bounds: {0}")]
    OutOfBounds(EvidenceOutOfBounds),
    #[error("a time in the evidence is outside the range it can be recorded in")]
    TimeOutOfRange,
    #[error("the evidence file {path:?} could not be written")]
    Write { path: PathBuf },
    #[error("the evidence file {path:?} could not be read")]
    Read { path: PathBuf },
    #[error("the evidence directory {path:?} could not be listed")]
    List { path: PathBuf },
}

/// Why a diagnostic run could not start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DiagnosticError {
    #[error("the deadlock detector could not be installed")]
    Install,
    #[error("the evidence that the deadlock detector runs could not be recorded")]
    RecordStart,
}
