//! The bytes of an evidence file: a header, then the rkyv encoding of the evidence's wire shape.
//!
//! An evidence file is the 8-byte magic `NVXDLEVD`, the record kind and the format version as
//! little-endian `u16`s, and then the rkyv payload. The header is checked before the payload is
//! touched: bytes of another kind or of another version are refused by what the header says rather
//! than validated as a shape they are not.
//!
//! The wire shapes hold primitive values only. Every value is validated once, when the evidence
//! converts from its wire shape: identities are nonzero, texts are ones a bounded text could have
//! kept, cycles and findings are within their bounds. Changing a shape here changes the bytes of
//! the evidence, so it also raises the format version.

use std::{
    num::NonZeroU64,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::deadlock::{
    Access, ActiveCycle, BlockedAttempt, BlockedThread, BoundedText, Finding, LockKind, LockSite,
    SourceSite, TrackedLockId, TrackedThreadId, WaitedLock,
};
use rkyv::{Archive, Deserialize, Serialize, rancor, util::AlignedVec};

use crate::{
    error::EvidenceError,
    evidence::{DeadlockEvidence, EvidenceOutOfBounds, ProcessRecord},
};

/// The bytes every evidence file begins with.
const MAGIC: [u8; 8] = *b"NVXDLEVD";

/// The record kind of deadlock evidence. The tag is the format's contract, so it never changes.
const EVIDENCE_KIND: u16 = 1;

/// The format version this crate writes and reads.
const EVIDENCE_VERSION: u16 = 1;

/// The magic, the kind and the version.
const HEADER_BYTES: usize = MAGIC.len() + 2 + 2;

/// The largest evidence file this crate writes or reads. The bounds keep a file of the most
/// findings, each with the most threads and the longest texts, under 2 MiB.
pub(crate) const MAX_EVIDENCE_BYTES: u64 = 4 * 1024 * 1024;

/// The alignment an rkyv payload is validated at.
const PAYLOAD_ALIGNMENT: usize = 16;

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct EvidenceWire {
    pub(crate) process: ProcessWire,
    pub(crate) findings: Vec<FindingWire>,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct ProcessWire {
    pub(crate) id: u32,
    pub(crate) program: Option<TextWire>,
    pub(crate) started_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct TextWire {
    pub(crate) text: String,
    pub(crate) original_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum FindingWire {
    ActiveCycle(CycleWire),
    Overflow { lost: u64 },
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct CycleWire {
    pub(crate) detected_at_unix_nanos: u64,
    pub(crate) threads: Vec<ThreadWire>,
    pub(crate) omitted_threads: u64,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct ThreadWire {
    pub(crate) thread: u64,
    pub(crate) name: Option<TextWire>,
    pub(crate) waits_for: Option<WaitedLockWire>,
    pub(crate) attempt: Option<AttemptWire>,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct WaitedLockWire {
    pub(crate) id: u64,
    pub(crate) site: Option<LockSiteWire>,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct LockSiteWire {
    pub(crate) kind: LockKindWire,
    pub(crate) constructed_at: SiteWire,
}

#[derive(Debug, Clone, Copy, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum LockKindWire {
    Mutex,
    RwLock,
    CondvarState,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct AttemptWire {
    pub(crate) access: AccessWire,
    pub(crate) at: SiteWire,
}

#[derive(Debug, Clone, Copy, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) enum AccessWire {
    Exclusive,
    Shared,
}

#[derive(Debug, Clone, PartialEq, Archive, Serialize, Deserialize)]
pub(crate) struct SiteWire {
    pub(crate) file: TextWire,
    pub(crate) line: u32,
    pub(crate) column: u32,
}

impl DeadlockEvidence {
    /// The complete evidence file: the header, then the rkyv payload.
    pub fn encode(&self) -> Result<Vec<u8>, Report<EvidenceError>> {
        let wire = EvidenceWire::try_from(self)?;
        encode_wire(&wire)
    }

    /// Checks the header, validates the payload with bytecheck, and converts it into evidence
    /// within every bound.
    pub fn decode(bytes: &[u8]) -> Result<Self, Report<EvidenceError>> {
        let wire = decode_wire(bytes)?;
        match Self::try_from(wire) {
            Ok(evidence) => Ok(evidence),
            Err(bounds) => Err(Report::new(EvidenceError::OutOfBounds(bounds))),
        }
    }
}

/// The header and the rkyv payload of `wire`.
pub(crate) fn encode_wire(wire: &EvidenceWire) -> Result<Vec<u8>, Report<EvidenceError>> {
    let payload = match rkyv::to_bytes::<rancor::Error>(wire) {
        Ok(payload) => payload,
        Err(error) => return Err(Report::new(error).change_context(EvidenceError::Encode)),
    };
    let length = HEADER_BYTES
        .checked_add(payload.len())
        .assured("an encoded payload fits the address space it was encoded in");
    let length_bytes = u64::try_from(length).assured("supported targets address at most 64 bits");
    if length_bytes > MAX_EVIDENCE_BYTES {
        return Err(Report::new(EvidenceError::TooLarge {
            length: length_bytes,
            limit: MAX_EVIDENCE_BYTES,
        }));
    }
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&EVIDENCE_KIND.to_le_bytes());
    bytes.extend_from_slice(&EVIDENCE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

/// The wire shape behind a checked header.
pub(crate) fn decode_wire(bytes: &[u8]) -> Result<EvidenceWire, Report<EvidenceError>> {
    let length = u64::try_from(bytes.len()).assured("supported targets address at most 64 bits");
    if length > MAX_EVIDENCE_BYTES {
        return Err(Report::new(EvidenceError::TooLarge {
            length,
            limit: MAX_EVIDENCE_BYTES,
        }));
    }
    let Some((magic, rest)) = bytes.split_first_chunk::<8>() else {
        return Err(Report::new(EvidenceError::ForeignMagic));
    };
    if *magic != MAGIC {
        return Err(Report::new(EvidenceError::ForeignMagic));
    }
    let Some((kind, rest)) = rest.split_first_chunk::<2>() else {
        return Err(Report::new(EvidenceError::ForeignMagic));
    };
    let kind = u16::from_le_bytes(*kind);
    if kind != EVIDENCE_KIND {
        return Err(Report::new(EvidenceError::ForeignKind { found: kind }));
    }
    let Some((version, payload)) = rest.split_first_chunk::<2>() else {
        return Err(Report::new(EvidenceError::ForeignMagic));
    };
    let version = u16::from_le_bytes(*version);
    if version != EVIDENCE_VERSION {
        return Err(Report::new(EvidenceError::UnsupportedVersion {
            found: version,
            supported: EVIDENCE_VERSION,
        }));
    }
    let mut aligned = AlignedVec::<PAYLOAD_ALIGNMENT>::with_capacity(payload.len());
    aligned.extend_from_slice(payload);
    match rkyv::from_bytes::<EvidenceWire, rancor::Error>(&aligned) {
        Ok(wire) => Ok(wire),
        Err(error) => Err(Report::new(error).change_context(EvidenceError::InvalidPayload)),
    }
}

impl TryFrom<&DeadlockEvidence> for EvidenceWire {
    type Error = Report<EvidenceError>;

    fn try_from(evidence: &DeadlockEvidence) -> Result<Self, Self::Error> {
        let process = evidence.process();
        let mut findings = Vec::with_capacity(evidence.findings().len());
        for finding in evidence.findings() {
            findings.push(FindingWire::try_from(finding)?);
        }
        Ok(Self {
            process: ProcessWire {
                id: process.id,
                program: process.program.as_ref().map(TextWire::from),
                started_at_unix_nanos: unix_nanos(process.started_at)?,
            },
            findings,
        })
    }
}

impl TryFrom<&Finding> for FindingWire {
    type Error = Report<EvidenceError>;

    fn try_from(finding: &Finding) -> Result<Self, Self::Error> {
        match finding {
            Finding::ActiveCycle(cycle) => {
                let mut threads = Vec::with_capacity(cycle.threads().len());
                for thread in cycle.threads() {
                    threads.push(ThreadWire::from(thread));
                }
                Ok(Self::ActiveCycle(CycleWire {
                    detected_at_unix_nanos: unix_nanos(cycle.detected_at())?,
                    threads,
                    omitted_threads: cycle.omitted_threads(),
                }))
            }
            Finding::Overflow { lost } => Ok(Self::Overflow { lost: lost.get() }),
        }
    }
}

impl From<&BlockedThread> for ThreadWire {
    fn from(thread: &BlockedThread) -> Self {
        Self {
            thread: thread.thread.get().get(),
            name: thread.name.as_ref().map(TextWire::from),
            waits_for: thread.waits_for.as_ref().map(WaitedLockWire::from),
            attempt: thread.attempt.as_ref().map(AttemptWire::from),
        }
    }
}

impl From<&WaitedLock> for WaitedLockWire {
    fn from(lock: &WaitedLock) -> Self {
        Self {
            id: lock.id.get().get(),
            site: lock.site.as_ref().map(LockSiteWire::from),
        }
    }
}

impl From<&LockSite> for LockSiteWire {
    fn from(site: &LockSite) -> Self {
        Self {
            kind: LockKindWire::from(site.kind),
            constructed_at: SiteWire::from(&site.constructed_at),
        }
    }
}

impl From<LockKind> for LockKindWire {
    fn from(kind: LockKind) -> Self {
        match kind {
            LockKind::Mutex => Self::Mutex,
            LockKind::RwLock => Self::RwLock,
            LockKind::CondvarState => Self::CondvarState,
        }
    }
}

impl From<&BlockedAttempt> for AttemptWire {
    fn from(attempt: &BlockedAttempt) -> Self {
        Self {
            access: AccessWire::from(attempt.access),
            at: SiteWire::from(&attempt.at),
        }
    }
}

impl From<Access> for AccessWire {
    fn from(access: Access) -> Self {
        match access {
            Access::Exclusive => Self::Exclusive,
            Access::Shared => Self::Shared,
        }
    }
}

impl From<&SourceSite> for SiteWire {
    fn from(site: &SourceSite) -> Self {
        Self {
            file: TextWire::from(&site.file),
            line: site.line,
            column: site.column,
        }
    }
}

impl From<&BoundedText> for TextWire {
    fn from(text: &BoundedText) -> Self {
        Self {
            text: text.as_str().to_string(),
            original_bytes: text.original_bytes(),
        }
    }
}

impl TryFrom<EvidenceWire> for DeadlockEvidence {
    type Error = EvidenceOutOfBounds;

    fn try_from(wire: EvidenceWire) -> Result<Self, Self::Error> {
        let mut findings = Vec::with_capacity(wire.findings.len());
        for finding in wire.findings {
            findings.push(Finding::try_from(finding)?);
        }
        let program = match wire.process.program {
            Some(program) => Some(BoundedText::try_from(program)?),
            None => None,
        };
        let process = ProcessRecord {
            id: wire.process.id,
            program,
            started_at: system_time(wire.process.started_at_unix_nanos),
        };
        Self::new(process, findings)
    }
}

impl TryFrom<FindingWire> for Finding {
    type Error = EvidenceOutOfBounds;

    fn try_from(wire: FindingWire) -> Result<Self, Self::Error> {
        match wire {
            FindingWire::ActiveCycle(cycle) => {
                let mut threads = Vec::with_capacity(cycle.threads.len());
                for thread in cycle.threads {
                    threads.push(BlockedThread::try_from(thread)?);
                }
                let cycle = ActiveCycle::new(
                    system_time(cycle.detected_at_unix_nanos),
                    threads,
                    cycle.omitted_threads,
                )
                .map_err(EvidenceOutOfBounds::Cycle)?;
                Ok(Self::ActiveCycle(cycle))
            }
            FindingWire::Overflow { lost } => {
                let lost = NonZeroU64::new(lost).ok_or(EvidenceOutOfBounds::NothingLost)?;
                Ok(Self::Overflow { lost })
            }
        }
    }
}

impl TryFrom<ThreadWire> for BlockedThread {
    type Error = EvidenceOutOfBounds;

    fn try_from(wire: ThreadWire) -> Result<Self, Self::Error> {
        let name = match wire.name {
            Some(name) => Some(BoundedText::try_from(name)?),
            None => None,
        };
        let waits_for = match wire.waits_for {
            Some(lock) => Some(WaitedLock::try_from(lock)?),
            None => None,
        };
        let attempt = match wire.attempt {
            Some(attempt) => Some(BlockedAttempt::try_from(attempt)?),
            None => None,
        };
        Ok(Self {
            thread: TrackedThreadId::new(nonzero(wire.thread)?),
            name,
            waits_for,
            attempt,
        })
    }
}

impl TryFrom<WaitedLockWire> for WaitedLock {
    type Error = EvidenceOutOfBounds;

    fn try_from(wire: WaitedLockWire) -> Result<Self, Self::Error> {
        let site = match wire.site {
            Some(site) => Some(LockSite {
                kind: LockKind::from(site.kind),
                constructed_at: SourceSite::try_from(site.constructed_at)?,
            }),
            None => None,
        };
        Ok(Self {
            id: TrackedLockId::new(nonzero(wire.id)?),
            site,
        })
    }
}

impl From<LockKindWire> for LockKind {
    fn from(wire: LockKindWire) -> Self {
        match wire {
            LockKindWire::Mutex => Self::Mutex,
            LockKindWire::RwLock => Self::RwLock,
            LockKindWire::CondvarState => Self::CondvarState,
        }
    }
}

impl TryFrom<AttemptWire> for BlockedAttempt {
    type Error = EvidenceOutOfBounds;

    fn try_from(wire: AttemptWire) -> Result<Self, Self::Error> {
        Ok(Self {
            access: Access::from(wire.access),
            at: SourceSite::try_from(wire.at)?,
        })
    }
}

impl From<AccessWire> for Access {
    fn from(wire: AccessWire) -> Self {
        match wire {
            AccessWire::Exclusive => Self::Exclusive,
            AccessWire::Shared => Self::Shared,
        }
    }
}

impl TryFrom<SiteWire> for SourceSite {
    type Error = EvidenceOutOfBounds;

    fn try_from(wire: SiteWire) -> Result<Self, Self::Error> {
        Ok(Self {
            file: BoundedText::try_from(wire.file)?,
            line: wire.line,
            column: wire.column,
        })
    }
}

impl TryFrom<TextWire> for BoundedText {
    type Error = EvidenceOutOfBounds;

    fn try_from(wire: TextWire) -> Result<Self, Self::Error> {
        Self::from_parts(wire.text, wire.original_bytes).map_err(EvidenceOutOfBounds::Text)
    }
}

fn nonzero(number: u64) -> Result<NonZeroU64, EvidenceOutOfBounds> {
    NonZeroU64::new(number).ok_or(EvidenceOutOfBounds::ZeroIdentity)
}

/// `time` as nanoseconds since the Unix epoch, the form evidence records it in.
pub(crate) fn unix_nanos(time: SystemTime) -> Result<u64, Report<EvidenceError>> {
    let since_epoch = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Report::new(EvidenceError::TimeOutOfRange))?;
    u64::try_from(since_epoch.as_nanos()).map_err(|_| Report::new(EvidenceError::TimeOutOfRange))
}

/// The time `nanos` nanoseconds after the Unix epoch.
fn system_time(nanos: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_nanos(nanos))
        .assured("supported targets represent every time up to 2^64 nanoseconds after the epoch")
}
