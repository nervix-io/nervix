//! The evidence a diagnostic process records: which process it was and what its detector found.

use std::{fmt, time::SystemTime};

use nervix_primitives::deadlock::{BoundedText, CycleOutOfBounds, Finding, TextOutOfBounds};

/// The most findings one process's evidence holds. The first active deadlock ends a diagnostic
/// process, so its evidence normally holds one.
pub const MAX_FINDINGS: usize = 16;

/// What a diagnostic process recorded: the process, and every finding of its detector up to the
/// bound, in the order it made them. Evidence without findings records a process whose detector
/// ran and found nothing, so far or at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadlockEvidence {
    process: ProcessRecord,
    findings: Vec<Finding>,
}

/// The process evidence was recorded by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessRecord {
    /// The operating system's identifier of the process.
    pub id: u32,
    /// The name of the program the process ran; absent when its arguments did not name one.
    pub program: Option<BoundedText>,
    /// When the process started its diagnostic run.
    pub started_at: SystemTime,
}

impl DeadlockEvidence {
    /// The evidence of `process` with `findings`. Refuses more findings than [`MAX_FINDINGS`].
    pub fn new(
        process: ProcessRecord,
        findings: Vec<Finding>,
    ) -> Result<Self, EvidenceOutOfBounds> {
        if findings.len() > MAX_FINDINGS {
            return Err(EvidenceOutOfBounds::TooManyFindings {
                findings: findings.len(),
            });
        }
        Ok(Self { process, findings })
    }

    /// The evidence of `process` before its detector found anything.
    pub fn started(process: ProcessRecord) -> Self {
        Self {
            process,
            findings: Vec::new(),
        }
    }

    pub fn process(&self) -> &ProcessRecord {
        &self.process
    }

    pub fn findings(&self) -> &[Finding] {
        &self.findings
    }

    /// The same evidence with `finding` recorded after the others. Refuses a finding past the
    /// bound.
    pub fn with_finding(mut self, finding: Finding) -> Result<Self, EvidenceOutOfBounds> {
        if self.findings.len() >= MAX_FINDINGS {
            return Err(EvidenceOutOfBounds::TooManyFindings {
                findings: self.findings.len(),
            });
        }
        self.findings.push(finding);
        Ok(self)
    }
}

/// Why a value is not one the evidence can hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceOutOfBounds {
    /// More findings than [`MAX_FINDINGS`].
    TooManyFindings { findings: usize },
    /// A text that is not one a bounded text could have kept.
    Text(TextOutOfBounds),
    /// A cycle outside its bounds.
    Cycle(CycleOutOfBounds),
    /// A thread or lock numbered zero, which the detector never assigns.
    ZeroIdentity,
    /// An overflow that lost no finding.
    NothingLost,
}

impl fmt::Display for EvidenceOutOfBounds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyFindings { findings } => write!(
                f,
                "{findings} findings, more than the {MAX_FINDINGS} evidence holds"
            ),
            Self::Text(text) => write!(f, "{text}"),
            Self::Cycle(cycle) => write!(f, "{cycle}"),
            Self::ZeroIdentity => f.write_str("a thread or lock numbered zero"),
            Self::NothingLost => f.write_str("an overflow that lost no finding"),
        }
    }
}
