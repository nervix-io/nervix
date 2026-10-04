//! The evidence a diagnostic process records: which process it was and what its detector found.

use std::{fmt, time::SystemTime};

use error_stack::Report;
use nervix_primitives::deadlock::{
    BoundedText, CycleOutOfBounds, DiagnosticSelection, OrderOutOfBounds, TextOutOfBounds,
};

use crate::{EvidenceError, FindingSelection, RecordedFinding, TriageProof, TriageRefusal};

/// The most findings one process's evidence holds. Potential cycles accumulate within this bound;
/// an active deadlock ends the diagnostic process.
pub const MAX_FINDINGS: usize = 16;

/// What a diagnostic process recorded: the process, and every finding of its detector up to the
/// bound, in the order it made them. Evidence without findings records a process whose detector
/// ran and found nothing, so far or at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadlockEvidence {
    process: ProcessRecord,
    findings: Vec<RecordedFinding>,
    scope: EvidenceScope,
}

/// A selected artifact is useful for investigation but cannot qualify the whole source process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceScope {
    WholeProcess,
    ActiveSelection,
    PotentialSelection,
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
    pub selection: DiagnosticSelection,
}

impl DeadlockEvidence {
    /// The evidence of `process` with `findings`. Refuses more findings than [`MAX_FINDINGS`].
    pub fn new(
        process: ProcessRecord,
        findings: Vec<RecordedFinding>,
    ) -> Result<Self, EvidenceOutOfBounds> {
        if findings.len() > MAX_FINDINGS {
            return Err(EvidenceOutOfBounds::TooManyFindings {
                findings: findings.len(),
            });
        }
        Ok(Self {
            process,
            findings,
            scope: EvidenceScope::WholeProcess,
        })
    }

    /// The evidence of `process` before its detector found anything.
    pub fn started(process: ProcessRecord) -> Self {
        Self {
            process,
            findings: Vec::new(),
            scope: EvidenceScope::WholeProcess,
        }
    }

    pub fn process(&self) -> &ProcessRecord {
        &self.process
    }

    pub fn findings(&self) -> &[RecordedFinding] {
        &self.findings
    }

    pub fn scope(&self) -> EvidenceScope {
        self.scope
    }

    pub(crate) fn with_scope(mut self, scope: EvidenceScope) -> Self {
        self.scope = scope;
        self
    }

    /// The same evidence with `finding` recorded after the others. Refuses a finding past the
    /// bound.
    pub fn with_finding(mut self, finding: RecordedFinding) -> Result<Self, EvidenceOutOfBounds> {
        // Retention is capped at MAX_FINDINGS; this bounded sequence keeps first-seen order.
        if let Some(recorded) = self
            .findings
            .iter_mut()
            .find(|recorded| recorded.same_potential(&finding))
        {
            recorded.merge(&finding)?;
            return Ok(self);
        }
        if self.findings.len() >= MAX_FINDINGS {
            return Err(EvidenceOutOfBounds::TooManyFindings {
                findings: self.findings.len(),
            });
        }
        self.findings.push(finding);
        Ok(self)
    }

    pub fn qualifies(&self) -> bool {
        self.scope == EvidenceScope::WholeProcess
            && self.findings.iter().all(RecordedFinding::qualifies)
    }

    pub fn triage(
        &mut self,
        finding: usize,
        proof: TriageProof,
    ) -> Result<(), Report<EvidenceError>> {
        let recorded = self.findings.get_mut(finding).ok_or_else(|| {
            Report::new(EvidenceError::Triage {
                finding,
                refusal: TriageRefusal::MissingFinding,
            })
        })?;
        recorded.triage(finding, proof)
    }

    /// Local selection retains complete values and loss findings. It does not qualify the source
    /// workload; qualification must inspect the original evidence, including every finding.
    pub fn selected(&self, selection: FindingSelection) -> Self {
        if selection == FindingSelection::All {
            return self.clone();
        }
        let findings = self
            .findings
            .iter()
            .filter(|finding| selection.includes(finding))
            .cloned()
            .collect();
        let scope = match selection {
            FindingSelection::All => self.scope,
            FindingSelection::Active => EvidenceScope::ActiveSelection,
            FindingSelection::Potential => EvidenceScope::PotentialSelection,
        };
        Self {
            process: self.process.clone(),
            findings,
            scope,
        }
    }
}

/// Why a value is not one the evidence can hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceOutOfBounds {
    /// More findings than [`MAX_FINDINGS`].
    TooManyFindings {
        findings: usize,
    },
    /// A text that is not one a bounded text could have kept.
    Text(TextOutOfBounds),
    /// A cycle outside its bounds.
    Cycle(CycleOutOfBounds),
    /// A thread or lock numbered zero, which the detector never assigns.
    ZeroIdentity,
    /// An overflow that lost no finding.
    NothingLost,
    Order(OrderOutOfBounds),
    InvalidReview,
    OccurrenceOverflow,
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
            Self::Order(order) => write!(f, "{order}"),
            Self::InvalidReview => f.write_str(
                "a review requires an untruncated reason and retained regression reference",
            ),
            Self::OccurrenceOverflow => {
                f.write_str("the finding repetition count exceeds its range")
            }
        }
    }
}
