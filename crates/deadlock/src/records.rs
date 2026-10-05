//! Finding retention and explicit engineer dispositions of potential order.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Repetition counts, review proofs and the qualification policy.
//! - **Depends on.** Bounded primitive findings and the owning evidence errors.
//! - **Must not know.** Application locks, workload admission or detector internals.

use std::num::NonZeroU64;

use error_stack::Report;
use nervix_primitives::deadlock::{
    Access, ActiveCycle, BoundedText, Finding, OverflowSource, PotentialCycle,
};

use crate::{EvidenceError, EvidenceOutOfBounds};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "report-tool", derive(clap::ValueEnum))]
pub enum ProofBasis {
    Correction,
    NonOverlap,
    SharedReaders,
    Lifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriageProof {
    basis: ProofBasis,
    reason: BoundedText,
    regression: BoundedText,
}

impl TriageProof {
    pub fn new(
        basis: ProofBasis,
        reason: &str,
        regression: &str,
    ) -> Result<Self, Report<EvidenceError>> {
        Self::from_parts(
            basis,
            BoundedText::new(reason),
            BoundedText::new(regression),
        )
        .map_err(|bounds| Report::new(EvidenceError::OutOfBounds(bounds)))
    }

    pub(crate) fn from_parts(
        basis: ProofBasis,
        reason: BoundedText,
        regression: BoundedText,
    ) -> Result<Self, EvidenceOutOfBounds> {
        for text in [&reason, &regression] {
            if text.as_str().trim().is_empty() || text.is_truncated() {
                return Err(EvidenceOutOfBounds::InvalidReview);
            }
        }
        Ok(Self {
            basis,
            reason,
            regression,
        })
    }

    pub fn basis(&self) -> ProofBasis {
        self.basis
    }
    pub fn reason(&self) -> &BoundedText {
        &self.reason
    }
    pub fn regression(&self) -> &BoundedText {
        &self.regression
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PotentialTriage {
    Unreviewed,
    Reviewed(TriageProof),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceLossSource {
    Handoff,
    OrderHistory,
    Retention,
}

impl From<OverflowSource> for EvidenceLossSource {
    fn from(source: OverflowSource) -> Self {
        match source {
            OverflowSource::Handoff => Self::Handoff,
            OverflowSource::OrderHistory => Self::OrderHistory,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedFinding {
    ActiveCycle(ActiveCycle),
    Potential {
        cycle: PotentialCycle,
        repetitions: NonZeroU64,
        triage: PotentialTriage,
    },
    Overflow {
        lost: NonZeroU64,
        source: EvidenceLossSource,
    },
}

impl From<Finding> for RecordedFinding {
    fn from(finding: Finding) -> Self {
        match finding {
            Finding::ActiveCycle(cycle) => Self::ActiveCycle(cycle),
            Finding::PotentialCycle(cycle) => Self::Potential {
                cycle,
                repetitions: NonZeroU64::MIN,
                triage: PotentialTriage::Unreviewed,
            },
            Finding::Overflow { lost, source } => Self::Overflow {
                lost,
                source: source.into(),
            },
        }
    }
}

impl RecordedFinding {
    pub fn qualifies(&self) -> bool {
        match self {
            Self::Potential {
                cycle,
                triage: PotentialTriage::Reviewed(proof),
                ..
            } => cycle.has_complete_context() && self.supports(proof.basis()),
            Self::ActiveCycle(_) | Self::Potential { .. } | Self::Overflow { .. } => false,
        }
    }

    pub fn same_potential(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Potential { cycle: left, .. }, Self::Potential { cycle: right, .. }) => {
                left.same_cycle(right)
            }
            _ => false,
        }
    }

    pub(crate) fn merge(&mut self, other: &Self) -> Result<bool, EvidenceOutOfBounds> {
        if let (
            Self::Potential {
                cycle,
                repetitions,
                triage,
            },
            Self::Potential {
                cycle: incoming,
                repetitions: count,
                ..
            },
        ) = (self, other)
        {
            let total = repetitions
                .get()
                .checked_add(count.get())
                .ok_or(EvidenceOutOfBounds::OccurrenceOverflow)?;
            let changed = cycle.merge(incoming).map_err(EvidenceOutOfBounds::Order)?;
            *repetitions = NonZeroU64::new(total).ok_or(EvidenceOutOfBounds::NothingLost)?;
            if changed {
                *triage = PotentialTriage::Unreviewed;
            }
            return Ok(changed);
        }
        Ok(false)
    }

    fn supports(&self, basis: ProofBasis) -> bool {
        if basis != ProofBasis::SharedReaders {
            return true;
        }
        let Self::Potential { cycle, .. } = self else {
            return false;
        };
        cycle.edges().iter().all(|edge| {
            edge.witnesses().iter().all(|witness| {
                witness.held.access == Access::Shared && witness.requested.access == Access::Shared
            })
        })
    }

    pub(crate) fn triage(
        &mut self,
        finding: usize,
        proof: TriageProof,
    ) -> Result<(), Report<EvidenceError>> {
        let Self::Potential { cycle, .. } = self else {
            return Err(Report::new(EvidenceError::Triage {
                finding,
                refusal: TriageRefusal::NotPotential,
            }));
        };
        if !cycle.has_complete_context() {
            return Err(Report::new(EvidenceError::Triage {
                finding,
                refusal: TriageRefusal::IncompleteContext,
            }));
        }
        if !self.supports(proof.basis()) {
            return Err(Report::new(EvidenceError::Triage {
                finding,
                refusal: TriageRefusal::ExclusiveAccess,
            }));
        }
        if let Self::Potential { triage, .. } = self {
            *triage = PotentialTriage::Reviewed(proof);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TriageRefusal {
    #[error("the evidence has no such finding")]
    MissingFinding,
    #[error("only potential order can receive an infeasibility or correction proof")]
    NotPotential,
    #[error("the cycle is truncated or lacks source context")]
    IncompleteContext,
    #[error("an exclusive acquisition contradicts a shared-reader proof")]
    ExclusiveAccess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "report-tool", derive(clap::ValueEnum))]
pub enum FindingSelection {
    All,
    Active,
    Potential,
}

impl FindingSelection {
    pub fn includes(self, finding: &RecordedFinding) -> bool {
        matches!(
            (self, finding),
            (Self::All, _)
                | (_, RecordedFinding::Overflow { .. })
                | (Self::Active, RecordedFinding::ActiveCycle(_))
                | (Self::Potential, RecordedFinding::Potential { .. })
        )
    }
}
