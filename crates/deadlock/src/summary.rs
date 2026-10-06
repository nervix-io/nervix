//! What one evidence file holds, counted by kind, for a supervisor that qualifies many of them.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The counts a qualification reports beside its verdict and their one-line rendering.
//! - **Depends on.** The recorded findings and the qualification policy of each finding.
//! - **Must not know.** Which run, workload or process the evidence came from, or how a supervisor
//!   aggregates several files.

use std::fmt;

use meticulous::OptionExt as _;

use crate::{
    DeadlockEvidence, EvidenceLossSource, EvidenceScope, PotentialTriage, RecordedFinding,
};

/// The counts of one evidence file. Repeated deliveries of one potential cycle are counted apart
/// from findings lost to overload: a repetition merged into a retained finding loses nothing, while
/// a lost finding was never retained at all.
///
/// The sums are 128 bits wide: evidence holds at most [`crate::MAX_FINDINGS`] records, each adding
/// less than 2^64, so no sum can overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvidenceSummary {
    pub scope: EvidenceScope,
    /// Every record the evidence holds.
    pub findings: usize,
    pub active: usize,
    /// Distinct potential cycles, each counted once however often it was delivered.
    pub potential: usize,
    /// Potential cycles without a review.
    pub unreviewed: usize,
    /// Records that prevent qualification: active cycles, loss, and potential cycles without a
    /// review their context supports.
    pub nonqualifying: usize,
    /// Deliveries of a potential cycle after its first, merged into the retained record.
    pub repeated_deliveries: u128,
    pub lost_handoff: u128,
    pub lost_order_history: u128,
    pub lost_retention: u128,
}

impl DeadlockEvidence {
    /// The counts of this evidence.
    pub fn summary(&self) -> EvidenceSummary {
        let mut summary = EvidenceSummary {
            scope: self.scope(),
            findings: self.findings().len(),
            active: 0,
            potential: 0,
            unreviewed: 0,
            nonqualifying: 0,
            repeated_deliveries: 0,
            lost_handoff: 0,
            lost_order_history: 0,
            lost_retention: 0,
        };
        for finding in self.findings() {
            if !finding.qualifies() {
                summary.nonqualifying = one_more(summary.nonqualifying);
            }
            match finding {
                RecordedFinding::ActiveCycle(_) => summary.active = one_more(summary.active),
                RecordedFinding::Potential {
                    repetitions,
                    triage,
                    ..
                } => {
                    summary.potential = one_more(summary.potential);
                    if let PotentialTriage::Unreviewed = triage {
                        summary.unreviewed = one_more(summary.unreviewed);
                    }
                    let repeated = repetitions
                        .get()
                        .checked_sub(1)
                        .assured("a retained finding was delivered at least once");
                    summary.repeated_deliveries =
                        add(summary.repeated_deliveries, u128::from(repeated));
                }
                RecordedFinding::Overflow { lost, source } => {
                    let total = match source {
                        EvidenceLossSource::Handoff => &mut summary.lost_handoff,
                        EvidenceLossSource::OrderHistory => &mut summary.lost_order_history,
                        EvidenceLossSource::Retention => &mut summary.lost_retention,
                    };
                    *total = add(*total, u128::from(lost.get()));
                }
            }
        }
        summary
    }
}

fn one_more(count: usize) -> usize {
    count
        .checked_add(1)
        .assured("evidence holds at most MAX_FINDINGS records")
}

fn add(total: u128, addend: u128) -> u128 {
    total
        .checked_add(addend)
        .assured("at most MAX_FINDINGS addends below 2^64 fit in 128 bits")
}

/// One line of `key=value` pairs in a fixed order, which `nervix-deadlock-report qualify` prints
/// for supervisors to read.
impl fmt::Display for EvidenceSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scope = match self.scope {
            EvidenceScope::WholeProcess => "whole-process",
            EvidenceScope::ActiveSelection => "active-selection",
            EvidenceScope::PotentialSelection => "potential-selection",
        };
        write!(
            f,
            "scope={scope} findings={} active={} potential={} unreviewed={} nonqualifying={} \
             repeated-deliveries={} lost-handoff={} lost-order-history={} lost-retention={}",
            self.findings,
            self.active,
            self.potential,
            self.unreviewed,
            self.nonqualifying,
            self.repeated_deliveries,
            self.lost_handoff,
            self.lost_order_history,
            self.lost_retention,
        )
    }
}
