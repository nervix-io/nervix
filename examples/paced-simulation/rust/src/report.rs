//! What the simulation prints: one line per observation as it happens, and a summary at the end.
//!
//! - **Owns.** The counters every loop of a run adds to, and the summary line they print as.
//! - **Depends on.** The outcome and effect classifications.
//! - **Must not know.** Sessions, clocks or files.
//!
//! Every line goes to standard output whole, so lines of concurrent loops never interleave. The
//! Python driver prints the same lines and the same summary for the same run.

use std::collections::BTreeSet;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::blocking::Mutex;

use crate::{effects::Applied, ledger::OutcomeKind};

/// Prints one report line.
pub(crate) fn line(text: impl AsRef<str>) {
    println!("{}", text.as_ref());
}

/// Prints one error line on standard error.
pub(crate) fn error(text: impl AsRef<str>) {
    eprintln!("error: {}", text.as_ref());
}

#[derive(Debug, Default)]
struct Counters {
    /// Readings the run submitted, replays included.
    readings: u64,
    completed: u64,
    not_admitted: u64,
    processing_failed: u64,
    outcome_unknown: u64,
    /// Readings the consumers applied for the first time.
    effects: u64,
    /// Rejection notices the consumers recorded for the first time.
    rejection_notices: u64,
    /// Delivered records the effect store already held.
    duplicates: u64,
    /// The START generations the run planned readings in.
    generations: BTreeSet<u64>,
    /// Tick progress the clock attachment delivered; ticks coalesce, so this counts observations.
    ticks_observed: u64,
    inspections: u64,
    /// Submissions that had to wait for the producer's credit.
    credit_waits: u64,
    peak_outstanding_bytes: u64,
    consumers_joined: u64,
    consumers_left: u64,
}

/// One more of something a run counts. A run counts far fewer than 2^64 of anything.
fn add(counter: &mut u64, amount: u64) {
    *counter = counter
        .checked_add(amount)
        .assured("a run counts far fewer than 2^64 readings, deliveries or observations");
}

/// The counters of one run.
#[derive(Debug, Default)]
pub(crate) struct Report {
    counters: Mutex<Counters>,
}

impl Report {
    pub(crate) fn submitted(&self, readings: u64) {
        add(&mut self.counters.lock().readings, readings);
    }

    pub(crate) fn outcome(&self, outcome: OutcomeKind, readings: u64) {
        let mut counters = self.counters.lock();
        let counter = match outcome {
            OutcomeKind::Completed => &mut counters.completed,
            OutcomeKind::NotAdmitted => &mut counters.not_admitted,
            OutcomeKind::ProcessingFailed => &mut counters.processing_failed,
            OutcomeKind::OutcomeUnknown => &mut counters.outcome_unknown,
        };
        add(counter, readings);
    }

    pub(crate) fn reading_effect(&self, applied: Applied) {
        let mut counters = self.counters.lock();
        match applied {
            Applied::New => add(&mut counters.effects, 1),
            Applied::Duplicate => add(&mut counters.duplicates, 1),
        }
    }

    pub(crate) fn rejection_notice(&self, applied: Applied) {
        let mut counters = self.counters.lock();
        match applied {
            Applied::New => add(&mut counters.rejection_notices, 1),
            Applied::Duplicate => add(&mut counters.duplicates, 1),
        }
    }

    pub(crate) fn generation(&self, generation: u64) {
        self.counters.lock().generations.insert(generation);
    }

    pub(crate) fn tick(&self) {
        add(&mut self.counters.lock().ticks_observed, 1);
    }

    pub(crate) fn inspection(&self) {
        add(&mut self.counters.lock().inspections, 1);
    }

    pub(crate) fn credit_wait(&self) {
        add(&mut self.counters.lock().credit_waits, 1);
    }

    pub(crate) fn outstanding(&self, bytes: u64) {
        let mut counters = self.counters.lock();
        if bytes > counters.peak_outstanding_bytes {
            counters.peak_outstanding_bytes = bytes;
        }
    }

    pub(crate) fn consumer_joined(&self) {
        add(&mut self.counters.lock().consumers_joined, 1);
    }

    pub(crate) fn consumer_left(&self) {
        add(&mut self.counters.lock().consumers_left, 1);
    }

    /// Whether every reading the run submitted completed.
    pub(crate) fn all_completed(&self) -> bool {
        let counters = self.counters.lock();
        counters.completed == counters.readings
    }

    /// The summary line, every count named.
    pub(crate) fn summary(&self) -> String {
        let counters = self.counters.lock();
        let generations = u64::try_from(counters.generations.len())
            .assured("a run plans in far fewer than 2^64 generations");
        format!(
            "SUMMARY readings={} completed={} not_admitted={} processing_failed={} \
             outcome_unknown={} effects={} rejection_notices={} duplicates={} generations={} \
             ticks_observed={} inspections={} credit_waits={} peak_outstanding_bytes={} \
             consumers_joined={} consumers_left={}",
            counters.readings,
            counters.completed,
            counters.not_admitted,
            counters.processing_failed,
            counters.outcome_unknown,
            counters.effects,
            counters.rejection_notices,
            counters.duplicates,
            generations,
            counters.ticks_observed,
            counters.inspections,
            counters.credit_waits,
            counters.peak_outstanding_bytes,
            counters.consumers_joined,
            counters.consumers_left,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_summary_names_every_count() {
        let report = Report::default();
        report.submitted(4);
        report.outcome(OutcomeKind::Completed, 3);
        report.outcome(OutcomeKind::OutcomeUnknown, 1);
        report.reading_effect(Applied::New);
        report.reading_effect(Applied::Duplicate);
        report.rejection_notice(Applied::New);
        report.generation(1);
        report.generation(1);
        report.generation(2);
        report.tick();
        report.inspection();
        report.credit_wait();
        report.outstanding(10);
        report.outstanding(4);
        report.consumer_joined();
        report.consumer_left();
        assert!(!report.all_completed());
        assert_eq!(
            report.summary(),
            "SUMMARY readings=4 completed=3 not_admitted=0 processing_failed=0 outcome_unknown=1 \
             effects=1 rejection_notices=1 duplicates=1 generations=2 ticks_observed=1 \
             inspections=1 credit_waits=1 peak_outstanding_bytes=10 consumers_joined=1 \
             consumers_left=1"
        );
    }
}
