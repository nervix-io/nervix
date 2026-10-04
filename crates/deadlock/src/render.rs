//! The description of a finding a diagnostic process writes to standard error.
//!
//! The description is bounded by the finding's own bounds: one line per thread a cycle describes,
//! and texts no longer than a bounded text keeps. It names threads, locks and source sites, and
//! never a lock's value, which the finding does not hold.

use std::fmt::Write as _;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::deadlock::{ActiveCycle, BlockedThread, OrderLock, PotentialCycle};

use crate::{PotentialTriage, RecordedFinding as Finding};

/// The lines that describe `finding`, each ending with a line break.
pub fn render_finding(finding: &Finding) -> String {
    let mut text = String::new();
    match finding {
        Finding::ActiveCycle(cycle) => render_cycle(&mut text, cycle),
        Finding::Potential {
            cycle,
            repetitions,
            triage,
        } => {
            render_order(&mut text, cycle, repetitions.get(), triage);
        }
        Finding::Overflow { lost, source } => {
            writeln!(
                text,
                "nervix deadlock detector: overload in {source:?}: at least {lost} findings or \
                 source contexts were lost; this run cannot qualify"
            )
            .assured("writing to a string cannot fail");
        }
    }
    text
}

fn render_order(
    text: &mut String,
    cycle: &PotentialCycle,
    repetitions: u64,
    triage: &PotentialTriage,
) {
    writeln!(
        text,
        "nervix deadlock detector: potential lock-order cycle, {repetitions} reports, detected at \
         {}",
        humantime::format_rfc3339_nanos(cycle.detected_at())
    )
    .assured("writing to a string cannot fail");
    for edge in cycle.edges() {
        write!(text, "  ").assured("writing to a string cannot fail");
        render_order_lock(text, &edge.before);
        write!(text, " -> ").assured("writing to a string cannot fail");
        render_order_lock(text, &edge.after);
        writeln!(text).assured("writing to a string cannot fail");
        if edge.witnesses().is_empty() {
            writeln!(text, "    acquisition context was not recorded")
                .assured("writing to a string cannot fail");
        }
        for witness in edge.witnesses() {
            writeln!(
                text,
                "    {}: held {} access at {}; requested {} access at {}; {} attempts, {} live \
                 guards at that held site",
                witness.thread,
                witness.held.access,
                witness.held.at,
                witness.requested.access,
                witness.requested.at,
                witness.attempts,
                witness.held_count
            )
            .assured("writing to a string cannot fail");
            match &witness.name {
                Some(name) => writeln!(text, "      thread name: {name}"),
                None => writeln!(text, "      thread name was not recorded"),
            }
            .assured("writing to a string cannot fail");
        }
        if edge.omitted_witnesses() > 0 {
            writeln!(
                text,
                "    {} witness contexts omitted",
                edge.omitted_witnesses()
            )
            .assured("writing to a string cannot fail");
        }
    }
    if cycle.omitted_edges() > 0 {
        writeln!(text, "  {} cycle edges omitted", cycle.omitted_edges())
            .assured("writing to a string cannot fail");
    }
    match triage {
        PotentialTriage::Unreviewed => writeln!(
            text,
            "  unreviewed: historical order does not establish an active outage; qualification \
             requires a correction or a specific proof and retained regression"
        ),
        PotentialTriage::Reviewed(proof) => writeln!(
            text,
            "  reviewed {:?}: {}; retained regression: {}",
            proof.basis(),
            proof.reason(),
            proof.regression()
        ),
    }
    .assured("writing to a string cannot fail");
}

fn render_order_lock(text: &mut String, lock: &OrderLock) {
    write!(text, "{} ({:?})", lock.id, lock.lifetime).assured("writing to a string cannot fail");
    match &lock.site {
        Some(site) => write!(
            text,
            ", {} constructed at {}",
            site.kind, site.constructed_at
        ),
        None => write!(text, ", construction context was not recorded"),
    }
    .assured("writing to a string cannot fail");
}

fn render_cycle(text: &mut String, cycle: &ActiveCycle) {
    let described = cycle.threads().len();
    let threads = u64::try_from(described)
        .assured("supported targets address at most 64 bits")
        .checked_add(cycle.omitted_threads())
        .assured("a cycle has fewer threads than a process can number");
    writeln!(
        text,
        "nervix deadlock detector: active deadlock among {threads} {}, detected at {}",
        if threads == 1 { "thread" } else { "threads" },
        humantime::format_rfc3339_nanos(cycle.detected_at())
    )
    .assured("writing to a string cannot fail");
    for thread in cycle.threads() {
        render_thread(text, thread);
    }
    if cycle.omitted_threads() > 0 {
        writeln!(
            text,
            "  ... and {} more threads, beyond the {described} a finding describes",
            cycle.omitted_threads()
        )
        .assured("writing to a string cannot fail");
    }
    let closing = if threads == 1 {
        "  the thread waits for a lock it holds itself"
    } else {
        "  each thread waits for a lock the next one holds, and the last for one the first holds"
    };
    writeln!(text, "{closing}").assured("writing to a string cannot fail");
}

fn render_thread(text: &mut String, thread: &BlockedThread) {
    write!(text, "  {}", thread.thread).assured("writing to a string cannot fail");
    match &thread.name {
        Some(name) => write!(text, " ({name})"),
        None => write!(text, " (unnamed)"),
    }
    .assured("writing to a string cannot fail");
    match &thread.waits_for {
        Some(lock) => {
            write!(text, " waits for {}", lock.id).assured("writing to a string cannot fail");
            match &lock.site {
                Some(site) => write!(
                    text,
                    ", a {} constructed at {}",
                    site.kind, site.constructed_at
                ),
                None => write!(text, ", whose construction was not recorded"),
            }
            .assured("writing to a string cannot fail");
        }
        None => write!(text, " waits for a lock the detector did not name")
            .assured("writing to a string cannot fail"),
    }
    match &thread.attempt {
        Some(attempt) => writeln!(
            text,
            "; it asked for {} access at {}",
            attempt.access, attempt.at
        ),
        None => writeln!(text, "; where it asked for the lock was not recorded"),
    }
    .assured("writing to a string cannot fail");
}
