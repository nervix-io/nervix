//! Installing the detector, and handing each cycle it reports to the installer's sink.
//!
//! Deloxide reports a cycle by calling one process-wide callback on a dispatcher thread of its own,
//! through a channel it never bounds, and it catches a panic in that callback and carries on: a
//! callback that fails cannot fail the process. So the callback here does as little as it can. It
//! copies the report's identities into a bounded queue, counts a report the full queue refuses, and
//! wakes the findings thread; it takes no lock, writes no log and cannot panic. The findings thread
//! correlates each report with the registry and calls the sink, and a sink that panics aborts the
//! process, because a finding it failed to handle must not pass for no finding at all.
//!
//! Deloxide prints a banner on standard output when it starts. Standard output carries a node's
//! logs and the completion scripts it prints, so the start runs with the descriptor pointed at
//! `/dev/null`, before any other thread of the process writes to it, and restored afterwards.
//!
//! Only cycles of the wait-for graph reach the callback: Deloxide reports potential lock-order
//! cycles only when it is built with its `lock-order-graph` feature, which this crate does not
//! enable and the primitive boundary allows no other crate to name.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "the detector's report hand-off is a primitive mechanism of the deloxide \
                  backend; consumer acquisitions are checked at their resolved calls"
    )
)]

use std::{
    fs::OpenOptions,
    io::{self, Write as _},
    num::NonZeroU64,
    panic::{AssertUnwindSafe, catch_unwind},
    thread::{self, Thread},
    time::SystemTime,
};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};

use super::{ActiveCycle, Finding, MAX_CYCLE_THREADS, registry::Registry};
use crate::{
    collections::ConcurrentQueue,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

/// How many reports wait for the sink at most. The first active cycle normally ends a diagnostic
/// process, so the queue needs room only for the cycles detected while the sink handles it.
const HANDOFF_CAPACITY: usize = 64;

const FINDINGS_THREAD: &str = "nervix-deadlock-findings";

/// Set by the first installation attempt, successful or not. Standalone flags: neither publishes
/// any other state, which the detector, the queue and the thread handles synchronize themselves.
static CLAIMED: AtomicBool = AtomicBool::new(false);
static INSTALLED: AtomicBool = AtomicBool::new(false);

/// Proof that this process installed its detector. The detector stays installed for the life of the
/// process: dropping this does not remove it.
#[derive(Debug)]
pub struct Detector {
    _installed: (),
}

/// Why the detector could not be installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InstallError {
    #[error("this process has already installed, or tried to install, its deadlock detector")]
    AlreadyClaimed,
    #[error("the thread that hands deadlock findings to their sink could not be started")]
    StartFindingsThread,
    #[error("standard output could not be redirected while the deadlock detector starts")]
    SilenceStandardOutput,
    #[error("standard output could not be restored after the deadlock detector started")]
    RestoreStandardOutput,
    #[error("the deadlock detector refused to start")]
    Start,
}

/// Whether this process installed its detector.
pub fn is_installed() -> bool {
    INSTALLED.load(Ordering::Relaxed)
}

/// Fail the calling operation unless this process installed its detector.
///
/// A tracked lock constructed before the detector is installed would run untracked in all but name:
/// Deloxide drops a cycle it detects while no callback is installed. So a `deloxide` build refuses
/// it, as a modeled build refuses a modeled primitive used outside its model.
#[track_caller]
pub(crate) fn require_installed() {
    assert!(
        is_installed(),
        "a tracked lock was constructed before this process installed its deadlock detector; a \
         `deloxide` build installs it through nervix_primitives::deadlock::install before any \
         tracked lock or runtime worker exists"
    );
}

/// Install this process's deadlock detector, and deliver every finding to `sink` on a thread of its
/// own, one at a time and in the order the detector made them.
///
/// Install once, before any tracked lock is constructed and before any runtime worker starts, and
/// after the process has registered the signals it must not lose: the detector's threads exist from
/// then on. Nothing uninstalls it. A second call fails, whether or not the first succeeded.
///
/// `sink` runs on the findings thread and decides what a finding means for the process. It must not
/// wait for a tracked lock, which a blocked thread may hold. A sink that panics aborts the process.
pub fn install<Sink>(sink: Sink) -> Result<Detector, Report<InstallError>>
where
    Sink: FnMut(Finding) + Send + 'static,
{
    if CLAIMED.swap(true, Ordering::Relaxed) {
        return Err(Report::new(InstallError::AlreadyClaimed));
    }
    let handoff = Arc::new(Handoff {
        reports: ConcurrentQueue::bounded(HANDOFF_CAPACITY),
        lost: AtomicU64::new(0),
    });
    let findings = {
        let handoff = Arc::clone(&handoff);
        thread::Builder::new()
            .name(FINDINGS_THREAD.to_string())
            .spawn(move || handoff.deliver(sink))
            .change_context(InstallError::StartFindingsThread)?
    };
    // Nothing joins the findings thread: it runs for the life of the process.
    let findings = findings.thread().clone();
    start_quietly(move |report: deloxide::DeadlockInfo| handoff.accept(report, &findings))?;
    INSTALLED.store(true, Ordering::Relaxed);
    Ok(Detector { _installed: () })
}

/// Start Deloxide with `callback`, keeping its banner off standard output.
fn start_quietly<Callback>(callback: Callback) -> Result<(), Report<InstallError>>
where
    Callback: Fn(deloxide::DeadlockInfo) + Send + Sync + 'static,
{
    let saved =
        nix::unistd::dup(io::stdout()).change_context(InstallError::SilenceStandardOutput)?;
    let discard = OpenOptions::new()
        .write(true)
        .open("/dev/null")
        .change_context(InstallError::SilenceStandardOutput)?;
    // Whatever the process already wrote belongs on the real standard output.
    io::stdout()
        .flush()
        .change_context(InstallError::SilenceStandardOutput)?;
    nix::unistd::dup2_stdout(&discard).change_context(InstallError::SilenceStandardOutput)?;
    let started = deloxide::Deloxide::new().callback(callback).start();
    // The banner ends with a line break, which flushed it; whatever remains goes to `/dev/null`.
    let flushed = io::stdout().flush();
    nix::unistd::dup2_stdout(&saved).change_context(InstallError::RestoreStandardOutput)?;
    flushed.change_context(InstallError::SilenceStandardOutput)?;
    match started {
        Ok(()) => Ok(()),
        Err(error) => Err(Report::new(InstallError::Start).attach_printable(format!("{error:#}"))),
    }
}

/// The queue between Deloxide's callback and the findings thread.
struct Handoff {
    reports: ConcurrentQueue<CycleReport>,
    /// Reports the full queue refused since the findings thread last looked.
    lost: AtomicU64,
}

/// The identities of one report, copied out of Deloxide's.
struct CycleReport {
    detected_at: SystemTime,
    threads: Vec<usize>,
    waits: Vec<Wait>,
}

/// A thread of a reported cycle and the lock Deloxide reported it waiting for.
struct Wait {
    thread: usize,
    lock: usize,
}

impl Handoff {
    /// Deloxide's callback, on its dispatcher thread.
    fn accept(&self, report: deloxide::DeadlockInfo, findings: &Thread) {
        let mut waits = Vec::with_capacity(report.thread_waiting_for_locks.len());
        for (thread, lock) in report.thread_waiting_for_locks {
            waits.push(Wait { thread, lock });
        }
        let report = CycleReport {
            detected_at: SystemTime::now(),
            threads: report.thread_cycle,
            waits,
        };
        if self.reports.push(report).is_err() {
            // A count of lost reports means at least that many, so at the largest count it stays.
            let counted = self
                .lost
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |lost| {
                    Some(lost.checked_add(1).unwrap_or(lost))
                });
            counted.assured("the update always produces a count");
        }
        findings.unpark();
    }

    /// The findings thread: deliver every report, and the count of lost ones, until the process
    /// ends.
    fn deliver<Sink>(&self, mut sink: Sink)
    where
        Sink: FnMut(Finding),
    {
        loop {
            while let Ok(report) = self.reports.pop() {
                deliver_one(|| {
                    let finding = Finding::ActiveCycle(report.describe(Registry::global()));
                    sink(finding);
                });
            }
            if let Some(lost) = NonZeroU64::new(self.lost.swap(0, Ordering::Relaxed)) {
                deliver_one(|| sink(Finding::Overflow { lost }));
            }
            thread::park();
        }
    }
}

impl CycleReport {
    /// The cycle in source terms: every thread in cycle order, up to the bound, with what the
    /// registry recorded for it.
    fn describe(self, registry: &Registry) -> ActiveCycle {
        let mut threads = Vec::with_capacity(self.threads.len().min(MAX_CYCLE_THREADS));
        for thread in self.threads.iter().take(MAX_CYCLE_THREADS) {
            let mut waits_for = None;
            for wait in &self.waits {
                if wait.thread == *thread {
                    waits_for = Some(wait.lock);
                }
            }
            threads.push(registry.blocked_thread(*thread, waits_for));
        }
        let omitted = self
            .threads
            .len()
            .checked_sub(threads.len())
            .assured("a cycle describes at most the threads it has");
        let omitted = u64::try_from(omitted).assured("supported targets address at most 64 bits");
        ActiveCycle::new(self.detected_at, threads, omitted).assured(
            "Deloxide reports only a validated, non-empty cycle, and the bound is applied above",
        )
    }
}

/// Run one delivery to the sink. A delivery that panics leaves a finding unhandled, so the process
/// aborts rather than pass for one that found nothing.
fn deliver_one<Delivery>(delivery: Delivery)
where
    Delivery: FnOnce(),
{
    if catch_unwind(AssertUnwindSafe(delivery)).is_ok() {
        return;
    }
    let message =
        b"nervix deadlock detector: a finding could not be delivered to its sink; aborting \
                    so the run cannot pass\n";
    let mut written = 0;
    while written < message.len() {
        match nix::unistd::write(io::stderr(), &message[written..]) {
            Ok(0) | Err(_) => break,
            Ok(count) => {
                written = written
                    .checked_add(count)
                    .assured("a write reports at most the bytes it was given");
            }
        }
    }
    std::process::abort();
}
