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
//! The callback preserves the upstream source: active wait-for cycles and historical order cycles
//! are different findings. Order instrumentation is compiled only by `deloxide-order`; an
//! instrumented build can disable checking at runtime, but keeps the upstream acquisition cost.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "the detector's report hand-off is a primitive mechanism of the deloxide \
                  backend; consumer acquisitions are checked at their resolved calls"
    )
)]

use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{self, Write as _},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::OnceLock,
    thread::{self, Thread},
    time::SystemTime,
};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};

use super::{
    ActiveCycle, DiagnosticSelection, Finding, MAX_CYCLE_THREADS, MAX_ORDER_EDGES, OverflowSource,
    handoff::ReportHandoff, registry::Registry,
};
use crate::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// How many reports wait for the sink at most. The first active cycle normally ends a diagnostic
/// process, so the queue needs room only for the cycles detected while the sink handles it.
const HANDOFF_CAPACITY: usize = 64;

const FINDINGS_THREAD: &str = "nervix-deadlock-findings";

/// Set by the first installation attempt, successful or not. Standalone flags: neither publishes
/// any other state, which the detector, the queue and the thread handles synchronize themselves.
static CLAIMED: AtomicBool = AtomicBool::new(false);
static ORDER_ENABLED: AtomicBool = AtomicBool::new(false);

// Installation readiness is the OnceLock's initialized state itself. No standalone flag is used
// to publish another location; the same publication holds the queue the callback uses.
static SELF_WAIT_HANDOFF: OnceLock<SelfWaitHandoff> = OnceLock::new();
struct SelfWaitHandoff {
    handoff: Arc<Handoff>,
    findings: Thread,
}

/// A failed mutex acquisition by a thread with its own live exclusive guard is an active cycle
/// of one. The upstream common-lock filter cannot establish infeasibility for one participant:
/// when order instrumentation or stress records its held set, it filters this real cycle. The boundary
/// reports the actual guard/failed-acquisition pair through its ordinary findings handoff.
pub(crate) fn report_self_wait(registry: &Registry, lock: usize) {
    if !registry.history.holds_exclusively(lock) {
        return;
    }
    let Some(thread) = registry.current_thread() else {
        registry.history.loss();
        return;
    };
    let thread =
        usize::try_from(thread.get().get()).assured("the vendor assigned this identity as usize");
    let handoff = SELF_WAIT_HANDOFF
        .get()
        .assured("tracked acquisitions require successful installation");
    handoff
        .handoff
        .reports
        .submit(CycleReport::Active(ActiveReport {
            detected_at: SystemTime::now(),
            threads: vec![thread],
            waits: vec![Wait { thread, lock }],
            omitted_threads: 0,
        }));
    handoff.findings.unpark();
}

pub fn order_enabled() -> bool {
    cfg!(feature = "deloxide-order") && ORDER_ENABLED.load(Ordering::Relaxed)
}

/// Proof that this process installed its detector. The detector stays installed for the life of the
/// process: dropping this does not remove it.
#[derive(Debug)]
pub struct Detector {
    _installed: (),
}

/// Why the detector could not be installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InstallError {
    #[error("diagnostic selection {selection:?} is unavailable in this build")]
    SelectionUnavailable { selection: DiagnosticSelection },
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
    SELF_WAIT_HANDOFF.get().is_some()
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
    install_selected(DiagnosticSelection::for_build(false), sink)
}

pub fn install_selected<Sink>(
    selection: DiagnosticSelection,
    sink: Sink,
) -> Result<Detector, Report<InstallError>>
where
    Sink: FnMut(Finding) + Send + 'static,
{
    if !selection.is_available() {
        return Err(Report::new(InstallError::SelectionUnavailable {
            selection,
        }));
    }
    if CLAIMED.swap(true, Ordering::Relaxed) {
        return Err(Report::new(InstallError::AlreadyClaimed));
    }
    let handoff = Arc::new(Handoff {
        reports: ReportHandoff::new(HANDOFF_CAPACITY),
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
    ORDER_ENABLED.store(selection.checks_order(), Ordering::Relaxed);
    let callback_handoff = Arc::clone(&handoff);
    let callback_findings = findings.clone();
    if let Err(error) = start_quietly(selection, move |report: deloxide::DeadlockInfo| {
        callback_handoff.accept(report, &callback_findings)
    }) {
        handoff.reports.close();
        findings.unpark();
        return Err(error);
    }
    assert!(
        SELF_WAIT_HANDOFF
            .set(SelfWaitHandoff { handoff, findings })
            .is_ok(),
        "installation is claimed once before publishing its handoff"
    );
    Ok(Detector { _installed: () })
}

/// Start Deloxide with `callback`, keeping its banner off standard output.
fn start_quietly<Callback>(
    selection: DiagnosticSelection,
    callback: Callback,
) -> Result<(), Report<InstallError>>
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
    let detector = deloxide::Deloxide::new().callback(callback);
    #[cfg(feature = "deloxide-order")]
    let detector = if selection.checks_order() {
        detector.with_lock_order_checking()
    } else {
        detector.no_lock_order_checking()
    };
    #[cfg(feature = "deloxide-stress")]
    let detector = match selection.stress() {
        Some(configuration) => detector
            .with_random_stress()
            .with_stress_config(vendor_stress(configuration)),
        None => detector,
    };
    #[cfg(not(any(feature = "deloxide-order", feature = "deloxide-stress")))]
    let _selection = selection;
    let started = detector.start();
    // The banner ends with a line break, which flushed it; whatever remains goes to `/dev/null`.
    let flushed = io::stdout().flush();
    nix::unistd::dup2_stdout(&saved).change_context(InstallError::RestoreStandardOutput)?;
    flushed.change_context(InstallError::SilenceStandardOutput)?;
    match started {
        Ok(()) => Ok(()),
        Err(error) => Err(Report::new(InstallError::Start).attach_printable(format!("{error:#}"))),
    }
}

/// Deloxide's own form of a stress configuration. The probability is exact: a count of millionths
/// is at most a million, which a double represents exactly, and the delays are whole microseconds.
#[cfg(feature = "deloxide-stress")]
fn vendor_stress(configuration: super::StressConfiguration) -> deloxide::StressConfig {
    let per_million = f64::from(configuration.preemptions_per_million().get());
    let scale = f64::from(super::PREEMPTION_SCALE);
    let micros = |delay: std::time::Duration| {
        u64::try_from(delay.as_micros()).assured("a stress delay is at most two milliseconds")
    };
    deloxide::StressConfig {
        preemption_probability: per_million / scale,
        min_delay_us: micros(configuration.shortest_delay()),
        max_delay_us: micros(configuration.longest_delay()),
        preempt_after_release: configuration.yield_after_release(),
    }
}

/// The queue between Deloxide's callback and the findings thread.
struct Handoff {
    reports: ReportHandoff<CycleReport>,
}

/// The identities of one report, copied out of Deloxide's.
enum CycleReport {
    Active(ActiveReport),
    Potential(OrderReport),
}

struct OrderReport {
    detected_at: SystemTime,
    locks: Vec<usize>,
    total_edges: u64,
}

struct ActiveReport {
    detected_at: SystemTime,
    threads: Vec<usize>,
    waits: Vec<Wait>,
    omitted_threads: u64,
}

/// A thread of a reported cycle and the lock Deloxide reported it waiting for.
struct Wait {
    thread: usize,
    lock: usize,
}

impl Handoff {
    /// Deloxide's callback, on its dispatcher thread.
    fn accept(&self, report: deloxide::DeadlockInfo, findings: &Thread) {
        let report = match report.source {
            deloxide::DeadlockSource::WaitForGraph => {
                let total = report.thread_cycle.len();
                let threads: Vec<_> = report
                    .thread_cycle
                    .iter()
                    .copied()
                    .take(MAX_CYCLE_THREADS)
                    .collect();
                let mut waits = Vec::with_capacity(threads.len());
                // Both retained vectors are bounded, even if the upstream report is larger.
                // The upstream vector is searched, not retained by this handoff.
                for thread in &threads {
                    if let Some((_, lock)) = report
                        .thread_waiting_for_locks
                        .iter()
                        .find(|(waiting, _)| waiting == thread)
                    {
                        waits.push(Wait {
                            thread: *thread,
                            lock: *lock,
                        });
                    }
                }
                let omitted_threads = u64::try_from(total - threads.len())
                    .assured("supported targets address at most 64 bits");
                CycleReport::Active(ActiveReport {
                    detected_at: SystemTime::now(),
                    threads,
                    waits,
                    omitted_threads,
                })
            }
            deloxide::DeadlockSource::LockOrderViolation => {
                let Some(mut locks) = report.lock_order_cycle else {
                    Registry::global().history.loss();
                    findings.unpark();
                    return;
                };
                if locks.first() == locks.last() {
                    locks.pop();
                }
                let total_edges =
                    u64::try_from(locks.len()).assured("supported targets address at most 64 bits");
                // A partial cycle needs the next identity for its last retained directed edge.
                let locks = locks.into_iter().take(MAX_ORDER_EDGES + 1).collect();
                CycleReport::Potential(OrderReport {
                    detected_at: SystemTime::now(),
                    locks,
                    total_edges,
                })
            }
        };
        self.reports.submit(report);
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
                deliver_one(|| match report {
                    CycleReport::Active(report) => {
                        sink(Finding::ActiveCycle(report.describe(Registry::global())))
                    }
                    CycleReport::Potential(report) => {
                        let registry = Registry::global();
                        if let Some(cycle) = registry.history.describe(
                            registry,
                            report.detected_at,
                            report.locks,
                            report.total_edges,
                        ) {
                            sink(Finding::PotentialCycle(cycle));
                        }
                    }
                });
            }
            if let Some(lost) = self.reports.take_lost() {
                deliver_one(|| {
                    sink(Finding::Overflow {
                        lost,
                        source: OverflowSource::Handoff,
                    })
                });
            }
            if let Some(lost) = Registry::global().history.take_lost() {
                deliver_one(|| {
                    sink(Finding::Overflow {
                        lost,
                        source: OverflowSource::OrderHistory,
                    })
                });
            }
            if self.reports.is_closed() {
                return;
            }
            // Context overflow can occur without a Deloxide cycle and must still reach the sink.
            thread::park_timeout(std::time::Duration::from_millis(100));
        }
    }
}

impl ActiveReport {
    /// The cycle in source terms: every thread in cycle order, up to the bound, with what the
    /// registry recorded for it.
    fn describe(self, registry: &Registry) -> ActiveCycle {
        let mut waited = BTreeMap::new();
        for wait in &self.waits {
            waited.insert(wait.thread, wait.lock);
        }
        let mut threads = Vec::with_capacity(self.threads.len().min(MAX_CYCLE_THREADS));
        for thread in self.threads.iter().take(MAX_CYCLE_THREADS) {
            threads.push(registry.blocked_thread(*thread, waited.get(thread).copied()));
        }
        ActiveCycle::new(self.detected_at, threads, self.omitted_threads).assured(
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
