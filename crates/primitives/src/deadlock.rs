//! Deadlocks among the thread-blocking locks, as a `deloxide` build detects and reports them.
//!
//! In a `deloxide` build every lock and condition variable of `sync::blocking` is an adapter over
//! one of Deloxide's tracked locks, and every acquisition that waits updates one process-wide
//! wait-for graph. When a waiting thread closes a cycle in that graph, every thread in the cycle
//! waits for a lock another one holds, and none of them can proceed: an active deadlock. `install`
//! starts the detector that reports such a cycle while its threads are still blocked, and hands each
//! report, as a [`Finding`], to the installer's sink on a thread of its own.
//! A `deloxide-order` build also records bounded acquisition history and correlates the vendor's
//! historical order cycles with held and requested modes, source sites and lock lifetimes. Such a
//! potential cycle does not assert that any thread is blocked. Runtime selection is retained in the
//! evidence; disabling order checking does not remove compile-time instrumentation.
//!
//! A finding names threads and locks by their run-local identities, the numbers the detector
//! assigned in this process, and correlates them with where in the source each lock was constructed
//! and where each thread waits. It never holds the value a lock protects. What the detector could
//! not tell, such as a thread that had no recorded acquisition attempt, stays absent rather than
//! guessed, and every text and every cycle a finding carries is bounded.
//!
//! The types here describe findings in every build, so a process that reads the evidence another
//! one recorded needs no detector of its own. Installing the detector exists only in a `deloxide`
//! build: a build that cannot detect has nothing to install.
//!
//! Layer: primitives.
//!
//! - **Owns.** Installing the detector once per process, handing each cycle it reports to a
//!   dedicated thread through a bounded queue that takes no lock, and the identities, source sites
//!   and bounds that describe a finding.
//! - **Depends on.** Deloxide in a `deloxide` build, the standard library's threads, and the native
//!   capability's lock-free queue and concurrent map.
//! - **Must not know.** What a finding means for the process that reports it: whether it is
//!   rendered, recorded as evidence, or ends the process belongs to the sink the installer supplies.

use std::{fmt, num::NonZeroU64, panic::Location, time::SystemTime};

use meticulous::ResultExt as _;

#[cfg(feature = "native")]
mod handoff;
mod order;
mod stress;
#[cfg(feature = "native")]
pub use handoff::ReportHandoff;
pub use order::{
    LockLifetime, MAX_ORDER_EDGES, MAX_ORDER_WITNESSES, OrderEdge, OrderLock, OrderOutOfBounds,
    OrderWitness, PotentialCycle,
};
pub use stress::{MAX_STRESS_DELAY, PREEMPTION_SCALE, StressConfiguration, StressOutOfBounds};

/// Compile-time instrumentation and runtime checking selected for one diagnostic process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DiagnosticSelection {
    /// No order graph was compiled.
    ActiveOnly,
    /// The graph is compiled and runtime order checking is enabled.
    OrderAnalysis,
    /// The graph is compiled, but runtime order checking is disabled. This still pays the
    /// instrumented acquisition cost and is never an ordinary or active-only fast path.
    OrderInstrumentedActiveOnly,
    /// No order graph was compiled, and Deloxide disturbs the schedule of nested acquisitions as
    /// the configuration says. Only a `deloxide-stress` build selects it.
    StressedActiveOnly(StressConfiguration),
}

impl DiagnosticSelection {
    /// The selection this build runs: a `deloxide-stress` build disturbs its schedule with
    /// [`StressConfiguration::LANE`] and has no order graph to enable, whatever `active_only` says.
    pub const fn for_build(active_only: bool) -> Self {
        if cfg!(feature = "deloxide-stress") {
            Self::StressedActiveOnly(StressConfiguration::LANE)
        } else if cfg!(feature = "deloxide-order") {
            if active_only {
                Self::OrderInstrumentedActiveOnly
            } else {
                Self::OrderAnalysis
            }
        } else {
            Self::ActiveOnly
        }
    }

    pub const fn checks_order(self) -> bool {
        matches!(self, Self::OrderAnalysis)
    }

    /// Whether this build can install the detector with this selection: the selection it was
    /// compiled for, or, in a `deloxide-stress` build, stress with any valid configuration.
    pub const fn is_available(self) -> bool {
        match self {
            Self::StressedActiveOnly(_) => cfg!(feature = "deloxide-stress"),
            Self::ActiveOnly => {
                !cfg!(feature = "deloxide-order") && !cfg!(feature = "deloxide-stress")
            }
            Self::OrderAnalysis | Self::OrderInstrumentedActiveOnly => {
                cfg!(feature = "deloxide-order")
            }
        }
    }

    /// The disturbance this selection applies, when it applies one.
    pub const fn stress(self) -> Option<StressConfiguration> {
        match self {
            Self::StressedActiveOnly(configuration) => Some(configuration),
            Self::ActiveOnly | Self::OrderAnalysis | Self::OrderInstrumentedActiveOnly => None,
        }
    }
}

#[cfg(all(feature = "native", feature = "deloxide", not(feature = "shuttle")))]
pub(crate) mod detector;
#[cfg(all(feature = "native", feature = "deloxide", not(feature = "shuttle")))]
pub(crate) mod order_history;
#[cfg(all(feature = "native", feature = "deloxide", not(feature = "shuttle")))]
pub(crate) mod registry;

#[cfg(all(feature = "native", feature = "deloxide", not(feature = "shuttle")))]
pub use detector::{
    Detector, InstallError, install, install_selected, is_installed, order_enabled,
};

/// The most threads one cycle describes. A longer cycle keeps its first threads in cycle order and
/// counts the rest.
pub const MAX_CYCLE_THREADS: usize = 64;

/// The most bytes a thread name or a source file path keeps.
pub const MAX_TEXT_BYTES: usize = 512;

/// A thread of this process, by the number the detector gave it when the thread first used a
/// tracked lock. Run-local: the same number names another thread in another process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TrackedThreadId(NonZeroU64);

impl TrackedThreadId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }

    pub const fn get(self) -> NonZeroU64 {
        self.0
    }
}

impl fmt::Display for TrackedThreadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "thread {}", self.0)
    }
}

/// A lock or condition variable of this process, by the number the detector gave it when it was
/// constructed. Run-local, and never reused while the lock lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TrackedLockId(NonZeroU64);

impl TrackedLockId {
    pub const fn new(id: NonZeroU64) -> Self {
        Self(id)
    }

    pub const fn get(self) -> NonZeroU64 {
        self.0
    }
}

impl fmt::Display for TrackedLockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lock {}", self.0)
    }
}

/// Text a finding carries, kept to at most [`MAX_TEXT_BYTES`]: a longer text keeps its leading
/// bytes up to a character boundary, and remembers how long it was.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BoundedText {
    text: String,
    original_bytes: u64,
}

/// The shortest a cut text can be: the bound less the three bytes a character can extend past the
/// last boundary within it.
const SHORTEST_CUT_BYTES: usize = MAX_TEXT_BYTES - 3;

impl BoundedText {
    /// `text`, cut to the bound at a character boundary when it is longer.
    pub fn new(text: &str) -> Self {
        let end = text.floor_char_boundary(MAX_TEXT_BYTES);
        Self {
            text: text[..end].to_string(),
            original_bytes: byte_count(text.len()),
        }
    }

    /// The text as it was kept, and the length of the text it was cut from. Refuses a pair that
    /// [`BoundedText::new`] could not have produced: a kept text over the bound, one longer than the
    /// original it claims, or one cut further than the bound requires.
    pub fn from_parts(text: String, original_bytes: u64) -> Result<Self, TextOutOfBounds> {
        let kept = byte_count(text.len());
        if text.len() > MAX_TEXT_BYTES {
            return Err(TextOutOfBounds::Kept { bytes: kept });
        }
        if kept > original_bytes {
            return Err(TextOutOfBounds::LongerThanOriginal {
                kept,
                original: original_bytes,
            });
        }
        if kept < original_bytes && text.len() < SHORTEST_CUT_BYTES {
            return Err(TextOutOfBounds::CutShort {
                kept,
                original: original_bytes,
            });
        }
        Ok(Self {
            text,
            original_bytes,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The length in bytes of the text this one was kept from.
    pub fn original_bytes(&self) -> u64 {
        self.original_bytes
    }

    /// Whether the text was cut to the bound.
    pub fn is_truncated(&self) -> bool {
        byte_count(self.text.len()) < self.original_bytes
    }
}

impl fmt::Display for BoundedText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_truncated() {
            write!(
                f,
                "{}... (cut from {} bytes)",
                self.text, self.original_bytes
            )
        } else {
            f.write_str(&self.text)
        }
    }
}

fn byte_count(length: usize) -> u64 {
    u64::try_from(length).assured("supported targets address at most 64 bits")
}

/// Why a kept text is not one [`BoundedText::new`] could have produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "bounded diagnostic text construction returns a pure validation refusal"
    )
)]
pub enum TextOutOfBounds {
    /// The kept text is longer than the bound.
    Kept { bytes: u64 },
    /// The kept text is longer than the text it claims to be cut from.
    LongerThanOriginal { kept: u64, original: u64 },
    /// The kept text was cut further than the bound requires.
    CutShort { kept: u64, original: u64 },
}

impl fmt::Display for TextOutOfBounds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kept { bytes } => write!(
                f,
                "a kept text of {bytes} bytes is longer than {MAX_TEXT_BYTES}"
            ),
            Self::LongerThanOriginal { kept, original } => write!(
                f,
                "a kept text of {kept} bytes is longer than the {original} bytes it was cut from"
            ),
            Self::CutShort { kept, original } => write!(
                f,
                "a text of {original} bytes was cut to {kept}, further than the bound requires"
            ),
        }
    }
}

impl std::error::Error for TextOutOfBounds {}

/// Where in the source an operation was written: a file, a line and a column, as the compiler
/// records them for a caller.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceSite {
    pub file: BoundedText,
    pub line: u32,
    pub column: u32,
}

impl SourceSite {
    pub fn from_location(location: &Location<'_>) -> Self {
        Self {
            file: BoundedText::new(location.file()),
            line: location.line(),
            column: location.column(),
        }
    }
}

impl fmt::Display for SourceSite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.file, self.line, self.column)
    }
}

/// Which kind of tracked lock a lock is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LockKind {
    Mutex,
    RwLock,
    /// The lock a condition variable keeps its waiters under. A thread waits for it while it starts
    /// or ends a wait, and while it notifies.
    CondvarState,
}

impl fmt::Display for LockKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Mutex => "mutex",
            Self::RwLock => "read-write lock",
            Self::CondvarState => "condition variable",
        })
    }
}

/// How a thread asked for a lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Access {
    /// Alone: a mutex, or a read-write lock for writing.
    Exclusive,
    /// Beside other readers: a read-write lock for reading.
    Shared,
}

impl fmt::Display for Access {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Exclusive => "exclusive",
            Self::Shared => "shared",
        })
    }
}

/// What kind of lock a tracked lock is and where it was constructed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LockSite {
    pub kind: LockKind,
    pub constructed_at: SourceSite,
}

/// A lock a blocked thread waits for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WaitedLock {
    pub id: TrackedLockId,
    /// Absent for a lock this process's adapters did not construct, which the detector can still
    /// name but nothing records a site for.
    pub site: Option<LockSite>,
}

/// The acquisition a blocked thread is waiting in: how it asked for the lock and where.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlockedAttempt {
    pub access: Access,
    pub at: SourceSite,
}

/// One thread of a cycle: it waits for a lock that the next thread of the cycle holds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockedThread {
    pub thread: TrackedThreadId,
    /// Absent for a thread the operating system knows by no name.
    pub name: Option<BoundedText>,
    /// Absent when the detector named no lock for this thread.
    pub waits_for: Option<WaitedLock>,
    /// Absent when no acquisition of the lock the thread waits for was recorded for it.
    pub attempt: Option<BlockedAttempt>,
}

/// Threads that each wait for a lock the next one holds, the last waiting for the first's: none of
/// them can proceed. A thread waiting for a lock it holds itself is a cycle of one.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActiveCycle {
    detected_at: SystemTime,
    threads: Vec<BlockedThread>,
    omitted_threads: u64,
}

impl ActiveCycle {
    /// A cycle detected at `detected_at` of the threads `threads`, in cycle order, with
    /// `omitted_threads` more that the bound left out. Refuses an empty cycle, and one that
    /// describes more than [`MAX_CYCLE_THREADS`] threads or omits threads while describing fewer.
    pub fn new(
        detected_at: SystemTime,
        threads: Vec<BlockedThread>,
        omitted_threads: u64,
    ) -> Result<Self, CycleOutOfBounds> {
        if threads.is_empty() {
            return Err(CycleOutOfBounds::Empty);
        }
        if threads.len() > MAX_CYCLE_THREADS {
            return Err(CycleOutOfBounds::TooManyThreads {
                threads: threads.len(),
            });
        }
        if omitted_threads > 0 && threads.len() < MAX_CYCLE_THREADS {
            return Err(CycleOutOfBounds::OmittedBelowBound {
                threads: threads.len(),
                omitted: omitted_threads,
            });
        }
        Ok(Self {
            detected_at,
            threads,
            omitted_threads,
        })
    }

    pub fn detected_at(&self) -> SystemTime {
        self.detected_at
    }

    /// The threads the cycle describes, in cycle order.
    pub fn threads(&self) -> &[BlockedThread] {
        &self.threads
    }

    /// How many more threads the cycle has than it describes.
    pub fn omitted_threads(&self) -> u64 {
        self.omitted_threads
    }
}

/// Why a cycle is not one the detector reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "bounded cycle construction returns a pure validation refusal"
    )
)]
pub enum CycleOutOfBounds {
    /// A cycle has at least one thread.
    Empty,
    /// The cycle describes more threads than the bound.
    TooManyThreads { threads: usize },
    /// Threads are omitted only once the cycle describes as many as the bound.
    OmittedBelowBound { threads: usize, omitted: u64 },
}

impl fmt::Display for CycleOutOfBounds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("a cycle has no threads"),
            Self::TooManyThreads { threads } => write!(
                f,
                "a cycle describes {threads} threads, more than {MAX_CYCLE_THREADS}"
            ),
            Self::OmittedBelowBound { threads, omitted } => write!(
                f,
                "a cycle omits {omitted} threads while describing only {threads}"
            ),
        }
    }
}

impl std::error::Error for CycleOutOfBounds {}

/// What the detector reports.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Finding {
    /// An active deadlock among tracked locks.
    ActiveCycle(ActiveCycle),
    /// Historical acquisition order, without a claim that threads currently cannot progress.
    PotentialCycle(PotentialCycle),
    /// Findings the detector made while the hand-off to the sink was full, which it could not
    /// describe. Each is at least one more cycle.
    Overflow {
        lost: NonZeroU64,
        source: OverflowSource,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OverflowSource {
    Handoff,
    OrderHistory,
}
