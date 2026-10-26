//! How long the runner task goes without polling the scenarios it drives.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** A periodic tick polled beside a run on the same task, how late each tick is polled,
//!   the totals of that lateness over a run, which ticks reached the stall bound, the CPU and
//!   run-queue time of the thread that polls the run, and keeping the allocator's purging of freed
//!   memory off that thread.
//! - **Depends on.** The binary's runtime and its monotonic timers, the calling thread's procfs
//!   scheduling counters, and jemalloc's control interface.
//! - **Must not know.** Which step held the task, or what a reporter does with a tick or a stall.
//!
//! Cucumber polls every running scenario, and the suite watchdog around them, from one runner task.
//! A step that blocks inside its poll holds every other scenario, and nothing else in the process
//! sees it: the watchdog is polled on that same task. A tick polled on the task, however, is polled
//! only once the blocking step returns, so the lateness of the tick is the length of the stall.
//!
//! One poll of the run polls every scenario woken since the last one, so a tick is also late when
//! many scenarios each do a little synchronous work in the same poll. The total lateness of every
//! tick, and the time the thread spent on a CPU, therefore measure how busy the runner task is, not
//! only its longest stalls: every scenario's steps wait for the same thread.
//!
//! That thread also allocates and frees on behalf of every scenario. jemalloc returns freed memory
//! to the kernel from whichever thread happens to allocate when a purge falls due, and each purge
//! in a process of hundreds of threads waits for a TLB shootdown across the CPUs running them, which
//! a virtual machine makes slow. Purging on jemalloc's own background threads keeps that wait off
//! the runner thread.

use std::{
    fmt, io,
    path::{Path, PathBuf},
    time::Duration,
};

use meticulous::OptionExt as _;
use nervix_primitives::time::Instant;

/// Where a thread's `schedstat` puts the nanoseconds the thread ran on a CPU, and the nanoseconds
/// it spent ready to run without one.
const SCHEDSTAT_ON_CPU: usize = 0;
const SCHEDSTAT_RUN_QUEUE_WAIT: usize = 1;

/// A period during which the runner task polled nothing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RunnerStall {
    /// How long after its due time the tick was polled.
    pub(crate) late: Duration,
}

impl fmt::Display for RunnerStall {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "runner task stalled for {:?}", self.late)
    }
}

/// One tick of the observer, polled `late` after it was due.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RunnerTick {
    /// How long after its due time the tick was polled.
    pub(crate) late: Duration,
    /// The shortest lateness the observer reports as a stall.
    pub(crate) reported: Duration,
}

impl RunnerTick {
    /// The stall this tick shows, when its lateness reached the observer's bound.
    pub(crate) fn stall(self) -> Option<RunnerStall> {
        if self.late < self.reported {
            return None;
        }
        Some(RunnerStall { late: self.late })
    }
}

/// The lateness of every tick of a run, added up.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RunnerLag {
    /// How many ticks the run polled.
    pub(crate) ticks: u64,
    /// How long after their due times the ticks were polled, together.
    pub(crate) late: Duration,
    /// How many ticks reached the stall bound.
    pub(crate) stalls: u64,
    /// The lateness of those ticks, together.
    pub(crate) stalled: Duration,
    /// The latest any tick was polled.
    pub(crate) longest: Duration,
}

impl RunnerLag {
    /// Adds one polled tick.
    pub(crate) fn record(&mut self, tick: RunnerTick) {
        self.ticks = self
            .ticks
            .checked_add(1)
            .assured("a run polls one tick a period, far fewer than 2^64");
        self.late = self
            .late
            .checked_add(tick.late)
            .assured("ticks of a run are late by far less in total than the range of a Duration");
        self.longest = self.longest.max(tick.late);
        if let Some(stall) = tick.stall() {
            self.stalls = self
                .stalls
                .checked_add(1)
                .assured("a run reports far fewer stalls than ticks");
            self.stalled = self
                .stalled
                .checked_add(stall.late)
                .assured("the stalls of a run add up to less than their ticks' lateness");
        }
    }
}

impl fmt::Display for RunnerLag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} ticks polled {:.1}s late in total; {} stalls took {:.1}s, the longest {:.1}s",
            self.ticks,
            self.late.as_secs_f64(),
            self.stalls,
            self.stalled.as_secs_f64(),
            self.longest.as_secs_f64()
        )
    }
}

/// The tick a run is observed with.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RunnerLagObserver {
    /// How often the task is expected to poll the tick. A stall is measured to within one period.
    pub(crate) period: Duration,
    /// The shortest lateness a tick reports as a stall.
    pub(crate) reported: Duration,
}

impl RunnerLagObserver {
    /// Polls `run` to its end on the calling task, and hands `report` every tick it polled with how
    /// late it was, so the reporter both adds up the lateness and sees each stall at or above the
    /// observer's bound. The tick is polled before `run` on each wake-up, so a stall inside the
    /// poll that ends `run` is reported too.
    pub(crate) async fn observe<F: Future>(
        self,
        run: F,
        mut report: impl FnMut(RunnerTick),
    ) -> F::Output {
        let mut run = std::pin::pin!(run);
        loop {
            nervix_primitives::task::consume_budget().await;
            let due = Instant::now()
                .checked_add(self.period)
                .assured("a tick period of a fraction of a second fits after the present instant");
            nervix_primitives::select! {
                biased;
                () = nervix_primitives::time::sleep_until(due) => {
                    report(RunnerTick {
                        late: due.elapsed(),
                        reported: self.reported,
                    });
                }
                output = &mut run => return output,
            }
        }
    }
}

/// The scheduling counters of one thread: how long it ran on a CPU and how long it was ready to
/// run without one, since the thread began.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ThreadTimes {
    pub(crate) on_cpu: Duration,
    pub(crate) run_queue_wait: Duration,
}

impl ThreadTimes {
    /// The counters accumulated since `earlier` was read of the same thread.
    pub(crate) fn since(self, earlier: Self) -> Self {
        let on_cpu = self
            .on_cpu
            .checked_sub(earlier.on_cpu)
            .assured("a thread's schedstat CPU time only grows");
        let run_queue_wait = self
            .run_queue_wait
            .checked_sub(earlier.run_queue_wait)
            .assured("a thread's schedstat run-queue wait only grows");
        Self {
            on_cpu,
            run_queue_wait,
        }
    }

    /// Reads the counters a thread's `schedstat` holds.
    pub(crate) fn parse(schedstat: &str) -> Option<Self> {
        let fields: Vec<&str> = schedstat.split_whitespace().collect();
        let on_cpu = fields.get(SCHEDSTAT_ON_CPU)?.parse().ok()?;
        let run_queue_wait = fields.get(SCHEDSTAT_RUN_QUEUE_WAIT)?.parse().ok()?;
        Some(Self {
            on_cpu: Duration::from_nanos(on_cpu),
            run_queue_wait: Duration::from_nanos(run_queue_wait),
        })
    }
}

/// How busy the runtime's workers have been, together, since the runtime began.
///
/// The in-process nodes of every running scenario run their tasks on these workers, so beside the
/// runner thread's time this says whether the nodes or the steps are what the run waits for.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WorkerBusy {
    pub(crate) workers: usize,
    pub(crate) busy: Duration,
}

impl WorkerBusy {
    /// Reads the workers of the runtime the caller runs in, or nothing outside a runtime.
    pub(crate) fn of_current_runtime() -> Option<Self> {
        let handle = nervix_primitives::runtime::Handle::try_current().ok()?;
        let metrics = handle.metrics();
        let workers = metrics.num_workers();
        let mut busy = Duration::ZERO;
        for worker in 0..workers {
            busy = busy
                .checked_add(metrics.worker_total_busy_duration(worker))
                .assured(
                    "the workers of one run are busy for far less than the range of a Duration",
                );
        }
        Some(Self { workers, busy })
    }

    /// The busy time the workers accumulated since `earlier` was read of the same runtime.
    pub(crate) fn since(self, earlier: Self) -> Duration {
        self.busy
            .checked_sub(earlier.busy)
            .assured("a runtime's workers only accumulate busy time")
    }
}

/// Moves jemalloc's purging of freed memory onto its own background threads for the rest of the
/// process, so no thread that allocates, the runner thread above all, waits for the TLB shootdown a
/// purge costs.
///
/// In the measurement of 26 October 2026 the thread that polls every scenario ran on a CPU for 86%
/// of a CI run, and a profile of the same workload found most of that time in the kernel, waiting
/// for those shootdowns while every scenario's steps waited for the thread. jemalloc keeps the
/// decay schedule it purges on; only the thread that purges changes.
pub(crate) fn purge_freed_memory_in_the_background() -> tikv_jemalloc_ctl::Result<()> {
    tikv_jemalloc_ctl::background_thread::write(true)
}

/// What procfs said about the thread a run is polled on.
#[derive(Debug, Clone)]
pub(crate) enum RunnerThread {
    /// The thread's `schedstat`, and its counters when the run began.
    Named {
        schedstat: PathBuf,
        began: ThreadTimes,
    },
    /// procfs did not name the thread or describe its counters in a form this harness reads.
    Unreadable(io::ErrorKind),
}

impl RunnerThread {
    /// The calling thread, with its counters as they stand now.
    ///
    /// The thread is named by its own procfs directory rather than by `/proc/thread-self`, so the
    /// counters read later are this thread's whichever thread reads them.
    pub(crate) fn current() -> Self {
        let task = match std::fs::read_link("/proc/thread-self") {
            Ok(task) => task,
            Err(error) => return Self::Unreadable(error.kind()),
        };
        let schedstat = PathBuf::from("/proc").join(task).join("schedstat");
        match Self::read(&schedstat) {
            Ok(began) => Self::Named { schedstat, began },
            Err(kind) => Self::Unreadable(kind),
        }
    }

    /// The counters the thread accumulated since [`Self::current`] named it.
    pub(crate) fn since_began(&self) -> Result<ThreadTimes, io::ErrorKind> {
        match self {
            Self::Named { schedstat, began } => {
                let now = Self::read(schedstat)?;
                Ok(now.since(*began))
            }
            Self::Unreadable(kind) => Err(*kind),
        }
    }

    fn read(schedstat: &Path) -> Result<ThreadTimes, io::ErrorKind> {
        let text = std::fs::read_to_string(schedstat).map_err(|error| error.kind())?;
        ThreadTimes::parse(&text).ok_or(io::ErrorKind::InvalidData)
    }
}
