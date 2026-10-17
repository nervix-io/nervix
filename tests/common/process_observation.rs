//! A bounded observation of one running process and of the host it runs on, read from procfs.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** Reading one process's state, CPU time, page faults, resident memory and storage
//!   reads, every thread's state, kernel wait channel and run-queue wait, and the host's load and
//!   pressure, and describing them within a fixed size.
//! - **Depends on.** Linux procfs, read off the runner task.
//! - **Must not know.** What the process runs, why it is observed, or what a test decides from it.
//!
//! An observation tells what a stalled process waits for without stopping it to read its stacks.
//! A thread in disk sleep waits for storage, in a page fault or a read. A thread asleep in a futex
//! waits for a lock or a condition variable, or for work, as an idle runtime worker does. A thread
//! asleep in epoll waits for network or timer events. A running thread is on a CPU or queued for
//! one, and the run-queue wait says how long the threads waited for a CPU they were ready to use.
//! A major fault is a page fault that had to read storage.

use std::{collections::BTreeMap, fmt, io, time::Duration};

use meticulous::OptionExt as _;

use super::runner_offload::off_the_runner;

/// The unit procfs reports CPU times in. Linux fixes `USER_HZ` at 100 ticks per second in the
/// procfs interface of every architecture, whatever the kernel's own tick rate.
const USER_HZ: u64 = 100;
const MILLISECONDS_PER_TICK: u64 = 1000 / USER_HZ;
const _: () = assert!(
    MILLISECONDS_PER_TICK * USER_HZ == 1000,
    "a procfs tick is a whole number of milliseconds"
);
/// How many groups of threads, and how many thread names in one group, a description lists.
/// Policy inputs: a server process runs a few dozen threads in a handful of states, so the bounds
/// only shorten the description of an unusual process.
const MAX_THREAD_GROUPS: usize = 16;
const MAX_NAMES_PER_GROUP: usize = 4;
/// Where `/proc/<pid>/stat` and `/proc/<pid>/task/<tid>/stat` put the fields read here, counted
/// from the state, the first field after the parenthesised command name.
const STAT_STATE: usize = 0;
const STAT_MINOR_FAULTS: usize = 7;
const STAT_MAJOR_FAULTS: usize = 9;
const STAT_USER_TICKS: usize = 11;
const STAT_SYSTEM_TICKS: usize = 12;
const STAT_THREADS: usize = 17;
/// Where a thread's `schedstat` puts the nanoseconds the thread spent ready to run without a CPU.
const SCHEDSTAT_RUN_QUEUE_WAIT: usize = 1;
const BYTES_PER_KILOBYTE: u64 = 1024;

/// One process and the host it runs on, as procfs described them at one moment.
#[derive(Debug)]
pub(crate) struct ProcessObservation {
    pid: u32,
    process: ProcessReading,
    host: HostPressure,
}

/// What procfs said about the process.
#[derive(Debug)]
enum ProcessReading {
    Read {
        counters: ProcessCounters,
        threads: ThreadWaits,
    },
    /// The process had exited and was reaped before it could be read.
    Ended,
    /// procfs did not describe the process in a form this harness reads.
    Unreadable(io::ErrorKind),
}

/// The process-wide counters of one process.
#[derive(Debug)]
struct ProcessCounters {
    state: ThreadState,
    threads: u64,
    cpu: Duration,
    minor_faults: u64,
    major_faults: u64,
    resident_bytes: Option<u64>,
    resident_file_bytes: Option<u64>,
    storage_read_bytes: Option<u64>,
}

/// A thread's scheduling state, as procfs reports it.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ThreadState {
    Running,
    Sleeping,
    DiskSleep,
    Stopped,
    TracingStop,
    Zombie,
    Dead,
    Idle,
    Other(char),
}

impl From<char> for ThreadState {
    fn from(code: char) -> Self {
        match code {
            'R' => Self::Running,
            'S' => Self::Sleeping,
            'D' => Self::DiskSleep,
            'T' => Self::Stopped,
            't' => Self::TracingStop,
            'Z' => Self::Zombie,
            'X' => Self::Dead,
            'I' => Self::Idle,
            other => Self::Other(other),
        }
    }
}

impl fmt::Display for ThreadState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => formatter.write_str("running"),
            Self::Sleeping => formatter.write_str("sleeping"),
            Self::DiskSleep => formatter.write_str("disk sleep"),
            Self::Stopped => formatter.write_str("stopped"),
            Self::TracingStop => formatter.write_str("tracing stop"),
            Self::Zombie => formatter.write_str("zombie"),
            Self::Dead => formatter.write_str("dead"),
            Self::Idle => formatter.write_str("idle"),
            Self::Other(code) => write!(formatter, "state {code}"),
        }
    }
}

/// The kernel function a thread waits in, as `wchan` names it.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum WaitChannel {
    Named(String),
    /// The thread waits in no kernel function: it is running, or ready to.
    NotWaiting,
    /// The channel could not be read.
    Unknown,
}

impl WaitChannel {
    fn of(wchan: &str) -> Self {
        let function = wchan.trim();
        if function.is_empty() || function == "0" {
            return Self::NotWaiting;
        }
        Self::Named(function.to_string())
    }
}

impl fmt::Display for WaitChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Named(function) => formatter.write_str(function),
            Self::NotWaiting => formatter.write_str("no wait"),
            Self::Unknown => formatter.write_str("an unknown wait"),
        }
    }
}

/// What one group of threads has in common.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ThreadWait {
    pub(crate) state: ThreadState,
    pub(crate) channel: WaitChannel,
}

/// A thread's name, as `comm` gives it.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ThreadName {
    Named(String),
    Unreadable,
}

impl fmt::Display for ThreadName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Named(name) => formatter.write_str(name),
            Self::Unreadable => formatter.write_str("an unreadable name"),
        }
    }
}

/// The threads of one group, and how many of them carry each name.
#[derive(Debug, Default)]
struct ThreadNames {
    threads: u64,
    names: BTreeMap<ThreadName, u64>,
}

/// A process's threads, counted by their state and the kernel function each waits in.
#[derive(Debug, Default)]
pub(crate) struct ThreadWaits {
    groups: BTreeMap<ThreadWait, ThreadNames>,
    /// How long the counted threads waited, in total, for a CPU while they were ready to run.
    run_queue_wait: Duration,
    /// Threads that ended between being listed and being read.
    vanished: u64,
    /// Why the threads could not be listed at all, when they could not.
    unlisted: Option<io::ErrorKind>,
}

impl ThreadWaits {
    /// Counts one thread named `name` that `wait` describes.
    pub(crate) fn count(&mut self, wait: ThreadWait, name: ThreadName) {
        let group = self.groups.entry(wait).or_default();
        group.threads = group
            .threads
            .checked_add(1)
            .assured("a process has far fewer than u64::MAX threads");
        let named = group.names.entry(name).or_default();
        *named = named
            .checked_add(1)
            .assured("a process has far fewer than u64::MAX threads");
    }

    /// Adds one thread's run-queue wait to the total.
    pub(crate) fn add_run_queue_wait(&mut self, wait: Duration) {
        self.run_queue_wait = self.run_queue_wait.checked_add(wait).assured(
            "the run-queue waits of one process's threads total far less than u64 seconds",
        );
    }

    fn count_vanished(&mut self) {
        self.vanished = self
            .vanished
            .checked_add(1)
            .assured("a process has far fewer than u64::MAX threads");
    }

    fn read(pid: u32) -> Self {
        let mut waits = Self::default();
        let tasks = match std::fs::read_dir(format!("/proc/{pid}/task")) {
            Ok(tasks) => tasks,
            Err(error) => {
                waits.unlisted = Some(error.kind());
                return waits;
            }
        };
        for task in tasks {
            let Ok(task) = task else {
                waits.count_vanished();
                continue;
            };
            let directory = task.path();
            let Ok(stat) = std::fs::read_to_string(directory.join("stat")) else {
                waits.count_vanished();
                continue;
            };
            let Some(fields) = StatFields::of(&stat) else {
                waits.count_vanished();
                continue;
            };
            let Some(state) = fields.state() else {
                waits.count_vanished();
                continue;
            };
            let name = match std::fs::read_to_string(directory.join("comm")) {
                Ok(name) => ThreadName::Named(name.trim().to_string()),
                Err(_) => ThreadName::Unreadable,
            };
            let channel = match std::fs::read_to_string(directory.join("wchan")) {
                Ok(wchan) => WaitChannel::of(&wchan),
                Err(_) => WaitChannel::Unknown,
            };
            if let Ok(schedstat) = std::fs::read_to_string(directory.join("schedstat"))
                && let Some(wait) = Self::run_queue_wait(&schedstat)
            {
                waits.add_run_queue_wait(wait);
            }
            waits.count(ThreadWait { state, channel }, name);
        }
        waits
    }

    /// The run-queue wait one thread's `schedstat` records.
    fn run_queue_wait(schedstat: &str) -> Option<Duration> {
        let field = schedstat.split_whitespace().nth(SCHEDSTAT_RUN_QUEUE_WAIT)?;
        let nanoseconds = field.parse().ok()?;
        Some(Duration::from_nanos(nanoseconds))
    }
}

impl fmt::Display for ThreadWaits {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(kind) = self.unlisted {
            return write!(formatter, "threads could not be listed: {kind}");
        }
        write!(
            formatter,
            "threads by state and kernel wait, {:.2?} of run-queue wait in total:",
            self.run_queue_wait
        )?;
        // The largest groups first, so the bound drops the rarest waits.
        let mut groups: Vec<(&ThreadWait, &ThreadNames)> = self.groups.iter().collect();
        groups.sort_by(|left, right| right.1.threads.cmp(&left.1.threads));
        for (wait, names) in groups.iter().take(MAX_THREAD_GROUPS) {
            write!(
                formatter,
                " [{} threads {} in {}:",
                names.threads, wait.state, wait.channel
            )?;
            let mut listed: Vec<(&ThreadName, &u64)> = names.names.iter().collect();
            listed.sort_by(|left, right| right.1.cmp(left.1));
            let mut separator = " ";
            for (name, threads) in listed.iter().take(MAX_NAMES_PER_GROUP) {
                write!(formatter, "{separator}{name} ×{threads}")?;
                separator = ", ";
            }
            if let Some(unlisted) = listed.len().checked_sub(MAX_NAMES_PER_GROUP)
                && unlisted > 0
            {
                write!(formatter, " and {unlisted} other names")?;
            }
            formatter.write_str("]")?;
        }
        if let Some(unlisted) = groups.len().checked_sub(MAX_THREAD_GROUPS)
            && unlisted > 0
        {
            write!(formatter, " and {unlisted} smaller groups")?;
        }
        if self.vanished > 0 {
            write!(
                formatter,
                "; {} threads ended while being read",
                self.vanished
            )?;
        }
        Ok(())
    }
}

/// The fields of a procfs `stat` file after the parenthesised command name, which may itself hold
/// spaces and parentheses.
struct StatFields<'a> {
    fields: Vec<&'a str>,
}

impl<'a> StatFields<'a> {
    fn of(stat: &'a str) -> Option<Self> {
        let name_end = stat.rfind(')')?;
        let fields_start = name_end.checked_add(1)?;
        let after_name = stat.get(fields_start..)?;
        Some(Self {
            fields: after_name.split_whitespace().collect(),
        })
    }

    fn state(&self) -> Option<ThreadState> {
        let field = self.fields.get(STAT_STATE)?;
        let code = field.chars().next()?;
        Some(ThreadState::from(code))
    }

    fn number(&self, index: usize) -> Option<u64> {
        let field = self.fields.get(index)?;
        field.parse().ok()
    }
}

/// A procfs file of `label: value` lines, such as `status` or `io`, when it could be read.
struct LabelledValues {
    text: Option<String>,
}

impl LabelledValues {
    fn read(path: &str) -> Self {
        Self {
            text: std::fs::read_to_string(path).ok(),
        }
    }

    fn number(&self, label: &str) -> Option<u64> {
        let text = self.text.as_deref()?;
        let line = text.lines().find(|line| line.starts_with(label))?;
        let value = line.get(label.len()..)?.split_whitespace().next()?;
        value.parse().ok()
    }

    /// A size reported in kilobytes, in bytes.
    fn kilobytes(&self, label: &str) -> Option<u64> {
        self.number(label)?.checked_mul(BYTES_PER_KILOBYTE)
    }
}

impl ProcessCounters {
    fn read(pid: u32, stat: &str) -> Option<Self> {
        let fields = StatFields::of(stat)?;
        let ticks = fields
            .number(STAT_USER_TICKS)?
            .checked_add(fields.number(STAT_SYSTEM_TICKS)?)?;
        let cpu_milliseconds = ticks.checked_mul(MILLISECONDS_PER_TICK)?;
        let status = LabelledValues::read(&format!("/proc/{pid}/status"));
        let storage = LabelledValues::read(&format!("/proc/{pid}/io"));
        Some(Self {
            state: fields.state()?,
            threads: fields.number(STAT_THREADS)?,
            cpu: Duration::from_millis(cpu_milliseconds),
            minor_faults: fields.number(STAT_MINOR_FAULTS)?,
            major_faults: fields.number(STAT_MAJOR_FAULTS)?,
            resident_bytes: status.kilobytes("VmRSS:"),
            resident_file_bytes: status.kilobytes("RssFile:"),
            storage_read_bytes: storage.number("read_bytes:"),
        })
    }
}

impl fmt::Display for ProcessCounters {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}, {} threads, {:.2?} of CPU, {} major and {} minor page faults",
            self.state, self.threads, self.cpu, self.major_faults, self.minor_faults
        )?;
        if let Some(resident) = self.resident_bytes {
            write!(formatter, ", {resident} bytes resident")?;
        }
        if let Some(resident_file) = self.resident_file_bytes {
            write!(formatter, " ({resident_file} of them file-backed)")?;
        }
        if let Some(read) = self.storage_read_bytes {
            write!(formatter, ", {read} bytes read from storage")?;
        }
        Ok(())
    }
}

/// A host's pressure stall averages for one resource, as procfs reports them.
#[derive(Debug)]
struct ResourcePressure {
    /// The `some` averages: the share of time at least one task stalled on the resource.
    some: String,
    /// The `full` averages: the share of time every non-idle task stalled on it. Older kernels
    /// report none for the CPU.
    full: Option<String>,
}

impl ResourcePressure {
    fn read(resource: &str) -> Option<Self> {
        let text = std::fs::read_to_string(format!("/proc/pressure/{resource}")).ok()?;
        let mut some = None;
        let mut full = None;
        for line in text.lines() {
            let mut fields = line.split_whitespace();
            let kind = fields.next();
            let averages: Vec<&str> = fields
                .filter(|field| field.starts_with("avg10=") || field.starts_with("avg60="))
                .collect();
            match kind {
                Some("some") => some = Some(averages.join(" ")),
                Some("full") => full = Some(averages.join(" ")),
                _ => {}
            }
        }
        Some(Self { some: some?, full })
    }
}

impl fmt::Display for ResourcePressure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "some {}", self.some)?;
        if let Some(full) = &self.full {
            write!(formatter, ", full {full}")?;
        }
        Ok(())
    }
}

/// The host's load averages and pressure stall information.
#[derive(Debug)]
struct HostPressure {
    load: Option<String>,
    cpu: Option<ResourcePressure>,
    io: Option<ResourcePressure>,
    memory: Option<ResourcePressure>,
}

impl HostPressure {
    fn read() -> Self {
        let load = match std::fs::read_to_string("/proc/loadavg") {
            Ok(text) => {
                let averages: Vec<&str> = text.split_whitespace().take(3).collect();
                Some(averages.join(" "))
            }
            Err(_) => None,
        };
        Self {
            load,
            cpu: ResourcePressure::read("cpu"),
            io: ResourcePressure::read("io"),
            memory: ResourcePressure::read("memory"),
        }
    }
}

impl fmt::Display for HostPressure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.load {
            Some(load) => write!(formatter, "host: load {load}")?,
            None => formatter.write_str("host: load unknown")?,
        }
        let resources = [
            ("cpu", &self.cpu),
            ("io", &self.io),
            ("memory", &self.memory),
        ];
        for (resource, pressure) in resources {
            match pressure {
                Some(pressure) => write!(formatter, "; {resource} pressure {pressure}")?,
                None => write!(formatter, "; {resource} pressure unknown")?,
            }
        }
        Ok(())
    }
}

impl ProcessObservation {
    /// Observes process `pid` and its host, off the runner task.
    pub(crate) async fn of(pid: u32) -> Self {
        off_the_runner(move || Self::read(pid)).await
    }

    fn read(pid: u32) -> Self {
        let process = match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Ok(stat) => match ProcessCounters::read(pid, &stat) {
                Some(counters) => ProcessReading::Read {
                    counters,
                    threads: ThreadWaits::read(pid),
                },
                None => ProcessReading::Unreadable(io::ErrorKind::InvalidData),
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => ProcessReading::Ended,
            Err(error) => ProcessReading::Unreadable(error.kind()),
        };
        Self {
            pid,
            process,
            host: HostPressure::read(),
        }
    }
}

impl fmt::Display for ProcessObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.process {
            ProcessReading::Read { counters, threads } => {
                write!(formatter, "process {}: {counters}\n{threads}", self.pid)?;
            }
            ProcessReading::Ended => {
                write!(
                    formatter,
                    "process {} had ended before it was observed",
                    self.pid
                )?;
            }
            ProcessReading::Unreadable(kind) => {
                write!(formatter, "process {} could not be read: {kind}", self.pid)?;
            }
        }
        write!(formatter, "\n{}", self.host)
    }
}
