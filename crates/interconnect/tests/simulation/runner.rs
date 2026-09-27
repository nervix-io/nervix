//! The synchronous, bounded Turmoil scheduler used by interconnect integration tests.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** Simulation configuration, host supervision, the real-time escape bound around a run
//!   and its cleanup, the first panic of each thread, the simulated UTC clock and entropy each
//!   simulated host is given, and the semantic event trace a replay is compared by.
//! - **Depends on.** Turmoil and Tokio test runtimes, and the Rustls clock contract.
//! - **Must not know.** Product graph state, connector drivers, or persisted cluster state.

use std::{
    any::Any,
    cell::RefCell,
    fmt::{self, Write as _},
    future::Future,
    io,
    num::NonZeroUsize,
    panic::{self, AssertUnwindSafe, PanicHookInfo},
    sync::{
        Arc as StdArc, Once,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_recovery::Discarded as _;
use parking_lot::Mutex;
use rustls::{pki_types::UnixTime, time_provider::TimeProvider};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Topology {
    Ipv4,
    Ipv6,
}

impl Topology {
    fn ip_version(self) -> turmoil::IpVersion {
        match self {
            Self::Ipv4 => turmoil::IpVersion::V4,
            Self::Ipv6 => turmoil::IpVersion::V6,
        }
    }
}

/// How every simulated link behaves before a fault plan partitions, holds or repairs it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub(super) struct NetworkParameters {
    /// The shortest delay of one simulated message.
    pub min_message_latency: Duration,
    /// The longest delay of one simulated message. Turmoil draws each delay between the two from
    /// its exponential latency curve, using the simulation seed.
    pub max_message_latency: Duration,
    /// The chance, on each step, that a working link starts dropping messages.
    pub fail_rate: f64,
    /// The chance, on each step, that a link dropping messages recovers.
    pub repair_rate: f64,
    /// Segments one simulated TCP connection buffers before its sender waits.
    pub tcp_capacity: usize,
}

impl NetworkParameters {
    /// Lossless links with Turmoil's reference latency range and buffer. Scenarios disrupt links
    /// only through their explicit fault plan.
    pub const LOSSLESS: Self = Self {
        min_message_latency: Duration::ZERO,
        max_message_latency: Duration::from_millis(100),
        fail_rate: 0.0,
        repair_rate: 1.0,
        tcp_capacity: 64,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SimulationBounds {
    pub simulated_duration: Duration,
    pub tick: Duration,
    pub max_steps: NonZeroUsize,
    /// Real time for the run and its cleanup together. It is measured outside the scheduler thread,
    /// so it expires even when a host blocks that thread and simulated time cannot advance.
    pub wall_duration: Duration,
}

/// Every input a run is built from. One configuration and one scenario replay the same run.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub(super) struct SimulationConfig {
    pub seed: u64,
    pub epoch: SystemTime,
    pub topology: Topology,
    pub network: NetworkParameters,
    pub bounds: SimulationBounds,
}

/// Where the scheduler thread was when a bound expired or a panic ended it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(super) enum SchedulerPhase {
    /// Building the simulation, running its setup, or stepping it.
    #[strum(serialize = "running")]
    Running,
    /// Dropping a completed simulation and every host runtime in it.
    #[strum(serialize = "cleaning up")]
    CleaningUp,
}

/// How far the scheduler had got: its completed steps and the simulated time they reached.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct SchedulerProgress {
    pub steps: usize,
    pub elapsed: Duration,
}

impl fmt::Display for SchedulerProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} completed steps at {:?} simulated time",
            self.steps, self.elapsed
        )
    }
}

thread_local! {
    /// The first panic this thread raised since the report was last taken.
    static FIRST_PANIC: RefCell<Option<PanicReport>> = const { RefCell::new(None) };
}

/// A panic's message and where it was raised.
///
/// A panic in a task on a simulated host shuts that host's runtime down, and the scheduler then
/// panics with Tokio's generic shutdown message. The report keeps the first panic instead, which
/// names the failed assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PanicReport {
    pub message: String,
    pub location: Option<String>,
}

impl PanicReport {
    /// Record the first panic of every thread before printing it as before. Installed once for
    /// the process; later calls do nothing.
    pub fn capture() {
        static INSTALLED: Once = Once::new();
        INSTALLED.call_once(|| {
            let previous = panic::take_hook();
            panic::set_hook(Box::new(move |info| {
                let report = Self::from_hook(info);
                FIRST_PANIC.with(|slot| {
                    // A panic raised while this thread holds its own slot keeps the earlier report.
                    if let Ok(mut slot) = slot.try_borrow_mut()
                        && slot.is_none()
                    {
                        *slot = Some(report);
                    }
                });
                previous(info);
            }));
        });
    }

    /// Take the first panic this thread raised since the last take.
    pub fn take() -> Option<Self> {
        FIRST_PANIC.with(|slot| slot.borrow_mut().take())
    }

    /// The report of a caught panic: the first one this thread raised, or the caught payload.
    pub fn caught(payload: &(dyn Any + Send)) -> Self {
        match Self::take() {
            Some(report) => report,
            None => Self {
                message: Self::message_of(payload),
                location: None,
            },
        }
    }

    fn from_hook(info: &PanicHookInfo<'_>) -> Self {
        Self {
            message: Self::message_of(info.payload()),
            location: info.location().map(ToString::to_string),
        }
    }

    fn message_of(payload: &(dyn Any + Send)) -> String {
        if let Some(message) = payload.downcast_ref::<&str>() {
            return (*message).to_string();
        }
        if let Some(message) = payload.downcast_ref::<String>() {
            return message.clone();
        }
        "a panic whose payload is not a message".to_string()
    }
}

impl fmt::Display for PanicReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.location {
            Some(location) => write!(formatter, "{} at {location}", self.message),
            None => write!(formatter, "{}", self.message),
        }
    }
}

#[derive(Debug, Error)]
pub(super) enum SimulationError {
    #[error("simulation {scenario} seed {seed}: {field} must be nonzero")]
    InvalidDuration {
        scenario: &'static str,
        seed: u64,
        field: &'static str,
    },
    #[error(
        "simulation {scenario} seed {seed}: wall_duration {wall_duration:?} exceeds the real clock"
    )]
    UnrepresentableWallDuration {
        scenario: &'static str,
        seed: u64,
        wall_duration: Duration,
    },
    #[error("simulation {scenario} seed {seed}: epoch precedes the Unix epoch")]
    InvalidEpoch { scenario: &'static str, seed: u64 },
    #[error(
        "simulation {scenario} seed {seed}: the test build lacks `--cfg tokio_unstable`, so Tokio \
         scheduling would not follow the seed and a panicking host task would not fail the run; \
         run it through a `just test-turmoil` recipe"
    )]
    TokioUnstableMissing { scenario: &'static str, seed: u64 },
    #[error("simulation {scenario} seed {seed}: cannot start the scheduler thread: {source}")]
    SchedulerUnavailable {
        scenario: &'static str,
        seed: u64,
        #[source]
        source: io::Error,
    },
    #[error("simulation {scenario} seed {seed}: host or client failed: {detail}")]
    Failed {
        scenario: &'static str,
        seed: u64,
        detail: String,
    },
    #[error(
        "simulation {scenario} seed {seed}: exhausted {max_steps} steps after {elapsed:?} \
         simulated time"
    )]
    StepsExhausted {
        scenario: &'static str,
        seed: u64,
        max_steps: NonZeroUsize,
        elapsed: Duration,
    },
    #[error(
        "simulation {scenario} seed {seed}: exceeded {wall_duration:?} of real wall time while \
         {phase} after {progress}; the scheduler thread was abandoned"
    )]
    WallTimeExhausted {
        scenario: &'static str,
        seed: u64,
        wall_duration: Duration,
        phase: SchedulerPhase,
        progress: SchedulerProgress,
    },
    #[error("simulation {scenario} seed {seed}: scheduler thread panicked while {phase}: {panic}")]
    SchedulerPanicked {
        scenario: &'static str,
        seed: u64,
        phase: SchedulerPhase,
        panic: PanicReport,
    },
}

/// The scheduler thread's two publications: the run's result, then how its cleanup ended.
struct SchedulerChannels {
    finished: SyncSender<Result<(), SimulationError>>,
    cleaned: SyncSender<Result<(), PanicReport>>,
}

/// The progress the scheduler thread publishes after every step, for a caller whose wall bound
/// expired while that thread was blocked.
#[derive(Debug, Default)]
struct ProgressCell {
    progress: Mutex<SchedulerProgress>,
}

impl ProgressCell {
    fn advance(&self, elapsed: Duration) {
        let mut progress = self.progress.lock();
        progress.steps = progress
            .steps
            .checked_add(1)
            .assured("the step loop stops at a usize step bound");
        progress.elapsed = elapsed;
    }

    fn snapshot(&self) -> SchedulerProgress {
        *self.progress.lock()
    }
}

impl SimulationConfig {
    pub fn run<F>(self, scenario: &'static str, setup: F) -> Result<(), SimulationError>
    where
        F: for<'a> FnOnce(&mut turmoil::Sim<'a>) + Send + 'static,
    {
        self.run_with_control(scenario, setup, |_| {})
    }

    /// Observe each completed scheduler step to inject host lifecycle events at a fixture milestone.
    ///
    /// The run and its cleanup share one real-time deadline, fixed before the scheduler starts. A
    /// failed run is returned even when its cleanup then fails too; that cleanup failure is written
    /// to standard error so it is not lost.
    pub fn run_with_control<F, C>(
        self,
        scenario: &'static str,
        setup: F,
        control: C,
    ) -> Result<(), SimulationError>
    where
        F: for<'a> FnOnce(&mut turmoil::Sim<'a>) + Send + 'static,
        C: for<'a> FnMut(&mut turmoil::Sim<'a>) + Send + 'static,
    {
        let deadline = self.validate(scenario)?;
        PanicReport::capture();

        let (finished, finished_receiver) = mpsc::sync_channel(1);
        let (cleaned, cleaned_receiver) = mpsc::sync_channel(1);
        let channels = SchedulerChannels { finished, cleaned };
        let progress = StdArc::new(ProgressCell::default());
        let scheduler_progress = StdArc::clone(&progress);
        let scheduler = std::thread::Builder::new()
            .name(format!("turmoil scheduler: {scenario} seed {}", self.seed))
            .spawn(move || self.schedule(scenario, setup, control, &scheduler_progress, channels))
            .map_err(|source| SimulationError::SchedulerUnavailable {
                scenario,
                seed: self.seed,
                source,
            })?;

        let run = self.await_phase(
            scenario,
            SchedulerPhase::Running,
            &finished_receiver,
            deadline,
            &progress,
        );
        let run = match run {
            PhaseEnd::Published(result) => result,
            PhaseEnd::Failed(error) => return Err(error),
        };
        let cleanup = self.await_phase(
            scenario,
            SchedulerPhase::CleaningUp,
            &cleaned_receiver,
            deadline,
            &progress,
        );
        let cleanup = match cleanup {
            PhaseEnd::Published(Ok(())) => Ok(()),
            PhaseEnd::Published(Err(panic)) => Err(SimulationError::SchedulerPanicked {
                scenario,
                seed: self.seed,
                phase: SchedulerPhase::CleaningUp,
                panic,
            }),
            PhaseEnd::Failed(error) => Err(error),
        };
        if cleanup.is_ok() {
            // The thread returns right after publishing its cleanup.
            scheduler
                .join()
                .assured("the scheduler thread runs its work inside `catch_unwind`");
        }
        match (run, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Ok(()), Err(cleanup)) => Err(cleanup),
            (Err(run), Ok(())) => Err(run),
            (Err(run), Err(cleanup)) => {
                eprintln!("{cleanup}, after the run had already failed");
                Err(run)
            }
        }
    }

    fn validate(&self, scenario: &'static str) -> Result<Instant, SimulationError> {
        for (field, duration) in [
            ("simulated_duration", self.bounds.simulated_duration),
            ("tick", self.bounds.tick),
            ("wall_duration", self.bounds.wall_duration),
        ] {
            if duration.is_zero() {
                return Err(SimulationError::InvalidDuration {
                    scenario,
                    seed: self.seed,
                    field,
                });
            }
        }
        if self.epoch.duration_since(UNIX_EPOCH).is_err() {
            return Err(SimulationError::InvalidEpoch {
                scenario,
                seed: self.seed,
            });
        }
        if !cfg!(tokio_unstable) {
            return Err(SimulationError::TokioUnstableMissing {
                scenario,
                seed: self.seed,
            });
        }
        let Some(deadline) = Instant::now().checked_add(self.bounds.wall_duration) else {
            return Err(SimulationError::UnrepresentableWallDuration {
                scenario,
                seed: self.seed,
                wall_duration: self.bounds.wall_duration,
            });
        };
        Ok(deadline)
    }

    /// Wait until `deadline` for the scheduler thread to publish the end of `phase`.
    fn await_phase<T>(
        &self,
        scenario: &'static str,
        phase: SchedulerPhase,
        receiver: &Receiver<T>,
        deadline: Instant,
        progress: &ProgressCell,
    ) -> PhaseEnd<T> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(remaining) {
            Ok(published) => PhaseEnd::Published(published),
            Err(RecvTimeoutError::Timeout) => {
                PhaseEnd::Failed(SimulationError::WallTimeExhausted {
                    scenario,
                    seed: self.seed,
                    wall_duration: self.bounds.wall_duration,
                    phase,
                    progress: progress.snapshot(),
                })
            }
            // Every phase is published from inside `catch_unwind`, so the sender outlives it.
            Err(RecvTimeoutError::Disconnected) => {
                unreachable!("the scheduler thread publishes {phase} before it returns")
            }
        }
    }

    /// The scheduler thread: run, publish the result, then drop the simulation and publish that.
    fn schedule<F, C>(
        self,
        scenario: &'static str,
        setup: F,
        control: C,
        progress: &ProgressCell,
        channels: SchedulerChannels,
    ) where
        F: for<'a> FnOnce(&mut turmoil::Sim<'a>),
        C: for<'a> FnMut(&mut turmoil::Sim<'a>),
    {
        let run = panic::catch_unwind(AssertUnwindSafe(|| {
            self.run_on_scheduler(scenario, setup, control, progress)
        }));
        let simulation = match run {
            Ok(Stepped { result, simulation }) => {
                // A panic that something on the scheduler thread caught, such as a host task's
                // destructor during a crash, still fails the run, and names the likelier cause.
                let result = match PanicReport::take() {
                    Some(panic) => Err(SimulationError::SchedulerPanicked {
                        scenario,
                        seed: self.seed,
                        phase: SchedulerPhase::Running,
                        panic,
                    }),
                    None => result,
                };
                channels
                    .finished
                    .send(result)
                    .discarded("the wall-time escape already returned to the test caller");
                simulation
            }
            Err(payload) => {
                let failure = SimulationError::SchedulerPanicked {
                    scenario,
                    seed: self.seed,
                    phase: SchedulerPhase::Running,
                    panic: PanicReport::caught(&*payload),
                };
                channels
                    .finished
                    .send(Err(failure))
                    .discarded("the wall-time escape already returned to the test caller");
                // Unwinding already dropped the simulation.
                channels
                    .cleaned
                    .send(Ok(()))
                    .discarded("the wall-time escape already returned to the test caller");
                return;
            }
        };

        // Tokio catches a panic raised while it drops a host's tasks, so the first panic this
        // thread raised during the drop is checked as well as the drop's own result.
        let cleanup = panic::catch_unwind(AssertUnwindSafe(move || drop(simulation)));
        let cleanup = match cleanup {
            Ok(()) => match PanicReport::take() {
                Some(panic) => Err(panic),
                None => Ok(()),
            },
            Err(payload) => Err(PanicReport::caught(&*payload)),
        };
        channels
            .cleaned
            .send(cleanup)
            .discarded("the wall-time escape already returned to the test caller");
    }

    fn run_on_scheduler<F, C>(
        self,
        scenario: &'static str,
        setup: F,
        mut control: C,
        progress: &ProgressCell,
    ) -> Stepped
    where
        F: for<'a> FnOnce(&mut turmoil::Sim<'a>),
        C: for<'a> FnMut(&mut turmoil::Sim<'a>),
    {
        let mut builder = turmoil::Builder::new();
        builder
            .rng_seed(self.seed)
            .epoch(self.epoch)
            .ip_version(self.topology.ip_version())
            .min_message_latency(self.network.min_message_latency)
            .max_message_latency(self.network.max_message_latency)
            .fail_rate(self.network.fail_rate)
            .repair_rate(self.network.repair_rate)
            .tcp_capacity(self.network.tcp_capacity)
            .simulation_duration(self.bounds.simulated_duration)
            .tick_duration(self.bounds.tick)
            .enable_random_order();
        let mut simulation = builder.build();
        setup(&mut simulation);

        for _ in 0..self.bounds.max_steps.get() {
            let step = simulation.step();
            progress.advance(simulation.elapsed());
            match step {
                Ok(true) => {
                    return Stepped {
                        result: Ok(()),
                        simulation,
                    };
                }
                Ok(false) => control(&mut simulation),
                Err(error) => {
                    return Stepped {
                        result: Err(SimulationError::Failed {
                            scenario,
                            seed: self.seed,
                            detail: error.to_string(),
                        }),
                        simulation,
                    };
                }
            }
        }
        Stepped {
            result: Err(SimulationError::StepsExhausted {
                scenario,
                seed: self.seed,
                max_steps: self.bounds.max_steps,
                elapsed: simulation.elapsed(),
            }),
            simulation,
        }
    }
}

/// A stepped simulation, kept alive so its cleanup is supervised as a phase of its own.
struct Stepped {
    result: Result<(), SimulationError>,
    simulation: turmoil::Sim<'static>,
}

/// How waiting for one scheduler phase ended.
enum PhaseEnd<T> {
    /// The scheduler thread published the phase's result in time.
    Published(T),
    /// The real-time deadline expired first.
    Failed(SimulationError),
}

pub(super) struct HostSupervisor;

impl HostSupervisor {
    pub async fn run<F, E>(future: F) -> turmoil::Result
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        let outcome = tokio::spawn(future).await?;
        outcome?;
        Ok(())
    }
}

/// How far one host's UTC clock reads from the simulation's UTC.
#[derive(Debug, Clone, Copy)]
pub(super) enum ClockSkew {
    Exact,
    Ahead(Duration),
    Behind(Duration),
}

/// The simulated UTC clock: the configured epoch plus simulated elapsed time, skewed per host.
///
/// It reads nothing outside the simulation. Outside a simulated host it has no current time, so
/// a check that reaches it there fails instead of consulting the host's wall clock.
#[derive(Debug)]
pub(super) struct SimulatedUtc {
    skew: ClockSkew,
}

impl SimulatedUtc {
    pub fn new(skew: ClockSkew) -> Self {
        Self { skew }
    }
}

impl TimeProvider for SimulatedUtc {
    fn current_time(&self) -> Option<UnixTime> {
        let since_epoch = turmoil::since_epoch()?;
        let skewed = match self.skew {
            ClockSkew::Exact => Some(since_epoch),
            ClockSkew::Ahead(offset) => since_epoch.checked_add(offset),
            ClockSkew::Behind(offset) => since_epoch.checked_sub(offset),
        }?;
        Some(UnixTime::since_unix_epoch(skewed))
    }
}

/// A seeded source of the values a production transport draws from the operating system.
///
/// Each named stream of one seed yields the same sequence in every process, so a scenario that
/// derives protocol identities from it replays them exactly.
#[derive(Debug)]
pub(super) struct SimulatedEntropy {
    state: AtomicU64,
}

impl SimulatedEntropy {
    const GOLDEN_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

    pub fn new(seed: u64, stream: &str) -> Self {
        // FNV-1a over the stream name separates hosts that share one seed.
        let mut state = seed ^ 0xcbf2_9ce4_8422_2325;
        for byte in stream.bytes() {
            state = (state ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
        }
        Self {
            state: AtomicU64::new(state),
        }
    }

    /// The next SplitMix64 output. Wrapping is the meaning of this hash mixer.
    pub fn next_u64(&self) -> u64 {
        let state = self
            .state
            .fetch_add(Self::GOLDEN_GAMMA, Ordering::Relaxed)
            .wrapping_add(Self::GOLDEN_GAMMA);
        let mixed = (state ^ (state >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }
}

/// One causally relevant event: when it happened in simulated time, where, and what was decided.
///
/// Events name identities, admission decisions and outcomes. They never carry key material,
/// certificate bytes, ciphertext or payload values, which either differ between runs without
/// changing any decision or do not belong in a failure record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TraceEvent {
    pub at: Duration,
    pub host: &'static str,
    pub event: String,
}

/// The ordered semantic events of one simulation run. Two runs of one scenario, seed and
/// configuration must record the same trace; any difference is a replay defect.
#[derive(Debug, Clone, Default)]
pub(super) struct SemanticTrace {
    events: StdArc<Mutex<Vec<TraceEvent>>>,
}

impl SemanticTrace {
    /// Record `event` on `host` at the current simulated time. Only simulated hosts record.
    pub fn record(&self, host: &'static str, event: impl Into<String>) {
        let event = TraceEvent {
            at: turmoil::elapsed(),
            host,
            event: event.into(),
        };
        self.events.lock().push(event);
    }

    pub fn events(&self) -> Vec<TraceEvent> {
        self.events.lock().clone()
    }

    /// One line per event, in the order they were recorded.
    pub fn render(&self) -> String {
        let mut rendered = String::new();
        for event in self.events.lock().iter() {
            writeln!(
                rendered,
                "{:>10.3}s {:<8} {}",
                event.at.as_secs_f64(),
                event.host,
                event.event
            )
            .assured("writing to a String cannot fail");
        }
        rendered
    }
}
