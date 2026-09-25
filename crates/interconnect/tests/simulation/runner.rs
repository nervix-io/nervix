//! The synchronous, bounded Turmoil scheduler used by interconnect integration tests.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** Simulation configuration, host supervision, real-time escape bounds, the simulated
//!   UTC clock and entropy each simulated host is given, and the semantic event trace a replay is
//!   compared by.
//! - **Depends on.** Turmoil and Tokio test runtimes, and the Rustls clock contract.
//! - **Must not know.** Product graph state, connector drivers, or persisted cluster state.

use std::{
    fmt::Write as _,
    future::Future,
    num::NonZeroUsize,
    sync::{
        Arc as StdArc,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use meticulous::ResultExt as _;
use nervix_recovery::Discarded as _;
use parking_lot::Mutex;
use rustls::{pki_types::UnixTime, time_provider::TimeProvider};
use thiserror::Error;

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone, Copy)]
pub(super) struct SimulationBounds {
    pub simulated_duration: Duration,
    pub tick: Duration,
    pub max_steps: NonZeroUsize,
    pub wall_duration: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SimulationConfig {
    pub seed: u64,
    pub epoch: SystemTime,
    pub topology: Topology,
    pub bounds: SimulationBounds,
}

#[derive(Debug, Error)]
pub(super) enum SimulationError {
    #[error("simulation {scenario} seed {seed}: {field} must be nonzero")]
    InvalidDuration {
        scenario: &'static str,
        seed: u64,
        field: &'static str,
    },
    #[error("simulation {scenario} seed {seed}: epoch precedes the Unix epoch")]
    InvalidEpoch { scenario: &'static str, seed: u64 },
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
    #[error("simulation {scenario} seed {seed}: exceeded {wall_duration:?} of real wall time")]
    WallTimeExhausted {
        scenario: &'static str,
        seed: u64,
        wall_duration: Duration,
    },
    #[error("simulation {scenario} seed {seed}: scheduler thread panicked")]
    SchedulerPanicked { scenario: &'static str, seed: u64 },
}

impl SimulationConfig {
    pub fn run<F>(self, scenario: &'static str, setup: F) -> Result<(), SimulationError>
    where
        F: for<'a> FnOnce(&mut turmoil::Sim<'a>) + Send + 'static,
    {
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

        let (sender, receiver) = mpsc::sync_channel(1);
        let scheduler = std::thread::spawn(move || {
            let result = self.run_on_scheduler(scenario, setup);
            sender
                .send(result)
                .discarded("the wall-time escape already returned to the test caller");
        });

        match receiver.recv_timeout(self.bounds.wall_duration) {
            Ok(result) => {
                if scheduler.join().is_err() {
                    return Err(SimulationError::SchedulerPanicked {
                        scenario,
                        seed: self.seed,
                    });
                }
                result
            }
            Err(RecvTimeoutError::Timeout) => {
                drop(scheduler);
                Err(SimulationError::WallTimeExhausted {
                    scenario,
                    seed: self.seed,
                    wall_duration: self.bounds.wall_duration,
                })
            }
            Err(RecvTimeoutError::Disconnected) => {
                // The sender is dropped only when the scheduler thread ends before publishing.
                drop(scheduler);
                Err(SimulationError::SchedulerPanicked {
                    scenario,
                    seed: self.seed,
                })
            }
        }
    }

    fn run_on_scheduler<F>(self, scenario: &'static str, setup: F) -> Result<(), SimulationError>
    where
        F: for<'a> FnOnce(&mut turmoil::Sim<'a>),
    {
        let mut builder = turmoil::Builder::new();
        builder
            .rng_seed(self.seed)
            .epoch(self.epoch)
            .ip_version(self.topology.ip_version())
            .simulation_duration(self.bounds.simulated_duration)
            .tick_duration(self.bounds.tick)
            .enable_random_order();
        let mut simulation = builder.build();
        setup(&mut simulation);

        for _ in 0..self.bounds.max_steps.get() {
            match simulation.step() {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(error) => {
                    return Err(SimulationError::Failed {
                        scenario,
                        seed: self.seed,
                        detail: error.to_string(),
                    });
                }
            }
        }
        Err(SimulationError::StepsExhausted {
            scenario,
            seed: self.seed,
            max_steps: self.bounds.max_steps,
            elapsed: simulation.elapsed(),
        })
    }
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
/// certificate bytes or ciphertext, which differ between runs without changing any decision.
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
