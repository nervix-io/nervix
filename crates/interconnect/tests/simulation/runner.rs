//! The synchronous, bounded Turmoil scheduler used by interconnect integration tests.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** Simulation configuration, host supervision, and real-time escape bounds.
//! - **Depends on.** Turmoil and Tokio test runtimes.
//! - **Must not know.** Product graph state, connector drivers, or persisted cluster state.

use std::{
    future::Future,
    num::NonZeroUsize,
    sync::mpsc::{self, RecvTimeoutError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use nervix_recovery::Discarded as _;
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
