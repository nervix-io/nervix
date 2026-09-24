//! Bounded scheduling and timer checks for the interconnect simulation harness.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** The initial simulation smoke and failure-propagation checks.
//! - **Depends on.** The Turmoil runner and Tokio test timers.
//! - **Must not know.** Product graph or persistent cluster state.

#[path = "simulation/runner.rs"]
mod runner;

use std::{
    num::NonZeroUsize,
    time::{Duration, SystemTime},
};

use meticulous::OptionExt as _;
use nervix_execution::{CpuClass, Executor, MemoryClass};
use runner::{HostSupervisor, SimulationBounds, SimulationConfig, SimulationError, Topology};

fn config() -> SimulationConfig {
    SimulationConfig {
        seed: 41,
        epoch: SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
        topology: Topology::Ipv4,
        bounds: SimulationBounds {
            simulated_duration: Duration::from_secs(1),
            tick: Duration::from_millis(1),
            max_steps: NonZeroUsize::new(200).assured("200 is nonzero"),
            wall_duration: Duration::from_secs(3),
        },
    }
}

#[test]
fn simulation_timer_smoke() {
    let result = config().run("timer smoke", |simulation| {
        simulation.host("worker", || async {
            HostSupervisor::run(async {
                tokio::time::sleep(Duration::from_millis(3)).await;
                Ok::<(), std::io::Error>(())
            })
            .await
        });
        simulation.client("observer", async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(())
        });
    });
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn bounded_cpu_job_runs_on_the_simulated_scheduler() {
    for seed in 1..=12 {
        let mut configuration = config();
        configuration.seed = seed;
        let result = configuration.run("bounded CPU worker", |simulation| {
            simulation.host("worker", || async {
                HostSupervisor::run(async {
                    let scheduler_thread = std::thread::current().id();
                    let executor = Executor::default();
                    for (cpu, memory) in [
                        (CpuClass::Control, MemoryClass::Management),
                        (CpuClass::Data, MemoryClass::Commands),
                        (CpuClass::Bulk, MemoryClass::Relay),
                        (CpuClass::Bulk, MemoryClass::Bulk),
                    ] {
                        tokio::task::consume_budget().await;
                        let reservation = executor
                            .try_reserve(memory, 1024)
                            .expect("the memory class starts with room");
                        let job_thread = executor
                            .run_cpu(cpu, reservation, |_, _| std::thread::current().id())
                            .await
                            .expect("the bounded job completes");
                        assert_eq!(job_thread, scheduler_thread);
                    }
                    let snapshot = executor.snapshot();
                    for workers in [snapshot.control_cpu, snapshot.data_cpu, snapshot.bulk_cpu] {
                        assert_eq!(workers.running, 0);
                        assert_eq!(workers.pending, 0);
                    }
                    for budget in [
                        snapshot.management_memory,
                        snapshot.commands_memory,
                        snapshot.relay_memory,
                        snapshot.bulk_memory,
                    ] {
                        assert_eq!(budget.reserved_bytes, 0);
                    }
                    Ok::<(), std::io::Error>(())
                })
                .await
            });
        });
        assert!(result.is_ok(), "seed {seed}: {result:?}");
    }
}

#[test]
fn simulation_ipv6_topology() {
    let mut configuration = config();
    configuration.topology = Topology::Ipv6;
    let result = configuration.run("IPv6 topology", |simulation| {
        simulation.client("observer", async {
            assert!(turmoil::lookup("observer").is_ipv6());
            Ok(())
        });
    });
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn simulation_supervised_host_failure_reaches_result() {
    let result = config().run("host failure", |simulation| {
        simulation.host("worker", || async {
            HostSupervisor::run(async { Err(std::io::Error::other("supervised worker failed")) })
                .await
        });
        simulation.client("observer", async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        });
    });
    let Err(error) = result else {
        panic!("a supervised host failure must fail the simulation");
    };
    assert!(matches!(error, SimulationError::Failed { .. }), "{error:?}");
    assert!(error.to_string().contains("host failure seed 41"));
    assert!(error.to_string().contains("supervised worker failed"));
}

#[test]
fn simulation_step_limit_names_scenario_and_seed() {
    let mut configuration = config();
    configuration.bounds.max_steps = NonZeroUsize::new(2).assured("2 is nonzero");
    let result = configuration.run("stalled observer", |simulation| {
        simulation.client("observer", async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        });
    });
    let Err(error) = result else {
        panic!("an unfinished client must exhaust the step bound");
    };
    assert!(
        matches!(error, SimulationError::StepsExhausted { .. }),
        "{error:?}"
    );
    assert!(error.to_string().contains("stalled observer seed 41"));
}

#[test]
fn simulation_wall_bound_is_outside_simulated_time() {
    let mut configuration = config();
    configuration.bounds.wall_duration = Duration::from_millis(1);
    let result = configuration.run("stalled scheduler", |simulation| {
        std::thread::sleep(Duration::from_millis(100));
        simulation.client("observer", async { Ok(()) });
    });
    let Err(error) = result else {
        panic!("a blocked scheduler must exhaust the real wall bound");
    };
    assert!(
        matches!(error, SimulationError::WallTimeExhausted { .. }),
        "{error:?}"
    );
    assert!(error.to_string().contains("stalled scheduler seed 41"));
}

#[test]
fn simulation_rejects_invalid_bounds_and_epoch() {
    let mut configuration = config();
    configuration.bounds.tick = Duration::ZERO;
    let result = configuration.run("invalid tick", |_simulation| {});
    assert!(matches!(
        result,
        Err(SimulationError::InvalidDuration { field: "tick", .. })
    ));

    let mut configuration = config();
    configuration.epoch = SystemTime::UNIX_EPOCH - Duration::from_secs(1);
    let result = configuration.run("invalid epoch", |_simulation| {});
    assert!(matches!(result, Err(SimulationError::InvalidEpoch { .. })));
}

#[test]
fn simulation_scheduler_panic_reaches_result() {
    let result = config().run("scheduler panic", |_simulation| {
        panic!("scheduler setup failed");
    });
    let Err(error) = result else {
        panic!("a scheduler panic must fail the simulation");
    };
    assert!(
        matches!(error, SimulationError::SchedulerPanicked { .. }),
        "{error:?}"
    );
    assert!(error.to_string().contains("scheduler panic seed 41"));
}
