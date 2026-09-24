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
