//! Bounded scheduling and timer checks for the interconnect simulation harness.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** The initial simulation smoke and failure-propagation checks, and the checks of the
//!   simulated clock, entropy and semantic trace the runner gives each host.
//! - **Depends on.** The Turmoil runner, Tokio test timers, and the Rustls clock contract.
//! - **Must not know.** Product graph or persistent cluster state.

#[path = "simulation/runner.rs"]
mod runner;
#[path = "simulation/transport.rs"]
mod transport;

use std::{
    num::NonZeroUsize,
    time::{Duration, SystemTime},
};

use meticulous::OptionExt as _;
use nervix_execution::{CpuClass, Executor, MemoryClass};
use nervix_interconnect::{PeerTarget, TransportEntropy};
use nervix_models::NodeEndpoint;
use runner::{
    ClockSkew, HostSupervisor, SemanticTrace, SimulatedEntropy, SimulatedUtc, SimulationBounds,
    SimulationConfig, SimulationError, Topology,
};
use rustls::time_provider::TimeProvider as _;

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
fn peer_resolution_uses_simulated_dns() {
    let result = config().run("peer resolution", |simulation| {
        simulation.host("server", || async { Ok(()) });
        simulation.client("observer", async {
            let targets = PeerTarget::resolve(&NodeEndpoint::new("server", 7443)).await?;
            assert_eq!(targets.len(), 1);
            assert_eq!(targets[0].addr.ip(), turmoil::lookup("server"));
            assert_eq!(targets[0].server_name, "server");
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

#[test]
fn simulated_utc_is_the_epoch_plus_simulated_elapsed_time() {
    let configuration = config();
    let epoch = configuration
        .epoch
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("the configured epoch follows the Unix epoch");
    let result = configuration.run("simulated UTC", move |simulation| {
        simulation.client("observer", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let elapsed = turmoil::elapsed();
            let exact = SimulatedUtc::new(ClockSkew::Exact)
                .current_time()
                .expect("a simulated host has a UTC reading");
            assert_eq!(exact.as_secs(), (epoch + elapsed).as_secs());
            let ahead = SimulatedUtc::new(ClockSkew::Ahead(Duration::from_secs(90)))
                .current_time()
                .expect("a clock ahead of the simulation has a reading");
            assert_eq!(ahead.as_secs(), exact.as_secs() + 90);
            let behind = SimulatedUtc::new(ClockSkew::Behind(Duration::from_secs(90)))
                .current_time()
                .expect("a clock behind the simulation has a reading");
            assert_eq!(behind.as_secs() + 90, exact.as_secs());
            Ok(())
        });
    });
    assert!(result.is_ok(), "{result:?}");
    assert!(
        SimulatedUtc::new(ClockSkew::Exact).current_time().is_none(),
        "outside a simulated host the clock must not fall back to the host's wall clock"
    );
}

#[test]
fn simulated_entropy_replays_each_seed_and_separates_streams() {
    let sequence = |seed, stream| {
        let entropy = SimulatedEntropy::new(seed, stream);
        [(); 4].map(|()| entropy.next_u64())
    };
    assert_eq!(sequence(5, "node-a"), sequence(5, "node-a"));
    assert_ne!(sequence(5, "node-a"), sequence(5, "node-b"));
    assert_ne!(sequence(5, "node-a"), sequence(6, "node-a"));

    let entropy = SimulatedEntropy::new(5, "node-a");
    let source = TransportEntropy::from_source(move || entropy.next_u64());
    assert_eq!(format!("{source:?}"), "TransportEntropy");
}

#[test]
fn semantic_trace_records_hosts_in_simulated_order() {
    let trace = SemanticTrace::default();
    let recorder = trace.clone();
    let result = config().run("trace order", move |simulation| {
        simulation.client("observer", async move {
            recorder.record("observer", "first");
            tokio::time::sleep(Duration::from_millis(20)).await;
            recorder.record("observer", "second");
            Ok(())
        });
    });
    assert!(result.is_ok(), "{result:?}");
    let events = trace.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event, "first");
    assert_eq!(events[1].at, events[0].at + Duration::from_millis(20));
    let rendered = trace.render();
    assert!(rendered.contains("observer second"), "{rendered}");
}
