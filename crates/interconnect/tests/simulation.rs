//! Bounded scheduling and timer checks for the interconnect simulation harness.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** The initial simulation smoke and failure-propagation checks, the checks of the
//!   simulated clock, entropy and semantic trace the runner gives each host, and the check that a
//!   recorded failure replays in a fresh process.
//! - **Depends on.** The Turmoil runner and scenario driver, Tokio test timers, and the Rustls
//!   clock contract.
//! - **Must not know.** Product graph or persistent cluster state.

#[path = "simulation/runner.rs"]
mod runner;
#[path = "simulation/scenario.rs"]
mod scenario;
#[path = "simulation/transport.rs"]
mod transport;

use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{Duration, SystemTime},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{CpuClass, Executor, MemoryClass};
use nervix_interconnect::{PeerTarget, TransportEntropy};
use nervix_models::NodeEndpoint;
use runner::{
    ClockSkew, HostSupervisor, NetworkParameters, SchedulerPhase, SemanticTrace, SimulatedEntropy,
    SimulatedUtc, SimulationBounds, SimulationConfig, SimulationError, Topology,
};
use rustls::time_provider::TimeProvider as _;
use scenario::Scenario;

fn config(seed: u64) -> SimulationConfig {
    SimulationConfig {
        seed,
        epoch: SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
        topology: Topology::Ipv4,
        network: NetworkParameters::LOSSLESS,
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
    let result = config(41).run("timer smoke", |simulation| {
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
    let scenario = Scenario {
        name: "peer resolution",
        fault_plan: "none; one client resolves a peer's endpoint through simulated DNS",
        seeds: &[41],
    };
    scenario.check(config, |run| {
        let trace = run.trace();
        run.simulate(move |simulation| {
            simulation.host("server", || async { Ok(()) });
            simulation.client("observer", async move {
                let targets = PeerTarget::resolve(&NodeEndpoint::new("server", 7443)).await?;
                assert_eq!(targets.len(), 1);
                assert_eq!(targets[0].addr.ip(), turmoil::lookup("server"));
                assert_eq!(targets[0].server_name, "server");
                trace.record("observer", "server resolved to its simulated address");
                Ok(())
            });
        })
    });
}

#[test]
fn bounded_cpu_job_runs_on_the_simulated_scheduler() {
    let scenario = Scenario {
        name: "bounded CPU worker",
        fault_plan: "none; one host runs a bounded job in every CPU and memory class",
        seeds: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    };
    scenario.check(config, |run| {
        let trace = run.trace();
        run.simulate(move |simulation| {
            simulation.host("worker", move || {
                let trace = trace.clone();
                async move {
                    HostSupervisor::run(async move {
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
                            trace.record("worker", format!("{cpu:?} job ran on the scheduler"));
                        }
                        let snapshot = executor.snapshot();
                        for workers in [snapshot.control_cpu, snapshot.data_cpu, snapshot.bulk_cpu]
                        {
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
                }
            });
        })
    });
}

#[test]
fn simulation_ipv6_topology() {
    let mut configuration = config(41);
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
    let result = config(41).run("host failure", |simulation| {
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
fn simulation_host_task_panic_reports_its_own_message() {
    let result = config(41).run("host task panic", |simulation| {
        simulation.host("worker", || async {
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(2)).await;
                panic!("unsupervised worker task failed its assertion");
            });
            std::future::pending::<()>().await;
            Ok(())
        });
        simulation.client("observer", async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        });
    });
    let Err(error) = result else {
        panic!("a panicking task on a host must fail the simulation");
    };
    let SimulationError::SchedulerPanicked {
        phase: SchedulerPhase::Running,
        ref panic,
        ..
    } = error
    else {
        panic!("the shut-down host runtime ends the run: {error:?}");
    };
    assert_eq!(
        panic.message,
        "unsupervised worker task failed its assertion"
    );
    let location = panic
        .location
        .as_deref()
        .assured("a panic hook sees the location");
    assert!(location.contains("simulation.rs"), "{location}");
}

/// A host's state whose teardown check fails when its simulation is dropped.
struct FailsTeardown;

impl Drop for FailsTeardown {
    fn drop(&mut self) {
        panic!("host state failed its teardown check");
    }
}

#[test]
fn simulation_cleanup_panic_fails_a_completed_run() {
    let result = config(41).run("cleanup panic", |simulation| {
        simulation.host("server", || async {
            let _state = FailsTeardown;
            std::future::pending::<()>().await;
            Ok(())
        });
        simulation.client("observer", async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(())
        });
    });
    let Err(error) = result else {
        panic!("a panic while dropping the simulation must fail the run");
    };
    let SimulationError::SchedulerPanicked {
        phase: SchedulerPhase::CleaningUp,
        ref panic,
        ..
    } = error
    else {
        panic!("the run completed, so the failure belongs to cleanup: {error:?}");
    };
    assert_eq!(panic.message, "host state failed its teardown check");
    assert!(error.to_string().contains("cleaning up"), "{error}");
}

#[test]
fn simulation_step_limit_names_scenario_and_seed() {
    let mut configuration = config(41);
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
    let mut configuration = config(41);
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
fn simulation_wall_bound_reports_where_simulated_time_stopped() {
    let mut configuration = config(41);
    configuration.bounds.wall_duration = Duration::from_millis(200);
    let result = configuration.run("blocked host", |simulation| {
        simulation.client("observer", async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            // Blocking the scheduler thread stops simulated time inside this step.
            std::thread::sleep(Duration::from_secs(2));
            Ok(())
        });
    });
    let Err(error) = result else {
        panic!("a host blocking the scheduler must exhaust the real wall bound");
    };
    let SimulationError::WallTimeExhausted {
        phase: SchedulerPhase::Running,
        progress,
        ..
    } = error
    else {
        panic!("the host blocked the run itself: {error:?}");
    };
    assert!(progress.steps >= 3, "{progress}");
    assert!(progress.elapsed >= Duration::from_millis(3), "{progress}");
    assert!(progress.elapsed < Duration::from_millis(10), "{progress}");
}

#[test]
fn simulation_rejects_invalid_bounds_and_epoch() {
    let mut configuration = config(41);
    configuration.bounds.tick = Duration::ZERO;
    let result = configuration.run("invalid tick", |_simulation| {});
    assert!(matches!(
        result,
        Err(SimulationError::InvalidDuration { field: "tick", .. })
    ));

    let mut configuration = config(41);
    configuration.epoch = SystemTime::UNIX_EPOCH - Duration::from_secs(1);
    let result = configuration.run("invalid epoch", |_simulation| {});
    assert!(matches!(result, Err(SimulationError::InvalidEpoch { .. })));

    let mut configuration = config(41);
    configuration.bounds.wall_duration = Duration::MAX;
    let result = configuration.run("unbounded wall time", |_simulation| {});
    assert!(matches!(
        result,
        Err(SimulationError::UnrepresentableWallDuration { .. })
    ));
}

#[test]
fn simulation_scheduler_panic_reaches_result() {
    let result = config(41).run("scheduler panic", |_simulation| {
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
    assert!(error.to_string().contains("scheduler setup failed"));
}

#[test]
fn simulated_utc_is_the_epoch_plus_simulated_elapsed_time() {
    let configuration = config(41);
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
    let result = config(41).run("trace order", move |simulation| {
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

/// The scenario the fresh-process check injects a failure into, and when the failure happens.
const INJECTED_TEST: &str = "peer_resolution_uses_simulated_dns";
const INJECTED_AT: &str = "100ms";

/// A scenario whose trace names the process that ran it, so its two runs always diverge. It runs
/// only when [`diverged_and_panicked_runs_leave_records`] starts it in a fresh process.
#[test]
#[ignore = "started in a fresh process by diverged_and_panicked_runs_leave_records"]
fn process_dependent_trace() {
    let scenario = Scenario {
        name: "process-dependent trace",
        fault_plan: "none; the trace names the process that ran the attempt",
        seeds: &[41],
    };
    scenario.check(config, |run| {
        let trace = run.trace();
        run.simulate(move |simulation| {
            simulation.client("observer", async move {
                trace.record("observer", format!("ran in process {}", std::process::id()));
                Ok(())
            });
        })
    });
}

/// A scenario whose check after the simulation fails. It runs only when
/// [`diverged_and_panicked_runs_leave_records`] starts it in a fresh process.
#[test]
#[ignore = "started in a fresh process by diverged_and_panicked_runs_leave_records"]
fn failing_check_after_the_simulation() {
    let scenario = Scenario {
        name: "failing check after the simulation",
        fault_plan: "none; the scenario's check after the run fails",
        seeds: &[41],
    };
    scenario.check(config, |run| {
        let trace = run.trace();
        let recorder = trace.clone();
        run.simulate(move |simulation| {
            simulation.client("observer", async move {
                recorder.record("observer", "simulation finished");
                Ok(())
            });
        })?;
        assert!(
            trace.events().is_empty(),
            "the check after the simulation expected no events"
        );
        Ok(())
    });
}

/// How a test run in a fresh process ended, with its standard output and error together.
struct FreshProcess {
    passed: bool,
    output: String,
}

/// Run one test of this binary in a fresh process, with only the driver variables given.
fn run_in_fresh_process(test: &str, variables: &[(&str, &Path)]) -> FreshProcess {
    let executable = std::env::current_exe().assured("the running test binary has a path");
    let mut command = Command::new(executable);
    command.args([
        test,
        "--exact",
        "--include-ignored",
        "--test-threads=1",
        "--nocapture",
    ]);
    for variable in [
        "NERVIX_TURMOIL_FAILURES",
        "NERVIX_TURMOIL_REPLAY",
        "NERVIX_TURMOIL_SWEEP",
        "NERVIX_TURMOIL_INJECT_FAILURE",
    ] {
        command.env_remove(variable);
    }
    for (variable, value) in variables {
        command.env(variable, value);
    }
    let Output {
        status,
        stdout,
        stderr,
    } = command.output().assured("the test binary starts again");
    FreshProcess {
        passed: status.success(),
        output: format!(
            "{}{}",
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        ),
    }
}

fn records_in(directory: &Path) -> Vec<PathBuf> {
    let mut records = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries = std::fs::read_dir(&directory).assured("the record directory is readable");
        for entry in entries {
            let path = entry
                .assured("the record directory lists its entries")
                .path();
            if path.is_dir() {
                pending.push(path);
            } else {
                records.push(path);
            }
        }
    }
    records
}

#[test]
fn injected_failure_is_recorded_and_replayed_in_a_fresh_process() {
    let directory = tempfile::tempdir().assured("the test creates a record directory");
    let failed = run_in_fresh_process(
        INJECTED_TEST,
        &[
            ("NERVIX_TURMOIL_FAILURES", directory.path()),
            ("NERVIX_TURMOIL_INJECT_FAILURE", Path::new(INJECTED_AT)),
        ],
    );
    let failed_output = failed.output;
    assert!(!failed.passed, "the injected run passed:\n{failed_output}");
    let records = records_in(directory.path());
    let [record] = records.as_slice() else {
        panic!("expected one failure record, found {records:?}:\n{failed_output}");
    };
    let text = std::fs::read_to_string(record).assured("the record was written");
    assert!(text.contains("injected harness failure at 100ms"), "{text}");
    assert!(
        text.contains("server resolved to its simulated address"),
        "{text}"
    );
    assert!(
        failed_output.contains("just test-turmoil-replay"),
        "{failed_output}"
    );

    let replayed = run_in_fresh_process(INJECTED_TEST, &[("NERVIX_TURMOIL_REPLAY", record)]);
    let replayed_output = replayed.output;
    assert!(
        !replayed.passed,
        "a reproduced failure fails its replay:\n{replayed_output}"
    );
    assert!(
        replayed_output.contains("turmoil replay: reproduced the recorded outcome and trace"),
        "{replayed_output}"
    );
    assert!(
        records_in(directory.path()) == records,
        "a replay leaves the record it replays in place"
    );
}

#[test]
fn diverged_and_panicked_runs_leave_records() {
    for (test, kind, detail) in [
        (
            "process_dependent_trace",
            "\"kind\": \"diverged\"",
            "two runs diverged at event 0",
        ),
        (
            "failing_check_after_the_simulation",
            "\"kind\": \"panicked\"",
            "the check after the simulation expected no events",
        ),
    ] {
        let directory = tempfile::tempdir().assured("the test creates a record directory");
        let failed = run_in_fresh_process(test, &[("NERVIX_TURMOIL_FAILURES", directory.path())]);
        assert!(!failed.passed, "{test} passed:\n{}", failed.output);
        assert!(failed.output.contains(detail), "{test}:\n{}", failed.output);
        let records = records_in(directory.path());
        let [record] = records.as_slice() else {
            panic!("{test}: expected one failure record, found {records:?}");
        };
        let text = std::fs::read_to_string(record).assured("the record was written");
        assert!(text.contains(kind), "{test}:\n{text}");
    }
}
