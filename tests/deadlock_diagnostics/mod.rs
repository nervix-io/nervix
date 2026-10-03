//! Steps that observe diagnostic nodes, in a scenario binary built for the `deloxide` mode: its
//! in-process nodes, which share the detector the binary installed, and real diagnostic server
//! processes, which record deadlock evidence. The module exists only in that build; the ordinary
//! suite leaves its scenarios out.
//!
//! Layer: test harness.
//! - **Owns.** Checking that this process runs its deadlock diagnostics, starting a real-process
//!   cluster of diagnostic nodes, signalling and awaiting all its members, and reading the evidence
//!   and logs they leave.
//! - **Depends on.** The deadlock evidence format, this process's diagnostic run, and the
//!   real-process cluster fixture.
//! - **Must not know.** How a node deadlocks: no step provokes one. The probes of nervix-deadlock
//!   own provoked deadlocks, in disposable processes.

use std::collections::BTreeSet;

use cucumber::{given, then, when};
use nervix_deadlock::{DeadlockEvidence, DiagnosticRun};
use nix::sys::signal::Signal;

use super::*;

/// What the deadlock detector prints when it starts, which no output of a diagnostic node may
/// contain.
const DETECTOR_BANNER: &str = "▄ ▄▖▖ ▄▖▖▖▄▖▄";

/// The program a diagnostic server process records itself as.
const SERVER_PROGRAM: &str = "nervix-server";

impl ScenarioWorld {
    fn diagnostic_cluster(&self) -> &ServerProcessCluster {
        self.server_process_cluster
            .as_ref()
            .verified("a preceding step started a diagnostic cluster")
    }

    fn diagnostic_cluster_mut(&mut self) -> &mut ServerProcessCluster {
        self.server_process_cluster
            .as_mut()
            .verified("a preceding step started a diagnostic cluster")
    }
}

#[given("the scenario process tracks its blocking locks for deadlocks")]
fn given_scenario_process_tracks_blocking_locks(_world: &mut ScenarioWorld) {
    assert!(
        nervix_primitives::deadlock::is_installed(),
        "the scenario binary installs its deadlock detector before any node starts"
    );
    let run = DiagnosticRun::current();
    let run = run.verified("the detector is installed only by the process's diagnostic run");
    assert_eq!(run.process().id, std::process::id());
}

#[then("the scenario process has recorded no deadlock findings")]
fn then_scenario_process_recorded_no_findings(_world: &mut ScenarioWorld) {
    let run = DiagnosticRun::current();
    let run = run.verified("a preceding step checked the process runs its diagnostics");
    // A finding would already have ended the process; this reads what its evidence says while it
    // runs: the process, and nothing found.
    let Some(file) = run.evidence_file() else {
        return;
    };
    let bytes = std::fs::read(file).unwrap_or_else(|error| {
        panic!("the scenario process's evidence {file:?} could not be read: {error}")
    });
    let evidence = DeadlockEvidence::decode(&bytes).unwrap_or_else(|error| {
        panic!("the scenario process's evidence {file:?} could not be decoded: {error:?}")
    });
    assert_eq!(evidence.process(), run.process());
    assert!(evidence.findings().is_empty(), "{:?}", evidence.findings());
}

#[given(
    expr = "a {int} node diagnostic nervix-server process cluster is started with deadlock \
            evidence"
)]
async fn given_diagnostic_server_process_cluster(world: &mut ScenarioWorld, nodes: usize) {
    assert!(
        world.server_process_cluster.is_none(),
        "a scenario starts at most one real-process cluster"
    );
    initialize_scenario_identity(world);
    world.server_process_cluster = Some(
        ServerProcessCluster::start_diagnostic(nodes)
            .await
            .unwrap_or_else(|error| panic!("failed to start the diagnostic cluster: {error}")),
    );
}

#[when("every server process of the cluster receives SIGTERM")]
async fn when_every_server_process_receives_sigterm(world: &mut ScenarioWorld) {
    let cluster = world.diagnostic_cluster_mut();
    for node_id in cluster.node_ids() {
        cluster
            .signal(&node_id, Signal::SIGTERM)
            .unwrap_or_else(|error| panic!("failed to signal server process {node_id}: {error}"));
    }
}

#[then(expr = "every server process of the cluster exits with status {int}")]
async fn then_every_server_process_exits_with_status(world: &mut ScenarioWorld, expected: i32) {
    let cluster = world.diagnostic_cluster_mut();
    for node_id in cluster.node_ids() {
        let status = cluster
            .wait_for_exit(&node_id)
            .await
            .unwrap_or_else(|error| panic!("server process {node_id} did not exit: {error}"));
        assert_eq!(
            status.code(),
            Some(expected),
            "server process {node_id} ended with {}",
            describe_exit(status)
        );
    }
}

#[then("no server process log of the cluster contains the deadlock detector's start-up output")]
fn then_no_server_process_log_contains_detector_banner(world: &mut ScenarioWorld) {
    let cluster = world.diagnostic_cluster();
    for node_id in cluster.node_ids() {
        let log = cluster
            .log(&node_id)
            .unwrap_or_else(|error| panic!("server process {node_id}'s log: {error}"));
        assert!(
            !log.contains(DETECTOR_BANNER),
            "server process {node_id} wrote the detector's banner:\n{log}"
        );
    }
}

#[then("every server process of the cluster recorded a running deadlock detector and no findings")]
fn then_every_server_process_recorded_running_detector(world: &mut ScenarioWorld) {
    let cluster = world.diagnostic_cluster();
    let evidence = cluster
        .deadlock_evidence()
        .read_all()
        .unwrap_or_else(|error| {
            panic!("the diagnostic cluster's deadlock evidence could not be read: {error:?}")
        });
    assert_eq!(
        evidence.len(),
        cluster.node_ids().len(),
        "every member records one evidence file"
    );
    let mut processes = BTreeSet::new();
    for evidence in &evidence {
        let process = evidence.process();
        let program = process.program.as_ref();
        let program = program
            .unwrap_or_else(|| panic!("evidence of process {} names no program", process.id));
        assert_eq!(program.as_str(), SERVER_PROGRAM);
        assert!(
            evidence.findings().is_empty(),
            "server process {} recorded {:?}",
            process.id,
            evidence.findings()
        );
        processes.insert(process.id);
    }
    assert_eq!(
        processes.len(),
        evidence.len(),
        "each file is a different process"
    );
}
