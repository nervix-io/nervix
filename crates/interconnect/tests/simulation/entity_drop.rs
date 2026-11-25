//! A drop's report request over the production transport while one of three nodes is partitioned
//! from the coordinator across the drop: the coordinator asks again after the repair and collects
//! what the node discarded once it applied the drop.
//!
//! Layer: test harness outside the product layer order.
//! - **Owns.** One coordinator and two nodes exchanging the production drop report request, a
//!   partition on the late node's link, and the reports the coordinator collects.
//! - **Depends on.** The authenticated transport fixture and the production drop report request
//!   shape, its deadline and its coordination identity.
//! - **Must not know.** Raft, the server's coordinator, or how a node destroys an entity.

use nervix_interconnect::entity_drop::{DropReportAnswer, DropReportRequest};
use nervix_models::{CoordinationIdentity, DiscardedWork, DiscardedWorkKind};
use nervix_primitives::time::Instant;

use super::*;

const COORDINATOR: &str = "coordinator";
const PROMPT: &str = "prompt";
const LATE: &str = "late";
/// How long one host waits for another to reach a stage. A repaired partition can take most of
/// the liveness recovery budget before the transport carries requests again, so a stage wait
/// spans it.
const STAGE_DEADLINE: Duration = Duration::from_secs(75);
/// How long the observer waits for every host to finish, within the fault configuration's 85
/// simulated seconds.
const RUN_DEADLINE: Duration = Duration::from_secs(80);
/// The runtime revision that published the drop.
const DROP_REVISION: u64 = 9;

/// What the coordinator lets the late node do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CoordinatorRequest {
    Nothing,
    /// The link is repaired; the late node has not applied the drop yet.
    Repaired,
    /// The coordinator saw the late node's report pending; the late node applies the drop.
    ApplyDrop,
}

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("fixture node names are valid")
}

fn domain() -> DomainName {
    DomainName::parse("dropping").assured("the fixture domain name is valid")
}

fn live() -> BTreeSet<ClusterNodeName> {
    BTreeSet::from([node(COORDINATOR), node(PROMPT), node(LATE)])
}

fn discarded(kind: ModelKind, name: &str, work: DiscardedWorkKind, items: u64) -> DiscardedWork {
    DiscardedWork {
        entity_kind: kind,
        entity: ModelName::parse(name).assured("fixture entity names are valid"),
        kind: work,
        items,
    }
}

fn prompt_report() -> Vec<DiscardedWork> {
    vec![discarded(
        ModelKind::Reorderer,
        "ordered",
        DiscardedWorkKind::HeldRows,
        2,
    )]
}

fn late_report() -> Vec<DiscardedWork> {
    vec![discarded(
        ModelKind::Emitter,
        "published",
        DiscardedWorkKind::UndeliveredRecords,
        5,
    )]
}

/// Answers liveness probes, so a peer can tell when a repaired link carries requests again.
fn answer_liveness(transport: &Transport) {
    transport
        .register_handler::<LivenessRequest, _, _>(|context, _| async move {
            LivenessResponse {
                peer: context.peer_node_id().clone(),
            }
        })
        .assured("the fixture registers its liveness handler once");
}

/// Waits until `signal` has come at least as far as `expected`.
async fn wait_until<T>(signal: &mut watch::Receiver<T>, expected: T)
where
    T: Copy + PartialOrd,
{
    nervix_primitives::time::timeout(STAGE_DEADLINE, async {
        while *signal.borrow_and_update() < expected {
            nervix_primitives::task::consume_budget().await;
            signal.changed().await.assured("fixture hosts remain alive");
        }
    })
    .await
    .assured("the peer reaches the expected stage within the simulated deadline");
}

/// Whether an exchange that started at `started` ended within `deadline`, allowing a quarter of the
/// deadline for the simulated transport to report the failure.
fn ended_within(started: Instant, deadline: Duration) -> bool {
    started.elapsed() <= deadline + deadline / 4
}

/// Whether `outcome` is the failure a request meets when its peer cannot answer in time.
fn unanswered<T: std::fmt::Debug>(outcome: &Result<T, error_stack::Report<RequestError>>) -> bool {
    match outcome {
        Ok(_) => false,
        Err(error) => matches!(
            error.current_context(),
            RequestError::Timeout { .. } | RequestError::Transport { .. }
        ),
    }
}

fn report(coordination: &CoordinationIdentity) -> DropReportRequest {
    DropReportRequest {
        coordination: coordination.clone(),
        domain: domain(),
        revision: DROP_REVISION,
    }
}

/// Serves the drop report requests of a node whose answer `answer` decides.
fn serve_drop_report<A>(transport: &Transport, answer: A)
where
    A: Fn() -> DropReportAnswer + Send + Sync + 'static,
{
    transport
        .register_handler::<DropReportRequest, _, _>(move |context, request| {
            assert_eq!(context.peer_node_id(), &node(COORDINATOR));
            assert_eq!(request.coordination.coordinator(), &node(COORDINATOR));
            assert_eq!(request.domain, domain());
            assert_eq!(request.revision, DROP_REVISION);
            let answer = answer();
            async move { Ok(answer) }
        })
        .assured("the fixture registers its drop report handler once");
}

#[test]
fn a_drop_report_reaches_a_node_partitioned_across_the_drop_after_repair() {
    Scenario {
        name: "a drop report reaches a node partitioned across the drop after repair",
        fault_plan: "partition the late node from the coordinator across a drop report request \
                     and repair; the late node reports pending until it applies the drop, while \
                     the prompt node reports at once",
        seeds: &[171, 173],
    }
    .check(fault_config, drop_with_a_partitioned_node);
}

fn drop_with_a_partitioned_node(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let trace = run.trace();
    let authority = Authority::new();
    let coordinator_credentials = authority.issue(COORDINATOR);
    let prompt_credentials = authority.issue(PROMPT);
    let late_credentials = authority.issue(LATE);
    let (coordinator_ready, coordinator_ready_rx) = watch::channel(false);
    let (prompt_ready, prompt_ready_rx) = watch::channel(false);
    let (late_ready, late_ready_rx) = watch::channel(false);
    let (request, request_rx) = watch::channel(CoordinatorRequest::Nothing);
    let (done, done_rx) = watch::channel(false);
    let (finished, finished_rx) = watch::channel(0_usize);
    run.simulate(move |simulation| {
        let coordinator_finished = finished.clone();
        let coordinator_trace = trace.clone();
        simulation.host(COORDINATOR, move || {
            let credentials = coordinator_credentials.clone();
            let ready = coordinator_ready.clone();
            let mut prompt_ready = prompt_ready_rx.clone();
            let mut late_ready = late_ready_rx.clone();
            let request = request.clone();
            let done = done.clone();
            let finished = coordinator_finished.clone();
            let trace = coordinator_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(COORDINATOR, credentials, seed).await;
                    answer_liveness(&transport);
                    transport.replace_live_nodes(&live());
                    ready.send_replace(true);
                    wait_for(&mut prompt_ready).await;
                    wait_for(&mut late_ready).await;
                    register_peer(&transport, PROMPT).await;
                    register_peer(&transport, LATE).await;
                    let prompt = node(PROMPT);
                    let late = node(LATE);
                    wait_for_connection(&transport, &prompt).await;
                    wait_for_connection(&transport, &late).await;
                    let coordination = transport
                        .next_coordination_identity()
                        .assured("the fixture coordinator allocates its identity");

                    // The prompt node applied the drop and reports at once.
                    let prompt_answer = transport
                        .request(&prompt, report(&coordination))
                        .await
                        .assured("the prompt node answers its drop report request")
                        .assured("the prompt node reports what it discarded");
                    assert_eq!(prompt_answer, DropReportAnswer::Reported(prompt_report()));
                    trace.record(COORDINATOR, "the prompt node reports at once");

                    // The late node is partitioned across the drop.
                    turmoil::partition(COORDINATOR, LATE);
                    let started = Instant::now();
                    let partitioned = transport.request(&late, report(&coordination)).await;
                    assert!(
                        unanswered(&partitioned),
                        "partitioned report outcome: {partitioned:?}"
                    );
                    assert!(
                        ended_within(started, DropReportRequest::DEADLINE.declared_default()),
                        "the partitioned report ended after {:?}",
                        started.elapsed()
                    );
                    trace.record(COORDINATOR, "a report across the partition ends unanswered");
                    turmoil::repair(COORDINATOR, LATE);
                    request.send_replace(CoordinatorRequest::Repaired);
                    wait_for_connection(&transport, &late).await;
                    wait_for_liveness_recovery(&transport, &late).await;

                    // After the repair the late node has not applied the drop yet; once it has,
                    // it reports what it discarded.
                    let pending = transport
                        .request(&late, report(&coordination))
                        .await
                        .assured("the repaired late node answers its drop report request")
                        .assured("the late node answers its drop report");
                    assert_eq!(pending, DropReportAnswer::Pending);
                    request.send_replace(CoordinatorRequest::ApplyDrop);
                    let mut late_answer = None;
                    for _ in 0..8 {
                        nervix_primitives::task::consume_budget().await;
                        let answer = transport
                            .request(&late, report(&coordination))
                            .await
                            .assured("the repaired late node answers its drop report request")
                            .assured("the late node answers its drop report");
                        if answer != DropReportAnswer::Pending {
                            late_answer = Some(answer);
                            break;
                        }
                        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
                    }
                    assert_eq!(
                        late_answer,
                        Some(DropReportAnswer::Reported(late_report())),
                        "the late node reports what it discarded once it applied the drop"
                    );
                    trace.record(COORDINATOR, "every node's report is collected after repair");
                    done.send_replace(true);
                    transport.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| *count += 1);
                result
            }
        });

        let prompt_finished = finished.clone();
        let prompt_done_rx = done_rx.clone();
        let prompt_coordinator_ready = coordinator_ready_rx.clone();
        simulation.host(PROMPT, move || {
            let credentials = prompt_credentials.clone();
            let ready = prompt_ready.clone();
            let mut coordinator_ready = prompt_coordinator_ready.clone();
            let mut done = prompt_done_rx.clone();
            let finished = prompt_finished.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(PROMPT, credentials, seed + 1).await;
                    answer_liveness(&transport);
                    serve_drop_report(&transport, || DropReportAnswer::Reported(prompt_report()));
                    transport.replace_live_nodes(&live());
                    ready.send_replace(true);
                    wait_for(&mut coordinator_ready).await;
                    wait_until(&mut done, true).await;
                    transport.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| *count += 1);
                result
            }
        });

        let late_finished = finished.clone();
        let late_done_rx = done_rx.clone();
        let late_coordinator_ready = coordinator_ready_rx.clone();
        let late_trace = trace.clone();
        simulation.host(LATE, move || {
            let credentials = late_credentials.clone();
            let ready = late_ready.clone();
            let mut coordinator_ready = late_coordinator_ready.clone();
            let mut done = late_done_rx.clone();
            let mut request = request_rx.clone();
            let finished = late_finished.clone();
            let trace = late_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(LATE, credentials, seed + 2).await;
                    answer_liveness(&transport);
                    let (applied, applied_rx) = watch::channel(false);
                    serve_drop_report(&transport, move || {
                        if *applied_rx.borrow() {
                            DropReportAnswer::Reported(late_report())
                        } else {
                            DropReportAnswer::Pending
                        }
                    });
                    transport.replace_live_nodes(&live());
                    ready.send_replace(true);
                    wait_for(&mut coordinator_ready).await;

                    wait_until(&mut request, CoordinatorRequest::ApplyDrop).await;
                    applied.send_replace(true);
                    trace.record(LATE, "drop applied");
                    wait_until(&mut done, true).await;
                    transport.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| *count += 1);
                result
            }
        });

        simulation.client("observer", async move {
            let mut finished = finished_rx;
            nervix_primitives::time::timeout(RUN_DEADLINE, async {
                while *finished.borrow() < 3 {
                    nervix_primitives::task::consume_budget().await;
                    finished
                        .changed()
                        .await
                        .assured("the fixture hosts remain alive");
                }
            })
            .await
            .assured("every host finishes within the fault configuration's simulated budget");
            Ok(())
        });
    })
}
