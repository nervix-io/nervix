//! A domain stop's drain and report requests over the production transport while one of three
//! nodes answers late: its drain view is held, and its report is cut off by a partition.
//!
//! Layer: test harness outside the product layer order.
//! - **Owns.** One stop owner and two nodes exchanging the production stop drain and report
//!   requests, a held view and a partition on the late node's link, and the views and reports the
//!   owner holds.
//! - **Depends on.** The authenticated transport fixture and the production stop request shapes,
//!   their deadlines and their coordination identity.
//! - **Must not know.** Raft, the server's stop owner, or how a node classifies its work.

use std::collections::BTreeMap;

use nervix_interconnect::domain_stop::{
    StopDrainAction, StopDrainRequest, StopDrainStatus, StopReportAnswer, StopReportRequest,
    StopWaitEntry, StopWaitKind,
};
use nervix_models::{CoordinationIdentity, NodeRef, StopCut, StopCutKind};
use nervix_primitives::time::Instant;

use super::*;

const OWNER: &str = "owner";
const PROMPT: &str = "prompt";
const LATE: &str = "late";
/// How long one host waits for another to reach a stage. A repaired partition can take most of
/// the liveness recovery budget before the transport carries requests again, so a stage wait
/// spans it.
const STAGE_DEADLINE: Duration = Duration::from_secs(75);
/// How long the observer waits for every host to finish, within the fault configuration's 85
/// simulated seconds.
const RUN_DEADLINE: Duration = Duration::from_secs(80);
/// The relay admissions each node counted for the domain, which a confirming round compares.
const PROMPT_MOTION: u64 = 17;
const LATE_MOTION: u64 = 23;

/// What the owner asks the late node to do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OwnerRequest {
    Nothing,
    /// The held view reached its deadline; the late node releases its views.
    ReleaseViews,
    /// The partitioned report request ended and the link is repaired; the late node's teardown
    /// is still running.
    Repaired,
    /// The owner saw the late node's report pending; its teardown ends.
    EndTeardown,
}

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("fixture node names are valid")
}

fn domain() -> DomainName {
    DomainName::parse("stopping").assured("the fixture domain name is valid")
}

fn live() -> BTreeSet<ClusterNodeName> {
    BTreeSet::from([node(OWNER), node(PROMPT), node(LATE)])
}

fn entity(kind: ModelKind, name: &str) -> NodeRef {
    NodeRef::new(
        kind,
        ModelName::parse(name).assured("fixture entity names are valid"),
    )
}

/// A node's view at rest: no work in motion, a held row it will cut, every obligation complete.
fn at_rest(motion: u64) -> StopDrainStatus {
    StopDrainStatus {
        entries: vec![StopWaitEntry {
            entity: Some(entity(ModelKind::Reorderer, "ordered")),
            kind: StopWaitKind::ProcessorHeld,
            items: 2,
        }],
        flush_obligations: 0,
        motion,
    }
}

/// The prompt node's first view: a route buffer still releasing its batch.
fn releasing() -> StopDrainStatus {
    StopDrainStatus {
        entries: vec![StopWaitEntry {
            entity: Some(entity(ModelKind::Junction, "routed")),
            kind: StopWaitKind::NodeWork,
            items: 1,
        }],
        flush_obligations: 1,
        motion: PROMPT_MOTION,
    }
}

fn cuts(kind: ModelKind, name: &str, cut: StopCutKind, items: u64) -> Vec<StopCut> {
    let entity = entity(kind, name);
    vec![StopCut {
        entity_kind: entity.kind,
        entity: entity.identifier,
        kind: cut,
        items,
    }]
}

fn prompt_cuts() -> Vec<StopCut> {
    cuts(ModelKind::Reorderer, "ordered", StopCutKind::ReordererRows, 2)
}

fn late_cuts() -> Vec<StopCut> {
    cuts(
        ModelKind::Emitter,
        "published",
        StopCutKind::EmitterUndelivered,
        5,
    )
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

fn drain(coordination: &CoordinationIdentity, action: StopDrainAction) -> StopDrainRequest {
    StopDrainRequest {
        coordination: coordination.clone(),
        domain: domain(),
        action,
    }
}

fn report(coordination: &CoordinationIdentity) -> StopReportRequest {
    StopReportRequest {
        coordination: coordination.clone(),
        domain: domain(),
        start_version: 3,
    }
}

/// The view `peer` answers to `request`, which must arrive.
async fn view(
    transport: &Transport,
    peer: &ClusterNodeName,
    request: StopDrainRequest,
) -> StopDrainStatus {
    transport
        .request(peer, request)
        .await
        .assured("the reachable node answers the drain request")
        .assured("the fixture node reports its view")
}

#[test]
fn a_stop_waits_for_a_node_that_answers_late_and_collects_its_report_after_repair() {
    Scenario {
        name: "a stop waits for a node that answers late and collects its report after repair",
        fault_plan: "hold the late node's drain view until the owner's request reaches its \
                     deadline while the prompt node answers, release it for the confirming \
                     rounds, then partition the late node from the owner across a report request \
                     and repair; the late node reports pending until its teardown ends",
        seeds: &[151, 153],
    }
    .check(fault_config, stop_with_a_late_node);
}

/// Serves the stop requests of a node whose views and report the closures decide.
fn serve_stop<V, R>(transport: &Transport, view: V, report: R)
where
    V: Fn(StopDrainAction) -> watch::Receiver<Option<StopDrainStatus>> + Send + Sync + 'static,
    R: Fn() -> StopReportAnswer + Send + Sync + 'static,
{
    transport
        .register_handler::<StopDrainRequest, _, _>(move |context, request| {
            assert_eq!(context.peer_node_id(), &node(OWNER));
            assert_eq!(request.coordination.coordinator(), &node(OWNER));
            assert_eq!(request.domain, domain());
            let mut answer = view(request.action);
            async move {
                loop {
                    nervix_primitives::task::consume_budget().await;
                    if let Some(view) = answer.borrow_and_update().clone() {
                        return Ok(view);
                    }
                    answer
                        .changed()
                        .await
                        .assured("the node keeps its view publication");
                }
            }
        })
        .assured("the fixture registers its drain handler once");
    transport
        .register_handler::<StopReportRequest, _, _>(move |context, request| {
            assert_eq!(context.peer_node_id(), &node(OWNER));
            assert_eq!(request.start_version, 3);
            let answer = report();
            async move { Ok(answer) }
        })
        .assured("the fixture registers its report handler once");
}

fn stop_with_a_late_node(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let trace = run.trace();
    let authority = Authority::new();
    let owner_credentials = authority.issue(OWNER);
    let prompt_credentials = authority.issue(PROMPT);
    let late_credentials = authority.issue(LATE);
    let (owner_ready, owner_ready_rx) = watch::channel(false);
    let (prompt_ready, prompt_ready_rx) = watch::channel(false);
    let (late_ready, late_ready_rx) = watch::channel(false);
    let (request, request_rx) = watch::channel(OwnerRequest::Nothing);
    let (done, done_rx) = watch::channel(false);
    let (finished, finished_rx) = watch::channel(0_usize);
    run.simulate(move |simulation| {
        let owner_finished = finished.clone();
        let owner_trace = trace.clone();
        simulation.host(OWNER, move || {
            let credentials = owner_credentials.clone();
            let ready = owner_ready.clone();
            let mut prompt_ready = prompt_ready_rx.clone();
            let mut late_ready = late_ready_rx.clone();
            let request = request.clone();
            let done = done.clone();
            let finished = owner_finished.clone();
            let trace = owner_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(OWNER, credentials, seed).await;
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
                        .assured("the fixture owner allocates its drain identity");

                    // The releasing round: the prompt node still releases a batch, and the late
                    // node's view is held, so the round cannot find the domain at rest.
                    let releasing_view = view(
                        &transport,
                        &prompt,
                        drain(&coordination, StopDrainAction::FlushMovingWork),
                    )
                    .await;
                    assert!(releasing_view.moving());
                    let started = Instant::now();
                    let held = transport
                        .request(&late, drain(&coordination, StopDrainAction::FlushMovingWork))
                        .await;
                    assert!(unanswered(&held), "held view outcome: {held:?}");
                    assert!(
                        ended_within(started, StopDrainRequest::DEADLINE.declared_default()),
                        "the held view ended after {:?}",
                        started.elapsed()
                    );
                    trace.record(OWNER, "a held view ends unanswered within its deadline");
                    request.send_replace(OwnerRequest::ReleaseViews);

                    // The confirming round and the round that compares it: every node answers at
                    // rest, the late node's view arrives intact, and nothing moved between them.
                    let mut confirming = BTreeMap::new();
                    for (peer, action) in [
                        (&prompt, StopDrainAction::Confirm),
                        (&late, StopDrainAction::Confirm),
                    ] {
                        let answered = view(&transport, peer, drain(&coordination, action)).await;
                        assert!(!answered.moving());
                        confirming.insert(peer.clone(), answered.motion);
                    }
                    let mut observed = BTreeMap::new();
                    for peer in [&prompt, &late] {
                        let answered = view(
                            &transport,
                            peer,
                            drain(&coordination, StopDrainAction::Observe),
                        )
                        .await;
                        assert_eq!(answered.flush_obligations, 0);
                        observed.insert(peer.clone(), answered);
                    }
                    assert_eq!(
                        observed.get(&late),
                        Some(&at_rest(LATE_MOTION)),
                        "the late node's view arrives intact once it answers"
                    );
                    let observed_motion = observed
                        .iter()
                        .map(|(peer, view)| (peer.clone(), view.motion))
                        .collect::<BTreeMap<_, _>>();
                    assert_eq!(observed_motion, confirming);
                    trace.record(OWNER, "the confirming rounds find every node at rest");

                    // The stop ended; its report is collected across a partition of the late
                    // node, which the owner retries until it reports.
                    turmoil::partition(OWNER, LATE);
                    let started = Instant::now();
                    let partitioned = transport.request(&late, report(&coordination)).await;
                    assert!(
                        unanswered(&partitioned),
                        "partitioned report outcome: {partitioned:?}"
                    );
                    assert!(
                        ended_within(started, StopReportRequest::DEADLINE.declared_default()),
                        "the partitioned report ended after {:?}",
                        started.elapsed()
                    );
                    trace.record(OWNER, "a report across the partition ends unanswered");
                    turmoil::repair(OWNER, LATE);
                    request.send_replace(OwnerRequest::Repaired);
                    wait_for_connection(&transport, &late).await;
                    wait_for_liveness_recovery(&transport, &late).await;
                    let pending = transport
                        .request(&late, report(&coordination))
                        .await
                        .assured("the repaired late node answers its report request")
                        .assured("the late node answers its report");
                    assert_eq!(pending, StopReportAnswer::Pending);
                    request.send_replace(OwnerRequest::EndTeardown);
                    let mut late_report = None;
                    for _ in 0..8 {
                        nervix_primitives::task::consume_budget().await;
                        let answer = transport
                            .request(&late, report(&coordination))
                            .await
                            .assured("the repaired late node answers its report request")
                            .assured("the late node answers its report");
                        if answer != StopReportAnswer::Pending {
                            late_report = Some(answer);
                            break;
                        }
                        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
                    }
                    assert_eq!(
                        late_report,
                        Some(StopReportAnswer::Reported(late_cuts())),
                        "the late node reports what its teardown cut once it ended"
                    );
                    let prompt_report = transport
                        .request(&prompt, report(&coordination))
                        .await
                        .assured("the prompt node answers its report request")
                        .assured("the prompt node answers its report");
                    assert_eq!(prompt_report, StopReportAnswer::Reported(prompt_cuts()));
                    trace.record(OWNER, "every node's report is collected after the repair");
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
        let prompt_owner_ready = owner_ready_rx.clone();
        simulation.host(PROMPT, move || {
            let credentials = prompt_credentials.clone();
            let ready = prompt_ready.clone();
            let mut owner_ready = prompt_owner_ready.clone();
            let mut done = prompt_done_rx.clone();
            let finished = prompt_finished.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(PROMPT, credentials, seed + 1).await;
                    answer_liveness(&transport);
                    serve_stop(
                        &transport,
                        |action| {
                            let answer = match action {
                                StopDrainAction::FlushMovingWork => releasing(),
                                StopDrainAction::Confirm | StopDrainAction::Observe => {
                                    at_rest(PROMPT_MOTION)
                                }
                            };
                            watch::channel(Some(answer)).1
                        },
                        || StopReportAnswer::Reported(prompt_cuts()),
                    );
                    transport.replace_live_nodes(&live());
                    ready.send_replace(true);
                    wait_for(&mut owner_ready).await;
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
        let late_owner_ready = owner_ready_rx.clone();
        let late_trace = trace.clone();
        simulation.host(LATE, move || {
            let credentials = late_credentials.clone();
            let ready = late_ready.clone();
            let mut owner_ready = late_owner_ready.clone();
            let mut done = late_done_rx.clone();
            let mut request = request_rx.clone();
            let finished = late_finished.clone();
            let trace = late_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(LATE, credentials, seed + 2).await;
                    answer_liveness(&transport);
                    // The view stays unpublished, so a request waits for it, until the owner's
                    // request to release it.
                    let (views, views_rx) = watch::channel(None::<StopDrainStatus>);
                    let (teardown_ended, teardown_ended_rx) = watch::channel(false);
                    serve_stop(
                        &transport,
                        move |_| views_rx.clone(),
                        move || {
                            if *teardown_ended_rx.borrow() {
                                StopReportAnswer::Reported(late_cuts())
                            } else {
                                StopReportAnswer::Pending
                            }
                        },
                    );
                    transport.replace_live_nodes(&live());
                    ready.send_replace(true);
                    wait_for(&mut owner_ready).await;

                    wait_until(&mut request, OwnerRequest::ReleaseViews).await;
                    views.send_replace(Some(at_rest(LATE_MOTION)));
                    trace.record(LATE, "views released");

                    wait_until(&mut request, OwnerRequest::EndTeardown).await;
                    teardown_ended.send_replace(true);
                    trace.record(LATE, "teardown ended");
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
                    finished.changed().await.assured("the fixture hosts remain alive");
                }
            })
            .await
            .assured("every host finishes within the fault configuration's simulated budget");
            Ok(())
        });
    })
}
