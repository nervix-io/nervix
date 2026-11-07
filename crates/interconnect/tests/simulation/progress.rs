//! Application progress reports and revision queries over the production transport while a
//! partition, a held reply and its repair interrupt them.
//!
//! Layer: test harness outside the product layer order.
//! - **Owns.** One reporter and one leader exchanging the production progress report and revision
//!   query, a partition and a held query reply between them, and the progress the leader holds.
//! - **Depends on.** The authenticated transport fixture and the production progress shapes, their
//!   deadlines and their merge.
//! - **Must not know.** Raft, the server's barriers, or how a node applies a revision.

use nervix_interconnect::{
    ApplicationProgress, ApplicationProgressPhase, ApplicationProgressReceipt,
    ApplicationProgressReport, ApplicationRevisionRequest,
};
use nervix_primitives::time::Instant;

use super::*;

const LEADER: &str = "leader";
const REPORTER: &str = "reporter";
/// How long one host waits for the other to reach a stage. A repaired partition can take most of
/// the liveness recovery budget before the transport carries requests again, so a stage wait
/// spans it.
const STAGE_DEADLINE: Duration = Duration::from_secs(75);
/// How long the observer waits for both hosts to finish, within the fault configuration's 85
/// simulated seconds.
const RUN_DEADLINE: Duration = Duration::from_secs(80);

/// How far the run has come, as the reporter announces it to the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Stage {
    Starting,
    /// The leader holds the first report.
    FirstReportHeld,
    /// The reporter is cut off from the leader and has reached revision 2.
    Partitioned,
    /// The partition is repaired and the reporter's next query reply is held.
    RepairedWithReplyHeld,
}

/// What the leader asks the reporter to do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum LeaderRequest {
    Nothing,
    /// The leader's query across the partition has ended.
    PartitionedQueryEnded,
    /// The held query reached its deadline; the reporter releases its replies.
    ReleaseReplies,
    /// The leader holds revision 2; the reporter reports revision 3 and then a late report.
    ReportLatest,
}

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("fixture node names are valid")
}

fn reporter_identity() -> ClusterNodeIdentity {
    ClusterNodeIdentity::new(node(REPORTER), ClusterNodeIncarnation::new(11))
}

fn live() -> BTreeSet<ClusterNodeName> {
    BTreeSet::from([node(LEADER), node(REPORTER)])
}

/// Answers liveness probes, so the peer can tell when a repaired link carries requests again.
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

/// The revision the leader holds for the reporter in `phase`, absent before it holds a report.
fn held_revision(
    held: &watch::Receiver<Option<ApplicationProgress>>,
    phase: ApplicationProgressPhase,
) -> Option<u64> {
    let held = held.borrow();
    let progress = held.as_ref()?;
    progress.reached(phase)
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

#[test]
fn application_progress_reaches_the_leader_through_a_partition_a_held_reply_and_repair() {
    Scenario {
        name: "application progress reaches the leader through a partition, a held reply and \
               repair",
        fault_plan: "partition the reporter from the leader after the first report, so a report \
                     and a revision query fail within their deadlines; repair, and once the link \
                     carries requests again hold the reporter's query reply until the leader's \
                     query reaches its deadline; release the reply, then report a newer and a \
                     late older record",
        seeds: &[131, 133],
    }
    .check(fault_config, progress_through_partition_and_hold);
}

fn progress_through_partition_and_hold(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let reporter_seed = seed.checked_add(1).assured("fixture seeds fit u64");
    // The Raft term the reporter observes the leader in, for every report of the run.
    let term = 3;
    let trace = run.trace();
    let authority = Authority::new();
    let leader_credentials = authority.issue(LEADER);
    let reporter_credentials = authority.issue(REPORTER);
    let (leader_ready, leader_ready_rx) = watch::channel(false);
    let (reporter_ready, reporter_ready_rx) = watch::channel(false);
    let (stage, stage_rx) = watch::channel(Stage::Starting);
    let (request, request_rx) = watch::channel(LeaderRequest::Nothing);
    let (done, done_rx) = watch::channel(false);
    let (finished, finished_rx) = watch::channel(0_usize);
    run.simulate(move |simulation| {
        let leader_finished = finished.clone();
        let leader_trace = trace.clone();
        simulation.host(LEADER, move || {
            let credentials = leader_credentials.clone();
            let ready = leader_ready.clone();
            let mut reporter_ready = reporter_ready_rx.clone();
            let mut stage = stage_rx.clone();
            let request = request.clone();
            let mut done = done_rx.clone();
            let finished = leader_finished.clone();
            let trace = leader_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(LEADER, credentials, seed).await;
                    let (held, held_rx) = watch::channel(None::<ApplicationProgress>);
                    let held_by_handler = held.clone();
                    transport
                        .register_handler::<ApplicationProgressReport, _, _>(
                            move |context, report| {
                                let held = held_by_handler.clone();
                                async move {
                                    if context.peer_node_id() != report.progress.identity.node_id()
                                    {
                                        return ApplicationProgressReceipt::ForeignReporter;
                                    }
                                    if report.leader.as_str() != LEADER || report.term != term {
                                        return ApplicationProgressReceipt::NotLeader;
                                    }
                                    held.send_modify(|held| match held {
                                        Some(progress) => {
                                            progress.absorb(&report.progress);
                                        }
                                        None => *held = Some(report.progress),
                                    });
                                    ApplicationProgressReceipt::Accepted
                                }
                            },
                        )
                        .assured("the fixture registers its report handler once");
                    answer_liveness(&transport);
                    transport.replace_live_nodes(&live());
                    ready.send_replace(true);
                    wait_for(&mut reporter_ready).await;
                    register_peer(&transport, REPORTER).await;
                    let reporter = node(REPORTER);

                    wait_until(&mut stage, Stage::FirstReportHeld).await;
                    assert_eq!(
                        held_revision(&held_rx, ApplicationProgressPhase::RuntimePrepared),
                        Some(1)
                    );
                    trace.record(LEADER, "holds the first report");

                    wait_until(&mut stage, Stage::Partitioned).await;
                    let started = Instant::now();
                    let outcome = transport
                        .request(&reporter, ApplicationRevisionRequest)
                        .await;
                    assert!(unanswered(&outcome), "partitioned query outcome: {outcome:?}");
                    assert!(
                        ended_within(started, ApplicationRevisionRequest::TIMEOUT),
                        "the partitioned query ended after {:?}",
                        started.elapsed()
                    );
                    trace.record(LEADER, "query across the partition fails within its deadline");
                    request.send_replace(LeaderRequest::PartitionedQueryEnded);

                    wait_until(&mut stage, Stage::RepairedWithReplyHeld).await;
                    // The partition broke the leader's connections, so the held query waits until
                    // the repaired link carries requests again and then meets only the held reply.
                    wait_for_connection(&transport, &reporter).await;
                    wait_for_liveness_recovery(&transport, &reporter).await;
                    trace.record(LEADER, "reaches the reporter again after the repair");
                    let started = Instant::now();
                    let outcome = transport
                        .request(&reporter, ApplicationRevisionRequest)
                        .await;
                    assert!(
                        matches!(&outcome, Err(error) if matches!(error.current_context(), RequestError::Timeout { .. })),
                        "held query outcome: {outcome:?}"
                    );
                    assert!(
                        ended_within(started, ApplicationRevisionRequest::TIMEOUT),
                        "the held query ended after {:?}",
                        started.elapsed()
                    );
                    trace.record(LEADER, "held query reaches its deadline");
                    request.send_replace(LeaderRequest::ReleaseReplies);

                    let answer = transport
                        .request(&reporter, ApplicationRevisionRequest)
                        .await
                        .assured("the released reporter answers its revision query");
                    assert_eq!(answer.identity, reporter_identity());
                    held.send_modify(|held| {
                        let progress = held.as_mut().assured("the first report is held");
                        progress.absorb(&answer);
                    });
                    assert_eq!(
                        held_revision(&held_rx, ApplicationProgressPhase::RuntimeReady),
                        Some(2)
                    );
                    trace.record(
                        LEADER,
                        "query after repair supplies the progress the failed report carried",
                    );
                    request.send_replace(LeaderRequest::ReportLatest);

                    // The reporter first waits for its own link to the leader to carry requests again.
                    wait_until(&mut done, true).await;
                    assert_eq!(
                        *held_rx.borrow(),
                        Some(ApplicationProgress {
                            identity: reporter_identity(),
                            authoritative: Some(9),
                            runtime_prepared: Some(3),
                            runtime_ready: Some(3),
                            domain_clock_installed: None,
                        }),
                        "a late report moves no phase back"
                    );
                    trace.record(LEADER, "a late report leaves the newer progress");
                    transport.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| *count += 1);
                result
            }
        });
        let reporter_finished = finished.clone();
        simulation.host(REPORTER, move || {
            let credentials = reporter_credentials.clone();
            let ready = reporter_ready.clone();
            let mut leader_ready = leader_ready_rx.clone();
            let stage = stage.clone();
            let mut request = request_rx.clone();
            let done = done.clone();
            let finished = reporter_finished.clone();
            let trace = trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(REPORTER, credentials, reporter_seed).await;
                    let (progress, progress_rx) =
                        watch::channel(ApplicationProgress::new(reporter_identity()));
                    let (replies_held, replies_held_rx) = watch::channel(false);
                    transport
                        .register_handler::<ApplicationRevisionRequest, _, _>(move |_, _| {
                            let answer = progress_rx.borrow().clone();
                            let mut replies_held = replies_held_rx.clone();
                            async move {
                                while *replies_held.borrow_and_update() {
                                    nervix_primitives::task::consume_budget().await;
                                    replies_held
                                        .changed()
                                        .await
                                        .assured("the reporter keeps its reply hold");
                                }
                                answer
                            }
                        })
                        .assured("the fixture registers its revision handler once");
                    answer_liveness(&transport);
                    transport.replace_live_nodes(&live());
                    ready.send_replace(true);
                    wait_for(&mut leader_ready).await;
                    register_peer(&transport, LEADER).await;
                    let leader = node(LEADER);
                    let report = |progress: &ApplicationProgress| ApplicationProgressReport {
                        progress: progress.clone(),
                        leader: node(LEADER),
                        term,
                    };

                    progress.send_modify(|progress| {
                        progress.advance(ApplicationProgressPhase::Authoritative, 5);
                        progress.advance(ApplicationProgressPhase::RuntimePrepared, 1);
                    });
                    let first = progress.borrow().clone();
                    let receipt = transport
                        .request(&leader, report(&first))
                        .await
                        .assured("the connected leader answers the first report");
                    assert_eq!(receipt, ApplicationProgressReceipt::Accepted);
                    trace.record(REPORTER, "first report accepted");
                    stage.send_replace(Stage::FirstReportHeld);

                    turmoil::partition(REPORTER, LEADER);
                    progress.send_modify(|progress| {
                        progress.advance(ApplicationProgressPhase::Authoritative, 7);
                        progress.advance(ApplicationProgressPhase::RuntimePrepared, 2);
                        progress.advance(ApplicationProgressPhase::RuntimeReady, 2);
                    });
                    let partitioned = progress.borrow().clone();
                    let started = Instant::now();
                    let outcome = transport.request(&leader, report(&partitioned)).await;
                    assert!(unanswered(&outcome), "partitioned report outcome: {outcome:?}");
                    assert!(
                        ended_within(started, ApplicationProgressReport::TIMEOUT),
                        "the partitioned report ended after {:?}",
                        started.elapsed()
                    );
                    trace.record(REPORTER, "report across the partition fails within its deadline");
                    stage.send_replace(Stage::Partitioned);

                    wait_until(&mut request, LeaderRequest::PartitionedQueryEnded).await;
                    replies_held.send_replace(true);
                    turmoil::repair(REPORTER, LEADER);
                    trace.record(REPORTER, "partition repaired with the reply held");
                    stage.send_replace(Stage::RepairedWithReplyHeld);

                    wait_until(&mut request, LeaderRequest::ReleaseReplies).await;
                    replies_held.send_replace(false);
                    trace.record(REPORTER, "reply released");

                    wait_until(&mut request, LeaderRequest::ReportLatest).await;
                    wait_for_connection(&transport, &leader).await;
                    wait_for_liveness_recovery(&transport, &leader).await;
                    let late = progress.borrow().clone();
                    progress.send_modify(|progress| {
                        progress.advance(ApplicationProgressPhase::Authoritative, 9);
                        progress.advance(ApplicationProgressPhase::RuntimePrepared, 3);
                        progress.advance(ApplicationProgressPhase::RuntimeReady, 3);
                    });
                    let newer = progress.borrow().clone();
                    let receipt = transport
                        .request(&leader, report(&newer))
                        .await
                        .assured("the repaired leader answers the newer report");
                    assert_eq!(receipt, ApplicationProgressReceipt::Accepted);
                    let receipt = transport
                        .request(&leader, report(&late))
                        .await
                        .assured("the repaired leader answers the late report");
                    assert_eq!(receipt, ApplicationProgressReceipt::Accepted);
                    trace.record(REPORTER, "newer and late reports accepted");
                    done.send_replace(true);
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
                while *finished.borrow() < 2 {
                    nervix_primitives::task::consume_budget().await;
                    finished.changed().await.assured("the fixture hosts remain alive");
                }
            })
            .await
            .assured("both hosts finish within the fault configuration's simulated budget");
            Ok(())
        });
    })
}
