//! Application progress reports and revision queries over the production transport while a
//! partition, a held reply and its repair interrupt them.
//!
//! Layer: test harness outside the product layer order.
//! - **Owns.** A leader and two reporters exchanging the production progress report and revision
//!   query, one of which fails to prepare a runtime revision; a partition and a held query reply
//!   between them, and the progress, and the failure with its reason, the leader holds.
//! - **Depends on.** The authenticated transport fixture and the production progress shapes, their
//!   deadlines and their merge.
//! - **Must not know.** Raft, the server's barriers, or how a node applies a revision.

use std::collections::BTreeMap;

use nervix_interconnect::{
    ApplicationProgress, ApplicationProgressPhase, ApplicationProgressReceipt,
    ApplicationProgressReport, ApplicationRevisionRequest, RuntimeApplicationFailure,
};
use nervix_primitives::time::Instant;

use super::*;

const LEADER: &str = "leader";
const REPORTER: &str = "reporter";
/// The reporter that fails to prepare runtime revision 2 and prepares it later.
const FAILING: &str = "failing";
/// Why the failing reporter did not prepare runtime revision 2, as it renders the failure.
const FAILURE_REASON: &str =
    "failed to build domain execution for 'tenant': injected entity-level schedule apply failure";
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

/// How far the failing reporter has come, as it announces it to the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum FailingStage {
    Starting,
    /// The leader holds the failing reporter's first report.
    FirstReportHeld,
    /// The failing reporter failed revision 2 and could not report it across its partition.
    FailedWhilePartitioned,
    /// The failing reporter's partition is repaired.
    Repaired,
    /// The failing reporter prepared revision 2 and reported it.
    Succeeded,
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
    /// The leader holds the failing reporter's failure; it prepares the revision and reports.
    ReportSuccess,
}

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("fixture node names are valid")
}

fn reporter_identity() -> ClusterNodeIdentity {
    ClusterNodeIdentity::new(node(REPORTER), ClusterNodeIncarnation::new(11))
}

fn failing_identity() -> ClusterNodeIdentity {
    ClusterNodeIdentity::new(node(FAILING), ClusterNodeIncarnation::new(13))
}

fn live() -> BTreeSet<ClusterNodeName> {
    BTreeSet::from([node(LEADER), node(REPORTER), node(FAILING)])
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

/// The progress the leader holds, by reporting incarnation.
type HeldProgress = BTreeMap<ClusterNodeIdentity, ApplicationProgress>;

/// The revision the leader holds for `reporter` in `phase`, absent before it holds a report.
fn held_revision(
    held: &watch::Receiver<HeldProgress>,
    reporter: &ClusterNodeIdentity,
    phase: ApplicationProgressPhase,
) -> Option<u64> {
    let held = held.borrow();
    let progress = held.get(reporter)?;
    progress.reached(phase)
}

/// Takes `progress` into what the leader holds for its incarnation.
fn hold(held: &watch::Sender<HeldProgress>, progress: &ApplicationProgress) {
    held.send_modify(|held| match held.get_mut(&progress.identity) {
        Some(holding) => {
            holding.absorb(progress);
        }
        None => {
            held.insert(progress.identity.clone(), progress.clone());
        }
    });
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
                     and a revision query fail within their deadlines, and partition the failing \
                     reporter too, so its report of a revision it failed to prepare fails; repair \
                     both, and once the link carries requests again hold the reporter's query \
                     reply until the leader's query reaches its deadline; release the reply, then \
                     report a newer and a late older record; query the failing reporter for its \
                     failure, then report its later success",
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
    let failing_credentials = authority.issue(FAILING);
    let failing_seed = seed.checked_add(2).assured("fixture seeds fit u64");
    let (failing_ready, failing_ready_rx) = watch::channel(false);
    let (failing_stage, failing_stage_rx) = watch::channel(FailingStage::Starting);
    let (finished, finished_rx) = watch::channel(0_usize);
    run.simulate(move |simulation| {
        let leader_finished = finished.clone();
        let leader_trace = trace.clone();
        let leader_failing_ready = failing_ready_rx.clone();
        let leader_failing_stage = failing_stage_rx.clone();
        let failing_leader_ready = leader_ready_rx.clone();
        let failing_reporter_stage = stage_rx.clone();
        let failing_request = request_rx.clone();
        simulation.host(LEADER, move || {
            let credentials = leader_credentials.clone();
            let ready = leader_ready.clone();
            let mut reporter_ready = reporter_ready_rx.clone();
            let mut failing_ready = leader_failing_ready.clone();
            let mut stage = stage_rx.clone();
            let mut failing_stage = leader_failing_stage.clone();
            let request = request.clone();
            let mut done = done_rx.clone();
            let finished = leader_finished.clone();
            let trace = leader_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(LEADER, credentials, seed).await;
                    let (held, held_rx) = watch::channel(HeldProgress::new());
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
                                    hold(&held, &report.progress);
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
                    wait_for(&mut failing_ready).await;
                    register_peer(&transport, FAILING).await;
                    let reporter = node(REPORTER);
                    let failing = node(FAILING);

                    wait_until(&mut stage, Stage::FirstReportHeld).await;
                    wait_until(&mut failing_stage, FailingStage::FirstReportHeld).await;
                    for identity in [reporter_identity(), failing_identity()] {
                        assert_eq!(
                            held_revision(
                                &held_rx,
                                &identity,
                                ApplicationProgressPhase::RuntimePrepared
                            ),
                            Some(1)
                        );
                    }
                    trace.record(LEADER, "holds the first report of each reporter");

                    wait_until(&mut stage, Stage::Partitioned).await;
                    let started = Instant::now();
                    let outcome = transport
                        .request(&reporter, ApplicationRevisionRequest)
                        .await;
                    assert!(unanswered(&outcome), "partitioned query outcome: {outcome:?}");
                    assert!(
                        ended_within(started, ApplicationRevisionRequest::DEADLINE.declared_default()),
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
                        ended_within(started, ApplicationRevisionRequest::DEADLINE.declared_default()),
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
                    hold(&held, &answer);
                    assert_eq!(
                        held_revision(
                            &held_rx,
                            &reporter_identity(),
                            ApplicationProgressPhase::RuntimeReady
                        ),
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
                        held_rx.borrow().get(&reporter_identity()),
                        Some(&ApplicationProgress {
                            identity: reporter_identity(),
                            authoritative: Some(9),
                            runtime_prepared: Some(3),
                            runtime_ready: Some(3),
                            domain_clock_installed: None,
                            runtime_failure: None,
                        }),
                        "a late report moves no phase back"
                    );
                    trace.record(LEADER, "a late report leaves the newer progress");

                    // The failing reporter's report of its failure met the partition, so a query
                    // after the repair carries the failure and its reason.
                    wait_until(&mut failing_stage, FailingStage::Repaired).await;
                    wait_for_connection(&transport, &failing).await;
                    wait_for_liveness_recovery(&transport, &failing).await;
                    let answer = transport
                        .request(&failing, ApplicationRevisionRequest)
                        .await
                        .assured("the repaired failing reporter answers its revision query");
                    assert_eq!(answer.identity, failing_identity());
                    hold(&held, &answer);
                    assert_eq!(
                        held_rx.borrow().get(&failing_identity()),
                        Some(&ApplicationProgress {
                            identity: failing_identity(),
                            authoritative: Some(7),
                            runtime_prepared: Some(1),
                            runtime_ready: None,
                            domain_clock_installed: None,
                            runtime_failure: Some(RuntimeApplicationFailure {
                                revision: 2,
                                reason: FAILURE_REASON.to_owned(),
                            }),
                        }),
                        "the leader holds the failure and its reason"
                    );
                    assert_eq!(
                        held_revision(
                            &held_rx,
                            &reporter_identity(),
                            ApplicationProgressPhase::RuntimePrepared
                        ),
                        Some(3),
                        "the other reporter prepared the revision the failing one did not"
                    );
                    trace.record(LEADER, "query after repair supplies the failure and its reason");
                    request.send_replace(LeaderRequest::ReportSuccess);

                    wait_until(&mut failing_stage, FailingStage::Succeeded).await;
                    assert_eq!(
                        held_rx.borrow().get(&failing_identity()),
                        Some(&ApplicationProgress {
                            identity: failing_identity(),
                            authoritative: Some(7),
                            runtime_prepared: Some(2),
                            runtime_ready: None,
                            domain_clock_installed: None,
                            runtime_failure: None,
                        }),
                        "preparing the failed revision later replaces the failure"
                    );
                    trace.record(LEADER, "a later success replaces the failure");
                    transport.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| *count += 1);
                result
            }
        });
        let failing_trace = trace.clone();
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
                        ended_within(started, ApplicationProgressReport::DEADLINE.declared_default()),
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
        let failing_finished = finished.clone();
        simulation.host(FAILING, move || {
            let credentials = failing_credentials.clone();
            let ready = failing_ready.clone();
            let mut leader_ready = failing_leader_ready.clone();
            let mut reporter_stage = failing_reporter_stage.clone();
            let failing_stage = failing_stage.clone();
            let mut request = failing_request.clone();
            let finished = failing_finished.clone();
            let trace = failing_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(FAILING, credentials, failing_seed).await;
                    let (progress, progress_rx) =
                        watch::channel(ApplicationProgress::new(failing_identity()));
                    transport
                        .register_handler::<ApplicationRevisionRequest, _, _>(move |_, _| {
                            let answer = progress_rx.borrow().clone();
                            async move { answer }
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
                    trace.record(FAILING, "first report accepted");
                    failing_stage.send_replace(FailingStage::FirstReportHeld);

                    wait_until(&mut reporter_stage, Stage::Partitioned).await;
                    turmoil::partition(FAILING, LEADER);
                    progress.send_modify(|progress| {
                        progress.advance(ApplicationProgressPhase::Authoritative, 7);
                        assert!(progress.fail_runtime(2, FAILURE_REASON.to_owned()));
                    });
                    let failed = progress.borrow().clone();
                    let started = Instant::now();
                    let outcome = transport.request(&leader, report(&failed)).await;
                    assert!(unanswered(&outcome), "partitioned failure report outcome: {outcome:?}");
                    assert!(
                        ended_within(started, ApplicationProgressReport::DEADLINE.declared_default()),
                        "the partitioned failure report ended after {:?}",
                        started.elapsed()
                    );
                    trace.record(FAILING, "failure report across the partition fails within its deadline");
                    failing_stage.send_replace(FailingStage::FailedWhilePartitioned);

                    wait_until(&mut reporter_stage, Stage::RepairedWithReplyHeld).await;
                    turmoil::repair(FAILING, LEADER);
                    trace.record(FAILING, "partition repaired");
                    failing_stage.send_replace(FailingStage::Repaired);

                    wait_until(&mut request, LeaderRequest::ReportSuccess).await;
                    wait_for_connection(&transport, &leader).await;
                    wait_for_liveness_recovery(&transport, &leader).await;
                    progress.send_modify(|progress| {
                        progress.advance(ApplicationProgressPhase::RuntimePrepared, 2);
                    });
                    let prepared = progress.borrow().clone();
                    let receipt = transport
                        .request(&leader, report(&prepared))
                        .await
                        .assured("the repaired leader answers the report of the later success");
                    assert_eq!(receipt, ApplicationProgressReceipt::Accepted);
                    trace.record(FAILING, "later success reported");
                    failing_stage.send_replace(FailingStage::Succeeded);
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
