//! A partition claim's release and forget over the production transport while a held reply, a
//! partition and its repair interrupt them.
//!
//! Layer: test harness outside the product layer order.
//! - **Owns.** One holding unit's owner and one claimant exchanging the production
//!   partition-transition requests, the holder's kept and forgotten key, the reply hold and
//!   partition between them, and the claim outcomes the claimant observes.
//! - **Depends on.** The authenticated transport fixture and the production claim shapes and their
//!   deadline.
//! - **Must not know.** How a server parks input, stops a branch or installs moved state.

use nervix_interconnect::{
    PartitionClaimOutcome, PartitionClaimStep, PartitionClaimSubject, PartitionTransitionAnswer,
    PartitionTransitionRequest, PartitionTransitionResponse, ReleasedKey, ReleasedRow,
};
use nervix_models::{
    BranchName, NodeRef, PartitionEpochNumber, RemoteRuntimeField, RemoteRuntimeRecordMetadata,
    RemoteRuntimeValue, Timestamp, Unit,
};
use nervix_primitives::time::Instant;

use super::*;

const HOLDER: &str = "holder";
const CLAIMANT: &str = "claimant";
/// How long one host waits for the other to reach a stage. A repaired partition can take most of
/// the liveness recovery budget before the transport carries requests again, so a stage wait spans
/// it.
const STAGE_DEADLINE: Duration = Duration::from_secs(75);
/// How long the observer waits for both hosts to finish, within the configuration's simulated
/// duration.
const RUN_DEADLINE: Duration = Duration::from_secs(140);

/// How far the claimant has come, as it announces it to the holder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Stage {
    Starting,
    /// The claimant's first release reached its deadline while the holder held its reply.
    ReleaseUnanswered,
    /// The claimant took the released row and is cut off from the holder.
    Partitioned,
    /// The claimant's forget reached the holder after the repair.
    Forgotten,
}

/// The one key the holder keeps outside its partition, and what the holder has done with it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Holding {
    /// Whether the holder still keeps the key's row.
    kept: bool,
    releases: usize,
    forgets: usize,
}

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("fixture node names are valid")
}

fn live() -> BTreeSet<ClusterNodeName> {
    BTreeSet::from([node(HOLDER), node(CLAIMANT)])
}

fn subject() -> PartitionClaimSubject {
    PartitionClaimSubject {
        domain: DomainName::parse(CLUSTER).assured("valid fixture domain"),
        entity: NodeRef::new(
            ModelKind::Relay,
            ModelName::parse("profiles").assured("valid fixture relay"),
        ),
        epoch: PartitionEpochNumber::FIRST.next(),
        holder: Unit::partition(1),
        claimant: Unit::partition(5),
    }
}

fn key() -> nervix_models::RemoteBranchKey {
    nervix_models::RemoteBranchKey::Direct(Box::new([RemoteRuntimeField {
        name: "tenant".to_string(),
        value: RemoteRuntimeValue::String("beta".to_string()),
    }]))
}

fn claim(step: PartitionClaimStep) -> PartitionTransitionRequest {
    PartitionTransitionRequest::Claim {
        subject: subject(),
        key: key(),
        step,
    }
}

/// The row the holder releases: the same every time it is asked, since it keeps the row until it
/// is told to forget it.
fn released_row() -> ReleasedRow {
    ReleasedRow {
        batch: vec![0xff, 0xff, 0xff, 0xff, 7, 11, 13],
        metadata: RemoteRuntimeRecordMetadata {
            ingested_at_low_watermark: Timestamp::from_unix_nanos(10),
            ingested_at_high_watermark: Timestamp::from_unix_nanos(20),
        },
    }
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

fn claim_config(seed: u64) -> SimulationConfig {
    let mut simulation = config(seed);
    simulation.bounds.simulated_duration = Duration::from_secs(150);
    simulation.bounds.max_steps = NonZeroUsize::new(200_000).assured("the step bound is nonzero");
    simulation.bounds.wall_duration = Duration::from_secs(120);
    simulation
}

#[test]
fn a_partition_claim_keeps_the_released_row_until_its_forget_arrives_through_loss_and_repair() {
    Scenario {
        name: "a partition claim keeps the released row until its forget arrives",
        fault_plan: "hold the holder's reply to the first release until it reaches its deadline, \
                     release again and take the same row; partition the claimant from the holder \
                     so its forget fails within its deadline while the holder keeps the row; \
                     repair, forget again, and read the holder's holdings",
        seeds: &[211, 213],
    }
    .check(claim_config, claim_through_hold_and_partition);
}

fn claim_through_hold_and_partition(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let claimant_seed = seed.checked_add(1).assured("fixture seeds fit u64");
    let trace = run.trace();
    let authority = Authority::new();
    let holder_credentials = authority.issue(HOLDER);
    let claimant_credentials = authority.issue(CLAIMANT);
    let (holder_ready, holder_ready_rx) = watch::channel(false);
    let (claimant_ready, claimant_ready_rx) = watch::channel(false);
    let (stage, stage_rx) = watch::channel(Stage::Starting);
    let (finished, finished_rx) = watch::channel(0_usize);
    run.simulate(move |simulation| {
        let holder_finished = finished.clone();
        let holder_trace = trace.clone();
        simulation.host(HOLDER, move || {
            let credentials = holder_credentials.clone();
            let ready = holder_ready.clone();
            let mut claimant_ready = claimant_ready_rx.clone();
            let mut stage = stage_rx.clone();
            let finished = holder_finished.clone();
            let trace = holder_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(HOLDER, credentials, seed).await;
                    let (holding, holding_rx) = watch::channel(Holding {
                        kept: true,
                        ..Holding::default()
                    });
                    // The first release's reply is held until the claimant's request has met its
                    // deadline; the holder has released the row by then and keeps it.
                    let (first_reply_held, first_reply_held_rx) = watch::channel(true);
                    let handled = holding.clone();
                    transport
                        .register_handler::<PartitionTransitionRequest, _, _>(
                            move |_context, request| {
                                let holding = handled.clone();
                                let mut held = first_reply_held_rx.clone();
                                async move {
                                    let answer = match request {
                                        PartitionTransitionRequest::Locate { .. } => {
                                            panic!(
                                                "this scenario sends direct-layout claim requests"
                                            )
                                        }
                                        PartitionTransitionRequest::Claim {
                                            step: PartitionClaimStep::Release,
                                            ..
                                        } => {
                                            let mut kept = false;
                                            holding.send_modify(|holding| {
                                                holding.releases += 1;
                                                kept = holding.kept;
                                            });
                                            while *held.borrow_and_update() {
                                                nervix_primitives::task::consume_budget().await;
                                                held.changed()
                                                    .await
                                                    .assured("the holder keeps its reply hold");
                                            }
                                            if kept {
                                                PartitionTransitionAnswer::Claim(
                                                    PartitionClaimOutcome::Released(
                                                        ReleasedKey::Row(released_row()),
                                                    ),
                                                )
                                            } else {
                                                PartitionTransitionAnswer::Claim(
                                                    PartitionClaimOutcome::NotHeld,
                                                )
                                            }
                                        }
                                        PartitionTransitionRequest::Claim {
                                            step: PartitionClaimStep::Forget,
                                            ..
                                        } => {
                                            holding.send_modify(|holding| {
                                                holding.forgets += 1;
                                                holding.kept = false;
                                            });
                                            PartitionTransitionAnswer::Claim(
                                                PartitionClaimOutcome::Forgotten,
                                            )
                                        }
                                        PartitionTransitionRequest::Holdings { .. } => {
                                            let pending = u64::from(holding.borrow().kept);
                                            PartitionTransitionAnswer::Holdings(vec![
                                                nervix_interconnect::UnitPartitionHoldings {
                                                    entity: subject().entity,
                                                    unit: subject().holder,
                                                    pending,
                                                    claimed: 0,
                                                },
                                            ])
                                        }
                                        PartitionTransitionRequest::CatalogShard { .. } => {
                                            PartitionTransitionAnswer::CatalogShard(
                                                nervix_interconnect::PartitionCatalogShard {
                                                    keys: Vec::new(),
                                                    next: None,
                                                },
                                            )
                                        }
                                    };
                                    PartitionTransitionResponse { result: Ok(answer) }
                                }
                            },
                        )
                        .assured("the fixture registers its claim handler once");
                    answer_liveness(&transport);
                    transport.replace_live_nodes(&live());
                    ready.send_replace(true);
                    wait_for(&mut claimant_ready).await;
                    register_peer(&transport, CLAIMANT).await;

                    wait_until(&mut stage, Stage::ReleaseUnanswered).await;
                    assert!(
                        holding_rx.borrow().kept,
                        "the holder dropped the row before any forget"
                    );
                    first_reply_held.send_replace(false);
                    trace.record(
                        HOLDER,
                        "releases the held reply after the first release ended",
                    );

                    wait_until(&mut stage, Stage::Partitioned).await;
                    assert!(
                        holding_rx.borrow().kept,
                        "the holder dropped the row while its forget could not reach it"
                    );
                    trace.record(HOLDER, "keeps the row while the claimant is cut off");

                    wait_until(&mut stage, Stage::Forgotten).await;
                    let observed = holding_rx.borrow().clone();
                    assert!(!observed.kept, "the holder kept the row after its forget");
                    assert!(
                        observed.releases >= 2,
                        "the claimant took the row without releasing it again: {observed:?}"
                    );
                    assert!(observed.forgets >= 1, "no forget reached the holder");
                    trace.record(HOLDER, "forgot the row once its forget arrived");
                    transport.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| *count += 1);
                result
            }
        });
        let claimant_finished = finished.clone();
        simulation.host(CLAIMANT, move || {
            let credentials = claimant_credentials.clone();
            let ready = claimant_ready.clone();
            let mut holder_ready = holder_ready_rx.clone();
            let stage = stage.clone();
            let finished = claimant_finished.clone();
            let trace = trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind(CLAIMANT, credentials, claimant_seed).await;
                    answer_liveness(&transport);
                    transport.replace_live_nodes(&live());
                    ready.send_replace(true);
                    wait_for(&mut holder_ready).await;
                    register_peer(&transport, HOLDER).await;
                    let holder = node(HOLDER);

                    let started = Instant::now();
                    let outcome = transport
                        .request(&holder, claim(PartitionClaimStep::Release))
                        .await;
                    assert!(unanswered(&outcome), "held release outcome: {outcome:?}");
                    assert!(
                        ended_within(
                            started,
                            transport.request_deadline::<PartitionTransitionRequest>()
                        ),
                        "the held release ended after {:?}",
                        started.elapsed()
                    );
                    trace.record(
                        CLAIMANT,
                        "release with a held reply fails within its deadline",
                    );
                    stage.send_replace(Stage::ReleaseUnanswered);

                    let released = loop {
                        nervix_primitives::task::consume_budget().await;
                        let response = transport
                            .request(&holder, claim(PartitionClaimStep::Release))
                            .await;
                        if let Ok(response) = response {
                            break response;
                        }
                        nervix_primitives::time::sleep(Duration::from_secs(1)).await;
                    };
                    assert_eq!(
                        released.result,
                        Ok(PartitionTransitionAnswer::Claim(
                            PartitionClaimOutcome::Released(ReleasedKey::Row(released_row()))
                        )),
                        "a release asked again answers the row the holder kept"
                    );
                    trace.record(CLAIMANT, "takes the kept row from a second release");

                    turmoil::partition(CLAIMANT, HOLDER);
                    let started = Instant::now();
                    let outcome = transport
                        .request(&holder, claim(PartitionClaimStep::Forget))
                        .await;
                    assert!(
                        unanswered(&outcome),
                        "partitioned forget outcome: {outcome:?}"
                    );
                    assert!(
                        ended_within(
                            started,
                            transport.request_deadline::<PartitionTransitionRequest>()
                        ),
                        "the partitioned forget ended after {:?}",
                        started.elapsed()
                    );
                    trace.record(
                        CLAIMANT,
                        "forget across the partition fails within its deadline",
                    );
                    stage.send_replace(Stage::Partitioned);

                    turmoil::repair(CLAIMANT, HOLDER);
                    wait_for_connection(&transport, &holder).await;
                    wait_for_liveness_recovery(&transport, &holder).await;
                    let forgotten = transport
                        .request(&holder, claim(PartitionClaimStep::Forget))
                        .await
                        .assured("the repaired holder answers the forget");
                    assert_eq!(
                        forgotten.result,
                        Ok(PartitionTransitionAnswer::Claim(
                            PartitionClaimOutcome::Forgotten
                        ))
                    );
                    let holdings = transport
                        .request(
                            &holder,
                            PartitionTransitionRequest::Holdings {
                                domain: subject().domain,
                                branch: BranchName::parse("by_tenant")
                                    .assured("valid fixture branch"),
                                epoch: subject().epoch,
                            },
                        )
                        .await
                        .assured("the repaired holder answers its holdings");
                    let Ok(PartitionTransitionAnswer::Holdings(units)) = holdings.result else {
                        panic!("the holder answered its holdings with another answer");
                    };
                    let pending = units.iter().map(|unit| unit.pending).sum::<u64>();
                    assert_eq!(pending, 0, "the holder still reports the forgotten key");
                    trace.record(CLAIMANT, "forget after the repair empties the holder");
                    stage.send_replace(Stage::Forgotten);
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
                    finished
                        .changed()
                        .await
                        .assured("the fixture hosts remain alive");
                }
            })
            .await
            .assured("both hosts finish within the scenario's simulated budget");
            Ok(())
        });
    })
}
