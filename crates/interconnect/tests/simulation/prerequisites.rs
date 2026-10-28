//! Prerequisite verification at join over the simulated network.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** The plans that partition a joiner whose heartbeat is beyond the compatibility bound
//!   from the member it dials, that roll a heartbeat change through three hosts, and that put a
//!   node of another cluster at an endpoint a returning node recorded for a member, each host's
//!   join terms, and the checks of every host's join progress, typed refusal and connections.
//! - **Depends on.** The transport fixture, the production handshake, its join progress and its
//!   connection observations.
//! - **Must not know.** Gossip, consensus admission, or how a server stops when it is refused.

use std::collections::BTreeMap;

use nervix_interconnect::{BootstrapExpectation, JoinProgress, JoinRefusal};
use nervix_models::{PrerequisiteMismatch, TimingPrerequisite};
use nervix_primitives::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use super::*;

const MEMBER: &str = "member";
const JOINER: &str = "joiner";
const LATECOMER: &str = "latecomer";
const RETURNING: &str = "returning";
/// The host a departed member was recorded at, which a node of another cluster now serves.
const DEPARTED: &str = "departed";
const STRANGER: &str = "stranger";
const ELSEWHERE: &str = "elsewhere";
const ALPHA: &str = "alpha";
const BRAVO: &str = "bravo";
const CHARLIE: &str = "charlie";

/// The heartbeat every host starts with.
const HEARTBEAT: Duration = Duration::from_millis(250);
/// A heartbeat a rolling change may reach: below twice every other host's.
const HEARTBEAT_WITHIN_BOUND: Duration = Duration::from_millis(400);
/// A heartbeat no rolling change may reach: at least twice every other host's.
const HEARTBEAT_BEYOND_BOUND: Duration = Duration::from_secs(1);
/// How long the joiner keeps dialing its member across the partition before it is repaired.
const PARTITION_HOLD: Duration = Duration::from_secs(3);

fn named(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("fixture node names are valid")
}

fn live(names: &[&str]) -> BTreeSet<ClusterNodeName> {
    let mut live = BTreeSet::new();
    for name in names {
        live.insert(named(name));
    }
    live
}

/// The fixture's join terms with `heartbeat` as the Raft heartbeat.
fn join_terms(heartbeat: Duration) -> JoinTerms {
    let fixture = fixture_join_terms();
    JoinTerms {
        durability: fixture.durability,
        timings: PrerequisiteTimings {
            raft_heartbeat_interval: heartbeat,
            ..fixture.timings
        },
    }
}

async fn bind_with_terms(
    name: &'static str,
    credentials: Credentials,
    seed: u64,
    join_terms: JoinTerms,
) -> Transport {
    let mut options = TransportOptions::default();
    let entropy = SimulatedEntropy::new(seed, name);
    options.entropy = TransportEntropy::from_source(move || entropy.next_u64());
    let (transport, _incoming) = Transport::bind(
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, PORT)),
        TransportIdentity {
            cluster_id: CLUSTER.to_string(),
            node_id: named(name),
            advertised_host: name.to_string(),
            join_terms,
        },
        credentials.bundle(),
        options,
        Executor::default(),
        PeerResolver::simulated(),
    )
    .await
    .assured("fixture transport binds on its simulated host");
    transport
}

/// Waits until `transport`'s join leaves `Joining`, and returns where it went.
async fn join_outcome(transport: &Transport) -> JoinProgress {
    let mut progress = transport.join_progress();
    nervix_primitives::time::timeout(HOST_DEADLINE, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let current = progress.borrow_and_update().clone();
            if current != JoinProgress::Joining {
                return current;
            }
            progress
                .changed()
                .await
                .assured("a running transport keeps publishing its join progress");
        }
    })
    .await
    .assured("the join ends within the simulated deadline")
}

/// The connection attempts `transport` counted as ended by a prerequisite refusal.
fn prerequisite_failures(transport: &Transport) -> u64 {
    let snapshot = transport.snapshot();
    let mut failures = 0_u64;
    for class in snapshot.counters.connection_failures {
        failures = failures
            .checked_add(class[ConnectionFailureReason::Prerequisite.index()])
            .assured("a simulated run counts far fewer than u64::MAX connection attempts");
    }
    failures
}

/// The refusal a host with `local` as its heartbeat reads after `peer`, holding `peer_heartbeat`,
/// refused it.
fn heartbeat_refusal(peer: &str, local: Duration, peer_heartbeat: Duration) -> JoinProgress {
    JoinProgress::Refused(JoinRefusal {
        peer: named(peer),
        mismatch: PrerequisiteMismatch::Timing {
            prerequisite: TimingPrerequisite::RaftHeartbeatInterval,
            local,
            peer: peer_heartbeat,
        },
    })
}

#[test]
fn a_joiner_beyond_the_heartbeat_bound_is_refused_once_a_partition_heals() {
    let scenario = Scenario {
        name: "a joiner beyond the heartbeat bound is refused once a partition heals",
        fault_plan: "the joiner dials its member across a partition that is repaired after three \
                     simulated seconds; a compatible latecomer joins the same member afterwards",
        seeds: &[111],
    };
    scenario.check(config, refuse_across_a_partition);
}

fn refuse_across_a_partition(run: ScenarioRun) -> Result<(), SimulationError> {
    let member_seed = run.seed();
    let joiner_seed = member_seed
        .checked_add(1)
        .assured("fixture seeds are below u64::MAX");
    let latecomer_seed = member_seed
        .checked_add(2)
        .assured("fixture seeds are below u64::MAX");
    let trace = run.trace();
    let authority = Authority::new();
    let member_credentials = authority.issue(MEMBER);
    let joiner_credentials = authority.issue(JOINER);
    let latecomer_credentials = authority.issue(LATECOMER);
    let (ready_tx, ready_rx) = watch::channel(false);
    let (refused_tx, refused_rx) = watch::channel(false);
    let (joined_tx, joined_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    run.simulate(move |simulation| {
        let member_finished = finished_tx.clone();
        let member_joined = joined_rx.clone();
        let member_trace = trace.clone();
        simulation.host(MEMBER, move || {
            let credentials = member_credentials.clone();
            let ready = ready_tx.clone();
            let joined = member_joined.clone();
            let finished = member_finished.clone();
            let trace = member_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let member =
                        bind_with_terms(MEMBER, credentials, member_seed, join_terms(HEARTBEAT))
                            .await;
                    member.replace_live_nodes(&live(&[MEMBER, JOINER, LATECOMER]));
                    ready.send_replace(true);
                    let mut joined = joined;
                    wait_for(&mut joined).await;
                    assert_eq!(
                        *member.join_progress().borrow(),
                        JoinProgress::Joining,
                        "a connection a peer dials never moves the join of the node it reaches"
                    );
                    trace.record(
                        MEMBER,
                        "refusing a joiner left the member's own join as it was",
                    );
                    member.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count
                        .checked_add(1)
                        .assured("only three fixture hosts finish");
                });
                result
            }
        });

        let joiner_finished = finished_tx.clone();
        let joiner_ready = ready_rx.clone();
        let joiner_joined = joined_rx.clone();
        let joiner_trace = trace.clone();
        simulation.host(JOINER, move || {
            let credentials = joiner_credentials.clone();
            let ready = joiner_ready.clone();
            let refused = refused_tx.clone();
            let joined = joiner_joined.clone();
            let finished = joiner_finished.clone();
            let trace = joiner_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let joiner = bind_with_terms(
                        JOINER,
                        credentials,
                        joiner_seed,
                        join_terms(HEARTBEAT_BEYOND_BOUND),
                    )
                    .await;
                    let mut ready = ready;
                    wait_for(&mut ready).await;
                    turmoil::partition(JOINER, MEMBER);
                    register_peer(&joiner, MEMBER).await;
                    joiner.replace_live_nodes(&live(&[MEMBER, JOINER]));
                    nervix_primitives::time::sleep(PARTITION_HOLD).await;
                    assert_eq!(
                        *joiner.join_progress().borrow(),
                        JoinProgress::Joining,
                        "a partition that keeps every handshake from completing decides no join"
                    );
                    assert_eq!(prerequisite_failures(&joiner), 0);
                    trace.record(JOINER, "the partitioned joiner is still joining");

                    turmoil::repair(JOINER, MEMBER);
                    assert_eq!(
                        join_outcome(&joiner).await,
                        heartbeat_refusal(MEMBER, HEARTBEAT_BEYOND_BOUND, HEARTBEAT),
                    );
                    assert!(
                        prerequisite_failures(&joiner) >= 1,
                        "the refusal is counted under its own reason: {:?}",
                        joiner.snapshot()
                    );
                    assert!(!joiner.is_connected_to(&named(MEMBER)));
                    trace.record(
                        JOINER,
                        "the repaired joiner was refused naming the heartbeat and both values",
                    );
                    refused.send_replace(true);
                    let mut joined = joined;
                    wait_for(&mut joined).await;
                    joiner.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count
                        .checked_add(1)
                        .assured("only three fixture hosts finish");
                });
                result
            }
        });

        let latecomer_finished = finished_tx.clone();
        let latecomer_trace = trace.clone();
        simulation.host(LATECOMER, move || {
            let credentials = latecomer_credentials.clone();
            let refused = refused_rx.clone();
            let joined = joined_tx.clone();
            let finished = latecomer_finished.clone();
            let trace = latecomer_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let mut refused = refused;
                    wait_for(&mut refused).await;
                    let latecomer = bind_with_terms(
                        LATECOMER,
                        credentials,
                        latecomer_seed,
                        join_terms(HEARTBEAT_WITHIN_BOUND),
                    )
                    .await;
                    register_peer(&latecomer, MEMBER).await;
                    latecomer.replace_live_nodes(&live(&[MEMBER, LATECOMER]));
                    assert_eq!(
                        join_outcome(&latecomer).await,
                        JoinProgress::Joined {
                            through: named(MEMBER),
                        },
                    );
                    trace.record(
                        LATECOMER,
                        "a compatible latecomer joined the member that refused the joiner",
                    );
                    joined.send_replace(true);
                    latecomer.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count
                        .checked_add(1)
                        .assured("only three fixture hosts finish");
                });
                result
            }
        });

        simulation.client("observer", async move {
            let mut finished = finished_rx;
            wait_for_count(&mut finished, 3).await;
            Ok(())
        });
    })
}

#[test]
fn a_rolling_heartbeat_change_joins_within_the_bound_and_is_refused_beyond_it() {
    let scenario = Scenario {
        name: "a rolling heartbeat change joins within the bound and is refused beyond it",
        fault_plan: "charlie crashes and restarts with a heartbeat within the bound, then bravo \
                     crashes and restarts with one beyond it, while alpha keeps running",
        seeds: &[113],
    };
    scenario.check(config, roll_a_heartbeat_change);
}

/// How far the roll has come, as the hosts report it.
mod roll {
    /// Charlie restarted and rejoined within the bound.
    pub(super) const CHARLIE_REJOINED: u8 = 1;
    /// Bravo restarted beyond the bound and was refused.
    pub(super) const BRAVO_REFUSED: u8 = 2;
}

/// Which hosts the control has restarted, in the order it restarts them.
mod restart {
    /// Charlie, once every host's first run has joined.
    pub(super) const CHARLIE: u8 = 1;
    /// Bravo, once charlie has rejoined.
    pub(super) const BRAVO: u8 = 2;
}

/// The seed of one run of a host: the scenario's, offset by the host and by how many times it
/// has started before.
fn run_seed(seed: u64, host_offset: u64, run: usize) -> u64 {
    let run = u64::try_from(run).assured("a fixture host starts a handful of times");
    seed.checked_add(host_offset)
        .assured("fixture seeds are far below u64::MAX")
        .checked_add(run)
        .assured("fixture seeds are far below u64::MAX")
}

/// Makes `transport` dial `peers` again from fresh targets, as gossip does once it sees a peer's
/// new run.
async fn rediscover(transport: &Transport, peers: &[&str]) {
    transport.replace_outbound_targets(&BTreeMap::new());
    for peer in peers {
        register_peer(transport, peer).await;
    }
}

fn roll_a_heartbeat_change(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let trace = run.trace();
    let authority = Authority::new();
    let alpha_credentials = authority.issue(ALPHA);
    let bravo_credentials = authority.issue(BRAVO);
    let charlie_credentials = authority.issue(CHARLIE);
    let (joined_tx, joined_rx) = watch::channel(0_usize);
    let (stage_tx, stage_rx) = watch::channel(0_u8);
    let (done_tx, done_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    let bravo_runs = StdArc::new(AtomicUsize::new(0));
    let charlie_runs = StdArc::new(AtomicUsize::new(0));
    let restarts = StdArc::new(AtomicU8::new(0));
    let control_restarts = restarts.clone();
    let control_joined = joined_rx.clone();
    let control_stage = stage_rx.clone();
    let result = run.simulate_with_control(
        move |simulation| {
            let alpha_joined = joined_tx.clone();
            let alpha_stage = stage_rx.clone();
            let alpha_done = done_tx.clone();
            let alpha_finished = finished_tx.clone();
            let alpha_trace = trace.clone();
            simulation.host(ALPHA, move || {
                let credentials = alpha_credentials.clone();
                let joined = alpha_joined.clone();
                let stage = alpha_stage.clone();
                let done = alpha_done.clone();
                let finished = alpha_finished.clone();
                let trace = alpha_trace.clone();
                async move {
                    let result = HostSupervisor::run(async move {
                        let alpha =
                            bind_with_terms(ALPHA, credentials, seed, join_terms(HEARTBEAT)).await;
                        register_peer(&alpha, BRAVO).await;
                        register_peer(&alpha, CHARLIE).await;
                        alpha.replace_live_nodes(&live(&[ALPHA, BRAVO, CHARLIE]));
                        let through = join_outcome(&alpha).await;
                        assert!(
                            matches!(&through, JoinProgress::Joined { .. }),
                            "alpha joined through a peer that shares its heartbeat: {through:?}"
                        );
                        joined.send_modify(|count| {
                            *count = count.checked_add(1).assured("three hosts join");
                        });
                        let mut stage = stage;
                        wait_for_stage(&mut stage, roll::CHARLIE_REJOINED).await;
                        rediscover(&alpha, &[BRAVO, CHARLIE]).await;
                        wait_for_connection(&alpha, &named(CHARLIE)).await;
                        trace.record(ALPHA, "alpha reconnected to charlie's new run");
                        wait_for_stage(&mut stage, roll::BRAVO_REFUSED).await;
                        rediscover(&alpha, &[BRAVO, CHARLIE]).await;
                        wait_for_prerequisite_refusal(&alpha).await;
                        wait_for_connection(&alpha, &named(CHARLIE)).await;
                        assert_eq!(
                            *alpha.join_progress().borrow(),
                            through,
                            "being refused by a newcomer never moves an incumbent's join"
                        );
                        trace.record(
                            ALPHA,
                            "alpha stayed joined and connected to charlie while bravo was refused",
                        );
                        done.send_replace(true);
                        alpha.shutdown().await;
                        Ok::<(), io::Error>(())
                    })
                    .await;
                    finished.send_modify(|count| {
                        *count = count.checked_add(1).assured("three hosts finish");
                    });
                    result
                }
            });

            let bravo_joined = joined_tx.clone();
            let bravo_stage = stage_tx.clone();
            let bravo_done = done_rx.clone();
            let bravo_finished = finished_tx.clone();
            let bravo_trace = trace.clone();
            simulation.host(BRAVO, move || {
                let run = bravo_runs.fetch_add(1, Ordering::SeqCst);
                let credentials = bravo_credentials.clone();
                let joined = bravo_joined.clone();
                let stage = bravo_stage.clone();
                let done = bravo_done.clone();
                let finished = bravo_finished.clone();
                let trace = bravo_trace.clone();
                async move {
                    let result = HostSupervisor::run(async move {
                        let process_seed = run_seed(seed, 10, run);
                        let heartbeat = if run == 0 {
                            HEARTBEAT
                        } else {
                            HEARTBEAT_BEYOND_BOUND
                        };
                        let bravo = bind_with_terms(
                            BRAVO,
                            credentials,
                            process_seed,
                            join_terms(heartbeat),
                        )
                        .await;
                        register_peer(&bravo, ALPHA).await;
                        register_peer(&bravo, CHARLIE).await;
                        bravo.replace_live_nodes(&live(&[ALPHA, BRAVO, CHARLIE]));
                        let outcome = join_outcome(&bravo).await;
                        if run == 0 {
                            assert!(
                                matches!(&outcome, JoinProgress::Joined { .. }),
                                "bravo joined through a peer that shares its heartbeat: \
                                 {outcome:?}"
                            );
                            joined.send_modify(|count| {
                                *count = count.checked_add(1).assured("three hosts join");
                            });
                            // The control crashes this run once charlie has rejoined.
                            std::future::pending::<()>().await;
                        }
                        let from_alpha =
                            heartbeat_refusal(ALPHA, HEARTBEAT_BEYOND_BOUND, HEARTBEAT);
                        let from_charlie = heartbeat_refusal(
                            CHARLIE,
                            HEARTBEAT_BEYOND_BOUND,
                            HEARTBEAT_WITHIN_BOUND,
                        );
                        assert!(
                            outcome == from_alpha || outcome == from_charlie,
                            "bravo is refused by the first peer it reaches, naming the heartbeat \
                             and both values: {outcome:?}"
                        );
                        trace.record(BRAVO, "bravo was refused beyond the bound");
                        stage.send_replace(roll::BRAVO_REFUSED);
                        let mut done = done;
                        wait_for(&mut done).await;
                        bravo.shutdown().await;
                        Ok::<(), io::Error>(())
                    })
                    .await;
                    if run > 0 {
                        finished.send_modify(|count| {
                            *count = count.checked_add(1).assured("three hosts finish");
                        });
                    }
                    result
                }
            });

            let charlie_joined = joined_tx.clone();
            let charlie_stage = stage_tx.clone();
            let charlie_stage_rx = stage_rx.clone();
            let charlie_done = done_rx.clone();
            let charlie_finished = finished_tx.clone();
            let charlie_trace = trace.clone();
            simulation.host(CHARLIE, move || {
                let run = charlie_runs.fetch_add(1, Ordering::SeqCst);
                let credentials = charlie_credentials.clone();
                let joined = charlie_joined.clone();
                let stage = charlie_stage.clone();
                let stage_rx = charlie_stage_rx.clone();
                let done = charlie_done.clone();
                let finished = charlie_finished.clone();
                let trace = charlie_trace.clone();
                async move {
                    let result = HostSupervisor::run(async move {
                        let process_seed = run_seed(seed, 20, run);
                        let heartbeat = if run == 0 {
                            HEARTBEAT
                        } else {
                            HEARTBEAT_WITHIN_BOUND
                        };
                        let charlie = bind_with_terms(
                            CHARLIE,
                            credentials,
                            process_seed,
                            join_terms(heartbeat),
                        )
                        .await;
                        register_peer(&charlie, ALPHA).await;
                        register_peer(&charlie, BRAVO).await;
                        charlie.replace_live_nodes(&live(&[ALPHA, BRAVO, CHARLIE]));
                        let through = join_outcome(&charlie).await;
                        assert!(
                            matches!(&through, JoinProgress::Joined { .. }),
                            "a heartbeat within the bound of every peer joins: {through:?}"
                        );
                        if run == 0 {
                            joined.send_modify(|count| {
                                *count = count.checked_add(1).assured("three hosts join");
                            });
                            // The control crashes this run once every host has joined.
                            std::future::pending::<()>().await;
                        }
                        trace.record(
                            CHARLIE,
                            "charlie rejoined with a heartbeat within the bound",
                        );
                        stage.send_replace(roll::CHARLIE_REJOINED);
                        let mut stage_rx = stage_rx;
                        wait_for_stage(&mut stage_rx, roll::BRAVO_REFUSED).await;
                        rediscover(&charlie, &[ALPHA, BRAVO]).await;
                        wait_for_connection(&charlie, &named(ALPHA)).await;
                        assert_eq!(
                            *charlie.join_progress().borrow(),
                            through,
                            "being refused by a newcomer never moves an incumbent's join"
                        );
                        let mut done = done;
                        wait_for(&mut done).await;
                        charlie.shutdown().await;
                        Ok::<(), io::Error>(())
                    })
                    .await;
                    if run > 0 {
                        finished.send_modify(|count| {
                            *count = count.checked_add(1).assured("three hosts finish");
                        });
                    }
                    result
                }
            });

            simulation.client("observer", async move {
                let mut finished = finished_rx;
                wait_for_count(&mut finished, 3).await;
                Ok(())
            });
        },
        move |simulation| {
            let restarted = control_restarts.load(Ordering::SeqCst);
            if restarted == 0 && *control_joined.borrow() == 3 {
                control_restarts.store(restart::CHARLIE, Ordering::SeqCst);
                simulation.crash(CHARLIE);
                simulation.bounce(CHARLIE);
            } else if restarted == restart::CHARLIE
                && *control_stage.borrow() >= roll::CHARLIE_REJOINED
            {
                control_restarts.store(restart::BRAVO, Ordering::SeqCst);
                simulation.crash(BRAVO);
                simulation.bounce(BRAVO);
            }
        },
    );
    result?;
    assert_eq!(
        restarts.load(Ordering::SeqCst),
        restart::BRAVO,
        "charlie and then bravo restarted exactly once each"
    );
    Ok(())
}

/// Waits until `transport` has counted a connection attempt a prerequisite refusal ended.
async fn wait_for_prerequisite_refusal(transport: &Transport) {
    nervix_primitives::time::timeout(HOST_DEADLINE, async {
        while prerequisite_failures(transport) == 0 {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .assured("a dial of a refusing peer is counted within the simulated deadline");
}

/// Waits until the roll has reached `expected`.
async fn wait_for_stage(stage: &mut watch::Receiver<u8>, expected: u8) {
    nervix_primitives::time::timeout(HOST_DEADLINE, async {
        while *stage.borrow_and_update() < expected {
            nervix_primitives::task::consume_budget().await;
            stage
                .changed()
                .await
                .assured("the roll's driver stays alive");
        }
    })
    .await
    .assured("the roll reaches each stage within the simulated deadline");
}

#[test]
fn a_recorded_endpoint_another_cluster_now_serves_is_a_stale_route() {
    let scenario = Scenario {
        name: "a recorded endpoint another cluster now serves is a stale route",
        fault_plan: "the endpoint a returning node recorded for a departed member is served by a \
                     node of another cluster sharing the certificate authority, while a recorded \
                     member still answers at its own endpoint",
        seeds: &[115],
    };
    scenario.check(config, cross_a_stale_route);
}

/// Binds node `node` of `cluster` on the simulated host `host`, with the fixture's join terms.
async fn bind_in_cluster(
    host: &'static str,
    node: &str,
    cluster: &str,
    credentials: Credentials,
    seed: u64,
) -> Transport {
    let mut options = TransportOptions::default();
    let entropy = SimulatedEntropy::new(seed, host);
    options.entropy = TransportEntropy::from_source(move || entropy.next_u64());
    let (transport, _incoming) = Transport::bind(
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, PORT)),
        TransportIdentity {
            cluster_id: cluster.to_string(),
            node_id: named(node),
            advertised_host: host.to_string(),
            join_terms: fixture_join_terms(),
        },
        credentials.bundle(),
        options,
        Executor::default(),
        PeerResolver::simulated(),
    )
    .await
    .assured("fixture transport binds on its simulated host");
    transport
}

/// The address `host` resolves to, with its name as the certificate name, as a node dials an
/// endpoint its stored membership recorded.
async fn recorded_target(transport: &Transport, host: &str) -> PeerTarget {
    transport
        .resolve(&NodeEndpoint::new(host, PORT))
        .await
        .assured("simulated DNS resolves the recorded host")
        .into_iter()
        .next()
        .assured("simulated DNS returned an address")
}

fn cross_a_stale_route(run: ScenarioRun) -> Result<(), SimulationError> {
    let member_seed = run.seed();
    let stranger_seed = member_seed
        .checked_add(1)
        .assured("fixture seeds are below u64::MAX");
    let returning_seed = member_seed
        .checked_add(2)
        .assured("fixture seeds are below u64::MAX");
    let trace = run.trace();
    let authority = Authority::new();
    let member_credentials = authority.issue(MEMBER);
    let stranger_credentials = authority.issue_for(ELSEWHERE, STRANGER, DEPARTED);
    let returning_credentials = authority.issue(RETURNING);
    let (ready_tx, ready_rx) = watch::channel(0_usize);
    let (done_tx, done_rx) = watch::channel(false);
    let (finished_tx, finished_rx) = watch::channel(0_usize);
    run.simulate(move |simulation| {
        let member_ready = ready_tx.clone();
        let member_done = done_rx.clone();
        let member_finished = finished_tx.clone();
        simulation.host(MEMBER, move || {
            let credentials = member_credentials.clone();
            let ready = member_ready.clone();
            let done = member_done.clone();
            let finished = member_finished.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let member =
                        bind_with_terms(MEMBER, credentials, member_seed, join_terms(HEARTBEAT))
                            .await;
                    member.replace_live_nodes(&live(&[MEMBER, RETURNING]));
                    ready.send_modify(|count| {
                        *count = count.checked_add(1).assured("two hosts announce readiness");
                    });
                    let mut done = done;
                    wait_for(&mut done).await;
                    member.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count
                        .checked_add(1)
                        .assured("only three fixture hosts finish");
                });
                result
            }
        });

        let stranger_ready = ready_tx.clone();
        let stranger_done = done_rx.clone();
        let stranger_finished = finished_tx.clone();
        simulation.host(DEPARTED, move || {
            let credentials = stranger_credentials.clone();
            let ready = stranger_ready.clone();
            let done = stranger_done.clone();
            let finished = stranger_finished.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let stranger =
                        bind_in_cluster(DEPARTED, STRANGER, ELSEWHERE, credentials, stranger_seed)
                            .await;
                    ready.send_modify(|count| {
                        *count = count.checked_add(1).assured("two hosts announce readiness");
                    });
                    let mut done = done;
                    wait_for(&mut done).await;
                    stranger.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count
                        .checked_add(1)
                        .assured("only three fixture hosts finish");
                });
                result
            }
        });

        let returning_finished = finished_tx.clone();
        let returning_trace = trace.clone();
        simulation.host(RETURNING, move || {
            let credentials = returning_credentials.clone();
            let ready = ready_rx.clone();
            let done = done_tx.clone();
            let finished = returning_finished.clone();
            let trace = returning_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let returning = bind_with_terms(
                        RETURNING,
                        credentials,
                        returning_seed,
                        join_terms(HEARTBEAT),
                    )
                    .await;
                    let mut ready = ready;
                    wait_for_count(&mut ready, 2).await;

                    let departed = recorded_target(&returning, DEPARTED).await;
                    let stale = returning
                        .bootstrap_target(
                            departed,
                            BootstrapExpectation::RecordedMember(named(DEPARTED)),
                        )
                        .await;
                    let error = match stale {
                        Ok(node) => panic!("node '{node}' answered for the departed member"),
                        Err(error) => error,
                    };
                    let TransportError::StaleRoute { endpoint, member } = error.current_context()
                    else {
                        panic!(
                            "another cluster at a recorded endpoint is a stale route: {error:?}"
                        );
                    };
                    assert_eq!(endpoint, &NodeEndpoint::new(DEPARTED, PORT));
                    assert_eq!(member, &named(DEPARTED));
                    assert_eq!(
                        *returning.join_progress().borrow(),
                        JoinProgress::Joining,
                        "a stale route refuses no one, so the join goes on"
                    );
                    assert_eq!(prerequisite_failures(&returning), 0);
                    trace.record(
                        RETURNING,
                        "the departed member's endpoint led to another cluster and refused no one",
                    );

                    let recorded = recorded_target(&returning, MEMBER).await;
                    let through = returning
                        .bootstrap_target(
                            recorded,
                            BootstrapExpectation::RecordedMember(named(MEMBER)),
                        )
                        .await
                        .assured("the recorded member still answers at its endpoint");
                    assert_eq!(through, named(MEMBER));
                    returning.replace_live_nodes(&live(&[MEMBER, RETURNING]));
                    assert_eq!(
                        join_outcome(&returning).await,
                        JoinProgress::Joined {
                            through: named(MEMBER),
                        },
                    );
                    trace.record(
                        RETURNING,
                        "the returning node joined through the member that still answers",
                    );
                    done.send_replace(true);
                    returning.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count
                        .checked_add(1)
                        .assured("only three fixture hosts finish");
                });
                result
            }
        });

        simulation.client("observer", async move {
            let mut finished = finished_rx;
            wait_for_count(&mut finished, 3).await;
            Ok(())
        });
    })
}
