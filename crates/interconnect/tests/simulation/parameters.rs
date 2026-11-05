//! Parameter publication and catch-up decisions over the production transport under a partition.
//!
//! Layer: test harness outside the product layer order.
//! - **Owns.** A bounded partition and repair around the current gossip encoding and adoption check.
//! - **Depends on.** The authenticated transport fixture and the production parameter vocabulary.
//! - **Must not know.** Fjall crash semantics, whole-node Raft scheduling or graph execution.

use nervix_models::{
    ClusterParameterCatchUp, ClusterParameterPublication, ClusterParameterRecord,
    ClusterParameterSeedValues, ClusterParameterValues,
};

use super::*;

#[derive(Debug, Archive, Serialize, Deserialize)]
struct PublicationRequest {
    known: String,
}

impl InterconnectRequest for PublicationRequest {
    type Response = String;
    const NAME: &'static str = "simulation_parameter_publication";
    const CLASS: PoolClass = PoolClass::Management;
    const SUBQUOTA: RequestSubquota = RequestSubquota::Discovery;
    const TIMEOUT: Duration = Duration::from_secs(1);
    const REQUIRES_LIVE_TARGET: bool = false;
}

fn record(revision: u64, seconds: u64) -> ClusterParameterRecord {
    ClusterParameterRecord {
        revision: std::num::NonZeroU64::new(revision).assured("fixture revisions are positive"),
        values: ClusterParameterValues::resolve(ClusterParameterSeedValues {
            node_unavailability_timeout: Some(Duration::from_secs(seconds)),
            state_snapshot_interval: Some(Duration::from_millis(250)),
            ..Default::default()
        })
        .assured("fixture policy satisfies its setting definitions"),
    }
}

#[test]
fn a_live_parameter_publication_gates_catch_up_across_a_partition() {
    Scenario {
        name: "a live parameter publication gates catch up across a partition",
        fault_plan: "partition the returning host before discovery, then repair; construction \
                     uses the live publication while its local applied record is behind",
        seeds: &[441],
    }
    .check(config, publication_after_partition);
}

fn publication_after_partition(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let peer_seed = seed.checked_add(1).assured("fixture seeds fit u64");
    let trace = run.trace();
    let authority = Authority::new();
    let leader_credentials = authority.issue("server");
    let returning_credentials = authority.issue("client");
    let current = record(3, 15);
    let stale = record(1, 10);
    let (ready, ready_rx) = watch::channel(false);
    let (done, done_rx) = watch::channel(false);
    let (finished, finished_rx) = watch::channel(0_usize);
    run.simulate(move |simulation| {
        let server_finished = finished.clone();
        simulation.host("server", move || {
            let credentials = leader_credentials.clone();
            let ready = ready.clone();
            let mut done = done_rx.clone();
            let finished = server_finished.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind("server", credentials, seed).await;
                    transport
                        .register_handler::<PublicationRequest, _, _>(
                            move |_, request| async move {
                                let known: ClusterParameterPublication =
                                    serde_json::from_str(&request.known)
                                        .assured("the current gossip publication decodes");
                                let publication = known
                                    .observe(current)
                                    .assured("the live revision follows the returning host")
                                    .install(current)
                                    .assured("the live node installed its committed revision");
                                serde_json::to_string(&publication)
                                    .assured("the complete publication encodes")
                            },
                        )
                        .assured("the fixture registers its handler once");
                    ready.send_replace(true);
                    wait_for(&mut done).await;
                    transport.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| *count += 1);
                result
            }
        });
        let client_finished = finished.clone();
        simulation.host("client", move || {
            let credentials = returning_credentials.clone();
            let mut ready = ready_rx.clone();
            let done = done.clone();
            let finished = client_finished.clone();
            let trace = trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let transport = bind("client", credentials, peer_seed).await;
                    wait_for(&mut ready).await;
                    register_peer(&transport, "server").await;
                    let peer = ClusterNodeName::parse("server").assured("fixture name is valid");
                    let known = serde_json::to_string(&ClusterParameterPublication::new(stale))
                        .assured("the returning host's current-shape publication encodes");
                    turmoil::partition("client", "server");
                    let result = transport
                        .request(
                            &peer,
                            PublicationRequest {
                                known: known.clone(),
                            },
                        )
                        .await;
                    assert!(matches!(&result, Err(error) if matches!(error.current_context(), RequestError::Timeout { .. } | RequestError::Transport { .. })), "partition outcome: {result:?}");
                    trace.record(
                        "client",
                        "discovery cannot adopt a policy across the partition",
                    );
                    turmoil::repair("client", "server");
                    let response = transport
                        .request(&peer, PublicationRequest { known })
                        .await
                        .assured("authenticated discovery recovers after repair");
                    let publication: ClusterParameterPublication = serde_json::from_str(&response)
                        .assured("production publication decoding validates the coherent pair");
                    let construction = publication.committed();
                    assert_eq!(construction, current);
                    assert_eq!(publication.running(), Some(current));
                    assert_eq!(
                        construction.catch_up(stale),
                        Ok(ClusterParameterCatchUp::Pending)
                    );
                    assert_eq!(
                        construction.catch_up(current),
                        Ok(ClusterParameterCatchUp::Ready)
                    );
                    assert_eq!(
                        publication
                            .observe(stale)
                            .assured("a delayed observation is ignored"),
                        publication
                    );
                    trace.record(
                        "client",
                        "live construction policy gates activation until the applied record \
                         catches up",
                    );
                    transport.shutdown().await;
                    done.send_replace(true);
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| *count += 1);
                result
            }
        });
        simulation.client("observer", async move {
            let mut finished = finished_rx;
            wait_for_count(&mut finished, 2).await;
            Ok(())
        });
    })
}
