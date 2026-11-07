//! Exact-process requests across a transport restart under the same authenticated node name.
//!
//! Layer: test harness.
//! - **Owns.** The leased connection's epoch fence and a replacement process's request count.
//! - **Depends on.** The production transport and its bounded simulation fixture.
//! - **Must not know.** Server clocks, graph installation or gossip.

use super::*;

#[test]
fn a_process_bound_request_cannot_reach_a_replacement_transport() {
    Scenario {
        name: "process bound request across transport replacement",
        fault_plan: "after an exact-process request succeeds, restart the receiver transport at \
                     the same address with a new process epoch, reject a request retaining the \
                     preceding epoch before its handler, then accept the current epoch",
        seeds: &[137, 139],
    }
    .check(config, process_target_across_restart);
}

fn process_target_across_restart(run: ScenarioRun) -> Result<(), SimulationError> {
    let seed = run.seed();
    let server_trace = run.trace();
    let client_trace = run.trace();
    let authority = Authority::new();
    let server_credentials = authority.issue("server");
    let client_credentials = authority.issue("client");
    let (epochs, epoch_rx) = watch::channel(Vec::<u64>::new());
    let (replace, replace_rx) = watch::channel(false);
    let (done, done_rx) = watch::channel(false);
    let (calls, calls_rx) = watch::channel(0_usize);
    let (finished, finished_rx) = watch::channel(0_usize);
    run.simulate(move |simulation| {
        let server_finished = finished.clone();
        simulation.host("server", move || {
            let credentials = server_credentials.clone();
            let epochs = epochs.clone();
            let mut replace = replace_rx.clone();
            let mut done = done_rx.clone();
            let calls = calls.clone();
            let finished = server_finished.clone();
            let trace = server_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    for incarnation in 0..2_u64 {
                        let server = bind(
                            "server",
                            credentials.clone(),
                            seed.checked_add(incarnation).assured("fixture seeds fit"),
                        )
                        .await;
                        let calls = calls.clone();
                        server
                            .register_handler::<LivenessRequest, _, _>(move |context, _| {
                                let calls = calls.clone();
                                async move {
                                    calls.send_modify(|count| {
                                        *count = count
                                            .checked_add(1)
                                            .assured("the fixture admits two requests");
                                    });
                                    LivenessResponse {
                                        peer: context.peer_node_id().clone(),
                                    }
                                }
                            })
                            .assured("each process registers its handler once");
                        let epoch = server
                            .next_coordination_identity()
                            .assured("the fixture process can allocate an identity")
                            .process_epoch();
                        server.replace_live_nodes(&BTreeSet::from([
                            server.node_id().clone(),
                            ClusterNodeName::parse("client").assured("fixture names are valid"),
                        ]));
                        epochs.send_modify(|epochs| epochs.push(epoch));
                        if incarnation == 0 {
                            wait_for(&mut replace).await;
                        } else {
                            wait_for(&mut done).await;
                        }
                        server.shutdown().await;
                        trace.record("server", "transport process ended");
                    }
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count.checked_add(1).assured("the fixture has two hosts");
                });
                result
            }
        });
        let client_finished = finished.clone();
        simulation.host("client", move || {
            let credentials = client_credentials.clone();
            let mut epochs = epoch_rx.clone();
            let replace = replace.clone();
            let done = done.clone();
            let calls = calls_rx.clone();
            let finished = client_finished.clone();
            let trace = client_trace.clone();
            async move {
                let result = HostSupervisor::run(async move {
                    let client = bind(
                        "client",
                        credentials,
                        seed.checked_add(2).assured("fixture seeds fit"),
                    )
                    .await;
                    let server = ClusterNodeName::parse("server").assured("fixture name is valid");
                    client.replace_live_nodes(&BTreeSet::from([
                        client.node_id().clone(),
                        server.clone(),
                    ]));
                    nervix_primitives::time::timeout(
                        HOST_DEADLINE,
                        epochs.wait_for(|epochs| epochs.len() == 1),
                    )
                    .await
                    .assured("the first process listens within the fixture deadline")
                    .assured("the receiver retains its epoch observations");
                    let first_epoch = epochs.borrow()[0];
                    register_peer(&client, "server").await;
                    client
                        .request_to_process(&server, first_epoch, LivenessRequest)
                        .await
                        .assured("the exact process accepts its request");
                    assert_eq!(*calls.borrow(), 1);
                    replace.send_replace(true);
                    nervix_primitives::time::timeout(
                        HOST_DEADLINE,
                        epochs.wait_for(|epochs| epochs.len() == 2),
                    )
                    .await
                    .assured("the replacement listens within the fixture deadline")
                    .assured("the receiver retains its epoch observations");
                    let current_epoch = epochs.borrow()[1];
                    assert_ne!(first_epoch, current_epoch);
                    client.replace_outbound_targets(&Default::default());
                    register_peer(&client, "server").await;
                    let error = match client
                        .request_to_process(&server, first_epoch, LivenessRequest)
                        .await
                    {
                        Err(error) => error,
                        Ok(_) => panic!("a preceding-process request reached the replacement"),
                    };
                    assert!(matches!(
                        error.current_context(),
                        RequestError::PeerProcessChanged { expected, actual, .. }
                            if *expected == first_epoch && *actual == current_epoch
                    ));
                    assert_eq!(*calls.borrow(), 1, "a refused request reaches no handler");
                    trace.record(
                        "client",
                        "preceding process request rejected before sending",
                    );
                    client
                        .request_to_process(&server, current_epoch, LivenessRequest)
                        .await
                        .assured("the replacement accepts a request for its own epoch");
                    assert_eq!(*calls.borrow(), 2);
                    trace.record("client", "replacement process request accepted");
                    done.send_replace(true);
                    client.shutdown().await;
                    Ok::<(), io::Error>(())
                })
                .await;
                finished.send_modify(|count| {
                    *count = count.checked_add(1).assured("the fixture has two hosts");
                });
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
