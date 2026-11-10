//! Declared transport windows and deadlines under delayed, held and lost traffic.
//!
//! Layer: test harness.
//! - **Owns.** A bounded known-seed exchange through configured production transport owners.
//! - **Depends on.** The shared simulation fixture and typed request API.
//! - **Must not know.** Application graphs, storage or consensus.
use super::*;

#[derive(Debug, Archive, Serialize, Deserialize)]
struct WindowProbe {
    payload: Vec<u8>,
}
impl InterconnectRequest for WindowProbe {
    type Response = Vec<u8>;
    const NAME: &'static str = "simulation_configured_windows";
    const DEADLINE: nervix_interconnect::RequestDeadline =
        nervix_interconnect::RequestDeadline::Request;
}

fn options() -> TransportOptions {
    nervix_interconnect::TransportSettings::resolve(nervix_interconnect::TransportSettingValues {
        max_peers: Some(16),
        request_quota: Some(4),
        stream_receive_window: Some(ubyte::ByteUnit::Kibibyte(4)),
        connection_receive_window: Some(ubyte::ByteUnit::Kibibyte(16)),
        request_timeout: Some(Duration::from_secs(2)),
        ..Default::default()
    })
    .assured("the declared bounds hold every reserved traffic class")
    .options(None, None)
}

#[test]
fn declared_windows_and_rpc_deadlines_survive_latency_loss_and_repair() {
    Scenario {
        name: "declared windows and RPC deadlines",
        fault_plan: "exchange a body larger than both windows over delayed TCP, hold every byte through the configured deadline, then partition and repair the link",
        seeds: &[141],
    }.check(|seed| {
        let mut config = config(seed);
        config.network.min_message_latency = Duration::from_millis(5);
        config.network.max_message_latency = Duration::from_millis(10);
        config
    }, |run| {
        let seed = run.seed();
        let authority = Authority::new();
        let server_credentials = authority.issue("server");
        let client_credentials = authority.issue("client");
        let (ready_tx, ready_rx) = watch::channel(false);
        let (done_tx, done_rx) = watch::channel(false);
        let (finished_tx, finished_rx) = watch::channel(0usize);
        let trace = run.trace();
        run.simulate(move |simulation| {
            let server_finished = finished_tx.clone();
            simulation.host("server", move || {
                let credentials = server_credentials.clone();
                let ready = ready_tx.clone();
                let mut done = done_rx.clone();
                let finished = server_finished.clone();
                async move {
                    let result = HostSupervisor::run(async move {
                        let (server, _incoming) = bind_with_incoming_options("server", credentials, seed, options()).await;
                        server.register_handler::<WindowProbe, _, _>(|_, request| async move { request.payload }).assured("one fixture handler");
                        ready.send_replace(true);
                        wait_for(&mut done).await;
                        server.shutdown().await;
                        assert_stopped(&server).await;
                        Ok::<(), io::Error>(())
                    }).await;
                    finished.send_modify(|count| *count += 1);
                    result
                }
            });
            let client_finished = finished_tx.clone();
            simulation.host("client", move || {
                let credentials = client_credentials.clone();
                let mut ready = ready_rx.clone();
                let done = done_tx.clone();
                let finished = client_finished.clone();
                let trace = trace.clone();
                async move {
                    let result = HostSupervisor::run(async move {
                        wait_for(&mut ready).await;
                        let (client, _incoming) = bind_with_incoming_options("client", credentials, seed + 1, options()).await;
                        register_peer(&client, "server").await;
                        let peer = ClusterNodeName::parse("server").assured("fixture name");
                        let payload = vec![7u8; 128 * 1024];
                        let response = client.request(&peer, WindowProbe { payload: payload.clone() }).await.assured("h2 replenishes the peer's smaller windows");
                        assert_eq!(response, payload);
                        trace.record("client", "128 KiB crossed 4 KiB stream and 16 KiB connection windows");
                        turmoil::hold("client", "server");
                        let started = nervix_primitives::time::Instant::now();
                        let failed = client.request(&peer, WindowProbe { payload: vec![9] }).await.err().assured("held traffic reaches its configured deadline");
                        assert!(matches!(failed.current_context(), RequestError::Timeout { timeout, .. } if *timeout == Duration::from_secs(2)));
                        assert!(started.elapsed() >= Duration::from_secs(2) && started.elapsed() < Duration::from_secs(3));
                        trace.record("client", "held RPC ended at the declared two-second deadline");
                        turmoil::release("client", "server");
                        turmoil::partition("client", "server");
                        let failed = client.request(&peer, WindowProbe { payload: vec![10] }).await;
                        assert!(failed.is_err(), "lost traffic cannot produce a successful reply");
                        turmoil::repair("client", "server");
                        let repaired = nervix_primitives::time::timeout(HOST_DEADLINE, async {
                            loop {
                                if let Ok(response) = client.request(&peer, WindowProbe { payload: vec![11] }).await { break response; }
                                nervix_primitives::time::sleep(Duration::from_millis(10)).await;
                            }
                        }).await.assured("the repaired peer answers within the host bound");
                        assert_eq!(repaired, vec![11]);
                        trace.record("client", "declared-capacity transport recovered after packet loss");
                        done.send_replace(true);
                        client.shutdown().await;
                        assert_stopped(&client).await;
                        Ok::<(), io::Error>(())
                    }).await;
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
    });
}
