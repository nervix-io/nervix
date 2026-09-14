//! Coordination identity allocation and authenticated process binding regressions.
//!
//! Layer: test harness.
//!
//! - **Owns.** Cross-process uniqueness and sender-binding regression scenarios.
//! - **Depends on.** The authenticated interconnect test fixture and entity-gate wire requests.
//! - **Must not know.** Runtime graph construction or control-plane scheduling.

use super::*;

#[tokio::test]
async fn coordination_identity_is_unique_and_bound_to_the_authenticated_process() {
    let ConnectedTransports {
        _authority: authority,
        transport_a,
        transport_b,
        node_a,
        node_b,
        ..
    } = connected_transports().await;
    let handled = StdArc::new(AtomicUsize::new(0));
    transport_b
        .register_handler::<EntityGateReleaseRequest, _, _>({
            let handled = handled.clone();
            move |_context, _request| {
                let handled = handled.clone();
                async move {
                    handled.fetch_add(1, Ordering::SeqCst);
                    EntityGateReleaseResponse { result: Ok(()) }
                }
            }
        })
        .expect("entity gate release handler should register");

    let current = transport_a
        .next_coordination_identity()
        .expect("the first coordinator should allocate an identity");
    let concurrent = transport_b
        .next_coordination_identity()
        .expect("the second coordinator should allocate an identity");
    assert_eq!(current.sequence(), 1);
    assert_eq!(current.sequence(), concurrent.sequence());
    assert_ne!(current, concurrent);
    let domain = DomainName::parse("default").expect("test domain should be valid");
    transport_a
        .request(
            &node_b,
            EntityGateReleaseRequest {
                coordination: current.clone(),
                domain: domain.clone(),
            },
        )
        .await
        .expect("the identity issued by this process should authenticate");

    let prior_incarnation = CoordinationIdentity::new(
        node_a.clone(),
        current.process_epoch() ^ 1,
        current.sequence(),
    );
    let error = transport_a
        .request(
            &node_b,
            EntityGateReleaseRequest {
                coordination: prior_incarnation,
                domain: domain.clone(),
            },
        )
        .await
        .expect_err("a prior process incarnation must be rejected");
    assert!(
        error
            .to_string()
            .contains("coordination identity does not belong to the authenticated request sender")
    );
    assert_eq!(handled.load(Ordering::SeqCst), 1);

    transport_a.shutdown().await;
    let (replacement_a, _replacement_incoming) = Transport::bind(
        "127.0.0.1:0".parse().expect("test address should be valid"),
        "localhost",
        "test-cluster",
        node_a.clone(),
        authority.issue("test-cluster", &node_a),
        TransportOptions::default(),
        Executor::default(),
    )
    .await
    .expect("replacement coordinator transport should bind");
    replacement_a.replace_live_nodes(&BTreeSet::from([node_a.clone(), node_b.clone()]));
    replacement_a
        .register_outbound_target(
            node_b.clone(),
            PeerTarget::new(transport_b.local_addr(), "localhost"),
        )
        .expect("replacement coordinator target should register");
    let replacement = replacement_a
        .next_coordination_identity()
        .expect("the replacement process should allocate an identity");
    assert_eq!(replacement.sequence(), current.sequence());
    assert_ne!(replacement, current);
    replacement_a
        .request(
            &node_b,
            EntityGateReleaseRequest {
                coordination: replacement,
                domain: domain.clone(),
            },
        )
        .await
        .expect("the replacement process identity should authenticate");
    let error = replacement_a
        .request(
            &node_b,
            EntityGateReleaseRequest {
                coordination: current,
                domain,
            },
        )
        .await
        .expect_err("the replacement process must not replay its prior incarnation identity");
    assert!(
        error
            .to_string()
            .contains("coordination identity does not belong to the authenticated request sender")
    );
    assert_eq!(handled.load(Ordering::SeqCst), 2);

    replacement_a.shutdown().await;
    transport_b.shutdown().await;
}

#[tokio::test]
async fn typed_rkyv_requests_reuse_an_authenticated_http2_pool() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_a,
        node_b,
        ..
    } = connected_transports().await;
    transport_b
        .register_handler::<EchoRequest, _, _>(|context, request| async move {
            EchoResponse {
                value: request.value,
                peer: context.peer_node_id().clone(),
                advertised_host: context.peer_advertised_host().to_string(),
            }
        })
        .expect("echo handler should register");

    timeout(Duration::from_secs(5), async {
        loop {
            tokio::task::consume_budget().await;
            if transport_a.is_connected_to(&node_b) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the target should become ready");
    assert_eq!(
        transport_a.active_outbound_connections().await,
        5,
        "readiness must include management, command, replication, and both relay connections"
    );

    for value in ["first", "second"] {
        let response = transport_a
            .request(
                &node_b,
                EchoRequest {
                    value: value.to_string(),
                },
            )
            .await
            .expect("typed request should cross the interconnect");
        assert_eq!(
            response,
            EchoResponse {
                value: value.to_string(),
                peer: node_a.clone(),
                advertised_host: "localhost".to_string(),
            }
        );
    }
    assert_eq!(transport_a.active_outbound_connections().await, 5);

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}
