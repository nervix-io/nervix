//! Authenticated typed-request pool coverage.
//!
//! Layer: test harness.
//!
//! - **Owns.** The authenticated typed-request reuse scenario.
//! - **Depends on.** The interconnect test fixture and typed test messages.
//! - **Must not know.** Runtime graph construction or control-plane scheduling.

use super::*;

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
