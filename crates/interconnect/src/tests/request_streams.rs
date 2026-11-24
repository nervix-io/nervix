//! When a request holds its stream.
//!
//! Layer: test harness.
//!
//! - **Owns.** The order in which a request of a reserved kind leases its stream, encodes its
//!   payload and decodes its answer, which makes the stream the bound of the copies it holds
//!   uncharged while an encoding or decoding waits for room.
//! - **Depends on.** The authenticated interconnect test fixture and the requester's budgets.
//! - **Must not know.** Product runtime graphs or consensus behavior.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;

/// A request of a reserved kind leases its stream before it encodes its payload, and keeps the
/// stream until its answer is decoded. The payload's copy goes uncharged while the envelope's
/// encoding waits for room, and a reserved kind holds no place of the incoming queue, so only the
/// streams a peer's connection has bound those copies: no count of a kind's requests does any more.
/// Here a cancellation's payload cannot be encoded until room returns, and the request already
/// holds its management stream meanwhile.
#[nervix_primitives::test]
async fn a_request_holds_its_stream_while_its_payload_waits_for_room() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        executor_a,
        ..
    } = connected_transports().await;
    transport_b
        .register_handler::<CancellationRequest, _, _>(|_context, _request| async move {
            CancellationResponse
        })
        .expect("cancellation handler should register");
    // One answered request first, so the management connection is up before its class fills.
    assert_eq!(
        transport_a
            .request(&node_b, CancellationRequest)
            .await
            .expect("the first cancellation should succeed"),
        CancellationResponse
    );
    let management = PoolClass::Management.index();
    assert_eq!(
        transport_a.snapshot().leased_streams[management],
        0,
        "an answered request gives its stream back"
    );
    let memory = executor_a.memory(MemoryClass::Management);
    let room = memory
        .capacity_bytes
        .checked_sub(memory.reserved_bytes)
        .expect("a budget never reserves more than its capacity");
    let occupied = executor_a
        .try_reserve(MemoryClass::Management, room)
        .expect("an idle requester's management budget takes the fill");

    let requester = transport_a.clone();
    let target = node_b.clone();
    let request = nervix_primitives::task::spawn(async move {
        requester.request(&target, CancellationRequest).await
    });
    timeout(Duration::from_secs(5), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if transport_a.snapshot().leased_streams[management] == 1 {
                break;
            }
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("the request leases its stream before its payload's encoding waits for room");
    assert!(
        !request.is_finished(),
        "the payload still waits for room in the full management class"
    );

    drop(occupied);
    assert_eq!(
        timeout(Duration::from_secs(5), request)
            .await
            .expect("the request finishes once room returns")
            .expect("the request task should join")
            .expect("the cancellation should succeed once room returns"),
        CancellationResponse
    );
    assert_eq!(
        transport_a.snapshot().leased_streams[management],
        0,
        "the request gives its stream back once its answer is decoded"
    );
    transport_a.shutdown().await;
    transport_b.shutdown().await;
}
