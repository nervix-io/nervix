//! Stream lease bookkeeping regressions.
//!
//! Layer: test harness.
//!
//! - **Owns.** The steady-state cost of leasing a stream from an established pool.
//! - **Depends on.** The authenticated interconnect test fixture and the per-thread allocation
//!   counter the test binary installs.
//! - **Must not know.** What an operation sends over the leased stream.

use super::*;

#[tokio::test]
async fn leasing_a_stream_from_an_established_pool_does_not_allocate() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
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
    .assured("the connected test transports become ready");
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(5))
        .assured("a five-second test deadline fits the monotonic clock");

    let leases = [
        (PoolClass::Relay, RequestSubquota::Shared),
        (PoolClass::Management, RequestSubquota::Admission),
        (PoolClass::Management, RequestSubquota::Terminal),
    ];
    for (class, subquota) in leases {
        // The lease is polled exactly once, so no other task runs on this thread while the counter
        // is reading. Yielding first gives that poll a fresh scheduling budget to complete in.
        tokio::task::yield_now().await;
        let (allocations, polled) = alloc_count::alloc_count!({
            transport_a
                .inner
                .lease(&node_b, class, subquota, deadline)
                .now_or_never()
        });
        let lease = polled
            .assured("every connection of an established pool has free stream slots")
            .assured("an established pool leases a stream before its deadline");
        drop(lease);
        assert_eq!(
            (allocations.alloc_calls, allocations.realloc_calls),
            (0, 0),
            "leasing a {class:?} stream from an established pool must not allocate"
        );
    }

    transport_a.shutdown().await;
    transport_b.shutdown().await;
}
