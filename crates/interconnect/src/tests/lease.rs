//! Stream lease bookkeeping regressions.
//!
//! Layer: test harness.
//!
//! - **Owns.** The steady-state cost of leasing a stream from an established pool.
//! - **Depends on.** The authenticated interconnect test fixture and the per-thread allocation
//!   counter the test binary installs.
//! - **Must not know.** What an operation sends over the leased stream.

use super::*;
use crate::connection::StreamLease;

#[nervix_primitives::test]
async fn leasing_a_stream_from_an_established_pool_does_not_allocate() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    timeout(Duration::from_secs(5), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if transport_a.is_connected_to(&node_b) {
                break;
            }
            nervix_primitives::task::yield_now().await;
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
        nervix_primitives::task::yield_now().await;
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

/// Leases every free stream of `class` and `subquota` to `node`, without waiting.
fn lease_every_free_stream(
    transport: &Transport,
    node: &ClusterNodeName,
    class: PoolClass,
    subquota: RequestSubquota,
    deadline: Instant,
) -> Vec<StreamLease> {
    let mut leases = Vec::new();
    loop {
        let Some(leased) = transport
            .inner
            .lease(node, class, subquota, deadline)
            .now_or_never()
        else {
            return leases;
        };
        leases.push(leased.assured("a stream that is free leases before its deadline"));
    }
}

/// A released stream wakes a waiter that can use it. With every admission and terminal stream to a
/// peer leased, a waiter for a terminal stream queues before a waiter for an admission stream; the
/// admission stream released next must reach the admission waiter, not wake the terminal waiter,
/// which cannot use it, and leave the admission waiter queued until its deadline.
#[nervix_primitives::test]
async fn a_released_stream_wakes_a_waiter_its_subquota_can_serve() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    timeout(Duration::from_secs(5), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if transport_a.is_connected_to(&node_b) {
                break;
            }
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .assured("the connected test transports become ready");
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(60))
        .assured("a minute-long test deadline fits the monotonic clock");
    let mut admissions = lease_every_free_stream(
        &transport_a,
        &node_b,
        PoolClass::Management,
        RequestSubquota::Admission,
        deadline,
    );
    let terminals = lease_every_free_stream(
        &transport_a,
        &node_b,
        PoolClass::Management,
        RequestSubquota::Terminal,
        deadline,
    );
    assert!(!admissions.is_empty() && !terminals.is_empty());

    let terminal_waiter = nervix_primitives::task::spawn({
        let transport = transport_a.clone();
        let node = node_b.clone();
        async move {
            transport
                .inner
                .lease(
                    &node,
                    PoolClass::Management,
                    RequestSubquota::Terminal,
                    deadline,
                )
                .await
        }
    });
    nervix_primitives::task::yield_now().await;
    let admission_waiter = nervix_primitives::task::spawn({
        let transport = transport_a.clone();
        let node = node_b.clone();
        async move {
            transport
                .inner
                .lease(
                    &node,
                    PoolClass::Management,
                    RequestSubquota::Admission,
                    deadline,
                )
                .await
        }
    });
    nervix_primitives::task::yield_now().await;
    drop(admissions.pop());

    let Ok(joined) = timeout(Duration::from_secs(10), admission_waiter).await else {
        panic!("a released admission stream did not reach the waiter for an admission stream");
    };
    let admitted = joined.assured("the admission waiter task only leases one stream");
    assert!(
        admitted.is_ok(),
        "the admission waiter got no stream although one was released"
    );
    terminal_waiter.abort();
    drop(terminals);
    drop(admissions);
    transport_a.shutdown().await;
    transport_b.shutdown().await;
}
