//! Stream lease bookkeeping regressions.
//!
//! Layer: test harness.
//!
//! - **Owns.** The steady-state cost of leasing a stream from an established pool.
//! - **Depends on.** The authenticated interconnect test fixture and the per-thread allocation
//!   counter the test binary installs.
//! - **Must not know.** What an operation sends over the leased stream.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

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
    let deadline = Some(crate::ExchangeDeadline::Caller(
        Instant::now()
            .checked_add(Duration::from_secs(5))
            .assured("a five-second test deadline fits the monotonic clock"),
    ));

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

    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

/// Leases every free stream of `class` and `subquota` to `node`, without waiting.
fn lease_every_free_stream(
    transport: &Transport,
    node: &ClusterNodeName,
    class: PoolClass,
    subquota: RequestSubquota,
    deadline: Option<crate::ExchangeDeadline>,
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
    let deadline = Some(crate::ExchangeDeadline::Caller(
        Instant::now()
            .checked_add(Duration::from_secs(60))
            .assured("a minute-long test deadline fits the monotonic clock"),
    ));
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
    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}

#[nervix_primitives::test]
#[ignore = "same-host established-lease throughput and tail latency measurement"]
async fn established_pool_cost() {
    let ConnectedTransports {
        transport_a,
        transport_b,
        node_b,
        ..
    } = connected_transports().await;
    timeout(Duration::from_secs(5), async {
        while !transport_a.is_connected_to(&node_b) {
            nervix_primitives::task::yield_now().await;
        }
    })
    .await
    .expect("fixture pools become ready");
    for (class, quota) in [
        (PoolClass::Relay, RequestSubquota::Shared),
        (PoolClass::Management, RequestSubquota::Admission),
        (PoolClass::Management, RequestSubquota::Terminal),
    ] {
        let mut timings = Vec::with_capacity(10_000);
        let deadline = Some(crate::ExchangeDeadline::Caller(
            Instant::now()
                .checked_add(Duration::from_secs(30))
                .expect("fixed benchmark duration fits the monotonic clock"),
        ));
        let mut allocation_calls = 0;
        let mut reallocation_calls = 0;
        for _ in 0..10_000 {
            nervix_primitives::task::yield_now().await;
            let start = Instant::now();
            let (allocations, polled) = alloc_count::alloc_count!({
                transport_a
                    .inner
                    .lease(&node_b, class, quota, deadline)
                    .now_or_never()
            });
            let lease = polled
                .expect("established lease is immediately ready")
                .expect("free pool slot");
            std::hint::black_box(lease);
            allocation_calls += allocations.alloc_calls;
            reallocation_calls += allocations.realloc_calls;
            timings.push(start.elapsed().as_nanos());
        }
        let elapsed: u128 = timings.iter().sum();
        timings.sort_unstable();
        println!(
            "established-pool class={class:?} quota={quota:?} operations=10000 ns_per_lease={} \
             p50_ns={} p95_ns={} p99_ns={} allocations={} reallocations={}",
            elapsed / 10_000,
            timings[5000],
            timings[9500],
            timings[9900],
            allocation_calls,
            reallocation_calls
        );
        assert_eq!((allocation_calls, reallocation_calls), (0, 0));
    }
    transport_a.shutdown(crate::test_teardown_ceiling()).await;
    transport_b.shutdown(crate::test_teardown_ceiling()).await;
}
