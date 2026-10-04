//! Stream lease bookkeeping regressions.
//!
//! Layer: test harness.
//!
//! - **Owns.** The steady-state cost of leasing a stream from an established pool.
//! - **Depends on.** The authenticated interconnect test fixture and the per-thread allocation
//!   counter the test binary installs.
//! - **Must not know.** What an operation sends over the leased stream.

use super::*;

// This real-network fixture enters the ordinary runtime from external contender threads.
// Production slot ownership has its separately registered Shuttle check.
#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test]
async fn concurrent_peer_registration_preserves_the_publication_limit() {
    let options = TransportOptions {
        max_peers: 1,
        ..TransportOptions::default()
    };
    let ConnectedTransports {
        transport_a,
        transport_b,
        ..
    } = bound_transports_with_options(options).await;
    let barrier =
        nervix_primitives::sync::Arc::new(nervix_primitives::sync::blocking::Barrier::new(3));
    let runtime = nervix_primitives::runtime::Handle::current();
    let mut contenders = Vec::new();
    for name in ["peer-left", "peer-right"] {
        let transport = transport_a.clone();
        let barrier = barrier.clone();
        let runtime = runtime.clone();
        let endpoint = NodeEndpoint::new("localhost", transport_b.local_addr().port());
        contenders.push(nervix_primitives::thread::spawn(move || {
            let _entered = runtime.enter();
            barrier.wait();
            transport.register_outbound_target(
                ClusterNodeName::parse(name).expect("valid contender identity"),
                endpoint,
            )
        }));
    }
    barrier.wait();
    let results = contenders
        .into_iter()
        .map(|contender| contender.join().expect("registration contender finishes"))
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    for error in results.into_iter().filter_map(Result::err) {
        assert!(matches!(
            error.current_context(),
            TransportError::PoolExhausted
        ));
    }
    transport_a.shutdown().await;
    transport_b.shutdown().await;
}

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
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(30))
            .expect("fixed benchmark duration fits the monotonic clock");
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
    transport_a.shutdown().await;
    transport_b.shutdown().await;
}
