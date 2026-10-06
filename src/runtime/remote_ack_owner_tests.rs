//! Remote correlation lifetime and exact-generation properties over production owners.
//!
//! Layer: test harness.
//! - **Owns.** Bounded admission, independent record outcomes and stale-generation assertions.
//! - **Depends on.** Remote correlation owners and current acknowledgement trees.
//! - **Must not know.** Serialized live ACKs, connectors or persistent state.

use futures_util::FutureExt as _;

use super::*;

fn receiver() -> ClusterNodeName {
    ClusterNodeName::parse("node-2").assured("the fixture node is valid")
}

#[test]
#[ignore = "native delivery cost probe run by bench-remote-owners"]
fn remote_ack_owner_cost() {
    use nervix_primitives::time::Instant;
    use sorted_vec::SortedVec;

    let owners = RemoteDispatchRegistry::new(Executor::default());
    for rows in [1, 64] {
        for sample in 0..5 {
            let mut timings = Vec::with_capacity(1_000);
            let mut peak_bytes = 0;
            let start = Instant::now();
            for _ in 0..1_000 {
                let delivery_start = Instant::now();
                let (acks, completions): (Vec<_>, Vec<_>) =
                    (0..rows).map(|_| AckSet::root()).unzip();
                let registrations = owners
                    .register_acks(receiver(), acks)
                    .assured("completed deliveries return their routing positions");
                peak_bytes = peak_bytes.max(owners.executor.snapshot().relay_memory.reserved_bytes);
                for id in registrations.into_iter().flatten() {
                    owners.admit_ack(id);
                    assert!(owners.report_ack(id));
                    assert!(owners.progress_ack(id, 1, true));
                    assert!(owners.progress_ack(id, 2, false));
                    assert!(owners.resolve_ack(id, AckOutcome::Ack));
                }
                for completion in completions {
                    assert_eq!(completion.wait().now_or_never(), Some(AckOutcome::Ack));
                }
                assert_eq!(owners.executor.snapshot().relay_memory.reserved_bytes, 0);
                timings.push(delivery_start.elapsed().as_nanos());
            }
            let elapsed = start.elapsed();
            let timings = SortedVec::from_unsorted(timings);
            println!(
                "remote-ack-owner rows={rows} sample={sample} deliveries={} ns_per_ack={} \
                 p50_delivery_ns={} p95_delivery_ns={} p99_delivery_ns={} peak_relay_bytes={} \
                 retained_relay_bytes=0",
                timings.len(),
                elapsed.as_nanos()
                    / u128::try_from(rows * timings.len()).assured("bounded sample size"),
                timings[timings.len() / 2],
                timings[timings.len() * 95 / 100],
                timings[timings.len() * 99 / 100],
                peak_bytes,
            );
        }
    }
    let start = Instant::now();
    for _ in 0..100 {
        assert!(owners.fail_silent_acks().is_empty());
    }
    println!(
        "remote-ack-owner positions={} empty_sweep_ns={}",
        owners.slots.len(),
        start.elapsed().as_nanos() / 100
    );
}

#[test]
fn a_wide_delivery_uses_row_positions_and_refusal_returns_its_memory() {
    let owners = RemoteDispatchRegistry::with_capacity(1);
    let capacity = owners.executor.snapshot().relay_memory.capacity_bytes;
    let occupied = owners
        .executor
        .try_reserve(MemoryClass::Relay, capacity)
        .assured("the fixture reserves exactly its relay budget");
    let (first, completion) = AckSet::root();
    let refused = owners
        .register_acks(receiver(), vec![first.clone()])
        .expect_err("charged record state cannot exceed the relay budget");
    assert_eq!(
        *refused.current_context(),
        RemoteDispatchError::CorrelationMemory
    );
    assert_eq!(owners.deliveries.len(), 1);
    drop(occupied);

    let (last, last_completion) = AckSet::root();
    let mut rows = vec![AckSet::empty(); 9000];
    rows[0] = first;
    rows[8999] = last;
    let ids = owners
        .register_acks(receiver(), rows)
        .assured("one delivery can carry more rows than there are delivery positions");
    let first = ids[0].assured("the first row has an acknowledgement");
    let last = ids[8999].assured("the final row has an acknowledgement");
    assert_eq!(last.checked_sub(first), Some(8999));
    assert!(!owners.report_ack(first + 1));
    assert!(!owners.resolve_ack(first + 9000, AckOutcome::Ack));
    let (refused, refused_completion) = AckSet::root();
    assert!(owners.register_ack(receiver(), refused).is_err());
    drop(refused_completion);
    assert!(owners.resolve_ack(last, AckOutcome::Ack));
    assert!(owners.executor.snapshot().relay_memory.reserved_bytes > 0);
    assert!(owners.resolve_ack(first, AckOutcome::Ack));
    assert_eq!(completion.wait().now_or_never(), Some(AckOutcome::Ack));
    assert_eq!(last_completion.wait().now_or_never(), Some(AckOutcome::Ack));
    assert_eq!(owners.executor.snapshot().relay_memory.reserved_bytes, 0);
    assert_eq!(owners.deliveries.len(), 1);
    assert_eq!(
        owners
            .register_acks(receiver(), vec![AckSet::empty(), AckSet::empty()])
            .assured("detached rows allocate no owner"),
        vec![None, None]
    );
}

#[test]
fn a_registration_kind_and_ordered_progress_are_validated_by_the_exact_owner() {
    let owners = RemoteDispatchRegistry::with_capacity(1);
    let (admission, updates) = owners
        .register_admission()
        .assured("the fixture has admission room");
    assert!(!owners.progress_ack(admission + 1, 1, true));
    assert!(!owners.resolve_ack(admission + 1, AckOutcome::Ack));
    assert!(owners.progress_ack(admission, 1, true));
    assert!(matches!(*updates.borrow(), RelayAdmissionUpdate::Alive));
    assert!(owners.resolve_ack(admission, AckOutcome::NoAck("refused".to_string())));
    assert!(
        matches!(&*updates.borrow(), RelayAdmissionUpdate::Rejected(reason) if reason == "refused")
    );
    assert!(!owners.report_ack(admission));

    let tracker = Arc::new(crate::runtime_ack::AckRootTracker::default());
    let (acks, completion) = AckSet::tracked_root(tracker.clone());
    let id = owners
        .register_ack(receiver(), acks)
        .assured("the fixture delivery fits");
    assert!(owners.progress_ack(id, 2, true));
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    assert!(owners.progress_ack(id, 1, false));
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    assert!(owners.progress_ack(id, 3, false));
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 1);
    owners.admit_ack(id);
    assert!(owners.resolve_ack(id, AckOutcome::Ack));
    assert_eq!(tracker.outstanding_for_ownership_handoff(), 0);
    assert_eq!(completion.wait().now_or_never(), Some(AckOutcome::Ack));
}

#[test]
fn record_outcomes_are_independent_and_the_delivery_releases_its_memory_last() {
    let owners = RemoteDispatchRegistry::with_capacity(1);
    let (first, first_completion) = AckSet::root();
    let (second, second_completion) = AckSet::root();
    let ids = owners
        .register_acks(receiver(), vec![first, AckSet::empty(), second])
        .assured("the fixture delivery fits");
    let first = ids[0].assured("the first row carries a record acknowledgement");
    let second = ids[2].assured("the third row carries a record acknowledgement");
    assert_eq!(ids[1], None);
    assert!(owners.executor.snapshot().relay_memory.reserved_bytes > 0);
    let (admission, updates) = owners
        .register_admission()
        .assured("record capacity leaves independent admission room");
    assert!(owners.resolve_ack(first, AckOutcome::Ack));
    assert_eq!(
        first_completion.wait().now_or_never(),
        Some(AckOutcome::Ack)
    );
    let mut second_wait = std::pin::pin!(second_completion.wait());
    assert!(second_wait.as_mut().now_or_never().is_none());
    assert!(!owners.resolve_ack(first, AckOutcome::NoAck("duplicate".to_string())));
    assert!(owners.holds_ack(second));
    assert!(owners.resolve_ack(second, AckOutcome::NoAck("sink refused".to_string())));
    assert_eq!(
        second_wait.as_mut().now_or_never(),
        Some(AckOutcome::NoAck("sink refused".to_string()))
    );
    assert_eq!(owners.executor.snapshot().relay_memory.reserved_bytes, 0);
    assert!(owners.resolve_ack(admission, AckOutcome::Ack));
    assert!(matches!(*updates.borrow(), RelayAdmissionUpdate::Admitted));
}

#[test]
fn ended_admissions_and_unadmitted_deliveries_return_their_capacity() {
    let owners = RemoteDispatchRegistry::with_capacity(1);
    let (admission, updates) = owners
        .register_admission()
        .assured("the fixture has admission room");
    drop(updates);
    assert!(owners.report_ack(admission));
    assert!(!owners.holds_ack(admission));
    let (_current, updates) = owners
        .register_admission()
        .assured("the departed waiter returned its position");
    let (acks, completion) = AckSet::root();
    let id = owners
        .register_ack(receiver(), acks)
        .assured("the fixture has delivery room");
    for _ in 0..ADMISSION_SWEEPS {
        assert!(owners.fail_silent_acks().is_empty());
    }
    assert_eq!(owners.fail_silent_acks().get(&receiver()), Some(&1));
    assert!(
        matches!(completion.wait().now_or_never(), Some(AckOutcome::NoAck(reason)) if reason.contains("did not admit"))
    );
    assert!(!owners.holds_ack(id));
    owners.shutdown();
    assert!(matches!(
        *updates.borrow(),
        RelayAdmissionUpdate::Rejected(_)
    ));
    assert!(owners.register_admission().is_err());
    assert_eq!(owners.executor.snapshot().relay_memory.reserved_bytes, 0);
}

#[test]
fn shutdown_resolves_live_records_and_exhaustion_seals_a_generation() {
    let owners = RemoteDispatchRegistry::with_capacity(1);
    let (acks, completion) = AckSet::root();
    owners
        .register_ack(receiver(), acks)
        .assured("the fixture has delivery room");
    owners.shutdown();
    assert_eq!(
        completion.wait().now_or_never(),
        Some(AckOutcome::NoAck(
            "remote correlation owner ended".to_string()
        ))
    );
    let exhausted = RemoteDispatchRegistry::with_capacity(1);
    *exhausted.slots[0].lock() = CorrelationSlot::Open {
        generation: u64::MAX,
        correlation: None,
    };
    let (acks, completion) = AckSet::root();
    let error = exhausted
        .register_ack(receiver(), acks)
        .expect_err("exhausted identities are refused");
    assert_eq!(
        *error.current_context(),
        RemoteDispatchError::CorrelationIdentityExhausted
    );
    assert!(matches!(
        *exhausted.slots[0].lock(),
        CorrelationSlot::Closed
    ));
    drop(completion);
}

#[test]
fn bolero_remote_correlations_preserve_capacity_and_exact_generation_outcomes() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let owners = RemoteDispatchRegistry::with_capacity(2);
            let mut live = BTreeMap::new();
            let mut retired = Vec::new();
            for byte in bytes {
                match byte % 5 {
                    0 => {
                        let (acks, completion) = AckSet::root();
                        match owners.register_ack(receiver(), acks) {
                            Ok(id) => {
                                assert!(live.insert(id, completion).is_none());
                            }
                            Err(error) => assert!(
                                live.len() == 2
                                    && matches!(
                                        error.current_context(),
                                        RemoteDispatchError::CorrelationCapacity { capacity: 2 }
                                    )
                            ),
                        }
                    }
                    1 | 2 => {
                        if let Some(id) = live
                            .keys()
                            .nth(usize::from(byte / 5) % live.len().max(1))
                            .copied()
                        {
                            let completion = live
                                .remove(&id)
                                .verified("the chosen live record is present");
                            let outcome = if byte % 5 == 1 {
                                AckOutcome::Ack
                            } else {
                                AckOutcome::NoAck("refused".to_string())
                            };
                            assert!(owners.resolve_ack(id, outcome.clone()));
                            assert_eq!(completion.wait().now_or_never(), Some(outcome));
                            retired.push(id);
                        }
                    }
                    3 => {
                        for id in live.keys() {
                            assert!(owners.report_ack(*id));
                        }
                    }
                    _ => {
                        for id in &retired {
                            assert!(!owners.report_ack(*id));
                            assert!(!owners.resolve_ack(*id, AckOutcome::Ack));
                        }
                    }
                }
                assert!(live.len() <= 2);
                for id in live.keys() {
                    assert!(owners.holds_ack(*id));
                }
            }
            owners.shutdown();
            for (id, completion) in live {
                assert!(!owners.holds_ack(id));
                assert_eq!(
                    completion.wait().now_or_never(),
                    Some(AckOutcome::NoAck(
                        "remote correlation owner ended".to_string()
                    ))
                );
            }
            assert_eq!(owners.executor.snapshot().relay_memory.reserved_bytes, 0);
        });
}
