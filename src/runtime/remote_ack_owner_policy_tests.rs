//! Layer: test harness.
//! - **Owns.** Captured delivery policy and skipped late-sweep evidence.
//! - **Depends on.** The production correlation owner and acknowledgement roots.
//! - **Must not know.** Transport, persisted records or graph placement.

use super::*;
use crate::runtime::remote_dispatch::REMOTE_RELAY_TOTAL_TIMEOUT;

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test(start_paused = true)]
async fn registrations_before_the_next_deadline_do_not_repeat_the_registry_scan() {
    use std::{
        cell::Cell,
        future::Future as _,
        task::{Context, Waker},
    };

    let owners = RemoteDispatchRegistry::with_capacity(2);
    let receiver = ClusterNodeName::parse("receiver").assured("valid fixture name");
    let (first, _first_completion) = AckSet::root();
    owners
        .register_ack(receiver.clone(), first)
        .assured("first free position");
    let scans = Cell::new(0_usize);
    let membership = TestMembership::default();
    let driver = PendingAckSweeps::watching(&owners.changed, &owners.next_sweep).run(|| {
        scans.set(scans.get() + 1);
        owners.sweep_acks(Some(Instant::now()), &membership).1
    });
    let mut driver = std::pin::pin!(driver);
    let mut context = Context::from_waker(Waker::noop());
    // Drive the real owner without advancing its paused clock. Its cooperative yields and
    // any already registered notification may complete, but no sweep deadline is due.
    for _ in 0..4 {
        assert!(driver.as_mut().poll(&mut context).is_pending());
    }
    let before = scans.get();
    let (second, _second_completion) = AckSet::root();
    let second = owners
        .register_ack(receiver.clone(), second)
        .assured("second free position");
    for _ in 0..4 {
        assert!(driver.as_mut().poll(&mut context).is_pending());
    }
    assert_eq!(
        scans.get(),
        before,
        "an arrival before the selected deadline must not revisit every correlation position"
    );

    assert!(owners.resolve_ack(second, AckOutcome::Ack));
    let mut faster = owners.timing();
    faster.remote_ack_sweep_interval = std::time::Duration::from_millis(1);
    faster
        .validate()
        .assured("a shorter sweep remains coherent");
    owners.install_timing(faster);
    let (arriving, _arriving_completion) = AckSet::root();
    owners
        .register_ack(receiver, arriving)
        .assured("the returned position is free");
    for _ in 0..4 {
        assert!(driver.as_mut().poll(&mut context).is_pending());
    }
    assert_eq!(
        scans.get(),
        before,
        "the earlier deadline is still in the future"
    );
    nervix_primitives::time::advance(std::time::Duration::from_millis(1)).await;
    nervix_primitives::task::yield_now().await;
    for _ in 0..4 {
        assert!(driver.as_mut().poll(&mut context).is_pending());
    }
    assert_eq!(
        scans.get(),
        before + 1,
        "the shorter cohort wakes its due sweep"
    );
}

#[cfg(not(feature = "shuttle"))]
#[test]
fn deliveries_sharing_an_interval_keep_one_registry_scan_per_tick() {
    let owners = RemoteDispatchRegistry::with_capacity(2);
    let receiver = ClusterNodeName::parse("receiver").assured("valid fixture name");
    let start = Instant::now();
    let interval = owners.timing().remote_ack_sweep_interval;
    let (first, _first_completion) = AckSet::root();
    owners
        .register_ack_with_clock(receiver.clone(), first, || start)
        .assured("first free position");
    let first_tick = owners.next_sweep.lock().assured("first sweep is armed");
    let (second, _second_completion) = AckSet::root();
    let second_arrival = start + first_tick.duration_since(start) / 2;
    owners
        .register_ack_with_clock(receiver, second, || second_arrival)
        .assured("second free position");

    let (_, next) = owners.sweep_acks(Some(first_tick), &TestMembership::default());
    assert_eq!(next, Some(first_tick + interval));
    owners.shutdown();
}

#[cfg(feature = "shuttle")]
pub(in crate::runtime) fn assert_announced_deadline_covers_retained_records(
    owners: &RemoteDispatchRegistry,
) {
    // This independent read visits all retained records after the competing operations join.
    // The production registration path maintains its wake deadline without this scan.
    let expected = owners
        .swept_positions()
        .filter_map(|position| {
            let mut slot = owners.slots[position].lock();
            match &mut *slot {
                CorrelationSlot::Open {
                    correlation: Some(correlation),
                    ..
                } => match &mut correlation.state {
                    RemoteCorrelationState::Records(records) => Some(*records.rows.policy().1),
                    RemoteCorrelationState::Admission(_) => None,
                },
                _ => None,
            }
        })
        .min();
    assert!(
        expected.is_some(),
        "the check retains both record deliveries"
    );
    assert_eq!(
        *owners.next_sweep.lock(),
        expected,
        "a racing scan cannot lose an earlier arrival"
    );
}

pub(in crate::runtime) fn policy_epochs() {
    let owners = RemoteDispatchRegistry::with_capacity(2);
    let membership = TestMembership::default();
    let start = Instant::now();
    let receiver = ClusterNodeName::parse("receiver").assured("valid fixture name");
    let (prior, _prior_completion) = AckSet::root();
    let prior = owners
        .register_ack_with_clock(receiver.clone(), prior, || start)
        .assured("free slot");
    let timing = RemoteAckTiming {
        remote_ack_alive_interval: std::time::Duration::from_millis(1),
        remote_ack_sweep_interval: std::time::Duration::from_millis(1),
        remote_relay_total_timeout: std::time::Duration::from_millis(50),
    };
    timing.validate().assured("coherent fixture policy");
    owners.install_timing(timing);
    let (current, _current_completion) = AckSet::root();
    let current = owners
        .register_ack_with_clock(receiver, current, || start)
        .assured("free slot");
    // Neither delivery reports its admission: each is given up at the admission bound it captured.
    for tick in 1..=51 {
        owners.sweep_acks(
            Some(start + std::time::Duration::from_millis(tick)),
            &membership,
        );
    }
    assert!(!owners.holds_ack(current));
    assert!(
        owners.holds_ack(prior),
        "a new policy cannot shorten a retained cohort"
    );
    let late = start + std::time::Duration::from_secs(16);
    owners.sweep_acks(Some(late), &membership);
    assert!(
        owners.holds_ack(prior),
        "a late sweep counts once, skipping missed ticks"
    );
    let admission_sweeps = REMOTE_RELAY_TOTAL_TIMEOUT.as_secs();
    for tick in 1..admission_sweeps {
        owners.sweep_acks(
            Some(late + std::time::Duration::from_secs(tick)),
            &membership,
        );
    }
    assert!(owners.holds_ack(prior), "the bound has not passed yet");
    owners.sweep_acks(
        Some(late + std::time::Duration::from_secs(admission_sweeps)),
        &membership,
    );
    assert!(!owners.holds_ack(prior));
    owners.shutdown();
}

#[cfg(not(feature = "shuttle"))]
#[test]
fn retained_deliveries_keep_their_policy_and_late_sweeps_skip_missed_ticks() {
    policy_epochs();
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test(start_paused = true)]
async fn an_idle_sweeper_wakes_for_a_new_cohort_and_releases_its_charge() {
    use futures_util::FutureExt as _;
    let owners = Arc::new(RemoteDispatchRegistry::with_capacity(1));
    let sweeping = owners.clone();
    let task = nervix_primitives::task::spawn(async move {
        sweeping
            .sweep_pending_acks(&TestMembership::default())
            .await
    });
    nervix_primitives::task::yield_now().await;
    let timing = RemoteAckTiming {
        remote_ack_sweep_interval: std::time::Duration::from_millis(1),
        remote_ack_alive_interval: std::time::Duration::from_millis(1),
        remote_relay_total_timeout: std::time::Duration::from_millis(10),
    };
    timing.validate().assured("coherent short cohort");
    owners.install_timing(timing);
    // The delivery never reports its admission, so the sweep gives its record up at the delivery's
    // admission bound.
    let (acks, completion) = AckSet::root();
    let id = owners
        .register_ack(
            ClusterNodeName::parse("receiver").assured("valid name"),
            acks,
        )
        .assured("free position");
    for _ in 0..12 {
        nervix_primitives::time::advance(std::time::Duration::from_millis(1)).await;
        nervix_primitives::task::yield_now().await;
    }
    assert!(matches!(
        completion.wait().now_or_never(),
        Some(AckOutcome::NoAck(_))
    ));
    assert!(!owners.holds_ack(id));
    assert_eq!(owners.executor.memory(MemoryClass::Relay).reserved_bytes, 0);
    task.abort();
    assert!(
        task.await
            .expect_err("the idle sweep task was cancelled")
            .is_cancelled()
    );
}

#[cfg(not(feature = "shuttle"))]
#[nervix_primitives::test(start_paused = true)]
async fn departed_admission_waiters_are_reaped_without_any_record_deliveries() {
    let owners = Arc::new(RemoteDispatchRegistry::with_capacity(1));
    let sweeping = owners.clone();
    let task = nervix_primitives::task::spawn(async move {
        sweeping
            .sweep_pending_acks(&TestMembership::default())
            .await
    });
    nervix_primitives::task::yield_now().await;
    let (_, waiter) = owners
        .register_admission()
        .assured("the admission lane is empty");
    nervix_primitives::task::yield_now().await;
    drop(waiter);
    nervix_primitives::time::advance(nervix_models::RemoteAckSweepInterval::DEFAULT).await;
    nervix_primitives::task::yield_now().await;
    let (_, replacement) = owners
        .register_admission()
        .assured("the idle sweeper reclaims departed admission waiters");
    drop(replacement);
    task.abort();
    assert!(
        task.await
            .expect_err("the sweep task was cancelled")
            .is_cancelled()
    );
}
