//! Layer: test harness.
//! - **Owns.** Captured delivery policy and skipped late-sweep evidence.
//! - **Depends on.** The production correlation owner and acknowledgement roots.
//! - **Must not know.** Transport, persisted records or graph placement.

use super::*;

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
    let driver = SilenceSweeps::watching(&owners.changed, &owners.next_sweep).run(|| {
        scans.set(scans.get() + 1);
        owners.sweep_acks(Some(Instant::now())).1
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
    let start = Instant::now();
    let receiver = ClusterNodeName::parse("receiver").assured("valid fixture name");
    let (prior, _prior_completion) = AckSet::root();
    let prior = owners
        .register_ack_with_clock(receiver.clone(), prior, || start)
        .assured("free slot");
    owners.admit_ack(prior);
    let timing = RemoteAckTiming {
        remote_ack_alive_interval: std::time::Duration::from_millis(1),
        remote_ack_dispatch_timeout: std::time::Duration::from_millis(1),
        remote_ack_silence_timeout: std::time::Duration::from_millis(10),
        remote_ack_sweep_interval: std::time::Duration::from_millis(1),
        remote_relay_total_timeout: std::time::Duration::from_millis(50),
        remote_relay_instantiation_wait: std::time::Duration::from_millis(5),
    };
    timing.validate().assured("coherent fixture policy");
    owners.install_timing(timing);
    let (current, _current_completion) = AckSet::root();
    let current = owners
        .register_ack_with_clock(receiver, current, || start)
        .assured("free slot");
    owners.admit_ack(current);
    for tick in 1..=11 {
        owners.sweep_acks(Some(start + std::time::Duration::from_millis(tick)));
    }
    assert!(!owners.holds_ack(current));
    assert!(
        owners.holds_ack(prior),
        "a new policy cannot shorten a retained cohort"
    );
    let late = start + std::time::Duration::from_secs(16);
    owners.sweep_acks(Some(late));
    assert!(
        owners.holds_ack(prior),
        "a late sweep counts once, skipping missed ticks"
    );
    for tick in 1..=15 {
        owners.sweep_acks(Some(late + std::time::Duration::from_secs(tick)));
    }
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
    let task = nervix_primitives::task::spawn(async move { sweeping.sweep_silent_acks().await });
    nervix_primitives::task::yield_now().await;
    let timing = RemoteAckTiming {
        remote_ack_sweep_interval: std::time::Duration::from_millis(1),
        remote_ack_silence_timeout: std::time::Duration::from_millis(10),
        remote_ack_alive_interval: std::time::Duration::from_millis(1),
        remote_ack_dispatch_timeout: std::time::Duration::from_millis(1),
        remote_relay_total_timeout: std::time::Duration::from_millis(50),
        remote_relay_instantiation_wait: std::time::Duration::from_millis(5),
    };
    timing.validate().assured("coherent short cohort");
    owners.install_timing(timing);
    let (acks, completion) = AckSet::root();
    let id = owners
        .register_ack(
            ClusterNodeName::parse("receiver").assured("valid name"),
            acks,
        )
        .assured("free position");
    owners.admit_ack(id);
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
    let task = nervix_primitives::task::spawn(async move { sweeping.sweep_silent_acks().await });
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
