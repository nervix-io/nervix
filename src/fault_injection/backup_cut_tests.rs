//! Backup cut gate qualification.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The holds a backup cut gate applies to every cut that reaches it, and cleanup's
//!   release of existing waiters and gates no cut reached yet.
//! - **Depends on.** The gate's fault controls and the runtime's paused clock.
//! - **Must not know.** Backup coordination, leases or archive contents.

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::DomainName;
use nervix_primitives::{task::JoinHandle, time::Instant};
use strum::IntoEnumIterator as _;

use super::{BackupCutBoundary, FaultInjection};

fn domain() -> DomainName {
    DomainName::parse("held").assured("the test domain name is valid")
}

/// A backup cut of `domain` reaching `boundary`, on a task of its own.
fn arriving_cut(faults: &FaultInjection, boundary: BackupCutBoundary) -> JoinHandle<()> {
    let faults = faults.clone();
    nervix_primitives::task::spawn(async move {
        faults.pause_backup_cut_if_armed(&domain(), boundary).await;
    })
}

#[nervix_primitives::test(start_paused = true)]
async fn a_later_cut_of_the_domain_waits_at_an_armed_gate_too() {
    let faults = FaultInjection::default();
    faults.pause_backup_cut_on(domain(), BackupCutBoundary::Capture);
    let first = arriving_cut(&faults, BackupCutBoundary::Capture);
    faults
        .wait_for_backup_cut_pause(&domain(), BackupCutBoundary::Capture)
        .await;

    // A backup that runs again under its execution reference cuts the domain again.
    let later = arriving_cut(&faults, BackupCutBoundary::Capture);
    nervix_primitives::task::yield_now().await;
    assert!(
        !later.is_finished(),
        "a later cut of the domain passed the armed gate while the first one waits"
    );

    faults.release_backup_cut_pause(&domain(), BackupCutBoundary::Capture);
    first.await.assured("the first cut continues once released");
    later.await.assured("the later cut continues once released");
    arriving_cut(&faults, BackupCutBoundary::Capture)
        .await
        .assured("a cut passes a released gate");
}

#[nervix_primitives::test(start_paused = true)]
async fn a_timed_hold_holds_every_cut_from_its_own_arrival() {
    let faults = FaultInjection::default();
    let hold = Duration::from_secs(12);
    faults.pause_backup_cut_on(domain(), BackupCutBoundary::Capture);
    faults.hold_backup_cuts_for(&domain(), BackupCutBoundary::Capture, hold);

    let first = arriving_cut(&faults, BackupCutBoundary::Capture);
    let first_arrival = faults
        .wait_for_backup_cut_pause(&domain(), BackupCutBoundary::Capture)
        .await;
    first
        .await
        .assured("the first cut continues after its hold");
    assert_eq!(first_arrival.elapsed(), hold);

    let later_arrival = Instant::now();
    arriving_cut(&faults, BackupCutBoundary::Capture)
        .await
        .assured("a later cut continues after its own hold");
    assert_eq!(later_arrival.elapsed(), hold);
}

#[nervix_primitives::test(start_paused = true)]
async fn a_hold_timed_while_a_cut_waits_counts_from_that_cut_s_arrival() {
    let faults = FaultInjection::default();
    faults.pause_backup_cut_on(domain(), BackupCutBoundary::Lease);
    let waiting = arriving_cut(&faults, BackupCutBoundary::Lease);
    let arrival = faults
        .wait_for_backup_cut_pause(&domain(), BackupCutBoundary::Lease)
        .await;
    nervix_primitives::time::advance(Duration::from_secs(5)).await;

    faults.hold_backup_cuts_for(&domain(), BackupCutBoundary::Lease, Duration::from_secs(12));
    waiting
        .await
        .assured("the waiting cut continues at the end of its hold");
    assert_eq!(arrival.elapsed(), Duration::from_secs(12));
}

#[nervix_primitives::test(start_paused = true)]
async fn a_cut_that_waited_longer_than_a_new_hold_continues_at_once() {
    let faults = FaultInjection::default();
    faults.pause_backup_cut_on(domain(), BackupCutBoundary::Lease);
    let waiting = arriving_cut(&faults, BackupCutBoundary::Lease);
    let arrival = faults
        .wait_for_backup_cut_pause(&domain(), BackupCutBoundary::Lease)
        .await;
    nervix_primitives::time::advance(Duration::from_secs(20)).await;

    faults.hold_backup_cuts_for(&domain(), BackupCutBoundary::Lease, Duration::from_secs(12));
    waiting
        .await
        .assured("a cut that already waited its hold continues at once");
    assert_eq!(arrival.elapsed(), Duration::from_secs(20));
}

#[nervix_primitives::test(start_paused = true)]
async fn a_boundary_no_scenario_armed_holds_no_cut() {
    let faults = FaultInjection::default();
    faults.pause_backup_cut_on(domain(), BackupCutBoundary::Lease);
    let started = Instant::now();
    arriving_cut(&faults, BackupCutBoundary::Capture)
        .await
        .assured("a cut passes a boundary no scenario armed");
    assert_eq!(started.elapsed(), Duration::ZERO);
}

#[nervix_primitives::test(start_paused = true)]
async fn cleanup_releases_waiting_cuts_at_every_boundary_before_clearing_the_registry() {
    let faults = FaultInjection::default();
    for boundary in BackupCutBoundary::iter() {
        faults.pause_backup_cut_on(domain(), boundary);
    }
    let mut waiting = Vec::new();
    for boundary in BackupCutBoundary::iter() {
        waiting.push(arriving_cut(&faults, boundary));
    }
    for boundary in BackupCutBoundary::iter() {
        faults.wait_for_backup_cut_pause(&domain(), boundary).await;
    }
    // Cleanup releases a timed hold too, rather than waiting for its duration.
    faults.hold_backup_cuts_for(
        &domain(),
        BackupCutBoundary::Capture,
        Duration::from_secs(300),
    );
    let cleanup_started = Instant::now();
    assert_eq!(
        faults.release_all_backup_cut_pauses(),
        BackupCutBoundary::iter().count()
    );
    assert!(faults.inner.backup_cut_gates.is_empty());

    nervix_primitives::time::timeout(Duration::from_secs(1), async {
        for cut in waiting {
            cut.await
                .assured("cleanup releases the cut waiting at each boundary");
        }
        for boundary in BackupCutBoundary::iter() {
            arriving_cut(&faults, boundary)
                .await
                .assured("later cuts pass after cleanup");
        }
    })
    .await
    .assured("releasing the borrowed gates wakes their cuts without waiting for a hold");
    assert_eq!(cleanup_started.elapsed(), Duration::ZERO);
    assert_eq!(faults.release_all_backup_cut_pauses(), 0);
}

#[nervix_primitives::test(start_paused = true)]
async fn cleanup_clears_unreached_gates_of_every_domain_and_is_idempotent() {
    let faults = FaultInjection::default();
    let other = DomainName::parse("another").assured("the other test domain name is valid");
    let mut armed = 0_usize;
    for name in [&domain(), &other] {
        for boundary in BackupCutBoundary::iter() {
            faults.pause_backup_cut_on(name.clone(), boundary);
            armed = armed
                .checked_add(1)
                .assured("two domains arm one gate per boundary");
        }
    }
    assert_eq!(faults.release_all_backup_cut_pauses(), armed);
    assert_eq!(faults.release_all_backup_cut_pauses(), 0);
    assert!(faults.inner.backup_cut_gates.is_empty());
    nervix_primitives::time::timeout(Duration::from_secs(1), async {
        for name in [&domain(), &other] {
            for boundary in BackupCutBoundary::iter() {
                faults.pause_backup_cut_if_armed(name, boundary).await;
            }
        }
    })
    .await
    .assured("a cut arriving at any cleaned-up gate passes immediately");
}
