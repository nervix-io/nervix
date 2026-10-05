//! Checks of the production report handoff, including scheduled close and refusal races.

use meticulous::ResultExt as _;
use nervix_primitives::{deadlock::ReportHandoff, sync::Arc, thread};

fn refusals_and_delivery() {
    let reports = Arc::new(ReportHandoff::new(1));
    let first = Arc::clone(&reports);
    let first = thread::spawn(move || first.submit(1));
    let second = Arc::clone(&reports);
    let second = thread::spawn(move || second.submit(2));
    let consumer = Arc::clone(&reports);
    let consumer = thread::spawn(move || {
        let delivered = consumer.pop().ok();
        let lost = consumer.take_lost().map_or(0, NonZeroU64::get);
        (delivered, lost)
    });
    let accepted = usize::from(first.join().assured("producer completes"))
        + usize::from(second.join().assured("producer completes"));
    let (delivered, lost) = consumer
        .join()
        .assured("consumer completes without a progress assumption");
    let remaining = reports.pop().ok();
    assert_eq!(
        usize::from(delivered.is_some()) + usize::from(remaining.is_some()),
        accepted
    );
    assert_ne!(
        delivered, remaining,
        "distinct accepted reports are never duplicated"
    );
    assert_eq!(
        lost + reports.take_lost().map_or(0, NonZeroU64::get),
        2 - u64::try_from(accepted).assured("two producers fit")
    );
    assert!(reports.pop().is_err());
    assert!(reports.take_lost().is_none());
}

fn close_and_delivery() {
    let reports = Arc::new(ReportHandoff::new(1));
    let producer = Arc::clone(&reports);
    let producer = thread::spawn(move || producer.submit(1));
    let closing = Arc::clone(&reports);
    let closing = thread::spawn(move || closing.close());
    let accepted = producer.join().assured("producer completes");
    closing.join().assured("close completes");
    assert!(reports.is_closed());
    assert_eq!(reports.pop().is_ok(), accepted);
    assert!(!reports.submit(2));
    assert_eq!(
        reports.take_lost().map_or(0, NonZeroU64::get),
        2 - u64::from(accepted)
    );
    assert!(reports.pop().is_err());
}

use std::num::NonZeroU64;

#[cfg(not(feature = "shuttle"))]
#[test]
fn bounded_delivery_accounts_for_every_refusal() {
    refusals_and_delivery();
}
#[cfg(not(feature = "shuttle"))]
#[test]
fn close_preserves_admitted_reports_and_counts_refusals() {
    close_and_delivery();
}
#[cfg(feature = "shuttle")]
#[test]
fn shuttle_bounded_report_handoff_accounts_for_concurrent_delivery_and_refusals() {
    nervix_model_harness::shuttle::check_dfs(refusals_and_delivery, None);
}
#[cfg(feature = "shuttle")]
#[test]
fn shuttle_report_handoff_close_preserves_admitted_reports_and_counts_refusals() {
    nervix_model_harness::shuttle::check_dfs(close_and_delivery, None);
}
