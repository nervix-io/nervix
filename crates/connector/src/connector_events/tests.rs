//! Layer: test harness.
//! - **Owns.** Construction refusal and retained event recording through the host contract.
//! - **Depends on.** The connector event handle and selected atomic observations.
//! - **Must not know.** Metrics, graph execution or transport delivery decisions.

use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::*;

#[test]
fn a_counter_keeps_the_resolved_recorder_without_recording_at_construction() {
    let recorded = Arc::new(AtomicU64::new(0));
    let observation = recorded.clone();
    let counter = ConnectorEventCounter::new(
        DeclaredEvents::new(&[ConnectorEvent::PublishedWithoutSubscribers]),
        ConnectorEvent::PublishedWithoutSubscribers,
        move || {
            observation.fetch_add(1, Ordering::Relaxed);
        },
    )
    .expect("the host declares this event");
    assert_eq!(recorded.load(Ordering::Relaxed), 0);
    for _ in 0..3 {
        counter.increment();
    }
    assert_eq!(recorded.load(Ordering::Relaxed), 3);
}

#[test]
fn an_undeclared_event_is_refused_before_its_recorder_can_run() {
    let error = ConnectorEventCounter::new(
        DeclaredEvents::NONE,
        ConnectorEvent::PublishedWithoutSubscribers,
        || panic!("an undeclared recorder must never run"),
    )
    .expect_err("the host declares no events");
    assert_eq!(
        error.current_context().event,
        ConnectorEvent::PublishedWithoutSubscribers
    );
    struct NoEvents;
    impl ConnectorEventProvider for NoEvents {}
    assert_eq!(
        NoEvents
            .event_counter(ConnectorEvent::PublishedWithoutSubscribers)
            .expect_err("a host with no events refuses the requested event")
            .current_context(),
        error.current_context(),
    );
}
