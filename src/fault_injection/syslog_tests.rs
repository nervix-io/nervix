//! Syslog listener fault arming qualification.
//!
//! Layer: test harness, outside the product layer order.
//! Owns: the retained request and acknowledgement boundary of one armed listener fault.
//! Depends on: the host's fault controls and execution-sensitive watch channels.
//! Must not know: socket drivers, source lifecycle or product configuration.

use super::FaultInjection;

#[test]
fn an_armed_listener_fault_remains_available_until_acknowledged() {
    let faults = FaultInjection::default();
    let channel = faults.syslog_listener_faults();
    let _initial_listener = channel.subscribe();
    let observed = faults.fail_next_syslog_stream_accept("first-listener".into());
    let late_listener = channel.subscribe();
    {
        let pending = late_listener.borrow();
        let fault = pending.as_ref().expect("one armed request");
        assert_eq!(fault.bind_addr, "first-listener");
        assert!(!*observed.borrow());
        fault.fired.send_replace(true);
    }
    assert!(*observed.borrow());
    let next_observed = faults.fail_next_syslog_stream_accept("next-listener".into());
    let pending = late_listener.borrow();
    assert_eq!(
        pending.as_ref().expect("the next armed request").bind_addr,
        "next-listener"
    );
    assert!(!*next_observed.borrow());
}

#[test]
#[should_panic(expected = "observe the armed Syslog listener fault before arming another")]
fn an_unobserved_listener_fault_cannot_be_overwritten() {
    let faults = FaultInjection::default();
    let _listener = faults.syslog_listener_faults().subscribe();
    let _observed = faults.fail_next_syslog_stream_accept("first-listener".into());
    faults.fail_next_syslog_stream_accept("another-listener".into());
}
