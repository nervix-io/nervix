//! Repository test harness, outside the product layer order.
//! Owns: deterministic examples of the production admission decision.
//! Depends on: current leases and pure memory observations.
//! Must not know: compiler processes, host load or real elapsed-time assumptions.

use super::*;

fn lease(identifier: &str) -> Lease {
    Lease {
        identifier: identifier.into(),
        pid: 1,
        crate_name: "nervix_server".into(),
        kind: "lib".into(),
        configuration: "ordinary".into(),
        root: "/work".into(),
        reservation_bytes: 8 * GIB,
        cgroup: Some("/user.slice/one.scope".into()),
        enqueued_ns: 1_000,
        polled_ns: 1_000,
        admitted_ns: None,
        driver_pid: None,
    }
}

#[test]
fn admission_charges_remaining_growth_and_uses_the_tightest_domain() {
    let waiter = lease("waiter");
    let mut holder = lease("holder");
    holder.admitted_ns = Some(1_000);
    holder.driver_pid = Some(42);
    let leases = [holder];
    let domains = [
        Domain {
            cgroup: None,
            available_bytes: 32 * GIB,
        },
        Domain {
            cgroup: Some("/user.slice".into()),
            available_bytes: 12 * GIB,
        },
    ];
    let decision = Decision::decide(&waiter, &leases, &domains, |_| Some(4 * GIB), 2_000);
    assert!(decision.admitted);
    assert!(decision.fits);
    assert_eq!(decision.left_bytes, Some(i128::from(8 * GIB)));
    let decision = Decision::decide(&waiter, &leases, &domains, |_| Some(2 * GIB), 2_000);
    assert!(!decision.admitted);
    assert_eq!(decision.left_bytes, Some(i128::from(6 * GIB)));
}

#[test]
fn responsive_older_waiters_go_first_and_stopped_waiters_do_not() {
    let waiter = lease("b");
    let leases = [lease("a"), lease("z")];
    let domains = [Domain {
        cgroup: None,
        available_bytes: 64 * GIB,
    }];
    assert_eq!(
        Decision::decide(&waiter, &leases, &domains, |_| None, 2_000).ahead,
        1
    );
    assert!(!Decision::decide(&waiter, &leases, &domains, |_| None, 2_000).admitted);
    assert!(Decision::decide(&waiter, &leases, &domains, |_| None, RESPONSIVE_NS + 2_000).admitted);
}

#[test]
fn unavailable_or_insufficient_memory_admits_only_one_compilation() {
    let waiter = lease("waiter");
    assert!(Decision::decide(&waiter, &[], &[], |_| None, 2_000).admitted);
    let mut holder = lease("holder");
    holder.admitted_ns = Some(1_000);
    let leases = [holder];
    assert!(!Decision::decide(&waiter, &leases, &[], |_| None, 2_000).admitted);
    let domains = [Domain {
        cgroup: None,
        available_bytes: GIB,
    }];
    let decision = Decision::decide(&waiter, &[], &domains, |_| None, 2_000);
    assert!(decision.admitted);
    assert!(!decision.fits);
}

#[test]
fn a_driver_that_has_ended_owes_no_further_growth() {
    let waiter = lease("waiter");
    let mut holder = lease("holder");
    holder.admitted_ns = Some(1_000);
    holder.driver_pid = Some(42);
    let leases = [holder];
    let domains = [Domain {
        cgroup: None,
        available_bytes: 8 * GIB,
    }];
    assert!(Decision::decide(&waiter, &leases, &domains, |_| None, 2_000).fits);
    assert!(Decision::decide(&waiter, &leases, &domains, |_| Some(16 * GIB), 2_000).fits);
}

#[test]
fn every_library_form_is_one_admission_kind() {
    for kind in ["lib", "rlib", "cdylib", "staticlib", "proc-macro"] {
        assert_eq!(target_kind(&[kind.into()]), "lib");
    }
    assert_eq!(target_kind(&["bin".into()]), "bin");
}
