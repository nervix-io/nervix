//! Repository test harness, outside the product layer order.
//! Owns: real advisory-lock and lease-lifetime examples of the production queue.
//! Depends on: private filesystem fixtures and the current lease contract.
//! Must not know: compiler execution, other worktrees or host-load timing.

use meticulous::{OptionExt as _, ResultExt as _};

use super::*;

fn lease() -> Lease {
    Lease {
        identifier: "current-lease".into(),
        pid: std::process::id(),
        crate_name: "example".into(),
        kind: "lib".into(),
        configuration: "ordinary".into(),
        root: "/fixture".into(),
        reservation_bytes: GIB,
        cgroup: None,
        enqueued_ns: 1_000,
        polled_ns: 1_000,
        admitted_ns: Some(1_000),
        driver_pid: None,
    }
}

#[test]
fn a_held_lease_is_live_and_release_removes_it() {
    let directory = tempfile::tempdir().assured("the queue fixture can create its directory");
    let queue_path = directory.path().join("queue");
    let place = Queue::open(&queue_path)
        .assured("the queue can be opened")
        .enter(lease())
        .assured("the current lease can enter");
    let queue = Queue::open(&queue_path).assured("another observer can open the queue");
    let _locked = queue
        .deciding(Instant::now() + POLL)
        .assured("the decision lock can be held");
    assert_eq!(
        queue.live().assured("the held lease is readable"),
        [lease()]
    );
    drop(place);
    assert!(
        queue
            .live()
            .assured("the released lease is absent")
            .is_empty()
    );
}

#[test]
fn the_kernel_releases_a_stale_lease_and_the_next_decision_reclaims_its_file() {
    let directory = tempfile::tempdir().assured("the queue fixture can create its directory");
    let queue = Queue::open(directory.path()).assured("the queue can be opened");
    let path = directory.path().join("leases/current-lease.json");
    crate::io::atomic_json(&path, &lease()).assured("an unlocked current lease can be written");
    let _locked = queue
        .deciding(Instant::now() + POLL)
        .assured("the decision lock can be held");
    assert!(
        queue
            .live()
            .assured("the unlocked lease is reclaimed")
            .is_empty()
    );
    assert!(!path.exists());
}

#[test]
fn a_held_queue_lock_reports_its_deadline_without_starting_another_decision() {
    let directory = tempfile::tempdir().assured("the queue fixture can create its directory");
    let queue = Queue::open(directory.path()).assured("the queue can be opened");
    let _locked = queue
        .deciding(Instant::now() + POLL)
        .assured("the first decision can be held");
    let report = queue
        .deciding(Instant::now())
        .err()
        .assured("the second decision finds the held lock");
    assert!(matches!(report.current_context(), Error::QueueLocked));
}

#[test]
fn a_queue_must_be_a_directory_owned_by_this_user_and_never_a_symlink() {
    let directory = tempfile::tempdir().assured("the queue fixture can create its directory");
    let file = directory.path().join("file");
    fs::write(&file, "content").assured("the fixture file can be written");
    assert!(Queue::open(&file).is_err());
    let alias = directory.path().join("alias");
    std::os::unix::fs::symlink(directory.path(), &alias)
        .assured("the fixture symlink can be written");
    assert!(Queue::open(&alias).is_err());
}

#[test]
fn an_unusable_queue_reports_unavailable_and_holds_no_place() {
    let directory = tempfile::tempdir().assured("the queue fixture can create its directory");
    let path = directory.path().join("file");
    fs::write(&path, "content").assured("the fixture file can be written");
    let policy = Policy {
        directory: path,
        threshold_bytes: 0,
        wait_limit_seconds: 1.0,
        reservations: Default::default(),
    };
    let admission = Admission::admit(
        &policy,
        &Compiled {
            crate_name: "example".into(),
            kind: "lib".into(),
        },
        GIB,
        "ordinary",
        directory.path(),
    );
    assert_eq!(admission.record.outcome, AdmissionOutcome::Unavailable);
    assert!(admission.place.is_none());
    assert!(admission.record.error.is_some());
}

fn policy(directory: &Path) -> Policy {
    Policy {
        directory: directory.into(),
        threshold_bytes: 0,
        wait_limit_seconds: 0.000_000_001,
        reservations: Default::default(),
    }
}

fn request(policy: &Policy, reservation: u64) -> Admission {
    Admission::admit(
        policy,
        &Compiled {
            crate_name: "example".into(),
            kind: "lib".into(),
        },
        reservation,
        "ordinary",
        Path::new("/fixture"),
    )
}

#[test]
fn an_immediate_fit_and_an_alone_reservation_keep_their_places_and_outcomes() {
    for (reservation, outcome) in [
        (0, AdmissionOutcome::Fit),
        (u64::MAX, AdmissionOutcome::Alone),
    ] {
        let directory = tempfile::tempdir().assured("the queue fixture can create its directory");
        let admission = request(&policy(directory.path()), reservation);
        assert_eq!(admission.record.outcome, outcome);
        assert!(admission.place.is_some());
        assert_eq!(admission.record.reservation_bytes, reservation);
    }
}

#[test]
fn the_wait_limit_bounds_a_nonfitting_waiter_and_a_held_decision_lock() {
    let directory = tempfile::tempdir().assured("the queue fixture can create its directory");
    let mut holder = lease();
    holder.reservation_bytes = u64::MAX;
    let place = Queue::open(directory.path())
        .assured("the queue opens")
        .enter(holder)
        .assured("the admitted holder enters");
    let admission = request(&policy(directory.path()), GIB);
    assert_eq!(admission.record.outcome, AdmissionOutcome::WaitLimit);
    assert_eq!(admission.record.holders, 1);
    assert!(admission.place.is_some());
    drop(admission);
    drop(place);
    let queue = Queue::open(directory.path()).assured("the queue reopens");
    let _locked = queue
        .deciding(Instant::now() + POLL)
        .assured("another decision holds the queue lock");
    let admission = request(&policy(directory.path()), GIB);
    assert_eq!(admission.record.outcome, AdmissionOutcome::QueueLocked);
    assert!(admission.place.is_some());
}

#[test]
fn an_unreadable_held_current_lease_is_reported_and_not_reclaimed() {
    let directory = tempfile::tempdir().assured("the queue fixture can create its directory");
    let queue = Queue::open(directory.path()).assured("the queue opens");
    let place = Queue::open(directory.path())
        .assured("the holder opens the queue")
        .enter(lease())
        .assured("the current holder enters");
    let mut incomplete = serde_json::to_value(&place.lease).assured("the current lease serializes");
    incomplete
        .as_object_mut()
        .assured("the current lease is an object")
        .remove("driver_pid");
    let bytes = serde_json::to_vec(&incomplete).assured("the incomplete current lease serializes");
    place
        .file
        .write_all_at(&bytes, 0)
        .assured("the holder writes its incomplete lease");
    place
        .file
        .set_len(u64::try_from(bytes.len()).assured("the bounded fixture length fits"))
        .assured("the holder truncates its lease");
    let _lock = queue
        .deciding(Instant::now() + POLL)
        .assured("the observer owns the decision lock");
    assert!(
        queue
            .live()
            .assured("a malformed held lease is skipped")
            .is_empty()
    );
    assert!(place.path.exists());
}

#[test]
fn a_driver_named_under_a_held_queue_lock_keeps_its_whole_reservation() {
    let directory = tempfile::tempdir().assured("the queue fixture can create its directory");
    let mut place = Queue::open(directory.path())
        .assured("the holder opens the queue")
        .enter(lease())
        .assured("the current holder enters");
    let queue = Queue::open(directory.path()).assured("the queue observer opens");
    let _lock = queue
        .deciding(Instant::now() + POLL)
        .assured("another decision holds the queue lock");
    place
        .name_driver(42)
        .assured("a held decision conservatively leaves the whole reservation");
    let observed: Lease =
        crate::io::read(&place.path).assured("the whole held lease remains readable");
    assert_eq!(observed.driver_pid, None);
    assert_eq!(observed.reservation_bytes, GIB);
}
