//! Regressions for reading where a node reports that a scheduled unit runs.
//!
//! Layer: test harness.
//! - **Owns.** `SHOW CLUSTER STATUS` and `DESCRIBE` excerpts shaped as nodes render them, and what
//!   a step waiting for a unit to move to another node reads from them.
//! - **Depends on.** The unit placement reader.
//! - **Must not know.** How a scenario reads a node's output.

use meticulous::OptionExt as _;

use crate::unit_placement::{ScheduledPlacement, described_owner, described_replicas};

const DOMAIN: &str = "d01takeover";

/// The schedule node-2 reported after node-1, which owned the WASM processor, stopped and before
/// failover placed the processor again.
const STATUS_BETWEEN_OWNERS: &str = "\
[schedule]
- domain=d01takeover kind=wasm_processor name=counting_guest owner=- replicas=-
- domain=d01takeover kind=relay name=counted_events owner=node-2 replicas=-
";

/// The same schedule once node-2 took the WASM processor over, with node-3 holding a replica of
/// its state.
const STATUS_AFTER_TAKEOVER: &str = "\
[schedule]
- domain=d01takeover kind=wasm_processor name=counting_guest owner=node-2 replicas=node-3
- domain=d01takeover kind=relay name=counted_events owner=node-2 replicas=-
";

fn counting_guest(status: &str) -> ScheduledPlacement<'_> {
    ScheduledPlacement::from_status(status, DOMAIN, "wasm_processor", "counting_guest")
        .assured("the excerpt lists the WASM processor")
}

#[test]
fn a_status_reports_no_owner_for_a_unit_no_node_owns() {
    assert_eq!(
        counting_guest(STATUS_BETWEEN_OWNERS),
        ScheduledPlacement {
            owner: None,
            replicas: Vec::new(),
        }
    );
}

#[test]
fn a_status_reports_the_node_that_owns_a_unit_and_its_replicas() {
    assert_eq!(
        counting_guest(STATUS_AFTER_TAKEOVER),
        ScheduledPlacement {
            owner: Some("node-2"),
            replicas: vec!["node-3"],
        }
    );
}

#[test]
fn a_unit_no_node_owns_has_not_moved_to_another_node() {
    let placement = counting_guest(STATUS_BETWEEN_OWNERS);
    assert!(
        !placement.is_owned_by_node_other_than("node-1"),
        "a unit no node owns has not moved off its former owner yet: {placement:?}"
    );
}

#[test]
fn a_unit_another_node_owns_has_moved_to_that_node() {
    let placement = counting_guest(STATUS_AFTER_TAKEOVER);
    assert!(
        placement.is_owned_by_node_other_than("node-1"),
        "node-2 owns the unit node-1 lost: {placement:?}"
    );
    assert!(
        !placement.is_owned_by_node_other_than("node-2"),
        "the unit has not moved off node-2, which owns it: {placement:?}"
    );
    assert_eq!(placement.replicas, ["node-3"]);
}

#[test]
fn a_status_lists_no_placement_for_a_unit_it_does_not_schedule() {
    assert_eq!(
        ScheduledPlacement::from_status(
            STATUS_AFTER_TAKEOVER,
            DOMAIN,
            "junction",
            "counting_guest"
        ),
        None,
        "no junction of that name is scheduled"
    );
    assert_eq!(
        ScheduledPlacement::from_status(
            STATUS_AFTER_TAKEOVER,
            "d01another0domain",
            "wasm_processor",
            "counting_guest"
        ),
        None,
        "another domain schedules no unit of that name"
    );
}

#[test]
fn a_describe_output_of_a_unit_no_node_owns_names_no_owner() {
    let output = "exists\nowner: -\nreplicas: -\n";
    assert_eq!(described_owner(output), None);
    assert!(described_replicas(output).is_empty());
}

#[test]
fn a_describe_output_names_the_owner_and_replicas_of_its_unit() {
    let output = "exists\nowner: node-2\nreplicas: node-1, node-3\n";
    assert_eq!(described_owner(output), Some("node-2"));
    assert_eq!(described_replicas(output), ["node-1", "node-3"]);
}
