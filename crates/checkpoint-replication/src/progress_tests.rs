use std::collections::BTreeSet;

use meticulous::ResultExt as _;
use nervix_models::ClusterNodeName;

use super::ReplicaProgress;

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("the test node names satisfy the cluster-node grammar")
}

fn nodes(names: &[&str]) -> BTreeSet<ClusterNodeName> {
    names.iter().map(|name| node(name)).collect()
}

#[test]
fn a_replica_holds_the_highest_revision_it_reported() {
    let mut progress = ReplicaProgress::default();
    assert_eq!(progress.held(&node("node-2")), None);
    assert!(!progress.holds(&node("node-2"), 0));

    assert!(progress.record(&node("node-2"), 4));
    assert_eq!(progress.held(&node("node-2")), Some(4));
    assert!(progress.holds(&node("node-2"), 4));
    assert!(progress.holds(&node("node-2"), 3));
    assert!(!progress.holds(&node("node-2"), 5));

    assert!(progress.record(&node("node-2"), 6));
    assert_eq!(progress.held(&node("node-2")), Some(6));
}

#[test]
fn an_older_or_repeated_report_changes_nothing() {
    let mut progress = ReplicaProgress::default();
    assert!(progress.record(&node("node-2"), 6));

    assert!(
        !progress.record(&node("node-2"), 4),
        "a delayed acknowledgement of an older revision must not lower the replica"
    );
    assert!(!progress.record(&node("node-2"), 6));
    assert_eq!(progress.held(&node("node-2")), Some(6));
}

#[test]
fn a_report_of_revision_zero_is_a_report() {
    let mut progress = ReplicaProgress::default();
    assert!(progress.record(&node("node-2"), 0));
    assert_eq!(progress.held(&node("node-2")), Some(0));
    assert!(progress.holds(&node("node-2"), 0));
    assert!(!progress.holds(&node("node-3"), 0));
}

#[test]
fn replicas_are_counted_only_while_they_hold_the_revision() {
    let mut progress = ReplicaProgress::default();
    let replicas = nodes(&["node-2", "node-3", "node-4"]);
    assert_eq!(progress.holding(&replicas, 5), 0);
    assert_eq!(progress.awaiting(&replicas, 5), replicas);

    progress.record(&node("node-2"), 5);
    progress.record(&node("node-3"), 4);
    progress.record(&node("node-5"), 9);
    assert_eq!(progress.holding(&replicas, 5), 1);
    assert_eq!(
        progress.awaiting(&replicas, 5),
        nodes(&["node-3", "node-4"])
    );
    assert_eq!(
        progress.holding(&replicas, 4),
        2,
        "a replica that holds a newer revision holds every older one"
    );
    assert_eq!(
        progress.awaiting(&nodes(&["node-5"]), 5),
        BTreeSet::new(),
        "a replica outside the assignment still reports what it holds"
    );
}
