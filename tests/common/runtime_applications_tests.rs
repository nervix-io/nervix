//! Regressions for the comparison of the runtime revisions a cluster's nodes applied.
//!
//! Layer: test harness.
//! - **Owns.** Application progress records shaped as nodes answer them, and the node a comparison
//!   of two sets of answers names.
//! - **Depends on.** The runtime-application comparison and the progress record it reads.
//! - **Must not know.** How a scenario queries a node, or what it publishes.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_interconnect::{ApplicationProgress, RuntimeApplicationFailure};
use nervix_models::{ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName};

use crate::runtime_applications::{NewerApplication, NodeRuntimeApplication, RuntimeApplications};

fn identity(node: &str, incarnation: u64) -> ClusterNodeIdentity {
    ClusterNodeIdentity::new(
        ClusterNodeName::parse(node).assured("the regression names valid nodes"),
        ClusterNodeIncarnation::new(incarnation),
    )
}

/// What a node's first incarnation answers after preparing `prepared` and failing `failed`.
fn answer(node: &str, prepared: Option<u64>, failed: Option<u64>) -> NodeRuntimeApplication {
    answer_of_incarnation(node, 1, prepared, failed)
}

fn answer_of_incarnation(
    node: &str,
    incarnation: u64,
    prepared: Option<u64>,
    failed: Option<u64>,
) -> NodeRuntimeApplication {
    let mut progress = ApplicationProgress::new(identity(node, incarnation));
    progress.runtime_prepared = prepared;
    progress.runtime_ready = prepared;
    progress.runtime_failure = failed.map(|revision| RuntimeApplicationFailure {
        revision,
        reason: "the regression's swap failed".to_string(),
    });
    NodeRuntimeApplication::of(&progress)
}

fn cluster(answers: Vec<(&str, NodeRuntimeApplication)>) -> RuntimeApplications {
    let mut applications = RuntimeApplications::default();
    for (node, application) in answers {
        applications.record(node.to_string(), application);
    }
    applications
}

fn three_nodes_at(revision: u64) -> RuntimeApplications {
    cluster(vec![
        ("node-1", answer("node-1", Some(revision), None)),
        ("node-2", answer("node-2", Some(revision), None)),
        ("node-3", answer("node-3", Some(revision), None)),
    ])
}

#[test]
fn a_cluster_that_applied_nothing_new_has_not_moved_on() {
    let recorded = three_nodes_at(12);
    assert_eq!(three_nodes_at(12).moved_on_from(&recorded), None);
}

#[test]
fn a_node_that_prepared_a_newer_revision_moved_on() {
    let recorded = three_nodes_at(12);
    let current = cluster(vec![
        ("node-1", answer("node-1", Some(12), None)),
        ("node-2", answer("node-2", Some(13), None)),
        ("node-3", answer("node-3", Some(12), None)),
    ]);
    let newer = current.moved_on_from(&recorded);
    assert_eq!(
        newer,
        Some(NewerApplication::Applied {
            node: identity("node-2", 1),
            recorded: Some(12),
            applied: 13,
        })
    );
    assert_eq!(
        newer.assured("the comparison found node-2").to_string(),
        "node-2#1 applied runtime revision 13 after revision 12"
    );
}

#[test]
fn a_node_that_failed_to_prepare_a_newer_revision_applied_it() {
    let recorded = three_nodes_at(12);
    let current = cluster(vec![
        ("node-1", answer("node-1", Some(12), Some(13))),
        ("node-2", answer("node-2", Some(12), None)),
    ]);
    assert_eq!(
        current.moved_on_from(&recorded),
        Some(NewerApplication::Applied {
            node: identity("node-1", 1),
            recorded: Some(12),
            applied: 13,
        })
    );
}

#[test]
fn a_failure_of_an_older_revision_is_nothing_new() {
    let recorded = three_nodes_at(12);
    let current = cluster(vec![
        ("node-1", answer("node-1", Some(12), Some(11))),
        ("node-2", answer("node-2", Some(12), None)),
        ("node-3", answer("node-3", Some(12), None)),
    ]);
    assert_eq!(current.moved_on_from(&recorded), None);
}

#[test]
fn a_node_that_answers_under_another_incarnation_restarted() {
    let recorded = three_nodes_at(12);
    let current = cluster(vec![
        ("node-1", answer("node-1", Some(12), None)),
        ("node-2", answer("node-2", Some(12), None)),
        ("node-3", answer_of_incarnation("node-3", 2, None, None)),
    ]);
    let newer = current.moved_on_from(&recorded);
    assert_eq!(
        newer,
        Some(NewerApplication::Restarted {
            recorded: identity("node-3", 1),
            current: identity("node-3", 2),
        })
    );
    assert_eq!(
        newer.assured("the comparison found node-3").to_string(),
        "node-3#1 restarted as node-3#2"
    );
}

#[test]
fn a_node_that_stopped_answering_shows_nothing_new() {
    let recorded = three_nodes_at(12);
    let current = cluster(vec![
        ("node-1", answer("node-1", Some(12), None)),
        ("node-2", answer("node-2", Some(12), None)),
    ]);
    assert_eq!(current.moved_on_from(&recorded), None);
}

#[test]
fn a_node_missing_from_the_recording_is_compared_with_the_newest_recorded_revision() {
    let recorded = cluster(vec![
        ("node-1", answer("node-1", Some(12), None)),
        ("node-2", answer("node-2", Some(11), None)),
    ]);
    let caught_up = cluster(vec![("node-3", answer("node-3", Some(12), None))]);
    assert_eq!(caught_up.moved_on_from(&recorded), None);

    let ahead = cluster(vec![("node-3", answer("node-3", Some(13), None))]);
    assert_eq!(
        ahead.moved_on_from(&recorded),
        Some(NewerApplication::Applied {
            node: identity("node-3", 1),
            recorded: Some(12),
            applied: 13,
        })
    );
}

#[test]
fn a_node_that_applied_its_first_revision_moved_on() {
    let recorded = cluster(vec![("node-1", answer("node-1", None, None))]);
    let current = cluster(vec![("node-1", answer("node-1", Some(4), None))]);
    let newer = current.moved_on_from(&recorded);
    assert_eq!(
        newer,
        Some(NewerApplication::Applied {
            node: identity("node-1", 1),
            recorded: None,
            applied: 4,
        })
    );
    assert_eq!(
        newer.assured("the comparison found node-1").to_string(),
        "node-1#1 applied runtime revision 4 after none"
    );
}

#[test]
fn the_recording_names_each_answer() {
    let applications = cluster(vec![
        ("node-1", answer("node-1", Some(12), None)),
        ("node-2", answer("node-2", None, None)),
    ]);
    assert_eq!(
        applications.to_string(),
        "node-1#1 at revision 12, node-2#1 at no revision"
    );
    assert_eq!(
        RuntimeApplications::default().to_string(),
        "no node answered"
    );
}
