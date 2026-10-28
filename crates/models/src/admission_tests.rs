//! Tests of the leader's verdicts and their gossip text.
//!
//! Layer: test harness.
//!
//! - **Owns.** Coverage of a verdict lookup by run and of the gossip text: one that reads back, and
//!   text that holds no verdicts or judges one run twice.
//! - **Depends on.** The verdicts, node identities and the prerequisite mismatch.
//! - **Must not know.** How a leader judges a run or how gossip carries the text.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{collections::BTreeMap, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};

use super::{AdmissionVerdict, AdmissionVerdicts, AdmissionVerdictsTextError};
use crate::{
    ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, PrerequisiteMismatch,
    TimingPrerequisite,
};

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("the test's node names are valid")
}

fn run(name: &str, incarnation: u64) -> ClusterNodeIdentity {
    ClusterNodeIdentity::new(node(name), ClusterNodeIncarnation::new(incarnation))
}

fn refused_beside_node_1() -> AdmissionVerdict {
    AdmissionVerdict::Refused {
        voter: node("node-1"),
        mismatch: PrerequisiteMismatch::Timing {
            prerequisite: TimingPrerequisite::RaftHeartbeatInterval,
            local: Duration::from_millis(166),
            peer: Duration::from_millis(400),
        },
    }
}

fn verdicts() -> AdmissionVerdicts {
    AdmissionVerdicts::new(
        7,
        BTreeMap::from([
            (run("node-1", 10), AdmissionVerdict::Accepted),
            (run("node-3", 30), refused_beside_node_1()),
        ]),
    )
}

#[test]
fn a_verdict_is_found_by_the_exact_run_it_judged() {
    let verdicts = verdicts();

    assert_eq!(verdicts.leader_term(), 7);
    assert_eq!(
        verdicts.verdict_of(&run("node-1", 10)),
        Some(&AdmissionVerdict::Accepted)
    );
    assert_eq!(
        verdicts.verdict_of(&run("node-3", 30)),
        Some(&refused_beside_node_1())
    );
    // Another run of the same node was not judged by these verdicts.
    assert_eq!(verdicts.verdict_of(&run("node-3", 31)), None);
}

#[test]
fn the_gossip_text_reads_back_as_the_same_verdicts() {
    let original = verdicts();

    let text = original.to_gossip_text();
    let read = AdmissionVerdicts::from_gossip_text(&text).assured("the verdicts were just written");

    assert_eq!(read, original);
}

#[test]
fn gossip_text_that_holds_no_verdicts_is_refused() {
    for text in ["", "{}", "not json", r#"{"leader_term":1}"#] {
        let error =
            AdmissionVerdicts::from_gossip_text(text).expect_err("the text holds no verdicts");
        assert_eq!(
            error.current_context(),
            &AdmissionVerdictsTextError::Malformed,
            "{text:?}"
        );
    }
}

#[test]
fn gossip_text_that_judges_one_run_twice_is_refused() {
    let once = verdicts().to_gossip_text();
    let list_open = once.find('[').assured("the text lists its runs");
    let entries_start = list_open
        .checked_add(1)
        .assured("an offset inside the text is below usize::MAX");
    let between_entries = once.find("},{").assured("the text lists two runs");
    let first_entry_end = between_entries
        .checked_add(1)
        .assured("an offset inside the text is below usize::MAX");
    let first_entry = &once[entries_start..first_entry_end];
    let twice = format!(
        "{}{first_entry},{}",
        &once[..entries_start],
        &once[entries_start..]
    );

    let error = AdmissionVerdicts::from_gossip_text(&twice)
        .expect_err("a run judged twice is not one leader's verdicts");

    assert_eq!(
        error.current_context(),
        &AdmissionVerdictsTextError::RepeatedRun {
            run: run("node-1", 10),
        }
    );
}
