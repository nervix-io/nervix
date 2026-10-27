//! Owner tests of the gap a scenario opens in one node's live view for another node's run.
//!
//! Layer: test harness, outside the product layer order.
//! Owns: what each look of the observing node's acknowledgement watchers finds while a gap is open.
//! Depends on: the host's fault controls and vocabulary identities.
//! Must not know: gossip, acknowledgement watches or the runtime.

use meticulous::ResultExt as _;
use nervix_models::ClusterNodeName;

use super::FaultInjection;

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("the test node name is a valid literal name")
}

#[test]
fn a_gap_shows_the_run_once_more_and_then_misses_it_until_it_is_closed() {
    let faults = FaultInjection::default();
    let observer = node("node-2");
    let missing = node("node-1");
    assert!(!faults.misses_live_run(&observer, &missing));

    faults.open_live_view_gap(observer.clone(), missing.clone());
    assert!(
        !faults.misses_live_run(&observer, &missing),
        "the next look still finds the run, so the watcher has seen it live"
    );
    assert!(faults.misses_live_run(&observer, &missing));
    assert!(
        faults.misses_live_run(&observer, &missing),
        "every later look misses the run while the gap is open"
    );
    assert!(
        !faults.misses_live_run(&missing, &observer),
        "the gap belongs to one observer and one missing node"
    );

    faults.close_live_view_gap(&observer, &missing);
    assert!(!faults.misses_live_run(&observer, &missing));
}

#[nervix_primitives::test]
async fn a_scenario_waits_for_the_look_that_missed_the_run() {
    let faults = FaultInjection::default();
    let observer = node("node-2");
    let missing = node("node-1");
    faults.open_live_view_gap(observer.clone(), missing.clone());
    assert!(!faults.misses_live_run(&observer, &missing));
    assert!(faults.misses_live_run(&observer, &missing));
    faults
        .wait_for_live_view_gap_read(&observer, &missing)
        .await;
    faults.close_live_view_gap(&observer, &missing);
}
