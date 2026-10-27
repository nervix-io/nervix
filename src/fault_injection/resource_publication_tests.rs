//! Uploaded-version publication refusal qualification.
//!
//! Layer: test harness, outside the product layer order.
//! Owns: the delivery, take-up and reply-hold observations of one armed publication refusal.
//! Depends on: the host's fault controls and vocabulary identities.
//! Must not know: how an upload installs or recovers, consensus or the resource store.

use futures_util::FutureExt as _;
use meticulous::ResultExt as _;
use nervix_models::ClusterNodeName;

use super::FaultInjection;

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("the test node is a valid literal name")
}

#[test]
fn a_publication_refusal_is_delivered_once_and_taken_up_only_after_delivery() {
    let faults = FaultInjection::default();
    let leader = node("node-1");
    let follower = node("node-2");
    assert_eq!(faults.refused_resource_publication_taken_up(&leader), None);
    assert!(!faults.refuses_resource_publication(&leader));

    faults.refuse_next_resource_publication_on(leader.clone());
    // A reconciliation that takes an upload up before a publication was refused leaves the
    // refusal armed.
    faults.take_up_refused_resource_upload(&leader);
    assert_eq!(
        faults.refused_resource_publication_taken_up(&leader),
        Some(false)
    );
    assert!(!faults.refuses_resource_publication(&follower));
    assert!(faults.refuses_resource_publication(&leader));
    // The refusal is one-shot, so the leader's next publication goes through.
    assert!(!faults.refuses_resource_publication(&leader));
    assert_eq!(
        faults.refused_resource_publication_taken_up(&leader),
        Some(false)
    );

    faults.take_up_refused_resource_upload(&leader);
    assert_eq!(
        faults.refused_resource_publication_taken_up(&leader),
        Some(true)
    );
    assert!(!faults.refuses_resource_publication(&leader));
}

#[test]
fn a_refused_upload_reply_waits_until_the_reconciliation_takes_the_upload_up() {
    let faults = FaultInjection::default();
    let leader = node("node-1");
    assert!(
        faults
            .hold_refused_resource_upload_reply(&leader)
            .now_or_never()
            .is_some(),
        "a node with no armed refusal holds no reply"
    );

    faults.refuse_next_resource_publication_on(leader.clone());
    assert!(
        faults
            .hold_refused_resource_upload_reply(&leader)
            .now_or_never()
            .is_some(),
        "a refusal that met no publication holds no reply"
    );

    assert!(faults.refuses_resource_publication(&leader));
    assert!(
        faults
            .hold_refused_resource_upload_reply(&leader)
            .now_or_never()
            .is_none(),
        "the refused upload's reply waits for the reconciliation"
    );

    faults.take_up_refused_resource_upload(&leader);
    assert!(
        faults
            .hold_refused_resource_upload_reply(&leader)
            .now_or_never()
            .is_some(),
        "the reconciliation's take-up releases the reply"
    );
}
