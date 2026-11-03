//! Installed upload reply loss qualification.
//!
//! Layer: test harness, outside the product layer order.
//! Owns: the delivery and observation of one armed loss of an installed upload's reply.
//! Depends on: the host's fault controls and vocabulary identities.
//! Must not know: how an upload installs or how a client recovers its outcome.

use meticulous::ResultExt as _;
use nervix_models::ClusterNodeName;

use super::FaultInjection;

fn node(name: &str) -> ClusterNodeName {
    ClusterNodeName::parse(name).assured("the test node is a valid literal name")
}

#[test]
fn an_installed_upload_reply_loss_is_delivered_once_on_its_own_node() {
    let faults = FaultInjection::default();
    let leader = node("node-1");
    let follower = node("node-2");
    assert_eq!(faults.lost_installed_resource_upload_reply(&leader), None);
    assert!(!faults.loses_installed_resource_upload_reply(&leader));

    faults.lose_next_installed_resource_upload_reply_on(leader.clone());
    assert_eq!(
        faults.lost_installed_resource_upload_reply(&leader),
        Some(false)
    );
    assert!(!faults.loses_installed_resource_upload_reply(&follower));
    assert_eq!(faults.lost_installed_resource_upload_reply(&follower), None);

    assert!(faults.loses_installed_resource_upload_reply(&leader));
    assert_eq!(
        faults.lost_installed_resource_upload_reply(&leader),
        Some(true)
    );
    // The loss is one-shot, so the call that repeats the upload keeps its reply.
    assert!(!faults.loses_installed_resource_upload_reply(&leader));
    assert_eq!(
        faults.lost_installed_resource_upload_reply(&leader),
        Some(true)
    );
}
