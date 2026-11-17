//! Transition assertions through typed public inspection, HTTPS requests and native notices.
//!
//! Layer: test harness.
//! - **Owns.** Stable identities/deadlines, acknowledgements and replacement scenario steps.
//! - **Depends on.** Public clients and the controllable signed licensing service.
//! - **Must not know.** Committed storage, policy evaluation or state-machine internals.

use std::{collections::BTreeMap, time::Duration};

use cucumber::{then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::{sleep, timeout};

use super::{
    STATUS_WAIT_BUDGET, ScenarioWorld, current_license, leader_license_client,
    public_license_timestamp, saved_license,
};

#[then(expr = "the committed licensing transition is saved as placeholder {string}")]
async fn save_transition(world: &mut ScenarioWorld, name: String) {
    let fields = timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let fields = current_license(world).await;
            if fields.contains_key("transition id") {
                break fields;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("the public description exposes the committed grant");
    world.placeholders.insert(
        name,
        serde_json::to_string(&fields).assured("public observation serializes"),
    );
}

fn same_grant(saved: &BTreeMap<String, String>, current: &BTreeMap<String, String>) {
    for field in ["transition id", "started at", "deadline", "usage at start"] {
        assert_eq!(
            current.get(field),
            saved.get(field),
            "stable public transition field {field}"
        );
    }
}

#[then(expr = "the committed licensing transition matches placeholder {string}")]
async fn unchanged_transition(world: &mut ScenarioWorld, name: String) {
    let saved = saved_license(world, &name);
    same_grant(&saved, &current_license(world).await);
}

#[then("the transition begins at the decision receipt after delayed delivery")]
async fn receipt_anchor(world: &mut ScenarioWorld) {
    let fields = current_license(world).await;
    let started =
        public_license_timestamp(fields.get("started at").verified("typed transition start"));
    let receipt =
        public_license_timestamp(fields.get("received at").verified("typed decision receipt"));
    assert_eq!(started, receipt);
    let issued = public_license_timestamp(
        fields
            .get("issued at")
            .verified("typed decision issue time"),
    );
    assert!(started.signed_duration_since(issued) >= chrono::TimeDelta::seconds(2));
}

#[when("the next licensing response is delayed by two seconds")]
async fn delay_response(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("HTTPS fixture started")
        .configure(|model| {
            model.script(nervix_licensing_test_service::Fault::Delay(
                Duration::from_secs(2),
            ));
        })
        .await;
}

#[when("licensing leadership moves to another node when one exists")]
async fn move_leader(world: &mut ScenarioWorld) {
    if world.cluster().node_ids().len() > 1 {
        let leader = super::super::running_leader_node(world).await;
        let successor = world
            .cluster()
            .node_ids()
            .into_iter()
            .find(|node| *node != leader)
            .verified("a multi-node cluster has another node");
        world
            .cluster()
            .move_leadership(&leader, &successor)
            .await
            .assured("public leadership transfer completes");
    }
}

#[then(expr = "the licensing service acknowledges transition placeholder {string}")]
async fn acknowledged(world: &mut ScenarioWorld, name: String) {
    let saved = saved_license(world, &name);
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let fields = current_license(world).await;
            if fields.get("acknowledged").map(String::as_str) == Some("true") {
                same_grant(&saved, &fields);
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("a signed acknowledgement reaches public inspection");
    let requests = world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .requests()
        .await;
    let expected_id = saved
        .get("transition id")
        .verified("saved transition identity");
    assert!(requests.iter().any(|request| matches!(request,
        nervix_licensing_protocol::requests::Request::Refresh { transitions, .. }
            if transitions.iter().any(|transition| transition.transition_id.to_string() == *expected_id
                && transition.started_at == public_license_timestamp(saved.get("started at").verified("saved start"))
                && transition.deadline == public_license_timestamp(saved.get("deadline").verified("saved deadline")))
    )), "refresh reports the complete committed transition");
}

#[when("the licensing administrator extends transitions by 600 seconds")]
async fn extend(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.extend_transitions(Duration::from_secs(600)))
        .await;
}

#[then(expr = "the licensing transition extends placeholder {string} without restarting")]
async fn extended(world: &mut ScenarioWorld, name: String) {
    let saved = saved_license(world, &name);
    let fields = current_license(world).await;
    for field in ["transition id", "started at", "usage at start"] {
        assert_eq!(fields.get(field), saved.get(field));
    }
    assert_eq!(
        public_license_timestamp(fields.get("deadline").verified("extended deadline")),
        public_license_timestamp(saved.get("deadline").verified("saved deadline"))
            + chrono::TimeDelta::seconds(600)
    );
    assert_eq!(fields.get("acknowledged").map(String::as_str), Some("true"));
}

#[when("a native session begins observing licensing transition notices")]
async fn observe_notices(world: &mut ScenarioWorld) {
    world.licensing_notice_client = Some(leader_license_client(world).await);
}

#[when(expr = "the licensing service sets the node quota to {int} with {int} seconds of grace")]
async fn node_quota(world: &mut ScenarioWorld, limit: u64, seconds: u64) {
    use nervix_licensing_protocol::conditions::*;
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| {
            let mut plan = model.conditions();
            plan.usage.quotas.insert(
                CLUSTER_NODES.to_owned(),
                ConditionValue::Known(Quota::Bounded { value: limit }),
            );
            plan.operating.durations.insert(
                QUOTA_GRACE.to_owned(),
                ConditionValue::Known(ConditionDuration::Bounded { seconds }),
            );
            model.plan(plan);
        })
        .await;
}

#[then("creating another domain cannot use a grant after activation expiry")]
async fn expiry_refuses_growth(world: &mut ScenarioWorld) {
    let client = leader_license_client(world).await;
    let command =
        super::super::expand_placeholders(world, "CREATE UNPACED DOMAIN {{domain}}_expired_grant;");
    let outcome = client
        .execute(&command)
        .await
        .assured("the known domain-creation command completes");
    assert!(
        !outcome.succeeded(),
        "a firm activation expiry cannot receive grace"
    );
    assert!(
        outcome.message.to_lowercase().contains("licens"),
        "a licensing cause refuses growth: {}",
        outcome.message
    );
}

#[when(expr = "the licensing transition is {string}")]
async fn acknowledgement_mode(world: &mut ScenarioWorld, mode: String) {
    match mode.as_str() {
        "pending" => {}
        "acknowledged" => {
            let client = leader_license_client(world).await;
            let outcome = client
                .execute("REFRESH LICENSE;")
                .await
                .assured("the cluster requests its acknowledgement");
            assert!(outcome.succeeded(), "{}", outcome.message);
        }
        _ => panic!("the scenario declares pending or acknowledged"),
    }
}
