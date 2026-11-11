//! Public notification observations through signed HTTPS peers, sessions and metrics.
//!
//! Layer: test harness.
//! - **Owns.** Notification fault controls and public committed-revision assertions.
//! - **Depends on.** The scenario fixture and public native client/metrics.
//! - **Must not know.** Coordinator tasks, stream parsing or policy internals.

use std::time::Duration;

use cucumber::{given, then, when};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_licensing_test_service::{Model, StreamMode};
use nervix_primitives::time::{sleep, timeout};

use super::{STATUS_WAIT_BUDGET, ScenarioWorld, current_license, public_license_timestamp};

#[given("the next licensing response is held at the service")]
async fn hold_response(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(Model::hold_next_response)
        .await;
}

#[then("the licensing refresh response is held after its decision is issued")]
async fn response_held(world: &mut ScenarioWorld) {
    let service = world.licensing_service.as_ref().verified("fixture started");
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if service.configure(|model| model.response_is_held()).await {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("the peer issued the signed decision before holding its response");
    let requests = service.requests().await;
    let Some(nervix_licensing_protocol::requests::Request::Refresh {
        state: nervix_licensing_protocol::requests::RefreshState::Committed(applied),
        ..
    }) = requests.last()
    else {
        panic!("the held request must refresh an established activation");
    };
    let current = current_license(world).await;
    assert_eq!(
        current.get("decision revision"),
        Some(&applied.applied_decision_revision.to_string()),
        "the signed response remains uncommitted while the peer holds it"
    );
    assert!(
        public_license_timestamp(
            current
                .get("refresh after")
                .verified("signed refresh deadline")
        ) > chrono::Utc::now(),
        "normal polling is not due during the gated notification interleaving"
    );
    let observations = service.configure(|model| model.fault_observations()).await;
    let held = observations.last().verified("the held request is recorded");
    assert!(matches!(
        held.fault,
        nervix_licensing_test_service::Fault::Hold(_)
    ));
    assert_eq!(
        held.request_id,
        requests
            .last()
            .verified("a refresh arrived")
            .common()
            .request_id
    );
    world
        .placeholders
        .insert("held_refresh_request".into(), held.request_id.to_string());
}

#[when("the held licensing response is released")]
async fn release_response(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(Model::release_response)
        .await;
}

#[then("only the held refresh and the explicit replay reached the licensing service")]
async fn no_redundant_refresh(world: &mut ScenarioWorld) {
    use nervix_licensing_protocol::requests::Request;
    use nervix_licensing_test_service::{Fault, Tamper};

    let service = world.licensing_service.as_ref().verified("fixture started");
    let requests = service.requests().await;
    let refreshes: Vec<_> = requests
        .iter()
        .filter(|request| matches!(request, Request::Refresh { .. }))
        .collect();
    let observations = service.configure(|model| model.fault_observations()).await;
    let fault_requests: Vec<_> = observations
        .iter()
        .map(|observation| (observation.request_id, &observation.fault))
        .collect();
    assert_eq!(
        refreshes.len(),
        2,
        "a sync reporting the in-flight decision must be covered by that refresh; \
         requests={requests:?}; faults={fault_requests:?}"
    );
    assert_eq!(
        refreshes[0].common().request_id.to_string(),
        *world
            .placeholders
            .get("held_refresh_request")
            .verified("held request was observed")
    );
    let replay = observations
        .last()
        .verified("the replay fault was consumed");
    assert!(matches!(replay.fault, Fault::Tamper(Tamper::PriorDecision)));
    assert_eq!(replay.request_id, refreshes[1].common().request_id);
}

#[given("the next licensing rotation response is delayed")]
async fn delay_rotation(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| {
            model.script(nervix_licensing_test_service::Fault::Delay(
                Duration::from_secs(5),
            ));
        })
        .await;
}

#[then("the licensing rotation request has reached the service")]
async fn rotation_started(world: &mut ScenarioWorld) {
    let service = world.licensing_service.as_ref().verified("fixture started");
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if service.requests().await.iter().any(|request| {
                matches!(
                    request,
                    nervix_licensing_protocol::requests::Request::RotateKey { .. }
                )
            }) {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("the delayed response leaves an admitted rotation in flight");
}

#[given(expr = "the licensing notification stream is {string}")]
#[when(expr = "the licensing notification stream is {string}")]
async fn stream_mode(world: &mut ScenarioWorld, mode: String) {
    let mode = match mode.as_str() {
        "normal" => StreamMode::Normal,
        "withholding events" => StreamMode::Withhold,
        "blocked" => StreamMode::Blocked,
        "oversized" => StreamMode::Oversized,
        "stale sync" => StreamMode::StaleSync,
        _ => panic!("undeclared fixture stream mode"),
    };
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.notification_mode(mode))
        .await;
}

#[given("the licensing service duplicates notification events")]
async fn duplicate(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.duplicate_notifications(true))
        .await;
}

#[when("the licensing notification stream is disconnected")]
async fn disconnect(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| model.disconnect_notifications())
        .await;
}

#[then("the licensing service holds exactly one authenticated notification stream")]
async fn one_stream(world: &mut ScenarioWorld) {
    let service = world.licensing_service.as_ref().verified("fixture started");
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if service
                .configure(|model| model.notification_stream_ready())
                .await
            {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("only the licensed leader retains its authenticated HTTPS stream");
}

#[then("the licensing notification reconnect uses a fresh ticket and its last event identifier")]
async fn reconnect_cursor(world: &mut ScenarioWorld) {
    let service = world.licensing_service.as_ref().verified("fixture started");
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let observations = service
                .configure(|model| model.notification_observations())
                .await;
            if observations.len() >= 2 {
                assert!(
                    observations[1..]
                        .iter()
                        .any(|observation| observation.last_event.is_some())
                );
                let ids: std::collections::BTreeSet<_> = observations
                    .iter()
                    .map(|observation| observation.request_id)
                    .collect();
                assert_eq!(
                    ids.len(),
                    observations.len(),
                    "each single-use ticket needs a new identity"
                );
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("notification stream reconnects through the authenticated route");
}

#[then("the idle licensing stream reconnects without inventing an event identifier")]
#[then("the oversized licensing stream reconnects without inventing an event identifier")]
async fn idle_reconnect(world: &mut ScenarioWorld) {
    let service = world.licensing_service.as_ref().verified("fixture started");
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let observations = service
                .configure(|model| model.notification_observations())
                .await;
            if observations.len() >= 2 {
                assert!(
                    observations
                        .iter()
                        .all(|observation| observation.last_event.is_none())
                );
                let ids: std::collections::BTreeSet<_> = observations
                    .iter()
                    .map(|observation| observation.request_id)
                    .collect();
                assert_eq!(ids.len(), observations.len());
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("the committed idle budget closes a silent response and obtains a fresh ticket");
}

#[when("the licensing service changes the domain quota to 12")]
async fn change_plan(world: &mut ScenarioWorld) {
    let count = world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .requests()
        .await
        .iter()
        .filter(|request| {
            matches!(
                request,
                nervix_licensing_protocol::requests::Request::Refresh { .. }
            )
        })
        .count();
    world
        .placeholders
        .insert("notification_refresh_count".into(), count.to_string());
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(update_domain_quota)
        .await;
}

fn update_domain_quota(model: &mut Model) {
    let mut plan = model.conditions();
    plan.usage.quotas.insert(
        "cluster.domains".to_owned(),
        nervix_licensing_protocol::conditions::ConditionValue::Known(
            nervix_licensing_protocol::conditions::Quota::Bounded { value: 12 },
        ),
    );
    model.plan(plan);
}

#[when("the licensing service temporarily refuses refresh requests")]
async fn temporary_refusal(world: &mut ScenarioWorld) {
    let service = world.licensing_service.as_ref().verified("fixture started");
    let count = service.requests().await.len();
    world
        .placeholders
        .insert("foreground_refresh_baseline".into(), count.to_string());
    service
        .configure(|model| {
            model.refuse(Some(
                nervix_licensing_protocol::problems::ProblemKind::Unavailable {
                    retry_after_seconds: 1,
                },
            ));
        })
        .await;
}

#[then("the foreground license refresh has reached its retryable refusal")]
async fn foreground_refusal(world: &mut ScenarioWorld) {
    let baseline = world
        .placeholders
        .get("foreground_refresh_baseline")
        .verified("the refusal saved its request baseline")
        .parse::<usize>()
        .assured("bounded request count");
    let service = world.licensing_service.as_ref().verified("fixture started");
    timeout(STATUS_WAIT_BUDGET, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if service.requests().await[baseline..].iter().any(|request| {
                matches!(
                    request,
                    nervix_licensing_protocol::requests::Request::Refresh { .. }
                )
            }) {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured("the signed foreground refresh reaches the refusing HTTPS service");
}

#[when("the licensing service resumes with a domain quota of 12")]
async fn resume_with_quota(world: &mut ScenarioWorld) {
    world
        .licensing_service
        .as_ref()
        .verified("fixture started")
        .configure(|model| {
            model.refuse(None);
            update_domain_quota(model);
        })
        .await;
}

#[then("the notification commits one verified refresh before the polling deadline")]
async fn refreshed(world: &mut ScenarioWorld) {
    timeout(Duration::from_secs(30), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let current = current_license(world).await;
            if current.get("quota cluster.domains").map(String::as_str) == Some("0 of 12") {
                assert_eq!(
                    current.get("verification").map(String::as_str),
                    Some("verified")
                );
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .assured(
        "a notification refresh commits within the request budget without advancing either clock",
    );
    let current = current_license(world).await;
    let refresh_after = public_license_timestamp(
        current
            .get("refresh after")
            .verified("verified timing is public"),
    );
    assert!(
        refresh_after > chrono::Utc::now(),
        "this was a notification, not an already-due poll"
    );
    let service = world.licensing_service.as_ref().verified("fixture started");
    assert!(
        service
            .configure(|model| model.duplicate_hints_were_sent())
            .await,
        "the duplicate scenario must deliver both identical hints through real SSE"
    );
    let count = service
        .requests()
        .await
        .iter()
        .filter(|request| {
            matches!(
                request,
                nervix_licensing_protocol::requests::Request::Refresh { .. }
            )
        })
        .count();
    let before = world
        .placeholders
        .get("notification_refresh_count")
        .verified("change retained its baseline")
        .parse::<usize>()
        .assured("count");
    assert_eq!(
        count,
        before.checked_add(1).assured("bounded scenario count")
    );
}
