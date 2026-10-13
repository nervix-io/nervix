//! Steps that drive deduplicator and window scenarios across a backup and restore.
//!
//! Layer: test harness.
//! - **Owns.** The shared domain text a feature saves once, the instants a scenario measures
//!   deduplicator expiry from, and the posts that wait for a restored endpoint or a key's expiry.
//! - **Depends on.** The scenario world's placeholders, the cluster's HTTP publishing, and the
//!   active session's relay subscriptions.
//! - **Must not know.** How the server archives, installs or restores branch-local state.

use chrono::{DateTime, Utc};
use cucumber::{given, then, when};

use super::*;

/// How long one post waits for the subscription to deliver what it admitted.
const DELIVERY_WINDOW: Duration = Duration::from_millis(350);

/// How long a repeated post waits before posting again.
const REPOST_INTERVAL: Duration = Duration::from_millis(100);

#[given(expr = "these NSPL commands are saved as placeholder {string}")]
fn given_nspl_commands_are_saved_as_placeholder(
    world: &mut ScenarioWorld,
    placeholder: String,
    #[step] step: &Step,
) {
    // The saved text names the scenario's domain and test identity, so they exist from here on.
    initialize_scenario_identity(world);
    let commands = expand_placeholders(world, docstring(step));
    world.placeholders.insert(placeholder, commands);
}

#[given(expr = "the current time is saved as timestamp placeholder {string}")]
#[when(expr = "the current time is saved as timestamp placeholder {string}")]
fn given_current_time_is_saved(world: &mut ScenarioWorld, placeholder: String) {
    world
        .placeholders
        .insert(placeholder, Utc::now().to_rfc3339());
}

/// The instant a timestamp placeholder holds.
fn saved_instant(world: &ScenarioWorld, placeholder: &str) -> DateTime<Utc> {
    let saved = world
        .placeholders
        .get(placeholder)
        .unwrap_or_else(|| panic!("timestamp placeholder '{placeholder}' is not defined"));
    DateTime::parse_from_rfc3339(saved)
        .unwrap_or_else(|error| {
            panic!("timestamp placeholder '{placeholder}' is not RFC 3339 ({saved}): {error}")
        })
        .with_timezone(&Utc)
}

/// The step's duration text as the signed interval a wall-clock instant is moved by.
fn wall_interval(duration: &str) -> chrono::Duration {
    let duration = parse_duration_text(duration).expect("step duration must be a valid duration");
    chrono::Duration::from_std(duration).expect("a scenario interval fits a wall-clock interval")
}

/// Waits until the wall clock has advanced the given interval past a saved instant. The wait ends
/// as soon as the condition holds, so it costs nothing when the scenario already took that long.
#[given(expr = "at least {string} have passed since timestamp placeholder {string}")]
async fn given_interval_has_passed(
    world: &mut ScenarioWorld,
    interval: String,
    placeholder: String,
) {
    let due = saved_instant(world, &placeholder) + wall_interval(&interval);
    loop {
        nervix_primitives::task::consume_budget().await;
        let now = Utc::now();
        if now >= due {
            return;
        }
        let remaining = (due - now)
            .to_std()
            .expect("an instant still ahead of now is a positive interval");
        nervix_primitives::time::sleep(remaining.min(Duration::from_millis(500))).await;
    }
}

#[then(expr = "within {string} node {string} accepts http payload for host {string} path {string}")]
async fn then_node_accepts_http_payload_within(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;
    loop {
        nervix_primitives::task::consume_budget().await;
        let posted = world
            .cluster()
            .publish_http(&node_id, &host, &path, &payload)
            .await;
        let Err(error) = posted else {
            return;
        };
        assert!(
            Instant::now() < deadline,
            "node '{node_id}' did not accept http payloads for host '{host}' path '{path}' within \
             its budget: {error}"
        );
        nervix_primitives::time::sleep(REPOST_INTERVAL).await;
    }
}

/// Posts the same payload until the subscription delivers it, and saves when that happened.
#[then(
    expr = "within {string} repeatedly posting http payload to node {string} with host {string} \
            path {string} yields a relay subscription payload saved as timestamp placeholder \
            {string}"
)]
async fn then_repeated_payload_is_delivered(
    world: &mut ScenarioWorld,
    duration: String,
    node_id: String,
    host: String,
    path: String,
    placeholder: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).expect("step duration must be a valid duration");
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let deadline = Instant::now() + duration;
    loop {
        nervix_primitives::task::consume_budget().await;
        world
            .cluster()
            .publish_http(&node_id, &host, &path, &payload)
            .await
            .expect("the repeated http payload is accepted");
        if try_capture_any_subscription_payload(world, DELIVERY_WINDOW).await {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for http payload posted to host '{host}' path '{path}' to reach \
             the relay subscription"
        );
        nervix_primitives::time::sleep(REPOST_INTERVAL).await;
    }
    world
        .placeholders
        .insert(placeholder, Utc::now().to_rfc3339());
}

/// Checks that an observed delivery did not precede the original key's retention interval.
/// The archive comparison checks the retained timestamp exactly; subscription receipt can lag
/// the server's expiration when a loaded suite delays the next probe or its response.
#[then(
    expr = "timestamp placeholder {string} is at least {string} after timestamp placeholder \
            {string}"
)]
fn then_timestamp_is_at_least(
    world: &mut ScenarioWorld,
    measured: String,
    interval: String,
    origin: String,
) {
    let measured = saved_instant(world, &measured);
    let origin = saved_instant(world, &origin);
    let earliest = origin + wall_interval(&interval);
    assert!(
        measured >= earliest,
        "the measured instant {measured} is before {earliest}"
    );
}
