//! Controlled subscription reopening and acknowledged input through the public transports.
//!
//! Layer: test harness.
//! - **Owns.** A client's explicit request timeout, one paused subscription-opening reply, and
//!   one acknowledged input for a restoration's exact row assertion.
//! - **Depends on.** The scenario cluster, the native client and the shared fault harness.
//! - **Must not know.** Private client restoration state or server subscription internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "scenario fixtures remain outside product execution")
)]

use cucumber::{given, then, when};
use nervix_client_core::{SubscriptionEvent, SubscriptionLifecycle};
use nervix_models::SubscriptionName;

use super::*;

#[given(expr = "client {string} is connected to node {string} with request timeout {string}")]
async fn connect_with_request_timeout(
    world: &mut ScenarioWorld,
    name: String,
    node_id: String,
    timeout: String,
) {
    let name = expand_placeholders(world, &name);
    let node_id = expand_placeholders(world, &node_id);
    let grpc_uri = world
        .cluster()
        .grpc_uri(&node_id)
        .assured("the node serves gRPC");
    connect_to_endpoint_with_request_timeout(world, name, grpc_uri, timeout).await;
}

#[given(expr = "client {string} is connected to {string} with request timeout {string}")]
async fn connect_to_endpoint_with_request_timeout(
    world: &mut ScenarioWorld,
    name: String,
    grpc_uri: String,
    timeout: String,
) {
    let name = expand_placeholders(world, &name);
    let grpc_uri = expand_placeholders(world, &grpc_uri);
    let mut options =
        client_connect_options(&grpc_uri).assured("the fixture's TLS options are valid");
    options.request_timeout = parse_duration_text(&timeout).assured("a valid request timeout");
    for node in world.cluster().node_ids() {
        let uri = world
            .cluster()
            .grpc_uri(&node)
            .assured("the seed serves gRPC");
        options
            .seed_servers
            .push(url::Url::parse(&uri).assured("a gRPC endpoint is a URL"));
    }
    let client = Client::connect_with_options(&grpc_uri, client_domain(&world.domain), options)
        .await
        .assured("the native client connects with its ordinary connection budget");
    assert!(world.transaction_clients.insert(name, client).is_none());
}

#[given(expr = "the next subscription opening reply on node {string} is held")]
fn hold_subscription_reply(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .pause_subscription_response_delivery_on(crate::common::cluster::node_name(&node_id));
}

#[then(expr = "the subscription opening reply on node {string} is held")]
async fn subscription_reply_is_held(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    let node = crate::common::cluster::node_name(&node_id);
    nervix_primitives::time::timeout(
        Duration::from_secs(30),
        world
            .fault_injection
            .wait_for_subscription_response_delivery_pause(&node),
    )
    .await
    .assured("the reopening reached the controlled response boundary");
}

#[when(expr = "the subscription opening reply on node {string} is released")]
fn release_subscription_reply(world: &mut ScenarioWorld, node_id: String) {
    let node_id = expand_placeholders(world, &node_id);
    world
        .fault_injection
        .release_subscription_response_delivery_pause(&crate::common::cluster::node_name(&node_id));
}

#[when(
    expr = "one http payload is posted with a completed graph acknowledgement to node {string} \
            host {string} path {string}"
)]
async fn post_one_acknowledged_payload(
    world: &mut ScenarioWorld,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    world
        .cluster()
        .acknowledged_http_publisher(&node_id, &host, &path, Duration::from_secs(30))
        .publish_once(&payload)
        .await
        .assured("the one input must reach its relay consumers before its delivery is asserted");
}

#[when(
    expr = "within {string} client {string} receives one subscription row from one acknowledged \
            http post to node {string} host {string} path {string}"
)]
async fn one_acknowledged_post_and_one_subscription_row(
    world: &mut ScenarioWorld,
    duration: String,
    client_name: String,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let duration = parse_duration_text(&duration).assured("a valid delivery bound");
    let client_name = expand_placeholders(world, &client_name);
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let client = world
        .transaction_clients
        .get(&client_name)
        .assured("the preceding step connected this client")
        .clone();
    let publisher = world
        .cluster()
        .acknowledged_http_publisher(&node_id, &host, &path, duration);
    let rows = nervix_primitives::time::timeout(duration, async {
        let receiving = async {
            loop {
                nervix_primitives::task::consume_budget().await;
                match client
                    .next_subscription()
                    .await
                    .assured("the subscription remains recoverable")
                {
                    SubscriptionEvent::Rows(rows) => return rows,
                    SubscriptionEvent::Interrupted(_) => {}
                    SubscriptionEvent::RestorationFailed(failure) => {
                        append_cucumber_log_line(&format!(
                            "subscription reopening will retry before row delivery: {failure:?}"
                        ));
                    }
                    event => panic!("the restored subscription produced {event:?}"),
                }
            }
        };
        let (posted, rows) = tokio::join!(publisher.publish_once(&payload), receiving);
        posted.assured("the single post completed its graph acknowledgement");
        rows
    })
    .await
    .assured("one acknowledged post and one row complete within the existing delivery bound");
    let mut lines = rows.display_lines().assured("the received row renders");
    let handle = rows.rows.subscription();
    assert_eq!(
        client.subscription_lifecycle(&handle.name),
        Some(SubscriptionLifecycle::Active(handle.clone())),
        "the row belongs to the acknowledged active subscription"
    );
    assert_eq!(
        lines.len(),
        1,
        "one post produces exactly one subscription row"
    );
    world.last_subscription_payload = Some(lines.remove(0));
}

/// How long a restoration and the one row after it may take together: the reopening waits for its
/// interest to be visible on every live node, and the row crosses nodes on three.
const RESTORED_DELIVERY_WAIT: Duration = Duration::from_secs(60);

/// Drives the restoration of `subscription` after its exchange ended, waits until the client
/// reports it active on the new session, and only then posts one acknowledged payload, whose row
/// the restored subscription must deliver within [`RESTORED_DELIVERY_WAIT`]. A row published
/// before the restoration completes falls in the gap [`SubscriptionEvent::Interrupted`] reports,
/// because reopening starts at the new session's live edge, so the post waits for the restoration
/// rather than racing it.
#[when(
    expr = "client {string} restores subscription {string} and receives one subscription row from \
            one acknowledged http post to node {string} host {string} path {string}"
)]
async fn restored_reader_receives_one_post(
    world: &mut ScenarioWorld,
    client_name: String,
    subscription: String,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let duration = RESTORED_DELIVERY_WAIT;
    let client_name = expand_placeholders(world, &client_name);
    let subscription = SubscriptionName::parse(&subscription).assured("a valid subscription name");
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let client = world
        .transaction_clients
        .get(&client_name)
        .assured("the preceding step connected this client")
        .clone();
    // Reading drives the restoration: the reader opens the next session and reopens the
    // subscription there.
    let reading = client.clone();
    let reader = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
        loop {
            nervix_primitives::task::consume_budget().await;
            match reading
                .next_subscription()
                .await
                .assured("the subscription reader remains recoverable")
            {
                SubscriptionEvent::Rows(rows) => return rows,
                SubscriptionEvent::Interrupted(_) => {}
                SubscriptionEvent::RestorationFailed(failure) => {
                    append_cucumber_log_line(&format!(
                        "subscription restoration will retry before row delivery: {failure:?}"
                    ));
                }
                event => panic!("the restored subscription produced {event:?}"),
            }
        }
    }));
    let publisher = world
        .cluster()
        .acknowledged_http_publisher(&node_id, &host, &path, duration);
    let rows = nervix_primitives::time::timeout(duration, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if let Some(SubscriptionLifecycle::Active(_)) =
                client.subscription_lifecycle(&subscription)
            {
                break;
            }
            nervix_primitives::time::sleep(Duration::from_millis(50)).await;
        }
        publisher
            .publish_once(&payload)
            .await
            .assured("the single post completes its graph acknowledgement");
        reader.await.assured("the subscription reader completes")
    })
    .await
    .assured("the subscription is restored and delivers the one post within the bound");
    let mut lines = rows.display_lines().assured("the received row renders");
    let handle = rows.rows.subscription();
    assert_eq!(
        client.subscription_lifecycle(&handle.name),
        Some(SubscriptionLifecycle::Active(handle.clone())),
        "the row belongs to the restored active subscription"
    );
    assert_eq!(
        lines.len(),
        1,
        "one post produces exactly one subscription row"
    );
    world.last_subscription_payload = Some(lines.remove(0));
}

#[when(
    expr = "client {string} waits through the held reopening of subscription {string} on node \
            {string} then receives one http payload with host {string} path {string}"
)]
async fn waiting_reader_recovers_before_one_post(
    world: &mut ScenarioWorld,
    client_name: String,
    subscription: String,
    node_id: String,
    host: String,
    path: String,
    #[step] step: &Step,
) {
    let client_name = expand_placeholders(world, &client_name);
    let subscription = SubscriptionName::parse(&subscription).assured("a valid subscription name");
    let node_id = expand_placeholders(world, &node_id);
    let host = expand_placeholders(world, &host);
    let path = expand_placeholders(world, &path);
    let payload = expand_placeholders(world, docstring(step));
    let client = world
        .transaction_clients
        .get(&client_name)
        .assured("the preceding step connected this named client")
        .clone();
    // The node still serves the held reopening, so it would keep the reader's wait alive with
    // progress reports. Withholding them makes the node silent, which is what lets the reader give
    // the reopening up and open another session.
    let held_node = crate::common::cluster::node_name(&node_id);
    world
        .fault_injection
        .withhold_request_progress_on(held_node.clone());
    let reading = client.clone();
    let task = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
        loop {
            nervix_primitives::task::consume_budget().await;
            match reading
                .next_subscription()
                .await
                .assured("the subscription reader remains recoverable")
            {
                SubscriptionEvent::Rows(rows) => return rows,
                SubscriptionEvent::Interrupted(_) => {}
                SubscriptionEvent::RestorationFailed(failure) => {
                    append_cucumber_log_line(&format!(
                        "controlled subscription reopening will retry: {failure:?}"
                    ));
                }
                event => panic!("the controlled reopening produced an unexpected event: {event:?}"),
            }
        }
    }));
    let recovered = nervix_primitives::time::timeout(Duration::from_secs(30), async {
        world
            .fault_injection
            .wait_for_subscription_response_delivery_pause(&crate::common::cluster::node_name(
                &node_id,
            ))
            .await;
        append_cucumber_log_line(&format!(
            "subscription reopening reply held: domain={} client={client_name} node={node_id} \
             lifecycle={:?}",
            world.domain,
            client.subscription_lifecycle(&subscription),
        ));
        let mut previous = None;
        loop {
            nervix_primitives::task::consume_budget().await;
            let lifecycle = client.subscription_lifecycle(&subscription);
            if lifecycle != previous {
                append_cucumber_log_line(&format!(
                    "subscription reopening observed: domain={} client={client_name} \
                     lifecycle={lifecycle:?}",
                    world.domain,
                ));
                previous = lifecycle.clone();
            }
            if matches!(lifecycle, Some(SubscriptionLifecycle::Active(_))) {
                break;
            }
            nervix_primitives::time::sleep(Duration::from_millis(50)).await;
        }
        world
            .cluster()
            .acknowledged_http_publisher(&node_id, &host, &path, Duration::from_secs(30))
            .publish_once(&payload)
            .await
            .assured("the single payload completes its graph acknowledgement");
        task.await
            .assured("the waiting subscription reader completes")
    })
    .await
    .assured(
        "the waiting reader restores the subscription and receives the one post within thirty \
         seconds",
    );
    world
        .fault_injection
        .restore_request_progress_on(&held_node);
    let mut lines = recovered
        .display_lines()
        .assured("the received row renders");
    assert_eq!(
        client.subscription_lifecycle(&subscription),
        Some(SubscriptionLifecycle::Active(
            recovered.rows.subscription().clone()
        )),
        "the row belongs to the acknowledged replacement subscription"
    );
    assert_eq!(
        lines.len(),
        1,
        "one post produces exactly one subscription row"
    );
    world.last_subscription_payload = Some(lines.remove(0));
}
