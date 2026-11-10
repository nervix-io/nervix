//! Public observations of session bounds, control-plane cadences and idle forwarding links.
//!
//! Outside the layer order: a scenario harness.
//! Owns: advertised-bound, public cadence and mixed-link progress assertions.
//! Depends on: the native client, public gRPC codec, endpoints and fixture credentials.
//! Must not know: server settings, transport internals or the link heartbeat implementation.

use std::time::Duration;

use cucumber::{gherkin::Step, then, when};
use nervix_client_wire::{SessionLimits, grpc::ClientExchangeCodec};
use nervix_primitives::{stream::wrappers::ReceiverStream, sync::mpsc};
use tonic::{Request, codegen::http::uri::PathAndQuery};

use crate::{ScenarioWorld, common::raw_session::session_channel};

#[then(expr = "node {string} control-plane cadence metric for {string} eventually equals {float}")]
async fn cadence_metric(world: &mut ScenarioWorld, node: String, operation: String, expected: f64) {
    let cluster = world.cluster();
    let labels = [format!("operation=\"{operation}\"")];
    let mut observed = None;
    let checked = nervix_primitives::time::timeout(Duration::from_secs(60), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let reading = cluster
                .read_observability_metric(&node, "nervix_control_plane_cadence_seconds", &labels)
                .await;
            if let Ok(value) = &reading
                && (*value - expected).abs() < f64::EPSILON
            {
                return;
            }
            observed = Some(reading);
            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        checked.is_ok(),
        "node {node} does not report cadence {operation} = {expected}: {observed:?}"
    );
}

#[when(
    expr = "client {string} observes clock progress for at least {string} while producer {string} \
            and consumer {string} stay idle"
)]
async fn idle_links(
    world: &mut ScenarioWorld,
    client: String,
    duration: String,
    producer: String,
    consumer: String,
) {
    let client = world
        .transaction_clients
        .get(&client)
        .expect("a connected client");
    let producer = world.producers.native(&producer);
    let consumer = world.consumers.native(&consumer);
    let producer_id = producer.id();
    let consumer_id = consumer.id();
    let duration =
        nervix_models::parse_duration_text(&duration).expect("a bounded fixture duration");
    assert!(duration <= Duration::from_secs(30));
    let domain = nervix_models::DomainName::parse(&world.domain).expect("the scenario domain");
    let attached = client
        .attach_domain_clock(domain.clone())
        .await
        .expect("the running paced domain can be followed");
    assert!(matches!(
        attached.disposition,
        nervix_client_wire::DomainClockAttachDisposition::Attached { .. }
    ));
    let started = nervix_primitives::time::Instant::now();
    nervix_primitives::time::timeout(Duration::from_secs(90), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let event = client
                .next_domain_clock_event()
                .await
                .expect("continuous public clock progress");
            assert_eq!(event.domain(), &domain);
            match event {
                nervix_client_core::DomainClockEvent::Observed(_) => {}
                nervix_client_core::DomainClockEvent::Ticked(_) => {
                    if started.elapsed() >= duration {
                        break;
                    }
                }
                other => panic!("the session clock stays attached while links are idle: {other:?}"),
            }
        }
    })
    .await
    .expect("the public clock keeps advancing within the harness deadline");
    assert_eq!(
        producer.connection(),
        nervix_client_core::ProducerConnection::Active
    );
    assert_eq!(
        consumer.connection(),
        nervix_client_core::ConsumerConnection::Active
    );
    assert_eq!(
        producer.id(),
        producer_id,
        "an idle link does not replace the producer"
    );
    assert_eq!(
        consumer.id(),
        consumer_id,
        "an idle link does not replace the consumer"
    );
}

#[then(expr = "a native client on node {string} executes a command with {int} bytes of padding")]
async fn native_large_frame(world: &mut ScenarioWorld, node: String, bytes: usize) {
    let client = native_client(world, &node).await;
    let name = format!("session-bounds-{node}");
    assert!(
        world
            .transaction_clients
            .insert(name, client.clone())
            .is_none()
    );
    execute_padded(&client, bytes).await;
}

#[then(
    expr = "the same native client on node {string} executes a command with {int} bytes of padding"
)]
async fn reconnected_large_frame(world: &mut ScenarioWorld, node: String, bytes: usize) {
    let client = world
        .transaction_clients
        .get(&format!("session-bounds-{node}"))
        .expect("the first command retained its native client");
    client
        .list_domains()
        .await
        .expect("the same client recovers its closed session");
    execute_padded(client, bytes).await;
}

async fn execute_padded(client: &nervix_client_core::Client, bytes: usize) {
    let result = client.execute(padded_command(bytes)).await;
    let outcome = result.expect("the advertised frame limit holds the command");
    assert!(
        outcome.succeeded(),
        "the read command succeeds: {}",
        outcome.message
    );
}

#[then(
    expr = "the same native client on node {string} refuses {int} padding bytes above its {int} \
            byte frame limit"
)]
async fn native_frame_refusal(world: &mut ScenarioWorld, node: String, bytes: usize, limit: usize) {
    let client = world
        .transaction_clients
        .get(&format!("session-bounds-{node}"))
        .expect("the first command retained its native client");
    client
        .list_domains()
        .await
        .expect("the same client recovers its closed session");
    let error = client
        .execute(padded_command(bytes))
        .await
        .expect_err("the source exceeds the contacted node's frame limit");
    assert!(
        matches!(error.downcast_ref::<nervix_client_wire::WireEncodeError>(),
            Some(nervix_client_wire::WireEncodeError::FrameTooLarge { limit: actual, .. })
                if *actual == limit),
        "the native encoder must use the advertised limit: {error:#}",
    );
}

fn padded_command(bytes: usize) -> String {
    assert!(
        bytes <= 16 * 1024 * 1024,
        "the fixture bounds its source allocation"
    );
    format!("DESCRIBE /*{}*/ CLUSTER;", "x".repeat(bytes))
}

async fn native_client(world: &ScenarioWorld, node: &str) -> nervix_client_core::Client {
    let cluster = world
        .cluster
        .as_ref()
        .expect("the scenario started its cluster");
    let uri = cluster
        .grpc_uri(node)
        .expect("the named node has a session endpoint");
    let options = crate::common::cluster::client_connect_options(&uri)
        .expect("the fixture has the node's trust and credentials");
    nervix_client_core::Client::connect_with_options(&uri, None, options)
        .await
        .expect("the native client opens an authenticated session")
}

#[then(expr = "node {string} advertises session bounds at open")]
async fn session_bounds(world: &mut ScenarioWorld, node: String, #[step] step: &Step) {
    let cluster = world
        .cluster
        .as_ref()
        .expect("the scenario started its cluster");
    let uri = cluster
        .grpc_uri(&node)
        .expect("the named node has a session endpoint");
    let table = step
        .table
        .as_ref()
        .expect("the scenario supplies the complete bounds");
    assert_eq!(table.rows.len(), 2, "one row specifies the bounds");
    let channel = session_channel(&uri)
        .await
        .expect("the public session endpoint is reachable");
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.expect("the channel is ready");
    let (sender, outbound) = mpsc::channel(1);
    let mut request = Request::new(ReceiverStream::new(outbound));
    request.metadata_mut().insert(
        "authorization",
        crate::common::cluster::test_basic_authorization()
            .parse()
            .expect("the fixture's authorization is valid metadata"),
    );
    let response = nervix_primitives::time::timeout(
        Duration::from_secs(60),
        client.streaming(
            request,
            PathAndQuery::from_static(nervix_client_wire::grpc::EXCHANGE_PATH),
            ClientExchangeCodec::new(SessionLimits::DEFAULT),
        ),
    )
    .await
    .expect("the authenticated session responds within the harness bound")
    .expect("the fixture authenticates at session open");
    for (name, expected) in table.rows[0].iter().zip(&table.rows[1]) {
        let header = format!("nervix-session-{}", name.replace('_', "-"));
        let actual = response.metadata().get(&header);
        assert_eq!(
            actual.map(|value| value.to_str().expect("announced bounds are ASCII")),
            Some(expected.as_str()),
            "the session must announce {name} before receiving a request",
        );
    }
    drop(response);
    drop(sender);
}
