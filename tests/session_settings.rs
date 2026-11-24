//! Public observations of session bounds and pacing, control-plane cadences and idle forwarding
//! links.
//!
//! Outside the layer order: a scenario harness.
//! Owns: advertised-bound, announced-pacing, public cadence and mixed-link progress assertions.
//! Depends on: the native client, public gRPC and WebSocket codecs, endpoints and fixture
//! credentials.
//! Must not know: server settings, transport internals or the link heartbeat implementation.

use std::time::Duration;

use cucumber::{gherkin::Step, then, when};
use futures_util::StreamExt as _;
use nervix_client_wire::{SessionLimits, SessionOpened, SessionPacing, grpc::ClientExchangeCodec};
use nervix_primitives::{stream::wrappers::ReceiverStream, sync::mpsc};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest as _};
use tonic::{Request, codegen::http::uri::PathAndQuery};

use crate::{
    ScenarioWorld,
    common::{cluster::test_basic_auth_token_for_password, raw_session::session_channel},
};

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
    format!("DESCRIBE {}CLUSTER;", " ".repeat(bytes))
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

/// The text of the pacing value `name` names, as the scenario's table writes it: durations in the
/// edge's human form, an absent retry validity as `absent`, and the restore queue as its count.
fn announced_pacing_value(pacing: &SessionPacing, name: &str) -> String {
    let web_console = pacing.web_console();
    match name {
        "snapshot_interval" => nervix_models::timing_text(pacing.snapshot_interval()),
        "command_retry_validity" => match pacing.command_retry_validity() {
            Some(validity) => nervix_models::timing_text(validity),
            None => "absent".to_string(),
        },
        "web_console_request_timeout" => nervix_models::timing_text(web_console.request_timeout()),
        "web_console_connect_timeout" => nervix_models::timing_text(web_console.connect_timeout()),
        "web_console_retry_backoff" => nervix_models::timing_text(web_console.retry_backoff()),
        "web_console_retry_max_backoff" => {
            nervix_models::timing_text(web_console.retry_max_backoff())
        }
        "web_console_restoration_backoff" => {
            nervix_models::timing_text(web_console.restoration_backoff())
        }
        "web_console_restore_frame_capacity" => web_console.restore_frame_capacity().to_string(),
        other => panic!("the scenario names an unknown pacing value '{other}'"),
    }
}

/// Opens an authenticated console session on the named node's console listener and checks that
/// its first frame announces exactly the pacing the table states.
#[then(expr = "node {string} announces session pacing when a console session opens")]
async fn console_session_pacing(world: &mut ScenarioWorld, node: String, #[step] step: &Step) {
    let cluster = world
        .cluster
        .as_ref()
        .expect("the scenario started its cluster");
    let table = step
        .table
        .as_ref()
        .expect("the scenario supplies the complete pacing");
    assert_eq!(table.rows.len(), 2, "one row specifies the pacing");
    let console = cluster
        .web_console_url(&node)
        .expect("the named node serves a console");
    let mut address = url::Url::parse(&console).expect("the harness builds a valid console URL");
    address.set_query(None);
    address.set_path("/console/ws");
    address
        .set_scheme("ws")
        .expect("an http console URL can take the ws scheme");
    let token = test_basic_auth_token_for_password(crate::common::cluster::TEST_AUTH_PASSWORD);
    address.query_pairs_mut().append_pair("auth", &token);
    let request = address
        .as_str()
        .into_client_request()
        .expect("the console session address is a valid request");
    let (mut socket, _) = nervix_primitives::time::timeout(
        Duration::from_secs(60),
        tokio_tungstenite::connect_async(request),
    )
    .await
    .expect("the console session upgrades within the harness bound")
    .expect("the fixture authenticates the console session");
    let first = nervix_primitives::time::timeout(Duration::from_secs(60), socket.next())
        .await
        .expect("the console session announces its opening within the harness bound");
    let Some(Ok(Message::Binary(payload))) = first else {
        panic!("a console session starts with a binary opening, got {first:?}");
    };
    let opened = SessionOpened::verify(payload.into())
        .expect("the console session's opening is complete and valid");
    for (name, expected) in table.rows[0].iter().zip(&table.rows[1]) {
        assert_eq!(
            announced_pacing_value(&opened.pacing, name),
            *expected,
            "the console session must announce {name} at its opening",
        );
    }
    socket
        .close(None)
        .await
        .expect("the harness closes the console session it opened");
}
