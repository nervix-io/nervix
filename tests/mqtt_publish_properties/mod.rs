//! Layer: test harness, outside product execution.
//! - **Owns.** A real MQTT 5 subscription and complete assertions on its delivered envelope.
//! - **Depends on.** The scenario endpoint, MQTT driver and bounded channel/task primitives.
//! - **Must not know.** Nervix emitter internals, acknowledgement state or packet preparation.

use cucumber::{given, then};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use nervix_recovery::NoReceiver as _;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, Publish, QoS, SessionMode};

use crate::{BROKER_TIMEOUT, MQTT_ADDR, ScenarioWorld, expand_placeholders};

mod endpoint;
pub(super) use endpoint::Endpoint;

pub(super) struct Observer {
    messages: mpsc::Receiver<Publish>,
    task: JoinHandle<()>,
}

impl Observer {
    async fn subscribe(world: &ScenarioWorld, topic: String) -> Self {
        let endpoint = world
            .dependencies
            .endpoints()
            .get(MQTT_ADDR)
            .assured("the scenario starts MQTT");
        let address = url::Url::parse(endpoint).assured("the fixture supplies an MQTT URL");
        let host = address
            .host_str()
            .assured("the broker address names a host");
        let port = address
            .port()
            .assured("the broker address names its mapped port");
        let client_id = format!("properties-observer-{}", world.test_id);
        let mut options = MqttOptions::new(client_id, (host, port));
        options.set_session_mode(SessionMode::Clean);
        let (client, mut events) = AsyncClient::builder(options).capacity(16).build();
        client
            .subscribe(topic.as_str(), QoS::ExactlyOnce)
            .await
            .assured("the observer queues its subscription");
        let (messages, receiver) = mpsc::channel(16);
        let (ready, subscribed) = oneshot::channel();
        let task = nervix_primitives::task::spawn(async move {
            let _client = client;
            let mut ready = Some(ready);
            loop {
                nervix_primitives::task::consume_budget().await;
                match events
                    .poll()
                    .await
                    .assured("the observer keeps its broker connection")
                {
                    Event::Incoming(Incoming::SubAck(_)) => {
                        if let Some(ready) = ready.take() {
                            ready.send(()).assured("the subscribe caller still waits");
                        }
                    }
                    Event::Incoming(Incoming::Publish(publish)) => {
                        messages
                            .send(publish)
                            .await
                            .means_peer_left("the MQTT envelope observer unsubscribed");
                    }
                    _ => {}
                }
            }
        });
        let observer = Self {
            messages: receiver,
            task,
        };
        timeout(BROKER_TIMEOUT, subscribed)
            .await
            .assured("the broker confirms the subscription within the observation bound")
            .assured("the subscription task remains alive");
        observer
    }

    async fn next(&mut self) -> Publish {
        timeout(BROKER_TIMEOUT, self.messages.recv())
            .await
            .assured("the publication reaches the observer within the observation bound")
            .assured("the observer stays connected")
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[given(expr = "MQTT 5 topic {string} is observed with its complete envelope")]
async fn observe(world: &mut ScenarioWorld, topic: String) {
    // End the preceding subscription before replacing its clean-session client identity.
    drop(world.mqtt_envelope_observer.take());
    let topic = expand_placeholders(world, &topic);
    world.mqtt_envelope_observer = Some(Observer::subscribe(world, topic).await);
}

#[then(expr = "the MQTT 5 observer receives the complete publication with retain {word}")]
async fn assert_publication(world: &mut ScenarioWorld, retain: String) {
    let response_topic = expand_placeholders(world, "responses/{{test_id}}");
    let publish = world
        .mqtt_envelope_observer
        .as_mut()
        .assured("the scenario subscribed before waiting")
        .next()
        .await;
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&publish.payload)
            .assured("the codec writes JSON"),
        serde_json::json!({"user_id":42})
    );
    assert_eq!(
        publish.retain,
        retain
            .parse::<bool>()
            .assured("the feature names a Boolean")
    );
    let properties = publish
        .properties
        .assured("the statement supplies MQTT 5 properties");
    assert_eq!(properties.content_type.as_deref(), Some("application/json"));
    assert_eq!(
        properties.response_topic.as_deref(),
        Some(response_topic.as_str())
    );
    assert_eq!(
        properties.correlation_data.as_deref(),
        Some(&[0_u8, 255, 16][..])
    );
    let remaining = properties
        .message_expiry_interval
        .assured("the statement sets message expiry");
    assert!(
        (1..=600).contains(&remaining),
        "the broker may decrement the declared 600 seconds"
    );
    assert_eq!(
        properties.user_properties,
        vec![
            ("trace".to_owned(), "first".to_owned()),
            ("trace".to_owned(), "second".to_owned()),
        ]
    );
}

#[given(expr = "an MQTT publication endpoint is configured for {word}")]
async fn configure_endpoint(world: &mut ScenarioWorld, purpose: String) {
    let (maximum, drop_first) = match purpose.as_str() {
        "retry" => (None, true),
        "size" => (Some(1024), false),
        _ => panic!("the feature selects retry or size"),
    };
    let endpoint = world
        .dependencies
        .endpoints()
        .get(MQTT_ADDR)
        .assured("the scenario starts MQTT");
    let mut address = url::Url::parse(endpoint).assured("the broker endpoint is a URL");
    let ip = address
        .host_str()
        .assured("the broker names its host")
        .parse()
        .assured("the test broker is addressed by its IP");
    let port = address.port().assured("the broker names its mapped port");
    let endpoint = Endpoint::start(std::net::SocketAddr::new(ip, port), maximum, drop_first)
        .await
        .assured("the scenario can bind a loopback forwarder");
    address
        .set_host(Some("127.0.0.1"))
        .assured("loopback is a URL host");
    address
        .set_port(Some(endpoint.port()))
        .assured("MQTT accepts an explicit port");
    world
        .placeholders
        .insert("mqtt_publication_addr".to_owned(), address.to_string());
    world.mqtt_publication_endpoint = Some(endpoint);
}

#[then("the MQTT publication retry carries the identical prepared envelope")]
async fn assert_retry(world: &mut ScenarioWorld) {
    let endpoint = world
        .mqtt_publication_endpoint
        .as_mut()
        .assured("the scenario configured the publication endpoint");
    let first = endpoint.next().await;
    let retry = endpoint.next().await;
    assert!(
        !first.forwarded,
        "the broker never receives the first attempt"
    );
    assert!(retry.forwarded, "the retry reaches the real broker");
    assert_eq!(retry.publish.payload, first.publish.payload);
    assert_eq!(retry.publish.topic, first.publish.topic);
    assert_eq!(retry.publish.qos, first.publish.qos);
    assert!(first.publish.retain);
    assert_eq!(retry.publish.retain, first.publish.retain);
    assert!(first.publish.properties.is_some());
    assert_eq!(retry.publish.properties, first.publish.properties);
}

#[then(expr = "the next MQTT publication at the endpoint has user_id {int}")]
async fn assert_endpoint_record(world: &mut ScenarioWorld, user_id: i64) {
    let endpoint = world
        .mqtt_publication_endpoint
        .as_mut()
        .assured("the scenario configured the publication endpoint");
    let captured = endpoint.next().await;
    assert!(captured.forwarded);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&captured.publish.payload)
            .assured("the codec writes JSON"),
        serde_json::json!({"user_id": user_id})
    );
}
