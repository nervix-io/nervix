//! Outside the layer order: qualification of the scenario publisher's MQTT completion.
//!
//! - **Owns.** A delayed fixture connection and complete broker-delivery assertions.
//! - **Depends on.** The real broker, shared publisher, observer, and TCP stall proxy.
//! - **Must not know.** Product source, emitter, graph, or acknowledgement state.

use super::*;

pub(super) struct Publication {
    observer: common::cluster::BrokerObserver,
    payload: String,
    count: usize,
    // Forwarding must remain alive until the last payload reaches the observer.
    _proxy: StallableTcpProxy,
}

#[when(
    expr = "{int} QoS {int} MQTT fixture payloads are published through a {string} connection \
            stall"
)]
async fn publish_through_connection_stall(
    world: &mut ScenarioWorld,
    count: usize,
    qos: u8,
    stall: String,
    #[step] step: &Step,
) {
    let qos = match qos {
        0 => rumqttc::QoS::AtMostOnce,
        1 => rumqttc::QoS::AtLeastOnce,
        other => panic!("unsupported fixture QoS {other}"),
    };
    let stall = parse_duration_text(&stall).expect("the feature declares a valid stall duration");
    let topic = expand_placeholders(world, "fixture_publication_{{test_id}}");
    let payload = expand_placeholders(world, docstring(step));
    let endpoints = world.dependencies.endpoints();
    let observer = common::cluster::observe_mqtt(endpoints, &topic, rumqttc::QoS::AtMostOnce)
        .await
        .expect("the observer must subscribe before publication");
    let endpoint = url::Url::parse(endpoints.get(MQTT_ADDR).expect("MQTT is running"))
        .expect("the fixture publishes a valid broker address");
    let proxy = StallableTcpProxy::start(
        endpoint
            .host_str()
            .expect("the broker has a host")
            .to_owned(),
        endpoint.port().expect("the broker has a mapped port"),
    )
    .await
    .expect("the proxy must listen before publication");
    proxy.set_paused(true);
    let publication = async {
        if count == 1 {
            common::cluster::publish_mqtt_to_address(
                "127.0.0.1".to_owned(),
                proxy.local_port(),
                &topic,
                &payload,
                qos,
                true,
            )
            .await
        } else {
            common::cluster::publish_mqtt_burst_to_address(
                "127.0.0.1".to_owned(),
                proxy.local_port(),
                &topic,
                &payload,
                count,
                qos,
            )
            .await
        }
    };
    let release = async {
        nervix_primitives::time::sleep(stall).await;
        proxy.set_paused(false);
    };
    let (publication, ()) = tokio::join!(publication, release);
    publication.expect("the fixture must complete publication through the delayed connection");
    world.mqtt_fixture_publication = Some(Publication {
        observer,
        payload,
        count,
        _proxy: proxy,
    });
}

#[then("every MQTT fixture payload reaches its observer unchanged")]
async fn every_payload_reaches_observer(world: &mut ScenarioWorld) {
    let mut publication = world
        .mqtt_fixture_publication
        .take()
        .expect("the preceding step published through the stalled connection");
    for _ in 0..publication.count {
        let observed = publication
            .observer
            .next_message()
            .await
            .expect("every completed publication must reach the broker observer");
        assert_eq!(observed.payload, publication.payload);
    }
}
