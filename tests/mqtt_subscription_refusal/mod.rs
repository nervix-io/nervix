//! Public checks of MQTT subscription refusal and session admission.
//!
//! Outside the layer order: a scenario harness.
//!
//! - **Owns.** Scenario-scoped broker authorization rules and wire observations of authenticated
//!   subscription refusal and the broker's retained empty session.
//! - **Depends on.** The public session API, MQTT driver, broker management API and test environment.
//! - **Must not know.** Product connector state, runtime tasks or acknowledgement ownership.

use nervix_test_environment::{
    MQTT_API_ADDR, MQTT_API_KEY, MQTT_API_SECRET, MQTT_AUTH_ADDR, MQTT_AUTH_PASSWORD,
    MQTT_AUTH_USERNAME,
};
use rumqttc::{
    AsyncClient, BrokerSessionResumePolicy, Event, EventLoop, Incoming, MqttOptions, QoS,
    SessionMode, SubscribeReasonCode,
};

use super::*;

const BROKER_OBSERVATION_BUDGET: Duration = Duration::from_secs(30);

#[given(expr = "MQTT client {string} is denied subscriptions")]
async fn deny_subscriptions(world: &mut ScenarioWorld, client_id: String) {
    let client_id = expand_placeholders(world, &client_id);
    set_subscription_permission(world, &client_id, "deny", false).await;
}

#[when(expr = "MQTT client {string} is allowed to subscribe")]
async fn allow_subscriptions(world: &mut ScenarioWorld, client_id: String) {
    let client_id = expand_placeholders(world, &client_id);
    set_subscription_permission(world, &client_id, "allow", true).await;
}

/// Each rule belongs to the scenario's unique client identity. The broker's no-match policy
/// admits all other clients, so this neither serializes nor changes a neighboring scenario.
async fn set_subscription_permission(
    world: &ScenarioWorld,
    client_id: &str,
    permission: &str,
    replace: bool,
) {
    let endpoints = world.dependencies.endpoints();
    let api = endpoints.get(MQTT_API_ADDR).assured("MQTT is running");
    let mut url = url::Url::parse(api).assured("the fixture publishes a valid management URL");
    let mut path = url
        .path_segments_mut()
        .assured("the management URL accepts path segments");
    path.extend([
        "api",
        "v5",
        "authorization",
        "sources",
        "built_in_database",
        "rules",
        "clients",
    ]);
    if replace {
        path.push(client_id);
    }
    drop(path);
    let rule = serde_json::json!({
        "clientid": client_id,
        "rules": [{"action": "subscribe", "permission": permission, "topic": "#"}]
    });
    let http = reqwest::Client::builder()
        .timeout(BROKER_OBSERVATION_BUDGET)
        .build()
        .assured("the broker observation client has valid options");
    let request = if replace {
        http.put(url).json(&rule)
    } else {
        http.post(url).json(&vec![rule])
    };
    request
        .basic_auth(MQTT_API_KEY, Some(MQTT_API_SECRET))
        .send()
        .await
        .assured("the broker answers the scenario's authorization update")
        .error_for_status()
        .assured("the broker accepts the scenario's client authorization rule");
}

/// Qualifies the real broker's response sequence with an independent probe identity. The source
/// under test has not connected yet, so this cannot seed its session or bypass its own SUBACK.
#[then(
    expr = "the MQTT broker retains an authenticated empty session after refusing client {string}"
)]
async fn retains_refused_session(world: &mut ScenarioWorld, client_id: String) {
    let client_id = expand_placeholders(world, &client_id);
    set_subscription_permission(world, &client_id, "deny", false).await;
    nervix_primitives::time::timeout(BROKER_OBSERVATION_BUDGET, async {
        let (client, mut connection) = authenticated_connection(world, &client_id);
        assert!(
            !connect(&mut connection).await,
            "the probe starts a fresh session"
        );
        client
            .subscribe("$share/refusal_probe/refused_probe", QoS::AtLeastOnce)
            .await
            .assured("the probe queues its subscription after authenticating");
        loop {
            nervix_primitives::task::consume_budget().await;
            if let Event::Incoming(Incoming::SubAck(answer)) = connection
                .poll()
                .await
                .assured("the authenticated broker answers the probe's subscription")
            {
                assert_eq!(
                    answer.return_codes,
                    vec![SubscribeReasonCode::NotAuthorized]
                );
                break;
            }
        }
        drop(connection);
        drop(client);
        let (_client, mut connection) = authenticated_connection(world, &client_id);
        assert!(
            connect(&mut connection).await,
            "the broker retains the empty session after its explicit SUBACK refusal"
        );
    })
    .await
    .assured("the authenticated refusal and retained-session probe complete within their budget");
}

fn authenticated_connection(world: &ScenarioWorld, client_id: &str) -> (AsyncClient, EventLoop) {
    let endpoints = world.dependencies.endpoints();
    let endpoint = endpoints.get(MQTT_AUTH_ADDR).assured("MQTT is running");
    let url = url::Url::parse(endpoint).assured("the fixture publishes a valid broker URL");
    let host = url.host_str().assured("the fixture URL names its broker");
    let port = url.port().assured("the fixture URL declares its port");
    let mut options = MqttOptions::new(client_id, (host, port));
    options.set_credentials(
        endpoints
            .get(MQTT_AUTH_USERNAME)
            .assured("the fixture declares its user")
            .to_string(),
        endpoints
            .get(MQTT_AUTH_PASSWORD)
            .assured("the fixture declares its password")
            .to_string(),
    );
    options.set_session_mode(SessionMode::Persistent);
    options.set_broker_session_resume_policy(BrokerSessionResumePolicy::AllowBrokerOnly);
    AsyncClient::builder(options).capacity(1).build()
}

async fn connect(connection: &mut EventLoop) -> bool {
    loop {
        nervix_primitives::task::consume_budget().await;
        if let Event::Incoming(Incoming::ConnAck(answer)) = connection
            .poll()
            .await
            .assured("the broker accepts the probe's valid username and password")
        {
            return answer.session_present;
        }
    }
}
