//! Public MQTT session ownership checks.
//!
//! Outside the layer order: a scenario harness.
//!
//! - **Owns.** Graph setup through Plexus and broker observations of session identities.
//! - **Depends on.** The public client, scenario fixtures and the MQTT driver.
//! - **Must not know.** Product runtime state or connector implementation details.

use rumqttc::{AsyncClient, BrokerSessionResumePolicy, Event, Incoming, MqttOptions, SessionMode};

use super::*;

#[given(expr = "an MQTT session ingestor named {string} runs with {int} instances")]
async fn given_session_ingestor(world: &mut ScenarioWorld, name: String, instances: u64) {
    start_session_ingestor(world, name, instances, None).await;
}

#[given(expr = "an MQTT session ingestor named {string} runs with client id {string}")]
async fn given_identified_session_ingestor(
    world: &mut ScenarioWorld,
    name: String,
    client_id: String,
) {
    start_session_ingestor(world, name, 1, Some(&client_id)).await;
}

async fn start_session_ingestor(
    world: &mut ScenarioWorld,
    name: String,
    instances: u64,
    client_id: Option<&str>,
) {
    let identity = match client_id {
        Some(client_id) => format!(", 'client_id' = '{client_id}'"),
        None => String::new(),
    };
    let commands = expand_placeholders(
        world,
        &format!(
            r#"
            CREATE UNPACED DOMAIN {{{{domain}}}};
            CREATE SCHEMA item (user_id I64);
            CREATE WIRE JSON SCHEMA item_wire MODE STRICT (user_id integer);
            CREATE CODEC item_codec FROM WIRE JSON SCHEMA item_wire TO SCHEMA item;
            CREATE RELAY output SCHEMA item UNBRANCHED;
            CREATE CLIENT broker TYPE MQTT CONFIG {{ 'addr' = '{{{{mqtt_addr}}}}'{identity} }};
            CREATE MQTT TOPIC INGESTOR {name}
              FROM broker TOPIC 'session_{{{{test_id}}}}' INSTANCES {instances}
              SESSION PERSISTENT QOS 1 MODE NO ACK
              ON QUIESCE SUSPEND DECODE USING item_codec
              TO output INHERIT ALL UNBRANCHED FLUSH IMMEDIATE
              ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
            START;
            "#
        ),
    );
    let leader = current_leader_node(world).await;
    execute_nspl_commands_on_node(world, &leader, &commands)
        .await
        .assured("the public MQTT session graph is valid");
    world
        .mqtt_ingestors_by_domain
        .entry(world.domain.clone())
        .or_default()
        .insert(name);
    wait_for_mqtt_ingestors_ready(world).await;
}

#[then(expr = "the MQTT broker retains session {string}")]
async fn then_session_is_retained(world: &mut ScenarioWorld, client_id: String) {
    let client_id = expand_placeholders(world, &client_id);
    assert!(
        session_present(world, &client_id).await,
        "the broker must retain MQTT session '{client_id}'"
    );
}

#[then(expr = "the MQTT broker has released session {string}")]
async fn then_session_is_released(world: &mut ScenarioWorld, client_id: String) {
    let client_id = expand_placeholders(world, &client_id);
    assert!(
        !session_present(world, &client_id).await,
        "the broker still retains MQTT session '{client_id}' after DROP"
    );
}

#[then(expr = "MQTT ingestors in domains {string} and {string} both deliver their input")]
async fn then_both_domains_deliver(world: &mut ScenarioWorld, first: String, second: String) {
    struct DomainSubscription {
        domain: String,
        session: TestSession,
    }

    let leader = current_leader_node(world).await;
    let mut subscriptions = Vec::new();
    for domain in [first, second] {
        nervix_primitives::task::consume_budget().await;
        let domain = expand_placeholders(world, &domain);
        let mut session = world
            .cluster()
            .open_session(&leader, &domain)
            .await
            .assured("each declared domain accepts a public session");
        session
            .run_command("CREATE SUBSCRIPTION observed TO output;")
            .await
            .assured("the graph exposes its output relay");
        subscriptions.push(DomainSubscription { domain, session });
    }
    let topic = expand_placeholders(world, "session_{{test_id}}");
    world
        .cluster()
        .publish_mqtt_qos1(&topic, r#"{"user_id":41}"#)
        .await
        .assured("the fixture broker accepts the input");
    for mut subscription in subscriptions {
        nervix_primitives::task::consume_budget().await;
        let event = subscription
            .session
            .try_next_subscription(Duration::from_secs(30))
            .await
            .assured("the public subscription remains open");
        let event = event.assured("both domains deliver the published input");
        assert!(
            event.payload.contains(r#""user_id":41"#),
            "domain {} delivered {}",
            subscription.domain,
            event.payload
        );
    }
}

async fn session_present(world: &ScenarioWorld, client_id: &str) -> bool {
    let endpoint = world
        .dependencies
        .endpoints()
        .get(MQTT_ADDR)
        .assured("MQTT is running");
    let url = url::Url::parse(endpoint).assured("the fixture publishes a valid MQTT URL");
    let host = url.host_str().assured("the fixture URL names its broker");
    let port = url.port().assured("the fixture URL declares its port");
    let mut options = MqttOptions::new(client_id, (host, port));
    options.set_session_mode(SessionMode::Persistent);
    options.set_broker_session_resume_policy(BrokerSessionResumePolicy::AllowBrokerOnly);
    let (_client, mut eventloop) = AsyncClient::builder(options).capacity(1).build();
    nervix_primitives::time::timeout(Duration::from_secs(30), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            match eventloop
                .poll()
                .await
                .assured("the fixture broker answers CONNECT")
            {
                Event::Incoming(Incoming::ConnAck(answer)) => return answer.session_present,
                Event::Incoming(_) | Event::Outgoing(_) | Event::Auth(_) => {}
            }
        }
    })
    .await
    .assured("the broker answers the session probe within the observation budget")
}

#[when("these Plexus statements are executed")]
async fn when_plexus_statements(world: &mut ScenarioWorld, #[step] step: &Step) {
    when_these_nspl_commands_are_executed(world, step).await;
}
