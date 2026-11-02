//! Public RabbitMQ emitter envelope observations.
//!
//! Outside the layer order: a scenario harness.
//! - **Owns.** Explicit durable queue provisioning and complete AMQP property observations.
//! - **Depends on.** The public broker protocol, Plexus scenario steps and an isolated restart fixture.
//! - **Must not know.** Connector publication or retained-payload internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns public RabbitMQ envelope observations")
)]

use lapin::{
    Connection, ConnectionProperties,
    options::{BasicGetOptions, QueueDeclareOptions},
    types::{AMQPValue, FieldTable},
};

use super::*;

fn address(world: &ScenarioWorld) -> &str {
    match &world.rabbitmq_envelope_broker {
        Some(broker) => broker
            .address()
            .assured("the isolated broker has an address"),
        None => world
            .dependencies
            .endpoints()
            .get(RABBITMQ_ADDR)
            .assured("RabbitMQ is running"),
    }
}

#[given("a scenario-owned RabbitMQ broker is running")]
async fn given_private_broker(world: &mut ScenarioWorld) {
    let broker = nervix_test_environment::restartable_rabbitmq::RestartableRabbitMq::start(
        format!("rabbitmq-envelope-{}", world.test_id),
    )
    .await
    .assured("the scenario-owned RabbitMQ broker starts");
    world.placeholders.insert(
        "envelope_broker_addr".to_owned(),
        broker.address().assured("the broker started").to_owned(),
    );
    world.rabbitmq_envelope_broker = Some(broker);
}

#[when("the scenario-owned RabbitMQ broker is restarted")]
async fn when_private_broker_restarts(world: &mut ScenarioWorld) {
    world
        .rabbitmq_envelope_broker
        .as_ref()
        .assured("the scenario started its broker")
        .restart()
        .await
        .assured("the broker restarts with the same data directory");
    let mut last_error = None;
    let connection = nervix_primitives::time::timeout(Duration::from_secs(30), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            match Connection::connect(address(world), ConnectionProperties::default()).await {
                Ok(connection) => return connection,
                Err(error) => last_error = Some(error),
            }
            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("the restarted broker did not accept AMQP within 30s: {last_error:?}")
    });
    connection
        .close(200, "restart readiness observed")
        .await
        .assured("the AMQP readiness probe closes");
}

#[given(expr = "durable RabbitMQ queue {string} exists")]
async fn given_durable_queue(world: &mut ScenarioWorld, queue: String) {
    let queue = expand_placeholders(world, &queue);
    let connection = Connection::connect(address(world), ConnectionProperties::default())
        .await
        .assured("the broker accepts the provisioning client");
    let channel = connection
        .create_channel()
        .await
        .assured("the provisioning channel opens");
    channel
        .queue_declare(
            queue.as_str().into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .assured("the operator provisions a durable queue");
}

#[then(expr = "RabbitMQ envelope queue {string} holds {int} ready messages")]
async fn then_ready_messages(world: &mut ScenarioWorld, queue: String, expected: u32) {
    let queue = expand_placeholders(world, &queue);
    let connection = Connection::connect(address(world), ConnectionProperties::default())
        .await
        .assured("the broker accepts the observer");
    let channel = connection
        .create_channel()
        .await
        .assured("the observation channel opens");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        nervix_primitives::task::consume_budget().await;
        let queue = channel
            .queue_declare(
                queue.as_str().into(),
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .assured("the queue already exists");
        if queue.message_count() == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected} ready messages; got {}",
            queue.message_count()
        );
        nervix_primitives::time::sleep(Duration::from_millis(100)).await;
    }
}

#[then(expr = "RabbitMQ queue {string} yields this envelope message")]
async fn then_envelope_message(world: &mut ScenarioWorld, queue: String, step: &Step) {
    let queue = expand_placeholders(world, &queue);
    let expected = expand_placeholders(
        world,
        step.docstring
            .as_deref()
            .assured("the step contains its expected message"),
    );
    let expected: serde_json::Value =
        serde_json::from_str(&expected).assured("the expected message is JSON");
    let delivery = nervix_primitives::time::timeout(Duration::from_secs(30), async {
        let connection = Connection::connect(address(world), ConnectionProperties::default())
            .await
            .assured("the broker accepts the observer");
        let channel = connection
            .create_channel()
            .await
            .assured("the observation channel opens");
        loop {
            nervix_primitives::task::consume_budget().await;
            if let Some(delivery) = channel
                .basic_get(queue.as_str().into(), BasicGetOptions { no_ack: true })
                .await
                .assured("the queue can be read")
            {
                return delivery;
            }
            nervix_primitives::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .assured("RabbitMQ receives the emitted message within 30s");
    let properties = &delivery.delivery.properties;
    let mut headers = serde_json::Map::new();
    if let Some(table) = properties.headers() {
        for (name, value) in table.inner() {
            let AMQPValue::LongString(value) = value else {
                panic!("the emitted header is text");
            };
            headers.insert(
                name.to_string(),
                serde_json::Value::String(
                    String::from_utf8(value.as_bytes().to_vec()).assured("the header is UTF-8"),
                ),
            );
        }
    }
    let payload: serde_json::Value =
        serde_json::from_slice(&delivery.delivery.data).assured("the emitted body is JSON");
    let actual = serde_json::json!({
        "payload": payload,
        "properties": {
            "content_type": properties.content_type().as_ref().map(|v| v.as_str()),
            "content_encoding": properties.content_encoding().as_ref().map(|v| v.as_str()),
            "message_id": properties.message_id().as_ref().map(|v| v.as_str()),
            "correlation_id": properties.correlation_id().as_ref().map(|v| v.as_str()),
            "reply_to": properties.reply_to().as_ref().map(|v| v.as_str()),
            "expiration": properties.expiration().as_ref().map(|v| v.as_str()),
            "type": properties.kind().as_ref().map(|v| v.as_str()),
            "app_id": properties.app_id().as_ref().map(|v| v.as_str()),
            "user_id": properties.user_id().as_ref().map(|v| v.as_str()),
            "priority": properties.priority(),
            "timestamp": properties.timestamp(),
            "delivery_mode": properties.delivery_mode(),
            "headers": headers
        }
    });
    assert_eq!(
        actual, expected,
        "every delivered property and header must match"
    );
}
