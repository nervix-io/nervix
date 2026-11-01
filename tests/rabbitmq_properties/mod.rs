//! Public RabbitMQ delivery proofs, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** The exchanges and bindings a scenario declares on its broker, and the deliveries it
//!   publishes through them with the AMQP basic properties a data table names.
//! - **Depends on.** The scenario's RabbitMQ endpoint and lapin's publishing client.
//! - **Must not know.** The RabbitMQ ingestor's consumers, its metadata row or any product state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public scenario boundary declared above")
)]

use cucumber::gherkin::Table;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::FieldTable,
};

use super::*;

/// One delivery a scenario publishes: its body and the basic properties its table sets.
struct PublishedDelivery {
    payload: String,
    properties: BasicProperties,
}

impl PublishedDelivery {
    /// Reads a two-column table of AMQP property names and values, whose `payload` row is the
    /// delivery's body. A property the table leaves out stays unset, as a publisher leaves it.
    fn from_table(world: &ScenarioWorld, table: &Table) -> Self {
        let mut payload = None;
        let mut properties = BasicProperties::default();
        for row in &table.rows {
            let [name, value] = row.as_slice() else {
                panic!("each row names one AMQP property and its value: {row:?}");
            };
            let value = expand_placeholders(world, value);
            if name == "payload" {
                payload = Some(value);
                continue;
            }
            properties = Self::with_property(properties, name, value);
        }
        let payload = payload.unwrap_or_else(|| panic!("the table names no payload row"));
        Self {
            payload,
            properties,
        }
    }

    /// `properties` with the basic property AMQP 0-9-1 names `name` set to `value`.
    fn with_property(properties: BasicProperties, name: &str, value: String) -> BasicProperties {
        match name {
            "content_type" => properties.with_content_type(value.into()),
            "content_encoding" => properties.with_content_encoding(value.into()),
            "message_id" => properties.with_message_id(value.into()),
            "correlation_id" => properties.with_correlation_id(value.into()),
            "reply_to" => properties.with_reply_to(value.into()),
            "type" => properties.with_type(value.into()),
            "app_id" => properties.with_app_id(value.into()),
            "user_id" => properties.with_user_id(value.into()),
            "expiration" => properties.with_expiration(value.into()),
            "priority" => {
                let priority: u8 = value
                    .parse()
                    .unwrap_or_else(|error| panic!("priority '{value}' is not an octet: {error}"));
                properties.with_priority(priority)
            }
            "timestamp" => {
                let seconds: u64 = value.parse().unwrap_or_else(|error| {
                    panic!("timestamp '{value}' is not a 64-bit count of seconds: {error}")
                });
                properties.with_timestamp(seconds)
            }
            other => panic!("the fixture sets no AMQP basic property named '{other}'"),
        }
    }
}

/// A channel on the scenario's broker, kept with the connection it lives on.
struct BrokerChannel {
    _connection: Connection,
    channel: Channel,
}

impl BrokerChannel {
    async fn open(world: &ScenarioWorld) -> Self {
        let addr = world
            .dependencies
            .endpoints()
            .get(RABBITMQ_ADDR)
            .unwrap_or_else(|error| panic!("RabbitMQ is not running: {error}"))
            .to_string();
        let connection = Connection::connect(&addr, ConnectionProperties::default())
            .await
            .unwrap_or_else(|error| panic!("failed to connect to RabbitMQ: {error}"));
        let channel = connection
            .create_channel()
            .await
            .unwrap_or_else(|error| panic!("failed to open a RabbitMQ channel: {error}"));
        Self {
            _connection: connection,
            channel,
        }
    }

    async fn declare_exchange(&self, exchange: &str, kind: ExchangeKind) {
        self.channel
            .exchange_declare(
                exchange.into(),
                kind,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap_or_else(|error| panic!("failed to declare exchange '{exchange}': {error}"));
    }

    /// Declares `queue` with the defaults every other RabbitMQ step declares it with, and binds it
    /// to `exchange` under `routing_key`.
    async fn bind_queue(&self, exchange: &str, routing_key: &str, queue: &str) {
        self.channel
            .queue_declare(
                queue.into(),
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap_or_else(|error| panic!("failed to declare queue '{queue}': {error}"));
        self.channel
            .queue_bind(
                queue.into(),
                exchange.into(),
                routing_key.into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("failed to bind queue '{queue}' to exchange '{exchange}': {error}")
            });
    }

    async fn publish(&self, exchange: &str, routing_key: &str, delivery: PublishedDelivery) {
        self.channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions::default(),
                delivery.payload.as_bytes(),
                delivery.properties,
            )
            .await
            .unwrap_or_else(|error| panic!("failed to publish to exchange '{exchange}': {error}"))
            .await
            .unwrap_or_else(|error| {
                panic!("the broker refused the delivery to exchange '{exchange}': {error}")
            });
    }
}

#[given(expr = "RabbitMQ exchange {string} routes {string} to queue {string}")]
async fn given_direct_exchange_routes_to_queue(
    world: &mut ScenarioWorld,
    exchange: String,
    routing_key: String,
    queue: String,
) {
    let exchange = expand_placeholders(world, &exchange);
    let routing_key = expand_placeholders(world, &routing_key);
    let queue = expand_placeholders(world, &queue);
    let broker = BrokerChannel::open(world).await;
    broker
        .declare_exchange(&exchange, ExchangeKind::Direct)
        .await;
    broker.bind_queue(&exchange, &routing_key, &queue).await;
}

#[given(expr = "RabbitMQ fanout exchange {string} delivers to queues {string} and {string}")]
async fn given_fanout_exchange_delivers_to_queues(
    world: &mut ScenarioWorld,
    exchange: String,
    first: String,
    second: String,
) {
    let exchange = expand_placeholders(world, &exchange);
    let first = expand_placeholders(world, &first);
    let second = expand_placeholders(world, &second);
    let broker = BrokerChannel::open(world).await;
    broker
        .declare_exchange(&exchange, ExchangeKind::Fanout)
        .await;
    broker.bind_queue(&exchange, "", &first).await;
    broker.bind_queue(&exchange, "", &second).await;
}

#[when(
    expr = "a RabbitMQ message is published to exchange {string} with routing key {string} and \
            these AMQP properties"
)]
async fn when_message_is_published_with_properties(
    world: &mut ScenarioWorld,
    exchange: String,
    routing_key: String,
    #[step] step: &Step,
) {
    let exchange = expand_placeholders(world, &exchange);
    let routing_key = expand_placeholders(world, &routing_key);
    let table = step
        .table
        .as_ref()
        .unwrap_or_else(|| panic!("the step lists the delivery's AMQP properties in a table"));
    let delivery = PublishedDelivery::from_table(world, table);
    let broker = BrokerChannel::open(world).await;
    broker.publish(&exchange, &routing_key, delivery).await;
}
