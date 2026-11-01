//! Public RabbitMQ delivery proofs, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** Deliveries a scenario publishes to an exchange with the AMQP basic properties a data
//!   table names.
//! - **Depends on.** The scenario's RabbitMQ endpoint and lapin's publishing client.
//! - **Must not know.** The RabbitMQ ingestor's consumers, its metadata row or any product state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the public scenario boundary declared above")
)]

use cucumber::gherkin::Table;
use lapin::{BasicProperties, Connection, ConnectionProperties, options::BasicPublishOptions};

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

    /// Publishes this delivery to `exchange` under `routing_key` on a connection of its own, and
    /// returns once the broker has accepted it.
    async fn publish(self, world: &ScenarioWorld, exchange: &str, routing_key: &str) {
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
        channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions::default(),
                self.payload.as_bytes(),
                self.properties,
            )
            .await
            .unwrap_or_else(|error| panic!("failed to publish to exchange '{exchange}': {error}"))
            .await
            .unwrap_or_else(|error| {
                panic!("the broker refused the delivery to exchange '{exchange}': {error}")
            });
    }
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
    delivery.publish(world, &exchange, &routing_key).await;
}
