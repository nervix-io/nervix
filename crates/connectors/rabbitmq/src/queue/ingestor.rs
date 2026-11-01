//! RabbitMQ ingestor transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The AMQP connection, channel and consumer each ingestor instance reads through, its
//!   declared consumer mode and prefetch window, AMQP headers as ingest headers, the delivery's
//!   envelope and basic properties as its typed metadata, and per-delivery acknowledgement and
//!   requeue.
//! - **Depends on.** The connector contract, typed client configuration entries, the crate's
//!   broker connection, `error-stack`, Tokio and `lapin`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or Plexus.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

mod reader;

use std::{borrow::Cow, fmt, num::NonZeroU16, time::Duration};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use lapin::{
    Acker, Channel, Connection, Consumer,
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    types::{AMQPValue, FieldTable, ShortString},
};
use nervix_connector::{
    BrokerIngestorConnector, IngestMessageHeaders, IngestMessageMetadata, IngestMetadataRow,
    IngestMetadataValues, IngestRowHeaders, IngestorBatch, IngestorBatchRequest, IngestorConnector,
    IngestorError, IngestorMessage, IngestorReadCancellation, IngestorResult, IngestorResume,
};
use nervix_dns::DnsResolver;
use nervix_models::{ClientConfigEntry, IngestorMetadataType, RabbitMqQueueName, Timestamp};
use reader::RabbitMqBatchReader;
use thiserror::Error;

use crate::queue::connection::RabbitMqBroker;

const RABBITMQ: &str = "rabbitmq";

/// Why a RabbitMQ ingestor could not connect, consume, or settle a delivery.
#[derive(Debug, Error)]
pub enum RabbitMqQueueIngestorError {
    /// The ingestor could not open a connection; the [`crate::queue::RabbitMqConnectError`] beneath it
    /// says why.
    #[error("failed to connect RabbitMQ client")]
    Connect,
    #[error("failed to open a RabbitMQ channel")]
    Channel,
    #[error("failed to configure the RabbitMQ prefetch window")]
    Qos,
    #[error("failed to consume RabbitMQ queue '{queue}'")]
    Consume { queue: String },
    #[error("failed to receive a RabbitMQ delivery")]
    Receive,
    #[error("RabbitMQ consumer closed")]
    ConsumerClosed,
    #[error("failed to acknowledge a RabbitMQ delivery")]
    Acknowledge,
    #[error("failed to requeue a RabbitMQ delivery")]
    Requeue,
    #[error("the RabbitMQ delivery was already settled or its channel closed")]
    DeliverySettled,
    #[error("RabbitMQ batch timeout exceeds the monotonic clock range")]
    BatchDeadline,
}

type RabbitMqQueueIngestorResult<T> = Result<T, Report<RabbitMqQueueIngestorError>>;

/// The consumer's transport policy, resolved before the connector is composed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RabbitMqConsumerMode {
    /// RabbitMQ considers a delivery complete as soon as it sends it.
    NoAck,
    /// Each delivery is settled on its own channel; prefetch bounds outstanding deliveries.
    Ack { prefetch: NonZeroU16 },
}

/// What one RabbitMQ ingestor consumes: its resolved client entries, the node resolver its broker
/// host resolves through, the queue, and the consumer tag each instance registers under, suffixed
/// with its instance index.
#[derive(Clone)]
pub struct RabbitMqQueueIngestorPlan {
    config: Vec<ClientConfigEntry>,
    dns: DnsResolver,
    queue: RabbitMqQueueName,
    consumer_tag: String,
    mode: RabbitMqConsumerMode,
}

/// The AMQP session one instance consumes through, kept together because dropping the channel
/// or the connection requeues every delivery it holds unacknowledged.
struct RabbitMqConsumer {
    _connection: Connection,
    _channel: Channel,
    reader: RabbitMqBatchReader<Consumer>,
}

impl RabbitMqQueueIngestorPlan {
    pub fn new(
        config: Vec<ClientConfigEntry>,
        dns: DnsResolver,
        queue: RabbitMqQueueName,
        consumer_tag: String,
        mode: RabbitMqConsumerMode,
    ) -> Self {
        Self {
            config,
            dns,
            queue,
            consumer_tag,
            mode,
        }
    }

    /// Connects and starts the consumer of one instance, every step within the client's
    /// `connect_timeout_ms`.
    async fn consume(&self, instance_index: u64) -> RabbitMqQueueIngestorResult<RabbitMqConsumer> {
        let broker = RabbitMqBroker::from_config(&self.config, self.dns.clone())
            .change_context(RabbitMqQueueIngestorError::Connect)?;
        let opening = broker
            .connect()
            .await
            .change_context(RabbitMqQueueIngestorError::Connect)?;
        let channel = opening
            .within_budget(
                "opening the RabbitMQ consumer's channel",
                opening.connection().create_channel(),
            )
            .await
            .map_err(|failure| failure.beneath(RabbitMqQueueIngestorError::Channel))?;
        let no_ack = match self.mode {
            RabbitMqConsumerMode::NoAck => true,
            RabbitMqConsumerMode::Ack { prefetch } => {
                opening
                    .within_budget(
                        "setting the RabbitMQ prefetch window",
                        channel.basic_qos(prefetch.get(), BasicQosOptions::default()),
                    )
                    .await
                    .map_err(|failure| failure.beneath(RabbitMqQueueIngestorError::Qos))?;
                false
            }
        };
        let consumer = opening
            .within_budget(
                "starting the RabbitMQ consumer",
                channel.basic_consume(
                    self.queue.as_str().into(),
                    format!("{}-{instance_index}", self.consumer_tag).into(),
                    BasicConsumeOptions {
                        no_ack,
                        ..BasicConsumeOptions::default()
                    },
                    FieldTable::default(),
                ),
            )
            .await
            .map_err(|failure| {
                failure.beneath(RabbitMqQueueIngestorError::Consume {
                    queue: self.queue.as_str().to_string(),
                })
            })?;
        Ok(RabbitMqConsumer {
            _connection: opening.opened(),
            _channel: channel,
            reader: RabbitMqBatchReader::new(consumer),
        })
    }
}

/// The acknowledgement handle of one delivery, which settles it on the channel it arrived on.
#[derive(Debug, Clone)]
pub struct RabbitMqQueueIngestorPosition {
    acker: Acker,
}

/// One received delivery, whose AMQP headers are its ingest headers.
///
/// Reading them visits the delivery properties in place, so a delivery without headers costs
/// nothing and a value only allocates when its AMQP type is not already a string.
pub struct RabbitMqDeliveryHeaders {
    delivery: Delivery,
}

impl RabbitMqDeliveryHeaders {
    fn header_value(value: &AMQPValue) -> Cow<'_, str> {
        match value {
            AMQPValue::Boolean(value) => Cow::Owned(value.to_string()),
            AMQPValue::ShortShortInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::ShortShortUInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::ShortInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::ShortUInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::LongInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::LongUInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::LongLongInt(value) => Cow::Owned(value.to_string()),
            AMQPValue::Float(value) => Cow::Owned(value.to_string()),
            AMQPValue::Double(value) => Cow::Owned(value.to_string()),
            AMQPValue::DecimalValue(value) => {
                Cow::Owned(format!("{}:{}", value.scale, value.value))
            }
            AMQPValue::ShortString(value) => Cow::Borrowed(value.as_str()),
            AMQPValue::LongString(value) => String::from_utf8_lossy(value.as_bytes()),
            AMQPValue::FieldArray(value) => Cow::Owned(
                value
                    .as_slice()
                    .iter()
                    .map(|value| Self::header_value(value).into_owned())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            AMQPValue::FieldTable(value) => Cow::Owned(
                value
                    .into_iter()
                    .map(|(name, value)| format!("{}={}", name.as_str(), Self::header_value(value)))
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            AMQPValue::Timestamp(value) => Cow::Owned(value.to_string()),
            AMQPValue::ByteArray(value) => String::from_utf8_lossy(value.as_slice()),
            AMQPValue::Void => Cow::Borrowed(""),
        }
    }
}

impl IngestMessageHeaders for RabbitMqDeliveryHeaders {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        let Some(headers) = self.delivery.properties.headers().as_ref() else {
            return;
        };
        for (name, value) in headers {
            visit(name.as_str(), Self::header_value(value).as_ref());
        }
    }
}

/// The AMQP timestamp of a delivery: a count of whole seconds since the Unix epoch, as AMQP 0-9-1
/// writes it.
#[derive(Debug, Clone, Copy)]
struct AmqpTimestamp(u64);

impl AmqpTimestamp {
    /// The instant this timestamp names, or `None` past 2262-04-11, the last whole second a signed
    /// count of nanoseconds holds.
    fn instant(self) -> Option<Timestamp> {
        let nanos = Duration::from_secs(self.0).as_nanos();
        let Ok(nanos) = i64::try_from(nanos) else {
            return None;
        };
        Some(Timestamp::from_unix_nanos(nanos))
    }
}

impl fmt::Display for AmqpTimestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "the AMQP timestamp {} seconds after the Unix epoch",
            self.0
        )
    }
}

/// One received delivery: its payload, its AMQP headers, its envelope and basic properties as its
/// metadata, and the position that settles it.
pub struct RabbitMqQueueIngestorMessage {
    headers: RabbitMqDeliveryHeaders,
    position: RabbitMqQueueIngestorPosition,
}

impl RabbitMqQueueIngestorMessage {
    fn new(delivery: Delivery) -> Self {
        let position = RabbitMqQueueIngestorPosition {
            acker: delivery.acker.clone(),
        };
        Self {
            headers: RabbitMqDeliveryHeaders { delivery },
            position,
        }
    }

    /// Hands over a text property, absent where the publisher left it unset.
    fn visit_text(values: &mut dyn IngestMetadataValues, property: Option<&ShortString>) {
        match property {
            Some(text) => values.string(text.as_str()),
            None => values.absent(),
        }
    }

    /// Hands over the AMQP timestamp as the instant it names, absent where the publisher left it
    /// unset, and unrepresentable where no `DATETIME` holds that instant.
    fn visit_timestamp(values: &mut dyn IngestMetadataValues, property: Option<u64>) {
        let Some(seconds) = property else {
            values.absent();
            return;
        };
        let timestamp = AmqpTimestamp(seconds);
        match timestamp.instant() {
            Some(instant) => values.datetime(instant),
            None => values.unrepresentable(IngestorMetadataType::Datetime, &timestamp),
        }
    }
}

/// The values of the metadata `RABBITMQ QUEUE` declares, in its order: the delivery's exchange,
/// routing key, delivery tag and redelivery flag, then the basic properties it carries. Each value is
/// read from the borrowed delivery, so handing them over copies nothing.
impl IngestMessageMetadata for RabbitMqQueueIngestorMessage {
    fn visit_metadata(&self, values: &mut dyn IngestMetadataValues) {
        let delivery = &self.headers.delivery;
        values.string(delivery.exchange.as_str());
        values.string(delivery.routing_key.as_str());
        values.u64(delivery.delivery_tag);
        values.bool(delivery.redelivered);
        let properties = &delivery.properties;
        Self::visit_text(values, properties.content_type().as_ref());
        Self::visit_text(values, properties.content_encoding().as_ref());
        Self::visit_text(values, properties.message_id().as_ref());
        Self::visit_text(values, properties.correlation_id().as_ref());
        Self::visit_text(values, properties.reply_to().as_ref());
        Self::visit_text(values, properties.kind().as_ref());
        Self::visit_text(values, properties.app_id().as_ref());
        Self::visit_text(values, properties.user_id().as_ref());
        match properties.priority() {
            Some(priority) => values.u8(*priority),
            None => values.absent(),
        }
        Self::visit_text(values, properties.expiration().as_ref());
        Self::visit_timestamp(values, *properties.timestamp());
    }
}

impl IngestorMessage for RabbitMqQueueIngestorMessage {
    type Position = RabbitMqQueueIngestorPosition;

    fn payload(&self) -> &[u8] {
        &self.headers.delivery.data
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.headers
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow {
            fields: self,
            headers: IngestRowHeaders::Borrowed(&self.headers),
            position: None,
        }
    }
}

/// One RabbitMQ ingestor instance: its AMQP session, absent while suspended or reconnecting.
pub struct RabbitMqQueueIngestor {
    plan: RabbitMqQueueIngestorPlan,
    instance_index: u64,
    consumer: Option<RabbitMqConsumer>,
}

#[async_trait]
impl IngestorConnector for RabbitMqQueueIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::RabbitMq(nervix_models::RabbitMqIngestorEntity::Queue);

    type Plan = RabbitMqQueueIngestorPlan;

    async fn open(plan: &Self::Plan, instance_index: u64) -> IngestorResult<Self> {
        Ok(Self {
            plan: plan.clone(),
            instance_index,
            consumer: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.consumer.is_none()
    }

    /// Suspension cancels the consumer by closing its channel and connection, which requeues
    /// every delivery still unacknowledged on them.
    async fn suspend(&mut self) -> IngestorResult<()> {
        self.consumer = None;
        Ok(())
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        if self.consumer.is_none() {
            let consumer = self
                .plan
                .consume(self.instance_index)
                .await
                .change_context(IngestorError::Resume {
                    connector: RABBITMQ,
                })?;
            self.consumer = Some(consumer);
        }
        Ok(IngestorResume::Ready)
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.consumer = None;
        Ok(())
    }
}

#[async_trait]
impl BrokerIngestorConnector for RabbitMqQueueIngestor {
    type Message = RabbitMqQueueIngestorMessage;
    type Position = RabbitMqQueueIngestorPosition;

    const SETTLEMENT: nervix_connector::IngestorSettlement =
        nervix_connector::IngestorSettlement::Individual;

    async fn next_batch(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        let Some(consumer) = self.consumer.as_mut() else {
            return Ok(IngestorBatch::ResumeRequired);
        };
        let batch = consumer.reader.read(request, cancellation).await;
        if batch.is_err() {
            self.consumer = None;
        }
        batch.change_context(IngestorError::Read {
            connector: RABBITMQ,
        })
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        if let RabbitMqConsumerMode::NoAck = self.plan.mode {
            return Ok(());
        }
        for position in positions {
            nervix_primitives::task::consume_budget().await;
            let settled = position
                .acker
                .ack(BasicAckOptions::default())
                .await
                .map_err(|source| {
                    Report::new(source).change_context(RabbitMqQueueIngestorError::Acknowledge)
                })
                .change_context(IngestorError::Acknowledge {
                    connector: RABBITMQ,
                })?;
            if !settled {
                return Err(Report::new(RabbitMqQueueIngestorError::DeliverySettled)
                    .change_context(IngestorError::Acknowledge {
                        connector: RABBITMQ,
                    }));
            }
        }
        Ok(())
    }

    /// A rejected delivery is negatively acknowledged with requeue, so the broker delivers it
    /// again rather than holding it against the prefetch window forever.
    async fn reject(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        if let RabbitMqConsumerMode::NoAck = self.plan.mode {
            return Ok(());
        }
        for position in positions {
            nervix_primitives::task::consume_budget().await;
            let settled = position
                .acker
                .nack(BasicNackOptions {
                    multiple: false,
                    requeue: true,
                })
                .await
                .map_err(|source| {
                    Report::new(source).change_context(RabbitMqQueueIngestorError::Requeue)
                })
                .change_context(IngestorError::Reject {
                    connector: RABBITMQ,
                })?;
            if !settled {
                return Err(Report::new(RabbitMqQueueIngestorError::DeliverySettled)
                    .change_context(IngestorError::Reject {
                        connector: RABBITMQ,
                    }));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use lapin::BasicProperties;
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_connector::{RetainedIngestMetadata, RetainedIngestMetadataValue as Value};

    use super::*;

    /// A delivery of the `orders.exchange` exchange with routing key `orders.created`, tag 7,
    /// delivered before, carrying `properties`.
    fn delivered(properties: BasicProperties) -> RabbitMqQueueIngestorMessage {
        let mut delivery = Delivery::mock(
            7,
            "orders.exchange".into(),
            "orders.created".into(),
            true,
            b"{}".to_vec(),
        );
        delivery.properties = properties;
        RabbitMqQueueIngestorMessage::new(delivery)
    }

    /// The last whole second after the Unix epoch that a signed count of nanoseconds holds.
    fn last_datetime_second() -> u64 {
        let nanos_per_second = Duration::from_secs(1).as_nanos();
        let last = u128::try_from(i64::MAX)
            .assured("i64::MAX is positive")
            .checked_div(nanos_per_second)
            .assured("one second is a nonzero count of nanoseconds");
        u64::try_from(last).assured("a quotient of i64::MAX fits in a u64")
    }

    fn instant_of(seconds: u64) -> Timestamp {
        let nanos = i64::try_from(Duration::from_secs(seconds).as_nanos())
            .assured("the fixture's seconds lie inside the DATETIME range");
        Timestamp::from_unix_nanos(nanos)
    }

    #[test]
    fn a_delivery_hands_over_its_envelope_and_every_basic_property_in_declared_order() {
        let message = delivered(
            BasicProperties::default()
                .with_content_type("application/json".into())
                .with_content_encoding("identity".into())
                .with_message_id("order-42".into())
                .with_correlation_id("request-7".into())
                .with_reply_to("order-replies".into())
                .with_type("order.created".into())
                .with_app_id("billing".into())
                .with_user_id("guest".into())
                .with_priority(u8::MAX)
                .with_expiration("600000".into())
                .with_timestamp(946_684_800)
                .with_delivery_mode(2)
                .with_cluster_id("unused".into()),
        );

        let retained = RetainedIngestMetadata::capture(&message);

        assert_eq!(
            retained.values(),
            [
                Value::String("orders.exchange".to_string()),
                Value::String("orders.created".to_string()),
                Value::U64(7),
                Value::Bool(true),
                Value::String("application/json".to_string()),
                Value::String("identity".to_string()),
                Value::String("order-42".to_string()),
                Value::String("request-7".to_string()),
                Value::String("order-replies".to_string()),
                Value::String("order.created".to_string()),
                Value::String("billing".to_string()),
                Value::String("guest".to_string()),
                Value::U8(u8::MAX),
                Value::String("600000".to_string()),
                Value::Datetime(instant_of(946_684_800)),
            ]
        );
        let declared = RabbitMqQueueIngestor::KIND.capabilities().metadata;
        assert_eq!(declared.fields().len(), retained.values().len());
        for (field, value) in declared.fields().iter().zip(retained.values()) {
            assert_eq!(value.ty(), Some(field.ty()), "metadata.{}", field.name());
        }
    }

    #[test]
    fn a_delivery_without_properties_hands_over_its_envelope_and_absent_properties() {
        let message = RabbitMqQueueIngestorMessage::new(Delivery::mock(
            1,
            "".into(),
            "orders".into(),
            false,
            Vec::new(),
        ));

        let retained = RetainedIngestMetadata::capture(&message);

        let mut expected = vec![
            Value::String(String::new()),
            Value::String("orders".to_string()),
            Value::U64(1),
            Value::Bool(false),
        ];
        expected.extend(std::iter::repeat_n(Value::Absent, 11));
        assert_eq!(retained.values(), expected.as_slice());
    }

    #[test]
    fn an_amqp_timestamp_past_the_last_datetime_second_is_handed_over_unrepresentable() {
        let last = last_datetime_second();
        let past = last
            .checked_add(1)
            .assured("the last DATETIME second lies far below u64::MAX");
        for (seconds, expected) in [
            (0, Value::Datetime(instant_of(0))),
            (last, Value::Datetime(instant_of(last))),
            (
                past,
                Value::Unrepresentable {
                    ty: IngestorMetadataType::Datetime,
                    value: format!("the AMQP timestamp {past} seconds after the Unix epoch"),
                },
            ),
            (
                u64::MAX,
                Value::Unrepresentable {
                    ty: IngestorMetadataType::Datetime,
                    value: format!(
                        "the AMQP timestamp {} seconds after the Unix epoch",
                        u64::MAX
                    ),
                },
            ),
        ] {
            let message = delivered(BasicProperties::default().with_timestamp(seconds));
            let retained = RetainedIngestMetadata::capture(&message);
            let timestamp = retained
                .values()
                .last()
                .verified("the declaration ends with metadata.timestamp");
            assert_eq!(timestamp, &expected, "{seconds} seconds");
        }
    }

    #[test]
    fn a_message_lends_its_metadata_and_headers_from_the_delivery() {
        let mut headers = FieldTable::default();
        headers.insert("tenant".into(), AMQPValue::LongString("north".into()));
        let message = delivered(BasicProperties::default().with_headers(headers));

        let row = message.metadata();

        assert_eq!(
            RetainedIngestMetadata::capture(row.fields),
            RetainedIngestMetadata::capture(&message)
        );
        let mut visited = Vec::new();
        row.headers
            .headers()
            .visit(&mut |name, value| visited.push(format!("{name}={value}")));
        assert_eq!(visited, ["tenant=north"]);
        assert!(row.position.is_none());
    }

    /// A resolver for literal addresses only: they need no query, so its name server is never
    /// asked.
    async fn literal_address_resolver() -> (tempfile::TempDir, DnsResolver) {
        let files = tempfile::tempdir().expect("a temporary directory can be created");
        let resolver_configuration = files.path().join("resolv.conf");
        let hosts_file = files.path().join("hosts");
        std::fs::write(&resolver_configuration, "options timeout:1 attempts:1\n")
            .expect("the resolver configuration can be written");
        std::fs::write(&hosts_file, "").expect("the hosts file can be written");
        let dns = DnsResolver::load(nervix_dns::DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: nervix_dns::NameServers::Explicit(vec![std::net::SocketAddr::from((
                [127, 0, 0, 1],
                53,
            ))]),
        })
        .await
        .expect("the fixture configuration is valid");
        (files, dns)
    }

    #[nervix_primitives::test]
    async fn failed_connections_keep_their_typed_cause_and_leave_the_source_to_resume() {
        let (_files, dns) = literal_address_resolver().await;
        let refusing = nervix_primitives::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port is available");
        let port = refusing
            .local_addr()
            .expect("a bound listener has an address")
            .port();
        drop(refusing);
        let cases = [
            (format!("amqp://guest:guest@127.0.0.1:{port}/%2f"), false),
            ("not a url".to_string(), true),
        ];
        for (addr, configuration) in cases {
            let plan = RabbitMqQueueIngestorPlan::new(
                vec![ClientConfigEntry {
                    key: "addr".to_string(),
                    value: addr.clone(),
                }],
                dns.clone(),
                RabbitMqQueueName::parse("orders").expect("a fixed queue name is valid"),
                "orders".to_string(),
                RabbitMqConsumerMode::Ack {
                    prefetch: NonZeroU16::MIN,
                },
            );
            let mut source = RabbitMqQueueIngestor::open(&plan, 0)
                .await
                .expect("opening a RabbitMQ ingestor connects nothing");
            let error = source
                .resume()
                .await
                .expect_err("a broker that cannot be reached fails the resume");
            assert!(matches!(
                error.current_context(),
                IngestorError::Resume {
                    connector: RABBITMQ
                }
            ));
            assert!(matches!(
                error.downcast_ref::<RabbitMqQueueIngestorError>(),
                Some(RabbitMqQueueIngestorError::Connect)
            ));
            let cause = error
                .downcast_ref::<crate::queue::RabbitMqConnectError>()
                .expect("the connection failure stays beneath the ingestor failure");
            assert_eq!(cause.is_configuration(), configuration, "{addr}");
            assert!(source.needs_resume(), "{addr}");
        }
    }

    #[nervix_primitives::test]
    async fn automatic_consumption_never_uses_a_deliveries_transport_acknowledgement() {
        let (_files, dns) = literal_address_resolver().await;
        let plan = RabbitMqQueueIngestorPlan::new(
            Vec::new(),
            dns,
            RabbitMqQueueName::parse("orders").expect("a fixed queue name is valid"),
            "orders".into(),
            RabbitMqConsumerMode::NoAck,
        );
        let mut source = RabbitMqQueueIngestor::open(&plan, 0)
            .await
            .expect("opening connects nothing");
        let delivery = Delivery::mock(1, "".into(), "orders".into(), false, vec![1]);
        let position = RabbitMqQueueIngestorPosition {
            acker: delivery.acker,
        };
        source
            .acknowledge(std::slice::from_ref(&position))
            .await
            .expect("an automatic delivery has no acknowledgement to send");
        source
            .reject(std::slice::from_ref(&position))
            .await
            .expect("an automatic delivery has no rejection to send");
        assert!(
            position.acker.usable(),
            "neither host operation may consume the AMQP acker"
        );
    }

    #[test]
    fn header_values_render_scalars_and_collections_as_strings() {
        assert_eq!(
            RabbitMqDeliveryHeaders::header_value(&AMQPValue::LongInt(-7)),
            "-7"
        );
        assert_eq!(
            RabbitMqDeliveryHeaders::header_value(&AMQPValue::LongString("v".into())),
            "v"
        );
        assert_eq!(
            RabbitMqDeliveryHeaders::header_value(&AMQPValue::FieldArray(
                vec![AMQPValue::Boolean(true), AMQPValue::ShortString("x".into())].into()
            )),
            "true,x"
        );
        assert_eq!(RabbitMqDeliveryHeaders::header_value(&AMQPValue::Void), "");
    }

    /// Both RabbitMQ QUEUE sides declare sorted by name with exact names: the AMQP field table a
    /// emitter writes keeps one value per name, the last one written, and a reader of the published
    /// message visits the names in ascending byte order, names that differ in letter case staying
    /// two names.
    #[test]
    fn written_headers_read_back_one_value_per_name_in_ascending_name_order() {
        let mut written = Vec::new();
        for (name, value) in [
            ("trace", "a"),
            ("Tenant", "x"),
            ("trace", "b"),
            ("tenant", "y"),
        ] {
            written.push((name.to_string(), value.to_string()));
        }
        let mut delivery = Delivery::mock(0, "".into(), "orders".into(), false, Vec::new());
        delivery.properties = crate::queue::RabbitMqQueueEmitter::properties(&written);
        let headers = RabbitMqDeliveryHeaders { delivery };
        let mut visited = Vec::new();
        headers.visit(&mut |name, value| visited.push(format!("{name}={value}")));
        assert_eq!(visited, ["Tenant=x", "tenant=y", "trace=b"]);
    }
}
