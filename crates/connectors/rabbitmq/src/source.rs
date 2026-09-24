//! RabbitMQ source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The AMQP connection, channel and consumer each source instance reads through, its
//!   one-delivery prefetch window, AMQP headers as ingest headers, and per-delivery
//!   acknowledgement and requeue.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   Tokio and `lapin`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

use std::borrow::Cow;

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::StreamExt as _;
use lapin::{
    Acker, Channel, Connection, ConnectionProperties, Consumer,
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    tcp::OwnedTLSConfig,
    types::{AMQPValue, FieldTable},
};
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, ServiceUrl, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceResult, SourceResume,
    client_config_value, client_tls_paths, read_tls_file,
};
use nervix_models::{ClientConfigEntry, QueueName};
use thiserror::Error;

const RABBITMQ: &str = "rabbitmq";

/// Why a RabbitMQ source could not connect, consume, or settle a delivery.
#[derive(Debug, Error)]
pub enum RabbitMqSourceError {
    #[error("invalid RabbitMQ client configuration")]
    ClientConfig,
    #[error("failed to parse RabbitMQ CA certificate")]
    ParseCaCertificate,
    #[error("RabbitMQ runtime is unavailable")]
    RuntimeUnavailable,
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
}

type RabbitMqSourceResult<T> = Result<T, Report<RabbitMqSourceError>>;

/// What one RabbitMQ source consumes: its resolved client entries, the queue, and the consumer
/// tag each instance registers under, suffixed with its instance index.
#[derive(Clone)]
pub struct RabbitMqSourcePlan {
    config: Vec<ClientConfigEntry>,
    queue: QueueName,
    consumer_tag: String,
}

/// The AMQP session one instance consumes through, kept together because dropping the channel
/// or the connection requeues every delivery it holds unacknowledged.
struct RabbitMqConsumer {
    _connection: Connection,
    _channel: Channel,
    consumer: Consumer,
}

impl RabbitMqSourcePlan {
    pub fn new(config: Vec<ClientConfigEntry>, queue: QueueName, consumer_tag: String) -> Self {
        Self {
            config,
            queue,
            consumer_tag,
        }
    }

    async fn consume(&self, instance_index: u64) -> RabbitMqSourceResult<RabbitMqConsumer> {
        let connection = Self::connection_from_config(&self.config).await?;
        let channel = connection.create_channel().await.map_err(|source| {
            Report::new(RabbitMqSourceError::Channel).attach_printable(source.to_string())
        })?;
        channel
            .basic_qos(1, BasicQosOptions::default())
            .await
            .map_err(|source| {
                Report::new(RabbitMqSourceError::Qos).attach_printable(source.to_string())
            })?;
        let consumer = channel
            .basic_consume(
                self.queue.as_str().into(),
                format!("{}-{instance_index}", self.consumer_tag).into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|source| {
                Report::new(RabbitMqSourceError::Consume {
                    queue: self.queue.as_str().to_string(),
                })
                .attach_printable(source.to_string())
            })?;
        Ok(RabbitMqConsumer {
            _connection: connection,
            _channel: channel,
            consumer,
        })
    }

    async fn connection_from_config(
        config: &[ClientConfigEntry],
    ) -> RabbitMqSourceResult<Connection> {
        let addr = client_config_value(config, "addr", "RabbitMQ")
            .change_context(RabbitMqSourceError::ClientConfig)?;
        if ServiceUrl::new(&addr, "RabbitMQ addr")
            .has_scheme("amqps")
            .change_context(RabbitMqSourceError::ClientConfig)?
        {
            let tls = client_tls_paths(config);
            let cert_chain = if let Some(ca_file) = tls.ca_file.as_ref() {
                Some(
                    String::from_utf8(
                        read_tls_file(ca_file, "TLS CA certificate")
                            .change_context(RabbitMqSourceError::ClientConfig)?,
                    )
                    .map_err(|source| {
                        Report::new(RabbitMqSourceError::ParseCaCertificate)
                            .attach_printable(source.to_string())
                    })?,
                )
            } else {
                None
            };
            Connection::connect_with_config(
                &addr,
                ConnectionProperties::default(),
                OwnedTLSConfig {
                    identity: None,
                    cert_chain,
                },
                lapin::runtime::default_runtime().map_err(|source| {
                    Report::new(RabbitMqSourceError::RuntimeUnavailable)
                        .attach_printable(source.to_string())
                })?,
            )
            .await
            .map_err(|source| {
                Report::new(RabbitMqSourceError::Connect).attach_printable(source.to_string())
            })
        } else {
            Connection::connect(&addr, ConnectionProperties::default())
                .await
                .map_err(|source| {
                    Report::new(RabbitMqSourceError::Connect).attach_printable(source.to_string())
                })
        }
    }
}

/// The acknowledgement handle of one delivery, which settles it on the channel it arrived on.
#[derive(Debug, Clone)]
pub struct RabbitMqSourcePosition {
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

pub struct RabbitMqSourceMessage {
    headers: RabbitMqDeliveryHeaders,
    position: RabbitMqSourcePosition,
}

impl RabbitMqSourceMessage {
    fn new(delivery: Delivery) -> Self {
        let position = RabbitMqSourcePosition {
            acker: delivery.acker.clone(),
        };
        Self {
            headers: RabbitMqDeliveryHeaders { delivery },
            position,
        }
    }
}

impl SourceMessage for RabbitMqSourceMessage {
    type Position = RabbitMqSourcePosition;

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
        IngestMetadataRow::Headers {
            headers: &self.headers,
        }
    }
}

/// One RabbitMQ source instance: its AMQP session, absent while suspended or reconnecting.
pub struct RabbitMqSource {
    plan: RabbitMqSourcePlan,
    instance_index: u64,
    consumer: Option<RabbitMqConsumer>,
}

#[async_trait]
impl SourceConnector for RabbitMqSource {
    type Plan = RabbitMqSourcePlan;

    async fn open(plan: &Self::Plan, instance_index: u64) -> SourceResult<Self> {
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
    async fn suspend(&mut self) -> SourceResult<()> {
        self.consumer = None;
        Ok(())
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if self.consumer.is_none() {
            let consumer = self
                .plan
                .consume(self.instance_index)
                .await
                .change_context(SourceError::Resume {
                    connector: RABBITMQ,
                })?;
            self.consumer = Some(consumer);
        }
        Ok(SourceResume::Ready)
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.consumer = None;
        Ok(())
    }
}

#[async_trait]
impl BrokerSourceConnector for RabbitMqSource {
    type Message = RabbitMqSourceMessage;
    type Position = RabbitMqSourcePosition;

    /// The prefetch window admits one delivery at a time, so a batch holds one delivery whatever
    /// the request asks for.
    async fn next_batch(
        &mut self,
        _request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        let Some(consumer) = self.consumer.as_mut() else {
            return Ok(SourceBatch::ResumeRequired);
        };
        match consumer.consumer.next().await {
            Some(Ok(delivery)) => Ok(SourceBatch::Messages(vec![RabbitMqSourceMessage::new(
                delivery,
            )])),
            Some(Err(error)) => {
                self.consumer = None;
                Err(Report::new(RabbitMqSourceError::Receive)
                    .attach_printable(error.to_string())
                    .change_context(SourceError::Read {
                        connector: RABBITMQ,
                    }))
            }
            None => {
                self.consumer = None;
                Err(
                    Report::new(RabbitMqSourceError::ConsumerClosed).change_context(
                        SourceError::Read {
                            connector: RABBITMQ,
                        },
                    ),
                )
            }
        }
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        for position in positions {
            tokio::task::consume_budget().await;
            let settled = position
                .acker
                .ack(BasicAckOptions::default())
                .await
                .map_err(|source| {
                    Report::new(RabbitMqSourceError::Acknowledge)
                        .attach_printable(source.to_string())
                })
                .change_context(SourceError::Acknowledge {
                    connector: RABBITMQ,
                })?;
            if !settled {
                return Err(
                    Report::new(RabbitMqSourceError::DeliverySettled).change_context(
                        SourceError::Acknowledge {
                            connector: RABBITMQ,
                        },
                    ),
                );
            }
        }
        Ok(())
    }

    /// A rejected delivery is negatively acknowledged with requeue, so the broker delivers it
    /// again rather than holding it against the prefetch window forever.
    async fn reject(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        for position in positions {
            tokio::task::consume_budget().await;
            let settled = position
                .acker
                .nack(BasicNackOptions {
                    multiple: false,
                    requeue: true,
                })
                .await
                .map_err(|source| {
                    Report::new(RabbitMqSourceError::Requeue).attach_printable(source.to_string())
                })
                .change_context(SourceError::Reject {
                    connector: RABBITMQ,
                })?;
            if !settled {
                return Err(
                    Report::new(RabbitMqSourceError::DeliverySettled).change_context(
                        SourceError::Reject {
                            connector: RABBITMQ,
                        },
                    ),
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connection_failures_keep_typed_context() {
        let unavailable = [ClientConfigEntry {
            key: "addr".to_string(),
            value: "amqp://127.0.0.1:1/%2f".to_string(),
        }];
        let error = RabbitMqSourcePlan::connection_from_config(&unavailable)
            .await
            .expect_err("an unavailable RabbitMQ broker must fail connection");
        assert!(matches!(
            error.current_context(),
            RabbitMqSourceError::Connect
        ));

        let root = tempfile::tempdir().expect("temporary RabbitMQ TLS directory should open");
        let ca = root.path().join("ca.pem");
        std::fs::write(&ca, [0xff]).expect("the non-UTF-8 RabbitMQ CA fixture should be writable");
        let invalid_ca = [
            ClientConfigEntry {
                key: "addr".to_string(),
                value: "amqps://127.0.0.1:1/%2f".to_string(),
            },
            ClientConfigEntry {
                key: "tls_ca_file".to_string(),
                value: ca.to_string_lossy().into_owned(),
            },
        ];
        let error = RabbitMqSourcePlan::connection_from_config(&invalid_ca)
            .await
            .expect_err("a non-UTF-8 RabbitMQ CA must fail parsing");
        assert!(matches!(
            error.current_context(),
            RabbitMqSourceError::ParseCaCertificate
        ));
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
}
