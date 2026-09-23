//! Pulsar source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The Pulsar client a source configuration declares, its TLS options, topic
//!   qualification, the shared-subscription consumer each instance reads through, message
//!   properties as ingest headers, and per-message acknowledgement and negative acknowledgement.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   Tokio and `pulsar`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

use std::num::NonZeroUsize;

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::StreamExt as _;
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceResult, SourceResume,
    client_config_value, client_tls_paths, optional_bool_client_config_value,
    optional_client_config_value, read_tls_file,
};
use nervix_models::{ClientConfigEntry, PulsarSubscriptionName, TopicName};
use pulsar::{
    Consumer as PulsarConsumer, ConsumerOptions as PulsarConsumerOptions, Pulsar,
    SubType as PulsarSubType, TlsOptions as PulsarTlsOptions, TokioExecutor,
    consumer::{InitialPosition as PulsarInitialPosition, Message as PulsarMessage},
    proto::MessageIdData,
};
use thiserror::Error;
use tokio::time::{Instant, sleep_until};
use tracing::warn;

const PULSAR: &str = "pulsar";

/// Why a Pulsar source could not connect, consume, or settle a message.
#[derive(Debug, Error)]
pub enum PulsarSourceError {
    #[error("invalid Pulsar client configuration")]
    ClientConfig,
    #[error(
        "Pulsar TLS supports 'tls_ca_file' but does not support client certificate authentication"
    )]
    UnsupportedTlsIdentity,
    #[error("failed to connect Pulsar client")]
    Connect,
    #[error("failed to subscribe to Pulsar topic '{topic}'")]
    Subscribe { topic: String },
    #[error("failed to receive a Pulsar message")]
    Receive,
    #[error("Pulsar consumer closed")]
    ConsumerClosed,
    #[error("failed to acknowledge a Pulsar message")]
    Acknowledge,
    #[error("failed to negatively acknowledge a Pulsar message")]
    NegativelyAcknowledge,
    #[error("failed to close the Pulsar consumer")]
    Close,
    #[error("Pulsar batch timeout exceeds the monotonic clock range")]
    BatchDeadline,
}

type PulsarSourceResult<T> = Result<T, Report<PulsarSourceError>>;

/// What one Pulsar source subscribes with: its resolved client entries, the topic it reads, the
/// shared subscription it joins, and the name its consumers register under.
pub struct PulsarSourceSettings<'a> {
    pub config: &'a [ClientConfigEntry],
    pub topic: &'a TopicName,
    pub subscription: PulsarSubscriptionName,
    /// Each instance's consumer registers under this name suffixed with its instance index.
    pub consumer_name: String,
}

/// One Pulsar source's client and the shared subscription every instance consumes from.
///
/// The client is connected once and shared by the instances, which each open their own consumer.
#[derive(Clone)]
pub struct PulsarSourcePlan {
    client: Pulsar<TokioExecutor>,
    topic: String,
    subscription: PulsarSubscriptionName,
    consumer_name: String,
}

impl PulsarSourcePlan {
    pub async fn connect(settings: PulsarSourceSettings<'_>) -> PulsarSourceResult<Self> {
        let PulsarSourceSettings {
            config,
            topic,
            subscription,
            consumer_name,
        } = settings;
        let client = Self::client_from_config(config).await?;
        Ok(Self {
            client,
            topic: Self::topic_from_config(config, topic.as_str()),
            subscription,
            consumer_name,
        })
    }

    async fn client_from_config(
        config: &[ClientConfigEntry],
    ) -> PulsarSourceResult<Pulsar<TokioExecutor>> {
        let addr = client_config_value(config, "addr", "Pulsar")
            .change_context(PulsarSourceError::ClientConfig)?;
        let mut builder = Pulsar::builder(addr, TokioExecutor);
        if let Some(tls_options) = Self::tls_options_from_config(config)? {
            if let Some(certificate_chain) = tls_options.certificate_chain {
                builder = builder.with_certificate_chain(certificate_chain);
            }
            builder = builder
                .with_allow_insecure_connection(tls_options.allow_insecure_connection)
                .with_tls_hostname_verification_enabled(
                    tls_options.tls_hostname_verification_enabled,
                );
        }
        builder.build().await.map_err(|source| {
            Report::new(PulsarSourceError::Connect).attach_printable(source.to_string())
        })
    }

    fn topic_from_config(config: &[ClientConfigEntry], topic: &str) -> String {
        if topic.contains("://") {
            return topic.to_string();
        }

        let namespace =
            optional_client_config_value(config, "namespace").unwrap_or("public/default");
        format!("persistent://{namespace}/{topic}")
    }

    fn tls_options_from_config(
        config: &[ClientConfigEntry],
    ) -> PulsarSourceResult<Option<PulsarTlsOptions>> {
        let tls = client_tls_paths(config);
        if tls.cert_file.is_some() || tls.key_file.is_some() {
            return Err(Report::new(PulsarSourceError::UnsupportedTlsIdentity));
        }

        let allow_insecure_connection =
            optional_bool_client_config_value(config, "tls_allow_insecure_connection")
                .change_context(PulsarSourceError::ClientConfig)?;
        let tls_hostname_verification_enabled =
            optional_bool_client_config_value(config, "tls_hostname_verification_enabled")
                .change_context(PulsarSourceError::ClientConfig)?;

        if tls.ca_file.is_none()
            && allow_insecure_connection.is_none()
            && tls_hostname_verification_enabled.is_none()
        {
            return Ok(None);
        }

        let mut tls_options = PulsarTlsOptions::default();
        if let Some(ca_file) = tls.ca_file.as_ref() {
            tls_options.certificate_chain = Some(
                read_tls_file(ca_file, "TLS CA certificate")
                    .change_context(PulsarSourceError::ClientConfig)?,
            );
        }
        if let Some(allow_insecure_connection) = allow_insecure_connection {
            tls_options.allow_insecure_connection = allow_insecure_connection;
        }
        if let Some(tls_hostname_verification_enabled) = tls_hostname_verification_enabled {
            tls_options.tls_hostname_verification_enabled = tls_hostname_verification_enabled;
        }
        Ok(Some(tls_options))
    }

    async fn consumer(
        &self,
        instance_index: u64,
    ) -> PulsarSourceResult<PulsarConsumer<Vec<u8>, TokioExecutor>> {
        self.client
            .consumer()
            .with_topic(self.topic.as_str())
            .with_consumer_name(format!("{}-{instance_index}", self.consumer_name))
            .with_subscription(self.subscription.as_str())
            .with_subscription_type(PulsarSubType::Shared)
            .with_options(
                PulsarConsumerOptions::default()
                    .with_initial_position(PulsarInitialPosition::Earliest),
            )
            .build()
            .await
            .map_err(|source| {
                Report::new(PulsarSourceError::Subscribe {
                    topic: self.topic.clone(),
                })
                .attach_printable(source.to_string())
            })
    }
}

/// The message one acknowledgement or negative acknowledgement settles: the topic it arrived on,
/// which a partitioned subscription routes by, and its Pulsar message id.
#[derive(Debug, Clone)]
pub struct PulsarSourcePosition {
    topic: String,
    message_id: MessageIdData,
}

/// One received Pulsar message, whose properties are its ingest headers.
pub struct PulsarMessageProperties {
    message: PulsarMessage<Vec<u8>>,
}

impl IngestMessageHeaders for PulsarMessageProperties {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for property in &self.message.metadata().properties {
            visit(&property.key, &property.value);
        }
    }
}

pub struct PulsarSourceMessage {
    properties: PulsarMessageProperties,
    position: PulsarSourcePosition,
}

impl PulsarSourceMessage {
    fn new(message: PulsarMessage<Vec<u8>>) -> Self {
        let position = PulsarSourcePosition {
            topic: message.topic.clone(),
            message_id: message.message_id().clone(),
        };
        Self {
            properties: PulsarMessageProperties { message },
            position,
        }
    }
}

impl SourceMessage for PulsarSourceMessage {
    type Position = PulsarSourcePosition;

    fn payload(&self) -> &[u8] {
        &self.properties.message.payload.data
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.properties
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow::Headers {
            headers: &self.properties,
        }
    }
}

/// One Pulsar source instance: its consumer on the shared subscription, absent while suspended.
pub struct PulsarSource {
    plan: PulsarSourcePlan,
    instance_index: u64,
    consumer: Option<PulsarConsumer<Vec<u8>, TokioExecutor>>,
}

impl PulsarSource {
    async fn close_consumer(&mut self) -> PulsarSourceResult<()> {
        let Some(mut consumer) = self.consumer.take() else {
            return Ok(());
        };
        consumer.close().await.map_err(|source| {
            Report::new(PulsarSourceError::Close).attach_printable(source.to_string())
        })
    }
}

#[async_trait]
impl SourceConnector for PulsarSource {
    type Plan = PulsarSourcePlan;

    async fn open(plan: &Self::Plan, instance_index: u64) -> SourceResult<Self> {
        let consumer = plan
            .consumer(instance_index)
            .await
            .change_context(SourceError::Open { connector: PULSAR })?;
        Ok(Self {
            plan: plan.clone(),
            instance_index,
            consumer: Some(consumer),
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.consumer.is_none()
    }

    /// Suspension disconnects the consumer, and the durable subscription redelivers what it held
    /// unacknowledged.
    async fn suspend(&mut self) -> SourceResult<()> {
        self.close_consumer()
            .await
            .change_context(SourceError::Suspend { connector: PULSAR })
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if self.consumer.is_none() {
            let consumer = self
                .plan
                .consumer(self.instance_index)
                .await
                .change_context(SourceError::Resume { connector: PULSAR })?;
            self.consumer = Some(consumer);
        }
        Ok(SourceResume::Ready)
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.close_consumer()
            .await
            .change_context(SourceError::Close { connector: PULSAR })
    }
}

#[async_trait]
impl BrokerSourceConnector for PulsarSource {
    type Message = PulsarSourceMessage;
    type Position = PulsarSourcePosition;

    async fn next_batch(
        &mut self,
        request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        let Some(consumer) = self.consumer.as_mut() else {
            return Ok(SourceBatch::ResumeRequired);
        };
        let first = match consumer.next().await {
            Some(Ok(message)) => message,
            Some(Err(error)) => {
                return Err(Report::new(PulsarSourceError::Receive)
                    .attach_printable(error.to_string())
                    .change_context(SourceError::Read { connector: PULSAR }));
            }
            None => {
                self.consumer = None;
                return Err(Report::new(PulsarSourceError::ConsumerClosed)
                    .change_context(SourceError::Read { connector: PULSAR }));
            }
        };
        let mut messages = Vec::with_capacity(request.max_messages.get());
        messages.push(PulsarSourceMessage::new(first));
        if request.max_messages == NonZeroUsize::MIN {
            return Ok(SourceBatch::Messages(messages));
        }
        let Some(batch_timeout) = request.batch_timeout else {
            return Ok(SourceBatch::Messages(messages));
        };
        let Some(deadline) = Instant::now().checked_add(batch_timeout) else {
            return Err(Report::new(PulsarSourceError::BatchDeadline)
                .change_context(SourceError::Read { connector: PULSAR }));
        };
        while messages.len() < request.max_messages.get() {
            tokio::task::consume_budget().await;
            tokio::select! {
                _ = sleep_until(deadline) => break,
                next = consumer.next() => {
                    match next {
                        Some(Ok(message)) => messages.push(PulsarSourceMessage::new(message)),
                        Some(Err(error)) => {
                            warn!(error = %error, "failed to receive another Pulsar batch message");
                        }
                        None => break,
                    }
                }
            }
        }
        Ok(SourceBatch::Messages(messages))
    }

    async fn acknowledge(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        let Some(consumer) = self.consumer.as_mut() else {
            return Err(Report::new(PulsarSourceError::ConsumerClosed)
                .change_context(SourceError::Acknowledge { connector: PULSAR }));
        };
        for position in positions {
            tokio::task::consume_budget().await;
            consumer
                .ack_with_id(&position.topic, position.message_id.clone())
                .await
                .map_err(|source| {
                    Report::new(PulsarSourceError::Acknowledge).attach_printable(source.to_string())
                })
                .change_context(SourceError::Acknowledge { connector: PULSAR })?;
        }
        Ok(())
    }

    /// A rejected message is negatively acknowledged, which asks the broker to redeliver it on the
    /// shared subscription.
    async fn reject(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        let Some(consumer) = self.consumer.as_mut() else {
            return Err(Report::new(PulsarSourceError::ConsumerClosed)
                .change_context(SourceError::Reject { connector: PULSAR }));
        };
        for position in positions {
            tokio::task::consume_budget().await;
            consumer
                .nack_with_id(&position.topic, position.message_id.clone())
                .await
                .map_err(|source| {
                    Report::new(PulsarSourceError::NegativelyAcknowledge)
                        .attach_printable(source.to_string())
                })
                .change_context(SourceError::Reject { connector: PULSAR })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn short_topic_names_are_qualified_by_the_configured_namespace() {
        assert_eq!(
            PulsarSourcePlan::topic_from_config(&[], "events"),
            "persistent://public/default/events"
        );
        assert_eq!(
            PulsarSourcePlan::topic_from_config(&[entry("namespace", "tenant/ns")], "events"),
            "persistent://tenant/ns/events"
        );
        assert_eq!(
            PulsarSourcePlan::topic_from_config(&[], "non-persistent://tenant/ns/events"),
            "non-persistent://tenant/ns/events"
        );
    }

    #[test]
    fn tls_options_load_trust_and_reject_client_identities() {
        assert!(
            PulsarSourcePlan::tls_options_from_config(&[])
                .expect("no TLS keys must be accepted")
                .is_none()
        );

        let error =
            PulsarSourcePlan::tls_options_from_config(&[entry("tls_cert_file", "client.pem")])
                .expect_err("a client certificate must be rejected");
        assert!(matches!(
            error.current_context(),
            PulsarSourceError::UnsupportedTlsIdentity
        ));

        let error = PulsarSourcePlan::tls_options_from_config(&[entry(
            "tls_allow_insecure_connection",
            "sometimes",
        )])
        .expect_err("an invalid boolean must be rejected");
        assert!(matches!(
            error.current_context(),
            PulsarSourceError::ClientConfig
        ));

        let root = tempfile::tempdir().expect("temporary Pulsar TLS directory should open");
        let ca = root.path().join("ca.pem");
        std::fs::write(&ca, b"test CA").expect("the Pulsar CA fixture should be writable");
        let options = PulsarSourcePlan::tls_options_from_config(&[
            entry("tls_ca_file", &ca.to_string_lossy()),
            entry("tls_hostname_verification_enabled", "false"),
        ])
        .expect("a CA file must be accepted")
        .expect("a CA file enables TLS options");
        assert_eq!(options.certificate_chain.as_deref(), Some(&b"test CA"[..]));
        assert!(!options.tls_hostname_verification_enabled);
        assert!(!options.allow_insecure_connection);
    }
}
