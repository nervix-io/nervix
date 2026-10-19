//! Pulsar source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The Pulsar client a source configuration declares, its TLS options, topic
//!   qualification, the shared-subscription consumer each instance reads through, message
//!   properties as ingest headers, acknowledgements the broker answers, redelivery requests, and
//!   the classification of an acknowledgement or a redelivery request that did not take effect.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   Tokio and `pulsar`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

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

use std::{collections::BTreeMap, fmt, io, num::NonZeroUsize};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::StreamExt as _;
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceReadCancellation,
    SourceResult, SourceResume, client_config_value, client_tls_paths,
    optional_bool_client_config_value, optional_client_config_value, read_tls_file,
};
use nervix_models::{ClientConfigEntry, PulsarSubscriptionName, PulsarTopicName};
use nervix_primitives::time::{Instant, sleep_until};
use pulsar::{
    Consumer as PulsarConsumer, ConsumerOptions as PulsarConsumerOptions, Pulsar,
    SubType as PulsarSubType, TlsOptions as PulsarTlsOptions, TokioExecutor,
    consumer::{InitialPosition as PulsarInitialPosition, Message as PulsarMessage},
    error::{ConnectionError as PulsarConnectionError, ConsumerError as PulsarConsumerError},
    message::proto::ServerError as PulsarServerError,
    proto::MessageIdData,
};
use thiserror::Error;
use tracing::warn;

const PULSAR: &str = "pulsar";

/// What a Pulsar broker answered when it refused a request: its server error, absent when the
/// client does not know the error's code, and the reason it gave, absent when it gave none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PulsarRefusal {
    pub error: Option<PulsarServerError>,
    pub reason: Option<String>,
}

impl fmt::Display for PulsarRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.error {
            Some(error) => formatter.write_str(error.as_str_name())?,
            None => formatter.write_str("a server error this client does not know")?,
        }
        if let Some(reason) = &self.reason {
            write!(formatter, ": {reason}")?;
        }
        Ok(())
    }
}

/// Why a request of a Pulsar consumer has no outcome it can rely on: the broker may or may not
/// have applied it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PulsarUnresolvedRequest {
    /// Nothing answered within the client's operation timeout, such as while its connection is
    /// stalled or a reconnection holds the consumer back.
    #[error("the client's operation timeout passed first")]
    OperationTimeout,
    /// The connection closed before the request was answered or sent.
    #[error("the connection closed first")]
    ConnectionClosed,
    /// The consumer stopped before the request was answered or sent.
    #[error("the consumer stopped first")]
    ConsumerStopped,
}

impl PulsarUnresolvedRequest {
    /// What `error` leaves unresolved, when it says the request went unanswered rather than that
    /// the broker refused it or the client failed otherwise.
    fn of(error: &PulsarConsumerError) -> Option<Self> {
        match error {
            PulsarConsumerError::Connection(PulsarConnectionError::Io(source))
                if source.kind() == io::ErrorKind::TimedOut =>
            {
                Some(Self::OperationTimeout)
            }
            PulsarConsumerError::Connection(
                PulsarConnectionError::Disconnected
                | PulsarConnectionError::Canceled
                | PulsarConnectionError::Shutdown,
            ) => Some(Self::ConnectionClosed),
            PulsarConsumerError::Closed => Some(Self::ConsumerStopped),
            _ => None,
        }
    }
}

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
    /// The broker refused an acknowledgement, so the messages it named stay unacknowledged on the
    /// subscription.
    #[error("the Pulsar broker refused an acknowledgement with {refusal}")]
    AcknowledgementRefused { refusal: PulsarRefusal },
    /// The broker did not confirm an acknowledgement, so whether it applied it is unknown.
    #[error("the Pulsar broker did not confirm an acknowledgement: {unresolved}")]
    AcknowledgementUnconfirmed { unresolved: PulsarUnresolvedRequest },
    /// The consumer did not confirm that it sent a redelivery request. The broker answers none,
    /// and it redelivers an unacknowledged message once the connection that delivered it is gone.
    #[error("the Pulsar consumer did not confirm sending a redelivery request: {unresolved}")]
    RedeliveryRequestUnsent { unresolved: PulsarUnresolvedRequest },
    /// An acknowledgement failed in a way this source does not classify; the client's error is the
    /// cause beneath.
    #[error("failed to acknowledge a Pulsar message")]
    Acknowledge,
    /// A redelivery request failed in a way this source does not classify; the client's error is
    /// the cause beneath.
    #[error("failed to negatively acknowledge a Pulsar message")]
    NegativelyAcknowledge,
    #[error("failed to close the Pulsar consumer")]
    Close,
    #[error("Pulsar batch timeout exceeds the monotonic clock range")]
    BatchDeadline,
}

impl PulsarSourceError {
    /// What a failed acknowledgement means: the refusal the broker answered with, an answer the
    /// client could not get, or a failure this source does not classify, which keeps the client's
    /// error beneath it.
    fn unsettled_acknowledgement(error: PulsarConsumerError) -> Report<Self> {
        if let PulsarConsumerError::Connection(PulsarConnectionError::PulsarError(code, reason)) =
            &error
        {
            return Report::new(Self::AcknowledgementRefused {
                refusal: PulsarRefusal {
                    error: *code,
                    reason: reason.clone(),
                },
            });
        }
        if let Some(unresolved) = PulsarUnresolvedRequest::of(&error) {
            return Report::new(Self::AcknowledgementUnconfirmed { unresolved });
        }
        Report::new(error).change_context(Self::Acknowledge)
    }

    /// What a failed redelivery request means: a request the consumer could not confirm sending,
    /// or a failure this source does not classify, which keeps the client's error beneath it.
    fn unsent_redelivery_request(error: PulsarConsumerError) -> Report<Self> {
        if let Some(unresolved) = PulsarUnresolvedRequest::of(&error) {
            return Report::new(Self::RedeliveryRequestUnsent { unresolved });
        }
        Report::new(error).change_context(Self::NegativelyAcknowledge)
    }
}

type PulsarSourceResult<T> = Result<T, Report<PulsarSourceError>>;

/// Whether acknowledging a Pulsar source's positions waits for the broker's answer, which the
/// ingestor's acknowledgement mode decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PulsarAckReceipts {
    /// `MODE NO ACK`: acknowledging returns once the client queued the acknowledgements. A mode
    /// that holds no acknowledgement tree reads one message at a time, and waiting for the broker
    /// would add a round trip to every read.
    Unawaited,
    /// An `ACK` mode: acknowledging returns once the broker answered the acknowledgements, which
    /// is the progress the host reports for the batch.
    Awaited,
}

impl PulsarAckReceipts {
    /// Whether the client sends acknowledgements with a request id and waits for their answer.
    fn enabled(self) -> bool {
        match self {
            Self::Unawaited => false,
            Self::Awaited => true,
        }
    }
}

/// What one Pulsar source subscribes with: its resolved client entries, the topic it reads, the
/// shared subscription it joins, the name its consumers register under, and whether acknowledging
/// waits for the broker.
pub struct PulsarSourceSettings<'a> {
    pub config: &'a [ClientConfigEntry],
    pub topic: &'a PulsarTopicName,
    pub subscription: PulsarSubscriptionName,
    /// Each instance's consumer registers under this name suffixed with its instance index.
    pub consumer_name: String,
    pub ack_receipts: PulsarAckReceipts,
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
    ack_receipts: PulsarAckReceipts,
}

impl PulsarSourcePlan {
    pub async fn connect(settings: PulsarSourceSettings<'_>) -> PulsarSourceResult<Self> {
        let PulsarSourceSettings {
            config,
            topic,
            subscription,
            consumer_name,
            ack_receipts,
        } = settings;
        let client = Self::client_from_config(config).await?;
        Ok(Self {
            client,
            topic: Self::topic_from_config(config, topic.as_str()),
            subscription,
            consumer_name,
            ack_receipts,
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
                    .with_initial_position(PulsarInitialPosition::Earliest)
                    .with_ack_receipt_enabled(self.ack_receipts.enabled()),
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
        cancellation: &SourceReadCancellation,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        let Some(consumer) = self.consumer.as_mut() else {
            return Ok(SourceBatch::ResumeRequired);
        };
        let Some(first) = cancellation.unless_cancelled(consumer.next()).await else {
            return Ok(SourceBatch::Interrupted {
                gathered: Vec::new(),
            });
        };
        let first = match first {
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
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
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
                () = cancellation.cancelled() => {
                    return Ok(SourceBatch::Interrupted { gathered: messages });
                }
            }
        }
        Ok(SourceBatch::Messages(messages))
    }

    /// The positions of each topic are acknowledged in one command. Under an `ACK` mode
    /// acknowledging returns once the broker answered every one of them, within the client's
    /// operation timeout; under `MODE NO ACK`, once the client queued them.
    async fn acknowledge(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        let Some(consumer) = self.consumer.as_mut() else {
            return Err(Report::new(PulsarSourceError::ConsumerClosed)
                .change_context(SourceError::Acknowledge { connector: PULSAR }));
        };
        let mut by_topic: BTreeMap<&str, Vec<MessageIdData>> = BTreeMap::new();
        for position in positions {
            by_topic
                .entry(position.topic.as_str())
                .or_default()
                .push(position.message_id.clone());
        }
        for (topic, message_ids) in by_topic {
            nervix_primitives::task::consume_budget().await;
            consumer
                .ack_with_ids(topic, message_ids)
                .await
                .map_err(PulsarSourceError::unsettled_acknowledgement)
                .change_context(SourceError::Acknowledge { connector: PULSAR })?;
        }
        Ok(())
    }

    /// A rejected message is negatively acknowledged, which asks the broker to redeliver it on the
    /// shared subscription. Rejecting returns once the consumer sent the request; the broker does
    /// not answer it.
    async fn reject(&mut self, positions: &[Self::Position]) -> SourceResult<()> {
        let Some(consumer) = self.consumer.as_mut() else {
            return Err(Report::new(PulsarSourceError::ConsumerClosed)
                .change_context(SourceError::Reject { connector: PULSAR }));
        };
        for position in positions {
            nervix_primitives::task::consume_budget().await;
            consumer
                .nack_with_id(&position.topic, position.message_id.clone())
                .await
                .map_err(PulsarSourceError::unsent_redelivery_request)
                .change_context(SourceError::Reject { connector: PULSAR })?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "source_tests.rs"]
mod settlement_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
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
