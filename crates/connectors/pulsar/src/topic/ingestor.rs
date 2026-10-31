//! Pulsar ingestor transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The Pulsar client an ingestor configuration declares, its TLS options, topic
//!   qualification, the shared-subscription consumer each instance reads through and the name it
//!   registers under, the check that a provisioned subscription exists, the removal of a
//!   subscription its ingestor owned, message properties as ingest headers, acknowledgements the
//!   broker answers, redelivery requests, and the classification of an acknowledgement or a
//!   redelivery request that did not take effect.
//! - **Depends on.** The connector contract, typed client configuration entries, `error-stack`,
//!   the recovery vocabulary, Tokio and `pulsar`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, why an
//!   ingestor's subscription is removed, or Plexus.

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

use std::{collections::BTreeMap, fmt, io, num::NonZeroUsize, time::Duration};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::StreamExt as _;
use nervix_connector::{
    BrokerIngestorConnector, IngestMessageHeaders, IngestMetadataRow, IngestorBatch,
    IngestorBatchRequest, IngestorConnector, IngestorError, IngestorMessage,
    IngestorReadCancellation, IngestorResult, IngestorResume, client_config_value,
    client_tls_paths, optional_bool_client_config_value, read_tls_file,
};
use nervix_models::{
    ClientConfigEntry, ConnectorKind, PulsarSubscription, PulsarSubscriptionName,
    PulsarSubscriptionOwnership, PulsarTopicName,
};
use nervix_primitives::time::{Instant, sleep_until, timeout};
use nervix_recovery::Reported as _;
use pulsar::{
    ConnectionRetryOptions as PulsarConnectionRetryOptions, Consumer as PulsarConsumer,
    ConsumerBuilder as PulsarConsumerBuilder, ConsumerOptions as PulsarConsumerOptions,
    Error as PulsarError, Pulsar, SubType as PulsarSubType, TlsOptions as PulsarTlsOptions,
    TokioExecutor,
    consumer::{InitialPosition as PulsarInitialPosition, Message as PulsarMessage},
    error::{ConnectionError as PulsarConnectionError, ConsumerError as PulsarConsumerError},
    message::proto::ServerError as PulsarServerError,
    proto::MessageIdData,
};
use thiserror::Error;
use tracing::warn;
use url::Url;

const PULSAR: &str = "pulsar";

/// The reason a broker gives when it refuses a non-durable subscribe because a durable
/// subscription of the same name exists. The broker refuses other subscribes with the same
/// `NotAllowedError`, such as one past a topic's subscription limit, so only this reason says that
/// the subscription exists.
const DURABLE_SUBSCRIPTION_EXISTS: &str = "Durable subscription with the same name already exists";

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

/// Why a Pulsar ingestor could not connect, consume, or settle a message.
#[derive(Debug, Error)]
pub enum PulsarTopicIngestorError {
    #[error("invalid Pulsar client configuration")]
    ClientConfig,
    #[error(
        "{connector} service address must use pulsar or pulsar+ssl and name a host",
        connector = ConnectorKind::Pulsar
    )]
    InvalidServiceAddress,
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
    /// An acknowledgement failed in a way this ingestor does not classify; the client's error is the
    /// cause beneath.
    #[error("failed to acknowledge a Pulsar message")]
    Acknowledge,
    /// A redelivery request failed in a way this ingestor does not classify; the client's error is
    /// the cause beneath.
    #[error("failed to negatively acknowledge a Pulsar message")]
    NegativelyAcknowledge,
    #[error("failed to close the Pulsar consumer")]
    Close,
    #[error("Pulsar batch timeout exceeds the monotonic clock range")]
    BatchDeadline,
    /// The subscription an operator provisioned does not exist. A provisioned subscription is
    /// never created, so the ingestor reports it until the operator provisions it.
    #[error(
        "{connector} subscription '{subscription}' of topic '{topic}' does not exist",
        connector = ConnectorKind::Pulsar
    )]
    MissingSubscription {
        subscription: PulsarSubscriptionName,
        topic: String,
    },
    /// Whether a provisioned subscription exists could not be learned; the client's error is the
    /// cause beneath.
    #[error(
        "failed to verify {connector} subscription '{subscription}' of topic '{topic}'",
        connector = ConnectorKind::Pulsar
    )]
    VerifySubscription {
        subscription: PulsarSubscriptionName,
        topic: String,
    },
    /// Removing a subscription its ingestor owned failed; the cause beneath says why.
    #[error(
        "failed to remove {connector} subscription '{subscription}' of topic '{topic}'",
        connector = ConnectorKind::Pulsar
    )]
    RemoveSubscription {
        subscription: PulsarSubscriptionName,
        topic: String,
    },
    /// The consumer that removes the subscription could not join it; the client's error is the
    /// cause beneath.
    #[error("failed to join the {connector} subscription to remove it", connector = ConnectorKind::Pulsar)]
    JoinSubscription,
    /// The broker did not remove the subscription, such as while another consumer is connected
    /// to it; the client's error is the cause beneath.
    #[error("the {connector} broker did not unsubscribe the subscription", connector = ConnectorKind::Pulsar)]
    Unsubscribe,
    /// The removal did not finish within its budget, such as while the broker cannot be reached.
    #[error(
        "{connector} subscription removal exceeded its connection budget of {budget:?}",
        connector = ConnectorKind::Pulsar
    )]
    RemovalDeadline { budget: Duration },
}

impl PulsarTopicIngestorError {
    /// What a failed acknowledgement means: the refusal the broker answered with, an answer the
    /// client could not get, or a failure this ingestor does not classify, which keeps the client's
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
    /// or a failure this ingestor does not classify, which keeps the client's error beneath it.
    fn unsent_redelivery_request(error: PulsarConsumerError) -> Report<Self> {
        if let Some(unresolved) = PulsarUnresolvedRequest::of(&error) {
            return Report::new(Self::RedeliveryRequestUnsent { unresolved });
        }
        Report::new(error).change_context(Self::NegativelyAcknowledge)
    }
}

type PulsarTopicIngestorResult<T> = Result<T, Report<PulsarTopicIngestorError>>;

/// Whether acknowledging a Pulsar ingestor's positions waits for the broker's answer, which the
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

/// What one Pulsar ingestor subscribes with: its resolved client entries, the topic it reads, the
/// shared subscription it joins and who owns it, the name its consumers register under, and
/// whether acknowledging waits for the broker.
pub struct PulsarTopicIngestorSettings<'a> {
    pub config: &'a [ClientConfigEntry],
    pub topic: &'a PulsarTopicName,
    pub subscription: PulsarSubscription,
    /// Each instance's consumer registers under this name suffixed with `-` and its instance
    /// index.
    pub consumer_name: String,
    pub ack_receipts: PulsarAckReceipts,
}

impl PulsarTopicIngestorSettings<'_> {
    /// Whether both inputs read the same broker subscription. Consumer policy and declared
    /// ownership do not change that identity. Configuration is resolved by the host first;
    /// comparison opens no connection and performs no DNS lookup.
    pub fn reads_same_subscription(&self, replacement: &Self) -> PulsarTopicIngestorResult<bool> {
        let previous = self.service_url()?;
        let next = replacement.service_url()?;
        let same_host = previous.host_str().map(str::to_ascii_lowercase)
            == next.host_str().map(str::to_ascii_lowercase);
        let same_broker =
            previous.scheme() == next.scheme() && same_host && previous.port() == next.port();
        let previous_topic = super::qualified_topic(self.config, self.topic);
        let next_topic = super::qualified_topic(replacement.config, replacement.topic);
        Ok(same_broker
            && previous_topic == next_topic
            && self.subscription.name == replacement.subscription.name)
    }

    fn service_url(&self) -> PulsarTopicIngestorResult<Url> {
        let addr = client_config_value(self.config, "addr", ConnectorKind::Pulsar.into())
            .change_context(PulsarTopicIngestorError::ClientConfig)?;
        let mut url = Url::parse(&addr).change_context(PulsarTopicIngestorError::ClientConfig)?;
        let default_port = match url.scheme() {
            "pulsar" => 6650,
            "pulsar+ssl" => 6651,
            _ => return Err(Report::new(PulsarTopicIngestorError::InvalidServiceAddress)),
        };
        if !url.has_host() {
            return Err(Report::new(PulsarTopicIngestorError::InvalidServiceAddress));
        }
        if url.port().is_none() {
            url.set_port(Some(default_port))
                .map_err(|()| Report::new(PulsarTopicIngestorError::InvalidServiceAddress))?;
        }
        Ok(url)
    }
}

/// One Pulsar ingestor's client and the shared subscription every instance consumes from.
///
/// The client is connected once and shared by the instances, which each open their own consumer.
#[derive(Clone)]
pub struct PulsarTopicIngestorPlan {
    client: Pulsar<TokioExecutor>,
    topic: String,
    subscription: PulsarSubscription,
    consumer_name: String,
    ack_receipts: PulsarAckReceipts,
}

impl PulsarTopicIngestorPlan {
    pub async fn connect(
        settings: PulsarTopicIngestorSettings<'_>,
    ) -> PulsarTopicIngestorResult<Self> {
        let PulsarTopicIngestorSettings {
            config,
            topic,
            subscription,
            consumer_name,
            ack_receipts,
        } = settings;
        let client = Self::client_from_config(config).await?;
        Ok(Self {
            client,
            topic: super::qualified_topic(config, topic),
            subscription,
            consumer_name,
            ack_receipts,
        })
    }

    async fn client_from_config(
        config: &[ClientConfigEntry],
    ) -> PulsarTopicIngestorResult<Pulsar<TokioExecutor>> {
        let addr = client_config_value(config, "addr", "Pulsar")
            .change_context(PulsarTopicIngestorError::ClientConfig)?;
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
            Report::new(PulsarTopicIngestorError::Connect).attach_printable(source.to_string())
        })
    }

    fn tls_options_from_config(
        config: &[ClientConfigEntry],
    ) -> PulsarTopicIngestorResult<Option<PulsarTlsOptions>> {
        let tls = client_tls_paths(config);
        if tls.cert_file.is_some() || tls.key_file.is_some() {
            return Err(Report::new(
                PulsarTopicIngestorError::UnsupportedTlsIdentity,
            ));
        }

        let allow_insecure_connection =
            optional_bool_client_config_value(config, "tls_allow_insecure_connection")
                .change_context(PulsarTopicIngestorError::ClientConfig)?;
        let tls_hostname_verification_enabled =
            optional_bool_client_config_value(config, "tls_hostname_verification_enabled")
                .change_context(PulsarTopicIngestorError::ClientConfig)?;

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
                    .change_context(PulsarTopicIngestorError::ClientConfig)?,
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

    /// The consumer instance `instance_index` reads through. An instance of a provisioned
    /// subscription first checks that the subscription exists, because joining an absent one
    /// would create it. The subscription can still disappear between that check and the join, in
    /// which case the join creates it as an owned subscription's join does.
    async fn consumer(
        &self,
        instance_index: u64,
    ) -> PulsarTopicIngestorResult<PulsarConsumer<Vec<u8>, TokioExecutor>> {
        let consumer_name = format!("{}-{instance_index}", self.consumer_name);
        match self.subscription.ownership {
            PulsarSubscriptionOwnership::Ingestor => {}
            PulsarSubscriptionOwnership::Provisioned => {
                self.verify_provisioned(&consumer_name).await?;
            }
        }
        let options = PulsarConsumerOptions::default()
            .with_initial_position(PulsarInitialPosition::Earliest)
            .with_ack_receipt_enabled(self.ack_receipts.enabled());
        self.consumer_builder(consumer_name, options)
            .build()
            .await
            .map_err(|source| {
                Report::new(PulsarTopicIngestorError::Subscribe {
                    topic: self.topic.clone(),
                })
                .attach_printable(source.to_string())
            })
    }

    /// A consumer of this ingestor's topic on its shared subscription, registered under
    /// `consumer_name`.
    fn consumer_builder(
        &self,
        consumer_name: String,
        options: PulsarConsumerOptions,
    ) -> PulsarConsumerBuilder<TokioExecutor> {
        self.client
            .consumer()
            .with_topic(self.topic.as_str())
            .with_consumer_name(consumer_name)
            .with_subscription(self.subscription.name.as_str())
            .with_subscription_type(PulsarSubType::Shared)
            .with_options(options)
    }

    /// Checks that the subscription an operator provisioned exists before an instance joins it.
    ///
    /// The broker refuses a non-durable subscribe while a durable subscription of the same name
    /// exists. Without one it opens a non-durable cursor of that name instead, which closing the
    /// probe removes again, so the check never creates the subscription it asks about.
    async fn verify_provisioned(&self, consumer_name: &str) -> PulsarTopicIngestorResult<()> {
        let options = PulsarConsumerOptions::default().durable(false);
        let probe = self
            .consumer_builder(consumer_name.to_string(), options)
            .build::<Vec<u8>>()
            .await;
        let mut probe = match probe {
            Ok(probe) => probe,
            Err(error) => {
                if Self::refused_for_a_durable_subscription(&error) {
                    return Ok(());
                }
                return Err(Report::new(error).change_context(
                    PulsarTopicIngestorError::VerifySubscription {
                        subscription: self.subscription.name.clone(),
                        topic: self.topic.clone(),
                    },
                ));
            }
        };
        // The broker also removes a non-durable cursor once the connection that opened it closes,
        // so a probe whose close fails is gone with its connection at the latest.
        probe
            .close()
            .await
            .reported("closing the probe of a missing provisioned Pulsar subscription");
        Err(Report::new(PulsarTopicIngestorError::MissingSubscription {
            subscription: self.subscription.name.clone(),
            topic: self.topic.clone(),
        }))
    }

    /// Whether the broker refused a non-durable subscribe because a durable subscription of the
    /// same name exists.
    fn refused_for_a_durable_subscription(error: &PulsarError) -> bool {
        let (PulsarError::Connection(refusal)
        | PulsarError::Consumer(PulsarConsumerError::Connection(refusal))) = error
        else {
            return false;
        };
        let PulsarConnectionError::PulsarError(
            Some(PulsarServerError::NotAllowedError),
            Some(reason),
        ) = refusal
        else {
            return false;
        };
        reason.starts_with(DURABLE_SUBSCRIPTION_EXISTS)
    }

    /// Removes the subscription an ingestor owned, once every consumer of that ingestor
    /// has closed.
    ///
    /// A durable consumer joins the subscription and unsubscribes it, and the broker removes the
    /// subscription with its backlog. The broker refuses while another consumer is still connected
    /// to the subscription, and an absent subscription is created by the join and removed again.
    /// The driver's connection timeout bounds the whole removal: connecting the client, joining
    /// and unsubscribing.
    pub async fn remove_subscription(
        settings: PulsarTopicIngestorSettings<'_>,
    ) -> PulsarTopicIngestorResult<()> {
        let budget = PulsarConnectionRetryOptions::default().connection_timeout;
        Self::remove_subscription_within(settings, budget).await
    }

    async fn remove_subscription_within(
        settings: PulsarTopicIngestorSettings<'_>,
        budget: Duration,
    ) -> PulsarTopicIngestorResult<()> {
        let removal = PulsarTopicIngestorError::RemoveSubscription {
            subscription: settings.subscription.name.clone(),
            topic: super::qualified_topic(settings.config, settings.topic),
        };
        let removed = timeout(budget, async {
            let plan = Self::connect(settings).await?;
            plan.unsubscribe().await
        })
        .await;
        let Ok(removed) = removed else {
            return Err(
                Report::new(PulsarTopicIngestorError::RemovalDeadline { budget })
                    .change_context(removal),
            );
        };
        removed.change_context(removal)
    }

    /// Joins the subscription with a durable consumer and unsubscribes it.
    async fn unsubscribe(&self) -> PulsarTopicIngestorResult<()> {
        let consumer_name = format!("{}-removal", self.consumer_name);
        let mut consumer = self
            .consumer_builder(consumer_name, PulsarConsumerOptions::default())
            .build::<Vec<u8>>()
            .await
            .map_err(|error| {
                Report::new(error).change_context(PulsarTopicIngestorError::JoinSubscription)
            })?;
        let unsubscribed = consumer.unsubscribe().await;
        let Err(error) = unsubscribed else {
            return Ok(());
        };
        // A refused unsubscribe leaves this consumer joined, which would keep the subscription it
        // was asked to remove busy while its connection stays open.
        consumer
            .close()
            .await
            .reported("closing the consumer of a refused Pulsar unsubscribe");
        Err(Report::new(error).change_context(PulsarTopicIngestorError::Unsubscribe))
    }
}

/// The message one acknowledgement or negative acknowledgement settles: the topic it arrived on,
/// which a partitioned subscription routes by, and its Pulsar message id.
#[derive(Debug, Clone)]
pub struct PulsarTopicIngestorPosition {
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

pub struct PulsarTopicIngestorMessage {
    properties: PulsarMessageProperties,
    position: PulsarTopicIngestorPosition,
}

impl PulsarTopicIngestorMessage {
    fn new(message: PulsarMessage<Vec<u8>>) -> Self {
        let position = PulsarTopicIngestorPosition {
            topic: message.topic.clone(),
            message_id: message.message_id().clone(),
        };
        Self {
            properties: PulsarMessageProperties { message },
            position,
        }
    }
}

impl IngestorMessage for PulsarTopicIngestorMessage {
    type Position = PulsarTopicIngestorPosition;

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
        IngestMetadataRow::headers(&self.properties)
    }
}

/// One Pulsar ingestor instance: its consumer on the shared subscription, absent while suspended.
pub struct PulsarTopicIngestor {
    plan: PulsarTopicIngestorPlan,
    instance_index: u64,
    consumer: Option<PulsarConsumer<Vec<u8>, TokioExecutor>>,
}

impl PulsarTopicIngestor {
    async fn close_consumer(&mut self) -> PulsarTopicIngestorResult<()> {
        let Some(mut consumer) = self.consumer.take() else {
            return Ok(());
        };
        consumer.close().await.map_err(|source| {
            Report::new(PulsarTopicIngestorError::Close).attach_printable(source.to_string())
        })
    }
}

#[async_trait]
impl IngestorConnector for PulsarTopicIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Pulsar(nervix_models::PulsarIngestorEntity::Topic);

    type Plan = PulsarTopicIngestorPlan;

    /// An instance of an owned subscription joins it here. An instance of a provisioned one joins
    /// in its first resume instead, where a subscription the operator has not provisioned yet is
    /// the ingestor's transient status rather than a failure that keeps the ingestor from starting.
    async fn open(plan: &Self::Plan, instance_index: u64) -> IngestorResult<Self> {
        let consumer = match plan.subscription.ownership {
            PulsarSubscriptionOwnership::Ingestor => {
                let consumer = plan
                    .consumer(instance_index)
                    .await
                    .change_context(IngestorError::Open { connector: PULSAR })?;
                Some(consumer)
            }
            PulsarSubscriptionOwnership::Provisioned => None,
        };
        Ok(Self {
            plan: plan.clone(),
            instance_index,
            consumer,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.consumer.is_none()
    }

    /// Suspension disconnects the consumer, and the durable subscription redelivers what it held
    /// unacknowledged.
    async fn suspend(&mut self) -> IngestorResult<()> {
        self.close_consumer()
            .await
            .change_context(IngestorError::Suspend { connector: PULSAR })
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        if self.consumer.is_none() {
            let consumer = self
                .plan
                .consumer(self.instance_index)
                .await
                .change_context(IngestorError::Resume { connector: PULSAR })?;
            self.consumer = Some(consumer);
        }
        Ok(IngestorResume::Ready)
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.close_consumer()
            .await
            .change_context(IngestorError::Close { connector: PULSAR })
    }
}

#[async_trait]
impl BrokerIngestorConnector for PulsarTopicIngestor {
    type Message = PulsarTopicIngestorMessage;
    type Position = PulsarTopicIngestorPosition;

    async fn next_batch(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        let Some(consumer) = self.consumer.as_mut() else {
            return Ok(IngestorBatch::ResumeRequired);
        };
        let Some(first) = cancellation.unless_cancelled(consumer.next()).await else {
            return Ok(IngestorBatch::Interrupted {
                gathered: Vec::new(),
            });
        };
        let first = match first {
            Some(Ok(message)) => message,
            Some(Err(error)) => {
                return Err(Report::new(PulsarTopicIngestorError::Receive)
                    .attach_printable(error.to_string())
                    .change_context(IngestorError::Read { connector: PULSAR }));
            }
            None => {
                self.consumer = None;
                return Err(Report::new(PulsarTopicIngestorError::ConsumerClosed)
                    .change_context(IngestorError::Read { connector: PULSAR }));
            }
        };
        let mut messages = Vec::with_capacity(request.max_messages.get());
        messages.push(PulsarTopicIngestorMessage::new(first));
        if request.max_messages == NonZeroUsize::MIN {
            return Ok(IngestorBatch::Messages(messages));
        }
        let Some(batch_timeout) = request.batch_timeout else {
            return Ok(IngestorBatch::Messages(messages));
        };
        let Some(deadline) = Instant::now().checked_add(batch_timeout) else {
            return Err(Report::new(PulsarTopicIngestorError::BatchDeadline)
                .change_context(IngestorError::Read { connector: PULSAR }));
        };
        while messages.len() < request.max_messages.get() {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                _ = sleep_until(deadline) => break,
                next = consumer.next() => {
                    match next {
                        Some(Ok(message)) => messages.push(PulsarTopicIngestorMessage::new(message)),
                        Some(Err(error)) => {
                            warn!(error = %error, "failed to receive another Pulsar batch message");
                        }
                        None => break,
                    }
                }
                () = cancellation.cancelled() => {
                    return Ok(IngestorBatch::Interrupted { gathered: messages });
                }
            }
        }
        Ok(IngestorBatch::Messages(messages))
    }

    /// The positions of each topic are acknowledged in one command. Under an `ACK` mode
    /// acknowledging returns once the broker answered every one of them, within the client's
    /// operation timeout; under `MODE NO ACK`, once the client queued them.
    async fn acknowledge(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let Some(consumer) = self.consumer.as_mut() else {
            return Err(Report::new(PulsarTopicIngestorError::ConsumerClosed)
                .change_context(IngestorError::Acknowledge { connector: PULSAR }));
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
                .map_err(PulsarTopicIngestorError::unsettled_acknowledgement)
                .change_context(IngestorError::Acknowledge { connector: PULSAR })?;
        }
        Ok(())
    }

    /// A rejected message is negatively acknowledged, which asks the broker to redeliver it on the
    /// shared subscription. Rejecting returns once the consumer sent the request; the broker does
    /// not answer it.
    async fn reject(&mut self, positions: &[Self::Position]) -> IngestorResult<()> {
        let Some(consumer) = self.consumer.as_mut() else {
            return Err(Report::new(PulsarTopicIngestorError::ConsumerClosed)
                .change_context(IngestorError::Reject { connector: PULSAR }));
        };
        for position in positions {
            nervix_primitives::task::consume_budget().await;
            consumer
                .nack_with_id(&position.topic, position.message_id.clone())
                .await
                .map_err(PulsarTopicIngestorError::unsent_redelivery_request)
                .change_context(IngestorError::Reject { connector: PULSAR })?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "ingestor_tests.rs"]
mod settlement_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "ingestor_subscription_tests.rs"]
mod subscription_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use pulsar::{
        consumer::data::MessageData,
        message::{
            Payload,
            proto::{KeyValue, MessageIdData, MessageMetadata},
        },
    };

    use super::*;

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn tls_options_load_trust_and_reject_client_identities() {
        assert!(
            PulsarTopicIngestorPlan::tls_options_from_config(&[])
                .expect("no TLS keys must be accepted")
                .is_none()
        );

        let error = PulsarTopicIngestorPlan::tls_options_from_config(&[entry(
            "tls_cert_file",
            "client.pem",
        )])
        .expect_err("a client certificate must be rejected");
        assert!(matches!(
            error.current_context(),
            PulsarTopicIngestorError::UnsupportedTlsIdentity
        ));

        let error = PulsarTopicIngestorPlan::tls_options_from_config(&[entry(
            "tls_allow_insecure_connection",
            "sometimes",
        )])
        .expect_err("an invalid boolean must be rejected");
        assert!(matches!(
            error.current_context(),
            PulsarTopicIngestorError::ClientConfig
        ));

        let root = tempfile::tempdir().expect("temporary Pulsar TLS directory should open");
        let ca = root.path().join("ca.pem");
        std::fs::write(&ca, b"test CA").expect("the Pulsar CA fixture should be writable");
        let options = PulsarTopicIngestorPlan::tls_options_from_config(&[
            entry("tls_ca_file", &ca.to_string_lossy()),
            entry("tls_hostname_verification_enabled", "false"),
        ])
        .expect("a CA file must be accepted")
        .expect("a CA file enables TLS options");
        assert_eq!(options.certificate_chain.as_deref(), Some(&b"test CA"[..]));
        assert!(!options.tls_hostname_verification_enabled);
        assert!(!options.allow_insecure_connection);
    }

    /// PULSAR TOPIC ingestors declare ordered repeats with exact names: a message's properties
    /// are visited in the order the message carries them, a repeated name once for each of its
    /// values, and names that differ in letter case stay two names.
    #[test]
    fn properties_are_visited_in_the_order_carried_with_every_repeat() {
        let mut properties = Vec::new();
        for (key, value) in [
            ("trace", "a"),
            ("Tenant", "x"),
            ("trace", "b"),
            ("tenant", "y"),
        ] {
            properties.push(KeyValue {
                key: key.to_string(),
                value: value.to_string(),
            });
        }
        let payload = Payload {
            metadata: MessageMetadata {
                properties,
                ..Default::default()
            },
            data: Vec::new(),
        };
        let message_id = MessageData {
            id: MessageIdData::default(),
            batch_size: None,
        };
        let headers = PulsarMessageProperties {
            message: PulsarMessage::new("persistent://public/default/events", message_id, payload),
        };
        let mut visited = Vec::new();
        headers.visit(&mut |name, value| visited.push(format!("{name}={value}")));
        assert_eq!(visited, ["trace=a", "Tenant=x", "trace=b", "tenant=y"]);
    }
}
