//! Pulsar ingestor transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Topic qualification, the consumer of the declared subscription type each instance
//!   reads through and the name it registers under, the check that a provisioned subscription
//!   exists, the removal of a subscription its ingestor owned, checked message metadata and ordered
//!   properties, acknowledgements the broker answers, redelivery requests, and the classification
//!   of a join, an acknowledgement or a redelivery request that did not take effect.
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
    BrokerIngestorConnector, IngestMessageHeaders, IngestMessageMetadata, IngestMetadataRow,
    IngestMetadataValues, IngestRowHeaders, IngestorBatch, IngestorBatchRequest, IngestorConnector,
    IngestorError, IngestorMessage, IngestorReadCancellation, IngestorResult, IngestorResume,
    client_config_value,
};
use nervix_models::{
    ClientConfigEntry, ConnectorKind, PulsarSubscription, PulsarSubscriptionName,
    PulsarSubscriptionOwnership, PulsarSubscriptionType, PulsarTopicName, Timestamp,
};
use nervix_primitives::time::{Instant, sleep_until, timeout};
use nervix_recovery::Reported as _;
use pulsar::{
    ConnectionRetryOptions as PulsarConnectionRetryOptions, Consumer as PulsarConsumer,
    ConsumerBuilder as PulsarConsumerBuilder, ConsumerOptions as PulsarConsumerOptions,
    Error as PulsarError, Pulsar, SubType as PulsarSubType, TokioExecutor,
    consumer::{InitialPosition as PulsarInitialPosition, Message as PulsarMessage},
    error::{ConnectionError as PulsarConnectionError, ConsumerError as PulsarConsumerError},
    message::proto::ServerError as PulsarServerError,
    proto::MessageIdData,
};
use thiserror::Error;
use url::Url;

use super::connection::{PulsarClientConnection, PulsarRefusal};

const PULSAR: &str = "pulsar";

#[cfg(test)]
#[path = "ingestor_metadata_tests.rs"]
mod metadata_tests;

/// The reason a broker gives when it refuses a non-durable subscribe because a durable
/// subscription of the same name exists. The broker refuses other subscribes with the same
/// `NotAllowedError`, such as one past a topic's subscription limit, so only this reason says that
/// the subscription exists.
const DURABLE_SUBSCRIPTION_EXISTS: &str = "Durable subscription with the same name already exists";

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
    /// The ingestor's client did not connect; [`super::PulsarClientError`] beneath says why.
    #[error("failed to connect {connector} client", connector = ConnectorKind::Pulsar)]
    Connect,
    #[error("failed to subscribe to Pulsar topic '{topic}'")]
    Subscribe { topic: String },
    /// The broker refused a consumer of the subscription, such as one another consumer holds.
    #[error(
        "the {connector} broker refused a consumer of subscription '{subscription}' of topic \
         '{topic}' with {refusal}",
        connector = ConnectorKind::Pulsar
    )]
    SubscriptionRefused {
        subscription: PulsarSubscriptionName,
        topic: String,
        refusal: PulsarRefusal,
    },
    #[error("failed to receive a Pulsar message")]
    Receive,
    #[error("{connector} message {position} on '{topic}' has an unsupported publish_time", connector = ConnectorKind::Pulsar, topic = .position.topic)]
    PublishTime {
        position: PulsarTopicIngestorPosition,
    },
    #[error("{connector} message {position} on '{topic}' has an unsupported event_time", connector = ConnectorKind::Pulsar, topic = .position.topic)]
    EventTime {
        position: PulsarTopicIngestorPosition,
    },
    #[error("{connector} message {position} on '{topic}' has a sequence_id outside I64", connector = ConnectorKind::Pulsar, topic = .position.topic)]
    SequenceId {
        position: PulsarTopicIngestorPosition,
    },
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

    /// Whether this failure is a refusal a later join of the same subscription can pass.
    fn holds_only_for_now(&self) -> bool {
        match self {
            Self::SubscriptionRefused { refusal, .. } => refusal.holds_only_for_now(),
            _ => false,
        }
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
/// subscription it joins with who owns it and its type, the name its consumers register under, and
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
    /// Whether both inputs read the same broker subscription. Consumer policy, declared ownership
    /// and declared type do not change that identity. Configuration is resolved by the host first;
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

/// One Pulsar ingestor's client and the subscription every instance consumes from.
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
        tls_policy: &nervix_connector::OutwardTlsPolicy,
    ) -> PulsarTopicIngestorResult<Self> {
        let PulsarTopicIngestorSettings {
            config,
            topic,
            subscription,
            consumer_name,
            ack_receipts,
        } = settings;
        let client = PulsarClientConnection::new(config, None, tls_policy)
            .open()
            .await
            .change_context(PulsarTopicIngestorError::Connect)?;
        Ok(Self {
            client,
            topic: super::qualified_topic(config, topic),
            subscription,
            consumer_name,
            ack_receipts,
        })
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
        if matches!(
            self.subscription.ownership,
            PulsarSubscriptionOwnership::Provisioned
        ) {
            self.verify_provisioned(&consumer_name).await?;
        }
        let options = PulsarConsumerOptions::default()
            .with_initial_position(self.initial_position())
            .with_ack_receipt_enabled(self.ack_receipts.enabled());
        self.consumer_builder(consumer_name, options)
            .build()
            .await
            .map_err(|error| self.join_failure(error))
    }

    /// What a failed join means: the refusal the broker answered with, or a failure this ingestor
    /// does not classify, which keeps the client's error beneath it.
    fn join_failure(&self, error: PulsarError) -> Report<PulsarTopicIngestorError> {
        if let Some(refusal) = PulsarRefusal::of_join(&error) {
            return Report::new(PulsarTopicIngestorError::SubscriptionRefused {
                subscription: self.subscription.name.clone(),
                topic: self.topic.clone(),
                refusal,
            });
        }
        Report::new(error).change_context(PulsarTopicIngestorError::Subscribe {
            topic: self.topic.clone(),
        })
    }

    /// The consumer an instance of an owned subscription opens with, absent when the broker refused
    /// the join for what holds right now. The instance joins again in its first resume, where the
    /// refusal is the ingestor's transient status rather than a failure that keeps it from starting.
    async fn consumer_at_open(
        &self,
        instance_index: u64,
    ) -> PulsarTopicIngestorResult<Option<PulsarConsumer<Vec<u8>, TokioExecutor>>> {
        match self.consumer(instance_index).await {
            Ok(consumer) => Ok(Some(consumer)),
            Err(failure) if failure.current_context().holds_only_for_now() => Ok(None),
            Err(failure) => Err(failure),
        }
    }

    /// The creation position shared by data consumers and the durable removal consumer.
    fn initial_position(&self) -> PulsarInitialPosition {
        match self.subscription.ownership {
            PulsarSubscriptionOwnership::Ingestor { start_at } => match start_at {
                nervix_models::IngestStartPosition::Earliest => PulsarInitialPosition::Earliest,
                nervix_models::IngestStartPosition::Latest => PulsarInitialPosition::Latest,
            },
            // The existing cursor controls a provisioned join. If it disappeared after the
            // existence check, retain the documented earliest position of that broker race.
            PulsarSubscriptionOwnership::Provisioned => PulsarInitialPosition::Earliest,
        }
    }

    /// A consumer of this ingestor's topic on its subscription, as the subscription's declared
    /// type, registered under `consumer_name`.
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
            .with_subscription_type(self.subscription_type())
            .with_options(options)
    }

    /// The driver's spelling of the subscription's declared type.
    fn subscription_type(&self) -> PulsarSubType {
        match self.subscription.kind {
            PulsarSubscriptionType::Shared => PulsarSubType::Shared,
            PulsarSubscriptionType::KeyShared => PulsarSubType::KeyShared,
            PulsarSubscriptionType::Failover => PulsarSubType::Failover,
            PulsarSubscriptionType::Exclusive => PulsarSubType::Exclusive,
        }
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
        let Some(PulsarRefusal {
            error: Some(PulsarServerError::NotAllowedError),
            reason: Some(reason),
        }) = PulsarRefusal::of_join(error)
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
        tls_policy: &nervix_connector::OutwardTlsPolicy,
    ) -> PulsarTopicIngestorResult<()> {
        let budget = PulsarConnectionRetryOptions::default().connection_timeout;
        Self::remove_subscription_within(settings, budget, tls_policy).await
    }

    async fn remove_subscription_within(
        settings: PulsarTopicIngestorSettings<'_>,
        budget: Duration,
        tls_policy: &nervix_connector::OutwardTlsPolicy,
    ) -> PulsarTopicIngestorResult<()> {
        let removal = PulsarTopicIngestorError::RemoveSubscription {
            subscription: settings.subscription.name.clone(),
            topic: super::qualified_topic(settings.config, settings.topic),
        };
        let removed = timeout(budget, async {
            let plan = Self::connect(settings, tls_policy).await?;
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
        let options =
            PulsarConsumerOptions::default().with_initial_position(self.initial_position());
        let mut consumer = self
            .consumer_builder(consumer_name, options)
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

impl fmt::Display for PulsarTopicIngestorPosition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id = &self.message_id;
        write!(
            formatter,
            "{}:{}:{}",
            id.ledger_id,
            id.entry_id,
            id.partition.unwrap_or(-1)
        )?;
        if let Some(batch_index) = id.batch_index {
            write!(formatter, ":{batch_index}")?;
        }
        Ok(())
    }
}

/// The ordered properties a received message lends as ingest headers.
pub struct PulsarMessageProperties {
    properties: Vec<pulsar::proto::KeyValue>,
}

impl IngestMessageHeaders for PulsarMessageProperties {
    fn visit(&self, visit: &mut dyn FnMut(&str, &str)) {
        for property in &self.properties {
            visit(&property.key, &property.value);
        }
    }
}

/// Metadata validated once when a driver delivery crosses into the connector.
struct PulsarReceivedMetadata {
    publish_time: Timestamp,
    event_time: Option<Timestamp>,
    key: Option<String>,
    ordering_key: Option<Vec<u8>>,
    producer_name: String,
    sequence_id: i64,
    redelivery_count: i64,
}

/// A validated message, retaining its exact broker position for settlement.
pub struct PulsarTopicIngestorMessage {
    payload: Vec<u8>,
    properties: PulsarMessageProperties,
    position: PulsarTopicIngestorPosition,
    metadata: PulsarReceivedMetadata,
}

impl TryFrom<PulsarMessage<Vec<u8>>> for PulsarTopicIngestorMessage {
    type Error = Report<PulsarTopicIngestorError>;

    fn try_from(message: PulsarMessage<Vec<u8>>) -> Result<Self, Self::Error> {
        // CommandMessage defines an omitted redelivery_count as zero.
        let redelivery_count = i64::from(message.redelivery_count().unwrap_or(0));
        let position = PulsarTopicIngestorPosition {
            topic: message.topic,
            message_id: message.message_id.id,
        };
        let raw = message.payload.metadata;
        let publish_time = Timestamp::from_unix_nanos(0)
            .checked_add(Duration::from_millis(raw.publish_time))
            .change_context_lazy(|| PulsarTopicIngestorError::PublishTime {
                position: position.clone(),
            })?;
        let event_time = match raw.event_time {
            Some(millis) => {
                let instant = Timestamp::from_unix_nanos(0)
                    .checked_add(Duration::from_millis(millis))
                    .change_context_lazy(|| PulsarTopicIngestorError::EventTime {
                        position: position.clone(),
                    })?;
                Some(instant)
            }
            None => None,
        };
        let sequence_id = i64::try_from(raw.sequence_id).change_context_lazy(|| {
            PulsarTopicIngestorError::SequenceId {
                position: position.clone(),
            }
        })?;
        Ok(Self {
            payload: message.payload.data,
            properties: PulsarMessageProperties {
                properties: raw.properties,
            },
            position,
            metadata: PulsarReceivedMetadata {
                publish_time,
                event_time,
                key: raw.partition_key,
                ordering_key: raw.ordering_key,
                producer_name: raw.producer_name,
                sequence_id,
                redelivery_count,
            },
        })
    }
}

impl IngestMessageMetadata for PulsarTopicIngestorMessage {
    fn visit_metadata(&self, values: &mut dyn IngestMetadataValues) {
        values.string(&self.position.topic);
        values.rendered(&self.position);
        values.datetime(self.metadata.publish_time);
        match self.metadata.event_time {
            Some(instant) => values.datetime(instant),
            None => values.absent(),
        }
        match &self.metadata.key {
            Some(key) => values.string(key),
            None => values.absent(),
        }
        match &self.metadata.ordering_key {
            Some(key) => values.bytes(key),
            None => values.absent(),
        }
        values.string(&self.metadata.producer_name);
        values.i64(self.metadata.sequence_id);
        values.i64(self.metadata.redelivery_count);
    }
}

impl IngestorMessage for PulsarTopicIngestorMessage {
    type Position = PulsarTopicIngestorPosition;

    fn payload(&self) -> &[u8] {
        &self.payload
    }
    fn position(&self) -> &Self::Position {
        &self.position
    }
    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.properties
    }
    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow {
            fields: self,
            headers: IngestRowHeaders::Borrowed(&self.properties),
            position: None,
        }
    }
}

/// One Pulsar ingestor instance: its consumer on the subscription, absent while suspended.
pub struct PulsarTopicIngestor {
    plan: PulsarTopicIngestorPlan,
    instance_index: u64,
    consumer: Option<PulsarConsumer<Vec<u8>, TokioExecutor>>,
    pending_read_error: Option<Report<PulsarTopicIngestorError>>,
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
            PulsarSubscriptionOwnership::Ingestor { .. } => plan
                .consumer_at_open(instance_index)
                .await
                .change_context(IngestorError::Open { connector: PULSAR })?,
            PulsarSubscriptionOwnership::Provisioned => None,
        };
        Ok(Self {
            plan: plan.clone(),
            instance_index,
            consumer,
            pending_read_error: None,
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
        if self.pending_read_error.is_some() && cancellation.is_cancelled() {
            return Ok(IngestorBatch::Interrupted {
                gathered: Vec::new(),
            });
        }
        if let Some(error) = self.pending_read_error.take() {
            return Err(error.change_context(IngestorError::Read { connector: PULSAR }));
        }
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
                return Err(Report::new(error)
                    .change_context(PulsarTopicIngestorError::Receive)
                    .change_context(IngestorError::Read { connector: PULSAR }));
            }
            None => {
                self.consumer = None;
                return Err(Report::new(PulsarTopicIngestorError::ConsumerClosed)
                    .change_context(IngestorError::Read { connector: PULSAR }));
            }
        };
        let mut messages = Vec::with_capacity(request.max_messages.get());
        messages.push(
            PulsarTopicIngestorMessage::try_from(first)
                .change_context(IngestorError::Read { connector: PULSAR })?,
        );
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
                        Some(Ok(message)) => match PulsarTopicIngestorMessage::try_from(message) {
                            Ok(message) => messages.push(message),
                            Err(error) => {
                                self.pending_read_error = Some(error);
                                break;
                            }
                        },
                        Some(Err(error)) => {
                            let failure = Report::new(error)
                                .change_context(PulsarTopicIngestorError::Receive);
                            self.pending_read_error = Some(failure);
                            break;
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

    /// A rejected message is negatively acknowledged, which asks the broker to redeliver it. A
    /// `FAILOVER` or `EXCLUSIVE` subscription takes no message of its own back: the broker rewinds
    /// the consumer to what it has not acknowledged, so what follows the message comes again too.
    /// Rejecting returns once the consumer sent the request; the broker does not answer it.
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
    use pulsar::proto::KeyValue;

    use super::*;

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
        let headers = PulsarMessageProperties { properties };
        let mut visited = Vec::new();
        headers.visit(&mut |name, value| visited.push(format!("{name}={value}")));
        assert_eq!(visited, ["trace=a", "Tenant=x", "trace=b", "tenant=y"]);
    }
}
