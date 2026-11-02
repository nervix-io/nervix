//! Pulsar ingestor and emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The one client both directions connect through, producer configuration, topic
//!   qualification, the shared-subscription consumers an ingestor reads through, message
//!   properties as ingest headers, per-message acknowledgement, record and property
//!   publication, the event time and delivery time a message carries from its record, the
//!   bounded receipts a NO ACK producer retains through finish, and the classification of a send
//!   or its receipt, including a message the client refuses because it exceeds the maximum
//!   message size the broker announced.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio and
//!   `pulsar`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

mod connection;
mod ingestor;
mod message_times;

#[cfg(test)]
mod message_times_tests;
#[cfg(test)]
mod receipt_tests;

use std::future::Future;

use arch_into::ArchInto as _;
use async_trait::async_trait;
use connection::PulsarClientConnection;
pub use connection::{PulsarClientError, PulsarRefusal};
use error_stack::Report;
use futures_util::{FutureExt, StreamExt as _, stream::FuturesUnordered};
pub use ingestor::{
    PulsarAckReceipts, PulsarMessageProperties, PulsarTopicIngestor, PulsarTopicIngestorError,
    PulsarTopicIngestorMessage, PulsarTopicIngestorPlan, PulsarTopicIngestorPosition,
    PulsarTopicIngestorSettings, PulsarUnresolvedRequest,
};
use message_times::{PulsarMessageTimeError, PulsarMessageTimes};
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, BrokerAnswer, BrokerPublishingMode, BrokerVerdict, ConfirmationWindow,
    ConfirmationWindowError, EmitterFinish, EmitterFinishError, EmitterFinishResult, EmitterHost,
    EmitterLifecycle, EmitterOpenError, EmitterRecord, EmitterRecordId, EmitterStartResult,
    EmitterWriteError, PerRecordOutcome, RecordEmitter, RejectedEmitterMember,
    optional_client_config_value,
};
use nervix_models::{ClientConfigEntry, ConnectorKind, PulsarTopicName, Timestamp};
use nervix_primitives::{
    task::unconstrained,
    time::{Instant, timeout_at},
};
use pulsar::{
    Error as PulsarError, TokioExecutor,
    error::{ConnectionError as PulsarConnectionError, ProducerError as PulsarProducerError},
    message::proto::{CommandSendReceipt, ServerError as PulsarServerError},
    producer::{Message as PulsarProducerMessage, SendFuture as PulsarSendFuture},
};
use thiserror::Error;

const PULSAR: ConnectorKind = ConnectorKind::Pulsar;
const PULSAR_OUTBOUND_CAPACITY: usize = 100;
const _: () = assert!(PULSAR_OUTBOUND_CAPACITY > 0);

/// The name the broker knows `topic` by: a fully qualified name as written, or a local name placed
/// in its client's namespace, whose default the Pulsar client declaration writes.
fn qualified_topic(config: &[ClientConfigEntry], topic: &PulsarTopicName) -> String {
    let name = topic.as_str();
    if name.contains("://") {
        return name.to_owned();
    }
    let schema = PULSAR
        .client()
        .assured("the Pulsar connector declares a configured client")
        .config;
    let effective = schema.effective_entries(config);
    let namespace = optional_client_config_value(&effective, "namespace")
        .assured("the Pulsar client declaration defaults its namespace");
    format!("persistent://{namespace}/{name}")
}

/// Why one message is refused for good: sending it again cannot succeed while the broker keeps its
/// configuration.
#[derive(Debug, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "a refused message is a semantic per-record rejection the emitter answers with"
    )
)]
enum PulsarRecordError {
    /// The client refused the message before writing it, because its metadata and payload
    /// together exceed the maximum message size the broker announced for the connection.
    #[error(
        "Pulsar message of {message_bytes} bytes, counting its metadata and properties, exceeds \
         the broker's maximum message size of {maximum} bytes"
    )]
    MessageTooLarge { message_bytes: usize, maximum: u32 },
    /// The broker received the message and refused it, such as for a topic's own maximum message
    /// size.
    #[error("the Pulsar broker does not allow the message: {reason}")]
    NotAllowed { reason: String },
    /// The record's envelope sets a time its message cannot carry.
    #[error(transparent)]
    MessageTime(#[from] PulsarMessageTimeError),
}

impl PulsarRecordError {
    fn message(&self) -> String {
        format!("{PULSAR} rejected record: {self}")
    }
}

/// What a failed send, or a failed receipt for a sent message, means for the record it carried.
#[derive(Debug)]
enum PulsarSendFailure {
    /// The message is refused for good, so the record follows the message error policy.
    Rejected(PulsarRecordError),
    /// The client, its connection or the broker failed, so the record's outcome is unknown and the
    /// attempt is retried.
    Infrastructure(PulsarError),
}

impl From<PulsarError> for PulsarSendFailure {
    fn from(error: PulsarError) -> Self {
        let PulsarError::Producer(PulsarProducerError::Connection(connection)) = &error else {
            return Self::Infrastructure(error);
        };
        match connection {
            PulsarConnectionError::MessageTooLarge {
                size,
                max_message_size,
            } => Self::Rejected(PulsarRecordError::MessageTooLarge {
                message_bytes: *size,
                maximum: *max_message_size,
            }),
            // The broker names the reason in every send error it answers a message with.
            PulsarConnectionError::PulsarError(
                Some(PulsarServerError::NotAllowedError),
                Some(reason),
            ) => Self::Rejected(PulsarRecordError::NotAllowed {
                reason: reason.clone(),
            }),
            _ => Self::Infrastructure(error),
        }
    }
}

/// What one Pulsar emitter publishes through: its client entries, topic and publishing mode.
pub struct PulsarTopicEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub topic: PulsarTopicName,
    pub mode: BrokerPublishingMode,
}

pub struct PulsarTopicEmitter {
    producer: pulsar::Producer<TokioExecutor>,
    publishing: PulsarPublishing,
}

enum PulsarPublishing {
    NoAck(UnconfirmedPulsarPublication),
    Ack(AckConfirmation),
}

impl From<BrokerPublishingMode> for PulsarPublishing {
    fn from(mode: BrokerPublishingMode) -> Self {
        match mode {
            BrokerPublishingMode::NoAck => Self::NoAck(UnconfirmedPulsarPublication::default()),
            BrokerPublishingMode::Ack(settings) => Self::Ack(settings),
        }
    }
}

/// The receipts of records delivered at producer acceptance. This state belongs to one emitter
/// task, and keeps no record payload or upstream acknowledgement.
struct UnconfirmedPulsarPublication<A = PulsarSendFuture> {
    receipts: FuturesUnordered<A>,
    failure: Option<PulsarError>,
}

impl<A> Default for UnconfirmedPulsarPublication<A> {
    fn default() -> Self {
        Self {
            receipts: FuturesUnordered::new(),
            failure: None,
        }
    }
}

impl UnconfirmedPulsarPublication {
    async fn publish(
        &mut self,
        producer: &mut pulsar::Producer<TokioExecutor>,
        records: Vec<EmitterRecord>,
    ) -> PerRecordOutcome<EmitterRecordId> {
        self.collect_ready();
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let message = match PulsarTopicEmitter::message(record) {
                Ok(message) => message,
                Err(rejection) => {
                    outcome.reject(PulsarTopicEmitter::rejected(
                        record_id,
                        occurred_at,
                        &rejection,
                    ));
                    continue;
                }
            };
            self.make_room().await;
            let sent = producer.send_non_blocking(message).await;
            match sent.map_err(PulsarSendFailure::from) {
                Ok(receipt) => {
                    self.retain(receipt);
                    outcome.deliver(record_id);
                }
                Err(PulsarSendFailure::Rejected(rejection)) => {
                    outcome.reject(PulsarTopicEmitter::rejected(
                        record_id,
                        occurred_at,
                        &rejection,
                    ));
                }
                Err(PulsarSendFailure::Infrastructure(error)) => {
                    outcome.fail(PulsarTopicEmitter::publish_error(error));
                    return outcome;
                }
            }
        }
        self.collect_ready();
        outcome
    }
}

impl<A> UnconfirmedPulsarPublication<A>
where
    A: Future<Output = Result<CommandSendReceipt, PulsarError>> + Unpin,
{
    fn retain(&mut self, receipt: A) {
        assert!(
            self.receipts.len() < PULSAR_OUTBOUND_CAPACITY,
            "make_room provides a slot before the emitter hands another record over"
        );
        self.receipts.push(receipt);
    }

    /// Receipt bookkeeping shares the driver's outbound channel capacity. A full set applies
    /// backpressure before another hand-over; records already accepted retain their NO ACK
    /// completion point. The driver's operation timeout bounds each outstanding receipt.
    async fn make_room(&mut self) {
        self.collect_ready();
        if self.receipts.len() == PULSAR_OUTBOUND_CAPACITY {
            let answer =
                self.receipts.next().await.verified(
                    "the receipt set holds the nonzero outbound capacity before this read",
                );
            self.record_answer(answer);
        }
    }

    fn record_answer(&mut self, answer: Result<CommandSendReceipt, PulsarError>) {
        if let Err(error) = answer
            && self.failure.is_none()
        {
            self.failure = Some(error);
        }
    }

    /// Visit each retained future once without waiting. A stream's own cooperative poll budget
    /// can return Pending before visiting every future, so counting at an expired deadline needs
    /// this complete pass of at most PULSAR_OUTBOUND_CAPACITY futures. Keeping it synchronous
    /// also leaves every pending receipt in its
    /// owner if the surrounding finish is cancelled at an await.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "polls the external Pulsar driver's receipt futures without \
                                   waiting; their implementations belong to the driver")
    )]
    fn collect_ready(&mut self) {
        let receipts = std::mem::take(&mut self.receipts);
        for mut receipt in receipts {
            match unconstrained(&mut receipt).now_or_never() {
                Some(answer) => self.record_answer(answer),
                None => self.receipts.push(receipt),
            }
        }
    }

    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult {
        let had_receipts = !self.receipts.is_empty();
        if Instant::now() >= deadline {
            self.collect_ready();
            return self.finish_outcome(had_receipts);
        }
        self.collect_ready();
        while !self.receipts.is_empty() {
            nervix_primitives::task::consume_budget().await;
            match timeout_at(deadline, self.receipts.next()).await {
                Ok(Some(answer)) => self.record_answer(answer),
                Ok(None) => break,
                Err(_) => {
                    self.collect_ready();
                    return self.finish_outcome(had_receipts);
                }
            }
        }
        self.finish_outcome(had_receipts)
    }

    fn finish_outcome(&mut self, had_receipts: bool) -> EmitterFinishResult {
        if !self.receipts.is_empty() {
            return Err(self.failed(EmitterFinishError::Unsent {
                connector: PULSAR.keyword(),
                records: self.receipts.len().arch_into(),
            }));
        }
        if self.failure.is_some() {
            return Err(self.failed(EmitterFinishError::Flush {
                connector: PULSAR.keyword(),
            }));
        }
        if had_receipts {
            Ok(EmitterFinish::Flushed)
        } else {
            Ok(EmitterFinish::NothingHeld)
        }
    }

    fn failed(&mut self, context: EmitterFinishError) -> Report<EmitterFinishError> {
        match self.failure.take() {
            Some(source) => {
                let message = source.to_string();
                Report::new(source)
                    .change_context(context)
                    .attach_printable(message)
            }
            None => Report::new(context),
        }
    }
}

impl PulsarTopicEmitter {
    pub async fn new(
        config: PulsarTopicEmitterConfig,
        _host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        if let BrokerPublishingMode::Ack(settings) = config.mode
            && Instant::now().checked_add(settings.timeout).is_none()
        {
            let error = ConfirmationWindowError::DeadlineOutOfRange {
                timeout: settings.timeout,
            };
            let message = error.to_string();
            return Err(Report::new(error)
                .change_context(EmitterOpenError::InvalidConfiguration {
                    connector: PULSAR.keyword(),
                })
                .attach_printable(message));
        }
        let producer =
            Self::producer_from_config(&config.config, &config.topic, config.mode).await?;
        Ok(Self {
            producer,
            publishing: config.mode.into(),
        })
    }

    async fn producer_from_config(
        config: &[ClientConfigEntry],
        topic: &PulsarTopicName,
        mode: BrokerPublishingMode,
    ) -> EmitterStartResult<pulsar::Producer<TokioExecutor>> {
        // A publish's acknowledgement timeout bounds what the driver waits for on its behalf.
        let operation_timeout = match mode {
            BrokerPublishingMode::NoAck => None,
            BrokerPublishingMode::Ack(settings) => Some(settings.timeout),
        };
        let pulsar = PulsarClientConnection::new(config, operation_timeout)
            .open()
            .await
            .map_err(Self::client_error)?;
        let topic_name = qualified_topic(config, topic);
        pulsar
            .producer()
            .with_topic(topic_name)
            .build()
            .await
            .map_err(Self::start_error)
    }

    /// Every way an ACK publish ends goes through the shared window, which settles answers
    /// already given before returning a driver, enqueue or timeout failure.
    async fn publish_confirmed(
        &mut self,
        records: Vec<EmitterRecord>,
        settings: AckConfirmation,
    ) -> PerRecordOutcome<EmitterRecordId> {
        let mut window = ConfirmationWindow::new(PULSAR.keyword(), settings, records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let message = match Self::message(record) {
                Ok(message) => message,
                Err(rejection) => {
                    window.reject(Self::rejected(record_id, occurred_at, &rejection));
                    continue;
                }
            };
            let sent = self.producer.send_non_blocking(message).await;
            let receipt = match sent.map_err(PulsarSendFailure::from) {
                Ok(receipt) => receipt,
                Err(PulsarSendFailure::Rejected(rejection)) => {
                    window.reject(Self::rejected(record_id, occurred_at, &rejection));
                    continue;
                }
                Err(PulsarSendFailure::Infrastructure(error)) => {
                    return window.fail(Self::publish_error(error));
                }
            };
            let answer = receipt.map(Self::broker_answer);
            if let Err(failure) = window.admit(record_id, occurred_at, answer).await {
                return window.fail(failure);
            }
        }
        window.finish().await
    }

    fn broker_answer(answer: Result<CommandSendReceipt, PulsarError>) -> BrokerAnswer {
        match answer.map_err(PulsarSendFailure::from) {
            Ok(_receipt) => Ok(BrokerVerdict::Confirmed),
            Err(PulsarSendFailure::Rejected(rejection)) => Ok(BrokerVerdict::Rejected {
                message: rejection.message(),
            }),
            Err(PulsarSendFailure::Infrastructure(error)) => Err(Self::publish_error(error)),
        }
    }

    /// The producer message of `record`, or why `record` cannot be one.
    fn message(record: EmitterRecord) -> Result<PulsarProducerMessage, PulsarRecordError> {
        let times = PulsarMessageTimes::of(&record)?;
        Ok(PulsarProducerMessage {
            payload: record.payload,
            properties: record.headers.into_iter().collect(),
            partition_key: record.key,
            event_time: times.event_time,
            deliver_at_time: times.deliver_at_time,
            ..Default::default()
        })
    }

    fn rejected(
        record: EmitterRecordId,
        occurred_at: Timestamp,
        rejection: &PulsarRecordError,
    ) -> RejectedEmitterMember<EmitterRecordId> {
        RejectedEmitterMember::external(record, occurred_at, rejection.message())
    }

    /// A client that did not connect: one its configuration cannot describe is invalid, and any
    /// other could not be initialized. An emitter shows the message its emitter attached, so the
    /// failure's whole chain is what its status says.
    fn client_error(error: Report<PulsarClientError>) -> Report<EmitterOpenError> {
        let connector = PULSAR.keyword();
        let context = match error.current_context() {
            PulsarClientError::Configuration => {
                EmitterOpenError::InvalidConfiguration { connector }
            }
            PulsarClientError::Refused { .. } | PulsarClientError::NotOpened => {
                EmitterOpenError::Initialize { connector }
            }
        };
        let description = format!("{error:#}");
        error.change_context(context).attach_printable(description)
    }

    fn start_error(error: PulsarError) -> Report<EmitterOpenError> {
        let message = error.to_string();
        Report::new(error)
            .change_context(EmitterOpenError::Initialize {
                connector: PULSAR.keyword(),
            })
            .attach_printable(message)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "formats the typed external Pulsar driver failure; no \
                                   internal runtime ownership is inferred")
    )]
    fn publish_error(error: PulsarError) -> Report<EmitterWriteError> {
        let message = error.to_string();
        Report::new(error)
            .change_context(EmitterWriteError::Publish {
                connector: PULSAR.keyword(),
            })
            .attach_printable(message)
    }
}

impl PulsarTopicEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Pulsar(nervix_models::PulsarEmitterEntity::Topic);
}

#[async_trait]
impl EmitterLifecycle for PulsarTopicEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// Producer acceptance completes NO ACK records, while finish waits for their receipts before
    /// dropping the producer. An ACK publish already settled all answers it could observe.
    async fn finish(&mut self, deadline: Instant) -> EmitterFinishResult {
        match &mut self.publishing {
            PulsarPublishing::NoAck(receipts) => receipts.finish(deadline).await,
            PulsarPublishing::Ack(_) => Ok(EmitterFinish::NothingHeld),
        }
    }
}

#[async_trait]
impl RecordEmitter for PulsarTopicEmitter {
    async fn publish(&mut self, records: Vec<EmitterRecord>) -> PerRecordOutcome<EmitterRecordId> {
        let settings = match &mut self.publishing {
            PulsarPublishing::NoAck(receipts) => {
                return receipts.publish(&mut self.producer, records).await;
            }
            PulsarPublishing::Ack(settings) => *settings,
        };
        self.publish_confirmed(records, settings).await
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use futures_util::SinkExt as _;
    use meticulous::ResultExt as _;
    use nervix_connector::RecordEnvelope;
    use nervix_primitives::{
        net::{TcpListener, TcpStream},
        sync::{CancellationToken, mpsc},
        time::sleep,
    };
    use pulsar::message::{
        Codec as PulsarCodec, Message as PulsarFrame,
        proto::{
            BaseCommand, CommandConnected, CommandLookupTopicResponse,
            CommandPartitionedTopicMetadataResponse, CommandPong, CommandProducerSuccess,
            CommandSendError, CommandSendReceipt, CommandSuccess, MessageIdData, MessageMetadata,
            base_command::Type as CommandType,
            command_lookup_topic_response::LookupType as TopicLookupType,
            command_partitioned_topic_metadata_response::LookupType as PartitionLookupType,
        },
    };
    use tokio_util::codec::Framed;

    use super::*;

    /// The error a failed send or receipt carries when the broker answers with `kind`.
    fn server_error(kind: PulsarServerError, reason: Option<&str>) -> PulsarError {
        PulsarError::Producer(PulsarProducerError::Connection(
            PulsarConnectionError::PulsarError(Some(kind), reason.map(str::to_string)),
        ))
    }

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn rejection(error: PulsarError) -> PulsarRecordError {
        match PulsarSendFailure::from(error) {
            PulsarSendFailure::Rejected(rejection) => rejection,
            PulsarSendFailure::Infrastructure(error) => {
                panic!("expected a record rejection, got the infrastructure failure {error}")
            }
        }
    }

    fn is_infrastructure(error: PulsarError) -> bool {
        matches!(
            PulsarSendFailure::from(error),
            PulsarSendFailure::Infrastructure(_)
        )
    }

    fn topic(name: &str) -> PulsarTopicName {
        PulsarTopicName::parse(name).assured("each fixture is a valid Pulsar topic")
    }

    #[test]
    fn a_local_topic_is_qualified_by_its_clients_namespace_or_the_declared_default() {
        assert_eq!(
            qualified_topic(&[], &topic("events")),
            "persistent://public/default/events"
        );
        assert_eq!(
            qualified_topic(&[entry("namespace", "tenant/ns")], &topic("events")),
            "persistent://tenant/ns/events"
        );
        assert_eq!(
            qualified_topic(
                &[entry("namespace", "tenant/ns")],
                &topic("non-persistent://other/ns/events")
            ),
            "non-persistent://other/ns/events"
        );
        assert_eq!(
            qualified_topic(&[], &topic("persistent://tenant/ns/events")),
            "persistent://tenant/ns/events"
        );
    }

    #[test]
    fn a_message_above_the_announced_maximum_is_rejected_with_its_size_and_the_limit() {
        let refused = rejection(PulsarError::Producer(PulsarProducerError::Connection(
            PulsarConnectionError::MessageTooLarge {
                size: 1_200_163,
                max_message_size: 1_048_576,
            },
        )));

        assert_eq!(
            refused,
            PulsarRecordError::MessageTooLarge {
                message_bytes: 1_200_163,
                maximum: 1_048_576,
            }
        );
        assert_eq!(
            refused.to_string(),
            "Pulsar message of 1200163 bytes, counting its metadata and properties, exceeds the \
             broker's maximum message size of 1048576 bytes"
        );
    }

    #[test]
    fn a_message_the_broker_does_not_allow_is_rejected_with_the_broker_reason() {
        let refused = rejection(server_error(
            PulsarServerError::NotAllowedError,
            Some("Exceed maximum message size"),
        ));

        assert_eq!(
            refused.to_string(),
            "the Pulsar broker does not allow the message: Exceed maximum message size"
        );
    }

    #[test]
    fn checksum_failure_remains_an_infrastructure_failure() {
        assert!(is_infrastructure(server_error(
            PulsarServerError::ChecksumError,
            Some("Checksum failed on the broker"),
        )));
    }

    #[test]
    fn missing_or_incompatible_topics_remain_infrastructure_failures() {
        for kind in [
            PulsarServerError::TopicNotFound,
            PulsarServerError::IncompatibleSchema,
            PulsarServerError::ServiceNotReady,
            PulsarServerError::PersistenceError,
        ] {
            assert!(is_infrastructure(server_error(
                kind,
                Some("broker failure")
            )));
        }
    }

    #[test]
    fn a_lost_connection_or_an_unexplained_refusal_remains_an_infrastructure_failure() {
        assert!(is_infrastructure(PulsarError::Producer(
            PulsarProducerError::Connection(PulsarConnectionError::Disconnected)
        )));
        assert!(is_infrastructure(server_error(
            PulsarServerError::NotAllowedError,
            None
        )));
        assert!(is_infrastructure(PulsarError::Connection(
            PulsarConnectionError::PulsarError(
                Some(PulsarServerError::NotAllowedError),
                Some("Reached the maximum number of connections".to_string()),
            )
        )));
    }

    /// The host shows the message an emitter attached, so a client's whole failure reaches the
    /// emitter's status while the emitter's own context stays what a caller decides from.
    #[test]
    fn an_emitter_describes_a_client_that_did_not_connect_by_its_whole_failure() {
        let refused = PulsarTopicEmitter::client_error(Report::new(PulsarClientError::Refused {
            refusal: PulsarRefusal {
                error: Some(PulsarServerError::AuthenticationError),
                reason: Some("Unable to authenticate".to_string()),
            },
        }));
        let not_opened = PulsarTopicEmitter::client_error(
            Report::new(PulsarError::Connection(PulsarConnectionError::Disconnected))
                .change_context(PulsarClientError::NotOpened),
        );
        let configuration =
            PulsarTopicEmitter::client_error(Report::new(PulsarClientError::Configuration));

        let connector = PULSAR.keyword();
        assert_eq!(
            refused.current_context(),
            &EmitterOpenError::Initialize { connector }
        );
        assert_eq!(
            not_opened.current_context(),
            &EmitterOpenError::Initialize { connector }
        );
        assert_eq!(
            configuration.current_context(),
            &EmitterOpenError::InvalidConfiguration { connector }
        );
        assert_eq!(
            attached_message(&refused),
            "the PULSAR broker refused the connection with AuthenticationError: Unable to \
             authenticate"
        );
        assert_eq!(
            attached_message(&not_opened),
            format!(
                "no connection to the PULSAR broker opened: {}",
                PulsarError::Connection(PulsarConnectionError::Disconnected)
            )
        );
        assert_eq!(
            attached_message(&configuration),
            "invalid PULSAR client configuration"
        );
    }

    /// The printable attachment the host reads for an emitter's status.
    fn attached_message(error: &Report<EmitterOpenError>) -> String {
        let attached = error
            .downcast_ref::<String>()
            .assured("the emitter attached the client's failure as text");
        attached.clone()
    }

    /// How the fake broker answers one message, chosen by the first byte of its payload.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum BrokerAnswer {
        /// Any other byte: the send receipt of a stored message.
        Receipt,
        /// `r`: the send error a topic's own maximum message size produces.
        NotAllowed,
        /// `p`: a storage failure, which says nothing about the message itself.
        PersistenceError,
    }

    impl BrokerAnswer {
        fn for_payload(payload: &[u8]) -> Self {
            match payload.first() {
                Some(b'r') => Self::NotAllowed,
                Some(b'p') => Self::PersistenceError,
                _ => Self::Receipt,
            }
        }
    }

    /// When the fake broker answers the messages it receives.
    #[derive(Clone, Debug)]
    enum AnswerOrder {
        /// Each message is answered as it arrives.
        OnArrival,
        /// Every receipt waits for the declared dependency fault duration.
        After(Duration),
        /// A test's action explicitly releases the held broker answer.
        OnRelease(CancellationToken),
        /// The answers wait until this many messages arrived, and the oldest is answered last.
        OldestLastAfter(usize),
    }

    /// One message that reached the fake broker.
    struct ReceivedMessage {
        payload: Vec<u8>,
        metadata: MessageMetadata,
    }

    /// A Pulsar broker on loopback that serves the handshake one producer needs, announces
    /// `max_message_size`, and answers each message it receives by its payload. It reports every
    /// message that reaches it.
    struct FakeBroker {
        service_url: String,
        received: mpsc::UnboundedReceiver<ReceivedMessage>,
    }

    /// One message the fake broker received and has not answered yet.
    struct HeldMessage {
        producer_id: u64,
        sequence_id: u64,
        answer: BrokerAnswer,
    }

    impl FakeBroker {
        async fn start(max_message_size: Option<i32>, order: AnswerOrder) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("a loopback port is available");
            let addr = listener
                .local_addr()
                .expect("a bound listener has an address");
            let service_url = format!("pulsar://{addr}");
            let (received_tx, received) = mpsc::unbounded_channel();
            let lookup_url = service_url.clone();
            nervix_primitives::task::spawn(async move {
                loop {
                    nervix_primitives::task::consume_budget().await;
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    nervix_primitives::task::spawn(Self::serve(
                        Framed::new(stream, PulsarCodec),
                        lookup_url.clone(),
                        max_message_size,
                        order.clone(),
                        received_tx.clone(),
                    ));
                }
            });
            Self {
                service_url,
                received,
            }
        }

        async fn serve(
            mut connection: Framed<TcpStream, PulsarCodec>,
            service_url: String,
            max_message_size: Option<i32>,
            order: AnswerOrder,
            received: mpsc::UnboundedSender<ReceivedMessage>,
        ) {
            let mut held: Vec<HeldMessage> = Vec::new();
            while let Some(Ok(frame)) = connection.next().await {
                nervix_primitives::task::consume_budget().await;
                let command = frame.command;
                let is_send = command.send.is_some();
                let mut replies = Vec::new();
                if command.connect.is_some() {
                    replies.push(Self::connected(max_message_size));
                } else if let Some(request) = command.partition_metadata {
                    replies.push(Self::unpartitioned(request.request_id));
                } else if let Some(request) = command.lookup_topic {
                    replies.push(Self::served_here(&service_url, request.request_id));
                } else if let Some(request) = command.producer {
                    replies.push(Self::producer_ready(request.request_id));
                } else if let Some(send) = command.send {
                    let message = frame.payload.expect("a send carries its message");
                    held.push(HeldMessage {
                        producer_id: send.producer_id,
                        sequence_id: send.sequence_id,
                        answer: BrokerAnswer::for_payload(&message.data),
                    });
                    let received_message = ReceivedMessage {
                        payload: message.data,
                        metadata: message.metadata,
                    };
                    if received.send(received_message).is_err() {
                        // The test that reads the payloads has finished.
                        return;
                    }
                    replies.extend(Self::due_answers(&mut held, &order));
                } else if command.ping.is_some() {
                    replies.push(Self::pong());
                } else if let Some(request) = command.close_producer {
                    replies.push(Self::success(request.request_id));
                }
                for reply in replies {
                    if is_send {
                        match &order {
                            AnswerOrder::After(delay) => sleep(*delay).await,
                            AnswerOrder::OnRelease(release) => release.cancelled().await,
                            AnswerOrder::OnArrival | AnswerOrder::OldestLastAfter(_) => {}
                        }
                    }
                    if connection.send(reply).await.is_err() {
                        return;
                    }
                }
            }
        }

        /// The answers the broker writes now, in the order it writes them.
        fn due_answers(held: &mut Vec<HeldMessage>, order: &AnswerOrder) -> Vec<PulsarFrame> {
            let due = match order {
                AnswerOrder::OnArrival | AnswerOrder::After(_) | AnswerOrder::OnRelease(_) => {
                    std::mem::take(held)
                }
                AnswerOrder::OldestLastAfter(count) if held.len() >= *count => {
                    let mut due = std::mem::take(held);
                    due.rotate_left(1);
                    due
                }
                AnswerOrder::OldestLastAfter(_) => Vec::new(),
            };
            due.into_iter().map(Self::answer).collect()
        }

        fn refusal(message: &HeldMessage, error: PulsarServerError, reason: &str) -> BaseCommand {
            BaseCommand {
                r#type: i32::from(CommandType::SendError),
                send_error: Some(CommandSendError {
                    producer_id: message.producer_id,
                    sequence_id: message.sequence_id,
                    error: i32::from(error),
                    message: reason.to_string(),
                }),
                ..Default::default()
            }
        }

        fn answer(message: HeldMessage) -> PulsarFrame {
            let producer_id = message.producer_id;
            let sequence_id = message.sequence_id;
            let command = match message.answer {
                BrokerAnswer::Receipt => BaseCommand {
                    r#type: i32::from(CommandType::SendReceipt),
                    send_receipt: Some(CommandSendReceipt {
                        producer_id,
                        sequence_id,
                        message_id: Some(MessageIdData {
                            ledger_id: 1,
                            entry_id: sequence_id,
                            ..Default::default()
                        }),
                        highest_sequence_id: None,
                    }),
                    ..Default::default()
                },
                BrokerAnswer::NotAllowed => Self::refusal(
                    &message,
                    PulsarServerError::NotAllowedError,
                    "Exceed maximum message size",
                ),
                BrokerAnswer::PersistenceError => Self::refusal(
                    &message,
                    PulsarServerError::PersistenceError,
                    "bookie unavailable",
                ),
            };
            Self::frame(command)
        }

        fn frame(command: BaseCommand) -> PulsarFrame {
            PulsarFrame {
                command,
                payload: None,
            }
        }

        fn connected(max_message_size: Option<i32>) -> PulsarFrame {
            Self::frame(BaseCommand {
                r#type: i32::from(CommandType::Connected),
                connected: Some(CommandConnected {
                    server_version: "fake".to_string(),
                    protocol_version: Some(12),
                    max_message_size,
                }),
                ..Default::default()
            })
        }

        fn unpartitioned(request_id: u64) -> PulsarFrame {
            Self::frame(BaseCommand {
                r#type: i32::from(CommandType::PartitionedMetadataResponse),
                partition_metadata_response: Some(CommandPartitionedTopicMetadataResponse {
                    partitions: Some(0),
                    request_id,
                    response: Some(i32::from(PartitionLookupType::Success)),
                    ..Default::default()
                }),
                ..Default::default()
            })
        }

        fn served_here(service_url: &str, request_id: u64) -> PulsarFrame {
            Self::frame(BaseCommand {
                r#type: i32::from(CommandType::LookupResponse),
                lookup_topic_response: Some(CommandLookupTopicResponse {
                    broker_service_url: Some(service_url.to_string()),
                    response: Some(i32::from(TopicLookupType::Connect)),
                    request_id,
                    authoritative: Some(true),
                    proxy_through_service_url: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            })
        }

        fn producer_ready(request_id: u64) -> PulsarFrame {
            Self::frame(BaseCommand {
                r#type: i32::from(CommandType::ProducerSuccess),
                producer_success: Some(CommandProducerSuccess {
                    request_id,
                    producer_name: "fake-producer".to_string(),
                    last_sequence_id: Some(-1),
                    producer_ready: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            })
        }

        fn pong() -> PulsarFrame {
            Self::frame(BaseCommand {
                r#type: i32::from(CommandType::Pong),
                pong: Some(CommandPong {}),
                ..Default::default()
            })
        }

        fn success(request_id: u64) -> PulsarFrame {
            Self::frame(BaseCommand {
                r#type: i32::from(CommandType::Success),
                success: Some(CommandSuccess {
                    request_id,
                    schema: None,
                }),
                ..Default::default()
            })
        }

        /// The next message to reach the broker. Messages on one connection arrive in the order
        /// they were written.
        async fn next_message(&mut self) -> ReceivedMessage {
            nervix_primitives::time::timeout(Duration::from_secs(60), self.received.recv())
                .await
                .expect("the broker receives the next message")
                .expect("the broker keeps listening while the test runs")
        }

        /// The next payload to reach the broker.
        async fn next_payload(&mut self) -> Vec<u8> {
            self.next_message().await.payload
        }
    }

    /// A host that ignores what the emitter reports, for tests that only read its publish outcomes.
    struct SilentHost;

    impl nervix_connector::EmitterTransientErrorStatus for SilentHost {
        fn record_transient_error(&self, _reason: String, _retry_after: Duration) {}

        fn clear_transient_error(&self) {}
    }

    impl nervix_connector::EmitterEventReporter for SilentHost {
        fn report_error(&self, _message: String) {}
    }

    impl nervix_connector::EmitterStagingDirectory for SilentHost {
        fn staging_directory(&self) -> std::path::PathBuf {
            std::env::temp_dir()
        }
    }

    impl nervix_connector::EmitterBoundedExecution for SilentHost {
        fn executor(&self) -> nervix_execution::Executor {
            nervix_execution::Executor::default()
        }
    }

    impl nervix_connector::EmitterGeneralErrorHandler for SilentHost {
        fn handle_general_error(
            &self,
            _acks: &nervix_connector::EmitterRecordAcknowledgements,
            _reason: String,
        ) {
        }
    }

    async fn connector(broker: &FakeBroker, mode: BrokerPublishingMode) -> PulsarTopicEmitter {
        let config = PulsarTopicEmitterConfig {
            config: vec![entry("addr", &broker.service_url)],
            topic: PulsarTopicName::parse("limited").expect("the test topic is a valid topic name"),
            mode,
        };
        PulsarTopicEmitter::new(config, EmitterHost::new(SilentHost))
            .await
            .expect("the fake broker serves the producer handshake")
    }

    fn confirmed(max_in_flight: usize) -> BrokerPublishingMode {
        BrokerPublishingMode::Ack(AckConfirmation {
            max_in_flight: NonZeroUsize::new(max_in_flight)
                .expect("the test window is at least one message"),
            timeout: Duration::from_secs(60),
        })
    }

    fn record(index: usize, payload: Vec<u8>) -> EmitterRecord {
        EmitterRecord::new(
            EmitterRecordId::new(index),
            None,
            payload,
            vec![("source".to_string(), "web".to_string())],
            Timestamp::from_unix_nanos(0),
        )
    }

    fn rejected_messages(
        rejected: &[RejectedEmitterMember<EmitterRecordId>],
    ) -> Vec<(EmitterRecordId, String)> {
        rejected
            .iter()
            .map(|rejected| (rejected.id, rejected.error.message.clone()))
            .collect()
    }

    #[nervix_primitives::test]
    async fn finishing_waits_for_the_receipt_of_an_unconfirmed_record() {
        let release = CancellationToken::new();
        let unconfirmed_broker =
            FakeBroker::start(None, AnswerOrder::OnRelease(release.clone())).await;
        let mut unconfirmed = connector(&unconfirmed_broker, BrokerPublishingMode::NoAck).await;
        let confirming_broker = FakeBroker::start(None, AnswerOrder::OnArrival).await;
        let mut confirming = connector(&confirming_broker, confirmed(1)).await;

        let published = unconfirmed
            .publish(vec![record(0, b"held".to_vec())])
            .await
            .into_parts();
        assert_eq!(published.delivered, vec![EmitterRecordId::new(0)]);
        let mut finishing = Box::pin(unconfirmed.finish(Instant::now() + Duration::from_secs(60)));
        assert!(
            finishing.as_mut().now_or_never().is_none(),
            "finish must await the held receipt"
        );
        release.cancel();
        let flushed = finishing
            .await
            .assured("the loopback broker answers the accepted record before the finish deadline");
        let nothing_held = confirming
            .finish(Instant::now())
            .await
            .assured("the confirming emitter has published no record and holds nothing");

        assert_eq!(flushed, EmitterFinish::Flushed);
        assert_eq!(nothing_held, EmitterFinish::NothingHeld);
    }

    #[nervix_primitives::test]
    async fn finishing_counts_the_unconfirmed_records_that_missed_its_deadline() {
        let mut broker = FakeBroker::start(None, AnswerOrder::OldestLastAfter(3)).await;
        let mut connector = connector(&broker, BrokerPublishingMode::NoAck).await;
        let published = connector
            .publish(vec![record(0, b"held".to_vec())])
            .await
            .into_parts();
        assert_eq!(published.delivered, vec![EmitterRecordId::new(0)]);
        assert_eq!(broker.next_payload().await, b"held");

        let Err(failure) = connector.finish(Instant::now()).await else {
            panic!("the finish must report the receipt the broker has not answered");
        };
        assert_eq!(
            *failure.current_context(),
            nervix_connector::EmitterFinishError::Unsent {
                connector: PULSAR.keyword(),
                records: 1
            }
        );
    }

    #[nervix_primitives::test]
    async fn a_receipt_waits_for_the_declared_timeout_above_the_driver_default() {
        let broker = FakeBroker::start(None, AnswerOrder::After(Duration::from_secs(35))).await;
        let mode = BrokerPublishingMode::Ack(AckConfirmation {
            max_in_flight: NonZeroUsize::new(1).assured("one record is a nonempty window"),
            timeout: Duration::from_secs(45),
        });
        let mut connector = connector(&broker, mode).await;
        let outcome = connector
            .publish(vec![record(0, b"delayed".to_vec())])
            .await
            .into_parts();
        assert!(
            outcome.infrastructure_error.is_none(),
            "{:?}",
            outcome.infrastructure_error
        );
        assert_eq!(outcome.delivered, vec![EmitterRecordId::new(0)]);
        assert!(outcome.rejected.is_empty());
    }

    #[nervix_primitives::test]
    async fn a_timeout_past_the_clock_range_is_refused_before_the_driver_uses_it() {
        let broker = FakeBroker::start(None, AnswerOrder::OnArrival).await;
        let config = PulsarTopicEmitterConfig {
            config: vec![entry("addr", &broker.service_url)],
            topic: PulsarTopicName::parse("limited").assured("the fixture topic is a valid name"),
            mode: BrokerPublishingMode::Ack(AckConfirmation {
                max_in_flight: NonZeroUsize::new(1).assured("one is nonzero"),
                timeout: Duration::MAX,
            }),
        };
        let Err(failure) = PulsarTopicEmitter::new(config, EmitterHost::new(SilentHost)).await
        else {
            panic!("the driver's timer must not receive a timeout past the clock range");
        };
        assert_eq!(
            *failure.current_context(),
            EmitterOpenError::InvalidConfiguration {
                connector: PULSAR.keyword()
            }
        );
        assert_eq!(
            failure.downcast_ref::<nervix_connector::ConfirmationWindowError>(),
            Some(
                &nervix_connector::ConfirmationWindowError::DeadlineOutOfRange {
                    timeout: Duration::MAX
                }
            )
        );
    }

    #[nervix_primitives::test]
    async fn a_message_above_the_announced_maximum_is_rejected_before_it_is_written() {
        for mode in [BrokerPublishingMode::NoAck, confirmed(1)] {
            let mut broker = FakeBroker::start(Some(4096), AnswerOrder::OnArrival).await;
            let mut connector = connector(&broker, mode).await;

            let outcome = connector
                .publish(vec![
                    record(0, vec![b'd'; 5000]),
                    record(1, vec![b'd'; 100]),
                ])
                .await
                .into_parts();

            assert_eq!(outcome.delivered, vec![EmitterRecordId::new(1)], "{mode:?}");
            let rejected = rejected_messages(&outcome.rejected);
            let [(rejected, message)] = rejected.as_slice() else {
                panic!("exactly the oversize record is rejected: {rejected:?}");
            };
            assert_eq!(*rejected, EmitterRecordId::new(0));
            assert!(
                message.starts_with("PULSAR rejected record: Pulsar message of 50"),
                "{message}"
            );
            assert!(
                message.ends_with("exceeds the broker's maximum message size of 4096 bytes"),
                "{message}"
            );
            assert!(outcome.infrastructure_error.is_none(), "{mode:?}");
            assert_eq!(
                broker.next_payload().await,
                vec![b'd'; 100],
                "the oversize message, written first, never reaches the broker"
            );
        }
    }

    #[nervix_primitives::test]
    async fn a_message_the_broker_refuses_is_rejected_and_the_window_continues() {
        let broker = FakeBroker::start(Some(4096), AnswerOrder::OnArrival).await;
        let mut connector = connector(&broker, confirmed(2)).await;

        let outcome = connector
            .publish(vec![
                record(0, b"r-oversize-for-the-topic".to_vec()),
                record(1, b"d".to_vec()),
                record(2, b"d".to_vec()),
            ])
            .await
            .into_parts();

        assert_eq!(
            outcome.delivered,
            vec![EmitterRecordId::new(1), EmitterRecordId::new(2)]
        );
        assert_eq!(
            rejected_messages(&outcome.rejected),
            vec![(
                EmitterRecordId::new(0),
                "PULSAR rejected record: the Pulsar broker does not allow the message: Exceed \
                 maximum message size"
                    .to_string()
            )]
        );
        assert!(outcome.infrastructure_error.is_none());
    }

    /// The broker answers the three newer messages before it fails the oldest one, so their
    /// answers are in by the time the emitter returns the failure. The delivered and the refused one
    /// are not sent again by the retry; the one that failed like the oldest is left to it.
    #[nervix_primitives::test]
    async fn answers_behind_a_failed_oldest_message_are_kept_including_a_refusal() {
        let broker = FakeBroker::start(None, AnswerOrder::OldestLastAfter(4)).await;
        let mut connector = connector(&broker, confirmed(4)).await;

        let outcome = connector
            .publish(vec![
                record(0, b"p".to_vec()),
                record(1, b"r".to_vec()),
                record(2, b"d".to_vec()),
                record(3, b"p".to_vec()),
            ])
            .await
            .into_parts();

        assert_eq!(outcome.delivered, vec![EmitterRecordId::new(2)]);
        assert_eq!(
            rejected_messages(&outcome.rejected)
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec![EmitterRecordId::new(1)]
        );
        let failure = outcome
            .infrastructure_error
            .expect("the oldest message's storage failure fails the attempt");
        assert!(
            format!("{failure:?}").contains("PersistenceError"),
            "{failure:?}"
        );
    }

    /// A record of a batch that executed at 2001-02-03T04:05:06.789Z, which writes `timestamp` and
    /// `deliver_after`.
    fn timed_record(
        index: usize,
        payload: &[u8],
        timestamp: Timestamp,
        deliver_after: Option<Duration>,
    ) -> EmitterRecord {
        let envelope = RecordEnvelope {
            timestamp: Some(timestamp),
            deliver_after,
            ..RecordEnvelope::default()
        };
        let mut record = record(index, payload.to_vec()).with_envelope(envelope);
        record.occurred_at = Timestamp::from_unix_nanos(981_173_106_789_000_000);
        record
    }

    #[nervix_primitives::test]
    async fn a_messages_times_reach_the_broker_and_a_record_without_them_carries_none() {
        for mode in [BrokerPublishingMode::NoAck, confirmed(1)] {
            let mut broker = FakeBroker::start(None, AnswerOrder::OnArrival).await;
            let mut connector = connector(&broker, mode).await;
            let timestamp = Timestamp::from_unix_nanos(1_000_000_002_000_000);
            let deliver_after = Some(Duration::from_secs(5));

            let outcome = connector
                .publish(vec![
                    timed_record(0, b"timed", timestamp, deliver_after),
                    record(1, b"untimed".to_vec()),
                ])
                .await
                .into_parts();

            assert_eq!(
                outcome.delivered,
                vec![EmitterRecordId::new(0), EmitterRecordId::new(1)],
                "{mode:?}"
            );
            let timed = broker.next_message().await;
            assert_eq!(timed.payload, b"timed");
            assert_eq!(timed.metadata.event_time, Some(1_000_000_002));
            assert_eq!(timed.metadata.deliver_at_time, Some(981_173_111_789));
            let untimed = broker.next_message().await;
            assert_eq!(untimed.payload, b"untimed");
            assert_eq!(untimed.metadata.event_time, None);
            assert_eq!(untimed.metadata.deliver_at_time, None);
        }
    }

    #[nervix_primitives::test]
    async fn a_record_whose_time_no_message_carries_is_rejected_before_it_is_written() {
        for mode in [BrokerPublishingMode::NoAck, confirmed(1)] {
            let mut broker = FakeBroker::start(None, AnswerOrder::OnArrival).await;
            let mut connector = connector(&broker, mode).await;
            let before_the_epoch = Timestamp::from_unix_nanos(-1);

            let outcome = connector
                .publish(vec![
                    timed_record(0, b"refused", before_the_epoch, None),
                    record(1, b"written".to_vec()),
                ])
                .await
                .into_parts();

            assert_eq!(outcome.delivered, vec![EmitterRecordId::new(1)], "{mode:?}");
            assert_eq!(
                rejected_messages(&outcome.rejected),
                vec![(
                    EmitterRecordId::new(0),
                    "PULSAR rejected record: message TIMESTAMP 1969-12-31T23:59:59.999999999Z \
                     precedes 1970-01-01T00:00:00.001Z, the earliest event time a PULSAR message \
                     carries"
                        .to_string()
                )],
                "{mode:?}"
            );
            assert!(outcome.infrastructure_error.is_none(), "{mode:?}");
            assert_eq!(
                broker.next_payload().await,
                b"written",
                "the refused message, written first, never reaches the broker"
            );
        }
    }

    /// The host hands a record it retained to every attempt, so each one writes what the first
    /// wrote, however much later it runs.
    #[nervix_primitives::test]
    async fn every_attempt_at_a_record_writes_the_times_of_the_first() {
        let mut broker = FakeBroker::start(None, AnswerOrder::OnArrival).await;
        let mut connector = connector(&broker, confirmed(1)).await;
        let timestamp = Timestamp::from_unix_nanos(1_000_000_002_000_000);
        let deliver_after = Some(Duration::from_secs(5));

        for _attempt in 0..2 {
            let attempt = timed_record(0, b"timed", timestamp, deliver_after);
            let outcome = connector.publish(vec![attempt]).await.into_parts();
            assert_eq!(outcome.delivered, vec![EmitterRecordId::new(0)]);
        }

        let first = broker.next_message().await;
        let second = broker.next_message().await;
        assert_eq!(first.metadata.event_time, second.metadata.event_time);
        assert_eq!(
            first.metadata.deliver_at_time,
            second.metadata.deliver_at_time
        );
        assert_eq!(second.metadata.deliver_at_time, Some(981_173_111_789));
    }

    /// PULSAR TOPIC emitters declare last wins with exact names: a record's header writes become
    /// the producer message's properties, one per name, the last value written for it, and names
    /// that differ in letter case stay two names.
    #[test]
    fn a_records_headers_become_one_property_per_name_holding_the_last_value() {
        let mut headers = Vec::new();
        for (name, value) in [
            ("trace", "a"),
            ("Tenant", "x"),
            ("trace", "b"),
            ("tenant", "y"),
        ] {
            headers.push((name.to_string(), value.to_string()));
        }
        let mut record = record(0, b"{}".to_vec());
        record.headers = headers;
        let message = PulsarTopicEmitter::message(record).assured("the record sets no time");
        let properties: std::collections::BTreeMap<_, _> = message.properties.into_iter().collect();
        let expected = std::collections::BTreeMap::from([
            ("Tenant".to_string(), "x".to_string()),
            ("tenant".to_string(), "y".to_string()),
            ("trace".to_string(), "b".to_string()),
        ]);
        assert_eq!(properties, expected);
    }
}
