//! Pulsar source and sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Pulsar client and producer configuration, its TLS options, topic
//!   qualification, the shared-subscription consumers a source reads through, message
//!   properties as ingest headers, per-message acknowledgement, record and property
//!   publication, and the classification of a send or its receipt, including a message the
//!   client refuses because it exceeds the maximum message size the broker announced.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio and
//!   `pulsar`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

mod source;

use std::{collections::VecDeque, time::Duration};

use async_trait::async_trait;
use error_stack::Report;
use futures_util::FutureExt;
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, BrokerPublishingMode, PerRecordOutcome, RecordSink, RejectedSinkRecord,
    SinkHost, SinkLifecycle, SinkPublishError, SinkPublishResult, SinkRecord, SinkRecordId,
    SinkStartError, SinkStartResult, client_config_value, client_tls_paths,
    optional_bool_client_config_value, optional_client_config_value, read_tls_file,
};
use nervix_models::{ClientConfigEntry, Timestamp, TopicName};
use nervix_primitives::time::{Instant, sleep};
use pulsar::{
    ConnectionRetryOptions, Error as PulsarError, OperationRetryOptions, Pulsar,
    TlsOptions as PulsarTlsOptions, TokioExecutor,
    error::{ConnectionError as PulsarConnectionError, ProducerError as PulsarProducerError},
    message::proto::ServerError as PulsarServerError,
    producer::{Message as PulsarProducerMessage, SendFuture as PulsarSendFuture},
};
pub use source::{
    PulsarMessageProperties, PulsarSource, PulsarSourceError, PulsarSourceMessage,
    PulsarSourcePlan, PulsarSourcePosition, PulsarSourceSettings,
};
use thiserror::Error;

const PULSAR: &str = "pulsar";

/// Why one message is refused for good: sending it again cannot succeed while the broker keeps its
/// configuration.
#[derive(Debug, PartialEq, Eq, Error)]
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

/// What one Pulsar sink publishes through: its client entries, topic and publishing mode.
pub struct PulsarSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub topic: TopicName,
    pub mode: BrokerPublishingMode,
}

pub struct PulsarSink {
    producer: pulsar::Producer<TokioExecutor>,
    mode: BrokerPublishingMode,
}

struct PendingPulsarConfirmation {
    record: SinkRecordId,
    occurred_at: Timestamp,
    deadline: Instant,
    confirmation: PulsarSendFuture,
}

impl PulsarSink {
    pub async fn new(config: PulsarSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let producer = Self::producer_from_config(&config.config, config.topic.as_str()).await?;
        Ok(Self {
            producer,
            mode: config.mode,
        })
    }

    async fn producer_from_config(
        config: &[ClientConfigEntry],
        topic: &str,
    ) -> SinkStartResult<pulsar::Producer<TokioExecutor>> {
        let pulsar = Self::client_from_config(config).await?;
        let topic_name = Self::topic_from_config(config, topic);
        pulsar
            .producer()
            .with_topic(topic_name)
            .build()
            .await
            .map_err(Self::start_error)
    }

    async fn client_from_config(
        config: &[ClientConfigEntry],
    ) -> SinkStartResult<Pulsar<TokioExecutor>> {
        let addr = Self::config_value(config, "addr")?;
        let (connection_retry_options, operation_retry_options) = Self::retry_options();
        let mut builder = Pulsar::builder(addr, TokioExecutor)
            .with_connection_retry_options(connection_retry_options)
            .with_operation_retry_options(operation_retry_options);
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
        builder.build().await.map_err(Self::start_error)
    }

    fn retry_options() -> (ConnectionRetryOptions, OperationRetryOptions) {
        (
            ConnectionRetryOptions {
                max_retries: 0,
                ..Default::default()
            },
            OperationRetryOptions {
                max_retries: Some(0),
                ..Default::default()
            },
        )
    }

    fn tls_options_from_config(
        config: &[ClientConfigEntry],
    ) -> SinkStartResult<Option<PulsarTlsOptions>> {
        let tls = client_tls_paths(config);
        if tls.cert_file.is_some() || tls.key_file.is_some() {
            return Err(Self::config_error(
                "Pulsar TLS currently supports only 'tls_ca_file'; client authentication via \
                 'tls_cert_file' and 'tls_key_file' is not supported",
            ));
        }

        let allow_insecure_connection =
            Self::optional_bool_config_value(config, "tls_allow_insecure_connection")?;
        let tls_hostname_verification_enabled =
            Self::optional_bool_config_value(config, "tls_hostname_verification_enabled")?;

        if tls.ca_file.is_none()
            && allow_insecure_connection.is_none()
            && tls_hostname_verification_enabled.is_none()
        {
            return Ok(None);
        }

        let mut tls_options = PulsarTlsOptions::default();
        if let Some(ca_file) = tls.ca_file.as_ref() {
            tls_options.certificate_chain = Some(
                read_tls_file(ca_file, "TLS CA certificate").map_err(|error| {
                    let message = error.current_context().to_string();
                    error
                        .change_context(SinkStartError::InvalidConfiguration { sink: PULSAR })
                        .attach_printable(message)
                })?,
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

    fn topic_from_config(config: &[ClientConfigEntry], topic: &str) -> String {
        if topic.contains("://") {
            return topic.to_string();
        }

        let namespace =
            optional_client_config_value(config, "namespace").unwrap_or("public/default");
        format!("persistent://{namespace}/{topic}")
    }

    /// `MODE NO_ACK`: a record is delivered once the producer accepts it, and the send receipt it
    /// would have produced is dropped rather than awaited.
    async fn publish_unconfirmed(
        &mut self,
        records: Vec<SinkRecord>,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) {
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let sent = self.producer.send_non_blocking(Self::message(record)).await;
            match sent.map_err(PulsarSendFailure::from) {
                Ok(confirmation) => {
                    drop(confirmation);
                    outcome.deliver(record_id);
                }
                Err(PulsarSendFailure::Rejected(rejection)) => {
                    outcome.reject(Self::rejected(record_id, occurred_at, &rejection));
                }
                Err(PulsarSendFailure::Infrastructure(source)) => {
                    outcome.fail(Self::publish_error(format!(
                        "failed to enqueue pulsar message: {source}"
                    )));
                    return;
                }
            }
        }
    }

    /// `MODE ACK`: at most `max_in_flight` send receipts are outstanding at once, and every one is
    /// awaited before the batch finishes. The window carries the confirmation settings, so the
    /// drain below never has to ask a mode that has no confirmations what its timeout is.
    async fn publish_confirmed(
        &mut self,
        records: Vec<SinkRecord>,
        AckConfirmation {
            max_in_flight,
            timeout,
        }: AckConfirmation,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) {
        let mut pending: VecDeque<PendingPulsarConfirmation> = VecDeque::new();
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let sent = self.producer.send_non_blocking(Self::message(record)).await;
            let confirmation = match sent.map_err(PulsarSendFailure::from) {
                Ok(confirmation) => confirmation,
                Err(PulsarSendFailure::Rejected(rejection)) => {
                    outcome.reject(Self::rejected(record_id, occurred_at, &rejection));
                    continue;
                }
                Err(PulsarSendFailure::Infrastructure(source)) => {
                    outcome.fail(Self::publish_error(format!(
                        "failed to enqueue pulsar message: {source}"
                    )));
                    return;
                }
            };
            let Some(deadline) = Instant::now().checked_add(timeout) else {
                outcome.fail(Self::publish_error(
                    "pulsar ACK TIMEOUT exceeds the monotonic clock range",
                ));
                return;
            };
            pending.push_back(PendingPulsarConfirmation {
                record: record_id,
                occurred_at,
                deadline,
                confirmation,
            });
            if pending.len() >= max_in_flight.get()
                && let Err(error) = Self::confirm_oldest(&mut pending, timeout, outcome).await
            {
                outcome.fail(error);
                return;
            }
        }
        while !pending.is_empty() {
            nervix_primitives::task::consume_budget().await;
            if let Err(error) = Self::confirm_oldest(&mut pending, timeout, outcome).await {
                outcome.fail(error);
                return;
            }
        }
    }

    fn message(record: SinkRecord) -> PulsarProducerMessage {
        PulsarProducerMessage {
            payload: record.payload,
            properties: record.headers.into_iter().collect(),
            partition_key: record.key,
            ..Default::default()
        }
    }

    async fn confirm_oldest(
        pending: &mut VecDeque<PendingPulsarConfirmation>,
        timeout: Duration,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) -> SinkPublishResult<()> {
        let Some(oldest) = pending.front_mut() else {
            return Err(Self::publish_error(
                "pulsar acknowledgment window unexpectedly became empty",
            ));
        };
        let remaining = oldest
            .deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            Self::harvest_ready_after_oldest_failure(pending, outcome);
            return Err(Self::publish_error(format!(
                "pulsar receipt exceeded ACK TIMEOUT {}",
                humantime::format_duration(timeout)
            )));
        }
        let result = nervix_primitives::select! {
            biased;
            result = &mut oldest.confirmation => Some(result),
            _ = sleep(remaining) => None,
        };
        let Some(result) = result else {
            Self::harvest_ready_after_oldest_failure(pending, outcome);
            return Err(Self::publish_error(format!(
                "pulsar receipt exceeded ACK TIMEOUT {}",
                humantime::format_duration(timeout)
            )));
        };
        let record_id = oldest.record;
        let occurred_at = oldest.occurred_at;
        match result.map_err(PulsarSendFailure::from) {
            Ok(_receipt) => {
                pending.pop_front();
                outcome.deliver(record_id);
                Ok(())
            }
            Err(PulsarSendFailure::Rejected(rejection)) => {
                pending.pop_front();
                outcome.reject(Self::rejected(record_id, occurred_at, &rejection));
                Ok(())
            }
            Err(PulsarSendFailure::Infrastructure(source)) => {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                Err(Self::publish_error(source))
            }
        }
    }

    /// Collects the records behind the oldest one whose confirmation already resolved.
    ///
    /// The caller reached here because the oldest record failed or timed out, and it is about to
    /// return that failure for the whole publish. Records behind it that already succeeded or were
    /// individually rejected are recorded so the retry does not send them again. Anything else is
    /// deliberately left in neither list: its failure is the same infrastructure failure the
    /// caller is returning, and classifying it per record would report one outage many times.
    fn harvest_ready_after_oldest_failure(
        pending: &mut VecDeque<PendingPulsarConfirmation>,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) {
        let mut index = 1;
        while index < pending.len() {
            let ready = pending
                .get_mut(index)
                .and_then(|confirmation| (&mut confirmation.confirmation).now_or_never());
            let Some(result) = ready else {
                index += 1;
                continue;
            };
            let confirmation = pending.remove(index).verified(
                "the index came from scanning this same pending window, which nothing else \
                 removes from",
            );
            match result.map_err(PulsarSendFailure::from) {
                Ok(_receipt) => outcome.deliver(confirmation.record),
                Err(PulsarSendFailure::Rejected(rejection)) => {
                    outcome.reject(Self::rejected(
                        confirmation.record,
                        confirmation.occurred_at,
                        &rejection,
                    ));
                }
                Err(PulsarSendFailure::Infrastructure(_)) => {}
            }
        }
    }

    fn rejected(
        record: SinkRecordId,
        occurred_at: Timestamp,
        rejection: &PulsarRecordError,
    ) -> RejectedSinkRecord<SinkRecordId> {
        RejectedSinkRecord::external(
            record,
            occurred_at,
            format!("pulsar rejected record: {rejection}"),
        )
    }

    fn config_value(config: &[ClientConfigEntry], key: &str) -> SinkStartResult<String> {
        client_config_value(config, key, "Pulsar").map_err(|error| {
            let message = error.current_context().to_string();
            error
                .change_context(SinkStartError::InvalidConfiguration { sink: PULSAR })
                .attach_printable(message)
        })
    }

    fn optional_bool_config_value(
        config: &[ClientConfigEntry],
        key: &str,
    ) -> SinkStartResult<Option<bool>> {
        optional_bool_client_config_value(config, key).map_err(|error| {
            let message = error.current_context().to_string();
            error
                .change_context(SinkStartError::InvalidConfiguration { sink: PULSAR })
                .attach_printable(message)
        })
    }

    fn config_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::InvalidConfiguration { sink: PULSAR })
            .attach_printable(error.to_string())
    }

    fn start_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::Initialize { sink: PULSAR }).attach_printable(error.to_string())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "formats the typed external Pulsar driver failure; no \
                                   internal runtime ownership is inferred")
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: PULSAR }).attach_printable(error.to_string())
    }
}

#[async_trait]
impl SinkLifecycle for PulsarSink {}

#[async_trait]
impl RecordSink for PulsarSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        match self.mode {
            BrokerPublishingMode::NoAck => {
                self.publish_unconfirmed(records, &mut outcome).await;
            }
            BrokerPublishingMode::Ack(confirmation) => {
                self.publish_confirmed(records, confirmation, &mut outcome)
                    .await;
            }
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use futures_util::{SinkExt as _, StreamExt as _};
    use nervix_primitives::{
        net::{TcpListener, TcpStream},
        sync::mpsc,
    };
    use pulsar::message::{
        Codec as PulsarCodec, Message as PulsarFrame,
        proto::{
            BaseCommand, CommandConnected, CommandLookupTopicResponse,
            CommandPartitionedTopicMetadataResponse, CommandPong, CommandProducerSuccess,
            CommandSendError, CommandSendReceipt, CommandSuccess, MessageIdData,
            base_command::Type as CommandType,
            command_lookup_topic_response::LookupType as TopicLookupType,
            command_partitioned_topic_metadata_response::LookupType as PartitionLookupType,
        },
    };
    use tempfile::tempdir;
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
    #[derive(Clone, Copy, Debug)]
    enum AnswerOrder {
        /// Each message is answered as it arrives.
        OnArrival,
        /// The answers wait until this many messages arrived, and the oldest is answered last.
        OldestLastAfter(usize),
    }

    /// A Pulsar broker on loopback that serves the handshake one producer needs, announces
    /// `max_message_size`, and answers each message it receives by its payload. It reports every
    /// payload that reaches it.
    struct FakeBroker {
        service_url: String,
        received: mpsc::UnboundedReceiver<Vec<u8>>,
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
                        order,
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
            received: mpsc::UnboundedSender<Vec<u8>>,
        ) {
            let mut held: Vec<HeldMessage> = Vec::new();
            while let Some(Ok(frame)) = connection.next().await {
                nervix_primitives::task::consume_budget().await;
                let command = frame.command;
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
                    let payload = frame.payload.expect("a send carries its message").data;
                    held.push(HeldMessage {
                        producer_id: send.producer_id,
                        sequence_id: send.sequence_id,
                        answer: BrokerAnswer::for_payload(&payload),
                    });
                    if received.send(payload).is_err() {
                        // The test that reads the payloads has finished.
                        return;
                    }
                    replies.extend(Self::due_answers(&mut held, order));
                } else if command.ping.is_some() {
                    replies.push(Self::pong());
                } else if let Some(request) = command.close_producer {
                    replies.push(Self::success(request.request_id));
                }
                for reply in replies {
                    if connection.send(reply).await.is_err() {
                        return;
                    }
                }
            }
        }

        /// The answers the broker writes now, in the order it writes them.
        fn due_answers(held: &mut Vec<HeldMessage>, order: AnswerOrder) -> Vec<PulsarFrame> {
            let due = match order {
                AnswerOrder::OnArrival => std::mem::take(held),
                AnswerOrder::OldestLastAfter(count) if held.len() >= count => {
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

        /// The next payload to reach the broker. Messages on one connection arrive in the order
        /// they were written.
        async fn next_payload(&mut self) -> Vec<u8> {
            nervix_primitives::time::timeout(Duration::from_secs(60), self.received.recv())
                .await
                .expect("the broker receives the next message")
                .expect("the broker keeps listening while the test runs")
        }
    }

    /// A host that ignores what the sink reports, for tests that only read its publish outcomes.
    struct SilentHost;

    impl nervix_connector::SinkTransientErrorStatus for SilentHost {
        fn record_transient_error(&self, _reason: String, _retry_after: Duration) {}

        fn clear_transient_error(&self) {}
    }

    impl nervix_connector::SinkEventReporter for SilentHost {
        fn report_error(&self, _message: String) {}
    }

    impl nervix_connector::SinkStagingDirectory for SilentHost {
        fn staging_directory(&self) -> std::path::PathBuf {
            std::env::temp_dir()
        }
    }

    impl nervix_connector::SinkBoundedExecution for SilentHost {
        fn executor(&self) -> nervix_execution::Executor {
            nervix_execution::Executor::default()
        }
    }

    impl nervix_connector::SinkGeneralErrorHandler for SilentHost {
        fn handle_general_error(
            &self,
            _acks: &nervix_connector::SinkAcknowledgements,
            _reason: String,
        ) {
        }
    }

    async fn sink(broker: &FakeBroker, mode: BrokerPublishingMode) -> PulsarSink {
        let config = PulsarSinkConfig {
            config: vec![entry("addr", &broker.service_url)],
            topic: TopicName::parse("limited").expect("the test topic is a valid topic name"),
            mode,
        };
        PulsarSink::new(config, SinkHost::new(SilentHost))
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

    fn record(index: usize, payload: Vec<u8>) -> SinkRecord {
        SinkRecord::new(
            SinkRecordId::new(index),
            None,
            payload,
            vec![("source".to_string(), "web".to_string())],
            Timestamp::from_unix_nanos(0),
        )
    }

    fn rejected_messages(
        rejected: &[RejectedSinkRecord<SinkRecordId>],
    ) -> Vec<(SinkRecordId, String)> {
        rejected
            .iter()
            .map(|rejected| (rejected.id, rejected.error.message.clone()))
            .collect()
    }

    #[nervix_primitives::test]
    async fn a_message_above_the_announced_maximum_is_rejected_before_it_is_written() {
        for mode in [BrokerPublishingMode::NoAck, confirmed(1)] {
            let mut broker = FakeBroker::start(Some(4096), AnswerOrder::OnArrival).await;
            let mut sink = sink(&broker, mode).await;

            let outcome = sink
                .publish(vec![
                    record(0, vec![b'd'; 5000]),
                    record(1, vec![b'd'; 100]),
                ])
                .await
                .into_parts();

            assert_eq!(outcome.delivered, vec![SinkRecordId::new(1)], "{mode:?}");
            let rejected = rejected_messages(&outcome.rejected);
            let [(rejected, message)] = rejected.as_slice() else {
                panic!("exactly the oversize record is rejected: {rejected:?}");
            };
            assert_eq!(*rejected, SinkRecordId::new(0));
            assert!(
                message.starts_with("pulsar rejected record: Pulsar message of 50"),
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
        let mut sink = sink(&broker, confirmed(2)).await;

        let outcome = sink
            .publish(vec![
                record(0, b"r-oversize-for-the-topic".to_vec()),
                record(1, b"d".to_vec()),
                record(2, b"d".to_vec()),
            ])
            .await
            .into_parts();

        assert_eq!(
            outcome.delivered,
            vec![SinkRecordId::new(1), SinkRecordId::new(2)]
        );
        assert_eq!(
            rejected_messages(&outcome.rejected),
            vec![(
                SinkRecordId::new(0),
                "pulsar rejected record: the Pulsar broker does not allow the message: Exceed \
                 maximum message size"
                    .to_string()
            )]
        );
        assert!(outcome.infrastructure_error.is_none());
    }

    /// The broker answers the three newer messages before it fails the oldest one, so their
    /// answers are in by the time the sink returns the failure. The delivered and the refused one
    /// are not sent again by the retry; the one that failed like the oldest is left to it.
    #[nervix_primitives::test]
    async fn answers_behind_a_failed_oldest_message_are_kept_including_a_refusal() {
        let broker = FakeBroker::start(None, AnswerOrder::OldestLastAfter(4)).await;
        let mut sink = sink(&broker, confirmed(4)).await;

        let outcome = sink
            .publish(vec![
                record(0, b"p".to_vec()),
                record(1, b"r".to_vec()),
                record(2, b"d".to_vec()),
                record(3, b"p".to_vec()),
            ])
            .await
            .into_parts();

        assert_eq!(outcome.delivered, vec![SinkRecordId::new(2)]);
        assert_eq!(
            rejected_messages(&outcome.rejected)
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec![SinkRecordId::new(1)]
        );
        let failure = outcome
            .infrastructure_error
            .expect("the oldest message's storage failure fails the attempt");
        assert!(
            format!("{failure:?}").contains("PersistenceError"),
            "{failure:?}"
        );
    }

    #[test]
    fn client_retries_are_disabled_in_favor_of_the_declared_retry_policy() {
        let (connection, operation) = PulsarSink::retry_options();

        assert_eq!(connection.max_retries, 0);
        assert_eq!(operation.max_retries, Some(0));
        assert!(!operation.allow_retry(0));
    }

    #[test]
    fn pulsar_tls_options_load_certificate_chain_and_flags() {
        let tempdir = tempdir().expect("tempdir should be created");
        let ca_path = tempdir.path().join("ca.pem");
        std::fs::write(&ca_path, "test-ca").expect("ca file should be written");

        let options = PulsarSink::tls_options_from_config(&[
            entry("tls_ca_file", &ca_path.display().to_string()),
            entry("tls_allow_insecure_connection", "true"),
            entry("tls_hostname_verification_enabled", "false"),
        ])
        .expect("pulsar tls options should load")
        .expect("tls options should be present");

        assert_eq!(
            options
                .certificate_chain
                .expect("certificate chain should be present"),
            b"test-ca".to_vec()
        );
        assert!(options.allow_insecure_connection);
        assert!(!options.tls_hostname_verification_enabled);
    }

    #[test]
    fn pulsar_tls_options_reject_client_auth_material() {
        let error = PulsarSink::tls_options_from_config(&[
            entry("tls_cert_file", "/tmp/client.crt"),
            entry("tls_key_file", "/tmp/client.key"),
        ])
        .expect_err("pulsar mTLS material should be rejected");
        let error = format!("{error:?}");

        assert!(error.contains("tls_cert_file"));
        assert!(error.contains("tls_key_file"));
    }

    #[test]
    fn pulsar_tls_options_reject_invalid_boolean_values() {
        let error =
            PulsarSink::tls_options_from_config(&[entry("tls_allow_insecure_connection", "maybe")])
                .expect_err("invalid pulsar tls boolean should be rejected");
        let error = format!("{error:?}");

        assert!(error.contains("tls_allow_insecure_connection"));
        assert!(error.contains("maybe"));
    }
}
