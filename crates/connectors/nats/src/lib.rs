//! NATS source and sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The NATS connection a client configures, its reconnect delay, the queue
//!   subscription a source reads through, core and JetStream publication, the JetStream answer
//!   each record's `PubAck` gives the confirmation window, header mapping in both directions, and
//!   per-record rejection classification.
//! - **Depends on.** The connector contract and its confirmation window, vocabulary values,
//!   `error-stack`, Tokio and `async-nats`.
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

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod scripted_server;
mod source;

use std::{future::Future, pin::Pin, time::Duration};

use async_nats::{
    Client as NatsClient, PublishError as NatsPublishError,
    PublishErrorKind as NatsPublishErrorKind, Subject,
    jetstream::{
        Context as NatsJetStream,
        context::{PublishAckFuture, PublishError as JetStreamPublishError, PublishErrorKind},
        publish::PublishAck,
    },
    message::OutboundMessage,
};
use async_trait::async_trait;
use error_stack::Report;
use futures_util::SinkExt;
use nervix_connector::{
    AckConfirmation, BrokerAnswer, BrokerVerdict, ConfirmationWindow, ParsedRetryPolicy,
    PerRecordOutcome, RecordSink, RejectedSinkRecord, SinkHost, SinkLifecycle, SinkPublishError,
    SinkRecord, SinkRecordId, SinkStartError, SinkStartResult, client_config_value,
    client_tls_paths,
};
use nervix_models::{ClientConfigEntry, SubjectName};
use nervix_primitives::sync::{
    StdArc,
    atomic::{AtomicBool, Ordering},
};
pub use source::{
    NatsMessageHeaders, NatsSource, NatsSourceError, NatsSourceMessage, NatsSourcePlan,
};

const NATS: &str = "nats";

/// Whether a NATS sink publishes through core NATS or waits for JetStream to store each record.
///
/// A NATS sink publishes through its own client even without an acknowledgement, so `MODE NO_ACK`
/// decides [`NatsPublishingMode::Core`] rather than a broker mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NatsPublishingMode {
    Core,
    JetStream(AckConfirmation),
}

/// What one NATS sink publishes through: its client entries, subject, delivery and reconnect
/// backoff.
pub struct NatsSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub subject: SubjectName,
    pub mode: NatsPublishingMode,
    pub retry_policy: ParsedRetryPolicy,
}

pub struct NatsSink {
    client: NatsClient,
    delivery: NatsDelivery,
    subject: Subject,
}

/// How this sink publishes, together with whatever that way of publishing needs.
///
/// A JetStream context exists only for `MODE ACK`, so pairing the context with the mode that
/// requires it means the publish path reads one value instead of matching a mode and then hoping
/// the separately stored context agrees with it.
enum NatsDelivery {
    Core,
    JetStream {
        context: Box<NatsJetStream>,
        confirmation: AckConfirmation,
    },
}

/// JetStream's answer for one record, which resolves when the record's `PubAck` arrives or the
/// client gives the record up.
type NatsAnswer = Pin<Box<dyn Future<Output = BrokerAnswer> + Send>>;

impl NatsDelivery {
    /// How a sink publishes through `client` in `mode`. JetStream gets a context that awaits each
    /// record's `PubAck` for at most the declared `ACK TIMEOUT` and holds at most `MAX` records
    /// awaiting one.
    fn new(mode: NatsPublishingMode, client: &NatsClient) -> Self {
        match mode {
            NatsPublishingMode::Core => Self::Core,
            NatsPublishingMode::JetStream(confirmation) => Self::JetStream {
                context: Box::new(
                    async_nats::jetstream::ContextBuilder::new()
                        .timeout(confirmation.timeout)
                        .ack_timeout(confirmation.timeout)
                        .max_ack_inflight(confirmation.max_in_flight.get())
                        .backpressure_on_inflight(true)
                        .build(client.clone()),
                ),
                confirmation,
            },
        }
    }
}

impl NatsSink {
    pub async fn new(config: NatsSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let client = Self::client_from_config(&config.config, config.retry_policy).await?;
        let delivery = NatsDelivery::new(config.mode, &client);
        Ok(Self {
            client,
            delivery,
            subject: Subject::from(config.subject.as_str().to_string()),
        })
    }

    async fn client_from_config(
        config: &[ClientConfigEntry],
        retry_policy: ParsedRetryPolicy,
    ) -> SinkStartResult<NatsClient> {
        let addr = Self::config_value(config, "addr")?;
        let connected_once = StdArc::new(AtomicBool::new(false));
        let event_connected_once = connected_once.clone();
        let delay_connected_once = connected_once;
        let mut options = async_nats::ConnectOptions::new()
            .event_callback(move |event| {
                let connected_once = event_connected_once.clone();
                async move {
                    if let async_nats::Event::Connected = event {
                        connected_once.store(true, Ordering::Relaxed);
                    }
                }
            })
            .reconnect_delay_callback(move |attempts| {
                Self::connection_delay(
                    retry_policy,
                    attempts,
                    delay_connected_once.load(Ordering::Relaxed),
                )
            });
        let tls = client_tls_paths(config);
        if let Some(ca_file) = tls.ca_file.as_ref() {
            options = options.add_root_certificates(ca_file.clone());
        }
        match (&tls.cert_file, &tls.key_file) {
            (Some(cert_file), Some(key_file)) => {
                options = options.add_client_certificate(cert_file.clone(), key_file.clone());
            }
            (None, None) => {}
            _ => {
                return Err(Self::config_error(
                    "NATS TLS client authentication requires both 'tls_cert_file' and \
                     'tls_key_file'",
                ));
            }
        }
        options.connect(addr).await.map_err(Self::start_error)
    }

    fn connection_delay(
        policy: ParsedRetryPolicy,
        attempts: usize,
        connected_once: bool,
    ) -> Duration {
        // The client counts reconnect attempts from one. The first attempt after a connection
        // that never succeeded retries immediately; from the first delayed attempt onward each
        // further attempt doubles the configured backoff.
        let first_delayed_attempt = if connected_once { 1 } else { 2 };
        let Some(retries) = attempts.checked_sub(first_delayed_attempt) else {
            return Duration::ZERO;
        };
        let mut delay = policy.backoff;
        for _ in 0..retries {
            if delay >= policy.max_backoff {
                return policy.max_backoff;
            }
            // Saturation is the policy here: the backoff doubles until it reaches the configured
            // ceiling and stays there, so a doubling that leaves `Duration` clamps to it too.
            let doubled = nervix_primitives::expect_lint!(
                nervix::clamped_arithmetic,
                "doubling past Duration MAX must remain at the configured retry ceiling",
                delay.saturating_mul(2)
            );
            delay = doubled.min(policy.max_backoff);
        }
        delay
    }

    async fn publish_core(&self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let mut sink = self.client.clone();
        let mut queued = Vec::with_capacity(records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let headers = if record.headers.is_empty() {
                None
            } else {
                Some(Self::header_map(&record.headers))
            };
            let result = sink
                .feed(OutboundMessage {
                    subject: self.subject.clone(),
                    reply: None,
                    payload: record.payload.into(),
                    headers,
                })
                .await;
            match result {
                Ok(()) => queued.push(record_id),
                Err(error) if Self::is_core_record_rejection(&error) => {
                    outcome.reject(RejectedSinkRecord::external(
                        record_id,
                        occurred_at,
                        format!("nats rejected record: {error}"),
                    ));
                }
                Err(error) => {
                    outcome.fail(Self::publish_error(error));
                    return outcome;
                }
            }
        }
        match self.client.flush().await {
            Ok(()) => {
                for record_id in queued {
                    outcome.deliver(record_id);
                }
            }
            Err(error) => outcome.fail(Self::publish_error(error)),
        }
        outcome
    }

    /// Publishes `records` to JetStream and awaits each record's `PubAck` through the
    /// confirmation window `settings` declare.
    ///
    /// Every way the publish can fail ends it through the window, which first settles each answer
    /// JetStream already gave, so the host's retry never resends a record the stream confirmed.
    async fn publish_jetstream(
        &self,
        jetstream: &NatsJetStream,
        settings: AckConfirmation,
        records: Vec<SinkRecord>,
    ) -> PerRecordOutcome<SinkRecordId> {
        let mut window: ConfirmationWindow<NatsAnswer> =
            ConfirmationWindow::new(NATS, settings, records.len());
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            let handed_over = if record.headers.is_empty() {
                jetstream
                    .publish(self.subject.clone(), record.payload.into())
                    .await
            } else {
                jetstream
                    .publish_with_headers(
                        self.subject.clone(),
                        Self::header_map(&record.headers),
                        record.payload.into(),
                    )
                    .await
            };
            let pending = match handed_over {
                Ok(pending) => pending,
                Err(error) if Self::is_jetstream_record_rejection(&error) => {
                    window.reject(RejectedSinkRecord::external(
                        record_id,
                        occurred_at,
                        format!("nats JetStream rejected record: {error}"),
                    ));
                    continue;
                }
                Err(error) => return window.fail(Self::refused_hand_over(error)),
            };
            let answer: NatsAnswer = Box::pin(Self::await_answer(pending));
            if let Err(failure) = window.admit(record_id, occurred_at, answer).await {
                return window.fail(failure);
            }
        }
        window.finish().await
    }

    /// Waits for the `PubAck` of the record `pending` tracks and reads JetStream's answer from it.
    async fn await_answer(pending: PublishAckFuture) -> BrokerAnswer {
        let acknowledgement = pending.await;
        Self::broker_answer(acknowledgement)
    }

    /// JetStream's answer for a record whose wait for its `PubAck` ended as `acknowledgement`.
    ///
    /// A `PubAck` confirms the record, and a message above the server's `max_payload` is refused
    /// for good. Any other ending leaves the record's outcome unknown: no stream captures the
    /// subject, the `PubAck` did not arrive, or the client or its connection failed.
    fn broker_answer(acknowledgement: Result<PublishAck, JetStreamPublishError>) -> BrokerAnswer {
        match acknowledgement {
            Ok(_ack) => Ok(BrokerVerdict::Confirmed),
            Err(error) if Self::is_jetstream_record_rejection(&error) => {
                Ok(BrokerVerdict::Rejected {
                    message: format!("nats JetStream rejected record: {error}"),
                })
            }
            Err(error) => {
                let description = format!("nats JetStream publish failed: {error}");
                Err(Report::new(error)
                    .change_context(SinkPublishError::Publish { sink: NATS })
                    .attach_printable(description))
            }
        }
    }

    /// The failure that ends a publish whose record the client did not take for a reason that is
    /// not the record's own, such as a client that could not queue it within `ACK TIMEOUT`.
    fn refused_hand_over(error: JetStreamPublishError) -> Report<SinkPublishError> {
        let description = format!("nats JetStream client did not take the record: {error}");
        Report::new(error)
            .change_context(SinkPublishError::Publish { sink: NATS })
            .attach_printable(description)
    }

    fn header_map(headers: &[(String, String)]) -> async_nats::HeaderMap {
        let mut header_map = async_nats::HeaderMap::new();
        for (name, value) in headers {
            header_map.append(name.as_str(), value.as_str());
        }
        header_map
    }

    fn is_core_record_rejection(error: &NatsPublishError) -> bool {
        matches!(error.kind(), NatsPublishErrorKind::MaxPayloadExceeded)
    }

    fn is_jetstream_record_rejection(error: &JetStreamPublishError) -> bool {
        matches!(error.kind(), PublishErrorKind::MaxPayloadExceeded)
    }

    #[cfg(test)]
    fn jetstream_error_is_missing_stream(error: &JetStreamPublishError) -> bool {
        matches!(error.kind(), PublishErrorKind::StreamNotFound)
    }

    fn config_value(config: &[ClientConfigEntry], key: &str) -> SinkStartResult<String> {
        {
            let mapping_input = client_config_value(config, key, "NATS");
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let message = error.current_context().to_string();
                    error
                        .change_context(SinkStartError::InvalidConfiguration { sink: NATS })
                        .attach_printable(message)
                }),
            }
        }
    }

    fn config_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::InvalidConfiguration { sink: NATS })
            .attach_printable(error.to_string())
    }

    fn start_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::Initialize { sink: NATS }).attach_printable(error.to_string())
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "formats the typed external NATS driver failure; no internal runtime \
                      ownership is inferred"
        )
    )]
    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: NATS }).attach_printable(error.to_string())
    }
}

#[async_trait]
impl SinkLifecycle for NatsSink {
    /// A publish failure leaves the client connected: it reconnects on its own schedule, and
    /// dropping it would discard the session the broker still holds.
    fn keeps_client_on_publish_failure(&self) -> bool {
        true
    }
}

#[async_trait]
impl RecordSink for NatsSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId> {
        match &self.delivery {
            NatsDelivery::Core => self.publish_core(records).await,
            NatsDelivery::JetStream {
                context,
                confirmation,
            } => {
                self.publish_jetstream(context, *confirmation, records)
                    .await
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::num::NonZeroUsize;

    use nervix_models::Timestamp;

    use super::*;
    use crate::scripted_server::ScriptedServer;

    /// A JetStream sink publishing to subject `events` through the server at `addr`, with at most
    /// `max_in_flight` records awaiting their `PubAck` at once.
    async fn jetstream_sink(addr: &str, max_in_flight: usize) -> NatsSink {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        };
        let config = [ClientConfigEntry {
            key: "addr".to_string(),
            value: addr.to_string(),
        }];
        let client = NatsSink::client_from_config(&config, policy)
            .await
            .expect("the sink connects to the scripted server");
        let confirmation = AckConfirmation {
            max_in_flight: NonZeroUsize::new(max_in_flight).expect("a window holds a record"),
            timeout: Duration::from_secs(30),
        };
        NatsSink {
            delivery: NatsDelivery::new(NatsPublishingMode::JetStream(confirmation), &client),
            client,
            subject: Subject::from("events"),
        }
    }

    fn record(index: usize, payload: &[u8], headers: &[(&str, &str)]) -> SinkRecord {
        let headers = headers
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect();
        SinkRecord::new(
            SinkRecordId::new(index),
            None,
            payload.to_vec(),
            headers,
            Timestamp::from_unix_nanos(0),
        )
    }

    const CONFIRMED: &str = r#"{"stream":"events","seq":1}"#;

    #[nervix_primitives::test]
    async fn jetstream_confirms_each_record_and_refuses_one_above_max_payload() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(64).await;
            let plain = connection.expect_publish().await;
            let tagged = connection.expect_publish().await;
            connection.reply(&plain, CONFIRMED).await;
            connection.reply(&tagged, CONFIRMED).await;
            (plain.message, tagged.message, connection)
        });
        let mut sink = jetstream_sink(&addr, 8).await;

        let outcome = sink
            .publish(vec![
                record(0, b"plain", &[]),
                record(1, &[b'x'; 128], &[]),
                record(2, b"tagged", &[("trace", "a")]),
            ])
            .await
            .into_parts();

        assert_eq!(
            outcome.delivered,
            vec![SinkRecordId::new(0), SinkRecordId::new(2)]
        );
        let rejected = outcome
            .rejected
            .iter()
            .map(|rejected| rejected.id)
            .collect::<Vec<_>>();
        assert_eq!(rejected, vec![SinkRecordId::new(1)]);
        assert!(outcome.infrastructure_error.is_none());
        let (plain, tagged, _connection) = script.await.expect("the scripted server ran");
        assert_eq!(plain, b"plain");
        assert_eq!(tagged, b"NATS/1.0\r\ntrace: a\r\n\r\ntagged");
    }

    #[nervix_primitives::test]
    async fn a_failed_pub_ack_ends_the_publish_after_settling_the_answers_already_given() {
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let mut publishes = Vec::new();
            for _ in 0..4 {
                publishes.push(connection.expect_publish().await);
            }
            // The fourth record's PubAck arrives before the third record's failure, so the
            // window holds a given answer when the failure ends the publish.
            for (index, reply) in [
                (0, CONFIRMED),
                (1, CONFIRMED),
                (3, CONFIRMED),
                (
                    2,
                    r#"{"error":{"code":500,"err_code":10049,"description":"stream store failed"}}"#,
                ),
            ] {
                let publish = publishes
                    .get(index)
                    .expect("the client published four records");
                connection.reply(publish, reply).await;
            }
            connection
        });
        let mut sink = jetstream_sink(&addr, 8).await;

        let outcome = sink
            .publish((0..4).map(|index| record(index, b"event", &[])).collect())
            .await
            .into_parts();

        assert_eq!(
            outcome.delivered,
            vec![
                SinkRecordId::new(0),
                SinkRecordId::new(1),
                SinkRecordId::new(3)
            ],
            "the publish settles every PubAck JetStream gave before the failure"
        );
        assert!(outcome.rejected.is_empty());
        let failure = outcome
            .infrastructure_error
            .expect("the failed PubAck ends the publish");
        let description = printable_description(&failure).expect("the failure describes itself");
        assert!(
            description.starts_with("nats JetStream publish failed: "),
            "{description}"
        );
        let _connection = script.await.expect("the scripted server ran");
    }

    /// JetStream answers every record but the oldest, then fails the oldest's `PubAck`: the
    /// publish settles every `PubAck` it was given, more of them than one cooperative budget lets
    /// a task read, so the host's retry resends none of the records the stream confirmed.
    #[nervix_primitives::test]
    async fn a_failure_settles_more_pub_acks_than_one_cooperative_budget() {
        const RECORDS: usize = 300;
        let server = ScriptedServer::start().await;
        let addr = server.addr();
        let script = nervix_primitives::task::spawn(async move {
            let mut connection = server.accept(1024 * 1024).await;
            let mut publishes = Vec::with_capacity(RECORDS);
            for _ in 0..RECORDS {
                nervix_primitives::task::consume_budget().await;
                publishes.push(connection.expect_publish().await);
            }
            for publish in publishes.iter().skip(1) {
                nervix_primitives::task::consume_budget().await;
                connection.reply(publish, CONFIRMED).await;
            }
            let oldest = publishes
                .first()
                .expect("the client published every record");
            connection
                .reply(
                    oldest,
                    r#"{"error":{"code":500,"err_code":10049,"description":"stream store failed"}}"#,
                )
                .await;
            connection
        });
        let mut sink = jetstream_sink(&addr, 1024).await;

        let outcome = sink
            .publish(
                (0..RECORDS)
                    .map(|index| record(index, b"event", &[]))
                    .collect(),
            )
            .await
            .into_parts();

        let confirmed = (1..RECORDS).map(SinkRecordId::new).collect::<Vec<_>>();
        assert_eq!(
            outcome.delivered.len(),
            confirmed.len(),
            "every PubAck JetStream gave is settled"
        );
        assert_eq!(outcome.delivered, confirmed);
        assert!(outcome.rejected.is_empty());
        assert!(
            outcome.infrastructure_error.is_some(),
            "the oldest record's failed PubAck ends the publish"
        );
        let _connection = script.await.expect("the scripted server ran");
    }

    #[test]
    fn only_core_max_payload_is_a_record_rejection() {
        let oversized = NatsPublishError::new(NatsPublishErrorKind::MaxPayloadExceeded);
        let invalid_subject = NatsPublishError::new(NatsPublishErrorKind::InvalidSubject);

        assert!(NatsSink::is_core_record_rejection(&oversized));
        assert!(!NatsSink::is_core_record_rejection(&invalid_subject));
    }

    #[test]
    fn missing_jetstream_is_infrastructure_not_record_rejection() {
        let error = JetStreamPublishError::new(PublishErrorKind::StreamNotFound);
        assert!(NatsSink::jetstream_error_is_missing_stream(&error));
        assert!(!NatsSink::is_jetstream_record_rejection(&error));
    }

    #[test]
    fn oversized_jetstream_payload_is_a_record_rejection() {
        let error = JetStreamPublishError::new(PublishErrorKind::MaxPayloadExceeded);
        assert!(NatsSink::is_jetstream_record_rejection(&error));
        assert!(!NatsSink::jetstream_error_is_missing_stream(&error));
    }

    /// The first printable attachment of `failure`: the description an emitter shows as its
    /// transient error.
    fn printable_description(failure: &Report<SinkPublishError>) -> Option<String> {
        for frame in failure.frames() {
            if let error_stack::FrameKind::Attachment(error_stack::AttachmentKind::Printable(
                attachment,
            )) = frame.kind()
            {
                return Some(attachment.to_string());
            }
        }
        None
    }

    #[test]
    fn a_pub_ack_confirms_the_record() {
        let ack = serde_json::from_str::<PublishAck>(r#"{"stream":"events","seq":7}"#)
            .expect("a PubAck names its stream and sequence");
        let answer = NatsSink::broker_answer(Ok(ack));
        assert!(matches!(answer, Ok(BrokerVerdict::Confirmed)));
    }

    #[test]
    fn an_oversized_message_is_refused_for_good() {
        let error = JetStreamPublishError::new(PublishErrorKind::MaxPayloadExceeded);
        let answer = NatsSink::broker_answer(Err(error));
        let Ok(BrokerVerdict::Rejected { message }) = answer else {
            panic!("a message above max_payload is refused for good");
        };
        assert_eq!(
            message,
            "nats JetStream rejected record: max payload size exceeded"
        );
    }

    #[test]
    fn a_failed_pub_ack_leaves_the_record_unresolved_with_its_typed_cause() {
        let error = JetStreamPublishError::new(PublishErrorKind::StreamNotFound);
        let Err(failure) = NatsSink::broker_answer(Err(error)) else {
            panic!("a missing stream leaves the record's outcome unknown");
        };
        assert!(matches!(
            failure.current_context(),
            SinkPublishError::Publish { sink: NATS }
        ));
        let cause = failure
            .downcast_ref::<JetStreamPublishError>()
            .expect("the failure keeps the driver's typed cause");
        assert_eq!(cause.kind(), PublishErrorKind::StreamNotFound);
        assert_eq!(
            printable_description(&failure).as_deref(),
            Some("nats JetStream publish failed: no stream found for given subject")
        );
    }

    #[test]
    fn a_refused_hand_over_describes_the_client_failure() {
        let failure =
            NatsSink::refused_hand_over(JetStreamPublishError::new(PublishErrorKind::TimedOut));
        assert!(matches!(
            failure.current_context(),
            SinkPublishError::Publish { sink: NATS }
        ));
        let cause = failure
            .downcast_ref::<JetStreamPublishError>()
            .expect("the failure keeps the driver's typed cause");
        assert_eq!(cause.kind(), PublishErrorKind::TimedOut);
        assert_eq!(
            printable_description(&failure).as_deref(),
            Some(
                "nats JetStream client did not take the record: timed out: didn't receive ack in \
                 time"
            )
        );
    }

    #[test]
    fn connection_delay_uses_declared_exponential_policy() {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(125),
            max_backoff: Duration::from_secs(1),
        };

        assert_eq!(NatsSink::connection_delay(policy, 1, false), Duration::ZERO);
        assert_eq!(
            NatsSink::connection_delay(policy, 2, false),
            Duration::from_millis(125)
        );
        assert_eq!(
            NatsSink::connection_delay(policy, 1, true),
            Duration::from_millis(125)
        );
        assert_eq!(
            NatsSink::connection_delay(policy, 2, true),
            Duration::from_millis(250)
        );
        assert_eq!(
            NatsSink::connection_delay(policy, 4, true),
            Duration::from_secs(1)
        );
        assert_eq!(
            NatsSink::connection_delay(policy, 50, true),
            Duration::from_secs(1)
        );
    }
}
