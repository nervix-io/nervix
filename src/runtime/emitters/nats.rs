//! NATS sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The NATS connection a client configures, its reconnect delay, core and JetStream
//!   publication, header mapping, and per-record rejection classification.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio and
//!   `async-nats`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_nats::{
    Client as NatsClient, PublishError as NatsPublishError,
    PublishErrorKind as NatsPublishErrorKind, Subject,
    jetstream::{
        Context as NatsJetStream,
        context::{PublishError as JetStreamPublishError, PublishErrorKind},
        publish::PublishAck,
    },
    message::OutboundMessage,
};
use async_trait::async_trait;
use error_stack::Report;
use futures_util::{FutureExt, SinkExt};
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, ParsedRetryPolicy, PerRecordOutcome, RecordSink, RejectedSinkRecord, SinkHost,
    SinkLifecycle, SinkPublishError, SinkPublishResult, SinkRecord, SinkRecordPosition,
    SinkStartError, SinkStartResult, client_config_value, client_tls_paths,
};
use nervix_models::{ClientConfigEntry, SubjectName, Timestamp};
use tokio::time::{Instant, sleep};

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

type NatsConfirmation =
    Pin<Box<dyn Future<Output = Result<PublishAck, JetStreamPublishError>> + Send>>;

struct PendingNatsConfirmation {
    position: SinkRecordPosition,
    occurred_at: Timestamp,
    deadline: Instant,
    confirmation: NatsConfirmation,
}

impl NatsSink {
    pub async fn new(config: NatsSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let client = Self::client_from_config(&config.config, config.retry_policy).await?;
        let delivery = match config.mode {
            NatsPublishingMode::Core => NatsDelivery::Core,
            NatsPublishingMode::JetStream(confirmation) => NatsDelivery::JetStream {
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
        };
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
            delay = delay.saturating_mul(2).min(policy.max_backoff);
        }
        delay
    }

    async fn publish_core(&self, records: Vec<SinkRecord>) -> PerRecordOutcome {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let mut sink = self.client.clone();
        let mut queued = Vec::with_capacity(records.len());
        for record in records {
            tokio::task::consume_budget().await;
            let position = record.position;
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
                Ok(()) => queued.push(position),
                Err(error) if Self::is_core_record_rejection(&error) => {
                    outcome.reject(RejectedSinkRecord::external(
                        position,
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
                for position in queued {
                    outcome.deliver(position);
                }
            }
            Err(error) => outcome.fail(Self::publish_error(error)),
        }
        outcome
    }

    async fn publish_jetstream(
        &self,
        jetstream: &NatsJetStream,
        AckConfirmation {
            max_in_flight,
            timeout,
        }: AckConfirmation,
        records: Vec<SinkRecord>,
    ) -> PerRecordOutcome {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let mut pending: VecDeque<PendingNatsConfirmation> = VecDeque::new();
        for record in records {
            tokio::task::consume_budget().await;
            let position = record.position;
            let occurred_at = record.occurred_at;
            let confirmation = if record.headers.is_empty() {
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
            let confirmation = match confirmation {
                Ok(confirmation) => confirmation,
                Err(error) if Self::is_jetstream_record_rejection(&error) => {
                    outcome.reject(RejectedSinkRecord::external(
                        position,
                        occurred_at,
                        format!("nats JetStream rejected record: {error}"),
                    ));
                    continue;
                }
                Err(error) => {
                    outcome.fail(Self::publish_error(error));
                    return outcome;
                }
            };
            let Some(deadline) = Instant::now().checked_add(timeout) else {
                outcome.fail(Self::publish_error(
                    "nats ACK TIMEOUT exceeds the monotonic clock range",
                ));
                return outcome;
            };
            pending.push_back(PendingNatsConfirmation {
                position,
                occurred_at,
                deadline,
                confirmation: Box::pin(confirmation.into_future()),
            });
            if pending.len() >= max_in_flight.get()
                && let Err(error) = Self::confirm_oldest(&mut pending, timeout, &mut outcome).await
            {
                outcome.fail(error);
                return outcome;
            }
        }
        while !pending.is_empty() {
            tokio::task::consume_budget().await;
            if let Err(error) = Self::confirm_oldest(&mut pending, timeout, &mut outcome).await {
                outcome.fail(error);
                return outcome;
            }
        }
        outcome
    }

    async fn confirm_oldest(
        pending: &mut VecDeque<PendingNatsConfirmation>,
        timeout: Duration,
        outcome: &mut PerRecordOutcome,
    ) -> SinkPublishResult<()> {
        let Some(oldest) = pending.front_mut() else {
            return Err(Self::publish_error(
                "NATS JetStream acknowledgment window unexpectedly became empty",
            ));
        };
        let remaining = oldest
            .deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            Self::harvest_ready_after_oldest_failure(pending, outcome);
            return Err(Self::confirm_timeout_error(timeout));
        }
        let result = tokio::select! {
            biased;
            result = &mut oldest.confirmation => Some(result),
            _ = sleep(remaining) => None,
        };
        let Some(result) = result else {
            Self::harvest_ready_after_oldest_failure(pending, outcome);
            return Err(Self::confirm_timeout_error(timeout));
        };
        let position = oldest.position;
        let occurred_at = oldest.occurred_at;
        match result {
            Ok(_ack) => {
                pending.pop_front();
                outcome.deliver(position);
                Ok(())
            }
            Err(error) if Self::is_jetstream_record_rejection(&error) => {
                pending.pop_front();
                outcome.reject(RejectedSinkRecord::external(
                    position,
                    occurred_at,
                    format!("nats JetStream rejected record: {error}"),
                ));
                Ok(())
            }
            Err(error) => {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                Err(Self::publish_error(error))
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
        pending: &mut VecDeque<PendingNatsConfirmation>,
        outcome: &mut PerRecordOutcome,
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
            match result {
                Ok(_ack) => outcome.deliver(confirmation.position),
                Err(error) if Self::is_jetstream_record_rejection(&error) => {
                    outcome.reject(RejectedSinkRecord::external(
                        confirmation.position,
                        confirmation.occurred_at,
                        format!("nats JetStream rejected record: {error}"),
                    ));
                }
                Err(_) => {}
            }
        }
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

    fn confirm_timeout_error(timeout: Duration) -> Report<SinkPublishError> {
        Self::publish_error(format!(
            "NATS JetStream PubAck exceeded ACK TIMEOUT {}",
            humantime::format_duration(timeout)
        ))
    }

    fn config_value(config: &[ClientConfigEntry], key: &str) -> SinkStartResult<String> {
        client_config_value(config, key, "NATS").map_err(|error| {
            let message = error.current_context().to_string();
            error
                .change_context(SinkStartError::InvalidConfiguration { sink: NATS })
                .attach_printable(message)
        })
    }

    fn config_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::InvalidConfiguration { sink: NATS })
            .attach_printable(error.to_string())
    }

    fn start_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::Initialize { sink: NATS }).attach_printable(error.to_string())
    }

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
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome {
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
mod tests {
    use super::*;

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
