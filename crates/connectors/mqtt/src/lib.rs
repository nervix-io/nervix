//! MQTT source and sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The MQTT client a configuration declares, its event loop and reconnect backoff,
//!   the session, quality of service and shared subscription a source reads through, manual
//!   acknowledgement and local replay of publishes, the quality of service each record is
//!   published at, the Maximum Packet Size the broker declares and the rejection of a record whose
//!   PUBLISH packet would exceed it, and confirmation classification.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio and
//!   `rumqttc`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

mod source;

use std::{
    collections::VecDeque,
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    pin::Pin,
    time::Duration,
};

use arch_into::ArchInto as _;
use async_trait::async_trait;
use error_stack::Report;
use futures_util::FutureExt;
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, ParsedRetryPolicy, PerRecordOutcome, RecordSink, RejectedSinkRecord, SinkHost,
    SinkLifecycle, SinkPublishError, SinkPublishResult, SinkRecord, SinkRecordId, SinkStartError,
    SinkStartResult, client_config_value, client_tls_paths, next_retry_delay,
    optional_client_config_value, read_tls_file,
};
use nervix_models::{ClientConfigEntry, Timestamp, TopicName};
use nervix_primitives::sync::{
    atomic::{AtomicU32, Ordering},
    watch,
};
use rumqttc::{
    AsyncClient, ClientError as MqttClientError, ConnAck, Event, Incoming, MqttOptions,
    PubAckReason as MqttPubAckReason, PubRecReason as MqttPubRecReason, PublishNoticeError,
    PublishOptions, SessionMode, TlsConfiguration, Transport as MqttTransport, ValidatedTopic,
};
pub use source::{
    MqttClientIdError, MqttSource, MqttSourceError, MqttSourceMessage, MqttSourcePlan,
    MqttSourcePosition, MqttSourceSettings,
};
use thiserror::Error;
use tokio::time::{Instant, sleep};
use tracing::warn;
use triomphe::Arc;
use url::{Host, Url};

const MQTT: &str = "mqtt";

/// The largest Remaining Length an MQTT packet can declare. Its variable byte integer holds at most
/// four bytes of seven value bits each.
const MQTT_MAX_REMAINING_LENGTH: usize = 268_435_455;

/// The quality of service an MQTT sink publishes at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttPublishingMode {
    Qos0,
    Qos1(AckConfirmation),
    Qos2(AckConfirmation),
}

impl MqttPublishingMode {
    fn publish_options(self) -> PublishOptions {
        match self {
            Self::Qos0 => PublishOptions::at_most_once(),
            Self::Qos1(_) => PublishOptions::at_least_once(),
            Self::Qos2(_) => PublishOptions::exactly_once(),
        }
    }

    fn confirmation_settings(self) -> Option<AckConfirmation> {
        match self {
            Self::Qos0 => None,
            Self::Qos1(confirmation) | Self::Qos2(confirmation) => Some(confirmation),
        }
    }
}

/// What one MQTT sink publishes through: its client entries, topic, quality of service, the
/// backoff its event loop reconnects on, and the client id to use when the configuration
/// declares none.
pub struct MqttSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub topic: TopicName,
    pub mode: MqttPublishingMode,
    pub retry_policy: ParsedRetryPolicy,
    pub default_client_id: String,
}

pub struct MqttSink {
    client: AsyncClient,
    topic: TopicName,
    mode: MqttPublishingMode,
    framing: MqttPublishFraming,
    broker_limit: Arc<MqttBrokerPacketLimit>,
    eventloop_shutdown: watch::Sender<bool>,
}

/// Why one record is not a PUBLISH packet this sink can send.
#[derive(Debug, PartialEq, Eq, Error)]
enum MqttRecordError {
    #[error(
        "MQTT PUBLISH packet of {packet_bytes} bytes exceeds the broker's maximum packet size of \
         {maximum} bytes"
    )]
    PacketTooLarge {
        packet_bytes: usize,
        maximum: NonZeroU32,
    },
    #[error(
        "MQTT PUBLISH payload of {payload_bytes} bytes does not fit the largest packet the \
         protocol can express"
    )]
    BeyondProtocol { payload_bytes: usize },
}

type MqttRecordResult<T> = Result<T, Report<MqttRecordError>>;

/// What the client writes around every payload this sink publishes, which its topic and quality
/// of service fix for the sink's lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MqttPublishFraming {
    /// The variable header in front of the payload: the topic's two-byte length prefix and its
    /// bytes, the packet identifier QoS 1 and 2 carry, and the one-byte length of the empty
    /// property section.
    variable_header_bytes: usize,
}

impl MqttPublishFraming {
    fn new(topic: &TopicName, mode: MqttPublishingMode) -> Self {
        const TOPIC_LENGTH_PREFIX_BYTES: usize = 2;
        const PROPERTY_LENGTH_BYTES: usize = 1;
        const IN_MEMORY: &str = "a topic name and a few framing bytes fit in usize";
        let packet_identifier_bytes = match mode {
            MqttPublishingMode::Qos0 => 0,
            MqttPublishingMode::Qos1(_) | MqttPublishingMode::Qos2(_) => 2,
        };
        let topic_bytes = TOPIC_LENGTH_PREFIX_BYTES
            .checked_add(topic.as_str().len())
            .assured(IN_MEMORY);
        let identified_bytes = topic_bytes
            .checked_add(packet_identifier_bytes)
            .assured(IN_MEMORY);
        let variable_header_bytes = identified_bytes
            .checked_add(PROPERTY_LENGTH_BYTES)
            .assured(IN_MEMORY);
        Self {
            variable_header_bytes,
        }
    }

    /// The size of the whole PUBLISH packet the client writes for a payload of `payload_bytes`:
    /// the fixed header byte, the Remaining Length, the variable header and the payload.
    fn packet_bytes(self, payload_bytes: usize) -> MqttRecordResult<usize> {
        const BOUNDED: &str = "the remaining length was checked against the MQTT maximum above";
        let Some(remaining_bytes) = self.variable_header_bytes.checked_add(payload_bytes) else {
            return Err(Report::new(MqttRecordError::BeyondProtocol {
                payload_bytes,
            }));
        };
        if remaining_bytes > MQTT_MAX_REMAINING_LENGTH {
            return Err(Report::new(MqttRecordError::BeyondProtocol {
                payload_bytes,
            }));
        }
        let length_bytes: usize = match remaining_bytes {
            0..=127 => 1,
            128..=16_383 => 2,
            16_384..=2_097_151 => 3,
            _ => 4,
        };
        let fixed_header_bytes = length_bytes.checked_add(1).verified(BOUNDED);
        let packet_bytes = remaining_bytes
            .checked_add(fixed_header_bytes)
            .verified(BOUNDED);
        Ok(packet_bytes)
    }
}

/// The Maximum Packet Size the broker declared in the CONNACK of the client's current connection.
///
/// The broker declares it on every connection, and a reconnect may change or withdraw it, so the
/// event loop replaces it with each CONNACK and a publish reads the latest one. The atomic holds
/// the declared size, or zero while the broker has declared none: MQTT forbids a Maximum Packet
/// Size of zero, so zero never stands for a declared value. It is a single independent value, so
/// relaxed ordering is its whole contract.
#[derive(Debug, Default)]
struct MqttBrokerPacketLimit(AtomicU32);

impl MqttBrokerPacketLimit {
    /// The value the atomic holds while the broker has declared no maximum.
    const UNDECLARED: u32 = 0;

    fn declare(&self, connack: &ConnAck) {
        let declared = match &connack.properties {
            Some(properties) => properties.max_packet_size,
            None => None,
        };
        self.0
            .store(declared.unwrap_or(Self::UNDECLARED), Ordering::Relaxed);
    }

    fn declared(&self) -> Option<NonZeroU32> {
        NonZeroU32::new(self.0.load(Ordering::Relaxed))
    }

    /// Accepts a PUBLISH packet of `packet_bytes` bytes, or names the declared maximum it exceeds.
    fn admit(&self, packet_bytes: usize) -> MqttRecordResult<()> {
        let Some(maximum) = self.declared() else {
            return Ok(());
        };
        let maximum_bytes: usize = maximum.get().arch_into();
        if packet_bytes <= maximum_bytes {
            return Ok(());
        }
        Err(Report::new(MqttRecordError::PacketTooLarge {
            packet_bytes,
            maximum,
        }))
    }
}

type MqttConfirmation = Pin<Box<dyn Future<Output = Result<(), PublishNoticeError>> + Send>>;

struct PendingMqttConfirmation {
    record: SinkRecordId,
    occurred_at: Timestamp,
    deadline: Instant,
    confirmation: MqttConfirmation,
}

/// The delay before the event loop's next reconnect attempt, doubling up to the declared ceiling.
struct MqttReconnectBackoff {
    policy: ParsedRetryPolicy,
    next: Duration,
}

impl MqttReconnectBackoff {
    fn from_policy(policy: ParsedRetryPolicy) -> Self {
        Self {
            policy,
            next: policy.backoff,
        }
    }

    fn reset(&mut self) {
        self.next = self.policy.backoff;
    }

    fn take_next_delay(&mut self) -> Duration {
        let delay = self.next;
        self.next = next_retry_delay(self.next, self.policy);
        delay
    }
}

struct MqttSinkAddr {
    host: String,
    port: u16,
    tls: bool,
}

impl MqttSink {
    pub fn new(config: MqttSinkConfig, host: SinkHost) -> SinkStartResult<Self> {
        let mode = config.mode;
        let (client, mut eventloop) =
            Self::client_from_config(&config.config, &config.default_client_id, mode)?;
        let retry_policy = config.retry_policy;
        let broker_limit = Arc::new(MqttBrokerPacketLimit::default());
        let declared_limit = broker_limit.clone();
        let (eventloop_shutdown, mut eventloop_shutdown_rx) = watch::channel(false);
        nervix_primitives::task::spawn(async move {
            let mut backoff = MqttReconnectBackoff::from_policy(retry_policy);
            loop {
                nervix_primitives::task::consume_budget().await;
                let polled = nervix_primitives::select! {
                    changed = eventloop_shutdown_rx.changed() => {
                        if changed.is_err() || *eventloop_shutdown_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    polled = eventloop.poll() => polled,
                };
                match polled {
                    Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                        declared_limit.declare(&connack);
                        backoff.reset();
                        host.clear_transient_error();
                    }
                    Ok(Event::Incoming(_) | Event::Outgoing(_) | Event::Auth(_)) => {
                        backoff.reset();
                        host.clear_transient_error();
                    }
                    Err(error) => {
                        let wait = backoff.take_next_delay();
                        host.record_transient_error(error.to_string(), wait);
                        host.report_error(format!(
                            "mqtt event loop failed; reconnecting in {}: {}",
                            humantime::format_duration(wait),
                            error
                        ));
                        warn!(
                            error = %error,
                            retry_in = %humantime::format_duration(wait),
                            "mqtt sink event loop reconnecting"
                        );
                        nervix_primitives::select! {
                            changed = eventloop_shutdown_rx.changed() => {
                                if changed.is_err() || *eventloop_shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            _ = sleep(wait) => {}
                        }
                    }
                }
            }
        });
        ValidatedTopic::new(config.topic.as_str()).map_err(Self::config_error)?;
        let framing = MqttPublishFraming::new(&config.topic, mode);
        Ok(Self {
            client,
            topic: config.topic,
            mode,
            framing,
            broker_limit,
            eventloop_shutdown,
        })
    }

    fn client_from_config(
        config: &[ClientConfigEntry],
        default_client_id: &str,
        mode: MqttPublishingMode,
    ) -> SinkStartResult<(AsyncClient, rumqttc::EventLoop)> {
        let addr = Self::config_value(config, "addr")?;
        let client_id = match optional_client_config_value(config, "client_id") {
            Some(client_id) => client_id.to_owned(),
            None => default_client_id.to_string(),
        };

        let mqtt_addr = Self::parse_addr(&addr)?;
        let mut options = MqttOptions::new(client_id, (mqtt_addr.host, mqtt_addr.port));
        options.set_session_mode(match mode {
            MqttPublishingMode::Qos0 => SessionMode::Clean,
            MqttPublishingMode::Qos1 { .. } | MqttPublishingMode::Qos2 { .. } => {
                SessionMode::Persistent
            }
        });
        if mqtt_addr.tls {
            let tls = client_tls_paths(config);
            let ca = if let Some(ca_file) = tls.ca_file.as_ref() {
                Self::read_tls_file(ca_file, "TLS CA certificate")?
            } else {
                return Err(Self::config_error(
                    "MQTT TLS requires client config key 'tls_ca_file'",
                ));
            };
            let client_auth = match (&tls.cert_file, &tls.key_file) {
                (Some(cert_file), Some(key_file)) => Some((
                    Self::read_tls_file(cert_file, "TLS certificate")?,
                    Self::read_tls_file(key_file, "TLS private key")?,
                )),
                (None, None) => None,
                _ => {
                    return Err(Self::config_error(
                        "MQTT TLS client authentication requires both 'tls_cert_file' and \
                         'tls_key_file'",
                    ));
                }
            };
            options.set_transport(MqttTransport::Tls(TlsConfiguration::Simple {
                ca,
                alpn: None,
                client_auth,
            }));
        }
        let request_capacity = match mode {
            MqttPublishingMode::Qos0 => NonZeroUsize::MIN,
            MqttPublishingMode::Qos1(confirmation) | MqttPublishingMode::Qos2(confirmation) => {
                confirmation.max_in_flight
            }
        };
        AsyncClient::builder(options)
            .capacity(request_capacity.get())
            .try_build()
            .map_err(|error| Self::config_error(format!("invalid MQTT client config: {error}")))
    }

    fn parse_addr(addr: &str) -> SinkStartResult<MqttSinkAddr> {
        let url = Url::parse(addr).map_err(|source| {
            Self::config_error(format!("invalid MQTT addr '{addr}': {source}"))
        })?;
        let tls = if url.scheme() == "mqtt" {
            false
        } else if url.scheme() == "mqtts" {
            true
        } else {
            return Err(Self::config_error(format!(
                "unsupported MQTT addr scheme '{}', expected mqtt:// or mqtts://",
                url.scheme()
            )));
        };
        let host = match url.host() {
            Some(Host::Domain(domain)) => domain.to_string(),
            Some(Host::Ipv4(address)) => address.to_string(),
            Some(Host::Ipv6(address)) => address.to_string(),
            None => String::new(),
        };
        if host.is_empty() {
            return Err(Self::config_error(format!(
                "missing host in MQTT addr '{addr}'"
            )));
        }
        let port = url
            .port()
            .ok_or_else(|| Self::config_error(format!("missing port in MQTT addr '{addr}'")))?;
        Ok(MqttSinkAddr { host, port, tls })
    }

    async fn confirm_oldest(
        pending: &mut VecDeque<PendingMqttConfirmation>,
        timeout: Duration,
        outcome: &mut PerRecordOutcome<SinkRecordId>,
    ) -> SinkPublishResult<()> {
        let Some(oldest) = pending.front_mut() else {
            return Err(Self::publish_error(
                "mqtt acknowledgment window unexpectedly became empty",
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
        let result = nervix_primitives::select! {
            biased;
            result = &mut oldest.confirmation => Some(result),
            _ = sleep(remaining) => None,
        };
        let Some(result) = result else {
            Self::harvest_ready_after_oldest_failure(pending, outcome);
            return Err(Self::confirm_timeout_error(timeout));
        };
        let record_id = oldest.record;
        let occurred_at = oldest.occurred_at;
        match result {
            Ok(()) => {
                pending.pop_front();
                outcome.deliver(record_id);
                Ok(())
            }
            Err(error) if Self::is_record_notice_rejection(&error) => {
                pending.pop_front();
                outcome.reject(RejectedSinkRecord::external(
                    record_id,
                    occurred_at,
                    format!("mqtt rejected record: {error}"),
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
        pending: &mut VecDeque<PendingMqttConfirmation>,
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
            match result {
                Ok(()) => outcome.deliver(confirmation.record),
                Err(error) if Self::is_record_notice_rejection(&error) => {
                    outcome.reject(RejectedSinkRecord::external(
                        confirmation.record,
                        confirmation.occurred_at,
                        format!("mqtt rejected record: {error}"),
                    ));
                }
                Err(_) => {}
            }
        }
    }

    /// Accepts a payload of `payload_bytes` whose PUBLISH packet the protocol can express and the
    /// broker declared it will receive, or says why no packet of it can be sent.
    fn admit(&self, payload_bytes: usize) -> MqttRecordResult<()> {
        let packet_bytes = self.framing.packet_bytes(payload_bytes)?;
        self.broker_limit.admit(packet_bytes)
    }

    fn is_record_client_rejection(error: &MqttClientError) -> bool {
        matches!(error, MqttClientError::InvalidRequest(_))
    }

    fn is_record_notice_rejection(error: &PublishNoticeError) -> bool {
        matches!(
            error,
            PublishNoticeError::V5PubAck(
                MqttPubAckReason::NotAuthorized
                    | MqttPubAckReason::TopicNameInvalid
                    | MqttPubAckReason::PayloadFormatInvalid
            ) | PublishNoticeError::V5PubRec(
                MqttPubRecReason::NotAuthorized
                    | MqttPubRecReason::TopicNameInvalid
                    | MqttPubRecReason::PayloadFormatInvalid
            )
        )
    }

    fn confirm_timeout_error(timeout: Duration) -> Report<SinkPublishError> {
        Self::publish_error(format!(
            "mqtt publish confirmation exceeded ACK TIMEOUT {}",
            humantime::format_duration(timeout)
        ))
    }

    fn config_value(config: &[ClientConfigEntry], key: &str) -> SinkStartResult<String> {
        client_config_value(config, key, "MQTT").map_err(|error| {
            let message = error.current_context().to_string();
            error
                .change_context(SinkStartError::InvalidConfiguration { sink: MQTT })
                .attach_printable(message)
        })
    }

    fn read_tls_file(path: &std::path::PathBuf, label: &str) -> SinkStartResult<Vec<u8>> {
        read_tls_file(path, label).map_err(|error| {
            let message = error.current_context().to_string();
            error
                .change_context(SinkStartError::InvalidConfiguration { sink: MQTT })
                .attach_printable(message)
        })
    }

    fn config_error(error: impl std::fmt::Display) -> Report<SinkStartError> {
        Report::new(SinkStartError::InvalidConfiguration { sink: MQTT })
            .attach_printable(error.to_string())
    }

    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: MQTT }).attach_printable(error.to_string())
    }
}

impl Drop for MqttSink {
    fn drop(&mut self) {
        self.eventloop_shutdown.send_replace(true);
    }
}

#[async_trait]
impl SinkLifecycle for MqttSink {
    /// A publish failure leaves the client connected: its event loop reconnects on its own
    /// schedule, and dropping it would discard the session the broker still holds.
    fn keeps_client_on_publish_failure(&self) -> bool {
        true
    }
}

#[async_trait]
impl RecordSink for MqttSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome<SinkRecordId> {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let mut pending: VecDeque<PendingMqttConfirmation> = VecDeque::new();
        for record in records {
            nervix_primitives::task::consume_budget().await;
            let record_id = record.id;
            let occurred_at = record.occurred_at;
            // The client would fail its connection on a packet the broker refuses to receive, so
            // a record whose packet cannot be sent is rejected before it is handed over.
            if let Err(error) = self.admit(record.payload.len()) {
                outcome.reject(RejectedSinkRecord::external(
                    record_id,
                    occurred_at,
                    format!("mqtt rejected record: {error}"),
                ));
                continue;
            }
            if let MqttPublishingMode::Qos0 = self.mode {
                match self.client.try_publish(
                    self.topic.as_str(),
                    record.payload,
                    PublishOptions::at_most_once(),
                ) {
                    Ok(()) => outcome.deliver(record_id),
                    Err(error) if Self::is_record_client_rejection(&error) => {
                        outcome.reject(RejectedSinkRecord::external(
                            record_id,
                            occurred_at,
                            format!("mqtt rejected record: {error}"),
                        ));
                    }
                    Err(error) => {
                        outcome.fail(Self::publish_error(error));
                        return outcome;
                    }
                }
                continue;
            }

            let notice = match self.client.try_publish_tracked(
                self.topic.as_str(),
                record.payload,
                self.mode.publish_options(),
            ) {
                Ok(notice) => notice,
                Err(error) if Self::is_record_client_rejection(&error) => {
                    outcome.reject(RejectedSinkRecord::external(
                        record_id,
                        occurred_at,
                        format!("mqtt rejected record: {error}"),
                    ));
                    continue;
                }
                Err(error) => {
                    outcome.fail(Self::publish_error(error));
                    return outcome;
                }
            };
            let AckConfirmation {
                max_in_flight,
                timeout,
            } = self.mode.confirmation_settings().verified(
                "this path only runs for the confirmed publishing mode, which carries the settings",
            );
            let Some(deadline) = Instant::now().checked_add(timeout) else {
                outcome.fail(Self::publish_error(
                    "mqtt ACK TIMEOUT exceeds the monotonic clock range",
                ));
                return outcome;
            };
            pending.push_back(PendingMqttConfirmation {
                record: record_id,
                occurred_at,
                deadline,
                confirmation: Box::pin(notice.wait_completion_async()),
            });
            if pending.len() >= max_in_flight.get()
                && let Err(error) = Self::confirm_oldest(&mut pending, timeout, &mut outcome).await
            {
                outcome.fail(error);
                return outcome;
            }
        }
        while !pending.is_empty() {
            nervix_primitives::task::consume_budget().await;
            let AckConfirmation { timeout, .. } = self.mode.confirmation_settings().verified(
                "this path only runs for the confirmed publishing mode, which carries the settings",
            );
            if let Err(error) = Self::confirm_oldest(&mut pending, timeout, &mut outcome).await {
                outcome.fail(error);
                return outcome;
            }
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending(
        index: usize,
        confirmation: MqttConfirmation,
        deadline: Instant,
    ) -> PendingMqttConfirmation {
        PendingMqttConfirmation {
            record: SinkRecordId::new(index),
            occurred_at: Timestamp::from_unix_nanos(0),
            deadline,
            confirmation,
        }
    }

    #[test]
    fn ready_younger_confirmations_are_accounted_before_retrying_oldest() {
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut window = VecDeque::from([
            pending(0, Box::pin(std::future::pending()), deadline),
            pending(1, Box::pin(async { Ok(()) }), deadline),
            pending(
                2,
                Box::pin(async {
                    Err(PublishNoticeError::V5PubAck(
                        MqttPubAckReason::NotAuthorized,
                    ))
                }),
                deadline,
            ),
            pending(
                3,
                Box::pin(async { Err(PublishNoticeError::SessionReset) }),
                deadline,
            ),
        ]);
        let mut outcome = PerRecordOutcome::empty();

        MqttSink::harvest_ready_after_oldest_failure(&mut window, &mut outcome);
        let outcome = outcome.into_parts();

        assert_eq!(window.len(), 1, "only the unresolved oldest must remain");
        assert_eq!(outcome.delivered, vec![SinkRecordId::new(1)]);
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].id, SinkRecordId::new(2));
        assert!(outcome.infrastructure_error.is_none());
    }

    #[test]
    fn authorization_topic_and_payload_rejections_are_record_specific() {
        for reason in [
            MqttPubAckReason::NotAuthorized,
            MqttPubAckReason::TopicNameInvalid,
            MqttPubAckReason::PayloadFormatInvalid,
        ] {
            assert!(MqttSink::is_record_notice_rejection(
                &PublishNoticeError::V5PubAck(reason)
            ));
        }
        for reason in [
            MqttPubRecReason::NotAuthorized,
            MqttPubRecReason::TopicNameInvalid,
            MqttPubRecReason::PayloadFormatInvalid,
        ] {
            assert!(MqttSink::is_record_notice_rejection(
                &PublishNoticeError::V5PubRec(reason)
            ));
        }
    }

    fn topic(name: &str) -> TopicName {
        TopicName::parse(name).expect("the test topic is a valid topic name")
    }

    fn confirmed() -> AckConfirmation {
        AckConfirmation {
            max_in_flight: NonZeroUsize::MIN,
            timeout: Duration::from_secs(1),
        }
    }

    /// The size of the packet the client itself encodes for `payload_bytes` on `topic`, with the
    /// packet identifier it assigns to QoS 1 and 2 publishes.
    fn encoded_packet_bytes(topic: &str, qos: rumqttc::QoS, payload_bytes: usize) -> usize {
        let mut publish = rumqttc::Publish::new(topic, qos, vec![0_u8; payload_bytes], None);
        if qos != rumqttc::QoS::AtMostOnce {
            publish.pkid = 1;
        }
        publish.size()
    }

    #[test]
    fn packet_size_matches_what_the_client_writes_across_remaining_length_widths() {
        let cases = [
            (MqttPublishingMode::Qos0, rumqttc::QoS::AtMostOnce),
            (
                MqttPublishingMode::Qos1(confirmed()),
                rumqttc::QoS::AtLeastOnce,
            ),
            (
                MqttPublishingMode::Qos2(confirmed()),
                rumqttc::QoS::ExactlyOnce,
            ),
        ];
        for (mode, qos) in cases {
            let framing = MqttPublishFraming::new(&topic("batched_out"), mode);
            for payload_bytes in [0, 1, 100, 127, 128, 16_000, 16_383, 16_384, 2_097_000] {
                let packet_bytes = framing
                    .packet_bytes(payload_bytes)
                    .expect("every tested payload fits an MQTT packet");
                assert_eq!(
                    packet_bytes,
                    encoded_packet_bytes("batched_out", qos, payload_bytes),
                    "{mode:?} with a {payload_bytes}-byte payload"
                );
            }
        }
    }

    #[test]
    fn a_payload_past_the_largest_remaining_length_has_no_packet() {
        let framing = MqttPublishFraming::new(&topic("t"), MqttPublishingMode::Qos0);
        // Two topic-length bytes, one topic byte and one property-length byte precede the payload.
        let largest_payload = MQTT_MAX_REMAINING_LENGTH - 4;

        let largest_packet = framing
            .packet_bytes(largest_payload)
            .expect("the largest remaining length is still a packet");
        assert_eq!(largest_packet, MQTT_MAX_REMAINING_LENGTH + 5);
        for payload_bytes in [largest_payload + 1, usize::MAX] {
            let error = framing
                .packet_bytes(payload_bytes)
                .expect_err("a payload past the largest remaining length has no packet");
            assert_eq!(
                error.current_context(),
                &MqttRecordError::BeyondProtocol { payload_bytes }
            );
        }
    }

    fn connack(max_packet_size: Option<u32>) -> ConnAck {
        let properties = rumqttc::ConnAckProperties {
            session_expiry_interval: None,
            receive_max: None,
            max_qos: None,
            retain_available: None,
            max_packet_size,
            assigned_client_identifier: None,
            topic_alias_max: None,
            reason_string: None,
            user_properties: Vec::new(),
            wildcard_subscription_available: None,
            subscription_identifiers_available: None,
            shared_subscription_available: None,
            server_keep_alive: None,
            response_information: None,
            server_reference: None,
            authentication_method: None,
            authentication_data: None,
        };
        ConnAck {
            session_present: false,
            code: rumqttc::ConnectReturnCode::Success,
            properties: Some(properties),
        }
    }

    #[test]
    fn the_latest_connack_decides_which_packets_the_broker_receives() {
        let limit = MqttBrokerPacketLimit::default();
        assert!(
            limit.admit(usize::MAX).is_ok(),
            "no CONNACK declared a limit yet"
        );

        limit.declare(&connack(Some(1_048_576)));
        assert!(limit.admit(1_048_576).is_ok());
        let refused = limit
            .admit(1_048_577)
            .expect_err("a packet past the declared maximum is refused");
        assert_eq!(
            refused.current_context(),
            &MqttRecordError::PacketTooLarge {
                packet_bytes: 1_048_577,
                maximum: NonZeroU32::new(1_048_576).expect("the test maximum is positive"),
            }
        );

        // A reconnect to a broker that declares no maximum withdraws the previous one.
        limit.declare(&connack(None));
        assert!(limit.admit(1_048_577).is_ok());
        limit.declare(&ConnAck {
            session_present: true,
            code: rumqttc::ConnectReturnCode::Success,
            properties: None,
        });
        assert_eq!(limit.declared(), None);
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

    impl nervix_connector::SinkGeneralErrorHandler for SilentHost {
        fn handle_general_error(
            &self,
            _acks: &nervix_connector::SinkAcknowledgements,
            _reason: String,
        ) {
        }
    }

    /// A QoS 0 sink on `topic` whose client connects to `addr`, retrying slowly enough that a test
    /// never sees a second attempt.
    fn qos0_sink(addr: &str, topic_name: &str) -> MqttSink {
        let config = MqttSinkConfig {
            config: vec![ClientConfigEntry {
                key: "addr".to_string(),
                value: addr.to_string(),
            }],
            topic: topic(topic_name),
            mode: MqttPublishingMode::Qos0,
            retry_policy: ParsedRetryPolicy {
                backoff: Duration::from_secs(60),
                max_backoff: Duration::from_secs(60),
            },
            default_client_id: "nervix-mqtt-sink-test".to_string(),
        };
        MqttSink::new(config, SinkHost::new(SilentHost)).expect("the test sink config is valid")
    }

    fn record(index: usize, payload_bytes: usize) -> SinkRecord {
        SinkRecord::new(
            SinkRecordId::new(index),
            None,
            vec![b'x'; payload_bytes],
            Vec::new(),
            Timestamp::from_unix_nanos(0),
        )
    }

    #[nervix_primitives::test]
    async fn a_record_the_broker_would_refuse_is_rejected_before_the_client_sees_it() {
        // Nothing listens on the discard port, so the client never connects and the accepted
        // record waits in its request queue.
        let mut sink = qos0_sink("mqtt://127.0.0.1:9", "limited");
        sink.broker_limit.declare(&connack(Some(64)));

        // The topic `limited` takes 12 bytes around a QoS 0 payload: the fixed header byte, one
        // remaining-length byte, the topic with its length prefix, and the property length.
        let outcome = sink
            .publish(vec![record(0, 52), record(1, 53)])
            .await
            .into_parts();

        assert_eq!(outcome.delivered, vec![SinkRecordId::new(0)]);
        let [rejected] = outcome.rejected.as_slice() else {
            panic!(
                "exactly the oversize record is rejected: {:?}",
                outcome.rejected
            );
        };
        assert_eq!(rejected.id, SinkRecordId::new(1));
        assert_eq!(
            rejected.error.message,
            "mqtt rejected record: MQTT PUBLISH packet of 65 bytes exceeds the broker's maximum \
             packet size of 64 bytes"
        );
        assert!(outcome.infrastructure_error.is_none());
    }

    #[nervix_primitives::test]
    async fn the_event_loop_records_the_maximum_packet_size_the_broker_declares() {
        let broker = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port is available");
        let addr = broker
            .local_addr()
            .expect("a bound listener has an address");
        let sink = qos0_sink(&format!("mqtt://{addr}"), "declared");

        let (mut connection, _) = broker
            .accept()
            .await
            .expect("the sink's client connects to the test broker");
        let mut connect = [0_u8; 256];
        let read = tokio::io::AsyncReadExt::read(&mut connection, &mut connect)
            .await
            .expect("the client sends its CONNECT packet");
        assert!(read > 0, "the client sends its CONNECT packet");
        // CONNACK: success, no session, and one property, a Maximum Packet Size of 1024 bytes.
        let connack = [0x20, 0x08, 0x00, 0x00, 0x05, 0x27, 0x00, 0x00, 0x04, 0x00];
        tokio::io::AsyncWriteExt::write_all(&mut connection, &connack)
            .await
            .expect("the test broker answers the CONNECT");

        let declared = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                nervix_primitives::task::consume_budget().await;
                if let Some(maximum) = sink.broker_limit.declared() {
                    return maximum;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the event loop records the CONNACK it received");
        assert_eq!(declared.get(), 1024);
    }

    #[test]
    fn quota_and_session_failures_remain_infrastructure_failures() {
        assert!(!MqttSink::is_record_notice_rejection(
            &PublishNoticeError::V5PubAck(MqttPubAckReason::QuotaExceeded)
        ));
        assert!(!MqttSink::is_record_notice_rejection(
            &PublishNoticeError::SessionReset
        ));
    }
}
