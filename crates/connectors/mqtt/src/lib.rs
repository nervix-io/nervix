//! MQTT sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The MQTT client a configuration declares, its event loop and reconnect backoff, the
//!   quality of service each record is published at, and confirmation classification.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio and
//!   `rumqttc`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use std::{collections::VecDeque, future::Future, num::NonZeroUsize, pin::Pin, time::Duration};

use async_trait::async_trait;
use error_stack::Report;
use futures_util::FutureExt;
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, ParsedRetryPolicy, PerRecordOutcome, RecordSink, RejectedSinkRecord, SinkHost,
    SinkLifecycle, SinkPublishError, SinkPublishResult, SinkRecord, SinkRecordPosition,
    SinkStartError, SinkStartResult, client_config_value, client_tls_paths, next_retry_delay,
    optional_client_config_value, read_tls_file,
};
use nervix_models::{ClientConfigEntry, Timestamp, TopicName};
use rumqttc::{
    AsyncClient, ClientError as MqttClientError, Event, MqttOptions,
    PubAckReason as MqttPubAckReason, PubRecReason as MqttPubRecReason, PublishNoticeError,
    PublishOptions, SessionMode, TlsConfiguration, Transport as MqttTransport, ValidatedTopic,
};
use tokio::{
    sync::watch,
    time::{Instant, sleep},
};
use tracing::warn;
use url::{Host, Url};

const MQTT: &str = "mqtt";

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
    eventloop_shutdown: watch::Sender<bool>,
}

type MqttConfirmation = Pin<Box<dyn Future<Output = Result<(), PublishNoticeError>> + Send>>;

struct PendingMqttConfirmation {
    position: SinkRecordPosition,
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
        let (eventloop_shutdown, mut eventloop_shutdown_rx) = watch::channel(false);
        tokio::spawn(async move {
            let mut backoff = MqttReconnectBackoff::from_policy(retry_policy);
            loop {
                tokio::task::consume_budget().await;
                let polled = tokio::select! {
                    changed = eventloop_shutdown_rx.changed() => {
                        if changed.is_err() || *eventloop_shutdown_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    polled = eventloop.poll() => polled,
                };
                match polled {
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
                        tokio::select! {
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
        Ok(Self {
            client,
            topic: config.topic,
            mode,
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
        outcome: &mut PerRecordOutcome,
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
            Ok(()) => {
                pending.pop_front();
                outcome.deliver(position);
                Ok(())
            }
            Err(error) if Self::is_record_notice_rejection(&error) => {
                pending.pop_front();
                outcome.reject(RejectedSinkRecord::external(
                    position,
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
                Ok(()) => outcome.deliver(confirmation.position),
                Err(error) if Self::is_record_notice_rejection(&error) => {
                    outcome.reject(RejectedSinkRecord::external(
                        confirmation.position,
                        confirmation.occurred_at,
                        format!("mqtt rejected record: {error}"),
                    ));
                }
                Err(_) => {}
            }
        }
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
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome {
        let mut outcome = PerRecordOutcome::with_capacity(records.len());
        let mut pending: VecDeque<PendingMqttConfirmation> = VecDeque::new();
        for record in records {
            tokio::task::consume_budget().await;
            let position = record.position;
            let occurred_at = record.occurred_at;
            if let MqttPublishingMode::Qos0 = self.mode {
                match self.client.try_publish(
                    self.topic.as_str(),
                    record.payload,
                    PublishOptions::at_most_once(),
                ) {
                    Ok(()) => outcome.deliver(position),
                    Err(error) if Self::is_record_client_rejection(&error) => {
                        outcome.reject(RejectedSinkRecord::external(
                            position,
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
                        position,
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
                position,
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
            tokio::task::consume_budget().await;
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

    fn position(row_index: usize) -> SinkRecordPosition {
        SinkRecordPosition {
            batch_index: 0,
            row_index,
        }
    }

    fn pending(
        row_index: usize,
        confirmation: MqttConfirmation,
        deadline: Instant,
    ) -> PendingMqttConfirmation {
        PendingMqttConfirmation {
            position: position(row_index),
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
        assert_eq!(outcome.delivered, vec![position(1)]);
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].position.row_index, 2);
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
