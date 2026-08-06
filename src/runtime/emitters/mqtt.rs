use std::{future::Future, pin::Pin};

use futures_util::FutureExt;
use rumqttc::{
    AsyncClient, ClientError as MqttClientError, Event, MqttOptions,
    PubAckReason as MqttPubAckReason, PubRecReason as MqttPubRecReason, PublishNoticeError,
    PublishOptions, SessionMode, TlsConfiguration, Transport as MqttTransport, ValidatedTopic,
};
use url::{Host, Url};

use super::*;

pub(in crate::runtime) struct MqttEmitter {
    client: Option<AsyncClient>,
    mode: MqttPublishingMode,
    eventloop_shutdown: watch::Sender<bool>,
}

type MqttConfirmation = Pin<Box<dyn Future<Output = Result<(), PublishNoticeError>> + Send>>;

struct PendingMqttConfirmation {
    position: BrokerRecordPosition,
    acks: AckSet,
    deadline: Instant,
    confirmation: MqttConfirmation,
}

impl MqttEmitter {
    pub(in crate::runtime) fn new(
        client: &CreateClientMqtt,
        resolved: Option<&ResolvedClientConfig>,
        topic: &Identifier,
        context: &EmitterSinkContext,
        mode: MqttPublishingMode,
        retry_policy: ParsedRetryPolicy,
    ) -> EmitterRuntimeResult<Self> {
        let (client, mut eventloop) = Self::client_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
            &format!("{}-{}", context.domain.as_str(), context.emitter.as_str()),
            mode,
        )?;
        let domain = context.domain.clone();
        let emitter = context.emitter.clone();
        let events = context.events.clone();
        let runtime = context.runtime.clone();
        let (eventloop_shutdown, mut eventloop_shutdown_rx) = watch::channel(false);
        tokio::spawn(async move {
            let mut backoff = RuntimeReconnectBackoff::from_policy(retry_policy);
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
                    Ok(Event::Incoming(_)) | Ok(Event::Outgoing(_)) | Ok(Event::Auth(_)) => {
                        backoff.reset();
                        runtime.clear_emitter_transient_error(&domain, &emitter);
                    }
                    Err(error) => {
                        let wait = backoff.take_next_delay();
                        runtime.record_emitter_transient_error_with_backoff(
                            &domain,
                            &emitter,
                            error.to_string(),
                            wait,
                        );
                        let _ = events.send(RuntimeEvent::Error(format!(
                            "mqtt emitter event loop failed for '{}' in domain '{}'; reconnecting \
                             in {}: {}",
                            emitter.as_str(),
                            domain.as_str(),
                            humantime::format_duration(wait),
                            error
                        )));
                        warn!(
                            domain = domain.as_str(),
                            emitter = emitter.as_str(),
                            error = %error,
                            retry_in = %humantime::format_duration(wait),
                            "mqtt emitter event loop reconnecting"
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
        ValidatedTopic::new(topic.as_str()).map_err(emitter_config_error)?;
        Ok(Self {
            client: Some(client),
            mode,
            eventloop_shutdown,
        })
    }

    fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
        default_client_id: &str,
        mode: MqttPublishingMode,
    ) -> EmitterRuntimeResult<(AsyncClient, rumqttc::EventLoop)> {
        let addr = emitter_config_value(config, "addr", || {
            "missing MQTT client config key 'addr'".to_string()
        })?;
        let client_id = optional_client_config_value(config, "client_id")
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| default_client_id.to_string());

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
                emitter_read_tls_file(ca_file, "TLS CA certificate")?
            } else {
                return Err(emitter_config_error(
                    "MQTT TLS requires client config key 'tls_ca_file'",
                ));
            };
            let client_auth = match (&tls.cert_file, &tls.key_file) {
                (Some(cert_file), Some(key_file)) => Some((
                    emitter_read_tls_file(cert_file, "TLS certificate")?,
                    emitter_read_tls_file(key_file, "TLS private key")?,
                )),
                (None, None) => None,
                _ => {
                    return Err(emitter_config_error(
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
            MqttPublishingMode::Qos0 => 1,
            MqttPublishingMode::Qos1 { max_in_flight, .. }
            | MqttPublishingMode::Qos2 { max_in_flight, .. } => max_in_flight,
        };
        AsyncClient::builder(options)
            .capacity(request_capacity)
            .try_build()
            .map_err(|error| emitter_config_error(format!("invalid MQTT client config: {error}")))
    }

    fn parse_addr(addr: &str) -> EmitterRuntimeResult<MqttEmitterAddr> {
        let url = Url::parse(addr).map_err(|source| {
            emitter_config_error(format!("invalid MQTT addr '{addr}': {source}"))
        })?;
        let tls = if url.scheme() == "mqtt" {
            false
        } else if url.scheme() == "mqtts" {
            true
        } else {
            return Err(emitter_config_error(format!(
                "unsupported MQTT addr scheme '{}', expected mqtt:// or mqtts://",
                url.scheme()
            )));
        };
        let host = url
            .host()
            .map(|host| match host {
                Host::Domain(domain) => domain.to_string(),
                Host::Ipv4(addr) => addr.to_string(),
                Host::Ipv6(addr) => addr.to_string(),
            })
            .filter(|host| !host.is_empty())
            .ok_or_else(|| emitter_config_error(format!("missing host in MQTT addr '{addr}'")))?;
        let port = url
            .port()
            .ok_or_else(|| emitter_config_error(format!("missing port in MQTT addr '{addr}'")))?;
        Ok(MqttEmitterAddr { host, port, tls })
    }

    pub(super) async fn publish_records(
        &self,
        topic: &Identifier,
        records: Vec<EncodedBrokerRecord>,
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        let Some(client) = self.client.as_ref() else {
            outcome.fail(
                Report::new(EmitterRuntimeError::SinkNotInitialized)
                    .attach_printable("no initialized mqtt sink client"),
            );
            return outcome;
        };
        outcome.delivered.reserve(records.len());
        let mut pending: VecDeque<PendingMqttConfirmation> = VecDeque::new();
        for record in records {
            tokio::task::consume_budget().await;
            for confirmation in &pending {
                confirmation.acks.ack_alive();
            }
            record.acks.ack_alive();
            let position = (record.batch_index, record.row_index);
            if let MqttPublishingMode::Qos0 = self.mode {
                match client.try_publish(
                    topic.as_str(),
                    record.payload,
                    PublishOptions::at_most_once(),
                ) {
                    Ok(()) => outcome.deliver(position),
                    Err(error) if Self::is_record_client_rejection(&error) => {
                        outcome.reject(position, format!("mqtt rejected record: {error}"));
                    }
                    Err(error) => {
                        outcome.fail(emitter_publish_error(error));
                        return outcome;
                    }
                }
                continue;
            }

            let notice = match client.try_publish_tracked(
                topic.as_str(),
                record.payload,
                self.mode.publish_options(),
            ) {
                Ok(notice) => notice,
                Err(error) if Self::is_record_client_rejection(&error) => {
                    outcome.reject(position, format!("mqtt rejected record: {error}"));
                    continue;
                }
                Err(error) => {
                    outcome.fail(emitter_publish_error(error));
                    return outcome;
                }
            };
            let (max_in_flight, timeout) = self
                .mode
                .confirmation_settings()
                .expect("confirmed MQTT mode must have confirmation settings");
            pending.push_back(PendingMqttConfirmation {
                position,
                acks: record.acks,
                deadline: Instant::now() + timeout,
                confirmation: Box::pin(notice.wait_completion_async()),
            });
            if pending.len() >= max_in_flight
                && let Err(error) = Self::confirm_oldest(&mut pending, timeout, &mut outcome).await
            {
                outcome.fail(error);
                return outcome;
            }
        }
        while !pending.is_empty() {
            tokio::task::consume_budget().await;
            let (_, timeout) = self
                .mode
                .confirmation_settings()
                .expect("confirmed MQTT mode must have confirmation settings");
            if let Err(error) = Self::confirm_oldest(&mut pending, timeout, &mut outcome).await {
                outcome.fail(error);
                return outcome;
            }
        }
        outcome
    }

    async fn confirm_oldest(
        pending: &mut VecDeque<PendingMqttConfirmation>,
        timeout: Duration,
        outcome: &mut PerRecordPublishOutcome,
    ) -> EmitterRuntimeResult<()> {
        loop {
            tokio::task::consume_budget().await;
            for confirmation in pending.iter() {
                confirmation.acks.ack_alive();
            }
            let Some(oldest) = pending.front_mut() else {
                return Err(emitter_publish_error(
                    "mqtt acknowledgment window unexpectedly became empty",
                ));
            };
            let remaining = oldest
                .deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                return Err(emitter_publish_error(format!(
                    "mqtt publish confirmation exceeded ACK TIMEOUT {}",
                    humantime::format_duration(timeout)
                )));
            }
            let wait = remaining.min(REMOTE_ACK_ALIVE_INTERVAL);
            let result = tokio::select! {
                biased;
                result = &mut oldest.confirmation => Some(result),
                _ = sleep(wait) => None,
            };
            let Some(result) = result else {
                continue;
            };
            let position = oldest.position;
            match result {
                Ok(()) => {
                    pending.pop_front();
                    outcome.deliver(position);
                    return Ok(());
                }
                Err(error) if Self::is_record_notice_rejection(&error) => {
                    pending.pop_front();
                    outcome.reject(position, format!("mqtt rejected record: {error}"));
                    return Ok(());
                }
                Err(error) => {
                    Self::harvest_ready_after_oldest_failure(pending, outcome);
                    return Err(emitter_publish_error(error));
                }
            }
        }
    }

    fn harvest_ready_after_oldest_failure(
        pending: &mut VecDeque<PendingMqttConfirmation>,
        outcome: &mut PerRecordPublishOutcome,
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
            let confirmation = pending
                .remove(index)
                .expect("ready MQTT confirmation must remain in the window");
            match result {
                Ok(()) => outcome.deliver(confirmation.position),
                Err(error) if Self::is_record_notice_rejection(&error) => outcome.reject(
                    confirmation.position,
                    format!("mqtt rejected record: {error}"),
                ),
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
}

impl Drop for MqttEmitter {
    fn drop(&mut self) {
        self.eventloop_shutdown.send_replace(true);
    }
}

impl MqttPublishingMode {
    fn publish_options(self) -> PublishOptions {
        match self {
            Self::Qos0 => PublishOptions::at_most_once(),
            Self::Qos1 { .. } => PublishOptions::at_least_once(),
            Self::Qos2 { .. } => PublishOptions::exactly_once(),
        }
    }

    fn confirmation_settings(self) -> Option<(usize, Duration)> {
        match self {
            Self::Qos0 => None,
            Self::Qos1 {
                max_in_flight,
                timeout,
            }
            | Self::Qos2 {
                max_in_flight,
                timeout,
            } => Some((max_in_flight, timeout)),
        }
    }
}

struct MqttEmitterAddr {
    host: String,
    port: u16,
    tls: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_younger_confirmations_are_accounted_before_retrying_oldest() {
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut pending = VecDeque::from([
            PendingMqttConfirmation {
                position: (0, 0),
                acks: AckSet::empty(),
                deadline,
                confirmation: Box::pin(std::future::pending()),
            },
            PendingMqttConfirmation {
                position: (0, 1),
                acks: AckSet::empty(),
                deadline,
                confirmation: Box::pin(async { Ok(()) }),
            },
            PendingMqttConfirmation {
                position: (0, 2),
                acks: AckSet::empty(),
                deadline,
                confirmation: Box::pin(async {
                    Err(PublishNoticeError::V5PubAck(
                        MqttPubAckReason::NotAuthorized,
                    ))
                }),
            },
            PendingMqttConfirmation {
                position: (0, 3),
                acks: AckSet::empty(),
                deadline,
                confirmation: Box::pin(async { Err(PublishNoticeError::SessionReset) }),
            },
        ]);
        let mut outcome = PerRecordPublishOutcome::empty();

        MqttEmitter::harvest_ready_after_oldest_failure(&mut pending, &mut outcome);

        assert_eq!(pending.len(), 1, "only the unresolved oldest must remain");
        assert_eq!(outcome.delivered, vec![(0, 1)]);
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].position, (0, 2));
        assert!(outcome.infrastructure_error.is_none());
    }

    #[test]
    fn authorization_topic_and_payload_rejections_are_record_specific() {
        for reason in [
            MqttPubAckReason::NotAuthorized,
            MqttPubAckReason::TopicNameInvalid,
            MqttPubAckReason::PayloadFormatInvalid,
        ] {
            assert!(MqttEmitter::is_record_notice_rejection(
                &PublishNoticeError::V5PubAck(reason)
            ));
        }
        for reason in [
            MqttPubRecReason::NotAuthorized,
            MqttPubRecReason::TopicNameInvalid,
            MqttPubRecReason::PayloadFormatInvalid,
        ] {
            assert!(MqttEmitter::is_record_notice_rejection(
                &PublishNoticeError::V5PubRec(reason)
            ));
        }
    }

    #[test]
    fn quota_and_session_failures_remain_infrastructure_failures() {
        assert!(!MqttEmitter::is_record_notice_rejection(
            &PublishNoticeError::V5PubAck(MqttPubAckReason::QuotaExceeded)
        ));
        assert!(!MqttEmitter::is_record_notice_rejection(
            &PublishNoticeError::SessionReset
        ));
    }
}
