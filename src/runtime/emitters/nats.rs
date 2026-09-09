use std::{future::Future, pin::Pin};

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
use futures_util::{FutureExt, SinkExt};
use nervix_models::SubjectName;

use super::*;

pub(in crate::runtime) struct NatsEmitter {
    client: Option<NatsClient>,
    delivery: NatsDelivery,
    subject: Subject,
}

/// How this emitter publishes, together with whatever that way of publishing needs.
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
    position: BrokerRecordPosition,
    deadline: Instant,
    confirmation: NatsConfirmation,
}

impl NatsEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientNats,
        resolved: Option<&ResolvedClientConfig>,
        subject: &SubjectName,
        mode: NatsPublishingMode,
        retry_policy: ParsedRetryPolicy,
    ) -> EmitterRuntimeResult<Self> {
        let client = Self::client_from_config(
            client_config_entries(resolved, client.config.as_slice()),
            retry_policy,
        )
        .await?;
        let delivery = match mode {
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
            client: Some(client),
            delivery,
            subject: Subject::from(subject.as_str().to_string()),
        })
    }

    async fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
        retry_policy: ParsedRetryPolicy,
    ) -> EmitterRuntimeResult<NatsClient> {
        let addr = emitter_config_value(config, "addr", || {
            "missing NATS client config key 'addr'".to_string()
        })?;
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
                return Err(emitter_config_error(
                    "NATS TLS client authentication requires both 'tls_cert_file' and \
                     'tls_key_file'",
                ));
            }
        }
        options.connect(addr).await.map_err(emitter_init_error)
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

    pub(super) async fn publish_records(
        &self,
        records: Vec<EncodedBrokerRecord>,
    ) -> PerRecordPublishOutcome {
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

    async fn publish_core(&self, records: Vec<EncodedBrokerRecord>) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        let Some(client) = self.client.as_ref() else {
            outcome.fail(
                Report::new(EmitterRuntimeError::SinkNotInitialized)
                    .attach_printable("no initialized nats sink client"),
            );
            return outcome;
        };
        let mut sink = client.clone();
        let mut queued = Vec::with_capacity(records.len());
        for record in records {
            tokio::task::consume_budget().await;
            let position = record.position();
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
                    outcome.reject(position, format!("nats rejected record: {error}"));
                }
                Err(error) => {
                    outcome.fail(emitter_publish_error(error));
                    return outcome;
                }
            }
        }
        match client.flush().await {
            Ok(()) => outcome.delivered.extend(queued),
            Err(error) => outcome.fail(emitter_publish_error(error)),
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
        records: Vec<EncodedBrokerRecord>,
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        outcome.delivered.reserve(records.len());
        let mut pending: VecDeque<PendingNatsConfirmation> = VecDeque::new();
        for record in records {
            tokio::task::consume_budget().await;
            let position = record.position();
            let publish = async {
                if record.headers.is_empty() {
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
                }
            };
            let confirmation = publish.await;
            let confirmation = match confirmation {
                Ok(confirmation) => confirmation,
                Err(error) if Self::is_jetstream_record_rejection(&error) => {
                    outcome.reject(position, format!("nats JetStream rejected record: {error}"));
                    continue;
                }
                Err(error) => {
                    outcome.fail(emitter_publish_error(error));
                    return outcome;
                }
            };
            pending.push_back(PendingNatsConfirmation {
                position,
                deadline: Instant::now() + timeout,
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
        outcome: &mut PerRecordPublishOutcome,
    ) -> EmitterRuntimeResult<()> {
        loop {
            tokio::task::consume_budget().await;
            let Some(oldest) = pending.front_mut() else {
                return Err(emitter_publish_error(
                    "NATS JetStream acknowledgment window unexpectedly became empty",
                ));
            };
            let remaining = oldest
                .deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                return Err(emitter_publish_error(format!(
                    "NATS JetStream PubAck exceeded ACK TIMEOUT {}",
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
                Ok(_ack) => {
                    pending.pop_front();
                    outcome.deliver(position);
                    return Ok(());
                }
                Err(error) if Self::is_jetstream_record_rejection(&error) => {
                    pending.pop_front();
                    outcome.reject(position, format!("nats JetStream rejected record: {error}"));
                    return Ok(());
                }
                Err(error) => {
                    Self::harvest_ready_after_oldest_failure(pending, outcome);
                    return Err(emitter_publish_error(error));
                }
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
            let confirmation = pending.remove(index).verified(
                "the index came from scanning this same pending window, which nothing else \
                 removes from",
            );
            match result {
                Ok(_ack) => outcome.deliver(confirmation.position),
                Err(error) if Self::is_jetstream_record_rejection(&error) => outcome.reject(
                    confirmation.position,
                    format!("nats JetStream rejected record: {error}"),
                ),
                Err(_) => {}
            }
        }
    }

    fn header_map(headers: &EmitterHeaders) -> async_nats::HeaderMap {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_core_max_payload_is_a_record_rejection() {
        let oversized = NatsPublishError::new(NatsPublishErrorKind::MaxPayloadExceeded);
        let invalid_subject = NatsPublishError::new(NatsPublishErrorKind::InvalidSubject);

        assert!(NatsEmitter::is_core_record_rejection(&oversized));
        assert!(!NatsEmitter::is_core_record_rejection(&invalid_subject));
    }

    #[test]
    fn missing_jetstream_is_infrastructure_not_record_rejection() {
        let error = JetStreamPublishError::new(PublishErrorKind::StreamNotFound);
        assert!(NatsEmitter::jetstream_error_is_missing_stream(&error));
        assert!(!NatsEmitter::is_jetstream_record_rejection(&error));
    }

    #[test]
    fn oversized_jetstream_payload_is_a_record_rejection() {
        let error = JetStreamPublishError::new(PublishErrorKind::MaxPayloadExceeded);
        assert!(NatsEmitter::is_jetstream_record_rejection(&error));
        assert!(!NatsEmitter::jetstream_error_is_missing_stream(&error));
    }

    #[test]
    fn connection_delay_uses_declared_exponential_policy() {
        let policy = ParsedRetryPolicy {
            backoff: Duration::from_millis(125),
            max_backoff: Duration::from_secs(1),
        };

        assert_eq!(
            NatsEmitter::connection_delay(policy, 1, false),
            Duration::ZERO
        );
        assert_eq!(
            NatsEmitter::connection_delay(policy, 2, false),
            Duration::from_millis(125)
        );
        assert_eq!(
            NatsEmitter::connection_delay(policy, 1, true),
            Duration::from_millis(125)
        );
        assert_eq!(
            NatsEmitter::connection_delay(policy, 2, true),
            Duration::from_millis(250)
        );
        assert_eq!(
            NatsEmitter::connection_delay(policy, 4, true),
            Duration::from_secs(1)
        );
        assert_eq!(
            NatsEmitter::connection_delay(policy, 50, true),
            Duration::from_secs(1)
        );
    }
}
