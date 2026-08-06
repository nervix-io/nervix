use ::pulsar::{
    ConnectionRetryOptions, Error as PulsarError, OperationRetryOptions, Pulsar,
    TlsOptions as PulsarTlsOptions, TokioExecutor,
    producer::{Message as PulsarProducerMessage, SendFuture as PulsarSendFuture},
};
use futures_util::FutureExt;

use super::*;

pub(in crate::runtime) struct PulsarEmitter {
    producer: Option<::pulsar::Producer<TokioExecutor>>,
    mode: BrokerPublishingMode,
}

struct PendingPulsarConfirmation {
    position: BrokerRecordPosition,
    acks: AckSet,
    deadline: Instant,
    confirmation: PulsarSendFuture,
}

impl PulsarEmitter {
    pub(super) async fn new(
        client: &CreateClientPulsar,
        resolved: Option<&ResolvedClientConfig>,
        topic: &Identifier,
        mode: BrokerPublishingMode,
    ) -> EmitterRuntimeResult<Self> {
        let producer = Self::producer_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
            topic.as_str(),
        )
        .await?;
        Ok(Self {
            producer: Some(producer),
            mode,
        })
    }

    async fn producer_from_config(
        config: &[nervix_models::ClientConfigEntry],
        topic: &str,
    ) -> EmitterRuntimeResult<::pulsar::Producer<TokioExecutor>> {
        let pulsar = Self::client_from_config(config).await?;
        let topic_name = Self::topic_from_config(config, topic);
        pulsar
            .producer()
            .with_topic(topic_name)
            .build()
            .await
            .map_err(emitter_init_error)
    }

    async fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<Pulsar<TokioExecutor>> {
        let addr = emitter_config_value(config, "addr", || {
            "missing Pulsar client config key 'addr'".to_string()
        })?;
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
        builder.build().await.map_err(emitter_init_error)
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

    pub(in crate::runtime) fn tls_options_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<Option<PulsarTlsOptions>> {
        let tls = client_tls_paths(config);
        if tls.cert_file.is_some() || tls.key_file.is_some() {
            return Err(emitter_config_error(
                "Pulsar TLS currently supports only 'tls_ca_file'; client authentication via \
                 'tls_cert_file' and 'tls_key_file' is not supported",
            ));
        }

        let allow_insecure_connection =
            emitter_optional_bool_client_config_value(config, "tls_allow_insecure_connection")?;
        let tls_hostname_verification_enabled =
            emitter_optional_bool_client_config_value(config, "tls_hostname_verification_enabled")?;

        if tls.ca_file.is_none()
            && allow_insecure_connection.is_none()
            && tls_hostname_verification_enabled.is_none()
        {
            return Ok(None);
        }

        let mut tls_options = PulsarTlsOptions::default();
        if let Some(ca_file) = tls.ca_file.as_ref() {
            tls_options.certificate_chain =
                Some(emitter_read_tls_file(ca_file, "TLS CA certificate")?);
        }
        if let Some(allow_insecure_connection) = allow_insecure_connection {
            tls_options.allow_insecure_connection = allow_insecure_connection;
        }
        if let Some(tls_hostname_verification_enabled) = tls_hostname_verification_enabled {
            tls_options.tls_hostname_verification_enabled = tls_hostname_verification_enabled;
        }
        Ok(Some(tls_options))
    }

    fn topic_from_config(config: &[nervix_models::ClientConfigEntry], topic: &str) -> String {
        if topic.contains("://") {
            return topic.to_string();
        }

        let namespace =
            optional_client_config_value(config, "namespace").unwrap_or("public/default");
        format!("persistent://{namespace}/{topic}")
    }

    pub(super) async fn publish(
        &mut self,
        records: Vec<EncodedBrokerRecord>,
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        let Some(producer) = self.producer.as_mut() else {
            outcome.fail(
                Report::new(EmitterRuntimeError::SinkNotInitialized)
                    .attach_printable("no initialized pulsar sink client"),
            );
            return outcome;
        };

        outcome.delivered.reserve(records.len());
        let mut pending: VecDeque<PendingPulsarConfirmation> = VecDeque::new();
        for record in records {
            tokio::task::consume_budget().await;
            let enqueue_acks = AckSet::merged(
                pending
                    .iter()
                    .map(|confirmation| confirmation.acks.clone())
                    .chain(std::iter::once(record.acks.clone())),
            );
            let position = (record.batch_index, record.row_index);
            let confirmation = match await_emitter_confirmation(
                &enqueue_acks,
                producer.send_non_blocking(PulsarProducerMessage {
                    payload: record.payload,
                    properties: record.headers.into_iter().collect(),
                    partition_key: record.key,
                    ..Default::default()
                }),
            )
            .await
            {
                Ok(confirmation) => confirmation,
                Err(source) if Self::is_record_rejection(&source) => {
                    outcome.reject(position, format!("pulsar rejected record: {source}"));
                    continue;
                }
                Err(source) => {
                    outcome.fail(emitter_publish_error(format!(
                        "failed to enqueue pulsar message: {source}"
                    )));
                    return outcome;
                }
            };
            match self.mode {
                BrokerPublishingMode::NoAck => {
                    drop(confirmation);
                    outcome.deliver(position);
                }
                BrokerPublishingMode::Ack {
                    max_in_flight,
                    timeout,
                } => {
                    pending.push_back(PendingPulsarConfirmation {
                        position,
                        acks: record.acks,
                        deadline: Instant::now() + timeout,
                        confirmation,
                    });
                    if pending.len() >= max_in_flight
                        && let Err(error) =
                            Self::confirm_oldest(&mut pending, timeout, &mut outcome).await
                    {
                        outcome.fail(error);
                        return outcome;
                    }
                }
            }
        }
        while !pending.is_empty() {
            tokio::task::consume_budget().await;
            let timeout = match self.mode {
                BrokerPublishingMode::Ack { timeout, .. } => timeout,
                BrokerPublishingMode::NoAck => unreachable!("NO_ACK has no confirmations"),
            };
            if let Err(error) = Self::confirm_oldest(&mut pending, timeout, &mut outcome).await {
                outcome.fail(error);
                return outcome;
            }
        }
        outcome
    }

    async fn confirm_oldest(
        pending: &mut VecDeque<PendingPulsarConfirmation>,
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
                    "pulsar acknowledgment window unexpectedly became empty",
                ));
            };
            let remaining = oldest
                .deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                return Err(emitter_publish_error(format!(
                    "pulsar receipt exceeded ACK TIMEOUT {}",
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
                Ok(_receipt) => {
                    pending.pop_front();
                    outcome.deliver(position);
                    return Ok(());
                }
                Err(source) if Self::is_record_rejection(&source) => {
                    pending.pop_front();
                    outcome.reject(position, format!("pulsar rejected record: {source}"));
                    return Ok(());
                }
                Err(source) => {
                    Self::harvest_ready_after_oldest_failure(pending, outcome);
                    return Err(emitter_publish_error(source));
                }
            }
        }
    }

    fn harvest_ready_after_oldest_failure(
        pending: &mut VecDeque<PendingPulsarConfirmation>,
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
                .expect("ready Pulsar confirmation must remain in the window");
            match result {
                Ok(_receipt) => outcome.deliver(confirmation.position),
                Err(source) if Self::is_record_rejection(&source) => outcome.reject(
                    confirmation.position,
                    format!("pulsar rejected record: {source}"),
                ),
                Err(_) => {}
            }
        }
    }

    fn is_record_rejection(_error: &PulsarError) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use ::pulsar::{
        error::ConnectionError as PulsarConnectionError,
        message::proto::ServerError as PulsarServerError,
    };

    use super::*;

    fn server_error(kind: PulsarServerError) -> PulsarError {
        PulsarError::Connection(PulsarConnectionError::PulsarError(Some(kind), None))
    }

    #[test]
    fn checksum_failure_remains_an_infrastructure_failure() {
        assert!(!PulsarEmitter::is_record_rejection(&server_error(
            PulsarServerError::ChecksumError
        )));
    }

    #[test]
    fn missing_or_incompatible_topics_remain_infrastructure_failures() {
        for kind in [
            PulsarServerError::TopicNotFound,
            PulsarServerError::IncompatibleSchema,
            PulsarServerError::ServiceNotReady,
        ] {
            assert!(!PulsarEmitter::is_record_rejection(&server_error(kind)));
        }
    }

    #[test]
    fn client_retries_are_disabled_in_favor_of_the_declared_retry_policy() {
        let (connection, operation) = PulsarEmitter::retry_options();

        assert_eq!(connection.max_retries, 0);
        assert_eq!(operation.max_retries, Some(0));
        assert!(!operation.allow_retry(0));
    }
}
