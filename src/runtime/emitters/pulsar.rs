//! Pulsar sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Pulsar client and producer configuration, its TLS options, topic qualification,
//!   record and property publication, and send-receipt classification.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio and
//!   `pulsar`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

use std::{collections::VecDeque, time::Duration};

use async_trait::async_trait;
use error_stack::Report;
use futures_util::FutureExt;
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, BrokerPublishingMode, PerRecordOutcome, RecordSink, RejectedSinkRecord,
    SinkHost, SinkLifecycle, SinkPublishError, SinkPublishResult, SinkRecord, SinkRecordPosition,
    SinkStartError, SinkStartResult, client_config_value, client_tls_paths,
    optional_bool_client_config_value, optional_client_config_value, read_tls_file,
};
use nervix_models::{ClientConfigEntry, Timestamp, TopicName};
use pulsar::{
    ConnectionRetryOptions, Error as PulsarError, OperationRetryOptions, Pulsar,
    TlsOptions as PulsarTlsOptions, TokioExecutor,
    producer::{Message as PulsarProducerMessage, SendFuture as PulsarSendFuture},
};
use tokio::time::{Instant, sleep};

const PULSAR: &str = "pulsar";

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
    position: SinkRecordPosition,
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
        outcome: &mut PerRecordOutcome,
    ) {
        for record in records {
            tokio::task::consume_budget().await;
            let position = record.position;
            let occurred_at = record.occurred_at;
            match self.producer.send_non_blocking(Self::message(record)).await {
                Ok(confirmation) => {
                    drop(confirmation);
                    outcome.deliver(position);
                }
                Err(source) if Self::is_record_rejection(&source) => {
                    outcome.reject(RejectedSinkRecord::external(
                        position,
                        occurred_at,
                        format!("pulsar rejected record: {source}"),
                    ));
                }
                Err(source) => {
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
        outcome: &mut PerRecordOutcome,
    ) {
        let mut pending: VecDeque<PendingPulsarConfirmation> = VecDeque::new();
        for record in records {
            tokio::task::consume_budget().await;
            let position = record.position;
            let occurred_at = record.occurred_at;
            let confirmation = match self.producer.send_non_blocking(Self::message(record)).await {
                Ok(confirmation) => confirmation,
                Err(source) if Self::is_record_rejection(&source) => {
                    outcome.reject(RejectedSinkRecord::external(
                        position,
                        occurred_at,
                        format!("pulsar rejected record: {source}"),
                    ));
                    continue;
                }
                Err(source) => {
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
                position,
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
            tokio::task::consume_budget().await;
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
        outcome: &mut PerRecordOutcome,
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
        let result = tokio::select! {
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
        let position = oldest.position;
        let occurred_at = oldest.occurred_at;
        match result {
            Ok(_receipt) => {
                pending.pop_front();
                outcome.deliver(position);
                Ok(())
            }
            Err(source) if Self::is_record_rejection(&source) => {
                pending.pop_front();
                outcome.reject(RejectedSinkRecord::external(
                    position,
                    occurred_at,
                    format!("pulsar rejected record: {source}"),
                ));
                Ok(())
            }
            Err(source) => {
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
                Ok(_receipt) => outcome.deliver(confirmation.position),
                Err(source) if Self::is_record_rejection(&source) => {
                    outcome.reject(RejectedSinkRecord::external(
                        confirmation.position,
                        confirmation.occurred_at,
                        format!("pulsar rejected record: {source}"),
                    ));
                }
                Err(_) => {}
            }
        }
    }

    fn is_record_rejection(_error: &PulsarError) -> bool {
        false
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

    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: PULSAR }).attach_printable(error.to_string())
    }
}

#[async_trait]
impl SinkLifecycle for PulsarSink {}

#[async_trait]
impl RecordSink for PulsarSink {
    async fn publish(&mut self, records: Vec<SinkRecord>) -> PerRecordOutcome {
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
    use pulsar::{
        error::ConnectionError as PulsarConnectionError,
        message::proto::ServerError as PulsarServerError,
    };
    use tempfile::tempdir;

    use super::*;

    fn server_error(kind: PulsarServerError) -> PulsarError {
        PulsarError::Connection(PulsarConnectionError::PulsarError(Some(kind), None))
    }

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn checksum_failure_remains_an_infrastructure_failure() {
        assert!(!PulsarSink::is_record_rejection(&server_error(
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
            assert!(!PulsarSink::is_record_rejection(&server_error(kind)));
        }
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
