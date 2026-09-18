//! Kafka sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Kafka producer configuration, record and header publication, delivery-report
//!   classification, and producer queue shutdown.
//! - **Depends on.** The connector contract, vocabulary values, `error-stack`, Tokio, and
//!   `rust-rdkafka`.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use std::{collections::VecDeque, time::Duration};

use async_trait::async_trait;
use error_stack::Report;
use futures_util::FutureExt;
use meticulous::OptionExt as _;
use nervix_connector::{
    AckConfirmation, BrokerPublishingMode, PerRecordOutcome, RecordSink, RejectedSinkRecord,
    SinkHost, SinkLifecycle, SinkPublishError, SinkPublishResult, SinkRecord, SinkRecordPosition,
    SinkStartError, SinkStartResult,
};
use nervix_models::{ClientConfigEntry, Timestamp, TopicName};
use rdkafka::{
    config::ClientConfig,
    error::{KafkaError, RDKafkaErrorCode},
    message::{Header as KafkaHeader, OwnedHeaders},
    producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer},
};
use tokio::time::{Instant, sleep};

const KAFKA: &str = "kafka";

pub struct KafkaSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub topic: TopicName,
    pub mode: BrokerPublishingMode,
}

pub struct KafkaSink {
    producer: FutureProducer,
    topic: TopicName,
    mode: BrokerPublishingMode,
}

struct PendingKafkaConfirmation {
    position: SinkRecordPosition,
    occurred_at: Timestamp,
    deadline: Instant,
    confirmation: DeliveryFuture,
}

impl KafkaSink {
    pub fn new(config: KafkaSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let producer = Self::producer_from_config(&config.config)?;
        Ok(Self {
            producer,
            topic: config.topic,
            mode: config.mode,
        })
    }

    fn producer_from_config(config: &[ClientConfigEntry]) -> SinkStartResult<FutureProducer> {
        let mut client_config = ClientConfig::new();
        for entry in config {
            client_config.set(&entry.key, &entry.value);
        }
        client_config.create().map_err(|source| {
            Report::new(SinkStartError::Initialize { sink: KAFKA }).attach_printable(source)
        })
    }

    async fn publish_unconfirmed(&self, records: Vec<SinkRecord>, outcome: &mut PerRecordOutcome) {
        for record in records {
            tokio::task::consume_budget().await;
            match self.enqueue(&record) {
                Ok(confirmation) => {
                    drop(confirmation);
                    outcome.deliver(record.position);
                }
                Err(error) if Self::is_record_rejection(&error) => {
                    outcome.reject(record.rejected(format!("kafka rejected record: {error}")));
                }
                Err(error) => {
                    outcome.fail(Self::publish_error(error));
                    return;
                }
            }
        }
    }

    async fn publish_confirmed(
        &self,
        records: Vec<SinkRecord>,
        AckConfirmation {
            max_in_flight,
            timeout,
        }: AckConfirmation,
        outcome: &mut PerRecordOutcome,
    ) {
        let mut pending: VecDeque<PendingKafkaConfirmation> = VecDeque::new();
        for record in records {
            tokio::task::consume_budget().await;
            let confirmation = match self.enqueue(&record) {
                Ok(confirmation) => confirmation,
                Err(error) if Self::is_record_rejection(&error) => {
                    outcome.reject(record.rejected(format!("kafka rejected record: {error}")));
                    continue;
                }
                Err(error) => {
                    outcome.fail(Self::publish_error(error));
                    return;
                }
            };
            let Some(deadline) = Instant::now().checked_add(timeout) else {
                outcome.fail(Self::publish_error(
                    "kafka ACK TIMEOUT exceeds the monotonic clock range",
                ));
                return;
            };
            pending.push_back(PendingKafkaConfirmation {
                position: record.position,
                occurred_at: record.occurred_at,
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

    fn enqueue(&self, message: &SinkRecord) -> Result<DeliveryFuture, KafkaError> {
        let mut record =
            FutureRecord::<str, [u8]>::to(self.topic.as_str()).payload(message.payload.as_slice());
        if let Some(key) = message.key.as_deref() {
            record = record.key(key);
        }
        if !message.headers.is_empty() {
            let owned_headers = message.headers.iter().fold(
                OwnedHeaders::new_with_capacity(message.headers.len()),
                |owned_headers, (key, value)| {
                    owned_headers.insert(KafkaHeader {
                        key,
                        value: Some(value.as_str()),
                    })
                },
            );
            record = record.headers(owned_headers);
        }
        self.producer
            .send_result(record)
            .map_err(|(source, _record)| source)
    }

    async fn confirm_oldest(
        pending: &mut VecDeque<PendingKafkaConfirmation>,
        timeout: Duration,
        outcome: &mut PerRecordOutcome,
    ) -> SinkPublishResult<()> {
        let Some(oldest) = pending.front_mut() else {
            return Err(Self::publish_error(
                "kafka acknowledgment window unexpectedly became empty",
            ));
        };
        let remaining = oldest
            .deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            Self::harvest_ready_after_oldest_failure(pending, outcome);
            return Err(Self::publish_error(format!(
                "kafka delivery report exceeded ACK TIMEOUT {}",
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
                "kafka delivery report exceeded ACK TIMEOUT {}",
                humantime::format_duration(timeout)
            )));
        };
        let position = oldest.position;
        let occurred_at = oldest.occurred_at;
        match result {
            Ok(Ok(_delivery)) => {
                pending.pop_front();
                outcome.deliver(position);
                Ok(())
            }
            Ok(Err((source, _message))) if Self::is_record_rejection(&source) => {
                pending.pop_front();
                outcome.reject(RejectedSinkRecord::external(
                    position,
                    occurred_at,
                    format!("kafka rejected record: {source}"),
                ));
                Ok(())
            }
            Ok(Err((source, _message))) => {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                Err(Self::publish_error(source))
            }
            Err(source) => {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                Err(Self::publish_error(format!(
                    "kafka delivery report channel closed: {source}"
                )))
            }
        }
    }

    fn harvest_ready_after_oldest_failure(
        pending: &mut VecDeque<PendingKafkaConfirmation>,
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
                Ok(Ok(_delivery)) => outcome.deliver(confirmation.position),
                Ok(Err((source, _message))) if Self::is_record_rejection(&source) => {
                    outcome.reject(RejectedSinkRecord::external(
                        confirmation.position,
                        confirmation.occurred_at,
                        format!("kafka rejected record: {source}"),
                    ));
                }
                Ok(Err(_)) | Err(_) => {}
            }
        }
    }

    fn is_record_rejection(error: &KafkaError) -> bool {
        let KafkaError::MessageProduction(code) = error else {
            return false;
        };
        matches!(
            code,
            RDKafkaErrorCode::InvalidMessage
                | RDKafkaErrorCode::InvalidMessageSize
                | RDKafkaErrorCode::MessageSizeTooLarge
                | RDKafkaErrorCode::InvalidTimestamp
                | RDKafkaErrorCode::InvalidRecord
        )
    }

    fn publish_error(error: impl std::fmt::Display) -> Report<SinkPublishError> {
        Report::new(SinkPublishError::Publish { sink: KAFKA }).attach_printable(error.to_string())
    }
}

#[async_trait]
impl SinkLifecycle for KafkaSink {
    async fn finish(&mut self, deadline: Instant) -> SinkPublishResult<()> {
        let producer = self.producer.clone();
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            return Err(Report::new(SinkPublishError::Finish { sink: KAFKA })
                .attach_printable("kafka local producer queue drain deadline elapsed"));
        }
        tokio::task::spawn_blocking(move || producer.flush(remaining))
            .await
            .map_err(|source| {
                Report::new(SinkPublishError::Finish { sink: KAFKA })
                    .attach_printable(format!("kafka producer queue drain task failed: {source}"))
            })?
            .map_err(|source| {
                Report::new(SinkPublishError::Finish { sink: KAFKA }).attach_printable(format!(
                    "kafka local producer queue did not drain before shutdown: {source}"
                ))
            })
    }

    fn keeps_client_on_publish_failure(&self) -> bool {
        true
    }
}

#[async_trait]
impl RecordSink for KafkaSink {
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
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    use super::KafkaSink;

    #[test]
    fn oversized_and_invalid_records_are_definitive_rejections() {
        for code in [
            RDKafkaErrorCode::InvalidMessage,
            RDKafkaErrorCode::InvalidMessageSize,
            RDKafkaErrorCode::MessageSizeTooLarge,
            RDKafkaErrorCode::InvalidTimestamp,
            RDKafkaErrorCode::InvalidRecord,
        ] {
            assert!(KafkaSink::is_record_rejection(
                &KafkaError::MessageProduction(code)
            ));
        }
    }

    #[test]
    fn broker_availability_errors_remain_infrastructure_failures() {
        for code in [
            RDKafkaErrorCode::QueueFull,
            RDKafkaErrorCode::UnknownTopicOrPartition,
            RDKafkaErrorCode::AllBrokersDown,
            RDKafkaErrorCode::MessageTimedOut,
        ] {
            assert!(!KafkaSink::is_record_rejection(
                &KafkaError::MessageProduction(code)
            ));
        }
    }
}
