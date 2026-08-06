use futures_util::FutureExt;
use rdkafka::{
    config::ClientConfig,
    error::{KafkaError, RDKafkaErrorCode},
    message::{Header as KafkaHeader, OwnedHeaders},
    producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer},
};

use super::*;

pub(in crate::runtime) struct KafkaEmitter {
    producer: Option<FutureProducer>,
    mode: BrokerPublishingMode,
}

struct PendingKafkaConfirmation {
    position: BrokerRecordPosition,
    acks: AckSet,
    deadline: Instant,
    confirmation: DeliveryFuture,
}

impl KafkaEmitter {
    pub(super) fn new(
        client: &CreateClientKafka,
        resolved: Option<&ResolvedClientConfig>,
        mode: BrokerPublishingMode,
    ) -> EmitterRuntimeResult<Self> {
        let producer = Self::producer_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        )?;
        Ok(Self {
            producer: Some(producer),
            mode,
        })
    }

    fn producer_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<FutureProducer> {
        let mut client_config = ClientConfig::new();
        for entry in config {
            client_config.set(&entry.key, &entry.value);
        }
        client_config.create().map_err(emitter_init_error)
    }

    pub(super) async fn publish(
        &self,
        topic: &Identifier,
        records: Vec<EncodedBrokerRecord>,
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        let Some(producer) = self.producer.as_ref() else {
            outcome.fail(
                Report::new(EmitterRuntimeError::SinkNotInitialized)
                    .attach_printable("no initialized kafka sink client"),
            );
            return outcome;
        };

        outcome.delivered.reserve(records.len());
        let mut pending: VecDeque<PendingKafkaConfirmation> = VecDeque::new();
        for record in records {
            tokio::task::consume_budget().await;
            for confirmation in &pending {
                confirmation.acks.ack_alive();
            }
            record.acks.ack_alive();
            let position = (record.batch_index, record.row_index);
            let confirmation = match Self::enqueue(producer, topic, &record) {
                Ok(confirmation) => confirmation,
                Err(error) if Self::is_record_rejection(&error) => {
                    outcome.reject(position, format!("kafka rejected record: {error}"));
                    continue;
                }
                Err(error) => {
                    outcome.fail(emitter_publish_error(error));
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
                    pending.push_back(PendingKafkaConfirmation {
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

    fn enqueue(
        producer: &FutureProducer,
        topic: &Identifier,
        message: &EncodedBrokerRecord,
    ) -> Result<DeliveryFuture, KafkaError> {
        let mut record =
            FutureRecord::<str, [u8]>::to(topic.as_str()).payload(message.payload.as_slice());
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
        producer
            .send_result(record)
            .map_err(|(source, _record)| source)
    }

    async fn confirm_oldest(
        pending: &mut VecDeque<PendingKafkaConfirmation>,
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
                    "kafka acknowledgment window unexpectedly became empty",
                ));
            };
            let remaining = oldest
                .deadline
                .checked_duration_since(Instant::now())
                .unwrap_or(Duration::ZERO);
            if remaining.is_zero() {
                Self::harvest_ready_after_oldest_failure(pending, outcome);
                return Err(emitter_publish_error(format!(
                    "kafka delivery report exceeded ACK TIMEOUT {}",
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
                Ok(Ok(_delivery)) => {
                    pending.pop_front();
                    outcome.deliver(position);
                    return Ok(());
                }
                Ok(Err((source, _message))) if Self::is_record_rejection(&source) => {
                    pending.pop_front();
                    outcome.reject(position, format!("kafka rejected record: {source}"));
                    return Ok(());
                }
                Ok(Err((source, _message))) => {
                    Self::harvest_ready_after_oldest_failure(pending, outcome);
                    return Err(emitter_publish_error(source));
                }
                Err(source) => {
                    Self::harvest_ready_after_oldest_failure(pending, outcome);
                    return Err(emitter_publish_error(format!(
                        "kafka delivery report channel closed: {source}"
                    )));
                }
            }
        }
    }

    fn harvest_ready_after_oldest_failure(
        pending: &mut VecDeque<PendingKafkaConfirmation>,
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
                .expect("ready Kafka confirmation must remain in the window");
            match result {
                Ok(Ok(_delivery)) => outcome.deliver(confirmation.position),
                Ok(Err((source, _message))) if Self::is_record_rejection(&source) => outcome
                    .reject(
                        confirmation.position,
                        format!("kafka rejected record: {source}"),
                    ),
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

    pub(super) async fn flush_local_queue(&self, deadline: Instant) -> EmitterRuntimeResult<()> {
        let Some(producer) = self.producer.as_ref().cloned() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized kafka sink client"));
        };
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            return Err(emitter_publish_error(
                "kafka local producer queue drain deadline elapsed",
            ));
        }
        tokio::task::spawn_blocking(move || producer.flush(remaining))
            .await
            .map_err(|source| {
                emitter_publish_error(format!("kafka producer queue drain task failed: {source}"))
            })?
            .map_err(|source| {
                emitter_publish_error(format!(
                    "kafka local producer queue did not drain before shutdown: {source}"
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_and_invalid_records_are_definitive_rejections() {
        for code in [
            RDKafkaErrorCode::InvalidMessage,
            RDKafkaErrorCode::InvalidMessageSize,
            RDKafkaErrorCode::MessageSizeTooLarge,
            RDKafkaErrorCode::InvalidTimestamp,
            RDKafkaErrorCode::InvalidRecord,
        ] {
            assert!(KafkaEmitter::is_record_rejection(
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
            assert!(!KafkaEmitter::is_record_rejection(
                &KafkaError::MessageProduction(code)
            ));
        }
    }
}
