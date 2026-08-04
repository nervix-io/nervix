use rdkafka::{
    config::ClientConfig,
    error::{KafkaError, RDKafkaErrorCode},
    message::{Header as KafkaHeader, OwnedHeaders},
    producer::{DeliveryFuture, FutureProducer, FutureRecord},
};

use super::*;

pub(in crate::runtime) struct KafkaEmitter {
    producer: Option<FutureProducer>,
}

impl KafkaEmitter {
    pub(in crate::runtime) fn new(
        client: &CreateClientKafka,
        resolved: Option<&ResolvedClientConfig>,
    ) -> EmitterRuntimeResult<Self> {
        let producer = Self::producer_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        )?;
        Ok(Self {
            producer: Some(producer),
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

    async fn await_delivery(delivery: DeliveryFuture) -> EmitterRuntimeResult<()> {
        delivery
            .await
            .map_err(|source| {
                emitter_publish_error(format!("kafka delivery channel closed: {source}"))
            })?
            .map(|_| ())
            .map_err(|(source, _)| emitter_publish_error(source))
    }

    pub(in crate::runtime) async fn publish_chunk(
        &self,
        topic: &Identifier,
        keys: &[Option<BranchKey>],
        payloads: Vec<Vec<u8>>,
        headers: &[EmitterHeaders],
        max_in_flight: usize,
    ) -> EmitterRuntimeResult<()> {
        let Some(producer) = self.producer.as_ref() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized kafka sink client"));
        };
        if payloads.len() != keys.len() || payloads.len() != headers.len() {
            return Err(emitter_publish_error(
                "kafka encoded payload, branch key, and header counts differ",
            ));
        }

        let mut deliveries = FuturesOrdered::new();
        for ((payload, key), headers) in payloads.into_iter().zip(keys).zip(headers) {
            tokio::task::consume_budget().await;
            let mut record = FutureRecord::<str, [u8]>::to(topic.as_str()).payload(&payload);
            if let Some(key) = key.as_ref() {
                record = record.key(key.as_str());
            }
            if !headers.is_empty() {
                let owned_headers = headers.iter().fold(
                    OwnedHeaders::new_with_capacity(headers.len()),
                    |owned_headers, (key, value)| {
                        owned_headers.insert(KafkaHeader {
                            key,
                            value: Some(value.as_str()),
                        })
                    },
                );
                record = record.headers(owned_headers);
            }

            let queue_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            let delivery = loop {
                match producer.send_result(record) {
                    Ok(delivery) => break delivery,
                    Err((source, returned_record)) => {
                        if let KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) = &source
                            && std::time::Instant::now() < queue_deadline
                        {
                            record = returned_record;
                            if let Some(delivery) = deliveries.next().await {
                                delivery?;
                            } else {
                                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            }
                            continue;
                        }
                        return Err(emitter_publish_error(source));
                    }
                }
            };
            deliveries.push_back(Self::await_delivery(delivery));
            if deliveries.len() >= max_in_flight {
                let delivery = deliveries
                    .next()
                    .await
                    .expect("kafka delivery queue must contain an in-flight publish");
                delivery?;
            }
        }
        while let Some(delivery) = deliveries.next().await {
            tokio::task::consume_budget().await;
            delivery?;
        }
        Ok(())
    }
}
