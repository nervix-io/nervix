use rdkafka::{
    config::ClientConfig,
    error::{KafkaError, RDKafkaErrorCode},
    message::{Header as KafkaHeader, OwnedHeaders},
    producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
};

use super::*;

type KafkaProducer = ThreadedProducer<DefaultProducerContext>;

pub(in crate::runtime) struct KafkaEmitter {
    producer: KafkaProducer,
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
        Ok(Self { producer })
    }

    fn producer_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<KafkaProducer> {
        let mut client_config = ClientConfig::new();
        for entry in config {
            client_config.set(&entry.key, &entry.value);
        }
        client_config.create().map_err(emitter_init_error)
    }

    pub(in crate::runtime) async fn publish_chunk(
        &self,
        topic: &Identifier,
        keys: &[Option<BranchKey>],
        payloads: Vec<Vec<u8>>,
        headers: &[EmitterHeaders],
    ) -> EmitterRuntimeResult<()> {
        if payloads.len() != keys.len() || payloads.len() != headers.len() {
            return Err(emitter_publish_error(
                "kafka encoded payload, branch key, and header counts differ",
            ));
        }

        for ((payload, key), headers) in payloads.into_iter().zip(keys).zip(headers) {
            tokio::task::consume_budget().await;
            self.publish_message(topic, key.as_ref(), &payload, headers)
                .await?;
        }
        Ok(())
    }

    pub(in crate::runtime) async fn publish_message(
        &self,
        topic: &Identifier,
        key: Option<&BranchKey>,
        payload: &[u8],
        headers: &EmitterHeaders,
    ) -> EmitterRuntimeResult<()> {
        let mut record = BaseRecord::<str, [u8]>::to(topic.as_str()).payload(payload);
        if let Some(key) = key {
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
        loop {
            tokio::task::consume_budget().await;
            match self.producer.send(record) {
                Ok(()) => return Ok(()),
                Err((source, returned_record)) => {
                    if let KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) = &source
                        && std::time::Instant::now() < queue_deadline
                    {
                        record = returned_record;
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        continue;
                    }
                    return Err(emitter_publish_error(source));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_emitter_has_an_initialized_producer_by_construction() {
        let producer = ClientConfig::new()
            .create::<KafkaProducer>()
            .expect("Kafka producer construction is lazy");
        let emitter = KafkaEmitter { producer };

        let _: &KafkaProducer = &emitter.producer;
    }

    #[tokio::test]
    async fn kafka_publish_is_fire_and_forget_after_local_enqueue() {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:1")
            .set("message.timeout.ms", "10000")
            .create::<KafkaProducer>()
            .expect("Kafka producer construction is lazy");
        let emitter = KafkaEmitter { producer };
        let topic = Identifier::parse("fire_and_forget").expect("valid topic");

        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            emitter.publish_chunk(
                &topic,
                &[None],
                vec![br#"{"value":"queued"}"#.to_vec()],
                &[Vec::new()],
            ),
        )
        .await
        .expect("publishing must not wait for broker delivery")
        .expect("the record must enter the local producer queue");
    }
}
