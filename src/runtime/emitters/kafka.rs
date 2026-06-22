use rdkafka::{
    config::ClientConfig,
    message::{Header as KafkaHeader, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
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

    pub(in crate::runtime) async fn publish(
        &self,
        topic: &Identifier,
        message: &RelayMessage,
        payload: &[u8],
        headers: &EmitterHeaders,
    ) -> EmitterRuntimeResult<()> {
        let Some(producer) = self.producer.as_ref() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized kafka sink client"));
        };
        let mut record = FutureRecord::<str, [u8]>::to(topic.as_str()).payload(payload);
        if let Some(key) = message.key.as_ref() {
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
        producer
            .send(record, std::time::Duration::from_secs(5))
            .await
            .map(|_| ())
            .map_err(|(source, _)| emitter_publish_error(source))
    }
}
