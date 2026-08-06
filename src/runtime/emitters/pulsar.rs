use ::pulsar::{
    Pulsar, TlsOptions as PulsarTlsOptions, TokioExecutor,
    producer::Message as PulsarProducerMessage,
};

use super::*;

pub(in crate::runtime) struct PulsarEmitter {
    producer: Option<::pulsar::Producer<TokioExecutor>>,
}

impl PulsarEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientPulsar,
        resolved: Option<&ResolvedClientConfig>,
        topic: &Identifier,
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
        let mut builder = Pulsar::builder(addr, TokioExecutor);
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

    pub(in crate::runtime) async fn publish_chunk(
        &mut self,
        keys: &[Option<BranchKey>],
        payloads: Vec<Vec<u8>>,
        headers: &[EmitterHeaders],
        max_in_flight: usize,
    ) -> EmitterRuntimeResult<()> {
        let Some(producer) = self.producer.as_mut() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized pulsar sink client"));
        };
        if payloads.len() != keys.len() || payloads.len() != headers.len() {
            return Err(emitter_publish_error(
                "pulsar encoded payload, branch key, and header counts differ",
            ));
        }
        let mut receipts = FuturesOrdered::new();
        for ((payload, key), headers) in payloads.into_iter().zip(keys).zip(headers) {
            tokio::task::consume_budget().await;
            let receipt = producer
                .send_non_blocking(PulsarProducerMessage {
                    payload,
                    properties: headers.iter().cloned().collect(),
                    partition_key: key.as_ref().map(|key| key.as_str().to_string()),
                    ..Default::default()
                })
                .await
                .map_err(|source| {
                    emitter_publish_error(format!("failed to enqueue pulsar message: {source}"))
                })?;
            receipts.push_back(receipt);
            if receipts.len() >= max_in_flight {
                receipts
                    .next()
                    .await
                    .expect("pulsar receipt queue must contain an in-flight publish")
                    .map_err(emitter_publish_error)?;
            }
        }
        while let Some(receipt) = receipts.next().await {
            tokio::task::consume_budget().await;
            receipt.map_err(emitter_publish_error)?;
        }
        Ok(())
    }
}
