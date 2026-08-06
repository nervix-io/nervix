use async_nats::{Client as NatsClient, Subject, message::OutboundMessage};
use futures_util::SinkExt;

use super::*;

pub(in crate::runtime) struct NatsEmitter {
    client: Option<NatsClient>,
    subject: Subject,
}

impl NatsEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientNats,
        resolved: Option<&ResolvedClientConfig>,
        subject: &Identifier,
    ) -> EmitterRuntimeResult<Self> {
        let client = Self::client_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        )
        .await?;
        Ok(Self {
            client: Some(client),
            subject: Subject::from(subject.as_str().to_string()),
        })
    }

    async fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<NatsClient> {
        let addr = emitter_config_value(config, "addr", || {
            "missing NATS client config key 'addr'".to_string()
        })?;
        let mut options = async_nats::ConnectOptions::new();
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

    pub(in crate::runtime) async fn publish_chunk(
        &self,
        payloads: Vec<Vec<u8>>,
        headers: &[EmitterHeaders],
    ) -> EmitterRuntimeResult<()> {
        let Some(client) = self.client.as_ref() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized nats sink client"));
        };
        if payloads.len() != headers.len() {
            return Err(emitter_publish_error(
                "nats encoded payload and header counts differ",
            ));
        }
        let mut sink = client.clone();
        for (payload, headers) in payloads.into_iter().zip(headers) {
            tokio::task::consume_budget().await;
            let headers = if headers.is_empty() {
                None
            } else {
                let mut header_map = async_nats::HeaderMap::new();
                for (name, value) in headers {
                    header_map.append(name.as_str(), value.as_str());
                }
                Some(header_map)
            };
            sink.feed(OutboundMessage {
                subject: self.subject.clone(),
                reply: None,
                payload: payload.into(),
                headers,
            })
            .await
            .map_err(emitter_publish_error)?;
        }
        SinkExt::flush(&mut sink)
            .await
            .map_err(emitter_publish_error)?;
        Ok(())
    }
}
