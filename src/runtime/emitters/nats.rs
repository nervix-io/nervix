use async_nats::Client as NatsClient;

use super::*;

pub(in crate::runtime) struct NatsEmitter {
    client: Option<NatsClient>,
}

impl NatsEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientNats,
        resolved: Option<&ResolvedClientConfig>,
    ) -> EmitterRuntimeResult<Self> {
        let client = Self::client_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        )
        .await?;
        Ok(Self {
            client: Some(client),
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

    pub(in crate::runtime) async fn publish(
        &mut self,
        subject: &Identifier,
        payload: &[u8],
        headers: &EmitterHeaders,
    ) -> EmitterRuntimeResult<()> {
        let Some(client) = self.client.as_mut() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized nats sink client"));
        };
        if headers.is_empty() {
            client
                .publish(subject.as_str().to_string(), payload.to_vec().into())
                .await
                .map_err(emitter_publish_error)
        } else {
            let mut header_map = async_nats::HeaderMap::new();
            for (name, value) in headers {
                header_map.append(name.as_str(), value.as_str());
            }
            client
                .publish_with_headers(
                    subject.as_str().to_string(),
                    header_map,
                    payload.to_vec().into(),
                )
                .await
                .map_err(emitter_publish_error)
        }
    }
}
