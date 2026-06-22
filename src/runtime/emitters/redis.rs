use ::redis::{
    AsyncCommands, Client as RedisClient, ClientTlsConfig, TlsCertificates as RedisTlsCertificates,
};

use super::*;

pub(in crate::runtime) struct RedisEmitter {
    connection: Option<::redis::aio::MultiplexedConnection>,
}

impl RedisEmitter {
    pub(in crate::runtime) async fn new(
        client: &CreateClientRedis,
        resolved: Option<&ResolvedClientConfig>,
    ) -> EmitterRuntimeResult<Self> {
        let config = resolved
            .map(|config| config.entries.as_slice())
            .unwrap_or(client.config.as_slice());
        let connection = Self::connection_from_config(config).await?;
        Ok(Self {
            connection: Some(connection),
        })
    }

    async fn connection_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<::redis::aio::MultiplexedConnection> {
        let addr = emitter_config_value(config, "addr", || {
            "missing Redis client config key 'addr'".to_string()
        })?;
        let client = Self::client_from_config(&addr, config)?;
        client
            .get_multiplexed_async_connection()
            .await
            .map_err(emitter_init_error)
    }

    fn client_from_config(
        addr: &str,
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<RedisClient> {
        let tls = client_tls_paths(config);
        if emitter_service_url_has_scheme(addr, "Redis addr", "rediss")?
            && (tls.ca_file.is_some() || tls.cert_file.is_some() || tls.key_file.is_some())
        {
            RedisClient::build_with_tls(
                addr,
                RedisTlsCertificates {
                    client_tls: match (&tls.cert_file, &tls.key_file) {
                        (Some(cert_file), Some(key_file)) => Some(ClientTlsConfig {
                            client_cert: emitter_read_tls_file(cert_file, "TLS certificate")?,
                            client_key: emitter_read_tls_file(key_file, "TLS private key")?,
                        }),
                        (None, None) => None,
                        _ => {
                            return Err(emitter_config_error(
                                "Redis TLS client authentication requires both 'tls_cert_file' \
                                 and 'tls_key_file'",
                            ));
                        }
                    },
                    root_cert: match tls.ca_file.as_ref() {
                        Some(ca_file) => {
                            Some(emitter_read_tls_file(ca_file, "TLS CA certificate")?)
                        }
                        None => None,
                    },
                },
            )
            .map_err(emitter_init_error)
        } else {
            RedisClient::open(addr).map_err(emitter_init_error)
        }
    }

    pub(in crate::runtime) async fn publish(
        &mut self,
        channel: &Identifier,
        payload: &[u8],
    ) -> EmitterRuntimeResult<()> {
        let Some(connection) = self.connection.as_mut() else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized redis sink client"));
        };
        let _: i64 = connection
            .publish(channel.as_str(), payload)
            .await
            .map_err(emitter_publish_error)?;
        Ok(())
    }
}
