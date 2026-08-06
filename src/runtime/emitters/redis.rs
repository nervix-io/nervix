use ::redis::{
    AsyncCommands, Client as RedisClient, ClientTlsConfig, ErrorKind as RedisErrorKind,
    ServerErrorKind, TlsCertificates as RedisTlsCertificates,
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

    pub(in crate::runtime) async fn publish_records(
        &mut self,
        channel: &Identifier,
        records: Vec<EncodedBrokerRecord>,
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        for record in records {
            tokio::task::consume_budget().await;
            let Some(connection) = self.connection.as_mut() else {
                outcome.fail(
                    Report::new(EmitterRuntimeError::SinkNotInitialized)
                        .attach_printable("no initialized redis sink client"),
                );
                break;
            };
            let published: ::redis::RedisResult<i64> = await_emitter_confirmation(
                &record.acks,
                connection.publish(channel.as_str(), record.payload.as_slice()),
            )
            .await;
            match published {
                Ok(_) => outcome.deliver((record.batch_index, record.row_index)),
                Err(error) if Self::is_record_failure(&error) => outcome.reject(
                    (record.batch_index, record.row_index),
                    format!("Redis rejected emitted record: {error}"),
                ),
                Err(error) => {
                    outcome.fail(emitter_publish_error(error));
                    break;
                }
            }
        }
        outcome
    }

    fn is_record_failure(error: &::redis::RedisError) -> bool {
        if !matches!(
            error.kind(),
            RedisErrorKind::Server(ServerErrorKind::ResponseError)
        ) {
            return false;
        }
        let detail = error.detail().unwrap_or_default().to_ascii_lowercase();
        detail.contains("protocol error: invalid bulk length")
            || detail.contains("string exceeds maximum allowed size")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_definitive_redis_command_rejections_are_record_failures() {
        let rejected = ::redis::RedisError::from((
            RedisErrorKind::Server(ServerErrorKind::ResponseError),
            "record rejected",
            "Protocol error: invalid bulk length".to_string(),
        ));
        let ambiguous = ::redis::RedisError::from((
            RedisErrorKind::Server(ServerErrorKind::ResponseError),
            "record rejected",
            "OOM command not allowed when used memory is greater than maxmemory".to_string(),
        ));
        let disconnected =
            ::redis::RedisError::from((RedisErrorKind::Io, "connection interrupted"));

        assert!(RedisEmitter::is_record_failure(&rejected));
        assert!(!RedisEmitter::is_record_failure(&ambiguous));
        assert!(!RedisEmitter::is_record_failure(&disconnected));
    }
}
