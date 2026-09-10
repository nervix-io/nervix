use ::redis::{
    AsyncCommands, Client as RedisClient, ClientTlsConfig, ErrorKind as RedisErrorKind,
    ServerErrorKind, TlsCertificates as RedisTlsCertificates,
};
use nervix_models::ChannelName;

use super::*;

/// The node's shared pool of Redis command connections for one named client.
///
/// Every entry is an independent connection, so the declared maximum bounds sockets rather than
/// handles, and a checked-out connection carries one publisher's outstanding operation alone.
pub(in crate::runtime) type RedisCommandPool = bb8::Pool<RedisClient>;

/// One command connection borrowed from that pool, returned when it is dropped.
pub(in crate::runtime) type RedisPooledConnection<'pool> =
    bb8::PooledConnection<'pool, RedisClient>;

/// Open the node's shared Redis command pool for one named client, sized by its declared bounds.
///
/// Subscription connections are not drawn from here: a Redis Pub/Sub ingestor owns its own
/// dedicated connection, so command-pool pressure never interrupts a healthy subscription.
pub(in crate::runtime) async fn open_redis_command_pool(
    config: &[nervix_models::ClientConfigEntry],
    bounds: ClientPoolBounds,
) -> Result<RedisCommandPool, Report<OpenClientError>> {
    let Some(addr) = optional_client_config_value(config, "addr") else {
        return Err(Report::new(OpenClientError::MissingConfig {
            transport: "Redis",
            key: "addr",
        }));
    };
    let client = RedisEmitter::client_from_config(addr, config).map_err(|error| {
        Report::new(OpenClientError::InvalidConfig {
            transport: "Redis",
            reason: emitter_error_message(&error),
        })
    })?;
    bb8::Pool::builder()
        .max_size(bounds.maximum().get())
        .min_idle(Some(bounds.minimum()))
        .build(client)
        .await
        .map_err(|source| {
            Report::new(OpenClientError::Connect {
                transport: "Redis",
                reason: source.to_string(),
            })
        })
}

pub(in crate::runtime) struct RedisEmitter {
    client: Option<RedisEmitterClient>,
}

/// This publisher's interest in the node's shared Redis command pool.
///
/// Each pooled entry is its own connection rather than another handle to one multiplexed
/// connection, so the declared maximum bounds physical connections and two concurrent publishers
/// never share one.
struct RedisEmitterClient {
    lease: SharedClientLease,
    /// The client borrowed from, named in this emitter's diagnostics and in its pool wait.
    client: ClientName,
    runtime: Runtime,
    /// This emitter, as the key its pool wait is recorded under for `DESCRIBE` to read.
    waiter: DomainNodeRef,
}

impl RedisEmitterClient {
    /// Borrow a command connection for one publish, reporting the wait until the pool hands one
    /// over. The connection returns to the shared pool as soon as the publish completes.
    async fn connection(&self) -> EmitterRuntimeResult<RedisPooledConnection<'_>> {
        let pool = self
            .lease
            .client()
            .redis(&self.client)
            .map_err(|error| emitter_init_error(error.to_string()))?;
        let waiting = self.runtime.pool_wait_guard(&self.waiter, &self.client);
        let connection = pool.get().await.map_err(emitter_init_error);
        drop(waiting);
        connection
    }
}

impl RedisEmitter {
    pub(in crate::runtime) async fn new(
        model: &Model,
        client: &CreateClientRedis,
        resolved: Option<&ResolvedClientConfig>,
        context: &EmitterSinkContext,
    ) -> EmitterRuntimeResult<Self> {
        let lease = context
            .runtime
            .lease_shared_client(&context.domain, &client.name, model, resolved)
            .await
            .map_err(|error| emitter_init_error(error.to_string()))?;
        Ok(Self {
            client: Some(RedisEmitterClient {
                lease,
                client: client.name.clone(),
                runtime: context.runtime.clone(),
                waiter: DomainNodeRef::node_in(
                    context.domain.clone(),
                    ModelKind::Emitter,
                    context.emitter.clone(),
                ),
            }),
        })
    }

    pub(in crate::runtime) fn client_from_config(
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
        channel: &ChannelName,
        records: Vec<EncodedBrokerRecord>,
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        for record in records {
            tokio::task::consume_budget().await;
            let Some(client) = self.client.as_ref() else {
                outcome.fail(
                    Report::new(EmitterRuntimeError::SinkNotInitialized)
                        .attach_printable("no initialized redis sink client"),
                );
                break;
            };
            // Borrowed per publish and returned with the guard: an emitter between publishes, or
            // waiting out a flush interval, holds no connection at all.
            let mut connection = match client.connection().await {
                Ok(connection) => connection,
                Err(error) => {
                    outcome.fail(error);
                    break;
                }
            };
            let published: ::redis::RedisResult<i64> = await_emitter_confirmation(
                &record.acks,
                connection.publish(channel.as_str(), record.payload.as_slice()),
            )
            .await;
            match published {
                Ok(_) => outcome.deliver(record.position()),
                Err(error) if Self::is_record_failure(&error) => outcome.reject(
                    record.position(),
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
