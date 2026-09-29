//! Redis Pub/Sub source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The dedicated subscription connection each source instance reads a channel
//!   through, including DNS, TCP, TLS, and Redis's own protocol setup.
//! - **Depends on.** The connector contract, node DNS resolver, typed client configuration
//!   entries, `error-stack`, Tokio and `redis`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

use std::{io, sync::Arc as StdArc};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::StreamExt as _;
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, NoIngestHeaders,
    RustlsClientConfigSource, SourceBatch, SourceBatchRequest, SourceConnector, SourceError,
    SourceMessage, SourceResult, SourceResume, client_config_value,
};
use nervix_dns::{ConnectionBudget, DnsLookupFailure, DnsResolver};
use nervix_models::{ChannelName, ClientConfigEntry};
use redis::{
    Client as RedisClient, ConnectionAddr, Msg,
    aio::{PubSub, PubSubStream},
};
use rustls_pki_types::ServerName;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::TlsConnector;

use crate::{CONNECT_BUDGET, redis_client};

const REDIS: &str = "redis";

/// Why a Redis Pub/Sub source could not open its subscription.
#[derive(Debug, Error)]
pub enum RedisPubSubSourceError {
    #[error("invalid Redis client configuration")]
    ClientConfig,
    #[error("failed to build Redis client")]
    BuildClient,
    #[error("resolving Redis subscription host '{host}' failed: {failure}")]
    Resolve {
        host: String,
        failure: DnsLookupFailure,
    },
    #[error("failed to connect Redis subscription: {reason}")]
    Connect { reason: String },
    #[error("unsupported Redis connection address")]
    UnsupportedAddress,
    #[error("failed to subscribe to Redis channel '{channel}'")]
    Subscribe { channel: String },
    #[error("Redis subscription closed")]
    SubscriptionClosed,
}

type RedisPubSubSourceResult<T> = Result<T, Report<RedisPubSubSourceError>>;

/// What one Redis Pub/Sub source subscribes to: its configured client, node resolver, and channel.
#[derive(Debug, Clone)]
pub struct RedisPubSubSourcePlan {
    client: RedisClient,
    dns: DnsResolver,
    tls: Option<StdArc<rustls::ClientConfig>>,
    channel: ChannelName,
}

trait RedisStream: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> RedisStream for T {}
type ConnectedStream = Box<dyn RedisStream + Send + Unpin>;

impl RedisPubSubSourcePlan {
    pub fn new(
        config: Vec<ClientConfigEntry>,
        channel: ChannelName,
        dns: DnsResolver,
    ) -> RedisPubSubSourceResult<Self> {
        let addr = client_config_value(&config, "addr", "Redis")
            .change_context(RedisPubSubSourceError::ClientConfig)?;
        let client =
            redis_client(&addr, &config).change_context(RedisPubSubSourceError::BuildClient)?;
        let tls = match client.get_connection_info().addr() {
            ConnectionAddr::TcpTls { .. } => {
                let config = RustlsClientConfigSource::new(&config)
                    .build_with_replacement_ca()
                    .change_context(RedisPubSubSourceError::BuildClient)?;
                Some(config)
            }
            _ => None,
        };
        Ok(Self {
            client,
            dns,
            tls,
            channel,
        })
    }

    async fn subscribe(&self) -> RedisPubSubSourceResult<PubSubStream> {
        let budget = ConnectionBudget::start(CONNECT_BUDGET);
        let stream = self.connect_with_budget(&budget).await?;
        let mut pubsub = timeout(
            budget.remaining(),
            PubSub::new(self.client.get_connection_info().redis_settings(), stream),
        )
        .await
        .map_err(|source| {
            Report::new(RedisPubSubSourceError::Connect {
                reason: source.to_string(),
            })
        })?
        .map_err(|source| {
            Report::new(RedisPubSubSourceError::Connect {
                reason: source.to_string(),
            })
        })?;
        timeout(budget.remaining(), pubsub.subscribe(self.channel.as_str()))
            .await
            .map_err(|source| {
                Report::new(RedisPubSubSourceError::Subscribe {
                    channel: self.channel.as_str().to_string(),
                })
                .attach_printable(source.to_string())
            })?
            .map_err(|source| {
                Report::new(RedisPubSubSourceError::Subscribe {
                    channel: self.channel.as_str().to_string(),
                })
                .attach_printable(source.to_string())
            })?;
        Ok(pubsub.into_on_message())
    }

    /// Open a fresh stream for every resume. Redis's Pub/Sub convenience method uses its default
    /// system resolver, so the connector opens the transport and lets Redis set up the protocol.
    #[cfg(test)]
    async fn connect(&self) -> RedisPubSubSourceResult<ConnectedStream> {
        let budget = ConnectionBudget::start(CONNECT_BUDGET);
        self.connect_with_budget(&budget).await
    }

    async fn connect_with_budget(
        &self,
        budget: &ConnectionBudget,
    ) -> RedisPubSubSourceResult<ConnectedStream> {
        match self.client.get_connection_info().addr() {
            ConnectionAddr::Tcp(host, port) => self.connect_tcp(host, *port, None, budget).await,
            ConnectionAddr::TcpTls {
                host,
                port,
                insecure,
                ..
            } => {
                if *insecure {
                    return Err(Report::new(RedisPubSubSourceError::BuildClient)
                        .attach_printable("insecure Redis TLS is not supported"));
                }
                self.connect_tcp(host, *port, self.tls.as_ref(), budget)
                    .await
            }
            #[cfg(unix)]
            ConnectionAddr::Unix(path) => {
                let stream = timeout(budget.remaining(), tokio::net::UnixStream::connect(path))
                    .await
                    .map_err(|source| {
                        Report::new(RedisPubSubSourceError::Connect {
                            reason: source.to_string(),
                        })
                    })?
                    .map_err(|source| {
                        Report::new(RedisPubSubSourceError::Connect {
                            reason: source.to_string(),
                        })
                    })?;
                Ok(Box::new(stream))
            }
            _ => Err(Report::new(RedisPubSubSourceError::UnsupportedAddress)),
        }
    }

    async fn connect_tcp(
        &self,
        host: &str,
        port: u16,
        tls: Option<&StdArc<rustls::ClientConfig>>,
        budget: &ConnectionBudget,
    ) -> RedisPubSubSourceResult<ConnectedStream> {
        let addresses = self
            .dns
            .resolve(host, port, budget.remaining())
            .await
            .map_err(|error| {
                let lookup = error.current_context();
                let host = lookup.name().to_string();
                let failure = lookup.failure();
                error.change_context(RedisPubSubSourceError::Resolve { host, failure })
            })?;
        let mut last_failure = None;
        for attempt in budget.attempts(&addresses) {
            nervix_primitives::task::consume_budget().await;
            let connected = timeout(attempt.budget, async {
                let stream = TcpStream::connect(attempt.address).await?;
                let Some(config) = tls else {
                    return Ok::<ConnectedStream, io::Error>(Box::new(stream));
                };
                let server_name =
                    ServerName::try_from(host.to_string()).map_err(io::Error::other)?;
                let connector = TlsConnector::from(config.clone());
                let stream = connector.connect(server_name, stream).await?;
                Ok(Box::new(stream))
            })
            .await;
            match connected {
                Ok(Ok(stream)) => return Ok(stream),
                Ok(Err(error)) => last_failure = Some(error.to_string()),
                Err(error) => last_failure = Some(error.to_string()),
            }
        }
        let failure =
            last_failure.unwrap_or_else(|| "no Redis address accepted a connection".to_string());
        Err(Report::new(RedisPubSubSourceError::Connect {
            reason: failure,
        }))
    }
}

/// One received channel message. Redis Pub/Sub carries no headers.
pub struct RedisPubSubSourceMessage {
    message: Msg,
    position: (),
    headers: NoIngestHeaders,
}

impl SourceMessage for RedisPubSubSourceMessage {
    type Position = ();

    fn payload(&self) -> &[u8] {
        self.message.get_payload_bytes()
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.headers
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow::Headers {
            headers: &self.headers,
        }
    }
}

/// One Redis Pub/Sub source instance: its subscription stream, absent while reconnecting.
pub struct RedisPubSubSource {
    plan: RedisPubSubSourcePlan,
    stream: Option<PubSubStream>,
}

#[async_trait]
impl SourceConnector for RedisPubSubSource {
    type Plan = RedisPubSubSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        Ok(Self {
            plan: plan.clone(),
            stream: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.stream.is_none()
    }

    async fn suspend(&mut self) -> SourceResult<()> {
        self.stream = None;
        Ok(())
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if self.stream.is_none() {
            let stream = self
                .plan
                .subscribe()
                .await
                .change_context(SourceError::Resume { connector: REDIS })?;
            self.stream = Some(stream);
        }
        Ok(SourceResume::Ready)
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.stream = None;
        Ok(())
    }
}

#[async_trait]
impl BrokerSourceConnector for RedisPubSubSource {
    type Message = RedisPubSubSourceMessage;
    type Position = ();

    async fn next_batch(
        &mut self,
        _request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(SourceBatch::ResumeRequired);
        };
        let Some(message) = stream.next().await else {
            self.stream = None;
            return Err(Report::new(RedisPubSubSourceError::SubscriptionClosed)
                .change_context(SourceError::Read { connector: REDIS }));
        };
        Ok(SourceBatch::Messages(vec![RedisPubSubSourceMessage {
            message,
            position: (),
            headers: NoIngestHeaders,
        }]))
    }

    async fn acknowledge(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }

    async fn reject(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        time::Duration,
    };

    use nervix_dns::{DnsLookupError, DnsLookupFailure};
    use nervix_test_environment::dns_authority::DnsAnswer;
    use tokio::net::TcpListener;

    use super::*;
    use crate::test_fixture::Fixture;

    fn plan(addr: &str, dns: DnsResolver) -> RedisPubSubSourcePlan {
        RedisPubSubSourcePlan::new(
            vec![ClientConfigEntry {
                key: "addr".to_string(),
                value: addr.to_string(),
            }],
            ChannelName::parse("events").expect("a plain channel name parses"),
            dns,
        )
        .expect("the Redis address is valid")
    }

    #[nervix_primitives::test]
    async fn the_plan_requires_the_address_before_it_subscribes() {
        let channel = ChannelName::parse("events").expect("a plain channel name parses");
        let fixture = Fixture::start().await;
        let dns = fixture.dns;
        let error = RedisPubSubSourcePlan::new(Vec::new(), channel, dns)
            .expect_err("a missing Redis address must fail");
        assert!(matches!(
            error.current_context(),
            RedisPubSubSourceError::ClientConfig
        ));
    }

    #[nervix_primitives::test]
    async fn a_subscription_dials_the_next_answer_when_the_first_refuses() {
        let fixture = Fixture::start().await;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("a loopback listener can bind");
        let port = listener
            .local_addr()
            .expect("listener has an address")
            .port();
        let name = "redis.nervix.test";
        fixture.answer(
            name,
            vec![
                IpAddr::V4(Ipv4Addr::new(127, 0, 7, 1)),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
            ],
        );
        let plan = plan(&format!("redis://{name}:{port}"), fixture.dns.clone());
        let stream = plan
            .connect()
            .await
            .expect("the second address accepts the subscription stream");
        let (_accepted, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("the accepted connection arrives")
            .expect("the listener accepts it");
        drop(stream);
        assert!(fixture.authority.questions_for(name) > 0);
    }

    #[nervix_primitives::test]
    async fn a_subscription_keeps_a_missing_name_as_its_typed_cause() {
        let fixture = Fixture::start().await;
        let name = "missing.nervix.test";
        fixture.authority.set(
            name,
            DnsAnswer::NameNotFound {
                negative_ttl: Duration::from_secs(1),
            },
        );
        let plan = plan(&format!("redis://{name}:6379"), fixture.dns);
        let error = match plan.connect().await {
            Ok(_) => panic!("the missing name cannot open a subscription stream"),
            Err(error) => error,
        };
        assert!(matches!(
            error.downcast_ref::<DnsLookupError>(),
            Some(lookup) if lookup.name() == name
                && lookup.failure() == DnsLookupFailure::NameNotFound
        ));
    }

    #[nervix_primitives::test]
    async fn a_subscription_keeps_an_empty_answer_as_its_typed_cause() {
        let fixture = Fixture::start().await;
        let name = "empty.nervix.test";
        fixture.authority.set(
            name,
            DnsAnswer::NoAddresses {
                negative_ttl: Duration::from_secs(1),
            },
        );
        let plan = plan(&format!("redis://{name}:6379"), fixture.dns);
        let error = match plan.connect().await {
            Ok(_) => panic!("an empty answer cannot open a subscription stream"),
            Err(error) => error,
        };
        assert!(matches!(
            error.current_context(),
            RedisPubSubSourceError::Resolve {
                failure: DnsLookupFailure::NoAddresses,
                ..
            }
        ));
    }

    #[nervix_primitives::test]
    async fn a_silent_lookup_ends_with_a_typed_timeout() {
        let fixture = Fixture::start().await;
        let name = "silent.nervix.test";
        fixture.authority.set(name, DnsAnswer::Silent);
        let plan = plan(&format!("redis://{name}:6379"), fixture.dns);
        let error = tokio::time::timeout(Duration::from_secs(5), plan.connect())
            .await
            .expect("the resolver bounds a silent authority")
            .err()
            .expect("a silent name cannot connect");
        assert!(
            matches!(
                error.current_context(),
                RedisPubSubSourceError::Resolve {
                    failure: DnsLookupFailure::Timeout,
                    ..
                }
            ),
            "{error:#}"
        );
    }

    #[nervix_primitives::test]
    async fn cancelling_a_lookup_leaves_the_source_ready_for_a_new_answer() {
        let fixture = Fixture::start().await;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("a loopback listener can bind");
        let port = listener
            .local_addr()
            .expect("listener has an address")
            .port();
        let name = "changing.nervix.test";
        fixture.authority.set(name, DnsAnswer::Silent);
        let plan = plan(&format!("redis://{name}:{port}"), fixture.dns.clone());
        let attempting = nervix_primitives::task::spawn({
            let plan = plan.clone();
            async move { plan.connect().await }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while fixture.authority.questions_for(name) == 0 {
                nervix_primitives::task::consume_budget().await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the first lookup asks the authority");
        attempting.abort();
        let stopped = match attempting.await {
            Err(stopped) => stopped,
            Ok(_) => panic!("the cancelled lookup stops"),
        };
        assert!(stopped.is_cancelled());
        fixture.answer(name, vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]);
        let stream = tokio::time::timeout(Duration::from_secs(3), plan.connect())
            .await
            .expect("a new connection is not held by the cancelled lookup")
            .expect("the new answer accepts a connection");
        let (_accepted, _) = listener.accept().await.expect("the listener accepts it");
        drop(stream);
    }

    #[nervix_primitives::test]
    async fn a_literal_ipv6_subscription_skips_dns() {
        let fixture = Fixture::start().await;
        let listener = TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, 0))
            .await
            .expect("the IPv6 loopback listener can bind");
        let port = listener
            .local_addr()
            .expect("listener has an address")
            .port();
        let plan = plan(&format!("redis://[::1]:{port}"), fixture.dns);
        let stream = plan
            .connect()
            .await
            .expect("the literal IPv6 address connects");
        let (_accepted, _) = listener
            .accept()
            .await
            .expect("the IPv6 listener accepts it");
        drop(stream);
        assert_eq!(fixture.authority.total_questions(), 0);
    }

    #[cfg(unix)]
    #[nervix_primitives::test]
    async fn a_unix_subscription_skips_dns() {
        let fixture = Fixture::start().await;
        let directory = tempfile::tempdir().expect("a temporary socket directory can be created");
        let path = directory.path().join("redis.sock");
        let listener =
            tokio::net::UnixListener::bind(&path).expect("the Unix socket listener can bind");
        let plan = plan(&format!("redis+unix://{}", path.display()), fixture.dns);
        let stream = plan.connect().await.expect("the Unix socket connects");
        let (_accepted, _) = listener
            .accept()
            .await
            .expect("the Unix listener accepts it");
        drop(stream);
        assert_eq!(fixture.authority.total_questions(), 0);
    }

    #[nervix_primitives::test]
    async fn a_subscription_keeps_the_clients_auth_database_and_protocol_settings() {
        let fixture = Fixture::start().await;
        let plan = plan(
            "redis://alice:secret@redis.nervix.test:6379/4?protocol=3",
            fixture.dns,
        );
        let settings = plan.client.get_connection_info().redis_settings();
        assert_eq!(settings.username(), Some("alice"));
        assert_eq!(settings.password(), Some("secret"));
        assert_eq!(settings.db(), 4);
        assert_eq!(settings.protocol(), redis::ProtocolVersion::RESP3);
    }
}
