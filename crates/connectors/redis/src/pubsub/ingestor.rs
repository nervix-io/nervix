//! Redis Pub/Sub ingestor transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The dedicated subscription connection each ingestor instance reads a channel
//!   through, including DNS, TCP, TLS, Redis's own protocol setup and the SUBSCRIBE, all within
//!   the client's `connect_timeout_ms`.
//! - **Depends on.** The connector contract, node DNS resolver, typed client configuration
//!   entries, `error-stack`, Tokio and `redis`.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or Plexus.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::io;

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use futures_util::StreamExt as _;
use nervix_connector::{
    BrokerIngestorConnector, ConfiguredDeadline, IngestMessageHeaders, IngestMetadataRow,
    IngestorBatch, IngestorBatchRequest, IngestorConnector, IngestorError, IngestorMessage,
    IngestorReadCancellation, IngestorResult, IngestorResume, NoIngestHeaders,
    RustlsClientConfigProvider, client_config_value,
};
use nervix_dns::{DnsLookupFailure, DnsResolver, LookupShare, OperationBudget};
use nervix_models::{ClientConfigEntry, RedisChannelName};
use nervix_primitives::{net::TcpStream, sync::StdArc, time::timeout};
use redis::{
    Client as RedisClient, ConnectionAddr, Msg,
    aio::{PubSub, PubSubStream},
};
use rustls_pki_types::ServerName;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsConnector;

use crate::pubsub::redis_client;

const REDIS: &str = "redis";

/// Why a Redis Pub/Sub ingestor could not open its subscription.
#[derive(Debug, Error)]
pub enum RedisPubSubIngestorError {
    #[error("invalid Redis client configuration")]
    ClientConfig,
    /// The client's `connect_timeout_ms` breaks the deadline rule, which the cause beneath names.
    #[error("invalid Redis client connect_timeout_ms")]
    InvalidConnectTimeout,
    #[error("failed to build Redis client")]
    BuildClient,
    #[error("resolving Redis subscription host '{host}' failed: {failure}")]
    Resolve {
        host: String,
        failure: DnsLookupFailure,
    },
    #[error("failed to connect Redis subscription: {reason}")]
    Connect { reason: String },
    /// The subscription's connection or Redis's setup on it ran out of the client's
    /// `connect_timeout_ms`; the cause beneath names the step and the bound.
    #[error("the Redis subscription connection was not established")]
    NotEstablished,
    #[error("unsupported Redis connection address")]
    UnsupportedAddress,
    #[error("failed to subscribe to Redis channel '{channel}'")]
    Subscribe { channel: String },
    #[error("Redis subscription closed")]
    SubscriptionClosed,
}

type RedisPubSubIngestorResult<T> = Result<T, Report<RedisPubSubIngestorError>>;

/// What one Redis Pub/Sub ingestor subscribes to: its configured client, node resolver, channel,
/// and the deadline its subscription is established within.
#[derive(Debug, Clone)]
pub struct RedisPubSubIngestorPlan {
    client: RedisClient,
    dns: DnsResolver,
    tls: Option<StdArc<rustls::ClientConfig>>,
    channel: RedisChannelName,
    connect_timeout: ConfiguredDeadline,
}

trait RedisStream: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> RedisStream for T {}
type ConnectedStream = Box<dyn RedisStream + Send + Unpin>;

impl RedisPubSubIngestorPlan {
    pub fn new(
        config: Vec<ClientConfigEntry>,
        channel: RedisChannelName,
        dns: DnsResolver,
    ) -> RedisPubSubIngestorResult<Self> {
        let addr = client_config_value(&config, "addr", "Redis")
            .change_context(RedisPubSubIngestorError::ClientConfig)?;
        let client =
            redis_client(&addr, &config).change_context(RedisPubSubIngestorError::BuildClient)?;
        let connect_timeout = ConfiguredDeadline::connect(&config)
            .change_context(RedisPubSubIngestorError::InvalidConnectTimeout)?;
        let tls = match client.get_connection_info().addr() {
            ConnectionAddr::TcpTls { .. } => {
                let config = RustlsClientConfigProvider::new(&config)
                    .build_with_replacement_ca()
                    .change_context(RedisPubSubIngestorError::BuildClient)?;
                Some(config)
            }
            _ => None,
        };
        Ok(Self {
            client,
            dns,
            tls,
            channel,
            connect_timeout,
        })
    }

    /// The operation budget one subscription is established within.
    fn budget(&self) -> OperationBudget {
        OperationBudget::start(self.connect_timeout.bound(), LookupShare::Whole)
    }

    /// Why the subscription was not established: `step` ran out of the client's
    /// `connect_timeout_ms`.
    fn not_established(&self, step: &'static str) -> Report<RedisPubSubIngestorError> {
        Report::new(self.connect_timeout.elapsed(step))
            .change_context(RedisPubSubIngestorError::NotEstablished)
    }

    async fn subscribe(&self) -> RedisPubSubIngestorResult<PubSubStream> {
        let budget = self.budget();
        let stream = self.connect_with_budget(&budget).await?;
        let setup = timeout(
            budget.remaining(),
            PubSub::new(self.client.get_connection_info().redis_settings(), stream),
        )
        .await;
        let mut pubsub = match setup {
            Ok(Ok(pubsub)) => pubsub,
            Ok(Err(source)) => {
                return Err(Report::new(RedisPubSubIngestorError::Connect {
                    reason: source.to_string(),
                }));
            }
            Err(_) => return Err(self.not_established("Redis's setup of the subscription")),
        };
        let subscribed = timeout(budget.remaining(), pubsub.subscribe(self.channel.as_str())).await;
        let subscribe_failed = || RedisPubSubIngestorError::Subscribe {
            channel: self.channel.as_str().to_string(),
        };
        match subscribed {
            Ok(Ok(())) => Ok(pubsub.into_on_message()),
            Ok(Err(source)) => {
                Err(Report::new(subscribe_failed()).attach_printable(source.to_string()))
            }
            Err(_) => Err(Report::new(
                self.connect_timeout
                    .elapsed("subscribing to the Redis channel"),
            )
            .change_context(subscribe_failed())),
        }
    }

    /// Open a fresh stream for every resume. Redis's Pub/Sub convenience method uses its default
    /// system resolver, so the connector opens the transport and lets Redis set up the protocol.
    #[cfg(test)]
    async fn connect(&self) -> RedisPubSubIngestorResult<ConnectedStream> {
        let budget = self.budget();
        self.connect_with_budget(&budget).await
    }

    async fn connect_with_budget(
        &self,
        budget: &OperationBudget,
    ) -> RedisPubSubIngestorResult<ConnectedStream> {
        match self.client.get_connection_info().addr() {
            ConnectionAddr::Tcp(host, port) => self.connect_tcp(host, *port, None, budget).await,
            ConnectionAddr::TcpTls {
                host,
                port,
                insecure,
                ..
            } => {
                if *insecure {
                    return Err(Report::new(RedisPubSubIngestorError::BuildClient)
                        .attach_printable("insecure Redis TLS is not supported"));
                }
                self.connect_tcp(host, *port, self.tls.as_ref(), budget)
                    .await
            }
            #[cfg(unix)]
            ConnectionAddr::Unix(path) => {
                let connected = timeout(
                    budget.remaining(),
                    nervix_primitives::net::UnixStream::connect(path),
                )
                .await;
                match connected {
                    Ok(Ok(stream)) => Ok(Box::new(stream)),
                    Ok(Err(source)) => Err(Report::new(RedisPubSubIngestorError::Connect {
                        reason: source.to_string(),
                    })),
                    Err(_) => Err(self.not_established("connecting to the Redis socket")),
                }
            }
            _ => Err(Report::new(RedisPubSubIngestorError::UnsupportedAddress)),
        }
    }

    async fn connect_tcp(
        &self,
        host: &str,
        port: u16,
        tls: Option<&StdArc<rustls::ClientConfig>>,
        budget: &OperationBudget,
    ) -> RedisPubSubIngestorResult<ConnectedStream> {
        let addresses = {
            let mapping_input = self.dns.resolve(host, port, budget.lookup()).await;
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(error) => Err({
                    let lookup = error.current_context();
                    let host = lookup.name().to_string();
                    let failure = lookup.failure();
                    error.change_context(RedisPubSubIngestorError::Resolve { host, failure })
                }),
            }
        }?;
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
        if budget.remaining().is_zero() {
            let mut report = self.not_established("connecting to the Redis server");
            if let Some(failure) = last_failure {
                report = report.attach_printable(failure);
            }
            return Err(report);
        }
        let failure =
            last_failure.unwrap_or_else(|| "no Redis address accepted a connection".to_string());
        Err(Report::new(RedisPubSubIngestorError::Connect {
            reason: failure,
        }))
    }
}

/// One received channel message. Redis Pub/Sub carries no headers.
pub struct RedisPubSubIngestorMessage {
    message: Msg,
    position: (),
    headers: NoIngestHeaders,
}

impl IngestorMessage for RedisPubSubIngestorMessage {
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

/// One Redis Pub/Sub ingestor instance: its subscription stream, absent while reconnecting.
pub struct RedisPubSubIngestor {
    plan: RedisPubSubIngestorPlan,
    stream: Option<PubSubStream>,
}

#[async_trait]
impl IngestorConnector for RedisPubSubIngestor {
    const KIND: nervix_models::IngestorEntityKind =
        nervix_models::IngestorEntityKind::Redis(nervix_models::RedisIngestorEntity::PubSub);

    type Plan = RedisPubSubIngestorPlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> IngestorResult<Self> {
        Ok(Self {
            plan: plan.clone(),
            stream: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.stream.is_none()
    }

    async fn suspend(&mut self) -> IngestorResult<()> {
        self.stream = None;
        Ok(())
    }

    async fn resume(&mut self) -> IngestorResult<IngestorResume> {
        if self.stream.is_none() {
            let stream = self
                .plan
                .subscribe()
                .await
                .change_context(IngestorError::Resume { connector: REDIS })?;
            self.stream = Some(stream);
        }
        Ok(IngestorResume::Ready)
    }

    async fn close(&mut self) -> IngestorResult<()> {
        self.stream = None;
        Ok(())
    }
}

#[async_trait]
impl BrokerIngestorConnector for RedisPubSubIngestor {
    type Message = RedisPubSubIngestorMessage;
    type Position = ();

    async fn next_batch(
        &mut self,
        _request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> IngestorResult<IngestorBatch<Self::Message>> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(IngestorBatch::ResumeRequired);
        };
        let Some(received) = cancellation.unless_cancelled(stream.next()).await else {
            return Ok(IngestorBatch::Interrupted {
                gathered: Vec::new(),
            });
        };
        let Some(message) = received else {
            self.stream = None;
            return Err(Report::new(RedisPubSubIngestorError::SubscriptionClosed)
                .change_context(IngestorError::Read { connector: REDIS }));
        };
        Ok(IngestorBatch::Messages(vec![RedisPubSubIngestorMessage {
            message,
            position: (),
            headers: NoIngestHeaders,
        }]))
    }

    async fn acknowledge(&mut self, _positions: &[Self::Position]) -> IngestorResult<()> {
        Ok(())
    }

    async fn reject(&mut self, _positions: &[Self::Position]) -> IngestorResult<()> {
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        time::Duration,
    };

    use nervix_connector::DeadlineElapsed;
    use nervix_dns::{DnsLookupError, DnsLookupFailure};
    use nervix_models::DeadlineKey;
    use nervix_primitives::net::TcpListener;
    use nervix_test_environment::dns_authority::DnsAnswer;

    use super::*;
    use crate::pubsub::test_fixture::Fixture;

    fn plan(addr: &str, dns: DnsResolver) -> RedisPubSubIngestorPlan {
        RedisPubSubIngestorPlan::new(
            vec![ClientConfigEntry {
                key: "addr".to_string(),
                value: addr.to_string(),
            }],
            RedisChannelName::parse("events").expect("a plain channel name parses"),
            dns,
        )
        .expect("the Redis address is valid")
    }

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[nervix_primitives::test]
    async fn an_invalid_connect_timeout_refuses_the_plan() {
        let fixture = Fixture::start().await;
        for value in ["0", "1.5", "soon"] {
            let error = RedisPubSubIngestorPlan::new(
                vec![
                    entry("addr", "redis://127.0.0.1:6379"),
                    entry("connect_timeout_ms", value),
                ],
                RedisChannelName::parse("events").expect("a plain channel name parses"),
                fixture.dns.clone(),
            )
            .expect_err("an invalid deadline cannot plan a subscription");
            assert!(
                matches!(
                    error.current_context(),
                    RedisPubSubIngestorError::InvalidConnectTimeout
                ),
                "{value}: {error:?}"
            );
        }
    }

    #[nervix_primitives::test]
    async fn a_server_that_never_answers_is_reported_within_connect_timeout() {
        let fixture = Fixture::start().await;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("a loopback listener can bind");
        let port = listener
            .local_addr()
            .expect("listener has an address")
            .port();
        // Accepts the subscription's connection and holds it without ever answering.
        let silent = nervix_primitives::task::spawn(async move {
            let (held, _) = listener.accept().await.expect("the listener accepts it");
            std::future::pending::<()>().await;
            drop(held);
        });
        let plan = RedisPubSubIngestorPlan::new(
            vec![
                entry("addr", &format!("redis://127.0.0.1:{port}")),
                entry("connect_timeout_ms", "200"),
            ],
            RedisChannelName::parse("events").expect("a plain channel name parses"),
            fixture.dns,
        )
        .expect("the Redis address and deadline are valid");
        let error = nervix_primitives::time::timeout(Duration::from_secs(10), plan.subscribe())
            .await
            .expect("connect_timeout_ms bounds the subscription")
            .err()
            .expect("a server that never answers cannot be subscribed to");
        let elapsed = error
            .frames()
            .find_map(|frame| frame.downcast_ref::<DeadlineElapsed>())
            .expect("the elapsed deadline is the innermost cause");
        assert_eq!(elapsed.key, DeadlineKey::ConnectTimeout);
        assert_eq!(elapsed.bound, Duration::from_millis(200));
        assert!(
            format!("{error:#}").contains("within connect_timeout_ms 200ms"),
            "{error:#}"
        );
        silent.abort();
    }

    #[nervix_primitives::test]
    async fn the_plan_requires_the_address_before_it_subscribes() {
        let channel = RedisChannelName::parse("events").expect("a plain channel name parses");
        let fixture = Fixture::start().await;
        let dns = fixture.dns;
        let error = RedisPubSubIngestorPlan::new(Vec::new(), channel, dns)
            .expect_err("a missing Redis address must fail");
        assert!(matches!(
            error.current_context(),
            RedisPubSubIngestorError::ClientConfig
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
        let (_accepted, _) =
            nervix_primitives::time::timeout(Duration::from_secs(2), listener.accept())
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
            RedisPubSubIngestorError::Resolve {
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
        let error = nervix_primitives::time::timeout(Duration::from_secs(5), plan.connect())
            .await
            .expect("the resolver bounds a silent authority")
            .err()
            .expect("a silent name cannot connect");
        assert!(
            matches!(
                error.current_context(),
                RedisPubSubIngestorError::Resolve {
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
        nervix_primitives::time::timeout(Duration::from_secs(2), async {
            while fixture.authority.questions_for(name) == 0 {
                nervix_primitives::task::consume_budget().await;
                nervix_primitives::time::sleep(Duration::from_millis(10)).await;
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
        let stream = nervix_primitives::time::timeout(Duration::from_secs(3), plan.connect())
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
        let listener = nervix_primitives::net::UnixListener::bind(&path)
            .expect("the Unix socket listener can bind");
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
