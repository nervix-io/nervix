//! Redis Pub/Sub ingestor transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The dedicated subscription connection each ingestor instance reads a channel or the
//!   channels of a pattern through, including DNS, TCP, TLS and protocol setup within
//!   `connect_timeout_ms`, the bounded read-ahead handoff, liveness probes within
//!   `idle_timeout_ms`, and the channel and pattern each delivered message carries as metadata.
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

mod subscription;

use std::{io, num::NonZeroUsize};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    BrokerIngestorConnector, ConfiguredDeadline, IngestMessageHeaders, IngestMessageMetadata,
    IngestMetadataRow, IngestMetadataValues, IngestRowHeaders, IngestorBatch, IngestorBatchRequest,
    IngestorConnector, IngestorError, IngestorMessage, IngestorReadCancellation, IngestorResult,
    IngestorResume, NoIngestHeaders, RustlsClientConfigProvider, client_config_value,
    optional_client_config_value,
};
use nervix_dns::{DnsLookupFailure, DnsResolver, LookupShare, OperationBudget};
use nervix_models::{ClientConfigEntry, ConnectorKind, RedisSubscriptionTarget};
use nervix_primitives::{
    net::TcpStream,
    sync::{Semaphore, StdArc},
    time::timeout,
};
use redis::{Client as RedisClient, ConnectionAddr};
use rustls_pki_types::ServerName;
use subscription::{DeliveredMessage, RedisSubscription};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsConnector;

use super::redis_client;

const REDIS: &str = "redis";

/// Why a Redis Pub/Sub source could not open its subscription.
#[derive(Debug, Error)]
pub enum RedisPubSubIngestorError {
    #[error("invalid Redis client configuration")]
    ClientConfig,
    /// The client's `connect_timeout_ms` breaks the deadline rule, which the cause beneath names.
    #[error("invalid Redis client connect_timeout_ms")]
    InvalidConnectTimeout,
    #[error("invalid {connector} client idle_timeout_ms", connector = ConnectorKind::Redis)]
    InvalidIdleTimeout,
    #[error("{connector} subscription requires client config key '{key}'", connector = ConnectorKind::Redis)]
    MissingConfiguration { key: &'static str },
    #[error("{connector} subscription_buffer_messages must be between 1 and {maximum}", connector = ConnectorKind::Redis)]
    InvalidBufferCapacity { maximum: usize },
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
    #[error("failed to subscribe to {connector} {subscription}", connector = ConnectorKind::Redis)]
    Subscribe {
        subscription: RedisSubscriptionTarget,
    },
    #[error("failed to initialize the {connector} subscription protocol", connector = ConnectorKind::Redis)]
    SubscriptionSetup,
    #[error("failed to read the {connector} subscription", connector = ConnectorKind::Redis)]
    SubscriptionRead,
    #[error("{connector} subscription idle probe failed", connector = ConnectorKind::Redis)]
    IdleProbe,
    #[error("{connector} subscription reader task failed", connector = ConnectorKind::Redis)]
    SubscriptionTask,
    #[error("unexpected {connector} subscription response during {operation}", connector = ConnectorKind::Redis)]
    UnexpectedResponse { operation: &'static str },
    #[error("Redis subscription closed")]
    SubscriptionClosed,
}

type RedisPubSubIngestorResult<T> = Result<T, Report<RedisPubSubIngestorError>>;

/// What one Redis Pub/Sub source subscribes to: its configured client, node resolver, channel or
/// pattern, and the deadline its subscription is established within.
#[derive(Debug, Clone)]
pub struct RedisPubSubIngestorPlan {
    client: RedisClient,
    dns: DnsResolver,
    tls: Option<StdArc<rustls::ClientConfig>>,
    subscription: RedisSubscriptionTarget,
    connect_timeout: ConfiguredDeadline,
    idle_timeout: ConfiguredDeadline,
    buffer_messages: NonZeroUsize,
}

trait RedisStream: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> RedisStream for T {}
type ConnectedStream = Box<dyn RedisStream + Send + Unpin>;

impl RedisPubSubIngestorPlan {
    pub fn new(
        config: Vec<ClientConfigEntry>,
        subscription: RedisSubscriptionTarget,
        dns: DnsResolver,
    ) -> RedisPubSubIngestorResult<Self> {
        let addr = client_config_value(&config, "addr", "Redis")
            .change_context(RedisPubSubIngestorError::ClientConfig)?;
        let client =
            redis_client(&addr, &config).change_context(RedisPubSubIngestorError::BuildClient)?;
        let connect_timeout = ConfiguredDeadline::connect(&config)
            .change_context(RedisPubSubIngestorError::InvalidConnectTimeout)?;
        let idle_timeout = ConfiguredDeadline::idle(&config)
            .change_context(RedisPubSubIngestorError::InvalidIdleTimeout)?;
        let Some(idle_timeout) = idle_timeout else {
            return Err(Report::new(
                RedisPubSubIngestorError::MissingConfiguration {
                    key: "idle_timeout_ms",
                },
            ));
        };
        let buffer_messages = Self::buffer_messages(&config)?;
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
            subscription,
            connect_timeout,
            idle_timeout,
            buffer_messages,
        })
    }

    pub(crate) fn buffer_messages(
        config: &[ClientConfigEntry],
    ) -> RedisPubSubIngestorResult<NonZeroUsize> {
        let Some(value) = optional_client_config_value(config, "subscription_buffer_messages")
        else {
            return Err(Report::new(
                RedisPubSubIngestorError::MissingConfiguration {
                    key: "subscription_buffer_messages",
                },
            ));
        };
        let invalid = || RedisPubSubIngestorError::InvalidBufferCapacity {
            maximum: Semaphore::MAX_PERMITS,
        };
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Report::new(invalid()));
        }
        let capacity = value.parse::<NonZeroUsize>().change_context(invalid())?;
        if capacity.get() > Semaphore::MAX_PERMITS {
            return Err(Report::new(invalid()));
        }
        Ok(capacity)
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

    async fn subscribe(&self) -> RedisPubSubIngestorResult<RedisSubscription> {
        let budget = self.budget();
        let stream = self.connect_with_budget(&budget).await?;
        let subscribed = timeout(
            budget.remaining(),
            RedisSubscription::open(
                stream,
                self.client.get_connection_info().redis_settings(),
                &self.subscription,
                self.buffer_messages,
                self.idle_timeout,
            ),
        )
        .await;
        let subscribe_failed = || RedisPubSubIngestorError::Subscribe {
            subscription: self.subscription.clone(),
        };
        match subscribed {
            Ok(Ok(subscription)) => Ok(subscription),
            Ok(Err(error)) => Err(error.change_context(subscribe_failed())),
            Err(_) => Err(self.not_established("Redis's setup of the subscription")),
        }
    }

    /// Open a fresh stream for every resume. Redis's Pub/Sub convenience method uses its default
    /// system resolver, so the connector owns the transport used by the subscription reader.
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

/// One received message, with the channel it was published to and the pattern that matched it.
/// Redis Pub/Sub carries no headers.
pub struct RedisPubSubIngestorMessage {
    message: DeliveredMessage,
    position: (),
    headers: NoIngestHeaders,
}

impl IngestorMessage for RedisPubSubIngestorMessage {
    type Position = ();

    fn payload(&self) -> &[u8] {
        self.message.payload()
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.headers
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow {
            fields: self,
            headers: IngestRowHeaders::Borrowed(&self.headers),
            position: None,
        }
    }
}

/// `metadata.channel`, then `metadata.pattern`, as the entity declares them. A channel whose bytes
/// are not UTF-8 text has no `STRING` value and reads as null, and a channel subscription has no
/// pattern.
impl IngestMessageMetadata for RedisPubSubIngestorMessage {
    fn visit_metadata(&self, values: &mut dyn IngestMetadataValues) {
        match std::str::from_utf8(self.message.channel()) {
            Ok(channel) => values.string(channel),
            Err(_) => values.absent(),
        }
        let Some(pattern) = self.message.pattern() else {
            values.absent();
            return;
        };
        match std::str::from_utf8(pattern) {
            Ok(pattern) => values.string(pattern),
            Err(_) => values.absent(),
        }
    }
}

/// One Redis Pub/Sub source instance: its subscription stream, absent while reconnecting.
pub struct RedisPubSubIngestor {
    plan: RedisPubSubIngestorPlan,
    stream: Option<RedisSubscription>,
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
        if let Some(stream) = self.stream.take() {
            stream
                .close()
                .await
                .change_context(IngestorError::Suspend { connector: REDIS })?;
        }
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
        if let Some(stream) = self.stream.take() {
            stream
                .close()
                .await
                .change_context(IngestorError::Close { connector: REDIS })?;
        }
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
        let Some(received) = cancellation.unless_cancelled(stream.next_message()).await else {
            return Ok(IngestorBatch::Interrupted {
                gathered: Vec::new(),
            });
        };
        let message = match received {
            Ok(message) => message,
            Err(error) => {
                self.stream = None;
                return Err(error.change_context(IngestorError::Read { connector: REDIS }));
            }
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

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_connector::DeadlineElapsed;
    use nervix_dns::{DnsLookupError, DnsLookupFailure};
    use nervix_models::DeadlineKey;
    use nervix_primitives::net::TcpListener;
    use nervix_test_environment::dns_authority::DnsAnswer;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;
    use crate::pubsub::test_fixture::Fixture;

    #[test]
    fn subscription_capacity_is_positive_and_fits_the_bounded_channel() {
        for capacity in [1, 2, Semaphore::MAX_PERMITS] {
            let value = capacity.to_string();
            let parsed = RedisPubSubIngestorPlan::buffer_messages(&[entry(
                "subscription_buffer_messages",
                &value,
            )])
            .assured("the table contains admitted message capacities");
            assert_eq!(parsed.get(), capacity);
        }
        let above_channel_limit = Semaphore::MAX_PERMITS
            .checked_add(1)
            .assured("the channel's permit ceiling is below usize::MAX")
            .to_string();
        for value in [
            "0",
            "-1",
            "+2",
            "1.5",
            "",
            above_channel_limit.as_str(),
            "18446744073709551616",
        ] {
            let error = RedisPubSubIngestorPlan::buffer_messages(&[entry(
                "subscription_buffer_messages",
                value,
            )])
            .expect_err("the capacity is not an admitted positive message count");
            assert!(matches!(
                error.current_context(),
                RedisPubSubIngestorError::InvalidBufferCapacity { .. }
            ));
        }
    }

    fn plan(addr: &str, dns: DnsResolver) -> RedisPubSubIngestorPlan {
        plan_subscribing(addr, events(), dns)
    }

    /// A plan of `subscription` through the Redis server at `addr`.
    fn plan_subscribing(
        addr: &str,
        subscription: RedisSubscriptionTarget,
        dns: DnsResolver,
    ) -> RedisPubSubIngestorPlan {
        RedisPubSubIngestorPlan::new(
            vec![
                entry("addr", addr),
                entry("subscription_buffer_messages", "1024"),
                entry("idle_timeout_ms", "30000"),
            ],
            subscription,
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

    /// The channel subscription the fixtures plan.
    fn events() -> RedisSubscriptionTarget {
        RedisSubscriptionTarget::Channel(
            nervix_models::RedisChannelName::parse("events").expect("a plain channel name parses"),
        )
    }

    /// The metadata values `delivery`, a reply Redis sent, hands the host.
    fn handed_over(delivery: redis::Value) -> Vec<nervix_connector::RetainedIngestMetadataValue> {
        let message = RedisPubSubIngestorMessage {
            message: DeliveredMessage::from_value(delivery)
                .assured("the fixture reply is a delivered message"),
            position: (),
            headers: NoIngestHeaders,
        };
        nervix_connector::RetainedIngestMetadata::capture(&message)
            .values()
            .to_vec()
    }

    /// One delivery Redis may send a subscription: its channel, the pattern that matched it for a
    /// pattern subscription, its payload, and whether RESP3 pushes it rather than RESP2 framing it
    /// as an array.
    #[derive(Debug, bolero::TypeGenerator)]
    struct Delivery {
        channel: Vec<u8>,
        pattern: Option<Vec<u8>>,
        payload: Vec<u8>,
        pushed: bool,
    }

    impl Delivery {
        /// The reply Redis sends for this delivery.
        fn reply(&self) -> redis::Value {
            let bulk = |bytes: &[u8]| redis::Value::BulkString(bytes.to_vec());
            let mut parts = Vec::new();
            if let Some(pattern) = &self.pattern {
                parts.push(bulk(pattern));
            }
            parts.push(bulk(&self.channel));
            parts.push(bulk(&self.payload));
            let (kind, name): (redis::PushKind, &[u8]) = match self.pattern {
                Some(_) => (redis::PushKind::PMessage, b"pmessage"),
                None => (redis::PushKind::Message, b"message"),
            };
            if self.pushed {
                return redis::Value::Push { kind, data: parts };
            }
            parts.insert(0, bulk(name));
            redis::Value::Array(parts)
        }

        /// What the metadata visit hands over for `bytes`: their text, or null where they are not
        /// UTF-8.
        fn text_or_null(bytes: &[u8]) -> nervix_connector::RetainedIngestMetadataValue {
            match std::str::from_utf8(bytes) {
                Ok(text) => nervix_connector::RetainedIngestMetadataValue::String(text.to_owned()),
                Err(_) => nervix_connector::RetainedIngestMetadataValue::Absent,
            }
        }

        /// The source hands over exactly the delivered bytes, and the channel and pattern as the
        /// declaration orders them.
        fn check(&self) {
            let message = RedisPubSubIngestorMessage {
                message: DeliveredMessage::from_value(self.reply())
                    .assured("every framed channel or pattern delivery is a delivered message"),
                position: (),
                headers: NoIngestHeaders,
            };
            assert_eq!(message.payload(), self.payload);
            assert_eq!(message.message.channel(), self.channel);
            assert_eq!(message.message.pattern(), self.pattern.as_deref());
            let pattern = match &self.pattern {
                Some(pattern) => Self::text_or_null(pattern),
                None => nervix_connector::RetainedIngestMetadataValue::Absent,
            };
            assert_eq!(
                nervix_connector::RetainedIngestMetadata::capture(&message).values(),
                [Self::text_or_null(&self.channel), pattern]
            );
        }
    }

    #[test]
    fn bolero_delivered_messages_keep_their_exact_bytes_and_metadata() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(512)
            .with_type::<Delivery>()
            .for_each(Delivery::check);
    }

    #[test]
    fn a_delivered_message_hands_over_its_channel_then_its_pattern_as_declared() {
        use nervix_connector::RetainedIngestMetadataValue::{Absent, String as Text};
        let declared: Vec<_> = <RedisPubSubIngestor as IngestorConnector>::KIND
            .capabilities()
            .metadata
            .fields()
            .iter()
            .map(|field| field.name())
            .collect();
        assert_eq!(declared, ["channel", "pattern"]);
        let bulk = |bytes: &[u8]| redis::Value::BulkString(bytes.to_vec());
        assert_eq!(
            handed_over(redis::Value::Array(vec![
                bulk(b"pmessage"),
                bulk(b"orders.*"),
                bulk(b"orders.eu"),
                bulk(b"{}"),
            ])),
            [Text("orders.eu".to_string()), Text("orders.*".to_string())]
        );
        assert_eq!(
            handed_over(redis::Value::Array(vec![
                bulk(b"message"),
                bulk(b"events"),
                bulk(b"{}"),
            ])),
            [Text("events".to_string()), Absent]
        );
        assert_eq!(
            handed_over(redis::Value::Array(vec![
                bulk(b"pmessage"),
                bulk(b"orders.*"),
                bulk(b"orders.\xff"),
                bulk(b"{}"),
            ])),
            [Absent, Text("orders.*".to_string())]
        );
    }

    #[nervix_primitives::test]
    async fn an_invalid_connect_timeout_refuses_the_plan() {
        let fixture = Fixture::start().await;
        for value in ["0", "1.5", "soon"] {
            let error = RedisPubSubIngestorPlan::new(
                vec![
                    entry("addr", "redis://127.0.0.1:6379"),
                    entry("connect_timeout_ms", value),
                    entry("subscription_buffer_messages", "1024"),
                    entry("idle_timeout_ms", "30000"),
                ],
                events(),
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
                entry("subscription_buffer_messages", "1024"),
                entry("idle_timeout_ms", "30000"),
            ],
            events(),
            fixture.dns,
        )
        .expect("the Redis address and deadline are valid");
        let error = nervix_primitives::time::timeout(Duration::from_secs(10), plan.subscribe())
            .await
            .expect("connect_timeout_ms bounds the subscription")
            .expect_err("a server that never answers cannot be subscribed to");
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
        let fixture = Fixture::start().await;
        let dns = fixture.dns;
        let error = RedisPubSubIngestorPlan::new(Vec::new(), events(), dns)
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
    async fn a_refused_pattern_subscription_names_its_pattern_above_the_servers_typed_cause() {
        let fixture = Fixture::start().await;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("a loopback listener can bind an ephemeral port");
        let port = listener
            .local_addr()
            .assured("a bound listener has an address")
            .port();
        let server = nervix_primitives::task::spawn(async move {
            let (mut stream, _) = listener
                .accept()
                .await
                .assured("the source connects to its Redis server");
            let command = b"*2\r\n$10\r\nPSUBSCRIBE\r\n$8\r\norders.*\r\n";
            let mut received = vec![0; command.len()];
            stream
                .read_exact(&mut received)
                .await
                .assured("the source asks for its pattern subscription");
            assert_eq!(received, command);
            stream
                .write_all(
                    b"-NOPERM this user has no permissions to access one of the channels\r\n",
                )
                .await
                .assured("the refusal reaches the source");
        });
        let subscription = RedisSubscriptionTarget::Pattern(
            nervix_models::RedisChannelPattern::parse("orders.*")
                .assured("a glob pattern is within the bound"),
        );
        let plan = plan_subscribing(
            &format!("redis://127.0.0.1:{port}"),
            subscription.clone(),
            fixture.dns,
        );
        let error = match plan.subscribe().await {
            Ok(_) => panic!("the server refused the pattern subscription"),
            Err(error) => error,
        };
        let RedisPubSubIngestorError::Subscribe {
            subscription: refused,
        } = error.current_context()
        else {
            panic!("a refused subscription reports the subscription it asked for");
        };
        assert_eq!(refused, &subscription);
        let cause = error
            .downcast_ref::<redis::RedisError>()
            .assured("the refusal retains the Redis driver's typed cause");
        assert_eq!(cause.code(), Some("NOPERM"));
        server.await.assured("the refusing server finishes");
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
