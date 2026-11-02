//! One bounded Redis subscription and its exact transport lifetime.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Protocol setup, the channel or pattern subscription and Redis's confirmation of it,
//!   the sole RESP reader and the messages it parses, its bounded message handoff, idle probes on
//!   the same socket, and ending the reader before its source resumes another connection.
//! - **Depends on.** Redis's command encoder and asynchronous RESP parser, validated deadlines,
//!   and the primitive boundary's bounded channel, task and timers.
//! - **Must not know.** Runtime hosts, graph state, quiesce policy, branches or Plexus.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the Redis subscription engine")
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "receives validated subscription settings")
)]

use std::{fmt, num::NonZeroUsize};

use combine::{
    parser::combinator::AnySendSyncPartialState,
    stream::{Decoder, PointerOffset},
};
use error_stack::{Report, ResultExt as _};
use nervix_connector::ConfiguredDeadline;
use nervix_dns::{LookupShare, OperationBudget};
use nervix_models::{ConnectorKind, RedisSubscriptionTarget};
use nervix_primitives::{
    sync::mpsc,
    task::{AbortOnDropHandle, consume_budget, spawn},
    time::{sleep, timeout},
};
use nervix_recovery::{Discarded as _, NoReceiver as _};
use redis::{Cmd, ProtocolVersion, PushKind, RedisConnectionInfo, Value};
use tokio::io::{AsyncWriteExt as _, ReadHalf, WriteHalf};

use super::{ConnectedStream, RedisPubSubIngestorError, RedisPubSubIngestorResult};

type RedisDecoder = Decoder<AnySendSyncPartialState, PointerOffset<[u8]>>;

/// The source retains this handle while the reader owns the socket and a bounded sender.
pub(super) struct RedisSubscription {
    messages: mpsc::Receiver<DeliveredMessage>,
    reader: AbortOnDropHandle<RedisPubSubIngestorResult<()>>,
}

impl fmt::Debug for RedisSubscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisSubscription")
            .finish_non_exhaustive()
    }
}

impl RedisSubscription {
    pub(super) async fn open(
        stream: ConnectedStream,
        settings: &RedisConnectionInfo,
        subscription: &RedisSubscriptionTarget,
        capacity: NonZeroUsize,
        idle_timeout: ConfiguredDeadline,
    ) -> RedisPubSubIngestorResult<Self> {
        let (input, output) = tokio::io::split(stream);
        let mut transport = SubscriptionReader {
            input,
            output,
            decoder: Decoder::new(),
            idle_timeout,
        };
        transport.setup(settings).await?;
        transport.subscribe(subscription).await?;
        let (sender, messages) = mpsc::channel(capacity.get());
        let reader = AbortOnDropHandle::new(spawn(transport.forward(sender)));
        Ok(Self { messages, reader })
    }

    pub(super) async fn next_message(&mut self) -> RedisPubSubIngestorResult<DeliveredMessage> {
        if let Some(message) = self.messages.recv().await {
            return Ok(message);
        }
        match (&mut self.reader).await {
            Ok(Err(error)) => Err(error),
            Ok(Ok(())) => Err(Report::new(RedisPubSubIngestorError::SubscriptionClosed)),
            Err(error) => {
                Err(Report::new(error).change_context(RedisPubSubIngestorError::SubscriptionTask))
            }
        }
    }

    pub(super) async fn close(self) -> RedisPubSubIngestorResult<()> {
        self.reader.abort();
        match self.reader.await {
            Ok(outcome) => {
                outcome.discarded(
                    "terminal source closure ends intake; read outcomes are owed only while \
                     intake is active",
                );
                Ok(())
            }
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => {
                Err(Report::new(error).change_context(RedisPubSubIngestorError::SubscriptionTask))
            }
        }
    }
}

/// Every decoded message has a queue permit before it is read. A full handoff stops this reader
/// and TCP applies backpressure; no driver task can accumulate another queue behind it.
struct SubscriptionReader {
    input: ReadHalf<ConnectedStream>,
    output: WriteHalf<ConnectedStream>,
    decoder: RedisDecoder,
    idle_timeout: ConfiguredDeadline,
}

impl SubscriptionReader {
    async fn setup(&mut self, settings: &RedisConnectionInfo) -> RedisPubSubIngestorResult<()> {
        if settings.protocol() == ProtocolVersion::RESP3 {
            let mut hello = redis::cmd("HELLO");
            hello.arg(3_u8);
            if let Some(password) = settings.password() {
                hello
                    .arg("AUTH")
                    .arg(settings.username().unwrap_or("default"))
                    .arg(password);
            }
            let response = self.command(&hello).await?;
            if !matches!(response, Value::Map(_)) {
                return Err(Report::new(RedisPubSubIngestorError::UnexpectedResponse {
                    operation: "HELLO",
                }));
            }
        } else if let Some(password) = settings.password() {
            let mut auth = redis::cmd("AUTH");
            if let Some(username) = settings.username() {
                auth.arg(username);
            }
            auth.arg(password);
            let response = self.command(&auth).await?;
            self.expect_ok(response, "AUTH")?;
        }
        if settings.db() != 0 {
            let mut select = redis::cmd("SELECT");
            select.arg(settings.db());
            let response = self.command(&select).await?;
            self.expect_ok(response, "SELECT")?;
        }
        Ok(())
    }

    fn expect_ok(&self, response: Value, operation: &'static str) -> RedisPubSubIngestorResult<()> {
        match response {
            Value::Okay => Ok(()),
            Value::SimpleString(value) if value == "OK" => Ok(()),
            _ => Err(Report::new(RedisPubSubIngestorError::UnexpectedResponse {
                operation,
            })),
        }
    }

    async fn command(&mut self, command: &Cmd) -> RedisPubSubIngestorResult<Value> {
        self.output
            .write_all(&command.get_packed_command())
            .await
            .change_context(RedisPubSubIngestorError::SubscriptionSetup)?;
        let response = redis::parse_redis_value_async(&mut self.decoder, &mut self.input)
            .await
            .change_context(RedisPubSubIngestorError::SubscriptionSetup)?;
        response
            .extract_error()
            .change_context(RedisPubSubIngestorError::SubscriptionSetup)
    }

    async fn subscribe(
        &mut self,
        subscription: &RedisSubscriptionTarget,
    ) -> RedisPubSubIngestorResult<()> {
        let request = SubscriptionRequest::of(subscription);
        let mut command = redis::cmd(request.command());
        command.arg(subscription.as_str());
        let response = self.command(&command).await?;
        if !request.confirmed_by(&response, subscription.as_str()) {
            return Err(Report::new(RedisPubSubIngestorError::UnexpectedResponse {
                operation: request.command(),
            }));
        }
        Ok(())
    }

    async fn forward(
        mut self,
        sender: mpsc::Sender<DeliveredMessage>,
    ) -> RedisPubSubIngestorResult<()> {
        loop {
            consume_budget().await;
            let permit = match sender.reserve().await {
                Ok(permit) => permit,
                Err(error) => {
                    Err::<(), _>(error)
                        .means_shutdown(&format!("{} subscription intake", ConnectorKind::Redis));
                    return Ok(());
                }
            };
            let response = self
                .next_value()
                .await?
                .extract_error()
                .change_context(RedisPubSubIngestorError::SubscriptionRead)?;
            let pong = self.is_pong(&response);
            if let Some(message) = DeliveredMessage::from_value(response) {
                permit.send(message);
            } else if !pong {
                return Err(Report::new(RedisPubSubIngestorError::UnexpectedResponse {
                    operation: "receiving a subscription message",
                }));
            }
        }
    }

    fn is_pong(&self, value: &Value) -> bool {
        match value {
            Value::SimpleString(value) => value == "PONG",
            Value::Array(parts) => matches!(parts.as_slice(),
                [Value::BulkString(kind), Value::BulkString(payload)]
                if kind == b"pong" && payload.is_empty()),
            _ => false,
        }
    }

    /// Keep the same parsing future alive while issuing a probe, so a partial RESP frame is never
    /// abandoned by a timer or by host cancellation. Any activity proves the connection is live.
    /// Waiting for queue room happens before this read and does not spend its idle interval.
    async fn next_value(&mut self) -> RedisPubSubIngestorResult<Value> {
        let idle = self.idle_timeout;
        let quiet = idle.bound() / 2;
        let budget = OperationBudget::start(idle.bound(), LookupShare::Whole);
        let mut incoming = std::pin::pin!(redis::parse_redis_value_async(
            &mut self.decoder,
            &mut self.input
        ));
        nervix_primitives::select! {
            biased;
            response = &mut incoming => response.change_context(RedisPubSubIngestorError::SubscriptionRead),
            _ = sleep(quiet) => {
                let elapsed = || Report::new(idle.elapsed("waiting for subscription activity"))
                    .change_context(RedisPubSubIngestorError::IdleProbe);
                if budget.remaining().is_zero() {
                    return Err(elapsed());
                }
                timeout(budget.remaining(), self.output.write_all(&redis::cmd("PING").get_packed_command()))
                    .await.map_err(|_| elapsed())?
                    .change_context(RedisPubSubIngestorError::IdleProbe)?;
                timeout(budget.remaining(), &mut incoming).await.map_err(|_| elapsed())?
                    .change_context(RedisPubSubIngestorError::SubscriptionRead)
            }
        }
    }
}

/// How Redis is asked for one subscription, and how it confirms it.
#[derive(Clone, Copy)]
enum SubscriptionRequest {
    /// `SUBSCRIBE` to one channel, confirmed by a `subscribe` reply.
    Channel,
    /// `PSUBSCRIBE` to one pattern, confirmed by a `psubscribe` reply.
    Pattern,
}

impl SubscriptionRequest {
    fn of(subscription: &RedisSubscriptionTarget) -> Self {
        match subscription {
            RedisSubscriptionTarget::Channel(_) => Self::Channel,
            RedisSubscriptionTarget::Pattern(_) => Self::Pattern,
        }
    }

    /// The command that asks for the subscription.
    const fn command(self) -> &'static str {
        match self {
            Self::Channel => "SUBSCRIBE",
            Self::Pattern => "PSUBSCRIBE",
        }
    }

    /// The kind a RESP2 confirmation names in its first element.
    const fn confirmation(self) -> &'static [u8] {
        match self {
            Self::Channel => b"subscribe",
            Self::Pattern => b"psubscribe",
        }
    }

    /// The kind of push a RESP3 confirmation is.
    const fn push(self) -> PushKind {
        match self {
            Self::Channel => PushKind::Subscribe,
            Self::Pattern => PushKind::PSubscribe,
        }
    }

    /// Whether `response` confirms this subscription to `name` as the connection's only one.
    fn confirmed_by(self, response: &Value, name: &str) -> bool {
        match response {
            Value::Array(parts) => matches!(parts.as_slice(),
                [Value::BulkString(kind), Value::BulkString(subscribed), Value::Int(1)]
                if kind.as_slice() == self.confirmation() && subscribed == name.as_bytes()),
            Value::Push { kind, data } => {
                *kind == self.push()
                    && matches!(data.as_slice(),
                        [Value::BulkString(subscribed), Value::Int(1)]
                        if subscribed == name.as_bytes())
            }
            _ => false,
        }
    }
}

/// One message the subscription delivered, as the bytes Redis sent: the channel it was published
/// to, the pattern that matched it for a pattern subscription, and its payload.
///
/// Parsing moves the bytes out of the reply Redis's parser built, so a message costs no copy.
pub(super) struct DeliveredMessage {
    channel: Vec<u8>,
    pattern: Option<Vec<u8>>,
    payload: Vec<u8>,
}

/// A message prints none of its bytes, since its payload may be sensitive.
impl fmt::Debug for DeliveredMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveredMessage")
            .finish_non_exhaustive()
    }
}

impl DeliveredMessage {
    /// The message a RESP2 `message` or `pmessage` array, or a RESP3 `message` or `pmessage` push,
    /// carries. Any other value is not a delivered message.
    pub(super) fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Push { kind, data } => Self::from_parts(kind, data.into_iter()),
            Value::Array(parts) => {
                let mut parts = parts.into_iter();
                let kind = match parts.next()? {
                    Value::BulkString(kind) if kind == b"message" => PushKind::Message,
                    Value::BulkString(kind) if kind == b"pmessage" => PushKind::PMessage,
                    _ => return None,
                };
                Self::from_parts(kind, parts)
            }
            _ => None,
        }
    }

    /// The message a delivery of `kind` carries in `parts`: a pattern first for a pattern
    /// delivery, then the channel and the payload, and nothing after them.
    fn from_parts(kind: PushKind, mut parts: std::vec::IntoIter<Value>) -> Option<Self> {
        let pattern = match kind {
            PushKind::Message => None,
            PushKind::PMessage => Some(Self::bytes(parts.next()?)?),
            _ => return None,
        };
        let channel = Self::bytes(parts.next()?)?;
        let payload = Self::bytes(parts.next()?)?;
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            channel,
            pattern,
            payload,
        })
    }

    /// The bytes of a bulk string, the only form Redis sends a channel, a pattern or a payload in.
    fn bytes(value: Value) -> Option<Vec<u8>> {
        match value {
            Value::BulkString(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// The channel the message was published to.
    pub(super) fn channel(&self) -> &[u8] {
        &self.channel
    }

    /// The pattern that matched the message, which a channel subscription has none of.
    pub(super) fn pattern(&self) -> Option<&[u8]> {
        self.pattern.as_deref()
    }

    pub(super) fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "ordinary in-memory protocol tests")
)]
mod tests {
    use std::time::Duration;

    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_connector::DeadlineElapsed;
    use nervix_models::{ClientConfigEntry, DeadlineKey, RedisChannelName, RedisChannelPattern};
    use nervix_primitives::time::{Instant, advance};
    use tokio::io::{AsyncReadExt as _, DuplexStream};

    use super::*;

    struct Peer {
        stream: DuplexStream,
        decoder: RedisDecoder,
        protocol: ProtocolVersion,
    }

    impl Peer {
        fn new(stream: DuplexStream, protocol: ProtocolVersion) -> Self {
            Self {
                stream,
                decoder: Decoder::new(),
                protocol,
            }
        }

        async fn command(&mut self, expected: &[&str]) {
            let value = redis::parse_redis_value_async(&mut self.decoder, &mut self.stream)
                .await
                .assured("the client writes a complete Redis command to the in-memory peer");
            let arguments: Vec<String> =
                redis::from_redis_value(value).assured("the scripted commands have text arguments");
            let expected: Vec<String> = expected
                .iter()
                .map(|argument| (*argument).to_string())
                .collect();
            assert_eq!(arguments, expected);
        }

        async fn answer(&mut self, answer: &[u8]) {
            self.stream
                .write_all(answer)
                .await
                .assured("the subscription remains open while its peer answers");
        }

        async fn subscribe(&mut self) {
            self.command(&["SUBSCRIBE", "events"]).await;
            match self.protocol {
                ProtocolVersion::RESP2 => {
                    self.answer(b"*3\r\n$9\r\nsubscribe\r\n$6\r\nevents\r\n:1\r\n")
                        .await
                }
                ProtocolVersion::RESP3 => {
                    self.answer(b">3\r\n$9\r\nsubscribe\r\n$6\r\nevents\r\n:1\r\n")
                        .await
                }
                _ => panic!("the fixture constructs only RESP2 and RESP3 peers"),
            }
        }

        /// The marker that opens a subscription reply: an array in RESP2 and a push in RESP3.
        fn subscription_reply_marker(&self) -> u8 {
            match self.protocol {
                ProtocolVersion::RESP2 => b'*',
                ProtocolVersion::RESP3 => b'>',
                _ => panic!("the fixture constructs only RESP2 and RESP3 peers"),
            }
        }

        /// Confirms the subscription to `pattern` a source asks for.
        async fn psubscribe(&mut self, pattern: &str) {
            self.command(&["PSUBSCRIBE", pattern]).await;
            let mut frame = vec![self.subscription_reply_marker()];
            frame.extend_from_slice(b"3\r\n$10\r\npsubscribe\r\n");
            Self::push_bulk(&mut frame, pattern.as_bytes());
            frame.extend_from_slice(b":1\r\n");
            self.answer(&frame).await;
        }

        /// Delivers `payload`, published to `channel`, as a match of `pattern`.
        async fn pmessage(&mut self, pattern: &[u8], channel: &[u8], payload: &[u8]) {
            let mut frame = vec![self.subscription_reply_marker()];
            frame.extend_from_slice(b"4\r\n$8\r\npmessage\r\n");
            for part in [pattern, channel, payload] {
                Self::push_bulk(&mut frame, part);
            }
            self.answer(&frame).await;
        }

        /// Appends `bytes` to `frame` as one RESP bulk string.
        fn push_bulk(frame: &mut Vec<u8>, bytes: &[u8]) {
            frame.push(b'$');
            frame.extend_from_slice(bytes.len().to_string().as_bytes());
            frame.extend_from_slice(b"\r\n");
            frame.extend_from_slice(bytes);
            frame.extend_from_slice(b"\r\n");
        }

        async fn message(&mut self, payload: u8) {
            let mut frame = b"*3\r\n$7\r\nmessage\r\n$6\r\nevents\r\n$1\r\n".to_vec();
            if self.protocol == ProtocolVersion::RESP3 {
                frame[0] = b'>';
            }
            frame.push(payload);
            frame.extend_from_slice(b"\r\n");
            self.answer(&frame).await;
        }

        async fn ended(&mut self) {
            let mut rest = Vec::new();
            self.stream
                .read_to_end(&mut rest)
                .await
                .assured("the source closes its in-memory transport without an I/O error");
        }
    }

    fn idle() -> ConfiguredDeadline {
        ConfiguredDeadline::idle(&[ClientConfigEntry {
            key: "idle_timeout_ms".to_string(),
            value: "1000".to_string(),
        }])
        .assured("one second is a positive schedulable deadline")
        .assured("the client declares its idle timeout")
    }

    fn capacity() -> NonZeroUsize {
        NonZeroUsize::new(2).assured("two is nonzero")
    }

    /// The channel subscription every channel fixture confirms.
    fn events() -> RedisSubscriptionTarget {
        RedisSubscriptionTarget::Channel(
            RedisChannelName::parse("events").assured("a plain channel is within the bound"),
        )
    }

    /// A pattern subscription.
    fn pattern(text: &str) -> RedisSubscriptionTarget {
        RedisSubscriptionTarget::Pattern(
            RedisChannelPattern::parse(text).assured("the fixture pattern is within the bound"),
        )
    }

    #[nervix_primitives::test]
    async fn a_two_message_handoff_preserves_every_binary_payload_and_its_order() {
        let (client, server) = tokio::io::duplex(128);
        let peer = spawn(async move {
            let mut peer = Peer::new(server, ProtocolVersion::RESP2);
            peer.subscribe().await;
            for payload in 0..=u8::MAX {
                consume_budget().await;
                peer.message(payload).await;
            }
            peer.ended().await;
        });
        let settings = RedisConnectionInfo::default();
        let mut subscription =
            RedisSubscription::open(Box::new(client), &settings, &events(), capacity(), idle())
                .await
                .assured("the peer acknowledges its subscription");
        assert_eq!(subscription.messages.max_capacity(), capacity().get());
        for payload in 0..=u8::MAX {
            consume_budget().await;
            let received = subscription
                .next_message()
                .await
                .assured("each byte the peer writes is handed over once");
            assert_eq!(received.channel(), b"events");
            assert_eq!(received.pattern(), None);
            assert_eq!(received.payload(), &[payload]);
        }
        subscription
            .close()
            .await
            .assured("closing joins the reader");
        peer.await.assured("the peer observes transport closure");
    }

    #[nervix_primitives::test]
    async fn a_subscription_read_preserves_the_typed_server_rejection() {
        let (client, server) = tokio::io::duplex(128);
        let peer = spawn(async move {
            let mut peer = Peer::new(server, ProtocolVersion::RESP2);
            peer.subscribe().await;
            peer.answer(b"-NOPERM subscription permission revoked\r\n")
                .await;
            peer.ended().await;
        });
        let mut subscription = RedisSubscription::open(
            Box::new(client),
            &RedisConnectionInfo::default(),
            &events(),
            capacity(),
            idle(),
        )
        .await
        .assured("the peer acknowledges its subscription before rejecting a read");
        let error = match subscription.next_message().await {
            Ok(_) => panic!("the peer rejected the read instead of delivering a message"),
            Err(error) => error,
        };
        let cause = error
            .downcast_ref::<redis::RedisError>()
            .assured("a server rejection retains the Redis driver's typed cause");
        assert_eq!(cause.code(), Some("NOPERM"));
        drop(subscription);
        peer.await
            .assured("the failed subscription reader releases its transport");
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_delayed_quiet_timer_cannot_restart_the_idle_budget() {
        let (client, _peer) = tokio::io::duplex(128);
        let stream: ConnectedStream = Box::new(client);
        let (input, output) = tokio::io::split(stream);
        let mut reader = SubscriptionReader {
            input,
            output,
            decoder: Decoder::new(),
            idle_timeout: idle(),
        };
        let started = Instant::now();
        let mut reading = std::pin::pin!(reader.next_value());
        assert!(futures_util::poll!(&mut reading).is_pending());
        advance(Duration::from_secs(2)).await;
        let error = reading
            .await
            .expect_err("the silent peer has exceeded the whole one-second idle budget");
        let cause = error
            .downcast_ref::<DeadlineElapsed>()
            .assured("the elapsed whole idle budget retains its typed cause");
        assert_eq!(cause.key, DeadlineKey::IdleTimeout);
        assert_eq!(cause.bound, Duration::from_secs(1));
        assert_eq!(
            Instant::now().duration_since(started),
            Duration::from_secs(2)
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_healthy_idle_subscription_answers_probes_on_its_own_connection() {
        for protocol in [ProtocolVersion::RESP2, ProtocolVersion::RESP3] {
            consume_budget().await;
            let (client, server) = tokio::io::duplex(128);
            let peer = spawn(async move {
                let mut peer = Peer::new(server, protocol);
                if protocol == ProtocolVersion::RESP3 {
                    peer.command(&["HELLO", "3"]).await;
                    peer.answer(b"%0\r\n").await;
                }
                peer.subscribe().await;
                for _ in 0..3 {
                    consume_budget().await;
                    peer.command(&["PING"]).await;
                    let reply: &[u8] = if protocol == ProtocolVersion::RESP3 {
                        b"+PONG\r\n"
                    } else {
                        b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"
                    };
                    peer.answer(reply).await;
                }
                peer.message(u8::MAX).await;
                peer.ended().await;
            });
            let settings = RedisConnectionInfo::default().set_protocol(protocol);
            let mut subscription =
                RedisSubscription::open(Box::new(client), &settings, &events(), capacity(), idle())
                    .await
                    .assured("the peer acknowledges its subscription");
            let message = subscription
                .next_message()
                .await
                .assured("answered probes keep the subscription alive until its message arrives");
            assert_eq!(message.payload(), &[u8::MAX]);
            subscription
                .close()
                .await
                .assured("closing joins the idle reader");
            peer.await
                .assured("all three probes used the subscription socket");
        }
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn an_unanswered_probe_keeps_the_declared_idle_deadline_as_its_typed_cause() {
        let (client, server) = tokio::io::duplex(128);
        let peer = spawn(async move {
            let mut peer = Peer::new(server, ProtocolVersion::RESP2);
            peer.subscribe().await;
            peer.command(&["PING"]).await;
            peer.ended().await;
        });
        let settings = RedisConnectionInfo::default();
        let mut subscription =
            RedisSubscription::open(Box::new(client), &settings, &events(), capacity(), idle())
                .await
                .assured("the peer acknowledges its subscription");
        let error = subscription
            .next_message()
            .await
            .expect_err("the peer never answers PING");
        assert!(matches!(
            error.current_context(),
            RedisPubSubIngestorError::IdleProbe
        ));
        let deadline = error
            .downcast_ref::<DeadlineElapsed>()
            .assured("the idle failure retains its typed configured deadline");
        assert_eq!(deadline.key, DeadlineKey::IdleTimeout);
        assert_eq!(deadline.bound, idle().bound());
        drop(subscription);
        peer.await.assured("the expired reader closes its socket");
    }

    #[nervix_primitives::test]
    async fn both_protocols_authenticate_select_the_database_and_preserve_messages() {
        for protocol in [ProtocolVersion::RESP2, ProtocolVersion::RESP3] {
            consume_budget().await;
            let (client, server) = tokio::io::duplex(128);
            let peer = spawn(async move {
                let mut peer = Peer::new(server, protocol);
                match protocol {
                    ProtocolVersion::RESP2 => {
                        peer.command(&["AUTH", "alice", "fixture-password"]).await;
                        peer.answer(b"+OK\r\n").await;
                    }
                    ProtocolVersion::RESP3 => {
                        peer.command(&["HELLO", "3", "AUTH", "alice", "fixture-password"])
                            .await;
                        peer.answer(b"%0\r\n").await;
                    }
                    _ => panic!("the fixture constructs only RESP2 and RESP3 peers"),
                }
                peer.command(&["SELECT", "3"]).await;
                peer.answer(b"+OK\r\n").await;
                peer.subscribe().await;
                peer.message(73).await;
                peer.ended().await;
            });
            let settings = RedisConnectionInfo::default()
                .set_protocol(protocol)
                .set_username("alice")
                .set_password("fixture-password")
                .set_db(3);
            let mut subscription =
                RedisSubscription::open(Box::new(client), &settings, &events(), capacity(), idle())
                    .await
                    .assured("the peer accepts the configured authentication and database");
            let message = subscription
                .next_message()
                .await
                .assured("the selected subscription receives its message");
            assert_eq!(message.payload(), &[73]);
            subscription
                .close()
                .await
                .assured("closing joins the authenticated reader");
            peer.await
                .assured("the peer observes the authenticated connection ending");
        }
    }

    #[nervix_primitives::test]
    async fn a_pattern_subscription_hands_over_each_channel_with_the_pattern_that_matched_it() {
        for protocol in [ProtocolVersion::RESP2, ProtocolVersion::RESP3] {
            consume_budget().await;
            let (client, server) = tokio::io::duplex(256);
            let peer = spawn(async move {
                let mut peer = Peer::new(server, protocol);
                if protocol == ProtocolVersion::RESP3 {
                    peer.command(&["HELLO", "3"]).await;
                    peer.answer(b"%0\r\n").await;
                }
                peer.psubscribe("orders.*").await;
                peer.pmessage(b"orders.*", b"orders.eu", b"eu").await;
                peer.pmessage(b"orders.*", b"orders.\xff", b"binary").await;
                peer.ended().await;
            });
            let settings = RedisConnectionInfo::default().set_protocol(protocol);
            let mut subscription = RedisSubscription::open(
                Box::new(client),
                &settings,
                &pattern("orders.*"),
                capacity(),
                idle(),
            )
            .await
            .assured("the peer confirms the pattern subscription");
            let first = subscription
                .next_message()
                .await
                .assured("the first matching message is handed over");
            assert_eq!(first.channel(), b"orders.eu");
            assert_eq!(first.pattern(), Some(&b"orders.*"[..]));
            assert_eq!(first.payload(), b"eu");
            let second = subscription
                .next_message()
                .await
                .assured("a channel that is not UTF-8 is handed over with its exact bytes");
            assert_eq!(second.channel(), b"orders.\xff");
            assert_eq!(second.pattern(), Some(&b"orders.*"[..]));
            assert_eq!(second.payload(), b"binary");
            subscription
                .close()
                .await
                .assured("closing joins the pattern reader");
            peer.await
                .assured("the peer observes the pattern subscription ending");
        }
    }

    #[nervix_primitives::test]
    async fn a_reply_other_than_this_subscriptions_confirmation_is_refused() {
        // A confirmation of a channel subscription, then a plain status reply that confirms no
        // subscription at all.
        let replies: [&'static [u8]; 2] = [
            b"*3\r\n$9\r\nsubscribe\r\n$6\r\nevents\r\n:1\r\n",
            b"+OK\r\n",
        ];
        for reply in replies {
            consume_budget().await;
            let (client, server) = tokio::io::duplex(128);
            let peer = spawn(async move {
                let mut peer = Peer::new(server, ProtocolVersion::RESP2);
                peer.command(&["PSUBSCRIBE", "events"]).await;
                peer.answer(reply).await;
                peer.ended().await;
            });
            let error = match RedisSubscription::open(
                Box::new(client),
                &RedisConnectionInfo::default(),
                &pattern("events"),
                capacity(),
                idle(),
            )
            .await
            {
                Ok(_) => panic!("only a psubscribe confirmation confirms a pattern subscription"),
                Err(error) => error,
            };
            assert!(matches!(
                error.current_context(),
                RedisPubSubIngestorError::UnexpectedResponse {
                    operation: "PSUBSCRIBE"
                }
            ));
            peer.await
                .assured("the refused subscription releases its transport");
        }
    }

    #[nervix_primitives::test]
    async fn a_reply_that_is_neither_a_delivery_nor_a_probe_answer_ends_the_reader() {
        let (client, server) = tokio::io::duplex(128);
        let peer = spawn(async move {
            let mut peer = Peer::new(server, ProtocolVersion::RESP2);
            peer.subscribe().await;
            peer.answer(b"*3\r\n$11\r\nunsubscribe\r\n$6\r\nevents\r\n:0\r\n")
                .await;
            peer.ended().await;
        });
        let mut subscription = RedisSubscription::open(
            Box::new(client),
            &RedisConnectionInfo::default(),
            &events(),
            capacity(),
            idle(),
        )
        .await
        .assured("the peer acknowledges its subscription before it ends it");
        let error = match subscription.next_message().await {
            Ok(_) => panic!("an unsubscribe confirmation is not a delivered message"),
            Err(error) => error,
        };
        assert!(matches!(
            error.current_context(),
            RedisPubSubIngestorError::UnexpectedResponse {
                operation: "receiving a subscription message"
            }
        ));
        drop(subscription);
        peer.await
            .assured("the ended subscription reader releases its transport");
    }

    #[test]
    fn only_a_complete_channel_or_pattern_delivery_is_a_delivered_message() {
        let bulk = |bytes: &[u8]| Value::BulkString(bytes.to_vec());
        let delivered = DeliveredMessage::from_value(Value::Array(vec![
            bulk(b"pmessage"),
            bulk(b"o*"),
            bulk(b"orders"),
            bulk(b"payload"),
        ]))
        .assured("a RESP2 pattern delivery carries a message");
        assert_eq!(delivered.pattern(), Some(&b"o*"[..]));
        assert_eq!(delivered.channel(), b"orders");
        assert_eq!(delivered.payload(), b"payload");
        assert_eq!(
            format!("{delivered:?}"),
            "DeliveredMessage { .. }",
            "a delivery prints none of its pattern, channel or payload bytes"
        );
        let pushed = DeliveredMessage::from_value(Value::Push {
            kind: PushKind::Message,
            data: vec![bulk(b"orders"), bulk(b"payload")],
        })
        .assured("a RESP3 channel delivery carries a message");
        assert_eq!(pushed.pattern(), None);
        assert_eq!(pushed.channel(), b"orders");
        assert_eq!(pushed.payload(), b"payload");
        for value in [
            Value::Array(vec![bulk(b"message"), bulk(b"orders")]),
            Value::Array(vec![
                bulk(b"message"),
                bulk(b"orders"),
                bulk(b"payload"),
                bulk(b"extra"),
            ]),
            Value::Array(vec![bulk(b"subscribe"), bulk(b"orders"), Value::Int(1)]),
            Value::Array(vec![bulk(b"message"), Value::Int(1), bulk(b"payload")]),
            Value::Push {
                kind: PushKind::SMessage,
                data: vec![bulk(b"orders"), bulk(b"payload")],
            },
            Value::SimpleString("PONG".to_string()),
        ] {
            assert!(
                DeliveredMessage::from_value(value).is_none(),
                "only a complete channel or pattern delivery is a message"
            );
        }
    }
}
