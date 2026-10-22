//! One bounded Redis subscription and its exact transport lifetime.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Protocol setup, the sole RESP reader, its bounded message handoff, idle probes on
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
use nervix_models::ConnectorKind;
use nervix_primitives::{
    sync::mpsc,
    task::{AbortOnDropHandle, consume_budget, spawn},
    time::{sleep, timeout},
};
use nervix_recovery::{Discarded as _, NoReceiver as _};
use redis::{Cmd, Msg, ProtocolVersion, PushKind, RedisConnectionInfo, Value};
use tokio::io::{AsyncWriteExt as _, ReadHalf, WriteHalf};

use super::{ConnectedStream, RedisPubSubSourceError, RedisPubSubSourceResult};

type RedisDecoder = Decoder<AnySendSyncPartialState, PointerOffset<[u8]>>;

/// The source retains this handle while the reader owns the socket and a bounded sender.
pub(super) struct RedisSubscription {
    messages: mpsc::Receiver<Msg>,
    reader: AbortOnDropHandle<RedisPubSubSourceResult<()>>,
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
        channel: &str,
        capacity: NonZeroUsize,
        idle_timeout: ConfiguredDeadline,
    ) -> RedisPubSubSourceResult<Self> {
        let (input, output) = tokio::io::split(stream);
        let mut transport = SubscriptionReader {
            input,
            output,
            decoder: Decoder::new(),
            idle_timeout,
        };
        transport.setup(settings).await?;
        transport.subscribe(channel).await?;
        let (sender, messages) = mpsc::channel(capacity.get());
        let reader = AbortOnDropHandle::new(spawn(transport.forward(sender)));
        Ok(Self { messages, reader })
    }

    pub(super) async fn next_message(&mut self) -> RedisPubSubSourceResult<Msg> {
        if let Some(message) = self.messages.recv().await {
            return Ok(message);
        }
        match (&mut self.reader).await {
            Ok(Err(error)) => Err(error),
            Ok(Ok(())) => Err(Report::new(RedisPubSubSourceError::SubscriptionClosed)),
            Err(error) => {
                Err(Report::new(error).change_context(RedisPubSubSourceError::SubscriptionTask))
            }
        }
    }

    pub(super) async fn close(self) -> RedisPubSubSourceResult<()> {
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
                Err(Report::new(error).change_context(RedisPubSubSourceError::SubscriptionTask))
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
    async fn setup(&mut self, settings: &RedisConnectionInfo) -> RedisPubSubSourceResult<()> {
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
                return Err(Report::new(RedisPubSubSourceError::UnexpectedResponse {
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

    fn expect_ok(&self, response: Value, operation: &'static str) -> RedisPubSubSourceResult<()> {
        match response {
            Value::Okay => Ok(()),
            Value::SimpleString(value) if value == "OK" => Ok(()),
            _ => Err(Report::new(RedisPubSubSourceError::UnexpectedResponse {
                operation,
            })),
        }
    }

    async fn command(&mut self, command: &Cmd) -> RedisPubSubSourceResult<Value> {
        self.output
            .write_all(&command.get_packed_command())
            .await
            .change_context(RedisPubSubSourceError::SubscriptionSetup)?;
        let response = redis::parse_redis_value_async(&mut self.decoder, &mut self.input)
            .await
            .change_context(RedisPubSubSourceError::SubscriptionSetup)?;
        response
            .extract_error()
            .change_context(RedisPubSubSourceError::SubscriptionSetup)
    }

    async fn subscribe(&mut self, channel: &str) -> RedisPubSubSourceResult<()> {
        let mut command = redis::cmd("SUBSCRIBE");
        command.arg(channel);
        let response = self.command(&command).await?;
        let subscribed = match response {
            Value::Array(parts) => matches!(parts.as_slice(),
                [Value::BulkString(kind), Value::BulkString(name), Value::Int(1)]
                if kind == b"subscribe" && name == channel.as_bytes()),
            Value::Push {
                kind: PushKind::Subscribe,
                data,
            } => matches!(data.as_slice(),
                [Value::BulkString(name), Value::Int(1)] if name == channel.as_bytes()),
            _ => false,
        };
        if !subscribed {
            return Err(Report::new(RedisPubSubSourceError::UnexpectedResponse {
                operation: "SUBSCRIBE",
            }));
        }
        Ok(())
    }

    async fn forward(mut self, sender: mpsc::Sender<Msg>) -> RedisPubSubSourceResult<()> {
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
                .change_context(RedisPubSubSourceError::SubscriptionRead)?;
            let pong = self.is_pong(&response);
            if let Some(message) = Msg::from_owned_value(response) {
                permit.send(message);
            } else if !pong {
                return Err(Report::new(RedisPubSubSourceError::UnexpectedResponse {
                    operation: "receiving a channel message",
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
    async fn next_value(&mut self) -> RedisPubSubSourceResult<Value> {
        let idle = self.idle_timeout;
        let quiet = idle.bound() / 2;
        let budget = OperationBudget::start(idle.bound(), LookupShare::Whole);
        let mut incoming = std::pin::pin!(redis::parse_redis_value_async(
            &mut self.decoder,
            &mut self.input
        ));
        nervix_primitives::select! {
            biased;
            response = &mut incoming => response.change_context(RedisPubSubSourceError::SubscriptionRead),
            _ = sleep(quiet) => {
                let elapsed = || Report::new(idle.elapsed("waiting for subscription activity"))
                    .change_context(RedisPubSubSourceError::IdleProbe);
                if budget.remaining().is_zero() {
                    return Err(elapsed());
                }
                timeout(budget.remaining(), self.output.write_all(&redis::cmd("PING").get_packed_command()))
                    .await.map_err(|_| elapsed())?
                    .change_context(RedisPubSubSourceError::IdleProbe)?;
                timeout(budget.remaining(), &mut incoming).await.map_err(|_| elapsed())?
                    .change_context(RedisPubSubSourceError::SubscriptionRead)
            }
        }
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
    use nervix_models::{ClientConfigEntry, DeadlineKey};
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
            RedisSubscription::open(Box::new(client), &settings, "events", capacity(), idle())
                .await
                .assured("the peer acknowledges its subscription");
        assert_eq!(subscription.messages.max_capacity(), capacity().get());
        for payload in 0..=u8::MAX {
            consume_budget().await;
            let received = subscription
                .next_message()
                .await
                .assured("each byte the peer writes is handed over once");
            let channel: Vec<u8> = received
                .get_channel()
                .assured("the peer's channel is a binary Redis string");
            assert_eq!(channel, b"events");
            assert_eq!(received.get_payload_bytes(), &[payload]);
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
            "events",
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
                RedisSubscription::open(Box::new(client), &settings, "events", capacity(), idle())
                    .await
                    .assured("the peer acknowledges its subscription");
            let message = subscription
                .next_message()
                .await
                .assured("answered probes keep the subscription alive until its message arrives");
            assert_eq!(message.get_payload_bytes(), &[u8::MAX]);
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
            RedisSubscription::open(Box::new(client), &settings, "events", capacity(), idle())
                .await
                .assured("the peer acknowledges its subscription");
        let error = subscription
            .next_message()
            .await
            .expect_err("the peer never answers PING");
        assert!(matches!(
            error.current_context(),
            RedisPubSubSourceError::IdleProbe
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
                RedisSubscription::open(Box::new(client), &settings, "events", capacity(), idle())
                    .await
                    .assured("the peer accepts the configured authentication and database");
            let message = subscription
                .next_message()
                .await
                .assured("the selected subscription receives its message");
            assert_eq!(message.get_payload_bytes(), &[73]);
            subscription
                .close()
                .await
                .assured("closing joins the authenticated reader");
            peer.await
                .assured("the peer observes the authenticated connection ending");
        }
    }
}
