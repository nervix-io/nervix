//! A loopback NATS server a check scripts operation by operation, for the NATS connector's checks.
//!
//! Outside the layer order: a test harness.
//!
//! - **Owns.** One loopback listener, whether it greets or ends the connections a client opens to
//!   it, and the protocol operations a check reads from a greeted connection or writes to it.
//! - **Depends on.** Tokio's TCP sockets and I/O traits, and the boundary's tasks and channels.
//! - **Must not know.** The connector's ingestor or emitter, the runtime host, or how a real server
//!   decides.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{
    net::{
        TcpListener, TcpSocket,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::oneshot,
    task::JoinHandle,
};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};

/// The prefix of the subjects a client receives the replies to its requests on.
const INBOX_PREFIX: &str = "_INBOX.";
/// The suffix of the one wildcard subscription a client receives every reply to its requests on.
const REQUEST_MUXER_SUFFIX: &str = ".*";

/// A server on a loopback port that answers only what its check tells it to.
pub(crate) struct ScriptedServer {
    listener: TcpListener,
}

/// A scripted server that went down. It keeps its port and ends every connection a client opens
/// before it greets it, as a server that stopped, until it comes back.
pub(crate) struct DownServer {
    back: oneshot::Sender<()>,
    refusing: JoinHandle<TcpListener>,
}

/// The connection a client opened to a scripted server.
pub(crate) struct ScriptedConnection {
    read: BufReader<OwnedReadHalf>,
    write: OwnedWriteHalf,
    /// The subscription the client receives the replies to its requests through, once it made it.
    inbox: Option<Subscription>,
}

/// A subscription the client made: the subject it listens on and the id it named it by.
pub(crate) struct Subscription {
    subject: String,
    sid: String,
}

/// A message the client published: the subject it published to, the subject it asked the reply
/// on, if any, and the bytes it carries, its header block first when it has one.
pub(crate) struct Publish {
    pub(crate) subject: String,
    pub(crate) reply: Option<String>,
    pub(crate) message: Vec<u8>,
}

/// An operation the client sent that a check reads.
enum Operation {
    Subscribe(Subscription),
    Publish(Publish),
}

impl ScriptedServer {
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port is available");
        Self { listener }
    }

    /// A server whose receive window keeps a large publish partly in its client's write buffer.
    pub(crate) async fn with_receive_buffer(bytes: u32) -> Self {
        let socket = TcpSocket::new_v4().assured("a loopback socket can be created");
        socket
            .set_recv_buffer_size(bytes)
            .assured("the scripted server can bound its receive buffer");
        socket
            .bind("127.0.0.1:0".parse().assured("a literal loopback address"))
            .assured("a loopback port is available");
        let listener = socket.listen(1).assured("the bound socket can listen");
        Self { listener }
    }

    /// The address a client dials to reach this server.
    pub(crate) fn addr(&self) -> String {
        let address = self
            .listener
            .local_addr()
            .expect("a bound listener has an address");
        format!("nats://{address}")
    }

    /// Accepts the client's connection, greets it with a `max_payload` of `max_payload` bytes and
    /// answers its CONNECT. The server stops listening, so a later attempt to connect is refused.
    pub(crate) async fn accept(self, max_payload: usize) -> ScriptedConnection {
        self.greet(max_payload).await
    }

    /// Goes down without giving up its port: every connection a client opens from now on ends
    /// before the server greets it, until [`DownServer::come_back`].
    pub(crate) fn go_down(self) -> DownServer {
        let (back, back_rx) = oneshot::channel();
        let refusing = nervix_primitives::task::spawn(Self::refuse(self.listener, back_rx));
        DownServer { back, refusing }
    }

    /// Ends every connection a client opens on `listener` until the server comes back, and then
    /// hands the listener back.
    async fn refuse(listener: TcpListener, mut back: oneshot::Receiver<()>) -> TcpListener {
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                _ = &mut back => return listener,
                accepted = listener.accept() => {
                    let (connection, _) =
                        accepted.expect("the scripted server accepts each connection it ends");
                    drop(connection);
                }
            }
        }
    }

    /// Accepts the client's next connection, greets it with a `max_payload` of `max_payload` bytes
    /// and answers its CONNECT, keeping the server's port.
    pub(crate) async fn greet(&self, max_payload: usize) -> ScriptedConnection {
        let (stream, _) = self
            .listener
            .accept()
            .await
            .expect("the client connects to the scripted server");
        let (read, write) = stream.into_split();
        let mut connection = ScriptedConnection {
            read: BufReader::new(read),
            write,
            inbox: None,
        };
        let info = serde_json::json!({
            "server_id": "scripted",
            "max_payload": max_payload,
            "proto": 1,
            "headers": true,
        });
        connection.send(format!("INFO {info}\r\n").as_bytes()).await;
        // The client sends CONNECT and then PING, and is connected once the PING is answered.
        connection.answer_ping().await;
        connection
    }
}

impl DownServer {
    /// Comes back on the port it kept, so the next connection a client opens is greeted.
    pub(crate) async fn come_back(self) -> ScriptedServer {
        self.back
            .send(())
            .expect("the refusing task waits for the server to come back");
        let listener = self
            .refusing
            .await
            .expect("the refusing task hands its listener back");
        ScriptedServer { listener }
    }
}

impl ScriptedConnection {
    /// Reads only a publish's operation line and prefix, leaving its remaining bytes unread.
    pub(crate) async fn expect_publish_prefix(&mut self, prefix: &[u8]) -> usize {
        let line = self.read_line().await;
        let mut tokens = line.split_whitespace();
        assert_eq!(tokens.next(), Some("PUB"), "{line:?}");
        let size = tokens
            .last()
            .assured("a publish names its payload length")
            .parse::<usize>()
            .assured("the payload length is a number");
        assert!(prefix.len() < size, "the check leaves payload bytes unread");
        let mut received = vec![0; prefix.len()];
        self.read
            .read_exact(&mut received)
            .await
            .assured("the client sends the beginning of its publish");
        assert_eq!(received, prefix);
        size
    }

    /// Reads what the client sends until its next subscription that is not its inbox, answering
    /// each PING on the way.
    pub(crate) async fn expect_subscription(&mut self) -> Subscription {
        match self.next_operation().await {
            Operation::Subscribe(subscription) => subscription,
            Operation::Publish(_) => panic!("the client published before it subscribed"),
        }
    }

    /// Reads what the client sends until its next publish, answering each PING on the way.
    pub(crate) async fn expect_publish(&mut self) -> Publish {
        match self.next_operation().await {
            Operation::Publish(publish) => publish,
            Operation::Subscribe(subscription) => {
                panic!(
                    "the client subscribed to '{}' instead of publishing",
                    subscription.subject
                )
            }
        }
    }

    /// Reads what the client sends until it unsubscribes `subscription`, answering no PING on the
    /// way, so a check can deliver what a server sends before it processes the unsubscription.
    pub(crate) async fn expect_unsubscribe(&mut self, subscription: &Subscription) {
        loop {
            let line = self.read_line().await;
            let tokens = line.split_whitespace().collect::<Vec<_>>();
            let Some(operation) = tokens.first() else {
                continue;
            };
            if operation.eq_ignore_ascii_case("UNSUB") {
                assert_eq!(
                    tokens.get(1).copied(),
                    Some(subscription.sid.as_str()),
                    "the client unsubscribed another subscription: {line:?}"
                );
                return;
            }
            assert!(
                !operation.eq_ignore_ascii_case("PING"),
                "the client pinged before it unsubscribed: {line:?}"
            );
        }
    }

    /// Pings the client and reads what it sends until it answers. The client answers only once it
    /// has read everything sent before the ping, so a check knows it holds those messages.
    pub(crate) async fn ping_client(&mut self) {
        self.send(b"PING\r\n").await;
        loop {
            let line = self.read_line().await;
            if line.trim_end().eq_ignore_ascii_case("PONG") {
                return;
            }
            assert!(
                !line.to_ascii_uppercase().starts_with("PUB")
                    && !line.to_ascii_uppercase().starts_with("HPUB"),
                "the client published while the check waited for its answer: {line:?}"
            );
        }
    }

    /// Reads what the client sends until a PING, and answers it.
    pub(crate) async fn answer_ping(&mut self) {
        loop {
            let line = self.read_line().await;
            if line.trim_end().eq_ignore_ascii_case("PING") {
                self.send(b"PONG\r\n").await;
                return;
            }
            assert!(
                !line.to_ascii_uppercase().starts_with("PUB")
                    && !line.to_ascii_uppercase().starts_with("HPUB"),
                "the client published while the check waited for its ping: {line:?}"
            );
        }
    }

    /// Delivers `payload` on `subscription`.
    pub(crate) async fn deliver(&mut self, subscription: &Subscription, payload: &[u8]) {
        let mut message = format!(
            "MSG {} {} {}\r\n",
            subscription.subject,
            subscription.sid,
            payload.len()
        )
        .into_bytes();
        message.extend_from_slice(payload);
        message.extend_from_slice(b"\r\n");
        self.send(&message).await;
    }

    /// Delivers `payload` on `subscription` with `reply` as the subject the client answers it on.
    pub(crate) async fn deliver_for_reply(
        &mut self,
        subscription: &Subscription,
        reply: &str,
        payload: &[u8],
    ) {
        let mut message = format!(
            "MSG {} {} {reply} {}\r\n",
            subscription.subject,
            subscription.sid,
            payload.len()
        )
        .into_bytes();
        message.extend_from_slice(payload);
        message.extend_from_slice(b"\r\n");
        self.send(&message).await;
    }

    /// Delivers a status message with `code` and `description` and no payload on `subscription`,
    /// as a server answers a pull request it ends or refuses.
    pub(crate) async fn deliver_status(
        &mut self,
        subscription: &Subscription,
        code: u16,
        description: &str,
    ) {
        let header = format!("NATS/1.0 {code} {description}\r\n\r\n");
        let mut message = format!(
            "HMSG {} {} {} {}\r\n",
            subscription.subject,
            subscription.sid,
            header.len(),
            header.len()
        )
        .into_bytes();
        message.extend_from_slice(header.as_bytes());
        message.extend_from_slice(b"\r\n");
        self.send(&message).await;
    }

    /// Replies `reply` to `publish` on the inbox the client receives its replies through.
    pub(crate) async fn reply(&mut self, publish: &Publish, reply: &str) {
        let subject = publish
            .reply
            .as_deref()
            .expect("the publish asked for a reply");
        let sid = self
            .inbox
            .as_ref()
            .expect("a client subscribes to its inbox before it asks for a reply")
            .sid
            .clone();
        let mut message = format!("MSG {subject} {sid} {}\r\n", reply.len()).into_bytes();
        message.extend_from_slice(reply.as_bytes());
        message.extend_from_slice(b"\r\n");
        self.send(&message).await;
    }

    /// Ends the connection, as a server that went down.
    pub(crate) async fn end(mut self) {
        self.write
            .shutdown()
            .await
            .expect("the scripted server ends its connection");
    }

    /// The next subscription or publish the client sends, answering each PING and noting the
    /// client's inbox on the way.
    async fn next_operation(&mut self) -> Operation {
        loop {
            let line = self.read_line().await;
            let tokens = line.split_whitespace().collect::<Vec<_>>();
            let Some(operation) = tokens.first() else {
                continue;
            };
            match operation.to_ascii_uppercase().as_str() {
                "PING" => self.send(b"PONG\r\n").await,
                "SUB" => {
                    let (Some(subject), Some(sid)) = (tokens.get(1), tokens.last()) else {
                        panic!("a SUB names its subject and its id: {line:?}");
                    };
                    let subscription = Subscription {
                        subject: (*subject).to_string(),
                        sid: (*sid).to_string(),
                    };
                    // The client receives the replies to its requests on one wildcard subscription;
                    // any other subscription, an inbox it reads deliveries on included, is the
                    // check's to see.
                    if subject.starts_with(INBOX_PREFIX) && subject.ends_with(REQUEST_MUXER_SUFFIX)
                    {
                        self.inbox = Some(subscription);
                        continue;
                    }
                    return Operation::Subscribe(subscription);
                }
                "PUB" | "HPUB" => return Operation::Publish(self.read_publish(&tokens).await),
                _ => {}
            }
        }
    }

    /// Reads the message a `PUB <subject> [reply] <size>` or `HPUB <subject> [reply] <header size>
    /// <size>` line announces.
    async fn read_publish(&mut self, tokens: &[&str]) -> Publish {
        let subject = tokens
            .get(1)
            .expect("a publish names its subject")
            .to_string();
        let is_headers = tokens
            .first()
            .is_some_and(|operation| operation.eq_ignore_ascii_case("HPUB"));
        let fixed_tokens = if is_headers { 4 } else { 3 };
        let reply = if tokens.len() > fixed_tokens {
            tokens.get(2).map(|reply| (*reply).to_string())
        } else {
            None
        };
        let size = tokens
            .last()
            .expect("a publish names its size")
            .parse::<usize>()
            .expect("a publish's size is a number");
        let framed = size
            .checked_add(2)
            .expect("a message these checks publish is far shorter than usize counts");
        let mut message = vec![0_u8; framed];
        self.read
            .read_exact(&mut message)
            .await
            .expect("the client sends the message its publish announced");
        message.truncate(size);
        Publish {
            subject,
            reply,
            message,
        }
    }

    async fn read_line(&mut self) -> String {
        let mut line = String::new();
        let read = self
            .read
            .read_line(&mut line)
            .await
            .expect("the scripted server reads what its client sends");
        assert_ne!(read, 0, "the client ended its connection");
        line
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.write
            .write_all(bytes)
            .await
            .expect("the scripted server writes to its client");
    }
}
