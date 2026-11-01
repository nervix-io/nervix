//! A NATS endpoint a scenario stands in front of the NATS server it shares with other scenarios.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One loopback listener, the connections it forwards to the server, the NATS protocol
//!   framing of what a client sends through it, whether it forwards, holds or refuses them, and how
//!   many it refused.
//! - **Depends on.** Tokio's TCP sockets and I/O traits, and the boundary's tasks, timers,
//!   cancellation and watch channel.
//! - **Must not know.** Nervix, or what a client sends beyond its protocol operation and its size.
//!
//! The endpoint takes the server away from the clients that connect through it without touching
//! the server the other scenarios share. Going down ends every connection it forwards and refuses
//! new ones, as a server that stopped, and counts each connection it refuses, so a scenario knows a
//! client found it down. Holding stops reading what each client sends once it forwarded a number of
//! publishes, as a server that stopped reading, while everything the server sends still reaches the
//! client: the client keeps the answers to what was forwarded, and what it sends after them backs
//! up in its own buffers.

use std::{future::Future, io, net::SocketAddr, time::Duration};

use meticulous::OptionExt as _;
use nervix_primitives::{
    net::{TcpListener, TcpStream},
    sync::{CancellationToken, watch},
    task::JoinHandle,
    time::{Instant, sleep},
};
use nervix_recovery::Discarded as _;
use tokio::io::{
    AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
};

/// The longest operation line a client sends in these scenarios, which bounds what one frame's line
/// may take: a `CONNECT` with its options is the longest, at a few hundred bytes.
const MAX_OPERATION_LINE: u64 = 64 * 1024;

/// What the endpoint does with the connections clients open through it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NatsEndpointMode {
    /// Every connection is forwarded in both directions.
    Forwarding,
    /// Each connection forwards what its client sends until it forwarded `publishes` publishes
    /// after the endpoint entered this mode, and then stops reading from its client.
    HoldingAfter { publishes: usize },
    /// Forward each publish but discard server replies after the next one, until forwarding resumes.
    DroppingRepliesAfterNextPublish,
    /// Every connection is ended and every new one refused.
    Down,
}

/// A listener that forwards every client connection to one NATS server unless the scenario holds
/// what clients send or takes the endpoint down.
#[derive(Debug)]
pub(crate) struct NatsEndpoint {
    local_addr: SocketAddr,
    mode: watch::Sender<NatsEndpointMode>,
    /// How many connections the endpoint refused while it was down.
    refused: watch::Sender<u64>,
    /// Publishes this endpoint has read but is holding instead of forwarding.
    held: watch::Sender<u64>,
    /// Data publishes forwarded to the server through this endpoint, excluding JetStream API requests.
    forwarded: watch::Sender<u64>,
    /// Server response chunks discarded after a data publish.
    discarded: watch::Sender<u64>,
    /// Cancelled to end every connection the endpoint forwards, when the endpoint stops.
    connections: CancellationToken,
    listener: JoinHandle<()>,
}

impl NatsEndpoint {
    /// Listens on a loopback port and forwards every connection it accepts to `server`.
    pub(crate) async fn start(server: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let local_addr = listener.local_addr()?;
        let (mode, observed) = watch::channel(NatsEndpointMode::Forwarding);
        let (refused, _) = watch::channel(0_u64);
        let (held, _) = watch::channel(0_u64);
        let (forwarded, _) = watch::channel(0_u64);
        let (discarded, _) = watch::channel(0_u64);
        let connections = CancellationToken::new();
        let listener = nervix_primitives::task::spawn(Self::accept(
            listener,
            server,
            observed,
            refused.clone(),
            held.clone(),
            forwarded.clone(),
            discarded.clone(),
            connections.clone(),
        ));
        Ok(Self {
            local_addr,
            mode,
            refused,
            held,
            forwarded,
            discarded,
            connections,
            listener,
        })
    }

    /// The loopback port clients connect to instead of the server's.
    pub(crate) fn local_port(&self) -> u16 {
        self.local_addr.port()
    }

    /// Forwards everything clients send from now on, and accepts new connections again.
    pub(crate) fn forward_everything(&self) {
        self.mode.send_replace(NatsEndpointMode::Forwarding);
    }

    /// Lets each connection forward `publishes` more publishes from its client, and then stops
    /// reading what that client sends.
    pub(crate) fn hold_after(&self, publishes: usize) {
        self.mode
            .send_replace(NatsEndpointMode::HoldingAfter { publishes });
    }

    /// Lose the next publish's PubAck while still letting the stream store its message.
    pub(crate) fn drop_replies_after_next_publish(&self) {
        self.mode
            .send_replace(NatsEndpointMode::DroppingRepliesAfterNextPublish);
    }

    pub(crate) async fn forwarded_publishes_at_least(&self, count: u64) {
        let mut forwarded = self.forwarded.subscribe();
        forwarded
            .wait_for(|forwarded| *forwarded >= count)
            .await
            .discarded("the endpoint retains its forwarded-publish observation sender");
    }

    pub(crate) async fn discarded_reply(&self) {
        let mut discarded = self.discarded.subscribe();
        discarded
            .wait_for(|discarded| *discarded > 0)
            .await
            .discarded("the endpoint retains its discarded-reply observation sender");
    }

    /// Ends every connection the endpoint forwards and refuses new ones until it forwards again.
    pub(crate) fn go_down(&self) {
        self.mode.send_replace(NatsEndpointMode::Down);
    }

    /// Waits until the endpoint has refused `count` connections while it was down.
    pub(crate) async fn refused_at_least(&self, count: u64) {
        let mut refused = self.refused.subscribe();
        refused
            .wait_for(|refused| *refused >= count)
            .await
            .discarded("the endpoint holds the sender for as long as it can be asked");
    }

    /// Waits for a publish that the endpoint has stopped reading behind.
    pub(crate) async fn holding_a_publish(&self) {
        let mut held = self.held.subscribe();
        held.wait_for(|held| *held > 0)
            .await
            .discarded("the endpoint retains its held-publish observation sender");
    }

    /// Ends the active connections, waits for a refused reconnection, and forwards again.
    pub(crate) fn reconnect_after_loss(&self) -> impl Future<Output = Instant> + use<> {
        let mode = self.mode.clone();
        let mut refused = self.refused.subscribe();
        let before = *refused.borrow_and_update();
        async move {
            mode.send_replace(NatsEndpointMode::Down);
            refused
                .wait_for(|refused| *refused > before)
                .await
                .discarded("the endpoint retains its reconnection observation sender");
            let forwarding_at = Instant::now();
            mode.send_replace(NatsEndpointMode::Forwarding);
            forwarding_at
        }
    }

    /// Forwards everything clients send once `delay` has passed, and answers when it started to.
    /// The returned future owns its control, so the action it runs beside can borrow the scenario.
    pub(crate) fn forward_everything_after(
        &self,
        delay: Duration,
    ) -> impl Future<Output = Instant> + use<> {
        let mode = self.mode.clone();
        async move {
            sleep(delay).await;
            let forwarding_at = Instant::now();
            mode.send_replace(NatsEndpointMode::Forwarding);
            forwarding_at
        }
    }

    async fn accept(
        listener: TcpListener,
        server: SocketAddr,
        mode: watch::Receiver<NatsEndpointMode>,
        refused: watch::Sender<u64>,
        held: watch::Sender<u64>,
        forwarded: watch::Sender<u64>,
        discarded: watch::Sender<u64>,
        connections: CancellationToken,
    ) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let Ok((client, _)) = listener.accept().await else {
                return;
            };
            // A server that is down refuses the connection: the client sees it end before the
            // server greets it.
            if *mode.borrow() == NatsEndpointMode::Down {
                drop(client);
                refused.send_modify(|refused| {
                    *refused = refused
                        .checked_add(1)
                        .assured("a scenario connects to one endpoint far fewer than 2^64 times");
                });
                continue;
            }
            nervix_primitives::task::spawn(Self::forward(
                client,
                server,
                mode.clone(),
                held.clone(),
                forwarded.clone(),
                discarded.clone(),
                connections.clone(),
            ));
        }
    }

    /// Carries one connection to `server` until either side ends it, the endpoint goes down or the
    /// endpoint stops. Dropping both sockets then ends the connection for the client and the
    /// server alike.
    async fn forward(
        client: TcpStream,
        server: SocketAddr,
        mode: watch::Receiver<NatsEndpointMode>,
        held: watch::Sender<u64>,
        forwarded: watch::Sender<u64>,
        discarded: watch::Sender<u64>,
        stopped: CancellationToken,
    ) {
        let Ok(server) = TcpStream::connect(server).await else {
            return;
        };
        let (client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) = server.into_split();
        let mut client_read = BufReader::new(client_read);
        let mut down = mode.clone();
        let (drop_replies, dropping) = watch::channel(false);
        nervix_primitives::select! {
            _ = stopped.cancelled() => {}
            _ = down.wait_for(|mode| *mode == NatsEndpointMode::Down) => {}
            _ = Self::forward_client_frames(&mut client_read, &mut server_write, mode.clone(), held, forwarded, drop_replies) => {}
            _ = Self::forward_server_bytes(&mut server_read, &mut client_write, mode, dropping, discarded) => {}
        }
    }

    /// Forward server bytes unless a publish was forwarded under the reply-loss mode.
    async fn forward_server_bytes(
        server: &mut (impl tokio::io::AsyncRead + Unpin),
        client: &mut (impl AsyncWrite + Unpin),
        mode: watch::Receiver<NatsEndpointMode>,
        dropping: watch::Receiver<bool>,
        discarded: watch::Sender<u64>,
    ) -> io::Result<()> {
        let mut bytes = [0_u8; 16 * 1024];
        loop {
            let read = server.read(&mut bytes).await?;
            if read == 0 {
                return client.shutdown().await;
            }
            if *mode.borrow() == NatsEndpointMode::DroppingRepliesAfterNextPublish
                && *dropping.borrow()
            {
                discarded.send_modify(|count| {
                    *count = count
                        .checked_add(1)
                        .assured("a scenario discards far fewer than 2^64 replies");
                });
                continue;
            }
            client.write_all(&bytes[..read]).await?;
        }
    }

    /// Forwards what a client sends one whole frame at a time. While the endpoint holds what the
    /// client sends, the publish it read last waits in the endpoint and nothing more is read from
    /// the client, so what the client sends after it backs up in the client's own buffers.
    async fn forward_client_frames(
        client: &mut (impl AsyncBufRead + Unpin),
        server: &mut (impl AsyncWrite + Unpin),
        mut mode: watch::Receiver<NatsEndpointMode>,
        held: watch::Sender<u64>,
        forwarded: watch::Sender<u64>,
        drop_replies: watch::Sender<bool>,
    ) -> io::Result<()> {
        let mut publishes_since_mode_change = 0_usize;
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(frame) = ClientFrame::read(client).await? else {
                return server.shutdown().await;
            };
            // A frame read before the mode changed is held to the mode it is forwarded under.
            let mut announced_hold = false;
            loop {
                nervix_primitives::task::consume_budget().await;
                if mode.has_changed().map_err(io::Error::other)? {
                    publishes_since_mode_change = 0;
                }
                let current = *mode.borrow_and_update();
                let may_forward = match current {
                    NatsEndpointMode::Forwarding => true,
                    NatsEndpointMode::HoldingAfter { publishes } => {
                        !frame.is_publish || publishes_since_mode_change < publishes
                    }
                    NatsEndpointMode::DroppingRepliesAfterNextPublish => true,
                    // The connection's task ends the connection itself.
                    NatsEndpointMode::Down => false,
                };
                if may_forward {
                    break;
                }
                if frame.is_publish && !announced_hold {
                    held.send_modify(|held| {
                        *held = held
                            .checked_add(1)
                            .assured("a scenario holds far fewer than 2^64 NATS publishes");
                    });
                    announced_hold = true;
                }
                mode.changed().await.map_err(io::Error::other)?;
            }
            if frame.is_data_publish()
                && *mode.borrow() == NatsEndpointMode::DroppingRepliesAfterNextPublish
            {
                drop_replies.send_replace(true);
            }
            server.write_all(&frame.bytes).await?;
            if frame.is_publish {
                if frame.is_data_publish() {
                    forwarded.send_modify(|count| {
                        *count = count
                            .checked_add(1)
                            .assured("a scenario forwards far fewer than 2^64 publishes");
                    });
                }
                publishes_since_mode_change =
                    publishes_since_mode_change.checked_add(1).ok_or_else(|| {
                        io::Error::other("a connection forwarded more publishes than usize counts")
                    })?;
            }
        }
    }
}

impl Drop for NatsEndpoint {
    fn drop(&mut self) {
        self.listener.abort();
        self.connections.cancel();
    }
}

/// One whole frame a NATS client sends: its operation line and, for a publish, the message that
/// line announces.
struct ClientFrame {
    bytes: Vec<u8>,
    is_publish: bool,
    subject: Option<String>,
}

impl ClientFrame {
    fn is_data_publish(&self) -> bool {
        self.subject
            .as_deref()
            .is_some_and(|subject| !subject.starts_with("$JS.API."))
    }

    /// Reads the next frame, or none when the stream ends before another one begins.
    async fn read(reader: &mut (impl AsyncBufRead + Unpin)) -> io::Result<Option<Self>> {
        let mut bytes = Vec::new();
        let line_bytes = (&mut *reader)
            .take(MAX_OPERATION_LINE)
            .read_until(b'\n', &mut bytes)
            .await?;
        if line_bytes == 0 {
            return Ok(None);
        }
        if bytes.last() != Some(&b'\n') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a NATS operation line ended without its line feed",
            ));
        }
        let line = String::from_utf8_lossy(&bytes).into_owned();
        let mut tokens = line.split_whitespace();
        let operation = tokens.next().unwrap_or_default().to_ascii_uppercase();
        // `PUB <subject> [reply-to] <size>` and `HPUB <subject> [reply-to] <header size> <size>`
        // both end with the size of what follows the line, before its own line feed.
        let is_publish = matches!(operation.as_str(), "PUB" | "HPUB");
        let subject = if is_publish {
            tokens.next().map(str::to_owned)
        } else {
            None
        };
        if is_publish {
            let Some(size) = tokens.last() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("a NATS publish line names no message size: {line:?}"),
                ));
            };
            let size = size.parse::<usize>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("a NATS publish line names no valid message size: {line:?}: {error}"),
                )
            })?;
            let message_bytes = size.checked_add(2).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "a NATS message is longer than memory can hold",
                )
            })?;
            let line_end = bytes.len();
            let frame_bytes = line_end.checked_add(message_bytes).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "a NATS frame is longer than memory can hold",
                )
            })?;
            bytes.resize(frame_bytes, 0);
            reader.read_exact(&mut bytes[line_end..]).await?;
        }
        Ok(Some(Self {
            bytes,
            is_publish,
            subject,
        }))
    }
}
