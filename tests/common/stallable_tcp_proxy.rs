//! A loopback TCP proxy a scenario stands in front of one dependency and pauses, or stands alone as
//! an endpoint that never answers.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One loopback listener, when it accepted each connection, the connections it forwards
//!   to its dependency or holds unanswered, the gate that pauses both directions of every forwarded
//!   one, resetting them, and rewriting the broker address a Pulsar topic lookup answers with.
//! - **Depends on.** Tokio's TCP sockets and codecs, the primitives boundary's watch channel and
//!   cancellation, and the Pulsar frame codec.
//! - **Must not know.** DNS, Nervix, or the protocol a forwarded connection carries beyond Pulsar's
//!   lookup answer.

use std::{
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use futures_util::SinkExt;
use nervix_primitives::{
    net::{TcpListener as TokioTcpListener, TcpStream},
    stream::StreamExt,
    sync::{CancellationToken, watch},
    task::JoinHandle,
    time::{Instant, sleep},
};
use nervix_recovery::Discarded as _;
use pulsar::message::Codec as PulsarCodec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::{FramedRead, FramedWrite};

const HOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// A loopback TCP proxy in front of one dependency that a scenario can pause, and whose forwarded
/// connections it can reset, without touching the dependency other scenarios share. A silent one has
/// no dependency: it accepts every connection and never answers on it.
#[derive(Debug)]
pub(crate) struct StallableTcpProxy {
    local_addr: std::net::SocketAddr,
    /// When the proxy accepted each connection, in the order it accepted them. Each follows the
    /// pause and reset controls from the moment it is recorded.
    accepted: watch::Sender<Vec<Instant>>,
    paused: watch::Sender<bool>,
    /// Signalled to reset every connection the proxy forwards at that moment.
    resets: watch::Sender<()>,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

/// What ended one connection the proxy forwarded.
enum ForwardingEnd {
    /// Both directions ended: a peer closed or failed its side, or the proxy stopped.
    Finished,
    /// The scenario asked the proxy to reset the connections it forwards.
    ResetRequested,
}

#[derive(Clone, Copy)]
enum ForwardingProtocol {
    Tcp,
    Pulsar,
}

/// What the proxy does with a connection it accepted.
#[derive(Clone)]
enum ProxyTarget {
    /// Forward it to the dependency at `host` and `port`, speaking `protocol`.
    Dependency {
        host: String,
        port: u16,
        protocol: ForwardingProtocol,
    },
    /// Hold it unanswered until the proxy resets its connections or stops.
    Silent,
}

impl StallableTcpProxy {
    pub(crate) async fn start(target_host: String, target_port: u16) -> io::Result<Self> {
        Self::listen(ProxyTarget::Dependency {
            host: target_host,
            port: target_port,
            protocol: ForwardingProtocol::Tcp,
        })
        .await
    }

    /// Topic discovery must keep producer and consumer connections behind the same fault gate.
    pub(crate) async fn start_pulsar(target_host: String, target_port: u16) -> io::Result<Self> {
        Self::listen(ProxyTarget::Dependency {
            host: target_host,
            port: target_port,
            protocol: ForwardingProtocol::Pulsar,
        })
        .await
    }

    /// A loopback endpoint that accepts every connection and never answers on it, as a host whose
    /// service stopped reading: a client completes its TCP handshake and then waits.
    pub(crate) async fn silent() -> io::Result<Self> {
        Self::listen(ProxyTarget::Silent).await
    }

    async fn listen(target: ProxyTarget) -> io::Result<Self> {
        let listener = TokioTcpListener::bind((HOST, 0)).await?;
        let local_addr = listener.local_addr()?;
        let (accepted, _) = watch::channel(Vec::new());
        let (paused, paused_rx) = watch::channel(false);
        let (resets, resets_rx) = watch::channel(());
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task_accepted = accepted.clone();
        let task = nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                let incoming = nervix_primitives::select! {
                    _ = task_cancellation.cancelled() => return,
                    incoming = listener.accept() => incoming,
                };
                let Ok((downstream, _)) = incoming else {
                    return;
                };
                let paused = paused_rx.clone();
                // Only a reset requested after this connection was accepted resets it.
                let mut resets = resets_rx.clone();
                resets.mark_unchanged();
                let connection_cancellation = task_cancellation.child_token();
                task_accepted.send_modify(|accepted| accepted.push(Instant::now()));
                match target.clone() {
                    ProxyTarget::Dependency {
                        host,
                        port,
                        protocol,
                    } => {
                        nervix_primitives::task::spawn(async move {
                            let Ok(mut upstream) = TcpStream::connect((host.as_str(), port)).await
                            else {
                                return;
                            };
                            let mut downstream = downstream;
                            let end = forward_through_stall_gate(
                                &mut downstream,
                                &mut upstream,
                                paused,
                                resets,
                                connection_cancellation,
                                protocol,
                                local_addr,
                            )
                            .await;
                            if let ForwardingEnd::ResetRequested = end {
                                // An abortive close: both peers see the connection reset at once
                                // instead of an orderly end of stream, and anything still buffered
                                // is discarded.
                                downstream.set_zero_linger().discarded(
                                    "a socket that cannot be reset still closes when dropped",
                                );
                                upstream.set_zero_linger().discarded(
                                    "a socket that cannot be reset still closes when dropped",
                                );
                            }
                        });
                    }
                    ProxyTarget::Silent => {
                        nervix_primitives::task::spawn(hold_unanswered(
                            downstream,
                            resets,
                            connection_cancellation,
                        ));
                    }
                }
            }
        });
        Ok(Self {
            local_addr,
            accepted,
            paused,
            resets,
            cancellation,
            task,
        })
    }

    /// The loopback address clients connect to.
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Wait until the proxy has accepted `count` connections.
    pub(crate) async fn accepted_at_least(&self, count: usize) {
        let mut accepted = self.accepted.subscribe();
        accepted
            .wait_for(|accepted| accepted.len() >= count)
            .await
            .discarded("the proxy holds the sender for as long as it can be asked");
    }

    /// When the proxy accepted the connection at `index` in the order it accepted them, counting
    /// from zero, or nothing while it has accepted fewer.
    pub(crate) fn accepted_at(&self, index: usize) -> Option<Instant> {
        self.accepted.borrow().get(index).copied()
    }

    pub(crate) fn local_port(&self) -> u16 {
        self.local_addr.port()
    }

    pub(crate) fn set_paused(&self, paused: bool) {
        self.paused.send_replace(paused);
    }

    /// End a scenario's timed dependency fault while another action runs. The returned future
    /// owns its controls, so the action can borrow the scenario independently.
    pub(crate) fn resume_after(
        &self,
        duration: Duration,
    ) -> impl Future<Output = io::Result<Instant>> + use<> {
        let paused = self.paused.clone();
        let cancellation = self.cancellation.clone();
        async move {
            nervix_primitives::select! {
                _ = cancellation.cancelled() => Err(io::Error::other("TCP proxy stopped before the timed fault ended")),
                _ = sleep(duration) => {
                    let resumed_at = Instant::now();
                    paused.send_replace(false);
                    Ok(resumed_at)
                }
            }
        }
    }

    /// Reset every connection the proxy forwards now, as a dependency that drops its clients
    /// does: each peer sees its connection reset. The proxy keeps listening, so a client that
    /// connects again is forwarded as before.
    pub(crate) fn reset_connections(&self) {
        self.resets.send_replace(());
    }
}

/// Forward one accepted connection in both directions through the stall gate until it ends or a
/// reset is requested.
async fn forward_through_stall_gate(
    downstream: &mut TcpStream,
    upstream: &mut TcpStream,
    paused: watch::Receiver<bool>,
    mut resets: watch::Receiver<()>,
    cancellation: CancellationToken,
    protocol: ForwardingProtocol,
    local_addr: SocketAddr,
) -> ForwardingEnd {
    let (downstream_read, downstream_write) = downstream.split();
    let (upstream_read, upstream_write) = upstream.split();
    let upstream_copy = copy_through_stall_gate(
        downstream_read,
        upstream_write,
        paused.clone(),
        cancellation.clone(),
    );
    let downstream_copy = async {
        match protocol {
            ForwardingProtocol::Tcp => {
                copy_through_stall_gate(upstream_read, downstream_write, paused, cancellation).await
            }
            ForwardingProtocol::Pulsar => {
                forward_pulsar_responses(
                    upstream_read,
                    downstream_write,
                    paused,
                    cancellation,
                    local_addr,
                )
                .await
            }
        }
    };
    let both_directions = async { tokio::join!(upstream_copy, downstream_copy) };
    nervix_primitives::select! {
        (upstream_copied, downstream_copied) = both_directions => {
            upstream_copied.discarded("a forwarded connection ends with whichever side failed");
            downstream_copied.discarded("a forwarded connection ends with whichever side failed");
            ForwardingEnd::Finished
        }
        requested = resets.changed() => match requested {
            Ok(()) => ForwardingEnd::ResetRequested,
            // The proxy itself stopped, and its cancellation ends both directions anyway.
            Err(_) => ForwardingEnd::Finished,
        },
    }
}

/// Hold one connection a silent proxy accepted, reading nothing and writing nothing, until a reset
/// is requested or the proxy stops.
async fn hold_unanswered(
    downstream: TcpStream,
    mut resets: watch::Receiver<()>,
    cancellation: CancellationToken,
) {
    nervix_primitives::select! {
        _ = cancellation.cancelled() => {}
        requested = resets.changed() => {
            if requested.is_ok() {
                // The same abortive close a forwarded connection gets.
                downstream
                    .set_zero_linger()
                    .discarded("a socket that cannot be reset still closes when dropped");
            }
        }
    }
}

impl Drop for StallableTcpProxy {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

async fn copy_through_stall_gate<Reader, Writer>(
    mut reader: Reader,
    mut writer: Writer,
    mut paused: watch::Receiver<bool>,
    cancellation: CancellationToken,
) -> io::Result<()>
where
    Reader: AsyncRead + Unpin,
    Writer: AsyncWrite + Unpin,
{
    let mut buffer = vec![0_u8; 16 * 1024];
    loop {
        nervix_primitives::task::consume_budget().await;
        wait_for_proxy_resume(&mut paused, &cancellation).await?;
        let read = nervix_primitives::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(()),
            changed = paused.changed() => {
                changed.map_err(io::Error::other)?;
                continue;
            }
            read = reader.read(&mut buffer) => read?,
        };
        if read == 0 {
            writer.shutdown().await?;
            return Ok(());
        }
        wait_for_proxy_resume(&mut paused, &cancellation).await?;
        writer.write_all(&buffer[..read]).await?;
    }
}

/// Preserve Pulsar frames while advertising this forwarder in topic lookup responses. An opaque
/// bootstrap proxy would let the client's discovered connection bypass the scenario's fault.
async fn forward_pulsar_responses<Reader, Writer>(
    reader: Reader,
    writer: Writer,
    mut paused: watch::Receiver<bool>,
    cancellation: CancellationToken,
    local_addr: SocketAddr,
) -> io::Result<()>
where
    Reader: AsyncRead + Unpin,
    Writer: AsyncWrite + Unpin,
{
    let mut reader = FramedRead::new(reader, PulsarCodec);
    let mut writer = FramedWrite::new(writer, PulsarCodec);
    loop {
        nervix_primitives::task::consume_budget().await;
        wait_for_proxy_resume(&mut paused, &cancellation).await?;
        let message = nervix_primitives::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(()),
            changed = paused.changed() => {
                changed.map_err(io::Error::other)?;
                continue;
            }
            message = reader.next() => message,
        };
        let Some(message) = message else {
            return writer.close().await.map_err(io::Error::other);
        };
        let mut message = message.map_err(io::Error::other)?;
        if let Some(lookup) = &mut message.command.lookup_topic_response {
            lookup.broker_service_url = Some(format!("pulsar://{local_addr}"));
        }
        wait_for_proxy_resume(&mut paused, &cancellation).await?;
        writer.send(message).await.map_err(io::Error::other)?;
    }
}

async fn wait_for_proxy_resume(
    paused: &mut watch::Receiver<bool>,
    cancellation: &CancellationToken,
) -> io::Result<()> {
    while *paused.borrow_and_update() {
        nervix_primitives::task::consume_budget().await;
        nervix_primitives::select! {
            _ = cancellation.cancelled() => return Err(io::Error::other("TCP proxy stopped")),
            changed = paused.changed() => changed.map_err(io::Error::other)?,
        }
    }
    Ok(())
}
