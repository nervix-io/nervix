//! The lifetime and I/O driver for one interconnect connection.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Connection setup deadlines, reconnect cadence, full-frame I/O, and connection task
//!   retirement.
//! - **Depends on.** The interconnect transport's authenticated wire envelopes and peer identity.
//! - **Must not know.** Runtime graph semantics or why an envelope is exchanged.

use std::{io, net::SocketAddr, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::ClusterNodeName;
use rustls::pki_types::ServerName;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{OwnedSemaphorePermit, mpsc},
    time::{MissedTickBehavior, interval, sleep, timeout},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use triomphe::Arc;

use super::{
    ConnectionHandle, ConnectionKey, Envelope, PING_INTERVAL, PING_TIMEOUT, ReceivedEnvelope,
    TransportError, TransportInner, TransportMode, configure_socket,
    wire::{
        QueuedFrame, WireEnvelope, WireFrame, decode_frame, encode_frame,
        read_and_verify_introduction, read_frame_bytes, write_wire_envelope,
    },
};

pub(super) fn spawn_outbound_connection(
    inner: Arc<TransportInner>,
    key: ConnectionKey,
    handle: ConnectionHandle,
    cancel: CancellationToken,
    rx: mpsc::Receiver<QueuedFrame>,
    permit: OwnedSemaphorePermit,
) {
    let tasks = inner.tasks.clone();
    tasks.spawn(async move {
        run_outbound_connection(inner.clone(), key.clone(), handle, cancel.clone(), rx).await;
        retire_outbound_connection(&inner, &key, &cancel);
        drop(permit);
    });
}

async fn run_outbound_connection(
    inner: Arc<TransportInner>,
    key: ConnectionKey,
    handle: ConnectionHandle,
    cancel: CancellationToken,
    mut rx: mpsc::Receiver<QueuedFrame>,
) {
    let mut pending = None;
    let mut backoff = ReconnectBackoff::new(
        inner.options.reconnect_backoff,
        inner.options.max_reconnect_backoff,
    );

    loop {
        tokio::task::consume_budget().await;
        if cancel.is_cancelled()
            || inner.draining.is_cancelled()
            || inner.force_close.is_cancelled()
        {
            return;
        }

        let established = match establish_outbound_connection(&inner, &key, &cancel).await {
            Ok(established) => established,
            Err(connect_err) => {
                if cancel.is_cancelled()
                    || inner.draining.is_cancelled()
                    || inner.force_close.is_cancelled()
                {
                    return;
                }
                debug!(?connect_err, target = %key.addr, "outbound interconnect reconnect failed");
                if !wait_for_reconnect(&inner, &cancel, backoff.next_delay()).await {
                    return;
                }
                continue;
            }
        };

        if !outbound_generation_is_current(&inner, &key, &cancel) {
            return;
        }
        backoff.reset();
        register_connected_peer(&inner, &established.peer_node_id, &handle);
        let peer_node_id = established.peer_node_id.clone();
        let result = drive_connection(
            inner.clone(),
            key.addr,
            handle.clone(),
            established,
            &cancel,
            &mut rx,
            &mut pending,
        )
        .await;
        unregister_connected_peer(&inner, &peer_node_id, &handle);

        if let Err(err) = result {
            debug!(?err, target = %key.addr, "outbound interconnect connection closed");
            if cancel.is_cancelled()
                || inner.draining.is_cancelled()
                || inner.force_close.is_cancelled()
            {
                return;
            }
            if !wait_for_reconnect(&inner, &cancel, backoff.next_delay()).await {
                return;
            }
        } else {
            return;
        }
    }
}

pub(super) type BoxedIo = Box<dyn AsyncReadWrite>;

pub(super) trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

type BoxedReader = tokio::io::ReadHalf<BoxedIo>;
type BoxedWriter = tokio::io::WriteHalf<BoxedIo>;

pub(super) struct EstablishedConnection {
    pub(super) peer_node_id: ClusterNodeName,
    reader: BoxedReader,
    writer: BoxedWriter,
}

pub(super) struct ReconnectBackoff {
    initial: Duration,
    next: Duration,
    max: Duration,
}

impl ReconnectBackoff {
    pub(super) fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            next: initial,
            max,
        }
    }

    pub(super) fn reset(&mut self) {
        self.next = self.initial;
    }

    pub(super) fn next_delay(&mut self) -> Duration {
        let nominal = self.next;
        self.next = match self.next.checked_mul(2) {
            Some(doubled) => doubled.min(self.max),
            None => self.max,
        };
        let nominal_millis = u64::try_from(nominal.as_millis())
            .assured("transport option validation guarantees reconnect delays fit in milliseconds");
        let jitter_floor = nominal_millis / 2;
        Duration::from_millis(fastrand::u64(jitter_floor..=nominal_millis))
    }
}

pub(super) async fn run_inbound_connection(
    inner: Arc<TransportInner>,
    stream: TcpStream,
    peer_addr: SocketAddr,
) -> Result<(), Report<TransportError>> {
    let cancel = CancellationToken::new();
    let (tx, mut rx) = mpsc::channel(inner.options.send_queue_capacity);
    let handle = ConnectionHandle::new(
        peer_addr,
        tx,
        inner.executor.clone(),
        cancel.clone(),
        inner.admission_closed.clone(),
        inner.options.queue_admission_timeout,
    );
    let established = establish_inbound_connection(&inner, stream, peer_addr, &cancel).await?;
    let peer_node_id = established.peer_node_id.clone();
    register_connected_peer(&inner, &peer_node_id, &handle);
    let mut pending = None;
    let result = drive_connection(
        inner.clone(),
        peer_addr,
        handle.clone(),
        established,
        &cancel,
        &mut rx,
        &mut pending,
    )
    .await;
    unregister_connected_peer(&inner, &peer_node_id, &handle);
    result
}

async fn establish_inbound_connection(
    inner: &Arc<TransportInner>,
    stream: TcpStream,
    peer_addr: SocketAddr,
    cancel: &CancellationToken,
) -> Result<EstablishedConnection, Report<TransportError>> {
    let setup_timeout = inner.options.connection_setup_timeout;
    tokio::select! {
        biased;
        _ = inner.force_close.cancelled() => Err(Report::new(TransportError::ShuttingDown)),
        _ = inner.draining.cancelled() => Err(Report::new(TransportError::ShuttingDown)),
        _ = cancel.cancelled() => Err(Report::new(TransportError::Closed(peer_addr))),
        result = timeout(setup_timeout, async {
            let io_stream = accept_inbound_stream(inner, stream, peer_addr).await?;
            exchange_introductions(inner, io_stream).await
        }) => {
            result.map_err(|_| Report::new(TransportError::ConnectionSetupTimeout {
                peer: peer_addr,
                timeout: setup_timeout,
            }))?
        }
    }
}

pub(super) async fn establish_outbound_connection(
    inner: &Arc<TransportInner>,
    key: &ConnectionKey,
    cancel: &CancellationToken,
) -> Result<EstablishedConnection, Report<TransportError>> {
    let setup_timeout = inner.options.connection_setup_timeout;
    let established = tokio::select! {
        biased;
        _ = inner.force_close.cancelled() => Err(Report::new(TransportError::ShuttingDown)),
        _ = inner.draining.cancelled() => Err(Report::new(TransportError::ShuttingDown)),
        _ = cancel.cancelled() => Err(Report::new(TransportError::Closed(key.addr))),
        result = timeout(setup_timeout, async {
            let io_stream = connect_outbound_stream(inner, key).await?;
            exchange_introductions(inner, io_stream).await
        }) => {
            result.map_err(|_| Report::new(TransportError::ConnectionSetupTimeout {
                peer: key.addr,
                timeout: setup_timeout,
            }))?
        }
    }?;
    if established.peer_node_id != key.peer_node_id {
        return Err(Report::new(TransportError::InvalidHandshake(format!(
            "expected node '{}' but authenticated '{}'",
            key.peer_node_id, established.peer_node_id
        ))));
    }
    Ok(established)
}

pub(super) async fn exchange_introductions(
    inner: &TransportInner,
    io_stream: BoxedIo,
) -> Result<EstablishedConnection, Report<TransportError>> {
    let (mut reader, mut writer) = tokio::io::split(io_stream);
    let introduction = encode_frame(
        &inner.executor,
        WireEnvelope::Introduction(inner.identity.signed_introduction()),
    )
    .await?;
    write_wire_envelope(&mut writer, &introduction).await?;
    let peer_node_id = read_and_verify_introduction(
        &mut reader,
        &inner.executor,
        inner.options.max_frame_bytes,
        &inner.peer_verifier,
    )
    .await?;
    Ok(EstablishedConnection {
        peer_node_id,
        reader,
        writer,
    })
}

async fn accept_inbound_stream(
    inner: &Arc<TransportInner>,
    stream: TcpStream,
    peer_addr: SocketAddr,
) -> Result<BoxedIo, Report<TransportError>> {
    match inner.mode {
        TransportMode::Plain => Ok(Box::new(stream)),
        TransportMode::Tls => {
            let acceptor = TlsAcceptor::from(
                inner
                    .server_config
                    .clone()
                    .ok_or(TransportError::MissingTlsConfig)?,
            );
            acceptor
                .accept(stream)
                .await
                .map(|stream| -> BoxedIo { Box::new(stream) })
                .map_err(|err| {
                    warn!(?err, %peer_addr, "failed to accept interconnect tls connection");
                    Report::new(TransportError::Io(io::Error::other(err.to_string())))
                })
        }
    }
}

pub(super) async fn connect_outbound_stream(
    inner: &Arc<TransportInner>,
    key: &ConnectionKey,
) -> Result<BoxedIo, Report<TransportError>> {
    let tcp = TcpStream::connect(key.addr).await.map_err(|err| {
        debug!(?err, target = %key.addr, "outbound interconnect connect failed");
        Report::new(TransportError::Io(err))
    })?;
    configure_socket(&tcp)
        .map_err(TransportError::Io)
        .map_err(Report::new)?;

    match key.mode {
        TransportMode::Plain => Ok(Box::new(tcp)),
        TransportMode::Tls => {
            let server_name = ServerName::try_from(key.server_name.clone())
                .map_err(|_| TransportError::InvalidServerName(key.server_name.clone()))?;
            let connector = TlsConnector::from(
                inner
                    .client_config
                    .clone()
                    .ok_or(TransportError::MissingTlsConfig)?,
            );
            connector
                .connect(server_name, tcp)
                .await
                .map(|stream| -> BoxedIo { Box::new(stream) })
                .map_err(|err| {
                    debug!(?err, target = %key.addr, "outbound interconnect tls connect failed");
                    Report::new(TransportError::Io(io::Error::other(err.to_string())))
                })
        }
    }
}

pub(super) async fn drive_connection(
    inner: Arc<TransportInner>,
    peer_addr: SocketAddr,
    reply_handle: ConnectionHandle,
    established: EstablishedConnection,
    cancel: &CancellationToken,
    rx: &mut mpsc::Receiver<QueuedFrame>,
    retry_payload: &mut Option<QueuedFrame>,
) -> Result<(), Report<TransportError>> {
    let mut pending = retry_payload.take();
    // The keepalive is the same fixed frame every time, so it is serialized once for the whole
    // connection instead of on every tick.
    let keepalive = encode_frame(&inner.executor, WireEnvelope::Ping).await?;
    let result = {
        let read = read_connection(
            inner.clone(),
            peer_addr,
            established.peer_node_id,
            reply_handle,
            established.reader,
        );
        let write = write_connection(
            established.writer,
            rx,
            &mut pending,
            &inner.draining,
            keepalive,
        );
        tokio::pin!(read);
        tokio::pin!(write);
        tokio::select! {
            biased;
            _ = inner.force_close.cancelled() => Ok(()),
            _ = cancel.cancelled() => Ok(()),
            result = &mut read => result,
            result = &mut write => result,
        }
    };
    // A frame that never reached the socket is retried as it stands. It is already encoded, so a
    // retry costs no second serialization and no second copy of its body.
    *retry_payload = pending;
    result
}

async fn read_connection(
    inner: Arc<TransportInner>,
    peer_addr: SocketAddr,
    peer_node_id: ClusterNodeName,
    reply_handle: ConnectionHandle,
    mut reader: BoxedReader,
) -> Result<(), Report<TransportError>> {
    loop {
        tokio::task::consume_budget().await;
        // The liveness deadline covers waiting for bytes on the socket, not the admitted work that
        // turns them into an envelope. Decoding behind a busy class must not be read as a dead peer.
        let frame = timeout(
            PING_TIMEOUT,
            read_frame_bytes(&mut reader, &inner.executor, inner.options.max_frame_bytes),
        )
        .await
        .map_err(|_| Report::new(TransportError::Closed(peer_addr)))??;
        let envelope = decode_frame(&inner.executor, frame).await?;
        match envelope {
            WireEnvelope::Introduction(_) => {
                return Err(Report::new(TransportError::InvalidHandshake(
                    "received duplicate introduction".to_string(),
                )));
            }
            WireEnvelope::Ping => {}
            WireEnvelope::Payload(envelope) => {
                let envelope = match envelope {
                    Envelope::Control(control) => {
                        let Some(control) = inner.requests.route_control(
                            &inner,
                            &peer_node_id,
                            &reply_handle,
                            control,
                        ) else {
                            continue;
                        };
                        Envelope::Control(control)
                    }
                    envelope => envelope,
                };
                inner
                    .incoming_tx
                    .send(ReceivedEnvelope {
                        peer_addr,
                        peer_node_id: peer_node_id.clone(),
                        envelope,
                        reply: reply_handle.clone(),
                    })
                    .await
                    .map_err(|_| TransportError::ShuttingDown)?;
            }
        }
    }
}

async fn write_connection(
    mut writer: BoxedWriter,
    rx: &mut mpsc::Receiver<QueuedFrame>,
    pending: &mut Option<QueuedFrame>,
    draining: &CancellationToken,
    keepalive_frame: WireFrame,
) -> Result<(), Report<TransportError>> {
    let mut keepalive = interval(PING_INTERVAL);
    keepalive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut drain_queue = draining.is_cancelled();
    loop {
        tokio::task::consume_budget().await;
        if pending.is_none() {
            if drain_queue {
                match rx.try_recv() {
                    Ok(frame) => *pending = Some(frame),
                    Err(
                        mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected,
                    ) => {
                        return Ok(());
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    _ = draining.cancelled() => {
                        drain_queue = true;
                        continue;
                    }
                    maybe_frame = rx.recv() => {
                        let Some(frame) = maybe_frame else {
                            return Ok(());
                        };
                        *pending = Some(frame);
                    }
                    _ = keepalive.tick() => *pending = Some(QueuedFrame::keepalive(&keepalive_frame)),
                }
            }
        }

        let frame = pending
            .as_ref()
            .assured("the writer fills its pending frame before attempting socket I/O");
        write_wire_envelope(&mut writer, frame.frame()).await?;
        *pending = None;
    }
}

async fn wait_for_reconnect(
    inner: &TransportInner,
    cancel: &CancellationToken,
    delay: Duration,
) -> bool {
    tokio::select! {
        biased;
        _ = inner.force_close.cancelled() => false,
        _ = inner.draining.cancelled() => false,
        _ = cancel.cancelled() => false,
        _ = sleep(delay) => true,
    }
}

fn outbound_generation_is_current(
    inner: &TransportInner,
    key: &ConnectionKey,
    cancel: &CancellationToken,
) -> bool {
    let Some(entry) = inner.outbound.get(key) else {
        return false;
    };
    entry.cancel == *cancel && !cancel.is_cancelled()
}

pub(super) fn retire_outbound_connection(
    inner: &TransportInner,
    key: &ConnectionKey,
    cancel: &CancellationToken,
) {
    cancel.cancel();
    inner
        .outbound
        .remove_if(key, |_, connection| connection.cancel == *cancel);
}

pub(super) fn register_connected_peer(
    inner: &TransportInner,
    peer_node_id: &ClusterNodeName,
    connection: &ConnectionHandle,
) {
    inner
        .connected_peers
        .entry(peer_node_id.clone())
        .or_default()
        .insert(connection.cancellation().clone(), connection.clone());
    inner.requests.connection_changed();
}

pub(super) fn unregister_connected_peer(
    inner: &TransportInner,
    peer_node_id: &ClusterNodeName,
    connection: &ConnectionHandle,
) {
    let Some(mut connections) = inner.connected_peers.get_mut(peer_node_id) else {
        return;
    };
    connections.remove(connection.cancellation());
    drop(connections);
    inner
        .connected_peers
        .remove_if(peer_node_id, |_, connections| connections.is_empty());
    inner.requests.connection_changed();
}
