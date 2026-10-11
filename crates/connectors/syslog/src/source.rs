//! Syslog listener source transport.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Syslog UDP, TCP and TLS listeners, RFC 6587 stream framing, peer metadata, and
//!   source lifecycle operations.
//! - **Depends on.** The connector source contract, connector-owned Syslog configuration, Tokio
//!   sockets, `memchr`, and rustls.
//! - **Must not know.** Runtime collectors, relays, branches, schedules, registry state, or NSPL.

use std::{net::SocketAddr, num::NonZeroUsize};

use async_trait::async_trait;
use bytes::{BufMut as _, buf::Limit};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    BrokerSourceConnector, IngestMessageHeaders, IngestMetadataRow, NoIngestHeaders, SourceBatch,
    SourceBatchRequest, SourceConnector, SourceError, SourceMessage, SourceResult, SourceResume,
};
use nervix_models::ClientConfigEntry;
use nervix_primitives::{
    net::{TcpListener, UdpSocket},
    sync::{mpsc, watch},
    task::JoinSet,
};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_rustls::TlsAcceptor;
use tracing::debug;

use crate::config::{SyslogClientConfig, SyslogConfigError, SyslogDirection, SyslogProtocol};

const SYSLOG: &str = "syslog";
const STREAM_INTAKE_QUEUE_CAPACITY: usize = 64;
const MAX_OCTET_COUNT_DIGITS: usize = 10;
/// The most bytes one read appends to a connection's frame buffer, so a buffer grows with the
/// frames a connection actually sends rather than to its frame bound on the first read.
const STREAM_READ_CHUNK: usize = 8_192;

#[derive(Clone)]
pub struct SyslogSourcePlan {
    config: SyslogClientConfig,
    bind_addr: String,
}

impl SyslogSourcePlan {
    pub fn new(
        entries: Vec<ClientConfigEntry>,
        bind_addr: impl FnOnce(&str) -> String,
    ) -> error_stack::Result<Self, SyslogConfigError> {
        let config = SyslogClientConfig::parse(&entries, SyslogDirection::Ingest)?;
        let bind_addr = bind_addr(&config.addr);
        Ok(Self { config, bind_addr })
    }
}

pub struct SyslogSourceMessage {
    payload: Vec<u8>,
    peer_addr: SocketAddr,
    position: (),
    headers: NoIngestHeaders,
}

impl SyslogSourceMessage {
    fn new(payload: Vec<u8>, peer_addr: SocketAddr) -> Self {
        Self {
            payload,
            peer_addr,
            position: (),
            headers: NoIngestHeaders,
        }
    }
}

impl SourceMessage for SyslogSourceMessage {
    type Position = ();

    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn position(&self) -> &Self::Position {
        &self.position
    }

    fn headers(&self) -> &dyn IngestMessageHeaders {
        &self.headers
    }

    fn metadata(&self) -> IngestMetadataRow<'_> {
        IngestMetadataRow::Syslog {
            peer_addr: self.peer_addr,
        }
    }
}

struct ReceivedSyslogFrame {
    payload: Vec<u8>,
    peer_addr: SocketAddr,
}

enum SyslogListener {
    Udp(SyslogUdpListener),
    Stream(SyslogStreamListener),
}

struct SyslogUdpListener {
    socket: UdpSocket,
    datagram: Vec<u8>,
}

struct SyslogStreamListener {
    listener: TcpListener,
    frame_tx: mpsc::Sender<ReceivedSyslogFrame>,
    frame_rx: mpsc::Receiver<ReceivedSyslogFrame>,
    connections: JoinSet<error_stack::Result<(), SyslogConnectionError>>,
    paused: watch::Sender<bool>,
}

pub struct SyslogSource {
    config: SyslogClientConfig,
    bind_addr: String,
    tls_acceptor: Option<TlsAcceptor>,
    listener: Option<SyslogListener>,
}

#[derive(Debug, Error)]
enum SyslogListenerError {
    #[error("Syslog {transport} bind '{addr}' failed")]
    Bind {
        transport: &'static str,
        addr: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Syslog UDP listener receive failed")]
    UdpReceive {
        #[source]
        source: std::io::Error,
    },
    #[error("Syslog stream listener accept failed")]
    StreamAccept {
        #[source]
        source: std::io::Error,
    },
    #[error("Syslog stream intake queue closed")]
    IntakeQueueClosed,
}

#[derive(Debug, Error)]
enum SyslogConnectionError {
    #[error("TLS handshake failed")]
    TlsHandshake {
        #[source]
        source: std::io::Error,
    },
    #[error("stream read failed")]
    StreamRead {
        #[source]
        source: std::io::Error,
    },
    #[error("connection ended with an incomplete Syslog frame")]
    IncompleteFrame,
    #[error("invalid Syslog stream frame")]
    Frame,
}

/// Why a connection's byte stream is not RFC 6587 framing the listener accepts.
#[derive(Debug, Error)]
pub enum SyslogFrameError {
    #[error("malformed Syslog octet-counting length prefix")]
    MalformedOctetCount,
    #[error("malformed Syslog octet count")]
    InvalidOctetCount {
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("Syslog octet count {length} exceeds max_message_size {maximum}")]
    OversizedOctetCount {
        length: usize,
        maximum: NonZeroUsize,
    },
    #[error("Syslog non-transparent frame exceeds max_message_size {maximum}")]
    OversizedNonTransparentFrame { maximum: NonZeroUsize },
    #[error("Syslog stream frame exceeds max_message_size {maximum}")]
    OversizedBufferedFrame { maximum: NonZeroUsize },
    #[error("Syslog TLS requires octet-counting framing")]
    NonOctetTlsFrame,
}

#[async_trait]
impl SourceConnector for SyslogSource {
    type Plan = SyslogSourcePlan;

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        let tls_acceptor = if plan.config.protocol == SyslogProtocol::Tls {
            let config = plan
                .config
                .tls_server_config()
                .change_context(SourceError::Open { connector: SYSLOG })?;
            Some(TlsAcceptor::from(config))
        } else {
            None
        };
        Ok(Self {
            config: plan.config.clone(),
            bind_addr: plan.bind_addr.clone(),
            tls_acceptor,
            listener: None,
        })
    }

    fn needs_resume(&mut self) -> bool {
        self.listener.is_none()
    }

    async fn suspend(&mut self) -> SourceResult<()> {
        if let Some(SyslogListener::Stream(listener)) = self.listener.as_mut() {
            listener.paused.send_replace(true);
        }
        Ok(())
    }

    async fn resume(&mut self) -> SourceResult<SourceResume> {
        if let Some(listener) = self.listener.as_mut() {
            if let SyslogListener::Stream(listener) = listener {
                listener.paused.send_replace(false);
            }
            return Ok(SourceResume::Ready);
        }
        let listener = self
            .bind_listener()
            .await
            .change_context(SourceError::Resume { connector: SYSLOG })?;
        self.listener = Some(listener);
        Ok(SourceResume::Ready)
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.drop_listener();
        Ok(())
    }
}

#[async_trait]
impl BrokerSourceConnector for SyslogSource {
    type Message = SyslogSourceMessage;
    type Position = ();

    async fn next_batch(
        &mut self,
        _request: SourceBatchRequest,
    ) -> SourceResult<SourceBatch<Self::Message>> {
        let result = match self.listener.as_mut() {
            Some(SyslogListener::Udp(listener)) => {
                listener.receive(self.config.max_message_size).await
            }
            Some(SyslogListener::Stream(listener)) => {
                listener
                    .receive(self.config.max_message_size, self.tls_acceptor.clone())
                    .await
            }
            None => return Ok(SourceBatch::ResumeRequired),
        };
        match result {
            Ok(message) => Ok(SourceBatch::Messages(vec![message])),
            Err(error) => {
                self.drop_listener();
                Err(error.change_context(SourceError::Read { connector: SYSLOG }))
            }
        }
    }

    async fn acknowledge(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }

    async fn reject(&mut self, _positions: &[Self::Position]) -> SourceResult<()> {
        Ok(())
    }
}

impl SyslogSource {
    async fn bind_listener(&self) -> error_stack::Result<SyslogListener, SyslogListenerError> {
        match self.config.protocol {
            SyslogProtocol::Udp => {
                let socket = UdpSocket::bind(&self.bind_addr).await.map_err(|source| {
                    Report::new(SyslogListenerError::Bind {
                        transport: "UDP",
                        addr: self.bind_addr.clone(),
                        source,
                    })
                })?;
                Ok(SyslogListener::Udp(SyslogUdpListener {
                    socket,
                    datagram: vec![0_u8; 65_535],
                }))
            }
            SyslogProtocol::Tcp | SyslogProtocol::Tls => {
                let listener = TcpListener::bind(&self.bind_addr).await.map_err(|source| {
                    Report::new(SyslogListenerError::Bind {
                        transport: "stream",
                        addr: self.bind_addr.clone(),
                        source,
                    })
                })?;
                let (frame_tx, frame_rx) = mpsc::channel(STREAM_INTAKE_QUEUE_CAPACITY);
                let (paused, _) = watch::channel(false);
                Ok(SyslogListener::Stream(SyslogStreamListener {
                    listener,
                    frame_tx,
                    frame_rx,
                    connections: JoinSet::new(),
                    paused,
                }))
            }
        }
    }

    fn drop_listener(&mut self) {
        if let Some(SyslogListener::Stream(listener)) = self.listener.as_mut() {
            listener.connections.abort_all();
        }
        self.listener = None;
    }
}

impl SyslogUdpListener {
    async fn receive(
        &mut self,
        max_message_size: NonZeroUsize,
    ) -> error_stack::Result<SyslogSourceMessage, SyslogListenerError> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let (size, peer_addr) = self
                .socket
                .recv_from(&mut self.datagram)
                .await
                .map_err(|source| Report::new(SyslogListenerError::UdpReceive { source }))?;
            if size > max_message_size.get() {
                debug!(
                    peer_addr = %peer_addr,
                    size,
                    max_message_size,
                    "dropped oversized syslog UDP datagram"
                );
                continue;
            }
            return Ok(SyslogSourceMessage::new(
                self.datagram[..size].to_vec(),
                peer_addr,
            ));
        }
    }
}

impl SyslogStreamListener {
    async fn receive(
        &mut self,
        max_message_size: NonZeroUsize,
        tls_acceptor: Option<TlsAcceptor>,
    ) -> error_stack::Result<SyslogSourceMessage, SyslogListenerError> {
        loop {
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
                accepted = self.listener.accept() => {
                    let (stream, peer_addr) = accepted.map_err(|source| {
                        Report::new(SyslogListenerError::StreamAccept { source })
                    })?;
                    if let Err(error) = stream.set_nodelay(true) {
                        debug!(
                            peer_addr = %peer_addr,
                            error = %error,
                            "failed to configure accepted syslog connection"
                        );
                        continue;
                    }
                    let tx = self.frame_tx.clone();
                    let paused = self.paused.subscribe();
                    let connection_tls = tls_acceptor.clone();
                    self.connections.spawn(async move {
                        if let Some(acceptor) = connection_tls {
                            let stream = acceptor
                                .accept(stream)
                                .await
                                .map_err(|source| Report::new(SyslogConnectionError::TlsHandshake { source }))?;
                            read_stream_connection(
                                stream,
                                peer_addr,
                                max_message_size,
                                false,
                                tx,
                                paused,
                            )
                            .await
                        } else {
                            read_stream_connection(
                                stream,
                                peer_addr,
                                max_message_size,
                                true,
                                tx,
                                paused,
                            )
                            .await
                        }
                    });
                }
                frame = self.frame_rx.recv() => {
                    let Some(frame) = frame else {
                        return Err(Report::new(SyslogListenerError::IntakeQueueClosed));
                    };
                    return Ok(SyslogSourceMessage::new(frame.payload, frame.peer_addr));
                }
                joined = self.connections.join_next(), if !self.connections.is_empty() => {
                    match joined {
                        Some(Ok(Err(error))) => {
                            debug!(error = ?error, "closed malformed or failed syslog connection");
                        }
                        Some(Err(error)) if !error.is_cancelled() => {
                            debug!(error = %error, "syslog connection task failed");
                        }
                        Some(Ok(Ok(()))) | Some(Err(_)) | None => {}
                    }
                }
            }
        }
    }
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "the generic external stream reader owns its I/O effects; local callbacks remain \
                  analyzed"
    )
)]
async fn read_stream_connection(
    mut stream: impl AsyncRead + Unpin,
    peer_addr: SocketAddr,
    max_message_size: NonZeroUsize,
    allow_non_transparent: bool,
    tx: mpsc::Sender<ReceivedSyslogFrame>,
    mut paused: watch::Receiver<bool>,
) -> error_stack::Result<(), SyslogConnectionError> {
    let mut decoder = StreamFrameDecoder::new(max_message_size, allow_non_transparent);
    loop {
        nervix_primitives::task::consume_budget().await;
        if *paused.borrow() {
            if paused.changed().await.is_err() {
                return Ok(());
            }
            continue;
        }
        if let Some(frame) = decoder
            .next_frame()
            .change_context(SyslogConnectionError::Frame)?
        {
            nervix_primitives::select! {
                sent = tx.send(ReceivedSyslogFrame { payload: frame, peer_addr }) => {
                    if sent.is_err() {
                        return Ok(());
                    }
                }
                changed = paused.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }
                }
            }
            continue;
        }
        let mut buffer = decoder
            .read_buffer()
            .change_context(SyslogConnectionError::Frame)?;
        let read = nervix_primitives::select! {
            changed = paused.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                continue;
            }
            read = stream.read_buf(&mut buffer) => read,
        }
        .map_err(|source| Report::new(SyslogConnectionError::StreamRead { source }))?;
        if read == 0 {
            return if decoder.is_empty() {
                Ok(())
            } else {
                Err(Report::new(SyslogConnectionError::IncompleteFrame))
            };
        }
    }
}

/// How far the frame at the read cursor has been examined, in bytes past the cursor, so that a
/// frame split across reads is not examined again from its start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrameProgress {
    /// Only the frame's first byte decides its framing. An octet count, at most ten digits and a
    /// space, is read whole again when a read splits it.
    Unread,
    /// An octet-counted frame whose count was read: its payload is the bytes
    /// `payload_start..frame_end`.
    OctetCounted {
        payload_start: usize,
        frame_end: usize,
    },
    /// A non-transparent frame whose first `searched` bytes hold no LF.
    NonTransparent { searched: usize },
}

/// RFC 6587 framing of one connection's byte stream.
///
/// Reads append to one buffer, and framing moves a cursor through it, copying each payload out.
/// The bytes before the cursor are discarded once, when the next read needs room, so the bytes
/// a read delivers are moved at most once however many frames they hold. Delimiters are found
/// with `memchr`, from where the previous search of the same frame stopped.
struct StreamFrameDecoder {
    /// Framed bytes before `cursor`, then the bytes of the frames not yet complete.
    buffer: Vec<u8>,
    /// The first byte of the next frame.
    cursor: usize,
    progress: FrameProgress,
    max_message_size: NonZeroUsize,
    allow_non_transparent: bool,
}

impl StreamFrameDecoder {
    fn new(max_message_size: NonZeroUsize, allow_non_transparent: bool) -> Self {
        Self {
            buffer: Vec::new(),
            cursor: 0,
            progress: FrameProgress::Unread,
            max_message_size,
            allow_non_transparent,
        }
    }

    fn is_empty(&self) -> bool {
        self.cursor == self.buffer.len()
    }

    /// Discards the framed bytes and returns the buffer the next read appends to, limited to what
    /// the frame bound still admits and to one read chunk.
    fn read_buffer(&mut self) -> error_stack::Result<Limit<&mut Vec<u8>>, SyslogFrameError> {
        let cap = self
            .max_message_size
            .get()
            .checked_add(MAX_OCTET_COUNT_DIGITS + 1)
            .ok_or_else(|| {
                Report::new(SyslogFrameError::OversizedBufferedFrame {
                    maximum: self.max_message_size,
                })
            })?;
        self.buffer.drain(..self.cursor);
        self.cursor = 0;
        // The buffer is filled to at most `cap` bytes, so a longer one has no room left.
        let remaining = cap.saturating_sub(self.buffer.len());
        if remaining == 0 {
            return Err(Report::new(SyslogFrameError::OversizedBufferedFrame {
                maximum: self.max_message_size,
            }));
        }
        let limit = remaining.min(STREAM_READ_CHUNK);
        self.buffer.reserve(limit);
        Ok((&mut self.buffer).limit(limit))
    }

    fn next_frame(&mut self) -> error_stack::Result<Option<Vec<u8>>, SyslogFrameError> {
        let Some(first) = self.buffer.get(self.cursor).copied() else {
            return Ok(None);
        };
        match self.progress {
            FrameProgress::Unread if first.is_ascii_digit() => self.read_octet_count(),
            FrameProgress::Unread if !self.allow_non_transparent => {
                Err(Report::new(SyslogFrameError::NonOctetTlsFrame))
            }
            FrameProgress::Unread => self.next_non_transparent_frame(0),
            FrameProgress::OctetCounted {
                payload_start,
                frame_end,
            } => Ok(self.take_octet_counted_frame(payload_start, frame_end)),
            FrameProgress::NonTransparent { searched } => self.next_non_transparent_frame(searched),
        }
    }

    /// Reads the octet count at the cursor. The count and its space are at most eleven bytes, so
    /// the search for the space looks no further.
    fn read_octet_count(&mut self) -> error_stack::Result<Option<Vec<u8>>, SyslogFrameError> {
        let pending = &self.buffer[self.cursor..];
        let window = &pending[..pending.len().min(MAX_OCTET_COUNT_DIGITS + 1)];
        let Some(delimiter) = memchr::memchr(b' ', window) else {
            if pending.len() > MAX_OCTET_COUNT_DIGITS
                || pending.iter().any(|byte| !byte.is_ascii_digit())
            {
                return Err(Report::new(SyslogFrameError::MalformedOctetCount));
            }
            return Ok(None);
        };
        let prefix = &pending[..delimiter];
        if prefix.first() == Some(&b'0') || !prefix.iter().all(|byte| byte.is_ascii_digit()) {
            return Err(Report::new(SyslogFrameError::MalformedOctetCount));
        }
        let prefix = std::str::from_utf8(prefix)
            .verified("the check above rejected every prefix that is not made of ASCII digits");
        let length = prefix
            .parse::<usize>()
            .map_err(|source| Report::new(SyslogFrameError::InvalidOctetCount { source }))?;
        if length > self.max_message_size.get() {
            return Err(Report::new(SyslogFrameError::OversizedOctetCount {
                length,
                maximum: self.max_message_size,
            }));
        }
        let payload_start = delimiter
            .checked_add(1)
            .verified("the delimiter position is an index into the buffered bytes");
        let frame_end = payload_start
            .checked_add(length)
            .verified("the octet count checked above is at most the maximum message size");
        self.progress = FrameProgress::OctetCounted {
            payload_start,
            frame_end,
        };
        Ok(self.take_octet_counted_frame(payload_start, frame_end))
    }

    fn take_octet_counted_frame(
        &mut self,
        payload_start: usize,
        frame_end: usize,
    ) -> Option<Vec<u8>> {
        let pending = &self.buffer[self.cursor..];
        let payload = pending.get(payload_start..frame_end)?;
        let frame = payload.to_vec();
        self.consume(frame_end);
        Some(frame)
    }

    /// Searches the frame at the cursor for its LF from byte `searched`, which earlier searches
    /// already covered.
    fn next_non_transparent_frame(
        &mut self,
        searched: usize,
    ) -> error_stack::Result<Option<Vec<u8>>, SyslogFrameError> {
        let pending = &self.buffer[self.cursor..];
        let Some(offset) = memchr::memchr(b'\n', &pending[searched..]) else {
            let pending_payload_size = pending
                .len()
                .checked_sub(usize::from(pending.last() == Some(&b'\r')))
                .verified("a trailing carriage return means the buffer holds at least one byte");
            if pending_payload_size > self.max_message_size.get() {
                return Err(Report::new(
                    SyslogFrameError::OversizedNonTransparentFrame {
                        maximum: self.max_message_size,
                    },
                ));
            }
            self.progress = FrameProgress::NonTransparent {
                searched: pending.len(),
            };
            return Ok(None);
        };
        let delimiter = searched
            .checked_add(offset)
            .verified("the offset indexes the bytes after the searched ones");
        let payload_end = if delimiter > 0 && pending[delimiter - 1] == b'\r' {
            delimiter - 1
        } else {
            delimiter
        };
        if payload_end > self.max_message_size.get() {
            return Err(Report::new(
                SyslogFrameError::OversizedNonTransparentFrame {
                    maximum: self.max_message_size,
                },
            ));
        }
        let frame = pending[..payload_end].to_vec();
        self.consume(delimiter + 1);
        Ok(Some(frame))
    }

    /// Moves the cursor past a complete frame of `length` bytes.
    fn consume(&mut self, length: usize) {
        self.cursor = self
            .cursor
            .checked_add(length)
            .verified("a complete frame lies inside the buffered bytes");
        self.progress = FrameProgress::Unread;
    }
}

/// Frames one connection's whole byte stream through the production decoder, reading it into the
/// decoder's buffer at most `read_size` bytes at a time as a connection does, and answers how many
/// payload bytes its frames held. Criterion measures the framer through it.
#[cfg(feature = "benchmarks")]
pub fn frame_stream(
    stream: &[u8],
    read_size: usize,
    max_message_size: NonZeroUsize,
) -> error_stack::Result<usize, SyslogFrameError> {
    const IN_MEMORY: &str = "every count is a length of the stream this call holds in memory";

    let mut decoder = StreamFrameDecoder::new(max_message_size, true);
    let mut position = 0_usize;
    let mut payload_bytes = 0_usize;
    loop {
        while let Some(frame) = decoder.next_frame()? {
            payload_bytes = payload_bytes.checked_add(frame.len()).assured(IN_MEMORY);
        }
        if position == stream.len() {
            return Ok(payload_bytes);
        }
        let mut buffer = decoder.read_buffer()?;
        let read = read_size.min(buffer.remaining_mut());
        let end = stream
            .len()
            .min(position.checked_add(read).assured(IN_MEMORY));
        buffer.put_slice(&stream[position..end]);
        position = end;
    }
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
