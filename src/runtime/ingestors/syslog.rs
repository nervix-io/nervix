//! Syslog source transport and runtime composition.
//!
//! Layer: data plane, pending the mechanical connector move.
//!
//! - **Owns.** Composing the Syslog connector plan with host-owned intake, plus the listener
//!   transport that moves to its connector crate in the following commit.
//! - **Depends on.** The connector source contract, typed Syslog configuration, socket transports,
//!   and pre-resolved runtime execution handles.
//! - **Must not know.** NSPL parsing, registry validation, or placement computation.

use std::{net::SocketAddr, num::NonZeroUsize};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use nervix_connector::{
    IngestMessageHeaders, IngestMetadataRow, NoIngestHeaders, ParsedRetryPolicy, SourceAckPolicy,
    SourceBatch, SourceBatchRequest, SourceCapabilities, SourceConnector, SourceError,
    SourceMessage, SourcePlan, SourceResult, SourceResume,
};
use nervix_models::ClientConfigEntry;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::{TcpListener, UdpSocket},
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;

use super::{
    super::*,
    source::{BrokerSourceHost, BrokerSourceHostSpec, run_source_instance_with_retry},
};
use crate::runtime::syslog::{SyslogClientConfig, SyslogDirection, SyslogProtocol};

const SYSLOG: &str = "syslog";
const STREAM_INTAKE_QUEUE_CAPACITY: usize = 64;
const MAX_OCTET_COUNT_DIGITS: usize = 10;
const SYSLOG_RETRY_POLICY: ParsedRetryPolicy = ParsedRetryPolicy {
    backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(30),
};

pub(in crate::runtime) struct SyslogIngestor;

#[derive(Clone)]
struct SyslogSourcePlan {
    config: SyslogClientConfig,
    bind_addr: String,
}

impl SyslogSourcePlan {
    fn new(
        entries: Vec<ClientConfigEntry>,
        bind_addr: impl FnOnce(&str) -> String,
    ) -> Result<Self, crate::runtime::syslog::SyslogConfigError> {
        let config = SyslogClientConfig::parse(&entries, SyslogDirection::Ingest)?;
        let bind_addr = bind_addr(&config.addr);
        Ok(Self { config, bind_addr })
    }
}

struct SyslogSourceMessage {
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
    connections: JoinSet<Result<(), SyslogConnectionError>>,
    paused: watch::Sender<bool>,
}

struct SyslogSource {
    config: SyslogClientConfig,
    bind_addr: String,
    tls_acceptor: Option<TlsAcceptor>,
    listener: Option<SyslogListener>,
}

#[derive(Debug, Error)]
enum SyslogListenerError {
    #[error("Syslog {transport} bind '{addr}' failed: {source}")]
    Bind {
        transport: &'static str,
        addr: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Syslog UDP listener receive failed: {source}")]
    UdpReceive {
        #[source]
        source: std::io::Error,
    },
    #[error("Syslog stream listener accept failed: {source}")]
    StreamAccept {
        #[source]
        source: std::io::Error,
    },
    #[error("Syslog stream intake queue closed")]
    IntakeQueueClosed,
}

#[derive(Debug, Error)]
enum SyslogConnectionError {
    #[error("TLS handshake failed: {source}")]
    TlsHandshake {
        #[source]
        source: std::io::Error,
    },
    #[error("stream read failed: {source}")]
    StreamRead {
        #[source]
        source: std::io::Error,
    },
    #[error("connection ended with an incomplete Syslog frame")]
    IncompleteFrame,
    #[error(transparent)]
    Frame(#[from] SyslogFrameError),
}

#[derive(Debug, Error)]
enum SyslogFrameError {
    #[error("malformed Syslog octet-counting length prefix")]
    MalformedOctetCount,
    #[error("malformed Syslog octet count: {source}")]
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
    type Message = SyslogSourceMessage;
    type Position = ();

    async fn open(plan: &Self::Plan, _instance_index: u64) -> SourceResult<Self> {
        let tls_acceptor = if plan.config.protocol == SyslogProtocol::Tls {
            let config = plan.config.tls_server_config().map_err(|error| {
                Report::new(error).change_context(SourceError::Open { connector: SYSLOG })
            })?;
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

impl SyslogSource {
    async fn bind_listener(&self) -> Result<SyslogListener, Report<SyslogListenerError>> {
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
    ) -> Result<SyslogSourceMessage, Report<SyslogListenerError>> {
        loop {
            tokio::task::consume_budget().await;
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
    ) -> Result<SyslogSourceMessage, Report<SyslogListenerError>> {
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
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
                                .map_err(|source| SyslogConnectionError::TlsHandshake { source })?;
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
                            debug!(error = %error, "closed malformed or failed syslog connection");
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

async fn read_stream_connection(
    mut stream: impl AsyncRead + Unpin,
    peer_addr: SocketAddr,
    max_message_size: NonZeroUsize,
    allow_non_transparent: bool,
    tx: mpsc::Sender<ReceivedSyslogFrame>,
    mut paused: watch::Receiver<bool>,
) -> Result<(), SyslogConnectionError> {
    let mut decoder = StreamFrameDecoder::new(max_message_size, allow_non_transparent);
    loop {
        tokio::task::consume_budget().await;
        if *paused.borrow() {
            if paused.changed().await.is_err() {
                return Ok(());
            }
            continue;
        }
        if let Some(frame) = decoder.next_frame()? {
            tokio::select! {
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
        let read_capacity = decoder.read_capacity()?;
        let mut chunk = [0_u8; 8_192];
        let read_capacity = read_capacity.min(chunk.len());
        let read = tokio::select! {
            changed = paused.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                continue;
            }
            read = stream.read(&mut chunk[..read_capacity]) => read,
        }
        .map_err(|source| SyslogConnectionError::StreamRead { source })?;
        if read == 0 {
            return if decoder.is_empty() {
                Ok(())
            } else {
                Err(SyslogConnectionError::IncompleteFrame)
            };
        }
        decoder.extend(&chunk[..read]);
    }
}

impl SyslogIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: SyslogIngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let SyslogIngestorStartPlan { ingestor, client } = plan;
        let domain = &ingestor.domain;
        let key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.name.clone());
        if runtime.inner.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }
        let resolved = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
        let connector = SyslogSourcePlan::new(resolved.entries, |configured| {
            runtime.syslog_ingestor_bind_addr(configured)
        })
        .map_err(|error| RuntimeError::StartIngestor {
            domain: domain.as_str().to_string(),
            ingestor: ingestor.name.as_str().to_string(),
            reason: error.to_string(),
        })?;
        let acknowledgement = SourceAckPolicy::None;
        let source_plan = SourcePlan {
            connector,
            capabilities: SourceCapabilities::new(
                ingestor.allow_header_reads,
                ingestor.metadata_kind.source_scope(),
                ingestor.quiesce.supports(ingestor.quiesce.mode()),
                NonZeroU64::MIN,
                acknowledgement.support(),
            ),
            acknowledgement,
        };
        let source = SyslogSource::open(&source_plan.connector, 0)
            .await
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
        let branched_runtime = runtime.start_branched_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.branched_templates,
        );
        let quiesce = runtime
            .ingestor_quiesce_control(domain, &ingestor.name)
            .verified(
                "the runtime registers quiesce control for an ingestor before it starts the task",
            );
        let (shutdown_tx, _) = watch::channel(false);
        runtime.prepare_ingestor_readiness(
            domain,
            &ingestor.name,
            source_plan.capabilities.instances(),
        );
        let host = BrokerSourceHost::build(BrokerSourceHostSpec {
            runtime: runtime.clone(),
            domain: domain.clone(),
            ingestor: ingestor.name.clone(),
            timestamp_source: ingestor.timestamp_source.clone(),
            output_routes: dependencies.output_routes,
            filter_where: dependencies.filter_where,
            codec: dependencies.codec,
            metrics: dependencies.metrics,
            branched_senders: branched_runtime.senders.clone(),
            quiesce,
            shutdown: shutdown_tx.subscribe(),
            instance_index: 0,
            metadata_kind: ingestor.metadata_kind,
            buffered_intake: true,
        });
        let task_domain = domain.clone();
        let task_ingestor = ingestor.name.clone();
        let shutdown = shutdown_tx.subscribe();
        let acknowledgement = source_plan.acknowledgement;
        let client_mounts = resolved.mounts;
        let task = tokio::spawn(async move {
            let _client_mounts = client_mounts;
            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                "started syslog ingestor"
            );
            run_source_instance_with_retry(
                source,
                host,
                acknowledgement,
                SYSLOG_RETRY_POLICY,
                shutdown,
            )
            .await;
            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                "stopped syslog ingestor"
            );
        });
        runtime.inner.ingestors.insert(
            key,
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
                tasks: vec![task],
            },
        );
        Ok(())
    }
}

struct StreamFrameDecoder {
    bytes: Vec<u8>,
    max_message_size: NonZeroUsize,
    allow_non_transparent: bool,
}

impl StreamFrameDecoder {
    fn new(max_message_size: NonZeroUsize, allow_non_transparent: bool) -> Self {
        Self {
            bytes: Vec::new(),
            max_message_size,
            allow_non_transparent,
        }
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn extend(&mut self, bytes: &[u8]) {
        self.bytes.extend_from_slice(bytes);
    }

    fn read_capacity(&self) -> Result<usize, SyslogFrameError> {
        let cap = self
            .max_message_size
            .get()
            .checked_add(MAX_OCTET_COUNT_DIGITS + 1)
            .ok_or(SyslogFrameError::OversizedBufferedFrame {
                maximum: self.max_message_size,
            })?;
        // The buffer is filled to at most `cap` bytes, so a longer one has no room left.
        let remaining = cap.saturating_sub(self.bytes.len());
        if remaining == 0 {
            Err(SyslogFrameError::OversizedBufferedFrame {
                maximum: self.max_message_size,
            })
        } else {
            Ok(remaining)
        }
    }

    fn next_frame(&mut self) -> Result<Option<Vec<u8>>, SyslogFrameError> {
        let Some(first) = self.bytes.first().copied() else {
            return Ok(None);
        };
        if first.is_ascii_digit() {
            self.next_octet_counted_frame()
        } else if !self.allow_non_transparent {
            Err(SyslogFrameError::NonOctetTlsFrame)
        } else {
            self.next_non_transparent_frame()
        }
    }

    fn next_octet_counted_frame(&mut self) -> Result<Option<Vec<u8>>, SyslogFrameError> {
        let delimiter = self.bytes.iter().position(|byte| *byte == b' ');
        let Some(delimiter) = delimiter else {
            if self.bytes.len() > MAX_OCTET_COUNT_DIGITS
                || self.bytes.iter().any(|byte| !byte.is_ascii_digit())
            {
                return Err(SyslogFrameError::MalformedOctetCount);
            }
            return Ok(None);
        };
        if delimiter == 0 || delimiter > MAX_OCTET_COUNT_DIGITS {
            return Err(SyslogFrameError::MalformedOctetCount);
        }
        let prefix = &self.bytes[..delimiter];
        if prefix.first() == Some(&b'0') || !prefix.iter().all(|byte| byte.is_ascii_digit()) {
            return Err(SyslogFrameError::MalformedOctetCount);
        }
        let prefix = std::str::from_utf8(prefix)
            .verified("the check above rejected every prefix that is not made of ASCII digits");
        let length = prefix
            .parse::<usize>()
            .map_err(|source| SyslogFrameError::InvalidOctetCount { source })?;
        if length > self.max_message_size.get() {
            return Err(SyslogFrameError::OversizedOctetCount {
                length,
                maximum: self.max_message_size,
            });
        }
        let payload_start = delimiter
            .checked_add(1)
            .verified("the delimiter position is an index into the buffered bytes");
        let frame_end = payload_start
            .checked_add(length)
            .verified("the octet count checked above is at most the maximum message size");
        if self.bytes.len() < frame_end {
            return Ok(None);
        }
        let frame = self.bytes[payload_start..frame_end].to_vec();
        self.bytes.drain(..frame_end);
        Ok(Some(frame))
    }

    fn next_non_transparent_frame(&mut self) -> Result<Option<Vec<u8>>, SyslogFrameError> {
        let Some(delimiter) = self.bytes.iter().position(|byte| *byte == b'\n') else {
            let pending_payload_size = self
                .bytes
                .len()
                .checked_sub(usize::from(self.bytes.last() == Some(&b'\r')))
                .verified("a trailing carriage return means the buffer holds at least one byte");
            if pending_payload_size > self.max_message_size.get() {
                return Err(SyslogFrameError::OversizedNonTransparentFrame {
                    maximum: self.max_message_size,
                });
            }
            return Ok(None);
        };
        let payload_end = if delimiter > 0 && self.bytes[delimiter - 1] == b'\r' {
            delimiter - 1
        } else {
            delimiter
        };
        if payload_end > self.max_message_size.get() {
            return Err(SyslogFrameError::OversizedNonTransparentFrame {
                maximum: self.max_message_size,
            });
        }
        let frame = self.bytes[..payload_end].to_vec();
        self.bytes.drain(..=delimiter);
        Ok(Some(frame))
    }
}

#[cfg(test)]
mod tests {
    use nonzero_ext::nonzero;

    use super::*;

    #[test]
    fn stream_decoder_interleaves_both_rfc6587_framings() {
        let mut decoder = StreamFrameDecoder::new(nonzero!(128usize), true);
        decoder.extend(b"5 helloalpha\r\n4 test");
        assert_eq!(
            decoder.next_frame().expect("valid frame"),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            decoder.next_frame().expect("valid frame"),
            Some(b"alpha".to_vec())
        );
        assert_eq!(
            decoder.next_frame().expect("valid frame"),
            Some(b"test".to_vec())
        );
        assert_eq!(decoder.next_frame().expect("needs data"), None);
    }

    #[test]
    fn stream_decoder_rejects_malformed_and_oversized_frames() {
        let mut malformed = StreamFrameDecoder::new(nonzero!(128usize), true);
        malformed.extend(b"12x payload");
        assert!(malformed.next_frame().is_err());

        let mut oversized_count = StreamFrameDecoder::new(nonzero!(4usize), true);
        oversized_count.extend(b"5 hello");
        assert!(oversized_count.next_frame().is_err());

        let mut oversized_line = StreamFrameDecoder::new(nonzero!(4usize), true);
        oversized_line.extend(b"hello\n");
        assert!(oversized_line.next_frame().is_err());
    }

    #[test]
    fn stream_decoder_limits_octet_count_prefix_to_ten_digits() {
        let mut decoder = StreamFrameDecoder::new(nonzero!(128usize), true);
        decoder.extend(b"12345678901");
        assert!(decoder.next_frame().is_err());
    }

    #[test]
    fn stream_decoder_rejects_zero_and_leading_zero_octet_counts() {
        for frame in [b"0 ".as_slice(), b"05 hello".as_slice()] {
            let mut decoder = StreamFrameDecoder::new(nonzero!(128usize), true);
            decoder.extend(frame);
            assert!(decoder.next_frame().is_err());
        }
    }

    #[test]
    fn stream_decoder_accepts_a_maximum_size_frame_with_split_crlf() {
        let mut decoder = StreamFrameDecoder::new(nonzero!(5usize), true);
        decoder.extend(b"hello\r");
        assert_eq!(
            decoder.next_frame().expect("trailing CR may await LF"),
            None
        );
        decoder.extend(b"\n");
        assert_eq!(
            decoder
                .next_frame()
                .expect("maximum-size CRLF frame is valid"),
            Some(b"hello".to_vec())
        );
    }

    #[test]
    fn stream_decoder_rejects_non_transparent_tls_framing() {
        let mut decoder = StreamFrameDecoder::new(nonzero!(128usize), false);
        decoder.extend(b"<13>line framed\n");
        assert!(matches!(
            decoder.next_frame(),
            Err(SyslogFrameError::NonOctetTlsFrame)
        ));
    }
}
