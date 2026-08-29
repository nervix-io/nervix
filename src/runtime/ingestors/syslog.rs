use std::net::SocketAddr;

use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    net::{TcpListener, UdpSocket},
    sync::mpsc,
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;

use super::super::*;
use crate::runtime::syslog::{SyslogClientConfig, SyslogDirection, SyslogProtocol};

const STREAM_INTAKE_QUEUE_CAPACITY: usize = 64;
const MAX_OCTET_COUNT_DIGITS: usize = 10;

pub(in crate::runtime) struct SyslogIngestor;

#[derive(Clone)]
struct SyslogIngestContext {
    runtime: Runtime,
    domain: Domain,
    ingestor: Identifier,
    timestamp_source: Option<IngestTimestampSource>,
    output_routes: RelayProcessorOutputsNode,
    filter_where: Option<CompiledProgramWithMaterializedInterest>,
    branched_senders: HashMap<Identifier, mpsc::Sender<BranchedEntrypointInput>>,
    codec: Arc<CompiledCodec>,
    quiesce: Arc<IngestorQuiesceControl>,
    events: broadcast::Sender<RuntimeEvent>,
}

struct ReceivedSyslogFrame {
    payload: Vec<u8>,
    peer_addr: SocketAddr,
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
    OversizedOctetCount { length: usize, maximum: usize },
    #[error("Syslog non-transparent frame exceeds max_message_size {maximum}")]
    OversizedNonTransparentFrame { maximum: usize },
    #[error("Syslog stream frame exceeds max_message_size {maximum}")]
    OversizedBufferedFrame { maximum: usize },
    #[error("Syslog TLS requires octet-counting framing")]
    NonOctetTlsFrame,
}

impl SyslogIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        client: CreateClientSyslog,
        ingestor: CreateIngestor,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }
        if let IngestSource::Syslog { .. } = &ingestor.source {
        } else {
            return Err(RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: "expected Syslog ingestor source".to_string(),
            });
        }
        let resolved = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason,
            })?;
        let config = SyslogClientConfig::parse(&resolved.entries, SyslogDirection::Ingest)
            .map_err(|reason| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: reason.to_string(),
            })?;
        let tls_acceptor = if config.protocol == SyslogProtocol::Tls {
            Some(TlsAcceptor::from(config.tls_server_config().map_err(
                |reason| RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: reason.to_string(),
                },
            )?))
        } else {
            None
        };

        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
        let branched_runtime = runtime.start_branched_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.branched_templates,
        );
        let context = SyslogIngestContext {
            runtime: runtime.clone(),
            domain: domain.clone(),
            ingestor: ingestor.name.clone(),
            timestamp_source: ingestor.timestamp_source.clone(),
            output_routes: dependencies.output_routes,
            filter_where: dependencies.filter_where,
            branched_senders: branched_runtime.senders.clone(),
            codec: dependencies.codec,
            quiesce: runtime
                .ingestor_quiesce_control(domain, &ingestor.name)
                .expect("scheduled Syslog ingestor must have quiesce control"),
            events: runtime.events.clone(),
        };
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let task_context = context.clone();
        let task = tokio::spawn(async move {
            let _client_mounts = resolved.mounts;
            let mut backoff = RuntimeReconnectBackoff::default();
            info!(
                domain = task_context.domain.as_str(),
                ingestor = task_context.ingestor.as_str(),
                "started syslog ingestor"
            );
            loop {
                tokio::task::consume_budget().await;
                if task_context
                    .runtime
                    .wait_if_ingestor_faulted(
                        &task_context.domain,
                        &task_context.ingestor,
                        &mut shutdown_rx,
                    )
                    .await
                {
                    break;
                }
                if task_context
                    .runtime
                    .ingestor_faults
                    .is_failed(&task_context.ingestor)
                {
                    continue;
                }
                let listener = match config.protocol {
                    SyslogProtocol::Udp => {
                        Self::run_udp_listener(
                            &task_context,
                            &config,
                            &mut backoff,
                            &mut shutdown_rx,
                        )
                        .await
                    }
                    SyslogProtocol::Tcp | SyslogProtocol::Tls => {
                        Self::run_stream_listener(
                            &task_context,
                            &config,
                            tls_acceptor.clone(),
                            &mut backoff,
                            &mut shutdown_rx,
                        )
                        .await
                    }
                };
                match listener {
                    Ok(()) => break,
                    Err(reason) => {
                        let reason = reason.to_string();
                        task_context
                            .runtime
                            .record_ingestor_transient_error_with_backoff(
                                &task_context.domain,
                                &task_context.ingestor,
                                reason.clone(),
                                backoff.next_delay(),
                            );
                        warn!(
                            domain = task_context.domain.as_str(),
                            ingestor = task_context.ingestor.as_str(),
                            error = reason,
                            "syslog listener failed; retrying"
                        );
                        if !backoff.wait(&mut shutdown_rx).await {
                            break;
                        }
                    }
                }
            }
            info!(
                domain = task_context.domain.as_str(),
                ingestor = task_context.ingestor.as_str(),
                "stopped syslog ingestor"
            );
        });

        runtime.ingestors.insert(
            key,
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
                tasks: vec![task],
            },
        );
        Ok(())
    }

    async fn run_udp_listener(
        context: &SyslogIngestContext,
        config: &SyslogClientConfig,
        backoff: &mut RuntimeReconnectBackoff,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> Result<(), SyslogListenerError> {
        let socket =
            UdpSocket::bind(&config.addr)
                .await
                .map_err(|source| SyslogListenerError::Bind {
                    transport: "UDP",
                    addr: config.addr.clone(),
                    source,
                })?;
        context
            .runtime
            .clear_ingestor_transient_error(&context.domain, &context.ingestor);
        backoff.reset();
        let mut collector = IngestRouteCollector::default();
        let mut datagram = vec![0_u8; 65_535];
        loop {
            tokio::task::consume_budget().await;
            if Self::dispatch_buffered(context, &mut collector).await {
                continue;
            }
            if context.quiesce.should_suspend_intake() {
                Self::flush_collector(context, &mut collector).await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return Ok(());
                        }
                    }
                    _ = context.quiesce.wait_until_not_suspended() => {}
                }
                continue;
            }
            let next_flush = collector.next_flush();
            let flush_at =
                next_flush.unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        Self::flush_collector(context, &mut collector).await;
                        return Ok(());
                    }
                }
                _ = sleep_until(flush_at), if next_flush.is_some() => {
                    Self::flush_collector(context, &mut collector).await;
                }
                _ = context.quiesce.wait_for_change() => {}
                received = socket.recv_from(&mut datagram) => {
                    let (size, peer_addr) = received
                        .map_err(|source| SyslogListenerError::UdpReceive { source })?;
                    if size > config.max_message_size {
                        debug!(
                            domain = context.domain.as_str(),
                            ingestor = context.ingestor.as_str(),
                            peer_addr = %peer_addr,
                            size,
                            max_message_size = config.max_message_size,
                            "dropped oversized syslog UDP datagram"
                        );
                        continue;
                    }
                    let payload = BufferedIngestPayload::new(
                        &datagram[..size],
                        IngestFilterMapMetadata::syslog(peer_addr),
                    );
                    if let IngestorQuiesceIntake::Dispatch(payload) =
                        context.quiesce.intake(0, payload, false)
                    {
                        Self::dispatch(context, &mut collector, &payload).await;
                    }
                }
            }
        }
    }

    async fn run_stream_listener(
        context: &SyslogIngestContext,
        config: &SyslogClientConfig,
        tls_acceptor: Option<TlsAcceptor>,
        backoff: &mut RuntimeReconnectBackoff,
        shutdown_rx: &mut watch::Receiver<bool>,
    ) -> Result<(), SyslogListenerError> {
        let listener =
            TcpListener::bind(&config.addr)
                .await
                .map_err(|source| SyslogListenerError::Bind {
                    transport: "stream",
                    addr: config.addr.clone(),
                    source,
                })?;
        context
            .runtime
            .clear_ingestor_transient_error(&context.domain, &context.ingestor);
        backoff.reset();
        let (frame_tx, mut frame_rx) = mpsc::channel(STREAM_INTAKE_QUEUE_CAPACITY);
        let mut connections = JoinSet::new();
        let mut collector = IngestRouteCollector::default();
        loop {
            tokio::task::consume_budget().await;
            if Self::dispatch_buffered(context, &mut collector).await {
                continue;
            }
            if context.quiesce.should_suspend_intake() {
                Self::flush_collector(context, &mut collector).await;
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            connections.abort_all();
                            return Ok(());
                        }
                    }
                    _ = context.quiesce.wait_until_not_suspended() => {}
                }
                continue;
            }
            let next_flush = collector.next_flush();
            let flush_at =
                next_flush.unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        connections.abort_all();
                        Self::flush_collector(context, &mut collector).await;
                        return Ok(());
                    }
                }
                _ = sleep_until(flush_at), if next_flush.is_some() => {
                    Self::flush_collector(context, &mut collector).await;
                }
                _ = context.quiesce.wait_for_change() => {}
                accepted = listener.accept() => {
                    let (stream, peer_addr) = accepted
                        .map_err(|source| SyslogListenerError::StreamAccept { source })?;
                    if let Err(error) = stream.set_nodelay(true) {
                        debug!(
                            domain = context.domain.as_str(),
                            ingestor = context.ingestor.as_str(),
                            peer_addr = %peer_addr,
                            error = %error,
                            "failed to configure accepted syslog connection"
                        );
                        continue;
                    }
                    let tx = frame_tx.clone();
                    let connection_quiesce = context.quiesce.clone();
                    let connection_shutdown = shutdown_rx.clone();
                    let max_message_size = config.max_message_size;
                    let connection_tls = tls_acceptor.clone();
                    let domain = context.domain.clone();
                    let ingestor = context.ingestor.clone();
                    connections.spawn(async move {
                        let result = if let Some(acceptor) = connection_tls {
                            match acceptor.accept(stream).await {
                                Ok(stream) => Self::read_stream_connection(
                                    stream,
                                    peer_addr,
                                    max_message_size,
                                    false,
                                    tx,
                                    connection_quiesce,
                                    connection_shutdown,
                                )
                                .await,
                                Err(source) => Err(SyslogConnectionError::TlsHandshake { source }),
                            }
                        } else {
                            Self::read_stream_connection(
                                stream,
                                peer_addr,
                                max_message_size,
                                true,
                                tx,
                                connection_quiesce,
                                connection_shutdown,
                            )
                            .await
                        };
                        if let Err(error) = result {
                            debug!(
                                domain = domain.as_str(),
                                ingestor = ingestor.as_str(),
                                peer_addr = %peer_addr,
                                error = %error,
                                "closed malformed or failed syslog connection"
                            );
                        }
                    });
                }
                frame = frame_rx.recv() => {
                    let Some(frame) = frame else {
                        return Err(SyslogListenerError::IntakeQueueClosed);
                    };
                    let payload = BufferedIngestPayload::new(
                        &frame.payload,
                        IngestFilterMapMetadata::syslog(frame.peer_addr),
                    );
                    if let IngestorQuiesceIntake::Dispatch(payload) =
                        context.quiesce.intake(0, payload, false)
                    {
                        Self::dispatch(context, &mut collector, &payload).await;
                    }
                }
                joined = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = joined
                        && !error.is_cancelled()
                    {
                        debug!(
                            domain = context.domain.as_str(),
                            ingestor = context.ingestor.as_str(),
                            error = %error,
                            "syslog connection task failed"
                        );
                    }
                }
            }
        }
    }

    async fn read_stream_connection(
        mut stream: impl AsyncRead + Unpin,
        peer_addr: SocketAddr,
        max_message_size: usize,
        allow_non_transparent: bool,
        tx: mpsc::Sender<ReceivedSyslogFrame>,
        quiesce: Arc<IngestorQuiesceControl>,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<(), SyslogConnectionError> {
        let mut decoder = StreamFrameDecoder::new(max_message_size, allow_non_transparent);
        loop {
            tokio::task::consume_budget().await;
            if quiesce.should_suspend_intake() {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return Ok(());
                        }
                    }
                    _ = quiesce.wait_until_not_suspended() => {}
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
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
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
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return Ok(());
                    }
                    continue;
                }
                _ = quiesce.wait_for_change() => continue,
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

    async fn dispatch_buffered(
        context: &SyslogIngestContext,
        collector: &mut IngestRouteCollector,
    ) -> bool {
        let Some(payload) = context.quiesce.pop_buffered(0) else {
            return false;
        };
        Self::dispatch(context, collector, &payload).await;
        true
    }

    async fn dispatch(
        context: &SyslogIngestContext,
        collector: &mut IngestRouteCollector,
        payload: &BufferedIngestPayload,
    ) {
        if let Err(error) = context
            .runtime
            .dispatch_raw_ingest_payload(RawIngestDispatch {
                domain: &context.domain,
                ingestor: &context.ingestor,
                timestamp_source: context.timestamp_source.as_ref(),
                output_routes: &context.output_routes,
                filter_where: context.filter_where.as_ref(),
                branched_senders: &context.branched_senders,
                codec: context.codec.clone(),
                payload,
                collector,
                flush: false,
            })
            .await
        {
            debug!(
                domain = context.domain.as_str(),
                ingestor = context.ingestor.as_str(),
                error,
                "skipped syslog frame after decode or route failure"
            );
        }
        if collector.len() >= INGEST_GROUP_MAX_ROWS {
            Self::flush_collector(context, collector).await;
        }
    }

    async fn flush_collector(context: &SyslogIngestContext, collector: &mut IngestRouteCollector) {
        if let Err(error) = context
            .runtime
            .flush_ingest_collector(
                &context.domain,
                &context.ingestor,
                &context.branched_senders,
                collector,
            )
            .await
        {
            let _ = context.events.send(RuntimeEvent::Error(format!(
                "failed to flush Syslog messages for ingestor '{}' in domain '{}': {error}",
                context.ingestor.as_str(),
                context.domain.as_str()
            )));
        }
    }
}

struct StreamFrameDecoder {
    bytes: Vec<u8>,
    max_message_size: usize,
    allow_non_transparent: bool,
}

impl StreamFrameDecoder {
    fn new(max_message_size: usize, allow_non_transparent: bool) -> Self {
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
            .saturating_add(MAX_OCTET_COUNT_DIGITS + 1);
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
        let prefix = std::str::from_utf8(prefix).expect("ASCII digit prefix must be valid UTF-8");
        let length = prefix
            .parse::<usize>()
            .map_err(|source| SyslogFrameError::InvalidOctetCount { source })?;
        if length > self.max_message_size {
            return Err(SyslogFrameError::OversizedOctetCount {
                length,
                maximum: self.max_message_size,
            });
        }
        let payload_start = delimiter + 1;
        let frame_end = payload_start.saturating_add(length);
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
                .saturating_sub(usize::from(self.bytes.last() == Some(&b'\r')));
            if pending_payload_size > self.max_message_size {
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
        if payload_end > self.max_message_size {
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
    use super::*;

    #[test]
    fn stream_decoder_interleaves_both_rfc6587_framings() {
        let mut decoder = StreamFrameDecoder::new(128, true);
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
        let mut malformed = StreamFrameDecoder::new(128, true);
        malformed.extend(b"12x payload");
        assert!(malformed.next_frame().is_err());

        let mut oversized_count = StreamFrameDecoder::new(4, true);
        oversized_count.extend(b"5 hello");
        assert!(oversized_count.next_frame().is_err());

        let mut oversized_line = StreamFrameDecoder::new(4, true);
        oversized_line.extend(b"hello\n");
        assert!(oversized_line.next_frame().is_err());
    }

    #[test]
    fn stream_decoder_limits_octet_count_prefix_to_ten_digits() {
        let mut decoder = StreamFrameDecoder::new(128, true);
        decoder.extend(b"12345678901");
        assert!(decoder.next_frame().is_err());
    }

    #[test]
    fn stream_decoder_rejects_zero_and_leading_zero_octet_counts() {
        for frame in [b"0 ".as_slice(), b"05 hello".as_slice()] {
            let mut decoder = StreamFrameDecoder::new(128, true);
            decoder.extend(frame);
            assert!(decoder.next_frame().is_err());
        }
    }

    #[test]
    fn stream_decoder_accepts_a_maximum_size_frame_with_split_crlf() {
        let mut decoder = StreamFrameDecoder::new(5, true);
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
        let mut decoder = StreamFrameDecoder::new(128, false);
        decoder.extend(b"<13>line framed\n");
        assert!(matches!(
            decoder.next_frame(),
            Err(SyslogFrameError::NonOctetTlsFrame)
        ));
    }
}
