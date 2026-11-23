//! I/O lifetimes and HTTP protocol timers at a listening boundary.
//!
//! Layer: edges.
//! - **Owns.** Idle expiry, admission leases, Hyper timers, every limit the HTTP/1 and gRPC
//!   protocol builders set, and completed-refusal transport teardown.
//! - **Depends on.** Selected timers and permits, Tokio I/O traits and protocol timer traits.
//! - **Must not know.** Routes, graphs or request execution.

#![cfg_attr(nervix_lint, nervix::layer(edge, reason = "owns timed listener I/O"))]

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use meticulous::ResultExt as _;
use nervix_primitives::{
    sync::OwnedSemaphorePermit,
    time::{Instant, Sleep, sleep, sleep_until, timeout},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::server::Connected;

use super::{listener_capacity::ConnectionExpiry, listener_settings::ListenerSettings};

/// The transport Hyper has finished and half-closed after an HTTP refusal.
pub(super) struct RefusedTransport<IO> {
    io: IO,
    settings: ListenerSettings,
}
impl<IO: AsyncRead + Unpin> RefusedTransport<IO> {
    pub(super) fn new(io: IO, settings: ListenerSettings) -> Self {
        Self { io, settings }
    }
    /// Discard opaque residual transport bytes (RFC 9112 §9.6), without parsing another message.
    /// The caller owns cancellation; this phase also ends at peer closure or its declared grace.
    pub(super) async fn finish(self) {
        use nervix_recovery::Reported as _;

        let capacity = usize::try_from(self.settings.buffer_bytes().as_u64())
            .assured("the listener read-buffer setting checks native representation");
        let mut reader = tokio::io::BufReader::with_capacity(capacity, self.io);
        let mut discarded = tokio::io::sink();
        let drain = tokio::io::copy_buf(&mut reader, &mut discarded);
        match timeout(self.settings.http_refusal_drain_timeout(), drain).await {
            Ok(result) => result
                .map_err(error_stack::Report::new)
                .reported("finishing an already refused HTTP transport"),
            Err(error) => {
                tracing::debug!(%error, "HTTP refusal transport drain reached its declared deadline");
            }
        }
    }
}

/// Admission follows the actual I/O, including when HTTP transfers it to a WebSocket.
pub(in crate::application) struct ListenerIo<IO> {
    io: IO,
    _lease: OwnedSemaphorePermit,
    idle: IdleDeadline,
    timeout: Duration,
}
enum IdleDeadline {
    Armed(Pin<Box<Sleep>>),
    Unrepresentable,
}
impl<IO> ListenerIo<IO> {
    pub(super) fn new(io: IO, lease: OwnedSemaphorePermit, timeout: Duration) -> Self {
        let idle = match Instant::now().checked_add(timeout) {
            Some(deadline) => IdleDeadline::Armed(Box::pin(sleep_until(deadline))),
            None => IdleDeadline::Unrepresentable,
        };
        Self {
            io,
            _lease: lease,
            idle,
            timeout,
        }
    }
    fn failure(&mut self, cx: &mut Context<'_>) -> Option<ConnectionExpiry> {
        match &mut self.idle {
            IdleDeadline::Unrepresentable => Some(ConnectionExpiry::Unrepresentable),
            IdleDeadline::Armed(timer) => timer
                .as_mut()
                .poll(cx)
                .is_ready()
                .then_some(ConnectionExpiry::Idle),
        }
    }
    fn progress(&mut self) {
        // Return the completed read/write unchanged even if renewing its deadline cannot fit.
        // The next I/O poll reports that typed clock failure before doing any more I/O.
        match Instant::now().checked_add(self.timeout) {
            Some(deadline) => {
                if let IdleDeadline::Armed(timer) = &mut self.idle {
                    timer.as_mut().reset(deadline);
                }
            }
            None => self.idle = IdleDeadline::Unrepresentable,
        }
    }
}
impl<IO: AsyncRead + Unpin> AsyncRead for ListenerIo<IO> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(failure) = this.failure(cx) {
            return Poll::Ready(Err(failure.into_io_error()));
        }
        let before = buf.filled().len();
        let result = Pin::new(&mut this.io).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            this.progress();
        }
        result
    }
}
impl<IO: AsyncWrite + Unpin> AsyncWrite for ListenerIo<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Some(failure) = this.failure(cx) {
            return Poll::Ready(Err(failure.into_io_error()));
        }
        let result = Pin::new(&mut this.io).poll_write(cx, buf);
        if matches!(result, Poll::Ready(Ok(n)) if n > 0) {
            this.progress();
        }
        result
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Some(failure) = this.failure(cx) {
            return Poll::Ready(Err(failure.into_io_error()));
        }
        let result = Pin::new(&mut this.io).poll_write_vectored(cx, bufs);
        if matches!(result, Poll::Ready(Ok(n)) if n > 0) {
            this.progress();
        }
        result
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Some(failure) = this.failure(cx) {
            return Poll::Ready(Err(failure.into_io_error()));
        }
        Pin::new(&mut this.io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}
impl<IO: Connected> Connected for ListenerIo<IO> {
    type ConnectInfo = IO::ConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.io.connect_info()
    }
}
#[cfg(all(test, feature = "testing"))]
impl ListenerIo<nervix_primitives::net::TcpStream> {
    pub(super) fn nodelay(&self) -> io::Result<bool> {
        self.io.nodelay()
    }
}

/// Hyper's interface names the real Instant type; every clock read and wakeup stays selected.
#[derive(Debug, Clone, Copy)]
pub(super) struct ListenerTimer;
impl hyper::rt::Timer for ListenerTimer {
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn hyper::rt::Sleep>> {
        Box::pin(HttpSleep(Box::pin(sleep(duration))))
    }
    fn sleep_until(
        &self,
        deadline: nervix_primitives::unmodeled::time::Instant,
    ) -> Pin<Box<dyn hyper::rt::Sleep>> {
        Box::pin(HttpSleep(Box::pin(sleep_until(deadline.into()))))
    }
    fn now(&self) -> nervix_primitives::unmodeled::time::Instant {
        Instant::now().into()
    }
    fn reset(
        &self,
        timer: &mut Pin<Box<dyn hyper::rt::Sleep>>,
        deadline: nervix_primitives::unmodeled::time::Instant,
    ) {
        if let Some(timer) = timer.as_mut().downcast_mut_pin::<HttpSleep>() {
            timer.get_mut().0.as_mut().reset(deadline.into());
        } else {
            *timer = self.sleep_until(deadline);
        }
    }
}
#[derive(Debug)]
struct HttpSleep(Pin<Box<Sleep>>);
impl Future for HttpSleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.get_mut().0.as_mut().poll(cx)
    }
}
impl hyper::rt::Sleep for HttpSleep {}

pub(super) fn http_builder(settings: ListenerSettings) -> hyper::server::conn::http1::Builder {
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .timer(ListenerTimer)
        .header_read_timeout(settings.header_timeout())
        .max_header_size(
            usize::try_from(settings.header_bytes().as_u64())
                .assured("the header setting checks usize representation"),
        )
        .max_buf_size(
            usize::try_from(settings.buffer_bytes().as_u64())
                .assured("the buffer setting checks usize representation"),
        )
        .max_headers(
            usize::try_from(settings.max_headers())
                .assured("the header count setting checks usize representation"),
        )
        .keep_alive(settings.http_keepalive());
    builder
}

/// The largest HTTP/2 frame the gRPC listener accepts.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 9113 section 6.5.2 starts SETTINGS_MAX_FRAME_SIZE at 16,384 octets, the \
                  frame every HTTP/2 peer must accept; the receive windows, not the frame size, \
                  bound what a connection buffers"
    )
)]
const GRPC_MAX_FRAME_BYTES: u32 = 16 * 1024;

/// Every HTTP/2 option the gRPC listener sets, from its declared settings and fixed values, so
/// that none is left to a library default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GrpcHttp2Limits {
    max_streams: u32,
    header_list_bytes: u32,
    keepalive_interval: Duration,
    keepalive_timeout: Duration,
    stream_window_bytes: u32,
    connection_window_bytes: u32,
    max_frame_bytes: u32,
    /// Whether flow control resizes the windows from measured bandwidth-delay. It never does: that
    /// grows them past the declared windows, which bound a connection's buffered input.
    adaptive_window: bool,
    send_buffer_bytes: usize,
    max_pending_reset_streams: usize,
    max_error_reset_streams: usize,
}
impl GrpcHttp2Limits {
    fn of(settings: &ListenerSettings) -> Self {
        Self {
            max_streams: settings.grpc_max_streams(),
            header_list_bytes: u32::try_from(settings.grpc_header_bytes().as_u64())
                .assured("the H2 header setting checks u32 representation"),
            keepalive_interval: settings.grpc_keepalive_interval(),
            keepalive_timeout: settings.grpc_keepalive_timeout(),
            stream_window_bytes: u32::try_from(settings.grpc_stream_window_bytes().as_u64())
                .assured("the stream window setting checks HTTP/2's 2^31-1 window bound"),
            connection_window_bytes: u32::try_from(
                settings.grpc_connection_window_bytes().as_u64(),
            )
            .assured("the connection window setting checks HTTP/2's 2^31-1 window bound"),
            max_frame_bytes: GRPC_MAX_FRAME_BYTES,
            adaptive_window: false,
            send_buffer_bytes: usize::try_from(settings.grpc_send_buffer_bytes().as_u64())
                .assured("the send buffer setting checks u32 and usize representation"),
            max_pending_reset_streams: usize::try_from(settings.grpc_max_pending_reset_streams())
                .assured("the reset stream setting checks usize representation"),
            max_error_reset_streams: usize::try_from(settings.grpc_max_error_reset_streams())
                .assured("the reset stream setting checks usize representation"),
        }
    }

    fn apply(self, server: tonic::transport::Server) -> tonic::transport::Server {
        server
            .max_concurrent_streams(self.max_streams)
            .http2_max_header_list_size(self.header_list_bytes)
            .http2_keepalive_interval(Some(self.keepalive_interval))
            .http2_keepalive_timeout(Some(self.keepalive_timeout))
            .initial_stream_window_size(self.stream_window_bytes)
            .initial_connection_window_size(self.connection_window_bytes)
            .max_frame_size(self.max_frame_bytes)
            .http2_adaptive_window(Some(self.adaptive_window))
            .http2_max_send_buf_size(self.send_buffer_bytes)
            .http2_max_pending_accept_reset_streams(Some(self.max_pending_reset_streams))
            .http2_max_local_error_reset_streams(Some(self.max_error_reset_streams))
    }
}

/// Tonic is served through a custom incoming stream, whose selected TCP owner applies kernel
/// keepalive. The transport builder owns the HTTP/2 and request-head admission bounds.
pub(super) fn grpc_builder(settings: ListenerSettings) -> tonic::transport::Server {
    let server = tonic::transport::Server::builder()
        .concurrency_limit_per_connection(
            usize::try_from(settings.grpc_concurrency())
                .assured("the concurrency setting checks native admission limits"),
        )
        .timeout(settings.grpc_request_timeout());
    GrpcHttp2Limits::of(&settings).apply(server)
}

#[cfg(all(
    test,
    not(any(feature = "shuttle", feature = "loom", feature = "deloxide"))
))]
mod tests {
    use meticulous::OptionExt as _;
    use nervix_primitives::sync::{Semaphore, StdArc};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use ubyte::ByteUnit;

    use super::*;
    use crate::application::listener_settings::ListenerOverrides;

    /// The defaults are the values hyper 1 and h2 0.4 applied while the listener left them to the
    /// libraries; declared settings reach the listener, and the fixed frame size and window
    /// policy never change with them.
    #[test]
    fn the_grpc_listener_sets_every_http2_option_to_its_declared_or_fixed_value() {
        assert_eq!(
            GrpcHttp2Limits::of(&ListenerSettings::default()),
            GrpcHttp2Limits {
                max_streams: 256,
                header_list_bytes: 16 * 1024,
                keepalive_interval: Duration::from_secs(30),
                keepalive_timeout: Duration::from_secs(20),
                stream_window_bytes: 1024 * 1024,
                connection_window_bytes: 1024 * 1024,
                max_frame_bytes: 16 * 1024,
                adaptive_window: false,
                send_buffer_bytes: 400 * 1024,
                max_pending_reset_streams: 20,
                max_error_reset_streams: 1024,
            }
        );
        let declared = ListenerSettings::resolve(ListenerOverrides {
            grpc_max_streams: Some(9),
            grpc_header_bytes: Some(ByteUnit::Kibibyte(32)),
            grpc_keepalive_interval: Some(Duration::from_secs(5)),
            grpc_keepalive_timeout: Some(Duration::from_secs(2)),
            grpc_stream_window_bytes: Some(ByteUnit::Kibibyte(96)),
            grpc_connection_window_bytes: Some(ByteUnit::Kibibyte(80)),
            grpc_send_buffer_bytes: Some(ByteUnit::Kibibyte(64)),
            grpc_max_pending_reset_streams: Some(7),
            grpc_max_error_reset_streams: Some(11),
            ..Default::default()
        })
        .assured("the declared gRPC limits are valid");
        assert_eq!(
            GrpcHttp2Limits::of(&declared),
            GrpcHttp2Limits {
                max_streams: 9,
                header_list_bytes: 32 * 1024,
                keepalive_interval: Duration::from_secs(5),
                keepalive_timeout: Duration::from_secs(2),
                stream_window_bytes: 96 * 1024,
                connection_window_bytes: 80 * 1024,
                max_frame_bytes: 16 * 1024,
                adaptive_window: false,
                send_buffer_bytes: 64 * 1024,
                max_pending_reset_streams: 7,
                max_error_reset_streams: 11,
            }
        );
    }

    #[nervix_primitives::test]
    async fn completed_io_keeps_its_byte_count_when_renewing_the_deadline_cannot_fit() {
        let lease = StdArc::new(Semaphore::new(1))
            .try_acquire_owned()
            .assured("the fixture's one semaphore permit is available");
        let (mut peer, socket) = tokio::io::duplex(16);
        let mut stream = ListenerIo::new(socket, lease, Duration::from_secs(1));
        // Exercise the owner's clock-failure path independently of startup validation.
        stream.timeout = Duration::MAX;
        let count = stream
            .write(b"sent")
            .await
            .assured("the fixture's empty duplex buffer holds four bytes");
        assert_eq!(count, 4, "completed writes must retain their exact count");
        let mut received = [0; 4];
        peer.read_exact(&mut received)
            .await
            .assured("the write completed before its renewal failed");
        assert_eq!(&received, b"sent");
        let error = stream
            .write(b"again")
            .await
            .expect_err("the next poll reports the unrepresentable deadline before I/O");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let report = error
            .get_ref()
            .and_then(|cause| {
                cause.downcast_ref::<super::super::listener_capacity::ConnectionExpiryReport>()
            })
            .assured("the listener wraps its complete typed expiry report in the I/O error");
        assert_eq!(
            *report.0.current_context(),
            ConnectionExpiry::Unrepresentable
        );
    }
}
