//! The resolved limits enforced at the node's listening boundaries.
//!
//! Layer: edges.
//! - **Owns.** Typed local listener settings, their defaults and pre-I/O validation.
//! - **Depends on.** Setting vocabulary, byte quantities and the selected monotonic clock.
//! - **Must not know.** Graphs, sockets or running connections.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns local listener settings")
)]

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
pub use nervix_interconnect::AcceptRetryDelay;
use nervix_interconnect::MAX_HTTP2_WINDOW_BYTES;
use nervix_models::{
    ListenerBounds, NodeCapabilities, SessionFrameBytes, Setting, SettingDefinition,
};
use nervix_primitives::{sync::Semaphore, time::Instant};
use thiserror::Error;
use ubyte::ByteUnit;

/// A setting which cannot be enforced by this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ListenerSettingsError {
    #[error("{name} must be positive and fit the listener implementation")]
    Invalid { name: &'static str },
    #[error("listener buffer capacity must be at least 8192 bytes")]
    BufferTooSmall,
    #[error("listener accept retry delay is invalid")]
    AcceptRetryDelay,
    #[error("HTTP body and read scratch exceed the intake memory budget of {capacity} bytes")]
    InputExceedsMemory { capacity: u64 },
    #[error("{name} must be from {minimum} through {maximum} bytes")]
    Http2Window {
        name: &'static str,
        minimum: u64,
        maximum: u64,
    },
    #[error(
        "{headers} ({max_headers}) exceeds the {limit} header lines {head} ({header_bytes} bytes) \
         can hold",
        headers = MaxHeaders::NAME,
        head = HeaderBytes::NAME
    )]
    HeadersBeyondHeaderBytes {
        max_headers: u32,
        header_bytes: u64,
        limit: u64,
    },
    #[error(
        "{name} ({write_buffer} bytes) with one more message of the largest size a public \
         WebSocket sends exceeds this platform's address space",
        name = WebsocketWriteBufferBytes::NAME
    )]
    WebsocketWriteCapacity { write_buffer: u64 },
}

fn valid(name: &'static str, condition: bool) -> Result<(), Report<ListenerSettingsError>> {
    if condition {
        Ok(())
    } else {
        Err(Report::new(ListenerSettingsError::Invalid { name }))
    }
}

macro_rules! definition {
    ($name:ident, $flag:literal, $ty:ty, $default:expr, $validate:expr) => {
        pub struct $name;
        impl SettingDefinition for $name {
            type Class = NodeCapabilities;
            type Value = $ty;
            type Context = ();
            type Error = ListenerSettingsError;
            const NAME: &'static str = $flag;
            const DERIVATION: Option<&'static str> = None;
            fn default_value(_: &()) -> Self::Value {
                $default
            }
            fn validate(value: &Self::Value, _: &()) -> Result<(), Report<Self::Error>> {
                valid(Self::NAME, ($validate)(value))
            }
        }
    };
}

definition!(
    MaxConnections,
    "listener-max-connections",
    u32,
    1024,
    |v: &u32| *v > 0
        && usize::try_from(*v).is_ok_and(|n| n <= Semaphore::MAX_PERMITS)
        && *v <= i32::MAX.unsigned_abs()
);

definition!(
    HeaderBytes,
    "listener-header-bytes",
    ByteUnit,
    ByteUnit::Byte(16384),
    |v: &ByteUnit| v.as_u64() > 0 && usize::try_from(v.as_u64()).is_ok()
);

/// The fewest bytes one HTTP/1 header line can take: a one-byte name, its colon and a bare line
/// feed.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 9112 section 5 makes a field line a token name, a colon and an optional \
                  value, and section 2.2 lets a recipient end it with a bare line feed, so no \
                  header line is shorter than three bytes"
    )
)]
const SHORTEST_HEADER_LINE_BYTES: u64 = 3;

/// The headers the parser accepts in one HTTP/1 request head, or chunked trailer section.
///
/// The parser reserves one slot per allowed header for every request it reads, so the count is
/// held to the header lines `listener-header-bytes` can hold. A request with more headers than
/// that already exceeds the byte bound, so the derived default refuses nothing the byte bound
/// admits.
pub struct MaxHeaders;
impl MaxHeaders {
    /// The most header lines `header_bytes` can hold, and at least one, so a head bound too
    /// small for any line still resolves, as it did before the count was declared.
    fn limit(header_bytes: ByteUnit) -> u64 {
        let lines = header_bytes
            .as_u64()
            .checked_div(SHORTEST_HEADER_LINE_BYTES)
            .assured("the shortest header line is a positive number of bytes");
        lines.max(1)
    }
}
impl SettingDefinition for MaxHeaders {
    type Class = NodeCapabilities;
    type Value = u32;
    type Context = ByteUnit;
    type Error = ListenerSettingsError;
    const NAME: &'static str = "listener-max-headers";
    const DERIVATION: Option<&'static str> =
        Some("100, or the three-byte header lines listener-header-bytes holds when fewer");
    fn default_value(header_bytes: &ByteUnit) -> u32 {
        match u32::try_from(Self::limit(*header_bytes)) {
            Ok(limit) if limit < 100 => limit,
            _ => 100,
        }
    }
    fn validate(max_headers: &u32, header_bytes: &ByteUnit) -> Result<(), Report<Self::Error>> {
        valid(
            Self::NAME,
            *max_headers > 0 && usize::try_from(*max_headers).is_ok(),
        )?;
        let limit = Self::limit(*header_bytes);
        if u64::from(*max_headers) > limit {
            return Err(Report::new(
                ListenerSettingsError::HeadersBeyondHeaderBytes {
                    max_headers: *max_headers,
                    header_bytes: header_bytes.as_u64(),
                    limit,
                },
            ));
        }
        Ok(())
    }
}

definition!(
    BufferBytes,
    "listener-buffer-bytes",
    ByteUnit,
    ByteUnit::Byte(417792),
    |v: &ByteUnit| v.as_u64() > 0 && usize::try_from(v.as_u64()).is_ok()
);

definition!(
    HeaderTimeout,
    "listener-header-timeout",
    Duration,
    Duration::from_secs(30),
    |v: &Duration| !v.is_zero() && Instant::now().checked_add(*v).is_some()
);

definition!(
    TlsTimeout,
    "listener-tls-timeout",
    Duration,
    Duration::from_secs(10),
    |v: &Duration| !v.is_zero() && Instant::now().checked_add(*v).is_some()
);

definition!(
    IdleTimeout,
    "listener-idle-timeout",
    Duration,
    Duration::from_secs(60),
    |v: &Duration| !v.is_zero() && Instant::now().checked_add(*v).is_some()
);

definition!(
    HttpKeepalive,
    "listener-http-keepalive",
    bool,
    true,
    |_: &bool| true
);

definition!(
    HttpBodyBytes,
    "http-body-bytes",
    ByteUnit,
    ByteUnit::Byte(33554432),
    |v: &ByteUnit| v.as_u64() > 0 && usize::try_from(v.as_u64()).is_ok()
);

definition!(
    HttpBodyCopyBytes,
    "http-body-copy-bytes",
    ByteUnit,
    ByteUnit::Byte(64 * 1024),
    |v: &ByteUnit| v.as_u64() > 0 && usize::try_from(v.as_u64()).is_ok()
);

definition!(
    HttpBodyTimeout,
    "http-body-timeout",
    Duration,
    Duration::from_secs(30),
    |v: &Duration| !v.is_zero() && Instant::now().checked_add(*v).is_some()
);

definition!(
    HttpRefusalDrainTimeout,
    "http-refusal-drain-timeout",
    Duration,
    Duration::from_secs(5),
    |v: &Duration| !v.is_zero() && Instant::now().checked_add(*v).is_some()
);

definition!(
    WebsocketMessageBytes,
    "websocket-message-bytes",
    ByteUnit,
    ByteUnit::Byte(67108864),
    |v: &ByteUnit| v.as_u64() > 0 && usize::try_from(v.as_u64()).is_ok()
);

definition!(
    WebsocketFrameBytes,
    "websocket-frame-bytes",
    ByteUnit,
    ByteUnit::Byte(16777216),
    |v: &ByteUnit| v.as_u64() > 0 && usize::try_from(v.as_u64()).is_ok()
);

// The bytes an endpoint or console WebSocket collects before it writes them to its connection.
definition!(
    WebsocketWriteBufferBytes,
    "websocket-write-buffer-bytes",
    ByteUnit,
    ByteUnit::Kibibyte(128),
    |v: &ByteUnit| v.as_u64() > 0 && usize::try_from(v.as_u64()).is_ok()
);

/// The longest header a WebSocket frame carries, which a write buffer holds beside the message.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 6455 section 5.2 frames carry at most 14 header bytes: two fixed bytes, an \
                  eight-byte extended payload length and a four-byte masking key"
    )
)]
const WEBSOCKET_FRAME_HEADER_BYTES: usize = 14;

definition!(
    GrpcHeaderBytes,
    "grpc-header-bytes",
    ByteUnit,
    ByteUnit::Byte(16384),
    |v: &ByteUnit| v.as_u64() > 0 && u32::try_from(v.as_u64()).is_ok()
);

definition!(GrpcConcurrency, "grpc-concurrency", u32, 128, |v: &u32| *v
    > 0
    && usize::try_from(*v).is_ok_and(|n| n <= Semaphore::MAX_PERMITS)
    && *v <= i32::MAX.unsigned_abs());

definition!(
    GrpcRequestTimeout,
    "grpc-request-timeout",
    Duration,
    Duration::from_secs(120),
    |v: &Duration| !v.is_zero() && Instant::now().checked_add(*v).is_some()
);

definition!(GrpcMaxStreams, "grpc-max-streams", u32, 256, |v: &u32| *v
    > 0
    && usize::try_from(*v).is_ok_and(|n| n <= Semaphore::MAX_PERMITS)
    && *v <= i32::MAX.unsigned_abs());

/// The flow-control window RFC 9113 gives every new HTTP/2 connection before any WINDOW_UPDATE.
///
/// A connection window only grows from it, so a smaller declared window would bound nothing.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 9113 section 6.9.2 starts every connection flow-control window at 65,535 \
                  octets, and the receiver can only raise it"
    )
)]
const HTTP2_INITIAL_CONNECTION_WINDOW_BYTES: u64 = 65_535;

/// Refuses a window HTTP/2 cannot advertise or that does not bound what it declares.
fn http2_window(
    name: &'static str,
    window: ByteUnit,
    minimum: u64,
) -> Result<(), Report<ListenerSettingsError>> {
    if (minimum..=MAX_HTTP2_WINDOW_BYTES).contains(&window.as_u64()) {
        return Ok(());
    }
    Err(Report::new(ListenerSettingsError::Http2Window {
        name,
        minimum,
        maximum: MAX_HTTP2_WINDOW_BYTES,
    }))
}

/// The HTTP/2 receive window of one gRPC stream: what a client may send on it before the listener
/// reads.
pub struct GrpcStreamWindowBytes;
impl SettingDefinition for GrpcStreamWindowBytes {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = ListenerSettingsError;
    const NAME: &'static str = "grpc-stream-window-bytes";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Mebibyte(1)
    }
    fn validate(window: &ByteUnit, _: &()) -> Result<(), Report<Self::Error>> {
        http2_window(Self::NAME, *window, 1)
    }
}

/// The HTTP/2 receive window of one gRPC connection: what a client may send across all of its
/// streams before the listener reads.
pub struct GrpcConnectionWindowBytes;
impl SettingDefinition for GrpcConnectionWindowBytes {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = ListenerSettingsError;
    const NAME: &'static str = "grpc-connection-window-bytes";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Mebibyte(1)
    }
    fn validate(window: &ByteUnit, _: &()) -> Result<(), Report<Self::Error>> {
        http2_window(Self::NAME, *window, HTTP2_INITIAL_CONNECTION_WINDOW_BYTES)
    }
}

// The response bytes one gRPC stream queues before it waits; the HTTP/2 implementation takes at
// most `u32::MAX`.
definition!(
    GrpcSendBufferBytes,
    "grpc-send-buffer-bytes",
    ByteUnit,
    ByteUnit::Kibibyte(400),
    |v: &ByteUnit| v.as_u64() > 0
        && u32::try_from(v.as_u64()).is_ok()
        && usize::try_from(v.as_u64()).is_ok()
);

// Streams a client opened and reset that wait unaccepted at once; one more ends the connection
// with ENHANCE_YOUR_CALM, the HTTP/2 rapid-reset protection.
definition!(
    GrpcMaxPendingResetStreams,
    "grpc-max-pending-reset-streams",
    u32,
    20,
    |v: &u32| *v > 0 && usize::try_from(*v).is_ok()
);

// Streams the listener resets for a client's protocol errors before it ends the connection with
// ENHANCE_YOUR_CALM.
definition!(
    GrpcMaxErrorResetStreams,
    "grpc-max-error-reset-streams",
    u32,
    1024,
    |v: &u32| *v > 0 && usize::try_from(*v).is_ok()
);

definition!(
    GrpcKeepaliveInterval,
    "grpc-keepalive-interval",
    Duration,
    Duration::from_secs(30),
    |v: &Duration| !v.is_zero() && Instant::now().checked_add(*v).is_some()
);

definition!(
    GrpcKeepaliveTimeout,
    "grpc-keepalive-timeout",
    Duration,
    Duration::from_secs(20),
    |v: &Duration| !v.is_zero() && Instant::now().checked_add(*v).is_some()
);

// Linux's TCP_KEEPIDLE/TCP_KEEPINTVL and TCP_KEEPCNT socket-option ceilings.
#[cfg_attr(
    nervix_lint,
    nervix::constant(fixed, reason = "kernel TCP keepalive duration ceiling")
)]
const TCP_KEEPALIVE_MAX_SECONDS: u64 = 32767;
#[cfg_attr(
    nervix_lint,
    nervix::constant(fixed, reason = "kernel TCP keepalive probe-count ceiling")
)]
const TCP_KEEPALIVE_MAX_RETRIES: u32 = 127;

definition!(
    TcpKeepaliveTime,
    "listener-tcp-keepalive-time",
    Duration,
    Duration::from_secs(60),
    |v: &Duration| !v.is_zero()
        && v.subsec_nanos() == 0
        && v.as_secs() <= TCP_KEEPALIVE_MAX_SECONDS
);

definition!(
    TcpKeepaliveInterval,
    "listener-tcp-keepalive-interval",
    Duration,
    Duration::from_secs(10),
    |v: &Duration| !v.is_zero()
        && v.subsec_nanos() == 0
        && v.as_secs() <= TCP_KEEPALIVE_MAX_SECONDS
);

definition!(
    TcpKeepaliveRetries,
    "listener-tcp-keepalive-retries",
    u32,
    3,
    |v: &u32| (1..=TCP_KEEPALIVE_MAX_RETRIES).contains(v)
);

/// The gRPC listener's own bounds: its request handlers and the HTTP/2 limits it advertises and
/// enforces on every connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcListenerSettings {
    pub header_bytes: Setting<GrpcHeaderBytes>,
    pub concurrency: Setting<GrpcConcurrency>,
    pub request_timeout: Setting<GrpcRequestTimeout>,
    pub max_streams: Setting<GrpcMaxStreams>,
    pub stream_window_bytes: Setting<GrpcStreamWindowBytes>,
    pub connection_window_bytes: Setting<GrpcConnectionWindowBytes>,
    pub send_buffer_bytes: Setting<GrpcSendBufferBytes>,
    pub max_pending_reset_streams: Setting<GrpcMaxPendingResetStreams>,
    pub max_error_reset_streams: Setting<GrpcMaxErrorResetStreams>,
    pub keepalive_interval: Setting<GrpcKeepaliveInterval>,
    pub keepalive_timeout: Setting<GrpcKeepaliveTimeout>,
}
impl GrpcListenerSettings {
    fn resolve(inputs: &ListenerOverrides) -> Result<Self, Report<ListenerSettingsError>> {
        Ok(Self {
            header_bytes: Setting::<GrpcHeaderBytes>::resolve(inputs.grpc_header_bytes, &())?,
            concurrency: Setting::<GrpcConcurrency>::resolve(inputs.grpc_concurrency, &())?,
            request_timeout: Setting::<GrpcRequestTimeout>::resolve(
                inputs.grpc_request_timeout,
                &(),
            )?,
            max_streams: Setting::<GrpcMaxStreams>::resolve(inputs.grpc_max_streams, &())?,
            stream_window_bytes: Setting::<GrpcStreamWindowBytes>::resolve(
                inputs.grpc_stream_window_bytes,
                &(),
            )?,
            connection_window_bytes: Setting::<GrpcConnectionWindowBytes>::resolve(
                inputs.grpc_connection_window_bytes,
                &(),
            )?,
            send_buffer_bytes: Setting::<GrpcSendBufferBytes>::resolve(
                inputs.grpc_send_buffer_bytes,
                &(),
            )?,
            max_pending_reset_streams: Setting::<GrpcMaxPendingResetStreams>::resolve(
                inputs.grpc_max_pending_reset_streams,
                &(),
            )?,
            max_error_reset_streams: Setting::<GrpcMaxErrorResetStreams>::resolve(
                inputs.grpc_max_error_reset_streams,
                &(),
            )?,
            keepalive_interval: Setting::<GrpcKeepaliveInterval>::resolve(
                inputs.grpc_keepalive_interval,
                &(),
            )?,
            keepalive_timeout: Setting::<GrpcKeepaliveTimeout>::resolve(
                inputs.grpc_keepalive_timeout,
                &(),
            )?,
        })
    }
}

/// Validated settings, resolved once before listeners bind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListenerSettings {
    pub max_connections: Setting<MaxConnections>,
    pub header_bytes: Setting<HeaderBytes>,
    pub max_headers: Setting<MaxHeaders>,
    pub buffer_bytes: Setting<BufferBytes>,
    pub header_timeout: Setting<HeaderTimeout>,
    pub tls_timeout: Setting<TlsTimeout>,
    pub idle_timeout: Setting<IdleTimeout>,
    pub accept_retry_delay: Setting<AcceptRetryDelay>,
    pub http_keepalive: Setting<HttpKeepalive>,
    pub http_body_bytes: Setting<HttpBodyBytes>,
    pub http_body_copy_bytes: Setting<HttpBodyCopyBytes>,
    pub http_body_timeout: Setting<HttpBodyTimeout>,
    pub http_refusal_drain_timeout: Setting<HttpRefusalDrainTimeout>,
    pub websocket_message_bytes: Setting<WebsocketMessageBytes>,
    pub websocket_frame_bytes: Setting<WebsocketFrameBytes>,
    pub websocket_write_buffer_bytes: Setting<WebsocketWriteBufferBytes>,
    pub grpc: GrpcListenerSettings,
    pub tcp_keepalive_time: Setting<TcpKeepaliveTime>,
    pub tcp_keepalive_interval: Setting<TcpKeepaliveInterval>,
    pub tcp_keepalive_retries: Setting<TcpKeepaliveRetries>,
}

/// Optional inputs read before resolution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListenerOverrides {
    pub max_connections: Option<u32>,
    pub header_bytes: Option<ByteUnit>,
    pub max_headers: Option<u32>,
    pub buffer_bytes: Option<ByteUnit>,
    pub header_timeout: Option<Duration>,
    pub tls_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub accept_retry_delay: Option<Duration>,
    pub http_keepalive: Option<bool>,
    pub http_body_bytes: Option<ByteUnit>,
    pub http_body_copy_bytes: Option<ByteUnit>,
    pub http_body_timeout: Option<Duration>,
    pub http_refusal_drain_timeout: Option<Duration>,
    pub websocket_message_bytes: Option<ByteUnit>,
    pub websocket_frame_bytes: Option<ByteUnit>,
    pub websocket_write_buffer_bytes: Option<ByteUnit>,
    pub grpc_header_bytes: Option<ByteUnit>,
    pub grpc_concurrency: Option<u32>,
    pub grpc_request_timeout: Option<Duration>,
    pub grpc_max_streams: Option<u32>,
    pub grpc_stream_window_bytes: Option<ByteUnit>,
    pub grpc_connection_window_bytes: Option<ByteUnit>,
    pub grpc_send_buffer_bytes: Option<ByteUnit>,
    pub grpc_max_pending_reset_streams: Option<u32>,
    pub grpc_max_error_reset_streams: Option<u32>,
    pub grpc_keepalive_interval: Option<Duration>,
    pub grpc_keepalive_timeout: Option<Duration>,
    pub tcp_keepalive_time: Option<Duration>,
    pub tcp_keepalive_interval: Option<Duration>,
    pub tcp_keepalive_retries: Option<u32>,
}
impl ListenerSettings {
    /// Initial retained read allocation, request-head scratch and collection metadata.
    pub fn input_scratch_bytes(&self) -> Option<u64> {
        self.buffer_bytes()
            .as_u64()
            .checked_mul(4)?
            .checked_add(self.header_bytes().as_u64())?
            .checked_add(
                u64::try_from(std::mem::size_of::<bytes::Bytes>())
                    .ok()?
                    .checked_mul(4)?,
            )
    }
    pub fn validate_intake(&self, capacity: ByteUnit) -> Result<(), Report<ListenerSettingsError>> {
        let required = self
            .http_body_bytes()
            .as_u64()
            .checked_mul(2)
            .and_then(|body| self.input_scratch_bytes()?.checked_add(body));
        if required.is_none_or(|required| required > capacity.as_u64()) {
            return Err(Report::new(ListenerSettingsError::InputExceedsMemory {
                capacity: capacity.as_u64(),
            }));
        }
        Ok(())
    }
    pub fn resolve(inputs: ListenerOverrides) -> Result<Self, Report<ListenerSettingsError>> {
        let header_bytes = Setting::<HeaderBytes>::resolve(inputs.header_bytes, &())?;
        let max_headers = Setting::<MaxHeaders>::resolve(inputs.max_headers, header_bytes.value())?;
        let result = Self {
            max_connections: Setting::<MaxConnections>::resolve(inputs.max_connections, &())?,
            header_bytes,
            max_headers,
            buffer_bytes: Setting::<BufferBytes>::resolve(inputs.buffer_bytes, &())?,
            header_timeout: Setting::<HeaderTimeout>::resolve(inputs.header_timeout, &())?,
            tls_timeout: Setting::<TlsTimeout>::resolve(inputs.tls_timeout, &())?,
            idle_timeout: Setting::<IdleTimeout>::resolve(inputs.idle_timeout, &())?,
            accept_retry_delay: Setting::<AcceptRetryDelay>::resolve(
                inputs.accept_retry_delay,
                &(),
            )
            .change_context(ListenerSettingsError::AcceptRetryDelay)?,
            http_keepalive: Setting::<HttpKeepalive>::resolve(inputs.http_keepalive, &())?,
            http_body_bytes: Setting::<HttpBodyBytes>::resolve(inputs.http_body_bytes, &())?,
            http_body_copy_bytes: Setting::<HttpBodyCopyBytes>::resolve(
                inputs.http_body_copy_bytes,
                &(),
            )?,
            http_body_timeout: Setting::<HttpBodyTimeout>::resolve(inputs.http_body_timeout, &())?,
            http_refusal_drain_timeout: Setting::<HttpRefusalDrainTimeout>::resolve(
                inputs.http_refusal_drain_timeout,
                &(),
            )?,
            websocket_message_bytes: Setting::<WebsocketMessageBytes>::resolve(
                inputs.websocket_message_bytes,
                &(),
            )?,
            websocket_frame_bytes: Setting::<WebsocketFrameBytes>::resolve(
                inputs.websocket_frame_bytes,
                &(),
            )?,
            websocket_write_buffer_bytes: Setting::<WebsocketWriteBufferBytes>::resolve(
                inputs.websocket_write_buffer_bytes,
                &(),
            )?,
            grpc: GrpcListenerSettings::resolve(&inputs)?,
            tcp_keepalive_time: Setting::<TcpKeepaliveTime>::resolve(
                inputs.tcp_keepalive_time,
                &(),
            )?,
            tcp_keepalive_interval: Setting::<TcpKeepaliveInterval>::resolve(
                inputs.tcp_keepalive_interval,
                &(),
            )?,
            tcp_keepalive_retries: Setting::<TcpKeepaliveRetries>::resolve(
                inputs.tcp_keepalive_retries,
                &(),
            )?,
        };
        if result.buffer_bytes().as_u64() < 8192 {
            return Err(Report::new(ListenerSettingsError::BufferTooSmall));
        }
        // An endpoint WebSocket's largest message is the declared message bound, a console
        // WebSocket's one session frame, which at most reaches the frame ceiling.
        let endpoint_message = usize::try_from(result.websocket_message_bytes().as_u64())
            .assured("the WebSocket message setting checks usize representation");
        for largest_message in [endpoint_message, SessionFrameBytes::MAXIMUM] {
            if result.websocket_write_capacity(largest_message).is_none() {
                return Err(Report::new(ListenerSettingsError::WebsocketWriteCapacity {
                    write_buffer: result.websocket_write_buffer_bytes().as_u64(),
                }));
            }
        }
        Ok(result)
    }
    pub fn max_connections(&self) -> u32 {
        *self.max_connections.value()
    }
    pub fn header_bytes(&self) -> ByteUnit {
        *self.header_bytes.value()
    }
    pub fn max_headers(&self) -> u32 {
        *self.max_headers.value()
    }
    pub fn buffer_bytes(&self) -> ByteUnit {
        *self.buffer_bytes.value()
    }
    pub fn header_timeout(&self) -> Duration {
        *self.header_timeout.value()
    }
    pub fn tls_timeout(&self) -> Duration {
        *self.tls_timeout.value()
    }
    pub fn idle_timeout(&self) -> Duration {
        *self.idle_timeout.value()
    }
    pub fn accept_retry_delay(&self) -> Duration {
        *self.accept_retry_delay.value()
    }
    pub fn http_keepalive(&self) -> bool {
        *self.http_keepalive.value()
    }
    pub fn http_body_bytes(&self) -> ByteUnit {
        *self.http_body_bytes.value()
    }
    pub fn http_body_copy_bytes(&self) -> ByteUnit {
        *self.http_body_copy_bytes.value()
    }
    pub fn http_body_timeout(&self) -> Duration {
        *self.http_body_timeout.value()
    }
    pub fn http_refusal_drain_timeout(&self) -> Duration {
        *self.http_refusal_drain_timeout.value()
    }
    pub fn websocket_message_bytes(&self) -> ByteUnit {
        *self.websocket_message_bytes.value()
    }
    pub fn websocket_frame_bytes(&self) -> ByteUnit {
        *self.websocket_frame_bytes.value()
    }
    pub fn websocket_write_buffer_bytes(&self) -> ByteUnit {
        *self.websocket_write_buffer_bytes.value()
    }
    /// The most bytes a public WebSocket whose messages are at most `largest_message` bytes holds
    /// for writing: its write buffer, one more message and that message's frame header, or
    /// nothing when the sum does not fit the address space.
    ///
    /// The socket refuses a message that would take its write buffer past this capacity. A
    /// message is buffered only while the buffer holds at most the write buffer's bytes, so a
    /// message within `largest_message` is never refused, and a peer that stops reading holds no
    /// more than this.
    pub fn websocket_write_capacity(&self, largest_message: usize) -> Option<usize> {
        let write_buffer = usize::try_from(self.websocket_write_buffer_bytes().as_u64()).ok()?;
        write_buffer
            .checked_add(largest_message)?
            .checked_add(WEBSOCKET_FRAME_HEADER_BYTES)
    }
    /// The write capacity of an endpoint WebSocket, whose messages are at most
    /// `websocket-message-bytes`.
    pub fn endpoint_websocket_write_capacity(&self) -> usize {
        let largest_message = usize::try_from(self.websocket_message_bytes().as_u64())
            .assured("the WebSocket message setting checks usize representation");
        self.websocket_write_capacity(largest_message)
            .assured("resolution refuses a write buffer the endpoint's largest message overflows")
    }
    pub fn grpc_header_bytes(&self) -> ByteUnit {
        *self.grpc.header_bytes.value()
    }
    pub fn grpc_concurrency(&self) -> u32 {
        *self.grpc.concurrency.value()
    }
    pub fn grpc_request_timeout(&self) -> Duration {
        *self.grpc.request_timeout.value()
    }
    pub fn grpc_max_streams(&self) -> u32 {
        *self.grpc.max_streams.value()
    }
    pub fn grpc_stream_window_bytes(&self) -> ByteUnit {
        *self.grpc.stream_window_bytes.value()
    }
    pub fn grpc_connection_window_bytes(&self) -> ByteUnit {
        *self.grpc.connection_window_bytes.value()
    }
    pub fn grpc_send_buffer_bytes(&self) -> ByteUnit {
        *self.grpc.send_buffer_bytes.value()
    }
    pub fn grpc_max_pending_reset_streams(&self) -> u32 {
        *self.grpc.max_pending_reset_streams.value()
    }
    pub fn grpc_max_error_reset_streams(&self) -> u32 {
        *self.grpc.max_error_reset_streams.value()
    }
    pub fn grpc_keepalive_interval(&self) -> Duration {
        *self.grpc.keepalive_interval.value()
    }
    pub fn grpc_keepalive_timeout(&self) -> Duration {
        *self.grpc.keepalive_timeout.value()
    }
    pub fn tcp_keepalive_time(&self) -> Duration {
        *self.tcp_keepalive_time.value()
    }
    pub fn tcp_keepalive_interval(&self) -> Duration {
        *self.tcp_keepalive_interval.value()
    }
    pub fn tcp_keepalive_retries(&self) -> u32 {
        *self.tcp_keepalive_retries.value()
    }
    pub fn bounds(&self) -> ListenerBounds {
        ListenerBounds {
            max_connections: std::num::NonZeroU32::new(self.max_connections())
                .assured("the typed listener setting rejects zero"),
            header_bytes: std::num::NonZeroU64::new(self.header_bytes().as_u64())
                .assured("the typed listener setting rejects zero"),
            max_headers: std::num::NonZeroU32::new(self.max_headers())
                .assured("the typed listener setting rejects zero"),
            buffer_bytes: std::num::NonZeroU64::new(self.buffer_bytes().as_u64())
                .assured("the typed listener setting rejects zero"),
            header_timeout: self.header_timeout(),
            tls_timeout: self.tls_timeout(),
            idle_timeout: self.idle_timeout(),
            accept_retry_delay: self.accept_retry_delay(),
            http_keepalive: self.http_keepalive(),
            http_body_bytes: std::num::NonZeroU64::new(self.http_body_bytes().as_u64())
                .assured("the typed listener setting rejects zero"),
            http_body_copy_bytes: std::num::NonZeroU64::new(self.http_body_copy_bytes().as_u64())
                .assured("the typed listener setting rejects zero"),
            http_body_timeout: self.http_body_timeout(),
            http_refusal_drain_timeout: self.http_refusal_drain_timeout(),
            websocket_message_bytes: std::num::NonZeroU64::new(
                self.websocket_message_bytes().as_u64(),
            )
            .assured("the typed listener setting rejects zero"),
            websocket_frame_bytes: std::num::NonZeroU64::new(self.websocket_frame_bytes().as_u64())
                .assured("the typed listener setting rejects zero"),
            websocket_write_buffer_bytes: std::num::NonZeroU64::new(
                self.websocket_write_buffer_bytes().as_u64(),
            )
            .assured("the typed listener setting rejects zero"),
            grpc_header_bytes: std::num::NonZeroU64::new(self.grpc_header_bytes().as_u64())
                .assured("the typed listener setting rejects zero"),
            grpc_concurrency: std::num::NonZeroU32::new(self.grpc_concurrency())
                .assured("the typed listener setting rejects zero"),
            grpc_request_timeout: self.grpc_request_timeout(),
            grpc_max_streams: std::num::NonZeroU32::new(self.grpc_max_streams())
                .assured("the typed listener setting rejects zero"),
            grpc_stream_window_bytes: std::num::NonZeroU64::new(
                self.grpc_stream_window_bytes().as_u64(),
            )
            .assured("the typed listener setting rejects zero"),
            grpc_connection_window_bytes: std::num::NonZeroU64::new(
                self.grpc_connection_window_bytes().as_u64(),
            )
            .assured("the typed listener setting rejects zero"),
            grpc_send_buffer_bytes: std::num::NonZeroU64::new(
                self.grpc_send_buffer_bytes().as_u64(),
            )
            .assured("the typed listener setting rejects zero"),
            grpc_max_pending_reset_streams: std::num::NonZeroU32::new(
                self.grpc_max_pending_reset_streams(),
            )
            .assured("the typed listener setting rejects zero"),
            grpc_max_error_reset_streams: std::num::NonZeroU32::new(
                self.grpc_max_error_reset_streams(),
            )
            .assured("the typed listener setting rejects zero"),
            grpc_keepalive_interval: self.grpc_keepalive_interval(),
            grpc_keepalive_timeout: self.grpc_keepalive_timeout(),
            tcp_keepalive_time: self.tcp_keepalive_time(),
            tcp_keepalive_interval: self.tcp_keepalive_interval(),
            tcp_keepalive_retries: std::num::NonZeroU32::new(self.tcp_keepalive_retries())
                .assured("the typed listener setting rejects zero"),
        }
    }
}
impl Default for ListenerSettings {
    fn default() -> Self {
        Self::resolve(ListenerOverrides::default()).assured("declared listener defaults are valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_keepalive_inputs_fit_the_kernel_socket_options() {
        for seconds in [1, TCP_KEEPALIVE_MAX_SECONDS] {
            assert!(
                ListenerSettings::resolve(ListenerOverrides {
                    tcp_keepalive_time: Some(Duration::from_secs(seconds)),
                    tcp_keepalive_interval: Some(Duration::from_secs(seconds)),
                    ..Default::default()
                })
                .is_ok()
            );
        }
        for seconds in [0, TCP_KEEPALIVE_MAX_SECONDS + 1] {
            for inputs in [
                ListenerOverrides {
                    tcp_keepalive_time: Some(Duration::from_secs(seconds)),
                    ..Default::default()
                },
                ListenerOverrides {
                    tcp_keepalive_interval: Some(Duration::from_secs(seconds)),
                    ..Default::default()
                },
            ] {
                assert!(ListenerSettings::resolve(inputs).is_err(), "{inputs:?}");
            }
        }
        for retries in [1, TCP_KEEPALIVE_MAX_RETRIES] {
            assert!(
                ListenerSettings::resolve(ListenerOverrides {
                    tcp_keepalive_retries: Some(retries),
                    ..Default::default()
                })
                .is_ok()
            );
        }
        for retries in [0, TCP_KEEPALIVE_MAX_RETRIES + 1] {
            assert!(
                ListenerSettings::resolve(ListenerOverrides {
                    tcp_keepalive_retries: Some(retries),
                    ..Default::default()
                })
                .is_err()
            );
        }
    }

    #[test]
    fn listener_limits_refuse_unenforceable_inputs_before_io() {
        for inputs in [
            ListenerOverrides {
                max_connections: Some(0),
                ..Default::default()
            },
            ListenerOverrides {
                buffer_bytes: Some(ByteUnit::Byte(8191)),
                ..Default::default()
            },
            ListenerOverrides {
                http_body_bytes: Some(ByteUnit::Byte(0)),
                ..Default::default()
            },
            ListenerOverrides {
                header_timeout: Some(Duration::ZERO),
                ..Default::default()
            },
            ListenerOverrides {
                http_refusal_drain_timeout: Some(Duration::ZERO),
                ..Default::default()
            },
            ListenerOverrides {
                http_refusal_drain_timeout: Some(Duration::MAX),
                ..Default::default()
            },
            ListenerOverrides {
                tls_timeout: Some(Duration::MAX),
                ..Default::default()
            },
            ListenerOverrides {
                accept_retry_delay: Some(Duration::ZERO),
                ..Default::default()
            },
            ListenerOverrides {
                tcp_keepalive_interval: Some(Duration::from_millis(1)),
                ..Default::default()
            },
            ListenerOverrides {
                grpc_header_bytes: Some(ByteUnit::Byte(u64::MAX)),
                ..Default::default()
            },
        ] {
            assert!(ListenerSettings::resolve(inputs).is_err(), "{inputs:?}");
        }
        let settings = ListenerSettings::default();
        assert!(settings.validate_intake(ByteUnit::Mebibyte(128)).is_ok());
        assert!(settings.validate_intake(ByteUnit::Mebibyte(64)).is_err());
    }

    /// The declared defaults are the values hyper 1, h2 0.4 and tungstenite 0.24 applied while the
    /// listeners left them to the libraries, so a node that sets none of them behaves as before.
    #[test]
    fn declared_listener_defaults_keep_the_values_the_libraries_applied() {
        let settings = ListenerSettings::default();
        assert_eq!(settings.max_headers(), 100);
        assert_eq!(settings.grpc_stream_window_bytes(), ByteUnit::Mebibyte(1));
        assert_eq!(
            settings.grpc_connection_window_bytes(),
            ByteUnit::Mebibyte(1)
        );
        assert_eq!(settings.grpc_send_buffer_bytes(), ByteUnit::Kibibyte(400));
        assert_eq!(settings.grpc_max_pending_reset_streams(), 20);
        assert_eq!(settings.grpc_max_error_reset_streams(), 1024);
        assert_eq!(
            settings.websocket_write_buffer_bytes(),
            ByteUnit::Kibibyte(128)
        );
    }

    #[test]
    fn grpc_windows_are_windows_http2_can_advertise_and_that_bound_what_they_declare() {
        let stream = |bytes| ListenerOverrides {
            grpc_stream_window_bytes: Some(ByteUnit::Byte(bytes)),
            ..Default::default()
        };
        let connection = |bytes| ListenerOverrides {
            grpc_connection_window_bytes: Some(ByteUnit::Byte(bytes)),
            ..Default::default()
        };
        for accepted in [
            stream(1),
            stream(MAX_HTTP2_WINDOW_BYTES),
            connection(HTTP2_INITIAL_CONNECTION_WINDOW_BYTES),
            connection(MAX_HTTP2_WINDOW_BYTES),
        ] {
            assert!(ListenerSettings::resolve(accepted).is_ok(), "{accepted:?}");
        }
        let above = MAX_HTTP2_WINDOW_BYTES
            .checked_add(1)
            .assured("one byte above HTTP/2's largest window fits u64");
        let below_initial = HTTP2_INITIAL_CONNECTION_WINDOW_BYTES
            .checked_sub(1)
            .assured("RFC 9113's initial window is positive");
        for (refused, name, minimum) in [
            (stream(0), GrpcStreamWindowBytes::NAME, 1),
            (stream(above), GrpcStreamWindowBytes::NAME, 1),
            (
                connection(below_initial),
                GrpcConnectionWindowBytes::NAME,
                HTTP2_INITIAL_CONNECTION_WINDOW_BYTES,
            ),
            (
                connection(above),
                GrpcConnectionWindowBytes::NAME,
                HTTP2_INITIAL_CONNECTION_WINDOW_BYTES,
            ),
        ] {
            let error = ListenerSettings::resolve(refused)
                .expect_err("a window HTTP/2 cannot advertise, or that bounds nothing, is refused");
            assert_eq!(
                error.current_context(),
                &ListenerSettingsError::Http2Window {
                    name,
                    minimum,
                    maximum: MAX_HTTP2_WINDOW_BYTES,
                }
            );
        }
    }

    #[test]
    fn the_header_count_fits_the_header_lines_the_head_bound_holds() {
        let head = |header_bytes, max_headers| ListenerOverrides {
            header_bytes: Some(ByteUnit::Byte(header_bytes)),
            max_headers,
            ..Default::default()
        };
        // A head bound too small for 100 three-byte lines defaults to the lines it holds, and one
        // too small for any line still resolves.
        let small = ListenerSettings::resolve(head(150, None))
            .assured("a 150-byte head bound resolves its derived header count");
        assert_eq!(small.max_headers(), 50);
        let tiny = ListenerSettings::resolve(head(2, None))
            .assured("a head bound too small for any line still resolves");
        assert_eq!(tiny.max_headers(), 1);
        let at_limit = ListenerSettings::resolve(head(16384, Some(5461)))
            .assured("16 KiB holds 5461 three-byte header lines");
        assert_eq!(at_limit.max_headers(), 5461);
        let error = ListenerSettings::resolve(head(16384, Some(5462)))
            .expect_err("a count the head bound cannot reach is refused before I/O");
        assert_eq!(
            error.current_context(),
            &ListenerSettingsError::HeadersBeyondHeaderBytes {
                max_headers: 5462,
                header_bytes: 16384,
                limit: 5461,
            }
        );
        assert!(ListenerSettings::resolve(head(16384, Some(0))).is_err());
    }

    #[test]
    fn a_websocket_write_capacity_holds_one_more_largest_message_and_its_frame_header() {
        let settings = ListenerSettings::resolve(ListenerOverrides {
            websocket_write_buffer_bytes: Some(ByteUnit::Kibibyte(1)),
            websocket_message_bytes: Some(ByteUnit::Kibibyte(8)),
            ..Default::default()
        })
        .assured("a 1 KiB write buffer and an 8 KiB message bound are valid");
        assert_eq!(
            settings.endpoint_websocket_write_capacity(),
            1024 + 8192 + 14
        );
        assert_eq!(
            settings.websocket_write_capacity(4096),
            Some(1024 + 4096 + 14)
        );
        assert_eq!(settings.websocket_write_capacity(usize::MAX), None);

        let largest = u64::try_from(usize::MAX).assured("usize fits u64 on supported targets");
        let error = ListenerSettings::resolve(ListenerOverrides {
            websocket_write_buffer_bytes: Some(ByteUnit::Byte(largest)),
            ..Default::default()
        })
        .expect_err("a write buffer whose capacity overflows the address space is refused");
        assert_eq!(
            error.current_context(),
            &ListenerSettingsError::WebsocketWriteCapacity {
                write_buffer: largest
            }
        );
    }

    #[test]
    fn grpc_send_buffers_and_reset_counts_fit_the_http2_implementation() {
        let send_buffer = |bytes| ListenerOverrides {
            grpc_send_buffer_bytes: Some(ByteUnit::Byte(bytes)),
            ..Default::default()
        };
        assert!(ListenerSettings::resolve(send_buffer(u64::from(u32::MAX))).is_ok());
        let above = u64::from(u32::MAX)
            .checked_add(1)
            .assured("one byte above u32::MAX fits u64");
        for refused in [
            send_buffer(0),
            send_buffer(above),
            ListenerOverrides {
                grpc_max_pending_reset_streams: Some(0),
                ..Default::default()
            },
            ListenerOverrides {
                grpc_max_error_reset_streams: Some(0),
                ..Default::default()
            },
            ListenerOverrides {
                websocket_write_buffer_bytes: Some(ByteUnit::Byte(0)),
                ..Default::default()
            },
        ] {
            assert!(ListenerSettings::resolve(refused).is_err(), "{refused:?}");
        }
    }
}
