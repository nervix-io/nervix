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
use nervix_models::{ListenerBounds, NodeCapabilities, Setting, SettingDefinition};
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

/// Validated settings, resolved once before listeners bind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListenerSettings {
    pub max_connections: Setting<MaxConnections>,
    pub header_bytes: Setting<HeaderBytes>,
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
    pub grpc_header_bytes: Setting<GrpcHeaderBytes>,
    pub grpc_concurrency: Setting<GrpcConcurrency>,
    pub grpc_request_timeout: Setting<GrpcRequestTimeout>,
    pub grpc_max_streams: Setting<GrpcMaxStreams>,
    pub grpc_keepalive_interval: Setting<GrpcKeepaliveInterval>,
    pub grpc_keepalive_timeout: Setting<GrpcKeepaliveTimeout>,
    pub tcp_keepalive_time: Setting<TcpKeepaliveTime>,
    pub tcp_keepalive_interval: Setting<TcpKeepaliveInterval>,
    pub tcp_keepalive_retries: Setting<TcpKeepaliveRetries>,
}

/// Optional inputs read before resolution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListenerOverrides {
    pub max_connections: Option<u32>,
    pub header_bytes: Option<ByteUnit>,
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
    pub grpc_header_bytes: Option<ByteUnit>,
    pub grpc_concurrency: Option<u32>,
    pub grpc_request_timeout: Option<Duration>,
    pub grpc_max_streams: Option<u32>,
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
        let result = Self {
            max_connections: Setting::<MaxConnections>::resolve(inputs.max_connections, &())?,
            header_bytes: Setting::<HeaderBytes>::resolve(inputs.header_bytes, &())?,
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
            grpc_header_bytes: Setting::<GrpcHeaderBytes>::resolve(inputs.grpc_header_bytes, &())?,
            grpc_concurrency: Setting::<GrpcConcurrency>::resolve(inputs.grpc_concurrency, &())?,
            grpc_request_timeout: Setting::<GrpcRequestTimeout>::resolve(
                inputs.grpc_request_timeout,
                &(),
            )?,
            grpc_max_streams: Setting::<GrpcMaxStreams>::resolve(inputs.grpc_max_streams, &())?,
            grpc_keepalive_interval: Setting::<GrpcKeepaliveInterval>::resolve(
                inputs.grpc_keepalive_interval,
                &(),
            )?,
            grpc_keepalive_timeout: Setting::<GrpcKeepaliveTimeout>::resolve(
                inputs.grpc_keepalive_timeout,
                &(),
            )?,
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
        Ok(result)
    }
    pub fn max_connections(&self) -> u32 {
        *self.max_connections.value()
    }
    pub fn header_bytes(&self) -> ByteUnit {
        *self.header_bytes.value()
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
    pub fn grpc_header_bytes(&self) -> ByteUnit {
        *self.grpc_header_bytes.value()
    }
    pub fn grpc_concurrency(&self) -> u32 {
        *self.grpc_concurrency.value()
    }
    pub fn grpc_request_timeout(&self) -> Duration {
        *self.grpc_request_timeout.value()
    }
    pub fn grpc_max_streams(&self) -> u32 {
        *self.grpc_max_streams.value()
    }
    pub fn grpc_keepalive_interval(&self) -> Duration {
        *self.grpc_keepalive_interval.value()
    }
    pub fn grpc_keepalive_timeout(&self) -> Duration {
        *self.grpc_keepalive_timeout.value()
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
            grpc_header_bytes: std::num::NonZeroU64::new(self.grpc_header_bytes().as_u64())
                .assured("the typed listener setting rejects zero"),
            grpc_concurrency: std::num::NonZeroU32::new(self.grpc_concurrency())
                .assured("the typed listener setting rejects zero"),
            grpc_request_timeout: self.grpc_request_timeout(),
            grpc_max_streams: std::num::NonZeroU32::new(self.grpc_max_streams())
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
}
