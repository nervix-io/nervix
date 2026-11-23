//! Declared transport capacity and deadline settings.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** One declaration, default and validation for each local transport policy input.
//!   Connections and request reserves follow the peers the transport has, so no input counts
//!   peers.
//! - **Depends on.** Vocabulary setting classes, primitive admission and transport pool geometry.
//! - **Must not know.** Command-line parsing, membership admission, graphs or placement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns local transport configuration")
)]

use std::time::Duration;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use nervix_primitives::{sync::Semaphore, time::Instant};
use thiserror::Error;
use ubyte::ByteUnit;

use crate::{TransportEntropy, TransportOptions};

/// A local claim that the transport cannot enforce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TransportSettingsError {
    #[error("interconnect-reconnect-jitter-permille ({0}) must be at most 1000")]
    Jitter(u16),
    #[error("{setting} ({value}) must be between 1 and {maximum}")]
    Capacity {
        setting: &'static str,
        value: usize,
        maximum: usize,
    },
    #[error("{setting} ({value:?}) must be positive and fit a monotonic deadline")]
    Deadline {
        setting: &'static str,
        value: Duration,
    },
    #[error(
        "{setting} ({value}) must be positive and fit HTTP/2's {maximum}-byte flow-control window"
    )]
    Window {
        setting: &'static str,
        value: ByteUnit,
        maximum: u64,
    },
    #[error(
        "interconnect-max-reconnect-backoff ({maximum:?}) must be at least \
         interconnect-reconnect-backoff ({initial:?})"
    )]
    Backoff {
        initial: Duration,
        maximum: Duration,
    },
}

/// The node's `interconnect-concurrent-handshakes` capacity.
pub struct ConcurrentHandshakes;
impl SettingDefinition for ConcurrentHandshakes {
    type Class = NodeCapabilities;
    type Value = usize;
    type Context = ();
    type Error = TransportSettingsError;
    const NAME: &'static str = "interconnect-concurrent-handshakes";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> usize {
        32
    }
    fn validate(value: &usize, _: &()) -> Result<(), Report<Self::Error>> {
        TransportSettings::validate_capacity(Self::NAME, *value)
    }
}

/// The node's `interconnect-incoming-queue-capacity` capacity.
pub struct IncomingQueueCapacity;
impl SettingDefinition for IncomingQueueCapacity {
    type Class = NodeCapabilities;
    type Value = usize;
    type Context = ();
    type Error = TransportSettingsError;
    const NAME: &'static str = "interconnect-incoming-queue-capacity";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> usize {
        1024
    }
    fn validate(value: &usize, _: &()) -> Result<(), Report<Self::Error>> {
        TransportSettings::validate_capacity(Self::NAME, *value)
    }
}

/// The node's `interconnect-connection-setup-timeout` budget.
pub struct ConnectionSetupTimeout;
impl SettingDefinition for ConnectionSetupTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = TransportSettingsError;
    const NAME: &'static str = "interconnect-connection-setup-timeout";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(5)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        TransportSettings::validate_deadline(Self::NAME, *value)
    }
}

/// The node's `interconnect-request-timeout` budget.
pub struct RequestTimeout;
impl SettingDefinition for RequestTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = TransportSettingsError;
    const NAME: &'static str = "interconnect-request-timeout";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(10)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        TransportSettings::validate_deadline(Self::NAME, *value)
    }
}

/// The node's `interconnect-progress-timeout` budget.
pub struct ProgressTimeout;
impl SettingDefinition for ProgressTimeout {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = TransportSettingsError;
    const NAME: &'static str = "interconnect-progress-timeout";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(5)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        TransportSettings::validate_deadline(Self::NAME, *value)
    }
}

/// The node's `interconnect-reconnect-backoff` budget.
pub struct ReconnectBackoff;
impl SettingDefinition for ReconnectBackoff {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = TransportSettingsError;
    const NAME: &'static str = "interconnect-reconnect-backoff";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_millis(200)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        TransportSettings::validate_deadline(Self::NAME, *value)
    }
}

/// The node's `interconnect-max-reconnect-backoff` budget.
pub struct MaxReconnectBackoff;
impl SettingDefinition for MaxReconnectBackoff {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = ();
    type Error = TransportSettingsError;
    const NAME: &'static str = "interconnect-max-reconnect-backoff";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> Duration {
        Duration::from_secs(5)
    }
    fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
        TransportSettings::validate_deadline(Self::NAME, *value)
    }
}

/// The HTTP/2 receive credit this node advertises.
pub struct StreamReceiveWindow;
impl SettingDefinition for StreamReceiveWindow {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = TransportSettingsError;
    const NAME: &'static str = "interconnect-stream-receive-window";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Kibibyte(64)
    }
    fn validate(value: &ByteUnit, _: &()) -> Result<(), Report<Self::Error>> {
        if value.as_u64() == 0 || value.as_u64() > super::MAX_HTTP2_WINDOW_BYTES {
            return Err(Report::new(TransportSettingsError::Window {
                setting: Self::NAME,
                value: *value,
                maximum: super::MAX_HTTP2_WINDOW_BYTES,
            }));
        }
        Ok(())
    }
}

/// The HTTP/2 receive credit this node advertises.
pub struct ConnectionReceiveWindow;
impl SettingDefinition for ConnectionReceiveWindow {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ();
    type Error = TransportSettingsError;
    const NAME: &'static str = "interconnect-connection-receive-window";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> ByteUnit {
        ByteUnit::Kibibyte(256)
    }
    fn validate(value: &ByteUnit, _: &()) -> Result<(), Report<Self::Error>> {
        if value.as_u64() == 0 || value.as_u64() > super::MAX_HTTP2_WINDOW_BYTES {
            return Err(Report::new(TransportSettingsError::Window {
                setting: Self::NAME,
                value: *value,
                maximum: super::MAX_HTTP2_WINDOW_BYTES,
            }));
        }
        Ok(())
    }
}

/// The fraction of a reconnect delay varied downward, in thousandths; zero preserves fixed delays.
pub struct ReconnectJitterPermille;
impl SettingDefinition for ReconnectJitterPermille {
    type Class = NodeCapabilities;
    type Value = u16;
    type Context = ();
    type Error = TransportSettingsError;
    const NAME: &'static str = "interconnect-reconnect-jitter-permille";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> u16 {
        0
    }
    fn validate(value: &u16, _: &()) -> Result<(), Report<Self::Error>> {
        if *value > 1000 {
            return Err(Report::new(TransportSettingsError::Jitter(*value)));
        }
        Ok(())
    }
}

/// Explicit local inputs; omitted derived values remain omitted at the edge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportSettingValues {
    pub reconnect_jitter_permille: Option<u16>,
    pub concurrent_handshakes: Option<usize>,
    pub incoming_queue_capacity: Option<usize>,
    pub stream_receive_window: Option<ByteUnit>,
    pub connection_receive_window: Option<ByteUnit>,
    pub connection_setup_timeout: Option<Duration>,
    pub request_timeout: Option<Duration>,
    pub progress_timeout: Option<Duration>,
    pub reconnect_backoff: Option<Duration>,
    pub max_reconnect_backoff: Option<Duration>,
}

/// Local capacity declarations. Engines receive their resolved projection once at construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportSettings {
    reconnect_jitter_permille: Setting<ReconnectJitterPermille>,
    concurrent_handshakes: Setting<ConcurrentHandshakes>,
    incoming_queue_capacity: Setting<IncomingQueueCapacity>,
    stream_receive_window: Setting<StreamReceiveWindow>,
    connection_receive_window: Setting<ConnectionReceiveWindow>,
    connection_setup_timeout: Setting<ConnectionSetupTimeout>,
    request_timeout: Setting<RequestTimeout>,
    progress_timeout: Setting<ProgressTimeout>,
    reconnect_backoff: Setting<ReconnectBackoff>,
    max_reconnect_backoff: Setting<MaxReconnectBackoff>,
}

impl TransportSettings {
    pub fn resolve(values: TransportSettingValues) -> Result<Self, Report<TransportSettingsError>> {
        let settings = Self {
            reconnect_jitter_permille: Setting::resolve(values.reconnect_jitter_permille, &())?,
            concurrent_handshakes: Setting::resolve(values.concurrent_handshakes, &())?,
            incoming_queue_capacity: Setting::resolve(values.incoming_queue_capacity, &())?,
            stream_receive_window: Setting::resolve(values.stream_receive_window, &())?,
            connection_receive_window: Setting::resolve(values.connection_receive_window, &())?,
            connection_setup_timeout: Setting::resolve(values.connection_setup_timeout, &())?,
            request_timeout: Setting::resolve(values.request_timeout, &())?,
            progress_timeout: Setting::resolve(values.progress_timeout, &())?,
            reconnect_backoff: Setting::resolve(values.reconnect_backoff, &())?,
            max_reconnect_backoff: Setting::resolve(values.max_reconnect_backoff, &())?,
        };
        if settings.max_reconnect_backoff.value() < settings.reconnect_backoff.value() {
            return Err(Report::new(TransportSettingsError::Backoff {
                initial: *settings.reconnect_backoff.value(),
                maximum: *settings.max_reconnect_backoff.value(),
            }));
        }
        Ok(settings)
    }

    fn validate_capacity(
        setting: &'static str,
        value: usize,
    ) -> Result<(), Report<TransportSettingsError>> {
        let maximum = Semaphore::MAX_PERMITS;
        if value == 0 || value > maximum {
            return Err(Report::new(TransportSettingsError::Capacity {
                setting,
                value,
                maximum,
            }));
        }
        Ok(())
    }

    pub(crate) fn validate_deadline(
        setting: &'static str,
        value: Duration,
    ) -> Result<(), Report<TransportSettingsError>> {
        let largest = value.checked_mul(12);
        let fits = match largest {
            Some(largest) => Instant::now().checked_add(largest).is_some(),
            None => false,
        };
        if value.is_zero() || !fits {
            return Err(Report::new(TransportSettingsError::Deadline {
                setting,
                value,
            }));
        }
        Ok(())
    }

    pub fn request_timeout(&self) -> Duration {
        *self.request_timeout.value()
    }

    /// Every resolved value, as explicit inputs that resolve to these settings again.
    pub fn values(&self) -> TransportSettingValues {
        TransportSettingValues {
            reconnect_jitter_permille: Some(*self.reconnect_jitter_permille.value()),
            concurrent_handshakes: Some(*self.concurrent_handshakes.value()),
            incoming_queue_capacity: Some(*self.incoming_queue_capacity.value()),
            stream_receive_window: Some(*self.stream_receive_window.value()),
            connection_receive_window: Some(*self.connection_receive_window.value()),
            connection_setup_timeout: Some(*self.connection_setup_timeout.value()),
            request_timeout: Some(*self.request_timeout.value()),
            progress_timeout: Some(*self.progress_timeout.value()),
            reconnect_backoff: Some(*self.reconnect_backoff.value()),
            max_reconnect_backoff: Some(*self.max_reconnect_backoff.value()),
        }
    }

    /// The same validated values used by construction, retained in this run's committed claim.
    pub fn capabilities(&self) -> nervix_models::InterconnectCapabilities {
        use std::num::NonZeroU64;
        let positive = |value: usize| {
            NonZeroU64::new(
                u64::try_from(value).assured("supported targets have at most 64-bit capacities"),
            )
            .verified("transport settings validated every derived capacity as positive")
        };
        nervix_models::InterconnectCapabilities {
            reconnect_jitter_permille: *self.reconnect_jitter_permille.value(),
            concurrent_handshakes: positive(*self.concurrent_handshakes.value()),
            incoming_queue_capacity: positive(*self.incoming_queue_capacity.value()),
            stream_window_bytes: u32::try_from(self.stream_receive_window.value().as_u64())
                .verified("validated HTTP/2 window"),
            connection_window_bytes: u32::try_from(self.connection_receive_window.value().as_u64())
                .verified("validated HTTP/2 window"),
            connection_setup_timeout: *self.connection_setup_timeout.value(),
            request_timeout: *self.request_timeout.value(),
            progress_timeout: *self.progress_timeout.value(),
            reconnect_backoff: *self.reconnect_backoff.value(),
            max_reconnect_backoff: *self.max_reconnect_backoff.value(),
        }
    }

    pub fn options(
        &self,
        peer_receive: Option<super::PeerReceiveSettings>,
        drain_timeout: Option<Duration>,
    ) -> TransportOptions {
        let request_timeout = *self.request_timeout.value();
        let shutdown_drain_timeout = match drain_timeout {
            Some(bound) => bound.min(request_timeout),
            None => request_timeout,
        };
        TransportOptions {
            accept_retry_delay: Setting::<crate::AcceptRetryDelay>::resolve(None, &())
                .assured("the declared accept-retry default is positive and clock-representable"),
            reconnect_jitter_permille: *self.reconnect_jitter_permille.value(),
            peer_receive,
            max_concurrent_handshakes: *self.concurrent_handshakes.value(),
            incoming_queue_capacity: *self.incoming_queue_capacity.value(),
            initial_stream_window_bytes: u32::try_from(self.stream_receive_window.value().as_u64())
                .verified("the receive window fits HTTP/2"),
            initial_connection_window_bytes: u32::try_from(
                self.connection_receive_window.value().as_u64(),
            )
            .verified("the receive window fits HTTP/2"),
            max_header_bytes: crate::DEFAULT_MAX_HEADER_BYTES,
            connection_setup_timeout: *self.connection_setup_timeout.value(),
            request_timeout,
            progress_timeout: *self.progress_timeout.value(),
            reconnect_backoff: *self.reconnect_backoff.value(),
            max_reconnect_backoff: *self.max_reconnect_backoff.value(),
            shutdown_drain_timeout,
            entropy: TransportEntropy::operating_system(),
        }
    }
}

impl Default for TransportSettings {
    fn default() -> Self {
        Self::resolve(TransportSettingValues::default())
            .assured("the declared transport defaults satisfy their relationships")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_inside_its_declared_fraction_and_disabled_jitter_reads_no_entropy() {
        let mut options = TransportOptions {
            entropy: TransportEntropy::from_source(|| {
                panic!("disabled jitter must preserve the entropy sequence")
            }),
            ..Default::default()
        };
        assert_eq!(
            options.retry_delay(Duration::from_secs(1)),
            Duration::from_secs(1)
        );
        options.reconnect_jitter_permille = 200;
        options.entropy = TransportEntropy::from_source(|| 200);
        assert_eq!(
            options.retry_delay(Duration::from_secs(1)),
            Duration::from_millis(800)
        );
        assert!(
            TransportSettings::resolve(TransportSettingValues {
                reconnect_jitter_permille: Some(1001),
                ..Default::default()
            })
            .is_err()
        );
    }

    #[test]
    fn unrepresentable_and_inconsistent_inputs_are_refused_before_io() {
        for values in [
            TransportSettingValues {
                incoming_queue_capacity: Some(0),
                ..Default::default()
            },
            TransportSettingValues {
                stream_receive_window: Some(ByteUnit::Byte(0)),
                ..Default::default()
            },
            TransportSettingValues {
                connection_receive_window: Some(ByteUnit::Gibibyte(2)),
                ..Default::default()
            },
            TransportSettingValues {
                request_timeout: Some(Duration::ZERO),
                ..Default::default()
            },
            TransportSettingValues {
                reconnect_backoff: Some(Duration::from_secs(6)),
                ..Default::default()
            },
        ] {
            assert!(TransportSettings::resolve(values).is_err(), "{values:?}");
        }
    }
}
