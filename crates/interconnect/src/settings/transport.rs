//! Declared transport capacity and deadline settings.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** One declaration, default and validation for each local transport policy input,
//!   and peer-relative connection, request, health and watermark capacities.
//! - **Depends on.** Vocabulary setting classes, primitive admission and transport pool geometry.
//! - **Must not know.** Command-line parsing, membership admission, graphs or placement.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns local transport configuration")
)]

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use nervix_primitives::{sync::Semaphore, time::Instant};
use thiserror::Error;
use ubyte::ByteUnit;

use crate::{PoolClass, TransportEntropy, TransportOptions};

/// A local claim that the transport cannot enforce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TransportSettingsError {
    #[error("interconnect-reconnect-jitter-permille ({0}) must be at most 1000")]
    Jitter(u16),
    #[error("invalid peer capacity")]
    PeerCapacity,
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

pub use nervix_models::MaxPeers;

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

/// The validated inputs of a peer-relative request capacity.
#[derive(Debug, Clone, Copy)]
pub struct RequestQuotaContext {
    pub max_peers: usize,
    pub incoming_capacity: usize,
}

/// Reserved requests of each ordinary class, independently in each direction.
pub struct RequestQuota;
impl SettingDefinition for RequestQuota {
    type Class = NodeCapabilities;
    type Value = Option<usize>;
    type Context = RequestQuotaContext;
    type Error = TransportSettingsError;
    const NAME: &'static str = "interconnect-request-quota";
    const DERIVATION: Option<&'static str> =
        Some("two requests per peer, bounded by the incoming queue");
    fn default_value(_: &RequestQuotaContext) -> Option<usize> {
        None
    }
    fn validate(
        value: &Option<usize>,
        context: &RequestQuotaContext,
    ) -> Result<(), Report<Self::Error>> {
        if let Some(value) = value
            && (*value == 0 || *value > context.incoming_capacity)
        {
            return Err(Report::new(TransportSettingsError::Capacity {
                setting: Self::NAME,
                value: *value,
                maximum: context.incoming_capacity,
            }));
        }
        Ok(())
    }
}

/// Explicit local inputs; omitted derived values remain omitted at the edge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportSettingValues {
    pub reconnect_jitter_permille: Option<u16>,
    pub max_peers: Option<usize>,
    pub concurrent_handshakes: Option<usize>,
    pub incoming_queue_capacity: Option<usize>,
    pub request_quota: Option<usize>,
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
    max_peers: Setting<MaxPeers>,
    concurrent_handshakes: Setting<ConcurrentHandshakes>,
    incoming_queue_capacity: Setting<IncomingQueueCapacity>,
    request_quota: Setting<RequestQuota>,
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
        let max_peers = Setting::<MaxPeers>::resolve(values.max_peers, &())
            .change_context(TransportSettingsError::PeerCapacity)?;
        let incoming_queue_capacity =
            Setting::<IncomingQueueCapacity>::resolve(values.incoming_queue_capacity, &())?;
        let context = RequestQuotaContext {
            max_peers: *max_peers.value(),
            incoming_capacity: *incoming_queue_capacity.value(),
        };
        let settings = Self {
            reconnect_jitter_permille: Setting::resolve(values.reconnect_jitter_permille, &())?,
            max_peers,
            concurrent_handshakes: Setting::resolve(values.concurrent_handshakes, &())?,
            incoming_queue_capacity,
            request_quota: Setting::new(values.request_quota, &context)?,
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
        settings.validate_derived_capacities()?;
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

    fn connection_slots_per_peer() -> usize {
        let mut slots = 0usize;
        for class in PoolClass::ALL {
            slots = slots
                .checked_add(class.connections_per_peer())
                .assured("the finite set of pool slots fits usize");
        }
        slots
            .checked_mul(2)
            .assured("the finite inbound and outbound pool geometry fits usize")
    }

    fn validate_derived_capacities(&self) -> Result<(), Report<TransportSettingsError>> {
        let maximum = Semaphore::MAX_PERMITS / Self::connection_slots_per_peer();
        if *self.max_peers.value() > maximum {
            return Err(Report::new(TransportSettingsError::Capacity {
                setting: MaxPeers::NAME,
                value: *self.max_peers.value(),
                maximum,
            }));
        }
        if self
            .incoming_queue_capacity
            .value()
            .checked_mul(8)
            .is_none()
        {
            return Err(Report::new(TransportSettingsError::Capacity {
                setting: IncomingQueueCapacity::NAME,
                value: *self.incoming_queue_capacity.value(),
                maximum: usize::MAX / 8,
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

    /// Both directions of every pool, including the on-demand bulk pool.
    pub fn max_connections(&self) -> usize {
        self.max_peers
            .value()
            .checked_mul(Self::connection_slots_per_peer())
            .verified("resolve validated capacity for every inbound and outbound pool")
    }

    pub fn health_probes(&self) -> usize {
        *self.max_peers.value()
    }

    pub fn request_timeout(&self) -> Duration {
        *self.request_timeout.value()
    }

    pub fn max_peers(&self) -> usize {
        *self.max_peers.value()
    }

    pub fn request_quota(&self) -> usize {
        match self.request_quota.value() {
            Some(value) => *value,
            None => self.derived_request_quota(),
        }
    }

    /// Preserve omission of a peer-derived quota when command-line overrides change its inputs.
    pub fn values(&self) -> TransportSettingValues {
        TransportSettingValues {
            reconnect_jitter_permille: Some(*self.reconnect_jitter_permille.value()),
            max_peers: Some(*self.max_peers.value()),
            concurrent_handshakes: Some(*self.concurrent_handshakes.value()),
            incoming_queue_capacity: Some(*self.incoming_queue_capacity.value()),
            request_quota: *self.request_quota.value(),
            stream_receive_window: Some(*self.stream_receive_window.value()),
            connection_receive_window: Some(*self.connection_receive_window.value()),
            connection_setup_timeout: Some(*self.connection_setup_timeout.value()),
            request_timeout: Some(*self.request_timeout.value()),
            progress_timeout: Some(*self.progress_timeout.value()),
            reconnect_backoff: Some(*self.reconnect_backoff.value()),
            max_reconnect_backoff: Some(*self.max_reconnect_backoff.value()),
        }
    }

    pub fn watermark_capacity(&self) -> usize {
        self.incoming_queue_capacity.value().checked_mul(8).assured(
            "the incoming capacity validation leaves room for eight retained watermarks per \
             pending request",
        )
    }

    /// A normal append stream and its replacement for every peer, inside node admission.
    fn derived_request_quota(&self) -> usize {
        self.max_peers
            .value()
            .checked_mul(2)
            .assured("validated peer capacity leaves room for stream replacement")
            .min(*self.incoming_queue_capacity.value())
    }

    /// Completion capacity scales with the peers whose outstanding work it resolves.
    pub fn completion_requests(&self) -> usize {
        (*self.max_peers.value() / 16)
            .max(1)
            .min(*self.incoming_queue_capacity.value())
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
            max_peers: positive(self.max_peers()),
            max_connections: positive(self.max_connections()),
            concurrent_handshakes: positive(*self.concurrent_handshakes.value()),
            incoming_queue_capacity: positive(*self.incoming_queue_capacity.value()),
            request_quota: positive(self.request_quota()),
            completion_quota: positive(self.completion_requests()),
            health_probes: positive(self.health_probes()),
            relay_watermarks: positive(self.watermark_capacity()),
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
            relay_retention: super::RelayRetentionSettings::default(),
            reconnect_jitter_permille: *self.reconnect_jitter_permille.value(),
            peer_receive,
            max_peers: *self.max_peers.value(),
            max_connections: self.max_connections(),
            max_concurrent_handshakes: *self.concurrent_handshakes.value(),
            incoming_queue_capacity: *self.incoming_queue_capacity.value(),
            relay_channel_watermark_capacity: self.watermark_capacity(),
            request_quota: self.request_quota(),
            append_request_quota: self.request_quota(),
            completion_request_quota: self.completion_requests(),
            max_concurrent_health_probes: self.health_probes(),
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
        let mut options = TransportOptions::default();
        options.entropy = TransportEntropy::from_source(|| {
            panic!("disabled jitter must preserve the entropy sequence")
        });
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
    fn declared_peers_scale_independent_reserves_without_materializing_derived_inputs() {
        let settings = TransportSettings::resolve(TransportSettingValues {
            max_peers: Some(10),
            ..Default::default()
        })
        .assured("ten peers fit every capacity");
        let options = settings.options(None, None);
        assert_eq!(options.max_connections, 120);
        assert_eq!(options.request_quota, 20);
        assert_eq!(options.append_request_quota, 20);
        assert_eq!(options.max_concurrent_health_probes, 10);
        assert_eq!(settings.values().request_quota, None);
        let explicit = TransportSettings::resolve(TransportSettingValues {
            max_peers: Some(16),
            request_quota: Some(4),
            ..Default::default()
        })
        .assured("a four-request reserve fits the incoming queue");
        assert_eq!(explicit.options(None, None).append_request_quota, 4);
        assert_eq!(explicit.values().request_quota, Some(4));
    }

    #[test]
    fn unrepresentable_and_inconsistent_inputs_are_refused_before_io() {
        for values in [
            TransportSettingValues {
                max_peers: Some(0),
                ..Default::default()
            },
            TransportSettingValues {
                max_peers: Some(usize::MAX),
                ..Default::default()
            },
            TransportSettingValues {
                incoming_queue_capacity: Some(3),
                request_quota: Some(4),
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
