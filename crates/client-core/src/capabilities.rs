//! Local connection, transport, retry and retention capabilities of a native client.
//!
//! Layer: edges.
//! - **Owns.** Client setting definitions, validated values, the HTTP/2 window bounds they are
//!   checked against and derived subscription queue bounds.
//! - **Depends on.** Vocabulary setting classes, session frame bounds and native channel bounds.
//! - **Must not know.** Cluster policy, server configuration or how a caller presents options.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns native client capabilities")
)]

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use nervix_primitives::sync::Semaphore;

use crate::{ClientError, ConnectOptions, SubscriptionEvent, wire::SessionLimits};

macro_rules! deadline {
    ($definition:ident, $name:literal, $field:literal, $default:expr) => {
        pub struct $definition;
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = Duration;
            type Context = ();
            type Error = ClientError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;
            fn default_value(_: &()) -> Duration {
                $default
            }
            fn validate(value: &Duration, _: &()) -> Result<(), Report<ClientError>> {
                // The native client's deadline contract is one millisecond through one day.
                if *value < Duration::from_millis(1) || *value > Duration::from_secs(24 * 60 * 60) {
                    return Err(Report::new(ClientError::InvalidDeadline { field: $field }));
                }
                Ok(())
            }
        }
    };
}

/// The largest flow-control window HTTP/2 allows.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 9113 section 6.9.1 caps a flow-control window at 2^31-1 octets"
    )
)]
pub const LARGEST_RECEIVE_WINDOW_BYTES: u32 = i32::MAX.unsigned_abs();

/// The window every HTTP/2 connection starts with. A receiver can widen it but never narrow it,
/// so a smaller connection window would not bound what the node sends first.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "RFC 9113 section 6.9.2 opens every connection with a 65,535-octet window, which \
                  WINDOW_UPDATE can only widen"
    )
)]
pub const INITIAL_CONNECTION_WINDOW_BYTES: u32 = 65_535;

macro_rules! receive_window {
    ($definition:ident, $name:literal, $smallest:expr, $default:expr) => {
        pub struct $definition;
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = NonZeroU32;
            type Context = ();
            type Error = ClientError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;
            fn default_value(_: &()) -> NonZeroU32 {
                NonZeroU32::new($default).assured("declared window defaults are positive")
            }
            fn validate(value: &NonZeroU32, _: &()) -> Result<(), Report<ClientError>> {
                if value.get() < $smallest || value.get() > LARGEST_RECEIVE_WINDOW_BYTES {
                    return Err(Report::new(ClientError::InvalidReceiveWindow {
                        field: Self::NAME,
                        bytes: value.get(),
                        smallest: $smallest,
                    }));
                }
                Ok(())
            }
        }
    };
}

macro_rules! capacity {
    ($definition:ident, $name:literal, $default:literal) => {
        pub struct $definition;
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = NonZeroUsize;
            type Context = ();
            type Error = ClientError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;
            fn default_value(_: &()) -> NonZeroUsize {
                NonZeroUsize::new($default).assured("declared queue defaults are positive")
            }
            fn validate(value: &NonZeroUsize, _: &()) -> Result<(), Report<ClientError>> {
                if value.get() > Semaphore::MAX_PERMITS {
                    return Err(Report::new(ClientError::InvalidFrameCapacity {
                        field: Self::NAME,
                        capacity: value.get(),
                    }));
                }
                Ok(())
            }
        }
    };
}

deadline!(
    ClientConnectTimeout,
    "connect-timeout",
    "connect_timeout",
    Duration::from_secs(10)
);
deadline!(
    ClientRequestTimeout,
    "request-timeout",
    "request_timeout",
    Duration::from_secs(120)
);
deadline!(
    ClientRetryTimeout,
    "retry-timeout",
    "retry_timeout",
    Duration::from_secs(120)
);
deadline!(
    ClientBackupWaitTimeout,
    "backup-wait-timeout",
    "backup_wait_timeout",
    Duration::from_secs(10 * 60)
);
deadline!(
    ClientRetryBackoff,
    "retry-backoff",
    "retry_backoff",
    Duration::from_millis(100)
);
deadline!(
    ClientRetryMaxBackoff,
    "retry-max-backoff",
    "retry_max_backoff",
    Duration::from_secs(1)
);
capacity!(
    ClientSubscriptionRecordCapacity,
    "subscription-record-capacity",
    32
);
capacity!(
    ClientSubscriptionRetainedFrames,
    "subscription-retained-frames",
    1
);
capacity!(
    ClientSubscriptionsRetainedInFull,
    "subscriptions-retained-in-full",
    4
);
capacity!(ClientUploadFrameCapacity, "upload-frame-capacity", 8);
capacity!(ClientRestoreFrameCapacity, "restore-frame-capacity", 8);
// While a call is open, a connection that receives no call data and no ping answer for the
// interval is pinged, and one whose ping is not answered within the timeout is lost: 30 s and 20 s,
// the cadence at which the server's gRPC listener pings its clients.
deadline!(
    ClientKeepaliveInterval,
    "keepalive-interval",
    "keepalive_interval",
    Duration::from_secs(30)
);
deadline!(
    ClientKeepaliveTimeout,
    "keepalive-timeout",
    "keepalive_timeout",
    Duration::from_secs(20)
);
// How many bytes the node may send before the client reads them: of one call for the stream
// window, and of every call on the connection together for the connection window. Both defaults
// are the windows hyper's HTTP/2 client advertised before they were declared.
receive_window!(
    ClientStreamWindowBytes,
    "stream-window-bytes",
    NonZeroU32::MIN.get(),
    2 * 1024 * 1024
);
receive_window!(
    ClientConnectionWindowBytes,
    "connection-window-bytes",
    INITIAL_CONNECTION_WINDOW_BYTES,
    5 * 1024 * 1024
);

macro_rules! capabilities {
    ($($field:ident: $definition:ident => $value:ty),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct ClientCapabilities { $($field: Setting<$definition>),+ }

        impl ClientCapabilities {
            pub fn from_options(options: &ConnectOptions) -> Result<Self, Report<ClientError>> {
                let settings = Self { $($field: Setting::new(options.$field, &())?),+ };
                settings.validate()?;
                Ok(settings)
            }
            $(pub fn $field(self) -> $value { *self.$field.value() })+
        }

        impl Default for ClientCapabilities {
            fn default() -> Self {
                let settings = Self { $($field: Setting::resolve(None, &()).assured("the declared client default validates")),+ };
                settings.validate().assured("default retry and retention relationships hold");
                settings
            }
        }
    };
}

capabilities! {
    connect_timeout: ClientConnectTimeout => Duration,
    request_timeout: ClientRequestTimeout => Duration,
    retry_timeout: ClientRetryTimeout => Duration,
    backup_wait_timeout: ClientBackupWaitTimeout => Duration,
    retry_backoff: ClientRetryBackoff => Duration,
    retry_max_backoff: ClientRetryMaxBackoff => Duration,
    subscription_record_capacity: ClientSubscriptionRecordCapacity => NonZeroUsize,
    subscription_retained_frames: ClientSubscriptionRetainedFrames => NonZeroUsize,
    subscriptions_retained_in_full: ClientSubscriptionsRetainedInFull => NonZeroUsize,
    upload_frame_capacity: ClientUploadFrameCapacity => NonZeroUsize,
    restore_frame_capacity: ClientRestoreFrameCapacity => NonZeroUsize,
    keepalive_interval: ClientKeepaliveInterval => Duration,
    keepalive_timeout: ClientKeepaliveTimeout => Duration,
    stream_window_bytes: ClientStreamWindowBytes => NonZeroU32,
    connection_window_bytes: ClientConnectionWindowBytes => NonZeroU32,
}

/// The complete record-count bounds used by one client's subscription dispatcher.
pub(crate) struct SubscriptionRecordLimits {
    pub(crate) records: usize,
    pub(crate) total_records: usize,
}

impl ClientCapabilities {
    fn validate(self) -> Result<(), Report<ClientError>> {
        if self.retry_backoff() > self.retry_max_backoff() {
            return Err(Report::new(ClientError::InvalidRetryBackoff));
        }
        self.checked_subscription_record_limits()?;
        self.subscription_byte_limits(&SessionLimits::DEFAULT)?;
        Ok(())
    }

    fn checked_subscription_record_limits(
        self,
    ) -> Result<SubscriptionRecordLimits, Report<ClientError>> {
        let records = self.subscription_record_capacity().get();
        let Some(total_records) = records.checked_mul(self.subscriptions_retained_in_full().get())
        else {
            return Err(Report::new(ClientError::InvalidSubscriptionRetention));
        };
        Ok(SubscriptionRecordLimits {
            records,
            total_records,
        })
    }

    pub(crate) fn subscription_record_limits(self) -> SubscriptionRecordLimits {
        self.checked_subscription_record_limits()
            .assured("capabilities validate derived record counts before construction")
    }

    pub(crate) fn subscription_byte_limits(
        self,
        limits: &SessionLimits,
    ) -> Result<(usize, usize), Report<ClientError>> {
        subscription_byte_limits(
            limits,
            self.subscription_retained_frames(),
            self.subscriptions_retained_in_full(),
        )
    }

    pub(crate) fn retry_delay(self, attempt: usize) -> Duration {
        let multiplier = match u32::try_from(attempt) {
            Ok(exponent) => 1_u32.checked_shl(exponent),
            Err(_) => None,
        };
        let multiplied = match multiplier {
            Some(multiplier) => self.retry_backoff().checked_mul(multiplier),
            None => None,
        };
        // A millisecond doubled 32 times is already beyond the largest allowed one-day cap.
        match multiplied {
            Some(delay) => delay.min(self.retry_max_backoff()),
            None => self.retry_max_backoff(),
        }
    }
}

/// The per-subscription and complete-exchange byte allowances for the contacted peer.
/// The queue retains only these two configured counts, rather than all client capabilities.
pub(crate) fn subscription_byte_limits(
    limits: &SessionLimits,
    retained_frames: NonZeroUsize,
    subscriptions: NonZeroUsize,
) -> Result<(usize, usize), Report<ClientError>> {
    let largest_event = limits
        .frame_bytes()
        .checked_add(SubscriptionEvent::HEADER_BYTES);
    let Some(largest_event) = largest_event else {
        return Err(Report::new(ClientError::InvalidSubscriptionRetention));
    };
    let Some(bytes) = largest_event.checked_mul(retained_frames.get()) else {
        return Err(Report::new(ClientError::InvalidSubscriptionRetention));
    };
    let Some(total_bytes) = bytes.checked_mul(subscriptions.get()) else {
        return Err(Report::new(ClientError::InvalidSubscriptionRetention));
    };
    Ok((bytes, total_bytes))
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn custom_capabilities_bound_retention_and_retry_delays() {
        let options = ConnectOptions {
            retry_backoff: Duration::from_millis(250),
            retry_max_backoff: Duration::from_secs(3),
            subscription_record_capacity: NonZeroUsize::new(7).assured("seven is positive"),
            subscription_retained_frames: NonZeroUsize::new(2).assured("two is positive"),
            subscriptions_retained_in_full: NonZeroUsize::new(3).assured("three is positive"),
            keepalive_interval: Duration::from_millis(1_500),
            keepalive_timeout: Duration::from_millis(750),
            stream_window_bytes: NonZeroU32::MIN,
            connection_window_bytes: NonZeroU32::new(INITIAL_CONNECTION_WINDOW_BYTES)
                .assured("the initial connection window is positive"),
            ..ConnectOptions::default()
        };
        let capabilities =
            ClientCapabilities::from_options(&options).assured("the custom bounds validate");
        assert_eq!(
            capabilities.keepalive_interval(),
            Duration::from_millis(1_500)
        );
        assert_eq!(capabilities.keepalive_timeout(), Duration::from_millis(750));
        assert_eq!(capabilities.stream_window_bytes(), NonZeroU32::MIN);
        assert_eq!(
            capabilities.connection_window_bytes().get(),
            INITIAL_CONNECTION_WINDOW_BYTES
        );
        let limits = capabilities.subscription_record_limits();
        let (bytes, total_bytes) = capabilities
            .subscription_byte_limits(&SessionLimits::DEFAULT)
            .assured("configured byte limits fit the default session");
        assert_eq!(limits.records, 7);
        assert_eq!(limits.total_records, 21);
        let event_bytes = SessionLimits::DEFAULT
            .frame_bytes()
            .checked_add(SubscriptionEvent::HEADER_BYTES)
            .assured("the test frame and event header fit usize");
        assert_eq!(
            bytes,
            event_bytes
                .checked_mul(2)
                .assured("two test frames fit usize")
        );
        assert_eq!(
            total_bytes,
            bytes.checked_mul(3).assured("six test frames fit usize")
        );
        assert_eq!(capabilities.retry_delay(0), Duration::from_millis(250));
        assert_eq!(capabilities.retry_delay(3), Duration::from_secs(2));
        assert_eq!(capabilities.retry_delay(4), Duration::from_secs(3));
        assert_eq!(capabilities.retry_delay(usize::MAX), Duration::from_secs(3));
    }

    #[test]
    fn invalid_native_bounds_and_derived_retention_fail_before_connecting() {
        let options = [
            ConnectOptions {
                connect_timeout: Duration::ZERO,
                ..ConnectOptions::default()
            },
            ConnectOptions {
                retry_backoff: Duration::from_secs(2),
                ..ConnectOptions::default()
            },
            ConnectOptions {
                upload_frame_capacity: NonZeroUsize::new(usize::MAX)
                    .assured("usize MAX is positive"),
                ..ConnectOptions::default()
            },
            ConnectOptions {
                subscription_retained_frames: NonZeroUsize::new(
                    nervix_primitives::sync::Semaphore::MAX_PERMITS,
                )
                .assured("the channel permit bound is positive"),
                ..ConnectOptions::default()
            },
            ConnectOptions {
                keepalive_interval: Duration::ZERO,
                ..ConnectOptions::default()
            },
            ConnectOptions {
                keepalive_timeout: Duration::from_secs(24 * 60 * 60 + 1),
                ..ConnectOptions::default()
            },
        ];
        for options in options {
            assert!(ClientCapabilities::from_options(&options).is_err());
        }
    }

    /// A window above the HTTP/2 maximum, or a connection window below the window every connection
    /// starts with, is refused with the option, the value and the smallest window it may take.
    #[test]
    fn receive_windows_outside_their_http2_bounds_are_refused() {
        let largest =
            NonZeroU32::new(LARGEST_RECEIVE_WINDOW_BYTES).assured("the largest window is positive");
        let above = largest
            .checked_add(1)
            .assured("the HTTP/2 maximum is below u32::MAX");
        let below_initial = NonZeroU32::new(INITIAL_CONNECTION_WINDOW_BYTES - 1)
            .assured("one byte below the initial window is positive");
        let refusals = [
            (
                ConnectOptions {
                    stream_window_bytes: above,
                    ..ConnectOptions::default()
                },
                "stream-window-bytes",
                above.get(),
                1,
            ),
            (
                ConnectOptions {
                    connection_window_bytes: above,
                    ..ConnectOptions::default()
                },
                "connection-window-bytes",
                above.get(),
                INITIAL_CONNECTION_WINDOW_BYTES,
            ),
            (
                ConnectOptions {
                    connection_window_bytes: below_initial,
                    ..ConnectOptions::default()
                },
                "connection-window-bytes",
                below_initial.get(),
                INITIAL_CONNECTION_WINDOW_BYTES,
            ),
        ];
        for (options, field, bytes, smallest) in refusals {
            let refused = ClientCapabilities::from_options(&options)
                .expect_err("a window outside its HTTP/2 bounds is refused");
            match refused.current_context() {
                ClientError::InvalidReceiveWindow {
                    field: refused_field,
                    bytes: refused_bytes,
                    smallest: refused_smallest,
                } => {
                    assert_eq!(*refused_field, field);
                    assert_eq!(*refused_bytes, bytes);
                    assert_eq!(*refused_smallest, smallest);
                }
                other => panic!("a window outside its bounds is refused as {other:?}"),
            }
        }
        let widest = ConnectOptions {
            stream_window_bytes: largest,
            connection_window_bytes: largest,
            ..ConnectOptions::default()
        };
        ClientCapabilities::from_options(&widest).assured("the HTTP/2 maximum is a valid window");
    }
}
