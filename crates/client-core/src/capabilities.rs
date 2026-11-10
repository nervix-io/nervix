//! Local connection, retry and retention capabilities of a native client.
//!
//! Layer: edges.
//! - **Owns.** Client setting definitions, validated values and derived subscription queue bounds.
//! - **Depends on.** Vocabulary setting classes, session frame bounds and native channel bounds.
//! - **Must not know.** Cluster policy, server configuration or how a caller presents options.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns native client capabilities")
)]

use std::{num::NonZeroUsize, time::Duration};

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
            ..ConnectOptions::default()
        };
        let capabilities =
            ClientCapabilities::from_options(&options).assured("the custom bounds validate");
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
        ];
        for options in options {
            assert!(ClientCapabilities::from_options(&options).is_err());
        }
    }
}
