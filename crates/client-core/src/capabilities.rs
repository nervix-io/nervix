//! Local connection, retry, restoration and retention capabilities of a native client.
//!
//! Layer: edges.
//! - **Owns.** Client setting definitions, validated values, the derived subscription queue
//!   bounds and the routing attempts a call's deadline holds.
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

/// The shortest deadline or backoff the native client accepts. A host binding that bounds a call
/// itself takes its bound from here rather than restating it.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the Rust client library refuses a deadline or backoff shorter than one \
                  millisecond"
    )
)]
pub const SHORTEST_DEADLINE: Duration = Duration::from_millis(1);

/// The longest deadline or backoff the native client accepts: one day. A host binding that bounds
/// a call itself takes its bound from here rather than restating it.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the Rust client library refuses a deadline or backoff longer than 24 hours"
    )
)]
pub const LONGEST_DEADLINE: Duration = Duration::from_secs(24 * 60 * 60);

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
                if *value < SHORTEST_DEADLINE || *value > LONGEST_DEADLINE {
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
// A restoration a new session refused is sent again on that session after a wait that starts at
// the first backoff and doubles after each refusal up to the ceiling, so a refusal that clears
// soon, such as a relay that is still starting, is retried promptly, while one that lasts costs
// one request per ceiling.
deadline!(
    ClientRestorationBackoff,
    "restoration-backoff",
    "restoration_backoff",
    Duration::from_secs(1)
);
deadline!(
    ClientRestorationMaxBackoff,
    "restoration-max-backoff",
    "restoration_max_backoff",
    Duration::from_secs(30)
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
// The request frames one exchange queues before a caller sending another waits for the transport.
capacity!(ClientRequestFrameCapacity, "request-frame-capacity", 32);
// The server notices one exchange retains for a caller that has not read them.
capacity!(ClientNoticeCapacity, "notice-capacity", 128);

/// The bytes of server notices one exchange retains for a caller that has not read them, each
/// notice charged its text and its in-memory header.
pub struct ClientNoticeBytes;

impl SettingDefinition for ClientNoticeBytes {
    type Class = NodeCapabilities;
    type Value = NonZeroUsize;
    type Context = ();
    type Error = ClientError;
    const NAME: &'static str = "notice-bytes";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> NonZeroUsize {
        NonZeroUsize::new(1024 * 1024).assured("one mebibyte is positive")
    }

    fn validate(_: &NonZeroUsize, _: &()) -> Result<(), Report<ClientError>> {
        // Every positive bound is valid. The queue checks its retained bytes with checked
        // arithmetic, and a notice larger than the bound drops the notices held and leaves a gap
        // that the caller's next read reports, as every notice that does not fit does.
        Ok(())
    }
}

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
                settings.validate().assured("default retry, restoration and retention relationships hold");
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
    restoration_backoff: ClientRestorationBackoff => Duration,
    restoration_max_backoff: ClientRestorationMaxBackoff => Duration,
    subscription_record_capacity: ClientSubscriptionRecordCapacity => NonZeroUsize,
    subscription_retained_frames: ClientSubscriptionRetainedFrames => NonZeroUsize,
    subscriptions_retained_in_full: ClientSubscriptionsRetainedInFull => NonZeroUsize,
    upload_frame_capacity: ClientUploadFrameCapacity => NonZeroUsize,
    restore_frame_capacity: ClientRestoreFrameCapacity => NonZeroUsize,
    request_frame_capacity: ClientRequestFrameCapacity => NonZeroUsize,
    notice_capacity: ClientNoticeCapacity => NonZeroUsize,
    notice_bytes: ClientNoticeBytes => NonZeroUsize,
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
        if self.restoration_backoff() > self.restoration_max_backoff() {
            return Err(Report::new(ClientError::InvalidRestorationBackoff));
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

    pub(crate) fn retry_delay(self, attempt: u32) -> Duration {
        let multiplied = match 1_u32.checked_shl(attempt) {
            Some(multiplier) => self.retry_backoff().checked_mul(multiplier),
            None => None,
        };
        // A millisecond doubled 32 times is already beyond the largest allowed one-day cap.
        match multiplied {
            Some(delay) => delay.min(self.retry_max_backoff()),
            None => self.retry_max_backoff(),
        }
    }

    /// How many times a call other than a `BACKUP` command is sent while it is redirected or
    /// waits for an election: the attempts the retry timeout holds, 1,200 by default. A restore
    /// stream and a backup download, which bound each frame rather than the whole call, make at
    /// most this many attempts too.
    pub(crate) fn routing_attempts(self) -> NonZeroU32 {
        self.attempts_within(self.retry_timeout())
    }

    /// How many times a `BACKUP` command, which its backup wait bounds instead of the retry
    /// timeout, is sent while it is redirected or waits for an election: the attempts the backup
    /// wait holds.
    pub(crate) fn backup_routing_attempts(self) -> NonZeroU32 {
        self.attempts_within(self.backup_wait_timeout())
    }

    /// As many election waits of the first retry backoff as `deadline` holds, and at least one.
    ///
    /// Every election wait lasts at least that backoff, so the waits of these attempts alone fill
    /// the deadline to within two backoffs: a call the deadline bounds ends at its deadline rather
    /// than on this count, whatever the doubling between the backoff and its ceiling. `deadline`
    /// is one of the validated deadlines, at most one day, and the backoff is at least one
    /// millisecond, so the count is at most 86,400,000 and fits the `u32` it is counted in.
    fn attempts_within(self, deadline: Duration) -> NonZeroU32 {
        let quotient = deadline
            .as_nanos()
            .checked_div(self.retry_backoff().as_nanos())
            .assured("a validated retry backoff is at least one millisecond");
        let attempts = u32::try_from(quotient).assured(
            "a validated deadline of at most one day holds at most 86,400,000 waits of a \
             validated backoff of at least one millisecond",
        );
        match NonZeroU32::new(attempts) {
            Some(attempts) => attempts,
            // A deadline shorter than one backoff still sends the call once.
            None => NonZeroU32::MIN,
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
        assert_eq!(capabilities.retry_delay(u32::MAX), Duration::from_secs(3));
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

    #[test]
    fn restoration_waits_request_frames_and_notice_bounds_are_validated_capabilities() {
        let options = ConnectOptions {
            restoration_backoff: Duration::from_millis(250),
            restoration_max_backoff: Duration::from_secs(4),
            request_frame_capacity: NonZeroUsize::new(5).assured("five is positive"),
            notice_capacity: NonZeroUsize::new(6).assured("six is positive"),
            notice_bytes: NonZeroUsize::new(usize::MAX).assured("usize MAX is positive"),
            ..ConnectOptions::default()
        };
        let capabilities =
            ClientCapabilities::from_options(&options).assured("the custom bounds validate");
        assert_eq!(
            capabilities.restoration_backoff(),
            Duration::from_millis(250)
        );
        assert_eq!(
            capabilities.restoration_max_backoff(),
            Duration::from_secs(4)
        );
        assert_eq!(capabilities.request_frame_capacity().get(), 5);
        assert_eq!(capabilities.notice_capacity().get(), 6);
        assert_eq!(capabilities.notice_bytes().get(), usize::MAX);

        let defaults = ClientCapabilities::default();
        assert_eq!(defaults.restoration_backoff(), Duration::from_secs(1));
        assert_eq!(defaults.restoration_max_backoff(), Duration::from_secs(30));
        assert_eq!(defaults.request_frame_capacity().get(), 32);
        assert_eq!(defaults.notice_capacity().get(), 128);
        assert_eq!(defaults.notice_bytes().get(), 1024 * 1024);

        let above_ceiling = ConnectOptions {
            restoration_backoff: Duration::from_secs(31),
            ..ConnectOptions::default()
        };
        let Err(refused) = ClientCapabilities::from_options(&above_ceiling) else {
            panic!("a first restoration wait above its ceiling is refused");
        };
        assert!(
            matches!(
                refused.current_context(),
                ClientError::InvalidRestorationBackoff
            ),
            "{refused:?}"
        );
        let beyond_a_day = ConnectOptions {
            restoration_max_backoff: LONGEST_DEADLINE
                .checked_add(Duration::from_millis(1))
                .assured("a day and a millisecond fit Duration"),
            ..ConnectOptions::default()
        };
        let Err(refused) = ClientCapabilities::from_options(&beyond_a_day) else {
            panic!("a restoration ceiling beyond one day is refused");
        };
        assert!(
            matches!(
                refused.current_context(),
                ClientError::InvalidDeadline {
                    field: "restoration_max_backoff"
                }
            ),
            "{refused:?}"
        );
        for options in [
            ConnectOptions {
                request_frame_capacity: NonZeroUsize::new(usize::MAX)
                    .assured("usize MAX is positive"),
                ..ConnectOptions::default()
            },
            ConnectOptions {
                notice_capacity: NonZeroUsize::new(usize::MAX).assured("usize MAX is positive"),
                ..ConnectOptions::default()
            },
        ] {
            let Err(refused) = ClientCapabilities::from_options(&options) else {
                panic!("a queue bound beyond the native channel limit is refused");
            };
            assert!(
                matches!(
                    refused.current_context(),
                    ClientError::InvalidFrameCapacity { .. }
                ),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn a_call_makes_as_many_routing_attempts_as_its_deadline_holds_waits_of_the_first_backoff() {
        /// The routing attempts of a call under `retry_timeout` whose every election wait is
        /// `retry_backoff`.
        fn attempts(retry_timeout: Duration, retry_backoff: Duration) -> u32 {
            let options = ConnectOptions {
                retry_timeout,
                retry_backoff,
                retry_max_backoff: retry_backoff,
                ..ConnectOptions::default()
            };
            let capabilities =
                ClientCapabilities::from_options(&options).assured("the test deadlines validate");
            capabilities.routing_attempts().get()
        }

        let defaults = ClientCapabilities::default();
        assert_eq!(defaults.routing_attempts().get(), 1_200);
        assert_eq!(defaults.backup_routing_attempts().get(), 6_000);
        assert_eq!(
            attempts(Duration::from_secs(120), Duration::from_millis(1)),
            120_000
        );
        assert_eq!(
            attempts(Duration::from_millis(250), Duration::from_millis(100)),
            2,
            "the count is the whole number of waits the deadline holds"
        );
        assert_eq!(
            attempts(Duration::from_millis(1), Duration::from_secs(1)),
            1,
            "a deadline shorter than one backoff still sends the call once"
        );
        assert_eq!(
            attempts(LONGEST_DEADLINE, SHORTEST_DEADLINE),
            86_400_000,
            "the longest deadline over the shortest backoff fits u32"
        );
    }
}
