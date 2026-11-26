//! The waits and the restore queue of the web console a node serves.
//!
//! Layer: vocabulary.
//! - **Owns.** The web console's capability definitions, their browser-timer bounds, the
//!   relationship between the longest backoff and the first waits it bounds, the restore queue's
//!   frame bound, and their validated serialization.
//! - **Depends on.** Setting classes, the operational capability validation and validated
//!   serialization.
//! - **Must not know.** Browsers, frames, transports or running sessions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the web console's capability vocabulary")
)]

use std::{num::NonZeroU64, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
    rancor::{Fallible, Source},
};
use serde::{Deserialize, Serialize};

use super::{LARGEST_RESTORE_CHUNK_BYTES, LONGEST_CLIENT_WAIT, NodeOperationalError};
use crate::{NodeCapabilities, Setting, SettingDefinition, settings::archive};

/// The resolution of a browser timer: the shortest wait the web console takes, and the unit every
/// one of its waits counts in.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "HTML timers take their delay as a whole number of milliseconds"
    )
)]
pub const BROWSER_TIMER_RESOLUTION: Duration = Duration::from_millis(1);

/// Refuses a web console wait a browser timer cannot keep exactly, or one beyond the longest wait
/// a client takes.
fn validate_wait(
    setting: &'static str,
    value: Duration,
) -> Result<(), Report<NodeOperationalError>> {
    let below_resolution = value
        .as_nanos()
        .checked_rem(BROWSER_TIMER_RESOLUTION.as_nanos())
        .assured("the timer resolution is a positive duration");
    let whole_milliseconds = below_resolution == 0;
    if !whole_milliseconds || value < BROWSER_TIMER_RESOLUTION || value > LONGEST_CLIENT_WAIT {
        return Err(Report::new(NodeOperationalError::WebConsoleWait {
            setting,
            value,
            minimum: BROWSER_TIMER_RESOLUTION,
            maximum: LONGEST_CLIENT_WAIT,
        }));
    }
    Ok(())
}

macro_rules! web_console_wait {
    ($(#[$documentation:meta])* $definition:ident, $name:literal, $default:expr) => {
        $(#[$documentation])*
        pub struct $definition;

        impl $definition {
            pub const DEFAULT: Duration = $default;
        }

        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = Duration;
            type Context = ();
            type Error = NodeOperationalError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = None;

            fn default_value(_: &()) -> Duration {
                Self::DEFAULT
            }

            fn validate(value: &Duration, _: &()) -> Result<(), Report<Self::Error>> {
                validate_wait(Self::NAME, *value)
            }
        }
    };
}

web_console_wait!(
    /// How long a backup download waits for its opening and for each next frame, and a restore
    /// for its connection to take a chunk and for its reply once the last chunk was sent.
    WebConsoleRequestTimeout,
    "web-console-request-timeout",
    Duration::from_secs(120)
);
web_console_wait!(
    /// How long the console waits for the answer to its credential check after a connection
    /// failed, before it keeps the credentials for the next attempt.
    WebConsoleConnectTimeout,
    "web-console-connect-timeout",
    Duration::from_secs(3)
);
web_console_wait!(
    /// The first wait before the console connects again, and before it repeats a command whose
    /// outcome is unknown. Each further wait doubles, up to the longest backoff.
    WebConsoleRetryBackoff,
    "web-console-retry-backoff",
    Duration::from_millis(250)
);
web_console_wait!(
    /// The first wait before an interrupted subscription tab is restored again, which doubles up
    /// to the longest backoff; the period of the session's retry timer; and the pause before a
    /// backup download or a restore is streamed again.
    WebConsoleRestorationBackoff,
    "web-console-restoration-backoff",
    Duration::from_secs(1)
);

/// The first waits the longest backoff bounds: it is at least each of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebConsoleFirstWaits {
    pub retry: Setting<WebConsoleRetryBackoff>,
    pub restoration: Setting<WebConsoleRestorationBackoff>,
}

/// The longest wait a doubling console backoff reaches: reconnects, command repetitions and
/// subscription restorations.
pub struct WebConsoleRetryMaxBackoff;

impl WebConsoleRetryMaxBackoff {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "this setting definition owns its single compile-time default"
        )
    )]
    pub const DEFAULT: Duration = Duration::from_secs(5);
}

impl SettingDefinition for WebConsoleRetryMaxBackoff {
    type Class = NodeCapabilities;
    type Value = Duration;
    type Context = WebConsoleFirstWaits;
    type Error = NodeOperationalError;
    const NAME: &'static str = "web-console-retry-max-backoff";
    const DERIVATION: Option<&'static str> =
        Some("at least the resolved web-console-retry-backoff and web-console-restoration-backoff");

    fn default_value(firsts: &Self::Context) -> Duration {
        Self::DEFAULT
            .max(*firsts.retry.value())
            .max(*firsts.restoration.value())
    }

    fn validate(value: &Duration, firsts: &Self::Context) -> Result<(), Report<Self::Error>> {
        validate_wait(Self::NAME, *value)?;
        let first_waits = [
            (WebConsoleRetryBackoff::NAME, *firsts.retry.value()),
            (
                WebConsoleRestorationBackoff::NAME,
                *firsts.restoration.value(),
            ),
        ];
        for (first_setting, first) in first_waits {
            if *value < first {
                return Err(Report::new(
                    NodeOperationalError::WebConsoleBackoffCeiling {
                        setting: Self::NAME,
                        value: *value,
                        first_setting,
                        first,
                    },
                ));
            }
        }
        Ok(())
    }
}

/// The restore frames the browser keeps queued on its connection before the stream waits for the
/// connection to send them.
pub struct WebConsoleRestoreFrameCapacity;

impl WebConsoleRestoreFrameCapacity {
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "this setting definition owns its single compile-time default"
        )
    )]
    pub const DEFAULT: u64 = 4;

    /// The most frames whose largest chunks the browser can count as queued: it reports a
    /// WebSocket's queued bytes as a 32-bit count.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "web-sys reports a WebSocket's bufferedAmount as a u32, which must hold \
                      every frame the queue admits at the largest restore chunk"
        )
    )]
    pub const MAXIMUM: u32 = u32::MAX / LARGEST_RESTORE_CHUNK_BYTES;
}

impl SettingDefinition for WebConsoleRestoreFrameCapacity {
    type Class = NodeCapabilities;
    type Value = NonZeroU64;
    type Context = ();
    type Error = NodeOperationalError;
    const NAME: &'static str = "web-console-restore-frame-capacity";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> NonZeroU64 {
        NonZeroU64::new(Self::DEFAULT).assured("the definition's default count is positive")
    }

    fn validate(value: &NonZeroU64, _: &()) -> Result<(), Report<Self::Error>> {
        let maximum = u64::from(Self::MAXIMUM);
        if value.get() > maximum {
            return Err(Report::new(NodeOperationalError::Count {
                setting: Self::NAME,
                value: value.get(),
                maximum,
            }));
        }
        Ok(())
    }
}

/// How the web console a node serves paces itself: its request and connect timeouts, its
/// backoffs, and how many restore frames it queues on a connection. The node announces them when
/// a console WebSocket opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Archive, RkyvSerialize)]
#[serde(deny_unknown_fields)]
pub struct WebConsoleCapabilities {
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    request_timeout: Setting<WebConsoleRequestTimeout>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    connect_timeout: Setting<WebConsoleConnectTimeout>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    retry_backoff: Setting<WebConsoleRetryBackoff>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    retry_max_backoff: Setting<WebConsoleRetryMaxBackoff>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    restoration_backoff: Setting<WebConsoleRestorationBackoff>,
    #[serde(with = "archive")]
    #[rkyv(with = archive::ValidatedSetting)]
    restore_frame_capacity: Setting<WebConsoleRestoreFrameCapacity>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WebConsoleCapabilityValues {
    pub request_timeout: Option<Duration>,
    pub connect_timeout: Option<Duration>,
    pub retry_backoff: Option<Duration>,
    pub retry_max_backoff: Option<Duration>,
    pub restoration_backoff: Option<Duration>,
    pub restore_frame_capacity: Option<NonZeroU64>,
}

impl WebConsoleCapabilities {
    pub fn resolve(
        values: WebConsoleCapabilityValues,
    ) -> Result<Self, Report<NodeOperationalError>> {
        let retry_backoff = Setting::resolve(values.retry_backoff, &())?;
        let restoration_backoff = Setting::resolve(values.restoration_backoff, &())?;
        let first_waits = WebConsoleFirstWaits {
            retry: retry_backoff,
            restoration: restoration_backoff,
        };
        Ok(Self {
            request_timeout: Setting::resolve(values.request_timeout, &())?,
            connect_timeout: Setting::resolve(values.connect_timeout, &())?,
            retry_backoff,
            retry_max_backoff: Setting::resolve(values.retry_max_backoff, &first_waits)?,
            restoration_backoff,
            restore_frame_capacity: Setting::resolve(values.restore_frame_capacity, &())?,
        })
    }

    pub fn request_timeout(&self) -> Duration {
        *self.request_timeout.value()
    }
    pub fn connect_timeout(&self) -> Duration {
        *self.connect_timeout.value()
    }
    pub fn retry_backoff(&self) -> Duration {
        *self.retry_backoff.value()
    }
    pub fn retry_max_backoff(&self) -> Duration {
        *self.retry_max_backoff.value()
    }
    pub fn restoration_backoff(&self) -> Duration {
        *self.restoration_backoff.value()
    }
    pub fn restore_frame_capacity(&self) -> NonZeroU64 {
        *self.restore_frame_capacity.value()
    }

    pub fn described(&self) -> Vec<(&'static str, String)> {
        vec![
            (
                WebConsoleRequestTimeout::NAME,
                crate::timing_text(self.request_timeout()),
            ),
            (
                WebConsoleConnectTimeout::NAME,
                crate::timing_text(self.connect_timeout()),
            ),
            (
                WebConsoleRetryBackoff::NAME,
                crate::timing_text(self.retry_backoff()),
            ),
            (
                WebConsoleRetryMaxBackoff::NAME,
                crate::timing_text(self.retry_max_backoff()),
            ),
            (
                WebConsoleRestorationBackoff::NAME,
                crate::timing_text(self.restoration_backoff()),
            ),
            (
                WebConsoleRestoreFrameCapacity::NAME,
                self.restore_frame_capacity().to_string(),
            ),
        ]
    }
}

impl Default for WebConsoleCapabilities {
    fn default() -> Self {
        Self::resolve(WebConsoleCapabilityValues::default())
            .assured("the web console defaults satisfy their bounds and relationship")
    }
}

impl<'de> Deserialize<'de> for WebConsoleCapabilities {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            request_timeout: Duration,
            connect_timeout: Duration,
            retry_backoff: Duration,
            retry_max_backoff: Duration,
            restoration_backoff: Duration,
            restore_frame_capacity: NonZeroU64,
        }
        let fields = Fields::deserialize(deserializer)?;
        Self::resolve(WebConsoleCapabilityValues {
            request_timeout: Some(fields.request_timeout),
            connect_timeout: Some(fields.connect_timeout),
            retry_backoff: Some(fields.retry_backoff),
            retry_max_backoff: Some(fields.retry_max_backoff),
            restoration_backoff: Some(fields.restoration_backoff),
            restore_frame_capacity: Some(fields.restore_frame_capacity),
        })
        .map_err(|error| D::Error::custom(format_args!("{error:#}")))
    }
}

impl<D: Fallible + ?Sized> RkyvDeserialize<WebConsoleCapabilities, D>
    for ArchivedWebConsoleCapabilities
where
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<WebConsoleCapabilities, D::Error> {
        WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
            request_timeout: Some(self.request_timeout.deserialize(deserializer)?),
            connect_timeout: Some(self.connect_timeout.deserialize(deserializer)?),
            retry_backoff: Some(self.retry_backoff.deserialize(deserializer)?),
            retry_max_backoff: Some(self.retry_max_backoff.deserialize(deserializer)?),
            restoration_backoff: Some(self.restoration_backoff.deserialize(deserializer)?),
            restore_frame_capacity: Some(self.restore_frame_capacity.deserialize(deserializer)?),
        })
        .map_err(|error| D::Error::new(error.into_error()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_timings_the_console_ran_with_before_they_were_declared() {
        let capabilities = WebConsoleCapabilities::default();
        assert_eq!(capabilities.request_timeout(), Duration::from_secs(120));
        assert_eq!(capabilities.connect_timeout(), Duration::from_secs(3));
        assert_eq!(capabilities.retry_backoff(), Duration::from_millis(250));
        assert_eq!(capabilities.retry_max_backoff(), Duration::from_secs(5));
        assert_eq!(capabilities.restoration_backoff(), Duration::from_secs(1));
        assert_eq!(capabilities.restore_frame_capacity().get(), 4);
        assert_eq!(
            capabilities.described(),
            vec![
                ("web-console-request-timeout", "2m".to_owned()),
                ("web-console-connect-timeout", "3s".to_owned()),
                ("web-console-retry-backoff", "250ms".to_owned()),
                ("web-console-retry-max-backoff", "5s".to_owned()),
                ("web-console-restoration-backoff", "1s".to_owned()),
                ("web-console-restore-frame-capacity", "4".to_owned()),
            ]
        );
    }

    #[test]
    fn an_omitted_longest_backoff_follows_a_longer_first_wait() {
        let capabilities = WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
            restoration_backoff: Some(Duration::from_secs(8)),
            ..Default::default()
        })
        .assured("an omitted longest backoff takes at least each first wait");
        assert_eq!(capabilities.retry_max_backoff(), Duration::from_secs(8));
    }

    #[test]
    fn a_longest_backoff_below_a_first_wait_names_both_settings() {
        let refused = WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
            retry_backoff: Some(Duration::from_secs(10)),
            retry_max_backoff: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .expect_err("the longest backoff bounds the first wait");
        assert_eq!(
            refused.current_context(),
            &NodeOperationalError::WebConsoleBackoffCeiling {
                setting: "web-console-retry-max-backoff",
                value: Duration::from_secs(5),
                first_setting: "web-console-retry-backoff",
                first: Duration::from_secs(10),
            }
        );
        assert_eq!(
            refused.current_context().to_string(),
            "web-console-retry-max-backoff (5s) must be at least web-console-retry-backoff (10s)"
        );
    }

    #[test]
    fn a_wait_a_browser_timer_cannot_keep_exactly_is_refused() {
        for value in [
            Duration::ZERO,
            Duration::from_micros(1500),
            LONGEST_CLIENT_WAIT + BROWSER_TIMER_RESOLUTION,
        ] {
            let refused = WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
                request_timeout: Some(value),
                ..Default::default()
            })
            .expect_err("a console wait is whole milliseconds from 1 ms through a day");
            assert!(matches!(
                refused.current_context(),
                NodeOperationalError::WebConsoleWait {
                    setting: "web-console-request-timeout",
                    ..
                }
            ));
        }
        for value in [BROWSER_TIMER_RESOLUTION, LONGEST_CLIENT_WAIT] {
            let accepted = WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
                request_timeout: Some(value),
                ..Default::default()
            })
            .assured("the bounds themselves are valid waits");
            assert_eq!(accepted.request_timeout(), value);
        }
    }

    #[test]
    fn a_restore_queue_the_browser_cannot_count_is_refused() {
        let largest = NonZeroU64::new(u64::from(WebConsoleRestoreFrameCapacity::MAXIMUM));
        let accepted = WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
            restore_frame_capacity: largest,
            ..Default::default()
        })
        .assured("the largest queue fits the browser's count");
        assert_eq!(
            accepted.restore_frame_capacity(),
            largest.assured("positive")
        );
        let beyond = largest
            .assured("positive")
            .checked_add(1)
            .assured("the largest queue is far below u64::MAX");
        let refused = WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
            restore_frame_capacity: Some(beyond),
            ..Default::default()
        })
        .expect_err("a queue the browser cannot count is refused");
        assert!(matches!(
            refused.current_context(),
            NodeOperationalError::Count {
                setting: "web-console-restore-frame-capacity",
                ..
            }
        ));
    }

    #[test]
    fn decoding_checks_the_longest_backoff_against_the_first_waits_again() {
        let json = serde_json::json!({
            "request_timeout": Duration::from_secs(30),
            "connect_timeout": Duration::from_secs(2),
            "retry_backoff": Duration::from_secs(4),
            "retry_max_backoff": Duration::from_secs(2),
            "restoration_backoff": Duration::from_secs(1),
            "restore_frame_capacity": 8,
        });
        assert!(serde_json::from_value::<WebConsoleCapabilities>(json).is_err());

        let capabilities = WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
            request_timeout: Some(Duration::from_secs(30)),
            connect_timeout: Some(Duration::from_secs(2)),
            retry_backoff: Some(Duration::from_millis(100)),
            retry_max_backoff: Some(Duration::from_secs(2)),
            restoration_backoff: Some(Duration::from_millis(500)),
            restore_frame_capacity: NonZeroU64::new(8),
        })
        .assured("the values satisfy every bound");
        let encoded = serde_json::to_vec(&capabilities).assured("capabilities encode");
        let decoded = serde_json::from_slice::<WebConsoleCapabilities>(&encoded)
            .assured("valid capabilities decode");
        assert_eq!(decoded, capabilities);
        let archived =
            rkyv::to_bytes::<rkyv::rancor::Error>(&capabilities).assured("capabilities archive");
        let restored = rkyv::from_bytes::<WebConsoleCapabilities, rkyv::rancor::Error>(&archived)
            .assured("a valid archive restores");
        assert_eq!(restored, capabilities);
    }

    #[test]
    fn an_archive_whose_longest_backoff_is_below_a_first_wait_is_refused() {
        let long_first_waits = WebConsoleFirstWaits {
            retry: Setting::new(Duration::from_secs(1), &()).assured("1 s is a console wait"),
            restoration: Setting::new(Duration::from_secs(1), &()).assured("1 s is a console wait"),
        };
        let stored = WebConsoleCapabilities {
            retry_backoff: Setting::new(Duration::from_secs(4), &())
                .assured("4 s is a console wait"),
            retry_max_backoff: Setting::new(Duration::from_secs(2), &long_first_waits)
                .assured("2 s bounds first waits of 1 s"),
            ..WebConsoleCapabilities::default()
        };
        let archived =
            rkyv::to_bytes::<rkyv::rancor::Error>(&stored).assured("capabilities archive");
        assert!(
            rkyv::from_bytes::<WebConsoleCapabilities, rkyv::rancor::Error>(&archived).is_err(),
            "a 2 s ceiling below a 4 s first wait must not restore"
        );
    }
}
