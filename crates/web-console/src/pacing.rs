//! The pacing the console follows: what the node of its last session opening announced.
//!
//! Layer: edges.
//! - **Owns.** The announced pacing the console keeps across reconnects, the declared defaults it
//!   follows before its first opening, its doubling backoffs and the graph freshness it derives
//!   from the snapshot interval.
//! - **Depends on.** The checked session opening and the web console's capability vocabulary.
//! - **Must not know.** Sockets, browser timers, rendered views or server configuration.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the pacing the console adopts from its openings")
)]

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::SessionPacing;
use nervix_models::{CommandRetryValidity, Setting, WebConsoleCapabilities};

/// How many snapshot intervals the graph may go without a snapshot before its feed reads `STALE`.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the web console chapter calls the feed stale after five snapshot intervals \
                  without a snapshot, as a client link tolerates five of its peer's heartbeat \
                  intervals"
    )
)]
const SNAPSHOT_INTERVALS_BEFORE_STALE: u32 = 5;

/// The pacing of the node the console's session reached last, which the console keeps across
/// reconnects. Before its first opening it holds each value's declared default. An opening that
/// does not know the cluster's command retry validity yet leaves the one learned before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConsolePacing {
    announced: SessionPacing,
    command_retry_validity: Duration,
}

impl Default for ConsolePacing {
    fn default() -> Self {
        let command_retry_validity = Setting::<CommandRetryValidity>::resolve(None, &())
            .assured("the retry validity's declared default is valid");
        Self {
            announced: SessionPacing::default(),
            command_retry_validity: *command_retry_validity.value(),
        }
    }
}

impl ConsolePacing {
    /// Follows the pacing an opening announced from now on.
    pub(crate) fn adopt(&mut self, announced: SessionPacing) {
        self.announced = announced;
        if let Some(validity) = announced.command_retry_validity() {
            self.command_retry_validity = validity;
        }
    }

    pub(crate) fn web_console(&self) -> WebConsoleCapabilities {
        self.announced.web_console()
    }

    /// How long the cluster recognizes a command's execution reference from its issue time.
    pub(crate) fn command_retry_validity(&self) -> Duration {
        self.command_retry_validity
    }

    /// The backoff of reconnects and of commands whose outcome is unknown.
    pub(crate) fn retry(&self) -> Backoff {
        let web_console = self.web_console();
        Backoff {
            first: web_console.retry_backoff(),
            ceiling: web_console.retry_max_backoff(),
        }
    }

    /// The backoff of interrupted subscription tabs, whose first wait is also the period of the
    /// session's retry timer.
    pub(crate) fn restoration(&self) -> Backoff {
        let web_console = self.web_console();
        Backoff {
            first: web_console.restoration_backoff(),
            ceiling: web_console.retry_max_backoff(),
        }
    }

    /// How often the graph's freshness is evaluated again: once per snapshot interval.
    pub(crate) fn freshness_tick(&self) -> Duration {
        self.announced.snapshot_interval()
    }

    /// How long a snapshot keeps the graph's feed `LIVE`.
    pub(crate) fn freshness_timeout(&self) -> Duration {
        self.announced
            .snapshot_interval()
            .checked_mul(SNAPSHOT_INTERVALS_BEFORE_STALE)
            .assured("a snapshot interval validated to at most u32::MAX seconds fits five times")
    }
}

/// A wait that starts at its first value and doubles after each further failure, up to its
/// ceiling, which the opening's validation keeps at least the first value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Backoff {
    first: Duration,
    ceiling: Duration,
}

impl Backoff {
    pub(crate) fn first(self) -> Duration {
        self.first
    }

    /// The wait after one that lasted `previous`.
    pub(crate) fn after(self, previous: Duration) -> Duration {
        let doubled = previous
            .checked_mul(2)
            .assured("a wait of at most a day, the longest console wait, doubles within Duration");
        doubled.min(self.ceiling)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use nervix_models::WebConsoleCapabilityValues;

    use super::*;

    fn announced(snapshot_interval: Duration, validity: Option<Duration>) -> SessionPacing {
        let web_console = WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
            retry_backoff: Some(Duration::from_millis(100)),
            retry_max_backoff: Some(Duration::from_millis(700)),
            restoration_backoff: Some(Duration::from_millis(300)),
            restore_frame_capacity: NonZeroU64::new(2),
            ..WebConsoleCapabilityValues::default()
        })
        .assured("the test waits are valid console waits");
        let validity = validity
            .map(|validity| Setting::new(validity, &()).assured("the test validity is valid"));
        SessionPacing::new(
            Setting::new(snapshot_interval, &()).assured("the test interval is valid"),
            validity,
            web_console,
        )
    }

    #[test]
    fn before_its_first_opening_the_console_follows_the_declared_defaults() {
        let pacing = ConsolePacing::default();
        assert_eq!(pacing.freshness_timeout(), Duration::from_millis(2_500));
        assert_eq!(pacing.freshness_tick(), Duration::from_millis(500));
        assert_eq!(pacing.retry().first(), Duration::from_millis(250));
        assert_eq!(pacing.restoration().first(), Duration::from_secs(1));
        assert_eq!(pacing.command_retry_validity(), Duration::from_secs(900));
    }

    #[test]
    fn the_feed_stays_live_for_five_announced_snapshot_intervals() {
        let mut pacing = ConsolePacing::default();
        pacing.adopt(announced(
            Duration::from_secs(5),
            Some(Duration::from_secs(60)),
        ));
        assert_eq!(pacing.freshness_timeout(), Duration::from_secs(25));
        assert_eq!(pacing.freshness_tick(), Duration::from_secs(5));
    }

    #[test]
    fn an_opening_without_a_retry_validity_keeps_the_one_learned_before() {
        let mut pacing = ConsolePacing::default();
        pacing.adopt(announced(
            Duration::from_secs(1),
            Some(Duration::from_secs(60)),
        ));
        pacing.adopt(announced(Duration::from_secs(2), None));
        assert_eq!(pacing.command_retry_validity(), Duration::from_secs(60));
        assert_eq!(pacing.freshness_tick(), Duration::from_secs(2));
    }

    #[test]
    fn backoffs_double_from_their_first_wait_up_to_the_ceiling() {
        let mut pacing = ConsolePacing::default();
        pacing.adopt(announced(Duration::from_secs(1), None));
        let retry = pacing.retry();
        let mut wait = retry.first();
        let mut waits = vec![wait];
        for _ in 0..4 {
            wait = retry.after(wait);
            waits.push(wait);
        }
        assert_eq!(
            waits,
            [100, 200, 400, 700, 700]
                .map(Duration::from_millis)
                .to_vec()
        );
        let restoration = pacing.restoration();
        assert_eq!(restoration.first(), Duration::from_millis(300));
        assert_eq!(
            restoration.after(restoration.first()),
            Duration::from_millis(600)
        );
        assert_eq!(
            restoration.after(Duration::from_millis(600)),
            Duration::from_millis(700)
        );
    }
}
