//! The web console pacing options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the web console's pacing capabilities: its request and
//!   connect timeouts, its backoffs and its restore queue; their conversion into the capability
//!   group, and the rendering of that group back into options.
//! - **Depends on.** The vocabulary's web console definitions and the edge's value forms.
//! - **Must not know.** How the console is served or how a browser paces itself.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::num::NonZeroU64;

use error_stack::Report;
use nervix_models::{
    NodeOperationalError, SettingDefinition as _, WebConsoleCapabilities,
    WebConsoleCapabilityValues, WebConsoleConnectTimeout, WebConsoleRequestTimeout,
    WebConsoleRestorationBackoff, WebConsoleRestoreFrameCapacity, WebConsoleRetryBackoff,
    WebConsoleRetryMaxBackoff,
};

use super::settings::{CommandLine, capability_help};
use crate::application::cli_values::HumanDuration;

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Web console pacing")]
pub struct WebConsoleArgs {
    #[arg(
        long = WebConsoleRequestTimeout::NAME,
        env = "NERVIX_WEB_CONSOLE_REQUEST_TIMEOUT",
        help = capability_help::<WebConsoleRequestTimeout>(
            "How long a console backup download or restore waits for its next frame, for room \
             on its connection or for its reply",
            HumanDuration(WebConsoleRequestTimeout::DEFAULT),
        )
    )]
    pub web_console_request_timeout: Option<HumanDuration>,
    #[arg(
        long = WebConsoleConnectTimeout::NAME,
        env = "NERVIX_WEB_CONSOLE_CONNECT_TIMEOUT",
        help = capability_help::<WebConsoleConnectTimeout>(
            "How long the console waits for its credential check after a connection failed",
            HumanDuration(WebConsoleConnectTimeout::DEFAULT),
        )
    )]
    pub web_console_connect_timeout: Option<HumanDuration>,
    #[arg(
        long = WebConsoleRetryBackoff::NAME,
        env = "NERVIX_WEB_CONSOLE_RETRY_BACKOFF",
        help = capability_help::<WebConsoleRetryBackoff>(
            "First wait before the console connects again or repeats a command whose outcome \
             is unknown; it doubles",
            HumanDuration(WebConsoleRetryBackoff::DEFAULT),
        )
    )]
    pub web_console_retry_backoff: Option<HumanDuration>,
    #[arg(
        long = WebConsoleRetryMaxBackoff::NAME,
        env = "NERVIX_WEB_CONSOLE_RETRY_MAX_BACKOFF",
        help = capability_help::<WebConsoleRetryMaxBackoff>(
            "Longest wait the console's doubling backoffs reach",
            HumanDuration(WebConsoleRetryMaxBackoff::DEFAULT),
        )
    )]
    pub web_console_retry_max_backoff: Option<HumanDuration>,
    #[arg(
        long = WebConsoleRestorationBackoff::NAME,
        env = "NERVIX_WEB_CONSOLE_RESTORATION_BACKOFF",
        help = capability_help::<WebConsoleRestorationBackoff>(
            "First wait before an interrupted subscription tab is restored again, the session's \
             retry timer, and the pause before a download or restore streams again",
            HumanDuration(WebConsoleRestorationBackoff::DEFAULT),
        )
    )]
    pub web_console_restoration_backoff: Option<HumanDuration>,
    #[arg(
        long = WebConsoleRestoreFrameCapacity::NAME,
        env = "NERVIX_WEB_CONSOLE_RESTORE_FRAME_CAPACITY",
        help = capability_help::<WebConsoleRestoreFrameCapacity>(
            "Restore frames the console keeps queued on its connection",
            WebConsoleRestoreFrameCapacity::DEFAULT,
        )
    )]
    pub web_console_restore_frame_capacity: Option<NonZeroU64>,
}

impl WebConsoleArgs {
    pub(super) fn settings(&self) -> Result<WebConsoleCapabilities, Report<NodeOperationalError>> {
        WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
            request_timeout: self.web_console_request_timeout.map(|value| value.0),
            connect_timeout: self.web_console_connect_timeout.map(|value| value.0),
            retry_backoff: self.web_console_retry_backoff.map(|value| value.0),
            retry_max_backoff: self.web_console_retry_max_backoff.map(|value| value.0),
            restoration_backoff: self.web_console_restoration_backoff.map(|value| value.0),
            restore_frame_capacity: self.web_console_restore_frame_capacity,
        })
    }

    pub(super) fn from_settings(capabilities: &WebConsoleCapabilities) -> Self {
        Self {
            web_console_request_timeout: Some(HumanDuration(capabilities.request_timeout())),
            web_console_connect_timeout: Some(HumanDuration(capabilities.connect_timeout())),
            web_console_retry_backoff: Some(HumanDuration(capabilities.retry_backoff())),
            web_console_retry_max_backoff: Some(HumanDuration(capabilities.retry_max_backoff())),
            web_console_restoration_backoff: Some(HumanDuration(
                capabilities.restoration_backoff(),
            )),
            web_console_restore_frame_capacity: Some(capabilities.restore_frame_capacity()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(
            WebConsoleRequestTimeout::NAME,
            self.web_console_request_timeout,
        );
        command_line.optional(
            WebConsoleConnectTimeout::NAME,
            self.web_console_connect_timeout,
        );
        command_line.optional(WebConsoleRetryBackoff::NAME, self.web_console_retry_backoff);
        command_line.optional(
            WebConsoleRetryMaxBackoff::NAME,
            self.web_console_retry_max_backoff,
        );
        command_line.optional(
            WebConsoleRestorationBackoff::NAME,
            self.web_console_restoration_backoff,
        );
        command_line.optional(
            WebConsoleRestoreFrameCapacity::NAME,
            self.web_console_restore_frame_capacity,
        );
    }
}
