//! Native client capabilities at the command-line boundary.
//!
//! Layer: edges.
//! - **Owns.** Connection flags, their environment twins and their lossless rendering as arguments.
//! - **Depends on.** The client's setting definitions and Clap.
//! - **Must not know.** Session retry, queue dispatch or transport implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns client capability arguments")
)]

use std::{num::NonZeroUsize, time::Duration};

use nervix_client_core::{ConnectOptions, capabilities as client_caps};
use nervix_models::SettingDefinition as _;

#[derive(clap::Args, Debug, Clone, PartialEq, Eq)]
#[command(next_help_heading = "Connection capabilities")]
pub(crate) struct ConnectionArgs {
    /// Deadline of each connection attempt.
    #[arg(long = client_caps::ClientConnectTimeout::NAME, env = "NERVIX_CLI_CONNECT_TIMEOUT", global = true, default_value = humantime::format_duration(client_caps::ClientConnectTimeout::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) connect_timeout: Duration,
    /// Deadline of each request.
    #[arg(long = client_caps::ClientRequestTimeout::NAME, env = "NERVIX_CLI_REQUEST_TIMEOUT", global = true, default_value = humantime::format_duration(client_caps::ClientRequestTimeout::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) request_timeout: Duration,
    /// Total recovery deadline across redirects and reconnects.
    #[arg(long = client_caps::ClientRetryTimeout::NAME, env = "NERVIX_CLI_RETRY_TIMEOUT", global = true, default_value = humantime::format_duration(client_caps::ClientRetryTimeout::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) retry_timeout: Duration,
    /// Total backup wait across cuts, redirects and reconnects.
    #[arg(long = client_caps::ClientBackupWaitTimeout::NAME, env = "NERVIX_CLI_BACKUP_WAIT_TIMEOUT", global = true, default_value = humantime::format_duration(client_caps::ClientBackupWaitTimeout::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) backup_wait_timeout: Duration,
    /// Initial reconnect and recovery delay.
    #[arg(long = client_caps::ClientRetryBackoff::NAME, env = "NERVIX_CLI_RETRY_BACKOFF", global = true, default_value = humantime::format_duration(client_caps::ClientRetryBackoff::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) retry_backoff: Duration,
    /// Ceiling for doubled reconnect and recovery delays.
    #[arg(long = client_caps::ClientRetryMaxBackoff::NAME, env = "NERVIX_CLI_RETRY_MAX_BACKOFF", global = true, default_value = humantime::format_duration(client_caps::ClientRetryMaxBackoff::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) retry_max_backoff: Duration,
    /// Events retained per subscription.
    #[arg(long = client_caps::ClientSubscriptionRecordCapacity::NAME, env = "NERVIX_CLI_SUBSCRIPTION_RECORD_CAPACITY", global = true, default_value = client_caps::ClientSubscriptionRecordCapacity::default_value(&()).to_string())]
    pub(crate) subscription_record_capacity: NonZeroUsize,
    /// Frame-limit units of bytes retained per subscription, including event headers.
    #[arg(long = client_caps::ClientSubscriptionRetainedFrames::NAME, env = "NERVIX_CLI_SUBSCRIPTION_RETAINED_FRAMES", global = true, default_value = client_caps::ClientSubscriptionRetainedFrames::default_value(&()).to_string())]
    pub(crate) subscription_retained_frames: NonZeroUsize,
    /// Multiplier for aggregate subscription record and byte bounds.
    #[arg(long = client_caps::ClientSubscriptionsRetainedInFull::NAME, env = "NERVIX_CLI_SUBSCRIPTIONS_RETAINED_IN_FULL", global = true, default_value = client_caps::ClientSubscriptionsRetainedInFull::default_value(&()).to_string())]
    pub(crate) subscriptions_retained_in_full: NonZeroUsize,
    /// Outbound resource-upload frames queued before backpressure.
    #[arg(long = client_caps::ClientUploadFrameCapacity::NAME, env = "NERVIX_CLI_UPLOAD_FRAME_CAPACITY", global = true, default_value = client_caps::ClientUploadFrameCapacity::default_value(&()).to_string())]
    pub(crate) upload_frame_capacity: NonZeroUsize,
    /// Outbound restore frames queued before backpressure.
    #[arg(long = client_caps::ClientRestoreFrameCapacity::NAME, env = "NERVIX_CLI_RESTORE_FRAME_CAPACITY", global = true, default_value = client_caps::ClientRestoreFrameCapacity::default_value(&()).to_string())]
    pub(crate) restore_frame_capacity: NonZeroUsize,
}

impl ConnectionArgs {
    pub(crate) fn connect_options(&self) -> ConnectOptions {
        ConnectOptions {
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            retry_timeout: self.retry_timeout,
            backup_wait_timeout: self.backup_wait_timeout,
            retry_backoff: self.retry_backoff,
            retry_max_backoff: self.retry_max_backoff,
            subscription_record_capacity: self.subscription_record_capacity,
            subscription_retained_frames: self.subscription_retained_frames,
            subscriptions_retained_in_full: self.subscriptions_retained_in_full,
            upload_frame_capacity: self.upload_frame_capacity,
            restore_frame_capacity: self.restore_frame_capacity,
            ..ConnectOptions::default()
        }
    }

    #[cfg(test)]
    pub(crate) fn command_line(&self) -> Vec<String> {
        let mut arguments = Vec::new();
        for (name, value) in [
            (
                client_caps::ClientConnectTimeout::NAME,
                humantime::format_duration(self.connect_timeout).to_string(),
            ),
            (
                client_caps::ClientRequestTimeout::NAME,
                humantime::format_duration(self.request_timeout).to_string(),
            ),
            (
                client_caps::ClientRetryTimeout::NAME,
                humantime::format_duration(self.retry_timeout).to_string(),
            ),
            (
                client_caps::ClientBackupWaitTimeout::NAME,
                humantime::format_duration(self.backup_wait_timeout).to_string(),
            ),
            (
                client_caps::ClientRetryBackoff::NAME,
                humantime::format_duration(self.retry_backoff).to_string(),
            ),
            (
                client_caps::ClientRetryMaxBackoff::NAME,
                humantime::format_duration(self.retry_max_backoff).to_string(),
            ),
            (
                client_caps::ClientSubscriptionRecordCapacity::NAME,
                self.subscription_record_capacity.to_string(),
            ),
            (
                client_caps::ClientSubscriptionRetainedFrames::NAME,
                self.subscription_retained_frames.to_string(),
            ),
            (
                client_caps::ClientSubscriptionsRetainedInFull::NAME,
                self.subscriptions_retained_in_full.to_string(),
            ),
            (
                client_caps::ClientUploadFrameCapacity::NAME,
                self.upload_frame_capacity.to_string(),
            ),
            (
                client_caps::ClientRestoreFrameCapacity::NAME,
                self.restore_frame_capacity.to_string(),
            ),
        ] {
            arguments.push(format!("--{name}"));
            arguments.push(value);
        }
        arguments
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_arbitrary::Entropy;

    use super::*;

    #[derive(clap::Parser)]
    struct Options {
        #[command(flatten)]
        connection: ConnectionArgs,
    }

    fn generated(bytes: &[u8]) -> ConnectionArgs {
        let mut entropy = Entropy::new(bytes);
        let mut duration = || {
            Duration::new(
                entropy.between(1..=86_399),
                u32::try_from(entropy.between(0..=999_999_999)).assured("nanoseconds fit u32"),
            )
        };
        let connect_timeout = duration();
        let request_timeout = duration();
        let retry_timeout = duration();
        let backup_wait_timeout = duration();
        let retry_backoff = duration();
        let retry_max_backoff = duration().max(retry_backoff);
        let mut capacity = || {
            NonZeroUsize::new(
                usize::try_from(entropy.boundary_biased(1..=128))
                    .assured("the bounded capacity fits usize"),
            )
            .assured("generated capacities are positive")
        };
        ConnectionArgs {
            connect_timeout,
            request_timeout,
            retry_timeout,
            backup_wait_timeout,
            retry_backoff,
            retry_max_backoff,
            subscription_record_capacity: capacity(),
            subscription_retained_frames: capacity(),
            subscriptions_retained_in_full: capacity(),
            upload_frame_capacity: capacity(),
            restore_frame_capacity: capacity(),
        }
    }

    fn round_trip(connection: ConnectionArgs) {
        let read = Options::try_parse_from(
            std::iter::once("nervix-cli".to_owned()).chain(connection.command_line()),
        )
        .assured("rendered capabilities parse")
        .connection;
        assert_eq!(read, connection);
        let expected =
            nervix_client_core::ClientCapabilities::from_options(&connection.connect_options())
                .assured("generated capabilities validate");
        let actual = nervix_client_core::ClientCapabilities::from_options(&read.connect_options())
            .assured("parsed capabilities validate");
        assert_eq!(actual, expected);
    }

    #[test]
    fn bolero_client_capabilities_survive_their_command_line() {
        bolero::check!()
            .with_iterations(128)
            .with_max_len(128)
            .for_each(|bytes: &[u8]| round_trip(generated(bytes)));
    }

    #[test]
    fn fractional_deadlines_and_capacity_boundaries_survive_the_command_line() {
        for bytes in [
            &[][..],
            &[0; 128][..],
            &[255; 128][..],
            &[1, 127, 255, 0][..],
        ] {
            round_trip(generated(bytes));
        }
    }

    #[test]
    fn help_lists_capabilities_and_invalid_relationships_are_rejected() {
        use clap::CommandFactory as _;
        let help = Options::command().render_long_help().to_string();
        let defaults = Options::try_parse_from(["nervix-cli"])
            .assured("defaults parse")
            .connection;
        let arguments = defaults.command_line();
        for pair in arguments.as_chunks::<2>().0 {
            assert!(help.contains(&pair[0]), "help contains {}", pair[0]);
        }
        let invalid = Options::try_parse_from([
            "nervix-cli",
            "--retry-backoff",
            "2s",
            "--retry-max-backoff",
            "1s",
        ])
        .assured("both durations parse");
        assert!(
            nervix_client_core::ClientCapabilities::from_options(
                &invalid.connection.connect_options()
            )
            .is_err()
        );
        assert!(Options::try_parse_from(["nervix-cli", "--upload-frame-capacity", "0"]).is_err());
    }
}
