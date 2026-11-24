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

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use nervix_client_core::{ConnectOptions, capabilities as client_caps};
use nervix_models::SettingDefinition as _;

#[derive(clap::Args, Debug, Clone, PartialEq, Eq)]
#[command(next_help_heading = "Connection capabilities")]
pub(crate) struct ConnectionArgs {
    /// Deadline of each connection attempt.
    #[arg(long = client_caps::ClientConnectTimeout::NAME, env = "NERVIX_CLI_CONNECT_TIMEOUT", global = true, default_value = humantime::format_duration(client_caps::ClientConnectTimeout::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) connect_timeout: Duration,
    /// How long each request may go without its reply or a server progress report.
    #[arg(long = client_caps::ClientRequestTimeout::NAME, env = "NERVIX_CLI_REQUEST_TIMEOUT", global = true, default_value = humantime::format_duration(client_caps::ClientRequestTimeout::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) request_timeout: Duration,
    /// How long each call may go, across redirects and reconnects, without its answer or a server
    /// progress report.
    #[arg(long = client_caps::ClientRetryTimeout::NAME, env = "NERVIX_CLI_RETRY_TIMEOUT", global = true, default_value = humantime::format_duration(client_caps::ClientRetryTimeout::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) retry_timeout: Duration,
    /// How long a backup may go, across cuts, redirects and reconnects, without its answer or a
    /// server progress report.
    #[arg(long = client_caps::ClientBackupWaitTimeout::NAME, env = "NERVIX_CLI_BACKUP_WAIT_TIMEOUT", global = true, default_value = humantime::format_duration(client_caps::ClientBackupWaitTimeout::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) backup_wait_timeout: Duration,
    /// Initial reconnect and recovery delay.
    #[arg(long = client_caps::ClientRetryBackoff::NAME, env = "NERVIX_CLI_RETRY_BACKOFF", global = true, default_value = humantime::format_duration(client_caps::ClientRetryBackoff::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) retry_backoff: Duration,
    /// Ceiling for doubled reconnect and recovery delays.
    #[arg(long = client_caps::ClientRetryMaxBackoff::NAME, env = "NERVIX_CLI_RETRY_MAX_BACKOFF", global = true, default_value = humantime::format_duration(client_caps::ClientRetryMaxBackoff::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) retry_max_backoff: Duration,
    /// First wait before a restoration the new session refused is sent again on it.
    #[arg(long = client_caps::ClientRestorationBackoff::NAME, env = "NERVIX_CLI_RESTORATION_BACKOFF", global = true, default_value = humantime::format_duration(client_caps::ClientRestorationBackoff::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) restoration_backoff: Duration,
    /// Ceiling for doubled restoration waits.
    #[arg(long = client_caps::ClientRestorationMaxBackoff::NAME, env = "NERVIX_CLI_RESTORATION_MAX_BACKOFF", global = true, default_value = humantime::format_duration(client_caps::ClientRestorationMaxBackoff::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) restoration_max_backoff: Duration,
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
    /// Outbound request frames of one session queued before backpressure.
    #[arg(long = client_caps::ClientRequestFrameCapacity::NAME, env = "NERVIX_CLI_REQUEST_FRAME_CAPACITY", global = true, default_value = client_caps::ClientRequestFrameCapacity::default_value(&()).to_string())]
    pub(crate) request_frame_capacity: NonZeroUsize,
    /// Server notices retained until they are read.
    #[arg(long = client_caps::ClientNoticeCapacity::NAME, env = "NERVIX_CLI_NOTICE_CAPACITY", global = true, default_value = client_caps::ClientNoticeCapacity::default_value(&()).to_string())]
    pub(crate) notice_capacity: NonZeroUsize,
    /// Bytes of server notices retained until they are read, each notice charged its text and
    /// in-memory header.
    #[arg(long = client_caps::ClientNoticeBytes::NAME, env = "NERVIX_CLI_NOTICE_BYTES", global = true, default_value = client_caps::ClientNoticeBytes::default_value(&()).to_string())]
    pub(crate) notice_bytes: NonZeroUsize,
    /// How long a connection with a call open may go without hearing from the node before it is
    /// pinged.
    #[arg(long = client_caps::ClientKeepaliveInterval::NAME, env = "NERVIX_CLI_KEEPALIVE_INTERVAL", global = true, default_value = humantime::format_duration(client_caps::ClientKeepaliveInterval::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) keepalive_interval: Duration,
    /// How long that ping may go unanswered before the connection is lost and recovered.
    #[arg(long = client_caps::ClientKeepaliveTimeout::NAME, env = "NERVIX_CLI_KEEPALIVE_TIMEOUT", global = true, default_value = humantime::format_duration(client_caps::ClientKeepaliveTimeout::default_value(&())).to_string(), value_parser = nervix_models::parse_duration_text)]
    pub(crate) keepalive_timeout: Duration,
    /// Bytes of one call the node may send before they are read.
    #[arg(long = client_caps::ClientStreamWindowBytes::NAME, env = "NERVIX_CLI_STREAM_WINDOW_BYTES", global = true, default_value = client_caps::ClientStreamWindowBytes::default_value(&()).to_string())]
    pub(crate) stream_window_bytes: NonZeroU32,
    /// Bytes of all calls on one connection the node may send before they are read.
    #[arg(long = client_caps::ClientConnectionWindowBytes::NAME, env = "NERVIX_CLI_CONNECTION_WINDOW_BYTES", global = true, default_value = client_caps::ClientConnectionWindowBytes::default_value(&()).to_string())]
    pub(crate) connection_window_bytes: NonZeroU32,
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
            restoration_backoff: self.restoration_backoff,
            restoration_max_backoff: self.restoration_max_backoff,
            subscription_record_capacity: self.subscription_record_capacity,
            subscription_retained_frames: self.subscription_retained_frames,
            subscriptions_retained_in_full: self.subscriptions_retained_in_full,
            upload_frame_capacity: self.upload_frame_capacity,
            restore_frame_capacity: self.restore_frame_capacity,
            request_frame_capacity: self.request_frame_capacity,
            notice_capacity: self.notice_capacity,
            notice_bytes: self.notice_bytes,
            keepalive_interval: self.keepalive_interval,
            keepalive_timeout: self.keepalive_timeout,
            stream_window_bytes: self.stream_window_bytes,
            connection_window_bytes: self.connection_window_bytes,
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
                client_caps::ClientRestorationBackoff::NAME,
                humantime::format_duration(self.restoration_backoff).to_string(),
            ),
            (
                client_caps::ClientRestorationMaxBackoff::NAME,
                humantime::format_duration(self.restoration_max_backoff).to_string(),
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
            (
                client_caps::ClientRequestFrameCapacity::NAME,
                self.request_frame_capacity.to_string(),
            ),
            (
                client_caps::ClientNoticeCapacity::NAME,
                self.notice_capacity.to_string(),
            ),
            (
                client_caps::ClientNoticeBytes::NAME,
                self.notice_bytes.to_string(),
            ),
            (
                client_caps::ClientKeepaliveInterval::NAME,
                humantime::format_duration(self.keepalive_interval).to_string(),
            ),
            (
                client_caps::ClientKeepaliveTimeout::NAME,
                humantime::format_duration(self.keepalive_timeout).to_string(),
            ),
            (
                client_caps::ClientStreamWindowBytes::NAME,
                self.stream_window_bytes.to_string(),
            ),
            (
                client_caps::ClientConnectionWindowBytes::NAME,
                self.connection_window_bytes.to_string(),
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
        let restoration_backoff = duration();
        let restoration_max_backoff = duration().max(restoration_backoff);
        let keepalive_interval = duration();
        let keepalive_timeout = duration();
        let mut capacity = || {
            NonZeroUsize::new(
                usize::try_from(entropy.boundary_biased(1..=128))
                    .assured("the bounded capacity fits usize"),
            )
            .assured("generated capacities are positive")
        };
        let subscription_record_capacity = capacity();
        let subscription_retained_frames = capacity();
        let subscriptions_retained_in_full = capacity();
        let upload_frame_capacity = capacity();
        let restore_frame_capacity = capacity();
        let request_frame_capacity = capacity();
        let notice_capacity = capacity();
        // Every positive notice byte bound is valid, so the bound reaches the largest usize.
        let largest_bytes =
            u64::try_from(usize::MAX).assured("supported targets address at most 64 bits");
        let notice_bytes = NonZeroUsize::new(
            usize::try_from(entropy.boundary_biased(1..=largest_bytes))
                .verified("the drawn bound is at most usize::MAX"),
        )
        .assured("generated byte bounds are positive");
        // Every window from the smallest each takes through the HTTP/2 maximum is valid.
        let largest_window = u64::from(client_caps::LARGEST_RECEIVE_WINDOW_BYTES);
        let mut window = |smallest: u32| {
            let bytes = entropy.boundary_biased(u64::from(smallest)..=largest_window);
            NonZeroU32::new(u32::try_from(bytes).assured("the bounded window fits u32"))
                .assured("every generated window is at least one byte")
        };
        let stream_window_bytes = window(NonZeroU32::MIN.get());
        let connection_window_bytes = window(client_caps::INITIAL_CONNECTION_WINDOW_BYTES);
        ConnectionArgs {
            connect_timeout,
            request_timeout,
            retry_timeout,
            backup_wait_timeout,
            retry_backoff,
            retry_max_backoff,
            restoration_backoff,
            restoration_max_backoff,
            subscription_record_capacity,
            subscription_retained_frames,
            subscriptions_retained_in_full,
            upload_frame_capacity,
            restore_frame_capacity,
            request_frame_capacity,
            notice_capacity,
            notice_bytes,
            keepalive_interval,
            keepalive_timeout,
            stream_window_bytes,
            connection_window_bytes,
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
        // The projection carries every flag into its own capability.
        assert_eq!(actual.connect_timeout(), connection.connect_timeout);
        assert_eq!(actual.request_timeout(), connection.request_timeout);
        assert_eq!(actual.retry_timeout(), connection.retry_timeout);
        assert_eq!(actual.backup_wait_timeout(), connection.backup_wait_timeout);
        assert_eq!(actual.retry_backoff(), connection.retry_backoff);
        assert_eq!(actual.retry_max_backoff(), connection.retry_max_backoff);
        assert_eq!(actual.restoration_backoff(), connection.restoration_backoff);
        assert_eq!(
            actual.restoration_max_backoff(),
            connection.restoration_max_backoff
        );
        assert_eq!(
            actual.subscription_record_capacity(),
            connection.subscription_record_capacity
        );
        assert_eq!(
            actual.subscription_retained_frames(),
            connection.subscription_retained_frames
        );
        assert_eq!(
            actual.subscriptions_retained_in_full(),
            connection.subscriptions_retained_in_full
        );
        assert_eq!(
            actual.upload_frame_capacity(),
            connection.upload_frame_capacity
        );
        assert_eq!(
            actual.restore_frame_capacity(),
            connection.restore_frame_capacity
        );
        assert_eq!(
            actual.request_frame_capacity(),
            connection.request_frame_capacity
        );
        assert_eq!(actual.notice_capacity(), connection.notice_capacity);
        assert_eq!(actual.notice_bytes(), connection.notice_bytes);
        assert_eq!(actual.keepalive_interval(), connection.keepalive_interval);
        assert_eq!(actual.keepalive_timeout(), connection.keepalive_timeout);
        assert_eq!(actual.stream_window_bytes(), connection.stream_window_bytes);
        assert_eq!(
            actual.connection_window_bytes(),
            connection.connection_window_bytes
        );
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
        for (base, ceiling) in [
            ("--retry-backoff", "--retry-max-backoff"),
            ("--restoration-backoff", "--restoration-max-backoff"),
        ] {
            let invalid = Options::try_parse_from(["nervix-cli", base, "2s", ceiling, "1s"])
                .assured("both durations parse");
            assert!(
                nervix_client_core::ClientCapabilities::from_options(
                    &invalid.connection.connect_options()
                )
                .is_err(),
                "{base} above {ceiling} is refused"
            );
        }
        for capacity in [
            "--upload-frame-capacity",
            "--request-frame-capacity",
            "--notice-capacity",
            "--notice-bytes",
        ] {
            assert!(
                Options::try_parse_from(["nervix-cli", capacity, "0"]).is_err(),
                "{capacity} refuses zero"
            );
        }
        assert!(Options::try_parse_from(["nervix-cli", "--stream-window-bytes", "0"]).is_err());
        let narrow = Options::try_parse_from(["nervix-cli", "--connection-window-bytes", "65534"])
            .assured("a positive window parses");
        assert!(
            nervix_client_core::ClientCapabilities::from_options(
                &narrow.connection.connect_options()
            )
            .is_err(),
            "a connection window below the initial HTTP/2 window is refused before connecting"
        );
    }

    /// Without flags the CLI takes the native client's keepalive and windows, so it pings a silent
    /// node like every other client.
    #[test]
    fn the_transport_defaults_are_the_native_clients() {
        let defaults = Options::try_parse_from(["nervix-cli"])
            .assured("defaults parse")
            .connection
            .connect_options();
        let native = ConnectOptions::default();
        assert_eq!(defaults.keepalive_interval, native.keepalive_interval);
        assert_eq!(defaults.keepalive_timeout, native.keepalive_timeout);
        assert_eq!(defaults.stream_window_bytes, native.stream_window_bytes);
        assert_eq!(
            defaults.connection_window_bytes,
            native.connection_window_bytes
        );
    }

    #[test]
    fn new_capability_flags_default_to_their_definitions() {
        let defaults = Options::try_parse_from(["nervix-cli"])
            .assured("defaults parse")
            .connection;
        let options = defaults.connect_options();
        assert_eq!(options.restoration_backoff, Duration::from_secs(1));
        assert_eq!(options.restoration_max_backoff, Duration::from_secs(30));
        assert_eq!(options.request_frame_capacity.get(), 32);
        assert_eq!(options.notice_capacity.get(), 128);
        assert_eq!(options.notice_bytes.get(), 1024 * 1024);
    }
}
