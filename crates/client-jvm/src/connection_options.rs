//! The current Java connection-options draft and its native argument projection.
//!
//! Layer: edges.
//! - **Owns.** The options Java exposes and their conversion at the native connect boundary.
//! - **Depends on.** Native client option types and their setting definitions.
//! - **Must not know.** JNI handles, monitors, connection recovery or the client implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns Java connection arguments")
)]

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use nervix_client_core::{ConnectDns, ConnectOptions, TlsRequirement, capabilities as caps};
use nervix_models::SettingDefinition as _;
use url::Url;

/// A mutable Java draft; native capabilities Java does not expose are resolved at projection.
#[derive(Clone)]
pub(crate) struct ConnectionOptions {
    dns: ConnectDns,
    pub(crate) tls_requirement: Option<TlsRequirement>,
    pub(crate) ca_certificate_pem: Option<Vec<u8>>,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) seed_servers: Vec<Url>,
    pub(crate) connect_timeout: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) retry_timeout: Duration,
    pub(crate) backup_wait_timeout: Duration,
    pub(crate) keepalive_interval: Duration,
    pub(crate) keepalive_timeout: Duration,
    pub(crate) stream_window_bytes: NonZeroU32,
    pub(crate) connection_window_bytes: NonZeroU32,
    pub(crate) call_queue_capacity: NonZeroUsize,
    pub(crate) header_list_bytes: NonZeroU32,
    pub(crate) send_buffer_bytes: NonZeroU32,
    pub(crate) max_error_reset_streams: NonZeroU32,
    pub(crate) remembered_reset_streams: NonZeroU32,
    pub(crate) reset_stream_duration: Duration,
    pub(crate) connection_attempt_delay: Duration,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        let options = ConnectOptions::default();
        Self {
            dns: options.dns,
            tls_requirement: options.tls_requirement,
            ca_certificate_pem: options.ca_certificate_pem,
            username: options.username,
            password: options.password,
            seed_servers: options.seed_servers,
            connect_timeout: options.connect_timeout,
            request_timeout: options.request_timeout,
            retry_timeout: options.retry_timeout,
            backup_wait_timeout: options.backup_wait_timeout,
            keepalive_interval: options.keepalive_interval,
            keepalive_timeout: options.keepalive_timeout,
            stream_window_bytes: options.stream_window_bytes,
            connection_window_bytes: options.connection_window_bytes,
            call_queue_capacity: options.call_queue_capacity,
            header_list_bytes: options.header_list_bytes,
            send_buffer_bytes: options.send_buffer_bytes,
            max_error_reset_streams: options.max_error_reset_streams,
            remembered_reset_streams: options.remembered_reset_streams,
            reset_stream_duration: options.reset_stream_duration,
            connection_attempt_delay: options.connection_attempt_delay,
        }
    }
}

impl ConnectionOptions {
    /// Moves the draft's owned parameters without constructing another DNS configuration.
    pub(crate) fn into_native(self) -> ConnectOptions {
        ConnectOptions {
            dns: self.dns,
            tls_requirement: self.tls_requirement,
            ca_certificate_pem: self.ca_certificate_pem,
            username: self.username,
            password: self.password,
            seed_servers: self.seed_servers,
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            retry_timeout: self.retry_timeout,
            backup_wait_timeout: self.backup_wait_timeout,
            retry_backoff: caps::ClientRetryBackoff::default_value(&()),
            retry_max_backoff: caps::ClientRetryMaxBackoff::default_value(&()),
            restoration_backoff: caps::ClientRestorationBackoff::default_value(&()),
            restoration_max_backoff: caps::ClientRestorationMaxBackoff::default_value(&()),
            subscription_record_capacity: caps::ClientSubscriptionRecordCapacity::default_value(&()),
            subscription_retained_frames: caps::ClientSubscriptionRetainedFrames::default_value(&()),
            subscriptions_retained_in_full: caps::ClientSubscriptionsRetainedInFull::default_value(
                &(),
            ),
            upload_frame_capacity: caps::ClientUploadFrameCapacity::default_value(&()),
            restore_frame_capacity: caps::ClientRestoreFrameCapacity::default_value(&()),
            request_frame_capacity: caps::ClientRequestFrameCapacity::default_value(&()),
            notice_capacity: caps::ClientNoticeCapacity::default_value(&()),
            notice_bytes: caps::ClientNoticeBytes::default_value(&()),
            keepalive_interval: self.keepalive_interval,
            keepalive_timeout: self.keepalive_timeout,
            stream_window_bytes: self.stream_window_bytes,
            connection_window_bytes: self.connection_window_bytes,
            call_queue_capacity: self.call_queue_capacity,
            header_list_bytes: self.header_list_bytes,
            send_buffer_bytes: self.send_buffer_bytes,
            max_error_reset_streams: self.max_error_reset_streams,
            remembered_reset_streams: self.remembered_reset_streams,
            reset_stream_duration: self.reset_stream_duration,
            connection_attempt_delay: self.connection_attempt_delay,
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    #[test]
    fn java_drafts_preserve_exposed_options_and_keep_native_capabilities_out_of_handles() {
        let draft = ConnectionOptions {
            tls_requirement: Some(TlsRequirement::Required),
            ca_certificate_pem: Some(vec![1, 2, 3]),
            username: Some("operator".to_owned()),
            password: Some("credential".to_owned()),
            seed_servers: vec![Url::parse("https://peer.example:9000").assured("a static origin")],
            connect_timeout: Duration::from_millis(1_125),
            request_timeout: Duration::from_millis(2_250),
            retry_timeout: Duration::from_millis(3_375),
            backup_wait_timeout: Duration::from_millis(4_500),
            keepalive_interval: Duration::from_millis(5_625),
            keepalive_timeout: Duration::from_millis(6_750),
            stream_window_bytes: NonZeroU32::new(1_000).assured("a positive window"),
            connection_window_bytes: NonZeroU32::new(300_000).assured("a positive window"),
            call_queue_capacity: NonZeroUsize::new(3).assured("a positive queue"),
            header_list_bytes: NonZeroU32::new(8_192).assured("a positive bound"),
            send_buffer_bytes: NonZeroU32::new(65_536).assured("a positive bound"),
            max_error_reset_streams: NonZeroU32::new(7).assured("a positive count"),
            remembered_reset_streams: NonZeroU32::new(9).assured("a positive count"),
            reset_stream_duration: Duration::from_millis(7_875),
            connection_attempt_delay: Duration::from_millis(250),
            ..ConnectionOptions::default()
        };
        let native = draft.clone().into_native();
        assert_eq!(native.tls_requirement, draft.tls_requirement);
        assert_eq!(native.ca_certificate_pem, draft.ca_certificate_pem);
        assert_eq!(native.username, draft.username);
        assert_eq!(native.password, draft.password);
        assert_eq!(native.seed_servers, draft.seed_servers);
        assert_eq!(native.connect_timeout, draft.connect_timeout);
        assert_eq!(native.request_timeout, draft.request_timeout);
        assert_eq!(native.retry_timeout, draft.retry_timeout);
        assert_eq!(native.backup_wait_timeout, draft.backup_wait_timeout);
        assert_eq!(native.keepalive_interval, draft.keepalive_interval);
        assert_eq!(native.keepalive_timeout, draft.keepalive_timeout);
        assert_eq!(native.stream_window_bytes, draft.stream_window_bytes);
        assert_eq!(
            native.connection_window_bytes,
            draft.connection_window_bytes
        );
        assert_eq!(native.call_queue_capacity, draft.call_queue_capacity);
        assert_eq!(native.header_list_bytes, draft.header_list_bytes);
        assert_eq!(native.send_buffer_bytes, draft.send_buffer_bytes);
        assert_eq!(
            native.max_error_reset_streams,
            draft.max_error_reset_streams
        );
        assert_eq!(
            native.remembered_reset_streams,
            draft.remembered_reset_streams
        );
        assert_eq!(native.reset_stream_duration, draft.reset_stream_duration);
        assert_eq!(
            native.connection_attempt_delay,
            draft.connection_attempt_delay
        );
        match (&native.dns, &draft.dns) {
            (ConnectDns::Configuration(left), ConnectDns::Configuration(right)) => {
                assert_eq!(left, right)
            }
            _ => panic!("the Java draft retains its configured DNS parameters"),
        }
        let defaults = caps::ClientCapabilities::default();
        assert_eq!(native.retry_backoff, defaults.retry_backoff());
        assert_eq!(native.retry_max_backoff, defaults.retry_max_backoff());
        assert_eq!(native.restoration_backoff, defaults.restoration_backoff());
        assert_eq!(
            native.restoration_max_backoff,
            defaults.restoration_max_backoff()
        );
        assert_eq!(
            native.subscription_record_capacity,
            defaults.subscription_record_capacity()
        );
        assert_eq!(
            native.subscription_retained_frames,
            defaults.subscription_retained_frames()
        );
        assert_eq!(
            native.subscriptions_retained_in_full,
            defaults.subscriptions_retained_in_full()
        );
        assert_eq!(
            native.upload_frame_capacity,
            defaults.upload_frame_capacity()
        );
        assert_eq!(
            native.restore_frame_capacity,
            defaults.restore_frame_capacity()
        );
        assert_eq!(
            native.request_frame_capacity,
            defaults.request_frame_capacity()
        );
        assert_eq!(native.notice_capacity, defaults.notice_capacity());
        assert_eq!(native.notice_bytes, defaults.notice_bytes());
        assert!(
            std::mem::size_of::<crate::handles::Value>() < std::mem::size_of::<ConnectOptions>()
        );
        eprintln!(
            "Java handle bytes: {}; native options bytes: {}",
            std::mem::size_of::<crate::handles::Value>(),
            std::mem::size_of::<ConnectOptions>()
        );
    }
}
