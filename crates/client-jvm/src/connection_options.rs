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

use std::time::Duration;

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
            subscription_record_capacity: caps::ClientSubscriptionRecordCapacity::default_value(&()),
            subscription_retained_frames: caps::ClientSubscriptionRetainedFrames::default_value(&()),
            subscriptions_retained_in_full: caps::ClientSubscriptionsRetainedInFull::default_value(
                &(),
            ),
            upload_frame_capacity: caps::ClientUploadFrameCapacity::default_value(&()),
            restore_frame_capacity: caps::ClientRestoreFrameCapacity::default_value(&()),
        }
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

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
        match (&native.dns, &draft.dns) {
            (ConnectDns::Configuration(left), ConnectDns::Configuration(right)) => {
                assert_eq!(left, right)
            }
            _ => panic!("the Java draft retains its configured DNS parameters"),
        }
        let defaults = caps::ClientCapabilities::default();
        assert_eq!(native.retry_backoff, defaults.retry_backoff());
        assert_eq!(native.retry_max_backoff, defaults.retry_max_backoff());
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
