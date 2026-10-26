//! WebSockets client configuration values.
//!
//! Layer: vocabulary.
//! - **Owns.** The client's closed key set, endpoint, positive physical timers and inbound
//!   frame/message/handoff limits, and TLS pairing rules, checked before commitment and after
//!   templates are rendered.
//! - **Depends on.** Client configuration entries, the URL parser and standard duration values.
//! - **Must not know.** DNS, sockets, TLS file loading, signaling or runtime scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{
    collections::BTreeMap,
    num::{NonZeroU16, NonZeroU32, ParseIntError},
    str::FromStr,
    time::Duration,
};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use strum::EnumProperty as _;
use thiserror::Error;
use url::Url;

use crate::{ClientConfigEntry, ClientDeadline, DEFAULT_CONNECT_TIMEOUT};

/// Nervix-owned CONFIG keys in their exact lowercase spelling.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    strum::AsRefStr,
    strum::Display,
    strum::EnumIter,
    strum::EnumString,
    strum::EnumProperty,
)]
#[strum(serialize_all = "snake_case")]
pub enum WebsocketsClientKey {
    Endpoint,
    ConnectTimeoutMs,
    PingIntervalMs,
    #[strum(props(default = "30000"))]
    IdleTimeoutMs,
    #[strum(props(default = "16"))]
    HandshakeBufferCapacity,
    #[strum(props(default = "67108864"))]
    MaxMessageSize,
    #[strum(props(default = "16777216"))]
    MaxFrameSize,
    TlsCaFile,
    TlsCertFile,
    TlsKeyFile,
}

/// A configuration refusal names its key without printing endpoint or credential values.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(outcome, reason = "pure WebSockets client configuration validation")
)]
pub enum WebsocketsClientConfigError {
    #[error("'{key}' is not a key this client reads")]
    UndeclaredKey { key: String },
    #[error("'{key}' is set more than once")]
    RepeatedKey { key: WebsocketsClientKey },
    #[error("'endpoint' is required")]
    MissingEndpoint,
    #[error("'endpoint' must be a ws or wss URL with a host and without a fragment")]
    InvalidEndpoint,
    #[error("'{key}' must be positive whole milliseconds within the schedulable duration bound")]
    InvalidDuration { key: WebsocketsClientKey },
    #[error("'{key}' must be a positive decimal integer within its declared range")]
    InvalidLimit { key: WebsocketsClientKey },
    #[error("three '{key}' intervals exceed the schedulable idle deadline")]
    IdleDerivationTooLong { key: WebsocketsClientKey },
    #[error("'{key}' requires a wss endpoint")]
    TlsRequiresSecureEndpoint { key: WebsocketsClientKey },
    #[error("'tls_cert_file' and 'tls_key_file' must be set together")]
    UnpairedTlsIdentity,
}

/// Validated client values. Absence of a ping interval means the client only answers peer pings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebsocketsClientConfig {
    endpoint: Url,
    connect_timeout: Duration,
    ping_interval: Option<Duration>,
    idle_timeout: Duration,
    handshake_buffer_capacity: NonZeroU16,
    max_message_size: NonZeroU32,
    max_frame_size: NonZeroU32,
}

impl WebsocketsClientConfig {
    pub fn parse(
        config: &[ClientConfigEntry],
    ) -> error_stack::Result<Self, WebsocketsClientConfigError> {
        let mut values = BTreeMap::new();
        for entry in config {
            let Ok(key) = WebsocketsClientKey::from_str(&entry.key) else {
                return Err(Report::new(WebsocketsClientConfigError::UndeclaredKey {
                    key: entry.key.clone(),
                }));
            };
            if values.insert(key, entry.value.as_str()).is_some() {
                return Err(Report::new(WebsocketsClientConfigError::RepeatedKey {
                    key,
                }));
            }
        }
        let Some(endpoint) = values.get(&WebsocketsClientKey::Endpoint) else {
            return Err(Report::new(WebsocketsClientConfigError::MissingEndpoint));
        };
        let endpoint =
            Url::parse(endpoint).change_context(WebsocketsClientConfigError::InvalidEndpoint)?;
        if !matches!(endpoint.scheme(), "ws" | "wss")
            || endpoint.host_str().is_none()
            || endpoint.fragment().is_some()
        {
            return Err(Report::new(WebsocketsClientConfigError::InvalidEndpoint));
        }
        for key in [
            WebsocketsClientKey::TlsCaFile,
            WebsocketsClientKey::TlsCertFile,
            WebsocketsClientKey::TlsKeyFile,
        ] {
            if values.contains_key(&key) && endpoint.scheme() != "wss" {
                return Err(Report::new(
                    WebsocketsClientConfigError::TlsRequiresSecureEndpoint { key },
                ));
            }
        }
        if values.contains_key(&WebsocketsClientKey::TlsCertFile)
            != values.contains_key(&WebsocketsClientKey::TlsKeyFile)
        {
            return Err(Report::new(
                WebsocketsClientConfigError::UnpairedTlsIdentity,
            ));
        }
        let connect_timeout = match Self::duration(&values, WebsocketsClientKey::ConnectTimeoutMs)?
        {
            Some(duration) => duration,
            None => DEFAULT_CONNECT_TIMEOUT,
        };
        let ping_interval = Self::duration(&values, WebsocketsClientKey::PingIntervalMs)?;
        let idle_timeout = match Self::duration(&values, WebsocketsClientKey::IdleTimeoutMs)? {
            Some(timeout) => timeout,
            None => match ping_interval {
                Some(period) => {
                    if period > ClientDeadline::LONGEST / 3 {
                        return Err(Report::new(
                            WebsocketsClientConfigError::IdleDerivationTooLong {
                                key: WebsocketsClientKey::PingIntervalMs,
                            },
                        ));
                    }
                    period
                        .checked_mul(3)
                        .verified("three periods fit the checked schedulable deadline bound")
                }
                None => {
                    let default = WebsocketsClientKey::IdleTimeoutMs
                        .get_str("default")
                        .verified("the idle key declares its default milliseconds");
                    ClientDeadline::parse(default)
                        .assured("the declared idle default is positive and schedulable")
                        .duration()
                }
            },
        };
        Ok(Self {
            endpoint,
            connect_timeout,
            ping_interval,
            idle_timeout,
            handshake_buffer_capacity: Self::limit(
                &values,
                WebsocketsClientKey::HandshakeBufferCapacity,
            )?,
            max_message_size: Self::limit(&values, WebsocketsClientKey::MaxMessageSize)?,
            max_frame_size: Self::limit(&values, WebsocketsClientKey::MaxFrameSize)?,
        })
    }

    /// Byte and item keys keep their single defaults beside the key declaration, in CONFIG's
    /// decimal representation. The parsed fields enforce their positive, portable ranges.
    fn limit<T: FromStr<Err = ParseIntError>>(
        values: &BTreeMap<WebsocketsClientKey, &str>,
        key: WebsocketsClientKey,
    ) -> error_stack::Result<T, WebsocketsClientConfigError> {
        let value = match values.get(&key) {
            Some(value) => *value,
            None => key
                .get_str("default")
                .verified("each size or capacity key declares its default"),
        };
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Report::new(WebsocketsClientConfigError::InvalidLimit {
                key,
            }));
        }
        value
            .parse::<T>()
            .change_context(WebsocketsClientConfigError::InvalidLimit { key })
    }

    fn duration(
        values: &BTreeMap<WebsocketsClientKey, &str>,
        key: WebsocketsClientKey,
    ) -> error_stack::Result<Option<Duration>, WebsocketsClientConfigError> {
        let Some(value) = values.get(&key) else {
            return Ok(None);
        };
        let duration = ClientDeadline::parse(value)
            .change_context(WebsocketsClientConfigError::InvalidDuration { key })?;
        Ok(Some(duration.duration()))
    }

    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }
    pub fn ping_interval(&self) -> Option<Duration> {
        self.ping_interval
    }
    /// The explicit idle timeout, three configured ping intervals, or the declared idle default.
    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }
    pub fn handshake_buffer_capacity(&self) -> usize {
        usize::from(self.handshake_buffer_capacity.get())
    }
    pub fn max_message_size(&self) -> usize {
        usize::try_from(self.max_message_size.get())
            .assured("Nervix's native and browser targets have at least 32-bit addresses")
    }
    pub fn max_frame_size(&self) -> usize {
        usize::try_from(self.max_frame_size.get())
            .assured("Nervix's native and browser targets have at least 32-bit addresses")
    }
}
