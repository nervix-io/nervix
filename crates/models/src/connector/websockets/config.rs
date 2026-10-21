//! WebSockets client configuration values.
//!
//! Layer: vocabulary.
//! - **Owns.** The client's closed key set, endpoint and positive physical timer values, and TLS
//!   pairing rules, checked before a client is committed and after its templates are rendered.
//! - **Depends on.** Client configuration entries, the URL parser and standard duration values.
//! - **Must not know.** DNS, sockets, TLS file loading, signaling or runtime scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::{collections::BTreeMap, str::FromStr as _, time::Duration};

use error_stack::{Report, ResultExt as _};
use thiserror::Error;
use url::Url;

use crate::{ClientConfigEntry, ClientDeadline, DEFAULT_CONNECT_TIMEOUT};

/// Nervix-owned CONFIG keys, matched without regard to ASCII case.
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
)]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
pub enum WebsocketsClientKey {
    Endpoint,
    ConnectTimeoutMs,
    PingIntervalMs,
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
}

impl WebsocketsClientConfig {
    /// The existing physical DNS, TCP and upgrade budget where no value is written.
    pub const CONNECT_TIMEOUT: Duration = DEFAULT_CONNECT_TIMEOUT;

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
            None => Self::CONNECT_TIMEOUT,
        };
        let ping_interval = Self::duration(&values, WebsocketsClientKey::PingIntervalMs)?;
        Ok(Self {
            endpoint,
            connect_timeout,
            ping_interval,
        })
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
}
