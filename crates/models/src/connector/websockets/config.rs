//! WebSockets client configuration.
//!
//! Layer: vocabulary.
//! - **Owns.** The client's declared key schema, and the typed values of a configuration that
//!   satisfies it, with the WebSocket rules the schema cannot state: an endpoint without a
//!   fragment, counts within their representations and a schedulable derived idle deadline.
//! - **Depends on.** The shared client configuration schema and deadline contract, the URL parser
//!   and standard duration values.
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
use thiserror::Error;
use url::Url;

use crate::{
    ClientConfigEntry, ClientConfigKey, ClientConfigSchema, ClientConfigValue, ClientDeadline,
    DEFAULT_CONNECT_TIMEOUT,
};

const ENDPOINT: ClientConfigKey =
    ClientConfigKey::required("endpoint", ClientConfigValue::Url(&["ws", "wss"]));
const HANDSHAKE_BUFFER_CAPACITY: ClientConfigKey = ClientConfigKey::defaulted(
    "handshake_buffer_capacity",
    ClientConfigValue::PositiveInteger,
    "16",
);
const MAX_FRAME_SIZE: ClientConfigKey = ClientConfigKey::defaulted(
    "max_frame_size",
    ClientConfigValue::PositiveInteger,
    "16777216",
);
const MAX_MESSAGE_SIZE: ClientConfigKey = ClientConfigKey::defaulted(
    "max_message_size",
    ClientConfigValue::PositiveInteger,
    "67108864",
);
const PING_INTERVAL: ClientConfigKey =
    ClientConfigKey::optional("ping_interval_ms", ClientConfigValue::Milliseconds);

/// The idle deadline when neither `idle_timeout_ms` nor a ping interval supplies one, in CONFIG's
/// milliseconds. It depends on the ping interval, so the schema cannot declare it as the idle
/// key's default.
const IDLE_TIMEOUT_WITHOUT_PINGS: &str = "30000";

/// The keys a WebSockets client reads, shared by validation, completion, inspection and the typed
/// configuration below.
pub(super) const CLIENT_CONFIG: ClientConfigSchema = ClientConfigSchema::new(
    &[
        ClientConfigKey::CONNECT_TIMEOUT,
        ENDPOINT,
        HANDSHAKE_BUFFER_CAPACITY,
        ClientConfigKey::IDLE_TIMEOUT,
        MAX_FRAME_SIZE,
        MAX_MESSAGE_SIZE,
        PING_INTERVAL,
        ClientConfigKey::TLS_CA.only_for_scheme(ENDPOINT.name, "wss"),
        ClientConfigKey::TLS_CERT.only_for_scheme(ENDPOINT.name, "wss"),
        ClientConfigKey::TLS_KEY.only_for_scheme(ENDPOINT.name, "wss"),
    ],
    &[],
    &[],
);

/// A refusal names its key without printing endpoint or credential values. The schema's own
/// refusal stays beneath `Schema`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(outcome, reason = "pure WebSockets client configuration validation")
)]
pub enum WebsocketsClientConfigError {
    #[error("the configuration does not satisfy the client's declared keys")]
    Schema,
    #[error("'endpoint' must not have a fragment")]
    EndpointFragment,
    #[error("'{key}' must be at most {maximum}")]
    AboveMaximum { key: &'static str, maximum: u32 },
    #[error("three 'ping_interval_ms' intervals exceed the schedulable idle deadline")]
    IdleDerivationTooLong,
}

/// A positive count whose representation sets the largest value a client holds.
trait Count: FromStr<Err = ParseIntError> {
    fn maximum() -> u32;
}

impl Count for NonZeroU16 {
    fn maximum() -> u32 {
        u32::from(Self::MAX.get())
    }
}

impl Count for NonZeroU32 {
    fn maximum() -> u32 {
        Self::MAX.get()
    }
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
    /// Holds `config` to the declared schema, then converts the effective values once.
    pub fn parse(
        config: &[ClientConfigEntry],
    ) -> error_stack::Result<Self, WebsocketsClientConfigError> {
        CLIENT_CONFIG
            .validate_rendered(config)
            .change_context(WebsocketsClientConfigError::Schema)?;
        let effective = CLIENT_CONFIG.effective_entries(config);
        // The schema refused repeated keys above, so each key maps to its one value.
        let mut values = BTreeMap::new();
        for entry in &effective {
            values.insert(entry.key.as_str(), entry.value.as_str());
        }
        let endpoint = values
            .get(ENDPOINT.name)
            .verified("the schema requires an endpoint");
        let endpoint = Url::parse(endpoint).verified("the schema parsed the endpoint as a URL");
        if endpoint.fragment().is_some() {
            return Err(Report::new(WebsocketsClientConfigError::EndpointFragment));
        }
        let connect_timeout = match Self::duration(&values, ClientConfigKey::CONNECT_TIMEOUT) {
            Some(timeout) => timeout,
            None => DEFAULT_CONNECT_TIMEOUT,
        };
        let ping_interval = Self::duration(&values, PING_INTERVAL);
        let idle_timeout = match Self::duration(&values, ClientConfigKey::IDLE_TIMEOUT) {
            Some(timeout) => timeout,
            None => Self::derived_idle_timeout(ping_interval)?,
        };
        Ok(Self {
            endpoint,
            connect_timeout,
            ping_interval,
            idle_timeout,
            handshake_buffer_capacity: Self::count(&values, HANDSHAKE_BUFFER_CAPACITY)?,
            max_message_size: Self::count(&values, MAX_MESSAGE_SIZE)?,
            max_frame_size: Self::count(&values, MAX_FRAME_SIZE)?,
        })
    }

    /// Three configured ping intervals, or the idle deadline without pings.
    fn derived_idle_timeout(
        ping_interval: Option<Duration>,
    ) -> error_stack::Result<Duration, WebsocketsClientConfigError> {
        let Some(period) = ping_interval else {
            let default = ClientDeadline::parse(IDLE_TIMEOUT_WITHOUT_PINGS)
                .assured("the idle deadline without pings is positive and schedulable");
            return Ok(default.duration());
        };
        if period > ClientDeadline::LONGEST / 3 {
            return Err(Report::new(
                WebsocketsClientConfigError::IdleDerivationTooLong,
            ));
        }
        let derived = period
            .checked_mul(3)
            .verified("three periods fit the checked schedulable deadline bound");
        Ok(derived)
    }

    /// The schema admitted a positive decimal count, so only a count above what the
    /// representation holds is refused here.
    fn count<T: Count>(
        values: &BTreeMap<&str, &str>,
        key: ClientConfigKey,
    ) -> error_stack::Result<T, WebsocketsClientConfigError> {
        let value = values
            .get(key.name)
            .verified("effective entries fill every defaulted count");
        value
            .parse::<T>()
            .change_context(WebsocketsClientConfigError::AboveMaximum {
                key: key.name,
                maximum: T::maximum(),
            })
    }

    fn duration(values: &BTreeMap<&str, &str>, key: ClientConfigKey) -> Option<Duration> {
        let value = values.get(key.name)?;
        let deadline =
            ClientDeadline::parse(value).verified("the schema admitted schedulable milliseconds");
        Some(deadline.duration())
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
    /// The explicit idle timeout, three configured ping intervals, or the idle deadline without
    /// pings.
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
