//! The deadline keys a client sets, read with the one meaning every connector gives them.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading `timeout_ms`, `connect_timeout_ms` and `idle_timeout_ms` from a client's
//!   entries, each held to the vocabulary's one deadline rule, the default a connection takes
//!   without its key, and the typed failure of a wait that ran out of its deadline.
//! - **Depends on.** The vocabulary's deadline keys, rule and default, and a client's
//!   configuration entries.
//! - **Must not know.** Which connector reads a key, or what its wait is for.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::time::Duration;

use error_stack::ResultExt as _;
use nervix_models::{ClientDeadline, DEFAULT_CONNECT_TIMEOUT, DeadlineKey};
use thiserror::Error;

use crate::client_config::{ClientConfigError, ClientConfigResult, optional_client_config_value};

/// One deadline a client's key sets on the waits it bounds, with the key it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfiguredDeadline {
    key: DeadlineKey,
    bound: Duration,
}

impl ConfiguredDeadline {
    /// The client's `timeout_ms`, where it sets one: the whole of one request or operation, name
    /// resolution, connection setup, TLS and the answer included.
    pub fn timeout(
        entries: &[nervix_models::ClientConfigEntry],
    ) -> ClientConfigResult<Option<Self>> {
        Self::read(entries, DeadlineKey::Timeout)
    }

    /// The client's `connect_timeout_ms`, or [`DEFAULT_CONNECT_TIMEOUT`] where it leaves the key
    /// out: establishing a connection, name resolution, the address attempts, TLS and the
    /// protocol's own handshake included.
    pub fn connect(entries: &[nervix_models::ClientConfigEntry]) -> ClientConfigResult<Self> {
        let configured = Self::read(entries, DeadlineKey::ConnectTimeout)?;
        match configured {
            Some(deadline) => Ok(deadline),
            None => Ok(Self {
                key: DeadlineKey::ConnectTimeout,
                bound: DEFAULT_CONNECT_TIMEOUT,
            }),
        }
    }

    /// The client's `idle_timeout_ms`, where it sets one: how long a connection may carry nothing
    /// while its connector waits to read from it, after which the connector closes it.
    pub fn idle(entries: &[nervix_models::ClientConfigEntry]) -> ClientConfigResult<Option<Self>> {
        Self::read(entries, DeadlineKey::IdleTimeout)
    }

    /// The client's `pool_acquire_timeout_ms`, where it sets one: one checkout from a connection
    /// pool, the wait for the pool and opening or testing the connection it hands over included.
    pub fn pool_acquire(
        entries: &[nervix_models::ClientConfigEntry],
    ) -> ClientConfigResult<Option<Self>> {
        Self::read(entries, DeadlineKey::PoolAcquireTimeout)
    }

    fn read(
        entries: &[nervix_models::ClientConfigEntry],
        key: DeadlineKey,
    ) -> ClientConfigResult<Option<Self>> {
        let Some(value) = optional_client_config_value(entries, key.key()) else {
            return Ok(None);
        };
        let deadline = ClientDeadline::parse(value)
            .change_context(ClientConfigError::InvalidDeadline { key: key.key() })?;
        Ok(Some(Self {
            key,
            bound: deadline.duration(),
        }))
    }

    /// The key that set this deadline.
    pub fn key(self) -> DeadlineKey {
        self.key
    }

    /// How long the waits this deadline bounds may take.
    pub fn bound(self) -> Duration {
        self.bound
    }

    /// The failure of `operation`, which ran out of this deadline.
    pub fn elapsed(self, operation: &'static str) -> DeadlineElapsed {
        DeadlineElapsed {
            operation,
            key: self.key,
            bound: self.bound,
        }
    }
}

/// A wait that one of a client's deadline keys bounds ran out of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error(
    "{operation} did not finish within {key} {bound}",
    key = .key.key(),
    bound = humantime::format_duration(*.bound)
)]
pub struct DeadlineElapsed {
    /// What did not finish, as a report names it: "the TLS handshake with 'broker.internal'".
    pub operation: &'static str,
    pub key: DeadlineKey,
    pub bound: Duration,
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_models::ClientConfigEntry;

    use super::*;

    fn entries(pairs: &[(&str, &str)]) -> Vec<ClientConfigEntry> {
        pairs
            .iter()
            .map(|(key, value)| ClientConfigEntry {
                key: (*key).to_string(),
                value: (*value).to_string(),
            })
            .collect()
    }

    #[test]
    fn a_client_without_timeout_ms_sets_no_request_deadline() {
        let timeout = ConfiguredDeadline::timeout(&entries(&[("addr", "http://127.0.0.1")]))
            .assured("a client without the key is valid");
        assert_eq!(timeout, None);
    }

    #[test]
    fn each_key_is_read_with_its_own_meaning() {
        let settings = entries(&[
            ("timeout_ms", "1500"),
            ("connect_timeout_ms", "250"),
            ("idle_timeout_ms", "60000"),
            ("pool_acquire_timeout_ms", "750"),
        ]);
        let timeout = ConfiguredDeadline::timeout(&settings)
            .assured("1500 ms is a valid deadline")
            .assured("the client sets timeout_ms");
        assert_eq!(timeout.key(), DeadlineKey::Timeout);
        assert_eq!(timeout.bound(), Duration::from_millis(1500));
        let connect = ConfiguredDeadline::connect(&settings).assured("250 ms is a valid deadline");
        assert_eq!(connect.key(), DeadlineKey::ConnectTimeout);
        assert_eq!(connect.bound(), Duration::from_millis(250));
        let idle = ConfiguredDeadline::idle(&settings)
            .assured("60000 ms is a valid deadline")
            .assured("the client sets idle_timeout_ms");
        assert_eq!(idle.key(), DeadlineKey::IdleTimeout);
        assert_eq!(idle.bound(), Duration::from_secs(60));
        let checkout = ConfiguredDeadline::pool_acquire(&settings)
            .assured("750 ms is a valid deadline")
            .assured("the client sets pool_acquire_timeout_ms");
        assert_eq!(checkout.key(), DeadlineKey::PoolAcquireTimeout);
        assert_eq!(checkout.bound(), Duration::from_millis(750));
    }

    #[test]
    fn a_client_without_idle_timeout_ms_never_closes_an_idle_connection() {
        let idle = ConfiguredDeadline::idle(&entries(&[("connect_timeout_ms", "250")]))
            .assured("a client without the key is valid");
        assert_eq!(idle, None);
    }

    #[test]
    fn a_connection_without_connect_timeout_ms_takes_the_default() {
        let connect = ConfiguredDeadline::connect(&entries(&[("timeout_ms", "1500")]))
            .assured("a client without the key takes the default");
        assert_eq!(connect.key(), DeadlineKey::ConnectTimeout);
        assert_eq!(connect.bound(), DEFAULT_CONNECT_TIMEOUT);
    }

    #[test]
    fn a_value_that_breaks_the_rule_names_its_key() {
        for value in ["0", "-1", "1.5", "1s", " 10", "", "99999999999999999999999"] {
            let timeout = ConfiguredDeadline::timeout(&entries(&[("timeout_ms", value)]))
                .expect_err("the value breaks the deadline rule");
            assert!(
                matches!(
                    timeout.current_context(),
                    ClientConfigError::InvalidDeadline { key: "timeout_ms" }
                ),
                "{value:?}: {timeout:?}"
            );
            let connect = ConfiguredDeadline::connect(&entries(&[("connect_timeout_ms", value)]))
                .expect_err("the value breaks the deadline rule");
            assert!(
                matches!(
                    connect.current_context(),
                    ClientConfigError::InvalidDeadline {
                        key: "connect_timeout_ms"
                    }
                ),
                "{value:?}: {connect:?}"
            );
            let idle = ConfiguredDeadline::idle(&entries(&[("idle_timeout_ms", value)]))
                .expect_err("the value breaks the deadline rule");
            assert!(
                matches!(
                    idle.current_context(),
                    ClientConfigError::InvalidDeadline {
                        key: "idle_timeout_ms"
                    }
                ),
                "{value:?}: {idle:?}"
            );
        }
    }

    #[test]
    fn an_elapsed_deadline_names_the_operation_the_key_and_the_bound() {
        let connect = ConfiguredDeadline::connect(&entries(&[("connect_timeout_ms", "1000")]))
            .assured("one second is a valid deadline");
        let elapsed = connect.elapsed("connecting to the broker");
        assert_eq!(elapsed.key, DeadlineKey::ConnectTimeout);
        assert_eq!(elapsed.bound, Duration::from_secs(1));
        assert_eq!(
            elapsed.to_string(),
            "connecting to the broker did not finish within connect_timeout_ms 1s"
        );
    }
}
