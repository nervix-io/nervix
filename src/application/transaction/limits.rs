//! The bounds the cluster puts on transactions, declared as settings.
//!
//! Layer: edges.
//!
//! - **Owns.** The setting definitions of the transaction idle timeout, tombstone retention and
//!   size and concurrency limits, their single defaults and the group that holds them.
//! - **Depends on.** The vocabulary's setting classes.
//! - **Must not know.** Command-line parsing or how a transaction is admitted.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::{convert::Infallible, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::{ClusterParameters, Setting, SettingDefinition};

/// How long an open transaction may wait for its next statement before the cluster abandons it.
pub struct TransactionIdleTimeout;

impl SettingDefinition for TransactionIdleTimeout {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "transaction-idle-timeout";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_secs(15 * 60)
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// How long the cluster remembers how a finished transaction ended.
pub struct TransactionTombstoneRetention;

impl SettingDefinition for TransactionTombstoneRetention {
    type Class = ClusterParameters;
    type Value = Duration;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "transaction-tombstone-retention";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> Duration {
        Duration::from_secs(15 * 60)
    }

    fn validate(_: &Duration, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The most statements one transaction may queue.
pub struct TransactionMaxStatements;

impl SettingDefinition for TransactionMaxStatements {
    type Class = ClusterParameters;
    type Value = usize;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "transaction-max-statements";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> usize {
        256
    }

    fn validate(_: &usize, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The most statement source bytes one transaction may queue.
pub struct TransactionMaxSourceBytes;

impl SettingDefinition for TransactionMaxSourceBytes {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "transaction-max-source-bytes";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        1024 * 1024
    }

    fn validate(_: &u64, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The most transactions the cluster keeps open at once.
pub struct TransactionMaxOpen;

impl SettingDefinition for TransactionMaxOpen {
    type Class = ClusterParameters;
    type Value = usize;
    type Context = ();
    type Error = Infallible;

    const NAME: &'static str = "transaction-max-open";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> usize {
        1024
    }

    fn validate(_: &usize, _: &()) -> Result<(), Report<Infallible>> {
        Ok(())
    }
}

/// The bounds the cluster puts on transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionLimits {
    pub idle_timeout: Setting<TransactionIdleTimeout>,
    pub tombstone_retention: Setting<TransactionTombstoneRetention>,
    pub max_statements: Setting<TransactionMaxStatements>,
    pub max_source_bytes: Setting<TransactionMaxSourceBytes>,
    pub max_open: Setting<TransactionMaxOpen>,
}

/// Transaction bounds an operator or a test gives; an absent bound takes its definition's default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionBounds {
    pub idle_timeout: Option<Duration>,
    pub tombstone_retention: Option<Duration>,
    pub max_statements: Option<usize>,
    pub max_source_bytes: Option<u64>,
    pub max_open: Option<usize>,
}

impl TransactionLimits {
    /// Resolves each bound that is not given through its definition's default.
    pub fn resolve(bounds: TransactionBounds) -> Result<Self, Report<Infallible>> {
        Ok(Self {
            idle_timeout: Setting::resolve(bounds.idle_timeout, &())?,
            tombstone_retention: Setting::resolve(bounds.tombstone_retention, &())?,
            max_statements: Setting::resolve(bounds.max_statements, &())?,
            max_source_bytes: Setting::resolve(bounds.max_source_bytes, &())?,
            max_open: Setting::resolve(bounds.max_open, &())?,
        })
    }

    pub fn idle_timeout(&self) -> Duration {
        *self.idle_timeout.value()
    }

    pub fn tombstone_retention(&self) -> Duration {
        *self.tombstone_retention.value()
    }

    pub fn max_statements(&self) -> usize {
        *self.max_statements.value()
    }

    pub fn max_source_bytes(&self) -> u64 {
        *self.max_source_bytes.value()
    }

    pub fn max_open(&self) -> usize {
        *self.max_open.value()
    }
}

impl Default for TransactionLimits {
    fn default() -> Self {
        Self::resolve(TransactionBounds::default())
            .assured("the transaction defaults declare no validation")
    }
}
