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

use std::time::Duration;

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::ResultExt as _;
use nervix_models::Setting;

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
    pub fn resolve(
        bounds: TransactionBounds,
    ) -> Result<Self, Report<nervix_models::ClusterParameterError>> {
        Ok(Self {
            idle_timeout: Setting::resolve(bounds.idle_timeout, &())?,
            tombstone_retention: Setting::resolve(bounds.tombstone_retention, &())?,
            max_statements: Setting::resolve(
                bounds.max_statements.map(|value| value.arch_into()),
                &(),
            )?,
            max_source_bytes: Setting::resolve(bounds.max_source_bytes, &())?,
            max_open: Setting::resolve(bounds.max_open.map(|value| value.arch_into()), &())?,
        })
    }

    pub fn idle_timeout(&self) -> Duration {
        *self.idle_timeout.value()
    }

    pub fn tombstone_retention(&self) -> Duration {
        *self.tombstone_retention.value()
    }

    pub fn max_statements(&self) -> usize {
        usize::try_from(*self.max_statements.value())
            .assured("the validated count fits a supported native target")
    }

    pub fn max_source_bytes(&self) -> u64 {
        *self.max_source_bytes.value()
    }

    pub fn max_open(&self) -> usize {
        usize::try_from(*self.max_open.value())
            .assured("the validated count fits a supported native target")
    }
}

impl Default for TransactionLimits {
    fn default() -> Self {
        Self::resolve(TransactionBounds::default())
            .assured("the transaction defaults have valid bounds")
    }
}

// Cluster policy definitions have one vocabulary owner. These facades hold bootstrap inputs.
pub use nervix_models::{
    TransactionIdleTimeout, TransactionMaxOpen, TransactionMaxSourceBytes,
    TransactionMaxStatements, TransactionTombstoneRetention,
};
