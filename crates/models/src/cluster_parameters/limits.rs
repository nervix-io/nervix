//! Cluster acceptance limits and their single defaults.
//!
//! Layer: vocabulary.
//! - **Owns.** Cluster acceptance limits and their single defaults.
//! - **Depends on.** Setting classes, semantic values and typed validation reports.
//! - **Must not know.** Parsing, consensus, command-line options or running services.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use error_stack::Report;

use super::{ClusterParameterError, ClusterParameterKey};
use crate::{ClusterParameters, SettingDefinition};

/// The most statements one transaction may queue.
pub struct TransactionMaxStatements;

impl SettingDefinition for TransactionMaxStatements {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "transaction-max-statements";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        256
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::TransactionMaxStatements.validate_count(*value, 0, u64::from(u32::MAX))
    }
}

/// The most source bytes one transaction may queue.
pub struct TransactionMaxSourceBytes;

impl SettingDefinition for TransactionMaxSourceBytes {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "transaction-max-source-bytes";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        1048576
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::TransactionMaxSourceBytes.validate_count(*value, 0, u64::MAX)
    }
}

/// The most transactions the cluster keeps open.
pub struct TransactionMaxOpen;

impl SettingDefinition for TransactionMaxOpen {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "transaction-max-open";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        1024
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::TransactionMaxOpen.validate_count(*value, 0, u64::from(u32::MAX))
    }
}

/// The most retained command executions, including applying work.
pub struct CommandExecutionCapacity;

impl SettingDefinition for CommandExecutionCapacity {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "command-execution-capacity";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        65536
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::CommandExecutionCapacity.validate_count(*value, 1, u64::from(u32::MAX))
    }
}

/// How many replicas the scheduler places beside each state owner.
pub struct ReplicaCount;

impl SettingDefinition for ReplicaCount {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "replica-count";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        0
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::ReplicaCount.validate_count(*value, 0, u64::from(u32::MAX))
    }
}

/// The most archive bytes one resource version may contain.
pub struct ResourceMaxArchiveBytes;

impl SettingDefinition for ResourceMaxArchiveBytes {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "resource-max-archive-bytes";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        4294967296
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::ResourceMaxArchiveBytes.validate_count(*value, 1, u64::MAX)
    }
}

/// The most extracted bytes one resource version may contain.
pub struct ResourceMaxExtractedBytes;

impl SettingDefinition for ResourceMaxExtractedBytes {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "resource-max-extracted-bytes";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        17179869184
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::ResourceMaxExtractedBytes.validate_count(*value, 1, u64::MAX)
    }
}

/// The most files one resource version may contain.
pub struct ResourceMaxFileCount;

impl SettingDefinition for ResourceMaxFileCount {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "resource-max-file-count";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        1000000
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::ResourceMaxFileCount.validate_count(*value, 1, u64::MAX)
    }
}

/// The most records one declared emitter batch may contain.
pub struct MaxBatchMessages;

impl SettingDefinition for MaxBatchMessages {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "max-batch-messages";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        65536
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::MaxBatchMessages.validate_count(*value, 1, u64::from(u32::MAX))
    }
}

/// The outstanding producer payload bytes one session may reserve.
pub struct ClientProducerSessionBytes;

impl SettingDefinition for ClientProducerSessionBytes {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "client-producer-session-bytes";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        Self::DEFAULT
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::ClientProducerSessionBytes.validate_count(*value, 1, u64::MAX)
    }
}

/// The outstanding consumer payload bytes one session may reserve.
pub struct ClientConsumerSessionBytes;

impl SettingDefinition for ClientConsumerSessionBytes {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "client-consumer-session-bytes";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        Self::DEFAULT
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::ClientConsumerSessionBytes.validate_count(*value, 1, u64::MAX)
    }
}

/// The most source bytes one Roto function may declare.
pub struct UdfCodeBytes;

impl SettingDefinition for UdfCodeBytes {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "udf-code-bytes";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        65536
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::UdfCodeBytes.validate_count(*value, 1, u64::MAX)
    }
}

/// The most application header bytes one HTTP invocation may construct.
pub struct HttpHeaderBytes;

impl SettingDefinition for HttpHeaderBytes {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "http-header-bytes";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        32768
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::HttpHeaderBytes.validate_count(*value, 1, u64::MAX)
    }
}

/// The most distinct application header fields one HTTP request may contain.
pub struct HttpHeaderFields;

impl HttpHeaderFields {
    /// Distinct fields that fit the native request map at its maximum allocation.
    pub const MAXIMUM: u64 = 24_576;
}

impl SettingDefinition for HttpHeaderFields {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;
    const NAME: &'static str = "http-header-fields";
    const DERIVATION: Option<&'static str> = None;
    fn default_value(_: &()) -> u64 {
        128
    }
    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::HttpHeaderFields.validate_count(*value, 1, Self::MAXIMUM)
    }
}

/// The most rows one client payload batch may contain.
pub struct ClientBatchRows;

impl SettingDefinition for ClientBatchRows {
    type Class = ClusterParameters;
    type Value = u64;
    type Context = ();
    type Error = ClusterParameterError;

    const NAME: &'static str = "client-batch-rows";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> u64 {
        u64::from(Self::DEFAULT)
    }

    fn validate(value: &u64, _: &()) -> Result<(), Report<ClusterParameterError>> {
        ClusterParameterKey::ClientBatchRows.validate_count(*value, 1, u64::from(u32::MAX))
    }
}

impl ClientProducerSessionBytes {
    /// The compile-time source of this setting definition's default.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the setting definition owns this compile-time default, not an additional \
                      policy input"
        )
    )]
    pub const DEFAULT: u64 = 33554432;
}

impl ClientConsumerSessionBytes {
    /// The compile-time source of this setting definition's default.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the setting definition owns this compile-time default, not an additional \
                      policy input"
        )
    )]
    pub const DEFAULT: u64 = 33554432;
}

impl ClientBatchRows {
    /// The compile-time source of this setting definition's default.
    #[cfg_attr(
        nervix_lint,
        nervix::constant(
            fixed,
            reason = "the setting definition owns this compile-time default, not an additional \
                      policy input"
        )
    )]
    pub const DEFAULT: u32 = 65536;
}
