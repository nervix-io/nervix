//! The transaction and command options of the server command line.
//!
//! Layer: edges.
//!
//! - **Owns.** The command-line form of the transaction limits and the command execution policy,
//!   their conversion into those settings groups, and the rendering of the groups back into
//!   options.
//! - **Depends on.** The transaction and command execution settings definitions and the edge's
//!   duration form.
//! - **Must not know.** How a transaction or a command is admitted.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::application::{
    cli_values::HumanDuration,
    command_execution::{CommandExecutionCapacity, CommandExecutionPolicy, CommandRetryValidity},
    transaction::{
        TransactionBounds, TransactionIdleTimeout, TransactionLimits, TransactionMaxOpen,
        TransactionMaxSourceBytes, TransactionMaxStatements, TransactionTombstoneRetention,
    },
};

#[derive(clap::Args, Debug, Clone, PartialEq)]
#[command(next_help_heading = "Transactions and commands")]
pub struct TransactionArgs {
    #[arg(
        long = TransactionIdleTimeout::NAME,
        env = "NERVIX_TRANSACTION_IDLE_TIMEOUT",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::TransactionIdleTimeout, "How long an open transaction may wait for its next statement before the cluster \
                abandons it")
    )]
    pub transaction_idle_timeout: Option<HumanDuration>,
    #[arg(
        long = TransactionTombstoneRetention::NAME,
        env = "NERVIX_TRANSACTION_TOMBSTONE_RETENTION",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::TransactionTombstoneRetention, "How long the cluster remembers how a finished transaction ended")
    )]
    pub transaction_tombstone_retention: Option<HumanDuration>,
    #[arg(
        long = CommandRetryValidity::NAME,
        env = "NERVIX_COMMAND_RETRY_VALIDITY",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::CommandRetryValidity, "How long the cluster recognizes a command reference, so a retried command finds \
                its first outcome")
    )]
    pub command_retry_validity: Option<HumanDuration>,
    #[arg(
        long = CommandExecutionCapacity::NAME,
        env = "NERVIX_COMMAND_EXECUTION_CAPACITY",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::CommandExecutionCapacity, "The most command executions the cluster retains outcomes for at once")
    )]
    pub command_execution_capacity: Option<usize>,
    #[arg(
        long = TransactionMaxStatements::NAME,
        env = "NERVIX_TRANSACTION_MAX_STATEMENTS",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::TransactionMaxStatements, "The most statements one transaction may queue")
    )]
    pub transaction_max_statements: Option<usize>,
    #[arg(
        long = TransactionMaxSourceBytes::NAME,
        env = "NERVIX_TRANSACTION_MAX_SOURCE_BYTES",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::TransactionMaxSourceBytes, "The most statement source bytes one transaction may queue")
    )]
    pub transaction_max_source_bytes: Option<u64>,
    #[arg(
        long = TransactionMaxOpen::NAME,
        env = "NERVIX_TRANSACTION_MAX_OPEN",
        help = super::settings::parameter_seed_help(nervix_models::ClusterParameterKey::TransactionMaxOpen, "The most transactions the cluster keeps open at once")
    )]
    pub transaction_max_open: Option<usize>,
}

impl TransactionArgs {
    pub(super) fn limits(&self) -> TransactionLimits {
        TransactionLimits::resolve(TransactionBounds {
            idle_timeout: self.transaction_idle_timeout.map(|value| value.0),
            tombstone_retention: self.transaction_tombstone_retention.map(|value| value.0),
            max_statements: self.transaction_max_statements,
            max_source_bytes: self.transaction_max_source_bytes,
            max_open: self.transaction_max_open,
        })
        .assured("the transaction limits have valid bounds")
    }

    pub(super) fn commands(&self) -> CommandExecutionPolicy {
        CommandExecutionPolicy::resolve(
            self.command_retry_validity.map(|value| value.0),
            self.command_execution_capacity,
        )
        .assured("the command execution settings have valid bounds")
    }

    pub(super) fn from_settings(
        limits: &TransactionLimits,
        commands: &CommandExecutionPolicy,
    ) -> Self {
        Self {
            transaction_idle_timeout: Some(HumanDuration(limits.idle_timeout())),
            transaction_tombstone_retention: Some(HumanDuration(limits.tombstone_retention())),
            command_retry_validity: Some(HumanDuration(commands.retry_validity())),
            command_execution_capacity: Some(commands.capacity()),
            transaction_max_statements: Some(limits.max_statements()),
            transaction_max_source_bytes: Some(limits.max_source_bytes()),
            transaction_max_open: Some(limits.max_open()),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(TransactionIdleTimeout::NAME, self.transaction_idle_timeout);
        command_line.optional(
            TransactionTombstoneRetention::NAME,
            self.transaction_tombstone_retention,
        );
        command_line.optional(CommandRetryValidity::NAME, self.command_retry_validity);
        command_line.optional(
            CommandExecutionCapacity::NAME,
            self.command_execution_capacity,
        );
        command_line.optional(
            TransactionMaxStatements::NAME,
            self.transaction_max_statements,
        );
        command_line.optional(
            TransactionMaxSourceBytes::NAME,
            self.transaction_max_source_bytes,
        );
        command_line.optional(TransactionMaxOpen::NAME, self.transaction_max_open);
    }
}
