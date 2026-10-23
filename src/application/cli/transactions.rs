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
        default_value_t = HumanDuration(TransactionIdleTimeout::default_value(&())),
        help = "How long an open transaction may wait for its next statement before the cluster \
                abandons it"
    )]
    pub transaction_idle_timeout: HumanDuration,
    #[arg(
        long = TransactionTombstoneRetention::NAME,
        env = "NERVIX_TRANSACTION_TOMBSTONE_RETENTION",
        default_value_t = HumanDuration(TransactionTombstoneRetention::default_value(&())),
        help = "How long the cluster remembers how a finished transaction ended"
    )]
    pub transaction_tombstone_retention: HumanDuration,
    #[arg(
        long = CommandRetryValidity::NAME,
        env = "NERVIX_COMMAND_RETRY_VALIDITY",
        default_value_t = HumanDuration(CommandRetryValidity::default_value(&())),
        help = "How long the cluster recognizes a command reference, so a retried command finds \
                its first outcome"
    )]
    pub command_retry_validity: HumanDuration,
    #[arg(
        long = CommandExecutionCapacity::NAME,
        env = "NERVIX_COMMAND_EXECUTION_CAPACITY",
        default_value_t = CommandExecutionCapacity::default_value(&()),
        help = "The most command executions the cluster retains outcomes for at once"
    )]
    pub command_execution_capacity: usize,
    #[arg(
        long = TransactionMaxStatements::NAME,
        env = "NERVIX_TRANSACTION_MAX_STATEMENTS",
        default_value_t = TransactionMaxStatements::default_value(&()),
        help = "The most statements one transaction may queue"
    )]
    pub transaction_max_statements: usize,
    #[arg(
        long = TransactionMaxSourceBytes::NAME,
        env = "NERVIX_TRANSACTION_MAX_SOURCE_BYTES",
        default_value_t = TransactionMaxSourceBytes::default_value(&()),
        help = "The most statement source bytes one transaction may queue"
    )]
    pub transaction_max_source_bytes: u64,
    #[arg(
        long = TransactionMaxOpen::NAME,
        env = "NERVIX_TRANSACTION_MAX_OPEN",
        default_value_t = TransactionMaxOpen::default_value(&()),
        help = "The most transactions the cluster keeps open at once"
    )]
    pub transaction_max_open: usize,
}

impl TransactionArgs {
    pub(super) fn limits(&self) -> TransactionLimits {
        TransactionLimits::resolve(TransactionBounds {
            idle_timeout: Some(self.transaction_idle_timeout.0),
            tombstone_retention: Some(self.transaction_tombstone_retention.0),
            max_statements: Some(self.transaction_max_statements),
            max_source_bytes: Some(self.transaction_max_source_bytes),
            max_open: Some(self.transaction_max_open),
        })
        .assured("the transaction limits declare no validation")
    }

    pub(super) fn commands(&self) -> CommandExecutionPolicy {
        CommandExecutionPolicy::resolve(
            Some(self.command_retry_validity.0),
            Some(self.command_execution_capacity),
        )
        .assured("the command execution settings declare no validation")
    }

    pub(super) fn from_settings(
        limits: &TransactionLimits,
        commands: &CommandExecutionPolicy,
    ) -> Self {
        Self {
            transaction_idle_timeout: HumanDuration(limits.idle_timeout()),
            transaction_tombstone_retention: HumanDuration(limits.tombstone_retention()),
            command_retry_validity: HumanDuration(commands.retry_validity()),
            command_execution_capacity: commands.capacity(),
            transaction_max_statements: limits.max_statements(),
            transaction_max_source_bytes: limits.max_source_bytes(),
            transaction_max_open: limits.max_open(),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.option(TransactionIdleTimeout::NAME, self.transaction_idle_timeout);
        command_line.option(
            TransactionTombstoneRetention::NAME,
            self.transaction_tombstone_retention,
        );
        command_line.option(CommandRetryValidity::NAME, self.command_retry_validity);
        command_line.option(
            CommandExecutionCapacity::NAME,
            self.command_execution_capacity,
        );
        command_line.option(
            TransactionMaxStatements::NAME,
            self.transaction_max_statements,
        );
        command_line.option(
            TransactionMaxSourceBytes::NAME,
            self.transaction_max_source_bytes,
        );
        command_line.option(TransactionMaxOpen::NAME, self.transaction_max_open);
    }
}
