//! The settings options of the server command line, as one group of groups.
//!
//! Layer: edges.
//!
//! - **Owns.** The composition of every area's settings options, their conversion into the
//!   node's settings, the rendering of those settings back into options, and the reading of
//!   settings named by their options over a node's own.
//! - **Depends on.** Each area's options and the node's settings.
//! - **Must not know.** How any owner uses its values.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::fmt;

pub(super) fn parameter_seed_help(
    parameter: nervix_models::ClusterParameterKey,
    description: &str,
) -> String {
    format!(
        "{description}. First-bootstrap default: {}. Change live policy with ALTER CLUSTER SET",
        parameter.default_text()
    )
}

use arch_into::ArchInto as _;

impl SettingsArgs {
    /// Preserve the presence of command-line and environment seeds, including explicit defaults.
    pub(super) fn parameter_seeds(&self) -> nervix_models::ClusterParameterSeedValues {
        nervix_models::ClusterParameterSeedValues {
            node_unavailability_timeout: self
                .membership
                .node_unavailability_timeout
                .map(|duration| duration.0),
            domain_drain_timeout: self
                .cluster_parameters
                .domain_drain_timeout
                .map(|duration| duration.0),
            entity_gate_deadline: self
                .cluster_parameters
                .entity_gate_deadline
                .map(|duration| duration.0),
            transaction_idle_timeout: self
                .transactions
                .transaction_idle_timeout
                .map(|duration| duration.0),
            transaction_tombstone_retention: self
                .transactions
                .transaction_tombstone_retention
                .map(|duration| duration.0),
            command_retry_validity: self
                .transactions
                .command_retry_validity
                .map(|duration| duration.0),
            state_snapshot_interval: self
                .runtime
                .state_snapshot_interval
                .map(|duration| duration.0),
            transaction_max_statements: self
                .transactions
                .transaction_max_statements
                .map(|count| count.arch_into()),
            transaction_max_source_bytes: self.transactions.transaction_max_source_bytes,
            transaction_max_open: self
                .transactions
                .transaction_max_open
                .map(|count| count.arch_into()),
            command_execution_capacity: self
                .transactions
                .command_execution_capacity
                .map(|count| count.arch_into()),
            replica_count: self.scheduling.replica_count.map(|count| count.arch_into()),
            resource_max_archive_bytes: self
                .resources
                .resource_max_archive_bytes
                .map(|bytes| bytes.0.as_u64()),
            resource_max_extracted_bytes: self
                .resources
                .resource_max_extracted_bytes
                .map(|bytes| bytes.0.as_u64()),
            resource_max_file_count: self.resources.resource_max_file_count,
            max_batch_messages: self.cluster_parameters.max_batch_messages,
            client_producer_session_bytes: self
                .cluster_parameters
                .client_producer_session_bytes
                .map(|bytes| bytes.0.as_u64()),
            client_consumer_session_bytes: self
                .cluster_parameters
                .client_consumer_session_bytes
                .map(|bytes| bytes.0.as_u64()),
            udf_code_bytes: self
                .cluster_parameters
                .udf_code_bytes
                .map(|bytes| bytes.0.as_u64()),
            http_header_bytes: self
                .cluster_parameters
                .http_header_bytes
                .map(|bytes| bytes.0.as_u64()),
            client_batch_rows: self.cluster_parameters.client_batch_rows,
            http_header_fields: self.cluster_parameters.http_header_fields,
        }
    }
}

use clap::Parser as _;
use error_stack::{Report, ResultExt as _};

use super::{
    capacity::CapacityArgs, consensus::ConsensusArgs, membership::MembershipArgs,
    memory_pressure::MemoryPressureArgs, resources::ResourceArgs, runtime::RuntimeArgs,
    scheduling::SchedulingArgs, shutdown::ShutdownArgs, transactions::TransactionArgs,
};
use crate::application::{error::AppError, node_settings::NodeSettings};

#[derive(clap::Args, Debug, Clone, PartialEq)]
pub struct SettingsArgs {
    #[command(flatten)]
    pub cluster_parameters: super::cluster_parameters::ClusterParameterArgs,
    #[command(flatten)]
    pub consensus: ConsensusArgs,
    #[command(flatten)]
    pub membership: MembershipArgs,
    #[command(flatten)]
    pub scheduling: SchedulingArgs,
    #[command(flatten)]
    pub runtime: RuntimeArgs,
    #[command(flatten)]
    pub transactions: TransactionArgs,
    #[command(flatten)]
    pub memory_pressure: MemoryPressureArgs,
    #[command(flatten)]
    pub shutdown: ShutdownArgs,
    #[command(flatten)]
    pub resources: ResourceArgs,
    #[command(flatten)]
    pub capacity: CapacityArgs,
}

impl TryFrom<SettingsArgs> for NodeSettings {
    type Error = Report<AppError>;

    /// Converts every area's options once, validating each value, and each relationship between
    /// values, before the node does any I/O.
    fn try_from(args: SettingsArgs) -> Result<Self, Self::Error> {
        let parameter_seeds = args.parameter_seeds();
        nervix_models::ClusterParameterValues::resolve(parameter_seeds)
            .change_context(AppError::InvalidClusterParameters)?;
        let raft = args
            .consensus
            .timings()
            .change_context(AppError::InvalidRaftTimings)?;
        let memory_pressure = args
            .memory_pressure
            .settings()
            .change_context(AppError::InvalidMemoryPressureConfig)?;
        let shutdown = args
            .shutdown
            .settings()
            .change_context(AppError::InvalidShutdownTimeouts)?;
        let membership = args
            .membership
            .settings()
            .change_context(AppError::InvalidMembershipSettings)?;
        let capacity = args
            .capacity
            .settings()
            .change_context(AppError::InvalidCapacitySettings)?;
        Ok(Self {
            parameter_seeds,
            raft,
            raft_retention: args.consensus.retention(),
            membership,
            scheduling: args.scheduling.settings(),
            runtime: args.runtime.settings(),
            transactions: args.transactions.limits(),
            commands: args.transactions.commands(),
            memory_pressure,
            shutdown,
            resources: args.resources.settings(),
            capacity,
        })
    }
}

impl From<&NodeSettings> for SettingsArgs {
    fn from(settings: &NodeSettings) -> Self {
        Self {
            cluster_parameters: super::cluster_parameters::ClusterParameterArgs::from_seeds(
                &settings.parameter_seeds,
            ),
            consensus: ConsensusArgs::from_settings(&settings.raft, &settings.raft_retention),
            membership: MembershipArgs::from_settings(&settings.membership),
            scheduling: SchedulingArgs::from_settings(&settings.scheduling),
            runtime: RuntimeArgs::from_settings(&settings.runtime),
            transactions: TransactionArgs::from_settings(
                &settings.transactions,
                &settings.commands,
            ),
            memory_pressure: MemoryPressureArgs::from_settings(&settings.memory_pressure),
            shutdown: ShutdownArgs::from_settings(&settings.shutdown),
            resources: ResourceArgs::from_settings(&settings.resources),
            capacity: CapacityArgs::from_settings(&settings.capacity),
        }
    }
}

impl SettingsArgs {
    /// Every option these settings hold, written as the command line reads it back.
    pub fn command_line(&self) -> Vec<String> {
        let mut command_line = CommandLine::default();
        self.cluster_parameters.render(&mut command_line);
        self.consensus.render(&mut command_line);
        self.membership.render(&mut command_line);
        self.scheduling.render(&mut command_line);
        self.runtime.render(&mut command_line);
        self.transactions.render(&mut command_line);
        self.memory_pressure.render(&mut command_line);
        self.shutdown.render(&mut command_line);
        self.resources.render(&mut command_line);
        self.capacity.render(&mut command_line);
        command_line.arguments
    }
}

/// Options written in the form the server's command line reads back.
#[derive(Debug, Default)]
pub(super) struct CommandLine {
    arguments: Vec<String>,
}

impl CommandLine {
    pub(super) fn optional(&mut self, name: &str, value: Option<impl fmt::Display>) {
        if let Some(value) = value {
            self.option(name, value);
        }
    }
    pub(super) fn option(&mut self, name: &str, value: impl fmt::Display) {
        self.arguments.push(format!("--{name}"));
        self.arguments.push(value.to_string());
    }
}

/// One setting named by its option, with its value written as the command line reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingOverride {
    pub name: String,
    pub value: String,
}

/// The settings options alone, read as a later option of a setting replaces an earlier one.
#[derive(clap::Parser, Debug)]
#[command(name = "nervix-server", args_override_self = true)]
struct SettingsCommandLine {
    #[command(flatten)]
    settings: SettingsArgs,
}

impl NodeSettings {
    /// These settings with each override applied in order, read and validated exactly as the
    /// server's command line reads and validates them.
    pub fn with_overrides(&self, overrides: &[SettingOverride]) -> Result<Self, Report<AppError>> {
        let mut arguments = vec![String::from("nervix-server")];
        arguments.extend(SettingsArgs::from(self).command_line());
        for setting in overrides {
            arguments.push(format!("--{}", setting.name));
            arguments.push(setting.value.clone());
        }
        let read = SettingsCommandLine::try_parse_from(arguments)
            .change_context(AppError::ReadSettings)?;
        let mut resolved = Self::try_from(read.settings)?;
        resolved.parameter_seeds.retain_sources(
            &self.parameter_seeds,
            overrides.iter().map(|setting| setting.name.as_str()),
        );
        Ok(resolved)
    }
}
