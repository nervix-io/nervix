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

use clap::Parser as _;
use error_stack::{Report, ResultExt as _};

use super::{
    consensus::ConsensusArgs, membership::MembershipArgs, memory_pressure::MemoryPressureArgs,
    resources::ResourceArgs, runtime::RuntimeArgs, scheduling::SchedulingArgs,
    shutdown::ShutdownArgs, transactions::TransactionArgs,
};
use crate::application::{error::AppError, node_settings::NodeSettings};

#[derive(clap::Args, Debug, Clone, PartialEq)]
pub struct SettingsArgs {
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
}

impl TryFrom<SettingsArgs> for NodeSettings {
    type Error = Report<AppError>;

    /// Converts every area's options once, validating each value, and each relationship between
    /// values, before the node does any I/O.
    fn try_from(args: SettingsArgs) -> Result<Self, Self::Error> {
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
        Ok(Self {
            raft,
            raft_retention: args.consensus.retention(),
            membership: args.membership.settings(),
            scheduling: args.scheduling.settings(),
            runtime: args.runtime.settings(),
            transactions: args.transactions.limits(),
            commands: args.transactions.commands(),
            memory_pressure,
            shutdown,
            resources: args.resources.settings(),
        })
    }
}

impl From<&NodeSettings> for SettingsArgs {
    fn from(settings: &NodeSettings) -> Self {
        Self {
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
        }
    }
}

impl SettingsArgs {
    /// Every option these settings hold, written as the command line reads it back.
    pub fn command_line(&self) -> Vec<String> {
        let mut command_line = CommandLine::default();
        self.consensus.render(&mut command_line);
        self.membership.render(&mut command_line);
        self.scheduling.render(&mut command_line);
        self.runtime.render(&mut command_line);
        self.transactions.render(&mut command_line);
        self.memory_pressure.render(&mut command_line);
        self.shutdown.render(&mut command_line);
        self.resources.render(&mut command_line);
        command_line.arguments
    }
}

/// Options written in the form the server's command line reads back.
#[derive(Debug, Default)]
pub(super) struct CommandLine {
    arguments: Vec<String>,
}

impl CommandLine {
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
        Self::try_from(read.settings)
    }
}
