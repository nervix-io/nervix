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

pub(super) fn capability_help<D>(description: &str, default: impl fmt::Display) -> String
where
    D: nervix_models::SettingDefinition<Class = nervix_models::NodeCapabilities>,
{
    match D::DERIVATION {
        Some(derivation) => {
            format!("{description}. Node capability. Default: {default}; {derivation}")
        }
        None => format!("{description}. Node capability. Default: {default}"),
    }
}

pub(super) fn parameter_seed_help(
    parameter: nervix_models::ClusterParameterKey,
    description: &str,
) -> String {
    format!(
        "{description}. First-bootstrap default: {}. Change live policy with ALTER CLUSTER SET",
        parameter.default_cli_text()
    )
}

use arch_into::ArchInto as _;

impl SettingsArgs {
    /// Preserve the presence of command-line and environment seeds, including explicit defaults.
    pub(super) fn parameter_seeds(&self) -> nervix_models::ClusterParameterSeedValues {
        nervix_models::ClusterParameterSeedValues {
            forced_recovery_budget: self
                .cluster_parameters
                .forced_recovery_budget
                .map(|value| value.0),
            ownership_handoff_poll_interval: self
                .cluster_parameters
                .ownership_handoff_poll_interval
                .map(|value| value.0),
            entity_gate_poll_interval: self
                .cluster_parameters
                .entity_gate_poll_interval
                .map(|value| value.0),
            domain_drain_poll_interval: self
                .cluster_parameters
                .domain_drain_poll_interval
                .map(|value| value.0),
            runtime_readiness_propagation_bound: self
                .cluster_parameters
                .runtime_readiness_propagation_bound
                .map(|value| value.0),
            licensing_request_timeout: self
                .cluster_parameters
                .licensing_request_timeout
                .map(|duration| duration.0),
            licensing_retry_backoff_max: self
                .cluster_parameters
                .licensing_retry_backoff_max
                .map(|duration| duration.0),
            licensing_stream_idle_timeout: self
                .cluster_parameters
                .licensing_stream_idle_timeout
                .map(|duration| duration.0),

            tls_cipher_suites: self.cluster_parameters.tls_cipher_suites,
            tls_key_exchange_groups: self.cluster_parameters.tls_key_exchange_groups,
            password_iterations: self.cluster_parameters.password_iterations,
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
    OperationalArgs, capacity::CapacityArgs, consensus::ConsensusArgs, execution::ExecutionArgs,
    membership::MembershipArgs, memory_pressure::MemoryPressureArgs, resources::ResourceArgs,
    runtime::RuntimeArgs, scheduling::SchedulingArgs, shutdown::ShutdownArgs,
    transactions::TransactionArgs,
};
use crate::application::{error::AppError, node_settings::NodeSettings};

#[derive(clap::Args, Debug, Clone, PartialEq)]
pub struct SettingsArgs {
    #[command(flatten)]
    pub database: super::database::DatabaseArgs,
    #[command(flatten)]
    pub consensus_operations: super::consensus_operations::ConsensusOperationArgs,
    #[command(flatten)]
    pub dns: super::dns::DnsArgs,
    #[command(flatten)]
    pub interconnect: super::interconnect::InterconnectArgs,
    #[command(flatten)]
    pub connector_host: super::connector_host::ConnectorHostArgs,
    #[command(flatten)]
    pub engines: super::engines::EngineArgs,
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
    #[command(flatten)]
    pub execution: ExecutionArgs,
    #[command(flatten)]
    pub operational: OperationalArgs,
    #[command(flatten)]
    pub listeners: super::listeners::ListenerArgs,
}

impl TryFrom<SettingsArgs> for NodeSettings {
    type Error = Report<AppError>;

    /// Converts every area's options once, validating each value, and each relationship between
    /// values, before the node does any I/O.
    fn try_from(args: SettingsArgs) -> Result<Self, Self::Error> {
        args.execution
            .overrides()
            .validate()
            .change_context(AppError::InvalidExecutionSettings)?;
        let runtime_threads = args.execution.runtime_threads();
        runtime_threads
            .validate()
            .change_context(AppError::InvalidRuntimeCapacity)?;
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
        let peer_receive = args
            .capacity
            .peer_receive()
            .change_context(AppError::InvalidPeerReceiveSettings)?;
        let interconnect = args
            .interconnect
            .settings()
            .change_context(AppError::InvalidTransportSettings)?;
        let dns = args
            .dns
            .settings(interconnect.max_peers())
            .change_context(AppError::InvalidDnsSettings)?;
        let mut runtime = args
            .runtime
            .settings()
            .change_context(AppError::InvalidStateInspectionSettings)?;
        runtime.connector_host = args
            .connector_host
            .settings()
            .change_context(AppError::InvalidConnectorHostSettings)?;
        runtime.vm = args
            .engines
            .settings()
            .change_context(AppError::InvalidVmSettings)?;
        runtime.wasm_engine = args
            .engines
            .wasm_settings()
            .change_context(AppError::InvalidWasmEngineSettings)?;
        Ok(Self {
            node_database: args
                .database
                .node()
                .change_context(AppError::InvalidDatabaseSettings)?,
            consensus_database: args
                .database
                .consensus()
                .change_context(AppError::InvalidDatabaseSettings)?,
            dns,
            consensus_operations: args
                .consensus_operations
                .settings()
                .change_context(AppError::InvalidConsensusOperations)?,
            parameter_seeds,
            interconnect,
            operational: args
                .operational
                .settings()
                .change_context(AppError::InvalidOperationalCapabilities)?,
            listeners: args
                .listeners
                .settings()
                .change_context(AppError::InvalidListenerSettings)?,
            raft,
            raft_retention: args
                .consensus
                .retention()
                .change_context(AppError::InvalidConsensusOperations)?,
            membership,
            scheduling: args.scheduling.settings(),
            runtime,
            transactions: args.transactions.limits(),
            commands: args.transactions.commands(),
            memory_pressure,
            shutdown,
            resources: args.resources.settings(),
            capacity,
            peer_receive,
            execution: args.execution.overrides(),
            runtime_threads,
            client_endpoints: args
                .execution
                .client_endpoints()
                .change_context(AppError::InvalidClientEndpointCapacity)?,
        })
    }
}

impl From<&NodeSettings> for SettingsArgs {
    fn from(settings: &NodeSettings) -> Self {
        Self {
            database: super::database::DatabaseArgs::from_settings(
                &settings.node_database,
                &settings.consensus_database,
            ),
            consensus_operations:
                super::consensus_operations::ConsensusOperationArgs::from_settings(
                    &settings.consensus_operations,
                ),
            dns: super::dns::DnsArgs::from_settings(&settings.dns),
            interconnect: super::interconnect::InterconnectArgs::from_settings(
                &settings.interconnect,
            ),
            connector_host: super::connector_host::ConnectorHostArgs::from_settings(
                &settings.runtime.connector_host,
            ),
            engines: super::engines::EngineArgs::from_settings(
                &settings.runtime.vm,
                &settings.runtime.wasm_engine,
            ),
            cluster_parameters: super::cluster_parameters::ClusterParameterArgs::from_seeds(
                &settings.parameter_seeds,
            ),
            operational: OperationalArgs::from_settings(&settings.operational),
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
            capacity: CapacityArgs::from_settings(&settings.capacity, &settings.peer_receive),
            listeners: super::listeners::ListenerArgs::from_settings(&settings.listeners),
            execution: ExecutionArgs::from_overrides(
                &settings.execution,
                &settings.client_endpoints,
                &settings.runtime_threads,
            ),
        }
    }
}

impl SettingsArgs {
    /// Every option these settings hold, written as the command line reads it back.
    pub fn command_line(&self) -> Vec<String> {
        let mut command_line = CommandLine::default();
        self.connector_host.render(&mut command_line);
        self.engines.render(&mut command_line);
        self.cluster_parameters.render(&mut command_line);
        self.interconnect.render(&mut command_line);
        self.dns.render(&mut command_line);
        self.consensus.render(&mut command_line);
        self.consensus_operations.render(&mut command_line);
        self.database.render(&mut command_line);
        self.membership.render(&mut command_line);
        self.scheduling.render(&mut command_line);
        self.runtime.render(&mut command_line);
        self.transactions.render(&mut command_line);
        self.memory_pressure.render(&mut command_line);
        self.shutdown.render(&mut command_line);
        self.resources.render(&mut command_line);
        self.capacity.render(&mut command_line);
        self.execution.render(&mut command_line);
        self.operational.render(&mut command_line);
        self.listeners.render(&mut command_line);
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
