//! Optional execution capacities on the server command line.
//!
//! Layer: edges.
//! - **Owns.** Reading and rendering local execution overrides before machine detection.
//! - **Depends on.** The execution setting declarations and the edge's byte representation.
//! - **Must not know.** Worker execution, memory admission or machine detection.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns execution command-line options")
)]

use std::num::{NonZeroU32, NonZeroUsize};

use nervix_execution::{
    BulkChunkBytes, BulkCpuPendingJobs, BulkCpuWorkers, BulkMemory, BulkWindowMemory,
    CommandsMemory, ConnectorStoragePendingJobs, ConnectorStorageWorkers,
    ConsensusStoragePendingJobs, ConsensusStorageWorkers, ControlCpuPendingJobs, ControlCpuWorkers,
    CredentialsCpuPendingJobs, CredentialsCpuWorkers, CredentialsMemory, DataCpuPendingJobs,
    DataCpuWorkers, ExecutionOverrides, ExtensionCpuPendingJobs, ExtensionCpuWorkers,
    FilesystemStoragePendingJobs, FilesystemStorageWorkers, IntakeMemory, ManagementMemory,
    RelayDecodedBytes, RelayEncodedBytes, RelayMemory, RelayScratchBytes,
    ResidentReplicationBatches, RestoreMetadataMemory, StateReadCacheMemory, StateReadMemory,
};
use nervix_models::SettingDefinition;

use super::settings::CommandLine;
use crate::{
    application::{
        RuntimeMaxBlockingThreads, RuntimeThreadOverrides, RuntimeThreadStackSize,
        RuntimeWorkerThreads, cli_values::HumanBytes,
    },
    runtime::{
        ClientConsumerNodeBytes, ClientEndpointCapacityError, ClientEndpointOverrides,
        ClientEndpointSettings, ClientProducerNodeBytes,
    },
};

#[derive(clap::Args, Debug, Clone, Default, PartialEq)]
#[command(next_help_heading = "Execution")]
pub struct ExecutionArgs {
    #[arg(id = RuntimeWorkerThreads::NAME, long = RuntimeWorkerThreads::NAME, env = "NERVIX_RUNTIME_WORKER_THREADS", help = "Native runtime worker threads; defaults to resolved CPU count")]
    pub runtime_worker_threads: Option<NonZeroUsize>,
    #[arg(id = RuntimeMaxBlockingThreads::NAME, long = RuntimeMaxBlockingThreads::NAME, env = "NERVIX_RUNTIME_MAX_BLOCKING_THREADS", help = "Native blocking-thread ceiling; defaults to admitted execution workers with the existing I/O allowance")]
    pub runtime_max_blocking_threads: Option<NonZeroUsize>,
    #[arg(id = RuntimeThreadStackSize::NAME, long = RuntimeThreadStackSize::NAME, env = "NERVIX_RUNTIME_THREAD_STACK_SIZE", help = format!("Stack capacity per native runtime worker; defaults to {}", RuntimeThreadStackSize::default_bytes()))]
    pub runtime_thread_stack_size: Option<HumanBytes>,
    #[arg(id = ClientProducerNodeBytes::NAME, long = ClientProducerNodeBytes::NAME, env = "NERVIX_CLIENT_PRODUCER_NODE_BYTES", help = "Node-wide producer byte credit; defaults to a share of node memory")]
    pub client_producer_node_bytes: Option<HumanBytes>,
    #[arg(id = ClientConsumerNodeBytes::NAME, long = ClientConsumerNodeBytes::NAME, env = "NERVIX_CLIENT_CONSUMER_NODE_BYTES", help = "Node-wide consumer byte credit; defaults to a share of node memory")]
    pub client_consumer_node_bytes: Option<HumanBytes>,
    #[arg(id = ControlCpuWorkers::NAME, long = ControlCpuWorkers::NAME, env = "NERVIX_EXECUTION_CONTROL_CPU_WORKERS", help = "Maximum admitted workers for the control cpu class; omitted values derive from resolved node capacity")]
    pub control_cpu_workers: Option<NonZeroUsize>,
    #[arg(id = ControlCpuPendingJobs::NAME, long = ControlCpuPendingJobs::NAME, env = "NERVIX_EXECUTION_CONTROL_CPU_PENDING_JOBS", help = "Maximum pending jobs for the control cpu class; omitted values derive from resolved node capacity")]
    pub control_cpu_pending_jobs: Option<NonZeroUsize>,
    #[arg(id = CredentialsCpuWorkers::NAME, long = CredentialsCpuWorkers::NAME, env = "NERVIX_EXECUTION_CREDENTIALS_CPU_WORKERS", help = "Maximum admitted workers for the credentials cpu class; omitted values derive from resolved node capacity")]
    pub credentials_cpu_workers: Option<NonZeroUsize>,
    #[arg(id = CredentialsCpuPendingJobs::NAME, long = CredentialsCpuPendingJobs::NAME, env = "NERVIX_EXECUTION_CREDENTIALS_CPU_PENDING_JOBS", help = "Maximum pending jobs for the credentials cpu class; omitted values derive from resolved node capacity")]
    pub credentials_cpu_pending_jobs: Option<NonZeroUsize>,
    #[arg(id = DataCpuWorkers::NAME, long = DataCpuWorkers::NAME, env = "NERVIX_EXECUTION_DATA_CPU_WORKERS", help = "Maximum admitted workers for the data cpu class; omitted values derive from resolved node capacity")]
    pub data_cpu_workers: Option<NonZeroUsize>,
    #[arg(id = DataCpuPendingJobs::NAME, long = DataCpuPendingJobs::NAME, env = "NERVIX_EXECUTION_DATA_CPU_PENDING_JOBS", help = "Maximum pending jobs for the data cpu class; omitted values derive from resolved node capacity")]
    pub data_cpu_pending_jobs: Option<NonZeroUsize>,
    #[arg(id = ExtensionCpuWorkers::NAME, long = ExtensionCpuWorkers::NAME, env = "NERVIX_EXECUTION_EXTENSION_CPU_WORKERS", help = "Maximum admitted workers for the extension cpu class; omitted values derive from resolved node capacity")]
    pub extension_cpu_workers: Option<NonZeroUsize>,
    #[arg(id = ExtensionCpuPendingJobs::NAME, long = ExtensionCpuPendingJobs::NAME, env = "NERVIX_EXECUTION_EXTENSION_CPU_PENDING_JOBS", help = "Maximum pending jobs for the extension cpu class; omitted values derive from resolved node capacity")]
    pub extension_cpu_pending_jobs: Option<NonZeroUsize>,
    #[arg(id = BulkCpuWorkers::NAME, long = BulkCpuWorkers::NAME, env = "NERVIX_EXECUTION_BULK_CPU_WORKERS", help = "Maximum admitted workers for the bulk cpu class; omitted values derive from resolved node capacity")]
    pub bulk_cpu_workers: Option<NonZeroUsize>,
    #[arg(id = BulkCpuPendingJobs::NAME, long = BulkCpuPendingJobs::NAME, env = "NERVIX_EXECUTION_BULK_CPU_PENDING_JOBS", help = "Maximum pending jobs for the bulk cpu class; omitted values derive from resolved node capacity")]
    pub bulk_cpu_pending_jobs: Option<NonZeroUsize>,
    #[arg(id = ConsensusStorageWorkers::NAME, long = ConsensusStorageWorkers::NAME, env = "NERVIX_EXECUTION_CONSENSUS_STORAGE_WORKERS", help = "Maximum admitted workers for the consensus storage class; omitted values derive from resolved node capacity")]
    pub consensus_storage_workers: Option<NonZeroUsize>,
    #[arg(id = ConsensusStoragePendingJobs::NAME, long = ConsensusStoragePendingJobs::NAME, env = "NERVIX_EXECUTION_CONSENSUS_STORAGE_PENDING_JOBS", help = "Maximum pending jobs for the consensus storage class; omitted values derive from resolved node capacity")]
    pub consensus_storage_pending_jobs: Option<NonZeroUsize>,
    #[arg(id = FilesystemStorageWorkers::NAME, long = FilesystemStorageWorkers::NAME, env = "NERVIX_EXECUTION_FILESYSTEM_STORAGE_WORKERS", help = "Maximum admitted workers for the filesystem storage class; omitted values derive from resolved node capacity")]
    pub filesystem_storage_workers: Option<NonZeroUsize>,
    #[arg(id = FilesystemStoragePendingJobs::NAME, long = FilesystemStoragePendingJobs::NAME, env = "NERVIX_EXECUTION_FILESYSTEM_STORAGE_PENDING_JOBS", help = "Maximum pending jobs for the filesystem storage class; omitted values derive from resolved node capacity")]
    pub filesystem_storage_pending_jobs: Option<NonZeroUsize>,
    #[arg(id = ConnectorStorageWorkers::NAME, long = ConnectorStorageWorkers::NAME, env = "NERVIX_EXECUTION_CONNECTOR_STORAGE_WORKERS", help = "Maximum admitted workers for the connector storage class; omitted values derive from resolved node capacity")]
    pub connector_storage_workers: Option<NonZeroUsize>,
    #[arg(id = ConnectorStoragePendingJobs::NAME, long = ConnectorStoragePendingJobs::NAME, env = "NERVIX_EXECUTION_CONNECTOR_STORAGE_PENDING_JOBS", help = "Maximum pending jobs for the connector storage class; omitted values derive from resolved node capacity")]
    pub connector_storage_pending_jobs: Option<NonZeroUsize>,
    #[arg(id = ManagementMemory::NAME, long = ManagementMemory::NAME, env = "NERVIX_EXECUTION_MANAGEMENT_MEMORY_BYTES", help = "Reserved management capacity; omitted values derive from node memory")]
    pub management_memory_bytes: Option<HumanBytes>,
    #[arg(id = CommandsMemory::NAME, long = CommandsMemory::NAME, env = "NERVIX_EXECUTION_COMMANDS_MEMORY_BYTES", help = "Reserved commands capacity; omitted values derive from node memory")]
    pub commands_memory_bytes: Option<HumanBytes>,
    #[arg(id = RelayMemory::NAME, long = RelayMemory::NAME, env = "NERVIX_EXECUTION_RELAY_MEMORY_BYTES", help = "Reserved relay capacity; omitted values derive from node memory")]
    pub relay_memory_bytes: Option<HumanBytes>,
    #[arg(id = StateReadMemory::NAME, long = StateReadMemory::NAME, env = "NERVIX_EXECUTION_STATE_READ_MEMORY_BYTES", help = "Reserved state read capacity; omitted values derive from node memory")]
    pub state_read_memory_bytes: Option<HumanBytes>,
    #[arg(id = StateReadCacheMemory::NAME, long = StateReadCacheMemory::NAME, env = "NERVIX_EXECUTION_STATE_READ_CACHE_MEMORY_BYTES", help = "Reserved state read cache capacity; omitted values derive from node memory")]
    pub state_read_cache_memory_bytes: Option<HumanBytes>,
    #[arg(id = BulkMemory::NAME, long = BulkMemory::NAME, env = "NERVIX_EXECUTION_BULK_MEMORY_BYTES", help = "Reserved bulk capacity; omitted values derive from node memory")]
    pub bulk_memory_bytes: Option<HumanBytes>,
    #[arg(id = BulkWindowMemory::NAME, long = BulkWindowMemory::NAME, env = "NERVIX_EXECUTION_BULK_WINDOW_MEMORY_BYTES", help = "Reserved bulk window capacity; omitted values derive from node memory")]
    pub bulk_window_memory_bytes: Option<HumanBytes>,
    #[arg(id = RestoreMetadataMemory::NAME, long = RestoreMetadataMemory::NAME, env = "NERVIX_EXECUTION_RESTORE_METADATA_MEMORY_BYTES", help = "Reserved restore metadata capacity; omitted values derive from node memory")]
    pub restore_metadata_memory_bytes: Option<HumanBytes>,
    #[arg(id = CredentialsMemory::NAME, long = CredentialsMemory::NAME, env = "NERVIX_EXECUTION_CREDENTIALS_MEMORY_BYTES", help = "Reserved credentials capacity; omitted values derive from node memory")]
    pub credentials_memory_bytes: Option<HumanBytes>,
    #[arg(id = IntakeMemory::NAME, long = IntakeMemory::NAME, env = "NERVIX_EXECUTION_INTAKE_MEMORY_BYTES", help = "Retained public input capacity, separate from decode workers; defaults to a share of node memory")]
    pub intake_memory_bytes: Option<HumanBytes>,
    #[arg(id = RelayEncodedBytes::NAME, long = RelayEncodedBytes::NAME, env = "NERVIX_EXECUTION_RELAY_ENCODED_BYTES", help = "Local relay encoded bytes bound; omitted values fit the resolved class budget")]
    pub relay_encoded_bytes: Option<HumanBytes>,
    #[arg(id = RelayDecodedBytes::NAME, long = RelayDecodedBytes::NAME, env = "NERVIX_EXECUTION_RELAY_DECODED_BYTES", help = "Local relay decoded bytes bound; omitted values fit the resolved class budget")]
    pub relay_decoded_bytes: Option<HumanBytes>,
    #[arg(id = RelayScratchBytes::NAME, long = RelayScratchBytes::NAME, env = "NERVIX_EXECUTION_RELAY_SCRATCH_BYTES", help = "Local relay scratch bytes bound; omitted values fit the resolved class budget")]
    pub relay_scratch_bytes: Option<HumanBytes>,
    #[arg(id = ResidentReplicationBatches::NAME, long = ResidentReplicationBatches::NAME, env = "NERVIX_EXECUTION_RESIDENT_REPLICATION_BATCHES", help = "Local resident replication batches bound; omitted values fit the resolved class budget")]
    pub resident_replication_batches: Option<NonZeroU32>,
    #[arg(id = BulkChunkBytes::NAME, long = BulkChunkBytes::NAME, env = "NERVIX_EXECUTION_BULK_CHUNK_BYTES", help = "Local bulk chunk bytes bound; omitted values fit the resolved class budget")]
    pub bulk_chunk_bytes: Option<HumanBytes>,
}

impl ExecutionArgs {
    pub(super) fn runtime_threads(&self) -> RuntimeThreadOverrides {
        RuntimeThreadOverrides {
            worker_threads: self.runtime_worker_threads,
            max_blocking_threads: self.runtime_max_blocking_threads,
            thread_stack_size: self.runtime_thread_stack_size.map(|bytes| bytes.0),
        }
    }
    pub(super) fn client_endpoints(
        &self,
    ) -> Result<ClientEndpointOverrides, error_stack::Report<ClientEndpointCapacityError>> {
        let overrides = ClientEndpointOverrides {
            producer_node_bytes: self.client_producer_node_bytes.map(|bytes| bytes.0),
            consumer_node_bytes: self.client_consumer_node_bytes.map(|bytes| bytes.0),
        };
        ClientEndpointSettings::resolve(overrides, None)?;
        Ok(overrides)
    }
    pub(super) fn overrides(&self) -> ExecutionOverrides {
        ExecutionOverrides {
            control_cpu_workers: self.control_cpu_workers,
            control_cpu_pending_jobs: self.control_cpu_pending_jobs,
            credentials_cpu_workers: self.credentials_cpu_workers,
            credentials_cpu_pending_jobs: self.credentials_cpu_pending_jobs,
            data_cpu_workers: self.data_cpu_workers,
            data_cpu_pending_jobs: self.data_cpu_pending_jobs,
            extension_cpu_workers: self.extension_cpu_workers,
            extension_cpu_pending_jobs: self.extension_cpu_pending_jobs,
            bulk_cpu_workers: self.bulk_cpu_workers,
            bulk_cpu_pending_jobs: self.bulk_cpu_pending_jobs,
            consensus_storage_workers: self.consensus_storage_workers,
            consensus_storage_pending_jobs: self.consensus_storage_pending_jobs,
            filesystem_storage_workers: self.filesystem_storage_workers,
            filesystem_storage_pending_jobs: self.filesystem_storage_pending_jobs,
            connector_storage_workers: self.connector_storage_workers,
            connector_storage_pending_jobs: self.connector_storage_pending_jobs,
            management_memory_bytes: self.management_memory_bytes.map(|bytes| bytes.0),
            commands_memory_bytes: self.commands_memory_bytes.map(|bytes| bytes.0),
            relay_memory_bytes: self.relay_memory_bytes.map(|bytes| bytes.0),
            state_read_memory_bytes: self.state_read_memory_bytes.map(|bytes| bytes.0),
            state_read_cache_memory_bytes: self.state_read_cache_memory_bytes.map(|bytes| bytes.0),
            bulk_memory_bytes: self.bulk_memory_bytes.map(|bytes| bytes.0),
            bulk_window_memory_bytes: self.bulk_window_memory_bytes.map(|bytes| bytes.0),
            restore_metadata_memory_bytes: self.restore_metadata_memory_bytes.map(|bytes| bytes.0),
            credentials_memory_bytes: self.credentials_memory_bytes.map(|bytes| bytes.0),
            intake_memory_bytes: self.intake_memory_bytes.map(|bytes| bytes.0),
            relay_encoded_bytes: self.relay_encoded_bytes.map(|bytes| bytes.0),
            relay_decoded_bytes: self.relay_decoded_bytes.map(|bytes| bytes.0),
            relay_scratch_bytes: self.relay_scratch_bytes.map(|bytes| bytes.0),
            resident_replication_batches: self.resident_replication_batches,
            bulk_chunk_bytes: self.bulk_chunk_bytes.map(|bytes| bytes.0),
        }
    }

    pub(super) fn from_overrides(
        settings: &ExecutionOverrides,
        clients: &ClientEndpointOverrides,
        runtime: &RuntimeThreadOverrides,
    ) -> Self {
        Self {
            runtime_worker_threads: runtime.worker_threads,
            runtime_max_blocking_threads: runtime.max_blocking_threads,
            runtime_thread_stack_size: runtime.thread_stack_size.map(HumanBytes),
            client_producer_node_bytes: clients.producer_node_bytes.map(HumanBytes),
            client_consumer_node_bytes: clients.consumer_node_bytes.map(HumanBytes),
            control_cpu_workers: settings.control_cpu_workers,
            control_cpu_pending_jobs: settings.control_cpu_pending_jobs,
            credentials_cpu_workers: settings.credentials_cpu_workers,
            credentials_cpu_pending_jobs: settings.credentials_cpu_pending_jobs,
            data_cpu_workers: settings.data_cpu_workers,
            data_cpu_pending_jobs: settings.data_cpu_pending_jobs,
            extension_cpu_workers: settings.extension_cpu_workers,
            extension_cpu_pending_jobs: settings.extension_cpu_pending_jobs,
            bulk_cpu_workers: settings.bulk_cpu_workers,
            bulk_cpu_pending_jobs: settings.bulk_cpu_pending_jobs,
            consensus_storage_workers: settings.consensus_storage_workers,
            consensus_storage_pending_jobs: settings.consensus_storage_pending_jobs,
            filesystem_storage_workers: settings.filesystem_storage_workers,
            filesystem_storage_pending_jobs: settings.filesystem_storage_pending_jobs,
            connector_storage_workers: settings.connector_storage_workers,
            connector_storage_pending_jobs: settings.connector_storage_pending_jobs,
            management_memory_bytes: settings.management_memory_bytes.map(HumanBytes),
            commands_memory_bytes: settings.commands_memory_bytes.map(HumanBytes),
            relay_memory_bytes: settings.relay_memory_bytes.map(HumanBytes),
            state_read_memory_bytes: settings.state_read_memory_bytes.map(HumanBytes),
            state_read_cache_memory_bytes: settings.state_read_cache_memory_bytes.map(HumanBytes),
            bulk_memory_bytes: settings.bulk_memory_bytes.map(HumanBytes),
            bulk_window_memory_bytes: settings.bulk_window_memory_bytes.map(HumanBytes),
            restore_metadata_memory_bytes: settings.restore_metadata_memory_bytes.map(HumanBytes),
            credentials_memory_bytes: settings.credentials_memory_bytes.map(HumanBytes),
            intake_memory_bytes: settings.intake_memory_bytes.map(HumanBytes),
            relay_encoded_bytes: settings.relay_encoded_bytes.map(HumanBytes),
            relay_decoded_bytes: settings.relay_decoded_bytes.map(HumanBytes),
            relay_scratch_bytes: settings.relay_scratch_bytes.map(HumanBytes),
            resident_replication_batches: settings.resident_replication_batches,
            bulk_chunk_bytes: settings.bulk_chunk_bytes.map(HumanBytes),
        }
    }

    pub(super) fn render(&self, command_line: &mut CommandLine) {
        command_line.optional(RuntimeWorkerThreads::NAME, self.runtime_worker_threads);
        command_line.optional(
            RuntimeMaxBlockingThreads::NAME,
            self.runtime_max_blocking_threads,
        );
        command_line.optional(RuntimeThreadStackSize::NAME, self.runtime_thread_stack_size);
        command_line.optional(
            ClientProducerNodeBytes::NAME,
            self.client_producer_node_bytes,
        );
        command_line.optional(
            ClientConsumerNodeBytes::NAME,
            self.client_consumer_node_bytes,
        );
        command_line.optional(ControlCpuWorkers::NAME, self.control_cpu_workers);
        command_line.optional(ControlCpuPendingJobs::NAME, self.control_cpu_pending_jobs);
        command_line.optional(CredentialsCpuWorkers::NAME, self.credentials_cpu_workers);
        command_line.optional(
            CredentialsCpuPendingJobs::NAME,
            self.credentials_cpu_pending_jobs,
        );
        command_line.optional(DataCpuWorkers::NAME, self.data_cpu_workers);
        command_line.optional(DataCpuPendingJobs::NAME, self.data_cpu_pending_jobs);
        command_line.optional(ExtensionCpuWorkers::NAME, self.extension_cpu_workers);
        command_line.optional(
            ExtensionCpuPendingJobs::NAME,
            self.extension_cpu_pending_jobs,
        );
        command_line.optional(BulkCpuWorkers::NAME, self.bulk_cpu_workers);
        command_line.optional(BulkCpuPendingJobs::NAME, self.bulk_cpu_pending_jobs);
        command_line.optional(
            ConsensusStorageWorkers::NAME,
            self.consensus_storage_workers,
        );
        command_line.optional(
            ConsensusStoragePendingJobs::NAME,
            self.consensus_storage_pending_jobs,
        );
        command_line.optional(
            FilesystemStorageWorkers::NAME,
            self.filesystem_storage_workers,
        );
        command_line.optional(
            FilesystemStoragePendingJobs::NAME,
            self.filesystem_storage_pending_jobs,
        );
        command_line.optional(
            ConnectorStorageWorkers::NAME,
            self.connector_storage_workers,
        );
        command_line.optional(
            ConnectorStoragePendingJobs::NAME,
            self.connector_storage_pending_jobs,
        );
        command_line.optional(ManagementMemory::NAME, self.management_memory_bytes);
        command_line.optional(CommandsMemory::NAME, self.commands_memory_bytes);
        command_line.optional(RelayMemory::NAME, self.relay_memory_bytes);
        command_line.optional(StateReadMemory::NAME, self.state_read_memory_bytes);
        command_line.optional(
            StateReadCacheMemory::NAME,
            self.state_read_cache_memory_bytes,
        );
        command_line.optional(BulkMemory::NAME, self.bulk_memory_bytes);
        command_line.optional(BulkWindowMemory::NAME, self.bulk_window_memory_bytes);
        command_line.optional(
            RestoreMetadataMemory::NAME,
            self.restore_metadata_memory_bytes,
        );
        command_line.optional(CredentialsMemory::NAME, self.credentials_memory_bytes);
        command_line.optional(IntakeMemory::NAME, self.intake_memory_bytes);
        command_line.optional(RelayEncodedBytes::NAME, self.relay_encoded_bytes);
        command_line.optional(RelayDecodedBytes::NAME, self.relay_decoded_bytes);
        command_line.optional(RelayScratchBytes::NAME, self.relay_scratch_bytes);
        command_line.optional(
            ResidentReplicationBatches::NAME,
            self.resident_replication_batches,
        );
        command_line.optional(BulkChunkBytes::NAME, self.bulk_chunk_bytes);
    }
}
