//! Capability-derived execution settings and their checked projection into executor limits.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Execution setting declarations, CPU shares, per-class queues and memory derivations.
//! - **Depends on.** Typed node capabilities, setting definitions and the executor's limits.
//! - **Must not know.** Detection, command-line parsing, graphs, peers or running tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns capability-derived executor configuration")
)]

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use nervix_primitives::sync::Semaphore;
use ubyte::ByteUnit;

use crate::{ExecutionConfig, ExecutionConfigError, MemoryBudgets, OperationLimits, WorkerCounts};

/// Local overrides kept optional until the node has resolved its detected capabilities.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionOverrides {
    pub control_cpu_workers: Option<NonZeroUsize>,
    pub control_cpu_pending_jobs: Option<NonZeroUsize>,
    pub credentials_cpu_workers: Option<NonZeroUsize>,
    pub credentials_cpu_pending_jobs: Option<NonZeroUsize>,
    pub data_cpu_workers: Option<NonZeroUsize>,
    pub data_cpu_pending_jobs: Option<NonZeroUsize>,
    pub extension_cpu_workers: Option<NonZeroUsize>,
    pub extension_cpu_pending_jobs: Option<NonZeroUsize>,
    pub bulk_cpu_workers: Option<NonZeroUsize>,
    pub bulk_cpu_pending_jobs: Option<NonZeroUsize>,
    pub consensus_storage_workers: Option<NonZeroUsize>,
    pub consensus_storage_pending_jobs: Option<NonZeroUsize>,
    pub filesystem_storage_workers: Option<NonZeroUsize>,
    pub filesystem_storage_pending_jobs: Option<NonZeroUsize>,
    pub connector_storage_workers: Option<NonZeroUsize>,
    pub connector_storage_pending_jobs: Option<NonZeroUsize>,
    pub management_memory_bytes: Option<ByteUnit>,
    pub commands_memory_bytes: Option<ByteUnit>,
    pub relay_memory_bytes: Option<ByteUnit>,
    pub state_read_memory_bytes: Option<ByteUnit>,
    pub state_read_cache_memory_bytes: Option<ByteUnit>,
    pub bulk_memory_bytes: Option<ByteUnit>,
    pub bulk_window_memory_bytes: Option<ByteUnit>,
    pub restore_metadata_memory_bytes: Option<ByteUnit>,
    pub credentials_memory_bytes: Option<ByteUnit>,
    pub intake_memory_bytes: Option<ByteUnit>,
    pub relay_encoded_bytes: Option<ByteUnit>,
    pub relay_decoded_bytes: Option<ByteUnit>,
    pub relay_scratch_bytes: Option<ByteUnit>,
    pub resident_replication_batches: Option<NonZeroU32>,
    pub bulk_chunk_bytes: Option<ByteUnit>,
}

/// Independently bounded queues; a blocked class never consumes another class's waiting places.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingJobCounts {
    pub control_cpu: NonZeroUsize,
    pub credentials_cpu: NonZeroUsize,
    pub data_cpu: NonZeroUsize,
    pub extension_cpu: NonZeroUsize,
    pub bulk_cpu: NonZeroUsize,
    pub consensus_storage: NonZeroUsize,
    pub filesystem_storage: NonZeroUsize,
    pub connector_storage: NonZeroUsize,
}

impl PendingJobCounts {
    /// A uniform explicit queue bound, used by focused admission tests.
    pub const fn uniform(capacity: NonZeroUsize) -> Self {
        Self {
            control_cpu: capacity,
            credentials_cpu: capacity,
            data_cpu: capacity,
            extension_cpu: capacity,
            bulk_cpu: capacity,
            consensus_storage: capacity,
            filesystem_storage: capacity,
            connector_storage: capacity,
        }
    }
}

/// The CPU-share defaults, after reserving the control and credentials capacities.
#[derive(Debug)]
pub struct WorkerDefaults {
    control_cpu: NonZeroUsize,
    credentials_cpu: NonZeroUsize,
    data_cpu: NonZeroUsize,
    extension_cpu: NonZeroUsize,
    bulk_cpu: NonZeroUsize,
    consensus_storage: NonZeroUsize,
    filesystem_storage: NonZeroUsize,
}

impl WorkerDefaults {
    fn resolve(
        overrides: &ExecutionOverrides,
        cpus: Option<NonZeroU64>,
    ) -> Result<Self, Report<ExecutionConfigError>> {
        let control = overrides.control_cpu_workers.unwrap_or(NonZeroUsize::MIN);
        let credentials = overrides
            .credentials_cpu_workers
            .unwrap_or(NonZeroUsize::MIN);
        let cpu_count = match cpus {
            Some(cpus) => usize::try_from(cpus.get()).map_err(|_| {
                Report::new(ExecutionConfigError::CpuCountTooLarge { cpus: cpus.get() })
            })?,
            None => NonZeroUsize::MIN.get(),
        };
        let reserved = control
            .get()
            .checked_add(credentials.get())
            .ok_or_else(|| Report::new(ExecutionConfigError::UnaddressableWorkers))?;
        // Three independent compute classes each need one admission place on small nodes.
        // This clamps a CPU allocation, not arithmetic used to decode or account for data.
        let available = match cpu_count.checked_sub(reserved) {
            Some(remaining) => remaining.max(3),
            None => 3,
        };
        let requests = [
            overrides.data_cpu_workers,
            overrides.extension_cpu_workers,
            overrides.bulk_cpu_workers,
        ];
        let mut requested = 0usize;
        let mut omitted = 0usize;
        for value in requests {
            match value {
                Some(value) => {
                    requested = requested
                        .checked_add(value.get())
                        .ok_or_else(|| Report::new(ExecutionConfigError::UnaddressableWorkers))?
                }
                None => {
                    omitted = omitted
                        .checked_add(1)
                        .assured("there are exactly three compute classes")
                }
            }
        }
        // Explicit over-allocation is rejected below; omitted defaults must still be positive.
        let free = match available.checked_sub(requested) {
            Some(remaining) => remaining.max(omitted),
            None => omitted,
        };
        let mut shares = [NonZeroUsize::MIN; 3];
        let mut ordinal = 0usize;
        for (index, request) in requests.into_iter().enumerate() {
            shares[index] = match request {
                Some(value) => value,
                None => {
                    let share = free
                        .checked_div(omitted)
                        .verified("an omitted class makes the divisor positive");
                    let remainder = free
                        .checked_rem(omitted)
                        .verified("an omitted class makes the divisor positive");
                    let extra = usize::from(ordinal < remainder);
                    ordinal = ordinal
                        .checked_add(1)
                        .assured("there are exactly three compute classes");
                    NonZeroUsize::new(
                        share
                            .checked_add(extra)
                            .assured("the share is bounded by the CPU count"),
                    )
                    .verified("every omitted class was granted at least one place")
                }
            };
        }
        let mut total = reserved;
        for share in shares {
            total = total
                .checked_add(share.get())
                .ok_or_else(|| Report::new(ExecutionConfigError::UnaddressableWorkers))?;
        }
        if let Some(cpus) = cpus {
            // Five independently usable CPU classes are the minimum service concurrency.
            let maximum = cpus.get().max(5);
            if u64::try_from(total).assured("usize fits u64 on supported targets") > maximum {
                return Err(Report::new(ExecutionConfigError::CpuBudgetExceeded {
                    workers: total,
                    cpus: cpus.get(),
                }));
            }
        }
        Ok(Self {
            control_cpu: control,
            credentials_cpu: credentials,
            data_cpu: shares[0],
            extension_cpu: shares[1],
            bulk_cpu: shares[2],
            consensus_storage: NonZeroUsize::MIN,
            filesystem_storage: NonZeroUsize::new(2).verified("two filesystem workers are nonzero"),
        })
    }
}

macro_rules! worker_setting {
    ($definition:ident, $name:literal, $field:ident) => {
        pub struct $definition;
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = NonZeroUsize;
            type Context = WorkerDefaults;
            type Error = ExecutionConfigError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> =
                Some("the node's independent CPU and storage allocation");
            fn default_value(context: &WorkerDefaults) -> NonZeroUsize {
                context.$field
            }
            fn validate(
                value: &NonZeroUsize,
                _: &WorkerDefaults,
            ) -> Result<(), Report<Self::Error>> {
                validate_count(Self::NAME, *value)
            }
        }
    };
}

macro_rules! queue_setting {
    ($definition:ident, $name:literal) => {
        pub struct $definition;
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = NonZeroUsize;
            type Context = NonZeroUsize;
            type Error = ExecutionConfigError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> =
                Some("64 pending jobs per worker, at most 1024 per class");
            fn default_value(workers: &NonZeroUsize) -> NonZeroUsize {
                let maximum = 1024;
                let capacity = match workers.get().checked_mul(64) {
                    Some(capacity) => capacity.min(maximum),
                    None => maximum,
                };
                NonZeroUsize::new(capacity)
                    .assured("positive workers yield positive queue capacity")
            }
            fn validate(value: &NonZeroUsize, _: &NonZeroUsize) -> Result<(), Report<Self::Error>> {
                validate_count(Self::NAME, *value)
            }
        }
    };
}

fn validate_count(
    name: &'static str,
    value: NonZeroUsize,
) -> Result<(), Report<ExecutionConfigError>> {
    if value.get() > Semaphore::MAX_PERMITS {
        return Err(Report::new(ExecutionConfigError::WorkerCountTooLarge {
            class: name,
            workers: value.get(),
            maximum: Semaphore::MAX_PERMITS,
        }));
    }
    Ok(())
}

worker_setting!(
    ControlCpuWorkers,
    "execution-control-cpu-workers",
    control_cpu
);
queue_setting!(ControlCpuPendingJobs, "execution-control-cpu-pending-jobs");
worker_setting!(
    CredentialsCpuWorkers,
    "execution-credentials-cpu-workers",
    credentials_cpu
);
queue_setting!(
    CredentialsCpuPendingJobs,
    "execution-credentials-cpu-pending-jobs"
);
worker_setting!(DataCpuWorkers, "execution-data-cpu-workers", data_cpu);
queue_setting!(DataCpuPendingJobs, "execution-data-cpu-pending-jobs");
worker_setting!(
    ExtensionCpuWorkers,
    "execution-extension-cpu-workers",
    extension_cpu
);
queue_setting!(
    ExtensionCpuPendingJobs,
    "execution-extension-cpu-pending-jobs"
);
worker_setting!(BulkCpuWorkers, "execution-bulk-cpu-workers", bulk_cpu);
queue_setting!(BulkCpuPendingJobs, "execution-bulk-cpu-pending-jobs");
worker_setting!(
    ConsensusStorageWorkers,
    "execution-consensus-storage-workers",
    consensus_storage
);
queue_setting!(
    ConsensusStoragePendingJobs,
    "execution-consensus-storage-pending-jobs"
);
worker_setting!(
    FilesystemStorageWorkers,
    "execution-filesystem-storage-workers",
    filesystem_storage
);
queue_setting!(
    FilesystemStoragePendingJobs,
    "execution-filesystem-storage-pending-jobs"
);
queue_setting!(
    ConnectorStoragePendingJobs,
    "execution-connector-storage-pending-jobs"
);

macro_rules! memory_setting {
    ($definition:ident, $name:literal, $default:expr, $divisor:expr, $minimum:expr) => {
        pub struct $definition;
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = ByteUnit;
            type Context = Option<NonZeroU64>;
            type Error = ExecutionConfigError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = Some(
                "a bounded fraction of the node memory, retaining the class's minimum working set",
            );
            fn default_value(memory: &Self::Context) -> ByteUnit {
                match memory {
                    Some(memory) => ByteUnit::Byte(
                        memory
                            .get()
                            .checked_div($divisor)
                            .assured("a memory fraction has a positive divisor")
                            .max($minimum)
                            .min(u64::from(u32::MAX)),
                    ),
                    None => ByteUnit::Mebibyte($default),
                }
            }
            fn validate(value: &ByteUnit, _: &Self::Context) -> Result<(), Report<Self::Error>> {
                validate_bytes(Self::NAME, *value)
            }
        }
    };
}

macro_rules! read_memory_setting {
    ($definition:ident, $name:literal) => {
        pub struct $definition;
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = ByteUnit;
            type Context = ByteUnit;
            type Error = ExecutionConfigError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> = Some("the resolved relay memory capacity");
            fn default_value(relay: &ByteUnit) -> ByteUnit {
                *relay
            }
            fn validate(value: &ByteUnit, _: &ByteUnit) -> Result<(), Report<Self::Error>> {
                validate_bytes(Self::NAME, *value)
            }
        }
    };
}

fn validate_bytes(name: &'static str, bytes: ByteUnit) -> Result<(), Report<ExecutionConfigError>> {
    if bytes.as_u64() == 0 {
        return Err(Report::new(ExecutionConfigError::EmptyBudget {
            class: name,
        }));
    }
    if bytes.as_u64() > u64::from(u32::MAX) {
        return Err(Report::new(ExecutionConfigError::BudgetTooLarge {
            class: name,
            budget: bytes.as_u64(),
        }));
    }
    Ok(())
}

memory_setting!(
    ManagementMemory,
    "execution-management-memory-bytes",
    8,
    128,
    64 * 1024
);
memory_setting!(
    CommandsMemory,
    "execution-commands-memory-bytes",
    24,
    32,
    24 * 1024 * 1024
);
memory_setting!(
    IntakeMemory,
    "execution-intake-memory-bytes",
    128,
    16,
    128 * 1024 * 1024
);
memory_setting!(RelayMemory, "execution-relay-memory-bytes", 192, 16, 1);
read_memory_setting!(StateReadMemory, "execution-state-read-memory-bytes");
read_memory_setting!(
    StateReadCacheMemory,
    "execution-state-read-cache-memory-bytes"
);
memory_setting!(
    BulkMemory,
    "execution-bulk-memory-bytes",
    32,
    32,
    32 * 1024 * 1024
);
memory_setting!(
    BulkWindowMemory,
    "execution-bulk-window-memory-bytes",
    16,
    64,
    16 * 1024 * 1024
);
memory_setting!(
    RestoreMetadataMemory,
    "execution-restore-metadata-memory-bytes",
    2048,
    8,
    1
);
memory_setting!(
    CredentialsMemory,
    "execution-credentials-memory-bytes",
    19,
    256,
    19 * 1024 * 1024
);

macro_rules! relay_limit_setting {
    ($definition:ident, $name:literal, $field:ident, $divisor:expr) => {
        pub struct $definition;
        impl SettingDefinition for $definition {
            type Class = NodeCapabilities;
            type Value = ByteUnit;
            type Context = ByteUnit;
            type Error = ExecutionConfigError;
            const NAME: &'static str = $name;
            const DERIVATION: Option<&'static str> =
                Some("a share of the relay budget, capped by the declared operation default");
            fn default_value(budget: &ByteUnit) -> ByteUnit {
                let defaults = OperationLimits::default();
                ByteUnit::Byte(
                    budget
                        .as_u64()
                        .checked_div($divisor)
                        .assured("the relay fraction has a positive divisor")
                        .max(1)
                        .min(defaults.$field.as_u64()),
                )
            }
            fn validate(value: &ByteUnit, _: &ByteUnit) -> Result<(), Report<Self::Error>> {
                validate_bytes(Self::NAME, *value)
            }
        }
    };
}
relay_limit_setting!(
    RelayEncodedBytes,
    "execution-relay-encoded-bytes",
    relay_encoded_bytes,
    6
);
relay_limit_setting!(
    RelayDecodedBytes,
    "execution-relay-decoded-bytes",
    relay_decoded_bytes,
    6
);
relay_limit_setting!(
    RelayScratchBytes,
    "execution-relay-scratch-bytes",
    relay_scratch_bytes,
    12
);

pub struct ResidentReplicationBatches;
impl SettingDefinition for ResidentReplicationBatches {
    type Class = NodeCapabilities;
    type Value = NonZeroU32;
    type Context = ByteUnit;
    type Error = ExecutionConfigError;
    const NAME: &'static str = "execution-resident-replication-batches";
    const DERIVATION: Option<&'static str> =
        Some("the command budget beside one encoded replication batch");
    fn default_value(budget: &ByteUnit) -> NonZeroU32 {
        let limits = OperationLimits::default();
        let held = budget
            .as_u64()
            .checked_div(limits.replication_batch_bytes.as_u64())
            .assured("the replication batch is positive");
        // Keep the answer's batch outside the resident window.
        let available = match held.checked_sub(1) {
            Some(remaining) => remaining.max(1),
            None => 1,
        };
        let batches = available.min(u64::from(limits.resident_replication_batches.get()));
        NonZeroU32::new(
            u32::try_from(batches).verified("the default resident count bounds this value"),
        )
        .verified("the resident count is positive")
    }
    fn validate(_: &NonZeroU32, _: &ByteUnit) -> Result<(), Report<Self::Error>> {
        Ok(())
    }
}

pub struct BulkChunkBytes;
impl SettingDefinition for BulkChunkBytes {
    type Class = NodeCapabilities;
    type Value = ByteUnit;
    type Context = ByteUnit;
    type Error = ExecutionConfigError;
    const NAME: &'static str = "execution-bulk-chunk-bytes";
    const DERIVATION: Option<&'static str> =
        Some("the bulk budget beside one sealed snapshot section");
    fn default_value(budget: &ByteUnit) -> ByteUnit {
        ByteUnit::Byte(
            budget
                .as_u64()
                .checked_div(512)
                .assured("the bulk fraction has a positive divisor")
                .max(1)
                .min(OperationLimits::default().bulk_chunk_bytes.as_u64()),
        )
    }
    fn validate(value: &ByteUnit, _: &ByteUnit) -> Result<(), Report<Self::Error>> {
        validate_bytes(Self::NAME, *value)
    }
}

/// Resolved worker capacities and their independent queue bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerSettings {
    pub control_cpu_workers: Setting<ControlCpuWorkers>,
    pub control_cpu_pending_jobs: Setting<ControlCpuPendingJobs>,
    pub credentials_cpu_workers: Setting<CredentialsCpuWorkers>,
    pub credentials_cpu_pending_jobs: Setting<CredentialsCpuPendingJobs>,
    pub data_cpu_workers: Setting<DataCpuWorkers>,
    pub data_cpu_pending_jobs: Setting<DataCpuPendingJobs>,
    pub extension_cpu_workers: Setting<ExtensionCpuWorkers>,
    pub extension_cpu_pending_jobs: Setting<ExtensionCpuPendingJobs>,
    pub bulk_cpu_workers: Setting<BulkCpuWorkers>,
    pub bulk_cpu_pending_jobs: Setting<BulkCpuPendingJobs>,
    pub consensus_storage_workers: Setting<ConsensusStorageWorkers>,
    pub consensus_storage_pending_jobs: Setting<ConsensusStoragePendingJobs>,
    pub filesystem_storage_workers: Setting<FilesystemStorageWorkers>,
    pub filesystem_storage_pending_jobs: Setting<FilesystemStoragePendingJobs>,
    pub connector_storage_workers: Setting<crate::ConnectorStorageWorkers>,
    pub connector_storage_pending_jobs: Setting<ConnectorStoragePendingJobs>,
}

impl WorkerSettings {
    pub fn resolve(
        overrides: &ExecutionOverrides,
        cpus: Option<NonZeroU64>,
    ) -> Result<Self, Report<ExecutionConfigError>> {
        let defaults = WorkerDefaults::resolve(overrides, cpus)?;
        let control_cpu_workers =
            Setting::<ControlCpuWorkers>::resolve(overrides.control_cpu_workers, &defaults)?;
        let control_cpu_pending_jobs = Setting::<ControlCpuPendingJobs>::resolve(
            overrides.control_cpu_pending_jobs,
            control_cpu_workers.value(),
        )?;
        let credentials_cpu_workers = Setting::<CredentialsCpuWorkers>::resolve(
            overrides.credentials_cpu_workers,
            &defaults,
        )?;
        let credentials_cpu_pending_jobs = Setting::<CredentialsCpuPendingJobs>::resolve(
            overrides.credentials_cpu_pending_jobs,
            credentials_cpu_workers.value(),
        )?;
        let data_cpu_workers =
            Setting::<DataCpuWorkers>::resolve(overrides.data_cpu_workers, &defaults)?;
        let data_cpu_pending_jobs = Setting::<DataCpuPendingJobs>::resolve(
            overrides.data_cpu_pending_jobs,
            data_cpu_workers.value(),
        )?;
        let extension_cpu_workers =
            Setting::<ExtensionCpuWorkers>::resolve(overrides.extension_cpu_workers, &defaults)?;
        let extension_cpu_pending_jobs = Setting::<ExtensionCpuPendingJobs>::resolve(
            overrides.extension_cpu_pending_jobs,
            extension_cpu_workers.value(),
        )?;
        let bulk_cpu_workers =
            Setting::<BulkCpuWorkers>::resolve(overrides.bulk_cpu_workers, &defaults)?;
        let bulk_cpu_pending_jobs = Setting::<BulkCpuPendingJobs>::resolve(
            overrides.bulk_cpu_pending_jobs,
            bulk_cpu_workers.value(),
        )?;
        let consensus_storage_workers = Setting::<ConsensusStorageWorkers>::resolve(
            overrides.consensus_storage_workers,
            &defaults,
        )?;
        let consensus_storage_pending_jobs = Setting::<ConsensusStoragePendingJobs>::resolve(
            overrides.consensus_storage_pending_jobs,
            consensus_storage_workers.value(),
        )?;
        let filesystem_storage_workers = Setting::<FilesystemStorageWorkers>::resolve(
            overrides.filesystem_storage_workers,
            &defaults,
        )?;
        let filesystem_storage_pending_jobs = Setting::<FilesystemStoragePendingJobs>::resolve(
            overrides.filesystem_storage_pending_jobs,
            filesystem_storage_workers.value(),
        )?;
        let connector_storage_workers =
            crate::ConnectorStorageWorkers::resolve(overrides.connector_storage_workers)?;
        let connector_storage_pending_jobs = Setting::<ConnectorStoragePendingJobs>::resolve(
            overrides.connector_storage_pending_jobs,
            connector_storage_workers.value(),
        )?;
        Ok(Self {
            control_cpu_workers,
            control_cpu_pending_jobs,
            credentials_cpu_workers,
            credentials_cpu_pending_jobs,
            data_cpu_workers,
            data_cpu_pending_jobs,
            extension_cpu_workers,
            extension_cpu_pending_jobs,
            bulk_cpu_workers,
            bulk_cpu_pending_jobs,
            consensus_storage_workers,
            consensus_storage_pending_jobs,
            filesystem_storage_workers,
            filesystem_storage_pending_jobs,
            connector_storage_workers,
            connector_storage_pending_jobs,
        })
    }

    pub fn counts(&self) -> WorkerCounts {
        WorkerCounts {
            control_cpu: *self.control_cpu_workers.value(),
            credentials_cpu: *self.credentials_cpu_workers.value(),
            data_cpu: *self.data_cpu_workers.value(),
            extension_cpu: *self.extension_cpu_workers.value(),
            bulk_cpu: *self.bulk_cpu_workers.value(),
            consensus_storage: *self.consensus_storage_workers.value(),
            filesystem_storage: *self.filesystem_storage_workers.value(),
            connector_storage: self.connector_storage_workers,
            pending_jobs: PendingJobCounts {
                control_cpu: *self.control_cpu_pending_jobs.value(),
                credentials_cpu: *self.credentials_cpu_pending_jobs.value(),
                data_cpu: *self.data_cpu_pending_jobs.value(),
                extension_cpu: *self.extension_cpu_pending_jobs.value(),
                bulk_cpu: *self.bulk_cpu_pending_jobs.value(),
                consensus_storage: *self.consensus_storage_pending_jobs.value(),
                filesystem_storage: *self.filesystem_storage_pending_jobs.value(),
                connector_storage: *self.connector_storage_pending_jobs.value(),
            },
        }
    }
}

/// Resolved, class-typed settings; the executor receives only their checked numeric projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionSettings {
    pub workers: WorkerSettings,
    pub management_memory_bytes: Setting<ManagementMemory>,
    pub commands_memory_bytes: Setting<CommandsMemory>,
    pub relay_memory_bytes: Setting<RelayMemory>,
    pub state_read_memory_bytes: Setting<StateReadMemory>,
    pub state_read_cache_memory_bytes: Setting<StateReadCacheMemory>,
    pub bulk_memory_bytes: Setting<BulkMemory>,
    pub bulk_window_memory_bytes: Setting<BulkWindowMemory>,
    pub restore_metadata_memory_bytes: Setting<RestoreMetadataMemory>,
    pub credentials_memory_bytes: Setting<CredentialsMemory>,
    pub intake_memory_bytes: Setting<IntakeMemory>,
    pub relay_encoded_bytes: Setting<RelayEncodedBytes>,
    pub relay_decoded_bytes: Setting<RelayDecodedBytes>,
    pub relay_scratch_bytes: Setting<RelayScratchBytes>,
    pub resident_replication_batches: Setting<ResidentReplicationBatches>,
    pub bulk_chunk_bytes: Setting<BulkChunkBytes>,
}

impl ExecutionSettings {
    pub fn resolve(
        overrides: ExecutionOverrides,
        cpus: Option<NonZeroU64>,
        memory: Option<NonZeroU64>,
    ) -> Result<Self, Report<ExecutionConfigError>> {
        let workers = WorkerSettings::resolve(&overrides, cpus)?;
        let relay_memory_bytes =
            Setting::<RelayMemory>::resolve(overrides.relay_memory_bytes, &memory)?;
        let management_memory_bytes =
            Setting::<ManagementMemory>::resolve(overrides.management_memory_bytes, &memory)?;
        let commands_memory_bytes =
            Setting::<CommandsMemory>::resolve(overrides.commands_memory_bytes, &memory)?;
        let state_read_memory_bytes = Setting::<StateReadMemory>::resolve(
            overrides.state_read_memory_bytes,
            relay_memory_bytes.value(),
        )?;
        let state_read_cache_memory_bytes = Setting::<StateReadCacheMemory>::resolve(
            overrides.state_read_cache_memory_bytes,
            relay_memory_bytes.value(),
        )?;
        let bulk_memory_bytes =
            Setting::<BulkMemory>::resolve(overrides.bulk_memory_bytes, &memory)?;
        let bulk_window_memory_bytes =
            Setting::<BulkWindowMemory>::resolve(overrides.bulk_window_memory_bytes, &memory)?;
        let restore_metadata_memory_bytes = Setting::<RestoreMetadataMemory>::resolve(
            overrides.restore_metadata_memory_bytes,
            &memory,
        )?;
        let intake_memory_bytes =
            Setting::<IntakeMemory>::resolve(overrides.intake_memory_bytes, &memory)?;
        let credentials_memory_bytes =
            Setting::<CredentialsMemory>::resolve(overrides.credentials_memory_bytes, &memory)?;
        let settings = Self {
            workers,
            management_memory_bytes,
            commands_memory_bytes,
            relay_memory_bytes,
            state_read_memory_bytes,
            state_read_cache_memory_bytes,
            bulk_memory_bytes,
            bulk_window_memory_bytes,
            restore_metadata_memory_bytes,
            credentials_memory_bytes,
            intake_memory_bytes,
            relay_encoded_bytes: Setting::resolve(
                overrides.relay_encoded_bytes,
                relay_memory_bytes.value(),
            )?,
            relay_decoded_bytes: Setting::resolve(
                overrides.relay_decoded_bytes,
                relay_memory_bytes.value(),
            )?,
            relay_scratch_bytes: Setting::resolve(
                overrides.relay_scratch_bytes,
                relay_memory_bytes.value(),
            )?,
            resident_replication_batches: Setting::resolve(
                overrides.resident_replication_batches,
                commands_memory_bytes.value(),
            )?,
            bulk_chunk_bytes: Setting::resolve(
                overrides.bulk_chunk_bytes,
                bulk_memory_bytes.value(),
            )?,
        };
        let config = settings.config();
        config.validate()?;
        if let Some(memory) = memory {
            let budgets = config
                .budgets
                .total()
                .verified("individually validated permit budgets sum within u64");
            if budgets.as_u64() > memory.get() {
                return Err(Report::new(ExecutionConfigError::MemoryBudgetExceeded {
                    budgets,
                    memory: ByteUnit::Byte(memory.get()),
                }));
            }
        }
        Ok(settings)
    }

    /// The validated configuration consumed by the executor; all defaults live in declarations.
    pub fn config(&self) -> ExecutionConfig {
        ExecutionConfig {
            workers: self.workers.counts(),
            budgets: MemoryBudgets {
                management: *self.management_memory_bytes.value(),
                commands: *self.commands_memory_bytes.value(),
                relay: *self.relay_memory_bytes.value(),
                state_read: *self.state_read_memory_bytes.value(),
                state_read_cache: *self.state_read_cache_memory_bytes.value(),
                bulk: *self.bulk_memory_bytes.value(),
                bulk_window: *self.bulk_window_memory_bytes.value(),
                restore_metadata: *self.restore_metadata_memory_bytes.value(),
                credentials: *self.credentials_memory_bytes.value(),
                intake: *self.intake_memory_bytes.value(),
            },
            limits: OperationLimits {
                relay_encoded_bytes: *self.relay_encoded_bytes.value(),
                relay_decoded_bytes: *self.relay_decoded_bytes.value(),
                relay_scratch_bytes: *self.relay_scratch_bytes.value(),
                resident_replication_batches: *self.resident_replication_batches.value(),
                bulk_chunk_bytes: *self.bulk_chunk_bytes.value(),
                ..OperationLimits::default()
            },
        }
    }
}

/// The same declared defaults when a standalone caller has no machine measurements.
pub(crate) fn default_config() -> ExecutionConfig {
    ExecutionSettings::resolve(ExecutionOverrides::default(), None, None)
        .assured("the declarations' defaults satisfy every executor relationship")
        .config()
}

impl ExecutionOverrides {
    /// Reject invalid explicit scalar requests before startup performs any I/O.
    pub fn validate(&self) -> Result<(), Report<ExecutionConfigError>> {
        for (name, value) in [
            (ControlCpuWorkers::NAME, self.control_cpu_workers),
            (ControlCpuPendingJobs::NAME, self.control_cpu_pending_jobs),
            (CredentialsCpuWorkers::NAME, self.credentials_cpu_workers),
            (
                CredentialsCpuPendingJobs::NAME,
                self.credentials_cpu_pending_jobs,
            ),
            (DataCpuWorkers::NAME, self.data_cpu_workers),
            (DataCpuPendingJobs::NAME, self.data_cpu_pending_jobs),
            (ExtensionCpuWorkers::NAME, self.extension_cpu_workers),
            (
                ExtensionCpuPendingJobs::NAME,
                self.extension_cpu_pending_jobs,
            ),
            (BulkCpuWorkers::NAME, self.bulk_cpu_workers),
            (BulkCpuPendingJobs::NAME, self.bulk_cpu_pending_jobs),
            (
                ConsensusStorageWorkers::NAME,
                self.consensus_storage_workers,
            ),
            (
                ConsensusStoragePendingJobs::NAME,
                self.consensus_storage_pending_jobs,
            ),
            (
                FilesystemStorageWorkers::NAME,
                self.filesystem_storage_workers,
            ),
            (
                FilesystemStoragePendingJobs::NAME,
                self.filesystem_storage_pending_jobs,
            ),
            (
                crate::ConnectorStorageWorkers::NAME,
                self.connector_storage_workers,
            ),
            (
                ConnectorStoragePendingJobs::NAME,
                self.connector_storage_pending_jobs,
            ),
        ] {
            if let Some(value) = value {
                validate_count(name, value)?;
            }
        }
        for (name, value) in [
            (ManagementMemory::NAME, self.management_memory_bytes),
            (CommandsMemory::NAME, self.commands_memory_bytes),
            (RelayMemory::NAME, self.relay_memory_bytes),
            (StateReadMemory::NAME, self.state_read_memory_bytes),
            (
                StateReadCacheMemory::NAME,
                self.state_read_cache_memory_bytes,
            ),
            (BulkMemory::NAME, self.bulk_memory_bytes),
            (BulkWindowMemory::NAME, self.bulk_window_memory_bytes),
            (
                RestoreMetadataMemory::NAME,
                self.restore_metadata_memory_bytes,
            ),
            (CredentialsMemory::NAME, self.credentials_memory_bytes),
            (IntakeMemory::NAME, self.intake_memory_bytes),
            (RelayEncodedBytes::NAME, self.relay_encoded_bytes),
            (RelayDecodedBytes::NAME, self.relay_decoded_bytes),
            (RelayScratchBytes::NAME, self.relay_scratch_bytes),
            (BulkChunkBytes::NAME, self.bulk_chunk_bytes),
        ] {
            if let Some(value) = value {
                validate_bytes(name, value)?;
            }
        }
        Ok(())
    }
}
