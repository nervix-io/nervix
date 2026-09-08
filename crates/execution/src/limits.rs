//! The classes a node admits work into and the limits that bound them, validated together.

use std::num::{NonZeroU32, NonZeroUsize};

use error_stack::Report;
use thiserror::Error;
use ubyte::ByteUnit;

/// A bounded pool of workers that runs CPU work off the async workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CpuClass {
    /// Control-plane and consensus work, whose capacity is reserved so that saturated data or
    /// bulk work cannot delay a heartbeat, a vote, an acknowledgement or an administrative reply.
    Control,
    /// Per-message data-plane work: relay body encoding and decoding, validation and hashing.
    Data,
    /// Whole-transfer work: resource archives, snapshots and other large read results.
    Bulk,
}

/// A bounded pool of workers that runs synchronous filesystem and database work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageClass {
    /// The single ordered worker consensus storage executes on, so appends, deletions and applied
    /// positions reach the durable barrier in the order they were admitted.
    Consensus,
    /// Every other synchronous filesystem and database operation.
    Filesystem,
}

/// A reserved transient-memory budget. A class cannot borrow beyond its own ceiling, so a
/// saturated relay or bulk transfer leaves management and command capacity untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemoryClass {
    Management,
    Commands,
    Relay,
    Bulk,
}

impl MemoryClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Management => "management",
            Self::Commands => "commands",
            Self::Relay => "relay",
            Self::Bulk => "bulk",
        }
    }
}

/// The name one worker pool reports itself under. Both execution kinds resolve to it so the pools
/// share one implementation without sharing their admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerClassName(&'static str);

impl WorkerClassName {
    pub fn as_str(self) -> &'static str {
        self.0
    }
}

impl From<CpuClass> for WorkerClassName {
    fn from(class: CpuClass) -> Self {
        Self(match class {
            CpuClass::Control => "control_cpu",
            CpuClass::Data => "data_cpu",
            CpuClass::Bulk => "bulk_cpu",
        })
    }
}

impl From<StorageClass> for WorkerClassName {
    fn from(class: StorageClass) -> Self {
        Self(match class {
            StorageClass::Consensus => "consensus_storage",
            StorageClass::Filesystem => "filesystem_storage",
        })
    }
}

/// How many workers each class runs, and how many jobs may wait for one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerCounts {
    pub control_cpu: NonZeroUsize,
    pub data_cpu: NonZeroUsize,
    pub bulk_cpu: NonZeroUsize,
    pub consensus_storage: NonZeroUsize,
    pub filesystem_storage: NonZeroUsize,
    /// The number of jobs one class may hold waiting for a worker. Beyond it, admission is refused
    /// with typed backpressure rather than growing an unbounded queue in front of the pool.
    pub pending_jobs: NonZeroUsize,
}

impl Default for WorkerCounts {
    /// One reserved control and consensus worker each, data and bulk concurrency at the greater of
    /// one and the available CPU count minus one, and two workers for ordinary filesystem work.
    fn default() -> Self {
        let available = match std::thread::available_parallelism() {
            Ok(available) => available.get(),
            Err(_) => 1,
        };
        // One CPU is reserved for control and consensus work; a single-CPU node still runs one
        // data and one bulk worker, because refusing to run either is not a useful bound.
        let data = match available.checked_sub(1) {
            Some(remaining) => match NonZeroUsize::new(remaining) {
                Some(data) => data,
                None => NonZeroUsize::MIN,
            },
            None => NonZeroUsize::MIN,
        };
        Self {
            control_cpu: NonZeroUsize::MIN,
            data_cpu: data,
            bulk_cpu: data,
            consensus_storage: NonZeroUsize::MIN,
            filesystem_storage: NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN),
            pending_jobs: NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN),
        }
    }
}

/// The transient interconnection memory a node divides among its classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryBudgets {
    pub management: ByteUnit,
    pub commands: ByteUnit,
    pub relay: ByteUnit,
    pub bulk: ByteUnit,
}

impl Default for MemoryBudgets {
    /// 256 MiB in total: 8 MiB reserved for management, 24 MiB for commands and replication,
    /// 192 MiB for relay work and 32 MiB for bulk buffers.
    fn default() -> Self {
        Self {
            management: ByteUnit::Mebibyte(8),
            commands: ByteUnit::Mebibyte(24),
            relay: ByteUnit::Mebibyte(192),
            bulk: ByteUnit::Mebibyte(32),
        }
    }
}

/// The largest single operation of each kind a node accepts. Every decoder bounds an untrusted
/// count against these before it allocates, and every incremental writer fails at them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OperationLimits {
    /// The complete encoded body of one relay batch, including its metadata.
    pub relay_encoded_bytes: ByteUnit,
    /// The decoded data one relay batch retains.
    pub relay_decoded_bytes: ByteUnit,
    /// The serialization and validation scratch one relay batch may occupy while it is converted.
    pub relay_scratch_bytes: ByteUnit,
    /// One semantic command.
    pub command_bytes: ByteUnit,
    /// One management or discovery event.
    pub management_event_bytes: ByteUnit,
    /// The application body a bulk transfer submits at a time.
    pub bulk_chunk_bytes: ByteUnit,
    /// How deeply a decoder may nest before it refuses the input.
    pub decoder_depth: NonZeroU32,
}

impl Default for OperationLimits {
    fn default() -> Self {
        Self {
            relay_encoded_bytes: ByteUnit::Mebibyte(32),
            relay_decoded_bytes: ByteUnit::Mebibyte(32),
            relay_scratch_bytes: ByteUnit::Mebibyte(16),
            command_bytes: ByteUnit::Mebibyte(1),
            management_event_bytes: ByteUnit::Kibibyte(64),
            bulk_chunk_bytes: ByteUnit::Kibibyte(64),
            decoder_depth: NonZeroU32::new(64).unwrap_or(NonZeroU32::MIN),
        }
    }
}

impl OperationLimits {
    /// Everything one relay operation may hold at once: its encoded body, the data it decodes into
    /// and the scratch the conversion overlaps them with. Limits whose sum does not fit an address
    /// are a contradiction the node reports instead of a number it silently clamps.
    pub fn relay_operation_bytes(&self) -> Option<u64> {
        self.relay_encoded_bytes
            .as_u64()
            .checked_add(self.relay_decoded_bytes.as_u64())?
            .checked_add(self.relay_scratch_bytes.as_u64())
    }
}

/// The settings a node's executor is built from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExecutionConfig {
    pub workers: WorkerCounts,
    pub budgets: MemoryBudgets,
    pub limits: OperationLimits,
}

/// The same settings after they have been checked against each other, with every byte budget
/// narrowed to the width a permit count is expressed in.
pub(crate) struct ValidatedConfig {
    pub(crate) workers: WorkerCounts,
    pub(crate) budgets: ValidatedBudgets,
    pub(crate) limits: OperationLimits,
}

pub(crate) struct ValidatedBudgets {
    pub(crate) management: u32,
    pub(crate) commands: u32,
    pub(crate) relay: u32,
    pub(crate) bulk: u32,
}

/// A node whose limits contradict each other never starts, so no operation discovers the
/// contradiction by waiting forever for a budget that cannot hold it.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExecutionConfigError {
    #[error(
        "the {class} memory budget of {budget} bytes cannot hold its largest {operation} of \
         {required} bytes"
    )]
    BudgetBelowOperation {
        class: &'static str,
        operation: &'static str,
        budget: u64,
        required: u64,
    },
    #[error("the {class} memory budget of {budget} bytes cannot be counted in permits")]
    BudgetTooLarge { class: &'static str, budget: u64 },
    #[error("the {class} memory budget must be greater than zero")]
    EmptyBudget { class: &'static str },
    #[error("the configured relay operation limits do not add up to an addressable size")]
    UnaddressableRelayOperation,
}

impl ExecutionConfig {
    pub(crate) fn validate(self) -> Result<ValidatedConfig, Report<ExecutionConfigError>> {
        // A blocked channel must not exhaust the capacity another maximum-size channel needs, so
        // the relay budget holds two independent maximum-size operations.
        let relay_required = match self.limits.relay_operation_bytes() {
            Some(operation) => match operation.checked_mul(2) {
                Some(pair) => pair,
                None => {
                    return Err(Report::new(
                        ExecutionConfigError::UnaddressableRelayOperation,
                    ));
                }
            },
            None => {
                return Err(Report::new(
                    ExecutionConfigError::UnaddressableRelayOperation,
                ));
            }
        };
        Ok(ValidatedConfig {
            workers: self.workers,
            budgets: ValidatedBudgets {
                management: permits(
                    MemoryClass::Management.as_str(),
                    self.budgets.management,
                    "management event",
                    self.limits.management_event_bytes.as_u64(),
                )?,
                commands: permits(
                    MemoryClass::Commands.as_str(),
                    self.budgets.commands,
                    "semantic command",
                    self.limits.command_bytes.as_u64(),
                )?,
                relay: permits(
                    MemoryClass::Relay.as_str(),
                    self.budgets.relay,
                    "pair of relay operations",
                    relay_required,
                )?,
                bulk: permits(
                    MemoryClass::Bulk.as_str(),
                    self.budgets.bulk,
                    "bulk body chunk",
                    self.limits.bulk_chunk_bytes.as_u64(),
                )?,
            },
            limits: self.limits,
        })
    }
}

fn permits(
    class: &'static str,
    budget: ByteUnit,
    operation: &'static str,
    required: u64,
) -> Result<u32, Report<ExecutionConfigError>> {
    let budget = budget.as_u64();
    if budget == 0 {
        return Err(Report::new(ExecutionConfigError::EmptyBudget { class }));
    }
    if budget < required {
        return Err(Report::new(ExecutionConfigError::BudgetBelowOperation {
            class,
            operation,
            budget,
            required,
        }));
    }
    u32::try_from(budget)
        .map_err(|_| Report::new(ExecutionConfigError::BudgetTooLarge { class, budget }))
}
