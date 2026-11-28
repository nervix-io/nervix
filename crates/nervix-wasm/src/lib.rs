//! The wasmtime host for Nervix WASM processors.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Engine configuration, instance lifetime, linear-memory and table limits, the guest
//!   contract's one memory, one table and the WebAssembly features a guest may use, epoch
//!   deadlines, the host half of the C ABI, the guest snapshot and state description calls, and
//!   the classification of every failed guest operation, including a guest's verdict on the saved
//!   state it was asked to restore.
//! - **Depends on.** The vocabulary for processor limits and timestamps, `nervix-wasm-protocol`
//!   for the envelopes it exchanges with a guest, and the node's bounded executor, whose bulk
//!   workers compile a module.
//! - **Must not know.** Plexus, the registry, the execution graph, or where a guest's output is
//!   routed. It calls a guest and returns what the guest produced.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

mod guest_proposals;
mod settings;
use std::{
    convert::Infallible,
    num::{NonZeroU32, NonZeroU64},
    ops::{Deref, DerefMut},
    time::Duration,
};

use arch_into::ArchInto as _;
use bytes::Bytes;
use error_stack::{Report, Result as StackResult, ResultExt as _};
use flatbuffers::{Allocator, FlatBufferBuilder};
use guest_proposals::{GUEST_WASM_FEATURES, RELAXED_SIMD_DETERMINISTIC};
pub use guest_proposals::{RefusedWasmProposal, RefusedWasmProposals};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Cancellation, CpuClass, Executor, MemoryClass};
use nervix_models::{ParseAsType, Timestamp, WasmProcessorLimits};
#[cfg(test)]
use nervix_primitives::sync::Arc;
#[cfg(test)]
use nervix_primitives::sync::atomic::Ordering;
use nervix_primitives::sync::{StdArc, mpsc};
// The external engine's epoch driver runs outside every model. Its wake handle is initialized
// once by its constructor, read only when its owner drops, and never carries a modeled protocol.
use nervix_primitives::unmodeled::{sync::OnceLock, thread::Thread, time::Instant as EpochInstant};
use nervix_recovery::NoReceiver as _;
use nervix_wasm_protocol as protocol;
pub use nervix_wasm_protocol::SavedStateRejection;
pub use settings::{
    WASM_HOST_STACK_BYTES, WasmEngineSettingOverrides, WasmEngineSettings, WasmEngineSettingsError,
    WasmMemoryGrowthReservationBytes, WasmMemoryGuardBytes, WasmMemoryReservationBytes,
    WasmStackBytes,
};
use thiserror::Error;
use wasmtime::{
    Caller, Config, Engine, Instance, InstancePre, Linker, Memory, Module, OptLevel,
    ResourceLimiter, Store, Trap, TypedFunc, WasmFeatures,
};

const ENV_MODULE: &str = "env";
const EXPORT_MEMORY: &str = "memory";
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WASM guest ABI's fallible exports return 0 on success"
    )
)]
const SUCCESS: i32 = 0;
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WASM Processor Guests chapter has nervix_timeout_after_nanos answer a \
                  negative delay with -1, which no granted handle, a count from one, can be"
    )
)]
const ERR_INVALID_SIZE: i32 = -1;

pub const ABI_SERIALIZATION_NAME: &str = protocol::SERIALIZATION_NAME;

#[cfg(test)]
mod properties;

nervix_primitives::unmodeled::task_local! {
    static INVOCATION_NOW: Timestamp;
}

/// The linear memories a guest module may define: the one it exports as `memory`, which holds
/// every buffer the host and the guest exchange.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WASM Processor Guests contract gives a guest one linear memory, the \
                  exported memory every ABI buffer lives in"
    )
)]
pub const GUEST_MEMORIES: u32 = 1;

/// The tables a guest module may define: the one function table its toolchain links.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the WASM Processor Guests contract gives a guest one table, the function table \
                  its toolchain links"
    )
)]
pub const GUEST_TABLES: u32 = 1;

/// The instances one branch store holds: the guest module, instantiated once for the branch.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the host instantiates exactly one guest module in each branch store"
    )
)]
const BRANCH_STORE_INSTANCES: usize = 1;

/// Why the Wasmtime engine could not start, or why a module could not be compiled and linked.
///
/// Like every error this crate reports, a variant names only its own failure and leaves the cause
/// to its source, which a report renders as a frame of its own.
#[derive(Debug, Error)]
pub enum WasmProcessorError {
    #[error("failed to configure wasmtime")]
    Configure(#[source] wasmtime::Error),
    #[error("failed to spawn the wasm epoch driver thread")]
    SpawnEpochDriver(#[source] std::io::Error),
    #[error("failed to compile wasm module")]
    Compile(#[source] wasmtime::Error),
    #[error("the node's bounded execution did not compile the wasm module")]
    CompileExecution,
    #[error("failed to link wasm module")]
    Link(#[source] wasmtime::Error),
    #[error("wasm module defines {defined} linear memories, but a guest defines at most {limit}")]
    TooManyMemories { defined: u32, limit: u32 },
    #[error("wasm module defines {defined} tables, but a guest defines at most {limit}")]
    TooManyTables { defined: u32, limit: u32 },
    #[error("wasm module uses {proposals}, which a guest may not use")]
    RefusedProposals { proposals: RefusedWasmProposals },
}

impl WasmProcessorError {
    /// Classifies `wasm`, which Wasmtime refused to compile with `source`: a module refused for the
    /// WebAssembly proposals outside the guest contract that it needs, above the compile failure
    /// they caused, or the compile failure alone when no such proposal is why.
    fn compile_failure(
        wasm: &[u8],
        source: wasmtime::Error,
        cancellation: &Cancellation,
    ) -> Report<Self> {
        let failure = Report::new(Self::Compile(source));
        match RefusedWasmProposals::needed_by(wasm, cancellation) {
            Ok(Some(proposals)) => failure.change_context(Self::RefusedProposals { proposals }),
            Ok(None) => failure,
            Err(cancelled) => Report::new(cancelled).change_context(Self::CompileExecution),
        }
    }
}

/// One operation the host runs on a guest branch instance, in the order an instance meets them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
pub enum WasmGuestOperation {
    /// Creating the branch store and its linear memory, instantiating the module, and resolving
    /// its exports.
    #[strum(serialize = "instantiation")]
    Instantiation,
    /// `nervix_init`, which hands a new instance its branch configuration.
    #[strum(serialize = "initialization")]
    Initialization,
    /// `nervix_load_state`, which hands an initialized instance the state saved last.
    #[strum(serialize = "state restore")]
    StateRestore,
    /// `nervix_process_batch` and the emit reads that follow it.
    #[strum(serialize = "batch processing")]
    BatchProcessing,
    /// `nervix_on_timeout` and the emit reads that follow it.
    #[strum(serialize = "timeout callback")]
    TimeoutCallback,
    /// `nervix_flush` and the emit reads that follow it.
    #[strum(serialize = "quiesce flush")]
    QuiesceFlush,
    /// `nervix_dump_state`.
    #[strum(serialize = "state snapshot")]
    StateSnapshot,
    /// `nervix_reset_state`.
    #[strum(serialize = "state reset")]
    StateReset,
    /// `nervix_describe_state`, which a state inspection calls on a scratch instance it restored
    /// from the branch's committed checkpoint, and decoding what it wrote.
    #[strum(serialize = "state description")]
    StateDescription,
    /// `nervix_current_domain_time_nanos`.
    #[strum(serialize = "domain-time read")]
    DomainTimeRead,
}

impl WasmGuestOperation {
    /// The export whose call runs this operation. Instantiation calls none.
    pub const fn entry_export(self) -> Option<&'static str> {
        match self {
            Self::Instantiation => None,
            Self::Initialization => Some("nervix_init"),
            Self::StateRestore => Some("nervix_load_state"),
            Self::BatchProcessing => Some("nervix_process_batch"),
            Self::TimeoutCallback => Some("nervix_on_timeout"),
            Self::QuiesceFlush => Some("nervix_flush"),
            Self::StateSnapshot => Some("nervix_dump_state"),
            Self::StateReset => Some("nervix_reset_state"),
            Self::StateDescription => Some("nervix_describe_state"),
            Self::DomainTimeRead => Some("nervix_current_domain_time_nanos"),
        }
    }
}

/// Why a call into the guest, or a host step of a guest operation, failed.
#[derive(Debug, Error)]
pub enum WasmGuestCallError {
    #[error("failed to instantiate wasm module")]
    Instantiate(#[source] wasmtime::Error),
    #[error("missing required export '{0}'")]
    MissingExport(&'static str),
    #[error("failed to reset MAX FUEL {limit}")]
    ResetFuel {
        limit: NonZeroU64,
        #[source]
        source: wasmtime::Error,
    },
    #[error("wasm guest exhausted MAX FUEL {limit}")]
    FuelExhausted {
        limit: NonZeroU64,
        export: Option<&'static str>,
    },
    #[error(
        "wasm guest exceeded MAX MEMORY {limit} bytes (growing linear memory by {growth} bytes \
         past the {allocated} bytes already allocated)"
    )]
    MemoryLimitExceeded {
        limit: u64,
        allocated: u64,
        growth: u64,
        export: Option<&'static str>,
    },
    #[error(
        "wasm guest exceeded MAX TABLE ELEMENTS {limit} (growing its table by {growth} elements \
         past the {allocated} elements already allocated)"
    )]
    TableLimitExceeded {
        limit: u64,
        allocated: u64,
        growth: u64,
        export: Option<&'static str>,
    },
    #[error("wasm export '{export}' trapped: {trap}")]
    Trap { export: &'static str, trap: Trap },
    #[error("failed to call wasm export '{export}'")]
    Call {
        export: &'static str,
        #[source]
        source: wasmtime::Error,
    },
    #[error("wasm export '{export}' returned error code {code}")]
    ErrorCode { export: &'static str, code: i32 },
    #[error("wasm guest reported global error: {reason}")]
    GlobalError { reason: String },
    #[error("wasm guest emitted an output envelope the host cannot decode")]
    InvalidEmission,
    #[error("wasm guest wrote a state description the host cannot decode")]
    InvalidStateDescription,
    #[error("failed to write guest memory")]
    MemoryWrite(#[source] wasmtime::MemoryAccessError),
    #[error("failed to read guest memory")]
    MemoryRead(#[source] wasmtime::MemoryAccessError),
    #[error("guest returned invalid memory offset {0}")]
    InvalidOffset(i32),
    #[error("guest returned invalid byte size {0}")]
    InvalidSize(i32),
    #[error(
        "guest returned buffer range offset {offset} size {size} outside linear memory size \
         {memory_size}"
    )]
    InvalidBufferRange {
        offset: usize,
        size: usize,
        memory_size: usize,
    },
    #[error("guest global error bytes are not valid UTF-8")]
    InvalidGlobalErrorText(#[source] std::string::FromUtf8Error),
    #[error("guest buffer size {size} exceeds configured limit {limit}")]
    GuestBufferTooLarge { size: usize, limit: usize },
}

impl WasmGuestCallError {
    /// The limit failure a guest call ended in, when `source` is one.
    fn limit_exceeded(
        limits: WasmProcessorLimits,
        export: Option<&'static str>,
        source: &wasmtime::Error,
    ) -> Option<Self> {
        if source.downcast_ref::<Trap>() == Some(&Trap::OutOfFuel) {
            return Some(Self::FuelExhausted {
                limit: limits.max_fuel,
                export,
            });
        }
        if let Some(exceeded) = source.downcast_ref::<GuestTableLimitExceeded>() {
            return Some(Self::TableLimitExceeded {
                limit: exceeded.limit,
                allocated: exceeded.allocated,
                growth: exceeded.growth,
                export,
            });
        }
        let exceeded = source.downcast_ref::<GuestMemoryLimitExceeded>()?;
        Some(Self::MemoryLimitExceeded {
            limit: exceeded.limit.arch_into(),
            allocated: exceeded.allocated.arch_into(),
            growth: exceeded.growth.arch_into(),
            export,
        })
    }

    /// Classifies a failed call of `export`: an exhausted limit, a trap the guest raised, or a call
    /// the host could not complete.
    fn call(
        limits: WasmProcessorLimits,
        export: &'static str,
        source: wasmtime::Error,
    ) -> Report<Self> {
        if let Some(cause) = Self::limit_exceeded(limits, Some(export), &source) {
            return Report::new(cause).attach(source);
        }
        if let Some(trap) = source.downcast_ref::<Trap>() {
            return Report::new(Self::Trap {
                export,
                trap: *trap,
            })
            .attach(source);
        }
        Report::new(Self::Call { export, source })
    }

    fn instantiation(limits: WasmProcessorLimits, source: wasmtime::Error) -> Report<Self> {
        match Self::limit_exceeded(limits, None, &source) {
            Some(cause) => Report::new(cause).attach(source),
            None => Report::new(Self::Instantiate(source)),
        }
    }

    const fn export(&self) -> Option<&'static str> {
        match self {
            Self::MissingExport(export)
            | Self::Trap { export, .. }
            | Self::Call { export, .. }
            | Self::ErrorCode { export, .. } => Some(*export),
            Self::FuelExhausted { export, .. }
            | Self::MemoryLimitExceeded { export, .. }
            | Self::TableLimitExceeded { export, .. } => *export,
            Self::InvalidEmission => Some("nervix_read_emit"),
            Self::Instantiate(_)
            | Self::ResetFuel { .. }
            | Self::GlobalError { .. }
            | Self::InvalidStateDescription
            | Self::MemoryWrite(_)
            | Self::MemoryRead(_)
            | Self::InvalidOffset(_)
            | Self::InvalidSize(_)
            | Self::InvalidBufferRange { .. }
            | Self::InvalidGlobalErrorText(_)
            | Self::GuestBufferTooLarge { .. } => None,
        }
    }
}

/// A guest operation on one branch instance failed.
///
/// A guest's verdict on the saved state it was asked to restore is its own variant, because it is
/// the only failure that says the saved bytes are unusable. Everything else, including a trap or an
/// exhausted limit while restoring, is a failure of the operation and leaves those bytes as usable
/// as they were.
#[derive(Debug, Error)]
pub enum WasmGuestError {
    #[error("wasm guest {operation} failed")]
    Failed { operation: WasmGuestOperation },
    #[error(
        "wasm guest rejected the snapshot envelope of its saved state{}",
        GuestReason(.reason.as_deref())
    )]
    SnapshotEnvelopeRejected { reason: Option<String> },
    #[error(
        "wasm guest rejected the application state in its saved snapshot{}",
        GuestReason(.reason.as_deref())
    )]
    ApplicationStateRejected { reason: Option<String> },
}

impl WasmGuestError {
    fn failed(operation: WasmGuestOperation, cause: Report<WasmGuestCallError>) -> Report<Self> {
        cause.change_context(Self::Failed { operation })
    }

    fn saved_state_rejected(
        rejection: SavedStateRejection,
        reason: Option<String>,
    ) -> Report<Self> {
        match rejection {
            SavedStateRejection::SnapshotEnvelope => {
                Report::new(Self::SnapshotEnvelopeRejected { reason })
            }
            SavedStateRejection::ApplicationState => {
                Report::new(Self::ApplicationStateRejected { reason })
            }
        }
    }

    /// The operation that failed.
    pub const fn operation(&self) -> WasmGuestOperation {
        match self {
            Self::Failed { operation, .. } => *operation,
            Self::SnapshotEnvelopeRejected { .. } | Self::ApplicationStateRejected { .. } => {
                WasmGuestOperation::StateRestore
            }
        }
    }

    /// The guest's verdict on the saved state it was asked to restore, when it refused that state.
    pub const fn saved_state_rejection(&self) -> Option<SavedStateRejection> {
        match self {
            Self::SnapshotEnvelopeRejected { .. } => Some(SavedStateRejection::SnapshotEnvelope),
            Self::ApplicationStateRejected { .. } => Some(SavedStateRejection::ApplicationState),
            Self::Failed { .. } => None,
        }
    }
}

/// Classification available to callers without discarding the typed cause frames of a failed
/// guest operation.
pub trait WasmGuestReportExt {
    fn is_resource_limit_exceeded(&self) -> bool;
    fn is_invalid_emission(&self) -> bool;
    fn export(&self) -> Option<&'static str>;
}

impl WasmGuestReportExt for Report<WasmGuestError> {
    fn is_resource_limit_exceeded(&self) -> bool {
        matches!(
            self.downcast_ref::<WasmGuestCallError>(),
            Some(
                WasmGuestCallError::FuelExhausted { .. }
                    | WasmGuestCallError::MemoryLimitExceeded { .. }
                    | WasmGuestCallError::TableLimitExceeded { .. }
            )
        )
    }

    fn is_invalid_emission(&self) -> bool {
        matches!(
            self.downcast_ref::<WasmGuestCallError>(),
            Some(WasmGuestCallError::InvalidEmission)
        )
    }

    fn export(&self) -> Option<&'static str> {
        if let Some(cause) = self.downcast_ref::<WasmGuestCallError>()
            && let Some(export) = cause.export()
        {
            return Some(export);
        }
        self.current_context().operation().entry_export()
    }
}

/// A reason a guest put on its global-error channel, rendered after the failure it explains.
struct GuestReason<'a>(Option<&'a str>);

impl std::fmt::Display for GuestReason<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(reason) => write!(formatter, ": {reason}"),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Error)]
#[error("growing guest linear memory by {growth} bytes past {allocated} exceeds {limit} bytes")]
struct GuestMemoryLimitExceeded {
    allocated: usize,
    growth: usize,
    limit: usize,
}

#[derive(Debug, Error)]
#[error("growing the guest's table by {growth} elements past {allocated} exceeds {limit} elements")]
struct GuestTableLimitExceeded {
    allocated: u64,
    growth: u64,
    limit: u64,
}

/// What one branch store's guest may allocate: `MAX MEMORY` bytes of linear memory and
/// `MAX TABLE ELEMENTS` table slots, from the initial sizes Wasmtime asks for at instantiation
/// through every later growth, in exactly one instance with at most one memory and one table.
#[derive(Debug)]
struct GuestResourceLimiter {
    max_memory_bytes: usize,
    max_table_elements: u64,
}

// `memory_growing` reads the size of the memory being grown as every byte of linear memory the
// guest holds, and `table_growing` the size of the table being grown as every slot it holds.
const _: () = assert!(GUEST_MEMORIES == 1);
const _: () = assert!(GUEST_TABLES == 1);

impl GuestResourceLimiter {
    fn new(limits: WasmProcessorLimits) -> Self {
        Self {
            max_memory_bytes: limits.max_memory_bytes.get().arch_into(),
            max_table_elements: limits.max_table_elements,
        }
    }
}

impl ResourceLimiter for GuestResourceLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        // A guest holds at most one linear memory, so the size Wasmtime reports for the memory
        // being grown is every byte of linear memory the guest holds. Admission reads that size
        // instead of keeping a running total: Wasmtime fails some growths without asking first,
        // such as one a memory of one-byte pages cannot represent, and a total rolled back on
        // failure could then subtract a growth that stood. A permitted growth that Wasmtime fails
        // afterwards leaves the size unchanged, so nothing needs undoing.
        let growth = desired
            .checked_sub(current)
            .assured("wasmtime only asks to grow a memory to a size at or above its current one");
        if desired > self.max_memory_bytes {
            return Err(wasmtime::Error::new(GuestMemoryLimitExceeded {
                allocated: current,
                growth,
                limit: self.max_memory_bytes,
            }));
        }
        Ok(true)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        // A guest holds at most one table, so the size Wasmtime reports for the table being grown
        // is every slot the guest holds. Admission reads that size instead of keeping a running
        // total: Wasmtime fails some growths without asking first, such as one whose new size
        // overflows, and a total rolled back on failure could then subtract a growth that stood.
        // A permitted growth that Wasmtime fails afterwards leaves the size unchanged, so nothing
        // needs undoing.
        let growth = desired
            .checked_sub(current)
            .assured("wasmtime only asks to grow a table to a size at or above its current one");
        let desired_elements: u64 = desired.arch_into();
        if desired_elements > self.max_table_elements {
            return Err(wasmtime::Error::new(GuestTableLimitExceeded {
                allocated: current.arch_into(),
                growth: growth.arch_into(),
                limit: self.max_table_elements,
            }));
        }
        Ok(true)
    }

    fn instances(&self) -> usize {
        BRANCH_STORE_INSTANCES
    }

    fn tables(&self) -> usize {
        GUEST_TABLES.arch_into()
    }

    fn memories(&self) -> usize {
        GUEST_MEMORIES.arch_into()
    }
}

enum GuestBufferAllocator<'a> {
    Direct(&'a mut [u8]),
    Spill(Vec<u8>),
}

enum GuestBufferBuild {
    Direct { offset: usize, size: usize },
    Spill { buffer: Vec<u8>, start: usize },
}

impl Deref for GuestBufferAllocator<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Direct(buffer) => buffer,
            Self::Spill(buffer) => buffer,
        }
    }
}

impl DerefMut for GuestBufferAllocator<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Direct(buffer) => buffer,
            Self::Spill(buffer) => buffer,
        }
    }
}

// SAFETY: growth preserves the previous allocation at the end of the new allocation, as required
// by FlatBuffers' backwards builder.
unsafe impl Allocator for GuestBufferAllocator<'_> {
    type Error = Infallible;

    fn grow_downwards(&mut self) -> Result<(), Self::Error> {
        let previous = std::mem::replace(self, Self::Spill(Vec::new()));
        let previous_len = previous.len();
        let new_len = previous_len
            .checked_mul(2)
            .unwrap_or(flatbuffers::FLATBUFFERS_MAX_BUFFER_SIZE)
            .max(1);
        let mut grown = vec![0; new_len];
        grown[new_len - previous_len..].copy_from_slice(&previous);
        *self = Self::Spill(grown);
        Ok(())
    }

    fn len(&self) -> usize {
        Deref::deref(self).len()
    }
}

#[derive(Debug, Clone)]
pub struct WasmRuntimeConfig {
    pub engine: WasmEngineSettings,
    pub optimize: bool,
    pub epoch_tick_interval: Duration,
    pub epoch_deadline_ticks: u64,
    pub max_guest_buffer_bytes: usize,
}

impl Default for WasmRuntimeConfig {
    fn default() -> Self {
        Self {
            engine: WasmEngineSettings::default(),
            optimize: nervix_models::DataPlaneSettings::default().wasm_optimize(),
            epoch_tick_interval: nervix_models::DataPlaneSettings::default()
                .wasm_epoch_tick_interval(),
            epoch_deadline_ticks: nervix_models::DataPlaneSettings::default()
                .wasm_epoch_deadline_ticks(),
            max_guest_buffer_bytes: nervix_models::DataPlaneSettings::default()
                .wasm_guest_buffer_bytes()
                .arch_into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct WasmRuntime {
    engine: Engine,
    _epoch_driver: StdArc<EpochDriver>,
    epoch_deadline_ticks: u64,
    max_guest_buffer_bytes: usize,
}

impl WasmRuntime {
    pub fn new(config: WasmRuntimeConfig) -> StackResult<Self, WasmProcessorError> {
        let mut wasmtime_config = Config::new();
        // The guest contract decides every WebAssembly feature, so none is left at the default of
        // this Wasmtime version: everything is disabled, then exactly the contract's features are
        // enabled, and an explicit setting overrides every default Wasmtime would apply.
        wasmtime_config.wasm_features(WasmFeatures::all(), false);
        wasmtime_config.wasm_features(GUEST_WASM_FEATURES, true);
        wasmtime_config.relaxed_simd_deterministic(RELAXED_SIMD_DETERMINISTIC);
        wasmtime_config.consume_fuel(true);
        wasmtime_config.epoch_interruption(true);
        // Modules already compile independently on the node's bulk workers. Letting each
        // Cranelift invocation fan out across the global CPU pool can starve Raft and Tokio
        // executor threads when several nodes or branch instances initialize together.
        wasmtime_config.parallel_compilation(config.engine.parallel_compilation());
        // Every guest call runs on a stack of its own, which must hold the guest's stack and the
        // host functions the guest calls beside it.
        wasmtime_config.max_wasm_stack(config.engine.stack_bytes());
        wasmtime_config.async_stack_size(config.engine.call_stack_bytes());
        wasmtime_config.memory_reservation(config.engine.memory_reservation_bytes());
        wasmtime_config.memory_guard_size(config.engine.memory_guard_bytes());
        wasmtime_config.guard_before_linear_memory(config.engine.guard_before_memory());
        wasmtime_config
            .memory_reservation_for_growth(config.engine.memory_growth_reservation_bytes());
        wasmtime_config.cranelift_opt_level(if config.optimize {
            OptLevel::Speed
        } else {
            OptLevel::None
        });
        let engine = Engine::new(&wasmtime_config)
            .map_err(|source| Report::new(WasmProcessorError::Configure(source)))?;
        let epoch_driver = spawn_epoch_driver(engine.clone(), config.epoch_tick_interval)
            .map_err(|source| Report::new(WasmProcessorError::SpawnEpochDriver(source)))?;
        Ok(Self {
            engine,
            _epoch_driver: epoch_driver,
            epoch_deadline_ticks: config.epoch_deadline_ticks,
            max_guest_buffer_bytes: config.max_guest_buffer_bytes,
        })
    }

    /// Compile and link `wasm` on the node's bulk workers: Cranelift compiles the whole module in
    /// one bounded unit that must not hold a data or control worker.
    pub async fn compile_processor(
        &self,
        executor: &Executor,
        wasm: impl AsRef<[u8]>,
    ) -> StackResult<CompiledWasmProcessor, WasmProcessorError> {
        let engine = self.engine.clone();
        let wasm = wasm.as_ref().to_vec();
        // Wasmtime allocates the code it generates itself and keeps it for as long as the
        // processor holds the module, which no transient budget can stand for, so the compilation
        // takes only the admission token onto the bulk workers.
        let reservation = executor
            .reserve_admission(MemoryClass::Bulk)
            .await
            .change_context(WasmProcessorError::CompileExecution)?;
        let compiled = executor
            .run_cpu(CpuClass::Bulk, reservation, move |_charge, cancellation| {
                cancellation
                    .check()
                    .change_context(WasmProcessorError::CompileExecution)?;
                let module = match Module::new(&engine, &wasm) {
                    Ok(module) => module,
                    Err(source) => {
                        return Err(WasmProcessorError::compile_failure(
                            &wasm,
                            source,
                            cancellation,
                        ));
                    }
                };
                refuse_module_beyond_guest_contract(&module)?;
                let mut linker = Linker::<BranchStore>::new(&engine);
                define_host_functions(&mut linker)?;
                linker
                    .instantiate_pre(&module)
                    .map_err(|source| Report::new(WasmProcessorError::Link(source)))
            })
            .await
            .change_context(WasmProcessorError::CompileExecution)?;
        let instance_pre = compiled?;
        Ok(CompiledWasmProcessor {
            engine: self.engine.clone(),
            instance_pre,
            epoch_deadline_ticks: self.epoch_deadline_ticks,
            max_guest_buffer_bytes: self.max_guest_buffer_bytes,
        })
    }
}

/// Refuses a module that defines more linear memories or tables than the guest contract allows,
/// before any branch instantiates it.
fn refuse_module_beyond_guest_contract(module: &Module) -> StackResult<(), WasmProcessorError> {
    let required = module.resources_required();
    if required.num_memories > GUEST_MEMORIES {
        return Err(Report::new(WasmProcessorError::TooManyMemories {
            defined: required.num_memories,
            limit: GUEST_MEMORIES,
        }));
    }
    if required.num_tables > GUEST_TABLES {
        return Err(Report::new(WasmProcessorError::TooManyTables {
            defined: required.num_tables,
            limit: GUEST_TABLES,
        }));
    }
    Ok(())
}

/// One retained wake handle per runtime lifetime. The epoch thread keeps only a weak reference
/// while parked, so the last runtime's drop can always wake it without waiting for the tick.
#[derive(Debug, Default)]
struct EpochDriver {
    thread: OnceLock<Thread>,
}
impl Drop for EpochDriver {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.get() {
            thread.unpark();
        }
    }
}

fn spawn_epoch_driver(engine: Engine, interval: Duration) -> std::io::Result<StdArc<EpochDriver>> {
    let driver = StdArc::new(EpochDriver::default());
    let weak_driver = StdArc::downgrade(&driver);
    // A drop during an epoch increment may end the owner on this thread. Its unpark permit is
    // durable too, so the next park returns immediately and the expired weak reference ends it.
    let handle = nervix_primitives::unmodeled::thread::Builder::new()
        .name("nervix-wasm-epoch".to_string())
        .spawn(move || {
            'epochs: loop {
                let started = EpochInstant::now();
                loop {
                    let remaining = interval
                        .checked_sub(started.elapsed())
                        .unwrap_or(Duration::ZERO);
                    nervix_primitives::unmodeled::thread::park_timeout(remaining);
                    let Some(driver) = weak_driver.upgrade() else {
                        break 'epochs;
                    };
                    // A park may wake spuriously. Only elapsed physical time advances the external
                    // engine, so a wake handle never shortens the configured preemption quantum.
                    let due = started.elapsed() >= interval;
                    if due {
                        engine.increment_epoch();
                    }
                    drop(driver);
                    if due {
                        break;
                    }
                }
            }
        })?;
    driver
        .thread
        .set(handle.thread().clone())
        .assured("the constructor alone installs the epoch driver's wake handle");
    Ok(driver)
}

#[derive(Debug, Clone)]
pub struct WasmBranchInit {
    pub domain_name: String,
    pub domain_type: String,
    pub branch_key: Option<Vec<u8>>,
    pub input_schema: WasmProcessorSchema,
    pub output_schemas: Vec<WasmProcessorSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmProcessorSchema {
    pub name: String,
    pub fields: Vec<WasmProcessorField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmProcessorField {
    pub name: String,
    pub ty: WasmProcessorType,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmProcessorType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    Bool,
    String,
    Bytes,
    Datetime,
    Duration,
    F32,
    F64,
    Array {
        element: Box<WasmProcessorType>,
        len: u32,
    },
    Vec {
        element: Box<WasmProcessorType>,
    },
}

/// What a guest described of its state: its sections in its order, each a schema naming the
/// section and its columns and one Arrow IPC stream of its rows.
///
/// The host has only decoded the guest's message. The caller validates names, types and rows
/// against the rules of the read that asked, because only the caller knows them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmStateDescription {
    pub sections: Vec<WasmStateSection>,
}

/// One section a guest described of its state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmStateSection {
    pub schema: WasmProcessorSchema,
    pub arrow_ipc_batch: Vec<u8>,
}

/// Why a type a guest declared names no Nervix type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "a guest-declared type outside the Nervix types is a typed refusal of the \
                  declaration that carries it; nothing is converted in part"
    )
)]
pub enum WasmProcessorTypeError {
    #[error("an ARRAY type declares zero elements")]
    EmptyArray,
}

impl WasmStateDescription {
    fn from_protocol(description: protocol::StateDescription) -> Self {
        Self {
            sections: description
                .sections
                .into_iter()
                .map(|section| WasmStateSection {
                    schema: WasmProcessorSchema::from_protocol(section.schema),
                    arrow_ipc_batch: section.arrow_ipc_batch,
                })
                .collect(),
        }
    }
}

impl WasmProcessorSchema {
    fn from_protocol(schema: protocol::ProcessorSchema) -> Self {
        Self {
            name: schema.name,
            fields: schema
                .fields
                .into_iter()
                .map(|field| WasmProcessorField {
                    name: field.name,
                    ty: WasmProcessorType::from_protocol(field.ty),
                    optional: field.optional,
                })
                .collect(),
        }
    }
}

impl WasmProcessorType {
    fn from_protocol(ty: protocol::ProcessorType) -> Self {
        match ty {
            protocol::ProcessorType::U8 => Self::U8,
            protocol::ProcessorType::I8 => Self::I8,
            protocol::ProcessorType::U16 => Self::U16,
            protocol::ProcessorType::I16 => Self::I16,
            protocol::ProcessorType::U32 => Self::U32,
            protocol::ProcessorType::I32 => Self::I32,
            protocol::ProcessorType::U64 => Self::U64,
            protocol::ProcessorType::I64 => Self::I64,
            protocol::ProcessorType::Bool => Self::Bool,
            protocol::ProcessorType::String => Self::String,
            protocol::ProcessorType::Bytes => Self::Bytes,
            protocol::ProcessorType::Datetime => Self::Datetime,
            protocol::ProcessorType::Duration => Self::Duration,
            protocol::ProcessorType::F32 => Self::F32,
            protocol::ProcessorType::F64 => Self::F64,
            protocol::ProcessorType::Array { element, len } => Self::Array {
                element: Box::new(Self::from_protocol(*element)),
                len,
            },
            protocol::ProcessorType::Vec { element } => Self::Vec {
                element: Box::new(Self::from_protocol(*element)),
            },
        }
    }
}

impl TryFrom<&WasmProcessorType> for ParseAsType {
    type Error = Report<WasmProcessorTypeError>;

    fn try_from(ty: &WasmProcessorType) -> Result<Self, Self::Error> {
        let parsed = match ty {
            WasmProcessorType::U8 => Self::U8,
            WasmProcessorType::I8 => Self::I8,
            WasmProcessorType::U16 => Self::U16,
            WasmProcessorType::I16 => Self::I16,
            WasmProcessorType::U32 => Self::U32,
            WasmProcessorType::I32 => Self::I32,
            WasmProcessorType::U64 => Self::U64,
            WasmProcessorType::I64 => Self::I64,
            WasmProcessorType::Bool => Self::Bool,
            WasmProcessorType::String => Self::String,
            WasmProcessorType::Bytes => Self::Bytes,
            WasmProcessorType::Datetime => Self::Datetime,
            WasmProcessorType::Duration => Self::Duration,
            WasmProcessorType::F32 => Self::F32,
            WasmProcessorType::F64 => Self::F64,
            WasmProcessorType::Array { element, len } => {
                let Some(len) = NonZeroU32::new(*len) else {
                    return Err(Report::new(WasmProcessorTypeError::EmptyArray));
                };
                Self::Array {
                    element: Box::new(Self::try_from(element.as_ref())?),
                    len,
                }
            }
            WasmProcessorType::Vec { element } => Self::Vec {
                element: Box::new(Self::try_from(element.as_ref())?),
            },
        };
        Ok(parsed)
    }
}

impl From<&nervix_models::CreateSchema> for WasmProcessorSchema {
    fn from(schema: &nervix_models::CreateSchema) -> Self {
        Self {
            name: schema.name.as_str().to_string(),
            fields: schema.fields.iter().map(WasmProcessorField::from).collect(),
        }
    }
}

impl From<&nervix_models::SchemaField> for WasmProcessorField {
    fn from(field: &nervix_models::SchemaField) -> Self {
        Self {
            name: field.name.as_str().to_string(),
            ty: WasmProcessorType::from(&field.ty),
            optional: field.optional,
        }
    }
}

impl From<&ParseAsType> for WasmProcessorType {
    fn from(ty: &ParseAsType) -> Self {
        match ty {
            ParseAsType::U8 => Self::U8,
            ParseAsType::I8 => Self::I8,
            ParseAsType::U16 => Self::U16,
            ParseAsType::I16 => Self::I16,
            ParseAsType::U32 => Self::U32,
            ParseAsType::I32 => Self::I32,
            ParseAsType::U64 => Self::U64,
            ParseAsType::I64 => Self::I64,
            ParseAsType::Bool => Self::Bool,
            ParseAsType::String => Self::String,
            ParseAsType::Bytes => Self::Bytes,
            ParseAsType::Datetime => Self::Datetime,
            ParseAsType::Duration => Self::Duration,
            ParseAsType::F32 => Self::F32,
            ParseAsType::F64 => Self::F64,
            ParseAsType::Array { element, len } => Self::Array {
                element: Box::new(Self::from(element.as_ref())),
                len: len.get(),
            },
            ParseAsType::Vec { element } => Self::Vec {
                element: Box::new(Self::from(element.as_ref())),
            },
        }
    }
}

/// A serialized-size estimate for one guest protocol message, in bytes.
///
/// Every term is the length of a value already resident in memory or a fixed per-element overhead
/// constant, so the running total is bounded by the address space those values already occupy. An
/// estimate that leaves `usize` was therefore built from something other than the message about to
/// be encoded, which is an invariant violation and not a size worth clamping.
#[derive(Clone, Copy, Debug, Default)]
struct CapacityHint(usize);

impl CapacityHint {
    const OVERFLOW: &'static str =
        "a serialized-size estimate only sums lengths of values already held in memory";

    const fn of(bytes: usize) -> Self {
        Self(bytes)
    }

    fn plus(self, bytes: usize) -> Self {
        Self(self.0.checked_add(bytes).assured(Self::OVERFLOW))
    }

    fn plus_all(self, bytes: impl IntoIterator<Item = usize>) -> Self {
        bytes.into_iter().fold(self, Self::plus)
    }

    fn plus_each(self, count: usize, bytes_each: usize) -> Self {
        self.plus(count.checked_mul(bytes_each).assured(Self::OVERFLOW))
    }

    const fn bytes(self) -> usize {
        self.0
    }
}

impl WasmBranchInit {
    fn serialized_capacity_hint(&self) -> usize {
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "an initial guest-buffer estimate with headroom over a BranchInit \
                          message's worst-case FlatBuffers framing (size prefix, identifier, root \
                          offset, Message and BranchInit tables and vtables, and its string and \
                          vector prefixes); an underestimate only spills and regrows the buffer, \
                          so it bounds nothing"
            )
        )]
        const BRANCH_INIT_OVERHEAD: usize = 512;

        let branch_key = match &self.branch_key {
            Some(branch_key) => branch_key.len(),
            None => 0,
        };
        CapacityHint::of(self.domain_name.len())
            .plus(self.domain_type.len())
            .plus(branch_key)
            .plus(self.input_schema.serialized_capacity_hint())
            .plus_all(
                self.output_schemas
                    .iter()
                    .map(WasmProcessorSchema::serialized_capacity_hint),
            )
            .plus(BRANCH_INIT_OVERHEAD)
            .bytes()
    }

    fn to_protocol(&self) -> protocol::BranchInit {
        protocol::BranchInit {
            domain_name: self.domain_name.clone(),
            domain_type: self.domain_type.clone(),
            branch_key: self.branch_key.clone(),
            input_schema: self.input_schema.to_protocol(),
            output_schemas: self
                .output_schemas
                .iter()
                .map(WasmProcessorSchema::to_protocol)
                .collect(),
        }
    }
}

impl WasmProcessorSchema {
    fn serialized_capacity_hint(&self) -> usize {
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "an initial guest-buffer estimate with headroom over a ProcessorSchema's \
                          worst-case FlatBuffers framing (its table and vtable, its name and \
                          field-vector prefixes and its slot in the output schemas); an \
                          underestimate only spills and regrows the buffer, so it bounds nothing"
            )
        )]
        const SCHEMA_OVERHEAD: usize = 96;

        CapacityHint::of(self.name.len())
            .plus(SCHEMA_OVERHEAD)
            .plus_all(
                self.fields
                    .iter()
                    .map(WasmProcessorField::serialized_capacity_hint),
            )
            .bytes()
    }

    fn to_protocol(&self) -> protocol::ProcessorSchema {
        protocol::ProcessorSchema {
            name: self.name.clone(),
            fields: self
                .fields
                .iter()
                .map(WasmProcessorField::to_protocol)
                .collect(),
        }
    }
}

impl WasmProcessorField {
    fn serialized_capacity_hint(&self) -> usize {
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "an initial guest-buffer estimate with headroom over a ProcessorField's \
                          worst-case FlatBuffers framing (its table and vtable, its name's prefix \
                          and terminator and its slot in the field vector); an underestimate only \
                          spills and regrows the buffer, so it bounds nothing"
            )
        )]
        const FIELD_OVERHEAD: usize = 64;

        CapacityHint::of(self.name.len())
            .plus(self.ty.serialized_capacity_hint())
            .plus(FIELD_OVERHEAD)
            .bytes()
    }

    fn to_protocol(&self) -> protocol::ProcessorField {
        protocol::ProcessorField {
            name: self.name.clone(),
            ty: self.ty.to_protocol(),
            optional: self.optional,
        }
    }
}

impl WasmProcessorType {
    fn serialized_capacity_hint(&self) -> usize {
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "an initial guest-buffer estimate with headroom over a ProcessorType \
                          table's worst-case FlatBuffers framing (its vtable offset and vtable, \
                          kind, element offset and array length); an underestimate only spills \
                          and regrows the buffer, so it bounds nothing"
            )
        )]
        const TYPE_OVERHEAD: usize = 64;

        match self {
            Self::Array { element, .. } | Self::Vec { element } => CapacityHint::of(TYPE_OVERHEAD)
                .plus(element.serialized_capacity_hint())
                .bytes(),
            _ => TYPE_OVERHEAD,
        }
    }

    fn to_protocol(&self) -> protocol::ProcessorType {
        match self {
            Self::U8 => protocol::ProcessorType::U8,
            Self::I8 => protocol::ProcessorType::I8,
            Self::U16 => protocol::ProcessorType::U16,
            Self::I16 => protocol::ProcessorType::I16,
            Self::U32 => protocol::ProcessorType::U32,
            Self::I32 => protocol::ProcessorType::I32,
            Self::U64 => protocol::ProcessorType::U64,
            Self::I64 => protocol::ProcessorType::I64,
            Self::Bool => protocol::ProcessorType::Bool,
            Self::String => protocol::ProcessorType::String,
            Self::Bytes => protocol::ProcessorType::Bytes,
            Self::Datetime => protocol::ProcessorType::Datetime,
            Self::Duration => protocol::ProcessorType::Duration,
            Self::F32 => protocol::ProcessorType::F32,
            Self::F64 => protocol::ProcessorType::F64,
            Self::Array { element, len } => protocol::ProcessorType::Array {
                element: Box::new(element.to_protocol()),
                len: *len,
            },
            Self::Vec { element } => protocol::ProcessorType::Vec {
                element: Box::new(element.to_protocol()),
            },
        }
    }
}

/// The domain time chosen by the data plane for one guest invocation.
///
/// Runtime lifecycle and clock-generation validation happen before this engine boundary. The WASM
/// host receives only the resulting timestamp and remains independent of domains and graphs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmExecutionContext {
    now: Timestamp,
}

impl WasmExecutionContext {
    pub const fn new(now: Timestamp) -> Self {
        Self { now }
    }

    pub const fn now(self) -> Timestamp {
        self.now
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WasmTimeoutHandle(i64);

impl WasmTimeoutHandle {
    pub const fn raw(self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmTimeoutRequest {
    pub handle: WasmTimeoutHandle,
    pub requested_at: Timestamp,
    pub delay: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WasmAckToken(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WasmAckTokenSet {
    pub tokens: Vec<WasmAckToken>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WasmOutputRow {
    pub tokens: Vec<WasmAckToken>,
    pub source_token: Option<WasmAckToken>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmNackSet {
    pub tokens: Vec<WasmAckToken>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmMessageErrorSet {
    pub tokens: Vec<WasmAckToken>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WasmAckSidecar {
    pub rows: Vec<WasmOutputRow>,
    pub acked: Vec<WasmAckTokenSet>,
    pub nacked: Vec<WasmNackSet>,
    pub message_errors: Vec<WasmMessageErrorSet>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmEnvelope {
    Input {
        arrow_ipc_batch: Vec<u8>,
        acks: WasmAckSidecar,
    },
    Output {
        generated_arrow_ipc_batch: Bytes,
        outputs: Vec<WasmRoutedOutput>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmRoutedOutput {
    pub output_relay: String,
    pub columns: Vec<WasmOutputColumnRef>,
    pub acks: WasmAckSidecar,
}

impl WasmRoutedOutput {
    pub fn new(
        output_relay: impl Into<String>,
        columns: Vec<WasmOutputColumnRef>,
        acks: WasmAckSidecar,
    ) -> Self {
        Self {
            output_relay: output_relay.into(),
            columns,
            acks,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmOutputColumnRef {
    Generated { column_index: u32 },
    Input { column_index: u32 },
    Uninitialized,
}

enum ProtocolEnvelopeEncoding<'a> {
    Input {
        arrow_ipc_batch: &'a [u8],
        acks: protocol::AckSidecar,
    },
    Output {
        generated_arrow_ipc_batch: &'a [u8],
        outputs: Vec<protocol::RoutedOutput>,
    },
}

impl<'a> ProtocolEnvelopeEncoding<'a> {
    fn new(envelope: &'a WasmEnvelope) -> Self {
        match envelope {
            WasmEnvelope::Input {
                arrow_ipc_batch,
                acks,
            } => Self::Input {
                arrow_ipc_batch,
                acks: acks.to_protocol(),
            },
            WasmEnvelope::Output {
                generated_arrow_ipc_batch,
                outputs,
            } => Self::Output {
                generated_arrow_ipc_batch,
                outputs: outputs.iter().map(WasmRoutedOutput::to_protocol).collect(),
            },
        }
    }

    fn encode_in<'b, A: Allocator + 'b>(&self, builder: &mut FlatBufferBuilder<'b, A>) {
        match self {
            Self::Input {
                arrow_ipc_batch,
                acks,
            } => protocol::encode_input_in(builder, arrow_ipc_batch, acks),
            Self::Output {
                generated_arrow_ipc_batch,
                outputs,
            } => protocol::encode_output_in(builder, generated_arrow_ipc_batch, outputs),
        }
    }
}

trait GuestMessageEncoding {
    fn encode_in<'a, A: Allocator + 'a>(&self, builder: &mut FlatBufferBuilder<'a, A>);
}

impl GuestMessageEncoding for ProtocolEnvelopeEncoding<'_> {
    fn encode_in<'a, A: Allocator + 'a>(&self, builder: &mut FlatBufferBuilder<'a, A>) {
        Self::encode_in(self, builder);
    }
}

impl GuestMessageEncoding for protocol::BranchInit {
    fn encode_in<'a, A: Allocator + 'a>(&self, builder: &mut FlatBufferBuilder<'a, A>) {
        protocol::BranchInit::encode_in(self, builder);
    }
}

impl WasmOutputColumnRef {
    pub const fn generated(column_index: u32) -> Self {
        Self::Generated { column_index }
    }

    pub const fn input(column_index: u32) -> Self {
        Self::Input { column_index }
    }

    pub const fn uninitialized() -> Self {
        Self::Uninitialized
    }

    pub const fn is_input(&self) -> bool {
        matches!(self, Self::Input { .. })
    }

    pub const fn is_uninitialized(&self) -> bool {
        matches!(self, Self::Uninitialized)
    }
}

impl WasmEnvelope {
    fn serialized_capacity_hint(&self) -> usize {
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "an initial guest-buffer estimate with headroom over an envelope's \
                          worst-case FlatBuffers framing (size prefix, identifier, root offset, \
                          Message and envelope tables and vtables, and its batch and output \
                          vector prefixes); an underestimate only spills and regrows the buffer, \
                          so it bounds nothing"
            )
        )]
        const ROOT_OVERHEAD: usize = 256;
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "an initial guest-buffer estimate with headroom over a RoutedOutput's \
                          worst-case FlatBuffers framing (its table and vtable, its relay name \
                          and column-vector prefixes and its slot in the outputs); an \
                          underestimate only spills and regrows the buffer, so it bounds nothing"
            )
        )]
        const ROUTED_OUTPUT_OVERHEAD: usize = 96;
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "an OutputColumnRef table, its vtable offset, index and padded source \
                          byte, and its slot in the columns vector take 16 bytes beside the \
                          vtable FlatBuffers shares between columns"
            )
        )]
        const COLUMN_OVERHEAD: usize = 16;

        match self {
            Self::Input {
                arrow_ipc_batch,
                acks,
            } => CapacityHint::of(arrow_ipc_batch.len())
                .plus(acks.serialized_capacity_hint())
                .plus(ROOT_OVERHEAD)
                .bytes(),
            Self::Output {
                generated_arrow_ipc_batch,
                outputs,
            } => outputs
                .iter()
                .fold(
                    CapacityHint::of(generated_arrow_ipc_batch.len()).plus(ROOT_OVERHEAD),
                    |size, output| {
                        size.plus(output.output_relay.len())
                            .plus_each(output.columns.len(), COLUMN_OVERHEAD)
                            .plus(output.acks.serialized_capacity_hint())
                            .plus(ROUTED_OUTPUT_OVERHEAD)
                    },
                )
                .bytes(),
        }
    }

    pub const fn input_acks(&self) -> Option<&WasmAckSidecar> {
        if let Self::Input { acks, .. } = self {
            Some(acks)
        } else {
            None
        }
    }

    pub fn input(arrow_ipc_batch: Vec<u8>, acks: WasmAckSidecar) -> Self {
        Self::Input {
            arrow_ipc_batch,
            acks,
        }
    }

    pub fn output(generated_arrow_ipc_batch: Vec<u8>, outputs: Vec<WasmRoutedOutput>) -> Self {
        Self::Output {
            generated_arrow_ipc_batch: generated_arrow_ipc_batch.into(),
            outputs,
        }
    }

    pub fn input_arrow_only(arrow_ipc_batch: Vec<u8>) -> Self {
        Self::Input {
            arrow_ipc_batch,
            acks: WasmAckSidecar::default(),
        }
    }

    pub fn encode(&self) -> StackResult<Vec<u8>, protocol::ProtocolError> {
        let encoding = ProtocolEnvelopeEncoding::new(self);
        let mut builder = FlatBufferBuilder::new();
        encoding.encode_in(&mut builder);
        Ok(builder.finished_data().to_vec())
    }

    pub fn decode(bytes: &[u8]) -> StackResult<Self, protocol::ProtocolError> {
        Self::decode_owned(bytes.to_vec())
    }

    pub fn decode_borrowed(
        bytes: &[u8],
    ) -> StackResult<protocol::EnvelopeRef<'_>, protocol::ProtocolError> {
        protocol::EnvelopeRef::decode(bytes)
    }

    fn decode_owned(bytes: Vec<u8>) -> StackResult<Self, protocol::ProtocolError> {
        let bytes = Bytes::from(bytes);
        let envelope = protocol::EnvelopeRef::decode(&bytes)?;
        match envelope {
            protocol::EnvelopeRef::Input(input) => Ok(Self::Input {
                arrow_ipc_batch: input.arrow_ipc_batch().to_vec(),
                acks: WasmAckSidecar::from_protocol(input.acks()),
            }),
            protocol::EnvelopeRef::Output(output) => {
                let generated = output.generated_arrow_ipc_batch();
                let generated_arrow_ipc_batch = if generated.is_empty() {
                    Bytes::new()
                } else {
                    let offset = generated
                        .as_ptr()
                        .addr()
                        .checked_sub(bytes.as_ptr().addr())
                        .assured("the accessor returns a subslice of the envelope bytes");
                    let end = offset
                        .checked_add(generated.len())
                        .assured("the subslice ends inside the envelope bytes");
                    bytes.slice(offset..end)
                };
                Ok(Self::Output {
                    generated_arrow_ipc_batch,
                    outputs: output
                        .outputs()?
                        .into_iter()
                        .map(WasmRoutedOutput::from_protocol)
                        .collect(),
                })
            }
        }
    }
}

impl WasmAckSidecar {
    fn serialized_capacity_hint(&self) -> usize {
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "an initial guest-buffer estimate with headroom over an AckSidecar's \
                          worst-case FlatBuffers framing (its table and vtable and the prefixes \
                          of its four set vectors); an underestimate only spills and regrows the \
                          buffer, so it bounds nothing"
            )
        )]
        const SIDECAR_OVERHEAD: usize = 128;
        #[cfg_attr(
            nervix_lint,
            nervix::constant(
                fixed,
                reason = "an initial guest-buffer estimate with headroom over one token set's \
                          worst-case FlatBuffers framing (its table and vtable, its token \
                          vector's prefix and alignment, its reason's prefix and its slot in the \
                          set vector); an underestimate only spills and regrows the buffer, so it \
                          bounds nothing"
            )
        )]
        const SET_OVERHEAD: usize = 64;

        let token_count = CapacityHint::default()
            .plus_all(
                self.rows
                    .iter()
                    .map(|row| row.tokens.len())
                    .chain(self.acked.iter().map(|set| set.tokens.len()))
                    .chain(self.nacked.iter().map(|set| set.tokens.len()))
                    .chain(self.message_errors.iter().map(|set| set.tokens.len())),
            )
            .bytes();
        let set_count = CapacityHint::of(self.rows.len())
            .plus(self.acked.len())
            .plus(self.nacked.len())
            .plus(self.message_errors.len())
            .bytes();
        let reason_bytes = CapacityHint::default()
            .plus_all(
                self.nacked
                    .iter()
                    .map(|set| set.reason.len())
                    .chain(self.message_errors.iter().map(|set| set.reason.len())),
            )
            .bytes();

        CapacityHint::default()
            .plus_each(token_count, std::mem::size_of::<u64>())
            .plus_each(set_count, SET_OVERHEAD)
            .plus(reason_bytes)
            .plus(SIDECAR_OVERHEAD)
            .bytes()
    }

    fn to_protocol(&self) -> protocol::AckSidecar {
        protocol::AckSidecar {
            rows: self
                .rows
                .iter()
                .map(|row| protocol::OutputRow {
                    tokens: row
                        .tokens
                        .iter()
                        .map(|token| protocol::AckToken(token.0))
                        .collect(),
                    source_token: row.source_token.map(|token| protocol::AckToken(token.0)),
                })
                .collect(),
            acked: self
                .acked
                .iter()
                .map(|set| protocol::AckTokenSet {
                    tokens: set
                        .tokens
                        .iter()
                        .map(|token| protocol::AckToken(token.0))
                        .collect(),
                })
                .collect(),
            nacked: self
                .nacked
                .iter()
                .map(|set| protocol::NackSet {
                    tokens: set
                        .tokens
                        .iter()
                        .map(|token| protocol::AckToken(token.0))
                        .collect(),
                    reason: set.reason.clone(),
                })
                .collect(),
            message_errors: self
                .message_errors
                .iter()
                .map(|set| protocol::MessageErrorSet {
                    tokens: set
                        .tokens
                        .iter()
                        .map(|token| protocol::AckToken(token.0))
                        .collect(),
                    reason: set.reason.clone(),
                })
                .collect(),
        }
    }

    fn from_protocol(sidecar: protocol::AckSidecar) -> Self {
        Self {
            rows: sidecar
                .rows
                .into_iter()
                .map(|row| WasmOutputRow {
                    tokens: row
                        .tokens
                        .into_iter()
                        .map(|token| WasmAckToken(token.0))
                        .collect(),
                    source_token: row.source_token.map(|token| WasmAckToken(token.0)),
                })
                .collect(),
            acked: sidecar
                .acked
                .into_iter()
                .map(|set| WasmAckTokenSet {
                    tokens: set
                        .tokens
                        .into_iter()
                        .map(|token| WasmAckToken(token.0))
                        .collect(),
                })
                .collect(),
            nacked: sidecar
                .nacked
                .into_iter()
                .map(|set| WasmNackSet {
                    tokens: set
                        .tokens
                        .into_iter()
                        .map(|token| WasmAckToken(token.0))
                        .collect(),
                    reason: set.reason,
                })
                .collect(),
            message_errors: sidecar
                .message_errors
                .into_iter()
                .map(|set| WasmMessageErrorSet {
                    tokens: set
                        .tokens
                        .into_iter()
                        .map(|token| WasmAckToken(token.0))
                        .collect(),
                    reason: set.reason,
                })
                .collect(),
        }
    }
}

impl WasmRoutedOutput {
    fn to_protocol(&self) -> protocol::RoutedOutput {
        protocol::RoutedOutput {
            output_relay: self.output_relay.clone(),
            columns: self
                .columns
                .iter()
                .map(|column| match column {
                    WasmOutputColumnRef::Generated { column_index } => {
                        protocol::OutputColumnRef::Generated {
                            column_index: *column_index,
                        }
                    }
                    WasmOutputColumnRef::Input { column_index } => {
                        protocol::OutputColumnRef::Input {
                            column_index: *column_index,
                        }
                    }
                    WasmOutputColumnRef::Uninitialized => protocol::OutputColumnRef::Uninitialized,
                })
                .collect(),
            acks: self.acks.to_protocol(),
        }
    }

    fn from_protocol(output: protocol::RoutedOutput) -> Self {
        Self {
            output_relay: output.output_relay,
            columns: output
                .columns
                .into_iter()
                .map(|column| match column {
                    protocol::OutputColumnRef::Generated { column_index } => {
                        WasmOutputColumnRef::Generated { column_index }
                    }
                    protocol::OutputColumnRef::Input { column_index } => {
                        WasmOutputColumnRef::Input { column_index }
                    }
                    protocol::OutputColumnRef::Uninitialized => WasmOutputColumnRef::Uninitialized,
                })
                .collect(),
            acks: WasmAckSidecar::from_protocol(output.acks),
        }
    }
}

/// What the guest asked the host to do with this branch's guest-state lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WasmGuestStateLifetime {
    /// Nothing was asked, so the branch continues in the lifetime its callback ran in.
    Kept,
    /// The guest asked the host to replace the branch's complete guest-state lifetime.
    ResetRequested,
}

impl WasmGuestStateLifetime {
    pub const fn is_reset_requested(self) -> bool {
        matches!(self, Self::ResetRequested)
    }
}

/// Whether the guest operation in progress may ask the host to replace this branch's guest-state
/// lifetime, and what it has asked for.
///
/// Only a callback the host completes with a checkpoint owns uncommitted effects a reset can
/// discard, so only a callback admits a request. Every other operation — initializing, saving,
/// restoring, draining the emit queue — is refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GuestStateResetRequests {
    /// No callback is running, so a request has nothing to discard.
    Refused,
    /// A callback is running and has not asked.
    Admitted,
    /// A callback asked, whether or not it is still running. Asking again changes nothing, because
    /// the host replaces the lifetime once.
    Requested,
}

impl GuestStateResetRequests {
    /// A callback is about to run and owns the effects a request would discard.
    const fn admitted(self) -> Self {
        match self {
            Self::Refused | Self::Admitted => Self::Admitted,
            Self::Requested => Self::Requested,
        }
    }

    /// The callback returned, so the rest of this host call no longer owns those effects.
    const fn closed(self) -> Self {
        match self {
            Self::Refused | Self::Admitted => Self::Refused,
            Self::Requested => Self::Requested,
        }
    }
}

struct BranchStore {
    resource_limiter: GuestResourceLimiter,
    timeout_requests: Vec<WasmTimeoutRequest>,
    next_timeout_handle: i64,
    emitted_batch_sender: Option<mpsc::UnboundedSender<WasmEnvelope>>,
    state_reset_requests: GuestStateResetRequests,
}

impl BranchStore {
    fn new(
        limits: WasmProcessorLimits,
        emitted_batch_sender: Option<mpsc::UnboundedSender<WasmEnvelope>>,
    ) -> Self {
        Self {
            resource_limiter: GuestResourceLimiter::new(limits),
            timeout_requests: Vec::new(),
            next_timeout_handle: 1,
            emitted_batch_sender,
            state_reset_requests: GuestStateResetRequests::Refused,
        }
    }

    fn admit_state_reset_requests(&mut self) {
        self.state_reset_requests = self.state_reset_requests.admitted();
    }

    fn close_state_reset_requests(&mut self) {
        self.state_reset_requests = self.state_reset_requests.closed();
    }

    /// Records a guest's request to replace this branch's guest-state lifetime and answers it.
    fn request_state_reset(&mut self) -> i32 {
        match self.state_reset_requests {
            GuestStateResetRequests::Refused => protocol::StateResetRequestAnswer::Refused.code(),
            GuestStateResetRequests::Admitted | GuestStateResetRequests::Requested => {
                self.state_reset_requests = GuestStateResetRequests::Requested;
                protocol::StateResetRequestAnswer::Accepted.code()
            }
        }
    }

    fn take_requested_state_reset(&mut self) -> WasmGuestStateLifetime {
        let requested = self.state_reset_requests == GuestStateResetRequests::Requested;
        self.state_reset_requests = GuestStateResetRequests::Refused;
        if requested {
            WasmGuestStateLifetime::ResetRequested
        } else {
            WasmGuestStateLifetime::Kept
        }
    }

    fn now(&self) -> Timestamp {
        INVOCATION_NOW
            .try_with(|now| *now)
            .assured("every guest call is scoped to an explicit WASM execution context")
    }

    fn timeout_after(&mut self, delay_nanos: i64) -> i64 {
        if delay_nanos < 0 {
            return ERR_INVALID_SIZE.into();
        }
        let Ok(delay_nanos) = u64::try_from(delay_nanos) else {
            return ERR_INVALID_SIZE.into();
        };
        let handle = WasmTimeoutHandle(self.next_timeout_handle);
        self.next_timeout_handle = self
            .next_timeout_handle
            .checked_add(1)
            .assured("a guest cannot request 2^64 timeouts within one branch instance");
        self.timeout_requests.push(WasmTimeoutRequest {
            handle,
            requested_at: self.now(),
            delay: Duration::from_nanos(delay_nanos),
        });
        handle.raw()
    }
}

pub struct CompiledWasmProcessor {
    engine: Engine,
    instance_pre: InstancePre<BranchStore>,
    epoch_deadline_ticks: u64,
    max_guest_buffer_bytes: usize,
}

impl CompiledWasmProcessor {
    /// Instantiates, initializes, and, when `restored_state` holds saved state, restores one branch
    /// instance. The failure names which of those operations failed.
    pub async fn instantiate_branch(
        &self,
        limits: WasmProcessorLimits,
        init: WasmBranchInit,
        context: WasmExecutionContext,
        restored_state: Option<&[u8]>,
    ) -> StackResult<WasmBranchInstance, WasmGuestError> {
        self.instantiate_branch_with_emitter(limits, init, context, restored_state, None)
            .await
    }

    pub async fn instantiate_branch_with_emitter(
        &self,
        limits: WasmProcessorLimits,
        init: WasmBranchInit,
        context: WasmExecutionContext,
        restored_state: Option<&[u8]>,
        emitted_batch_sender: Option<mpsc::UnboundedSender<WasmEnvelope>>,
    ) -> StackResult<WasmBranchInstance, WasmGuestError> {
        let instantiation = INVOCATION_NOW
            .scope(
                context.now(),
                self.instantiate_store(limits, emitted_batch_sender),
            )
            .await;
        let mut branch = match instantiation {
            Ok(branch) => branch,
            Err(cause) => {
                return Err(WasmGuestError::failed(
                    WasmGuestOperation::Instantiation,
                    cause,
                ));
            }
        };
        branch.init(init, context).await?;
        if let Some(restored_state) = restored_state {
            branch.load_state(restored_state, context).await?;
        }
        Ok(branch)
    }

    async fn instantiate_store(
        &self,
        limits: WasmProcessorLimits,
        emitted_batch_sender: Option<mpsc::UnboundedSender<WasmEnvelope>>,
    ) -> StackResult<WasmBranchInstance, WasmGuestCallError> {
        let mut store = Store::new(&self.engine, BranchStore::new(limits, emitted_batch_sender));
        store.limiter(|state| &mut state.resource_limiter);
        store.set_fuel(limits.max_fuel.get()).map_err(|source| {
            Report::new(WasmGuestCallError::ResetFuel {
                limit: limits.max_fuel,
                source,
            })
        })?;
        store.set_epoch_deadline(self.epoch_deadline_ticks);
        store.epoch_deadline_async_yield_and_update(self.epoch_deadline_ticks);
        let instance = self
            .instance_pre
            .instantiate_async(&mut store)
            .await
            .map_err(|source| WasmGuestCallError::instantiation(limits, source))?;
        WasmBranchInstance::load_exports(store, instance, self.max_guest_buffer_bytes, limits)
    }
}

pub struct WasmBranchInstance {
    store: Store<BranchStore>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    init: TypedFunc<(i32, i32), i32>,
    process_batch: TypedFunc<(i32, i32), i32>,
    on_timeout: TypedFunc<i64, i32>,
    flush: TypedFunc<(), i32>,
    dump_state: TypedFunc<(), i32>,
    load_state: TypedFunc<(i32, i32), i32>,
    reset_state: TypedFunc<(), i32>,
    describe_state: TypedFunc<(), i32>,
    read_emit: TypedFunc<(), i32>,
    current_domain_time_nanos: TypedFunc<(), i64>,
    buffer_ptr: TypedFunc<(), i32>,
    global_error: Option<WasmGlobalErrorExports>,
    guest_buffer_capacity: usize,
    max_guest_buffer_bytes: usize,
    limits: WasmProcessorLimits,
}

struct WasmGlobalErrorExports {
    ptr: TypedFunc<(), i32>,
    len: TypedFunc<(), i32>,
    clear: TypedFunc<(), i32>,
}

impl std::fmt::Debug for WasmBranchInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmBranchInstance").finish_non_exhaustive()
    }
}

impl WasmBranchInstance {
    fn load_exports(
        mut store: Store<BranchStore>,
        instance: Instance,
        max_guest_buffer_bytes: usize,
        limits: WasmProcessorLimits,
    ) -> StackResult<Self, WasmGuestCallError> {
        let memory = instance
            .get_memory(&mut store, EXPORT_MEMORY)
            .ok_or_else(|| Report::new(WasmGuestCallError::MissingExport(EXPORT_MEMORY)))?;
        Ok(Self {
            alloc: typed_export(&mut store, &instance, "nervix_alloc")?,
            init: typed_export(&mut store, &instance, "nervix_init")?,
            process_batch: typed_export(&mut store, &instance, "nervix_process_batch")?,
            on_timeout: typed_export(&mut store, &instance, "nervix_on_timeout")?,
            flush: typed_export(&mut store, &instance, "nervix_flush")?,
            dump_state: typed_export(&mut store, &instance, "nervix_dump_state")?,
            load_state: typed_export(&mut store, &instance, "nervix_load_state")?,
            reset_state: typed_export(&mut store, &instance, "nervix_reset_state")?,
            describe_state: typed_export(&mut store, &instance, "nervix_describe_state")?,
            read_emit: typed_export(&mut store, &instance, "nervix_read_emit")?,
            current_domain_time_nanos: typed_export(
                &mut store,
                &instance,
                "nervix_current_domain_time_nanos",
            )?,
            buffer_ptr: typed_export(&mut store, &instance, "nervix_buffer_ptr")?,
            global_error: optional_global_error_exports(&mut store, &instance)?,
            guest_buffer_capacity: 0,
            max_guest_buffer_bytes,
            limits,
            store,
            memory,
        })
    }

    fn begin_operation(&mut self) -> StackResult<(), WasmGuestCallError> {
        self.store
            .set_fuel(self.limits.max_fuel.get())
            .map_err(|source| {
                Report::new(WasmGuestCallError::ResetFuel {
                    limit: self.limits.max_fuel,
                    source,
                })
            })
    }

    /// Calls one guest callback with state-reset requests admitted.
    ///
    /// A callback is the only operation whose effects are still uncommitted while it runs, so it
    /// is the only operation a request can discard anything from. Requests close again as soon as
    /// it returns, before the host drains the emit queue.
    async fn call_guest_callback<Params>(
        &mut self,
        callback: &TypedFunc<Params, i32>,
        params: Params,
    ) -> wasmtime::Result<i32>
    where
        Params: wasmtime::WasmParams + Send + Sync,
    {
        self.store.data_mut().admit_state_reset_requests();
        let called = callback.call_async(&mut self.store, params).await;
        self.store.data_mut().close_state_reset_requests();
        called
    }

    /// What the callback that just ran asked the host to do with this branch's guest-state
    /// lifetime.
    ///
    /// Taken once per callback, whether it succeeded or failed: the request belongs to the
    /// callback that made it, and a callback that asked and then failed is still asking.
    pub fn take_requested_state_reset(&mut self) -> WasmGuestStateLifetime {
        self.store.data_mut().take_requested_state_reset()
    }

    /// Settles a guest callback that reports failure through its return code and the global-error
    /// channel. A reason on that channel explains the failure better than the code or the trap it
    /// ended in, so it wins over both unless an execution limit ended the call.
    async fn settle_callback(
        &mut self,
        export: &'static str,
        call_result: wasmtime::Result<i32>,
    ) -> StackResult<(), WasmGuestCallError> {
        let code = match call_result {
            Ok(code) => code,
            Err(source) => {
                if let Some(cause) =
                    WasmGuestCallError::limit_exceeded(self.limits, Some(export), &source)
                {
                    return Err(Report::new(cause).attach(source));
                }
                if let Some(reason) = self.take_global_error().await? {
                    return Err(Report::new(WasmGuestCallError::GlobalError { reason }));
                }
                return Err(WasmGuestCallError::call(self.limits, export, source));
            }
        };
        if let Some(reason) = self.take_global_error().await? {
            return Err(Report::new(WasmGuestCallError::GlobalError { reason }));
        }
        ensure_success(export, code)
    }

    async fn init(
        &mut self,
        init: WasmBranchInit,
        context: WasmExecutionContext,
    ) -> StackResult<(), WasmGuestError> {
        let initialization = INVOCATION_NOW
            .scope(context.now(), async {
                self.begin_operation()?;
                let capacity_hint = init.serialized_capacity_hint();
                let encoding = init.to_protocol();
                let (ptr, size) = self
                    .write_message_to_guest(&encoding, capacity_hint)
                    .await?;
                let call_result = self.init.call_async(&mut self.store, (ptr, size)).await;
                self.settle_callback("nervix_init", call_result).await
            })
            .await;
        initialization
            .map_err(|cause| WasmGuestError::failed(WasmGuestOperation::Initialization, cause))
    }

    pub async fn init_in_context(
        &mut self,
        init: WasmBranchInit,
        context: WasmExecutionContext,
    ) -> StackResult<(), WasmGuestError> {
        self.init(init, context).await
    }

    async fn current_domain_time(
        &mut self,
        context: WasmExecutionContext,
    ) -> StackResult<Timestamp, WasmGuestError> {
        let read = INVOCATION_NOW
            .scope(context.now(), async {
                self.begin_operation()?;
                let nanos = self
                    .current_domain_time_nanos
                    .call_async(&mut self.store, ())
                    .await
                    .map_err(|source| {
                        WasmGuestCallError::call(
                            self.limits,
                            "nervix_current_domain_time_nanos",
                            source,
                        )
                    })?;
                Ok(Timestamp::from_unix_nanos(nanos))
            })
            .await;
        read.map_err(|cause| WasmGuestError::failed(WasmGuestOperation::DomainTimeRead, cause))
    }

    pub async fn current_domain_time_in_context(
        &mut self,
        context: WasmExecutionContext,
    ) -> StackResult<Timestamp, WasmGuestError> {
        self.current_domain_time(context).await
    }

    pub async fn process_batch_in_context(
        &mut self,
        arrow_ipc_batch: &[u8],
        context: WasmExecutionContext,
    ) -> StackResult<Vec<WasmEnvelope>, WasmGuestError> {
        let envelope = WasmEnvelope::input_arrow_only(arrow_ipc_batch.to_vec());
        self.process_envelope_in_context(&envelope, context).await
    }

    async fn process_envelope(
        &mut self,
        envelope: &WasmEnvelope,
        context: WasmExecutionContext,
    ) -> StackResult<Vec<WasmEnvelope>, WasmGuestError> {
        let processing = INVOCATION_NOW
            .scope(context.now(), async {
                self.begin_operation()?;
                let (ptr, size) = self.write_envelope_to_guest(envelope).await?;
                let process_batch = self.process_batch.clone();
                let call_result = self.call_guest_callback(&process_batch, (ptr, size)).await;
                self.settle_callback("nervix_process_batch", call_result)
                    .await?;
                self.read_pending_emit().await
            })
            .await;
        processing
            .map_err(|cause| WasmGuestError::failed(WasmGuestOperation::BatchProcessing, cause))
    }

    pub async fn process_envelope_in_context(
        &mut self,
        envelope: &WasmEnvelope,
        context: WasmExecutionContext,
    ) -> StackResult<Vec<WasmEnvelope>, WasmGuestError> {
        self.process_envelope(envelope, context).await
    }

    async fn on_timeout(
        &mut self,
        handle: WasmTimeoutHandle,
        context: WasmExecutionContext,
    ) -> StackResult<Vec<WasmEnvelope>, WasmGuestError> {
        let callback = INVOCATION_NOW
            .scope(context.now(), async {
                self.begin_operation()?;
                let on_timeout = self.on_timeout.clone();
                let call_result = self.call_guest_callback(&on_timeout, handle.raw()).await;
                self.settle_callback("nervix_on_timeout", call_result)
                    .await?;
                self.read_pending_emit().await
            })
            .await;
        callback.map_err(|cause| WasmGuestError::failed(WasmGuestOperation::TimeoutCallback, cause))
    }

    pub async fn on_timeout_in_context(
        &mut self,
        handle: WasmTimeoutHandle,
        context: WasmExecutionContext,
    ) -> StackResult<Vec<WasmEnvelope>, WasmGuestError> {
        self.on_timeout(handle, context).await
    }

    /// Asks the guest to release everything it is holding because the host is quiescing this
    /// branch. Returns the output envelopes the guest emitted, which the caller must dispatch
    /// before snapshotting so a handoff neither loses nor duplicates them.
    async fn flush(
        &mut self,
        context: WasmExecutionContext,
    ) -> StackResult<Vec<WasmEnvelope>, WasmGuestError> {
        let flush = INVOCATION_NOW
            .scope(context.now(), async {
                self.begin_operation()?;
                let flush = self.flush.clone();
                let call_result = self.call_guest_callback(&flush, ()).await;
                self.settle_callback("nervix_flush", call_result).await?;
                self.read_pending_emit().await
            })
            .await;
        flush.map_err(|cause| WasmGuestError::failed(WasmGuestOperation::QuiesceFlush, cause))
    }

    pub async fn flush_in_context(
        &mut self,
        context: WasmExecutionContext,
    ) -> StackResult<Vec<WasmEnvelope>, WasmGuestError> {
        self.flush(context).await
    }

    /// Asks the guest to serialize its state. `nervix_dump_state` returns the size of the snapshot
    /// it wrote, or a negative code when it cannot produce one.
    async fn save_state(
        &mut self,
        context: WasmExecutionContext,
    ) -> StackResult<Vec<u8>, WasmGuestError> {
        let snapshot = INVOCATION_NOW
            .scope(context.now(), async {
                let export = "nervix_dump_state";
                self.begin_operation()?;
                let size = self
                    .dump_state
                    .call_async(&mut self.store, ())
                    .await
                    .map_err(|source| WasmGuestCallError::call(self.limits, export, source))?;
                if size < 0 {
                    if let Some(reason) = self.take_global_error().await? {
                        return Err(Report::new(WasmGuestCallError::GlobalError { reason }));
                    }
                    return Err(Report::new(WasmGuestCallError::ErrorCode {
                        export,
                        code: size,
                    }));
                }
                self.read_guest_buffer(size).await
            })
            .await;
        snapshot.map_err(|cause| WasmGuestError::failed(WasmGuestOperation::StateSnapshot, cause))
    }

    pub async fn save_state_in_context(
        &mut self,
        context: WasmExecutionContext,
    ) -> StackResult<Vec<u8>, WasmGuestError> {
        self.save_state(context).await
    }

    /// Hands the guest the state saved last. A rejection code is the guest's verdict on that
    /// state; any other failure is a failure of the restore itself.
    async fn load_state(
        &mut self,
        state: &[u8],
        context: WasmExecutionContext,
    ) -> StackResult<(), WasmGuestError> {
        let restore = WasmGuestOperation::StateRestore;
        INVOCATION_NOW
            .scope(context.now(), async {
                let export = "nervix_load_state";
                self.begin_operation()
                    .map_err(|cause| WasmGuestError::failed(restore, cause))?;
                let (ptr, size) = self
                    .write_to_guest_buffer(state)
                    .await
                    .map_err(|cause| WasmGuestError::failed(restore, cause))?;
                let call_result = self
                    .load_state
                    .call_async(&mut self.store, (ptr, size))
                    .await;
                let code = match call_result {
                    Ok(code) => code,
                    Err(source) => {
                        let cause = WasmGuestCallError::call(self.limits, export, source);
                        return Err(WasmGuestError::failed(restore, cause));
                    }
                };
                if code == SUCCESS {
                    return Ok(());
                }
                let reason = self
                    .take_global_error()
                    .await
                    .map_err(|cause| WasmGuestError::failed(restore, cause))?;
                if let Some(rejection) = SavedStateRejection::from_code(code) {
                    return Err(WasmGuestError::saved_state_rejected(rejection, reason));
                }
                let cause = match reason {
                    Some(reason) => WasmGuestCallError::GlobalError { reason },
                    None => WasmGuestCallError::ErrorCode { export, code },
                };
                Err(WasmGuestError::failed(restore, Report::new(cause)))
            })
            .await
    }

    pub async fn load_state_in_context(
        &mut self,
        state: &[u8],
        context: WasmExecutionContext,
    ) -> StackResult<(), WasmGuestError> {
        self.load_state(state, context).await
    }

    async fn reset_state(
        &mut self,
        context: WasmExecutionContext,
    ) -> StackResult<(), WasmGuestError> {
        let reset = INVOCATION_NOW
            .scope(context.now(), async {
                self.begin_operation()?;
                let code = self
                    .reset_state
                    .call_async(&mut self.store, ())
                    .await
                    .map_err(|source| {
                        WasmGuestCallError::call(self.limits, "nervix_reset_state", source)
                    })?;
                ensure_success("nervix_reset_state", code)
            })
            .await;
        reset.map_err(|cause| WasmGuestError::failed(WasmGuestOperation::StateReset, cause))
    }

    pub async fn reset_state_in_context(
        &mut self,
        context: WasmExecutionContext,
    ) -> StackResult<(), WasmGuestError> {
        self.reset_state(context).await
    }

    /// Asks the guest to describe its state. `nervix_describe_state` returns the size of the
    /// `StateDescription` it wrote, zero when it describes no state, or a negative code when it
    /// cannot describe its state. A description of no state has no sections.
    async fn describe_state(
        &mut self,
        context: WasmExecutionContext,
    ) -> StackResult<WasmStateDescription, WasmGuestError> {
        let description = INVOCATION_NOW
            .scope(context.now(), async {
                let export = "nervix_describe_state";
                self.begin_operation()?;
                let size = self
                    .describe_state
                    .call_async(&mut self.store, ())
                    .await
                    .map_err(|source| WasmGuestCallError::call(self.limits, export, source))?;
                if size < 0 {
                    if let Some(reason) = self.take_global_error().await? {
                        return Err(Report::new(WasmGuestCallError::GlobalError { reason }));
                    }
                    return Err(Report::new(WasmGuestCallError::ErrorCode {
                        export,
                        code: size,
                    }));
                }
                if size == 0 {
                    return Ok(WasmStateDescription {
                        sections: Vec::new(),
                    });
                }
                let message = self.read_guest_buffer(size).await?;
                let decoded = protocol::StateDescription::decode(&message)
                    .change_context(WasmGuestCallError::InvalidStateDescription)?;
                Ok(WasmStateDescription::from_protocol(decoded))
            })
            .await;
        description
            .map_err(|cause| WasmGuestError::failed(WasmGuestOperation::StateDescription, cause))
    }

    pub async fn describe_state_in_context(
        &mut self,
        context: WasmExecutionContext,
    ) -> StackResult<WasmStateDescription, WasmGuestError> {
        self.describe_state(context).await
    }

    pub fn timeout_requests(&self) -> &[WasmTimeoutRequest] {
        &self.store.data().timeout_requests
    }

    pub fn take_timeout_requests(&mut self) -> Vec<WasmTimeoutRequest> {
        std::mem::take(&mut self.store.data_mut().timeout_requests)
    }

    pub fn take_due_timeout_requests(&mut self, now: Timestamp) -> Vec<WasmTimeoutRequest> {
        let mut pending = Vec::new();
        let mut due = Vec::new();
        for request in std::mem::take(&mut self.store.data_mut().timeout_requests) {
            // A guest may ask for a delay that runs past the representable timestamp range. Such
            // a request has no deadline this clock can reach, so it stays pending forever rather
            // than becoming due immediately.
            let deadline = i64::try_from(request.delay.as_nanos())
                .ok()
                .and_then(|delay| request.requested_at.unix_nanos().checked_add(delay));
            if deadline.is_some_and(|deadline| deadline <= now.unix_nanos()) {
                due.push(request);
            } else {
                pending.push(request);
            }
        }
        self.store.data_mut().timeout_requests = pending;
        due
    }

    async fn take_global_error(&mut self) -> StackResult<Option<String>, WasmGuestCallError> {
        let Some(exports) = &self.global_error else {
            return Ok(None);
        };
        let size = exports
            .len
            .call_async(&mut self.store, ())
            .await
            .map_err(|source| {
                WasmGuestCallError::call(self.limits, "nervix_global_error_len", source)
            })?;
        if size == 0 {
            return Ok(None);
        }
        let size = usize::try_from(size)
            .map_err(|_| Report::new(WasmGuestCallError::InvalidSize(size)))?;
        if size > self.max_guest_buffer_bytes {
            return Err(Report::new(WasmGuestCallError::GuestBufferTooLarge {
                size,
                limit: self.max_guest_buffer_bytes,
            }));
        }
        let ptr = exports
            .ptr
            .call_async(&mut self.store, ())
            .await
            .map_err(|source| {
                WasmGuestCallError::call(self.limits, "nervix_global_error_ptr", source)
            })?;
        let ptr = usize::try_from(ptr)
            .map_err(|_| Report::new(WasmGuestCallError::InvalidOffset(ptr)))?;
        let mut bytes = vec![0; size];
        self.memory
            .read(&mut self.store, ptr, &mut bytes)
            .map_err(|source| Report::new(WasmGuestCallError::MemoryRead(source)))?;
        let code = exports
            .clear
            .call_async(&mut self.store, ())
            .await
            .map_err(|source| {
                WasmGuestCallError::call(self.limits, "nervix_clear_global_error", source)
            })?;
        ensure_success("nervix_clear_global_error", code)?;
        let reason = String::from_utf8(bytes)
            .map_err(|source| Report::new(WasmGuestCallError::InvalidGlobalErrorText(source)))?;
        Ok(Some(reason))
    }

    async fn write_envelope_to_guest(
        &mut self,
        envelope: &WasmEnvelope,
    ) -> StackResult<(i32, i32), WasmGuestCallError> {
        let encoding = ProtocolEnvelopeEncoding::new(envelope);
        self.write_message_to_guest(&encoding, envelope.serialized_capacity_hint())
            .await
    }

    async fn write_message_to_guest<E: GuestMessageEncoding>(
        &mut self,
        encoding: &E,
        capacity_hint: usize,
    ) -> StackResult<(i32, i32), WasmGuestCallError> {
        let capacity = self
            .guest_buffer_capacity
            .max(capacity_hint)
            .min(self.max_guest_buffer_bytes);
        let base_ptr = self.allocate_guest_buffer(capacity).await?;
        let base_offset = usize::try_from(base_ptr)
            .map_err(|_| Report::new(WasmGuestCallError::InvalidOffset(base_ptr)))?;

        let build = {
            let memory = self.memory.data_mut(&mut self.store);
            let memory_size = memory.len();
            let end = base_offset.checked_add(capacity).ok_or_else(|| {
                Report::new(WasmGuestCallError::InvalidBufferRange {
                    offset: base_offset,
                    size: capacity,
                    memory_size,
                })
            })?;
            let region = memory.get_mut(base_offset..end).ok_or_else(|| {
                Report::new(WasmGuestCallError::InvalidBufferRange {
                    offset: base_offset,
                    size: capacity,
                    memory_size,
                })
            })?;
            let allocator = GuestBufferAllocator::Direct(region);
            let mut builder = FlatBufferBuilder::new_in(allocator);
            encoding.encode_in(&mut builder);
            let (allocator, start) = builder.collapse_in();
            let size = allocator.len() - start;
            match allocator {
                GuestBufferAllocator::Direct(_) => GuestBufferBuild::Direct {
                    offset: start,
                    size,
                },
                GuestBufferAllocator::Spill(buffer) => GuestBufferBuild::Spill { buffer, start },
            }
        };

        match build {
            GuestBufferBuild::Direct { offset, size } => {
                let ptr = base_offset.checked_add(offset).ok_or_else(|| {
                    Report::new(WasmGuestCallError::InvalidBufferRange {
                        offset: base_offset,
                        size,
                        memory_size: self.memory.data_size(&self.store),
                    })
                })?;
                let ptr = i32::try_from(ptr).map_err(|_| {
                    Report::new(WasmGuestCallError::InvalidBufferRange {
                        offset: ptr,
                        size,
                        memory_size: self.memory.data_size(&self.store),
                    })
                })?;
                let size = i32::try_from(size)
                    .map_err(|_| Report::new(WasmGuestCallError::InvalidSize(-1)))?;
                Ok((ptr, size))
            }
            GuestBufferBuild::Spill { buffer, start } => {
                let bytes = &buffer[start..];
                if bytes.len() > self.max_guest_buffer_bytes {
                    return Err(Report::new(WasmGuestCallError::GuestBufferTooLarge {
                        size: bytes.len(),
                        limit: self.max_guest_buffer_bytes,
                    }));
                }
                // Doubling is a growth policy rather than an exact size: the target is clamped
                // to the configured guest buffer limit, so a doubling that leaves `usize` clamps
                // to that same limit.
                let growth_target = capacity
                    .checked_mul(2)
                    .unwrap_or(self.max_guest_buffer_bytes)
                    .clamp(bytes.len(), self.max_guest_buffer_bytes);
                let ptr = self.allocate_guest_buffer(growth_target).await?;
                self.memory
                    .write(
                        &mut self.store,
                        usize::try_from(ptr)
                            .verified("the guest allocator returned a non-negative pointer"),
                        bytes,
                    )
                    .map_err(|source| Report::new(WasmGuestCallError::MemoryWrite(source)))?;
                let size = i32::try_from(bytes.len())
                    .map_err(|_| Report::new(WasmGuestCallError::InvalidSize(-1)))?;
                Ok((ptr, size))
            }
        }
    }

    async fn allocate_guest_buffer(&mut self, size: usize) -> StackResult<i32, WasmGuestCallError> {
        if size > self.max_guest_buffer_bytes {
            return Err(Report::new(WasmGuestCallError::GuestBufferTooLarge {
                size,
                limit: self.max_guest_buffer_bytes,
            }));
        }
        let size =
            i32::try_from(size).map_err(|_| Report::new(WasmGuestCallError::InvalidSize(-1)))?;
        let ptr = self
            .alloc
            .call_async(&mut self.store, size)
            .await
            .map_err(|source| WasmGuestCallError::call(self.limits, "nervix_alloc", source))?;
        if ptr < 0 {
            return Err(Report::new(WasmGuestCallError::ErrorCode {
                export: "nervix_alloc",
                code: ptr,
            }));
        }
        self.guest_buffer_capacity = self.guest_buffer_capacity.max(
            usize::try_from(size)
                .verified("this size was converted from a supported host buffer length"),
        );
        Ok(ptr)
    }

    async fn write_to_guest_buffer(
        &mut self,
        bytes: &[u8],
    ) -> StackResult<(i32, i32), WasmGuestCallError> {
        let ptr = self.allocate_guest_buffer(bytes.len()).await?;
        let size = i32::try_from(bytes.len())
            .map_err(|_| Report::new(WasmGuestCallError::InvalidSize(-1)))?;
        self.memory
            .write(
                &mut self.store,
                usize::try_from(ptr)
                    .verified("the guest allocator returned a non-negative pointer"),
                bytes,
            )
            .map_err(|source| Report::new(WasmGuestCallError::MemoryWrite(source)))?;
        Ok((ptr, size))
    }

    async fn read_guest_buffer(&mut self, size: i32) -> StackResult<Vec<u8>, WasmGuestCallError> {
        let size = usize::try_from(size)
            .map_err(|_| Report::new(WasmGuestCallError::InvalidSize(size)))?;
        if size > self.max_guest_buffer_bytes {
            return Err(Report::new(WasmGuestCallError::GuestBufferTooLarge {
                size,
                limit: self.max_guest_buffer_bytes,
            }));
        }
        let ptr = self
            .buffer_ptr
            .call_async(&mut self.store, ())
            .await
            .map_err(|source| WasmGuestCallError::call(self.limits, "nervix_buffer_ptr", source))?;
        let ptr = usize::try_from(ptr)
            .map_err(|_| Report::new(WasmGuestCallError::InvalidOffset(ptr)))?;
        let mut out = vec![0; size];
        self.memory
            .read(&mut self.store, ptr, &mut out)
            .map_err(|source| Report::new(WasmGuestCallError::MemoryRead(source)))?;
        Ok(out)
    }

    async fn read_pending_emit(&mut self) -> StackResult<Vec<WasmEnvelope>, WasmGuestCallError> {
        let mut batches = Vec::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            let size = self
                .read_emit
                .call_async(&mut self.store, ())
                .await
                .map_err(|source| {
                    WasmGuestCallError::call(self.limits, "nervix_read_emit", source)
                })?;
            if size == 0 {
                break;
            }
            if size < 0 {
                if let Some(reason) = self.take_global_error().await? {
                    return Err(Report::new(WasmGuestCallError::GlobalError { reason }));
                }
                return Err(Report::new(WasmGuestCallError::ErrorCode {
                    export: "nervix_read_emit",
                    code: size,
                }));
            }
            let emitted = self.read_guest_buffer(size).await?;
            let batch = WasmEnvelope::decode_owned(emitted)
                .change_context(WasmGuestCallError::InvalidEmission)?;
            if let Some(sender) = self.store.data().emitted_batch_sender.as_ref() {
                sender
                    .send(batch.clone())
                    .means_peer_left("emitted batch observer");
            }
            batches.push(batch);
        }
        Ok(batches)
    }
}

fn define_host_functions(linker: &mut Linker<BranchStore>) -> StackResult<(), WasmProcessorError> {
    linker
        .func_wrap(
            ENV_MODULE,
            "nervix_domain_time_nanos",
            |caller: Caller<'_, BranchStore>| caller.data().now().unix_nanos(),
        )
        .map_err(|source| Report::new(WasmProcessorError::Link(source)))?;
    linker
        .func_wrap(
            ENV_MODULE,
            "nervix_timeout_after_nanos",
            |mut caller: Caller<'_, BranchStore>, delay_nanos: i64| {
                caller.data_mut().timeout_after(delay_nanos)
            },
        )
        .map_err(|source| Report::new(WasmProcessorError::Link(source)))?;
    linker
        .func_wrap(
            ENV_MODULE,
            "nervix_request_state_reset",
            |mut caller: Caller<'_, BranchStore>| caller.data_mut().request_state_reset(),
        )
        .map_err(|source| Report::new(WasmProcessorError::Link(source)))?;
    Ok(())
}

/// Wasmtime's error owns a complete engine diagnostic but does not implement the standard
/// error trait. This frame keeps that typed value and renders its chain at the report boundary.
#[derive(Debug, Error)]
#[error("{0:#}")]
struct WasmtimeFailure(wasmtime::Error);

fn typed_export<Params, Results>(
    store: &mut Store<BranchStore>,
    instance: &Instance,
    name: &'static str,
) -> StackResult<TypedFunc<Params, Results>, WasmGuestCallError>
where
    Params: wasmtime::WasmParams,
    Results: wasmtime::WasmResults,
{
    instance.get_typed_func(store, name).map_err(|cause| {
        Report::new(WasmtimeFailure(cause)).change_context(WasmGuestCallError::MissingExport(name))
    })
}

fn optional_typed_export<Params, Results>(
    store: &mut Store<BranchStore>,
    instance: &Instance,
    name: &'static str,
) -> StackResult<Option<TypedFunc<Params, Results>>, WasmGuestCallError>
where
    Params: wasmtime::WasmParams,
    Results: wasmtime::WasmResults,
{
    // A guest that does not define the export, and one that defines it with another signature,
    // are the same thing to the host: this optional capability is not available on this module.
    // The host has no second reading to give the error, and every caller answers both the same.
    match instance.get_typed_func(&mut *store, name) {
        Ok(export) => Ok(Some(export)),
        Err(_) => Ok(None),
    }
}

fn optional_global_error_exports(
    store: &mut Store<BranchStore>,
    instance: &Instance,
) -> StackResult<Option<WasmGlobalErrorExports>, WasmGuestCallError> {
    let ptr = optional_typed_export(store, instance, "nervix_global_error_ptr")?;
    let len = optional_typed_export(store, instance, "nervix_global_error_len")?;
    let clear = optional_typed_export(store, instance, "nervix_clear_global_error")?;
    match (ptr, len, clear) {
        (None, None, None) => Ok(None),
        (Some(ptr), Some(len), Some(clear)) => Ok(Some(WasmGlobalErrorExports { ptr, len, clear })),
        (ptr, len, _clear) => {
            let missing = if ptr.is_none() {
                "nervix_global_error_ptr"
            } else if len.is_none() {
                "nervix_global_error_len"
            } else {
                "nervix_clear_global_error"
            };
            Err(Report::new(WasmGuestCallError::MissingExport(missing)))
        }
    }
}

fn ensure_success(export: &'static str, code: i32) -> StackResult<(), WasmGuestCallError> {
    if code == SUCCESS {
        Ok(())
    } else {
        Err(Report::new(WasmGuestCallError::ErrorCode { export, code }))
    }
}

#[cfg(test)]
mod tests {
    /// A configured preemption period never becomes the lifetime of a detached engine after
    /// the final runtime handle ends. Keep one clone first to exercise the retained owner too.
    #[nervix_primitives::test]
    async fn dropping_the_final_runtime_wakes_a_long_period_epoch_driver() {
        let runtime = WasmRuntime::new(WasmRuntimeConfig {
            epoch_tick_interval: Duration::from_secs(60),
            ..WasmRuntimeConfig::default()
        })
        .expect("the long epoch interval configures");
        let engine = runtime.engine.weak();
        let retained = runtime.clone();
        drop(runtime);
        assert!(
            engine.upgrade().is_some(),
            "another runtime handle retains the engine"
        );
        drop(retained);
        nervix_primitives::time::timeout(Duration::from_secs(10), async {
            while engine.upgrade().is_some() {
                nervix_primitives::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("ending the owner promptly releases its epoch thread's engine");
    }

    use std::path::{Path, PathBuf};

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use nervix_models::{CreateSchema, FieldName, ParseAsType, SchemaField, SchemaName};
    use nervix_primitives::sync::{StdArc, atomic::AtomicU64};
    use nonzero_ext::nonzero;

    use super::*;

    /// The guest protocol schema of a test schema.
    fn guest_schema(schema: &nervix_models::CreateSchema) -> WasmProcessorSchema {
        WasmProcessorSchema::from(schema)
    }

    /// A `DURATION`, alone and as a nested list element, reaches the guest as the protocol's
    /// duration kind, and the type a guest declares back names the `DURATION` again.
    #[test]
    fn duration_schema_type_reaches_wasm_protocol_and_returns_as_duration() {
        let two = std::num::NonZeroU32::new(2).assured("two is not zero");
        for (schema_type, protocol_type) in [
            (ParseAsType::Duration, protocol::ProcessorType::Duration),
            (
                ParseAsType::Vec {
                    element: Box::new(ParseAsType::Array {
                        element: Box::new(ParseAsType::Duration),
                        len: two,
                    }),
                },
                protocol::ProcessorType::Vec {
                    element: Box::new(protocol::ProcessorType::Array {
                        element: Box::new(protocol::ProcessorType::Duration),
                        len: 2,
                    }),
                },
            ),
        ] {
            let ty = WasmProcessorType::from(&schema_type);
            assert_eq!(ty.to_protocol(), protocol_type);
            let declared = WasmProcessorType::from_protocol(protocol_type);
            assert_eq!(
                ParseAsType::try_from(&declared).assured("a duration names a Nervix type"),
                schema_type
            );
        }
    }

    #[test]
    fn bytes_schema_type_reaches_wasm_protocol_without_text_conversion() {
        let ty = WasmProcessorType::from(&ParseAsType::Vec {
            element: Box::new(ParseAsType::Bytes),
        });
        assert_eq!(
            ty.to_protocol(),
            protocol::ProcessorType::Vec {
                element: Box::new(protocol::ProcessorType::Bytes),
            }
        );
    }

    fn test_execution_context() -> WasmExecutionContext {
        WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234))
    }

    const TEST_WASM: &str = r#"
        (module
            (import "env" "nervix_domain_time_nanos" (func $now (result i64)))
            (import "env" "nervix_timeout_after_nanos" (func $timeout (param i64) (result i64)))
            (memory (export "memory") 1)
            (global $buffer_len (mut i32) (i32.const 0))
            (global $emit_len (mut i32) (i32.const 0))
            (global $read_ptr (mut i32) (i32.const 1024))
            (global $processed (mut i64) (i64.const 0))

            (func (export "nervix_buffer_ptr") (result i32)
                global.get $read_ptr)

            (func (export "nervix_alloc") (param $size i32) (result i32)
                local.get $size
                global.set $buffer_len
                i32.const 1024
                global.set $read_ptr
                i32.const 1024)

            (func (export "nervix_init") (param $ptr i32) (param $size i32) (result i32)
                i32.const 0)

            (func (export "nervix_current_domain_time_nanos") (result i64)
                call $now)

            (func (export "nervix_process_batch")
                (param $ptr i32)
                (param $size i32)
                (result i32)
                i64.const 5000000
                call $timeout
                drop
                local.get $ptr
                global.set $read_ptr
                global.get $processed
                i64.const 1
                i64.add
                global.set $processed
                local.get $size
                global.set $emit_len
                i32.const 0)

            (func (export "nervix_on_timeout") (param $handle i64) (result i32)
                i32.const 0)

            (func (export "nervix_flush") (result i32)
                i32.const 0)

            (func (export "nervix_read_emit") (result i32)
                global.get $emit_len
                global.set $buffer_len
                i32.const 0
                global.set $emit_len
                global.get $buffer_len)

            (func (export "nervix_dump_state") (result i32)
                i32.const 1024
                global.set $read_ptr
                i32.const 1024
                global.get $processed
                i64.store
                i32.const 8)

            (func (export "nervix_load_state") (param $ptr i32) (param $size i32) (result i32)
                local.get $size
                i32.const 8
                i32.ne
                if (result i32)
                    i32.const -8
                else
                    local.get $ptr
                    i64.load
                    global.set $processed
                    i32.const 0
                end)

            (func (export "nervix_reset_state") (result i32)
                i64.const 0
                global.set $processed
                i32.const 0
                global.set $emit_len
                i32.const 0)

            (func (export "nervix_describe_state") (result i32)
                i32.const 0)
        )
    "#;

    const CPU_BOUND_WASM: &str = r#"
        (module
            (memory (export "memory") 1)
            (global $emit_len (mut i32) (i32.const 0))
            (global $read_ptr (mut i32) (i32.const 1024))

            (func (export "nervix_buffer_ptr") (result i32)
                global.get $read_ptr)

            (func (export "nervix_alloc") (param $size i32) (result i32)
                i32.const 1024
                global.set $read_ptr
                i32.const 1024)

            (func (export "nervix_init") (param $ptr i32) (param $size i32) (result i32)
                i32.const 0)

            (func (export "nervix_current_domain_time_nanos") (result i64)
                i64.const 0)

            (func (export "nervix_process_batch")
                (param $ptr i32)
                (param $size i32)
                (result i32)
                (local $i i64)
                local.get $ptr
                global.set $read_ptr
                (loop $again
                    local.get $i
                    i64.const 1
                    i64.add
                    local.tee $i
                    i64.const 20000000
                    i64.lt_u
                    br_if $again)
                local.get $size
                global.set $emit_len
                i32.const 0)

            (func (export "nervix_on_timeout") (param $handle i64) (result i32)
                i32.const 0)

            (func (export "nervix_flush") (result i32)
                i32.const 0)

            (func (export "nervix_read_emit") (result i32)
                global.get $emit_len
                i32.const 0
                global.set $emit_len)

            (func (export "nervix_dump_state") (result i32)
                i32.const 0)

            (func (export "nervix_load_state") (param $ptr i32) (param $size i32) (result i32)
                i32.const 0)

            (func (export "nervix_reset_state") (result i32)
                i32.const 0)

            (func (export "nervix_describe_state") (result i32)
                i32.const 0)
        )
    "#;

    /// A guest whose one memory starts at two pages and grows by one page per batch.
    const MEMORY_GROWING_WASM: &str = r#"
        (module
            (memory (export "memory") 2)
            (global $read_ptr (mut i32) (i32.const 1024))
            (func (export "nervix_buffer_ptr") (result i32) global.get $read_ptr)
            (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
            (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
            (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
            (func (export "nervix_process_batch") (param i32 i32) (result i32)
                i32.const 1
                memory.grow
                drop
                i32.const 0)
            (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
            (func (export "nervix_flush") (result i32) i32.const 0)
            (func (export "nervix_read_emit") (result i32) i32.const 0)
            (func (export "nervix_dump_state") (result i32) i32.const 0)
            (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
            (func (export "nervix_reset_state") (result i32) i32.const 0)
            (func (export "nervix_describe_state") (result i32) i32.const 0)
        )
    "#;

    fn init() -> WasmBranchInit {
        WasmBranchInit {
            domain_name: "events".to_string(),
            domain_type: "PACED".to_string(),
            branch_key: Some(b"user=42".to_vec()),
            input_schema: guest_schema(&processor_schema("input_events")),
            output_schemas: vec![guest_schema(&processor_schema("output_events"))],
        }
    }

    fn processor_schema(name: &str) -> CreateSchema {
        CreateSchema {
            labels: Default::default(),
            name: SchemaName::parse(name).expect("schema name must be valid"),
            fields: vec![SchemaField {
                name: FieldName::parse("value").expect("field name must be valid"),
                ty: ParseAsType::I32,
                optional: false,
                sensitive: false,
            }],
        }
    }

    fn sample_arrow_ipc(values: &[i32]) -> Vec<u8> {
        let schema = StdArc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let array = StdArc::new(Int32Array::from(values.to_vec()));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).expect("batch must build");
        let mut output = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut output, &schema).expect("ipc writer must build");
            writer.write(&batch).expect("batch must encode");
            writer.finish().expect("ipc stream must finish");
        }
        output
    }

    fn sample_arrow_ipc_with_strings(values: &[i32], payloads: &[&str]) -> Vec<u8> {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(Int32Array::from(values.to_vec())),
                StdArc::new(StringArray::from(payloads.to_vec())),
            ],
        )
        .expect("batch must build");
        let mut output = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut output, &schema).expect("ipc writer must build");
            writer.write(&batch).expect("batch must encode");
            writer.finish().expect("ipc stream must finish");
        }
        output
    }

    fn string_passthrough_init() -> WasmBranchInit {
        let schema = CreateSchema {
            labels: Default::default(),
            name: SchemaName::parse("input_events").expect("schema name must be valid"),
            fields: vec![
                SchemaField {
                    name: FieldName::parse("value").expect("field name must be valid"),
                    ty: ParseAsType::I32,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: FieldName::parse("payload").expect("field name must be valid"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
            ],
        };
        let mut output_schema = guest_schema(&schema);
        output_schema.name = "output_events".to_string();
        WasmBranchInit {
            domain_name: "events".to_string(),
            domain_type: "PACED".to_string(),
            branch_key: Some(b"user=42".to_vec()),
            input_schema: guest_schema(&schema),
            output_schemas: vec![output_schema],
        }
    }

    fn shared_generated_init() -> WasmBranchInit {
        let input_schema = CreateSchema {
            labels: Default::default(),
            name: SchemaName::parse("input_events").expect("schema name must be valid"),
            fields: vec![SchemaField {
                name: FieldName::parse("value").expect("field name must be valid"),
                ty: ParseAsType::I32,
                optional: false,
                sensitive: false,
            }],
        };
        let enriched_schema = CreateSchema {
            labels: Default::default(),
            name: SchemaName::parse("enriched_events").expect("schema name must be valid"),
            fields: vec![
                input_schema.fields[0].clone(),
                SchemaField {
                    name: FieldName::parse("bucket").expect("field name must be valid"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
            ],
        };
        let audit_schema = CreateSchema {
            labels: Default::default(),
            name: SchemaName::parse("audit_events").expect("schema name must be valid"),
            fields: vec![
                input_schema.fields[0].clone(),
                SchemaField {
                    name: FieldName::parse("classification").expect("field name must be valid"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
            ],
        };
        WasmBranchInit {
            domain_name: "events".to_string(),
            domain_type: "PACED".to_string(),
            branch_key: Some(b"tenant=alpha".to_vec()),
            input_schema: guest_schema(&input_schema),
            output_schemas: vec![guest_schema(&enriched_schema), guest_schema(&audit_schema)],
        }
    }

    fn output_row(token: u64) -> WasmOutputRow {
        WasmOutputRow {
            tokens: vec![WasmAckToken(token)],
            source_token: Some(WasmAckToken(token)),
        }
    }

    fn token_set(token: u64) -> WasmAckTokenSet {
        WasmAckTokenSet {
            tokens: vec![WasmAckToken(token)],
        }
    }

    #[test]
    fn input_envelope_flatbuffer_round_trip_borrows_arrow_ipc() {
        let envelope = WasmEnvelope::input(
            vec![0, 1, 2, 255],
            WasmAckSidecar {
                rows: vec![output_row(7)],
                acked: Vec::new(),
                nacked: Vec::new(),
                message_errors: Vec::new(),
            },
        );

        let encoded = envelope.encode().expect("input envelope must encode");
        assert_eq!(
            WasmEnvelope::decode(&encoded).expect("input envelope must decode"),
            envelope
        );
        assert_eq!(&encoded[8..12], protocol::FILE_IDENTIFIER.as_bytes());
        let protocol::EnvelopeRef::Input(view) =
            WasmEnvelope::decode_borrowed(&encoded).expect("borrowed input must decode")
        else {
            panic!("expected borrowed input envelope");
        };
        assert_eq!(view.arrow_ipc_batch(), [0, 1, 2, 255]);
        let encoded_start = encoded.as_ptr().addr();
        let encoded_end = encoded_start + encoded.len();
        assert!((encoded_start..encoded_end).contains(&view.arrow_ipc_batch().as_ptr().addr()));
    }

    #[test]
    fn guest_buffer_allocator_serializes_directly_and_spills_without_changing_bytes() {
        let envelope = WasmEnvelope::input(
            vec![7; 512],
            WasmAckSidecar {
                rows: vec![WasmOutputRow {
                    tokens: vec![WasmAckToken(41)],
                    source_token: Some(WasmAckToken(41)),
                }],
                ..WasmAckSidecar::default()
            },
        );
        let expected = envelope.encode().expect("envelope must encode");
        let encoding = ProtocolEnvelopeEncoding::new(&envelope);

        let mut direct_memory = vec![0; expected.len() + 64];
        let mut direct_builder =
            FlatBufferBuilder::new_in(GuestBufferAllocator::Direct(&mut direct_memory));
        encoding.encode_in(&mut direct_builder);
        let (direct_allocator, direct_start) = direct_builder.collapse_in();
        assert!(matches!(direct_allocator, GuestBufferAllocator::Direct(_)));
        assert_eq!(&direct_allocator[direct_start..], expected);

        let mut undersized_memory = [0; 32];
        let mut spill_builder =
            FlatBufferBuilder::new_in(GuestBufferAllocator::Direct(&mut undersized_memory));
        encoding.encode_in(&mut spill_builder);
        let (spill_allocator, spill_start) = spill_builder.collapse_in();
        assert!(matches!(spill_allocator, GuestBufferAllocator::Spill(_)));
        assert_eq!(&spill_allocator[spill_start..], expected);
    }

    #[nervix_primitives::test]
    async fn underestimated_guest_capacity_grows_and_copies_finished_spill() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), TEST_WASM)
            .await
            .expect("module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect("branch must instantiate");
        branch.guest_buffer_capacity = 0;

        let envelope = WasmEnvelope::input_arrow_only(vec![9; 512]);
        let expected = envelope.encode().expect("envelope must encode");
        let encoding = ProtocolEnvelopeEncoding::new(&envelope);
        let (ptr, size) = branch
            .write_message_to_guest(&encoding, 1)
            .await
            .expect("spill must be copied into a grown guest buffer");

        assert_eq!(
            usize::try_from(size).verified("the returned buffer size is positive"),
            expected.len()
        );
        assert!(branch.guest_buffer_capacity >= expected.len());
        let mut actual = vec![0; expected.len()];
        branch
            .memory
            .read(
                &branch.store,
                usize::try_from(ptr).verified("the returned buffer pointer is positive"),
                &mut actual,
            )
            .expect("finished spill must be readable from guest memory");
        assert_eq!(actual, expected);
    }

    #[test]
    fn shared_generated_output_flatbuffer_round_trip_borrows_generated_ipc() {
        let generated_arrow_ipc_batch = vec![9, 8, 7];
        let envelope = WasmEnvelope::output(
            generated_arrow_ipc_batch.clone(),
            vec![
                WasmRoutedOutput::new(
                    "enriched_events",
                    vec![
                        WasmOutputColumnRef::input(0),
                        WasmOutputColumnRef::generated(0),
                        WasmOutputColumnRef::generated(0),
                    ],
                    WasmAckSidecar {
                        rows: vec![output_row(11)],
                        acked: vec![token_set(12)],
                        nacked: Vec::new(),
                        message_errors: Vec::new(),
                    },
                ),
                WasmRoutedOutput::new(
                    "audit_events",
                    vec![
                        WasmOutputColumnRef::input(0),
                        WasmOutputColumnRef::generated(0),
                    ],
                    WasmAckSidecar {
                        rows: vec![output_row(11)],
                        acked: Vec::new(),
                        nacked: Vec::new(),
                        message_errors: Vec::new(),
                    },
                ),
            ],
        );

        let encoded = envelope.encode().expect("output envelope must encode");
        assert_eq!(
            WasmEnvelope::decode(&encoded).expect("output envelope must decode"),
            envelope
        );
        let protocol::EnvelopeRef::Output(view) =
            WasmEnvelope::decode_borrowed(&encoded).expect("borrowed output must decode")
        else {
            panic!("expected borrowed output envelope");
        };
        assert_eq!(view.generated_arrow_ipc_batch(), generated_arrow_ipc_batch);
        let encoded_start = encoded.as_ptr().addr();
        let encoded_end = encoded_start + encoded.len();
        assert!(
            (encoded_start..encoded_end)
                .contains(&view.generated_arrow_ipc_batch().as_ptr().addr())
        );
        let WasmEnvelope::Output {
            generated_arrow_ipc_batch,
            ..
        } = WasmEnvelope::decode_owned(encoded).expect("owned output must decode")
        else {
            panic!("expected owned output envelope");
        };
        assert!((encoded_start..encoded_end).contains(&generated_arrow_ipc_batch.as_ptr().addr()));
    }

    #[test]
    fn input_only_output_envelope_uses_empty_generated_pool() {
        let envelope = WasmEnvelope::output(
            Vec::new(),
            vec![WasmRoutedOutput::new(
                "output_events",
                vec![WasmOutputColumnRef::input(0)],
                WasmAckSidecar::default(),
            )],
        );

        let encoded = envelope.encode().expect("output envelope must encode");
        assert_eq!(
            WasmEnvelope::decode(&encoded).expect("output envelope must decode"),
            envelope
        );
    }

    #[test]
    fn envelope_decode_rejects_non_flatbuffer_and_trailing_data() {
        assert!(WasmEnvelope::decode(&[0xa0]).is_err());

        let mut trailing = WasmEnvelope::input_arrow_only(Vec::new())
            .encode()
            .expect("input envelope must encode");
        trailing.push(0);
        assert!(WasmEnvelope::decode(&trailing).is_err());
    }

    #[test]
    fn wasm_schema_contract_converts_from_nervix_schema_model() {
        let source = CreateSchema {
            labels: Default::default(),
            name: SchemaName::parse("events").expect("schema name must be valid"),
            fields: vec![
                SchemaField {
                    name: FieldName::parse("value").expect("field name must be valid"),
                    ty: ParseAsType::I32,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: FieldName::parse("tags").expect("field name must be valid"),
                    ty: ParseAsType::Vec {
                        element: Box::new(ParseAsType::String),
                    },
                    optional: true,
                    sensitive: false,
                },
            ],
        };

        let converted = guest_schema(&source);

        assert_eq!(
            converted,
            WasmProcessorSchema {
                name: "events".to_string(),
                fields: vec![
                    WasmProcessorField {
                        name: "value".to_string(),
                        ty: WasmProcessorType::I32,
                        optional: false,
                    },
                    WasmProcessorField {
                        name: "tags".to_string(),
                        ty: WasmProcessorType::Vec {
                            element: Box::new(WasmProcessorType::String),
                        },
                        optional: true,
                    },
                ],
            }
        );
    }

    fn runtime() -> WasmRuntime {
        WasmRuntime::new(WasmRuntimeConfig::default()).expect("runtime must initialize")
    }

    /// The table bound every test guest's limits declare: a test guest has no table or a table of
    /// one slot.
    const GUEST_TABLE_ELEMENTS: u64 = 4_096;

    fn limits() -> WasmProcessorLimits {
        WasmProcessorLimits {
            max_fuel: nonzero!(1_000_000_000u64),
            max_memory_bytes: nonzero!(67_108_864u64),
            max_table_elements: GUEST_TABLE_ELEMENTS,
        }
    }

    /// The reason every lifecycle guest below can put on its global-error channel, 32 bytes long.
    const LIFECYCLE_REASON: &str = "guest refused the saved counters";

    /// A guest whose `nervix_init`, `nervix_dump_state`, and `nervix_load_state` run the given
    /// bodies. A body stores [`LIFECYCLE_REASON`] on the global-error channel by setting
    /// `$reason_len` to 32.
    fn lifecycle_wasm(init_body: &str, dump_state_body: &str, load_state_body: &str) -> String {
        format!(
            r#"
            (module
                (memory (export "memory") 1)
                (data (i32.const 2048) "{LIFECYCLE_REASON}")
                (global $reason_len (mut i32) (i32.const 0))
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) {init_body})
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_flush") (result i32) i32.const 0)
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) {dump_state_body})
                (func (export "nervix_load_state") (param i32 i32) (result i32) {load_state_body})
                (func (export "nervix_reset_state") (result i32) i32.const 0)
                (func (export "nervix_describe_state") (result i32) i32.const 0)
                (func (export "nervix_global_error_ptr") (result i32) i32.const 2048)
                (func (export "nervix_global_error_len") (result i32) global.get $reason_len)
                (func (export "nervix_clear_global_error") (result i32)
                    i32.const 0
                    global.set $reason_len
                    i32.const 0)
            )
            "#
        )
    }

    async fn failed_instantiation(
        wasm: impl AsRef<[u8]>,
        limits: WasmProcessorLimits,
        restored_state: Option<&[u8]>,
    ) -> Report<WasmGuestError> {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), wasm)
            .await
            .expect("module should compile");
        compiled
            .instantiate_branch(
                limits,
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                restored_state,
            )
            .await
            .expect_err("the branch instance must not come up")
    }

    fn call_failure(
        error: &Report<WasmGuestError>,
        operation: WasmGuestOperation,
    ) -> &WasmGuestCallError {
        assert_eq!(error.current_context().operation(), operation);
        error
            .downcast_ref::<WasmGuestCallError>()
            .expect("a failed guest operation keeps its typed call cause")
    }

    /// A guest that describes its state with `describe_body` after pointing its buffer at
    /// `message`, which a data segment holds at 8192, away from the buffer the host writes into.
    fn describing_wasm(describe_body: &str, message: &[u8]) -> String {
        let mut data = String::with_capacity(message.len() * 3);
        for byte in message {
            data.push_str(&format!("\\{byte:02x}"));
        }
        format!(
            r#"
            (module
                (memory (export "memory") 1)
                (data (i32.const 2048) "{LIFECYCLE_REASON}")
                (data (i32.const 8192) "{data}")
                (global $reason_len (mut i32) (i32.const 0))
                (global $read_ptr (mut i32) (i32.const 1024))
                (func (export "nervix_buffer_ptr") (result i32) global.get $read_ptr)
                (func (export "nervix_alloc") (param i32) (result i32)
                    i32.const 1024
                    global.set $read_ptr
                    i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_flush") (result i32) i32.const 0)
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
                (func (export "nervix_describe_state") (result i32)
                    i32.const 8192
                    global.set $read_ptr
                    {describe_body})
                (func (export "nervix_global_error_ptr") (result i32) i32.const 2048)
                (func (export "nervix_global_error_len") (result i32) global.get $reason_len)
                (func (export "nervix_clear_global_error") (result i32)
                    i32.const 0
                    global.set $reason_len
                    i32.const 0)
            )
            "#
        )
    }

    async fn described(
        wasm: impl AsRef<[u8]>,
    ) -> StackResult<WasmStateDescription, WasmGuestError> {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), wasm)
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect("the branch instance must come up");
        branch
            .describe_state_in_context(WasmExecutionContext::new(Timestamp::from_unix_nanos(0)))
            .await
    }

    #[nervix_primitives::test]
    async fn a_described_state_reaches_the_host_section_by_section_in_the_guests_order() {
        let message = protocol::StateDescription {
            sections: vec![
                protocol::StateSection {
                    schema: protocol::ProcessorSchema {
                        name: "progress".into(),
                        fields: vec![protocol::ProcessorField {
                            name: "processed_rows".into(),
                            ty: protocol::ProcessorType::U64,
                            optional: false,
                        }],
                    },
                    arrow_ipc_batch: vec![1, 2, 3],
                },
                protocol::StateSection {
                    schema: protocol::ProcessorSchema {
                        name: "windows".into(),
                        fields: vec![protocol::ProcessorField {
                            name: "opened".into(),
                            ty: protocol::ProcessorType::Vec {
                                element: Box::new(protocol::ProcessorType::Datetime),
                            },
                            optional: true,
                        }],
                    },
                    arrow_ipc_batch: Vec::new(),
                },
            ],
        }
        .encode();
        let wasm = describing_wasm(&format!("i32.const {}", message.len()), &message);

        let description = described(wasm).await.expect("a valid description decodes");

        assert_eq!(
            description,
            WasmStateDescription {
                sections: vec![
                    WasmStateSection {
                        schema: WasmProcessorSchema {
                            name: "progress".into(),
                            fields: vec![WasmProcessorField {
                                name: "processed_rows".into(),
                                ty: WasmProcessorType::U64,
                                optional: false,
                            }],
                        },
                        arrow_ipc_batch: vec![1, 2, 3],
                    },
                    WasmStateSection {
                        schema: WasmProcessorSchema {
                            name: "windows".into(),
                            fields: vec![WasmProcessorField {
                                name: "opened".into(),
                                ty: WasmProcessorType::Vec {
                                    element: Box::new(WasmProcessorType::Datetime),
                                },
                                optional: true,
                            }],
                        },
                        arrow_ipc_batch: Vec::new(),
                    },
                ],
            }
        );
    }

    #[nervix_primitives::test]
    async fn a_guest_that_describes_no_state_reaches_the_host_without_sections() {
        let wasm = describing_wasm("i32.const 0", &[]);

        let description = described(wasm).await.expect("describing no state succeeds");

        assert!(description.sections.is_empty());
    }

    #[nervix_primitives::test]
    async fn a_refused_description_fails_the_state_description_with_the_guest_reason() {
        let wasm = describing_wasm("i32.const 32 global.set $reason_len i32.const -6", &[]);

        let error = described(wasm)
            .await
            .expect_err("a refused description fails");

        assert_eq!(error.export(), Some("nervix_describe_state"));
        assert!(matches!(
            call_failure(&error, WasmGuestOperation::StateDescription),
            WasmGuestCallError::GlobalError { reason } if reason == LIFECYCLE_REASON
        ));
        assert_eq!(
            error.current_context().to_string(),
            "wasm guest state description failed"
        );
    }

    #[nervix_primitives::test]
    async fn a_negative_description_code_without_a_reason_names_the_code() {
        let wasm = describing_wasm("i32.const -6", &[]);

        let error = described(wasm).await.expect_err("a negative code fails");

        assert!(matches!(
            call_failure(&error, WasmGuestOperation::StateDescription),
            WasmGuestCallError::ErrorCode {
                export: "nervix_describe_state",
                code: -6
            }
        ));
    }

    #[nervix_primitives::test]
    async fn a_description_the_host_cannot_decode_fails_the_state_description() {
        let wasm = describing_wasm("i32.const 16", &[0xff; 16]);

        let error = described(wasm).await.expect_err("undecodable bytes fail");

        assert_eq!(error.export(), Some("nervix_describe_state"));
        assert!(matches!(
            call_failure(&error, WasmGuestOperation::StateDescription),
            WasmGuestCallError::InvalidStateDescription
        ));
    }

    #[nervix_primitives::test]
    async fn a_module_without_the_state_description_export_fails_instantiation_naming_it() {
        let export = r#"(func (export "nervix_describe_state") (result i32) i32.const 0)"#;
        let complete = lifecycle_wasm("i32.const 0", "i32.const 0", "i32.const 0");
        assert!(complete.contains(export));
        let wasm = complete.replace(export, "");

        let error = failed_instantiation(wasm, limits(), None).await;

        assert_eq!(error.export(), Some("nervix_describe_state"));
        assert!(matches!(
            call_failure(&error, WasmGuestOperation::Instantiation),
            WasmGuestCallError::MissingExport("nervix_describe_state")
        ));
    }

    #[nervix_primitives::test]
    async fn an_application_state_rejection_carries_the_guest_reason() {
        let wasm = lifecycle_wasm(
            "i32.const 0",
            "i32.const 0",
            "i32.const 32 global.set $reason_len i32.const -8",
        );

        let error = failed_instantiation(wasm, limits(), Some(b"saved state")).await;

        let failure = error.current_context();
        assert_eq!(
            failure.saved_state_rejection(),
            Some(SavedStateRejection::ApplicationState)
        );
        assert_eq!(failure.operation(), WasmGuestOperation::StateRestore);
        assert_eq!(error.export(), Some("nervix_load_state"));
        assert!(
            matches!(
                failure,
                WasmGuestError::ApplicationStateRejected { reason: Some(reason) }
                    if reason == LIFECYCLE_REASON
            ),
            "unexpected restore failure: {failure:?}"
        );
    }

    #[nervix_primitives::test]
    async fn a_snapshot_envelope_rejection_is_its_own_verdict() {
        let wasm = lifecycle_wasm("i32.const 0", "i32.const 0", "i32.const -7");

        let error = failed_instantiation(wasm, limits(), Some(b"saved state")).await;

        assert_eq!(
            error.current_context().saved_state_rejection(),
            Some(SavedStateRejection::SnapshotEnvelope)
        );
        assert!(matches!(
            error.current_context(),
            WasmGuestError::SnapshotEnvelopeRejected { reason: None }
        ));
    }

    #[nervix_primitives::test]
    async fn a_negative_restore_code_that_is_not_a_verdict_is_a_failed_restore() {
        let wasm = lifecycle_wasm("i32.const 0", "i32.const 0", "i32.const -1");

        let error = failed_instantiation(wasm, limits(), Some(b"saved state")).await;

        let failure = error.current_context();
        assert_eq!(failure.saved_state_rejection(), None);
        assert!(
            matches!(
                call_failure(&error, WasmGuestOperation::StateRestore),
                WasmGuestCallError::ErrorCode {
                    export: "nervix_load_state",
                    code: -1
                }
            ),
            "unexpected restore failure: {failure:?}"
        );
    }

    #[nervix_primitives::test]
    async fn a_trap_while_restoring_is_a_failed_restore() {
        let wasm = lifecycle_wasm("i32.const 0", "i32.const 0", "unreachable");

        let error = failed_instantiation(wasm, limits(), Some(b"saved state")).await;

        let failure = error.current_context();
        assert_eq!(failure.saved_state_rejection(), None);
        assert!(
            matches!(
                call_failure(&error, WasmGuestOperation::StateRestore),
                WasmGuestCallError::Trap {
                    export: "nervix_load_state",
                    trap: Trap::UnreachableCodeReached
                }
            ),
            "unexpected restore failure: {failure:?}"
        );
    }

    #[nervix_primitives::test]
    async fn exhausting_fuel_while_restoring_is_a_failed_restore() {
        let wasm = lifecycle_wasm(
            "i32.const 0",
            "i32.const 0",
            "(loop $spin br $spin) i32.const -8",
        );
        let limited = WasmProcessorLimits {
            max_fuel: nonzero!(100_000u64),
            max_memory_bytes: nonzero!(67_108_864u64),
            max_table_elements: GUEST_TABLE_ELEMENTS,
        };

        let error = failed_instantiation(wasm, limited, Some(b"saved state")).await;

        let failure = error.current_context();
        assert_eq!(failure.saved_state_rejection(), None);
        assert!(error.is_resource_limit_exceeded());
        assert_eq!(failure.operation(), WasmGuestOperation::StateRestore);
    }

    #[nervix_primitives::test]
    async fn a_guest_that_cannot_serialize_its_state_reports_why() {
        let wasm = lifecycle_wasm(
            "i32.const 0",
            "i32.const 32 global.set $reason_len i32.const -6",
            "i32.const 0",
        );
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), &wasm)
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(limits(), init(), test_execution_context(), None)
            .await
            .expect("branch must instantiate");

        let error = branch
            .save_state_in_context(test_execution_context())
            .await
            .expect_err("a guest that cannot serialize its state must not look saved");

        let failure = error.current_context();
        assert_eq!(error.export(), Some("nervix_dump_state"));
        assert!(
            matches!(
                call_failure(&error, WasmGuestOperation::StateSnapshot),
                WasmGuestCallError::GlobalError { reason } if reason == LIFECYCLE_REASON
            ),
            "unexpected snapshot failure: {failure:?}"
        );
    }

    #[nervix_primitives::test]
    async fn a_failed_initialization_is_reported_under_initialization_with_its_reason() {
        let wasm = lifecycle_wasm(
            "i32.const 32 global.set $reason_len i32.const -6",
            "i32.const 0",
            "i32.const 0",
        );

        let fresh = failed_instantiation(&wasm, limits(), None).await;
        let restoring = failed_instantiation(&wasm, limits(), Some(b"saved state")).await;

        for error in [fresh, restoring] {
            let failure = error.current_context();
            assert_eq!(failure.saved_state_rejection(), None);
            assert_eq!(error.export(), Some("nervix_init"));
            assert!(
                matches!(
                    call_failure(&error, WasmGuestOperation::Initialization),
                    WasmGuestCallError::GlobalError { reason } if reason == LIFECYCLE_REASON
                ),
                "unexpected initialization failure: {failure:?}"
            );
        }
    }

    fn return_code_wasm(process_code: i32, timeout_code: i32) -> String {
        return_code_wasm_with_flush(process_code, timeout_code, 0)
    }

    fn return_code_wasm_with_flush(
        process_code: i32,
        timeout_code: i32,
        flush_code: i32,
    ) -> String {
        format!(
            r#"
            (module
                (memory (export "memory") 1)
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32 i32) (result i32) i32.const {process_code})
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const {timeout_code})
                (func (export "nervix_flush") (result i32) i32.const {flush_code})
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
                (func (export "nervix_describe_state") (result i32) i32.const 0)
            )
            "#
        )
    }

    /// A guest whose `nervix_process_batch` and `nervix_dump_state` run the given bodies, with the
    /// `nervix_request_state_reset` host import available to both.
    fn state_reset_wasm(process_batch_body: &str, dump_state_body: &str) -> String {
        format!(
            r#"
            (module
                (import "env" "nervix_request_state_reset" (func $request_state_reset (result i32)))
                (memory (export "memory") 1)
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32 i32) (result i32) {process_batch_body})
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_flush") (result i32) i32.const 0)
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) {dump_state_body})
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
                (func (export "nervix_describe_state") (result i32) i32.const 0)
            )
            "#
        )
    }

    async fn state_reset_branch(wasm: String) -> WasmBranchInstance {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), wasm.as_bytes())
            .await
            .expect("module should compile");
        compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect("guest branch should instantiate")
    }

    /// A callback that asks is answered, and the host reads that request back exactly once.
    #[nervix_primitives::test]
    async fn a_callback_that_asks_for_a_new_state_lifetime_is_accepted_once() {
        let mut branch =
            state_reset_branch(state_reset_wasm("call $request_state_reset", "i32.const 0")).await;

        branch
            .process_batch_in_context(b"input", test_execution_context())
            .await
            .expect("an accepted request must not fail the callback");

        assert_eq!(
            branch.take_requested_state_reset(),
            WasmGuestStateLifetime::ResetRequested
        );
        assert_eq!(
            branch.take_requested_state_reset(),
            WasmGuestStateLifetime::Kept,
            "a request belongs to the callback that made it and is read back once"
        );
    }

    /// Asking repeatedly inside one callback is one request, because the host replaces the
    /// lifetime once.
    #[nervix_primitives::test]
    async fn repeated_requests_in_one_callback_are_one_request() {
        let mut branch = state_reset_branch(state_reset_wasm(
            "call $request_state_reset drop call $request_state_reset drop call              \
             $request_state_reset",
            "i32.const 0",
        ))
        .await;

        branch
            .process_batch_in_context(b"input", test_execution_context())
            .await
            .expect("repeated requests must not fail the callback");

        assert_eq!(
            branch.take_requested_state_reset(),
            WasmGuestStateLifetime::ResetRequested
        );
        assert_eq!(
            branch.take_requested_state_reset(),
            WasmGuestStateLifetime::Kept
        );
    }

    /// A callback that asks and then fails is still asking: its uncommitted effects are exactly
    /// what the reset discards.
    #[nervix_primitives::test]
    async fn a_failed_callback_that_asked_still_asks() {
        let mut branch = state_reset_branch(state_reset_wasm(
            "call $request_state_reset drop i32.const -6",
            "i32.const 0",
        ))
        .await;

        let error = branch
            .process_batch_in_context(b"input", test_execution_context())
            .await
            .expect_err("the failing callback must be reported");

        assert_eq!(error.export(), Some("nervix_process_batch"));
        assert_eq!(
            branch.take_requested_state_reset(),
            WasmGuestStateLifetime::ResetRequested
        );
    }

    /// Saving is not a callback: it owns no uncommitted effects, so the host refuses the request
    /// and schedules nothing.
    #[nervix_primitives::test]
    async fn a_request_outside_a_callback_is_refused() {
        let mut branch =
            state_reset_branch(state_reset_wasm("i32.const 0", "call $request_state_reset")).await;

        let error = branch
            .save_state_in_context(test_execution_context())
            .await
            .expect_err("the guest returns the refusal it received");

        match call_failure(&error, WasmGuestOperation::StateSnapshot) {
            WasmGuestCallError::ErrorCode { export, code } => {
                assert_eq!(*export, "nervix_dump_state");
                assert_eq!(*code, protocol::StateResetRequestAnswer::Refused.code());
            }
            other => panic!("expected a refused state reset request, got {other:?}"),
        }
        assert_eq!(
            branch.take_requested_state_reset(),
            WasmGuestStateLifetime::Kept
        );
    }

    #[nervix_primitives::test]
    async fn oversized_guest_input_is_rejected_before_calling_guest() {
        let runtime = WasmRuntime::new(WasmRuntimeConfig {
            engine: WasmEngineSettings::default(),
            optimize: false,
            epoch_tick_interval: Duration::from_millis(1),
            epoch_deadline_ticks: 1,
            max_guest_buffer_bytes: 512,
        })
        .expect("runtime must initialize");
        let compiled = runtime
            .compile_processor(&Executor::default(), return_code_wasm(0, 0).as_bytes())
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect("guest branch should instantiate");

        let error = branch
            .process_batch_in_context(&vec![0; 600], test_execution_context())
            .await
            .expect_err("oversized input should be rejected by host");

        match call_failure(&error, WasmGuestOperation::BatchProcessing) {
            WasmGuestCallError::GuestBufferTooLarge { size, limit } => {
                assert!(*size > 512);
                assert_eq!(*limit, 512);
            }
            other => panic!("expected guest buffer limit error, got {other:?}"),
        }
    }

    #[nervix_primitives::test]
    async fn process_batch_negative_guest_code_is_reported() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), return_code_wasm(-4, 0).as_bytes())
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect("guest branch should instantiate");

        let error = branch
            .process_batch_in_context(b"bad", test_execution_context())
            .await
            .expect_err("negative process code should be reported");

        match call_failure(&error, WasmGuestOperation::BatchProcessing) {
            WasmGuestCallError::ErrorCode { export, code } => {
                assert_eq!(*export, "nervix_process_batch");
                assert_eq!(*code, -4);
            }
            other => panic!("expected process guest error, got {other:?}"),
        }
    }

    #[nervix_primitives::test]
    async fn timeout_negative_guest_code_is_reported() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), return_code_wasm(0, -5).as_bytes())
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect("guest branch should instantiate");

        let error = branch
            .on_timeout(WasmTimeoutHandle(42), test_execution_context())
            .await
            .expect_err("negative timeout code should be reported");

        match call_failure(&error, WasmGuestOperation::TimeoutCallback) {
            WasmGuestCallError::ErrorCode { export, code } => {
                assert_eq!(*export, "nervix_on_timeout");
                assert_eq!(*code, -5);
            }
            other => panic!("expected timeout guest error, got {other:?}"),
        }
    }

    #[nervix_primitives::test]
    async fn flush_negative_guest_code_is_reported() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(
                &Executor::default(),
                return_code_wasm_with_flush(0, 0, -6).as_bytes(),
            )
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect("guest branch should instantiate");

        let error = branch
            .flush(test_execution_context())
            .await
            .expect_err("a guest that refuses to quiesce must be reported, not ignored");

        match call_failure(&error, WasmGuestOperation::QuiesceFlush) {
            WasmGuestCallError::ErrorCode { export, code } => {
                assert_eq!(*export, "nervix_flush");
                assert_eq!(*code, -6);
            }
            other => panic!("expected flush guest error, got {other:?}"),
        }
    }

    #[nervix_primitives::test]
    async fn a_guest_that_never_buffers_stays_usable_across_a_quiesce_flush() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), return_code_wasm(0, 0).as_bytes())
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect("guest branch should instantiate");

        assert!(
            branch
                .flush(test_execution_context())
                .await
                .expect("a guest that holds nothing must accept the quiesce flush")
                .is_empty()
        );
        branch
            .process_batch_in_context(b"after", test_execution_context())
            .await
            .expect("the branch must keep processing after a quiesce flush that emitted nothing");
    }

    #[nervix_primitives::test]
    async fn flush_trap_is_reported_with_its_trap_code() {
        let wasm = r#"
            (module
                (memory (export "memory") 1)
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_flush") (result i32) unreachable)
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
                (func (export "nervix_describe_state") (result i32) i32.const 0)
            )
        "#;
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), wasm.as_bytes())
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect("guest branch should instantiate");

        let error = branch
            .flush(test_execution_context())
            .await
            .expect_err("a trapping quiesce flush must surface as a trap");

        match call_failure(&error, WasmGuestOperation::QuiesceFlush) {
            WasmGuestCallError::Trap { export, trap } => {
                assert_eq!(*export, "nervix_flush");
                assert_eq!(*trap, Trap::UnreachableCodeReached);
            }
            other => panic!("expected flush trap, got {other:?}"),
        }
    }

    #[nervix_primitives::test]
    async fn a_global_error_raised_while_flushing_is_reported() {
        let wasm = r#"
            (module
                (memory (export "memory") 1)
                (data (i32.const 2048) "flush refused")
                (global $err_len (mut i32) (i32.const 0))
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_flush") (result i32)
                    i32.const 13
                    global.set $err_len
                    i32.const 0)
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
                (func (export "nervix_describe_state") (result i32) i32.const 0)
                (func (export "nervix_global_error_ptr") (result i32) i32.const 2048)
                (func (export "nervix_global_error_len") (result i32) global.get $err_len)
                (func (export "nervix_clear_global_error") (result i32)
                    i32.const 0
                    global.set $err_len
                    i32.const 0)
            )
        "#;
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), wasm.as_bytes())
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect("guest branch should instantiate");

        let error = branch.flush(test_execution_context()).await.expect_err(
            "a guest that reports a global error while quiescing must not look drained",
        );

        match call_failure(&error, WasmGuestOperation::QuiesceFlush) {
            WasmGuestCallError::GlobalError { reason } => assert_eq!(reason, "flush refused"),
            other => panic!("expected flush global error, got {other:?}"),
        }
    }

    #[nervix_primitives::test]
    async fn flushing_a_guest_that_holds_nothing_yields_no_output() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(&rust_guest_path()))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");

        assert!(
            branch
                .flush(test_execution_context())
                .await
                .expect("quiescing a guest that never received input must succeed")
                .is_empty(),
            "a branch gated before its first batch has nothing to release"
        );
    }

    #[nervix_primitives::test]
    async fn a_guest_without_the_flush_export_cannot_instantiate() {
        let wasm = r#"
            (module
                (memory (export "memory") 1)
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
                (func (export "nervix_describe_state") (result i32) i32.const 0)
            )
        "#;
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), wasm.as_bytes())
            .await
            .expect("module should compile");
        let error = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect_err("a guest that cannot be quiesced must be rejected at instantiation");

        match call_failure(&error, WasmGuestOperation::Instantiation) {
            WasmGuestCallError::MissingExport(export) => assert_eq!(*export, "nervix_flush"),
            other => panic!("expected missing flush export, got {other:?}"),
        }
    }

    #[nervix_primitives::test]
    async fn missing_required_export_is_reported() {
        let wasm = r#"
            (module
                (memory (export "memory") 1)
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_flush") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
                (func (export "nervix_describe_state") (result i32) i32.const 0)
            )
        "#;
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), wasm.as_bytes())
            .await
            .expect("module should compile");
        let error = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(0)),
                None,
            )
            .await
            .expect_err("missing export should reject instantiation");

        match call_failure(&error, WasmGuestOperation::Instantiation) {
            WasmGuestCallError::MissingExport(export) => {
                assert_eq!(*export, "nervix_read_emit");
            }
            other => panic!("expected missing export error, got {other:?}"),
        }
    }

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crate directory must have crates parent")
            .parent()
            .expect("crates directory must have repo parent")
            .to_path_buf()
    }

    fn rust_guest_path() -> PathBuf {
        repo_root().join(
            "examples/wasm-processors/rust-guest/target/wasm32-unknown-unknown/release/\
             nervix_wasm_processor_rust_guest.wasm",
        )
    }

    fn go_guest_path() -> PathBuf {
        repo_root().join("examples/wasm-processors/go-guest/nervix_wasm_processor_go_guest.wasm")
    }

    fn read_guest(path: &Path) -> Vec<u8> {
        std::fs::read(path).unwrap_or_else(|error| {
            panic!(
                "failed to read guest wasm '{}': {error}; run `just wasm-processor-guests` first",
                path.display()
            )
        })
    }

    async fn guest_emits_flatbuffer_input_reference_output(path: &Path) {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(path))
            .await
            .expect("guest module must compile");
        let mut processor_exports = Vec::new();
        for export in compiled.instance_pre.module().exports() {
            if export.name().starts_with("nervix_") {
                processor_exports.push(export.name());
            }
        }
        processor_exports.sort_unstable();
        assert_eq!(
            processor_exports,
            [
                "nervix_alloc",
                "nervix_buffer_ptr",
                "nervix_clear_global_error",
                "nervix_current_domain_time_nanos",
                "nervix_describe_state",
                "nervix_dump_state",
                "nervix_flush",
                "nervix_global_error_len",
                "nervix_global_error_ptr",
                "nervix_init",
                "nervix_load_state",
                "nervix_on_timeout",
                "nervix_process_batch",
                "nervix_read_emit",
                "nervix_reset_state",
            ],
            "{} must export the Nervix guest functions",
            path.display()
        );
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");
        let input = WasmEnvelope::input(
            sample_arrow_ipc(&[1, 2, 3, 4]),
            WasmAckSidecar {
                rows: vec![
                    output_row(10),
                    output_row(20),
                    output_row(30),
                    output_row(40),
                ],
                acked: Vec::new(),
                nacked: Vec::new(),
                message_errors: Vec::new(),
            },
        );

        assert_eq!(
            branch
                .process_envelope(&input, test_execution_context())
                .await
                .expect("first input must process"),
            Vec::<WasmEnvelope>::new()
        );
        let timeout = branch
            .take_timeout_requests()
            .pop()
            .expect("guest should request a timeout");
        let output = branch
            .on_timeout(timeout.handle, test_execution_context())
            .await
            .expect("timeout must emit output");
        assert_eq!(output.len(), 1);
        let WasmEnvelope::Output {
            generated_arrow_ipc_batch,
            outputs,
        } = &output[0]
        else {
            panic!("guest must emit an output envelope");
        };
        assert!(generated_arrow_ipc_batch.is_empty());
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].output_relay, "output_events");
        assert_eq!(outputs[0].columns, &[WasmOutputColumnRef::input(0)]);
        assert_eq!(outputs[0].acks.rows, vec![output_row(20), output_row(40)]);
        assert_eq!(outputs[0].acks.acked, vec![token_set(10), token_set(30)]);
    }

    #[nervix_primitives::test]
    async fn rust_guest_interoperates_with_host_flatbuffer_format() {
        guest_emits_flatbuffer_input_reference_output(&rust_guest_path()).await;
    }

    #[nervix_primitives::test]
    async fn rust_guest_output_omits_unchanged_string_payloads() {
        const SENTINEL: &str = "UNCHANGED_PAYLOAD_SENTINEL";

        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(&rust_guest_path()))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                string_passthrough_init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");
        let input = WasmEnvelope::input(
            sample_arrow_ipc_with_strings(&[1, 2], &["dropped", SENTINEL]),
            WasmAckSidecar {
                rows: vec![output_row(10), output_row(20)],
                acked: Vec::new(),
                nacked: Vec::new(),
                message_errors: Vec::new(),
            },
        );

        assert!(
            branch
                .process_envelope(&input, test_execution_context())
                .await
                .expect("input must process")
                .is_empty()
        );
        let timeout = branch
            .take_timeout_requests()
            .pop()
            .expect("guest should request a timeout");
        let outputs = branch
            .on_timeout(timeout.handle, test_execution_context())
            .await
            .expect("timeout must emit output");
        let WasmEnvelope::Output {
            generated_arrow_ipc_batch,
            outputs: routed_outputs,
        } = &outputs[0]
        else {
            panic!("guest must emit an output envelope");
        };
        assert!(generated_arrow_ipc_batch.is_empty());
        assert_eq!(routed_outputs.len(), 1);
        assert_eq!(
            routed_outputs[0].columns,
            &[WasmOutputColumnRef::input(0), WasmOutputColumnRef::input(1),]
        );
        let encoded = outputs[0].encode().expect("output envelope must encode");
        assert!(
            !encoded
                .windows(SENTINEL.len())
                .any(|window| window == SENTINEL.as_bytes()),
            "unchanged string payload must not be serialized guest-to-host"
        );
    }

    #[nervix_primitives::test]
    async fn rust_guest_timeout_shares_one_generated_column_across_routes() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(&rust_guest_path()))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                shared_generated_init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");
        let input = WasmEnvelope::input(
            sample_arrow_ipc(&[1, 2]),
            WasmAckSidecar {
                rows: vec![output_row(10), output_row(20)],
                acked: Vec::new(),
                nacked: Vec::new(),
                message_errors: Vec::new(),
            },
        );

        assert!(
            branch
                .process_envelope(&input, test_execution_context())
                .await
                .expect("input must process")
                .is_empty()
        );
        let timeout = branch
            .take_timeout_requests()
            .pop()
            .expect("guest should request a timeout");
        let envelopes = branch
            .on_timeout(timeout.handle, test_execution_context())
            .await
            .expect("timeout must emit output");
        let WasmEnvelope::Output {
            generated_arrow_ipc_batch,
            outputs,
        } = &envelopes[0]
        else {
            panic!("guest must emit an output group");
        };

        assert!(!generated_arrow_ipc_batch.is_empty());
        assert_eq!(outputs.len(), 2);
        for output in outputs {
            assert_eq!(
                output.columns,
                &[
                    WasmOutputColumnRef::input(0),
                    WasmOutputColumnRef::generated(0),
                ]
            );
            assert_eq!(output.acks.rows, vec![output_row(20)]);
        }
        let reader =
            arrow_ipc::reader::StreamReader::try_new(generated_arrow_ipc_batch.as_ref(), None)
                .expect("generated Arrow IPC must decode");
        assert_eq!(reader.schema().fields().len(), 1);
        assert!(reader.schema().field(0).name().is_empty());
        assert_eq!(reader.collect::<Result<Vec<_>, _>>().unwrap().len(), 1);
    }

    #[nervix_primitives::test]
    async fn quiesce_flush_releases_the_batch_the_guest_still_buffers() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(&rust_guest_path()))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");
        let buffered = WasmEnvelope::input(
            sample_arrow_ipc(&[2]),
            WasmAckSidecar {
                rows: vec![output_row(10)],
                ..WasmAckSidecar::default()
            },
        );

        assert!(
            branch
                .process_envelope(&buffered, test_execution_context())
                .await
                .expect("input must process")
                .is_empty(),
            "the guest buffers this batch instead of emitting it"
        );

        let flushed = branch
            .flush(test_execution_context())
            .await
            .expect("quiesce flush must reach the guest");
        assert_eq!(
            flushed.len(),
            1,
            "a quiescing host must be able to make the guest release what it holds"
        );
        assert!(
            branch
                .flush(test_execution_context())
                .await
                .expect("a second quiesce flush must succeed")
                .is_empty(),
            "a drained guest must not re-emit on the next flush"
        );
    }

    /// The ACK sidecar of the only route in the only output group one guest callback emitted.
    fn only_route_acks(envelopes: &[WasmEnvelope]) -> &WasmAckSidecar {
        let [WasmEnvelope::Output { outputs, .. }] = envelopes else {
            panic!("expected exactly one output group, got {envelopes:?}");
        };
        let [route] = outputs.as_slice() else {
            panic!("expected exactly one routed output, got {outputs:?}");
        };
        &route.acks
    }

    /// Drives the sequence an `ENTITY_PAUSE` handoff performs: gate, flush, snapshot, restore. The
    /// flush is where the guest releases the batch it buffers, because its snapshot holds only the
    /// row ordinal: the replacement numbers its rows after the released batch and emits only the
    /// input it receives itself.
    #[nervix_primitives::test]
    async fn a_quiesce_flush_releases_buffered_input_before_the_handoff_snapshot() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(&rust_guest_path()))
            .await
            .expect("guest module must compile");
        let buffered = WasmEnvelope::input(
            sample_arrow_ipc(&[2]),
            WasmAckSidecar {
                rows: vec![output_row(10)],
                ..WasmAckSidecar::default()
            },
        );
        let follow_up = WasmEnvelope::input(
            sample_arrow_ipc(&[4]),
            WasmAckSidecar {
                rows: vec![output_row(20)],
                ..WasmAckSidecar::default()
            },
        );

        let mut owner = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");
        assert!(
            owner
                .process_envelope(&buffered, test_execution_context())
                .await
                .expect("input must process")
                .is_empty(),
            "the guest buffers this batch instead of emitting it"
        );
        let released = owner
            .flush(test_execution_context())
            .await
            .expect("quiesce flush must reach the guest");
        assert_eq!(
            only_route_acks(&released),
            &WasmAckSidecar {
                acked: vec![token_set(10)],
                ..WasmAckSidecar::default()
            },
            "the flush releases the buffered batch, whose only row has the odd ordinal 1"
        );
        let snapshot = owner
            .save_state(test_execution_context())
            .await
            .expect("a flushed guest must save its state");

        let mut replacement = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(2_345)),
                Some(&snapshot),
            )
            .await
            .expect("the replacement must restore the handoff snapshot");
        assert!(
            replacement
                .process_envelope(&follow_up, test_execution_context())
                .await
                .expect("the replacement must accept new input")
                .is_empty(),
            "the replacement buffers the batch it receives"
        );
        let resumed = replacement
            .flush(test_execution_context())
            .await
            .expect("quiesce flush must reach the replacement");
        assert_eq!(
            only_route_acks(&resumed),
            &WasmAckSidecar {
                rows: vec![output_row(20)],
                ..WasmAckSidecar::default()
            },
            "the replacement numbers its first row 2, after the released batch, and keeps it"
        );
    }

    #[nervix_primitives::test]
    async fn a_timeout_after_a_quiesce_flush_emits_nothing() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(&rust_guest_path()))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");
        branch
            .process_envelope(
                &WasmEnvelope::input(
                    sample_arrow_ipc(&[2]),
                    WasmAckSidecar {
                        rows: vec![output_row(10)],
                        ..WasmAckSidecar::default()
                    },
                ),
                test_execution_context(),
            )
            .await
            .expect("input must process");

        assert_eq!(
            branch
                .flush(test_execution_context())
                .await
                .expect("quiesce flush must reach the guest")
                .len(),
            1
        );
        let timeout = branch
            .timeout_requests()
            .first()
            .map(|request| request.handle)
            .expect("the guest requests a flush timeout while processing");
        assert!(
            branch
                .on_timeout(timeout, test_execution_context())
                .await
                .expect("the guest timeout must still run after a flush")
                .is_empty(),
            "a timeout that fires after the host already drained the guest must not duplicate the \
             flushed output"
        );
    }

    #[nervix_primitives::test]
    async fn the_go_guest_releases_its_buffered_batch_on_quiesce_flush() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(&go_guest_path()))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");

        assert!(
            branch
                .process_envelope(
                    &WasmEnvelope::input(
                        sample_arrow_ipc(&[2]),
                        WasmAckSidecar {
                            rows: vec![output_row(10)],
                            ..WasmAckSidecar::default()
                        },
                    ),
                    test_execution_context(),
                )
                .await
                .expect("input must process")
                .is_empty(),
            "the Go guest buffers this batch instead of emitting it"
        );
        assert_eq!(
            branch
                .flush(test_execution_context())
                .await
                .expect("quiesce flush must reach the Go guest")
                .len(),
            1,
            "every guest language implements the same quiesce contract"
        );
        assert!(
            branch
                .flush(test_execution_context())
                .await
                .expect("a second quiesce flush must succeed")
                .is_empty(),
            "a drained Go guest must not re-emit on the next flush"
        );
    }

    #[nervix_primitives::test]
    async fn repeated_read_emit_calls_return_multiple_output_groups() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(&rust_guest_path()))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");
        let first = WasmEnvelope::input(
            sample_arrow_ipc(&[2]),
            WasmAckSidecar {
                rows: vec![output_row(10)],
                ..WasmAckSidecar::default()
            },
        );
        let second = WasmEnvelope::input(
            sample_arrow_ipc(&[4]),
            WasmAckSidecar {
                rows: vec![output_row(20)],
                ..WasmAckSidecar::default()
            },
        );

        assert!(
            branch
                .process_envelope(&first, test_execution_context())
                .await
                .expect("first input must process")
                .is_empty()
        );
        let groups = branch
            .process_envelope(&second, test_execution_context())
            .await
            .expect("second input must flush both pending layouts");

        assert_eq!(groups.len(), 2);
        for group in groups {
            let WasmEnvelope::Output {
                generated_arrow_ipc_batch,
                outputs,
            } = group
            else {
                panic!("guest must emit output groups");
            };
            assert!(generated_arrow_ipc_batch.is_empty());
            assert_eq!(outputs.len(), 1);
        }
    }

    fn input_row_batch(value: i32, token: u64) -> WasmEnvelope {
        WasmEnvelope::input(
            sample_arrow_ipc(&[value]),
            WasmAckSidecar {
                rows: vec![output_row(token)],
                ..WasmAckSidecar::default()
            },
        )
    }

    /// A bundled guest saves its row ordinal and nothing it buffers. A replacement restored from a
    /// snapshot taken while the guest still held a batch numbers its next row after that batch and
    /// emits only the input it received itself.
    async fn guest_restores_its_row_ordinal_without_the_input_it_buffered(path: &Path) {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(path))
            .await
            .expect("guest module must compile");
        let mut owner = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");
        assert!(
            owner
                .process_envelope(&input_row_batch(1, 10), test_execution_context())
                .await
                .expect("input must process")
                .is_empty(),
            "the guest buffers this batch instead of emitting it"
        );
        let snapshot = owner
            .save_state(test_execution_context())
            .await
            .expect("a guest that buffers input must save its state");

        let mut replacement = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(5_678)),
                Some(&snapshot),
            )
            .await
            .expect("the replacement must restore the snapshot");
        assert!(
            replacement
                .process_envelope(&input_row_batch(2, 20), test_execution_context())
                .await
                .expect("the replacement must accept new input")
                .is_empty(),
            "the replacement buffers the batch it receives"
        );
        let released = replacement
            .flush(test_execution_context())
            .await
            .expect("quiesce flush must reach the replacement");
        assert_eq!(
            only_route_acks(&released),
            &WasmAckSidecar {
                rows: vec![output_row(20)],
                ..WasmAckSidecar::default()
            },
            "the replacement numbers its first row 2 and keeps it"
        );
    }

    #[nervix_primitives::test]
    async fn the_rust_guest_restores_its_row_ordinal_without_the_input_it_buffered() {
        guest_restores_its_row_ordinal_without_the_input_it_buffered(&rust_guest_path()).await;
    }

    #[nervix_primitives::test]
    async fn the_go_guest_restores_its_row_ordinal_without_the_input_it_buffered() {
        guest_restores_its_row_ordinal_without_the_input_it_buffered(&go_guest_path()).await;
    }

    /// The sentinel `-400` leaves a bundled guest unable to serialize its state, so the save reports
    /// the guest's reason instead of returning a snapshot.
    async fn guest_that_cannot_serialize_its_state_reports_why(path: &Path) {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(path))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");
        branch
            .process_envelope(&input_row_batch(-400, 10), test_execution_context())
            .await
            .expect("the guest accepts the batch that leaves it unable to serialize its state");

        let failure = branch
            .save_state(test_execution_context())
            .await
            .expect_err("a guest that cannot serialize its state must not look saved");

        assert_eq!(failure.export(), Some("nervix_dump_state"));
        assert!(
            matches!(
                call_failure(&failure, WasmGuestOperation::StateSnapshot),
                WasmGuestCallError::GlobalError { reason }
                    if reason == "guest cannot serialize its state for value -400"
            ),
            "unexpected snapshot failure: {failure:?}"
        );
    }

    #[nervix_primitives::test]
    async fn the_rust_guest_reports_why_it_cannot_serialize_its_state() {
        guest_that_cannot_serialize_its_state_reports_why(&rust_guest_path()).await;
    }

    #[nervix_primitives::test]
    async fn the_go_guest_reports_why_it_cannot_serialize_its_state() {
        guest_that_cannot_serialize_its_state_reports_why(&go_guest_path()).await;
    }

    /// A callback that fails latches the SDK guest into error state, which refuses every later
    /// callback of that instance. The error state belongs to the instance: its snapshot still holds
    /// the application state, and a replacement restored from it processes input again.
    #[nervix_primitives::test]
    async fn a_rust_guest_restored_after_its_error_state_processes_input() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(&rust_guest_path()))
            .await
            .expect("guest module must compile");
        let mut owner = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");
        owner
            .process_envelope(&input_row_batch(1, 10), test_execution_context())
            .await
            .expect("input must process");
        let latched = owner
            .process_envelope(&input_row_batch(-300, 20), test_execution_context())
            .await
            .expect_err("the sentinel latches the guest into error state");
        assert!(matches!(
            call_failure(&latched, WasmGuestOperation::BatchProcessing),
            WasmGuestCallError::GlobalError { reason }
                if reason == "guest error state for value -300"
        ));
        let snapshot = owner
            .save_state(test_execution_context())
            .await
            .expect("a guest in error state still saves its application state");

        let mut replacement = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(5_678)),
                Some(&snapshot),
            )
            .await
            .expect("the replacement must restore the snapshot");
        assert!(
            replacement
                .process_envelope(&input_row_batch(2, 30), test_execution_context())
                .await
                .expect("the replacement must process input")
                .is_empty(),
            "the replacement buffers the batch it receives"
        );
        let released = replacement
            .flush(test_execution_context())
            .await
            .expect("quiesce flush must reach the replacement");
        assert_eq!(
            only_route_acks(&released),
            &WasmAckSidecar {
                rows: vec![output_row(30)],
                ..WasmAckSidecar::default()
            },
            "the replacement numbers its first row 2 and keeps it"
        );
    }

    /// A snapshot names the branch configuration it was taken under, and a bundled guest refuses to
    /// restore it into an instance initialized with another one.
    async fn guest_rejects_a_snapshot_of_another_branch_configuration(path: &Path) {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), read_guest(path))
            .await
            .expect("guest module must compile");
        let mut owner = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1_234)),
                None,
            )
            .await
            .expect("guest branch must instantiate");
        owner
            .process_envelope(&input_row_batch(1, 10), test_execution_context())
            .await
            .expect("input must process");
        let snapshot = owner
            .save_state(test_execution_context())
            .await
            .expect("guest state must save");
        let mut other_branch = init();
        other_branch.branch_key = Some(b"user=7".to_vec());

        let error = compiled
            .instantiate_branch(
                limits(),
                other_branch,
                WasmExecutionContext::new(Timestamp::from_unix_nanos(5_678)),
                Some(&snapshot),
            )
            .await
            .expect_err("a snapshot of another branch configuration must not restore");

        let failure = error.current_context();
        assert!(
            matches!(
                failure,
                WasmGuestError::SnapshotEnvelopeRejected { reason: Some(reason) }
                    if reason == "saved snapshot was taken under a different branch configuration"
            ),
            "unexpected restore failure: {failure:?}"
        );
    }

    #[nervix_primitives::test]
    async fn the_rust_guest_rejects_a_snapshot_of_another_branch_configuration() {
        guest_rejects_a_snapshot_of_another_branch_configuration(&rust_guest_path()).await;
    }

    #[nervix_primitives::test]
    async fn the_go_guest_rejects_a_snapshot_of_another_branch_configuration() {
        guest_rejects_a_snapshot_of_another_branch_configuration(&go_guest_path()).await;
    }

    #[nervix_primitives::test]
    async fn the_rust_guest_rejects_bytes_that_are_not_a_snapshot_envelope() {
        let guest = read_guest(&rust_guest_path());

        let error = failed_instantiation(guest, limits(), Some(b"not a guest snapshot")).await;

        let failure = error.current_context();
        assert_eq!(
            failure.saved_state_rejection(),
            Some(SavedStateRejection::SnapshotEnvelope),
            "unexpected restore failure: {failure:?}"
        );
        assert!(
            matches!(
                failure,
                WasmGuestError::SnapshotEnvelopeRejected { reason: Some(_) }
            ),
            "the SDK explains why it cannot decode the envelope: {failure:?}"
        );
    }

    /// A bundled guest saves its row ordinal in eight bytes, which the Rust guest follows with
    /// a total wait when its input has one, and refuses any other application state with its
    /// own `reason`.
    async fn guest_rejects_application_state_that_is_not_a_row_ordinal(
        path: &Path,
        expected_reason: &str,
    ) {
        let guest = read_guest(path);
        let snapshot = protocol::GuestSnapshot {
            init_metadata: init().to_protocol().encode(),
            application_state: vec![1, 2, 3],
        }
        .encode();

        let error = failed_instantiation(guest, limits(), Some(&snapshot)).await;

        let failure = error.current_context();
        assert_eq!(
            failure.saved_state_rejection(),
            Some(SavedStateRejection::ApplicationState),
            "unexpected restore failure: {failure:?}"
        );
        assert!(
            matches!(
                failure,
                WasmGuestError::ApplicationStateRejected { reason: Some(reason) }
                    if reason == expected_reason
            ),
            "the guest's own restore error is the reason: {failure:?}"
        );
    }

    #[nervix_primitives::test]
    async fn the_rust_guest_rejects_application_state_its_processor_cannot_restore() {
        guest_rejects_application_state_that_is_not_a_row_ordinal(
            &rust_guest_path(),
            "saved state must be an 8-byte row ordinal, optionally followed by an 8-byte total \
             wait",
        )
        .await;
    }

    #[nervix_primitives::test]
    async fn the_go_guest_rejects_application_state_it_cannot_restore() {
        guest_rejects_application_state_that_is_not_a_row_ordinal(
            &go_guest_path(),
            "saved row ordinal must be exactly 8 bytes",
        )
        .await;
    }

    #[nervix_primitives::test]
    async fn the_go_guest_rejects_bytes_that_are_not_a_snapshot_envelope() {
        let guest = read_guest(&go_guest_path());

        let error = failed_instantiation(guest, limits(), Some(b"not a guest snapshot")).await;

        assert!(
            matches!(
                error.current_context(),
                WasmGuestError::SnapshotEnvelopeRejected { reason: Some(reason) }
                    if reason == "saved state is not a guest snapshot envelope"
            ),
            "unexpected restore failure: {:?}",
            error.current_context()
        );
    }

    #[nervix_primitives::test]
    async fn go_guest_interoperates_with_host_flatbuffer_format() {
        guest_emits_flatbuffer_input_reference_output(&go_guest_path()).await;
    }

    #[nervix_primitives::test]
    async fn branch_instances_reuse_compiled_module_with_isolated_stores() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), TEST_WASM)
            .await
            .expect("module must compile");
        let left_context = WasmExecutionContext::new(Timestamp::from_unix_nanos(100));
        let right_context = WasmExecutionContext::new(Timestamp::from_unix_nanos(200));

        let mut left = compiled
            .instantiate_branch(limits(), init(), left_context, None)
            .await
            .expect("left branch must instantiate");
        let mut right = compiled
            .instantiate_branch(limits(), init(), right_context, None)
            .await
            .expect("right branch must instantiate");

        assert_eq!(
            left.current_domain_time_in_context(left_context)
                .await
                .expect("clock must work"),
            Timestamp::from_unix_nanos(100)
        );
        assert_eq!(
            right
                .current_domain_time_in_context(right_context)
                .await
                .expect("clock must work"),
            Timestamp::from_unix_nanos(200)
        );

        let left_out = left
            .process_batch_in_context(b"left batch", left_context)
            .await
            .expect("left batch must process");
        let right_out = right
            .process_batch_in_context(b"right batch", right_context)
            .await
            .expect("right batch must process");

        assert_eq!(
            left_out,
            vec![WasmEnvelope::input_arrow_only(b"left batch".to_vec())]
        );
        assert_eq!(
            right_out,
            vec![WasmEnvelope::input_arrow_only(b"right batch".to_vec())]
        );
        assert_eq!(
            left.timeout_requests()[0].requested_at,
            Timestamp::from_unix_nanos(100)
        );
        assert_eq!(
            right.timeout_requests()[0].requested_at,
            Timestamp::from_unix_nanos(200)
        );

        // Coordinated reset drops the selected live instance. Its replacement starts with a
        // fresh store; the sibling keeps its own armed request throughout that transition.
        drop(left);
        let fresh_context = WasmExecutionContext::new(Timestamp::from_unix_nanos(2_000_100));
        let mut fresh = compiled
            .instantiate_branch(limits(), init(), fresh_context, None)
            .await
            .expect("the fresh branch must instantiate");
        assert!(fresh.timeout_requests().is_empty());
        fresh
            .process_batch_in_context(b"fresh batch", fresh_context)
            .await
            .expect("the fresh branch accepts its first batch");

        let prior_deadline = Timestamp::from_unix_nanos(5_000_100);
        assert!(fresh.take_due_timeout_requests(prior_deadline).is_empty());
        let sibling_due = right.take_due_timeout_requests(Timestamp::from_unix_nanos(5_000_200));
        assert_eq!(sibling_due.len(), 1);
        assert_eq!(sibling_due[0].requested_at, right_context.now());
        assert_eq!(fresh.timeout_requests().len(), 1);
        let fresh_due = fresh.take_due_timeout_requests(Timestamp::from_unix_nanos(7_000_100));
        assert_eq!(fresh_due.len(), 1);
        assert_eq!(fresh_due[0].requested_at, fresh_context.now());
        assert!(right.timeout_requests().is_empty());
    }

    #[nervix_primitives::test]
    async fn every_guest_invocation_uses_its_explicit_context() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), TEST_WASM)
            .await
            .expect("module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(100)),
                None,
            )
            .await
            .expect("branch must instantiate");

        let observed = branch
            .current_domain_time_in_context(WasmExecutionContext::new(Timestamp::from_unix_nanos(
                500,
            )))
            .await
            .expect("explicit clock read must work");
        branch
            .process_batch_in_context(
                b"context batch",
                WasmExecutionContext::new(Timestamp::from_unix_nanos(600)),
            )
            .await
            .expect("context batch must process");
        assert_eq!(observed, Timestamp::from_unix_nanos(500));
        assert_eq!(
            branch.timeout_requests()[0].requested_at,
            Timestamp::from_unix_nanos(600)
        );
    }

    #[nervix_primitives::test]
    async fn saved_state_loads_into_new_branch_store() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), TEST_WASM)
            .await
            .expect("module must compile");
        let first_context = WasmExecutionContext::new(Timestamp::from_unix_nanos(700));
        let mut branch = compiled
            .instantiate_branch(limits(), init(), first_context, None)
            .await
            .expect("branch must instantiate");
        branch
            .process_batch_in_context(b"one", first_context)
            .await
            .expect("batch must process");
        let state = branch
            .save_state_in_context(first_context)
            .await
            .expect("state must dump");

        let restored_context = WasmExecutionContext::new(Timestamp::from_unix_nanos(900));
        let mut restored = compiled
            .instantiate_branch(limits(), init(), restored_context, Some(&state))
            .await
            .expect("restored branch must instantiate");
        let restored_state = restored
            .save_state_in_context(restored_context)
            .await
            .expect("state must dump");

        assert_eq!(restored_state, 1_i64.to_le_bytes());
    }

    #[nervix_primitives::test]
    async fn resetting_a_guest_clears_its_saved_computation_state() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), TEST_WASM)
            .await
            .expect("module must compile");
        let context = test_execution_context();
        let mut branch = compiled
            .instantiate_branch(limits(), init(), context, None)
            .await
            .expect("branch must instantiate");
        branch
            .process_batch_in_context(b"one", context)
            .await
            .expect("batch must process");
        assert_eq!(
            branch
                .save_state_in_context(context)
                .await
                .expect("guest state must save"),
            1_i64.to_le_bytes()
        );

        branch
            .reset_state_in_context(context)
            .await
            .expect("guest reset must succeed");

        assert_eq!(
            branch
                .save_state_in_context(context)
                .await
                .expect("reset state must save"),
            0_i64.to_le_bytes()
        );
    }

    #[nervix_primitives::test]
    async fn emitted_batches_can_be_streamed_to_integration_owner() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), TEST_WASM)
            .await
            .expect("module must compile");
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let mut branch = compiled
            .instantiate_branch_with_emitter(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
                Some(sender),
            )
            .await
            .expect("branch must instantiate");

        let returned = branch
            .process_batch_in_context(b"stream me", test_execution_context())
            .await
            .expect("batch must process");
        assert_eq!(
            returned,
            vec![WasmEnvelope::input_arrow_only(b"stream me".to_vec())]
        );
        assert_eq!(
            receiver.recv().await.expect("emitted batch must arrive"),
            WasmEnvelope::input_arrow_only(b"stream me".to_vec())
        );
    }

    #[nervix_primitives::test(flavor = "current_thread")]
    async fn module_compilation_yields_to_async_executor() {
        let runtime = runtime();
        let mut wasm = String::from("(module");
        for index in 0..2_048 {
            wasm.push_str(&format!(
                "(func $compile_stress_{index} (param i64) (result i64) local.get 0 i64.const \
                 {index} i64.add)"
            ));
        }
        wasm.push(')');

        let ticks = Arc::new(AtomicU64::new(0));
        let task_ticks = Arc::clone(&ticks);
        let progress_task = nervix_primitives::task::spawn(async move {
            loop {
                nervix_primitives::task::consume_budget().await;
                task_ticks.fetch_add(1, Ordering::Relaxed);
                nervix_primitives::task::yield_now().await;
            }
        });
        nervix_primitives::task::yield_now().await;
        let ticks_before_compile = ticks.load(Ordering::Relaxed);

        runtime
            .compile_processor(&Executor::default(), wasm.as_bytes())
            .await
            .expect("stress module must compile");

        let ticks_after_compile = ticks.load(Ordering::Relaxed);
        progress_task.abort();
        assert!(
            ticks_after_compile > ticks_before_compile,
            "wasm compilation must not block the async executor"
        );
    }

    #[nervix_primitives::test]
    async fn max_fuel_traps_a_cpu_bound_guest() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), CPU_BOUND_WASM)
            .await
            .expect("module must compile");
        let configured_limits = WasmProcessorLimits {
            max_fuel: nonzero!(1_000u64),
            max_memory_bytes: nonzero!(67_108_864u64),
            max_table_elements: GUEST_TABLE_ELEMENTS,
        };
        let mut branch = compiled
            .instantiate_branch(
                configured_limits,
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
            )
            .await
            .expect("branch must instantiate");

        let error = branch
            .process_batch_in_context(b"cpu", test_execution_context())
            .await
            .expect_err("CPU-bound guest must exhaust its fuel budget");

        assert!(matches!(
            call_failure(&error, WasmGuestOperation::BatchProcessing),
            WasmGuestCallError::FuelExhausted {
                limit,
                export: Some("nervix_process_batch"),
            } if *limit == nonzero!(1_000u64)
        ));
    }

    #[nervix_primitives::test]
    async fn max_fuel_is_reset_for_each_logical_guest_operation() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), TEST_WASM)
            .await
            .expect("module must compile");
        let configured_limits = WasmProcessorLimits {
            max_fuel: nonzero!(10_000u64),
            max_memory_bytes: nonzero!(67_108_864u64),
            max_table_elements: GUEST_TABLE_ELEMENTS,
        };
        let mut branch = compiled
            .instantiate_branch(
                configured_limits,
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
            )
            .await
            .expect("branch must instantiate");

        branch
            .process_batch_in_context(b"first", test_execution_context())
            .await
            .expect("first operation must fit its fuel budget");
        let first_remaining = branch.store.get_fuel().expect("fuel must be enabled");
        branch
            .process_batch_in_context(b"second", test_execution_context())
            .await
            .expect("second operation must receive a fresh fuel budget");
        let second_remaining = branch.store.get_fuel().expect("fuel must be enabled");

        assert!(first_remaining < configured_limits.max_fuel.get());
        assert_eq!(second_remaining, first_remaining);
    }

    #[nervix_primitives::test]
    async fn max_memory_traps_guest_linear_memory_growth() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), MEMORY_GROWING_WASM)
            .await
            .expect("module must compile");
        let configured_limits = WasmProcessorLimits {
            max_fuel: nonzero!(100_000u64),
            max_memory_bytes: nonzero!(131_072u64),
            max_table_elements: GUEST_TABLE_ELEMENTS,
        };
        let mut branch = compiled
            .instantiate_branch(
                configured_limits,
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
            )
            .await
            .expect("a two-page memory instantiates at its memory limit");

        let error = branch
            .process_batch_in_context(b"grow", test_execution_context())
            .await
            .expect_err("guest memory growth must exceed MAX MEMORY");

        assert!(
            matches!(
                call_failure(&error, WasmGuestOperation::BatchProcessing),
                WasmGuestCallError::MemoryLimitExceeded {
                    limit: 131_072,
                    allocated: 131_072,
                    growth: 65_536,
                    export: Some("nervix_process_batch"),
                }
            ),
            "unexpected growth memory error: {error:?}"
        );
    }

    #[nervix_primitives::test]
    async fn max_memory_rejects_guest_initial_linear_memory() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), MEMORY_GROWING_WASM)
            .await
            .expect("module must compile");
        let configured_limits = WasmProcessorLimits {
            max_fuel: nonzero!(100_000u64),
            max_memory_bytes: nonzero!(65_536u64),
            max_table_elements: GUEST_TABLE_ELEMENTS,
        };

        let error = compiled
            .instantiate_branch(
                configured_limits,
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
            )
            .await
            .expect_err("two initial pages must exceed MAX MEMORY");

        assert!(
            matches!(
                call_failure(&error, WasmGuestOperation::Instantiation),
                WasmGuestCallError::MemoryLimitExceeded {
                    limit: 65_536,
                    allocated: 0,
                    growth: 131_072,
                    export: None,
                }
            ),
            "unexpected instantiation memory error: {error:?}"
        );
    }

    #[nervix_primitives::test]
    async fn epoch_driver_yields_cpu_bound_guest_to_async_executor() {
        let runtime = WasmRuntime::new(WasmRuntimeConfig {
            engine: WasmEngineSettings::default(),
            optimize: true,
            epoch_tick_interval: Duration::from_micros(100),
            epoch_deadline_ticks: 1,
            max_guest_buffer_bytes: WasmRuntimeConfig::default().max_guest_buffer_bytes,
        })
        .expect("runtime must initialize");
        let compiled = runtime
            .compile_processor(&Executor::default(), CPU_BOUND_WASM)
            .await
            .expect("module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
            )
            .await
            .expect("branch must instantiate");

        let ticks = Arc::new(AtomicU64::new(0));
        let task_ticks = Arc::clone(&ticks);
        let progress_task = nervix_primitives::task::spawn(async move {
            loop {
                task_ticks.fetch_add(1, Ordering::Relaxed);
                nervix_primitives::task::yield_now().await;
            }
        });

        let returned = branch
            .process_batch_in_context(b"cpu", test_execution_context())
            .await
            .expect("batch must process");
        progress_task.abort();

        assert_eq!(
            returned,
            vec![WasmEnvelope::input_arrow_only(b"cpu".to_vec())]
        );
        assert!(
            ticks.load(Ordering::Relaxed) > 0,
            "epoch async yielding should allow another task to run during guest execution"
        );
    }

    /// A guest whose batch processing recurses `depth` times. Every frame keeps eight 64-bit
    /// values live across its call and sums them after it returns, so each level needs stack of
    /// its own and a deep enough call needs more than Wasmtime's 512 KiB default.
    fn recursing_wasm(depth: u32) -> String {
        format!(
            r#"
            (module
                (memory (export "memory") 1)
                (func $deep (param $n i32)
                    (param $a i64) (param $b i64) (param $c i64) (param $d i64)
                    (param $e i64) (param $f i64) (param $g i64) (param $h i64)
                    (result i64)
                    local.get $n
                    i32.eqz
                    if (result i64)
                        local.get $a
                    else
                        local.get $n
                        i32.const 1
                        i32.sub
                        local.get $b
                        local.get $c
                        local.get $d
                        local.get $e
                        local.get $f
                        local.get $g
                        local.get $h
                        local.get $a
                        call $deep
                        local.get $a
                        i64.add
                        local.get $b
                        i64.add
                        local.get $c
                        i64.add
                        local.get $d
                        i64.add
                        local.get $e
                        i64.add
                        local.get $f
                        i64.add
                        local.get $g
                        i64.add
                        local.get $h
                        i64.add
                    end)
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32 i32) (result i32)
                    i32.const {depth}
                    i64.const 1
                    i64.const 2
                    i64.const 3
                    i64.const 4
                    i64.const 5
                    i64.const 6
                    i64.const 7
                    i64.const 8
                    call $deep
                    drop
                    i32.const 0)
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_flush") (result i32) i32.const 0)
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
                (func (export "nervix_describe_state") (result i32) i32.const 0)
            )
            "#
        )
    }

    /// How deep [`recursing_wasm`] recurses below: past Wasmtime's 512 KiB default stack, and well
    /// inside a 4 MiB one.
    const DEEP_RECURSION: u32 = 12_000;

    async fn process_recursing_batch(
        runtime: &WasmRuntime,
    ) -> StackResult<Vec<WasmEnvelope>, WasmGuestError> {
        let compiled = runtime
            .compile_processor(&Executor::default(), recursing_wasm(DEEP_RECURSION))
            .await
            .expect("module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
            )
            .await
            .expect("branch must instantiate");
        branch
            .process_batch_in_context(b"deep", test_execution_context())
            .await
    }

    #[nervix_primitives::test]
    async fn the_default_stack_traps_a_guest_that_recurses_past_it() {
        let error = process_recursing_batch(&runtime())
            .await
            .expect_err("the recursion needs more than the default 512 KiB stack");

        assert!(
            matches!(
                call_failure(&error, WasmGuestOperation::BatchProcessing),
                WasmGuestCallError::Trap {
                    export: "nervix_process_batch",
                    trap: Trap::StackOverflow,
                }
            ),
            "unexpected deep recursion failure: {error:?}"
        );
    }

    /// Wasmtime runs every guest call on a separate stack and refuses a guest stack larger than
    /// it. A stack setting above Wasmtime's 2 MiB default for that separate stack still builds the
    /// engine and gives a guest call the whole stack the setting names.
    #[nervix_primitives::test]
    async fn a_stack_above_wasmtimes_default_call_stack_runs_a_guest_that_needs_it() {
        let engine = WasmEngineSettings::resolve(WasmEngineSettingOverrides {
            stack_bytes: Some(4 * 1024 * 1024),
            ..WasmEngineSettingOverrides::default()
        })
        .expect("a 4 MiB stack is a valid setting");
        let runtime = WasmRuntime::new(WasmRuntimeConfig {
            engine,
            ..WasmRuntimeConfig::default()
        })
        .expect("the engine builds with a 4 MiB stack");

        process_recursing_batch(&runtime)
            .await
            .expect("the guest recursion fits a 4 MiB stack");
    }

    /// A guest with one table of one slot whose batch processing grows that table by a million
    /// slots.
    const TABLE_GROWING_WASM: &str = r#"
        (module
            (memory (export "memory") 1)
            (table $slots 1 funcref)
            (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
            (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
            (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
            (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
            (func (export "nervix_process_batch") (param i32 i32) (result i32)
                ref.null func
                i32.const 1000000
                table.grow $slots
                drop
                i32.const 0)
            (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
            (func (export "nervix_flush") (result i32) i32.const 0)
            (func (export "nervix_read_emit") (result i32) i32.const 0)
            (func (export "nervix_dump_state") (result i32) i32.const 0)
            (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
            (func (export "nervix_reset_state") (result i32) i32.const 0)
            (func (export "nervix_describe_state") (result i32) i32.const 0)
        )
    "#;

    #[nervix_primitives::test]
    async fn growing_a_table_past_its_limit_fails_the_guest_call() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(&Executor::default(), TABLE_GROWING_WASM)
            .await
            .expect("module must compile");
        let mut branch = compiled
            .instantiate_branch(
                limits(),
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
            )
            .await
            .expect("a one-slot table fits the limit");

        let error = branch
            .process_batch_in_context(b"grow", test_execution_context())
            .await
            .expect_err("a million more table slots exceed MAX TABLE ELEMENTS");

        assert!(
            matches!(
                call_failure(&error, WasmGuestOperation::BatchProcessing),
                WasmGuestCallError::TableLimitExceeded {
                    limit: GUEST_TABLE_ELEMENTS,
                    allocated: 1,
                    growth: 1_000_000,
                    export: Some("nervix_process_batch"),
                }
            ),
            "unexpected table growth error: {error:?}"
        );
        assert!(error.is_resource_limit_exceeded());
    }

    /// A refused growth leaves what the guest already holds counted exactly once: a growth that
    /// still fits succeeds after it.
    #[nervix_primitives::test]
    async fn a_table_growth_that_fits_the_limit_succeeds_up_to_the_limit() {
        let wasm = TABLE_GROWING_WASM.replace("i32.const 1000000", "i32.const 3");
        let compiled = runtime()
            .compile_processor(&Executor::default(), wasm)
            .await
            .expect("module must compile");
        let mut branch = compiled
            .instantiate_branch(
                WasmProcessorLimits {
                    max_table_elements: 7,
                    ..limits()
                },
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
            )
            .await
            .expect("a one-slot table fits seven elements");

        for _ in 0..2 {
            branch
                .process_batch_in_context(b"grow", test_execution_context())
                .await
                .expect("three more slots fit while the total stays at or below seven");
        }
        let error = branch
            .process_batch_in_context(b"grow", test_execution_context())
            .await
            .expect_err("ten slots exceed seven");

        assert!(
            matches!(
                call_failure(&error, WasmGuestOperation::BatchProcessing),
                WasmGuestCallError::TableLimitExceeded {
                    limit: 7,
                    allocated: 7,
                    growth: 3,
                    export: Some("nervix_process_batch"),
                }
            ),
            "unexpected table growth error: {error:?}"
        );
    }

    #[nervix_primitives::test]
    async fn max_table_elements_rejects_a_guest_initial_table() {
        let compiled = runtime()
            .compile_processor(&Executor::default(), TABLE_GROWING_WASM)
            .await
            .expect("module must compile");

        let error = compiled
            .instantiate_branch(
                WasmProcessorLimits {
                    max_table_elements: 0,
                    ..limits()
                },
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
            )
            .await
            .expect_err("a one-slot initial table exceeds zero elements");

        assert!(
            matches!(
                call_failure(&error, WasmGuestOperation::Instantiation),
                WasmGuestCallError::TableLimitExceeded {
                    limit: 0,
                    allocated: 0,
                    growth: 1,
                    export: None,
                }
            ),
            "unexpected instantiation table error: {error:?}"
        );
    }

    /// A guest with one 64-bit table of one slot whose batch processing grows that table by three
    /// slots and then asks for a growth whose new size overflows. Wasmtime fails the overflowing
    /// growth without asking the store's limiter first.
    const TABLE64_OVERFLOWING_WASM: &str = r#"
        (module
            (memory (export "memory") 1)
            (table $slots i64 1 funcref)
            (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
            (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
            (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
            (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
            (func (export "nervix_process_batch") (param i32 i32) (result i32)
                ref.null func
                i64.const 3
                table.grow $slots
                drop
                ref.null func
                i64.const -1
                table.grow $slots
                drop
                i32.const 0)
            (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
            (func (export "nervix_flush") (result i32) i32.const 0)
            (func (export "nervix_read_emit") (result i32) i32.const 0)
            (func (export "nervix_dump_state") (result i32) i32.const 0)
            (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
            (func (export "nervix_reset_state") (result i32) i32.const 0)
            (func (export "nervix_describe_state") (result i32) i32.const 0)
        )
    "#;

    /// A growth whose size overflows fails in the guest, and the slots the guest already holds
    /// stay counted: repeating it between admitted growths never lets the table pass its limit.
    #[nervix_primitives::test]
    async fn an_overflowing_table_growth_leaves_the_held_slots_counted() {
        let compiled = runtime()
            .compile_processor(&Executor::default(), TABLE64_OVERFLOWING_WASM)
            .await
            .expect("module must compile");
        let mut branch = compiled
            .instantiate_branch(
                WasmProcessorLimits {
                    max_table_elements: 7,
                    ..limits()
                },
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
            )
            .await
            .expect("a one-slot table fits seven elements");

        for _ in 0..2 {
            branch
                .process_batch_in_context(b"grow", test_execution_context())
                .await
                .expect("three more slots fit while the table stays at or below seven");
        }
        let error = branch
            .process_batch_in_context(b"grow", test_execution_context())
            .await
            .expect_err("ten slots exceed seven, whatever growths failed in between");

        assert!(
            matches!(
                call_failure(&error, WasmGuestOperation::BatchProcessing),
                WasmGuestCallError::TableLimitExceeded {
                    limit: 7,
                    allocated: 7,
                    growth: 3,
                    export: Some("nervix_process_batch"),
                }
            ),
            "unexpected table growth error: {error:?}"
        );
    }

    /// A growth past the table's own declared maximum of five slots fails in the guest and takes
    /// none of `MAX TABLE ELEMENTS`: the growth after it still has the whole remaining budget.
    #[nervix_primitives::test]
    async fn a_growth_past_the_declared_table_maximum_takes_none_of_the_limit() {
        let wasm = TABLE_GROWING_WASM
            .replace("(table $slots 1 funcref)", "(table $slots 1 5 funcref)")
            .replace(
                "i32.const 1000000",
                "i32.const 8 table.grow $slots drop ref.null func i32.const 4",
            );
        let compiled = runtime()
            .compile_processor(&Executor::default(), wasm)
            .await
            .expect("module must compile");
        let mut branch = compiled
            .instantiate_branch(
                WasmProcessorLimits {
                    max_table_elements: 9,
                    ..limits()
                },
                init(),
                WasmExecutionContext::new(Timestamp::from_unix_nanos(1)),
                None,
            )
            .await
            .expect("a one-slot table fits nine elements");

        branch
            .process_batch_in_context(b"grow", test_execution_context())
            .await
            .expect("the declared maximum refuses eight slots, and four more fit both bounds");
        let error = branch
            .process_batch_in_context(b"grow", test_execution_context())
            .await
            .expect_err("eight more slots past the five held exceed nine");

        assert!(
            matches!(
                call_failure(&error, WasmGuestOperation::BatchProcessing),
                WasmGuestCallError::TableLimitExceeded {
                    limit: 9,
                    allocated: 5,
                    growth: 8,
                    export: Some("nervix_process_batch"),
                }
            ),
            "unexpected table growth error: {error:?}"
        );
    }

    /// A guest that implements every export the host requires and defines `declarations` beside
    /// its exported memory.
    fn guest_defining(declarations: &str) -> String {
        guest_with_memory(r#"(memory (export "memory") 1)"#, declarations)
    }

    /// A guest that implements every export the host requires, exports the linear memory `memory`
    /// declares, and defines `declarations` beside it.
    fn guest_with_memory(memory: &str, declarations: &str) -> String {
        format!(
            r#"
            (module
                {memory}
                {declarations}
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_flush") (result i32) i32.const 0)
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
                (func (export "nervix_describe_state") (result i32) i32.const 0)
            )
            "#
        )
    }

    #[nervix_primitives::test]
    async fn a_module_with_two_memories_is_refused_when_it_compiles() {
        let Err(refusal) = runtime()
            .compile_processor(&Executor::default(), guest_defining("(memory 1)"))
            .await
        else {
            panic!("a guest defines at most one linear memory");
        };

        assert!(
            matches!(
                refusal.current_context(),
                WasmProcessorError::TooManyMemories {
                    defined: 2,
                    limit: GUEST_MEMORIES,
                }
            ),
            "unexpected refusal: {refusal:?}"
        );
        assert_eq!(
            refusal.current_context().to_string(),
            "wasm module defines 2 linear memories, but a guest defines at most 1"
        );
    }

    #[nervix_primitives::test]
    async fn a_module_with_two_tables_is_refused_when_it_compiles() {
        let Err(refusal) = runtime()
            .compile_processor(
                &Executor::default(),
                guest_defining("(table 1 funcref) (table 1 funcref)"),
            )
            .await
        else {
            panic!("a guest defines at most one table");
        };

        assert!(
            matches!(
                refusal.current_context(),
                WasmProcessorError::TooManyTables {
                    defined: 2,
                    limit: GUEST_TABLES,
                }
            ),
            "unexpected refusal: {refusal:?}"
        );
        assert_eq!(
            refusal.current_context().to_string(),
            "wasm module defines 2 tables, but a guest defines at most 1"
        );
    }

    /// The proposals outside the guest contract a refused compilation named, or a panic naming the
    /// refusal it got instead.
    fn refused_proposals(refusal: &Report<WasmProcessorError>) -> Vec<RefusedWasmProposal> {
        let WasmProcessorError::RefusedProposals { proposals } = refusal.current_context() else {
            panic!("the refusal must name the proposals the module needs: {refusal:?}");
        };
        proposals.iter().collect()
    }

    /// Compiles `wasm`, which the guest contract must refuse.
    async fn refusal_of(wasm: String) -> Report<WasmProcessorError> {
        let Err(refusal) = runtime()
            .compile_processor(&Executor::default(), wasm)
            .await
        else {
            panic!("the module must be refused when it compiles");
        };
        refusal
    }

    #[nervix_primitives::test]
    async fn a_module_with_a_shared_memory_is_refused_naming_the_threads_proposal() {
        let refusal = refusal_of(guest_with_memory(
            r#"(memory (export "memory") 1 1 shared)"#,
            "",
        ))
        .await;

        assert_eq!(refused_proposals(&refusal), [RefusedWasmProposal::Threads]);
        assert_eq!(
            refusal.current_context().to_string(),
            "wasm module uses the threads proposal, which a guest may not use"
        );
        assert!(
            refusal
                .frames()
                .filter_map(|frame| frame.downcast_ref::<WasmProcessorError>())
                .any(|context| matches!(context, WasmProcessorError::Compile(_))),
            "the compile failure the proposal caused stays beneath the refusal: {refusal:?}"
        );
    }

    #[nervix_primitives::test]
    async fn a_module_with_one_byte_pages_is_refused_naming_the_custom_page_sizes_proposal() {
        let refusal = refusal_of(guest_with_memory(
            r#"(memory (export "memory") 65536 (pagesize 1))"#,
            "",
        ))
        .await;

        assert_eq!(
            refused_proposals(&refusal),
            [RefusedWasmProposal::CustomPageSizes]
        );
        assert_eq!(
            refusal.current_context().to_string(),
            "wasm module uses the custom page sizes proposal, which a guest may not use"
        );
    }

    #[nervix_primitives::test]
    async fn a_module_needing_two_refused_proposals_is_refused_naming_both() {
        let refusal = refusal_of(guest_with_memory(
            r#"(memory (export "memory") 65536 65536 shared (pagesize 1))"#,
            "",
        ))
        .await;

        assert_eq!(
            refused_proposals(&refusal),
            [
                RefusedWasmProposal::Threads,
                RefusedWasmProposal::CustomPageSizes
            ]
        );
        assert_eq!(
            refusal.current_context().to_string(),
            "wasm module uses the threads proposal and the custom page sizes proposal, which a \
             guest may not use"
        );
    }

    #[nervix_primitives::test]
    async fn every_refused_proposal_a_module_can_use_is_named_when_it_compiles() {
        let refused = [
            (
                "atomic instructions",
                guest_defining("(func (result i32) (i32.atomic.load (i32.const 0)))"),
                RefusedWasmProposal::Threads,
            ),
            (
                "throw",
                guest_defining("(tag $failed) (func (throw $failed))"),
                RefusedWasmProposal::ExceptionHandling,
            ),
            (
                "an externref parameter",
                guest_defining("(func (param externref))"),
                RefusedWasmProposal::GarbageCollectedReferences,
            ),
            (
                "a struct type",
                guest_defining("(type (struct (field i32)))"),
                RefusedWasmProposal::GarbageCollectedReferences,
            ),
            (
                "a component",
                "(component)".to_string(),
                RefusedWasmProposal::ComponentModel,
            ),
        ];

        for (construct, wasm, proposal) in refused {
            let refusal = refusal_of(wasm).await;

            assert_eq!(
                refused_proposals(&refusal),
                [proposal],
                "a module using {construct} needs {proposal}"
            );
        }
    }

    #[nervix_primitives::test]
    async fn a_module_invalid_whatever_the_engine_enables_names_no_proposal() {
        for wasm in [
            "not a module".to_string(),
            guest_defining("(func (result i32) (i64.const 0))"),
        ] {
            let refusal = refusal_of(wasm).await;

            assert!(
                matches!(refusal.current_context(), WasmProcessorError::Compile(_)),
                "a module no proposal makes valid is a compile failure: {refusal:?}"
            );
        }
    }

    #[nervix_primitives::test]
    async fn every_feature_of_the_guest_contract_compiles() {
        let features = [
            (
                "bulk memory operations",
                guest_defining("(func (memory.copy (i32.const 0) (i32.const 16) (i32.const 8)))"),
            ),
            (
                "reference types with funcref",
                guest_defining(
                    "(table 1 funcref) (elem declare func $target) (func $target) (func \
                     (table.set (i32.const 0) (ref.func $target)))",
                ),
            ),
            (
                "sign-extension operators",
                guest_defining("(func (param i32) (result i32) (i32.extend8_s (local.get 0)))"),
            ),
            (
                "non-trapping float-to-integer conversions",
                guest_defining(
                    "(func (param f32) (result i32) (i32.trunc_sat_f32_s (local.get 0)))",
                ),
            ),
            (
                "multiple results",
                guest_defining("(func (result i32 i64) (i32.const 1) (i64.const 2))"),
            ),
            (
                "fixed-width SIMD",
                guest_defining("(func (result v128) (v128.const i32x4 1 2 3 4))"),
            ),
            (
                "mutable globals",
                guest_defining(r#"(global (export "counter") (mut i32) (i32.const 0))"#),
            ),
            (
                "tail calls",
                guest_defining(
                    "(func $target (result i32) (i32.const 1)) (func (result i32) (return_call \
                     $target))",
                ),
            ),
            (
                "extended constant expressions",
                guest_defining("(global i32 (i32.add (i32.const 1) (i32.const 2)))"),
            ),
            (
                "typed function references",
                guest_defining(
                    "(type $answer (func (result i32))) (func $target (type $answer) (i32.const \
                     1)) (elem declare func $target) (func (result i32) (call_ref $answer \
                     (ref.func $target)))",
                ),
            ),
            (
                "the type system of the GC proposal",
                guest_defining(
                    "(rec (type $base (sub (func))) (type $derived (sub $base (func)))) (func \
                     (type $derived))",
                ),
            ),
            (
                "relaxed SIMD",
                guest_defining(
                    "(func (param v128) (result v128) (i32x4.relaxed_trunc_f32x4_s (local.get 0)))",
                ),
            ),
            (
                "64-bit memories",
                guest_with_memory(r#"(memory (export "memory") i64 1)"#, ""),
            ),
            (
                "wide arithmetic",
                guest_defining(
                    "(func (param i64 i64 i64 i64) (result i64 i64) (i64.add128 (local.get 0) \
                     (local.get 1) (local.get 2) (local.get 3)))",
                ),
            ),
        ];

        for (feature, wasm) in features {
            if let Err(refusal) = runtime()
                .compile_processor(&Executor::default(), wasm)
                .await
            {
                panic!("a guest may use {feature}: {refusal:?}");
            }
        }
    }

    /// A guest whose memory of one-byte pages starts empty. `grow_past_failures` repeats, the
    /// given number of times, a growth of 4 KiB the limiter is asked about and a growth to the end
    /// of the 32-bit address space, which a memory of one-byte pages cannot reach, so Wasmtime
    /// fails it without asking the limiter. It returns the memory's size in bytes.
    const ONE_BYTE_PAGES_GROWING_WASM: &str = r#"
        (module
            (memory (export "memory") 0 (pagesize 1))
            (func (export "grow_past_failures") (param $cycles i32) (result i32)
                (local $cycle i32)
                (loop $next
                    (drop (memory.grow (i32.const 4096)))
                    (drop (memory.grow (i32.const -1)))
                    (local.set $cycle (i32.add (local.get $cycle) (i32.const 1)))
                    (br_if $next (i32.lt_u (local.get $cycle) (local.get $cycles))))
                memory.size))
    "#;

    #[test]
    fn max_memory_holds_through_growths_wasmtime_fails_without_asking() {
        // The guest contract refuses one-byte pages, and they are the only memories for which
        // Wasmtime 49 fails a growth without asking the limiter, so this engine enables them to
        // drive the limiter through that path.
        let mut config = Config::new();
        config.wasm_custom_page_sizes(true);
        let engine = Engine::new(&config).expect("an engine with one-byte pages must start");
        let module = Module::new(&engine, ONE_BYTE_PAGES_GROWING_WASM)
            .expect("the growing guest must compile with one-byte pages enabled");
        let mut store = Store::new(
            &engine,
            GuestResourceLimiter::new(WasmProcessorLimits {
                max_memory_bytes: nonzero!(65_536u64),
                ..limits()
            }),
        );
        store.limiter(|limiter| limiter);
        let instance =
            Instance::new(&mut store, &module, &[]).expect("an empty memory instantiates");
        let grow = instance
            .get_typed_func::<i32, i32>(&mut store, "grow_past_failures")
            .expect("the guest exports grow_past_failures");

        let error = grow
            .call(&mut store, 32)
            .expect_err("32 growths of 4 KiB do not fit a MAX MEMORY of 64 KiB");

        let exceeded = error
            .downcast_ref::<GuestMemoryLimitExceeded>()
            .unwrap_or_else(|| panic!("the limiter must refuse the growth: {error:?}"));
        assert_eq!(
            (exceeded.allocated, exceeded.growth, exceeded.limit),
            (65_536, 4_096, 65_536)
        );
        let memory = instance
            .get_memory(&mut store, "memory")
            .expect("the guest exports its memory");
        assert_eq!(memory.data_size(&store), 65_536);
    }

    #[nervix_primitives::test]
    async fn a_module_with_one_memory_and_one_table_compiles() {
        runtime()
            .compile_processor(&Executor::default(), guest_defining("(table 1 funcref)"))
            .await
            .expect("one memory and one table are what a guest may define");
    }

    #[nervix_primitives::test]
    async fn a_module_compiles_on_the_bulk_workers() {
        let executor = Executor::default();

        runtime()
            .compile_processor(&executor, TEST_WASM)
            .await
            .expect("the bulk workers compile the module");

        let snapshot = executor.snapshot();
        assert_eq!(snapshot.bulk_cpu.admitted, 1);
        assert_eq!(snapshot.bulk_cpu.completed, 1);
        assert_eq!(snapshot.bulk_memory.reserved_bytes, 0);
    }
}
