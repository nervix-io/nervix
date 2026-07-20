use std::{
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use bytes::Bytes;
use nervix_models::{ParseAsType, Timestamp};
use nervix_wasm_protocol as protocol;
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::mpsc;
use triomphe::Arc;
use wasmtime::{
    Caller, Config, Engine, Instance, InstancePre, Linker, Memory, Module, OptLevel, Store,
    TypedFunc,
};

const ENV_MODULE: &str = "env";
const EXPORT_MEMORY: &str = "memory";
const SUCCESS: i32 = 0;
const ERR_INVALID_SIZE: i32 = -1;
const DEFAULT_EPOCH_TICK_INTERVAL: Duration = Duration::from_millis(1);
const DEFAULT_EPOCH_DEADLINE_TICKS: u64 = 1;
const DEFAULT_MAX_GUEST_BUFFER_BYTES: usize = 64 * 1024 * 1024;
pub const ABI_SERIALIZATION_NAME: &str = protocol::SERIALIZATION_NAME;

#[derive(Debug, Error)]
pub enum WasmProcessorError {
    #[error("failed to configure wasmtime: {0}")]
    Configure(#[source] wasmtime::Error),
    #[error("failed to compile wasm module: {0}")]
    Compile(#[source] wasmtime::Error),
    #[error("failed to join wasm compilation task: {0}")]
    CompileTask(#[source] tokio::task::JoinError),
    #[error("failed to link wasm module: {0}")]
    Link(#[source] wasmtime::Error),
    #[error("failed to instantiate wasm module: {0}")]
    Instantiate(#[source] wasmtime::Error),
    #[error("missing required export '{0}'")]
    MissingExport(&'static str),
    #[error("failed to call wasm export '{name}': {source}")]
    Call {
        name: &'static str,
        #[source]
        source: wasmtime::Error,
    },
    #[error("wasm export '{name}' returned error code {code}")]
    GuestError { name: &'static str, code: i32 },
    #[error("wasm guest reported global error: {0}")]
    GuestGlobalError(String),
    #[error("failed to write guest memory: {0}")]
    MemoryWrite(#[source] wasmtime::MemoryAccessError),
    #[error("failed to read guest memory: {0}")]
    MemoryRead(#[source] wasmtime::MemoryAccessError),
    #[error("guest returned invalid memory offset {0}")]
    InvalidOffset(i32),
    #[error("guest returned invalid byte size {0}")]
    InvalidSize(i32),
    #[error(transparent)]
    Protocol(#[from] WasmProtocolError),
    #[error("guest global error bytes are not valid UTF-8: {0}")]
    InvalidGuestGlobalError(#[source] std::string::FromUtf8Error),
    #[error("guest buffer size {size} exceeds configured limit {limit}")]
    GuestBufferTooLarge { size: usize, limit: usize },
}

#[derive(Debug, Error)]
pub enum WasmProtocolError {
    #[error(transparent)]
    FlatBuffers(#[from] protocol::ProtocolError),
}

#[derive(Debug, Clone)]
pub struct WasmRuntimeConfig {
    pub optimize: bool,
    pub epoch_tick_interval: Duration,
    pub epoch_deadline_ticks: u64,
    pub max_guest_buffer_bytes: usize,
}

impl Default for WasmRuntimeConfig {
    fn default() -> Self {
        Self {
            optimize: true,
            epoch_tick_interval: DEFAULT_EPOCH_TICK_INTERVAL,
            epoch_deadline_ticks: DEFAULT_EPOCH_DEADLINE_TICKS,
            max_guest_buffer_bytes: DEFAULT_MAX_GUEST_BUFFER_BYTES,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WasmRuntime {
    engine: Engine,
    stop: StdArc<AtomicBool>,
    epoch_deadline_ticks: u64,
    max_guest_buffer_bytes: usize,
}

impl WasmRuntime {
    pub fn new(config: WasmRuntimeConfig) -> Result<Self, WasmProcessorError> {
        let mut wasmtime_config = Config::new();
        wasmtime_config.epoch_interruption(true);
        // Runtime instances already compile independently on blocking workers. Letting each
        // Cranelift invocation fan out across the global CPU pool can starve Raft and Tokio
        // executor threads when several nodes or branch instances initialize together.
        wasmtime_config.parallel_compilation(false);
        wasmtime_config.cranelift_opt_level(if config.optimize {
            OptLevel::Speed
        } else {
            OptLevel::None
        });
        let engine = Engine::new(&wasmtime_config).map_err(WasmProcessorError::Configure)?;
        let stop = StdArc::new(AtomicBool::new(false));
        spawn_epoch_driver(
            engine.clone(),
            StdArc::clone(&stop),
            config.epoch_tick_interval,
        );
        Ok(Self {
            engine,
            stop,
            epoch_deadline_ticks: config.epoch_deadline_ticks,
            max_guest_buffer_bytes: config.max_guest_buffer_bytes,
        })
    }

    pub async fn compile_processor(
        &self,
        wasm: impl AsRef<[u8]>,
    ) -> Result<CompiledWasmProcessor, WasmProcessorError> {
        let engine = self.engine.clone();
        let wasm = wasm.as_ref().to_vec();
        let instance_pre = tokio::task::spawn_blocking(move || {
            let module = Module::new(&engine, wasm).map_err(WasmProcessorError::Compile)?;
            let mut linker = Linker::<BranchStore>::new(&engine);
            define_host_functions(&mut linker)?;
            linker
                .instantiate_pre(&module)
                .map_err(WasmProcessorError::Link)
        })
        .await
        .map_err(WasmProcessorError::CompileTask)??;
        Ok(CompiledWasmProcessor {
            engine: self.engine.clone(),
            instance_pre,
            epoch_deadline_ticks: self.epoch_deadline_ticks,
            max_guest_buffer_bytes: self.max_guest_buffer_bytes,
        })
    }
}

impl Drop for WasmRuntime {
    fn drop(&mut self) {
        if StdArc::strong_count(&self.stop) == 1 {
            self.stop.store(true, Ordering::Relaxed);
        }
    }
}

fn spawn_epoch_driver(engine: Engine, stop: StdArc<AtomicBool>, interval: Duration) {
    let weak_stop = StdArc::downgrade(&stop);
    thread::Builder::new()
        .name("nervix-wasm-epoch".to_string())
        .spawn(move || {
            loop {
                thread::sleep(interval);
                let Some(stop) = weak_stop.upgrade() else {
                    break;
                };
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                engine.increment_epoch();
            }
        })
        .expect("failed to spawn nervix wasm epoch driver");
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
    Datetime,
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
            ParseAsType::Datetime => Self::Datetime,
            ParseAsType::F32 => Self::F32,
            ParseAsType::F64 => Self::F64,
            ParseAsType::Array { element, len } => Self::Array {
                element: Box::new(Self::from(element.as_ref())),
                len: *len,
            },
            ParseAsType::Vec { element } => Self::Vec {
                element: Box::new(Self::from(element.as_ref())),
            },
        }
    }
}

impl WasmBranchInit {
    fn encode(&self) -> Vec<u8> {
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
        .encode()
    }
}

impl WasmProcessorSchema {
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
    fn to_protocol(&self) -> protocol::ProcessorField {
        protocol::ProcessorField {
            name: self.name.clone(),
            ty: self.ty.to_protocol(),
            optional: self.optional,
        }
    }
}

impl WasmProcessorType {
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
            Self::Datetime => protocol::ProcessorType::Datetime,
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

pub trait DomainClock: Send + Sync {
    fn now(&self) -> Timestamp;
}

#[derive(Debug, Clone)]
pub struct FixedDomainClock {
    now: Arc<Mutex<Timestamp>>,
}

impl FixedDomainClock {
    pub fn new(now: Timestamp) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    pub fn set(&self, now: Timestamp) {
        *self.now.lock() = now;
    }
}

impl DomainClock for FixedDomainClock {
    fn now(&self) -> Timestamp {
        *self.now.lock()
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
        if let Self::Input { .. } = self {
            true
        } else {
            false
        }
    }

    pub const fn is_uninitialized(&self) -> bool {
        if let Self::Uninitialized = self {
            true
        } else {
            false
        }
    }
}

impl WasmEnvelope {
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

    pub fn encode(&self) -> Result<Vec<u8>, WasmProtocolError> {
        Ok(self.to_protocol().encode())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WasmProtocolError> {
        Self::decode_owned(bytes.to_vec())
    }

    pub fn decode_borrowed(bytes: &[u8]) -> Result<protocol::EnvelopeRef<'_>, WasmProtocolError> {
        Ok(protocol::EnvelopeRef::decode(bytes)?)
    }

    fn to_protocol(&self) -> protocol::Envelope {
        match self {
            Self::Input {
                arrow_ipc_batch,
                acks,
            } => protocol::Envelope::Input {
                arrow_ipc_batch: arrow_ipc_batch.clone(),
                acks: acks.to_protocol(),
            },
            Self::Output {
                generated_arrow_ipc_batch,
                outputs,
            } => protocol::Envelope::Output {
                generated_arrow_ipc_batch: generated_arrow_ipc_batch.to_vec(),
                outputs: outputs.iter().map(WasmRoutedOutput::to_protocol).collect(),
            },
        }
    }

    fn decode_owned(bytes: Vec<u8>) -> Result<Self, WasmProtocolError> {
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
                    let offset = generated.as_ptr() as usize - bytes.as_ptr() as usize;
                    bytes.slice(offset..offset + generated.len())
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

struct BranchStore {
    clock: Box<dyn DomainClock>,
    timeout_requests: Vec<WasmTimeoutRequest>,
    next_timeout_handle: i64,
    emitted_batch_sender: Option<mpsc::UnboundedSender<WasmEnvelope>>,
}

impl BranchStore {
    fn new(
        clock: Box<dyn DomainClock>,
        emitted_batch_sender: Option<mpsc::UnboundedSender<WasmEnvelope>>,
    ) -> Self {
        Self {
            clock,
            timeout_requests: Vec::new(),
            next_timeout_handle: 1,
            emitted_batch_sender,
        }
    }

    fn now(&self) -> Timestamp {
        self.clock.now()
    }

    fn timeout_after(&mut self, delay_nanos: i64) -> i64 {
        if delay_nanos < 0 {
            return ERR_INVALID_SIZE.into();
        }
        let Ok(delay_nanos) = u64::try_from(delay_nanos) else {
            return ERR_INVALID_SIZE.into();
        };
        let handle = WasmTimeoutHandle(self.next_timeout_handle);
        self.next_timeout_handle = self.next_timeout_handle.saturating_add(1);
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
    pub async fn instantiate_branch(
        &self,
        init: WasmBranchInit,
        clock: Box<dyn DomainClock>,
        restored_state: Option<&[u8]>,
    ) -> Result<WasmBranchInstance, WasmProcessorError> {
        self.instantiate_branch_with_emitter(init, clock, restored_state, None)
            .await
    }

    pub async fn instantiate_branch_with_emitter(
        &self,
        init: WasmBranchInit,
        clock: Box<dyn DomainClock>,
        restored_state: Option<&[u8]>,
        emitted_batch_sender: Option<mpsc::UnboundedSender<WasmEnvelope>>,
    ) -> Result<WasmBranchInstance, WasmProcessorError> {
        let mut store = Store::new(&self.engine, BranchStore::new(clock, emitted_batch_sender));
        store.set_epoch_deadline(self.epoch_deadline_ticks);
        store.epoch_deadline_async_yield_and_update(self.epoch_deadline_ticks);
        let instance = self
            .instance_pre
            .instantiate_async(&mut store)
            .await
            .map_err(WasmProcessorError::Instantiate)?;
        let mut branch =
            WasmBranchInstance::load_exports(store, instance, self.max_guest_buffer_bytes)?;
        branch.init(init).await?;
        if let Some(restored_state) = restored_state {
            branch.load_state(restored_state).await?;
        }
        Ok(branch)
    }
}

pub struct WasmBranchInstance {
    store: Store<BranchStore>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    init: TypedFunc<(i32, i32), i32>,
    process_batch: TypedFunc<i32, i32>,
    on_timeout: TypedFunc<i64, i32>,
    dump_state: TypedFunc<(), i32>,
    load_state: TypedFunc<(i32, i32), i32>,
    reset_state: TypedFunc<(), i32>,
    read_emit: TypedFunc<(), i32>,
    current_domain_time_nanos: TypedFunc<(), i64>,
    buffer_ptr: TypedFunc<(), i32>,
    global_error: Option<WasmGlobalErrorExports>,
    max_guest_buffer_bytes: usize,
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
    ) -> Result<Self, WasmProcessorError> {
        let memory = instance
            .get_memory(&mut store, EXPORT_MEMORY)
            .ok_or(WasmProcessorError::MissingExport(EXPORT_MEMORY))?;
        Ok(Self {
            alloc: typed_export(&mut store, &instance, "nervix_alloc")?,
            init: typed_export(&mut store, &instance, "nervix_init")?,
            process_batch: typed_export(&mut store, &instance, "nervix_process_batch")?,
            on_timeout: typed_export(&mut store, &instance, "nervix_on_timeout")?,
            dump_state: typed_export(&mut store, &instance, "nervix_dump_state")?,
            load_state: typed_export(&mut store, &instance, "nervix_load_state")?,
            reset_state: typed_export(&mut store, &instance, "nervix_reset_state")?,
            read_emit: typed_export(&mut store, &instance, "nervix_read_emit")?,
            current_domain_time_nanos: typed_export(
                &mut store,
                &instance,
                "nervix_current_domain_time_nanos",
            )?,
            buffer_ptr: typed_export(&mut store, &instance, "nervix_buffer_ptr")?,
            global_error: optional_global_error_exports(&mut store, &instance)?,
            max_guest_buffer_bytes,
            store,
            memory,
        })
    }

    pub async fn init(&mut self, init: WasmBranchInit) -> Result<(), WasmProcessorError> {
        let encoded = init.encode();
        let (ptr, size) = self.write_to_guest_buffer(&encoded).await?;
        let code = self
            .init
            .call_async(&mut self.store, (ptr, size))
            .await
            .map_err(|source| WasmProcessorError::Call {
                name: "nervix_init",
                source,
            })?;
        ensure_success("nervix_init", code)
    }

    pub async fn current_domain_time(&mut self) -> Result<Timestamp, WasmProcessorError> {
        let nanos = self
            .current_domain_time_nanos
            .call_async(&mut self.store, ())
            .await
            .map_err(|source| WasmProcessorError::Call {
                name: "nervix_current_domain_time_nanos",
                source,
            })?;
        Ok(Timestamp::from_unix_nanos(nanos))
    }

    pub async fn process_batch(
        &mut self,
        arrow_ipc_batch: &[u8],
    ) -> Result<Vec<WasmEnvelope>, WasmProcessorError> {
        let envelope = WasmEnvelope::input_arrow_only(arrow_ipc_batch.to_vec());
        self.process_envelope(&envelope).await
    }

    pub async fn process_envelope(
        &mut self,
        envelope: &WasmEnvelope,
    ) -> Result<Vec<WasmEnvelope>, WasmProcessorError> {
        let bytes = envelope.encode()?;
        let (_ptr, size) = self.write_to_guest_buffer(&bytes).await?;
        let call_result = self.process_batch.call_async(&mut self.store, size).await;
        let code = match call_result {
            Ok(code) => code,
            Err(source) => {
                if let Some(reason) = self.take_global_error().await? {
                    return Err(WasmProcessorError::GuestGlobalError(reason));
                }
                return Err(WasmProcessorError::Call {
                    name: "nervix_process_batch",
                    source,
                });
            }
        };
        if let Some(reason) = self.take_global_error().await? {
            return Err(WasmProcessorError::GuestGlobalError(reason));
        }
        ensure_success("nervix_process_batch", code)?;
        if let Some(reason) = self.take_global_error().await? {
            return Err(WasmProcessorError::GuestGlobalError(reason));
        }
        self.read_pending_emit().await
    }

    pub async fn on_timeout(
        &mut self,
        handle: WasmTimeoutHandle,
    ) -> Result<Vec<WasmEnvelope>, WasmProcessorError> {
        let call_result = self
            .on_timeout
            .call_async(&mut self.store, handle.raw())
            .await;
        let code = match call_result {
            Ok(code) => code,
            Err(source) => {
                if let Some(reason) = self.take_global_error().await? {
                    return Err(WasmProcessorError::GuestGlobalError(reason));
                }
                return Err(WasmProcessorError::Call {
                    name: "nervix_on_timeout",
                    source,
                });
            }
        };
        if let Some(reason) = self.take_global_error().await? {
            return Err(WasmProcessorError::GuestGlobalError(reason));
        }
        ensure_success("nervix_on_timeout", code)?;
        if let Some(reason) = self.take_global_error().await? {
            return Err(WasmProcessorError::GuestGlobalError(reason));
        }
        self.read_pending_emit().await
    }

    pub async fn save_state(&mut self) -> Result<Vec<u8>, WasmProcessorError> {
        let size = self
            .dump_state
            .call_async(&mut self.store, ())
            .await
            .map_err(|source| WasmProcessorError::Call {
                name: "nervix_dump_state",
                source,
            })?;
        self.read_guest_buffer(size).await
    }

    pub async fn load_state(&mut self, state: &[u8]) -> Result<(), WasmProcessorError> {
        let (ptr, size) = self.write_to_guest_buffer(state).await?;
        let code = self
            .load_state
            .call_async(&mut self.store, (ptr, size))
            .await
            .map_err(|source| WasmProcessorError::Call {
                name: "nervix_load_state",
                source,
            })?;
        ensure_success("nervix_load_state", code)
    }

    pub async fn reset_state(&mut self) -> Result<(), WasmProcessorError> {
        let code = self
            .reset_state
            .call_async(&mut self.store, ())
            .await
            .map_err(|source| WasmProcessorError::Call {
                name: "nervix_reset_state",
                source,
            })?;
        ensure_success("nervix_reset_state", code)
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
            let deadline = request
                .requested_at
                .unix_nanos()
                .saturating_add(i64::try_from(request.delay.as_nanos()).unwrap_or(i64::MAX));
            if deadline <= now.unix_nanos() {
                due.push(request);
            } else {
                pending.push(request);
            }
        }
        self.store.data_mut().timeout_requests = pending;
        due
    }

    pub async fn take_global_error(&mut self) -> Result<Option<String>, WasmProcessorError> {
        let Some(exports) = &self.global_error else {
            return Ok(None);
        };
        let size = exports
            .len
            .call_async(&mut self.store, ())
            .await
            .map_err(|source| WasmProcessorError::Call {
                name: "nervix_global_error_len",
                source,
            })?;
        if size == 0 {
            return Ok(None);
        }
        let size = usize::try_from(size).map_err(|_| WasmProcessorError::InvalidSize(size))?;
        if size > self.max_guest_buffer_bytes {
            return Err(WasmProcessorError::GuestBufferTooLarge {
                size,
                limit: self.max_guest_buffer_bytes,
            });
        }
        let ptr = exports
            .ptr
            .call_async(&mut self.store, ())
            .await
            .map_err(|source| WasmProcessorError::Call {
                name: "nervix_global_error_ptr",
                source,
            })?;
        let ptr = usize::try_from(ptr).map_err(|_| WasmProcessorError::InvalidOffset(ptr))?;
        let mut bytes = vec![0; size];
        self.memory
            .read(&mut self.store, ptr, &mut bytes)
            .map_err(WasmProcessorError::MemoryRead)?;
        let code = exports
            .clear
            .call_async(&mut self.store, ())
            .await
            .map_err(|source| WasmProcessorError::Call {
                name: "nervix_clear_global_error",
                source,
            })?;
        ensure_success("nervix_clear_global_error", code)?;
        let reason =
            String::from_utf8(bytes).map_err(WasmProcessorError::InvalidGuestGlobalError)?;
        Ok(Some(reason))
    }

    async fn write_to_guest_buffer(
        &mut self,
        bytes: &[u8],
    ) -> Result<(i32, i32), WasmProcessorError> {
        if bytes.len() > self.max_guest_buffer_bytes {
            return Err(WasmProcessorError::GuestBufferTooLarge {
                size: bytes.len(),
                limit: self.max_guest_buffer_bytes,
            });
        }
        let size = i32::try_from(bytes.len()).map_err(|_| WasmProcessorError::InvalidSize(-1))?;
        let ptr = self
            .alloc
            .call_async(&mut self.store, size)
            .await
            .map_err(|source| WasmProcessorError::Call {
                name: "nervix_alloc",
                source,
            })?;
        if ptr < 0 {
            return Err(WasmProcessorError::GuestError {
                name: "nervix_alloc",
                code: ptr,
            });
        }
        self.memory
            .write(&mut self.store, ptr as usize, bytes)
            .map_err(WasmProcessorError::MemoryWrite)?;
        Ok((ptr, size))
    }

    async fn read_guest_buffer(&mut self, size: i32) -> Result<Vec<u8>, WasmProcessorError> {
        let size = usize::try_from(size).map_err(|_| WasmProcessorError::InvalidSize(size))?;
        if size > self.max_guest_buffer_bytes {
            return Err(WasmProcessorError::GuestBufferTooLarge {
                size,
                limit: self.max_guest_buffer_bytes,
            });
        }
        let ptr = self
            .buffer_ptr
            .call_async(&mut self.store, ())
            .await
            .map_err(|source| WasmProcessorError::Call {
                name: "nervix_buffer_ptr",
                source,
            })?;
        let ptr = usize::try_from(ptr).map_err(|_| WasmProcessorError::InvalidOffset(ptr))?;
        let mut out = vec![0; size];
        self.memory
            .read(&mut self.store, ptr, &mut out)
            .map_err(WasmProcessorError::MemoryRead)?;
        Ok(out)
    }

    async fn read_pending_emit(&mut self) -> Result<Vec<WasmEnvelope>, WasmProcessorError> {
        let mut batches = Vec::new();
        loop {
            let size = self
                .read_emit
                .call_async(&mut self.store, ())
                .await
                .map_err(|source| WasmProcessorError::Call {
                    name: "nervix_read_emit",
                    source,
                })?;
            if size == 0 {
                break;
            }
            if size < 0 {
                if let Some(reason) = self.take_global_error().await? {
                    return Err(WasmProcessorError::GuestGlobalError(reason));
                }
                ensure_success("nervix_read_emit", size)?;
            }
            let batch = WasmEnvelope::decode_owned(self.read_guest_buffer(size).await?)
                .map_err(|error| WasmProcessorError::GuestGlobalError(error.to_string()))?;
            if let Some(sender) = self.store.data().emitted_batch_sender.as_ref() {
                let _ = sender.send(batch.clone());
            }
            batches.push(batch);
        }
        Ok(batches)
    }
}

fn define_host_functions(linker: &mut Linker<BranchStore>) -> Result<(), WasmProcessorError> {
    linker
        .func_wrap(
            ENV_MODULE,
            "nervix_domain_time_nanos",
            |caller: Caller<'_, BranchStore>| caller.data().now().unix_nanos(),
        )
        .map_err(WasmProcessorError::Link)?;
    linker
        .func_wrap(
            ENV_MODULE,
            "nervix_timeout_after_nanos",
            |mut caller: Caller<'_, BranchStore>, delay_nanos: i64| {
                caller.data_mut().timeout_after(delay_nanos)
            },
        )
        .map_err(WasmProcessorError::Link)?;
    Ok(())
}

fn typed_export<Params, Results>(
    store: &mut Store<BranchStore>,
    instance: &Instance,
    name: &'static str,
) -> Result<TypedFunc<Params, Results>, WasmProcessorError>
where
    Params: wasmtime::WasmParams,
    Results: wasmtime::WasmResults,
{
    instance
        .get_typed_func(store, name)
        .map_err(|_| WasmProcessorError::MissingExport(name))
}

fn optional_typed_export<Params, Results>(
    store: &mut Store<BranchStore>,
    instance: &Instance,
    name: &'static str,
) -> Result<Option<TypedFunc<Params, Results>>, WasmProcessorError>
where
    Params: wasmtime::WasmParams,
    Results: wasmtime::WasmResults,
{
    match instance.get_typed_func(&mut *store, name) {
        Ok(export) => Ok(Some(export)),
        Err(_) => Ok(None),
    }
}

fn optional_global_error_exports(
    store: &mut Store<BranchStore>,
    instance: &Instance,
) -> Result<Option<WasmGlobalErrorExports>, WasmProcessorError> {
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
            Err(WasmProcessorError::MissingExport(missing))
        }
    }
}

fn ensure_success(name: &'static str, code: i32) -> Result<(), WasmProcessorError> {
    if code == SUCCESS {
        Ok(())
    } else {
        Err(WasmProcessorError::GuestError { name, code })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        sync::{Arc as StdArc, atomic::AtomicU64},
    };

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
    use nervix_models::{CreateSchema, Identifier, ParseAsType, SchemaField};

    use super::*;

    const TEST_WASM: &str = r#"
        (module
            (import "env" "nervix_domain_time_nanos" (func $now (result i64)))
            (import "env" "nervix_timeout_after_nanos" (func $timeout (param i64) (result i64)))
            (memory (export "memory") 1)
            (global $buffer_len (mut i32) (i32.const 0))
            (global $emit_len (mut i32) (i32.const 0))
            (global $processed (mut i64) (i64.const 0))
            (global $last_now (mut i64) (i64.const 0))
            (global $last_timeout (mut i64) (i64.const 0))

            (func (export "nervix_buffer_ptr") (result i32)
                i32.const 1024)

            (func (export "nervix_alloc") (param $size i32) (result i32)
                local.get $size
                global.set $buffer_len
                i32.const 1024)

            (func (export "nervix_init") (param $ptr i32) (param $size i32) (result i32)
                i32.const 0)

            (func (export "nervix_current_domain_time_nanos") (result i64)
                call $now
                global.set $last_now
                global.get $last_now)

            (func (export "nervix_process_batch") (param $size i32) (result i32)
                call $now
                global.set $last_now
                i64.const 5000000
                call $timeout
                global.set $last_timeout
                global.get $processed
                i64.const 1
                i64.add
                global.set $processed
                local.get $size
                global.set $emit_len
                i32.const 0)

            (func (export "nervix_on_timeout") (param $handle i64) (result i32)
                local.get $handle
                global.set $last_timeout
                i32.const 0)

            (func (export "nervix_read_emit") (result i32)
                global.get $emit_len
                global.set $buffer_len
                i32.const 0
                global.set $emit_len
                global.get $buffer_len)

            (func (export "nervix_dump_state") (result i32)
                i32.const 1024
                global.get $processed
                i64.store
                i32.const 1032
                global.get $last_now
                i64.store
                i32.const 1040
                global.get $last_timeout
                i64.store
                i32.const 24)

            (func (export "nervix_load_state") (param $ptr i32) (param $size i32) (result i32)
                local.get $size
                i32.const 24
                i32.ne
                if (result i32)
                    i32.const -2
                else
                    local.get $ptr
                    i64.load
                    global.set $processed
                    local.get $ptr
                    i32.const 8
                    i32.add
                    i64.load
                    global.set $last_now
                    local.get $ptr
                    i32.const 16
                    i32.add
                    i64.load
                    global.set $last_timeout
                    i32.const 0
                end)

            (func (export "nervix_reset_state") (result i32)
                i64.const 0
                global.set $processed
                i64.const 0
                global.set $last_now
                i64.const 0
                global.set $last_timeout
                i32.const 0
                global.set $emit_len
                i32.const 0)
        )
    "#;

    const CPU_BOUND_WASM: &str = r#"
        (module
            (memory (export "memory") 1)
            (global $emit_len (mut i32) (i32.const 0))

            (func (export "nervix_buffer_ptr") (result i32)
                i32.const 1024)

            (func (export "nervix_alloc") (param $size i32) (result i32)
                i32.const 1024)

            (func (export "nervix_init") (param $ptr i32) (param $size i32) (result i32)
                i32.const 0)

            (func (export "nervix_current_domain_time_nanos") (result i64)
                i64.const 0)

            (func (export "nervix_process_batch") (param $size i32) (result i32)
                (local $i i64)
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
        )
    "#;

    fn init() -> WasmBranchInit {
        WasmBranchInit {
            domain_name: "events".to_string(),
            domain_type: "PACED".to_string(),
            branch_key: Some(b"user=42".to_vec()),
            input_schema: WasmProcessorSchema::from(&processor_schema("input_events")),
            output_schemas: vec![WasmProcessorSchema::from(&processor_schema(
                "output_events",
            ))],
        }
    }

    fn processor_schema(name: &str) -> CreateSchema {
        CreateSchema {
            name: Identifier::parse(name).expect("schema name must be valid"),
            fields: vec![SchemaField {
                name: Identifier::parse("value").expect("field name must be valid"),
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
            name: Identifier::parse("input_events").expect("schema name must be valid"),
            fields: vec![
                SchemaField {
                    name: Identifier::parse("value").expect("field name must be valid"),
                    ty: ParseAsType::I32,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: Identifier::parse("payload").expect("field name must be valid"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
            ],
        };
        let mut output_schema = WasmProcessorSchema::from(&schema);
        output_schema.name = "output_events".to_string();
        WasmBranchInit {
            domain_name: "events".to_string(),
            domain_type: "PACED".to_string(),
            branch_key: Some(b"user=42".to_vec()),
            input_schema: WasmProcessorSchema::from(&schema),
            output_schemas: vec![output_schema],
        }
    }

    fn shared_generated_init() -> WasmBranchInit {
        let input_schema = CreateSchema {
            name: Identifier::parse("input_events").expect("schema name must be valid"),
            fields: vec![SchemaField {
                name: Identifier::parse("value").expect("field name must be valid"),
                ty: ParseAsType::I32,
                optional: false,
                sensitive: false,
            }],
        };
        let enriched_schema = CreateSchema {
            name: Identifier::parse("enriched_events").expect("schema name must be valid"),
            fields: vec![
                input_schema.fields[0].clone(),
                SchemaField {
                    name: Identifier::parse("bucket").expect("field name must be valid"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
            ],
        };
        let audit_schema = CreateSchema {
            name: Identifier::parse("audit_events").expect("schema name must be valid"),
            fields: vec![
                input_schema.fields[0].clone(),
                SchemaField {
                    name: Identifier::parse("classification").expect("field name must be valid"),
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
            input_schema: WasmProcessorSchema::from(&input_schema),
            output_schemas: vec![
                WasmProcessorSchema::from(&enriched_schema),
                WasmProcessorSchema::from(&audit_schema),
            ],
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
        let encoded_start = encoded.as_ptr() as usize;
        let encoded_end = encoded_start + encoded.len();
        assert!((encoded_start..encoded_end).contains(&(view.arrow_ipc_batch().as_ptr() as usize)));
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
        let encoded_start = encoded.as_ptr() as usize;
        let encoded_end = encoded_start + encoded.len();
        assert!(
            (encoded_start..encoded_end)
                .contains(&(view.generated_arrow_ipc_batch().as_ptr() as usize))
        );
        let WasmEnvelope::Output {
            generated_arrow_ipc_batch,
            ..
        } = WasmEnvelope::decode_owned(encoded).expect("owned output must decode")
        else {
            panic!("expected owned output envelope");
        };
        assert!(
            (encoded_start..encoded_end).contains(&(generated_arrow_ipc_batch.as_ptr() as usize))
        );
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
            name: Identifier::parse("events").expect("schema name must be valid"),
            fields: vec![
                SchemaField {
                    name: Identifier::parse("value").expect("field name must be valid"),
                    ty: ParseAsType::I32,
                    optional: false,
                    sensitive: false,
                },
                SchemaField {
                    name: Identifier::parse("tags").expect("field name must be valid"),
                    ty: ParseAsType::Vec {
                        element: Box::new(ParseAsType::String),
                    },
                    optional: true,
                    sensitive: false,
                },
            ],
        };

        let converted = WasmProcessorSchema::from(&source);

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

    #[tokio::test]
    async fn load_state_rejection_is_reported() {
        let wasm = r#"
            (module
                (memory (export "memory") 1)
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32) (result i32) i32.const 0)
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const -1)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
            )
        "#;
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(wasm.as_bytes())
            .await
            .expect("module should compile");
        let error = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(0))),
                Some(b"bad state"),
            )
            .await
            .expect_err("guest should reject restored state");

        match error {
            WasmProcessorError::GuestError { name, code } => {
                assert_eq!(name, "nervix_load_state");
                assert_eq!(code, -1);
            }
            other => panic!("expected load_state guest error, got {other:?}"),
        }
    }

    fn return_code_wasm(process_code: i32, timeout_code: i32) -> String {
        format!(
            r#"
            (module
                (memory (export "memory") 1)
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32) (result i32) i32.const {process_code})
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const {timeout_code})
                (func (export "nervix_read_emit") (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
            )
            "#
        )
    }

    #[tokio::test]
    async fn oversized_guest_input_is_rejected_before_calling_guest() {
        let runtime = WasmRuntime::new(WasmRuntimeConfig {
            optimize: false,
            epoch_tick_interval: Duration::from_millis(1),
            epoch_deadline_ticks: 1,
            max_guest_buffer_bytes: 512,
        })
        .expect("runtime must initialize");
        let compiled = runtime
            .compile_processor(return_code_wasm(0, 0).as_bytes())
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(0))),
                None,
            )
            .await
            .expect("guest branch should instantiate");

        let error = branch
            .process_batch(&vec![0; 600])
            .await
            .expect_err("oversized input should be rejected by host");

        match error {
            WasmProcessorError::GuestBufferTooLarge { size, limit } => {
                assert!(size > 512);
                assert_eq!(limit, 512);
            }
            other => panic!("expected guest buffer limit error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn process_batch_negative_guest_code_is_reported() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(return_code_wasm(-4, 0).as_bytes())
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(0))),
                None,
            )
            .await
            .expect("guest branch should instantiate");

        let error = branch
            .process_batch(b"bad")
            .await
            .expect_err("negative process code should be reported");

        match error {
            WasmProcessorError::GuestError { name, code } => {
                assert_eq!(name, "nervix_process_batch");
                assert_eq!(code, -4);
            }
            other => panic!("expected process guest error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn timeout_negative_guest_code_is_reported() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(return_code_wasm(0, -5).as_bytes())
            .await
            .expect("module should compile");
        let mut branch = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(0))),
                None,
            )
            .await
            .expect("guest branch should instantiate");

        let error = branch
            .on_timeout(WasmTimeoutHandle(42))
            .await
            .expect_err("negative timeout code should be reported");

        match error {
            WasmProcessorError::GuestError { name, code } => {
                assert_eq!(name, "nervix_on_timeout");
                assert_eq!(code, -5);
            }
            other => panic!("expected timeout guest error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_required_export_is_reported() {
        let wasm = r#"
            (module
                (memory (export "memory") 1)
                (func (export "nervix_buffer_ptr") (result i32) i32.const 1024)
                (func (export "nervix_alloc") (param i32) (result i32) i32.const 1024)
                (func (export "nervix_init") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_current_domain_time_nanos") (result i64) i64.const 0)
                (func (export "nervix_process_batch") (param i32) (result i32) i32.const 0)
                (func (export "nervix_on_timeout") (param i64) (result i32) i32.const 0)
                (func (export "nervix_dump_state") (result i32) i32.const 0)
                (func (export "nervix_load_state") (param i32 i32) (result i32) i32.const 0)
                (func (export "nervix_reset_state") (result i32) i32.const 0)
            )
        "#;
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(wasm.as_bytes())
            .await
            .expect("module should compile");
        let error = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(0))),
                None,
            )
            .await
            .expect_err("missing export should reject instantiation");

        match error {
            WasmProcessorError::MissingExport(name) => {
                assert_eq!(name, "nervix_read_emit");
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
            .compile_processor(read_guest(path))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(1_234))),
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
                .process_envelope(&input)
                .await
                .expect("first input must process"),
            Vec::<WasmEnvelope>::new()
        );
        let timeout = branch
            .take_timeout_requests()
            .pop()
            .expect("guest should request a timeout");
        let output = branch
            .on_timeout(timeout.handle)
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

    #[tokio::test]
    async fn rust_guest_interoperates_with_host_flatbuffer_format() {
        guest_emits_flatbuffer_input_reference_output(&rust_guest_path()).await;
    }

    #[tokio::test]
    async fn rust_guest_output_omits_unchanged_string_payloads() {
        const SENTINEL: &str = "UNCHANGED_PAYLOAD_SENTINEL";

        let runtime = runtime();
        let compiled = runtime
            .compile_processor(read_guest(&rust_guest_path()))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                string_passthrough_init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(1_234))),
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
                .process_envelope(&input)
                .await
                .expect("input must process")
                .is_empty()
        );
        let timeout = branch
            .take_timeout_requests()
            .pop()
            .expect("guest should request a timeout");
        let outputs = branch
            .on_timeout(timeout.handle)
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

    #[tokio::test]
    async fn rust_guest_timeout_shares_one_generated_column_across_routes() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(read_guest(&rust_guest_path()))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                shared_generated_init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(1_234))),
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
                .process_envelope(&input)
                .await
                .expect("input must process")
                .is_empty()
        );
        let timeout = branch
            .take_timeout_requests()
            .pop()
            .expect("guest should request a timeout");
        let envelopes = branch
            .on_timeout(timeout.handle)
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

    #[tokio::test]
    async fn repeated_read_emit_calls_return_multiple_output_groups() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(read_guest(&rust_guest_path()))
            .await
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(1_234))),
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
                .process_envelope(&first)
                .await
                .expect("first input must process")
                .is_empty()
        );
        let groups = branch
            .process_envelope(&second)
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

    #[tokio::test]
    async fn go_guest_interoperates_with_host_flatbuffer_format() {
        guest_emits_flatbuffer_input_reference_output(&go_guest_path()).await;
    }

    #[tokio::test]
    async fn branch_instances_reuse_compiled_module_with_isolated_stores() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(TEST_WASM)
            .await
            .expect("module must compile");
        let left_clock = FixedDomainClock::new(Timestamp::from_unix_nanos(100));
        let right_clock = FixedDomainClock::new(Timestamp::from_unix_nanos(200));

        let mut left = compiled
            .instantiate_branch(init(), Box::new(left_clock.clone()), None)
            .await
            .expect("left branch must instantiate");
        let mut right = compiled
            .instantiate_branch(init(), Box::new(right_clock.clone()), None)
            .await
            .expect("right branch must instantiate");

        assert_eq!(
            left.current_domain_time().await.expect("clock must work"),
            Timestamp::from_unix_nanos(100)
        );
        assert_eq!(
            right.current_domain_time().await.expect("clock must work"),
            Timestamp::from_unix_nanos(200)
        );

        let left_out = left
            .process_batch(b"left batch")
            .await
            .expect("left batch must process");
        let right_out = right
            .process_batch(b"right batch")
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
    }

    #[tokio::test]
    async fn saved_state_loads_into_new_branch_store() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(TEST_WASM)
            .await
            .expect("module must compile");
        let clock = FixedDomainClock::new(Timestamp::from_unix_nanos(700));
        let mut branch = compiled
            .instantiate_branch(init(), Box::new(clock.clone()), None)
            .await
            .expect("branch must instantiate");
        branch
            .process_batch(b"one")
            .await
            .expect("batch must process");
        let state = branch.save_state().await.expect("state must dump");

        clock.set(Timestamp::from_unix_nanos(900));
        let mut restored = compiled
            .instantiate_branch(init(), Box::new(clock), Some(&state))
            .await
            .expect("restored branch must instantiate");
        let restored_state = restored.save_state().await.expect("state must dump");

        assert_eq!(&restored_state[..8], &1_i64.to_le_bytes());
        assert_eq!(&restored_state[8..16], &700_i64.to_le_bytes());
    }

    #[tokio::test]
    async fn emitted_batches_can_be_streamed_to_integration_owner() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(TEST_WASM)
            .await
            .expect("module must compile");
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let mut branch = compiled
            .instantiate_branch_with_emitter(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(1))),
                None,
                Some(sender),
            )
            .await
            .expect("branch must instantiate");

        let returned = branch
            .process_batch(b"stream me")
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

    #[tokio::test(flavor = "current_thread")]
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
        let progress_task = tokio::spawn(async move {
            loop {
                tokio::task::consume_budget().await;
                task_ticks.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });
        tokio::task::yield_now().await;
        let ticks_before_compile = ticks.load(Ordering::Relaxed);

        runtime
            .compile_processor(wasm.as_bytes())
            .await
            .expect("stress module must compile");

        let ticks_after_compile = ticks.load(Ordering::Relaxed);
        progress_task.abort();
        assert!(
            ticks_after_compile > ticks_before_compile,
            "wasm compilation must not block the async executor"
        );
    }

    #[tokio::test]
    async fn epoch_driver_yields_cpu_bound_guest_to_async_executor() {
        let runtime = WasmRuntime::new(WasmRuntimeConfig {
            optimize: true,
            epoch_tick_interval: Duration::from_micros(100),
            epoch_deadline_ticks: 1,
            max_guest_buffer_bytes: DEFAULT_MAX_GUEST_BUFFER_BYTES,
        })
        .expect("runtime must initialize");
        let compiled = runtime
            .compile_processor(CPU_BOUND_WASM)
            .await
            .expect("module must compile");
        let mut branch = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(1))),
                None,
            )
            .await
            .expect("branch must instantiate");

        let ticks = Arc::new(AtomicU64::new(0));
        let task_ticks = Arc::clone(&ticks);
        let progress_task = tokio::spawn(async move {
            loop {
                task_ticks.fetch_add(1, Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        });

        let returned = branch
            .process_batch(b"cpu")
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
}
