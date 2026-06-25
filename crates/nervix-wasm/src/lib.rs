use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use nervix_models::{ParseAsType, Timestamp};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;
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

#[derive(Debug, Error)]
pub enum WasmProcessorError {
    #[error("failed to configure wasmtime: {0}")]
    Configure(#[source] wasmtime::Error),
    #[error("failed to compile wasm module: {0}")]
    Compile(#[source] wasmtime::Error),
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
    #[error("failed to encode branch init metadata as CBOR: {0}")]
    EncodeInit(#[source] ciborium::ser::Error<std::io::Error>),
    #[error("failed to encode WASM batch envelope ack sidecar as CBOR: {0}")]
    EncodeEnvelope(#[source] ciborium::ser::Error<std::io::Error>),
    #[error("failed to decode WASM batch envelope: {0}")]
    DecodeEnvelope(String),
    #[error("guest buffer size {size} exceeds configured limit {limit}")]
    GuestBufferTooLarge { size: usize, limit: usize },
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
    stop: Arc<AtomicBool>,
    epoch_deadline_ticks: u64,
    max_guest_buffer_bytes: usize,
}

impl WasmRuntime {
    pub fn new(config: WasmRuntimeConfig) -> Result<Self, WasmProcessorError> {
        let mut wasmtime_config = Config::new();
        wasmtime_config.epoch_interruption(true);
        wasmtime_config.cranelift_opt_level(if config.optimize {
            OptLevel::Speed
        } else {
            OptLevel::None
        });
        let engine = Engine::new(&wasmtime_config).map_err(WasmProcessorError::Configure)?;
        let stop = Arc::new(AtomicBool::new(false));
        spawn_epoch_driver(
            engine.clone(),
            Arc::clone(&stop),
            config.epoch_tick_interval,
        );
        Ok(Self {
            engine,
            stop,
            epoch_deadline_ticks: config.epoch_deadline_ticks,
            max_guest_buffer_bytes: config.max_guest_buffer_bytes,
        })
    }

    pub fn compile_processor(
        &self,
        wasm: impl AsRef<[u8]>,
    ) -> Result<CompiledWasmProcessor, WasmProcessorError> {
        let module =
            Module::new(&self.engine, wasm.as_ref()).map_err(WasmProcessorError::Compile)?;
        let mut linker = Linker::<BranchStore>::new(&self.engine);
        define_host_functions(&mut linker)?;
        let instance_pre = linker
            .instantiate_pre(&module)
            .map_err(WasmProcessorError::Link)?;
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
        if Arc::strong_count(&self.stop) == 1 {
            self.stop.store(true, Ordering::Relaxed);
        }
    }
}

fn spawn_epoch_driver(engine: Engine, stop: Arc<AtomicBool>, interval: Duration) {
    let weak_stop = Arc::downgrade(&stop);
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmBranchInit {
    pub domain_name: String,
    pub domain_type: String,
    pub branch_key: Option<Vec<u8>>,
    pub input_schema: WasmProcessorSchema,
    pub output_schemas: Vec<WasmProcessorSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmProcessorSchema {
    pub name: String,
    pub fields: Vec<WasmProcessorField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmProcessorField {
    pub name: String,
    pub ty: WasmProcessorType,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WasmAckToken(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WasmRowAckSet {
    pub tokens: Vec<WasmAckToken>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmNackSet {
    pub tokens: Vec<WasmAckToken>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WasmMessageErrorSet {
    pub tokens: Vec<WasmAckToken>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct WasmAckSidecar {
    #[serde(default)]
    pub rows: Vec<WasmRowAckSet>,
    #[serde(default)]
    pub acked: Vec<WasmRowAckSet>,
    #[serde(default)]
    pub nacked: Vec<WasmNackSet>,
    #[serde(default)]
    pub message_errors: Vec<WasmMessageErrorSet>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmBatchEnvelope {
    pub output_relay: Option<String>,
    pub arrow_ipc_batch: Vec<u8>,
    pub acks: WasmAckSidecar,
}

impl WasmBatchEnvelope {
    pub fn new(arrow_ipc_batch: Vec<u8>, acks: WasmAckSidecar) -> Self {
        Self {
            output_relay: None,
            arrow_ipc_batch,
            acks,
        }
    }

    pub fn output(
        output_relay: impl Into<String>,
        arrow_ipc_batch: Vec<u8>,
        acks: WasmAckSidecar,
    ) -> Self {
        Self {
            output_relay: Some(output_relay.into()),
            arrow_ipc_batch,
            acks,
        }
    }

    pub fn arrow_only(arrow_ipc_batch: Vec<u8>) -> Self {
        Self {
            output_relay: None,
            arrow_ipc_batch,
            acks: WasmAckSidecar::default(),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, WasmProcessorError> {
        let output_relay = self.output_relay.as_deref().unwrap_or_default().as_bytes();
        let output_len = u32::try_from(output_relay.len()).map_err(|_| {
            WasmProcessorError::DecodeEnvelope("output relay name is too large".to_string())
        })?;
        let arrow_len = u32::try_from(self.arrow_ipc_batch.len()).map_err(|_| {
            WasmProcessorError::DecodeEnvelope("Arrow IPC payload is too large".to_string())
        })?;
        let mut ack_bytes = Vec::new();
        ciborium::into_writer(&self.acks, &mut ack_bytes)
            .map_err(WasmProcessorError::EncodeEnvelope)?;
        let ack_len = u32::try_from(ack_bytes.len()).map_err(|_| {
            WasmProcessorError::DecodeEnvelope("ack sidecar payload is too large".to_string())
        })?;
        let mut encoded = Vec::with_capacity(
            12 + output_relay.len() + self.arrow_ipc_batch.len() + ack_bytes.len(),
        );
        encoded.extend_from_slice(&output_len.to_le_bytes());
        encoded.extend_from_slice(output_relay);
        encoded.extend_from_slice(&arrow_len.to_le_bytes());
        encoded.extend_from_slice(&self.arrow_ipc_batch);
        encoded.extend_from_slice(&ack_len.to_le_bytes());
        encoded.extend_from_slice(&ack_bytes);
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WasmProcessorError> {
        if bytes.len() < 12 {
            return Err(WasmProcessorError::DecodeEnvelope(
                "envelope is shorter than length prefixes".to_string(),
            ));
        }
        let output_len = read_u32_len(bytes, 0, "output relay")?;
        let output_start = 4_usize;
        let output_end = output_start.checked_add(output_len).ok_or_else(|| {
            WasmProcessorError::DecodeEnvelope("output relay length overflow".to_string())
        })?;
        if output_end + 8 > bytes.len() {
            return Err(WasmProcessorError::DecodeEnvelope(
                "output relay length exceeds envelope size".to_string(),
            ));
        }
        let output_relay = if output_len == 0 {
            None
        } else {
            Some(
                std::str::from_utf8(&bytes[output_start..output_end])
                    .map_err(|error| {
                        WasmProcessorError::DecodeEnvelope(format!(
                            "invalid output relay UTF-8: {error}"
                        ))
                    })?
                    .to_string(),
            )
        };
        let arrow_len = read_u32_len(bytes, output_end, "Arrow IPC")?;
        let arrow_start = output_end + 4;
        let arrow_end = arrow_start.checked_add(arrow_len).ok_or_else(|| {
            WasmProcessorError::DecodeEnvelope("Arrow IPC length overflow".to_string())
        })?;
        if arrow_end + 4 > bytes.len() {
            return Err(WasmProcessorError::DecodeEnvelope(
                "Arrow IPC length exceeds envelope size".to_string(),
            ));
        }
        let ack_len = read_u32_len(bytes, arrow_end, "ack sidecar")?;
        let ack_start = arrow_end + 4;
        let ack_end = ack_start.checked_add(ack_len).ok_or_else(|| {
            WasmProcessorError::DecodeEnvelope("ack sidecar length overflow".to_string())
        })?;
        if ack_end != bytes.len() {
            return Err(WasmProcessorError::DecodeEnvelope(
                "ack sidecar length does not consume the envelope".to_string(),
            ));
        }
        let acks = ciborium::from_reader(&bytes[ack_start..ack_end]).map_err(|error| {
            WasmProcessorError::DecodeEnvelope(format!("invalid ack sidecar CBOR: {error}"))
        })?;
        Ok(Self {
            output_relay,
            arrow_ipc_batch: bytes[arrow_start..arrow_end].to_vec(),
            acks,
        })
    }
}

fn read_u32_len(
    bytes: &[u8],
    offset: usize,
    label: &'static str,
) -> Result<usize, WasmProcessorError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| WasmProcessorError::DecodeEnvelope(format!("{label} length overflow")))?;
    let raw = bytes.get(offset..end).ok_or_else(|| {
        WasmProcessorError::DecodeEnvelope(format!("missing {label} length prefix"))
    })?;
    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize)
}

struct BranchStore {
    clock: Box<dyn DomainClock>,
    timeout_requests: Vec<WasmTimeoutRequest>,
    next_timeout_handle: i64,
    emitted_batch_sender: Option<mpsc::UnboundedSender<WasmBatchEnvelope>>,
}

impl BranchStore {
    fn new(
        clock: Box<dyn DomainClock>,
        emitted_batch_sender: Option<mpsc::UnboundedSender<WasmBatchEnvelope>>,
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
        emitted_batch_sender: Option<mpsc::UnboundedSender<WasmBatchEnvelope>>,
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
        let mut encoded = Vec::new();
        ciborium::into_writer(&init, &mut encoded).map_err(WasmProcessorError::EncodeInit)?;
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
    ) -> Result<Vec<Vec<u8>>, WasmProcessorError> {
        let envelope = WasmBatchEnvelope::arrow_only(arrow_ipc_batch.to_vec());
        let emitted = self.process_envelope(&envelope).await?;
        Ok(emitted
            .into_iter()
            .map(|envelope| envelope.arrow_ipc_batch)
            .collect())
    }

    pub async fn process_envelope(
        &mut self,
        envelope: &WasmBatchEnvelope,
    ) -> Result<Vec<WasmBatchEnvelope>, WasmProcessorError> {
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
    ) -> Result<Vec<WasmBatchEnvelope>, WasmProcessorError> {
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
        let reason = String::from_utf8(bytes).map_err(|error| {
            WasmProcessorError::DecodeEnvelope(format!("invalid guest global error UTF-8: {error}"))
        })?;
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

    async fn read_pending_emit(&mut self) -> Result<Vec<WasmBatchEnvelope>, WasmProcessorError> {
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
            let batch = WasmBatchEnvelope::decode(&self.read_guest_buffer(size).await?)
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

    use arrow_array::{Array, Int32Array, RecordBatch};
    use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
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

    fn decode_arrow_ipc_values(bytes: &[u8]) -> Vec<i32> {
        let reader = StreamReader::try_new(bytes, None).expect("ipc reader must build");
        let mut values = Vec::new();
        for batch in reader {
            let batch = batch.expect("ipc batch must decode");
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("value column must be i32");
            for row in 0..column.len() {
                if column.is_valid(row) {
                    values.push(column.value(row));
                }
            }
        }
        values
    }

    fn row_ack(token: u64) -> WasmRowAckSet {
        WasmRowAckSet {
            tokens: vec![WasmAckToken(token)],
        }
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

    async fn guest_flushes_on_timeout_and_periodic_boundary(path: &Path) {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(read_guest(path))
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(1_234))),
                None,
            )
            .await
            .expect("guest branch must instantiate");

        assert_eq!(
            branch
                .current_domain_time()
                .await
                .expect("guest must read domain clock"),
            Timestamp::from_unix_nanos(1_234)
        );

        let first = sample_arrow_ipc(&[1, 2, 3, 4]);
        let second = sample_arrow_ipc(&[5, 6, 7, 8]);
        let third = sample_arrow_ipc(&[9, 10]);
        let fourth = sample_arrow_ipc(&[11, 12, 13]);

        assert_eq!(
            branch
                .process_batch(&first)
                .await
                .expect("first batch must process"),
            Vec::<Vec<u8>>::new()
        );
        let timeout = branch
            .take_timeout_requests()
            .pop()
            .expect("first batch should request a flush timeout");
        assert_eq!(timeout.requested_at, Timestamp::from_unix_nanos(1_234));
        assert_eq!(timeout.delay, Duration::from_secs(1));
        let timeout_output = branch
            .on_timeout(timeout.handle)
            .await
            .expect("timeout should flush pending batch");
        assert_eq!(timeout_output.len(), 1);
        assert_eq!(
            decode_arrow_ipc_values(&timeout_output[0].arrow_ipc_batch),
            vec![2, 4]
        );

        let second_output = branch
            .process_batch(&second)
            .await
            .expect("second batch must process");
        assert_eq!(second_output.len(), 1);
        assert_eq!(decode_arrow_ipc_values(&second_output[0]), vec![6, 8]);
        assert_eq!(
            branch
                .process_batch(&third)
                .await
                .expect("third batch must process"),
            Vec::<Vec<u8>>::new()
        );
        let fourth_output = branch
            .process_batch(&fourth)
            .await
            .expect("fourth batch must process");
        assert_eq!(fourth_output.len(), 1);
        assert_eq!(decode_arrow_ipc_values(&fourth_output[0]), vec![12]);
    }

    async fn guest_state_restoration_preserves_processed_row_count(path: &Path) {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(read_guest(path))
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(5_000))),
                None,
            )
            .await
            .expect("guest branch must instantiate");

        assert_eq!(
            branch
                .process_batch(&sample_arrow_ipc(&[1, 2, 3]))
                .await
                .expect("first batch must process"),
            Vec::<Vec<u8>>::new()
        );
        let timeout = branch
            .take_timeout_requests()
            .pop()
            .expect("first batch should request a flush timeout");
        let first_output = branch
            .on_timeout(timeout.handle)
            .await
            .expect("timeout should flush first batch");
        assert_eq!(first_output.len(), 1);
        assert_eq!(
            decode_arrow_ipc_values(&first_output[0].arrow_ipc_batch),
            vec![2]
        );

        let saved_state = branch.save_state().await.expect("state must dump");
        let mut restored = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(6_000))),
                Some(&saved_state),
            )
            .await
            .expect("restored guest branch must instantiate");

        let restored_output = restored
            .process_batch(&sample_arrow_ipc(&[4, 5, 6]))
            .await
            .expect("restored branch must process next batch");
        assert_eq!(restored_output.len(), 1);
        assert_eq!(decode_arrow_ipc_values(&restored_output[0]), vec![4, 6]);
    }

    async fn guest_preserves_and_completes_ack_sidecar(path: &Path) {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(read_guest(path))
            .expect("guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(5_000))),
                None,
            )
            .await
            .expect("guest branch must instantiate");

        assert_eq!(
            branch
                .process_envelope(&WasmBatchEnvelope::new(
                    sample_arrow_ipc(&[1, 2, 3, 4]),
                    WasmAckSidecar {
                        rows: vec![row_ack(10), row_ack(20), row_ack(30), row_ack(40),],
                        acked: Vec::new(),
                        nacked: Vec::new(),
                        message_errors: Vec::new(),
                    },
                ))
                .await
                .expect("first batch must process"),
            Vec::<WasmBatchEnvelope>::new()
        );
        let timeout = branch
            .take_timeout_requests()
            .pop()
            .expect("first batch should request a flush timeout");
        let output = branch
            .on_timeout(timeout.handle)
            .await
            .expect("timeout should flush pending batch");
        assert_eq!(output.len(), 1);
        assert_eq!(
            decode_arrow_ipc_values(&output[0].arrow_ipc_batch),
            vec![2, 4]
        );
        assert_eq!(output[0].acks.rows, vec![row_ack(20), row_ack(40)]);
        assert_eq!(output[0].acks.acked, vec![row_ack(10), row_ack(30)]);
        assert_eq!(output[0].acks.nacked, Vec::<WasmNackSet>::new());
    }

    #[tokio::test]
    async fn go_guest_filters_rust_guest_output_envelope() {
        let runtime = runtime();
        let rust_compiled = runtime
            .compile_processor(read_guest(&rust_guest_path()))
            .expect("rust guest module must compile");
        let go_compiled = runtime
            .compile_processor(read_guest(&go_guest_path()))
            .expect("go guest module must compile");
        let mut rust_branch = rust_compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(5_000))),
                None,
            )
            .await
            .expect("rust guest branch must instantiate");
        let mut go_branch = go_compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(5_000))),
                None,
            )
            .await
            .expect("go guest branch must instantiate");

        rust_branch
            .process_envelope(&WasmBatchEnvelope::new(
                sample_arrow_ipc(&[1, 2, 3, 4]),
                WasmAckSidecar {
                    rows: vec![row_ack(10), row_ack(20), row_ack(30), row_ack(40)],
                    acked: Vec::new(),
                    nacked: Vec::new(),
                    message_errors: Vec::new(),
                },
            ))
            .await
            .expect("rust guest first batch must process");
        let rust_timeout = rust_branch
            .take_timeout_requests()
            .pop()
            .expect("rust guest should request flush timeout");
        let rust_output = rust_branch
            .on_timeout(rust_timeout.handle)
            .await
            .expect("rust guest timeout should flush");
        assert_eq!(rust_output.len(), 1);
        assert_eq!(
            decode_arrow_ipc_values(&rust_output[0].arrow_ipc_batch),
            vec![2, 4]
        );

        let go_output = go_branch
            .process_envelope(&rust_output[0])
            .await
            .expect("go guest must accept rust guest output envelope");
        assert_eq!(go_output, Vec::<WasmBatchEnvelope>::new());
        let go_timeout = go_branch
            .take_timeout_requests()
            .pop()
            .expect("go guest should request flush timeout");
        let go_output = go_branch
            .on_timeout(go_timeout.handle)
            .await
            .expect("go guest timeout should flush");
        assert_eq!(go_output.len(), 1);
        assert_eq!(
            decode_arrow_ipc_values(&go_output[0].arrow_ipc_batch),
            vec![4]
        );
        assert_eq!(go_output[0].acks.rows, vec![row_ack(40)]);
        assert_eq!(
            go_output[0].acks.acked,
            vec![row_ack(10), row_ack(30), row_ack(20)]
        );
    }

    #[tokio::test]
    async fn go_guest_handles_many_rust_guest_output_envelopes() {
        let runtime = runtime();
        let rust_compiled = runtime
            .compile_processor(read_guest(&rust_guest_path()))
            .expect("rust guest module must compile");
        let go_compiled = runtime
            .compile_processor(read_guest(&go_guest_path()))
            .expect("go guest module must compile");
        let mut rust_branch = rust_compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(5_000))),
                None,
            )
            .await
            .expect("rust guest branch must instantiate");
        let mut go_branch = go_compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(5_000))),
                None,
            )
            .await
            .expect("go guest branch must instantiate");

        let values = (1..=100_i32).collect::<Vec<_>>();
        assert_eq!(
            rust_branch
                .process_envelope(&WasmBatchEnvelope::new(
                    sample_arrow_ipc(&values),
                    WasmAckSidecar {
                        rows: Vec::new(),
                        acked: Vec::new(),
                        nacked: Vec::new(),
                        message_errors: Vec::new(),
                    },
                ))
                .await
                .expect("rust guest must process input envelope"),
            Vec::<WasmBatchEnvelope>::new()
        );

        let mut observed = Vec::new();
        for timeout in rust_branch.take_timeout_requests() {
            let rust_outputs = rust_branch
                .on_timeout(timeout.handle)
                .await
                .expect("rust guest timeout must flush");
            for rust_output in rust_outputs {
                let go_outputs = go_branch
                    .process_envelope(&rust_output)
                    .await
                    .expect("go guest must process rust timeout output envelope");
                for go_output in go_outputs {
                    observed.extend(decode_arrow_ipc_values(&go_output.arrow_ipc_batch));
                }
            }
        }
        for timeout in go_branch.take_timeout_requests() {
            let go_outputs = go_branch
                .on_timeout(timeout.handle)
                .await
                .expect("go guest timeout must flush");
            for go_output in go_outputs {
                observed.extend(decode_arrow_ipc_values(&go_output.arrow_ipc_batch));
            }
        }

        assert!(observed.contains(&100));
    }

    #[tokio::test]
    async fn go_guest_timeout_survives_state_dump_after_process() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(read_guest(&go_guest_path()))
            .expect("go guest module must compile");
        let mut branch = compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(1_234))),
                None,
            )
            .await
            .expect("go guest branch must instantiate");

        assert_eq!(
            branch
                .process_envelope(&WasmBatchEnvelope::new(
                    sample_arrow_ipc(&[1, 2, 3, 4]),
                    WasmAckSidecar {
                        rows: vec![row_ack(10), row_ack(20), row_ack(30), row_ack(40)],
                        acked: Vec::new(),
                        nacked: Vec::new(),
                        message_errors: Vec::new(),
                    },
                ))
                .await
                .expect("go guest first batch must process"),
            Vec::<WasmBatchEnvelope>::new()
        );
        let timeout = branch
            .take_timeout_requests()
            .pop()
            .expect("go guest should request flush timeout");

        let _saved_state = branch.save_state().await.expect("state must dump");
        let output = branch
            .on_timeout(timeout.handle)
            .await
            .expect("timeout should flush pending batch after state dump");

        assert_eq!(output.len(), 1);
        assert_eq!(
            decode_arrow_ipc_values(&output[0].arrow_ipc_batch),
            vec![2, 4]
        );
        assert_eq!(output[0].acks.rows, vec![row_ack(20), row_ack(40)]);
    }

    #[tokio::test]
    async fn go_guest_filters_repeated_single_row_rust_outputs() {
        let runtime = runtime();
        let rust_compiled = runtime
            .compile_processor(read_guest(&rust_guest_path()))
            .expect("rust guest module must compile");
        let go_compiled = runtime
            .compile_processor(read_guest(&go_guest_path()))
            .expect("go guest module must compile");
        let mut rust_branch = rust_compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(5_000))),
                None,
            )
            .await
            .expect("rust guest branch must instantiate");
        let mut go_branch = go_compiled
            .instantiate_branch(
                init(),
                Box::new(FixedDomainClock::new(Timestamp::from_unix_nanos(5_000))),
                None,
            )
            .await
            .expect("go guest branch must instantiate");

        let mut go_values = Vec::new();
        for value in 1..=8 {
            let mut rust_outputs = rust_branch
                .process_envelope(&WasmBatchEnvelope::new(
                    sample_arrow_ipc(&[value]),
                    WasmAckSidecar {
                        rows: vec![row_ack(value as u64)],
                        acked: Vec::new(),
                        nacked: Vec::new(),
                        message_errors: Vec::new(),
                    },
                ))
                .await
                .expect("rust guest single-row batch must process");
            let rust_timeout = rust_branch
                .take_timeout_requests()
                .pop()
                .expect("rust guest should request flush timeout");
            rust_outputs.extend(
                rust_branch
                    .on_timeout(rust_timeout.handle)
                    .await
                    .expect("rust guest timeout should flush"),
            );

            for envelope in rust_outputs {
                let process_output = go_branch
                    .process_envelope(&envelope)
                    .await
                    .expect("go guest must process rust output envelope");
                for envelope in process_output {
                    go_values.extend(decode_arrow_ipc_values(&envelope.arrow_ipc_batch));
                }
                let Some(go_timeout) = go_branch.take_timeout_requests().pop() else {
                    continue;
                };
                let go_output = go_branch
                    .on_timeout(go_timeout.handle)
                    .await
                    .expect("go guest timeout should flush");
                for envelope in go_output {
                    go_values.extend(decode_arrow_ipc_values(&envelope.arrow_ipc_batch));
                }
            }
        }

        assert_eq!(go_values, vec![4, 8]);
    }

    #[tokio::test]
    async fn branch_instances_reuse_compiled_module_with_isolated_stores() {
        let runtime = runtime();
        let compiled = runtime
            .compile_processor(TEST_WASM)
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

        assert_eq!(left_out, vec![b"left batch".to_vec()]);
        assert_eq!(right_out, vec![b"right batch".to_vec()]);
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
        assert_eq!(returned, vec![b"stream me".to_vec()]);
        assert_eq!(
            receiver.recv().await.expect("emitted batch must arrive"),
            WasmBatchEnvelope::arrow_only(b"stream me".to_vec())
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

        assert_eq!(returned, vec![b"cpu".to_vec()]);
        assert!(
            ticks.load(Ordering::Relaxed) > 0,
            "epoch async yielding should allow another task to run during guest execution"
        );
    }

    #[tokio::test]
    async fn rust_guest_artifact_runs_in_isolation() {
        guest_flushes_on_timeout_and_periodic_boundary(&rust_guest_path()).await;
    }

    #[tokio::test]
    async fn go_guest_artifact_runs_in_isolation() {
        guest_flushes_on_timeout_and_periodic_boundary(&go_guest_path()).await;
    }

    #[tokio::test]
    async fn rust_guest_artifact_preserves_and_completes_ack_sidecar() {
        guest_preserves_and_completes_ack_sidecar(&rust_guest_path()).await;
    }

    #[tokio::test]
    async fn go_guest_artifact_preserves_and_completes_ack_sidecar() {
        guest_preserves_and_completes_ack_sidecar(&go_guest_path()).await;
    }

    #[tokio::test]
    async fn rust_guest_state_restoration_preserves_processed_row_count() {
        guest_state_restoration_preserves_processed_row_count(&rust_guest_path()).await;
    }

    #[tokio::test]
    async fn go_guest_state_restoration_preserves_processed_row_count() {
        guest_state_restoration_preserves_processed_row_count(&go_guest_path()).await;
    }
}
