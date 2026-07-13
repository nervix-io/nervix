use std::{
    io::Cursor,
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

mod cbor_byte_string {
    use std::fmt;

    use serde::{Deserializer, Serializer, de::Visitor};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde::Serialize::serialize(serde_bytes::Bytes::new(bytes), serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(ByteStringVisitor)
    }

    struct ByteStringVisitor;

    impl Visitor<'_> for ByteStringVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a CBOR byte string")
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_vec())
        }

        fn visit_borrowed_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value.to_vec())
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value)
        }
    }
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

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
    #[error("failed to encode branch init metadata as CBOR: {0}")]
    EncodeInit(#[source] ciborium::ser::Error<std::io::Error>),
    #[error(transparent)]
    Protocol(#[from] WasmProtocolError),
    #[error("guest global error bytes are not valid UTF-8: {0}")]
    InvalidGuestGlobalError(#[source] std::string::FromUtf8Error),
    #[error("guest buffer size {size} exceeds configured limit {limit}")]
    GuestBufferTooLarge { size: usize, limit: usize },
}

#[derive(Debug, Error)]
pub enum WasmProtocolError {
    #[error("failed to encode WASM envelope as CBOR: {0}")]
    EncodeEnvelope(#[source] ciborium::ser::Error<std::io::Error>),
    #[error("failed to decode WASM envelope from CBOR: {0}")]
    DecodeEnvelope(#[source] ciborium::de::Error<std::io::Error>),
    #[error("WASM envelope has {remaining} trailing bytes")]
    TrailingEnvelopeBytes { remaining: usize },
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
#[serde(deny_unknown_fields)]
pub struct WasmAckTokenSet {
    pub tokens: Vec<WasmAckToken>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasmOutputRow {
    pub tokens: Vec<WasmAckToken>,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub source_token: Option<WasmAckToken>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasmNackSet {
    pub tokens: Vec<WasmAckToken>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasmMessageErrorSet {
    pub tokens: Vec<WasmAckToken>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasmAckSidecar {
    pub rows: Vec<WasmOutputRow>,
    pub acked: Vec<WasmAckTokenSet>,
    pub nacked: Vec<WasmNackSet>,
    pub message_errors: Vec<WasmMessageErrorSet>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WasmEnvelope {
    Input {
        #[serde(with = "cbor_byte_string")]
        arrow_ipc_batch: Vec<u8>,
        acks: WasmAckSidecar,
    },
    Output {
        output_relay: String,
        columns: Vec<WasmOutputColumn>,
        acks: WasmAckSidecar,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WasmOutputColumn {
    GuestArrow {
        #[serde(with = "cbor_byte_string")]
        ipc: Vec<u8>,
    },
    Input {
        column_index: u32,
    },
}

impl WasmOutputColumn {
    pub const fn is_input(&self) -> bool {
        if let Self::Input { .. } = self {
            true
        } else {
            false
        }
    }
}

impl WasmEnvelope {
    pub const fn acks(&self) -> &WasmAckSidecar {
        match self {
            Self::Input { acks, .. } | Self::Output { acks, .. } => acks,
        }
    }

    pub fn input(arrow_ipc_batch: Vec<u8>, acks: WasmAckSidecar) -> Self {
        Self::Input {
            arrow_ipc_batch,
            acks,
        }
    }

    pub fn output(
        output_relay: impl Into<String>,
        columns: Vec<WasmOutputColumn>,
        acks: WasmAckSidecar,
    ) -> Self {
        Self::Output {
            output_relay: output_relay.into(),
            columns,
            acks,
        }
    }

    pub fn input_arrow_only(arrow_ipc_batch: Vec<u8>) -> Self {
        Self::Input {
            arrow_ipc_batch,
            acks: WasmAckSidecar::default(),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, WasmProtocolError> {
        let mut encoded = Vec::new();
        ciborium::into_writer(self, &mut encoded).map_err(WasmProtocolError::EncodeEnvelope)?;
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, WasmProtocolError> {
        let mut cursor = Cursor::new(bytes);
        let envelope =
            ciborium::from_reader(&mut cursor).map_err(WasmProtocolError::DecodeEnvelope)?;
        let consumed = usize::try_from(cursor.position()).unwrap_or(usize::MAX);
        if consumed != bytes.len() {
            return Err(WasmProtocolError::TrailingEnvelopeBytes {
                remaining: bytes.len().saturating_sub(consumed),
            });
        }
        Ok(envelope)
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
            let batch = WasmEnvelope::decode(&self.read_guest_buffer(size).await?)
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
    fn input_envelope_cbor_round_trip_uses_byte_string_for_arrow_ipc() {
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
        let value: ciborium::Value =
            ciborium::from_reader(encoded.as_slice()).expect("encoded envelope must be CBOR");
        let ciborium::Value::Map(fields) = value else {
            panic!("envelope must encode as a CBOR map");
        };
        let arrow = fields
            .into_iter()
            .find_map(|(key, value)| {
                (key == ciborium::Value::Text("arrow_ipc_batch".into())).then_some(value)
            })
            .expect("input envelope must contain arrow_ipc_batch");
        assert_eq!(arrow, ciborium::Value::Bytes(vec![0, 1, 2, 255]));
    }

    #[test]
    fn mixed_output_envelope_cbor_round_trip() {
        let envelope = WasmEnvelope::output(
            "enriched_events",
            vec![
                WasmOutputColumn::Input { column_index: 0 },
                WasmOutputColumn::GuestArrow { ipc: vec![9, 8, 7] },
            ],
            WasmAckSidecar {
                rows: vec![output_row(11)],
                acked: vec![token_set(12)],
                nacked: Vec::new(),
                message_errors: Vec::new(),
            },
        );

        let encoded = envelope.encode().expect("output envelope must encode");
        assert_eq!(
            WasmEnvelope::decode(&encoded).expect("output envelope must decode"),
            envelope
        );
    }

    #[test]
    fn envelope_decode_rejects_unknown_missing_and_trailing_data() {
        let unknown_field = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("kind".into()),
                ciborium::Value::Text("input".into()),
            ),
            (
                ciborium::Value::Text("arrow_ipc_batch".into()),
                ciborium::Value::Bytes(Vec::new()),
            ),
            (
                ciborium::Value::Text("acks".into()),
                empty_ack_sidecar_value(),
            ),
            (ciborium::Value::Text("extra".into()), ciborium::Value::Null),
        ]);
        let missing_field = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("kind".into()),
                ciborium::Value::Text("input".into()),
            ),
            (
                ciborium::Value::Text("acks".into()),
                empty_ack_sidecar_value(),
            ),
        ]);
        let unknown_kind = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("kind".into()),
                ciborium::Value::Text("legacy".into()),
            ),
            (
                ciborium::Value::Text("acks".into()),
                empty_ack_sidecar_value(),
            ),
        ]);
        let integer_array_arrow_payload = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("kind".into()),
                ciborium::Value::Text("input".into()),
            ),
            (
                ciborium::Value::Text("arrow_ipc_batch".into()),
                ciborium::Value::Array(vec![ciborium::Value::Integer(1.into())]),
            ),
            (
                ciborium::Value::Text("acks".into()),
                empty_ack_sidecar_value(),
            ),
        ]);
        let unknown_column_kind = output_envelope_value(ciborium::Value::Map(vec![(
            ciborium::Value::Text("kind".into()),
            ciborium::Value::Text("legacy".into()),
        )]));
        let malformed_ack_sidecar = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("kind".into()),
                ciborium::Value::Text("input".into()),
            ),
            (
                ciborium::Value::Text("arrow_ipc_batch".into()),
                ciborium::Value::Bytes(Vec::new()),
            ),
            (
                ciborium::Value::Text("acks".into()),
                ciborium::Value::Map(vec![
                    (
                        ciborium::Value::Text("rows".into()),
                        ciborium::Value::Array(vec![ciborium::Value::Map(vec![(
                            ciborium::Value::Text("tokens".into()),
                            ciborium::Value::Array(Vec::new()),
                        )])]),
                    ),
                    (
                        ciborium::Value::Text("acked".into()),
                        ciborium::Value::Array(Vec::new()),
                    ),
                    (
                        ciborium::Value::Text("nacked".into()),
                        ciborium::Value::Array(Vec::new()),
                    ),
                    (
                        ciborium::Value::Text("message_errors".into()),
                        ciborium::Value::Array(Vec::new()),
                    ),
                ]),
            ),
        ]);
        for (case, value) in [
            ("unknown field", unknown_field),
            ("missing field", missing_field),
            ("unknown kind", unknown_kind),
            ("integer-array Arrow payload", integer_array_arrow_payload),
            ("unknown column kind", unknown_column_kind),
            ("malformed ACK sidecar", malformed_ack_sidecar),
        ] {
            let mut encoded = Vec::new();
            ciborium::into_writer(&value, &mut encoded).expect("test CBOR must encode");
            assert!(
                WasmEnvelope::decode(&encoded).is_err(),
                "{case} must be rejected"
            );
        }

        let mut trailing = WasmEnvelope::input_arrow_only(Vec::new())
            .encode()
            .expect("input envelope must encode");
        trailing.push(0);
        assert!(matches!(
            WasmEnvelope::decode(&trailing),
            Err(WasmProtocolError::TrailingEnvelopeBytes { remaining: 1 })
        ));
    }

    fn empty_ack_sidecar_value() -> ciborium::Value {
        ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("rows".into()),
                ciborium::Value::Array(Vec::new()),
            ),
            (
                ciborium::Value::Text("acked".into()),
                ciborium::Value::Array(Vec::new()),
            ),
            (
                ciborium::Value::Text("nacked".into()),
                ciborium::Value::Array(Vec::new()),
            ),
            (
                ciborium::Value::Text("message_errors".into()),
                ciborium::Value::Array(Vec::new()),
            ),
        ])
    }

    fn output_envelope_value(column: ciborium::Value) -> ciborium::Value {
        ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("kind".into()),
                ciborium::Value::Text("output".into()),
            ),
            (
                ciborium::Value::Text("output_relay".into()),
                ciborium::Value::Text("events".into()),
            ),
            (
                ciborium::Value::Text("columns".into()),
                ciborium::Value::Array(vec![column]),
            ),
            (
                ciborium::Value::Text("acks".into()),
                empty_ack_sidecar_value(),
            ),
        ])
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

    async fn guest_emits_cbor_input_reference_output(path: &Path) {
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
            output_relay,
            columns,
            acks,
        } = &output[0]
        else {
            panic!("guest must emit an output envelope");
        };
        assert_eq!(output_relay, "output_events");
        assert_eq!(columns, &[WasmOutputColumn::Input { column_index: 0 }]);
        assert_eq!(acks.rows, vec![output_row(20), output_row(40)]);
        assert_eq!(acks.acked, vec![token_set(10), token_set(30)]);
    }

    #[tokio::test]
    async fn rust_guest_interoperates_with_host_cbor_format() {
        guest_emits_cbor_input_reference_output(&rust_guest_path()).await;
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
        let WasmEnvelope::Output { columns, .. } = &outputs[0] else {
            panic!("guest must emit an output envelope");
        };
        assert_eq!(
            columns,
            &[
                WasmOutputColumn::Input { column_index: 0 },
                WasmOutputColumn::Input { column_index: 1 },
            ]
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
    async fn go_guest_interoperates_with_host_cbor_format() {
        guest_emits_cbor_input_reference_output(&go_guest_path()).await;
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
