use std::{cell::UnsafeCell, io::Cursor, panic::AssertUnwindSafe, sync::Arc};

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};

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

const SUCCESS: i32 = 0;
const ERR_INVALID_SIZE: i32 = -1;
const ERR_OUT_OF_BOUNDS: i32 = -2;
const ERR_NOT_INITIALIZED: i32 = -3;
const ERR_ARROW_IPC: i32 = -4;
const ERR_ENVELOPE: i32 = -5;
const ERR_ERROR_STATE: i32 = -6;
const DEFAULT_TIMEOUT_NANOS: i64 = 1_000_000_000;
const FLUSH_EVERY_BATCHES: u64 = 2;

#[link(wasm_import_module = "env")]
unsafe extern "C" {
    fn nervix_domain_time_nanos() -> i64;
    fn nervix_timeout_after_nanos(delay_nanos: i64) -> i64;
}

struct GuestState {
    buffer: Vec<u8>,
    init_metadata: Vec<u8>,
    pending_batch: Vec<u8>,
    pending_emit: Vec<Vec<u8>>,
    global_error: Vec<u8>,
    saved_state: Vec<u8>,
    input_schema: Option<ProcessorSchema>,
    output_schemas: Vec<ProcessorSchema>,
    pending_start_row: u64,
    initialized: bool,
    processed_batches: u64,
    processed_rows: u64,
    last_domain_time_nanos: i64,
    last_timeout_handle: i64,
    error_state: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct GuestSnapshot {
    processed_batches: u64,
    processed_rows: u64,
    pending_start_row: u64,
    last_domain_time_nanos: i64,
    last_timeout_handle: i64,
    pending_batch: Vec<u8>,
    init_metadata: Vec<u8>,
    saved_state: Vec<u8>,
    #[serde(default)]
    error_state: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Envelope {
    Input {
        #[serde(with = "cbor_byte_string")]
        arrow_ipc_batch: Vec<u8>,
        acks: AckSidecar,
    },
    Output {
        #[serde(with = "cbor_byte_string")]
        generated_arrow_ipc_batch: Vec<u8>,
        outputs: Vec<RoutedOutput>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutedOutput {
        output_relay: String,
        columns: Vec<OutputColumnRef>,
        acks: AckSidecar,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum OutputColumnRef {
    Generated {
        column_index: u32,
    },
    Input {
        column_index: u32,
    },
}

#[derive(Clone, Deserialize)]
struct BranchInitMetadata {
    input_schema: ProcessorSchema,
    output_schemas: Vec<ProcessorSchema>,
}

#[derive(Clone, Deserialize)]
struct ProcessorSchema {
    name: String,
    fields: Vec<ProcessorField>,
}

#[derive(Clone, Deserialize)]
struct ProcessorField {
    name: String,
    ty: ProcessorType,
    optional: bool,
}

#[derive(Clone, PartialEq, Eq, Deserialize)]
enum ProcessorType {
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
        element: Box<ProcessorType>,
        len: u32,
    },
    Vec {
        element: Box<ProcessorType>,
    },
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AckSidecar {
    rows: Vec<OutputRow>,
    acked: Vec<AckTokenSet>,
    nacked: Vec<NackSet>,
    message_errors: Vec<WasmMessageErrorSet>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutputRow {
    tokens: Vec<AckToken>,
    #[serde(deserialize_with = "deserialize_required_option")]
    source_token: Option<AckToken>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AckTokenSet {
    tokens: Vec<AckToken>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NackSet {
    tokens: Vec<AckToken>,
    reason: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WasmMessageErrorSet {
    tokens: Vec<AckToken>,
    reason: String,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
struct AckToken(u64);

impl GuestState {
    const fn new() -> Self {
        Self {
            buffer: Vec::new(),
            init_metadata: Vec::new(),
            pending_batch: Vec::new(),
            pending_emit: Vec::new(),
            global_error: Vec::new(),
            saved_state: Vec::new(),
            input_schema: None,
            output_schemas: Vec::new(),
            pending_start_row: 0,
            initialized: false,
            processed_batches: 0,
            processed_rows: 0,
            last_domain_time_nanos: 0,
            last_timeout_handle: 0,
            error_state: None,
        }
    }

    fn alloc(&mut self, size: usize) -> i32 {
        if self.buffer.capacity() < size {
            self.buffer.reserve_exact(size - self.buffer.capacity());
        }
        self.buffer.resize(size, 0);
        self.buffer.as_mut_ptr() as i32
    }

    fn read_memory(&self, ptr: i32, size: i32) -> Result<Vec<u8>, i32> {
        let ptr = usize::try_from(ptr).map_err(|_| ERR_OUT_OF_BOUNDS)?;
        let size = usize::try_from(size).map_err(|_| ERR_INVALID_SIZE)?;
        let end = ptr.checked_add(size).ok_or(ERR_OUT_OF_BOUNDS)?;
        let base = self.buffer.as_ptr() as usize;
        if ptr < base || end > base + self.buffer.len() {
            return Err(ERR_OUT_OF_BOUNDS);
        }

        let source = ptr as *const u8;
        let mut out = vec![0; size];
        // The checked range above guarantees these bytes sit inside the module's
        // current reusable buffer.
        unsafe {
            std::ptr::copy_nonoverlapping(source, out.as_mut_ptr(), size);
        }
        Ok(out)
    }

    fn flush_pending(&mut self) -> i32 {
        let envelope = match Envelope::decode(&self.pending_batch) {
            Ok(envelope) => envelope,
            Err(error) => return error,
        };
        let Some(input_schema) = self.input_schema.as_ref() else {
            return ERR_NOT_INITIALIZED;
        };
        match filter_envelope_by_global_row(
            envelope,
            self.pending_start_row,
            input_schema,
            &self.output_schemas,
        ) {
            Ok(filtered) => {
                for envelope in filtered {
                    match envelope.encode() {
                        Ok(encoded) => self.pending_emit.push(encoded),
                        Err(error) => return error,
                    }
                }
            }
            Err(error) => return error,
        }
        self.pending_batch.clear();
        self.pending_start_row = self.processed_rows;
        SUCCESS
    }

    fn dump_state(&mut self) -> i32 {
        let snapshot = GuestSnapshot {
            processed_batches: self.processed_batches,
            processed_rows: self.processed_rows,
            pending_start_row: self.pending_start_row,
            last_domain_time_nanos: self.last_domain_time_nanos,
            last_timeout_handle: self.last_timeout_handle,
            pending_batch: self.pending_batch.clone(),
            init_metadata: self.init_metadata.clone(),
            saved_state: self.saved_state.clone(),
            error_state: self.error_state.clone(),
        };

        self.buffer.clear();
        if ciborium::into_writer(&snapshot, &mut self.buffer).is_err() {
            return ERR_INVALID_SIZE;
        }
        self.buffer.len() as i32
    }

    fn reset(&mut self) {
        self.init_metadata.clear();
        self.pending_batch.clear();
        self.pending_emit.clear();
        self.global_error.clear();
        self.saved_state.clear();
        self.input_schema = None;
        self.output_schemas.clear();
        self.pending_start_row = 0;
        self.initialized = false;
        self.processed_batches = 0;
        self.processed_rows = 0;
        self.last_domain_time_nanos = 0;
        self.last_timeout_handle = 0;
        self.error_state = None;
    }

    fn set_global_error(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        self.error_state = Some(reason.clone());
        self.global_error.clear();
        self.global_error.extend_from_slice(reason.as_bytes());
    }

    fn load_state_bytes(&mut self, saved_state: Vec<u8>) -> i32 {
        let Ok(snapshot) = ciborium::from_reader::<GuestSnapshot, _>(Cursor::new(saved_state))
        else {
            return ERR_INVALID_SIZE;
        };

        self.processed_batches = snapshot.processed_batches;
        self.processed_rows = snapshot.processed_rows;
        self.pending_start_row = snapshot.pending_start_row;
        self.last_domain_time_nanos = snapshot.last_domain_time_nanos;
        self.last_timeout_handle = snapshot.last_timeout_handle;
        self.pending_batch = snapshot.pending_batch;
        self.init_metadata = snapshot.init_metadata;
        let Ok(metadata) = schemas_from_init_metadata(&self.init_metadata) else {
            return ERR_INVALID_SIZE;
        };
        if !self.pending_batch.is_empty() && Envelope::decode(&self.pending_batch).is_err() {
            return ERR_INVALID_SIZE;
        }
        self.input_schema = Some(metadata.input_schema);
        self.output_schemas = metadata.output_schemas;
        self.saved_state = snapshot.saved_state;
        self.error_state = snapshot.error_state;
        SUCCESS
    }
}

impl Envelope {
    fn encode(&self) -> Result<Vec<u8>, i32> {
        let mut encoded = Vec::new();
        ciborium::into_writer(self, &mut encoded).map_err(|_| ERR_ENVELOPE)?;
        Ok(encoded)
    }

    fn decode(bytes: &[u8]) -> Result<Self, i32> {
        let mut cursor = Cursor::new(bytes);
        let envelope = ciborium::from_reader(&mut cursor).map_err(|_| ERR_ENVELOPE)?;
        if usize::try_from(cursor.position()).map_err(|_| ERR_ENVELOPE)? != bytes.len() {
            return Err(ERR_ENVELOPE);
        }
        Ok(envelope)
    }
}

struct Global<T>(UnsafeCell<T>);

unsafe impl<T> Sync for Global<T> {}

static STATE: Global<GuestState> = Global(UnsafeCell::new(GuestState::new()));

impl Global<GuestState> {
    fn with_mut<R>(&self, f: impl FnOnce(&mut GuestState) -> R) -> R {
        let state = unsafe { &mut *self.0.get() };
        f(state)
    }

    fn guarded_export(&self, f: impl FnOnce(&mut GuestState) -> i32) -> i32 {
        self.guarded_state_export(true, f)
    }

    fn guarded_state_export(
        &self,
        check_error_state: bool,
        f: impl FnOnce(&mut GuestState) -> i32,
    ) -> i32 {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.with_mut(|state| {
                if check_error_state {
                    if let Some(error_state) = state.error_state.clone() {
                        if state.global_error.is_empty() {
                            state.global_error.extend_from_slice(error_state.as_bytes());
                        }
                        return ERR_ERROR_STATE;
                    }
                }
                f(state)
            })
        }));
        match result {
            Ok(code) => code,
            Err(payload) => self.with_mut(|state| {
                state.set_global_error(panic_reason(payload));
                ERR_ERROR_STATE
            }),
        }
    }
}

fn panic_reason(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(reason) = payload.downcast_ref::<&str>() {
        format!("guest panic: {reason}")
    } else if let Some(reason) = payload.downcast_ref::<String>() {
        format!("guest panic: {reason}")
    } else {
        "guest panic".to_string()
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_buffer_ptr() -> i32 {
    STATE.with_mut(|state| state.buffer.as_mut_ptr() as i32)
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_buffer_len() -> i32 {
    STATE.with_mut(|state| state.buffer.len() as i32)
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_buffer_capacity() -> i32 {
    STATE.with_mut(|state| state.buffer.capacity() as i32)
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_global_error_ptr() -> i32 {
    STATE.with_mut(|state| {
        if state.global_error.is_empty() {
            0
        } else {
            state.global_error.as_mut_ptr() as i32
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_global_error_len() -> i32 {
    STATE.with_mut(|state| state.global_error.len() as i32)
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_clear_global_error() -> i32 {
    STATE.with_mut(|state| {
        state.global_error.clear();
        SUCCESS
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_alloc(size: i32) -> i32 {
    let Ok(size) = usize::try_from(size) else {
        return ERR_INVALID_SIZE;
    };
    STATE.with_mut(|state| state.alloc(size))
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_init(ptr: i32, size: i32) -> i32 {
    STATE.guarded_export(|state| match state.read_memory(ptr, size) {
        Ok(metadata) => {
            let schemas = match schemas_from_init_metadata(&metadata) {
                Ok(schemas) => schemas,
                Err(error) => return error,
            };
            state.init_metadata = metadata;
            state.input_schema = Some(schemas.input_schema);
            state.output_schemas = schemas.output_schemas;
            state.initialized = true;
            SUCCESS
        }
        Err(error) => error,
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_current_domain_time_nanos() -> i64 {
    let now = unsafe { nervix_domain_time_nanos() };
    STATE.with_mut(|state| {
        state.last_domain_time_nanos = now;
    });
    now
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_process_batch(size: i32) -> i32 {
    let Ok(size) = usize::try_from(size) else {
        return ERR_INVALID_SIZE;
    };

    STATE.guarded_export(|state| {
        if !state.initialized {
            return ERR_NOT_INITIALIZED;
        }
        if size > state.buffer.len() {
            return ERR_OUT_OF_BOUNDS;
        }

        state.processed_batches += 1;
        state.last_domain_time_nanos = unsafe { nervix_domain_time_nanos() };
        state.last_timeout_handle = unsafe { nervix_timeout_after_nanos(DEFAULT_TIMEOUT_NANOS) };
        let envelope = match Envelope::decode(&state.buffer[..size]) {
            Ok(envelope) => envelope,
            Err(error) => return error,
        };
        let Envelope::Input {
            arrow_ipc_batch,
            acks,
        } = envelope
        else {
            return ERR_ENVELOPE;
        };
        match first_i32_value(&arrow_ipc_batch) {
            Ok(Some(-300)) => {
                state.set_global_error("guest error state for value -300");
                return ERR_ERROR_STATE;
            }
            Ok(Some(-200)) => {
                state.set_global_error("guest global error for value -200");
                return SUCCESS;
            }
            Ok(Some(-100)) => {
                let Some(input_schema) = state.input_schema.as_ref() else {
                    return ERR_NOT_INITIALIZED;
                };
                let Some(output_schema) = state.output_schemas.first() else {
                    return ERR_NOT_INITIALIZED;
                };
                let encoded = match message_error_envelope(
                    input_schema,
                    output_schema,
                    acks,
                    "guest message error for value -100".to_string(),
                ) {
                    Ok(envelope) => match envelope.encode() {
                        Ok(encoded) => encoded,
                        Err(error) => return error,
                    },
                    Err(error) => return error,
                };
                state.pending_emit.clear();
                state.pending_emit.push(encoded);
                return SUCCESS;
            }
            Ok(_) => {}
            Err(error) => return error,
        }
        state.pending_emit.clear();
        if !state.pending_batch.is_empty() {
            let result = state.flush_pending();
            if result != SUCCESS {
                return result;
            }
        }
        let row_count = match arrow_ipc_row_count(&arrow_ipc_batch) {
            Ok(row_count) => row_count,
            Err(error) => return error,
        };
        state.pending_start_row = state.processed_rows;
        state.processed_rows = state.processed_rows.saturating_add(row_count);
        state.pending_batch.clear();
        let pending = Envelope::Input {
            arrow_ipc_batch,
            acks,
        };
        let pending = match pending.encode() {
            Ok(pending) => pending,
            Err(error) => return error,
        };
        state.pending_batch.extend_from_slice(&pending);

        if state.processed_batches % FLUSH_EVERY_BATCHES == 0 {
            return state.flush_pending();
        }
        SUCCESS
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_on_timeout(handle: i64) -> i32 {
    STATE.guarded_export(|state| {
        state.last_timeout_handle = handle;
        if state.pending_batch.is_empty() {
            SUCCESS
        } else {
            state.pending_emit.clear();
            state.flush_pending()
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_read_emit() -> i32 {
    STATE.guarded_export(|state| {
        if state.pending_emit.is_empty() {
            return 0;
        }
        state.buffer.clear();
        state.buffer.extend_from_slice(&state.pending_emit.remove(0));
        state.buffer.len() as i32
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_dump_state() -> i32 {
    STATE.guarded_state_export(false, GuestState::dump_state)
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_load_state(ptr: i32, size: i32) -> i32 {
    STATE.guarded_state_export(false, |state| match state.read_memory(ptr, size) {
        Ok(saved_state) => state.load_state_bytes(saved_state),
        Err(error) => error,
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_reset_state() -> i32 {
    STATE.with_mut(|state| {
        state.reset();
        SUCCESS
    })
}

fn arrow_ipc_row_count(bytes: &[u8]) -> Result<u64, i32> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|_| ERR_ARROW_IPC)?;
    let mut rows = 0_u64;
    for batch in reader {
        let batch = batch.map_err(|_| ERR_ARROW_IPC)?;
        rows = rows.saturating_add(batch.num_rows() as u64);
    }
    Ok(rows)
}

fn filter_envelope_by_global_row(
    envelope: Envelope,
    start_row: u64,
    input_schema: &ProcessorSchema,
    output_schemas: &[ProcessorSchema],
) -> Result<Vec<Envelope>, i32> {
    let Envelope::Input {
        arrow_ipc_batch,
        acks,
    } = envelope
    else {
        return Err(ERR_ENVELOPE);
    };
    let reader = StreamReader::try_new(Cursor::new(&arrow_ipc_batch), None)
        .map_err(|_| ERR_ARROW_IPC)?;
    if output_schemas.is_empty() {
        return Err(ERR_NOT_INITIALIZED);
    }
    let mut selected_values = Vec::new();
    let mut selected_acks = Vec::new();
    let mut acked = acks.acked;
    let mut next_row = start_row;
    let mut input_row = 0_usize;
    for batch in reader {
        let batch = batch.map_err(|_| ERR_ARROW_IPC)?;
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or(ERR_ARROW_IPC)?;
        for row in 0..values.len() {
            next_row = next_row.saturating_add(1);
            if next_row.is_multiple_of(2) && values.is_valid(row) {
                selected_values.push(values.value(row));
                selected_acks.push(acks.rows.get(input_row).cloned().unwrap_or_default());
            } else if let Some(ack) = acks.rows.get(input_row) {
                acked.push(AckTokenSet {
                    tokens: ack.tokens.clone(),
                });
            }
            input_row += 1;
        }
    }

    let generated_fields = generated_fields(input_schema, output_schemas);
    let generated_arrow_ipc_batch = generated_batch_ipc(&generated_fields, &selected_values)?;
    let outputs = output_schemas
        .iter()
        .enumerate()
        .map(|(index, output_schema)| {
            Ok(RoutedOutput {
                output_relay: output_schema.name.clone(),
                columns: output_columns(input_schema, output_schema, &generated_fields)?,
                acks: AckSidecar {
                    rows: selected_acks.clone(),
                    acked: if index == 0 { acked.clone() } else { Vec::new() },
                    nacked: if index == 0 {
                        acks.nacked.clone()
                    } else {
                        Vec::new()
                    },
                    message_errors: if index == 0 {
                        acks.message_errors.clone()
                    } else {
                        Vec::new()
                    },
                },
            })
        })
        .collect::<Result<Vec<_>, i32>>()?;
    Ok(vec![Envelope::Output {
        generated_arrow_ipc_batch,
        outputs,
    }])
}

fn schemas_from_init_metadata(metadata: &[u8]) -> Result<BranchInitMetadata, i32> {
    ciborium::from_reader(Cursor::new(metadata)).map_err(|_| ERR_INVALID_SIZE)
}

fn output_columns(
    input_schema: &ProcessorSchema,
    output_schema: &ProcessorSchema,
    generated_fields: &[ProcessorField],
) -> Result<Vec<OutputColumnRef>, i32> {
    output_schema
        .fields
        .iter()
        .map(|destination| {
            if let Some((column_index, _)) = input_schema
                .fields
                .iter()
                .enumerate()
                .find(|(_, source)| {
                    source.name == destination.name
                        && source.ty == destination.ty
                        && source.optional == destination.optional
                })
            {
                return Ok(OutputColumnRef::Input {
                    column_index: u32::try_from(column_index).map_err(|_| ERR_ENVELOPE)?,
                });
            }
            let column_index = generated_fields
                .iter()
                .position(|field| {
                    field.ty == destination.ty && field.optional == destination.optional
                })
                .ok_or(ERR_ENVELOPE)?;
            Ok(OutputColumnRef::Generated {
                column_index: u32::try_from(column_index).map_err(|_| ERR_ENVELOPE)?,
            })
        })
        .collect()
}

fn generated_fields(
    input_schema: &ProcessorSchema,
    output_schemas: &[ProcessorSchema],
) -> Vec<ProcessorField> {
    let mut generated = Vec::<ProcessorField>::new();
    for destination in output_schemas.iter().flat_map(|schema| &schema.fields) {
        let is_input = input_schema.fields.iter().any(|source| {
            source.name == destination.name
                && source.ty == destination.ty
                && source.optional == destination.optional
        });
        if !is_input
            && !generated.iter().any(|field| {
                field.ty == destination.ty && field.optional == destination.optional
            })
        {
            generated.push(destination.clone());
        }
    }
    generated
}

fn generated_batch_ipc(
    fields: &[ProcessorField],
    selected_values: &[i32],
) -> Result<Vec<u8>, i32> {
    if fields.is_empty() {
        return Ok(Vec::new());
    }
    let arrow_fields = fields
        .iter()
        .map(|field| Ok(Field::new("", field_arrow_type(&field.ty)?, field.optional)))
        .collect::<Result<Vec<_>, i32>>()?;
    let arrays = fields
        .iter()
        .map(|field| generated_output_column(field, selected_values))
        .collect::<Result<Vec<_>, _>>()?;
    let schema = Arc::new(Schema::new(arrow_fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).map_err(|_| ERR_ARROW_IPC)?;
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &schema).map_err(|_| ERR_ARROW_IPC)?;
        writer.write(&batch).map_err(|_| ERR_ARROW_IPC)?;
        writer.finish().map_err(|_| ERR_ARROW_IPC)?;
    }
    Ok(ipc)
}

fn field_arrow_type(ty: &ProcessorType) -> Result<DataType, i32> {
    match ty {
        ProcessorType::I32 => Ok(DataType::Int32),
        ProcessorType::String => Ok(DataType::Utf8),
        ProcessorType::Array { element, len } => {
            let _ = (element.as_ref(), len);
            Err(ERR_ARROW_IPC)
        }
        ProcessorType::Vec { element } => {
            let _ = element.as_ref();
            Err(ERR_ARROW_IPC)
        }
        _ => Err(ERR_ARROW_IPC),
    }
}

fn generated_output_column(
    field: &ProcessorField,
    selected_values: &[i32],
) -> Result<Arc<dyn Array>, i32> {
    match &field.ty {
        ProcessorType::I32 if field.name == "value" => {
            Ok(Arc::new(Int32Array::from(selected_values.to_vec())))
        }
        ProcessorType::I32 if field.optional => Ok(Arc::new(Int32Array::from(
            vec![None; selected_values.len()],
        ))),
        ProcessorType::String if !field.optional => {
            let values = selected_values
                .iter()
                .map(|_| Some("EVEN"))
                .collect::<Vec<_>>();
            Ok(Arc::new(StringArray::from(values)))
        }
        ProcessorType::String if field.optional => {
            let values = vec![None::<&str>; selected_values.len()];
            Ok(Arc::new(StringArray::from(values)))
        }
        _ => Err(ERR_ARROW_IPC),
    }
}

fn first_i32_value(bytes: &[u8]) -> Result<Option<i32>, i32> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(|_| ERR_ARROW_IPC)?;
    for batch in reader {
        let batch = batch.map_err(|_| ERR_ARROW_IPC)?;
        if batch.num_rows() == 0 {
            continue;
        }
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or(ERR_ARROW_IPC)?;
        if values.is_valid(0) {
            return Ok(Some(values.value(0)));
        }
    }
    Ok(None)
}

fn message_error_envelope(
    input_schema: &ProcessorSchema,
    output_schema: &ProcessorSchema,
    acks: AckSidecar,
    reason: String,
) -> Result<Envelope, i32> {
    let tokens = acks
        .rows
        .first()
        .map(|row| row.tokens.clone())
        .unwrap_or_default();
    let generated_fields = generated_fields(input_schema, std::slice::from_ref(output_schema));
    Ok(Envelope::Output {
        generated_arrow_ipc_batch: generated_batch_ipc(&generated_fields, &[])?,
        outputs: vec![RoutedOutput {
            output_relay: output_schema.name.clone(),
            columns: output_columns(input_schema, output_schema, &generated_fields)?,
            acks: AckSidecar {
                rows: Vec::new(),
                acked: acks.acked,
                nacked: acks.nacked,
                message_errors: vec![WasmMessageErrorSet { tokens, reason }],
            },
        }],
    })
}
