use std::{cell::UnsafeCell, io::Cursor, panic::AssertUnwindSafe, sync::Arc};

use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use serde::{Deserialize, Serialize};

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
    pending_emit: Vec<u8>,
    global_error: Vec<u8>,
    saved_state: Vec<u8>,
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

#[derive(Clone)]
struct BatchEnvelope {
    arrow_ipc_batch: Vec<u8>,
    acks: AckSidecar,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct AckSidecar {
    #[serde(default)]
    rows: Vec<RowAckSet>,
    #[serde(default)]
    acked: Vec<RowAckSet>,
    #[serde(default)]
    nacked: Vec<NackSet>,
    #[serde(default)]
    message_errors: Vec<WasmMessageErrorSet>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct RowAckSet {
    tokens: Vec<AckToken>,
}

#[derive(Clone, Serialize, Deserialize)]
struct NackSet {
    tokens: Vec<AckToken>,
    reason: String,
}

#[derive(Clone, Serialize, Deserialize)]
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
        let envelope = match BatchEnvelope::decode(&self.pending_batch) {
            Ok(envelope) => envelope,
            Err(error) => return error,
        };
        match filter_envelope_by_global_row(envelope, self.pending_start_row) {
            Ok(filtered) => match filtered.encode() {
                Ok(encoded) => self.pending_emit = encoded,
                Err(error) => return error,
            },
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
        self.saved_state = snapshot.saved_state;
        self.error_state = snapshot.error_state;
        SUCCESS
    }
}

impl BatchEnvelope {
    fn encode(&self) -> Result<Vec<u8>, i32> {
        let arrow_len = u32::try_from(self.arrow_ipc_batch.len()).map_err(|_| ERR_ENVELOPE)?;
        let mut ack_bytes = Vec::new();
        ciborium::into_writer(&self.acks, &mut ack_bytes).map_err(|_| ERR_ENVELOPE)?;
        let ack_len = u32::try_from(ack_bytes.len()).map_err(|_| ERR_ENVELOPE)?;
        let mut encoded = Vec::with_capacity(8 + self.arrow_ipc_batch.len() + ack_bytes.len());
        encoded.extend_from_slice(&arrow_len.to_le_bytes());
        encoded.extend_from_slice(&self.arrow_ipc_batch);
        encoded.extend_from_slice(&ack_len.to_le_bytes());
        encoded.extend_from_slice(&ack_bytes);
        Ok(encoded)
    }

    fn decode(bytes: &[u8]) -> Result<Self, i32> {
        if bytes.len() < 8 {
            return Err(ERR_ENVELOPE);
        }
        let arrow_len = read_u32_len(bytes, 0)?;
        let arrow_start = 4_usize;
        let arrow_end = arrow_start.checked_add(arrow_len).ok_or(ERR_ENVELOPE)?;
        if arrow_end.checked_add(4).ok_or(ERR_ENVELOPE)? > bytes.len() {
            return Err(ERR_ENVELOPE);
        }
        let ack_len = read_u32_len(bytes, arrow_end)?;
        let ack_start = arrow_end + 4;
        let ack_end = ack_start.checked_add(ack_len).ok_or(ERR_ENVELOPE)?;
        if ack_end != bytes.len() {
            return Err(ERR_ENVELOPE);
        }
        let acks = ciborium::from_reader(&bytes[ack_start..ack_end]).map_err(|_| ERR_ENVELOPE)?;
        Ok(Self {
            arrow_ipc_batch: bytes[arrow_start..arrow_end].to_vec(),
            acks,
        })
    }
}

fn read_u32_len(bytes: &[u8], offset: usize) -> Result<usize, i32> {
    let end = offset.checked_add(4).ok_or(ERR_ENVELOPE)?;
    let raw = bytes.get(offset..end).ok_or(ERR_ENVELOPE)?;
    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize)
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
            state.init_metadata = metadata;
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
        let envelope = match BatchEnvelope::decode(&state.buffer[..size]) {
            Ok(envelope) => envelope,
            Err(error) => return error,
        };
        match first_i32_value(&envelope.arrow_ipc_batch) {
            Ok(Some(-300)) => {
                state.set_global_error("guest error state for value -300");
                return ERR_ERROR_STATE;
            }
            Ok(Some(-200)) => {
                state.set_global_error("guest global error for value -200");
                return SUCCESS;
            }
            Ok(Some(-100)) => {
                let encoded = match message_error_envelope(
                    envelope,
                    "guest message error for value -100".to_string(),
                ) {
                    Ok(envelope) => match envelope.encode() {
                        Ok(encoded) => encoded,
                        Err(error) => return error,
                    },
                    Err(error) => return error,
                };
                state.pending_emit = encoded;
                return SUCCESS;
            }
            Ok(_) => {}
            Err(error) => return error,
        }
        let row_count = match arrow_ipc_row_count(&envelope.arrow_ipc_batch) {
            Ok(row_count) => row_count,
            Err(error) => return error,
        };
        state.pending_start_row = state.processed_rows;
        state.processed_rows = state.processed_rows.saturating_add(row_count);
        state.pending_batch.clear();
        state.pending_batch.extend_from_slice(&state.buffer[..size]);

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
        state.buffer.extend_from_slice(&state.pending_emit);
        state.pending_emit.clear();
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
    envelope: BatchEnvelope,
    start_row: u64,
) -> Result<BatchEnvelope, i32> {
    let reader = StreamReader::try_new(Cursor::new(&envelope.arrow_ipc_batch), None)
        .map_err(|_| ERR_ARROW_IPC)?;
    let schema = reader.schema();
    let mut output_batches = Vec::new();
    let mut output_acks = AckSidecar {
        rows: Vec::new(),
        acked: envelope.acks.acked,
        nacked: envelope.acks.nacked,
        message_errors: envelope.acks.message_errors,
    };
    let mut next_row = start_row;
    let mut input_row = 0_usize;
    for batch in reader {
        let batch = batch.map_err(|_| ERR_ARROW_IPC)?;
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or(ERR_ARROW_IPC)?;
        let mut filtered = Vec::new();
        for row in 0..values.len() {
            next_row = next_row.saturating_add(1);
            if next_row.is_multiple_of(2) && values.is_valid(row) {
                filtered.push(values.value(row));
                output_acks.rows.push(
                    envelope
                        .acks
                        .rows
                        .get(input_row)
                        .cloned()
                        .unwrap_or_default(),
                );
            } else if let Some(ack) = envelope.acks.rows.get(input_row) {
                output_acks.acked.push(ack.clone());
            }
            input_row += 1;
        }
        let column: Arc<dyn Array> = Arc::new(Int32Array::from(filtered));
        output_batches
            .push(RecordBatch::try_new(schema.clone(), vec![column]).map_err(|_| ERR_ARROW_IPC)?);
    }

    let mut output = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output, &schema).map_err(|_| ERR_ARROW_IPC)?;
        for batch in output_batches {
            writer.write(&batch).map_err(|_| ERR_ARROW_IPC)?;
        }
        writer.finish().map_err(|_| ERR_ARROW_IPC)?;
    }
    Ok(BatchEnvelope {
        arrow_ipc_batch: output,
        acks: output_acks,
    })
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

fn message_error_envelope(envelope: BatchEnvelope, reason: String) -> Result<BatchEnvelope, i32> {
    let reader = StreamReader::try_new(Cursor::new(&envelope.arrow_ipc_batch), None)
        .map_err(|_| ERR_ARROW_IPC)?;
    let schema = reader.schema();
    let empty_column: Arc<dyn Array> = Arc::new(Int32Array::from(Vec::<i32>::new()));
    let empty_batch =
        RecordBatch::try_new(schema.clone(), vec![empty_column]).map_err(|_| ERR_ARROW_IPC)?;
    let mut output = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut output, &schema).map_err(|_| ERR_ARROW_IPC)?;
        writer.write(&empty_batch).map_err(|_| ERR_ARROW_IPC)?;
        writer.finish().map_err(|_| ERR_ARROW_IPC)?;
    }
    let tokens = envelope
        .acks
        .rows
        .first()
        .map(|row| row.tokens.clone())
        .unwrap_or_default();
    Ok(BatchEnvelope {
        arrow_ipc_batch: output,
        acks: AckSidecar {
            rows: Vec::new(),
            acked: envelope.acks.acked,
            nacked: envelope.acks.nacked,
            message_errors: vec![WasmMessageErrorSet { tokens, reason }],
        },
    })
}
