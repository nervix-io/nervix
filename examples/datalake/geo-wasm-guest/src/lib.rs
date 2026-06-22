use std::{cell::UnsafeCell, io::Cursor, net::IpAddr, panic::AssertUnwindSafe, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
    builder::{Float64Builder, Int64Builder, StringBuilder},
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use geo::{Distance, Haversine, Point};
use geohash::{Coord, encode};
use maxminddb::{geoip2, Reader};
use serde::{Deserialize, Serialize};

const SUCCESS: i32 = 0;
const ERR_INVALID_SIZE: i32 = -1;
const ERR_OUT_OF_BOUNDS: i32 = -2;
const ERR_NOT_INITIALIZED: i32 = -3;
const ERR_ARROW_IPC: i32 = -4;
const ERR_ENVELOPE: i32 = -5;
const ERR_ERROR_STATE: i32 = -6;
const DBIP_MMDB: &[u8] = include_bytes!(concat!(
    env!("OUT_DIR"),
    "/dbip-city-lite-2026-06.mmdb"
));

const SOURCE: usize = 0;
const EVENT_ID: usize = 1;
const TENANT_ID: usize = 2;
const DEVICE_ID: usize = 3;
const SESSION_ID: usize = 4;
const EDGE_ID: usize = 5;
const EVENT_TYPE: usize = 6;
const SOURCE_IP: usize = 7;
const DEVICE_LAT: usize = 8;
const DEVICE_LON: usize = 9;
const BATTERY_PCT: usize = 10;
const FIRMWARE: usize = 11;
const TS: usize = 12;
const SEQ: usize = 13;
const GEOHASH_PRECISION: usize = 8;
const METERS_PER_KM: f64 = 1000.0;

#[link(wasm_import_module = "env")]
unsafe extern "C" {
    fn nervix_domain_time_nanos() -> i64;
}

struct GuestState {
    buffer: Vec<u8>,
    init_metadata: Vec<u8>,
    pending_emit: Vec<u8>,
    global_error: Vec<u8>,
    geoip_reader: Option<Reader<&'static [u8]>>,
    initialized: bool,
    processed_batches: u64,
    processed_rows: u64,
    last_domain_time_nanos: i64,
    error_state: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct GuestSnapshot {
    init_metadata: Vec<u8>,
    processed_batches: u64,
    processed_rows: u64,
    last_domain_time_nanos: i64,
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

struct ResolvedGeo {
    continent: String,
    country: String,
    region: String,
    city: String,
    lat: f64,
    lon: f64,
}

#[derive(Clone, Copy)]
struct Hub {
    name: &'static str,
    lat: f64,
    lon: f64,
}

const HUBS: &[Hub] = &[
    Hub {
        name: "sfo",
        lat: 37.7749,
        lon: -122.4194,
    },
    Hub {
        name: "ord",
        lat: 41.8781,
        lon: -87.6298,
    },
    Hub {
        name: "zrh",
        lat: 47.3769,
        lon: 8.5417,
    },
    Hub {
        name: "syd",
        lat: -33.8688,
        lon: 151.2093,
    },
];

impl GuestState {
    const fn new() -> Self {
        Self {
            buffer: Vec::new(),
            init_metadata: Vec::new(),
            pending_emit: Vec::new(),
            global_error: Vec::new(),
            geoip_reader: None,
            initialized: false,
            processed_batches: 0,
            processed_rows: 0,
            last_domain_time_nanos: 0,
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
        unsafe {
            std::ptr::copy_nonoverlapping(source, out.as_mut_ptr(), size);
        }
        Ok(out)
    }

    fn dump_state(&mut self) -> i32 {
        let snapshot = GuestSnapshot {
            init_metadata: self.init_metadata.clone(),
            processed_batches: self.processed_batches,
            processed_rows: self.processed_rows,
            last_domain_time_nanos: self.last_domain_time_nanos,
            error_state: self.error_state.clone(),
        };

        self.buffer.clear();
        if ciborium::into_writer(&snapshot, &mut self.buffer).is_err() {
            return ERR_INVALID_SIZE;
        }
        self.buffer.len() as i32
    }

    fn load_state_bytes(&mut self, saved_state: Vec<u8>) -> i32 {
        let Ok(snapshot) = ciborium::from_reader::<GuestSnapshot, _>(Cursor::new(saved_state))
        else {
            return ERR_INVALID_SIZE;
        };

        self.init_metadata = snapshot.init_metadata;
        self.processed_batches = snapshot.processed_batches;
        self.processed_rows = snapshot.processed_rows;
        self.last_domain_time_nanos = snapshot.last_domain_time_nanos;
        self.error_state = snapshot.error_state;
        let Ok(reader) = geoip_reader() else {
            return ERR_INVALID_SIZE;
        };
        self.geoip_reader = Some(reader);
        self.initialized = true;
        SUCCESS
    }

    fn reset(&mut self) {
        self.init_metadata.clear();
        self.pending_emit.clear();
        self.global_error.clear();
        self.geoip_reader = None;
        self.initialized = false;
        self.processed_batches = 0;
        self.processed_rows = 0;
        self.last_domain_time_nanos = 0;
        self.error_state = None;
    }

    fn set_global_error(&mut self, reason: impl Into<String>) {
        let reason = reason.into();
        self.error_state = Some(reason.clone());
        self.global_error.clear();
        self.global_error.extend_from_slice(reason.as_bytes());
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
            let Ok(reader) = geoip_reader() else {
                return ERR_INVALID_SIZE;
            };
            state.init_metadata = metadata;
            state.geoip_reader = Some(reader);
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

        state.processed_batches = state.processed_batches.saturating_add(1);
        state.last_domain_time_nanos = unsafe { nervix_domain_time_nanos() };
        let envelope = match BatchEnvelope::decode(&state.buffer[..size]) {
            Ok(envelope) => envelope,
            Err(error) => return error,
        };
        let row_count = match arrow_ipc_row_count(&envelope.arrow_ipc_batch) {
            Ok(row_count) => row_count,
            Err(error) => return error,
        };
        state.processed_rows = state.processed_rows.saturating_add(row_count);

        let Some(reader) = state.geoip_reader.as_ref() else {
            return ERR_NOT_INITIALIZED;
        };
        let enriched = match geo_enrich_envelope(envelope, reader) {
            Ok(envelope) => envelope,
            Err(error) => return error,
        };
        match enriched.encode() {
            Ok(encoded) => {
                state.pending_emit = encoded;
                SUCCESS
            }
            Err(error) => error,
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn nervix_on_timeout(_handle: i64) -> i32 {
    SUCCESS
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

fn read_u32_len(bytes: &[u8], offset: usize) -> Result<usize, i32> {
    let end = offset.checked_add(4).ok_or(ERR_ENVELOPE)?;
    let raw = bytes.get(offset..end).ok_or(ERR_ENVELOPE)?;
    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize)
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

fn geo_enrich_envelope(
    envelope: BatchEnvelope,
    reader: &Reader<&'static [u8]>,
) -> Result<BatchEnvelope, i32> {
    let ipc_reader = StreamReader::try_new(Cursor::new(&envelope.arrow_ipc_batch), None)
        .map_err(|_| ERR_ARROW_IPC)?;
    let schema = output_schema();
    let mut output_batches = Vec::new();
    for batch in ipc_reader {
        output_batches.push(geo_enrich_batch(&batch.map_err(|_| ERR_ARROW_IPC)?, reader)?);
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
        acks: envelope.acks,
    })
}

fn geo_enrich_batch(
    batch: &RecordBatch,
    reader: &Reader<&'static [u8]>,
) -> Result<RecordBatch, i32> {
    let source = string_column(batch, SOURCE)?;
    let event_id = string_column(batch, EVENT_ID)?;
    let tenant_id = string_column(batch, TENANT_ID)?;
    let device_id = string_column(batch, DEVICE_ID)?;
    let session_id = string_column(batch, SESSION_ID)?;
    let edge_id = string_column(batch, EDGE_ID)?;
    let event_type = string_column(batch, EVENT_TYPE)?;
    let source_ip = string_column(batch, SOURCE_IP)?;
    let device_lat = f64_column(batch, DEVICE_LAT)?;
    let device_lon = f64_column(batch, DEVICE_LON)?;
    let battery_pct = f64_column(batch, BATTERY_PCT)?;
    let firmware = string_column(batch, FIRMWARE)?;
    let ts = timestamp_column(batch, TS)?;
    let seq = i64_column(batch, SEQ)?;

    let row_count = batch.num_rows();
    let mut out_source = StringBuilder::new();
    let mut out_event_id = StringBuilder::new();
    let mut out_tenant_id = StringBuilder::new();
    let mut out_device_id = StringBuilder::new();
    let mut out_session_id = StringBuilder::new();
    let mut out_edge_id = StringBuilder::new();
    let mut out_event_type = StringBuilder::new();
    let mut out_source_ip = StringBuilder::new();
    let mut out_device_lat = Float64Builder::new();
    let mut out_device_lon = Float64Builder::new();
    let mut out_battery_pct = Float64Builder::new();
    let mut out_firmware = StringBuilder::new();
    let mut out_ts = Vec::with_capacity(row_count);
    let mut out_seq = Int64Builder::new();
    let mut out_geoip_database = StringBuilder::new();
    let mut out_geoip_continent = StringBuilder::new();
    let mut out_geoip_country = StringBuilder::new();
    let mut out_geoip_region = StringBuilder::new();
    let mut out_geoip_city = StringBuilder::new();
    let mut out_geoip_lat = Float64Builder::new();
    let mut out_geoip_lon = Float64Builder::new();
    let mut out_geoip_geohash = StringBuilder::new();
    let mut out_nearest_hub = StringBuilder::new();
    let mut out_distance_to_hub_km = Float64Builder::new();

    for row in 0..row_count {
        append_string(&mut out_source, source, row);
        append_string(&mut out_event_id, event_id, row);
        append_string(&mut out_tenant_id, tenant_id, row);
        append_string(&mut out_device_id, device_id, row);
        append_string(&mut out_session_id, session_id, row);
        append_string(&mut out_edge_id, edge_id, row);
        append_string(&mut out_event_type, event_type, row);
        append_string(&mut out_source_ip, source_ip, row);
        out_device_lat.append_value(non_null_f64(device_lat, row)?);
        out_device_lon.append_value(non_null_f64(device_lon, row)?);
        out_battery_pct.append_value(non_null_f64(battery_pct, row)?);
        append_string(&mut out_firmware, firmware, row);
        out_ts.push(Some(non_null_timestamp(ts, row)?));
        out_seq.append_value(non_null_i64(seq, row)?);

        let geo = resolve_ip(reader, string_value(source_ip, row));
        let geo_hash = geo_hash(geo.lat, geo.lon);
        let (hub, distance) = nearest_hub(geo.lat, geo.lon);
        out_geoip_database
            .append_value(reader.metadata.database_type.as_str());
        out_geoip_continent.append_value(geo.continent.as_str());
        out_geoip_country.append_value(geo.country.as_str());
        out_geoip_region.append_value(geo.region.as_str());
        out_geoip_city.append_value(geo.city.as_str());
        out_geoip_lat.append_value(geo.lat);
        out_geoip_lon.append_value(geo.lon);
        out_geoip_geohash.append_value(geo_hash);
        out_nearest_hub.append_value(hub);
        out_distance_to_hub_km.append_value(distance);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(out_source.finish()),
        Arc::new(out_event_id.finish()),
        Arc::new(out_tenant_id.finish()),
        Arc::new(out_device_id.finish()),
        Arc::new(out_session_id.finish()),
        Arc::new(out_edge_id.finish()),
        Arc::new(out_event_type.finish()),
        Arc::new(out_source_ip.finish()),
        Arc::new(out_device_lat.finish()),
        Arc::new(out_device_lon.finish()),
        Arc::new(out_battery_pct.finish()),
        Arc::new(out_firmware.finish()),
        Arc::new(TimestampNanosecondArray::from(out_ts).with_timezone_utc()),
        Arc::new(out_seq.finish()),
        Arc::new(out_geoip_database.finish()),
        Arc::new(out_geoip_continent.finish()),
        Arc::new(out_geoip_country.finish()),
        Arc::new(out_geoip_region.finish()),
        Arc::new(out_geoip_city.finish()),
        Arc::new(out_geoip_lat.finish()),
        Arc::new(out_geoip_lon.finish()),
        Arc::new(out_geoip_geohash.finish()),
        Arc::new(out_nearest_hub.finish()),
        Arc::new(out_distance_to_hub_km.finish()),
    ];

    RecordBatch::try_new(output_schema(), columns).map_err(|_| ERR_ARROW_IPC)
}

fn output_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("source", DataType::Utf8, false),
        Field::new("event_id", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("device_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("edge_id", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("source_ip", DataType::Utf8, false),
        Field::new("device_lat", DataType::Float64, false),
        Field::new("device_lon", DataType::Float64, false),
        Field::new("battery_pct", DataType::Float64, false),
        Field::new("firmware", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            false,
        ),
        Field::new("seq", DataType::Int64, false),
        Field::new("geoip_database", DataType::Utf8, false),
        Field::new("geoip_continent", DataType::Utf8, false),
        Field::new("geoip_country", DataType::Utf8, false),
        Field::new("geoip_region", DataType::Utf8, false),
        Field::new("geoip_city", DataType::Utf8, false),
        Field::new("geoip_lat", DataType::Float64, false),
        Field::new("geoip_lon", DataType::Float64, false),
        Field::new("geoip_geohash", DataType::Utf8, false),
        Field::new("nearest_hub", DataType::Utf8, false),
        Field::new("distance_to_hub_km", DataType::Float64, false),
    ]))
}

fn string_column(batch: &RecordBatch, index: usize) -> Result<&StringArray, i32> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(ERR_ARROW_IPC)
}

fn f64_column(batch: &RecordBatch, index: usize) -> Result<&Float64Array, i32> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or(ERR_ARROW_IPC)
}

fn i64_column(batch: &RecordBatch, index: usize) -> Result<&Int64Array, i32> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(ERR_ARROW_IPC)
}

fn timestamp_column(batch: &RecordBatch, index: usize) -> Result<&TimestampNanosecondArray, i32> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or(ERR_ARROW_IPC)
}

fn string_value(array: &StringArray, row: usize) -> &str {
    if array.is_valid(row) {
        array.value(row)
    } else {
        ""
    }
}

fn append_string(builder: &mut StringBuilder, array: &StringArray, row: usize) {
    builder.append_value(string_value(array, row));
}

fn non_null_f64(array: &Float64Array, row: usize) -> Result<f64, i32> {
    if array.is_valid(row) {
        Ok(array.value(row))
    } else {
        Err(ERR_ARROW_IPC)
    }
}

fn non_null_i64(array: &Int64Array, row: usize) -> Result<i64, i32> {
    if array.is_valid(row) {
        Ok(array.value(row))
    } else {
        Err(ERR_ARROW_IPC)
    }
}

fn non_null_timestamp(array: &TimestampNanosecondArray, row: usize) -> Result<i64, i32> {
    if array.is_valid(row) {
        Ok(array.value(row))
    } else {
        Err(ERR_ARROW_IPC)
    }
}

fn geoip_reader() -> Result<Reader<&'static [u8]>, i32> {
    Reader::from_source(DBIP_MMDB).map_err(|_| ERR_INVALID_SIZE)
}

fn resolve_ip(reader: &Reader<&'static [u8]>, source_ip: &str) -> ResolvedGeo {
    let Ok(ip) = source_ip.parse::<IpAddr>() else {
        return unknown_geo();
    };

    let Ok(result) = reader.lookup(ip) else {
        return unknown_geo();
    };
    let Ok(Some(city)) = result.decode::<geoip2::City>() else {
        return unknown_geo();
    };
    let Some(lat) = city.location.latitude else {
        return unknown_geo();
    };
    let Some(lon) = city.location.longitude else {
        return unknown_geo();
    };

    ResolvedGeo {
        continent: city.continent.code.unwrap_or("ZZ").to_string(),
        country: city.country.iso_code.unwrap_or("ZZ").to_string(),
        region: city
            .subdivisions
            .first()
            .and_then(|subdivision| subdivision.names.english)
            .unwrap_or("unknown")
            .to_string(),
        city: city.city.names.english.unwrap_or("unknown").to_string(),
        lat,
        lon,
    }
}

fn unknown_geo() -> ResolvedGeo {
    ResolvedGeo {
        continent: "ZZ".to_string(),
        country: "ZZ".to_string(),
        region: "unknown".to_string(),
        city: "unknown".to_string(),
        lat: 0.0,
        lon: 0.0,
    }
}

fn geo_hash(lat: f64, lon: f64) -> String {
    encode(Coord { x: lon, y: lat }, GEOHASH_PRECISION).unwrap_or_default()
}

fn nearest_hub(lat: f64, lon: f64) -> (&'static str, f64) {
    let mut best = HUBS[0];
    let mut best_distance = distance_km(lat, lon, best.lat, best.lon);
    for hub in &HUBS[1..] {
        let distance = distance_km(lat, lon, hub.lat, hub.lon);
        if distance < best_distance {
            best = *hub;
            best_distance = distance;
        }
    }
    (best.name, best_distance)
}

fn distance_km(left_lat: f64, left_lon: f64, right_lat: f64, right_lon: f64) -> f64 {
    Haversine.distance(
        Point::new(left_lon, left_lat),
        Point::new(right_lon, right_lat),
    ) / METERS_PER_KM
}
