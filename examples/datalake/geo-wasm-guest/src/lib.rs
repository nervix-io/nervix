use std::{cell::UnsafeCell, io::Cursor, net::IpAddr, panic::AssertUnwindSafe, sync::Arc};

use arrow_array::{
    Array, ArrayRef, RecordBatch, StringArray,
    builder::{Float64Builder, StringBuilder},
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use geo::{Distance, Haversine, Point};
use geohash::{Coord, encode};
use maxminddb::{geoip2, Reader};
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
const DBIP_MMDB: &[u8] = include_bytes!(concat!(
    env!("OUT_DIR"),
    "/dbip-city-lite-2026-06.mmdb"
));

const SOURCE_IP: usize = 7;
const GEOHASH_PRECISION: usize = 8;
const METERS_PER_KM: f64 = 1000.0;

#[link(wasm_import_module = "env")]
unsafe extern "C" {
    fn nervix_domain_time_nanos() -> i64;
}

struct GuestState {
    buffer: Vec<u8>,
    init_metadata: Vec<u8>,
    pending_emit: Vec<Vec<u8>>,
    global_error: Vec<u8>,
    output_relays: Vec<String>,
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Envelope {
    Input {
        #[serde(with = "cbor_byte_string")]
        arrow_ipc_batch: Vec<u8>,
        acks: AckSidecar,
    },
    Output {
        output_relay: String,
        columns: Vec<OutputColumn>,
        acks: AckSidecar,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum OutputColumn {
    GuestArrow {
        #[serde(with = "cbor_byte_string")]
        ipc: Vec<u8>,
    },
    Input {
        column_index: u32,
    },
}

#[derive(Clone, Deserialize)]
struct BranchInitMetadata {
    #[serde(default)]
    output_schemas: Vec<ProcessorSchema>,
}

#[derive(Clone, Deserialize)]
struct ProcessorSchema {
    name: String,
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
            output_relays: Vec::new(),
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
        self.output_relays = output_relays_from_init_metadata(&self.init_metadata)
            .unwrap_or_default();
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
        self.output_relays.clear();
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
            let Ok(reader) = geoip_reader() else {
                return ERR_INVALID_SIZE;
            };
            let output_relays = match output_relays_from_init_metadata(&metadata) {
                Ok(output_relays) => output_relays,
                Err(error) => return error,
            };
            state.init_metadata = metadata;
            state.output_relays = output_relays;
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
        let row_count = match arrow_ipc_row_count(&arrow_ipc_batch) {
            Ok(row_count) => row_count,
            Err(error) => return error,
        };
        state.processed_rows = state.processed_rows.saturating_add(row_count);

        let Some(reader) = state.geoip_reader.as_ref() else {
            return ERR_NOT_INITIALIZED;
        };
        let enriched = match geo_enrich_envelope(arrow_ipc_batch, acks, reader) {
            Ok(envelope) => envelope,
            Err(error) => return error,
        };
        if state.output_relays.is_empty() {
            return ERR_NOT_INITIALIZED;
        }
        state.pending_emit.clear();
        for (index, relay) in state.output_relays.iter().enumerate() {
            let mut output = enriched.clone();
            let Envelope::Output {
                output_relay,
                acks,
                ..
            } = &mut output
            else {
                return ERR_ENVELOPE;
            };
            *output_relay = relay.clone();
            if index > 0 {
                acks.acked.clear();
                acks.nacked.clear();
                acks.message_errors.clear();
            }
            match output.encode() {
                Ok(encoded) => state.pending_emit.push(encoded),
                Err(error) => return error,
            }
        }
        SUCCESS
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

fn output_relays_from_init_metadata(metadata: &[u8]) -> Result<Vec<String>, i32> {
    ciborium::from_reader::<BranchInitMetadata, _>(Cursor::new(metadata))
        .map(|metadata| {
            metadata
                .output_schemas
                .into_iter()
                .map(|schema| schema.name)
                .collect()
        })
        .map_err(|_| ERR_INVALID_SIZE)
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
    arrow_ipc_batch: Vec<u8>,
    acks: AckSidecar,
    reader: &Reader<&'static [u8]>,
) -> Result<Envelope, i32> {
    let ipc_reader = StreamReader::try_new(Cursor::new(&arrow_ipc_batch), None)
        .map_err(|_| ERR_ARROW_IPC)?;
    let batches = ipc_reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ERR_ARROW_IPC)?;
    if batches.len() != 1 {
        return Err(ERR_ARROW_IPC);
    }
    let generated = geo_enrich_columns(&batches[0], reader)?;
    let fields = generated_fields();
    let mut columns = (0..14_u32)
        .map(|column_index| OutputColumn::Input { column_index })
        .collect::<Vec<_>>();
    for (field, array) in fields.into_iter().zip(generated) {
        columns.push(OutputColumn::GuestArrow {
            ipc: encode_guest_column(field, array)?,
        });
    }
    Ok(Envelope::Output {
        output_relay: String::new(),
        columns,
        acks,
    })
}

fn geo_enrich_columns(
    batch: &RecordBatch,
    reader: &Reader<&'static [u8]>,
) -> Result<Vec<ArrayRef>, i32> {
    let source_ip = string_column(batch, SOURCE_IP)?;
    let row_count = batch.num_rows();
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
        let geo = resolve_ip(reader, string_value(source_ip, row));
        let geo_hash = geo_hash(geo.lat, geo.lon);
        let (hub, distance) = nearest_hub(geo.lat, geo.lon);
        out_geoip_database.append_value(reader.metadata.database_type.as_str());
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

    Ok(vec![
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
    ])
}

fn generated_fields() -> Vec<Field> {
    vec![
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
    ]
}

fn encode_guest_column(field: Field, array: ArrayRef) -> Result<Vec<u8>, i32> {
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).map_err(|_| ERR_ARROW_IPC)?;
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &schema).map_err(|_| ERR_ARROW_IPC)?;
        writer.write(&batch).map_err(|_| ERR_ARROW_IPC)?;
        writer.finish().map_err(|_| ERR_ARROW_IPC)?;
    }
    Ok(ipc)
}

fn string_column(batch: &RecordBatch, index: usize) -> Result<&StringArray, i32> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(ERR_ARROW_IPC)
}

fn string_value(array: &StringArray, row: usize) -> &str {
    if array.is_valid(row) {
        array.value(row)
    } else {
        ""
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
