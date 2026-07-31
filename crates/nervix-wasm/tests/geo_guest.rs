//! End-to-end checks for the datalake GeoIP reference guest in
//! `examples/datalake/geo-wasm-guest`.
//!
//! These are ignored by default because they need the built guest artifact, which embeds a 60 MB
//! DB-IP city database fetched from the network. Produce it and run them with:
//!
//! ```text
//! just wasm-datalake-geo-guest
//! cargo test -p nervix-wasm --test geo_guest_smoke -- --ignored --nocapture
//! ```

use std::{sync::Arc, time::Duration};

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use nervix_models::Timestamp;
use nervix_wasm::{
    DomainClock, WasmAckSidecar, WasmAckToken, WasmBranchInit, WasmEnvelope, WasmOutputColumnRef,
    WasmOutputRow, WasmProcessorField, WasmProcessorSchema, WasmProcessorType, WasmRuntime,
    WasmRuntimeConfig,
};

const GUEST: &str = "../../examples/datalake/geo-wasm-guest/target/wasm32-unknown-unknown/release/\
                     nervix_datalake_geo_wasm_guest.wasm";

#[derive(Debug)]
struct FixedClock(Timestamp);

impl DomainClock for FixedClock {
    fn now(&self) -> Timestamp {
        self.0
    }
}

fn field(name: &str, ty: WasmProcessorType) -> WasmProcessorField {
    WasmProcessorField {
        name: name.to_string(),
        ty,
        optional: false,
    }
}

fn input_fields() -> Vec<WasmProcessorField> {
    vec![
        field("source", WasmProcessorType::String),
        field("event_id", WasmProcessorType::String),
        field("tenant_id", WasmProcessorType::String),
        field("device_id", WasmProcessorType::String),
        field("session_id", WasmProcessorType::String),
        field("edge_id", WasmProcessorType::String),
        field("event_type", WasmProcessorType::String),
        field("source_ip", WasmProcessorType::String),
        field("device_lat", WasmProcessorType::F64),
        field("device_lon", WasmProcessorType::F64),
        field("battery_pct", WasmProcessorType::F64),
        field("firmware", WasmProcessorType::String),
        field("ts", WasmProcessorType::Datetime),
        field("seq", WasmProcessorType::I64),
    ]
}

fn output_fields() -> Vec<WasmProcessorField> {
    let mut fields = input_fields();
    fields.extend([
        field("geoip_database", WasmProcessorType::String),
        field("geoip_continent", WasmProcessorType::String),
        field("geoip_country", WasmProcessorType::String),
        field("geoip_region", WasmProcessorType::String),
        field("geoip_city", WasmProcessorType::String),
        field("geoip_lat", WasmProcessorType::F64),
        field("geoip_lon", WasmProcessorType::F64),
        field("geoip_geohash", WasmProcessorType::String),
        field("nearest_hub", WasmProcessorType::String),
        field("distance_to_hub_km", WasmProcessorType::F64),
    ]);
    fields
}

fn init() -> WasmBranchInit {
    WasmBranchInit {
        domain_name: "datalake".to_string(),
        domain_type: "UNPACED".to_string(),
        branch_key: Some(b"device=1".to_vec()),
        input_schema: WasmProcessorSchema {
            name: "device_location".to_string(),
            fields: input_fields(),
        },
        output_schemas: vec![
            WasmProcessorSchema {
                name: "device_location_geo_events".to_string(),
                fields: output_fields(),
            },
            WasmProcessorSchema {
                name: "device_location_geo_audit_events".to_string(),
                fields: output_fields(),
            },
        ],
    }
}

fn input_arrow(source_ip: &str) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
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
    ]));
    let text = |value: &str| Arc::new(StringArray::from(vec![value.to_string()]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            text("edge"),
            text("evt-1"),
            text("tenant-1"),
            text("device-1"),
            text("session-1"),
            text("edge-1"),
            text("location"),
            text(source_ip),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(Float64Array::from(vec![2.0])),
            Arc::new(Float64Array::from(vec![90.0])),
            text("1.0.0"),
            Arc::new(
                TimestampNanosecondArray::from(vec![1_000_000_000_i64]).with_timezone("+00:00"),
            ),
            Arc::new(Int64Array::from(vec![7_i64])),
        ],
    )
    .expect("input batch must build");
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &schema).expect("writer must build");
        writer.write(&batch).expect("batch must encode");
        writer.finish().expect("stream must finish");
    }
    ipc
}

#[tokio::test]
#[ignore = "needs `just wasm-datalake-geo-guest`"]
async fn the_geo_guest_enriches_every_declared_route() {
    let runtime = WasmRuntime::new(WasmRuntimeConfig {
        optimize: false,
        epoch_tick_interval: Duration::from_millis(5),
        epoch_deadline_ticks: 2_000,
        max_guest_buffer_bytes: 64 * 1024 * 1024,
    })
    .expect("runtime must initialize");
    let compiled = runtime
        .compile_processor(&std::fs::read(GUEST).expect("build the guest first"))
        .await
        .expect("geo guest must compile");
    let mut branch = compiled
        .instantiate_branch(
            init(),
            Box::new(FixedClock(Timestamp::from_unix_nanos(
                1_700_000_000_000_000_000,
            ))),
            None,
        )
        .await
        .expect("geo guest must instantiate");

    let outputs = branch
        .process_envelope(&WasmEnvelope::input(
            input_arrow("8.8.8.8"),
            WasmAckSidecar {
                rows: vec![WasmOutputRow {
                    tokens: vec![WasmAckToken(1)],
                    source_token: Some(WasmAckToken(1)),
                }],
                ..WasmAckSidecar::default()
            },
        ))
        .await
        .expect("geo guest must enrich the batch");

    assert_eq!(outputs.len(), 1, "one output group per input batch");
    let WasmEnvelope::Output {
        generated_arrow_ipc_batch,
        outputs,
    } = &outputs[0]
    else {
        panic!("guest must emit an output envelope");
    };

    assert_eq!(outputs.len(), 2, "one route per declared TO relay");
    assert_eq!(outputs[0].output_relay, "device_location_geo_events");
    assert_eq!(outputs[1].output_relay, "device_location_geo_audit_events");
    for output in outputs {
        assert_eq!(output.columns.len(), 24, "14 input + 10 generated columns");
        for (index, column) in output.columns.iter().enumerate().take(14) {
            assert_eq!(
                column,
                &WasmOutputColumnRef::Input {
                    column_index: index as u32
                }
            );
        }
        for (offset, column) in output.columns.iter().skip(14).enumerate() {
            assert_eq!(
                column,
                &WasmOutputColumnRef::Generated {
                    column_index: offset as u32
                }
            );
        }
    }
    assert_eq!(outputs[0].acks.rows.len(), 1);
    assert_eq!(outputs[1].acks.rows.len(), 1);

    let reader = StreamReader::try_new(generated_arrow_ipc_batch.as_ref(), None)
        .expect("generated pool must decode");
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("generated batch must decode");
    assert_eq!(batches.len(), 1);
    let generated = &batches[0];
    assert_eq!(generated.num_columns(), 10);
    assert_eq!(generated.num_rows(), 1);

    let text = |index: usize| {
        generated
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column")
            .value(0)
            .to_string()
    };
    let number = |index: usize| {
        generated
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float column")
            .value(0)
    };

    println!(
        "8.8.8.8 -> {}/{} {}/{} at {},{} geohash={} hub={} ({} km)",
        text(1),
        text(2),
        text(3),
        text(4),
        number(5),
        number(6),
        text(7),
        text(8),
        number(9)
    );

    assert!(!text(0).is_empty(), "the database type must be reported");
    assert_eq!(text(1), "NA", "8.8.8.8 resolves to North America");
    assert_eq!(text(2), "US", "8.8.8.8 resolves to the United States");
    assert!(
        number(5) > 30.0 && number(5) < 45.0,
        "latitude must be plausible"
    );
    assert!(number(6) < -100.0, "longitude must be plausible");
    assert!(!text(7).is_empty(), "the geohash must be computed");
    assert_eq!(
        text(8),
        "sfo",
        "the nearest configured hub to Mountain View"
    );
    assert!(number(9) > 0.0, "the distance to the hub must be positive");
}

#[tokio::test]
#[ignore = "needs `just wasm-datalake-geo-guest`"]
async fn the_geo_guest_rejects_a_destination_schema_it_cannot_fill() {
    let runtime = WasmRuntime::new(WasmRuntimeConfig {
        optimize: false,
        epoch_tick_interval: Duration::from_millis(5),
        epoch_deadline_ticks: 2_000,
        max_guest_buffer_bytes: 64 * 1024 * 1024,
    })
    .expect("runtime must initialize");
    let compiled = runtime
        .compile_processor(&std::fs::read(GUEST).expect("build the guest first"))
        .await
        .expect("geo guest must compile");

    let mut broken = init();
    broken.output_schemas[0].fields.pop();
    let error = compiled
        .instantiate_branch(
            broken,
            Box::new(FixedClock(Timestamp::from_unix_nanos(0))),
            None,
        )
        .await
        .expect_err("a destination the guest cannot fill must be rejected at init");

    match error {
        nervix_wasm::WasmProcessorError::GuestError { name, .. } => {
            assert_eq!(
                name, "nervix_init",
                "a misdeclared destination is rejected before any data reaches the guest"
            );
        }
        other => panic!("expected an init rejection, got {other:?}"),
    }
}
