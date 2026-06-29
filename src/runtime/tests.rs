use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use ahash::{HashMap, HashSet};
use arc_swap::ArcSwapOption;
use fjall::Database;
use nervix_interconnect::{RelayPayload, RelayPayloadKind};
use nervix_models::{
    AckMode, BranchParameterization, ClientConfigEntry, ClusterSchedule, CodecWireFormat,
    CreateClientHttp, CreateClientMqtt, CreateClientPrometheus, CreateClientWebsockets,
    CreateClientZeroMq, CreateCodec, CreateDeduplicator, CreateEmitter, CreateGenerator,
    CreateInferencer, CreateIngestor, CreateJsonWireSchema, CreateLookup, CreateReingestor,
    CreateRelay, CreateSchema, CreateUnifier, CreateWasmProcessor, CreateWindowProcessor, Domain,
    DomainConfig, DomainPace, DomainSchedule, DomainState, DomainStatus, DomainTick, EmitSink,
    ErrorPolicies, GeneralErrorPolicy, Identifier, InferencerTensorMapping, IngestSource,
    IngestTimestampSource, JsonType, MessageErrorPolicy, ModelKind, MqttIngestMode, MqttQos,
    MqttSession, ParameterValueMapping, ParseAsType, ProcessorInputWhere, ProcessorOutput,
    ProcessorOutputs, RelayParameterization, RemoteAckOutcome, RemoteAckResolution, ResourceId,
    ResourceVersion, ResourceVersionStatus, RetryPolicy, ScheduledNode, SchemaField, Timestamp,
    WindowBound, WireSchemaField, ZeroMqIngestMode,
};
use nervix_wasm::{
    WasmAckSidecar, WasmAckToken, WasmBatchEnvelope, WasmMessageErrorSet, WasmNackSet,
    WasmRowAckSet,
};
use ordered_float::OrderedFloat;
use sorted_vec::SortedVec;
use tempfile::tempdir;
use tokio::{
    sync::{Mutex, mpsc, watch},
    time::{Duration, Instant, sleep, timeout},
};

use super::{
    BranchKey, ParameterizedProcessorOperationSpec, ParametrizerRegistry, RelayMessage,
    RuntimeStateKind, RuntimeStatePlacement, RuntimeStateStore, STUPID_CHANNEL_CAPACITY_REMOVE_ME,
    WindowAggregateFunction, WindowProcessorState, advance_window, evaluate_window_aggregate,
    message_timestamp, parse_aggregate_program, window_output_metadata,
};
use crate::{
    metrics::RuntimeMetrics,
    resource::ResourceStore,
    runtime_ack::{AckOutcome, AckSet},
    runtime_schema::{RuntimeRecord, RuntimeRecordMetadata, RuntimeValue, compile_schema},
};

fn identifier(raw: &str) -> Identifier {
    Identifier::parse(raw).expect("valid identifier")
}

fn branch_key(fields: impl IntoIterator<Item = (Identifier, RuntimeValue)>) -> Option<BranchKey> {
    BranchKey::from_fields(fields)
        .expect("test branch key must be non-empty")
        .into()
}

fn concrete_branch_key(fields: impl IntoIterator<Item = (Identifier, RuntimeValue)>) -> BranchKey {
    branch_key(fields).expect("test branch key must be concrete")
}

fn string_branch_key(field: &str, value: &str) -> Option<BranchKey> {
    branch_key([(identifier(field), RuntimeValue::String(value.to_string()))])
}

#[test]
fn branch_key_rejects_empty_fields() {
    assert!(BranchKey::from_fields([]).is_err());
}

fn u32_branch_key(field: &str, value: u32) -> Option<BranchKey> {
    branch_key([(identifier(field), RuntimeValue::U32(value))])
}

fn key_label(key: &Option<BranchKey>) -> &str {
    key.as_ref().expect("test branch key must exist").as_str()
}

fn domain(raw: &str) -> Domain {
    Domain::parse(raw).expect("valid domain")
}

const TWO_ITEM_TEST_CHANNEL_CAPACITY: usize = 2;

fn nonzero_capacity(capacity: usize) -> NonZeroUsize {
    NonZeroUsize::new(capacity).expect("test relay capacity must be nonzero")
}

fn parameterized_by(schema: &str, relay: &str, fields: &[&str]) -> BranchParameterization {
    BranchParameterization::parameterized_with_ttl(
        identifier(schema),
        fields
            .iter()
            .map(|field| ParameterValueMapping {
                field: identifier(field),
                relay: identifier(relay),
                relay_field: identifier(field),
            })
            .collect(),
        "5m".to_string(),
    )
}

fn test_relay_boundary_services() -> Arc<super::RelayBoundaryServices> {
    Arc::new(super::RelayBoundaryServices {
        fanout: super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(
            STUPID_CHANNEL_CAPACITY_REMOVE_ME,
        )),
        attached_runtime_consumer_count: 0,
        detached_runtime_consumer_count: 0,
        remote_runtime_consumers: Arc::from([]),
        remote_dispatcher: None,
    })
}

#[tokio::test]
async fn memory_pressure_pause_stops_registered_ingestors() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let ingestor = identifier("source");
    let key = super::RuntimeKey::new(domain.clone(), ingestor.clone());
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let stopped = Arc::new(AtomicBool::new(false));
    let task_stopped = stopped.clone();
    let task = tokio::spawn(async move {
        let _ = shutdown_rx.wait_for(|shutdown| *shutdown).await;
        task_stopped.store(true, Ordering::SeqCst);
    });

    runtime.ingestors.insert(
        key.clone(),
        super::IngestorRuntime::Background {
            shutdown: shutdown_tx,
            parameterized: Vec::new(),
            tasks: vec![task],
        },
    );

    assert_eq!(runtime.pause_ingestors_for_memory_pressure().await, 1);
    assert!(runtime.ingestors_paused_for_memory_pressure());
    assert!(stopped.load(Ordering::SeqCst));
    assert!(runtime.ingestors.get(&key).is_none());
}

#[tokio::test]
async fn memory_pressure_resume_clears_pause_when_no_ingestors_are_pending() {
    let runtime = super::Runtime::default();

    assert_eq!(runtime.pause_ingestors_for_memory_pressure().await, 0);
    assert!(runtime.ingestors_paused_for_memory_pressure());
    assert!(
        !runtime
            .resume_one_ingestor_after_memory_pressure()
            .await
            .expect("resume should succeed")
    );
    assert!(!runtime.ingestors_paused_for_memory_pressure());
}

#[tokio::test]
async fn ack_alive_resets_ingestor_ack_timeout() {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let (acks, completion) = AckSet::root();
    let ack_task = acks.clone();

    tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        ack_task.ack_alive();
        sleep(Duration::from_millis(150)).await;
        ack_task.ack_success();
    });

    assert_eq!(
        super::Runtime::await_ack_completion(
            &mut shutdown_rx,
            completion,
            Duration::from_millis(200),
        )
        .await,
        Some(AckOutcome::Ack)
    );
    drop(shutdown_tx);
}

#[tokio::test]
async fn remote_ack_alive_packet_resets_ingestor_ack_timeout() {
    let runtime = super::Runtime::default();
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let (acks, completion) = AckSet::root();
    runtime.pending_remote_acks.insert(7, acks);
    let runtime_task = runtime.clone();

    tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        runtime_task.handle_remote_ack_resolution(RemoteAckResolution {
            ack_id: 7,
            outcome: RemoteAckOutcome::Alive,
        });
        sleep(Duration::from_millis(150)).await;
        runtime_task.handle_remote_ack_resolution(RemoteAckResolution {
            ack_id: 7,
            outcome: RemoteAckOutcome::Ack,
        });
    });

    assert_eq!(
        super::Runtime::await_ack_completion(
            &mut shutdown_rx,
            completion,
            Duration::from_millis(200),
        )
        .await,
        Some(AckOutcome::Ack)
    );
    assert!(
        runtime.pending_remote_acks.get(&7).is_none(),
        "terminal ack must clear the pending remote ack"
    );
    drop(shutdown_tx);
}

#[test]
fn window_aggregate_evaluator_computes_count_percentile_and_array() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("count"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("p50"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("latencies"),
                ty: ParseAsType::Array {
                    element: Box::new(ParseAsType::F64),
                    len: 2,
                },
                optional: false,
                sensitive: false,
            },
        ],
    });
    let aggregate = parse_aggregate_program(
        "summary.count = COUNT(events.latency), summary.p50 = \
         PERCENTILE_LINEAR_HISTOGRAM(events.latency, 50, 10, 0, 100, '2s'), summary.latencies = \
         [PERCENTILE_LINEAR_HISTOGRAM(events.latency, 50, 10, 0, 100, '2s'), \
         PERCENTILE_LINEAR_HISTOGRAM(events.latency, 100, 10, 0, 100, '2s')]",
    )
    .expect("aggregate should parse");
    let mut state = WindowProcessorState::new(&aggregate.inner);
    for value in [10.0, 20.0, 30.0] {
        state
            .push_message(
                &aggregate.inner,
                Timestamp::now(),
                RelayMessage {
                    key: None,
                    record: RuntimeRecord::from_fields([(
                        "latency".to_string(),
                        RuntimeValue::F64(OrderedFloat(value)),
                    )]),
                    acks: AckSet::empty(),
                },
            )
            .expect("aggregate state should accept message");
    }

    let record = evaluate_window_aggregate(
        &aggregate.inner,
        &state,
        output_schema.arrow_schema().as_ref(),
    )
    .expect("aggregate should evaluate");

    assert_eq!(record.value("count"), Some(&RuntimeValue::I64(3)));
    assert_eq!(
        record.value("p50"),
        Some(&RuntimeValue::F64(OrderedFloat(25.0)))
    );
    assert_eq!(
        record.value("latencies"),
        Some(&RuntimeValue::Array(vec![
            RuntimeValue::F64(OrderedFloat(25.0)),
            RuntimeValue::F64(OrderedFloat(35.0)),
        ]))
    );
}

#[test]
fn window_linear_histogram_percentiles_share_accumulator_by_config() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("p50"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("p90"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("p50_other_range"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
        ],
    });
    let aggregate = parse_aggregate_program(
        "summary.p50 = PERCENTILE_LINEAR_HISTOGRAM(events.latency, 50, 10, 0, 100, '2s'), \
         summary.p90 = PERCENTILE_LINEAR_HISTOGRAM(events.latency, 90, 10, 0, 100, '2s'), \
         summary.p50_other_range = PERCENTILE_LINEAR_HISTOGRAM(events.latency, 50, 10, 0, 200, \
         '2s')",
    )
    .expect("aggregate should parse");
    let mut state = WindowProcessorState::new(&aggregate.inner);

    assert_eq!(
        state.accumulators.len(),
        2,
        "same input and histogram config should share one accumulator"
    );

    for value in [10, 20, 30] {
        state
            .push_message(
                &aggregate.inner,
                Timestamp::now(),
                RelayMessage {
                    key: None,
                    record: RuntimeRecord::from_fields([(
                        "latency".to_string(),
                        RuntimeValue::I64(value),
                    )]),
                    acks: AckSet::empty(),
                },
            )
            .expect("aggregate state should accept message");
    }

    let record = evaluate_window_aggregate(
        &aggregate.inner,
        &state,
        output_schema.arrow_schema().as_ref(),
    )
    .expect("aggregate should evaluate");

    assert_eq!(
        record.value("p50"),
        Some(&RuntimeValue::F64(OrderedFloat(25.0)))
    );
    assert_eq!(
        record.value("p90"),
        Some(&RuntimeValue::F64(OrderedFloat(35.0)))
    );
    assert_eq!(
        record.value("p50_other_range"),
        Some(&RuntimeValue::F64(OrderedFloat(30.0)))
    );
}

#[test]
fn window_advance_removes_step_messages() {
    let aggregate = parse_aggregate_program("summary.count = COUNT(events.latency)")
        .expect("aggregate should parse");
    let mut state = WindowProcessorState::new(&aggregate.inner);
    for sequence in 0..5 {
        state
            .push_message(
                &aggregate.inner,
                Timestamp::now(),
                RelayMessage {
                    key: None,
                    record: RuntimeRecord::from_fields([(
                        "latency".to_string(),
                        RuntimeValue::I64(sequence as i64),
                    )]),
                    acks: AckSet::empty(),
                },
            )
            .expect("aggregate state should accept message");
    }

    advance_window(
        &mut state,
        &aggregate.inner,
        Some(2),
        None,
        Timestamp::now(),
    )
    .expect("window should advance");

    assert_eq!(state.entries.len(), 3);
    assert_eq!(state.entries.front().map(|entry| entry.sequence), Some(2));
    assert_eq!(
        state.accumulators[0]
            .evaluate(WindowAggregateFunction::Count, None)
            .expect("count should evaluate"),
        RuntimeValue::I64(3)
    );
}

#[test]
fn linear_histogram_zero_delay_removes_step_values_immediately() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![nervix_models::SchemaField {
            name: identifier("p0"),
            ty: ParseAsType::F64,
            optional: false,
            sensitive: false,
        }],
    });
    let aggregate = parse_aggregate_program(
        "summary.p0 = PERCENTILE_LINEAR_HISTOGRAM(events.latency, 0, 10, 0, 100, '0ms')",
    )
    .expect("aggregate should parse");
    let mut state = WindowProcessorState::new(&aggregate.inner);
    for (timestamp, value) in [
        (Timestamp::from_unix_nanos(0), 10),
        (Timestamp::from_unix_nanos(1_000_000_000), 90),
    ] {
        state
            .push_message(
                &aggregate.inner,
                timestamp,
                RelayMessage {
                    key: None,
                    record: RuntimeRecord::from_fields([(
                        "latency".to_string(),
                        RuntimeValue::I64(value),
                    )]),
                    acks: AckSet::empty(),
                },
            )
            .expect("aggregate state should accept message");
    }

    advance_window(
        &mut state,
        &aggregate.inner,
        Some(1),
        None,
        Timestamp::from_unix_nanos(1_000_000_000),
    )
    .expect("window should advance");
    let record = evaluate_window_aggregate(
        &aggregate.inner,
        &state,
        output_schema.arrow_schema().as_ref(),
    )
    .expect("aggregate should evaluate");

    assert_eq!(
        record.value("p0"),
        Some(&RuntimeValue::F64(OrderedFloat(95.0)))
    );
}

#[test]
fn linear_histogram_delay_retains_removed_step_values_until_expired() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![nervix_models::SchemaField {
            name: identifier("p0"),
            ty: ParseAsType::F64,
            optional: false,
            sensitive: false,
        }],
    });
    let aggregate = parse_aggregate_program(
        "summary.p0 = PERCENTILE_LINEAR_HISTOGRAM(events.latency, 0, 10, 0, 100, '2s')",
    )
    .expect("aggregate should parse");
    let mut state = WindowProcessorState::new(&aggregate.inner);
    for (timestamp, value) in [
        (Timestamp::from_unix_nanos(0), 10),
        (Timestamp::from_unix_nanos(1_000_000_000), 90),
    ] {
        state
            .push_message(
                &aggregate.inner,
                timestamp,
                RelayMessage {
                    key: None,
                    record: RuntimeRecord::from_fields([(
                        "latency".to_string(),
                        RuntimeValue::I64(value),
                    )]),
                    acks: AckSet::empty(),
                },
            )
            .expect("aggregate state should accept message");
    }

    advance_window(
        &mut state,
        &aggregate.inner,
        Some(1),
        None,
        Timestamp::from_unix_nanos(1_000_000_000),
    )
    .expect("window should advance");
    let retained = evaluate_window_aggregate(
        &aggregate.inner,
        &state,
        output_schema.arrow_schema().as_ref(),
    )
    .expect("aggregate should evaluate while delay retains value");
    assert_eq!(
        retained.value("p0"),
        Some(&RuntimeValue::F64(OrderedFloat(15.0)))
    );

    state
        .push_message(
            &aggregate.inner,
            Timestamp::from_unix_nanos(2_000_000_000),
            RelayMessage {
                key: None,
                record: RuntimeRecord::from_fields([(
                    "latency".to_string(),
                    RuntimeValue::I64(90),
                )]),
                acks: AckSet::empty(),
            },
        )
        .expect("aggregate state should accept message before delay expires");
    let still_retained = evaluate_window_aggregate(
        &aggregate.inner,
        &state,
        output_schema.arrow_schema().as_ref(),
    )
    .expect("aggregate should evaluate before delay expires");
    assert_eq!(
        still_retained.value("p0"),
        Some(&RuntimeValue::F64(OrderedFloat(15.0)))
    );

    state
        .push_message(
            &aggregate.inner,
            Timestamp::from_unix_nanos(4_000_000_000),
            RelayMessage {
                key: None,
                record: RuntimeRecord::from_fields([(
                    "latency".to_string(),
                    RuntimeValue::I64(90),
                )]),
                acks: AckSet::empty(),
            },
        )
        .expect("aggregate state should accept message after delay expires");
    let expired = evaluate_window_aggregate(
        &aggregate.inner,
        &state,
        output_schema.arrow_schema().as_ref(),
    )
    .expect("aggregate should evaluate after delay expires");
    assert_eq!(
        expired.value("p0"),
        Some(&RuntimeValue::F64(OrderedFloat(95.0)))
    );
}

#[test]
fn linear_histogram_delay_exposes_timeout_deadline_without_new_messages() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![nervix_models::SchemaField {
            name: identifier("p0"),
            ty: ParseAsType::F64,
            optional: false,
            sensitive: false,
        }],
    });
    let aggregate = parse_aggregate_program(
        "summary.p0 = PERCENTILE_LINEAR_HISTOGRAM(events.latency, 0, 10, 0, 100, '2s')",
    )
    .expect("aggregate should parse");
    let mut state = WindowProcessorState::new(&aggregate.inner);
    for (timestamp, value) in [
        (Timestamp::from_unix_nanos(0), 10),
        (Timestamp::from_unix_nanos(1_000_000_000), 90),
    ] {
        state
            .push_message(
                &aggregate.inner,
                timestamp,
                RelayMessage {
                    key: None,
                    record: RuntimeRecord::from_fields([(
                        "latency".to_string(),
                        RuntimeValue::I64(value),
                    )]),
                    acks: AckSet::empty(),
                },
            )
            .expect("aggregate state should accept message");
    }

    advance_window(
        &mut state,
        &aggregate.inner,
        Some(1),
        None,
        Timestamp::from_unix_nanos(1_000_000_000),
    )
    .expect("window should advance");
    assert_eq!(
        state.next_timeout_deadline(),
        Some(Timestamp::from_unix_nanos(3_000_000_000))
    );

    assert!(
        !state
            .purge_timeouts(Timestamp::from_unix_nanos(2_999_999_999))
            .expect("early purge check should succeed")
    );
    assert!(
        state
            .purge_timeouts(Timestamp::from_unix_nanos(3_000_000_000))
            .expect("due purge should succeed")
    );
    assert_eq!(state.next_timeout_deadline(), None);

    let record = evaluate_window_aggregate(
        &aggregate.inner,
        &state,
        output_schema.arrow_schema().as_ref(),
    )
    .expect("aggregate should evaluate after timeout purge");
    assert_eq!(
        record.value("p0"),
        Some(&RuntimeValue::F64(OrderedFloat(95.0)))
    );
}

#[test]
fn window_aggregate_state_updates_first_last_min_max_and_sum() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("first_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("last_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("min_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("max_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("total_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
        ],
    });
    let aggregate = parse_aggregate_program(
        "summary.first_latency = FIRST(events.latency), summary.last_latency = \
         LAST(events.latency), summary.min_latency = MIN(events.latency), summary.max_latency = \
         MAX(events.latency), summary.total_latency = SUM(events.latency)",
    )
    .expect("aggregate should parse");
    let mut state = WindowProcessorState::new(&aggregate.inner);
    for value in [30, 10, 20] {
        state
            .push_message(
                &aggregate.inner,
                Timestamp::now(),
                RelayMessage {
                    key: None,
                    record: RuntimeRecord::from_fields([(
                        "latency".to_string(),
                        RuntimeValue::I64(value),
                    )]),
                    acks: AckSet::empty(),
                },
            )
            .expect("aggregate state should accept message");
    }

    let record = evaluate_window_aggregate(
        &aggregate.inner,
        &state,
        output_schema.arrow_schema().as_ref(),
    )
    .expect("aggregate should evaluate");

    assert_eq!(record.value("first_latency"), Some(&RuntimeValue::I64(30)));
    assert_eq!(record.value("last_latency"), Some(&RuntimeValue::I64(20)));
    assert_eq!(record.value("min_latency"), Some(&RuntimeValue::I64(10)));
    assert_eq!(record.value("max_latency"), Some(&RuntimeValue::I64(30)));
    assert_eq!(record.value("total_latency"), Some(&RuntimeValue::I64(60)));

    advance_window(
        &mut state,
        &aggregate.inner,
        Some(1),
        None,
        Timestamp::now(),
    )
    .expect("window should advance");
    let record = evaluate_window_aggregate(
        &aggregate.inner,
        &state,
        output_schema.arrow_schema().as_ref(),
    )
    .expect("aggregate should evaluate after removal");

    assert_eq!(record.value("first_latency"), Some(&RuntimeValue::I64(10)));
    assert_eq!(record.value("last_latency"), Some(&RuntimeValue::I64(20)));
    assert_eq!(record.value("min_latency"), Some(&RuntimeValue::I64(10)));
    assert_eq!(record.value("max_latency"), Some(&RuntimeValue::I64(20)));
    assert_eq!(record.value("total_latency"), Some(&RuntimeValue::I64(30)));
}

#[test]
fn window_message_timestamp_uses_low_watermark() {
    let message = RelayMessage {
        key: None,
        record: RuntimeRecord::from_fields([]).with_metadata(
            RuntimeRecordMetadata::from_ingested_at_watermarks(
                Timestamp::from_unix_nanos(10),
                Timestamp::from_unix_nanos(20),
            ),
        ),
        acks: AckSet::empty(),
    };

    let timestamp = message_timestamp(&message);

    assert_eq!(timestamp, Timestamp::from_unix_nanos(10));
}

#[test]
fn window_output_metadata_uses_window_low_and_emit_high_watermark() {
    let aggregate = parse_aggregate_program("summary.count = COUNT(events.latency)")
        .expect("aggregate should parse");
    let mut state = WindowProcessorState::new(&aggregate.inner);
    for timestamp in [
        Timestamp::from_unix_nanos(30),
        Timestamp::from_unix_nanos(10),
        Timestamp::from_unix_nanos(20),
    ] {
        state
            .push_message(
                &aggregate.inner,
                timestamp,
                RelayMessage {
                    key: None,
                    record: RuntimeRecord::from_fields([(
                        "latency".to_string(),
                        RuntimeValue::I64(timestamp.unix_nanos()),
                    )]),
                    acks: AckSet::empty(),
                },
            )
            .expect("aggregate state should accept message");
    }

    let metadata = window_output_metadata(&state, Timestamp::from_unix_nanos(40))
        .expect("non-empty window should emit metadata");

    assert_eq!(
        metadata.ingested_at_low_watermark(),
        Timestamp::from_unix_nanos(10)
    );
    assert_eq!(
        metadata.ingested_at_high_watermark(),
        Timestamp::from_unix_nanos(40)
    );
}

#[test]
fn window_processor_state_snapshot_roundtrips_entries_and_accumulators() {
    let output_schema = compile_schema(&CreateSchema {
        name: identifier("summary"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("count"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("first_latency"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("p50"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
        ],
    });
    let aggregate = parse_aggregate_program(
        "summary.count = COUNT(events.latency), summary.first_latency = FIRST(events.latency), \
         summary.p50 = PERCENTILE_LINEAR_HISTOGRAM(events.latency, 50, 10, 0, 100, '2s')",
    )
    .expect("aggregate should parse");
    let mut state = WindowProcessorState::new(&aggregate.inner);
    for (timestamp, value) in [
        (Timestamp::from_unix_nanos(10), 10),
        (Timestamp::from_unix_nanos(20), 30),
    ] {
        state
            .push_message(
                &aggregate.inner,
                timestamp,
                RelayMessage {
                    key: string_branch_key("tenant", "acme"),
                    record: RuntimeRecord::from_fields([(
                        "latency".to_string(),
                        RuntimeValue::I64(value),
                    )])
                    .with_metadata(
                        RuntimeRecordMetadata::from_ingested_at_watermarks(timestamp, timestamp),
                    ),
                    acks: AckSet::empty(),
                },
            )
            .expect("window should accept message");
    }

    let restored = WindowProcessorState::from_snapshot(&aggregate.inner, state.to_snapshot())
        .expect("snapshot should restore");
    let record = evaluate_window_aggregate(
        &aggregate.inner,
        &restored,
        output_schema.arrow_schema().as_ref(),
    )
    .expect("restored aggregate should evaluate");

    assert_eq!(restored.entries.len(), 2);
    assert_eq!(
        key_label(&restored.entries.front().unwrap().message.key),
        r#"{"tenant":"acme"}"#
    );
    assert_eq!(record.value("count"), Some(&RuntimeValue::I64(2)));
    assert_eq!(record.value("first_latency"), Some(&RuntimeValue::I64(10)));
    assert_eq!(
        record.value("p50"),
        Some(&RuntimeValue::F64(OrderedFloat(35.0)))
    );
}

fn paced_domain_state(raw: &str) -> DomainState {
    DomainState {
        id: domain(raw),
        config: DomainConfig {
            pace: DomainPace::Paced,
            period: "1s".to_string(),
            skew: "250ms".to_string(),
        },
        status: DomainStatus::Running,
        start_version: 0,
        last_start: nervix_models::DomainStartPoint::Resume,
    }
}

fn test_schema(fields: &[(&str, ParseAsType)]) -> Arc<super::CompiledSchema> {
    Arc::new(compile_schema(&CreateSchema {
        name: identifier("test_schema"),
        fields: fields
            .iter()
            .map(|(name, ty)| nervix_models::SchemaField {
                name: identifier(name),
                ty: ty.clone(),
                optional: false,
                sensitive: false,
            })
            .collect(),
    }))
}

fn wasm_test_arrow_ipc(schema: &Arc<super::CompiledSchema>, values: &[i32]) -> Vec<u8> {
    schema
        .arrow_batch_from_records(
            &values
                .iter()
                .map(|value| {
                    RuntimeRecord::from_fields([("value".to_string(), RuntimeValue::I32(*value))])
                })
                .collect::<Vec<_>>(),
        )
        .expect("test arrow batch should build")
        .to_arrow_ipc_bytes()
        .expect("test arrow batch should encode")
}

#[test]
fn wasm_input_envelope_builds_ack_context_from_source_records() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let metadata = RuntimeRecordMetadata::from_ingested_at_watermarks(
        Timestamp::from_unix_nanos(10),
        Timestamp::from_unix_nanos(20),
    );
    let record = RuntimeRecord::from_fields_with_metadata(
        [("value".to_string(), RuntimeValue::I32(42))],
        metadata.clone(),
    );
    let branch = Some(concrete_branch_key([(
        identifier("branch"),
        RuntimeValue::String("test".to_string()),
    )]));
    let batch = super::RelayRecordBatch::single(schema, branch, record.clone(), AckSet::empty())
        .expect("relay batch should build");
    let mut next_ack_token = 1;

    let (envelope, ack_map) = super::wasm_envelope_from_relay_batch(&batch, &mut next_ack_token)
        .expect("wasm envelope should build");

    assert_eq!(envelope.acks.rows.len(), 1);
    assert_eq!(envelope.acks.rows[0].tokens, vec![WasmAckToken(1)]);
    let context = ack_map.get(&1).expect("ack token context should exist");
    assert_eq!(context.record.value("value"), Some(&RuntimeValue::I32(42)));
    assert_eq!(
        context.metadata.ingested_at_low_watermark(),
        metadata.ingested_at_low_watermark()
    );
    assert_eq!(
        context.metadata.ingested_at_high_watermark(),
        metadata.ingested_at_high_watermark()
    );
    assert_eq!(next_ack_token, 2);
}

#[test]
fn wasm_output_rejects_ack_sidecar_row_count_mismatch() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let mut ack_map = super::WasmAckMap::default();
    let branch = Some(concrete_branch_key([(
        identifier("branch"),
        RuntimeValue::String("test".to_string()),
    )]));
    let mut token_use_counts = HashMap::default();

    let error = super::relay_batch_from_wasm_envelope(
        &branch,
        &schema,
        WasmBatchEnvelope::new(
            wasm_test_arrow_ipc(&schema, &[1, 2]),
            WasmAckSidecar {
                rows: vec![WasmRowAckSet { tokens: vec![] }],
                acked: vec![],
                nacked: vec![],
                message_errors: vec![],
            },
        ),
        &mut ack_map,
        &mut token_use_counts,
    )
    .err()
    .expect("row count mismatch should reject guest output");

    assert_eq!(
        error,
        "wasm output row count 2 does not match ack sidecar row count 1"
    );
}

#[test]
fn wasm_output_rejects_malformed_arrow_ipc_before_relay_dispatch() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let mut ack_map = super::WasmAckMap::default();
    let branch = Some(concrete_branch_key([(
        identifier("branch"),
        RuntimeValue::String("test".to_string()),
    )]));
    let mut token_use_counts = HashMap::default();

    let error = super::relay_batch_from_wasm_envelope(
        &branch,
        &schema,
        WasmBatchEnvelope::new(
            b"not arrow ipc".to_vec(),
            WasmAckSidecar {
                rows: vec![WasmRowAckSet { tokens: vec![] }],
                acked: vec![],
                nacked: vec![],
                message_errors: vec![],
            },
        ),
        &mut ack_map,
        &mut token_use_counts,
    )
    .err()
    .expect("malformed guest Arrow IPC should reject guest output");

    assert_eq!(
        error,
        "wasm output Arrow IPC is invalid: Io error: failed to fill whole buffer"
    );
}

#[test]
fn wasm_output_rejects_unknown_carried_ack_token() {
    let schema = test_schema(&[("value", ParseAsType::I32)]);
    let mut ack_map = super::WasmAckMap::default();
    let branch = Some(concrete_branch_key([(
        identifier("branch"),
        RuntimeValue::String("test".to_string()),
    )]));
    let mut token_use_counts = HashMap::default();

    let error = super::relay_batch_from_wasm_envelope(
        &branch,
        &schema,
        WasmBatchEnvelope::new(
            wasm_test_arrow_ipc(&schema, &[1]),
            WasmAckSidecar {
                rows: vec![WasmRowAckSet {
                    tokens: vec![WasmAckToken(99)],
                }],
                acked: vec![],
                nacked: vec![],
                message_errors: vec![],
            },
        ),
        &mut ack_map,
        &mut token_use_counts,
    )
    .err()
    .expect("unknown carried ack token should reject guest output");

    assert_eq!(error, "wasm output referenced unknown ack token 99");
}

#[tokio::test]
async fn wasm_output_rejects_unknown_terminal_ack_token() {
    let ack_map = super::WasmAckMap::default();

    let error = super::validate_wasm_sidecar_token_decisions(
        &ack_map,
        &WasmAckSidecar {
            rows: vec![],
            acked: vec![WasmRowAckSet {
                tokens: vec![WasmAckToken(77)],
            }],
            nacked: vec![],
            message_errors: vec![],
        },
    )
    .expect_err("unknown terminal ack token should reject guest output");

    assert_eq!(
        error,
        "wasm output terminal ack referenced unknown ack token 77"
    );
}

#[tokio::test]
async fn wasm_output_rejects_unknown_message_error_token() {
    let ack_map = super::WasmAckMap::default();

    let error = super::validate_wasm_sidecar_token_decisions(
        &ack_map,
        &WasmAckSidecar {
            rows: vec![],
            acked: vec![],
            nacked: vec![],
            message_errors: vec![WasmMessageErrorSet {
                tokens: vec![WasmAckToken(88)],
                reason: "bad row".to_string(),
            }],
        },
    )
    .expect_err("unknown message error token should reject guest output");

    assert_eq!(
        error,
        "wasm output message error referenced unknown ack token 88"
    );
}

#[tokio::test]
async fn wasm_output_rejects_duplicate_terminal_ack_decision() {
    let (acks, completion) = AckSet::root();
    let now = Timestamp::from_unix_nanos(1);
    let mut ack_map = super::WasmAckMap::default();
    ack_map.insert(
        7,
        super::WasmAckContext {
            acks,
            metadata: RuntimeRecordMetadata::from_ingested_at_watermarks(now, now),
            record: RuntimeRecord::from_fields_with_metadata(
                [("value".to_string(), RuntimeValue::I32(7))],
                RuntimeRecordMetadata::from_ingested_at_watermarks(now, now),
            ),
        },
    );

    let error = super::validate_wasm_sidecar_token_decisions(
        &ack_map,
        &WasmAckSidecar {
            rows: vec![],
            acked: vec![WasmRowAckSet {
                tokens: vec![WasmAckToken(7)],
            }],
            nacked: vec![WasmNackSet {
                tokens: vec![WasmAckToken(7)],
                reason: "bad row".to_string(),
            }],
            message_errors: vec![],
        },
    )
    .expect_err("duplicate terminal ack token should reject guest output");

    assert_eq!(
        error,
        "wasm output made more than one terminal ack decision for token 7"
    );
    assert!(
        timeout(Duration::from_millis(50), completion.wait())
            .await
            .is_err(),
        "validation failure should not apply partial ack decisions"
    );
}

fn scheduled_model(
    kind: ModelKind,
    identifier: Identifier,
    model: nervix_models::Model,
) -> ScheduledNode {
    ScheduledNode {
        identifier,
        kind,
        config: Box::new(model),
        effective_parameterization: None,
        kafka_partition_schedule: None,
        primary_node: Some("node-1".to_string()),
        assigned_nodes: vec!["node-1".to_string()],
    }
}

#[tokio::test]
async fn scheduled_mqtt_client_id_conflicts_are_visible_on_describe() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        DomainState {
            id: domain.clone(),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "1s".to_string(),
                skew: "0s".to_string(),
            },
            status: DomainStatus::Running,
            start_version: 0,
            last_start: nervix_models::DomainStartPoint::Resume,
        },
    )]));

    let schema = identifier("notification");
    let wire_schema = identifier("notification_wire");
    let codec = identifier("notification_json");
    let relay = identifier("notifications");
    let client = identifier("mqtt_main");
    let ingestor = identifier("mqtt_notifications");
    let result = runtime
        .apply_cluster_schedule(
            "node-1",
            &ClusterSchedule {
                domains: vec![DomainSchedule {
                    domain: domain.clone(),
                    nodes: vec![
                        scheduled_model(
                            ModelKind::Schema,
                            schema.clone(),
                            nervix_models::Model::Schema(CreateSchema {
                                name: schema.clone(),
                                fields: vec![SchemaField {
                                    name: identifier("user_id"),
                                    ty: ParseAsType::I64,
                                    optional: false,
                                    sensitive: false,
                                }],
                            }),
                        ),
                        scheduled_model(
                            ModelKind::WireSchema,
                            wire_schema.clone(),
                            nervix_models::Model::WireSchema(
                                nervix_models::CreateWireSchemaStmt::Json(CreateJsonWireSchema {
                                    name: wire_schema.clone(),
                                    strictness: Default::default(),
                                    fields: vec![WireSchemaField {
                                        name: identifier("user_id"),
                                        ty: JsonType::Integer,
                                        optional: false,
                                    }],
                                }),
                            ),
                        ),
                        scheduled_model(
                            ModelKind::Codec,
                            codec.clone(),
                            nervix_models::Model::Codec(CreateCodec {
                                name: codec.clone(),
                                wire_format: CodecWireFormat::Json,
                                wire_schema: Some(wire_schema.clone()),
                                schema: schema.clone(),
                                encoding_rules: Vec::new(),
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Relay,
                            relay.clone(),
                            nervix_models::Model::Relay(CreateRelay {
                                name: relay.clone(),
                                schema: schema.clone(),
                                buffer: 2,
                                parameterization: RelayParameterization::unparameterized(),
                                materialized_state: None,
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Client,
                            client.clone(),
                            nervix_models::Model::ClientMqtt(CreateClientMqtt {
                                name: client.clone(),
                                mount: None,
                                config: vec![
                                    ClientConfigEntry {
                                        key: "addr".to_string(),
                                        value: "mqtt://127.0.0.1:1883".to_string(),
                                    },
                                    ClientConfigEntry {
                                        key: "client_id".to_string(),
                                        value: "fixed-client".to_string(),
                                    },
                                ],
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Ingestor,
                            ingestor.clone(),
                            nervix_models::Model::Ingestor(CreateIngestor {
                                name: ingestor.clone(),
                                output_routes: ProcessorOutputs::single(relay.clone()),
                                decode_using_codec: codec.clone(),
                                parameterized_by: BranchParameterization::unparameterized(),
                                flush_each: "100ms".to_string(),
                                max_batch_size: Some("1MiB".to_string()),
                                timestamp_source: None,
                                source: IngestSource::Mqtt {
                                    client,
                                    topic: "notifications".to_string(),
                                    instances: 2,
                                    mode: MqttIngestMode::NoAckSequential {
                                        session: MqttSession::Clean,
                                        qos: MqttQos::AtMostOnce,
                                    },
                                },
                                error_policies: ErrorPolicies {
                                    message: MessageErrorPolicy::Log,
                                    general: GeneralErrorPolicy::Log,
                                },
                                filter_where: None,
                            }),
                        ),
                    ],
                }],
            },
        )
        .await;

    result.expect("fixed mqtt client_id conflict should be reported by ingestor state");

    let describe = runtime
        .describe_local_ingestor(&domain, &ingestor)
        .expect("describe should succeed for scheduled ingestor");
    assert!(describe.running);
    assert!(
        describe.transient_error.as_deref().is_some_and(
            |error| error.contains("MQTT client_id 'fixed-client' is shared by 2 instances")
        ),
        "describe should expose mqtt client_id conflict, got {:?}",
        describe.transient_error
    );
}

#[tokio::test]
async fn scheduled_ingestor_start_failure_removes_partial_domain_execution() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    runtime.sync_domains(&BTreeMap::from([(
        domain.clone(),
        DomainState {
            id: domain.clone(),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "1s".to_string(),
                skew: "0s".to_string(),
            },
            status: DomainStatus::Running,
            start_version: 0,
            last_start: nervix_models::DomainStartPoint::Resume,
        },
    )]));

    let schema = identifier("notification");
    let wire_schema = identifier("notification_wire");
    let codec = identifier("notification_json");
    let relay = identifier("notifications");
    let client = identifier("mqtt_main");
    let ingestor = identifier("mqtt_notifications");
    let result = runtime
        .apply_cluster_schedule(
            "node-1",
            &ClusterSchedule {
                domains: vec![DomainSchedule {
                    domain: domain.clone(),
                    nodes: vec![
                        scheduled_model(
                            ModelKind::Schema,
                            schema.clone(),
                            nervix_models::Model::Schema(CreateSchema {
                                name: schema.clone(),
                                fields: vec![SchemaField {
                                    name: identifier("user_id"),
                                    ty: ParseAsType::I64,
                                    optional: false,
                                    sensitive: false,
                                }],
                            }),
                        ),
                        scheduled_model(
                            ModelKind::WireSchema,
                            wire_schema.clone(),
                            nervix_models::Model::WireSchema(
                                nervix_models::CreateWireSchemaStmt::Json(CreateJsonWireSchema {
                                    name: wire_schema.clone(),
                                    strictness: Default::default(),
                                    fields: vec![WireSchemaField {
                                        name: identifier("user_id"),
                                        ty: JsonType::Integer,
                                        optional: false,
                                    }],
                                }),
                            ),
                        ),
                        scheduled_model(
                            ModelKind::Codec,
                            codec.clone(),
                            nervix_models::Model::Codec(CreateCodec {
                                name: codec.clone(),
                                wire_format: CodecWireFormat::Json,
                                wire_schema: Some(wire_schema.clone()),
                                schema: schema.clone(),
                                encoding_rules: Vec::new(),
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Relay,
                            relay.clone(),
                            nervix_models::Model::Relay(CreateRelay {
                                name: relay.clone(),
                                schema: schema.clone(),
                                buffer: 2,
                                parameterization: RelayParameterization::unparameterized(),
                                materialized_state: None,
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Client,
                            client.clone(),
                            nervix_models::Model::ClientMqtt(CreateClientMqtt {
                                name: client.clone(),
                                mount: None,
                                config: vec![ClientConfigEntry {
                                    key: "addr".to_string(),
                                    value: "mqtt://127.0.0.1:1883".to_string(),
                                }],
                            }),
                        ),
                        scheduled_model(
                            ModelKind::Ingestor,
                            ingestor.clone(),
                            nervix_models::Model::Ingestor(CreateIngestor {
                                name: ingestor.clone(),
                                output_routes: ProcessorOutputs::single(relay.clone()),
                                decode_using_codec: codec.clone(),
                                parameterized_by: BranchParameterization::unparameterized(),
                                flush_each: "100ms".to_string(),
                                max_batch_size: Some("1MiB".to_string()),
                                timestamp_source: None,
                                source: IngestSource::Mqtt {
                                    client,
                                    topic: "notifications".to_string(),
                                    instances: 1,
                                    mode: MqttIngestMode::AckSequential {
                                        timeout: "oops".to_string(),
                                        retry_policy: RetryPolicy {
                                            backoff: "100ms".to_string(),
                                            max_backoff: "200ms".to_string(),
                                        },
                                    },
                                },
                                error_policies: ErrorPolicies {
                                    message: MessageErrorPolicy::Log,
                                    general: GeneralErrorPolicy::Log,
                                },
                                filter_where: None,
                            }),
                        ),
                    ],
                }],
            },
        )
        .await;

    let error = result.expect_err("invalid ACK timeout must fail schedule application");
    assert!(
        error.to_string().contains("invalid ack timeout 'oops'"),
        "unexpected start error: {error}"
    );
    assert!(
        !runtime.executions.contains_key(&domain),
        "failed scheduled ingestor start must not leave a partial domain execution"
    );
    assert!(
        !runtime
            .ingestors
            .contains_key(&super::RuntimeKey::new(domain.clone(), ingestor.clone())),
        "failed scheduled ingestor start must not leave an ingestor runtime"
    );
    let describe_error = runtime
        .describe_local_ingestor(&domain, &ingestor)
        .expect_err("describe should expose the domain instantiation error");
    assert!(
        describe_error.contains("invalid ack timeout 'oops'"),
        "describe should expose start error, got {describe_error}"
    );
}

#[tokio::test]
async fn branch_preserving_processors_reject_standalone_schedule_nodes() {
    let cases = [
        (
            ModelKind::Deduplicator,
            identifier("dedup_orders"),
            nervix_models::Model::Deduplicator(CreateDeduplicator {
                name: identifier("dedup_orders"),
                from_relay: identifier("orders"),
                from_where: Vec::new(),
                output_routes: ProcessorOutputs::single(identifier("projected_orders")),
                parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                deduplicate_on: "orders.order_id".to_string(),
                max_time: "10m".to_string(),
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: AckMode::Attached,
                message_error_policy: MessageErrorPolicy::Log,
                filter_where: None,
            }),
            "deduplicator 'dedup_orders' is not attached to a branch root",
        ),
        (
            ModelKind::Unifier,
            identifier("join_orders"),
            nervix_models::Model::Unifier(CreateUnifier {
                name: identifier("join_orders"),
                from_relays: vec![identifier("left_orders"), identifier("right_orders")],
                from_where: Vec::new(),
                output_routes: ProcessorOutputs::single(identifier("joined_orders")),
                parameterized_by: parameterized_by("tenant", "left_orders", &["tenant"]),
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: AckMode::Attached,
                message_error_policy: MessageErrorPolicy::Log,
                filter_where: None,
            }),
            "unifier 'join_orders' is not attached to a branch root",
        ),
        (
            ModelKind::WindowProcessor,
            identifier("orders_window"),
            nervix_models::Model::WindowProcessor(CreateWindowProcessor {
                name: identifier("orders_window"),
                from_relay: identifier("orders"),
                from_where: Vec::new(),
                output_routes: ProcessorOutputs::single(identifier("order_summaries")),
                parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                width: WindowBound {
                    messages: Some(10),
                    duration: None,
                },
                step: WindowBound {
                    messages: Some(10),
                    duration: None,
                },
                mode: AckMode::Attached,
                aggregate: "order_summaries.count = COUNT(orders.amount)".to_string(),
                message_error_policy: MessageErrorPolicy::Log,
                filter_where: None,
            }),
            "window processor 'orders_window' is not attached to a branch root",
        ),
        (
            ModelKind::Inferencer,
            identifier("score_orders"),
            nervix_models::Model::Inferencer(CreateInferencer {
                name: identifier("score_orders"),
                from_relay: identifier("orders"),
                from_where: Vec::new(),
                output_routes: ProcessorOutputs::single(identifier("scores")),
                parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                resource: identifier("score_model"),
                resource_version: None,
                file: "models/score.onnx".to_string(),
                inputs: vec![InferencerTensorMapping {
                    tensor: "features".to_string(),
                    relay: identifier("orders"),
                    field: identifier("features"),
                }],
                outputs: vec![InferencerTensorMapping {
                    tensor: "score".to_string(),
                    relay: identifier("scores"),
                    field: identifier("score"),
                }],
                flush_each: "IMMEDIATE".to_string(),
                max_batch_size: None,
                mode: AckMode::Attached,
                message_error_policy: MessageErrorPolicy::Log,
                filter_where: None,
            }),
            "inferencer 'score_orders' is not attached to a branch root",
        ),
    ];

    for (kind, identifier, model, expected) in cases {
        let runtime = super::Runtime::default();
        let domain = domain("default");
        let err = runtime
            .rebuild_domain_from_schedule(
                "node-1",
                &domain,
                Some(DomainSchedule {
                    domain: domain.clone(),
                    nodes: vec![scheduled_model(kind, identifier, model)],
                }),
            )
            .await
            .expect_err("standalone branch-preserving processor must fail");
        let rendered = err.to_string();
        assert!(
            rendered.contains(expected),
            "expected {rendered:?} to contain {expected:?}"
        );
    }
}

#[test]
fn runtime_state_store_persists_latest_snapshot_with_monotonic_lsm() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        branch_key: string_branch_key("tenant", "acme"),
    };

    let first_lsm = 1;
    store
        .persist_latest_snapshot(&placement, first_lsm, b"first")
        .expect("first snapshot should persist");
    let second_lsm = 2;
    store
        .persist_latest_snapshot(&placement, second_lsm, b"second")
        .expect("second snapshot should persist");

    assert_eq!(first_lsm, 1);
    assert_eq!(second_lsm, 2);
    assert_eq!(
        store
            .latest_snapshot(&placement)
            .expect("latest snapshot should load")
            .expect("latest snapshot should exist")
            .payload,
        b"second".to_vec()
    );
}

#[test]
fn kafka_offset_state_roundtrips_partition_schedule_through_fjall() {
    let dir = tempdir().expect("temp dir should open");
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db).expect("state store should open");
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::KafkaOffset,
        kind: ModelKind::Ingestor,
        identifier: identifier("kafka_notifications"),
        branch_key: None,
    };
    let state =
        super::ReplicatedKafkaOffsetState::new(placement.clone(), None, Vec::new(), 0, None)
            .expect("kafka state should initialize");
    let (offset_lsm, offset_payload) = state
        .replace_offsets(HashMap::from_iter([
            (("notifications".to_string(), 0), 12),
            (("notifications".to_string(), 1), 18),
        ]))
        .expect("offsets should update");
    store
        .persist_latest_snapshot(&placement, offset_lsm, &offset_payload)
        .expect("offset snapshot should persist");
    let (schedule_lsm, schedule_payload) = state
        .update_partition_schedule("notifications", 2, vec![0, 1])
        .expect("schedule should update")
        .expect("schedule snapshot should be produced");
    store
        .persist_latest_snapshot(&placement, schedule_lsm, &schedule_payload)
        .expect("schedule snapshot should persist");

    let restored = super::ReplicatedKafkaOffsetState::new(
        placement.clone(),
        None,
        Vec::new(),
        0,
        store
            .latest_snapshot(&placement)
            .expect("snapshot should load"),
    )
    .expect("restored kafka state should initialize");
    assert_eq!(restored.next_offset("notifications", 0), Some(12));
    assert_eq!(restored.next_offset("notifications", 1), Some(18));
    assert_eq!(
        restored.describe_topic("notifications"),
        Some(super::KafkaDomainOffsetDescribe {
            topic: "notifications".to_string(),
            instances: 2,
            observed_partitions: vec![0, 1],
            rebalance_epoch: 0,
            instance_assignments: vec![vec![0], vec![1]],
        })
    );
}

#[test]
fn branch_aggregated_state_snapshot_roundtrips_metrics() {
    let metrics = RuntimeMetrics::default();
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Ingestor,
        identifier: identifier("redis_notifications"),
        branch_key: None,
    };
    let relay = identifier("notifications");
    let state = super::ReplicatedBranchAggregatedState::new(
        placement.clone(),
        Some("node-1".to_string()),
        "node-1".to_string(),
        Vec::new(),
        0,
        &metrics,
        None,
    )
    .expect("branch-aggregated state should initialize");
    metrics.observe_global_node_sent(crate::metrics::NodeBatchObservation {
        domain: &placement.domain,
        kind: placement.kind,
        node: &placement.identifier,
        relay: &relay,
        physical_node_id: Some("node-1"),
        messages: 2,
        bytes: 64,
        domain_timestamp: None,
    });
    let lsm = state.mark_metrics_updated();
    let snapshot = state
        .latest_snapshot(&metrics)
        .expect("metrics snapshot should encode");
    assert_eq!(snapshot.lsm, lsm);

    let restored_metrics = RuntimeMetrics::default();
    let _restored = super::ReplicatedBranchAggregatedState::new(
        placement.clone(),
        Some("node-1".to_string()),
        "node-1".to_string(),
        Vec::new(),
        0,
        &restored_metrics,
        Some(snapshot),
    )
    .expect("branch-aggregated state should restore");

    let rendered = restored_metrics.describe_global_target(
        &placement.domain,
        "INGESTOR",
        &placement.identifier,
    );
    assert!(
        rendered.iter().any(
            |line| line.contains("messages_total sent relay=notifications")
                && line.contains("total=2")
        ),
        "expected restored metrics total in {rendered:?}"
    );
}

#[test]
fn describe_restores_branch_aggregated_metrics_from_store_without_materialized_state() {
    let dir = tempdir().expect("temp dir should open");
    let domain = domain("default");
    let ingestor = identifier("redis_notifications");
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Ingestor,
        identifier: ingestor.clone(),
        branch_key: None,
    };
    {
        let db = Database::builder(dir.path())
            .open()
            .expect("db should open");
        let store = RuntimeStateStore::from_database(db).expect("state store should open");
        let metrics = RuntimeMetrics::default();
        metrics.observe_global_node_sent(crate::metrics::NodeBatchObservation {
            domain: &domain,
            kind: ModelKind::Ingestor,
            node: &ingestor,
            relay: &identifier("notifications"),
            physical_node_id: Some("node-3"),
            messages: 19,
            bytes: 1900,
            domain_timestamp: None,
        });
        let snapshot = super::BranchAggregatedRuntimeStateSnapshot {
            metrics: metrics.snapshot_global_target(
                &domain,
                ModelKind::Ingestor,
                &ingestor,
                "node-3",
            ),
        };
        let payload = super::encode_branch_aggregated_snapshot(&snapshot)
            .expect("branch-aggregated snapshot should encode");
        store
            .persist_latest_snapshot(&placement, 7, &payload)
            .expect("snapshot should persist");
    }

    let db = Database::builder(dir.path())
        .open()
        .expect("db should reopen");
    let runtime =
        super::Runtime::with_persistence(Some(db), Duration::from_millis(100), Default::default())
            .expect("runtime should open persisted state");
    runtime
        .metrics
        .register_global_node(&domain, ModelKind::Ingestor, &ingestor, Some("node-3"));

    let rendered = runtime.describe_metrics_for(&domain, "INGESTOR", &ingestor);
    assert!(
        rendered.iter().any(|line| line
            .contains("messages_total sent relay=notifications physical_node=node-3")
            && line.contains("total=19")),
        "expected persisted branch-aggregated metrics before START retry in {rendered:?}"
    );
}

#[test]
fn describe_restores_branch_aggregated_metrics_when_state_lsm_is_current_but_metrics_missing() {
    let dir = tempdir().expect("temp dir should open");
    let domain = domain("default");
    let ingestor = identifier("redis_notifications");
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Ingestor,
        identifier: ingestor.clone(),
        branch_key: None,
    };
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db.clone()).expect("state store should open");
    let persisted_metrics = RuntimeMetrics::default();
    persisted_metrics.observe_global_node_sent(crate::metrics::NodeBatchObservation {
        domain: &domain,
        kind: ModelKind::Ingestor,
        node: &ingestor,
        relay: &identifier("notifications"),
        physical_node_id: Some("node-3"),
        messages: 19,
        bytes: 1900,
        domain_timestamp: None,
    });
    let snapshot = super::BranchAggregatedRuntimeStateSnapshot {
        metrics: persisted_metrics.snapshot_global_target(
            &domain,
            ModelKind::Ingestor,
            &ingestor,
            "node-3",
        ),
    };
    let payload = super::encode_branch_aggregated_snapshot(&snapshot)
        .expect("branch-aggregated snapshot should encode");
    store
        .persist_latest_snapshot(&placement, 7, &payload)
        .expect("snapshot should persist");

    let runtime =
        super::Runtime::with_persistence(Some(db), Duration::from_millis(100), Default::default())
            .expect("runtime should open persisted state");
    let stale_state = Arc::new(
        super::ReplicatedBranchAggregatedState::new(
            placement.clone(),
            Some("node-3".to_string()),
            "node-3".to_string(),
            Vec::new(),
            0,
            &RuntimeMetrics::default(),
            store
                .latest_snapshot(&placement)
                .expect("snapshot should load"),
        )
        .expect("stale state should initialize"),
    );
    stale_state.mark_metrics_updated();
    runtime
        .replicated_branch_aggregated_states
        .insert(placement, stale_state);
    runtime
        .metrics
        .register_global_node(&domain, ModelKind::Ingestor, &ingestor, Some("node-3"));

    let rendered = runtime.describe_metrics_for(&domain, "INGESTOR", &ingestor);
    assert!(
        rendered.iter().any(|line| line
            .contains("messages_total sent relay=notifications physical_node=node-3")
            && line.contains("total=19")),
        "expected persisted branch-aggregated metrics despite current stale LSM in {rendered:?}"
    );
}

#[test]
fn describe_does_not_reapply_equal_lsm_snapshot_over_active_metrics() {
    let dir = tempdir().expect("temp dir should open");
    let domain = domain("default");
    let ingestor = identifier("redis_notifications");
    let placement = RuntimeStatePlacement {
        domain: domain.clone(),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Ingestor,
        identifier: ingestor.clone(),
        branch_key: None,
    };
    let db = Database::builder(dir.path())
        .open()
        .expect("db should open");
    let store = RuntimeStateStore::from_database(db.clone()).expect("state store should open");
    let persisted_metrics = RuntimeMetrics::default();
    persisted_metrics.observe_global_node_sent(crate::metrics::NodeBatchObservation {
        domain: &domain,
        kind: ModelKind::Ingestor,
        node: &ingestor,
        relay: &identifier("notifications"),
        physical_node_id: Some("node-3"),
        messages: 19,
        bytes: 1900,
        domain_timestamp: None,
    });
    let snapshot = super::BranchAggregatedRuntimeStateSnapshot {
        metrics: persisted_metrics.snapshot_global_target(
            &domain,
            ModelKind::Ingestor,
            &ingestor,
            "node-3",
        ),
    };
    let payload = super::encode_branch_aggregated_snapshot(&snapshot)
        .expect("branch-aggregated snapshot should encode");
    store
        .persist_latest_snapshot(&placement, 7, &payload)
        .expect("snapshot should persist");

    let runtime =
        super::Runtime::with_persistence(Some(db), Duration::from_millis(100), Default::default())
            .expect("runtime should open persisted state");
    let state = Arc::new(
        super::ReplicatedBranchAggregatedState::new(
            placement.clone(),
            Some("node-3".to_string()),
            "node-3".to_string(),
            Vec::new(),
            0,
            &RuntimeMetrics::default(),
            store
                .latest_snapshot(&placement)
                .expect("snapshot should load"),
        )
        .expect("state should initialize"),
    );
    runtime
        .replicated_branch_aggregated_states
        .insert(placement, state);
    runtime
        .metrics
        .observe_global_node_sent(crate::metrics::NodeBatchObservation {
            domain: &domain,
            kind: ModelKind::Ingestor,
            node: &ingestor,
            relay: &identifier("notifications"),
            physical_node_id: Some("node-3"),
            messages: 1,
            bytes: 100,
            domain_timestamp: None,
        });

    let rendered = runtime.describe_metrics_for(&domain, "INGESTOR", &ingestor);
    assert!(
        rendered.iter().any(|line| line
            .contains("messages_total sent relay=notifications physical_node=node-3")
            && line.contains(" total=1 ")),
        "expected active metrics to remain authoritative for equal LSM in {rendered:?}"
    );
}

#[tokio::test]
async fn state_sync_request_returns_latest_snapshot_only_when_lsm_advances() {
    let runtime = super::Runtime::default();
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = runtime
        .replicated_deduplicator_state(placement.clone(), Vec::new(), 0)
        .expect("deduplicator state should initialize");
    let (lsm, _payload) = state
        .apply_new_key(
            "txn-1".to_string(),
            Timestamp::from_unix_nanos(1),
            Duration::from_secs(600),
        )
        .expect("deduplicator update should succeed")
        .expect("deduplicator key should be new");

    let first = runtime
        .handle_state_sync_request(&placement, 0)
        .await
        .expect("state sync request should succeed")
        .expect("snapshot should be returned");
    assert_eq!(first.lsm, lsm);

    let none = runtime
        .handle_state_sync_request(&placement, lsm)
        .await
        .expect("state sync request should succeed");
    assert!(none.is_none());
}

#[test]
fn runtime_state_placement_storage_key_includes_branch_key() {
    let tenant_beta = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        branch_key: string_branch_key("tenant", "beta"),
    };
    let tenant = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        branch_key: string_branch_key("tenant", "acme"),
    };

    assert_ne!(tenant_beta.as_storage_key(), tenant.as_storage_key());
    let branch_aggregated = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::BranchAggregated,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        branch_key: None,
    };
    assert_ne!(
        tenant_beta.as_storage_key(),
        branch_aggregated.as_storage_key()
    );
    let deduplicator_global = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        branch_key: None,
    };
    assert_ne!(
        deduplicator_global.as_storage_key(),
        branch_aggregated.as_storage_key()
    );
}

#[tokio::test]
async fn replica_quorum_waits_for_replication_ack() {
    let runtime = super::Runtime::default();
    let placement = RuntimeStatePlacement {
        domain: domain("default"),
        state: RuntimeStateKind::Deduplicator,
        kind: ModelKind::Deduplicator,
        identifier: identifier("dedup_orders"),
        branch_key: string_branch_key("tenant", "acme"),
    };
    let state = Arc::new(
        super::ReplicatedDeduplicatorState::new(placement, vec!["node-2".to_string()], 1, None)
            .expect("replicated state should initialize"),
    );
    let (lsm, _payload) = state
        .apply_new_key(
            "txn-1".to_string(),
            Timestamp::from_unix_nanos(1),
            Duration::from_secs(600),
        )
        .expect("deduplicator update should succeed")
        .expect("deduplicator key should be new");

    let waiter = {
        let runtime = runtime.clone();
        let state = state.clone();
        tokio::spawn(async move { runtime.wait_for_replica_quorum(&state, lsm).await })
    };
    sleep(Duration::from_millis(50)).await;
    state.mark_replica_progress("node-2", lsm);

    assert!(waiter.await.expect("waiter task should join").is_ok());
}

#[tokio::test]
async fn relay_dispatch_detaches_subscription_delivery_from_ack_chain() {
    let runtime = super::Runtime::default();
    let domain = Domain::parse("default").expect("valid domain");
    let relay = Identifier::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("customer_id", ParseAsType::String)]);
    let registry = super::RelayRegistry::new();
    let services = test_relay_boundary_services();
    let mut subscription_rx = services.subscription_receiver();
    let mut runtime_rx = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Attached);

    let (acks, completion) = AckSet::root();
    let batch = super::RelayRecordBatch::single(
        schema,
        string_branch_key("customer", "42"),
        RuntimeRecord::from_fields([(
            "customer_id".to_string(),
            RuntimeValue::String("42".to_string()),
        )]),
        acks.clone(),
    )
    .expect("batch should build");

    runtime
        .ingest_stream_boundary_message(&domain, &relay, &registry, &services, &batch)
        .await
        .expect("dispatch should succeed");

    let subscription_batch = subscription_rx
        .recv()
        .await
        .expect("subscription should receive batch");
    assert!(subscription_batch.acks.iter().all(AckSet::is_empty));

    acks.ack_success();

    let runtime_message = runtime_rx
        .recv()
        .await
        .expect("runtime consumer should receive");
    for ack in runtime_message.acks.iter() {
        ack.ack_success();
    }

    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
    drop(subscription_batch);
}

#[tokio::test]
async fn relay_dispatch_detaches_detached_runtime_consumers_from_ack_chain() {
    let runtime = super::Runtime::default();
    let domain = Domain::parse("default").expect("valid domain");
    let relay = Identifier::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let registry = super::RelayRegistry::new();
    let services = test_relay_boundary_services();
    let mut runtime_rx = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Detached);
    let (acks, completion) = AckSet::root();
    let batch = super::RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 52),
        RuntimeRecord::from_fields([("user_id".to_string(), RuntimeValue::U32(52))]),
        acks.clone(),
    )
    .expect("batch should build");

    runtime
        .ingest_stream_boundary_message(&domain, &relay, &registry, &services, &batch)
        .await
        .expect("dispatch should succeed");

    acks.ack_success();

    let runtime_message = runtime_rx
        .recv()
        .await
        .expect("runtime consumer should receive message");
    assert!(runtime_message.acks.iter().all(AckSet::is_empty));
    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
}

#[tokio::test]
async fn relay_runtime_consumer_broadcast_fans_out_to_multiple_attached_receivers() {
    let runtime = super::Runtime::default();
    let domain = Domain::parse("default").expect("valid domain");
    let relay = Identifier::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let registry = super::RelayRegistry::new();
    let services = test_relay_boundary_services();
    let mut first_consumer = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Attached);
    let mut second_consumer = services
        .fanout
        .runtime_consumer_receiver_for_mode(AckMode::Attached);

    let (acks, completion) = AckSet::root();
    let batch = super::RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 52),
        RuntimeRecord::from_fields([("user_id".to_string(), RuntimeValue::U32(52))]),
        acks.clone(),
    )
    .expect("batch should build");

    runtime
        .ingest_stream_boundary_message(&domain, &relay, &registry, &services, &batch)
        .await
        .expect("dispatch should succeed");
    acks.ack_success();

    let first_message = first_consumer
        .recv()
        .await
        .expect("first runtime consumer should receive message");
    let second_message = second_consumer
        .recv()
        .await
        .expect("second runtime consumer should receive message");
    for ack in first_message.acks.iter().chain(second_message.acks.iter()) {
        ack.ack_success();
    }

    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
}

#[tokio::test]
async fn concrete_relay_reuses_branch_collapse_for_runtime_consumers() {
    let runtime = super::Runtime::default();
    let domain = Domain::parse("default").expect("valid domain");
    let relay = Identifier::parse("notifications").expect("valid identifier");
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let registry = super::RelayRegistry::new();
    let branch_collapse = Arc::new(super::BranchCollapseNode::with_capacity(nonzero_capacity(
        STUPID_CHANNEL_CAPACITY_REMOVE_ME,
    )));
    let mut first_fan_in = super::RelayRuntimeFanIn::new(
        branch_collapse.runtime_consumer_receiver_for_mode(AckMode::Attached),
    );
    let mut second_fan_in = super::RelayRuntimeFanIn::new(
        branch_collapse.runtime_consumer_receiver_for_mode(AckMode::Attached),
    );
    let services = Arc::new(super::RelayBoundaryServices {
        fanout: super::RelayBoundaryFanout::BranchCollapse(branch_collapse),
        attached_runtime_consumer_count: 2,
        detached_runtime_consumer_count: 0,
        remote_runtime_consumers: Arc::from([]),
        remote_dispatcher: None,
    });
    let mut relay_runtime = super::ConcreteRelayRuntime::new(super::ConcreteRelayRuntimeBuild {
        runtime: runtime.clone(),
        domain: domain.clone(),
        relay: relay.clone(),
        registry,
        services,
        key: Some(concrete_branch_key([(
            identifier("user_id"),
            RuntimeValue::U32(52),
        )])),
    });
    let (acks, completion) = AckSet::root();
    let batch = super::RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 52),
        RuntimeRecord::from_fields([("user_id".to_string(), RuntimeValue::U32(52))]),
        acks.clone(),
    )
    .expect("batch should build");

    relay_runtime
        .dispatch_boundary(&batch)
        .await
        .expect("concrete relay should dispatch");

    let received = timeout(Duration::from_secs(1), first_fan_in.recv())
        .await
        .expect("first fan-in should receive")
        .expect("first fan-in should stay open");
    assert_eq!(received.message_count(), 1);
    for ack in received.acks {
        ack.ack_success();
    }
    let received = timeout(Duration::from_secs(1), second_fan_in.recv())
        .await
        .expect("second fan-in should receive")
        .expect("second fan-in should stay open");
    assert_eq!(received.message_count(), 1);
    for ack in received.acks {
        ack.ack_success();
    }
    acks.ack_success();
    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
}

#[tokio::test]
async fn unparameterized_relay_uses_direct_fanout_without_branch_collapse() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("notifications");

    let fanout = runtime
        .relay_boundary_fanout_with_capacity(
            &domain,
            &relay,
            false,
            nonzero_capacity(STUPID_CHANNEL_CAPACITY_REMOVE_ME),
        )
        .await;

    assert!(!fanout.uses_branch_collapse());
}

#[tokio::test]
async fn execution_builder_uses_direct_fanout_for_unparameterized_relay() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let schema = identifier("notification");
    let relay = identifier("notifications");

    runtime
        .rebuild_domain_from_schedule(
            "node-1",
            &domain,
            Some(DomainSchedule {
                domain: domain.clone(),
                nodes: vec![
                    scheduled_model(
                        ModelKind::Schema,
                        schema.clone(),
                        nervix_models::Model::Schema(CreateSchema {
                            name: schema.clone(),
                            fields: vec![nervix_models::SchemaField {
                                name: identifier("user_id"),
                                ty: ParseAsType::I64,
                                optional: false,
                                sensitive: false,
                            }],
                        }),
                    ),
                    scheduled_model(
                        ModelKind::Relay,
                        relay.clone(),
                        nervix_models::Model::Relay(CreateRelay {
                            name: relay.clone(),
                            schema,
                            buffer: STUPID_CHANNEL_CAPACITY_REMOVE_ME,
                            parameterization: RelayParameterization::unparameterized(),
                            materialized_state: None,
                        }),
                    ),
                ],
            }),
        )
        .await
        .expect("unparameterized relay execution should build");

    let execution = runtime
        .executions
        .get(&domain)
        .expect("domain execution should exist");
    let services = execution
        .relay_services
        .get(&relay)
        .expect("relay services should exist");
    assert!(!services.fanout.uses_branch_collapse());
}

#[tokio::test]
async fn recv_stream_message_batch_collects_until_flush_deadline() {
    let (sender, mut receiver) = mpsc::channel(TWO_ITEM_TEST_CHANNEL_CAPACITY);
    let schema = test_schema(&[]);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    sender
        .send(
            super::RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("tenant", "acme"),
                RuntimeRecord::from_fields([]),
                AckSet::empty(),
            )
            .expect("batch should build"),
        )
        .await
        .expect("first message should send");
    sender
        .send(
            super::RelayRecordBatch::single(
                schema,
                string_branch_key("tenant", "acme"),
                RuntimeRecord::from_fields([]),
                AckSet::empty(),
            )
            .expect("batch should build"),
        )
        .await
        .expect("second message should send");
    drop(sender);

    let batch = super::Runtime::recv_stream_message_batch(
        &mut receiver,
        &mut shutdown_rx,
        super::RuntimeFlushPolicy::Each {
            interval: Duration::from_millis(20),
            max_batch_size: u64::MAX,
        },
    )
    .await;

    let super::BatchedInput::Batch(batch) = batch else {
        panic!("expected message batch");
    };
    assert_eq!(batch.message_count(), 2);
    drop(shutdown_tx);
}

#[tokio::test]
async fn recv_stream_message_batch_flushes_when_max_batch_size_is_reached() {
    let (sender, mut receiver) = mpsc::channel(TWO_ITEM_TEST_CHANNEL_CAPACITY);
    let schema = test_schema(&[]);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    sender
        .send(
            super::RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("tenant", "acme"),
                RuntimeRecord::from_fields([]),
                AckSet::empty(),
            )
            .expect("first message should build"),
        )
        .await
        .expect("first message should send");
    sender
        .send(
            super::RelayRecordBatch::single(
                schema,
                string_branch_key("tenant", "acme"),
                RuntimeRecord::from_fields([]),
                AckSet::empty(),
            )
            .expect("second message should build"),
        )
        .await
        .expect("second message should send");

    let batch = super::Runtime::recv_stream_message_batch(
        &mut receiver,
        &mut shutdown_rx,
        super::RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: 0,
        },
    )
    .await;

    let super::BatchedInput::Batch(batch) = batch else {
        panic!("expected message batch");
    };
    assert_eq!(batch.message_count(), 1);
    drop(sender);
    drop(shutdown_tx);
}

#[tokio::test]
async fn recv_stream_message_batch_flush_immediate_drains_cached_batches() {
    let (sender, mut receiver) = mpsc::channel(TWO_ITEM_TEST_CHANNEL_CAPACITY);
    let schema = test_schema(&[]);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    sender
        .send(
            super::RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("tenant", "acme"),
                RuntimeRecord::from_fields([]),
                AckSet::empty(),
            )
            .expect("batch should build"),
        )
        .await
        .expect("first message should send");
    sender
        .send(
            super::RelayRecordBatch::single(
                schema,
                string_branch_key("tenant", "acme"),
                RuntimeRecord::from_fields([]),
                AckSet::empty(),
            )
            .expect("batch should build"),
        )
        .await
        .expect("second message should send");

    let batch = super::Runtime::recv_stream_message_batch(
        &mut receiver,
        &mut shutdown_rx,
        super::RuntimeFlushPolicy::Immediate,
    )
    .await;

    let super::BatchedInput::Batch(batch) = batch else {
        panic!("expected message batch");
    };
    assert_eq!(batch.message_count(), 2);
    drop(sender);
    drop(shutdown_tx);
}

#[tokio::test]
async fn recv_runtime_consumer_batch_flush_immediate_drains_cached_batches() {
    let fanout = super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(
        TWO_ITEM_TEST_CHANNEL_CAPACITY,
    ));
    let schema = test_schema(&[]);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let mut receiver =
        super::RelayRuntimeFanIn::new(fanout.runtime_consumer_receiver_for_mode(AckMode::Attached));
    let broadcast = match &fanout {
        super::RelayBoundaryFanout::Direct(fanout) => {
            fanout.runtime_consumer_broadcast_for_mode(AckMode::Attached)
        }
        super::RelayBoundaryFanout::BranchCollapse(_) => {
            panic!("test services should use direct fanout")
        }
    };

    broadcast
        .broadcast(
            super::RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("tenant", "acme"),
                RuntimeRecord::from_fields([]),
                AckSet::empty(),
            )
            .expect("first batch should build"),
        )
        .await
        .expect("first batch should send");
    broadcast
        .broadcast(
            super::RelayRecordBatch::single(
                schema,
                string_branch_key("tenant", "acme"),
                RuntimeRecord::from_fields([]),
                AckSet::empty(),
            )
            .expect("second batch should build"),
        )
        .await
        .expect("second batch should send");

    let batch = super::Runtime::recv_runtime_consumer_batch(
        &mut receiver,
        &mut shutdown_rx,
        super::RuntimeFlushPolicy::Immediate,
    )
    .await;

    let super::BatchedInput::Batch(batch) = batch else {
        panic!("expected runtime consumer batch");
    };
    assert_eq!(batch.message_count(), 2);
    drop(shutdown_tx);
}

#[tokio::test]
async fn recv_stream_message_batch_flushes_collected_messages_on_shutdown() {
    let (sender, mut receiver) = mpsc::channel(STUPID_CHANNEL_CAPACITY_REMOVE_ME);
    let schema = test_schema(&[]);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    sender
        .send(
            super::RelayRecordBatch::single(
                schema,
                string_branch_key("tenant", "acme"),
                RuntimeRecord::from_fields([]),
                AckSet::empty(),
            )
            .expect("message should build"),
        )
        .await
        .expect("message should send");

    let shutdown_task = tokio::spawn(async move {
        tokio::task::yield_now().await;
        shutdown_tx.send(true).expect("shutdown should send");
    });
    let batch = super::Runtime::recv_stream_message_batch(
        &mut receiver,
        &mut shutdown_rx,
        super::RuntimeFlushPolicy::Each {
            interval: Duration::from_secs(60),
            max_batch_size: u64::MAX,
        },
    )
    .await;
    shutdown_task.await.expect("shutdown task should join");
    let super::BatchedInput::Batch(batch) = batch else {
        panic!("expected collected messages to flush on shutdown");
    };
    assert_eq!(batch.message_count(), 1);
    drop(sender);
}

#[test]
fn relay_record_batches_can_be_concatenated_without_losing_metadata() {
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let left = super::RelayRecordBatch::single(
        schema.clone(),
        u32_branch_key("user_id", 42),
        RuntimeRecord::from_fields([("user_id".to_string(), RuntimeValue::U32(42))])
            .with_ingested_at_watermarks(Timestamp::from_unix_nanos(100)),
        AckSet::empty(),
    )
    .expect("left batch should build");
    let right = super::RelayRecordBatch::single(
        schema,
        u32_branch_key("user_id", 42),
        RuntimeRecord::from_fields([("user_id".to_string(), RuntimeValue::U32(43))])
            .with_ingested_at_watermarks(Timestamp::from_unix_nanos(200)),
        AckSet::empty(),
    )
    .expect("right batch should build");

    let concatenated =
        super::RelayRecordBatch::concat(vec![left, right]).expect("batches should concat");

    assert_eq!(concatenated.batch.batch().num_rows(), 2);
    let messages = concatenated
        .try_into_messages()
        .expect("concatenated batch should decode");
    assert_eq!(messages.len(), 2);
    assert_eq!(
        messages[0].record.metadata().ingested_at_low_watermark(),
        Timestamp::from_unix_nanos(100)
    );
    assert_eq!(
        messages[1].record.metadata().ingested_at_low_watermark(),
        Timestamp::from_unix_nanos(200)
    );
}

#[tokio::test]
async fn remote_stream_payload_touches_expiring_stream_state() {
    let runtime = super::Runtime::default();
    let domain = Domain::parse("default").expect("valid domain");
    let relay_id = Identifier::parse("notifications").expect("valid identifier");
    let expiring_state = runtime.expiring_stream_state(&domain, &relay_id, Duration::from_secs(1));
    let registry = expiring_state.registry.clone();
    let services = test_relay_boundary_services();
    let (shutdown, _) = watch::channel(false);
    let mut relay_registries = HashMap::default();
    relay_registries.insert(relay_id.clone(), registry);
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let mut relay_schemas = HashMap::default();
    relay_schemas.insert(relay_id.clone(), schema.clone());
    let mut relay_services = HashMap::default();
    relay_services.insert(relay_id.clone(), services);
    runtime.executions.insert(
        domain.clone(),
        super::DomainExecution {
            schedule: DomainSchedule {
                domain: domain.clone(),
                nodes: Vec::new(),
            },
            passive_only: false,
            shutdown,
            graph: Arc::new(ArcSwapOption::empty()),
            relay_registries,
            relay_schemas,
            relay_services,
            relay_parameterizations: HashMap::default(),
            relay_parameterization_schemas: HashMap::default(),
            materialized_stream_specs: HashMap::default(),
            materialized_stream_owner_nodes: HashMap::default(),
            parameterized_ingestors: HashMap::default(),
            parameterized_entrypoints: HashMap::default(),
            codecs: HashMap::default(),
            signaling_protocols: HashMap::default(),
            lookups: HashMap::default(),
            endpoint_routes: HashMap::default(),
            tasks: Vec::new(),
        },
    );
    let batch_ipc = schema
        .arrow_batch_from_records(&[RuntimeRecord::from_fields([(
            "user_id".to_string(),
            RuntimeValue::U32(42),
        )])])
        .expect("batch should build")
        .to_arrow_ipc_bytes()
        .expect("batch ipc should serialize");

    let key = u32_branch_key("user_id", 42);
    runtime
        .handle_remote_stream(RelayPayload {
            kind: RelayPayloadKind::Routed,
            domain: domain.clone(),
            relay: relay_id.clone(),
            key: BranchKey::to_remote_key(&key),
            batch_ipc,
            metadata: vec![
                RuntimeRecord::from_fields([("user_id".to_string(), RuntimeValue::U32(42))])
                    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(42))
                    .metadata()
                    .to_remote(),
            ],
            acks: vec![None],
        })
        .await
        .expect("remote relay payload should dispatch");

    assert!(
        runtime
            .describe_local_stream_exists(&domain, &relay_id, &key)
            .expect("stream existence should be queryable")
    );
}

#[tokio::test]
async fn stop_domain_execution_preserves_expiring_relay_branch_registry() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("notifications");
    let branch = string_branch_key("tenant", "acme");
    let expiring_state = runtime.expiring_stream_state(&domain, &relay, Duration::from_secs(60));
    expiring_state.touch(&branch, Timestamp::from_unix_nanos(1));
    let (shutdown, _) = watch::channel(false);

    runtime
        .stop_domain_execution(
            &domain,
            super::DomainExecution {
                schedule: DomainSchedule {
                    domain: domain.clone(),
                    nodes: Vec::new(),
                },
                passive_only: false,
                shutdown,
                graph: Arc::new(ArcSwapOption::empty()),
                relay_registries: HashMap::default(),
                relay_schemas: HashMap::default(),
                relay_services: HashMap::default(),
                relay_parameterizations: HashMap::default(),
                relay_parameterization_schemas: HashMap::default(),
                materialized_stream_specs: HashMap::default(),
                materialized_stream_owner_nodes: HashMap::default(),
                parameterized_ingestors: HashMap::default(),
                parameterized_entrypoints: HashMap::default(),
                codecs: HashMap::default(),
                signaling_protocols: HashMap::default(),
                lookups: HashMap::default(),
                endpoint_routes: HashMap::default(),
                tasks: Vec::new(),
            },
        )
        .await;

    assert!(expiring_state.contains_key(&branch));
}

#[test]
fn lookup_queries_surface_recorded_domain_instantiation_errors() {
    let runtime = super::Runtime::new();
    runtime.domain_instantiation_errors.insert(
        domain("default"),
        "failed to build domain execution for 'default': lookup load failed".to_string(),
    );

    let error = runtime
        .query_local_lookup(&domain("default"), &identifier("zip_codes"), "99926")
        .expect_err("lookup should surface stored instantiation errors");

    assert!(error.contains("failed to build domain execution for 'default'"));
    assert!(error.contains("lookup load failed"));
}

#[tokio::test]
async fn describe_ingestor_surfaces_instantiation_error_when_runtime_is_missing() {
    let runtime = super::Runtime::new();
    let domain = domain("default");
    let ingestor = identifier("mqtt_notifications");
    runtime.domain_instantiation_errors.insert(
        domain.clone(),
        "failed to build domain execution for 'default': ingestor start failed".to_string(),
    );
    let (shutdown, _) = watch::channel(false);
    runtime.executions.insert(
        domain.clone(),
        super::DomainExecution {
            schedule: DomainSchedule {
                domain: domain.clone(),
                nodes: Vec::new(),
            },
            passive_only: false,
            shutdown,
            graph: Arc::new(ArcSwapOption::empty()),
            relay_registries: HashMap::default(),
            relay_schemas: HashMap::default(),
            relay_services: HashMap::default(),
            relay_parameterizations: HashMap::default(),
            relay_parameterization_schemas: HashMap::default(),
            materialized_stream_specs: HashMap::default(),
            materialized_stream_owner_nodes: HashMap::default(),
            parameterized_ingestors: HashMap::default(),
            parameterized_entrypoints: HashMap::default(),
            codecs: HashMap::default(),
            signaling_protocols: HashMap::default(),
            lookups: HashMap::default(),
            endpoint_routes: HashMap::default(),
            tasks: Vec::new(),
        },
    );

    let describe = runtime
        .describe_local_ingestor(&domain, &ingestor)
        .expect("describe should succeed");

    assert!(!describe.running);
    assert!(
        describe
            .transient_error
            .as_deref()
            .is_some_and(|error| error.contains("ingestor start failed")),
        "describe should expose domain instantiation error, got {:?}",
        describe.transient_error
    );
}

#[test]
fn runtime_uses_configured_timestamp_field_when_present() {
    let runtime = super::Runtime::new();
    let record = RuntimeRecord::from_fields([(
        "occurred_at".to_string(),
        RuntimeValue::Datetime(
            chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z").expect("valid timestamp"),
        ),
    )])
    .with_ingested_at_watermarks(Timestamp::from_unix_nanos(1));

    let timestamp = runtime
        .resolve_ingested_record_timestamp(
            &domain("paced"),
            &identifier("ing"),
            Some(&IngestTimestampSource::At(identifier("occurred_at"))),
            &record,
        )
        .expect("timestamp should resolve");

    assert_eq!(
        timestamp,
        Timestamp::from(
            chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z")
                .expect("valid timestamp")
                .to_utc()
        )
    );
}

#[test]
fn runtime_uses_ingested_watermark_for_timestamp_now() {
    let runtime = super::Runtime::new();
    let record = RuntimeRecord::from_fields([])
        .with_ingested_at_watermarks(Timestamp::from_unix_nanos(9_876_543));

    let timestamp = runtime
        .resolve_ingested_record_timestamp(
            &domain("paced"),
            &identifier("ing"),
            Some(&IngestTimestampSource::Now),
            &record,
        )
        .expect("timestamp should resolve");

    assert_eq!(timestamp, Timestamp::from_unix_nanos(9_876_543));
}

#[test]
fn paced_domain_requires_explicit_timestamp_source() {
    let runtime = super::Runtime::new();
    let mut domains = BTreeMap::new();
    domains.insert(domain("paced"), paced_domain_state("paced"));
    runtime.sync_domains(&domains);

    let error = runtime
        .resolve_ingested_record_timestamp(
            &domain("paced"),
            &identifier("ing"),
            None,
            &RuntimeRecord::from_fields([])
                .with_ingested_at_watermarks(Timestamp::from_unix_nanos(1)),
        )
        .expect_err("paced domain should require explicit timestamp source");

    assert!(error.contains("TIMESTAMP NOW or TIMESTAMP AT <field>"));
}

#[test]
fn paced_domains_accept_records_inside_tick_window() {
    let runtime = super::Runtime::new();
    let mut domains = BTreeMap::new();
    domains.insert(domain("paced"), paced_domain_state("paced"));
    runtime.sync_domains(&domains);
    runtime.handle_domain_tick(
        &domain("paced"),
        &DomainTick {
            tick_id: 1,
            logical_timestamp: Timestamp::from_unix_nanos(0),
            wall_clock: Timestamp::from_unix_nanos(10_000_000_000),
            duration_ms: 1_000,
        },
    );

    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("paced"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(10_200_000_000),
            )
            .is_ok()
    );
    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("paced"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(10_400_000_000),
            )
            .is_err()
    );
}

#[test]
fn paced_domains_accept_records_while_clock_is_running_before_ticks_arrive() {
    let runtime = super::Runtime::new();
    let mut domains = BTreeMap::new();
    domains.insert(domain("paced"), paced_domain_state("paced"));
    runtime.sync_domains(&domains);
    runtime.handle_domain_clock_start(
        &domain("paced"),
        Timestamp::from_unix_nanos(10_000_000_000),
        Timestamp::from_unix_nanos(10_000_000_000),
        "1.0",
    );

    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("paced"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(10_200_000_000),
            )
            .is_ok()
    );
    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("paced"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(11_200_000_000),
            )
            .is_ok()
    );
}

#[test]
fn sync_domains_clears_ticks_when_paced_domain_stops() {
    let runtime = super::Runtime::new();
    let mut domains = BTreeMap::new();
    domains.insert(domain("paced"), paced_domain_state("paced"));
    runtime.sync_domains(&domains);
    runtime.handle_domain_tick(
        &domain("paced"),
        &DomainTick {
            tick_id: 1,
            logical_timestamp: Timestamp::from_unix_nanos(0),
            wall_clock: Timestamp::from_unix_nanos(10_000_000_000),
            duration_ms: 1_000,
        },
    );

    domains.insert(
        domain("paced"),
        DomainState {
            id: domain("paced"),
            config: DomainConfig {
                pace: DomainPace::Paced,
                period: "1s".to_string(),
                skew: "250ms".to_string(),
            },
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: nervix_models::DomainStartPoint::Resume,
        },
    );
    runtime.sync_domains(&domains);

    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("paced"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(10_000_000),
            )
            .is_err()
    );
}

#[test]
fn stopped_unpaced_domain_rejects_ingestion() {
    let runtime = super::Runtime::new();
    let mut domains = BTreeMap::new();
    domains.insert(
        domain("default"),
        DomainState {
            id: domain("default"),
            config: DomainConfig {
                pace: DomainPace::Unpaced,
                period: "1s".to_string(),
                skew: "0ms".to_string(),
            },
            status: DomainStatus::Stopped,
            start_version: 0,
            last_start: nervix_models::DomainStartPoint::Resume,
        },
    );
    runtime.sync_domains(&domains);

    assert!(
        runtime
            .ensure_domain_allows_ingestion(
                &domain("default"),
                &identifier("ing"),
                Timestamp::from_unix_nanos(10_000_000),
            )
            .is_err()
    );
}

#[tokio::test]
async fn direct_fanout_subscription_uses_configured_buffer_capacity() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("orders");
    let schema = test_schema(&[]);
    let fanout = runtime
        .relay_boundary_fanout_with_capacity(&domain, &relay, false, nonzero_capacity(1))
        .await;
    let direct_fanout = match &fanout {
        super::RelayBoundaryFanout::Direct(fanout) => fanout.clone(),
        super::RelayBoundaryFanout::BranchCollapse(_) => {
            panic!("unparameterized relay must use direct fanout")
        }
    };
    let mut receiver = fanout.subscription_receiver();

    direct_fanout
        .subscriptions
        .broadcast(
            super::RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("branch", "first"),
                RuntimeRecord::from_fields([]),
                AckSet::empty(),
            )
            .expect("first batch should build"),
        )
        .await
        .expect("first send should succeed");

    let pending_send = tokio::spawn({
        let direct_fanout = direct_fanout.clone();
        async move {
            direct_fanout
                .subscriptions
                .broadcast(
                    super::RelayRecordBatch::single(
                        schema,
                        string_branch_key("branch", "second"),
                        RuntimeRecord::from_fields([]),
                        AckSet::empty(),
                    )
                    .expect("second batch should build"),
                )
                .await
        }
    });

    sleep(Duration::from_millis(50)).await;
    assert!(
        !pending_send.is_finished(),
        "second send must wait for receiver capacity"
    );

    let first = receiver
        .recv()
        .await
        .expect("receiver should get first batch");
    assert_eq!(key_label(&first.key), r#"{"branch":"first"}"#);

    pending_send
        .await
        .expect("pending send should join")
        .expect("second send should succeed");

    let second = receiver
        .recv()
        .await
        .expect("receiver should get second batch");
    assert_eq!(key_label(&second.key), r#"{"branch":"second"}"#);
}

#[tokio::test]
async fn relay_boundary_fanout_resize_preserves_existing_subscription_receiver() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("orders");
    let schema = test_schema(&[]);
    let fanout = runtime
        .relay_boundary_fanout_with_capacity(&domain, &relay, false, nonzero_capacity(1))
        .await;
    let mut receiver = fanout.subscription_receiver();
    let resized = runtime
        .relay_boundary_fanout_with_capacity(&domain, &relay, false, nonzero_capacity(5))
        .await;

    let broadcast = match (&fanout, &resized) {
        (
            super::RelayBoundaryFanout::Direct(original),
            super::RelayBoundaryFanout::Direct(resized_fanout),
        ) => {
            assert!(Arc::ptr_eq(original, resized_fanout));
            assert_eq!(resized_fanout.subscriptions.capacity(), 5);
            assert_eq!(resized_fanout.attached_runtime_consumers.capacity(), 5);
            assert_eq!(resized_fanout.detached_runtime_consumers.capacity(), 5);
            &resized_fanout.subscriptions
        }
        _ => panic!("unparameterized relay must use direct fanout"),
    };

    broadcast
        .broadcast(
            super::RelayRecordBatch::single(
                schema,
                string_branch_key("branch", "after_resize"),
                RuntimeRecord::from_fields([]),
                AckSet::empty(),
            )
            .expect("batch should build"),
        )
        .await
        .expect("send after resize should succeed");

    let batch = receiver
        .recv()
        .await
        .expect("existing receiver should get batch after resize");
    assert_eq!(key_label(&batch.key), r#"{"branch":"after_resize"}"#);
}

#[test]
fn resolve_concrete_branch_returns_root_for_empty_parameterization() {
    let record = RuntimeRecord::from_fields([
        (
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        ),
        ("user_id".to_string(), RuntimeValue::U32(42)),
    ]);

    assert_eq!(
        super::resolve_concrete_branch(&record, &[], &identifier("ing"))
            .expect("root branch should resolve"),
        super::ConcreteBranch::Root
    );
}

#[test]
fn resolve_concrete_branch_uses_parameter_values() {
    let record = RuntimeRecord::from_fields([
        (
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        ),
        ("user_id".to_string(), RuntimeValue::U32(42)),
    ]);

    assert_eq!(
        super::resolve_concrete_branch(
            &record,
            &[identifier("tenant"), identifier("user_id")],
            &identifier("ing")
        )
        .expect("keyed branch should resolve"),
        super::ConcreteBranch::Key(concrete_branch_key([
            (
                identifier("tenant"),
                RuntimeValue::String("acme".to_string()),
            ),
            (identifier("user_id"), RuntimeValue::U32(42)),
        ]))
    );
}

#[test]
fn resolve_concrete_branch_mapping_can_read_branch_key() {
    let record = RuntimeRecord::from_fields([(
        "tenant".to_string(),
        RuntimeValue::String("message-tenant".to_string()),
    )]);
    let branch_key = concrete_branch_key([(
        identifier("tenant"),
        RuntimeValue::String("branch-tenant".to_string()),
    )]);

    assert_eq!(
        super::resolve_concrete_branch_from_mappings(
            &record,
            Some(&branch_key),
            &[ParameterValueMapping {
                field: identifier("tenant"),
                relay: identifier("branch"),
                relay_field: identifier("tenant"),
            }],
            &identifier("reingestor")
        )
        .expect("branch mapping should resolve"),
        super::ConcreteBranch::Key(concrete_branch_key([(
            identifier("tenant"),
            RuntimeValue::String("branch-tenant".to_string()),
        )]))
    );
}

#[test]
fn resolve_concrete_branch_errors_when_parameter_field_is_missing() {
    let record = RuntimeRecord::from_fields([(
        "tenant".to_string(),
        RuntimeValue::String("acme".to_string()),
    )]);

    let error =
        super::resolve_concrete_branch(&record, &[identifier("user_id")], &identifier("ing"))
            .expect_err("missing parameterized field should fail");

    assert!(error.contains("parameterized field 'user_id' is missing"));
    assert!(error.contains("'ing'"));
}

#[test]
fn normalize_http_host_strips_port_and_normalizes_case() {
    assert_eq!(
        super::normalize_http_host(" Example.COM:8080 "),
        "example.com"
    );
    assert_eq!(
        super::normalize_http_host("api.example.com"),
        "api.example.com"
    );
}

#[test]
fn next_retry_delay_doubles_and_caps() {
    let policy = super::ParsedRetryPolicy {
        backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(1),
    };

    assert_eq!(
        super::next_retry_delay(policy.backoff, policy),
        Duration::from_millis(200)
    );
    assert_eq!(
        super::next_retry_delay(Duration::from_millis(700), policy),
        Duration::from_secs(1)
    );
}

#[test]
fn parse_mqtt_addr_handles_valid_and_invalid_inputs() {
    assert_eq!(
        super::ingestors::mqtt::MqttIngestor::parse_addr(
            "mqtt://user:pass@broker.example.com:1883/topic"
        )
        .expect("must parse"),
        super::ingestors::mqtt::MqttIngestorAddr {
            host: "broker.example.com".to_string(),
            port: 1883,
            tls: false,
        }
    );
    assert_eq!(
        super::ingestors::mqtt::MqttIngestor::parse_addr("mqtts://broker.example.com:8883")
            .expect("must parse"),
        super::ingestors::mqtt::MqttIngestorAddr {
            host: "broker.example.com".to_string(),
            port: 8883,
            tls: true,
        }
    );
    assert_eq!(
        super::ingestors::mqtt::MqttIngestor::parse_addr("mqtt://[2001:db8::1]:1883/topic")
            .expect("must parse"),
        super::ingestors::mqtt::MqttIngestorAddr {
            host: "2001:db8::1".to_string(),
            port: 1883,
            tls: false,
        }
    );
    assert_eq!(
        super::ingestors::mqtt::MqttIngestor::parse_addr(
            "mqtt://broker.example.com:1883?keep_alive=30"
        )
        .expect("must parse"),
        super::ingestors::mqtt::MqttIngestorAddr {
            host: "broker.example.com".to_string(),
            port: 1883,
            tls: false,
        }
    );
    assert!(
        super::ingestors::mqtt::MqttIngestor::parse_addr("http://broker.example.com:1883").is_err()
    );
    assert!(super::ingestors::mqtt::MqttIngestor::parse_addr("mqtt://broker.example.com").is_err());
    assert!(super::ingestors::mqtt::MqttIngestor::parse_addr("mqtt://:1883").is_err());
}

#[test]
fn url_scheme_detection_uses_url_parser() {
    assert!(
        super::ServiceUrl::new(
            "amqps://guest:guest@[2001:db8::1]:5671/%2f?heartbeat=30",
            "RabbitMQ addr"
        )
        .has_scheme("amqps")
        .expect("must parse")
    );
    assert!(
        super::ServiceUrl::new("rediss://127.0.0.1:6380/?protocol=resp3", "Redis addr")
            .has_scheme("rediss")
            .expect("must parse")
    );
    assert!(
        super::ServiceUrl::new("tls://127.0.0.1:4223?name=nervix", "NATS addr")
            .has_scheme("tls")
            .expect("must parse")
    );
    assert!(
        !super::ServiceUrl::new("amqp://guest:guest@127.0.0.1:5672/%2f", "RabbitMQ addr")
            .has_scheme("amqps")
            .expect("must parse")
    );
    assert_eq!(
        super::ServiceUrl::new("wss://example.com/socket?token=abc", "WebSockets endpoint")
            .scheme()
            .expect("must parse"),
        "wss"
    );
    assert!(
        super::ServiceUrl::new("not a url", "RabbitMQ addr")
            .has_scheme("amqps")
            .is_err()
    );
}

#[test]
fn mqtt_client_builder_uses_configured_or_default_client_id() {
    let client = CreateClientMqtt {
        name: identifier("mqtt_main"),
        mount: None,
        config: vec![nervix_models::ClientConfigEntry {
            key: "addr".to_string(),
            value: "mqtt://broker.example.com:1883".to_string(),
        }],
    };

    super::ingestors::mqtt::MqttIngestor::client_from_client(&client, "default-client")
        .expect("must build client from default id");

    let client_with_id = CreateClientMqtt {
        name: identifier("mqtt_main"),
        mount: None,
        config: vec![
            nervix_models::ClientConfigEntry {
                key: "addr".to_string(),
                value: "mqtt://broker.example.com:1883".to_string(),
            },
            nervix_models::ClientConfigEntry {
                key: "client_id".to_string(),
                value: "explicit-client".to_string(),
            },
        ],
    };

    super::ingestors::mqtt::MqttIngestor::client_from_client(&client_with_id, "default-client")
        .expect("must build client from explicit id");
}

#[test]
fn prometheus_helpers_render_payload_and_validate_inputs() {
    let sample = super::ingestors::prometheus::PrometheusVectorResult {
        metric: BTreeMap::from([("source".to_string(), "local".to_string())]),
        value: (1_735_782_245.25, "12.5".to_string()),
    };

    let timestamp =
        super::ingestors::prometheus::PrometheusIngestor::timestamp_to_rfc3339(sample.value.0)
            .expect("valid ts");
    assert!(timestamp.starts_with("2025-"));

    let payload = super::ingestors::prometheus::PrometheusIngestor::sample_payload(&sample)
        .expect("must render");
    let value: serde_json::Value = serde_json::from_slice(&payload).expect("valid json");
    assert_eq!(value["source"], "local");
    assert_eq!(value["value"], 12.5);
    assert_eq!(value["timestamp"], timestamp);

    let bad_value = super::ingestors::prometheus::PrometheusVectorResult {
        metric: BTreeMap::new(),
        value: (1.0, "NaN".to_string()),
    };
    assert!(super::ingestors::prometheus::PrometheusIngestor::sample_payload(&bad_value).is_err());
    assert!(
        super::ingestors::prometheus::PrometheusIngestor::timestamp_to_rfc3339(f64::INFINITY)
            .is_err()
    );
}

#[test]
fn prometheus_query_url_uses_url_parser_for_path_and_query() {
    let url = super::ingestors::prometheus::PrometheusIngestor::query_url(
        "http://prometheus:9090/base/?stale=true",
        vec![("query".to_string(), "vector(1)".to_string())],
    )
    .expect("must build url");
    assert_eq!(
        url.as_str(),
        "http://prometheus:9090/base/api/v1/query?query=vector%281%29"
    );
}

#[test]
fn runtime_duration_parsers_validate_and_report_context() {
    let domain = domain("default");
    let ingestor = identifier("orders_ingestor");

    assert_eq!(
        super::Runtime::parse_ack_timeout(&domain, &ingestor, "2s").expect("valid timeout"),
        Duration::from_secs(2)
    );
    assert_eq!(
        super::Runtime::parse_duration_setting(&domain, &ingestor, "batch timeout", "250ms")
            .expect("valid duration"),
        Duration::from_millis(250)
    );

    let err = super::Runtime::parse_ack_timeout(&domain, &ingestor, "oops")
        .expect_err("invalid ack timeout");
    assert!(
        matches!(err, super::RuntimeError::StartIngestor { reason, .. } if reason.contains("invalid ack timeout 'oops'"))
    );

    let err = super::Runtime::parse_duration_setting(&domain, &ingestor, "batch timeout", "oops")
        .expect_err("invalid duration");
    assert!(
        matches!(err, super::RuntimeError::StartIngestor { reason, .. } if reason.contains("invalid batch timeout 'oops'"))
    );

    let retry = RetryPolicy {
        backoff: "100ms".to_string(),
        max_backoff: "1s".to_string(),
    };
    let parsed =
        super::Runtime::parse_retry_policy(&domain, &ingestor, &retry).expect("valid retry policy");
    assert_eq!(parsed.backoff, Duration::from_millis(100));
    assert_eq!(parsed.max_backoff, Duration::from_secs(1));

    let bad_retry = RetryPolicy {
        backoff: "oops".to_string(),
        max_backoff: "1s".to_string(),
    };
    let err = super::Runtime::parse_retry_policy(&domain, &ingestor, &bad_retry)
        .expect_err("invalid retry backoff");
    assert!(
        matches!(err, super::RuntimeError::StartIngestor { reason, .. } if reason.contains("invalid retry backoff 'oops'"))
    );

    let bad_max_retry = RetryPolicy {
        backoff: "100ms".to_string(),
        max_backoff: "oops".to_string(),
    };
    let err = super::Runtime::parse_retry_policy(&domain, &ingestor, &bad_max_retry)
        .expect_err("invalid retry max_backoff");
    assert!(
        matches!(err, super::RuntimeError::StartIngestor { reason, .. } if reason.contains("retry max backoff") && reason.contains("oops"))
    );
}

#[tokio::test]
async fn client_resource_mounts_expand_into_runtime_paths() {
    let store_root = tempdir().expect("resource store tempdir");
    let source_root = tempdir().expect("resource source tempdir");
    let ca_path = source_root.path().join("ca.pem");
    std::fs::write(&ca_path, "test-ca").expect("ca file should be written");

    let store = ResourceStore::open(store_root.path()).expect("resource store should open");
    store
        .install_from_directory(
            identifier("dev_tls"),
            1,
            source_root.path(),
            "node-1",
            Timestamp::from_unix_nanos(0),
        )
        .await
        .expect("resource version should install");

    let runtime = super::Runtime::new();
    runtime.attach_resources(
        Arc::new(store),
        ResourceVersionStatus {
            next_version_by_identifier: SortedVec::from_unsorted(vec![(identifier("dev_tls"), 2)]),
            versions: SortedVec::from_unsorted(vec![ResourceVersion {
                id: ResourceId::new(identifier("dev_tls"), 1),
                root_checksum: "root".to_string(),
                manifest_checksum: "manifest".to_string(),
                file_count: 1,
                total_bytes: 7,
                created_at: Timestamp::from_unix_nanos(0),
                created_by_node: "node-1".to_string(),
            }]),
            replicas: SortedVec::new(),
        },
    );

    let resolved = runtime
        .resolve_client_config(
            Some(&identifier("dev_tls")),
            &[ClientConfigEntry {
                key: "tls_ca_file".to_string(),
                value: "{{ dev_tls }}/ca.pem".to_string(),
            }],
        )
        .expect("client config should resolve");

    assert!(resolved.mounts.is_some());
    assert_eq!(resolved.entries.len(), 1);
    let mounted_ca = PathBuf::from(&resolved.entries[0].value);
    assert!(mounted_ca.ends_with("ca.pem"));
    assert_eq!(
        std::fs::read_to_string(&mounted_ca).expect("mounted ca should be readable"),
        "test-ca"
    );
}

#[test]
fn client_resource_mounts_reject_unknown_placeholders() {
    let runtime = super::Runtime::new();
    let error = runtime
        .resolve_client_config(
            None,
            &[ClientConfigEntry {
                key: "tls_ca_file".to_string(),
                value: "{{dev_tls}}/ca.pem".to_string(),
            }],
        )
        .expect_err("unknown placeholder should fail");
    assert!(error.contains("failed to render client config template"));
}

#[test]
fn client_config_instance_placeholder_renders_for_concrete_instance() {
    let runtime = super::Runtime::new();
    let resolved = runtime
        .resolve_client_config_with_instance(
            None,
            &[ClientConfigEntry {
                key: "client_id".to_string(),
                value: "mqtt-client-{{instance}}".to_string(),
            }],
            7,
        )
        .expect("instance placeholder should resolve");

    assert_eq!(resolved.entries[0].value, "mqtt-client-7");
}

#[test]
fn client_config_extractors_handle_defaults_and_missing_keys() {
    let zeromq = CreateClientZeroMq {
        name: identifier("zmq"),
        mount: None,
        config: vec![
            ClientConfigEntry {
                key: "addr".to_string(),
                value: "tcp://127.0.0.1:5555".to_string(),
            },
            ClientConfigEntry {
                key: "bind".to_string(),
                value: "TRUE".to_string(),
            },
        ],
    };
    assert_eq!(
        super::ingestors::zeromq::ZeroMqIngestor::addr_from_client(&zeromq).expect("addr"),
        "tcp://127.0.0.1:5555"
    );
    assert!(super::ingestors::zeromq::ZeroMqIngestor::bind_from_client(
        &zeromq
    ));

    let http = CreateClientHttp {
        name: identifier("http"),
        mount: None,
        config: vec![ClientConfigEntry {
            key: "endpoint".to_string(),
            value: "https://example.com/api".to_string(),
        }],
    };
    assert_eq!(
        super::ingestors::http::HttpIngestor::endpoint_from_client(&http).expect("endpoint"),
        "https://example.com/api"
    );
    assert_eq!(
        super::ingestors::http::HttpIngestor::method_from_client(&http).expect("default method"),
        reqwest::Method::GET
    );

    let http_post = CreateClientHttp {
        name: identifier("http"),
        mount: None,
        config: vec![
            ClientConfigEntry {
                key: "endpoint".to_string(),
                value: "https://example.com/api".to_string(),
            },
            ClientConfigEntry {
                key: "method".to_string(),
                value: "POST".to_string(),
            },
        ],
    };
    assert_eq!(
        super::ingestors::http::HttpIngestor::method_from_client(&http_post).expect("post method"),
        reqwest::Method::POST
    );
    assert!(
        super::ingestors::http::HttpIngestor::method_from_client(&CreateClientHttp {
            name: identifier("http"),
            mount: None,
            config: vec![ClientConfigEntry {
                key: "method".to_string(),
                value: "NOT A METHOD".to_string(),
            }],
        })
        .is_err()
    );

    let websocket = CreateClientWebsockets {
        name: identifier("ws"),
        mount: None,
        signaling_protocol: None,
        config: vec![ClientConfigEntry {
            key: "endpoint".to_string(),
            value: "wss://example.com/socket".to_string(),
        }],
    };
    assert_eq!(
        super::ingestors::websockets::WebsocketsIngestor::endpoint_from_client(&websocket)
            .expect("endpoint"),
        "wss://example.com/socket"
    );

    let prometheus = CreateClientPrometheus {
        name: identifier("prom"),
        mount: None,
        config: vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: "http://prometheus:9090".to_string(),
        }],
    };
    assert_eq!(
        super::ingestors::prometheus::PrometheusIngestor::addr_from_client(&prometheus)
            .expect("addr"),
        "http://prometheus:9090"
    );

    let zeromq_default = CreateClientZeroMq {
        name: identifier("zmq"),
        mount: None,
        config: vec![ClientConfigEntry {
            key: "addr".to_string(),
            value: "tcp://127.0.0.1:5555".to_string(),
        }],
    };
    assert!(!super::ingestors::zeromq::ZeroMqIngestor::bind_from_client(
        &zeromq_default
    ));

    assert!(
        super::ingestors::zeromq::ZeroMqIngestor::addr_from_client(&CreateClientZeroMq {
            name: identifier("zmq"),
            mount: None,
            config: vec![],
        })
        .expect_err("missing zeromq addr")
        .contains("missing ZeroMQ client config key 'addr'")
    );
    assert!(
        super::ingestors::http::HttpIngestor::endpoint_from_client(&CreateClientHttp {
            name: identifier("http"),
            mount: None,
            config: vec![],
        })
        .expect_err("missing http endpoint")
        .contains("missing HTTP client config key 'endpoint'")
    );
    assert!(
        super::ingestors::websockets::WebsocketsIngestor::endpoint_from_client(
            &CreateClientWebsockets {
                name: identifier("ws"),
                mount: None,
                signaling_protocol: None,
                config: vec![],
            }
        )
        .expect_err("missing websocket endpoint")
        .contains("missing WebSockets client config key 'endpoint'")
    );
    assert!(
        super::ingestors::prometheus::PrometheusIngestor::addr_from_client(
            &CreateClientPrometheus {
                name: identifier("prom"),
                mount: None,
                config: vec![],
            }
        )
        .expect_err("missing prometheus addr")
        .contains("missing Prometheus client config key 'addr'")
    );
}

#[test]
fn clickhouse_client_config_validates_tls_ca_file() {
    let error = match super::emitters::clickhouse::ClickHouseEmitter::client_from_config(&[
        ClientConfigEntry {
            key: "addr".to_string(),
            value: "https://127.0.0.1:8124".to_string(),
        },
        ClientConfigEntry {
            key: "tls_ca_file".to_string(),
            value: "/tmp/nervix-missing-clickhouse-ca.pem".to_string(),
        },
    ]) {
        Ok(_) => panic!("missing ClickHouse TLS CA should fail"),
        Err(error) => error,
    };
    let error = format!("{error:?}");

    assert!(error.contains("TLS CA certificate"));
}

#[test]
fn pulsar_tls_options_load_certificate_chain_and_flags() {
    let tempdir = tempdir().expect("tempdir should be created");
    let ca_path = tempdir.path().join("ca.pem");
    std::fs::write(&ca_path, "test-ca").expect("ca file should be written");

    let options = super::emitters::pulsar::PulsarEmitter::tls_options_from_config(&[
        ClientConfigEntry {
            key: "tls_ca_file".to_string(),
            value: ca_path.display().to_string(),
        },
        ClientConfigEntry {
            key: "tls_allow_insecure_connection".to_string(),
            value: "true".to_string(),
        },
        ClientConfigEntry {
            key: "tls_hostname_verification_enabled".to_string(),
            value: "false".to_string(),
        },
    ])
    .expect("pulsar tls options should load")
    .expect("tls options should be present");

    assert_eq!(
        options
            .certificate_chain
            .expect("certificate chain should be present"),
        b"test-ca".to_vec()
    );
    assert!(options.allow_insecure_connection);
    assert!(!options.tls_hostname_verification_enabled);
}

#[test]
fn pulsar_tls_options_reject_client_auth_material() {
    let error = super::emitters::pulsar::PulsarEmitter::tls_options_from_config(&[
        ClientConfigEntry {
            key: "tls_cert_file".to_string(),
            value: "/tmp/client.crt".to_string(),
        },
        ClientConfigEntry {
            key: "tls_key_file".to_string(),
            value: "/tmp/client.key".to_string(),
        },
    ])
    .expect_err("pulsar mTLS material should be rejected");
    let error = format!("{error:?}");

    assert!(error.contains("tls_cert_file"));
    assert!(error.contains("tls_key_file"));
}

#[test]
fn pulsar_tls_options_reject_invalid_boolean_values() {
    let error =
        super::emitters::pulsar::PulsarEmitter::tls_options_from_config(&[ClientConfigEntry {
            key: "tls_allow_insecure_connection".to_string(),
            value: "maybe".to_string(),
        }])
        .expect_err("invalid pulsar tls boolean should be rejected");
    let error = format!("{error:?}");

    assert!(error.contains("tls_allow_insecure_connection"));
    assert!(error.contains("maybe"));
}

#[test]
fn http_and_prometheus_clients_validate_timeout_configuration() {
    let client = super::HttpClientConfig::new(
        &[ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "250".to_string(),
        }],
        "HTTP",
    )
    .build();
    assert!(client.is_ok());

    let err = super::HttpClientConfig::new(
        &[ClientConfigEntry {
            key: "timeout_ms".to_string(),
            value: "oops".to_string(),
        }],
        "HTTP",
    )
    .build()
    .expect_err("invalid timeout");
    assert!(err.contains("invalid HTTP timeout_ms 'oops'"));

    let err = super::ingestors::prometheus::PrometheusIngestor::client_from_client(
        &CreateClientPrometheus {
            name: identifier("prom"),
            mount: None,
            config: vec![ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "oops".to_string(),
            }],
        },
    )
    .expect_err("invalid prometheus timeout");
    assert!(err.contains("Prometheus timeout_ms"));
}

#[test]
fn kinesis_start_position_defaults_to_latest_and_accepts_trim_horizon() {
    assert!(matches!(
        super::ingestors::kinesis::KinesisIngestor::start_position_from_config(&[])
            .expect("default start position"),
        super::ingestors::kinesis::KinesisStartPosition::Latest
    ));
    assert!(matches!(
        super::ingestors::kinesis::KinesisIngestor::start_position_from_config(&[
            ClientConfigEntry {
                key: "start_position".to_string(),
                value: "trim_horizon".to_string(),
            }
        ])
        .expect("trim horizon start position"),
        super::ingestors::kinesis::KinesisStartPosition::TrimHorizon
    ));
    assert!(
        super::ingestors::kinesis::KinesisIngestor::start_position_from_config(&[
            ClientConfigEntry {
                key: "start_position".to_string(),
                value: "consumer_group".to_string(),
            }
        ])
        .expect_err("invalid kinesis start position")
        .contains("unsupported Kinesis client start_position")
    );
}

#[test]
fn mqtt_client_builder_requires_addr_and_retry_delay_handles_overflow() {
    let err = super::ingestors::mqtt::MqttIngestor::client_from_client(
        &CreateClientMqtt {
            name: identifier("mqtt_main"),
            mount: None,
            config: vec![],
        },
        "default-client",
    )
    .err()
    .expect("missing mqtt addr");
    assert!(err.contains("missing MQTT client config key 'addr'"));

    let policy = super::ParsedRetryPolicy {
        backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(10),
    };
    assert_eq!(
        super::next_retry_delay(Duration::MAX, policy),
        Duration::from_secs(10)
    );
}

#[test]
fn parameterized_ingestor_specs_capture_downstream_processing_tree() {
    let specs = super::parameterized_ingestor_specs_from_models(
        [
            (
                ModelKind::Ingestor,
                identifier("orders_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("orders_ingestor"),
                    output_routes: ProcessorOutputs::single(identifier("orders")),
                    decode_using_codec: identifier("orders_codec"),
                    parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                    },
                    error_policies: ErrorPolicies::handled_by_log(),
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_orders"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_orders"),
                    from_relay: identifier("orders"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("projected_orders")),
                    parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                    deduplicate_on: "projected_orders.order_id".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_projected_orders"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_projected_orders"),
                    from_relay: identifier("projected_orders"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("aggregated_orders")),
                    parameterized_by: parameterized_by("tenant", "projected_orders", &["tenant"]),
                    deduplicate_on: "tenant_orders.order_id".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Emitter,
                identifier("orders_emitter"),
                nervix_models::Model::Emitter(CreateEmitter {
                    name: identifier("orders_emitter"),
                    from_relay: identifier("aggregated_orders"),
                    encode_using_codec: Some(identifier("orders_codec")),
                    sink: EmitSink::ZeroMq {
                        client: identifier("zmq_client"),
                    },
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    error_policies: ErrorPolicies::handled_by_log(),
                    filter_map: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.len(), 1);
    let spec = &specs[0];
    assert_eq!(spec.identifier, identifier("orders_ingestor"));
    assert_eq!(spec.root_relay, identifier("orders"));
    assert_eq!(spec.roots.len(), 1);
    assert_eq!(spec.roots[0].processor, identifier("dedup_orders"));
    let ParameterizedProcessorOperationSpec::Deduplicator { output_routes, .. } =
        &spec.roots[0].operation
    else {
        panic!("expected deduplicator output");
    };
    let output = output_routes
        .routes
        .first()
        .expect("deduplicator should have output route");
    assert_eq!(output.children.len(), 1);
    assert_eq!(
        output.children[0].processor,
        identifier("dedup_projected_orders")
    );
}

#[test]
fn parameterized_ingestor_specs_capture_window_processor_as_branch_node() {
    let specs = super::parameterized_ingestor_specs_from_models(
        [
            (
                ModelKind::Ingestor,
                identifier("metrics_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("metrics_ingestor"),
                    output_routes: ProcessorOutputs::single(identifier("metrics")),
                    decode_using_codec: identifier("metrics_codec"),
                    parameterized_by: parameterized_by("host", "metrics", &["host"]),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                    },
                    error_policies: ErrorPolicies::handled_by_log(),
                    filter_where: None,
                }),
                Some(vec![identifier("host")]),
            ),
            (
                ModelKind::WindowProcessor,
                identifier("metric_window"),
                nervix_models::Model::WindowProcessor(CreateWindowProcessor {
                    name: identifier("metric_window"),
                    from_relay: identifier("metrics"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("metric_summary")),
                    parameterized_by: parameterized_by("host", "metrics", &["host"]),
                    width: WindowBound {
                        messages: Some(100),
                        duration: None,
                    },
                    step: WindowBound {
                        messages: Some(10),
                        duration: None,
                    },
                    mode: AckMode::Attached,
                    aggregate: "metric_summary.count = COUNT(metrics.latency)".to_string(),
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("host")]),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_summary"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_summary"),
                    from_relay: identifier("metric_summary"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("projected_summary")),
                    parameterized_by: parameterized_by("host", "metric_summary", &["host"]),
                    deduplicate_on: "metric_summary.count".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("host")]),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.len(), 1);
    let spec = &specs[0];
    assert_eq!(spec.root_relay, identifier("metrics"));
    assert_eq!(spec.roots.len(), 1);
    assert_eq!(spec.roots[0].processor, identifier("metric_window"));
    let ParameterizedProcessorOperationSpec::WindowProcessor {
        output_routes,
        width,
        step,
        aggregate,
    } = &spec.roots[0].operation
    else {
        panic!("expected window processor branch node");
    };
    let output = output_routes
        .routes
        .first()
        .expect("window processor should have output route");
    assert_eq!(output.relay, identifier("metric_summary"));
    assert_eq!(output.children.len(), 1);
    assert_eq!(output.children[0].processor, identifier("dedup_summary"));
    assert_eq!(width.messages, Some(100));
    assert_eq!(step.messages, Some(10));
    assert!(aggregate.contains("COUNT(metrics.latency)"));
}

#[test]
fn parameterized_ingestor_specs_capture_inferencer_as_branch_node() {
    let specs = super::parameterized_ingestor_specs_from_models(
        [
            (
                ModelKind::Ingestor,
                identifier("features_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("features_ingestor"),
                    output_routes: ProcessorOutputs::single(identifier("features")),
                    decode_using_codec: identifier("features_codec"),
                    parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                    },
                    error_policies: ErrorPolicies::handled_by_log(),
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Inferencer,
                identifier("score_model"),
                nervix_models::Model::Inferencer(CreateInferencer {
                    name: identifier("score_model"),
                    from_relay: identifier("features"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("scores")),
                    parameterized_by: parameterized_by("tenant", "features", &["tenant"]),
                    resource: identifier("fraud_model"),
                    resource_version: Some(3),
                    file: "models/fraud.onnx".to_string(),
                    inputs: vec![InferencerTensorMapping {
                        tensor: "features".to_string(),
                        relay: identifier("features"),
                        field: identifier("vector"),
                    }],
                    outputs: vec![InferencerTensorMapping {
                        tensor: "score".to_string(),
                        relay: identifier("scores"),
                        field: identifier("score"),
                    }],
                    flush_each: "IMMEDIATE".to_string(),
                    max_batch_size: None,
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: Some("WHERE active".to_string()),
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_scores"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_scores"),
                    from_relay: identifier("scores"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("projected_scores")),
                    parameterized_by: parameterized_by("tenant", "scores", &["tenant"]),
                    deduplicate_on: "scores.score".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.len(), 1);
    let spec = &specs[0];
    assert_eq!(spec.root_relay, identifier("features"));
    assert_eq!(spec.roots.len(), 1);
    assert_eq!(spec.roots[0].processor, identifier("score_model"));
    let ParameterizedProcessorOperationSpec::Inferencer {
        output_routes,
        resource,
        resource_version,
        file,
        inputs,
        outputs,
        flush_each,
        ..
    } = &spec.roots[0].operation
    else {
        panic!("expected inferencer branch node");
    };
    let output = output_routes
        .routes
        .first()
        .expect("inferencer should have output route");
    assert_eq!(output.relay, identifier("scores"));
    assert_eq!(output.children.len(), 1);
    assert_eq!(output.children[0].processor, identifier("dedup_scores"));
    assert_eq!(resource, &identifier("fraud_model"));
    assert_eq!(*resource_version, Some(3));
    assert_eq!(file, "models/fraud.onnx");
    assert_eq!(inputs.len(), 1);
    assert_eq!(outputs.len(), 1);
    assert_eq!(flush_each, "IMMEDIATE");
    assert_eq!(spec.roots[0].filter_where.as_deref(), Some("WHERE active"));
}

#[test]
fn parameterized_ingestor_specs_capture_reingestor_entrypoint_tree() {
    let specs = super::parameterized_ingestor_specs_from_models(
        [
            (
                ModelKind::Reingestor,
                identifier("tenant_partition"),
                nervix_models::Model::Reingestor(CreateReingestor {
                    name: identifier("tenant_partition"),
                    from_relay: identifier("orders"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("tenant_orders")),
                    parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_orders"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_orders"),
                    from_relay: identifier("tenant_orders"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("projected_orders")),
                    parameterized_by: parameterized_by("tenant", "tenant_orders", &["tenant"]),
                    deduplicate_on: "urgent_orders.order_id".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.len(), 1);
    let spec = &specs[0];
    assert_eq!(spec.kind, ModelKind::Reingestor);
    assert_eq!(spec.identifier, identifier("tenant_partition"));
    assert_eq!(spec.root_relay, identifier("tenant_orders"));
    assert_eq!(spec.roots.len(), 1);
    assert_eq!(spec.roots[0].processor, identifier("dedup_orders"));
}

#[test]
fn parameterized_ingestor_specs_capture_processor_output_route_tree() {
    let specs = super::parameterized_ingestor_specs_from_models(
        [
            (
                ModelKind::Ingestor,
                identifier("orders_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("orders_ingestor"),
                    output_routes: ProcessorOutputs::single(identifier("orders")),
                    decode_using_codec: identifier("orders_codec"),
                    parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                    },
                    error_policies: ErrorPolicies::handled_by_log(),
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Deduplicator,
                identifier("orders_splitter"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("orders_splitter"),
                    from_relay: identifier("orders"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::new(vec![
                        ProcessorOutput {
                            relay: identifier("urgent_orders"),
                            filter_map: Some("WHERE urgent".to_string()),
                        },
                        ProcessorOutput {
                            relay: identifier("default_orders"),
                            filter_map: None,
                        },
                    ]),
                    parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                    deduplicate_on: "orders.order_id".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: Some("WHERE active".to_string()),
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_urgent"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_urgent"),
                    from_relay: identifier("urgent_orders"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("urgent_projected")),
                    parameterized_by: parameterized_by("tenant", "urgent_orders", &["tenant"]),
                    deduplicate_on: "default_orders.order_id".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_default"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_default"),
                    from_relay: identifier("default_orders"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("default_projected")),
                    parameterized_by: parameterized_by("tenant", "default_orders", &["tenant"]),
                    deduplicate_on: "projected_orders.order_id".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.len(), 1);
    let spec = &specs[0];
    assert_eq!(spec.roots.len(), 1);
    assert_eq!(spec.roots[0].processor, identifier("orders_splitter"));
    let ParameterizedProcessorOperationSpec::Deduplicator { output_routes, .. } =
        &spec.roots[0].operation
    else {
        panic!("expected deduplicator output routes");
    };
    assert_eq!(spec.roots[0].filter_where.as_deref(), Some("WHERE active"));
    assert_eq!(output_routes.routes.len(), 2);
    assert_eq!(
        output_routes.routes[0].filter_map.as_deref(),
        Some("WHERE urgent")
    );
    assert_eq!(output_routes.routes[0].children.len(), 1);
    assert_eq!(
        output_routes.routes[0].children[0].processor,
        identifier("dedup_urgent")
    );
    assert_eq!(output_routes.routes.len(), 2);
    assert_eq!(output_routes.routes[1].relay, identifier("default_orders"));
    assert_eq!(output_routes.routes[1].children.len(), 1);
    assert_eq!(
        output_routes.routes[1].children[0].processor,
        identifier("dedup_default")
    );
}

#[test]
fn parameterized_ingestor_specs_capture_unifier_as_single_branch_processor() {
    let specs = super::parameterized_ingestor_specs_from_models(
        [
            (
                ModelKind::Ingestor,
                identifier("left_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("left_ingestor"),
                    output_routes: ProcessorOutputs::single(identifier("left_stream")),
                    decode_using_codec: identifier("notification_codec"),
                    parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                    },
                    error_policies: ErrorPolicies::handled_by_log(),

                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Ingestor,
                identifier("right_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("right_ingestor"),
                    output_routes: ProcessorOutputs::single(identifier("right_stream")),
                    decode_using_codec: identifier("notification_codec"),
                    parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                    },
                    error_policies: ErrorPolicies::handled_by_log(),

                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Unifier,
                identifier("join_streams"),
                nervix_models::Model::Unifier(CreateUnifier {
                    name: identifier("join_streams"),
                    from_relays: vec![identifier("left_stream"), identifier("right_stream")],
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("joined_stream")),
                    parameterized_by: parameterized_by("tenant", "left_stream", &["tenant"]),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_joined"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_joined"),
                    from_relay: identifier("joined_stream"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("projected_joined")),
                    parameterized_by: parameterized_by("tenant", "joined_stream", &["tenant"]),
                    deduplicate_on: "joined_stream.tenant".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.len(), 2);
    for spec in &specs {
        assert_eq!(
            spec.processors
                .iter()
                .filter(|processor| processor.processor == identifier("join_streams"))
                .count(),
            1
        );
        let unifier = spec
            .processors
            .iter()
            .find(|processor| processor.processor == identifier("join_streams"))
            .expect("unifier should be reachable");
        assert_eq!(
            unifier.input_relays,
            vec![identifier("left_stream"), identifier("right_stream")]
        );
        let ParameterizedProcessorOperationSpec::Unifier { output_routes, .. } = &unifier.operation
        else {
            panic!("expected unifier processor");
        };
        let output = output_routes
            .routes
            .first()
            .expect("unifier should have output route");
        assert_eq!(output.relay, identifier("joined_stream"));
        assert!(
            spec.processors
                .iter()
                .any(|processor| processor.processor == identifier("dedup_joined"))
        );
    }
}

#[test]
fn parameterized_ingestor_specs_capture_single_processor_output_route_tree() {
    let specs = super::parameterized_ingestor_specs_from_models(
        [
            (
                ModelKind::Ingestor,
                identifier("orders_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("orders_ingestor"),
                    output_routes: ProcessorOutputs::single(identifier("orders")),
                    decode_using_codec: identifier("orders_codec"),
                    parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                    },
                    error_policies: ErrorPolicies::handled_by_log(),

                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Deduplicator,
                identifier("orders_filter"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("orders_filter"),
                    from_relay: identifier("orders"),
                    from_where: vec![ProcessorInputWhere {
                        relay: identifier("orders"),
                        where_clause: "WHERE orders.active".to_string(),
                    }],
                    output_routes: ProcessorOutputs::single(identifier("projected_orders")),
                    parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                    deduplicate_on: "orders.order_id".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: Some("WHERE active".to_string()),
                }),
                Some(vec![identifier("tenant")]),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_projected"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_projected"),
                    from_relay: identifier("projected_orders"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("aggregated_orders")),
                    parameterized_by: parameterized_by("tenant", "projected_orders", &["tenant"]),
                    deduplicate_on: "orders.order_id".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(vec![identifier("tenant")]),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.len(), 1);
    let spec = &specs[0];
    assert_eq!(spec.roots.len(), 1);
    assert_eq!(spec.roots[0].processor, identifier("orders_filter"));
    assert_eq!(
        spec.roots[0].from_where.get(&identifier("orders")),
        Some(&"WHERE orders.active".to_string())
    );
    let ParameterizedProcessorOperationSpec::Deduplicator { output_routes, .. } =
        &spec.roots[0].operation
    else {
        panic!("expected processor output routes");
    };
    assert_eq!(spec.roots[0].filter_where.as_deref(), Some("WHERE active"));
    assert_eq!(output_routes.routes.len(), 1);
    assert_eq!(
        output_routes.routes[0].relay,
        identifier("projected_orders")
    );
    assert_eq!(output_routes.routes[0].children.len(), 1);
    assert_eq!(
        output_routes.routes[0].children[0].processor,
        identifier("dedup_projected")
    );
}

#[test]
fn parameterized_ingestor_specs_include_singleton_branch_for_empty_parameterization() {
    let specs = super::parameterized_ingestor_specs_from_models(
        [
            (
                ModelKind::Ingestor,
                identifier("orders_ingestor"),
                nervix_models::Model::Ingestor(CreateIngestor {
                    name: identifier("orders_ingestor"),
                    output_routes: ProcessorOutputs::single(identifier("orders")),
                    decode_using_codec: identifier("orders_codec"),
                    parameterized_by: parameterized_by("root", "orders", &[]),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    timestamp_source: None,
                    source: IngestSource::ZeroMq {
                        client: identifier("zmq_client"),
                        mode: ZeroMqIngestMode::NoAckSequential,
                    },
                    error_policies: ErrorPolicies::handled_by_log(),

                    filter_where: None,
                }),
                Some(Vec::new()),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_orders"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_orders"),
                    from_relay: identifier("orders"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("projected_orders")),
                    parameterized_by: parameterized_by("root", "orders", &[]),
                    deduplicate_on: "orders.order_id".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(Vec::new()),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].identifier, identifier("orders_ingestor"));
    assert_eq!(specs[0].root_relay, identifier("orders"));
    assert_eq!(specs[0].roots.len(), 1);
    assert_eq!(specs[0].roots[0].processor, identifier("dedup_orders"));
}

#[test]
fn parameterized_ingestor_specs_use_explicit_unparameterized_relay_as_root() {
    let specs = super::parameterized_ingestor_specs_from_models(
        [
            (
                ModelKind::Relay,
                identifier("orders"),
                nervix_models::Model::Relay(CreateRelay {
                    name: identifier("orders"),
                    schema: identifier("order_event"),
                    buffer: 1,
                    parameterization: RelayParameterization::unparameterized(),
                    materialized_state: None,
                }),
                Some(Vec::new()),
            ),
            (
                ModelKind::Deduplicator,
                identifier("dedup_orders"),
                nervix_models::Model::Deduplicator(CreateDeduplicator {
                    name: identifier("dedup_orders"),
                    from_relay: identifier("orders"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("projected_orders")),
                    parameterized_by: BranchParameterization::unparameterized(),
                    deduplicate_on: "orders.order_id".to_string(),
                    max_time: "10m".to_string(),
                    flush_each: "100ms".to_string(),
                    max_batch_size: Some("1MiB".to_string()),
                    mode: AckMode::Attached,
                    message_error_policy: MessageErrorPolicy::Log,
                    filter_where: None,
                }),
                Some(Vec::new()),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].kind, ModelKind::Relay);
    assert_eq!(specs[0].identifier, identifier("orders"));
    assert_eq!(specs[0].root_relay, identifier("orders"));
    assert_eq!(specs[0].roots.len(), 1);
    assert_eq!(specs[0].roots[0].processor, identifier("dedup_orders"));
}

#[test]
fn parameterized_wasm_processor_specs_preserve_global_error_policy() {
    let specs = super::parameterized_ingestor_specs_from_models(
        [
            (
                ModelKind::Relay,
                identifier("orders"),
                nervix_models::Model::Relay(CreateRelay {
                    name: identifier("orders"),
                    schema: identifier("order_event"),
                    buffer: 1,
                    parameterization: RelayParameterization::unparameterized(),
                    materialized_state: None,
                }),
                Some(Vec::new()),
            ),
            (
                ModelKind::WasmProcessor,
                identifier("filter_orders"),
                nervix_models::Model::WasmProcessor(CreateWasmProcessor {
                    name: identifier("filter_orders"),
                    from_relay: identifier("orders"),
                    from_where: Vec::new(),
                    output_routes: ProcessorOutputs::single(identifier("filtered_orders")),
                    parameterized_by: BranchParameterization::unparameterized(),
                    resource: identifier("filter_resource"),
                    resource_version: None,
                    file: "filter.wasm".to_string(),
                    message_error_policy: MessageErrorPolicy::Log,
                    global_error_policy: GeneralErrorPolicy::Ignore,
                    mode: AckMode::Attached,
                    filter_where: None,
                }),
                Some(Vec::new()),
            ),
        ]
        .into_iter(),
    );

    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].roots.len(), 1);
    assert_eq!(
        specs[0].roots[0].error_policies.general,
        GeneralErrorPolicy::Ignore
    );
    assert_eq!(
        specs[0].roots[0].error_policies.message,
        MessageErrorPolicy::Log
    );
}

#[test]
fn parameterized_ingestor_specs_include_reingestor_with_declared_parameterization() {
    let specs = super::parameterized_ingestor_specs_from_models(
        [(
            ModelKind::Reingestor,
            identifier("tenant_partition"),
            nervix_models::Model::Reingestor(CreateReingestor {
                name: identifier("tenant_partition"),
                from_relay: identifier("notifications"),
                from_where: Vec::new(),
                output_routes: ProcessorOutputs::single(identifier("tenant_notifications")),
                parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: AckMode::Attached,
                message_error_policy: MessageErrorPolicy::Log,
                filter_where: None,
            }),
            None,
        )]
        .into_iter(),
    );

    assert_eq!(specs.len(), 1);
    assert_eq!(specs[0].identifier, identifier("tenant_partition"));
    assert_eq!(specs[0].root_relay, identifier("tenant_notifications"));
}

#[tokio::test]
async fn parameterized_root_without_children_acks_success() {
    let runtime = super::Runtime::default();
    let root_domain = domain("default");
    let root_relay = identifier("tenant_orders");
    let root_registry = super::RelayRegistry::new();
    let root_services = test_relay_boundary_services();
    let mut root = super::BranchRuntime {
        key: Some(concrete_branch_key([(
            identifier("tenant"),
            RuntimeValue::String("acme".to_string()),
        )])),
        runtime: runtime.clone(),
        domain: root_domain.clone(),
        source_kind: ModelKind::Ingestor,
        source: identifier("metric_ingestor"),
        root_relay: root_relay.clone(),
        error_policies: ErrorPolicies::handled_by_log(),
        relays: [(
            root_relay.clone(),
            super::ConcreteRelayRuntime::new(super::ConcreteRelayRuntimeBuild {
                runtime,
                domain: root_domain,
                relay: root_relay,
                registry: root_registry,
                services: root_services,
                key: Some(concrete_branch_key([(
                    identifier("tenant"),
                    RuntimeValue::String("acme".to_string()),
                )])),
            }),
        )]
        .into_iter()
        .collect(),
        materializers: HashMap::default(),
        processors: HashMap::default(),
        processors_by_input: HashMap::default(),
    };
    let graph = Arc::new(ArcSwapOption::from(None));
    let (acks, completion) = AckSet::root();
    let schema = test_schema(&[("tenant", ParseAsType::String)]);

    root.dispatch(
        &graph,
        super::RelayRecordBatch::single(
            schema,
            string_branch_key("tenant", "acme"),
            RuntimeRecord::from_fields([(
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            )]),
            acks,
        )
        .expect("batch should build"),
    )
    .await;

    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("ack completion should resolve"),
        AckOutcome::Ack
    );
}

#[tokio::test]
async fn reingestor_parameterized_entrypoint_splits_batches_with_arrow_filters() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let root_relay = identifier("tenant_orders");
    let fanout = super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(
        TWO_ITEM_TEST_CHANNEL_CAPACITY,
    ));
    let mut fan_in =
        super::RelayRuntimeFanIn::new(fanout.runtime_consumer_receiver_for_mode(AckMode::Attached));
    let services = Arc::new(super::RelayBoundaryServices {
        fanout,
        attached_runtime_consumer_count: 1,
        detached_runtime_consumer_count: 0,
        remote_runtime_consumers: Arc::from([]),
        remote_dispatcher: None,
    });
    let schema = test_schema(&[("tenant", ParseAsType::String), ("value", ParseAsType::U32)]);
    let template = super::ParametrizerTemplate {
        source_kind: ModelKind::Reingestor,
        source: identifier("tenant_partition"),
        root_relay: root_relay.clone(),
        branch_ttl: None,
        entrypoint_schema: schema.clone(),
        entrypoint_parameter_mappings: parameterized_by("tenant", "orders", &["tenant"])
            .values()
            .to_vec(),
        entrypoint_ack_boundary: super::ParametrizerAckBoundary::Reingestor(AckMode::Attached),
        entrypoint_flush_each: super::RuntimeFlushPolicy::Immediate,
        error_policies: ErrorPolicies::handled_by_log(),
        relays: [(
            root_relay.clone(),
            super::RelayProcessorRelayTemplate {
                registry: super::RelayRegistry::new(),
                services,
            },
        )]
        .into_iter()
        .collect(),
        materialized_streams: HashSet::default(),
        processors: HashMap::default(),
        processors_by_input: HashMap::default(),
    };
    let input = super::RelayRecordBatch::from_messages(
        schema.clone(),
        vec![
            RelayMessage {
                key: string_branch_key("site", "north"),
                record: RuntimeRecord::from_fields([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("acme".to_string()),
                    ),
                    ("value".to_string(), RuntimeValue::U32(1)),
                ]),
                acks: AckSet::empty(),
            },
            RelayMessage {
                key: string_branch_key("site", "north"),
                record: RuntimeRecord::from_fields([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("beta".to_string()),
                    ),
                    ("value".to_string(), RuntimeValue::U32(2)),
                ]),
                acks: AckSet::empty(),
            },
            RelayMessage {
                key: string_branch_key("site", "north"),
                record: RuntimeRecord::from_fields([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String("acme".to_string()),
                    ),
                    ("value".to_string(), RuntimeValue::U32(3)),
                ]),
                acks: AckSet::empty(),
            },
        ],
    )
    .expect("batch should build");
    let graph = Arc::new(ArcSwapOption::from(None));
    let mut instances =
        ParametrizerRegistry::<Option<BranchKey>, Mutex<super::BranchRuntime>>::new();

    super::ParameterizedIngestorRuntime::dispatch_entrypoint_inputs(
        super::ParameterizedBranchDispatchContext {
            runtime_handle: &runtime,
            domain: &domain,
            ingestor: &identifier("tenant_partition"),
            graph: &graph,
            template: &template,
            now: Timestamp::from_unix_nanos(1_000_000_000),
        },
        &mut instances,
        vec![super::ParameterizedEntrypointInput::PendingParameterizationBatch(input)],
    )
    .await;

    let mut outputs = [
        timeout(Duration::from_secs(1), fan_in.recv())
            .await
            .expect("first output batch should arrive")
            .expect("runtime consumer should remain open"),
        timeout(Duration::from_secs(1), fan_in.recv())
            .await
            .expect("second output batch should arrive")
            .expect("runtime consumer should remain open"),
    ];
    outputs.sort_by(|left, right| key_label(&left.key).cmp(key_label(&right.key)));

    assert_eq!(key_label(&outputs[0].key), r#"{"tenant":"acme"}"#);
    assert_eq!(outputs[0].message_count(), 2);
    assert_eq!(key_label(&outputs[1].key), r#"{"tenant":"beta"}"#);
    assert_eq!(outputs[1].message_count(), 1);
}

#[tokio::test]
async fn reingestor_parameterized_entrypoint_reuses_existing_branches() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let root_relay = identifier("tenant_orders");
    let services = Arc::new(super::RelayBoundaryServices {
        fanout: super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(1)),
        attached_runtime_consumer_count: 0,
        detached_runtime_consumer_count: 0,
        remote_runtime_consumers: Arc::from([]),
        remote_dispatcher: None,
    });
    let schema = test_schema(&[("tenant", ParseAsType::String), ("value", ParseAsType::U32)]);
    let template = super::ParametrizerTemplate {
        source_kind: ModelKind::Reingestor,
        source: identifier("tenant_partition"),
        root_relay: root_relay.clone(),
        branch_ttl: None,
        entrypoint_schema: schema.clone(),
        entrypoint_parameter_mappings: parameterized_by("tenant", "orders", &["tenant"])
            .values()
            .to_vec(),
        entrypoint_ack_boundary: super::ParametrizerAckBoundary::Reingestor(AckMode::Detached),
        entrypoint_flush_each: super::RuntimeFlushPolicy::Immediate,
        error_policies: ErrorPolicies::handled_by_log(),
        relays: [(
            root_relay.clone(),
            super::RelayProcessorRelayTemplate {
                registry: super::RelayRegistry::new(),
                services,
            },
        )]
        .into_iter()
        .collect(),
        materialized_streams: HashSet::default(),
        processors: HashMap::default(),
        processors_by_input: HashMap::default(),
    };
    let graph = Arc::new(ArcSwapOption::from(None));
    let mut instances =
        ParametrizerRegistry::<Option<BranchKey>, Mutex<super::BranchRuntime>>::new();

    for round in 0..3 {
        let messages = (0..64)
            .map(|index| RelayMessage {
                key: string_branch_key("site", "north"),
                record: RuntimeRecord::from_fields([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String(format!("tenant-{index}")),
                    ),
                    ("value".to_string(), RuntimeValue::U32(round * 64 + index)),
                ]),
                acks: AckSet::empty(),
            })
            .collect::<Vec<_>>();
        let input = super::RelayRecordBatch::from_messages(schema.clone(), messages)
            .expect("batch should build");

        super::ParameterizedIngestorRuntime::dispatch_entrypoint_inputs(
            super::ParameterizedBranchDispatchContext {
                runtime_handle: &runtime,
                domain: &domain,
                ingestor: &identifier("tenant_partition"),
                graph: &graph,
                template: &template,
                now: Timestamp::from_unix_nanos(1_000_000_000 + i64::from(round)),
            },
            &mut instances,
            vec![super::ParameterizedEntrypointInput::PendingParameterizationBatch(input)],
        )
        .await;

        assert_eq!(instances.len(), 64);
    }
}

#[tokio::test]
async fn reingestor_propagates_attached_ack_into_parameterized_entrypoint() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let relay = identifier("tenant_orders");
    let output_registry = super::RelayRegistry::new();
    let output_services = test_relay_boundary_services();
    let mut output_subscription = output_services.subscription_receiver();
    let schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("user_id", ParseAsType::U32),
    ]);
    let parameterized_runtime = super::ParameterizedIngestorRuntime::new(
        runtime.clone(),
        domain.clone(),
        identifier("tenant_partition"),
        Arc::new(ArcSwapOption::from(None)),
        super::ParametrizerTemplate {
            source_kind: ModelKind::Reingestor,
            source: identifier("tenant_partition"),
            root_relay: relay.clone(),
            branch_ttl: Some(Duration::from_secs(30)),
            entrypoint_schema: schema.clone(),
            entrypoint_parameter_mappings: parameterized_by("tenant", "orders", &["tenant"])
                .values()
                .to_vec(),
            entrypoint_ack_boundary: super::ParametrizerAckBoundary::Reingestor(AckMode::Attached),
            entrypoint_flush_each: super::RuntimeFlushPolicy::Immediate,
            error_policies: ErrorPolicies::handled_by_log(),
            relays: [(
                relay.clone(),
                super::RelayProcessorRelayTemplate {
                    registry: output_registry,
                    services: output_services,
                },
            )]
            .into_iter()
            .collect(),
            materialized_streams: HashSet::default(),
            processors: HashMap::default(),
            processors_by_input: HashMap::default(),
        },
        Duration::from_secs(30),
    );
    assert_eq!(
        parameterized_runtime.sender().max_capacity(),
        super::STUPID_CHANNEL_CAPACITY_REMOVE_ME
    );
    let (shutdown_tx, _) = watch::channel(false);
    let broadcast =
        super::RelayBroadcast::with_capacity(nonzero_capacity(STUPID_CHANNEL_CAPACITY_REMOVE_ME));
    let fan_in = super::RelayRuntimeFanIn::new(broadcast.new_receiver());
    let mut parameterized_entrypoint_senders = HashMap::default();
    parameterized_entrypoint_senders.insert(relay, parameterized_runtime.sender());
    let task = runtime
        .spawn_reingestor_task(
            &domain,
            &shutdown_tx,
            &parameterized_entrypoint_senders,
            CreateReingestor {
                name: identifier("tenant_partition"),
                from_relay: identifier("orders"),
                from_where: Vec::new(),
                output_routes: ProcessorOutputs::single(identifier("tenant_orders")),
                parameterized_by: parameterized_by("tenant", "orders", &["tenant"]),
                flush_each: "100ms".to_string(),
                max_batch_size: Some("1MiB".to_string()),
                mode: AckMode::Attached,
                message_error_policy: MessageErrorPolicy::Log,
                filter_where: None,
            },
            fan_in,
        )
        .expect("reingestor task should spawn");
    let (acme_acks, acme_completion) = AckSet::root();
    let (beta_acks, beta_completion) = AckSet::root();

    broadcast
        .broadcast(
            super::RelayRecordBatch::from_messages(
                schema,
                vec![
                    RelayMessage {
                        key: string_branch_key("site", "north"),
                        record: RuntimeRecord::from_fields([
                            (
                                "tenant".to_string(),
                                RuntimeValue::String("acme".to_string()),
                            ),
                            ("user_id".to_string(), RuntimeValue::U32(42)),
                        ]),
                        acks: acme_acks.clone(),
                    },
                    RelayMessage {
                        key: string_branch_key("site", "north"),
                        record: RuntimeRecord::from_fields([
                            (
                                "tenant".to_string(),
                                RuntimeValue::String("beta".to_string()),
                            ),
                            ("user_id".to_string(), RuntimeValue::U32(7)),
                        ]),
                        acks: beta_acks.clone(),
                    },
                ],
            )
            .expect("batch should build"),
        )
        .await
        .expect("message should broadcast");
    acme_acks.ack_success();
    beta_acks.ack_success();

    assert_eq!(
        timeout(Duration::from_secs(1), acme_completion.wait())
            .await
            .expect("acme ack completion should resolve"),
        AckOutcome::Ack
    );
    assert_eq!(
        timeout(Duration::from_secs(1), beta_completion.wait())
            .await
            .expect("beta ack completion should resolve"),
        AckOutcome::Ack
    );
    let first_output_batch = timeout(Duration::from_secs(1), output_subscription.recv())
        .await
        .expect("first output subscription should receive")
        .expect("output subscription should stay open");
    let second_output_batch = timeout(Duration::from_secs(1), output_subscription.recv())
        .await
        .expect("second output subscription should receive")
        .expect("output subscription should stay open");
    let tenants = [first_output_batch, second_output_batch]
        .into_iter()
        .flat_map(|batch| {
            batch
                .try_into_messages()
                .expect("output batch should expand")
        })
        .filter_map(|output| match output.record.value("tenant") {
            Some(RuntimeValue::String(tenant)) => Some(tenant.clone()),
            _ => None,
        })
        .collect::<HashSet<_>>();
    assert_eq!(
        tenants,
        ["acme".to_string(), "beta".to_string()]
            .into_iter()
            .collect::<HashSet<_>>()
    );

    let _ = shutdown_tx.send(true);
    let _ = task.await;
    parameterized_runtime.shutdown().await;
}

#[tokio::test]
async fn canceled_parameterized_dispatch_does_not_leave_detached_branch_tasks() {
    let runtime = super::Runtime::default();
    let domain = domain("default");
    let root_relay = identifier("tenant_orders");
    let fanout = super::RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(1));
    let mut fan_in =
        super::RelayRuntimeFanIn::new(fanout.runtime_consumer_receiver_for_mode(AckMode::Attached));
    let services = Arc::new(super::RelayBoundaryServices {
        fanout: fanout.clone(),
        attached_runtime_consumer_count: 1,
        detached_runtime_consumer_count: 0,
        remote_runtime_consumers: Arc::from([]),
        remote_dispatcher: None,
    });
    let schema = test_schema(&[("tenant", ParseAsType::String)]);
    let template = super::ParametrizerTemplate {
        source_kind: ModelKind::Ingestor,
        source: identifier("metric_ingestor"),
        root_relay: root_relay.clone(),
        branch_ttl: None,
        entrypoint_schema: schema.clone(),
        entrypoint_parameter_mappings: parameterized_by("tenant", "orders", &["tenant"])
            .values()
            .to_vec(),
        entrypoint_ack_boundary: super::ParametrizerAckBoundary::Preserve,
        entrypoint_flush_each: super::RuntimeFlushPolicy::Immediate,
        error_policies: ErrorPolicies::handled_by_log(),
        relays: [(
            root_relay.clone(),
            super::RelayProcessorRelayTemplate {
                registry: super::RelayRegistry::new(),
                services,
            },
        )]
        .into_iter()
        .collect(),
        materialized_streams: HashSet::default(),
        processors: HashMap::default(),
        processors_by_input: HashMap::default(),
    };
    let inputs = (0..8)
        .map(
            |index| super::ParameterizedEntrypointInput::UnresolvedRecord {
                record: RuntimeRecord::from_fields([(
                    "tenant".to_string(),
                    RuntimeValue::String(format!("tenant-{index}")),
                )]),
                acks: AckSet::empty(),
            },
        )
        .collect::<Vec<_>>();
    let graph = Arc::new(ArcSwapOption::from(None));
    let dispatch_task = tokio::spawn({
        let runtime = runtime.clone();
        let domain = domain.clone();
        let ingestor = identifier("metric_ingestor");
        let template = template.clone();
        async move {
            let mut instances =
                ParametrizerRegistry::<Option<BranchKey>, Mutex<super::BranchRuntime>>::new();
            super::ParameterizedIngestorRuntime::dispatch_entrypoint_inputs(
                super::ParameterizedBranchDispatchContext {
                    runtime_handle: &runtime,
                    domain: &domain,
                    ingestor: &ingestor,
                    graph: &graph,
                    template: &template,
                    now: Timestamp::from_unix_nanos(1_000_000_000),
                },
                &mut instances,
                inputs,
            )
            .await
        }
    });

    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        tokio::task::consume_budget().await;
        if fanout.runtime_consumer_buffer_len_for_mode(AckMode::Attached) == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "parameterized dispatch should fill the bounded runtime consumer buffer"
        );
        sleep(Duration::from_millis(10)).await;
    }

    dispatch_task.abort();
    let _ = dispatch_task.await;

    let first = timeout(Duration::from_secs(1), fan_in.recv())
        .await
        .expect("queued branch batch should be readable")
        .expect("runtime consumer should remain open");
    assert_eq!(first.message_count(), 1);
    assert!(
        timeout(Duration::from_millis(100), fan_in.recv())
            .await
            .is_err(),
        "cancelled parameterized dispatch must not keep detached branch tasks that publish after \
         receiver capacity is freed"
    );
}

#[tokio::test]
async fn filter_map_lookup_hash_map_enriches_rows_and_filters_misses() {
    let input_schema = test_schema(&[
        ("id", ParseAsType::String),
        ("active", ParseAsType::Bool),
        ("title", ParseAsType::String),
    ]);
    let lookup_schema = test_schema(&[
        ("normalized_title", ParseAsType::String),
        ("city_name", ParseAsType::String),
    ]);
    let lookup = Arc::new(super::LookupRuntime {
        model: CreateLookup {
            name: identifier("titles_by_normalized"),
            key_field: identifier("normalized_title"),
            resource: identifier("titles_data"),
            path: "lookup.jsonl".to_string(),
            decode_using_codec: identifier("title_lookup_codec"),
        },
        resource_version: 1,
        schema: lookup_schema,
        entries: Arc::new(HashMap::from_iter([(
            "mr".to_string(),
            crate::runtime_schema::DecodedRecord::from_fields([(
                "city_name".to_string(),
                RuntimeValue::String("Chicago".to_string()),
            )]),
        )])),
    });
    let lookups = HashMap::from_iter([(identifier("titles_by_normalized"), lookup)]);
    let program = super::compile_filter_map_program(
        &domain("default"),
        &identifier("project_titles"),
        &[identifier("incoming_logs")],
        Some(
            "SET incoming_logs.city = LOOKUP_HASH_MAP(\"titles_by_normalized\", \
             lower(incoming_logs.title), \"city_name\") WHERE NOT \
             is_null(LOOKUP_HASH_MAP(\"titles_by_normalized\", lower(incoming_logs.title), \
             \"city_name\"))",
        ),
        input_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        Arc::new(compile_schema(&CreateSchema {
            name: identifier("lookup_output"),
            fields: vec![
                nervix_models::SchemaField {
                    name: identifier("id"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: identifier("active"),
                    ty: ParseAsType::Bool,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: identifier("title"),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                },
                nervix_models::SchemaField {
                    name: identifier("city"),
                    ty: ParseAsType::String,
                    optional: true,
                    sensitive: false,
                },
            ],
        }))
        .arrow_schema(),
        super::VmSchemaSensitivity::default(),
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &lookups,
            current_parameterization: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
        },
    )
    .expect("filter-map should compile")
    .expect("program should exist");
    assert_eq!(program.lookup_hash_maps.len(), 1);

    let (hit_acks, _hit_completion) = AckSet::root();
    let (miss_acks, _miss_completion) = AckSet::root();
    let batch = super::RelayRecordBatch::from_messages(
        input_schema,
        vec![
            RelayMessage {
                key: string_branch_key("tenant", "acme"),
                record: RuntimeRecord::from_fields([
                    ("id".to_string(), RuntimeValue::String("hit-1".to_string())),
                    ("active".to_string(), RuntimeValue::Bool(true)),
                    ("title".to_string(), RuntimeValue::String("MR".to_string())),
                ]),
                acks: hit_acks,
            },
            RelayMessage {
                key: string_branch_key("tenant", "acme"),
                record: RuntimeRecord::from_fields([
                    ("id".to_string(), RuntimeValue::String("miss-1".to_string())),
                    ("active".to_string(), RuntimeValue::Bool(true)),
                    (
                        "title".to_string(),
                        RuntimeValue::String("Unknown".to_string()),
                    ),
                ]),
                acks: miss_acks,
            },
        ],
    )
    .expect("batch should build");

    let plan = super::plan_filter_map_messages(
        "deduplicator",
        &identifier("project_titles"),
        "FILTER-MAP",
        &program,
        batch,
        super::current_timestamp(),
        &HashMap::default(),
    )
    .await
    .expect("filter-map planning should succeed");
    let messages = plan
        .batch
        .expect("filter-map should produce a batch")
        .try_into_messages()
        .expect("filter-map batch should convert to messages");

    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0].record.value("city"),
        Some(&RuntimeValue::String("Chicago".to_string()))
    );
}

#[tokio::test]
async fn filter_map_can_read_branch_namespace() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("amount", ParseAsType::I64),
        ("branch_tenant", ParseAsType::String),
    ]);
    let branch_schema = test_schema(&[("tenant", ParseAsType::String)]).arrow_schema();
    let program = super::compile_filter_map_program(
        &domain("default"),
        &identifier("project_notifications"),
        &[identifier("notifications")],
        Some(
            "SET notifications.branch_tenant = branch.tenant, notifications.amount = \
             notifications.amount + 1 WHERE branch.tenant = notifications.tenant",
        ),
        input_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        input_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_parameterization: &[identifier("tenant")],
            current_branch_schema: Some(&branch_schema),
            current_branch_sensitivity: None,
        },
    )
    .expect("filter-map should compile")
    .expect("program should exist");

    let (acks, _completion) = AckSet::root();
    let batch = super::RelayRecordBatch::from_messages(
        input_schema,
        vec![RelayMessage {
            key: string_branch_key("tenant", "acme"),
            record: RuntimeRecord::from_fields([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("amount".to_string(), RuntimeValue::I64(7)),
                (
                    "branch_tenant".to_string(),
                    RuntimeValue::String("".to_string()),
                ),
            ]),
            acks,
        }],
    )
    .expect("batch should build");

    let plan = super::plan_filter_map_messages(
        "deduplicator",
        &identifier("project_notifications"),
        "FILTER-MAP",
        &program,
        batch,
        super::current_timestamp(),
        &HashMap::default(),
    )
    .await
    .expect("filter-map planning should succeed");

    let messages = plan
        .batch
        .expect("filter-map should produce a batch")
        .try_into_messages()
        .expect("filter-map batch should convert to messages");

    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0].record.value("branch_tenant"),
        Some(&RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(
        messages[0].record.value("amount"),
        Some(&RuntimeValue::I64(8))
    );
}

#[tokio::test]
async fn filter_map_with_unset_can_read_branch_namespace() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("active", ParseAsType::Bool),
        ("amount", ParseAsType::I64),
    ]);
    let output_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("amount", ParseAsType::I64),
        ("branch_tenant", ParseAsType::String),
    ]);
    let branch_schema = test_schema(&[("tenant", ParseAsType::String)]).arrow_schema();
    let program = super::compile_filter_map_program(
        &domain("default"),
        &identifier("project_notifications"),
        &[identifier("notifications")],
        Some(
            "SET notifications.branch_tenant = branch.tenant, notifications.amount = \
             notifications.amount + 1 UNSET notifications.active WHERE branch.tenant = \
             notifications.tenant",
        ),
        input_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        output_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_parameterization: &[identifier("tenant")],
            current_branch_schema: Some(&branch_schema),
            current_branch_sensitivity: None,
        },
    )
    .expect("filter-map should compile")
    .expect("program should exist");

    let (acks, _completion) = AckSet::root();
    let batch = super::RelayRecordBatch::from_messages(
        input_schema,
        vec![RelayMessage {
            key: string_branch_key("tenant", "acme"),
            record: RuntimeRecord::from_fields([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("active".to_string(), RuntimeValue::Bool(true)),
                ("amount".to_string(), RuntimeValue::I64(7)),
            ]),
            acks,
        }],
    )
    .expect("batch should build");

    let plan = super::plan_filter_map_messages(
        "processor",
        &identifier("project_notifications"),
        "FILTER-MAP",
        &program,
        batch,
        super::current_timestamp(),
        &HashMap::default(),
    )
    .await
    .expect("filter-map planning should succeed");

    let messages = plan
        .batch
        .expect("filter-map should produce a batch")
        .try_into_messages()
        .expect("filter-map batch should convert to messages");

    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0].record.value("branch_tenant"),
        Some(&RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(
        messages[0].record.value("amount"),
        Some(&RuntimeValue::I64(8))
    );
    assert_eq!(
        messages[0].key.as_ref(),
        string_branch_key("tenant", "acme").as_ref()
    );
}

#[test]
fn filter_map_rejects_branch_namespace_without_branch_schema() {
    let schema = test_schema(&[("tenant", ParseAsType::String)]);
    let error = super::compile_filter_map_program(
        &domain("default"),
        &identifier("project_notifications"),
        &[identifier("notifications")],
        Some("WHERE branch.tenant = notifications.tenant"),
        schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_parameterization: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
        },
    )
    .expect_err("branch namespace must require a branch schema");
    let error = error.to_string();

    assert!(
        error.contains("branch.tenant") || error.contains("namespace 'branch'"),
        "expected branch namespace error, got {error}"
    );
}

#[test]
fn filter_map_rejects_missing_branch_key() {
    let schema = test_schema(&[("tenant", ParseAsType::String)]);
    let branch_schema = test_schema(&[("region", ParseAsType::String)]).arrow_schema();
    let error = super::compile_filter_map_program(
        &domain("default"),
        &identifier("project_notifications"),
        &[identifier("notifications")],
        Some("WHERE branch.tenant = notifications.tenant"),
        schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_parameterization: &[identifier("region")],
            current_branch_schema: Some(&branch_schema),
            current_branch_sensitivity: None,
        },
    )
    .expect_err("branch namespace must reject missing keys");
    let error = error.to_string();

    assert!(
        error.contains("branch.tenant") || error.contains("tenant"),
        "expected missing branch key error, got {error}"
    );
}

#[tokio::test]
async fn filter_map_internal_types_roundtrip_matches_http_logic_fixture() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("active", ParseAsType::Bool),
        ("u8", ParseAsType::U8),
        ("i8", ParseAsType::I8),
        ("u16", ParseAsType::U16),
        ("i16", ParseAsType::I16),
        ("u32", ParseAsType::U32),
        ("i32", ParseAsType::I32),
        ("u64", ParseAsType::U64),
        ("i64", ParseAsType::I64),
        ("f32", ParseAsType::F32),
        ("f64", ParseAsType::F64),
        ("occurred_at", ParseAsType::Datetime),
        ("raw", ParseAsType::String),
    ]);
    let output_schema = Arc::new(compile_schema(&CreateSchema {
        name: identifier("logic_output"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("u8_next"),
                ty: ParseAsType::U8,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("i8_abs"),
                ty: ParseAsType::I8,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("u16_keep"),
                ty: ParseAsType::U16,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("i16_prev"),
                ty: ParseAsType::I16,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("u32_same"),
                ty: ParseAsType::U32,
                optional: true,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("i32_neg"),
                ty: ParseAsType::I32,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("u64_next"),
                ty: ParseAsType::U64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("i64_keep"),
                ty: ParseAsType::I64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("f32_next"),
                ty: ParseAsType::F32,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("f64_keep"),
                ty: ParseAsType::F64,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("bool_copy"),
                ty: ParseAsType::Bool,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("occurred_text"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("occurred_copy"),
                ty: ParseAsType::Datetime,
                optional: false,
                sensitive: false,
            },
        ],
    }));
    let program = super::compile_ingestor_filter_map_program(
        &domain("default"),
        &identifier("logic_ingestor"),
        &identifier("logic_notifications"),
        &IngestSource::Endpoint {
            endpoint: identifier("logic_endpoint"),
            mode: nervix_models::EndpointIngestMode::NoAckSequential,
        },
        Some(
            "SET logic_notifications.u8_next = message.u8 + (1 AS U8), logic_notifications.i8_abs \
             = abs(message.i8), logic_notifications.u16_keep = coalesce(message.u16, (0 AS U16)), \
             logic_notifications.i16_prev = message.i16 - (1 AS I16), \
             logic_notifications.u32_same = coalesce(nullif(message.u32, (999 AS U32)), (0 AS \
             U32)), logic_notifications.i32_neg = -message.i32, logic_notifications.u64_next = \
             message.u64 + (2 AS U64), logic_notifications.i64_keep = message.i64, \
             logic_notifications.f32_next = message.f32 + (1.5 AS F32), \
             logic_notifications.f64_keep = message.f64, logic_notifications.bool_copy = \
             message.active, logic_notifications.occurred_text = message.occurred_at AS STRING, \
             logic_notifications.occurred_copy = (message.occurred_at AS STRING) AS DATETIME \
             UNSET logic_notifications.active, logic_notifications.u8, logic_notifications.i8, \
             logic_notifications.u16, logic_notifications.i16, logic_notifications.u32, \
             logic_notifications.i32, logic_notifications.u64, logic_notifications.i64, \
             logic_notifications.f32, logic_notifications.f64, logic_notifications.occurred_at, \
             logic_notifications.raw WHERE message.active AND message.occurred_at > \
             ('2026-04-07T00:00:00Z' AS DATETIME)",
        ),
        super::RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_parameterization: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
        },
    )
    .expect("filter-map must compile")
    .expect("program must exist");

    let record = RuntimeRecord::from_fields([
        (
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        ),
        ("active".to_string(), RuntimeValue::Bool(true)),
        ("u8".to_string(), RuntimeValue::U8(5)),
        ("i8".to_string(), RuntimeValue::I8(-7)),
        ("u16".to_string(), RuntimeValue::U16(9)),
        ("i16".to_string(), RuntimeValue::I16(12)),
        ("u32".to_string(), RuntimeValue::U32(42)),
        ("i32".to_string(), RuntimeValue::I32(-11)),
        ("u64".to_string(), RuntimeValue::U64(100)),
        ("i64".to_string(), RuntimeValue::I64(-64)),
        ("f32".to_string(), RuntimeValue::F32(OrderedFloat(2.5))),
        ("f64".to_string(), RuntimeValue::F64(OrderedFloat(7.25))),
        (
            "occurred_at".to_string(),
            RuntimeValue::Datetime(
                chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z")
                    .expect("valid timestamp"),
            ),
        ),
        (
            "raw".to_string(),
            RuntimeValue::String("ignored".to_string()),
        ),
    ]);

    let output = super::execute_filter_map_on_record(
        &program,
        record,
        None,
        None,
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect("filter-map must execute")
    .expect("record must not be filtered out");

    assert_eq!(
        output.value("tenant"),
        Some(&RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(output.value("u8_next"), Some(&RuntimeValue::U8(6)));
    assert_eq!(output.value("i8_abs"), Some(&RuntimeValue::I8(7)));
    assert_eq!(output.value("u16_keep"), Some(&RuntimeValue::U16(9)));
    assert_eq!(output.value("i16_prev"), Some(&RuntimeValue::I16(11)));
    assert_eq!(output.value("u32_same"), Some(&RuntimeValue::U32(42)));
    assert_eq!(output.value("i32_neg"), Some(&RuntimeValue::I32(11)));
    assert_eq!(output.value("u64_next"), Some(&RuntimeValue::U64(102)));
    assert_eq!(output.value("i64_keep"), Some(&RuntimeValue::I64(-64)));
    assert_eq!(
        output.value("f32_next"),
        Some(&RuntimeValue::F32(OrderedFloat(4.0)))
    );
    assert_eq!(
        output.value("f64_keep"),
        Some(&RuntimeValue::F64(OrderedFloat(7.25)))
    );
    assert_eq!(output.value("bool_copy"), Some(&RuntimeValue::Bool(true)));
    assert_eq!(
        output.value("occurred_text"),
        Some(&RuntimeValue::String(
            "2026-04-07T12:34:56+00:00".to_string()
        ))
    );
    assert_eq!(
        output.value("occurred_copy"),
        Some(&RuntimeValue::Datetime(
            chrono::DateTime::parse_from_rfc3339("2026-04-07T12:34:56Z").expect("valid timestamp"),
        ))
    );
}

#[tokio::test]
async fn reorderer_key_program_evaluates_direct_u32_field() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("sequence", ParseAsType::U32),
        ("payload", ParseAsType::String),
    ]);
    let program = super::compile_reorderer_program(
        &identifier("order_notifications"),
        &identifier("incoming_notifications"),
        "incoming_notifications.sequence",
        input_schema.arrow_schema(),
    )
    .expect("reorderer key program should compile");
    let records = vec![
        RuntimeRecord::from_fields([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("sequence".to_string(), RuntimeValue::U32(3)),
            (
                "payload".to_string(),
                RuntimeValue::String("third".to_string()),
            ),
        ]),
        RuntimeRecord::from_fields([
            (
                "tenant".to_string(),
                RuntimeValue::String("acme".to_string()),
            ),
            ("sequence".to_string(), RuntimeValue::U32(1)),
            (
                "payload".to_string(),
                RuntimeValue::String("first".to_string()),
            ),
        ]),
    ];
    let input = super::vm_typed_batch_from_runtime_records(&records, &program.program.input_schema)
        .expect("VM input batch should build");
    let output = super::execute_program_with_selection_in_context(
        &program.program,
        &input,
        &super::VmExecutionContext {
            now: Timestamp::from_unix_nanos(1),
        },
    )
    .await
    .expect("reorderer key program should execute");

    assert_eq!(program.key_count, 1);
    assert_eq!(
        program.key_column_offset,
        output.batch.columns().len().saturating_sub(1)
    );
    assert_eq!(
        super::reorder_key_part(output.batch.column(program.key_column_offset), 0),
        super::ReorderKeyPart::UInt64(3)
    );
    assert_eq!(
        super::reorder_key_part(output.batch.column(program.key_column_offset), 1),
        super::ReorderKeyPart::UInt64(1)
    );
}

#[tokio::test]
async fn large_vm_batches_preserve_results_through_public_vm_api() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("sequence", ParseAsType::U32),
        ("payload", ParseAsType::String),
    ]);
    let program = super::compile_reorderer_program(
        &identifier("order_notifications"),
        &identifier("incoming_notifications"),
        "incoming_notifications.sequence",
        input_schema.arrow_schema(),
    )
    .expect("reorderer key program should compile");
    let records = (0..=super::VM_SPAWN_BLOCKING_ROW_THRESHOLD)
        .map(|sequence| {
            RuntimeRecord::from_fields([
                (
                    "tenant".to_string(),
                    RuntimeValue::String("acme".to_string()),
                ),
                ("sequence".to_string(), RuntimeValue::U32(sequence as u32)),
                (
                    "payload".to_string(),
                    RuntimeValue::String(format!("payload-{sequence}")),
                ),
            ])
        })
        .collect::<Vec<_>>();
    let input = super::vm_typed_batch_from_runtime_records(&records, &program.program.input_schema)
        .expect("VM input batch should build");

    let output = super::execute_program_with_selection_in_context(
        &program.program,
        &input,
        &super::VmExecutionContext {
            now: Timestamp::from_unix_nanos(1),
        },
    )
    .await
    .expect("large VM batch should execute");

    assert_eq!(
        output.batch.row_count(),
        super::VM_SPAWN_BLOCKING_ROW_THRESHOLD + 1
    );
}

#[test]
fn processor_key_programs_reject_unqualified_fields() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("sequence", ParseAsType::U32),
    ]);
    let reorderer_error = super::compile_reorderer_program(
        &identifier("order_notifications"),
        &identifier("incoming_notifications"),
        "sequence",
        input_schema.arrow_schema(),
    )
    .expect_err("reorderer BY must require qualified field references");
    assert!(
        reorderer_error.contains("BY parse failed"),
        "unexpected reorderer error: {reorderer_error}"
    );

    let deduplicator_error = super::compile_deduplicator_key_program(
        &identifier("deduplicate_notifications"),
        &identifier("incoming_notifications"),
        "sequence",
        input_schema.arrow_schema(),
    )
    .expect_err("deduplicator DEDUPLICATE ON must require qualified field references");
    assert!(
        deduplicator_error.contains("DEDUPLICATE ON parse failed"),
        "unexpected deduplicator error: {deduplicator_error}"
    );
}

#[tokio::test]
async fn ingestor_filter_map_accepts_missing_optional_input_fields() {
    let input_schema = Arc::new(compile_schema(&CreateSchema {
        name: identifier("optional_logic"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("raw"),
                ty: ParseAsType::String,
                optional: true,
                sensitive: false,
            },
        ],
    }));
    let output_schema = Arc::new(compile_schema(&CreateSchema {
        name: identifier("optional_logic_output"),
        fields: vec![
            nervix_models::SchemaField {
                name: identifier("tenant"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            },
            nervix_models::SchemaField {
                name: identifier("normalized"),
                ty: ParseAsType::String,
                optional: true,
                sensitive: false,
            },
        ],
    }));
    let program = super::compile_ingestor_filter_map_program(
        &domain("default"),
        &identifier("logic_ingestor"),
        &identifier("logic_notifications"),
        &IngestSource::Endpoint {
            endpoint: identifier("logic_endpoint"),
            mode: nervix_models::EndpointIngestMode::NoAckSequential,
        },
        Some(
            "SET logic_notifications.normalized = lower(message.raw) UNSET logic_notifications.raw",
        ),
        super::RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: output_schema.arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_parameterization: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
        },
    )
    .expect("filter-map must compile")
    .expect("program must exist");

    let output = super::execute_filter_map_on_record(
        &program,
        RuntimeRecord::from_fields([(
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        )]),
        None,
        None,
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect("filter-map must execute")
    .expect("record must not be filtered out");

    assert_eq!(
        output.value("tenant"),
        Some(&RuntimeValue::String("acme".to_string()))
    );
    assert!(output.value("raw").is_none());
    assert!(output.value("normalized").is_none());
}

#[tokio::test]
async fn kafka_ingestor_filter_map_can_read_metadata_namespace() {
    let input_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("active", ParseAsType::Bool),
        ("amount", ParseAsType::I64),
        ("raw", ParseAsType::String),
    ]);
    let program = super::compile_ingestor_filter_map_program(
        &domain("default"),
        &identifier("logic_ingestor"),
        &identifier("logic_notifications"),
        &IngestSource::Kafka {
            client: identifier("logic_kafka"),
            topic: identifier("logic_notifications"),
            offset_mode: nervix_models::KafkaOffsetMode::Domain,
            instances: 1,
            mode: nervix_models::KafkaIngestMode::AckSequential {
                timeout: "5s".to_string(),
                retry_policy: nervix_models::RetryPolicy {
                    backoff: "100ms".to_string(),
                    max_backoff: "200ms".to_string(),
                },
            },
        },
        Some(
            "SET logic_notifications.topic = metadata.topic, logic_notifications.partition = \
             metadata.partition, logic_notifications.offset = metadata.offset UNSET \
             logic_notifications.active, logic_notifications.amount, logic_notifications.raw \
             WHERE metadata.offset >= 0",
        ),
        super::RuntimeVmSchemaPair {
            input: input_schema.arrow_schema(),
            input_sensitivity: super::VmSchemaSensitivity::default(),
            output: Arc::new(compile_schema(&CreateSchema {
                name: identifier("metadata_output"),
                fields: vec![
                    nervix_models::SchemaField {
                        name: identifier("tenant"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    },
                    nervix_models::SchemaField {
                        name: identifier("topic"),
                        ty: ParseAsType::String,
                        optional: true,
                        sensitive: false,
                    },
                    nervix_models::SchemaField {
                        name: identifier("partition"),
                        ty: ParseAsType::I32,
                        optional: true,
                        sensitive: false,
                    },
                    nervix_models::SchemaField {
                        name: identifier("offset"),
                        ty: ParseAsType::I64,
                        optional: true,
                        sensitive: false,
                    },
                ],
            }))
            .arrow_schema(),
            output_sensitivity: super::VmSchemaSensitivity::default(),
        },
        super::RuntimeVmCompileContext {
            available_materialized_streams: &HashMap::default(),
            available_lookups: &HashMap::default(),
            current_parameterization: &[],
            current_branch_schema: None,
            current_branch_sensitivity: None,
        },
    )
    .expect("filter-map must compile")
    .expect("program must exist");

    let record = RuntimeRecord::from_fields([
        (
            "tenant".to_string(),
            RuntimeValue::String("acme".to_string()),
        ),
        ("active".to_string(), RuntimeValue::Bool(true)),
        ("amount".to_string(), RuntimeValue::I64(7)),
        ("raw".to_string(), RuntimeValue::String("meta".to_string())),
    ]);
    let metadata = super::IngestFilterMapMetadata::kafka(
        "logic_notifications_t123".to_string(),
        2,
        42,
        None,
        Vec::new(),
    );

    let output = super::execute_filter_map_on_record(
        &program,
        record,
        None,
        Some(&metadata),
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect("filter-map must execute")
    .expect("record must not be filtered out");

    assert_eq!(
        output.value("tenant"),
        Some(&RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(
        output.value("topic"),
        Some(&RuntimeValue::String(
            "logic_notifications_t123".to_string()
        ))
    );
    assert_eq!(output.value("partition"), Some(&RuntimeValue::I32(2)));
    assert_eq!(output.value("offset"), Some(&RuntimeValue::I64(42)));
    assert!(output.value("active").is_none());
    assert!(output.value("amount").is_none());
    assert!(output.value("raw").is_none());
}

#[tokio::test]
async fn generator_set_program_can_project_from_materialized_relay_namespace() {
    let source_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("amount", ParseAsType::I64),
    ]);
    let output_schema = test_schema(&[
        ("tenant", ParseAsType::String),
        ("amount", ParseAsType::I64),
    ]);
    let generator = CreateGenerator {
        name: identifier("synth_notifications"),
        into_relay: identifier("generated_notifications"),
        parameterized_by: parameterized_by("tenant", "generated_notifications", &["tenant"]),
        each: "100ms".to_string(),
        flush_each: "100ms".to_string(),
        max_batch_size: Some("1MiB".to_string()),
        set: "SET generated_notifications.tenant = notifications.tenant, \
              generated_notifications.amount = notifications.amount + 1"
            .to_string(),
        message_error_policy: MessageErrorPolicy::Log,
    };

    let program = super::compile_generator_set_program(
        &domain("default"),
        &generator,
        output_schema.arrow_schema(),
        super::VmSchemaSensitivity::default(),
        &[(identifier("notifications"), source_schema.arrow_schema())],
    )
    .expect("generator set program must compile");

    let mut values = HashMap::default();
    values.insert(
        "notifications.tenant".to_string(),
        RuntimeValue::String("acme".to_string()),
    );
    values.insert("notifications.amount".to_string(), RuntimeValue::I64(7));
    let input = super::generator_context_batch(&program.input_schema, &values)
        .expect("generator input batch must build");

    let output = super::execute_generator_program_on_context(
        &program,
        &input,
        Timestamp::from_unix_nanos(1),
    )
    .await
    .expect("generator program must execute")
    .expect("generator program must emit one row");

    assert_eq!(
        output.value("tenant"),
        Some(&RuntimeValue::String("acme".to_string()))
    );
    assert_eq!(output.value("amount"), Some(&RuntimeValue::I64(8)));
}
