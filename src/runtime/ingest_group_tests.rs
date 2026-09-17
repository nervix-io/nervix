//! Ingest grouping and branch dispatch tests.
//!
//! Layer: test harness.
//!
//! - **Owns.** Focused tests for ingest grouping, typed structural failures, sidecar alignment,
//!   branch dispatch, and ownership-preserving error returns.
//! - **Depends on.** The runtime ingest grouping implementation and its test fixtures.
//! - **Must not know.** Production control-plane orchestration or external connector behavior.

use std::sync::Arc as StdArc;

use ahash::HashMap;
use arc_swap::ArcSwapOption;
use nervix_models::{
    AckMode, CodecJaqFormat, CodecJaqTransformations, CodecWireFormat, CreateCodec, CreateSchema,
    CreateWireSchema, ErrorPolicies, JsonType, ModelKind, ParseAsType, ResolvedCodecWireFormat,
    SchemaField, Timestamp, WireSchemaField,
};
use tokio::time::{Duration, timeout};
use triomphe::Arc;

use super::*;
use crate::{
    runtime::branch_runtime::BranchExecutionRuntime,
    runtime_ack::{AckOutcome, AckRootTracker, AckSet},
    runtime_schema::{
        RECORD_BUILDER_SETS_OPENED, RECORD_COLUMN_SETS_BUILT, RuntimeRecordBatch,
        RuntimeRecordMetadata, RuntimeValue, compile_codec, test_runtime_row,
    },
};

/// The one-field schema the ingest-group decode tests below decode into.
fn grouped_event_schema() -> Arc<CompiledSchema> {
    Arc::new(compile_schema(&CreateSchema {
        name: named("grouped_event"),
        fields: vec![SchemaField {
            name: named("user_id"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive: false,
        }],
    }))
}

/// A JSON codec over the grouped event schema, for the ingest-group decode tests below.
fn grouped_event_codec() -> Arc<CompiledCodec> {
    let schema = grouped_event_schema();
    compile_codec(
        &CreateCodec {
            name: named("grouped_event_codec"),
            wire_format: CodecWireFormat::Json {
                wire_schema: named("grouped_event_wire"),
            },
            schema: named("grouped_event"),
            encoding_rules: Vec::new(),
        },
        schema,
        ResolvedCodecWireFormat::Json(&CreateWireSchema {
            name: named("grouped_event_wire"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: named("user_id"),
                ty: JsonType::Integer,
                optional: false,
            }],
        }),
    )
    .expect("the grouped event codec should compile")
}

/// A JSON codec over the grouped event schema whose ON INGESTION program is `program`.
fn unfolding_event_codec(program: &str) -> Arc<CompiledCodec> {
    let transformations = CodecJaqTransformations {
        on_ingestion: Some(program.to_string()),
        on_emitting: None,
    };
    compile_codec(
        &CreateCodec {
            name: named("unfolding_event_codec"),
            wire_format: CodecWireFormat::JaqNative {
                format: CodecJaqFormat::Json,
                transformations: transformations.clone(),
            },
            schema: named("grouped_event"),
            encoding_rules: Vec::new(),
        },
        grouped_event_schema(),
        ResolvedCodecWireFormat::JaqNative {
            format: CodecJaqFormat::Json,
            transformations: &transformations,
        },
    )
    .expect("the unfolding event codec should compile")
}

fn grouped_event_ingestor_metrics() -> MessageMetricsHandle {
    let ingestor: IngestorName = named("grouped_event_source");
    RuntimeMetrics::default().resolve_global_node_message_metrics(
        &domain("default"),
        ModelKind::Ingestor,
        &ModelName::from(&ingestor),
        None,
        "received",
    )
}

/// Accepts the oldest payloads `collector` has decoded, one for each ACK set, as an ingestor
/// does after a successful decode.
fn accept_decoded_payloads(
    collector: &mut IngestRouteCollector,
    acks: Vec<AckSet>,
) -> Result<(), String> {
    let headers = NoIngestHeaders;
    let metadata = (0..acks.len())
        .map(|_| IngestMetadataRow::Headers { headers: &headers })
        .collect::<Vec<_>>();
    collector
        .collect(IngestGroupContribution {
            domain: &domain("default"),
            ingestor: &named("grouped_event_source"),
            timestamp_source: None,
            output_routes: &RelayProcessorOutputsNode { routes: Vec::new() },
            filter_where: None,
            metadata: &metadata,
            acks,
            ingested_at: Timestamp::from_unix_nanos(1),
        })
        .map_err(|error| error.to_string())
}

fn two_ingest_rows() -> IngestGroupRows {
    let first = test_runtime_row([("value".to_string(), RuntimeValue::I64(1))]);
    let second = test_runtime_row([("value".to_string(), RuntimeValue::I64(2))]);
    let batch =
        RuntimeRecordBatch::from_rows(first.batch().schema(), [&first, &second].into_iter())
            .expect("test rows should form one ingest group");
    IngestGroupRows {
        batch: Arc::new(batch),
        record_metadata: vec![RuntimeRecordMetadata::test(); 2],
        ingest_metadata: ingest_metadata_for_test(
            IngestMetadataKind::Headers,
            &[
                IngestMetadataRow::Headers {
                    headers: &NoIngestHeaders,
                },
                IngestMetadataRow::Headers {
                    headers: &NoIngestHeaders,
                },
            ],
        ),
        acks: vec![AckSet::empty(), AckSet::empty()],
    }
}

fn branched_input(tenant: &str, value: i64) -> RelayRecordBatch {
    let schema = test_schema(&[("tenant", ParseAsType::String), ("value", ParseAsType::I64)]);
    RelayRecordBatch::single(
        schema,
        string_branch_key("tenant", tenant),
        test_runtime_row([
            (
                "tenant".to_string(),
                RuntimeValue::String(tenant.to_string()),
            ),
            ("value".to_string(), RuntimeValue::I64(value)),
        ]),
        AckSet::empty(),
    )
    .expect("branched input fixture must match its compiled schema")
}

fn expect_failure<T, E>(result: Result<T, E>, reason: &str) -> E {
    match result {
        Ok(_) => panic!("{reason}"),
        Err(error) => error,
    }
}

/// A group of `n` messages must cost one set of Arrow columns, not `n` single-row batches and
/// a concatenation.
#[tokio::test]
async fn ingest_group_builds_one_record_column_set_for_all_of_its_messages() {
    let codec = grouped_event_codec();
    let mut collector = IngestRouteCollector::new(
        IngestMetadataKind::Headers,
        8,
        grouped_event_ingestor_metrics(),
    );

    RECORD_BUILDER_SETS_OPENED.with(|count| count.set(0));
    RECORD_COLUMN_SETS_BUILT.with(|count| count.set(0));

    for user_id in 0..3i64 {
        collector
            .decode_payload(
                &codec,
                Cow::Owned(format!(r#"{{"user_id":{user_id}}}"#).into_bytes()),
            )
            .await
            .expect("each payload should decode into the open group");
        accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
            .expect("each decoded payload should be accepted");
    }

    assert_eq!(
        RECORD_BUILDER_SETS_OPENED.with(std::cell::Cell::get),
        1,
        "a group must open exactly one record builder, not one per message"
    );
    assert_eq!(
        RECORD_COLUMN_SETS_BUILT.with(std::cell::Cell::get),
        0,
        "an open group must not build record columns before it closes"
    );

    let (_, rows) = collector
        .take_pending()
        .expect("the group must close")
        .expect("the group holds rows");

    assert_eq!(
        RECORD_COLUMN_SETS_BUILT.with(std::cell::Cell::get),
        1,
        "closing a group must build exactly one record column set"
    );
    assert_eq!(rows.len(), 3);
    for user_id in 0..3usize {
        assert_eq!(
            rows.batch
                .value(user_id, "user_id")
                .expect("the decoded column must be readable"),
            Some(RuntimeValue::I64(
                user_id.try_into().expect("a small test index fits i64")
            ))
        );
    }
}

/// An acknowledged poll group decodes its whole batch up front and then accepts the payloads
/// one at a time, so a contribution covers a prefix of what the group has decoded.
#[tokio::test]
async fn ingest_group_accepts_its_decoded_payloads_one_at_a_time() {
    let codec = grouped_event_codec();
    let mut collector = IngestRouteCollector::new(
        IngestMetadataKind::Headers,
        3,
        grouped_event_ingestor_metrics(),
    );

    for user_id in 0..3i64 {
        collector
            .decode_payload(
                &codec,
                Cow::Owned(format!(r#"{{"user_id":{user_id}}}"#).into_bytes()),
            )
            .await
            .expect("each payload should decode into the open group");
    }
    for _ in 0..3 {
        accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
            .expect("each decoded payload should be accepted on its own");
    }

    let (_, rows) = collector
        .take_pending()
        .expect("the group must close")
        .expect("the group holds rows");

    assert_eq!(rows.len(), 3);
    for user_id in 0..3usize {
        assert_eq!(
            rows.batch
                .value(user_id, "user_id")
                .expect("the decoded column must be readable"),
            Some(RuntimeValue::I64(
                user_id.try_into().expect("a small test index fits i64")
            ))
        );
    }
}

/// A payload the codec rejects stays attributable to its own message: the group keeps the rows
/// around it, and its records, metadata and ACKs stay row-aligned.
#[tokio::test]
async fn ingest_group_keeps_its_other_messages_when_one_payload_fails_to_decode() {
    let codec = grouped_event_codec();
    let mut collector = IngestRouteCollector::new(
        IngestMetadataKind::Headers,
        8,
        grouped_event_ingestor_metrics(),
    );

    collector
        .decode_payload(&codec, Cow::Borrowed(br#"{"user_id":1}"#))
        .await
        .expect("the first payload should decode");
    accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
        .expect("the first payload should be accepted");

    collector
        .decode_payload(&codec, Cow::Borrowed(br#"{"user_id":"two"}"#))
        .await
        .expect_err("a user id of the wrong type should be rejected");

    collector
        .decode_payload(&codec, Cow::Borrowed(br#"{"user_id":3}"#))
        .await
        .expect("the payload after the rejected one should decode");
    accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
        .expect("the third payload should be accepted");

    let (_, rows) = collector
        .take_pending()
        .expect("the group must close")
        .expect("the group holds rows");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows.acks.len(), 2);
    assert_eq!(rows.record_metadata.len(), 2);
    assert_eq!(
        rows.batch.value(0, "user_id").expect("readable"),
        Some(RuntimeValue::I64(1))
    );
    assert_eq!(
        rows.batch.value(1, "user_id").expect("readable"),
        Some(RuntimeValue::I64(3))
    );
}

/// Every message a payload unfolds into takes the payload's metadata and a share of its ACK
/// set, so the source's acknowledgement waits for all of them.
#[tokio::test]
async fn ingest_group_gives_every_unfolded_message_its_payload_metadata_and_an_ack_share() {
    let codec = unfolding_event_codec(".[]");
    let mut collector = IngestRouteCollector::new(
        IngestMetadataKind::Headers,
        1,
        grouped_event_ingestor_metrics(),
    );
    let tracker = Arc::new(AckRootTracker::default());
    let (payload_acks, completion) = AckSet::tracked_root(tracker.clone());

    collector
        .decode_payload(
            &codec,
            Cow::Borrowed(br#"[{"user_id":1},{"user_id":2},{"user_id":3}]"#),
        )
        .await
        .expect("the payload should unfold into the open group");
    accept_decoded_payloads(&mut collector, vec![payload_acks])
        .expect("the unfolded payload should be accepted");

    assert_eq!(
        collector.len(),
        3,
        "the group counts messages, not payloads"
    );
    let (_, rows) = collector
        .take_pending()
        .expect("the group must close")
        .expect("the group holds rows");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows.record_metadata.len(), 3);
    assert_eq!(rows.acks.len(), 3);
    assert!(
        rows.metadata_row(2).is_some(),
        "every unfolded message must carry the payload's ingest metadata"
    );
    for (row, user_id) in (1..=3i64).enumerate() {
        assert_eq!(
            rows.batch.value(row, "user_id").expect("readable"),
            Some(RuntimeValue::I64(user_id))
        );
    }

    rows.acks[0].ack_success();
    rows.acks[1].ack_success();
    assert_eq!(
        tracker.outstanding(),
        1,
        "the payload must stay outstanding until its last message is acknowledged"
    );
    rows.acks[2].ack_success();
    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("the payload acknowledgement should resolve"),
        AckOutcome::Ack
    );
}

/// A payload that unfolds into no message is acknowledged when it is accepted, and it neither
/// opens the group nor schedules its idle close.
#[tokio::test]
async fn ingest_group_acknowledges_a_payload_that_unfolds_into_no_messages() {
    let codec = unfolding_event_codec(".[] | select(.keep)");
    let mut collector = IngestRouteCollector::new(
        IngestMetadataKind::Headers,
        1,
        grouped_event_ingestor_metrics(),
    );
    let tracker = Arc::new(AckRootTracker::default());
    let (payload_acks, completion) = AckSet::tracked_root(tracker.clone());

    collector
        .decode_payload(&codec, Cow::Borrowed(br#"[{"user_id":1,"keep":false}]"#))
        .await
        .expect("a payload the program selects nothing from should decode");
    accept_decoded_payloads(&mut collector, vec![payload_acks])
        .expect("the payload without messages should be accepted");

    assert_eq!(tracker.outstanding(), 0);
    assert_eq!(
        timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("the payload acknowledgement should resolve"),
        AckOutcome::Ack
    );
    assert!(collector.is_empty());
    assert!(collector.next_flush().is_none());
    assert!(
        collector
            .take_pending()
            .expect("the empty group must close")
            .is_none()
    );
}

/// A payload that fails part-way through unfolding contributes no message, and the group keeps
/// the messages of the payloads around it.
#[tokio::test]
async fn ingest_group_keeps_no_message_of_a_payload_that_fails_part_way_through_unfolding() {
    let codec = unfolding_event_codec(".[]");
    let mut collector = IngestRouteCollector::new(
        IngestMetadataKind::Headers,
        8,
        grouped_event_ingestor_metrics(),
    );

    collector
        .decode_payload(&codec, Cow::Borrowed(br#"[{"user_id":1}]"#))
        .await
        .expect("the first payload should unfold");
    accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
        .expect("the first payload should be accepted");

    let error = collector
        .decode_payload(
            &codec,
            Cow::Borrowed(br#"[{"user_id":2},{"user_id":"confidential"}]"#),
        )
        .await
        .expect_err("an element of the wrong type must reject its whole payload");
    let message = error.to_string();
    assert!(message.contains("(input value 0, output 1)"), "{message}");
    assert!(!message.contains("confidential"), "{message}");

    collector
        .decode_payload(&codec, Cow::Borrowed(br#"[{"user_id":3},{"user_id":4}]"#))
        .await
        .expect("the payload after the rejected one should unfold");
    accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
        .expect("the last payload should be accepted");

    let (_, rows) = collector
        .take_pending()
        .expect("the group must close")
        .expect("the group holds rows");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows.acks.len(), 3);
    for (row, user_id) in [1i64, 3, 4].into_iter().enumerate() {
        assert_eq!(
            rows.batch.value(row, "user_id").expect("readable"),
            Some(RuntimeValue::I64(user_id))
        );
    }
}

/// A rejected payload in an otherwise empty group releases the rows it abandoned instead of
/// keeping them allocated until a message arrives.
#[tokio::test]
async fn ingest_group_releases_the_rows_a_rejected_payload_abandoned_in_an_empty_group() {
    let codec = unfolding_event_codec(".[]");
    let mut collector = IngestRouteCollector::new(
        IngestMetadataKind::Headers,
        8,
        grouped_event_ingestor_metrics(),
    );

    collector
        .decode_payload(
            &codec,
            Cow::Borrowed(br#"[{"user_id":1},{"user_id":"two"}]"#),
        )
        .await
        .expect_err("an element of the wrong type must reject its whole payload");

    assert!(
        collector.pending.records.is_none(),
        "an empty group must drop the builder a rejected payload abandoned rows in"
    );
}

#[test]
fn ingest_group_rows_share_the_group_batch_allocation() {
    let rows = two_ingest_rows();

    let first = rows.row(0).expect("first row should exist");
    let second = rows.row(1).expect("second row should exist");

    assert!(
        Arc::ptr_eq(first.batch(), second.batch()),
        "row views from one ingest group must retain the same batch allocation"
    );
}

#[test]
fn pending_ingest_group_rejects_misaligned_payload_sidecars() {
    let headers = NoIngestHeaders;
    let metadata = [IngestMetadataRow::Headers { headers: &headers }];
    let ingested_at = Timestamp::from_unix_nanos(1);
    let mut pending = PendingIngestGroup::new(IngestMetadataKind::Headers, 1);

    let missing_ack = expect_failure(
        pending.append(&metadata, Vec::new(), ingested_at),
        "every payload metadata row requires an ACK set",
    );
    assert!(matches!(
        missing_ack.current_context(),
        IngestGroupError::PayloadSidecarCount {
            metadata_rows: 1,
            ack_sets: 0,
        }
    ));

    let missing_payload = expect_failure(
        pending.append(&metadata, vec![AckSet::empty()], ingested_at),
        "metadata cannot accept a payload that was not decoded",
    );
    assert!(matches!(
        missing_payload.current_context(),
        IngestGroupError::DecodedPayloadCount {
            metadata_rows: 1,
            decoded_payloads: 0,
        }
    ));
}

#[test]
fn pending_ingest_group_reports_structural_close_errors() {
    let mut timestamp_mismatch = PendingIngestGroup::new(IngestMetadataKind::Headers, 1);
    timestamp_mismatch.acks.push(AckSet::empty());
    let timestamp_error = expect_failure(
        timestamp_mismatch.into_rows(),
        "ACKs and ingestion timestamps must remain aligned",
    );
    assert!(matches!(
        timestamp_error.current_context(),
        IngestGroupError::TimestampCount {
            ack_sets: 1,
            ingest_timestamps: 0,
        }
    ));

    let missing_metadata = expect_failure(
        PendingIngestGroup::new(IngestMetadataKind::Headers, 1).into_rows(),
        "a closed group must have opened metadata builders",
    );
    assert!(matches!(
        missing_metadata.current_context(),
        IngestGroupError::MissingMetadataBuilders
    ));

    let mut missing_records = PendingIngestGroup::new(IngestMetadataKind::Headers, 1);
    missing_records.metadata = Some(IngestMetadataBuilders::new(IngestMetadataKind::Headers, 1));
    let missing_record_error = expect_failure(
        missing_records.into_rows(),
        "a closed group must have opened a record builder",
    );
    assert!(matches!(
        missing_record_error.current_context(),
        IngestGroupError::MissingRecordBuilder
    ));

    let mut metadata_mismatch = PendingIngestGroup::new(IngestMetadataKind::Headers, 1);
    let mut metadata = IngestMetadataBuilders::new(IngestMetadataKind::Headers, 1);
    metadata
        .append(&IngestMetadataRow::Headers {
            headers: &NoIngestHeaders,
        })
        .expect("the headers fixture must append");
    metadata_mismatch.metadata = Some(metadata);
    let metadata_error = expect_failure(
        metadata_mismatch.into_rows(),
        "metadata rows must match accepted record count",
    );
    assert!(matches!(
        metadata_error.current_context(),
        IngestGroupError::MetadataRowCount {
            records: 0,
            metadata_rows: 1,
        }
    ));

    let mut decoded_mismatch = PendingIngestGroup::new(IngestMetadataKind::Headers, 1);
    let mut metadata = IngestMetadataBuilders::new(IngestMetadataKind::Headers, 1);
    metadata
        .append(&IngestMetadataRow::Headers {
            headers: &NoIngestHeaders,
        })
        .expect("the headers fixture must append");
    decoded_mismatch.metadata = Some(metadata);
    decoded_mismatch.records = Some(grouped_event_schema().batch_builder(1));
    decoded_mismatch.acks.push(AckSet::empty());
    decoded_mismatch
        .ingested_at
        .push(Timestamp::from_unix_nanos(1));
    let decoded_error = expect_failure(
        decoded_mismatch.into_rows(),
        "decoded Arrow rows must match accepted record count",
    );
    assert!(matches!(
        decoded_error.current_context(),
        IngestGroupError::DecodedRowCount {
            records: 1,
            decoded_rows: 0,
        }
    ));
}

#[test]
fn ingest_group_rows_validate_views_and_selection_alignment() {
    let out_of_bounds = expect_failure(
        two_ingest_rows().row(2),
        "a row view must address existing metadata",
    );
    assert!(matches!(
        out_of_bounds.current_context(),
        IngestGroupError::RecordMetadataRowOutOfBounds {
            row: 2,
            record_metadata_rows: 2,
        }
    ));

    let mut misaligned = two_ingest_rows();
    misaligned.record_metadata.pop();
    let alignment_error = expect_failure(
        misaligned.select(&[true, false]),
        "selection must reject misaligned sidecars",
    );
    assert!(matches!(
        alignment_error.current_context(),
        IngestGroupError::RowCount {
            arrow_rows: 2,
            record_metadata_rows: 1,
            ingest_metadata_rows: 2,
            ack_sets: 2,
        }
    ));

    let selection_error = expect_failure(
        two_ingest_rows().select(&[true]),
        "selection length must match the ingest group",
    );
    assert!(matches!(
        selection_error.current_context(),
        IngestGroupError::SelectionLengthMismatch {
            expected: 2,
            found: 1,
        }
    ));

    let selected = two_ingest_rows()
        .select(&[false, true])
        .expect("a row-aligned selection must succeed");
    assert_eq!(selected.len(), 1);
    assert_eq!(selected.record_metadata.len(), 1);
    assert_eq!(selected.ingest_metadata.len(), 1);
    assert_eq!(selected.acks.len(), 1);
    assert_eq!(
        selected.batch.value(0, "value").expect("readable value"),
        Some(RuntimeValue::I64(2))
    );
}

#[tokio::test]
async fn ingest_route_collector_reports_identity_and_unaccepted_payloads() {
    let codec = grouped_event_codec();
    let mut undispatched = IngestRouteCollector::new(
        IngestMetadataKind::Headers,
        1,
        grouped_event_ingestor_metrics(),
    );
    undispatched
        .decode_payload(&codec, Cow::Borrowed(br#"{"user_id":1}"#))
        .await
        .expect("the fixture payload must decode");
    let undispatched_error = expect_failure(
        undispatched.take_pending(),
        "decoded payloads must be accepted before the group closes",
    );
    assert!(matches!(
        undispatched_error.current_context(),
        IngestGroupError::UndispatchedPayloads { payloads: 1 }
    ));
    assert_eq!(undispatched.pending.undispatched_payloads(), 0);

    let mut collector = IngestRouteCollector::new(
        IngestMetadataKind::Headers,
        2,
        grouped_event_ingestor_metrics(),
    );
    collector
        .decode_payload(&codec, Cow::Borrowed(br#"{"user_id":1}"#))
        .await
        .expect("the first fixture payload must decode");
    accept_decoded_payloads(&mut collector, vec![AckSet::empty()])
        .expect("the first fixture payload must be accepted");
    collector
        .decode_payload(&codec, Cow::Borrowed(br#"{"user_id":2}"#))
        .await
        .expect("the second fixture payload must decode");

    let other_domain = domain("other");
    let ingestor: IngestorName = named("grouped_event_source");
    let routes = RelayProcessorOutputsNode { routes: Vec::new() };
    let metadata = [IngestMetadataRow::Headers {
        headers: &NoIngestHeaders,
    }];
    let identity_error = expect_failure(
        collector.collect(IngestGroupContribution {
            domain: &other_domain,
            ingestor: &ingestor,
            timestamp_source: None,
            output_routes: &routes,
            filter_where: None,
            metadata: &metadata,
            acks: vec![AckSet::empty()],
            ingested_at: Timestamp::from_unix_nanos(2),
        }),
        "one collector cannot mix source identities",
    );
    match identity_error.current_context() {
        IngestGroupError::CollectorIdentity {
            existing_domain,
            existing_ingestor,
            received_domain,
            received_ingestor,
        } => {
            assert_eq!(existing_domain.as_str(), "default");
            assert_eq!(existing_ingestor.as_str(), "grouped_event_source");
            assert_eq!(received_domain.as_str(), "other");
            assert_eq!(received_ingestor.as_str(), "grouped_event_source");
        }
        other => panic!("unexpected collector error: {other}"),
    }
    assert_eq!(collector.pending.undispatched_payloads(), 0);
}

#[tokio::test]
async fn ingest_collector_flush_reports_missing_route_dependencies() {
    let runtime = Runtime::default();
    let test_domain = domain("default");
    install_unpaced_test_domain(&runtime, &test_domain);
    let ingestor: IngestorName = named("grouped_event_source");
    let relay: RelayName = named("grouped_events");
    let senders = HashMap::default();
    let message = || RelayMessage {
        key: None,
        record: test_runtime_row([("user_id".to_string(), RuntimeValue::I64(1))]),
        acks: AckSet::empty(),
    };

    let mut missing_schema_collector = IngestRouteCollector::new(
        IngestMetadataKind::Headers,
        1,
        grouped_event_ingestor_metrics(),
    );
    missing_schema_collector.push(&relay, message());
    let missing_schema = expect_failure(
        runtime
            .flush_ingest_collector(
                &test_domain,
                &ingestor,
                &senders,
                &mut missing_schema_collector,
            )
            .await,
        "a routed ingest batch requires its relay schema",
    );
    assert!(matches!(
        missing_schema.current_context(),
        IngestGroupError::RelaySchemaMissing { domain, relay: missing }
            if domain == &test_domain && missing == &relay
    ));

    runtime
        .inner
        .domain_routings
        .get(&test_domain)
        .expect("the test domain routing must remain installed")
        .store(StdArc::new(DomainRoutingSnapshot {
            relay_schemas: [(relay.clone(), grouped_event_schema())]
                .into_iter()
                .collect(),
            ..DomainRoutingSnapshot::default()
        }));
    let mut missing_entrypoint_collector = IngestRouteCollector::new(
        IngestMetadataKind::Headers,
        1,
        grouped_event_ingestor_metrics(),
    );
    missing_entrypoint_collector.push(&relay, message());
    let missing_entrypoint = expect_failure(
        runtime
            .flush_ingest_collector(
                &test_domain,
                &ingestor,
                &senders,
                &mut missing_entrypoint_collector,
            )
            .await,
        "a routed ingest batch requires its branch entrypoint",
    );
    assert!(matches!(
        missing_entrypoint.current_context(),
        IngestGroupError::BranchEntrypointMissing {
            ingestor: missing_ingestor,
            relay: missing_relay,
        } if missing_ingestor == &ingestor && missing_relay == &relay
    ));
}

#[test]
fn branched_entrypoint_batch_reports_structural_input_errors() {
    let empty = expect_failure(
        BranchedEntrypointBatch::from_inputs(Vec::new()),
        "a branch input batch needs at least one input",
    );
    assert!(empty.preserved.is_empty());
    assert!(matches!(
        empty.error.current_context(),
        IngestGroupError::EmptyBranchInputs
    ));

    let mut malformed = branched_input("acme", 1);
    malformed.keys.clear();
    let misaligned = expect_failure(
        BranchedEntrypointBatch::from_inputs(vec![malformed]),
        "branch input sidecars must stay aligned",
    );
    assert_eq!(misaligned.preserved.len(), 1);
    assert!(matches!(
        misaligned.error.current_context(),
        IngestGroupError::BranchInputRowCount {
            arrow_rows: 1,
            metadata_rows: 1,
            branch_keys: 0,
            ack_sets: 1,
        }
    ));

    let other_schema = test_schema(&[("other_value", ParseAsType::U64)]);
    let other = RelayRecordBatch::single(
        other_schema,
        None,
        test_runtime_row([("other_value".to_string(), RuntimeValue::U64(2))]),
        AckSet::empty(),
    )
    .expect("the second fixture must match its own schema");
    let schema_mismatch = expect_failure(
        BranchedEntrypointBatch::from_inputs(vec![branched_input("acme", 1), other]),
        "branch inputs with different schemas cannot concatenate",
    );
    assert_eq!(schema_mismatch.preserved.len(), 2);
    assert!(matches!(
        schema_mismatch.error.current_context(),
        IngestGroupError::RuntimeSchema {
            operation: IngestGroupRecordOperation::ConcatenateBranchInputs,
        }
    ));
    assert!(
        schema_mismatch
            .error
            .contains::<crate::runtime_schema::RuntimeSchemaError>()
    );
}

#[tokio::test]
async fn branched_entrypoint_batch_groups_and_filters_each_branch() {
    let batch = BranchedEntrypointBatch::from_inputs(vec![
        branched_input("acme", 1),
        branched_input("beta", 2),
        branched_input("acme", 3),
    ])
    .expect("compatible branch inputs must concatenate");
    let selections = batch
        .branch_selections()
        .expect("aligned branch inputs must produce a plan");
    assert_eq!(selections.len(), 2);
    assert_eq!(selections[0].rows, [0, 2]);
    assert_eq!(selections[1].rows, [1]);

    let acme = batch
        .filter_branch(selections[0].clone(), BranchInstanceAckBoundary::Preserve)
        .expect("a valid branch selection must filter the batch");
    assert_eq!(acme.message_count(), 2);
    assert_eq!(
        acme.batch.value(0, "value").expect("readable value"),
        Some(RuntimeValue::I64(1))
    );
    assert_eq!(
        acme.batch.value(1, "value").expect("readable value"),
        Some(RuntimeValue::I64(3))
    );

    let invalid_selection = BranchedBranchSelection {
        key: string_branch_key("tenant", "missing"),
        rows: vec![3],
    };
    let invalid = expect_failure(
        batch.filter_branch(invalid_selection, BranchInstanceAckBoundary::Preserve),
        "a branch selection cannot address a missing row",
    );
    assert_eq!(invalid.preserved.len(), 3);
    assert!(matches!(
        invalid.error.current_context(),
        IngestGroupError::BranchSelectionRowOutOfBounds {
            row: 3,
            batch_rows: 3,
        }
    ));

    let mut malformed = BranchedEntrypointBatch::from_inputs(vec![branched_input("acme", 1)])
        .expect("the fixture input must build");
    malformed.acks.clear();
    let plan_error = expect_failure(
        malformed.branch_selections(),
        "a branch plan requires aligned sidecars",
    );
    assert!(matches!(
        plan_error.current_context(),
        IngestGroupError::BranchInputRowCount {
            arrow_rows: 1,
            metadata_rows: 1,
            branch_keys: 1,
            ack_sets: 0,
        }
    ));

    let blocking = branched_entrypoint_batch_from_inputs_blocking(vec![
        branched_input("acme", 10),
        branched_input("beta", 20),
    ])
    .await
    .expect("blocking input construction must retain a valid batch");
    let plan = branched_branch_plan_blocking(blocking.clone())
        .await
        .expect("blocking branch planning must preserve the grouped keys");
    let (key, filtered) = branched_branch_filter_blocking(
        blocking,
        plan[1].clone(),
        BranchInstanceAckBoundary::Preserve,
    )
    .await
    .expect("blocking branch filtering must return its selected batch");
    assert_eq!(key, string_branch_key("tenant", "beta"));
    assert_eq!(filtered.message_count(), 1);

    let empty_blocking = expect_failure(
        branched_entrypoint_batch_from_inputs_blocking(Vec::new()).await,
        "blocking construction must retain the empty-input error",
    );
    assert!(empty_blocking.preserved.is_empty());
    assert!(matches!(
        empty_blocking.error.current_context(),
        IngestGroupError::EmptyBranchInputs
    ));
}

#[tokio::test]
async fn branched_root_without_children_acks_success() {
    let runtime = Runtime::default();
    let root_domain = domain("default");
    install_unpaced_test_domain(&runtime, &root_domain);
    let root_relay = named("tenant_orders");
    let root_registry = RelayRegistry::new();
    let root_services = test_relay_boundary_services();
    let owner_task = runtime.spawn_relay_owner_task(
        &root_domain,
        &root_relay,
        root_registry.clone(),
        root_services.clone(),
        RelayRetention::default(),
    );
    let root_key = Some(concrete_branch_key([(
        named("tenant"),
        RuntimeValue::String("acme".to_string()),
    )]));
    let root_source = named("metric_ingestor");
    let root_metrics = runtime
        .inner
        .metrics
        .resolve_node_batch_metrics(NodeBatchMetricsSpec {
            domain: &root_domain,
            kind: ModelKind::Ingestor,
            node: &ModelName::from(&root_source),
            relay: &root_relay,
            physical_node_id: None,
            direction: "sent",
            branch_key: Some(branch_key_display(&root_key)),
        });
    let mut root = BranchRuntime {
        key: root_key.clone(),
        runtime: runtime.clone(),
        domain: root_domain.clone(),
        routing: None,
        routing_snapshot: None,
        domain_clock: test_domain_clock(&root_domain),
        source_kind: ModelKind::Ingestor,
        source: root_source,
        root_relay: root_relay.clone(),
        error_policies: ErrorPolicies::handled_by_log(),
        relays: [(
            root_relay.clone(),
            ConcreteRelayRuntime::new(ConcreteRelayRuntimeBuild {
                runtime,
                domain: root_domain,
                relay: root_relay,
                registry: root_registry,
                services: root_services,
                key: root_key,
            }),
        )]
        .into_iter()
        .collect(),
        materialized_states: HashMap::default(),
        relay_state_epoch: None,
        processors: HashMap::default(),
        metrics: BranchRuntimeMetrics {
            source: root_metrics,
            source_input: None,
            processor_inputs: HashMap::default(),
            processor_outputs: HashMap::default(),
        },
    };
    let graph = StdArc::new(ArcSwapOption::from(None));
    let (acks, completion) = AckSet::root();
    let schema = test_schema(&[("tenant", ParseAsType::String)]);

    root.dispatch(
        &graph,
        RelayRecordBatch::single(
            schema,
            string_branch_key("tenant", "acme"),
            test_runtime_row([(
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
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn branch_entrypoint_dispatches_an_ingestor_prepared_batch_immediately() {
    let runtime = Runtime::default();
    let domain = domain("default");
    install_unpaced_test_domain(&runtime, &domain);
    let root_relay = named("notifications");
    let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(1));
    let mut fan_in =
        RelayRuntimeFanIn::new(fanout.runtime_consumer_receiver_for_mode(AckMode::Attached));
    let services = Arc::new(RelayBoundaryServices::new(fanout, 1, 0, Vec::new(), None));
    let registry = RelayRegistry::new();
    let owner_task = runtime.spawn_relay_owner_task(
        &domain,
        &root_relay,
        registry.clone(),
        services.clone(),
        RelayRetention::default(),
    );
    let schema = test_schema(&[("user_id", ParseAsType::U32)]);
    let branched_runtime = BranchExecutionRuntime::new(
        runtime,
        domain,
        named("notifications_ingestor"),
        StdArc::new(ArcSwapOption::from(None)),
        BranchInstanceTemplate {
            source_kind: ModelKind::Ingestor,
            source: named("notifications_ingestor"),
            root_relay: root_relay.clone(),
            branch: None,
            branch_ttl: None,
            branch_max_instances: None,
            error_policies: ErrorPolicies::handled_by_log(),
            relays: [(
                root_relay,
                RelayProcessorRelayTemplate {
                    registry,
                    services: services.clone(),
                },
            )]
            .into_iter()
            .collect(),
            processors: HashMap::default(),
        },
        Duration::from_secs(30),
    );

    branched_runtime
        .sender()
        .send(
            RelayRecordBatch::single(
                schema,
                None,
                test_runtime_row([("user_id".to_string(), RuntimeValue::U32(42))]),
                AckSet::empty(),
            )
            .expect("ingestor output batch should build"),
        )
        .await
        .expect("branch entrypoint should accept an ingestor-prepared batch");

    let batch = timeout(Duration::from_millis(100), fan_in.recv())
        .await
        .expect("branch entrypoint must not apply a second flush delay")
        .expect("runtime consumer should remain open");
    assert_eq!(batch.message_count(), 1);

    branched_runtime.shutdown().await;
    owner_task
        .stop(Duration::from_secs(1))
        .await
        .expect("relay owner should stop");
}

#[tokio::test]
async fn ingestor_and_reingestor_routes_apply_size_boundaries_independently_per_branch() {
    let cases = [
        (
            ModelKind::Ingestor,
            BranchInstanceAckBoundary::Preserve,
            "notifications_ingestor",
        ),
        (
            ModelKind::Reingestor,
            BranchInstanceAckBoundary::Reingestor(AckMode::Attached),
            "notifications_reingestor",
        ),
    ];
    for (source_kind, ack_boundary, source) in cases {
        tokio::task::consume_budget().await;
        let runtime = Runtime::default();
        let domain = domain("default");
        install_unpaced_test_domain(&runtime, &domain);
        let root_relay = named("notifications");
        let fanout = RelayBoundaryFanout::direct_with_capacity(nonzero_capacity(4));
        let mut fan_in =
            RelayRuntimeFanIn::new(fanout.runtime_consumer_receiver_for_mode(AckMode::Attached));
        let services = Arc::new(RelayBoundaryServices::new(fanout, 1, 0, Vec::new(), None));
        let registry = RelayRegistry::new();
        let owner_task = runtime.spawn_relay_owner_task(
            &domain,
            &root_relay,
            registry.clone(),
            services.clone(),
            RelayRetention::default(),
        );
        let schema = test_schema(&[
            ("tenant", ParseAsType::String),
            ("user_id", ParseAsType::U32),
        ]);
        let batch = |tenant: &str, user_id| {
            RelayRecordBatch::single(
                schema.clone(),
                string_branch_key("tenant", tenant),
                test_runtime_row([
                    (
                        "tenant".to_string(),
                        RuntimeValue::String(tenant.to_string()),
                    ),
                    ("user_id".to_string(), RuntimeValue::U32(user_id)),
                ]),
                AckSet::empty(),
            )
            .expect("ingestor output batch should build")
        };
        let acme_one = batch("acme", 1);
        let max_batch_size = acme_one.estimated_bytes() + 1;
        let route_runtime = IngestorRouteRuntime::new(
            runtime,
            domain,
            named(source),
            StdArc::new(ArcSwapOption::from(None)),
            IngestorRouteTemplate {
                branch: BranchInstanceTemplate {
                    source_kind,
                    source: named(source),
                    root_relay: root_relay.clone(),
                    branch: None,
                    branch_ttl: None,
                    branch_max_instances: None,
                    error_policies: ErrorPolicies::handled_by_log(),
                    relays: [(
                        root_relay,
                        RelayProcessorRelayTemplate {
                            registry,
                            services: services.clone(),
                        },
                    )]
                    .into_iter()
                    .collect(),
                    processors: HashMap::default(),
                },
                ack_boundary,
                flush_policy: RuntimeFlushPolicy::Each {
                    interval: Duration::from_secs(10),
                    max_batch_size,
                },
            },
            Duration::from_secs(30),
        );

        route_runtime
            .sender()
            .send(acme_one)
            .await
            .expect("acme batch should enter the route");
        route_runtime
            .sender()
            .send(batch("beta", 1))
            .await
            .expect("beta batch should enter the route");
        assert!(
            timeout(Duration::from_millis(50), fan_in.recv())
                .await
                .is_err(),
            "different branches must not share a size boundary"
        );

        route_runtime
            .sender()
            .send(batch("acme", 2))
            .await
            .expect("second acme batch should enter the route");
        let acme = timeout(Duration::from_secs(1), fan_in.recv())
            .await
            .expect("acme size boundary should flush")
            .expect("runtime consumer should remain open");
        assert_eq!(key_label(&acme.key), r#"{"tenant":"acme"}"#);
        assert_eq!(acme.message_count(), 2);
        assert!(
            timeout(Duration::from_millis(50), fan_in.recv())
                .await
                .is_err(),
            "beta must remain pending until its own size boundary"
        );

        route_runtime
            .sender()
            .send(batch("beta", 2))
            .await
            .expect("second beta batch should enter the route");
        let beta = timeout(Duration::from_secs(1), fan_in.recv())
            .await
            .expect("beta size boundary should flush")
            .expect("runtime consumer should remain open");
        assert_eq!(key_label(&beta.key), r#"{"tenant":"beta"}"#);
        assert_eq!(beta.message_count(), 2);

        route_runtime.shutdown().await;
        owner_task
            .stop(Duration::from_secs(1))
            .await
            .expect("relay owner should stop");
    }
}
