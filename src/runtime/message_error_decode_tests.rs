//! Decode rejection tests.
//!
//! Layer: test harness.
//!
//! - **Owns.** The record a codec refusal becomes, its transport position, the fields it names and
//!   how an ingestor's general error policy reports it.
//! - **Depends on.** The runtime's codec compilation, its runtime event bus and its test fixtures.
//! - **Must not know.** A source driver or how a source host settles a payload on its transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_connector::{IngestMetadataRow, NoIngestHeaders};
use nervix_execution::{Executor, QueueAdmission};
use nervix_models::{
    CodecWireFormat, CreateCodec, CreateSchema, CreateWireSchema, JsonType, ParseAsType,
    ResolvedCodecWireFormat, SchemaField, WireSchemaField,
};

use super::*;
use crate::runtime_schema::{CodecError, compile_codec, compile_schema};

/// A strict JSON codec whose one field is a required integer.
fn event_codec() -> Arc<CompiledCodec> {
    let schema = Arc::new(compile_schema(&CreateSchema {
        name: named("event"),
        fields: vec![SchemaField {
            name: named("user_id"),
            ty: ParseAsType::I64,
            optional: false,
            sensitive: false,
        }],
    }));
    compile_codec(
        &CreateCodec {
            name: named("event_codec"),
            wire_format: CodecWireFormat::Json {
                wire_schema: named("event_wire"),
            },
            schema: named("event"),
            encoding_rules: Vec::new(),
        },
        schema,
        ResolvedCodecWireFormat::Json(&CreateWireSchema {
            name: named("event_wire"),
            strictness: Default::default(),
            fields: vec![WireSchemaField {
                name: named("user_id"),
                ty: JsonType::Integer,
                optional: false,
            }],
        }),
    )
    .assured("the event codec compiles")
}

/// The report `codec` refuses `payload` with, as an ingest group decodes it.
async fn refusal(codec: &Arc<CompiledCodec>, payload: &[u8]) -> Report<CodecError> {
    let mut decoder = JsonDecoder::default();
    let mut records = codec.schema().batch_builder(1);
    let decoded = decode_ingested_payload(
        &Executor::default(),
        QueueAdmission::RefuseWhenFull,
        codec,
        payload,
        &mut decoder,
        &mut records,
    )
    .await;
    match decoded {
        Err(PayloadDecodeFailure::Codec(report)) => report,
        Err(PayloadDecodeFailure::NotAdmitted(refusal)) => {
            panic!("an idle executor unfolded nothing for a JSON codec: {refusal:?}")
        }
        Ok(messages) => panic!("the test payload decoded into {messages} messages"),
    }
}

fn kafka_metadata() -> IngestMetadataRow<'static> {
    IngestMetadataRow {
        position: Some(nervix_connector::IngestTransportPosition::Kafka {
            topic: "orders",
            partition: 3,
            offset: 42,
        }),
        ..IngestMetadataRow::headers(&NoIngestHeaders)
    }
}

fn occurred_at() -> Timestamp {
    Timestamp::from_unix_nanos(1_700_000_000_000_000_000)
}

#[nervix_primitives::test]
async fn a_refused_field_is_named_with_its_codec_and_its_kafka_position() {
    let codec = event_codec();
    let report = refusal(&codec, br#"{"user_id":"one"}"#).await;
    let rejection = DecodeRejection::new(&codec, &kafka_metadata(), &report, occurred_at());

    assert_eq!(rejection.codec, CodecName::from(&codec.name));
    assert_eq!(
        rejection.position,
        Some(TransportPosition::Kafka {
            topic: "orders".to_string(),
            partition: 3,
            offset: 42,
        })
    );
    assert_eq!(
        rejection
            .fields
            .iter()
            .map(FieldPath::as_str)
            .collect::<Vec<_>>(),
        ["user_id"]
    );
    assert_eq!(rejection.message, format!("{report:#}"));
    assert_eq!(rejection.occurred_at, occurred_at());
    assert_eq!(
        rejection.to_string(),
        format!(
            "{} at {}: codec 'event_codec' refused the payload from Kafka topic 'orders' \
             partition 3 offset 42: {report:#}",
            rejection.reference,
            occurred_at().to_rfc3339()
        )
    );
}

#[nervix_primitives::test]
async fn a_payload_that_is_not_json_names_no_field_and_a_header_source_no_position() {
    let codec = event_codec();
    let report = refusal(&codec, b"not json").await;
    let headers = IngestMetadataRow::headers(&NoIngestHeaders);
    let rejection = DecodeRejection::new(&codec, &headers, &report, occurred_at());

    assert_eq!(rejection.position, None);
    assert!(rejection.fields.is_empty());
    assert_eq!(
        rejection.to_string(),
        format!(
            "{} at {}: codec 'event_codec' refused the payload: {report:#}",
            rejection.reference,
            occurred_at().to_rfc3339()
        )
    );
}

#[nervix_primitives::test]
async fn a_syslog_peer_is_the_position_of_its_frame() {
    let codec = event_codec();
    let report = refusal(&codec, b"{}").await;
    let peer = "192.0.2.7:514"
        .parse()
        .assured("the peer is a socket address literal");
    let rejection = DecodeRejection::new(
        &codec,
        &IngestMetadataRow {
            position: Some(nervix_connector::IngestTransportPosition::Syslog { peer }),
            ..IngestMetadataRow::headers(&NoIngestHeaders)
        },
        &report,
        occurred_at(),
    );

    let position = rejection
        .position
        .as_ref()
        .assured("a syslog frame names its peer");
    assert_eq!(*position, TransportPosition::Syslog { peer });
    assert_eq!(position.to_string(), "syslog peer 192.0.2.7:514");
    assert_eq!(
        rejection
            .fields
            .iter()
            .map(FieldPath::as_str)
            .collect::<Vec<_>>(),
        ["user_id"],
        "a missing field is named at fault"
    );
}

#[nervix_primitives::test]
async fn each_rejection_has_a_reference_of_its_own() {
    let codec = event_codec();
    let report = refusal(&codec, b"not json").await;
    let first = DecodeRejection::new(&codec, &kafka_metadata(), &report, occurred_at());
    let second = DecodeRejection::new(&codec, &kafka_metadata(), &report, occurred_at());

    assert_ne!(first.reference, second.reference);
    assert_eq!(first.reference.get_version_num(), 7);
}

/// `LOG` publishes the rejection after the ingestor's name and domain, and `IGNORE` publishes
/// nothing: events arrive in the order they were reported, so the first one is the logged
/// rejection reported after the ignored one.
#[nervix_primitives::test]
async fn the_general_policy_logs_a_rejection_or_reports_nothing() {
    let runtime = Runtime::default();
    let mut events = runtime.subscribe_events();
    let codec = event_codec();
    let report = refusal(&codec, br#"{"user_id":"one"}"#).await;
    let ignored = DecodeRejection::new(&codec, &kafka_metadata(), &report, occurred_at());
    let logged = DecodeRejection::new(&codec, &kafka_metadata(), &report, occurred_at());
    let domain = domain("orders");
    let ingestor: IngestorName = named("orders_source");

    runtime.report_decode_rejection(&domain, &ingestor, &GeneralErrorPolicy::Ignore, &ignored);
    runtime.report_decode_rejection(&domain, &ingestor, &GeneralErrorPolicy::Log, &logged);

    let RuntimeEvent::Error(message) = events
        .recv()
        .await
        .assured("a logged rejection is reported to the node's observers");
    assert_eq!(
        message,
        format!(
            "ingestor 'orders_source' general error in domain 'orders': decode rejection {logged}"
        )
    );
}
