//! The envelope the host evaluates for every record an emitter publishes.
//!
//! Layer: test harness.
//! - **Owns.** Compiling and evaluating envelope plans over published batches: fixed clauses,
//!   computed values, per-row failures and the scopes the values read.
//! - **Depends on.** The envelope plan, the runtime compile context and relay batch fixtures.
//! - **Must not know.** Connectors, the registry or the grammar.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use std::time::Duration;

use nervix_models::{ParseAsType, PropertyDeclaration};

use super::*;
use crate::{emitter_execution_plan::EnvelopeValue, runtime_schema::test_runtime_row};

/// What an envelope compiles against beyond its schemas: no materialized state, no lookups, no
/// branch and no UDFs.
struct CompileFixture {
    streams: HashMap<RelayName, RuntimeMaterializedRelaySpec>,
    lookups: HashMap<LookupName, Arc<LookupRuntime>>,
    branching: ResolvedBranching,
}

impl CompileFixture {
    fn new() -> Self {
        Self {
            streams: HashMap::default(),
            lookups: HashMap::default(),
            branching: ResolvedBranching::unbranched(),
        }
    }

    fn context(&self) -> RuntimeVmCompileContext<'_> {
        RuntimeVmCompileContext {
            available_materialized_streams: &self.streams,
            available_lookups: &self.lookups,
            current_branching: &self.branching,
            udfs: None,
        }
    }
}

fn vm_schema(schema: &Arc<CompiledSchema>) -> RuntimeVmSchema {
    RuntimeVmSchema {
        schema: schema.arrow_schema(),
        sensitivity: VmSchemaSensitivity::default(),
    }
}

/// The value of `target`, of type `ty`, computed by the expression `text`.
fn value(target: EnvelopeTarget, ty: ParseAsType, text: &str) -> EnvelopeValue {
    EnvelopeValue {
        target,
        ty,
        expression: expression(text),
    }
}

/// The envelope that carries `fixed` and computes `values`, compiled against `input` and, where
/// the route constructs one, `output`.
fn compile(
    fixed: RecordEnvelope,
    values: Vec<EnvelopeValue>,
    input: &Arc<CompiledSchema>,
    output: Option<&Arc<CompiledSchema>>,
) -> CompiledEnvelope {
    let plan = EmitterEnvelopePlan::computing(fixed, values, output.is_some())
        .expect("the envelope program lowers");
    let fixture = CompileFixture::new();
    CompiledEnvelope::compile(
        &named("enveloped"),
        &plan,
        EnvelopeSchemas {
            input: vm_schema(input),
            output: output.map(vm_schema),
        },
        fixture.context(),
    )
    .expect("the envelope program compiles")
}

fn batch(schema: Arc<CompiledSchema>, rows: Vec<Vec<(&str, RuntimeValue)>>) -> RelayRecordBatch {
    let messages = rows
        .into_iter()
        .map(|fields| {
            let (acks, _completion) = AckSet::root();
            let fields = fields
                .into_iter()
                .map(|(name, value)| (name.to_string(), value));
            RelayMessage {
                key: None,
                record: test_runtime_row(fields),
                acks,
            }
        })
        .collect::<Vec<_>>();
    RelayRecordBatch::from_messages(schema, messages).expect("the batch builds")
}

async fn evaluate(
    envelope: &CompiledEnvelope,
    published: &mut RelayRecordBatch,
    input: EnvelopeInput<'_>,
) -> RowEnvelopes {
    envelope
        .evaluate(
            &Executor::default(),
            published,
            input,
            &HashMap::default(),
            Timestamp::from_unix_nanos(1),
        )
        .await
        .expect("the envelope evaluates over the batch")
}

fn rows(
    envelopes: &RowEnvelopes,
    row_count: usize,
) -> Vec<Result<&RecordEnvelope, &EnvelopeError>> {
    (0..row_count)
        .map(|row| {
            envelopes
                .envelope(row)
                .expect("the envelopes describe every row of the batch")
                .map(|envelope| &**envelope)
        })
        .collect()
}

fn datetime(text: &str) -> RuntimeValue {
    RuntimeValue::Datetime(
        chrono::DateTime::parse_from_rfc3339(text).expect("the test datetime parses"),
    )
}

fn nanos(text: &str) -> Timestamp {
    let parsed = chrono::DateTime::parse_from_rfc3339(text).expect("the test datetime parses");
    Timestamp::from_unix_nanos(
        parsed
            .timestamp_nanos_opt()
            .expect("the test datetime fits nanoseconds"),
    )
}

fn persistent_with_delay() -> RecordEnvelope {
    RecordEnvelope {
        persistent: true,
        delay: Some(Duration::from_secs(5)),
        ..RecordEnvelope::default()
    }
}

#[nervix_primitives::test]
async fn an_envelope_that_computes_nothing_is_carried_by_every_row_alike() {
    let input = test_schema(&[("id", ParseAsType::String)]);
    let envelope = compile(persistent_with_delay(), Vec::new(), &input, None);
    let mut published = batch(
        input,
        vec![vec![("id", RuntimeValue::String("a".to_string()))]],
    );

    let envelopes = evaluate(&envelope, &mut published, EnvelopeInput::Published).await;

    assert_eq!(envelopes.row_count(), None);
    assert_eq!(
        envelopes.envelope(7),
        Some(Ok(&Arc::new(persistent_with_delay())))
    );
    assert_eq!(envelopes.estimated_bytes(), 0);
    assert_eq!(
        published.acks.len(),
        1,
        "the batch keeps its acknowledgements"
    );
}

#[nervix_primitives::test]
async fn computed_values_are_evaluated_for_each_published_row_beside_the_fixed_clauses() {
    let input = test_schema(&[
        ("id", ParseAsType::String),
        ("at", ParseAsType::Datetime),
        ("priority", ParseAsType::U8),
    ]);
    let envelope = compile(
        persistent_with_delay(),
        vec![
            value(
                EnvelopeTarget::Clause(EnvelopeClauseKind::Timestamp),
                ParseAsType::Datetime,
                "input.at",
            ),
            value(
                EnvelopeTarget::Clause(EnvelopeClauseKind::ReplyTo),
                ParseAsType::String,
                "'replies'",
            ),
            value(
                EnvelopeTarget::Clause(EnvelopeClauseKind::MessageId),
                ParseAsType::String,
                "input.id",
            ),
            value(
                EnvelopeTarget::Property(PropertyDeclaration {
                    name: "priority",
                    ty: PropertyType::U8,
                }),
                ParseAsType::U8,
                "input.priority",
            ),
            value(
                EnvelopeTarget::Property(PropertyDeclaration {
                    name: "content_type",
                    ty: PropertyType::String,
                }),
                ParseAsType::String,
                "'application/json'",
            ),
        ],
        &input,
        None,
    );
    let mut published = batch(
        input,
        [
            ("first", "2026-10-27T10:00:00Z", 1),
            ("second", "2026-10-27T10:00:01Z", 9),
        ]
        .into_iter()
        .map(|(id, at, priority)| {
            vec![
                ("id", RuntimeValue::String(id.to_string())),
                ("at", datetime(at)),
                ("priority", RuntimeValue::U8(priority)),
            ]
        })
        .collect(),
    );

    let envelopes = evaluate(&envelope, &mut published, EnvelopeInput::Published).await;

    let expected = |id: &str, at: &str, priority: u8| RecordEnvelope {
        timestamp: Some(nanos(at)),
        reply_to: Some("replies".to_string()),
        message_id: Some(id.to_string()),
        properties: vec![
            RecordProperty {
                name: "priority",
                value: PropertyValue::U8(priority),
            },
            RecordProperty {
                name: "content_type",
                value: PropertyValue::String("application/json".to_string()),
            },
        ],
        ..persistent_with_delay()
    };
    let first = expected("first", "2026-10-27T10:00:00Z", 1);
    let second = expected("second", "2026-10-27T10:00:01Z", 9);
    assert_eq!(envelopes.row_count(), Some(2));
    assert_eq!(rows(&envelopes, 2), vec![Ok(&first), Ok(&second)]);
    assert_eq!(
        envelopes.estimated_bytes(),
        first
            .held_bytes()
            .checked_add(second.held_bytes())
            .expect("small test sizes add")
    );
    assert_eq!(
        envelopes.estimated_bytes_of_rows(&[1]),
        second.held_bytes(),
        "a subset of rows counts only their own envelopes"
    );
    assert_eq!(
        published.acks.len(),
        2,
        "the batch keeps its acknowledgements"
    );
}

#[nervix_primitives::test]
async fn a_row_whose_value_fails_has_no_envelope_and_names_the_value_that_failed() {
    let input = test_schema(&[("left", ParseAsType::I64), ("divisor", ParseAsType::I64)]);
    let envelope = compile(
        RecordEnvelope::default(),
        vec![
            value(
                EnvelopeTarget::Clause(EnvelopeClauseKind::ReplyTo),
                ParseAsType::String,
                "'replies'",
            ),
            value(
                EnvelopeTarget::Clause(EnvelopeClauseKind::MessageId),
                ParseAsType::String,
                "(input.left / input.divisor) AS STRING",
            ),
        ],
        &input,
        None,
    );
    let mut published = batch(
        input,
        [(7, 2), (7, 0), (9, 3)]
            .into_iter()
            .map(|(left, divisor)| {
                vec![
                    ("left", RuntimeValue::I64(left)),
                    ("divisor", RuntimeValue::I64(divisor)),
                ]
            })
            .collect(),
    );

    let envelopes = evaluate(&envelope, &mut published, EnvelopeInput::Published).await;

    let outcomes = rows(&envelopes, 3);
    let [divided, failed, last] = outcomes[..] else {
        panic!("every published row has an envelope outcome: {outcomes:?}");
    };
    assert_eq!(
        divided.map(|envelope| envelope.message_id.clone()),
        Ok(Some("3".to_string()))
    );
    assert_eq!(
        last.map(|envelope| envelope.message_id.clone()),
        Ok(Some("3".to_string()))
    );
    let Err(EnvelopeError::ExpressionFailed { site, code, .. }) = failed else {
        panic!("dividing by zero fails only its own row: {failed:?}");
    };
    assert_eq!(*site, EnvelopeSite::Clause(EnvelopeClauseKind::MessageId));
    assert_eq!(*code, nervix_vm::ErrorCode::DivisionByZero);
    let message = failed
        .map_err(ToString::to_string)
        .expect_err("the failed row has no envelope");
    assert!(
        message.starts_with("envelope MESSAGE ID expression failed with division_by_zero at "),
        "{message}"
    );
    assert_eq!(
        envelopes.estimated_bytes_of_rows(&[1]),
        0,
        "a row without an envelope holds only its reason"
    );
}

#[nervix_primitives::test]
async fn values_read_their_own_source_record_and_the_output_the_route_constructed() {
    let input = test_schema(&[("raw", ParseAsType::String)]);
    let output = test_schema(&[("normalized", ParseAsType::String)]);
    let envelope = compile(
        RecordEnvelope::default(),
        vec![
            value(
                EnvelopeTarget::Clause(EnvelopeClauseKind::MessageId),
                ParseAsType::String,
                "input.raw",
            ),
            value(
                EnvelopeTarget::Clause(EnvelopeClauseKind::DeduplicationId),
                ParseAsType::String,
                "output.normalized",
            ),
            value(
                EnvelopeTarget::Clause(EnvelopeClauseKind::ReplyTo),
                ParseAsType::String,
                "message.normalized",
            ),
        ],
        &input,
        Some(&output),
    );
    let source = batch(
        input,
        ["Ignored", "Alpha", "Beta"]
            .into_iter()
            .map(|raw| vec![("raw", RuntimeValue::String(raw.to_string()))])
            .collect(),
    );
    let records = SourceRecords::of(&source);
    // The route kept the second and third source rows, in the reverse of their source order.
    let mut published = batch(
        output,
        ["beta", "alpha"]
            .into_iter()
            .map(|normalized| vec![("normalized", RuntimeValue::String(normalized.to_string()))])
            .collect(),
    );

    let envelopes = evaluate(
        &envelope,
        &mut published,
        EnvelopeInput::Source {
            records: &records,
            rows: &[2, 1],
        },
    )
    .await;

    let read = |raw: &str, normalized: &str| RecordEnvelope {
        message_id: Some(raw.to_string()),
        deduplication_id: Some(normalized.to_string()),
        reply_to: Some(normalized.to_string()),
        ..RecordEnvelope::default()
    };
    assert_eq!(
        rows(&envelopes, 2),
        vec![Ok(&read("Beta", "beta")), Ok(&read("Alpha", "alpha"))]
    );
}

#[test]
fn a_publish_batch_without_envelopes_gives_no_row_one_and_rows_share_a_fixed_envelope() {
    let input = test_schema(&[("id", ParseAsType::String)]);
    let published = batch(
        input,
        vec![
            vec![("id", RuntimeValue::String("a".to_string()))],
            vec![("id", RuntimeValue::String("b".to_string()))],
        ],
    );
    let publish_batch = EmitterPublishBatch::from_batch(published, Timestamp::from_unix_nanos(1));
    assert_eq!(
        publish_batch
            .envelope(1)
            .expect("a batch without envelopes answers for every row"),
        Ok(None)
    );
    assert!(
        publish_batch.envelope(2).is_ok(),
        "a batch without envelopes answers alike for any row"
    );

    let fixed =
        publish_batch.with_envelopes(RowEnvelopes::Fixed(Arc::new(persistent_with_delay())));
    let carried = |row: usize| match fixed.envelope(row).expect("the row exists") {
        Ok(Some(envelope)) => envelope,
        other => panic!("row {row} carries the fixed envelope, found {other:?}"),
    };
    assert_eq!(*carried(1), persistent_with_delay());
    assert!(
        Arc::ptr_eq(&carried(0), &carried(1)),
        "every row shares the one fixed envelope rather than a copy of it"
    );
}

#[nervix_primitives::test]
async fn evaluated_envelopes_stay_with_the_publish_batch_they_were_evaluated_over() {
    let input = test_schema(&[("id", ParseAsType::String)]);
    let envelope = compile(
        RecordEnvelope::default(),
        vec![value(
            EnvelopeTarget::Clause(EnvelopeClauseKind::MessageId),
            ParseAsType::String,
            "input.id",
        )],
        &input,
        None,
    );
    let published = batch(
        input,
        vec![
            vec![("id", RuntimeValue::String("a".to_string()))],
            vec![("id", RuntimeValue::String("b".to_string()))],
        ],
    );
    let publish_batch = EmitterPublishBatch::from_batch(published, Timestamp::from_unix_nanos(1));

    let enveloped = publish_batch
        .with_evaluated_envelopes(
            &envelope,
            &Executor::default(),
            EnvelopeInput::Published,
            &HashMap::default(),
            Timestamp::from_unix_nanos(1),
        )
        .await
        .expect("the envelope evaluates over the batch");

    let message_ids = [0, 1].map(
        |row| match enveloped.envelope(row).expect("the row exists") {
            Ok(Some(envelope)) => envelope.message_id.clone(),
            Ok(None) => panic!("row {row} has an envelope"),
            Err(failure) => panic!("row {row} has an envelope: {failure}"),
        },
    );
    assert_eq!(message_ids, [Some("a".to_string()), Some("b".to_string())]);
    assert_eq!(
        enveloped.relay_batch().acks.len(),
        2,
        "the batch keeps its acknowledgements"
    );
}

#[nervix_primitives::test]
async fn every_property_type_carries_a_value_of_its_own_type() {
    let input = test_schema(&[
        ("f_u8", ParseAsType::U8),
        ("f_i8", ParseAsType::I8),
        ("f_u16", ParseAsType::U16),
        ("f_i16", ParseAsType::I16),
        ("f_u32", ParseAsType::U32),
        ("f_i32", ParseAsType::I32),
        ("f_u64", ParseAsType::U64),
        ("f_i64", ParseAsType::I64),
        ("f_bool", ParseAsType::Bool),
        ("f_text", ParseAsType::String),
        ("f_at", ParseAsType::Datetime),
    ]);
    // Each property reads the input field of its own type, and the bytes property the UTF-8 bytes
    // of the text, since no input field holds bytes.
    let declared = [
        ("p_u8", PropertyType::U8, "input.f_u8"),
        ("p_i8", PropertyType::I8, "input.f_i8"),
        ("p_u16", PropertyType::U16, "input.f_u16"),
        ("p_i16", PropertyType::I16, "input.f_i16"),
        ("p_u32", PropertyType::U32, "input.f_u32"),
        ("p_i32", PropertyType::I32, "input.f_i32"),
        ("p_u64", PropertyType::U64, "input.f_u64"),
        ("p_i64", PropertyType::I64, "input.f_i64"),
        ("p_bool", PropertyType::Bool, "input.f_bool"),
        ("p_text", PropertyType::String, "input.f_text"),
        ("p_at", PropertyType::Datetime, "input.f_at"),
        (
            "p_bytes",
            PropertyType::Bytes,
            "bytes_from_utf8(input.f_text)",
        ),
    ];
    let values = declared
        .iter()
        .map(|(name, ty, text)| {
            value(
                EnvelopeTarget::Property(PropertyDeclaration { name, ty: *ty }),
                ParseAsType::from(*ty),
                text,
            )
        })
        .collect();
    let envelope = compile(RecordEnvelope::default(), values, &input, None);
    // Each value lies outside the range of every narrower or differently signed type, so a
    // property read through another type's accessor cannot produce it.
    let mut published = batch(
        input,
        vec![vec![
            ("f_u8", RuntimeValue::U8(200)),
            ("f_i8", RuntimeValue::I8(-100)),
            ("f_u16", RuntimeValue::U16(60_000)),
            ("f_i16", RuntimeValue::I16(-30_000)),
            ("f_u32", RuntimeValue::U32(4_000_000_000)),
            ("f_i32", RuntimeValue::I32(-2_000_000_000)),
            ("f_u64", RuntimeValue::U64(u64::MAX)),
            ("f_i64", RuntimeValue::I64(i64::MIN)),
            ("f_bool", RuntimeValue::Bool(true)),
            ("f_text", RuntimeValue::String("text".to_string())),
            ("f_at", datetime("2026-10-27T10:00:00Z")),
        ]],
    );

    let envelopes = evaluate(&envelope, &mut published, EnvelopeInput::Published).await;

    let property = |name: &'static str, value: PropertyValue| RecordProperty { name, value };
    let expected = RecordEnvelope {
        properties: vec![
            property("p_u8", PropertyValue::U8(200)),
            property("p_i8", PropertyValue::I8(-100)),
            property("p_u16", PropertyValue::U16(60_000)),
            property("p_i16", PropertyValue::I16(-30_000)),
            property("p_u32", PropertyValue::U32(4_000_000_000)),
            property("p_i32", PropertyValue::I32(-2_000_000_000)),
            property("p_u64", PropertyValue::U64(u64::MAX)),
            property("p_i64", PropertyValue::I64(i64::MIN)),
            property("p_bool", PropertyValue::Bool(true)),
            property("p_text", PropertyValue::String("text".to_string())),
            property(
                "p_at",
                PropertyValue::Datetime(nanos("2026-10-27T10:00:00Z")),
            ),
            property("p_bytes", PropertyValue::Bytes(b"text".to_vec())),
        ],
        ..RecordEnvelope::default()
    };
    assert_eq!(rows(&envelopes, 1), vec![Ok(&expected)]);
}

#[test]
fn an_emitter_compiles_an_envelope_only_where_it_writes_one() {
    let input = test_schema(&[("id", ParseAsType::String)]);
    let output = test_schema(&[("key", ParseAsType::String)]);
    let fixture = CompileFixture::new();

    let plain =
        CompiledEnvelope::for_emitter(&named("plain"), None, &input, &output, fixture.context())
            .expect("an emitter that writes no envelope compiles");
    // The value reads the output the route constructs, so it compiles only where the emitter
    // hands the output schema to the program.
    let plan = EmitterEnvelopePlan::computing(
        RecordEnvelope::default(),
        vec![value(
            EnvelopeTarget::Clause(EnvelopeClauseKind::MessageId),
            ParseAsType::String,
            "output.key",
        )],
        true,
    )
    .expect("the envelope program lowers");
    let enveloped = CompiledEnvelope::for_emitter(
        &named("enveloped"),
        Some(&plan),
        &input,
        &output,
        fixture.context(),
    )
    .expect("a value that reads the constructed output compiles against the output schema");

    assert!(
        plain.is_none(),
        "an emitter that writes no envelope compiles none"
    );
    assert!(
        enveloped.is_some(),
        "an emitter that writes an envelope compiles it"
    );
}
