//! Generated Arrow batches through the relay body and snapshot section codecs.
//!
//! Layer: test harness.
//!
//! - **Owns.** The round-trip and damaged-body properties of every Arrow IPC body the runtime
//!   encodes and decodes, and the boundary of each limit those decoders enforce.
//! - **Depends on.** The generated batches and their logical oracle, the body codecs and the
//!   executor that charges them.
//! - **Must not know.** Relays, peers or the interconnect that carries a body.

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema as ArrowSchema;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_execution::{ChargedBytes, ExecutionConfig, Executor, MemoryClass, OperationLimits};
use nervix_primitives::sync::StdArc;
use rstest::rstest;
use ubyte::ByteUnit;

use super::ArrowBodyError;
use crate::runtime_schema::{
    RuntimeRecordBatch,
    crafted_streams::{StreamDefect, receiver_schema},
    generated_batches::{
        Damage, GeneratedDomain, GeneratedSchema, assert_rewritten_batch, assert_same_batch,
    },
    ipc_stream::IpcStreamError,
};

/// The bytes one round-trip case reads its schema, rows and section count from.
const ROUND_TRIP_BYTES: usize = 2048;

/// The bytes one damaged-body case reads its batch and damage from.
const DAMAGED_BYTES: usize = 2048;

/// The most sections a generated multi-section body carries.
const SECTIONS: usize = 3;

/// One generated batch and the runtime that drives the asynchronous codecs over it.
struct BodyCase {
    schema: GeneratedSchema,
    rows: RecordBatch,
}

impl BodyCase {
    fn new(arbitrary: &mut Arbitrary<'_>) -> Self {
        let schema = GeneratedDomain::Arrow.schema(arbitrary);
        let rows = GeneratedDomain::Arrow.batch(arbitrary, &schema);
        Self { schema, rows }
    }

    fn runtime_batch(&self) -> RuntimeRecordBatch {
        self.schema.runtime_batch(self.rows.clone())
    }

    fn arrow_schema(&self) -> StdArc<ArrowSchema> {
        self.schema.compiled.arrow_schema()
    }

    /// The rows written as `sections` record batch messages of one stream, as a peer whose body
    /// carried more than one section would have written them.
    fn sections(&self, sections: usize) -> Vec<u8> {
        let mut body = Vec::new();
        let mut writer =
            StreamWriter::try_new(&mut body, &self.arrow_schema()).assured("the schema writes");
        for _ in 0..sections {
            writer.write(&self.rows).assured("a generated batch writes");
        }
        writer.finish().assured("the stream ends");
        drop(writer);
        body
    }
}

fn property_runtime() -> nervix_primitives::runtime::Runtime {
    nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the property runtime opens")
}

fn charged(executor: &Executor, bytes: Vec<u8>) -> ChargedBytes {
    executor
        .try_charge_owned(MemoryClass::Relay, bytes)
        .assured("a bounded test body fits the relay budget")
}

/// The batch `sections` copies of `rows` decode into when a decoder concatenates them.
fn repeated(rows: &RecordBatch, sections: usize) -> RecordBatch {
    let copies = vec![rows.clone(); sections];
    arrow_select::concat::concat_batches(&rows.schema(), &copies)
        .assured("copies of one batch share its schema")
}

/// Every relay body and snapshot section decodes to the batch it was encoded from: the exact
/// schema, every row, and every value and null, floats by their bits, through a view whose columns
/// start inside a larger batch. A body of several sections is refused by the decoders that accept
/// exactly one and concatenated by the one that accepts any number. Nothing stays charged.
#[test]
fn bolero_relay_bodies_carry_the_exact_schema_and_every_value() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(128)
        .with_max_len(ROUND_TRIP_BYTES)
        .for_each(|input| {
            let executor = Executor::default();
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let case = BodyCase::new(&mut arbitrary);
            let sections = arbitrary
                .entropy()
                .count(SECTIONS.checked_sub(2).assured("more than one section"))
                .checked_add(2)
                .verified("a small count");
            runtime.block_on(async {
                let batch = case.runtime_batch();
                let body = batch
                    .encode_arrow_ipc(&executor)
                    .await
                    .assured("a bounded batch encodes");
                let exact = case
                    .schema
                    .compiled
                    .decode_arrow_body(&executor, body.clone())
                    .await
                    .assured("the encoded body decodes against its schema");
                assert_same_batch(exact.batch(), &case.rows);
                let any = RuntimeRecordBatch::decode_arrow_ipc(&executor, body)
                    .await
                    .assured("the encoded body decodes without a known schema");
                assert_same_batch(any.batch(), &case.rows);

                let section = batch
                    .encode_arrow_snapshot_section(&executor)
                    .await
                    .assured("a bounded batch encodes as a section");
                let restored = RuntimeRecordBatch::decode_arrow_snapshot_section(
                    &executor,
                    case.arrow_schema(),
                    section,
                )
                .await
                .assured("the section decodes against its schema");
                assert_same_batch(restored.batch(), &case.rows);

                let several = charged(&executor, case.sections(sections));
                let refused = case
                    .schema
                    .compiled
                    .decode_arrow_body(&executor, several.clone())
                    .await
                    .expect_err("a relay body carries exactly one section");
                assert!(
                    matches!(
                        refused.current_context(),
                        ArrowBodyError::TooManySections { sections: counted, limit: 1 }
                            if *counted == sections
                    ),
                    "every section past the first is refused: {refused:?}"
                );
                let section_refused = RuntimeRecordBatch::decode_arrow_snapshot_section(
                    &executor,
                    case.arrow_schema(),
                    several.clone(),
                )
                .await
                .expect_err("a snapshot section carries exactly one section");
                assert!(matches!(
                    section_refused.current_context(),
                    ArrowBodyError::TooManySections { sections: counted, limit: 1 }
                        if *counted == sections
                ));
                let concatenated = RuntimeRecordBatch::decode_arrow_ipc(&executor, several)
                    .await
                    .assured("a body of several sections concatenates them");
                assert_same_batch(concatenated.batch(), &repeated(&case.rows, sections));
            });
            let snapshot = executor.snapshot();
            assert_eq!(
                snapshot.relay_memory.reserved_bytes, 0,
                "no relay charge outlives its body"
            );
            assert_eq!(
                snapshot.bulk_memory.reserved_bytes, 0,
                "no section charge outlives it"
            );
        });
}

/// A damaged relay body, snapshot section or arbitrary bytes either fail with a typed body
/// failure the decoder reports for the bytes themselves, never one of admission or execution, or
/// decode to a valid batch: of the expected schema where the decoder knows it, and one that
/// encodes and decodes back to itself.
#[test]
fn bolero_damaged_relay_bodies_fail_typed_or_decode_within_their_contract() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(DAMAGED_BYTES)
        .for_each(|input| {
            let executor = Executor::default();
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let case = BodyCase::new(&mut arbitrary);
            let damage = arbitrary.entropy().pick(Damage::ALL);
            let valid = case.sections(1);
            let damaged = damage.apply(arbitrary.entropy(), valid);
            runtime.block_on(async {
                let exact = case
                    .schema
                    .compiled
                    .decode_arrow_body(&executor, charged(&executor, damaged.clone()))
                    .await;
                check_damaged_outcome(&executor, exact, Some(&case.arrow_schema())).await;
                let section = RuntimeRecordBatch::decode_arrow_snapshot_section(
                    &executor,
                    case.arrow_schema(),
                    charged(&executor, damaged.clone()),
                )
                .await;
                check_damaged_outcome(&executor, section, Some(&case.arrow_schema())).await;
                let any =
                    RuntimeRecordBatch::decode_arrow_ipc(&executor, charged(&executor, damaged))
                        .await;
                check_damaged_outcome(&executor, any, None).await;
            });
            let snapshot = executor.snapshot();
            assert_eq!(
                snapshot.relay_memory.reserved_bytes, 0,
                "no relay charge outlives its body"
            );
            assert_eq!(
                snapshot.bulk_memory.reserved_bytes, 0,
                "no section charge outlives it"
            );
        });
}

async fn check_damaged_outcome(
    executor: &Executor,
    outcome: Result<RuntimeRecordBatch, error_stack::Report<ArrowBodyError>>,
    expected: Option<&StdArc<ArrowSchema>>,
) {
    let decoded = match outcome {
        Ok(decoded) => decoded,
        Err(failure) => {
            assert!(
                matches!(
                    failure.current_context(),
                    ArrowBodyError::Decode { .. }
                        | ArrowBodyError::TooManySections { .. }
                        | ArrowBodyError::NoSection
                        | ArrowBodyError::DecodedTooLarge { .. }
                ),
                "a damaged body fails with the defect of its own bytes: {failure:?}"
            );
            return;
        }
    };
    let body = decoded
        .encode_arrow_ipc(executor)
        .await
        .assured("a decoded batch encodes again");
    let again = RuntimeRecordBatch::decode_arrow_ipc(executor, body)
        .await
        .assured("a re-encoded valid batch decodes");
    let Some(expected) = expected else {
        // Without a known schema the damaged stream may declare a zone no schema of the node's
        // declares, which the writer rewrites.
        assert_rewritten_batch(again.batch(), decoded.batch());
        return;
    };
    assert_eq!(
        &decoded.schema(),
        expected,
        "an accepted body has the expected schema"
    );
    assert_same_batch(again.batch(), decoded.batch());
}

/// A relay with the given decoded limit and the default everything else.
fn executor_with(limits: OperationLimits) -> Executor {
    Executor::new(ExecutionConfig {
        limits,
        ..ExecutionConfig::default()
    })
    .assured("a narrower relay limit keeps the default budgets valid")
}

/// The first generated case holding a row, drawn from bytes spread from `seed`, for the boundary
/// tests below.
fn fixed_case(seed: u8) -> BodyCase {
    for attempt in 0..=u8::MAX {
        let mut bytes = Vec::with_capacity(512);
        for index in 0..512_usize {
            let scaled = index
                .checked_mul(31)
                .assured("a small index times a small factor fits in usize");
            let spread = scaled
                .checked_add(usize::from(seed))
                .assured("a small sum fits in usize");
            let spread = spread
                .checked_add(usize::from(attempt))
                .assured("a small sum fits in usize");
            bytes.push(u8::try_from(spread % 256).assured("a remainder of 256 is one byte"));
        }
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
        let case = BodyCase::new(&mut arbitrary);
        if case.rows.num_rows() > 0 {
            return case;
        }
    }
    panic!("no spread of seed {seed} generates a row");
}

#[nervix_primitives::test]
async fn a_body_longer_than_the_encoded_limit_is_refused_before_it_is_read() {
    let case = fixed_case(7);
    let body = case.sections(1);
    let length = u64::try_from(body.len()).assured("a bounded body length");
    let limit = length.checked_sub(1).assured("an encoded body has bytes");
    let executor = executor_with(OperationLimits {
        relay_encoded_bytes: ByteUnit::Byte(limit),
        ..OperationLimits::default()
    });
    let refused = case
        .schema
        .compiled
        .decode_arrow_body(&executor, charged(&executor, body))
        .await
        .expect_err("the body is one byte over the limit");
    assert!(
        matches!(
            refused.current_context(),
            ArrowBodyError::BodyTooLarge { size, limit: refused_limit }
                if *size == length && *refused_limit == limit
        ),
        "the refusal names the size and the limit: {refused:?}"
    );
}

#[nervix_primitives::test]
async fn sections_that_decode_past_the_decoded_limit_stop_at_the_section_that_crossed_it() {
    let case = fixed_case(9);
    // The limit is written in the bytes a decoded batch holds, which a view cut from a larger
    // batch need not equal, so it is measured on the decoded section itself.
    let generous = Executor::default();
    let measured = case
        .schema
        .compiled
        .decode_arrow_body(&generous, charged(&generous, case.sections(1)))
        .await
        .assured("one section decodes under the default limits");
    let one = super::batch_payload_bytes(measured.batch());
    assert!(one > 0, "a section of rows holds bytes");
    let executor = executor_with(OperationLimits {
        relay_decoded_bytes: ByteUnit::Byte(one),
        ..OperationLimits::default()
    });
    let accepted = case
        .schema
        .compiled
        .decode_arrow_body(&executor, charged(&executor, case.sections(1)))
        .await
        .assured("one section of exactly the decoded limit decodes");
    assert_same_batch(accepted.batch(), &case.rows);
    let refused =
        RuntimeRecordBatch::decode_arrow_ipc(&executor, charged(&executor, case.sections(2)))
            .await
            .expect_err("two sections decode past the limit");
    assert!(
        matches!(
            refused.current_context(),
            ArrowBodyError::DecodedTooLarge { limit, .. } if *limit == one
        ),
        "the refusal names the decoded limit: {refused:?}"
    );
}

#[nervix_primitives::test]
async fn a_stream_without_a_section_is_refused_where_one_is_required() {
    let case = fixed_case(11);
    let executor = Executor::default();
    let empty = case.sections(0);
    let refused = case
        .schema
        .compiled
        .decode_arrow_body(&executor, charged(&executor, empty.clone()))
        .await
        .expect_err("a relay body carries one section");
    assert!(matches!(
        refused.current_context(),
        ArrowBodyError::NoSection
    ));
    let section_refused = RuntimeRecordBatch::decode_arrow_snapshot_section(
        &executor,
        case.arrow_schema(),
        charged(&executor, empty.clone()),
    )
    .await
    .expect_err("a snapshot section carries one section");
    assert!(matches!(
        section_refused.current_context(),
        ArrowBodyError::NoSection
    ));
    let any = RuntimeRecordBatch::decode_arrow_ipc(&executor, charged(&executor, empty))
        .await
        .assured("a stream of no sections is an empty batch of its schema");
    assert_eq!(any.schema(), case.arrow_schema());
    assert_eq!(any.batch().num_rows(), 0);
}

#[nervix_primitives::test]
async fn a_body_of_another_schema_is_refused_where_the_schema_is_known() {
    let case = fixed_case(13);
    let other = fixed_case(17);
    assert_ne!(
        case.arrow_schema(),
        other.arrow_schema(),
        "the fixed cases differ"
    );
    let executor = Executor::default();
    let refused = case
        .schema
        .compiled
        .decode_arrow_body(&executor, charged(&executor, other.sections(1)))
        .await
        .expect_err("the body declares another schema");
    assert!(matches!(
        refused.current_context(),
        ArrowBodyError::Decode { .. }
    ));
}

/// A body declaring what Arrow's reader would panic on is refused as undecodable by the framing
/// scan, before the reader reads it, by every relay body decoder.
#[rstest]
fn a_body_arrows_reader_would_panic_on_is_refused_before_it_is_read(
    #[values(
        StreamDefect::BufferPastBody,
        StreamDefect::IntegerOfSevenBits,
        StreamDefect::ListWithoutChild,
        StreamDefect::DictionaryEncoded,
        StreamDefect::SchemaWithoutFields,
        StreamDefect::ValidityShorterThanRows,
        StreamDefect::OffsetsCutInsideAnOffset,
        StreamDefect::VariadicBufferCounts,
        StreamDefect::FixedSizeListTooLongToCount
    )]
    defect: StreamDefect,
) {
    let schema = receiver_schema();
    let stream = defect.stream();
    let executor = Executor::default();
    property_runtime().block_on(async {
        let exact = schema
            .compiled
            .decode_arrow_body(&executor, charged(&executor, stream.clone()))
            .await
            .expect_err("the stream declares what no valid stream does");
        let any = RuntimeRecordBatch::decode_arrow_ipc(&executor, charged(&executor, stream))
            .await
            .expect_err("the stream declares what no valid stream does");
        for refused in [exact, any] {
            assert!(
                matches!(refused.current_context(), ArrowBodyError::Decode { .. }),
                "the body is refused as undecodable: {refused:?}"
            );
            assert_eq!(
                refused.downcast_ref::<IpcStreamError>(),
                Some(&defect.refusal()),
                "the scan refuses the body for what it declares"
            );
        }
    });
}
