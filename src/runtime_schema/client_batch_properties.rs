//! Generated producer batches through the client library's encoder and the node's decoder.
//!
//! Layer: test harness.
//!
//! - **Owns.** The round-trip and damaged-stream properties of the one Arrow IPC form a client
//!   producer submits a batch in.
//! - **Depends on.** The generated batches and their logical oracle, the client library's batch
//!   encoder and the node's client batch decoder.
//! - **Must not know.** Producers, sessions, ingestors or what happens to an accepted batch.

use std::num::{NonZeroU64, NonZeroUsize};

use arrow_array::RecordBatch;
use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_client_core::ProducerBatch;
use nervix_execution::Executor;
use nervix_models::{ClientBatchDefect, SchemaField};
use rstest::rstest;

use super::{ClientBatchError, ClientBatchLimits};
use crate::runtime_schema::{
    crafted_streams::{StreamDefect, receiver_schema},
    generated_batches::{Damage, GeneratedDomain, GeneratedSchema, assert_same_batch},
    ipc_stream::IpcStreamError,
};

/// The bytes one case reads its schema, rows and damage from.
const CASE_BYTES: usize = 2048;

/// The most rows the generous limits below admit.
const GENEROUS_ROWS: usize = 65_536;

/// The most bytes the generous limits below admit.
const GENEROUS_BYTES: u64 = 4 * 1024 * 1024;

fn property_runtime() -> nervix_primitives::runtime::Runtime {
    nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the property runtime opens")
}

fn generous_limits() -> ClientBatchLimits {
    ClientBatchLimits {
        max_bytes: NonZeroU64::new(GENEROUS_BYTES).assured("a literal non-zero limit"),
        max_rows: NonZeroUsize::new(GENEROUS_ROWS).assured("a literal non-zero limit"),
    }
}

/// The stream the client library submits for `rows`.
fn submitted(rows: &RecordBatch) -> Bytes {
    ProducerBatch::from_record_batch(rows)
        .assured("a batch of its own schema encodes")
        .arrow_ipc()
        .clone()
}

async fn decode(
    executor: &Executor,
    schema: &GeneratedSchema,
    body: Bytes,
    limits: ClientBatchLimits,
) -> Result<RecordBatch, error_stack::Report<ClientBatchError>> {
    let decoded = schema
        .compiled
        .decode_client_batch(executor, body, limits)
        .await?;
    Ok(decoded.batch().clone())
}

/// A batch the client library writes for a producer decodes on the node to exactly that batch: the
/// schema the vocabulary maps the ingestor's fields to, every row, and every value and null, floats
/// by their bits, through a view whose columns start inside a larger batch. Limits equal to the
/// batch's own row count and size admit it, and one row or one byte less refuses it before its
/// columns are decoded.
#[test]
fn bolero_producer_batches_decode_to_the_submitted_rows() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(128)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let executor = Executor::default();
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let schema = GeneratedDomain::Arrow.schema(&mut arbitrary);
            let rows = GeneratedDomain::Arrow.batch(&mut arbitrary, &schema);
            assert_eq!(
                SchemaField::arrow_schema(&schema.model.fields),
                *schema.compiled.arrow_schema(),
                "the client library and the node map the ingestor's fields to one Arrow schema"
            );
            let body = submitted(&rows);
            let size = u64::try_from(body.len()).assured("a bounded stream length");
            let row_count = rows.num_rows();
            runtime.block_on(async {
                let exact = ClientBatchLimits {
                    max_bytes: NonZeroU64::new(size).assured("a stream has bytes"),
                    max_rows: NonZeroUsize::new(row_count.max(1)).assured("at least one row"),
                };
                let decoded = decode(&executor, &schema, body.clone(), exact)
                    .await
                    .assured("a batch within its exact limits decodes");
                assert_same_batch(&decoded, &rows);

                let smaller_bytes = size.checked_sub(1).assured("a stream has bytes");
                if let Some(max_bytes) = NonZeroU64::new(smaller_bytes) {
                    let refused = decode(
                        &executor,
                        &schema,
                        body.clone(),
                        ClientBatchLimits { max_bytes, ..exact },
                    )
                    .await
                    .expect_err("one byte over the limit is refused");
                    assert!(
                        matches!(
                            refused.current_context(),
                            ClientBatchError::TooLarge { size: refused_size, limit }
                                if *refused_size == size && *limit == smaller_bytes
                        ),
                        "the refusal names the size and the limit: {refused:?}"
                    );
                }
                if let Some(max_rows) = row_count.checked_sub(1).and_then(NonZeroUsize::new) {
                    let refused = decode(
                        &executor,
                        &schema,
                        body.clone(),
                        ClientBatchLimits { max_rows, ..exact },
                    )
                    .await
                    .expect_err("one row over the limit is refused");
                    assert!(
                        matches!(
                            refused.current_context(),
                            ClientBatchError::TooManyRows { rows: refused_rows, limit }
                                if *refused_rows == u64::try_from(row_count).assured("a bounded row count")
                                    && *limit == max_rows.get()
                        ),
                        "the refusal names the rows and the limit: {refused:?}"
                    );
                }
            });
            assert_eq!(
                executor.snapshot().relay_memory.reserved_bytes,
                0,
                "no decode charge outlives its batch"
            );
        });
}

/// A damaged producer stream, or arbitrary bytes, is refused with the defect it has, which a
/// producer reads as a property of its batch, never as a busy node; or it decodes to a batch of the
/// ingestor's exact schema within the row limit, which the client library writes and the node
/// decodes back to itself.
#[test]
fn bolero_damaged_producer_batches_fail_with_their_defect_or_decode_within_their_limits() {
    let runtime = property_runtime();
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let executor = Executor::default();
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let schema = GeneratedDomain::Arrow.schema(&mut arbitrary);
            let rows = GeneratedDomain::Arrow.batch(&mut arbitrary, &schema);
            let damage = arbitrary.entropy().pick(Damage::ALL);
            let damaged = damage.apply(arbitrary.entropy(), submitted(&rows).to_vec());
            runtime.block_on(async {
                let outcome =
                    decode(&executor, &schema, Bytes::from(damaged), generous_limits()).await;
                let decoded = match outcome {
                    Ok(decoded) => decoded,
                    Err(refused) => {
                        assert!(
                            refused.current_context().defect().is_some(),
                            "a damaged batch is refused for what it is: {refused:?}"
                        );
                        return;
                    }
                };
                assert_eq!(decoded.schema(), schema.compiled.arrow_schema());
                assert!(decoded.num_rows() <= GENEROUS_ROWS);
                let again = decode(&executor, &schema, submitted(&decoded), generous_limits())
                    .await
                    .assured("an accepted batch is one the client library writes again");
                assert_same_batch(&again, &decoded);
            });
        });
}

/// A producer stream declaring what Arrow's reader would panic on is refused by the framing scan,
/// before the reader reads it, with the defect of the batch the scan found: a field type Nervix
/// does not carry is a schema that differs, a record batch at odds with its schema is invalid data,
/// and a stream that does not frame its messages is malformed.
#[rstest]
#[case::buffer_past_body(StreamDefect::BufferPastBody, ClientBatchDefect::Malformed)]
#[case::integer_of_seven_bits(StreamDefect::IntegerOfSevenBits, ClientBatchDefect::SchemaMismatch)]
#[case::list_without_child(StreamDefect::ListWithoutChild, ClientBatchDefect::SchemaMismatch)]
#[case::dictionary_encoded(StreamDefect::DictionaryEncoded, ClientBatchDefect::SchemaMismatch)]
#[case::schema_without_fields(StreamDefect::SchemaWithoutFields, ClientBatchDefect::Malformed)]
#[case::validity_shorter_than_rows(
    StreamDefect::ValidityShorterThanRows,
    ClientBatchDefect::InvalidData
)]
#[case::offsets_cut_inside_an_offset(
    StreamDefect::OffsetsCutInsideAnOffset,
    ClientBatchDefect::InvalidData
)]
#[case::variadic_buffer_counts(StreamDefect::VariadicBufferCounts, ClientBatchDefect::InvalidData)]
#[case::fixed_size_list_too_long_to_count(
    StreamDefect::FixedSizeListTooLongToCount,
    ClientBatchDefect::InvalidData
)]
fn a_stream_arrows_reader_would_panic_on_is_refused_with_its_defect(
    #[case] defect: StreamDefect,
    #[case] reported: ClientBatchDefect,
) {
    let schema = receiver_schema();
    let executor = Executor::default();
    let refused = property_runtime()
        .block_on(decode(
            &executor,
            &schema,
            Bytes::from(defect.stream()),
            generous_limits(),
        ))
        .expect_err("the stream declares what no valid stream does");
    assert_eq!(
        refused.current_context().defect(),
        Some(reported),
        "the stream is refused for what it declares: {refused:?}"
    );
    assert_eq!(
        refused.downcast_ref::<IpcStreamError>(),
        Some(&defect.refusal()),
        "the scan refuses the stream for what it declares"
    );
}
