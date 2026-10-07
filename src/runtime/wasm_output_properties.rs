//! Generated column pools through the decoding of the Arrow IPC stream a WASM guest writes beside
//! its routed outputs.
//!
//! Layer: test harness.
//!
//! - **Owns.** The round-trip and damaged-stream properties of a guest's generated pool.
//! - **Depends on.** The generated batches and their logical oracle, and the output validator that
//!   decodes the pool.
//! - **Must not know.** Guests, branches, acknowledgements or the routes that reference the pool's
//!   columns.

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{Field, Schema as ArrowSchema};
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_primitives::sync::StdArc;
use rstest::rstest;

use super::*;
use crate::runtime_schema::{
    IpcStreamError,
    crafted_streams::{END_OF_STREAM, StreamDefect},
    generated_batches::{Damage, GeneratedDomain, assert_rewritten_batch, assert_same_batch},
};

/// The bytes one round-trip case reads its pool and its ending from.
const ROUND_TRIP_BYTES: usize = 2048;

/// The bytes one damaged-stream case reads its pool, its ending and its damage from.
const DAMAGED_BYTES: usize = 2048;

/// How a guest ends the stream of its pool.
#[derive(Debug, Clone, Copy)]
enum PoolEnding {
    /// The writer writes the end-of-stream marker.
    Marker,
    /// The writer closes the stream after its record batch, as the Arrow format allows.
    Closed,
}

impl PoolEnding {
    const ALL: [Self; 2] = [Self::Marker, Self::Closed];
}

/// One generated column pool: a batch whose fields are unnamed, as a guest writes them.
struct GeneratedPool {
    batch: RecordBatch,
}

impl GeneratedPool {
    fn new(arbitrary: &mut Arbitrary<'_>) -> Self {
        let schema = GeneratedDomain::Arrow.schema(arbitrary);
        let rows = GeneratedDomain::Arrow.batch(arbitrary, &schema);
        let named = rows.schema();
        let unnamed: Vec<Field> = named
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_name(""))
            .collect();
        let pool_schema = StdArc::new(ArrowSchema::new_with_metadata(
            unnamed,
            named.metadata().clone(),
        ));
        let batch = RecordBatch::try_new(pool_schema, rows.columns().to_vec())
            .assured("a field's name never decides which columns it admits");
        Self { batch }
    }

    /// `batch` written as one Arrow IPC stream that ends as `ending` says.
    fn stream(batch: &RecordBatch, ending: PoolEnding) -> Vec<u8> {
        let mut stream = Vec::new();
        let mut writer =
            StreamWriter::try_new(&mut stream, &batch.schema()).assured("the schema writes");
        writer.write(batch).assured("a generated batch writes");
        writer.finish().assured("the stream ends");
        drop(writer);
        if let PoolEnding::Closed = ending {
            let closed = stream
                .len()
                .checked_sub(END_OF_STREAM.len())
                .assured("a finished stream ends with its eight-byte marker");
            stream.truncate(closed);
        }
        stream
    }
}

/// Decodes `stream` as the generated pool of one output group. Decoding the pool reads none of the
/// validator's acknowledgements, schemas or routes, so the validator holds no acknowledgement, no
/// route and a placeholder input schema.
fn decode_pool(stream: &[u8]) -> error_stack::Result<Option<RecordBatch>, WasmOutputError> {
    let ack_map = WasmAckMap::default();
    let input_schema = test_schema(&[("value", ParseAsType::I32)]);
    let output_routes = RelayProcessorOutputsNode { routes: Vec::new() };
    let validator = WasmOutputValidator {
        ack_map: &ack_map,
        input_schema: &input_schema,
        output_schemas: &[],
        output_routes: &output_routes,
    };
    validator.decode_generated_batch(stream)
}

/// A pool a guest writes, ending its stream either way the Arrow format allows, decodes to every
/// column it wrote: the unnamed fields with their types, nullability and metadata, every row, and
/// every value and null with floats by their bits.
#[test]
fn bolero_generated_pools_decode_to_the_columns_the_guest_wrote() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(ROUND_TRIP_BYTES)
        .for_each(|input| {
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            // What shapes the case is read before the case: an ordinary run's few bytes run out
            // while a pool is generated, and a choice read after that takes its first option.
            let ending = arbitrary.entropy().pick(PoolEnding::ALL);
            let pool = GeneratedPool::new(&mut arbitrary);
            let stream = GeneratedPool::stream(&pool.batch, ending);
            let decoded = decode_pool(&stream)
                .assured("a pool its guest wrote whole decodes")
                .assured("a stream of one or more columns is a present pool");
            assert_same_batch(&decoded, &pool.batch);
        });
}

/// A damaged pool fails with a typed defect of the guest's output, or decodes to a pool the guest
/// could have written: one record batch of unnamed columns that Arrow's writer writes and the node
/// decodes back to the same rows. Only the empty byte string is the empty pool.
#[test]
fn bolero_damaged_generated_pools_fail_typed_or_decode_to_a_pool() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(DAMAGED_BYTES)
        .for_each(|input| {
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let ending = arbitrary.entropy().pick(PoolEnding::ALL);
            let damage = Damage::draw(arbitrary.entropy());
            let pool = GeneratedPool::new(&mut arbitrary);
            let damaged = damage.apply(GeneratedPool::stream(&pool.batch, ending));
            let decoded = match decode_pool(&damaged) {
                Ok(Some(decoded)) => decoded,
                Ok(None) => {
                    assert!(damaged.is_empty(), "only the empty byte string is no pool");
                    return;
                }
                Err(failure) => {
                    assert!(
                        matches!(
                            failure.current_context(),
                            WasmOutputError::InvalidGeneratedArrowIpc { .. }
                                | WasmOutputError::GeneratedRecordBatchCount { .. }
                        ),
                        "a damaged pool fails with the defect of its own bytes: {failure:?}"
                    );
                    return;
                }
            };
            assert!(decoded.num_columns() > 0, "an accepted pool has a column");
            for field in decoded.schema().fields() {
                assert!(
                    field.name().is_empty(),
                    "an accepted pool's fields are unnamed"
                );
            }
            let again = decode_pool(&GeneratedPool::stream(&decoded, PoolEnding::Marker))
                .assured("an accepted pool writes as a valid stream")
                .assured("an accepted pool has a column");
            assert_rewritten_batch(&again, &decoded);
        });
}

/// A guest's stream declaring what Arrow's reader would panic on is refused by the framing scan as
/// a defect of the generated Arrow IPC however the stream ends, before the reader reads it on the
/// processor's task.
#[rstest]
fn a_generated_pool_arrows_reader_would_panic_on_is_refused_however_it_ends(
    #[values(
        StreamDefect::BufferPastBody,
        StreamDefect::IntegerOfSevenBits,
        StreamDefect::ListWithoutChild,
        StreamDefect::DictionaryEncoded,
        StreamDefect::SchemaWithoutFields,
        StreamDefect::ValidityShorterThanRows,
        StreamDefect::OffsetsCutInsideAnOffset,
        StreamDefect::VariadicBufferCounts,
        StreamDefect::FixedSizeListTooLongToCount,
        StreamDefect::BodyLongerThanStream
    )]
    defect: StreamDefect,
) {
    let marked = defect.stream();
    let closed_length = marked
        .len()
        .checked_sub(END_OF_STREAM.len())
        .assured("a crafted stream ends with its end-of-stream marker");
    let closed = marked[..closed_length].to_vec();
    for stream in [marked, closed] {
        let refused =
            decode_pool(&stream).expect_err("the stream declares what no valid stream does");
        let reported = match defect.refusal() {
            IpcStreamError::UnsupportedField { field, kind } => {
                WasmGeneratedIpcDefect::UnsupportedField {
                    field_index: field,
                    kind,
                }
            }
            _ => WasmGeneratedIpcDefect::Unreadable,
        };
        assert!(
            matches!(
                refused.current_context(),
                WasmOutputError::InvalidGeneratedArrowIpc { defect } if *defect == reported
            ),
            "the generated stream is refused for what it declares: {refused:?}"
        );
        assert_eq!(
            refused.downcast_ref::<IpcStreamError>(),
            Some(&defect.refusal()),
            "the scan refuses the stream for what it declares"
        );
    }
}
