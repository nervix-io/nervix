//! Generated client emitter deliveries through the sink's encoder and the client library's decoder.
//!
//! Layer: test harness.
//!
//! - **Owns.** The property that the rows a client emitter sink writes for one delivery are the
//!   rows an application consumer decodes, under exactly the fields the emitter declares, and that
//!   the decoder refuses the delivery under any other fields or row count.
//! - **Depends on.** The generated batches and their logical oracle, the sink's delivery encoder
//!   and the client library's delivery decoder.
//! - **Must not know.** Endpoints, consumers, sessions, the frames a delivery travels in between
//!   nodes, or what settles a delivery.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_client_core::{ClientError, EmitterDelivery, RequestKind};
use nervix_models::SchemaField;

use super::ClientConsumerConnector;
use crate::runtime_schema::generated_batches::{
    GeneratedDomain, GeneratedSchema, LogicalValue, assert_same_batch,
};

/// The bytes one case reads its selection, schema and rows from.
const CASE_BYTES: usize = 2048;

/// The rows of `source` a delivery carries: those `mask` names by their bit, in the order they
/// lie in the batch. A sink packs a delivery from the pending rows of one branch, which need not
/// be adjacent, and never packs none, so a batch with rows always gives at least its first one.
fn delivered_rows(source: &RecordBatch, mask: u8) -> Vec<usize> {
    let mut rows = Vec::new();
    for row in 0..source.num_rows() {
        // A generated view holds fewer rows than the mask has bits.
        if (mask >> row) & 1 == 1 {
            rows.push(row);
        }
    }
    if rows.is_empty() && source.num_rows() > 0 {
        rows.push(0);
    }
    rows
}

/// The stream the sink writes for `rows` of `source`.
fn delivery(schema: &GeneratedSchema, source: &RecordBatch, rows: &[usize]) -> Bytes {
    ClientConsumerConnector::encode_rows(&schema.compiled, source, rows)
        .assured("rows of the emitter's own output schema encode")
}

fn assert_refused(outcome: error_stack::Result<RecordBatch, ClientError>, what: &str) {
    let refused = match outcome {
        Ok(decoded) => panic!("{what} decoded to {} rows", decoded.num_rows()),
        Err(refused) => refused,
    };
    assert!(
        matches!(
            refused.current_context(),
            ClientError::UnexpectedReply {
                request: RequestKind::ReadEmitterBatch
            }
        ),
        "{what} is refused as a batch the consumer did not ask for: {refused:?}"
    );
}

/// The rows a client emitter sink writes for a delivery are the rows an application consumer
/// decodes from it: the Arrow schema of exactly the fields the emitter declares, every selected
/// row in its order, and every value and null, floats by their bits, taken from a view whose
/// columns start inside a larger batch. The consumer's decoder refuses the same bytes under one row
/// more or one row fewer, and under fields that differ in a field's optionality, their order or
/// their number.
#[test]
fn bolero_client_emitter_deliveries_decode_to_the_delivered_rows() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            // Which rows are delivered is read before the batch: an ordinary run's few bytes run
            // out while a batch is generated, and a mask read after that would name no row.
            let mask = arbitrary.entropy().byte();
            let schema = GeneratedDomain::Arrow.schema(&mut arbitrary);
            let source = GeneratedDomain::Arrow.batch(&mut arbitrary, &schema);
            let rows = delivered_rows(&source, mask);
            let members = u32::try_from(rows.len()).assured("a generated view holds few rows");

            let declared = schema.compiled.declared_fields();
            assert_eq!(
                declared, schema.model.fields,
                "an emitter declares its output schema field for field"
            );
            let body = delivery(&schema, &source, &rows);
            let decoded = EmitterDelivery::decode_record_batch(&declared, &body, members)
                .assured("a delivery the sink wrote decodes under the fields its emitter declares");

            assert_eq!(
                decoded.schema().as_ref(),
                &SchemaField::arrow_schema(&declared),
                "a delivery carries the Arrow schema of the declared fields"
            );
            let source_rows = LogicalValue::rows(&source);
            let mut expected = Vec::with_capacity(rows.len());
            for row in &rows {
                expected.push(source_rows[*row].clone());
            }
            assert_eq!(
                LogicalValue::rows(&decoded),
                expected,
                "every delivered row, value and null is preserved in order"
            );

            let one_more = members
                .checked_add(1)
                .assured("a generated view holds few rows");
            assert_refused(
                EmitterDelivery::decode_record_batch(&declared, &body, one_more),
                "a delivery read as one row more",
            );
            if let Some(one_fewer) = members.checked_sub(1) {
                assert_refused(
                    EmitterDelivery::decode_record_batch(&declared, &body, one_fewer),
                    "a delivery read as one row fewer",
                );
            }
            let mut other_optionality = declared.clone();
            other_optionality[0].optional = !other_optionality[0].optional;
            assert_refused(
                EmitterDelivery::decode_record_batch(&other_optionality, &body, members),
                "a delivery read under a field of another optionality",
            );
            let mut one_field_fewer = declared.clone();
            one_field_fewer.pop();
            assert_refused(
                EmitterDelivery::decode_record_batch(&one_field_fewer, &body, members),
                "a delivery read under one field fewer",
            );
            let mut reordered = declared.clone();
            reordered.reverse();
            if SchemaField::arrow_schema(&reordered) != SchemaField::arrow_schema(&declared) {
                assert_refused(
                    EmitterDelivery::decode_record_batch(&reordered, &body, members),
                    "a delivery read under its fields in another order",
                );
            }
        });
}

/// The bytes of one deterministic case: every byte value in turn, twice over, each changed by
/// `seed`, so each seed builds another schema and batch.
fn seeded_bytes(seed: u8) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(512);
    for index in 0..512_u16 {
        let low = index.to_le_bytes()[0];
        bytes.push(low ^ seed);
    }
    bytes
}

/// A delivery of every row of a batch is the batch itself, so the sink's encoder agrees with the
/// oracle the other Arrow properties compare whole batches with.
#[test]
fn a_delivery_of_every_row_decodes_to_the_whole_batch() {
    for seed in 0..=u8::MAX {
        let bytes = seeded_bytes(seed);
        let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
        let schema = GeneratedDomain::Arrow.schema(&mut arbitrary);
        let source = GeneratedDomain::Arrow.batch(&mut arbitrary, &schema);
        let rows: Vec<usize> = (0..source.num_rows()).collect();
        let members = u32::try_from(rows.len()).assured("a generated view holds few rows");
        let body = delivery(&schema, &source, &rows);
        let decoded = EmitterDelivery::decode_record_batch(
            &schema.compiled.declared_fields(),
            &body,
            members,
        )
        .assured("a delivery the sink wrote decodes under the fields its emitter declares");
        assert_same_batch(&decoded, &source);
    }
}

/// A stream of `schema` that holds `source` as many times as `batches` counts.
fn stream_of(schema: &GeneratedSchema, source: &RecordBatch, batches: usize) -> Bytes {
    let mut writer = StreamWriter::try_new(Vec::new(), &schema.compiled.arrow_schema())
        .assured("a stream of a generated schema opens");
    for _ in 0..batches {
        writer
            .write(source)
            .assured("a batch of the stream's own schema is written");
    }
    writer.finish().assured("an in-memory stream finishes");
    let stream = writer
        .into_inner()
        .assured("a finished in-memory stream gives its bytes back");
    Bytes::from(stream)
}

/// A delivery is one record batch of its declared schema. The decoder accepts the stream that holds
/// exactly one, and refuses a stream that holds none or two, a stream cut short and no bytes at
/// all, each as a batch the consumer did not ask for.
#[test]
fn a_delivery_that_is_not_one_batch_is_refused() {
    let bytes = seeded_bytes(7);
    let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
    let schema = GeneratedDomain::Arrow.schema(&mut arbitrary);
    let source = GeneratedDomain::Arrow.batch(&mut arbitrary, &schema);
    let declared = schema.compiled.declared_fields();
    let members = u32::try_from(source.num_rows()).assured("a generated view holds few rows");

    let one = stream_of(&schema, &source, 1);
    let decoded = EmitterDelivery::decode_record_batch(&declared, &one, members)
        .assured("a stream of one batch of the declared schema is a delivery");
    assert_same_batch(&decoded, &source);

    assert_refused(
        EmitterDelivery::decode_record_batch(&declared, &stream_of(&schema, &source, 0), 0),
        "a stream that holds no batch",
    );
    assert_refused(
        EmitterDelivery::decode_record_batch(&declared, &stream_of(&schema, &source, 2), members),
        "a stream that holds two batches",
    );
    let cut_short = one.slice(..one.len() / 2);
    assert_refused(
        EmitterDelivery::decode_record_batch(&declared, &cut_short, members),
        "a stream cut short",
    );
    assert_refused(
        EmitterDelivery::decode_record_batch(&declared, &Bytes::new(), 0),
        "no bytes at all",
    );
}
