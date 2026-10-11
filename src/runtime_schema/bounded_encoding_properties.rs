//! Generated encodings held to `MAX SIZE`, compared with the exact bytes the codec writes.
//!
//! Layer: test harness.
//!
//! - **Owns.** The property that a bounded row or batch encoding is the complete unbounded
//!   encoding exactly when that encoding fits the limit, and otherwise an oversize outcome naming
//!   the limit.
//! - **Depends on.** The generated schemaful codecs and batches with their formats' container
//!   framing, and the codec's row encoder, batch members and batch containers.
//! - **Must not know.** Emitters, how a candidate that did not fit is divided, or sinks.

use std::num::NonZeroU64;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain, Entropy};
use nervix_models::{ByteSizeUnit, PayloadSizeLimit};

use super::{
    BatchMember, BatchMemberEncoding, BoundedBatchEncoding, BoundedRowEncoding, CompiledCodec,
    codec_properties::{CodecCase, SchemafulFormat},
    generated_batches::BatchRows,
};

/// The bytes one case reads its codec, rows and limits from.
const CASE_BYTES: usize = 4096;

/// A limit far above every generated encoding, under which every encoding completes.
const UNBOUNDED: u64 = 1 << 40;

/// Container sizes past the rows a generated batch holds, where a count's encoding grows: CBOR's
/// count leaves its inline form at 24 members and needs two bytes at 256, and an Avro count, a
/// zig-zag long, needs a second byte at 64.
const CONTAINER_BOUNDARIES: [usize; 3] = [24, 64, 256];

fn byte_limit(bytes: u64) -> PayloadSizeLimit {
    let bytes = NonZeroU64::new(bytes).assured("every limit a case draws is at least one byte");
    PayloadSizeLimit::new(bytes, ByteSizeUnit::B).assured("a byte count fits u64")
}

/// The limits an encoding of `length` bytes is held to, each at least one byte: one byte short of
/// it, exactly it, one byte over it, and one drawn anywhere up to twice it.
fn limits_around(entropy: &mut Entropy<'_>, length: usize) -> Vec<u64> {
    let length = u64::try_from(length).assured("an encoding's length fits u64");
    let mut limits = vec![length + 1, entropy.between(1..=2 * length + 1)];
    if length > 0 {
        limits.push(length);
    }
    if length > 1 {
        limits.push(length - 1);
    }
    limits
}

/// The container `codec` writes for `members`, under `limit`.
fn container(
    codec: &CompiledCodec,
    members: &[&BatchMember],
    limit: PayloadSizeLimit,
) -> BoundedBatchEncoding {
    codec
        .encode_batch_within(members, limit)
        .assured("a schemaful container of encoded members has no evaluation to fail")
}

/// Every row encoding, every batch member and every batch container a schemaful codec writes
/// under `MAX SIZE` is the complete unbounded encoding, byte for byte, exactly when that encoding
/// is at most the limit; an encoding one byte over it is oversize and names the limit. A batch
/// container frames exactly the members' own encodings, in order, at every count where a format's
/// count encoding grows.
#[test]
fn bolero_bounded_encodings_are_the_exact_bytes_within_max_size() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            // The format and the number of rows are read before the schema and its values, which
            // an ordinary run's few bytes run out while generating.
            let format = arbitrary.entropy().pick(SchemafulFormat::ALL);
            let shape = BatchRows::draw(arbitrary.entropy());
            let schema = format.domain().schema(&mut arbitrary);
            let case = CodecCase::with_schema(&mut arbitrary, format, schema);
            let rows = format
                .domain()
                .batch_of(&mut arbitrary, &case.schema, shape);
            let batch = case.schema.runtime_batch(rows);
            let codec = &case.codec;
            let encoder = codec
                .batch_encoder(&batch)
                .assured("a batch of the codec's schema opens an encoder");
            let row_count = batch.batch().num_rows();

            let mut encodings = Vec::with_capacity(row_count);
            let mut members = Vec::with_capacity(row_count);
            for row in 0..row_count {
                let mut unbounded = Vec::new();
                encoder
                    .encode_row_into(row, &mut unbounded)
                    .assured("every value of the codec's domain encodes");
                for limit in limits_around(arbitrary.entropy(), unbounded.len()) {
                    let fits = u64::try_from(unbounded.len()).assured("fits u64") <= limit;
                    let bounded = encoder
                        .encode_row_within(row, byte_limit(limit))
                        .assured("every value of the codec's domain encodes");
                    match bounded {
                        BoundedRowEncoding::Encoded(bytes) => {
                            assert!(fits, "row {row} completed past {limit} bytes");
                            assert_eq!(bytes, unbounded, "row {row} under {limit} bytes");
                        }
                        BoundedRowEncoding::Oversize(exceeded) => {
                            assert!(!fits, "row {row} of {} bytes refused", unbounded.len());
                            assert_eq!(exceeded.limit, byte_limit(limit));
                        }
                    }
                    let member = encoder
                        .batch_member(row, byte_limit(limit))
                        .assured("every value of the codec's domain is a member");
                    match member {
                        BatchMemberEncoding::Member(_) => {
                            assert!(fits, "member {row} admitted past {limit} bytes");
                        }
                        BatchMemberEncoding::Oversize(exceeded) => {
                            assert!(!fits, "member {row} of {} bytes refused", unbounded.len());
                            assert_eq!(exceeded.limit, byte_limit(limit));
                        }
                    }
                }
                let BatchMemberEncoding::Member(member) = encoder
                    .batch_member(row, byte_limit(UNBOUNDED))
                    .assured("every value of the codec's domain is a member")
                else {
                    panic!("member {row} is oversize under an unbounded limit");
                };
                encodings.push(unbounded);
                members.push(member);
            }
            if members.is_empty() {
                return;
            }

            // Every run of consecutive members, from each start.
            let start = arbitrary.entropy().count(members.len() - 1);
            for end in start + 1..=members.len() {
                let run = members[start..end].iter().collect::<Vec<_>>();
                let expected = case.format.framed(&encodings[start..end]);
                let BoundedBatchEncoding::Encoded(unbounded) =
                    container(codec, &run, byte_limit(UNBOUNDED))
                else {
                    panic!("members {start}..{end} are oversize under an unbounded limit");
                };
                assert_eq!(unbounded, expected, "members {start}..{end}");
                for limit in limits_around(arbitrary.entropy(), expected.len()) {
                    let fits = u64::try_from(expected.len()).assured("fits u64") <= limit;
                    match container(codec, &run, byte_limit(limit)) {
                        BoundedBatchEncoding::Encoded(bytes) => {
                            assert!(fits, "members {start}..{end} completed past {limit} bytes");
                            assert_eq!(bytes, expected, "members {start}..{end}");
                        }
                        BoundedBatchEncoding::Oversize(exceeded) => {
                            assert!(!fits, "members {start}..{end} refused under {limit} bytes");
                            assert_eq!(exceeded.limit, byte_limit(limit));
                        }
                    }
                }
            }

            // Containers of the generated members repeated past the counts a batch reaches.
            for count in CONTAINER_BOUNDARIES {
                let mut run = Vec::with_capacity(count);
                let mut run_encodings = Vec::with_capacity(count);
                for index in 0..count {
                    let member = index % members.len();
                    run.push(&members[member]);
                    run_encodings.push(encodings[member].clone());
                }
                let BoundedBatchEncoding::Encoded(bytes) =
                    container(codec, &run, byte_limit(UNBOUNDED))
                else {
                    panic!("{count} members are oversize under an unbounded limit");
                };
                assert_eq!(bytes, case.format.framed(&run_encodings), "{count} members");
            }
        });
}
