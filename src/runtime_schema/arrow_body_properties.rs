//! The Arrow IPC bodies a batch travels and is sealed as, read back whole and read back damaged.
//!
//! Layer: test harness.
//! - **Owns.** Generated batches whose columns take every buffer layout, their relay bodies and
//!   sealed snapshot sections, and the same bodies damaged once.
//! - **Depends on.** The production body encoder and decoder, and the vocabulary generators.
//! - **Must not know.** Relays, snapshots, or what a decoded batch is used for.

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Int64Array, RecordBatch, StringArray,
    builder::{Int64Builder, ListBuilder},
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_execution::{ChargedBytes, Executor, MemoryClass};
use nervix_primitives::sync::StdArc;

use super::{ArrowBodyError, RuntimeRecordBatch};

/// The most rows, list elements and text or byte lengths one generated batch draws.
const GENERATED_BOUND: usize = 4;

/// Which path a generated body travels, which decides its budget and whether its reader already
/// knows its schema.
#[derive(Debug, Clone, Copy)]
enum Carriage {
    /// A relay batch, read by a receiver that learns the schema from the body.
    Relay,
    /// One Arrow section of a sealed snapshot, read against the schema installed for it.
    SnapshotSection,
}

impl Carriage {
    fn generated(arbitrary: &mut Arbitrary<'_>) -> Self {
        if arbitrary.entropy().flag() {
            Self::Relay
        } else {
            Self::SnapshotSection
        }
    }

    const fn memory_class(self) -> MemoryClass {
        match self {
            Self::Relay => MemoryClass::Relay,
            Self::SnapshotSection => MemoryClass::Bulk,
        }
    }

    async fn encode(
        self,
        batch: &RuntimeRecordBatch,
        executor: &Executor,
    ) -> Result<ChargedBytes, Report<ArrowBodyError>> {
        match self {
            Self::Relay => batch.encode_arrow_ipc(executor).await,
            Self::SnapshotSection => batch.encode_arrow_snapshot_section(executor).await,
        }
    }

    /// Reads `bytes` as a body of `schema` on this path.
    async fn decode(
        self,
        schema: &StdArc<ArrowSchema>,
        executor: &Executor,
        bytes: Vec<u8>,
    ) -> Result<RuntimeRecordBatch, Report<ArrowBodyError>> {
        let body = executor
            .charge_owned(self.memory_class(), bytes)
            .await
            .assured("the default budget holds one small generated body");
        match self {
            Self::Relay => RuntimeRecordBatch::decode_arrow_ipc(executor, body).await,
            Self::SnapshotSection => {
                RuntimeRecordBatch::decode_arrow_snapshot_section(
                    executor,
                    StdArc::clone(schema),
                    body,
                )
                .await
            }
        }
    }
}

/// A short lowercase text.
fn generated_text(arbitrary: &mut Arbitrary<'_>) -> String {
    let length = arbitrary.entropy().count(GENERATED_BOUND);
    let mut text = String::with_capacity(length);
    for _ in 0..length {
        let letter = b'a'
            .checked_add(arbitrary.entropy().byte() % 26)
            .verified("a letter index below 26 stays within the lowercase letters");
        text.push(char::from(letter));
    }
    text
}

/// A batch of up to four rows whose columns take every buffer layout a body carries: fixed-width
/// values, a validity bitmap, variable-length offsets, bit-packed values and a nested list.
fn generated_batch(arbitrary: &mut Arbitrary<'_>) -> RuntimeRecordBatch {
    let rows = arbitrary.entropy().count(GENERATED_BOUND);
    let mut wide = Vec::with_capacity(rows);
    let mut text = Vec::with_capacity(rows);
    let mut flags = Vec::with_capacity(rows);
    let mut bytes = Vec::with_capacity(rows);
    let mut lists = ListBuilder::new(Int64Builder::new());
    for _ in 0..rows {
        wide.push(arbitrary.entropy().any_i64());
        if arbitrary.entropy().flag() {
            text.push(None);
        } else {
            text.push(Some(generated_text(arbitrary)));
        }
        if arbitrary.entropy().flag() {
            flags.push(None);
        } else {
            flags.push(Some(arbitrary.entropy().flag()));
        }
        let length = arbitrary.entropy().count(GENERATED_BOUND);
        let mut value = Vec::with_capacity(length);
        for _ in 0..length {
            value.push(arbitrary.entropy().byte());
        }
        bytes.push(value);
        if arbitrary.entropy().flag() {
            lists.append_null();
        } else {
            let elements = arbitrary.entropy().count(GENERATED_BOUND);
            for _ in 0..elements {
                if arbitrary.entropy().flag() {
                    lists.values().append_null();
                } else {
                    lists.values().append_value(arbitrary.entropy().any_i64());
                }
            }
            lists.append(true);
        }
    }
    let schema = StdArc::new(ArrowSchema::new(vec![
        Field::new("wide", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("bytes", DataType::Binary, false),
        Field::new(
            "list",
            DataType::List(StdArc::new(Field::new_list_field(DataType::Int64, true))),
            true,
        ),
    ]));
    let byte_values = bytes.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let columns: Vec<ArrayRef> = vec![
        StdArc::new(Int64Array::from(wide)),
        StdArc::new(StringArray::from(text)),
        StdArc::new(BooleanArray::from(flags)),
        StdArc::new(BinaryArray::from(byte_values)),
        StdArc::new(lists.finish()),
    ];
    let batch = RecordBatch::try_new(StdArc::clone(&schema), columns)
        .assured("every generated column has its field's type and the batch's row count");
    RuntimeRecordBatch::from_record_batch(schema, batch)
        .assured("the batch has the schema it was built with")
}

/// Writes `value` over the bytes of `body` from `position`, as far as the body reaches.
fn overwrite(body: &mut [u8], position: usize, value: &[u8]) {
    for (offset, byte) in value.iter().enumerate() {
        let index = position
            .checked_add(offset)
            .verified("a position at most a test body's length is far below usize::MAX");
        if let Some(slot) = body.get_mut(index) {
            *slot = *byte;
        }
    }
}

/// `body` damaged once at a generated position: a flipped bit, a cut, appended bytes, or a four-
/// or eight-byte little-endian length overwritten with a boundary value.
fn damaged(arbitrary: &mut Arbitrary<'_>, mut body: Vec<u8>) -> Vec<u8> {
    let length = u64::try_from(body.len()).assured("a body length fits in u64");
    let position = arbitrary.entropy().up_to(length);
    let position = usize::try_from(position).verified("a position at most the body length");
    match arbitrary.entropy().byte() % 5 {
        0 => {
            let bit = arbitrary.entropy().byte() % 8;
            if let Some(byte) = body.get_mut(position) {
                *byte ^= 1_u8 << bit;
            }
        }
        1 => body.truncate(position),
        2 => {
            let added = arbitrary.entropy().count(16);
            for _ in 0..added {
                body.push(arbitrary.entropy().byte());
            }
        }
        3 => {
            let whole = u32::try_from(body.len()).assured("a generated body is far below 4 GiB");
            let value = arbitrary.entropy().pick([0, 1, whole, u32::MAX]);
            overwrite(&mut body, position, &value.to_le_bytes());
        }
        _ => {
            let whole = i64::try_from(body.len()).assured("a generated body is far below 8 EiB");
            let value = arbitrary
                .entropy()
                .pick([0, 1, whole, 1 << 48, i64::MAX, -1]);
            overwrite(&mut body, position, &value.to_le_bytes());
        }
    }
    body
}

/// Asserts that `bytes` either are refused for what they hold or decode to a batch like any
/// other, which encodes and decodes back to itself.
async fn assert_refused_typed_or_decodes_canonically(
    carriage: Carriage,
    schema: &StdArc<ArrowSchema>,
    executor: &Executor,
    bytes: Vec<u8>,
) {
    let decoded = match carriage.decode(schema, executor, bytes).await {
        Ok(decoded) => decoded,
        Err(report) => {
            assert_refused_for_its_content(&report);
            return;
        }
    };
    let body = carriage
        .encode(&decoded, executor)
        .await
        .assured("a decoded batch encodes");
    let decoded_schema = decoded.batch().schema();
    let reopened = carriage
        .decode(&decoded_schema, executor, body.as_ref().to_vec())
        .await
        .assured("an encoded body decodes");
    assert_eq!(reopened.batch(), decoded.batch());
}

/// Asserts that `report` refuses a body for what it holds: what its framing or its limits get
/// wrong, or what the reader rejects or panics on. It is never refused as work the node could
/// not admit or execute.
fn assert_refused_for_its_content(report: &Report<ArrowBodyError>) {
    assert!(
        matches!(
            report.current_context(),
            ArrowBodyError::Decode { .. }
                | ArrowBodyError::Framing { .. }
                | ArrowBodyError::NoSection
                | ArrowBodyError::TooManySections { .. }
                | ArrowBodyError::BodyTooLarge { .. }
                | ArrowBodyError::DecodedTooLarge { .. }
        ),
        "a damaged body is refused for what it holds: {report:?}"
    );
}

/// A batch read back from its relay body and from its sealed snapshot section has the schema it
/// was written with, every value of every column, and every null.
#[test]
fn bolero_arrow_bodies_restore_every_column_value_and_null() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(1024)
        .for_each(|input: &[u8]| {
            let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .assured("a property runtime opens");
            let executor = Executor::default();
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let batch = generated_batch(&mut arbitrary);
            let schema = batch.batch().schema();
            runtime.block_on(async {
                for carriage in [Carriage::Relay, Carriage::SnapshotSection] {
                    let body = carriage
                        .encode(&batch, &executor)
                        .await
                        .assured("a bounded generated batch encodes");
                    let decoded = carriage
                        .decode(&schema, &executor, body.as_ref().to_vec())
                        .await
                        .assured("an encoded body decodes");
                    assert_eq!(decoded.batch(), batch.batch());
                }
            });
        });
}

/// A relay body or a sealed snapshot section damaged once, and arbitrary bytes, are refused for
/// what they hold or decode to a batch that encodes and decodes back to itself. No damage makes
/// the decoder allocate from a length the body does not carry, which would abort the process.
#[test]
fn bolero_damaged_arrow_bodies_are_refused_typed_or_decode_canonically() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|input: &[u8]| {
            let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .assured("a property runtime opens");
            let executor = Executor::default();
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let batch = generated_batch(&mut arbitrary);
            let schema = batch.batch().schema();
            let carriage = Carriage::generated(&mut arbitrary);
            runtime.block_on(async {
                let body = carriage
                    .encode(&batch, &executor)
                    .await
                    .assured("a bounded generated batch encodes");
                let damaged_body = damaged(&mut arbitrary, body.as_ref().to_vec());
                assert_refused_typed_or_decodes_canonically(
                    carriage,
                    &schema,
                    &executor,
                    damaged_body,
                )
                .await;
                assert_refused_typed_or_decodes_canonically(
                    carriage,
                    &schema,
                    &executor,
                    input.to_vec(),
                )
                .await;
            });
        });
}
