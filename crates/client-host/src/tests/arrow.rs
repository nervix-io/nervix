//! Batches through the Arrow C data interface, as a host's Arrow library exchanges them.
//!
//! A batch built level by level exports its row schema, its rows as one struct array and a stream
//! of itself, and importing the array or the stream yields the same batch, down to its canonical
//! Arrow IPC stream. Arrow data a host's library made is held to the endpoint's schema.

use std::num::{NonZeroU32, NonZeroUsize};

use arrow_array::{
    Array as _, ArrayRef, Int64Array, ListArray, RecordBatch, RecordBatchIterator, StringArray,
    StructArray, TimestampNanosecondArray, UInt64Array, ffi::to_ffi,
    ffi_stream::FFI_ArrowArrayStream,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::Entropy;
use nervix_models::{FieldName, ParseAsType, SchemaField};
use nervix_primitives::sync::StdArc;

use crate::{Batch, BatchBuilder, FailureKind, FieldType, Schema};

/// The most rows a generated batch holds.
const ROWS: usize = 6;

/// The most columns a generated batch holds beyond its first.
const MORE_COLUMNS: usize = 3;

/// The most lists a generated column nests around its scalar.
const LISTS: usize = 2;

/// The most elements a generated variable-length list holds.
const ELEMENTS: usize = 2;

/// The most elements a generated fixed-size list holds.
const FIXED_ELEMENTS: u64 = 3;

/// The most characters of a generated text, or bytes of a generated blob.
const TEXT: usize = 3;

/// What a generated text is made of: ASCII, two- and four-byte UTF-8, and NUL.
const ALPHABET: [&str; 4] = ["a", "\u{e9}", "\u{1f680}", "\0"];

fn field(raw: &str, ty: ParseAsType) -> SchemaField {
    SchemaField {
        name: FieldName::try_from(raw).assured("the test passes a valid name"),
        ty,
        optional: false,
        sensitive: false,
    }
}

fn list(element: ParseAsType) -> ParseAsType {
    ParseAsType::Vec {
        element: Box::new(element),
    }
}

/// Any scalar the vocabulary has.
fn scalar(entropy: &mut Entropy<'_>) -> ParseAsType {
    let scalars = ParseAsType::scalar_variants();
    let count = NonZeroUsize::new(scalars.len()).assured("the vocabulary has scalar types");
    let index = entropy.index(count);
    scalars[index].clone()
}

/// A scalar inside up to [`LISTS`] lists, each a variable-length list or a fixed-size list of one
/// to [`FIXED_ELEMENTS`] elements.
fn generated_type(entropy: &mut Entropy<'_>) -> ParseAsType {
    let mut ty = scalar(entropy);
    for _ in 0..entropy.count(LISTS) {
        let element = Box::new(ty);
        if entropy.flag() {
            ty = ParseAsType::Vec { element };
            continue;
        }
        let length = entropy.boundary_biased(1..=FIXED_ELEMENTS);
        let length = u32::try_from(length).verified("a length drawn up to FIXED_ELEMENTS fits u32");
        let len = NonZeroU32::new(length).verified("a length drawn from one is positive");
        ty = ParseAsType::Array { element, len };
    }
    ty
}

/// The values of a string or bytes level: one offset per value and a final end, starting at zero,
/// into the concatenated values.
struct Varlen {
    offsets: Vec<u64>,
    data: Vec<u8>,
}

impl Varlen {
    fn of(values: &[Vec<u8>]) -> Self {
        let mut offsets = Vec::with_capacity(values.len() + 1);
        let mut data = Vec::new();
        offsets.push(0);
        for value in values {
            data.extend_from_slice(value);
            offsets.push(u64::try_from(data.len()).assured("a generated level is small"));
        }
        Self { offsets, data }
    }
}

/// `count` generated texts.
fn texts(entropy: &mut Entropy<'_>, count: usize) -> Vec<Vec<u8>> {
    let mut texts = Vec::with_capacity(count);
    for _ in 0..count {
        let mut text = String::new();
        for _ in 0..entropy.count(TEXT) {
            text.push_str(entropy.pick(ALPHABET));
        }
        texts.push(text.into_bytes());
    }
    texts
}

/// `count` generated blobs of any bytes.
fn blobs(entropy: &mut Entropy<'_>, count: usize) -> Vec<Vec<u8>> {
    let mut blobs = Vec::with_capacity(count);
    for _ in 0..count {
        let mut blob = Vec::new();
        for _ in 0..entropy.count(TEXT) {
            blob.push(entropy.byte());
        }
        blobs.push(blob);
    }
    blobs
}

/// Sets every level of column `column`, whose field is `field`, to generated values for `rows`
/// rows: any value under a null row, and every bit pattern of a fixed-width value.
fn set_column(
    entropy: &mut Entropy<'_>,
    builder: &mut BatchBuilder,
    column: usize,
    field: &SchemaField,
    rows: usize,
) {
    if field.optional {
        let mut present = Vec::with_capacity(rows);
        for _ in 0..rows {
            present.push(entropy.flag());
        }
        builder
            .presence(column, &present)
            .assured("an optional column takes nulls");
    }
    let mut cells = rows;
    let mut ty = &field.ty;
    let mut level = 0_usize;
    loop {
        match ty {
            ParseAsType::Vec { element } => {
                let mut offsets = Vec::with_capacity(cells + 1);
                let mut end = 0_u64;
                offsets.push(end);
                for _ in 0..cells {
                    let elements = u64::try_from(entropy.count(ELEMENTS))
                        .assured("a generated count is small");
                    end += elements;
                    offsets.push(end);
                }
                builder
                    .offsets(column, level, &offsets)
                    .assured("generated offsets start at zero and never fall");
                cells = usize::try_from(end).assured("a generated level is small");
                ty = element;
            }
            ParseAsType::Array { element, len } => {
                let per_list = usize::try_from(len.get()).assured("a generated length is small");
                cells *= per_list;
                ty = element;
            }
            ParseAsType::String => {
                let values = Varlen::of(&texts(entropy, cells));
                builder
                    .varlen(column, level, &values.offsets, &values.data)
                    .assured("generated texts are UTF-8");
                return;
            }
            ParseAsType::Bytes => {
                let values = Varlen::of(&blobs(entropy, cells));
                builder
                    .varlen(column, level, &values.offsets, &values.data)
                    .assured("generated blobs fill their level");
                return;
            }
            ParseAsType::Bool => {
                let mut values = Vec::with_capacity(cells);
                for _ in 0..cells {
                    values.push(u8::from(entropy.flag()));
                }
                builder
                    .fixed(column, level, &values)
                    .assured("generated booleans are zero or one");
                return;
            }
            scalar => {
                let width = FieldType::from(scalar)
                    .fixed_width()
                    .assured("every other scalar is fixed-width");
                let mut values = Vec::with_capacity(cells * width);
                for _ in 0..cells * width {
                    values.push(entropy.byte());
                }
                builder
                    .fixed(column, level, &values)
                    .assured("generated values fill their level");
                return;
            }
        }
        level += 1;
    }
}

/// A batch of up to [`ROWS`] rows of one to four generated columns, nullable or not and
/// sensitive or not, built level by level as a host builds one.
fn generated_batch(entropy: &mut Entropy<'_>) -> Batch {
    let rows = entropy.count(ROWS);
    let columns = 1 + entropy.count(MORE_COLUMNS);
    let mut fields = Vec::with_capacity(columns);
    for index in 0..columns {
        let ty = generated_type(entropy);
        fields.push(SchemaField {
            optional: entropy.flag(),
            sensitive: entropy.flag(),
            ..field(&format!("column_{index}"), ty)
        });
    }
    let schema = Schema::of_fields(fields.clone());
    let mut builder = BatchBuilder::new(schema, rows);
    for (column, field) in fields.iter().enumerate() {
        set_column(entropy, &mut builder, column, field, rows);
    }
    builder
        .finish()
        .assured("every level of every column is set")
}

/// Exports `batch` as a host's Arrow library reads it, imports it back through its struct array
/// and through its stream, and asserts both copies are `batch`.
fn assert_round_trip(batch: &Batch) {
    let exported = batch
        .export_schema()
        .assured("a built batch exports its row schema");
    let rows = DataType::try_from(&exported).assured("an exported schema imports");
    let fields = batch.record_batch().schema().fields().clone();
    assert_eq!(rows, DataType::Struct(fields), "the exported row schema");

    let exported = batch
        .export_array()
        .assured("a built batch exports its rows");
    let arrayed = Batch::import_array(exported.array, &exported.schema, batch.schema().clone())
        .assured("an exported struct array imports as its own schema");
    let streamed = Batch::import_stream(batch.export_stream(), batch.schema().clone())
        .assured("an exported stream imports as its own schema");
    let canonical = batch.ipc().assured("a built batch writes its stream");
    for copy in [&arrayed, &streamed] {
        assert_eq!(copy.row_count(), batch.row_count());
        assert_eq!(copy.record_batch(), batch.record_batch());
        let written = copy.ipc().assured("an imported batch writes its stream");
        assert_eq!(
            written, canonical,
            "the canonical stream of the imported batch"
        );
    }
}

#[test]
fn bolero_host_columns_round_trip_through_the_arrow_c_data_interface() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .for_each(|input: &[u8]| {
            let mut entropy = Entropy::new(input);
            let batch = generated_batch(&mut entropy);
            assert_round_trip(&batch);
        });
}

/// The schema of the endpoint a host's Arrow data is held to: an id, a list of tags, a time and
/// an optional note.
fn endpoint_schema() -> Schema {
    Schema::of_fields(vec![
        field("id", ParseAsType::U64),
        field("tags", list(ParseAsType::String)),
        field("at", ParseAsType::Datetime),
        SchemaField {
            optional: true,
            ..field("note", ParseAsType::String)
        },
    ])
}

/// Two rows of [`endpoint_schema`], built as a host builds them through the runtime.
fn built_rows() -> Batch {
    let mut builder = BatchBuilder::new(endpoint_schema(), 2);
    let ids: Vec<u8> = [1_u64, u64::MAX]
        .iter()
        .flat_map(|id| id.to_ne_bytes())
        .collect();
    builder.fixed(0, 0, &ids).assured("two ids fill the column");
    builder
        .offsets(1, 0, &[0, 2, 3])
        .assured("three tags fill two lists");
    builder
        .varlen(1, 1, &[0, 1, 2, 3], b"abc")
        .assured("three tags fill the level");
    let times: Vec<u8> = [i64::MIN, 7_i64]
        .iter()
        .flat_map(|at| at.to_ne_bytes())
        .collect();
    builder
        .fixed(2, 0, &times)
        .assured("two times fill the column");
    builder
        .presence(3, &[true, false])
        .assured("the note is optional");
    builder
        .varlen(3, 0, &[0, 2, 2], b"hi")
        .assured("one note fills the column");
    builder.finish().assured("every column is set")
}

/// The columns of [`built_rows`] as a host's Arrow library makes them: every field nullable, the
/// tags' elements named `element` and nullable, and the times in `timezone`.
struct Brought {
    ids: ArrayRef,
    tags: ArrayRef,
    times: ArrayRef,
    notes: ArrayRef,
}

impl Brought {
    fn new() -> Self {
        Self::in_timezone("+00:00")
    }

    fn in_timezone(timezone: &str) -> Self {
        let tags = StringArray::from(vec!["a", "b", "c"]);
        Self {
            ids: StdArc::new(UInt64Array::from(vec![1, u64::MAX])),
            tags: Self::tags(StdArc::new(tags)),
            times: StdArc::new(
                TimestampNanosecondArray::from(vec![i64::MIN, 7]).with_timezone(timezone),
            ),
            notes: StdArc::new(StringArray::from(vec![Some("hi"), None])),
        }
    }

    /// Two lists of `elements`: the first two, then the third.
    fn tags(elements: ArrayRef) -> ArrayRef {
        let element = Field::new("element", elements.data_type().clone(), true);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3]));
        StdArc::new(ListArray::new(
            StdArc::new(element),
            offsets,
            elements,
            None,
        ))
    }

    /// The columns under their names, in the endpoint's order.
    fn named(self) -> Vec<(&'static str, ArrayRef)> {
        vec![
            ("id", self.ids),
            ("tags", self.tags),
            ("at", self.times),
            ("note", self.notes),
        ]
    }

    fn batch(self) -> RecordBatch {
        RecordBatch::try_from_iter(self.named()).assured("the brought columns have two rows each")
    }
}

/// Asserts that `batch` is refused as Arrow data of [`endpoint_schema`].
fn assert_refused(batch: RecordBatch, why: &str) {
    match Batch::conform(batch, endpoint_schema()) {
        Ok(_) => panic!("{why} was imported"),
        Err(failure) => assert_eq!(
            failure.kind(),
            FailureKind::InvalidArgument,
            "{why}: {}",
            failure.message()
        ),
    }
}

#[test]
fn arrow_data_is_held_to_the_endpoint_schema() {
    let built = built_rows();

    // Another element name and nullable fields with no null are the same batch.
    let conformed = Batch::conform(Brought::new().batch(), endpoint_schema())
        .assured("a host's batch of the endpoint's columns conforms");
    assert_eq!(conformed.record_batch(), built.record_batch());
    assert_eq!(
        conformed
            .ipc()
            .assured("a conformed batch writes its stream"),
        built.ipc().assured("a built batch writes its stream")
    );

    let mut retyped = Brought::new();
    retyped.ids = StdArc::new(Int64Array::from(vec![1, i64::MAX]));
    assert_refused(retyped.batch(), "a signed id");

    let mut null_id = Brought::new();
    null_id.ids = StdArc::new(UInt64Array::from(vec![Some(1), None]));
    assert_refused(null_id.batch(), "a null in a required field");

    let mut null_tag = Brought::new();
    null_tag.tags = Brought::tags(StdArc::new(StringArray::from(vec![
        Some("a"),
        None,
        Some("c"),
    ])));
    assert_refused(null_tag.batch(), "a null list element");

    assert_refused(
        Brought::in_timezone("UTC").batch(),
        "a time zone spelled UTC",
    );

    let mut renamed = Brought::new().named();
    renamed[0].0 = "ident";
    let renamed = RecordBatch::try_from_iter(renamed).assured("the columns have two rows each");
    assert_refused(renamed, "a renamed column");

    let mut reordered = Brought::new().named();
    reordered.swap(0, 2);
    let reordered = RecordBatch::try_from_iter(reordered).assured("the columns have two rows each");
    assert_refused(reordered, "reordered columns");

    let mut short = Brought::new().named();
    short.pop();
    let short = RecordBatch::try_from_iter(short).assured("the columns have two rows each");
    assert_refused(short, "a missing column");
}

#[test]
fn an_imported_array_is_a_struct_without_null_rows_and_a_stream_is_concatenated() {
    let built = built_rows();
    let rows = StructArray::from(Brought::new().batch());

    let (array, schema) = to_ffi(&rows.to_data()).assured("a struct array exports");
    let imported = Batch::import_array(array, &schema, endpoint_schema())
        .assured("a struct array of the endpoint's columns imports");
    assert_eq!(imported.record_batch(), built.record_batch());

    let (fields, columns, _) = rows.clone().into_parts();
    let null_row = StructArray::new(fields, columns, Some(NullBuffer::from(vec![true, false])));
    let (array, schema) = to_ffi(&null_row.to_data()).assured("a struct array exports");
    match Batch::import_array(array, &schema, endpoint_schema()) {
        Ok(_) => panic!("a struct array with a null row was imported"),
        Err(failure) => assert_eq!(failure.kind(), FailureKind::InvalidArgument),
    }

    let ids = UInt64Array::from(vec![1, u64::MAX]);
    let (array, schema) = to_ffi(&ids.to_data()).assured("an array exports");
    match Batch::import_array(array, &schema, endpoint_schema()) {
        Ok(_) => panic!("an array that is not a struct of columns was imported"),
        Err(failure) => assert_eq!(failure.kind(), FailureKind::InvalidArgument),
    }

    let brought = Brought::new().batch();
    let first = brought.slice(0, 1);
    let second = brought.slice(1, 1);
    let reader = RecordBatchIterator::new([Ok(first), Ok(second)], brought.schema());
    let stream = FFI_ArrowArrayStream::new(Box::new(reader));
    let concatenated = Batch::import_stream(stream, endpoint_schema())
        .assured("a stream of the endpoint's columns imports");
    assert_eq!(concatenated.record_batch(), built.record_batch());
}
