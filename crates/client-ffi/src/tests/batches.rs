//! Building and reading endpoint batches through the C ABI, as a host calls it.
//!
//! A host's columns are built from buffers the test overwrites as soon as each call returns, and
//! read back one level at a time: from the finished batch, and from the canonical stream a
//! delivery of it would carry. Both reads have to give back exactly what the host passed.

use std::{io::Cursor, num::NonZeroU32, ptr};

use arrow_ipc::reader::StreamReader;
use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{ParseAsType, SchemaField};
use nervix_primitives::thread;

use super::{failure_kind, field, succeeded};
use crate::{
    Batch, BatchBuilder, FailureKind, Schema, nx_batch_builder_finish, nx_batch_builder_fixed,
    nx_batch_builder_free, nx_batch_builder_new, nx_batch_builder_offsets, nx_batch_builder_states,
    nx_batch_builder_varlen, nx_batch_cells, nx_batch_fixed, nx_batch_ipc, nx_batch_offsets,
    nx_batch_release, nx_batch_retain, nx_batch_row_count, nx_batch_schema, nx_batch_states,
    nx_batch_varlen, nx_schema_field_count, nx_schema_field_level, nx_schema_field_levels,
    nx_schema_free,
};

const VALUE: u8 = 1;
const NULL: u8 = 2;
const REDACTED: u8 = 3;

/// One list level of a host's column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum HostList {
    /// Variable-length lists and their offsets into the next level.
    Variable(Vec<u64>),
    /// Fixed-size lists of this many elements.
    Fixed(u32),
}

/// The innermost values of a host's column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum HostValues {
    Fixed(Vec<u8>),
    Varlen { offsets: Vec<u64>, data: Vec<u8> },
}

/// One column as a host passes it: row states when a row can be null, its list levels, and its
/// innermost values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct HostColumn {
    pub(super) states: Option<Vec<u8>>,
    pub(super) lists: Vec<HostList>,
    pub(super) values: HostValues,
}

/// A builder, freed when dropped.
struct OwnedBuilder(*mut BatchBuilder);

impl Drop for OwnedBuilder {
    fn drop(&mut self) {
        // SAFETY: the builder is live and freed once, here.
        unsafe { nx_batch_builder_free(self.0) };
    }
}

/// One reference to a batch, released when dropped.
pub(super) struct SharedBatch(pub(super) *mut Batch);

// SAFETY: a batch is immutable and its references are counted atomically.
unsafe impl Send for SharedBatch {}

impl Drop for SharedBatch {
    fn drop(&mut self) {
        // SAFETY: the reference is live and released once, here.
        unsafe { nx_batch_release(self.0) };
    }
}

fn schema_of(fields: Vec<SchemaField>) -> Schema {
    Schema::of_fields(fields)
}

fn optional(mut field: SchemaField) -> SchemaField {
    field.optional = true;
    field
}

fn list(element: ParseAsType) -> ParseAsType {
    ParseAsType::Vec {
        element: Box::new(element),
    }
}

fn array(element: ParseAsType, len: u32) -> ParseAsType {
    ParseAsType::Array {
        element: Box::new(element),
        len: NonZeroU32::new(len).assured("a test array has elements"),
    }
}

/// Builds a batch of `rows` rows of `schema` from `columns`, overwriting every buffer as soon as
/// the call that received it returns.
pub(super) fn build(schema: &Schema, rows: usize, columns: &[HostColumn]) -> SharedBatch {
    let mut builder = ptr::null_mut();
    // SAFETY: the schema is live and `builder` is writable.
    succeeded(unsafe { nx_batch_builder_new(schema, rows, &mut builder) });
    let builder = OwnedBuilder(builder);
    for (index, column) in columns.iter().enumerate() {
        let mut column = column.clone();
        if let Some(states) = column.states.as_mut() {
            // SAFETY: the builder is live and the states address their length.
            succeeded(unsafe {
                nx_batch_builder_states(builder.0, index, states.as_ptr(), states.len())
            });
            states.fill(0xaa);
        }
        for (level, host_list) in column.lists.iter_mut().enumerate() {
            if let HostList::Variable(offsets) = host_list {
                // SAFETY: the builder is live and the offsets address their length.
                succeeded(unsafe {
                    nx_batch_builder_offsets(
                        builder.0,
                        index,
                        level,
                        offsets.as_ptr(),
                        offsets.len(),
                    )
                });
                offsets.fill(u64::MAX);
            }
        }
        let leaf = column.lists.len();
        match &mut column.values {
            HostValues::Fixed(values) => {
                // SAFETY: the builder is live and the values address their length.
                succeeded(unsafe {
                    nx_batch_builder_fixed(
                        builder.0,
                        index,
                        leaf,
                        values.as_ptr().cast(),
                        values.len(),
                    )
                });
                values.fill(0xaa);
            }
            HostValues::Varlen { offsets, data } => {
                // SAFETY: the builder is live and the buffers address their lengths.
                succeeded(unsafe {
                    nx_batch_builder_varlen(
                        builder.0,
                        index,
                        leaf,
                        offsets.as_ptr(),
                        offsets.len(),
                        data.as_ptr(),
                        data.len(),
                    )
                });
                offsets.fill(u64::MAX);
                data.fill(0xaa);
            }
        }
    }
    let mut batch = ptr::null_mut();
    // SAFETY: the builder is live and `batch` is writable.
    succeeded(unsafe { nx_batch_builder_finish(builder.0, &mut batch) });
    SharedBatch(batch)
}

fn cells(batch: &SharedBatch, column: usize, level: usize) -> usize {
    let mut count = 0;
    // SAFETY: the batch is live and `count` is writable.
    succeeded(unsafe { nx_batch_cells(batch.0, column, level, &mut count) });
    count
}

/// Reads one column back in one call per level, in the host's layout for `like`.
pub(super) fn read(batch: &SharedBatch, column: usize, like: &HostColumn) -> HostColumn {
    let rows = cells(batch, column, 0);
    let mut states = vec![0_u8; rows];
    // SAFETY: the batch is live and `states` holds one byte per row.
    succeeded(unsafe { nx_batch_states(batch.0, column, states.as_mut_ptr(), states.len()) });
    let states = match like.states {
        Some(_) => Some(states),
        None => {
            assert!(states.iter().all(|state| *state == VALUE));
            None
        }
    };
    let mut lists = Vec::with_capacity(like.lists.len());
    for (level, host_list) in like.lists.iter().enumerate() {
        match host_list {
            HostList::Variable(_) => {
                let mut offsets = vec![0_u64; cells(batch, column, level) + 1];
                // SAFETY: the batch is live and `offsets` holds one entry per list and an end.
                succeeded(unsafe {
                    nx_batch_offsets(batch.0, column, level, offsets.as_mut_ptr(), offsets.len())
                });
                lists.push(HostList::Variable(offsets));
            }
            HostList::Fixed(length) => lists.push(HostList::Fixed(*length)),
        }
    }
    let leaf = like.lists.len();
    let count = cells(batch, column, leaf);
    let values = match &like.values {
        HostValues::Fixed(values) => {
            let width = values.len().checked_div(count).unwrap_or(0);
            let mut read = vec![0_u8; width * count];
            if count > 0 {
                // SAFETY: the batch is live and `read` holds one width per cell.
                succeeded(unsafe {
                    nx_batch_fixed(batch.0, column, leaf, read.as_mut_ptr().cast(), read.len())
                });
            }
            HostValues::Fixed(read)
        }
        HostValues::Varlen { .. } => {
            let mut offsets = vec![0_u64; count + 1];
            let mut data_len = 0;
            // SAFETY: the batch is live; a null data buffer asks for the length only.
            succeeded(unsafe {
                nx_batch_varlen(
                    batch.0,
                    column,
                    leaf,
                    offsets.as_mut_ptr(),
                    offsets.len(),
                    ptr::null_mut(),
                    0,
                    &mut data_len,
                )
            });
            let mut data = vec![0_u8; data_len];
            // SAFETY: the batch is live and both buffers hold what the level needs.
            succeeded(unsafe {
                nx_batch_varlen(
                    batch.0,
                    column,
                    leaf,
                    offsets.as_mut_ptr(),
                    offsets.len(),
                    data.as_mut_ptr(),
                    data.len(),
                    &mut data_len,
                )
            });
            HostValues::Varlen { offsets, data }
        }
    };
    HostColumn {
        states,
        lists,
        values,
    }
}

/// The canonical stream of a batch, borrowed and copied.
pub(super) fn stream(batch: &SharedBatch) -> Vec<u8> {
    let mut ipc = ptr::null();
    let mut ipc_len = 0;
    // SAFETY: the batch is live, and the stream is copied before the reference can be released.
    unsafe {
        succeeded(nx_batch_ipc(batch.0, &mut ipc, &mut ipc_len));
        std::slice::from_raw_parts(ipc, ipc_len).to_vec()
    }
}

/// The batch a delivery carrying `ipc` for `schema` would read, decoded the way the Rust client
/// decodes a delivery.
fn delivered(schema: &Schema, ipc: Vec<u8>) -> SharedBatch {
    let mut reader =
        StreamReader::try_new(Cursor::new(ipc.clone()), None).assured("a written stream opens");
    let batch = reader
        .next()
        .assured("a written stream holds a batch")
        .assured("a written batch decodes");
    assert!(reader.next().is_none(), "a written stream holds one batch");
    SharedBatch(Batch::decoded(batch, schema.clone(), Bytes::from(ipc)).into_shared())
}

fn native<const WIDTH: usize>(values: impl IntoIterator<Item = [u8; WIDTH]>) -> Vec<u8> {
    values.into_iter().flatten().collect()
}

pub(super) fn varlen<'a>(values: impl IntoIterator<Item = &'a [u8]>) -> HostValues {
    let mut offsets = vec![0_u64];
    let mut data = Vec::new();
    for value in values {
        data.extend_from_slice(value);
        offsets.push(u64::try_from(data.len()).assured("a test column is small"));
    }
    HostValues::Varlen { offsets, data }
}

/// A schema with a field of every kind a host builds, and three rows of each.
fn every_kind() -> (Schema, Vec<HostColumn>) {
    let schema = schema_of(vec![
        field("id", ParseAsType::U32),
        field("flag", ParseAsType::Bool),
        optional(field("text", ParseAsType::String)),
        field("raw", ParseAsType::Bytes),
        field("at", ParseAsType::Datetime),
        optional(field("maybe", ParseAsType::F64)),
        field("tags", list(ParseAsType::String)),
        field("grid", array(array(ParseAsType::I16, 2), 2)),
        optional(field("spans", list(array(ParseAsType::Datetime, 2)))),
    ]);
    let columns = vec![
        HostColumn {
            states: None,
            lists: Vec::new(),
            values: HostValues::Fixed(native([1_u32, u32::MAX, 0].map(u32::to_ne_bytes))),
        },
        HostColumn {
            states: None,
            lists: Vec::new(),
            values: HostValues::Fixed(vec![1, 0, 1]),
        },
        HostColumn {
            states: Some(vec![VALUE, NULL, VALUE]),
            lists: Vec::new(),
            values: varlen(["h\u{e9}llo \u{1f680}".as_bytes(), b"", b"a\0b"]),
        },
        HostColumn {
            states: None,
            lists: Vec::new(),
            values: varlen([&[0xff_u8, 0x00][..], &[], &[0x80]]),
        },
        HostColumn {
            states: None,
            lists: Vec::new(),
            values: HostValues::Fixed(native([i64::MIN, i64::MAX, 1].map(i64::to_ne_bytes))),
        },
        HostColumn {
            states: Some(vec![NULL, VALUE, VALUE]),
            lists: Vec::new(),
            values: HostValues::Fixed(native(
                [
                    f64::from_bits(7),
                    -0.0,
                    f64::from_bits(0x7ff8_0000_0000_0001),
                ]
                .map(f64::to_ne_bytes),
            )),
        },
        HostColumn {
            states: None,
            lists: vec![HostList::Variable(vec![0, 2, 2, 3])],
            values: varlen([&b"a"[..], b"", "\u{4e16}".as_bytes()]),
        },
        HostColumn {
            states: None,
            lists: vec![HostList::Fixed(2), HostList::Fixed(2)],
            values: HostValues::Fixed(native(
                [1_i16, -2, i16::MAX, i16::MIN, 0, 0, 0, 0, -1, 1, 2, -2].map(i16::to_ne_bytes),
            )),
        },
        HostColumn {
            states: Some(vec![VALUE, NULL, VALUE]),
            lists: vec![HostList::Variable(vec![0, 1, 1, 1]), HostList::Fixed(2)],
            values: HostValues::Fixed(native([i64::MIN, i64::MAX].map(i64::to_ne_bytes))),
        },
    ];
    (schema, columns)
}

#[test]
fn every_kind_of_column_reads_back_from_the_batch_and_from_its_stream() {
    let (schema, columns) = every_kind();
    let batch = build(&schema, 3, &columns);
    // SAFETY: the batch is live.
    assert_eq!(unsafe { nx_batch_row_count(batch.0) }, 3);
    for (index, column) in columns.iter().enumerate() {
        assert_eq!(&read(&batch, index, column), column, "column {index}");
    }
    let delivered = delivered(&schema, stream(&batch));
    for (index, column) in columns.iter().enumerate() {
        assert_eq!(&read(&delivered, index, column), column, "column {index}");
    }
    assert_eq!(stream(&delivered), stream(&batch));
}

#[test]
fn a_batch_reports_its_schema_and_the_levels_of_every_field() {
    let (schema, columns) = every_kind();
    let batch = build(&schema, 3, &columns);
    let mut reported = ptr::null_mut();
    // SAFETY: the batch is live and `reported` is writable.
    succeeded(unsafe { nx_batch_schema(batch.0, &mut reported) });
    let mut count = 0;
    let mut levels = 0;
    let mut level_type = crate::FieldType::U8;
    let mut length = 0;
    // SAFETY: the schema is live until it is freed below, and every out-parameter is writable.
    unsafe {
        succeeded(nx_schema_field_count(reported, 1, &mut count));
        assert_eq!(count, 9);
        succeeded(nx_schema_field_levels(reported, 1, 7, &mut levels));
        assert_eq!(levels, 3);
        succeeded(nx_schema_field_level(
            reported,
            1,
            7,
            1,
            &mut level_type,
            &mut length,
        ));
        assert_eq!((level_type, length), (crate::FieldType::FixedList, 2));
        succeeded(nx_schema_field_level(
            reported,
            1,
            7,
            2,
            &mut level_type,
            &mut length,
        ));
        assert_eq!((level_type, length), (crate::FieldType::I16, 0));
        succeeded(nx_schema_field_levels(reported, 1, 0, &mut levels));
        assert_eq!(levels, 1);
        assert_eq!(
            failure_kind(nx_schema_field_level(
                reported,
                1,
                7,
                3,
                &mut level_type,
                &mut length
            )),
            FailureKind::InvalidArgument
        );
        nx_schema_free(reported);
    }
}

#[test]
fn a_builder_refuses_what_a_column_cannot_hold() {
    let (schema, _) = every_kind();
    let mut builder = ptr::null_mut();
    // SAFETY: the schema is live and `builder` is writable.
    succeeded(unsafe { nx_batch_builder_new(&schema, 3, &mut builder) });
    let builder = OwnedBuilder(builder);
    let refuse = failure_kind;
    // SAFETY: the builder is live and every buffer addresses its length.
    unsafe {
        // A required field cannot hold a null, no host marks a cell redacted, and a state names
        // a row.
        assert_eq!(
            refuse(nx_batch_builder_states(
                builder.0,
                0,
                [VALUE, NULL, VALUE].as_ptr(),
                3
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            refuse(nx_batch_builder_states(
                builder.0,
                2,
                [VALUE, REDACTED, VALUE].as_ptr(),
                3
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            refuse(nx_batch_builder_states(builder.0, 2, [VALUE].as_ptr(), 1)),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            refuse(nx_batch_builder_states(
                builder.0,
                2,
                [VALUE, 9, VALUE].as_ptr(),
                3
            )),
            FailureKind::InvalidArgument
        );
        // Offsets start at zero and never decrease, and only a variable-length level has them.
        assert_eq!(
            refuse(nx_batch_builder_offsets(
                builder.0,
                6,
                0,
                [1_u64, 2, 3, 4].as_ptr(),
                4
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            refuse(nx_batch_builder_offsets(
                builder.0,
                6,
                0,
                [0_u64, 2, 1, 3].as_ptr(),
                4
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            refuse(nx_batch_builder_offsets(
                builder.0,
                7,
                0,
                [0_u64, 2, 4, 6].as_ptr(),
                4
            )),
            FailureKind::Type
        );
        assert_eq!(
            refuse(nx_batch_builder_offsets(
                builder.0,
                6,
                1,
                [0_u64, 1].as_ptr(),
                2
            )),
            FailureKind::Type
        );
        // Values fill the innermost level only, at their type's width, and a boolean is 0 or 1.
        assert_eq!(
            refuse(nx_batch_builder_fixed(
                builder.0,
                0,
                0,
                [0_u8; 5].as_ptr().cast(),
                5
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            refuse(nx_batch_builder_fixed(
                builder.0,
                1,
                0,
                [0_u8, 2, 1].as_ptr().cast(),
                3
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            refuse(nx_batch_builder_fixed(
                builder.0,
                2,
                0,
                [0_u8; 3].as_ptr().cast(),
                3
            )),
            FailureKind::Type
        );
        assert_eq!(
            refuse(nx_batch_builder_fixed(
                builder.0,
                7,
                1,
                [0_u8; 8].as_ptr().cast(),
                8
            )),
            FailureKind::Type
        );
        assert_eq!(
            refuse(nx_batch_builder_varlen(
                builder.0,
                0,
                0,
                [0_u64, 0].as_ptr(),
                2,
                ptr::null(),
                0
            )),
            FailureKind::Type
        );
        assert_eq!(
            refuse(nx_batch_builder_varlen(
                builder.0,
                3,
                0,
                [0_u64, 3].as_ptr(),
                2,
                [0_u8].as_ptr(),
                1
            )),
            FailureKind::InvalidArgument
        );
        // A column past the schema, a missing buffer, and a level past the type are refused.
        assert_eq!(
            refuse(nx_batch_builder_fixed(
                builder.0,
                9,
                0,
                [0_u8; 4].as_ptr().cast(),
                4
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            refuse(nx_batch_builder_states(builder.0, 2, ptr::null(), 3)),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            refuse(nx_batch_builder_fixed(
                builder.0,
                7,
                3,
                [0_u8; 2].as_ptr().cast(),
                2
            )),
            FailureKind::Type
        );
        // Finishing names what is missing, and what does not add up.
        let mut batch = ptr::null_mut();
        assert_eq!(
            refuse(nx_batch_builder_finish(builder.0, &mut batch)),
            FailureKind::InvalidArgument
        );
    }
}

/// Finishes a batch of `rows` rows of one `id` column and one `tags` list of text, which has to
/// fail.
fn finishing_fails(rows: usize, ids: &[u32], tag_offsets: &[u64], tag_data: &[u8]) {
    let schema = schema_of(vec![
        field("id", ParseAsType::U32),
        field("tags", list(ParseAsType::String)),
    ]);
    let ids = native(ids.iter().map(|id| id.to_ne_bytes()));
    let text_offsets = [
        0_u64,
        u64::try_from(tag_data.len()).assured("a test value is small"),
    ];
    let mut builder = ptr::null_mut();
    // SAFETY: the schema is live and `builder` is writable.
    succeeded(unsafe { nx_batch_builder_new(&schema, rows, &mut builder) });
    let builder = OwnedBuilder(builder);
    let mut batch = ptr::null_mut();
    // SAFETY: the builder is live and every buffer addresses its length.
    unsafe {
        succeeded(nx_batch_builder_fixed(
            builder.0,
            0,
            0,
            ids.as_ptr().cast(),
            ids.len(),
        ));
        succeeded(nx_batch_builder_offsets(
            builder.0,
            1,
            0,
            tag_offsets.as_ptr(),
            tag_offsets.len(),
        ));
        succeeded(nx_batch_builder_varlen(
            builder.0,
            1,
            1,
            text_offsets.as_ptr(),
            text_offsets.len(),
            tag_data.as_ptr(),
            tag_data.len(),
        ));
        assert_eq!(
            failure_kind(nx_batch_builder_finish(builder.0, &mut batch)),
            FailureKind::InvalidArgument
        );
    }
}

#[test]
fn finishing_refuses_values_that_do_not_fill_their_level_and_text_that_is_not_utf8() {
    // Two rows, and one id.
    finishing_fails(2, &[1], &[0, 1, 1], b"a");
    // Two rows of lists, and offsets for one.
    finishing_fails(2, &[1, 2], &[0, 1], b"a");
    // Text that is not UTF-8.
    finishing_fails(1, &[1], &[0, 1], &[0xff]);
}

#[test]
fn a_reader_refuses_a_level_its_type_does_not_hold_and_buffers_of_another_size() {
    let (schema, columns) = every_kind();
    let batch = build(&schema, 3, &columns);
    let mut small = [0_u8; 2];
    let mut offsets = [0_u64; 4];
    let mut data_len = 0;
    // SAFETY: the batch is live and every buffer addresses its length.
    unsafe {
        assert_eq!(
            failure_kind(nx_batch_states(batch.0, 0, small.as_mut_ptr(), small.len())),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_batch_fixed(
                batch.0,
                0,
                0,
                small.as_mut_ptr().cast(),
                small.len()
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_batch_fixed(
                batch.0,
                2,
                0,
                small.as_mut_ptr().cast(),
                small.len()
            )),
            FailureKind::Type
        );
        assert_eq!(
            failure_kind(nx_batch_offsets(
                batch.0,
                0,
                0,
                offsets.as_mut_ptr(),
                offsets.len()
            )),
            FailureKind::Type
        );
        assert_eq!(
            failure_kind(nx_batch_varlen(
                batch.0,
                0,
                0,
                offsets.as_mut_ptr(),
                offsets.len(),
                ptr::null_mut(),
                0,
                &mut data_len
            )),
            FailureKind::Type
        );
        assert_eq!(
            failure_kind(nx_batch_offsets(
                batch.0,
                9,
                0,
                offsets.as_mut_ptr(),
                offsets.len()
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(
            failure_kind(nx_batch_offsets(
                batch.0,
                6,
                2,
                offsets.as_mut_ptr(),
                offsets.len()
            )),
            FailureKind::InvalidArgument
        );
        let mut too_small = [0_u8; 1];
        assert_eq!(
            failure_kind(nx_batch_varlen(
                batch.0,
                2,
                0,
                offsets.as_mut_ptr(),
                offsets.len(),
                too_small.as_mut_ptr(),
                too_small.len(),
                &mut data_len
            )),
            FailureKind::InvalidArgument
        );
        assert_eq!(data_len, 14);
    }
}

#[test]
fn a_retained_batch_keeps_its_stream_after_another_reference_is_released_on_another_thread() {
    let (schema, columns) = every_kind();
    let first = build(&schema, 3, &columns);
    // SAFETY: the reference is live, and the binding returns a new one.
    let retained = SharedBatch(unsafe { nx_batch_retain(first.0) });
    let written = stream(&retained);
    let mut ipc = ptr::null();
    let mut ipc_len = 0;
    // SAFETY: the reference is live and the out-parameters are writable.
    succeeded(unsafe { nx_batch_ipc(retained.0, &mut ipc, &mut ipc_len) });
    thread::spawn(move || drop(first))
        .join()
        .assured("releasing on another thread does not panic");
    // SAFETY: the borrowed stream stays valid while `retained` is.
    let borrowed = unsafe { std::slice::from_raw_parts(ipc, ipc_len) };
    assert_eq!(borrowed, written.as_slice());
    assert_eq!(&read(&retained, 7, &columns[7]), &columns[7]);
    // SAFETY: releasing null does nothing.
    unsafe { nx_batch_release(ptr::null_mut()) };
}

/// Reads bounded values from Bolero's input, zero past its end.
struct Draw<'a> {
    input: &'a [u8],
    next: usize,
}

impl Draw<'_> {
    fn byte(&mut self) -> u8 {
        let byte = match self.input.get(self.next) {
            Some(byte) => *byte,
            None => 0,
        };
        self.next += 1;
        byte
    }

    /// A value below `bound`, which is positive.
    fn below(&mut self, bound: u8) -> u8 {
        self.byte() % bound
    }

    fn scalar(&mut self) -> ParseAsType {
        let scalars = ParseAsType::scalar_variants();
        let index = usize::from(self.byte()) % scalars.len();
        scalars[index].clone()
    }
}

/// A generated type: up to two list levels around a scalar.
fn generated_type(draw: &mut Draw<'_>) -> ParseAsType {
    let mut levels = Vec::new();
    for _ in 0..draw.below(3) {
        levels.push(match draw.below(2) {
            0 => None,
            _ => Some(u32::from(draw.below(3)) + 1),
        });
    }
    let mut ty = draw.scalar();
    for level in levels.into_iter().rev() {
        ty = match level {
            None => list(ty),
            Some(len) => array(ty, len),
        };
    }
    ty
}

/// A generated column of `rows` rows of `ty`, with any value in a null row.
fn generated_column(
    draw: &mut Draw<'_>,
    ty: &ParseAsType,
    nullable: bool,
    rows: usize,
) -> HostColumn {
    let states = if nullable {
        Some(
            (0..rows)
                .map(|_| if draw.below(3) == 0 { NULL } else { VALUE })
                .collect(),
        )
    } else {
        None
    };
    let mut lists = Vec::new();
    let mut count = rows;
    let mut current = ty;
    loop {
        match current {
            ParseAsType::Vec { element } => {
                let mut offsets = vec![0_u64];
                let mut end = 0_u64;
                for _ in 0..count {
                    end += u64::from(draw.below(3));
                    offsets.push(end);
                }
                count = usize::try_from(end).assured("a generated level is small");
                lists.push(HostList::Variable(offsets));
                current = element;
            }
            ParseAsType::Array { element, len } => {
                count *= usize::try_from(len.get()).assured("a generated length is small");
                lists.push(HostList::Fixed(len.get()));
                current = element;
            }
            ParseAsType::String => {
                let alphabet = ["a", "\u{e9}", "\u{1f680}", "\0"];
                let mut texts = Vec::new();
                for _ in 0..count {
                    let mut text = String::new();
                    for _ in 0..draw.below(3) {
                        text.push_str(alphabet[usize::from(draw.below(4))]);
                    }
                    texts.push(text);
                }
                let values = varlen(texts.iter().map(String::as_bytes));
                return HostColumn {
                    states,
                    lists,
                    values,
                };
            }
            ParseAsType::Bytes => {
                let mut blobs = Vec::new();
                for _ in 0..count {
                    let blob: Vec<u8> = (0..draw.below(3)).map(|_| draw.byte()).collect();
                    blobs.push(blob);
                }
                let values = varlen(blobs.iter().map(Vec::as_slice));
                return HostColumn {
                    states,
                    lists,
                    values,
                };
            }
            ParseAsType::Bool => {
                let values = (0..count).map(|_| draw.below(2)).collect();
                return HostColumn {
                    states,
                    lists,
                    values: HostValues::Fixed(values),
                };
            }
            scalar => {
                let width = crate::FieldType::from(scalar)
                    .fixed_width()
                    .assured("every other scalar is fixed-width");
                let values = (0..count * width).map(|_| draw.byte()).collect();
                return HostColumn {
                    states,
                    lists,
                    values: HostValues::Fixed(values),
                };
            }
        }
    }
}

#[test]
fn bolero_host_columns_round_trip_through_builder_and_stream() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .for_each(|input: &[u8]| {
            let mut draw = Draw { input, next: 0 };
            let rows = usize::from(draw.below(7));
            let mut fields = Vec::new();
            let mut columns = Vec::new();
            for index in 0..1 + draw.below(4) {
                let ty = generated_type(&mut draw);
                let nullable = draw.below(2) == 1;
                columns.push(generated_column(&mut draw, &ty, nullable, rows));
                fields.push(SchemaField {
                    optional: nullable,
                    ..field(&format!("column_{index}"), ty)
                });
            }
            let schema = schema_of(fields);
            let batch = build(&schema, rows, &columns);
            let delivered = delivered(&schema, stream(&batch));
            for (index, column) in columns.iter().enumerate() {
                assert_eq!(&read(&batch, index, column), column, "built column {index}");
                assert_eq!(
                    &read(&delivered, index, column),
                    column,
                    "decoded column {index}"
                );
            }
        });
}
