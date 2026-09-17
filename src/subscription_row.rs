//! Typed subscription rows at the public client boundary.
//!
//! Layer: edges.
//!
//! - **Owns.** Binding one subscription generation to its row metadata and encoding selected
//!   Arrow rows into bounded FlatBuffers frames.
//! - **Depends on.** The runtime Arrow carrier, branch keys, the client wire codec, and vocabulary
//!   schema types.
//! - **Must not know.** Session transport, subscription lifecycle, relay scheduling, persistence,
//!   acknowledgements, or internal relay and interconnect serialization.

use std::{collections::BTreeMap, num::NonZeroUsize};

use arrow_array::{
    Array, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, ListArray, RecordBatch, StringArray, TimestampNanosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType as ArrowDataType;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    CellWriter, EncodedFrame, RowBranch, RowSchema, ServerFrame, SessionLimits, SubscriptionHandle,
    SubscriptionOpened, SubscriptionRowsEncoder, SubscriptionType, WireEncodeError,
};
use nervix_models::{
    BranchName, CreateSchema, DomainName, FieldName, ParseAsType, RelayName, SchemaField, Timestamp,
};
use thiserror::Error;

use crate::{
    runtime::BranchKey,
    runtime_schema::{RuntimeValue, RuntimeValueKind, parse_as_type_from_arrow},
};

/// The declared branch schema needed to describe concrete branch keys to a client.
pub struct SubscriptionBranchSchema<'a> {
    pub name: &'a BranchName,
    pub schema: &'a CreateSchema,
    pub fields: &'a [FieldName],
}

/// Builds the metadata that makes later row frames independently decodable.
pub fn subscription_row_schema(
    payload: &CreateSchema,
    branch: Option<SubscriptionBranchSchema<'_>>,
) -> Result<RowSchema, Report<SubscriptionRowEncodingError>> {
    let branch = match branch {
        Some(branch) => Some(subscription_row_branch(branch)?),
        None => None,
    };
    Ok(RowSchema {
        fields: payload.fields.clone(),
        branch,
    })
}

fn subscription_row_branch(
    branch: SubscriptionBranchSchema<'_>,
) -> Result<RowBranch, Report<SubscriptionRowEncodingError>> {
    let mut schema_fields = BTreeMap::new();
    for field in &branch.schema.fields {
        if schema_fields.insert(field.name.clone(), field).is_some() {
            return Err(Report::new(
                SubscriptionRowEncodingError::DuplicateBranchSchemaField {
                    field: field.name.clone(),
                },
            ));
        }
    }

    let mut selected = BTreeMap::new();
    let mut fields = Vec::with_capacity(branch.fields.len());
    for name in branch.fields {
        if selected.insert(name.clone(), ()).is_some() {
            return Err(Report::new(
                SubscriptionRowEncodingError::DuplicateBranchField {
                    field: name.clone(),
                },
            ));
        }
        let Some(field) = schema_fields.get(name) else {
            return Err(Report::new(
                SubscriptionRowEncodingError::MissingBranchSchemaField {
                    branch: branch.name.clone(),
                    field: name.clone(),
                },
            ));
        };
        fields.push((*field).clone());
    }
    match RowBranch::new(branch.name.clone(), fields) {
        Ok(branch) => Ok(branch),
        Err(error) => Err(
            error.change_context(SubscriptionRowEncodingError::EmptyBranchFields {
                branch: branch.name.clone(),
            }),
        ),
    }
}

/// Which rows of one Arrow batch survived per-record filtering and sampling.
#[derive(Debug, Clone, Copy)]
pub enum SubscriptionRowSelection<'a> {
    All,
    Rows(&'a [usize]),
}

impl SubscriptionRowSelection<'_> {
    fn len(self, batch_rows: usize) -> usize {
        match self {
            Self::All => batch_rows,
            Self::Rows(rows) => rows.len(),
        }
    }

    fn row(self, position: usize) -> usize {
        match self {
            Self::All => position,
            Self::Rows(rows) => rows[position],
        }
    }

    fn validate(self, batch_rows: usize) -> Result<(), Report<SubscriptionRowEncodingError>> {
        let Self::Rows(rows) = self else {
            return Ok(());
        };
        for row in rows {
            if *row >= batch_rows {
                return Err(Report::new(
                    SubscriptionRowEncodingError::SelectedRowOutOfBounds {
                        row: *row,
                        batch_rows,
                    },
                ));
            }
        }
        for pair in rows.windows(2) {
            if pair[0] >= pair[1] {
                return Err(Report::new(
                    SubscriptionRowEncodingError::SelectedRowsNotIncreasing {
                        previous: pair[0],
                        next: pair[1],
                    },
                ));
            }
        }
        Ok(())
    }
}

/// A subscription generation whose opening metadata has not yet been produced.
///
/// Consuming this value with [`Self::open`] produces the one successful-opening metadata value
/// for the generation and the encoder for its later rows. A restored subscription or a new
/// generation constructs a new opening, so metadata can repeat once for that generation without
/// appearing in each data frame.
pub struct SubscriptionRowOpening {
    subscription: SubscriptionHandle,
    schema: RowSchema,
    limits: SessionLimits,
    rows_per_frame: NonZeroUsize,
}

impl SubscriptionRowOpening {
    pub fn new(
        subscription: SubscriptionHandle,
        schema: RowSchema,
        limits: SessionLimits,
        rows_per_frame: NonZeroUsize,
    ) -> Result<Self, Report<SubscriptionRowEncodingError>> {
        if rows_per_frame.get() > limits.collection_entries() {
            return Err(Report::new(
                SubscriptionRowEncodingError::RowsPerFrameAboveCollectionLimit {
                    rows_per_frame: rows_per_frame.get(),
                    collection_entries: limits.collection_entries(),
                },
            ));
        }
        Ok(Self {
            subscription,
            schema,
            limits,
            rows_per_frame,
        })
    }

    /// Produces the metadata a successful creation queues before any frames from the returned
    /// encoder.
    pub fn open(
        self,
        domain: DomainName,
        relay: RelayName,
    ) -> (SubscriptionOpened, SubscriptionRowEncoder) {
        let metadata = SubscriptionOpened {
            subscription: self.subscription.clone(),
            domain,
            relay,
            subscription_type: SubscriptionType::Row,
            schema: self.schema.clone(),
        };
        let encoder = SubscriptionRowEncoder {
            subscription: self.subscription,
            schema: self.schema,
            limits: self.limits,
            rows_per_frame: self.rows_per_frame,
        };
        (metadata, encoder)
    }
}

/// Encodes selected runtime rows for one opened subscription generation.
pub struct SubscriptionRowEncoder {
    subscription: SubscriptionHandle,
    schema: RowSchema,
    limits: SessionLimits,
    rows_per_frame: NonZeroUsize,
}

impl SubscriptionRowEncoder {
    /// Encodes the selected rows directly from `batch`'s Arrow columns.
    ///
    /// `branch_keys` has one entry per Arrow row. Rows with different concrete keys are separated
    /// into different wire batches, while adjacent selected rows with one key share frames up to
    /// the explicit row limit and the session frame-byte limit.
    pub fn encode(
        &self,
        batch: &RecordBatch,
        branch_keys: &[Option<BranchKey>],
        selection: SubscriptionRowSelection<'_>,
    ) -> Result<Vec<EncodedFrame<ServerFrame>>, Report<SubscriptionRowEncodingError>> {
        let columns = ArrowRowBatch::new(&self.schema, batch)?;
        if branch_keys.len() != batch.num_rows() {
            return Err(Report::new(
                SubscriptionRowEncodingError::BranchKeyRowCount {
                    batch_rows: batch.num_rows(),
                    branch_keys: branch_keys.len(),
                },
            ));
        }
        selection.validate(batch.num_rows())?;

        let selected_rows = selection.len(batch.num_rows());
        let mut frames = Vec::new();
        let mut position = 0;
        while position < selected_rows {
            let first_row = selection.row(position);
            let key = &branch_keys[first_row];
            let mut end = position
                .checked_add(1)
                .assured("position is below the selected row count");
            while end < selected_rows {
                let row = selection.row(end);
                if branch_keys[row] != *key {
                    break;
                }
                end = end
                    .checked_add(1)
                    .assured("end remains at most the selected row count");
            }
            let branch =
                ValidatedBranchKey::new(self.schema.branch.as_ref(), key.as_ref(), first_row)?;
            self.encode_one_branch(&columns, &branch, selection, position, end, &mut frames)?;
            position = end;
        }
        Ok(frames)
    }

    fn encode_one_branch(
        &self,
        columns: &ArrowRowBatch<'_>,
        branch: &ValidatedBranchKey<'_>,
        selection: SubscriptionRowSelection<'_>,
        start: usize,
        end: usize,
        frames: &mut Vec<EncodedFrame<ServerFrame>>,
    ) -> Result<(), Report<SubscriptionRowEncodingError>> {
        let first_row = selection.row(start);
        let mut current = self.start_frame(branch, first_row)?;
        for position in start..end {
            let row = selection.row(position);
            if current.rows() == self.rows_per_frame.get() {
                let next = self.start_frame(branch, row)?;
                let full = std::mem::replace(&mut current, next);
                frames.push(self.finish_frame(full)?);
            }

            let result = current.push_row(|cells| columns.write_row(row, cells));
            if let Err(error) = result {
                let frame_full = matches!(
                    error.current_context(),
                    WireEncodeError::FrameTooLarge { .. }
                );
                if frame_full && current.rows() > 0 {
                    let next = self.start_frame(branch, row)?;
                    let full = std::mem::replace(&mut current, next);
                    frames.push(self.finish_frame(full)?);
                    if let Err(error) = current.push_row(|cells| columns.write_row(row, cells)) {
                        return Err(self.row_error(error, row));
                    }
                } else {
                    return Err(self.row_error(error, row));
                }
            }
        }
        if current.rows() > 0 {
            frames.push(self.finish_frame(current)?);
        }
        Ok(())
    }

    fn start_frame(
        &self,
        branch: &ValidatedBranchKey<'_>,
        row: usize,
    ) -> Result<SubscriptionRowsEncoder, Report<SubscriptionRowEncodingError>> {
        match branch {
            ValidatedBranchKey::Unbranched => {
                SubscriptionRowsEncoder::unbranched(self.subscription.clone(), &self.limits)
                    .change_context(SubscriptionRowEncodingError::StartFrame { row })
            }
            ValidatedBranchKey::Branched(cells) => SubscriptionRowsEncoder::branched(
                self.subscription.clone(),
                &self.limits,
                |writer| cells.write(writer),
            )
            .change_context(SubscriptionRowEncodingError::StartFrame { row }),
        }
    }

    fn finish_frame(
        &self,
        frame: SubscriptionRowsEncoder,
    ) -> Result<EncodedFrame<ServerFrame>, Report<SubscriptionRowEncodingError>> {
        frame
            .finish()
            .change_context(SubscriptionRowEncodingError::FinishFrame)
    }

    fn row_error(
        &self,
        error: Report<WireEncodeError>,
        row: usize,
    ) -> Report<SubscriptionRowEncodingError> {
        if let WireEncodeError::FrameTooLarge { .. } = error.current_context() {
            return error.change_context(SubscriptionRowEncodingError::RowExceedsFrame {
                row,
                frame_bytes: self.limits.frame_bytes(),
            });
        }
        error.change_context(SubscriptionRowEncodingError::EncodeRow { row })
    }
}

struct ArrowRowBatch<'a> {
    columns: Vec<ArrowFieldColumn<'a>>,
}

impl<'a> ArrowRowBatch<'a> {
    fn new(
        schema: &'a RowSchema,
        batch: &'a RecordBatch,
    ) -> Result<Self, Report<SubscriptionRowEncodingError>> {
        if schema.fields.len() != batch.num_columns() {
            return Err(Report::new(SubscriptionRowEncodingError::ColumnCount {
                schema_fields: schema.fields.len(),
                arrow_columns: batch.num_columns(),
            }));
        }
        let arrow_fields = batch.schema_ref().fields();
        let mut columns = Vec::with_capacity(schema.fields.len());
        for index in 0..schema.fields.len() {
            let field = &schema.fields[index];
            let arrow_field = arrow_fields
                .get(index)
                .verified("the Arrow batch has one schema field per column");
            if field.name.as_str() != arrow_field.name() {
                return Err(Report::new(SubscriptionRowEncodingError::FieldName {
                    column: index,
                    expected: field.name.clone(),
                    actual: arrow_field.name().clone(),
                }));
            }
            if field.optional != arrow_field.is_nullable() {
                return Err(Report::new(SubscriptionRowEncodingError::Nullability {
                    field: field.name.clone(),
                    expected_nullable: field.optional,
                    actual_nullable: arrow_field.is_nullable(),
                }));
            }
            let array = batch
                .columns()
                .get(index)
                .verified("the Arrow batch reported this column count");
            let values = ArrowColumn::new(&field.name, &field.ty, array.as_ref())?;
            if !field.optional && array.null_count() > 0 {
                return Err(Report::new(
                    SubscriptionRowEncodingError::RequiredFieldNulls {
                        field: field.name.clone(),
                        nulls: array.null_count(),
                    },
                ));
            }
            columns.push(ArrowFieldColumn { field, values });
        }
        Ok(Self { columns })
    }

    fn write_row(
        &self,
        row: usize,
        writer: &mut CellWriter<'_, 'static>,
    ) -> Result<(), Report<WireEncodeError>> {
        for column in &self.columns {
            if column.field.sensitive {
                writer.push_redacted()?;
            } else {
                column.values.write(row, column.field.optional, writer)?;
            }
        }
        Ok(())
    }
}

struct ArrowFieldColumn<'a> {
    field: &'a SchemaField,
    values: ArrowColumn<'a>,
}

enum ArrowColumn<'a> {
    U8(&'a UInt8Array),
    I8(&'a Int8Array),
    U16(&'a UInt16Array),
    I16(&'a Int16Array),
    U32(&'a UInt32Array),
    I32(&'a Int32Array),
    U64(&'a UInt64Array),
    I64(&'a Int64Array),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    Bool(&'a BooleanArray),
    String(&'a StringArray),
    Datetime(&'a TimestampNanosecondArray),
    FixedList {
        array: &'a FixedSizeListArray,
        element: Box<ArrowColumn<'a>>,
    },
    List {
        array: &'a ListArray,
        element: Box<ArrowColumn<'a>>,
    },
}

impl<'a> ArrowColumn<'a> {
    fn new(
        field: &FieldName,
        expected: &ParseAsType,
        array: &'a dyn Array,
    ) -> Result<Self, Report<SubscriptionRowEncodingError>> {
        let actual = match parse_as_type_from_arrow(array.data_type()) {
            Ok(actual) => actual,
            Err(error) => {
                return Err(error.change_context(
                    SubscriptionRowEncodingError::UnsupportedArrowType {
                        field: field.clone(),
                        data_type: array.data_type().clone(),
                    },
                ));
            }
        };
        if &actual != expected {
            return Err(Report::new(SubscriptionRowEncodingError::ColumnType {
                field: field.clone(),
                expected: expected.clone(),
                actual: array.data_type().clone(),
            }));
        }
        Ok(match expected {
            ParseAsType::U8 => Self::U8(downcast(array)),
            ParseAsType::I8 => Self::I8(downcast(array)),
            ParseAsType::U16 => Self::U16(downcast(array)),
            ParseAsType::I16 => Self::I16(downcast(array)),
            ParseAsType::U32 => Self::U32(downcast(array)),
            ParseAsType::I32 => Self::I32(downcast(array)),
            ParseAsType::U64 => Self::U64(downcast(array)),
            ParseAsType::I64 => Self::I64(downcast(array)),
            ParseAsType::F32 => Self::F32(downcast(array)),
            ParseAsType::F64 => Self::F64(downcast(array)),
            ParseAsType::Bool => Self::Bool(downcast(array)),
            ParseAsType::String => Self::String(downcast(array)),
            ParseAsType::Datetime => Self::Datetime(downcast(array)),
            ParseAsType::Array { element, .. } => {
                let array: &FixedSizeListArray = downcast(array);
                let element_nullable = match array.data_type() {
                    ArrowDataType::FixedSizeList(element, _) => element.is_nullable(),
                    _ => false,
                };
                Self::list_element(field, element_nullable, array.values().as_ref())?;
                Self::FixedList {
                    array,
                    element: Box::new(Self::new(field, element, array.values().as_ref())?),
                }
            }
            ParseAsType::Vec { element } => {
                let array: &ListArray = downcast(array);
                let element_nullable = match array.data_type() {
                    ArrowDataType::List(element) => element.is_nullable(),
                    _ => false,
                };
                Self::list_element(field, element_nullable, array.values().as_ref())?;
                Self::List {
                    array,
                    element: Box::new(Self::new(field, element, array.values().as_ref())?),
                }
            }
        })
    }

    fn list_element(
        field: &FieldName,
        nullable: bool,
        values: &dyn Array,
    ) -> Result<(), Report<SubscriptionRowEncodingError>> {
        if nullable {
            return Err(Report::new(
                SubscriptionRowEncodingError::NullableListElements {
                    field: field.clone(),
                },
            ));
        }
        if values.null_count() > 0 {
            return Err(Report::new(
                SubscriptionRowEncodingError::ListElementNulls {
                    field: field.clone(),
                    nulls: values.null_count(),
                },
            ));
        }
        Ok(())
    }

    fn is_null(&self, index: usize) -> bool {
        match self {
            Self::U8(array) => array.is_null(index),
            Self::I8(array) => array.is_null(index),
            Self::U16(array) => array.is_null(index),
            Self::I16(array) => array.is_null(index),
            Self::U32(array) => array.is_null(index),
            Self::I32(array) => array.is_null(index),
            Self::U64(array) => array.is_null(index),
            Self::I64(array) => array.is_null(index),
            Self::F32(array) => array.is_null(index),
            Self::F64(array) => array.is_null(index),
            Self::Bool(array) => array.is_null(index),
            Self::String(array) => array.is_null(index),
            Self::Datetime(array) => array.is_null(index),
            Self::FixedList { array, .. } => array.is_null(index),
            Self::List { array, .. } => array.is_null(index),
        }
    }

    fn write(
        &self,
        index: usize,
        nullable: bool,
        writer: &mut CellWriter<'_, 'static>,
    ) -> Result<(), Report<WireEncodeError>> {
        if self.is_null(index) {
            nullable
                .then_some(())
                .verified("required columns were checked for nulls before encoding");
            return writer.push_null();
        }
        self.write_present(index, writer)
    }

    fn write_required(
        &self,
        index: usize,
        writer: &mut CellWriter<'_, 'static>,
    ) -> Result<(), Report<WireEncodeError>> {
        (!self.is_null(index))
            .then_some(())
            .verified("list child columns were checked for nulls before encoding");
        self.write_present(index, writer)
    }

    fn write_present(
        &self,
        index: usize,
        writer: &mut CellWriter<'_, 'static>,
    ) -> Result<(), Report<WireEncodeError>> {
        match self {
            Self::U8(array) => writer.push_u8(array.value(index)),
            Self::I8(array) => writer.push_i8(array.value(index)),
            Self::U16(array) => writer.push_u16(array.value(index)),
            Self::I16(array) => writer.push_i16(array.value(index)),
            Self::U32(array) => writer.push_u32(array.value(index)),
            Self::I32(array) => writer.push_i32(array.value(index)),
            Self::U64(array) => writer.push_u64(array.value(index)),
            Self::I64(array) => writer.push_i64(array.value(index)),
            Self::F32(array) => writer.push_f32(array.value(index)),
            Self::F64(array) => writer.push_f64(array.value(index)),
            Self::Bool(array) => writer.push_bool(array.value(index)),
            Self::String(array) => writer.push_string(array.value(index)),
            Self::Datetime(array) => {
                writer.push_datetime(Timestamp::from_unix_nanos(array.value(index)))
            }
            Self::FixedList { array, element } => {
                let start = usize::try_from(array.value_offset(index))
                    .verified("Arrow validates fixed-list offsets as nonnegative");
                let length = usize::try_from(array.value_length())
                    .verified("the schema admitted a positive fixed-list length");
                let end = start
                    .checked_add(length)
                    .verified("Arrow validates fixed-list offsets within the child array");
                writer.push_list(|writer| {
                    for element_index in start..end {
                        element.write_required(element_index, writer)?;
                    }
                    Ok(())
                })
            }
            Self::List { array, element } => {
                let offsets = array.value_offsets();
                let start = usize::try_from(offsets[index])
                    .verified("Arrow validates list offsets as nonnegative");
                let end = usize::try_from(offsets[index + 1])
                    .verified("Arrow validates list offsets as nonnegative");
                writer.push_list(|writer| {
                    for element_index in start..end {
                        element.write_required(element_index, writer)?;
                    }
                    Ok(())
                })
            }
        }
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> &T {
    array
        .as_any()
        .downcast_ref::<T>()
        .verified("the exact Arrow data type was checked before downcasting")
}

enum ValidatedBranchKey<'a> {
    Unbranched,
    Branched(BranchCells<'a>),
}

impl<'a> ValidatedBranchKey<'a> {
    fn new(
        schema: Option<&'a RowBranch>,
        key: Option<&'a BranchKey>,
        row: usize,
    ) -> Result<Self, Report<SubscriptionRowEncodingError>> {
        let (schema, key) = match (schema, key) {
            (None, None) => return Ok(Self::Unbranched),
            (None, Some(_)) => {
                return Err(Report::new(
                    SubscriptionRowEncodingError::UnexpectedBranchKey { row },
                ));
            }
            (Some(_), None) => {
                return Err(Report::new(
                    SubscriptionRowEncodingError::MissingBranchKey { row },
                ));
            }
            (Some(schema), Some(key)) => (schema, key),
        };
        if key.field_count() != schema.fields().len() {
            return Err(Report::new(
                SubscriptionRowEncodingError::BranchFieldCount {
                    row,
                    expected: schema.fields().len(),
                    actual: key.field_count(),
                },
            ));
        }
        let mut cells = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let Some(value) = key.field_value(field.name.as_str()) else {
                return Err(Report::new(
                    SubscriptionRowEncodingError::MissingBranchField {
                        row,
                        field: field.name.clone(),
                    },
                ));
            };
            validate_runtime_value(row, &field.name, value, &field.ty)?;
            cells.push(BranchCell { field, value });
        }
        Ok(Self::Branched(BranchCells { cells }))
    }
}

struct BranchCells<'a> {
    cells: Vec<BranchCell<'a>>,
}

impl BranchCells<'_> {
    fn write(&self, writer: &mut CellWriter<'_, 'static>) -> Result<(), Report<WireEncodeError>> {
        for cell in &self.cells {
            if cell.field.sensitive {
                writer.push_redacted()?;
            } else {
                write_runtime_value(cell.value, writer)?;
            }
        }
        Ok(())
    }
}

struct BranchCell<'a> {
    field: &'a SchemaField,
    value: &'a RuntimeValue,
}

fn validate_runtime_value(
    row: usize,
    field: &FieldName,
    value: &RuntimeValue,
    expected: &ParseAsType,
) -> Result<(), Report<SubscriptionRowEncodingError>> {
    let elements = match (value, expected) {
        (RuntimeValue::U8(_), ParseAsType::U8)
        | (RuntimeValue::I8(_), ParseAsType::I8)
        | (RuntimeValue::U16(_), ParseAsType::U16)
        | (RuntimeValue::I16(_), ParseAsType::I16)
        | (RuntimeValue::U32(_), ParseAsType::U32)
        | (RuntimeValue::I32(_), ParseAsType::I32)
        | (RuntimeValue::U64(_), ParseAsType::U64)
        | (RuntimeValue::I64(_), ParseAsType::I64)
        | (RuntimeValue::F32(_), ParseAsType::F32)
        | (RuntimeValue::F64(_), ParseAsType::F64)
        | (RuntimeValue::Bool(_), ParseAsType::Bool)
        | (RuntimeValue::String(_), ParseAsType::String)
        | (RuntimeValue::Datetime(_), ParseAsType::Datetime) => return Ok(()),
        (RuntimeValue::Array(elements), ParseAsType::Array { element, len }) => {
            let expected_length = usize::try_from(len.get())
                .assured("a u32 fixed-list length fits every supported target");
            if elements.len() != expected_length {
                return Err(Report::new(
                    SubscriptionRowEncodingError::BranchArrayLength {
                        row,
                        field: field.clone(),
                        expected: len.get(),
                        actual: elements.len(),
                    },
                ));
            }
            (elements, element.as_ref())
        }
        (RuntimeValue::Vec(elements), ParseAsType::Vec { element }) => (elements, element.as_ref()),
        (value, expected) => {
            return Err(Report::new(SubscriptionRowEncodingError::BranchFieldType {
                row,
                field: field.clone(),
                expected: expected.clone(),
                actual: value.kind(),
            }));
        }
    };
    for element in elements.0 {
        validate_runtime_value(row, field, element, elements.1)?;
    }
    Ok(())
}

fn write_runtime_value(
    value: &RuntimeValue,
    writer: &mut CellWriter<'_, 'static>,
) -> Result<(), Report<WireEncodeError>> {
    match value {
        RuntimeValue::U8(value) => writer.push_u8(*value),
        RuntimeValue::I8(value) => writer.push_i8(*value),
        RuntimeValue::U16(value) => writer.push_u16(*value),
        RuntimeValue::I16(value) => writer.push_i16(*value),
        RuntimeValue::U32(value) => writer.push_u32(*value),
        RuntimeValue::I32(value) => writer.push_i32(*value),
        RuntimeValue::U64(value) => writer.push_u64(*value),
        RuntimeValue::I64(value) => writer.push_i64(*value),
        RuntimeValue::F32(value) => writer.push_f32(value.0),
        RuntimeValue::F64(value) => writer.push_f64(value.0),
        RuntimeValue::Bool(value) => writer.push_bool(*value),
        RuntimeValue::String(value) => writer.push_string(value),
        RuntimeValue::Datetime(value) => {
            let unix_nanos = value.timestamp_nanos_opt().verified(
                "branch datetimes came from a nanosecond Arrow value or validated literal",
            );
            writer.push_datetime(Timestamp::from_unix_nanos(unix_nanos))
        }
        RuntimeValue::Array(values) | RuntimeValue::Vec(values) => writer.push_list(|writer| {
            for value in values {
                write_runtime_value(value, writer)?;
            }
            Ok(())
        }),
    }
}

/// Reproducible comparison with the current per-record protobuf/keyed-JSON construction.
#[cfg(feature = "benchmarks")]
pub mod benchmark {
    use std::sync::Arc as StdArc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use nervix_client_wire::{EncodedFrame, RowBranch, ServerFrame};
    use nervix_models::SubscriptionName;
    use prost::Message as _;

    use super::*;
    use crate::runtime_schema::RuntimeRecordBatch;

    /// Fixed Arrow input used to compare typed Row encoding with the outgoing JSON it replaces.
    pub struct SubscriptionRowBenchmark {
        batch: RuntimeRecordBatch,
        keys: Vec<Option<BranchKey>>,
        encoder: SubscriptionRowEncoder,
    }

    impl SubscriptionRowBenchmark {
        /// Builds the task 01 subscription workload with alternating concrete branches.
        pub fn new(rows: NonZeroUsize, detail_bytes: NonZeroUsize) -> Self {
            let detail = "x".repeat(detail_bytes.get());
            let tenants = StringArray::from_iter_values((0..rows.get()).map(|index| {
                if index.is_multiple_of(2) {
                    "acme"
                } else {
                    "beta"
                }
            }));
            let sequence = (0..rows.get())
                .map(|index| i64::try_from(index).assured("the in-memory row index fits i64"))
                .collect::<Int64Array>();
            let details =
                StringArray::from_iter_values(std::iter::repeat_n(detail.as_str(), rows.get()));
            let arrow_schema = StdArc::new(ArrowSchema::new(vec![
                ArrowField::new("tenant", ArrowDataType::Utf8, false),
                ArrowField::new("sequence", ArrowDataType::Int64, false),
                ArrowField::new("detail", ArrowDataType::Utf8, false),
            ]));
            let columns: Vec<ArrayRef> = vec![
                StdArc::new(tenants),
                StdArc::new(sequence),
                StdArc::new(details),
            ];
            let record_batch = RecordBatch::try_new(StdArc::clone(&arrow_schema), columns)
                .assured("the benchmark columns follow the fixed Arrow schema");
            let batch = RuntimeRecordBatch::from_record_batch(arrow_schema, record_batch)
                .assured("the benchmark wrapper receives its exact Arrow schema");

            let tenant: FieldName = named("tenant");
            let branch = RowBranch::new(
                named("by_tenant"),
                vec![SchemaField {
                    name: tenant.clone(),
                    ty: ParseAsType::String,
                    optional: false,
                    sensitive: false,
                }],
            )
            .assured("the benchmark branch has one field");
            let schema = RowSchema {
                fields: vec![
                    SchemaField {
                        name: named("tenant"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    },
                    SchemaField {
                        name: named("sequence"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    },
                    SchemaField {
                        name: named("detail"),
                        ty: ParseAsType::String,
                        optional: false,
                        sensitive: false,
                    },
                ],
                branch: Some(branch),
            };
            let acme = BranchKey::from_fields([(
                tenant.clone(),
                RuntimeValue::String("acme".to_string()),
            )])
            .assured("the benchmark branch key is nonempty");
            let beta = BranchKey::from_fields([(tenant, RuntimeValue::String("beta".to_string()))])
                .assured("the benchmark branch key is nonempty");
            let keys = (0..rows.get())
                .map(|index| {
                    if index.is_multiple_of(2) {
                        Some(acme.clone())
                    } else {
                        Some(beta.clone())
                    }
                })
                .collect();
            let opening = SubscriptionRowOpening::new(
                SubscriptionHandle {
                    name: named::<SubscriptionName>("baseline"),
                    generation: std::num::NonZeroU64::new(1)
                        .assured("the benchmark generation is nonzero"),
                },
                schema,
                SessionLimits::DEFAULT,
                rows,
            )
            .assured("the benchmark row count fits the default collection limit");
            let (_, encoder) = opening.open(named("baseline"), named("events"));
            Self {
                batch,
                keys,
                encoder,
            }
        }

        /// Encodes the whole Arrow batch through the typed Row path.
        pub fn encode_typed_rows(&self) -> Vec<EncodedFrame<ServerFrame>> {
            self.encoder
                .encode(
                    self.batch.batch(),
                    &self.keys,
                    SubscriptionRowSelection::All,
                )
                .assured("the fixed benchmark rows encode")
        }

        /// Encodes the current keyed JSON subscription response for the same rows.
        pub fn encode_protobuf_json_rows(&self) -> Vec<Vec<u8>> {
            let mut encoded = Vec::with_capacity(self.batch.batch().num_rows());
            for row in 0..self.batch.batch().num_rows() {
                let payload = self
                    .batch
                    .row_to_json_string(row)
                    .assured("the fixed benchmark row serializes to JSON");
                let key = self.keys[row]
                    .as_ref()
                    .verified("the benchmark gives every row a branch key");
                let payload = format!("key={} payload={payload}", key.as_str());
                let response = crate::proto::SessionResponse {
                    event: Some(crate::proto::session_response::Event::Subscription(
                        crate::proto::SubscriptionEvent {
                            subscription: "baseline".to_string(),
                            relay: "events".to_string(),
                            payload,
                        },
                    )),
                };
                encoded.push(response.encode_to_vec());
            }
            encoded
        }
    }

    fn named<N>(raw: &str) -> N
    where
        N: TryFrom<String>,
        N::Error: std::fmt::Debug,
    {
        N::try_from(raw.to_string()).assured("the fixed benchmark identifier is valid")
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn reports_typed_row_and_json_allocation_evidence() {
            let rows = NonZeroUsize::new(100).assured("the evidence row count is nonzero");
            let detail_bytes =
                NonZeroUsize::new(1_024).assured("the evidence detail width is nonzero");
            let benchmark = SubscriptionRowBenchmark::new(rows, detail_bytes);

            let (typed_allocations, typed) =
                alloc_count::alloc_count!({ benchmark.encode_typed_rows() });
            let (json_allocations, json) =
                alloc_count::alloc_count!({ benchmark.encode_protobuf_json_rows() });
            let typed_calls = typed_allocations
                .alloc_calls
                .checked_add(typed_allocations.realloc_calls)
                .assured("allocation statistics fit usize");
            let json_calls = json_allocations
                .alloc_calls
                .checked_add(json_allocations.realloc_calls)
                .assured("allocation statistics fit usize");
            let typed_requested_bytes = typed_allocations
                .bytes_allocated
                .checked_add(typed_allocations.bytes_reallocated)
                .assured("allocation byte statistics fit usize");
            let json_requested_bytes = json_allocations
                .bytes_allocated
                .checked_add(json_allocations.bytes_reallocated)
                .assured("allocation byte statistics fit usize");
            let typed_bytes = typed.iter().fold(0_usize, |total, frame| {
                total
                    .checked_add(frame.bytes().len())
                    .assured("the evidence output is bounded by in-memory frame vectors")
            });
            let json_bytes = json.iter().fold(0_usize, |total, row| {
                total
                    .checked_add(row.len())
                    .assured("the evidence output is bounded by in-memory string vectors")
            });

            eprintln!(
                "subscription_row_allocations typed_calls={typed_calls} \
                 typed_requested_bytes={typed_requested_bytes} typed_wire_bytes={typed_bytes} \
                 json_calls={json_calls} json_requested_bytes={json_requested_bytes} \
                 json_text_bytes={json_bytes}",
            );
            assert!(typed_calls > 0);
            assert!(json_calls > 0);
            assert!(typed_requested_bytes > 0);
            assert!(json_requested_bytes > 0);
            assert!(
                typed_requested_bytes < json_requested_bytes,
                "direct typed encoding should request fewer allocator bytes than protobuf/keyed \
                 JSON"
            );
        }
    }
}

/// Why an Arrow subscription batch could not become typed Row frames.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SubscriptionRowEncodingError {
    #[error("branch schema contains duplicate field `{field}`")]
    DuplicateBranchSchemaField { field: FieldName },
    #[error("branch declaration repeats field `{field}`")]
    DuplicateBranchField { field: FieldName },
    #[error("branch `{branch}` references missing schema field `{field}`")]
    MissingBranchSchemaField {
        branch: BranchName,
        field: FieldName,
    },
    #[error("branch `{branch}` has no key fields")]
    EmptyBranchFields { branch: BranchName },
    #[error("row limit {rows_per_frame} exceeds the session collection limit {collection_entries}")]
    RowsPerFrameAboveCollectionLimit {
        rows_per_frame: usize,
        collection_entries: usize,
    },
    #[error("row schema has {schema_fields} fields, but Arrow has {arrow_columns} columns")]
    ColumnCount {
        schema_fields: usize,
        arrow_columns: usize,
    },
    #[error("Arrow column {column} is `{actual}`, expected field `{expected}`")]
    FieldName {
        column: usize,
        expected: FieldName,
        actual: String,
    },
    #[error(
        "field `{field}` nullable={actual_nullable} in Arrow, expected \
         nullable={expected_nullable}"
    )]
    Nullability {
        field: FieldName,
        expected_nullable: bool,
        actual_nullable: bool,
    },
    #[error("field `{field}` uses unsupported Arrow type {data_type}")]
    UnsupportedArrowType {
        field: FieldName,
        data_type: ArrowDataType,
    },
    #[error("field `{field}` expects {expected}, but Arrow has {actual}")]
    ColumnType {
        field: FieldName,
        expected: ParseAsType,
        actual: ArrowDataType,
    },
    #[error("required field `{field}` contains {nulls} null values")]
    RequiredFieldNulls { field: FieldName, nulls: usize },
    #[error("list field `{field}` contains {nulls} null elements")]
    ListElementNulls { field: FieldName, nulls: usize },
    #[error("list field `{field}` declares nullable elements")]
    NullableListElements { field: FieldName },
    #[error("Arrow batch has {batch_rows} rows, but {branch_keys} branch keys")]
    BranchKeyRowCount {
        batch_rows: usize,
        branch_keys: usize,
    },
    #[error("selected row {row} is outside an Arrow batch with {batch_rows} rows")]
    SelectedRowOutOfBounds { row: usize, batch_rows: usize },
    #[error("selected rows are not increasing at {previous} followed by {next}")]
    SelectedRowsNotIncreasing { previous: usize, next: usize },
    #[error("row {row} has a branch key for an unbranched subscription")]
    UnexpectedBranchKey { row: usize },
    #[error("row {row} has no branch key for a branched subscription")]
    MissingBranchKey { row: usize },
    #[error("row {row} branch key has {actual} fields, expected {expected}")]
    BranchFieldCount {
        row: usize,
        expected: usize,
        actual: usize,
    },
    #[error("row {row} branch key is missing field `{field}`")]
    MissingBranchField { row: usize, field: FieldName },
    #[error("row {row} branch field `{field}` expects {expected}, found {actual}")]
    BranchFieldType {
        row: usize,
        field: FieldName,
        expected: ParseAsType,
        actual: RuntimeValueKind,
    },
    #[error(
        "row {row} branch field `{field}` has {actual} elements, expected fixed length {expected}"
    )]
    BranchArrayLength {
        row: usize,
        field: FieldName,
        expected: u32,
        actual: usize,
    },
    #[error("failed to start a Row frame at Arrow row {row}")]
    StartFrame { row: usize },
    #[error("Arrow row {row} exceeds the Row frame limit of {frame_bytes} bytes")]
    RowExceedsFrame { row: usize, frame_bytes: usize },
    #[error("failed to encode Arrow row {row}")]
    EncodeRow { row: usize },
    #[error("failed to finish a Row frame")]
    FinishFrame,
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU32, NonZeroUsize},
        sync::Arc as StdArc,
    };

    use arrow_array::{ArrayRef, BinaryArray, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_client_wire::{
        CellView, ServerEvent, ServerMessage, SessionLimitSettings, SessionLimits,
        SubscriptionHandle, SubscriptionType,
    };
    use nervix_models::{
        BranchName, CreateSchema, DomainName, FieldName, ParseAsType, RelayName, SchemaField,
        Timestamp,
    };

    use super::{
        SubscriptionBranchSchema, SubscriptionRowEncoder, SubscriptionRowEncodingError,
        SubscriptionRowOpening, SubscriptionRowSelection, subscription_row_schema,
    };
    use crate::{
        runtime::BranchKey,
        runtime_schema::{RuntimeRecordBatch, RuntimeValue, compile_schema},
    };

    fn named<N>(raw: &str) -> N
    where
        N: TryFrom<String>,
        N::Error: std::fmt::Debug,
    {
        N::try_from(raw.to_string()).assured("the test identifier is valid")
    }

    fn field(name: &str, ty: ParseAsType, optional: bool, sensitive: bool) -> SchemaField {
        SchemaField {
            name: named(name),
            ty,
            optional,
            sensitive,
        }
    }

    fn payload_schema() -> CreateSchema {
        CreateSchema {
            name: named("events"),
            fields: vec![
                field("sequence", ParseAsType::I64, false, false),
                field("detail", ParseAsType::String, true, false),
                field("secret", ParseAsType::String, false, true),
                field(
                    "bytes",
                    ParseAsType::Vec {
                        element: Box::new(ParseAsType::U8),
                    },
                    false,
                    false,
                ),
                field("at", ParseAsType::Datetime, false, false),
                field("ratio", ParseAsType::F32, false, false),
            ],
        }
    }

    fn branch_schema() -> CreateSchema {
        CreateSchema {
            name: named("tenant_key"),
            fields: vec![
                field("tenant", ParseAsType::String, false, false),
                field("shard", ParseAsType::U16, false, false),
                field("credential", ParseAsType::String, false, true),
            ],
        }
    }

    fn handle(generation: u64) -> SubscriptionHandle {
        SubscriptionHandle {
            name: named("live"),
            generation: std::num::NonZeroU64::new(generation)
                .assured("the test generation is nonzero"),
        }
    }

    fn opening(
        schema: nervix_client_wire::RowSchema,
        rows_per_frame: usize,
    ) -> SubscriptionRowOpening {
        SubscriptionRowOpening::new(
            handle(7),
            schema,
            SessionLimits::DEFAULT,
            NonZeroUsize::new(rows_per_frame).assured("the test row limit is nonzero"),
        )
        .assured("the test row limit fits the session collection limit")
    }

    fn open(
        schema: nervix_client_wire::RowSchema,
        rows_per_frame: usize,
    ) -> (
        nervix_client_wire::SubscriptionOpened,
        SubscriptionRowEncoder,
    ) {
        opening(schema, rows_per_frame).open(named("tenant"), named("events"))
    }

    fn rows_from_frame(
        frame: nervix_client_wire::EncodedFrame<nervix_client_wire::ServerFrame>,
    ) -> nervix_client_wire::SubscriptionRows {
        rows_from_frame_under(frame, &SessionLimits::DEFAULT)
    }

    fn rows_from_frame_under(
        frame: nervix_client_wire::EncodedFrame<nervix_client_wire::ServerFrame>,
        limits: &SessionLimits,
    ) -> nervix_client_wire::SubscriptionRows {
        let frame = frame
            .verify(limits)
            .assured("the encoder emits a verified frame");
        let ServerMessage::Event(ServerEvent::SubscriptionRows(rows)) =
            ServerMessage::decode(&frame).assured("the encoded frame decodes")
        else {
            panic!("the encoder emitted a non-row event");
        };
        rows
    }

    fn size(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).assured("the test limit is nonzero")
    }

    fn runtime_batch(schema: &CreateSchema) -> RuntimeRecordBatch {
        compile_schema(schema)
            .batch_from_test_rows([
                vec![
                    ("sequence".to_string(), RuntimeValue::I64(i64::MIN)),
                    (
                        "secret".to_string(),
                        RuntimeValue::String("first secret".to_string()),
                    ),
                    (
                        "bytes".to_string(),
                        RuntimeValue::Vec(vec![RuntimeValue::U8(0), RuntimeValue::U8(u8::MAX)]),
                    ),
                    (
                        "at".to_string(),
                        RuntimeValue::Datetime(
                            Timestamp::from_unix_nanos(i64::MIN)
                                .into_datetime()
                                .fixed_offset(),
                        ),
                    ),
                    ("ratio".to_string(), RuntimeValue::F32((-0.0_f32).into())),
                ],
                vec![
                    ("sequence".to_string(), RuntimeValue::I64(i64::MAX)),
                    (
                        "detail".to_string(),
                        RuntimeValue::String("present".to_string()),
                    ),
                    (
                        "secret".to_string(),
                        RuntimeValue::String("second secret".to_string()),
                    ),
                    (
                        "bytes".to_string(),
                        RuntimeValue::Vec(vec![RuntimeValue::U8(1), RuntimeValue::U8(2)]),
                    ),
                    (
                        "at".to_string(),
                        RuntimeValue::Datetime(
                            Timestamp::from_unix_nanos(i64::MAX)
                                .into_datetime()
                                .fixed_offset(),
                        ),
                    ),
                    (
                        "ratio".to_string(),
                        RuntimeValue::F32(f32::from_bits(0x7fc0_0001).into()),
                    ),
                ],
            ])
            .assured("the test rows follow their schema")
    }

    fn branch_key(tenant: &str) -> BranchKey {
        BranchKey::from_fields([
            (named("tenant"), RuntimeValue::String(tenant.to_string())),
            (named("shard"), RuntimeValue::U16(17)),
            (
                named("credential"),
                RuntimeValue::String("withheld".to_string()),
            ),
        ])
        .assured("the test branch key is nonempty")
    }

    #[test]
    fn opening_metadata_precedes_exact_arrow_rows_for_one_generation() {
        let payload = payload_schema();
        let branch = branch_schema();
        let branch_name: BranchName = named("by_tenant");
        let branch_fields: Vec<FieldName> = ["tenant", "shard", "credential"]
            .into_iter()
            .map(named)
            .collect();
        let schema = subscription_row_schema(
            &payload,
            Some(SubscriptionBranchSchema {
                name: &branch_name,
                schema: &branch,
                fields: &branch_fields,
            }),
        )
        .assured("the branch fields exist");
        let (opened, encoder) = open(schema.clone(), 8);

        assert_eq!(opened.subscription, handle(7));
        assert_eq!(opened.domain, named::<DomainName>("tenant"));
        assert_eq!(opened.relay, named::<RelayName>("events"));
        assert_eq!(opened.subscription_type, SubscriptionType::Row);
        assert_eq!(opened.schema, schema);

        let batch = runtime_batch(&payload);
        let key = branch_key("acme");
        let keys = vec![Some(key); batch.batch().num_rows()];
        let frames = encoder
            .encode(batch.batch(), &keys, SubscriptionRowSelection::All)
            .assured("the Arrow rows encode");
        assert_eq!(frames.len(), 1);

        let rows = rows_from_frame(frames.into_iter().next().assured("one frame was encoded"));
        assert_eq!(rows.subscription(), &handle(7));
        rows.batch()
            .conform(&opened.schema)
            .assured("encoded rows follow the announced metadata");
        let key = rows
            .batch()
            .branch_key()
            .assured("the batch is branched")
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(
            key,
            [
                CellView::String("acme"),
                CellView::U16(17),
                CellView::Redacted,
            ]
        );

        let first = rows
            .batch()
            .row(0)
            .assured("the first row exists")
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(first[0], CellView::I64(i64::MIN));
        assert_eq!(first[1], CellView::Null);
        assert_eq!(first[2], CellView::Redacted);
        let CellView::List(bytes) = first[3] else {
            panic!("the byte vector is a typed list");
        };
        assert_eq!(
            bytes.iter().collect::<Vec<_>>(),
            [CellView::U8(0), CellView::U8(u8::MAX)]
        );
        assert_eq!(
            first[4],
            CellView::Datetime(Timestamp::from_unix_nanos(i64::MIN))
        );
        let CellView::F32(negative_zero) = first[5] else {
            panic!("the ratio remains F32");
        };
        assert!(negative_zero.is_sign_negative());

        let second = rows
            .batch()
            .row(1)
            .assured("the second row exists")
            .iter()
            .collect::<Vec<_>>();
        let CellView::F32(nan) = second[5] else {
            panic!("the ratio remains F32");
        };
        assert_eq!(nan.to_bits(), 0x7fc0_0001);

        let encoded = rows.frame().bytes();
        assert!(
            !encoded
                .windows("sequence".len())
                .any(|window| window == b"sequence"),
            "row frames must not repeat field names"
        );
    }

    #[test]
    fn every_arrow_value_kind_keeps_its_exact_wire_type() {
        let fixed_length = NonZeroU32::new(2).assured("the fixed-list length is nonzero");
        let schema = CreateSchema {
            name: named("all_types"),
            fields: vec![
                field("u8_value", ParseAsType::U8, false, false),
                field("i8_value", ParseAsType::I8, false, false),
                field("u16_value", ParseAsType::U16, false, false),
                field("i16_value", ParseAsType::I16, false, false),
                field("u32_value", ParseAsType::U32, false, false),
                field("i32_value", ParseAsType::I32, false, false),
                field("u64_value", ParseAsType::U64, false, false),
                field("i64_value", ParseAsType::I64, false, false),
                field("f32_value", ParseAsType::F32, false, false),
                field("f64_value", ParseAsType::F64, false, false),
                field("bool_value", ParseAsType::Bool, false, false),
                field("string_value", ParseAsType::String, false, false),
                field("datetime_value", ParseAsType::Datetime, false, false),
                field(
                    "array_value",
                    ParseAsType::Array {
                        element: Box::new(ParseAsType::I16),
                        len: fixed_length,
                    },
                    false,
                    false,
                ),
                field(
                    "vec_value",
                    ParseAsType::Vec {
                        element: Box::new(ParseAsType::U32),
                    },
                    false,
                    false,
                ),
            ],
        };
        let f32_bits = 0x7fc0_0021;
        let f64_bits = 0x7ff8_0000_0000_0042;
        let batch = compile_schema(&schema)
            .batch_from_test_rows([vec![
                ("u8_value".to_string(), RuntimeValue::U8(u8::MAX)),
                ("i8_value".to_string(), RuntimeValue::I8(i8::MIN)),
                ("u16_value".to_string(), RuntimeValue::U16(u16::MAX)),
                ("i16_value".to_string(), RuntimeValue::I16(i16::MIN)),
                ("u32_value".to_string(), RuntimeValue::U32(u32::MAX)),
                ("i32_value".to_string(), RuntimeValue::I32(i32::MIN)),
                ("u64_value".to_string(), RuntimeValue::U64(u64::MAX)),
                ("i64_value".to_string(), RuntimeValue::I64(i64::MIN)),
                (
                    "f32_value".to_string(),
                    RuntimeValue::F32(f32::from_bits(f32_bits).into()),
                ),
                (
                    "f64_value".to_string(),
                    RuntimeValue::F64(f64::from_bits(f64_bits).into()),
                ),
                ("bool_value".to_string(), RuntimeValue::Bool(true)),
                (
                    "string_value".to_string(),
                    RuntimeValue::String("typed".to_string()),
                ),
                (
                    "datetime_value".to_string(),
                    RuntimeValue::Datetime(
                        Timestamp::from_unix_nanos(i64::MAX)
                            .into_datetime()
                            .fixed_offset(),
                    ),
                ),
                (
                    "array_value".to_string(),
                    RuntimeValue::Array(vec![
                        RuntimeValue::I16(i16::MIN),
                        RuntimeValue::I16(i16::MAX),
                    ]),
                ),
                (
                    "vec_value".to_string(),
                    RuntimeValue::Vec(vec![RuntimeValue::U32(0), RuntimeValue::U32(u32::MAX)]),
                ),
            ]])
            .assured("the test row follows the all-types schema");
        let wire_schema = subscription_row_schema(&schema, None)
            .assured("an unbranched schema needs no branch metadata");
        let (_, encoder) = open(wire_schema, 8);
        let frames = encoder
            .encode(batch.batch(), &[None], SubscriptionRowSelection::All)
            .assured("every supported Arrow value encodes");
        let rows = rows_from_frame(
            frames
                .into_iter()
                .next()
                .assured("one row produces one frame"),
        );
        let row = rows.batch().row(0).assured("the frame has one row");
        let values = row.iter().collect::<Vec<_>>();

        assert_eq!(values[0], CellView::U8(u8::MAX));
        assert_eq!(values[1], CellView::I8(i8::MIN));
        assert_eq!(values[2], CellView::U16(u16::MAX));
        assert_eq!(values[3], CellView::I16(i16::MIN));
        assert_eq!(values[4], CellView::U32(u32::MAX));
        assert_eq!(values[5], CellView::I32(i32::MIN));
        assert_eq!(values[6], CellView::U64(u64::MAX));
        assert_eq!(values[7], CellView::I64(i64::MIN));
        assert_eq!(values[8], CellView::F32(f32::from_bits(f32_bits)));
        assert_eq!(values[9], CellView::F64(f64::from_bits(f64_bits)));
        assert_eq!(values[10], CellView::Bool(true));
        assert_eq!(values[11], CellView::String("typed"));
        assert_eq!(
            values[12],
            CellView::Datetime(Timestamp::from_unix_nanos(i64::MAX))
        );
        let CellView::List(array) = values[13] else {
            panic!("the fixed array remains a typed list");
        };
        assert_eq!(
            array.iter().collect::<Vec<_>>(),
            [CellView::I16(i16::MIN), CellView::I16(i16::MAX)]
        );
        let CellView::List(vector) = values[14] else {
            panic!("the vector remains a typed list");
        };
        assert_eq!(
            vector.iter().collect::<Vec<_>>(),
            [CellView::U32(0), CellView::U32(u32::MAX)]
        );
    }

    #[test]
    fn selected_rows_batch_to_the_explicit_row_limit() {
        let schema = CreateSchema {
            name: named("events"),
            fields: vec![field("sequence", ParseAsType::I64, false, false)],
        };
        let batch = compile_schema(&schema)
            .batch_from_test_rows(
                (0_i64..6).map(|sequence| [("sequence".to_string(), RuntimeValue::I64(sequence))]),
            )
            .assured("the test rows follow their schema");
        let wire_schema = subscription_row_schema(&schema, None)
            .assured("an unbranched schema needs no branch metadata");
        let (_, encoder) = open(wire_schema, 2);
        let keys = vec![None; batch.batch().num_rows()];
        let selected = [0, 2, 3, 5];
        let frames = encoder
            .encode(
                batch.batch(),
                &keys,
                SubscriptionRowSelection::Rows(&selected),
            )
            .assured("the selected rows encode");

        assert_eq!(frames.len(), 2);
        let decoded = frames.into_iter().map(rows_from_frame).collect::<Vec<_>>();
        assert_eq!(decoded[0].batch().len(), 2);
        assert_eq!(decoded[1].batch().len(), 2);
        let mut values = Vec::new();
        for rows in &decoded {
            let batch = rows.batch();
            for row in batch.rows() {
                let CellView::I64(value) = row.get(0).assured("the row has one cell") else {
                    panic!("the sequence remains I64");
                };
                values.push(value);
            }
        }
        assert_eq!(values, [0, 2, 3, 5]);
    }

    #[test]
    fn encoded_rows_split_before_the_session_byte_limit() {
        let schema = CreateSchema {
            name: named("events"),
            fields: vec![field("detail", ParseAsType::String, false, false)],
        };
        let batch = compile_schema(&schema)
            .batch_from_test_rows((0..20).map(|index| {
                [(
                    "detail".to_string(),
                    RuntimeValue::String(format!("{index:02}-{}", "v".repeat(97))),
                )]
            }))
            .assured("the test rows follow their schema");
        let limits = SessionLimits::try_from(SessionLimitSettings {
            frame_bytes: size(1024),
            transfer_bytes: size(1024),
            nesting_depth: size(64),
            collection_entries: size(100),
            string_bytes: size(1024),
        })
        .assured("the test limits are valid");
        let wire_schema = subscription_row_schema(&schema, None)
            .assured("an unbranched schema needs no branch metadata");
        let (_, encoder) = SubscriptionRowOpening::new(handle(7), wire_schema, limits, size(100))
            .assured("the row limit fits the collection limit")
            .open(named("tenant"), named("events"));
        let keys = vec![None; batch.batch().num_rows()];
        let frames = encoder
            .encode(batch.batch(), &keys, SubscriptionRowSelection::All)
            .assured("the encoder splits rows before overflowing a frame");

        assert!(frames.len() > 1);
        assert!(frames.iter().all(|frame| frame.bytes().len() <= 1024));
        let decoded_rows = frames
            .into_iter()
            .map(|frame| rows_from_frame_under(frame, &limits).batch().len())
            .sum::<usize>();
        assert_eq!(decoded_rows, 20);
    }

    #[test]
    fn interleaved_branches_keep_their_typed_identity_and_order() {
        let payload = CreateSchema {
            name: named("events"),
            fields: vec![field("sequence", ParseAsType::I64, false, false)],
        };
        let branch = CreateSchema {
            name: named("tenant_key"),
            fields: vec![field("tenant", ParseAsType::String, false, false)],
        };
        let branch_name: BranchName = named("by_tenant");
        let branch_fields = vec![named("tenant")];
        let wire_schema = subscription_row_schema(
            &payload,
            Some(SubscriptionBranchSchema {
                name: &branch_name,
                schema: &branch,
                fields: &branch_fields,
            }),
        )
        .assured("the branch field exists");
        let (_, encoder) = open(wire_schema, 8);
        let batch = compile_schema(&payload)
            .batch_from_test_rows(
                (0_i64..4).map(|sequence| [("sequence".to_string(), RuntimeValue::I64(sequence))]),
            )
            .assured("the test rows follow their schema");
        let acme =
            BranchKey::from_fields([(named("tenant"), RuntimeValue::String("acme".to_string()))])
                .assured("the branch key is nonempty");
        let beta =
            BranchKey::from_fields([(named("tenant"), RuntimeValue::String("beta".to_string()))])
                .assured("the branch key is nonempty");
        let keys = vec![
            Some(acme.clone()),
            Some(beta.clone()),
            Some(acme),
            Some(beta),
        ];

        let frames = encoder
            .encode(batch.batch(), &keys, SubscriptionRowSelection::All)
            .assured("interleaved branch rows encode");
        assert_eq!(frames.len(), 4);
        let mut identity_and_rows = Vec::new();
        for frame in frames {
            let rows = rows_from_frame(frame);
            let batch = rows.batch();
            let CellView::String(key) = batch
                .branch_key()
                .assured("every batch has a branch key")
                .get(0)
                .assured("the key has one cell")
            else {
                panic!("the branch key remains a string");
            };
            let CellView::I64(value) = batch
                .row(0)
                .assured("every batch has one row")
                .get(0)
                .assured("the row has one cell")
            else {
                panic!("the sequence remains I64");
            };
            identity_and_rows.push((key.to_string(), value));
        }
        assert_eq!(
            identity_and_rows,
            [
                ("acme".to_string(), 0),
                ("beta".to_string(), 1),
                ("acme".to_string(), 2),
                ("beta".to_string(), 3),
            ]
        );
    }

    #[test]
    fn unsupported_arrow_columns_are_rejected_before_encoding() {
        let arrow_schema = StdArc::new(ArrowSchema::new(vec![ArrowField::new(
            "payload",
            ArrowDataType::Binary,
            false,
        )]));
        let binary: ArrayRef = StdArc::new(BinaryArray::from(vec![b"payload".as_slice()]));
        let record_batch = RecordBatch::try_new(arrow_schema.clone(), vec![binary])
            .assured("the Arrow column follows its Arrow schema");
        let runtime_batch = RuntimeRecordBatch::from_record_batch(arrow_schema, record_batch)
            .assured("the wrapper accepts an exactly matching Arrow schema");
        let row_schema = nervix_client_wire::RowSchema {
            fields: vec![field("payload", ParseAsType::String, false, false)],
            branch: None,
        };
        let (_, encoder) = open(row_schema, 8);
        let error = encoder
            .encode(
                runtime_batch.batch(),
                &[None],
                SubscriptionRowSelection::All,
            )
            .expect_err("binary is not a current subscription row type");

        assert_eq!(
            error.current_context(),
            &SubscriptionRowEncodingError::UnsupportedArrowType {
                field: named("payload"),
                data_type: ArrowDataType::Binary,
            }
        );
    }
}
