//! Typed rows: the schema a subscription announces, the cells its batches carry, and the checks
//! that hold a batch to that schema.
//!
//! A batch is written cell by cell straight into the frame and read back through borrowed views,
//! so no row is materialized as a map of values on either side. Views only exist for batches whose
//! structure was checked when their frame was decoded, which is why reading a cell cannot fail.

use std::{fmt, num::NonZeroU32};

use error_stack::Report;
use flatbuffers::{ForwardsUOffset, Vector, WIPOffset};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{BranchName, FieldName, ParseAsType, SchemaField, Timestamp};
use thiserror::Error;

use crate::{
    codec::{Decoder, EncodedUnion, Encoder, WireDecodeError, WireEncodeError, wire_enum},
    wire,
};

/// The schema a subscription's rows follow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowSchema {
    /// One entry per cell of a row, in row order.
    pub fields: Vec<SchemaField>,
    /// The declared branch and its key fields, or `None` for an unbranched relay.
    pub branch: Option<RowBranch>,
}

/// The declared branch of a branched relay and the fields of its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowBranch {
    branch: BranchName,
    fields: Vec<SchemaField>,
}

/// Why a row branch refused to be constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("a branch key must have at least one field")]
pub struct EmptyBranchKey;

impl RowBranch {
    pub fn new(
        branch: BranchName,
        fields: Vec<SchemaField>,
    ) -> Result<Self, Report<EmptyBranchKey>> {
        if fields.is_empty() {
            return Err(Report::new(EmptyBranchKey));
        }
        Ok(Self { branch, fields })
    }

    pub fn branch(&self) -> &BranchName {
        &self.branch
    }

    /// The branch key fields in key order.
    pub fn fields(&self) -> &[SchemaField] {
        &self.fields
    }
}

impl RowSchema {
    /// Encodes the schema as a table at `depth`, counting the frame's root as depth 1.
    pub(crate) fn encode<'fbb>(
        &self,
        encoder: &mut Encoder<'fbb>,
        depth: usize,
    ) -> Result<WIPOffset<wire::RowSchema<'fbb>>, Report<WireEncodeError>> {
        let field_depth = depth
            .checked_add(1)
            .assured("a schema sits at a fixed depth within its frame");
        let branch_field_depth = depth
            .checked_add(2)
            .assured("a schema sits at a fixed depth within its frame");
        let fields = encoder.table_vector("RowSchema.fields", &self.fields, |field, encoder| {
            encode_field(field, encoder, field_depth)
        })?;
        let branch = match &self.branch {
            Some(branch) => {
                let name = encoder.text("RowBranch.branch", branch.branch.as_str())?;
                let fields = encoder.table_vector(
                    "RowBranch.fields",
                    &branch.fields,
                    |field, encoder| encode_field(field, encoder, branch_field_depth),
                )?;
                Some(wire::RowBranch::create(
                    encoder.fbb(),
                    &wire::RowBranchArgs {
                        branch: Some(name),
                        fields: Some(fields),
                    },
                ))
            }
            None => None,
        };
        Ok(wire::RowSchema::create(
            encoder.fbb(),
            &wire::RowSchemaArgs {
                fields: Some(fields),
                branch,
            },
        ))
    }

    pub(crate) fn decode(
        decoder: Decoder<'_>,
        schema: wire::RowSchema<'_>,
    ) -> Result<Self, Report<WireDecodeError>> {
        let fields = decoder.table_vector("RowSchema.fields", schema.fields(), |field| {
            decode_field(decoder, field)
        })?;
        let branch = match schema.branch() {
            Some(branch) => {
                let name = decoder.name("RowBranch.branch", branch.branch())?;
                let fields =
                    decoder.table_vector("RowBranch.fields", branch.fields(), |field| {
                        decode_field(decoder, field)
                    })?;
                match RowBranch::new(name, fields) {
                    Ok(branch) => Some(branch),
                    Err(error) => {
                        return Err(error.change_context(WireDecodeError::EmptyCollection {
                            field: "RowBranch.fields",
                        }));
                    }
                }
            }
            None => None,
        };
        Ok(Self { fields, branch })
    }
}

/// Encodes a field as a table at `depth`.
fn encode_field<'fbb>(
    field: &SchemaField,
    encoder: &mut Encoder<'fbb>,
    depth: usize,
) -> Result<WIPOffset<wire::RowField<'fbb>>, Report<WireEncodeError>> {
    let name = encoder.text("RowField.name", field.name.as_str())?;
    let type_depth = depth
        .checked_add(1)
        .assured("a field sits at a fixed depth within its frame");
    let field_type = encode_field_type(encoder, &field.ty, type_depth)?;
    Ok(wire::RowField::create(
        encoder.fbb(),
        &wire::RowFieldArgs {
            name: Some(name),
            field_type: Some(field_type),
            nullable: field.optional,
            sensitive: field.sensitive,
        },
    ))
}

fn decode_field(
    decoder: Decoder<'_>,
    field: wire::RowField<'_>,
) -> Result<SchemaField, Report<WireDecodeError>> {
    let name: FieldName = decoder.name("RowField.name", field.name())?;
    let ty = decode_field_type(decoder, field.field_type())?;
    Ok(SchemaField {
        name,
        ty,
        optional: field.nullable(),
        sensitive: field.sensitive(),
    })
}

/// The scalar types a field can hold, as the schema names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScalarType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    Bool,
    String,
    Bytes,
    Datetime,
}

wire_enum!(ALL_SCALAR_TYPES: ScalarType => wire::ScalarType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    Bool,
    String,
    Bytes,
    Datetime,
});

/// Encodes a field type as a table at `depth`. Its shape is one level deeper, and a list's element
/// type two.
fn encode_field_type<'fbb>(
    encoder: &mut Encoder<'fbb>,
    ty: &ParseAsType,
    depth: usize,
) -> Result<WIPOffset<wire::FieldType<'fbb>>, Report<WireEncodeError>> {
    let shape_depth = depth
        .checked_add(1)
        .assured("every enclosing level passed the nesting check, which bounds the depth");
    encoder.nesting("FieldType.shape", shape_depth)?;
    let element_depth = depth
        .checked_add(2)
        .verified("the nesting check above held the shape's depth to the nesting limit");
    let shape = match ty {
        ParseAsType::U8 => ScalarType::U8.encode(encoder),
        ParseAsType::I8 => ScalarType::I8.encode(encoder),
        ParseAsType::U16 => ScalarType::U16.encode(encoder),
        ParseAsType::I16 => ScalarType::I16.encode(encoder),
        ParseAsType::U32 => ScalarType::U32.encode(encoder),
        ParseAsType::I32 => ScalarType::I32.encode(encoder),
        ParseAsType::U64 => ScalarType::U64.encode(encoder),
        ParseAsType::I64 => ScalarType::I64.encode(encoder),
        ParseAsType::F32 => ScalarType::F32.encode(encoder),
        ParseAsType::F64 => ScalarType::F64.encode(encoder),
        ParseAsType::Bool => ScalarType::Bool.encode(encoder),
        ParseAsType::String => ScalarType::String.encode(encoder),
        ParseAsType::Bytes => ScalarType::Bytes.encode(encoder),
        ParseAsType::Datetime => ScalarType::Datetime.encode(encoder),
        ParseAsType::Array { element, len } => {
            let element = encode_field_type(encoder, element, element_depth)?;
            let list = wire::FixedListFieldType::create(
                encoder.fbb(),
                &wire::FixedListFieldTypeArgs {
                    element: Some(element),
                    length: len.get(),
                },
            );
            EncodedUnion::new(wire::FieldTypeShape::FixedListFieldType, list)
        }
        ParseAsType::Vec { element } => {
            let element = encode_field_type(encoder, element, element_depth)?;
            let list = wire::ListFieldType::create(
                encoder.fbb(),
                &wire::ListFieldTypeArgs {
                    element: Some(element),
                },
            );
            EncodedUnion::new(wire::FieldTypeShape::ListFieldType, list)
        }
    };
    Ok(wire::FieldType::create(
        encoder.fbb(),
        &wire::FieldTypeArgs {
            shape_type: shape.discriminant,
            shape: Some(shape.value),
        },
    ))
}

impl ScalarType {
    fn encode(self, encoder: &mut Encoder<'_>) -> EncodedUnion<wire::FieldTypeShape> {
        let scalar = wire::ScalarFieldType::create(
            encoder.fbb(),
            &wire::ScalarFieldTypeArgs {
                scalar: Some(self.into()),
            },
        );
        EncodedUnion::new(wire::FieldTypeShape::ScalarFieldType, scalar)
    }
}

fn decode_field_type(
    decoder: Decoder<'_>,
    field_type: wire::FieldType<'_>,
) -> Result<ParseAsType, Report<WireDecodeError>> {
    if let Some(scalar) = field_type.shape_as_scalar_field_type() {
        let scalar = decoder.required_enumeration("ScalarFieldType.scalar", scalar.scalar())?;
        let ty = match scalar {
            ScalarType::U8 => ParseAsType::U8,
            ScalarType::I8 => ParseAsType::I8,
            ScalarType::U16 => ParseAsType::U16,
            ScalarType::I16 => ParseAsType::I16,
            ScalarType::U32 => ParseAsType::U32,
            ScalarType::I32 => ParseAsType::I32,
            ScalarType::U64 => ParseAsType::U64,
            ScalarType::I64 => ParseAsType::I64,
            ScalarType::F32 => ParseAsType::F32,
            ScalarType::F64 => ParseAsType::F64,
            ScalarType::Bool => ParseAsType::Bool,
            ScalarType::String => ParseAsType::String,
            ScalarType::Bytes => ParseAsType::Bytes,
            ScalarType::Datetime => ParseAsType::Datetime,
        };
        return Ok(ty);
    }
    if let Some(list) = field_type.shape_as_fixed_list_field_type() {
        let element = decode_field_type(decoder, list.element())?;
        let Some(len) = NonZeroU32::new(list.length()) else {
            return Err(Report::new(WireDecodeError::ZeroValue {
                field: "FixedListFieldType.length",
            }));
        };
        return Ok(ParseAsType::Array {
            element: Box::new(element),
            len,
        });
    }
    if let Some(list) = field_type.shape_as_list_field_type() {
        let element = decode_field_type(decoder, list.element())?;
        return Ok(ParseAsType::Vec {
            element: Box::new(element),
        });
    }
    Err(decoder.unknown_union("FieldType.shape", field_type.shape_type().0))
}

/// The most bytes one cell's tables add to a frame beyond the string it may hold: the cell table
/// and its value table, their vtables, and alignment padding. A test measures every cell kind
/// against it.
pub(crate) const CELL_TABLES_BYTES: usize = 64;

/// Writes the cells of one row, one branch key, or one list value, in field order.
///
/// Every cell is written straight into the frame being built, after it is checked against the
/// nesting, collection and byte limits, so a batch never grows far past its frame. A list value
/// opens a nested writer for its elements, so a list can never be left unterminated.
pub struct CellWriter<'e, 'fbb> {
    encoder: &'e mut Encoder<'fbb>,
    field: &'static str,
    /// The depth of the cell tables written here, counting the frame's root as depth 1. Each
    /// cell's value table is one level deeper.
    depth: usize,
    cells: Vec<WIPOffset<wire::Cell<'fbb>>>,
}

impl<'e, 'fbb> CellWriter<'e, 'fbb> {
    pub(crate) fn new(encoder: &'e mut Encoder<'fbb>, field: &'static str, depth: usize) -> Self {
        Self {
            encoder,
            field,
            depth,
            cells: Vec::new(),
        }
    }

    /// A null value, for a nullable field.
    pub fn push_null(&mut self) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::NullCell::create(self.encoder.fbb(), &wire::NullCellArgs {});
        self.push_cell(wire::CellValue::NullCell, cell);
        Ok(())
    }

    /// The withheld value of a sensitive field.
    pub fn push_redacted(&mut self) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::RedactedCell::create(self.encoder.fbb(), &wire::RedactedCellArgs {});
        self.push_cell(wire::CellValue::RedactedCell, cell);
        Ok(())
    }

    pub fn push_u8(&mut self, value: u8) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::U8Cell::create(self.encoder.fbb(), &wire::U8CellArgs { value });
        self.push_cell(wire::CellValue::U8Cell, cell);
        Ok(())
    }

    pub fn push_i8(&mut self, value: i8) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::I8Cell::create(self.encoder.fbb(), &wire::I8CellArgs { value });
        self.push_cell(wire::CellValue::I8Cell, cell);
        Ok(())
    }

    pub fn push_u16(&mut self, value: u16) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::U16Cell::create(self.encoder.fbb(), &wire::U16CellArgs { value });
        self.push_cell(wire::CellValue::U16Cell, cell);
        Ok(())
    }

    pub fn push_i16(&mut self, value: i16) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::I16Cell::create(self.encoder.fbb(), &wire::I16CellArgs { value });
        self.push_cell(wire::CellValue::I16Cell, cell);
        Ok(())
    }

    pub fn push_u32(&mut self, value: u32) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::U32Cell::create(self.encoder.fbb(), &wire::U32CellArgs { value });
        self.push_cell(wire::CellValue::U32Cell, cell);
        Ok(())
    }

    pub fn push_i32(&mut self, value: i32) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::I32Cell::create(self.encoder.fbb(), &wire::I32CellArgs { value });
        self.push_cell(wire::CellValue::I32Cell, cell);
        Ok(())
    }

    pub fn push_u64(&mut self, value: u64) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::U64Cell::create(self.encoder.fbb(), &wire::U64CellArgs { value });
        self.push_cell(wire::CellValue::U64Cell, cell);
        Ok(())
    }

    pub fn push_i64(&mut self, value: i64) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::I64Cell::create(self.encoder.fbb(), &wire::I64CellArgs { value });
        self.push_cell(wire::CellValue::I64Cell, cell);
        Ok(())
    }

    /// A 32-bit float, with its exact bits including the sign of zero and any NaN payload.
    pub fn push_f32(&mut self, value: f32) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::F32Cell::create(
            self.encoder.fbb(),
            &wire::F32CellArgs { value: Some(value) },
        );
        self.push_cell(wire::CellValue::F32Cell, cell);
        Ok(())
    }

    /// A 64-bit float, with its exact bits including the sign of zero and any NaN payload.
    pub fn push_f64(&mut self, value: f64) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::F64Cell::create(
            self.encoder.fbb(),
            &wire::F64CellArgs { value: Some(value) },
        );
        self.push_cell(wire::CellValue::F64Cell, cell);
        Ok(())
    }

    pub fn push_bool(&mut self, value: bool) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::BoolCell::create(self.encoder.fbb(), &wire::BoolCellArgs { value });
        self.push_cell(wire::CellValue::BoolCell, cell);
        Ok(())
    }

    pub fn push_string(&mut self, value: &str) -> Result<(), Report<WireEncodeError>> {
        self.admit(value.len())?;
        let value = self.encoder.text("StringCell.value", value)?;
        let cell = wire::StringCell::create(
            self.encoder.fbb(),
            &wire::StringCellArgs { value: Some(value) },
        );
        self.push_cell(wire::CellValue::StringCell, cell);
        Ok(())
    }

    pub fn push_bytes(&mut self, value: &[u8]) -> Result<(), Report<WireEncodeError>> {
        self.admit(value.len())?;
        let value = self.encoder.bytes("BytesCell.value", value)?;
        let cell = wire::BytesCell::create(
            self.encoder.fbb(),
            &wire::BytesCellArgs { value: Some(value) },
        );
        self.push_cell(wire::CellValue::BytesCell, cell);
        Ok(())
    }

    pub fn push_datetime(&mut self, value: Timestamp) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let cell = wire::DatetimeCell::create(
            self.encoder.fbb(),
            &wire::DatetimeCellArgs {
                unix_nanos: value.unix_nanos(),
            },
        );
        self.push_cell(wire::CellValue::DatetimeCell, cell);
        Ok(())
    }

    /// A list value whose elements `write_elements` writes.
    pub fn push_list(
        &mut self,
        write_elements: impl FnOnce(&mut CellWriter<'_, 'fbb>) -> Result<(), Report<WireEncodeError>>,
    ) -> Result<(), Report<WireEncodeError>> {
        self.admit(0)?;
        let element_depth = self
            .depth
            .checked_add(2)
            .verified("admitting the list held the depth to the nesting limit");
        let mut elements = CellWriter::new(self.encoder, "ListCell.elements", element_depth);
        write_elements(&mut elements)?;
        let elements = elements.finish()?;
        // The elements grew the frame after the list was admitted, so its own tables are checked
        // again before they are written.
        self.encoder.reserve(self.field, CELL_TABLES_BYTES)?;
        let cell = wire::ListCell::create(
            self.encoder.fbb(),
            &wire::ListCellArgs {
                elements: Some(elements),
            },
        );
        self.push_cell(wire::CellValue::ListCell, cell);
        Ok(())
    }

    /// Checks one more cell, holding `payload` bytes beyond its tables, against the limits.
    fn admit(&self, payload: usize) -> Result<(), Report<WireEncodeError>> {
        let value_depth = self.depth.checked_add(1).assured(
            "a writer is at most one level past a nesting limit of at most MAX_NESTING_DEPTH",
        );
        self.encoder.nesting(self.field, value_depth)?;
        let cells = self
            .cells
            .len()
            .checked_add(1)
            .assured("a vector of four-byte offsets holds at most isize::MAX / 4 entries");
        self.encoder.entries(self.field, cells)?;
        let bytes = payload
            .checked_add(CELL_TABLES_BYTES)
            .assured("a payload is the length of an in-memory string, at most isize::MAX bytes");
        self.encoder.reserve(self.field, bytes)
    }

    fn push_cell<T>(&mut self, discriminant: wire::CellValue, value: WIPOffset<T>) {
        let cell = wire::Cell::create(
            self.encoder.fbb(),
            &wire::CellArgs {
                value_type: discriminant,
                value: Some(value.as_union_value()),
            },
        );
        self.cells.push(cell);
    }

    /// The bytes the frame being written holds so far.
    #[cfg(test)]
    pub(crate) fn encoded_bytes(&self) -> usize {
        self.encoder.encoded_bytes()
    }

    /// The cells written, as a vector.
    pub(crate) fn finish(
        self,
    ) -> Result<WIPOffset<Vector<'fbb, ForwardsUOffset<wire::Cell<'fbb>>>>, Report<WireEncodeError>>
    {
        self.encoder.tables(self.field, &self.cells)
    }
}

/// A batch of rows read from a verified frame.
#[derive(Clone, Copy)]
pub struct RowBatchView<'a> {
    batch: wire::RowBatch<'a>,
}

impl<'a> RowBatchView<'a> {
    /// Checks the batch's structure and wraps it. Only a batch that passed this check is ever
    /// read through a view.
    pub(crate) fn check(
        decoder: Decoder<'_>,
        batch: wire::RowBatch<'a>,
    ) -> Result<Self, Report<WireDecodeError>> {
        if let Some(branch_key) = batch.branch_key() {
            CellsView::check(decoder, "BranchKey.cells", branch_key.cells())?;
        }
        let rows = batch.rows();
        if rows.is_empty() {
            return Err(Report::new(WireDecodeError::EmptyCollection {
                field: "RowBatch.rows",
            }));
        }
        decoder.entries("RowBatch.rows", rows.len())?;
        for row in rows.iter() {
            CellsView::check(decoder, "Row.cells", row.cells())?;
        }
        Ok(Self { batch })
    }

    /// Wraps a batch whose structure was checked when its frame was decoded.
    pub(crate) fn checked(batch: wire::RowBatch<'a>) -> Self {
        Self { batch }
    }

    /// The concrete branch the rows belong to, or `None` for an unbranched relay.
    pub fn branch_key(&self) -> Option<CellsView<'a>> {
        self.batch.branch_key().map(|branch_key| CellsView {
            cells: branch_key.cells(),
        })
    }

    /// The number of rows. A batch is never empty.
    pub fn len(&self) -> usize {
        self.batch.rows().len()
    }

    /// Whether the batch holds no rows, which a checked batch never does.
    pub fn is_empty(&self) -> bool {
        self.batch.rows().is_empty()
    }

    pub fn row(&self, index: usize) -> Option<CellsView<'a>> {
        let rows = self.batch.rows();
        if index >= rows.len() {
            return None;
        }
        Some(CellsView {
            cells: rows.get(index).cells(),
        })
    }

    pub fn rows(&self) -> impl ExactSizeIterator<Item = CellsView<'a>> + 'a {
        self.batch
            .rows()
            .iter()
            .map(|row| CellsView { cells: row.cells() })
    }

    /// Checks every cell of the batch against the schema its subscription announced.
    pub fn conform(&self, schema: &RowSchema) -> Result<(), Report<RowConformanceError>> {
        match (self.branch_key(), &schema.branch) {
            (Some(key), Some(branch)) => {
                key.conform(RowLocation::BranchKey, branch.fields())?;
            }
            (None, None) => {}
            (Some(_), None) => {
                return Err(Report::new(RowConformanceError::UnexpectedBranchKey));
            }
            (None, Some(_)) => return Err(Report::new(RowConformanceError::MissingBranchKey)),
        }
        for (index, row) in self.rows().enumerate() {
            row.conform(RowLocation::Row { index }, &schema.fields)?;
        }
        Ok(())
    }
}

impl fmt::Debug for RowBatchView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RowBatchView")
            .field("branch_key", &self.branch_key())
            .field("rows", &self.rows().collect::<Vec<_>>())
            .finish()
    }
}

/// The cells of one row, one branch key, or one list value.
#[derive(Clone, Copy)]
pub struct CellsView<'a> {
    cells: Vector<'a, ForwardsUOffset<wire::Cell<'a>>>,
}

impl<'a> CellsView<'a> {
    /// Checks the structure of cells before any view reads them: declared discriminants, present
    /// float values, and the string and collection limits, recursively through list values.
    fn check(
        decoder: Decoder<'_>,
        field: &'static str,
        cells: Vector<'_, ForwardsUOffset<wire::Cell<'_>>>,
    ) -> Result<(), Report<WireDecodeError>> {
        decoder.entries(field, cells.len())?;
        for cell in cells.iter() {
            match cell.value_type() {
                wire::CellValue::NullCell
                | wire::CellValue::RedactedCell
                | wire::CellValue::U8Cell
                | wire::CellValue::I8Cell
                | wire::CellValue::U16Cell
                | wire::CellValue::I16Cell
                | wire::CellValue::U32Cell
                | wire::CellValue::I32Cell
                | wire::CellValue::U64Cell
                | wire::CellValue::I64Cell
                | wire::CellValue::BoolCell
                | wire::CellValue::DatetimeCell => {}
                wire::CellValue::F32Cell => {
                    let value = checked_variant(cell.value_as_f32_cell()).value();
                    decoder.required("F32Cell.value", value)?;
                }
                wire::CellValue::F64Cell => {
                    let value = checked_variant(cell.value_as_f64_cell()).value();
                    decoder.required("F64Cell.value", value)?;
                }
                wire::CellValue::StringCell => {
                    let value = checked_variant(cell.value_as_string_cell()).value();
                    decoder.check_text("StringCell.value", value)?;
                }
                wire::CellValue::BytesCell => {
                    let _verified_value = checked_variant(cell.value_as_bytes_cell()).value();
                }
                wire::CellValue::ListCell => {
                    let elements = checked_variant(cell.value_as_list_cell()).elements();
                    Self::check(decoder, "ListCell.elements", elements)?;
                }
                undeclared => return Err(decoder.unknown_union("Cell.value", undeclared.0)),
            }
        }
        Ok(())
    }

    /// Checks one cell per field, in order, against the fields' types.
    fn conform(
        &self,
        location: RowLocation,
        fields: &[SchemaField],
    ) -> Result<(), Report<RowConformanceError>> {
        if self.len() != fields.len() {
            return Err(Report::new(RowConformanceError::CellCount {
                location,
                expected: fields.len(),
                actual: self.len(),
            }));
        }
        for (cell, field) in self.iter().zip(fields) {
            cell.conform_field(location, field)?;
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<CellView<'a>> {
        if index >= self.cells.len() {
            return None;
        }
        Some(CellView::checked(self.cells.get(index)))
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = CellView<'a>> + 'a {
        self.cells.iter().map(CellView::checked)
    }
}

impl fmt::Debug for CellsView<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq for CellsView<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|(left, right)| left == right)
    }
}

/// One cell of a checked batch.
///
/// Floats compare by their exact bits, so a cell equals the cell it was encoded from even when it
/// holds a NaN.
#[derive(Debug, Clone, Copy)]
pub enum CellView<'a> {
    Null,
    Redacted,
    U8(u8),
    I8(i8),
    U16(u16),
    I16(i16),
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    String(&'a str),
    Bytes(&'a [u8]),
    Datetime(Timestamp),
    List(CellsView<'a>),
}

impl<'a> CellView<'a> {
    fn checked(cell: wire::Cell<'a>) -> Self {
        match cell.value_type() {
            wire::CellValue::NullCell => Self::Null,
            wire::CellValue::RedactedCell => Self::Redacted,
            wire::CellValue::U8Cell => Self::U8(checked_variant(cell.value_as_u8_cell()).value()),
            wire::CellValue::I8Cell => Self::I8(checked_variant(cell.value_as_i8_cell()).value()),
            wire::CellValue::U16Cell => {
                Self::U16(checked_variant(cell.value_as_u16_cell()).value())
            }
            wire::CellValue::I16Cell => {
                Self::I16(checked_variant(cell.value_as_i16_cell()).value())
            }
            wire::CellValue::U32Cell => {
                Self::U32(checked_variant(cell.value_as_u32_cell()).value())
            }
            wire::CellValue::I32Cell => {
                Self::I32(checked_variant(cell.value_as_i32_cell()).value())
            }
            wire::CellValue::U64Cell => {
                Self::U64(checked_variant(cell.value_as_u64_cell()).value())
            }
            wire::CellValue::I64Cell => {
                Self::I64(checked_variant(cell.value_as_i64_cell()).value())
            }
            wire::CellValue::F32Cell => Self::F32(
                checked_variant(cell.value_as_f32_cell())
                    .value()
                    .assured("batch checking requires a float cell to carry its value"),
            ),
            wire::CellValue::F64Cell => Self::F64(
                checked_variant(cell.value_as_f64_cell())
                    .value()
                    .assured("batch checking requires a float cell to carry its value"),
            ),
            wire::CellValue::BoolCell => {
                Self::Bool(checked_variant(cell.value_as_bool_cell()).value())
            }
            wire::CellValue::StringCell => {
                Self::String(checked_variant(cell.value_as_string_cell()).value())
            }
            wire::CellValue::BytesCell => {
                Self::Bytes(checked_variant(cell.value_as_bytes_cell()).value().bytes())
            }
            wire::CellValue::DatetimeCell => Self::Datetime(Timestamp::from_unix_nanos(
                checked_variant(cell.value_as_datetime_cell()).unix_nanos(),
            )),
            // Batch checking admits only declared discriminants, and every other one is matched
            // above, so this arm reads a list cell.
            _ => Self::List(CellsView {
                cells: checked_variant(cell.value_as_list_cell()).elements(),
            }),
        }
    }

    /// The name of the cell's kind, for diagnostics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Redacted => "REDACTED",
            Self::U8(_) => "U8",
            Self::I8(_) => "I8",
            Self::U16(_) => "U16",
            Self::I16(_) => "I16",
            Self::U32(_) => "U32",
            Self::I32(_) => "I32",
            Self::U64(_) => "U64",
            Self::I64(_) => "I64",
            Self::F32(_) => "F32",
            Self::F64(_) => "F64",
            Self::Bool(_) => "BOOL",
            Self::String(_) => "STRING",
            Self::Bytes(_) => "BYTES",
            Self::Datetime(_) => "DATETIME",
            Self::List(_) => "LIST",
        }
    }

    /// Checks the cell against its field: a sensitive field is always redacted, a null needs a
    /// nullable field, and any other value must hold the field's type.
    fn conform_field(
        self,
        location: RowLocation,
        field: &SchemaField,
    ) -> Result<(), Report<RowConformanceError>> {
        if field.sensitive {
            if let Self::Redacted = self {
                return Ok(());
            }
            return Err(Report::new(
                RowConformanceError::SensitiveFieldNotRedacted {
                    location,
                    field: field.name.clone(),
                },
            ));
        }
        match self {
            Self::Null if field.optional => Ok(()),
            Self::Null => Err(Report::new(RowConformanceError::UnexpectedNull {
                location,
                field: field.name.clone(),
            })),
            Self::Redacted => Err(Report::new(RowConformanceError::UnexpectedRedaction {
                location,
                field: field.name.clone(),
            })),
            value => value.conform_value(location, &field.name, &field.ty),
        }
    }

    /// Checks that the cell holds a value of `ty`, recursively through list elements.
    fn conform_value(
        self,
        location: RowLocation,
        field: &FieldName,
        ty: &ParseAsType,
    ) -> Result<(), Report<RowConformanceError>> {
        let elements = match (ty, self) {
            (ParseAsType::U8, Self::U8(_))
            | (ParseAsType::I8, Self::I8(_))
            | (ParseAsType::U16, Self::U16(_))
            | (ParseAsType::I16, Self::I16(_))
            | (ParseAsType::U32, Self::U32(_))
            | (ParseAsType::I32, Self::I32(_))
            | (ParseAsType::U64, Self::U64(_))
            | (ParseAsType::I64, Self::I64(_))
            | (ParseAsType::F32, Self::F32(_))
            | (ParseAsType::F64, Self::F64(_))
            | (ParseAsType::Bool, Self::Bool(_))
            | (ParseAsType::String, Self::String(_))
            | (ParseAsType::Bytes, Self::Bytes(_))
            | (ParseAsType::Datetime, Self::Datetime(_)) => return Ok(()),
            (ParseAsType::Array { element, len }, Self::List(elements)) => {
                let expected = usize::try_from(len.get())
                    .assured("a u32 length fits usize on every target this crate builds for");
                if elements.len() != expected {
                    return Err(Report::new(RowConformanceError::FixedListLength {
                        location,
                        field: field.clone(),
                        expected: len.get(),
                        actual: elements.len(),
                    }));
                }
                ElementCheck { element, elements }
            }
            (ParseAsType::Vec { element }, Self::List(elements)) => {
                ElementCheck { element, elements }
            }
            (ty, cell) => {
                return Err(Report::new(RowConformanceError::TypeMismatch {
                    location,
                    field: field.clone(),
                    expected: ty.clone(),
                    actual: cell.kind(),
                }));
            }
        };
        for element in elements.elements.iter() {
            element.conform_value(location, field, elements.element)?;
        }
        Ok(())
    }
}

impl PartialEq for CellView<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) | (Self::Redacted, Self::Redacted) => true,
            (Self::U8(left), Self::U8(right)) => left == right,
            (Self::I8(left), Self::I8(right)) => left == right,
            (Self::U16(left), Self::U16(right)) => left == right,
            (Self::I16(left), Self::I16(right)) => left == right,
            (Self::U32(left), Self::U32(right)) => left == right,
            (Self::I32(left), Self::I32(right)) => left == right,
            (Self::U64(left), Self::U64(right)) => left == right,
            (Self::I64(left), Self::I64(right)) => left == right,
            (Self::F32(left), Self::F32(right)) => left.to_bits() == right.to_bits(),
            (Self::F64(left), Self::F64(right)) => left.to_bits() == right.to_bits(),
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Bytes(left), Self::Bytes(right)) => left == right,
            (Self::Datetime(left), Self::Datetime(right)) => left == right,
            (Self::List(left), Self::List(right)) => left == right,
            _ => false,
        }
    }
}

/// Reads the union member a checked cell's discriminant names.
fn checked_variant<T>(variant: Option<T>) -> T {
    variant.assured("a cell's discriminant names the member its value accessor reads")
}

/// Where in a batch a conformance problem is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowLocation {
    BranchKey,
    Row { index: usize },
}

impl fmt::Display for RowLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BranchKey => formatter.write_str("the branch key"),
            Self::Row { index } => write!(formatter, "row {index}"),
        }
    }
}

/// How a batch departs from its subscription's schema.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RowConformanceError {
    #[error("the batch carries a branch key, but the relay is unbranched")]
    UnexpectedBranchKey,
    #[error("the relay is branched, but the batch carries no branch key")]
    MissingBranchKey,
    #[error("{location} holds {actual} cells for {expected} fields")]
    CellCount {
        location: RowLocation,
        expected: usize,
        actual: usize,
    },
    #[error("{location} field `{field}` holds a {actual} cell where {expected} is declared")]
    TypeMismatch {
        location: RowLocation,
        field: FieldName,
        expected: ParseAsType,
        actual: &'static str,
    },
    #[error("{location} field `{field}` holds a null, but the field is not nullable")]
    UnexpectedNull {
        location: RowLocation,
        field: FieldName,
    },
    #[error("{location} field `{field}` is redacted, but the field is not sensitive")]
    UnexpectedRedaction {
        location: RowLocation,
        field: FieldName,
    },
    #[error("{location} field `{field}` is sensitive, but its cell is not redacted")]
    SensitiveFieldNotRedacted {
        location: RowLocation,
        field: FieldName,
    },
    #[error(
        "{location} field `{field}` holds {actual} list elements for a fixed length of {expected}"
    )]
    FixedListLength {
        location: RowLocation,
        field: FieldName,
        expected: u32,
        actual: usize,
    },
}

/// The elements of a list value and the type each must hold.
struct ElementCheck<'t, 'a> {
    element: &'t ParseAsType,
    elements: CellsView<'a>,
}
