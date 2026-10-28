//! How an Iceberg sink writes the columns its `VALUES` maps into its table's current schema.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Matching the mapped columns to the columns of the table's current schema by name,
//!   refusing a table they cannot be written into, and projecting each staged batch onto that
//!   schema at every depth: each mapped column into the table column of its name, and a null into
//!   each optional column `VALUES` leaves out.
//! - **Depends on.** Arrow arrays and schemas, the `iceberg` crate's schema and its Arrow form, the
//!   sink's start and commit errors, and `error-stack`.
//! - **Must not know.** Staging, catalogs, commits, acknowledgements, or the host.
//!
//! The Parquet writer pairs a batch's columns with a data file's columns by position, in the
//! table's column order, while a staged batch carries the mapped columns in `VALUES` order. A
//! staged batch therefore reaches the writer only as a projection planned against the table lays
//! it out. The sink plans one when it opens, which refuses a table the mapped columns cannot be
//! written into, and again before every commit writes, against the schema the commit finds.

#![cfg_attr(
    nervix_lint,
    nervix::input(
        plans,
        reason = "the sink checks the mapped columns its validated plan names"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use ::iceberg::{
    arrow::{arrow_schema_to_schema_auto_assign_ids, schema_to_arrow_schema},
    spec::{Schema as IcebergSchema, Type},
};
use ahash::{HashMap, HashSet};
use arrow_array::{
    Array, ArrayRef, LargeBinaryArray, ListArray, MapArray, RecordBatch, RecordBatchOptions,
    StructArray,
    cast::AsArray as _,
    new_null_array,
    types::{Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type},
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, FieldRef, Fields, SchemaRef};
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_connector::{EmitterOpenError, EmitterStartResult, EmitterWriteError};
use nervix_primitives::sync::StdArc;

use super::{ICEBERG, IcebergTableEmitter};

/// Why the mapped columns cannot be written into a table's current schema, or a staged batch into
/// the table's columns. A variant that concerns one column names it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum IcebergColumnError {
    /// The mapped columns have no Iceberg form. The `iceberg` crate's conversion error is beneath.
    #[error("mapped Iceberg schema cannot be written")]
    MappedSchema,
    /// The table's current schema has no Arrow form to write data files in. The `iceberg` crate's
    /// conversion error is beneath.
    #[error("the Iceberg table's current schema has no Arrow form")]
    TableSchema,
    #[error("mapped column '{column}' does not exist in the Iceberg table")]
    Missing { column: String },
    #[error("mapped column '{column}' has Iceberg type {mapped}, but the table requires {table}")]
    TypeMismatch {
        column: String,
        mapped: Box<Type>,
        table: Box<Type>,
    },
    #[error("table column '{column}' is required, but VALUES does not map it")]
    UnmappedRequired { column: String },
    #[error(
        "table column '{column}' is required, but the VALUES expression mapped into it can \
         produce a null"
    )]
    NullableIntoRequired { column: String },
    #[error(
        "table column '{column}' requires every value nested in it, but the VALUES expression \
         mapped into it can produce a null one"
    )]
    NullableNestedIntoRequired { column: String },
    /// The Iceberg types agree, but the staged values have an Arrow layout the sink does not lay
    /// out as the table's.
    #[error(
        "mapped column '{column}' holds Arrow {data_type} values, which the Iceberg sink does not \
         write into a {table} column"
    )]
    UnwritableLayout {
        column: String,
        data_type: DataType,
        table: Box<Type>,
    },
    /// Laying a staged column out as the table column of its name failed. The Arrow error is
    /// beneath.
    #[error("mapped column '{column}' cannot be laid out as the table column of its name")]
    Write { column: String },
    /// A staged batch does not carry the columns the projection was planned for, or its laid-out
    /// columns do not form a batch of the table's schema, whose Arrow error is then beneath.
    #[error("the staged rows cannot be laid out as the Iceberg table's columns")]
    Batch,
}

impl IcebergColumnError {
    /// The open failure `refusal` is, with its description attached for the emitter to show as
    /// its transient error while it retries the open.
    pub(crate) fn start_failure(refusal: Report<Self>) -> Report<EmitterOpenError> {
        let description = refusal.current_context().to_string();
        refusal
            .change_context(EmitterOpenError::InvalidConfiguration { connector: ICEBERG })
            .attach_printable(description)
    }

    /// The commit failure `refusal` is, with its description attached for the emitter to show as
    /// its transient error while it retries the commit, which keeps its staged rows and their
    /// acknowledgements.
    pub(crate) fn commit_failure(refusal: Report<Self>) -> Report<EmitterWriteError> {
        let description = refusal.current_context().to_string();
        refusal
            .change_context(EmitterWriteError::Commit { connector: ICEBERG })
            .attach_printable(description)
    }

    /// A current table schema that cannot lay out a batch when publication measures its file.
    pub(crate) fn publish_failure(refusal: Report<Self>) -> Report<EmitterWriteError> {
        let description = refusal.current_context().to_string();
        refusal
            .change_context(EmitterWriteError::Publish { connector: ICEBERG })
            .attach_printable(description)
    }
}

impl IcebergTableEmitter {
    /// Refuses, when the sink opens, a table the mapped columns `staged` cannot be written into,
    /// by planning the projection every commit writes them with.
    pub(super) fn validate_table_schema(
        staged: &arrow_schema::Schema,
        table: &IcebergSchema,
    ) -> EmitterStartResult<()> {
        let staged = StdArc::new(staged.clone());
        match IcebergTableProjection::plan(&staged, table) {
            Ok(_) => Ok(()),
            Err(refusal) => Err(IcebergColumnError::start_failure(refusal)),
        }
    }
}

/// How staged batches are written into one table schema: the table's columns in its order, each
/// taken from the mapped column of its name or written as null.
#[derive(Debug)]
pub(crate) struct IcebergTableProjection {
    /// The staged schema of the batches this projection lays out.
    staged: SchemaRef,
    /// The table's schema in the Arrow form its data files are written in.
    table: SchemaRef,
    /// Where each table column's values come from, in the table's column order.
    columns: Vec<IcebergTableColumn>,
}

/// One column of the table's schema, and where its values come from.
#[derive(Debug)]
struct IcebergTableColumn {
    field: FieldRef,
    source: IcebergColumnSource,
}

#[derive(Debug)]
enum IcebergColumnSource {
    /// The staged column at `index`, laid out as `layout` describes.
    Mapped {
        index: usize,
        layout: IcebergValueLayout,
    },
    /// An optional column `VALUES` leaves out, written as null.
    Unmapped,
}

impl IcebergTableProjection {
    /// Matches the mapped columns `staged` to the columns of `table` by name. Every mapped column
    /// must name a table column whose Iceberg type it has. A required table column must be mapped
    /// from an expression that cannot produce a null, and a value nested in a mapped column that
    /// the table requires must not be able to be null either. Every table column `VALUES` leaves
    /// out is optional and written as null.
    pub(crate) fn plan(
        staged: &SchemaRef,
        table: &IcebergSchema,
    ) -> error_stack::Result<Self, IcebergColumnError> {
        // The staged columns' Iceberg types, which a type refusal names. Their generated field IDs
        // describe nothing in the table, so types are compared without them.
        let mapped = arrow_schema_to_schema_auto_assign_ids(staged)
            .change_context(IcebergColumnError::MappedSchema)?;
        let table_arrow =
            schema_to_arrow_schema(table).change_context(IcebergColumnError::TableSchema)?;
        let mut table_columns = HashSet::default();
        for field in table_arrow.fields() {
            table_columns.insert(field.name().as_str());
        }
        for field in staged.fields() {
            if !table_columns.contains(field.name().as_str()) {
                return Err(Report::new(IcebergColumnError::Missing {
                    column: field.name().clone(),
                }));
            }
        }
        let mut staged_columns = HashMap::default();
        for (index, field) in staged.fields().iter().enumerate() {
            staged_columns.insert(field.name().as_str(), index);
        }
        let mut columns = Vec::with_capacity(table_arrow.fields().len());
        for field in table_arrow.fields() {
            let source = match staged_columns.get(field.name().as_str()) {
                Some(&index) => {
                    let column = IcebergMappedColumn {
                        staged: staged.field(index),
                        table: field,
                        mapped: &mapped,
                        table_schema: table,
                    };
                    let layout = column.layout()?;
                    IcebergColumnSource::Mapped { index, layout }
                }
                None if field.is_nullable() => IcebergColumnSource::Unmapped,
                None => {
                    return Err(Report::new(IcebergColumnError::UnmappedRequired {
                        column: field.name().clone(),
                    }));
                }
            };
            columns.push(IcebergTableColumn {
                field: StdArc::clone(field),
                source,
            });
        }
        Ok(Self {
            staged: StdArc::clone(staged),
            table: StdArc::new(table_arrow),
            columns,
        })
    }

    /// `staged`, laid out as the table's columns in the table's order.
    pub(crate) fn project(
        &self,
        staged: &RecordBatch,
    ) -> error_stack::Result<RecordBatch, IcebergColumnError> {
        if staged.schema_ref().as_ref() != self.staged.as_ref() {
            return Err(Report::new(IcebergColumnError::Batch));
        }
        let rows = staged.num_rows();
        let mut columns = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let values = match &column.source {
                IcebergColumnSource::Mapped { index, layout } => {
                    let staged_values = staged
                        .columns()
                        .get(*index)
                        .verified("the batch carries the staged schema the plan indexes");
                    layout.write(staged_values).change_context_lazy(|| {
                        IcebergColumnError::Write {
                            column: column.field.name().clone(),
                        }
                    })?
                }
                IcebergColumnSource::Unmapped => new_null_array(column.field.data_type(), rows),
            };
            columns.push(values);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        RecordBatch::try_new_with_options(StdArc::clone(&self.table), columns, &options)
            .change_context(IcebergColumnError::Batch)
    }
}

/// One mapped column and the table column of its name, with the Iceberg schemas that name their
/// types.
struct IcebergMappedColumn<'a> {
    staged: &'a arrow_schema::Field,
    table: &'a FieldRef,
    /// The mapped columns' Iceberg form, with generated field IDs.
    mapped: &'a IcebergSchema,
    table_schema: &'a IcebergSchema,
}

impl IcebergMappedColumn<'_> {
    /// How the staged values are laid out as the table column's, or why they cannot be.
    fn layout(&self) -> error_stack::Result<IcebergValueLayout, IcebergColumnError> {
        let column = self.table.name();
        let mapped_type = &self
            .mapped
            .as_struct()
            .field_by_name(column)
            .assured("the mapped columns' Iceberg form holds every mapped column")
            .field_type;
        let table_type = &self
            .table_schema
            .as_struct()
            .field_by_name(column)
            .assured("the table schema's Arrow form names only the table's own columns")
            .field_type;
        if !Self::iceberg_types_match(mapped_type, table_type) {
            return Err(Report::new(IcebergColumnError::TypeMismatch {
                column: column.clone(),
                mapped: mapped_type.clone(),
                table: table_type.clone(),
            }));
        }
        if !self.table.is_nullable() && self.staged.is_nullable() {
            return Err(Report::new(IcebergColumnError::NullableIntoRequired {
                column: column.clone(),
            }));
        }
        match IcebergValueLayout::plan(self.staged.data_type(), self.table.data_type()) {
            Ok(layout) => Ok(layout),
            Err(IcebergLayoutRefusal::NullableNested) => Err(Report::new(
                IcebergColumnError::NullableNestedIntoRequired {
                    column: column.clone(),
                },
            )),
            Err(IcebergLayoutRefusal::Unwritable) => {
                Err(Report::new(IcebergColumnError::UnwritableLayout {
                    column: column.clone(),
                    data_type: self.staged.data_type().clone(),
                    table: table_type.clone(),
                }))
            }
        }
    }

    /// Whether a mapped column of Iceberg type `mapped` has the table column's type `table`: the
    /// same primitive, a list of the same element type, a struct of the same field names and
    /// types, or a map of the same key and value types.
    fn iceberg_types_match(mapped: &Type, table: &Type) -> bool {
        match (mapped, table) {
            (Type::Primitive(mapped), Type::Primitive(table)) => mapped == table,
            (Type::List(mapped), Type::List(table)) => Self::iceberg_types_match(
                &mapped.element_field.field_type,
                &table.element_field.field_type,
            ),
            (Type::Struct(mapped), Type::Struct(table)) => {
                if mapped.fields().len() != table.fields().len() {
                    return false;
                }
                for field in mapped.fields() {
                    let Some(target) = table.field_by_name(&field.name) else {
                        return false;
                    };
                    if !Self::iceberg_types_match(&field.field_type, &target.field_type) {
                        return false;
                    }
                }
                true
            }
            (Type::Map(mapped), Type::Map(table)) => {
                Self::iceberg_types_match(&mapped.key_field.field_type, &table.key_field.field_type)
                    && Self::iceberg_types_match(
                        &mapped.value_field.field_type,
                        &table.value_field.field_type,
                    )
            }
            _ => false,
        }
    }
}

/// Why staged values of one Arrow type cannot be laid out as a table type's, before the column is
/// named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IcebergLayoutRefusal {
    /// A value nested in the table type is required, and the staged one can be null.
    NullableNested,
    /// The sink does not lay the staged Arrow type out as the table's.
    Unwritable,
}

/// How the values of a staged column, or of a value nested in it, are laid out as the values of a
/// table column's Arrow type. Every layout keeps each value: it changes how Arrow arranges the
/// values, or widens an integer into the wider integer Iceberg holds it as.
#[derive(Debug)]
enum IcebergValueLayout {
    /// The staged values already have the table type's Arrow form.
    Same,
    /// Integers Iceberg holds as an `int` or a `long`, each widened to the same value.
    Widened(IcebergWidening),
    /// Binary values, whose offsets the table's Arrow form holds as 64-bit integers.
    LargeBinary,
    /// A list, as the table's list of the same elements.
    List {
        from: IcebergListForm,
        element: FieldRef,
        values: Box<IcebergValueLayout>,
    },
    /// A struct, its fields taken by name in the table's field order.
    Struct {
        fields: Fields,
        children: Vec<IcebergChildLayout>,
    },
    /// A map, its entries laid out as the table's entries of the same keys and values.
    Map {
        entries: FieldRef,
        ordered: bool,
        layout: Box<IcebergValueLayout>,
    },
}

/// The integer types Iceberg holds as a wider integer: `int` for the narrower integers and `long`
/// for an unsigned 32-bit one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IcebergWidening {
    Int8,
    Int16,
    UInt8,
    UInt16,
    UInt32,
}

/// How staged list values are arranged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IcebergListForm {
    /// Each list has its own length.
    Variable,
    /// Every list has the same length.
    Fixed,
}

/// One field of a table struct, and the staged field at `index` it is laid out from.
#[derive(Debug)]
struct IcebergChildLayout {
    index: usize,
    layout: IcebergValueLayout,
}

impl IcebergValueLayout {
    /// How staged values of Arrow type `staged` are laid out as values of the table's Arrow type
    /// `table`, which Iceberg holds as the same type.
    fn plan(staged: &DataType, table: &DataType) -> Result<Self, IcebergLayoutRefusal> {
        if staged == table {
            return Ok(Self::Same);
        }
        match (staged, table) {
            (DataType::Int8, DataType::Int32) => Ok(Self::Widened(IcebergWidening::Int8)),
            (DataType::Int16, DataType::Int32) => Ok(Self::Widened(IcebergWidening::Int16)),
            (DataType::UInt8, DataType::Int32) => Ok(Self::Widened(IcebergWidening::UInt8)),
            (DataType::UInt16, DataType::Int32) => Ok(Self::Widened(IcebergWidening::UInt16)),
            (DataType::UInt32, DataType::Int64) => Ok(Self::Widened(IcebergWidening::UInt32)),
            (DataType::Binary, DataType::LargeBinary) => Ok(Self::LargeBinary),
            (DataType::List(staged), DataType::List(table)) => {
                Self::list(IcebergListForm::Variable, staged, table)
            }
            (DataType::FixedSizeList(staged, _), DataType::List(table)) => {
                Self::list(IcebergListForm::Fixed, staged, table)
            }
            (DataType::Struct(staged), DataType::Struct(table)) => Self::structure(staged, table),
            (DataType::Map(staged, _), DataType::Map(table, ordered)) => {
                Self::map(staged, table, *ordered)
            }
            _ => Err(IcebergLayoutRefusal::Unwritable),
        }
    }

    fn list(
        from: IcebergListForm,
        staged: &FieldRef,
        table: &FieldRef,
    ) -> Result<Self, IcebergLayoutRefusal> {
        if !table.is_nullable() && staged.is_nullable() {
            return Err(IcebergLayoutRefusal::NullableNested);
        }
        let values = Self::plan(staged.data_type(), table.data_type())?;
        Ok(Self::List {
            from,
            element: StdArc::clone(table),
            values: Box::new(values),
        })
    }

    fn structure(staged: &Fields, table: &Fields) -> Result<Self, IcebergLayoutRefusal> {
        if staged.len() != table.len() {
            return Err(IcebergLayoutRefusal::Unwritable);
        }
        let mut children = Vec::with_capacity(table.len());
        for field in table {
            // The scan is bounded by the fields of one struct of the table's schema, and the
            // projection is planned once per commit, never per row.
            let Some((index, staged_field)) = staged.find(field.name()) else {
                return Err(IcebergLayoutRefusal::Unwritable);
            };
            if !field.is_nullable() && staged_field.is_nullable() {
                return Err(IcebergLayoutRefusal::NullableNested);
            }
            let layout = Self::plan(staged_field.data_type(), field.data_type())?;
            children.push(IcebergChildLayout { index, layout });
        }
        Ok(Self::Struct {
            fields: table.clone(),
            children,
        })
    }

    /// A map's entries are its keys and its values, in that order, whatever each Arrow layout names
    /// them, so they are matched by position rather than by name.
    fn map(
        staged: &FieldRef,
        table: &FieldRef,
        ordered: bool,
    ) -> Result<Self, IcebergLayoutRefusal> {
        let (DataType::Struct(staged_entries), DataType::Struct(table_entries)) =
            (staged.data_type(), table.data_type())
        else {
            return Err(IcebergLayoutRefusal::Unwritable);
        };
        let ([staged_key, staged_value], [table_key, table_value]) =
            (staged_entries.as_ref(), table_entries.as_ref())
        else {
            return Err(IcebergLayoutRefusal::Unwritable);
        };
        if !table_value.is_nullable() && staged_value.is_nullable() {
            return Err(IcebergLayoutRefusal::NullableNested);
        }
        let keys = Self::plan(staged_key.data_type(), table_key.data_type())?;
        let values = Self::plan(staged_value.data_type(), table_value.data_type())?;
        let layout = Self::Struct {
            fields: table_entries.clone(),
            children: vec![
                IcebergChildLayout {
                    index: 0,
                    layout: keys,
                },
                IcebergChildLayout {
                    index: 1,
                    layout: values,
                },
            ],
        };
        Ok(Self::Map {
            entries: StdArc::clone(table),
            ordered,
            layout: Box::new(layout),
        })
    }

    /// `values`, laid out in the table type's Arrow form. The caller checked that `values` has the
    /// staged type this layout was planned from.
    fn write(&self, values: &ArrayRef) -> Result<ArrayRef, ArrowError> {
        match self {
            Self::Same => Ok(StdArc::clone(values)),
            Self::Widened(widening) => Ok(widening.widen(values)),
            Self::LargeBinary => Self::large_binary(values),
            Self::List {
                from,
                element,
                values: layout,
            } => Self::write_list(*from, element, layout, values),
            Self::Struct { fields, children } => Self::write_struct(fields, children, values),
            Self::Map {
                entries,
                ordered,
                layout,
            } => Self::write_map(entries, *ordered, layout, values),
        }
    }

    fn large_binary(values: &ArrayRef) -> Result<ArrayRef, ArrowError> {
        let binary = values
            .as_binary_opt::<i32>()
            .verified("the layout was planned from a binary column");
        let mut offsets = Vec::with_capacity(binary.offsets().len());
        for offset in binary.offsets().iter() {
            offsets.push(i64::from(*offset));
        }
        // Widening keeps every offset, so they still start at zero or above and never decrease.
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let large =
            LargeBinaryArray::try_new(offsets, binary.values().clone(), binary.nulls().cloned())?;
        Ok(StdArc::new(large))
    }

    fn write_list(
        from: IcebergListForm,
        element: &FieldRef,
        layout: &Self,
        values: &ArrayRef,
    ) -> Result<ArrayRef, ArrowError> {
        match from {
            IcebergListForm::Variable => {
                let list = values
                    .as_list_opt::<i32>()
                    .verified("the layout was planned from a list column");
                let items = layout.write(list.values())?;
                let list = ListArray::try_new(
                    StdArc::clone(element),
                    list.offsets().clone(),
                    items,
                    list.nulls().cloned(),
                )?;
                Ok(StdArc::new(list))
            }
            IcebergListForm::Fixed => {
                let list = values
                    .as_fixed_size_list_opt()
                    .verified("the layout was planned from a fixed-size list column");
                let items = layout.write(list.values())?;
                let offsets = Self::fixed_list_offsets(list.len(), list.value_length())?;
                let list = ListArray::try_new(
                    StdArc::clone(element),
                    offsets,
                    items,
                    list.nulls().cloned(),
                )?;
                Ok(StdArc::new(list))
            }
        }
    }

    /// The offsets of `lists` lists of `length` elements each. A null list keeps its elements, as
    /// a fixed-size list holds them, and the writer reads none of a null list's elements.
    fn fixed_list_offsets(lists: usize, length: i32) -> Result<OffsetBuffer<i32>, ArrowError> {
        let capacity = lists
            .checked_add(1)
            .assured("a batch holds fewer lists than the largest usize");
        let mut offsets = Vec::with_capacity(capacity);
        let mut offset = 0_i32;
        offsets.push(offset);
        for _ in 0..lists {
            let Some(next) = offset.checked_add(length) else {
                return Err(ArrowError::ComputeError(
                    "fixed-size list elements exceed the offsets an Iceberg list holds".to_string(),
                ));
            };
            offset = next;
            offsets.push(offset);
        }
        // An Arrow list length is never negative, so the offsets start at zero and never decrease.
        Ok(OffsetBuffer::new(ScalarBuffer::from(offsets)))
    }

    fn write_struct(
        fields: &Fields,
        children: &[IcebergChildLayout],
        values: &ArrayRef,
    ) -> Result<ArrayRef, ArrowError> {
        let row = values
            .as_struct_opt()
            .verified("the layout was planned from a struct column");
        let mut columns = Vec::with_capacity(children.len());
        for child in children {
            let staged = row
                .columns()
                .get(child.index)
                .verified("the layout indexes the staged struct it was planned from");
            columns.push(child.layout.write(staged)?);
        }
        let row = StructArray::try_new(fields.clone(), columns, row.nulls().cloned())?;
        Ok(StdArc::new(row))
    }

    fn write_map(
        entries: &FieldRef,
        ordered: bool,
        layout: &Self,
        values: &ArrayRef,
    ) -> Result<ArrayRef, ArrowError> {
        let map = values
            .as_map_opt()
            .verified("the layout was planned from a map column");
        let staged_entries: ArrayRef = StdArc::new(map.entries().clone());
        let laid_out = layout.write(&staged_entries)?;
        let laid_out = laid_out
            .as_struct_opt()
            .assured("a map's entries are laid out as a struct")
            .clone();
        let map = MapArray::try_new(
            StdArc::clone(entries),
            map.offsets().clone(),
            laid_out,
            map.nulls().cloned(),
            ordered,
        )?;
        Ok(StdArc::new(map))
    }
}

impl IcebergWidening {
    /// `values`, each widened to the same value of the integer Iceberg holds it as.
    fn widen(self, values: &ArrayRef) -> ArrayRef {
        let reason = "the layout was planned from a column of this integer type";
        match self {
            Self::Int8 => StdArc::new(
                values
                    .as_primitive_opt::<Int8Type>()
                    .verified(reason)
                    .unary::<_, Int32Type>(i32::from),
            ),
            Self::Int16 => StdArc::new(
                values
                    .as_primitive_opt::<Int16Type>()
                    .verified(reason)
                    .unary::<_, Int32Type>(i32::from),
            ),
            Self::UInt8 => StdArc::new(
                values
                    .as_primitive_opt::<UInt8Type>()
                    .verified(reason)
                    .unary::<_, Int32Type>(i32::from),
            ),
            Self::UInt16 => StdArc::new(
                values
                    .as_primitive_opt::<UInt16Type>()
                    .verified(reason)
                    .unary::<_, Int32Type>(i32::from),
            ),
            Self::UInt32 => StdArc::new(
                values
                    .as_primitive_opt::<UInt32Type>()
                    .verified(reason)
                    .unary::<_, Int64Type>(i64::from),
            ),
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests;
