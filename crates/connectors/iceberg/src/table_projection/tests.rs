//! The projection of staged batches onto a table's current schema, planned from the staged Arrow
//! schema and the table's Iceberg schema alone, without a catalog.

use ::iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, StructType};
use arrow_array::{
    BinaryArray, FixedSizeListArray, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, StringArray, UInt8Array, UInt16Array, UInt32Array,
    builder::{Int64Builder, ListBuilder, MapBuilder, StringBuilder},
};
use arrow_buffer::NullBuffer;
use arrow_schema::Field;
use meticulous::ResultExt as _;

use super::*;
use crate::commit_tests::description;

fn primitive(primitive: PrimitiveType) -> Type {
    Type::Primitive(primitive)
}

/// The table schema of `fields`, in their order.
fn table(fields: Vec<NestedField>) -> IcebergSchema {
    IcebergSchema::builder()
        .with_fields(fields.into_iter().map(StdArc::new))
        .build()
        .assured("the fixture's table schema is valid")
}

/// The staged schema of `fields`, in `VALUES` order.
fn staged(fields: Vec<Field>) -> SchemaRef {
    StdArc::new(arrow_schema::Schema::new(fields))
}

/// The table's schema in the Arrow form its data files are written in.
fn table_arrow(table: &IcebergSchema) -> arrow_schema::Schema {
    schema_to_arrow_schema(table).assured("the fixture's table schema has an Arrow form")
}

/// The projection of `staged` onto `table`, which the fixture plans to succeed.
fn projection(staged: &SchemaRef, table: &IcebergSchema) -> IcebergTableProjection {
    IcebergTableProjection::plan(staged, table)
        .assured("the fixture's mapped columns can be written into its table")
}

/// Why planning `staged` against `table` is refused.
fn refusal(staged: &SchemaRef, table: &IcebergSchema) -> IcebergColumnError {
    let refused = IcebergTableProjection::plan(staged, table)
        .expect_err("the fixture's mapped columns cannot be written into its table");
    refused.current_context().clone()
}

/// The staged batch of `columns`, which carry `staged`.
fn batch(staged: &SchemaRef, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(StdArc::clone(staged), columns)
        .assured("the fixture's columns carry its staged schema")
}

/// `staged`, laid out by `projection`.
fn project(projection: &IcebergTableProjection, staged: &RecordBatch) -> RecordBatch {
    projection
        .project(staged)
        .assured("a planned projection lays out the batches of its staged schema")
}

#[test]
fn a_staged_batch_is_laid_out_in_the_table_order_with_null_in_each_optional_column_left_out() {
    let table = table(vec![
        NestedField::required(1, "id", primitive(PrimitiveType::Long)),
        NestedField::optional(2, "name", primitive(PrimitiveType::String)),
        NestedField::optional(3, "note", primitive(PrimitiveType::String)),
    ]);
    let staged = staged(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
    ]);
    let projection = projection(&staged, &table);
    let staged_batch = batch(
        &staged,
        vec![
            StdArc::new(StringArray::from(vec![Some("first"), None])),
            StdArc::new(Int64Array::from(vec![1, 2])),
        ],
    );

    let projected = project(&projection, &staged_batch);

    assert_eq!(projected.schema().as_ref(), &table_arrow(&table));
    let id: ArrayRef = StdArc::new(Int64Array::from(vec![1, 2]));
    let name: ArrayRef = StdArc::new(StringArray::from(vec![Some("first"), None]));
    let note: ArrayRef = StdArc::new(StringArray::from(vec![None::<&str>, None]));
    assert_eq!(projected.column(0), &id);
    assert_eq!(projected.column(1), &name);
    assert_eq!(projected.column(2), &note);
}

#[test]
fn every_kind_of_optional_column_left_out_is_written_as_null() {
    let tags =
        ListType::new(NestedField::list_element(4, primitive(PrimitiveType::String), true).into());
    let labels = MapType::optional(
        5,
        primitive(PrimitiveType::String),
        6,
        primitive(PrimitiveType::Long),
    );
    let dimension = StructType::new(vec![
        NestedField::required(7, "tag", primitive(PrimitiveType::String)).into(),
    ]);
    let table = table(vec![
        NestedField::required(1, "id", primitive(PrimitiveType::Long)),
        NestedField::optional(2, "tags", Type::List(tags)),
        NestedField::optional(3, "labels", Type::Map(labels)),
        NestedField::optional(8, "dimension", Type::Struct(dimension)),
    ]);
    let staged = staged(vec![Field::new("id", DataType::Int64, false)]);
    let projection = projection(&staged, &table);

    let projected = project(
        &projection,
        &batch(&staged, vec![StdArc::new(Int64Array::from(vec![7, 8, 9]))]),
    );

    assert_eq!(projected.schema().as_ref(), &table_arrow(&table));
    assert_eq!(projected.num_rows(), 3);
    for column in projected.columns().iter().skip(1) {
        assert_eq!(column.null_count(), 3, "{:?}", column.data_type());
    }
}

#[test]
fn integers_iceberg_holds_wider_keep_every_value() {
    let table = table(vec![
        NestedField::optional(1, "tiny", primitive(PrimitiveType::Int)),
        NestedField::optional(2, "small", primitive(PrimitiveType::Int)),
        NestedField::optional(3, "byte", primitive(PrimitiveType::Int)),
        NestedField::optional(4, "word", primitive(PrimitiveType::Int)),
        NestedField::optional(5, "count", primitive(PrimitiveType::Long)),
    ]);
    let staged = staged(vec![
        Field::new("count", DataType::UInt32, true),
        Field::new("word", DataType::UInt16, true),
        Field::new("byte", DataType::UInt8, true),
        Field::new("small", DataType::Int16, true),
        Field::new("tiny", DataType::Int8, true),
    ]);
    let projection = projection(&staged, &table);
    let staged_batch = batch(
        &staged,
        vec![
            StdArc::new(UInt32Array::from(vec![Some(0), None, Some(u32::MAX)])),
            StdArc::new(UInt16Array::from(vec![Some(0), None, Some(u16::MAX)])),
            StdArc::new(UInt8Array::from(vec![Some(0), None, Some(u8::MAX)])),
            StdArc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])),
            StdArc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])),
        ],
    );

    let projected = project(&projection, &staged_batch);

    let tiny: ArrayRef = StdArc::new(Int32Array::from(vec![Some(-128), None, Some(127)]));
    let small: ArrayRef = StdArc::new(Int32Array::from(vec![Some(-32_768), None, Some(32_767)]));
    let byte: ArrayRef = StdArc::new(Int32Array::from(vec![Some(0), None, Some(255)]));
    let word: ArrayRef = StdArc::new(Int32Array::from(vec![Some(0), None, Some(65_535)]));
    let count: ArrayRef = StdArc::new(Int64Array::from(vec![Some(0), None, Some(4_294_967_295)]));
    assert_eq!(projected.column(0), &tiny);
    assert_eq!(projected.column(1), &small);
    assert_eq!(projected.column(2), &byte);
    assert_eq!(projected.column(3), &word);
    assert_eq!(projected.column(4), &count);
}

#[test]
fn binary_values_keep_their_bytes_and_nulls_in_the_large_binary_layout_of_the_table() {
    let table = table(vec![NestedField::optional(
        1,
        "payload",
        primitive(PrimitiveType::Binary),
    )]);
    let staged = staged(vec![Field::new("payload", DataType::Binary, true)]);
    let projection = projection(&staged, &table);
    let values = BinaryArray::from(vec![
        Some(b"sliced away".as_ref()),
        Some(b"".as_ref()),
        None,
        Some([0_u8, 255].as_ref()),
    ]);
    // A slice keeps the whole values buffer, so its first offset is not zero.
    let sliced: ArrayRef = StdArc::new(values.slice(1, 3));

    let projected = project(&projection, &batch(&staged, vec![sliced]));

    let payload: ArrayRef = StdArc::new(LargeBinaryArray::from(vec![
        Some(b"".as_ref()),
        None,
        Some([0_u8, 255].as_ref()),
    ]));
    assert_eq!(projected.column(0), &payload);
}

#[test]
fn a_fixed_size_list_is_written_as_a_list_of_the_same_elements() {
    let readings =
        ListType::new(NestedField::list_element(2, primitive(PrimitiveType::Long), true).into());
    let table = table(vec![NestedField::optional(
        1,
        "readings",
        Type::List(readings),
    )]);
    let item = StdArc::new(Field::new("item", DataType::Int64, false));
    let staged = staged(vec![Field::new(
        "readings",
        DataType::FixedSizeList(StdArc::clone(&item), 2),
        true,
    )]);
    let projection = projection(&staged, &table);
    let values = FixedSizeListArray::try_new(
        item,
        2,
        StdArc::new(Int64Array::from(vec![9, 9, 1, 2, 0, 0, 3, 4])),
        Some(NullBuffer::from(vec![true, true, false, true])),
    )
    .assured("four lists of two elements hold eight elements");
    let sliced: ArrayRef = StdArc::new(values.slice(1, 3));

    let projected = project(&projection, &batch(&staged, vec![sliced]));

    let DataType::List(element) = table_arrow(&table).field(0).data_type().clone() else {
        panic!("an Iceberg list has an Arrow list form");
    };
    let readings: ArrayRef = StdArc::new(
        ListArray::try_new(
            element,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4, 6])),
            StdArc::new(Int64Array::from(vec![1, 2, 0, 0, 3, 4])),
            Some(NullBuffer::from(vec![true, false, true])),
        )
        .assured("three lists of two elements hold six elements"),
    );
    assert_eq!(projected.column(0), &readings);
}

#[test]
fn list_elements_take_the_table_element_field() {
    let tags =
        ListType::new(NestedField::list_element(2, primitive(PrimitiveType::String), false).into());
    let table = table(vec![NestedField::optional(1, "tags", Type::List(tags))]);
    let mut builder = ListBuilder::new(StringBuilder::new()).with_field(StdArc::new(Field::new(
        "item",
        DataType::Utf8,
        false,
    )));
    builder.values().append_value("a");
    builder.values().append_value("b");
    builder.append(true);
    builder.append(true);
    builder.append(false);
    let values: ArrayRef = StdArc::new(builder.finish());
    let staged = staged(vec![Field::new("tags", values.data_type().clone(), true)]);
    let projection = projection(&staged, &table);

    let projected = project(&projection, &batch(&staged, vec![values]));

    let table_field = table_arrow(&table).field(0).clone();
    assert_eq!(projected.column(0).data_type(), table_field.data_type());
    let tags = projected
        .column(0)
        .as_list_opt::<i32>()
        .assured("the projected column is a list");
    let first: ArrayRef = StdArc::new(StringArray::from(vec!["a", "b"]));
    assert_eq!(&tags.value(0), &first);
    assert_eq!(tags.value(1).len(), 0);
    assert!(tags.is_null(2));
}

#[test]
fn a_required_nested_value_is_refused_where_the_staged_one_can_be_null() {
    let tags =
        ListType::new(NestedField::list_element(2, primitive(PrimitiveType::String), true).into());
    let table = table(vec![NestedField::optional(1, "tags", Type::List(tags))]);
    let staged = staged(vec![Field::new(
        "tags",
        DataType::List(StdArc::new(Field::new("item", DataType::Utf8, true))),
        true,
    )]);

    assert_eq!(
        refusal(&staged, &table),
        IcebergColumnError::NullableNestedIntoRequired {
            column: "tags".to_string()
        }
    );
}

#[test]
fn struct_fields_are_taken_by_name_in_the_table_field_order() {
    let dimension = StructType::new(vec![
        NestedField::optional(2, "tag", primitive(PrimitiveType::String)).into(),
        NestedField::optional(3, "score", primitive(PrimitiveType::Long)).into(),
    ]);
    let table = table(vec![NestedField::optional(
        1,
        "dimension",
        Type::Struct(dimension),
    )]);
    let score = StdArc::new(Field::new("score", DataType::Int64, true));
    let tag = StdArc::new(Field::new("tag", DataType::Utf8, true));
    let staged_scores: ArrayRef = StdArc::new(Int64Array::from(vec![1, 2]));
    let staged_tags: ArrayRef = StdArc::new(StringArray::from(vec!["x", "y"]));
    let values: ArrayRef = StdArc::new(StructArray::from(vec![
        (score, staged_scores),
        (tag, staged_tags),
    ]));
    let staged = staged(vec![Field::new(
        "dimension",
        values.data_type().clone(),
        true,
    )]);
    let projection = projection(&staged, &table);

    let projected = project(&projection, &batch(&staged, vec![values]));

    assert_eq!(
        projected.column(0).data_type(),
        table_arrow(&table).field(0).data_type()
    );
    let dimension = projected
        .column(0)
        .as_struct_opt()
        .assured("the projected column is a struct");
    let tags: ArrayRef = StdArc::new(StringArray::from(vec!["x", "y"]));
    let scores: ArrayRef = StdArc::new(Int64Array::from(vec![1, 2]));
    assert_eq!(dimension.column(0), &tags);
    assert_eq!(dimension.column(1), &scores);
}

#[test]
fn map_entries_take_the_table_key_and_value_fields() {
    let labels = MapType::optional(
        2,
        primitive(PrimitiveType::String),
        3,
        primitive(PrimitiveType::Long),
    );
    let table = table(vec![NestedField::optional(1, "labels", Type::Map(labels))]);
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    builder.keys().append_value("first");
    builder.values().append_value(1);
    builder.keys().append_value("second");
    builder.values().append_null();
    builder
        .append(true)
        .assured("the entry's key and value are appended");
    builder.append(false).assured("a null map holds no entry");
    let values: ArrayRef = StdArc::new(builder.finish());
    let staged = staged(vec![Field::new("labels", values.data_type().clone(), true)]);
    let projection = projection(&staged, &table);

    let projected = project(&projection, &batch(&staged, vec![values]));

    assert_eq!(
        projected.column(0).data_type(),
        table_arrow(&table).field(0).data_type()
    );
    let labels = projected
        .column(0)
        .as_map_opt()
        .assured("the projected column is a map");
    let keys: ArrayRef = StdArc::new(StringArray::from(vec!["first", "second"]));
    let counts: ArrayRef = StdArc::new(Int64Array::from(vec![Some(1), None]));
    assert_eq!(labels.keys(), &keys);
    assert_eq!(labels.values(), &counts);
    assert!(labels.is_null(1));
}

#[test]
fn a_batch_without_the_planned_staged_schema_is_refused() {
    let table = table(vec![NestedField::optional(
        1,
        "id",
        primitive(PrimitiveType::Long),
    )]);
    let planned = staged(vec![Field::new("id", DataType::Int64, true)]);
    let projection = projection(&planned, &table);
    let other = staged(vec![Field::new("id", DataType::Int64, false)]);

    let refused = projection
        .project(&batch(&other, vec![StdArc::new(Int64Array::from(vec![1]))]))
        .expect_err("a batch of another staged schema is not laid out");

    assert_eq!(refused.current_context(), &IcebergColumnError::Batch);
}

#[test]
fn a_staged_layout_the_sink_does_not_lay_out_is_refused_naming_its_arrow_type() {
    let table = table(vec![NestedField::optional(
        1,
        "name",
        primitive(PrimitiveType::String),
    )]);
    let staged = staged(vec![Field::new("name", DataType::LargeUtf8, true)]);

    assert_eq!(
        refusal(&staged, &table),
        IcebergColumnError::UnwritableLayout {
            column: "name".to_string(),
            data_type: DataType::LargeUtf8,
            table: Box::new(primitive(PrimitiveType::String)),
        }
    );
}

#[test]
fn each_refusal_names_the_column_it_concerns() {
    let table = table(vec![
        NestedField::required(1, "id", primitive(PrimitiveType::Long)),
        NestedField::optional(2, "action", primitive(PrimitiveType::String)),
    ]);

    assert_eq!(
        refusal(
            &staged(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("other", DataType::Int64, true),
            ]),
            &table
        ),
        IcebergColumnError::Missing {
            column: "other".to_string()
        }
    );
    assert_eq!(
        refusal(
            &staged(vec![Field::new("id", DataType::Utf8, false)]),
            &table
        ),
        IcebergColumnError::TypeMismatch {
            column: "id".to_string(),
            mapped: Box::new(primitive(PrimitiveType::String)),
            table: Box::new(primitive(PrimitiveType::Long)),
        }
    );
    assert_eq!(
        refusal(
            &staged(vec![Field::new("action", DataType::Utf8, true)]),
            &table
        ),
        IcebergColumnError::UnmappedRequired {
            column: "id".to_string()
        }
    );
    assert_eq!(
        refusal(
            &staged(vec![Field::new("id", DataType::Int64, true)]),
            &table
        ),
        IcebergColumnError::NullableIntoRequired {
            column: "id".to_string()
        }
    );
}

#[test]
fn an_open_or_commit_refusal_carries_its_own_description_for_the_emitter() {
    let table = table(vec![NestedField::required(
        1,
        "id",
        primitive(PrimitiveType::Long),
    )]);
    let staged = staged(vec![Field::new("id", DataType::Int64, true)]);
    let expected = "table column 'id' is required, but the VALUES expression mapped into it can \
                    produce a null";

    let refused = IcebergTableProjection::plan(&staged, &table)
        .expect_err("a nullable mapping into a required column is refused");
    let open = IcebergColumnError::start_failure(refused);
    assert_eq!(
        open.current_context(),
        &SinkStartError::InvalidConfiguration { sink: ICEBERG }
    );
    assert_eq!(description(&open).as_deref(), Some(expected));
    assert!(open.contains::<IcebergColumnError>());

    let refused = IcebergTableProjection::plan(&staged, &table)
        .expect_err("a nullable mapping into a required column is refused");
    let commit = IcebergColumnError::commit_failure(refused);
    assert_eq!(
        commit.current_context(),
        &SinkPublishError::Commit { sink: ICEBERG }
    );
    assert_eq!(description(&commit).as_deref(), Some(expected));
    assert!(commit.contains::<IcebergColumnError>());
}

#[test]
fn a_struct_is_refused_when_its_fields_are_not_the_tables_or_can_be_null_where_it_requires_one() {
    let table: Fields = vec![
        Field::new("tag", DataType::Utf8, false),
        Field::new("score", DataType::Int64, true),
    ]
    .into();
    let fewer: Fields = vec![Field::new("tag", DataType::Utf8, false)].into();
    let renamed: Fields = vec![
        Field::new("label", DataType::Utf8, false),
        Field::new("score", DataType::Int64, true),
    ]
    .into();
    let nullable_tag: Fields = vec![
        Field::new("tag", DataType::Utf8, true),
        Field::new("score", DataType::Int64, true),
    ]
    .into();
    let table = DataType::Struct(table);

    let refusal =
        |staged: Fields| IcebergValueLayout::plan(&DataType::Struct(staged), &table).err();

    assert_eq!(refusal(fewer), Some(IcebergLayoutRefusal::Unwritable));
    assert_eq!(refusal(renamed), Some(IcebergLayoutRefusal::Unwritable));
    assert_eq!(
        refusal(nullable_tag),
        Some(IcebergLayoutRefusal::NullableNested)
    );
}

#[test]
fn a_map_value_the_table_requires_is_refused_where_the_staged_one_can_be_null() {
    let labels = MapType::required(
        2,
        primitive(PrimitiveType::String),
        3,
        primitive(PrimitiveType::Long),
    );
    let table = table(vec![NestedField::optional(1, "labels", Type::Map(labels))]);
    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    let empty = builder.finish();
    let staged = staged(vec![Field::new("labels", empty.data_type().clone(), true)]);

    assert_eq!(
        refusal(&staged, &table),
        IcebergColumnError::NullableNestedIntoRequired {
            column: "labels".to_string()
        }
    );
}

#[test]
fn a_map_layout_needs_entries_of_one_key_and_one_value() {
    let entries = |fields: Vec<Field>| {
        DataType::Map(
            StdArc::new(Field::new(
                "entries",
                DataType::Struct(fields.into()),
                false,
            )),
            false,
        )
    };
    let table = entries(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
    ]);
    let three = entries(vec![
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", DataType::Int64, true),
        Field::new("extra", DataType::Int64, true),
    ]);
    let not_a_struct = DataType::Map(
        StdArc::new(Field::new("entries", DataType::Int64, false)),
        false,
    );

    assert_eq!(
        IcebergValueLayout::plan(&three, &table).err(),
        Some(IcebergLayoutRefusal::Unwritable)
    );
    assert_eq!(
        IcebergValueLayout::plan(&not_a_struct, &table).err(),
        Some(IcebergLayoutRefusal::Unwritable)
    );
}

#[test]
fn fixed_size_lists_whose_elements_pass_the_offsets_a_list_holds_are_refused() {
    let offsets = IcebergValueLayout::fixed_list_offsets(3, 2)
        .assured("three lists of two elements hold six elements");
    assert_eq!(offsets.as_ref(), &[0, 2, 4, 6]);

    let refused = IcebergValueLayout::fixed_list_offsets(3, i32::MAX / 2)
        .expect_err("three lists of a third of the offset range pass it");
    assert!(matches!(refused, ArrowError::ComputeError(_)));
}

#[test]
fn a_column_whose_values_cannot_be_laid_out_names_it() {
    // The plan never lays nullable elements out as required ones; a projection built around that
    // shows how any write failure is reported.
    let item = StdArc::new(Field::new("item", DataType::Int64, true));
    let element = StdArc::new(Field::new("element", DataType::Int64, false));
    let staged = staged(vec![Field::new(
        "readings",
        DataType::List(StdArc::clone(&item)),
        true,
    )]);
    let field = StdArc::new(Field::new(
        "readings",
        DataType::List(StdArc::clone(&element)),
        true,
    ));
    let projection = IcebergTableProjection {
        staged: StdArc::clone(&staged),
        table: StdArc::new(arrow_schema::Schema::new(vec![StdArc::clone(&field)])),
        columns: vec![IcebergTableColumn {
            field,
            source: IcebergColumnSource::Mapped {
                index: 0,
                layout: IcebergValueLayout::List {
                    from: IcebergListForm::Variable,
                    element,
                    values: Box::new(IcebergValueLayout::Same),
                },
            },
        }],
    };
    let readings: ArrayRef = StdArc::new(
        ListArray::try_new(
            item,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            StdArc::new(Int64Array::from(vec![None])),
            None,
        )
        .assured("a nullable element may be null"),
    );

    let refused = projection
        .project(&batch(&staged, vec![readings]))
        .expect_err("a null element cannot be laid out as a required one");

    assert_eq!(
        refused.current_context(),
        &IcebergColumnError::Write {
            column: "readings".to_string()
        }
    );
    assert!(refused.contains::<ArrowError>());
}

/// The Iceberg primitives a generated table column has, each with the Arrow type its mapped column
/// is staged as: the same layout, a narrower integer or a binary layout the table lays out wider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratedKind {
    Long,
    Text,
    IntFromInt16,
    LongFromUInt32,
    Bytes,
}

impl GeneratedKind {
    fn of(kind: u8) -> Self {
        match kind % 5 {
            0 => Self::Long,
            1 => Self::Text,
            2 => Self::IntFromInt16,
            3 => Self::LongFromUInt32,
            _ => Self::Bytes,
        }
    }

    fn iceberg(self) -> Type {
        match self {
            Self::Long | Self::LongFromUInt32 => primitive(PrimitiveType::Long),
            Self::Text => primitive(PrimitiveType::String),
            Self::IntFromInt16 => primitive(PrimitiveType::Int),
            Self::Bytes => primitive(PrimitiveType::Binary),
        }
    }

    fn staged(self) -> DataType {
        match self {
            Self::Long => DataType::Int64,
            Self::Text => DataType::Utf8,
            Self::IntFromInt16 => DataType::Int16,
            Self::LongFromUInt32 => DataType::UInt32,
            Self::Bytes => DataType::Binary,
        }
    }

    /// The staged column of `values`, each one brought into this kind's range.
    fn staged_values(self, values: &[Option<i64>]) -> ArrayRef {
        match self {
            Self::Long => StdArc::new(Int64Array::from(values.to_vec())),
            Self::Text => {
                let texts = values
                    .iter()
                    .map(|value| value.map(|value| value.to_string()))
                    .collect::<Vec<_>>();
                StdArc::new(StringArray::from(texts))
            }
            Self::IntFromInt16 => {
                let mut narrowed = Vec::with_capacity(values.len());
                for value in values {
                    let Some(value) = value else {
                        narrowed.push(None);
                        continue;
                    };
                    let shifted = value.rem_euclid(1 << 16) - (1 << 15);
                    narrowed.push(Some(
                        i16::try_from(shifted)
                            .assured("a remainder modulo 2^16 less 2^15 fits i16"),
                    ));
                }
                StdArc::new(Int16Array::from(narrowed))
            }
            Self::LongFromUInt32 => {
                let mut narrowed = Vec::with_capacity(values.len());
                for value in values {
                    let Some(value) = value else {
                        narrowed.push(None);
                        continue;
                    };
                    narrowed.push(Some(
                        u32::try_from(value.rem_euclid(1 << 32))
                            .assured("a remainder modulo 2^32 fits u32"),
                    ));
                }
                StdArc::new(UInt32Array::from(narrowed))
            }
            Self::Bytes => {
                let bytes = values
                    .iter()
                    .map(|value| value.map(i64::to_le_bytes))
                    .collect::<Vec<_>>();
                let slices = bytes
                    .iter()
                    .map(|value| value.as_ref().map(<[u8; 8]>::as_slice))
                    .collect::<Vec<_>>();
                StdArc::new(BinaryArray::from(slices))
            }
        }
    }
}

/// One value a column holds, whichever Arrow layout holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum GeneratedCell {
    Null,
    Integer(i64),
    Text(String),
    Bytes(Vec<u8>),
}

impl GeneratedCell {
    /// Every value of `values`, in row order.
    fn all(values: &ArrayRef) -> Vec<Self> {
        let mut cells = Vec::with_capacity(values.len());
        for row in 0..values.len() {
            if values.is_null(row) {
                cells.push(Self::Null);
                continue;
            }
            let cell = match values.data_type() {
                DataType::Int16 => Self::Integer(i64::from(
                    values
                        .as_primitive_opt::<Int16Type>()
                        .assured("an Int16 column")
                        .value(row),
                )),
                DataType::Int32 => Self::Integer(i64::from(
                    values
                        .as_primitive_opt::<Int32Type>()
                        .assured("an Int32 column")
                        .value(row),
                )),
                DataType::UInt32 => Self::Integer(i64::from(
                    values
                        .as_primitive_opt::<UInt32Type>()
                        .assured("a UInt32 column")
                        .value(row),
                )),
                DataType::Int64 => Self::Integer(
                    values
                        .as_primitive_opt::<Int64Type>()
                        .assured("an Int64 column")
                        .value(row),
                ),
                DataType::Utf8 => Self::Text(
                    values
                        .as_string_opt::<i32>()
                        .assured("a Utf8 column")
                        .value(row)
                        .to_string(),
                ),
                DataType::Binary => Self::Bytes(
                    values
                        .as_binary_opt::<i32>()
                        .assured("a Binary column")
                        .value(row)
                        .to_vec(),
                ),
                DataType::LargeBinary => Self::Bytes(
                    values
                        .as_binary_opt::<i64>()
                        .assured("a LargeBinary column")
                        .value(row)
                        .to_vec(),
                ),
                other => panic!("the property generates no {other} column"),
            };
            cells.push(cell);
        }
        cells
    }
}

/// A generated table column: its kind, whether the table requires it, whether `VALUES` maps it,
/// whether the expression mapped into it can produce a null, and where it falls in `VALUES`.
#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedColumn {
    kind: u8,
    required: bool,
    mapped: bool,
    nullable: bool,
    position: u8,
}

/// A generated table of one to six columns, and the values of the rows staged into it.
#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedTable {
    #[generator(bolero::generator::produce_with::<Vec<GeneratedColumn>>().len(1_usize..=6))]
    columns: Vec<GeneratedColumn>,
    #[generator(bolero::generator::produce_with::<Vec<Option<i64>>>().len(0_usize..=8))]
    values: Vec<Option<i64>>,
}

impl GeneratedTable {
    fn name(index: usize) -> String {
        format!("c{index}")
    }

    fn kind(&self, index: usize) -> GeneratedKind {
        GeneratedKind::of(self.columns[index].kind)
    }

    fn table(&self) -> IcebergSchema {
        let mut fields = Vec::with_capacity(self.columns.len());
        for (index, column) in self.columns.iter().enumerate() {
            let id = i32::try_from(index + 1).assured("a generated table has at most six columns");
            let kind = self.kind(index).iceberg();
            if column.required {
                fields.push(NestedField::required(id, Self::name(index), kind));
            } else {
                fields.push(NestedField::optional(id, Self::name(index), kind));
            }
        }
        table(fields)
    }

    /// The table columns `VALUES` maps, in `VALUES` order.
    fn mapped(&self) -> Vec<usize> {
        let mut mapped = Vec::new();
        for (index, column) in self.columns.iter().enumerate() {
            if column.mapped {
                mapped.push(index);
            }
        }
        mapped.sort_by_key(|index| (self.columns[*index].position, *index));
        mapped
    }

    fn staged(&self, mapped: &[usize]) -> SchemaRef {
        let mut fields = Vec::with_capacity(mapped.len());
        for index in mapped {
            fields.push(Field::new(
                Self::name(*index),
                self.kind(*index).staged(),
                self.columns[*index].nullable,
            ));
        }
        staged(fields)
    }

    /// The first table column, in the table's order, that the mapped columns cannot write: a
    /// required one `VALUES` leaves out or maps from an expression that can produce a null.
    fn refusal(&self) -> Option<IcebergColumnError> {
        for (index, column) in self.columns.iter().enumerate() {
            if !column.required {
                continue;
            }
            if !column.mapped {
                return Some(IcebergColumnError::UnmappedRequired {
                    column: Self::name(index),
                });
            }
            if column.nullable {
                return Some(IcebergColumnError::NullableIntoRequired {
                    column: Self::name(index),
                });
            }
        }
        None
    }

    /// The staged values of the column at `index`: the generated values, with a null replaced by
    /// zero where the expression mapped into the column never produces one.
    fn staged_values(&self, index: usize) -> ArrayRef {
        let values = if self.columns[index].nullable {
            self.values.clone()
        } else {
            self.values
                .iter()
                .map(|value| Some(value.unwrap_or(0)))
                .collect()
        };
        self.kind(index).staged_values(&values)
    }
}

/// Over generated tables, column orders and subsets of mapped columns, a projection is refused
/// exactly when a required column is left out or mapped from an expression that can produce a
/// null, naming the first such column. Otherwise every staged value lands, unchanged, in the table
/// column of its name, in the table's Arrow form, and every column `VALUES` leaves out is null.
#[test]
fn bolero_staged_columns_land_in_the_table_columns_of_their_names() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .with_type::<GeneratedTable>()
        .for_each(|generated| {
            let table = generated.table();
            let mapped = generated.mapped();
            let staged = generated.staged(&mapped);
            let planned = IcebergTableProjection::plan(&staged, &table);
            let projection = match (planned, generated.refusal()) {
                (Ok(projection), None) => projection,
                (Err(refused), Some(expected)) => {
                    assert_eq!(refused.current_context(), &expected);
                    return;
                }
                (Ok(_), Some(expected)) => {
                    panic!("the plan admitted a table refused as {expected}")
                }
                (Err(refused), None) => panic!("the plan refused a writable table: {refused:?}"),
            };
            let mut columns = Vec::with_capacity(mapped.len());
            for index in &mapped {
                columns.push(generated.staged_values(*index));
            }
            // A table whose every column is optional can be written with no column mapped, so the
            // batch states its rows, as the sink's staging does.
            let options = RecordBatchOptions::new().with_row_count(Some(generated.values.len()));
            let staged_batch =
                RecordBatch::try_new_with_options(StdArc::clone(&staged), columns, &options)
                    .assured("the generated columns carry the generated staged schema");

            let projected = project(&projection, &staged_batch);

            assert_eq!(projected.schema().as_ref(), &table_arrow(&table));
            assert_eq!(projected.num_rows(), generated.values.len());
            for index in 0..generated.columns.len() {
                let written = GeneratedCell::all(projected.column(index));
                let position = mapped.iter().position(|mapped| *mapped == index);
                let Some(position) = position else {
                    assert!(
                        written.iter().all(|cell| *cell == GeneratedCell::Null),
                        "column {index} is left out and holds {written:?}"
                    );
                    continue;
                };
                let staged_cells = GeneratedCell::all(staged_batch.column(position));
                assert_eq!(written, staged_cells, "column {index}");
            }
        });
}
