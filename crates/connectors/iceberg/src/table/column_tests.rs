//! Which table column an Iceberg commit writes each mapped column into, and which tables the mapped
//! columns are refused for, driven through the sink against Iceberg's in-memory catalog.

use ::iceberg::{
    NamespaceIdent, TableCreation, TableIdent,
    spec::{NestedField, PrimitiveType, Schema, Type},
    transaction::AddColumn,
};
use arrow_array::{Int64Array, StringArray};
use arrow_schema::Field;
use futures_util::TryStreamExt as _;
use serde_json::json;

use super::{
    commit_tests::{description, memory_catalog},
    *,
};

const NAMESPACE: &str = "nervix";
const TABLE: &str = "columns";

fn table_ident() -> TableIdent {
    TableIdent::new(
        NamespaceIdent::new(NAMESPACE.to_string()),
        TABLE.to_string(),
    )
}

fn optional(id: i32, name: &str, primitive: PrimitiveType) -> NestedField {
    NestedField::optional(id, name, Type::Primitive(primitive))
}

fn required(id: i32, name: &str, primitive: PrimitiveType) -> NestedField {
    NestedField::required(id, name, Type::Primitive(primitive))
}

/// The fixture table, with the columns `fields` in their order.
async fn create_table(catalog: &dyn Catalog, fields: Vec<NestedField>) -> Table {
    let namespace = NamespaceIdent::new(NAMESPACE.to_string());
    catalog
        .create_namespace(&namespace, Default::default())
        .await
        .assured("the in-memory catalog creates the namespace");
    let schema = Schema::builder()
        .with_fields(fields.into_iter().map(StdArc::new))
        .build()
        .assured("the fixture schema is valid");
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(TABLE.to_string())
                .schema(schema)
                .build(),
        )
        .await
        .assured("the in-memory catalog creates the table")
}

/// A sink that stages the columns `mapped` and appends them to `table`, as an emitter whose
/// `VALUES` maps those columns, in that order, opens one.
fn fixture_emitter(
    catalog: StdArc<dyn Catalog>,
    table: Table,
    mapped: &arrow_schema::Schema,
) -> IcebergTableEmitter {
    IcebergTableEmitter {
        client: IcebergTableEmitterClient {
            catalog,
            table,
            file_name_prefix: "columns-memory".to_string(),
            data_file_sequence: 0,
        },
        commit_state: IcebergCommitState::default(),
        staged_schema: IcebergTableEmitter::staged_arrow_schema(mapped)
            .assured("the fixture's mapped columns are distinct"),
        commit_policy: IcebergCommitPolicy {
            interval: Duration::from_secs(60),
            max_size: 1 << 20,
        },
        batch_policy: None,
        staging_dir: tempfile::tempdir().assured("the staging directory is created"),
        executor: Executor::default(),
        staged_sequence: 0,
        staged_batches: Vec::new(),
        staged_rows: 0,
        staged_bytes: 0,
    }
}

/// Stages every row of `batch` as one carrier.
async fn stage(sink: &mut IcebergTableEmitter, batch: &RecordBatch) {
    let selected_rows = (0..batch.num_rows()).collect::<Vec<_>>();
    let target_columns = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let outcome = sink
        .publish(MappedEmitterRows {
            target_columns: &target_columns,
            carriers: vec![MappedEmitterCarrier {
                batch_index: 0,
                batch,
                selected_rows: &selected_rows,
                occurred_at: Timestamp::from_unix_nanos(1),
                acknowledgements: None,
            }],
        })
        .await;
    assert!(
        !outcome.has_infrastructure_error(),
        "the sink stages every row it is handed"
    );
}

/// One value of a fixture table column, as JSON.
fn cell(column: &dyn Array, row: usize) -> serde_json::Value {
    if column.is_null(row) {
        return serde_json::Value::Null;
    }
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        return json!(values.value(row));
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return json!(values.value(row));
    }
    panic!("the fixture tables hold only long and string columns, not {column:?}");
}

/// Every row of the fixture table, each column read back by its name.
async fn table_rows(catalog: &dyn Catalog) -> Vec<serde_json::Value> {
    let table = catalog
        .load_table(&table_ident())
        .await
        .assured("the fixture table exists");
    // A table no commit appended to has no snapshot to scan.
    if table.metadata().current_snapshot().is_none() {
        return Vec::new();
    }
    let batches: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .assured("the fixture table can be scanned")
        .to_arrow()
        .await
        .assured("the scan reads the fixture table")
        .try_collect()
        .await
        .assured("every data file of the fixture table is readable");
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut values = serde_json::Map::new();
            for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                values.insert(field.name().clone(), cell(column.as_ref(), row));
            }
            rows.push(serde_json::Value::Object(values));
        }
    }
    rows
}

/// Commits the schema change `change` applies to the fixture table, as an operator who evolves the
/// table after the emitter opened does.
async fn evolve_table(
    catalog: &dyn Catalog,
    change: impl FnOnce(Transaction) -> IcebergResult<Transaction>,
) {
    let table = catalog
        .load_table(&table_ident())
        .await
        .assured("the fixture table exists");
    change(Transaction::new(&table))
        .assured("the fixture's schema change is valid")
        .commit(catalog)
        .await
        .assured("the in-memory catalog applies the schema change");
}

#[nervix_primitives::test]
async fn a_commit_writes_each_mapped_column_into_the_table_column_of_its_name() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(memory_catalog().await);
    let table = create_table(
        catalog.as_ref(),
        vec![
            optional(1, "a", PrimitiveType::String),
            optional(2, "b", PrimitiveType::String),
        ],
    )
    .await;
    let mapped = arrow_schema::Schema::new(vec![
        Field::new("b", DataType::Utf8, true),
        Field::new("a", DataType::Utf8, true),
    ]);
    let mut sink = fixture_emitter(StdArc::clone(&catalog), table, &mapped);
    let batch = RecordBatch::try_new(
        StdArc::new(mapped),
        vec![
            StdArc::new(StringArray::from(vec!["written to b"])),
            StdArc::new(StringArray::from(vec!["written to a"])),
        ],
    )
    .assured("the columns match the mapped schema");
    stage(&mut sink, &batch).await;

    sink.commit()
        .await
        .assured("columns of one type in another order commit")
        .assured("the commit publishes the staged row");

    assert_eq!(
        table_rows(catalog.as_ref()).await,
        vec![json!({"a": "written to a", "b": "written to b"})]
    );
}

#[nervix_primitives::test]
async fn columns_of_different_types_mapped_out_of_the_table_order_commit_by_name() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(memory_catalog().await);
    let table = create_table(
        catalog.as_ref(),
        vec![
            optional(1, "user_id", PrimitiveType::Long),
            optional(2, "action", PrimitiveType::String),
        ],
    )
    .await;
    let mapped = arrow_schema::Schema::new(vec![
        Field::new("action", DataType::Utf8, true),
        Field::new("user_id", DataType::Int64, true),
    ]);
    let mut sink = fixture_emitter(StdArc::clone(&catalog), table, &mapped);
    let batch = RecordBatch::try_new(
        StdArc::new(mapped),
        vec![
            StdArc::new(StringArray::from(vec!["open", "close"])),
            StdArc::new(Int64Array::from(vec![7, 8])),
        ],
    )
    .assured("the columns match the mapped schema");
    stage(&mut sink, &batch).await;

    sink.commit()
        .await
        .assured("columns of different types in another order commit")
        .assured("the commit publishes the staged rows");

    assert_eq!(
        table_rows(catalog.as_ref()).await,
        vec![
            json!({"user_id": 7, "action": "open"}),
            json!({"user_id": 8, "action": "close"}),
        ]
    );
}

#[nervix_primitives::test]
async fn an_optional_table_column_values_leaves_unmapped_is_written_as_null() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(memory_catalog().await);
    let table = create_table(
        catalog.as_ref(),
        vec![
            optional(1, "user_id", PrimitiveType::Long),
            optional(2, "action", PrimitiveType::String),
        ],
    )
    .await;
    let mapped = arrow_schema::Schema::new(vec![Field::new("action", DataType::Utf8, true)]);
    let mut sink = fixture_emitter(StdArc::clone(&catalog), table, &mapped);
    let batch = RecordBatch::try_new(
        StdArc::new(mapped),
        vec![StdArc::new(StringArray::from(vec!["open"]))],
    )
    .assured("the column matches the mapped schema");
    stage(&mut sink, &batch).await;

    sink.commit()
        .await
        .assured("a table column VALUES leaves out does not fail the commit")
        .assured("the commit publishes the staged row");

    assert_eq!(
        table_rows(catalog.as_ref()).await,
        vec![json!({"user_id": null, "action": "open"})]
    );
}

#[nervix_primitives::test]
async fn a_nullable_mapping_into_a_required_column_fails_the_commit_instead_of_writing_a_value() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(memory_catalog().await);
    let table = create_table(
        catalog.as_ref(),
        vec![
            required(1, "user_id", PrimitiveType::Long),
            optional(2, "action", PrimitiveType::String),
        ],
    )
    .await;
    let mapped = arrow_schema::Schema::new(vec![
        Field::new("user_id", DataType::Int64, true),
        Field::new("action", DataType::Utf8, true),
    ]);
    let mut sink = fixture_emitter(StdArc::clone(&catalog), table, &mapped);
    let batch = RecordBatch::try_new(
        StdArc::new(mapped),
        vec![
            StdArc::new(Int64Array::from(vec![None])),
            StdArc::new(StringArray::from(vec!["open"])),
        ],
    )
    .assured("the columns match the mapped schema");
    stage(&mut sink, &batch).await;

    let refused = sink
        .commit()
        .await
        .err()
        .assured("a required column the mapping can leave null fails the commit");

    assert_eq!(
        refused.current_context(),
        &EmitterWriteError::Commit { connector: ICEBERG }
    );
    assert_eq!(
        description(&refused),
        Some(
            "table column 'user_id' is required, but the VALUES expression mapped into it can \
             produce a null"
                .to_string()
        )
    );
    assert_eq!(
        table_rows(catalog.as_ref()).await,
        Vec::<serde_json::Value>::new()
    );
    assert_eq!(sink.staged_rows, 1, "a refused commit keeps its staged row");
}

#[nervix_primitives::test]
async fn a_commit_writes_null_into_an_optional_column_added_after_open() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(memory_catalog().await);
    let table = create_table(
        catalog.as_ref(),
        vec![
            optional(1, "user_id", PrimitiveType::Long),
            optional(2, "action", PrimitiveType::String),
        ],
    )
    .await;
    let mapped = arrow_schema::Schema::new(vec![
        Field::new("user_id", DataType::Int64, true),
        Field::new("action", DataType::Utf8, true),
    ]);
    let mut sink = fixture_emitter(StdArc::clone(&catalog), table, &mapped);
    evolve_table(catalog.as_ref(), |transaction| {
        let added = transaction.update_schema().add_column(AddColumn::optional(
            "note",
            Type::Primitive(PrimitiveType::String),
        ));
        added.apply(transaction)
    })
    .await;
    let batch = RecordBatch::try_new(
        StdArc::new(mapped),
        vec![
            StdArc::new(Int64Array::from(vec![7])),
            StdArc::new(StringArray::from(vec!["open"])),
        ],
    )
    .assured("the columns match the mapped schema");
    stage(&mut sink, &batch).await;

    sink.commit()
        .await
        .assured("a column added after open does not fail the commit")
        .assured("the commit publishes the staged row");

    assert_eq!(
        table_rows(catalog.as_ref()).await,
        vec![json!({"user_id": 7, "action": "open", "note": null})]
    );
}

#[nervix_primitives::test]
async fn a_mapped_column_deleted_after_open_fails_the_commit_naming_it() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(memory_catalog().await);
    let table = create_table(
        catalog.as_ref(),
        vec![
            optional(1, "user_id", PrimitiveType::Long),
            optional(2, "action", PrimitiveType::String),
        ],
    )
    .await;
    let mapped = arrow_schema::Schema::new(vec![
        Field::new("user_id", DataType::Int64, true),
        Field::new("action", DataType::Utf8, true),
    ]);
    let mut sink = fixture_emitter(StdArc::clone(&catalog), table, &mapped);
    evolve_table(catalog.as_ref(), |transaction| {
        let deleted = transaction.update_schema().delete_column("action");
        deleted.apply(transaction)
    })
    .await;
    let batch = RecordBatch::try_new(
        StdArc::new(mapped),
        vec![
            StdArc::new(Int64Array::from(vec![7])),
            StdArc::new(StringArray::from(vec!["open"])),
        ],
    )
    .assured("the columns match the mapped schema");
    stage(&mut sink, &batch).await;

    let refused = sink
        .commit()
        .await
        .err()
        .assured("a mapped column the table no longer has fails the commit");

    assert_eq!(
        refused.current_context(),
        &EmitterWriteError::Commit { connector: ICEBERG }
    );
    assert_eq!(
        description(&refused),
        Some("mapped column 'action' does not exist in the Iceberg table".to_string())
    );
    assert_eq!(
        table_rows(catalog.as_ref()).await,
        Vec::<serde_json::Value>::new()
    );
    assert_eq!(sink.staged_rows, 1, "a refused commit keeps its staged row");
}

#[test]
fn the_open_check_refuses_a_required_table_column_values_leaves_unmapped() {
    let table = Schema::builder()
        .with_fields(vec![
            StdArc::new(required(1, "user_id", PrimitiveType::Long)),
            StdArc::new(optional(2, "action", PrimitiveType::String)),
        ])
        .build()
        .assured("the fixture schema is valid");
    let mapped = arrow_schema::Schema::new(vec![Field::new("action", DataType::Utf8, false)]);

    let error = IcebergTableEmitter::validate_table_schema(&mapped, &table)
        .expect_err("a required column VALUES leaves out fails the open");

    assert_eq!(
        *error.current_context(),
        EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
    );
    assert_eq!(
        description(&error),
        Some("table column 'user_id' is required, but VALUES does not map it".to_string())
    );
}

#[test]
fn the_open_check_refuses_a_nullable_mapping_into_a_required_column() {
    let table = Schema::builder()
        .with_fields(vec![
            StdArc::new(required(1, "user_id", PrimitiveType::Long)),
            StdArc::new(optional(2, "action", PrimitiveType::String)),
        ])
        .build()
        .assured("the fixture schema is valid");
    let nullable = arrow_schema::Schema::new(vec![
        Field::new("user_id", DataType::Int64, true),
        Field::new("action", DataType::Utf8, true),
    ]);

    let error = IcebergTableEmitter::validate_table_schema(&nullable, &table)
        .expect_err("a required column mapped from a nullable expression fails the open");

    assert_eq!(
        *error.current_context(),
        EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
    );
    assert_eq!(
        description(&error),
        Some(
            "table column 'user_id' is required, but the VALUES expression mapped into it can \
             produce a null"
                .to_string()
        )
    );

    // An expression that never produces a null may write the required column, and the optional
    // column takes a null either way.
    let never_null = arrow_schema::Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("action", DataType::Utf8, true),
    ]);
    IcebergTableEmitter::validate_table_schema(&never_null, &table)
        .assured("a never-null expression writes a required column");
}
