//! What an Iceberg commit charges the bulk memory class for, which staged batches it publishes,
//! and which staging files hold them, driven through the emitter against Iceberg's in-memory
//! catalog.

use ::iceberg::{
    MemoryCatalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
    spec::{NestedField, PrimitiveType, Schema, Type},
};
use arrow_array::{Int64Array, StringArray};
use arrow_schema::Field;
use error_stack::{AttachmentKind, FrameKind};
use futures_util::TryStreamExt as _;
use nervix_execution::{AdmissionError, ExecutionConfig, Reservation};
use nervix_primitives::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::*;

const NAMESPACE: &str = "nervix";
const TABLE: &str = "notifications";

/// What one acknowledgement handover was resolved as.
#[derive(Default)]
struct RecordedAcknowledgements {
    acknowledged: AtomicUsize,
    rejected: AtomicUsize,
}

impl RecordedAcknowledgements {
    fn acknowledged(&self) -> usize {
        self.acknowledged.load(Ordering::Acquire)
    }

    fn rejected(&self) -> usize {
        self.rejected.load(Ordering::Acquire)
    }
}

/// One acknowledgement handover the emitter holds, recording how it was resolved.
struct RecordedHandover(StdArc<RecordedAcknowledgements>);

impl EmitterRecordAcknowledgementServices for RecordedHandover {
    fn acknowledge(&self) {
        self.0.acknowledged.fetch_add(1, Ordering::AcqRel);
    }

    fn keep_alive(&self) {}

    fn reject(&self, _reason: String) {
        self.0.rejected.fetch_add(1, Ordering::AcqRel);
    }

    fn is_empty(&self) -> bool {
        false
    }
}

/// The in-memory catalog, refusing the first append it is asked to commit without applying it, as
/// a catalog outage does.
#[derive(Debug)]
struct RefusingFirstAppend {
    inner: MemoryCatalog,
    refused: AtomicBool,
}

#[async_trait::async_trait]
impl Catalog for RefusingFirstAppend {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    #[expect(
        clippy::disallowed_types,
        reason = "iceberg's Catalog trait fixes namespace properties to the standard library's map"
    )]
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        self.inner.namespace_exists(namespace).await
    }

    #[expect(
        clippy::disallowed_types,
        reason = "iceberg's Catalog trait fixes namespace properties to the standard library's map"
    )]
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> IcebergResult<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> IcebergResult<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> IcebergResult<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        if !self.refused.swap(true, Ordering::AcqRel) {
            return Err(IcebergError::new(
                IcebergErrorKind::Unexpected,
                "the fixture catalog refuses its first append",
            ));
        }
        self.inner.update_table(commit).await
    }
}

pub(super) async fn memory_catalog() -> MemoryCatalog {
    MemoryCatalogBuilder::default()
        .load(
            "memory",
            [(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                "memory://warehouse".to_string(),
            )]
            .into_iter()
            .collect(),
        )
        .await
        .assured("the in-memory catalog takes a warehouse location")
}

fn table_ident() -> TableIdent {
    TableIdent::new(
        NamespaceIdent::new(NAMESPACE.to_string()),
        TABLE.to_string(),
    )
}

/// The table the fixture emitter appends to, with the mapped columns `user_id` and `action`.
async fn create_table(catalog: &dyn Catalog) -> Table {
    let namespace = NamespaceIdent::new(NAMESPACE.to_string());
    catalog
        .create_namespace(&namespace, Default::default())
        .await
        .assured("the in-memory catalog creates the namespace");
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "user_id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "action", Type::Primitive(PrimitiveType::String)).into(),
        ])
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

fn mapped_schema() -> StdArc<arrow_schema::Schema> {
    StdArc::new(arrow_schema::Schema::new(vec![
        Field::new("user_id", DataType::Int64, true),
        Field::new("action", DataType::Utf8, true),
    ]))
}

/// An emitter over `catalog`'s fixture table that stages through `executor`.
async fn fixture_emitter(catalog: StdArc<dyn Catalog>, executor: Executor) -> IcebergTableEmitter {
    let table = create_table(catalog.as_ref()).await;
    IcebergTableEmitter {
        host: nervix_connector_fake::FakeEmitterHost::on(executor.clone()).host(),
        client: IcebergTableEmitterClient {
            catalog,
            table,
            file_name_prefix: "commit-memory".to_string(),
            data_file_sequence: 0,
        },
        commit_state: IcebergCommitState::default(),
        staged_schema: IcebergTableEmitter::staged_arrow_schema(&mapped_schema())
            .assured("the fixture's mapped columns are distinct"),
        commit_policy: IcebergCommitPolicy {
            interval: Duration::from_secs(60),
            max_size: 1 << 20,
        },
        batch_policy: None,
        staging_dir: tempfile::tempdir().assured("the staging directory is created"),
        executor,
        staging_sequence: 0,
        staging_files: BTreeMap::new(),
        appending_to: None,
        staged_batches: Vec::new(),
        staged_rows: 0,
        staged_bytes: 0,
    }
}

/// Stage the rows `user_ids` as one carrier, handing the emitter `acknowledgements` to resolve.
async fn stage(
    connector: &mut IcebergTableEmitter,
    user_ids: &[i64],
    acknowledgements: &StdArc<RecordedAcknowledgements>,
) {
    stage_carriers(connector, &[user_ids], acknowledgements).await;
}

/// The mapped rows of one carrier holding `user_ids`.
fn carrier_batch(user_ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        mapped_schema(),
        vec![
            StdArc::new(Int64Array::from(user_ids.to_vec())),
            StdArc::new(StringArray::from(vec!["staged"; user_ids.len()])),
        ],
    )
    .assured("the columns match the mapped schema")
}

/// Stage one write of a carrier for each of `carriers`, each holding its `user_ids` and handing
/// the emitter its own acknowledgements in `acknowledgements` to resolve.
async fn stage_carriers(
    connector: &mut IcebergTableEmitter,
    carriers: &[&[i64]],
    acknowledgements: &StdArc<RecordedAcknowledgements>,
) {
    let batches = carriers
        .iter()
        .map(|user_ids| carrier_batch(user_ids))
        .collect::<Vec<_>>();
    let selected_rows = carriers
        .iter()
        .map(|user_ids| (0..user_ids.len()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let mut mapped = Vec::with_capacity(carriers.len());
    for (batch_index, (batch, selected_rows)) in batches.iter().zip(&selected_rows).enumerate() {
        mapped.push(MappedEmitterCarrier {
            batch_index,
            batch,
            selected_rows,
            occurred_at: Timestamp::from_unix_nanos(1),
            acknowledgements: Some(EmitterRecordAcknowledgements::new(RecordedHandover(
                StdArc::clone(acknowledgements),
            ))),
        });
    }
    let target_columns = vec!["user_id".to_string(), "action".to_string()];
    let outcome = connector
        .publish(MappedEmitterRows {
            target_columns: &target_columns,
            carriers: mapped,
        })
        .await;
    assert!(
        !outcome.has_infrastructure_error(),
        "the emitter stages every row it is handed"
    );
}

/// How many files the emitter's staging directory holds.
fn staging_files(connector: &IcebergTableEmitter) -> usize {
    std::fs::read_dir(connector.staging_dir.path())
        .assured("the fixture's staging directory exists while its emitter does")
        .count()
}

/// Every `user_id` the fixture table holds, in order.
async fn committed_user_ids(catalog: &dyn Catalog) -> Vec<i64> {
    let table = catalog
        .load_table(&table_ident())
        .await
        .assured("the fixture table exists");
    let mut user_ids = Vec::new();
    // A table no commit appended to has no snapshot to scan.
    if table.metadata().current_snapshot().is_none() {
        return user_ids;
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
    for batch in batches {
        let column = batch
            .column_by_name("user_id")
            .assured("every row the emitter appends has a user_id column");
        let column = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .assured("user_id is written as a long");
        user_ids.extend(column.iter().flatten());
    }
    user_ids.sort_unstable();
    user_ids
}

/// The data files the fixture table's only snapshot appended.
async fn appended_data_files(catalog: &dyn Catalog) -> String {
    let table = catalog
        .load_table(&table_ident())
        .await
        .assured("the fixture table exists");
    let snapshots = table.metadata().snapshots().collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 1, "one commit appends one snapshot");
    let snapshot = snapshots
        .first()
        .assured("the assertion above found exactly one snapshot");
    snapshot
        .summary()
        .additional_properties
        .get("added-data-files")
        .assured("an append's summary counts the data files it added")
        .clone()
}

/// Hold every byte of `executor`'s bulk class but `room`.
fn hold_bulk_memory_leaving(executor: &Executor, room: u64) -> Reservation {
    let memory = executor.memory(MemoryClass::Bulk);
    let free = memory
        .capacity_bytes
        .checked_sub(memory.reserved_bytes)
        .assured("a budget never reserves more than its capacity");
    let held = free
        .checked_sub(room)
        .assured("the fixture leaves at most what is free");
    executor
        .try_reserve(MemoryClass::Bulk, held)
        .assured("the fixture takes memory that is free")
}

/// An executor whose bulk class holds `capacity` bytes.
fn executor_with_bulk_capacity(capacity: u64) -> Executor {
    let mut config = ExecutionConfig::default();
    config.budgets.bulk = ubyte::ByteUnit::from(capacity);
    // The bulk class must hold one sealed snapshot section beside one transfer chunk.
    config.limits.snapshot_section_bytes = ubyte::ByteUnit::from(64_u64);
    config.limits.snapshot_header_bytes = ubyte::ByteUnit::from(64_u64);
    config.limits.bulk_chunk_bytes = ubyte::ByteUnit::from(64_u64);
    Executor::new(config).assured("a bulk class above 256 bytes holds the fixture's limits")
}

/// The description a failure gives itself, which an emitter shows as its transient error.
pub(super) fn description<C>(report: &Report<C>) -> Option<String> {
    report.frames().find_map(|frame| match frame.kind() {
        FrameKind::Attachment(AttachmentKind::Printable(attachment)) => {
            Some(attachment.to_string())
        }
        FrameKind::Context(_) | FrameKind::Attachment(_) => None,
    })
}

#[nervix_primitives::test]
async fn a_commit_the_bulk_class_has_no_room_for_keeps_its_staged_rows_and_acknowledgements() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(memory_catalog().await);
    let executor = Executor::default();
    let mut connector = fixture_emitter(StdArc::clone(&catalog), executor.clone()).await;
    let acknowledgements = StdArc::new(RecordedAcknowledgements::default());
    stage(&mut connector, &[1, 2], &acknowledgements).await;
    let staged = connector.staged_bytes;
    let short_by_one = staged
        .checked_sub(1)
        .assured("a staged batch holds at least its message header");
    let held = hold_bulk_memory_leaving(&executor, short_by_one);

    let refused = connector
        .commit()
        .await
        .err()
        .assured("a class one byte short of the staged batch refuses the commit's charge");

    assert_eq!(
        refused.current_context(),
        &EmitterWriteError::Commit { connector: ICEBERG }
    );
    assert_eq!(
        refused.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::BudgetExhausted {
            class: "bulk",
            requested: staged,
        })
    );
    assert_eq!(
        description(&refused),
        Some(format!(
            "Iceberg commit waits for room in the bulk memory class for {staged} staged bytes"
        ))
    );
    assert_eq!(connector.staged_bytes, staged);
    assert_eq!(connector.staged_rows, 2);
    assert_eq!(
        staging_files(&connector),
        1,
        "a refused commit keeps its staging file"
    );
    assert_eq!(acknowledgements.acknowledged(), 0);
    assert_eq!(acknowledgements.rejected(), 0);
    assert_eq!(
        committed_user_ids(catalog.as_ref()).await,
        Vec::<i64>::new()
    );
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        held.bytes(),
        "a refused charge takes nothing"
    );

    // Exactly the staged bytes are free now, and the commit's charge takes every one of them.
    drop(held);
    let held = hold_bulk_memory_leaving(&executor, staged);
    let report = connector
        .commit()
        .await
        .assured("a class with room for exactly the staged bytes admits the commit")
        .assured("the commit publishes the staged rows");

    assert_eq!(report.messages, 2);
    assert_eq!(report.bytes, staged);
    assert_eq!(committed_user_ids(catalog.as_ref()).await, vec![1, 2]);
    assert_eq!(acknowledgements.acknowledged(), 1);
    assert_eq!(acknowledgements.rejected(), 0);
    assert!(connector.staged_batches.is_empty());
    assert_eq!(connector.staged_bytes, 0);
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        held.bytes(),
        "the commit returns its whole charge"
    );
}

#[nervix_primitives::test]
async fn a_staged_total_larger_than_the_bulk_class_commits_in_groups_within_one_snapshot() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(memory_catalog().await);
    let mut connector = fixture_emitter(StdArc::clone(&catalog), Executor::default()).await;
    let acknowledgements = StdArc::new(RecordedAcknowledgements::default());
    for user_id in [1, 2, 3] {
        stage(&mut connector, &[user_id], &acknowledgements).await;
    }
    let batch_bytes = connector
        .staged_batches
        .iter()
        .map(IcebergStagedBatch::bytes)
        .max()
        .assured("three batches are staged");
    // Two staged batches fit the class together and the third does not.
    let two_batches = batch_bytes
        .checked_mul(2)
        .assured("a staged batch of one row is small");
    let capacity = two_batches
        .checked_add(batch_bytes / 2)
        .assured("a staged batch of one row is small");
    let executor = executor_with_bulk_capacity(capacity);
    connector.executor = executor.clone();

    let report = connector
        .commit()
        .await
        .assured("a staged total larger than the class is committed")
        .assured("the commit publishes the staged rows");

    assert_eq!(report.messages, 3);
    assert_eq!(committed_user_ids(catalog.as_ref()).await, vec![1, 2, 3]);
    assert_eq!(
        appended_data_files(catalog.as_ref()).await,
        "2",
        "the two groups the class could load write their own data files into the one append"
    );
    assert_eq!(acknowledgements.acknowledged(), 3);
    assert!(connector.staged_batches.is_empty());
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

#[nervix_primitives::test]
async fn a_staged_batch_larger_than_the_whole_bulk_class_commits_once_the_whole_class_is_free() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(memory_catalog().await);
    let mut connector = fixture_emitter(StdArc::clone(&catalog), Executor::default()).await;
    let acknowledgements = StdArc::new(RecordedAcknowledgements::default());
    stage(&mut connector, &[1, 2, 3, 4, 5, 6, 7, 8], &acknowledgements).await;
    let staged = connector.staged_bytes;
    let capacity = staged
        .checked_sub(1)
        .assured("a staged batch holds at least its message header");
    let executor = executor_with_bulk_capacity(capacity);
    connector.executor = executor.clone();
    // Other bulk work holds one byte, so the class is short of the whole of itself.
    let all_but_one = capacity
        .checked_sub(1)
        .assured("the fixture's class holds more than one byte");
    let held = hold_bulk_memory_leaving(&executor, all_but_one);

    let refused = connector
        .commit()
        .await
        .err()
        .assured("a staged batch larger than the class waits until the whole class is free");

    assert_eq!(
        refused.current_context(),
        &EmitterWriteError::Commit { connector: ICEBERG }
    );
    assert_eq!(
        refused.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::BudgetExhausted {
            class: "bulk",
            requested: capacity,
        })
    );
    assert_eq!(
        description(&refused),
        Some(format!(
            "Iceberg commit waits for the whole bulk memory class of {capacity} bytes for a \
             staged batch of {staged} bytes"
        ))
    );
    assert_eq!(connector.staged_bytes, staged);
    assert_eq!(
        staging_files(&connector),
        1,
        "a refused commit keeps its staging file"
    );
    assert_eq!(acknowledgements.acknowledged(), 0);
    assert_eq!(acknowledgements.rejected(), 0);
    assert_eq!(
        committed_user_ids(catalog.as_ref()).await,
        Vec::<i64>::new()
    );
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        held.bytes(),
        "a refused charge takes nothing"
    );

    // The whole class is free now, and the batch is read charged with all of it.
    drop(held);
    let report = connector
        .commit()
        .await
        .assured("a free class admits a staged batch larger than itself")
        .assured("the commit publishes the staged rows");

    assert_eq!(report.messages, 8);
    assert_eq!(report.bytes, staged);
    assert_eq!(
        committed_user_ids(catalog.as_ref()).await,
        vec![1, 2, 3, 4, 5, 6, 7, 8]
    );
    assert_eq!(acknowledgements.acknowledged(), 1);
    assert_eq!(acknowledgements.rejected(), 0);
    assert!(connector.staged_batches.is_empty());
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        0,
        "the commit returns its whole charge"
    );
}

/// Declares in the fixture table the master key its files are encrypted under.
async fn declare_encryption_key(catalog: &dyn Catalog) {
    let table = catalog
        .load_table(&table_ident())
        .await
        .assured("the fixture table exists");
    let transaction = Transaction::new(&table);
    let declaration = transaction.update_table_properties().set(
        TableProperties::PROPERTY_ENCRYPTION_KEY_ID.to_string(),
        "fixture-master-key".to_string(),
    );
    declaration
        .apply(transaction)
        .assured("a property update applies to the fixture table")
        .commit(catalog)
        .await
        .assured("the in-memory catalog records the table property");
}

#[nervix_primitives::test]
async fn a_table_that_declares_an_encryption_key_refuses_the_commit_and_keeps_its_staged_rows() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(memory_catalog().await);
    let mut connector = fixture_emitter(StdArc::clone(&catalog), Executor::default()).await;
    let acknowledgements = StdArc::new(RecordedAcknowledgements::default());
    stage(&mut connector, &[1, 2], &acknowledgements).await;
    declare_encryption_key(catalog.as_ref()).await;

    let refused = connector
        .commit()
        .await
        .err()
        .assured("the emitter writes no encrypted files, so it refuses an encrypted table");

    assert_eq!(
        refused.current_context(),
        &EmitterWriteError::Commit { connector: ICEBERG }
    );
    assert_eq!(
        description(&refused),
        Some(
            "the Iceberg table declares an encryption key, and the emitter writes no encrypted \
             files"
                .to_string()
        )
    );
    assert_eq!(
        connector.client.data_file_sequence, 0,
        "a refused table receives no data file"
    );
    assert_eq!(
        committed_user_ids(catalog.as_ref()).await,
        Vec::<i64>::new()
    );
    assert_eq!(connector.staged_rows, 2);
    assert_eq!(
        staging_files(&connector),
        1,
        "a refused commit keeps its staging file"
    );
    assert_eq!(acknowledgements.acknowledged(), 0);
    assert_eq!(acknowledgements.rejected(), 0);
}

#[nervix_primitives::test]
async fn rows_staged_after_a_refused_append_are_written_before_they_are_acknowledged() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(RefusingFirstAppend {
        inner: memory_catalog().await,
        refused: AtomicBool::new(false),
    });
    let mut connector = fixture_emitter(StdArc::clone(&catalog), Executor::default()).await;
    let observations = nervix_connector_fake::FakeEmitterHost::on(connector.executor.clone());
    connector.host = observations.host();
    let first = StdArc::new(RecordedAcknowledgements::default());
    stage(&mut connector, &[1, 2], &first).await;
    let refused = connector
        .commit()
        .await
        .err()
        .assured("the fixture catalog refuses the first append");
    assert_eq!(
        refused.current_context(),
        &EmitterWriteError::Commit { connector: ICEBERG }
    );
    assert_eq!(
        committed_user_ids(catalog.as_ref()).await,
        Vec::<i64>::new()
    );

    // The host stages what its buffer gathered before it retries the commit.
    let measured = observations.observations();
    assert_eq!(measured.payloads.len(), 1);
    assert_eq!(measured.payloads[0].source_messages, 2);
    assert!(measured.payloads[0].encoded_bytes > 0);
    let second = StdArc::new(RecordedAcknowledgements::default());
    stage(&mut connector, &[3], &second).await;
    let report = connector
        .commit()
        .await
        .assured("the catalog accepts the retried append")
        .assured("the retry publishes the staged rows");

    assert_eq!(report.messages, 3);
    assert_eq!(committed_user_ids(catalog.as_ref()).await, vec![1, 2, 3]);
    assert_eq!(first.acknowledged(), 1);
    assert_eq!(second.acknowledged(), 1);
    assert_eq!(first.rejected() + second.rejected(), 0);
    assert!(connector.staged_batches.is_empty());
    assert_eq!(connector.staged_rows, 0);
    let retried = observations.observations();
    assert_eq!(retried.payloads.len(), 2);
    assert_eq!(retried.payloads[0], measured.payloads[0]);
    assert_eq!(retried.payloads[1].source_messages, 1);
}

#[nervix_primitives::test]
async fn every_batch_staged_before_a_commit_begins_shares_one_staging_file() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(memory_catalog().await);
    let mut connector = fixture_emitter(StdArc::clone(&catalog), Executor::default()).await;
    let acknowledgements = StdArc::new(RecordedAcknowledgements::default());

    // One write of three carriers, as a flush of three source batches hands it over, and a second
    // write, as the next flush does.
    stage_carriers(&mut connector, &[&[1], &[2, 3], &[4]], &acknowledgements).await;
    stage(&mut connector, &[5], &acknowledgements).await;

    assert_eq!(
        staging_files(&connector),
        1,
        "every batch staged before a commit begins is appended to one staging file"
    );
    assert_eq!(connector.staged_rows, 5);
    let report = connector
        .commit()
        .await
        .assured("the in-memory catalog accepts the append")
        .assured("the commit publishes the staged rows");
    assert_eq!(report.messages, 5);
    assert_eq!(
        committed_user_ids(catalog.as_ref()).await,
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(
        appended_data_files(catalog.as_ref()).await,
        "1",
        "the rows of every staged batch the class holds together share one data file"
    );
    assert_eq!(acknowledgements.acknowledged(), 4);
    assert_eq!(acknowledgements.rejected(), 0);
    assert_eq!(
        staging_files(&connector),
        0,
        "a commit removes the staging file it published"
    );

    // Rows staged after that commit start a staging file of their own.
    stage(&mut connector, &[6], &acknowledgements).await;
    assert_eq!(staging_files(&connector), 1);
}

#[nervix_primitives::test]
async fn a_commit_that_began_seals_its_staging_file_and_later_rows_start_another() {
    let catalog: StdArc<dyn Catalog> = StdArc::new(RefusingFirstAppend {
        inner: memory_catalog().await,
        refused: AtomicBool::new(false),
    });
    let mut connector = fixture_emitter(StdArc::clone(&catalog), Executor::default()).await;
    let first = StdArc::new(RecordedAcknowledgements::default());
    stage_carriers(&mut connector, &[&[1], &[2]], &first).await;
    let refused = connector
        .commit()
        .await
        .err()
        .assured("the fixture catalog refuses the first append");
    assert_eq!(
        refused.current_context(),
        &EmitterWriteError::Commit { connector: ICEBERG }
    );

    // The refused commit covers the file it began with, so what the host stages before it retries
    // is appended to a new file that a commit of its own publishes.
    let second = StdArc::new(RecordedAcknowledgements::default());
    stage_carriers(&mut connector, &[&[3], &[4]], &second).await;
    assert_eq!(staging_files(&connector), 2);
    let report = connector
        .commit()
        .await
        .assured("the catalog accepts the retried append")
        .assured("the retry publishes the staged rows");

    assert_eq!(report.messages, 4);
    assert_eq!(committed_user_ids(catalog.as_ref()).await, vec![1, 2, 3, 4]);
    assert_eq!(first.acknowledged(), 2);
    assert_eq!(second.acknowledged(), 2);
    assert_eq!(staging_files(&connector), 0);
}

#[test]
fn a_load_group_reads_one_span_from_each_staging_file_it_takes_batches_from() {
    let staged = |file: u64, range: Range<u64>| IcebergStagedBatch {
        file,
        range,
        rows: 1,
        acknowledgements: None,
        domain_timestamp: Timestamp::from_unix_nanos(1),
    };
    let staging_file = |path: &str, end: u64, batches: usize| IcebergStagingFile {
        path: PathBuf::from(path),
        schema_end: 64,
        end,
        batches,
    };
    let files = BTreeMap::from([
        (1, staging_file("staging-1.arrow", 300, 2)),
        (2, staging_file("staging-2.arrow", 164, 1)),
    ]);
    let group = [staged(1, 64..200), staged(1, 200..300), staged(2, 64..164)];

    assert_eq!(
        IcebergTableEmitter::staged_spans(&files, &group),
        vec![
            staging::Span {
                path: PathBuf::from("staging-1.arrow"),
                schema_end: 64,
                bytes: 64..300,
                batches: 2,
            },
            staging::Span {
                path: PathBuf::from("staging-2.arrow"),
                schema_end: 64,
                bytes: 64..164,
                batches: 1,
            },
        ]
    );
    assert_eq!(
        IcebergTableEmitter::staged_spans(&files, &group[1..2]),
        vec![staging::Span {
            path: PathBuf::from("staging-1.arrow"),
            schema_end: 64,
            bytes: 200..300,
            batches: 1,
        }]
    );
}
