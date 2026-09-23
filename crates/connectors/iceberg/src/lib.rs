//! Iceberg sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The REST catalog and object-store clients one Iceberg table is loaded and committed
//!   through, the local Arrow IPC staging of every mapped batch, the `COMMIT EACH` cadence and
//!   maximum commit size that release the staged files, the Parquet data files one commit writes,
//!   and the acknowledgements it retains until that commit succeeds.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio
//!   and the `iceberg` crates.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.
//!
//! Local staging is not an acknowledgement boundary. The sink completion point for `MODE ACK` is
//! the successful catalog commit, and an appended row is never idempotent, as
//! [Emitters](docs/src/emitters.md) documents.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use std::{fs::File, path::PathBuf, sync::Arc as StdArc, time::Duration};

use ::iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, Result as IcebergResult, TableIdent,
    arrow::FieldMatchMode,
    io::{
        ADLS_ACCOUNT_KEY, ADLS_ACCOUNT_NAME, ADLS_AUTHORITY_HOST, ADLS_CLIENT_ID,
        ADLS_CLIENT_SECRET, ADLS_CONNECTION_STRING, ADLS_SAS_TOKEN, ADLS_TENANT_ID, CLIENT_REGION,
        GCS_ALLOW_ANONYMOUS, GCS_CREDENTIALS_JSON, GCS_DISABLE_CONFIG_LOAD,
        GCS_DISABLE_VM_METADATA, GCS_NO_AUTH, GCS_SERVICE_PATH, GCS_TOKEN, S3_ACCESS_KEY_ID,
        S3_ALLOW_ANONYMOUS, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_ENDPOINT,
        S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
    },
    spec::{DataFile, DataFileFormat, Snapshot, TableProperties},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};
use ahash::{HashMap, HashSet};
use arch_into::ArchInto as _;
use arrow_array::{
    Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions, TimestampMicrosecondArray,
    TimestampNanosecondArray,
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, TimeUnit};
use arrow_select::{concat::concat as concat_arrow_arrays, filter::filter as filter_arrow_array};
use error_stack::{Report, ResultExt as _};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalStorageFactory;
use meticulous::OptionExt as _;
use nervix_connector::{
    MappedSinkRows, PerRecordOutcome, RowSink, SinkAcknowledgementServices, SinkAcknowledgements,
    SinkCommitReport, SinkDeadline, SinkHost, SinkLifecycle, SinkPublishError, SinkPublishResult,
    SinkRecordPosition, SinkStartError, SinkStartResult, physical_time::actual_utc_now,
};
use nervix_models::{ClientConfigEntry, IcebergStorageBackend, TableName, Timestamp};
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;
use tracing::{debug, trace};
use url::Url;

const ICEBERG: &str = "iceberg";

/// The snapshot property that names the append one prepared commit stands for, so a retry after an
/// ambiguous catalog result recognizes its own work instead of appending it twice.
const ICEBERG_APPEND_ID_PROPERTY: &str = "nervix.emitter.append-id";

/// When staged data is published: the domain duration between commits, and the staged size that
/// commits before that duration elapses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IcebergCommitPolicy {
    pub interval: Duration,
    pub max_size: u64,
}

/// What one Iceberg emitter stages and commits through, from its typed sink plan.
pub struct IcebergSinkConfig {
    pub backend: IcebergStorageBackend,
    pub storage_config: Vec<ClientConfigEntry>,
    pub catalog_name: String,
    pub catalog_config: Vec<ClientConfigEntry>,
    /// The catalog namespace this emitter's table lives in.
    pub namespace: String,
    pub table: TableName,
    pub location: String,
    /// The mapped columns every staged batch carries, as the host projects them.
    pub mapped_schema: StdArc<arrow_schema::Schema>,
    pub commit: IcebergCommitPolicy,
    /// How generated staging and data files name the writer that produced them.
    pub writer: String,
}

/// The Iceberg sink, which stages each mapped batch as a local Arrow IPC file and appends every
/// staged file to its table in one catalog commit.
pub struct IcebergSink {
    client: IcebergSinkClient,
    commit_state: IcebergCommitState,
    /// The exact Arrow schema every staged file is written and read back with, which is the host's
    /// mapped schema with datetime columns narrowed to the microsecond resolution Iceberg stores.
    staged_schema: StdArc<arrow_schema::Schema>,
    commit_policy: IcebergCommitPolicy,
    staging_dir: TempDir,
    staged_sequence: u64,
    staged_batches: Vec<IcebergStagedBatch>,
    staged_rows: u64,
    staged_bytes: u64,
    commit_deadline: IcebergCommitDeadline,
}

/// The domain time by which the staged batches must be published.
///
/// The deadline is armed when the first batch is staged and left alone afterwards, so a later
/// batch joining the same staged set neither moves the commit nor waits for a second cadence.
/// Reaching the declared maximum commit size brings it to the staging time itself, which is when
/// the commit became due.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct IcebergCommitDeadline {
    due_at: Option<Timestamp>,
}

impl IcebergCommitDeadline {
    fn arm(&mut self, policy: IcebergCommitPolicy, staged_at: Timestamp, staged_bytes: u64) {
        if self.due_at.is_none() {
            // Saturation is the meaning here: a commit deadline past the nanosecond range is past
            // any domain time this emitter will reach.
            let due_at = staged_at
                .checked_add(policy.interval)
                .unwrap_or_else(|_| Timestamp::from_unix_nanos(i64::MAX));
            self.due_at = Some(due_at);
        }
        if staged_bytes >= policy.max_size {
            self.due_at = Some(staged_at);
        }
    }

    fn due_at(self) -> Option<Timestamp> {
        self.due_at
    }

    fn clear(&mut self) {
        self.due_at = None;
    }
}

/// One mapped batch written to local Arrow IPC, with the acknowledgements its commit resolves.
struct IcebergStagedBatch {
    path: PathBuf,
    rows: u64,
    bytes: u64,
    acknowledgements: Option<SinkAcknowledgements>,
    domain_timestamp: Timestamp,
}

/// Every acknowledgement this sink retained, as one handle the host keeps alive while it waits.
struct RetainedAcknowledgements(Vec<SinkAcknowledgements>);

impl SinkAcknowledgementServices for RetainedAcknowledgements {
    fn acknowledge(&self) {
        for acknowledgements in &self.0 {
            acknowledgements.acknowledge();
        }
    }

    fn keep_alive(&self) {
        for acknowledgements in &self.0 {
            acknowledgements.keep_alive();
        }
    }

    fn reject(&self, reason: String) {
        for acknowledgements in &self.0 {
            acknowledgements.reject(reason.clone());
        }
    }

    fn is_empty(&self) -> bool {
        self.0.iter().all(SinkAcknowledgements::is_empty)
    }
}

/// The catalog and table one emitter commits through, and the sequence its data files are named by.
struct IcebergSinkClient {
    catalog: StdArc<RestCatalog>,
    table: Table,
    file_name_prefix: String,
    data_file_sequence: u64,
}

/// The Parquet data files one commit appends, and the append identity that recognizes them again.
struct IcebergPreparedCommit {
    append_id: uuid::Uuid,
    data_files: Vec<DataFile>,
}

impl IcebergPreparedCommit {
    fn new(data_files: Vec<DataFile>) -> Self {
        Self::with_append_id(data_files, uuid::Uuid::now_v7())
    }

    fn with_append_id(data_files: Vec<DataFile>, append_id: uuid::Uuid) -> Self {
        Self {
            append_id,
            data_files,
        }
    }

    fn append_id(&self) -> uuid::Uuid {
        self.append_id
    }

    fn data_files(&self) -> &[DataFile] {
        self.data_files.as_slice()
    }

    fn snapshot_properties(&self) -> impl Iterator<Item = (String, String)> {
        [(
            ICEBERG_APPEND_ID_PROPERTY.to_string(),
            self.append_id.to_string(),
        )]
        .into_iter()
    }

    fn matches_snapshot(&self, snapshot: &Snapshot) -> bool {
        snapshot
            .summary()
            .additional_properties
            .get(ICEBERG_APPEND_ID_PROPERTY)
            .and_then(|append_id| uuid::Uuid::parse_str(append_id).ok())
            .is_some_and(|append_id| append_id == self.append_id)
    }

    fn is_committed_to(&self, table: &Table) -> bool {
        let metadata = table.metadata();
        let mut snapshot = metadata
            .current_snapshot()
            .map(|snapshot| snapshot.as_ref());
        while let Some(current) = snapshot {
            if self.matches_snapshot(current) {
                return true;
            }
            snapshot = match current.parent_snapshot_id() {
                Some(parent) => metadata
                    .snapshot_by_id(parent)
                    .map(|parent| parent.as_ref()),
                None => None,
            };
        }
        false
    }
}

/// The prepared commit a failed catalog attempt keeps, so a retry appends the same data files
/// rather than writing them again.
#[derive(Default)]
struct IcebergCommitState {
    prepared: Option<IcebergPreparedCommit>,
}

impl IcebergCommitState {
    fn store(&mut self, prepared: IcebergPreparedCommit) {
        debug_assert!(self.prepared.is_none());
        self.prepared = Some(prepared);
    }

    fn prepared(&self) -> Option<&IcebergPreparedCommit> {
        self.prepared.as_ref()
    }

    fn finish(&mut self) {
        self.prepared = None;
    }
}

/// The object-store properties one backend's client configuration is translated into.
#[derive(Debug, Clone)]
struct IcebergObjectStoreProperties {
    backend: IcebergStorageBackend,
    props: HashMap<String, String>,
}

trait IcebergStorageBackendExt {
    fn accepts_location_scheme(self, scheme: &str) -> bool;
    fn storage_factory(self) -> StdArc<dyn ::iceberg::io::StorageFactory>;
}

impl IcebergStorageBackendExt for IcebergStorageBackend {
    fn accepts_location_scheme(self, scheme: &str) -> bool {
        match self {
            Self::S3 => scheme == "s3",
            Self::Gcs => scheme == "gs",
            Self::AzureBlob => scheme == "wasb" || scheme == "wasbs",
        }
    }

    fn storage_factory(self) -> StdArc<dyn ::iceberg::io::StorageFactory> {
        match self {
            Self::S3 => StdArc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }),
            Self::Gcs => StdArc::new(OpenDalStorageFactory::Gcs),
            Self::AzureBlob => StdArc::new(OpenDalStorageFactory::Azdls),
        }
    }
}

impl IcebergSink {
    pub async fn new(config: IcebergSinkConfig, host: SinkHost) -> SinkStartResult<Self> {
        let IcebergSinkConfig {
            backend,
            storage_config,
            catalog_name,
            catalog_config,
            namespace,
            table,
            location,
            mapped_schema,
            commit,
            writer,
        } = config;
        let staged_schema = Self::staged_arrow_schema(&mapped_schema)?;
        let staging_dir = Self::create_staging_dir(&host)?;
        Self::validate_blob_location(backend, "table", &location)?;
        let properties = IcebergObjectStoreProperties::from_entries(backend, &storage_config);
        let catalog = StdArc::new(
            properties
                .rest_catalog(&catalog_name, &catalog_config)
                .await
                .map_err(|error| {
                    Report::new(SinkStartError::Initialize { sink: ICEBERG }).attach_printable(
                        format!("failed to initialize Iceberg catalog '{catalog_name}': {error}"),
                    )
                })?,
        );
        let table_ident =
            TableIdent::new(NamespaceIdent::new(namespace), table.as_str().to_string());
        let loaded = catalog.load_table(&table_ident).await.map_err(|error| {
            Report::new(SinkStartError::Initialize { sink: ICEBERG }).attach_printable(format!(
                "failed to initialize Iceberg table {table_ident}: {error}"
            ))
        })?;
        if loaded.metadata().location() != location {
            return Err(
                Report::new(SinkStartError::InvalidConfiguration { sink: ICEBERG })
                    .attach_printable(format!(
                        "table '{table_ident}' is registered at '{}' but emitter location is \
                         '{location}'",
                        loaded.metadata().location()
                    )),
            );
        }
        Ok(Self {
            client: IcebergSinkClient {
                catalog,
                table: loaded,
                file_name_prefix: format!(
                    "{writer}-{}-{}-{}",
                    table.as_str(),
                    actual_utc_now().unix_nanos(),
                    fastrand::u64(..)
                ),
                data_file_sequence: 0,
            },
            commit_state: IcebergCommitState::default(),
            staged_schema,
            commit_policy: commit,
            staging_dir,
            staged_sequence: 0,
            staged_batches: Vec::new(),
            staged_rows: 0,
            staged_bytes: 0,
            commit_deadline: IcebergCommitDeadline::default(),
        })
    }

    /// The Arrow schema staged files carry, which narrows every UTC datetime column to the
    /// microsecond resolution an Iceberg `timestamptz` stores.
    fn staged_arrow_schema(
        mapped_schema: &arrow_schema::Schema,
    ) -> SinkStartResult<StdArc<arrow_schema::Schema>> {
        let mut seen = HashSet::default();
        let mut fields = Vec::with_capacity(mapped_schema.fields().len());
        for field in mapped_schema.fields() {
            if !seen.insert(field.name().clone()) {
                return Err(
                    Report::new(SinkStartError::InvalidConfiguration { sink: ICEBERG })
                        .attach_printable(format!("duplicate mapped column: {}", field.name())),
                );
            }
            fields.push(arrow_schema::Field::new(
                field.name(),
                Self::staged_arrow_data_type(field.data_type()),
                true,
            ));
        }
        Ok(StdArc::new(arrow_schema::Schema::new(fields)))
    }

    fn staged_arrow_data_type(data_type: &DataType) -> DataType {
        if let DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)) = data_type
            && (timezone.as_ref() == "+00:00" || timezone.as_ref() == "UTC")
        {
            return DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()));
        }
        data_type.clone()
    }

    fn create_staging_dir(host: &SinkHost) -> SinkStartResult<TempDir> {
        let root = host.staging_directory();
        std::fs::create_dir_all(&root).map_err(|error| {
            Report::new(SinkStartError::Initialize { sink: ICEBERG }).attach_printable(format!(
                "failed to create Iceberg staging directory under '{}': {error}",
                root.display()
            ))
        })?;
        TempDir::new_in(&root).map_err(|error| {
            Report::new(SinkStartError::Initialize { sink: ICEBERG }).attach_printable(format!(
                "failed to create Iceberg staging directory under '{}': {error}",
                root.display()
            ))
        })
    }

    fn validate_blob_location(
        backend: IcebergStorageBackend,
        label: &str,
        location: &str,
    ) -> SinkStartResult<()> {
        let invalid = |reason: String| {
            Report::new(SinkStartError::InvalidConfiguration { sink: ICEBERG })
                .attach_printable(reason)
        };
        let url = Url::parse(location)
            .map_err(|error| invalid(format!("{label} location '{location}': {error}")))?;
        if !backend.accepts_location_scheme(url.scheme()) {
            let expected = match backend {
                IcebergStorageBackend::S3 => "s3://",
                IcebergStorageBackend::Gcs => "gs://",
                IcebergStorageBackend::AzureBlob => "wasb:// or wasbs://",
            };
            return Err(invalid(format!(
                "{label} location '{location}' must use {expected}"
            )));
        }
        if url.host_str().is_none() {
            return Err(invalid(format!(
                "{label} location '{location}' must include a {} bucket",
                backend.as_ref()
            )));
        }
        if let IcebergStorageBackend::AzureBlob = backend {
            if url.username().is_empty() {
                return Err(invalid(format!(
                    "{label} location '{location}' must include an Azure container before @"
                )));
            }
            let host = url.host_str().unwrap_or_default();
            if !host.contains(".blob.") {
                return Err(invalid(format!(
                    "{label} location '{location}' must use an Azure Blob host"
                )));
            }
        }
        Ok(())
    }

    /// The staged columns of one write: the rows the host selected, in this sink's exact staged
    /// types.
    fn staged_batch(&self, rows: &MappedSinkRows<'_>) -> SinkPublishResult<RecordBatch> {
        let row_count = rows.batch.num_rows();
        let selects_every_row = rows.selected_rows.len() == row_count;
        let mut selected = vec![false; row_count];
        if !selects_every_row {
            for row in rows.selected_rows {
                let Some(selected) = selected.get_mut(*row) else {
                    return Err(Report::new(SinkPublishError::Publish { sink: ICEBERG })
                        .attach_printable(format!(
                            "Iceberg selected row {row} outside {row_count} mapped rows"
                        )));
                };
                *selected = true;
            }
        }
        let predicate = BooleanArray::from(selected);
        let mut columns = Vec::with_capacity(self.staged_schema.fields().len());
        for (index, field) in self.staged_schema.fields().iter().enumerate() {
            let Some(column) = rows.batch.columns().get(index) else {
                return Err(Report::new(SinkPublishError::Publish { sink: ICEBERG })
                    .attach_printable(format!(
                        "Iceberg mapped batch has {} columns for {} staged columns",
                        rows.batch.num_columns(),
                        self.staged_schema.fields().len()
                    )));
            };
            let column = Self::staged_column(column, field.data_type())?;
            columns.push(if selects_every_row {
                column
            } else {
                filter_arrow_array(column.as_ref(), &predicate)
                    .change_context(SinkPublishError::Publish { sink: ICEBERG })?
            });
        }
        if columns.is_empty() {
            return RecordBatch::try_new_with_options(
                self.staged_schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(rows.selected_rows.len())),
            )
            .change_context(SinkPublishError::Publish { sink: ICEBERG });
        }
        RecordBatch::try_new(self.staged_schema.clone(), columns)
            .change_context(SinkPublishError::Publish { sink: ICEBERG })
    }

    /// One mapped column in the exact type its staged file carries.
    fn staged_column(column: &ArrayRef, data_type: &DataType) -> SinkPublishResult<ArrayRef> {
        let DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) = data_type else {
            return Ok(column.clone());
        };
        let Some(values) = column.as_any().downcast_ref::<TimestampNanosecondArray>() else {
            return Err(
                Report::new(SinkPublishError::Publish { sink: ICEBERG }).attach_printable(format!(
                    "Iceberg datetime column arrived as {} instead of a nanosecond timestamp",
                    column.data_type()
                )),
            );
        };
        let microseconds = values
            .iter()
            .map(|value| value.map(|nanos| nanos.div_euclid(1_000)))
            .collect::<TimestampMicrosecondArray>()
            .with_timezone(timezone.clone());
        Ok(StdArc::new(microseconds))
    }

    fn next_staged_path(&mut self) -> PathBuf {
        self.staged_sequence = self
            .staged_sequence
            .checked_add(1)
            .assured("an emitter cannot stage 2^64 batches in the lifetime of a node");
        self.staging_dir
            .path()
            .join(format!("batch-{}.arrow", self.staged_sequence))
    }

    async fn write_ipc_batch(path: PathBuf, batch: RecordBatch) -> SinkPublishResult<u64> {
        let staged = |error: &dyn std::fmt::Display, path: &PathBuf| {
            Report::new(SinkPublishError::Publish { sink: ICEBERG }).attach_printable(format!(
                "failed to write Iceberg staged Arrow IPC '{}': {error}",
                path.display()
            ))
        };
        tokio::task::spawn_blocking(move || {
            let file = File::create(&path).map_err(|error| staged(&error, &path))?;
            let mut writer = StreamWriter::try_new(file, batch.schema().as_ref())
                .map_err(|error| staged(&error, &path))?;
            writer
                .write(&batch)
                .map_err(|error| staged(&error, &path))?;
            writer.finish().map_err(|error| staged(&error, &path))?;
            std::fs::metadata(&path)
                .map(|metadata| metadata.len())
                .map_err(|error| staged(&error, &path))
        })
        .await
        .map_err(|error| {
            Report::new(SinkPublishError::Publish { sink: ICEBERG })
                .attach_printable(format!("Iceberg staging task failed: {error}"))
        })?
    }

    async fn read_ipc_batches(
        schema: StdArc<arrow_schema::Schema>,
        paths: &[PathBuf],
    ) -> SinkPublishResult<RecordBatch> {
        let paths = paths.to_vec();
        let staged = |error: String, path: &PathBuf| {
            Report::new(SinkPublishError::Commit { sink: ICEBERG }).attach_printable(format!(
                "failed to read Iceberg staged Arrow IPC '{}': {error}",
                path.display()
            ))
        };
        tokio::task::spawn_blocking(move || {
            let mut batches = Vec::new();
            for path in paths {
                let file = File::open(&path).map_err(|error| staged(error.to_string(), &path))?;
                let reader = StreamReader::try_new(file, None)
                    .map_err(|error| staged(error.to_string(), &path))?;
                if reader.schema().as_ref() != schema.as_ref() {
                    return Err(staged("schema does not match".to_string(), &path));
                }
                let path_batches = reader
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| staged(error.to_string(), &path))?;
                batches.extend(path_batches);
            }
            Self::concat_arrow_batches(schema, batches)
        })
        .await
        .map_err(|error| {
            Report::new(SinkPublishError::Commit { sink: ICEBERG })
                .attach_printable(format!("Iceberg staged read task failed: {error}"))
        })?
    }

    fn concat_arrow_batches(
        schema: StdArc<arrow_schema::Schema>,
        batches: Vec<RecordBatch>,
    ) -> SinkPublishResult<RecordBatch> {
        let commit_failure = |reason: &str| {
            Report::new(SinkPublishError::Commit { sink: ICEBERG })
                .attach_printable(format!("failed to read Iceberg staged Arrow IPC: {reason}"))
        };
        let Some(first) = batches.first() else {
            return Err(commit_failure("cannot commit zero staged Iceberg batches"));
        };
        if first.schema().as_ref() != schema.as_ref()
            || batches
                .iter()
                .any(|batch| batch.schema().as_ref() != schema.as_ref())
        {
            return Err(commit_failure("staged Iceberg batch schemas do not match"));
        }
        if batches.len() == 1 {
            return Ok(first.clone());
        }
        let columns = if schema.fields().is_empty() {
            Vec::new()
        } else {
            let mut columns = Vec::with_capacity(schema.fields().len());
            for column_index in 0..schema.fields().len() {
                let arrays = batches
                    .iter()
                    .map(|batch| batch.column(column_index).as_ref())
                    .collect::<Vec<_>>();
                columns.push(
                    concat_arrow_arrays(&arrays)
                        .change_context(SinkPublishError::Commit { sink: ICEBERG })?,
                );
            }
            columns
        };
        let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        if columns.is_empty() {
            RecordBatch::try_new_with_options(
                schema,
                columns,
                &RecordBatchOptions::new().with_row_count(Some(row_count)),
            )
        } else {
            RecordBatch::try_new(schema, columns)
        }
        .change_context(SinkPublishError::Commit { sink: ICEBERG })
    }

    /// Every acknowledgement this sink still holds, newest staged batch last.
    fn retained_acknowledgements(&self) -> Vec<SinkAcknowledgements> {
        self.staged_batches
            .iter()
            .filter_map(|batch| batch.acknowledgements.clone())
            .collect()
    }
}

impl Drop for IcebergSink {
    fn drop(&mut self) {
        for acknowledgements in self.retained_acknowledgements() {
            acknowledgements.reject("Iceberg emitter dropped staged batch".to_string());
        }
    }
}

#[async_trait::async_trait]
impl SinkLifecycle for IcebergSink {
    /// A staged batch outlives its catalog client, so a publish failure keeps the client rather
    /// than reopening it and losing what is already on disk.
    fn keeps_client_on_publish_failure(&self) -> bool {
        true
    }

    fn commit_deadline(&self) -> Option<SinkDeadline> {
        self.commit_deadline.due_at().map(SinkDeadline::Domain)
    }

    fn pending_acks(&self) -> Option<SinkAcknowledgements> {
        let retained = self.retained_acknowledgements();
        if retained.is_empty() {
            return None;
        }
        Some(SinkAcknowledgements::new(RetainedAcknowledgements(
            retained,
        )))
    }

    fn retains_acknowledgements(&self) -> bool {
        true
    }

    fn staged_messages(&self) -> u64 {
        self.staged_rows
    }

    async fn commit(&mut self) -> SinkPublishResult<Option<SinkCommitReport>> {
        if self.staged_batches.is_empty() {
            self.commit_deadline.clear();
            return Ok(None);
        }
        if self.commit_state.prepared().is_none() {
            let paths = self
                .staged_batches
                .iter()
                .map(|batch| batch.path.clone())
                .collect::<Vec<_>>();
            let batch = Self::read_ipc_batches(self.staged_schema.clone(), &paths).await?;
            let prepared = self.client.prepare_batch(batch).await?;
            self.commit_state.store(prepared);
        }
        self.client
            .commit_prepared(self.commit_state.prepared().verified(
                "the commit state holds its prepared commit from preparation until this call \
                 completes",
            ))
            .await?;
        self.commit_state.finish();
        let staged = std::mem::take(&mut self.staged_batches);
        let messages = staged
            .iter()
            .map(|batch| batch.rows)
            .try_fold(0_u64, u64::checked_add)
            .assured("the total counts rows this sink already staged on disk");
        let bytes = staged
            .iter()
            .map(|batch| batch.bytes)
            .try_fold(0_u64, u64::checked_add)
            .assured("the total counts bytes this sink already staged on disk");
        let domain_timestamp = staged
            .iter()
            .map(|batch| batch.domain_timestamp)
            .max()
            .verified("this call returns before this point when no batch is staged");
        let mut acknowledgements = Vec::with_capacity(staged.len());
        for batch in staged {
            acknowledgements.extend(batch.acknowledgements);
            if let Err(error) = tokio::fs::remove_file(&batch.path).await {
                debug!(
                    path = %batch.path.display(),
                    error = %error,
                    "failed to remove committed Iceberg staged batch"
                );
            }
        }
        self.staged_rows = 0;
        self.staged_bytes = 0;
        self.commit_deadline.clear();
        for acknowledgements in acknowledgements {
            acknowledgements.acknowledge();
        }
        Ok(Some(SinkCommitReport {
            messages,
            bytes,
            domain_timestamp,
        }))
    }
}

#[async_trait::async_trait]
impl RowSink for IcebergSink {
    async fn publish(&mut self, rows: MappedSinkRows<'_>) -> PerRecordOutcome {
        let mut outcome = PerRecordOutcome::with_capacity(rows.selected_rows.len());
        let staged = match self.staged_batch(&rows) {
            Ok(staged) => staged,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        let staged_rows: u64 = staged.num_rows().arch_into();
        let path = self.next_staged_path();
        let staged_bytes = match Self::write_ipc_batch(path.clone(), staged).await {
            Ok(bytes) => bytes,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        self.staged_batches.push(IcebergStagedBatch {
            path,
            rows: staged_rows,
            bytes: staged_bytes,
            acknowledgements: rows.acknowledgements,
            domain_timestamp: rows.occurred_at,
        });
        self.staged_rows = self
            .staged_rows
            .checked_add(staged_rows)
            .assured("both counts total rows this sink already staged on disk");
        self.staged_bytes = self
            .staged_bytes
            .checked_add(staged_bytes)
            .assured("both counts total bytes this sink already staged on disk");
        self.commit_deadline
            .arm(self.commit_policy, rows.occurred_at, self.staged_bytes);
        for row in rows.selected_rows {
            outcome.deliver(SinkRecordPosition {
                batch_index: rows.batch_index,
                row_index: *row,
            });
        }
        trace!(
            rows = staged_rows,
            bytes = staged_bytes,
            "emitter staged iceberg rows"
        );
        outcome
    }
}

impl IcebergSinkClient {
    /// The table this commit attempts against, with the catalog library's own retry disabled so
    /// the emitter's declared retry policy is the only one that runs.
    fn single_attempt_table(table: &Table) -> SinkPublishResult<Table> {
        let commit_failure = |error: &dyn std::fmt::Display| {
            Report::new(SinkPublishError::Commit { sink: ICEBERG })
                .attach_printable(format!("failed to commit Iceberg staged batches: {error}"))
        };
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(
                [(
                    TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
                    "0".to_string(),
                )]
                .into_iter()
                .collect(),
            )
            .map_err(|error| commit_failure(&error))?
            .build()
            .map_err(|error| commit_failure(&error))?
            .metadata;
        let mut builder = Table::builder()
            .file_io(table.file_io().clone())
            .metadata(metadata)
            .identifier(table.identifier().clone())
            .runtime(::iceberg::Runtime::try_current().map_err(|error| commit_failure(&error))?);
        if let Some(metadata_location) = table.metadata_location() {
            builder = builder.metadata_location(metadata_location);
        }
        builder.build().map_err(|error| commit_failure(&error))
    }

    async fn refresh_table(&mut self) -> SinkPublishResult<()> {
        let table_ident = self.table.identifier().clone();
        self.table = self
            .catalog
            .load_table(&table_ident)
            .await
            .map_err(|error| {
                Report::new(SinkPublishError::Commit { sink: ICEBERG }).attach_printable(format!(
                    "failed to load Iceberg table {table_ident} for commit: {error}"
                ))
            })?;
        Ok(())
    }

    async fn prepare_batch(
        &mut self,
        batch: RecordBatch,
    ) -> SinkPublishResult<IcebergPreparedCommit> {
        let commit_failure = |error: &dyn std::fmt::Display| {
            Report::new(SinkPublishError::Commit { sink: ICEBERG })
                .attach_printable(format!("failed to commit Iceberg staged batches: {error}"))
        };
        self.refresh_table().await?;
        let location_generator = DefaultLocationGenerator::new(self.table.metadata())
            .map_err(|error| commit_failure(&error))?;
        self.data_file_sequence = self
            .data_file_sequence
            .checked_add(1)
            .assured("an emitter cannot commit 2^64 data files in the lifetime of a node");
        let file_name_generator = DefaultFileNameGenerator::new(
            format!("{}-{}", self.file_name_prefix, self.data_file_sequence),
            None,
            DataFileFormat::Parquet,
        );
        let parquet_writer = ParquetWriterBuilder::new_with_match_mode(
            WriterProperties::builder().build(),
            self.table.metadata().current_schema().clone(),
            FieldMatchMode::Name,
        );
        let rolling_writer = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer,
            self.table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let mut writer = DataFileWriterBuilder::new(rolling_writer)
            .build(None)
            .await
            .map_err(|error| commit_failure(&error))?;
        writer
            .write(batch)
            .await
            .map_err(|error| commit_failure(&error))?;
        let data_files = writer
            .close()
            .await
            .map_err(|error| commit_failure(&error))?;
        Ok(IcebergPreparedCommit::new(data_files))
    }

    async fn commit_prepared(&mut self, prepared: &IcebergPreparedCommit) -> SinkPublishResult<()> {
        let commit_failure = |error: &dyn std::fmt::Display| {
            Report::new(SinkPublishError::Commit { sink: ICEBERG })
                .attach_printable(format!("failed to commit Iceberg staged batches: {error}"))
        };
        self.refresh_table().await?;
        if prepared.is_committed_to(&self.table) {
            return Ok(());
        }
        let single_attempt_table = Self::single_attempt_table(&self.table)?;
        let tx = Transaction::new(&single_attempt_table);
        let action = tx
            .fast_append()
            .set_commit_uuid(prepared.append_id())
            .set_snapshot_properties(prepared.snapshot_properties().collect())
            .add_data_files(prepared.data_files().iter().cloned());
        let tx = action.apply(tx).map_err(|error| commit_failure(&error))?;
        match tx.commit(self.catalog.as_ref()).await {
            Ok(table) => {
                self.table = table;
                Ok(())
            }
            Err(error) => {
                let commit_error = commit_failure(&error);
                let table_ident = self.table.identifier().clone();
                if let Ok(refreshed) = self.catalog.load_table(&table_ident).await {
                    let committed = prepared.is_committed_to(&refreshed);
                    self.table = refreshed;
                    if committed {
                        return Ok(());
                    }
                }
                Err(commit_error)
            }
        }
    }
}

impl IcebergObjectStoreProperties {
    fn from_entries(backend: IcebergStorageBackend, config: &[ClientConfigEntry]) -> Self {
        let mut props = HashMap::default();
        for entry in config {
            props.insert(
                Self::property_key(backend, &entry.key).to_string(),
                entry.value.clone(),
            );
        }
        Self { backend, props }
    }

    fn property_key(backend: IcebergStorageBackend, key: &str) -> &str {
        match backend {
            IcebergStorageBackend::S3 => Self::s3_property_key(key),
            IcebergStorageBackend::Gcs => Self::gcs_property_key(key),
            IcebergStorageBackend::AzureBlob => Self::azure_blob_property_key(key),
        }
    }

    fn s3_property_key(key: &str) -> &str {
        let normalized = key.to_ascii_lowercase();
        match normalized.as_str() {
            "endpoint" | "s3.endpoint" => S3_ENDPOINT,
            "region" | "s3.region" => S3_REGION,
            "client_region" | "client.region" => CLIENT_REGION,
            "access_key_id" | "access-key-id" | "s3.access-key-id" => S3_ACCESS_KEY_ID,
            "secret_access_key" | "secret-access-key" | "s3.secret-access-key" => {
                S3_SECRET_ACCESS_KEY
            }
            "session_token" | "session-token" | "s3.session-token" => S3_SESSION_TOKEN,
            "path_style_access" | "path-style-access" | "s3.path-style-access" => {
                S3_PATH_STYLE_ACCESS
            }
            "allow_anonymous" | "allow-anonymous" | "s3.allow-anonymous" => S3_ALLOW_ANONYMOUS,
            "disable_ec2_metadata" | "disable-ec2-metadata" | "s3.disable-ec2-metadata" => {
                S3_DISABLE_EC2_METADATA
            }
            "disable_config_load" | "disable-config-load" | "s3.disable-config-load" => {
                S3_DISABLE_CONFIG_LOAD
            }
            _ => key,
        }
    }

    fn gcs_property_key(key: &str) -> &str {
        let normalized = key.to_ascii_lowercase();
        match normalized.as_str() {
            "endpoint" | "service_path" | "service-path" | "service.path" | "gcs.service.path" => {
                GCS_SERVICE_PATH
            }
            "credentials_json" | "credentials-json" | "credential" | "gcs.credentials-json" => {
                GCS_CREDENTIALS_JSON
            }
            "token" | "oauth2_token" | "oauth2-token" | "oauth2.token" | "gcs.oauth2.token" => {
                GCS_TOKEN
            }
            "no_auth" | "no-auth" | "gcs.no-auth" => GCS_NO_AUTH,
            "allow_anonymous" | "allow-anonymous" | "gcs.allow-anonymous" => GCS_ALLOW_ANONYMOUS,
            "disable_vm_metadata" | "disable-vm-metadata" | "gcs.disable-vm-metadata" => {
                GCS_DISABLE_VM_METADATA
            }
            "disable_config_load" | "disable-config-load" | "gcs.disable-config-load" => {
                GCS_DISABLE_CONFIG_LOAD
            }
            _ => key,
        }
    }

    fn azure_blob_property_key(key: &str) -> &str {
        let normalized = key.to_ascii_lowercase();
        match normalized.as_str() {
            "account_name" | "account-name" | "azure.account-name" | "adls.account-name" => {
                ADLS_ACCOUNT_NAME
            }
            "account_key" | "account-key" | "azure.account-key" | "adls.account-key" => {
                ADLS_ACCOUNT_KEY
            }
            "sas_token" | "sas-token" | "azure.sas-token" | "adls.sas-token" => ADLS_SAS_TOKEN,
            "tenant_id" | "tenant-id" | "azure.tenant-id" | "adls.tenant-id" => ADLS_TENANT_ID,
            "client_id" | "client-id" | "azure.client-id" | "adls.client-id" => ADLS_CLIENT_ID,
            "client_secret" | "client-secret" | "azure.client-secret" | "adls.client-secret" => {
                ADLS_CLIENT_SECRET
            }
            "authority_host"
            | "authority-host"
            | "azure.authority-host"
            | "adls.authority-host" => ADLS_AUTHORITY_HOST,
            "connection_string"
            | "connection-string"
            | "azure.connection-string"
            | "adls.connection-string" => ADLS_CONNECTION_STRING,
            _ => key,
        }
    }

    async fn rest_catalog(
        &self,
        name: &str,
        catalog_config: &[ClientConfigEntry],
    ) -> IcebergResult<RestCatalog> {
        let props = self
            .props
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .chain(
                catalog_config
                    .iter()
                    .map(|entry| (entry.key.clone(), entry.value.clone())),
            );
        RestCatalogBuilder::default()
            .with_storage_factory(self.backend.storage_factory())
            .load(name, props.collect())
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ::iceberg::{
        arrow::arrow_schema_to_schema_auto_assign_ids,
        io::FileIO,
        spec::{
            DataContentType, DataFileBuilder, FormatVersion, Operation, PartitionSpec, Schema,
            SortOrder, Struct, Summary, TableMetadataBuilder,
        },
    };
    use arrow_schema::Field;

    use super::*;

    fn commit_policy(interval_millis: u64, max_size: u64) -> IcebergCommitPolicy {
        IcebergCommitPolicy {
            interval: Duration::from_millis(interval_millis),
            max_size,
        }
    }

    /// What one acknowledgement handover was resolved as, counted by the test that shares it.
    #[derive(Default)]
    struct RecordedAcknowledgements {
        acknowledged: AtomicUsize,
        kept_alive: AtomicUsize,
        rejected: AtomicUsize,
    }

    /// One acknowledgement handover the sink holds, recording into the shared counters.
    struct RecordedHandover(StdArc<RecordedAcknowledgements>);

    impl SinkAcknowledgementServices for RecordedHandover {
        fn acknowledge(&self) {
            self.0.acknowledged.fetch_add(1, Ordering::Release);
        }

        fn keep_alive(&self) {
            self.0.kept_alive.fetch_add(1, Ordering::Release);
        }

        fn reject(&self, _reason: String) {
            self.0.rejected.fetch_add(1, Ordering::Release);
        }

        fn is_empty(&self) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn iceberg_catalog_transaction_disables_library_internal_retries() {
        let metadata = TableMetadataBuilder::new(
            Schema::builder().build().expect("valid empty schema"),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/table".to_string(),
            FormatVersion::V2,
            [(
                TableProperties::PROPERTY_COMMIT_NUM_RETRIES.to_string(),
                "7".to_string(),
            )]
            .into_iter()
            .collect(),
        )
        .expect("valid table metadata builder")
        .build()
        .expect("valid table metadata")
        .metadata;
        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("memory://warehouse/table/metadata/v1.json")
            .identifier(TableIdent::from_strs(["test", "table"]).expect("valid table ident"))
            .file_io(FileIO::new_with_memory())
            .runtime(::iceberg::Runtime::try_current().expect("test runs in Tokio runtime"))
            .build()
            .expect("valid in-memory table");

        let single_attempt = IcebergSinkClient::single_attempt_table(&table)
            .expect("single-attempt table must build");

        assert_eq!(
            single_attempt
                .metadata()
                .table_properties()
                .expect("valid transient table properties")
                .commit_num_retries,
            0
        );
        assert_eq!(
            table
                .metadata()
                .table_properties()
                .expect("valid source table properties")
                .commit_num_retries,
            7,
            "the catalog table metadata must not be mutated"
        );
    }

    #[test]
    fn iceberg_catalog_retry_retains_prepared_data_files_until_commit_finishes() {
        let append_id = uuid::Uuid::now_v7();
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/table/data/prepared.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(128)
            .record_count(2)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .expect("valid prepared Iceberg data file");
        let mut state = IcebergCommitState::default();
        state.store(IcebergPreparedCommit::with_append_id(
            vec![data_file],
            append_id,
        ));

        let first_attempt = state.prepared().expect("prepared append must be retained");
        assert_eq!(first_attempt.append_id(), append_id);
        assert_eq!(
            first_attempt.data_files()[0].file_path(),
            "s3://bucket/table/data/prepared.parquet"
        );
        let retry_attempt = state
            .prepared()
            .expect("a failed catalog attempt must not discard prepared files");
        assert_eq!(retry_attempt.append_id(), append_id);
        assert_eq!(retry_attempt.data_files(), first_attempt.data_files());

        state.finish();
        assert!(state.prepared().is_none());
    }

    #[test]
    fn iceberg_append_marker_identifies_an_ambiguously_successful_commit() {
        let append_id = uuid::Uuid::now_v7();
        let prepared = IcebergPreparedCommit::with_append_id(Vec::new(), append_id);
        let snapshot = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("s3://bucket/table/metadata/manifest-list.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: [(
                    ICEBERG_APPEND_ID_PROPERTY.to_string(),
                    append_id.to_string(),
                )]
                .into_iter()
                .collect(),
            })
            .build();

        assert!(prepared.matches_snapshot(&snapshot));
        assert!(
            !IcebergPreparedCommit::with_append_id(Vec::new(), uuid::Uuid::now_v7())
                .matches_snapshot(&snapshot)
        );
    }

    #[test]
    fn iceberg_staged_schema_uses_microsecond_utc_timestamps() {
        let mapped = arrow_schema::Schema::new(vec![Field::new(
            "observed_at",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            true,
        )]);

        let staged = IcebergSink::staged_arrow_schema(&mapped)
            .expect("a mapped datetime column must narrow to microseconds");

        assert_eq!(
            staged.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        );
        let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&staged)
            .expect("microsecond timestamp schema must convert to Iceberg");
        let serialized =
            serde_json::to_string(&iceberg_schema).expect("Iceberg schema must serialize");
        assert!(serialized.contains("timestamptz"));
        assert!(!serialized.contains("timestamptz_ns"));
    }

    #[test]
    fn iceberg_staged_schema_rejects_a_column_mapped_twice() {
        let mapped = arrow_schema::Schema::new(vec![
            Field::new("user_id", DataType::Int64, true),
            Field::new("user_id", DataType::Int64, true),
        ]);

        let error = IcebergSink::staged_arrow_schema(&mapped)
            .expect_err("a column written twice has no single staged value");

        assert_eq!(
            *error.current_context(),
            SinkStartError::InvalidConfiguration { sink: ICEBERG }
        );
    }

    #[test]
    fn iceberg_datetime_columns_are_staged_as_microseconds() {
        let values: ArrayRef = StdArc::new(
            TimestampNanosecondArray::from(vec![Some(1_234_567), None, Some(-1)])
                .with_timezone_utc(),
        );

        let staged = IcebergSink::staged_column(
            &values,
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        )
        .expect("a nanosecond datetime column must stage as microseconds");

        assert_eq!(
            staged.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        );
        let staged = staged
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Iceberg datetime column must be a microsecond timestamp array");
        assert_eq!(staged.value(0), 1_234);
        assert!(staged.is_null(1));
        assert_eq!(staged.value(2), -1);
    }

    #[test]
    fn iceberg_commit_deadline_holds_its_first_cadence_until_the_maximum_size() {
        let policy = commit_policy(100, 1_024);
        let mut deadline = IcebergCommitDeadline::default();

        deadline.arm(policy, Timestamp::from_unix_nanos(1_000), 16);
        assert_eq!(
            deadline.due_at(),
            Some(Timestamp::from_unix_nanos(100_001_000))
        );

        // A later batch joins the same staged set without moving the cadence it already started.
        deadline.arm(policy, Timestamp::from_unix_nanos(50_000_000), 32);
        assert_eq!(
            deadline.due_at(),
            Some(Timestamp::from_unix_nanos(100_001_000))
        );

        // Reaching the declared maximum makes the commit due as of that staging time.
        deadline.arm(policy, Timestamp::from_unix_nanos(60_000_000), 1_024);
        assert_eq!(
            deadline.due_at(),
            Some(Timestamp::from_unix_nanos(60_000_000))
        );

        deadline.clear();
        assert_eq!(deadline.due_at(), None);
    }

    #[test]
    fn iceberg_retained_acknowledgements_resolve_every_staged_batch_at_once() {
        let first = StdArc::new(RecordedAcknowledgements::default());
        let second = StdArc::new(RecordedAcknowledgements::default());
        let retained = RetainedAcknowledgements(vec![
            SinkAcknowledgements::new(RecordedHandover(first.clone())),
            SinkAcknowledgements::new(RecordedHandover(second.clone())),
        ]);

        retained.keep_alive();
        retained.acknowledge();
        retained.reject("Iceberg emitter dropped staged batch".to_string());

        assert!(!retained.is_empty());
        for recorded in [&first, &second] {
            assert_eq!(recorded.kept_alive.load(Ordering::Acquire), 1);
            assert_eq!(recorded.acknowledged.load(Ordering::Acquire), 1);
            assert_eq!(recorded.rejected.load(Ordering::Acquire), 1);
        }
    }
}
