//! Iceberg emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The REST catalog and object-store clients one Iceberg table is loaded and committed
//!   through, the local Arrow IPC staging of every mapped batch, what the staged files wait for —
//!   the `COMMIT EACH` cadence the host measures from their staging, or the maximum commit size
//!   that makes them due at once — the Parquet data files one commit writes, the bulk memory
//!   charge of the staged files it loads, and the acknowledgements it retains until that commit
//!   succeeds.
//! - **Depends on.** The connector contract, the bounded executor its host hands it, vocabulary
//!   values, Arrow arrays, `error-stack`, Tokio, `nervix-dns`, Reqwest, OpenDAL, and the `iceberg`
//!   crates.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.
//!
//! Local staging is not an acknowledgement boundary. The emitter completion point for `MODE ACK` is
//! the successful catalog commit, and an appended row is never idempotent, as
//! [Emitters](docs/src/emitters.md) documents.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

mod offline_config;
mod parquet_bounds;
mod storage;
mod table_projection;

use std::{fs::File, ops::Range, path::PathBuf, time::Duration};

use ::iceberg::{
    Catalog, CatalogBuilder, Error as IcebergError, ErrorKind as IcebergErrorKind, NamespaceIdent,
    Result as IcebergResult, TableIdent,
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
use ahash::HashSet;
use arch_into::ArchInto as _;
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, LargeListArray, ListArray, MapArray,
    RecordBatch, RecordBatchOptions, StructArray, TimestampMicrosecondArray,
    TimestampNanosecondArray,
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, TimeUnit};
use arrow_select::filter::filter as filter_arrow_array;
use error_stack::{Report, ResultExt as _};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    ConfiguredDeadline, EmitterCommitReport, EmitterFinish, EmitterFinishResult, EmitterHost,
    EmitterLifecycle, EmitterOpenError, EmitterRecordAcknowledgementServices,
    EmitterRecordAcknowledgements, EmitterRecordPosition, EmitterStagedCommit, EmitterStartResult,
    EmitterWriteError, EmitterWriteResult, MappedEmitterCarrier, MappedEmitterRows,
    MeasuredRequest, PerRecordOutcome, RowEmitter, RowRequest, RowRequestLimits,
    RustlsClientConfigProvider, physical_time::actual_utc_now,
};
use nervix_dns::DnsResolver;
use nervix_execution::{
    AdmissionError, CpuClass, Executor, MemoryClass, QueueAdmission, Reservation, StorageClass,
};
use nervix_models::{
    ClientConfigEntry, EmitterBatchPolicy, IcebergCatalogProperties, IcebergStorageBackend,
    IcebergTableName, Timestamp,
};
use nervix_primitives::{sync::StdArc, time::Instant};
pub use offline_config::{validate_catalog_configuration, validate_client_configuration};
use parquet::file::properties::WriterProperties;
use parquet_bounds::measured_file_size;
use storage::DnsStorageFactory;
use table_projection::{IcebergColumnError, IcebergTableProjection};
use tempfile::TempDir;
use tracing::{debug, trace, warn};
use url::Url;

const ICEBERG: &str = "iceberg";

/// The snapshot property that names the append one prepared commit stands for, so a retry after an
/// ambiguous catalog result recognizes its own work instead of appending it twice.
const ICEBERG_APPEND_ID_PROPERTY: &str = "nervix.emitter.append-id";

/// When staged data is published: the domain duration that passes after it is staged, and the
/// staged size that commits it before that duration elapses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IcebergCommitPolicy {
    pub interval: Duration,
    pub max_size: u64,
}

impl IcebergCommitPolicy {
    /// What `staged_bytes` of staged files wait for: this policy's cadence, or nothing once they
    /// reach the maximum commit size.
    fn staged_commit(self, staged_bytes: u64) -> EmitterStagedCommit {
        if staged_bytes >= self.max_size {
            return EmitterStagedCommit::SizeReached;
        }
        EmitterStagedCommit::Cadence(self.interval)
    }
}

/// What one Iceberg emitter stages and commits through, from its typed emitter plan.
pub struct IcebergTableEmitterConfig {
    pub backend: IcebergStorageBackend,
    pub dns: DnsResolver,
    pub storage_config: Vec<ClientConfigEntry>,
    pub catalog_name: String,
    pub catalog_config: Vec<ClientConfigEntry>,
    /// The catalog namespace this emitter's table lives in.
    pub namespace: nervix_models::IcebergNamespace,
    pub table: IcebergTableName,
    pub location: String,
    /// The mapped columns every staged batch carries, in `VALUES` order, each nullable exactly
    /// when its expression can produce a null.
    pub mapped_schema: StdArc<arrow_schema::Schema>,
    /// The optional hard row and encoded-Parquet-byte limits of one appended data file.
    pub batch: Option<EmitterBatchPolicy>,
    pub commit: IcebergCommitPolicy,
    /// How generated staging and data files name the writer that produced them.
    pub writer: String,
}

/// What one staged write is charged. The batch is already in memory and the writer streams it to
/// the file, so the charge only admits the write onto the filesystem workers.
const STAGED_WRITE_RESERVATION_BYTES: u64 = 1;

/// The Iceberg emitter, which stages each mapped batch as a local Arrow IPC file and appends every
/// staged file to its table in one catalog commit.
pub struct IcebergTableEmitter {
    client: IcebergTableEmitterClient,
    commit_state: IcebergCommitState,
    /// The exact Arrow schema every staged file is written and read back with, which is the host's
    /// mapped schema with datetime columns narrowed to the microsecond resolution Iceberg stores.
    /// Each column keeps its expression's nullability, which a commit checks against the columns
    /// the table requires.
    staged_schema: StdArc<arrow_schema::Schema>,
    commit_policy: IcebergCommitPolicy,
    batch_policy: Option<EmitterBatchPolicy>,
    staging_dir: TempDir,
    /// The node's bounded executor, whose filesystem workers write and read the staged files.
    executor: Executor,
    staged_sequence: u64,
    staged_batches: Vec<IcebergStagedBatch>,
    staged_rows: u64,
    staged_bytes: u64,
}

/// One mapped batch written to local Arrow IPC, with the acknowledgements its commit resolves.
struct IcebergStagedBatch {
    path: PathBuf,
    rows: u64,
    bytes: u64,
    acknowledgements: Option<EmitterRecordAcknowledgements>,
    domain_timestamp: Timestamp,
}

/// Every acknowledgement this emitter retained, as one handle the host keeps alive while it waits.
struct RetainedAcknowledgements(Vec<EmitterRecordAcknowledgements>);

impl EmitterRecordAcknowledgementServices for RetainedAcknowledgements {
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
        self.0.iter().all(EmitterRecordAcknowledgements::is_empty)
    }
}

/// The catalog and table one emitter commits through, and the sequence its data files are named by.
struct IcebergTableEmitterClient {
    catalog: StdArc<dyn Catalog>,
    table: Table,
    file_name_prefix: String,
    data_file_sequence: u64,
}

/// One commit's progress: the leading staged files it publishes, the data files written for those
/// it has loaded so far, and the append identity that recognizes those data files in the table. A
/// failed attempt keeps it, so the retry neither writes the data files again nor publishes files
/// staged after the commit began.
///
/// A commit loads the files it covers in groups the bulk memory class can hold at once and writes
/// each group's rows into data files of their own; the data files of every group join the one
/// catalog append.
struct IcebergCommitPreparation {
    append_id: uuid::Uuid,
    /// The leading staged files this commit publishes.
    covered: usize,
    /// The leading staged files whose rows `data_files` already hold.
    written: usize,
    data_files: Vec<DataFile>,
}

impl IcebergCommitPreparation {
    /// A commit of the `covered` leading staged files, none of them written yet.
    fn new(covered: usize) -> Self {
        Self::with_append_id(covered, uuid::Uuid::now_v7())
    }

    fn with_append_id(covered: usize, append_id: uuid::Uuid) -> Self {
        Self {
            append_id,
            covered,
            written: 0,
            data_files: Vec::new(),
        }
    }

    /// Where the covered files no data file holds yet are among the staged files.
    fn unwritten(&self) -> std::ops::Range<usize> {
        self.written..self.covered
    }

    /// Whether data files hold every covered file, so the commit can append them.
    fn is_written(&self) -> bool {
        self.written == self.covered
    }

    /// Records that `data_files` hold the rows of the next `files` covered staged files.
    fn record_written(&mut self, files: usize, data_files: Vec<DataFile>) {
        let written = self
            .written
            .checked_add(files)
            .assured("a commit writes no more files than it covers");
        debug_assert!(
            written <= self.covered,
            "a commit writes only the files it covers"
        );
        self.written = written;
        self.data_files.extend(data_files);
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

/// What the emitter's commits carry from one attempt to the next.
#[derive(Default)]
struct IcebergCommitState {
    /// The commit in progress, absent between commits.
    preparation: Option<IcebergCommitPreparation>,
    /// What commits published during an attempt that went on to fail, which the next attempt that
    /// succeeds reports beside its own.
    unreported: Option<EmitterCommitReport>,
}

impl IcebergCommitState {
    /// Adds `published` to what this emitter published and has not reported yet.
    fn record_published(&mut self, published: EmitterCommitReport) {
        let merged = match self.unreported.take() {
            Some(earlier) => EmitterCommitReport {
                messages: earlier
                    .messages
                    .checked_add(published.messages)
                    .assured("the total counts rows this emitter already staged on disk"),
                bytes: earlier
                    .bytes
                    .checked_add(published.bytes)
                    .assured("the total counts bytes this emitter already staged on disk"),
                domain_timestamp: earlier.domain_timestamp.max(published.domain_timestamp),
            },
            None => published,
        };
        self.unreported = Some(merged);
    }
}

/// The staged files one load reads into memory at once, and what it charges for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IcebergLoadGroup {
    files: usize,
    charge: IcebergLoadCharge,
}

impl IcebergLoadGroup {
    /// The leading files of `unwritten` a bulk memory class of `capacity` bytes holds together, and
    /// at least the first. A first file larger than the whole class is read alone, charged with the
    /// whole class.
    fn leading(unwritten: &[IcebergStagedBatch], capacity: u64) -> Self {
        let mut files = 0_usize;
        let mut bytes = 0_u64;
        for staged in unwritten {
            let with_file = bytes
                .checked_add(staged.bytes)
                .assured("the total counts bytes this emitter already staged on disk");
            if files > 0 && with_file > capacity {
                break;
            }
            files = files
                .checked_add(1)
                .assured("a group counts files this emitter already staged");
            bytes = with_file;
        }
        let charge = if bytes > capacity {
            IcebergLoadCharge::WholeClass {
                file_bytes: bytes,
                capacity,
            }
        } else {
            IcebergLoadCharge::Staged { bytes }
        };
        Self { files, charge }
    }
}

/// What one load charges the bulk memory class for the staged files it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IcebergLoadCharge {
    /// Every byte of the files, which the class holds together.
    Staged { bytes: u64 },
    /// One staged file larger than the whole class, which a route on the same node can produce by
    /// flushing batches above it. The load charges the whole class: every byte of the file the
    /// class can account for, leaving none for other bulk work beside the read.
    WholeClass { file_bytes: u64, capacity: u64 },
}

impl IcebergLoadCharge {
    /// The bytes the load reserves in the bulk memory class.
    fn reserved_bytes(self) -> u64 {
        match self {
            Self::Staged { bytes } => bytes,
            Self::WholeClass { capacity, .. } => capacity,
        }
    }

    /// The commit failure the class's refusal of this charge becomes. Its description is what the
    /// emitter shows as its transient error while it retries the commit, which keeps its staged
    /// files and acknowledgements.
    fn refused(self, refusal: Report<AdmissionError>) -> Report<EmitterWriteError> {
        let description = match (refusal.current_context(), self) {
            (AdmissionError::BudgetExhausted { .. }, Self::Staged { bytes }) => format!(
                "Iceberg commit waits for room in the bulk memory class for {bytes} staged bytes"
            ),
            (
                AdmissionError::BudgetExhausted { .. },
                Self::WholeClass {
                    file_bytes,
                    capacity,
                },
            ) => format!(
                "Iceberg commit waits for the whole bulk memory class of {capacity} bytes for a \
                 staged file of {file_bytes} bytes"
            ),
            (
                AdmissionError::ExceedsBudget { .. }
                | AdmissionError::BudgetClosed { .. }
                | AdmissionError::DifferentBudget { .. },
                _,
            ) => format!(
                "Iceberg commit could not charge the bulk memory class: {}",
                refusal.current_context()
            ),
        };
        refusal
            .change_context(EmitterWriteError::Commit { connector: ICEBERG })
            .attach_printable(description)
    }
}

/// The rows of one load group, read back into memory, and the bulk memory charge that covers them
/// for as long as the commit holds them.
struct IcebergLoadedGroup {
    batches: Vec<RecordBatch>,
    charge: Reservation,
}

/// Which of an Iceberg emitter's two HTTP clients could not be configured from its TLS files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum IcebergClientTlsError {
    /// The REST catalog client, configured from the `ICEBERG_REST` client's files.
    #[error("failed to configure the Iceberg catalog client's TLS")]
    Catalog,
    /// The object-store client, configured from the storage client's files.
    #[error("failed to configure the Iceberg object-store client's TLS")]
    ObjectStore,
}

impl IcebergClientTlsError {
    /// The rustls configuration this client verifies servers with and presents its identity from:
    /// the bundled WebPKI and platform roots with the client's `tls_ca_file` added, and the identity
    /// of its `tls_cert_file` and `tls_key_file`.
    fn client_config(
        self,
        config: &[ClientConfigEntry],
    ) -> EmitterStartResult<StdArc<rustls::ClientConfig>> {
        RustlsClientConfigProvider::new(config)
            .build_with_default_roots()
            .change_context(self)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: ICEBERG })
    }
}

/// What an emitter opens its REST catalog with: the properties the pairing of its two clients
/// yields, the catalog client's TLS configuration, and the storage factory its tables' file I/O
/// uses.
struct IcebergCatalogConnection<'a> {
    name: &'a str,
    properties: IcebergCatalogProperties<'a>,
    tls: &'a rustls::ClientConfig,
    storage: DnsStorageFactory,
}

impl IcebergTableEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::Iceberg(nervix_models::IcebergEmitterEntity::Table);

    pub async fn new(
        config: IcebergTableEmitterConfig,
        host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        let IcebergTableEmitterConfig {
            backend,
            dns,
            storage_config,
            catalog_name,
            catalog_config,
            namespace,
            table,
            location,
            mapped_schema,
            batch,
            commit,
            writer,
        } = config;
        let staged_schema = Self::staged_arrow_schema(&mapped_schema)?;
        let staging_dir = Self::create_staging_dir(&host)?;
        let executor = host.executor();
        let storage_root = Self::validate_blob_location(backend, "table", &location)?;
        let storage_deadline = ConfiguredDeadline::timeout(&storage_config)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: ICEBERG })?;
        let properties = IcebergCatalogProperties::pair(backend, &storage_config, &catalog_config)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: ICEBERG })?;
        let catalog_tls = IcebergClientTlsError::Catalog.client_config(&catalog_config)?;
        let storage_tls = IcebergClientTlsError::ObjectStore.client_config(&storage_config)?;
        let initialize_catalog = |error: IcebergError| {
            Report::new(EmitterOpenError::Initialize { connector: ICEBERG }).attach_printable(
                format!("failed to initialize Iceberg catalog '{catalog_name}': {error}"),
            )
        };
        let storage =
            DnsStorageFactory::new(backend, &dns, &storage_root, storage_deadline, &storage_tls)
                .map_err(initialize_catalog)?;
        let catalog: StdArc<dyn Catalog> = StdArc::new(
            IcebergCatalogConnection {
                name: &catalog_name,
                properties,
                tls: &catalog_tls,
                storage,
            }
            .open(&dns)
            .await
            .map_err(initialize_catalog)?,
        );
        let namespace = NamespaceIdent::from_strs(namespace.levels())
            .assured("a validated Iceberg namespace has at least one nonempty level");
        let table_ident = TableIdent::new(namespace, table.as_str().to_string());
        let loaded = catalog.load_table(&table_ident).await.map_err(|error| {
            Report::new(EmitterOpenError::Initialize { connector: ICEBERG }).attach_printable(
                format!("failed to initialize Iceberg table {table_ident}: {error}"),
            )
        })?;
        if loaded.metadata().location() != location {
            return Err(
                Report::new(EmitterOpenError::InvalidConfiguration { connector: ICEBERG })
                    .attach_printable(format!(
                        "table '{table_ident}' is registered at '{}' but emitter location is \
                         '{location}'",
                        loaded.metadata().location()
                    )),
            );
        }
        Self::validate_table_schema(&staged_schema, loaded.metadata().current_schema())?;
        Ok(Self {
            client: IcebergTableEmitterClient {
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
            batch_policy: batch,
            staging_dir,
            executor,
            staged_sequence: 0,
            staged_batches: Vec::new(),
            staged_rows: 0,
            staged_bytes: 0,
        })
    }

    /// The Arrow schema staged files carry, which narrows every UTC datetime column to the
    /// microsecond resolution an Iceberg `timestamptz` stores and keeps each column's nullability.
    fn staged_arrow_schema(
        mapped_schema: &arrow_schema::Schema,
    ) -> EmitterStartResult<StdArc<arrow_schema::Schema>> {
        let mut seen = HashSet::default();
        let mut fields = Vec::with_capacity(mapped_schema.fields().len());
        for field in mapped_schema.fields() {
            if !seen.insert(field.name().clone()) {
                return Err(Report::new(EmitterOpenError::InvalidConfiguration {
                    connector: ICEBERG,
                })
                .attach_printable(format!("duplicate mapped column: {}", field.name())));
            }
            fields.push(arrow_schema::Field::new(
                field.name(),
                Self::staged_arrow_data_type(field.data_type()),
                field.is_nullable(),
            ));
        }
        Ok(StdArc::new(arrow_schema::Schema::new(fields)))
    }

    fn staged_arrow_data_type(data_type: &DataType) -> DataType {
        match data_type {
            DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone))
                if timezone.as_ref() == "+00:00" || timezone.as_ref() == "UTC" =>
            {
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
            }
            DataType::List(field) => DataType::List(StdArc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(Self::staged_arrow_data_type(field.data_type())),
            )),
            DataType::LargeList(field) => DataType::LargeList(StdArc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(Self::staged_arrow_data_type(field.data_type())),
            )),
            DataType::FixedSizeList(field, size) => DataType::FixedSizeList(
                StdArc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(Self::staged_arrow_data_type(field.data_type())),
                ),
                *size,
            ),
            DataType::Struct(fields) => DataType::Struct(
                fields
                    .iter()
                    .map(|field| {
                        StdArc::new(
                            field
                                .as_ref()
                                .clone()
                                .with_data_type(Self::staged_arrow_data_type(field.data_type())),
                        )
                    })
                    .collect(),
            ),
            DataType::Map(entries, ordered) => DataType::Map(
                StdArc::new(
                    entries
                        .as_ref()
                        .clone()
                        .with_data_type(Self::staged_arrow_data_type(entries.data_type())),
                ),
                *ordered,
            ),
            _ => data_type.clone(),
        }
    }

    fn create_staging_dir(host: &EmitterHost) -> EmitterStartResult<TempDir> {
        let root = host.staging_directory();
        std::fs::create_dir_all(&root).map_err(|error| {
            Report::new(EmitterOpenError::Initialize { connector: ICEBERG }).attach_printable(
                format!(
                    "failed to create Iceberg staging directory under '{}': {error}",
                    root.display()
                ),
            )
        })?;
        TempDir::new_in(&root).map_err(|error| {
            Report::new(EmitterOpenError::Initialize { connector: ICEBERG }).attach_printable(
                format!(
                    "failed to create Iceberg staging directory under '{}': {error}",
                    root.display()
                ),
            )
        })
    }

    fn validate_blob_location(
        backend: IcebergStorageBackend,
        label: &str,
        location: &str,
    ) -> EmitterStartResult<Url> {
        let invalid = |reason: String| {
            Report::new(EmitterOpenError::InvalidConfiguration { connector: ICEBERG })
                .attach_printable(reason)
        };
        let url = Url::parse(location)
            .map_err(|error| invalid(format!("{label} location '{location}': {error}")))?;
        if !backend.location_schemes().contains(&url.scheme()) {
            let expected = backend
                .location_schemes()
                .iter()
                .map(|scheme| format!("{scheme}://"))
                .collect::<Vec<_>>()
                .join(" or ");
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
            let service = if url.scheme().starts_with("abfs") {
                "dfs"
            } else {
                "blob"
            };
            if host.split('.').nth(1) != Some(service) {
                return Err(invalid(format!(
                    "{label} location '{location}' must use an Azure {service} host"
                )));
            }
        }
        Ok(url)
    }

    /// The staged columns of one carrier: the rows the host selected, in this emitter's exact staged
    /// types.
    fn staged_batch(&self, rows: &MappedEmitterCarrier<'_>) -> EmitterWriteResult<RecordBatch> {
        let row_count = rows.batch.num_rows();
        let selects_every_row = rows.selected_rows.len() == row_count;
        let mut selected = vec![false; row_count];
        if !selects_every_row {
            for row in rows.selected_rows {
                let Some(selected) = selected.get_mut(*row) else {
                    return Err(
                        Report::new(EmitterWriteError::Publish { connector: ICEBERG })
                            .attach_printable(format!(
                                "Iceberg selected row {row} outside {row_count} mapped rows"
                            )),
                    );
                };
                *selected = true;
            }
        }
        let predicate = BooleanArray::from(selected);
        let mut columns = Vec::with_capacity(self.staged_schema.fields().len());
        for (index, field) in self.staged_schema.fields().iter().enumerate() {
            let Some(column) = rows.batch.columns().get(index) else {
                return Err(
                    Report::new(EmitterWriteError::Publish { connector: ICEBERG })
                        .attach_printable(format!(
                            "Iceberg mapped batch has {} columns for {} staged columns",
                            rows.batch.num_columns(),
                            self.staged_schema.fields().len()
                        )),
                );
            };
            let column = Self::staged_column(column, field.data_type())?;
            columns.push(if selects_every_row {
                column
            } else {
                filter_arrow_array(column.as_ref(), &predicate)
                    .change_context(EmitterWriteError::Publish { connector: ICEBERG })?
            });
        }
        if columns.is_empty() {
            return RecordBatch::try_new_with_options(
                self.staged_schema.clone(),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(rows.selected_rows.len())),
            )
            .change_context(EmitterWriteError::Publish { connector: ICEBERG });
        }
        RecordBatch::try_new(self.staged_schema.clone(), columns)
            .change_context(EmitterWriteError::Publish { connector: ICEBERG })
    }

    /// One mapped column in the exact type its staged file carries.
    fn staged_column(column: &ArrayRef, data_type: &DataType) -> EmitterWriteResult<ArrayRef> {
        if column.data_type() == data_type {
            return Ok(column.clone());
        }
        let invalid_type = || {
            Report::new(EmitterWriteError::Publish { connector: ICEBERG }).attach_printable(
                format!(
                    "Iceberg column arrived as {} instead of staged type {data_type}",
                    column.data_type()
                ),
            )
        };
        match (column.data_type(), data_type) {
            (
                DataType::Timestamp(TimeUnit::Nanosecond, _),
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
            ) => {
                let values = column
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(invalid_type)?;
                let microseconds = values
                    .iter()
                    .map(|value| value.map(|nanos| nanos.div_euclid(1_000)))
                    .collect::<TimestampMicrosecondArray>()
                    .with_timezone(timezone.clone());
                Ok(StdArc::new(microseconds))
            }
            (DataType::List(_), DataType::List(field)) => {
                let list = column
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(invalid_type)?;
                let values = Self::staged_column(list.values(), field.data_type())?;
                Ok(StdArc::new(
                    ListArray::try_new(
                        field.clone(),
                        list.offsets().clone(),
                        values,
                        list.nulls().cloned(),
                    )
                    .change_context(EmitterWriteError::Publish { connector: ICEBERG })?,
                ))
            }
            (DataType::LargeList(_), DataType::LargeList(field)) => {
                let list = column
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .ok_or_else(invalid_type)?;
                let values = Self::staged_column(list.values(), field.data_type())?;
                Ok(StdArc::new(
                    LargeListArray::try_new(
                        field.clone(),
                        list.offsets().clone(),
                        values,
                        list.nulls().cloned(),
                    )
                    .change_context(EmitterWriteError::Publish { connector: ICEBERG })?,
                ))
            }
            (DataType::FixedSizeList(_, _), DataType::FixedSizeList(field, size)) => {
                let list = column
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(invalid_type)?;
                let values = Self::staged_column(list.values(), field.data_type())?;
                Ok(StdArc::new(
                    FixedSizeListArray::try_new(
                        field.clone(),
                        *size,
                        values,
                        list.nulls().cloned(),
                    )
                    .change_context(EmitterWriteError::Publish { connector: ICEBERG })?,
                ))
            }
            (DataType::Struct(_), DataType::Struct(fields)) => {
                let row = column
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(invalid_type)?;
                let columns = fields
                    .iter()
                    .zip(row.columns())
                    .map(|(field, column)| Self::staged_column(column, field.data_type()))
                    .collect::<EmitterWriteResult<Vec<_>>>()?;
                Ok(StdArc::new(
                    StructArray::try_new(fields.clone(), columns, row.nulls().cloned())
                        .change_context(EmitterWriteError::Publish { connector: ICEBERG })?,
                ))
            }
            (DataType::Map(_, _), DataType::Map(field, ordered)) => {
                let map = column
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(invalid_type)?;
                let entries: ArrayRef = StdArc::new(map.entries().clone());
                let entries = Self::staged_column(&entries, field.data_type())?;
                let entries = entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(invalid_type)?;
                Ok(StdArc::new(
                    MapArray::try_new(
                        field.clone(),
                        map.offsets().clone(),
                        entries.clone(),
                        map.nulls().cloned(),
                        *ordered,
                    )
                    .change_context(EmitterWriteError::Publish { connector: ICEBERG })?,
                ))
            }
            _ => Err(invalid_type()),
        }
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

    /// Write one staged file on the node's filesystem workers. The batch is already in memory and
    /// the writer streams it to the file, so the charge only admits the write.
    async fn write_ipc_batch(
        executor: &Executor,
        path: PathBuf,
        batch: RecordBatch,
    ) -> EmitterWriteResult<u64> {
        let staged = |error: &dyn std::fmt::Display, path: &PathBuf| {
            Report::new(EmitterWriteError::Publish { connector: ICEBERG }).attach_printable(
                format!(
                    "failed to write Iceberg staged Arrow IPC '{}': {error}",
                    path.display()
                ),
            )
        };
        let reservation = executor
            .reserve(MemoryClass::Bulk, STAGED_WRITE_RESERVATION_BYTES)
            .await
            .change_context(EmitterWriteError::Publish { connector: ICEBERG })?;
        executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation| {
                    cancellation
                        .check()
                        .change_context(EmitterWriteError::Publish { connector: ICEBERG })?;
                    let file = File::create(&path).map_err(|error| staged(&error, &path))?;
                    let mut writer = StreamWriter::try_new(file, batch.schema().as_ref())
                        .map_err(|error| staged(&error, &path))?;
                    writer
                        .write(&batch)
                        .map_err(|error| staged(&error, &path))?;
                    writer.finish().map_err(|error| staged(&error, &path))?;
                    {
                        let mapping_input = std::fs::metadata(&path).map(|metadata| metadata.len());
                        match mapping_input {
                            Ok(mapped_value) => Ok(mapped_value),
                            Err(error) => Err(staged(&error, &path)),
                        }
                    }
                },
            )
            .await
            .change_context(EmitterWriteError::Publish { connector: ICEBERG })?
    }

    /// Read the staged files at `paths` back on the node's filesystem workers once the bulk memory
    /// class has granted their `charge`, checking before each file whether the commit stopped
    /// waiting. The reservation comes back with the rows it covers, so it lasts for as long as the
    /// commit holds them.
    ///
    /// The charge is taken only when the class has room for it now: a place in the class's queue
    /// would take every byte the class frees until it was satisfied, holding the node's other bulk
    /// work behind this commit. The read waits for a place among the filesystem workers instead of
    /// being refused, because nothing could present again the rows the emitter already accepted.
    async fn load_group(
        executor: &Executor,
        schema: StdArc<arrow_schema::Schema>,
        paths: Vec<PathBuf>,
        charge: IcebergLoadCharge,
    ) -> EmitterWriteResult<IcebergLoadedGroup> {
        let staged = |error: String, path: &PathBuf| {
            Report::new(EmitterWriteError::Commit { connector: ICEBERG }).attach_printable(format!(
                "failed to read Iceberg staged Arrow IPC '{}': {error}",
                path.display()
            ))
        };
        let reservation = executor
            .try_reserve(MemoryClass::Bulk, charge.reserved_bytes())
            .map_err(|refusal| charge.refused(refusal))?;
        executor
            .run_storage_with(
                StorageClass::Filesystem,
                QueueAdmission::WaitForPlace,
                reservation,
                move |reservation, cancellation| {
                    let mut batches = Vec::new();
                    for path in paths {
                        cancellation
                            .check()
                            .change_context(EmitterWriteError::Commit { connector: ICEBERG })?;
                        let file =
                            File::open(&path).map_err(|error| staged(error.to_string(), &path))?;
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
                    Ok(IcebergLoadedGroup {
                        batches,
                        charge: reservation,
                    })
                },
            )
            .await
            .change_context(EmitterWriteError::Commit { connector: ICEBERG })?
    }

    /// Every acknowledgement this emitter still holds, newest staged batch last.
    fn retained_acknowledgements(&self) -> Vec<EmitterRecordAcknowledgements> {
        self.staged_batches
            .iter()
            .filter_map(|batch| batch.acknowledgements.clone())
            .collect()
    }
}

impl Drop for IcebergTableEmitter {
    fn drop(&mut self) {
        for acknowledgements in self.retained_acknowledgements() {
            acknowledgements.reject("Iceberg emitter dropped staged batch".to_string());
        }
    }
}

#[async_trait::async_trait]
impl EmitterLifecycle for IcebergTableEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// The catalog and object-store clients hold nothing between writes. Rows wait staged for
    /// the commit, which a stop asks for before it finishes the emitter.
    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }

    /// A staged batch outlives its catalog client, so a publish failure keeps the client rather
    /// than reopening it and losing what is already on disk.
    fn keeps_client_on_publish_failure(&self) -> bool {
        true
    }

    fn staged_commit(&self) -> Option<EmitterStagedCommit> {
        if self.staged_batches.is_empty() {
            return None;
        }
        Some(self.commit_policy.staged_commit(self.staged_bytes))
    }

    fn pending_acks(&self) -> Option<EmitterRecordAcknowledgements> {
        let retained = self.retained_acknowledgements();
        if retained.is_empty() {
            return None;
        }
        Some(EmitterRecordAcknowledgements::new(
            RetainedAcknowledgements(retained),
        ))
    }

    fn retains_acknowledgements(&self) -> bool {
        true
    }

    fn staged_messages(&self) -> u64 {
        self.staged_rows
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "external Iceberg SDK commit and error formatting own their \
                                   effects; no internal runtime ownership is inferred")
    )]
    async fn commit(&mut self) -> EmitterWriteResult<Option<EmitterCommitReport>> {
        // A commit that resumes publishes only the files it began with, so the files staged since
        // then follow in a commit of their own until nothing staged is left.
        loop {
            nervix_primitives::task::consume_budget().await;
            if self.staged_batches.is_empty() {
                return Ok(self.commit_state.unreported.take());
            }
            let published = self.commit_covered().await?;
            self.commit_state.record_published(published);
        }
    }
}

#[async_trait::async_trait]
impl RowEmitter for IcebergTableEmitter {
    /// Stages each carrier in one file without BATCH, or in bounded files with BATCH.
    async fn publish(
        &mut self,
        rows: MappedEmitterRows<'_>,
    ) -> PerRecordOutcome<EmitterRecordPosition> {
        let mut outcome = PerRecordOutcome::with_capacity(rows.member_count());
        for carrier in rows.carriers {
            nervix_primitives::task::consume_budget().await;
            if let Err(error) = self.stage(carrier, &mut outcome).await {
                outcome.fail(error);
                return outcome;
            }
        }
        outcome
    }
}

impl IcebergTableEmitter {
    /// Divides a carrier into staged files by the exact Parquet bytes its current table schema
    /// would write. A row too large to fit in one file is returned for message-error delivery.
    async fn staged_file_division(
        &self,
        staged: &RecordBatch,
        policy: EmitterBatchPolicy,
    ) -> EmitterWriteResult<nervix_connector::RowRequests<()>> {
        let projection = IcebergTableProjection::plan(
            &self.staged_schema,
            self.client.table.metadata().current_schema(),
        )
        .map_err(IcebergColumnError::publish_failure)?;
        let projected = projection
            .project(staged)
            .map_err(IcebergColumnError::publish_failure)?;
        let reservation = self
            .executor
            .reserve(MemoryClass::Bulk, STAGED_WRITE_RESERVATION_BYTES)
            .await
            .change_context(EmitterWriteError::Publish { connector: ICEBERG })?;
        self.executor
            .run_cpu_with(
                CpuClass::Bulk,
                QueueAdmission::WaitForPlace,
                reservation,
                move |_reservation, cancellation| {
                    RowRequestLimits::from(policy).try_divide(projected.num_rows(), |members| {
                        cancellation
                            .check()
                            .change_context(EmitterWriteError::Publish { connector: ICEBERG })?;
                        let candidate = projected.slice(members.start, members.len());
                        let size = measured_file_size(&candidate)
                            .change_context(EmitterWriteError::Publish { connector: ICEBERG })?;
                        Ok(MeasuredRequest { size, request: () })
                    })
                },
            )
            .await
            .change_context(EmitterWriteError::Publish { connector: ICEBERG })?
    }

    /// Stages a carrier's accepted rows in BATCH-sized files, retaining its one acknowledgement
    /// handle on the final file until the catalog commit publishes all of them.
    async fn stage(
        &mut self,
        carrier: MappedEmitterCarrier<'_>,
        outcome: &mut PerRecordOutcome<EmitterRecordPosition>,
    ) -> EmitterWriteResult<()> {
        let staged = self.staged_batch(&carrier)?;
        let mut ranges = Vec::<Range<usize>>::new();
        let mut rejected_rows = vec![false; staged.num_rows()];
        if let Some(policy) = self.batch_policy {
            for request in self.staged_file_division(&staged, policy).await?.requests {
                match request {
                    RowRequest::Write { members, .. } => ranges.push(members),
                    RowRequest::Oversize { member, oversize } => {
                        rejected_rows[member] = true;
                        outcome.reject(oversize.rejected(
                            EmitterRecordPosition {
                                batch_index: carrier.batch_index,
                                row_index: carrier.selected_rows[member],
                            },
                            carrier.occurred_at,
                            "Iceberg Parquet data file",
                        ));
                    }
                }
            }
        } else {
            ranges.push(0..staged.num_rows());
        }

        let mut new_files = Vec::with_capacity(ranges.len());
        let mut staged_rows = 0_u64;
        let mut staged_bytes = 0_u64;
        for range in ranges {
            let path = self.next_staged_path();
            let rows: u64 = range.len().arch_into();
            let batch = staged.slice(range.start, range.len());
            let bytes = match Self::write_ipc_batch(&self.executor, path.clone(), batch).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    if let Err(cleanup_error) = tokio::fs::remove_file(&path).await {
                        debug!(
                            path = %path.display(),
                            error = %cleanup_error,
                            "failed to remove incomplete Iceberg staged batch"
                        );
                    }
                    for file in new_files {
                        let file: IcebergStagedBatch = file;
                        if let Err(cleanup_error) = tokio::fs::remove_file(&file.path).await {
                            debug!(
                                path = %file.path.display(),
                                error = %cleanup_error,
                                "failed to remove staged batch after Iceberg publish error"
                            );
                        }
                    }
                    return Err(error);
                }
            };
            new_files.push(IcebergStagedBatch {
                path,
                rows,
                bytes,
                acknowledgements: None,
                domain_timestamp: carrier.occurred_at,
            });
            staged_rows = staged_rows
                .checked_add(rows)
                .assured("the accepted ranges are disjoint rows of one mapped batch");
            staged_bytes = staged_bytes
                .checked_add(bytes)
                .assured("staged bytes fit the node's u64 byte count");
        }
        if let Some(last) = new_files.last_mut() {
            last.acknowledgements = carrier.acknowledgements;
        }
        self.staged_batches.extend(new_files);
        self.staged_rows = self
            .staged_rows
            .checked_add(staged_rows)
            .assured("both counts total rows this emitter already staged on disk");
        self.staged_bytes = self
            .staged_bytes
            .checked_add(staged_bytes)
            .assured("both counts total bytes this emitter already staged on disk");
        for (member, row) in carrier.selected_rows.iter().enumerate() {
            if !rejected_rows[member] {
                outcome.deliver(EmitterRecordPosition {
                    batch_index: carrier.batch_index,
                    row_index: *row,
                });
            }
        }
        trace!(
            rows = staged_rows,
            bytes = staged_bytes,
            "emitter staged iceberg rows"
        );
        Ok(())
    }

    /// Publishes the staged files the commit in progress covers, beginning a commit of every
    /// staged file when none is in progress, and reports what they held.
    async fn commit_covered(&mut self) -> EmitterWriteResult<EmitterCommitReport> {
        let staged_files = self.staged_batches.len();
        let capacity = self.executor.memory(MemoryClass::Bulk).capacity_bytes;
        let preparation = self
            .commit_state
            .preparation
            .get_or_insert_with(|| IcebergCommitPreparation::new(staged_files));
        while !preparation.is_written() {
            nervix_primitives::task::consume_budget().await;
            let unwritten = self
                .staged_batches
                .get(preparation.unwritten())
                .assured("a commit covers files that stay staged until it publishes them");
            let group = IcebergLoadGroup::leading(unwritten, capacity);
            let mut paths = Vec::with_capacity(group.files);
            for staged in unwritten.iter().take(group.files) {
                paths.push(staged.path.clone());
            }
            let IcebergLoadedGroup { batches, charge } = Self::load_group(
                &self.executor,
                StdArc::clone(&self.staged_schema),
                paths,
                group.charge,
            )
            .await?;
            let data_files = self
                .client
                .write_data_files(&self.staged_schema, batches, self.batch_policy)
                .await?;
            // The rows the charge covered are in data files now.
            drop(charge);
            preparation.record_written(group.files, data_files);
        }
        self.client.commit_prepared(preparation).await?;
        let covered = preparation.covered;
        self.commit_state.preparation = None;
        Ok(self.publish_committed(covered).await)
    }

    /// Removes the `covered` leading staged files a commit appended, resolves the acknowledgements
    /// they retained, and reports what they held.
    async fn publish_committed(&mut self, covered: usize) -> EmitterCommitReport {
        let committed = self.staged_batches.drain(..covered).collect::<Vec<_>>();
        let mut messages = 0_u64;
        let mut bytes = 0_u64;
        for batch in &committed {
            messages = messages
                .checked_add(batch.rows)
                .assured("the total counts rows this emitter already staged on disk");
            bytes = bytes
                .checked_add(batch.bytes)
                .assured("the total counts bytes this emitter already staged on disk");
        }
        let domain_timestamp = committed
            .iter()
            .map(|batch| batch.domain_timestamp)
            .max()
            .assured("a commit begins only over staged files and covers at least one");
        self.staged_rows = self
            .staged_rows
            .checked_sub(messages)
            .assured("the staged total counts the rows of every staged file");
        self.staged_bytes = self
            .staged_bytes
            .checked_sub(bytes)
            .assured("the staged total counts the bytes of every staged file");
        let mut acknowledgements = Vec::with_capacity(committed.len());
        for batch in committed {
            nervix_primitives::task::consume_budget().await;
            acknowledgements.extend(batch.acknowledgements);
            if let Err(error) = tokio::fs::remove_file(&batch.path).await {
                debug!(
                    path = %batch.path.display(),
                    error = %error,
                    "failed to remove committed Iceberg staged batch"
                );
            }
        }
        for acknowledgements in acknowledgements {
            acknowledgements.acknowledge();
        }
        EmitterCommitReport {
            messages,
            bytes,
            domain_timestamp,
        }
    }
}

impl IcebergTableEmitterClient {
    /// Installs catalog metadata while retaining the emitter's initialized storage. Catalog loads
    /// supply a fresh lazy FileIO; replacing ours would initialize another operator on each commit.
    fn install_table(&mut self, table: Table) -> EmitterWriteResult<()> {
        let mut builder = Table::builder()
            .file_io(self.table.file_io().clone())
            .metadata(table.metadata_ref())
            .identifier(table.identifier().clone())
            .readonly(table.readonly())
            .runtime(
                ::iceberg::Runtime::try_current()
                    .change_context(EmitterWriteError::Commit { connector: ICEBERG })?,
            );
        if let Some(location) = table.metadata_location() {
            builder = builder.metadata_location(location);
        }
        self.table = builder
            .build()
            .change_context(EmitterWriteError::Commit { connector: ICEBERG })?;
        Ok(())
    }

    /// The table this commit attempts against, with the catalog library's own retry disabled so
    /// the emitter's declared retry policy is the only one that runs.
    fn single_attempt_table(table: &Table) -> EmitterWriteResult<Table> {
        let commit_failure = |error: &dyn std::fmt::Display| {
            Report::new(EmitterWriteError::Commit { connector: ICEBERG })
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

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the external Iceberg catalog owns table loading and its request effects"
        )
    )]
    async fn refresh_table(&mut self) -> EmitterWriteResult<()> {
        let table_ident = self.table.identifier().clone();
        let table = self
            .catalog
            .load_table(&table_ident)
            .await
            .map_err(|error| {
                Report::new(EmitterWriteError::Commit { connector: ICEBERG }).attach_printable(
                    format!("failed to load Iceberg table {table_ident} for commit: {error}"),
                )
            })?;
        self.install_table(table)
    }

    /// Writes `batches`, which carry the columns of `staged`, into new Parquet data files of the
    /// table, without committing them. Each batch is laid out as the table's current columns first,
    /// each mapped column in the column of its name, so a table changed since the sink opened is
    /// written as it is now or refused, naming the column it cannot take.
    async fn write_data_files(
        &mut self,
        staged: &arrow_schema::SchemaRef,
        batches: Vec<RecordBatch>,
        batch_policy: Option<EmitterBatchPolicy>,
    ) -> EmitterWriteResult<Vec<DataFile>> {
        let commit_failure = |error: &dyn std::fmt::Display| {
            Report::new(EmitterWriteError::Commit { connector: ICEBERG })
                .attach_printable(format!("failed to commit Iceberg staged batches: {error}"))
        };
        self.refresh_table().await?;
        let projection =
            IcebergTableProjection::plan(staged, self.table.metadata().current_schema())
                .map_err(IcebergColumnError::commit_failure)?;
        if let Some(policy) = batch_policy {
            let mut written = Vec::with_capacity(batches.len());
            for batch in batches {
                nervix_primitives::task::consume_budget().await;
                let projected = projection
                    .project(&batch)
                    .map_err(IcebergColumnError::commit_failure)?;
                if projected.num_rows() == 0 {
                    continue;
                }
                let max_rows: usize = policy.max_messages.get().get().arch_into();
                if projected.num_rows() > max_rows {
                    return Err(commit_failure(&format!(
                        "Iceberg staged batch has {} rows, above BATCH MAX MESSAGES {max_rows}",
                        projected.num_rows()
                    )));
                }
                let location_generator = DefaultLocationGenerator::new(self.table.metadata())
                    .map_err(|error| commit_failure(&error))?;
                self.data_file_sequence = self
                    .data_file_sequence
                    .checked_add(1)
                    .assured("an emitter cannot commit 2^64 data files in one node lifetime");
                let file_name_generator = DefaultFileNameGenerator::new(
                    format!("{}-{}", self.file_name_prefix, self.data_file_sequence),
                    None,
                    DataFileFormat::Parquet,
                );
                let parquet_writer = ParquetWriterBuilder::new(
                    WriterProperties::builder().build(),
                    self.table.metadata().current_schema().clone(),
                );
                // Every staged BATCH segment is one file. The rolling writer's own size check
                // runs before a write and could overshoot; the measured segment and the returned
                // physical file size enforce the hard bound instead.
                let rolling_writer = RollingFileWriterBuilder::new(
                    parquet_writer,
                    usize::MAX,
                    self.table.file_io().clone(),
                    location_generator,
                    file_name_generator,
                );
                let mut writer = DataFileWriterBuilder::new(rolling_writer)
                    .build(None)
                    .await
                    .map_err(|error| commit_failure(&error))?;
                writer
                    .write(projected)
                    .await
                    .map_err(|error| commit_failure(&error))?;
                let files = writer
                    .close()
                    .await
                    .map_err(|error| commit_failure(&error))?;
                if files.len() != 1 {
                    return Err(commit_failure(&format!(
                        "Iceberg wrote {} data files for one staged BATCH segment",
                        files.len()
                    )));
                }
                let file = files
                    .into_iter()
                    .next()
                    .assured("exactly one data file was returned");
                let max_rows_u64: u64 = max_rows.arch_into();
                if file.record_count() > max_rows_u64
                    || file.file_size_in_bytes() > policy.max_size.bytes().get()
                {
                    let reason = format!(
                        "Iceberg data file has {} rows and {} bytes, above BATCH MAX MESSAGES \
                         {max_rows} or MAX SIZE {}",
                        file.record_count(),
                        file.file_size_in_bytes(),
                        policy.max_size
                    );
                    if let Err(cleanup_error) = self.table.file_io().delete(file.file_path()).await
                    {
                        warn!(
                            path = file.file_path(),
                            error = %cleanup_error,
                            "failed to remove Iceberg data file rejected by BATCH bounds"
                        );
                    }
                    return Err(commit_failure(&reason));
                }
                written.push(file);
            }
            return Ok(written);
        }
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
        let parquet_writer = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            self.table.metadata().current_schema().clone(),
        );
        let default_file_target =
            usize::try_from(nervix_models::IcebergTableEmitter::DEFAULT_DATA_FILE_TARGET_BYTES)
                .assured("Nervix builds for 64-bit targets only");
        let rolling_writer = RollingFileWriterBuilder::new(
            parquet_writer,
            default_file_target,
            self.table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let mut writer = DataFileWriterBuilder::new(rolling_writer)
            .build(None)
            .await
            .map_err(|error| commit_failure(&error))?;
        for batch in batches {
            nervix_primitives::task::consume_budget().await;
            let projected = projection
                .project(&batch)
                .map_err(IcebergColumnError::commit_failure)?;
            writer
                .write(projected)
                .await
                .map_err(|error| commit_failure(&error))?;
        }
        writer.close().await.map_err(|error| commit_failure(&error))
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the external Iceberg catalog owns commit and request effects")
    )]
    async fn commit_prepared(
        &mut self,
        prepared: &IcebergCommitPreparation,
    ) -> EmitterWriteResult<()> {
        let commit_failure = |error: &dyn std::fmt::Display| {
            Report::new(EmitterWriteError::Commit { connector: ICEBERG })
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
                self.install_table(table)?;
                Ok(())
            }
            Err(error) => {
                let commit_error = commit_failure(&error);
                let table_ident = self.table.identifier().clone();
                if let Ok(refreshed) = self.catalog.load_table(&table_ident).await {
                    let committed = prepared.is_committed_to(&refreshed);
                    self.install_table(refreshed)?;
                    if committed {
                        return Ok(());
                    }
                }
                Err(commit_error)
            }
        }
    }
}

impl IcebergCatalogConnection<'_> {
    /// Builds the catalog's HTTP client over the node resolver and its TLS configuration, and
    /// loads the catalog with the paired properties and the storage factory.
    async fn open(self, dns: &DnsResolver) -> IcebergResult<RestCatalog> {
        let client = reqwest_iceberg::Client::builder()
            .dns_resolver(StdArc::new(dns.clone()))
            .use_preconfigured_tls(self.tls.clone())
            .build()
            .map_err(|error| {
                IcebergError::new(
                    IcebergErrorKind::Unexpected,
                    format!("failed to build the Iceberg REST client: {error}"),
                )
            })?;
        let properties = self
            .properties
            .iter()
            .map(|(property, value)| (property.to_string(), value.to_string()));
        RestCatalogBuilder::default()
            .with_client(client)
            .with_storage_factory(StdArc::new(self.storage))
            .load(self.name, properties.collect())
            .await
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use ::iceberg::{
        arrow::arrow_schema_to_schema_auto_assign_ids,
        io::{
            ADLS_ACCOUNT_KEY, ADLS_ACCOUNT_NAME, ADLS_AUTHORITY_HOST, ADLS_CLIENT_ID,
            ADLS_CLIENT_SECRET, ADLS_SAS_TOKEN, ADLS_TENANT_ID, CLIENT_REGION, FileIO,
            GCS_ALLOW_ANONYMOUS, GCS_CREDENTIALS_JSON, GCS_DISABLE_CONFIG_LOAD,
            GCS_DISABLE_VM_METADATA, GCS_NO_AUTH, GCS_SERVICE_PATH, GCS_TOKEN, S3_ACCESS_KEY_ID,
            S3_ALLOW_ANONYMOUS, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_ENDPOINT,
            S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN,
        },
        spec::{
            DataContentType, DataFileBuilder, FormatVersion, Operation, PartitionSpec, Schema,
            SortOrder, Struct, Summary, TableMetadataBuilder,
        },
    };
    use arrow_array::builder::{
        FixedSizeListBuilder, LargeListBuilder, ListBuilder, MapBuilder, StringBuilder,
        TimestampNanosecondBuilder,
    };
    use arrow_schema::Field;
    use nervix_dns::{DnsConfiguration, NameServers};
    use nervix_primitives::sync::atomic::{AtomicUsize, Ordering};
    use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    use super::*;

    #[test]
    fn the_vocabulary_names_the_storage_properties_the_iceberg_crate_reads() {
        for (backend, key, property) in [
            (IcebergStorageBackend::S3, "access_key_id", S3_ACCESS_KEY_ID),
            (
                IcebergStorageBackend::S3,
                "allow_anonymous",
                S3_ALLOW_ANONYMOUS,
            ),
            (IcebergStorageBackend::S3, "client_region", CLIENT_REGION),
            (
                IcebergStorageBackend::S3,
                "disable_config_load",
                S3_DISABLE_CONFIG_LOAD,
            ),
            (
                IcebergStorageBackend::S3,
                "disable_ec2_metadata",
                S3_DISABLE_EC2_METADATA,
            ),
            (IcebergStorageBackend::S3, "endpoint", S3_ENDPOINT),
            (
                IcebergStorageBackend::S3,
                "path_style_access",
                S3_PATH_STYLE_ACCESS,
            ),
            (IcebergStorageBackend::S3, "region", S3_REGION),
            (
                IcebergStorageBackend::S3,
                "secret_access_key",
                S3_SECRET_ACCESS_KEY,
            ),
            (IcebergStorageBackend::S3, "session_token", S3_SESSION_TOKEN),
            (
                IcebergStorageBackend::Gcs,
                "allow_anonymous",
                GCS_ALLOW_ANONYMOUS,
            ),
            (
                IcebergStorageBackend::Gcs,
                "credentials_json",
                GCS_CREDENTIALS_JSON,
            ),
            (
                IcebergStorageBackend::Gcs,
                "disable_config_load",
                GCS_DISABLE_CONFIG_LOAD,
            ),
            (
                IcebergStorageBackend::Gcs,
                "disable_vm_metadata",
                GCS_DISABLE_VM_METADATA,
            ),
            (IcebergStorageBackend::Gcs, "no_auth", GCS_NO_AUTH),
            (IcebergStorageBackend::Gcs, "service_path", GCS_SERVICE_PATH),
            (IcebergStorageBackend::Gcs, "token", GCS_TOKEN),
            (
                IcebergStorageBackend::AzureBlob,
                "account_key",
                ADLS_ACCOUNT_KEY,
            ),
            (
                IcebergStorageBackend::AzureBlob,
                "account_name",
                ADLS_ACCOUNT_NAME,
            ),
            (
                IcebergStorageBackend::AzureBlob,
                "authority_host",
                ADLS_AUTHORITY_HOST,
            ),
            (
                IcebergStorageBackend::AzureBlob,
                "client_id",
                ADLS_CLIENT_ID,
            ),
            (
                IcebergStorageBackend::AzureBlob,
                "client_secret",
                ADLS_CLIENT_SECRET,
            ),
            (
                IcebergStorageBackend::AzureBlob,
                "sas_token",
                ADLS_SAS_TOKEN,
            ),
            (
                IcebergStorageBackend::AzureBlob,
                "tenant_id",
                ADLS_TENANT_ID,
            ),
        ] {
            assert_eq!(backend.file_io_property(key), Some(property), "{key}");
        }
    }

    #[test]
    fn a_client_tls_file_that_cannot_be_read_stops_the_emitter_naming_its_client() {
        let missing = tempfile::tempdir()
            .assured("the fixture can create a directory")
            .path()
            .join("missing-ca.pem");
        let config = [ClientConfigEntry {
            key: "tls_ca_file".to_string(),
            value: missing.display().to_string(),
        }];
        for client in [
            IcebergClientTlsError::Catalog,
            IcebergClientTlsError::ObjectStore,
        ] {
            let refusal = client
                .client_config(&config)
                .expect_err("an unreadable CA file cannot configure a client");
            assert!(matches!(
                refusal.current_context(),
                EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
            ));
            assert_eq!(
                refusal.downcast_ref::<IcebergClientTlsError>(),
                Some(&client)
            );
            assert!(matches!(
                refusal.downcast_ref::<nervix_connector::TlsClientConfigError>(),
                Some(nervix_connector::TlsClientConfigError::ReadCaCertificate { path })
                    if *path == missing
            ));
        }
        IcebergClientTlsError::Catalog
            .client_config(&[])
            .assured("a client without TLS files trusts the bundled and platform roots");
    }

    #[test]
    fn azure_locations_use_the_storage_service_named_by_their_scheme() {
        for (scheme, service) in [
            ("wasb", "blob"),
            ("wasbs", "blob"),
            ("abfs", "dfs"),
            ("abfss", "dfs"),
        ] {
            let location =
                format!("{scheme}://container@account.{service}.core.windows.net/events");
            let url = IcebergTableEmitter::validate_blob_location(
                IcebergStorageBackend::AzureBlob,
                "table",
                &location,
            )
            .assured("the scheme and host identify the same Azure storage service");
            assert_eq!(url.as_str(), location);
        }
    }

    #[nervix_primitives::test]
    async fn iceberg_catalog_authentication_uses_the_node_dns_client() {
        const CATALOG: &str = "catalog.nervix.test";
        const AUTH: &str = "auth.nervix.test";
        let authority = DnsAuthority::start_on_loopback()
            .await
            .assured("the fixture can bind a loopback DNS port");
        for name in [CATALOG, AUTH] {
            authority.set(
                name,
                DnsAnswer::Addresses {
                    addresses: vec![std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)],
                    ttl: Duration::from_secs(1),
                },
            );
        }
        let files = tempfile::tempdir().assured("the fixture can create a directory");
        let resolver_configuration = files.path().join("resolv.conf");
        let hosts_file = files.path().join("hosts");
        std::fs::write(
            &resolver_configuration,
            "search nervix.test\noptions ndots:1 timeout:1 attempts:1\n",
        )
        .assured("the fixture resolver configuration can be written");
        std::fs::write(&hosts_file, "").assured("the fixture hosts file can be written");
        let dns = DnsResolver::load(DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: NameServers::Explicit(vec![authority.address()]),
        })
        .await
        .assured("the fixture resolver configuration is valid");

        let auth_listener =
            nervix_primitives::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .assured("the OAuth endpoint can bind");
        let auth_port = auth_listener
            .local_addr()
            .assured("the OAuth endpoint has an address")
            .port();
        let auth_server = nervix_primitives::task::spawn(async move {
            let (mut stream, _) = auth_listener
                .accept()
                .await
                .assured("the OAuth request connects");
            let mut bytes = [0_u8; 4096];
            let length = stream
                .read(&mut bytes)
                .await
                .assured("the OAuth request is readable");
            let request = String::from_utf8_lossy(&bytes[..length]).to_ascii_lowercase();
            assert!(request.starts_with("post /token http/1.1"), "{request}");
            assert!(
                request.contains(&format!("host: {AUTH}:{auth_port}")),
                "{request}"
            );
            let body = r#"{"access_token":"fixture-token","token_type":"Bearer"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .assured("the OAuth response can be written");
        });

        let catalog_listener =
            nervix_primitives::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .assured("the catalog endpoint can bind");
        let catalog_port = catalog_listener
            .local_addr()
            .assured("the catalog endpoint has an address")
            .port();
        let catalog_server = nervix_primitives::task::spawn(async move {
            let (mut stream, _) = catalog_listener
                .accept()
                .await
                .assured("the catalog request connects");
            let mut bytes = [0_u8; 4096];
            let length = stream
                .read(&mut bytes)
                .await
                .assured("the catalog request is readable");
            let request = String::from_utf8_lossy(&bytes[..length]).to_ascii_lowercase();
            assert!(request.starts_with("get /v1/config http/1.1"), "{request}");
            assert!(
                request.contains(&format!("host: {CATALOG}:{catalog_port}")),
                "{request}"
            );
            assert!(
                request.contains("authorization: bearer fixture-token"),
                "{request}"
            );
            let body = r#"{"overrides":{},"defaults":{}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .assured("the catalog response can be written");
        });

        let catalog_config = [
            ClientConfigEntry {
                key: "uri".to_string(),
                value: format!("http://{CATALOG}:{catalog_port}"),
            },
            ClientConfigEntry {
                key: "credential".to_string(),
                value: "client:secret".to_string(),
            },
            ClientConfigEntry {
                key: "oauth2-server-uri".to_string(),
                value: format!("http://{AUTH}:{auth_port}/token"),
            },
        ];
        let tls = IcebergClientTlsError::Catalog
            .client_config(&catalog_config)
            .assured("a catalog client without TLS files trusts the default roots");
        let storage_root =
            Url::parse("s3://bucket/table").assured("the fixture storage root is a URL");
        let storage =
            DnsStorageFactory::new(IcebergStorageBackend::S3, &dns, &storage_root, None, &tls)
                .assured("the object-store client can be built without a request");
        let catalog = IcebergCatalogConnection {
            name: "fixture",
            properties: IcebergCatalogProperties::pair(
                IcebergStorageBackend::S3,
                &[],
                &catalog_config,
            )
            .assured("a catalog paired with an empty storage client sets no shared property"),
            tls: &tls,
            storage,
        }
        .open(&dns)
        .await
        .assured("the REST catalog can be configured");
        nervix_primitives::time::timeout(Duration::from_secs(10), catalog.invalidate_token())
            .await
            .assured("the catalog initialization completes")
            .assured("the catalog accepts its runtime configuration");
        assert!(authority.questions_for(CATALOG) > 0);
        assert!(authority.questions_for(AUTH) > 0);
        auth_server.await.assured("the OAuth server task completes");
        catalog_server
            .await
            .assured("the catalog server task completes");
    }

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

    /// One acknowledgement handover the emitter holds, recording into the shared counters.
    struct RecordedHandover(StdArc<RecordedAcknowledgements>);

    impl EmitterRecordAcknowledgementServices for RecordedHandover {
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

    #[nervix_primitives::test]
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

        let single_attempt = IcebergTableEmitterClient::single_attempt_table(&table)
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

    #[nervix_primitives::test]
    async fn installing_catalog_metadata_retains_the_initialized_storage() {
        const PATH: &str = "memory://warehouse/table/data.parquet";
        #[expect(
            clippy::disallowed_types,
            reason = "Iceberg TableMetadataBuilder requires a standard-library property map at \
                      its external API boundary"
        )]
        let metadata = TableMetadataBuilder::new(
            Schema::builder()
                .build()
                .assured("an empty table schema is valid"),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/table".to_string(),
            FormatVersion::V2,
            std::collections::HashMap::default(),
        )
        .assured("the table metadata has every required field")
        .build()
        .assured("the table metadata is valid")
        .metadata;
        let table = Table::builder()
            .metadata(metadata)
            .identifier(
                TableIdent::from_strs(["test", "table"])
                    .assured("the table has a namespace and name"),
            )
            .metadata_location("memory://warehouse/table/metadata/initial.json")
            .file_io(FileIO::new_with_memory())
            .runtime(::iceberg::Runtime::try_current().assured("the async test runs inside Tokio"))
            .build()
            .assured("the in-memory table has every required field");
        table
            .file_io()
            .new_output(PATH)
            .assured("the object URL uses memory storage")
            .write(bytes::Bytes::from_static(b"retained object"))
            .await
            .assured("the fixture can write its object");
        let refreshed = Table::builder()
            .metadata(table.metadata_ref())
            .identifier(table.identifier().clone())
            .metadata_location("memory://warehouse/table/metadata/refreshed.json")
            .readonly(true)
            .file_io(FileIO::new_with_memory())
            .runtime(::iceberg::Runtime::try_current().assured("the async test runs inside Tokio"))
            .build()
            .assured("the refreshed table has every required field");
        #[expect(
            clippy::disallowed_types,
            reason = "Iceberg CatalogBuilder requires a standard-library property map at its \
                      external API boundary"
        )]
        let catalog = ::iceberg::memory::MemoryCatalogBuilder::default()
            .load(
                "fixture",
                std::collections::HashMap::from_iter([(
                    "warehouse".to_string(),
                    "memory://warehouse".to_string(),
                )]),
            )
            .await
            .assured("the fixture catalog has a warehouse");
        let mut client = IcebergTableEmitterClient {
            catalog: StdArc::new(catalog),
            table,
            file_name_prefix: "fixture".to_string(),
            data_file_sequence: 0,
        };
        client
            .install_table(refreshed)
            .assured("catalog metadata can be installed");
        let content = client
            .table
            .file_io()
            .new_input(PATH)
            .assured("the retained object URL uses memory storage")
            .read()
            .await
            .assured("the installed table retains its initialized object storage");
        assert_eq!(content.as_ref(), b"retained object");
        assert_eq!(
            client.table.metadata_location(),
            Some("memory://warehouse/table/metadata/refreshed.json")
        );
        assert!(client.table.readonly());
    }

    #[test]
    fn a_preparation_keeps_its_data_files_and_append_identity_until_every_covered_file_is_written()
    {
        let append_id = uuid::Uuid::now_v7();
        let data_file = |path: &str| {
            DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(128)
                .record_count(2)
                .partition_spec_id(0)
                .partition(Struct::empty())
                .build()
                .expect("valid prepared Iceberg data file")
        };
        let mut preparation = IcebergCommitPreparation::with_append_id(3, append_id);
        assert_eq!(preparation.unwritten(), 0..3);
        assert!(!preparation.is_written());

        preparation.record_written(2, vec![data_file("s3://bucket/table/data/first.parquet")]);
        assert_eq!(preparation.unwritten(), 2..3);
        assert!(!preparation.is_written());
        preparation.record_written(1, vec![data_file("s3://bucket/table/data/second.parquet")]);

        assert!(preparation.is_written());
        assert_eq!(preparation.append_id(), append_id);
        let paths = preparation
            .data_files()
            .iter()
            .map(DataFile::file_path)
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            [
                "s3://bucket/table/data/first.parquet",
                "s3://bucket/table/data/second.parquet"
            ]
        );
    }

    #[test]
    fn what_commits_published_before_a_failed_attempt_is_reported_with_the_next_success() {
        let mut state = IcebergCommitState::default();
        state.record_published(EmitterCommitReport {
            messages: 2,
            bytes: 100,
            domain_timestamp: Timestamp::from_unix_nanos(7),
        });
        state.record_published(EmitterCommitReport {
            messages: 1,
            bytes: 40,
            domain_timestamp: Timestamp::from_unix_nanos(5),
        });

        assert_eq!(
            state.unreported,
            Some(EmitterCommitReport {
                messages: 3,
                bytes: 140,
                domain_timestamp: Timestamp::from_unix_nanos(7),
            })
        );
    }

    #[test]
    fn load_groups_take_the_leading_staged_files_the_class_holds_together() {
        let staged = |bytes: u64| IcebergStagedBatch {
            path: PathBuf::from("batch.arrow"),
            rows: 1,
            bytes,
            acknowledgements: None,
            domain_timestamp: Timestamp::from_unix_nanos(1),
        };
        let files = [staged(400), staged(500), staged(300), staged(900)];

        assert_eq!(
            IcebergLoadGroup::leading(&files, 2_100),
            IcebergLoadGroup {
                files: 4,
                charge: IcebergLoadCharge::Staged { bytes: 2_100 },
            }
        );
        assert_eq!(
            IcebergLoadGroup::leading(&files, 1_000),
            IcebergLoadGroup {
                files: 2,
                charge: IcebergLoadCharge::Staged { bytes: 900 },
            }
        );
        assert_eq!(
            IcebergLoadGroup::leading(&files[2..], 1_000),
            IcebergLoadGroup {
                files: 1,
                charge: IcebergLoadCharge::Staged { bytes: 300 },
            }
        );
        // A first file larger than the whole class is a group of its own, charged the whole class.
        let oversized = IcebergLoadGroup::leading(&files[3..], 800);
        assert_eq!(
            oversized,
            IcebergLoadGroup {
                files: 1,
                charge: IcebergLoadCharge::WholeClass {
                    file_bytes: 900,
                    capacity: 800,
                },
            }
        );
        assert_eq!(oversized.charge.reserved_bytes(), 800);
        // A file larger than the whole class after a smaller one is left for a group of its own.
        assert_eq!(
            IcebergLoadGroup::leading(&files[2..], 800),
            IcebergLoadGroup {
                files: 1,
                charge: IcebergLoadCharge::Staged { bytes: 300 },
            }
        );
    }

    #[test]
    fn iceberg_append_marker_identifies_an_ambiguously_successful_commit() {
        let append_id = uuid::Uuid::now_v7();
        let prepared = IcebergCommitPreparation::with_append_id(0, append_id);
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
            !IcebergCommitPreparation::with_append_id(0, uuid::Uuid::now_v7())
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

        let staged = IcebergTableEmitter::staged_arrow_schema(&mapped)
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

        let error = IcebergTableEmitter::staged_arrow_schema(&mapped)
            .expect_err("a column written twice has no single staged value");

        assert_eq!(
            *error.current_context(),
            EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
        );
    }

    #[test]
    fn iceberg_open_checks_mapped_column_names_and_types() {
        let table = arrow_schema_to_schema_auto_assign_ids(&arrow_schema::Schema::new(vec![
            Field::new(
                "observed",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                true,
            ),
            Field::new("user_id", DataType::Int64, true),
        ]))
        .assured("table schema converts to Iceberg");
        let compatible = arrow_schema::Schema::new(vec![
            Field::new("user_id", DataType::Int64, true),
            Field::new(
                "observed",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
                true,
            ),
        ]);
        IcebergTableEmitter::validate_table_schema(&compatible, &table)
            .assured("column order and field IDs do not change the schema contract");

        let wrong_type =
            arrow_schema::Schema::new(vec![Field::new("user_id", DataType::Utf8, true)]);
        let error = IcebergTableEmitter::validate_table_schema(&wrong_type, &table)
            .expect_err("a mapped type the table cannot write fails at open");
        assert_eq!(
            *error.current_context(),
            EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
        );
        assert!(format!("{error:?}").contains(
            "mapped column 'user_id' has Iceberg type string, but the table requires long"
        ));

        let missing = arrow_schema::Schema::new(vec![Field::new("unknown", DataType::Int64, true)]);
        let error = IcebergTableEmitter::validate_table_schema(&missing, &table)
            .expect_err("a column absent from the table fails at open");
        assert_eq!(
            *error.current_context(),
            EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
        );
    }

    #[test]
    fn iceberg_open_checks_nested_column_names_and_types() {
        let datetime = DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()));
        let list = |element| DataType::List(StdArc::new(Field::new("item", element, true)));
        let structure = |fields: Vec<Field>| DataType::Struct(fields.into());
        let map = |key, value| {
            let entries = structure(vec![
                Field::new("key", key, false),
                Field::new("value", value, true),
            ]);
            DataType::Map(StdArc::new(Field::new("entries", entries, false)), false)
        };
        let observed = Field::new("observed", list(datetime.clone()), true);
        let dimension = Field::new(
            "dimension",
            structure(vec![
                Field::new("tag", DataType::Utf8, true),
                Field::new("score", DataType::Int64, true),
            ]),
            true,
        );
        let labels = Field::new("labels", map(DataType::Utf8, datetime.clone()), true);
        let table = arrow_schema_to_schema_auto_assign_ids(&arrow_schema::Schema::new(vec![
            observed.clone(),
            dimension,
            labels.clone(),
        ]))
        .assured("the fixture uses Iceberg-supported list, struct, map and UTC timestamp types");
        let reordered = arrow_schema::Schema::new(vec![
            labels,
            Field::new(
                "dimension",
                structure(vec![
                    Field::new("score", DataType::Int64, true),
                    Field::new("tag", DataType::Utf8, true),
                ]),
                true,
            ),
            observed,
        ]);
        IcebergTableEmitter::validate_table_schema(&reordered, &table)
            .assured("matching nested names and types are independent of generated field IDs");

        let mismatches = [
            Field::new("observed", list(DataType::Int64), true),
            Field::new("observed", DataType::Utf8, true),
            Field::new(
                "dimension",
                structure(vec![Field::new("tag", DataType::Utf8, true)]),
                true,
            ),
            Field::new(
                "dimension",
                structure(vec![
                    Field::new("category", DataType::Utf8, true),
                    Field::new("score", DataType::Int64, true),
                ]),
                true,
            ),
            Field::new(
                "dimension",
                structure(vec![
                    Field::new("tag", DataType::Utf8, true),
                    Field::new("score", DataType::Utf8, true),
                ]),
                true,
            ),
            Field::new("labels", map(DataType::Int64, datetime), true),
            Field::new("labels", map(DataType::Utf8, DataType::Int64), true),
        ];
        for field in mismatches {
            let name = field.name().clone();
            let mapped = arrow_schema::Schema::new(vec![field]);
            let error = IcebergTableEmitter::validate_table_schema(&mapped, &table)
                .expect_err("an incompatible nested column must fail before staging");
            assert_eq!(
                *error.current_context(),
                EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
            );
            assert!(format!("{error:?}").contains(&format!("mapped column '{name}'")));
        }
    }

    #[test]
    fn iceberg_open_retains_the_schema_conversion_cause() {
        let table =
            arrow_schema_to_schema_auto_assign_ids(&arrow_schema::Schema::new(vec![Field::new(
                "user_id",
                DataType::Int64,
                true,
            )]))
            .assured("table schema converts to Iceberg");
        let unsupported =
            arrow_schema::Schema::new(vec![Field::new("user_id", DataType::Null, true)]);

        let error = IcebergTableEmitter::validate_table_schema(&unsupported, &table)
            .expect_err("an unsupported mapped type fails at open");

        assert_eq!(
            *error.current_context(),
            EmitterOpenError::InvalidConfiguration { connector: ICEBERG }
        );
        assert!(error.contains::<::iceberg::Error>());
    }

    #[test]
    fn iceberg_datetime_columns_are_staged_as_microseconds() {
        let values: ArrayRef = StdArc::new(
            TimestampNanosecondArray::from(vec![Some(1_234_567), None, Some(-1)])
                .with_timezone_utc(),
        );

        let staged = IcebergTableEmitter::staged_column(
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
    fn iceberg_struct_datetime_is_staged_as_microseconds() {
        let field = StdArc::new(Field::new(
            "observed",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            true,
        ));
        let values: ArrayRef = StdArc::new(
            TimestampNanosecondArray::from(vec![Some(-1), None, Some(1_234_567)])
                .with_timezone_utc(),
        );
        let source: ArrayRef = StdArc::new(
            StructArray::try_new(vec![field].into(), vec![values], None)
                .assured("source struct is valid"),
        );
        let mapped =
            arrow_schema::Schema::new(vec![Field::new("event", source.data_type().clone(), true)]);
        let staged_schema = IcebergTableEmitter::staged_arrow_schema(&mapped)
            .assured("datetime in a struct narrows in staged schema");
        let staged =
            IcebergTableEmitter::staged_column(&source, staged_schema.field(0).data_type())
                .assured("datetime in a struct narrows in staged values");
        assert_eq!(staged.data_type(), staged_schema.field(0).data_type());
        let observed = staged
            .as_any()
            .downcast_ref::<StructArray>()
            .assured("struct is retained")
            .column(0);
        let observed = observed
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .assured("struct member is a microsecond datetime");
        assert_eq!(observed.value(0), -1);
        assert!(observed.is_null(1));
        assert_eq!(observed.value(2), 1_234);
    }

    #[test]
    fn iceberg_nested_container_shapes_keep_values_offsets_and_nulls() {
        let timestamp_field = StdArc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            true,
        ));
        let stage = |source: &ArrayRef| {
            let mapped = arrow_schema::Schema::new(vec![Field::new(
                "observed",
                source.data_type().clone(),
                true,
            )]);
            let schema = IcebergTableEmitter::staged_arrow_schema(&mapped)
                .assured("nested datetime schema stages as microseconds");
            let staged = IcebergTableEmitter::staged_column(source, schema.field(0).data_type())
                .assured("nested datetime values stage as microseconds");
            assert_eq!(staged.data_type(), schema.field(0).data_type());
            staged
        };

        let mut large =
            LargeListBuilder::new(TimestampNanosecondBuilder::new().with_timezone("+00:00"))
                .with_field(timestamp_field.clone());
        large.values().append_value(-1);
        large.values().append_value(1_999);
        large.append(true);
        large.append(false);
        let source: ArrayRef = StdArc::new(large.finish());
        let staged = stage(&source);
        let large = staged
            .as_any()
            .downcast_ref::<LargeListArray>()
            .assured("large list remains a large list");
        assert_eq!(large.value_offsets(), &[0, 2, 2]);
        assert!(large.is_null(1));
        let values = large
            .values()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .assured("large list members are microsecond datetimes");
        assert_eq!(values.values().as_ref(), &[-1, 1]);

        let mut fixed =
            FixedSizeListBuilder::new(TimestampNanosecondBuilder::new().with_timezone("+00:00"), 2)
                .with_field(timestamp_field);
        fixed.values().append_value(1_999);
        fixed.values().append_null();
        fixed.append(true);
        fixed.values().append_value(0);
        fixed.values().append_value(0);
        fixed.append(false);
        let source: ArrayRef = StdArc::new(fixed.finish());
        let staged = stage(&source);
        let fixed = staged
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .assured("fixed list remains a fixed list");
        assert_eq!(fixed.value_length(), 2);
        assert!(fixed.is_null(1));
        let values = fixed
            .values()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .assured("fixed list members are microsecond datetimes");
        assert_eq!(values.value(0), 1);
        assert!(values.is_null(1));

        let mut map = MapBuilder::new(
            None,
            StringBuilder::new(),
            TimestampNanosecondBuilder::new().with_timezone("+00:00"),
        );
        map.keys().append_value("first");
        map.values().append_value(-1);
        map.append(true).assured("map entry is valid");
        let source: ArrayRef = StdArc::new(map.finish());
        let staged = stage(&source);
        let map = staged
            .as_any()
            .downcast_ref::<MapArray>()
            .assured("map remains a map");
        assert_eq!(map.value_offsets(), &[0, 1]);
        let values = map
            .entries()
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .assured("map values are microsecond datetimes");
        assert_eq!(values.value(0), -1);

        let error = IcebergTableEmitter::staged_column(&source, &DataType::Utf8)
            .expect_err("an unplanned staged type must fail publication");
        assert_eq!(
            *error.current_context(),
            EmitterWriteError::Publish { connector: ICEBERG }
        );
    }

    /// The staged schema and the staged values follow every list level, retaining each list
    /// boundary and rounding negative nanoseconds down to the containing microsecond.
    #[test]
    fn bolero_nested_iceberg_datetimes_are_narrowed_at_every_depth() {
        let values = bolero::generator::produce_with::<Vec<i64>>().len(0_usize..=16);
        bolero::check!()
            .with_iterations(256)
            .with_max_len(160)
            .with_generator(values)
            .for_each(|nanoseconds| {
                let timestamp_field = StdArc::new(Field::new(
                    "item",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                    false,
                ));
                let inner =
                    ListBuilder::new(TimestampNanosecondBuilder::new().with_timezone("+00:00"))
                        .with_field(timestamp_field.clone());
                let inner_field =
                    StdArc::new(Field::new("item", DataType::List(timestamp_field), false));
                let mut outer = ListBuilder::new(inner).with_field(inner_field);
                for group in nanoseconds.chunks(3) {
                    for &nanosecond in group {
                        outer.values().values().append_value(nanosecond);
                    }
                    outer.values().append(true);
                }
                outer.append(true);
                let source: ArrayRef = StdArc::new(outer.finish());
                let mapped = arrow_schema::Schema::new(vec![Field::new(
                    "observed",
                    source.data_type().clone(),
                    true,
                )]);

                let staged_schema = IcebergTableEmitter::staged_arrow_schema(&mapped)
                    .assured("nested datetime schema stages as microseconds");
                let staged =
                    IcebergTableEmitter::staged_column(&source, staged_schema.field(0).data_type())
                        .assured("nested datetime values stage as microseconds");
                assert_eq!(staged.data_type(), staged_schema.field(0).data_type());
                let staged_outer = staged
                    .as_any()
                    .downcast_ref::<arrow_array::ListArray>()
                    .assured("outer list is retained");
                let staged_inner = staged_outer
                    .values()
                    .as_any()
                    .downcast_ref::<arrow_array::ListArray>()
                    .assured("inner lists are retained");
                let staged_values = staged_inner
                    .values()
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .assured("the nested datetime is narrowed");
                let expected = nanoseconds
                    .iter()
                    .map(|value| value.div_euclid(1_000))
                    .collect::<Vec<_>>();
                assert_eq!(staged_values.values().as_ref(), expected.as_slice());
                assert_eq!(staged_inner.len(), nanoseconds.chunks(3).len());
                assert_eq!(staged_outer.len(), 1);
                for (index, group) in nanoseconds.chunks(3).enumerate() {
                    assert_eq!(staged_inner.value(index).len(), group.len());
                }
            });
    }

    #[test]
    fn iceberg_staged_files_wait_for_the_commit_cadence_until_the_maximum_size() {
        let policy = commit_policy(100, 1_024);
        let cadence = EmitterStagedCommit::Cadence(Duration::from_millis(100));

        assert_eq!(policy.staged_commit(16), cadence);
        assert_eq!(policy.staged_commit(1_023), cadence);
        // Reaching the declared maximum makes the commit due without waiting for the cadence.
        assert_eq!(
            policy.staged_commit(1_024),
            EmitterStagedCommit::SizeReached
        );
        assert_eq!(
            policy.staged_commit(4_096),
            EmitterStagedCommit::SizeReached
        );
    }

    #[test]
    fn iceberg_retained_acknowledgements_resolve_every_staged_batch_at_once() {
        let first = StdArc::new(RecordedAcknowledgements::default());
        let second = StdArc::new(RecordedAcknowledgements::default());
        let retained = RetainedAcknowledgements(vec![
            EmitterRecordAcknowledgements::new(RecordedHandover(first.clone())),
            EmitterRecordAcknowledgements::new(RecordedHandover(second.clone())),
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

    #[nervix_primitives::test]
    async fn staged_files_are_written_and_loaded_on_the_filesystem_workers_under_their_charge() {
        let executor = Executor::default();
        let directory = tempfile::tempdir().assured("the staging directory is created");
        let path = directory.path().join("batch-1.arrow");
        let schema = StdArc::new(arrow_schema::Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            StdArc::clone(&schema),
            vec![StdArc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .assured("the column matches the schema");

        let written = IcebergTableEmitter::write_ipc_batch(&executor, path.clone(), batch.clone())
            .await
            .assured("the filesystem workers write the staged file");
        let IcebergLoadedGroup { batches, charge } = IcebergTableEmitter::load_group(
            &executor,
            schema,
            vec![path],
            IcebergLoadCharge::Staged { bytes: written },
        )
        .await
        .assured("the filesystem workers read the staged file back");

        assert!(written > 0);
        assert_eq!(batches, vec![batch]);
        assert_eq!(charge.bytes(), written);
        assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, written);
        drop(charge);
        let snapshot = executor.snapshot();
        assert_eq!(snapshot.filesystem_storage.admitted, 2);
        assert_eq!(snapshot.filesystem_storage.completed, 2);
        assert_eq!(snapshot.bulk_memory.reserved_bytes, 0);
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod commit_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod column_tests;
