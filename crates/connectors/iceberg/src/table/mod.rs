//! Iceberg emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The REST catalog and object-store clients one Iceberg table is loaded and committed
//!   through, the staged batches it appends to local Arrow IPC staging files and the file each
//!   write continues, what the staged rows wait for — the `COMMIT EACH` cadence the host measures
//!   from their staging, or the maximum commit size that makes them due at once — the Parquet data
//!   files one commit writes, the bulk memory charge of the staged batches it loads, and the
//!   acknowledgements it retains until that commit succeeds.
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

mod catalog_table;
mod offline_config;
mod parquet_bounds;
mod staging;
mod storage;
mod table_projection;

use std::{collections::BTreeMap, ops::Range, path::PathBuf, time::Duration};

use ::iceberg::{
    Catalog, CatalogBuilder, Error as IcebergError, ErrorKind as IcebergErrorKind,
    Result as IcebergResult,
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
use arrow_schema::{DataType, TimeUnit};
use arrow_select::filter::filter as filter_arrow_array;
use catalog_table::IcebergCatalogTable;
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
    AdmissionError, Cancellation, CpuClass, Executor, MemoryClass, QueueAdmission, Reservation,
    StorageClass,
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

/// What one staging append is charged. The batches are already in memory and the writer streams
/// them to the file, so the charge only admits the append onto the filesystem workers.
const STAGED_WRITE_RESERVATION_BYTES: u64 = 1;

/// The Iceberg emitter, which appends each mapped batch to a local Arrow IPC staging file and
/// appends every staged batch to its table in one catalog commit.
pub struct IcebergTableEmitter {
    host: EmitterHost,
    client: IcebergTableEmitterClient,
    commit_state: IcebergCommitState,
    /// The exact Arrow schema every staging file is written and read back with, which is the
    /// host's mapped schema with datetime columns narrowed to the microsecond resolution Iceberg
    /// stores. Each column keeps its expression's nullability, which a commit checks against the
    /// columns the table requires.
    staged_schema: StdArc<arrow_schema::Schema>,
    commit_policy: IcebergCommitPolicy,
    batch_policy: Option<EmitterBatchPolicy>,
    staging_dir: TempDir,
    /// The node's bounded executor, whose filesystem workers write and read the staging files.
    executor: Executor,
    /// The sequence the newest staging file is named by.
    staging_sequence: u64,
    /// Every staging file that holds staged batches, by the sequence it is named by.
    staging_files: BTreeMap<u64, IcebergStagingFile>,
    /// The staging file the next write appends to: absent until a write starts one, and again once
    /// a commit seals it.
    appending_to: Option<u64>,
    /// Every staged batch in the order it was staged, which is the order the staging files hold
    /// them in.
    staged_batches: Vec<IcebergStagedBatch>,
    staged_rows: u64,
    /// The Arrow IPC bytes of every staged batch, which the maximum commit size counts.
    staged_bytes: u64,
}

/// One staged batch — a carrier's mapped rows, or one `BATCH` segment of them — at its byte range
/// in a staging file, with the acknowledgements its commit resolves.
struct IcebergStagedBatch {
    /// The sequence of the staging file that holds this batch.
    file: u64,
    /// The bytes of that file that hold this batch's messages.
    range: Range<u64>,
    rows: u64,
    acknowledgements: Option<EmitterRecordAcknowledgements>,
    domain_timestamp: Timestamp,
}

impl IcebergStagedBatch {
    /// The Arrow IPC bytes this batch takes, which its commit charges for reading it back.
    fn bytes(&self) -> u64 {
        self.range
            .end
            .checked_sub(self.range.start)
            .assured("a staged batch's range ends after it starts")
    }
}

/// One local Arrow IPC staging file: the staged schema's message, then the staged batches appended
/// after it.
struct IcebergStagingFile {
    path: PathBuf,
    /// Where the schema message ends and the file's first staged batch starts.
    schema_end: u64,
    /// Where the file's last staged batch ends, which is where the next append starts.
    end: u64,
    /// How many staged batches the file holds that no commit has published yet.
    batches: usize,
}

/// The rows of one carrier that one append staged: the members of each staged batch, the
/// acknowledgements the final batch retains, and when the host mapped them.
struct IcebergStagedRows<'a> {
    ranges: &'a [Range<usize>],
    acknowledgements: Option<EmitterRecordAcknowledgements>,
    occurred_at: Timestamp,
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

/// One commit's progress: the leading staged batches it publishes, the data files written for
/// those it has loaded so far, and the append identity that recognizes those data files in the
/// table. A failed attempt keeps it, so the retry neither writes the data files again nor publishes
/// batches staged after the commit began.
///
/// A commit loads the batches it covers in groups the bulk memory class can hold at once and writes
/// each group's rows into data files of their own; the data files of every group join the one
/// catalog append.
struct IcebergCommitPreparation {
    append_id: uuid::Uuid,
    /// The leading staged batches this commit publishes.
    covered: usize,
    /// The leading staged batches whose rows `data_files` already hold.
    written: usize,
    data_files: Vec<DataFile>,
}

impl IcebergCommitPreparation {
    /// A commit of the `covered` leading staged batches, none of them written yet.
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

/// The staged batches one load reads into memory at once, and what it charges for them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IcebergLoadGroup {
    batches: usize,
    charge: IcebergLoadCharge,
}

impl IcebergLoadGroup {
    /// The leading batches of `unwritten` a bulk memory class of `capacity` bytes holds together,
    /// and at least the first. A first batch larger than the whole class is read alone, charged
    /// with the whole class.
    fn leading(unwritten: &[IcebergStagedBatch], capacity: u64) -> Self {
        let mut batches = 0_usize;
        let mut bytes = 0_u64;
        for staged in unwritten {
            let with_batch = bytes
                .checked_add(staged.bytes())
                .assured("the total counts bytes this emitter already staged on disk");
            if batches > 0 && with_batch > capacity {
                break;
            }
            batches = batches
                .checked_add(1)
                .assured("a group counts batches this emitter already staged");
            bytes = with_batch;
        }
        let charge = if bytes > capacity {
            IcebergLoadCharge::WholeClass {
                batch_bytes: bytes,
                capacity,
            }
        } else {
            IcebergLoadCharge::Staged { bytes }
        };
        Self { batches, charge }
    }
}

/// What one load charges the bulk memory class for the staged batches it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IcebergLoadCharge {
    /// Every byte of the staged batches, which the class holds together.
    Staged { bytes: u64 },
    /// One staged batch larger than the whole class, which a route on the same node can produce by
    /// flushing batches above it. The load charges the whole class: every byte of the batch the
    /// class can account for, leaving none for other bulk work beside the read.
    WholeClass { batch_bytes: u64, capacity: u64 },
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
                    batch_bytes,
                    capacity,
                },
            ) => format!(
                "Iceberg commit waits for the whole bulk memory class of {capacity} bytes for a \
                 staged batch of {batch_bytes} bytes"
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

/// A table that declares the master key its files are encrypted under, whatever its format
/// version. The emitter writes its data files and manifests unencrypted, so it publishes nothing
/// into a table whose readers decrypt what it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the Iceberg table declares an encryption key, and the emitter writes no encrypted files")]
struct IcebergEncryptedTable;

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
        let catalog_table = IcebergCatalogTable {
            namespace: &namespace,
            table: &table,
        };
        let loaded = catalog_table.load(catalog.as_ref()).await?;
        if loaded.metadata().location() != location {
            return Err(
                Report::new(EmitterOpenError::InvalidConfiguration { connector: ICEBERG })
                    .attach_printable(format!(
                        "table '{}' is registered at '{}' but emitter location is '{location}'",
                        loaded.identifier(),
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
            staging_sequence: 0,
            staging_files: BTreeMap::new(),
            appending_to: None,
            staged_batches: Vec::new(),
            staged_rows: 0,
            staged_bytes: 0,
            host,
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

    /// The staging file the next write appends to, and the sequence that names it: the open file,
    /// or a new one named by the next sequence.
    fn append_target(&mut self) -> (u64, staging::AppendTarget) {
        if let Some(sequence) = self.appending_to {
            let file = self
                .staging_files
                .get(&sequence)
                .assured("the open staging file stays until a commit publishes what it holds");
            let target = staging::AppendTarget::Open {
                path: file.path.clone(),
                schema_end: file.schema_end,
                end: file.end,
            };
            return (sequence, target);
        }
        self.staging_sequence = self
            .staging_sequence
            .checked_add(1)
            .assured("an emitter cannot start 2^64 staging files in the lifetime of a node");
        let path = self
            .staging_dir
            .path()
            .join(format!("staging-{}.arrow", self.staging_sequence));
        (self.staging_sequence, staging::AppendTarget::New { path })
    }

    /// Appends `batches` to the staging file `target` names on the node's filesystem workers,
    /// creating it first when it is new. The batches are already in memory and the writer streams
    /// them to the file, so the charge only admits the append.
    async fn append_staged(
        executor: &Executor,
        target: &staging::AppendTarget,
        schema: StdArc<arrow_schema::Schema>,
        batches: Vec<RecordBatch>,
    ) -> EmitterWriteResult<staging::Staged> {
        let reservation = executor
            .reserve(MemoryClass::Bulk, STAGED_WRITE_RESERVATION_BYTES)
            .await
            .change_context(EmitterWriteError::Publish { connector: ICEBERG })?;
        let target = target.clone();
        executor
            .run_storage(
                StorageClass::Filesystem,
                reservation,
                move |_charge, cancellation: &Cancellation| {
                    target
                        .append(&schema, &batches, || cancellation.check())
                        .change_context(EmitterWriteError::Publish { connector: ICEBERG })
                },
            )
            .await
            .change_context(EmitterWriteError::Publish { connector: ICEBERG })?
    }

    /// Read the contiguous staged batches each of `spans` names back on the node's filesystem
    /// workers once the bulk memory class has granted their `charge`, checking before each span
    /// whether the commit stopped waiting. The reservation comes back with the rows it covers, so
    /// it lasts for as long as the commit holds them.
    ///
    /// The charge is taken only when the class has room for it now: a place in the class's queue
    /// would take every byte the class frees until it was satisfied, holding the node's other bulk
    /// work behind this commit. The read waits for a place among the filesystem workers instead of
    /// being refused, because nothing could present again the rows the emitter already accepted.
    async fn load_group(
        executor: &Executor,
        schema: StdArc<arrow_schema::Schema>,
        spans: Vec<staging::Span>,
        charge: IcebergLoadCharge,
    ) -> EmitterWriteResult<IcebergLoadedGroup> {
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
                    for span in spans {
                        cancellation
                            .check()
                            .change_context(EmitterWriteError::Commit { connector: ICEBERG })?;
                        let read = span
                            .read(schema.as_ref())
                            .change_context(EmitterWriteError::Commit { connector: ICEBERG })?;
                        batches.extend(read);
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

    /// The spans `group`'s staged batches take in `files`, one for each run of them a staging file
    /// holds, in order. A file holds its batches back to back, so each run is one contiguous span.
    fn staged_spans(
        files: &BTreeMap<u64, IcebergStagingFile>,
        group: &[IcebergStagedBatch],
    ) -> Vec<staging::Span> {
        let mut spans: Vec<staging::Span> = Vec::new();
        let mut spanned_file = None;
        for staged in group {
            if spanned_file == Some(staged.file)
                && let Some(last) = spans.last_mut()
            {
                last.bytes.end = staged.range.end;
                last.batches = last
                    .batches
                    .checked_add(1)
                    .assured("a group counts batches this emitter already staged");
                continue;
            }
            let file = files
                .get(&staged.file)
                .assured("a staged batch's file stays until a commit publishes it");
            spans.push(staging::Span {
                path: file.path.clone(),
                schema_end: file.schema_end,
                bytes: staged.range.clone(),
                batches: 1,
            });
            spanned_file = Some(staged.file);
        }
        spans
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

    /// Stages a carrier's accepted rows, as one batch or as BATCH-sized segments, by appending them
    /// to the open staging file, and retains its one acknowledgement handle on its final staged
    /// batch until the catalog commit publishes all of them. The carrier is staged whole or not at
    /// all: a failed append is cut back off the file.
    async fn stage(
        &mut self,
        carrier: MappedEmitterCarrier<'_>,
        outcome: &mut PerRecordOutcome<EmitterRecordPosition>,
    ) -> EmitterWriteResult<()> {
        let staged = self.staged_batch(&carrier)?;
        let mut ranges = Vec::<Range<usize>>::new();
        let mut rejected_rows = vec![false; staged.num_rows()];
        if let Some(policy) = self.batch_policy {
            let division = self.staged_file_division(&staged, policy).await?;
            self.host.record_payload_subdivisions(division.subdivisions);
            for request in division.requests {
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

        let mut segments = Vec::with_capacity(ranges.len());
        for range in &ranges {
            segments.push(staged.slice(range.start, range.len()));
        }
        if !segments.is_empty() {
            let (sequence, target) = self.append_target();
            let appended = Self::append_staged(
                &self.executor,
                &target,
                StdArc::clone(&self.staged_schema),
                segments,
            )
            .await?;
            let rows = IcebergStagedRows {
                ranges: &ranges,
                acknowledgements: carrier.acknowledgements,
                occurred_at: carrier.occurred_at,
            };
            self.record_append(sequence, target, appended, rows);
        }
        for (member, row) in carrier.selected_rows.iter().enumerate() {
            if !rejected_rows[member] {
                outcome.deliver(EmitterRecordPosition {
                    batch_index: carrier.batch_index,
                    row_index: *row,
                });
            }
        }
        Ok(())
    }

    /// Records the staged batches one append wrote to the staging file `target` named by
    /// `sequence`, holding the rows `staged` describes. A new file becomes the one later writes
    /// append to.
    fn record_append(
        &mut self,
        sequence: u64,
        target: staging::AppendTarget,
        appended: staging::Staged,
        staged: IcebergStagedRows<'_>,
    ) {
        let staging::Staged {
            schema_end,
            appended,
        } = appended;
        match target {
            staging::AppendTarget::Open { .. } => {
                let file = self
                    .staging_files
                    .get_mut(&sequence)
                    .assured("the open staging file stays until a commit publishes what it holds");
                file.end = appended.end;
                file.batches = file
                    .batches
                    .checked_add(appended.ranges.len())
                    .assured("a staging file counts batches this emitter already staged");
            }
            staging::AppendTarget::New { path } => {
                let file = IcebergStagingFile {
                    path,
                    schema_end,
                    end: appended.end,
                    batches: appended.ranges.len(),
                };
                self.staging_files.insert(sequence, file);
                self.appending_to = Some(sequence);
            }
        }
        let mut staged_rows = 0_u64;
        let mut staged_bytes = 0_u64;
        let first_batch = self.staged_batches.len();
        for (members, range) in staged.ranges.iter().zip(appended.ranges) {
            let rows: u64 = members.len().arch_into();
            let batch = IcebergStagedBatch {
                file: sequence,
                range,
                rows,
                acknowledgements: None,
                domain_timestamp: staged.occurred_at,
            };
            staged_rows = staged_rows
                .checked_add(rows)
                .assured("the accepted ranges are disjoint rows of one mapped batch");
            staged_bytes = staged_bytes
                .checked_add(batch.bytes())
                .assured("staged bytes fit the node's u64 byte count");
            self.staged_batches.push(batch);
        }
        if let Some(last) = self.staged_batches[first_batch..].last_mut() {
            last.acknowledgements = staged.acknowledgements;
        }
        self.staged_rows = self
            .staged_rows
            .checked_add(staged_rows)
            .assured("both counts total rows this emitter already staged on disk");
        self.staged_bytes = self
            .staged_bytes
            .checked_add(staged_bytes)
            .assured("both counts total bytes this emitter already staged on disk");
        trace!(
            rows = staged_rows,
            bytes = staged_bytes,
            "emitter staged iceberg rows"
        );
    }

    /// Publishes the staged batches the commit in progress covers, beginning a commit of every
    /// staged batch when none is in progress, and reports what they held. A commit that begins
    /// seals the open staging file, so what later writes stage goes to a file of its own.
    async fn commit_covered(&mut self) -> EmitterWriteResult<EmitterCommitReport> {
        let staged_batches = self.staged_batches.len();
        let capacity = self.executor.memory(MemoryClass::Bulk).capacity_bytes;
        if self.commit_state.preparation.is_none() {
            self.appending_to = None;
        }
        let preparation = self
            .commit_state
            .preparation
            .get_or_insert_with(|| IcebergCommitPreparation::new(staged_batches));
        while !preparation.is_written() {
            nervix_primitives::task::consume_budget().await;
            let unwritten = self
                .staged_batches
                .get(preparation.unwritten())
                .assured("a commit covers batches that stay staged until it publishes them");
            let group = IcebergLoadGroup::leading(unwritten, capacity);
            let spans = Self::staged_spans(&self.staging_files, &unwritten[..group.batches]);
            let IcebergLoadedGroup { batches, charge } = Self::load_group(
                &self.executor,
                StdArc::clone(&self.staged_schema),
                spans,
                group.charge,
            )
            .await?;
            let data_files = self
                .client
                .write_data_files(&self.staged_schema, batches, self.batch_policy)
                .await?;
            // The rows the charge covered are in data files now.
            drop(charge);
            for file in &data_files {
                self.host.record_prepared_payload(
                    nervix_connector::EmitterPayloadMeasurement::sized(
                        file.record_count(),
                        file.file_size_in_bytes(),
                    ),
                );
            }
            preparation.record_written(group.batches, data_files);
        }
        self.client.commit_prepared(preparation).await?;
        let covered = preparation.covered;
        self.commit_state.preparation = None;
        Ok(self.publish_committed(covered).await)
    }

    /// Removes the `covered` leading staged batches a commit appended, removes every staging file
    /// that no longer holds a staged batch, resolves the acknowledgements the batches retained,
    /// and reports what they held.
    async fn publish_committed(&mut self, covered: usize) -> EmitterCommitReport {
        let committed = self.staged_batches.drain(..covered).collect::<Vec<_>>();
        let mut messages = 0_u64;
        let mut bytes = 0_u64;
        for batch in &committed {
            messages = messages
                .checked_add(batch.rows)
                .assured("the total counts rows this emitter already staged on disk");
            bytes = bytes
                .checked_add(batch.bytes())
                .assured("the total counts bytes this emitter already staged on disk");
        }
        let domain_timestamp = committed
            .iter()
            .map(|batch| batch.domain_timestamp)
            .max()
            .assured("a commit begins only over staged batches and covers at least one");
        self.staged_rows = self
            .staged_rows
            .checked_sub(messages)
            .assured("the staged total counts the rows of every staged batch");
        self.staged_bytes = self
            .staged_bytes
            .checked_sub(bytes)
            .assured("the staged total counts the bytes of every staged batch");
        let mut acknowledgements = Vec::with_capacity(committed.len());
        for batch in committed {
            nervix_primitives::task::consume_budget().await;
            acknowledgements.extend(batch.acknowledgements);
            self.release_staged_batch(batch.file).await;
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

    /// Counts one staged batch of the staging file `sequence` published, and removes the file once
    /// it holds no staged batch.
    async fn release_staged_batch(&mut self, sequence: u64) {
        let file = self
            .staging_files
            .get_mut(&sequence)
            .assured("a staged batch's file stays until a commit publishes it");
        file.batches = file
            .batches
            .checked_sub(1)
            .assured("a staging file counts every staged batch it holds");
        if file.batches > 0 {
            return;
        }
        let file = self
            .staging_files
            .remove(&sequence)
            .assured("the file this looked up is still held");
        if self.appending_to == Some(sequence) {
            self.appending_to = None;
        }
        if let Err(error) = tokio::fs::remove_file(&file.path).await {
            debug!(
                path = %file.path.display(),
                error = %error,
                "failed to remove committed Iceberg staging file"
            );
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
        self.install_table(table)?;
        self.refuse_encrypted_table()
    }

    /// Refuses the installed table when it declares an encryption key. Every commit reloads the
    /// table before it writes a data file and again before it appends, so a table that declares
    /// one receives neither.
    fn refuse_encrypted_table(&self) -> EmitterWriteResult<()> {
        let declared = self
            .table
            .metadata()
            .properties()
            .contains_key(TableProperties::PROPERTY_ENCRYPTION_KEY_ID);
        if !declared {
            return Ok(());
        }
        let refusal = IcebergEncryptedTable;
        let description = refusal.to_string();
        Err(Report::new(refusal)
            .change_context(EmitterWriteError::Commit { connector: ICEBERG })
            .attach_printable(description))
    }

    /// Writes `batches`, which carry the columns of `staged`, into new Parquet data files of the
    /// table, without committing them. Each batch is laid out as the table's current columns first,
    /// each mapped column in the column of its name, so a table changed since the emitter opened is
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
mod tests;

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
