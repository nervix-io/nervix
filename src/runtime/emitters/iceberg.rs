use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc as StdArc,
};

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
    spec::DataFileFormat,
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
use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_select::concat::concat as concat_arrow_arrays;
use error_stack::{Report, ResultExt};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::file::properties::WriterProperties;
use thiserror::Error;
use triomphe::Arc;
use url::Url;

use super::*;

pub(in crate::runtime) struct IcebergEmitter {
    client: IcebergEmitterClient,
    program: CompiledSqlValuesProgram,
    mapped_schema: StdArc<arrow_schema::Schema>,
    input_schema: Arc<CompiledSchema>,
    flush_policy: RuntimeFlushPolicy,
    commit_policy: IcebergCommitPolicy,
    staging_dir: TempDir,
    pending_sequence: u64,
    pending_batches: Vec<IcebergPendingBatch>,
    pending_rows: u64,
    pending_bytes: u64,
    flush_at: Option<Instant>,
    staged_batches: Vec<IcebergStagedBatch>,
    staged_rows: u64,
    staged_bytes: u64,
    commit_at: Option<Instant>,
}

struct IcebergEmitterClient {
    catalog: StdArc<RestCatalog>,
    table: Table,
    file_name_prefix: String,
    data_file_sequence: u64,
}

pub(in crate::runtime::emitters) type IcebergEmitterResult<T> =
    Result<T, Report<IcebergEmitterError>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(in crate::runtime::emitters) enum IcebergEmitterError {
    #[error("invalid Iceberg flush policy")]
    InvalidFlushPolicy,
    #[error("invalid Iceberg commit policy")]
    InvalidCommitPolicy,
    #[error("failed to compile Iceberg VALUES program")]
    CompileValues,
    #[error("failed to create Iceberg staging directory")]
    CreateStagingDir,
    #[error("failed to build Iceberg table schema")]
    BuildSchema,
    #[error("invalid Iceberg object-store location")]
    InvalidLocation,
    #[error("failed to initialize Iceberg catalog")]
    InitializeCatalog,
    #[error("failed to initialize Iceberg table")]
    InitializeTable,
    #[error("failed to flush Iceberg batch to local Arrow IPC")]
    FlushToDisk,
    #[error("failed to map Iceberg VALUES batch")]
    MapBatch,
    #[error("failed to write Iceberg staged Arrow IPC")]
    WriteStagedIpc,
    #[error("failed to read Iceberg staged Arrow IPC")]
    ReadStagedIpc,
    #[error("failed to commit Iceberg staged batches")]
    Commit,
}

impl IcebergEmitterError {
    pub(in crate::runtime::emitters) fn is_retryable_publish_failure(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy)]
struct IcebergCommitPolicy {
    interval: Duration,
    max_size: u64,
}

struct IcebergPendingBatch {
    batch: RuntimeRecordBatch,
    metadata: Vec<RuntimeRecordMetadata>,
    keys: Vec<Option<BranchKey>>,
    acks: Vec<AckSet>,
    domain_timestamp: Timestamp,
}

struct IcebergStagedBatch {
    path: PathBuf,
    rows: u64,
    bytes: u64,
    acks: Vec<AckSet>,
    domain_timestamp: Timestamp,
}

#[derive(Debug, Clone)]
struct IcebergObjectStoreProperties {
    backend: IcebergStorageBackend,
    props: HashMap<String, String>,
}

struct IcebergEmitterClientInit<'a> {
    config: &'a [nervix_models::ClientConfigEntry],
    backend: IcebergStorageBackend,
    catalog_client: &'a CreateClientIcebergRest,
    catalog_config: &'a [nervix_models::ClientConfigEntry],
    context: &'a EmitterSinkContext,
    table: &'a Identifier,
    location: &'a str,
    catalog: &'a IcebergCatalog,
}

pub(in crate::runtime::emitters) enum IcebergEmitterClientConfig<'a> {
    S3(&'a CreateClientS3),
    Gcs(&'a CreateClientGcs),
    AzureBlob(&'a CreateClientAzureBlob),
}

pub(in crate::runtime::emitters) struct IcebergEmitterInit<'a> {
    pub(in crate::runtime::emitters) client: IcebergEmitterClientConfig<'a>,
    pub(in crate::runtime::emitters) resolved: Option<&'a ResolvedClientConfig>,
    pub(in crate::runtime::emitters) catalog_client: &'a CreateClientIcebergRest,
    pub(in crate::runtime::emitters) catalog_resolved: Option<&'a ResolvedClientConfig>,
    pub(in crate::runtime::emitters) context: &'a EmitterSinkContext,
    pub(in crate::runtime::emitters) table: &'a Identifier,
    pub(in crate::runtime::emitters) values: &'a [IcebergValueMapping],
    pub(in crate::runtime::emitters) location: &'a str,
    pub(in crate::runtime::emitters) catalog: &'a IcebergCatalog,
    pub(in crate::runtime::emitters) flush_each: &'a str,
    pub(in crate::runtime::emitters) max_batch_size: Option<&'a str>,
    pub(in crate::runtime::emitters) commit_each: &'a str,
    pub(in crate::runtime::emitters) max_commit_size: &'a str,
    pub(in crate::runtime::emitters) input_schema: Arc<CompiledSchema>,
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

impl IcebergEmitterClientConfig<'_> {
    fn backend(&self) -> IcebergStorageBackend {
        match self {
            Self::S3(_) => IcebergStorageBackend::S3,
            Self::Gcs(_) => IcebergStorageBackend::Gcs,
            Self::AzureBlob(_) => IcebergStorageBackend::AzureBlob,
        }
    }

    fn config(&self) -> &[nervix_models::ClientConfigEntry] {
        match self {
            Self::S3(client) => client.config.as_slice(),
            Self::Gcs(client) => client.config.as_slice(),
            Self::AzureBlob(client) => client.config.as_slice(),
        }
    }
}

impl IcebergEmitter {
    pub(in crate::runtime) async fn new(
        init: IcebergEmitterInit<'_>,
    ) -> IcebergEmitterResult<Self> {
        let IcebergEmitterInit {
            client,
            resolved,
            catalog_client,
            catalog_resolved,
            context,
            table,
            values,
            location,
            catalog,
            flush_each,
            max_batch_size,
            commit_each,
            max_commit_size,
            input_schema,
        } = init;
        let flush_policy = Runtime::parse_runtime_node_flush_policy(
            &context.domain,
            "iceberg emitter",
            &context.emitter,
            flush_each,
            max_batch_size,
        )
        .map_err(|error| {
            Report::new(IcebergEmitterError::InvalidFlushPolicy).attach_printable(error.to_string())
        })?;
        let commit_policy = Self::parse_commit_policy(context, commit_each, max_commit_size)?;
        let backend = client.backend();
        let program = compile_iceberg_values_program(
            &context.domain,
            &context.emitter,
            values,
            input_schema.arrow_schema(),
        )
        .map_err(|error| {
            Report::new(IcebergEmitterError::CompileValues).attach_printable(error.to_string())
        })?;
        let mapped_schema = Self::mapped_arrow_schema(&program, values)?;
        let staging_dir = Self::create_staging_dir(context.temp_dir.as_path())?;
        let client_init = IcebergEmitterClientInit {
            config: resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or_else(|| client.config()),
            backend,
            catalog_client,
            catalog_config: catalog_resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or_else(|| catalog_client.config.as_slice()),
            context,
            table,
            location,
            catalog,
        };
        let client = Self::client_from_config(client_init).await?;
        Ok(Self {
            client,
            program,
            mapped_schema,
            input_schema,
            flush_policy,
            commit_policy,
            staging_dir,
            pending_sequence: 0,
            pending_batches: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
            flush_at: None,
            staged_batches: Vec::new(),
            staged_rows: 0,
            staged_bytes: 0,
            commit_at: None,
        })
    }

    fn create_staging_dir(root: &Path) -> IcebergEmitterResult<TempDir> {
        std::fs::create_dir_all(root)
            .change_context(IcebergEmitterError::CreateStagingDir)
            .attach_printable(format!("staging root: {}", root.display()))?;
        TempDir::new_in(root)
            .change_context(IcebergEmitterError::CreateStagingDir)
            .attach_printable(format!("staging root: {}", root.display()))
    }

    fn parse_commit_policy(
        context: &EmitterSinkContext,
        commit_each: &str,
        max_commit_size: &str,
    ) -> IcebergEmitterResult<IcebergCommitPolicy> {
        let interval = Runtime::parse_runtime_node_duration_setting(
            &context.domain,
            "iceberg emitter",
            &context.emitter,
            "commit_each",
            commit_each,
        )
        .map_err(|error| {
            Report::new(IcebergEmitterError::InvalidCommitPolicy)
                .attach_printable(error.to_string())
        })?;
        let max_size = max_commit_size
            .parse::<ubyte::ByteUnit>()
            .map_err(|error| {
                Report::new(IcebergEmitterError::InvalidCommitPolicy)
                    .attach_printable(format!("max_commit_size '{max_commit_size}': {error}"))
            })?
            .as_u64();
        Ok(IcebergCommitPolicy { interval, max_size })
    }

    fn mapped_arrow_schema(
        program: &CompiledSqlValuesProgram,
        values: &[IcebergValueMapping],
    ) -> IcebergEmitterResult<StdArc<arrow_schema::Schema>> {
        let output_fields = program.program.output_schema.fields();
        if output_fields.len() != values.len() {
            return Err(
                Report::new(IcebergEmitterError::BuildSchema).attach_printable(format!(
                    "VALUES output fields: {}, mappings: {}",
                    output_fields.len(),
                    values.len()
                )),
            );
        }
        let mut seen = HashSet::default();
        let mut fields = Vec::with_capacity(output_fields.len());
        for (field, mapping) in output_fields.iter().zip(values) {
            if !seen.insert(mapping.column.as_str().to_string()) {
                return Err(Report::new(IcebergEmitterError::BuildSchema)
                    .attach_printable(format!("duplicate mapped column: {}", mapping.column)));
            }
            fields.push(Self::iceberg_arrow_field(field, &mapping.column));
        }
        Ok(StdArc::new(arrow_schema::Schema::new(fields)))
    }

    fn iceberg_arrow_field(field: &arrow_schema::Field, column: &str) -> arrow_schema::Field {
        arrow_schema::Field::new(
            column,
            Self::iceberg_arrow_data_type(field.data_type()),
            true,
        )
    }

    fn iceberg_arrow_data_type(data_type: &arrow_schema::DataType) -> arrow_schema::DataType {
        if let arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some(tz)) =
            data_type
            && (tz.as_ref() == "+00:00" || tz.as_ref() == "UTC")
        {
            return arrow_schema::DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("+00:00".into()),
            );
        }
        data_type.clone()
    }

    async fn client_from_config(
        init: IcebergEmitterClientInit<'_>,
    ) -> IcebergEmitterResult<IcebergEmitterClient> {
        let IcebergEmitterClientInit {
            config,
            backend,
            catalog_client,
            catalog_config,
            context,
            table,
            location,
            catalog,
        } = init;
        let IcebergCatalog::Rest {
            client: catalog_ref,
        } = catalog;
        if catalog_ref != &catalog_client.name {
            return Err(
                Report::new(IcebergEmitterError::InitializeCatalog).attach_printable(format!(
                    "emitter catalog reference '{}' resolved to client '{}'",
                    catalog_ref.as_str(),
                    catalog_client.name.as_str()
                )),
            );
        }
        Self::validate_blob_location(backend, "table", location)?;
        let properties = IcebergObjectStoreProperties::from_entries(backend, config);
        let catalog = StdArc::new(
            properties
                .rest_catalog(catalog_client.name.as_str(), catalog_config)
                .await
                .change_context(IcebergEmitterError::InitializeCatalog)?,
        );
        let namespace = NamespaceIdent::new(context.domain.as_str().to_string());
        let table_name = table.as_str().to_string();
        let table_ident = TableIdent::new(namespace, table_name.clone());
        let table = catalog
            .load_table(&table_ident)
            .await
            .change_context(IcebergEmitterError::InitializeTable)
            .attach_printable(format!("table: {table_ident}"))?;
        if table.metadata().location() != location {
            return Err(
                Report::new(IcebergEmitterError::InvalidLocation).attach_printable(format!(
                    "table '{}' is registered at '{}' but emitter location is '{}'",
                    table_ident,
                    table.metadata().location(),
                    location
                )),
            );
        }
        Ok(IcebergEmitterClient {
            catalog,
            table,
            file_name_prefix: format!(
                "{}-{}-{}-{}",
                context.emitter.as_str(),
                table_name,
                current_timestamp().unix_nanos(),
                fastrand::u64(..)
            ),
            data_file_sequence: 0,
        })
    }

    fn validate_blob_location(
        backend: IcebergStorageBackend,
        label: &str,
        location: &str,
    ) -> IcebergEmitterResult<String> {
        let url = Url::parse(location)
            .change_context(IcebergEmitterError::InvalidLocation)
            .attach_printable(format!("{label} location: {location}"))?;
        if !backend.accepts_location_scheme(url.scheme()) {
            let expected = match backend {
                IcebergStorageBackend::S3 => "s3://",
                IcebergStorageBackend::Gcs => "gs://",
                IcebergStorageBackend::AzureBlob => "wasb:// or wasbs://",
            };
            return Err(Report::new(IcebergEmitterError::InvalidLocation)
                .attach_printable(format!("{label} location '{location}' must use {expected}")));
        }
        if url.host_str().is_none() {
            return Err(
                Report::new(IcebergEmitterError::InvalidLocation).attach_printable(format!(
                    "{label} location '{location}' must include a {} bucket",
                    backend.as_ref()
                )),
            );
        }
        if let IcebergStorageBackend::AzureBlob = backend {
            if url.username().is_empty() {
                return Err(
                    Report::new(IcebergEmitterError::InvalidLocation).attach_printable(format!(
                        "{label} location '{location}' must include an Azure container before @"
                    )),
                );
            }
            let host = url.host_str().unwrap_or_default();
            if !host.contains(".blob.") {
                return Err(
                    Report::new(IcebergEmitterError::InvalidLocation).attach_printable(format!(
                        "{label} location '{location}' must use an Azure Blob host"
                    )),
                );
            }
        }
        Ok(url.scheme().to_string())
    }

    pub(in crate::runtime) fn flush_deadline(&self) -> Option<Instant> {
        match (self.flush_at, self.commit_at) {
            (Some(flush_at), Some(commit_at)) => Some(flush_at.min(commit_at)),
            (Some(flush_at), None) => Some(flush_at),
            (None, Some(commit_at)) => Some(commit_at),
            (None, None) => None,
        }
    }

    pub(in crate::runtime) async fn publish_batch(
        &mut self,
        batch: RelayRecordBatch,
    ) -> IcebergEmitterResult<Option<PublishReport>> {
        let bytes = batch.estimated_bytes();
        let rows = batch.message_count();
        let domain_timestamp = batch.domain_timestamp().unwrap_or_else(current_timestamp);
        self.pending_batches.push(IcebergPendingBatch {
            batch: batch.batch,
            metadata: batch.metadata,
            keys: batch.keys,
            acks: batch.acks,
            domain_timestamp,
        });
        self.pending_rows = self.pending_rows.saturating_add(rows);
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        let should_flush = match self.flush_policy {
            RuntimeFlushPolicy::Immediate => true,
            RuntimeFlushPolicy::Each {
                interval,
                max_batch_size,
            } => {
                if self.flush_at.is_none() {
                    self.flush_at = Some(Instant::now() + interval);
                }
                self.pending_bytes >= max_batch_size
            }
        };
        if should_flush {
            self.flush_to_disk().await?;
            self.commit_if_due(false).await
        } else {
            Ok(None)
        }
    }

    pub(in crate::runtime) async fn flush_due(
        &mut self,
    ) -> IcebergEmitterResult<Option<PublishReport>> {
        let now = Instant::now();
        if self.flush_at.is_some_and(|deadline| deadline <= now) {
            self.flush_to_disk().await?;
        }
        self.commit_if_due(false).await
    }

    pub(in crate::runtime) async fn finish(
        &mut self,
    ) -> IcebergEmitterResult<Option<PublishReport>> {
        self.flush_to_disk().await?;
        self.commit_if_due(true).await
    }

    async fn flush_to_disk(&mut self) -> IcebergEmitterResult<()> {
        if self.pending_rows == 0 {
            self.flush_at = None;
            return Ok(());
        }
        let pending_batches = self
            .pending_batches
            .iter()
            .map(|batch| &batch.batch)
            .collect::<Vec<_>>();
        let input_batch = match RuntimeRecordBatch::concat(&pending_batches) {
            Ok(batch) => batch,
            Err(error) => {
                self.flush_at = Some(Instant::now() + Duration::from_secs(1));
                return Err(Report::new(IcebergEmitterError::FlushToDisk).attach_printable(error));
            }
        };
        let metadata = self
            .pending_batches
            .iter()
            .flat_map(|batch| batch.metadata.iter().cloned())
            .collect::<Vec<_>>();
        let keys = self
            .pending_batches
            .iter()
            .flat_map(|batch| batch.keys.iter().cloned())
            .collect::<Vec<_>>();
        let batch = match self
            .mapped_arrow_batch_from_runtime_batch(&input_batch, metadata, keys)
            .await
        {
            Ok(batch) => batch,
            Err(error) => {
                self.flush_at = Some(Instant::now() + Duration::from_secs(1));
                return Err(error);
            }
        };
        let path = self.next_staged_path();
        let staged_bytes = match Self::write_ipc_batch(path.clone(), batch).await {
            Ok(bytes) => bytes,
            Err(error) => {
                self.flush_at = Some(Instant::now() + Duration::from_secs(1));
                return Err(error);
            }
        };
        let rows = self.pending_rows;
        let domain_timestamp = self
            .pending_batches
            .iter()
            .map(|batch| batch.domain_timestamp)
            .max()
            .unwrap_or_else(current_timestamp);
        let pending = std::mem::take(&mut self.pending_batches);
        let acks = pending
            .into_iter()
            .flat_map(|batch| batch.acks)
            .collect::<Vec<_>>();
        self.staged_batches.push(IcebergStagedBatch {
            path,
            rows,
            bytes: staged_bytes,
            acks,
            domain_timestamp,
        });
        self.staged_rows = self.staged_rows.saturating_add(rows);
        self.staged_bytes = self.staged_bytes.saturating_add(staged_bytes);
        self.pending_rows = 0;
        self.pending_bytes = 0;
        self.flush_at = None;
        if self.commit_at.is_none() {
            self.commit_at = Some(Instant::now() + self.commit_policy.interval);
        }
        Ok(())
    }

    async fn commit_if_due(&mut self, force: bool) -> IcebergEmitterResult<Option<PublishReport>> {
        if self.staged_batches.is_empty() {
            self.commit_at = None;
            return Ok(None);
        }
        let now = Instant::now();
        let time_due = self.commit_at.is_some_and(|deadline| deadline <= now);
        let size_due = self.staged_bytes >= self.commit_policy.max_size;
        if !force && !time_due && !size_due {
            return Ok(None);
        }
        let paths = self
            .staged_batches
            .iter()
            .map(|batch| batch.path.clone())
            .collect::<Vec<_>>();
        let batch = match Self::read_ipc_batches(self.mapped_schema.clone(), paths.as_slice()).await
        {
            Ok(batch) => batch,
            Err(error) => {
                self.commit_at = Some(Instant::now() + Duration::from_secs(1));
                return Err(error);
            }
        };
        if let Err(error) = self.client.write_batch(batch).await {
            self.commit_at = Some(Instant::now() + Duration::from_secs(1));
            return Err(error);
        }
        let staged = std::mem::take(&mut self.staged_batches);
        let messages = staged
            .iter()
            .map(|batch| batch.rows)
            .fold(0_u64, u64::saturating_add);
        let bytes = staged
            .iter()
            .map(|batch| batch.bytes)
            .fold(0_u64, u64::saturating_add);
        let domain_timestamp = staged
            .iter()
            .map(|batch| batch.domain_timestamp)
            .max()
            .unwrap_or_else(current_timestamp);
        let mut acks = Vec::new();
        for batch in staged {
            acks.extend(batch.acks);
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
        self.commit_at = None;
        for ack in acks {
            ack.ack_success();
        }
        Ok(Some(PublishReport::flushed(
            messages,
            bytes,
            domain_timestamp,
        )))
    }

    async fn mapped_arrow_batch_from_runtime_batch(
        &self,
        batch: &RuntimeRecordBatch,
        metadata: Vec<RuntimeRecordMetadata>,
        keys: Vec<Option<BranchKey>>,
    ) -> IcebergEmitterResult<RecordBatch> {
        let decoded = self
            .input_schema
            .decoded_records_from_arrow_batch(batch)
            .map_err(|error| Report::new(IcebergEmitterError::MapBatch).attach_printable(error))?;
        if decoded.len() != metadata.len() {
            return Err(
                Report::new(IcebergEmitterError::MapBatch).attach_printable(format!(
                    "metadata count {} does not match row count {}",
                    metadata.len(),
                    decoded.len()
                )),
            );
        }
        if decoded.len() != keys.len() {
            return Err(
                Report::new(IcebergEmitterError::MapBatch).attach_printable(format!(
                    "branch key count {} does not match row count {}",
                    keys.len(),
                    decoded.len()
                )),
            );
        }
        let records = decoded
            .into_iter()
            .zip(metadata)
            .map(|(record, metadata)| record.into_runtime_record(metadata))
            .collect::<Vec<_>>();
        let records = augment_runtime_records_with_branch_keys(records, &keys)
            .map_err(|error| Report::new(IcebergEmitterError::MapBatch).attach_printable(error))?;
        let input =
            vm_typed_batch_from_runtime_records(&records, &self.program.program.input_schema)
                .map_err(|error| {
                    Report::new(IcebergEmitterError::MapBatch).attach_printable(error)
                })?;
        let result = execute_program_with_selection_in_context(
            self.program.program.as_ref(),
            &input,
            &VmExecutionContext {
                now: current_timestamp(),
                injector: None,
            },
        )
        .await
        .map_err(|error| {
            Report::new(IcebergEmitterError::MapBatch).attach_printable(error.to_string())
        })?;
        if result.batch.row_count() != batch.batch().num_rows() {
            return Err(
                Report::new(IcebergEmitterError::MapBatch).attach_printable(format!(
                    "VALUES produced {} rows for {} staged records",
                    result.batch.row_count(),
                    batch.batch().num_rows()
                )),
            );
        }
        if let Some(side_error) = result.batch.errors().iter().flatten().next() {
            return Err(
                Report::new(IcebergEmitterError::MapBatch).attach_printable(format!(
                    "VALUES side error {}: {} at {}",
                    side_error.code.as_str(),
                    side_error.message,
                    side_error.span
                )),
            );
        }
        let columns = result
            .batch
            .columns()
            .iter()
            .zip(self.mapped_schema.fields())
            .map(|(array, field)| Self::typed_array_to_array_ref(array, field.data_type()))
            .collect::<Vec<_>>();
        RecordBatch::try_new(self.mapped_schema.clone(), columns)
            .change_context(IcebergEmitterError::MapBatch)
    }

    fn typed_array_to_array_ref(
        array: &VmTypedArray,
        data_type: &arrow_schema::DataType,
    ) -> arrow_array::ArrayRef {
        match (array, data_type) {
            (
                VmTypedArray::Datetime(values),
                arrow_schema::DataType::Timestamp(
                    arrow_schema::TimeUnit::Microsecond,
                    Some(timezone),
                ),
            ) if timezone.as_ref() == "+00:00" || timezone.as_ref() == "UTC" => StdArc::new(
                values
                    .iter()
                    .map(|value| value.map(|nanos| nanos.div_euclid(1_000)))
                    .collect::<arrow_array::TimestampMicrosecondArray>()
                    .with_timezone_utc(),
            ),
            _ => Self::typed_array_to_native_array_ref(array),
        }
    }

    fn typed_array_to_native_array_ref(array: &VmTypedArray) -> arrow_array::ArrayRef {
        array.to_array_ref()
    }

    fn next_staged_path(&mut self) -> PathBuf {
        self.pending_sequence = self.pending_sequence.saturating_add(1);
        self.staging_dir
            .path()
            .join(format!("batch-{}.arrow", self.pending_sequence))
    }

    async fn write_ipc_batch(path: PathBuf, batch: RecordBatch) -> IcebergEmitterResult<u64> {
        tokio::task::spawn_blocking(move || {
            let file = File::create(&path)
                .change_context(IcebergEmitterError::WriteStagedIpc)
                .attach_printable(format!("path: {}", path.display()))?;
            let mut writer = StreamWriter::try_new(file, batch.schema().as_ref())
                .change_context(IcebergEmitterError::WriteStagedIpc)
                .attach_printable(format!("path: {}", path.display()))?;
            writer
                .write(&batch)
                .change_context(IcebergEmitterError::WriteStagedIpc)
                .attach_printable(format!("path: {}", path.display()))?;
            writer
                .finish()
                .change_context(IcebergEmitterError::WriteStagedIpc)
                .attach_printable(format!("path: {}", path.display()))?;
            std::fs::metadata(&path)
                .map(|metadata| metadata.len())
                .change_context(IcebergEmitterError::WriteStagedIpc)
                .attach_printable(format!("path: {}", path.display()))
        })
        .await
        .change_context(IcebergEmitterError::WriteStagedIpc)?
    }

    async fn read_ipc_batches(
        schema: StdArc<arrow_schema::Schema>,
        paths: &[PathBuf],
    ) -> IcebergEmitterResult<RecordBatch> {
        let paths = paths.to_vec();
        tokio::task::spawn_blocking(move || {
            let mut batches = Vec::new();
            for path in paths {
                let file = File::open(&path)
                    .change_context(IcebergEmitterError::ReadStagedIpc)
                    .attach_printable(format!("path: {}", path.display()))?;
                let reader = StreamReader::try_new(file, None)
                    .change_context(IcebergEmitterError::ReadStagedIpc)
                    .attach_printable(format!("path: {}", path.display()))?;
                if reader.schema().as_ref() != schema.as_ref() {
                    return Err(
                        Report::new(IcebergEmitterError::ReadStagedIpc).attach_printable(format!(
                            "path {} schema does not match",
                            path.display()
                        )),
                    );
                }
                let path_batches = reader
                    .collect::<Result<Vec<_>, _>>()
                    .change_context(IcebergEmitterError::ReadStagedIpc)
                    .attach_printable(format!("path: {}", path.display()))?;
                batches.extend(path_batches);
            }
            Self::concat_arrow_batches(schema, batches)
        })
        .await
        .change_context(IcebergEmitterError::ReadStagedIpc)?
    }

    fn concat_arrow_batches(
        schema: StdArc<arrow_schema::Schema>,
        batches: Vec<RecordBatch>,
    ) -> IcebergEmitterResult<RecordBatch> {
        let Some(first) = batches.first() else {
            return Err(Report::new(IcebergEmitterError::ReadStagedIpc)
                .attach_printable("cannot commit zero staged Iceberg batches"));
        };
        if first.schema().as_ref() != schema.as_ref()
            || batches
                .iter()
                .any(|batch| batch.schema().as_ref() != schema.as_ref())
        {
            return Err(Report::new(IcebergEmitterError::ReadStagedIpc)
                .attach_printable("staged Iceberg batch schemas do not match"));
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
                        .change_context(IcebergEmitterError::ReadStagedIpc)?,
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
        .change_context(IcebergEmitterError::ReadStagedIpc)
    }
}

impl IcebergEmitterClient {
    async fn write_batch(&mut self, batch: RecordBatch) -> IcebergEmitterResult<()> {
        let location_generator = DefaultLocationGenerator::new(self.table.metadata().clone())
            .change_context(IcebergEmitterError::Commit)?;
        self.data_file_sequence = self.data_file_sequence.saturating_add(1);
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
            .change_context(IcebergEmitterError::Commit)?;
        writer
            .write(batch)
            .await
            .change_context(IcebergEmitterError::Commit)?;
        let data_files = writer
            .close()
            .await
            .change_context(IcebergEmitterError::Commit)?;
        let tx = Transaction::new(&self.table);
        let action = tx.fast_append().add_data_files(data_files);
        let tx = action
            .apply(tx)
            .change_context(IcebergEmitterError::Commit)?;
        self.table = tx
            .commit(self.catalog.as_ref())
            .await
            .change_context(IcebergEmitterError::Commit)?;
        Ok(())
    }
}

impl IcebergObjectStoreProperties {
    fn from_entries(
        backend: IcebergStorageBackend,
        config: &[nervix_models::ClientConfigEntry],
    ) -> Self {
        let mut props = HashMap::new();
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
        catalog_config: &[nervix_models::ClientConfigEntry],
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
    use ::iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
    use arrow_array::{Array, TimestampMicrosecondArray, TimestampNanosecondArray};
    use arrow_schema::{DataType, Field, TimeUnit};

    use super::*;

    #[test]
    fn iceberg_schema_uses_microsecond_utc_timestamps() {
        let field = Field::new(
            "observed_at",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            true,
        );

        let iceberg_field = IcebergEmitter::iceberg_arrow_field(&field, "observed_at");

        assert_eq!(
            iceberg_field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        );
        let schema = arrow_schema::Schema::new(vec![iceberg_field]);
        let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(&schema)
            .expect("microsecond timestamp schema must convert to Iceberg");
        let serialized =
            serde_json::to_string(&iceberg_schema).expect("Iceberg schema must serialize");
        assert!(serialized.contains("timestamptz"));
        assert!(!serialized.contains("timestamptz_ns"));
    }

    #[test]
    fn iceberg_datetime_arrays_are_converted_to_microseconds() {
        let values = TimestampNanosecondArray::from(vec![Some(1_234_567), None, Some(-1)])
            .with_timezone_utc();

        let converted = IcebergEmitter::typed_array_to_array_ref(
            &VmTypedArray::Datetime(values),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
        );

        assert_eq!(
            converted.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        );
        let converted = converted
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("Iceberg datetime column must be a microsecond timestamp array");
        assert_eq!(converted.value(0), 1_234);
        assert!(converted.is_null(1));
        assert_eq!(converted.value(2), -1);
    }
}
