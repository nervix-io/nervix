//! MongoDB sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The node's shared MongoDB client and its TLS and pool options, the database its
//!   configuration names, the BSON document each mapped row becomes, the mapped values it has no
//!   BSON representation for, the upsert model of a conflict action, and write-error
//!   classification.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio
//!   and the `mongodb` driver.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use std::path::PathBuf;

use ahash::HashMap;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, ListArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use async_trait::async_trait;
use chrono::DateTime;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use mongodb::{
    Client as DriverClient, Namespace,
    bson::{Bson, Document, doc},
    error::{Error as DriverError, ErrorKind, PartialBulkWriteResult},
    options::{
        ClientOptions, Tls, TlsOptions, UpdateOneModel as MongoDbUpdateOneModel,
        WriteModel as MongoDbWriteModel,
    },
};
use nervix_connector::{
    MappedSinkRows, PerRecordOutcome, RejectedSinkRecord, RowSink, SinkHost, SinkLifecycle,
    SinkPublishError, SinkPublishResult, SinkRecordPosition, SinkStartError, SinkStartResult,
    optional_client_config_value,
};
use nervix_models::{
    ClientConfigEntry, ClientPoolBounds, CollectionName, FieldPath, Timestamp,
};
use tracing::trace;

const MONGODB: &str = "mongodb";

/// The node's shared MongoDB client for one named client, whose driver pools per server.
#[derive(Clone)]
pub struct MongoDbClient(DriverClient);

/// The node-owned client this sink writes through while its host holds the lease.
pub trait MongoDbClientSource: Send + Sync + 'static {
    fn client(&self) -> SinkPublishResult<MongoDbClient>;
}

/// What one MongoDB emitter writes with, from its typed sink plan.
pub struct MongoDbSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub collection: CollectionName,
    pub conflict_action: MongoDbConflictAction,
}

/// What a write does with a document the target collection already holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MongoDbConflictAction {
    None,
    DoNothing { target: Vec<String> },
    DoUpdate { target: Vec<String> },
}

/// The MongoDB sink, which encodes each mapped row as one BSON document.
pub struct MongoDbSink {
    client: Box<dyn MongoDbClientSource>,
    database: String,
    collection: CollectionName,
    conflict_action: MongoDbConflictAction,
}

impl MongoDbClient {
    /// Open the node's shared MongoDB client for one named client, sized by its declared bounds.
    ///
    /// The driver keeps one application pool per server in the topology and applies both bounds to
    /// each, so the ceiling is enforced by the driver rather than by any emitter counting its own.
    pub async fn open(
        config: &[ClientConfigEntry],
        bounds: ClientPoolBounds,
    ) -> SinkStartResult<Self> {
        let invalid = || Report::new(SinkStartError::InvalidConfiguration { sink: MONGODB });
        let Some(addr) = optional_client_config_value(config, "addr") else {
            return Err(invalid().attach_printable("missing MongoDB client config key 'addr'"));
        };
        let mut options = ClientOptions::parse(addr).await.map_err(|source| {
            invalid().attach_printable(format!("failed to parse client addr: {source}"))
        })?;
        if let Some(ca_file) = optional_client_config_value(config, "tls_ca_file") {
            options.tls = Some(Tls::Enabled(
                TlsOptions::builder()
                    .ca_file_path(PathBuf::from(ca_file))
                    .build(),
            ));
        }
        options.min_pool_size = Some(bounds.minimum());
        options.max_pool_size = Some(bounds.maximum().get());
        let client = DriverClient::with_options(options)
            .map_err(|source| invalid().attach_printable(source.to_string()))?;
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|source| {
                Report::new(SinkStartError::Initialize { sink: MONGODB })
                    .attach_printable(format!("failed to validate connection: {source}"))
            })?;
        Ok(Self(client))
    }
}

/// The database an emitter writes into, from the client's own configuration.
///
/// The database belongs to the client's configuration rather than to its connections, so it is
/// read per user while the driver client itself is shared.
fn database_name(config: &[ClientConfigEntry]) -> SinkStartResult<String> {
    if let Some(database) = optional_client_config_value(config, "database") {
        return Ok(database.to_owned());
    }
    let invalid = || Report::new(SinkStartError::InvalidConfiguration { sink: MONGODB });
    let missing = || invalid().attach_printable("missing MongoDB client config key 'database'");
    let Some(addr) = optional_client_config_value(config, "addr") else {
        return Err(invalid().attach_printable("missing MongoDB client config key 'addr'"));
    };
    let Some((_, tail)) = addr.rsplit_once('/') else {
        return Err(missing());
    };
    let database = tail.split('?').next().unwrap_or_default();
    if database.is_empty() {
        Err(missing())
    } else {
        Ok(database.to_string())
    }
}

/// A write code the server reports for a document it will never accept.
fn is_record_write_error(code: i32) -> bool {
    matches!(code, 121 | 10334 | 11000)
}

fn safe_infrastructure_error(operation: &str) -> Report<SinkPublishError> {
    Report::new(SinkPublishError::Publish { sink: MONGODB })
        .attach_printable(format!("MongoDB {operation} failed"))
}

impl MongoDbSink {
    pub fn new(
        config: MongoDbSinkConfig,
        client: Box<dyn MongoDbClientSource>,
        _host: SinkHost,
    ) -> SinkStartResult<Self> {
        let MongoDbSinkConfig {
            config,
            collection,
            conflict_action,
        } = config;
        Ok(Self {
            client,
            database: database_name(&config)?,
            collection,
            conflict_action,
        })
    }

    fn conflict_filter(document: &Document, target: &[String]) -> SinkPublishResult<Document> {
        let mut filter = Document::new();
        for field in target {
            let Some(value) = document.get(field) else {
                return Err(Report::new(SinkPublishError::Publish { sink: MONGODB })
                    .attach_printable(format!(
                        "MongoDB ON CONFLICT target field '{field}' is missing from VALUES \
                         document"
                    )));
            };
            filter.insert(field.clone(), value.clone());
        }
        Ok(filter)
    }

    fn conflict_update_model(
        namespace: &Namespace,
        document: Document,
        conflict_action: &MongoDbConflictAction,
    ) -> SinkPublishResult<MongoDbWriteModel> {
        let (filter, update) = match conflict_action {
            MongoDbConflictAction::None => {
                return Err(Report::new(SinkPublishError::Publish { sink: MONGODB })
                    .attach_printable("MongoDB bulk update requires an ON CONFLICT action"));
            }
            MongoDbConflictAction::DoNothing { target } => {
                let filter = Self::conflict_filter(&document, target)?;
                (filter, doc! { "$setOnInsert": document })
            }
            MongoDbConflictAction::DoUpdate { target } => {
                let filter = Self::conflict_filter(&document, target)?;
                let target_fields = target
                    .iter()
                    .map(String::as_str)
                    .collect::<std::collections::BTreeSet<_>>();
                let mut set = Document::new();
                let mut set_on_insert = Document::new();
                for (key, value) in document {
                    if target_fields.contains(key.as_str()) {
                        set_on_insert.insert(key, value);
                    } else {
                        set.insert(key, value);
                    }
                }
                if set.is_empty() {
                    return Err(Report::new(SinkPublishError::Publish { sink: MONGODB })
                        .attach_printable(
                            "MongoDB ON CONFLICT DO UPDATE requires at least one non-conflict \
                             VALUES field to update",
                        ));
                }
                (
                    filter,
                    doc! {
                        "$set": set,
                        "$setOnInsert": set_on_insert,
                    },
                )
            }
        };
        Ok(MongoDbUpdateOneModel::builder()
            .namespace(namespace.clone())
            .filter(filter)
            .update(update)
            .upsert(true)
            .build()
            .into())
    }

    fn apply_insert_many_error(
        batch_index: usize,
        chunk: &[usize],
        occurred_at: Timestamp,
        error: DriverError,
        outcome: &mut PerRecordOutcome,
    ) {
        let ErrorKind::InsertMany(insert_error) = error.kind.as_ref() else {
            outcome.fail(safe_infrastructure_error("insert_many request"));
            return;
        };
        if insert_error.write_concern_error.is_some() {
            outcome.fail(safe_infrastructure_error("insert_many write concern"));
            return;
        }
        let Some(write_errors) = insert_error.write_errors.as_ref() else {
            outcome.fail(safe_infrastructure_error("insert_many request"));
            return;
        };
        let errors = write_errors
            .iter()
            .map(|error| (error.index, error.code))
            .collect::<Vec<_>>();
        Self::apply_insert_many_write_errors(batch_index, chunk, occurred_at, &errors, outcome);
    }

    fn apply_insert_many_write_errors(
        batch_index: usize,
        chunk: &[usize],
        occurred_at: Timestamp,
        write_errors: &[(usize, i32)],
        outcome: &mut PerRecordOutcome,
    ) {
        let errors = write_errors.iter().copied().collect::<HashMap<_, _>>();
        let mut has_infrastructure_error = errors.len() != write_errors.len()
            || write_errors
                .iter()
                .any(|(index, code)| *index >= chunk.len() || !is_record_write_error(*code));
        for (local_index, row) in chunk.iter().enumerate() {
            let position = SinkRecordPosition {
                batch_index,
                row_index: *row,
            };
            if let Some(code) = errors.get(&local_index) {
                if is_record_write_error(*code) {
                    outcome.reject(RejectedSinkRecord::external(
                        position,
                        occurred_at,
                        format!("MongoDB rejected document with code {code}"),
                    ));
                } else {
                    has_infrastructure_error = true;
                }
            } else {
                outcome.deliver(position);
            }
        }
        if has_infrastructure_error {
            outcome.fail(safe_infrastructure_error("insert_many request"));
        }
    }

    fn apply_bulk_write_error(
        batch_index: usize,
        chunk: &[usize],
        occurred_at: Timestamp,
        error: DriverError,
        outcome: &mut PerRecordOutcome,
    ) {
        let ErrorKind::BulkWrite(bulk_error) = error.kind.as_ref() else {
            outcome.fail(safe_infrastructure_error("bulk write request"));
            return;
        };
        if !bulk_error.write_concern_errors.is_empty() {
            outcome.fail(safe_infrastructure_error("bulk write concern"));
            return;
        }
        let mut accounted = vec![false; chunk.len()];
        if let Some(PartialBulkWriteResult::Verbose(result)) = bulk_error.partial_result.as_ref() {
            for local_index in result
                .insert_results
                .keys()
                .chain(result.update_results.keys())
                .chain(result.delete_results.keys())
            {
                if let Some(row) = chunk.get(*local_index) {
                    outcome.deliver(SinkRecordPosition {
                        batch_index,
                        row_index: *row,
                    });
                    accounted[*local_index] = true;
                }
            }
        }
        let mut has_infrastructure_error = false;
        for (local_index, error) in &bulk_error.write_errors {
            let Some(row) = chunk.get(*local_index) else {
                has_infrastructure_error = true;
                continue;
            };
            if is_record_write_error(error.code) {
                outcome.reject(RejectedSinkRecord::external(
                    SinkRecordPosition {
                        batch_index,
                        row_index: *row,
                    },
                    occurred_at,
                    format!("MongoDB rejected document with code {}", error.code),
                ));
                accounted[*local_index] = true;
            } else {
                has_infrastructure_error = true;
            }
        }
        if has_infrastructure_error || accounted.iter().any(|accounted| !accounted) {
            outcome.fail(safe_infrastructure_error("bulk write request"));
        }
    }
}

#[async_trait]
impl SinkLifecycle for MongoDbSink {}

#[async_trait]
impl RowSink for MongoDbSink {
    async fn publish(&mut self, rows: MappedSinkRows<'_>) -> PerRecordOutcome {
        let mut outcome = PerRecordOutcome::with_capacity(rows.selected_rows.len());
        let columns = match MappedBsonColumns::new(rows.batch, rows.target_columns) {
            Ok(columns) => columns,
            Err(error) => {
                outcome.fail(
                    Report::new(SinkPublishError::Publish { sink: MONGODB })
                        .attach_printable(error.to_string()),
                );
                return outcome;
            }
        };
        let client = match self.client.client() {
            Ok(client) => client,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        let database = client.0.database(&self.database);
        let collection_names = match database
            .list_collection_names()
            .filter(doc! { "name": self.collection.as_str() })
            .authorized_collections(true)
            .await
        {
            Ok(collection_names) => collection_names,
            Err(_) => {
                outcome.fail(safe_infrastructure_error("collection lookup request"));
                return outcome;
            }
        };
        if collection_names.is_empty() {
            outcome.fail(
                Report::new(SinkPublishError::Publish { sink: MONGODB })
                    .attach_printable("MongoDB target collection is not provisioned"),
            );
            return outcome;
        }
        let mongodb_collection = database.collection::<Document>(self.collection.as_str());
        let namespace = Namespace::new(&self.database, self.collection.as_str());
        for chunk in rows.selected_row_chunks {
            tokio::task::consume_budget().await;
            let Some(chunk_rows) = rows.selected_rows.get(chunk.clone()) else {
                outcome.fail(
                    Report::new(SinkPublishError::Publish { sink: MONGODB }).attach_printable(
                        format!(
                            "MongoDB chunk {chunk:?} is outside its {} selected rows",
                            rows.selected_rows.len()
                        ),
                    ),
                );
                return outcome;
            };
            let mut documents = Vec::with_capacity(chunk_rows.len());
            let mut encoded_rows = Vec::with_capacity(chunk_rows.len());
            for row in chunk_rows {
                let position = SinkRecordPosition {
                    batch_index: rows.batch_index,
                    row_index: *row,
                };
                match columns.document(*row) {
                    Ok(document) => {
                        documents.push(document);
                        encoded_rows.push(*row);
                    }
                    Err(unencodable) => {
                        outcome.reject(unencodable.rejected(position, rows.occurred_at));
                    }
                }
            }
            // A chunk whose every row was rejected leaves no document for this write to carry.
            if documents.is_empty() {
                continue;
            }
            match &self.conflict_action {
                MongoDbConflictAction::None => {
                    match mongodb_collection
                        .insert_many(documents)
                        .ordered(false)
                        .await
                    {
                        Ok(_) => {
                            for row in &encoded_rows {
                                outcome.deliver(SinkRecordPosition {
                                    batch_index: rows.batch_index,
                                    row_index: *row,
                                });
                            }
                        }
                        Err(error) => {
                            Self::apply_insert_many_error(
                                rows.batch_index,
                                &encoded_rows,
                                rows.occurred_at,
                                error,
                                &mut outcome,
                            );
                            if outcome.has_infrastructure_error() {
                                return outcome;
                            }
                        }
                    }
                }
                MongoDbConflictAction::DoNothing { .. }
                | MongoDbConflictAction::DoUpdate { .. } => {
                    let models = documents
                        .into_iter()
                        .map(|document| {
                            Self::conflict_update_model(&namespace, document, &self.conflict_action)
                        })
                        .collect::<SinkPublishResult<Vec<_>>>();
                    let models = match models {
                        Ok(models) => models,
                        Err(error) => {
                            outcome.fail(error);
                            return outcome;
                        }
                    };
                    match client
                        .0
                        .bulk_write(models)
                        .ordered(false)
                        .verbose_results()
                        .await
                    {
                        Ok(_) => {
                            for row in &encoded_rows {
                                outcome.deliver(SinkRecordPosition {
                                    batch_index: rows.batch_index,
                                    row_index: *row,
                                });
                            }
                        }
                        Err(error) => {
                            Self::apply_bulk_write_error(
                                rows.batch_index,
                                &encoded_rows,
                                rows.occurred_at,
                                error,
                                &mut outcome,
                            );
                            if outcome.has_infrastructure_error() {
                                return outcome;
                            }
                        }
                    }
                }
            }
        }
        trace!(
            collection = self.collection.as_str(),
            rows = rows.selected_rows.len(),
            "emitter published mongodb documents"
        );
        outcome
    }
}

/// A mapped value this sink has no BSON representation for, named with the field that carries it.
#[derive(Debug, thiserror::Error)]
enum UnencodableMappedValue {
    #[error("MongoDB VALUES field '{field}' has unsupported exact type {data_type}")]
    UnsupportedColumn {
        field: String,
        data_type: arrow_schema::DataType,
    },
    #[error(
        "MongoDB VALUES field '{field}' holds an unsigned integer above the BSON signed 64-bit \
         range"
    )]
    UnsignedIntegerRange { field: String },
}

impl UnencodableMappedValue {
    /// The rejection the host delivers for the row this value came from.
    ///
    /// The rejection names the field and what MongoDB has no representation for, never the value
    /// itself, so a rejected payload reaches neither an error route nor a log. It is definitive,
    /// so the record follows `ON MESSAGE ERROR` instead of entering the host's retry loop.
    fn rejected(&self, position: SinkRecordPosition, occurred_at: Timestamp) -> RejectedSinkRecord {
        let field = match self {
            Self::UnsupportedColumn { field, .. } | Self::UnsignedIntegerRange { field } => field,
        };
        RejectedSinkRecord::invalid(
            position,
            occurred_at,
            self.to_string(),
            [FieldPath::new(format!("{MONGODB}.{field}"))],
        )
    }
}

/// The mapped columns of one batch, downcast once so every document reads from the column that
/// holds its values.
struct MappedBsonColumns<'a> {
    fields: Vec<MappedBsonField<'a>>,
}

/// One target field of the document: its name and the Arrow column its values come from.
struct MappedBsonField<'a> {
    name: String,
    values: MappedBsonColumn<'a>,
}

impl<'a> MappedBsonColumns<'a> {
    fn new(
        batch: &'a RecordBatch,
        target_columns: &[String],
    ) -> Result<Self, UnencodableMappedValue> {
        let mut fields = Vec::with_capacity(target_columns.len());
        for (index, column) in target_columns.iter().enumerate() {
            let array = batch.column(index);
            let values = MappedBsonColumn::new(array).ok_or_else(|| {
                UnencodableMappedValue::UnsupportedColumn {
                    field: column.clone(),
                    data_type: array.data_type().clone(),
                }
            })?;
            fields.push(MappedBsonField {
                name: column.clone(),
                values,
            });
        }
        Ok(Self { fields })
    }

    /// One document, read field by field at the row the host selected.
    ///
    /// A field this row holds no BSON value for rejects the whole document, so a record is never
    /// written carrying a value it does not have.
    fn document(&self, row: usize) -> Result<Document, UnencodableMappedValue> {
        let mut document = Document::new();
        for field in &self.fields {
            let value = field.values.value(row, &field.name)?;
            document.insert(field.name.clone(), value);
        }
        Ok(document)
    }
}

/// One mapped column, held as the typed Arrow array it arrived in.
enum MappedBsonColumn<'a> {
    Bool(&'a BooleanArray),
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
    String(&'a StringArray),
    Datetime(&'a TimestampNanosecondArray),
    List {
        offsets: &'a ListArray,
        elements: Box<MappedBsonColumn<'a>>,
    },
}

impl<'a> MappedBsonColumn<'a> {
    fn new(array: &'a ArrayRef) -> Option<Self> {
        let array = array.as_ref();
        if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
            return Some(Self::Bool(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
            return Some(Self::U8(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
            return Some(Self::I8(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
            return Some(Self::U16(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
            return Some(Self::I16(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
            return Some(Self::U32(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
            return Some(Self::I32(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
            return Some(Self::U64(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
            return Some(Self::I64(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
            return Some(Self::F32(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
            return Some(Self::F64(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            return Some(Self::String(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            return Some(Self::Datetime(values));
        }
        let values = array.as_any().downcast_ref::<ListArray>()?;
        let elements = Self::new(values.values())?;
        Some(Self::List {
            offsets: values,
            elements: Box::new(elements),
        })
    }

    /// The BSON value one row carries, read from the column that holds it.
    ///
    /// Every integer width writes as a 64-bit integer, and an unsigned value above that range has
    /// no BSON integer at all, so its row is rejected rather than written as a value it is not.
    /// The field names the mapped column this value belongs to, including for a list element,
    /// whose failure belongs to the list its row carries.
    fn value(&self, row: usize, field: &str) -> Result<Bson, UnencodableMappedValue> {
        if self.is_null(row) {
            return Ok(Bson::Null);
        }
        let value = match self {
            Self::Bool(values) => Bson::Boolean(values.value(row)),
            Self::U8(values) => Bson::Int64(i64::from(values.value(row))),
            Self::I8(values) => Bson::Int64(i64::from(values.value(row))),
            Self::U16(values) => Bson::Int64(i64::from(values.value(row))),
            Self::I16(values) => Bson::Int64(i64::from(values.value(row))),
            Self::U32(values) => Bson::Int64(i64::from(values.value(row))),
            Self::I32(values) => Bson::Int64(i64::from(values.value(row))),
            Self::U64(values) => {
                let Ok(value) = i64::try_from(values.value(row)) else {
                    return Err(UnencodableMappedValue::UnsignedIntegerRange {
                        field: field.to_owned(),
                    });
                };
                Bson::Int64(value)
            }
            Self::I64(values) => Bson::Int64(values.value(row)),
            Self::F32(values) => Bson::Double(f64::from(values.value(row))),
            Self::F64(values) => Bson::Double(values.value(row)),
            Self::String(values) => Bson::String(values.value(row).to_string()),
            Self::Datetime(values) => Bson::String(
                DateTime::from_timestamp_nanos(values.value(row))
                    .fixed_offset()
                    .to_rfc3339(),
            ),
            Self::List { offsets, elements } => {
                let offsets = offsets.value_offsets();
                let non_negative = "Arrow builds list offsets as non-negative element positions";
                let start = usize::try_from(offsets[row]).assured(non_negative);
                let end = usize::try_from(
                    offsets[row.checked_add(1).assured(
                        "a list array holds one offset more than it holds rows, so the position \
                         after the last row is addressable",
                    )],
                )
                .assured(non_negative);
                let mut items = Vec::with_capacity(end.checked_sub(start).assured(
                    "Arrow list offsets increase, so a row ends no earlier than it starts",
                ));
                for element in start..end {
                    items.push(elements.value(element, field)?);
                }
                Bson::Array(items)
            }
        };
        Ok(value)
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Bool(values) => values.is_null(row),
            Self::U8(values) => values.is_null(row),
            Self::I8(values) => values.is_null(row),
            Self::U16(values) => values.is_null(row),
            Self::I16(values) => values.is_null(row),
            Self::U32(values) => values.is_null(row),
            Self::I32(values) => values.is_null(row),
            Self::U64(values) => values.is_null(row),
            Self::I64(values) => values.is_null(row),
            Self::F32(values) => values.is_null(row),
            Self::F64(values) => values.is_null(row),
            Self::String(values) => values.is_null(row),
            Self::Datetime(values) => values.is_null(row),
            Self::List { offsets, .. } => offsets.is_null(row),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use arrow_array::types::UInt64Type;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use nervix_models::{MessageErrorCode, MessageErrorOperation};

    use super::*;

    #[test]
    fn classifies_only_definitive_document_write_codes_as_record_errors() {
        for code in [121, 10334, 11000] {
            assert!(
                is_record_write_error(code),
                "{code} should be a definitive document error"
            );
        }
        for code in [2, 6, 7, 50, 64, 89, 91, 11600, 11602, 16755] {
            assert!(
                !is_record_write_error(code),
                "{code} requires infrastructure retry"
            );
        }
    }

    #[test]
    fn mixed_mongodb_write_results_preserve_successes_and_record_rejections() {
        let mut outcome = PerRecordOutcome::empty();

        MongoDbSink::apply_insert_many_write_errors(
            7,
            &[10, 11, 12],
            Timestamp::from_unix_nanos(3),
            &[(1, 11000), (2, 91)],
            &mut outcome,
        );

        let outcome = outcome.into_parts();
        assert_eq!(
            outcome.delivered,
            [SinkRecordPosition {
                batch_index: 7,
                row_index: 10,
            }]
        );
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(
            outcome.rejected[0].position,
            SinkRecordPosition {
                batch_index: 7,
                row_index: 11,
            }
        );
        assert!(outcome.infrastructure_error.is_some());
    }

    #[test]
    fn encodes_each_mapped_field_from_the_row_it_holds() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("at", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![Some(7), None])),
                StdArc::new(Float64Array::from(vec![Some(1.5), Some(2.0)])),
                StdArc::new(StringArray::from(vec![Some("first"), Some("second")])),
                StdArc::new(TimestampNanosecondArray::from(vec![
                    Some(1_700_000_000_123_456_789),
                    None,
                ])),
            ],
        )
        .expect("the mapped batch should build");
        let names = [
            "id".to_string(),
            "score".to_string(),
            "name".to_string(),
            "at".to_string(),
        ];

        let columns = MappedBsonColumns::new(&batch, &names).expect("columns should be mapped");

        assert_eq!(
            columns.document(0).expect("the first row should encode"),
            doc! {
                "id": 7_i64,
                "score": 1.5_f64,
                "name": "first",
                "at": "2023-11-14T22:13:20.123456789+00:00",
            }
        );
        assert_eq!(
            columns.document(1).expect("the second row should encode"),
            doc! {
                "id": Bson::Null,
                "score": 2.0_f64,
                "name": "second",
                "at": Bson::Null,
            }
        );
    }

    #[test]
    fn rejects_only_the_row_whose_unsigned_value_has_no_bson_integer() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("count", DataType::UInt64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(UInt64Array::from(vec![
                    Some(u64::try_from(i64::MAX).expect("the signed maximum is unsigned")),
                    Some(u64::MAX),
                    None,
                ])),
                StdArc::new(StringArray::from(vec![
                    Some("representable"),
                    Some("out-of-range"),
                    Some("genuine-null"),
                ])),
            ],
        )
        .expect("the mapped batch should build");
        let names = ["count".to_string(), "name".to_string()];

        let columns = MappedBsonColumns::new(&batch, &names).expect("columns should be mapped");

        assert_eq!(
            columns.document(0).expect("the signed maximum should encode"),
            doc! { "count": i64::MAX, "name": "representable" }
        );
        let rejected = columns
            .document(1)
            .expect_err("an unsigned value above the signed range has no BSON integer");
        assert!(matches!(
            rejected,
            UnencodableMappedValue::UnsignedIntegerRange { ref field } if field == "count"
        ));
        assert_eq!(
            columns.document(2).expect("a genuine null should encode"),
            doc! { "count": Bson::Null, "name": "genuine-null" }
        );
    }

    #[test]
    fn reports_a_rejected_value_without_the_value_it_refused() {
        let rejected = UnencodableMappedValue::UnsignedIntegerRange {
            field: "mongodb_user_id".to_string(),
        };

        let record = rejected.rejected(
            SinkRecordPosition {
                batch_index: 2,
                row_index: 5,
            },
            Timestamp::from_unix_nanos(11),
        );

        assert_eq!(
            record.position,
            SinkRecordPosition {
                batch_index: 2,
                row_index: 5,
            }
        );
        assert_eq!(record.error.code, MessageErrorCode::Validation);
        assert_eq!(record.error.operation, MessageErrorOperation::Values);
        assert_eq!(
            record
                .error
                .fields
                .iter()
                .map(FieldPath::as_str)
                .collect::<Vec<_>>(),
            ["mongodb.mongodb_user_id"]
        );
        assert_eq!(
            record.error.message,
            "MongoDB VALUES field 'mongodb_user_id' holds an unsigned integer above the BSON \
             signed 64-bit range"
        );
    }

    #[test]
    fn rejects_the_row_whose_nested_unsigned_element_has_no_bson_integer() {
        let counts = ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![
            Some(vec![Some(1_u64), Some(2_u64)]),
            Some(vec![Some(3_u64), Some(u64::MAX)]),
        ]);
        let schema = StdArc::new(Schema::new(vec![Field::new(
            "counts",
            counts.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![StdArc::new(counts)])
            .expect("the mapped batch should build");
        let names = ["counts".to_string()];

        let columns = MappedBsonColumns::new(&batch, &names).expect("columns should be mapped");

        assert_eq!(
            columns
                .document(0)
                .expect("a list of representable elements should encode"),
            doc! { "counts": [1_i64, 2_i64] }
        );
        let rejected = columns
            .document(1)
            .expect_err("a list element above the signed range has no BSON integer");
        assert!(matches!(
            rejected,
            UnencodableMappedValue::UnsignedIntegerRange { ref field } if field == "counts"
        ));
    }
}
