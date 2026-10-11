//! MongoDB sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The node's shared MongoDB client and its TLS and pool options, the database its
//!   configuration names, the BSON document each mapped row becomes, the mapped values it has no
//!   BSON representation for, the documents the server's document limit refuses, the insert or
//!   bulk write every write becomes under the emitter's `BATCH` limits and the exact BSON size of
//!   what it carries, the upsert model of a conflict action, and write-error classification.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio
//!   and the `mongodb` driver.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

use std::{ops::Range, path::PathBuf};

use ahash::HashMap;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use async_trait::async_trait;
use chrono::DateTime;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use mongodb::{
    Client as DriverClient, Collection, Namespace,
    bson::{Binary, Bson, Document, RawDocumentBuf, doc, spec::BinarySubtype},
    error::{Error as DriverError, ErrorKind, PartialBulkWriteResult},
    options::{
        ClientOptions, Tls, TlsOptions, UpdateOneModel as MongoDbUpdateOneModel,
        WriteModel as MongoDbWriteModel,
    },
};
use nervix_connector::{
    MappedSinkMember, MappedSinkRows, MeasuredRequest, PerRecordOutcome, RejectedSinkRecord,
    RowRequest, RowRequestLimits, RowSink, SinkHost, SinkLifecycle, SinkPublishError,
    SinkPublishResult, SinkRecordPosition, SinkStartError, SinkStartResult,
    optional_client_config_value,
};
use nervix_models::{
    ClientConfigEntry, ClientPoolBounds, CollectionName, EmitterBatchPolicy, FieldPath, Timestamp,
};
use tracing::{debug, trace};

const MONGODB: &str = "mongodb";

/// What `MAX SIZE` measures on a MongoDB write, which an oversized row's rejection names.
const MEASURED_REQUEST: &str = "MongoDB write";

/// The largest document MongoDB stores: the BSON document limit every server applies. The driver
/// refuses a larger document before sending the write that carries it, so such a row is rejected
/// here instead of failing every other row of that write.
const MAX_DOCUMENT_BYTES: u64 = 16 * 1024 * 1024;

/// The `_id` MongoDB gives a document that carries none: its element type, the name `_id` and its
/// terminator, and a twelve-byte ObjectId. The driver prepends it to an inserted document before
/// sending it, and the server adds it to a document an upsert inserts.
const GENERATED_ID_BYTES: u64 = 17;

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
    /// The emitter's `BATCH` limits, which bound the documents and the measured bytes of every
    /// write.
    pub batch: EmitterBatchPolicy,
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
    limits: RowRequestLimits,
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
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow column access and MongoDB driver publication own their \
                      effects"
        )
    )]
    pub fn new(
        config: MongoDbSinkConfig,
        client: Box<dyn MongoDbClientSource>,
        _host: SinkHost,
    ) -> SinkStartResult<Self> {
        let MongoDbSinkConfig {
            config,
            collection,
            conflict_action,
            batch,
        } = config;
        // MAX MESSAGES never exceeds 65,536, below the 100,000 writes a server takes in one
        // command, so the emitter's limits are the only ones a write keeps to.
        Ok(Self {
            client,
            database: database_name(&config)?,
            collection,
            conflict_action,
            limits: RowRequestLimits::from(batch),
        })
    }

    /// The database this sink writes into, once the collection it writes has been found there:
    /// Nervix creates nothing in MongoDB, so a missing collection is a publish failure.
    async fn provisioned_database(
        &self,
        client: &MongoDbClient,
    ) -> SinkPublishResult<mongodb::Database> {
        let database = client.0.database(&self.database);
        let collection_names = database
            .list_collection_names()
            .filter(doc! { "name": self.collection.as_str() })
            .authorized_collections(true)
            .await
            .map_err(|_| safe_infrastructure_error("collection lookup request"))?;
        if collection_names.is_empty() {
            return Err(Report::new(SinkPublishError::Publish { sink: MONGODB })
                .attach_printable("MongoDB target collection is not provisioned"));
        }
        Ok(database)
    }

    /// Inserts `documents` in bulk inserts under this sink's limits, each measured as the
    /// documents it carries exactly as they are stored.
    async fn insert_documents(
        &self,
        collection: &Collection<RawDocumentBuf>,
        documents: Vec<StorableDocument>,
        outcome: &mut PerRecordOutcome<SinkRecordPosition>,
    ) {
        let mut stored_ends = Vec::with_capacity(documents.len());
        let mut stored = 0_u64;
        for document in &documents {
            stored = stored
                .checked_add(document.stored_bytes)
                .assured("the documents of one write are held in memory");
            stored_ends.push(stored);
        }
        let requests = self
            .limits
            .divide(documents.len(), |candidate| MeasuredRequest {
                size: running_span(&stored_ends, candidate),
                request: (),
            });
        if requests.subdivisions > 0 {
            debug!(
                collection = self.collection.as_str(),
                subdivisions = requests.subdivisions,
                "halved MongoDB inserts that exceeded MAX SIZE"
            );
        }
        for request in requests.requests {
            nervix_primitives::task::consume_budget().await;
            let written = match request {
                RowRequest::Write { members, .. } => members,
                RowRequest::Oversize { member, oversize } => {
                    let document = &documents[member];
                    outcome.reject(oversize.rejected(
                        document.row.position,
                        document.row.occurred_at,
                        MEASURED_REQUEST,
                    ));
                    continue;
                }
            };
            let carried = &documents[written];
            let rows = carried
                .iter()
                .map(|document| document.row)
                .collect::<Vec<_>>();
            let inserted = collection
                .insert_many(carried.iter().map(|document| &document.raw))
                .ordered(false)
                .await;
            match inserted {
                Ok(_) => {
                    for row in &rows {
                        outcome.deliver(row.position);
                    }
                }
                Err(error) => {
                    Self::apply_insert_many_error(&rows, error, outcome);
                    if outcome.has_infrastructure_error() {
                        return;
                    }
                }
            }
        }
    }

    /// Upserts `documents` in bulk writes under this sink's limits, each measured as the filter
    /// and update documents it carries for every row.
    async fn upsert_documents(
        &self,
        client: &MongoDbClient,
        documents: Vec<StorableDocument>,
        outcome: &mut PerRecordOutcome<SinkRecordPosition>,
    ) {
        let namespace = Namespace::new(&self.database, self.collection.as_str());
        let mut upserts = Vec::with_capacity(documents.len());
        let mut upsert_ends = Vec::with_capacity(documents.len());
        let mut measured = 0_u64;
        for document in documents {
            let upsert = match ConflictUpsert::new(document, &self.conflict_action) {
                Ok(upsert) => upsert,
                Err(error) => {
                    outcome.fail(error);
                    return;
                }
            };
            measured = measured
                .checked_add(upsert.measured_bytes)
                .assured("the upserts of one write are held in memory");
            upsert_ends.push(measured);
            upserts.push(Some(upsert));
        }
        let requests = self
            .limits
            .divide(upserts.len(), |candidate| MeasuredRequest {
                size: running_span(&upsert_ends, candidate),
                request: (),
            });
        if requests.subdivisions > 0 {
            debug!(
                collection = self.collection.as_str(),
                subdivisions = requests.subdivisions,
                "halved MongoDB bulk writes that exceeded MAX SIZE"
            );
        }
        for request in requests.requests {
            nervix_primitives::task::consume_budget().await;
            let written = match request {
                RowRequest::Write { members, .. } => members,
                RowRequest::Oversize { member, oversize } => {
                    let upsert = upserts[member]
                        .as_ref()
                        .assured("an oversize row is never taken for a write");
                    outcome.reject(oversize.rejected(
                        upsert.row.position,
                        upsert.row.occurred_at,
                        MEASURED_REQUEST,
                    ));
                    continue;
                }
            };
            let mut rows = Vec::with_capacity(written.len());
            let mut models = Vec::with_capacity(written.len());
            // Every row travels in at most one bulk write of this write, so each takes its own.
            for upsert in &mut upserts[written] {
                let upsert = upsert
                    .take()
                    .assured("each row is written by the one request that carries it");
                rows.push(upsert.row);
                models.push(upsert.into_model(&namespace));
            }
            let written = client
                .0
                .bulk_write(models)
                .ordered(false)
                .verbose_results()
                .await;
            match written {
                Ok(_) => {
                    for row in &rows {
                        outcome.deliver(row.position);
                    }
                }
                Err(error) => {
                    Self::apply_bulk_write_error(&rows, error, outcome);
                    if outcome.has_infrastructure_error() {
                        return;
                    }
                }
            }
        }
    }

    /// The document of every row MongoDB can store, in the order the write carries its rows.
    ///
    /// A row with a value BSON cannot hold, or whose document exceeds the server's document limit,
    /// is rejected alone before any write, so it never costs the rows around it their write.
    fn storable_documents(
        rows: &MappedSinkRows<'_>,
        members: &[MappedSinkMember],
        carriers: &[MappedBsonColumns<'_>],
        outcome: &mut PerRecordOutcome<SinkRecordPosition>,
    ) -> Vec<StorableDocument> {
        let mut documents = Vec::with_capacity(members.len());
        for member in members {
            let row = WrittenRow {
                position: rows.position(*member),
                occurred_at: rows.occurred_at(*member),
            };
            let mapped = carriers
                .get(member.carrier)
                .assured("every carrier of the write was mapped before its rows were encoded");
            let document = match mapped.document(member.row) {
                Ok(document) => document,
                Err(unencodable) => {
                    outcome.reject(unencodable.rejected(row.position, row.occurred_at));
                    continue;
                }
            };
            match StorableDocument::new(row, document) {
                Ok(document) => documents.push(document),
                Err(error @ UnstorableDocument::AboveDocumentLimit { .. }) => {
                    outcome.reject(RejectedSinkRecord::external(
                        row.position,
                        row.occurred_at,
                        error.to_string(),
                    ));
                }
                Err(error @ UnstorableDocument::InvalidFieldNames) => {
                    outcome.fail(
                        Report::new(SinkPublishError::Misconfigured { sink: MONGODB })
                            .attach_printable(error.to_string()),
                    );
                    return documents;
                }
            }
        }
        documents
    }

    fn apply_insert_many_error(
        rows: &[WrittenRow],
        error: DriverError,
        outcome: &mut PerRecordOutcome<SinkRecordPosition>,
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
        Self::apply_insert_many_write_errors(rows, &errors, outcome);
    }

    fn apply_insert_many_write_errors(
        rows: &[WrittenRow],
        write_errors: &[(usize, i32)],
        outcome: &mut PerRecordOutcome<SinkRecordPosition>,
    ) {
        let errors = write_errors.iter().copied().collect::<HashMap<_, _>>();
        let mut has_infrastructure_error = errors.len() != write_errors.len()
            || write_errors
                .iter()
                .any(|(index, code)| *index >= rows.len() || !is_record_write_error(*code));
        for (local_index, row) in rows.iter().enumerate() {
            if let Some(code) = errors.get(&local_index) {
                if is_record_write_error(*code) {
                    outcome.reject(RejectedSinkRecord::external(
                        row.position,
                        row.occurred_at,
                        format!("MongoDB rejected document with code {code}"),
                    ));
                } else {
                    has_infrastructure_error = true;
                }
            } else {
                outcome.deliver(row.position);
            }
        }
        if has_infrastructure_error {
            outcome.fail(safe_infrastructure_error("insert_many request"));
        }
    }

    fn apply_bulk_write_error(
        rows: &[WrittenRow],
        error: DriverError,
        outcome: &mut PerRecordOutcome<SinkRecordPosition>,
    ) {
        let ErrorKind::BulkWrite(bulk_error) = error.kind.as_ref() else {
            outcome.fail(safe_infrastructure_error("bulk write request"));
            return;
        };
        if !bulk_error.write_concern_errors.is_empty() {
            outcome.fail(safe_infrastructure_error("bulk write concern"));
            return;
        }
        let mut accounted = vec![false; rows.len()];
        if let Some(PartialBulkWriteResult::Verbose(result)) = bulk_error.partial_result.as_ref() {
            for local_index in result
                .insert_results
                .keys()
                .chain(result.update_results.keys())
                .chain(result.delete_results.keys())
            {
                if let Some(row) = rows.get(*local_index) {
                    outcome.deliver(row.position);
                    accounted[*local_index] = true;
                }
            }
        }
        let mut has_infrastructure_error = false;
        for (local_index, error) in &bulk_error.write_errors {
            let Some(row) = rows.get(*local_index) else {
                has_infrastructure_error = true;
                continue;
            };
            if is_record_write_error(error.code) {
                outcome.reject(RejectedSinkRecord::external(
                    row.position,
                    row.occurred_at,
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

/// The bytes the members `members` span in a running total of member sizes, whose entry `n` is
/// the total of the first `n + 1` members.
fn running_span(ends: &[u64], members: Range<usize>) -> u64 {
    let start = match members.start.checked_sub(1) {
        Some(previous) => *ends
            .get(previous)
            .assured("a request starts at a member of the write"),
        None => 0,
    };
    let last = members
        .end
        .checked_sub(1)
        .assured("a request carries at least one member");
    let end = *ends
        .get(last)
        .assured("a request ends at a member of the write");
    end.checked_sub(start)
        .assured("the running size of later members is at least that of earlier ones")
}

/// A row one write carries, as MongoDB answers for it: where it sits in the host's batches and
/// when its mapping was evaluated.
#[derive(Debug, Clone, Copy)]
struct WrittenRow {
    position: SinkRecordPosition,
    occurred_at: Timestamp,
}

/// One row's document, which the server's document limit admits.
struct StorableDocument {
    row: WrittenRow,
    document: Document,
    /// The document as the driver sends it.
    raw: RawDocumentBuf,
    /// What the document occupies once stored, with the `_id` it is given when it carries none.
    stored_bytes: u64,
}

/// Why a row's document can never be stored.
#[derive(Debug, thiserror::Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "unstorable documents produce definitive semantic record rejections"
    )
)]
enum UnstorableDocument {
    #[error(
        "MongoDB document of one row measures {stored_bytes} bytes, above the \
         {MAX_DOCUMENT_BYTES} bytes one MongoDB document holds"
    )]
    AboveDocumentLimit { stored_bytes: u64 },
    #[error("MongoDB VALUES field names cannot be written as a BSON document")]
    InvalidFieldNames,
}

impl StorableDocument {
    /// The document of `row`, or why MongoDB could never store it.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow column access and MongoDB driver publication own their \
                      effects"
        )
    )]
    fn new(row: WrittenRow, document: Document) -> Result<Self, UnstorableDocument> {
        let raw = RawDocumentBuf::from_document(&document)
            .map_err(|_| UnstorableDocument::InvalidFieldNames)?;
        let encoded = u64::try_from(raw.as_bytes().len())
            .assured("Nervix builds for 64-bit targets only, where u64 holds usize");
        let stored_bytes = if document.contains_key("_id") {
            encoded
        } else {
            encoded
                .checked_add(GENERATED_ID_BYTES)
                .assured("one document this node holds in memory")
        };
        if stored_bytes > MAX_DOCUMENT_BYTES {
            return Err(UnstorableDocument::AboveDocumentLimit { stored_bytes });
        }
        Ok(Self {
            row,
            document,
            raw,
            stored_bytes,
        })
    }
}

/// One row's upsert under a conflict action: the filter that finds the document it conflicts with
/// and the update it applies, and the bytes those two documents measure.
struct ConflictUpsert {
    row: WrittenRow,
    filter: Document,
    update: Document,
    measured_bytes: u64,
}

impl ConflictUpsert {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow column access and MongoDB driver publication own their \
                      effects"
        )
    )]
    fn new(
        document: StorableDocument,
        conflict_action: &MongoDbConflictAction,
    ) -> SinkPublishResult<Self> {
        let StorableDocument { row, document, .. } = document;
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
        let mut measured_bytes = 0_u64;
        for part in [&filter, &update] {
            let raw = RawDocumentBuf::from_document(part).map_err(|_| {
                Report::new(SinkPublishError::Misconfigured { sink: MONGODB })
                    .attach_printable("MongoDB VALUES field names cannot be written as BSON")
            })?;
            let encoded = u64::try_from(raw.as_bytes().len())
                .assured("Nervix builds for 64-bit targets only, where u64 holds usize");
            measured_bytes = measured_bytes
                .checked_add(encoded)
                .assured("one upsert this node holds in memory");
        }
        Ok(Self {
            row,
            filter,
            update,
            measured_bytes,
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

    fn into_model(self, namespace: &Namespace) -> MongoDbWriteModel {
        MongoDbUpdateOneModel::builder()
            .namespace(namespace.clone())
            .filter(self.filter)
            .update(self.update)
            .upsert(true)
            .build()
            .into()
    }
}

#[async_trait]
impl SinkLifecycle for MongoDbSink {}

#[async_trait]
impl RowSink for MongoDbSink {
    /// Writes the rows of every carrier in inserts or bulk writes of at most `MAX MESSAGES`
    /// documents whose measured BSON is at most `MAX SIZE` bytes.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow column access and MongoDB driver publication own their \
                      effects"
        )
    )]
    async fn publish(&mut self, rows: MappedSinkRows<'_>) -> PerRecordOutcome<SinkRecordPosition> {
        let members = rows.members();
        let mut outcome = PerRecordOutcome::with_capacity(members.len());
        let mut carriers = Vec::with_capacity(rows.carriers.len());
        for carrier in &rows.carriers {
            match MappedBsonColumns::new(carrier.batch, rows.target_columns) {
                Ok(columns) => carriers.push(columns),
                Err(error) => {
                    outcome.fail(
                        Report::new(SinkPublishError::Publish { sink: MONGODB })
                            .attach_printable(error.to_string()),
                    );
                    return outcome;
                }
            }
        }
        let client = match self.client.client() {
            Ok(client) => client,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        let database = match self.provisioned_database(&client).await {
            Ok(database) => database,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        let documents = Self::storable_documents(&rows, &members, &carriers, &mut outcome);
        if outcome.has_infrastructure_error() {
            return outcome;
        }
        match &self.conflict_action {
            MongoDbConflictAction::None => {
                let collection = database.collection::<RawDocumentBuf>(self.collection.as_str());
                self.insert_documents(&collection, documents, &mut outcome)
                    .await;
            }
            MongoDbConflictAction::DoNothing { .. } | MongoDbConflictAction::DoUpdate { .. } => {
                self.upsert_documents(&client, documents, &mut outcome)
                    .await;
            }
        }
        trace!(
            collection = self.collection.as_str(),
            rows = members.len(),
            "emitter published mongodb documents"
        );
        outcome
    }
}

/// A mapped value this sink has no BSON representation for, named with the field that carries it.
#[derive(Debug, thiserror::Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "unencodable mapped values produce definitive semantic record rejections"
    )
)]
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
    fn rejected(
        &self,
        position: SinkRecordPosition,
        occurred_at: Timestamp,
    ) -> RejectedSinkRecord<SinkRecordPosition> {
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
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow column access and MongoDB driver publication own their \
                      effects"
        )
    )]
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
    /// Octets, written as generic BSON binary data.
    Bytes(&'a BinaryArray),
    Datetime(&'a TimestampNanosecondArray),
    List {
        offsets: &'a ListArray,
        elements: Box<MappedBsonColumn<'a>>,
    },
    /// A fixed-size array, such as an array literal, whose rows all hold the same element count.
    FixedList {
        list: &'a FixedSizeListArray,
        elements: Box<MappedBsonColumn<'a>>,
    },
}

impl<'a> MappedBsonColumn<'a> {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow column access and MongoDB driver publication own their \
                      effects"
        )
    )]
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
        if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
            return Some(Self::Bytes(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            return Some(Self::Datetime(values));
        }
        if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
            let elements = Self::new(list.values())?;
            return Some(Self::FixedList {
                list,
                elements: Box::new(elements),
            });
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
            Self::Bytes(values) => Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: values.value(row).to_vec(),
            }),
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
                elements.array(start..end, field)?
            }
            Self::FixedList { list, elements } => {
                let non_negative = "Arrow builds fixed-size list offsets and widths as \
                                    non-negative element counts";
                let start = usize::try_from(list.value_offset(row)).assured(non_negative);
                let width = usize::try_from(list.value_length()).assured(non_negative);
                let end = start
                    .checked_add(width)
                    .assured("a fixed-size list row ends inside its element array");
                elements.array(start..end, field)?
            }
        };
        Ok(value)
    }

    /// The elements `elements` of this column as one BSON array, for the mapped column `field`.
    fn array(&self, elements: Range<usize>, field: &str) -> Result<Bson, UnencodableMappedValue> {
        let mut items = Vec::with_capacity(elements.len());
        for element in elements {
            items.push(self.value(element, field)?);
        }
        Ok(Bson::Array(items))
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
            Self::Bytes(values) => values.is_null(row),
            Self::Datetime(values) => values.is_null(row),
            Self::List { offsets, .. } => offsets.is_null(row),
            Self::FixedList { list, .. } => list.is_null(row),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::types::UInt64Type;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use nervix_models::{MessageErrorCode, MessageErrorOperation};
    use nervix_primitives::sync::StdArc;

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
        let rows = [10, 11, 12].map(|row_index| WrittenRow {
            position: SinkRecordPosition {
                batch_index: 7,
                row_index,
            },
            occurred_at: Timestamp::from_unix_nanos(3),
        });

        MongoDbSink::apply_insert_many_write_errors(&rows, &[(1, 11000), (2, 91)], &mut outcome);

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
            outcome.rejected[0].id,
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
            columns
                .document(0)
                .expect("the signed maximum should encode"),
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
            record.id,
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

    fn written_row(row_index: usize) -> WrittenRow {
        WrittenRow {
            position: SinkRecordPosition {
                batch_index: 0,
                row_index,
            },
            occurred_at: Timestamp::from_unix_nanos(1),
        }
    }

    /// An inserted document is measured as the driver writes it, with the `_id` it prepends to a
    /// document that has none, so a scenario's `MAX SIZE` can be written to the byte.
    #[test]
    fn measures_a_document_as_it_is_stored() {
        let generated = StorableDocument::new(written_row(0), doc! { "seq": 1_i64, "note": "abc" })
            .expect("a small document is storable");
        let given = StorableDocument::new(
            written_row(1),
            doc! { "_id": 7_i64, "seq": 1_i64, "note": "abc" },
        )
        .expect("a small document is storable");

        assert_eq!(generated.stored_bytes, 49);
        let mut with_id = doc! { "_id": mongodb::bson::oid::ObjectId::new() };
        with_id.extend(generated.document.clone());
        assert_eq!(
            generated.stored_bytes,
            u64::try_from(
                RawDocumentBuf::from_document(&with_id)
                    .expect("the test document encodes")
                    .as_bytes()
                    .len()
            )
            .expect("a test document fits u64")
        );
        assert_eq!(
            given.stored_bytes,
            u64::try_from(given.raw.as_bytes().len()).expect("a test document fits u64")
        );
    }

    #[test]
    fn a_document_above_the_server_limit_is_unstorable() {
        let note = "x".repeat(16 * 1024 * 1024);

        let rejected = StorableDocument::new(written_row(0), doc! { "note": note });

        let Err(UnstorableDocument::AboveDocumentLimit { stored_bytes }) = rejected else {
            panic!("a document above 16 MiB must be unstorable");
        };
        assert!(stored_bytes > MAX_DOCUMENT_BYTES);
        assert!(
            UnstorableDocument::AboveDocumentLimit { stored_bytes }
                .to_string()
                .contains("16777216"),
            "the rejection names the server's document limit"
        );
    }

    #[test]
    fn an_upsert_measures_its_filter_and_update_documents() {
        let document = StorableDocument::new(written_row(0), doc! { "seq": 1_i64, "note": "a" })
            .expect("a small document is storable");

        let upsert = ConflictUpsert::new(
            document,
            &MongoDbConflictAction::DoUpdate {
                target: vec!["seq".to_string()],
            },
        )
        .expect("the upsert builds");

        let filter = RawDocumentBuf::from_document(&upsert.filter)
            .expect("the filter encodes")
            .as_bytes()
            .len();
        let update = RawDocumentBuf::from_document(&upsert.update)
            .expect("the update encodes")
            .as_bytes()
            .len();
        assert_eq!(upsert.filter, doc! { "seq": 1_i64 });
        assert_eq!(
            upsert.update,
            doc! { "$set": { "note": "a" }, "$setOnInsert": { "seq": 1_i64 } }
        );
        assert_eq!(
            upsert.measured_bytes,
            u64::try_from(filter + update).expect("a test upsert fits u64")
        );
    }

    #[test]
    fn writes_bytes_as_binary_data_and_fixed_size_arrays_as_arrays() {
        let pairs = FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", DataType::Int64, false)),
            2,
            StdArc::new(Int64Array::from(vec![1, 10])),
            None,
        )
        .expect("one row of two elements builds");
        let raw = BinaryArray::from(vec![b"\x00\xff".as_slice()]);
        let schema = StdArc::new(Schema::new(vec![
            Field::new("pair", pairs.data_type().clone(), true),
            Field::new("raw", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![StdArc::new(pairs), StdArc::new(raw)])
            .expect("the mapped batch should build");
        let names = ["pair".to_string(), "raw".to_string()];

        let columns = MappedBsonColumns::new(&batch, &names).expect("columns should be mapped");

        assert_eq!(
            columns.document(0).expect("the row should encode"),
            doc! {
                "pair": [1_i64, 10_i64],
                "raw": Binary { subtype: BinarySubtype::Generic, bytes: vec![0x00, 0xff] },
            }
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
