//! MongoDB emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The node's shared MongoDB client and its TLS and pool options, the database its
//!   configuration names, the BSON document each mapped row becomes, the mapped values it has no
//!   BSON representation for, the documents the server's document limit refuses, the insert or
//!   bulk write every write becomes under the emitter's `BATCH` limits and the exact BSON size of
//!   what it carries, the upsert model of a conflict action, write-error classification, and the
//!   account a failed write gives of each document it carried.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio
//!   and the `mongodb` driver.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

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

use std::{
    collections::{BTreeMap, BTreeSet, btree_map},
    fmt,
    ops::Range,
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use async_trait::async_trait;
use chrono::DateTime;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use mongodb::{
    Client as DriverClient, Collection, Namespace,
    bson::{Binary, Bson, Document, RawDocumentBuf, doc, spec::BinarySubtype},
    error::{
        BulkWriteError, Error as DriverError, ErrorKind, InsertManyError, PartialBulkWriteResult,
        WriteConcernError,
    },
    options::{UpdateOneModel as MongoDbUpdateOneModel, WriteModel as MongoDbWriteModel},
};
use nervix_connector::{
    EmitterFinish, EmitterFinishResult, EmitterHost, EmitterLifecycle, EmitterOpenError,
    EmitterRecordPosition, EmitterStartResult, EmitterWriteError, EmitterWriteResult,
    MappedEmitterMember, MappedEmitterRows, MeasuredRequest, PerRecordOutcome,
    RejectedEmitterMember, RowEmitter, RowRequest, RowRequestLimits,
};
use nervix_execution::{Executor, MemoryClass, StorageClass};
use nervix_models::{
    ClientConfigEntry, ClientPoolBounds, EmitterBatchPolicy, FieldPath, MONGODB_MAX_DOCUMENT_BYTES,
    MongoDbCollectionName, Timestamp,
};
use nervix_primitives::{sync::Arc, time::Instant};
use tempfile::NamedTempFile;
use tracing::{debug, trace};

use crate::{offline_config::MongoDbClientConfiguration, tls_identity::PreparedMongoDbClient};

const MONGODB: &str = "mongodb";

/// What `MAX SIZE` measures on a MongoDB write, which an oversized row's rejection names.
const MEASURED_REQUEST: &str = "MongoDB write";

/// The `_id` MongoDB gives a document that carries none: its element type, the name `_id` and its
/// terminator, and a twelve-byte ObjectId. The driver prepends it to an inserted document before
/// sending it, and the server adds it to a document an upsert inserts.
const GENERATED_ID_BYTES: u64 = 17;

#[cfg(test)]
mod mapping_tests;

#[cfg(test)]
mod tls_tests;

/// The node's shared MongoDB client for one named client, whose driver pools per server.
#[derive(Clone)]
pub struct MongoDbClient(Arc<MongoDbClientInner>);

struct MongoDbClientInner {
    driver: DriverClient,
    /// Every client handle retains this client's private combined identity file; its final drop
    /// removes the file together with the native client.
    _identity: Option<NamedTempFile>,
}

/// The node-owned client this emitter writes through while its host holds the lease.
pub trait MongoDbClientProvider: Send + Sync + 'static {
    fn client(&self) -> EmitterWriteResult<MongoDbClient>;
}

/// What one MongoDB emitter writes with, from its typed emitter plan.
pub struct MongoDbCollectionEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub collection: MongoDbCollectionName,
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

/// The MongoDB emitter, which encodes each mapped row as one BSON document.
pub struct MongoDbCollectionEmitter {
    client: Box<dyn MongoDbClientProvider>,
    database: String,
    collection: MongoDbCollectionName,
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
        executor: &Executor,
    ) -> EmitterStartResult<Self> {
        let options = MongoDbClientConfiguration::parse(config)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: MONGODB })?
            .into_options();
        let mut prepared = options
            .prepare(executor)
            .await
            .change_context(EmitterOpenError::Initialize { connector: MONGODB })?;
        prepared.options.min_pool_size = Some(bounds.minimum());
        prepared.options.max_pool_size = Some(bounds.maximum().get());
        let client = Self::from_prepared(prepared, executor).await?;
        client
            .0
            .driver
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .change_context(EmitterOpenError::Initialize { connector: MONGODB })?;
        Ok(client)
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            lifecycle,
            reason = "constructs one named client's native TLS and pool state"
        )
    )]
    async fn from_prepared(
        prepared: PreparedMongoDbClient,
        executor: &Executor,
    ) -> EmitterStartResult<Self> {
        // Charge the prepared job envelope. The driver's client, TLS and pool allocations remain
        // driver-owned; this reservation is not a claim about the driver's internal working set.
        let bytes = u64::try_from(std::mem::size_of::<PreparedMongoDbClient>())
            .change_context(EmitterOpenError::Initialize { connector: MONGODB })?;
        let charge = executor
            .try_reserve(MemoryClass::Management, bytes)
            .change_context(EmitterOpenError::Initialize { connector: MONGODB })?;
        executor
            .run_storage(StorageClass::Filesystem, charge, move |_, cancellation| {
                cancellation
                    .check()
                    .change_context(EmitterOpenError::Initialize { connector: MONGODB })?;
                let PreparedMongoDbClient { options, identity } = prepared;
                // The native constructor synchronously reads CA/identity files and parses PEM.
                let driver = DriverClient::with_options(options).change_context(
                    EmitterOpenError::InvalidConfiguration { connector: MONGODB },
                )?;
                Ok(Self(Arc::new(MongoDbClientInner {
                    driver,
                    _identity: identity,
                })))
            })
            .await
            .change_context(EmitterOpenError::Initialize { connector: MONGODB })?
    }
}

/// A write code the server reports for a document it will never accept.
fn is_record_write_error(code: i32) -> bool {
    matches!(code, 121 | 10334 | 11000)
}

fn safe_infrastructure_error(operation: &str) -> Report<EmitterWriteError> {
    Report::new(EmitterWriteError::Publish { connector: MONGODB })
        .attach_printable(format!("MongoDB {operation} failed"))
}

/// The driver remains available for typed inspection. Its free-form server and document text
/// is private; reporting exposes only the operation, I/O class or numeric server code.
struct MongoRequestFailure {
    operation: &'static str,
    driver: DriverError,
}

impl fmt::Display for MongoRequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MongoDB {} failed", self.operation)?;
        match self.driver.kind.as_ref() {
            ErrorKind::Io(cause) => write!(formatter, ": {}", cause.kind()),
            ErrorKind::Command(cause) => write!(formatter, ": server code {}", cause.code),
            ErrorKind::Authentication { .. } => formatter.write_str(": authentication failed"),
            ErrorKind::ServerSelection { .. } => formatter.write_str(": server selection failed"),
            _ => Ok(()),
        }
    }
}

impl fmt::Debug for MongoRequestFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for MongoRequestFailure {}

fn driver_failure(operation: &'static str, driver: DriverError) -> Report<EmitterWriteError> {
    Report::new(MongoRequestFailure { operation, driver })
        .change_context(EmitterWriteError::Publish { connector: MONGODB })
        .attach_printable(format!("MongoDB {operation} failed"))
}

#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "BSON construction checks whether document field names are representable; a \
                  refusal becomes the typed record or configuration outcome, without exposing \
                  document contents"
    )
)]
fn checked_document(document: &Document) -> Result<RawDocumentBuf, mongodb::bson::raw::Error> {
    RawDocumentBuf::from_document(document)
}

impl MongoDbCollectionEmitter {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow column access and MongoDB driver publication own their \
                      effects"
        )
    )]
    pub fn new(
        config: MongoDbCollectionEmitterConfig,
        client: Box<dyn MongoDbClientProvider>,
        _host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        let MongoDbCollectionEmitterConfig {
            config,
            collection,
            conflict_action,
            batch,
        } = config;
        let configuration = MongoDbClientConfiguration::parse(&config)
            .change_context(EmitterOpenError::InvalidConfiguration { connector: MONGODB })?;
        collection
            .validate_namespace(configuration.database())
            .change_context(EmitterOpenError::InvalidConfiguration { connector: MONGODB })?;
        // MAX MESSAGES never exceeds 65,536, below the 100,000 writes a server takes in one
        // command, so the emitter's limits are the only ones a write keeps to.
        Ok(Self {
            client,
            database: configuration.database().to_owned(),
            collection,
            conflict_action,
            limits: RowRequestLimits::from(batch),
        })
    }

    /// The database this emitter writes into, once the collection it writes has been found there:
    /// Nervix creates nothing in MongoDB, so a missing collection is a publish failure.
    async fn provisioned_database(
        &self,
        client: &MongoDbClient,
    ) -> EmitterWriteResult<mongodb::Database> {
        let database = client.0.driver.database(&self.database);
        let collection_names = database
            .list_collection_names()
            .filter(doc! { "name": self.collection.as_str() })
            .authorized_collections(true)
            .await
            .map_err(|driver| driver_failure("collection lookup request", driver))?;
        if collection_names.is_empty() {
            return Err(
                Report::new(EmitterWriteError::Publish { connector: MONGODB })
                    .attach_printable("MongoDB target collection is not provisioned"),
            );
        }
        Ok(database)
    }

    /// Inserts `documents` in bulk inserts under this emitter's limits, each measured as the
    /// documents it carries exactly as they are stored.
    async fn insert_documents(
        &self,
        collection: &Collection<RawDocumentBuf>,
        documents: Vec<StorableDocument>,
        outcome: &mut PerRecordOutcome<EmitterRecordPosition>,
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

    /// Upserts `documents` in bulk writes under this emitter's limits, each measured as the filter
    /// and update documents it carries for every row.
    async fn upsert_documents(
        &self,
        client: &MongoDbClient,
        documents: Vec<StorableDocument>,
        outcome: &mut PerRecordOutcome<EmitterRecordPosition>,
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
                .driver
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
        rows: &MappedEmitterRows<'_>,
        members: &[MappedEmitterMember],
        carriers: &[MappedBsonColumns<'_>],
        outcome: &mut PerRecordOutcome<EmitterRecordPosition>,
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
                    outcome.reject(RejectedEmitterMember::external(
                        row.position,
                        row.occurred_at,
                        error.to_string(),
                    ));
                }
                Err(error @ UnstorableDocument::InvalidFieldNames) => {
                    outcome.fail(
                        Report::new(EmitterWriteError::Misconfigured { connector: MONGODB })
                            .attach_printable(error.to_string()),
                    );
                    return documents;
                }
            }
        }
        documents
    }

    /// Answers for the rows of an `insert_many` the driver reported as failed: by the server's
    /// account of each document when it gave one, and otherwise by leaving every row unresolved.
    fn apply_insert_many_error(
        rows: &[WrittenRow],
        error: DriverError,
        outcome: &mut PerRecordOutcome<EmitterRecordPosition>,
    ) {
        let ErrorKind::InsertMany(insert_error) = error.kind.as_ref() else {
            outcome.fail(driver_failure("insert_many request", error));
            return;
        };
        WriteAnswers::insert_many(insert_error).apply(rows, outcome);
    }

    /// Answers for the rows of a `bulk_write` the driver reported as failed: by the server's
    /// account of each operation when it gave one, and otherwise by leaving every row unresolved.
    fn apply_bulk_write_error(
        rows: &[WrittenRow],
        error: DriverError,
        outcome: &mut PerRecordOutcome<EmitterRecordPosition>,
    ) {
        let ErrorKind::BulkWrite(bulk_error) = error.kind.as_ref() else {
            outcome.fail(driver_failure("bulk_write request", error));
            return;
        };
        WriteAnswers::bulk_write(bulk_error).apply(rows, outcome);
    }
}

/// What MongoDB answered for each document of one unordered write it reported as failed, by the
/// document's index in that write.
///
/// The server attempts every document of an unordered write. It applies each one it does not
/// refuse, names each one it refuses with the code of the refusal, and reports a write concern it
/// could not satisfy beside the documents it applied, which a write concern failure never undoes.
/// So even a failed write says which of its documents are stored.
struct WriteAnswers {
    request: FailedRequest,
    /// The refusals the server reported, by the index of the document each one names.
    refusals: BTreeMap<usize, Refusal>,
    /// The write concern the server could not satisfy for the documents it applied.
    unsatisfied_write_concern: Option<UnsatisfiedWriteConcern>,
}

/// The request a failed write was sent as, which decides how the server reports the documents it
/// applied.
#[derive(strum::AsRefStr)]
#[strum(serialize_all = "snake_case")]
enum FailedRequest {
    /// An unordered `insert_many`, which names only the documents it refused: the server applied
    /// every other one.
    InsertMany,
    /// A verbose `bulk_write`, which reports a result for each operation it applied, by its
    /// index.
    BulkWrite { applied: BTreeSet<usize> },
}

/// How the server refused one document of a write.
enum Refusal {
    /// One refusal, with the code it carries.
    Code(i32),
    /// More than one refusal of the same document, which leaves its outcome unknown.
    Repeated,
}

/// A write concern the server could not satisfy, as it named it.
struct UnsatisfiedWriteConcern {
    code: i32,
    /// The name of the code, when the server gave one.
    code_name: Option<String>,
}

/// What one document of a failed write resolves to.
enum DocumentAnswer {
    /// The server applied the document.
    Delivered,
    /// The server refused the document for good, with this code.
    Rejected { code: i32 },
    /// Nothing says whether the server applied the document.
    Unresolved,
}

impl WriteAnswers {
    /// The answers of an unordered `insert_many`. The driver names each refused document by its
    /// index in the whole insert, across the commands it divides the insert into, and reports the
    /// insert as failed only after every command was answered.
    fn insert_many(error: &InsertManyError) -> Self {
        let mut answers = Self::new(
            FailedRequest::InsertMany,
            error.write_concern_error.as_ref(),
        );
        if let Some(write_errors) = &error.write_errors {
            for write_error in write_errors {
                answers.refuse(write_error.index, write_error.code);
            }
        }
        answers
    }

    /// The answers of a verbose `bulk_write`. A bulk write the driver stopped early reports results
    /// only for the operations it applied before it stopped, so every later operation stays
    /// unresolved.
    fn bulk_write(error: &BulkWriteError) -> Self {
        let mut applied = BTreeSet::new();
        // Every bulk write asks for verbose results; a summary names no operation.
        if let Some(PartialBulkWriteResult::Verbose(results)) = &error.partial_result {
            applied.extend(results.insert_results.keys().copied());
            applied.extend(results.update_results.keys().copied());
            applied.extend(results.delete_results.keys().copied());
        }
        let mut answers = Self::new(
            FailedRequest::BulkWrite { applied },
            error.write_concern_errors.first(),
        );
        for (index, write_error) in &error.write_errors {
            answers.refuse(*index, write_error.code);
        }
        answers
    }

    fn new(request: FailedRequest, write_concern: Option<&WriteConcernError>) -> Self {
        let mut unsatisfied_write_concern = None;
        if let Some(write_concern) = write_concern {
            let code_name = if write_concern.code_name.is_empty() {
                None
            } else {
                Some(write_concern.code_name.clone())
            };
            unsatisfied_write_concern = Some(UnsatisfiedWriteConcern {
                code: write_concern.code,
                code_name,
            });
        }
        Self {
            request,
            refusals: BTreeMap::new(),
            unsatisfied_write_concern,
        }
    }

    /// Records the server's refusal of the document at `index` with `code`.
    fn refuse(&mut self, index: usize, code: i32) {
        match self.refusals.entry(index) {
            btree_map::Entry::Vacant(entry) => {
                entry.insert(Refusal::Code(code));
            }
            btree_map::Entry::Occupied(mut entry) => {
                *entry.get_mut() = Refusal::Repeated;
            }
        }
    }

    /// What the server answered for the document at `index`.
    ///
    /// A refusal decides the document, even beside a result for it: a document refused for good
    /// is rejected, and any other refusal leaves it unresolved for the host to retry.
    fn answer(&self, index: usize) -> DocumentAnswer {
        match self.refusals.get(&index) {
            Some(Refusal::Code(code)) if is_record_write_error(*code) => {
                return DocumentAnswer::Rejected { code: *code };
            }
            Some(Refusal::Code(_) | Refusal::Repeated) => return DocumentAnswer::Unresolved,
            None => {}
        }
        let applied = match &self.request {
            FailedRequest::InsertMany => true,
            FailedRequest::BulkWrite { applied } => applied.contains(&index),
        };
        if applied {
            DocumentAnswer::Delivered
        } else {
            DocumentAnswer::Unresolved
        }
    }

    /// Whether an answer names a document beyond the `documents` the write carried, which leaves
    /// the server's account of the write in doubt.
    fn names_foreign_document(&self, documents: usize) -> bool {
        if self.refusals.range(documents..).next().is_some() {
            return true;
        }
        match &self.request {
            FailedRequest::InsertMany => false,
            FailedRequest::BulkWrite { applied } => applied.range(documents..).next().is_some(),
        }
    }

    /// Answers for every row of the write, whose index in the write is its position in `rows`:
    /// a document the server applied is delivered, one it refused for good is rejected with its
    /// code, and every other row stays unresolved.
    ///
    /// The write fails, for the host to retry what it left unresolved on its backoff, when a row
    /// is unresolved, when an answer names a document the write did not carry, or when the server
    /// could not satisfy the write concern. The documents it applied stay delivered even then:
    /// writing them again would store them twice.
    fn apply(&self, rows: &[WrittenRow], outcome: &mut PerRecordOutcome<EmitterRecordPosition>) {
        let mut leaves_unresolved = false;
        for (index, row) in rows.iter().enumerate() {
            match self.answer(index) {
                DocumentAnswer::Delivered => outcome.deliver(row.position),
                DocumentAnswer::Rejected { code } => {
                    outcome.reject(RejectedEmitterMember::external(
                        row.position,
                        row.occurred_at,
                        format!("MongoDB rejected document with code {code}"),
                    ));
                }
                DocumentAnswer::Unresolved => leaves_unresolved = true,
            }
        }
        if let Some(write_concern) = &self.unsatisfied_write_concern {
            outcome.fail(write_concern.failure(&self.request));
            return;
        }
        if leaves_unresolved || self.names_foreign_document(rows.len()) {
            outcome.fail(self.request.failure());
        }
    }
}

impl FailedRequest {
    /// The failure of this request when the server's answers leave it unresolved.
    fn failure(&self) -> Report<EmitterWriteError> {
        safe_infrastructure_error(&format!("{} request", self.as_ref()))
    }
}

impl UnsatisfiedWriteConcern {
    /// The failure of `request` whose write concern this is. It names the server's code, never a
    /// document the request carried.
    fn failure(&self, request: &FailedRequest) -> Report<EmitterWriteError> {
        let request = request.as_ref();
        let code = self.code;
        let message = match &self.code_name {
            Some(code_name) => {
                format!("MongoDB {request} write concern failed with code {code} ({code_name})")
            }
            None => format!("MongoDB {request} write concern failed with code {code}"),
        };
        Report::new(EmitterWriteError::Publish { connector: MONGODB }).attach_printable(message)
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
    position: EmitterRecordPosition,
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
         {MONGODB_MAX_DOCUMENT_BYTES} bytes one MongoDB document holds"
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
        let raw = checked_document(&document).map_err(|_| UnstorableDocument::InvalidFieldNames)?;
        let encoded = u64::try_from(raw.as_bytes().len())
            .assured("Nervix builds for 64-bit targets only, where u64 holds usize");
        let stored_bytes = if document.contains_key("_id") {
            encoded
        } else {
            encoded
                .checked_add(GENERATED_ID_BYTES)
                .assured("one document this node holds in memory")
        };
        if stored_bytes > MONGODB_MAX_DOCUMENT_BYTES {
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
    ) -> EmitterWriteResult<Self> {
        let StorableDocument { row, document, .. } = document;
        let (filter, update) = match conflict_action {
            MongoDbConflictAction::None => {
                return Err(
                    Report::new(EmitterWriteError::Publish { connector: MONGODB })
                        .attach_printable("MongoDB bulk update requires an ON CONFLICT action"),
                );
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
                    return Err(
                        Report::new(EmitterWriteError::Publish { connector: MONGODB })
                            .attach_printable(
                                "MongoDB ON CONFLICT DO UPDATE requires at least one non-conflict \
                                 VALUES field to update",
                            ),
                    );
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
            let raw = checked_document(part).map_err(|_| {
                Report::new(EmitterWriteError::Misconfigured { connector: MONGODB })
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

    fn conflict_filter(document: &Document, target: &[String]) -> EmitterWriteResult<Document> {
        let mut filter = Document::new();
        for field in target {
            let Some(value) = document.get(field) else {
                return Err(
                    Report::new(EmitterWriteError::Publish { connector: MONGODB })
                        .attach_printable(format!(
                            "MongoDB ON CONFLICT target field '{field}' is missing from VALUES \
                             document"
                        )),
                );
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

impl MongoDbCollectionEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::MongoDb(nervix_models::MongoDbEmitterEntity::Collection);
}

#[async_trait]
impl EmitterLifecycle for MongoDbCollectionEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// Each unordered write answers for every document before the publish returns, so the
    /// client holds nothing.
    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }
}

#[async_trait]
impl RowEmitter for MongoDbCollectionEmitter {
    /// Writes the rows of every carrier in inserts or bulk writes of at most `MAX MESSAGES`
    /// documents whose measured BSON is at most `MAX SIZE` bytes.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow column access and MongoDB driver publication own their \
                      effects"
        )
    )]
    async fn publish(
        &mut self,
        rows: MappedEmitterRows<'_>,
    ) -> PerRecordOutcome<EmitterRecordPosition> {
        let members = rows.members();
        let mut outcome = PerRecordOutcome::with_capacity(members.len());
        let mut carriers = Vec::with_capacity(rows.carriers.len());
        for carrier in &rows.carriers {
            match MappedBsonColumns::new(carrier.batch, rows.target_columns) {
                Ok(columns) => carriers.push(columns),
                Err(error) => {
                    outcome.fail(
                        Report::new(EmitterWriteError::Publish { connector: MONGODB })
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

/// A mapped value this emitter has no BSON representation for, named with the field that carries it.
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
        position: EmitterRecordPosition,
        occurred_at: Timestamp,
    ) -> RejectedEmitterMember<EmitterRecordPosition> {
        let field = match self {
            Self::UnsupportedColumn { field, .. } | Self::UnsignedIntegerRange { field } => field,
        };
        RejectedEmitterMember::invalid(
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
    /// Integer widths use the declaration's native BSON width, and an unsigned value above the
    /// signed 64-bit range has no BSON integer, so its row is rejected.
    /// The field names the mapped column this value belongs to, including for a list element,
    /// whose failure belongs to the list its row carries.
    fn value(&self, row: usize, field: &str) -> Result<Bson, UnencodableMappedValue> {
        if self.is_null(row) {
            return Ok(Bson::Null);
        }
        let value = match self {
            Self::Bool(values) => Bson::Boolean(values.value(row)),
            Self::U8(values) => Bson::Int32(i32::from(values.value(row))),
            Self::I8(values) => Bson::Int32(i32::from(values.value(row))),
            Self::U16(values) => Bson::Int32(i32::from(values.value(row))),
            Self::I16(values) => Bson::Int32(i32::from(values.value(row))),
            Self::U32(values) => Bson::Int64(i64::from(values.value(row))),
            Self::I32(values) => Bson::Int32(values.value(row)),
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
            Self::Datetime(values) => Bson::DateTime(mongodb::bson::DateTime::from_millis(
                DateTime::from_timestamp_nanos(values.value(row)).timestamp_millis(),
            )),
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use arrow_array::types::UInt64Type;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use mongodb::{
        error::{BulkWriteError, InsertManyError, WriteConcernError, WriteError},
        results::{DeleteResult, InsertOneResult, UpdateResult, VerboseBulkWriteResult},
    };
    use nervix_models::{MessageErrorCode, MessageErrorOperation};
    use nervix_primitives::sync::StdArc;

    use super::*;

    #[test]
    fn infrastructure_reports_retain_driver_causes_without_document_text() {
        let mut outcome = PerRecordOutcome::empty();
        let driver = DriverError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "confidential driver detail",
        ));
        MongoDbCollectionEmitter::apply_insert_many_error(&[], driver, &mut outcome);
        let failure = outcome
            .into_parts()
            .infrastructure_error
            .expect("request failed");
        let cause = failure
            .downcast_ref::<MongoRequestFailure>()
            .expect("typed driver cause");
        let ErrorKind::Io(io) = cause.driver.kind.as_ref() else {
            panic!("I/O cause retained")
        };
        assert_eq!(io.kind(), std::io::ErrorKind::ConnectionRefused);
        let rendered = format!("{failure:#}");
        assert!(rendered.contains("connection refused"), "{rendered}");
        assert!(
            !rendered.contains("confidential driver detail"),
            "{rendered}"
        );
    }

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
            position: EmitterRecordPosition {
                batch_index: 7,
                row_index,
            },
            occurred_at: Timestamp::from_unix_nanos(3),
        });

        let insert_error: InsertManyError = mongodb::bson::from_document(doc! {
            "writeErrors": [
                { "index": 1, "code": 11000, "errmsg": "E11000 duplicate key error" },
                { "index": 2, "code": 91, "errmsg": "The server is in quiesce mode" },
            ],
        })
        .expect("a server's insert reply deserializes");

        MongoDbCollectionEmitter::apply_insert_many_error(
            &rows,
            DriverError::from(ErrorKind::InsertMany(insert_error)),
            &mut outcome,
        );

        let outcome = outcome.into_parts();
        assert_eq!(
            outcome.delivered,
            [EmitterRecordPosition {
                batch_index: 7,
                row_index: 10,
            }]
        );
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(
            outcome.rejected[0].id,
            EmitterRecordPosition {
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
                "at": mongodb::bson::DateTime::from_millis(1_700_000_000_123),
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
            EmitterRecordPosition {
                batch_index: 2,
                row_index: 5,
            },
            Timestamp::from_unix_nanos(11),
        );

        assert_eq!(
            record.id,
            EmitterRecordPosition {
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
            position: EmitterRecordPosition {
                batch_index: 0,
                row_index,
            },
            occurred_at: Timestamp::from_unix_nanos(1),
        }
    }

    /// The write concern error a single-member replica set reports for a write that asks for a
    /// second data-bearing member, after it applied the write.
    fn unsatisfiable_write_concern() -> WriteConcernError {
        mongodb::bson::from_document(doc! {
            "code": 100,
            "codeName": "UnsatisfiableWriteConcern",
            "errmsg": "Not enough data-bearing nodes",
        })
        .expect("a server's write concern error deserializes")
    }

    /// An unordered insert whose write concern failed still applied every document it did not
    /// refuse, so those are delivered and only the refused one is rejected: resending them would
    /// store them twice. The write still fails, so the host retries what remains on its backoff.
    #[test]
    fn an_insert_whose_write_concern_failed_delivers_the_documents_it_wrote() {
        let rows = [written_row(0), written_row(1), written_row(2)];
        let mut insert_error: InsertManyError = mongodb::bson::from_document(doc! {
            "writeErrors": [{ "index": 1, "code": 11000, "errmsg": "E11000 duplicate key error" }],
        })
        .expect("a server's insert reply deserializes");
        insert_error.write_concern_error = Some(unsatisfiable_write_concern());
        let mut outcome = PerRecordOutcome::empty();

        MongoDbCollectionEmitter::apply_insert_many_error(
            &rows,
            DriverError::from(ErrorKind::InsertMany(insert_error)),
            &mut outcome,
        );

        let outcome = outcome.into_parts();
        assert_eq!(outcome.delivered, [rows[0].position, rows[2].position]);
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].id, rows[1].position);
        assert!(
            outcome.infrastructure_error.is_some(),
            "a write concern the server could not satisfy fails the write"
        );
    }

    /// A bulk write whose write concern failed reports a result for every upsert it applied, so
    /// those are delivered, a refused one is rejected, and a row it reported nothing for is left
    /// unresolved for the host to retry.
    #[test]
    fn a_bulk_write_whose_write_concern_failed_delivers_the_upserts_it_wrote() {
        let rows = [
            written_row(0),
            written_row(1),
            written_row(2),
            written_row(3),
        ];
        let mut written = VerboseBulkWriteResult::default();
        written.update_results.insert(0, UpdateResult::default());
        written.update_results.insert(2, UpdateResult::default());
        let mut bulk_error = BulkWriteError::default();
        bulk_error.partial_result = Some(PartialBulkWriteResult::Verbose(written));
        bulk_error.write_errors.insert(
            1,
            mongodb::bson::from_document::<WriteError>(doc! {
                "code": 11000,
                "errmsg": "E11000 duplicate key error",
            })
            .expect("a server's write error deserializes"),
        );
        bulk_error
            .write_concern_errors
            .push(unsatisfiable_write_concern());
        let mut outcome = PerRecordOutcome::empty();

        MongoDbCollectionEmitter::apply_bulk_write_error(
            &rows,
            DriverError::from(ErrorKind::BulkWrite(bulk_error)),
            &mut outcome,
        );

        let outcome = outcome.into_parts();
        assert_eq!(outcome.delivered, [rows[0].position, rows[2].position]);
        assert_eq!(outcome.rejected.len(), 1);
        assert_eq!(outcome.rejected[0].id, rows[1].position);
        assert!(
            outcome.infrastructure_error.is_some(),
            "the unanswered upsert and the write concern fail the write"
        );
    }

    /// The write concern failure names the request and the server's code, which is what an
    /// emitter reports as its transient error.
    #[test]
    fn a_write_concern_failure_names_the_request_and_the_server_code() {
        let rows = [written_row(0)];
        let mut insert_error: InsertManyError =
            mongodb::bson::from_document(doc! {}).expect("an empty insert reply deserializes");
        insert_error.write_concern_error = Some(unsatisfiable_write_concern());
        let mut outcome = PerRecordOutcome::empty();

        MongoDbCollectionEmitter::apply_insert_many_error(
            &rows,
            DriverError::from(ErrorKind::InsertMany(insert_error)),
            &mut outcome,
        );

        let outcome = outcome.into_parts();
        assert_eq!(outcome.delivered, [rows[0].position]);
        let failure = outcome
            .infrastructure_error
            .expect("the write concern fails the write");
        assert_eq!(
            failure_message(&failure),
            Some(
                "MongoDB insert_many write concern failed with code 100 \
                 (UnsatisfiableWriteConcern)"
            )
        );
    }

    #[test]
    fn a_write_concern_without_a_code_name_retains_its_numeric_code() {
        use meticulous::OptionExt as _;

        let rows = [written_row(0)];
        let mut write_concern = unsatisfiable_write_concern();
        write_concern.code_name.clear();
        let answers = WriteAnswers::new(FailedRequest::InsertMany, Some(&write_concern));
        let mut outcome = PerRecordOutcome::empty();

        answers.apply(&rows, &mut outcome);

        let outcome = outcome.into_parts();
        assert_eq!(outcome.delivered, [rows[0].position]);
        assert!(outcome.rejected.is_empty());
        let failure = outcome
            .infrastructure_error
            .assured("the unsatisfied write concern always fails the request");
        assert_eq!(
            failure_message(&failure),
            Some("MongoDB insert_many write concern failed with code 100")
        );
    }

    /// The message an emitter reports for `failure`: its first printable attachment.
    fn failure_message(failure: &Report<EmitterWriteError>) -> Option<&str> {
        for frame in failure.frames() {
            if let Some(message) = frame.downcast_ref::<String>() {
                return Some(message);
            }
        }
        None
    }

    /// The codes a generated refusal carries: each code a document is rejected for, then codes of
    /// failures the host retries.
    const REFUSAL_CODES: [i32; 8] = [121, 10334, 11000, 2, 64, 91, 11600, 11602];

    /// The codes a generated write concern failure carries: a write concern that timed out, one
    /// the set can never satisfy, an unknown write concern mode, and a primary that shut down or
    /// stepped down after it applied the write.
    const WRITE_CONCERN_CODES: [i32; 5] = [64, 100, 79, 91, 11602];

    /// One more than the largest index a generated answer names, beyond the twelve rows the
    /// largest generated write carries, so some answers name documents the write never carried.
    const ANSWERED_INDEXES: u8 = 16;

    /// A refusal a server reports for a failed write: the document it names and its code.
    #[derive(Debug, bolero::TypeGenerator)]
    struct GeneratedRefusal {
        document: u8,
        code: u8,
    }

    impl GeneratedRefusal {
        fn document(&self) -> usize {
            usize::from(self.document % ANSWERED_INDEXES)
        }

        fn code(&self) -> i32 {
            REFUSAL_CODES[usize::from(self.code) % REFUSAL_CODES.len()]
        }
    }

    /// A failed unordered write of one to twelve rows and everything a server may answer for it:
    /// the operations a bulk write reports it applied, refusals, repeated refusals of one
    /// document of an insert, answers naming documents beyond the write, and a write concern
    /// failure.
    #[derive(Debug, bolero::TypeGenerator)]
    struct FailedWriteCase {
        rows: u8,
        bulk_write: bool,
        /// The operations a bulk write applied, each byte naming its index and which kind of
        /// result reports it.
        #[generator(bolero::generator::produce_with::<Vec<u8>>().len(0_usize..=16))]
        applied: Vec<u8>,
        #[generator(bolero::generator::produce_with::<Vec<GeneratedRefusal>>().len(0_usize..=16))]
        refusals: Vec<GeneratedRefusal>,
        write_concern: Option<u8>,
    }

    impl FailedWriteCase {
        fn rows(&self) -> Vec<WrittenRow> {
            let count = usize::from(self.rows % 12) + 1;
            (0..count).map(written_row).collect()
        }

        fn write_concern_error(&self) -> Option<WriteConcernError> {
            let code =
                WRITE_CONCERN_CODES[usize::from(self.write_concern?) % WRITE_CONCERN_CODES.len()];
            let error = mongodb::bson::from_document(doc! {
                "code": code,
                "codeName": "GeneratedWriteConcernFailure",
                "errmsg": "generated write concern failure",
            })
            .expect("a generated write concern error deserializes");
            Some(error)
        }

        /// The driver's error for this write, exactly as the emitter receives it.
        fn driver_error(&self) -> DriverError {
            if !self.bulk_write {
                let mut write_errors = Vec::with_capacity(self.refusals.len());
                for refusal in &self.refusals {
                    write_errors.push(doc! {
                        "index": i64::try_from(refusal.document()).expect("an index fits i64"),
                        "code": refusal.code(),
                        "errmsg": "generated refusal",
                    });
                }
                let mut insert_error: InsertManyError =
                    mongodb::bson::from_document(doc! { "writeErrors": write_errors })
                        .expect("a generated insert reply deserializes");
                insert_error.write_concern_error = self.write_concern_error();
                return DriverError::from(ErrorKind::InsertMany(insert_error));
            }
            let mut results = VerboseBulkWriteResult::default();
            for applied in &self.applied {
                let index = usize::from(applied % ANSWERED_INDEXES);
                match applied / ANSWERED_INDEXES % 3 {
                    0 => {
                        results
                            .insert_results
                            .insert(index, InsertOneResult::default());
                    }
                    1 => {
                        results
                            .update_results
                            .insert(index, UpdateResult::default());
                    }
                    _ => {
                        results
                            .delete_results
                            .insert(index, DeleteResult::default());
                    }
                }
            }
            let mut bulk_error = BulkWriteError::default();
            bulk_error.partial_result = Some(PartialBulkWriteResult::Verbose(results));
            for refusal in &self.refusals {
                let write_error = mongodb::bson::from_document::<WriteError>(doc! {
                    "code": refusal.code(),
                    "errmsg": "generated refusal",
                })
                .expect("a generated write error deserializes");
                bulk_error
                    .write_errors
                    .insert(refusal.document(), write_error);
            }
            bulk_error
                .write_concern_errors
                .extend(self.write_concern_error());
            DriverError::from(ErrorKind::BulkWrite(bulk_error))
        }

        /// Every code the server reported for each refused document. A bulk write reports one
        /// refusal per operation, its last; an insert reports each refusal it made.
        fn reported_refusals(&self) -> BTreeMap<usize, Vec<i32>> {
            let mut reported = BTreeMap::<usize, Vec<i32>>::new();
            for refusal in &self.refusals {
                let codes = reported.entry(refusal.document()).or_default();
                if self.bulk_write {
                    codes.clear();
                }
                codes.push(refusal.code());
            }
            reported
        }

        /// The operations a bulk write reports it applied.
        fn reported_applied(&self) -> BTreeSet<usize> {
            let mut applied = BTreeSet::new();
            if self.bulk_write {
                for operation in &self.applied {
                    applied.insert(usize::from(operation % ANSWERED_INDEXES));
                }
            }
            applied
        }
    }

    /// Every row of a failed unordered write is answered once, exactly as the server's answers
    /// decide it: delivered when the server applied it and refused it nowhere, rejected when one
    /// refusal for good names it, and otherwise left unresolved. The write fails exactly when a
    /// row is unresolved, an answer names a document beyond the write, or the write concern
    /// failed, and that failure names the request and the write concern's code.
    #[test]
    fn bolero_failed_write_answers_partition_the_write() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(256)
            .with_type::<FailedWriteCase>()
            .for_each(|case| {
                let rows = case.rows();
                let mut outcome = PerRecordOutcome::empty();
                if case.bulk_write {
                    MongoDbCollectionEmitter::apply_bulk_write_error(
                        &rows,
                        case.driver_error(),
                        &mut outcome,
                    );
                } else {
                    MongoDbCollectionEmitter::apply_insert_many_error(
                        &rows,
                        case.driver_error(),
                        &mut outcome,
                    );
                }
                let outcome = outcome.into_parts();

                let refusals = case.reported_refusals();
                let applied = case.reported_applied();
                let mut delivered = Vec::new();
                let mut rejected = Vec::new();
                let mut unresolved = false;
                for (index, row) in rows.iter().enumerate() {
                    let codes: &[i32] = match refusals.get(&index) {
                        Some(codes) => codes,
                        None => &[],
                    };
                    match codes {
                        [code] if is_record_write_error(*code) => {
                            rejected.push((row.position, *code));
                        }
                        [] if !case.bulk_write || applied.contains(&index) => {
                            delivered.push(row.position);
                        }
                        _ => unresolved = true,
                    }
                }
                let names_foreign_document = refusals.range(rows.len()..).next().is_some()
                    || applied.range(rows.len()..).next().is_some();

                assert_eq!(outcome.delivered, delivered);
                let mut observed_rejections = Vec::with_capacity(outcome.rejected.len());
                for rejection in &outcome.rejected {
                    observed_rejections.push((rejection.id, rejection.error.message.clone()));
                }
                let mut expected_rejections = Vec::with_capacity(rejected.len());
                for (position, code) in rejected {
                    expected_rejections.push((
                        position,
                        format!("MongoDB rejected document with code {code}"),
                    ));
                }
                // The reference answers each row once, so equal answers answer each row once too.
                assert_eq!(observed_rejections, expected_rejections);

                let request = if case.bulk_write {
                    "bulk_write"
                } else {
                    "insert_many"
                };
                let expected_failure = match case.write_concern_error() {
                    Some(write_concern) => Some(format!(
                        "MongoDB {request} write concern failed with code {} \
                         (GeneratedWriteConcernFailure)",
                        write_concern.code
                    )),
                    None if unresolved || names_foreign_document => {
                        Some(format!("MongoDB {request} request failed"))
                    }
                    None => None,
                };
                match (&outcome.infrastructure_error, &expected_failure) {
                    (Some(failure), Some(expected)) => {
                        assert_eq!(failure_message(failure), Some(expected.as_str()));
                    }
                    (None, None) => {}
                    (observed, expected) => {
                        panic!("the write failed as {observed:?}, but should fail as {expected:?}");
                    }
                }
            });
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
        assert!(stored_bytes > MONGODB_MAX_DOCUMENT_BYTES);
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
