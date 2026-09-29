//! Postgres sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The node's shared Postgres pool and its connection options, the declared type of
//!   each mapped column, the `unnest` insert every write becomes under the emitter's `BATCH` limits
//!   and the size of one protocol message, its text parameters and their exact encoded size, the
//!   `ON CONFLICT` clause of a conflict action, and SQLSTATE classification.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio
//!   and the `sqlx` driver.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

use std::{num::NonZeroU64, ops::Range, time::Duration};

use ahash::HashMap;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::DateTime;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    MappedSinkMember, MappedSinkRows, MeasuredRequest, PerRecordOutcome, RejectedSinkRecord,
    RowRequest, RowRequestLimits, RowSink, SinkHost, SinkLifecycle, SinkPublishError,
    SinkPublishResult, SinkRecordPosition, SinkStartError, SinkStartResult, client_tls_paths,
    optional_client_config_value,
};
use nervix_models::{ClientConfigEntry, ClientPoolBounds, EmitterBatchPolicy, TableName};
use nonzero_ext::nonzero;
use sqlx::{
    AssertSqlSafe, Executor as _, Row as _,
    pool::PoolConnection,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode, Postgres},
};
use tracing::{debug, trace};
use url::Url;

const POSTGRES: &str = "postgres";

/// What `MAX SIZE` measures on a Postgres write, which an oversized row's rejection names.
const MEASURED_REQUEST: &str = "Postgres insert";

/// The longest protocol message the Postgres server reads, `PQ_LARGE_MESSAGE_LIMIT`: one byte
/// below the largest allocation it makes. A Bind message above it closes the connection.
///
/// A write's measured size — its statement text and the arrays it binds — is never smaller than
/// the Bind message that carries those arrays: the statement names every column several times,
/// while the message adds a few bytes per parameter around them. Keeping the measured size within
/// this limit therefore keeps every message within it, and every bound array within the 1 GB a
/// single value may hold.
const MESSAGE_LIMIT: NonZeroU64 = nonzero!(1_073_741_822_u64);

/// The binary header of a one-dimensional array parameter: its dimension count, flags, element
/// type, length and lower bound, four bytes each.
const ARRAY_HEADER_BYTES: u64 = 20;

/// The length word before every element of an array parameter, which alone encodes a NULL.
const ELEMENT_LENGTH_BYTES: u64 = 4;

/// How long a borrower waits for a connection, including pool wait, establishment,
/// authentication and validation. Independent of how long an accepted query then runs.
const ACQUIRE_DEADLINE: Duration = Duration::from_secs(30);

/// How long a connection above the maintained minimum may sit idle before it is retired.
const IDLE_LIFETIME: Duration = Duration::from_secs(10 * 60);

/// How old any connection may become before it is retired and replaced within the same maximum.
const MAX_LIFETIME: Duration = Duration::from_secs(30 * 60);

/// The node's shared Postgres pool for one named client.
pub struct PostgresPool(sqlx::postgres::PgPool);

/// One connection borrowed from the shared pool, returned to it when this is dropped.
pub struct PostgresConnection(PoolConnection<Postgres>);

/// The node-owned pool this sink borrows connections from while its host holds the lease.
#[async_trait]
pub trait PostgresConnections: Send + Sync + 'static {
    /// Borrow one connection for one operation, with the host's pool wait recorded around it.
    async fn connection(&self) -> SinkPublishResult<PostgresConnection>;
}

/// What one Postgres emitter inserts with, from its typed sink plan.
pub struct PostgresSinkConfig {
    pub table: TableName,
    pub conflict_action: PostgresConflictAction,
    /// The emitter's `BATCH` limits, which bound the rows and the measured bytes of every insert.
    pub batch: EmitterBatchPolicy,
}

/// What an insert does with a row the target table already holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresConflictAction {
    None,
    DoNothing { target: Vec<String> },
    DoUpdate { target: Vec<String> },
}

/// The Postgres sink, which binds each mapped column as one text array of an `unnest` insert.
pub struct PostgresSink {
    connections: Box<dyn PostgresConnections>,
    table: TableName,
    conflict_action: PostgresConflictAction,
    limits: RowRequestLimits,
}

#[derive(Debug, thiserror::Error)]
enum PostgresWriteError {
    #[error("failed to load Postgres table metadata: {0}")]
    Metadata(sqlx::Error),
    #[error("Postgres table '{table}' has no column '{column}'")]
    MissingColumn { table: String, column: String },
    #[error("invalid Postgres VALUES: {0}")]
    InvalidValues(String),
    #[error("Postgres insert failed: {0}")]
    Execute(sqlx::Error),
    #[error("{0}")]
    Pool(String),
}

/// The SQLSTATE a database error carries, when it came from the server at all.
fn sqlstate(error: &sqlx::Error) -> Option<String> {
    let sqlx::Error::Database(error) = error else {
        return None;
    };
    error.code().map(|code| code.into_owned())
}

/// A SQLSTATE the server reports for a row, or for a set of rows, it will never accept.
///
/// Data exceptions and integrity violations name a row the table refuses. A cardinality violation
/// is how `ON CONFLICT DO UPDATE` refuses one insert whose rows share a conflict key; the same rows
/// inserted one at a time each update what the one before them wrote.
fn is_record_sqlstate(code: &str) -> bool {
    code.starts_with("21") || code.starts_with("22") || code.starts_with("23")
}

impl PostgresWriteError {
    fn is_record_error(&self) -> bool {
        let Self::Execute(error) = self else {
            return false;
        };
        match sqlstate(error) {
            Some(code) => is_record_sqlstate(&code),
            None => false,
        }
    }

    fn record_reason(&self) -> String {
        let code = match self {
            Self::Execute(error) => sqlstate(error),
            _ => None,
        };
        match code {
            Some(code) => format!("Postgres rejected record with SQLSTATE {code}"),
            None => "Postgres rejected record".to_string(),
        }
    }

    fn into_report(self) -> Report<SinkPublishError> {
        let publish = || Report::new(SinkPublishError::Publish { sink: POSTGRES });
        match self {
            Self::InvalidValues(reason) => publish().attach_printable(reason),
            Self::Execute(error) => {
                let code = match sqlstate(&error) {
                    Some(code) => code,
                    None => "unknown".to_string(),
                };
                publish().attach_printable(format!("Postgres request failed with SQLSTATE {code}"))
            }
            error => publish().attach_printable(error.to_string()),
        }
    }
}

impl PostgresPool {
    /// Open the node's shared Postgres pool for one named client, sized by its declared bounds.
    ///
    /// SQLx's own pool enforces the ceiling, validates a connection before handing it out, and
    /// retires idle and aged connections within the same maximum. The acquisition deadline covers
    /// pool wait, establishment, authentication and validation, and is independent of how long an
    /// accepted query then runs.
    pub async fn open(
        config: &[ClientConfigEntry],
        bounds: ClientPoolBounds,
    ) -> SinkStartResult<Self> {
        let Some(addr) = optional_client_config_value(config, "addr") else {
            return Err(
                Report::new(SinkStartError::InvalidConfiguration { sink: POSTGRES })
                    .attach_printable("missing Postgres client config key 'addr'"),
            );
        };
        let options = connect_options(addr, config)?;
        PgPoolOptions::new()
            .min_connections(bounds.minimum())
            .max_connections(bounds.maximum().get())
            .acquire_timeout(ACQUIRE_DEADLINE)
            .idle_timeout(Some(IDLE_LIFETIME))
            .max_lifetime(Some(MAX_LIFETIME))
            .test_before_acquire(true)
            .connect_with(options)
            .await
            .map(Self)
            .map_err(|source| {
                Report::new(SinkStartError::Initialize { sink: POSTGRES })
                    .attach_printable(source.to_string())
            })
    }

    /// Borrow one connection, which returns to the pool when the borrow is dropped.
    pub async fn connection(&self) -> SinkPublishResult<PostgresConnection> {
        self.0
            .acquire()
            .await
            .map(PostgresConnection)
            .map_err(|source| {
                Report::new(SinkPublishError::NotInitialized { sink: POSTGRES })
                    .attach_printable(source.to_string())
            })
    }
}

/// The connection options one Postgres client connects with.
///
/// The URL carries the endpoint, the user and the database explicitly, and selects one of two TLS
/// policies. Mounted files are the client's TLS-file interface: an opportunistic fallback and an
/// encrypted connection without peer verification are both refused rather than silently allowed.
fn connect_options(addr: &str, config: &[ClientConfigEntry]) -> SinkStartResult<PgConnectOptions> {
    let invalid = |reason: String| {
        Report::new(SinkStartError::InvalidConfiguration { sink: POSTGRES })
            .attach_printable(reason)
    };
    let url = Url::parse(addr).map_err(|source| invalid(format!("invalid addr: {source}")))?;
    if url.scheme() != "postgres" && url.scheme() != "postgresql" {
        return Err(invalid(format!(
            "addr must be a postgres:// or postgresql:// URL, found '{}'",
            url.scheme()
        )));
    }
    let mut options: PgConnectOptions = addr
        .parse()
        .map_err(|source| invalid(format!("invalid addr: {source}")))?;

    let ssl_mode = url
        .query_pairs()
        .find(|(key, _)| key == "sslmode")
        .map(|(_, value)| value.to_string());
    let verify_full = match ssl_mode.as_deref() {
        Some("verify-full") => true,
        Some("disable") => false,
        Some(other) => {
            return Err(invalid(format!(
                "sslmode must be 'disable' or 'verify-full', found '{other}'"
            )));
        }
        None => {
            return Err(invalid(
                "addr must select sslmode=disable or sslmode=verify-full".to_string(),
            ));
        }
    };
    options = options.ssl_mode(if verify_full {
        PgSslMode::VerifyFull
    } else {
        PgSslMode::Disable
    });

    let tls = client_tls_paths(config);
    if !tls.is_empty() && !verify_full {
        return Err(invalid(
            "TLS files require sslmode=verify-full on the client addr".to_string(),
        ));
    }
    match (&tls.cert_file, &tls.key_file) {
        (Some(cert_file), Some(key_file)) => {
            options = options.ssl_client_cert(cert_file).ssl_client_key(key_file);
        }
        (None, None) => {}
        _ => {
            return Err(invalid(
                "TLS client authentication requires both 'tls_cert_file' and 'tls_key_file'"
                    .to_string(),
            ));
        }
    }
    if let Some(ca_file) = &tls.ca_file {
        options = options.ssl_root_cert(ca_file);
    }
    Ok(options)
}

impl PostgresSink {
    pub fn new(
        config: PostgresSinkConfig,
        connections: Box<dyn PostgresConnections>,
        _host: SinkHost,
    ) -> SinkStartResult<Self> {
        let PostgresSinkConfig {
            table,
            conflict_action,
            batch,
        } = config;
        Ok(Self {
            connections,
            table,
            conflict_action,
            limits: RowRequestLimits::from(batch).with_native_bytes(MESSAGE_LIMIT),
        })
    }

    fn quote_ident(identifier: &str) -> String {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    /// The declared type of each mapped column, read on a connection borrowed for this lookup
    /// alone and returned before the inserts that follow it.
    async fn column_types(&self, columns: &[String]) -> Result<Vec<String>, PostgresWriteError> {
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|error| PostgresWriteError::Pool(format!("{error:?}")))?;
        let table_name = self.table.as_str().to_string();
        let column_refs = columns.to_vec();
        let rows = sqlx::query(
            "SELECT a.attname, a.atttypid::regtype::text FROM pg_attribute a WHERE a.attrelid = \
             to_regclass($1) AND a.attname = ANY($2::text[]) AND a.attnum > 0 AND NOT \
             a.attisdropped",
        )
        .bind(table_name)
        .bind(column_refs)
        .fetch_all(&mut *connection.0)
        .await
        .map_err(PostgresWriteError::Metadata)?;
        let types_by_column = rows
            .into_iter()
            .map(|row| {
                let column: String = row.get(0);
                let ty: String = row.get(1);
                (column, ty)
            })
            .collect::<HashMap<_, _>>();
        columns
            .iter()
            .map(|column| {
                types_by_column.get(column).cloned().ok_or_else(|| {
                    PostgresWriteError::MissingColumn {
                        table: self.table.as_str().to_string(),
                        column: column.clone(),
                    }
                })
            })
            .collect()
    }

    /// The one statement every insert of a write executes: it binds one text array per mapped
    /// column, however many rows the insert carries, and casts each to the column's declared type.
    fn insert_sql(&self, columns: &[String], column_types: &[String]) -> SinkPublishResult<String> {
        let param_refs = (1..=columns.len())
            .map(|index| format!("${index}::text[]"))
            .collect::<Vec<_>>()
            .join(", ");
        let unnest_columns = columns
            .iter()
            .map(|column| Self::quote_ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let select_columns = columns
            .iter()
            .zip(column_types.iter())
            .map(|(column, ty)| format!("t.{}::{}", Self::quote_ident(column), ty))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_columns = unnest_columns.clone();
        let conflict_clause = Self::conflict_clause(columns, &self.conflict_action)
            .map_err(PostgresWriteError::into_report)?;
        Ok(format!(
            "INSERT INTO {} ({insert_columns}) SELECT {select_columns} FROM unnest({param_refs}) \
             AS t({unnest_columns}){conflict_clause}",
            Self::quote_ident(self.table.as_str()),
        ))
    }

    /// One insert of `members`, on a connection borrowed for that insert and returned after it, so
    /// a write of several inserts lets other local emitters through between them.
    async fn insert(
        &self,
        inserts: &UnnestInserts,
        members: Range<usize>,
    ) -> Result<u64, PostgresWriteError> {
        // Every value is a bound parameter and every identifier went through `quote_ident`, so the
        // only thing interpolated into this statement is a quoted name or a positional placeholder.
        let mut query = sqlx::query(AssertSqlSafe(inserts.sql.as_str()));
        for column in &inserts.columns {
            let texts = column
                .get(members.clone())
                .assured("an insert carries rows of the write whose texts were bound");
            query = query.bind(texts);
        }
        let mut connection = self
            .connections
            .connection()
            .await
            .map_err(|error| PostgresWriteError::Pool(format!("{error:?}")))?;
        let result = connection
            .0
            .execute(query)
            .await
            .map_err(PostgresWriteError::Execute)?;
        Ok(result.rows_affected())
    }

    fn conflict_clause(
        columns: &[String],
        action: &PostgresConflictAction,
    ) -> Result<String, PostgresWriteError> {
        match action {
            PostgresConflictAction::None => Ok(String::new()),
            PostgresConflictAction::DoNothing { target } => {
                let target = Self::conflict_target_sql(target)?;
                Ok(format!(" ON CONFLICT{target} DO NOTHING"))
            }
            PostgresConflictAction::DoUpdate { target } => {
                if target.is_empty() {
                    return Err(PostgresWriteError::InvalidValues(
                        "Postgres ON CONFLICT DO UPDATE requires a conflict target".to_string(),
                    ));
                }
                let assignments = columns
                    .iter()
                    .filter(|column| !target.contains(column))
                    .map(|column| {
                        let quoted = Self::quote_ident(column);
                        format!("{quoted} = EXCLUDED.{quoted}")
                    })
                    .collect::<Vec<_>>();
                if assignments.is_empty() {
                    return Err(PostgresWriteError::InvalidValues(
                        "Postgres ON CONFLICT DO UPDATE requires at least one non-conflict VALUES \
                         column to update"
                            .to_string(),
                    ));
                }
                let target = Self::conflict_target_sql(target)?;
                Ok(format!(
                    " ON CONFLICT{target} DO UPDATE SET {}",
                    assignments.join(", ")
                ))
            }
        }
    }

    fn conflict_target_sql(target: &[String]) -> Result<String, PostgresWriteError> {
        if target.is_empty() {
            Ok(String::new())
        } else if target.iter().any(|column| column.is_empty()) {
            Err(PostgresWriteError::InvalidValues(
                "Postgres ON CONFLICT target columns must not be empty".to_string(),
            ))
        } else {
            Ok(format!(
                " ({})",
                target
                    .iter()
                    .map(|column| Self::quote_ident(column))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        }
    }
}

#[async_trait]
impl SinkLifecycle for PostgresSink {}

#[async_trait]
impl RowSink for PostgresSink {
    /// Writes the rows of every carrier in `unnest` inserts of at most `MAX MESSAGES` rows whose
    /// statement and bound arrays measure at most `MAX SIZE` bytes.
    async fn publish(&mut self, rows: MappedSinkRows<'_>) -> PerRecordOutcome<SinkRecordPosition> {
        let members = rows.members();
        let mut outcome = PerRecordOutcome::with_capacity(members.len());
        let mut carriers = Vec::with_capacity(rows.carriers.len());
        for carrier in &rows.carriers {
            match MappedTextColumns::new(carrier.batch, rows.target_columns) {
                Ok(columns) => carriers.push(columns),
                Err(error) => {
                    outcome.fail(
                        Report::new(SinkPublishError::Publish { sink: POSTGRES })
                            .attach_printable(error.to_string()),
                    );
                    return outcome;
                }
            }
        }
        let column_types = match self.column_types(rows.target_columns).await {
            Ok(column_types) => column_types,
            Err(error) => {
                outcome.fail(error.into_report());
                return outcome;
            }
        };
        let sql = match self.insert_sql(rows.target_columns, &column_types) {
            Ok(sql) => sql,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        let inserts = UnnestInserts::bind(sql, &carriers, &members, rows.target_columns.len());
        let requests = self
            .limits
            .divide(members.len(), |candidate| MeasuredRequest {
                size: inserts.measure(candidate),
                request: (),
            });
        if requests.subdivisions > 0 {
            debug!(
                table = self.table.as_str(),
                subdivisions = requests.subdivisions,
                "halved Postgres inserts that exceeded their byte limit"
            );
        }
        for request in requests.requests {
            nervix_primitives::task::consume_budget().await;
            let written = match request {
                RowRequest::Write {
                    members: written, ..
                } => written,
                RowRequest::Oversize { member, oversize } => {
                    let member = members[member];
                    outcome.reject(oversize.rejected(
                        rows.position(member),
                        rows.occurred_at(member),
                        MEASURED_REQUEST,
                    ));
                    continue;
                }
            };
            match self.insert(&inserts, written.clone()).await {
                Ok(_) => {
                    for index in written {
                        outcome.deliver(rows.position(members[index]));
                    }
                }
                // A record-specific failure of a multi-row insert is isolated by inserting each of
                // its rows alone, so healthy rows land and only the rejected ones follow the error
                // policy.
                Err(error) if error.is_record_error() && written.len() > 1 => {
                    for index in written {
                        nervix_primitives::task::consume_budget().await;
                        let member = members[index];
                        let alone = index
                            .checked_add(1)
                            .assured("a row of the write is followed by at most its end");
                        match self.insert(&inserts, index..alone).await {
                            Ok(_) => outcome.deliver(rows.position(member)),
                            Err(error) if error.is_record_error() => {
                                outcome.reject(RejectedSinkRecord::external(
                                    rows.position(member),
                                    rows.occurred_at(member),
                                    error.record_reason(),
                                ));
                            }
                            Err(error) => {
                                outcome.fail(error.into_report());
                                return outcome;
                            }
                        }
                    }
                }
                Err(error) if error.is_record_error() => {
                    let member = members[written.start];
                    outcome.reject(RejectedSinkRecord::external(
                        rows.position(member),
                        rows.occurred_at(member),
                        error.record_reason(),
                    ));
                }
                Err(error) => {
                    outcome.fail(error.into_report());
                    return outcome;
                }
            }
        }
        trace!(
            table = self.table.as_str(),
            rows = members.len(),
            "emitter published postgres rows"
        );
        outcome
    }
}

/// The inserts of one write: the one `unnest` statement they all execute, the text every mapped
/// column binds for each of the write's rows, and the size those texts add to an insert.
struct UnnestInserts {
    sql: String,
    /// One text per row for each mapped column, in the order the write carries its rows.
    columns: Vec<Vec<Option<String>>>,
    /// The bytes the elements of the first `n + 1` rows add to the arrays an insert binds, for
    /// each `n`.
    element_ends: Vec<u64>,
}

impl UnnestInserts {
    fn bind(
        sql: String,
        carriers: &[MappedTextColumns<'_>],
        members: &[MappedSinkMember],
        column_count: usize,
    ) -> Self {
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(Vec::with_capacity(members.len()));
        }
        let mut element_ends = Vec::with_capacity(members.len());
        let mut elements = 0_u64;
        for member in members {
            let mapped = carriers
                .get(member.carrier)
                .assured("every carrier of the write was mapped before its rows were bound");
            for (column, values) in columns.iter_mut().zip(&mapped.values) {
                let text = values.text(member.row);
                elements = elements
                    .checked_add(Self::element_bytes(text.as_deref()))
                    .assured("the texts one write binds are held in memory");
                column.push(text);
            }
            element_ends.push(elements);
        }
        Self {
            sql,
            columns,
            element_ends,
        }
    }

    /// The bytes one element adds to a text array as the extended-query protocol encodes it: a
    /// length word, then the text, or the length word alone for a NULL.
    fn element_bytes(text: Option<&str>) -> u64 {
        let text_bytes = match text {
            Some(text) => u64::try_from(text.len())
                .assured("Nervix builds for 64-bit targets only, where u64 holds usize"),
            None => 0,
        };
        ELEMENT_LENGTH_BYTES
            .checked_add(text_bytes)
            .assured("one text this node holds in memory")
    }

    /// The exact size an insert of `members` is measured at: its statement text and every array
    /// it binds, each as the extended-query protocol encodes it. The protocol messages around
    /// them, such as each parameter's own length word, are framing outside the measure.
    fn measure(&self, members: Range<usize>) -> u64 {
        let start = match members.start.checked_sub(1) {
            Some(previous) => *self
                .element_ends
                .get(previous)
                .assured("an insert starts at a row the write bound"),
            None => 0,
        };
        let last = members
            .end
            .checked_sub(1)
            .assured("an insert carries at least one row");
        let end = *self
            .element_ends
            .get(last)
            .assured("an insert ends at a row the write bound");
        let elements = end
            .checked_sub(start)
            .assured("the running size of later rows is at least that of earlier ones");
        let arrays = u64::try_from(self.columns.len())
            .assured("Nervix builds for 64-bit targets only, where u64 holds usize")
            .checked_mul(ARRAY_HEADER_BYTES)
            .assured("an insert binds one array per mapped column");
        let statement = u64::try_from(self.sql.len())
            .assured("Nervix builds for 64-bit targets only, where u64 holds usize");
        let framed = statement
            .checked_add(arrays)
            .assured("the statement and array headers of one insert are held in memory");
        framed
            .checked_add(elements)
            .assured("every term measures bytes this node holds in memory")
    }
}

/// A mapped column this sink cannot bind, named with the exact type it carries.
#[derive(Debug, thiserror::Error)]
#[error("Postgres VALUES column '{column}' has unsupported exact type {data_type}")]
struct UnsupportedMappedColumn {
    column: String,
    data_type: arrow_schema::DataType,
}

/// The mapped columns of one batch, downcast once so every row binds from the column that holds it.
struct MappedTextColumns<'a> {
    values: Vec<MappedTextColumn<'a>>,
}

impl<'a> MappedTextColumns<'a> {
    fn new(
        batch: &'a RecordBatch,
        target_columns: &[String],
    ) -> Result<Self, UnsupportedMappedColumn> {
        let mut values = Vec::with_capacity(target_columns.len());
        for (index, column) in target_columns.iter().enumerate() {
            let array = batch.column(index);
            let mapped = MappedTextColumn::new(array).ok_or_else(|| UnsupportedMappedColumn {
                column: column.clone(),
                data_type: array.data_type().clone(),
            })?;
            values.push(mapped);
        }
        Ok(Self { values })
    }
}

/// One mapped column, held as the typed Arrow array it arrived in.
enum MappedTextColumn<'a> {
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
    /// Octets, bound in the hex format a `bytea` column reads back as those octets.
    Bytes(&'a BinaryArray),
    Datetime(&'a TimestampNanosecondArray),
    List {
        offsets: &'a ListArray,
        elements: Box<MappedTextColumn<'a>>,
    },
    /// A fixed-size array, such as an array literal, whose rows all hold the same element count.
    FixedList {
        list: &'a FixedSizeListArray,
        elements: Box<MappedTextColumn<'a>>,
    },
}

impl<'a> MappedTextColumn<'a> {
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

    /// The text this row binds as, which the insert casts to the column's declared type.
    fn text(&self, row: usize) -> Option<String> {
        if self.is_null(row) {
            return None;
        }
        let text = match self {
            Self::Bool(values) => values.value(row).to_string(),
            Self::U8(values) => values.value(row).to_string(),
            Self::I8(values) => values.value(row).to_string(),
            Self::U16(values) => values.value(row).to_string(),
            Self::I16(values) => values.value(row).to_string(),
            Self::U32(values) => values.value(row).to_string(),
            Self::I32(values) => values.value(row).to_string(),
            Self::U64(values) => values.value(row).to_string(),
            Self::I64(values) => values.value(row).to_string(),
            // A float binds in the shortest form that reads back as the same value, and a value
            // no decimal form represents binds as NULL rather than as a word Postgres rejects.
            Self::F32(values) => return float_text(f64::from(values.value(row))),
            Self::F64(values) => return float_text(values.value(row)),
            Self::String(values) => values.value(row).to_string(),
            Self::Bytes(values) => format!("\\x{}", faster_hex::hex_string(values.value(row))),
            Self::Datetime(values) => DateTime::from_timestamp_nanos(values.value(row))
                .fixed_offset()
                .to_rfc3339(),
            // A list binds as the JSON text of its elements, which is what a JSON column reads.
            Self::List { .. } | Self::FixedList { .. } => self.json_value(row).to_string(),
        };
        Some(text)
    }

    fn json_value(&self, row: usize) -> serde_json::Value {
        if self.is_null(row) {
            return serde_json::Value::Null;
        }
        match self {
            Self::Bool(values) => serde_json::Value::from(values.value(row)),
            Self::U8(values) => serde_json::Value::from(values.value(row)),
            Self::I8(values) => serde_json::Value::from(values.value(row)),
            Self::U16(values) => serde_json::Value::from(values.value(row)),
            Self::I16(values) => serde_json::Value::from(values.value(row)),
            Self::U32(values) => serde_json::Value::from(values.value(row)),
            Self::I32(values) => serde_json::Value::from(values.value(row)),
            Self::U64(values) => serde_json::Value::from(values.value(row)),
            Self::I64(values) => serde_json::Value::from(values.value(row)),
            Self::F32(values) => serde_json::Value::from(values.value(row)),
            Self::F64(values) => serde_json::Value::from(values.value(row)),
            Self::String(values) => serde_json::Value::from(values.value(row)),
            // Inside JSON, octets are the canonical padded base64 text every Nervix JSON value
            // carries them as.
            Self::Bytes(values) => serde_json::Value::from(BASE64.encode(values.value(row))),
            Self::Datetime(values) => serde_json::Value::from(
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
                elements.json_array(start..end)
            }
            Self::FixedList { list, elements } => {
                let non_negative = "Arrow builds fixed-size list offsets and widths as \
                                    non-negative element counts";
                let start = usize::try_from(list.value_offset(row)).assured(non_negative);
                let width = usize::try_from(list.value_length()).assured(non_negative);
                let end = start
                    .checked_add(width)
                    .assured("a fixed-size list row ends inside its element array");
                elements.json_array(start..end)
            }
        }
    }

    /// The elements `elements` of this column as one JSON array.
    fn json_array(&self, elements: Range<usize>) -> serde_json::Value {
        let mut items = Vec::with_capacity(elements.len());
        for element in elements {
            items.push(self.json_value(element));
        }
        serde_json::Value::Array(items)
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

/// The decimal text of a float, or nothing for a value no decimal form represents.
fn float_text(value: f64) -> Option<String> {
    let number = serde_json::Number::from_f64(value)?;
    Some(number.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use arrow_array::builder::{BinaryBuilder, ListBuilder};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn classifies_only_data_and_constraint_sqlstates_as_record_errors() {
        for code in ["22001", "22003", "22P02", "23000", "23502", "23505"] {
            assert!(
                is_record_sqlstate(code),
                "{code} should be a definitive record error"
            );
        }
        for code in ["08006", "40001", "42P01", "53300", "57P01"] {
            assert!(
                !is_record_sqlstate(code),
                "{code} requires infrastructure retry"
            );
        }
    }

    #[test]
    fn binds_one_text_array_per_mapped_column() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("at", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![Some(7), Some(8), None])),
                StdArc::new(Float64Array::from(vec![Some(1.5), None, Some(2.0)])),
                StdArc::new(TimestampNanosecondArray::from(vec![
                    Some(1_700_000_000_123_456_789),
                    None,
                    None,
                ])),
            ],
        )
        .expect("the mapped batch should build");
        let names = ["id".to_string(), "score".to_string(), "at".to_string()];

        let columns = MappedTextColumns::new(&batch, &names).expect("columns should be mapped");
        let inserts = UnnestInserts::bind(
            String::new(),
            &[columns],
            &[
                MappedSinkMember { carrier: 0, row: 0 },
                MappedSinkMember { carrier: 0, row: 2 },
            ],
            names.len(),
        );

        assert_eq!(
            inserts.columns,
            vec![
                vec![Some("7".to_string()), None],
                vec![Some("1.5".to_string()), Some("2.0".to_string())],
                vec![
                    Some("2023-11-14T22:13:20.123456789+00:00".to_string()),
                    None
                ],
            ]
        );
    }

    #[test]
    fn binds_bytes_as_bytea_hex_and_arrays_as_json_text() {
        let pairs = FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", DataType::Int64, false)),
            2,
            StdArc::new(Int64Array::from(vec![1, 10])),
            None,
        )
        .expect("one row of two elements builds");
        let mut blobs = ListBuilder::new(BinaryBuilder::new());
        blobs.values().append_value(b"\x00\xff");
        blobs.values().append_value(b"");
        blobs.append(true);
        let blobs = blobs.finish();
        let raw = BinaryArray::from(vec![b"\x00\xff\\".as_slice()]);
        let schema = StdArc::new(Schema::new(vec![
            Field::new("pair", pairs.data_type().clone(), true),
            Field::new("blobs", blobs.data_type().clone(), true),
            Field::new("raw", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(pairs), StdArc::new(blobs), StdArc::new(raw)],
        )
        .expect("the mapped batch should build");
        let names = ["pair".to_string(), "blobs".to_string(), "raw".to_string()];

        let columns = MappedTextColumns::new(&batch, &names).expect("columns should be mapped");

        let texts = columns
            .values
            .iter()
            .map(|column| column.text(0))
            .collect::<Vec<_>>();
        assert_eq!(
            texts,
            vec![
                Some("[1,10]".to_string()),
                Some(r#"["AP8=",""]"#.to_string()),
                Some(r"\x00ff5c".to_string()),
            ]
        );
    }

    /// The measured size is the statement text and every array exactly as the driver encodes
    /// it, so a scenario's `MAX SIZE` can be written to the byte.
    #[test]
    fn measures_the_statement_and_the_arrays_the_driver_encodes() {
        let sink = PostgresSink {
            connections: Box::new(NoConnections),
            table: TableName::parse("limit_a_t0192a1b2c3d4e5f60718293a4b5c6d7e")
                .expect("the test table name is valid"),
            conflict_action: PostgresConflictAction::None,
            limits: RowRequestLimits::from(EmitterBatchPolicy {
                max_messages: nervix_models::BatchMessageLimit::try_from(3_u32)
                    .expect("three is a valid message limit"),
                max_size: "1KiB".parse().expect("1KiB is a valid size"),
            }),
        };
        let names = ["seq".to_string(), "note".to_string()];
        let sql = sink
            .insert_sql(&names, &["bigint".to_string(), "text".to_string()])
            .expect("the insert statement builds");
        let schema = StdArc::new(Schema::new(vec![
            Field::new("seq", DataType::Int64, true),
            Field::new("note", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
                StdArc::new(StringArray::from(vec![Some("abc"), None, Some("abc")])),
            ],
        )
        .expect("the mapped batch should build");
        let columns = MappedTextColumns::new(&batch, &names).expect("columns should be mapped");
        let members = [0, 1, 2].map(|row| MappedSinkMember { carrier: 0, row });

        let inserts = UnnestInserts::bind(sql, &[columns], &members, names.len());

        assert_eq!(inserts.sql.len(), 166);
        for range in [0..1, 0..3, 1..3, 2..3] {
            let mut encoded = 0;
            for column in &inserts.columns {
                let mut buffer = sqlx::postgres::PgArgumentBuffer::default();
                let texts = &column[range.clone()];
                let written = <&[Option<String>] as sqlx::Encode<'_, Postgres>>::encode_by_ref(
                    &texts,
                    &mut buffer,
                )
                .expect("a text array encodes");
                assert!(matches!(written, sqlx::encode::IsNull::No));
                encoded += buffer.len();
            }
            let expected =
                u64::try_from(inserts.sql.len() + encoded).expect("a test insert fits u64");
            assert_eq!(inserts.measure(range), expected);
        }
    }

    /// Stands in for the pool where a test never opens a connection.
    struct NoConnections;

    #[async_trait]
    impl PostgresConnections for NoConnections {
        async fn connection(&self) -> SinkPublishResult<PostgresConnection> {
            Err(Report::new(SinkPublishError::NotInitialized {
                sink: POSTGRES,
            }))
        }
    }

    #[test]
    fn a_repeated_conflict_key_in_one_insert_is_isolated_row_by_row() {
        assert!(is_record_sqlstate("21000"));
    }
}
