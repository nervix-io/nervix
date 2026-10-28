//! MySQL emitter connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The node's shared MySQL pool and how its driver counts and hands over the
//!   connections the keeper of its declared minimum restores, the multi-row insert every write
//!   becomes under the emitter's `BATCH` limits and the placeholders one statement binds, its bind
//!   parameters and their exact encoded size, the `ON DUPLICATE KEY` clause of a conflict action,
//!   and server-error classification.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio
//!   and the `mysql_async` driver.
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

use std::{num::NonZeroUsize, ops::Range, path::PathBuf, time::Duration};

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
use mysql_async::{
    Conn, Opts, OptsBuilder, Params, Pool as DriverPool, PoolConstraints, PoolOpts, SslOpts, Value,
    prelude::Queryable as _,
};
use nervix_connector::{
    EmitterFinish, EmitterFinishResult, EmitterHost, EmitterLifecycle, EmitterOpenError,
    EmitterRecordPosition, EmitterStartResult, EmitterWriteError, EmitterWriteResult,
    MappedEmitterMember, MappedEmitterRows, MeasuredRequest, PerRecordOutcome,
    RejectedEmitterMember, RowEmitter, RowRequest, RowRequestLimits, optional_client_config_value,
};
use nervix_models::{
    ClientConfigEntry, ClientPoolBounds, EmitterBatchPolicy, MYSQL_MAX_PLACEHOLDERS, MySqlTableName,
};
use nervix_primitives::{sync::atomic::Ordering, time::Instant};
use tracing::{debug, trace};

mod pool_minimum;

use pool_minimum::{KeptPool, PoolMinimum, PoolMinimumError};

const MYSQL: &str = "mysql";

/// What `MAX SIZE` measures on a MySQL write, which an oversized row's rejection names.
const MEASURED_REQUEST: &str = "MySQL insert";

/// What separates one row's placeholders from the next in a multi-row insert.
const ROW_SEPARATOR: &str = ", ";

/// How often an idle MySQL pool is topped back up to its declared minimum.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);

/// The node's shared MySQL pool, with the task that keeps it at its declared minimum.
pub struct MySqlPool {
    pool: DriverPool,
    /// Stopped when the shared client closes, together with the pool it keeps.
    _minimum: PoolMinimum,
}

/// One connection borrowed from the shared pool, returned to it when this is dropped.
pub struct MySqlConnection(Conn);

impl MySqlConnection {
    /// Execute an insert on this borrow, retaining it for any record-error isolation that follows.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the external MySQL driver owns statement execution")
    )]
    async fn insert(
        &mut self,
        statement: &MultiRowInsert,
        rows: usize,
        params: Vec<Value>,
    ) -> error_stack::Result<u64, MySqlWriteError> {
        self.0
            .exec_drop(statement.sql(rows), Params::Positional(params))
            .await
            .map_err(MySqlWriteError::report_execute)?;
        Ok(self.0.affected_rows())
    }
}

/// The node-owned pool this emitter borrows connections from while its host holds the lease.
#[async_trait]
pub trait MySqlConnections: Send + Sync + 'static {
    /// Borrow for one insert and its row isolation, with the host's pool wait recorded around it.
    async fn connection(&self) -> EmitterWriteResult<MySqlConnection>;
}

/// What one MySQL emitter inserts with, from its typed emitter plan.
pub struct MySqlTableEmitterConfig {
    pub table: MySqlTableName,
    pub conflict_action: MySqlConflictAction,
    /// The emitter's `BATCH` limits, which bound the rows and the measured bytes of every insert.
    pub batch: EmitterBatchPolicy,
}

/// What an insert does with a row the target table already holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySqlConflictAction {
    None,
    DoNothing,
    DoUpdate,
}

/// The MySQL emitter, which binds each mapped row as the parameters of one multi-row insert.
pub struct MySqlTableEmitter {
    connections: Box<dyn MySqlConnections>,
    table: MySqlTableName,
    conflict_action: MySqlConflictAction,
    limits: RowRequestLimits,
}

#[derive(Debug, thiserror::Error)]
enum MySqlWriteError {
    #[error("MySQL ON CONFLICT DO NOTHING requires at least one VALUES column")]
    MissingInsertColumn,
    #[error("MySQL ON CONFLICT DO UPDATE requires at least one VALUES column")]
    MissingUpdateColumn,
    #[error("{reason}")]
    Pool { reason: String },
    #[error("MySQL insert failed with SQLSTATE {state} and code {code}")]
    Server { state: String, code: u16 },
    #[error("MySQL insert failed")]
    Execute(#[source] mysql_async::Error),
}

impl MySqlWriteError {
    fn report_execute(error: mysql_async::Error) -> Report<Self> {
        match error {
            // Server text can repeat a rejected parameter; its state and code are safe.
            mysql_async::Error::Server(error) => Report::new(Self::Server {
                state: error.state,
                code: error.code,
            }),
            error => Report::new(Self::Execute(error)),
        }
    }

    fn is_record_error(&self) -> bool {
        let Self::Server { state, code } = self else {
            return false;
        };
        is_record_server_error(state, *code)
    }

    fn record_reason(&self) -> String {
        let server_error = match self {
            Self::Server { state, code } => Some((state, code)),
            _ => None,
        };
        match server_error {
            Some((state, code)) => format!(
                "MySQL rejected record with SQLSTATE {} and code {}",
                state, code
            ),
            None => "MySQL rejected record".to_string(),
        }
    }

    fn into_report(report: Report<Self>) -> Report<EmitterWriteError> {
        let reason = match report.current_context() {
            Self::MissingInsertColumn | Self::MissingUpdateColumn => {
                Some(report.current_context().to_string())
            }
            Self::Server { state, code } => Some(format!(
                "MySQL request failed with SQLSTATE {} and code {}",
                state, code
            )),
            Self::Execute(error) => Some(format!("MySQL insert failed: {error}")),
            Self::Pool { reason } => Some(reason.clone()),
        };
        let report = report.change_context(EmitterWriteError::Publish { connector: MYSQL });
        match reason {
            Some(reason) => report.attach_printable(reason),
            None => report,
        }
    }
}

/// A SQLSTATE and error code the server reports for a row it will never accept.
///
/// Besides data exceptions and integrity violations, a packet above `max_allowed_packet`, a value
/// its column cannot hold, and a violated `CHECK` constraint name the rows of one insert, although
/// the server reports the last two under the generic SQLSTATE `HY000`.
fn is_record_server_error(state: &str, code: u16) -> bool {
    state.starts_with("22") || state.starts_with("23") || matches!(code, 1153 | 1366 | 3819)
}

/// The driver's pool as the keeper of its declared minimum counts and draws on it.
impl KeptPool for DriverPool {
    type Checkout = Conn;

    /// The driver counts a connection from the moment it starts opening it until it closes it,
    /// whether the connection is idle, checked out or being recycled.
    fn established(&self) -> usize {
        self.metrics().connection_count.load(Ordering::Relaxed)
    }

    /// The driver hands over a connection it holds idle before it opens a new one.
    async fn check_out(&self) -> Result<Conn, Report<PoolMinimumError>> {
        self.get_conn()
            .await
            .map_err(|error| Report::new(error).change_context(PoolMinimumError::CheckOut))
    }
}

impl MySqlPool {
    /// Open the node's shared MySQL pool for one named client, sized by its declared bounds.
    ///
    /// The bounds are the driver's own constraints, so the ceiling is enforced by the pool that
    /// hands out connections rather than by any emitter counting its own. Initialization validates
    /// one authenticated connection before the first user becomes operational, and returns it
    /// immediately.
    pub async fn open(
        config: &[ClientConfigEntry],
        bounds: ClientPoolBounds,
    ) -> EmitterStartResult<Self> {
        let invalid = || Report::new(EmitterOpenError::InvalidConfiguration { connector: MYSQL });
        let connect = || Report::new(EmitterOpenError::Initialize { connector: MYSQL });
        let Some(addr) = optional_client_config_value(config, "addr") else {
            return Err(invalid().attach_printable("missing MySQL client config key 'addr'"));
        };
        let opts = Opts::from_url(addr)
            .map_err(|source| invalid().attach_printable(format!("invalid addr: {source}")))?;
        let builder = if let Some(ca_file) = optional_client_config_value(config, "tls_ca_file") {
            let ssl_opts = SslOpts::default()
                .with_root_certs(vec![PathBuf::from(ca_file).into()])
                .with_disable_built_in_roots(true);
            OptsBuilder::from_opts(opts).ssl_opts(Some(ssl_opts))
        } else {
            OptsBuilder::from_opts(opts)
        };
        let minimum = usize::try_from(bounds.minimum()).map_err(|_| {
            invalid().attach_printable("POOL SIZE MIN exceeds this platform's pointer width")
        })?;
        let maximum = usize::try_from(bounds.maximum().get()).map_err(|_| {
            invalid().attach_printable("POOL SIZE MAX exceeds this platform's pointer width")
        })?;
        let constraints = PoolConstraints::new(minimum, maximum).ok_or_else(|| {
            invalid().attach_printable(format!(
                "driver rejected pool bounds MIN {minimum} MAX {maximum}"
            ))
        })?;
        let pool =
            DriverPool::new(builder.pool_opts(PoolOpts::default().with_constraints(constraints)));
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|source| connect().attach_printable(source.to_string()))?;
        conn.query_drop("SELECT 1").await.map_err(|source| {
            connect().attach_printable(format!("failed to validate connection: {source}"))
        })?;
        drop(conn);
        let kept_minimum = PoolMinimum::keep(pool.clone(), minimum, MAINTENANCE_INTERVAL);
        Ok(Self {
            pool,
            _minimum: kept_minimum,
        })
    }

    /// Borrow one connection, which returns to the pool when the borrow is dropped.
    pub async fn connection(&self) -> EmitterWriteResult<MySqlConnection> {
        {
            let mapping_input = self.pool.get_conn().await.map(MySqlConnection);
            match mapping_input {
                Ok(mapped_value) => Ok(mapped_value),
                Err(source) => {
                    Err(
                        Report::new(EmitterWriteError::NotInitialized { connector: MYSQL })
                            .attach_printable(source.to_string()),
                    )
                }
            }
        }
    }
}

impl MySqlTableEmitter {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow access and MySQL driver insertion own their effects"
        )
    )]
    pub fn new(
        config: MySqlTableEmitterConfig,
        connections: Box<dyn MySqlConnections>,
        _host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        let MySqlTableEmitterConfig {
            table,
            conflict_action,
            batch,
        } = config;
        Ok(Self {
            connections,
            table,
            conflict_action,
            limits: RowRequestLimits::from(batch),
        })
    }

    fn quote_ident(identifier: &str) -> String {
        format!("`{}`", identifier.replace('`', "``"))
    }

    /// The statement every insert of a write executes, around the placeholders of its rows.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "visits the vocabulary's opaque iterator over at most two \
                                   immutable table parts bounded to 128 bytes total; local \
                                   quoting callbacks remain analyzed")
    )]
    fn insert_statement(&self, columns: &[String]) -> EmitterWriteResult<MultiRowInsert> {
        let quoted_table = self
            .table
            .parts()
            .map(Self::quote_ident)
            .collect::<Vec<_>>()
            .join(".");
        let quoted_columns = columns
            .iter()
            .map(|column| Self::quote_ident(column))
            .collect::<Vec<_>>();
        let columns_sql = quoted_columns.join(", ");
        let row = format!(
            "({})",
            std::iter::repeat_n("?", quoted_columns.len())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let suffix = Self::conflict_clause(&quoted_columns, self.conflict_action)
            .map_err(MySqlWriteError::into_report)?;
        Ok(MultiRowInsert {
            prefix: format!("INSERT INTO {} ({columns_sql}) VALUES ", quoted_table),
            row,
            suffix,
        })
    }

    /// Borrow once for a bounded insert and the isolation of any rows the database refuses.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the host owns external driver-pool acquisition and its wait publication"
        )
    )]
    async fn connection(&self) -> error_stack::Result<MySqlConnection, MySqlWriteError> {
        match self.connections.connection().await {
            Ok(connection) => Ok(connection),
            Err(error) => {
                let reason = format!("{error:?}");
                Err(error.change_context(MySqlWriteError::Pool { reason }))
            }
        }
    }

    fn conflict_clause(
        quoted_columns: &[String],
        conflict_action: MySqlConflictAction,
    ) -> error_stack::Result<String, MySqlWriteError> {
        match conflict_action {
            MySqlConflictAction::None => Ok(String::new()),
            MySqlConflictAction::DoNothing => {
                let Some(column) = quoted_columns.first() else {
                    return Err(Report::new(MySqlWriteError::MissingInsertColumn));
                };
                Ok(format!(" ON DUPLICATE KEY UPDATE {column} = {column}"))
            }
            MySqlConflictAction::DoUpdate => {
                if quoted_columns.is_empty() {
                    return Err(Report::new(MySqlWriteError::MissingUpdateColumn));
                }
                let updates = quoted_columns
                    .iter()
                    .map(|column| format!("{column} = VALUES({column})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!(" ON DUPLICATE KEY UPDATE {updates}"))
            }
        }
    }
}

impl MySqlTableEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::MySql(nervix_models::MySqlEmitterEntity::Table);
}

#[async_trait]
impl EmitterLifecycle for MySqlTableEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// Each insert completes before the publish returns, so the pool holds nothing.
    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }
}

#[async_trait]
impl RowEmitter for MySqlTableEmitter {
    /// Writes the rows of every carrier in multi-row inserts of at most `MAX MESSAGES` rows and as
    /// many as fit the placeholders one statement binds, whose statement and bound values measure
    /// at most `MAX SIZE` bytes.
    async fn publish(
        &mut self,
        rows: MappedEmitterRows<'_>,
    ) -> PerRecordOutcome<EmitterRecordPosition> {
        let members = rows.members();
        let mut outcome = PerRecordOutcome::with_capacity(members.len());
        let mut carriers = Vec::with_capacity(rows.carriers.len());
        for carrier in &rows.carriers {
            match MappedMySqlColumns::new(carrier.batch, rows.target_columns) {
                Ok(columns) => carriers.push(columns),
                Err(error) => {
                    outcome.fail(
                        error.change_context(EmitterWriteError::Publish { connector: MYSQL }),
                    );
                    return outcome;
                }
            }
        }
        let column_count = rows.target_columns.len();
        let Some(rows_per_statement) = MultiRowInsert::rows_per_statement(column_count) else {
            outcome.fail(
                Report::new(EmitterWriteError::Misconfigured { connector: MYSQL })
                    .attach_printable(format!(
                        "a MySQL insert of {column_count} columns binds more than the \
                         {MYSQL_MAX_PLACEHOLDERS} placeholders one statement accepts"
                    )),
            );
            return outcome;
        };
        let statement = match self.insert_statement(rows.target_columns) {
            Ok(statement) => statement,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        let mut values = BoundValues::bind(&carriers, &members, column_count);
        let limits = self.limits.with_native_rows(rows_per_statement);
        let requests = limits.divide(members.len(), |candidate| MeasuredRequest {
            size: statement.measure(&values, candidate),
            request: (),
        });
        if requests.subdivisions > 0 {
            debug!(
                table = self.table.as_str(),
                subdivisions = requests.subdivisions,
                "halved MySQL inserts that exceeded MAX SIZE"
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
            let params = values.take(written.clone());
            let mut connection = match self.connection().await {
                Ok(connection) => connection,
                Err(error) => {
                    outcome.fail(MySqlWriteError::into_report(error));
                    return outcome;
                }
            };
            match connection.insert(&statement, written.len(), params).await {
                Ok(_) => {
                    for index in written {
                        outcome.deliver(rows.position(members[index]));
                    }
                }
                // A record-specific failure of a multi-row insert is isolated by inserting each of
                // its rows alone, so healthy rows land and only the rejected ones follow the error
                // policy. Retain this insert's connection so isolation does not queue behind a
                // different writer. Bind the failed insert's values again from the row's columns.
                Err(error) if error.current_context().is_record_error() && written.len() > 1 => {
                    for index in written {
                        nervix_primitives::task::consume_budget().await;
                        let member = members[index];
                        let params = carriers
                            .get(member.carrier)
                            .assured("every carrier of the write was mapped before it was bound")
                            .row_values(member.row);
                        match connection.insert(&statement, 1, params).await {
                            Ok(_) => outcome.deliver(rows.position(member)),
                            Err(error) if error.current_context().is_record_error() => {
                                outcome.reject(RejectedEmitterMember::external(
                                    rows.position(member),
                                    rows.occurred_at(member),
                                    error.current_context().record_reason(),
                                ));
                            }
                            Err(error) => {
                                outcome.fail(MySqlWriteError::into_report(error));
                                return outcome;
                            }
                        }
                    }
                }
                Err(error) if error.current_context().is_record_error() => {
                    let member = members[written.start];
                    outcome.reject(RejectedEmitterMember::external(
                        rows.position(member),
                        rows.occurred_at(member),
                        error.current_context().record_reason(),
                    ));
                }
                Err(error) => {
                    outcome.fail(MySqlWriteError::into_report(error));
                    return outcome;
                }
            }
        }
        trace!(
            table = self.table.as_str(),
            rows = members.len(),
            "emitter published mysql rows"
        );
        outcome
    }
}

/// The statement of a multi-row insert, which repeats one placeholder group per row it carries.
struct MultiRowInsert {
    /// `INSERT INTO <table> (<columns>) VALUES `.
    prefix: String,
    /// One row's placeholder group, such as `(?, ?)`.
    row: String,
    /// The duplicate-key clause of the conflict action, when it has one.
    suffix: String,
}

impl MultiRowInsert {
    /// How many rows of `column_count` placeholders each fit one statement, or nothing when not
    /// even one row does.
    fn rows_per_statement(column_count: usize) -> Option<NonZeroUsize> {
        let rows = MYSQL_MAX_PLACEHOLDERS.checked_div(column_count)?;
        NonZeroUsize::new(rows)
    }

    /// The statement an insert of `rows` rows executes.
    fn sql(&self, rows: usize) -> String {
        let placeholders = std::iter::repeat_n(self.row.as_str(), rows)
            .collect::<Vec<_>>()
            .join(ROW_SEPARATOR);
        format!("{}{placeholders}{}", self.prefix, self.suffix)
    }

    /// The exact size an insert of `members` is measured at: its statement text and every value
    /// it binds, as the binary protocol encodes it. The packet headers and the statement's
    /// parameter types and null bitmap are framing outside the measure.
    fn measure(&self, values: &BoundValues, members: Range<usize>) -> u64 {
        let rows = members.len();
        let separators = rows
            .checked_sub(1)
            .assured("an insert carries at least one row")
            .checked_mul(ROW_SEPARATOR.len())
            .assured("the separators of one statement are held in memory");
        let groups = rows
            .checked_mul(self.row.len())
            .assured("the placeholders of one statement are held in memory");
        let mut statement = self.prefix.len();
        for part in [groups, separators, self.suffix.len()] {
            statement = statement
                .checked_add(part)
                .assured("one statement's text is held in memory");
        }
        let statement = u64::try_from(statement)
            .assured("Nervix builds for 64-bit targets only, where u64 holds usize");
        statement
            .checked_add(values.measure(members))
            .assured("every term measures bytes this node holds in memory")
    }
}

/// The values every row of one write binds, row by row in the order the write carries its rows,
/// with the running size those values add to an insert.
struct BoundValues {
    /// Each row's values in mapping order, rows in the write's order. An insert takes the values of
    /// its rows when it runs.
    values: Vec<Value>,
    column_count: usize,
    /// The bytes the values of the first `n + 1` rows encode to, for each `n`.
    value_ends: Vec<u64>,
}

impl BoundValues {
    fn bind(
        carriers: &[MappedMySqlColumns<'_>],
        members: &[MappedEmitterMember],
        column_count: usize,
    ) -> Self {
        let mut values = Vec::with_capacity(
            members
                .len()
                .checked_mul(column_count)
                .assured("the values of one write are held in memory"),
        );
        let mut value_ends = Vec::with_capacity(members.len());
        let mut encoded = 0_u64;
        for member in members {
            let mapped = carriers
                .get(member.carrier)
                .assured("every carrier of the write was mapped before its rows were bound");
            for column in &mapped.values {
                let value = column.value(member.row);
                encoded = encoded
                    .checked_add(value.bin_len())
                    .assured("the values one write binds are held in memory");
                values.push(value);
            }
            value_ends.push(encoded);
        }
        Self {
            values,
            column_count,
            value_ends,
        }
    }

    /// The bytes the values of `members` encode to in the binary protocol.
    fn measure(&self, members: Range<usize>) -> u64 {
        let start = match members.start.checked_sub(1) {
            Some(previous) => *self
                .value_ends
                .get(previous)
                .assured("an insert starts at a row the write bound"),
            None => 0,
        };
        let last = members
            .end
            .checked_sub(1)
            .assured("an insert carries at least one row");
        let end = *self
            .value_ends
            .get(last)
            .assured("an insert ends at a row the write bound");
        end.checked_sub(start)
            .assured("the running size of later rows is at least that of earlier ones")
    }

    /// The values of `members`, taken for the insert that binds them. Every row travels in at most
    /// one insert of the write, and a row isolated after a failed insert is bound again from its
    /// columns.
    fn take(&mut self, members: Range<usize>) -> Vec<Value> {
        let start = members
            .start
            .checked_mul(self.column_count)
            .assured("a row's first value sits inside the write's values");
        let end = members
            .end
            .checked_mul(self.column_count)
            .assured("a row's last value sits inside the write's values");
        let taken = self
            .values
            .get_mut(start..end)
            .assured("an insert takes the values of rows the write bound");
        let mut params = Vec::with_capacity(taken.len());
        for value in taken {
            params.push(std::mem::replace(value, Value::NULL));
        }
        params
    }
}

/// A mapped column this emitter cannot bind, named with the exact type it carries.
#[derive(Debug, thiserror::Error)]
#[error("MySQL VALUES column '{column}' has unsupported exact type {data_type}")]
struct UnsupportedMappedColumn {
    column: String,
    data_type: arrow_schema::DataType,
}

/// The mapped columns of one batch, downcast once so every row binds from the column that holds it.
struct MappedMySqlColumns<'a> {
    values: Vec<MappedMySqlColumn<'a>>,
}

impl<'a> MappedMySqlColumns<'a> {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow access and MySQL driver insertion own their effects"
        )
    )]
    fn new(
        batch: &'a RecordBatch,
        target_columns: &[String],
    ) -> error_stack::Result<Self, UnsupportedMappedColumn> {
        let mut values = Vec::with_capacity(target_columns.len());
        for (index, column) in target_columns.iter().enumerate() {
            let array = batch.column(index);
            let mapped = MappedMySqlColumn::new(array).ok_or_else(|| {
                Report::new(UnsupportedMappedColumn {
                    column: column.clone(),
                    data_type: array.data_type().clone(),
                })
            })?;
            values.push(mapped);
        }
        Ok(Self { values })
    }

    /// The values one row binds, in mapping order.
    fn row_values(&self, row: usize) -> Vec<Value> {
        let mut values = Vec::with_capacity(self.values.len());
        for column in &self.values {
            values.push(column.value(row));
        }
        values
    }
}

/// One mapped column, held as the typed Arrow array it arrived in.
enum MappedMySqlColumn<'a> {
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
    /// Octets, bound as they are, which a binary column stores unchanged.
    Bytes(&'a BinaryArray),
    Datetime(&'a TimestampNanosecondArray),
    List {
        offsets: &'a ListArray,
        elements: Box<MappedMySqlColumn<'a>>,
    },
    /// A fixed-size array, such as an array literal, whose rows all hold the same element count.
    FixedList {
        list: &'a FixedSizeListArray,
        elements: Box<MappedMySqlColumn<'a>>,
    },
}

impl<'a> MappedMySqlColumn<'a> {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "external Arrow access and MySQL driver insertion own their effects"
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

    /// The bind parameter for one row, read from the column that holds it.
    fn value(&self, row: usize) -> Value {
        if self.is_null(row) {
            return Value::NULL;
        }
        match self {
            Self::Bool(values) => Value::Int(i64::from(values.value(row))),
            Self::U8(values) => Value::Int(i64::from(values.value(row))),
            Self::I8(values) => Value::Int(i64::from(values.value(row))),
            Self::U16(values) => Value::Int(i64::from(values.value(row))),
            Self::I16(values) => Value::Int(i64::from(values.value(row))),
            Self::U32(values) => Value::Int(i64::from(values.value(row))),
            Self::I32(values) => Value::Int(i64::from(values.value(row))),
            Self::U64(values) => match i64::try_from(values.value(row)) {
                Ok(value) => Value::Int(value),
                Err(_) => Value::UInt(values.value(row)),
            },
            Self::I64(values) => Value::Int(values.value(row)),
            Self::F32(values) => Value::Double(f64::from(values.value(row))),
            Self::F64(values) => Value::Double(values.value(row)),
            Self::String(values) => Value::Bytes(values.value(row).as_bytes().to_vec()),
            Self::Bytes(values) => Value::Bytes(values.value(row).to_vec()),
            Self::Datetime(values) => Value::Bytes(
                DateTime::from_timestamp_nanos(values.value(row))
                    .fixed_offset()
                    .to_rfc3339()
                    .into_bytes(),
            ),
            // A list binds as the JSON text of its elements, which is what a JSON column reads and
            // a text column stores.
            Self::List { .. } | Self::FixedList { .. } => {
                Value::Bytes(self.json_value(row).to_string().into_bytes())
            }
        }
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

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use arrow_array::builder::{BinaryBuilder, ListBuilder};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use nervix_primitives::sync::StdArc;

    use super::*;

    #[test]
    fn invalid_insert_clause_keeps_its_typed_cause_at_the_host_boundary() {
        let failure = MySqlTableEmitter::conflict_clause(&[], MySqlConflictAction::DoUpdate)
            .expect_err("an update needs mapped values");
        let report = MySqlWriteError::into_report(failure);
        assert!(report.contains::<MySqlWriteError>());
    }

    #[test]
    fn a_driver_failure_names_its_cause_once() {
        let failure = mysql_async::Error::Driver(mysql_async::DriverError::PoolDisconnected);
        let cause = failure.to_string();
        let report = MySqlWriteError::report_execute(failure);
        let rendered = format!("{report:#}");

        assert!(
            rendered.starts_with(&format!("MySQL insert failed: {cause}")),
            "{rendered}"
        );
        assert_eq!(rendered.matches(&cause).count(), 1, "{rendered}");
    }

    #[test]
    fn server_rejection_keeps_safe_classification_without_quoting_a_row() {
        let report =
            MySqlWriteError::report_execute(mysql_async::Error::Server(mysql_async::ServerError {
                state: "22001".to_string(),
                code: 1406,
                message: "secret-row-value".to_string(),
            }));
        assert!(report.current_context().is_record_error());
        let report = MySqlWriteError::into_report(report);
        let diagnostic = format!("{report:?}");
        assert!(report.contains::<MySqlWriteError>());
        assert!(diagnostic.contains("22001"));
        assert!(!diagnostic.contains("secret-row-value"));
    }

    #[test]
    fn classifies_only_definitive_mysql_server_errors_as_record_errors() {
        for state in ["22001", "22003", "22007", "23000"] {
            assert!(
                is_record_server_error(state, 0),
                "{state} should be a definitive record error"
            );
        }
        assert!(is_record_server_error("08S01", 1153));
        assert!(
            is_record_server_error("HY000", 3819),
            "a violated CHECK constraint names the rows of one insert even under HY000"
        );
        assert!(
            is_record_server_error("HY000", 1366),
            "invalid string values are definitive record errors even when MySQL reports HY000"
        );
        for state in ["08S01", "40001", "42S02", "HY000"] {
            assert!(
                !is_record_server_error(state, 0),
                "{state} requires infrastructure retry"
            );
        }
    }

    #[test]
    fn binds_each_mapped_column_from_the_row_it_holds() {
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

        let columns = MappedMySqlColumns::new(&batch, &names).expect("columns should be mapped");

        let first = columns
            .values
            .iter()
            .map(|column| column.value(0))
            .collect::<Vec<_>>();
        assert_eq!(
            first,
            vec![
                Value::Int(7),
                Value::Double(1.5),
                Value::Bytes(b"first".to_vec()),
                Value::Bytes(b"2023-11-14T22:13:20.123456789+00:00".to_vec()),
            ]
        );
        let second = columns
            .values
            .iter()
            .map(|column| column.value(1))
            .collect::<Vec<_>>();
        assert_eq!(
            second,
            vec![
                Value::NULL,
                Value::Double(2.0),
                Value::Bytes(b"second".to_vec()),
                Value::NULL,
            ]
        );
    }

    #[test]
    fn declared_scalar_encodings_match_the_driver_parameters() {
        use nervix_models::{MySqlValueEncoding, ParseAsType};

        struct BindingCase {
            ty: ParseAsType,
            array: ArrayRef,
            expected: Value,
            encoding: MySqlValueEncoding,
        }
        macro_rules! binding {
            ($ty:ident, $array:ident, $value:expr, $expected:expr, $encoding:ident) => {
                BindingCase {
                    ty: ParseAsType::$ty,
                    array: StdArc::new($array::from(vec![$value])),
                    expected: $expected,
                    encoding: MySqlValueEncoding::$encoding,
                }
            };
        }
        let cases = [
            binding!(Bool, BooleanArray, true, Value::Int(1), BooleanInteger),
            binding!(U8, UInt8Array, u8::MAX, Value::Int(255), SignedInteger),
            binding!(I8, Int8Array, i8::MIN, Value::Int(-128), SignedInteger),
            binding!(
                U16,
                UInt16Array,
                u16::MAX,
                Value::Int(65_535),
                SignedInteger
            ),
            binding!(
                I16,
                Int16Array,
                i16::MIN,
                Value::Int(-32_768),
                SignedInteger
            ),
            binding!(
                U32,
                UInt32Array,
                u32::MAX,
                Value::Int(4_294_967_295),
                SignedInteger
            ),
            binding!(
                I32,
                Int32Array,
                i32::MIN,
                Value::Int(-2_147_483_648),
                SignedInteger
            ),
            binding!(
                I64,
                Int64Array,
                i64::MIN,
                Value::Int(i64::MIN),
                SignedInteger
            ),
            binding!(U64, UInt64Array, 7_u64, Value::Int(7), UnsignedInteger),
            binding!(
                U64,
                UInt64Array,
                u64::MAX,
                Value::UInt(u64::MAX),
                UnsignedInteger
            ),
            binding!(
                F32,
                Float32Array,
                f32::MAX,
                Value::Double(f64::from(f32::MAX)),
                Double
            ),
            binding!(F64, Float64Array, f64::NAN, Value::Double(f64::NAN), Double),
            binding!(
                String,
                StringArray,
                "é",
                Value::Bytes("é".as_bytes().to_vec()),
                Utf8Bytes
            ),
            binding!(
                Bytes,
                BinaryArray,
                b"\x00\xff".as_slice(),
                Value::Bytes(vec![0, 255]),
                Bytes
            ),
            binding!(
                Datetime,
                TimestampNanosecondArray,
                0_i64,
                Value::Bytes(b"1970-01-01T00:00:00+00:00".to_vec()),
                TimestampBytes
            ),
            binding!(I64, Int64Array, None::<i64>, Value::NULL, SignedInteger),
        ];
        for case in cases {
            assert_eq!(
                MySqlValueEncoding::for_type(&case.ty),
                case.encoding,
                "{}",
                case.ty
            );
            let column = MappedMySqlColumn::new(&case.array).expect("the declared scalar binds");
            match (column.value(0), case.expected) {
                (Value::Double(actual), Value::Double(expected)) => {
                    assert_eq!(actual.to_bits(), expected.to_bits(), "{}", case.ty);
                }
                (actual, expected) => assert_eq!(actual, expected, "{}", case.ty),
            }
        }
        for ty in [
            ParseAsType::Array {
                element: Box::new(ParseAsType::I64),
                len: std::num::NonZeroU32::new(2).expect("two is positive"),
            },
            ParseAsType::Vec {
                element: Box::new(ParseAsType::Bytes),
            },
        ] {
            assert_eq!(
                MySqlValueEncoding::for_type(&ty),
                MySqlValueEncoding::JsonText
            );
        }
    }
    /// Stands in for the pool where a test never opens a connection.
    struct NoConnections;

    #[async_trait]
    impl MySqlConnections for NoConnections {
        async fn connection(&self) -> EmitterWriteResult<MySqlConnection> {
            Err(
                Report::new(EmitterWriteError::NotInitialized { connector: MYSQL })
                    .attach_printable("pool-acquire-detail"),
            )
        }
    }

    fn test_emitter(conflict_action: MySqlConflictAction) -> MySqlTableEmitter {
        MySqlTableEmitter {
            connections: Box::new(NoConnections),
            table: MySqlTableName::parse("limit_a_t0192a1b2c3d4e5f60718293a4b5c6d7e")
                .expect("the test table name is valid"),
            conflict_action,
            limits: RowRequestLimits::from(EmitterBatchPolicy {
                max_messages: nervix_models::BatchMessageLimit::try_from(3_u32)
                    .expect("three is a valid message limit"),
                max_size: "1KiB".parse().expect("1KiB is a valid size"),
            }),
        }
    }

    #[test]
    fn a_qualified_table_quotes_each_case_preserving_part() {
        let mut emitter = test_emitter(MySqlConflictAction::None);
        emitter.table = MySqlTableName::parse("Shop.Orders").expect("two table path parts");
        let statement = emitter
            .insert_statement(&["key.with.dot".to_string(), "tick`column".to_string()])
            .expect("the quoted columns form an insert");
        assert_eq!(
            statement.sql(1),
            "INSERT INTO `Shop`.`Orders` (`key.with.dot`, `tick``column`) VALUES (?, ?)"
        );
    }

    #[test]
    fn bolero_mysql_insert_measure_matches_binary_parameters() {
        use mysql_common::proto::MySerialize as _;
        use nervix_arbitrary::{Arbitrary, Domain};

        bolero::check!()
            .with_iterations(256)
            .with_max_len(1024)
            .for_each(|bytes: &[u8]| {
                let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
                let mut emitter = test_emitter(arbitrary.entropy().pick([
                    MySqlConflictAction::None,
                    MySqlConflictAction::DoNothing,
                    MySqlConflictAction::DoUpdate,
                ]));
                emitter.table = arbitrary.mysql_table_name();
                let rows = usize::try_from(arbitrary.entropy().boundary_biased(1..=8))
                    .assured("the generated row count is between one and eight");
                let mut numbers = Vec::with_capacity(rows);
                let mut strings = Vec::with_capacity(rows);
                let mut octets = Vec::with_capacity(rows);
                for _ in 0..rows {
                    let length = usize::try_from(arbitrary.entropy().boundary_biased(0..=300))
                        .assured("a generated payload has at most 300 bytes");
                    let number = arbitrary.entropy().any_u64();
                    let text = arbitrary.entropy().pick(["a", "é", "'", "`", "\0"]);
                    let octet = arbitrary.entropy().byte();
                    if arbitrary.entropy().flag() {
                        numbers.push(None);
                        strings.push(None);
                        octets.push(None);
                    } else {
                        numbers.push(Some(number));
                        strings.push(Some(text.repeat(length)));
                        octets.push(Some(vec![octet; length]));
                    }
                }
                let names = [
                    "key.with.dot".to_string(),
                    "tick`column".to_string(),
                    "octets".to_string(),
                ];
                let schema = StdArc::new(Schema::new(vec![
                    Field::new("id", DataType::UInt64, true),
                    Field::new("text", DataType::Utf8, true),
                    Field::new("data", DataType::Binary, true),
                ]));
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        StdArc::new(UInt64Array::from(numbers)),
                        StdArc::new(StringArray::from(strings)),
                        StdArc::new(BinaryArray::from_iter(octets)),
                    ],
                )
                .assured("the generated columns have the same bounded row count");
                let columns = MappedMySqlColumns::new(&batch, &names)
                    .assured("the declared integer, string and byte columns are supported");
                let mut members = (0..rows)
                    .map(|row| MappedEmitterMember { carrier: 0, row })
                    .collect::<Vec<_>>();
                let rotation = arbitrary.entropy().count(
                    rows.checked_sub(1)
                        .assured("the generated row count is positive"),
                );
                members.rotate_left(rotation);
                let statement = emitter
                    .insert_statement(&names)
                    .assured("a declared table and three columns form an insert");
                let values = BoundValues::bind(&[columns], &members, names.len());
                for start in 0..rows {
                    let first_end = start
                        .checked_add(1)
                        .assured("at most eight rows are generated");
                    for end in first_end..=rows {
                        let begin = start
                            .checked_mul(names.len())
                            .assured("eight rows of three parameters fit usize");
                        let finish = end
                            .checked_mul(names.len())
                            .assured("eight rows of three parameters fit usize");
                        let mut encoded = Vec::new();
                        for value in &values.values[begin..finish] {
                            value.serialize(&mut encoded);
                        }
                        let sql = statement.sql(
                            end.checked_sub(start)
                                .assured("the selected row range is nonempty"),
                        );
                        let actual = sql
                            .len()
                            .checked_add(encoded.len())
                            .assured("a bounded insert's text and parameters fit usize");
                        assert_eq!(
                            statement.measure(&values, start..end),
                            u64::try_from(actual).assured("a bounded encoded insert fits u64"),
                            "rows {start}..{end}, SQL {sql}"
                        );
                    }
                }
            });
    }

    #[nervix_primitives::test]
    async fn connection_pool_failure_keeps_the_acquire_diagnostic_at_the_host_boundary() {
        let connector = test_emitter(MySqlConflictAction::None);
        let error = match connector.connection().await {
            Ok(_) => panic!("the test pool has no connection"),
            Err(error) => error,
        };
        assert!(matches!(
            error.current_context(),
            MySqlWriteError::Pool { .. }
        ));
        let report = MySqlWriteError::into_report(error);
        assert!(report.contains::<MySqlWriteError>());
        assert!(format!("{report:?}").contains("pool-acquire-detail"));
    }

    #[nervix_primitives::test]
    async fn an_unsupported_mapped_column_keeps_its_exact_cause_and_fails_the_publish() {
        let mut connector = test_emitter(MySqlConflictAction::None);
        let batch = RecordBatch::try_new(
            StdArc::new(Schema::new(vec![Field::new(
                "partition",
                DataType::Date32,
                false,
            )])),
            vec![StdArc::new(arrow_array::Date32Array::from(vec![1, 2]))],
        )
        .expect("the foreign Arrow batch matches its exact schema");
        let target_columns = ["partition".to_string()];
        let selected_rows = [0, 1];
        let parts = connector
            .publish(MappedEmitterRows {
                target_columns: &target_columns,
                carriers: vec![nervix_connector::MappedEmitterCarrier {
                    batch_index: 0,
                    batch: &batch,
                    selected_rows: &selected_rows,
                    occurred_at: nervix_models::Timestamp::from_unix_nanos(5),
                    acknowledgements: None,
                }],
            })
            .await
            .into_parts();
        assert!(parts.delivered.is_empty());
        assert!(parts.rejected.is_empty());
        let report = parts
            .infrastructure_error
            .expect("the publish fails as a whole");
        let cause = report
            .downcast_ref::<UnsupportedMappedColumn>()
            .expect("the exact column refusal remains typed");
        assert_eq!(cause.column, "partition");
        assert_eq!(cause.data_type, DataType::Date32);
        let rendered = format!("{report:#}");
        assert!(rendered.contains("partition"), "{rendered}");
        assert!(rendered.contains("Date32"), "{rendered}");
    }

    /// The measured size is the statement a candidate executes and every value exactly as the
    /// binary protocol encodes it, so a scenario's `MAX SIZE` can be written to the byte.
    #[test]
    fn measures_the_statement_and_the_values_the_protocol_encodes() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("seq", DataType::Int64, true),
            Field::new("note", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), None])),
                StdArc::new(StringArray::from(vec![
                    Some("abc"),
                    Some("abc"),
                    Some("abc"),
                    Some("a longer note"),
                ])),
            ],
        )
        .expect("the mapped batch should build");
        let names = ["seq".to_string(), "note".to_string()];
        let columns = MappedMySqlColumns::new(&batch, &names).expect("columns should be mapped");
        let members = [0, 1, 2, 3].map(|row| MappedEmitterMember { carrier: 0, row });
        for conflict_action in [
            MySqlConflictAction::None,
            MySqlConflictAction::DoNothing,
            MySqlConflictAction::DoUpdate,
        ] {
            let statement = test_emitter(conflict_action)
                .insert_statement(&names)
                .expect("the insert statement builds");
            let values = BoundValues::bind(std::slice::from_ref(&columns), &members, names.len());
            for range in [0..1, 0..3, 1..4, 3..4] {
                let mut encoded = 0;
                for member in &members[range.clone()] {
                    for value in columns.row_values(member.row) {
                        encoded += value.bin_len();
                    }
                }
                let sql = statement.sql(range.len());
                let expected =
                    u64::try_from(sql.len()).expect("a test statement fits u64") + encoded;
                assert_eq!(statement.measure(&values, range), expected, "{sql}");
            }
        }
        let statement = test_emitter(MySqlConflictAction::None)
            .insert_statement(&names)
            .expect("the insert statement builds");
        let values = BoundValues::bind(&[columns], &members, names.len());
        assert_eq!(statement.measure(&values, 0..3), 137);
        assert_eq!(statement.measure(&values, 0..2), 117);
    }

    #[test]
    fn a_statement_carries_the_rows_its_placeholders_allow() {
        assert_eq!(
            MultiRowInsert::rows_per_statement(5).map(NonZeroUsize::get),
            Some(13_107)
        );
        assert_eq!(
            MultiRowInsert::rows_per_statement(1).map(NonZeroUsize::get),
            Some(MYSQL_MAX_PLACEHOLDERS)
        );
        assert_eq!(
            MultiRowInsert::rows_per_statement(MYSQL_MAX_PLACEHOLDERS + 1),
            None
        );
        assert_eq!(MultiRowInsert::rows_per_statement(0), None);
    }

    #[test]
    fn an_insert_takes_the_values_of_its_rows_in_order() {
        let schema = StdArc::new(Schema::new(vec![Field::new("seq", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(Int64Array::from(vec![
                Some(1),
                Some(2),
                Some(3),
            ]))],
        )
        .expect("the mapped batch should build");
        let names = ["seq".to_string()];
        let columns = MappedMySqlColumns::new(&batch, &names).expect("columns should be mapped");
        let members = [2, 0, 1].map(|row| MappedEmitterMember { carrier: 0, row });
        let mut values = BoundValues::bind(&[columns], &members, names.len());

        assert_eq!(values.take(1..3), vec![Value::Int(1), Value::Int(2)]);
        assert_eq!(values.take(0..1), vec![Value::Int(3)]);
    }

    #[test]
    fn binds_bytes_as_they_are_and_arrays_as_json_text() {
        let pairs = FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", DataType::Int64, false)),
            2,
            StdArc::new(Int64Array::from(vec![1, 10])),
            None,
        )
        .expect("one row of two elements builds");
        let mut blobs = ListBuilder::new(BinaryBuilder::new());
        blobs.values().append_value(b"\x00\xff");
        blobs.append(true);
        let blobs = blobs.finish();
        let raw = BinaryArray::from(vec![b"\x00\xff".as_slice()]);
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

        let columns = MappedMySqlColumns::new(&batch, &names).expect("columns should be mapped");

        assert_eq!(
            columns.row_values(0),
            vec![
                Value::Bytes(b"[1,10]".to_vec()),
                Value::Bytes(br#"["AP8="]"#.to_vec()),
                Value::Bytes(b"\x00\xff".to_vec()),
            ]
        );
    }

    /// The driver's pool as its minimum's keeper reads and draws on it: a pool that has opened
    /// nothing counts no connection, and a checkout the server refuses names itself above the
    /// driver's error.
    #[nervix_primitives::test]
    async fn a_refused_driver_checkout_is_reported_as_the_keepers_checkout() {
        let opts = Opts::from_url("mysql://nervix@127.0.0.1:1/nervix")
            .expect("the address is a valid MySQL URL");
        let pool = DriverPool::new(opts);
        assert_eq!(
            pool.established(),
            0,
            "a pool that has opened nothing has established nothing"
        );

        let Err(report) = pool.check_out().await else {
            panic!("a checkout from a port that refuses connections must fail");
        };

        assert!(
            matches!(report.current_context(), PoolMinimumError::CheckOut),
            "the checkout names its own failure: {report:?}"
        );
        assert!(
            report.contains::<mysql_async::Error>(),
            "the driver's error stays beneath the checkout: {report:?}"
        );
    }
}
