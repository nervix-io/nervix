//! MySQL sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The node's shared MySQL pool and the task that keeps it at its declared minimum,
//!   the insert statement each mapped batch becomes, its bind parameters, the `ON DUPLICATE KEY`
//!   clause of a conflict action, and server-error classification.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio
//!   and the `mysql_async` driver.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use std::{path::PathBuf, sync::atomic::Ordering, time::Duration};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, ListArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use async_trait::async_trait;
use chrono::DateTime;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use mysql_async::{
    Conn, Opts, OptsBuilder, Params, Pool as DriverPool, PoolConstraints, PoolOpts, SslOpts, Value,
    prelude::Queryable as _,
};
use nervix_connector::{
    MappedSinkRows, PerRecordOutcome, RejectedSinkRecord, RowSink, SinkHost, SinkLifecycle,
    SinkPublishError, SinkPublishResult, SinkRecordPosition, SinkStartError, SinkStartResult,
    optional_client_config_value,
};
use nervix_models::{ClientConfigEntry, ClientPoolBounds, TableName};
use tracing::trace;

const MYSQL: &str = "mysql";

/// How often an idle MySQL pool is topped back up to its declared minimum.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);

/// The node's shared MySQL pool, with the task that keeps it at its declared minimum.
pub struct MySqlPool {
    pool: DriverPool,
    /// Aborted when the shared client closes, which is what stops maintenance with the pool it
    /// maintains rather than leaving it reconnecting to a database nothing is writing to.
    _maintenance: PoolMaintenance,
}

/// The maintenance task of one pool, stopped with the pool it maintains.
struct PoolMaintenance(tokio::task::JoinHandle<()>);

impl Drop for PoolMaintenance {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One connection borrowed from the shared pool, returned to it when this is dropped.
pub struct MySqlConnection(Conn);

/// The node-owned pool this sink borrows connections from while its host holds the lease.
#[async_trait]
pub trait MySqlConnections: Send + Sync + 'static {
    /// Borrow one connection for one insert, with the host's pool wait recorded around it.
    async fn connection(&self) -> SinkPublishResult<MySqlConnection>;
}

/// What one MySQL emitter inserts with, from its typed sink plan.
pub struct MySqlSinkConfig {
    pub table: TableName,
    pub conflict_action: MySqlConflictAction,
}

/// What an insert does with a row the target table already holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySqlConflictAction {
    None,
    DoNothing,
    DoUpdate,
}

/// The MySQL sink, which binds each mapped row as the parameters of one multi-row insert.
pub struct MySqlSink {
    connections: Box<dyn MySqlConnections>,
    table: TableName,
    conflict_action: MySqlConflictAction,
}

#[derive(Debug, thiserror::Error)]
enum MySqlWriteError {
    #[error("invalid MySQL VALUES: {0}")]
    InvalidValues(String),
    #[error("{0}")]
    Pool(String),
    #[error("MySQL insert failed: {0}")]
    Execute(mysql_async::Error),
}

impl MySqlWriteError {
    fn is_record_error(&self) -> bool {
        let Self::Execute(mysql_async::Error::Server(error)) = self else {
            return false;
        };
        is_record_server_error(&error.state, error.code)
    }

    fn record_reason(&self) -> String {
        let server_error = match self {
            Self::Execute(mysql_async::Error::Server(error)) => Some(error),
            _ => None,
        };
        match server_error {
            Some(error) => format!(
                "MySQL rejected record with SQLSTATE {} and code {}",
                error.state, error.code
            ),
            None => "MySQL rejected record".to_string(),
        }
    }

    fn into_report(self) -> Report<SinkPublishError> {
        let publish = || Report::new(SinkPublishError::Publish { sink: MYSQL });
        match self {
            Self::InvalidValues(reason) => publish().attach_printable(reason),
            Self::Execute(mysql_async::Error::Server(error)) => {
                publish().attach_printable(format!(
                    "MySQL request failed with SQLSTATE {} and code {}",
                    error.state, error.code
                ))
            }
            error => publish().attach_printable(error.to_string()),
        }
    }
}

/// A SQLSTATE and error code the server reports for a row it will never accept.
fn is_record_server_error(state: &str, code: u16) -> bool {
    state.starts_with("22") || state.starts_with("23") || matches!(code, 1153 | 1366)
}

/// Keep `pool` at `minimum` established connections without waiting for traffic.
///
/// `mysql_async` treats its minimum as a retention floor: it keeps that many idle connections once
/// they have been returned, but never opens one. A client whose emitters are idle would therefore
/// sit below its declared minimum indefinitely, so the shortfall is opened here and released
/// straight back. Only the shortfall is opened, so this neither exceeds the maximum nor takes
/// capacity a writer already holds.
async fn maintain_minimum(pool: DriverPool, minimum: usize) {
    loop {
        tokio::task::consume_budget().await;
        tokio::time::sleep(MAINTENANCE_INTERVAL).await;
        let established = pool.metrics().connection_count.load(Ordering::Relaxed);
        let Some(shortfall) = minimum.checked_sub(established) else {
            continue;
        };
        // Held together and released together: taking them one at a time would let the pool hand
        // the same connection back for the next iteration and never reach the minimum.
        let mut opened = Vec::with_capacity(shortfall);
        for _ in 0..shortfall {
            match pool.get_conn().await {
                Ok(conn) => opened.push(conn),
                // A database that cannot supply the minimum is a client infrastructure condition
                // reported by whoever tries to write; maintenance simply retries on the next tick.
                Err(_) => break,
            }
        }
        drop(opened);
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
    ) -> SinkStartResult<Self> {
        let invalid = || Report::new(SinkStartError::InvalidConfiguration { sink: MYSQL });
        let connect = || Report::new(SinkStartError::Initialize { sink: MYSQL });
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
        let maintenance = PoolMaintenance(tokio::spawn(maintain_minimum(pool.clone(), minimum)));
        Ok(Self {
            pool,
            _maintenance: maintenance,
        })
    }

    /// Borrow one connection, which returns to the pool when the borrow is dropped.
    pub async fn connection(&self) -> SinkPublishResult<MySqlConnection> {
        self.pool
            .get_conn()
            .await
            .map(MySqlConnection)
            .map_err(|source| {
                Report::new(SinkPublishError::NotInitialized { sink: MYSQL })
                    .attach_printable(source.to_string())
            })
    }
}

impl MySqlSink {
    pub fn new(
        config: MySqlSinkConfig,
        connections: Box<dyn MySqlConnections>,
        _host: SinkHost,
    ) -> SinkStartResult<Self> {
        let MySqlSinkConfig {
            table,
            conflict_action,
        } = config;
        Ok(Self {
            connections,
            table,
            conflict_action,
        })
    }

    fn quote_ident(identifier: &str) -> String {
        format!("`{}`", identifier.replace('`', "``"))
    }

    /// One bounded insert, on a connection borrowed for that insert and returned after it.
    async fn publish_rows(
        &self,
        columns: &MappedMySqlColumns<'_>,
        rows: &[usize],
    ) -> Result<u64, MySqlWriteError> {
        if rows.is_empty() {
            return Ok(0);
        }
        let quoted_columns = columns
            .names
            .iter()
            .map(|column| Self::quote_ident(column))
            .collect::<Vec<_>>();
        let columns_sql = quoted_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        let row_placeholders = format!(
            "({})",
            std::iter::repeat_n("?", quoted_columns.len())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let value_placeholders = std::iter::repeat_n(row_placeholders, rows.len())
            .collect::<Vec<_>>()
            .join(", ");
        let conflict_clause = Self::conflict_clause(&quoted_columns, self.conflict_action)?;
        let sql = format!(
            "INSERT INTO {} ({columns_sql}) VALUES {value_placeholders}{conflict_clause}",
            Self::quote_ident(self.table.as_str())
        );
        let mut params = Vec::with_capacity(
            rows.len()
                .checked_mul(quoted_columns.len())
                .assured("a batch this node holds in memory has bindable parameters"),
        );
        for row in rows {
            params.extend(columns.values.iter().map(|column| column.value(*row)));
        }
        let mut conn = self
            .connections
            .connection()
            .await
            .map_err(|error| MySqlWriteError::Pool(format!("{error:?}")))?;
        conn.0
            .exec_drop(sql, Params::Positional(params))
            .await
            .map_err(MySqlWriteError::Execute)?;
        Ok(conn.0.affected_rows())
    }

    fn conflict_clause(
        quoted_columns: &[String],
        conflict_action: MySqlConflictAction,
    ) -> Result<String, MySqlWriteError> {
        match conflict_action {
            MySqlConflictAction::None => Ok(String::new()),
            MySqlConflictAction::DoNothing => {
                let Some(column) = quoted_columns.first() else {
                    return Err(MySqlWriteError::InvalidValues(
                        "MySQL ON CONFLICT DO NOTHING requires at least one VALUES column"
                            .to_string(),
                    ));
                };
                Ok(format!(" ON DUPLICATE KEY UPDATE {column} = {column}"))
            }
            MySqlConflictAction::DoUpdate => {
                if quoted_columns.is_empty() {
                    return Err(MySqlWriteError::InvalidValues(
                        "MySQL ON CONFLICT DO UPDATE requires at least one VALUES column"
                            .to_string(),
                    ));
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

#[async_trait]
impl SinkLifecycle for MySqlSink {}

#[async_trait]
impl RowSink for MySqlSink {
    async fn publish(&mut self, rows: MappedSinkRows<'_>) -> PerRecordOutcome {
        let mut outcome = PerRecordOutcome::with_capacity(rows.selected_rows.len());
        let columns = match MappedMySqlColumns::new(rows.batch, rows.target_columns) {
            Ok(columns) => columns,
            Err(error) => {
                outcome.fail(
                    Report::new(SinkPublishError::Publish { sink: MYSQL })
                        .attach_printable(error.to_string()),
                );
                return outcome;
            }
        };
        for chunk in rows.selected_row_chunks {
            tokio::task::consume_budget().await;
            let Some(chunk_rows) = rows.selected_rows.get(chunk.clone()) else {
                outcome.fail(
                    Report::new(SinkPublishError::Publish { sink: MYSQL }).attach_printable(
                        format!(
                            "MySQL chunk {chunk:?} is outside its {} selected rows",
                            rows.selected_rows.len()
                        ),
                    ),
                );
                return outcome;
            };
            match self.publish_rows(&columns, chunk_rows).await {
                Ok(_) => {
                    for row in chunk_rows {
                        outcome.deliver(SinkRecordPosition {
                            batch_index: rows.batch_index,
                            row_index: *row,
                        });
                    }
                }
                Err(error) if error.is_record_error() && chunk_rows.len() > 1 => {
                    for row in chunk_rows {
                        tokio::task::consume_budget().await;
                        let position = SinkRecordPosition {
                            batch_index: rows.batch_index,
                            row_index: *row,
                        };
                        match self.publish_rows(&columns, &[*row]).await {
                            Ok(_) => outcome.deliver(position),
                            Err(error) if error.is_record_error() => {
                                outcome.reject(RejectedSinkRecord::external(
                                    position,
                                    rows.occurred_at,
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
                    if let Some(row) = chunk_rows.first() {
                        outcome.reject(RejectedSinkRecord::external(
                            SinkRecordPosition {
                                batch_index: rows.batch_index,
                                row_index: *row,
                            },
                            rows.occurred_at,
                            error.record_reason(),
                        ));
                    }
                }
                Err(error) => {
                    outcome.fail(error.into_report());
                    return outcome;
                }
            }
        }
        trace!(
            table = self.table.as_str(),
            rows = rows.selected_rows.len(),
            "emitter published mysql rows"
        );
        outcome
    }
}

/// A mapped column this sink cannot bind, named with the exact type it carries.
#[derive(Debug, thiserror::Error)]
#[error("MySQL VALUES column '{column}' has unsupported exact type {data_type}")]
struct UnsupportedMappedColumn {
    column: String,
    data_type: arrow_schema::DataType,
}

/// The mapped columns of one batch, downcast once so every row binds from the column that holds it.
struct MappedMySqlColumns<'a> {
    names: &'a [String],
    values: Vec<MappedMySqlColumn<'a>>,
}

impl<'a> MappedMySqlColumns<'a> {
    fn new(
        batch: &'a RecordBatch,
        target_columns: &'a [String],
    ) -> Result<Self, UnsupportedMappedColumn> {
        let mut values = Vec::with_capacity(target_columns.len());
        for (index, column) in target_columns.iter().enumerate() {
            let array = batch.column(index);
            let mapped = MappedMySqlColumn::new(array).ok_or_else(|| UnsupportedMappedColumn {
                column: column.clone(),
                data_type: array.data_type().clone(),
            })?;
            values.push(mapped);
        }
        Ok(Self {
            names: target_columns,
            values,
        })
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
    Datetime(&'a TimestampNanosecondArray),
    List {
        offsets: &'a ListArray,
        elements: Box<MappedMySqlColumn<'a>>,
    },
}

impl<'a> MappedMySqlColumn<'a> {
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
            Self::Datetime(values) => Value::Bytes(
                DateTime::from_timestamp_nanos(values.value(row))
                    .fixed_offset()
                    .to_rfc3339()
                    .into_bytes(),
            ),
            // A list binds as the JSON text of its elements, which is what a JSON column reads and
            // a text column stores.
            Self::List { .. } => Value::Bytes(self.json_value(row).to_string().into_bytes()),
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
                let mut items = Vec::with_capacity(end.checked_sub(start).assured(
                    "Arrow list offsets increase, so a row ends no earlier than it starts",
                ));
                for element in start..end {
                    items.push(elements.json_value(element));
                }
                serde_json::Value::Array(items)
            }
        }
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

    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::*;

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
}
