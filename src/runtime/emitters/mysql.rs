pub(in crate::runtime) use mysql_async::Pool as MySqlPool;
use mysql_async::{
    Conn as MySqlPooledConn, Opts as MySqlOpts, OptsBuilder as MySqlOptsBuilder,
    Params as MySqlParams, PoolConstraints as MySqlPoolConstraints, PoolOpts as MySqlPoolOpts,
    SslOpts as MySqlSslOpts, Value as MySqlValue, prelude::Queryable as MySqlQueryable,
};
use nervix_models::TableName;

use super::*;

pub(in crate::runtime) struct MySqlEmitter {
    client: Option<MySqlEmitterClient>,
    program: Option<CompiledSqlValuesProgram>,
}

/// This emitter's interest in the node's shared MySQL client.
///
/// The pool is the client's, not the emitter's: holding the lease keeps it open for as long as
/// this emitter can write, and every other local emitter on the same client borrows from it too.
struct MySqlEmitterClient {
    lease: SharedClientLease,
    /// The client borrowed from, named in this emitter's diagnostics and in its pool wait.
    client: ClientName,
    runtime: Runtime,
    /// This emitter, as the key its pool wait is recorded under for `DESCRIBE` to read.
    waiter: DomainNodeRef,
}

impl MySqlEmitterClient {
    /// Borrow a connection for one insert, reporting the wait until the pool hands one over.
    ///
    /// The borrow lasts for the insert and no longer: the connection returns to the shared pool
    /// when the returned guard is dropped, so a flush between inserts holds none.
    async fn connection(&self) -> Result<MySqlPooledConn, Report<SharedClientError>> {
        let pool = self.lease.client().mysql(&self.client)?;
        let waiting = self.runtime.pool_wait_guard(&self.waiter, &self.client);
        let conn = pool.get_conn().await.map_err(|source| {
            Report::new(SharedClientError::Open {
                client: self.client.as_str().to_string(),
            })
            .attach_printable(source.to_string())
        });
        drop(waiting);
        conn
    }
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
        MySqlEmitter::is_record_server_error(&error.state, error.code)
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

    fn into_report(self) -> Report<EmitterRuntimeError> {
        match self {
            Self::InvalidValues(reason) => {
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(reason)
            }
            Self::Execute(mysql_async::Error::Server(error)) => {
                Report::new(EmitterRuntimeError::PublishBatch).attach_printable(format!(
                    "MySQL request failed with SQLSTATE {} and code {}",
                    error.state, error.code
                ))
            }
            error => {
                Report::new(EmitterRuntimeError::PublishBatch).attach_printable(error.to_string())
            }
        }
    }
}

/// How often an idle MySQL pool is topped back up to its declared minimum.
const MYSQL_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);

/// The node's shared MySQL pool, with the task that keeps it at its declared minimum.
pub(in crate::runtime) struct MySqlSharedPool {
    pool: MySqlPool,
    /// Aborted when the shared client closes, which is what stops maintenance with the pool it
    /// maintains rather than leaving it reconnecting to a database nothing is writing to.
    _maintenance: AbortOnDropHandle<()>,
}

impl MySqlSharedPool {
    pub(in crate::runtime) fn pool(&self) -> &MySqlPool {
        &self.pool
    }
}

/// Keep `pool` at `minimum` established connections without waiting for traffic.
///
/// `mysql_async` treats its minimum as a retention floor: it keeps that many idle connections once
/// they have been returned, but never opens one. A client whose emitters are idle would therefore
/// sit below its declared minimum indefinitely, so the shortfall is opened here and released
/// straight back. Only the shortfall is opened, so this neither exceeds the maximum nor takes
/// capacity a writer already holds.
async fn maintain_mysql_minimum(pool: MySqlPool, minimum: usize) {
    loop {
        tokio::task::consume_budget().await;
        tokio::time::sleep(MYSQL_MAINTENANCE_INTERVAL).await;
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

/// Open the node's shared MySQL pool for one named client, sized by its declared bounds.
///
/// The bounds are the driver's own constraints, so the ceiling is enforced by the pool that hands
/// out connections rather than by any emitter counting its own. Initialization validates one
/// authenticated connection before the first user becomes operational, and returns it immediately.
pub(in crate::runtime) async fn open_mysql_pool(
    config: &[nervix_models::ClientConfigEntry],
    bounds: ClientPoolBounds,
) -> Result<MySqlSharedPool, Report<OpenClientError>> {
    let Some(addr) = optional_client_config_value(config, "addr") else {
        return Err(Report::new(OpenClientError::MissingConfig {
            transport: "MySQL",
            key: "addr",
        }));
    };
    let opts = MySqlOpts::from_url(addr).map_err(|source| {
        Report::new(OpenClientError::InvalidConfig {
            transport: "MySQL",
            reason: format!("failed to parse client addr: {source}"),
        })
    })?;
    let builder = if let Some(ca_file) = optional_client_config_value(config, "tls_ca_file") {
        let ssl_opts = MySqlSslOpts::default()
            .with_root_certs(vec![PathBuf::from(ca_file).into()])
            .with_disable_built_in_roots(true);
        MySqlOptsBuilder::from_opts(opts).ssl_opts(Some(ssl_opts))
    } else {
        MySqlOptsBuilder::from_opts(opts)
    };
    let invalid = |reason: String| {
        Report::new(OpenClientError::InvalidConfig {
            transport: "MySQL",
            reason,
        })
    };
    let minimum = usize::try_from(bounds.minimum())
        .map_err(|_| invalid("POOL SIZE MIN exceeds this platform's pointer width".to_string()))?;
    let maximum = usize::try_from(bounds.maximum().get())
        .map_err(|_| invalid("POOL SIZE MAX exceeds this platform's pointer width".to_string()))?;
    let constraints = MySqlPoolConstraints::new(minimum, maximum).ok_or_else(|| {
        invalid(format!(
            "driver rejected pool bounds MIN {minimum} MAX {maximum}"
        ))
    })?;
    let pool =
        MySqlPool::new(builder.pool_opts(MySqlPoolOpts::default().with_constraints(constraints)));
    let mut conn = pool.get_conn().await.map_err(|source| {
        Report::new(OpenClientError::Connect {
            transport: "MySQL",
            reason: source.to_string(),
        })
    })?;
    conn.query_drop("SELECT 1").await.map_err(|source| {
        Report::new(OpenClientError::Connect {
            transport: "MySQL",
            reason: format!("failed to validate connection: {source}"),
        })
    })?;
    drop(conn);
    let maintenance = AbortOnDropHandle::new(tokio::spawn(maintain_mysql_minimum(
        pool.clone(),
        minimum,
    )));
    Ok(MySqlSharedPool {
        pool,
        _maintenance: maintenance,
    })
}

impl MySqlEmitter {
    fn is_record_server_error(state: &str, code: u16) -> bool {
        state.starts_with("22") || state.starts_with("23") || matches!(code, 1153 | 1366)
    }

    pub(in crate::runtime) async fn new(
        model: &Model,
        client: &nervix_models::CreateClientMySql,
        resolved: Option<&ResolvedClientConfig>,
        context: &EmitterSinkContext,
        values: &[MySqlValueMapping],
        input_schema: StdArc<arrow_schema::Schema>,
    ) -> Self {
        let client = match context
            .runtime
            .lease_shared_client(&context.domain, &client.name, model, resolved)
            .await
        {
            Ok(lease) => Some(MySqlEmitterClient {
                lease,
                client: client.name.clone(),
                runtime: context.runtime.clone(),
                waiter: DomainNodeRef::node_in(
                    context.domain.clone(),
                    ModelKind::Emitter,
                    context.emitter.clone(),
                ),
            }),
            Err(error) => {
                context.report_init_error("mysql", &error.to_string());
                None
            }
        };
        let program = match compile_mysql_values_program(
            &context.domain,
            &context.emitter,
            values,
            input_schema,
            context.udfs.as_ref(),
        ) {
            Ok(program) => Some(program),
            Err(error) => {
                context.runtime.events().report_error(error.to_string());
                warn!(
                    domain = context.domain.as_str(),
                    emitter = context.emitter.as_str(),
                    error = %error,
                    "failed to compile mysql emitter values"
                );
                None
            }
        };
        Self { client, program }
    }

    fn value(value: &serde_json::Value) -> MySqlValue {
        match value {
            serde_json::Value::Null => MySqlValue::NULL,
            serde_json::Value::String(value) => MySqlValue::Bytes(value.as_bytes().to_vec()),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    MySqlValue::Int(value)
                } else if let Some(value) = value.as_u64() {
                    MySqlValue::UInt(value)
                } else if let Some(value) = value.as_f64() {
                    MySqlValue::Double(value)
                } else {
                    MySqlValue::Bytes(value.to_string().into_bytes())
                }
            }
            serde_json::Value::Bool(value) => MySqlValue::Int(i64::from(*value)),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                MySqlValue::Bytes(value.to_string().into_bytes())
            }
        }
    }

    fn quote_ident(identifier: &str) -> String {
        format!("`{}`", identifier.replace('`', "``"))
    }

    async fn publish_rows(
        client: &MySqlEmitterClient,
        table: &TableName,
        mappings: &[MySqlValueMapping],
        conflict_action: &MySqlConflictAction,
        rows: &[&[serde_json::Value]],
    ) -> Result<u64, MySqlWriteError> {
        if rows.is_empty() {
            return Ok(0);
        }
        let columns = mappings
            .iter()
            .map(|mapping| mapping.column.as_str())
            .collect::<Vec<_>>();
        let quoted_columns = columns
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
            std::iter::repeat_n("?", mappings.len())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let value_placeholders = std::iter::repeat_n(row_placeholders, rows.len())
            .collect::<Vec<_>>()
            .join(", ");
        let conflict_clause = Self::conflict_clause(&quoted_columns, conflict_action)?;
        let sql = format!(
            "INSERT INTO {} ({columns_sql}) VALUES {value_placeholders}{conflict_clause}",
            Self::quote_ident(table.as_str())
        );
        let mut params = Vec::with_capacity(rows.len() * mappings.len());
        for row in rows {
            if row.len() != mappings.len() {
                return Err(MySqlWriteError::InvalidValues(format!(
                    "MySQL VALUES produced {} columns for {} mappings",
                    row.len(),
                    mappings.len()
                )));
            }
            params.extend(row.iter().map(Self::value));
        }
        let mut conn = client
            .connection()
            .await
            .map_err(|error| MySqlWriteError::Pool(error.to_string()))?;
        conn.exec_drop(sql, MySqlParams::Positional(params))
            .await
            .map_err(MySqlWriteError::Execute)?;
        Ok(conn.affected_rows())
    }

    fn conflict_clause(
        quoted_columns: &[String],
        conflict_action: &MySqlConflictAction,
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

    pub(super) async fn publish_pending_chunks(
        &self,
        batch_index: usize,
        table: &TableName,
        values: &[MySqlValueMapping],
        conflict_action: &MySqlConflictAction,
        batch: &RelayRecordBatch,
        pending_chunks: &[Vec<usize>],
    ) -> PerRecordPublishOutcome {
        let mut outcome = PerRecordPublishOutcome::empty();
        if pending_chunks.is_empty() {
            return outcome;
        }
        let (Some(client), Some(program)) = (self.client.as_ref(), self.program.as_ref()) else {
            outcome.fail(
                Report::new(EmitterRuntimeError::SinkNotInitialized)
                    .attach_printable("no initialized mysql sink client"),
            );
            return outcome;
        };
        let rows = match sql_mapped_batch_values(program, values, batch, current_timestamp()).await
        {
            Ok(rows) => rows,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        let pending_chunks =
            match outcome.filter_mapped_chunks(batch_index, &rows, pending_chunks, "mysql") {
                Ok(pending_chunks) => pending_chunks,
                Err(error) => {
                    outcome.fail(error);
                    return outcome;
                }
            };
        if pending_chunks.is_empty() {
            return outcome;
        }
        let request_acks = batch.merged_acks();
        for chunk in &pending_chunks {
            tokio::task::consume_budget().await;
            let chunk_rows = match Self::rows_at_indices(&rows, chunk) {
                Ok(rows) => rows,
                Err(error) => {
                    outcome.fail(error.into_report());
                    return outcome;
                }
            };
            match await_emitter_confirmation(
                &request_acks,
                Self::publish_rows(client, table, values, conflict_action, &chunk_rows),
            )
            .await
            {
                Ok(_) => {
                    for row in chunk {
                        outcome.deliver(BrokerRecordPosition {
                            batch_index,
                            row_index: *row,
                        });
                    }
                }
                Err(error) if error.is_record_error() && chunk.len() > 1 => {
                    for row in chunk {
                        tokio::task::consume_budget().await;
                        let single_row = match rows.get(*row) {
                            Some(Ok(row_values)) => [row_values.as_slice()],
                            _ => {
                                outcome.fail(
                                    MySqlWriteError::InvalidValues(format!(
                                        "pending row {row} has no mapped VALUES in batch with {} \
                                         rows",
                                        rows.len()
                                    ))
                                    .into_report(),
                                );
                                return outcome;
                            }
                        };
                        match await_emitter_confirmation(
                            &request_acks,
                            Self::publish_rows(client, table, values, conflict_action, &single_row),
                        )
                        .await
                        {
                            Ok(_) => outcome.deliver(BrokerRecordPosition {
                                batch_index,
                                row_index: *row,
                            }),
                            Err(error) if error.is_record_error() => outcome.reject(
                                BrokerRecordPosition {
                                    batch_index,
                                    row_index: *row,
                                },
                                error.record_reason(),
                            ),
                            Err(error) => {
                                outcome.fail(error.into_report());
                                return outcome;
                            }
                        }
                    }
                }
                Err(error) if error.is_record_error() => {
                    if let Some(row) = chunk.first() {
                        outcome.reject(
                            BrokerRecordPosition {
                                batch_index,
                                row_index: *row,
                            },
                            error.record_reason(),
                        );
                    }
                }
                Err(error) => {
                    outcome.fail(error.into_report());
                    return outcome;
                }
            }
        }
        trace!(
            table = table.as_str(),
            rows = outcome.delivered.len(),
            rejected = outcome.rejected.len(),
            "emitter published mysql rows"
        );
        outcome
    }

    fn rows_at_indices<'a>(
        rows: &'a [Result<Vec<serde_json::Value>, StructuredMessageError>],
        indices: &[usize],
    ) -> Result<Vec<&'a [serde_json::Value]>, MySqlWriteError> {
        indices
            .iter()
            .map(|row| {
                let Some(values) = rows.get(*row) else {
                    return Err(MySqlWriteError::InvalidValues(format!(
                        "pending row {row} has no mapped VALUES in batch with {} rows",
                        rows.len()
                    )));
                };
                let Ok(values) = values else {
                    return Err(MySqlWriteError::InvalidValues(format!(
                        "pending row {row} has no mapped VALUES in batch with {} rows",
                        rows.len()
                    )));
                };
                Ok(values.as_slice())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_only_definitive_mysql_server_errors_as_record_errors() {
        for state in ["22001", "22003", "22007", "23000"] {
            assert!(
                MySqlEmitter::is_record_server_error(state, 0),
                "{state} should be a definitive record error"
            );
        }
        assert!(MySqlEmitter::is_record_server_error("08S01", 1153));
        assert!(
            MySqlEmitter::is_record_server_error("HY000", 1366),
            "invalid string values are definitive record errors even when MySQL reports HY000"
        );
        for state in ["08S01", "40001", "42S02", "HY000"] {
            assert!(
                !MySqlEmitter::is_record_server_error(state, 0),
                "{state} requires infrastructure retry"
            );
        }
    }
}
