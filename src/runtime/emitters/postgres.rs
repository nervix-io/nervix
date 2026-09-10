use nervix_models::TableName;
pub(in crate::runtime) use sqlx::postgres::PgPool;
use sqlx::{
    AssertSqlSafe, Executor as _, Row as _,
    pool::PoolConnection as SqlxPoolConnection,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode, Postgres as SqlxPostgres},
};
use url::Url;

/// One connection borrowed from the shared Postgres pool, returned when it is dropped.
type PgPoolConnection = SqlxPoolConnection<SqlxPostgres>;

use super::*;

/// How long a borrower waits for a connection, including pool wait, establishment,
/// authentication and validation. Independent of how long an accepted query then runs.
const POSTGRES_ACQUIRE_DEADLINE: Duration = Duration::from_secs(30);

/// How long a connection above the maintained minimum may sit idle before it is retired.
const POSTGRES_IDLE_LIFETIME: Duration = Duration::from_secs(10 * 60);

/// How old any connection may become before it is retired and replaced within the same maximum.
const POSTGRES_MAX_LIFETIME: Duration = Duration::from_secs(30 * 60);

pub(in crate::runtime) struct PostgresEmitter {
    client: Option<PostgresEmitterClient>,
    program: Option<CompiledSqlValuesProgram>,
}

/// This emitter's interest in the node's shared Postgres pool.
///
/// The pool belongs to the named client, so every local emitter of that client borrows from it and
/// the declared maximum bounds the node's connections rather than this emitter's.
struct PostgresEmitterClient {
    lease: SharedClientLease,
    /// The client borrowed from, named in this emitter's diagnostics and in its pool wait.
    client: ClientName,
    runtime: Runtime,
    /// This emitter, as the key its pool wait is recorded under for `DESCRIBE` to read.
    waiter: DomainNodeRef,
}

impl PostgresEmitterClient {
    /// Borrow a connection for one operation, reporting the wait until the pool hands one over.
    ///
    /// The borrow covers a bounded insert or its metadata work and no more: between inserts, and
    /// across a flush interval or a retry backoff, this emitter holds no connection.
    async fn connection(&self) -> Result<PgPoolConnection, Report<SharedClientError>> {
        let pool = self.lease.client().postgres(&self.client)?;
        let waiting = self.runtime.pool_wait_guard(&self.waiter, &self.client);
        let connection = pool.acquire().await.map_err(|source| {
            Report::new(SharedClientError::Open {
                client: self.client.as_str().to_string(),
            })
            .attach_printable(source.to_string())
        });
        drop(waiting);
        connection
    }
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
fn postgres_sqlstate(error: &sqlx::Error) -> Option<String> {
    let sqlx::Error::Database(error) = error else {
        return None;
    };
    error.code().map(|code| code.into_owned())
}

impl PostgresWriteError {
    fn is_record_error(&self) -> bool {
        let Self::Execute(error) = self else {
            return false;
        };
        match postgres_sqlstate(error) {
            Some(code) => PostgresEmitter::is_record_sqlstate(&code),
            None => false,
        }
    }

    fn record_reason(&self) -> String {
        let code = match self {
            Self::Execute(error) => postgres_sqlstate(error),
            _ => None,
        };
        match code {
            Some(code) => format!("Postgres rejected record with SQLSTATE {code}"),
            None => "Postgres rejected record".to_string(),
        }
    }

    fn into_report(self) -> Report<EmitterRuntimeError> {
        match self {
            Self::InvalidValues(reason) => {
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(reason)
            }
            Self::Execute(error) => {
                let code = match postgres_sqlstate(&error) {
                    Some(code) => code,
                    None => "unknown".to_string(),
                };
                Report::new(EmitterRuntimeError::PublishBatch)
                    .attach_printable(format!("Postgres request failed with SQLSTATE {code}"))
            }
            error => {
                Report::new(EmitterRuntimeError::PublishBatch).attach_printable(error.to_string())
            }
        }
    }
}

/// Open the node's shared Postgres pool for one named client, sized by its declared bounds.
///
/// SQLx's own pool enforces the ceiling, validates a connection before handing it out, and retires
/// idle and aged connections within the same maximum. The acquisition deadline covers pool wait,
/// establishment, authentication and validation, and is independent of how long an accepted query
/// then runs.
pub(in crate::runtime) async fn open_postgres_pool(
    config: &[nervix_models::ClientConfigEntry],
    bounds: ClientPoolBounds,
) -> Result<PgPool, Report<OpenClientError>> {
    let Some(addr) = optional_client_config_value(config, "addr") else {
        return Err(Report::new(OpenClientError::MissingConfig {
            transport: "Postgres",
            key: "addr",
        }));
    };
    let options = postgres_connect_options(addr, config)?;
    PgPoolOptions::new()
        .min_connections(bounds.minimum())
        .max_connections(bounds.maximum().get())
        .acquire_timeout(POSTGRES_ACQUIRE_DEADLINE)
        .idle_timeout(Some(POSTGRES_IDLE_LIFETIME))
        .max_lifetime(Some(POSTGRES_MAX_LIFETIME))
        .test_before_acquire(true)
        .connect_with(options)
        .await
        .map_err(|source| {
            Report::new(OpenClientError::Connect {
                transport: "Postgres",
                reason: source.to_string(),
            })
        })
}

/// The connection options one Postgres client connects with.
///
/// The URL carries the endpoint, the user and the database explicitly, and selects one of two TLS
/// policies. Mounted files are the client's TLS-file interface: an opportunistic fallback and an
/// encrypted connection without peer verification are both refused rather than silently allowed.
fn postgres_connect_options(
    addr: &str,
    config: &[nervix_models::ClientConfigEntry],
) -> Result<PgConnectOptions, Report<OpenClientError>> {
    let invalid = |reason: String| {
        Report::new(OpenClientError::InvalidConfig {
            transport: "Postgres",
            reason,
        })
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

impl PostgresEmitter {
    fn is_record_sqlstate(code: &str) -> bool {
        code.starts_with("22") || code.starts_with("23")
    }

    pub(in crate::runtime) async fn new(
        model: &Model,
        client: &nervix_models::CreateClientPostgres,
        resolved: Option<&ResolvedClientConfig>,
        context: &EmitterSinkContext,
        values: &[PostgresValueMapping],
        input_schema: StdArc<arrow_schema::Schema>,
    ) -> Self {
        let client = match context
            .runtime
            .lease_shared_client(&context.domain, &client.name, model, resolved)
            .await
        {
            Ok(lease) => Some(PostgresEmitterClient {
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
                context.report_init_error("postgres", &error.to_string());
                None
            }
        };
        let program = match compile_postgres_values_program(
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
                    "failed to compile postgres emitter values"
                );
                None
            }
        };
        Self { client, program }
    }

    fn value_to_text(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::Null => None,
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => Some(value.to_string()),
        }
    }

    fn quote_ident(identifier: &str) -> String {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    /// The declared type of each mapped column, read on a connection borrowed for this lookup
    /// alone and returned before the inserts that follow it.
    async fn column_types(
        client: &PostgresEmitterClient,
        table: &TableName,
        columns: &[String],
    ) -> Result<Vec<String>, PostgresWriteError> {
        let mut connection = client
            .connection()
            .await
            .map_err(|error| PostgresWriteError::Pool(error.to_string()))?;
        let table_name = table.as_str().to_string();
        let column_refs = columns.to_vec();
        let rows = sqlx::query(
            "SELECT a.attname, a.atttypid::regtype::text FROM pg_attribute a WHERE a.attrelid = \
             to_regclass($1) AND a.attname = ANY($2::text[]) AND a.attnum > 0 AND NOT \
             a.attisdropped",
        )
        .bind(table_name)
        .bind(column_refs)
        .fetch_all(&mut *connection)
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
                        table: table.as_str().to_string(),
                        column: column.clone(),
                    }
                })
            })
            .collect()
    }

    /// One bounded insert, on a connection borrowed for that insert and returned after it, so a
    /// flush of several chunks lets other local emitters through between them.
    async fn publish_rows_with_types(
        client: &PostgresEmitterClient,
        table: &TableName,
        mappings: &[PostgresValueMapping],
        conflict_action: &PostgresConflictAction,
        column_types: &[String],
        rows: &[&[serde_json::Value]],
    ) -> Result<u64, PostgresWriteError> {
        if rows.is_empty() {
            return Ok(0);
        }
        let columns = mappings
            .iter()
            .map(|mapping| mapping.column.clone())
            .collect::<Vec<_>>();
        let mut column_values = vec![Vec::<Option<String>>::new(); columns.len()];
        for row in rows {
            if row.len() != columns.len() {
                return Err(PostgresWriteError::InvalidValues(format!(
                    "Postgres VALUES produced {} columns for {} mappings",
                    row.len(),
                    columns.len()
                )));
            }
            for (index, value) in row.iter().enumerate() {
                column_values[index].push(Self::value_to_text(value));
            }
        }
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
        let conflict_clause = Self::conflict_clause(&columns, conflict_action)?;
        let sql = format!(
            "INSERT INTO {} ({insert_columns}) SELECT {select_columns} FROM unnest({param_refs}) \
             AS t({unnest_columns}){conflict_clause}",
            Self::quote_ident(table.as_str()),
        );
        // Every value is a bound parameter and every identifier went through `quote_ident`, so the
        // only thing interpolated into this statement is a quoted name or a positional placeholder.
        let mut query = sqlx::query(AssertSqlSafe(sql));
        for values in column_values {
            query = query.bind(values);
        }
        let mut connection = client
            .connection()
            .await
            .map_err(|error| PostgresWriteError::Pool(error.to_string()))?;
        let result = connection
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
                let target_columns = target.iter().collect::<HashSet<_>>();
                let assignments = columns
                    .iter()
                    .filter(|column| !target_columns.contains(column))
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

    pub(super) async fn publish_pending_chunks(
        &self,
        batch_index: usize,
        table: &TableName,
        values: &[PostgresValueMapping],
        conflict_action: &PostgresConflictAction,
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
                    .attach_printable("no initialized postgres sink client"),
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
            match outcome.filter_mapped_chunks(batch_index, &rows, pending_chunks, "postgres") {
                Ok(pending_chunks) => pending_chunks,
                Err(error) => {
                    outcome.fail(error);
                    return outcome;
                }
            };
        if pending_chunks.is_empty() {
            return outcome;
        }
        let columns = values
            .iter()
            .map(|mapping| mapping.column.clone())
            .collect::<Vec<_>>();
        let request_acks = batch.merged_acks();
        let column_types = match await_emitter_confirmation(
            &request_acks,
            Self::column_types(client, table, &columns),
        )
        .await
        {
            Ok(column_types) => column_types,
            Err(error) => {
                outcome.fail(error.into_report());
                return outcome;
            }
        };
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
                Self::publish_rows_with_types(
                    client,
                    table,
                    values,
                    conflict_action,
                    &column_types,
                    &chunk_rows,
                ),
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
                                    PostgresWriteError::InvalidValues(format!(
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
                            Self::publish_rows_with_types(
                                client,
                                table,
                                values,
                                conflict_action,
                                &column_types,
                                &single_row,
                            ),
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
            "emitter published postgres rows"
        );
        outcome
    }

    fn rows_at_indices<'a>(
        rows: &'a [Result<Vec<serde_json::Value>, StructuredMessageError>],
        indices: &[usize],
    ) -> Result<Vec<&'a [serde_json::Value]>, PostgresWriteError> {
        indices
            .iter()
            .map(|row| {
                let Some(values) = rows.get(*row) else {
                    return Err(PostgresWriteError::InvalidValues(format!(
                        "pending row {row} has no mapped VALUES in batch with {} rows",
                        rows.len()
                    )));
                };
                let Ok(values) = values else {
                    return Err(PostgresWriteError::InvalidValues(format!(
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
    fn classifies_only_data_and_constraint_sqlstates_as_record_errors() {
        for code in ["22001", "22003", "22P02", "23000", "23502", "23505"] {
            assert!(
                PostgresEmitter::is_record_sqlstate(code),
                "{code} should be a definitive record error"
            );
        }
        for code in ["08006", "40001", "42P01", "53300", "57P01"] {
            assert!(
                !PostgresEmitter::is_record_sqlstate(code),
                "{code} requires infrastructure retry"
            );
        }
    }
}
