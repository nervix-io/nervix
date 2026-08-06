use mysql_async::{
    Opts as MySqlOpts, OptsBuilder as MySqlOptsBuilder, Params as MySqlParams, Pool as MySqlPool,
    SslOpts as MySqlSslOpts, Value as MySqlValue, prelude::Queryable as MySqlQueryable,
};

use super::*;

pub(in crate::runtime) struct MySqlEmitter {
    client: Option<MySqlEmitterClient>,
    program: Option<CompiledSqlValuesProgram>,
}

struct MySqlEmitterClient {
    pool: MySqlPool,
}

#[derive(Debug, thiserror::Error)]
enum MySqlWriteError {
    #[error("invalid MySQL VALUES: {0}")]
    InvalidValues(String),
    #[error("failed to connect to MySQL: {0}")]
    Connect(mysql_async::Error),
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
        server_error.map_or_else(
            || "MySQL rejected record".to_string(),
            |error| {
                format!(
                    "MySQL rejected record with SQLSTATE {} and code {}",
                    error.state, error.code
                )
            },
        )
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

impl MySqlEmitter {
    fn is_record_server_error(state: &str, code: u16) -> bool {
        state.starts_with("22") || state.starts_with("23") || matches!(code, 1153 | 1366)
    }

    pub(in crate::runtime) async fn new(
        client: &nervix_models::CreateClientMySql,
        resolved: Option<&ResolvedClientConfig>,
        context: &EmitterSinkContext,
        values: &[MySqlValueMapping],
        input_schema: StdArc<arrow_schema::Schema>,
    ) -> Self {
        let client = match Self::client_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        )
        .await
        {
            Ok(client) => Some(client),
            Err(error) => {
                context.report_init_error("mysql", &emitter_error_message(&error));
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
                let _ = context.events.send(RuntimeEvent::Error(error.to_string()));
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

    async fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<MySqlEmitterClient> {
        let addr = emitter_config_value(config, "addr", || {
            "missing MySQL client config key 'addr'".to_string()
        })?;
        let opts = MySqlOpts::from_url(&addr).map_err(|source| {
            emitter_config_error(format!("failed to parse MySQL client addr: {source}"))
        })?;
        let opts = if let Some(ca_file) = optional_client_config_value(config, "tls_ca_file") {
            let ssl_opts = MySqlSslOpts::default()
                .with_root_certs(vec![PathBuf::from(ca_file).into()])
                .with_disable_built_in_roots(true);
            MySqlOptsBuilder::from_opts(opts).ssl_opts(Some(ssl_opts))
        } else {
            MySqlOptsBuilder::from_opts(opts)
        };
        let pool = MySqlPool::new(opts);
        let mut conn = pool.get_conn().await.map_err(|source| {
            emitter_init_error(format!("failed to connect to MySQL: {source}"))
        })?;
        conn.query_drop("SELECT 1").await.map_err(|source| {
            emitter_init_error(format!("failed to validate MySQL connection: {source}"))
        })?;
        drop(conn);
        Ok(MySqlEmitterClient { pool })
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
        table: &Identifier,
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
            .pool
            .get_conn()
            .await
            .map_err(MySqlWriteError::Connect)?;
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
        table: &Identifier,
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
                        outcome.deliver((batch_index, *row));
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
                            Ok(_) => outcome.deliver((batch_index, *row)),
                            Err(error) if error.is_record_error() => {
                                outcome.reject((batch_index, *row), error.record_reason())
                            }
                            Err(error) => {
                                outcome.fail(error.into_report());
                                return outcome;
                            }
                        }
                    }
                }
                Err(error) if error.is_record_error() => {
                    if let Some(row) = chunk.first() {
                        outcome.reject((batch_index, *row), error.record_reason());
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
                rows.get(*row)
                    .and_then(|values| values.as_ref().ok())
                    .map(Vec::as_slice)
                    .ok_or_else(|| {
                        MySqlWriteError::InvalidValues(format!(
                            "pending row {row} has no mapped VALUES in batch with {} rows",
                            rows.len()
                        ))
                    })
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
