use postgres_types::ToSql;
use tokio_postgres::{Client as PostgresClient, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

use super::*;

pub(in crate::runtime) struct PostgresEmitter {
    client: Option<PostgresEmitterClient>,
    program: Option<CompiledSqlValuesProgram>,
}

struct PostgresEmitterClient {
    client: PostgresClient,
    _connection_task: JoinHandle<()>,
}

#[derive(Debug, thiserror::Error)]
enum PostgresWriteError {
    #[error("failed to load Postgres table metadata: {0}")]
    Metadata(tokio_postgres::Error),
    #[error("Postgres table '{table}' has no column '{column}'")]
    MissingColumn { table: String, column: String },
    #[error("invalid Postgres VALUES: {0}")]
    InvalidValues(String),
    #[error("Postgres insert failed: {0}")]
    Execute(tokio_postgres::Error),
}

impl PostgresWriteError {
    fn is_record_error(&self) -> bool {
        let Self::Execute(error) = self else {
            return false;
        };
        error
            .as_db_error()
            .is_some_and(|error| PostgresEmitter::is_record_sqlstate(error.code().code()))
    }

    fn record_reason(&self) -> String {
        let code = match self {
            Self::Execute(error) => error.as_db_error().map(|error| error.code().code()),
            _ => None,
        };
        code.map_or_else(
            || "Postgres rejected record".to_string(),
            |code| format!("Postgres rejected record with SQLSTATE {code}"),
        )
    }

    fn into_report(self) -> Report<EmitterRuntimeError> {
        match self {
            Self::InvalidValues(reason) => {
                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(reason)
            }
            Self::Execute(error) => {
                let code = error
                    .as_db_error()
                    .map(|error| error.code().code())
                    .unwrap_or("unknown");
                Report::new(EmitterRuntimeError::PublishBatch)
                    .attach_printable(format!("Postgres request failed with SQLSTATE {code}"))
            }
            error => {
                Report::new(EmitterRuntimeError::PublishBatch).attach_printable(error.to_string())
            }
        }
    }
}

impl PostgresEmitter {
    fn is_record_sqlstate(code: &str) -> bool {
        code.starts_with("22") || code.starts_with("23")
    }

    pub(in crate::runtime) async fn new(
        client: &nervix_models::CreateClientPostgres,
        resolved: Option<&ResolvedClientConfig>,
        context: &EmitterSinkContext,
        values: &[PostgresValueMapping],
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
                context.report_init_error("postgres", &emitter_error_message(&error));
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
                let _ = context.events.send(RuntimeEvent::Error(error.to_string()));
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

    async fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<PostgresEmitterClient> {
        let addr = emitter_config_value(config, "addr", || {
            "missing Postgres client config key 'addr'".to_string()
        })?;
        if let Some(tls_config) = RustlsClientConfigSource::new(config)
            .build()
            .map_err(emitter_config_error)?
        {
            let connector = MakeRustlsConnect::new((*tls_config).clone());
            let (client, connection) =
                tokio_postgres::connect(&addr, connector)
                    .await
                    .map_err(|source| {
                        emitter_init_error(format!("failed to connect to Postgres: {source}"))
                    })?;
            let connection_task = tokio::spawn(async move {
                if let Err(error) = connection.await {
                    warn!(error = %error, "postgres connection task failed");
                }
            });
            Ok(PostgresEmitterClient {
                client,
                _connection_task: connection_task,
            })
        } else {
            let (client, connection) =
                tokio_postgres::connect(&addr, NoTls)
                    .await
                    .map_err(|source| {
                        emitter_init_error(format!("failed to connect to Postgres: {source}"))
                    })?;
            let connection_task = tokio::spawn(async move {
                if let Err(error) = connection.await {
                    warn!(error = %error, "postgres connection task failed");
                }
            });
            Ok(PostgresEmitterClient {
                client,
                _connection_task: connection_task,
            })
        }
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

    async fn column_types(
        client: &PostgresClient,
        table: &Identifier,
        columns: &[String],
    ) -> Result<Vec<String>, PostgresWriteError> {
        let table_name = table.as_str().to_string();
        let column_refs = columns.to_vec();
        let rows = client
            .query(
                "SELECT a.attname, a.atttypid::regtype::text FROM pg_attribute a WHERE a.attrelid \
                 = to_regclass($1) AND a.attname = ANY($2::text[]) AND a.attnum > 0 AND NOT \
                 a.attisdropped",
                &[&table_name, &column_refs],
            )
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

    async fn publish_rows_with_types(
        client: &PostgresClient,
        table: &Identifier,
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
        let params = column_values
            .iter()
            .map(|values| values as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
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
        client
            .execute(&sql, &params)
            .await
            .map_err(PostgresWriteError::Execute)
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
        table: &Identifier,
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
            Self::column_types(&client.client, table, &columns),
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
                    &client.client,
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
                                &client.client,
                                table,
                                values,
                                conflict_action,
                                &column_types,
                                &single_row,
                            ),
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
                rows.get(*row)
                    .and_then(|values| values.as_ref().ok())
                    .map(Vec::as_slice)
                    .ok_or_else(|| {
                        PostgresWriteError::InvalidValues(format!(
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
