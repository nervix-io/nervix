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

impl PostgresEmitter {
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
    ) -> EmitterRuntimeResult<Vec<String>> {
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
            .map_err(|source| {
                Report::new(EmitterRuntimeError::PublishBatch)
                    .attach_printable(format!("failed to load Postgres table metadata: {source}"))
            })?;
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
                    Report::new(EmitterRuntimeError::PublishBatch).attach_printable(format!(
                        "Postgres table '{}' has no column '{}'",
                        table.as_str(),
                        column
                    ))
                })
            })
            .collect()
    }

    async fn publish_rows(
        client: &PostgresClient,
        table: &Identifier,
        mappings: &[PostgresValueMapping],
        conflict_action: &PostgresConflictAction,
        rows: &[Vec<serde_json::Value>],
    ) -> EmitterRuntimeResult<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let columns = mappings
            .iter()
            .map(|mapping| mapping.column.clone())
            .collect::<Vec<_>>();
        let column_types = Self::column_types(client, table, &columns).await?;
        let mut column_values = vec![Vec::<Option<String>>::new(); columns.len()];
        for row in rows {
            if row.len() != columns.len() {
                return Err(
                    Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                        "Postgres VALUES produced {} columns for {} mappings",
                        row.len(),
                        columns.len()
                    )),
                );
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
        client.execute(&sql, &params).await.map_err(|source| {
            Report::new(EmitterRuntimeError::PublishBatch)
                .attach_printable(format!("failed to publish Postgres rows: {source}"))
        })
    }

    fn conflict_clause(
        columns: &[String],
        action: &PostgresConflictAction,
    ) -> EmitterRuntimeResult<String> {
        match action {
            PostgresConflictAction::None => Ok(String::new()),
            PostgresConflictAction::DoNothing { target } => {
                let target = Self::conflict_target_sql(target)?;
                Ok(format!(" ON CONFLICT{target} DO NOTHING"))
            }
            PostgresConflictAction::DoUpdate { target } => {
                if target.is_empty() {
                    return Err(
                        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(
                            "Postgres ON CONFLICT DO UPDATE requires a conflict target",
                        ),
                    );
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
                    return Err(
                        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(
                            "Postgres ON CONFLICT DO UPDATE requires at least one non-conflict \
                             VALUES column to update",
                        ),
                    );
                }
                let target = Self::conflict_target_sql(target)?;
                Ok(format!(
                    " ON CONFLICT{target} DO UPDATE SET {}",
                    assignments.join(", ")
                ))
            }
        }
    }

    fn conflict_target_sql(target: &[String]) -> EmitterRuntimeResult<String> {
        if target.is_empty() {
            Ok(String::new())
        } else if target.iter().any(|column| column.is_empty()) {
            Err(Report::new(EmitterRuntimeError::EncodeBatch)
                .attach_printable("Postgres ON CONFLICT target columns must not be empty"))
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

    pub(super) async fn publish_batch(
        &self,
        table: &Identifier,
        values: &[PostgresValueMapping],
        conflict_action: &PostgresConflictAction,
        batch: &RelayRecordBatch,
    ) -> EmitterRuntimeResult<()> {
        let (Some(client), Some(program)) = (self.client.as_ref(), self.program.as_ref()) else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized postgres sink client"));
        };
        let rows = sql_mapped_batch_values(program, values, batch, current_timestamp()).await?;
        Self::publish_rows(&client.client, table, values, conflict_action, &rows).await?;
        trace!(
            table = table.as_str(),
            rows = rows.len(),
            "emitter published postgres rows"
        );
        Ok(())
    }
}
