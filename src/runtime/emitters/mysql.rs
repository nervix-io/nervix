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

impl MySqlEmitter {
    pub(in crate::runtime) async fn new(
        client: &nervix_models::CreateClientMySql,
        resolved: Option<&ResolvedClientConfig>,
        context: &EmitterSinkContext,
        values: &[MySqlValueMapping],
        input_schema: Arc<arrow_schema::Schema>,
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
            &context.from_relay,
            values,
            input_schema,
            context.branch_schema.clone(),
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
        rows: &[Vec<serde_json::Value>],
    ) -> EmitterRuntimeResult<u64> {
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
                return Err(
                    Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                        "MySQL VALUES produced {} columns for {} mappings",
                        row.len(),
                        mappings.len()
                    )),
                );
            }
            params.extend(row.iter().map(Self::value));
        }
        let mut conn = client.pool.get_conn().await.map_err(|source| {
            Report::new(EmitterRuntimeError::PublishBatch)
                .attach_printable(format!("failed to connect to MySQL: {source}"))
        })?;
        conn.exec_drop(sql, MySqlParams::Positional(params))
            .await
            .map_err(|source| {
                Report::new(EmitterRuntimeError::PublishBatch)
                    .attach_printable(format!("failed to publish MySQL rows: {source}"))
            })?;
        Ok(conn.affected_rows())
    }

    fn conflict_clause(
        quoted_columns: &[String],
        conflict_action: &MySqlConflictAction,
    ) -> EmitterRuntimeResult<String> {
        match conflict_action {
            MySqlConflictAction::None => Ok(String::new()),
            MySqlConflictAction::DoNothing => {
                let Some(column) = quoted_columns.first() else {
                    return Err(
                        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(
                            "MySQL ON CONFLICT DO NOTHING requires at least one VALUES column",
                        ),
                    );
                };
                Ok(format!(" ON DUPLICATE KEY UPDATE {column} = {column}"))
            }
            MySqlConflictAction::DoUpdate => {
                if quoted_columns.is_empty() {
                    return Err(
                        Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(
                            "MySQL ON CONFLICT DO UPDATE requires at least one VALUES column",
                        ),
                    );
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

    pub(super) async fn publish_batch(
        &self,
        table: &Identifier,
        values: &[MySqlValueMapping],
        conflict_action: &MySqlConflictAction,
        batch: &RelayRecordBatch,
    ) -> EmitterRuntimeResult<()> {
        let (Some(client), Some(program)) = (self.client.as_ref(), self.program.as_ref()) else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized mysql sink client"));
        };
        let rows = sql_mapped_batch_values(program, values, batch, current_timestamp()).await?;
        Self::publish_rows(client, table, values, conflict_action, &rows).await?;
        trace!(
            table = table.as_str(),
            rows = rows.len(),
            "emitter published mysql rows"
        );
        Ok(())
    }
}
