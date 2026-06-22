use ::clickhouse::Client as ClickHouseClient;
use hyper_util::{
    client::legacy::{Client as HyperClient, connect::HttpConnector},
    rt::TokioExecutor as HyperTokioExecutor,
};

use super::*;

pub(in crate::runtime) struct ClickHouseEmitter {
    client: Option<ClickHouseClient>,
    program: Option<CompiledSqlValuesProgram>,
}

impl ClickHouseEmitter {
    pub(in crate::runtime) fn new(
        client: &nervix_models::CreateClientClickHouse,
        resolved: Option<&ResolvedClientConfig>,
        context: &EmitterSinkContext,
        values: &[ClickHouseValueMapping],
        input_schema: Arc<arrow_schema::Schema>,
    ) -> Self {
        let client = match Self::client_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        ) {
            Ok(client) => Some(client),
            Err(error) => {
                context.report_init_error("clickhouse", &emitter_error_message(&error));
                None
            }
        };
        let program = match compile_clickhouse_values_program(
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
                    "failed to compile clickhouse emitter values"
                );
                None
            }
        };
        Self { client, program }
    }

    pub(in crate::runtime) fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<ClickHouseClient> {
        let addr = emitter_config_value(config, "addr", || {
            "missing ClickHouse client config key 'addr'".to_string()
        })?;
        let mut client = if let Some(tls_config) = RustlsClientConfigSource::new(config)
            .build()
            .map_err(emitter_config_error)?
        {
            let mut connector = HttpConnector::new();
            connector.set_keepalive(Some(Duration::from_secs(60)));
            connector.enforce_http(false);
            let connector = hyper_rustls::HttpsConnectorBuilder::new()
                .with_tls_config((*tls_config).clone())
                .https_or_http()
                .enable_http1()
                .wrap_connector(connector);
            let http_client = HyperClient::builder(HyperTokioExecutor::new())
                .pool_idle_timeout(Duration::from_secs(2))
                .build(connector);
            ClickHouseClient::with_http_client(http_client)
        } else {
            ClickHouseClient::default()
        }
        .with_url(addr);
        if let Some(user) = optional_client_config_value(config, "user") {
            client = client.with_user(user);
        }
        if let Some(password) = optional_client_config_value(config, "password") {
            client = client.with_password(password);
        }
        if let Some(database) = optional_client_config_value(config, "database") {
            client = client.with_database(database);
        }
        Ok(client)
    }

    fn row_json_line(
        mappings: &[ClickHouseValueMapping],
        values: Vec<serde_json::Value>,
    ) -> EmitterRuntimeResult<String> {
        let mut object = serde_json::Map::new();
        for (mapping, value) in mappings.iter().zip(values) {
            object.insert(mapping.column.clone(), value);
        }
        serde_json::to_string(&serde_json::Value::Object(object)).map_err(|source| {
            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(source.to_string())
        })
    }

    async fn batch_json_lines(
        program: &CompiledSqlValuesProgram,
        mappings: &[ClickHouseValueMapping],
        batch: &RelayRecordBatch,
        execution_now: Timestamp,
    ) -> EmitterRuntimeResult<Vec<String>> {
        let rows = sql_mapped_batch_values(program, mappings, batch, execution_now).await?;
        rows.into_iter()
            .map(|row| Self::row_json_line(mappings, row))
            .collect()
    }

    async fn publish_json_line(
        client: &ClickHouseClient,
        table: &str,
        lines: &[String],
    ) -> EmitterRuntimeResult<()> {
        if lines.is_empty() {
            return Ok(());
        }
        let sql = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let mut insert = client.insert_formatted_with(sql);
        let mut data = lines.join("\n").into_bytes();
        if !data.ends_with(b"\n") {
            data.push(b'\n');
        }
        insert.send(data.into()).await.map_err(|source| {
            Report::new(EmitterRuntimeError::PublishBatch).attach_printable(source.to_string())
        })?;
        insert.end().await.map_err(|source| {
            Report::new(EmitterRuntimeError::PublishBatch).attach_printable(source.to_string())
        })
    }

    pub(super) async fn publish_batch(
        &self,
        table: &Identifier,
        values: &[ClickHouseValueMapping],
        batch: &RelayRecordBatch,
    ) -> EmitterRuntimeResult<()> {
        let (Some(client), Some(program)) = (self.client.as_ref(), self.program.as_ref()) else {
            return Err(Report::new(EmitterRuntimeError::SinkNotInitialized)
                .attach_printable("no initialized clickhouse sink client"));
        };
        let lines = Self::batch_json_lines(program, values, batch, current_timestamp()).await?;
        Self::publish_json_line(client, table.as_str(), &lines).await?;
        trace!(
            table = table.as_str(),
            rows = lines.len(),
            "emitter published clickhouse rows"
        );
        Ok(())
    }
}
