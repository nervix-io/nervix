use ::clickhouse::{Client as ClickHouseClient, error::Error as ClickHouseError};
use hyper_util::{
    client::legacy::{Client as HyperClient, connect::HttpConnector},
    rt::TokioExecutor as HyperTokioExecutor,
};

use super::*;

pub(in crate::runtime) struct ClickHouseEmitter {
    client: Option<ClickHouseClient>,
    request_timeout: Option<Duration>,
    program: Option<CompiledSqlValuesProgram>,
}

#[derive(Debug, thiserror::Error)]
#[error("ClickHouse insert failed: {0}")]
struct ClickHouseWriteError(ClickHouseError);

impl ClickHouseWriteError {
    fn record_error_name(&self) -> Option<&'static str> {
        let ClickHouseError::BadResponse(response) = &self.0 else {
            return None;
        };
        if response.contains("413 Payload Too Large")
            || response.contains("413 Request Entity Too Large")
        {
            return Some("PAYLOAD_TOO_LARGE");
        }
        [
            "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN",
            "CANNOT_PARSE_DATETIME",
            "CANNOT_PARSE_DATE",
            "CANNOT_PARSE_NUMBER",
            "CANNOT_PARSE_TEXT",
            "TOO_LARGE_STRING_SIZE",
            "VIOLATED_CONSTRAINT",
        ]
        .into_iter()
        .find(|name| response.contains(&format!("({name})")))
    }

    fn is_record_error(&self) -> bool {
        self.record_error_name().is_some()
    }

    fn record_reason(&self) -> String {
        self.record_error_name().map_or_else(
            || "ClickHouse rejected record".to_string(),
            |name| format!("ClickHouse rejected record with {name}"),
        )
    }

    fn into_report(self) -> Report<EmitterRuntimeError> {
        let reason = self.record_error_name().map_or_else(
            || "ClickHouse insert request failed".to_string(),
            |name| format!("ClickHouse insert request failed with {name}"),
        );
        Report::new(EmitterRuntimeError::PublishBatch).attach_printable(reason)
    }
}

impl ClickHouseEmitter {
    pub(in crate::runtime) fn new(
        client: &nervix_models::CreateClientClickHouse,
        resolved: Option<&ResolvedClientConfig>,
        context: &EmitterSinkContext,
        values: &[ClickHouseValueMapping],
        input_schema: StdArc<arrow_schema::Schema>,
    ) -> Self {
        let (client, request_timeout) = match Self::client_from_config(
            resolved
                .map(|config| config.entries.as_slice())
                .unwrap_or(client.config.as_slice()),
        ) {
            Ok((client, request_timeout)) => (Some(client), request_timeout),
            Err(error) => {
                context.report_init_error("clickhouse", &emitter_error_message(&error));
                (None, None)
            }
        };
        let program = match compile_clickhouse_values_program(
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
                    "failed to compile clickhouse emitter values"
                );
                None
            }
        };
        Self {
            client,
            request_timeout,
            program,
        }
    }

    pub(in crate::runtime) fn client_from_config(
        config: &[nervix_models::ClientConfigEntry],
    ) -> EmitterRuntimeResult<(ClickHouseClient, Option<Duration>)> {
        let addr = emitter_config_value(config, "addr", || {
            "missing ClickHouse client config key 'addr'".to_string()
        })?;
        let request_timeout = optional_client_config_value(config, "timeout_ms")
            .map(|timeout_ms| {
                timeout_ms
                    .parse::<u64>()
                    .map(Duration::from_millis)
                    .map_err(|_| {
                        emitter_config_error(format!(
                            "invalid ClickHouse timeout_ms '{timeout_ms}'"
                        ))
                    })
            })
            .transpose()?;
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
        Ok((client, request_timeout))
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
    ) -> EmitterRuntimeResult<Vec<Result<String, StructuredMessageError>>> {
        let rows = sql_mapped_batch_values(program, mappings, batch, execution_now).await?;
        let mut lines = Vec::with_capacity(rows.len());
        for row in rows {
            lines.push(match row {
                Ok(row) => Ok(Self::row_json_line(mappings, row)?),
                Err(error) => Err(error),
            });
        }
        Ok(lines)
    }

    async fn publish_json_lines(
        client: &ClickHouseClient,
        table: &str,
        lines: &[&str],
        request_timeout: Option<Duration>,
    ) -> Result<(), ClickHouseWriteError> {
        if lines.is_empty() {
            return Ok(());
        }
        let sql = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let mut insert = client
            .insert_formatted_with(sql)
            .with_timeouts(request_timeout, request_timeout);
        let mut data = lines.join("\n").into_bytes();
        if !data.ends_with(b"\n") {
            data.push(b'\n');
        }
        insert
            .send(data.into())
            .await
            .map_err(ClickHouseWriteError)?;
        insert.end().await.map_err(ClickHouseWriteError)
    }

    pub(super) async fn publish_pending_chunks(
        &self,
        batch_index: usize,
        table: &Identifier,
        values: &[ClickHouseValueMapping],
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
                    .attach_printable("no initialized clickhouse sink client"),
            );
            return outcome;
        };
        let lines = match Self::batch_json_lines(program, values, batch, current_timestamp()).await
        {
            Ok(lines) => lines,
            Err(error) => {
                outcome.fail(error);
                return outcome;
            }
        };
        let pending_chunks =
            match outcome.filter_mapped_chunks(batch_index, &lines, pending_chunks, "clickhouse") {
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
            let chunk_lines = match chunk
                .iter()
                .map(|row| {
                    lines
                        .get(*row)
                        .and_then(|line| line.as_ref().ok())
                        .map(String::as_str)
                        .ok_or_else(|| {
                            Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(format!(
                                "clickhouse pending row {row} has no mapped line in batch with {} \
                                 rows",
                                lines.len()
                            ))
                        })
                })
                .collect::<EmitterRuntimeResult<Vec<_>>>()
            {
                Ok(chunk_lines) => chunk_lines,
                Err(error) => {
                    outcome.fail(error);
                    return outcome;
                }
            };
            match await_emitter_confirmation(
                &request_acks,
                Self::publish_json_lines(
                    client,
                    table.as_str(),
                    &chunk_lines,
                    self.request_timeout,
                ),
            )
            .await
            {
                Ok(()) => {
                    for row in chunk {
                        outcome.deliver((batch_index, *row));
                    }
                }
                Err(error) if error.is_record_error() && chunk.len() > 1 => {
                    for row in chunk {
                        tokio::task::consume_budget().await;
                        let Some(line) = lines
                            .get(*row)
                            .and_then(|line| line.as_ref().ok())
                            .map(String::as_str)
                        else {
                            outcome.fail(
                                Report::new(EmitterRuntimeError::EncodeBatch).attach_printable(
                                    format!(
                                        "clickhouse pending row {row} is outside mapped batch \
                                         with {} rows",
                                        lines.len()
                                    ),
                                ),
                            );
                            return outcome;
                        };
                        match await_emitter_confirmation(
                            &request_acks,
                            Self::publish_json_lines(
                                client,
                                table.as_str(),
                                &[line],
                                self.request_timeout,
                            ),
                        )
                        .await
                        {
                            Ok(()) => outcome.deliver((batch_index, *row)),
                            Err(error) if error.is_record_error() => {
                                outcome.reject((batch_index, *row), error.record_reason());
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
            "emitter published clickhouse rows"
        );
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_config(
        addr: impl Into<String>,
        timeout_ms: &str,
    ) -> Vec<nervix_models::ClientConfigEntry> {
        vec![
            nervix_models::ClientConfigEntry {
                key: "addr".to_string(),
                value: addr.into(),
            },
            nervix_models::ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: timeout_ms.to_string(),
            },
        ]
    }

    #[test]
    fn classifies_only_definitive_clickhouse_record_errors() {
        for name in [
            "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN",
            "CANNOT_PARSE_NUMBER",
            "TOO_LARGE_STRING_SIZE",
            "VIOLATED_CONSTRAINT",
        ] {
            let error = ClickHouseWriteError(ClickHouseError::BadResponse(format!(
                "Code: 1. DB::Exception: rejected ({name})"
            )));
            assert!(
                error.is_record_error(),
                "{name} should be a definitive record error"
            );
        }
        let oversized = ClickHouseWriteError(ClickHouseError::BadResponse(
            "413 Payload Too Large".to_string(),
        ));
        assert!(oversized.is_record_error());
        for name in [
            "NETWORK_ERROR",
            "TABLE_IS_DROPPED",
            "TIMEOUT_EXCEEDED",
            "TOO_MANY_REQUESTS",
        ] {
            let error = ClickHouseWriteError(ClickHouseError::BadResponse(format!(
                "Code: 1. DB::Exception: rejected ({name})"
            )));
            assert!(
                !error.is_record_error(),
                "{name} requires infrastructure retry"
            );
        }
    }

    #[test]
    fn client_rejects_an_invalid_request_timeout() {
        let error = match ClickHouseEmitter::client_from_config(&client_config(
            "http://127.0.0.1:8123",
            "later",
        )) {
            Ok(_) => panic!("invalid ClickHouse timeout should fail client initialization"),
            Err(error) => error,
        };

        assert!(
            format!("{error:?}").contains("invalid ClickHouse timeout_ms 'later'"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn client_parses_the_request_timeout() {
        let (_, request_timeout) =
            ClickHouseEmitter::client_from_config(&client_config("http://127.0.0.1:8123", "275"))
                .expect("ClickHouse client config should be valid");

        assert_eq!(request_timeout, Some(Duration::from_millis(275)));
    }

    #[tokio::test]
    async fn configured_timeout_bounds_clickhouse_insert_completion() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let addr = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("test listener should have an address")
        );
        let (client, request_timeout) =
            ClickHouseEmitter::client_from_config(&client_config(addr, "30"))
                .expect("ClickHouse client config should be valid");

        let result = tokio::time::timeout(
            Duration::from_millis(250),
            ClickHouseEmitter::publish_json_lines(
                &client,
                "events",
                &[r#"{"id":1}"#],
                request_timeout,
            ),
        )
        .await
        .expect("configured ClickHouse timeout should bound the insert")
        .expect_err("the non-responsive endpoint should time out");

        assert!(
            matches!(result.0, ClickHouseError::TimedOut),
            "unexpected ClickHouse insert error: {result:?}"
        );
    }
}
