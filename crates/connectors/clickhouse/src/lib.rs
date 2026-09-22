//! ClickHouse sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** ClickHouse client and TLS configuration, the `JSONEachRow` encoding of each mapped
//!   row, insert chunking into single rows after a rejected write, and insert-error
//!   classification.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio
//!   and the `clickhouse` driver.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use std::time::Duration;

use ::clickhouse::{Client as ClickHouseClient, error::Error as ClickHouseError};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, ListArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use chrono::DateTime;
use error_stack::Report;
use hyper_util::{
    client::legacy::{Client as HyperClient, connect::HttpConnector},
    rt::TokioExecutor as HyperTokioExecutor,
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    MappedSinkRows, PerRecordOutcome, RejectedSinkRecord, RowSink, RustlsClientConfigSource,
    SinkHost, SinkLifecycle, SinkPublishError, SinkRecordPosition, SinkStartError, SinkStartResult,
    client_config_value, optional_client_config_value,
};
use nervix_models::{ClientConfigEntry, TableName};
use tracing::trace;

const CLICKHOUSE: &str = "clickhouse";

/// What one ClickHouse emitter inserts through, from its typed sink plan.
pub struct ClickHouseSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub table: TableName,
}

/// The ClickHouse sink, which encodes each mapped row as one `JSONEachRow` line.
pub struct ClickHouseSink {
    client: ClickHouseClient,
    request_timeout: Option<Duration>,
    table: TableName,
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
        match self.record_error_name() {
            Some(name) => format!("ClickHouse rejected record with {name}"),
            None => "ClickHouse rejected record".to_string(),
        }
    }

    fn into_report(self) -> Report<SinkPublishError> {
        let reason = match self.record_error_name() {
            Some(name) => format!("ClickHouse insert request failed with {name}"),
            None => "ClickHouse insert request failed".to_string(),
        };
        Report::new(SinkPublishError::Publish { sink: CLICKHOUSE }).attach_printable(reason)
    }
}

/// A mapped column this sink cannot write as JSON, named with the exact type it carries.
#[derive(Debug, thiserror::Error)]
#[error("ClickHouse VALUES column '{column}' has unsupported exact type {data_type}")]
struct UnsupportedMappedColumn {
    column: String,
    data_type: DataType,
}

/// The mapped columns of one batch, downcast once so every row reads from the column that holds it.
///
/// Each target column is quoted once here rather than once per row, and the line is written in
/// mapping order so the same mapping always produces the same bytes.
struct MappedJsonColumns<'a> {
    columns: Vec<MappedJsonField<'a>>,
}

/// One target column of the insert: its quoted JSON key and the Arrow column its values come from.
struct MappedJsonField<'a> {
    key: String,
    values: MappedJsonColumn<'a>,
}

impl<'a> MappedJsonColumns<'a> {
    fn new(
        batch: &'a RecordBatch,
        target_columns: &[String],
    ) -> Result<Self, UnsupportedMappedColumn> {
        let mut columns = Vec::with_capacity(target_columns.len());
        for (index, column) in target_columns.iter().enumerate() {
            let array = batch.column(index);
            let values = MappedJsonColumn::new(array).ok_or_else(|| UnsupportedMappedColumn {
                column: column.clone(),
                data_type: array.data_type().clone(),
            })?;
            columns.push(MappedJsonField {
                key: serde_json::Value::String(column.clone()).to_string(),
                values,
            });
        }
        Ok(Self { columns })
    }

    /// One `JSONEachRow` line, read column by column at the row the host selected.
    fn row_line(&self, row: usize) -> String {
        let mut line = String::from("{");
        for (index, field) in self.columns.iter().enumerate() {
            if index != 0 {
                line.push(',');
            }
            line.push_str(&field.key);
            line.push(':');
            line.push_str(&field.values.value(row).to_string());
        }
        line.push('}');
        line
    }
}

/// One mapped column, held as the typed Arrow array it arrived in.
enum MappedJsonColumn<'a> {
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
        elements: Box<MappedJsonColumn<'a>>,
    },
}

impl<'a> MappedJsonColumn<'a> {
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

    fn value(&self, row: usize) -> serde_json::Value {
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
                    items.push(elements.value(element));
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

impl ClickHouseSink {
    pub fn new(config: ClickHouseSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let ClickHouseSinkConfig { config, table } = config;
        let (client, request_timeout) = Self::client_from_config(&config)?;
        Ok(Self {
            client,
            request_timeout,
            table,
        })
    }

    pub fn client_from_config(
        config: &[ClientConfigEntry],
    ) -> SinkStartResult<(ClickHouseClient, Option<Duration>)> {
        let invalid = || Report::new(SinkStartError::InvalidConfiguration { sink: CLICKHOUSE });
        let addr = client_config_value(config, "addr", "ClickHouse")
            .map_err(|error| invalid().attach_printable(error.to_string()))?;
        let request_timeout = optional_client_config_value(config, "timeout_ms")
            .map(|timeout_ms| {
                timeout_ms
                    .parse::<u64>()
                    .map(Duration::from_millis)
                    .map_err(|_| {
                        invalid().attach_printable(format!(
                            "invalid ClickHouse timeout_ms '{timeout_ms}'"
                        ))
                    })
            })
            .transpose()?;
        let tls_config = RustlsClientConfigSource::new(config)
            .build()
            .map_err(|error| invalid().attach_printable(error.to_string()))?;
        let mut client = if let Some(tls_config) = tls_config {
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
}

#[async_trait::async_trait]
impl SinkLifecycle for ClickHouseSink {}

#[async_trait::async_trait]
impl RowSink for ClickHouseSink {
    async fn publish(&mut self, rows: MappedSinkRows<'_>) -> PerRecordOutcome {
        let mut outcome = PerRecordOutcome::with_capacity(rows.selected_rows.len());
        let columns = match MappedJsonColumns::new(rows.batch, rows.target_columns) {
            Ok(columns) => columns,
            Err(error) => {
                outcome.fail(
                    Report::new(SinkPublishError::Publish { sink: CLICKHOUSE })
                        .attach_printable(error.to_string()),
                );
                return outcome;
            }
        };
        for chunk in rows.selected_row_chunks {
            tokio::task::consume_budget().await;
            let Some(chunk_rows) = rows.selected_rows.get(chunk.clone()) else {
                outcome.fail(
                    Report::new(SinkPublishError::Publish { sink: CLICKHOUSE }).attach_printable(
                        format!(
                            "ClickHouse chunk {chunk:?} is outside its {} selected rows",
                            rows.selected_rows.len()
                        ),
                    ),
                );
                return outcome;
            };
            let lines = chunk_rows
                .iter()
                .map(|row| columns.row_line(*row))
                .collect::<Vec<_>>();
            let chunk_lines = lines.iter().map(String::as_str).collect::<Vec<_>>();
            match Self::publish_json_lines(
                &self.client,
                self.table.as_str(),
                &chunk_lines,
                self.request_timeout,
            )
            .await
            {
                Ok(()) => {
                    for row in chunk_rows {
                        outcome.deliver(SinkRecordPosition {
                            batch_index: rows.batch_index,
                            row_index: *row,
                        });
                    }
                }
                Err(error) if error.is_record_error() && chunk_rows.len() > 1 => {
                    for (offset, row) in chunk_rows.iter().enumerate() {
                        tokio::task::consume_budget().await;
                        let position = SinkRecordPosition {
                            batch_index: rows.batch_index,
                            row_index: *row,
                        };
                        let line = [lines[offset].as_str()];
                        match Self::publish_json_lines(
                            &self.client,
                            self.table.as_str(),
                            &line,
                            self.request_timeout,
                        )
                        .await
                        {
                            Ok(()) => outcome.deliver(position),
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
            "emitter published clickhouse rows"
        );
        outcome
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use arrow_schema::{Field, Schema, TimeUnit};

    use super::*;

    fn client_config(addr: impl Into<String>, timeout_ms: &str) -> Vec<ClientConfigEntry> {
        vec![
            ClientConfigEntry {
                key: "addr".to_string(),
                value: addr.into(),
            },
            ClientConfigEntry {
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
    fn encodes_each_mapped_column_from_the_row_it_holds() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("at", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "tags",
                DataType::List(StdArc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ]));
        let tags = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![]),
        ]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![Some(7), None])),
                StdArc::new(StringArray::from(vec![Some("first"), Some("second")])),
                StdArc::new(TimestampNanosecondArray::from(vec![
                    Some(1_700_000_000_123_456_789),
                    None,
                ])),
                StdArc::new(tags),
            ],
        )
        .expect("the mapped batch should build");
        let columns = [
            "id".to_string(),
            "name".to_string(),
            "at".to_string(),
            "tags".to_string(),
        ];

        let mapped = MappedJsonColumns::new(&batch, &columns).expect("columns should be mapped");

        assert_eq!(
            mapped.row_line(0),
            r#"{"id":7,"name":"first","at":"2023-11-14T22:13:20.123456789+00:00","tags":[1,2]}"#
        );
        assert_eq!(
            mapped.row_line(1),
            r#"{"id":null,"name":"second","at":null,"tags":[]}"#
        );
    }

    #[test]
    fn client_rejects_an_invalid_request_timeout() {
        let error = match ClickHouseSink::client_from_config(&client_config(
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
            ClickHouseSink::client_from_config(&client_config("http://127.0.0.1:8123", "275"))
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
            ClickHouseSink::client_from_config(&client_config(addr, "30"))
                .expect("ClickHouse client config should be valid");

        let result = tokio::time::timeout(
            Duration::from_millis(250),
            ClickHouseSink::publish_json_lines(
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

    #[test]
    fn clickhouse_client_config_validates_tls_ca_file() {
        let error = match ClickHouseSink::client_from_config(&[
            ClientConfigEntry {
                key: "addr".to_string(),
                value: "https://127.0.0.1:8124".to_string(),
            },
            ClientConfigEntry {
                key: "tls_ca_file".to_string(),
                value: "/tmp/nervix-missing-clickhouse-ca.pem".to_string(),
            },
        ]) {
            Ok(_) => panic!("missing ClickHouse TLS CA should fail"),
            Err(error) => error,
        };
        let error = format!("{error:?}");

        assert!(error.contains("TLS CA certificate"));
    }
}
