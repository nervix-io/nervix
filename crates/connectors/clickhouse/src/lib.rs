//! ClickHouse sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** ClickHouse client and TLS configuration, the HTTP connector every connection of the
//!   client is made through, the `JSONEachRow` encoding of each mapped row, insert chunking into
//!   single rows after a rejected write, and insert-error classification.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio,
//!   the node resolver, and the `clickhouse` driver with its Hyper client.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.
//!
//! # Connections
//!
//! The client makes every connection through Hyper's `HttpConnector` with the node resolver as its
//! DNS service, so each new connection resolves the host of `addr` again, tries the answers in
//! order, and dials a literal address as written. The request keeps `addr` as its URL and
//! authority, and a TLS client verifies the server certificate against that host whichever address
//! accepted the connection. The insert's request timeout covers the connection, lookup included,
//! because the driver makes it while the insert waits for its result.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use std::time::Duration;

use ::clickhouse::{Client as ClickHouseClient, error::Error as ClickHouseError};
use error_stack::Report;
use hyper_util::{
    client::legacy::{Client as HyperClient, connect::HttpConnector},
    rt::TokioExecutor as HyperTokioExecutor,
};
use meticulous::ResultExt as _;
use nervix_columnar_json::{FieldNulls, Float32Encoding, JsonColumnSpec, JsonColumns, NestedNulls};
use nervix_connector::{
    MappedSinkRows, PerRecordOutcome, RejectedSinkRecord, RowSink, RustlsClientConfigSource,
    SinkHost, SinkLifecycle, SinkPublishError, SinkRecordPosition, SinkStartError, SinkStartResult,
    client_config_value, optional_client_config_value,
};
use nervix_dns::{DnsLookupError, DnsResolver};
use nervix_models::{ClientConfigEntry, TableName};
use tracing::trace;

const CLICKHOUSE: &str = "clickhouse";
/// The TCP keepalive of every connection, the driver's own default.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);
/// How long an idle pooled connection may be reused, the driver's own default. ClickHouse closes an
/// idle HTTP connection after three seconds, so the client stops reusing one a second earlier.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// What one ClickHouse emitter inserts through, from its typed sink plan.
pub struct ClickHouseSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub table: TableName,
    /// The node resolver every connection of the client resolves the host of `addr` through.
    pub dns: DnsResolver,
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

    /// The failed lookup of the ClickHouse host, when resolving it is what failed the request.
    fn lookup_failure(&self) -> Option<&DnsLookupError> {
        let ClickHouseError::Network(error) = &self.0 else {
            return None;
        };
        DnsLookupError::find_in(error.as_ref())
    }

    /// A request that never reached ClickHouse, described by the error and every cause under it.
    ///
    /// Those causes describe the connection, never a row, and carry no credentials. A response from
    /// ClickHouse is not described this way, because its text can repeat values of the rows.
    fn transport_failure(&self) -> Option<String> {
        let ClickHouseError::Network(error) = &self.0 else {
            return None;
        };
        let mut description = error.to_string();
        let mut cause = error.source();
        while let Some(current) = cause {
            description.push_str(": ");
            description.push_str(&current.to_string());
            cause = current.source();
        }
        Some(description)
    }

    fn into_report(self) -> Report<SinkPublishError> {
        let publish = SinkPublishError::Publish { sink: CLICKHOUSE };
        if let Some(lookup) = self.lookup_failure() {
            let reason = format!("ClickHouse insert request failed: {lookup}");
            return Report::new(lookup.clone())
                .change_context(publish)
                .attach_printable(reason);
        }
        let reason = if let Some(name) = self.record_error_name() {
            format!("ClickHouse insert request failed with {name}")
        } else if let Some(transport) = self.transport_failure() {
            format!("ClickHouse insert request failed: {transport}")
        } else {
            "ClickHouse insert request failed".to_string()
        };
        Report::new(publish).attach_printable(reason)
    }
}

impl ClickHouseSink {
    pub fn new(config: ClickHouseSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let ClickHouseSinkConfig { config, table, dns } = config;
        let (client, request_timeout) = Self::client_from_config(&config, dns)?;
        Ok(Self {
            client,
            request_timeout,
            table,
        })
    }

    /// The client `config` describes, whose every connection resolves through `dns`, and the
    /// request timeout of its inserts.
    ///
    /// Without TLS entries the client speaks plain HTTP only, as the driver's own default client
    /// does in a build without its TLS features; an `https` address needs the TLS entries.
    pub fn client_from_config(
        config: &[ClientConfigEntry],
        dns: DnsResolver,
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
        let mut connector = HttpConnector::new_with_resolver(dns);
        connector.set_keepalive(Some(TCP_KEEPALIVE));
        let mut pool = HyperClient::builder(HyperTokioExecutor::new());
        pool.pool_idle_timeout(POOL_IDLE_TIMEOUT);
        let client = match tls_config {
            Some(tls_config) => {
                connector.enforce_http(false);
                let connector = hyper_rustls::HttpsConnectorBuilder::new()
                    .with_tls_config((*tls_config).clone())
                    .https_or_http()
                    .enable_http1()
                    .wrap_connector(connector);
                ClickHouseClient::with_http_client(pool.build(connector))
            }
            None => ClickHouseClient::with_http_client(pool.build(connector)),
        };
        let mut client = client.with_url(addr);
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
    async fn publish(&mut self, rows: MappedSinkRows<'_>) -> PerRecordOutcome<SinkRecordPosition> {
        let mut outcome = PerRecordOutcome::with_capacity(rows.selected_rows.len());
        let specs = rows
            .target_columns
            .iter()
            .map(|name| {
                JsonColumnSpec::new(name, FieldNulls::Write)
                    .with_float32_encoding(Float32Encoding::WidenedF64)
            })
            .collect::<Vec<_>>();
        let columns = match JsonColumns::new(rows.batch, &specs, NestedNulls::Write) {
            Ok(columns) => columns,
            Err(error) => {
                outcome.fail(error.change_context(SinkPublishError::Publish { sink: CLICKHOUSE }));
                return outcome;
            }
        };
        let mut previous_row_bytes = 0;
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
            let mut lines = Vec::with_capacity(chunk_rows.len());
            for row in chunk_rows {
                let mut encoded = Vec::with_capacity(previous_row_bytes);
                if let Err(error) = columns.write_row(*row, &mut encoded) {
                    outcome
                        .fail(error.change_context(SinkPublishError::Publish { sink: CLICKHOUSE }));
                    return outcome;
                }
                previous_row_bytes = encoded.len();
                lines.push(
                    String::from_utf8(encoded).assured(
                        "Arrow strings are UTF-8 and the JSON writer adds only ASCII syntax",
                    ),
                );
            }
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
mod connection_tests;

#[cfg(test)]
mod tests {
    use std::sync::Arc as StdArc;

    use arrow_array::{
        Float32Array, Int64Array, ListArray, RecordBatch, StringArray, TimestampNanosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::*;
    use crate::connection_tests::Fixture;

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
            Field::new("ratio", DataType::Float32, true),
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
                StdArc::new(Float32Array::from(vec![Some(1.2), None])),
                StdArc::new(tags),
            ],
        )
        .expect("the mapped batch should build");
        let columns = [
            "id".to_string(),
            "name".to_string(),
            "at".to_string(),
            "ratio".to_string(),
            "tags".to_string(),
        ];

        let specs = columns
            .iter()
            .map(|name| {
                JsonColumnSpec::new(name, FieldNulls::Write)
                    .with_float32_encoding(Float32Encoding::WidenedF64)
            })
            .collect::<Vec<_>>();
        let mapped = JsonColumns::new(&batch, &specs, NestedNulls::Write)
            .assured("the test columns have supported Arrow types");

        let mut first = Vec::new();
        mapped
            .write_row(0, &mut first)
            .assured("the first test row is valid JSON");
        let mut second = Vec::new();
        mapped
            .write_row(1, &mut second)
            .assured("the second test row is valid JSON");

        assert_eq!(
            first,
            br#"{"id":7,"name":"first","at":"2023-11-14T22:13:20.123456789+00:00","ratio":1.2000000476837158,"tags":[1,2]}"#
        );
        assert_eq!(
            second,
            br#"{"id":null,"name":"second","at":null,"ratio":null,"tags":[]}"#
        );
    }

    #[tokio::test]
    async fn client_rejects_an_invalid_request_timeout() {
        let fixture = Fixture::start().await;
        let error = match ClickHouseSink::client_from_config(
            &client_config("http://127.0.0.1:8123", "later"),
            fixture.dns(),
        ) {
            Ok(_) => panic!("invalid ClickHouse timeout should fail client initialization"),
            Err(error) => error,
        };

        assert!(
            format!("{error:?}").contains("invalid ClickHouse timeout_ms 'later'"),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn client_parses_the_request_timeout() {
        let fixture = Fixture::start().await;
        let (_, request_timeout) = ClickHouseSink::client_from_config(
            &client_config("http://127.0.0.1:8123", "275"),
            fixture.dns(),
        )
        .expect("ClickHouse client config should be valid");

        assert_eq!(request_timeout, Some(Duration::from_millis(275)));
    }

    #[tokio::test]
    async fn configured_timeout_bounds_clickhouse_insert_completion() {
        let fixture = Fixture::start().await;
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
            ClickHouseSink::client_from_config(&client_config(addr, "30"), fixture.dns())
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

    #[tokio::test]
    async fn clickhouse_client_config_validates_tls_ca_file() {
        let fixture = Fixture::start().await;
        let error = match ClickHouseSink::client_from_config(
            &[
                ClientConfigEntry {
                    key: "addr".to_string(),
                    value: "https://127.0.0.1:8124".to_string(),
                },
                ClientConfigEntry {
                    key: "tls_ca_file".to_string(),
                    value: "/tmp/nervix-missing-clickhouse-ca.pem".to_string(),
                },
            ],
            fixture.dns(),
        ) {
            Ok(_) => panic!("missing ClickHouse TLS CA should fail"),
            Err(error) => error,
        };
        let error = format!("{error:?}");

        assert!(error.contains("TLS CA certificate"));
    }
}
