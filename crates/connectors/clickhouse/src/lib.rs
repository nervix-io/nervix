//! ClickHouse sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** ClickHouse client and TLS configuration, the HTTP connector every connection of the
//!   client is made through, the `JSONEachRow` encoding of each mapped row, the exact body every
//!   insert carries under the emitter's `BATCH` limits, re-executing a rejected insert one row at a
//!   time, and insert-error classification.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, the shared columnar
//!   JSON writer, `error-stack`, Tokio, the node resolver, and the `clickhouse` driver with its Hyper
//!   client.
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

use std::{ops::Range, time::Duration};

use ::clickhouse::{Client as ClickHouseClient, error::Error as ClickHouseError};
use bytes::Bytes;
use error_stack::Report;
use hyper_util::{
    client::legacy::{Client as HyperClient, connect::HttpConnector},
    rt::TokioExecutor as HyperTokioExecutor,
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_columnar_json::{
    BytesEncoding, FieldNulls, Float32Encoding, JsonColumnSpec, JsonColumns, JsonWriteError,
    NestedNulls,
};
use nervix_connector::{
    MappedSinkMember, MappedSinkRows, MeasuredRequest, PerRecordOutcome, RejectedSinkRecord,
    RowRequest, RowRequestLimits, RowSink, RustlsClientConfigSource, SinkHost, SinkLifecycle,
    SinkPublishError, SinkRecordPosition, SinkStartError, SinkStartResult, client_config_value,
    optional_client_config_value,
};
use nervix_dns::{DnsLookupError, DnsResolver};
use nervix_models::{ClientConfigEntry, EmitterBatchPolicy, TableName};
use tracing::{debug, trace};

const CLICKHOUSE: &str = "clickhouse";
/// What `MAX SIZE` measures on a ClickHouse write, which an oversized row's rejection names.
const MEASURED_REQUEST: &str = "ClickHouse JSONEachRow body";
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
    /// The emitter's `BATCH` limits, which bound the rows and the body bytes of every insert.
    pub batch: EmitterBatchPolicy,
}

/// The ClickHouse sink, which encodes each mapped row as one `JSONEachRow` line.
pub struct ClickHouseSink {
    client: ClickHouseClient,
    request_timeout: Option<Duration>,
    table: TableName,
    limits: RowRequestLimits,
}

#[derive(Debug, thiserror::Error)]
enum ClickHouseWriteError {
    #[error("ClickHouse rejected an insert")]
    Response { name: Option<&'static str> },
    #[error("ClickHouse insert failed: {0}")]
    Driver(#[source] ClickHouseError),
}

impl ClickHouseWriteError {
    fn report(error: ClickHouseError) -> Report<Self> {
        // ClickHouse may quote a rejected row in BadResponse. Keep only its safe classification.
        let failure = match error {
            ClickHouseError::BadResponse(response) => Self::Response {
                name: Self::response_error_name(&response),
            },
            error => Self::Driver(error),
        };
        if let Some(lookup) = failure.lookup_failure().cloned() {
            Report::new(lookup).change_context(failure)
        } else {
            Report::new(failure)
        }
    }

    fn response_error_name(response: &str) -> Option<&'static str> {
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

    fn record_error_name(&self) -> Option<&'static str> {
        match self {
            Self::Response { name } => *name,
            Self::Driver(_) => None,
        }
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
        let Self::Driver(ClickHouseError::Network(error)) = self else {
            return None;
        };
        DnsLookupError::find_in(error.as_ref())
    }

    /// A request that never reached ClickHouse, described by the error and every cause under it.
    ///
    /// Those causes describe the connection, never a row, and carry no credentials. A response from
    /// ClickHouse is not described this way, because its text can repeat values of the rows.
    fn transport_failure(&self) -> Option<String> {
        let Self::Driver(ClickHouseError::Network(error)) = self else {
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

    fn into_report(report: Report<Self>) -> Report<SinkPublishError> {
        let error = report.current_context();
        let publish = SinkPublishError::Publish { sink: CLICKHOUSE };
        if let Some(lookup) = error.lookup_failure() {
            let reason = format!("ClickHouse insert request failed: {lookup}");
            return report.change_context(publish).attach_printable(reason);
        }
        let reason = if let Some(name) = error.record_error_name() {
            format!("ClickHouse insert request failed with {name}")
        } else if let Some(transport) = error.transport_failure() {
            format!("ClickHouse insert request failed: {transport}")
        } else {
            "ClickHouse insert request failed".to_string()
        };
        report.change_context(publish).attach_printable(reason)
    }
}

/// The `JSONEachRow` lines of one write's rows, each followed by its newline, in the order the
/// write carries its rows.
///
/// Every line is encoded once. An insert's body is the lines of its rows exactly as they sit here,
/// so the size a candidate is measured at is the size of the body that is sent.
struct EncodedLines {
    body: Bytes,
    /// Where each row's line ends in `body`, its newline included.
    ends: Vec<usize>,
}

impl EncodedLines {
    fn encode(
        carriers: &[JsonColumns<'_>],
        members: &[MappedSinkMember],
    ) -> error_stack::Result<Self, JsonWriteError> {
        let mut body = Vec::new();
        let mut ends = Vec::with_capacity(members.len());
        for member in members {
            let columns = carriers
                .get(member.carrier)
                .assured("every carrier of the write was mapped before its rows were encoded");
            columns.write_row(member.row, &mut body)?;
            body.push(b'\n');
            ends.push(body.len());
        }
        Ok(Self {
            body: Bytes::from(body),
            ends,
        })
    }

    /// Where the lines of `members` sit in the body.
    fn span(&self, members: Range<usize>) -> Range<usize> {
        let start = match members.start.checked_sub(1) {
            Some(previous) => *self
                .ends
                .get(previous)
                .assured("a request starts at a row the write encoded"),
            None => 0,
        };
        let last = members
            .end
            .checked_sub(1)
            .assured("a request carries at least one row");
        let end = *self
            .ends
            .get(last)
            .assured("a request ends at a row the write encoded");
        start..end
    }

    /// The exact size of the body an insert of `members` sends.
    fn measure(&self, members: Range<usize>) -> u64 {
        let span = self.span(members);
        let length = span
            .end
            .checked_sub(span.start)
            .assured("a later line ends after an earlier one");
        u64::try_from(length)
            .assured("Nervix builds for 64-bit targets only, where u64 holds usize")
    }

    /// The body an insert of `members` sends, which shares this write's encoded bytes.
    fn body(&self, members: Range<usize>) -> Bytes {
        self.body.slice(self.span(members))
    }
}

impl ClickHouseSink {
    pub fn new(config: ClickHouseSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let ClickHouseSinkConfig {
            config,
            table,
            dns,
            batch,
        } = config;
        let (client, request_timeout) = Self::client_from_config(&config, dns)?;
        // ClickHouse streams an insert body without a limit of its own, so the emitter's limits
        // are the only ones an insert keeps to.
        Ok(Self {
            client,
            request_timeout,
            table,
            limits: RowRequestLimits::from(batch),
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

    /// How each mapped column is written into a `JSONEachRow` line: a null as `null`, an `F32`
    /// widened to the `F64` ClickHouse formats, and `BYTES` as the octets a `String` column stores.
    fn column_specs(target_columns: &[String]) -> Vec<JsonColumnSpec> {
        target_columns
            .iter()
            .map(|name| {
                JsonColumnSpec::new(name, FieldNulls::Write)
                    .with_float32_encoding(Float32Encoding::WidenedF64)
                    .with_bytes_encoding(BytesEncoding::Octets)
            })
            .collect()
    }

    /// One insert whose `JSONEachRow` body is `body`, newline-terminated lines of the rows it
    /// carries.
    async fn insert(
        client: &ClickHouseClient,
        table: &str,
        body: Bytes,
        request_timeout: Option<Duration>,
    ) -> error_stack::Result<(), ClickHouseWriteError> {
        let sql = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let mut insert = client
            .insert_formatted_with(sql)
            .with_timeouts(request_timeout, request_timeout);
        insert
            .send(body)
            .await
            .map_err(ClickHouseWriteError::report)?;
        insert.end().await.map_err(ClickHouseWriteError::report)
    }
}

#[async_trait::async_trait]
impl SinkLifecycle for ClickHouseSink {}

#[async_trait::async_trait]
impl RowSink for ClickHouseSink {
    /// Writes the rows of every carrier in inserts of at most `MAX MESSAGES` rows whose body is at
    /// most `MAX SIZE` bytes.
    async fn publish(&mut self, rows: MappedSinkRows<'_>) -> PerRecordOutcome<SinkRecordPosition> {
        let members = rows.members();
        let mut outcome = PerRecordOutcome::with_capacity(members.len());
        let specs = Self::column_specs(rows.target_columns);
        let mut carriers = Vec::with_capacity(rows.carriers.len());
        for carrier in &rows.carriers {
            match JsonColumns::new(carrier.batch, &specs, NestedNulls::Write) {
                Ok(columns) => carriers.push(columns),
                Err(error) => {
                    outcome
                        .fail(error.change_context(SinkPublishError::Publish { sink: CLICKHOUSE }));
                    return outcome;
                }
            }
        }
        let lines = match EncodedLines::encode(&carriers, &members) {
            Ok(lines) => lines,
            Err(error) => {
                outcome.fail(error.change_context(SinkPublishError::Publish { sink: CLICKHOUSE }));
                return outcome;
            }
        };
        let requests = self
            .limits
            .divide(members.len(), |candidate| MeasuredRequest {
                size: lines.measure(candidate),
                request: (),
            });
        if requests.subdivisions > 0 {
            debug!(
                table = self.table.as_str(),
                subdivisions = requests.subdivisions,
                "halved ClickHouse inserts whose body exceeded MAX SIZE"
            );
        }
        for request in requests.requests {
            nervix_primitives::task::consume_budget().await;
            let written = match request {
                RowRequest::Write {
                    members: written, ..
                } => written,
                RowRequest::Oversize { member, oversize } => {
                    let member = members[member];
                    outcome.reject(oversize.rejected(
                        rows.position(member),
                        rows.occurred_at(member),
                        MEASURED_REQUEST,
                    ));
                    continue;
                }
            };
            let inserted = Self::insert(
                &self.client,
                self.table.as_str(),
                lines.body(written.clone()),
                self.request_timeout,
            )
            .await;
            match inserted {
                Ok(()) => {
                    for index in written {
                        outcome.deliver(rows.position(members[index]));
                    }
                }
                // A record-specific failure of a multi-row insert is isolated by inserting each of
                // its rows alone, so healthy rows land and only the rejected ones follow the error
                // policy.
                Err(error) if error.current_context().is_record_error() && written.len() > 1 => {
                    for index in written {
                        nervix_primitives::task::consume_budget().await;
                        let member = members[index];
                        let alone = index
                            .checked_add(1)
                            .assured("a row of the write is followed by at most its end");
                        let inserted = Self::insert(
                            &self.client,
                            self.table.as_str(),
                            lines.body(index..alone),
                            self.request_timeout,
                        )
                        .await;
                        match inserted {
                            Ok(()) => outcome.deliver(rows.position(member)),
                            Err(error) if error.current_context().is_record_error() => {
                                outcome.reject(RejectedSinkRecord::external(
                                    rows.position(member),
                                    rows.occurred_at(member),
                                    error.current_context().record_reason(),
                                ));
                            }
                            Err(error) => {
                                outcome.fail(ClickHouseWriteError::into_report(error));
                                return outcome;
                            }
                        }
                    }
                }
                Err(error) if error.current_context().is_record_error() => {
                    let member = members[written.start];
                    outcome.reject(RejectedSinkRecord::external(
                        rows.position(member),
                        rows.occurred_at(member),
                        error.current_context().record_reason(),
                    ));
                }
                Err(error) => {
                    outcome.fail(ClickHouseWriteError::into_report(error));
                    return outcome;
                }
            }
        }
        trace!(
            table = self.table.as_str(),
            rows = members.len(),
            "emitter published clickhouse rows"
        );
        outcome
    }
}

#[cfg(test)]
mod connection_tests;

#[cfg(test)]
mod tests {
    use arrow_array::{
        Array as _, BinaryArray, FixedSizeListArray, Float32Array, Int64Array, ListArray,
        RecordBatch, StringArray, TimestampNanosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use nervix_primitives::sync::StdArc;

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
            let report = ClickHouseWriteError::report(ClickHouseError::BadResponse(format!(
                "Code: 1. DB::Exception: rejected ({name})"
            )));
            assert!(
                report.current_context().is_record_error(),
                "{name} should be a definitive record error"
            );
        }
        let oversized = ClickHouseWriteError::report(ClickHouseError::BadResponse(
            "413 Payload Too Large".to_string(),
        ));
        assert!(oversized.current_context().is_record_error());
        for name in [
            "NETWORK_ERROR",
            "TABLE_IS_DROPPED",
            "TIMEOUT_EXCEEDED",
            "TOO_MANY_REQUESTS",
        ] {
            let report = ClickHouseWriteError::report(ClickHouseError::BadResponse(format!(
                "Code: 1. DB::Exception: rejected ({name})"
            )));
            assert!(
                !report.current_context().is_record_error(),
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

        let specs = ClickHouseSink::column_specs(&columns);
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

    #[test]
    fn writes_bytes_as_their_own_octets_and_fixed_size_arrays_as_json_arrays() {
        let pairs = FixedSizeListArray::try_new(
            StdArc::new(Field::new("item", DataType::Int64, false)),
            2,
            StdArc::new(Int64Array::from(vec![1, 10, 2, 20])),
            None,
        )
        .expect("two rows of two elements build");
        let raw = BinaryArray::from(vec![Some(b"\xff\x00\"\\\n~".as_slice()), None]);
        let schema = StdArc::new(Schema::new(vec![
            Field::new("pair", pairs.data_type().clone(), true),
            Field::new("raw", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![StdArc::new(pairs), StdArc::new(raw)])
            .expect("the mapped batch should build");
        let specs = ClickHouseSink::column_specs(&["pair".to_string(), "raw".to_string()]);
        let mapped = JsonColumns::new(&batch, &specs, NestedNulls::Write)
            .expect("fixed-size arrays and bytes are supported");

        let mut first = Vec::new();
        mapped
            .write_row(0, &mut first)
            .expect("the first test row should encode");
        let mut second = Vec::new();
        mapped
            .write_row(1, &mut second)
            .expect("the second test row should encode");

        assert_eq!(
            first,
            b"{\"pair\":[1,10],\"raw\":\"\xff\\u0000\\\"\\\\\\u000a~\"}".to_vec()
        );
        assert_eq!(second, br#"{"pair":[2,20],"raw":null}"#.to_vec());
    }

    #[test]
    fn an_insert_body_is_exactly_the_lines_of_its_rows_and_measures_its_length() {
        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(Int64Array::from(vec![Some(1), Some(22), None]))],
        )
        .expect("the mapped batch should build");
        let specs = ClickHouseSink::column_specs(&["id".to_string()]);
        let mapped = [JsonColumns::new(&batch, &specs, NestedNulls::Write)
            .expect("the test column has a supported Arrow type")];
        let members = [
            MappedSinkMember { carrier: 0, row: 2 },
            MappedSinkMember { carrier: 0, row: 0 },
            MappedSinkMember { carrier: 0, row: 1 },
        ];

        let lines = EncodedLines::encode(&mapped, &members).expect("the test rows should encode");

        assert_eq!(
            lines.body(0..3).as_ref(),
            b"{\"id\":null}\n{\"id\":1}\n{\"id\":22}\n".as_slice()
        );
        assert_eq!(
            lines.body(1..3).as_ref(),
            b"{\"id\":1}\n{\"id\":22}\n".as_slice()
        );
        for range in [0..1, 0..3, 1..2, 1..3, 2..3] {
            assert_eq!(
                lines.measure(range.clone()),
                u64::try_from(lines.body(range).len()).expect("a test body fits u64")
            );
        }
    }

    #[nervix_primitives::test]
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

    #[nervix_primitives::test]
    async fn client_parses_the_request_timeout() {
        let fixture = Fixture::start().await;
        let (_, request_timeout) = ClickHouseSink::client_from_config(
            &client_config("http://127.0.0.1:8123", "275"),
            fixture.dns(),
        )
        .expect("ClickHouse client config should be valid");

        assert_eq!(request_timeout, Some(Duration::from_millis(275)));
    }

    #[nervix_primitives::test]
    async fn configured_timeout_bounds_clickhouse_insert_completion() {
        let fixture = Fixture::start().await;
        let listener = nervix_primitives::net::TcpListener::bind("127.0.0.1:0")
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

        let result = nervix_primitives::time::timeout(
            Duration::from_millis(250),
            ClickHouseSink::insert(
                &client,
                "events",
                Bytes::from_static(b"{\"id\":1}\n"),
                request_timeout,
            ),
        )
        .await
        .expect("configured ClickHouse timeout should bound the insert")
        .expect_err("the non-responsive endpoint should time out");

        assert!(
            matches!(
                result.current_context(),
                ClickHouseWriteError::Driver(ClickHouseError::TimedOut)
            ),
            "unexpected ClickHouse insert error: {result:?}"
        );
    }

    #[nervix_primitives::test]
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
