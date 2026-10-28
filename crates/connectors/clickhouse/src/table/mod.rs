//! ClickHouse emitter connector.
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

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "the host supplies a validated connector plan")
)]

use std::{ops::Range, time::Duration};

use ::clickhouse::{Client as ClickHouseClient, error::Error as ClickHouseError};
use bytes::Bytes;
use error_stack::{Report, ResultExt as _};
use hyper_util::{
    client::legacy::{Client as HyperClient, connect::HttpConnector},
    rt::{TokioExecutor as HyperTokioExecutor, TokioTimer as HyperTokioTimer},
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_columnar_json::{
    BytesEncoding, FieldNulls, Float32Encoding, JsonColumnSpec, JsonColumns, JsonWriteError,
    NestedNulls,
};
use nervix_connector::{
    ConfiguredDeadline, EmitterFinish, EmitterFinishResult, EmitterHost, EmitterLifecycle,
    EmitterOpenError, EmitterRecordPosition, EmitterStartResult, EmitterWriteError,
    MappedEmitterMember, MappedEmitterRows, MeasuredRequest, PerRecordOutcome,
    RejectedEmitterMember, RowEmitter, RowRequest, RowRequestLimits, RustlsClientConfigProvider,
    client_config_value, optional_client_config_value,
};
use nervix_dns::{DnsLookupError, DnsResolver};
use nervix_models::{
    ClickHouseTableName, ClientConfigEntry, ClientDeadline, ConnectorKind, EmitterBatchPolicy,
};
use nervix_primitives::time::Instant;
use tracing::{debug, trace};

const CLICKHOUSE: &str = "clickhouse";
/// What `MAX SIZE` measures on a ClickHouse write, which an oversized row's rejection names.
const MEASURED_REQUEST: &str = "ClickHouse JSONEachRow body";

/// What one ClickHouse emitter inserts through, from its typed emitter plan.
pub struct ClickHouseTableEmitterConfig {
    pub config: Vec<ClientConfigEntry>,
    pub table: ClickHouseTableName,
    /// The node resolver every connection of the client resolves the host of `addr` through.
    pub dns: DnsResolver,
    /// The emitter's `BATCH` limits, which bound the rows and the body bytes of every insert.
    pub batch: EmitterBatchPolicy,
}

/// The ClickHouse emitter, which encodes each mapped row as one `JSONEachRow` line.
pub struct ClickHouseTableEmitter {
    client: ClickHouseClient,
    request_timeout: Duration,
    table: ClickHouseTableName,
    limits: RowRequestLimits,
}

#[derive(Debug, thiserror::Error)]
enum ClickHouseWriteError {
    #[error("ClickHouse rejected an insert")]
    Response { name: Option<&'static str> },
    #[error("ClickHouse insert failed")]
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
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "formats the typed external ClickHouse transport cause")
    )]
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

    fn into_report(report: Report<Self>) -> Report<EmitterWriteError> {
        let error = report.current_context();
        let publish = EmitterWriteError::Publish {
            connector: CLICKHOUSE,
        };
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
        members: &[MappedEmitterMember],
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

impl ClickHouseTableEmitter {
    pub const KIND: nervix_models::EmitterEntityKind =
        nervix_models::EmitterEntityKind::ClickHouse(nervix_models::ClickHouseEmitterEntity::Table);

    pub fn new(
        config: ClickHouseTableEmitterConfig,
        _host: EmitterHost,
    ) -> EmitterStartResult<Self> {
        let ClickHouseTableEmitterConfig {
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
    /// Without TLS entries the client speaks plain HTTP. The validated plan requires the request
    /// timeout and pairs an HTTPS address with mounted trust material.
    pub fn client_from_config(
        config: &[ClientConfigEntry],
        dns: DnsResolver,
    ) -> EmitterStartResult<(ClickHouseClient, Duration)> {
        let schema = ConnectorKind::ClickHouse
            .client()
            .assured("the ClickHouse connector declares a configured client")
            .config;
        let effective = schema.effective_entries(config);
        let config = effective.as_slice();
        let invalid = || {
            Report::new(EmitterOpenError::InvalidConfiguration {
                connector: CLICKHOUSE,
            })
        };
        let addr = client_config_value(config, "addr", "ClickHouse")
            .map_err(|error| invalid().attach_printable(error.to_string()))?;
        let request_timeout = ConfiguredDeadline::timeout(config)
            .change_context(EmitterOpenError::InvalidConfiguration {
                connector: CLICKHOUSE,
            })?
            .ok_or_else(|| invalid().attach_printable("missing ClickHouse timeout_ms"))?
            .bound();
        let tls_config = RustlsClientConfigProvider::new(config)
            .build()
            .map_err(|error| invalid().attach_printable(error.to_string()))?;
        let tcp_keepalive = Self::connection_interval(config, "tcp_keepalive_ms")?;
        let pool_idle_timeout = Self::connection_interval(config, "pool_idle_timeout_ms")?;
        let mut connector = HttpConnector::new_with_resolver(dns);
        connector.set_keepalive(Some(tcp_keepalive));
        let mut pool = HyperClient::builder(HyperTokioExecutor::new());
        pool.pool_timer(HyperTokioTimer::new());
        pool.pool_idle_timeout(pool_idle_timeout);
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

    fn connection_interval(
        config: &[ClientConfigEntry],
        name: &'static str,
    ) -> EmitterStartResult<Duration> {
        let schema = ConnectorKind::ClickHouse
            .client()
            .assured("the ClickHouse connector declares a configured client")
            .config;
        let key = schema
            .key(name)
            .assured("the builder reads a declared ClickHouse connection interval");
        let value = optional_client_config_value(config, name)
            .assured("the effective configuration supplies both defaulted intervals");
        key.validate(value)
            .change_context(EmitterOpenError::InvalidConfiguration {
                connector: CLICKHOUSE,
            })?;
        Ok(ClientDeadline::parse(value)
            .assured("the declared interval validator checked positive schedulable milliseconds")
            .duration())
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
        table: &ClickHouseTableName,
        body: Bytes,
        request_timeout: Duration,
    ) -> error_stack::Result<(), ClickHouseWriteError> {
        let sql = format!(
            "INSERT INTO {} FORMAT JSONEachRow",
            Self::quoted_table(table)
        );
        let mut insert = client
            .insert_formatted_with(sql)
            .with_timeouts(Some(request_timeout), Some(request_timeout));
        insert
            .send(body)
            .await
            .map_err(ClickHouseWriteError::report)?;
        insert.end().await.map_err(ClickHouseWriteError::report)
    }

    fn quoted_table(table: &ClickHouseTableName) -> String {
        match table.as_str().split_once('.') {
            Some((database, table)) => format!("`{database}`.`{table}`"),
            None => format!("`{table}`"),
        }
    }
}

#[async_trait::async_trait]
impl EmitterLifecycle for ClickHouseTableEmitter {
    fn kind(&self) -> nervix_models::EmitterEntityKind {
        Self::KIND
    }

    /// Each insert is answered before the publish returns, so the client holds nothing.
    async fn finish(&mut self, _deadline: Instant) -> EmitterFinishResult {
        Ok(EmitterFinish::NothingHeld)
    }
}

#[async_trait::async_trait]
impl RowEmitter for ClickHouseTableEmitter {
    /// Writes the rows of every carrier in inserts of at most `MAX MESSAGES` rows whose body is at
    /// most `MAX SIZE` bytes.
    async fn publish(
        &mut self,
        rows: MappedEmitterRows<'_>,
    ) -> PerRecordOutcome<EmitterRecordPosition> {
        let members = rows.members();
        let mut outcome = PerRecordOutcome::with_capacity(members.len());
        let specs = Self::column_specs(rows.target_columns);
        let mut carriers = Vec::with_capacity(rows.carriers.len());
        for carrier in &rows.carriers {
            match JsonColumns::new(carrier.batch, &specs, NestedNulls::Write) {
                Ok(columns) => carriers.push(columns),
                Err(error) => {
                    outcome.fail(error.change_context(EmitterWriteError::Publish {
                        connector: CLICKHOUSE,
                    }));
                    return outcome;
                }
            }
        }
        let lines = match EncodedLines::encode(&carriers, &members) {
            Ok(lines) => lines,
            Err(error) => {
                outcome.fail(error.change_context(EmitterWriteError::Publish {
                    connector: CLICKHOUSE,
                }));
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
                &self.table,
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
                            &self.table,
                            lines.body(index..alone),
                            self.request_timeout,
                        )
                        .await;
                        match inserted {
                            Ok(()) => outcome.deliver(rows.position(member)),
                            Err(error) if error.current_context().is_record_error() => {
                                outcome.reject(RejectedEmitterMember::external(
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
                    outcome.reject(RejectedEmitterMember::external(
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod connection_tests;

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use arrow_array::{
        Array as _, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array,
        Float64Array, Int64Array, ListArray, RecordBatch, StringArray, TimestampNanosecondArray,
        UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use nervix_primitives::sync::StdArc;

    use super::*;
    use crate::table::connection_tests::Fixture;

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

        let specs = ClickHouseTableEmitter::column_specs(&columns);
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
        let specs = ClickHouseTableEmitter::column_specs(&["pair".to_string(), "raw".to_string()]);
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
    fn encodes_full_width_numbers_nonfinite_floats_and_nested_octets() {
        let mut nested =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::BinaryBuilder::new());
        nested.values().append_value(b"\x80");
        nested.values().append_null();
        nested.append(true);
        let data: Vec<(&str, ArrayRef)> = vec![
            ("flag", StdArc::new(BooleanArray::from(vec![true]))),
            ("largest", StdArc::new(UInt64Array::from(vec![u64::MAX]))),
            (
                "small_float",
                StdArc::new(Float32Array::from(vec![f32::NAN])),
            ),
            (
                "large_float",
                StdArc::new(Float64Array::from(vec![f64::INFINITY])),
            ),
            ("text", StdArc::new(StringArray::from(vec!["a\"\\\n"]))),
            ("at", StdArc::new(TimestampNanosecondArray::from(vec![0]))),
            ("octets", StdArc::new(nested.finish())),
        ];
        let names = data
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect::<Vec<_>>();
        let fields = data
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            StdArc::new(Schema::new(fields)),
            data.into_iter().map(|(_, array)| array).collect(),
        )
        .expect("the mapped batch should build");
        let specs = ClickHouseTableEmitter::column_specs(&names);
        let columns = JsonColumns::new(&batch, &specs, NestedNulls::Write)
            .expect("each declared ClickHouse type has a JSONEachRow encoding");
        let mut line = Vec::new();
        columns.write_row(0, &mut line).expect("the row encodes");
        assert_eq!(
            line,
            b"{\"flag\":true,\"largest\":18446744073709551615,\"small_float\":null,\"large_float\":null,\"text\":\"a\\\"\\\\\\n\",\"at\":\"1970-01-01T00:00:00+00:00\",\"octets\":[\"\x80\",null]}".to_vec()
        );
    }

    #[test]
    fn an_insert_body_is_exactly_the_lines_of_its_rows_and_measures_its_length() {
        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(Int64Array::from(vec![Some(1), Some(22), None]))],
        )
        .expect("the mapped batch should build");
        let specs = ClickHouseTableEmitter::column_specs(&["id".to_string()]);
        let mapped = [JsonColumns::new(&batch, &specs, NestedNulls::Write)
            .expect("the test column has a supported Arrow type")];
        let members = [
            MappedEmitterMember { carrier: 0, row: 2 },
            MappedEmitterMember { carrier: 0, row: 0 },
            MappedEmitterMember { carrier: 0, row: 1 },
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
        for value in ["later", "0"] {
            let error = match ClickHouseTableEmitter::client_from_config(
                &client_config("http://127.0.0.1:8123", value),
                fixture.dns(),
            ) {
                Ok(_) => panic!("invalid ClickHouse timeout should fail client initialization"),
                Err(error) => error,
            };

            assert!(
                error.frames().any(|frame| matches!(
                    frame.downcast_ref::<nervix_connector::ClientConfigError>(),
                    Some(nervix_connector::ClientConfigError::InvalidDeadline {
                        key: "timeout_ms"
                    })
                )),
                "{value}: unexpected error: {error:?}"
            );
        }
    }

    #[nervix_primitives::test]
    async fn client_parses_the_request_timeout() {
        let fixture = Fixture::start().await;
        let (_, request_timeout) = ClickHouseTableEmitter::client_from_config(
            &client_config("http://127.0.0.1:8123", "275"),
            fixture.dns(),
        )
        .expect("ClickHouse client config should be valid");

        assert_eq!(request_timeout, Duration::from_millis(275));
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
            ClickHouseTableEmitter::client_from_config(&client_config(addr, "30"), fixture.dns())
                .expect("ClickHouse client config should be valid");

        let result = nervix_primitives::time::timeout(
            Duration::from_millis(250),
            ClickHouseTableEmitter::insert(
                &client,
                &ClickHouseTableName::parse("events").expect("valid table name"),
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
        assert_eq!(
            format!("{result:#}"),
            format!("ClickHouse insert failed: {}", ClickHouseError::TimedOut)
        );
    }

    #[nervix_primitives::test]
    async fn clickhouse_client_config_validates_tls_ca_file() {
        let fixture = Fixture::start().await;
        let error = match ClickHouseTableEmitter::client_from_config(
            &[
                ClientConfigEntry {
                    key: "addr".to_string(),
                    value: "https://127.0.0.1:8124".to_string(),
                },
                ClientConfigEntry {
                    key: "timeout_ms".to_string(),
                    value: "5000".to_string(),
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

    #[test]
    fn quotes_each_case_preserving_table_part_for_insert_sql() {
        for (name, quoted) in [
            ("Orders", "`Orders`"),
            ("Sales.Order-Items", "`Sales`.`Order-Items`"),
        ] {
            let table = ClickHouseTableName::parse(name).expect("valid ClickHouse table name");
            assert_eq!(ClickHouseTableEmitter::quoted_table(&table), quoted);
        }
    }
}
