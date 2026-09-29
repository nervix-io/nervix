//! ClickHouse sink connector.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** ClickHouse client and TLS configuration, the `JSONEachRow` encoding of each mapped
//!   row, the exact body every insert carries under the emitter's `BATCH` limits, re-executing a
//!   rejected insert one row at a time, and insert-error classification.
//! - **Depends on.** The connector contract, vocabulary values, Arrow arrays, `error-stack`, Tokio
//!   and the `clickhouse` driver.
//! - **Must not know.** Runtime batches, relays, branches, schedules, registry state, or another
//!   connector implementation.

#[cfg(feature = "shuttle")]
extern crate shuttle_tokio as tokio;

use std::{io::Write as _, ops::Range, time::Duration};

use ::clickhouse::{Client as ClickHouseClient, error::Error as ClickHouseError};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use bytes::Bytes;
use chrono::DateTime;
use error_stack::Report;
use hyper_util::{
    client::legacy::{Client as HyperClient, connect::HttpConnector},
    rt::TokioExecutor as HyperTokioExecutor,
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    MappedSinkMember, MappedSinkRows, MeasuredRequest, PerRecordOutcome, RejectedSinkRecord,
    RowRequest, RowRequestLimits, RowSink, RustlsClientConfigSource, SinkHost, SinkLifecycle,
    SinkPublishError, SinkRecordPosition, SinkStartError, SinkStartResult, client_config_value,
    optional_client_config_value,
};
use nervix_models::{ClientConfigEntry, EmitterBatchPolicy, TableName};
use tracing::{debug, trace};

const CLICKHOUSE: &str = "clickhouse";

/// What `MAX SIZE` measures on a ClickHouse write, which an oversized row's rejection names.
const MEASURED_REQUEST: &str = "ClickHouse JSONEachRow body";

/// The guarantee every write into an encoding buffer relies on.
const IN_MEMORY_JSON: &str = "serializing a number or a string into memory cannot fail";

/// What one ClickHouse emitter inserts through, from its typed sink plan.
pub struct ClickHouseSinkConfig {
    pub config: Vec<ClientConfigEntry>,
    pub table: TableName,
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

    /// Appends one `JSONEachRow` line to `out`, read column by column at the row the host
    /// selected, without its newline.
    fn write_line(&self, row: usize, out: &mut Vec<u8>) {
        out.push(b'{');
        for (index, field) in self.columns.iter().enumerate() {
            if index != 0 {
                out.push(b',');
            }
            out.extend_from_slice(field.key.as_bytes());
            out.push(b':');
            field.values.write_json(row, out);
        }
        out.push(b'}');
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
    fn encode(carriers: &[MappedJsonColumns<'_>], members: &[MappedSinkMember]) -> Self {
        let mut body = Vec::new();
        let mut ends = Vec::with_capacity(members.len());
        for member in members {
            let columns = carriers
                .get(member.carrier)
                .assured("every carrier of the write was mapped before its rows were encoded");
            columns.write_line(member.row, &mut body);
            body.push(b'\n');
            ends.push(body.len());
        }
        Self {
            body: Bytes::from(body),
            ends,
        }
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
    /// Octets a ClickHouse `String` column stores exactly as they are.
    Bytes(&'a BinaryArray),
    Datetime(&'a TimestampNanosecondArray),
    List {
        offsets: &'a ListArray,
        elements: Box<MappedJsonColumn<'a>>,
    },
    /// A fixed-size array, such as an array literal, whose rows all hold the same element count.
    FixedList {
        list: &'a FixedSizeListArray,
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
        if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
            return Some(Self::Bytes(values));
        }
        if let Some(values) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
            return Some(Self::Datetime(values));
        }
        if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
            let elements = Self::new(list.values())?;
            return Some(Self::FixedList {
                list,
                elements: Box::new(elements),
            });
        }
        let values = array.as_any().downcast_ref::<ListArray>()?;
        let elements = Self::new(values.values())?;
        Some(Self::List {
            offsets: values,
            elements: Box::new(elements),
        })
    }

    /// Appends the JSON value of one row to `out`.
    fn write_json(&self, row: usize, out: &mut Vec<u8>) {
        if self.is_null(row) {
            out.extend_from_slice(b"null");
            return;
        }
        match self {
            Self::Bool(values) => {
                serde_json::to_writer(&mut *out, &values.value(row)).assured(IN_MEMORY_JSON)
            }
            Self::U8(values) => {
                serde_json::to_writer(&mut *out, &values.value(row)).assured(IN_MEMORY_JSON)
            }
            Self::I8(values) => {
                serde_json::to_writer(&mut *out, &values.value(row)).assured(IN_MEMORY_JSON)
            }
            Self::U16(values) => {
                serde_json::to_writer(&mut *out, &values.value(row)).assured(IN_MEMORY_JSON)
            }
            Self::I16(values) => {
                serde_json::to_writer(&mut *out, &values.value(row)).assured(IN_MEMORY_JSON)
            }
            Self::U32(values) => {
                serde_json::to_writer(&mut *out, &values.value(row)).assured(IN_MEMORY_JSON)
            }
            Self::I32(values) => {
                serde_json::to_writer(&mut *out, &values.value(row)).assured(IN_MEMORY_JSON)
            }
            Self::U64(values) => {
                serde_json::to_writer(&mut *out, &values.value(row)).assured(IN_MEMORY_JSON)
            }
            Self::I64(values) => {
                serde_json::to_writer(&mut *out, &values.value(row)).assured(IN_MEMORY_JSON)
            }
            Self::F32(values) => serde_json::to_writer(&mut *out, &f64::from(values.value(row)))
                .assured(IN_MEMORY_JSON),
            Self::F64(values) => {
                serde_json::to_writer(&mut *out, &values.value(row)).assured(IN_MEMORY_JSON)
            }
            Self::String(values) => {
                serde_json::to_writer(&mut *out, values.value(row)).assured(IN_MEMORY_JSON)
            }
            Self::Bytes(values) => write_json_octets(values.value(row), out),
            Self::Datetime(values) => {
                let text = DateTime::from_timestamp_nanos(values.value(row))
                    .fixed_offset()
                    .to_rfc3339();
                serde_json::to_writer(&mut *out, &text).assured(IN_MEMORY_JSON)
            }
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
                elements.write_json_array(start..end, out);
            }
            Self::FixedList { list, elements } => {
                let non_negative = "Arrow builds fixed-size list offsets and widths as \
                                    non-negative element counts";
                let start = usize::try_from(list.value_offset(row)).assured(non_negative);
                let width = usize::try_from(list.value_length()).assured(non_negative);
                let end = start
                    .checked_add(width)
                    .assured("a fixed-size list row ends inside its element array");
                elements.write_json_array(start..end, out);
            }
        }
    }

    /// Appends the elements `elements` of this column as one JSON array.
    fn write_json_array(&self, elements: Range<usize>, out: &mut Vec<u8>) {
        out.push(b'[');
        for element in elements.clone() {
            if element != elements.start {
                out.push(b',');
            }
            self.write_json(element, out);
        }
        out.push(b']');
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
            Self::Bytes(values) => values.is_null(row),
            Self::Datetime(values) => values.is_null(row),
            Self::List { offsets, .. } => offsets.is_null(row),
            Self::FixedList { list, .. } => list.is_null(row),
        }
    }
}

/// Appends `octets` as one JSON string whose characters ClickHouse reads back as exactly those
/// octets.
///
/// ClickHouse stores a `String` as octets and does not require the text of a `JSONEachRow` string
/// to be UTF-8, so every octet is written as it is except the quote, the backslash and the control
/// octets, which are escaped. A `\u00XX` escape of a control octet reads back as that one octet.
fn write_json_octets(octets: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    for octet in octets {
        match *octet {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            control @ 0x00..=0x1f => {
                write!(out, "\\u{control:04x}").assured("writing into memory cannot fail")
            }
            other => out.push(other),
        }
    }
    out.push(b'"');
}

impl ClickHouseSink {
    pub fn new(config: ClickHouseSinkConfig, _host: SinkHost) -> SinkStartResult<Self> {
        let ClickHouseSinkConfig {
            config,
            table,
            batch,
        } = config;
        let (client, request_timeout) = Self::client_from_config(&config)?;
        // ClickHouse streams an insert body without a limit of its own, so the emitter's limits
        // are the only ones an insert keeps to.
        Ok(Self {
            client,
            request_timeout,
            table,
            limits: RowRequestLimits::from(batch),
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

    /// One insert whose `JSONEachRow` body is `body`, newline-terminated lines of the rows it
    /// carries.
    async fn insert(
        client: &ClickHouseClient,
        table: &str,
        body: Bytes,
        request_timeout: Option<Duration>,
    ) -> Result<(), ClickHouseWriteError> {
        let sql = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let mut insert = client
            .insert_formatted_with(sql)
            .with_timeouts(request_timeout, request_timeout);
        insert.send(body).await.map_err(ClickHouseWriteError)?;
        insert.end().await.map_err(ClickHouseWriteError)
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
        let mut carriers = Vec::with_capacity(rows.carriers.len());
        for carrier in &rows.carriers {
            match MappedJsonColumns::new(carrier.batch, rows.target_columns) {
                Ok(columns) => carriers.push(columns),
                Err(error) => {
                    outcome.fail(
                        Report::new(SinkPublishError::Publish { sink: CLICKHOUSE })
                            .attach_printable(error.to_string()),
                    );
                    return outcome;
                }
            }
        }
        let lines = EncodedLines::encode(&carriers, &members);
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
            tokio::task::consume_budget().await;
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
                Err(error) if error.is_record_error() && written.len() > 1 => {
                    for index in written {
                        tokio::task::consume_budget().await;
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
                            Err(error) if error.is_record_error() => {
                                outcome.reject(RejectedSinkRecord::external(
                                    rows.position(member),
                                    rows.occurred_at(member),
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
                    let member = members[written.start];
                    outcome.reject(RejectedSinkRecord::external(
                        rows.position(member),
                        rows.occurred_at(member),
                        error.record_reason(),
                    ));
                }
                Err(error) => {
                    outcome.fail(error.into_report());
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
            line(&mapped, 0),
            r#"{"id":7,"name":"first","at":"2023-11-14T22:13:20.123456789+00:00","tags":[1,2]}"#
        );
        assert_eq!(
            line(&mapped, 1),
            r#"{"id":null,"name":"second","at":null,"tags":[]}"#
        );
    }

    fn line(mapped: &MappedJsonColumns<'_>, row: usize) -> String {
        let mut out = Vec::new();
        mapped.write_line(row, &mut out);
        String::from_utf8(out).expect("these test rows encode as UTF-8 text")
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
        let columns = ["pair".to_string(), "raw".to_string()];

        let mapped = MappedJsonColumns::new(&batch, &columns).expect("columns should be mapped");

        let mut first = Vec::new();
        mapped.write_line(0, &mut first);
        assert_eq!(
            first,
            b"{\"pair\":[1,10],\"raw\":\"\xff\\u0000\\\"\\\\\\u000a~\"}".to_vec()
        );
        assert_eq!(line(&mapped, 1), r#"{"pair":[2,20],"raw":null}"#);
    }

    #[test]
    fn an_insert_body_is_exactly_the_lines_of_its_rows_and_measures_its_length() {
        let schema = StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![StdArc::new(Int64Array::from(vec![Some(1), Some(22), None]))],
        )
        .expect("the mapped batch should build");
        let columns = ["id".to_string()];
        let mapped = [MappedJsonColumns::new(&batch, &columns).expect("columns should be mapped")];
        let members = [
            MappedSinkMember { carrier: 0, row: 2 },
            MappedSinkMember { carrier: 0, row: 0 },
            MappedSinkMember { carrier: 0, row: 1 },
        ];

        let lines = EncodedLines::encode(&mapped, &members);

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
