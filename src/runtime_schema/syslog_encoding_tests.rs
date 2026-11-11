//! Qualification of the fixed SYSLOG codec's RFC 5424 encoding.
//!
//! Layer: test harness.
//! - **Owns.** Specification examples, byte fidelity, borrowed-header allocation and write-failure
//!   evidence for single-record messages and the JSON message of a batch frame.
//! - **Depends on.** The production codec and its bounded encoders, Arrow, and an independent
//!   RFC 5424 parser used only as a test oracle.
//! - **Must not know.** Connector transport, emitter scheduling or acknowledgement state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{collections::BTreeMap, io, num::NonZeroU64};

use arrow_array::{ArrayRef, RecordBatch};
use nervix_arbitrary::Entropy;
use nervix_models::{
    ByteSizeUnit, CreateSchema, PayloadSizeLimit, ResolvedCodecWireFormat, SchemaField,
};
use nervix_primitives::sync::{Arc, StdArc};
use rstest::rstest;

use super::*;
use crate::runtime_schema::{
    BatchMember, BatchMemberEncoding, BoundedBatchEncoding, BoundedRowEncoding, JsonDecoder,
    RuntimeRecordBatch, compile_codec_spec_with_protobuf, compile_schema, decode_with_codec,
};

#[path = "syslog_encoding_properties.rs"]
mod properties;

const CASE_BYTES: usize = 4096;
const CASES: usize = 256;
const AMPLE_LIMIT: u64 = 1 << 20;

/// The complete logical value of a record, independent of its Arrow representation.
#[derive(Clone, Debug)]
struct Record {
    facility: u8,
    severity: u8,
    timestamp: Option<i64>,
    hostname: Option<String>,
    app_name: Option<String>,
    proc_id: Option<String>,
    msg_id: Option<String>,
    structured_data: Option<Vec<Element>>,
    message: String,
}

#[derive(Clone, Debug)]
struct Element {
    id: String,
    parameters: Vec<Parameter>,
}

#[derive(Clone, Debug)]
struct Parameter {
    name: String,
    value: String,
}

impl Element {
    /// Fixture syntax from RFC 5424 section 6.3; production never calls this writer.
    fn text(&self) -> String {
        let mut text = format!("[{}", self.id);
        for parameter in &self.parameters {
            let value = parameter
                .value
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace(']', "\\]");
            text.push_str(&format!(" {}=\"{value}\"", parameter.name));
        }
        text.push(']');
        text
    }
}

impl Record {
    fn minimal() -> Self {
        Self {
            facility: 0,
            severity: 0,
            timestamp: None,
            hostname: None,
            app_name: None,
            proc_id: None,
            msg_id: None,
            structured_data: None,
            message: String::new(),
        }
    }

    fn structured_text(&self) -> Option<String> {
        let Some(elements) = &self.structured_data else {
            return None;
        };
        let mut text = String::new();
        for element in elements {
            text.push_str(&element.text());
        }
        Some(text)
    }

    /// The canonical record after the codec's documented microsecond truncation.
    fn canonical(&self) -> Option<Self> {
        let mut record = self.clone();
        record.message = self.bom_free_message().to_string();
        if let Some(timestamp) = record.timestamp {
            let timestamp = i128::from(timestamp)
                .div_euclid(1000)
                .checked_mul(1000)
                .assured("an i64 timestamp rounded in i128 cannot overflow");
            let Ok(timestamp) = i64::try_from(timestamp) else {
                return None;
            };
            record.timestamp = Some(timestamp);
        }
        Some(record)
    }

    /// Exact wire bytes use an independent timestamp renderer, not the production formatter.
    fn prefix(&self) -> String {
        let facility = u16::from(self.facility)
            .checked_mul(8)
            .assured("a valid syslog facility is at most 23");
        let priority = facility
            .checked_add(u16::from(self.severity))
            .assured("a valid syslog priority is at most 191");
        let timestamp = match self.timestamp {
            Some(timestamp) => {
                let timestamp = i128::from(timestamp)
                    .div_euclid(1000)
                    .checked_mul(1000)
                    .assured("an i64 timestamp rounded in i128 cannot overflow");
                jiff::Timestamp::from_nanosecond(timestamp)
                    .assured("the generated Arrow timestamp lies within Jiff's range")
                    .to_string()
            }
            None => "-".to_string(),
        };
        let data = self.structured_text();
        format!(
            "<{priority}>1 {timestamp} {} {} {} {} {} ",
            self.hostname.as_deref().unwrap_or("-"),
            self.app_name.as_deref().unwrap_or("-"),
            self.proc_id.as_deref().unwrap_or("-"),
            self.msg_id.as_deref().unwrap_or("-"),
            data.as_deref().unwrap_or("-"),
        )
    }

    fn expected(&self) -> Vec<u8> {
        let mut text = self.prefix();
        text.push_str(self.bom_free_message());
        text.into_bytes()
    }

    fn bom_free_message(&self) -> &str {
        self.message
            .strip_prefix('\u{feff}')
            .unwrap_or(&self.message)
    }

    fn draw(entropy: &mut Entropy<'_>) -> Self {
        let facility = entropy.byte() % 24;
        let severity = entropy.byte() % 8;
        let timestamp = if entropy.flag() {
            Some(entropy.any_i64())
        } else {
            None
        };
        let hostname = Self::header(entropy, 255);
        let app_name = Self::header(entropy, 48);
        let proc_id = Self::header(entropy, 128);
        let msg_id = Self::header(entropy, 32);
        let structured_data = if entropy.flag() {
            let count = entropy
                .count(2)
                .checked_add(1)
                .assured("at most three elements are drawn");
            let mut elements = Vec::new();
            for index in 0..count {
                let id = match index {
                    0 => "z@32473".to_string(),
                    1 => "a".repeat(32),
                    _ => "m@32473".to_string(),
                };
                let count = entropy.count(3);
                let mut parameters = Vec::new();
                for parameter in 0..count {
                    let name = match parameter {
                        0 => "z".to_string(),
                        1 => "a".repeat(32),
                        _ => "m".to_string(),
                    };
                    parameters.push(Parameter {
                        name,
                        value: Self::text(entropy),
                    });
                }
                elements.push(Element { id, parameters });
            }
            Some(elements)
        } else {
            None
        };
        let mut message = String::new();
        if entropy.flag() {
            message.push('\u{feff}');
        }
        message.push_str("message ");
        message.push_str(&Self::text(entropy));
        // The decoder trims only trailing CR/LF/NUL; this domain preserves every message byte.
        message.push_str(" end");
        Self {
            facility,
            severity,
            timestamp,
            hostname,
            app_name,
            proc_id,
            msg_id,
            structured_data,
            message,
        }
    }

    fn header(entropy: &mut Entropy<'_>, maximum: usize) -> Option<String> {
        if !entropy.flag() {
            return None;
        }
        let length = entropy.pick([
            1,
            maximum,
            maximum
                .checked_sub(1)
                .assured("all header limits exceed one"),
            12,
        ]);
        let mut text = "h".to_string();
        for _ in 1..length {
            let byte = (entropy.byte() % 94)
                .checked_add(b'!')
                .assured("the printable non-space ASCII interval ends at 126");
            text.push(char::from(byte));
        }
        Some(text)
    }

    fn text(entropy: &mut Entropy<'_>) -> String {
        let count = entropy.count(8);
        let mut text = String::new();
        for _ in 0..count {
            let token = entropy.pick([
                "",
                "plain",
                "\"",
                "\\",
                "]",
                " \t",
                "\u{e9}",
                "\u{1f600}",
                "\u{feff}",
            ]);
            text.push_str(token);
        }
        text
    }

    fn assert_reference(&self, bytes: &[u8]) {
        use syslog_rfc5424::{SyslogFacility, SyslogMessage as ReferenceMessage, SyslogSeverity};

        let text = std::str::from_utf8(bytes).assured("the writer emits only UTF-8");
        let parsed = text
            .parse::<ReferenceMessage>()
            .assured("the independent parser accepts the complete RFC 5424 message");
        assert_eq!(parsed.version, 1);
        assert_eq!(
            parsed.facility,
            SyslogFacility::try_from(i32::from(self.facility))
                .assured("the generated facility is valid")
        );
        assert_eq!(
            parsed.severity,
            SyslogSeverity::try_from(i32::from(self.severity))
                .assured("the generated severity is valid")
        );
        let timestamp = self.timestamp.map(DateTime::from_timestamp_nanos);
        assert_eq!(
            parsed.timestamp,
            timestamp.map(|timestamp| timestamp.timestamp())
        );
        assert_eq!(
            parsed.timestamp_nanos,
            timestamp.map(|timestamp| {
                timestamp
                    .timestamp_subsec_micros()
                    .checked_mul(1000)
                    .assured(
                        "microseconds below one million scale to fewer than a billion nanoseconds",
                    )
            })
        );
        assert_eq!(parsed.hostname, self.hostname);
        assert_eq!(parsed.appname, self.app_name);
        let proc_id = match parsed.procid {
            Some(syslog_rfc5424::message::ProcId::PID(value)) => Some(value.to_string()),
            Some(syslog_rfc5424::message::ProcId::Name(value)) => Some(value),
            None => None,
        };
        assert_eq!(proc_id, self.proc_id);
        assert_eq!(parsed.msgid, self.msg_id);
        let mut elements = BTreeMap::new();
        if let Some(data) = &self.structured_data {
            for element in data {
                let mut parameters = BTreeMap::new();
                for parameter in &element.parameters {
                    parameters.insert(parameter.name.clone(), parameter.value.clone());
                }
                elements.insert(element.id.clone(), parameters);
            }
        }
        assert_eq!(*parsed.sd, elements);
        assert_eq!(parsed.msg, self.bom_free_message());
    }
}

struct CodecFixture {
    codec: Arc<CompiledCodec>,
}

impl CodecFixture {
    fn new() -> Self {
        let schema = CreateSchema {
            name: "syslog_event"
                .parse()
                .assured("the literal is a schema name"),
            fields: vec![
                Self::field("facility", ParseAsType::U8, false),
                Self::field("severity", ParseAsType::U8, false),
                Self::field("timestamp", ParseAsType::Datetime, true),
                Self::field("hostname", ParseAsType::String, true),
                Self::field("app_name", ParseAsType::String, true),
                Self::field("proc_id", ParseAsType::String, true),
                Self::field("msg_id", ParseAsType::String, true),
                Self::field("structured_data", ParseAsType::String, true),
                Self::field("message", ParseAsType::String, false),
            ],
        };
        let schema = Arc::new(compile_schema(&schema));
        let codec = compile_codec_spec_with_protobuf(
            &"syslog_codec"
                .parse()
                .assured("the literal is a codec name"),
            &[],
            schema,
            ResolvedCodecWireFormat::Syslog,
            None,
        )
        .assured("the fixed SYSLOG schema compiles");
        Self { codec }
    }

    fn field(name: &str, ty: ParseAsType, optional: bool) -> SchemaField {
        SchemaField {
            name: name.parse().assured("the literal is a field name"),
            ty,
            optional,
            sensitive: false,
        }
    }

    fn batch(&self, records: &[Record]) -> RuntimeRecordBatch {
        let data: Vec<_> = records.iter().map(Record::structured_text).collect();
        // Arrow's external array interface requires standard shared ownership.
        let columns: Vec<ArrayRef> = vec![
            StdArc::new(UInt8Array::from_iter_values(
                records.iter().map(|record| record.facility),
            )),
            StdArc::new(UInt8Array::from_iter_values(
                records.iter().map(|record| record.severity),
            )),
            StdArc::new(
                TimestampNanosecondArray::from_iter(records.iter().map(|record| record.timestamp))
                    .with_timezone("+00:00"),
            ),
            StdArc::new(StringArray::from_iter(
                records.iter().map(|record| record.hostname.as_deref()),
            )),
            StdArc::new(StringArray::from_iter(
                records.iter().map(|record| record.app_name.as_deref()),
            )),
            StdArc::new(StringArray::from_iter(
                records.iter().map(|record| record.proc_id.as_deref()),
            )),
            StdArc::new(StringArray::from_iter(
                records.iter().map(|record| record.msg_id.as_deref()),
            )),
            StdArc::new(StringArray::from_iter(data.iter().map(Option::as_deref))),
            StdArc::new(StringArray::from_iter_values(
                records.iter().map(|record| record.message.as_str()),
            )),
        ];
        let schema = self.codec.schema.arrow_schema();
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .assured("the nine columns follow the fixed SYSLOG schema");
        RuntimeRecordBatch::from_record_batch(schema, batch)
            .assured("the Arrow batch has the codec's exact schema")
    }

    fn decode(&self, bytes: &[u8]) -> error_stack::Result<RuntimeRecordBatch, CodecError> {
        let mut builder = self.codec.schema.batch_builder(1);
        decode_with_codec(
            &self.codec,
            bytes,
            &mut JsonDecoder::default(),
            &mut builder,
            usize::try_from(nervix_models::PayloadUnfoldLimit::DEFAULT)
                .assured("the current default fits usize"),
        )?;
        builder.finish().change_context(CodecError::SyslogDecode {
            codec: "syslog_codec".to_string(),
        })
    }

    fn encode(&self, batch: &RuntimeRecordBatch, row: usize) -> Vec<u8> {
        let encoder = self
            .codec
            .batch_encoder(batch)
            .assured("the fixture matches the codec");
        let mut bytes = Vec::new();
        encoder
            .encode_row_into(row, &mut bytes)
            .assured("the fixture record is valid");
        bytes
    }

    fn members(&self, batch: &RuntimeRecordBatch) -> Vec<BatchMember> {
        let encoder = self
            .codec
            .batch_encoder(batch)
            .assured("the fixture matches the codec");
        let mut members = Vec::new();
        for row in 0..batch.batch().num_rows() {
            let member = encoder
                .batch_member(row, limit(AMPLE_LIMIT))
                .assured("the fixture record is valid");
            let BatchMemberEncoding::Member(member) = member else {
                panic!("the bounded fixture fits one MiB");
            };
            members.push(member);
        }
        members
    }
}

fn limit(bytes: u64) -> PayloadSizeLimit {
    PayloadSizeLimit::new(
        NonZeroU64::new(bytes).assured("the test supplies a positive byte limit"),
        ByteSizeUnit::B,
    )
    .assured("a byte count fits u64")
}

#[rstest]
#[case(b"<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 - 'su root' failed for lonvick on /dev/pts/8")]
#[case(b"<165>1 2003-08-24T12:14:15.000003Z 192.0.2.1 myproc 8710 - - %% It's time to make the do-nuts.")]
#[case(br#"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] An application event log entry..."#)]
#[case(br#"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473 class="high"] "#)]
fn specification_examples_reencode_without_changing_a_byte(#[case] expected: &[u8]) {
    let fixture = CodecFixture::new();
    let batch = fixture
        .decode(expected)
        .assured("the specification fixture is valid");
    assert_eq!(fixture.encode(&batch, 0), expected);
    let reference = std::str::from_utf8(expected)
        .assured("the examples are UTF-8")
        .parse::<syslog_rfc5424::SyslogMessage>()
        .assured("the independent parser accepts the RFC examples");
    assert_eq!(reference.version, 1);
}

#[test]
fn nil_fields_and_an_empty_message_keep_their_exact_spaces() {
    let fixture = CodecFixture::new();
    let record = Record::minimal();
    let batch = fixture.batch(std::slice::from_ref(&record));
    let bytes = fixture.encode(&batch, 0);
    assert_eq!(bytes, b"<0>1 - - - - - - ");
    record.assert_reference(&bytes);
    let members = fixture.members(&batch);
    let frame = fixture
        .codec
        .encode_batch_within(&[&members[0]], limit(AMPLE_LIMIT))
        .assured("the singleton frame is valid");
    let BoundedBatchEncoding::Encoded(frame) = frame else {
        panic!("the singleton frame fits one MiB");
    };
    assert_eq!(frame, br#"<0>1 - - - - - - ["<0>1 - - - - - - "]"#);
    let empty = fixture
        .codec
        .encode_batch_within(&[], limit(1))
        .assured("an empty frame needs no output");
    assert!(matches!(empty, BoundedBatchEncoding::Encoded(bytes) if bytes.is_empty()));
}

#[rstest]
#[case(0, "1970-01-01T00:00:00Z")]
#[case(999, "1970-01-01T00:00:00Z")]
#[case(1000, "1970-01-01T00:00:00.000001Z")]
#[case(100_000_000, "1970-01-01T00:00:00.1Z")]
#[case(123_456_789, "1970-01-01T00:00:00.123456Z")]
#[case(-1, "1969-12-31T23:59:59.999999Z")]
#[case(i64::MIN, "1677-09-21T00:12:43.145224Z")]
#[case(i64::MAX, "2262-04-11T23:47:16.854775Z")]
fn timestamp_precision_and_arrow_extremes_keep_their_exact_spelling(
    #[case] timestamp: i64,
    #[case] spelling: &str,
) {
    let fixture = CodecFixture::new();
    let mut record = Record::minimal();
    record.timestamp = Some(timestamp);
    let batch = fixture.batch(std::slice::from_ref(&record));
    let encoded = fixture.encode(&batch, 0);
    assert_eq!(encoded, format!("<0>1 {spelling} - - - - - ").as_bytes());
    record.assert_reference(&encoded);
}

#[test]
fn frame_members_compare_every_header_field_and_its_presence() {
    let fixture = CodecFixture::new();
    let first = Record::minimal();
    let batch = fixture.batch(std::slice::from_ref(&first));
    let members = fixture.members(&batch);
    for field in 0..8 {
        let mut record = first.clone();
        match field {
            0 => record.facility = 1,
            1 => record.severity = 1,
            2 => record.hostname = Some("host".to_string()),
            3 => record.app_name = Some("app".to_string()),
            4 => record.proc_id = Some("worker".to_string()),
            5 => record.msg_id = Some("event".to_string()),
            6 => {
                record.structured_data = Some(vec![Element {
                    id: "id@1".to_string(),
                    parameters: Vec::new(),
                }])
            }
            _ => record.hostname = Some("-".to_string()),
        }
        let changed = fixture.batch(&[record]);
        let changed = fixture.members(&changed);
        assert!(
            !members[0].shares_container_with(&changed[0]),
            "header field {field} must split the run"
        );
    }
    let mut same_header = first.clone();
    same_header.timestamp = Some(123_456_789);
    same_header.message = "another message".to_string();
    let same_header = fixture.batch(&[same_header]);
    let same_header = fixture.members(&same_header);
    assert!(members[0].shares_container_with(&same_header[0]));
}

#[test]
fn prepared_single_message_writing_allocates_nothing_and_batch_writing_keeps_one_member_vector() {
    let fixture = CodecFixture::new();
    let mut record = Record::minimal();
    record.hostname = Some("h".repeat(255));
    record.app_name = Some("a".repeat(48));
    record.proc_id = Some("p".repeat(128));
    record.msg_id = Some("i".repeat(32));
    record.message = "borrowed message".to_string();
    let batch = fixture.batch(std::slice::from_ref(&record));
    let row = ArrowCodecRow {
        codec: &fixture.codec,
        batch: &batch,
        row_index: 0,
    };
    let message = SyslogMessage::from_row(&row).assured("the longest headers are valid");
    let member = SyslogBatchMember::from_row(&row).assured("the longest headers are valid");
    let mut storage = [0_u8; 4096];
    let mut output = io::Cursor::new(storage.as_mut_slice());
    let (single, result) = alloc_count::alloc_count!({ message.write(&row, &mut output) });
    result.assured("the complete message fits the stack buffer");
    assert_eq!((single.alloc_calls, single.realloc_calls), (0, 0));
    output.set_position(0);
    let (batch, result) =
        alloc_count::alloc_count!({ SyslogBatchMember::write_frame(&[&member], &mut output) });
    result.assured("the complete batch frame fits the stack buffer");
    assert_eq!((batch.alloc_calls, batch.realloc_calls), (1, 0));
}

#[derive(Debug, thiserror::Error)]
#[error("the test writer refused its next byte")]
struct WriteFailure;

struct FailingWriter {
    remaining: usize,
    bytes: Vec<u8>,
}

impl io::Write for FailingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::other(WriteFailure));
        }
        let count = self.remaining.min(bytes.len());
        self.bytes.extend_from_slice(&bytes[..count]);
        self.remaining = self
            .remaining
            .checked_sub(count)
            .verified("a successful write consumes at most the remaining budget");
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn partial_header_and_message_writes_keep_the_original_io_cause() {
    let fixture = CodecFixture::new();
    let mut record = Record::minimal();
    record.message = "message after header".to_string();
    let batch = fixture.batch(std::slice::from_ref(&record));
    let row = ArrowCodecRow {
        codec: &fixture.codec,
        batch: &batch,
        row_index: 0,
    };
    let message = SyslogMessage::from_row(&row).assured("the record is valid");
    let expected = record.expected();
    for budget in 0..expected.len() {
        let mut writer = FailingWriter {
            remaining: budget,
            bytes: Vec::new(),
        };
        let error = message
            .write(&row, &mut writer)
            .expect_err("the writer refuses a byte");
        assert!(
            matches!(error.current_context(), CodecError::SyslogEncode { codec } if codec == "syslog_codec")
        );
        let source = error
            .downcast_ref::<io::Error>()
            .assured("the report retains the writer error");
        assert!(
            source
                .get_ref()
                .is_some_and(|cause| cause.is::<WriteFailure>())
        );
        assert_eq!(writer.bytes, expected[..budget]);
    }
    let member = SyslogBatchMember::from_row(&row).assured("the record is valid");
    let mut frame = Vec::new();
    SyslogBatchMember::write_frame(&[&member], &mut frame).assured("the frame is valid");
    for budget in 0..frame.len() {
        let mut writer = FailingWriter {
            remaining: budget,
            bytes: Vec::new(),
        };
        let error = SyslogBatchMember::write_frame(&[&member], &mut writer)
            .expect_err("the writer refuses a byte");
        assert!(
            error
                .get_ref()
                .is_some_and(|cause| cause.is::<WriteFailure>())
        );
        assert_eq!(writer.bytes, frame[..budget]);
    }
}
