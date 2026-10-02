//! Syslog wire decoding and encoding.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Typed Arrow conversion for RFC 3164 and RFC 5424 syslog messages, and the RFC 5424
//!   frame one batch of records is published in.
//! - **Depends on.** Wire codec models, Arrow builders and UTC for omitted RFC 3164 years.
//! - **Must not know.** Domains, runtime clocks, schedules or connector lifecycle.

use std::fmt;

use ahash::HashSet;
use arrow_array::{
    Array, StringArray, TimestampNanosecondArray, UInt8Array,
    builder::{StringBuilder, TimestampNanosecondBuilder, UInt8Builder},
};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime, Utc};
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{CodecEncodingRule, CodecName, ParseAsType};
use thiserror::Error;

use super::{
    ArrowCodecRow, CodecContractError, CodecError, CompiledCodec, CompiledSchema, FieldEncodeError,
    RuntimeRecordBatchBuilder, RuntimeSchemaError, RuntimeValueLocation, SyslogHeaderIssue,
    SyslogStructuredDataIssue,
};

const DEFAULT_PRIORITY: u8 = 13;

/// Why a payload is not a syslog message the codec can decode. It is the cause beneath
/// [`CodecError::SyslogDecode`].
#[derive(Debug, Error)]
pub(crate) enum SyslogDecodeError {
    #[error("payload is empty after trailing delimiters")]
    Empty,
    #[error("payload is not valid UTF-8")]
    InvalidUtf8 {
        #[source]
        source: std::str::Utf8Error,
    },
    #[error("RFC 5424 VERSION must be 1")]
    Version,
    #[error("RFC 5424 header is missing {field}")]
    MissingHeaderField { field: SyslogHeaderField },
    #[error("RFC 5424 {field} is empty")]
    EmptyHeaderField { field: SyslogHeaderField },
    /// The runtime-schema failure beneath names the offending byte and the rule it breaks.
    #[error("invalid {field}")]
    InvalidHeaderField { field: SyslogHeaderField },
    /// The runtime-schema failure beneath names the offending byte and the rule it breaks.
    #[error("invalid STRUCTURED-DATA")]
    StructuredData,
    #[error("STRUCTURED-DATA must be followed by a space or end of message")]
    StructuredDataDelimiter,
    #[error("invalid RFC 5424 TIMESTAMP time offset")]
    TimestampOffset,
    #[error("invalid RFC 5424 TIMESTAMP date or time shape")]
    TimestampShape,
    #[error("invalid RFC 5424 TIMESTAMP fractional seconds")]
    TimestampFraction,
    #[error("invalid RFC 5424 TIMESTAMP")]
    Timestamp {
        #[source]
        source: chrono::ParseError,
    },
}

/// A header field of a syslog message, as a decoding failure names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
pub(crate) enum SyslogHeaderField {
    #[strum(serialize = "VERSION")]
    Version,
    #[strum(serialize = "TIMESTAMP")]
    Timestamp,
    #[strum(serialize = "HOSTNAME")]
    Hostname,
    #[strum(serialize = "APP-NAME")]
    AppName,
    #[strum(serialize = "PROCID")]
    ProcId,
    #[strum(serialize = "MSGID")]
    MsgId,
    #[strum(serialize = "RFC 3164 HOSTNAME")]
    Rfc3164Hostname,
    #[strum(serialize = "RFC 3164 TAG")]
    Rfc3164Tag,
}

/// Why a record cannot be written as a syslog message. It is the cause beneath
/// [`CodecError::SyslogEncode`].
#[derive(Debug, Error)]
pub(crate) enum SyslogEncodeError {
    #[error("SYSLOG encoding requires schema field '{field}'")]
    MissingSchemaField { field: String },
}

/// The type and optionality the fixed SYSLOG field contract gives a field, or a schema declares
/// for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SyslogFieldShape {
    ty: ParseAsType,
    optional: bool,
}

impl fmt::Display for SyslogFieldShape {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.ty)?;
        if self.optional {
            formatter.write_str(" OPTIONAL")?;
        }
        Ok(())
    }
}

struct ParsedSyslog<'a> {
    facility: u8,
    severity: u8,
    timestamp: Option<DateTime<FixedOffset>>,
    hostname: Option<&'a str>,
    app_name: Option<&'a str>,
    proc_id: Option<&'a str>,
    msg_id: Option<&'a str>,
    structured_data: Option<&'a str>,
    message: &'a str,
}

pub(super) fn validate_compiled_schema(
    codec: &CodecName,
    encoding_rules: &[CodecEncodingRule],
    schema: &CompiledSchema,
) -> error_stack::Result<(), CodecError> {
    if !encoding_rules.is_empty() {
        return Err(CodecError::contract_violation(
            codec.as_str(),
            CodecContractError::SyslogEncodingRules,
        ));
    }
    for field in &schema.fields {
        let expected = match field.name.as_str() {
            "facility" | "severity" => Some((ParseAsType::U8, false)),
            "timestamp" => Some((ParseAsType::Datetime, true)),
            "hostname" | "app_name" | "proc_id" | "msg_id" | "structured_data" => {
                Some((ParseAsType::String, true))
            }
            "message" => Some((ParseAsType::String, false)),
            _ => None,
        };
        let Some((expected_type, expected_optional)) = expected else {
            return Err(CodecError::contract_violation(
                codec.as_str(),
                CodecContractError::SyslogFieldOutsideContract {
                    field: field.name.clone(),
                },
            ));
        };
        if field.ty != expected_type || field.optional != expected_optional {
            return Err(CodecError::contract_violation(
                codec.as_str(),
                CodecContractError::SyslogFieldShape {
                    field: field.name.clone(),
                    expected: SyslogFieldShape {
                        ty: expected_type,
                        optional: expected_optional,
                    },
                    found: SyslogFieldShape {
                        ty: field.ty.clone(),
                        optional: field.optional,
                    },
                },
            ));
        }
    }
    Ok(())
}

pub(super) fn decode(
    codec: &CompiledCodec,
    payload: &[u8],
    builder: &mut RuntimeRecordBatchBuilder,
) -> error_stack::Result<(), CodecError> {
    let last_kept = payload
        .iter()
        .rposition(|byte| !matches!(byte, b'\r' | b'\n' | b'\0'));
    let end = match last_kept {
        Some(index) => index + 1,
        None => 0,
    };
    let payload = &payload[..end];
    if payload.is_empty() {
        return Err(decode_failure(codec, SyslogDecodeError::Empty));
    }
    let payload = std::str::from_utf8(payload)
        .map_err(|source| decode_failure(codec, SyslogDecodeError::InvalidUtf8 { source }))?;
    let split = split_priority(payload);
    let parsed = if split.has_priority && looks_like_rfc5424(split.body) {
        parse_rfc5424(codec, split.priority, split.body)?
    } else {
        parse_rfc3164(codec, split.priority, split.body)?
    };
    append_row(codec, &parsed, builder)
}

pub(super) fn encode_row(
    row: &ArrowCodecRow<'_>,
    output: &mut impl std::io::Write,
) -> error_stack::Result<(), CodecError> {
    SyslogMessage::from_row(row)?.write(row, output)
}

/// The RFC 5424 header fields a batch frame carries once for all of its members: every header
/// field except `TIMESTAMP`, which each member keeps inside its own message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SyslogFrameHeader {
    priority: u16,
    hostname: Option<String>,
    app_name: Option<String>,
    proc_id: Option<String>,
    msg_id: Option<String>,
    structured_data: Option<String>,
}

impl SyslogFrameHeader {
    /// Writes `<PRI>1 TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA` with the given
    /// timestamp, without the space that separates the header from `MSG`.
    fn write(&self, timestamp: &str, output: &mut impl std::io::Write) -> std::io::Result<()> {
        write!(
            output,
            "<{}>1 {timestamp} {} {} {} {} {}",
            self.priority,
            self.hostname.as_deref().unwrap_or("-"),
            self.app_name.as_deref().unwrap_or("-"),
            self.proc_id.as_deref().unwrap_or("-"),
            self.msg_id.as_deref().unwrap_or("-"),
            self.structured_data.as_deref().unwrap_or("-"),
        )
    }
}

/// One record as the RFC 5424 message it encodes to on its own, kept as a batch member.
#[derive(Debug, Clone)]
pub(super) struct SyslogBatchMember {
    header: SyslogFrameHeader,
    /// The member's own `TIMESTAMP`, or the nil value.
    timestamp: String,
    /// The member's complete RFC 5424 message.
    message: String,
}

impl SyslogBatchMember {
    pub(super) fn from_row(row: &ArrowCodecRow<'_>) -> error_stack::Result<Self, CodecError> {
        let message = SyslogMessage::from_row(row)?;
        let mut encoded = Vec::new();
        message.write(row, &mut encoded)?;
        let encoded = String::from_utf8(encoded)
            .assured("every part of an RFC 5424 message is written from UTF-8 text");
        Ok(Self {
            header: message.header.to_owned_header(),
            timestamp: message.timestamp,
            message: encoded,
        })
    }

    /// The exact length of the member's own message.
    pub(super) fn len(&self) -> usize {
        self.message.len()
    }

    /// Whether this member and `other` agree on every header field a frame carries once.
    pub(super) fn shares_frame_with(&self, other: &Self) -> bool {
        self.header == other.header
    }

    /// Writes one RFC 5424 message carrying `members`: their common header, the first member's
    /// timestamp, and a `MSG` that is the JSON array of the members' own messages.
    ///
    /// The members must share a frame header, which the packing that chose them guarantees.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the external writer interface encodes one admitted syslog frame"
        )
    )]
    pub(super) fn write_frame(
        members: &[&Self],
        output: &mut impl std::io::Write,
    ) -> std::io::Result<()> {
        let Some(first) = members.first() else {
            return Ok(());
        };
        first.header.write(&first.timestamp, output)?;
        output.write_all(b" ")?;
        let messages = members
            .iter()
            .map(|member| member.message.as_str())
            .collect::<Vec<_>>();
        serde_json::to_writer(output, &messages).map_err(std::io::Error::from)
    }
}

/// The header fields one record's RFC 5424 message carries, borrowed from its row.
struct SyslogHeaderFields<'a> {
    priority: u16,
    hostname: Option<&'a str>,
    app_name: Option<&'a str>,
    proc_id: Option<&'a str>,
    msg_id: Option<&'a str>,
    structured_data: Option<&'a str>,
}

impl SyslogHeaderFields<'_> {
    fn to_owned_header(&self) -> SyslogFrameHeader {
        SyslogFrameHeader {
            priority: self.priority,
            hostname: self.hostname.map(str::to_string),
            app_name: self.app_name.map(str::to_string),
            proc_id: self.proc_id.map(str::to_string),
            msg_id: self.msg_id.map(str::to_string),
            structured_data: self.structured_data.map(str::to_string),
        }
    }
}

/// One record's RFC 5424 message, validated and ready to write.
struct SyslogMessage<'a> {
    header: SyslogHeaderFields<'a>,
    timestamp: String,
    message: &'a str,
}

impl<'a> SyslogMessage<'a> {
    fn from_row(row: &'a ArrowCodecRow<'_>) -> error_stack::Result<Self, CodecError> {
        let facility = required_u8(row, "facility")?;
        if facility > 23 {
            return Err(encode_field_failure(
                row,
                "facility",
                FieldEncodeError::AboveMaximum { maximum: 23 },
            ));
        }
        let severity = required_u8(row, "severity")?;
        if severity > 7 {
            return Err(encode_field_failure(
                row,
                "severity",
                FieldEncodeError::AboveMaximum { maximum: 7 },
            ));
        }
        let message = required_string(row, "message")?;
        let message = message.strip_prefix('\u{feff}').unwrap_or(message);
        let hostname = header_value(row, "hostname", 255)?;
        let app_name = header_value(row, "app_name", 48)?;
        let proc_id = header_value(row, "proc_id", 128)?;
        let msg_id = header_value(row, "msg_id", 32)?;
        let structured_data = optional_string(row, "structured_data")?;
        if let Some(structured_data) = structured_data {
            let consumed = structured_data_prefix(structured_data, true)
                .change_context_lazy(|| encode_field_context(row, "structured_data"))?;
            if consumed != structured_data.len() {
                return Err(encode_field_failure(
                    row,
                    "structured_data",
                    FieldEncodeError::TrailingStructuredData,
                ));
            }
        }

        let timestamp = match optional_datetime(row, "timestamp")?.as_ref() {
            Some(timestamp) => format_rfc5424_timestamp(timestamp),
            None => "-".to_string(),
        };
        let priority = u16::from(facility) * 8 + u16::from(severity);
        Ok(Self {
            header: SyslogHeaderFields {
                priority,
                hostname,
                app_name,
                proc_id,
                msg_id,
                structured_data,
            },
            timestamp,
            message,
        })
    }

    fn write(
        &self,
        row: &ArrowCodecRow<'_>,
        output: &mut impl std::io::Write,
    ) -> error_stack::Result<(), CodecError> {
        let header = &self.header;
        write!(
            output,
            "<{}>1 {} {} {} {} {} {} {}",
            header.priority,
            self.timestamp,
            header.hostname.unwrap_or("-"),
            header.app_name.unwrap_or("-"),
            header.proc_id.unwrap_or("-"),
            header.msg_id.unwrap_or("-"),
            header.structured_data.unwrap_or("-"),
            self.message,
        )
        .map_err(|source| Report::new(source).change_context(encode_context(row)))
    }
}

/// The context of a failure to decode a payload as a syslog message.
fn decode_context(codec: &CompiledCodec) -> CodecError {
    CodecError::SyslogDecode {
        codec: codec.name.as_str().to_string(),
    }
}

/// A payload failing to decode as a syslog message because of `issue`.
fn decode_failure(codec: &CompiledCodec, issue: SyslogDecodeError) -> Report<CodecError> {
    Report::new(issue).change_context(decode_context(codec))
}

/// The context of a failure to encode a record as a syslog message.
fn encode_context(row: &ArrowCodecRow<'_>) -> CodecError {
    CodecError::SyslogEncode {
        codec: row.codec.name.as_str().to_string(),
    }
}

/// The context of a failure to encode `field` of a record as a syslog message.
fn encode_field_context(row: &ArrowCodecRow<'_>, field: &str) -> CodecError {
    CodecError::EncodeField {
        codec: row.codec.name.as_str().to_string(),
        field: field.to_string(),
    }
}

/// `field` of a record failing to encode as a syslog message because of `issue`.
fn encode_field_failure(
    row: &ArrowCodecRow<'_>,
    field: &str,
    issue: FieldEncodeError,
) -> Report<CodecError> {
    Report::new(issue).change_context(encode_field_context(row, field))
}

/// A required `field` of a record holding null.
fn required_null(row: &ArrowCodecRow<'_>, field: &str) -> Report<CodecError> {
    encode_field_failure(row, field, FieldEncodeError::RequiredNull)
}

/// A `field` of a record held in a column of another type than the `expected` one the fixed
/// SYSLOG field contract gives it.
fn column_type(row: &ArrowCodecRow<'_>, field: &str, expected: ParseAsType) -> Report<CodecError> {
    encode_field_failure(row, field, FieldEncodeError::ColumnType { expected })
}

/// A record whose schema lacks `field`, which the syslog message it encodes to requires.
fn missing_schema_field(row: &ArrowCodecRow<'_>, field: &str) -> Report<CodecError> {
    Report::new(SyslogEncodeError::MissingSchemaField {
        field: field.to_string(),
    })
    .change_context(encode_context(row))
}

/// A payload split at its `<PRI>` header. `has_priority` says whether the header was actually
/// present, because a payload without one keeps the default priority and is never read as RFC
/// 5424.
struct SplitPriority<'payload> {
    priority: u8,
    body: &'payload str,
    has_priority: bool,
}

impl<'payload> SplitPriority<'payload> {
    /// A payload whose `<PRI>` header is missing or malformed, which stays whole and unprefixed.
    const fn absent(payload: &'payload str) -> Self {
        Self {
            priority: DEFAULT_PRIORITY,
            body: payload,
            has_priority: false,
        }
    }
}

fn split_priority(payload: &str) -> SplitPriority<'_> {
    let Some(rest) = payload.strip_prefix('<') else {
        return SplitPriority::absent(payload);
    };
    let Some(end) = rest.find('>') else {
        return SplitPriority::absent(payload);
    };
    let digits = &rest[..end];
    if digits.is_empty()
        || digits.len() > 3
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
        || (digits.len() > 1 && digits.starts_with('0'))
    {
        return SplitPriority::absent(payload);
    }
    let Ok(priority) = digits.parse::<u8>() else {
        return SplitPriority::absent(payload);
    };
    if priority > 191 {
        return SplitPriority::absent(payload);
    }
    SplitPriority {
        priority,
        body: &rest[end + 1..],
        has_priority: true,
    }
}

fn looks_like_rfc5424(body: &str) -> bool {
    let version = body.split_once(' ').map(|(version, _)| version);
    version.is_some_and(|version| {
        !version.is_empty()
            && version.len() <= 3
            && version.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn parse_rfc5424<'a>(
    codec: &CompiledCodec,
    priority: u8,
    body: &'a str,
) -> error_stack::Result<ParsedSyslog<'a>, CodecError> {
    let mut body = body;
    let version = take_token(codec, &mut body, SyslogHeaderField::Version)?;
    if version != "1" {
        return Err(decode_failure(codec, SyslogDecodeError::Version));
    }
    let timestamp = take_token(codec, &mut body, SyslogHeaderField::Timestamp)?;
    let hostname = take_token(codec, &mut body, SyslogHeaderField::Hostname)?;
    let app_name = take_token(codec, &mut body, SyslogHeaderField::AppName)?;
    let proc_id = take_token(codec, &mut body, SyslogHeaderField::ProcId)?;
    let msg_id = take_token(codec, &mut body, SyslogHeaderField::MsgId)?;

    let timestamp = if timestamp == "-" {
        None
    } else {
        Some(parse_rfc5424_timestamp(codec, timestamp)?)
    };
    let hostname = parse_header(codec, SyslogHeaderField::Hostname, hostname, 255)?;
    let app_name = parse_header(codec, SyslogHeaderField::AppName, app_name, 48)?;
    let proc_id = parse_header(codec, SyslogHeaderField::ProcId, proc_id, 128)?;
    let msg_id = parse_header(codec, SyslogHeaderField::MsgId, msg_id, 32)?;

    let structured_end = structured_data_prefix(body, false)
        .change_context(SyslogDecodeError::StructuredData)
        .change_context_lazy(|| decode_context(codec))?;
    let structured_data_raw = &body[..structured_end];
    let tail = &body[structured_end..];
    let message = if tail.is_empty() {
        ""
    } else if let Some(message) = tail.strip_prefix(' ') {
        message.strip_prefix('\u{feff}').unwrap_or(message)
    } else {
        return Err(decode_failure(
            codec,
            SyslogDecodeError::StructuredDataDelimiter,
        ));
    };

    Ok(ParsedSyslog {
        facility: priority / 8,
        severity: priority % 8,
        timestamp,
        hostname,
        app_name,
        proc_id,
        msg_id,
        structured_data: (structured_data_raw != "-").then_some(structured_data_raw),
        message,
    })
}

fn take_token<'a>(
    codec: &CompiledCodec,
    body: &mut &'a str,
    field: SyslogHeaderField,
) -> error_stack::Result<&'a str, CodecError> {
    let Some((token, remainder)) = body.split_once(' ') else {
        return Err(decode_failure(
            codec,
            SyslogDecodeError::MissingHeaderField { field },
        ));
    };
    if token.is_empty() {
        return Err(decode_failure(
            codec,
            SyslogDecodeError::EmptyHeaderField { field },
        ));
    }
    *body = remainder;
    Ok(token)
}

fn parse_header<'a>(
    codec: &CompiledCodec,
    field: SyslogHeaderField,
    value: &'a str,
    max_len: usize,
) -> error_stack::Result<Option<&'a str>, CodecError> {
    if value == "-" {
        return Ok(None);
    }
    validate_decoded_header(codec, field, value, max_len)?;
    Ok(Some(value))
}

/// Checks the shape of a decoded header `field`, whose failure names the field and keeps the
/// offending byte beneath it.
fn validate_decoded_header(
    codec: &CompiledCodec,
    field: SyslogHeaderField,
    value: &str,
    max_len: usize,
) -> error_stack::Result<(), CodecError> {
    validate_header_shape(value, max_len)
        .change_context(SyslogDecodeError::InvalidHeaderField { field })
        .change_context_lazy(|| decode_context(codec))
}

fn parse_rfc3164<'a>(
    codec: &CompiledCodec,
    priority: u8,
    body: &'a str,
) -> error_stack::Result<ParsedSyslog<'a>, CodecError> {
    let (timestamp, remainder) = parse_rfc3164_timestamp(body);
    let Some(timestamp) = timestamp else {
        return Ok(ParsedSyslog {
            facility: priority / 8,
            severity: priority % 8,
            timestamp: None,
            hostname: None,
            app_name: None,
            proc_id: None,
            msg_id: None,
            structured_data: None,
            message: body,
        });
    };
    let (hostname, remainder) = match remainder.split_once(' ') {
        Some((hostname, remainder)) => (hostname, remainder),
        None => (remainder, ""),
    };
    let hostname = if hostname.is_empty() {
        None
    } else {
        validate_decoded_header(codec, SyslogHeaderField::Rfc3164Hostname, hostname, 255)?;
        Some(hostname)
    };
    let tag_len = remainder
        .bytes()
        .take_while(u8::is_ascii_alphanumeric)
        .count();
    let (app_name, message) = if tag_len == 0 {
        (None, remainder)
    } else {
        let tag = &remainder[..tag_len];
        validate_decoded_header(codec, SyslogHeaderField::Rfc3164Tag, tag, 32)?;
        let mut content = &remainder[tag_len..];
        if let Some(process_suffix) = content.strip_prefix('[')
            && let Some(end) = process_suffix.find(']')
        {
            content = &process_suffix[end + 1..];
        }
        if let Some(after_colon) = content.strip_prefix(':') {
            content = after_colon.strip_prefix(' ').unwrap_or(after_colon);
        } else if let Some(after_space) = content.strip_prefix(' ') {
            content = after_space;
        }
        (Some(tag), content)
    };
    Ok(ParsedSyslog {
        facility: priority / 8,
        severity: priority % 8,
        timestamp: Some(timestamp),
        hostname,
        app_name,
        proc_id: None,
        msg_id: None,
        structured_data: None,
        message,
    })
}

fn parse_rfc3164_timestamp(body: &str) -> (Option<DateTime<FixedOffset>>, &str) {
    if body.len() < 15 || !body.is_char_boundary(15) {
        return (None, body);
    }
    let timestamp = &body[..15];
    let with_year = format!("{} {timestamp}", Utc::now().year());
    let Ok(timestamp) = NaiveDateTime::parse_from_str(&with_year, "%Y %b %e %H:%M:%S") else {
        return (None, body);
    };
    let remainder = &body[15..];
    let remainder = remainder.strip_prefix(' ').unwrap_or(remainder);
    (Some(timestamp.and_utc().fixed_offset()), remainder)
}

fn parse_rfc5424_timestamp(
    codec: &CompiledCodec,
    value: &str,
) -> error_stack::Result<DateTime<FixedOffset>, CodecError> {
    let bytes = value.as_bytes();
    let zone_start = if bytes.last() == Some(&b'Z') {
        bytes
            .len()
            .checked_sub(1)
            .verified("a trailing byte means the value holds at least one byte")
    } else if bytes.len() >= 6
        && matches!(bytes[bytes.len() - 6], b'+' | b'-')
        && bytes[bytes.len() - 3] == b':'
        && bytes[bytes.len() - 5..bytes.len() - 3]
            .iter()
            .chain(&bytes[bytes.len() - 2..])
            .all(u8::is_ascii_digit)
    {
        bytes.len() - 6
    } else {
        return Err(decode_failure(codec, SyslogDecodeError::TimestampOffset));
    };
    let fixed_shape = bytes.len() >= 20
        && bytes.get(4) == Some(&b'-')
        && bytes.get(7) == Some(&b'-')
        && bytes.get(10) == Some(&b'T')
        && bytes.get(13) == Some(&b':')
        && bytes.get(16) == Some(&b':')
        && [
            &bytes[0..4],
            &bytes[5..7],
            &bytes[8..10],
            &bytes[11..13],
            &bytes[14..16],
            &bytes[17..19],
        ]
        .into_iter()
        .flatten()
        .all(u8::is_ascii_digit);
    if !fixed_shape || zone_start < 19 {
        return Err(decode_failure(codec, SyslogDecodeError::TimestampShape));
    }
    if zone_start > 19 {
        let fraction = &bytes[20..zone_start];
        if bytes.get(19) != Some(&b'.')
            || fraction.is_empty()
            || fraction.len() > 6
            || !fraction.iter().all(u8::is_ascii_digit)
        {
            return Err(decode_failure(codec, SyslogDecodeError::TimestampFraction));
        }
    }
    DateTime::parse_from_rfc3339(value)
        .map_err(|source| decode_failure(codec, SyslogDecodeError::Timestamp { source }))
}

fn format_rfc5424_timestamp(value: &DateTime<FixedOffset>) -> String {
    let value = value.with_timezone(&Utc);
    let mut formatted = value.format("%Y-%m-%dT%H:%M:%S").to_string();
    let micros = value.timestamp_subsec_micros();
    if micros != 0 {
        let fraction = format!("{micros:06}");
        formatted.push('.');
        formatted.push_str(fraction.trim_end_matches('0'));
    }
    formatted.push('Z');
    formatted
}

fn validate_header_shape(
    value: &str,
    max_len: usize,
) -> error_stack::Result<(), RuntimeSchemaError> {
    if value.is_empty() {
        return Err(Report::new(RuntimeSchemaError::InvalidSyslogHeader {
            position: 0,
            issue: SyslogHeaderIssue::Empty,
        }));
    }
    if value.len() > max_len {
        return Err(Report::new(RuntimeSchemaError::InvalidSyslogHeader {
            position: max_len,
            issue: SyslogHeaderIssue::TooLong {
                length: value.len(),
                maximum: max_len,
            },
        }));
    }
    if let Some((position, _)) = value
        .bytes()
        .enumerate()
        .find(|(_, byte)| !(b'!'..=b'~').contains(byte))
    {
        return Err(Report::new(RuntimeSchemaError::InvalidSyslogHeader {
            position,
            issue: SyslogHeaderIssue::NonPrintableAscii,
        }));
    }
    Ok(())
}

fn structured_data_prefix(
    value: &str,
    strict_escapes: bool,
) -> error_stack::Result<usize, RuntimeSchemaError> {
    let bytes = value.as_bytes();
    if bytes.first() == Some(&b'-') {
        return Ok(1);
    }
    if bytes.first() != Some(&b'[') {
        return Err(Report::new(
            RuntimeSchemaError::InvalidSyslogStructuredData {
                position: 0,
                issue: SyslogStructuredDataIssue::MissingElement,
            },
        ));
    }
    let mut element_ids = HashSet::default();
    let mut cursor = 0;
    while bytes.get(cursor) == Some(&b'[') {
        cursor += 1;
        let id_start = cursor;
        while let Some(byte) = bytes.get(cursor)
            && *byte != b' '
            && *byte != b']'
        {
            if !valid_sd_name_byte(*byte) {
                return Err(Report::new(
                    RuntimeSchemaError::InvalidSyslogStructuredData {
                        position: cursor,
                        issue: SyslogStructuredDataIssue::InvalidElementIdCharacter,
                    },
                ));
            }
            cursor += 1;
        }
        let id_len = cursor - id_start;
        if id_len == 0 || id_len > 32 {
            return Err(Report::new(
                RuntimeSchemaError::InvalidSyslogStructuredData {
                    position: id_start,
                    issue: SyslogStructuredDataIssue::ElementIdLength { length: id_len },
                },
            ));
        }
        if !element_ids.insert(&bytes[id_start..cursor]) {
            return Err(Report::new(
                RuntimeSchemaError::InvalidSyslogStructuredData {
                    position: id_start,
                    issue: SyslogStructuredDataIssue::DuplicateElementId,
                },
            ));
        }
        let mut parameter_names = HashSet::default();
        loop {
            match bytes.get(cursor) {
                Some(b']') => {
                    cursor += 1;
                    break;
                }
                Some(b' ') => cursor += 1,
                Some(_) => {
                    return Err(Report::new(
                        RuntimeSchemaError::InvalidSyslogStructuredData {
                            position: cursor,
                            issue: SyslogStructuredDataIssue::ElementDelimiter,
                        },
                    ));
                }
                None => {
                    return Err(Report::new(
                        RuntimeSchemaError::InvalidSyslogStructuredData {
                            position: cursor,
                            issue: SyslogStructuredDataIssue::UnterminatedElement,
                        },
                    ));
                }
            }
            let name_start = cursor;
            while let Some(byte) = bytes.get(cursor)
                && *byte != b'='
            {
                if !valid_sd_name_byte(*byte) {
                    return Err(Report::new(
                        RuntimeSchemaError::InvalidSyslogStructuredData {
                            position: cursor,
                            issue: SyslogStructuredDataIssue::InvalidParameterNameCharacter,
                        },
                    ));
                }
                cursor += 1;
            }
            let name_len = cursor - name_start;
            if name_len == 0 || name_len > 32 {
                return Err(Report::new(
                    RuntimeSchemaError::InvalidSyslogStructuredData {
                        position: name_start,
                        issue: SyslogStructuredDataIssue::ParameterNameLength { length: name_len },
                    },
                ));
            }
            if !parameter_names.insert(&bytes[name_start..cursor]) {
                return Err(Report::new(
                    RuntimeSchemaError::InvalidSyslogStructuredData {
                        position: name_start,
                        issue: SyslogStructuredDataIssue::DuplicateParameterName,
                    },
                ));
            }
            if bytes.get(cursor) != Some(&b'=') || bytes.get(cursor + 1) != Some(&b'"') {
                return Err(Report::new(
                    RuntimeSchemaError::InvalidSyslogStructuredData {
                        position: cursor,
                        issue: SyslogStructuredDataIssue::ParameterShape,
                    },
                ));
            }
            cursor += 2;
            loop {
                match bytes.get(cursor) {
                    Some(b'"') => {
                        cursor += 1;
                        break;
                    }
                    Some(b'\\') => {
                        let Some(escaped) = bytes.get(cursor + 1) else {
                            return Err(Report::new(
                                RuntimeSchemaError::InvalidSyslogStructuredData {
                                    position: cursor,
                                    issue: SyslogStructuredDataIssue::UnterminatedEscape,
                                },
                            ));
                        };
                        if strict_escapes && !matches!(escaped, b'"' | b'\\' | b']') {
                            return Err(Report::new(
                                RuntimeSchemaError::InvalidSyslogStructuredData {
                                    position: cursor + 1,
                                    issue: SyslogStructuredDataIssue::InvalidEscape,
                                },
                            ));
                        }
                        cursor += if matches!(escaped, b'"' | b'\\' | b']') {
                            2
                        } else {
                            1
                        };
                    }
                    Some(b']') => {
                        return Err(Report::new(
                            RuntimeSchemaError::InvalidSyslogStructuredData {
                                position: cursor,
                                issue: SyslogStructuredDataIssue::UnescapedClosingBracket,
                            },
                        ));
                    }
                    Some(_) => cursor += 1,
                    None => {
                        return Err(Report::new(
                            RuntimeSchemaError::InvalidSyslogStructuredData {
                                position: cursor,
                                issue: SyslogStructuredDataIssue::UnterminatedParameterValue,
                            },
                        ));
                    }
                }
            }
            if !matches!(bytes.get(cursor), Some(b' ') | Some(b']')) {
                return Err(Report::new(
                    RuntimeSchemaError::InvalidSyslogStructuredData {
                        position: cursor,
                        issue: SyslogStructuredDataIssue::ParameterDelimiter,
                    },
                ));
            }
        }
    }
    Ok(cursor)
}

fn valid_sd_name_byte(byte: u8) -> bool {
    (b'!'..=b'~').contains(&byte) && !matches!(byte, b'=' | b']' | b'"')
}

fn append_row(
    codec: &CompiledCodec,
    parsed: &ParsedSyslog<'_>,
    builder: &mut RuntimeRecordBatchBuilder,
) -> error_stack::Result<(), CodecError> {
    for index in 0..codec.schema.fields.len() {
        let field = codec.schema.fields[index].name.as_str();
        let appended = match field {
            "facility" => append_u8(builder, index, parsed.facility),
            "severity" => append_u8(builder, index, parsed.severity),
            "timestamp" => append_datetime(builder, index, parsed.timestamp.as_ref()),
            "hostname" => append_string(builder, index, parsed.hostname),
            "app_name" => append_string(builder, index, parsed.app_name),
            "proc_id" => append_string(builder, index, parsed.proc_id),
            "msg_id" => append_string(builder, index, parsed.msg_id),
            "structured_data" => append_string(builder, index, parsed.structured_data),
            "message" => append_string(builder, index, Some(parsed.message)),
            unknown => Err(Report::new(RuntimeSchemaError::UnsupportedSyslogField {
                field: unknown.to_string(),
            })),
        };
        appended.change_context_lazy(|| decode_context(codec))?;
    }
    Ok(())
}

fn prepare_append(
    builder: &mut RuntimeRecordBatchBuilder,
    index: usize,
) -> error_stack::Result<(), RuntimeSchemaError> {
    let next = builder.next_field_index()?;
    if next != index {
        return Err(Report::new(RuntimeSchemaError::SyslogBuilderColumnOrder {
            expected: next,
            found: index,
        }));
    }
    Ok(())
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(reason = "the external Arrow builder owns one appended scalar")
)]
fn append_u8(
    builder: &mut RuntimeRecordBatchBuilder,
    index: usize,
    value: u8,
) -> error_stack::Result<(), RuntimeSchemaError> {
    prepare_append(builder, index)?;
    let field = builder.fields[index].name.clone();
    let expected = builder.fields[index].ty.arrow_data_type();
    if !builder.builders[index].as_any().is::<UInt8Builder>() {
        return Err(Report::new(RuntimeSchemaError::ExactTypeMismatch {
            location: RuntimeValueLocation::CodecField {
                field,
                elements: Vec::new(),
            },
            expected,
            found: builder.builders[index].finish_cloned().data_type().clone(),
        }));
    }
    builder.builders[index]
        .as_any_mut()
        .downcast_mut::<UInt8Builder>()
        .verified("the SYSLOG U8 builder's concrete type was checked immediately above")
        .append_value(value);
    builder.next_column += 1;
    Ok(())
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(reason = "the external Arrow builder owns one appended scalar")
)]
fn append_string(
    builder: &mut RuntimeRecordBatchBuilder,
    index: usize,
    value: Option<&str>,
) -> error_stack::Result<(), RuntimeSchemaError> {
    prepare_append(builder, index)?;
    let field = builder.fields[index].name.clone();
    let expected = builder.fields[index].ty.arrow_data_type();
    if !builder.builders[index].as_any().is::<StringBuilder>() {
        return Err(Report::new(RuntimeSchemaError::ExactTypeMismatch {
            location: RuntimeValueLocation::CodecField {
                field,
                elements: Vec::new(),
            },
            expected,
            found: builder.builders[index].finish_cloned().data_type().clone(),
        }));
    }
    builder.builders[index]
        .as_any_mut()
        .downcast_mut::<StringBuilder>()
        .verified("the SYSLOG STRING builder's concrete type was checked immediately above")
        .append_option(value);
    builder.next_column += 1;
    Ok(())
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(reason = "the external Arrow builder owns one appended scalar")
)]
fn append_datetime(
    builder: &mut RuntimeRecordBatchBuilder,
    index: usize,
    value: Option<&DateTime<FixedOffset>>,
) -> error_stack::Result<(), RuntimeSchemaError> {
    prepare_append(builder, index)?;
    let value = value
        .map(|value| {
            value
                .timestamp_nanos_opt()
                .ok_or_else(|| Report::new(RuntimeSchemaError::SyslogTimestampOutOfRange))
        })
        .transpose()?;
    let field = builder.fields[index].name.clone();
    let expected = builder.fields[index].ty.arrow_data_type();
    if !builder.builders[index]
        .as_any()
        .is::<TimestampNanosecondBuilder>()
    {
        return Err(Report::new(RuntimeSchemaError::ExactTypeMismatch {
            location: RuntimeValueLocation::CodecField {
                field,
                elements: Vec::new(),
            },
            expected,
            found: builder.builders[index].finish_cloned().data_type().clone(),
        }));
    }
    builder.builders[index]
        .as_any_mut()
        .downcast_mut::<TimestampNanosecondBuilder>()
        .verified("the SYSLOG DATETIME builder's concrete type was checked immediately above")
        .append_option(value);
    builder.next_column += 1;
    Ok(())
}

fn field_index(row: &ArrowCodecRow<'_>, name: &str) -> Option<usize> {
    row.codec
        .schema
        .fields
        .iter()
        .position(|field| field.name == name)
}

fn required_u8(row: &ArrowCodecRow<'_>, name: &str) -> error_stack::Result<u8, CodecError> {
    let Some(index) = field_index(row, name) else {
        return Err(missing_schema_field(row, name));
    };
    let array = row.batch.batch.column(index);
    if array.is_null(row.row_index) {
        return Err(required_null(row, name));
    }
    let Some(array) = array.as_any().downcast_ref::<UInt8Array>() else {
        return Err(column_type(row, name, ParseAsType::U8));
    };
    Ok(array.value(row.row_index))
}

fn required_string<'a>(
    row: &'a ArrowCodecRow<'_>,
    name: &str,
) -> error_stack::Result<&'a str, CodecError> {
    if let Some(value) = optional_string(row, name)? {
        return Ok(value);
    }
    if field_index(row, name).is_none() {
        return Err(missing_schema_field(row, name));
    }
    Err(required_null(row, name))
}

fn optional_string<'a>(
    row: &'a ArrowCodecRow<'_>,
    name: &str,
) -> error_stack::Result<Option<&'a str>, CodecError> {
    let Some(index) = field_index(row, name) else {
        return Ok(None);
    };
    let array = row.batch.batch.column(index);
    if array.is_null(row.row_index) {
        return Ok(None);
    }
    let Some(array) = array.as_any().downcast_ref::<StringArray>() else {
        return Err(column_type(row, name, ParseAsType::String));
    };
    Ok(Some(array.value(row.row_index)))
}

fn optional_datetime(
    row: &ArrowCodecRow<'_>,
    name: &str,
) -> error_stack::Result<Option<DateTime<FixedOffset>>, CodecError> {
    let Some(index) = field_index(row, name) else {
        return Ok(None);
    };
    let array = row.batch.batch.column(index);
    if array.is_null(row.row_index) {
        return Ok(None);
    }
    let Some(array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() else {
        return Err(column_type(row, name, ParseAsType::Datetime));
    };
    Ok(Some(
        DateTime::from_timestamp_nanos(array.value(row.row_index)).fixed_offset(),
    ))
}

fn header_value<'a>(
    row: &'a ArrowCodecRow<'_>,
    name: &str,
    max_len: usize,
) -> error_stack::Result<Option<&'a str>, CodecError> {
    let value = optional_string(row, name)?;
    if let Some(value) = value {
        validate_header_shape(value, max_len)
            .change_context_lazy(|| encode_field_context(row, name))?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_header_reports_the_offending_byte_and_typed_reason() {
        let error = validate_header_shape("host name", 255)
            .expect_err("a space is not valid in an RFC 5424 header value");

        assert!(matches!(
            error.current_context(),
            RuntimeSchemaError::InvalidSyslogHeader {
                position: 4,
                issue: SyslogHeaderIssue::NonPrintableAscii,
            }
        ));
    }

    #[test]
    fn invalid_structured_data_reports_the_offending_byte_and_typed_reason() {
        let error = structured_data_prefix("[bad=id]", true)
            .expect_err("an equals sign is not valid in an SD-ID");

        assert!(matches!(
            error.current_context(),
            RuntimeSchemaError::InvalidSyslogStructuredData {
                position: 4,
                issue: SyslogStructuredDataIssue::InvalidElementIdCharacter,
            }
        ));
    }
}
