//! Syslog wire decoding and encoding.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Typed Arrow conversion for RFC 3164 and RFC 5424 syslog messages, and the RFC 5424
//!   frame one batch of records is published in.
//! - **Depends on.** Wire codec models, Arrow builders and UTC for omitted RFC 3164 years.
//! - **Must not know.** Domains, runtime clocks, schedules or connector lifecycle.

use ahash::HashSet;
use arrow_array::{
    Array, StringArray, TimestampNanosecondArray, UInt8Array,
    builder::{StringBuilder, TimestampNanosecondBuilder, UInt8Builder},
};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime, Utc};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{CreateCodec, ParseAsType};

use super::{
    ArrowCodecRow, CodecError, CompiledCodec, CompiledSchema, RuntimeRecordBatchBuilder,
    RuntimeSchemaError, RuntimeValueLocation, SyslogHeaderIssue, SyslogStructuredDataIssue,
    arrow_data_type,
};

const DEFAULT_PRIORITY: u8 = 13;

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
    codec: &CreateCodec,
    schema: &CompiledSchema,
) -> Result<(), CodecError> {
    if !codec.encoding_rules.is_empty() {
        return Err(invalid_codec(
            codec,
            "SYSLOG codecs do not support ENCODE field rules",
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
            return Err(invalid_codec(
                codec,
                format!(
                    "SYSLOG schema field '{}' is outside the fixed field contract",
                    field.name
                ),
            ));
        };
        if field.ty != expected_type || field.optional != expected_optional {
            return Err(invalid_codec(
                codec,
                format!(
                    "SYSLOG field '{}' must be {}{}, found {}{}",
                    field.name,
                    expected_type,
                    if expected_optional { " OPTIONAL" } else { "" },
                    field.ty,
                    if field.optional { " OPTIONAL" } else { "" },
                ),
            ));
        }
    }
    Ok(())
}

pub(super) fn decode(
    codec: &CompiledCodec,
    payload: &[u8],
    builder: &mut RuntimeRecordBatchBuilder,
) -> Result<(), CodecError> {
    let last_kept = payload
        .iter()
        .rposition(|byte| !matches!(byte, b'\r' | b'\n' | b'\0'));
    let end = match last_kept {
        Some(index) => index + 1,
        None => 0,
    };
    let payload = &payload[..end];
    if payload.is_empty() {
        return Err(decode_error(
            codec,
            "payload is empty after trailing delimiters",
        ));
    }
    let payload = std::str::from_utf8(payload)
        .map_err(|error| decode_error(codec, format!("payload is not valid UTF-8: {error}")))?;
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
            return Err(Report::new(encode_field_error(
                row,
                "facility",
                "value must be at most 23",
            )));
        }
        let severity = required_u8(row, "severity")?;
        if severity > 7 {
            return Err(Report::new(encode_field_error(
                row,
                "severity",
                "value must be at most 7",
            )));
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
                .map_err(|error| encode_field_error(row, "structured_data", error.to_string()))?;
            if consumed != structured_data.len() {
                return Err(Report::new(encode_field_error(
                    row,
                    "structured_data",
                    "text contains trailing content after the SD elements",
                )));
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
        .map_err(|error| Report::new(encode_error(row, error.to_string())))
    }
}

fn invalid_codec(codec: &CreateCodec, reason: impl Into<String>) -> CodecError {
    CodecError::InvalidCodec {
        codec: codec.name.as_str().to_string(),
        reason: reason.into(),
    }
}

fn decode_error(codec: &CompiledCodec, reason: impl Into<String>) -> CodecError {
    CodecError::SyslogDecode {
        codec: codec.name.as_str().to_string(),
        reason: reason.into(),
    }
}

fn encode_error(row: &ArrowCodecRow<'_>, reason: impl Into<String>) -> CodecError {
    CodecError::SyslogEncode {
        codec: row.codec.name.as_str().to_string(),
        reason: reason.into(),
    }
}

fn encode_field_error(
    row: &ArrowCodecRow<'_>,
    field: &str,
    reason: impl Into<String>,
) -> CodecError {
    CodecError::EncodeField {
        codec: row.codec.name.as_str().to_string(),
        field: field.to_string(),
        reason: reason.into(),
    }
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
) -> Result<ParsedSyslog<'a>, CodecError> {
    let mut body = body;
    let version = take_token(codec, &mut body, "VERSION")?;
    if version != "1" {
        return Err(decode_error(codec, "RFC 5424 VERSION must be 1"));
    }
    let timestamp = take_token(codec, &mut body, "TIMESTAMP")?;
    let hostname = take_token(codec, &mut body, "HOSTNAME")?;
    let app_name = take_token(codec, &mut body, "APP-NAME")?;
    let proc_id = take_token(codec, &mut body, "PROCID")?;
    let msg_id = take_token(codec, &mut body, "MSGID")?;

    let timestamp = if timestamp == "-" {
        None
    } else {
        Some(parse_rfc5424_timestamp(codec, timestamp)?)
    };
    let hostname = parse_header(codec, "HOSTNAME", hostname, 255)?;
    let app_name = parse_header(codec, "APP-NAME", app_name, 48)?;
    let proc_id = parse_header(codec, "PROCID", proc_id, 128)?;
    let msg_id = parse_header(codec, "MSGID", msg_id, 32)?;

    let structured_end = structured_data_prefix(body, false)
        .map_err(|reason| decode_error(codec, format!("invalid STRUCTURED-DATA: {reason}")))?;
    let structured_data_raw = &body[..structured_end];
    let tail = &body[structured_end..];
    let message = if tail.is_empty() {
        ""
    } else if let Some(message) = tail.strip_prefix(' ') {
        message.strip_prefix('\u{feff}').unwrap_or(message)
    } else {
        return Err(decode_error(
            codec,
            "STRUCTURED-DATA must be followed by a space or end of message",
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
    label: &str,
) -> Result<&'a str, CodecError> {
    let Some((token, remainder)) = body.split_once(' ') else {
        return Err(decode_error(
            codec,
            format!("RFC 5424 header is missing {label}"),
        ));
    };
    if token.is_empty() {
        return Err(decode_error(codec, format!("RFC 5424 {label} is empty")));
    }
    *body = remainder;
    Ok(token)
}

fn parse_header<'a>(
    codec: &CompiledCodec,
    label: &str,
    value: &'a str,
    max_len: usize,
) -> Result<Option<&'a str>, CodecError> {
    if value == "-" {
        return Ok(None);
    }
    validate_header_shape(value, max_len)
        .map_err(|reason| decode_error(codec, format!("invalid {label}: {reason}")))?;
    Ok(Some(value))
}

fn parse_rfc3164<'a>(
    codec: &CompiledCodec,
    priority: u8,
    body: &'a str,
) -> Result<ParsedSyslog<'a>, CodecError> {
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
        validate_header_shape(hostname, 255).map_err(|reason| {
            decode_error(codec, format!("invalid RFC 3164 HOSTNAME: {reason}"))
        })?;
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
        validate_header_shape(tag, 32)
            .map_err(|reason| decode_error(codec, format!("invalid RFC 3164 TAG: {reason}")))?;
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
) -> Result<DateTime<FixedOffset>, CodecError> {
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
        return Err(decode_error(
            codec,
            "invalid RFC 5424 TIMESTAMP time offset",
        ));
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
        return Err(decode_error(
            codec,
            "invalid RFC 5424 TIMESTAMP date or time shape",
        ));
    }
    if zone_start > 19 {
        let fraction = &bytes[20..zone_start];
        if bytes.get(19) != Some(&b'.')
            || fraction.is_empty()
            || fraction.len() > 6
            || !fraction.iter().all(u8::is_ascii_digit)
        {
            return Err(decode_error(
                codec,
                "invalid RFC 5424 TIMESTAMP fractional seconds",
            ));
        }
    }
    DateTime::parse_from_rfc3339(value)
        .map_err(|error| decode_error(codec, format!("invalid RFC 5424 TIMESTAMP: {error}")))
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
) -> Result<(), CodecError> {
    for index in 0..codec.schema.fields.len() {
        let field = codec.schema.fields[index].name.as_str();
        match field {
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
        }
        .map_err(|error| decode_error(codec, error.to_string()))?;
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

fn append_u8(
    builder: &mut RuntimeRecordBatchBuilder,
    index: usize,
    value: u8,
) -> error_stack::Result<(), RuntimeSchemaError> {
    prepare_append(builder, index)?;
    let field = builder.fields[index].name.clone();
    let expected = arrow_data_type(&builder.fields[index].ty);
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

fn append_string(
    builder: &mut RuntimeRecordBatchBuilder,
    index: usize,
    value: Option<&str>,
) -> error_stack::Result<(), RuntimeSchemaError> {
    prepare_append(builder, index)?;
    let field = builder.fields[index].name.clone();
    let expected = arrow_data_type(&builder.fields[index].ty);
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
    let expected = arrow_data_type(&builder.fields[index].ty);
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

fn required_u8(row: &ArrowCodecRow<'_>, name: &str) -> Result<u8, CodecError> {
    let index = field_index(row, name).ok_or_else(|| {
        encode_error(
            row,
            format!("SYSLOG encoding requires schema field '{name}'"),
        )
    })?;
    let array = row.batch.batch.column(index);
    if array.is_null(row.row_index) {
        return Err(encode_field_error(row, name, "required field is null"));
    }
    let Some(array) = array.as_any().downcast_ref::<UInt8Array>() else {
        return Err(encode_field_error(row, name, "field is not a U8 column"));
    };
    Ok(array.value(row.row_index))
}

fn required_string<'a>(row: &'a ArrowCodecRow<'_>, name: &str) -> Result<&'a str, CodecError> {
    optional_string(row, name)?.ok_or_else(|| {
        if field_index(row, name).is_none() {
            encode_error(
                row,
                format!("SYSLOG encoding requires schema field '{name}'"),
            )
        } else {
            encode_field_error(row, name, "required field is null")
        }
    })
}

fn optional_string<'a>(
    row: &'a ArrowCodecRow<'_>,
    name: &str,
) -> Result<Option<&'a str>, CodecError> {
    let Some(index) = field_index(row, name) else {
        return Ok(None);
    };
    let array = row.batch.batch.column(index);
    if array.is_null(row.row_index) {
        return Ok(None);
    }
    let Some(array) = array.as_any().downcast_ref::<StringArray>() else {
        return Err(encode_field_error(
            row,
            name,
            "field is not a STRING column",
        ));
    };
    Ok(Some(array.value(row.row_index)))
}

fn optional_datetime(
    row: &ArrowCodecRow<'_>,
    name: &str,
) -> Result<Option<DateTime<FixedOffset>>, CodecError> {
    let Some(index) = field_index(row, name) else {
        return Ok(None);
    };
    let array = row.batch.batch.column(index);
    if array.is_null(row.row_index) {
        return Ok(None);
    }
    let Some(array) = array.as_any().downcast_ref::<TimestampNanosecondArray>() else {
        return Err(encode_field_error(
            row,
            name,
            "field is not a DATETIME column",
        ));
    };
    Ok(Some(
        DateTime::from_timestamp_nanos(array.value(row.row_index)).fixed_offset(),
    ))
}

fn header_value<'a>(
    row: &'a ArrowCodecRow<'_>,
    name: &str,
    max_len: usize,
) -> Result<Option<&'a str>, CodecError> {
    let value = optional_string(row, name)?;
    if let Some(value) = value {
        validate_header_shape(value, max_len)
            .map_err(|error| encode_field_error(row, name, error.to_string()))?;
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
