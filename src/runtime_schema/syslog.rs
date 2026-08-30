use ahash::HashSet;
use arrow_array::{
    Array, StringArray, TimestampNanosecondArray, UInt8Array,
    builder::{StringBuilder, TimestampNanosecondBuilder, UInt8Builder},
};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime, Utc};
use nervix_models::{CreateCodec, ParseAsType};

use super::{
    ArrowCodecRow, CodecError, CompiledCodec, CompiledSchema, RuntimeRecordBatch,
    RuntimeRecordBatchBuilder,
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
) -> Result<RuntimeRecordBatch, CodecError> {
    let end = payload
        .iter()
        .rposition(|byte| !matches!(byte, b'\r' | b'\n' | b'\0'))
        .map_or(0, |index| index + 1);
    let payload = &payload[..end];
    if payload.is_empty() {
        return Err(decode_error(
            codec,
            "payload is empty after trailing delimiters",
        ));
    }
    let payload = std::str::from_utf8(payload)
        .map_err(|error| decode_error(codec, format!("payload is not valid UTF-8: {error}")))?;
    let (priority, body, has_priority) = split_priority(payload);
    let parsed = if has_priority && looks_like_rfc5424(body) {
        parse_rfc5424(codec, priority, body)?
    } else {
        parse_rfc3164(codec, priority, body)?
    };
    build_batch(codec, &parsed)
}

pub(super) fn encode_row(row: &ArrowCodecRow<'_>, payload: &mut Vec<u8>) -> Result<(), CodecError> {
    let facility = required_u8(row, "facility")?;
    if facility > 23 {
        return Err(encode_field_error(
            row,
            "facility",
            "value must be at most 23",
        ));
    }
    let severity = required_u8(row, "severity")?;
    if severity > 7 {
        return Err(encode_field_error(
            row,
            "severity",
            "value must be at most 7",
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
            .map_err(|reason| encode_field_error(row, "structured_data", reason))?;
        if consumed != structured_data.len() {
            return Err(encode_field_error(
                row,
                "structured_data",
                "text contains trailing content after the SD elements",
            ));
        }
    }

    let timestamp = optional_datetime(row, "timestamp")?
        .as_ref()
        .map(format_rfc5424_timestamp)
        .unwrap_or_else(|| "-".to_string());
    let priority = u16::from(facility) * 8 + u16::from(severity);
    use std::io::Write as _;
    write!(
        payload,
        "<{priority}>1 {timestamp} {} {} {} {} {} {message}",
        hostname.unwrap_or("-"),
        app_name.unwrap_or("-"),
        proc_id.unwrap_or("-"),
        msg_id.unwrap_or("-"),
        structured_data.unwrap_or("-"),
    )
    .map_err(|error| CodecError::SyslogEncode {
        codec: row.codec.name.as_str().to_string(),
        reason: error.to_string(),
    })?;
    Ok(())
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

fn split_priority(payload: &str) -> (u8, &str, bool) {
    let Some(rest) = payload.strip_prefix('<') else {
        return (DEFAULT_PRIORITY, payload, false);
    };
    let Some(end) = rest.find('>') else {
        return (DEFAULT_PRIORITY, payload, false);
    };
    let digits = &rest[..end];
    if digits.is_empty()
        || digits.len() > 3
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
        || (digits.len() > 1 && digits.starts_with('0'))
    {
        return (DEFAULT_PRIORITY, payload, false);
    }
    let Ok(priority) = digits.parse::<u8>() else {
        return (DEFAULT_PRIORITY, payload, false);
    };
    if priority > 191 {
        return (DEFAULT_PRIORITY, payload, false);
    }
    (priority, &rest[end + 1..], true)
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
    let (hostname, remainder) = remainder
        .split_once(' ')
        .map_or((remainder, ""), |(hostname, remainder)| {
            (hostname, remainder)
        });
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
        bytes.len().saturating_sub(1)
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

fn validate_header_shape(value: &str, max_len: usize) -> Result<(), String> {
    if value.is_empty() {
        return Err("value is empty".to_string());
    }
    if value.len() > max_len {
        return Err(format!(
            "value exceeds the maximum length of {max_len} bytes"
        ));
    }
    if value.bytes().any(|byte| !(b'!'..=b'~').contains(&byte)) {
        return Err("value contains characters outside printable US-ASCII".to_string());
    }
    Ok(())
}

fn structured_data_prefix(value: &str, strict_escapes: bool) -> Result<usize, String> {
    let bytes = value.as_bytes();
    if bytes.first() == Some(&b'-') {
        return Ok(1);
    }
    if bytes.first() != Some(&b'[') {
        return Err("expected '-' or an SD element beginning with '['".to_string());
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
                return Err("SD-ID contains an invalid character".to_string());
            }
            cursor += 1;
        }
        let id_len = cursor - id_start;
        if id_len == 0 || id_len > 32 {
            return Err(format!("SD-ID length {id_len} is outside 1..=32"));
        }
        if !element_ids.insert(&bytes[id_start..cursor]) {
            return Err("STRUCTURED-DATA contains a duplicate SD-ID".to_string());
        }
        let mut parameter_names = HashSet::default();
        loop {
            match bytes.get(cursor) {
                Some(b']') => {
                    cursor += 1;
                    break;
                }
                Some(b' ') => cursor += 1,
                Some(_) => return Err("expected a space or ']' after SD-ID".to_string()),
                None => return Err("unterminated SD element".to_string()),
            }
            let name_start = cursor;
            while let Some(byte) = bytes.get(cursor)
                && *byte != b'='
            {
                if !valid_sd_name_byte(*byte) {
                    return Err("PARAM-NAME contains an invalid character".to_string());
                }
                cursor += 1;
            }
            let name_len = cursor - name_start;
            if name_len == 0 || name_len > 32 {
                return Err(format!("PARAM-NAME length {name_len} is outside 1..=32"));
            }
            if !parameter_names.insert(&bytes[name_start..cursor]) {
                return Err("SD element contains a duplicate PARAM-NAME".to_string());
            }
            if bytes.get(cursor) != Some(&b'=') || bytes.get(cursor + 1) != Some(&b'"') {
                return Err("SD parameter must use name=\"value\" shape".to_string());
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
                            return Err("unterminated escape in PARAM-VALUE".to_string());
                        };
                        if strict_escapes && !matches!(escaped, b'"' | b'\\' | b']') {
                            return Err("PARAM-VALUE contains an invalid escape".to_string());
                        }
                        cursor += if matches!(escaped, b'"' | b'\\' | b']') {
                            2
                        } else {
                            1
                        };
                    }
                    Some(b']') => {
                        return Err("unescaped ']' in PARAM-VALUE".to_string());
                    }
                    Some(_) => cursor += 1,
                    None => return Err("unterminated PARAM-VALUE".to_string()),
                }
            }
            if !matches!(bytes.get(cursor), Some(b' ') | Some(b']')) {
                return Err("expected a space or ']' after PARAM-VALUE".to_string());
            }
        }
    }
    Ok(cursor)
}

fn valid_sd_name_byte(byte: u8) -> bool {
    (b'!'..=b'~').contains(&byte) && !matches!(byte, b'=' | b']' | b'"')
}

fn build_batch(
    codec: &CompiledCodec,
    parsed: &ParsedSyslog<'_>,
) -> Result<RuntimeRecordBatch, CodecError> {
    let mut builder = codec.schema.batch_builder(1);
    for index in 0..codec.schema.fields.len() {
        let field = codec.schema.fields[index].name.as_str();
        match field {
            "facility" => append_u8(&mut builder, index, parsed.facility),
            "severity" => append_u8(&mut builder, index, parsed.severity),
            "timestamp" => append_datetime(&mut builder, index, parsed.timestamp.as_ref()),
            "hostname" => append_string(&mut builder, index, parsed.hostname),
            "app_name" => append_string(&mut builder, index, parsed.app_name),
            "proc_id" => append_string(&mut builder, index, parsed.proc_id),
            "msg_id" => append_string(&mut builder, index, parsed.msg_id),
            "structured_data" => append_string(&mut builder, index, parsed.structured_data),
            "message" => append_string(&mut builder, index, Some(parsed.message)),
            unknown => Err(format!("unsupported SYSLOG schema field '{unknown}'")),
        }
        .map_err(|reason| decode_error(codec, reason))?;
    }
    builder
        .finish_row()
        .and_then(|()| builder.finish())
        .map_err(|reason| decode_error(codec, reason))
}

fn prepare_append(builder: &mut RuntimeRecordBatchBuilder, index: usize) -> Result<(), String> {
    let next = builder.next_field_index()?;
    if next != index {
        return Err(format!(
            "SYSLOG Arrow builder expected column {next}, received {index}"
        ));
    }
    Ok(())
}

fn append_u8(
    builder: &mut RuntimeRecordBatchBuilder,
    index: usize,
    value: u8,
) -> Result<(), String> {
    prepare_append(builder, index)?;
    builder.builders[index]
        .as_any_mut()
        .downcast_mut::<UInt8Builder>()
        .ok_or_else(|| {
            format!(
                "SYSLOG field '{}' is not a U8 column",
                builder.fields[index].name
            )
        })?
        .append_value(value);
    builder.next_column += 1;
    Ok(())
}

fn append_string(
    builder: &mut RuntimeRecordBatchBuilder,
    index: usize,
    value: Option<&str>,
) -> Result<(), String> {
    prepare_append(builder, index)?;
    builder.builders[index]
        .as_any_mut()
        .downcast_mut::<StringBuilder>()
        .ok_or_else(|| {
            format!(
                "SYSLOG field '{}' is not a STRING column",
                builder.fields[index].name
            )
        })?
        .append_option(value);
    builder.next_column += 1;
    Ok(())
}

fn append_datetime(
    builder: &mut RuntimeRecordBatchBuilder,
    index: usize,
    value: Option<&DateTime<FixedOffset>>,
) -> Result<(), String> {
    prepare_append(builder, index)?;
    let value = value
        .map(|value| {
            value
                .timestamp_nanos_opt()
                .ok_or_else(|| "SYSLOG timestamp is outside nanosecond range".to_string())
        })
        .transpose()?;
    builder.builders[index]
        .as_any_mut()
        .downcast_mut::<TimestampNanosecondBuilder>()
        .ok_or_else(|| {
            format!(
                "SYSLOG field '{}' is not a DATETIME column",
                builder.fields[index].name
            )
        })?
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
    array
        .as_any()
        .downcast_ref::<UInt8Array>()
        .map(|array| array.value(row.row_index))
        .ok_or_else(|| encode_field_error(row, name, "field is not a U8 column"))
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
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|array| Some(array.value(row.row_index)))
        .ok_or_else(|| encode_field_error(row, name, "field is not a STRING column"))
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
    array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .map(|array| {
            Some(DateTime::from_timestamp_nanos(array.value(row.row_index)).fixed_offset())
        })
        .ok_or_else(|| encode_field_error(row, name, "field is not a DATETIME column"))
}

fn header_value<'a>(
    row: &'a ArrowCodecRow<'_>,
    name: &str,
    max_len: usize,
) -> Result<Option<&'a str>, CodecError> {
    let value = optional_string(row, name)?;
    if let Some(value) = value {
        validate_header_shape(value, max_len)
            .map_err(|reason| encode_field_error(row, name, reason))?;
    }
    Ok(value)
}
