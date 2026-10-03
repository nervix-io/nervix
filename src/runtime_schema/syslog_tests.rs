//! Syslog header and structured-data scanning against per-byte definitions.
//!
//! Layer: test harness.
//! - **Owns.** The byte position and typed reason of every header and structured-data failure,
//!   values that cross the scanners' 64-byte blocks, and the differential property against
//!   per-byte readings of RFC 5424.
//! - **Depends on.** The production header check and structured-data parser.
//! - **Must not know.** Codecs, Arrow builders or connector transports.

use super::*;

/// A structured-data verdict: the parsed length, or the failing byte and its reason.
type Verdict = Result<usize, (usize, SyslogStructuredDataIssue)>;

fn parsed(value: &str, strict_escapes: bool) -> Verdict {
    match structured_data_prefix(value, strict_escapes) {
        Ok(length) => Ok(length),
        Err(error) => match error.current_context() {
            RuntimeSchemaError::InvalidSyslogStructuredData { position, issue } => {
                Err((*position, *issue))
            }
            other => panic!("structured data failed with {other:?}"),
        },
    }
}

fn header_verdict(value: &str, max_len: usize) -> Result<(), (usize, SyslogHeaderIssue)> {
    match validate_header_shape(value, max_len) {
        Ok(()) => Ok(()),
        Err(error) => match error.current_context() {
            RuntimeSchemaError::InvalidSyslogHeader { position, issue } => Err((*position, *issue)),
            other => panic!("a header failed with {other:?}"),
        },
    }
}

/// RFC 5424 header validation read one byte at a time.
fn reference_header(value: &str, max_len: usize) -> Result<(), (usize, SyslogHeaderIssue)> {
    if value.is_empty() {
        return Err((0, SyslogHeaderIssue::Empty));
    }
    if value.len() > max_len {
        let length = value.len();
        return Err((
            max_len,
            SyslogHeaderIssue::TooLong {
                length,
                maximum: max_len,
            },
        ));
    }
    for (position, byte) in value.bytes().enumerate() {
        if !(b'!'..=b'~').contains(&byte) {
            return Err((position, SyslogHeaderIssue::NonPrintableAscii));
        }
    }
    Ok(())
}

fn sd_name_byte(byte: u8) -> bool {
    (b'!'..=b'~').contains(&byte) && !matches!(byte, b'=' | b']' | b'"')
}

/// RFC 5424 `STRUCTURED-DATA` read one byte at a time, as the grammar states it.
fn reference_structured_data(value: &str, strict_escapes: bool) -> Verdict {
    use SyslogStructuredDataIssue as Issue;

    let bytes = value.as_bytes();
    if bytes.first() == Some(&b'-') {
        return Ok(1);
    }
    if bytes.first() != Some(&b'[') {
        return Err((0, Issue::MissingElement));
    }
    let mut element_ids = Vec::new();
    let mut cursor = 0;
    while bytes.get(cursor) == Some(&b'[') {
        cursor += 1;
        let id_start = cursor;
        while let Some(byte) = bytes.get(cursor)
            && *byte != b' '
            && *byte != b']'
        {
            if !sd_name_byte(*byte) {
                return Err((cursor, Issue::InvalidElementIdCharacter));
            }
            cursor += 1;
        }
        let id_len = cursor - id_start;
        if id_len == 0 || id_len > 32 {
            return Err((id_start, Issue::ElementIdLength { length: id_len }));
        }
        let id = &bytes[id_start..cursor];
        if element_ids.contains(&id) {
            return Err((id_start, Issue::DuplicateElementId));
        }
        element_ids.push(id);
        let mut parameter_names = Vec::new();
        loop {
            match bytes.get(cursor) {
                Some(b']') => {
                    cursor += 1;
                    break;
                }
                Some(b' ') => cursor += 1,
                Some(_) => return Err((cursor, Issue::ElementDelimiter)),
                None => return Err((cursor, Issue::UnterminatedElement)),
            }
            let name_start = cursor;
            while let Some(byte) = bytes.get(cursor)
                && *byte != b'='
            {
                if !sd_name_byte(*byte) {
                    return Err((cursor, Issue::InvalidParameterNameCharacter));
                }
                cursor += 1;
            }
            let name_len = cursor - name_start;
            if name_len == 0 || name_len > 32 {
                return Err((name_start, Issue::ParameterNameLength { length: name_len }));
            }
            let name = &bytes[name_start..cursor];
            if parameter_names.contains(&name) {
                return Err((name_start, Issue::DuplicateParameterName));
            }
            parameter_names.push(name);
            if bytes.get(cursor) != Some(&b'=') || bytes.get(cursor + 1) != Some(&b'"') {
                return Err((cursor, Issue::ParameterShape));
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
                            return Err((cursor, Issue::UnterminatedEscape));
                        };
                        let special = matches!(escaped, b'"' | b'\\' | b']');
                        if strict_escapes && !special {
                            return Err((cursor + 1, Issue::InvalidEscape));
                        }
                        cursor += if special { 2 } else { 1 };
                    }
                    Some(b']') => return Err((cursor, Issue::UnescapedClosingBracket)),
                    Some(_) => cursor += 1,
                    None => return Err((cursor, Issue::UnterminatedParameterValue)),
                }
            }
            if !matches!(bytes.get(cursor), Some(b' ') | Some(b']')) {
                return Err((cursor, Issue::ParameterDelimiter));
            }
        }
    }
    Ok(cursor)
}

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
fn headers_report_their_first_byte_outside_printable_us_ascii_in_any_block() {
    let long = "h".repeat(200);
    let cases = [
        ("edge-1", 255, Ok(())),
        ("", 255, Err((0, SyslogHeaderIssue::Empty))),
        (
            "123456789",
            8,
            Err((
                8,
                SyslogHeaderIssue::TooLong {
                    length: 9,
                    maximum: 8,
                },
            )),
        ),
        (
            "tab\there",
            255,
            Err((3, SyslogHeaderIssue::NonPrintableAscii)),
        ),
        (
            "del\u{7F}",
            255,
            Err((3, SyslogHeaderIssue::NonPrintableAscii)),
        ),
        (
            "h\u{e9}te",
            255,
            Err((1, SyslogHeaderIssue::NonPrintableAscii)),
        ),
        ("!~", 255, Ok(())),
    ];
    for (value, max_len, expected) in cases {
        assert_eq!(header_verdict(value, max_len), expected, "{value:?}");
    }
    for position in [63, 64, 65, 127, 128, 199] {
        let mut value = long.clone().into_bytes();
        value[position] = b' ';
        let value = String::from_utf8(value).expect("ASCII text");
        assert_eq!(
            header_verdict(&value, 255),
            Err((position, SyslogHeaderIssue::NonPrintableAscii))
        );
    }
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

#[test]
fn every_structured_data_issue_reports_its_byte() {
    use SyslogStructuredDataIssue as Issue;

    let long_id = "i".repeat(33);
    let long_name = format!("[id {}=\"v\"]", "n".repeat(33));
    let element_id_too_long = format!("[{long_id}]");
    let cases: [(&str, bool, Verdict); 23] = [
        ("-", true, Ok(1)),
        ("- message", true, Ok(1)),
        ("[id]", true, Ok(4)),
        ("[id a=\"1\" b=\"2\"][other] message", true, Ok(23)),
        ("[id a=\"esc\\\"\\\\\\]aped\"]", true, Ok(22)),
        ("[id a=\"loose\\n\"]", false, Ok(16)),
        ("", true, Err((0, Issue::MissingElement))),
        ("x", true, Err((0, Issue::MissingElement))),
        (
            "[i\u{e9}d]",
            true,
            Err((2, Issue::InvalidElementIdCharacter)),
        ),
        ("[]", true, Err((1, Issue::ElementIdLength { length: 0 }))),
        (
            &element_id_too_long,
            true,
            Err((1, Issue::ElementIdLength { length: 33 })),
        ),
        ("[id][id]", true, Err((5, Issue::DuplicateElementId))),
        ("[id", true, Err((3, Issue::UnterminatedElement))),
        (
            "[id a\"=\"v\"]",
            true,
            Err((5, Issue::InvalidParameterNameCharacter)),
        ),
        (
            "[id =\"v\"]",
            true,
            Err((4, Issue::ParameterNameLength { length: 0 })),
        ),
        (
            &long_name,
            true,
            Err((4, Issue::ParameterNameLength { length: 33 })),
        ),
        (
            "[id a=\"1\" a=\"2\"]",
            true,
            Err((10, Issue::DuplicateParameterName)),
        ),
        ("[id a=1]", true, Err((5, Issue::ParameterShape))),
        ("[id a=\"v\\", true, Err((8, Issue::UnterminatedEscape))),
        ("[id a=\"\\n\"]", true, Err((8, Issue::InvalidEscape))),
        (
            "[id a=\"v]\"]",
            true,
            Err((8, Issue::UnescapedClosingBracket)),
        ),
        (
            "[id a=\"open",
            true,
            Err((11, Issue::UnterminatedParameterValue)),
        ),
        ("[id a=\"v\"x]", true, Err((9, Issue::ParameterDelimiter))),
    ];
    for (value, strict_escapes, expected) in cases {
        assert_eq!(parsed(value, strict_escapes), expected, "{value:?}");
        assert_eq!(
            reference_structured_data(value, strict_escapes),
            expected,
            "the per-byte reading agrees: {value:?}"
        );
    }
    assert_eq!(
        parsed("[id a=\"1\"]x", true),
        Ok(10),
        "the caller decides what may follow the last element"
    );
    assert_eq!(parsed("[id a=\"v\"]", true), Ok(10));
    assert_eq!(
        parsed("[id abc", true),
        Err((7, Issue::ParameterShape)),
        "a PARAM-NAME that runs to the end has no value"
    );
    assert_eq!(
        parsed("[id x]", true),
        Err((5, Issue::InvalidParameterNameCharacter))
    );
}

#[test]
fn values_and_escapes_on_every_block_boundary_match_the_per_byte_reading() {
    for padding in 0..=140 {
        let value = "v".repeat(padding);
        for escape in ["\\\"", "\\\\", "\\]", "\\n", "]", "\"", "\\"] {
            for strict_escapes in [true, false] {
                let structured_data = format!("[id@1 p=\"{value}{escape}tail\" q=\"{value}\"] msg");
                assert_eq!(
                    parsed(&structured_data, strict_escapes),
                    reference_structured_data(&structured_data, strict_escapes),
                    "{structured_data:?} strict={strict_escapes}"
                );
            }
        }
    }
}

/// Characters that build structured data: its delimiters, name and value bytes, bytes no name
/// may hold, and multi-byte UTF-8.
const TOKENS: [&str; 18] = [
    "[",
    "]",
    " ",
    "=",
    "\"",
    "\\",
    "-",
    "a",
    "b",
    "@1",
    "id",
    "x=\"",
    "\"]",
    "\t",
    "\u{7F}",
    "\u{e9}",
    "\u{1F600}",
    "\u{0}",
];

#[test]
fn bolero_structured_data_and_headers_match_the_per_byte_reading() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(512)
        .with_type::<(Vec<(u8, u8)>, bool)>()
        .for_each(|(tokens, strict_escapes)| {
            // Token runs long enough to carry names and values across the scanners' blocks.
            let mut value = String::new();
            for (token, run) in tokens {
                let token = TOKENS[usize::from(*token) % TOKENS.len()];
                let run = if run % 4 == 0 {
                    usize::from(*run % 80)
                } else {
                    1
                };
                for _ in 0..run {
                    value.push_str(token);
                }
            }
            assert_eq!(
                parsed(&value, *strict_escapes),
                reference_structured_data(&value, *strict_escapes),
                "{value:?}"
            );
            for max_len in [32, 255, 1_024] {
                assert_eq!(
                    header_verdict(&value, max_len),
                    reference_header(&value, max_len),
                    "{value:?}"
                );
            }
        });
}
