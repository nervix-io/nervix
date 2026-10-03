//! XML character data qualification against the standard library at every SIMD level.
//!
//! Layer: test harness.
//! - **Owns.** The `Char` production's edges, UTF-8 failures and their precedence, block and span
//!   boundaries, and the differential property against standard UTF-8 decoding.
//! - **Depends on.** The production XML character check and the supported SIMD levels.
//! - **Must not know.** Which service or format requires XML characters.

use super::*;
use crate::supported_levels;

/// Whether the XML 1.0 `Char` production admits `character`, decoded by the standard library.
fn admitted(character: char) -> bool {
    matches!(character, '\t' | '\n' | '\r')
        || matches!(
            u32::from(character),
            0x20..=0xD7FF | 0xE000..=0xFFFD | 0x1_0000..=0x10_FFFF
        )
}

fn scalar_verdict(bytes: &[u8]) -> XmlCharsVerdict {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return XmlCharsVerdict::InvalidUtf8;
    };
    if text.chars().all(admitted) {
        XmlCharsVerdict::Admitted
    } else {
        XmlCharsVerdict::Excluded
    }
}

fn check(bytes: &[u8]) {
    let expected = scalar_verdict(bytes);
    for level in supported_levels() {
        assert_eq!(
            verdict(level, bytes),
            expected,
            "level={level:?} bytes={bytes:?}"
        );
        if let Ok(text) = std::str::from_utf8(bytes) {
            assert_eq!(
                admits_with_level(level, text),
                expected == XmlCharsVerdict::Admitted,
                "level={level:?} text={text:?}"
            );
        }
    }
}

/// Characters on both sides of every edge of the production, in each UTF-8 length.
const EDGE_CHARACTERS: [char; 22] = [
    '\u{0}',
    '\u{8}',
    '\t',
    '\n',
    '\u{B}',
    '\u{C}',
    '\r',
    '\u{E}',
    '\u{1F}',
    ' ',
    '\u{7F}',
    '\u{80}',
    '\u{9F}',
    '\u{D7FF}',
    '\u{E000}',
    '\u{FDD0}',
    '\u{FFFD}',
    '\u{FFFE}',
    '\u{FFFF}',
    '\u{1_0000}',
    '\u{1_FFFE}',
    '\u{10_FFFF}',
];

/// Byte sequences that are not UTF-8: a lone continuation byte, an overlong encoding, a UTF-16
/// surrogate, a code point above U+10FFFF, a truncated sequence and bytes that never occur.
const INVALID_SEQUENCES: [&[u8]; 8] = [
    &[0x80],
    &[0xC0, 0x80],
    &[0xED, 0xA0, 0x80],
    &[0xF4, 0x90, 0x80, 0x80],
    &[0xEF, 0xBF],
    &[0xF0, 0x9F, 0x98],
    &[0xFE],
    &[0xFF],
];

fn utf8(character: char) -> Vec<u8> {
    let mut encoded = [0_u8; 4];
    character.encode_utf8(&mut encoded).as_bytes().to_vec()
}

/// `fragment` inside ASCII text of `length` bytes, starting at `position`.
fn placed(fragment: &[u8], position: usize, length: usize) -> Vec<u8> {
    let mut bytes = vec![b'x'; position];
    bytes.extend_from_slice(fragment);
    if bytes.len() < length {
        bytes.resize(length, b'x');
    }
    bytes
}

#[test]
fn every_edge_character_at_every_block_offset_matches_standard_decoding() {
    for character in EDGE_CHARACTERS {
        let encoded = utf8(character);
        for position in 0..=2 * WORD_LANES + 2 {
            check(&placed(&encoded, position, 2 * WORD_LANES + 8));
            check(&placed(&encoded, position, position + encoded.len()));
        }
    }
}

#[test]
fn characters_across_span_boundaries_match_standard_decoding() {
    let length = 2 * SPAN + 100;
    for character in EDGE_CHARACTERS {
        let encoded = utf8(character);
        for boundary in [SPAN, 2 * SPAN] {
            for position in boundary - 4..=boundary + 1 {
                check(&placed(&encoded, position, length));
            }
        }
    }
    for sequence in INVALID_SEQUENCES {
        for position in SPAN - 4..=SPAN + 1 {
            check(&placed(sequence, position, length));
        }
    }
}

#[test]
fn invalid_utf8_matches_standard_decoding_at_every_block_offset() {
    for sequence in INVALID_SEQUENCES {
        for position in 0..=WORD_LANES + 2 {
            check(&placed(sequence, position, WORD_LANES + 8));
            check(&placed(sequence, position, position + sequence.len()));
        }
    }
}

#[test]
fn invalid_utf8_outranks_an_excluded_character_anywhere_in_the_bytes() {
    let length = 3 * SPAN;
    let excluded = utf8('\u{FFFF}');
    for (excluded_at, invalid_at) in [(10, 2 * SPAN + 5), (2 * SPAN + 5, 10), (10, 70)] {
        let mut bytes = placed(&excluded, excluded_at, length);
        bytes[invalid_at] = 0xFF;
        for level in supported_levels() {
            assert_eq!(verdict(level, &bytes), XmlCharsVerdict::InvalidUtf8);
        }
        let report = XmlChars::into_string(bytes).expect_err("the bytes are not UTF-8");
        assert_eq!(report.current_context(), &XmlCharsError::InvalidUtf8);
    }
}

#[test]
fn admitted_bytes_become_the_same_text() {
    let text = "{\"note\":\"tab\tline\nreturn\r \u{7F} \u{D7FF} \u{E000} \u{FFFD} \u{1F600}\"}";
    let owned =
        XmlChars::into_string(text.as_bytes().to_vec()).expect("every character is admitted");
    assert_eq!(owned, text);
    assert_eq!(
        XmlChars::into_string(Vec::new()).expect("empty text has no character to exclude"),
        ""
    );
    let excluded = XmlChars::into_string("before \u{FFFE} after".as_bytes().to_vec())
        .expect_err("U+FFFE is a noncharacter the production excludes");
    assert_eq!(
        excluded.current_context(),
        &XmlCharsError::ExcludedCharacter
    );
    assert!(XmlChars::admits("tenant-\u{10_FFFF}"));
    assert!(!XmlChars::admits("tenant\u{0}"));
}

#[test]
fn bolero_xml_chars_match_standard_decoding_at_every_level() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1_024)
        .with_type::<(Vec<(u8, u32)>, u16)>()
        .for_each(|(fragments, filler)| {
            // Mostly characters, so most generated bytes are UTF-8 whose verdict depends on the
            // `Char` production, with raw bytes among them to reach invalid sequences.
            let mut bytes = Vec::new();
            for (selector, value) in fragments {
                match selector % 16 {
                    0 => bytes.push(value.to_le_bytes()[0]),
                    1..=4 => {
                        let edge = EDGE_CHARACTERS[usize::from(*selector) % EDGE_CHARACTERS.len()];
                        bytes.extend_from_slice(&utf8(edge));
                    }
                    5 => {
                        if let Some(character) = char::from_u32(*value % 0x11_0000) {
                            bytes.extend_from_slice(&utf8(character));
                        }
                    }
                    _ => {
                        let run = usize::from(*filler % 128);
                        bytes.extend(std::iter::repeat_n(b'x', run));
                        bytes.push(value.to_le_bytes()[1] % 0x80);
                    }
                }
            }
            check(&bytes);
        });
}
