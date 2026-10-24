//! OTEL identifier acceptance and exact bytes against an independent scalar decoder.
//!
//! Layer: test harness.
//! - **Owns.** Bounded valid identifiers, arbitrary UTF-8 text and the per-digit reference.
//! - **Depends on.** The production identifier conversion and Bolero.
//! - **Must not know.** Export transport, endpoint resolution, retries or graph routing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the identifier harness declared above")
)]

use super::parse_hex_id;

#[derive(Debug, PartialEq, Eq)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(outcome, reason = "the oracle classifies semantic identifier refusals")
)]
enum HexRefusal {
    LengthOrDigit,
    AllZero,
}

impl HexRefusal {
    fn reason(&self, key: &str, byte_len: usize) -> String {
        match self {
            Self::LengthOrDigit => format!(
                "OTEL {key} must contain exactly {} hexadecimal characters",
                byte_len * 2
            ),
            Self::AllZero => format!("OTEL {key} cannot be all zeroes"),
        }
    }
}

struct IdentifierCase {
    bytes: [u8; 16],
    text: String,
}

impl IdentifierCase {
    fn draw(input: &[u8]) -> Self {
        let mut bytes = [0; 16];
        for (output, input) in bytes.iter_mut().zip(input) {
            *output = *input;
        }
        Self {
            bytes,
            text: String::from_utf8_lossy(input).into_owned(),
        }
    }

    fn valid_bytes(&self, byte_len: usize) -> Vec<u8> {
        let mut bytes = self.bytes[..byte_len].to_vec();
        if bytes.iter().all(|byte| *byte == 0) {
            bytes[0] = 1;
        }
        bytes
    }

    fn text(bytes: &[u8], letter_case: usize) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut text = String::new();
        for (position, byte) in bytes.iter().enumerate() {
            for nibble in [byte >> 4, byte & 15] {
                let mut digit = DIGITS[usize::from(nibble)];
                if letter_case == 1 || (letter_case == 2 && position % 2 == 0) {
                    digit = digit.to_ascii_uppercase();
                }
                text.push(char::from(digit));
            }
        }
        text
    }

    fn reference(value: &str, byte_len: usize) -> Result<Vec<u8>, HexRefusal> {
        let characters = value.as_bytes();
        if characters.len() != byte_len * 2 {
            return Err(HexRefusal::LengthOrDigit);
        }
        let mut bytes = Vec::new();
        for pair in characters.as_chunks::<2>().0 {
            let high = Self::digit(pair[0])?;
            let low = Self::digit(pair[1])?;
            bytes.push(high * 16 + low);
        }
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(HexRefusal::AllZero);
        }
        Ok(bytes)
    }

    fn digit(value: u8) -> Result<u8, HexRefusal> {
        match value {
            b'0'..=b'9' => Ok(value - b'0'),
            b'a'..=b'f' => Ok(value - b'a' + 10),
            b'A'..=b'F' => Ok(value - b'A' + 10),
            _ => Err(HexRefusal::LengthOrDigit),
        }
    }

    fn assert_agreement(text: &str, byte_len: usize, key: &str) {
        match (
            Self::reference(text, byte_len),
            parse_hex_id(text, byte_len, key),
        ) {
            (Ok(expected), Ok(actual)) => assert_eq!(actual, expected, "{key}: {text:?}"),
            (Err(expected), Err(actual)) => {
                assert_eq!(actual.current_context().key, key);
                assert_eq!(
                    actual.current_context().reason,
                    expected.reason(key, byte_len)
                );
            }
            (expected, actual) => {
                panic!("{key}: {text:?}: scalar={expected:?}, production={actual:?}")
            }
        }
    }
}

#[test]
fn bolero_hex_ids_match_the_scalar_reference() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(128)
        .for_each(|input: &[u8]| {
            let case = IdentifierCase::draw(input);
            for (byte_len, key) in [(16, "trace_id"), (8, "span_id"), (8, "parent_span_id")] {
                IdentifierCase::assert_agreement(&case.text, byte_len, key);
                let expected = case.valid_bytes(byte_len);
                for letter_case in 0..3 {
                    let text = IdentifierCase::text(&expected, letter_case);
                    let actual = parse_hex_id(&text, byte_len, key);
                    let Ok(actual) = actual else {
                        panic!("a generated nonzero identifier is valid: {actual:?}");
                    };
                    assert_eq!(actual, expected, "{key}: {text}");
                    IdentifierCase::assert_agreement(&text, byte_len, key);
                }
            }
        });
}
