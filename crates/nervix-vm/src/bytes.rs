//! Native BYTES transformations over Arrow binary columns.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Encoding, decoding, hashing and UTF-8 conversion of binary columns, including
//!   per-row failures and Arrow offset bounds.
//! - **Depends on.** Arrow columns, VM row errors and SIMD-backed encoding kernels.
//! - **Must not know.** NSPL syntax, routes, schemas or connectors.

use std::num::NonZeroUsize;

use arrow_array::{
    Array, BinaryArray, StringArray, UInt64Array,
    builder::{BinaryBuilder, StringBuilder, UInt64Builder},
};
use meticulous::OptionExt as _;
use sha2::{Digest as _, Sha256};

use crate::{
    RowErrors, SideError, SideErrorReason, error::BytesOperation, program::Span,
    text_column::TextColumnBuilder,
};

/// UTF-8 bytes and STRING have the same Arrow offsets and values buffers. Only their types differ.
pub(crate) fn from_utf8(input: &StringArray) -> BinaryArray {
    BinaryArray::new(
        input.offsets().clone(),
        input.values().clone(),
        input.nulls().cloned(),
    )
}

pub(crate) fn to_utf8(input: &BinaryArray, errors: &mut RowErrors, span: Span) -> StringArray {
    let mut output = StringBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        match std::str::from_utf8(input.value(row)) {
            Ok(value) => output.append_value(value),
            Err(_) => {
                output.append_null();
                errors.push(
                    row,
                    SideError {
                        reason: SideErrorReason::InvalidUtf8Bytes,
                        span,
                    },
                );
            }
        }
    }
    output.finish()
}

pub(crate) fn encode_base64(
    input: &BinaryArray,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> StringArray {
    let mut output = TextColumnBuilder::new(StringBuilder::new(), rows_per_value);
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        let value = input.value(row);
        let rounded_length = value.len().checked_add(2);
        let group_count = rounded_length.map(|length| length / 3);
        let encoded_length = match group_count {
            Some(groups) => groups.checked_mul(4),
            None => None,
        };
        if !encoded_length.is_some_and(|length| output.fits(length)) {
            output.append_null();
            errors.push(
                row,
                SideError {
                    reason: SideErrorReason::BytesTooLong(BytesOperation::Base64Encode),
                    span,
                },
            );
            continue;
        }
        let encoded = base64_simd::STANDARD.encode_to_string(value);
        output
            .append_value(&encoded)
            .then_some(())
            .verified("the base64 output length was checked against this column before encoding");
    }
    output.finish()
}

pub(crate) fn decode_base64(
    input: &StringArray,
    errors: &mut RowErrors,
    span: Span,
) -> BinaryArray {
    let mut output = BinaryBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        match base64_simd::STANDARD.decode_to_vec(input.value(row).as_bytes()) {
            Ok(decoded) => output.append_value(decoded),
            Err(_) => {
                output.append_null();
                errors.push(
                    row,
                    SideError {
                        reason: SideErrorReason::InvalidBytesEncoding(BytesOperation::Base64Decode),
                        span,
                    },
                );
            }
        }
    }
    output.finish()
}

pub(crate) fn encode_hex(
    input: &BinaryArray,
    rows_per_value: NonZeroUsize,
    errors: &mut RowErrors,
    span: Span,
) -> StringArray {
    let mut output = TextColumnBuilder::new(StringBuilder::new(), rows_per_value);
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        let value = input.value(row);
        let encoded_length = value.len().checked_mul(2);
        if !encoded_length.is_some_and(|length| output.fits(length)) {
            output.append_null();
            errors.push(
                row,
                SideError {
                    reason: SideErrorReason::BytesTooLong(BytesOperation::HexEncode),
                    span,
                },
            );
            continue;
        }
        let encoded = faster_hex::hex_string(value);
        output.append_value(&encoded).then_some(()).verified(
            "the hexadecimal output length was checked against this column before encoding",
        );
    }
    output.finish()
}

pub(crate) fn decode_hex(input: &StringArray, errors: &mut RowErrors, span: Span) -> BinaryArray {
    let mut output = BinaryBuilder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        let encoded = input.value(row).as_bytes();
        let decoded = if encoded.len().is_multiple_of(2) {
            let mut bytes = vec![0; encoded.len() / 2];
            if faster_hex::hex_decode(encoded, &mut bytes).is_ok() {
                Some(bytes)
            } else {
                None
            }
        } else {
            None
        };
        if let Some(decoded) = decoded {
            output.append_value(decoded);
        } else {
            output.append_null();
            errors.push(
                row,
                SideError {
                    reason: SideErrorReason::InvalidBytesEncoding(BytesOperation::HexDecode),
                    span,
                },
            );
        }
    }
    output.finish()
}

pub(crate) fn sha256(input: &BinaryArray, errors: &mut RowErrors, span: Span) -> BinaryArray {
    let mut output = BinaryBuilder::new();
    let mut used = 0usize;
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
            continue;
        }
        let Some(next) = used
            .checked_add(32)
            .filter(|total| i32::try_from(*total).is_ok())
        else {
            output.append_null();
            errors.push(
                row,
                SideError {
                    reason: SideErrorReason::BytesTooLong(BytesOperation::Sha256),
                    span,
                },
            );
            continue;
        };
        let digest = Sha256::digest(input.value(row));
        output.append_value(digest);
        used = next;
    }
    output.finish()
}

pub(crate) fn xxh3_64(input: &BinaryArray) -> UInt64Array {
    let mut output = UInt64Builder::new();
    for row in 0..input.len() {
        if input.is_null(row) {
            output.append_null();
        } else {
            output.append_value(xxhash_rust::xxh3::xxh3_64_with_seed(input.value(row), 0));
        }
    }
    output.finish()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use arrow_array::{BinaryArray, StringArray};

    use super::*;

    const SPAN: Span = Span { start: 0, end: 1 };

    #[test]
    fn binary_encodings_preserve_non_utf8_empty_and_null_values() {
        let input = BinaryArray::from(vec![Some(&[0, 255][..]), Some(&[][..]), None]);
        let mut errors = RowErrors::new(input.len());

        let hex = encode_hex(&input, NonZeroUsize::MIN, &mut errors, SPAN);
        assert_eq!(hex.value(0), "00ff");
        assert_eq!(hex.value(1), "");
        assert!(hex.is_null(2));
        assert_eq!(decode_hex(&hex, &mut errors, SPAN), input);

        let base64 = encode_base64(&input, NonZeroUsize::MIN, &mut errors, SPAN);
        assert_eq!(base64.value(0), "AP8=");
        assert_eq!(base64.value(1), "");
        assert!(base64.is_null(2));
        assert_eq!(decode_base64(&base64, &mut errors, SPAN), input);
        assert!(errors.is_error_free());
    }

    #[test]
    fn invalid_encodings_and_non_utf8_bytes_report_only_their_rows() {
        let hex = StringArray::from(vec![Some("ff"), Some("0"), Some("gg"), None]);
        let mut errors = RowErrors::new(hex.len());
        let decoded = decode_hex(&hex, &mut errors, SPAN);
        assert_eq!(decoded.value(0), &[255]);
        assert!(decoded.is_null(1));
        assert!(decoded.is_null(2));
        assert!(decoded.is_null(3));
        assert!(errors.row(0).is_empty());
        assert_eq!(
            errors.row(1)[0].reason,
            SideErrorReason::InvalidBytesEncoding(BytesOperation::HexDecode)
        );
        assert_eq!(
            errors.row(2)[0].reason,
            SideErrorReason::InvalidBytesEncoding(BytesOperation::HexDecode)
        );

        let base64 = StringArray::from(vec![Some("AP8="), Some("AP8"), Some("AP8!"), None]);
        let mut errors = RowErrors::new(base64.len());
        let decoded = decode_base64(&base64, &mut errors, SPAN);
        assert_eq!(decoded.value(0), &[0, 255]);
        assert!(decoded.is_null(1));
        assert!(decoded.is_null(2));
        assert!(errors.row(3).is_empty());

        let mut errors = RowErrors::new(decoded.len());
        let text = to_utf8(&decoded, &mut errors, SPAN);
        assert!(text.is_null(0));
        assert_eq!(errors.row(0)[0].reason, SideErrorReason::InvalidUtf8Bytes);
    }

    #[test]
    fn encoded_columns_report_arrow_offset_overflow_per_row() {
        let input = BinaryArray::from(vec![Some(&[1][..]), None]);
        let mut errors = RowErrors::new(input.len());
        let hex = encode_hex(&input, NonZeroUsize::MAX, &mut errors, SPAN);
        assert!(hex.is_null(0));
        assert!(hex.is_null(1));
        assert_eq!(
            errors.row(0)[0].reason,
            SideErrorReason::BytesTooLong(BytesOperation::HexEncode)
        );
        assert!(errors.row(1).is_empty());

        let mut errors = RowErrors::new(input.len());
        let base64 = encode_base64(&input, NonZeroUsize::MAX, &mut errors, SPAN);
        assert!(base64.is_null(0));
        assert!(base64.is_null(1));
        assert_eq!(
            errors.row(0)[0].reason,
            SideErrorReason::BytesTooLong(BytesOperation::Base64Encode)
        );
        assert!(errors.row(1).is_empty());
    }

    #[test]
    fn hashes_match_fixed_algorithm_vectors() {
        let input = BinaryArray::from(vec![Some(&b""[..]), Some(&b"abc"[..]), None]);
        let mut errors = RowErrors::new(input.len());
        let digest = sha256(&input, &mut errors, SPAN);
        let hex = encode_hex(&digest, NonZeroUsize::MIN, &mut errors, SPAN);
        assert_eq!(
            hex.value(0),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex.value(1),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(hex.is_null(2));

        let fast = xxh3_64(&input);
        assert_eq!(fast.value(0), 0x2d06800538d394c2);
        assert_eq!(fast.value(1), 0x78af5f94892f3950);
        assert!(fast.is_null(2));
        assert!(errors.is_error_free());
    }

    #[test]
    fn utf8_conversion_is_explicit_and_lossless() {
        let text = StringArray::from(vec![Some("snowman ☃"), Some(""), None]);
        let binary = from_utf8(&text);
        let mut errors = RowErrors::new(text.len());
        assert_eq!(to_utf8(&binary, &mut errors, SPAN), text);
        assert!(errors.is_error_free());
    }
}
