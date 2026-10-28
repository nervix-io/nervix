//! Complete OTLP header-value decoding through the production configuration parser.
//!
//! Layer: test harness.
//! - **Owns.** Bounded encoded pairs and their complete decoded value and order oracle.
//! - **Depends on.** The OTLP configuration owner and Bolero.
//! - **Must not know.** Requests, connection lifecycle, retries or graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "checks the OTLP header configuration representation"
    )
)]

use std::fmt::Write as _;

use meticulous::ResultExt as _;

use super::{HeaderPairsError, PercentEncodedHeaders};

#[test]
fn decoding_preserves_escaped_whitespace_plus_equals_and_invalid_utf8_replacement() {
    let pairs = PercentEncodedHeaders::parse(" Example = %20a+b%2Bc%3Dd%2C%2520%20,example=%FF ")
        .assured("both entries have complete names and values")
        .into_pairs();
    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0].name(), "example");
    assert_eq!(pairs[0].value(), " a+b+c=d,%20 ");
    assert_eq!(pairs[1].name(), "example");
    assert_eq!(pairs[1].value(), "\u{fffd}");
    assert!(
        PercentEncodedHeaders::parse(" \t ")
            .assured("empty headers are optional")
            .into_pairs()
            .is_empty()
    );
    for (raw, expected) in [
        ("missing-equals", HeaderPairsError::Entry),
        (" =secret", HeaderPairsError::Name),
    ] {
        assert_eq!(
            PercentEncodedHeaders::parse(raw)
                .expect_err("the header shape is incomplete")
                .current_context(),
            &expected
        );
    }
}

struct HeaderCase {
    encoded: String,
    expected: Vec<ExpectedHeader>,
}

#[derive(Debug, PartialEq, Eq)]
struct ExpectedHeader {
    name: String,
    value: String,
}

impl HeaderCase {
    fn draw(input: &[u8]) -> Self {
        let mut encoded = String::new();
        let mut expected = Vec::new();
        for (index, chunk) in input.chunks(32).take(4).enumerate() {
            if index > 0 {
                encoded.push(',');
            }
            write!(&mut encoded, " X-Header-{index} = ")
                .assured("formatting into a String cannot fail");
            let value = String::from_utf8_lossy(chunk).into_owned();
            for byte in value.as_bytes() {
                write!(&mut encoded, "%{byte:02X}").assured("formatting into a String cannot fail");
            }
            encoded.push(' ');
            expected.push(ExpectedHeader {
                name: format!("x-header-{index}"),
                value,
            });
        }
        Self { encoded, expected }
    }
}

#[test]
fn bolero_percent_encoded_headers_decode_to_complete_pairs() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(128)
        .for_each(|input: &[u8]| {
            let case = HeaderCase::draw(input);
            let actual = PercentEncodedHeaders::parse(&case.encoded)
                .assured("generated pairs have names and fully percent-encoded UTF-8 values");
            let actual: Vec<_> = actual
                .into_pairs()
                .into_iter()
                .map(|pair| ExpectedHeader {
                    name: pair.name().to_owned(),
                    value: pair.value().to_owned(),
                })
                .collect();
            assert_eq!(actual, case.expected);
        });
}
