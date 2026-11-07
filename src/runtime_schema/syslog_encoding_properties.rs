//! Generated complete SYSLOG encodings and their exact bounded sizes.
//!
//! Layer: test harness.
//! - **Owns.** Complete byte and independent-parser oracles, canonical Arrow round trips,
//!   ordered batch membership, header compatibility and every row/member/frame size boundary.
//! - **Depends on.** The SYSLOG fixture and the production row and batch encoders.
//! - **Must not know.** Connector transport, emitter scheduling or acknowledgement state.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use super::*;

impl CodecFixture {
    fn assert_round_trip(&self, record: &Record, encoded: &[u8]) {
        assert_eq!(encoded, record.expected());
        record.assert_reference(encoded);
        let decoded = self.decode(encoded);
        if let Some(canonical) = record.canonical() {
            let decoded = decoded.assured("a representable canonical record decodes");
            let expected = self.batch(&[canonical]);
            assert_eq!(
                decoded.batch(),
                expected.batch(),
                "every schema, value and null must round trip"
            );
        } else {
            // Truncating the first representable Arrow nanosecond down to a microsecond can produce
            // a valid RFC timestamp just outside Arrow's i64 range; the decode boundary must refuse it.
            let error = decoded.expect_err("the canonical instant lies outside Arrow's range");
            assert!(matches!(
                error.downcast_ref::<RuntimeSchemaError>(),
                Some(RuntimeSchemaError::SyslogTimestampOutOfRange)
            ));
        }
    }
}

#[test]
fn bolero_syslog_messages_preserve_complete_fields_and_wire_bytes() {
    bolero::check!()
        .with_iterations(CASES)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let fixture = CodecFixture::new();
            let record = Record::draw(&mut Entropy::new(input));
            let batch = fixture.batch(std::slice::from_ref(&record));
            let encoded = fixture.encode(&batch, 0);
            fixture.assert_round_trip(&record, &encoded);
        });
}

fn limits(entropy: &mut Entropy<'_>, length: usize) -> [u64; 5] {
    let length = u64::try_from(length).assured("bounded fixture lengths fit u64");
    let below = length
        .checked_sub(1)
        .assured("an RFC 5424 encoding is longer than one byte");
    let above = length
        .checked_add(1)
        .assured("bounded fixture lengths fit u64");
    [1, below, length, above, entropy.between(1..=above)]
}

impl CodecFixture {
    fn assert_row_limits(&self, record: &Record, entropy: &mut Entropy<'_>) {
        let batch = self.batch(std::slice::from_ref(record));
        let expected = record.expected();
        let encoder = self
            .codec
            .batch_encoder(&batch)
            .assured("the fixture matches the codec");
        for bytes in limits(entropy, expected.len()) {
            let bound = limit(bytes);
            let fits =
                u64::try_from(expected.len()).assured("the bounded fixture fits u64") <= bytes;
            let bounded = encoder
                .encode_row_within(0, bound)
                .assured("the record is valid");
            match bounded {
                BoundedRowEncoding::Encoded(encoded) => {
                    assert!(fits);
                    assert_eq!(encoded, expected);
                }
                BoundedRowEncoding::Oversize(exceeded) => {
                    assert!(!fits);
                    assert_eq!(exceeded.limit, bound);
                }
            }
            let member = encoder
                .batch_member(0, bound)
                .assured("the record is valid");
            match member {
                BatchMemberEncoding::Member(_) => assert!(fits),
                BatchMemberEncoding::Oversize(exceeded) => {
                    assert!(!fits);
                    assert_eq!(exceeded.limit, bound);
                }
            }
        }
    }
}

#[test]
fn bolero_syslog_rows_members_and_frames_respect_exact_byte_limits() {
    bolero::check!()
        .with_iterations(CASES)
        .with_max_len(CASE_BYTES)
        .for_each(|input| {
            let fixture = CodecFixture::new();
            let mut entropy = Entropy::new(input);
            let count = entropy
                .count(3)
                .checked_add(1)
                .assured("one to four members are drawn");
            let first = Record::draw(&mut entropy);
            let mut records = Vec::new();
            let mut messages = Vec::new();
            for index in 0..count {
                let mut record = first.clone();
                if index > 0 {
                    record.timestamp = if entropy.flag() {
                        Some(entropy.any_i64())
                    } else {
                        None
                    };
                    record.message = format!("member {index} {} end", Record::text(&mut entropy));
                }
                fixture.assert_row_limits(&record, &mut entropy);
                let encoded = record.expected();
                messages.push(String::from_utf8(encoded).assured("the expected message is UTF-8"));
                records.push(record);
            }
            let batch = fixture.batch(&records);
            let members = fixture.members(&batch);
            for member in &members {
                assert!(
                    members[0].shares_container_with(member),
                    "timestamps and messages do not split a header run"
                );
            }
            let references: Vec<_> = members.iter().collect();
            let mut expected = first.prefix().into_bytes();
            serde_json::to_writer(&mut expected, &messages).assured("fixture messages serialize");
            let mut envelope = first.clone();
            envelope.message =
                serde_json::to_string(&messages).assured("fixture messages serialize");
            envelope.assert_reference(&expected);
            for bytes in limits(&mut entropy, expected.len()) {
                let fits =
                    u64::try_from(expected.len()).assured("the bounded fixture fits u64") <= bytes;
                let bounded = fixture
                    .codec
                    .encode_batch_within(&references, limit(bytes))
                    .assured("members with one header form a valid frame");
                match bounded {
                    BoundedBatchEncoding::Encoded(encoded) => {
                        assert!(fits);
                        assert_eq!(encoded, expected);
                    }
                    BoundedBatchEncoding::Oversize(exceeded) => {
                        assert!(!fits);
                        assert_eq!(exceeded.limit, limit(bytes));
                    }
                }
            }
            let mut different = first.clone();
            different.severity = first
                .severity
                .checked_add(1)
                .assured("a severity below eight can be incremented in u8")
                % 8;
            let different_batch = fixture.batch(&[different]);
            let different = fixture.members(&different_batch);
            assert!(!members[0].shares_container_with(&different[0]));
        });
}

#[test]
fn bolero_syslog_malformed_messages_do_not_publish_partial_rows() {
    bolero::check!()
        .with_iterations(CASES)
        .with_max_len(512)
        .for_each(|input| {
            let fixture = CodecFixture::new();
            let mut record = Record::minimal();
            record.message = "the preceding complete row".to_string();
            let expected = fixture.batch(std::slice::from_ref(&record));
            let valid = record.expected();
            let mut builder = fixture.codec.schema.batch_builder(2);
            let mut decoder = JsonDecoder::default();
            let count = decode_with_codec(&fixture.codec, &valid, &mut decoder, &mut builder)
                .assured("the preceding record is valid");
            assert_eq!(count, 1);
            // The fixed head selects the RFC 5424 parser even when the generated tail is not UTF-8.
            let mut payload = b"<34>1 ".to_vec();
            payload.extend_from_slice(input);
            let outcome = decode_with_codec(&fixture.codec, &payload, &mut decoder, &mut builder);
            let expected_rows = match outcome {
                Ok(count) => {
                    assert_eq!(count, 1);
                    2
                }
                Err(error) => {
                    assert!(matches!(
                        error.current_context(),
                        CodecError::SyslogDecode { .. }
                    ));
                    1
                }
            };
            let batch = builder
                .finish()
                .assured("a failed decode abandons its partial row");
            assert_eq!(batch.batch().num_rows(), expected_rows);
            assert_eq!(&batch.batch().slice(0, 1), expected.batch());
        });
}
