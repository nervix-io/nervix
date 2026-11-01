//! Complete SQS system metadata projection and numeric boundary checks.
//!
//! Layer: test harness.
//! - **Owns.** Bounded SDK messages, their complete typed row oracle, and malformed numeric cases.
//! - **Depends on.** The production SQS message/read owners and the shared retained metadata API.
//! - **Must not know.** Graph routing, placement, persistence or a copied projection algorithm.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the SQS metadata oracle")
)]

use std::num::ParseIntError;

use meticulous::ResultExt as _;
use nervix_connector::{RetainedIngestMetadata, RetainedIngestMetadataValue as Value};

use super::*;

/// The fixed conversion between the service's milliseconds and the internal nanoseconds.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "one Unix millisecond is exactly one million internal nanoseconds"
    )
)]
const NANOS_PER_MILLISECOND: i64 = 1_000_000;

struct MetadataCase {
    message: SqsMessage,
    values: [Value; 8],
    header: String,
}

impl MetadataCase {
    fn new(input: &[u8; 64]) -> Self {
        let mut values = std::array::from_fn(|_| Value::Absent);
        let header = format!("trace-{:02x}", input[63]);
        let attribute = MessageAttributeValue::builder()
            .data_type("String")
            .string_value(&header)
            .build()
            .assured("the generated header supplies the required type and a string value");
        let mut message = SqsMessage::builder()
            .body("{\"marker\":\"metadata\"}")
            .receipt_handle("receipt-1")
            .message_attributes("trace", attribute);
        if input[0] & 1 != 0 {
            let id = format!("message-{:02x}", input[40]);
            message = message.message_id(&id);
            values[0] = Value::String(id);
        }
        if input[0] & 2 != 0 {
            let milliseconds = Self::milliseconds(&input[8..16]);
            message = message.attributes(
                MessageSystemAttributeName::SentTimestamp,
                milliseconds.to_string(),
            );
            values[1] = Value::Datetime(Self::timestamp(milliseconds));
        }
        if input[0] & 4 != 0 {
            let bytes = input[1..5].try_into().assured("this slice has four bytes");
            let count = u32::from_le_bytes(bytes);
            message = message.attributes(
                MessageSystemAttributeName::ApproximateReceiveCount,
                count.to_string(),
            );
            values[2] = Value::U32(count);
        }
        if input[0] & 8 != 0 {
            let milliseconds = Self::milliseconds(&input[16..24]);
            message = message.attributes(
                MessageSystemAttributeName::ApproximateFirstReceiveTimestamp,
                milliseconds.to_string(),
            );
            values[3] = Value::Datetime(Self::timestamp(milliseconds));
        }
        if input[0] & 16 != 0 {
            let group = format!("group-{:02x}", input[41]);
            message = message.attributes(MessageSystemAttributeName::MessageGroupId, &group);
            values[4] = Value::String(group);
        }
        if input[0] & 32 != 0 {
            let deduplication = format!("dedup-{:02x}", input[42]);
            message = message.attributes(
                MessageSystemAttributeName::MessageDeduplicationId,
                &deduplication,
            );
            values[5] = Value::String(deduplication);
        }
        if input[0] & 64 != 0 {
            let bytes = input[24..40]
                .try_into()
                .assured("this slice has sixteen bytes");
            let sequence = u128::from_le_bytes(bytes).to_string();
            message = message.attributes(MessageSystemAttributeName::SequenceNumber, &sequence);
            values[6] = Value::String(sequence);
        }
        if input[0] & 128 != 0 {
            let sender = format!("sender-{:02x}", input[43]);
            message = message.attributes(MessageSystemAttributeName::SenderId, &sender);
            values[7] = Value::String(sender);
        }
        Self {
            message: message.build(),
            values,
            header,
        }
    }

    fn milliseconds(bytes: &[u8]) -> i64 {
        let bytes = bytes
            .try_into()
            .assured("both callers supply an eight-byte slice");
        i64::from_le_bytes(bytes) / NANOS_PER_MILLISECOND
    }

    fn timestamp(milliseconds: i64) -> Timestamp {
        let nanos = milliseconds
            .checked_mul(NANOS_PER_MILLISECOND)
            .assured("the test takes whole milliseconds from a representable nanosecond count");
        Timestamp::from_unix_nanos(nanos)
    }

    fn check(self) {
        let message = SqsQueueIngestorMessage::new(self.message).assured(
            "the generated count and whole-millisecond timestamps fit their declared types",
        );
        let row = message.metadata();
        let retained = RetainedIngestMetadata::capture(row.fields);
        assert_eq!(retained.values(), self.values);
        assert_eq!(RetainedIngestMetadata::capture(&retained), retained);
        let fields = SqsQueueIngestor::KIND.capabilities().metadata.fields();
        assert_eq!(retained.values().len(), fields.len());
        for (value, field) in retained.values().iter().zip(fields) {
            if let Some(ty) = value.ty() {
                assert_eq!(ty, field.ty(), "{}", field.name());
            }
        }
        let mut headers = Vec::new();
        row.headers
            .headers()
            .visit(&mut |name, value| headers.push((name.to_string(), value.to_string())));
        assert_eq!(headers, [("trace".to_string(), self.header)]);
        assert_eq!(message.payload(), b"{\"marker\":\"metadata\"}");
        assert_eq!(
            message.position().receipt_handle.as_deref(),
            Some("receipt-1")
        );
        assert!(row.position.is_none());
    }
}

#[test]
fn bolero_sqs_system_metadata_projects_and_retains_every_field() {
    bolero::check!()
        .with_type::<[u8; 64]>()
        .for_each(|input| MetadataCase::new(input).check());
}

#[test]
fn system_metadata_preserves_empty_strings_zero_counts_and_millisecond_boundaries() {
    for milliseconds in [
        i64::MIN / NANOS_PER_MILLISECOND,
        -1,
        0,
        1,
        i64::MAX / NANOS_PER_MILLISECOND,
    ] {
        for count in [0, 1, u32::MAX] {
            let message = SqsMessage::builder()
                .message_id("")
                .receipt_handle("receipt-1")
                .attributes(
                    MessageSystemAttributeName::SentTimestamp,
                    milliseconds.to_string(),
                )
                .attributes(
                    MessageSystemAttributeName::ApproximateReceiveCount,
                    count.to_string(),
                )
                .attributes(
                    MessageSystemAttributeName::ApproximateFirstReceiveTimestamp,
                    milliseconds.to_string(),
                )
                .attributes(MessageSystemAttributeName::MessageGroupId, "")
                .attributes(MessageSystemAttributeName::MessageDeduplicationId, "")
                .attributes(
                    MessageSystemAttributeName::SequenceNumber,
                    "000340282366920938463463374607431768211455",
                )
                .attributes(MessageSystemAttributeName::SenderId, "")
                .build();
            let message = SqsQueueIngestorMessage::new(message).assured(
                "the test's whole-millisecond boundaries and u32 counts are representable",
            );
            let retained = RetainedIngestMetadata::capture(message.metadata().fields);
            assert_eq!(
                retained.values(),
                [
                    Value::String(String::new()),
                    Value::Datetime(MetadataCase::timestamp(milliseconds)),
                    Value::U32(count),
                    Value::Datetime(MetadataCase::timestamp(milliseconds)),
                    Value::String(String::new()),
                    Value::String(String::new()),
                    Value::String("000340282366920938463463374607431768211455".to_string()),
                    Value::String(String::new()),
                ]
            );
        }
    }
}

#[test]
fn malformed_system_attribute_numbers_keep_their_typed_parse_cause() {
    for attribute in [
        MessageSystemAttributeName::SentTimestamp,
        MessageSystemAttributeName::ApproximateFirstReceiveTimestamp,
        MessageSystemAttributeName::ApproximateReceiveCount,
    ] {
        for value in ["", "sensitive-system-text", "18446744073709551616"] {
            let message = SqsMessage::builder()
                .receipt_handle("receipt-1")
                .attributes(attribute.clone(), value)
                .build();
            let error = match SqsQueueIngestorMessage::new(message) {
                Ok(_) => panic!("a malformed system number must fail at the transport boundary"),
                Err(error) => error,
            };
            assert!(
                matches!(error.current_context(), SqsQueueIngestorError::SystemAttribute { attribute: found } if found == &attribute)
            );
            assert!(error.downcast_ref::<ParseIntError>().is_some(), "{error:?}");
            let rendered = format!("{error:#}");
            assert!(rendered.contains(attribute.as_str()), "{rendered}");
            if !value.is_empty() {
                assert!(!rendered.contains(value), "{rendered}");
            }
        }
    }
}

#[test]
fn system_timestamps_outside_the_nanosecond_range_keep_the_typed_range_cause() {
    for attribute in [
        MessageSystemAttributeName::SentTimestamp,
        MessageSystemAttributeName::ApproximateFirstReceiveTimestamp,
    ] {
        for milliseconds in [
            i64::MIN,
            i64::MAX,
            (i64::MIN / NANOS_PER_MILLISECOND) - 1,
            (i64::MAX / NANOS_PER_MILLISECOND) + 1,
        ] {
            let message = SqsMessage::builder()
                .receipt_handle("receipt-1")
                .attributes(attribute.clone(), milliseconds.to_string())
                .build();
            let error = match SqsQueueIngestorMessage::new(message) {
                Ok(_) => panic!("a millisecond count outside Timestamp's range must fail"),
                Err(error) => error,
            };
            assert!(
                matches!(error.current_context(), SqsQueueIngestorError::SystemAttribute { attribute: found } if found == &attribute)
            );
            assert!(
                error
                    .downcast_ref::<nervix_models::TimestampError>()
                    .is_some(),
                "{error:?}"
            );
        }
    }
}

struct MalformedMessage;

impl QueueReceive for MalformedMessage {
    async fn receive(&self) -> SqsQueueIngestorResult<Vec<SqsMessage>> {
        Ok(vec![
            SqsMessage::builder()
                .receipt_handle("receipt-1")
                .attributes(
                    MessageSystemAttributeName::ApproximateReceiveCount,
                    "4294967296",
                )
                .build(),
        ])
    }
}

#[nervix_primitives::test]
async fn a_system_metadata_refusal_is_a_read_failure_with_its_exact_parse_cause() {
    let result =
        SqsQueueIngestor::read_batch(&MalformedMessage, &IngestorReadCancellation::new()).await;
    let error = match result {
        Ok(_) => panic!("a receive count above u32::MAX must fail the read"),
        Err(error) => error,
    };
    assert_eq!(
        error.current_context(),
        &IngestorError::Read { connector: SQS }
    );
    assert!(matches!(
        error.downcast_ref::<SqsQueueIngestorError>(),
        Some(SqsQueueIngestorError::SystemAttribute {
            attribute: MessageSystemAttributeName::ApproximateReceiveCount,
        })
    ));
    let Some(parse) = error.downcast_ref::<ParseIntError>() else {
        panic!("the production read must preserve its parser's cause: {error:?}");
    };
    assert_eq!(parse.kind(), &std::num::IntErrorKind::PosOverflow);
}
