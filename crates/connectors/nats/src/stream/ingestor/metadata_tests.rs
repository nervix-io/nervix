//! NATS delivery metadata through its borrowed and retained production representations.
//!
//! Layer: test harness.
//! - **Owns.** Complete delivery metadata, ordered-header and malformed-input assertions.
//! - **Depends on.** The stream message owner, connector metadata retention and bounded entropy.
//! - **Must not know.** The graph, registry, placement or host scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the connector message boundary")
)]

use nervix_arbitrary::Entropy;
use nervix_connector::{
    RetainedIngestHeaders, RetainedIngestMetadata, RetainedIngestMetadataValue as Value,
};

use super::*;

fn metadata_delivery(reply: &str) -> NatsMessage {
    NatsMessage {
        subject: "orders.created".into(),
        reply: Some(reply.to_string().into()),
        payload: b"{}".as_slice().into(),
        headers: None,
        status: None,
        description: None,
        length: "orders.created".len() + reply.len() + 2,
    }
}

#[test]
fn an_opaque_acknowledgement_is_kept_before_metadata_is_requested() {
    let reply = "application.opaque.acknowledgement";
    let message = NatsStreamIngestorMessage::delivered(metadata_delivery(reply))
        .expect("a delivery retains its transport acknowledgement without reading metadata");
    assert_eq!(message.position.acknowledgement.as_str(), reply);
    assert_eq!(message.payload(), b"{}");
}

#[test]
fn stream_metadata_retains_the_complete_delivery_and_unsigned_boundaries() {
    let reply = "$JS.ACK._.account.orders.reader.18446744073709551615.18446744073709551615.3.\
                 1730000000000000000.0.token";
    let message = NatsStreamIngestorMessage::delivered(metadata_delivery(reply))
        .assured("the complete unsigned sequence and count are valid delivery metadata");
    assert_eq!(
        RetainedIngestMetadata::capture(message.metadata().fields).values(),
        &[
            Value::String("orders.created".to_string()),
            Value::String(reply.to_string()),
            Value::String("orders".to_string()),
            Value::String("reader".to_string()),
            Value::U64(u64::MAX),
            Value::Datetime(Timestamp::from_unix_nanos(1_730_000_000_000_000_000)),
            Value::U64(u64::MAX),
        ]
    );
}

#[test]
fn a_timestamp_field_failure_retains_its_typed_conversion_cause() {
    let reply = "$JS.ACK._.account.orders.reader.1.7.3.100000000000000000000.0.token";
    let message = NatsStreamIngestorMessage::delivered(metadata_delivery(reply)).unwrap();
    let retained = RetainedIngestMetadata::capture(message.metadata().fields);
    let Value::Invalid { ty, failure } = &retained.values()[5] else {
        panic!("the timestamp field retains its conversion failure");
    };
    assert_eq!(*ty, IngestorMetadataType::Datetime);
    assert!(
        failure
            .error()
            .source()
            .unwrap()
            .is::<std::num::TryFromIntError>()
    );
    assert_eq!(retained, RetainedIngestMetadata::capture(&retained));
}

#[test]
fn invalid_delivery_fields_keep_their_declared_types_and_causes() {
    for (reply, field) in [
        (
            "$JS.ACK._.account.orders.reader.0.7.3.1730000000000000000.0.token",
            6,
        ),
        (
            "$JS.ACK._.account.orders.reader.1.0.3.1730000000000000000.0.token",
            4,
        ),
        ("application.reply", 2),
    ] {
        let message = NatsStreamIngestorMessage::delivered(metadata_delivery(reply)).unwrap();
        let retained = RetainedIngestMetadata::capture(message.metadata().fields);
        let Value::Invalid { ty, failure } = &retained.values()[field] else {
            panic!("a requested malformed field carries a typed failure");
        };
        assert_eq!(
            *ty,
            if field == 2 {
                IngestorMetadataType::String
            } else {
                IngestorMetadataType::U64
            }
        );
        assert!(failure.error().is::<NatsStreamMetadataError>());
    }
}

#[test]
fn a_delivery_field_failure_keeps_the_drivers_typed_cause() {
    let reply = "$JS.ACK._.account.orders.reader.invalid.7.3.1730000000000000000.0.token";
    let message = NatsStreamIngestorMessage::delivered(metadata_delivery(reply)).unwrap();
    let retained = RetainedIngestMetadata::capture(message.metadata().fields);
    let Value::Invalid { failure, .. } = &retained.values()[6] else {
        panic!("the requested delivery count has a parsing failure");
    };
    let Some(NatsStreamMetadataError::DeliveryMetadata { source }) = failure.error().downcast_ref()
    else {
        panic!("the driver owns malformed delivery metadata");
    };
    assert!(source.is::<std::num::ParseIntError>());
}

/// A valid delivery retains all seven fields, its body, acknowledgement identity and the exact
/// header pairs after the transport message is gone. Names and values are bounded; numeric fields
/// deliberately exercise their complete representable ranges.
#[test]
fn bolero_delivery_metadata_retains_every_wire_field() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let mut entropy = Entropy::new(bytes);
            let subject = format!("orders.{}", entropy.byte());
            let stream = format!("stream-{}", entropy.byte());
            let consumer = format!("consumer-{}", entropy.byte());
            let delivered = entropy.boundary_biased(1..=u64::MAX);
            let sequence = entropy.boundary_biased(1..=u64::MAX);
            let timestamp = entropy.any_i64();
            let reply = format!(
                "$JS.ACK._.account.{stream}.{consumer}.{delivered}.{sequence}.1.{timestamp}.0.\
                 token"
            );
            let written = if entropy.flag() {
                vec![
                    ("first".to_string(), entropy.byte().to_string()),
                    ("second".to_string(), entropy.byte().to_string()),
                    ("first".to_string(), entropy.byte().to_string()),
                ]
            } else {
                Vec::new()
            };
            let mut raw = metadata_delivery(&reply);
            raw.subject = subject.clone().into();
            raw.headers = if written.is_empty() {
                None
            } else {
                Some(crate::subject::NatsSubjectEmitter::header_map(
                    written.clone(),
                ))
            };
            let header_bytes = if written.is_empty() {
                0
            } else {
                b"NATS/1.0\r\n\r\n".len()
                    + written
                        .iter()
                        .map(|(name, value)| name.len() + value.len() + 4)
                        .sum::<usize>()
            };
            raw.length = subject.len() + reply.len() + raw.payload.len() + header_bytes;
            let message = NatsStreamIngestorMessage::delivered(raw)
                .assured("the generated delivery is within every field's current range");
            let retained = RetainedIngestMetadata::capture(message.metadata().fields);
            let headers = RetainedIngestHeaders::capture(message.headers());
            assert_eq!(message.payload(), b"{}");
            assert_eq!(message.position().acknowledgement.as_str(), reply);
            drop(message);
            assert_eq!(
                retained.values(),
                &[
                    Value::String(subject),
                    Value::String(reply),
                    Value::String(stream),
                    Value::String(consumer),
                    Value::U64(sequence),
                    Value::Datetime(Timestamp::from_unix_nanos(timestamp)),
                    Value::U64(delivered),
                ]
            );
            let mut actual = Vec::new();
            headers.visit(&mut |name, value| actual.push((name.to_string(), value.to_string())));
            assert_eq!(actual, written);
        });
}
