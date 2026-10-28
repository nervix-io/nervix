//! What a record envelope holds and how a record carries it.
//!
//! Layer: test harness.
//! - **Owns.** Evidence that an envelope counts exactly the bytes of its variable-length values,
//!   and that a record carries the envelope the host gives it.
//! - **Depends on.** The emitter contract's record and envelope types.
//! - **Must not know.** Any connector, the host's evaluation of an envelope, or its retention.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns record envelope evidence")
)]

use super::*;

#[test]
fn an_envelope_holds_the_bytes_of_its_texts_and_variable_length_property_values() {
    let fixed_size = [
        PropertyValue::U8(1),
        PropertyValue::I8(-1),
        PropertyValue::U16(2),
        PropertyValue::I16(-2),
        PropertyValue::U32(3),
        PropertyValue::I32(-3),
        PropertyValue::U64(4),
        PropertyValue::I64(-4),
        PropertyValue::Bool(true),
        PropertyValue::Datetime(Timestamp::from_unix_nanos(5)),
    ];
    let mut properties = vec![
        RecordProperty {
            name: "content_type",
            value: PropertyValue::String("application/json".to_string()),
        },
        RecordProperty {
            name: "signature",
            value: PropertyValue::Bytes(vec![1, 2, 3]),
        },
    ];
    for value in fixed_size {
        properties.push(RecordProperty {
            name: "fixed",
            value,
        });
    }
    let envelope = RecordEnvelope {
        persistent: true,
        mandatory: true,
        retain: true,
        timestamp: Some(Timestamp::from_unix_nanos(6)),
        deliver_after: Some(Duration::from_secs(5)),
        delay: Some(Duration::from_secs(1)),
        reply_to: Some("replies".to_string()),
        message_id: Some("event-1".to_string()),
        deduplication_id: Some("dedup".to_string()),
        properties,
    };

    // "replies", "event-1" and "dedup", then "application/json" and three signature bytes. Flags,
    // durations, the timestamp and fixed-size values hold nothing beyond the envelope itself.
    assert_eq!(envelope.held_bytes(), 7 + 7 + 5 + 16 + 3);
    assert_eq!(RecordEnvelope::default().held_bytes(), 0);
}

#[test]
fn a_record_carries_the_envelope_the_host_evaluated_for_it() {
    let record = EmitterRecord::new(
        EmitterRecordId::new(0),
        None,
        b"payload".to_vec(),
        Vec::new(),
        Timestamp::from_unix_nanos(1),
    );
    assert_eq!(
        record.envelope,
        RecordEnvelope::default(),
        "a record of an emitter without envelope clauses carries the empty envelope"
    );

    let envelope = RecordEnvelope {
        persistent: true,
        message_id: Some("event-1".to_string()),
        ..RecordEnvelope::default()
    };
    let record = record.with_envelope(envelope.clone());

    assert_eq!(record.envelope, envelope);
    assert_eq!(record.payload, b"payload");
}
