//! What a record's envelope and typed headers hold and how a record carries them.
//!
//! Layer: test harness.
//! - **Owns.** Evidence that an envelope and a typed header count exactly the bytes of their
//!   variable-length values, that a property value states its exact type, and that a record
//!   carries the envelope and the typed header writes the host gives it.
//! - **Depends on.** The emitter contract's record, envelope and typed header types.
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

#[test]
fn a_record_carries_typed_header_writes_in_place_of_text_ones() {
    let record = EmitterRecord::new(
        EmitterRecordId::new(0),
        None,
        b"payload".to_vec(),
        Vec::new(),
        Timestamp::from_unix_nanos(1),
    );
    assert!(
        record.typed_headers.is_empty(),
        "a record of an emitter with text header values carries no typed writes"
    );

    let writes = vec![
        TypedRecordHeader {
            name: "tenant".to_string(),
            value: PropertyValue::String("acme".to_string()),
        },
        TypedRecordHeader {
            name: "user".to_string(),
            value: PropertyValue::I64(7),
        },
    ];
    let record = record.with_typed_headers(writes.clone());

    assert_eq!(record.typed_headers, writes);
    assert!(record.headers.is_empty());
}

#[test]
fn a_typed_header_holds_the_bytes_of_its_name_and_variable_length_value() {
    let header = |name: &str, value: PropertyValue| TypedRecordHeader {
        name: name.to_string(),
        value,
    };

    // "tenant" and "acme"; "user" and an I64, which holds nothing beyond its fixed size; "digest"
    // and three bytes.
    assert_eq!(
        header("tenant", PropertyValue::String("acme".to_string())).held_bytes(),
        6 + 4
    );
    assert_eq!(header("user", PropertyValue::I64(7)).held_bytes(), 4);
    assert_eq!(
        header("digest", PropertyValue::Bytes(vec![0, 255, 16])).held_bytes(),
        6 + 3
    );
}

#[test]
fn a_property_value_states_its_exact_type() {
    let cases = [
        (PropertyValue::U8(1), PropertyType::U8),
        (PropertyValue::I8(-1), PropertyType::I8),
        (PropertyValue::U16(2), PropertyType::U16),
        (PropertyValue::I16(-2), PropertyType::I16),
        (PropertyValue::U32(3), PropertyType::U32),
        (PropertyValue::I32(-3), PropertyType::I32),
        (PropertyValue::U64(4), PropertyType::U64),
        (PropertyValue::I64(-4), PropertyType::I64),
        (PropertyValue::Bool(true), PropertyType::Bool),
        (
            PropertyValue::String("text".to_string()),
            PropertyType::String,
        ),
        (
            PropertyValue::Datetime(Timestamp::from_unix_nanos(5)),
            PropertyType::Datetime,
        ),
        (PropertyValue::Bytes(vec![1]), PropertyType::Bytes),
    ];
    for (value, ty) in cases {
        assert_eq!(value.ty(), ty, "{value:?}");
    }
}
