//! Exact Pulsar key encoding and SDK partition selection evidence.
//!
//! Layer: test harness.
//! - **Owns.** Logical key presence, value kind, byte equality and source-ordinal routing checks.
//! - **Depends on.** The production message mapper, routing callback and connector record contract.
//! - **Must not know.** Runtime branches, graph configuration or live brokers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns destination representation evidence")
)]

use nervix_connector::{EmitterRecordId, RecordDestination};
use nervix_primitives::sync::Arc;

use super::*;

fn check(key: Option<RecordKey>, source: u64, count: usize) {
    let destination = RecordDestination {
        key: key.clone(),
        source_partition: Some(source),
    };
    let record = EmitterRecord::new(
        EmitterRecordId::new(0),
        Some(Arc::new(destination)),
        vec![0, 255],
        Vec::new(),
        Timestamp::from_unix_nanos(1),
    );
    let message = PulsarTopicEmitter::message(record).expect("the record carries no invalid time");
    let logical_key = message.partition_key.as_ref().map(|key| {
        if message.partition_key_b64_encoded == Some(true) {
            STANDARD
                .decode(key)
                .expect("the maintained codec emitted base64")
        } else {
            key.as_bytes().to_vec()
        }
    });
    assert_eq!(
        logical_key.as_deref(),
        key.as_ref().map(RecordKey::as_bytes)
    );
    match key {
        Some(RecordKey::Bytes(_)) => assert_eq!(message.partition_key_b64_encoded, Some(true)),
        Some(RecordKey::String(value)) => {
            assert_eq!(message.partition_key.as_deref(), Some(value.as_str()));
            assert_eq!(message.partition_key_b64_encoded, None);
        }
        None => assert_eq!(message.partition_key_b64_encoded, None),
    }
    assert_eq!(message.payload, vec![0, 255]);
    let selected = Arc::new(AtomicU64::new(0));
    let policy = BranchPartitionRouting {
        selected: Arc::clone(&selected),
    };
    BranchPartitionRouting::select(Some(&selected), Some(source));
    let count_wide = u128::from(u64::try_from(count).expect("the bounded SDK count fits u64"));
    let expected =
        usize::try_from(u128::from(source) % count_wide).expect("a remainder fits usize");
    assert_eq!(policy.route(&message, count), expected);
}

#[test]
fn destination_keys_keep_absence_empty_values_utf8_and_binary_octets() {
    for key in [
        None,
        Some(RecordKey::String(String::new())),
        Some(RecordKey::String("café".to_owned())),
        Some(RecordKey::Bytes(Vec::new())),
        Some(RecordKey::Bytes(vec![0, 255, 16])),
    ] {
        for source in [0, 1, 3, u64::MAX] {
            for count in [1, 2, 3, 17] {
                check(key.clone(), source, count);
            }
        }
    }
}

#[test]
fn bolero_destinations_preserve_key_bytes_presence_and_partition_modulo() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(128)
        .with_type::<(u8, [u8; 64], u64, u8)>()
        .for_each(|(kind, bytes, source, partitions)| {
            let bytes = &bytes[..usize::from(*kind) % 65];
            let key = match kind % 3 {
                0 => None,
                1 => Some(RecordKey::String(
                    bytes.iter().copied().map(char::from).collect(),
                )),
                _ => Some(RecordKey::Bytes(bytes.to_vec())),
            };
            check(key, *source, usize::from(*partitions) + 1);
        });
}

#[test]
#[cfg(target_pointer_width = "64")]
fn the_driver_keeps_its_native_fixed_footprint() {
    assert!(std::mem::size_of::<PulsarTopicEmitter>() <= 504);
}
