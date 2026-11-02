//! Current ingestor instancing declarations and their serialization boundaries.
//!
//! Layer: test harness.
//! - **Owns.** Count bounds, partition choice and complete JSON/archive preservation.
//! - **Depends on.** The vocabulary's instancing declaration and its production encodings.
//! - **Must not know.** Placement allocation, connector drivers or runtime state.

use std::num::NonZeroU16;

use nervix_models::IngestorInstances;

#[test]
fn bolero_instancing_preserves_its_count_or_partition_form() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2)
        .for_each(|bytes: &[u8]| {
            let count = u16::from_le_bytes([
                bytes.first().copied().unwrap_or(0),
                bytes.get(1).copied().unwrap_or(0),
            ]);
            let count = NonZeroU16::new(count).unwrap_or(NonZeroU16::MIN);
            for declaration in [
                IngestorInstances::Count(count),
                IngestorInstances::Count(NonZeroU16::MIN),
                IngestorInstances::Count(NonZeroU16::MAX),
                IngestorInstances::PerPartition,
            ] {
                let json =
                    serde_json::to_vec(&declaration).expect("the current declaration serializes");
                assert_eq!(
                    serde_json::from_slice::<IngestorInstances>(&json)
                        .expect("the current JSON decodes"),
                    declaration
                );
                let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&declaration)
                    .expect("the current declaration archives");
                assert_eq!(
                    rkyv::from_bytes::<IngestorInstances, rkyv::rancor::Error>(&archive)
                        .expect("the current archive decodes"),
                    declaration
                );
            }
        });
}
