//! Current ingestor instancing declarations and their serialization boundaries.
//!
//! Layer: test harness.
//! - **Owns.** Count bounds, partition choice and complete JSON/archive preservation.
//! - **Depends on.** The vocabulary's instancing declaration and its production encodings.
//! - **Must not know.** Placement allocation, connector drivers or runtime state.

use std::num::NonZeroU64;

use nervix_models::IngestorInstances;

#[test]
fn bolero_instancing_preserves_its_count_or_partition_form() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(8)
        .for_each(|bytes: &[u8]| {
            let count = u64::from_le_bytes(std::array::from_fn(|index| {
                bytes.get(index).copied().unwrap_or(0)
            }));
            let count = NonZeroU64::new(count).unwrap_or(NonZeroU64::MIN);
            for declaration in [
                IngestorInstances::Count(count),
                IngestorInstances::Count(NonZeroU64::MIN),
                IngestorInstances::Count(NonZeroU64::MAX),
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

#[test]
fn instancing_count_preserves_the_complete_positive_u64_range() {
    for count in [1_u64, 65_536, u64::from(u32::MAX) + 1, u64::MAX] {
        let json = format!("{{\"Count\":{count}}}");
        let declaration = serde_json::from_str::<IngestorInstances>(&json)
            .expect("every positive full-width instance count is a current declaration");
        assert_eq!(declaration.to_string(), count.to_string());
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&declaration)
            .expect("the current declaration archives");
        assert_eq!(
            rkyv::from_bytes::<IngestorInstances, rkyv::rancor::Error>(&archive)
                .expect("the current declaration decodes"),
            declaration,
        );
    }
}
