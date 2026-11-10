//! Emitter instancing declarations and their serialization boundaries.
//!
//! Layer: test harness.
//! - **Owns.** The full positive count range, the partition form and complete JSON/archive
//!   preservation of an emitter's `INSTANCES` declaration.
//! - **Depends on.** The vocabulary's emitter instancing declaration and its production encodings.
//! - **Must not know.** Placement allocation, connector drivers or runtime state.

use std::num::NonZeroU64;

use nervix_models::EmitterInstances;

#[test]
fn bolero_emitter_instancing_preserves_its_count_or_partition_form() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(8)
        .for_each(|bytes: &[u8]| {
            let mut little_endian = [0_u8; 8];
            for (slot, byte) in little_endian.iter_mut().zip(bytes) {
                *slot = *byte;
            }
            let count = match NonZeroU64::new(u64::from_le_bytes(little_endian)) {
                Some(count) => count,
                None => NonZeroU64::MIN,
            };
            for declaration in [
                EmitterInstances::Count(count),
                EmitterInstances::ONE,
                EmitterInstances::Count(NonZeroU64::MAX),
                EmitterInstances::PerPartition,
            ] {
                let json =
                    serde_json::to_vec(&declaration).expect("the current declaration serializes");
                assert_eq!(
                    serde_json::from_slice::<EmitterInstances>(&json)
                        .expect("the current JSON decodes"),
                    declaration
                );
                let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&declaration)
                    .expect("the current declaration archives");
                assert_eq!(
                    rkyv::from_bytes::<EmitterInstances, rkyv::rancor::Error>(&archive)
                        .expect("the current archive decodes"),
                    declaration
                );
            }
        });
}

#[test]
fn emitter_instancing_count_preserves_the_complete_positive_u64_range() {
    for count in [1_u64, 2, 65_536, u64::from(u32::MAX) + 1, u64::MAX] {
        let json = format!("{{\"Count\":{count}}}");
        let declaration = serde_json::from_str::<EmitterInstances>(&json)
            .expect("every positive full-width instance count is a current declaration");
        assert_eq!(declaration.to_string(), count.to_string());
        assert_eq!(declaration.is_one(), count == 1);
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&declaration)
            .expect("the current declaration archives");
        assert_eq!(
            rkyv::from_bytes::<EmitterInstances, rkyv::rancor::Error>(&archive)
                .expect("the current declaration decodes"),
            declaration,
        );
    }
    assert!(serde_json::from_str::<EmitterInstances>("{\"Count\":0}").is_err());
    assert_eq!(EmitterInstances::PerPartition.to_string(), "PER PARTITION");
}
