//! Kafka's native partition selection evidence.
//!
//! Layer: test harness.
//! - **Owns.** The explicit destination ordinal's range and modulo contract.
//! - **Depends on.** The production partition mapper and positive native partition counts.
//! - **Must not know.** Runtime branches, graph configuration or broker I/O.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns partition mapping evidence")
)]

use super::*;

#[test]
fn explicit_partitions_include_zero_and_the_full_native_count_range() {
    for count in [1, 2, 3, 65_536, i32::MAX.unsigned_abs()] {
        let count = NonZeroU32::new(count).expect("positive count");
        for source in [0, 1, 3, 65_535, u64::MAX] {
            let partition = KafkaTopicEmitter::destination_partition(source, count);
            assert_eq!(
                u128::try_from(partition).expect("a destination partition is nonnegative"),
                u128::from(source) % u128::from(count.get())
            );
        }
    }
}

#[test]
fn bolero_explicit_partitions_match_the_wide_modulo_reference() {
    bolero::check!()
        .with_iterations(128)
        .with_max_len(32)
        .with_type::<(u64, u16)>()
        .for_each(|(source, count)| {
            let count = NonZeroU32::new(u32::from(*count) + 1).expect("one was added");
            let partition = KafkaTopicEmitter::destination_partition(*source, count);
            assert_eq!(
                u128::try_from(partition).expect("a destination partition is nonnegative"),
                u128::from(*source) % u128::from(count.get())
            );
        });
}

#[test]
#[cfg(target_pointer_width = "64")]
fn the_driver_keeps_its_native_fixed_footprint() {
    assert!(std::mem::size_of::<KafkaTopicEmitter>() <= 64);
}
