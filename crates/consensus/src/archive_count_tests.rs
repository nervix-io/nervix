//! Count fidelity through the current consensus records and their production storage codec.
//!
//! Layer: test harness.
//! - **Owns.** Complete record equality at native count boundaries and generated values.
//! - **Depends on.** Current consensus record owners and their bounded encoder and decoder.
//! - **Must not know.** Historical stored shapes, graph execution, or external services.

use std::fmt::Debug;

use meticulous::ResultExt as _;
use nervix_arbitrary::Entropy;

use crate::durable_batch::{DurableBatch, StorageDecode, StorageEncode};

pub(crate) fn assert_round_trip<T>(value: &T)
where
    T: Debug + PartialEq + StorageEncode + StorageDecode,
{
    let bytes = DurableBatch::encode(value, 64 * 1024)
        .assured("these bounded current records fit the storage codec budget");
    let decoded = T::decode_record(&bytes)
        .assured("a current record's own encoding must restore its complete value");
    assert_eq!(&decoded, value);
}

#[test]
fn bolero_consensus_counts_round_trip() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(16)
        .for_each(|bytes: &[u8]| {
            let generated = Entropy::new(bytes).any_u64();
            for count in [0, u64::from(u32::MAX) + 1, u64::MAX, generated] {
                let count = usize::try_from(count)
                    .assured("the native test and fuzz targets address 64 bits");
                crate::transaction::assert_count_archives(count);
                crate::transaction_plan::assert_count_archives(count);
                crate::transaction_report::assert_count_archives(count);
                crate::command_execution::assert_count_archives(count);
                crate::assert_count_command_archives(count);
            }
        });
}
