//! Complete current native restore conversions and their checkpoint round trips.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current lifecycle and Kafka values and complete value oracles.
//! - **Depends on.** Native production encoders, decoders, and admitted execution.
//! - **Must not know.** Live graph tasks, publication fencing, or historical encodings.

use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_backup::{StateField, StateValue};
use nervix_execution::{CpuClass, ExecutionConfig, Executor, MemoryClass};
use nervix_models::{ModelName, Timestamp};

use super::{
    BackupBranchLifecycleEntry, decode_backup_branch_lifecycle, decode_backup_kafka_offsets,
    write_restored_branch_lifecycle, write_restored_kafka_offsets,
};

#[test]
fn bolero_streamed_lifecycle_preserves_complete_current_values() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let mut entries = Vec::new();
            for _ in 0..arbitrary.entropy().count(16) {
                let key = if arbitrary.entropy().flag() {
                    Some(
                        vec![
                            StateField {
                                name: "identity".into(),
                                value: scalar(&mut arbitrary),
                            },
                            StateField {
                                name: "nested".into(),
                                value: StateValue::Array(vec![
                                    StateValue::Vec(vec![
                                        scalar(&mut arbitrary),
                                        scalar(&mut arbitrary),
                                    ]),
                                    StateValue::Array(vec![]),
                                ]),
                            },
                            StateField {
                                name: "signed_zero".into(),
                                value: StateValue::F64Bits((-0.0_f64).to_bits()),
                            },
                        ]
                        .into_iter()
                        .map(StateField::into_remote)
                        .collect(),
                    )
                } else {
                    None
                };
                entries.push(BackupBranchLifecycleEntry {
                    key,
                    last_ingestion: Timestamp::from_unix_nanos(arbitrary.entropy().any_i64()),
                    incarnation: arbitrary.positive_u64().get(),
                });
            }
            let expected = lifecycle_values(&entries);
            let entity = ModelName::parse("metrics").assured("literal name is valid");
            let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .assured("the case runtime opens");
            let executor =
                Executor::new(ExecutionConfig::default()).assured("default limits validate");
            let encoded = runtime.block_on(async {
                let charge = executor
                    .reserve(MemoryClass::RestoreMetadata, 1024 * 1024)
                    .await
                    .assured("the bounded property fits its preparation grant");
                executor
                    .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                        let mut bytes = Vec::new();
                        write_restored_branch_lifecycle(
                            entries.iter().cloned(),
                            &entity,
                            &mut bytes,
                            cancellation,
                        )
                        .assured("the complete current lifecycle streams");
                        bytes
                    })
                    .await
                    .assured("the encoder job finishes")
            });
            let entity = ModelName::parse("metrics").assured("literal name is valid");
            let decoded = decode_backup_branch_lifecycle(&encoded, &entity)
                .assured("the current native lifecycle decodes");
            assert_eq!(lifecycle_values(&decoded), expected);
        });
}

fn lifecycle_values(
    entries: &[BackupBranchLifecycleEntry],
) -> Vec<(Option<Vec<StateField>>, Timestamp, u64)> {
    entries
        .iter()
        .map(|entry| {
            (
                entry
                    .key
                    .clone()
                    .map(|fields| fields.into_iter().map(StateField::from_remote).collect()),
                entry.last_ingestion,
                entry.incarnation,
            )
        })
        .collect()
}

fn scalar(arbitrary: &mut Arbitrary<'_>) -> StateValue {
    match arbitrary.entropy().byte() % 13 {
        0 => StateValue::U8(arbitrary.entropy().byte()),
        1 => StateValue::I8(i8::from_le_bytes([arbitrary.entropy().byte()])),
        2 => StateValue::U16(u16::from(arbitrary.entropy().byte())),
        3 => StateValue::I16(i16::from(arbitrary.entropy().byte())),
        4 => StateValue::U32(u32::from(arbitrary.entropy().byte())),
        5 => StateValue::I32(i32::from(arbitrary.entropy().byte())),
        6 => StateValue::U64(arbitrary.entropy().any_u64()),
        7 => StateValue::I64(arbitrary.entropy().any_i64()),
        8 => StateValue::Bool(arbitrary.entropy().flag()),
        9 => StateValue::String(arbitrary.string()),
        10 => StateValue::Datetime("1969-12-31T23:59:59.999999999+00:00".into()),
        11 => {
            let bits = u32::try_from(arbitrary.entropy().any_u64() >> 32)
                .assured("the selected high half contains at most 32 bits");
            // Native branch keys have a finite canonical JSON representation. Clearing one
            // exponent bit selects the finite domain while preserving every finite input bit.
            StateValue::F32Bits(if f32::from_bits(bits).is_finite() {
                bits
            } else {
                bits & 0xff7f_ffff
            })
        }
        _ => {
            let bits = arbitrary.entropy().any_u64();
            StateValue::F64Bits(if f64::from_bits(bits).is_finite() {
                bits
            } else {
                bits & 0xffef_ffff_ffff_ffff
            })
        }
    }
}

#[test]
fn bolero_streamed_kafka_preserves_every_partition_and_offset() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let mut offsets = std::collections::BTreeMap::new();
            for _ in 0..arbitrary.entropy().count(64) {
                let topic = format!("topic_{}", arbitrary.entropy().byte());
                let partition = i32::from(arbitrary.entropy().byte());
                let next_offset = arbitrary.entropy().between(
                    0..=u64::try_from(i64::MAX).assured("the positive signed maximum fits u64"),
                );
                offsets.insert(
                    (topic, partition),
                    i64::try_from(next_offset).assured("bounded by i64::MAX"),
                );
            }
            let offsets = offsets
                .into_iter()
                .map(|((topic, partition), offset)| (topic, partition, offset))
                .collect::<Vec<_>>();
            let input = offsets.clone();
            let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .assured("the case runtime opens");
            let executor =
                Executor::new(ExecutionConfig::default()).assured("default limits validate");
            let encoded = runtime.block_on(async {
                let charge = executor
                    .reserve(MemoryClass::RestoreMetadata, 1024 * 1024)
                    .await
                    .assured("the bounded case is admitted");
                executor
                    .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                        let mut encoded = Vec::new();
                        write_restored_kafka_offsets(
                            input.iter().cloned(),
                            &mut encoded,
                            cancellation,
                        )
                        .assured("the bounded current offset set streams");
                        encoded
                    })
                    .await
                    .assured("the encoder job finishes")
            });
            assert_eq!(
                decode_backup_kafka_offsets(&encoded).assured("current native offsets decode"),
                offsets
            );
        });
}
