//! Complete current native lifecycle and Kafka conversions: restore's streamed native encodings,
//! and a backup cut's streamed archive records of stored native checkpoints.
//!
//! Layer: test harness.
//! - **Owns.** Bounded current lifecycle and Kafka values and complete value oracles.
//! - **Depends on.** Native production encoders, decoders, validated checkpoint views, the
//!   archive's streamed records, and admitted execution.
//! - **Must not know.** Live graph tasks, publication fencing, or historical encodings.

use std::mem::MaybeUninit;

use meticulous::ResultExt as _;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_backup::{
    ArchiveRecord as _, BranchLifecycleEntry, BranchLifecycleRecord, KafkaOffsetsRecord,
    KafkaPartitionOffset, StateField, StateValue, StreamedBranchLifecycle, StreamedKafkaOffsets,
};
use nervix_execution::{Cancellation, CpuClass, ExecutionConfig, Executor, MemoryClass};
use nervix_models::{DomainName, ModelKind, ModelName, SchemaFingerprint, Timestamp};

use super::{
    BackupBranchLifecycleEntry,
    backup_state::{
        NativeKafkaCheckpoint, NativeLifecycleCheckpoint, decode_backup_branch_lifecycle,
        decode_backup_kafka_offsets,
    },
    state_store::checkpoint_reader::AlignedCheckpoint,
    write_restored_branch_lifecycle, write_restored_kafka_offsets,
};

/// Runs `work` as one admitted bulk CPU job under a bounded preparation grant.
fn in_admitted_job<T: Send + 'static>(work: impl FnOnce(&Cancellation) -> T + Send + 'static) -> T {
    let runtime = nervix_primitives::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .assured("the case runtime opens");
    let executor = Executor::new(ExecutionConfig::default()).assured("default limits validate");
    runtime.block_on(async {
        let charge = executor
            .reserve(MemoryClass::RestoreMetadata, 1024 * 1024)
            .await
            .assured("the bounded property fits its preparation grant");
        executor
            .run_cpu(CpuClass::Bulk, charge, move |_charge, cancellation| {
                work(cancellation)
            })
            .await
            .assured("the bounded job finishes")
    })
}

fn aligned(revision: u64, payload: &[u8]) -> AlignedCheckpoint {
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
    aligned.extend_from_slice(payload);
    AlignedCheckpoint {
        lsm: revision,
        payload: aligned,
    }
}

fn generated_lifecycle(arbitrary: &mut Arbitrary<'_>) -> Vec<BackupBranchLifecycleEntry> {
    let mut entries = Vec::new();
    for _ in 0..arbitrary.entropy().count(16) {
        let key = if arbitrary.entropy().flag() {
            Some(
                vec![
                    StateField {
                        name: "identity".into(),
                        value: scalar(arbitrary),
                    },
                    StateField {
                        name: "nested".into(),
                        value: StateValue::Array(vec![
                            StateValue::Vec(vec![scalar(arbitrary), scalar(arbitrary)]),
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
    entries
}

#[test]
fn bolero_streamed_lifecycle_preserves_complete_current_values() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let entries = generated_lifecycle(&mut arbitrary);
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

#[test]
fn bolero_captured_lifecycle_streams_its_complete_archive_record() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let entries = generated_lifecycle(&mut arbitrary);
            let revision = arbitrary.entropy().any_u64();
            let expected = entries
                .iter()
                .cloned()
                .map(BranchLifecycleEntry::from)
                .collect::<Vec<_>>();
            let domain = DomainName::parse("orders").assured("literal name is valid");
            let entity = ModelName::parse("metrics").assured("literal name is valid");
            let schema = SchemaFingerprint::from_digest([9; 32]);
            let record_domain = domain.clone();
            let record_entity = entity.clone();
            let section = in_admitted_job(move |cancellation| {
                let mut native = Vec::new();
                write_restored_branch_lifecycle(
                    entries.into_iter(),
                    &entity,
                    &mut native,
                    cancellation,
                )
                .assured("the complete current lifecycle streams natively");
                let lifecycle = NativeLifecycleCheckpoint::validate(
                    aligned(revision, &native),
                    &entity,
                    cancellation,
                )
                .assured("the stored native lifecycle validates");
                lifecycle.with_branches(|branches| {
                    let record = StreamedBranchLifecycle {
                        domain: record_domain,
                        owner_kind: ModelKind::Ingestor,
                        entity: record_entity,
                        schema,
                        revision: lifecycle.revision,
                        branches: branches.map(BranchLifecycleEntry::from),
                    };
                    let mut scratch = vec![
                        MaybeUninit::uninit();
                        record
                            .scratch_bytes()
                            .assured("bounded branches have scratch")
                    ];
                    let mut section = Vec::new();
                    record
                        .write(&mut scratch, &mut section, &|| false)
                        .assured("the captured lifecycle streams its archive record");
                    section
                })
            });
            let record = BranchLifecycleRecord::decode("captured.rkyv", &section)
                .assured("the archive validator accepts the captured lifecycle");
            assert_eq!(
                record,
                BranchLifecycleRecord {
                    domain,
                    owner_kind: ModelKind::Ingestor,
                    entity: ModelName::parse("metrics").assured("literal name is valid"),
                    schema,
                    revision,
                    branches: expected,
                }
            );
        });
}

#[test]
fn bolero_captured_kafka_offsets_stream_in_archive_order() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            // Stored in any order, and a partition may be recorded more than once.
            let mut stored = Vec::new();
            for _ in 0..arbitrary.entropy().count(64) {
                let next_offset = arbitrary.entropy().between(
                    0..=u64::try_from(i64::MAX).assured("the positive signed maximum fits u64"),
                );
                stored.push((
                    format!("topic_{}", arbitrary.entropy().byte() % 8),
                    i32::from(arbitrary.entropy().byte() % 8),
                    i64::try_from(next_offset).assured("bounded by i64::MAX"),
                ));
            }
            // A later record of a partition replaces an earlier one, as decoding the table keeps it.
            let mut ordered = std::collections::BTreeMap::new();
            for (topic, partition, next_offset) in &stored {
                ordered.insert((topic.clone(), *partition), *next_offset);
            }
            let mut expected = Vec::new();
            for ((topic, partition), next_offset) in ordered {
                expected.push(KafkaPartitionOffset {
                    topic,
                    partition,
                    next_offset,
                });
            }
            let revision = arbitrary.entropy().any_u64();
            let domain = DomainName::parse("orders").assured("literal name is valid");
            let entity = ModelName::parse("source").assured("literal name is valid");
            let schema = SchemaFingerprint::from_digest([7; 32]);
            let record_domain = domain.clone();
            let record_entity = entity.clone();
            let captured = in_admitted_job(move |cancellation| {
                let mut native = Vec::new();
                write_restored_kafka_offsets(stored.into_iter(), &mut native, cancellation)
                    .assured("the stored offsets stream natively");
                let decoded = decode_backup_kafka_offsets(&native)
                    .assured("the runtime decodes its own native offsets");
                let offsets = NativeKafkaCheckpoint::validate(
                    aligned(revision, &native),
                    &entity,
                    cancellation,
                )
                .assured("the stored native offsets validate");
                let section = offsets.with_positions(|positions| {
                    let record = StreamedKafkaOffsets {
                        domain: record_domain,
                        entity: record_entity,
                        schema,
                        revision: offsets.revision,
                        offsets: positions.map(KafkaPartitionOffset::from),
                    };
                    let mut scratch = vec![
                        MaybeUninit::uninit();
                        record
                            .scratch_bytes()
                            .assured("bounded offsets have scratch")
                    ];
                    let mut section = Vec::new();
                    record
                        .write(&mut scratch, &mut section, &|| false)
                        .assured("the captured offsets stream their archive record");
                    section
                });
                CapturedOffsets { decoded, section }
            });
            let record = KafkaOffsetsRecord::decode("captured.rkyv", &captured.section)
                .assured("the archive validator accepts the captured offsets");
            let mut decoded = Vec::new();
            for (topic, partition, next_offset) in captured.decoded {
                decoded.push(KafkaPartitionOffset {
                    topic,
                    partition,
                    next_offset,
                });
            }
            assert_eq!(decoded, expected, "the runtime's own decoding agrees");
            assert_eq!(
                record,
                KafkaOffsetsRecord {
                    domain,
                    entity: ModelName::parse("source").assured("literal name is valid"),
                    schema,
                    revision,
                    offsets: expected,
                }
            );
        });
}

/// The runtime's whole decoding of a native offset checkpoint beside its streamed archive record.
struct CapturedOffsets {
    decoded: Vec<(String, i32, i64)>,
    section: Vec<u8>,
}
