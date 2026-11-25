//! Native regression tests for sealed materialized snapshots.
//!
//! Layer: test harness, outside the product layer order.
//!
//! - **Owns.** Sealing, inspecting, staging and opening materialized generations within their
//!   memory classes, including an open that waits for room and one that runs again within a
//!   larger share.
//! - **Depends on.** The snapshot module's containers and sources, its staging and the executor.
//! - **Must not know.** The processors, relays or interconnect that move snapshots.

use std::num::NonZeroU32;

use futures_util::FutureExt as _;
use meticulous::ResultExt as _;
use nervix_execution::{ExecutionConfig, MemoryBudgets, OperationLimits};
use nervix_models::FieldName;
use ubyte::ByteUnit;

use super::*;
use crate::{
    runtime::snapshot_staging::{SnapshotStaging, SnapshotStagingLimits},
    runtime_schema::{RuntimeValue, test_runtime_row},
};

/// One row of payload in the snapshot under test. Large enough that a handful of records
/// exceed several section limits, small enough that no single record does.
const RECORD_BYTES: usize = 512 * 1024;

/// How many records the snapshot carries. Their columns alone exceed eight mebibytes and the
/// bulk transfer budget the narrow executor allows.
const RECORDS: usize = 80;

/// The sealed container of `records`, as a node persists it, with the schema its rows share.
async fn sealed_payload(
    executor: &Executor,
    records: Vec<MaterializedGenerationRecord>,
) -> (StdArc<ArrowSchema>, Vec<u8>) {
    let root = tempfile::tempdir().assured("the staging directory opens");
    let staging = SnapshotStaging::new(
        root.path().to_path_buf(),
        executor.clone(),
        SnapshotStagingLimits::default(),
    );
    let schema = records[0].row.arrow_schema();
    let sealed = MaterializedGeneration::new(7, 3, 5, schema.clone(), records)
        .seal(executor, &staging)
        .await
        .assured("the bounded generation seals");
    let entry = sealed
        .into_persisted_entry(executor)
        .await
        .assured("the sealed generation admits its resident entry");
    (schema, entry.payload)
}

/// One record per tenant, whose one column is its index.
fn indexed_records(
    count: usize,
    tenant: impl Fn(usize) -> String,
) -> Vec<MaterializedGenerationRecord> {
    let mut records = Vec::with_capacity(count);
    for index in 0..count {
        records.push(MaterializedGenerationRecord {
            branch: Some(tenant_branch(&tenant(index))),
            row: test_runtime_row([("index".to_string(), RuntimeValue::I64(test_index(index)))]),
        });
    }
    records
}

/// What one opened snapshot keeps charged to the state-read class.
fn kept_bytes(snapshot: &RestoredMaterializedSnapshot) -> u64 {
    let mut kept = snapshot._metadata_charge.bytes();
    for charge in snapshot._columns_charge.iter() {
        kept = kept
            .checked_add(charge.bytes())
            .assured("one snapshot's charges fit the class that admitted them");
    }
    kept
}

/// An open of a container read from storage whose record identities need more of the state-read
/// class than the container's size suggests gives back everything its first attempt took and
/// opens the container again within the most its rest can keep, so it runs exactly twice. It
/// ends holding exactly the charges the snapshot keeps.
#[nervix_primitives::test]
async fn a_stored_open_whose_record_identities_outgrow_its_first_share_runs_once_more() {
    let executor = Executor::new(ExecutionConfig::default()).assured("default bounds are valid");
    // Long branch keys beside one small column: the identities fill the container, and each of
    // their bytes is eight bytes of row metadata once decoded.
    let records = indexed_records(64, |index| format!("{index:04}-{}", "t".repeat(200)));
    let (schema, payload) = sealed_payload(&executor, records).await;
    let before = executor.memory(MemoryClass::StateRead);
    let restored = RestoredMaterializedSnapshot::open_relay(
        &executor,
        &schema,
        RelayContainerSource::Sealed(SealedSource::stored(
            executor.clone(),
            crate::runtime::state_store::checkpoint_reader::CheckpointReader::Inline(
                std::io::Cursor::new(payload),
            ),
        )),
    )
    .await
    .assured("the snapshot opens within a larger share");
    let after = executor.memory(MemoryClass::StateRead);
    assert_eq!(restored.records.len(), 64, "every record is restored");
    assert_eq!(
        after.granted - before.granted,
        2,
        "the open took a first share, gave it back and took the most its rest can keep"
    );
    assert_eq!(
        after.reserved_bytes - before.reserved_bytes,
        kept_bytes(&restored),
        "the open keeps exactly the snapshot's own charges, nothing of its shares"
    );
}

/// The same container in memory declares exactly what its sections keep, so its open takes
/// exactly that share once.
#[nervix_primitives::test]
async fn an_open_of_a_container_in_memory_takes_exactly_its_share_once() {
    let executor = Executor::new(ExecutionConfig::default()).assured("default bounds are valid");
    let records = indexed_records(64, |index| format!("{index:04}-{}", "t".repeat(200)));
    let (schema, payload) = sealed_payload(&executor, records).await;
    let before = executor.memory(MemoryClass::StateRead);
    let restored = RestoredMaterializedSnapshot::open_relay(
        &executor,
        &schema,
        RelayContainerSource::Sealed(SealedSource::borrowed(&executor, &payload)),
    )
    .await
    .assured("the snapshot opens");
    let after = executor.memory(MemoryClass::StateRead);
    assert_eq!(restored.records.len(), 64, "every record is restored");
    assert_eq!(
        after.granted - before.granted,
        1,
        "an open of a container in memory takes one share"
    );
    assert_eq!(
        after.reserved_bytes - before.reserved_bytes,
        kept_bytes(&restored),
        "the share it took is exactly the snapshot's own charges"
    );
}

/// An open whose state-read class has room for its row metadata and no more waits for room
/// for everything it keeps, rather than refusing the snapshot once its decoded identities and
/// columns outgrow that room. It waits while it holds nothing of the bulk class, and opens the
/// snapshot once room frees.
#[nervix_primitives::test]
async fn an_open_waits_for_state_read_room_while_it_holds_no_bulk_charge() {
    let executor = Executor::new(ExecutionConfig::default()).assured("default bounds are valid");
    let (schema, payload) = sealed_payload(
        &executor,
        indexed_records(4, |index| format!("tenant-{index}")),
    )
    .await;
    let summary =
        inspect_sealed_container(&payload, executor.limits().snapshot_header_bytes.as_u64())
            .assured("the sealed container frames its header");
    let metadata_bytes = SealedSnapshotHeader {
        revision: summary.revision,
        fence: summary.fence,
        branch_generation: summary.branch_generation,
        records: summary.records,
        groups: summary.groups,
    }
    .metadata_layout(
        RestoredRows::Relay,
        executor.memory(MemoryClass::Relay).capacity_bytes,
    )
    .assured("the header's records fit the relay budget")
    .metadata_bytes
    .max(1);
    let capacity = executor.memory(MemoryClass::StateRead).capacity_bytes;
    let held = executor
        .try_reserve(
            MemoryClass::StateRead,
            capacity
                .checked_sub(metadata_bytes)
                .assured("the row metadata of four records fits the class"),
        )
        .assured("the test holds the state-read class but the row metadata's room");
    let waits = nervix_execution::CapacityWaits::new();
    let reporting = executor.reporting_waits_to(&waits);
    let opening = nervix_primitives::task::spawn(async move {
        RestoredMaterializedSnapshot::open_relay(
            &reporting,
            &schema,
            RelayContainerSource::Sealed(SealedSource::borrowed(&reporting, &payload)),
        )
        .await
    });
    nervix_primitives::time::timeout(std::time::Duration::from_secs(30), waits.began())
        .await
        .assured("the open waits for state-read room");
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        0,
        "an open waits for state-read room while it holds nothing of the bulk class"
    );
    drop(held);
    let restored = nervix_primitives::time::timeout(std::time::Duration::from_secs(30), opening)
        .await
        .assured("the open finishes once room frees")
        .assured("the opening task joins")
        .assured("the snapshot opens once room frees");
    assert_eq!(restored.records.len(), 4, "every record is restored");
}

/// An open whose bulk class has room for a section's bytes and none for its decoder waits for
/// room for both as one charge, rather than refusing the decoding while it holds the bytes. It
/// waits while it holds nothing of the class, and opens the snapshot once room frees.
#[nervix_primitives::test]
async fn an_open_waits_for_bulk_room_while_it_holds_none_of_the_class() {
    let executor = Executor::new(ExecutionConfig::default()).assured("default bounds are valid");
    let (schema, payload) = sealed_payload(
        &executor,
        indexed_records(4, |index| format!("tenant-{index}")),
    )
    .await;
    let capacity = executor.memory(MemoryClass::Bulk).capacity_bytes;
    // Room for the container's framing and header, none for the header's decoder.
    let held = executor
        .try_reserve(
            MemoryClass::Bulk,
            capacity
                .checked_sub(1024)
                .assured("the bulk class holds more than a kibibyte"),
        )
        .assured("the test holds the bulk class but a kibibyte");
    let waits = nervix_execution::CapacityWaits::new();
    let reporting = executor.reporting_waits_to(&waits);
    let opening = nervix_primitives::task::spawn(async move {
        RestoredMaterializedSnapshot::open_relay(
            &reporting,
            &schema,
            RelayContainerSource::Sealed(SealedSource::borrowed(&reporting, &payload)),
        )
        .await
    });
    nervix_primitives::time::timeout(std::time::Duration::from_secs(30), waits.began())
        .await
        .assured("the open waits for bulk room");
    drop(held);
    let restored = nervix_primitives::time::timeout(std::time::Duration::from_secs(30), opening)
        .await
        .assured("the open finishes once room frees")
        .assured("the opening task joins")
        .assured("the snapshot opens once room frees");
    assert_eq!(restored.records.len(), 4, "every record is restored");
}

/// A container held under a bulk charge carries the scratch its decoders draw from, taken with
/// it as one charge, so its open never waits for bulk room behind the container it reads, even
/// with the rest of the class held.
#[nervix_primitives::test]
async fn an_owned_container_decodes_within_the_scratch_charged_with_it() {
    let executor = Executor::new(ExecutionConfig::default()).assured("default bounds are valid");
    let (schema, payload) = sealed_payload(
        &executor,
        indexed_records(4, |index| format!("tenant-{index}")),
    )
    .await;
    let length: u64 = payload.len().arch_into();
    let held = executor
        .reserve(
            MemoryClass::Bulk,
            length
                .checked_add(RestoredMaterializedSnapshot::decode_scratch_bound(
                    &executor, length,
                ))
                .assured("a bounded container and its scratch fit u64"),
        )
        .await
        .assured("the container is charged with its decoders' scratch");
    let (container, working) = held
        .split(length)
        .assured("the container's part fits the charge taken for it");
    let snapshot = executor.memory(MemoryClass::Bulk);
    let rest = executor
        .try_reserve(
            MemoryClass::Bulk,
            snapshot
                .capacity_bytes
                .checked_sub(snapshot.reserved_bytes)
                .assured("the class holds no more than its capacity"),
        )
        .assured("the test holds the rest of the bulk class");
    let restored = nervix_primitives::time::timeout(
        std::time::Duration::from_secs(30),
        RestoredMaterializedSnapshot::open_relay(
            &executor,
            &schema,
            RelayContainerSource::Sealed(SealedSource::memory_with_working(
                ChargedBytes::from_owned(payload, container),
                working,
            )),
        ),
    )
    .await
    .assured("the open draws its decoders' scratch from the container's charge")
    .assured("the snapshot opens");
    assert_eq!(restored.records.len(), 4, "every record is restored");
    drop(rest);
}

#[nervix_primitives::test]
async fn snapshot_decoder_refuses_bulk_wait_while_retaining_its_metadata_input() {
    let config = ExecutionConfig::default();
    let bulk_bytes = config.budgets.bulk.as_u64();
    let limit = config.limits.snapshot_header_bytes.as_u64();
    let executor = Executor::new(config).assured("the default executor is valid");
    let expected = SealedSnapshotHeader {
        revision: 7,
        fence: 9,
        branch_generation: 11,
        records: 0,
        groups: 0,
    };
    let charge = executor
        .reserve(MemoryClass::Bulk, limit)
        .await
        .assured("an idle pool admits the header's encoding");
    let input = encode_rkyv_within(&executor, expected.clone(), limit, charge)
        .await
        .assured("the current snapshot header encodes");
    let input_bytes: u64 = input.len().arch_into();
    let occupied_bytes = bulk_bytes
        .checked_sub(input_bytes)
        .assured("the header is smaller than the bulk budget");
    let occupied = executor
        .try_reserve(MemoryClass::Bulk, occupied_bytes)
        .assured("the test occupies the memory outside the decoder's input");

    let result = decode_rkyv::<SealedSnapshotHeader>(&executor, input, limit).now_or_never();
    assert!(
        result.is_some(),
        "a snapshot decoder must refuse rather than wait while its input holds bulk memory"
    );
    let error = result
        .verified("the decoder returned without waiting for its own input to be released")
        .expect_err("the full bulk budget cannot admit metadata decoding");
    assert_eq!(
        error.downcast_ref::<AdmissionError>(),
        Some(&AdmissionError::BudgetExhausted {
            class: "bulk",
            requested: limit,
        })
    );
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        occupied.bytes(),
        "refusal releases the complete input charge"
    );
    drop(occupied);

    let charge = executor
        .reserve(MemoryClass::Bulk, limit)
        .await
        .assured("an idle pool admits the header's encoding");
    let input = encode_rkyv_within(&executor, expected.clone(), limit, charge)
        .await
        .assured("the current snapshot header encodes after contention ends");
    let decoded = decode_rkyv::<SealedSnapshotHeader>(&executor, input, limit)
        .await
        .assured("the complete header remains readable once bulk room is available");
    assert_eq!(decoded, expected);
    assert_eq!(executor.memory(MemoryClass::Bulk).reserved_bytes, 0);
}

fn generated_schema_type(
    arbitrary: &mut nervix_arbitrary::Arbitrary<'_>,
    depth: u8,
) -> nervix_models::ParseAsType {
    use nervix_models::ParseAsType as T;
    let scalar_types = [
        T::U8,
        T::U16,
        T::U32,
        T::U64,
        T::I8,
        T::I16,
        T::I32,
        T::I64,
        T::F32,
        T::F64,
        T::Bool,
        T::String,
        T::Datetime,
        T::Bytes,
        T::Duration,
    ];
    let choice = usize::from(arbitrary.entropy().byte())
        % if depth == 0 {
            scalar_types.len()
        } else {
            scalar_types.len() + 2
        };
    match choice {
        15 => T::Vec {
            element: Box::new(generated_schema_type(arbitrary, depth - 1)),
        },
        16 => T::Array {
            element: Box::new(generated_schema_type(arbitrary, depth - 1)),
            len: NonZeroU32::new(u32::from(arbitrary.entropy().byte() % 4) + 1)
                .assured("generated fixed-size lists contain one to four elements"),
        },
        index => scalar_types[index].clone(),
    }
}

fn generated_columns(
    arbitrary: &mut nervix_arbitrary::Arbitrary<'_>,
    ty: &nervix_models::ParseAsType,
    valid: &[bool],
) -> arrow_array::ArrayRef {
    use arrow_array::*;
    use nervix_models::ParseAsType as T;
    macro_rules! primitive {
        ($array:ty, $value:expr) => {
            StdArc::new(<$array>::from(
                valid
                    .iter()
                    .map(|valid| valid.then(|| $value))
                    .collect::<Vec<_>>(),
            ))
        };
    }
    match ty {
        T::U8 => primitive!(UInt8Array, arbitrary.entropy().byte()),
        T::I8 => primitive!(Int8Array, i8::from_le_bytes([arbitrary.entropy().byte()])),
        T::U16 => primitive!(
            UInt16Array,
            u16::from_le_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()))
        ),
        T::I16 => primitive!(
            Int16Array,
            i16::from_le_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()))
        ),
        T::U32 => primitive!(
            UInt32Array,
            u32::from_le_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()))
        ),
        T::I32 => primitive!(
            Int32Array,
            i32::from_le_bytes(std::array::from_fn(|_| arbitrary.entropy().byte()))
        ),
        T::U64 => primitive!(UInt64Array, arbitrary.entropy().any_u64()),
        T::I64 => primitive!(Int64Array, arbitrary.entropy().any_i64()),
        T::Bool => primitive!(BooleanArray, arbitrary.entropy().flag()),
        T::String => StdArc::new(StringArray::from_iter(
            valid.iter().map(|valid| valid.then(|| arbitrary.string())),
        )),
        T::Datetime => StdArc::new(
            TimestampNanosecondArray::from(
                valid
                    .iter()
                    .map(|valid| valid.then(|| arbitrary.entropy().any_i64()))
                    .collect::<Vec<_>>(),
            )
            .with_timezone("+00:00"),
        ),
        T::F32 => primitive!(
            Float32Array,
            f32::from_bits(u32::from_le_bytes(std::array::from_fn(|_| arbitrary
                .entropy()
                .byte())))
        ),
        T::F64 => primitive!(Float64Array, f64::from_bits(arbitrary.entropy().any_u64())),
        T::Duration => primitive!(DurationNanosecondArray, arbitrary.entropy().any_i64()),
        T::Bytes => {
            StdArc::new(BinaryArray::from_iter(valid.iter().map(|valid| {
                valid.then(|| arbitrary.entropy().any_u64().to_le_bytes())
            })))
        }
        T::Array { element, len } => {
            let child_count = valid
                .len()
                .checked_mul(usize::try_from(len.get()).verified("generated array length fits"))
                .assured("bounded generated array fits");
            let child = generated_columns(arbitrary, element, &vec![true; child_count]);
            let arrow_schema::DataType::FixedSizeList(field, length) = ty.arrow_data_type() else {
                unreachable!("a fixed-array type has a fixed-list carrier")
            };
            StdArc::new(
                FixedSizeListArray::try_new(
                    field,
                    length,
                    child,
                    Some(arrow_buffer::NullBuffer::from(valid.to_vec())),
                )
                .assured("generated fixed-list columns match their type"),
            )
        }
        T::Vec { element } => {
            let mut offsets = vec![0_i32];
            let mut count = 0_usize;
            for present in valid {
                let length = if *present {
                    usize::from(arbitrary.entropy().byte() % 4)
                } else {
                    0
                };
                count = count
                    .checked_add(length)
                    .assured("bounded vector count fits");
                offsets.push(i32::try_from(count).verified("bounded vector offsets fit"));
            }
            let child = generated_columns(arbitrary, element, &vec![true; count]);
            let arrow_schema::DataType::List(field) = ty.arrow_data_type() else {
                unreachable!("a vector type has a list carrier")
            };
            StdArc::new(
                ListArray::try_new(
                    field,
                    arrow_buffer::OffsetBuffer::new(offsets.into()),
                    child,
                    Some(arrow_buffer::NullBuffer::from(valid.to_vec())),
                )
                .assured("generated list columns match their type"),
            )
        }
    }
}

#[test]
fn bolero_materialized_archive_columns_preserve_the_exact_schema_and_rows() {
    use nervix_arbitrary::{Arbitrary, Domain};
    use nervix_backup::{
        ArchiveRecord, MaterializedIdentitiesRecord, MaterializedRecordIdentity,
        MaterializedRelayDescriptor, StateField,
    };
    use nervix_models::{CreateSchema, ParseAsType, SchemaField, SchemaFingerprint, SchemaName};
    bolero::check!()
        .with_iterations(128)
        .with_max_len(4096)
        .for_each(|input| {
            let runtime = nervix_primitives::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .assured("property runtime opens");
            let executor =
                Executor::new(ExecutionConfig::default()).assured("default bounds are valid");
            let mut arbitrary = Arbitrary::new(input, Domain::Vocabulary);
            let scalar = generated_schema_type(&mut arbitrary, 3);
            let schema_model = CreateSchema {
                labels: Default::default(),
                name: SchemaName::parse("event").assured("schema name is valid"),
                fields: vec![
                    SchemaField {
                        name: FieldName::parse("nullable").assured("field is valid"),
                        ty: scalar,
                        optional: true,
                        sensitive: arbitrary.entropy().flag(),
                    },
                    SchemaField {
                        name: FieldName::parse("secret").assured("field is valid"),
                        ty: ParseAsType::String,
                        optional: arbitrary.entropy().flag(),
                        sensitive: true,
                    },
                    SchemaField {
                        name: FieldName::parse("nested").assured("field is valid"),
                        ty: ParseAsType::Vec {
                            element: Box::new(ParseAsType::Array {
                                element: Box::new(ParseAsType::I64),
                                len: NonZeroU32::new(2).assured("array length is positive"),
                            }),
                        },
                        optional: arbitrary.entropy().flag(),
                        sensitive: arbitrary.entropy().flag(),
                    },
                    SchemaField {
                        name: FieldName::parse("bytes").assured("field is valid"),
                        ty: ParseAsType::Bytes,
                        optional: arbitrary.entropy().flag(),
                        sensitive: arbitrary.entropy().flag(),
                    },
                ],
            };
            let compiled = crate::runtime_schema::compile_schema(&schema_model);
            let columns = schema_model
                .fields
                .iter()
                .map(|field| {
                    let valid = (0..2)
                        .map(|_| !field.optional || arbitrary.entropy().flag())
                        .collect::<Vec<_>>();
                    generated_columns(&mut arbitrary, &field.ty, &valid)
                })
                .collect::<Vec<_>>();
            let arrow_schema = compiled.arrow_schema().clone();
            let arrow = arrow_array::RecordBatch::try_new(arrow_schema.clone(), columns)
                .assured("generated Arrow columns match their exact schema");
            let batch = Arc::new(
                RuntimeRecordBatch::from_record_batch(arrow_schema, arrow)
                    .assured("generated payload remains columnar"),
            );
            let low = arbitrary.entropy().any_i64();
            let high = arbitrary.entropy().any_i64();
            let metadata = RuntimeRecordMetadata::from_remote(RemoteRuntimeRecordMetadata {
                ingested_at_low_watermark: nervix_models::Timestamp::from_unix_nanos(low.min(high)),
                ingested_at_high_watermark: nervix_models::Timestamp::from_unix_nanos(
                    low.max(high),
                ),
            });
            let records = (0..2)
                .map(|index| MaterializedGenerationRecord {
                    branch: Some(tenant_branch(if index == 0 { "alpha" } else { "beta" })),
                    row: RuntimeRow::new(batch.clone(), index, metadata.clone())
                        .assured("row exists"),
                })
                .collect();
            let generation = MaterializedGeneration::new(
                arbitrary.entropy().any_u64(),
                arbitrary.entropy().any_u64(),
                arbitrary.entropy().any_u64(),
                batch.schema(),
                records,
            );
            let descriptor = MaterializedRelayDescriptor {
                domain: nervix_models::DomainName::parse("prod").assured("domain is valid"),
                entity: nervix_models::ModelName::parse("state").assured("relay is valid"),
                unit: nervix_models::Unit::SINGLE,
                schema: SchemaFingerprint::from_digest(std::array::from_fn(|_| {
                    arbitrary.entropy().byte()
                })),
                revision: generation.revision(),
                fence: generation.fence(),
                branch_generation: generation.branch_generation(),
                record_count: 2,
                groups: 1,
            };
            let descriptor = MaterializedRelayDescriptor::decode(
                "descriptor.rkyv",
                &descriptor.encode().assured("descriptor encodes"),
            )
            .assured("descriptor validates");
            let identities = MaterializedIdentitiesRecord {
                domain: descriptor.domain.clone(),
                entity: descriptor.entity.clone(),
                unit: descriptor.unit,
                group: 0,
                identities: generation
                    .records()
                    .iter()
                    .map(|record| MaterializedRecordIdentity {
                        branch: BranchKey::to_remote_key(&record.branch)
                            .map_fields(StateField::from_remote),
                        watermarks: record.row.metadata().to_remote(),
                    })
                    .collect(),
            };
            let identities = MaterializedIdentitiesRecord::decode(
                "identities.rkyv",
                &identities.encode().assured("identities encode"),
            )
            .assured("identities validate");
            runtime.block_on(async {
                let columns = generation
                    .encode_columns(&executor, &(0..2))
                    .await
                    .assured("exact columns encode");
                let decoded = RuntimeRecordBatch::decode_arrow_snapshot_section(
                    &executor,
                    batch.schema(),
                    columns.clone(),
                )
                .await
                .assured("exact columns validate");
                assert_eq!(decoded.schema(), batch.schema());
                assert_eq!(
                    decoded
                        .encode_arrow_snapshot_section(&executor)
                        .await
                        .assured("restored columns encode")
                        .as_ref(),
                    columns.as_ref()
                );
                let mut bytes = materialized_container_header(
                    descriptor.revision,
                    descriptor.fence,
                    descriptor.branch_generation,
                    descriptor.record_count,
                    descriptor.groups,
                )
                .assured("native header encodes");
                let native_identities = identities
                    .identities
                    .into_iter()
                    .map(|identity| {
                        (
                            identity.branch.map_fields(StateField::into_remote),
                            identity.watermarks,
                        )
                    })
                    .collect::<Vec<_>>();
                bytes.extend(
                    materialized_identity_section(native_identities.clone())
                        .assured("native identities encode"),
                );
                bytes.extend(
                    materialized_columns_frame(columns.len()).assured("column frame encodes"),
                );
                bytes.extend_from_slice(columns.as_ref());
                let mut archived = CapturedMaterializedCheckpoint::new(
                    executor.clone(),
                    crate::runtime::state_store::checkpoint_reader::CheckpointReader::Inline(
                        std::io::Cursor::new(bytes.clone()),
                    ),
                )
                .open(&executor)
                .await
                .assured("stored generation opens for re-export");
                assert_eq!(
                    archived.summary(),
                    SealedSnapshotSummary {
                        revision: descriptor.revision,
                        fence: descriptor.fence,
                        branch_generation: descriptor.branch_generation,
                        records: descriptor.record_count,
                        groups: descriptor.groups,
                    }
                );
                let group = archived
                    .next_group(
                        &executor,
                        nervix_backup::MATERIALIZED_IDENTITIES_BYTES,
                        nervix_backup::MATERIALIZED_COLUMNS_BYTES,
                    )
                    .await
                    .assured("stored group validates")
                    .assured("the group exists");
                assert_eq!(group.identities, native_identities);
                assert_eq!(group.columns.as_ref(), columns.as_ref());
                drop(group);
                assert!(
                    archived
                        .next_group(
                            &executor,
                            nervix_backup::MATERIALIZED_IDENTITIES_BYTES,
                            nervix_backup::MATERIALIZED_COLUMNS_BYTES,
                        )
                        .await
                        .assured("stored framing and count are complete")
                        .is_none()
                );
                drop(archived);
                drop(columns);
                let length: u64 = bytes.len().arch_into();
                let held = executor
                    .reserve(
                        MemoryClass::Bulk,
                        length
                            .checked_add(RestoredMaterializedSnapshot::decode_scratch_bound(
                                &executor, length,
                            ))
                            .assured("a bounded container and its scratch fit u64"),
                    )
                    .await
                    .assured("bounded container is charged with its decoders' scratch");
                let (container, working) = held
                    .split(length)
                    .assured("the container's part fits the charge taken for it");
                let restored = RestoredMaterializedSnapshot::open_relay(
                    &executor,
                    &batch.schema(),
                    RelayContainerSource::Sealed(SealedSource::memory_with_working(
                        ChargedBytes::from_owned(bytes, container),
                        working,
                    )),
                )
                .await
                .assured("native generation opens");
                assert_eq!(
                    (
                        restored.revision,
                        restored.fence,
                        restored.branch_generation
                    ),
                    (
                        descriptor.revision,
                        descriptor.fence,
                        descriptor.branch_generation
                    )
                );
                assert_eq!(restored.records.len(), generation.records().len());
                for (expected, actual) in generation.records().iter().zip(restored.records) {
                    assert_eq!(actual.branch, expected.branch);
                    assert_eq!(
                        actual.row.metadata().to_remote(),
                        expected.row.metadata().to_remote()
                    );
                    let actual_columns = actual
                        .row
                        .one_row_batch()
                        .encode_arrow_snapshot_section(&executor)
                        .await
                        .assured("native restored row encodes");
                    let expected_columns = expected
                        .row
                        .one_row_batch()
                        .encode_arrow_snapshot_section(&executor)
                        .await
                        .assured("captured row encodes");
                    assert_eq!(actual_columns.as_ref(), expected_columns.as_ref());
                }
            });
        });
}

/// Memory a reader admitted for a relay container's layout opens it on its own: exactly the need
/// the layout names suffices while every other byte of the bulk and state-read pools is taken, one
/// byte less runs short, and the opened snapshot keeps only its rows' share.
#[nervix_primitives::test]
async fn a_relay_container_opens_within_exactly_the_need_its_layout_names() {
    let executor = narrow_executor();
    let records = (0..3)
        .map(|index| MaterializedGenerationRecord {
            branch: Some(tenant_branch(&format!("tenant-{index}"))),
            row: test_runtime_row([
                ("index".to_string(), RuntimeValue::I64(test_index(index))),
                (
                    "payload".to_string(),
                    RuntimeValue::String("p".repeat(4096)),
                ),
            ]),
        })
        .collect::<Vec<_>>();
    let schema = records[0].row.arrow_schema();
    let generation = MaterializedGeneration::new(7, 3, 5, schema.clone(), records);
    let container = generation
        .encode_resident_container(&executor)
        .await
        .assured("a bounded generation encodes");
    let container_bytes: u64 = container.bytes.len().arch_into();
    let need = container
        .layout
        .relay_opening(&executor, container_bytes)
        .assured("the layout is within the node's bounds")
        .need()
        .assured("the opening's parts add up within u64");

    let short = executor
        .try_reserve(MemoryClass::Bulk, need - 1)
        .assured("an idle pool admits the short opening");
    let error = RestoredMaterializedSnapshot::open_relay(
        &executor,
        &schema,
        RelayContainerSource::Received {
            sealed: container.bytes.clone(),
            layout: &container.layout,
            memory: short,
        },
    )
    .await
    .expect_err("one byte less than the need runs short at the opening's peak");
    assert!(
        matches!(
            error.current_context(),
            MaterializedSnapshotError::Admission
        ),
        "{error:?}"
    );

    let memory = executor
        .try_reserve(MemoryClass::Bulk, need)
        .assured("an idle pool admits the opening");
    let bulk = executor.memory(MemoryClass::Bulk);
    let bulk_left = bulk.capacity_bytes - bulk.reserved_bytes;
    let held_bulk = executor
        .try_reserve(MemoryClass::Bulk, bulk_left)
        .assured("the rest of the bulk pool is free");
    let state_read = executor.memory(MemoryClass::StateRead).capacity_bytes;
    let held_state_read = executor
        .try_reserve(MemoryClass::StateRead, state_read)
        .assured("the state-read pool is free");
    let restored = nervix_primitives::time::timeout(
        std::time::Duration::from_secs(30),
        RestoredMaterializedSnapshot::open_relay(
            &executor,
            &schema,
            RelayContainerSource::Received {
                sealed: container.bytes,
                layout: &container.layout,
                memory,
            },
        ),
    )
    .await
    .assured("opening never waits on the pools")
    .assured("the container opens within its need");
    assert_eq!(restored.records.len(), 3);
    for (index, record) in restored.records.iter().enumerate() {
        assert_eq!(
            record.branch,
            Some(tenant_branch(&format!("tenant-{index}")))
        );
        assert_eq!(
            record.row.value_at(0).assured("the index loads"),
            Some(RuntimeValue::I64(test_index(index)))
        );
    }
    let kept = executor.memory(MemoryClass::Bulk).reserved_bytes - held_bulk.bytes();
    assert!(
        kept > 0 && kept < need,
        "the snapshot keeps its rows' share of the {need} admitted bytes and the received \
         {container_bytes}-byte container went back, found {kept}"
    );
    drop(restored);
    assert_eq!(
        executor.memory(MemoryClass::Bulk).reserved_bytes,
        held_bulk.bytes(),
        "closing the snapshot gives back everything it kept"
    );
    drop(held_state_read);
}

/// A node whose snapshot sections are far smaller than the snapshot under test, so a snapshot
/// that seals and moves proves it did so as many bounded sections and many bounded chunks
/// rather than as one large one.
fn narrow_executor() -> Executor {
    Executor::new(ExecutionConfig {
        budgets: MemoryBudgets {
            bulk: ByteUnit::Mebibyte(32),
            ..MemoryBudgets::default()
        },
        limits: OperationLimits {
            snapshot_section_bytes: ByteUnit::Mebibyte(1),
            snapshot_record_bytes: ByteUnit::Kibibyte(256),
            snapshot_header_bytes: ByteUnit::Kibibyte(64),
            bulk_chunk_bytes: ByteUnit::Kibibyte(64),
            decoder_depth: NonZeroU32::new(64).assured("sixty-four is not zero"),
            ..OperationLimits::default()
        },
        ..ExecutionConfig::default()
    })
    .assured("a bulk budget above two sections plus a header and a chunk is consistent")
}

fn test_index(index: usize) -> i64 {
    i64::try_from(index).assured("the test builds fewer records than an i64 counts")
}

fn tenant_branch(tenant: &str) -> BranchKey {
    BranchKey::from_fields([(
        FieldName::try_from("tenant".to_string())
            .assured("the test branch field name satisfies the field grammar"),
        RuntimeValue::String(tenant.to_string()),
    )])
    .assured("a one-field branch key is well formed")
}

fn wide_generation() -> MaterializedGeneration {
    let records = (0..RECORDS)
        .map(|index| {
            let row = test_runtime_row([
                ("index".to_string(), RuntimeValue::I64(test_index(index))),
                (
                    "payload".to_string(),
                    RuntimeValue::String("p".repeat(RECORD_BYTES)),
                ),
            ]);
            MaterializedGenerationRecord {
                branch: Some(tenant_branch(&format!("tenant-{index:02}"))),
                row,
            }
        })
        .collect::<Vec<_>>();
    let schema = records[0].row.arrow_schema();
    MaterializedGeneration::new(7, 3, 5, schema, records)
}

#[nervix_primitives::test]
async fn admitted_handoff_preserves_the_generation_and_validates_its_frames() {
    let executor = narrow_executor();
    let root = tempfile::tempdir().assured("handoff staging directory opens");
    let staging = SnapshotStaging::new(
        root.path().to_path_buf(),
        executor.clone(),
        SnapshotStagingLimits::default(),
    );
    let row = test_runtime_row([("value".to_string(), RuntimeValue::I64(42))]);
    let schema = row.arrow_schema();
    let metadata = row.metadata().to_remote();
    let branch = Some(tenant_branch("alpha"));
    let sealed = MaterializedGeneration::new(
        7,
        3,
        5,
        schema.clone(),
        vec![MaterializedGenerationRecord {
            branch: branch.clone(),
            row,
        }],
    )
    .seal(&executor, &staging)
    .await
    .assured("the bounded generation seals");
    let descriptor = sealed.descriptor;
    let entry = sealed
        .into_persisted_entry(&executor)
        .await
        .assured("the handoff admits its resident entry");
    assert_eq!(entry.lsm, 7);
    assert_eq!(
        u64::try_from(entry.payload.len()).verified("the admitted payload length fits"),
        descriptor.length
    );
    assert_eq!(*blake3::hash(&entry.payload).as_bytes(), descriptor.digest);
    let header_limit = executor.limits().snapshot_header_bytes.as_u64();
    let summary = inspect_sealed_container(&entry.payload, header_limit)
        .assured("the complete handoff frame validates without decoding columns");
    assert_eq!(
        summary,
        SealedSnapshotSummary {
            revision: 7,
            fence: 3,
            branch_generation: 5,
            records: 1,
            groups: 1,
        }
    );
    let restored = RestoredMaterializedSnapshot::open_relay(
        &executor,
        &schema,
        RelayContainerSource::Sealed(SealedSource::borrowed(&executor, &entry.payload)),
    )
    .await
    .assured("the handoff opens against the installed schema");
    assert_eq!(restored.records[0].branch, branch);
    assert_eq!(restored.records[0].row.metadata().to_remote(), metadata);
    assert_eq!(
        restored.records[0].row.value_at(0).assured("value loads"),
        Some(RuntimeValue::I64(42))
    );
    for length in [0, 8, 11, 12, entry.payload.len() - 1] {
        let error = inspect_sealed_container(&entry.payload[..length], header_limit)
            .expect_err("a truncated frame cannot be admitted for handoff");
        assert!(matches!(
            error.current_context(),
            MaterializedSnapshotError::Truncated { .. }
        ));
    }
    let mut bytes = entry.payload.clone();
    bytes[0] ^= 1;
    assert!(matches!(
        inspect_sealed_container(&bytes, header_limit)
            .expect_err("a foreign container has no Nervix header")
            .current_context(),
        MaterializedSnapshotError::NotASnapshot
    ));
    let mut bytes = entry.payload.clone();
    bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        inspect_sealed_container(&bytes, header_limit)
            .expect_err("header admission precedes reading its declared bytes")
            .current_context(),
        MaterializedSnapshotError::HeaderTooLarge { .. }
    ));
    let header_length: usize = u32::from_le_bytes(
        entry.payload[8..12]
            .try_into()
            .assured("the header length is four bytes"),
    )
    .arch_into();
    let first_section = CONTAINER_FRAME_BYTES + header_length;
    let mut bytes = entry.payload.clone();
    bytes[first_section] = u8::from(SealedSectionKind::RecordColumns);
    assert!(matches!(
        inspect_sealed_container(&bytes, header_limit)
            .expect_err("each group begins with its identities")
            .current_context(),
        MaterializedSnapshotError::UnexpectedSection { .. }
    ));
    let mut bytes = entry.payload.clone();
    bytes[first_section + 1..first_section + SECTION_FRAME_BYTES]
        .copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        inspect_sealed_container(&bytes, header_limit)
            .expect_err("a section cannot overstate the bytes that remain")
            .current_context(),
        MaterializedSnapshotError::Truncated { .. }
    ));
    let mut bytes = entry.payload.clone();
    bytes.push(17);
    assert!(matches!(
        inspect_sealed_container(&bytes, header_limit)
            .expect_err("a handoff consumes the complete container")
            .current_context(),
        MaterializedSnapshotError::Truncated { .. }
    ));
}

#[nervix_primitives::test]
async fn current_materialized_containers_validate_counts_keys_and_full_consumption() {
    let executor = Executor::default();
    let rows = (0..2)
        .map(|index| test_runtime_row(vec![("value".to_string(), RuntimeValue::I64(index))]))
        .collect::<Vec<_>>();
    let schema = rows[0].arrow_schema();
    let batch = RuntimeRecordBatch::from_rows(schema.clone(), rows.iter())
        .assured("two columnar rows have the same schema");
    let columns = batch
        .encode_arrow_snapshot_section(&executor)
        .await
        .assured("bounded fixture columns encode");
    let alpha = Some(tenant_branch("alpha"));
    let beta = Some(tenant_branch("beta"));
    for (records, groups, keys, trailing, reason) in [
        (0, 1, vec![], false, "group count"),
        (2, 0, vec![], false, "group count"),
        (1, 2, vec![], false, "group count"),
        (u64::MAX, 1, vec![], false, "metadata"),
        (2, 1, vec![alpha.clone(), alpha.clone()], false, "duplicate"),
        (2, 1, vec![None, beta.clone()], false, "unbranched"),
        (2, 1, vec![alpha.clone()], false, "describes"),
        (
            1,
            1,
            vec![alpha.clone(), beta.clone()],
            false,
            "declared count",
        ),
        (3, 1, vec![alpha.clone(), beta.clone()], false, "describes"),
        (2, 1, vec![alpha, beta], true, "trailing"),
    ] {
        let mut bytes = materialized_container_header(7, 3, 5, records, groups)
            .assured("a current header encodes");
        if !keys.is_empty() {
            bytes.extend(
                materialized_identity_section(
                    keys.into_iter()
                        .map(|key| {
                            (
                                BranchKey::to_remote_key(&key),
                                rows[0].metadata().to_remote(),
                            )
                        })
                        .collect(),
                )
                .assured("current scalar identities encode"),
            );
            bytes.extend(materialized_columns_frame(columns.len()).assured("column frame encodes"));
            bytes.extend_from_slice(columns.as_ref());
        }
        if trailing {
            bytes.push(17);
        }
        let error = RestoredMaterializedSnapshot::open_relay(
            &executor,
            &schema,
            RelayContainerSource::Sealed(SealedSource::borrowed(&executor, &bytes)),
        )
        .await
        .expect_err("invalid current snapshot must fail before installation");
        assert!(format!("{error:?}").contains(reason), "{error:?}");
    }
    let empty =
        materialized_container_header(0, 0, 0, 0, 0).assured("an empty current generation encodes");
    let restored = RestoredMaterializedSnapshot::open_relay(
        &executor,
        &schema,
        RelayContainerSource::Sealed(SealedSource::borrowed(&executor, &empty)),
    )
    .await
    .assured("an empty current generation consumes its complete container");
    assert!(restored.records.is_empty());
}

#[nervix_primitives::test]
async fn a_snapshot_larger_than_the_transfer_budget_moves_through_bounded_chunks() {
    let executor = narrow_executor();
    let staging_root = tempfile::tempdir().assured("the test can create a staging directory");
    let staging = SnapshotStaging::new(
        staging_root.path().to_path_buf(),
        executor.clone(),
        SnapshotStagingLimits::default(),
    );
    let generation = wide_generation();
    let schema = generation.records()[0].row.arrow_schema();
    let sealed = generation
        .seal(&executor, &staging)
        .await
        .assured("a generation of bounded records seals into bounded sections");
    assert!(
        sealed.descriptor.length > ByteUnit::Mebibyte(32).as_u64(),
        "the sealed snapshot is {} bytes, which does not exercise a large transfer",
        sealed.descriptor.length
    );
    let four_sections = executor
        .limits()
        .snapshot_section_bytes
        .as_u64()
        .checked_mul(4)
        .assured("four mebibyte-sized sections fit a u64");
    assert!(
        sealed.descriptor.length > four_sections,
        "a snapshot that fits a few sections does not exercise a sectioned transfer"
    );
    let error = sealed
        .clone()
        .into_persisted_entry(&executor)
        .await
        .expect_err("resident handoff refuses a generation above its memory budget");
    assert!(matches!(
        error.current_context(),
        MaterializedSnapshotError::Admission
    ));

    // Move it exactly as a transfer does: bounded chunks into staging, then a length and
    // digest check, then one bounded section read at a time.
    let mut writer = staging
        .stage(sealed.descriptor.length)
        .await
        .assured("the node's staging quota admits one snapshot");
    let mut reader = sealed
        .artifact
        .open_reader()
        .await
        .assured("sealed artifact opens");
    while let Some(chunk) = reader
        .next_chunk(executor.limits().bulk_chunk_bytes.as_u64())
        .await
        .assured("one bounded sealed chunk reads")
    {
        writer
            .write_chunk(chunk)
            .await
            .assured("a staged chunk within the declared length is accepted");
    }
    let staged = writer
        .finish(sealed.descriptor.digest)
        .await
        .assured("a complete transfer matches the length and digest it declared");

    let restored = RestoredMaterializedSnapshot::open_relay(
        &executor,
        &schema,
        RelayContainerSource::Sealed(SealedSource::staged(staged)),
    )
    .await
    .assured("a staged snapshot opens one bounded section at a time");

    assert_eq!(restored.revision, 7);
    assert_eq!(restored.fence, 3);
    assert_eq!(restored.branch_generation, 5);
    assert_eq!(restored.records.len(), RECORDS);
    for (index, record) in restored.records.iter().enumerate() {
        assert_eq!(
            record.branch,
            Some(tenant_branch(&format!("tenant-{index:02}")))
        );
        assert_eq!(
            record
                .row
                .value_at(0)
                .assured("the restored index column loads"),
            Some(RuntimeValue::I64(test_index(index)))
        );
    }
}

#[nervix_primitives::test]
async fn a_truncated_transfer_is_refused_before_anything_reads_it() {
    let executor = narrow_executor();
    let staging_root = tempfile::tempdir().assured("the test can create a staging directory");
    let staging = SnapshotStaging::new(
        staging_root.path().to_path_buf(),
        executor.clone(),
        SnapshotStagingLimits::default(),
    );
    let sealed = wide_generation()
        .seal(&executor, &staging)
        .await
        .assured("a generation of bounded records seals into bounded sections");
    let mut writer = staging
        .stage(sealed.descriptor.length)
        .await
        .assured("the node's staging quota admits one snapshot");
    let mut reader = sealed
        .artifact
        .open_reader()
        .await
        .assured("sealed artifact opens");
    let short = reader
        .next_chunk(executor.limits().bulk_chunk_bytes.as_u64())
        .await
        .assured("a bounded chunk reads")
        .assured("the snapshot contains one chunk");
    writer
        .write_chunk(short)
        .await
        .assured("a chunk within the declared length is accepted");

    assert!(writer.finish(sealed.descriptor.digest).await.is_err());
}

#[nervix_primitives::test]
async fn a_corrupted_transfer_is_refused_before_anything_reads_it() {
    let executor = narrow_executor();
    let staging_root = tempfile::tempdir().assured("the test can create a staging directory");
    let staging = SnapshotStaging::new(
        staging_root.path().to_path_buf(),
        executor.clone(),
        SnapshotStagingLimits::default(),
    );
    let sealed = wide_generation()
        .seal(&executor, &staging)
        .await
        .assured("a generation of bounded records seals into bounded sections");
    let mut writer = staging
        .stage(sealed.descriptor.length)
        .await
        .assured("the node's staging quota admits one snapshot");
    let mut reader = sealed
        .artifact
        .open_reader()
        .await
        .assured("sealed artifact opens");
    while let Some(chunk) = reader
        .next_chunk(executor.limits().bulk_chunk_bytes.as_u64())
        .await
        .assured("a bounded chunk reads")
    {
        writer
            .write_chunk(chunk)
            .await
            .assured("a bounded chunk is accepted");
    }

    assert!(writer.finish([0; 32]).await.is_err());
}
