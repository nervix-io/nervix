//! Raft log entries, votes, log positions, recovery metadata, snapshot manifests and sections, and
//! their keys, through the codec and key layout the consensus store writes them with.
//!
//! Layer: test harness.
//! - **Owns.** Round-trip and malformed-input properties of the Raft records consensus storage
//!   holds beside the state machine.
//! - **Depends on.** The production record codec, record conversions and key layout, and the
//!   consensus and vocabulary generators.
//! - **Must not know.** Raft scheduling, transport, or how commands are applied.

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::ClusterNodeName;

use super::{
    generators::{entry, optional_log_id, stored_membership, vote},
    *,
};

/// What the consensus store encodes one generated record within. Every generated record is a few
/// kibibytes at most.
const RECORD_LIMIT: u64 = 16 * 1024 * 1024;

/// What one generated snapshot section may hold before the writer starts another.
const SECTION_LIMIT: u64 = 64 * 1024;

pub(super) fn assert_decode_frees_allocations<F, T>(decode: F)
where
    F: Fn() -> T,
{
    drop(decode());
    let before = alloc_count::stats();
    drop(decode());
    let after = alloc_count::stats();
    let allocated = after
        .alloc_calls
        .checked_sub(before.alloc_calls)
        .assured("a thread's allocation count only grows");
    let freed = after
        .dealloc_calls
        .checked_sub(before.dealloc_calls)
        .assured("a thread's deallocation count only grows");
    assert_eq!(
        freed, allocated,
        "a refused Raft archive retains no allocation"
    );
}

/// `value` encoded through the storage codec and decoded back the way recovery reads it.
fn stored_and_recovered<T>(value: &T) -> T
where
    T: crate::durable_batch::StorageEncode + crate::durable_batch::StorageDecode,
{
    let encoded = DurableBatch::encode(value, RECORD_LIMIT)
        .assured("a bounded generated record fits the storage codec budget");
    storage_decode::<T>(&encoded).assured("a current record decodes from its own encoding")
}

/// Whether `error` is the documented failure of a consensus store holding malformed records.
fn is_invalid_storage(error: &io::Error) -> bool {
    let Some(inner) = error.get_ref() else {
        return false;
    };
    matches!(
        inner.downcast_ref::<StorageFailure>(),
        Some(StorageFailure::InvalidState)
    )
}

/// Up to three records of arbitrary key and value bytes, as a snapshot section copies them from
/// the state-machine keyspace.
fn section_records(arbitrary: &mut Arbitrary<'_>) -> Vec<(Vec<u8>, Vec<u8>)> {
    arbitrary.records(|arbitrary| {
        let key = arbitrary.string().into_bytes();
        let value = arbitrary.string().into_bytes();
        (key, value)
    })
}

/// Every Raft record restores through the storage codec and its conversion to the complete value
/// it was stored from, and every stored key names exactly the position or section it was built for.
#[test]
fn bolero_consensus_raft_records_round_trip_through_the_storage_codec() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);

            let vote = vote(&mut arbitrary);
            let restored = stored_and_recovered(&VoteRecord::from(vote.clone())).into_vote();
            assert_eq!(restored, vote);

            let committed = optional_log_id(&mut arbitrary);
            let stored = committed.clone().map(LogIdRecord::from);
            let restored = stored_and_recovered(&stored).map(LogIdRecord::into_log_id);
            assert_eq!(restored, committed);

            let entry = entry(&mut arbitrary);
            let index = entry.log_id.index;
            let encoded = DurableBatch::encode(&EntryRecord::from(entry.clone()), RECORD_LIMIT)
                .assured("a bounded generated entry fits the storage codec budget");
            let restored = StoreInner::decode_log_entry(&StoreInner::log_key(index), &encoded)
                .assured("an entry stored under its own index recovers");
            assert_eq!(restored, entry);
            let index = arbitrary.entropy().any_u64();
            let restored = StoreInner::log_index(&StoreInner::log_key(index))
                .assured("a log key names the index it was built from");
            assert_eq!(restored, index);

            let metadata = StateMetadata {
                encoding: StateEncoding::RestoredLifecycle,
                last_applied_log_id: optional_log_id(&mut arbitrary),
                last_membership: Arc::new(stored_membership(&mut arbitrary)),
                runtime_revision: arbitrary.entropy().any_u64(),
                command_retry_fence: if arbitrary.entropy().flag() {
                    Some(arbitrary.timestamp())
                } else {
                    None
                },
            };
            let restored = StateMetadata::try_from(stored_and_recovered(
                &StateMetadataRecord::from(&metadata),
            ))
            .assured("recovery metadata with a valid membership converts back");
            let StateEncoding::RestoredLifecycle = restored.encoding;
            assert_eq!(restored.last_applied_log_id, metadata.last_applied_log_id);
            assert_eq!(restored.last_membership, metadata.last_membership);
            assert_eq!(restored.runtime_revision, metadata.runtime_revision);
            assert_eq!(restored.command_retry_fence, metadata.command_retry_fence);

            let section_count = arbitrary.entropy().up_to(u64::from(u32::MAX));
            let manifest = SnapshotManifest {
                generation: arbitrary.entropy().any_u64(),
                last_applied_log_id: optional_log_id(&mut arbitrary),
                last_membership: Arc::new(stored_membership(&mut arbitrary)),
                section_count: u32::try_from(section_count).verified("the draw ends at u32::MAX"),
                total_bytes: arbitrary.entropy().any_u64(),
            };
            let restored = SnapshotManifest::try_from(stored_and_recovered(
                &SnapshotManifestRecord::from(&manifest),
            ))
            .assured("a manifest with a valid membership converts back");
            assert_eq!(restored.generation, manifest.generation);
            assert_eq!(restored.last_applied_log_id, manifest.last_applied_log_id);
            assert_eq!(restored.last_membership, manifest.last_membership);
            assert_eq!(restored.section_count, manifest.section_count);
            assert_eq!(restored.total_bytes, manifest.total_bytes);

            let generation = arbitrary.entropy().any_u64();
            let section = arbitrary.entropy().up_to(u64::from(u32::MAX));
            let section = u32::try_from(section).verified("the draw ends at u32::MAX");
            let key = section_key(generation, section);
            assert_eq!(section_generation(&key), Some(generation));
            assert!(key.starts_with(&generation_prefix(generation)));
            assert_eq!(key.len(), 13);
            assert_eq!(key[9..], section.to_be_bytes());

            let records = section_records(&mut arbitrary);
            let mut writer = SectionWriter::new(SECTION_LIMIT);
            for (key, value) in &records {
                let fits = writer
                    .try_push(key, value)
                    .assured("a record of a few bytes is charged without overflow");
                assert!(fits, "three small records fit one section");
            }
            let sealed = writer.finish().assured("a section within its limit seals");
            let restored = match sealed {
                Some(sealed) => {
                    let stored = stored_and_recovered(&sealed);
                    assert_eq!(stored, sealed);
                    let section: SnapshotSection = storage_decode(&stored)
                        .assured("a sealed section decodes from its own encoding");
                    section
                        .records
                        .into_iter()
                        .map(|record| (record.key, record.value))
                        .collect()
                }
                None => Vec::new(),
            };
            assert_eq!(restored, records);
        });
}

/// Arbitrary bytes read as any Raft record, log key or section key either fail with the documented
/// typed failure or name a value that stores back to the same record.
#[test]
fn bolero_malformed_consensus_raft_records_fail_typed() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            assert_decode_frees_allocations(|| {
                drop(StoreInner::decode_log_entry(&StoreInner::log_key(0), bytes));
                drop(storage_decode::<VoteRecord>(bytes));
                drop(storage_decode::<Option<LogIdRecord>>(bytes));
                drop(storage_decode::<StateMetadataRecord>(bytes));
                drop(storage_decode::<SnapshotManifestRecord>(bytes));
                drop(storage_decode::<SnapshotSection>(bytes));
            });
            match StoreInner::decode_log_entry(&StoreInner::log_key(0), bytes) {
                Ok(entry) => {
                    assert_eq!(entry.log_id.index, 0);
                    let record = EntryRecord::from(entry.clone());
                    let restored = stored_and_recovered(&record)
                        .into_entry()
                        .assured("an entry that decoded once decodes again");
                    assert_eq!(restored, entry);
                }
                Err(error) => assert!(is_invalid_storage(&error), "{error:?}"),
            }
            match storage_decode::<VoteRecord>(bytes) {
                Ok(record) => assert_eq!(stored_and_recovered(&record), record),
                Err(error) => assert!(is_invalid_storage(&error), "{error:?}"),
            }
            match storage_decode::<Option<LogIdRecord>>(bytes) {
                Ok(record) => assert_eq!(stored_and_recovered(&record), record),
                Err(error) => assert!(is_invalid_storage(&error), "{error:?}"),
            }
            match storage_decode::<StateMetadataRecord>(bytes) {
                Ok(record) => {
                    if let Err(error) = StateMetadata::try_from(record) {
                        assert!(is_invalid_storage(&error), "{error:?}");
                    }
                }
                Err(error) => assert!(is_invalid_storage(&error), "{error:?}"),
            }
            match storage_decode::<SnapshotManifestRecord>(bytes) {
                Ok(record) => {
                    if let Err(error) = SnapshotManifest::try_from(record) {
                        assert!(is_invalid_storage(&error), "{error:?}");
                    }
                }
                Err(error) => assert!(is_invalid_storage(&error), "{error:?}"),
            }
            if let Err(error) = storage_decode::<SnapshotSection>(bytes) {
                assert!(is_invalid_storage(&error), "{error:?}");
            }
            match StoreInner::log_index(bytes) {
                Ok(index) => assert_eq!(StoreInner::log_key(index), bytes),
                Err(error) => assert!(is_invalid_storage(&error), "{error:?}"),
            }
            if let Some(generation) = section_generation(bytes) {
                assert_eq!(bytes.first(), Some(&b's'));
                assert!(bytes.starts_with(&generation_prefix(generation)));
            }
        });
}

#[test]
fn a_durable_batch_refusing_its_second_node_frees_the_first() {
    let names = vec![
        ClusterNodeName::parse("first_replica").assured("the first node name is valid"),
        ClusterNodeName::parse("second_replica").assured("the second node name is valid"),
    ];
    let mut encoded = DurableBatch::encode(&names, RECORD_LIMIT)
        .assured("a bounded node list fits the durable codec");
    let target = b"second_replica";
    assert_eq!(
        encoded
            .windows(target.len())
            .filter(|window| *window == target)
            .count(),
        1,
        "the second node name occurs once"
    );
    let start = encoded
        .windows(target.len())
        .position(|window| window == target)
        .assured("the second node name is archived");
    encoded[start + 6] = b'!';

    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(encoded.len());
    aligned.extend_from_slice(&encoded);
    rkyv::access::<rkyv::Archived<Vec<ClusterNodeName>>, rkyv::rancor::Error>(&aligned)
        .assured("the changed name leaves a valid archive shape");
    assert_decode_frees_allocations(|| {
        let result = storage_decode::<Vec<ClusterNodeName>>(&encoded);
        assert!(result.as_ref().is_err_and(is_invalid_storage));
        result
    });
}
