//! Persisted runtime-state archive alignment regression.
//!
//! Outside the layer order: a focused test module.
//!
//! - **Owns.** Proving that storage reads restore the alignment required by the current archive.
//! - **Depends on.** The runtime-state entry's current encoding and its storage-boundary helper.
//! - **Must not know.** Runtime scheduling, replication policy, or historical stored shapes.

use rkyv::Archive;

use super::PersistedRuntimeStateEntry;

#[test]
fn persisted_runtime_state_restores_archive_alignment_after_storage_reads() {
    let expected = PersistedRuntimeStateEntry {
        lsm: 7,
        payload: vec![1, 2, 3],
    };
    let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&expected)
        .expect("the current runtime state entry should encode");
    let archive_alignment =
        std::mem::align_of::<<PersistedRuntimeStateEntry as Archive>::Archived>();
    let mut storage = rkyv::util::AlignedVec::<16>::with_capacity(encoded.len() + 1);
    storage.push(0);
    storage.extend_from_slice(&encoded);
    let unaligned = &storage[1..];
    assert_ne!(unaligned.as_ptr().align_offset(archive_alignment), 0);

    let aligned = PersistedRuntimeStateEntry::align_stored_bytes(unaligned);
    assert_eq!(aligned.as_ptr().align_offset(archive_alignment), 0);
    let restored = rkyv::from_bytes::<PersistedRuntimeStateEntry, rkyv::rancor::Error>(&aligned)
        .expect("an aligned current runtime state entry should decode");
    assert_eq!(restored, expected);
}
