//! Checkpoints stored through a node's runtime-state store and read back, inline or in chunks.
//!
//! Layer: test harness.
//! - **Owns.** Generated checkpoints of every placement in one domain, stored inline, as a chunked
//!   stream or in a published restore generation, with lengths at and between the chunk
//!   boundaries, beside checkpoints a cut of that domain never lists; and their reads through the
//!   latest-snapshot read, the streaming reader and a backup cut's listing, before and after the
//!   database reopens.
//! - **Depends on.** The runtime-state store, its checkpoint writers and readers, a temporary
//!   database, and the vocabulary generators.
//! - **Must not know.** Replication, assignment authority, or what a payload encodes.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    collections::BTreeMap,
    io::{Cursor, Read as _},
};

use ahash::HashSet;
use nervix_arbitrary::{Arbitrary, Domain};
use nervix_interconnect::backup::RestoreStateInventory;
use nervix_models::RestoreStateAuthority;

use super::{
    checkpoint_reader::ListedCheckpoint,
    generation::{CheckpointMetadata, RESTORE_STATE_CHUNK_BYTES, StateNamespace},
    key_properties::generated_placement,
    *,
};

/// A payload length at, beside, or between the chunk boundaries a stream is cut at, or empty.
fn payload_length(arbitrary: &mut Arbitrary<'_>) -> usize {
    let chunk = RESTORE_STATE_CHUNK_BYTES;
    let lengths = [
        0,
        1,
        chunk - 1,
        chunk,
        chunk + 1,
        2 * chunk - 1,
        2 * chunk,
        2 * chunk + 1,
    ];
    if arbitrary.entropy().flag() {
        arbitrary.entropy().pick(lengths)
    } else {
        arbitrary.entropy().count(3 * chunk)
    }
}

/// `length` bytes that differ from offset to offset, so a chunk stored at the wrong offset or a
/// lost chunk changes what reads back.
fn payload(arbitrary: &mut Arbitrary<'_>) -> Vec<u8> {
    let length = payload_length(arbitrary);
    let mut state = arbitrary.entropy().any_u64() | 1;
    let mut payload = Vec::with_capacity(length);
    for _ in 0..length {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push(state.to_le_bytes()[0]);
    }
    payload
}

fn open_store(path: &std::path::Path) -> RuntimeStateStore {
    let database = Database::builder(path)
        .open()
        .assured("a temporary test directory holds a database");
    RuntimeStateStore::from_database(
        database,
        nervix_execution::Executor::default(),
        crate::runtime::DEFAULT_RESTORE_STAGING_MAX_BYTES,
    )
    .assured("a new test database opens as a runtime-state store")
}

/// How a generated checkpoint reaches the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Writer {
    /// A sealed snapshot, stored inline beside its header.
    Inline,
    /// A periodic checkpoint stream, stored in chunks.
    Streamed,
    /// A restore's staged checkpoint, stored in chunks and selected when its generation publishes.
    Restored,
}

/// One generated checkpoint, the placement it is stored for and how it reaches the store.
struct GeneratedCheckpoint {
    placement: RuntimeStatePlacement,
    lsm: u64,
    payload: Vec<u8>,
    writer: Writer,
}

impl GeneratedCheckpoint {
    fn length(&self) -> u64 {
        u64::try_from(self.payload.len()).assured("a payload length fits in u64")
    }

    fn metadata(&self) -> CheckpointMetadata {
        CheckpointMetadata {
            lsm: self.lsm,
            length: self.length(),
            digest: *blake3::hash(&self.payload).as_bytes(),
        }
    }

    /// What a backup cut charges for the stored checkpoint: an inline checkpoint's whole stored
    /// value, which holds the payload beside its header, or a chunked checkpoint's payload.
    fn stored_bytes(&self) -> u64 {
        match self.writer {
            Writer::Inline => {
                let entry = PersistedRuntimeStateEntry {
                    lsm: self.lsm,
                    payload: self.payload.clone(),
                };
                let stored = StoredCheckpoint::Inline(entry)
                    .encode()
                    .assured("an inline checkpoint of a bounded payload encodes");
                u64::try_from(stored.len()).assured("a stored value's length fits in u64")
            }
            Writer::Streamed | Writer::Restored => self.length(),
        }
    }
}

/// One domain's generated checkpoints, keyed by where its active namespace stores them, and the
/// restore generation published for the domain, if any.
struct GeneratedDomain {
    domain: DomainName,
    restore: Option<RestoreStateAuthority>,
    checkpoints: BTreeMap<Vec<u8>, GeneratedCheckpoint>,
}

impl GeneratedDomain {
    /// Up to three placements of one domain, each with a checkpoint stored inline or as a stream,
    /// or, when the domain has a published restore generation, staged in it.
    fn generated(arbitrary: &mut Arbitrary<'_>) -> Self {
        let first = generated_placement(arbitrary);
        let domain = first.domain.clone();
        let restore = if arbitrary.entropy().flag() {
            Some(arbitrary.restore_state_authority())
        } else {
            None
        };
        let namespace = match &restore {
            Some(authority) => StateNamespace::Restored(authority.generation),
            None => StateNamespace::Initial,
        };
        let mut placements = vec![first];
        let count = arbitrary
            .entropy()
            .positive_count(nonzero_ext::nonzero!(3_usize));
        for _ in 1..count {
            placements.push(generated_placement(arbitrary));
        }
        let mut checkpoints = BTreeMap::new();
        for mut placement in placements {
            placement.domain = domain.clone();
            let writer = if restore.is_some() {
                arbitrary
                    .entropy()
                    .pick([Writer::Inline, Writer::Streamed, Writer::Restored])
            } else {
                arbitrary.entropy().pick([Writer::Inline, Writer::Streamed])
            };
            let lsm = arbitrary.entropy().any_u64();
            let payload = payload(arbitrary);
            let key = namespace
                .key(&placement)
                .assured("a generated placement stays inside the key bound");
            // A placement generated twice keeps its first checkpoint.
            checkpoints
                .entry(key)
                .or_insert_with(|| GeneratedCheckpoint {
                    placement,
                    lsm,
                    payload,
                    writer,
                });
        }
        Self {
            domain,
            restore,
            checkpoints,
        }
    }

    /// Stores every checkpoint: the staged ones first, in the restore generation that then
    /// publishes, and the others through the writers of the namespace that is active after it.
    fn store(&self, store: &RuntimeStateStore) {
        if let Some(authority) = &self.restore {
            let mut inventory = RestoreStateInventory::default();
            for checkpoint in self.checkpoints.values() {
                if checkpoint.writer != Writer::Restored {
                    continue;
                }
                store
                    .stage_restored_checkpoint(
                        authority,
                        &checkpoint.placement,
                        checkpoint.metadata(),
                        checkpoint.payload.as_slice(),
                        || Ok(()),
                    )
                    .assured("a bounded checkpoint stages in its restore generation");
                inventory.checkpoints = inventory
                    .checkpoints
                    .checked_add(1)
                    .assured("a case stages at most three checkpoints");
                inventory.payload_bytes = inventory
                    .payload_bytes
                    .checked_add(checkpoint.length())
                    .assured("a case stages at most three bounded payloads");
            }
            store
                .publish_restored_state(&self.domain, authority, inventory, || Ok(()))
                .assured("a complete restore generation publishes");
        }
        for checkpoint in self.checkpoints.values() {
            match checkpoint.writer {
                Writer::Inline => store
                    .publish_sealed_snapshot(
                        &checkpoint.placement,
                        checkpoint.lsm,
                        &checkpoint.payload,
                    )
                    .assured("a bounded checkpoint stores inline"),
                Writer::Streamed => store
                    .checkpoint_stream_writer()
                    .publish_checkpoint_stream(
                        &checkpoint.placement,
                        checkpoint.metadata(),
                        Cursor::new(checkpoint.payload.clone()),
                        || Ok(()),
                    )
                    .assured("a bounded checkpoint stream publishes"),
                Writer::Restored => {}
            }
        }
    }

    /// Stores checkpoints a cut of this domain never lists: one of its placements in a later
    /// restore generation that is staged but never published, and in another domain.
    fn store_unlisted(&self, store: &RuntimeStateStore, arbitrary: &mut Arbitrary<'_>) {
        let Some(first) = self.checkpoints.values().next() else {
            return;
        };
        let staged = match &self.restore {
            Some(published) => published.generation.checked_add(1),
            None => Some(arbitrary.entropy().any_u64()),
        };
        if let Some(generation) = staged {
            let mut authority = arbitrary.restore_state_authority();
            authority.generation = generation;
            let unlisted = GeneratedCheckpoint {
                placement: first.placement.clone(),
                lsm: arbitrary.entropy().any_u64(),
                payload: payload(arbitrary),
                writer: Writer::Restored,
            };
            store
                .stage_restored_checkpoint(
                    &authority,
                    &unlisted.placement,
                    unlisted.metadata(),
                    unlisted.payload.as_slice(),
                    || Ok(()),
                )
                .assured("a bounded checkpoint stages in an unpublished generation");
        }
        let other: DomainName = arbitrary.rule_name();
        if other != self.domain {
            let mut placement = first.placement.clone();
            placement.domain = other;
            store
                .publish_sealed_snapshot(&placement, first.lsm, &payload(arbitrary))
                .assured("a bounded checkpoint of another domain stores inline");
        }
    }

    /// Asserts that `store` reads every checkpoint back through the latest-snapshot read and the
    /// streaming reader, and that a backup cut of the domain lists exactly these checkpoints, in
    /// key order and filtered by the kinds it selects, each reading back whole through every
    /// reader a listing offers.
    fn assert_reads_back(&self, store: &RuntimeStateStore) {
        let mut kinds = HashSet::default();
        for checkpoint in self.checkpoints.values() {
            assert_checkpoint_reads_back(store, checkpoint);
            kinds.insert(checkpoint.placement.state.kind());
        }
        let kinds = kinds.into_iter().collect::<Vec<_>>();
        let listed = store
            .snapshot_backup_domain(&self.domain, &kinds)
            .assured("a cut of the stored domain lists its checkpoints")
            .checkpoints;
        assert_eq!(listed.len(), self.checkpoints.len());
        for (listed, expected) in listed.iter().zip(self.checkpoints.values()) {
            assert_listed(listed, expected);
        }

        let Some(first) = self.checkpoints.values().next() else {
            return;
        };
        let selected = first.placement.state.kind();
        let listed = store
            .snapshot_backup_domain(&self.domain, &[selected])
            .assured("a cut of one kind lists its checkpoints")
            .checkpoints;
        let mut expected = Vec::new();
        for checkpoint in self.checkpoints.values() {
            if checkpoint.placement.state.kind() == selected {
                expected.push(checkpoint);
            }
        }
        assert_eq!(listed.len(), expected.len());
        for (listed, expected) in listed.iter().zip(expected) {
            assert_listed_placement(listed, expected);
        }
    }
}

/// Asserts that both readers of `store` return exactly the checkpoint's payload at its revision.
fn assert_checkpoint_reads_back(store: &RuntimeStateStore, checkpoint: &GeneratedCheckpoint) {
    let latest = store
        .latest_snapshot(&checkpoint.placement)
        .assured("a stored checkpoint reads back")
        .verified("the checkpoint was stored for this placement above");
    assert_eq!(latest.lsm, checkpoint.lsm);
    assert_eq!(latest.payload, checkpoint.payload);
    let mut reader = store
        .checkpoint_reader(&checkpoint.placement)
        .assured("a stored checkpoint opens for reading")
        .verified("the checkpoint was stored for this placement above");
    assert_eq!(reader.remaining(), checkpoint.length());
    let mut read = Vec::new();
    reader
        .read_to_end(&mut read)
        .assured("a stored checkpoint reads to its end");
    assert_eq!(read, checkpoint.payload);
}

/// Asserts that a listed checkpoint names the placement `expected` is stored for and charges its
/// stored bytes.
fn assert_listed_placement(listed: &ListedCheckpoint, expected: &GeneratedCheckpoint) {
    assert_eq!(listed.placement.state, expected.placement.state);
    assert_eq!(listed.placement.kind, expected.placement.kind);
    assert_eq!(listed.placement.identifier, expected.placement.identifier);
    assert_eq!(
        listed.placement.branch,
        expected
            .placement
            .branch_key
            .as_ref()
            .map(BranchKey::fingerprint)
    );
    assert_eq!(
        listed
            .stored_bytes()
            .assured("a listed checkpoint's stored size reads"),
        expected.stored_bytes()
    );
}

/// Asserts that a listed checkpoint is `expected` and that every reader the listing offers
/// returns exactly its payload at its revision: the aligned copy, the whole entry, and the bounded
/// reader opened with and without its revision.
fn assert_listed(listed: &ListedCheckpoint, expected: &GeneratedCheckpoint) {
    assert_listed_placement(listed, expected);

    let aligned = listed
        .read_aligned(|| Ok(()))
        .assured("a listed checkpoint reads into one aligned allocation");
    assert_eq!(aligned.lsm, expected.lsm);
    assert_eq!(aligned.payload.as_slice(), expected.payload.as_slice());

    let entry = listed
        .read_entry()
        .assured("a listed checkpoint reads whole");
    assert_eq!(entry.lsm, expected.lsm);
    assert_eq!(entry.payload, expected.payload);

    let (revision, mut reader) = listed
        .open_with_revision()
        .assured("a listed checkpoint opens with its revision");
    assert_eq!(revision, expected.lsm);
    assert_eq!(reader.remaining(), expected.length());
    let mut read = Vec::new();
    reader
        .read_to_end(&mut read)
        .assured("a listed checkpoint reads to its end");
    assert_eq!(read, expected.payload);

    let mut reader = listed.open().assured("a listed checkpoint opens");
    let mut read = Vec::new();
    reader
        .read_to_end(&mut read)
        .assured("a listed checkpoint reads to its end");
    assert_eq!(read, expected.payload);
}

/// Checkpoints of up to three placements of one domain, stored inline, as chunked streams or in a
/// published restore generation, at, beside and between the chunk boundaries, read back byte for
/// byte at their revisions through the latest-snapshot read and the streaming reader. A backup cut
/// of the domain lists exactly them in key order, ignoring a later unpublished generation and
/// another domain, and each listed checkpoint reports its stored size and reads back whole through
/// its aligned, entry and streaming readers, before and after the database reopens.
#[test]
fn bolero_runtime_state_checkpoints_read_back_from_the_store() {
    bolero::check!()
        .with_iterations(64)
        .with_max_len(4096)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let case = GeneratedDomain::generated(&mut arbitrary);
            let directory = tempfile::tempdir()
                .assured("the test host provides a writable temporary directory");
            {
                let store = open_store(directory.path());
                case.store(&store);
                case.store_unlisted(&store, &mut arbitrary);
                case.assert_reads_back(&store);
            }
            let store = open_store(directory.path());
            case.assert_reads_back(&store);
        });
}
