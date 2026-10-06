//! Checkpoints stored through a node's runtime-state store and read back, inline or in chunks.
//!
//! Layer: test harness.
//! - **Owns.** Generated checkpoints of every placement, stored inline or as a chunked stream
//!   whose length meets the chunk boundaries, and read back through both readers before and after
//!   the database reopens.
//! - **Depends on.** The runtime-state store, its checkpoint writers and readers, a temporary
//!   database, and the vocabulary generators.
//! - **Must not know.** Replication, assignment authority, or what a payload encodes.

use std::io::{Cursor, Read as _};

use nervix_arbitrary::{Arbitrary, Domain};

use super::{
    generation::{CheckpointMetadata, RESTORE_STATE_CHUNK_BYTES},
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

/// Asserts that both readers of `store` return exactly `payload` at `lsm` for `placement`.
fn assert_reads_back(
    store: &RuntimeStateStore,
    placement: &RuntimeStatePlacement,
    lsm: u64,
    payload: &[u8],
) {
    let latest = store
        .latest_snapshot(placement)
        .assured("a stored checkpoint reads back")
        .verified("the checkpoint was stored for this placement above");
    assert_eq!(latest.lsm, lsm);
    assert_eq!(latest.payload, payload);
    let mut reader = store
        .checkpoint_reader(placement)
        .assured("a stored checkpoint opens for reading")
        .verified("the checkpoint was stored for this placement above");
    assert_eq!(
        reader.remaining(),
        u64::try_from(payload.len()).assured("a payload length fits in u64")
    );
    let mut read = Vec::new();
    reader
        .read_to_end(&mut read)
        .assured("a stored checkpoint reads to its end");
    assert_eq!(read, payload);
}

/// A checkpoint stored inline or as a chunked stream for any placement reads back byte for byte at
/// its revision through the latest-snapshot read and the streaming reader, before and after the
/// database reopens.
#[test]
fn bolero_runtime_state_checkpoints_read_back_from_the_store() {
    bolero::check!()
        .with_iterations(64)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let placement = generated_placement(&mut arbitrary);
            let lsm = arbitrary.entropy().any_u64();
            let payload = payload(&mut arbitrary);
            let streamed = arbitrary.entropy().flag();
            let directory = tempfile::tempdir()
                .assured("the test host provides a writable temporary directory");
            {
                let store = open_store(directory.path());
                if streamed {
                    let metadata = CheckpointMetadata {
                        lsm,
                        length: u64::try_from(payload.len()).assured("a payload length fits"),
                        digest: *blake3::hash(&payload).as_bytes(),
                    };
                    store
                        .checkpoint_stream_writer()
                        .publish_checkpoint_stream(
                            &placement,
                            metadata,
                            Cursor::new(payload.clone()),
                            || Ok(()),
                        )
                        .assured("a bounded checkpoint stream publishes");
                } else {
                    store
                        .publish_sealed_snapshot(&placement, lsm, &payload)
                        .assured("a bounded checkpoint stores inline");
                }
                assert_reads_back(&store, &placement, lsm, &payload);
            }
            let store = open_store(directory.path());
            assert_reads_back(&store, &placement, lsm, &payload);
        });
}
