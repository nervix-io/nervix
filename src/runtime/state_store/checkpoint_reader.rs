//! Sequential reads of a checkpoint selected from one immutable database view.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Pinning checkpoint namespace, metadata and chunks to one snapshot, checking length
//!   and digest while reading at most one stored chunk at a time, and the checkpoints a listed
//!   view opens only when its owner reads them.
//! - **Depends on.** Current checkpoint shapes and Fjall snapshot reads.
//! - **Must not know.** Archive records, schemas or runtime activation.

use std::io::{self, Cursor, Read};

use super::{
    generation::{
        ArchivedStoredCheckpoint, CheckpointMetadata, RESTORE_STATE_CHUNK_BYTES, StoredCheckpoint,
        chunk_prefix, read_checkpoint,
    },
    *,
};

pub(in crate::runtime) enum CheckpointReader {
    Inline(Cursor<Vec<u8>>),
    Segmented {
        view: fjall::Snapshot,
        chunks: Keyspace,
        prefix: Vec<u8>,
        metadata: CheckpointMetadata,
        offset: u64,
        buffer: Cursor<Vec<u8>>,
        hasher: Box<blake3::Hasher>,
    },
}

impl CheckpointReader {
    /// The reader of `checkpoint`, stored under `key`, whose chunks `view` holds.
    fn select(
        view: fjall::Snapshot,
        chunks: Keyspace,
        key: &[u8],
        checkpoint: StoredCheckpoint,
    ) -> Self {
        match checkpoint {
            StoredCheckpoint::Inline(entry) => Self::Inline(Cursor::new(entry.payload)),
            StoredCheckpoint::Segmented(metadata) => Self::Segmented {
                prefix: chunk_prefix(key, metadata.lsm),
                metadata,
                view,
                chunks,
                offset: 0,
                buffer: Cursor::new(Vec::new()),
                hasher: Box::new(blake3::Hasher::new()),
            },
        }
    }

    pub(in crate::runtime) fn remaining(&self) -> u64 {
        match self {
            Self::Inline(reader) => u64::try_from(reader.get_ref().len())
                .verified("a checkpoint length fits")
                .checked_sub(reader.position())
                .assured("a cursor stays within its checkpoint"),
            Self::Segmented {
                metadata, offset, ..
            } => metadata
                .length
                .checked_sub(*offset)
                .assured("a reader stays within its checkpoint"),
        }
    }
}

impl Read for CheckpointReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Inline(reader) => reader.read(output),
            Self::Segmented {
                view,
                chunks,
                prefix,
                metadata,
                offset,
                buffer,
                hasher,
            } => {
                if output.is_empty() {
                    return Ok(0);
                }
                if *offset == metadata.length {
                    if hasher.finalize().as_bytes() != &metadata.digest {
                        return Err(io::Error::other("checkpoint digest mismatch"));
                    }
                    return Ok(0);
                }
                if buffer.position()
                    == u64::try_from(buffer.get_ref().len()).verified("one chunk fits")
                {
                    let mut key = prefix.clone();
                    key.extend_from_slice(&offset.to_be_bytes());
                    let chunk = view
                        .get(&*chunks, key)
                        .map_err(io::Error::other)?
                        .ok_or_else(|| io::Error::other("checkpoint chunk is missing"))?;
                    let expected = (metadata.length - *offset)
                        .min(u64::try_from(RESTORE_STATE_CHUNK_BYTES).verified("chunk size fits"));
                    if u64::try_from(chunk.len()).ok() != Some(expected) {
                        return Err(io::Error::other("checkpoint chunk length mismatch"));
                    }
                    *buffer = Cursor::new(chunk.to_vec());
                }
                let count = buffer.read(output)?;
                hasher.update(&output[..count]);
                *offset = offset
                    .checked_add(u64::try_from(count).verified("one chunk fits"))
                    .ok_or_else(|| io::Error::other("checkpoint length overflow"))?;
                if *offset == metadata.length && hasher.finalize().as_bytes() != &metadata.digest {
                    return Err(io::Error::other("checkpoint digest mismatch"));
                }
                Ok(count)
            }
        }
    }
}

impl RuntimeStateStore {
    /// Select once, keeping the same namespace and chunk view through every section read.
    pub(in crate::runtime) fn checkpoint_reader(
        &self,
        placement: &RuntimeStatePlacement,
    ) -> error_stack::Result<Option<CheckpointReader>, RuntimePersistenceError> {
        let view = self.db.snapshot();
        let namespace =
            backup::active_namespace(&view, &self.restore_publications, &placement.domain)?;
        let key = namespace.key(placement)?;
        let Some(raw) = view
            .get(&self.latest, &key)
            .map_err(|_| RuntimePersistenceError::ReadValue)?
        else {
            return Ok(None);
        };
        self.checkpoint_reader_at(view, &key, &raw).map(Some)
    }

    /// Reuse the caller's selected view and namespace throughout the checkpoint's lifetime.
    pub(in crate::runtime) fn checkpoint_reader_at(
        &self,
        view: fjall::Snapshot,
        key: &[u8],
        raw: &[u8],
    ) -> error_stack::Result<CheckpointReader, RuntimePersistenceError> {
        let checkpoint = StoredCheckpoint::decode(raw)?;
        Ok(CheckpointReader::select(
            view,
            self.checkpoint_chunks.clone(),
            key,
            checkpoint,
        ))
    }
}

/// One checkpoint a database view listed. Listing read only its key and stored header; its
/// payload is read through that same view when its owner opens it, so every checkpoint read from
/// one listing belongs to one cut, however late it is read.
pub(in crate::runtime) struct ListedCheckpoint {
    pub(in crate::runtime) placement: StoredPlacement,
    view: fjall::Snapshot,
    latest: Keyspace,
    chunks: Keyspace,
    key: Vec<u8>,
    stored_bytes: u64,
}

/// A checkpoint's revision and its whole payload in one allocation aligned for archived access.
pub(in crate::runtime) struct AlignedCheckpoint {
    pub(in crate::runtime) lsm: u64,
    pub(in crate::runtime) payload: rkyv::util::AlignedVec<16>,
}

impl ListedCheckpoint {
    /// Lists the checkpoint `view` stores under `key` with the stored value `raw`. Only a value no
    /// larger than a segmented header is decoded: a larger one is an inline checkpoint, and its
    /// length is all a listing needs.
    pub(super) fn new(
        placement: StoredPlacement,
        view: fjall::Snapshot,
        latest: Keyspace,
        chunks: Keyspace,
        key: Vec<u8>,
        raw: &[u8],
    ) -> error_stack::Result<Self, RuntimePersistenceError> {
        let raw_bytes =
            u64::try_from(raw.len()).verified("a stored value is addressed within 64 bits");
        let stored_bytes = if raw.len() > StoredCheckpoint::SEGMENTED_BYTES {
            raw_bytes
        } else {
            match StoredCheckpoint::decode(raw)? {
                StoredCheckpoint::Inline(_) => raw_bytes,
                StoredCheckpoint::Segmented(metadata) => metadata.length,
            }
        };
        Ok(Self {
            placement,
            view,
            latest,
            chunks,
            key,
            stored_bytes,
        })
    }

    /// What the checkpoint occupies as stored: a segmented checkpoint's payload, or an inline
    /// checkpoint's whole stored value, which holds its payload beside its header.
    pub(in crate::runtime) fn stored_bytes(&self) -> u64 {
        self.stored_bytes
    }

    fn raw(&self) -> error_stack::Result<fjall::Slice, RuntimePersistenceError> {
        let raw = self
            .view
            .get(&self.latest, &self.key)
            .map_err(|_| RuntimePersistenceError::ReadValue)?;
        // A value the listing's snapshot listed stays readable through it, so its absence is a
        // storage failure rather than a checkpoint that ended.
        raw.ok_or_else(|| Report::new(RuntimePersistenceError::ReadValue))
    }

    /// A bounded reader of the checkpoint's payload. An inline checkpoint's payload is held by the
    /// reader; a segmented one is read one stored chunk at a time.
    pub(in crate::runtime) fn open(
        &self,
    ) -> error_stack::Result<CheckpointReader, RuntimePersistenceError> {
        let checkpoint = StoredCheckpoint::decode(&self.raw()?)?;
        Ok(CheckpointReader::select(
            self.view.clone(),
            self.chunks.clone(),
            &self.key,
            checkpoint,
        ))
    }

    /// The whole checkpoint and its revision.
    pub(in crate::runtime) fn read_entry(
        &self,
    ) -> error_stack::Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        read_checkpoint(&self.view, &self.chunks, &self.key, &self.raw()?)
    }

    /// The whole checkpoint in one allocation aligned for archived access. An inline payload is
    /// copied once out of its aligned stored value; a segmented one is read one stored chunk at a
    /// time, running `check` before each. The caller's admission covers the copy: at most twice
    /// [`Self::stored_bytes`] while an inline payload moves into alignment, and the payload itself
    /// afterwards.
    pub(in crate::runtime) fn read_aligned(
        &self,
        mut check: impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<AlignedCheckpoint, RuntimePersistenceError> {
        let raw = self.raw()?;
        let mut stored = rkyv::util::AlignedVec::<16>::with_capacity(raw.len());
        stored.extend_from_slice(&raw);
        drop(raw);
        let metadata = {
            let header = rkyv::access::<ArchivedStoredCheckpoint, rkyv::rancor::Error>(&stored)
                .change_context(RuntimePersistenceError::DecodeState)?;
            match header {
                ArchivedStoredCheckpoint::Inline(entry) => {
                    let mut payload =
                        rkyv::util::AlignedVec::<16>::with_capacity(entry.payload.len());
                    payload.extend_from_slice(entry.payload.as_slice());
                    return Ok(AlignedCheckpoint {
                        lsm: entry.lsm.to_native(),
                        payload,
                    });
                }
                ArchivedStoredCheckpoint::Segmented(metadata) => {
                    rkyv::deserialize::<CheckpointMetadata, rkyv::rancor::Error>(metadata)
                        .change_context(RuntimePersistenceError::DecodeState)?
                }
            }
        };
        drop(stored);
        let length = usize::try_from(metadata.length)
            .map_err(|_| Report::new(RuntimePersistenceError::InvalidCheckpointChunks))?;
        let lsm = metadata.lsm;
        let mut reader = CheckpointReader::select(
            self.view.clone(),
            self.chunks.clone(),
            &self.key,
            StoredCheckpoint::Segmented(metadata),
        );
        let mut payload = rkyv::util::AlignedVec::<16>::with_capacity(length);
        let mut block = vec![0_u8; RESTORE_STATE_CHUNK_BYTES.min(length).max(1)];
        loop {
            check()?;
            let read = reader
                .read(&mut block)
                .map_err(Report::new)
                .change_context(RuntimePersistenceError::InvalidCheckpointChunks)?;
            if read == 0 {
                break;
            }
            payload.extend_from_slice(&block[..read]);
        }
        if payload.len() != length {
            return Err(Report::new(
                RuntimePersistenceError::InvalidCheckpointChunks,
            ));
        }
        Ok(AlignedCheckpoint { lsm, payload })
    }
}

#[cfg(test)]
pub(super) mod tests {
    use nervix_interconnect::backup::RestoreStateInventory;

    use super::*;
    use crate::runtime::state_store::generation::StateNamespace;

    fn placement() -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse("orders").assured("domain is valid"),
            kind: ModelKind::Relay,
            identifier: ModelName::parse("state").assured("relay is valid"),
            state: RuntimeState::MaterializedRelay {
                schema: nervix_models::SchemaFingerprint::from_digest([7; 32]).materialized_at(37),
            },
            branch_key: None,
        }
    }

    fn authority() -> nervix_models::RestoreStateAuthority {
        nervix_models::RestoreStateAuthority {
            leader: nervix_models::ClusterNodeName::parse("restored-1").assured("node is valid"),
            term: 4,
            execution: nervix_models::CommandExecutionReference::parse("restore-reader")
                .assured("reference is valid"),
            mutation_revision: 11,
            generation: 19,
        }
    }

    fn store(path: &std::path::Path) -> RuntimeStateStore {
        RuntimeStateStore::from_database(
            Database::builder(path).open().assured("database opens"),
            Executor::default(),
            super::super::DEFAULT_RESTORE_STAGING_MAX_BYTES,
        )
        .assured("store opens")
    }

    fn stage(
        store: &RuntimeStateStore,
        authority: &nervix_models::RestoreStateAuthority,
        placement: &RuntimeStatePlacement,
        revision: u64,
        bytes: &[u8],
    ) {
        store
            .stage_restored_checkpoint(
                authority,
                placement,
                CheckpointMetadata {
                    lsm: revision,
                    length: u64::try_from(bytes.len()).verified("fixture length fits"),
                    digest: *blake3::hash(bytes).as_bytes(),
                },
                bytes,
                || Ok(()),
            )
            .assured("complete checkpoint stages");
    }

    fn publish(
        store: &RuntimeStateStore,
        authority: &nervix_models::RestoreStateAuthority,
        placement: &RuntimeStatePlacement,
        length: usize,
    ) {
        store
            .publish_restored_state(
                &placement.domain,
                authority,
                RestoreStateInventory {
                    checkpoints: 1,
                    payload_bytes: u64::try_from(length).verified("fixture length fits"),
                },
                || Ok(()),
            )
            .assured("complete generation publishes");
    }

    #[test]
    pub(in crate::runtime::state_store) fn a_materialized_reader_retains_its_generation_across_publication_and_queued_writes()
     {
        let directory = tempfile::tempdir().assured("state directory opens");
        let state = store(directory.path());
        let placement = placement();
        let first = authority();
        let bytes = (0_u32..200_000)
            .map(|index| u8::try_from(index % 251).verified("byte fits"))
            .collect::<Vec<_>>();
        stage(&state, &first, &placement, 8, &bytes);
        publish(&state, &first, &placement, bytes.len());
        let mut reader = state
            .checkpoint_reader(&placement)
            .assured("reader opens")
            .assured("checkpoint exists");
        assert_eq!(reader.read(&mut []).assured("empty read succeeds"), 0);
        let mut prefix = [0_u8; 13];
        reader.read_exact(&mut prefix).assured("prefix reads");
        assert_eq!(prefix.as_slice(), &bytes[..13]);
        let writer = state.latest_snapshot_writer();
        let next = nervix_models::RestoreStateAuthority {
            generation: first.generation + 1,
            ..first.clone()
        };
        let replacement = vec![9_u8; 220_000];
        stage(&state, &next, &placement, 9, &replacement);
        publish(&state, &next, &placement, replacement.len());
        writer
            .write_latest_snapshot(&placement, 99, b"queued")
            .assured("queued writer finishes in its selected namespace");
        publish(&state, &next, &placement, replacement.len());
        let mut remainder = Vec::new();
        reader
            .read_to_end(&mut remainder)
            .assured("the pinned snapshot reads through its original digest");
        assert_eq!(remainder, bytes[13..]);
        assert_eq!(reader.read(&mut prefix).assured("verified EOF repeats"), 0);
        drop(reader);
        drop(writer);
        drop(state);
        let state = store(directory.path());
        publish(&state, &next, &placement, replacement.len());
        let mut restored = Vec::new();
        state
            .checkpoint_reader(&placement)
            .assured("reader reopens")
            .assured("checkpoint exists")
            .read_to_end(&mut restored)
            .assured("durable generation reads");
        assert_eq!(restored, replacement);
    }

    #[test]
    pub(in crate::runtime::state_store) fn current_checkpoint_readers_validate_chunks_lengths_and_digests()
     {
        for fault in ["missing", "length", "digest"] {
            let directory = tempfile::tempdir().assured("state directory opens");
            let state = store(directory.path());
            let placement = placement();
            assert!(
                state
                    .checkpoint_reader(&placement)
                    .assured("absent checkpoint reads")
                    .is_none()
            );
            let authority = authority();
            let bytes = vec![5_u8; RESTORE_STATE_CHUNK_BYTES + 17];
            stage(&state, &authority, &placement, 8, &bytes);
            publish(&state, &authority, &placement, bytes.len());
            let key = StateNamespace::Restored(authority.generation)
                .key(&placement)
                .assured("key is bounded");
            let mut chunk = chunk_prefix(&key, 8);
            chunk.extend_from_slice(&0_u64.to_be_bytes());
            match fault {
                "missing" => state
                    .checkpoint_chunks
                    .remove(chunk)
                    .assured("chunk is removed"),
                "length" => state
                    .checkpoint_chunks
                    .insert(chunk, [5_u8])
                    .assured("short chunk is installed"),
                "digest" => state
                    .checkpoint_chunks
                    .insert(chunk, vec![6_u8; RESTORE_STATE_CHUNK_BYTES])
                    .assured("same-length corrupt chunk is installed"),
                _ => unreachable!("finite fixture faults"),
            }
            let mut reader = state
                .checkpoint_reader(&placement)
                .assured("current header selects")
                .assured("checkpoint exists");
            let mut output = Vec::new();
            let error = reader
                .read_to_end(&mut output)
                .expect_err("an invalid current chunk refuses the read");
            assert!(error.to_string().contains(fault), "{error}");
        }
    }

    #[test]
    pub(in crate::runtime::state_store) fn inline_and_empty_current_checkpoints_read_without_a_segment_allocation()
     {
        let directory = tempfile::tempdir().assured("state directory opens");
        let state = store(directory.path());
        let placement = placement();
        state
            .persist_latest_snapshot(&placement, 1, b"native")
            .assured("current inline checkpoint persists");
        let mut reader = state
            .checkpoint_reader(&placement)
            .assured("inline reader selects")
            .assured("checkpoint exists");
        let mut bytes = Vec::new();
        reader
            .read_to_end(&mut bytes)
            .assured("inline checkpoint reads");
        assert_eq!(bytes, b"native");
        let authority = authority();
        stage(&state, &authority, &placement, 2, &[]);
        publish(&state, &authority, &placement, 0);
        let mut reader = state
            .checkpoint_reader(&placement)
            .assured("empty reader selects")
            .assured("checkpoint exists");
        assert_eq!(
            reader
                .read(&mut [0_u8; 1])
                .assured("empty checkpoint digest verifies"),
            0
        );
    }
}
