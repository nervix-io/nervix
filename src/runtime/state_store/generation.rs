//! Physical storage of one domain's complete checkpoint generation.
//!
//! Layer: infrastructure.
//! - **Owns.** Namespace keys, checkpoint headers, bounded chunk verification and deletion.
//! - **Depends on.** Typed state placements, Fjall database views and BLAKE3.
//! - **Must not know.** Consensus authority grants, archive layouts or running state handles.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "generation storage runs in admitted checkpoint and restore storage jobs"
    )
)]

use super::*;

/// Caller-owned chunk buffers, placement encoding and a bounded deletion batch fit in this
/// reservation, independent of checkpoint length and count. Fjall's internal reads, caches,
/// memtables and snapshot retention have separate allocation ownership.
pub(crate) const RESTORE_STATE_CHUNK_BYTES: usize = 64 * 1024;
const CHUNK_AND_DATABASE_COPIES: u64 = 512 * 1024;
const PLACEMENT_AND_HEADER_ENCODING: u64 = 512 * 1024;
const DELETION_BATCH_AND_COPIES: u64 = 512 * 1024;
const STORAGE_ALLOCATION_OVERHEAD: u64 = 512 * 1024;
pub(crate) const RESTORE_STATE_WORKING_BYTES: u64 = CHUNK_AND_DATABASE_COPIES
    + PLACEMENT_AND_HEADER_ENCODING
    + DELETION_BATCH_AND_COPIES
    + STORAGE_ALLOCATION_OVERHEAD;
const DELETE_BATCH_BYTES: usize = RESTORE_STATE_CHUNK_BYTES;
const MAX_PLACEMENT_BYTES: usize = 60 * 1024;
const MAX_STORAGE_KEY_BYTES: usize = (1_usize << u16::BITS) - 1;
pub(super) const CHUNK_COORDINATE_BYTES: usize = 2 * std::mem::size_of::<u64>() + 2;
const _: () = assert!(MAX_PLACEMENT_BYTES + CHUNK_COORDINATE_BYTES <= MAX_STORAGE_KEY_BYTES);
const _: () = assert!(StoredCheckpoint::SEGMENTED_BYTES <= RESTORE_STATE_CHUNK_BYTES);
pub(super) const STORAGE_FORMAT: &[u8] = b"namespaced-checkpoint-segments";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StateNamespace {
    Initial,
    Restored(u64),
}

pub(super) fn domain_prefix(domain: &DomainName) -> Vec<u8> {
    let mut prefix = domain.as_str().as_bytes().to_vec();
    prefix.push(0);
    prefix
}

impl StateNamespace {
    pub(super) fn prefix(self, domain: &DomainName) -> Vec<u8> {
        let mut prefix = domain_prefix(domain);
        match self {
            Self::Initial => prefix.push(b'i'),
            Self::Restored(generation) => {
                prefix.push(b'r');
                prefix.extend_from_slice(&generation.to_be_bytes());
            }
        }
        prefix.push(0);
        prefix
    }

    pub(super) fn key(
        self,
        placement: &RuntimeStatePlacement,
    ) -> error_stack::Result<Vec<u8>, RuntimePersistenceError> {
        // Branch text is the only variable-size placement component. Refuse an oversized key
        // before copying it; the typed names and fingerprint/generation segments are bounded.
        if placement
            .branch_key
            .as_ref()
            .is_some_and(|branch| branch.as_str().len() > MAX_PLACEMENT_BYTES)
        {
            return Err(Report::new(
                RuntimePersistenceError::CheckpointPlacementTooLarge,
            ));
        }
        let mut key = self.prefix(&placement.domain);
        key.extend_from_slice(&placement.as_storage_key());
        check_placement_bound(&key)?;
        Ok(key)
    }

    pub(super) fn obsolete_at(self, generation: u64) -> bool {
        match self {
            Self::Initial => true,
            Self::Restored(value) => value < generation,
        }
    }
}

pub(super) fn physical_namespace(
    key: &[u8],
) -> error_stack::Result<(StateNamespace, &[u8]), RuntimePersistenceError> {
    let Some(separator) = key.iter().position(|byte| *byte == 0) else {
        return Err(Report::new(RuntimePersistenceError::InvalidStorageFormat));
    };
    // Every key begins with the canonical text of its domain, which no other spelling may stand
    // for.
    let domain = std::str::from_utf8(&key[..separator])
        .change_context(RuntimePersistenceError::InvalidStorageFormat)?;
    DomainName::decode(domain).change_context(RuntimePersistenceError::InvalidStorageFormat)?;
    let domain_end = separator + 1;
    let (namespace, tail) = match key.get(domain_end..) {
        Some([b'i', 0, tail @ ..]) => (StateNamespace::Initial, tail),
        Some([b'r', bytes @ ..]) if bytes.len() >= 9 && bytes[8] == 0 => {
            let generation =
                u64::from_be_bytes(bytes[..8].try_into().verified("eight generation bytes"));
            (StateNamespace::Restored(generation), &bytes[9..])
        }
        _ => return Err(Report::new(RuntimePersistenceError::InvalidStorageFormat)),
    };
    if !tail.starts_with(&key[..domain_end]) {
        return Err(Report::new(RuntimePersistenceError::InvalidStorageFormat));
    }
    Ok((namespace, tail))
}

pub(super) fn physical_placement(
    key: &[u8],
) -> error_stack::Result<(StateNamespace, StoredPlacement), RuntimePersistenceError> {
    let (namespace, tail) = physical_namespace(key)?;
    Ok((namespace, stored_placement(tail)?))
}

pub(super) fn index_key(placement_key: &[u8], lsm: u64) -> Vec<u8> {
    let mut key = placement_key.to_vec();
    key.push(0);
    key.extend_from_slice(&lsm.to_be_bytes());
    key
}

pub(super) fn chunk_prefix(placement_key: &[u8], lsm: u64) -> Vec<u8> {
    let mut prefix = index_key(placement_key, lsm);
    prefix.push(0);
    prefix
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(in crate::runtime) struct CheckpointMetadata {
    pub(in crate::runtime) lsm: u64,
    pub(in crate::runtime) length: u64,
    pub(in crate::runtime) digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(in crate::runtime) enum StoredCheckpoint {
    Inline(PersistedRuntimeStateEntry),
    Segmented(CheckpointMetadata),
}

impl StoredCheckpoint {
    // Segmented headers have no out-of-line archived fields: their encoding is exactly the
    // archived root. Larger current values are Inline. Maintenance never copies or decodes those
    // large payloads; Fjall's size query and caches retain their separate allocation owner.
    pub(super) const SEGMENTED_BYTES: usize = std::mem::size_of::<ArchivedStoredCheckpoint>();

    pub(super) fn decode(raw: &[u8]) -> error_stack::Result<Self, RuntimePersistenceError> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(raw.len());
        aligned.extend_from_slice(raw);
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(&aligned)
            .map_err(|error| Report::new(RuntimePersistenceError::DecodeState(error.to_string())))
    }

    pub(super) fn encode(&self) -> error_stack::Result<Vec<u8>, RuntimePersistenceError> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))?;
        Ok(bytes.to_vec())
    }

    pub(super) fn lsm(&self) -> u64 {
        match self {
            Self::Inline(entry) => entry.lsm,
            Self::Segmented(metadata) => metadata.lsm,
        }
    }
}

/// One placement/revision segment set. Its exclusive cursor skips referenced chunks without
/// visiting their payloads or allocating a collection proportional to the checkpoint length.
pub(super) struct CheckpointChunkSet<'a> {
    pub(super) placement_key: &'a [u8],
    pub(super) lsm: u64,
}

impl<'a> TryFrom<&'a [u8]> for CheckpointChunkSet<'a> {
    type Error = Report<RuntimePersistenceError>;

    fn try_from(key: &'a [u8]) -> Result<Self, Self::Error> {
        let start = key
            .len()
            .checked_sub(CHUNK_COORDINATE_BYTES)
            .ok_or(RuntimePersistenceError::InvalidCheckpointChunks)?;
        let (placement_key, coordinates) = key.split_at(start);
        let revision_end = 1 + std::mem::size_of::<u64>();
        if coordinates[0] != 0 || coordinates[revision_end] != 0 {
            return Err(Report::new(
                RuntimePersistenceError::InvalidCheckpointChunks,
            ));
        }
        physical_placement(placement_key)?;
        let lsm = u64::from_be_bytes(
            coordinates[1..revision_end]
                .try_into()
                .verified("the chunk coordinate contains exactly eight revision bytes"),
        );
        Ok(Self { placement_key, lsm })
    }
}

impl CheckpointChunkSet<'_> {
    pub(super) fn exclusive_end(&self) -> Vec<u8> {
        let mut end = chunk_prefix(self.placement_key, self.lsm);
        *end.last_mut()
            .verified("a constructed chunk prefix ends with a zero separator") = 1;
        end
    }

    pub(super) fn is_selected(
        &self,
        view: &fjall::Snapshot,
        latest: &Keyspace,
    ) -> error_stack::Result<bool, RuntimePersistenceError> {
        let Some(bytes) = view
            .size_of(latest, self.placement_key)
            .change_context(RuntimePersistenceError::ReadValue)?
        else {
            return Ok(false);
        };
        if u64::from(bytes)
            > u64::try_from(StoredCheckpoint::SEGMENTED_BYTES)
                .verified("the fixed archived header size fits u64")
        {
            return Ok(false);
        }
        let raw = view
            .get(latest, self.placement_key)
            .change_context(RuntimePersistenceError::ReadValue)?
            .ok_or(RuntimePersistenceError::ReadValue)?;
        match StoredCheckpoint::decode(&raw)? {
            StoredCheckpoint::Segmented(metadata) => Ok(metadata.lsm == self.lsm),
            StoredCheckpoint::Inline(_) => Ok(false),
        }
    }
}

pub(super) fn check_placement_bound(
    key: &[u8],
) -> error_stack::Result<(), RuntimePersistenceError> {
    if key.len() > MAX_PLACEMENT_BYTES {
        return Err(Report::new(
            RuntimePersistenceError::CheckpointPlacementTooLarge,
        ));
    }
    Ok(())
}

/// Visits one bounded chunk at a time from the same view that selected its checkpoint header.
#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "the storage caller supplies a bounded synchronous chunk consumer and \
                  cancellation check"
    )
)]
pub(super) fn visit_chunks(
    view: &impl Readable,
    chunks: &Keyspace,
    placement_key: &[u8],
    checkpoint: &StoredCheckpoint,
    mut consume: impl FnMut(&[u8]) -> error_stack::Result<(), RuntimePersistenceError>,
) -> error_stack::Result<(), RuntimePersistenceError> {
    let StoredCheckpoint::Segmented(CheckpointMetadata {
        lsm,
        length,
        digest,
    }) = checkpoint
    else {
        return Err(Report::new(RuntimePersistenceError::InvalidStorageFormat));
    };
    let prefix = chunk_prefix(placement_key, *lsm);
    let mut offset = 0_u64;
    let mut hasher = blake3::Hasher::new();
    for item in view.prefix(chunks, &prefix) {
        let (key, bytes) = item
            .into_inner()
            .map_err(|_| RuntimePersistenceError::ReadValue)?;
        let Some(suffix) = key.strip_prefix(prefix.as_slice()) else {
            return Err(Report::new(
                RuntimePersistenceError::InvalidCheckpointChunks,
            ));
        };
        let Ok(encoded_offset) = <[u8; 8]>::try_from(suffix) else {
            return Err(Report::new(
                RuntimePersistenceError::InvalidCheckpointChunks,
            ));
        };
        let chunk_bytes = usize::try_from(
            (*length - offset)
                .min(u64::try_from(RESTORE_STATE_CHUNK_BYTES).verified("chunk size fits")),
        )
        .verified("a bounded chunk fits the address space");
        if u64::from_be_bytes(encoded_offset) != offset
            || bytes.len() != chunk_bytes
            || bytes.is_empty()
        {
            return Err(Report::new(
                RuntimePersistenceError::InvalidCheckpointChunks,
            ));
        }
        consume(bytes.as_ref())?;
        hasher.update(&bytes);
        offset = offset
            .checked_add(u64::try_from(bytes.len()).verified("chunk size fits"))
            .ok_or(RuntimePersistenceError::InvalidCheckpointChunks)?;
    }
    if offset != *length || hasher.finalize().as_bytes() != digest {
        return Err(Report::new(
            RuntimePersistenceError::InvalidCheckpointChunks,
        ));
    }
    Ok(())
}

pub(super) fn read_checkpoint(
    view: &impl Readable,
    chunks: &Keyspace,
    placement_key: &[u8],
    raw: &[u8],
) -> error_stack::Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
    let checkpoint = StoredCheckpoint::decode(raw)?;
    match checkpoint {
        StoredCheckpoint::Inline(entry) => Ok(entry),
        StoredCheckpoint::Segmented(CheckpointMetadata { lsm, length, .. }) => {
            let capacity = usize::try_from(length)
                .map_err(|_| RuntimePersistenceError::InvalidCheckpointChunks)?;
            let mut payload = Vec::new();
            payload
                .try_reserve_exact(capacity)
                .map_err(|_| RuntimePersistenceError::ReadValue)?;
            visit_chunks(view, chunks, placement_key, &checkpoint, |bytes| {
                payload.extend_from_slice(bytes);
                Ok(())
            })?;
            Ok(PersistedRuntimeStateEntry { lsm, payload })
        }
    }
}

/// Iterator snapshots retain only their cursor. A deletion batch contains at most one maximum
/// placement beyond the byte bound and is committed before the next bounded unit.
#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "the admitted storage caller supplies cancellation and namespace selection; \
                  local callbacks remain analyzed"
    )
)]
pub(super) fn remove_bounded(
    db: &Database,
    keyspace: &Keyspace,
    prefix: &[u8],
    mut select: impl FnMut(&[u8]) -> error_stack::Result<bool, RuntimePersistenceError>,
    check: &mut impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
) -> error_stack::Result<u64, RuntimePersistenceError> {
    let mut batch = db.batch();
    let mut bytes = 0_usize;
    let mut reclaimed = 0_u64;
    for item in keyspace.prefix(prefix) {
        check()?;
        let key = item.key().map_err(|_| RuntimePersistenceError::ReadValue)?;
        if !select(&key)? {
            continue;
        }
        if key.len() > MAX_STORAGE_KEY_BYTES {
            return Err(Report::new(
                RuntimePersistenceError::CheckpointPlacementTooLarge,
            ));
        }
        let value_bytes = keyspace
            .size_of(&key)
            .change_context(RuntimePersistenceError::ReadValue)?
            .ok_or(RuntimePersistenceError::ReadValue)?;
        let key_value_bytes = u64::try_from(key.len())
            .verified("bounded database keys fit u64")
            .checked_add(u64::from(value_bytes))
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        reclaimed = reclaimed
            .checked_add(key_value_bytes)
            .ok_or(RuntimePersistenceError::RestoreStagingSize)?;
        bytes = bytes
            .checked_add(key.len() + 32)
            .ok_or(RuntimePersistenceError::WriteValue)?;
        batch.remove(keyspace, key);
        if bytes >= DELETE_BATCH_BYTES {
            batch
                .commit()
                .map_err(|_| RuntimePersistenceError::WriteValue)?;
            batch = db.batch();
            bytes = 0;
        }
    }
    if bytes > 0 {
        batch
            .commit()
            .map_err(|_| RuntimePersistenceError::WriteValue)?;
    }
    Ok(reclaimed)
}
