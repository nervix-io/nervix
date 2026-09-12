//! Sealed, sectioned Raft snapshots and the generations node-owned storage retains.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The bounded section format of a snapshot generation, sealing and atomic
//!   publication, the pins an active transfer holds, and the removal of unreferenced generations.
//! - **Depends on.** Consensus storage, the durable batch, and the execution budget.
//! - **Must not know.** Raft scheduling, peer transport, or what a replicated record means.
//!
//! A generation is a sequence of bounded sections of keyed state-machine records. No value is
//! carried as one aggregate record, so a snapshot of a large cluster state becomes more sections
//! rather than one that no longer fits. Sections are sealed first; the manifest that names the
//! generation, its applied index, and its membership is published afterwards in one durable batch,
//! and only a published generation is ever read.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

use meticulous::OptionExt as _;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use triomphe::Arc;

use crate::{LogIdOf, StoredMembershipOf};

pub(crate) const KEY_MANIFEST: &[u8] = b"manifest";
const SECTION_TAG: u8 = b's';

/// One keyed state-machine record inside a snapshot section.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

/// One bounded part of a snapshot generation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct SnapshotSection {
    pub(crate) records: Vec<StoredRecord>,
}

/// What a durably published snapshot generation is.
///
/// Publishing this record is the moment the generation becomes the node's snapshot: it switches
/// the active generation, the applied index, the membership and the snapshot identity together.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SnapshotManifest {
    pub(crate) generation: u64,
    pub(crate) last_applied_log_id: Option<LogIdOf>,
    pub(crate) last_membership: Arc<StoredMembershipOf>,
    pub(crate) section_count: u32,
    pub(crate) total_bytes: u64,
}

impl SnapshotManifest {
    pub(crate) fn snapshot_meta(
        &self,
    ) -> openraft::SnapshotMeta<
        openraft::type_config::alias::CommittedLeaderIdOf<crate::TypeConfig>,
        nervix_models::ClusterNodeName,
        crate::Node,
    > {
        openraft::SnapshotMeta {
            last_log_id: self.last_applied_log_id.clone(),
            last_membership: self.last_membership.as_ref().clone(),
        }
    }
}

/// The key one section of one generation is stored under.
pub(crate) fn section_key(generation: u64, index: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8 + 4);
    key.push(SECTION_TAG);
    key.extend_from_slice(&generation.to_be_bytes());
    key.extend_from_slice(&index.to_be_bytes());
    key
}

/// The key range covering every section of one generation.
pub(crate) fn generation_prefix(generation: u64) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(1 + 8);
    prefix.push(SECTION_TAG);
    prefix.extend_from_slice(&generation.to_be_bytes());
    prefix
}

/// The generation a stored section key names, or `None` for a key that is not a section.
pub(crate) fn section_generation(key: &[u8]) -> Option<u64> {
    let (tag, rest) = key.split_first()?;
    if *tag != SECTION_TAG {
        return None;
    }
    let generation: [u8; 8] = rest.get(..8)?.try_into().ok()?;
    Some(u64::from_be_bytes(generation))
}

/// Every sealed section of one generation, and what they encode to in total.
pub(crate) struct SealedSections {
    pub(crate) sections: Vec<Vec<u8>>,
    pub(crate) total_bytes: u64,
}

/// Gathers keyed records into sections no larger than the configured section limit.
///
/// A record never spans two sections, so a record larger than the limit forms a section of its own
/// and the bound is the record limit the state machine already enforces.
pub(crate) struct SectionWriter {
    limit: u64,
    pending: SnapshotSection,
    pending_bytes: u64,
    sealed: Vec<Vec<u8>>,
    total_bytes: u64,
}

impl SectionWriter {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            limit,
            pending: SnapshotSection::default(),
            pending_bytes: 0,
            sealed: Vec::new(),
            total_bytes: 0,
        }
    }

    pub(crate) fn push(&mut self, record: StoredRecord) -> io::Result<()> {
        let record_bytes = u64::try_from(
            record
                .key
                .len()
                .checked_add(record.value.len())
                .ok_or_else(|| io::Error::other(crate::durable_batch::StorageFailure::Capacity))?,
        )
        .map_err(io::Error::other)?;
        let would_hold = self
            .pending_bytes
            .checked_add(record_bytes)
            .ok_or_else(|| io::Error::other(crate::durable_batch::StorageFailure::Capacity))?;
        if !self.pending.records.is_empty() && would_hold > self.limit {
            self.seal()?;
        }
        self.pending_bytes = self
            .pending_bytes
            .checked_add(record_bytes)
            .ok_or_else(|| io::Error::other(crate::durable_batch::StorageFailure::Capacity))?;
        self.pending.records.push(record);
        Ok(())
    }

    /// Seal whatever is still pending and hand over every section of this generation.
    pub(crate) fn finish(mut self) -> io::Result<SealedSections> {
        if !self.pending.records.is_empty() {
            self.seal()?;
        }
        Ok(SealedSections {
            sections: self.sealed,
            total_bytes: self.total_bytes,
        })
    }

    fn seal(&mut self) -> io::Result<()> {
        let section = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;
        let encoded = crate::durable_batch::DurableBatch::encode(&section, self.limit)?;
        let encoded_bytes = u64::try_from(encoded.len()).map_err(io::Error::other)?;
        self.total_bytes = self
            .total_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| io::Error::other(crate::durable_batch::StorageFailure::Capacity))?;
        self.sealed.push(encoded);
        Ok(())
    }
}

/// The snapshot generations node-owned storage retains, and who is still reading them.
///
/// The newest published generation is always retained. One older generation stays only while an
/// What a node is keeping in snapshot storage at one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRetention {
    /// The generation a follower would be caught up from.
    pub active_generation: Option<u64>,
    /// Generations an outgoing transfer is holding against deletion.
    pub pinned_generations: usize,
    /// Readers those pins are held for.
    pub pinned_readers: usize,
    /// Pinned generations a newer one displaced, whose transfers restart from the newest.
    pub obsolete_generations: usize,
    /// Generations nothing references, waiting for the next durable batch to delete them.
    pub unreferenced_generations: usize,
}

/// active transfer is reading it; a second older one would exceed that quota, so the transfer
/// holding it is cancelled and restarts from the newest generation.
pub(crate) struct SnapshotGenerations {
    state: Mutex<GenerationState>,
}

struct GenerationState {
    active: Option<SnapshotManifest>,
    next_generation: u64,
    /// Generations an outgoing transfer is still reading, and how many readers each has.
    pinned: BTreeMap<u64, usize>,
    /// Pinned generations whose quota was taken by a newer one. Reading one fails, which ends the
    /// transfer that held it so OpenRaft restarts from the newest generation.
    obsolete: BTreeSet<u64>,
    /// Generations nothing references any more. The next durable batch deletes their sections.
    unreferenced: BTreeSet<u64>,
}

impl SnapshotGenerations {
    pub(crate) fn new(active: Option<SnapshotManifest>) -> Self {
        let next_generation = match &active {
            Some(manifest) => manifest
                .generation
                .checked_add(1)
                .assured("a node that published u64::MAX generations has outlived its storage"),
            None => 1,
        };
        Self {
            state: Mutex::new(GenerationState {
                active,
                next_generation,
                pinned: BTreeMap::new(),
                obsolete: BTreeSet::new(),
                unreferenced: BTreeSet::new(),
            }),
        }
    }

    pub(crate) fn active(&self) -> Option<SnapshotManifest> {
        self.state.lock().active.clone()
    }

    /// What this node is holding in snapshot storage beyond its active generation.
    pub(crate) fn retention(&self) -> SnapshotRetention {
        let state = self.state.lock();
        let mut readers = 0_usize;
        for pinned in state.pinned.values() {
            readers = readers
                .checked_add(*pinned)
                .assured("one node never opens usize::MAX concurrent snapshot transfers");
        }
        SnapshotRetention {
            active_generation: state.active.as_ref().map(|manifest| manifest.generation),
            pinned_generations: state.pinned.len(),
            pinned_readers: readers,
            obsolete_generations: state.obsolete.len(),
            unreferenced_generations: state.unreferenced.len(),
        }
    }

    /// The generation number the next build or staged transfer writes its sections under.
    pub(crate) fn claim_generation(&self) -> u64 {
        let mut state = self.state.lock();
        let generation = state.next_generation;
        state.next_generation = generation
            .checked_add(1)
            .assured("a node that staged u64::MAX generations has outlived its storage");
        generation
    }

    /// Make `manifest` the node's snapshot. Whatever it supersedes becomes unreferenced unless a
    /// transfer still holds it.
    pub(crate) fn publish(&self, manifest: SnapshotManifest) {
        let mut state = self.state.lock();
        let active = manifest.generation;
        if state.next_generation <= active {
            state.next_generation = active
                .checked_add(1)
                .assured("a node that published u64::MAX generations has outlived its storage");
        }
        // Republishing the generation already active supersedes nothing; a resumed install does
        // exactly that once its records are back in place.
        if let Some(superseded) = state.active.replace(manifest)
            && superseded.generation != active
            && !state.pinned.contains_key(&superseded.generation)
        {
            state.unreferenced.insert(superseded.generation);
        }
        // At most one prior generation may stay pinned. An older transfer past that quota is
        // cancelled rather than allowed to hold a third generation in storage.
        let mut prior = Vec::new();
        for pinned in state.pinned.keys() {
            if *pinned == active {
                continue;
            }
            prior.push(*pinned);
        }
        // `pinned` is ordered, so the last entry is the newest prior generation and keeps its pin.
        prior.pop();
        for generation in prior {
            state.obsolete.insert(generation);
        }
    }

    /// Hold `generation` against deletion while a transfer reads it.
    pub(crate) fn pin(&self, generation: u64) -> u64 {
        let mut state = self.state.lock();
        let readers = state.pinned.entry(generation).or_insert(0);
        *readers = readers
            .checked_add(1)
            .assured("one node never opens usize::MAX concurrent snapshot transfers");
        generation
    }

    /// Whether a pinned generation may still be read, or was cancelled to keep retention inside
    /// its quota.
    pub(crate) fn is_obsolete(&self, generation: u64) -> bool {
        self.state.lock().obsolete.contains(&generation)
    }

    pub(crate) fn release(&self, generation: u64) {
        let mut state = self.state.lock();
        let remaining = match state.pinned.get_mut(&generation) {
            Some(readers) => {
                *readers = readers
                    .checked_sub(1)
                    .verified("every release matches a pin taken earlier");
                *readers
            }
            None => 0,
        };
        if remaining > 0 {
            return;
        }
        state.pinned.remove(&generation);
        state.obsolete.remove(&generation);
        if state.active.as_ref().map(|manifest| manifest.generation) != Some(generation) {
            state.unreferenced.insert(generation);
        }
    }

    /// Record every stored generation the published manifest does not name. A restarted node calls
    /// this so an interrupted build or staged transfer leaves nothing behind.
    pub(crate) fn observe_stored(&self, stored: impl Iterator<Item = u64>) {
        let mut state = self.state.lock();
        let active = state.active.as_ref().map(|manifest| manifest.generation);
        let mut orphans = Vec::new();
        for generation in stored {
            if Some(generation) == active {
                continue;
            }
            orphans.push(generation);
        }
        for generation in orphans {
            if state.next_generation <= generation {
                state.next_generation = generation
                    .checked_add(1)
                    .assured("a stored generation number leaves room inside a u64");
            }
            state.unreferenced.insert(generation);
        }
    }

    /// Give up a generation nothing published or pinned, such as an abandoned staged transfer.
    pub(crate) fn abandon(&self, generation: u64) {
        let mut state = self.state.lock();
        if state.active.as_ref().map(|manifest| manifest.generation) == Some(generation)
            || state.pinned.contains_key(&generation)
        {
            return;
        }
        state.unreferenced.insert(generation);
    }

    /// The generations whose sections the next durable batch deletes.
    pub(crate) fn take_unreferenced(&self) -> Vec<u64> {
        std::mem::take(&mut self.state.lock().unreferenced)
            .into_iter()
            .collect()
    }
}

/// A published snapshot generation, read one bounded section at a time.
///
/// Holding this keeps the generation in storage; dropping it releases the hold, after which an
/// unreferenced generation is deleted by the next durable batch.
pub struct SealedSnapshot {
    store: crate::storage::StoreInner,
    manifest: SnapshotManifest,
}

impl SealedSnapshot {
    pub(crate) fn new(store: crate::storage::StoreInner, manifest: SnapshotManifest) -> Self {
        Self { store, manifest }
    }

    pub(crate) fn manifest(&self) -> &SnapshotManifest {
        &self.manifest
    }

    /// One sealed section of this generation.
    pub(crate) async fn section(&self, index: u32) -> io::Result<Vec<u8>> {
        self.store
            .read_section(self.manifest.generation, index)
            .await
    }
}

impl Drop for SealedSnapshot {
    fn drop(&mut self) {
        self.store.snapshots.release(self.manifest.generation);
    }
}
