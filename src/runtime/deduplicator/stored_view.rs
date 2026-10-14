//! Bounded key groups read from an admitted immutable current keyspace archive.
//!
//! Layer: data plane.
//! - **Owns.** Complete native validation, borrowed key uniqueness, cached group sizes and
//!   conversion of only the selected key group.
//! - **Depends on.** The owning keyspace codec, admitted immutable file views and the executor.
//! - **Must not know.** Archive paths, scheduling, configuration Models or branch activation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns bounded native keyspace conversion")
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(lifecycle, reason = "a backup converts one selected stored keyspace")
)]

use std::{hash::Hasher, ops::Range};

use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_execution::{Executor, MemoryClass, Reservation};
use nervix_primitives::sync::Arc;
use ordered_float::OrderedFloat;

use super::{
    ArchivedDeduplicatorKeyPartSnapshot as Part, ArchivedDeduplicatorKeySnapshot,
    ArchivedDeduplicatorSnapshot, DeduplicatorEntrySnapshot, DeduplicatorKey,
    PublishedDeduplicatorKey, SNAPSHOT_HEADER,
};
use crate::runtime::{
    deduplicator_archive::DeduplicatorArchiveError, snapshot_staging::MappedArtifact,
};

#[derive(Debug, Clone)]
pub(in crate::runtime) struct StoredKeyspaceView {
    // Copying jobs retain the view independently of the size index's borrow.
    view: MappedArtifact,
    index: Arc<KeyspaceIndex>,
}

#[derive(Debug)]
struct KeyspaceIndex {
    prefixes: Vec<u64>,
    _memory: Reservation,
}

impl StoredKeyspaceView {
    pub(in crate::runtime) async fn open(
        executor: &Executor,
        view: MappedArtifact,
    ) -> error_stack::Result<Self, DeduplicatorArchiveError> {
        let working = executor
            .try_reserve(MemoryClass::RestoreMetadata, 1)
            .change_context(DeduplicatorArchiveError::Admission)?;
        let inspecting = executor.clone();
        let index = view
            .inspect(working, move |bytes, _working, cancellation| {
                cancellation
                    .check()
                    .change_context(DeduplicatorArchiveError::Cancelled)?;
                let root = validated_root(bytes)?;
                let count = root.entries.len();
                // A prefix value per entry and the temporary borrowed-key hash table. No key's
                // text or bytes are copied into the uniqueness table.
                let prefix_count = count
                    .checked_add(1)
                    .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
                let memory_bytes = prefix_count
                    .checked_mul(64)
                    .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
                let memory = inspecting
                    .try_reserve(
                        MemoryClass::RestoreMetadata,
                        u64::try_from(memory_bytes)
                            .map_err(|_| Report::new(DeduplicatorArchiveError::Admission))?,
                    )
                    .change_context(DeduplicatorArchiveError::Admission)?;
                let mut unique =
                    ahash::HashSet::with_capacity_and_hasher(count, ahash::RandomState::default());
                let mut prefixes = Vec::with_capacity(prefix_count);
                prefixes.push(0_u64);
                for entry in root.entries.iter() {
                    cancellation
                        .check()
                        .change_context(DeduplicatorArchiveError::Cancelled)?;
                    if !unique.insert(BorrowedKey { key: &entry.key }) {
                        return Err(Report::new(DeduplicatorArchiveError::DuplicateKey));
                    }
                    let mut size = 8_u64;
                    for part in entry.key.0.iter() {
                        let bytes = match part {
                            Part::Utf8(value) => {
                                u64::try_from(value.len()).verified("mapped text length fits")
                            }
                            Part::Bytes(value) => {
                                u64::try_from(value.len()).verified("mapped byte length fits")
                            }
                            _ => 8,
                        };
                        size = size
                            .checked_add(bytes)
                            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
                    }
                    let previous = *prefixes
                        .last()
                        .assured("the index starts with its zero prefix");
                    prefixes.push(
                        previous
                            .checked_add(size)
                            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?,
                    );
                }
                Ok::<_, Report<DeduplicatorArchiveError>>(KeyspaceIndex {
                    prefixes,
                    _memory: memory,
                })
            })
            .await
            .change_context(DeduplicatorArchiveError::Admission)??;
        Ok(Self {
            view,
            index: Arc::new(index),
        })
    }

    pub(in crate::runtime) fn count(&self) -> usize {
        self.index
            .prefixes
            .len()
            .checked_sub(1)
            .assured("an opened index has its zero prefix")
    }

    pub(in crate::runtime) fn group_bytes(
        &self,
        range: &Range<usize>,
    ) -> error_stack::Result<u64, DeduplicatorArchiveError> {
        let start = self
            .index
            .prefixes
            .get(range.start)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Columns))?;
        let end = self
            .index
            .prefixes
            .get(range.end)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Columns))?;
        end.checked_sub(*start)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Columns))
    }

    pub(in crate::runtime) fn groups(&self, limit: u64) -> Vec<Range<usize>> {
        let mut groups = Vec::new();
        let mut start = 0;
        let mut bytes = 0_u64;
        for index in 0..self.count() {
            let size = self.index.prefixes[index + 1] - self.index.prefixes[index];
            let next = bytes.checked_add(size);
            let overruns = match next {
                Some(next) => next > limit,
                None => true,
            };
            if index > start && overruns {
                groups.push(start..index);
                start = index;
                bytes = size;
            } else {
                bytes = next.unwrap_or(limit);
            }
        }
        if start < self.count() {
            groups.push(start..self.count());
        }
        groups
    }

    /// Convert this group under a separate retained charge; the complete keyspace is never
    /// deserialized into a key map or published generation.
    pub(in crate::runtime) async fn read_group(
        &self,
        executor: &Executor,
        range: Range<usize>,
    ) -> error_stack::Result<StoredKeyGroup, DeduplicatorArchiveError> {
        let payload = self.group_bytes(&range)?;
        let rows = range
            .end
            .checked_sub(range.start)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Columns))?;
        let key_bytes = payload
            .checked_mul(4)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
        let row_count =
            u64::try_from(rows).map_err(|_| Report::new(DeduplicatorArchiveError::Admission))?;
        let row_bytes = row_count
            .checked_mul(256)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
        let memory_bytes = key_bytes
            .checked_add(row_bytes)
            .ok_or_else(|| Report::new(DeduplicatorArchiveError::Admission))?;
        let working = executor
            .try_reserve(MemoryClass::RestoreMetadata, memory_bytes.max(1))
            .change_context(DeduplicatorArchiveError::Admission)?;
        self.view
            .inspect(working, move |bytes, memory, cancellation| {
                let root = validated_root(bytes)?;
                let entries = root
                    .entries
                    .get(range)
                    .ok_or_else(|| Report::new(DeduplicatorArchiveError::Columns))?;
                let mut keys = Vec::with_capacity(entries.len());
                for entry in entries {
                    cancellation
                        .check()
                        .change_context(DeduplicatorArchiveError::Cancelled)?;
                    let entry =
                        rkyv::deserialize::<DeduplicatorEntrySnapshot, rkyv::rancor::Error>(entry)
                            .change_context(DeduplicatorArchiveError::Decode)?;
                    keys.push(PublishedDeduplicatorKey {
                        key: Arc::new(DeduplicatorKey::from(entry.key)),
                        seen_at: entry.seen_at,
                    });
                }
                Ok::<_, Report<DeduplicatorArchiveError>>(StoredKeyGroup {
                    keys,
                    _memory: memory,
                })
            })
            .await
            .change_context(DeduplicatorArchiveError::Admission)?
    }
}

pub(in crate::runtime) struct StoredKeyGroup {
    pub(in crate::runtime) keys: Vec<PublishedDeduplicatorKey>,
    pub(in crate::runtime) _memory: Reservation,
}

fn validated_root(
    bytes: &[u8],
) -> error_stack::Result<&ArchivedDeduplicatorSnapshot, DeduplicatorArchiveError> {
    let archive = bytes
        .strip_prefix(SNAPSHOT_HEADER)
        .ok_or_else(|| Report::new(DeduplicatorArchiveError::Decode))?;
    rkyv::access::<ArchivedDeduplicatorSnapshot, rkyv::rancor::Error>(archive)
        .change_context(DeduplicatorArchiveError::Decode)
}

struct BorrowedKey<'a> {
    key: &'a ArchivedDeduplicatorKeySnapshot,
}

impl PartialEq for BorrowedKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        if self.key.0.len() != other.key.0.len() {
            return false;
        }
        for (left, right) in self.key.0.iter().zip(other.key.0.iter()) {
            let equal = match (left, right) {
                (Part::Null, Part::Null) => true,
                (Part::Boolean(a), Part::Boolean(b)) => a == b,
                (Part::Int64(a), Part::Int64(b)) => a == b,
                (Part::UInt64(a), Part::UInt64(b)) => a == b,
                (Part::Datetime(a), Part::Datetime(b)) => a == b,
                (Part::Float64(a), Part::Float64(b)) => {
                    OrderedFloat(f64::from_bits(a.to_native()))
                        == OrderedFloat(f64::from_bits(b.to_native()))
                }
                (Part::Utf8(a), Part::Utf8(b)) => a.as_str() == b.as_str(),
                (Part::Bytes(a), Part::Bytes(b)) => a.as_slice() == b.as_slice(),
                _ => false,
            };
            if !equal {
                return false;
            }
        }
        true
    }
}
impl Eq for BorrowedKey<'_> {}
impl std::hash::Hash for BorrowedKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.0.len().hash(state);
        for part in self.key.0.iter() {
            std::mem::discriminant(part).hash(state);
            match part {
                Part::Null => {}
                Part::Boolean(value) => value.hash(state),
                Part::Int64(value) | Part::Datetime(value) => value.to_native().hash(state),
                Part::UInt64(value) => value.to_native().hash(state),
                Part::Float64(value) => OrderedFloat(f64::from_bits(value.to_native())).hash(state),
                Part::Utf8(value) => value.as_str().hash(state),
                Part::Bytes(value) => value.as_slice().hash(state),
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests remain outside product execution")
)]
mod tests {
    use super::{
        super::{DeduplicatorKeyPartSnapshot, DeduplicatorKeySnapshot, DeduplicatorSnapshot},
        *,
    };
    use crate::runtime::snapshot_staging::{SnapshotStaging, SnapshotStagingLimits};

    #[nervix_primitives::test]
    async fn stored_float_keys_use_the_same_uniqueness_as_running_keys() {
        for values in [
            [0.0_f64.to_bits(), (-0.0_f64).to_bits()],
            [0x7ff8_0000_0000_0001, 0x7ff8_0000_0000_0002],
        ] {
            let directory = tempfile::tempdir().assured("staging directory opens");
            let executor = Executor::default();
            let staging = SnapshotStaging::new(
                directory.path().to_path_buf(),
                executor.clone(),
                SnapshotStagingLimits::default(),
            );
            let native = DeduplicatorSnapshot {
                entries: values
                    .into_iter()
                    .enumerate()
                    .map(|(index, bits)| DeduplicatorEntrySnapshot {
                        key: DeduplicatorKeySnapshot(vec![DeduplicatorKeyPartSnapshot::Float64(
                            bits,
                        )]),
                        seen_at: nervix_models::Timestamp::from_unix_nanos(
                            i64::try_from(index).assured("two sightings fit"),
                        ),
                    })
                    .collect(),
            };
            let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&native)
                .assured("the current native root encodes");
            let mut bytes = SNAPSHOT_HEADER.to_vec();
            bytes.extend_from_slice(&archive);
            let mut writer = staging
                .try_stage(u64::try_from(bytes.len()).assured("fixture fits"))
                .await
                .assured("the fixture has disk quota");
            writer
                .write_chunk(
                    executor
                        .charge_owned(MemoryClass::Bulk, bytes)
                        .await
                        .assured("the bounded fixture is admitted"),
                )
                .await
                .assured("the fixture stages");
            let view = writer
                .finish_artifact()
                .await
                .assured("the fixture finishes")
                .into_mapped()
                .await
                .assured("the fixture view is admitted");
            let error = StoredKeyspaceView::open(&executor, view)
                .await
                .expect_err("equivalent native keys are duplicate keys");
            assert_eq!(
                error.current_context(),
                &DeduplicatorArchiveError::DuplicateKey
            );
            assert_eq!(
                executor.snapshot().restore_metadata_memory.reserved_bytes,
                0
            );
            assert_eq!(executor.snapshot().bulk_memory.reserved_bytes, 0);
        }
    }
}
