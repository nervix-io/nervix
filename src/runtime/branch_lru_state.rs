//! The persisted lifecycle checkpoint for concrete processor branches.
//!
//! Layer: data plane.
//! - **Owns.** Encoding and validating branch keys, last activity, and incarnations in the
//!   lifecycle snapshot, and walking a validated stored snapshot one branch at a time.
//! - **Depends on.** Typed branch identities, timestamps, and the state codec.
//! - **Must not know.** NSPL parsing, graph scheduling, connector protocols, or record payloads.

use std::{io::Write, mem::MaybeUninit};

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use nervix_execution::Cancellation;
use nervix_models::{RemoteRuntimeElementValue, RemoteRuntimeField, RemoteRuntimeValue, Timestamp};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use thiserror::Error;

use super::{
    BackupBranchLifecycleEntry, BranchInstanceSnapshotEntry, BranchKey,
    native_checkpoint_encoding::{
        CancellableEntry, CancellableIterator, IteratorAsVec, scratch_bytes,
    },
};

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct BranchLruSnapshotEntry {
    key: Option<Vec<RemoteRuntimeField>>,
    last_ingestion_unix_nanos: i64,
    incarnation: u64,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct BranchLruSnapshot {
    entries: Vec<BranchLruSnapshotEntry>,
}

/// A serialization view of the current archived lifecycle root.
#[derive(Archive, RkyvSerialize)]
#[rkyv(as = ArchivedBranchLruSnapshot)]
#[rkyv(serialize_bounds(__S: rkyv::ser::Writer + rkyv::ser::Allocator, __S::Error: rkyv::rancor::Source))]
struct StreamingBranchLruSnapshot<'a, I: ExactSizeIterator<Item = BranchLruSnapshotEntry> + Clone> {
    #[rkyv(with = IteratorAsVec<CancellableEntry<'a, BranchLruSnapshotEntry>>, omit_bounds)]
    entries: CancellableIterator<'a, I>,
}

/// Writes the current native shape without retaining converted entries or encoded output.
/// The caller holds the archive's preparation charge, including these bounded resolver and
/// per-entry conversion allocations, for the complete storage job.
pub(super) fn write_branch_lru_snapshot(
    entries: impl ExactSizeIterator<Item = BackupBranchLifecycleEntry> + Clone,
    writer: &mut dyn Write,
    cancellation: &Cancellation,
) -> error_stack::Result<(), BranchLruSnapshotError> {
    let encode_error = || BranchLruSnapshotError::Encode {
        entries: entries.len(),
    };
    let mut nested = 0;
    for (index, entry) in entries.clone().enumerate() {
        cancellation.check().change_context_lazy(encode_error)?;
        if entry.incarnation == 0 {
            return Err(Report::new(BranchLruSnapshotError::Incarnation {
                entry: index,
            }));
        }
        let key = BranchKey::from_remote_key(entry.key)
            .change_context(BranchLruSnapshotError::BranchKey { entry: index })?;
        let fields = BranchKey::to_remote_key(&key);
        nested = nested.max(
            field_scratch(fields.as_deref().unwrap_or(&[]))
                .ok_or_else(|| Report::new(encode_error()))?,
        );
    }
    let capacity = scratch_bytes::<BranchLruSnapshotEntry>(entries.len(), nested)
        .ok_or_else(|| Report::new(encode_error()))?;
    let mut scratch = vec![MaybeUninit::uninit(); capacity];
    let snapshot = StreamingBranchLruSnapshot {
        entries: CancellableIterator {
            cancellation,
            entries: entries.clone().map(|entry| {
                // The immutable input was validated above; both serializer passes reconstruct exactly
                // that entry. Typed normalization preserves the ordinary checkpoint's field ordering.
                let key = BranchKey::from_remote_key(entry.key).verified(
                    "every entry of this immutable lifecycle was validated before serialization",
                );
                BranchLruSnapshotEntry {
                    key: BranchKey::to_remote_key(&key),
                    last_ingestion_unix_nanos: entry.last_ingestion.unix_nanos(),
                    incarnation: entry.incarnation,
                }
            }),
        },
    };
    rkyv::api::low::to_bytes_in_with_alloc::<_, _, rkyv::rancor::Error>(
        &snapshot,
        rkyv::ser::writer::IoWriter::new(writer),
        rkyv::ser::allocator::SubAllocator::new(&mut scratch),
    )
    .map(|_| ())
    .map_err(|error| Report::new(error).change_context(encode_error()))
}

fn field_scratch(fields: &[RemoteRuntimeField]) -> Option<usize> {
    let mut bytes = fields
        .len()
        .checked_mul(std::mem::size_of::<<RemoteRuntimeField as Archive>::Resolver>())?;
    for field in fields {
        if let RemoteRuntimeValue::Array(values) | RemoteRuntimeValue::Vec(values) = &field.value {
            bytes = bytes.checked_add(element_scratch(values)?)?;
        }
    }
    Some(bytes)
}

fn element_scratch(values: &[RemoteRuntimeElementValue]) -> Option<usize> {
    let mut bytes = values.len().checked_mul(std::mem::size_of::<
        <RemoteRuntimeElementValue as Archive>::Resolver,
    >())?;
    for value in values {
        if let RemoteRuntimeElementValue::Array(values) | RemoteRuntimeElementValue::Vec(values) =
            value
        {
            bytes = bytes.checked_add(element_scratch(values)?)?;
        }
    }
    Some(bytes)
}

#[derive(Debug, Error)]
pub(super) enum BranchLruSnapshotError {
    #[error("failed to encode {entries} branch-LRU entries")]
    Encode { entries: usize },
    #[error("failed to decode the branch-LRU snapshot")]
    Decode,
    #[error("reading the branch-LRU snapshot was cancelled between entries")]
    Cancelled,
    #[error("branch-LRU entry {entry} carries an invalid branch key")]
    BranchKey { entry: usize },
    #[error("branch-LRU entry {entry} carries no branch incarnation")]
    Incarnation { entry: usize },
    #[error("branch-LRU entry {entry} stores its branch key outside the key's canonical form")]
    NonCanonicalKey { entry: usize },
    #[error("the branch lifecycle has no placement under the committed schedule")]
    Unplaced,
    #[error("failed to read the restorable branch-LRU snapshot")]
    Read,
    #[error("failed to restore the branch of branch-LRU entry {entry}")]
    Restore { entry: usize },
    #[error("failed to persist the branch-LRU snapshot at lsm {lsm}")]
    Persist { lsm: u64 },
}

pub(super) fn encode_branch_lru_snapshot(
    entries: &[BranchInstanceSnapshotEntry<Option<BranchKey>>],
) -> error_stack::Result<Vec<u8>, BranchLruSnapshotError> {
    let snapshot = BranchLruSnapshot {
        entries: entries
            .iter()
            .map(|entry| BranchLruSnapshotEntry {
                key: BranchKey::to_remote_key(&entry.key),
                last_ingestion_unix_nanos: entry.last_ingestion.unix_nanos(),
                incarnation: entry.incarnation,
            })
            .collect(),
    };
    rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot)
        .map(|bytes| bytes.to_vec())
        .map_err(|error| {
            Report::new(BranchLruSnapshotError::Encode {
                entries: entries.len(),
            })
            .attach_printable(error)
        })
}

pub(super) fn decode_branch_lru_snapshot(
    payload: &[u8],
) -> error_stack::Result<Vec<BranchInstanceSnapshotEntry<Option<BranchKey>>>, BranchLruSnapshotError>
{
    let snapshot = access_snapshot(payload)?;
    let mut entries = Vec::with_capacity(snapshot.entries.len());
    for (index, entry) in snapshot.entries.iter().enumerate() {
        entries.push(entry.instance(index)?);
    }
    Ok(entries)
}

fn access_snapshot(
    payload: &[u8],
) -> error_stack::Result<&ArchivedBranchLruSnapshot, BranchLruSnapshotError> {
    rkyv::access::<ArchivedBranchLruSnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| Report::new(BranchLruSnapshotError::Decode).attach_printable(error))
}

impl ArchivedBranchLruSnapshotEntry {
    /// The typed branch the archived entry at `index` records: its key, last ingestion and
    /// incarnation, which is never zero.
    fn instance(
        &self,
        index: usize,
    ) -> error_stack::Result<BranchInstanceSnapshotEntry<Option<BranchKey>>, BranchLruSnapshotError>
    {
        let incarnation = self.incarnation.to_native();
        if incarnation == 0 {
            return Err(Report::new(BranchLruSnapshotError::Incarnation {
                entry: index,
            }));
        }
        let key = BranchKey::from_remote_key(self.stored_key()?)
            .change_context(BranchLruSnapshotError::BranchKey { entry: index })?;
        Ok(BranchInstanceSnapshotEntry {
            key,
            last_ingestion: self.last_ingestion(),
            incarnation,
        })
    }

    /// The entry's branch key in the remote form it is stored in.
    fn stored_key(
        &self,
    ) -> error_stack::Result<Option<Vec<RemoteRuntimeField>>, BranchLruSnapshotError> {
        rkyv::deserialize::<Option<Vec<RemoteRuntimeField>>, rkyv::rancor::Error>(&self.key)
            .map_err(|error| Report::new(BranchLruSnapshotError::Decode).attach_printable(error))
    }

    fn last_ingestion(&self) -> Timestamp {
        Timestamp::from_unix_nanos(self.last_ingestion_unix_nanos.to_native())
    }
}

/// A stored lifecycle snapshot validated once in its aligned allocation. Its branches convert one
/// at a time as they are walked, so reading a lifecycle never collects its keys.
pub(crate) struct NativeBranchLifecycle {
    payload: rkyv::util::AlignedVec<16>,
}

impl NativeBranchLifecycle {
    /// Validates the archived snapshot and every branch's incarnation and typed key, and that each
    /// key is stored in the remote form its typed key gives, checking `cancellation` before each
    /// branch. A validated branch's stored key therefore stands for its typed key unconverted.
    pub(super) fn validate(
        payload: rkyv::util::AlignedVec<16>,
        cancellation: &Cancellation,
    ) -> error_stack::Result<Self, BranchLruSnapshotError> {
        let snapshot = access_snapshot(&payload)?;
        for (index, entry) in snapshot.entries.iter().enumerate() {
            cancellation
                .check()
                .change_context(BranchLruSnapshotError::Cancelled)?;
            let instance = entry.instance(index)?;
            if BranchKey::to_remote_key(&instance.key) != entry.stored_key()? {
                return Err(Report::new(BranchLruSnapshotError::NonCanonicalKey {
                    entry: index,
                }));
            }
        }
        Ok(Self { payload })
    }

    /// Hands `visit` the branches in LRU order, through an iterator it may clone and walk again.
    pub(crate) fn with_branches<R>(&self, visit: impl FnOnce(LifecycleBranches<'_>) -> R) -> R {
        let snapshot = access_snapshot(&self.payload)
            .verified("the lifecycle was validated when it was read and has not changed since");
        visit(LifecycleBranches {
            entries: snapshot.entries.iter().enumerate(),
        })
    }
}

/// One branch of a validated lifecycle, converted only into the form it is asked for.
#[derive(Clone, Copy)]
pub(crate) struct LifecycleBranch<'a> {
    entry: &'a ArchivedBranchLruSnapshotEntry,
    index: usize,
}

impl LifecycleBranch<'_> {
    /// The branch with its typed key, which also names its fingerprint.
    pub(crate) fn typed(self) -> BranchInstanceSnapshotEntry<Option<BranchKey>> {
        self.entry
            .instance(self.index)
            .verified("every branch was validated when the lifecycle was read")
    }

    /// The branch with its key as stored, which validation found to be the remote form of its
    /// typed key.
    pub(crate) fn stored(self) -> BackupBranchLifecycleEntry {
        BackupBranchLifecycleEntry {
            key: self
                .entry
                .stored_key()
                .verified("every branch key was read when the lifecycle was validated"),
            last_ingestion: self.entry.last_ingestion(),
            incarnation: self.entry.incarnation.to_native(),
        }
    }
}

/// The branches of a validated lifecycle in LRU order.
#[derive(Clone)]
pub(crate) struct LifecycleBranches<'a> {
    entries: std::iter::Enumerate<std::slice::Iter<'a, ArchivedBranchLruSnapshotEntry>>,
}

impl<'a> Iterator for LifecycleBranches<'a> {
    type Item = LifecycleBranch<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let (index, entry) = self.entries.next()?;
        Some(LifecycleBranch { entry, index })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries.size_hint()
    }
}

impl ExactSizeIterator for LifecycleBranches<'_> {
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// A branch key's remote form as archived bytes, which tell apart what value equality does not:
/// NaN payloads, signed zeros and datetime offsets.
#[cfg(test)]
fn key_bits(key: &Option<BranchKey>) -> Vec<u8> {
    use meticulous::ResultExt as _;

    rkyv::to_bytes::<rkyv::rancor::Error>(&BranchKey::to_remote_key(key))
        .assured("a remote branch key archives")
        .to_vec()
}

/// Asserts that `restored` holds exactly the entries of `expected`, in order, with bit-exact keys.
#[cfg(test)]
fn assert_same_entries(
    restored: &[BranchInstanceSnapshotEntry<Option<BranchKey>>],
    expected: &[BranchInstanceSnapshotEntry<Option<BranchKey>>],
) {
    assert_eq!(restored.len(), expected.len());
    for (restored, expected) in restored.iter().zip(expected) {
        assert_eq!(key_bits(&restored.key), key_bits(&expected.key));
        assert_eq!(restored.last_ingestion, expected.last_ingestion);
        assert_eq!(restored.incarnation, expected.incarnation);
    }
}

/// A generated lifecycle snapshot stores through `stored`, the checkpoint envelope a node keeps it
/// in, and restores every branch's typed key, last activity and incarnation in order.
#[cfg(test)]
pub(in crate::runtime) fn assert_generated_lifecycle_survives(
    arbitrary: &mut nervix_arbitrary::Arbitrary<'_>,
    stored: impl FnOnce(Vec<u8>) -> Vec<u8>,
) {
    use meticulous::ResultExt as _;

    let entries = arbitrary.records(|arbitrary| BranchInstanceSnapshotEntry {
        key: BranchKey::generated_scope(arbitrary),
        last_ingestion: arbitrary.timestamp(),
        incarnation: arbitrary.positive_u64().get(),
    });
    let payload =
        encode_branch_lru_snapshot(&entries).assured("a bounded generated lifecycle encodes");
    let restored = decode_branch_lru_snapshot(&stored(payload))
        .assured("a stored lifecycle decodes from its own encoding");
    assert_same_entries(&restored, &entries);
}

/// Arbitrary bytes read as a stored lifecycle either fail with the snapshot's typed decode, key or
/// incarnation failure, or restore entries that store back unchanged.
#[cfg(test)]
pub(in crate::runtime) fn assert_lifecycle_payload_decodes_typed(payload: &[u8]) {
    use meticulous::ResultExt as _;

    match decode_branch_lru_snapshot(payload) {
        Ok(entries) => {
            let encoded =
                encode_branch_lru_snapshot(&entries).assured("a decoded lifecycle encodes");
            let again =
                decode_branch_lru_snapshot(&encoded).assured("a re-encoded lifecycle decodes");
            assert_same_entries(&again, &entries);
        }
        Err(report) => assert!(
            matches!(
                report.current_context(),
                BranchLruSnapshotError::Decode
                    | BranchLruSnapshotError::BranchKey { .. }
                    | BranchLruSnapshotError::Incarnation { .. }
            ),
            "{report:?}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use meticulous::OptionExt as _;

    use super::*;

    #[test]
    fn lifecycle_snapshot_preserves_the_branch_incarnation() {
        let entries = vec![BranchInstanceSnapshotEntry {
            key: None,
            last_ingestion: Timestamp::from_unix_nanos(42),
            incarnation: 7,
        }];
        let encoded = encode_branch_lru_snapshot(&entries)
            .expect("a current lifecycle snapshot should encode");
        let decoded = decode_branch_lru_snapshot(&encoded)
            .expect("a current lifecycle snapshot should decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].key, None);
        assert_eq!(decoded[0].last_ingestion, Timestamp::from_unix_nanos(42));
        assert_eq!(decoded[0].incarnation, 7);
    }

    #[test]
    fn lifecycle_snapshot_refuses_an_absent_incarnation() {
        let snapshot = BranchLruSnapshot {
            entries: vec![BranchLruSnapshotEntry {
                key: None,
                last_ingestion_unix_nanos: 42,
                incarnation: 0,
            }],
        };
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot)
            .expect("the malformed current snapshot shape should encode");
        let error = decode_branch_lru_snapshot(&payload)
            .expect_err("an absent branch incarnation must not restore");
        assert!(matches!(
            error.current_context(),
            BranchLruSnapshotError::Incarnation { entry: 0 }
        ));
    }

    fn aligned(payload: &[u8]) -> rkyv::util::AlignedVec<16> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
        aligned.extend_from_slice(payload);
        aligned
    }

    #[nervix_primitives::test]
    async fn a_validated_lifecycle_walks_every_branch_in_order_each_time() {
        let entries = vec![
            BranchInstanceSnapshotEntry {
                key: super::super::string_branch_key("tenant", "beta"),
                last_ingestion: Timestamp::from_unix_nanos(7),
                incarnation: 3,
            },
            BranchInstanceSnapshotEntry {
                key: None,
                last_ingestion: Timestamp::from_unix_nanos(5),
                incarnation: 1,
            },
            BranchInstanceSnapshotEntry {
                key: super::super::string_branch_key("tenant", "alpha"),
                last_ingestion: Timestamp::from_unix_nanos(9),
                incarnation: 8,
            },
        ];
        let payload = encode_branch_lru_snapshot(&entries).assured("a current lifecycle encodes");
        let executor = nervix_execution::Executor::default();
        let reservation = executor
            .reserve(nervix_execution::MemoryClass::Bulk, 1)
            .await
            .assured("the job is admitted");
        let walked = executor
            .run_cpu(
                nervix_execution::CpuClass::Bulk,
                reservation,
                move |_charge, cancellation| {
                    let lifecycle =
                        NativeBranchLifecycle::validate(aligned(&payload), cancellation)
                            .assured("a current lifecycle validates");
                    lifecycle.with_branches(|branches| {
                        assert_eq!(branches.len(), 3);
                        let typed = branches
                            .clone()
                            .map(LifecycleBranch::typed)
                            .collect::<Vec<_>>();
                        let stored = branches.map(LifecycleBranch::stored).collect::<Vec<_>>();
                        assert_eq!(typed.len(), stored.len());
                        for (typed, stored) in typed.iter().zip(&stored) {
                            assert_eq!(BranchKey::to_remote_key(&typed.key), stored.key);
                            assert_eq!(typed.last_ingestion, stored.last_ingestion);
                            assert_eq!(typed.incarnation, stored.incarnation);
                        }
                        typed
                    })
                },
            )
            .await
            .assured("the job finishes");
        assert_same_entries(&walked, &entries);
    }

    #[nervix_primitives::test]
    async fn validating_a_lifecycle_keeps_the_decoder_s_typed_failures() {
        let zero = BranchLruSnapshot {
            entries: vec![BranchLruSnapshotEntry {
                key: None,
                last_ingestion_unix_nanos: 42,
                incarnation: 0,
            }],
        };
        let zero = rkyv::to_bytes::<rkyv::rancor::Error>(&zero)
            .assured("the malformed current snapshot shape encodes")
            .to_vec();
        let empty_key = BranchLruSnapshot {
            entries: vec![BranchLruSnapshotEntry {
                key: Some(Vec::new()),
                last_ingestion_unix_nanos: 0,
                incarnation: 1,
            }],
        };
        let empty_key = rkyv::to_bytes::<rkyv::rancor::Error>(&empty_key)
            .assured("the malformed current snapshot shape encodes")
            .to_vec();
        // A typed key orders its fields by name, so a key stored with them reversed is not the
        // remote form of the key it stands for.
        let field = |name: &str| RemoteRuntimeField {
            name: name.to_string(),
            value: RemoteRuntimeValue::U8(1),
        };
        let unordered_key = BranchLruSnapshot {
            entries: vec![BranchLruSnapshotEntry {
                key: Some(vec![field("zone"), field("tenant")]),
                last_ingestion_unix_nanos: 0,
                incarnation: 1,
            }],
        };
        let unordered_key = rkyv::to_bytes::<rkyv::rancor::Error>(&unordered_key)
            .assured("the unordered current snapshot shape encodes")
            .to_vec();
        let executor = nervix_execution::Executor::default();
        let reservation = executor
            .reserve(nervix_execution::MemoryClass::Bulk, 1)
            .await
            .assured("the job is admitted");
        executor
            .run_cpu(
                nervix_execution::CpuClass::Bulk,
                reservation,
                move |_charge, cancellation| {
                    for (payload, expected) in [
                        (b"not a branch LRU snapshot".to_vec(), "decode"),
                        (zero, "incarnation"),
                        (empty_key, "key"),
                        (unordered_key, "canonical"),
                    ] {
                        let failure =
                            NativeBranchLifecycle::validate(aligned(&payload), cancellation)
                                .err()
                                .assured("an invalid lifecycle is refused");
                        let classified = match failure.current_context() {
                            BranchLruSnapshotError::Decode => "decode",
                            BranchLruSnapshotError::Incarnation { entry: 0 } => "incarnation",
                            BranchLruSnapshotError::BranchKey { entry: 0 } => "key",
                            BranchLruSnapshotError::NonCanonicalKey { entry: 0 } => "canonical",
                            other => panic!("unexpected classification {other:?}"),
                        };
                        assert_eq!(classified, expected);
                    }
                },
            )
            .await
            .assured("the job finishes");
    }

    #[test]
    fn snapshot_decode_failures_keep_their_typed_classification() {
        let malformed = decode_branch_lru_snapshot(b"not a branch LRU snapshot")
            .expect_err("malformed snapshot bytes must fail");
        assert!(matches!(
            malformed.current_context(),
            BranchLruSnapshotError::Decode
        ));

        let snapshot = BranchLruSnapshot {
            entries: vec![BranchLruSnapshotEntry {
                key: Some(Vec::new()),
                last_ingestion_unix_nanos: 0,
                incarnation: 1,
            }],
        };
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot)
            .expect("the current snapshot shape must encode");
        let invalid_key = decode_branch_lru_snapshot(&payload)
            .expect_err("a concrete branch key cannot have zero fields");
        assert!(matches!(
            invalid_key.current_context(),
            BranchLruSnapshotError::BranchKey { entry: 0 }
        ));
    }
}
