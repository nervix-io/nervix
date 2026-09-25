//! The persisted lifecycle checkpoint for concrete processor branches.
//!
//! Layer: data plane.
//! - **Owns.** Encoding and validating branch keys, last activity, and incarnations in the
//!   lifecycle snapshot.
//! - **Depends on.** Typed branch identities, timestamps, and the state codec.
//! - **Must not know.** NSPL parsing, graph scheduling, connector protocols, or record payloads.

use error_stack::Report;
use nervix_models::{RemoteRuntimeField, Timestamp};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use thiserror::Error;

use super::{BranchInstanceSnapshotEntry, BranchKey};

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

#[derive(Debug, Error)]
pub(super) enum BranchLruSnapshotError {
    #[error("failed to encode {entries} branch-LRU entries")]
    Encode { entries: usize },
    #[error("failed to decode the branch-LRU snapshot")]
    Decode,
    #[error("branch-LRU entry {entry} carries an invalid branch key")]
    BranchKey { entry: usize },
    #[error("branch-LRU entry {entry} carries no branch incarnation")]
    Incarnation { entry: usize },
    #[error("the branch lifecycle has no placement under the committed schedule")]
    Unplaced,
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
    let snapshot = rkyv::from_bytes::<BranchLruSnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| Report::new(BranchLruSnapshotError::Decode).attach_printable(error))?;
    snapshot
        .entries
        .into_iter()
        .enumerate()
        .map(|(entry_index, entry)| {
            if entry.incarnation == 0 {
                return Err(Report::new(BranchLruSnapshotError::Incarnation {
                    entry: entry_index,
                }));
            }
            let key = BranchKey::from_remote_key(entry.key).map_err(|reason| {
                Report::new(BranchLruSnapshotError::BranchKey { entry: entry_index })
                    .attach_printable(reason)
            })?;
            Ok(BranchInstanceSnapshotEntry {
                key,
                last_ingestion: Timestamp::from_unix_nanos(entry.last_ingestion_unix_nanos),
                incarnation: entry.incarnation,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
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
