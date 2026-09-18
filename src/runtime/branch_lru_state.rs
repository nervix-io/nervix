use error_stack::Report;
use nervix_models::{RemoteRuntimeField, Timestamp};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use thiserror::Error;

use super::BranchKey;

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct BranchLruSnapshotEntry {
    key: Option<Vec<RemoteRuntimeField>>,
    last_ingestion_unix_nanos: i64,
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
    #[error("the branch lifecycle has no placement under the committed schedule")]
    Unplaced,
}

pub(super) fn encode_branch_lru_snapshot(
    entries: &[(Option<BranchKey>, Timestamp)],
) -> error_stack::Result<Vec<u8>, BranchLruSnapshotError> {
    let snapshot = BranchLruSnapshot {
        entries: entries
            .iter()
            .map(|(key, last_ingestion)| BranchLruSnapshotEntry {
                key: BranchKey::to_remote_key(key),
                last_ingestion_unix_nanos: last_ingestion.unix_nanos(),
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
) -> error_stack::Result<Vec<(Option<BranchKey>, Timestamp)>, BranchLruSnapshotError> {
    let snapshot = rkyv::from_bytes::<BranchLruSnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| Report::new(BranchLruSnapshotError::Decode).attach_printable(error))?;
    snapshot
        .entries
        .into_iter()
        .enumerate()
        .map(|(entry_index, entry)| {
            let key = BranchKey::from_remote_key(entry.key).map_err(|reason| {
                Report::new(BranchLruSnapshotError::BranchKey { entry: entry_index })
                    .attach_printable(reason)
            })?;
            Ok((
                key,
                Timestamp::from_unix_nanos(entry.last_ingestion_unix_nanos),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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
