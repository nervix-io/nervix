use nervix_models::{RemoteRuntimeField, Timestamp};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

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

pub(super) fn encode_branch_lru_snapshot(
    entries: &[(Option<BranchKey>, Timestamp)],
) -> Result<Vec<u8>, String> {
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
        .map_err(|error| error.to_string())
}

pub(super) fn decode_branch_lru_snapshot(
    payload: &[u8],
) -> Result<Vec<(Option<BranchKey>, Timestamp)>, String> {
    let snapshot = rkyv::from_bytes::<BranchLruSnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| error.to_string())?;
    snapshot
        .entries
        .into_iter()
        .map(|entry| {
            BranchKey::from_remote_key(entry.key).map(|key| {
                (
                    key,
                    Timestamp::from_unix_nanos(entry.last_ingestion_unix_nanos),
                )
            })
        })
        .collect()
}
