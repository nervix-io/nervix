//! The names of the consensus database's keyspaces and the stored-shape marker they carry.
//!
//! Layer: test harness.
//! - **Owns.** Evidence that every keyspace name carries the one fixed-width stored-shape marker,
//!   and that the compile-time check of the names admits exactly the names fjall opens.
//! - **Depends on.** The storage module's keyspace names and name rule, and a real fjall database.
//! - **Must not know.** Raft, the state machine's records or the interconnect.

use fjall::{Database, KeyspaceCreateOptions};
use tempfile::TempDir;

use super::TestResult;
use crate::storage::{
    KEYSPACE_LOGS, KEYSPACE_NAME_MAX_BYTES, KEYSPACE_NAMES, fjall_accepts_keyspace_name,
};

/// Every keyspace is named `raft_`, the one stored-shape marker and its role, and the marker is 16
/// lowercase hexadecimal digits. A stored-shape change replaces the marker instead of lengthening
/// it, so the names keep their length however many changes follow.
#[test]
fn every_keyspace_name_holds_the_one_fixed_width_stored_shape_marker() {
    let Some(marked) = KEYSPACE_LOGS.strip_prefix("raft_") else {
        panic!("the log keyspace {KEYSPACE_LOGS} does not begin with raft_");
    };
    let Some(marker) = marked.strip_suffix("_logs") else {
        panic!("the log keyspace {KEYSPACE_LOGS} does not end with its role");
    };
    assert_eq!(
        marker.len(),
        16,
        "the stored-shape marker {marker} is not 16 digits"
    );
    assert!(
        marker
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')),
        "the stored-shape marker {marker} is not lowercase hexadecimal"
    );
    let expected =
        ["logs", "meta", "state", "snapshots"].map(|role| format!("raft_{marker}_{role}"));
    assert_eq!(KEYSPACE_NAMES.map(str::to_owned), expected);
}

/// The compile-time check of the keyspace names admits exactly the names fjall opens: a name of the
/// longest admitted length opens, and fjall refuses an empty name and one a byte longer with a
/// panic.
#[test]
fn fjall_opens_exactly_the_keyspace_names_the_compile_time_check_admits() -> TestResult {
    let directory = TempDir::new()?;
    let database = Database::builder(directory.path()).open()?;
    let longest = "x".repeat(KEYSPACE_NAME_MAX_BYTES);
    assert!(fjall_accepts_keyspace_name(&longest));
    database.keyspace(&longest, KeyspaceCreateOptions::default)?;
    let too_long = "x".repeat(KEYSPACE_NAME_MAX_BYTES + 1);
    for refused in ["", too_long.as_str()] {
        assert!(!fjall_accepts_keyspace_name(refused));
        let opened = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            database.keyspace(refused, KeyspaceCreateOptions::default)
        }));
        assert!(
            opened.is_err(),
            "fjall opened the keyspace name {refused:?}, which the check refuses"
        );
    }
    Ok(())
}
