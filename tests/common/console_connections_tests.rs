//! Regressions for what a console page's connection record shows.
//!
//! Layer: test harness.
//! - **Owns.** Records in the shape the page's connection recorder reads them out: a session
//!   redirected once and reconnected to its leader, a second session beside the first, sessions
//!   that leave their leader, never reach it or start elsewhere, backup streams that miss the
//!   redirect, and reconnects before and after a loss.
//! - **Depends on.** The console connection record and the console WebSocket paths of the client
//!   wire contract.
//! - **Must not know.** How a console page records its connections or how a node serves them.

use meticulous::ResultExt as _;
use nervix_client_wire::websocket::{
    CONSOLE_BACKUP_DOWNLOAD_PATH, CONSOLE_RESTORE_PATH, CONSOLE_SESSION_PATH,
};
use serde_json::{Value, json};

use crate::console_connections::{
    BackupStream, ConnectionRecordViolation, ConsoleAddress, ConsoleConnectionRecord,
};

/// The console the page is opened on, which redirects its session to the leader's.
const FOLLOWER: &str = "ws://127.0.0.1:41001";
/// The console of the leader the follower redirects to.
const LEADER: &str = "ws://127.0.0.1:41002";
/// A console the session is never redirected to.
const STRANGER: &str = "ws://127.0.0.1:41003";

fn address(address: &str) -> ConsoleAddress {
    ConsoleAddress::try_from(address.to_string())
        .assured("the test names consoles by literal WebSocket origins")
}

/// A record in the shape the page's recorder reads it out.
fn record(connections: Vec<Value>, losses: &[u64]) -> ConsoleConnectionRecord {
    serde_json::from_value(json!({ "connections": connections, "losses": losses }))
        .assured("the test builds every record in the recorder's shape")
}

/// A session redirected from the follower, then lost and connected again to the leader, beside the
/// backup download and the dry run's and the restore's streams.
fn redirected_and_reconnected() -> ConsoleConnectionRecord {
    record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 3 }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 4, "connected": 5, "released": 7 }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 8, "connected": 9, "released": null }),
            json!({ "address": LEADER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 10, "connected": 11, "released": 12 }),
            json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 13, "connected": 14, "released": 15 }),
            json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 16, "connected": 17, "released": 18 }),
        ],
        &[6],
    )
}

#[test]
fn a_page_address_and_a_websocket_origin_name_the_same_console() {
    let page = address("http://127.0.0.1:41002/console/?auth=token");

    assert_eq!(page, address(LEADER));
    assert_ne!(page, address(FOLLOWER));
}

#[test]
fn a_session_redirected_once_and_reconnected_to_its_leader_meets_every_check() {
    let record = redirected_and_reconnected();

    assert_eq!(record.one_session_connection_at_a_time(), Ok(()));
    assert_eq!(
        record.session_followed_redirect(&address(FOLLOWER), &address(LEADER)),
        Ok(())
    );
    assert_eq!(record.backup_streams_reached(&address(LEADER)), Ok(()));
    assert_eq!(
        record.session_reconnected_after_loss(&address(LEADER)),
        Ok(true)
    );
}

#[test]
fn a_session_constructed_while_another_is_held_is_a_second_session() {
    let never_released = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": null }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 3, "connected": 4, "released": null }),
        ],
        &[],
    );
    // Letting go of the first only after constructing the second still held both at once.
    let released_too_late = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 3 }),
            json!({ "address": LEADER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 4, "connected": 5, "released": null }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 6, "connected": 7, "released": 9 }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 8, "connected": 10, "released": null }),
        ],
        &[],
    );

    assert_eq!(
        never_released.one_session_connection_at_a_time(),
        Err(ConnectionRecordViolation::SessionsHeldAtOnce {
            earlier: 1,
            later: 2
        })
    );
    assert_eq!(
        released_too_late.one_session_connection_at_a_time(),
        Err(ConnectionRecordViolation::SessionsHeldAtOnce {
            earlier: 3,
            later: 4
        })
    );
}

#[test]
fn a_session_must_start_where_the_page_was_opened_and_stay_with_its_leader() {
    let started_elsewhere = record(
        vec![json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                     "opened": 1, "connected": 2, "released": null })],
        &[],
    );
    let never_redirected = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 3 }),
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 4, "connected": 5, "released": null }),
        ],
        &[],
    );
    let left_leader = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 3 }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 4, "connected": 5, "released": 6 }),
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 7, "connected": 8, "released": null }),
        ],
        &[],
    );
    let reached_stranger = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 3 }),
            json!({ "address": STRANGER, "path": CONSOLE_SESSION_PATH,
                    "opened": 4, "connected": 5, "released": null }),
        ],
        &[],
    );
    let no_session = record(
        vec![json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                     "opened": 1, "connected": 2, "released": 3 })],
        &[],
    );
    let follower = address(FOLLOWER);
    let leader = address(LEADER);

    assert_eq!(
        started_elsewhere.session_followed_redirect(&follower, &leader),
        Err(ConnectionRecordViolation::SessionStartedElsewhere {
            connection: 1,
            reached: leader.clone(),
            expected: follower.clone(),
        })
    );
    assert_eq!(
        never_redirected.session_followed_redirect(&follower, &leader),
        Err(ConnectionRecordViolation::NeverRedirected {
            leader: leader.clone()
        })
    );
    assert_eq!(
        left_leader.session_followed_redirect(&follower, &leader),
        Err(ConnectionRecordViolation::SessionLeftLeader {
            connection: 3,
            reached: follower.clone(),
            leader: leader.clone(),
        })
    );
    assert_eq!(
        reached_stranger.session_followed_redirect(&follower, &leader),
        Err(ConnectionRecordViolation::SessionReachedStranger {
            connection: 2,
            reached: address(STRANGER),
            origin: follower.clone(),
            leader: leader.clone(),
        })
    );
    assert_eq!(
        no_session.session_followed_redirect(&follower, &leader),
        Err(ConnectionRecordViolation::NoSessionConnection)
    );
}

#[test]
fn backup_streams_must_follow_the_session_to_its_leader() {
    let download_on_follower = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": null }),
            json!({ "address": FOLLOWER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 3, "connected": 4, "released": 5 }),
        ],
        &[],
    );
    let no_restore = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": null }),
            json!({ "address": LEADER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 3, "connected": 4, "released": 5 }),
        ],
        &[],
    );
    let leader = address(LEADER);

    assert_eq!(
        download_on_follower.backup_streams_reached(&leader),
        Err(ConnectionRecordViolation::BackupStreamElsewhere {
            stream: BackupStream::Download,
            connection: 2,
            reached: address(FOLLOWER),
            leader: leader.clone(),
        })
    );
    assert_eq!(
        no_restore.backup_streams_reached(&leader),
        Err(ConnectionRecordViolation::NoBackupStream {
            stream: BackupStream::Restore
        })
    );
}

#[test]
fn only_a_session_connection_opened_after_the_loss_is_a_reconnect() {
    let lost = record(
        vec![json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 4 })],
        &[3],
    );
    let constructed_not_open = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 4 }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 5, "connected": null, "released": null }),
        ],
        &[3],
    );
    let never_lost = record(
        vec![json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                     "opened": 1, "connected": 2, "released": null })],
        &[],
    );
    let leader = address(LEADER);

    assert_eq!(lost.session_reconnected_after_loss(&leader), Ok(false));
    assert_eq!(
        constructed_not_open.session_reconnected_after_loss(&leader),
        Ok(false)
    );
    assert_eq!(
        redirected_and_reconnected().session_reconnected_after_loss(&address(FOLLOWER)),
        Ok(false)
    );
    assert_eq!(
        never_lost.session_reconnected_after_loss(&leader),
        Err(ConnectionRecordViolation::NoLoss)
    );
}
