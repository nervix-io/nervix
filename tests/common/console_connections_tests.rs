//! Regressions for what a console page's connection record shows.
//!
//! Layer: test harness.
//! - **Owns.** Records in the shape the page's connection recorder reads them out, with the frames
//!   the nodes sent on them: a session redirected once and reconnected to its leader, a session
//!   that follows leadership as it moves, a second session beside the first, sessions that start
//!   elsewhere, are never redirected or reach a node no report named, backup streams that reach a
//!   node no report named, frames that report a leader, report none or do not read, addresses that
//!   name no console, and reconnects before and after a loss.
//! - **Depends on.** The console connection record and the console WebSocket paths, codecs and
//!   messages of the client wire contract.
//! - **Must not know.** How a console page records its connections or how a node serves them.

use std::num::NonZeroU64;

use meticulous::ResultExt as _;
use nervix_client_wire::{
    BackupDownloadMessage, CommandDisposition, CommandOutcome, LeaderRedirect, Leadership,
    LeadershipObserved, NodeEndpoints, NoticeLevel, OutcomeOrigin, Reply, ReplyBody, ReplyDelivery,
    RequestId, RestoreDisposition, RestoreReply, RestoreUploadFailure, ServerNotice,
    SessionEndReason, SessionEnding, SessionLimits,
    websocket::{
        CONSOLE_BACKUP_DOWNLOAD_PATH, CONSOLE_RESTORE_PATH, CONSOLE_SESSION_PATH,
        ServerBackupDownloadWebSocketCodec, ServerRestoreWebSocketCodec, ServerWebSocketCodec,
    },
};
use nervix_models::{ClusterNodeName, CommandExecutionReference};
use serde_json::{Value, json};
use url::Url;

use crate::console_connections::{
    BackupStream, ConnectionPurpose, ConnectionRecordViolation, ConsoleAddress,
    ConsoleAddressError, ConsoleConnectionRecord, FrameReadFailure, ReportedLeaders,
    UnreadableFrame,
};

/// The console the page is opened on, which redirects its session to the leader's.
const FOLLOWER: &str = "ws://127.0.0.1:41001";
/// The console of the leader the follower redirects to.
const LEADER: &str = "ws://127.0.0.1:41002";
/// A console the session is never redirected to.
const STRANGER: &str = "ws://127.0.0.1:41003";

/// The limits the console reads every frame under.
const LIMITS: SessionLimits = SessionLimits::DEFAULT;

fn address(address: &str) -> ConsoleAddress {
    ConsoleAddress::try_from(address.to_string())
        .assured("the test names consoles by literal WebSocket origins")
}

/// A record in the shape the page's recorder reads it out.
fn record(connections: Vec<Value>, losses: &[u64]) -> ConsoleConnectionRecord {
    serde_json::from_value(json!({ "connections": connections, "losses": losses }))
        .assured("the test builds every record in the recorder's shape")
}

/// The leaders the frames of `record` report.
fn leaders(record: &ConsoleConnectionRecord) -> ReportedLeaders {
    record
        .reported_leaders()
        .assured("the test builds every frame with the wire contract's encoders")
}

/// A frame the page received at `received`, in the shape the recorder keeps it.
fn frame(received: u64, bytes: Vec<u8>) -> Value {
    json!({ "received": received, "bytes": bytes })
}

/// The leader whose node advertises `web_console`.
fn leader_advertising(web_console: Url) -> NodeEndpoints {
    NodeEndpoints {
        node: ClusterNodeName::parse("node-leader").assured("a literal node name"),
        grpc_uri: None,
        web_console_uri: Some(web_console),
    }
}

/// The leader `console` as a node that does not lead names it: by the web console a node
/// advertises, under the scheme it serves its pages at.
fn leader_at(console: &str) -> NodeEndpoints {
    let web_console = Url::parse(&console.replace("ws://", "http://"))
        .assured("the test names consoles by literal WebSocket origins");
    leader_advertising(web_console)
}

/// The redirect a node that does not lead answers with while the node at `console` leads.
fn redirect_to(console: &str) -> LeaderRedirect {
    LeaderRedirect {
        leader: Some(leader_at(console)),
    }
}

/// The report the leader opens every console session it serves with.
fn serving() -> Vec<u8> {
    let observed = LeadershipObserved {
        leadership: Leadership::ServingNode(
            ClusterNodeName::parse("node-leader").assured("a literal node name"),
        ),
    };
    let frame = observed
        .encode(&LIMITS)
        .assured("a leadership report fits a frame");
    Vec::from(ServerWebSocketCodec::new(LIMITS).encode(frame))
}

/// A report that the node at `console` leads, as a node that does not lead observes it.
fn leading_elsewhere(console: &str) -> Vec<u8> {
    let observed = LeadershipObserved {
        leadership: Leadership::Remote(leader_at(console)),
    };
    let frame = observed
        .encode(&LIMITS)
        .assured("a leadership report fits a frame");
    Vec::from(ServerWebSocketCodec::new(LIMITS).encode(frame))
}

/// The report a node sends while no node is known to lead, as during an election.
fn no_known_leader() -> Vec<u8> {
    let observed = LeadershipObserved {
        leadership: Leadership::Unknown,
    };
    let frame = observed
        .encode(&LIMITS)
        .assured("a leadership report fits a frame");
    Vec::from(ServerWebSocketCodec::new(LIMITS).encode(frame))
}

/// A notice a node sends every session it serves.
fn notice() -> Vec<u8> {
    let notice = ServerNotice {
        level: NoticeLevel::Info,
        message: "a notice for every session".to_string(),
    };
    let frame = notice
        .encode(&LIMITS)
        .assured("a short notice fits a frame");
    Vec::from(ServerWebSocketCodec::new(LIMITS).encode(frame))
}

/// The ending a console session is given for `reason`.
fn session_ended_for(reason: SessionEndReason) -> Vec<u8> {
    let ending = SessionEnding { reason };
    let frame = ending
        .encode(&LIMITS)
        .assured("a session ending fits a frame");
    Vec::from(ServerWebSocketCodec::new(LIMITS).encode(frame))
}

/// The ending a node that does not lead gives a console session, with `redirect`.
fn session_ended(redirect: LeaderRedirect) -> Vec<u8> {
    session_ended_for(SessionEndReason::LeaderRedirect(redirect))
}

/// The outcome of a command with `disposition`.
fn outcome(disposition: CommandDisposition) -> CommandOutcome {
    CommandOutcome {
        execution_reference: CommandExecutionReference::parse("console-command")
            .assured("a literal execution reference"),
        origin: OutcomeOrigin::Executed,
        disposition,
        message: String::new(),
        diagnostics: Vec::new(),
        statements: Vec::new(),
        transaction: None,
        transaction_admission: None,
        inspection: None,
        wasm_state: None,
        resource: None,
        backup: None,
        restore: None,
    }
}

/// The reply to a command a node could not run because the node at `console` leads.
fn command_redirected(console: &str) -> Vec<u8> {
    let reply = Reply {
        request_id: RequestId::new(NonZeroU64::MIN),
        body: ReplyBody::Command(Box::new(outcome(CommandDisposition::NotLeader(
            redirect_to(console),
        )))),
    };
    let delivery = reply
        .encode(&LIMITS)
        .assured("a redirected command's reply encodes");
    let ReplyDelivery::Frame(frame) = delivery else {
        panic!("a redirected command's reply did not fit one frame");
    };
    Vec::from(ServerWebSocketCodec::new(LIMITS).encode(frame))
}

/// The answer to a download from a node that neither leads nor retains the archive, while the node
/// at `console` leads.
fn download_redirected(console: &str) -> Vec<u8> {
    let frame = BackupDownloadMessage::encode_redirect(&redirect_to(console), &LIMITS)
        .assured("a download redirect fits a frame");
    Vec::from(ServerBackupDownloadWebSocketCodec::new(LIMITS).encode(frame))
}

/// The last frame of a download that delivered its archive.
fn download_completed() -> Vec<u8> {
    let frame =
        BackupDownloadMessage::encode_complete(&LIMITS).assured("a completion fits a frame");
    Vec::from(ServerBackupDownloadWebSocketCodec::new(LIMITS).encode(frame))
}

/// The reply that ends a restore stream with `disposition`.
fn restore_answered(disposition: RestoreDisposition) -> Vec<u8> {
    let reply = RestoreReply {
        request_id: Some(RequestId::new(NonZeroU64::MIN)),
        disposition,
    };
    let frame = reply
        .encode(&LIMITS)
        .assured("a restore's reply fits a frame");
    Vec::from(ServerRestoreWebSocketCodec::new(LIMITS).encode(frame))
}

/// The reply to a restore a node could not run because the node at `console` leads.
fn restore_redirected(console: &str) -> Vec<u8> {
    restore_answered(RestoreDisposition::Outcome(Box::new(outcome(
        CommandDisposition::NotLeader(redirect_to(console)),
    ))))
}

/// A session redirected from the follower, then lost and connected again to the leader, beside the
/// backup download and the dry run's and the restore's streams.
fn redirected_and_reconnected() -> ConsoleConnectionRecord {
    record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 4,
                    "frames": [frame(3, session_ended(redirect_to(LEADER)))] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 5, "connected": 6, "released": 9,
                    "frames": [frame(7, serving())] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 10, "connected": 11, "released": null,
                    "frames": [frame(12, serving())] }),
            json!({ "address": LEADER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 13, "connected": 14, "released": 16,
                    "frames": [frame(15, download_completed())] }),
            json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 17, "connected": 18, "released": 19, "frames": [] }),
            json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 20, "connected": 21, "released": 22, "frames": [] }),
        ],
        &[8],
    )
}

#[test]
fn a_page_address_a_websocket_origin_and_a_redirect_name_the_same_console() {
    let page = address("http://127.0.0.1:41002/console/?auth=token");
    let advertised = Url::parse("http://127.0.0.1:41002/").assured("a literal web console URL");

    assert_eq!(page, address(LEADER));
    assert_ne!(page, address(FOLLOWER));
    assert_eq!(
        ConsoleAddress::try_from(&advertised).ok(),
        Some(address(LEADER))
    );
}

#[test]
fn an_address_names_a_console_only_by_a_host_and_a_port() {
    let not_url = ConsoleAddress::try_from("a console".to_string());
    let no_host = ConsoleAddress::try_from("data:text/plain,a-console".to_string());
    // A scheme with no default port leaves the port to the address, which names none.
    let no_port = ConsoleAddress::try_from("console://127.0.0.1/".to_string());

    assert!(matches!(not_url, Err(ConsoleAddressError::NotUrl { .. })));
    assert!(matches!(no_host, Err(ConsoleAddressError::NoHost { .. })));
    assert!(matches!(no_port, Err(ConsoleAddressError::NoPort { .. })));
}

#[test]
fn a_session_redirected_once_and_reconnected_to_its_leader_meets_every_check() {
    let record = redirected_and_reconnected();
    let leaders = leaders(&record);

    assert_eq!(record.one_session_connection_at_a_time(), Ok(()));
    assert_eq!(
        record.session_followed_reported_leaders(&leaders, &address(FOLLOWER), &address(LEADER)),
        Ok(())
    );
    assert_eq!(
        record.backup_streams_reached_reported_leaders(&leaders),
        Ok(())
    );
    assert_eq!(record.session_reconnected_where_it_was_lost(), Ok(()));
}

/// The record of the failure these checks were rewritten for: leadership moved from the leader
/// back to the follower the page was opened on after the redirect. The leader ended the session
/// while it knew no leader, the session connected to it again and was redirected to the follower,
/// and the restore that had been streaming to the former leader was sent there again before its
/// own redirect took it to the follower.
#[test]
fn a_session_that_follows_a_leadership_move_meets_every_check() {
    let record = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 4,
                    "frames": [frame(3, session_ended(redirect_to(LEADER)))] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 5, "connected": 6, "released": 25,
                    "frames": [
                        frame(7, serving()),
                        frame(24, session_ended(LeaderRedirect { leader: None })),
                    ] }),
            json!({ "address": LEADER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 8, "connected": 9, "released": 11,
                    "frames": [frame(10, download_completed())] }),
            json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 12, "connected": 13, "released": 14, "frames": [] }),
            json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 15, "connected": 16, "released": 23, "frames": [] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 26, "connected": 27, "released": 29,
                    "frames": [frame(28, session_ended(redirect_to(FOLLOWER)))] }),
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 30, "connected": 31, "released": null,
                    "frames": [frame(32, serving())] }),
            json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 33, "connected": 34, "released": 36,
                    "frames": [frame(35, restore_redirected(FOLLOWER))] }),
            json!({ "address": FOLLOWER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 37, "connected": 38, "released": 39, "frames": [] }),
        ],
        &[],
    );
    let leaders = leaders(&record);

    assert_eq!(record.one_session_connection_at_a_time(), Ok(()));
    assert_eq!(
        record.session_followed_reported_leaders(&leaders, &address(FOLLOWER), &address(LEADER)),
        Ok(())
    );
    assert_eq!(
        record.backup_streams_reached_reported_leaders(&leaders),
        Ok(())
    );
}

#[test]
fn a_session_constructed_while_another_is_held_is_a_second_session() {
    let never_released = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": null, "frames": [] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 3, "connected": 4, "released": null, "frames": [] }),
        ],
        &[],
    );
    // Letting go of the first only after constructing the second still held both at once.
    let released_too_late = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 3, "frames": [] }),
            json!({ "address": LEADER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 4, "connected": 5, "released": null, "frames": [] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 6, "connected": 7, "released": 9, "frames": [] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 8, "connected": 10, "released": null, "frames": [] }),
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
fn a_session_must_start_where_the_page_was_opened_and_follow_its_redirect() {
    let started_elsewhere = record(
        vec![json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                     "opened": 1, "connected": 2, "released": null,
                     "frames": [frame(3, serving())] })],
        &[],
    );
    let never_redirected = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 3, "frames": [] }),
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 4, "connected": 5, "released": null, "frames": [] }),
        ],
        &[],
    );
    let reached_stranger = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 4,
                    "frames": [frame(3, session_ended(redirect_to(STRANGER)))] }),
            json!({ "address": STRANGER, "path": CONSOLE_SESSION_PATH,
                    "opened": 5, "connected": 6, "released": null, "frames": [] }),
        ],
        &[],
    );
    // The page reached the leader before any node had named it: the only report of it arrived on
    // that connection itself.
    let reached_leader_unreported = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 3, "frames": [] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 4, "connected": 5, "released": null,
                    "frames": [frame(6, serving())] }),
        ],
        &[],
    );
    let no_session = record(
        vec![json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                     "opened": 1, "connected": 2, "released": 3, "frames": [] })],
        &[],
    );
    let follower = address(FOLLOWER);
    let leader = address(LEADER);

    assert_eq!(
        started_elsewhere.session_followed_reported_leaders(
            &leaders(&started_elsewhere),
            &follower,
            &leader
        ),
        Err(ConnectionRecordViolation::SessionStartedElsewhere {
            connection: 1,
            reached: leader.clone(),
            expected: follower.clone(),
        })
    );
    assert_eq!(
        never_redirected.session_followed_reported_leaders(
            &leaders(&never_redirected),
            &follower,
            &leader
        ),
        Err(ConnectionRecordViolation::NeverRedirected {
            leader: leader.clone()
        })
    );
    assert_eq!(
        reached_stranger.session_followed_reported_leaders(
            &leaders(&reached_stranger),
            &follower,
            &leader
        ),
        Err(ConnectionRecordViolation::SessionReachedStranger {
            connection: 2,
            reached: address(STRANGER),
            origin: follower.clone(),
            leader: leader.clone(),
        })
    );
    assert_eq!(
        reached_leader_unreported.session_followed_reported_leaders(
            &leaders(&reached_leader_unreported),
            &follower,
            &leader
        ),
        Err(ConnectionRecordViolation::SessionReachedUnreportedNode {
            connection: 2,
            reached: leader.clone(),
        })
    );
    assert_eq!(
        no_session.session_followed_reported_leaders(&leaders(&no_session), &follower, &leader),
        Err(ConnectionRecordViolation::NoSessionConnection)
    );
}

#[test]
fn a_redirected_session_reaches_only_nodes_reported_as_the_leader() {
    // After its redirect the session went back to the follower, which no node had named.
    let back_to_its_origin = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 4,
                    "frames": [frame(3, session_ended(redirect_to(LEADER)))] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 5, "connected": 6, "released": 8,
                    "frames": [frame(7, serving())] }),
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 9, "connected": 10, "released": null, "frames": [] }),
        ],
        &[],
    );
    // The leader answered a command with a redirect after leadership moved, and the session
    // followed it to the node it named.
    let followed_a_command_redirect = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 4,
                    "frames": [frame(3, session_ended(redirect_to(LEADER)))] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 5, "connected": 6, "released": 9,
                    "frames": [frame(7, serving()), frame(8, command_redirected(STRANGER))] }),
            json!({ "address": STRANGER, "path": CONSOLE_SESSION_PATH,
                    "opened": 10, "connected": 11, "released": null,
                    "frames": [frame(12, serving())] }),
        ],
        &[],
    );
    let follower = address(FOLLOWER);
    let leader = address(LEADER);

    assert_eq!(
        back_to_its_origin.session_followed_reported_leaders(
            &leaders(&back_to_its_origin),
            &follower,
            &leader
        ),
        Err(ConnectionRecordViolation::SessionReachedUnreportedNode {
            connection: 3,
            reached: follower.clone(),
        })
    );
    assert_eq!(
        followed_a_command_redirect.session_followed_reported_leaders(
            &leaders(&followed_a_command_redirect),
            &follower,
            &leader
        ),
        Ok(())
    );
}

#[test]
fn backup_streams_must_reach_nodes_reported_as_the_leader() {
    let download_on_follower = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": null,
                    "frames": [frame(3, serving())] }),
            json!({ "address": FOLLOWER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 4, "connected": 5, "released": 6, "frames": [] }),
        ],
        &[],
    );
    // A download a node redirected streams again from the leader it named, but no restore
    // followed it.
    let download_redirected_without_restore = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": null,
                    "frames": [frame(3, serving())] }),
            json!({ "address": LEADER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 4, "connected": 5, "released": 7,
                    "frames": [frame(6, download_redirected(STRANGER))] }),
            json!({ "address": STRANGER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 8, "connected": 9, "released": 11,
                    "frames": [frame(10, download_completed())] }),
        ],
        &[],
    );
    // The restore reached the stranger before the redirect that names it arrived.
    let restore_ahead_of_its_report = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": null,
                    "frames": [frame(3, serving())] }),
            json!({ "address": LEADER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 4, "connected": 5, "released": 6, "frames": [] }),
            json!({ "address": STRANGER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 7, "connected": 8, "released": 10, "frames": [] }),
            json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 11, "connected": 12, "released": 14,
                    "frames": [frame(13, restore_redirected(STRANGER))] }),
        ],
        &[],
    );

    assert_eq!(
        download_on_follower
            .backup_streams_reached_reported_leaders(&leaders(&download_on_follower)),
        Err(
            ConnectionRecordViolation::BackupStreamReachedUnreportedNode {
                stream: BackupStream::Download,
                connection: 2,
                reached: address(FOLLOWER),
            }
        )
    );
    assert_eq!(
        download_redirected_without_restore.backup_streams_reached_reported_leaders(&leaders(
            &download_redirected_without_restore
        )),
        Err(ConnectionRecordViolation::NoBackupStream {
            stream: BackupStream::Restore
        })
    );
    assert_eq!(
        restore_ahead_of_its_report
            .backup_streams_reached_reported_leaders(&leaders(&restore_ahead_of_its_report)),
        Err(
            ConnectionRecordViolation::BackupStreamReachedUnreportedNode {
                stream: BackupStream::Restore,
                connection: 3,
                reached: address(STRANGER),
            }
        )
    );
}

#[test]
fn every_report_of_a_leader_names_its_console_from_its_first_moment() {
    let record = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 4,
                    "frames": [frame(3, session_ended(LeaderRedirect { leader: None }))] }),
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 5, "connected": 6, "released": 8,
                    "frames": [frame(7, session_ended(redirect_to(LEADER)))] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 9, "connected": 10, "released": 20,
                    "frames": [
                        frame(11, serving()),
                        frame(13, leading_elsewhere(FOLLOWER)),
                        frame(19, command_redirected(STRANGER)),
                    ] }),
            // A download the page made while its session was held reports the stranger before
            // the session's later redirect does.
            json!({ "address": LEADER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 12, "connected": 14, "released": 16,
                    "frames": [frame(15, download_redirected(STRANGER))] }),
            json!({ "address": STRANGER, "path": CONSOLE_BACKUP_DOWNLOAD_PATH,
                    "opened": 17, "connected": 18, "released": 22,
                    "frames": [frame(21, download_completed())] }),
        ],
        &[],
    );

    // A redirect that knows no leader and a download's own frames name none. The leader that
    // serves the session names itself, and each console counts from the first report of it.
    assert_eq!(
        leaders(&record).to_string(),
        "\n  127.0.0.1:41001 first reported at 13\n  127.0.0.1:41002 first reported at 7\n  \
         127.0.0.1:41003 first reported at 15"
    );
}

#[test]
fn frames_that_know_no_leader_or_answer_otherwise_report_none() {
    let record = record(
        vec![
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 6,
                    "frames": [
                        frame(3, notice()),
                        frame(4, no_known_leader()),
                        frame(5, session_ended_for(SessionEndReason::ServerShuttingDown)),
                    ] }),
            json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 7, "connected": 8, "released": 10,
                    "frames": [frame(9, restore_answered(RestoreDisposition::UploadFailed {
                        failure: RestoreUploadFailure::SizeMismatch,
                        message: "the archive is shorter than declared".to_string(),
                    }))] }),
            json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 11, "connected": 12, "released": 14,
                    "frames": [frame(13, restore_answered(RestoreDisposition::Outcome(Box::new(
                        outcome(CommandDisposition::Completed { already_existed: false }),
                    ))))] }),
            // No console call is served at this path, so its frames are not read at all.
            json!({ "address": LEADER, "path": "/elsewhere",
                    "opened": 15, "connected": 16, "released": 18,
                    "frames": [frame(17, vec![1, 2, 3])] }),
        ],
        &[],
    );

    assert_eq!(
        leaders(&record).to_string(),
        "\n  no node was reported as the leader"
    );
}

#[test]
fn a_frame_that_does_not_read_as_its_call_fails_the_record() {
    let misread = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": null,
                    "frames": [frame(3, serving())] }),
            // A download's frame where a restore's reply belongs.
            json!({ "address": LEADER, "path": CONSOLE_RESTORE_PATH,
                    "opened": 4, "connected": 5, "released": 7,
                    "frames": [frame(6, download_completed())] }),
        ],
        &[],
    );

    // The redirect names the leader's web console by an address with no port.
    let portless = Url::parse("console://127.0.0.1/").assured("a literal URL");
    let unaddressed = record(
        vec![json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                     "opened": 1, "connected": 2, "released": 4,
                     "frames": [frame(3, session_ended(LeaderRedirect {
                         leader: Some(leader_advertising(portless)),
                     }))] })],
        &[],
    );

    let Err(unreadable) = misread.reported_leaders() else {
        panic!("a download's frame read as a restore's reply");
    };
    let Err(no_console) = unaddressed.reported_leaders() else {
        panic!("a console without a port read as a leader's");
    };

    assert_eq!(
        unreadable.current_context(),
        &UnreadableFrame {
            connection: 2,
            purpose: ConnectionPurpose::Restore,
            received: 6,
        }
    );
    assert_eq!(
        no_console.current_context(),
        &UnreadableFrame {
            connection: 1,
            purpose: ConnectionPurpose::Session,
            received: 3,
        }
    );
    assert_eq!(
        no_console.downcast_ref::<FrameReadFailure>(),
        Some(&FrameReadFailure::NoConsole)
    );
}

#[test]
fn only_a_session_connection_opened_after_the_loss_where_it_was_lost_is_a_reconnect() {
    let lost = record(
        vec![json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                     "opened": 1, "connected": 2, "released": 4, "frames": [] })],
        &[3],
    );
    let constructed_not_open = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 4, "frames": [] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 5, "connected": null, "released": null, "frames": [] }),
        ],
        &[3],
    );
    let reconnected_elsewhere = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": 4, "frames": [] }),
            json!({ "address": FOLLOWER, "path": CONSOLE_SESSION_PATH,
                    "opened": 5, "connected": 6, "released": null, "frames": [] }),
        ],
        &[3],
    );
    let never_lost = record(
        vec![json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                     "opened": 1, "connected": 2, "released": null, "frames": [] })],
        &[],
    );
    // The only session connection was constructed after the loss, so the page lost none.
    let lost_before_any_session = record(
        vec![json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                     "opened": 5, "connected": 6, "released": null, "frames": [] })],
        &[3],
    );
    // A connection the page still held when it was lost, and a reconnect there, satisfy the check
    // even before the page saw the lost connection close.
    let reconnected_while_held = record(
        vec![
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 1, "connected": 2, "released": null, "frames": [] }),
            json!({ "address": LEADER, "path": CONSOLE_SESSION_PATH,
                    "opened": 4, "connected": 5, "released": null, "frames": [] }),
        ],
        &[3],
    );
    let leader = address(LEADER);

    assert_eq!(
        lost.session_reconnected_where_it_was_lost(),
        Err(ConnectionRecordViolation::NotReconnected {
            lost: leader.clone()
        })
    );
    assert_eq!(
        constructed_not_open.session_reconnected_where_it_was_lost(),
        Err(ConnectionRecordViolation::NotReconnected {
            lost: leader.clone()
        })
    );
    assert_eq!(
        reconnected_elsewhere.session_reconnected_where_it_was_lost(),
        Err(ConnectionRecordViolation::NotReconnected {
            lost: leader.clone()
        })
    );
    assert_eq!(
        never_lost.session_reconnected_where_it_was_lost(),
        Err(ConnectionRecordViolation::NoLoss)
    );
    assert_eq!(
        lost_before_any_session.session_reconnected_where_it_was_lost(),
        Err(ConnectionRecordViolation::NoSessionHeldAtLoss { loss: 3 })
    );
    assert_eq!(
        reconnected_while_held.session_reconnected_where_it_was_lost(),
        Ok(())
    );
}
