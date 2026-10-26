//! The restore driver against the in-memory browser: how it measures an archive file, streams it
//! in chunks within the connection's room, stops when the leader answers early, follows the
//! leader, streams again while the outcome is unknown, and reports what it could not learn.

use std::cell::Cell;

use error_stack::Report;
use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    CommandDisposition, LeaderEndpoints, LeaderRedirect, RestoreDisposition, RestoreMessage,
    RestoreReply, RestoreUploadFailure, UnknownOutcomeCause,
    websocket::{ServerRestoreWebSocketCodec, WebSocketData},
};
use nervix_models::{ArchiveDigest, ClusterNodeName, RestoreArchive};

use super::{outcome, reference};
use crate::{
    SESSION_LIMITS,
    backup_dialog::{
        browser::{
            Received,
            in_memory::{InMemoryBrowser, MemoryFile, Reply, Script},
        },
        restore_stream::{
            DRAIN_POLL, MAX_BUFFERED_BYTES, MAX_DRAIN_POLLS, MAX_RESTORE_ATTEMPTS,
            MEASURE_SLICE_BYTES, RESTORE_CHUNK_BYTES, RETRY_DELAY, RestoreEnd, RestoreStreamError,
            RestoreUpload, measure_archive,
        },
    },
};

const CONSOLE: &str = "http://127.0.0.1:47420";
const LEADER_CONSOLE: &str = "http://127.0.0.1:47421";
const STATEMENT: &str = "RESTORE DOMAIN tenant AS tenant_copy FROM 'tenant.nvxb' DRY RUN;";

/// An archive that streams as three chunks, the last one short.
const ARCHIVE_LENGTH: usize = 2 * RESTORE_CHUNK_BYTES + 1000;

/// A reading of queued bytes above the most a restore lets the browser hold.
const OVER_THE_CAP: u32 = MAX_BUFFERED_BYTES + 1;

/// The bytes of an archive file of `ARCHIVE_LENGTH` bytes.
fn archive_bytes() -> Vec<u8> {
    (0..ARCHIVE_LENGTH)
        .map(|index| u8::try_from(index % 251).assured("a remainder of 251 fits a byte"))
        .collect()
}

fn archive_of(bytes: &[u8]) -> RestoreArchive {
    let length = u64::try_from(bytes.len()).assured("a test archive's length fits u64");
    RestoreArchive {
        total_bytes: std::num::NonZeroU64::new(length).assured("a test archive has bytes"),
        digest: ArchiveDigest::from_bytes(*blake3::hash(bytes).as_bytes()),
    }
}

fn upload(bytes: Vec<u8>) -> RestoreUpload<MemoryFile> {
    RestoreUpload {
        reference: reference(),
        statement: STATEMENT.to_string(),
        archive: archive_of(&bytes),
        file: MemoryFile {
            bytes,
            unreadable: false,
        },
        base_url: Some(CONSOLE.to_string()),
        auth_token: "token".to_string(),
    }
}

/// The binary message that carries `disposition` as the server's reply.
fn reply(disposition: RestoreDisposition) -> Received {
    let reply = RestoreReply {
        request_id: None,
        disposition,
    };
    let frame = reply
        .encode(&SESSION_LIMITS)
        .assured("a test reply fits a frame");
    let codec = ServerRestoreWebSocketCodec::new(SESSION_LIMITS);
    Received::Binary(Vec::from(codec.encode(frame)))
}

fn answered(disposition: CommandDisposition) -> Received {
    reply(RestoreDisposition::Outcome(Box::new(outcome(disposition))))
}

fn completed() -> Received {
    answered(CommandDisposition::Completed {
        already_existed: false,
    })
}

/// A connection whose server answers with `received` once `after_sent` messages arrived.
fn answering_after(after_sent: usize, received: Received) -> Script {
    Script {
        replies: [Reply {
            after_sent,
            received,
        }]
        .into(),
        ..Script::default()
    }
}

/// A connection whose server answers with `received` once the start and every chunk arrived.
fn answering_at_the_end(received: Received) -> Script {
    answering_after(4, received)
}

/// How a restore in the in-memory browser ended, and the last progress it reported.
struct Ended {
    result: Result<RestoreEnd, Report<RestoreStreamError>>,
    sent: u64,
}

impl Ended {
    fn failure(&self) -> &RestoreStreamError {
        let Err(failure) = &self.result else {
            panic!("the restore was expected to fail: {:?}", self.result);
        };
        failure.current_context()
    }
}

/// Runs `upload` in `browser`, whose timers elapse at once.
fn run(upload: RestoreUpload<MemoryFile>, browser: &InMemoryBrowser) -> Ended {
    let sent = Cell::new(0);
    let progress = |done: u64, _total: u64| sent.set(done);
    let result = upload
        .run(browser, progress)
        .now_or_never()
        .assured("an in-memory restore never waits on the network");
    Ended {
        result,
        sent: sent.get(),
    }
}

/// The restore frames the server received, in order.
fn received_by_server(sent: &[Vec<u8>]) -> Vec<RestoreMessage> {
    let codec = ServerRestoreWebSocketCodec::new(SESSION_LIMITS);
    let mut messages = Vec::with_capacity(sent.len());
    for payload in sent {
        let frame = codec
            .decode(WebSocketData::Binary(payload.clone().into()))
            .assured("the console sends restore frames");
        messages.push(RestoreMessage::decode(&frame).assured("each restore frame decodes"));
    }
    messages
}

fn redirect_to(console: Option<&str>) -> LeaderRedirect {
    let leader = console.map(|console| LeaderEndpoints {
        node: ClusterNodeName::parse("node-2").assured("the test node name is valid"),
        grpc_uri: None,
        web_console_uri: Some(url::Url::parse(console).assured("the test URL parses")),
    });
    LeaderRedirect { leader }
}

#[test]
fn an_archive_file_is_measured_and_digested_slice_by_slice() {
    let slice = usize::try_from(MEASURE_SLICE_BYTES).assured("a 4 MiB slice fits usize");
    let length = slice
        .checked_add(5)
        .assured("a slice and five bytes fit usize");
    let bytes = vec![7_u8; length];
    let file = MemoryFile {
        bytes: bytes.clone(),
        unreadable: false,
    };
    let reads = std::cell::RefCell::new(Vec::new());
    let progress = |done: u64, total: u64| reads.borrow_mut().push((done, total));
    let measured = measure_archive(&file, progress)
        .now_or_never()
        .assured("an in-memory file reads at once")
        .assured("a readable file measures");
    assert_eq!(measured, archive_of(&bytes));
    let total = u64::try_from(length).assured("the test length fits u64");
    assert_eq!(
        reads.into_inner(),
        vec![(MEASURE_SLICE_BYTES, total), (total, total)]
    );

    let empty = MemoryFile {
        bytes: Vec::new(),
        unreadable: false,
    };
    let refused = measure_archive(&empty, |_, _| {})
        .now_or_never()
        .assured("an empty file is refused at once");
    let Err(error) = refused else {
        panic!("an empty archive file is refused");
    };
    assert_eq!(*error.current_context(), RestoreStreamError::EmptyArchive);

    let unreadable = MemoryFile {
        bytes: vec![1, 2, 3],
        unreadable: true,
    };
    let refused = measure_archive(&unreadable, |_, _| {})
        .now_or_never()
        .assured("an unreadable file is refused at once");
    let Err(error) = refused else {
        panic!("an unreadable archive file is refused");
    };
    assert_eq!(*error.current_context(), RestoreStreamError::ReadArchive);
}

#[test]
fn a_restore_streams_its_start_and_chunks_and_returns_the_reply() {
    let bytes = archive_bytes();
    let browser = InMemoryBrowser::with_scripts([answering_at_the_end(completed())]);
    let ended = run(upload(bytes.clone()), &browser);
    let Ok(RestoreEnd::Outcome(outcome)) = &ended.result else {
        panic!("the reply ends the restore: {:?}", ended.result);
    };
    assert_eq!(outcome.execution_reference, reference());
    let total = u64::try_from(bytes.len()).assured("the test length fits u64");
    assert_eq!(ended.sent, total);

    let observed = browser.observed();
    assert_eq!(
        observed.opened,
        vec!["ws://127.0.0.1:47420/console/backups/restore?auth=token".to_string()]
    );
    let messages = received_by_server(&observed.sent);
    let [RestoreMessage::Start(start), chunks @ ..] = messages.as_slice() else {
        panic!("a restore begins with its start");
    };
    assert_eq!(start.statement, STATEMENT);
    assert_eq!(start.execution_reference, reference());
    assert_eq!(start.archive, archive_of(&bytes));
    let mut streamed = Vec::with_capacity(bytes.len());
    for chunk in chunks {
        let RestoreMessage::Chunk(chunk) = chunk else {
            panic!("only chunks follow the start");
        };
        streamed.extend_from_slice(chunk.bytes());
    }
    assert_eq!(chunks.len(), 3);
    assert_eq!(streamed, bytes);
}

#[test]
fn a_restore_the_leader_answers_early_sends_nothing_more() {
    let refused = reply(RestoreDisposition::UploadFailed {
        failure: RestoreUploadFailure::DigestMismatch,
        message: "the archive's digest is not the declared one".to_string(),
    });
    let browser = InMemoryBrowser::with_scripts([answering_after(1, refused)]);
    let ended = run(upload(archive_bytes()), &browser);
    let Ok(RestoreEnd::Refused { failure, .. }) = &ended.result else {
        panic!("an early refusal ends the restore: {:?}", ended.result);
    };
    assert_eq!(*failure, RestoreUploadFailure::DigestMismatch);
    assert_eq!(
        browser.observed().sent.len(),
        1,
        "only the start was sent before the answer"
    );
}

#[test]
fn a_restore_waits_for_its_connection_to_drain_and_stalls_when_it_never_does() {
    let mut draining = answering_at_the_end(completed());
    draining.buffered = [OVER_THE_CAP, OVER_THE_CAP].into();
    let browser = InMemoryBrowser::with_scripts([draining]);
    let ended = run(upload(archive_bytes()), &browser);
    assert!(matches!(ended.result, Ok(RestoreEnd::Outcome(_))));
    assert_eq!(browser.observed().waits, vec![DRAIN_POLL, DRAIN_POLL]);

    // One more reading than the polls a stalled connection is given.
    let polls = usize::try_from(MAX_DRAIN_POLLS).assured("the drain polls fit usize");
    let readings = polls
        .checked_add(1)
        .assured("one more reading than the polls fits usize");
    let mut full = answering_at_the_end(completed());
    full.buffered = std::iter::repeat_n(OVER_THE_CAP, readings).collect();
    let browser = InMemoryBrowser::with_scripts([full, answering_at_the_end(completed())]);
    let ended = run(upload(archive_bytes()), &browser);
    assert!(
        matches!(ended.result, Ok(RestoreEnd::Outcome(_))),
        "a stalled connection is streamed again: {:?}",
        ended.result
    );
    let observed = browser.observed();
    assert_eq!(observed.opened.len(), 2);
    assert_eq!(observed.waits.last(), Some(&RETRY_DELAY));
}

#[test]
fn a_restore_follows_the_leader_and_streams_again_while_its_outcome_is_unknown() {
    let redirected = answering_at_the_end(answered(CommandDisposition::NotLeader(redirect_to(
        Some(LEADER_CONSOLE),
    ))));
    let no_leader =
        answering_at_the_end(answered(CommandDisposition::NotLeader(redirect_to(None))));
    let unknown = answering_at_the_end(answered(CommandDisposition::OutcomeUnknown(
        UnknownOutcomeCause::StillApplying,
    )));
    let browser = InMemoryBrowser::with_scripts([
        redirected,
        no_leader,
        unknown,
        answering_at_the_end(completed()),
    ]);
    let ended = run(upload(archive_bytes()), &browser);
    assert!(matches!(ended.result, Ok(RestoreEnd::Outcome(_))));
    let observed = browser.observed();
    let leader = "ws://127.0.0.1:47421/console/backups/restore?auth=token".to_string();
    assert_eq!(
        observed.opened,
        vec![
            "ws://127.0.0.1:47420/console/backups/restore?auth=token".to_string(),
            leader.clone(),
            leader.clone(),
            leader,
        ]
    );
    assert_eq!(observed.waits, vec![RETRY_DELAY, RETRY_DELAY]);
}

#[test]
fn a_restore_that_cannot_learn_its_outcome_says_so() {
    let browser = InMemoryBrowser::default();
    let ended = run(upload(archive_bytes()), &browser);
    assert_eq!(*ended.failure(), RestoreStreamError::Transport);
    assert!(ended.failure().leaves_outcome_unknown());
    assert_eq!(browser.observed().opened.len(), MAX_RESTORE_ATTEMPTS);

    let silent = Script::default();
    let browser = InMemoryBrowser::with_scripts([silent]);
    let ended = run(upload(archive_bytes()), &browser);
    assert_eq!(*ended.failure(), RestoreStreamError::Transport);

    let refused_send = Script {
        accepted: Some(1),
        ..Script::default()
    };
    let browser = InMemoryBrowser::with_scripts([refused_send]);
    let ended = run(upload(archive_bytes()), &browser);
    assert_eq!(*ended.failure(), RestoreStreamError::Transport);
    assert_eq!(browser.observed().sent.len(), 1, "the start was sent");
}

#[test]
fn a_restore_ends_at_once_when_its_file_or_reply_cannot_be_read() {
    let unreadable = RestoreUpload {
        file: MemoryFile {
            bytes: archive_bytes(),
            unreadable: true,
        },
        ..upload(archive_bytes())
    };
    let browser = InMemoryBrowser::with_scripts([answering_at_the_end(completed())]);
    let ended = run(unreadable, &browser);
    assert_eq!(*ended.failure(), RestoreStreamError::ReadArchive);
    assert!(!ended.failure().leaves_outcome_unknown());

    let browser = InMemoryBrowser::with_scripts([answering_at_the_end(Received::Text)]);
    let ended = run(upload(archive_bytes()), &browser);
    assert_eq!(*ended.failure(), RestoreStreamError::InvalidReply);

    let browser = InMemoryBrowser::with_scripts([answering_at_the_end(completed())]);
    let ended = run(
        RestoreUpload {
            base_url: None,
            ..upload(archive_bytes())
        },
        &browser,
    );
    assert_eq!(*ended.failure(), RestoreStreamError::NoAddress);
}
