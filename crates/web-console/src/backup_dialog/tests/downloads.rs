//! The download driver against the in-memory browser: the archive it saves, the attempts it starts
//! again, the redirects it follows, and the reasons it ends with.

use std::cell::Cell;

use error_stack::Report;
use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    BackupArchiveStart, BackupDownloadFailed, BackupDownloadFailure, BackupDownloadFrame,
    BackupDownloadMessage, BackupDownloadRequest, EncodedFrame, LeaderEndpoints, LeaderRedirect,
    websocket::{ServerBackupDownloadWebSocketCodec, WebSocketData},
};
use nervix_models::ClusterNodeName;

use super::{ARCHIVE, reference, summary};
use crate::{
    SESSION_LIMITS,
    backup_dialog::{
        archive_download::{
            ArchiveDownload, DownloadError, FRAME_TIMEOUT, MAX_DOWNLOAD_ATTEMPTS, RETRY_DELAY,
        },
        browser::{
            Received,
            in_memory::{InMemoryBrowser, SavedArchive, Script},
        },
    },
};

const CONSOLE: &str = "http://127.0.0.1:47420";
const LEADER_CONSOLE: &str = "http://127.0.0.1:47421";

/// The binary message that carries `frame` from the server.
fn sent_by_server(frame: EncodedFrame<BackupDownloadFrame>) -> Received {
    let codec = ServerBackupDownloadWebSocketCodec::new(SESSION_LIMITS);
    Received::Binary(Vec::from(codec.encode(frame)))
}

fn start() -> Received {
    let summary = summary(ARCHIVE);
    let start = BackupArchiveStart {
        total_bytes: summary.total_bytes,
        digest: summary.digest,
    };
    sent_by_server(
        BackupDownloadMessage::encode_start(&start, &SESSION_LIMITS)
            .assured("a test start fits a frame"),
    )
}

fn chunk(bytes: &[u8]) -> Received {
    sent_by_server(
        BackupDownloadMessage::encode_chunk(bytes, &SESSION_LIMITS)
            .assured("a test chunk fits a frame"),
    )
}

fn complete() -> Received {
    sent_by_server(
        BackupDownloadMessage::encode_complete(&SESSION_LIMITS)
            .assured("a completion fits a frame"),
    )
}

fn refused(failure: BackupDownloadFailure) -> Received {
    let failed = BackupDownloadFailed {
        failure,
        message: "the archive is gone".to_string(),
    };
    sent_by_server(
        BackupDownloadMessage::encode_failed(&failed, &SESSION_LIMITS)
            .assured("a refusal fits a frame"),
    )
}

/// A redirect to the leader whose console is `leader_console`, or to no known leader.
fn redirected(leader_console: Option<&str>) -> Received {
    let leader = leader_console.map(|console| LeaderEndpoints {
        node: ClusterNodeName::parse("node-2").assured("the test node name is valid"),
        grpc_uri: None,
        web_console_uri: Some(url::Url::parse(console).assured("the test URL parses")),
    });
    let redirect = LeaderRedirect { leader };
    sent_by_server(
        BackupDownloadMessage::encode_redirect(&redirect, &SESSION_LIMITS)
            .assured("a redirect fits a frame"),
    )
}

/// The whole archive, in two chunks.
fn whole_archive() -> Script {
    Script::answering([
        start(),
        chunk(&ARCHIVE[..10]),
        chunk(&ARCHIVE[10..]),
        complete(),
    ])
}

fn download() -> ArchiveDownload {
    ArchiveDownload {
        reference: reference(),
        summary: summary(ARCHIVE),
        base_url: Some(CONSOLE.to_string()),
        auth_token: "token".to_string(),
        file_name: "tenant.nvxb".to_string(),
    }
}

/// How a download in the in-memory browser ended, and the last byte count it reported.
struct Ended {
    result: Result<(), Report<DownloadError>>,
    received: u64,
}

impl Ended {
    /// The reason the download failed.
    fn failure(&self) -> &DownloadError {
        let Err(failure) = &self.result else {
            panic!("the download was expected to fail");
        };
        failure.current_context()
    }
}

/// Runs `download` in `browser`, whose timers elapse at once.
fn run(download: ArchiveDownload, browser: &InMemoryBrowser) -> Ended {
    let received = Cell::new(0);
    let progress = |bytes: u64| received.set(bytes);
    let result = download
        .run(browser, progress)
        .now_or_never()
        .assured("an in-memory download never waits on the network");
    Ended {
        result,
        received: received.get(),
    }
}

fn saved_archive() -> SavedArchive {
    SavedArchive {
        file_name: "tenant.nvxb".to_string(),
        bytes: ARCHIVE.to_vec(),
    }
}

fn download_address(console: &str) -> String {
    let host = console.trim_start_matches("http://");
    format!("ws://{host}/console/backups/download?auth=token")
}

#[test]
fn a_download_saves_the_verified_archive_under_its_file_name() {
    let browser = InMemoryBrowser::with_scripts([whole_archive()]);
    let ended = run(download(), &browser);
    ended
        .result
        .assured("the whole archive matches its summary");
    let archive_bytes = u64::try_from(ARCHIVE.len()).assured("a test archive is short");
    assert_eq!(ended.received, archive_bytes);

    let observed = browser.observed();
    assert_eq!(observed.saved, Some(saved_archive()));
    assert_eq!(observed.opened, vec![download_address(CONSOLE)]);
    let [request] = observed.sent.as_slice() else {
        panic!(
            "a download sends exactly its request, found {:?}",
            observed.sent
        );
    };
    let codec = ServerBackupDownloadWebSocketCodec::new(SESSION_LIMITS);
    let frame = codec
        .decode(WebSocketData::Binary(request.clone().into()))
        .assured("the console sends one download request frame");
    let request = BackupDownloadRequest::decode(&frame).assured("the request decodes");
    assert_eq!(request.execution_reference, reference());
}

#[test]
fn a_download_starts_again_after_a_failure_and_follows_the_leader() {
    let interrupted = Script::answering([start(), chunk(&ARCHIVE[..10]), Received::Ended]);
    let stalled = Script::answering([start()]);
    let redirect = Script::answering([redirected(Some(LEADER_CONSOLE))]);
    let browser = InMemoryBrowser::with_scripts([interrupted, stalled, redirect, whole_archive()]);
    let ended = run(download(), &browser);
    ended.result.assured("the download completes at the leader");

    let observed = browser.observed();
    assert_eq!(observed.saved, Some(saved_archive()));
    assert_eq!(
        observed.opened,
        vec![
            download_address(CONSOLE),
            download_address(CONSOLE),
            download_address(CONSOLE),
            download_address(LEADER_CONSOLE),
        ]
    );
    assert_eq!(
        observed.waits,
        vec![RETRY_DELAY, FRAME_TIMEOUT, RETRY_DELAY],
        "the interrupted attempt waits, the silent server stalls the next one, which waits too, \
         and the redirect is followed at once"
    );
}

#[test]
fn a_download_the_server_refuses_or_that_does_not_match_its_summary_ends_at_once() {
    let browser = InMemoryBrowser::with_scripts([Script::answering([refused(
        BackupDownloadFailure::NotRetained,
    )])]);
    let ended = run(download(), &browser);
    let DownloadError::Refused { failure, .. } = ended.failure() else {
        panic!("a refusal ends the download: {:?}", ended.result);
    };
    assert_eq!(*failure, BackupDownloadFailure::NotRetained);
    assert!(!ended.failure().is_retryable());
    assert_eq!(browser.observed().opened.len(), 1);

    let altered = Script::answering([
        start(),
        chunk(&ARCHIVE[1..]),
        chunk(&ARCHIVE[..1]),
        complete(),
    ]);
    let browser = InMemoryBrowser::with_scripts([altered]);
    let ended = run(download(), &browser);
    assert_eq!(*ended.failure(), DownloadError::Mismatch);
    assert_eq!(browser.observed().saved, None);

    let browser = InMemoryBrowser::with_scripts([Script::answering([Received::Text])]);
    let ended = run(download(), &browser);
    assert_eq!(*ended.failure(), DownloadError::InvalidFrame);
}

#[test]
fn a_download_without_an_address_or_a_place_to_save_fails() {
    let browser = InMemoryBrowser::with_scripts([whole_archive()]);
    let ended = run(
        ArchiveDownload {
            base_url: None,
            ..download()
        },
        &browser,
    );
    assert_eq!(*ended.failure(), DownloadError::NoAddress);
    assert!(browser.observed().opened.is_empty());

    let browser = InMemoryBrowser::with_scripts([whole_archive()]).refusing_saves();
    let ended = run(download(), &browser);
    assert_eq!(*ended.failure(), DownloadError::Save);
}

#[test]
fn a_download_gives_up_on_redirects_that_never_reach_an_archive() {
    let redirects =
        (0..MAX_DOWNLOAD_ATTEMPTS).map(|_| Script::answering([redirected(Some(LEADER_CONSOLE))]));
    let browser = InMemoryBrowser::with_scripts(redirects);
    let ended = run(download(), &browser);
    assert_eq!(*ended.failure(), DownloadError::RedirectLoop);

    let no_leader = (0..MAX_DOWNLOAD_ATTEMPTS).map(|_| Script::answering([redirected(None)]));
    let browser = InMemoryBrowser::with_scripts(no_leader);
    let ended = run(download(), &browser);
    assert_eq!(*ended.failure(), DownloadError::NoLeader);
    assert_eq!(
        browser.observed().waits.len(),
        MAX_DOWNLOAD_ATTEMPTS,
        "each redirect without a leader waits for an election"
    );

    // A browser that refuses every connection fails each attempt in transport.
    let browser = InMemoryBrowser::default();
    let ended = run(download(), &browser);
    assert_eq!(*ended.failure(), DownloadError::Transport);
    assert_eq!(browser.observed().opened.len(), MAX_DOWNLOAD_ATTEMPTS);
}
