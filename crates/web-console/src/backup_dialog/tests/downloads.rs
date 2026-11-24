//! The download driver against the in-memory browser: the archive it saves, the attempts it starts
//! again, the redirects it follows, and the reasons it ends with.

use std::{cell::Cell, time::Duration};

use error_stack::Report;
use futures_util::FutureExt as _;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    BackupArchiveStart, BackupDownloadFailed, BackupDownloadFailure, BackupDownloadFrame,
    BackupDownloadMessage, BackupDownloadRequest, EncodedFrame, LeaderRedirect, NodeEndpoints,
    SessionPacing,
    websocket::{ServerBackupDownloadWebSocketCodec, WebSocketData},
};
use nervix_models::{ClusterNodeName, Setting, WebConsoleCapabilities, WebConsoleCapabilityValues};

use super::{ARCHIVE, reference, summary, summary_retained_for};
use crate::{
    SESSION_LIMITS,
    backup_dialog::{
        archive_download::{ArchiveDownload, DownloadError},
        browser::{
            Received,
            in_memory::{InMemoryBrowser, SavedArchive, Script},
        },
    },
    pacing::ConsolePacing,
};

/// How long the archives of the tests that never reach one stay retained: five pauses.
const SHORT_RETENTION: Duration = Duration::from_secs(5);

/// The pause before a download starts again, at the declared restoration backoff.
fn retry_pause() -> Duration {
    ConsolePacing::default().web_console().restoration_backoff()
}

/// The declared request timeout, within which each frame must arrive.
fn request_timeout() -> Duration {
    ConsolePacing::default().web_console().request_timeout()
}

const CONSOLE: &str = "http://127.0.0.1:47420";
const LEADER_CONSOLE: &str = "http://127.0.0.1:47421";
const HOLDER_CONSOLE: &str = "http://127.0.0.1:47422";

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
    let leader = leader_console.map(|console| NodeEndpoints {
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

/// The answer that sends a download to the node retaining the archive, whose console is
/// `holder_console`, or which advertises no console.
fn sent_to_holder(holder_console: Option<&str>) -> Received {
    let holder = NodeEndpoints {
        node: ClusterNodeName::parse("node-3").assured("the test node name is valid"),
        grpc_uri: None,
        web_console_uri: holder_console
            .map(|console| url::Url::parse(console).assured("the test URL parses")),
    };
    sent_by_server(
        BackupDownloadMessage::encode_holder(&holder, &SESSION_LIMITS)
            .assured("a holder answer fits a frame"),
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
        pacing: ConsolePacing::default(),
    }
}

/// A download whose archive the server retains for [`SHORT_RETENTION`] only.
fn briefly_retained_download() -> ArchiveDownload {
    ArchiveDownload {
        summary: summary_retained_for(ARCHIVE, SHORT_RETENTION),
        ..download()
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
        vec![retry_pause(), request_timeout(), retry_pause()],
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
fn a_download_gives_up_on_redirects_once_its_archive_is_retained_no_longer() {
    // The first redirect is followed at once; every later one names the leader the download
    // already reached, so each pauses first, until the five seconds of retention ended.
    let redirects = (0..20).map(|_| Script::answering([redirected(Some(LEADER_CONSOLE))]));
    let browser = InMemoryBrowser::with_scripts(redirects);
    let ended = run(briefly_retained_download(), &browser);
    assert_eq!(*ended.failure(), DownloadError::RedirectLoop);
    let observed = browser.observed();
    assert_eq!(observed.opened.len(), 7);
    assert_eq!(observed.waits, vec![retry_pause(); 5]);
    drop(observed);

    let no_leader = (0..20).map(|_| Script::answering([redirected(None)]));
    let browser = InMemoryBrowser::with_scripts(no_leader);
    let ended = run(briefly_retained_download(), &browser);
    assert_eq!(*ended.failure(), DownloadError::NoLeader);
    assert_eq!(
        browser.observed().waits,
        vec![retry_pause(); 5],
        "each redirect without a leader waits for an election"
    );

    // A browser that refuses every connection fails each attempt in transport.
    let browser = InMemoryBrowser::default();
    let ended = run(briefly_retained_download(), &browser);
    assert_eq!(*ended.failure(), DownloadError::Transport);
    assert_eq!(browser.observed().opened.len(), 6);
}

#[test]
fn a_download_that_is_sent_back_to_a_console_it_reached_pauses_before_it_follows() {
    let to_leader = Script::answering([redirected(Some(LEADER_CONSOLE))]);
    let back = Script::answering([redirected(Some(CONSOLE))]);
    let browser = InMemoryBrowser::with_scripts([to_leader, back, whole_archive()]);
    let ended = run(download(), &browser);
    ended
        .result
        .assured("the download completes once the views agree");
    let observed = browser.observed();
    assert_eq!(
        observed.opened,
        vec![
            download_address(CONSOLE),
            download_address(LEADER_CONSOLE),
            download_address(CONSOLE),
        ]
    );
    assert_eq!(
        observed.waits,
        vec![retry_pause()],
        "the leader is followed at once, and the redirect back pauses first"
    );
}

#[test]
fn a_download_follows_the_pacing_its_node_announced() {
    let web_console = WebConsoleCapabilities::resolve(WebConsoleCapabilityValues {
        request_timeout: Some(Duration::from_secs(30)),
        restoration_backoff: Some(Duration::from_secs(2)),
        ..WebConsoleCapabilityValues::default()
    })
    .assured("the test waits are valid console waits");
    let pacing = SessionPacing::new(
        Setting::resolve(None, &()).assured("the default snapshot interval is valid"),
        None,
        web_console,
    );
    let interrupted = Script::answering([start(), chunk(&ARCHIVE[..10]), Received::Ended]);
    let stalled = Script::answering([start()]);
    let browser =
        InMemoryBrowser::with_scripts([interrupted, stalled, whole_archive()]).announcing(pacing);
    let ended = run(download(), &browser);
    ended.result.assured("the download completes");
    assert_eq!(
        browser.observed().waits,
        vec![
            Duration::from_secs(2),
            Duration::from_secs(30),
            Duration::from_secs(2)
        ],
        "every pause and frame wait follows the node the attempt's opening reached"
    );
}

#[test]
fn a_download_goes_on_at_the_console_of_the_node_that_retains_the_archive() {
    let holder = Script::answering([sent_to_holder(Some(HOLDER_CONSOLE))]);
    let browser = InMemoryBrowser::with_scripts([holder, whole_archive()]);
    let ended = run(download(), &browser);
    ended.result.assured("the download completes at the holder");

    let observed = browser.observed();
    assert_eq!(observed.saved, Some(saved_archive()));
    assert_eq!(
        observed.opened,
        vec![download_address(CONSOLE), download_address(HOLDER_CONSOLE)]
    );
    assert!(
        observed.waits.is_empty(),
        "the holder answer is followed at once"
    );
}

#[test]
fn a_download_that_fails_at_the_holder_asks_the_reporting_console_again() {
    let holder = Script::answering([sent_to_holder(Some(HOLDER_CONSOLE))]);
    let interrupted = Script::answering([start(), chunk(&ARCHIVE[..10]), Received::Ended]);
    let holder_again = Script::answering([sent_to_holder(Some(HOLDER_CONSOLE))]);
    let browser =
        InMemoryBrowser::with_scripts([holder, interrupted, holder_again, whole_archive()]);
    let ended = run(download(), &browser);
    ended.result.assured("the download completes at the holder");

    let observed = browser.observed();
    assert_eq!(
        observed.opened,
        vec![
            download_address(CONSOLE),
            download_address(HOLDER_CONSOLE),
            download_address(CONSOLE),
            download_address(HOLDER_CONSOLE),
        ],
        "the console the download started from names the holder again"
    );
    assert_eq!(observed.waits, vec![retry_pause()]);
}

#[test]
fn a_holder_without_a_console_is_asked_for_again_until_the_archive_is_retained_no_longer() {
    let holders = (0..20).map(|_| Script::answering([sent_to_holder(None)]));
    let browser = InMemoryBrowser::with_scripts(holders);
    let ended = run(briefly_retained_download(), &browser);
    assert_eq!(*ended.failure(), DownloadError::HolderUnreachable);
    let observed = browser.observed();
    assert_eq!(observed.waits, vec![retry_pause(); 5]);
    assert!(
        observed
            .opened
            .iter()
            .all(|address| address == &download_address(CONSOLE)),
        "a holder without a console leaves the download where it started"
    );
}
