//! Downloading a completed backup's archive in the browser.
//!
//! A download is the `DownloadBackup` call carried over a console WebSocket of its own: one
//! request names the backup's execution reference, and the answer is one refusal or redirect, or
//! the archive's start, its chunks in order and a completion. Every chunk is checked against the
//! summary the backup reported as it arrives, and the archive reaches the browser as a download
//! only once its exact size and BLAKE3 digest match, so a partial or foreign archive is never
//! saved under the name the operator asked for.
//!
//! A download that fails in transport, stalls or ends early starts again from the first byte, for
//! as long as the server retains the archive. Each frame must arrive within its own timeout; the
//! download as a whole is not bounded, because an archive may be far larger than any reply. A
//! frame that arrives as its timeout ends still counts.

use std::{num::NonZeroU64, pin::pin, time::Duration};

use error_stack::Report;
use futures_util::future::{Either, select};
use meticulous::ResultExt as _;
use nervix_client_wire::{
    BackupArchiveStart, BackupDownloadFailure, BackupDownloadMessage, BackupDownloadRequest,
    LeaderRedirect,
    websocket::{CONSOLE_BACKUP_DOWNLOAD_PATH, ClientBackupDownloadWebSocketCodec, WebSocketData},
};
use nervix_models::{ArchiveDigest, BackupArchiveSummary, CommandExecutionReference};
use thiserror::Error;

use super::browser::{Browser, CallSocket as _, DownloadedArchive, Received};
use crate::{SESSION_LIMITS, web_console_websocket_url_from_base};

/// How long a download waits for its next frame before it counts as stalled.
pub(crate) const FRAME_TIMEOUT: Duration = Duration::from_secs(120);

/// How many times one download starts again or follows a redirect before it gives up.
pub(crate) const MAX_DOWNLOAD_ATTEMPTS: usize = 8;

/// How long a download waits before it starts again from the first byte.
pub(crate) const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Why a backup's archive was not downloaded. The backup itself completed either way.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum DownloadError {
    #[error("the server refused the download ({failure:?}): {message}")]
    Refused {
        failure: BackupDownloadFailure,
        message: String,
    },
    #[error("the download's connection failed")]
    Transport,
    #[error("no frame of the download arrived within {} seconds", FRAME_TIMEOUT.as_secs())]
    Stalled,
    #[error("the download ended before its archive completed")]
    Interrupted,
    #[error("the node does not retain the archive, and no leader console is known to ask instead")]
    NoLeader,
    #[error("the download was sent elsewhere more times than the console follows")]
    RedirectLoop,
    #[error("the server sent download frames out of order")]
    OutOfOrder,
    #[error("a download frame does not decode")]
    InvalidFrame,
    #[error("the archive's size or BLAKE3 digest is not the one the backup reported")]
    Mismatch,
    #[error("the console has no address to download the archive from")]
    NoAddress,
    #[error("the browser could not save the archive")]
    Save,
}

impl DownloadError {
    /// Whether downloading the archive again from its first byte may succeed: the server keeps
    /// an archive it could not read, and a transport failure says nothing about the archive.
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Refused { failure, .. } => *failure == BackupDownloadFailure::ReadFailed,
            Self::Transport | Self::Stalled | Self::Interrupted => true,
            Self::NoLeader
            | Self::RedirectLoop
            | Self::OutOfOrder
            | Self::InvalidFrame
            | Self::Mismatch
            | Self::NoAddress
            | Self::Save => false,
        }
    }
}

/// What a download received of the archive its backup reported, checked as it arrives.
pub(crate) struct ArchiveReceipt {
    total_bytes: NonZeroU64,
    digest: ArchiveDigest,
    received: u64,
    hasher: blake3::Hasher,
}

impl ArchiveReceipt {
    pub(crate) fn new(summary: &BackupArchiveSummary) -> Self {
        Self {
            total_bytes: summary.total_bytes,
            digest: summary.digest,
            received: 0,
            hasher: blake3::Hasher::new(),
        }
    }

    /// The bytes received so far.
    pub(crate) fn received(&self) -> u64 {
        self.received
    }

    /// Whether the archive a download starts is the one the backup reported.
    fn starts(&self, start: &BackupArchiveStart) -> bool {
        start.total_bytes == self.total_bytes && start.digest == self.digest
    }

    /// Takes one chunk, refusing an archive that grows past the size its backup reported.
    pub(crate) fn append(&mut self, bytes: &[u8]) -> Result<(), Report<DownloadError>> {
        let length = u64::try_from(bytes.len())
            .assured("a chunk held in memory is shorter than u64 counts on every target");
        let Some(received) = self.received.checked_add(length) else {
            return Err(Report::new(DownloadError::Mismatch));
        };
        if received > self.total_bytes.get() {
            return Err(Report::new(DownloadError::Mismatch));
        }
        self.hasher.update(bytes);
        self.received = received;
        Ok(())
    }

    /// Accepts the archive once every byte arrived with the reported digest.
    pub(crate) fn accept(&self) -> Result<(), Report<DownloadError>> {
        if self.received != self.total_bytes.get() {
            return Err(Report::new(DownloadError::Mismatch));
        }
        if ArchiveDigest::from_bytes(*self.hasher.finalize().as_bytes()) != self.digest {
            return Err(Report::new(DownloadError::Mismatch));
        }
        Ok(())
    }
}

/// Where one download attempt stands in the order its frames must follow.
pub(crate) enum AttemptStage {
    /// The start has not arrived.
    AwaitingStart,
    /// The start arrived, and the archive's chunks follow until its completion.
    Receiving,
}

/// What one frame did to a download attempt.
#[derive(Debug, PartialEq)]
pub(crate) enum FrameStep {
    /// The next frame follows.
    Continue,
    /// Every byte arrived and matches the summary.
    Complete,
    /// The node does not retain the archive and is not the leader.
    Redirect(LeaderRedirect),
}

impl AttemptStage {
    /// Takes the next frame of an attempt, whose chunks `receipt` checks.
    pub(crate) fn accept(
        &mut self,
        message: &BackupDownloadMessage,
        receipt: &mut ArchiveReceipt,
    ) -> Result<FrameStep, Report<DownloadError>> {
        match (&self, message) {
            (Self::AwaitingStart, BackupDownloadMessage::Start(start)) => {
                if !receipt.starts(start) {
                    return Err(Report::new(DownloadError::Mismatch));
                }
                *self = Self::Receiving;
                Ok(FrameStep::Continue)
            }
            (Self::AwaitingStart, BackupDownloadMessage::NotLeader(redirect)) => {
                Ok(FrameStep::Redirect(redirect.clone()))
            }
            (Self::AwaitingStart | Self::Receiving, BackupDownloadMessage::Failed(failed)) => {
                Err(Report::new(DownloadError::Refused {
                    failure: failed.failure,
                    message: failed.message.clone(),
                }))
            }
            (Self::Receiving, BackupDownloadMessage::Chunk(chunk)) => {
                receipt.append(chunk.bytes())?;
                Ok(FrameStep::Continue)
            }
            (Self::Receiving, BackupDownloadMessage::Complete) => {
                receipt.accept()?;
                Ok(FrameStep::Complete)
            }
            (
                Self::AwaitingStart,
                BackupDownloadMessage::Chunk(_) | BackupDownloadMessage::Complete,
            )
            | (
                Self::Receiving,
                BackupDownloadMessage::Start(_) | BackupDownloadMessage::NotLeader(_),
            ) => Err(Report::new(DownloadError::OutOfOrder)),
        }
    }
}

/// The archive one completed backup assembled, and where the console downloads it from.
pub(crate) struct ArchiveDownload {
    pub(crate) reference: CommandExecutionReference,
    pub(crate) summary: BackupArchiveSummary,
    /// The console of the node whose session reported the backup, which retains its archive.
    pub(crate) base_url: Option<String>,
    pub(crate) auth_token: String,
    /// The name the browser saves the archive under.
    pub(crate) file_name: String,
}

/// How one attempt ended, when it did not fail.
enum AttemptEnd<A> {
    /// Every byte arrived and matched; the browser keeps the archive.
    Complete(A),
    /// The download goes on at the leader's console.
    Redirect(LeaderRedirect),
}

impl ArchiveDownload {
    /// Downloads the archive in `browser` and saves it there, starting again from the first byte
    /// after every failure a retry may overcome. `progress` is told every byte count received.
    pub(crate) async fn run<B: Browser>(
        mut self,
        browser: &B,
        progress: impl Fn(u64) + Copy,
    ) -> Result<(), Report<DownloadError>> {
        let mut last_failure = Report::new(DownloadError::RedirectLoop);
        for _ in 0..MAX_DOWNLOAD_ATTEMPTS {
            progress(0);
            let ended = self.attempt(browser, progress).await;
            match ended {
                Ok(AttemptEnd::Complete(archive)) => return self.save(archive),
                Ok(AttemptEnd::Redirect(redirect)) => match redirect_console(&redirect) {
                    Some(leader) => {
                        self.base_url = Some(leader);
                        last_failure = Report::new(DownloadError::RedirectLoop);
                    }
                    // No leader is known yet: wait for an election, then ask again.
                    None => {
                        last_failure = Report::new(DownloadError::NoLeader);
                        browser.wait(RETRY_DELAY).await;
                    }
                },
                Err(failure) => {
                    if !failure.current_context().is_retryable() {
                        return Err(failure);
                    }
                    last_failure = failure;
                    browser.wait(RETRY_DELAY).await;
                }
            }
        }
        Err(last_failure)
    }

    /// Downloads the archive once, from the console the download is addressed to.
    async fn attempt<B: Browser>(
        &self,
        browser: &B,
        progress: impl Fn(u64),
    ) -> Result<AttemptEnd<B::Archive>, Report<DownloadError>> {
        let Some(url) = self.socket_url() else {
            return Err(Report::new(DownloadError::NoAddress));
        };
        let codec = ClientBackupDownloadWebSocketCodec::new(SESSION_LIMITS);
        let request = BackupDownloadRequest {
            execution_reference: self.reference.clone(),
        }
        .encode(&SESSION_LIMITS)
        .assured("an execution reference is far shorter than a frame");
        let Some(mut socket) = browser.open(&url) else {
            return Err(Report::new(DownloadError::Transport));
        };
        if socket.send(Vec::from(codec.encode(request))).await.is_err() {
            return Err(Report::new(DownloadError::Transport));
        }
        let mut stage = AttemptStage::AwaitingStart;
        let mut receipt = ArchiveReceipt::new(&self.summary);
        let mut archive = browser.new_archive();
        loop {
            let message = next_frame(browser, &mut socket, &codec).await?;
            match stage.accept(&message, &mut receipt)? {
                FrameStep::Continue => {}
                FrameStep::Complete => return Ok(AttemptEnd::Complete(archive)),
                FrameStep::Redirect(redirect) => return Ok(AttemptEnd::Redirect(redirect)),
            }
            let BackupDownloadMessage::Chunk(chunk) = &message else {
                continue;
            };
            // Each checked chunk leaves the page at once, so the archive never accumulates as one
            // buffer here.
            if archive.keep(chunk.bytes()).is_err() {
                return Err(Report::new(DownloadError::Save));
            }
            progress(receipt.received());
        }
    }

    fn socket_url(&self) -> Option<String> {
        let base = self.base_url.as_deref()?;
        let base = url::Url::parse(base).ok()?;
        web_console_websocket_url_from_base(&base, CONSOLE_BACKUP_DOWNLOAD_PATH, &self.auth_token)
    }

    /// Hands the verified archive to the browser as a download named after the backup's file.
    fn save<A: DownloadedArchive>(&self, archive: A) -> Result<(), Report<DownloadError>> {
        match archive.save(&self.file_name) {
            Ok(()) => Ok(()),
            Err(failure) => Err(Report::new(failure).change_context(DownloadError::Save)),
        }
    }
}

/// The next frame of a download, which must arrive within the frame timeout.
async fn next_frame<B: Browser>(
    browser: &B,
    socket: &mut B::Socket,
    codec: &ClientBackupDownloadWebSocketCodec,
) -> Result<BackupDownloadMessage, Report<DownloadError>> {
    let received = pin!(socket.receive());
    let timeout = pin!(browser.wait(FRAME_TIMEOUT));
    let received = match select(received, timeout).await {
        Either::Left((received, _)) => received,
        Either::Right(((), _)) => return Err(Report::new(DownloadError::Stalled)),
    };
    let data = match received {
        Received::Binary(payload) => WebSocketData::Binary(payload.into()),
        Received::Text => WebSocketData::Text,
        // A close before the completion, or a failed connection, ends the attempt early.
        Received::Ended => return Err(Report::new(DownloadError::Interrupted)),
    };
    let frame = match codec.decode(data) {
        Ok(frame) => frame,
        Err(error) => return Err(error.change_context(DownloadError::InvalidFrame)),
    };
    match BackupDownloadMessage::decode(&frame) {
        Ok(message) => Ok(message),
        Err(error) => Err(error.change_context(DownloadError::InvalidFrame)),
    }
}

/// The console of the leader a redirect names, when it advertises one.
fn redirect_console(redirect: &LeaderRedirect) -> Option<String> {
    let leader = redirect.leader.as_ref()?;
    let console = leader.web_console_uri.as_ref()?;
    Some(console.to_string())
}
