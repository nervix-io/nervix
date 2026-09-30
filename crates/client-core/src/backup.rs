//! Backup downloads: fetching the archive a completed backup retains, by the backup's execution
//! reference, into a local file.
//!
//! - **Owns.** Streaming one backup's archive into a private file beside its destination,
//!   verifying every byte against the summary the backup reported, replacing the destination only
//!   with a verified archive, and the retries, redirects and reconnects a download calls for.
//! - **Depends on.** The wire contract's download frames, tonic's gRPC client, and the client's
//!   session recovery and leader routing.
//! - **Must not know.** How the server assembled or retains the archive, or what the archive holds.
//!
//! A download is not bounded by the request timeout, because an archive may be far larger than
//! one reply: each frame must arrive within the request timeout instead, so a stalled stream is
//! detected however long a healthy one runs. A download that fails in transport, stalls, or ends
//! early starts again from the first byte, for as long as the server retains the archive.

use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
};

use arch_into::ArchInto as _;
use error_stack::Report;
use nervix_client_wire::{
    BackupArchiveStart, BackupDownloadFailure, BackupDownloadFrame, BackupDownloadMessage,
    BackupDownloadRequest, BackupDownloadRequestFrame, EncodedFrame, LeaderRedirect, VerifiedFrame,
    WireDecodeError, WireEncodeError,
    grpc::{ClientBackupDownloadCodec, DOWNLOAD_BACKUP_PATH},
};
use nervix_models::{ArchiveDigest, BackupArchiveSummary, CommandExecutionReference};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncSeekExt as _, AsyncWriteExt as _},
};
use tonic::{Request, Status, Streaming, codegen::http::uri::PathAndQuery};

use crate::{
    client::{Client, RecoveryMode, SessionRecovery},
    error::ClientError,
    exchange::SESSION_LIMITS,
    outcome::Routing,
    upload::expand_user_path,
};

/// Why a backup's archive was not downloaded.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BackupDownloadError {
    /// The server refused to serve the archive.
    #[error("the server refused the download ({failure:?}): {message}")]
    Refused {
        failure: BackupDownloadFailure,
        message: String,
    },
    /// The download's transport failed with this status.
    #[error("the download transport failed with status {code:?}")]
    Transport { code: tonic::Code },
    /// No frame arrived within the request timeout.
    #[error("the download stalled: no frame arrived within the request timeout")]
    Stalled,
    /// The stream ended before the archive completed.
    #[error("the download ended before its archive completed")]
    Interrupted,
    /// The serving node does not retain the archive, and no leader is known to ask instead.
    #[error("the node does not retain the archive, and no leader is elected to ask instead")]
    NoLeader,
    /// The download was redirected more times than the client follows.
    #[error("the download was redirected more times than the client follows")]
    RedirectLoop,
    /// The session could not be moved or restored for the download to continue.
    #[error("the session could not be restored to download the archive")]
    SessionLost,
    /// The server sent frames in an order the download protocol does not allow.
    #[error("the server sent download frames out of order")]
    OutOfOrder,
    /// A frame did not decode.
    #[error("a download frame does not decode")]
    InvalidFrame(#[source] WireDecodeError),
    /// The download request did not encode.
    #[error("the download request does not encode")]
    EncodeRequest(#[source] WireEncodeError),
    /// The archive the server sent is not the one the backup reported.
    #[error("the downloaded archive does not match the backup's summary")]
    Mismatch,
    /// The archive could not be written to the local file system.
    #[error("the archive could not be written beside '{path}' ({kind})")]
    Write {
        path: PathBuf,
        kind: std::io::ErrorKind,
    },
}

impl BackupDownloadError {
    /// Whether downloading the archive again from its first byte may succeed.
    fn is_retryable(&self) -> bool {
        match self {
            Self::Transport { code } => matches!(
                code,
                tonic::Code::Cancelled
                    | tonic::Code::Unknown
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Unavailable
            ),
            Self::Stalled | Self::Interrupted => true,
            Self::Refused { .. }
            | Self::NoLeader
            | Self::RedirectLoop
            | Self::SessionLost
            | Self::OutOfOrder
            | Self::InvalidFrame(_)
            | Self::EncodeRequest(_)
            | Self::Mismatch
            | Self::Write { .. } => false,
        }
    }

    fn transport(status: Status) -> Report<Self> {
        let code = status.code();
        Report::new(status).change_context(Self::Transport { code })
    }
}

/// How one download attempt ended, when it did not fail.
enum AttemptEnd {
    /// Every byte arrived and matched the summary.
    Complete,
    /// The serving node does not retain the archive and is not the leader.
    NotLeader(LeaderRedirect),
}

impl Client {
    /// Downloads the archive the backup `reference` assembled, which `summary` describes, into
    /// `destination`.
    ///
    /// The archive is written to a private file beside `destination` and moved over it only once
    /// its size and BLAKE3 digest match the summary, so `destination` never holds a partial or
    /// foreign archive. A download that fails in transport starts again from the first byte until
    /// the server stops retaining the archive; a complete download releases the archive on the
    /// server, and downloading it again is refused.
    pub async fn download_backup(
        &self,
        reference: &CommandExecutionReference,
        summary: &BackupArchiveSummary,
        destination: &Path,
    ) -> error_stack::Result<(), ClientError> {
        match self.fetch_backup(summary, reference, destination).await {
            Ok(()) => Ok(()),
            Err(report) => {
                let error = ClientError::BackupDownload {
                    reference: reference.clone(),
                    source: report.current_context().clone(),
                };
                Err(report.change_context(error))
            }
        }
    }

    /// Downloads the archive `summary` describes into `destination`, starting again from the first
    /// byte after every failure a retry may overcome, for as long as the server retains it.
    pub(crate) async fn fetch_backup(
        &self,
        summary: &BackupArchiveSummary,
        reference: &CommandExecutionReference,
        destination: &Path,
    ) -> Result<(), Report<BackupDownloadError>> {
        let destination = expand_user_path(destination);
        let mut file = DownloadFile::beside(&destination).await?;
        let encoded = BackupDownloadRequest {
            execution_reference: reference.clone(),
        }
        .encode(&SESSION_LIMITS);
        let request = match encoded {
            Ok(request) => request,
            Err(report) => {
                let context = BackupDownloadError::EncodeRequest(report.current_context().clone());
                return Err(report.change_context(context));
            }
        };
        for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
            nervix_primitives::task::consume_budget().await;
            file.restart().await?;
            let ended = self
                .download_attempt(request.clone(), summary, &mut file)
                .await;
            let failure = match ended {
                Ok(AttemptEnd::Complete) => return file.persist(&destination).await,
                Ok(AttemptEnd::NotLeader(redirect)) => {
                    self.follow_download_redirect(&redirect, attempt).await?;
                    continue;
                }
                Err(failure) => failure,
            };
            // Once the archive's retry validity ends the server refuses it as expired, which is
            // not retried, so the attempts end with the archive's retention.
            let retry_permitted =
                failure.current_context().is_retryable() && Self::await_retry(attempt).await;
            if !retry_permitted {
                return Err(failure);
            }
            let recovery = self.recover_session(RecoveryMode::Replace).await;
            match recovery {
                Ok(SessionRecovery::Ready) => {}
                Ok(SessionRecovery::Unavailable) => return Err(failure),
                Err(error) => {
                    return Err(Report::new(error).change_context(BackupDownloadError::SessionLost));
                }
            }
        }
        Err(Report::new(BackupDownloadError::RedirectLoop))
    }

    /// Moves the session to the leader a node sent the download to, or waits for an election
    /// while no leader is known.
    async fn follow_download_redirect(
        &self,
        redirect: &LeaderRedirect,
        attempt: usize,
    ) -> Result<(), Report<BackupDownloadError>> {
        let Routing::Redirect(leader) = Routing::for_redirect(redirect) else {
            if Self::await_retry(attempt).await {
                return Ok(());
            }
            return Err(Report::new(BackupDownloadError::NoLeader));
        };
        match self.follow_leader(leader).await {
            Ok(()) => Ok(()),
            Err(error) => Err(Report::new(error).change_context(BackupDownloadError::SessionLost)),
        }
    }

    /// Downloads the archive once, from the server the session is connected to, into `file`.
    async fn download_attempt(
        &self,
        request: EncodedFrame<BackupDownloadRequestFrame>,
        summary: &BackupArchiveSummary,
        file: &mut DownloadFile,
    ) -> Result<AttemptEnd, Report<BackupDownloadError>> {
        let mut frames = self.open_download(request).await?;
        let first = self.next_download_frame(&mut frames).await?;
        let start = match first {
            BackupDownloadMessage::Start(start) => start,
            BackupDownloadMessage::NotLeader(redirect) => {
                return Ok(AttemptEnd::NotLeader(redirect));
            }
            BackupDownloadMessage::Failed(failed) => {
                return Err(Report::new(BackupDownloadError::Refused {
                    failure: failed.failure,
                    message: failed.message,
                }));
            }
            BackupDownloadMessage::Chunk(_) | BackupDownloadMessage::Complete => {
                return Err(Report::new(BackupDownloadError::OutOfOrder));
            }
        };
        if !start_matches(&start, summary) {
            return Err(Report::new(BackupDownloadError::Mismatch));
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            match self.next_download_frame(&mut frames).await? {
                BackupDownloadMessage::Chunk(chunk) => {
                    file.append(chunk.bytes(), summary.total_bytes.get())
                        .await?;
                }
                BackupDownloadMessage::Complete => break,
                BackupDownloadMessage::Failed(failed) => {
                    return Err(Report::new(BackupDownloadError::Refused {
                        failure: failed.failure,
                        message: failed.message,
                    }));
                }
                BackupDownloadMessage::Start(_) | BackupDownloadMessage::NotLeader(_) => {
                    return Err(Report::new(BackupDownloadError::OutOfOrder));
                }
            }
        }
        if !file.matches(summary) {
            return Err(Report::new(BackupDownloadError::Mismatch));
        }
        Ok(AttemptEnd::Complete)
    }

    /// Opens a download call on the session's current channel.
    async fn open_download(
        &self,
        request: EncodedFrame<BackupDownloadRequestFrame>,
    ) -> Result<Streaming<VerifiedFrame<BackupDownloadFrame>>, Report<BackupDownloadError>> {
        let channel = self.current_channel().await;
        let mut client = tonic::client::Grpc::new(channel)
            .max_decoding_message_size(SESSION_LIMITS.frame_bytes())
            .max_encoding_message_size(SESSION_LIMITS.frame_bytes());
        let opened =
            nervix_primitives::time::timeout(self.inner.connector.request_timeout(), async {
                client
                    .ready()
                    .await
                    .map_err(|error| Status::from_error(Box::new(error)))?;
                let mut request = Request::new(request);
                self.inner.connector.authorize(&mut request);
                client
                    .server_streaming(
                        request,
                        PathAndQuery::from_static(DOWNLOAD_BACKUP_PATH),
                        ClientBackupDownloadCodec::new(SESSION_LIMITS),
                    )
                    .await
            })
            .await;
        match opened {
            Ok(Ok(response)) => Ok(response.into_inner()),
            Ok(Err(status)) => Err(BackupDownloadError::transport(status)),
            Err(_) => Err(Report::new(BackupDownloadError::Stalled)),
        }
    }

    /// The next frame of a download, which must arrive within the request timeout.
    async fn next_download_frame(
        &self,
        frames: &mut Streaming<VerifiedFrame<BackupDownloadFrame>>,
    ) -> Result<BackupDownloadMessage, Report<BackupDownloadError>> {
        let received = nervix_primitives::time::timeout(
            self.inner.connector.request_timeout(),
            frames.message(),
        )
        .await;
        let frame = match received {
            Err(_) => return Err(Report::new(BackupDownloadError::Stalled)),
            Ok(Err(status)) => return Err(BackupDownloadError::transport(status)),
            Ok(Ok(None)) => return Err(Report::new(BackupDownloadError::Interrupted)),
            Ok(Ok(Some(frame))) => frame,
        };
        match BackupDownloadMessage::decode(&frame) {
            Ok(message) => Ok(message),
            Err(report) => {
                let context = BackupDownloadError::InvalidFrame(report.current_context().clone());
                Err(report.change_context(context))
            }
        }
    }
}

/// Whether the archive a download starts is the one the backup reported.
fn start_matches(start: &BackupArchiveStart, summary: &BackupArchiveSummary) -> bool {
    start.total_bytes == summary.total_bytes && start.digest == summary.digest
}

/// The private file a download writes an archive into before it replaces the destination.
///
/// The file is created beside the destination, so replacing the destination is a rename within
/// one file system, and with owner-only permissions, because an archive holds password hashes and
/// secrets.
struct DownloadFile {
    staged: NamedTempFile,
    file: File,
    hasher: blake3::Hasher,
    written: u64,
    destination: PathBuf,
}

impl DownloadFile {
    async fn beside(destination: &Path) -> Result<Self, Report<BackupDownloadError>> {
        let directory = match destination.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            Some(_) | None => PathBuf::from("."),
        };
        let created = tempfile::Builder::new()
            .prefix(".nervix-backup-")
            .tempfile_in(&directory);
        let staged = match created {
            Ok(staged) => staged,
            Err(error) => return Err(write_failure(destination, error)),
        };
        let file = match staged.reopen() {
            Ok(file) => file,
            Err(error) => return Err(write_failure(destination, error)),
        };
        Ok(Self {
            staged,
            file: File::from_std(file),
            hasher: blake3::Hasher::new(),
            written: 0,
            destination: destination.to_path_buf(),
        })
    }

    /// Empties the file for a download that starts again from the first byte.
    async fn restart(&mut self) -> Result<(), Report<BackupDownloadError>> {
        if let Err(error) = self.file.set_len(0).await {
            return Err(write_failure(&self.destination, error));
        }
        if let Err(error) = self.file.seek(SeekFrom::Start(0)).await {
            return Err(write_failure(&self.destination, error));
        }
        self.hasher = blake3::Hasher::new();
        self.written = 0;
        Ok(())
    }

    /// Appends one chunk, refusing an archive that grows past the size its backup reported.
    async fn append(
        &mut self,
        bytes: &[u8],
        total_bytes: u64,
    ) -> Result<(), Report<BackupDownloadError>> {
        let length: u64 = bytes.len().arch_into();
        let Some(written) = self.written.checked_add(length) else {
            return Err(Report::new(BackupDownloadError::Mismatch));
        };
        if written > total_bytes {
            return Err(Report::new(BackupDownloadError::Mismatch));
        }
        if let Err(error) = self.file.write_all(bytes).await {
            return Err(write_failure(&self.destination, error));
        }
        self.hasher.update(bytes);
        self.written = written;
        Ok(())
    }

    /// Whether every byte written is the archive `summary` describes.
    fn matches(&self, summary: &BackupArchiveSummary) -> bool {
        let digest = ArchiveDigest::from_bytes(*self.hasher.finalize().as_bytes());
        self.written == summary.total_bytes.get() && digest == summary.digest
    }

    /// Makes the verified archive durable and moves it over `destination`.
    async fn persist(mut self, destination: &Path) -> Result<(), Report<BackupDownloadError>> {
        let flushed = async {
            self.file.flush().await?;
            self.file.sync_all().await
        }
        .await;
        if let Err(error) = flushed {
            return Err(write_failure(&self.destination, error));
        }
        let target = destination.to_path_buf();
        let staged = self.staged;
        let persisted =
            nervix_primitives::task::spawn_blocking(move || staged.persist(&target)).await;
        let error = match persisted {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(error)) => error.error,
            Err(_) => std::io::Error::other("the task that moved the archive into place stopped"),
        };
        Err(write_failure(&self.destination, error))
    }
}

/// A local write failure for the archive bound for `destination`.
fn write_failure(destination: &Path, error: std::io::Error) -> Report<BackupDownloadError> {
    let kind = error.kind();
    Report::new(error).change_context(BackupDownloadError::Write {
        path: destination.to_path_buf(),
        kind,
    })
}
