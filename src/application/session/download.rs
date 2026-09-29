//! Backup archive downloads streamed over a session transport.
//!
//! Layer: edges.
//!
//! - **Owns.** Reading one download request, answering it with the archive its backup retains on
//!   this node, a redirect to the leader, or a typed refusal, and the bounded frame queue that
//!   carries the answer to the transport.
//! - **Depends on.** This node's retained backups and the stream that sends one, the durable
//!   command execution record that tells a spent or expired reference from an unknown one, and the
//!   client wire contract.
//! - **Must not know.** How an archive was assembled, or how the transport carries the stream or
//!   authenticates it.
//!
//! A backup's archive stays on the node that assembled it, for no longer than the retry validity
//! of the backup's execution reference, which any node reads from the reference itself. A node that
//! does not retain an unexpired archive sends the client to the leader, and the leader, which then
//! does not retain it either, answers from the backup's durable outcome: the archive was already
//! downloaded, or its node no longer holds it.

use std::pin::Pin;

use error_stack::Report;
use futures_util::{Stream, stream};
use nervix_client_wire::{
    BackupDownloadFailed, BackupDownloadFailure, BackupDownloadFrame, BackupDownloadMessage,
    BackupDownloadRequest, BackupDownloadRequestFrame, EncodedFrame, LeaderRedirect, SessionLimits,
    VerifiedFrame, WireEncodeError,
};
use nervix_consensus::{CommandExecution, CommandExecutionState};
use nervix_models::{CommandExecutionReference, Timestamp, UserName};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

use super::outcome::leader_redirect;
use crate::{
    application::{
        backup::retained::{
            ArchiveLease, DOWNLOAD_FRAME_CAPACITY, DownloadEnd, RetainedArchiveRefusal,
            stream_archive,
        },
        domain_clock::current_timestamp,
        session_service::SessionServiceImpl,
    },
    runtime::StagedArtifact,
};

/// How a node answers one download request.
enum DownloadAnswer {
    /// The node retains the archive and streams it.
    Stream(ArchiveLease<StagedArtifact>),
    /// The node does not retain the archive and is not the leader.
    Redirect(LeaderRedirect),
    /// The archive cannot be downloaded, for the reason given.
    Refused(BackupDownloadFailed),
}

fn refused(failure: BackupDownloadFailure, message: String) -> DownloadAnswer {
    DownloadAnswer::Refused(BackupDownloadFailed { failure, message })
}

/// The frames that answer one download request.
pub(in crate::application) enum DownloadFrames {
    /// The one frame of an answer that streams no archive: a redirect or a refusal.
    Answer(EncodedFrame<BackupDownloadFrame>),
    /// The frames of a streamed archive, which end after its last one.
    Archive(mpsc::Receiver<EncodedFrame<BackupDownloadFrame>>),
}

/// The frames of one download, in the order a transport sends them.
pub(in crate::application) type DownloadFrameStream =
    Pin<Box<dyn Stream<Item = EncodedFrame<BackupDownloadFrame>> + Send + 'static>>;

impl DownloadFrames {
    pub(in crate::application) fn into_stream(self) -> DownloadFrameStream {
        match self {
            Self::Answer(frame) => Box::pin(stream::iter([frame])),
            Self::Archive(frames) => Box::pin(ReceiverStream::new(frames)),
        }
    }
}

impl SessionServiceImpl {
    /// Answers one download request from `user`. A streamed archive is read only as fast as the
    /// transport takes its frames. An answer that does not fit a frame is returned as the
    /// encoding error, which the transport reports as its own failure.
    pub(in crate::application) async fn serve_backup_download(
        &self,
        user: UserName,
        request: &VerifiedFrame<BackupDownloadRequestFrame>,
        limits: SessionLimits,
    ) -> Result<DownloadFrames, Report<WireEncodeError>> {
        let reference = match BackupDownloadRequest::decode(request) {
            Ok(request) => request.execution_reference,
            Err(error) => {
                let failed = BackupDownloadFailed {
                    failure: BackupDownloadFailure::InvalidRequest,
                    message: format!(
                        "the download request is invalid: {}",
                        error.current_context()
                    ),
                };
                let frame = BackupDownloadMessage::encode_failed(&failed, &limits)?;
                return Ok(DownloadFrames::Answer(frame));
            }
        };
        let lease = match self.download_answer(&user, &reference).await {
            DownloadAnswer::Stream(lease) => lease,
            DownloadAnswer::Redirect(redirect) => {
                let frame = BackupDownloadMessage::encode_redirect(&redirect, &limits)?;
                return Ok(DownloadFrames::Answer(frame));
            }
            DownloadAnswer::Refused(failed) => {
                let frame = BackupDownloadMessage::encode_failed(&failed, &limits)?;
                return Ok(DownloadFrames::Answer(frame));
            }
        };
        let reader = match lease.archive().open_reader().await {
            Ok(reader) => reader,
            Err(error) => {
                debug!(
                    execution_reference = %reference,
                    error = %error,
                    "a retained backup archive could not be opened for download"
                );
                let failed = BackupDownloadFailed {
                    failure: BackupDownloadFailure::ReadFailed,
                    message: "the retained archive could not be read; download it again"
                        .to_string(),
                };
                let frame = BackupDownloadMessage::encode_failed(&failed, &limits)?;
                return Ok(DownloadFrames::Answer(frame));
            }
        };
        let (frames, archive_frames) = mpsc::channel(DOWNLOAD_FRAME_CAPACITY);
        self.inner.service_tasks.spawn(async move {
            let end = stream_archive(lease, reader, frames, limits).await;
            match end {
                DownloadEnd::Collected => {
                    info!(
                        execution_reference = %reference,
                        "backup archive downloaded and released"
                    );
                }
                DownloadEnd::AlreadyCollected
                | DownloadEnd::ClientGone
                | DownloadEnd::ReadFailed => {
                    debug!(
                        execution_reference = %reference,
                        end = ?end,
                        "backup archive download ended"
                    );
                }
            }
        });
        Ok(DownloadFrames::Archive(archive_frames))
    }

    /// How this node answers a download of the archive `reference` names for `user`.
    async fn download_answer(
        &self,
        user: &UserName,
        reference: &CommandExecutionReference,
    ) -> DownloadAnswer {
        let now = current_timestamp();
        if self.backup_reference_expired(reference, now) {
            return expired(reference);
        }
        let refusal = match self.inner.retained_backups.open(reference, user, now) {
            Ok(lease) => return DownloadAnswer::Stream(lease),
            Err(refusal) => refusal,
        };
        match refusal {
            RetainedArchiveRefusal::Expired => expired(reference),
            RetainedArchiveRefusal::NotOwner => not_owner(reference),
            RetainedArchiveRefusal::NotRetained => {
                self.unretained_answer(user, reference, now).await
            }
        }
    }

    /// Whether the retry validity of `reference` ended by `now`. An archive is retained no longer
    /// than that on any node, and the reference's own time says so without its execution history,
    /// which the cluster reclaims after the same window.
    fn backup_reference_expired(
        &self,
        reference: &CommandExecutionReference,
        now: Timestamp,
    ) -> bool {
        // A reference without a UUIDv7 time was never admitted as a backup, so it has no retry
        // window to end; the answer for it is that no archive is retained.
        let Ok(issued_at) = reference.retry_issued_at() else {
            return false;
        };
        let validity = self.inner.command_execution_policy.retry_validity();
        match issued_at.checked_add(validity) {
            Ok(retained_until) => now >= retained_until,
            // A window ending past the last representable instant has not ended.
            Err(_) => false,
        }
    }

    /// How a node that does not retain the archive `reference` names answers its download.
    async fn unretained_answer(
        &self,
        user: &UserName,
        reference: &CommandExecutionReference,
        now: Timestamp,
    ) -> DownloadAnswer {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            let redirect = self.redirect_to_leader(leader).await;
            return DownloadAnswer::Redirect(leader_redirect(redirect));
        }
        let execution = self
            .inner
            .consensus
            .current_command_execution(reference)
            .await;
        let Some(execution) = execution else {
            return refused(
                BackupDownloadFailure::NotRetained,
                format!("no backup archive is retained under execution reference '{reference}'"),
            );
        };
        durable_answer(&execution, user, reference, now)
    }
}

/// How the leader answers a download from the durable outcome of the command `reference` names,
/// when no node it can name retains the archive.
fn durable_answer(
    execution: &CommandExecution,
    user: &UserName,
    reference: &CommandExecutionReference,
    now: Timestamp,
) -> DownloadAnswer {
    if let Some(owner) = execution.owner()
        && owner != user
    {
        return not_owner(reference);
    }
    let result = match &execution.state {
        CommandExecutionState::Expired => return expired(reference),
        CommandExecutionState::Applying { .. } => {
            return refused(
                BackupDownloadFailure::NotRetained,
                format!(
                    "backup '{reference}' is still assembling its archive; run it again with the \
                     same execution reference to wait for its outcome"
                ),
            );
        }
        CommandExecutionState::Finished { result, .. } => result,
    };
    let Some(summary) = &result.backup else {
        return refused(
            BackupDownloadFailure::NotRetained,
            format!("command '{reference}' assembled no backup archive"),
        );
    };
    if now >= summary.retained_until {
        return expired(reference);
    }
    refused(
        BackupDownloadFailure::NotRetained,
        format!(
            "the archive of backup '{reference}' is no longer retained: a download already \
             collected it, or the node that assembled it restarted; run the backup again"
        ),
    )
}

fn expired(reference: &CommandExecutionReference) -> DownloadAnswer {
    refused(
        BackupDownloadFailure::Expired,
        format!(
            "the archive of backup '{reference}' expired with its execution reference's retry \
             validity; run the backup again"
        ),
    )
}

fn not_owner(reference: &CommandExecutionReference) -> DownloadAnswer {
    refused(
        BackupDownloadFailure::NotOwner,
        format!("backup '{reference}' was run by another user"),
    )
}
