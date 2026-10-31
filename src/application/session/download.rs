//! Backup archive downloads streamed over a session transport.
//!
//! Layer: edges.
//!
//! - **Owns.** Reading one download request, answering it with the archive its backup retains on
//!   this node, the endpoints of the node that retains it, a redirect to the leader, or a typed
//!   refusal, and the bounded frame queue that carries the answer to the transport.
//! - **Depends on.** This node's retained backups and the stream that sends one, the durable
//!   command execution record that names the node retaining an archive and tells a spent or
//!   expired reference from an unknown one, cluster gossip for which node processes are live, and
//!   the client wire contract.
//! - **Must not know.** How an archive was assembled, or how the transport carries the stream or
//!   authenticates it.
//!
//! A backup's archive stays on the node that assembled it, for no longer than the retry validity
//! of the backup's execution reference, which any node reads from the reference itself. The
//! backup's recorded outcome names that node's process, and only that node streams the archive:
//! leadership can move after the backup completes, and a client recovers the outcome from whichever
//! node it reaches. Every other node that applied the outcome sends the client to the named node
//! while its process is live. A node without the outcome sends the client to the leader, whose
//! record answers why nothing can be downloaded.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use std::pin::Pin;

use error_stack::Report;
use futures_util::{Stream, stream};
use meticulous::OptionExt as _;
use nervix_client_wire::{
    BackupDownloadFailed, BackupDownloadFailure, BackupDownloadFrame, BackupDownloadMessage,
    BackupDownloadRequest, BackupDownloadRequestFrame, EncodedFrame, LeaderRedirect, NodeEndpoints,
    SessionLimits, VerifiedFrame, WireEncodeError,
};
use nervix_consensus::{CommandExecution, CommandExecutionState, RecordedBackupArchive};
use nervix_models::{ClusterNodeIdentity, CommandExecutionReference, Timestamp, UserName};
use nervix_primitives::{stream::wrappers::ReceiverStream, sync::mpsc};
use tracing::{debug, info};

use super::outcome::{leader_redirect, node_endpoints};
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
    /// The recorded outcome names another live node's process as the one retaining the archive.
    Holder(NodeEndpoints),
    /// The node has not applied the backup's recorded outcome and is not the leader.
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
            DownloadAnswer::Holder(holder) => {
                let frame = BackupDownloadMessage::encode_holder(&holder, &limits)?;
                return Ok(DownloadFrames::Answer(frame));
            }
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
        #[cfg(feature = "testing")]
        let runtime = self.inner.runtime.clone();
        #[cfg(feature = "testing")]
        let node_id = self.inner.consensus.local_node_id().clone();
        self.inner.service_tasks.spawn(async move {
            #[cfg(feature = "testing")]
            runtime.pause_backup_download_if_armed(&node_id).await;
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
        if let Err(error) = self.available_cluster_parameters() {
            return refused(BackupDownloadFailure::ReadFailed, format!("{error:#}"));
        }
        let now = current_timestamp();
        if self.backup_reference_expired(reference, now) {
            return expired(reference);
        }
        let execution = self
            .inner
            .consensus
            .current_command_execution(reference)
            .await;
        let record = DownloadRecord::of(execution.as_ref());
        let local = self.inner.cluster.local_node_identity().await;
        match route_download(record, &local, user, reference, now) {
            DownloadRoute::Here => self.retained_answer(user, reference, now),
            DownloadRoute::Elsewhere(holder) => {
                // An archive this node retains under the reference was assembled by a run whose
                // outcome was not recorded, so no download may take it.
                self.inner.retained_backups.release(reference);
                self.holder_answer(reference, &holder).await
            }
            DownloadRoute::Answered(answer) => answer,
            DownloadRoute::Unrecorded(unrecorded) => {
                self.unrecorded_answer(reference, unrecorded).await
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
        let validity = self.command_execution_policy().retry_validity();
        match issued_at.checked_add(validity) {
            Ok(retained_until) => now >= retained_until,
            // A window ending past the last representable instant has not ended.
            Err(_) => false,
        }
    }

    /// How the node process the recorded outcome names answers: with the archive while it
    /// retains it.
    fn retained_answer(
        &self,
        user: &UserName,
        reference: &CommandExecutionReference,
        now: Timestamp,
    ) -> DownloadAnswer {
        let refusal = match self.inner.retained_backups.open(reference, user, now) {
            Ok(lease) => return DownloadAnswer::Stream(lease),
            Err(refusal) => refusal,
        };
        match refusal {
            RetainedArchiveRefusal::Expired => expired(reference),
            RetainedArchiveRefusal::NotOwner => not_owner(reference),
            // The process that assembled the archive still runs, so only a download released it.
            RetainedArchiveRefusal::NotRetained => refused(
                BackupDownloadFailure::NotRetained,
                format!(
                    "the archive of backup '{reference}' is no longer retained: a download \
                     already collected it; run the backup again"
                ),
            ),
        }
    }

    /// How a node answers when the recorded outcome names `holder`, another node's process: with
    /// that node's endpoints while the process is live.
    async fn holder_answer(
        &self,
        reference: &CommandExecutionReference,
        holder: &ClusterNodeIdentity,
    ) -> DownloadAnswer {
        let availability = self.inner.cluster.availability_state().await;
        if !availability.live_identities().contains(holder) {
            return refused(
                BackupDownloadFailure::NotRetained,
                format!(
                    "the archive of backup '{reference}' is no longer retained: node '{}', which \
                     assembled it, restarted or left the cluster; run the backup again",
                    holder.node_id()
                ),
            );
        }
        let location = self.node_location(holder.node_id().clone()).await;
        DownloadAnswer::Holder(node_endpoints(location))
    }

    /// How a node answers when its record holds no completed backup's archive under the
    /// reference: the leader's record says why, so a follower sends the client there.
    async fn unrecorded_answer(
        &self,
        reference: &CommandExecutionReference,
        unrecorded: Unrecorded,
    ) -> DownloadAnswer {
        let leader = self.inner.consensus.current_leader().await;
        if leader.as_ref() != Some(self.inner.consensus.local_node_id()) {
            let redirect = self.redirect_to_leader(leader.into()).await;
            return DownloadAnswer::Redirect(leader_redirect(redirect));
        }
        unrecorded.refusal(reference)
    }
}

/// What a node's record of a command says, as a download of its archive needs it.
#[derive(Debug, Clone, Copy)]
enum DownloadRecord<'a> {
    /// The node records no command under the reference.
    Unknown,
    /// The command is still applying.
    Applying { owner: &'a UserName },
    /// The reference's retry validity ended, and the command's outcome with it.
    Expired,
    /// The command finished, recording the archive it assembled, or none.
    Finished {
        owner: &'a UserName,
        archive: Option<&'a RecordedBackupArchive>,
    },
}

impl<'a> DownloadRecord<'a> {
    fn of(execution: Option<&'a CommandExecution>) -> Self {
        let Some(execution) = execution else {
            return Self::Unknown;
        };
        match &execution.state {
            CommandExecutionState::Applying { owner, .. } => Self::Applying { owner },
            CommandExecutionState::Expired => Self::Expired,
            CommandExecutionState::Finished { result, .. } => {
                let owner = execution
                    .owner()
                    .assured("a finished execution keeps the owner of its request");
                Self::Finished {
                    owner,
                    archive: result.backup.as_ref(),
                }
            }
        }
    }
}

/// Where a download goes, by what this node's record says.
enum DownloadRoute {
    /// The recorded outcome names this node's process, which streams the archive while it
    /// retains it.
    Here,
    /// The recorded outcome names another node's process.
    Elsewhere(ClusterNodeIdentity),
    /// The record answers the download wherever the archive is.
    Answered(DownloadAnswer),
    /// The record holds no completed backup's archive.
    Unrecorded(Unrecorded),
}

/// Why a node's record holds no completed backup's archive under a reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unrecorded {
    Unknown,
    Applying,
    Expired,
    NoArchive,
}

impl Unrecorded {
    /// How the leader, whose record is the authority, refuses the download.
    fn refusal(self, reference: &CommandExecutionReference) -> DownloadAnswer {
        match self {
            Self::Unknown => refused(
                BackupDownloadFailure::NotRetained,
                format!("no backup archive is retained under execution reference '{reference}'"),
            ),
            Self::Applying => refused(
                BackupDownloadFailure::NotRetained,
                format!(
                    "backup '{reference}' is still assembling its archive; run it again with the \
                     same execution reference to wait for its outcome"
                ),
            ),
            Self::Expired => expired(reference),
            Self::NoArchive => refused(
                BackupDownloadFailure::NotRetained,
                format!("command '{reference}' assembled no backup archive"),
            ),
        }
    }
}

/// Where a download of the archive `reference` names goes for `user` at `now`, by what `record`,
/// the record of the node process `local`, says.
fn route_download(
    record: DownloadRecord<'_>,
    local: &ClusterNodeIdentity,
    user: &UserName,
    reference: &CommandExecutionReference,
    now: Timestamp,
) -> DownloadRoute {
    match record {
        DownloadRecord::Unknown => DownloadRoute::Unrecorded(Unrecorded::Unknown),
        DownloadRecord::Expired => DownloadRoute::Unrecorded(Unrecorded::Expired),
        DownloadRecord::Applying { owner } => {
            if owner != user {
                return DownloadRoute::Answered(not_owner(reference));
            }
            DownloadRoute::Unrecorded(Unrecorded::Applying)
        }
        DownloadRecord::Finished { owner, archive } => {
            if owner != user {
                return DownloadRoute::Answered(not_owner(reference));
            }
            let Some(archive) = archive else {
                return DownloadRoute::Unrecorded(Unrecorded::NoArchive);
            };
            if now >= archive.summary.retained_until {
                return DownloadRoute::Answered(expired(reference));
            }
            if archive.holder == *local {
                return DownloadRoute::Here;
            }
            DownloadRoute::Elsewhere(archive.holder.clone())
        }
    }
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

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;
    use nervix_models::{
        ArchiveDigest, BackupArchiveSummary, BackupResources, ClusterNodeIncarnation,
        ClusterNodeName,
    };

    use super::*;

    fn identity(node: &str, incarnation: u64) -> ClusterNodeIdentity {
        ClusterNodeIdentity::new(
            ClusterNodeName::parse(node).assured("the test node is an identifier-shaped literal"),
            ClusterNodeIncarnation::new(incarnation),
        )
    }

    fn user(raw: &str) -> UserName {
        UserName::parse(raw).assured("the test user is valid")
    }

    fn reference() -> CommandExecutionReference {
        CommandExecutionReference::parse("0192d4e4-7b36-7c3e-9f00-5b2d8c3a1e44")
            .assured("the test reference is a UUID")
    }

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_unix_nanos(seconds * 1_000_000_000)
    }

    /// An archive retained by `holder` until the hundredth second.
    fn archive(holder: ClusterNodeIdentity) -> RecordedBackupArchive {
        RecordedBackupArchive {
            summary: BackupArchiveSummary {
                total_bytes: std::num::NonZeroU64::new(4096).assured("4096 is not zero"),
                digest: ArchiveDigest::from_bytes([5; 32]),
                captured_at: at(10),
                retained_until: at(100),
                resources: BackupResources::Included,
                users: Some(1),
                domains: Vec::new(),
            },
            holder,
        }
    }

    fn finished<'a>(
        owner: &'a UserName,
        archive: Option<&'a RecordedBackupArchive>,
    ) -> DownloadRecord<'a> {
        DownloadRecord::Finished { owner, archive }
    }

    /// The refusal a route answers with, which a test expects.
    fn refusal(route: DownloadRoute) -> BackupDownloadFailure {
        let DownloadRoute::Answered(DownloadAnswer::Refused(refused)) = route else {
            panic!("the route answers with a refusal");
        };
        refused.failure
    }

    #[test]
    fn only_the_node_process_the_outcome_names_streams_the_archive() {
        let local = identity("node-1", 1);
        let alice = user("alice");
        let here = archive(local.clone());
        let route = route_download(
            finished(&alice, Some(&here)),
            &local,
            &alice,
            &reference(),
            at(50),
        );
        assert!(
            matches!(route, DownloadRoute::Here),
            "the recorded holder streams it"
        );

        let other = archive(identity("node-2", 1));
        let route = route_download(
            finished(&alice, Some(&other)),
            &local,
            &alice,
            &reference(),
            at(50),
        );
        let DownloadRoute::Elsewhere(holder) = route else {
            panic!("another node's archive is downloaded there");
        };
        assert_eq!(holder, identity("node-2", 1));

        // The outcome names this node's former process, whose archive went with its restart.
        let former = archive(identity("node-1", 0));
        let route = route_download(
            finished(&alice, Some(&former)),
            &local,
            &alice,
            &reference(),
            at(50),
        );
        let DownloadRoute::Elsewhere(holder) = route else {
            panic!("a former process of this node is not this process");
        };
        assert_eq!(holder, identity("node-1", 0));
    }

    #[test]
    fn a_recorded_archive_is_refused_to_another_user_and_after_its_retention() {
        let local = identity("node-1", 1);
        let alice = user("alice");
        let bob = user("bob");
        let other = archive(identity("node-2", 1));
        let route = route_download(
            finished(&alice, Some(&other)),
            &local,
            &bob,
            &reference(),
            at(50),
        );
        assert_eq!(refusal(route), BackupDownloadFailure::NotOwner);
        let route = route_download(
            finished(&alice, Some(&other)),
            &local,
            &alice,
            &reference(),
            at(100),
        );
        assert_eq!(refusal(route), BackupDownloadFailure::Expired);
        let route = route_download(
            DownloadRecord::Applying { owner: &alice },
            &local,
            &bob,
            &reference(),
            at(50),
        );
        assert_eq!(refusal(route), BackupDownloadFailure::NotOwner);
    }

    #[test]
    fn a_record_without_a_completed_archive_leaves_the_answer_to_the_leader() {
        let local = identity("node-1", 1);
        let alice = user("alice");
        let routes = [
            (DownloadRecord::Unknown, Unrecorded::Unknown),
            (
                DownloadRecord::Applying { owner: &alice },
                Unrecorded::Applying,
            ),
            (DownloadRecord::Expired, Unrecorded::Expired),
            (finished(&alice, None), Unrecorded::NoArchive),
        ];
        for (record, expected) in routes {
            let route = route_download(record, &local, &alice, &reference(), at(50));
            let DownloadRoute::Unrecorded(unrecorded) = route else {
                panic!("the record holds no completed archive: {record:?}");
            };
            assert_eq!(unrecorded, expected);
        }
        for unrecorded in [
            Unrecorded::Unknown,
            Unrecorded::Applying,
            Unrecorded::NoArchive,
        ] {
            let DownloadAnswer::Refused(refused) = unrecorded.refusal(&reference()) else {
                panic!("the leader refuses a download its record has no archive for");
            };
            assert_eq!(refused.failure, BackupDownloadFailure::NotRetained);
        }
        let DownloadAnswer::Refused(refused) = Unrecorded::Expired.refusal(&reference()) else {
            panic!("the leader refuses an expired reference");
        };
        assert_eq!(refused.failure, BackupDownloadFailure::Expired);
    }
}
