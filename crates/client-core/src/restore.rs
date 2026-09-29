//! Restores: the archive a local file holds, and the stream that carries it to the leader.
//!
//! - **Owns.** Reading the archive a `RESTORE` names on this machine, declaring its size and
//!   BLAKE3 digest, streaming it as restore frames under the restore's execution reference, and the
//!   routing each reply calls for until the restore's outcome is known.
//! - **Depends on.** The wire contract's restore frames, tonic's gRPC client, and the client's
//!   session recovery and leader routing.
//! - **Must not know.** How the server stages, verifies, plans or applies a restore.
//!
//! Every attempt, redirect and repetition sends the same execution reference, so a restore the
//! leader already admitted is joined or its outcome recovered, never applied twice. An archive may
//! take far longer to send than a command takes to run, so no attempt is bounded as a whole: each
//! frame must reach the transport within the request timeout, and so must the reply once the last
//! frame was sent.

use std::{
    io::{self, Read as _},
    num::NonZeroU64,
    path::{Path, PathBuf},
    time::Duration,
};

use arch_into::ArchInto as _;
use meticulous::ResultExt as _;
use nervix_client_wire::{
    EncodedFrame, RequestId, RestoreChunk, RestoreDisposition, RestoreFrame, RestoreReply,
    RestoreReplyFrame, RestoreStart, VerifiedFrame,
    grpc::{ClientRestoreCodec, RESTORE_BACKUP_PATH},
};
use nervix_models::{ArchiveDigest, CommandExecutionReference, Restore, RestoreArchive};
use nervix_primitives::{stream::wrappers::ReceiverStream, sync::mpsc};
use tokio::{fs::File, io::AsyncReadExt as _};
use tonic::{Request, Response, Status, codegen::http::uri::PathAndQuery, transport::Channel};

use crate::{
    client::{Client, RecoveryMode, SessionRecovery},
    connection::GrpcConnector,
    error::{ClientError, RequestKind},
    exchange::SESSION_LIMITS,
    outcome::{CommandOutcome, Routing},
    upload::{expand_user_path, upload_status_is_retryable},
};

/// Archive bytes one restore frame carries.
const RESTORE_CHUNK_BYTES: usize = 256 * 1024;

// A chunk fills at most half a frame, far more room than its frame's envelope needs, so a
// non-empty chunk always encodes.
const _: () = assert!(RESTORE_CHUNK_BYTES <= SESSION_LIMITS.frame_bytes() / 2);

/// Restore frames queued for the stream before the archive reader waits for the transport.
const RESTORE_FRAME_CAPACITY: usize = 8;

/// The request identity of a restore's start frame. Every attempt is a stream of its own that
/// exactly one reply answers, so the identity only has to be non-zero.
const RESTORE_REQUEST_ID: RequestId = RequestId::new(NonZeroU64::MIN);

/// Reads the whole archive at `path` once, off the async workers, to measure and digest it.
async fn measure_archive(path: PathBuf) -> io::Result<RestoreDigest> {
    let measure = nervix_primitives::task::spawn_blocking(move || -> io::Result<RestoreDigest> {
        let mut file = std::fs::File::open(&path)?;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0_u8; RESTORE_CHUNK_BYTES];
        let mut length = 0_u64;
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            let read: u64 = read.arch_into();
            length = length
                .checked_add(read)
                .ok_or_else(|| io::Error::other("the archive is larger than u64 counts"))?;
        }
        Ok(RestoreDigest {
            length,
            digest: *hasher.finalize().as_bytes(),
        })
    });
    match measure.await {
        Ok(measured) => measured,
        Err(error) => Err(io::Error::other(error)),
    }
}

/// What reading an archive through measured.
struct RestoreDigest {
    length: u64,
    digest: [u8; 32],
}

impl Client {
    /// Restores from the archive the local file `restore.source` names, under a new execution
    /// reference. `on_progress` is told each number of archive bytes handed to the transport; a
    /// retried stream counts its bytes again.
    pub async fn restore(
        &self,
        restore: &Restore,
        on_progress: impl Fn(u64) + Send + Sync + Clone + 'static,
    ) -> Result<CommandOutcome, ClientError> {
        let reference = CommandExecutionReference::parse(uuid::Uuid::now_v7().to_string()).assured(
            "a hyphenated UUID is 36 ASCII hex digits and hyphens, within the execution reference \
             grammar",
        );
        self.restore_with_reference(restore, &reference, on_progress)
            .await
    }

    /// Restores from the archive the local file `restore.source` names, under `reference`.
    /// Sending the same reference again joins the restore the leader admitted under it, or
    /// returns its recorded outcome.
    pub async fn restore_with_reference(
        &self,
        restore: &Restore,
        reference: &CommandExecutionReference,
        on_progress: impl Fn(u64) + Send + Sync + Clone + 'static,
    ) -> Result<CommandOutcome, ClientError> {
        match self.run_restore(restore, reference, on_progress).await {
            Ok(outcome) => Ok(outcome),
            Err(error) if error.can_hide_admitted_work() => Err(ClientError::UncertainCommand {
                reference: reference.clone(),
                source: Box::new(error),
            }),
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn run_restore(
        &self,
        restore: &Restore,
        reference: &CommandExecutionReference,
        on_progress: impl Fn(u64) + Send + Sync + Clone + 'static,
    ) -> Result<CommandOutcome, ClientError> {
        let path = expand_user_path(Path::new(&restore.source));
        let measured = match measure_archive(path.clone()).await {
            Ok(measured) => measured,
            Err(error) => {
                return Err(ClientError::ReadRestoreArchive {
                    path,
                    kind: error.kind(),
                });
            }
        };
        let Some(total_bytes) = NonZeroU64::new(measured.length) else {
            return Err(ClientError::EmptyRestoreArchive { path });
        };
        let archive = RestoreArchive {
            total_bytes,
            digest: ArchiveDigest::from_bytes(measured.digest),
        };
        let statement = restore.to_canonical_nspl();
        let frame_timeout = self.inner.connector.request_timeout();
        for attempt in 0..Self::MAX_LEADER_ROUTING_ATTEMPTS {
            nervix_primitives::task::consume_budget().await;
            let start = RestoreStart {
                request_id: RESTORE_REQUEST_ID,
                execution_reference: reference.clone(),
                statement: statement.clone(),
                archive,
            }
            .encode(&SESSION_LIMITS)
            .map_err(|report| ClientError::EncodeRequest {
                request: RequestKind::Restore,
                source: report.current_context().clone(),
            })?;
            let archive_file = match File::open(&path).await {
                Ok(archive_file) => archive_file,
                Err(error) => {
                    return Err(ClientError::ReadRestoreArchive {
                        path: path.clone(),
                        kind: error.kind(),
                    });
                }
            };
            let sent = RestoreStreamAttempt {
                channel: self.current_channel().await,
                start,
                archive: archive_file,
                frame_timeout,
            }
            .send(&self.inner.connector, on_progress.clone())
            .await;
            let response = match sent {
                Ok(response) => response,
                Err(AttemptFailure::Read(kind)) => {
                    return Err(ClientError::ReadRestoreArchive {
                        path: path.clone(),
                        kind,
                    });
                }
                Err(AttemptFailure::Transport(status))
                    if upload_status_is_retryable(&status) && Self::await_retry(attempt).await =>
                {
                    match self.recover_session(RecoveryMode::Replace).await? {
                        SessionRecovery::Ready => continue,
                        SessionRecovery::Unavailable => return Err(ClientError::Restore(status)),
                    }
                }
                Err(AttemptFailure::Transport(status)) => return Err(ClientError::Restore(status)),
            };
            let reply = RestoreReply::decode(response.get_ref()).map_err(|report| {
                ClientError::InvalidRestoreReply(report.current_context().clone())
            })?;
            if let Some(request_id) = reply.request_id
                && request_id != RESTORE_REQUEST_ID
            {
                return Err(ClientError::UnexpectedReply {
                    request: RequestKind::Restore,
                });
            }
            let outcome = match reply.disposition {
                RestoreDisposition::Outcome(outcome) => {
                    if outcome.execution_reference != *reference {
                        return Err(ClientError::ExecutionReferenceMismatch {
                            expected: reference.clone(),
                            received: outcome.execution_reference,
                        });
                    }
                    CommandOutcome::from(*outcome)
                }
                RestoreDisposition::UploadFailed { failure, message } => {
                    CommandOutcome::restore_refused(reference.clone(), failure, message)
                }
            };
            match outcome.routing() {
                Routing::Redirect(leader) => self.follow_leader(leader).await?,
                Routing::AwaitElection | Routing::AwaitOutcome
                    if Self::await_retry(attempt).await => {}
                _ => return Ok(outcome),
            }
        }
        let mut exhausted =
            CommandOutcome::failed_locally("restore redirect loop exceeded".to_string());
        exhausted.execution_reference = Some(reference.clone());
        Ok(exhausted)
    }
}

/// Why one attempt to stream a restore ended without a reply.
enum AttemptFailure {
    /// The local archive could not be read.
    Read(io::ErrorKind),
    /// The call failed, or a frame or the reply did not arrive in time.
    Transport(Box<Status>),
}

/// How the task that feeds one attempt's frames ended.
enum Feeding {
    /// Every frame reached the transport.
    Sent,
    /// The call ended before it took every frame, and its reply or status says why.
    CallEnded,
    /// A frame did not reach the transport within the frame timeout.
    Stalled,
    /// The local archive could not be read.
    ReadFailed(io::ErrorKind),
}

/// One attempt to stream a restore: its start frame, then the archive in chunks.
struct RestoreStreamAttempt {
    channel: Channel,
    start: EncodedFrame<RestoreFrame>,
    archive: File,
    frame_timeout: Duration,
}

impl RestoreStreamAttempt {
    /// Streams the attempt and waits for the one reply that answers it.
    async fn send(
        self,
        connector: &GrpcConnector,
        on_progress: impl Fn(u64) + Send + Sync + 'static,
    ) -> Result<Response<VerifiedFrame<RestoreReplyFrame>>, AttemptFailure> {
        let Self {
            channel,
            start,
            archive,
            frame_timeout,
        } = self;
        let (frames, outbound) = mpsc::channel(RESTORE_FRAME_CAPACITY);
        let feeder = nervix_primitives::task::spawn(feed(
            start,
            archive,
            frames,
            frame_timeout,
            on_progress,
        ));
        let mut client = tonic::client::Grpc::new(channel)
            .max_decoding_message_size(SESSION_LIMITS.frame_bytes())
            .max_encoding_message_size(SESSION_LIMITS.frame_bytes());
        if let Err(error) = client.ready().await {
            return Err(AttemptFailure::Transport(Box::new(Status::from_error(
                Box::new(error),
            ))));
        }
        let mut request = Request::new(ReceiverStream::new(outbound));
        connector.authorize(&mut request);
        let call = client.client_streaming(
            request,
            PathAndQuery::from_static(RESTORE_BACKUP_PATH),
            ClientRestoreCodec::new(SESSION_LIMITS),
        );
        tokio::pin!(call);
        tokio::pin!(feeder);
        let fed = nervix_primitives::select! {
            reply = &mut call => return reply.map_err(|status| AttemptFailure::Transport(Box::new(status))),
            fed = &mut feeder => fed,
        };
        let feeding = match fed {
            Ok(feeding) => feeding,
            Err(_) => Feeding::ReadFailed(io::ErrorKind::Interrupted),
        };
        match feeding {
            Feeding::Sent => match tokio::time::timeout(frame_timeout, call).await {
                Ok(reply) => reply.map_err(|status| AttemptFailure::Transport(Box::new(status))),
                Err(_) => Err(AttemptFailure::Transport(Box::new(
                    Status::deadline_exceeded(
                        "the restore reply did not arrive within the request timeout",
                    ),
                ))),
            },
            Feeding::CallEnded => call
                .await
                .map_err(|status| AttemptFailure::Transport(Box::new(status))),
            Feeding::Stalled => Err(AttemptFailure::Transport(Box::new(
                Status::deadline_exceeded(
                    "a restore frame did not reach the transport within the request timeout",
                ),
            ))),
            Feeding::ReadFailed(kind) => Err(AttemptFailure::Read(kind)),
        }
    }
}

/// Feeds the start frame and then the archive's chunks into `frames`, each within
/// `frame_timeout`.
async fn feed(
    start: EncodedFrame<RestoreFrame>,
    mut archive: File,
    frames: mpsc::Sender<EncodedFrame<RestoreFrame>>,
    frame_timeout: Duration,
    on_progress: impl Fn(u64) + Send + Sync + 'static,
) -> Feeding {
    match tokio::time::timeout(frame_timeout, frames.send(start)).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return Feeding::CallEnded,
        Err(_) => return Feeding::Stalled,
    }
    let mut buffer = vec![0_u8; RESTORE_CHUNK_BYTES];
    loop {
        nervix_primitives::task::consume_budget().await;
        let read = match archive.read(&mut buffer).await {
            Ok(read) => read,
            Err(error) => return Feeding::ReadFailed(error.kind()),
        };
        if read == 0 {
            return Feeding::Sent;
        }
        let chunk = RestoreChunk::encode(&buffer[..read], &SESSION_LIMITS).assured(
            "a non-empty chunk of at most half a frame, as a const assertion holds it, always \
             encodes",
        );
        match tokio::time::timeout(frame_timeout, frames.send(chunk)).await {
            Ok(Ok(())) => on_progress(read.arch_into()),
            Ok(Err(_)) => return Feeding::CallEnded,
            Err(_) => return Feeding::Stalled,
        }
    }
}
