//! Restores: the archive a local file holds, and the stream that carries it to the leader.
//!
//! Layer: edges.
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

mod handover;

#[cfg(all(test, feature = "shuttle"))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "restore/shuttle_tests.rs"]
mod shuttle_tests;

use std::{
    io::{self, Read as _},
    num::NonZeroU64,
    path::{Path, PathBuf},
    time::Duration,
};

use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use handover::RestoreHandover;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    CommandDisposition, EncodedFrame, OutcomeOrigin, RequestId, RestoreChunk, RestoreDisposition,
    RestoreFrame, RestoreReply, RestoreReplyFrame, RestoreStart, VerifiedFrame,
    grpc::{ClientRestoreCodec, FrameRejection, RESTORE_BACKUP_PATH},
};
use nervix_models::{ArchiveDigest, CommandExecutionReference, Restore, RestoreArchive};
use nervix_primitives::{
    stream::{StreamExt as _, wrappers::ReceiverStream},
    sync::{Arc, mpsc},
};
use nervix_recovery::Reported as _;
use tokio::{fs::File, io::AsyncReadExt as _};
use tonic::{Request, Response, Status, codegen::http::uri::PathAndQuery, transport::Channel};

use crate::{
    client::{Client, RecoveryDeadline, RecoveryMode, SessionRecovery},
    connection::GrpcConnector,
    error::{ClientError, RequestKind},
    exchange::{ExchangeEnd, SESSION_LIMITS},
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

/// Knowledge retained by one restore call while it repeats its stream.
#[derive(Clone, Copy)]
enum RestoreAttemptHistory {
    NoUncertainAttempt,
    Uncertain,
}

impl RestoreAttemptHistory {
    fn is_uncertain(self) -> bool {
        matches!(self, Self::Uncertain)
    }

    fn observe_attempt(&mut self, attempt: &RestoreAttemptResult) {
        if !attempt.archive_sent {
            return;
        }
        match &attempt.result {
            Err(AttemptFailure::Read(_)) => *self = Self::Uncertain,
            Err(AttemptFailure::Transport(status)) if upload_status_is_retryable(status) => {
                *self = Self::Uncertain;
            }
            _ => {}
        }
    }

    fn observe_outcome(&mut self, outcome: &CommandOutcome, archive_sent: bool) {
        match outcome.disposition {
            CommandDisposition::OutcomeUnknown(_) => *self = Self::Uncertain,
            CommandDisposition::NotLeader(_) if archive_sent => *self = Self::Uncertain,
            _ => {}
        }
    }

    fn cannot_settle(self, outcome: &CommandOutcome) -> bool {
        if !self.is_uncertain() {
            return false;
        }
        match outcome.disposition {
            CommandDisposition::ExecutionReferenceExpired => true,
            CommandDisposition::Failed => {
                outcome.origin != Some(OutcomeOrigin::Recovered) && outcome.restore.is_none()
            }
            _ => false,
        }
    }

    fn finish(
        self,
        result: error_stack::Result<CommandOutcome, ClientError>,
        reference: &CommandExecutionReference,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        if !self.is_uncertain() {
            return result;
        }
        match result {
            Ok(outcome) => Ok(outcome),
            Err(report) => {
                if let ClientError::UncertainCommand { .. } = report.current_context() {
                    Err(report)
                } else {
                    Err(report.change_context(ClientError::UncertainCommand {
                        reference: reference.clone(),
                    }))
                }
            }
        }
    }
}

impl Client {
    /// Restores from the archive the local file `restore.source` names, under a new execution
    /// reference. `on_progress` is told each number of archive bytes handed to the transport; a
    /// retried stream counts its bytes again.
    pub async fn restore(
        &self,
        restore: &Restore,
        on_progress: impl Fn(u64) + Send + Sync + Clone + 'static,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
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
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        match self.run_restore(restore, reference, on_progress).await {
            Ok(outcome) => Ok(outcome),
            Err(report) if report.current_context().can_hide_admitted_work() => Err(report
                .change_context(ClientError::UncertainCommand {
                    reference: reference.clone(),
                })),
            Err(report) => Err(report),
        }
    }

    pub(crate) async fn run_restore(
        &self,
        restore: &Restore,
        reference: &CommandExecutionReference,
        on_progress: impl Fn(u64) + Send + Sync + Clone + 'static,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let mut history = RestoreAttemptHistory::NoUncertainAttempt;
        let result = self
            .run_restore_attempts(restore, reference, on_progress, &mut history)
            .await;
        history.finish(result, reference)
    }

    async fn run_restore_attempts(
        &self,
        restore: &Restore,
        reference: &CommandExecutionReference,
        on_progress: impl Fn(u64) + Send + Sync + Clone + 'static,
        history: &mut RestoreAttemptHistory,
    ) -> error_stack::Result<CommandOutcome, ClientError> {
        let path = expand_user_path(Path::new(&restore.source));
        let measured = match measure_archive(path.clone()).await {
            Ok(measured) => measured,
            Err(error) => {
                let kind = error.kind();
                return Err(Report::new(error)
                    .change_context(ClientError::ReadRestoreArchive { path, kind }));
            }
        };
        let Some(total_bytes) = NonZeroU64::new(measured.length) else {
            return Err(Report::new(ClientError::EmptyRestoreArchive { path }));
        };
        let archive = RestoreArchive {
            total_bytes,
            digest: ArchiveDigest::from_bytes(measured.digest),
        };
        let statement = restore.to_canonical_plexus();
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
            .change_context(ClientError::EncodeRequest {
                request: RequestKind::Restore,
            })?;
            let archive_file = match File::open(&path).await {
                Ok(archive_file) => archive_file,
                Err(error) => {
                    let kind = error.kind();
                    return Err(Report::new(error).change_context(
                        ClientError::ReadRestoreArchive {
                            path: path.clone(),
                            kind,
                        },
                    ));
                }
            };
            let (channel, generation) = {
                let exchange = self.inner.exchange.lock().await;
                (exchange.channel.clone(), exchange.generation.clone())
            };
            let sent = RestoreStreamAttempt {
                channel,
                start,
                archive: archive_file,
                archive_bytes: total_bytes,
                frame_timeout,
            }
            .send(&self.inner.connector, on_progress.clone())
            .await;
            history.observe_attempt(&sent);
            let response = match sent.result {
                Ok(response) => response,
                Err(AttemptFailure::Read(error)) => {
                    let kind = error.kind();
                    return Err(Report::new(error).change_context(
                        ClientError::ReadRestoreArchive {
                            path: path.clone(),
                            kind,
                        },
                    ));
                }
                Err(AttemptFailure::Transport(status))
                    if upload_status_is_retryable(&status) && Self::await_retry(attempt).await =>
                {
                    match self
                        .recover_session(RecoveryMode::Replace, RecoveryDeadline::RetryBudget)
                        .await?
                    {
                        SessionRecovery::Ready => continue,
                        SessionRecovery::Unavailable => {
                            return Err(Report::new(ClientError::Restore(status)));
                        }
                    }
                }
                Err(AttemptFailure::Transport(status)) => {
                    return Err(Report::new(ClientError::Restore(status)));
                }
                Err(AttemptFailure::Reply(status)) => {
                    let mut exchange = self.inner.exchange.lock().await;
                    if Arc::ptr_eq(&exchange.generation, &generation) {
                        exchange.interrupt(ExchangeEnd::Closed).await;
                    }
                    return Err(Report::new(*status)
                        .change_context(ClientError::InvalidRestoreReply)
                        .change_context(ClientError::UncertainCommand {
                            reference: reference.clone(),
                        }));
                }
            };
            let reply = self
                .validate_restore_reply(response.get_ref(), &generation, reference)
                .await?;
            let outcome = match reply.disposition {
                RestoreDisposition::Outcome(outcome) => {
                    if outcome.execution_reference != *reference {
                        let mut exchange = self.inner.exchange.lock().await;
                        if Arc::ptr_eq(&exchange.generation, &generation) {
                            exchange
                                .interrupt(ExchangeEnd::ExecutionMismatch {
                                    expected: reference.clone(),
                                    received: outcome.execution_reference.clone(),
                                })
                                .await;
                        }
                        return Err(Report::new(ClientError::ExecutionReferenceMismatch {
                            expected: reference.clone(),
                            received: outcome.execution_reference,
                        }));
                    }
                    CommandOutcome::from(*outcome)
                }
                RestoreDisposition::UploadFailed { failure, message } => {
                    if history.is_uncertain() {
                        return Err(Report::new(ClientError::RestoreStreamRefused {
                            failure,
                            message,
                        }));
                    }
                    CommandOutcome::restore_refused(reference.clone(), failure, message)
                }
            };
            if history.cannot_settle(&outcome) {
                return Err(Report::new(ClientError::LastUnresolvedAnswer {
                    disposition: outcome.disposition,
                    message: outcome.message,
                }));
            }
            history.observe_outcome(&outcome, sent.archive_sent);
            match outcome.routing() {
                Routing::Redirect(leader) => {
                    self.follow_leader(leader, RecoveryDeadline::RetryBudget)
                        .await?
                }
                Routing::AwaitElection | Routing::AwaitOutcome
                    if Self::await_retry(attempt).await => {}
                Routing::AwaitElection if history.is_uncertain() => {
                    return Err(Report::new(ClientError::RetryDeadline));
                }
                _ => return Ok(outcome),
            }
        }
        if history.is_uncertain() {
            return Err(Report::new(ClientError::RetryDeadline));
        }
        let mut exhausted =
            CommandOutcome::failed_locally("restore redirect loop exceeded".to_string());
        exhausted.execution_reference = Some(reference.clone());
        Ok(exhausted)
    }

    /// An invalid reply cannot establish whether the restore was applied. End its session,
    /// preserving a replacement installed while the archive was streaming.
    async fn validate_restore_reply(
        &self,
        frame: &VerifiedFrame<RestoreReplyFrame>,
        generation: &Arc<()>,
        reference: &CommandExecutionReference,
    ) -> error_stack::Result<RestoreReply, ClientError> {
        let validated = RestoreReply::decode(frame)
            .change_context(ClientError::InvalidRestoreReply)
            .and_then(|reply| {
                if reply
                    .request_id
                    .is_some_and(|request_id| request_id != RESTORE_REQUEST_ID)
                {
                    Err(Report::new(ClientError::UnexpectedReply {
                        request: RequestKind::Restore,
                    }))
                } else {
                    Ok(reply)
                }
            });
        match validated {
            Ok(reply) => Ok(reply),
            Err(report) => {
                let mut exchange = self.inner.exchange.lock().await;
                if Arc::ptr_eq(&exchange.generation, generation) {
                    exchange.interrupt(ExchangeEnd::Closed).await;
                }
                Err(report.change_context(ClientError::UncertainCommand {
                    reference: reference.clone(),
                }))
            }
        }
    }
}

/// Why one attempt to stream a restore ended without a reply.
enum AttemptFailure {
    /// The local archive could not be read.
    Read(io::Error),
    /// The call failed, or a frame or the reply did not arrive in time.
    Transport(Box<Status>),
    /// The response began, but its reply could not establish the restore's outcome.
    Reply(Box<Status>),
}

impl AttemptFailure {
    /// Preserve retryable transport failures and known authentication refusals even after headers.
    /// Other failures reading an admitted response cannot establish a definitive restore refusal.
    fn reading_reply(status: Status) -> Self {
        if FrameRejection::find_in(&status).is_none()
            && (upload_status_is_retryable(&status)
                || matches!(
                    status.code(),
                    tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
                ))
        {
            Self::Transport(Box::new(status))
        } else {
            Self::Reply(Box::new(status))
        }
    }
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
    ReadFailed(io::Error),
}

/// One queued frame, with the final declared chunk's transport handover observation.
struct RestoreQueuedFrame {
    frame: EncodedFrame<RestoreFrame>,
    completion: Option<Arc<RestoreHandover>>,
}

impl RestoreQueuedFrame {
    fn can_handover(&self) -> bool {
        self.completion
            .as_ref()
            .is_none_or(|completion| completion.try_handover())
    }

    fn into_frame(self) -> EncodedFrame<RestoreFrame> {
        self.frame
    }
}

/// One stream's result and whether its complete declared archive was handed to the transport.
struct RestoreAttemptResult {
    result: Result<Response<VerifiedFrame<RestoreReplyFrame>>, AttemptFailure>,
    archive_sent: bool,
}

/// One attempt to stream a restore: its start frame, then the archive in chunks.
struct RestoreStreamAttempt {
    channel: Channel,
    start: EncodedFrame<RestoreFrame>,
    archive: File,
    archive_bytes: NonZeroU64,
    frame_timeout: Duration,
}

impl RestoreStreamAttempt {
    /// Streams the attempt and waits for the one reply that answers it.
    async fn send(
        self,
        connector: &GrpcConnector,
        on_progress: impl Fn(u64) + Send + Sync + 'static,
    ) -> RestoreAttemptResult {
        let archive_sent = Arc::new(RestoreHandover::new());
        let result = self
            .send_with_progress(connector, on_progress, archive_sent.clone())
            .await;
        RestoreAttemptResult {
            result,
            archive_sent: archive_sent.finish(),
        }
    }

    async fn send_with_progress(
        self,
        connector: &GrpcConnector,
        on_progress: impl Fn(u64) + Send + Sync + 'static,
        archive_sent: Arc<RestoreHandover>,
    ) -> Result<Response<VerifiedFrame<RestoreReplyFrame>>, AttemptFailure> {
        let Self {
            channel,
            start,
            archive,
            archive_bytes,
            frame_timeout,
        } = self;
        let (frames, outbound) = mpsc::channel(RESTORE_FRAME_CAPACITY);
        let feeder = nervix_primitives::task::spawn(feed(
            start,
            archive,
            frames,
            frame_timeout,
            on_progress,
            archive_bytes,
            archive_sent,
        ));
        let mut client = tonic::client::Grpc::new(channel)
            .max_decoding_message_size(SESSION_LIMITS.frame_bytes())
            .max_encoding_message_size(SESSION_LIMITS.frame_bytes());
        if let Err(error) = client.ready().await {
            return Err(AttemptFailure::Transport(Box::new(Status::from_error(
                Box::new(error),
            ))));
        }
        let mut request = Request::new(
            ReceiverStream::new(outbound)
                .take_while(RestoreQueuedFrame::can_handover)
                .map(RestoreQueuedFrame::into_frame),
        );
        connector.authorize(&mut request);
        // Retain tonic's client-streaming response semantics, but keep the response-read phase
        // visible: its local decoder and bounded receiver failures are not remote refusals.
        let call = async {
            let (metadata, mut body, extensions) = client
                .streaming(
                    request,
                    PathAndQuery::from_static(RESTORE_BACKUP_PATH),
                    ClientRestoreCodec::new(SESSION_LIMITS),
                )
                .await
                .map_err(|status| AttemptFailure::Transport(Box::new(status)))?
                .into_parts();
            let mut headers = metadata.into_headers();
            let message = match body.message().await {
                Ok(Some(message)) => message,
                Ok(None) => {
                    return Err(AttemptFailure::reading_reply(Status::internal(
                        "Missing response message.",
                    )));
                }
                Err(mut status) => {
                    let mut failure_headers = std::mem::take(status.metadata_mut()).into_headers();
                    failure_headers.extend(headers.clone());
                    *status.metadata_mut() =
                        tonic::metadata::MetadataMap::from_headers(failure_headers);
                    return Err(AttemptFailure::reading_reply(status));
                }
            };
            if let Some(trailers) = body
                .trailers()
                .await
                .map_err(AttemptFailure::reading_reply)?
            {
                headers.extend(trailers.into_headers());
            }
            Ok(Response::from_parts(
                tonic::metadata::MetadataMap::from_headers(headers),
                message,
                extensions,
            ))
        };
        tokio::pin!(call);
        tokio::pin!(feeder);
        let fed = nervix_primitives::select! {
            reply = &mut call => return reply,
            fed = &mut feeder => fed,
        };
        let feeding = match fed {
            Ok(feeding) => feeding,
            Err(error) => {
                let error = io::Error::new(io::ErrorKind::Interrupted, error);
                Err::<(), _>(&error).reported("stream local restore archive");
                Feeding::ReadFailed(error)
            }
        };
        match feeding {
            Feeding::Sent => match nervix_primitives::time::timeout(frame_timeout, call).await {
                Ok(reply) => reply,
                Err(_) => Err(AttemptFailure::Transport(Box::new(
                    Status::deadline_exceeded(
                        "the restore reply did not arrive within the request timeout",
                    ),
                ))),
            },
            Feeding::CallEnded => call.await,
            Feeding::Stalled => Err(AttemptFailure::Transport(Box::new(
                Status::deadline_exceeded(
                    "a restore frame did not reach the transport within the request timeout",
                ),
            ))),
            Feeding::ReadFailed(error) => Err(AttemptFailure::Read(error)),
        }
    }
}

/// Feeds the start frame and then the archive's chunks into `frames`, each within
/// `frame_timeout`.
async fn feed(
    start: EncodedFrame<RestoreFrame>,
    mut archive: File,
    frames: mpsc::Sender<RestoreQueuedFrame>,
    frame_timeout: Duration,
    on_progress: impl Fn(u64) + Send + Sync + 'static,
    archive_bytes: NonZeroU64,
    archive_sent: Arc<RestoreHandover>,
) -> Feeding {
    let start = RestoreQueuedFrame {
        frame: start,
        completion: None,
    };
    match nervix_primitives::time::timeout(frame_timeout, frames.send(start)).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return Feeding::CallEnded,
        Err(_) => return Feeding::Stalled,
    }
    let mut buffer = vec![0_u8; RESTORE_CHUNK_BYTES];
    let mut remaining = archive_bytes.get();
    let mut completion = Some(archive_sent);
    loop {
        nervix_primitives::task::consume_budget().await;
        let read = match archive.read(&mut buffer).await {
            Ok(read) => read,
            Err(error) => return Feeding::ReadFailed(error),
        };
        if read == 0 {
            return Feeding::Sent;
        }
        let chunk = RestoreChunk::encode(&buffer[..read], &SESSION_LIMITS).assured(
            "a non-empty chunk of at most half a frame, as a const assertion holds it, always \
             encodes",
        );
        let supplied: u64 = read.arch_into();
        let final_chunk = if remaining == 0 {
            None
        } else if supplied >= remaining {
            // Extra bytes from a file that grew cannot erase knowledge that all declared
            // bytes might have reached the transport. Only its first such chunk owns the flag.
            remaining = 0;
            completion.take()
        } else {
            remaining = remaining
                .checked_sub(supplied)
                .verified("the chunk is smaller than the remaining declared archive bytes");
            None
        };
        let chunk = RestoreQueuedFrame {
            frame: chunk,
            completion: final_chunk,
        };
        match nervix_primitives::time::timeout(frame_timeout, frames.send(chunk)).await {
            Ok(Ok(())) => on_progress(supplied),
            Ok(Err(_)) => return Feeding::CallEnded,
            Err(_) => return Feeding::Stalled,
        }
    }
}
