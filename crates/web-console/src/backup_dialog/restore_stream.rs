//! Streaming a restore from the browser.
//!
//! A restore is the `RestoreBackup` call carried over a console WebSocket of its own: its start
//! names the restore's execution reference, its canonical statement and the archive's exact size
//! and BLAKE3 digest, and the archive follows in chunks read from the file the operator chose. A
//! WebSocket cannot half-close, so the stream ends with the chunk that completes the declared size,
//! and one reply answers it. The console reads the file once to measure and digest it before the
//! start, and again as it streams, holding one slice of it at a time.
//!
//! Every attempt sends the same reference, statement and archive, so a restore the leader already
//! admitted is joined or its outcome recovered, never applied twice. A redirect streams the restore
//! again to the leader, an unknown outcome streams it again after a pause, and a failure in
//! transport streams it again from its first byte. No attempt is bounded as a whole: the connection
//! must take each chunk within the frame timeout, and the reply must arrive within it once the
//! last chunk was sent. A reply that arrives as that timeout ends still counts.
//!
//! The leader admits a restore only once it holds the whole archive. An attempt that handed its
//! connection the archive's last byte and learned no outcome, or a reply that leaves the outcome
//! unknown, therefore leaves the restore possibly admitted, and only a reply that gives its
//! outcome settles that. Until one does, whatever ends the restore without its outcome leaves that
//! outcome unknown: an archive file the browser can no longer read, a console address the console
//! lacks, a reply it cannot read, a stream the leader refuses, or an execution reference the leader
//! no longer holds. The leader refuses some streams before it looks the restore up, and stages the
//! archive of a restore it admitted but does not hold again, so a refusal of a later stream says
//! nothing about an earlier attempt.

use std::{num::NonZeroU64, pin::pin, time::Duration};

use error_stack::Report;
use futures_util::future::{Either, select};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    CommandDisposition, CommandOutcome, LeaderRedirect, RequestId, RestoreChunk,
    RestoreDisposition, RestoreReply, RestoreStart, RestoreUploadFailure,
    websocket::{CONSOLE_RESTORE_PATH, ClientRestoreWebSocketCodec, WebSocketData},
};
use nervix_models::{ArchiveDigest, CommandExecutionReference, RestoreArchive};
use thiserror::Error;
use url::Url;

use super::browser::{ArchiveFile, Browser, CallSocket as _, Received};
use crate::{SESSION_LIMITS, web_console_websocket_url_from_base};

/// Archive bytes one restore frame carries, as the native clients send them.
pub(crate) const RESTORE_CHUNK_BYTES: usize = 256 * 1024;

// A chunk fills at most half a frame, far more room than its frame's envelope needs, so a
// non-empty chunk always encodes.
const _: () = assert!(RESTORE_CHUNK_BYTES <= SESSION_LIMITS.frame_bytes() / 2);

/// Archive bytes one read of the file digests at a time.
pub(crate) const MEASURE_SLICE_BYTES: u64 = 4 * 1024 * 1024;

/// Bytes the browser may hold queued on the connection before the stream waits for it to drain.
pub(crate) const MAX_BUFFERED_BYTES: u32 = 1024 * 1024;

/// How often a stream that waits for its connection to drain looks again.
pub(crate) const DRAIN_POLL: Duration = Duration::from_millis(10);

/// How long the connection may take to accept a chunk, and the reply to arrive once the last
/// chunk was sent.
pub(crate) const FRAME_TIMEOUT: Duration = Duration::from_secs(120);

/// How many times a stream looks at a connection that does not drain before it counts as stalled.
pub(crate) const MAX_DRAIN_POLLS: u128 = FRAME_TIMEOUT.as_millis() / DRAIN_POLL.as_millis();
const _: () = assert!(MAX_DRAIN_POLLS > 0);

/// How many times one restore is streamed again before the console gives up on learning its
/// outcome: about ten minutes of pauses, well inside the reference's retry validity.
pub(crate) const MAX_RESTORE_ATTEMPTS: usize = 600;

/// The pause before a restore is streamed again.
pub(crate) const RETRY_DELAY: Duration = Duration::from_secs(1);

/// The request identity of a restore's start. Each attempt is a stream of its own that exactly one
/// reply answers, so the identity only has to be non-zero.
const RESTORE_REQUEST_ID: RequestId = RequestId::new(NonZeroU64::MIN);

/// Why a restore's outcome was not learned.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum RestoreStreamError {
    #[error("the browser could not read the archive file")]
    ReadArchive,
    #[error("the archive file is empty")]
    EmptyArchive,
    #[error("the console has no address to stream the restore to")]
    NoAddress,
    #[error("the restore's connection failed")]
    Transport,
    #[error("the restore's connection took no bytes within {} seconds", FRAME_TIMEOUT.as_secs())]
    Stalled,
    #[error("no reply arrived within {} seconds of the archive's last byte", FRAME_TIMEOUT.as_secs())]
    NoReply,
    #[error("the restore's reply does not decode")]
    InvalidReply,
    #[error("the reply answers another request")]
    UnexpectedReply,
    #[error("the reply names execution reference '{received}', not the restore's '{expected}'")]
    ReferenceMismatch {
        expected: CommandExecutionReference,
        received: CommandExecutionReference,
    },
    #[error(
        "the restore was sent elsewhere, or its outcome stayed unknown, more times than the \
         console waits"
    )]
    Exhausted,
    /// The leader refused one stream of the restore before it ran anything from that stream. It
    /// is the cause beneath an unresolved admission: a refusal that ends the restore on its own is
    /// the restore's end, not a failure.
    #[error("the leader refused a stream of the restore ({failure:?}): {message}")]
    StreamRefused {
        failure: RestoreUploadFailure,
        message: String,
    },
    /// The leader no longer holds the restore's execution reference, so it can give no outcome
    /// under it. It is the cause beneath an unresolved admission: when no attempt can have had the
    /// restore admitted, the answer is the restore's outcome.
    #[error("the leader no longer holds the restore's execution reference: {message}")]
    ReferenceExpired { message: String },
    /// An attempt may have had the restore admitted, and the stream ended before any reply gave
    /// its outcome. What ended it is beneath.
    #[error(
        "the console stopped before learning the outcome of a restore the leader may have admitted"
    )]
    UnresolvedAdmission,
}

impl RestoreStreamError {
    /// Whether streaming the restore again may still learn its outcome.
    fn is_retryable(&self) -> bool {
        match self {
            Self::Transport | Self::Stalled | Self::NoReply => true,
            Self::ReadArchive
            | Self::EmptyArchive
            | Self::NoAddress
            | Self::InvalidReply
            | Self::UnexpectedReply
            | Self::ReferenceMismatch { .. }
            | Self::Exhausted
            | Self::StreamRefused { .. }
            | Self::ReferenceExpired { .. }
            | Self::UnresolvedAdmission => false,
        }
    }

    /// Whether the restore may have been admitted, so its outcome is unknown rather than absent.
    pub(crate) fn leaves_outcome_unknown(&self) -> bool {
        match self {
            Self::Transport
            | Self::Stalled
            | Self::NoReply
            | Self::Exhausted
            | Self::UnresolvedAdmission => true,
            Self::ReadArchive
            | Self::EmptyArchive
            | Self::NoAddress
            | Self::InvalidReply
            | Self::UnexpectedReply
            | Self::ReferenceMismatch { .. }
            | Self::StreamRefused { .. }
            | Self::ReferenceExpired { .. } => false,
        }
    }
}

/// How much of the archive one attempt handed to its connection before it ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Streamed {
    /// The attempt ended before it handed over the archive's last byte, so the leader cannot have
    /// admitted the restore from it.
    Partly,
    /// The attempt handed over every byte of the archive, so the leader may have admitted the
    /// restore from it.
    Whole,
}

/// How one attempt ended: the reply that answered it, or why none was learned, and how much of
/// the archive it handed over first.
struct AttemptEnd {
    streamed: Streamed,
    reply: Result<RestoreReply, Report<RestoreStreamError>>,
}

/// How handing one attempt's archive to its connection ended.
enum Sending<S> {
    /// The leader answered before the archive's last byte was handed over, and nothing more was
    /// sent.
    Answered(RestoreReply),
    /// The connection took the whole archive, and the reply is awaited on it.
    Sent(S),
}

/// Whether the leader may have admitted the restore on an attempt whose outcome the console has
/// not learned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    /// No attempt handed over the whole archive, and no reply said the restore's outcome is
    /// unknown, so the leader has not admitted the restore.
    Impossible,
    /// An attempt handed over the whole archive, or a reply said the restore's outcome is
    /// unknown, and no reply has given the outcome since: the leader may have admitted the
    /// restore, and only its outcome settles that.
    Possible,
}

impl Admission {
    /// What is known once an attempt that handed over `streamed` of the archive ended without the
    /// restore's outcome.
    fn after(self, streamed: Streamed) -> Self {
        match streamed {
            Streamed::Whole => Self::Possible,
            Streamed::Partly => self,
        }
    }

    /// The failure that ends the restore, which leaves its outcome unknown when the leader may
    /// have admitted it, whatever the failure says on its own.
    fn ending(self, failure: Report<RestoreStreamError>) -> Report<RestoreStreamError> {
        let Self::Possible = self else {
            return failure;
        };
        if failure.current_context().leaves_outcome_unknown() {
            return failure;
        }
        failure.change_context(RestoreStreamError::UnresolvedAdmission)
    }

    /// How the leader's refusal of the current attempt's stream ends the restore: it changed
    /// nothing when no attempt can have had the restore admitted, and otherwise leaves the
    /// outcome of the earlier admission unknown.
    fn refused(
        self,
        failure: RestoreUploadFailure,
        message: String,
    ) -> Result<RestoreEnd, Report<RestoreStreamError>> {
        match self {
            Self::Impossible => Ok(RestoreEnd::Refused { failure, message }),
            Self::Possible => {
                let refused = Report::new(RestoreStreamError::StreamRefused { failure, message });
                Err(refused.change_context(RestoreStreamError::UnresolvedAdmission))
            }
        }
    }

    /// How an answer that the leader no longer holds the restore's execution reference ends the
    /// restore: it is the restore's outcome when no attempt can have had the restore admitted,
    /// since the leader then admitted nothing under the reference, and otherwise leaves the outcome
    /// of the earlier admission unknown, which the leader can no longer give.
    fn expired(
        self,
        outcome: Box<CommandOutcome>,
    ) -> Result<RestoreEnd, Report<RestoreStreamError>> {
        match self {
            Self::Impossible => Ok(RestoreEnd::Outcome(outcome)),
            Self::Possible => {
                let expired = Report::new(RestoreStreamError::ReferenceExpired {
                    message: outcome.message,
                });
                Err(expired.change_context(RestoreStreamError::UnresolvedAdmission))
            }
        }
    }
}

/// How a streamed restore ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RestoreEnd {
    /// The restore's outcome, as the command it is.
    Outcome(Box<CommandOutcome>),
    /// The stream was refused before the restore ran, which changed nothing.
    Refused {
        failure: RestoreUploadFailure,
        message: String,
    },
}

/// What a reply tells the console to do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplyStep {
    /// The restore ended.
    End(RestoreEnd),
    /// The node is not the leader: stream the restore again to the leader's console.
    Redirect(Url),
    /// No leader is known, or it advertises no console: stream the restore again after a pause.
    AwaitElection,
    /// The restore's outcome is not known yet, so the leader may have admitted it: stream it again
    /// after a pause.
    AwaitOutcome,
    /// The leader no longer holds the restore's execution reference: the restore ends, with this
    /// answer as its outcome unless an earlier attempt may have had it admitted.
    ReferenceExpired(Box<CommandOutcome>),
}

/// Reads the reply to the restore under `reference`.
pub(crate) fn reply_step(
    reply: RestoreReply,
    reference: &CommandExecutionReference,
) -> Result<ReplyStep, Report<RestoreStreamError>> {
    if let Some(request_id) = reply.request_id
        && request_id != RESTORE_REQUEST_ID
    {
        return Err(Report::new(RestoreStreamError::UnexpectedReply));
    }
    let outcome = match reply.disposition {
        RestoreDisposition::Outcome(outcome) => outcome,
        RestoreDisposition::UploadFailed { failure, message } => {
            return Ok(ReplyStep::End(RestoreEnd::Refused { failure, message }));
        }
    };
    if outcome.execution_reference != *reference {
        return Err(Report::new(RestoreStreamError::ReferenceMismatch {
            expected: reference.clone(),
            received: outcome.execution_reference.clone(),
        }));
    }
    match &outcome.disposition {
        CommandDisposition::NotLeader(redirect) => match redirect_console(redirect) {
            Some(console) => Ok(ReplyStep::Redirect(console)),
            None => Ok(ReplyStep::AwaitElection),
        },
        CommandDisposition::OutcomeUnknown(_) => Ok(ReplyStep::AwaitOutcome),
        CommandDisposition::ExecutionReferenceExpired => Ok(ReplyStep::ReferenceExpired(outcome)),
        CommandDisposition::Completed { .. }
        | CommandDisposition::Failed
        | CommandDisposition::TransactionDetached { .. }
        | CommandDisposition::TransactionTakenOver { .. }
        | CommandDisposition::ExecutionReferenceConflict(_)
        | CommandDisposition::PreviewStale { .. } => {
            Ok(ReplyStep::End(RestoreEnd::Outcome(outcome)))
        }
    }
}

/// The console of the leader a redirect names, when it advertises one.
fn redirect_console(redirect: &LeaderRedirect) -> Option<Url> {
    let leader = redirect.leader.as_ref()?;
    leader.web_console_uri.clone()
}

/// Reads the whole archive once to measure and digest it, telling `progress` every count of bytes
/// read and the file's size.
pub(crate) async fn measure_archive<F: ArchiveFile>(
    file: &F,
    progress: impl Fn(u64, u64),
) -> Result<RestoreArchive, Report<RestoreStreamError>> {
    let Some(size) = file.size() else {
        return Err(Report::new(RestoreStreamError::ReadArchive));
    };
    let Some(total_bytes) = NonZeroU64::new(size) else {
        return Err(Report::new(RestoreStreamError::EmptyArchive));
    };
    let mut hasher = blake3::Hasher::new();
    let mut read = 0_u64;
    while read < size {
        let remaining = size
            .checked_sub(read)
            .verified("the loop reads only while bytes of the file remain");
        let end = read
            .checked_add(remaining.min(MEASURE_SLICE_BYTES))
            .verified("a slice ends within the file's size, which is a u64");
        let Some(bytes) = file.read(read, end).await else {
            return Err(Report::new(RestoreStreamError::ReadArchive));
        };
        if bytes.is_empty() {
            return Err(Report::new(RestoreStreamError::ReadArchive));
        }
        hasher.update(&bytes);
        let length = u64::try_from(bytes.len())
            .assured("a slice held in memory is shorter than u64 counts on every target");
        read = read
            .checked_add(length)
            .verified("the bytes read stay within the file's size, which is a u64");
        progress(read, size);
    }
    Ok(RestoreArchive {
        total_bytes,
        digest: ArchiveDigest::from_bytes(*hasher.finalize().as_bytes()),
    })
}

/// One restore the console streams until it learns the outcome.
pub(crate) struct RestoreUpload<F> {
    pub(crate) reference: CommandExecutionReference,
    /// The `RESTORE` statement as canonical Plexus.
    pub(crate) statement: String,
    pub(crate) archive: RestoreArchive,
    pub(crate) file: F,
    /// The console the restore is streamed to: the leader's, as the session follows it.
    pub(crate) base_url: Option<String>,
    pub(crate) auth_token: String,
}

impl<F: ArchiveFile> RestoreUpload<F> {
    /// Streams the restore from `browser` until a reply ends it, telling `progress` every count of
    /// bytes sent on the current attempt and the archive's size.
    pub(crate) async fn run<B: Browser>(
        mut self,
        browser: &B,
        progress: impl Fn(u64, u64) + Copy,
    ) -> Result<RestoreEnd, Report<RestoreStreamError>> {
        let mut admission = Admission::Impossible;
        let mut last_failure = Report::new(RestoreStreamError::Exhausted);
        for _ in 0..MAX_RESTORE_ATTEMPTS {
            progress(0, self.archive.total_bytes.get());
            let attempt = self.attempt(browser, progress).await;
            let reply = match attempt.reply {
                Ok(reply) => reply,
                Err(failure) => {
                    admission = admission.after(attempt.streamed);
                    if !failure.current_context().is_retryable() {
                        return Err(admission.ending(failure));
                    }
                    last_failure = failure;
                    browser.wait(RETRY_DELAY).await;
                    continue;
                }
            };
            let step = match reply_step(reply, &self.reference) {
                Ok(step) => step,
                Err(failure) => return Err(admission.after(attempt.streamed).ending(failure)),
            };
            match step {
                // The leader refused this attempt's stream before it ran anything from it, which
                // settles nothing an earlier attempt may have had admitted.
                ReplyStep::End(RestoreEnd::Refused { failure, message }) => {
                    return admission.refused(failure, message);
                }
                ReplyStep::End(end) => return Ok(end),
                ReplyStep::Redirect(console) => {
                    // A leader that loses its leadership while the restore applies redirects too.
                    admission = admission.after(attempt.streamed);
                    self.base_url = Some(console.to_string());
                    last_failure = Report::new(RestoreStreamError::Exhausted);
                }
                ReplyStep::AwaitElection => {
                    admission = admission.after(attempt.streamed);
                    last_failure = Report::new(RestoreStreamError::Exhausted);
                    browser.wait(RETRY_DELAY).await;
                }
                ReplyStep::AwaitOutcome => {
                    admission = Admission::Possible;
                    last_failure = Report::new(RestoreStreamError::Exhausted);
                    browser.wait(RETRY_DELAY).await;
                }
                // The leader admits nothing under a reference it no longer holds, so only an
                // earlier attempt can have had the restore admitted.
                ReplyStep::ReferenceExpired(outcome) => return admission.expired(outcome),
            }
        }
        Err(admission.ending(last_failure))
    }

    /// Streams the restore once and waits for the reply that answers it.
    async fn attempt<B: Browser>(&self, browser: &B, progress: impl Fn(u64, u64)) -> AttemptEnd {
        let codec = ClientRestoreWebSocketCodec::new(SESSION_LIMITS);
        let sending = self.send_archive(browser, &codec, progress).await;
        let mut socket = match sending {
            Ok(Sending::Sent(socket)) => socket,
            Ok(Sending::Answered(reply)) => {
                return AttemptEnd {
                    streamed: Streamed::Partly,
                    reply: Ok(reply),
                };
            }
            Err(failure) => {
                return AttemptEnd {
                    streamed: Streamed::Partly,
                    reply: Err(failure),
                };
            }
        };
        let reply = next_reply(browser, &mut socket, &codec).await;
        AttemptEnd {
            streamed: Streamed::Whole,
            reply,
        }
    }

    /// Opens one attempt's connection and hands it the start and every chunk of the archive,
    /// unless the leader answers first.
    async fn send_archive<B: Browser>(
        &self,
        browser: &B,
        codec: &ClientRestoreWebSocketCodec,
        progress: impl Fn(u64, u64),
    ) -> Result<Sending<B::Socket>, Report<RestoreStreamError>> {
        let Some(url) = self.socket_url() else {
            return Err(Report::new(RestoreStreamError::NoAddress));
        };
        let start = RestoreStart {
            request_id: RESTORE_REQUEST_ID,
            execution_reference: self.reference.clone(),
            statement: self.statement.clone(),
            archive: self.archive,
        };
        // The statement came from the same parser and renderer whose limits bound a frame.
        let start = start
            .encode(&SESSION_LIMITS)
            .assured("a restore statement is far shorter than a frame");
        let Some(mut socket) = browser.open(&url) else {
            return Err(Report::new(RestoreStreamError::Transport));
        };
        if socket.send(Vec::from(codec.encode(start))).await.is_err() {
            return Err(Report::new(RestoreStreamError::Transport));
        }
        let total = self.archive.total_bytes.get();
        let chunk_bytes = u64::try_from(RESTORE_CHUNK_BYTES)
            .assured("a chunk of a quarter mebibyte is counted in u64 on every target");
        let mut sent = 0_u64;
        while sent < total {
            // The leader may answer before every byte arrived, as when it refuses the restore or
            // recovers the outcome of one it already ran; nothing more is sent then.
            if let Some(reply) = wait_for_room(browser, &mut socket, codec).await? {
                return Ok(Sending::Answered(reply));
            }
            let remaining = total
                .checked_sub(sent)
                .verified("the loop streams only while declared bytes remain");
            let end = sent
                .checked_add(remaining.min(chunk_bytes))
                .verified("a chunk ends within the declared size, which is a u64");
            let Some(bytes) = self.file.read(sent, end).await else {
                return Err(Report::new(RestoreStreamError::ReadArchive));
            };
            if bytes.is_empty() {
                return Err(Report::new(RestoreStreamError::ReadArchive));
            }
            let chunk = RestoreChunk::encode(&bytes, &SESSION_LIMITS).assured(
                "a non-empty chunk of at most half a frame, as a const assertion holds it, always \
                 encodes",
            );
            if socket.send(Vec::from(codec.encode(chunk))).await.is_err() {
                return Err(Report::new(RestoreStreamError::Transport));
            }
            let length = u64::try_from(bytes.len())
                .assured("a chunk held in memory is shorter than u64 counts on every target");
            sent = sent
                .checked_add(length)
                .verified("the bytes sent stay within the declared size, which is a u64");
            progress(sent, total);
        }
        Ok(Sending::Sent(socket))
    }

    fn socket_url(&self) -> Option<String> {
        let base = self.base_url.as_deref()?;
        let base = Url::parse(base).ok()?;
        web_console_websocket_url_from_base(&base, CONSOLE_RESTORE_PATH, &self.auth_token)
    }
}

/// Waits until the connection has room for another chunk, and returns the reply when it arrives
/// first.
async fn wait_for_room<B: Browser>(
    browser: &B,
    socket: &mut B::Socket,
    codec: &ClientRestoreWebSocketCodec,
) -> Result<Option<RestoreReply>, Report<RestoreStreamError>> {
    let mut polls = 0_u128;
    loop {
        if let Some(received) = socket.try_receive() {
            let reply = reply_of(received, codec)?;
            return Ok(Some(reply));
        }
        if socket.buffered_amount() <= MAX_BUFFERED_BYTES {
            return Ok(None);
        }
        if polls >= MAX_DRAIN_POLLS {
            return Err(Report::new(RestoreStreamError::Stalled));
        }
        browser.wait(DRAIN_POLL).await;
        polls = polls
            .checked_add(1)
            .verified("the polls stop at MAX_DRAIN_POLLS, far below u128::MAX");
    }
}

/// The reply once the last chunk was sent, which must arrive within the frame timeout.
async fn next_reply<B: Browser>(
    browser: &B,
    socket: &mut B::Socket,
    codec: &ClientRestoreWebSocketCodec,
) -> Result<RestoreReply, Report<RestoreStreamError>> {
    let received = pin!(socket.receive());
    let timeout = pin!(browser.wait(FRAME_TIMEOUT));
    let received = match select(received, timeout).await {
        Either::Left((received, _)) => received,
        Either::Right(((), _)) => return Err(Report::new(RestoreStreamError::NoReply)),
    };
    reply_of(received, codec)
}

/// The reply a received message carries.
fn reply_of(
    received: Received,
    codec: &ClientRestoreWebSocketCodec,
) -> Result<RestoreReply, Report<RestoreStreamError>> {
    let data = match received {
        Received::Binary(payload) => WebSocketData::Binary(payload.into()),
        Received::Text => WebSocketData::Text,
        // A connection that ends before its reply leaves the restore's outcome unknown.
        Received::Ended => return Err(Report::new(RestoreStreamError::Transport)),
    };
    let frame = match codec.decode(data) {
        Ok(frame) => frame,
        Err(error) => return Err(error.change_context(RestoreStreamError::InvalidReply)),
    };
    match RestoreReply::decode(&frame) {
        Ok(reply) => Ok(reply),
        Err(error) => Err(error.change_context(RestoreStreamError::InvalidReply)),
    }
}
