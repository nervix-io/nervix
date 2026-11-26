//! Streaming a restore from the browser.
//!
//! Layer: edges.
//! - **Owns.** Browser restore attempts, bounded archive streaming and outcome classification.
//! - **Depends on.** The browser boundary, typed restore frames and vocabulary identities.
//! - **Must not know.** Server admission, execution or persistence implementation.
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
//! transport streams it again from its first byte, until the reference's retry validity ends: its
//! UUIDv7 issue time on the browser's clock plus the validity the cluster committed, after which
//! the leader holds no restore under it. No attempt is bounded as a whole: the connection must take
//! each chunk within the request timeout of the node the attempt reached, and the reply must arrive
//! within it once the last chunk was sent. A reply that arrives as that timeout ends still counts.
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
    CommandDisposition, CommandOutcome, LeaderRedirect, OutcomeOrigin, RequestId, RestoreChunk,
    RestoreDisposition, RestoreReply, RestoreStart, RestoreUploadFailure, SessionLimits,
    websocket::{CONSOLE_RESTORE_PATH, ClientRestoreWebSocketCodec, WebSocketData},
};
use nervix_models::{ArchiveDigest, CommandExecutionReference, RestoreArchive, Timestamp};
use thiserror::Error;
use url::Url;

use super::{
    browser::{ArchiveFile, Browser, CallSocket as _, Received},
    redirect_chain::{NextAttempt, RedirectChain},
};
use crate::{pacing::ConsolePacing, web_console_websocket_url_from_base};

/// Archive bytes one read of the file digests at a time.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the web console chapter's bounded buffers read at most 4 MiB of an archive at a \
                  time while a restore digests it"
    )
)]
pub(crate) const MEASURE_SLICE_BYTES: u64 = 4 * 1024 * 1024;

/// How often a stream that waits for its connection to drain looks again. The browser reports how
/// many bytes a WebSocket holds queued only when asked and signals no drain, so a stream that
/// waits for room asks again at this period, for at most the request timeout.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the web console chapter looks at a restore connection's queued bytes every 10 \
                  milliseconds while the stream waits for room, because the browser's WebSocket \
                  reports them only when asked"
    )
)]
pub(crate) const DRAIN_POLL: Duration = Duration::from_millis(10);

/// How many times a stream looks at a connection that does not drain before it counts as
/// stalled: as many drain polls as the request timeout holds, and at least one.
pub(crate) fn drain_polls(request_timeout: Duration) -> u128 {
    let polls = request_timeout
        .as_nanos()
        .checked_div(DRAIN_POLL.as_nanos())
        .assured("the drain poll is a positive duration");
    polls.max(1)
}

/// The bytes the browser may hold queued on a restore's connection before the stream waits for it
/// to drain: the announced restore frame capacity at the chunk size `limits` allow.
pub(crate) fn queued_bytes_limit(frame_capacity: NonZeroU64, limits: &SessionLimits) -> u64 {
    let chunk = u64::try_from(RestoreChunk::largest_bytes(limits))
        .assured("a chunk of at most a quarter mebibyte is counted in u64 on every target");
    frame_capacity.get().checked_mul(chunk).assured(
        "the frame capacity is validated so its largest chunks fit the browser's 32-bit queue \
         count",
    )
}

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
    #[error("the restore's connection took no bytes within {}", nervix_models::timing_text(*.timeout))]
    Stalled { timeout: Duration },
    #[error(
        "no reply arrived within {} of the archive's last byte",
        nervix_models::timing_text(*.timeout)
    )]
    NoReply { timeout: Duration },
    #[error("the restore request does not fit the peer session limits")]
    InvalidRequest,
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
        "the restore was still being sent elsewhere, or its outcome was still unknown, when its \
         execution reference's retry validity ended"
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
            Self::Transport | Self::Stalled { .. } | Self::NoReply { .. } => true,
            Self::ReadArchive
            | Self::InvalidRequest
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
            | Self::Stalled { .. }
            | Self::NoReply { .. }
            | Self::Exhausted
            | Self::InvalidReply
            | Self::UnexpectedReply
            | Self::ReferenceMismatch { .. }
            | Self::UnresolvedAdmission => true,
            Self::ReadArchive
            | Self::InvalidRequest
            | Self::EmptyArchive
            | Self::NoAddress
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
    Sent(S, ClientRestoreWebSocketCodec),
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

    /// An unrecorded request failure cannot settle an earlier possible admission. Recovered
    /// failures and failures carrying a restore report remain authoritative, with every step.
    fn answered(self, end: RestoreEnd) -> RestoreEnd {
        match end {
            RestoreEnd::Outcome(outcome)
                if matches!(self, Self::Possible)
                    && matches!(&outcome.disposition, CommandDisposition::Failed)
                    && outcome.origin == OutcomeOrigin::Executed
                    && outcome.restore.is_none() =>
            {
                RestoreEnd::Unresolved(outcome)
            }
            end => end,
        }
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
    /// A later request failed without giving an earlier possibly admitted restore's outcome.
    /// Its complete typed answer is consumed only at the presentation boundary.
    Unresolved(Box<CommandOutcome>),
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
/// Non-security BLAKE3: identifies operator-selected restore bytes, not their provenance.
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
    /// The pacing the restore follows: the session's when it starts, and then that of the node
    /// each attempt's opening reached.
    pub(crate) pacing: ConsolePacing,
}

impl<F: ArchiveFile> RestoreUpload<F> {
    /// Streams the restore from `browser` until a reply ends it or the reference's retry validity
    /// ends, telling `progress` every count of bytes sent on the current attempt and the archive's
    /// size.
    pub(crate) async fn run<B: Browser>(
        mut self,
        browser: &B,
        progress: impl Fn(u64, u64) + Copy,
    ) -> Result<RestoreEnd, Report<RestoreStreamError>> {
        let issued_at = self
            .reference
            .retry_issued_at()
            .assured("the console issues every restore under a UUIDv7 execution reference");
        let mut admission = Admission::Impossible;
        // Every attempt that does not end the restore names why, before the validity is checked.
        let mut last_failure: Report<RestoreStreamError>;
        let mut consoles = RedirectChain::starting_at(self.base_url.as_deref());
        loop {
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
                    if self.retry_validity_ended(browser, issued_at) {
                        return Err(admission.ending(last_failure));
                    }
                    self.pause(browser, &mut consoles).await;
                    continue;
                }
            };
            let step = match reply_step(reply, &self.reference) {
                Ok(step) => step,
                Err(failure) => return Err(admission.after(attempt.streamed).ending(failure)),
            };
            let next = match step {
                // The leader refused this attempt's stream before it ran anything from it, which
                // settles nothing an earlier attempt may have had admitted.
                ReplyStep::End(RestoreEnd::Refused { failure, message }) => {
                    return admission.refused(failure, message);
                }
                ReplyStep::End(end) => return Ok(admission.answered(end)),
                ReplyStep::Redirect(console) => {
                    // A leader that loses its leadership while the restore applies redirects too.
                    admission = admission.after(attempt.streamed);
                    let console = console.to_string();
                    let next = consoles.follow(&console);
                    self.base_url = Some(console);
                    next
                }
                ReplyStep::AwaitElection => {
                    admission = admission.after(attempt.streamed);
                    NextAttempt::AfterPause
                }
                ReplyStep::AwaitOutcome => {
                    admission = Admission::Possible;
                    NextAttempt::AfterPause
                }
                // The leader admits nothing under a reference it no longer holds, so only an
                // earlier attempt can have had the restore admitted.
                ReplyStep::ReferenceExpired(outcome) => return admission.expired(outcome),
            };
            last_failure = Report::new(RestoreStreamError::Exhausted);
            // Once the reference's retry validity ends, the leader holds no restore under it.
            if self.retry_validity_ended(browser, issued_at) {
                return Err(admission.ending(last_failure));
            }
            if let NextAttempt::AfterPause = next {
                self.pause(browser, &mut consoles).await;
            }
        }
    }

    /// Whether the retry validity of the restore's reference, issued at `issued_at` on the
    /// browser's clock, has ended.
    fn retry_validity_ended<B: Browser>(&self, browser: &B, issued_at: Timestamp) -> bool {
        match issued_at.checked_add(self.pacing.command_retry_validity()) {
            Ok(ends) => browser.now() >= ends,
            // A validity that would end past the last representable instant has not ended.
            Err(_) => false,
        }
    }

    /// Pauses before the next attempt, which goes to the console the restore is addressed to.
    async fn pause<B: Browser>(&self, browser: &B, consoles: &mut RedirectChain) {
        consoles.paused_at(self.base_url.as_deref());
        browser
            .wait(self.pacing.web_console().restoration_backoff())
            .await;
    }

    /// Streams the restore once and waits for the reply that answers it.
    async fn attempt<B: Browser>(
        &mut self,
        browser: &B,
        progress: impl Fn(u64, u64),
    ) -> AttemptEnd {
        let sending = self.send_archive(browser, progress).await;
        let (mut socket, codec) = match sending {
            Ok(Sending::Sent(socket, codec)) => (socket, codec),
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
        let timeout = self.pacing.web_console().request_timeout();
        let reply = next_reply(browser, &mut socket, &codec, timeout).await;
        AttemptEnd {
            streamed: Streamed::Whole,
            reply,
        }
    }

    /// Opens one attempt's connection and hands it the start and every chunk of the archive,
    /// unless the leader answers first.
    async fn send_archive<B: Browser>(
        &mut self,
        browser: &B,
        progress: impl Fn(u64, u64),
    ) -> Result<Sending<B::Socket>, Report<RestoreStreamError>> {
        let Some(url) = self.socket_url() else {
            return Err(Report::new(RestoreStreamError::NoAddress));
        };
        let Some(mut socket) = browser.open(&url) else {
            return Err(Report::new(RestoreStreamError::Transport));
        };
        let opening_timeout = self.pacing.web_console().request_timeout();
        let opening = super::browser::read_opening(browser, &mut socket, opening_timeout).await;
        let opened = match opening {
            Ok(opened) => opened,
            Err(report) => {
                let context = match report.current_context() {
                    super::browser::CallOpeningError::Transport => RestoreStreamError::Transport,
                    super::browser::CallOpeningError::TimedOut => RestoreStreamError::Stalled {
                        timeout: opening_timeout,
                    },
                    super::browser::CallOpeningError::Invalid => RestoreStreamError::InvalidReply,
                };
                return Err(report.change_context(context));
            }
        };
        // This attempt and the pauses after it follow the node its opening reached.
        self.pacing.adopt(opened.pacing);
        let limits = opened.limits;
        let web_console = self.pacing.web_console();
        let room = ConnectionRoom {
            queued_bytes: queued_bytes_limit(web_console.restore_frame_capacity(), &limits),
            timeout: web_console.request_timeout(),
        };
        let codec = ClientRestoreWebSocketCodec::new(limits);
        let start = RestoreStart {
            request_id: RESTORE_REQUEST_ID,
            execution_reference: self.reference.clone(),
            statement: self.statement.clone(),
            archive: self.archive,
        };
        let start = start
            .encode(&limits)
            .map_err(|report| report.change_context(RestoreStreamError::InvalidRequest))?;
        if socket.send(Vec::from(codec.encode(start))).await.is_err() {
            return Err(Report::new(RestoreStreamError::Transport));
        }
        let total = self.archive.total_bytes.get();
        let chunk_bytes = u64::try_from(RestoreChunk::largest_bytes(&limits))
            .assured("a chunk of a quarter mebibyte is counted in u64 on every target");
        let mut sent = 0_u64;
        while sent < total {
            // The leader may answer before every byte arrived, as when it refuses the restore or
            // recovers the outcome of one it already ran; nothing more is sent then.
            if let Some(reply) = wait_for_room(browser, &mut socket, &codec, room).await? {
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
            let chunk = RestoreChunk::encode(&bytes, &limits).assured(
                "a non-empty chunk of at most half the announced frame limit always encodes",
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
        Ok(Sending::Sent(socket, codec))
    }

    fn socket_url(&self) -> Option<String> {
        let base = self.base_url.as_deref()?;
        let base = Url::parse(base).ok()?;
        web_console_websocket_url_from_base(&base, CONSOLE_RESTORE_PATH, &self.auth_token)
    }
}

/// How much a restore's connection may hold queued, and how long the stream waits for it to drain
/// below that, as the node the attempt reached announced.
#[derive(Debug, Clone, Copy)]
struct ConnectionRoom {
    queued_bytes: u64,
    timeout: Duration,
}

/// Waits until the connection has room for another chunk, and returns the reply when it arrives
/// first.
async fn wait_for_room<B: Browser>(
    browser: &B,
    socket: &mut B::Socket,
    codec: &ClientRestoreWebSocketCodec,
    room: ConnectionRoom,
) -> Result<Option<RestoreReply>, Report<RestoreStreamError>> {
    let most_polls = drain_polls(room.timeout);
    let mut polls = 0_u128;
    loop {
        if let Some(received) = socket.try_receive() {
            let reply = reply_of(received, codec)?;
            return Ok(Some(reply));
        }
        if u64::from(socket.buffered_amount()) <= room.queued_bytes {
            return Ok(None);
        }
        if polls >= most_polls {
            return Err(Report::new(RestoreStreamError::Stalled {
                timeout: room.timeout,
            }));
        }
        browser.wait(DRAIN_POLL).await;
        polls = polls
            .checked_add(1)
            .verified("the polls stop at the request timeout's drain polls, far below u128::MAX");
    }
}

/// The reply once the last chunk was sent, which must arrive within `timeout`, the request timeout
/// of the node the attempt reached.
async fn next_reply<B: Browser>(
    browser: &B,
    socket: &mut B::Socket,
    codec: &ClientRestoreWebSocketCodec,
    timeout: Duration,
) -> Result<RestoreReply, Report<RestoreStreamError>> {
    let received = pin!(socket.receive());
    let waited = pin!(browser.wait(timeout));
    let received = match select(received, waited).await {
        Either::Left((received, _)) => received,
        Either::Right(((), _)) => return Err(Report::new(RestoreStreamError::NoReply { timeout })),
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
