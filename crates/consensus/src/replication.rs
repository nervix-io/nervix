//! Pipelined Raft replication over one ordered append stream per follower.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Outstanding-batch admission, submission and acknowledgement order, the liveness
//!   contract of each append path, and the point at which a failed stream stops submitting.
//! - **Depends on.** OpenRaft's append contract and the interconnect duplex stream.
//! - **Must not know.** What a replicated command means, or which node should lead.
//!
//! A batch is charged before it is read out of the log, by the reservation the log reader takes to
//! materialize it, and it counts against this follower's outstanding budget from the moment it is
//! submitted until its answer arrives. The encoded copy is charged separately by the transport for
//! as long as it exists. Nothing here waits for the final network send to discover the bound.
//!
//! A follower answers a batch only after appending it durably, so how long one answer takes says
//! nothing about whether the stream still works. A stream with a batch outstanding stalls only when,
//! for the whole idle bound, no answer arrives and the follower accepts none of the leader's bytes.
//! The bound belongs to this crate rather than to OpenRaft's soft TTL, which follows the heartbeat
//! interval and so measures leader liveness instead of follower storage.
//!
//! Receiving has its own bound. A follower keeps at most `resident_replication_batches` decoded
//! batches on one stream, each charged to the Commands class from the moment it is decoded until
//! OpenRaft answers it. The slot is taken before the frame is decoded, so reaching the bound stops
//! this node reading rather than decoding a batch it has nowhere to put: the leader's flow-control
//! window closes and it stops sending, instead of this node accumulating batches no budget
//! measures. The executor validates at startup that the class holds the whole window beside one
//! batch being encoded, so a full window can always produce the answer that releases it. Batches
//! belonging to a stream the leader has already torn down are released with the stream rather than
//! staying queued behind it.

use std::{pin::pin, time::Duration};

use arch_into::ArchInto as _;
use futures_util::{Stream, StreamExt as _, stream::BoxStream};
use meticulous::OptionExt as _;
use nervix_execution::{Executor, Reservation};
use nervix_interconnect::{
    ChargedItem, DuplexItems, DuplexReceiver, DuplexSendProgress, DuplexSender,
    InterconnectDuplexRequest as _, Transport,
};
use nervix_models::ClusterNodeName;
use nervix_recovery::Discarded as _;
use openraft::{
    error::{RPCError, Unreachable},
    network::RPCOption,
    raft::{AppendEntriesRequest, StreamAppendResult},
};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    time::{Instant, sleep_until, timeout},
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::debug;

use crate::{LogIdOf, ProtocolReceiver, TypeConfig, validate_protocol_origin, wire};

/// How many entries one append batch may gather before the byte target stops it.
pub(crate) const MAX_APPEND_BATCH_ENTRIES: u64 = 1024;
/// How many submitted batches one follower may leave unacknowledged.
pub(crate) const MAX_OUTSTANDING_APPEND_BATCHES: usize = 16;
/// How many submitted bytes one follower may leave unacknowledged. The node's aggregate command
/// budget bounds the encoded copies across all followers independently.
pub(crate) const MAX_OUTSTANDING_APPEND_BYTES: u64 = 16 * 1024 * 1024;
/// How long an append stream with a batch outstanding may go without an answer while its follower
/// also accepts none of the leader's bytes.
const APPEND_STREAM_IDLE_BOUND: Duration = Duration::from_secs(5);
/// The ceiling on a heartbeat's deadline. OpenRaft's soft TTL is usually far shorter, and the
/// shorter of the two always wins.
const HEARTBEAT_DEADLINE_CEILING: Duration = Duration::from_secs(5);

const _: () = assert!(
    MAX_OUTSTANDING_APPEND_BATCHES > 0
        && MAX_APPEND_BATCH_ENTRIES > 0
        && !APPEND_STREAM_IDLE_BOUND.is_zero(),
    "append pipeline policy inputs must be nonzero",
);

/// What one append batch aims to carry when the log reader fills it.
///
/// A single semantic command is never split, so a batch gathers whole commands up to this size and
/// a command larger than it travels alone. The configured replication batch limit is validated to
/// hold that pair.
pub(crate) fn append_batch_target_bytes(executor: &Executor) -> u64 {
    executor.limits().command_bytes.as_u64()
}

/// Which liveness contract a network client's appends belong to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendPath {
    /// Leader liveness probes, carried one at a time on the management pool so replication
    /// backpressure cannot delay them.
    Heartbeat,
    /// Log replication, carried on one ordered append stream per follower.
    Replication,
}

/// The deadline for one heartbeat or leadership probe: whichever of OpenRaft's soft TTL and this
/// crate's ceiling expires first.
pub(crate) fn heartbeat_deadline(option: &RPCOption) -> Duration {
    option.soft_ttl().min(HEARTBEAT_DEADLINE_CEILING)
}

#[derive(Debug, Error)]
#[error("append stream to node '{target}' neither answered nor accepted bytes for {idle_bound:?}")]
struct AppendStreamStalled {
    target: ClusterNodeName,
    idle_bound: Duration,
}

#[derive(Debug, Error)]
#[error("append stream to node '{target}' failed: {reason}")]
struct AppendStreamFailed {
    target: ClusterNodeName,
    reason: String,
}

fn stream_failed(target: &ClusterNodeName, reason: impl std::fmt::Display) -> RPCError<TypeConfig> {
    RPCError::Unreachable(Unreachable::new(&AppendStreamFailed {
        target: target.clone(),
        reason: reason.to_string(),
    }))
}

/// Why one follower's submission stopped. Either way nothing more belongs on this stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubmissionStopped {
    /// The response reader ended this generation, so no further batch can be acknowledged.
    GenerationEnded,
    /// The stream to the follower failed while a batch was being written.
    StreamFailed,
}

/// One batch the leader has submitted and not yet seen acknowledged.
struct OutstandingBatch {
    last_log_id: Option<LogIdOf>,
    bytes: u64,
}

/// Submits batches in OpenRaft's order, no more than the per-follower bound allows outstanding.
struct AppendSubmission {
    sender: DuplexSender<wire::OpenAppendStream>,
    outstanding: mpsc::Sender<OutstandingBatch>,
    submitted_bytes: u64,
    batch_target_bytes: u64,
    acknowledged: mpsc::UnboundedReceiver<u64>,
}

impl AppendSubmission {
    async fn submit(
        &mut self,
        request: AppendEntriesRequest<TypeConfig>,
    ) -> Result<(), SubmissionStopped> {
        let last_log_id = match request.entries.last() {
            Some(entry) => Some(entry.log_id.clone()),
            None => request.prev_log_id.clone(),
        };
        self.wait_for_follower_capacity().await?;
        let record = wire::AppendEntriesRecord::from_request(request);
        let Ok(bytes) = self.sender.send(record).await else {
            return Err(SubmissionStopped::StreamFailed);
        };
        self.submitted_bytes = self
            .submitted_bytes
            .checked_add(bytes)
            .assured("outstanding bytes never exceed the per-follower bound plus one batch");
        let outstanding = OutstandingBatch { last_log_id, bytes };
        match self.outstanding.send(outstanding).await {
            Ok(()) => Ok(()),
            Err(_) => Err(SubmissionStopped::GenerationEnded),
        }
    }

    /// Wait until this follower's unacknowledged bytes leave room for one more target-size batch.
    async fn wait_for_follower_capacity(&mut self) -> Result<(), SubmissionStopped> {
        while self
            .submitted_bytes
            .checked_add(self.batch_target_bytes)
            .is_none_or(|total| total > MAX_OUTSTANDING_APPEND_BYTES)
        {
            tokio::task::consume_budget().await;
            let Some(acknowledged) = self.acknowledged.recv().await else {
                return Err(SubmissionStopped::GenerationEnded);
            };
            self.submitted_bytes = self
                .submitted_bytes
                .checked_sub(acknowledged)
                .assured("every acknowledgement names bytes this submission charged earlier");
        }
        Ok(())
    }
}

/// Decides when an append stream with a batch outstanding has stalled.
///
/// Nothing is measured while no batch is outstanding, so a healthy stream with nothing to carry
/// stays open however long it idles.
struct AppendStreamIdleBound {
    bound: Duration,
    last_answer_at: Instant,
}

impl AppendStreamIdleBound {
    fn new(bound: Duration) -> Self {
        Self {
            bound,
            last_answer_at: Instant::now(),
        }
    }

    /// Wait for the answer to an outstanding batch, or return `None` once the stream has stalled.
    ///
    /// `sender_accepted_at` reports when the follower last accepted the leader's bytes. The wait
    /// gives up only after a whole bound in which neither an answer nor accepted bytes moved the
    /// stream, however long the answer itself has been outstanding.
    async fn answer<F: Future>(
        &mut self,
        answer: F,
        sender_accepted_at: impl Fn() -> Instant,
    ) -> Option<F::Output> {
        let mut answer = pin!(answer);
        let mut stall = pin!(sleep_until(self.stalls_at(sender_accepted_at())));
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                biased;
                output = &mut answer => {
                    self.last_answer_at = Instant::now();
                    return Some(output);
                }
                () = &mut stall => {
                    let stalls_at = self.stalls_at(sender_accepted_at());
                    if stalls_at <= Instant::now() {
                        return None;
                    }
                    stall.as_mut().reset(stalls_at);
                }
            }
        }
    }

    /// When the stream stalls unless an answer arrives or the follower accepts bytes first.
    fn stalls_at(&self, sender_accepted_at: Instant) -> Instant {
        let last_movement = self.last_answer_at.max(sender_accepted_at);
        last_movement
            .checked_add(self.bound)
            .assured("the monotonic clock's range reaches far past a recent instant plus seconds")
    }
}

/// Everything one append stream generation owns while it is live.
///
/// A generation delivers its first failure and then stops: no later answer belonging to it can
/// advance OpenRaft past the batch that failed.
struct AppendStreamGeneration {
    receiver: DuplexReceiver<wire::OpenAppendStream>,
    sender_progress: DuplexSendProgress,
    outstanding: mpsc::Receiver<OutstandingBatch>,
    acknowledge: mpsc::UnboundedSender<u64>,
    idle_bound: AppendStreamIdleBound,
    target: ClusterNodeName,
    finished: bool,
    _submission: DropGuard,
}

impl AppendStreamGeneration {
    async fn next(
        &mut self,
    ) -> Option<Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>> {
        if self.finished {
            return None;
        }
        let outstanding = self.outstanding.recv().await?;
        let sender_progress = &self.sender_progress;
        let answer = self
            .idle_bound
            .answer(self.receiver.next(), || sender_progress.last_accepted_at())
            .await;
        let answer = match answer {
            Some(Ok(Some(answer))) => answer,
            Some(Ok(None)) => {
                return self.fail(stream_failed(&self.target, "the follower ended the stream"));
            }
            Some(Err(error)) => return self.fail(stream_failed(&self.target, error)),
            None => {
                return self.fail(RPCError::Unreachable(Unreachable::new(
                    &AppendStreamStalled {
                        target: self.target.clone(),
                        idle_bound: self.idle_bound.bound,
                    },
                )));
            }
        };
        self.acknowledge
            .send(outstanding.bytes)
            .discarded("a submission that already stopped needs no released capacity");
        let answer = match answer {
            Ok(answer) => answer.into_result(),
            Err(error) => return self.fail(stream_failed(&self.target, error)),
        };
        match &answer {
            Ok(matching) if matching == &outstanding.last_log_id => Some(Ok(answer)),
            // Partial acceptance and every append failure end this generation. OpenRaft resumes
            // from the progress this answer confirmed.
            _ => {
                self.finished = true;
                Some(Ok(answer))
            }
        }
    }

    fn fail(
        &mut self,
        error: RPCError<TypeConfig>,
    ) -> Option<Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>> {
        self.finished = true;
        Some(Err(error))
    }
}

/// Open one ordered append stream to `target` and pipeline `input` over it.
pub(crate) async fn open_append_stream<S>(
    interconnect: &Transport,
    executor: &Executor,
    local_node_id: &ClusterNodeName,
    target: &ClusterNodeName,
    input: S,
) -> Result<
    BoxStream<'static, Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>>,
    RPCError<TypeConfig>,
>
where
    S: Stream<Item = AppendEntriesRequest<TypeConfig>> + Send + Unpin + 'static,
{
    // The follower's handler accepts the stream before its Raft core sees any batch, so opening
    // takes the stream's own setup deadline rather than one sized for a heartbeat. Applying it here
    // also bounds the local admission the transport performs outside its peer-facing deadline.
    let opened = timeout(
        wire::OpenAppendStream::SETUP_TIMEOUT,
        interconnect.open_duplex_stream(
            target,
            wire::OpenAppendStream {
                leader_node_id: local_node_id.clone(),
            },
        ),
    )
    .await;
    let (sender, receiver) = match opened {
        Ok(Ok(halves)) => halves,
        Ok(Err(error)) => return Err(stream_failed(target, error)),
        Err(_) => {
            return Err(stream_failed(
                target,
                "the append stream did not open in time",
            ));
        }
    };
    let sender_progress = sender.progress();

    let (outstanding_tx, outstanding_rx) = mpsc::channel(MAX_OUTSTANDING_APPEND_BATCHES);
    let (acknowledge_tx, acknowledge_rx) = mpsc::unbounded_channel();
    let cancel_submission = CancellationToken::new();
    let submission_guard = cancel_submission.clone().drop_guard();
    let mut submission = AppendSubmission {
        sender,
        outstanding: outstanding_tx,
        submitted_bytes: 0,
        batch_target_bytes: append_batch_target_bytes(executor),
        acknowledged: acknowledge_rx,
    };
    let submission_target = target.clone();
    tokio::spawn(async move {
        let mut input = input;
        loop {
            tokio::task::consume_budget().await;
            let next = tokio::select! {
                () = cancel_submission.cancelled() => None,
                next = input.next() => next,
            };
            let Some(request) = next else {
                break;
            };
            if let Err(stopped) = submission.submit(request).await {
                debug!(target = %submission_target, ?stopped, "raft append stream stopped submitting");
                break;
            }
        }
        submission
            .sender
            .finish()
            .discarded("a stream the follower already closed needs no half-close");
    });

    let generation = AppendStreamGeneration {
        receiver,
        sender_progress,
        outstanding: outstanding_rx,
        acknowledge: acknowledge_tx,
        idle_bound: AppendStreamIdleBound::new(APPEND_STREAM_IDLE_BOUND),
        target: target.clone(),
        finished: false,
        _submission: submission_guard,
    };
    let answers = futures_util::stream::unfold(generation, |mut generation| async move {
        let answer = generation.next().await?;
        Some((answer, generation))
    });
    Ok(Box::pin(answers))
}

impl ProtocolReceiver {
    /// Answer one follower-side append stream, preserving the leader's submission order.
    ///
    /// Every decoded batch stays charged until OpenRaft answers it, and no more than the resident
    /// window are decoded at once, so a core that has fallen behind stops this node reading
    /// instead of queueing batches its budget never sees.
    pub(crate) fn answer_append_stream(
        &self,
        peer_node_id: ClusterNodeName,
        items: DuplexItems<wire::AppendEntriesRecord>,
    ) -> BoxStream<'static, Result<wire::StreamAppendResultRecord, wire::ConsensusRequestError>>
    {
        let window: usize = self
            .inner
            .store
            .limits()
            .resident_replication_batches
            .get()
            .arch_into();
        // One slot per resident batch. Holding the charge here is what bounds the window: it is
        // returned only when the answer to the batch it covers has been produced.
        let (charged, release) = mpsc::channel::<Reservation>(window);
        let requests = futures_util::stream::unfold(
            (items, peer_node_id, charged),
            |(mut items, peer_node_id, charged)| async move {
                // Taking the slot before the frame is decoded is what closes the leader's
                // flow-control window, rather than decoding a batch with nowhere to put it.
                let Ok(slot) = charged.reserve_owned().await else {
                    return None;
                };
                let decoded = match items.next().await {
                    Ok(Some(decoded)) => decoded,
                    Ok(None) => return None,
                    Err(error) => {
                        debug!(%error, "raft append stream ended");
                        return None;
                    }
                };
                let ChargedItem {
                    item: record,
                    charge,
                } = decoded;
                if let Err(error) =
                    validate_protocol_origin(&peer_node_id, record.origin_node_id(), "replication")
                {
                    debug!(%error, "raft append stream carried a foreign batch");
                    return None;
                }
                let request = match record.into_request() {
                    Ok(request) => request,
                    Err(error) => {
                        debug!(%error, "raft append stream carried an invalid batch");
                        return None;
                    }
                };
                let charged = slot.send(charge);
                Some((request, (items, peer_node_id, charged)))
            },
        );
        let answers = self.inner.raft.stream_append(requests);
        let answers = futures_util::stream::unfold(
            (Box::pin(answers), release),
            |(mut answers, mut release)| async move {
                let answer = answers.next().await?;
                // The batch this answer belongs to has been appended durably, so it is no longer
                // resident here and its charge opens the window for the next frame.
                let released = release.recv().await;
                drop(released);
                let answer = match answer {
                    Ok(result) => Ok(wire::StreamAppendResultRecord::from(result)),
                    Err(fatal) => Err(wire::ConsensusRequestError::raft(fatal)),
                };
                Some((answer, (answers, release)))
            },
        );
        Box::pin(answers)
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, future::pending};

    use tokio::time::sleep;

    use super::*;

    /// How often a follower that keeps reading accepts more of the leader's bytes.
    const ACCEPTANCE_CADENCE: Duration = Duration::from_millis(2_500);
    /// How long a follower that keeps reading takes to answer.
    const SLOW_ANSWER: Duration = Duration::from_secs(15);
    /// How long each answer takes from a follower that accepts no further bytes.
    const PROMPT_ANSWER: Duration = Duration::from_secs(4);

    const _: () = assert!(
        ACCEPTANCE_CADENCE.as_nanos() < APPEND_STREAM_IDLE_BOUND.as_nanos()
            && PROMPT_ANSWER.as_nanos() < APPEND_STREAM_IDLE_BOUND.as_nanos(),
        "every step of a healthy stream must fit inside one idle bound",
    );
    const _: () = assert!(
        SLOW_ANSWER.as_nanos() > 2 * APPEND_STREAM_IDLE_BOUND.as_nanos()
            && 2 * PROMPT_ANSWER.as_nanos() > APPEND_STREAM_IDLE_BOUND.as_nanos(),
        "each healthy stream must outlast the idle bound it would miss if nothing moved",
    );

    #[tokio::test(start_paused = true)]
    async fn a_follower_that_keeps_accepting_bytes_is_not_cut_while_its_answer_is_outstanding() {
        let mut idle_bound = AppendStreamIdleBound::new(APPEND_STREAM_IDLE_BOUND);
        let started_at = Instant::now();
        let sender_accepted_at = Cell::new(started_at);
        // The follower keeps taking the leader's bytes and answers only after several bounds.
        let answer = async {
            while started_at.elapsed() < SLOW_ANSWER {
                tokio::task::consume_budget().await;
                sleep(ACCEPTANCE_CADENCE).await;
                sender_accepted_at.set(Instant::now());
            }
        };

        let answered = idle_bound.answer(answer, || sender_accepted_at.get()).await;

        assert!(
            answered.is_some(),
            "a follower that keeps accepting bytes must not be cut while its answer is outstanding"
        );
        assert!(
            started_at.elapsed() >= SLOW_ANSWER,
            "the answer must have outlasted several idle bounds"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_outstanding_batch_with_neither_an_answer_nor_accepted_bytes_is_cut_after_the_bound()
    {
        let mut idle_bound = AppendStreamIdleBound::new(APPEND_STREAM_IDLE_BOUND);
        let started_at = Instant::now();

        let answered = idle_bound.answer(pending::<()>(), || started_at).await;

        assert!(
            answered.is_none(),
            "a stream that neither answered nor accepted bytes for the bound must be cut"
        );
        assert!(
            started_at.elapsed() >= APPEND_STREAM_IDLE_BOUND,
            "the stream must not be cut before the whole bound passed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_follower_that_stops_accepting_bytes_is_cut_one_bound_after_its_last_acceptance() {
        let mut idle_bound = AppendStreamIdleBound::new(APPEND_STREAM_IDLE_BOUND);
        let started_at = Instant::now();
        let sender_accepted_at = Cell::new(started_at);
        let answer = async {
            sleep(ACCEPTANCE_CADENCE).await;
            sender_accepted_at.set(Instant::now());
            pending::<()>().await;
        };

        let answered = idle_bound.answer(answer, || sender_accepted_at.get()).await;

        assert!(
            answered.is_none(),
            "a follower that stopped accepting bytes and never answered must be cut"
        );
        let since_last_acceptance = sender_accepted_at.get().elapsed();
        let noticed_late = APPEND_STREAM_IDLE_BOUND
            .checked_add(ACCEPTANCE_CADENCE)
            .assured("an idle bound plus an acceptance cadence of seconds fits in a Duration");
        assert!(
            since_last_acceptance >= APPEND_STREAM_IDLE_BOUND
                && since_last_acceptance < noticed_late,
            "the stream must be cut one whole bound after the last acceptance, not \
             {since_last_acceptance:?} after it"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn each_answer_restarts_the_bound_for_the_next_outstanding_batch() {
        let mut idle_bound = AppendStreamIdleBound::new(APPEND_STREAM_IDLE_BOUND);
        let started_at = Instant::now();

        let first = idle_bound.answer(sleep(PROMPT_ANSWER), || started_at).await;
        let second = idle_bound.answer(sleep(PROMPT_ANSWER), || started_at).await;

        assert!(
            first.is_some() && second.is_some(),
            "answers that each arrive within the bound must keep the stream open, even once the \
             stream has carried them for longer than one bound"
        );
    }
}
