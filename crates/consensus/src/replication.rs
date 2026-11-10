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

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{pin::pin, time::Duration};

use arch_into::ArchInto as _;
use error_stack::Report;
use futures_util::{Stream, StreamExt as _, stream::BoxStream};
use meticulous::OptionExt as _;
use nervix_execution::{Executor, Reservation};
use nervix_interconnect::{
    ChargedItem, DuplexItems, DuplexReceiver, DuplexSendProgress, DuplexSender, RequestError,
    Transport,
};
use nervix_models::ClusterNodeName;
use nervix_primitives::{
    sync::{CancellationToken, DropGuard, mpsc},
    time::{Instant, sleep_until, timeout},
};
use nervix_recovery::Discarded as _;
use openraft::{
    error::{RPCError, Unreachable},
    network::{RPCOption, RaftNetworkV2 as _},
    raft::{AppendEntriesRequest, StreamAppendResult},
};
use thiserror::Error;
use tracing::debug;

use crate::{
    AppendStreamOpenRecorder, AppendSubmissionFault, LogIdOf, NetworkClient, ProtocolReceiver,
    TypeConfig, unreachable_err, validate_protocol_origin, wire,
};

#[cfg(test)]
mod append_setup_tests;

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

/// Heartbeat answers may arrive throughout the worker's lease-sized hard TTL. Initial empty
/// replication probes retain their shorter soft TTL and configured append-idle ceiling.
pub(crate) fn append_request_deadline(
    option: &RPCOption,
    path: AppendPath,
    idle_bound: Duration,
) -> Duration {
    match path {
        AppendPath::Heartbeat => option.hard_ttl(),
        AppendPath::Replication => option.soft_ttl().min(idle_bound),
    }
}

/// Initial probes have no log data; commit updates following data stay on the ordered stream.
enum ReplicationReplyPhase<S> {
    Probing {
        input: S,
        confirmed_prefix: Option<LogIdOf>,
    },
    Streaming(BoxStream<'static, Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>>),
    Finished,
}

/// One task owns the transition from management probes to the follower's data stream.
struct ReplicationReplies<Recorder, S> {
    client: NetworkClient<Recorder>,
    option: RPCOption,
    phase: ReplicationReplyPhase<S>,
}

impl<Recorder, S> ReplicationReplies<Recorder, S>
where
    Recorder: AppendStreamOpenRecorder,
    S: Stream<Item = AppendEntriesRequest<TypeConfig>> + Send + Unpin + 'static,
{
    async fn next(
        &mut self,
    ) -> Option<Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>> {
        nervix_primitives::task::consume_budget().await;
        let phase = std::mem::replace(&mut self.phase, ReplicationReplyPhase::Finished);
        match phase {
            ReplicationReplyPhase::Probing {
                mut input,
                confirmed_prefix,
            } => {
                let request = input.next().await?;
                if request.entries.is_empty() {
                    let boundary = request.prev_log_id.clone();
                    let response = match self
                        .client
                        .append_entries(request, self.option.clone())
                        .await
                    {
                        Ok(response) => response,
                        Err(error) => return Some(Err(error)),
                    };
                    let answer = response.into_stream_result(boundary.clone(), boundary.clone());
                    if answer == Ok(boundary.clone()) {
                        self.phase = ReplicationReplyPhase::Probing {
                            input,
                            confirmed_prefix: boundary,
                        };
                    }
                    Some(Ok(answer))
                } else {
                    let mut answers = match self
                        .client
                        .open_replication_data(request, input, confirmed_prefix)
                        .await
                    {
                        Ok(answers) => answers,
                        Err(error) => return Some(Err(error)),
                    };
                    let answer = answers.next().await;
                    self.phase = ReplicationReplyPhase::Streaming(answers);
                    answer
                }
            }
            ReplicationReplyPhase::Streaming(mut answers) => {
                let answer = answers.next().await;
                self.phase = ReplicationReplyPhase::Streaming(answers);
                answer
            }
            ReplicationReplyPhase::Finished => None,
        }
    }
}

impl<Recorder> NetworkClient<Recorder>
where
    Recorder: AppendStreamOpenRecorder,
{
    pub(crate) async fn replication_answers<S>(
        mut self,
        mut input: S,
        option: RPCOption,
    ) -> Result<
        BoxStream<'static, Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>>,
        RPCError<TypeConfig>,
    >
    where
        S: Stream<Item = AppendEntriesRequest<TypeConfig>> + Send + Unpin + 'static,
    {
        // Consume input before even an immediate refusal, preserving the caller's retry gate.
        let Some(first) = input.next().await else {
            return Ok(Box::pin(futures_util::stream::empty()));
        };
        if !first.entries.is_empty() {
            return self.open_replication_data(first, input, None).await;
        }
        let boundary = first.prev_log_id.clone();
        let response = self.append_entries(first, option.clone()).await?;
        let answer = response.into_stream_result(boundary.clone(), boundary.clone());
        let continues = answer == Ok(boundary.clone());
        let first_answer = futures_util::stream::iter([Ok(answer)]);
        if !continues {
            return Ok(Box::pin(first_answer));
        }
        let owner = ReplicationReplies {
            client: self,
            option,
            phase: ReplicationReplyPhase::Probing {
                input,
                confirmed_prefix: boundary,
            },
        };
        let remaining = futures_util::stream::unfold(owner, |mut owner| async move {
            let answer = owner.next().await?;
            Some((answer, owner))
        });
        Ok(Box::pin(first_answer.chain(remaining)))
    }

    async fn open_replication_data<S>(
        &self,
        first: AppendEntriesRequest<TypeConfig>,
        input: S,
        confirmed_prefix: Option<LogIdOf>,
    ) -> Result<
        BoxStream<'static, Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>>,
        RPCError<TypeConfig>,
    >
    where
        S: Stream<Item = AppendEntriesRequest<TypeConfig>> + Send + Unpin + 'static,
    {
        self.connectivity.check().map_err(unreachable_err)?;
        let setup = async {
            self.append_stream_open_recorder
                .record_append_stream_attempt(&self.target);
            if let Some(previous) = &first.prev_log_id
                && Some(previous) != confirmed_prefix.as_ref()
            {
                // A first data batch may start at an unknown follower boundary. Check only that
                // prefix before transferring entries. Capping the probe's commit position keeps
                // it from committing an unchecked suffix; the original request stays intact.
                let prefix = AppendEntriesRequest::<TypeConfig> {
                    vote: first.vote.clone(),
                    prev_log_id: first.prev_log_id.clone(),
                    entries: Vec::new(),
                    leader_commit: first.leader_commit.clone().min(first.prev_log_id.clone()),
                };
                let prefix =
                    wire::HeartbeatRequest(wire::AppendEntriesRecord::from_request(prefix));
                let response = self
                    .interconnect
                    .request_with_timeout(
                        &self.target,
                        prefix,
                        self.interconnect
                            .duplex_setup_deadline::<wire::OpenAppendStream>(),
                    )
                    .await
                    .map_err(|error| unreachable_err(error.into_error()))?;
                let response = response.map_err(unreachable_err)?.into_response();
                let answer = response
                    .into_stream_result(first.prev_log_id.clone(), first.prev_log_id.clone());
                debug!(
                    target = %self.target,
                    previous = ?previous,
                    result = ?answer,
                    "raft data prefix checked before stream admission"
                );
                if answer != Ok(first.prev_log_id.clone()) {
                    let answers: BoxStream<
                        'static,
                        Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>,
                    > = Box::pin(futures_util::stream::iter([Ok(answer)]));
                    return Ok(answers);
                }
            }
            debug!(
                target = %self.target,
                previous = ?first.prev_log_id,
                first_entry = ?first.entries.first().map(|entry| &entry.log_id),
                last_entry = ?first.entries.last().map(|entry| &entry.log_id),
                entry_count = first.entries.len(),
                "raft append stream opening with first batch"
            );
            let input = futures_util::stream::iter([first]).chain(input);
            let answers = open_append_stream(
                &self.interconnect,
                &self.executor,
                self.operations,
                &self.local_node_id,
                &self.target,
                self.append_stream_open_recorder
                    .append_submission_fault(&self.target),
                input,
            )
            .await?;
            self.append_stream_open_recorder
                .record_append_stream_open(&self.target);
            Ok(answers)
        };
        // One setup deadline covers prefix verification, local admission and stream headers.
        // Data setup has no dependency on the caller's heartbeat-derived soft TTL.
        match timeout(
            self.interconnect
                .duplex_setup_deadline::<wire::OpenAppendStream>(),
            setup,
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(stream_failed(
                &self.target,
                "the append stream did not open in time",
            )),
        }
    }
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
#[derive(Debug)]
enum SubmissionStopped {
    /// The response reader ended this generation, so no further batch can be acknowledged.
    GenerationEnded,
    /// Writing a batch to the follower failed, so neither it nor any later batch reached it.
    StreamFailed(Report<RequestError>),
}

/// One batch the leader has submitted and not yet seen acknowledged.
struct OutstandingBatch {
    last_log_id: Option<LogIdOf>,
    bytes: u64,
}

/// What the submission tells the answers, in the order it happened.
enum Submitted {
    /// A batch the follower now holds and owes an answer for.
    Batch(OutstandingBatch),
    /// Writing the next batch failed. The batches submitted before it still get their answers;
    /// then the stream ends with this failure, because a stream that ended without one would tell
    /// OpenRaft that every request it sent was answered.
    Failed(Report<RequestError>),
}

#[derive(Debug, Error)]
#[error("the append stream stopped before it submitted every batch")]
struct AppendSubmissionFailed;

/// Submits batches in OpenRaft's order, no more than the per-follower bound allows outstanding.
struct AppendSubmission {
    sender: DuplexSender<wire::OpenAppendStream>,
    outstanding: mpsc::Sender<Submitted>,
    submitted_bytes: u64,
    outstanding_limit_bytes: u64,
    batch_target_bytes: u64,
    acknowledged: mpsc::UnboundedReceiver<u64>,
    _fault: AppendSubmissionFault,
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
        #[cfg(any(test, feature = "testing"))]
        if self._fault.take(&request).await {
            let failure = std::io::Error::other("injected append submission failure");
            let failure = Report::new(failure).change_context(RequestError::Encode {
                request: <wire::OpenAppendStream as nervix_interconnect::InterconnectDuplexRequest>::NAME,
            });
            return Err(SubmissionStopped::StreamFailed(failure));
        }
        let record = wire::AppendEntriesRecord::from_request(request);
        let bytes = match self.sender.send(record).await {
            Ok(bytes) => bytes,
            Err(error) => return Err(SubmissionStopped::StreamFailed(error)),
        };
        self.submitted_bytes = self
            .submitted_bytes
            .checked_add(bytes)
            .assured("outstanding bytes never exceed the per-follower bound plus one batch");
        let outstanding = OutstandingBatch { last_log_id, bytes };
        match self.outstanding.send(Submitted::Batch(outstanding)).await {
            Ok(()) => Ok(()),
            Err(_) => Err(SubmissionStopped::GenerationEnded),
        }
    }

    /// Hands a failed write to the answers, after the batches submitted before it. A generation
    /// that already ended reads no answers, so it needs no failure either.
    async fn stop(&mut self, stopped: SubmissionStopped) {
        let SubmissionStopped::StreamFailed(failure) = stopped else {
            return;
        };
        self.outstanding
            .send(Submitted::Failed(failure))
            .await
            .discarded("a generation that already ended reads no further answers");
    }

    /// Wait until this follower's unacknowledged bytes leave room for one more target-size batch.
    async fn wait_for_follower_capacity(&mut self) -> Result<(), SubmissionStopped> {
        while self
            .submitted_bytes
            .checked_add(self.batch_target_bytes)
            .is_none_or(|total| total > self.outstanding_limit_bytes)
        {
            nervix_primitives::task::consume_budget().await;
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
            nervix_primitives::task::consume_budget().await;
            nervix_primitives::select! {
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
    outstanding: mpsc::Receiver<Submitted>,
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
        // The submission's channel closes without a failure only once OpenRaft's input ended and
        // every batch it carried was submitted, so only then does the stream end as exhausted.
        let outstanding = match self.outstanding.recv().await? {
            Submitted::Batch(outstanding) => outstanding,
            Submitted::Failed(failure) => {
                let failure = failure.change_context(AppendSubmissionFailed);
                return self.fail(RPCError::Unreachable(unreachable_err(failure.into_error())));
            }
        };
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
                debug!(
                    target = %self.target,
                    submitted = ?outstanding.last_log_id,
                    result = ?answer,
                    "raft append stream ended after follower response"
                );
                self.finished = true;
                Some(Ok(answer))
            }
        }
    }

    fn fail(
        &mut self,
        error: RPCError<TypeConfig>,
    ) -> Option<Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>> {
        debug!(target = %self.target, %error, "raft append stream generation failed");
        self.finished = true;
        Some(Err(error))
    }
}

/// Open one ordered append stream to `target` and pipeline `input` over it.
async fn open_append_stream<S>(
    interconnect: &Transport,
    executor: &Executor,
    operations: crate::RaftOperationSettings,
    local_node_id: &ClusterNodeName,
    target: &ClusterNodeName,
    fault: AppendSubmissionFault,
    input: S,
) -> Result<
    BoxStream<'static, Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>>,
    RPCError<TypeConfig>,
>
where
    S: Stream<Item = AppendEntriesRequest<TypeConfig>> + Send + Unpin + 'static,
{
    // The network client's setup deadline surrounds this private materialization, including
    // local admission, after any required prefix verification.
    let (sender, receiver) = interconnect
        .open_duplex_stream(
            target,
            wire::OpenAppendStream {
                leader_node_id: local_node_id.clone(),
            },
        )
        .await
        .map_err(|error| stream_failed(target, error))?;
    let sender_progress = sender.progress();

    let (outstanding_tx, outstanding_rx) =
        mpsc::channel(*operations.outstanding_append_batches.value());
    let (acknowledge_tx, acknowledge_rx) = mpsc::unbounded_channel();
    let cancel_submission = CancellationToken::new();
    let submission_guard = cancel_submission.clone().drop_guard();
    let mut submission = AppendSubmission {
        sender,
        outstanding: outstanding_tx,
        submitted_bytes: 0,
        outstanding_limit_bytes: operations.outstanding_append_bytes.value().as_u64(),
        batch_target_bytes: append_batch_target_bytes(executor),
        acknowledged: acknowledge_rx,
        _fault: fault,
    };
    let submission_target = target.clone();
    nervix_primitives::task::spawn(async move {
        let mut input = input;
        loop {
            nervix_primitives::task::consume_budget().await;
            let next = nervix_primitives::select! {
                () = cancel_submission.cancelled() => None,
                next = input.next() => next,
            };
            let Some(request) = next else {
                break;
            };
            if let Err(stopped) = submission.submit(request).await {
                match &stopped {
                    SubmissionStopped::GenerationEnded => {
                        debug!(target = %submission_target, "raft append stream stopped submitting after its answers ended");
                    }
                    SubmissionStopped::StreamFailed(error) => {
                        debug!(
                            target = %submission_target,
                            error = %format_args!("{error:#}"),
                            "raft append stream stopped submitting"
                        );
                    }
                }
                submission.stop(stopped).await;
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
        idle_bound: AppendStreamIdleBound::new(*operations.append_idle_bound.value()),
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
                        debug!(error = %format_args!("{error:#}"), "raft append stream ended");
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
                    debug!(error = %format_args!("{error:#}"), "raft append stream carried a foreign batch");
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
#[path = "replication/initial_probe_tests.rs"]
mod initial_probe_tests;

#[cfg(test)]
mod tests {
    use std::{cell::Cell, future::pending};

    use nervix_primitives::time::sleep;

    use super::*;

    #[test]
    fn heartbeat_answer_uses_the_lease_without_extending_replication_probes() {
        let lease = RPCOption::new(Duration::from_millis(1_992));
        let idle_bound = Duration::from_secs(5);
        assert_eq!(
            append_request_deadline(&lease, AppendPath::Heartbeat, idle_bound),
            Duration::from_millis(1_992)
        );
        assert_eq!(
            append_request_deadline(&lease, AppendPath::Replication, idle_bound),
            Duration::from_millis(1_494)
        );

        let long_probe = RPCOption::new(Duration::from_secs(12));
        assert_eq!(
            append_request_deadline(&long_probe, AppendPath::Replication, idle_bound),
            idle_bound
        );
        assert_eq!(
            append_request_deadline(&long_probe, AppendPath::Replication, Duration::from_secs(2)),
            Duration::from_secs(2)
        );
    }

    /// How often a follower that keeps reading accepts more of the leader's bytes.
    const ACCEPTANCE_CADENCE: Duration = Duration::from_millis(2_500);
    /// How long a follower that keeps reading takes to answer.
    const SLOW_ANSWER: Duration = Duration::from_secs(15);
    /// How long each answer takes from a follower that accepts no further bytes.
    const PROMPT_ANSWER: Duration = Duration::from_secs(4);

    #[nervix_primitives::test(start_paused = true)]
    async fn a_follower_that_keeps_accepting_bytes_is_not_cut_while_its_answer_is_outstanding() {
        let mut idle_bound = AppendStreamIdleBound::new(
            <crate::AppendIdleBound as nervix_models::SettingDefinition>::default_value(&()),
        );
        let started_at = Instant::now();
        let sender_accepted_at = Cell::new(started_at);
        // The follower keeps taking the leader's bytes and answers only after several bounds.
        let answer = async {
            while started_at.elapsed() < SLOW_ANSWER {
                nervix_primitives::task::consume_budget().await;
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

    #[nervix_primitives::test(start_paused = true)]
    async fn an_outstanding_batch_with_neither_an_answer_nor_accepted_bytes_is_cut_after_the_bound()
    {
        let mut idle_bound = AppendStreamIdleBound::new(
            <crate::AppendIdleBound as nervix_models::SettingDefinition>::default_value(&()),
        );
        let started_at = Instant::now();

        let answered = idle_bound.answer(pending::<()>(), || started_at).await;

        assert!(
            answered.is_none(),
            "a stream that neither answered nor accepted bytes for the bound must be cut"
        );
        assert!(
            started_at.elapsed()
                >= <crate::AppendIdleBound as nervix_models::SettingDefinition>::default_value(&()),
            "the stream must not be cut before the whole bound passed"
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn a_follower_that_stops_accepting_bytes_is_cut_one_bound_after_its_last_acceptance() {
        let mut idle_bound = AppendStreamIdleBound::new(
            <crate::AppendIdleBound as nervix_models::SettingDefinition>::default_value(&()),
        );
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
        let noticed_late =
            <crate::AppendIdleBound as nervix_models::SettingDefinition>::default_value(&())
                .checked_add(ACCEPTANCE_CADENCE)
                .assured("an idle bound plus an acceptance cadence of seconds fits in a Duration");
        assert!(
            since_last_acceptance
                >= <crate::AppendIdleBound as nervix_models::SettingDefinition>::default_value(&())
                && since_last_acceptance < noticed_late,
            "the stream must be cut one whole bound after the last acceptance, not \
             {since_last_acceptance:?} after it"
        );
    }

    #[nervix_primitives::test(start_paused = true)]
    async fn each_answer_restarts_the_bound_for_the_next_outstanding_batch() {
        let mut idle_bound = AppendStreamIdleBound::new(
            <crate::AppendIdleBound as nervix_models::SettingDefinition>::default_value(&()),
        );
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
