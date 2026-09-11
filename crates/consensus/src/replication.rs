//! Pipelined Raft replication over one ordered append stream per follower.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Outstanding-batch admission, submission and acknowledgement order, per-response
//!   progress deadlines, and the point at which a failed stream stops submitting.
//! - **Depends on.** OpenRaft's append contract and the interconnect duplex stream.
//! - **Must not know.** What a replicated command means, or which node should lead.
//!
//! A batch is charged before it is read out of the log, by the reservation the log reader takes to
//! materialize it, and it counts against this follower's outstanding budget from the moment it is
//! submitted until its answer arrives. The encoded copy is charged separately by the transport for
//! as long as it exists. Nothing here waits for the final network send to discover the bound.

use std::time::Duration;

use futures_util::{Stream, StreamExt as _, stream::BoxStream};
use meticulous::OptionExt as _;
use nervix_execution::Executor;
use nervix_interconnect::{DuplexItems, DuplexReceiver, DuplexSender, Transport};
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
    time::{Instant, timeout},
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
/// The ceiling on setup and per-response deadlines. OpenRaft's soft TTL is usually far shorter,
/// and the shorter of the two always wins.
const APPEND_DEADLINE_CEILING: Duration = Duration::from_secs(5);

const _: () = assert!(
    MAX_OUTSTANDING_APPEND_BATCHES > 0 && MAX_APPEND_BATCH_ENTRIES > 0,
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

/// The effective deadline for one step of an append stream: whichever of OpenRaft's soft TTL and
/// this crate's own ceiling expires first.
pub(crate) fn append_deadline(option: &RPCOption) -> Duration {
    option.soft_ttl().min(APPEND_DEADLINE_CEILING)
}

#[derive(Debug, Error)]
#[error("append stream to node '{target}' made no progress for {deadline:?}")]
struct AppendStreamStalled {
    target: ClusterNodeName,
    deadline: Duration,
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

/// Everything one append stream generation owns while it is live.
///
/// A generation delivers its first failure and then stops: no later answer belonging to it can
/// advance OpenRaft past the batch that failed.
struct AppendStreamGeneration {
    receiver: DuplexReceiver<wire::OpenAppendStream>,
    outstanding: mpsc::Receiver<OutstandingBatch>,
    acknowledge: mpsc::UnboundedSender<u64>,
    response_deadline: Duration,
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
        // The deadline is armed only while this batch is outstanding, so a healthy stream with
        // nothing to carry is never cut short.
        let answer = match timeout(self.response_deadline, self.receiver.next()).await {
            Ok(Ok(Some(answer))) => answer,
            Ok(Ok(None)) => {
                return self.fail(stream_failed(&self.target, "the follower ended the stream"));
            }
            Ok(Err(error)) => return self.fail(stream_failed(&self.target, error)),
            Err(_) => {
                return self.fail(RPCError::Unreachable(Unreachable::new(
                    &AppendStreamStalled {
                        target: self.target.clone(),
                        deadline: self.response_deadline,
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
    option: RPCOption,
) -> Result<
    BoxStream<'static, Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>>,
    RPCError<TypeConfig>,
>
where
    S: Stream<Item = AppendEntriesRequest<TypeConfig>> + Send + Unpin + 'static,
{
    let deadline = append_deadline(&option);
    let setup_deadline = Instant::now().checked_add(deadline).ok_or_else(|| {
        stream_failed(
            target,
            "the setup deadline exceeds the monotonic clock range",
        )
    })?;
    let opened = timeout(
        setup_deadline.saturating_duration_since(Instant::now()),
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
        outstanding: outstanding_rx,
        acknowledge: acknowledge_tx,
        response_deadline: deadline,
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
    pub(crate) fn answer_append_stream(
        &self,
        peer_node_id: ClusterNodeName,
        items: DuplexItems<wire::AppendEntriesRecord>,
    ) -> BoxStream<'static, Result<wire::StreamAppendResultRecord, wire::ConsensusRequestError>>
    {
        let requests = futures_util::stream::unfold(
            (items, peer_node_id),
            |(mut items, peer_node_id)| async move {
                let record = match items.next().await {
                    Ok(Some(record)) => record,
                    Ok(None) => return None,
                    Err(error) => {
                        debug!(%error, "raft append stream ended");
                        return None;
                    }
                };
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
                Some((request, (items, peer_node_id)))
            },
        );
        let answers = self.inner.raft.stream_append(requests);
        Box::pin(answers.map(|answer| match answer {
            Ok(result) => Ok(wire::StreamAppendResultRecord::from(result)),
            Err(fatal) => Err(wire::ConsensusRequestError::raft(fatal)),
        }))
    }
}
