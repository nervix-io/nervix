//! The broker loop's read of one batch, which the host may interrupt but never abandons.
//!
//! Layer: data plane.
//!
//! - **Owns.** Waiting on one broker read, flushing the ingest group while the read waits,
//!   cancelling the read when a quiesce change or a stop needs the loop back, and telling the loop
//!   how the read ended, which decides whether a retry delay still applies after it.
//! - **Depends on.** The connector ingestor contract and the flush, quiesce and stop signals of the
//!   ingestor host.
//! - **Must not know.** A broker driver, or how the loop takes in a batch and settles its
//!   positions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::{future, pin::pin};

use nervix_connector::{IngestorReadCancellation, IngestorResult};

use super::*;

/// How a broker read ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadEnd {
    /// The read returned without the host cancelling it.
    Returned,
    /// The host cancelled the read because the ingestor stops. The loop ends once it has taken in
    /// what the read returned.
    Stopping,
    /// The host cancelled the read because the ingestor's quiesce state changed, or its stop
    /// signal changed without stopping it. The loop takes in what the read returned and starts its
    /// next turn at once, which observes the new state.
    Restarting,
}

impl ReadEnd {
    /// Whether the host cancelled the read.
    fn cancelled(self) -> bool {
        match self {
            Self::Returned => false,
            Self::Stopping | Self::Restarting => true,
        }
    }

    pub(super) fn stops(self) -> bool {
        match self {
            Self::Stopping => true,
            Self::Returned | Self::Restarting => false,
        }
    }

    /// Waits out `delay` before the loop's next read, and returns whether the loop goes on.
    ///
    /// The stop or quiesce change that ended a cancelled read would have ended this wait too, so
    /// after one the loop stops, or starts its next turn, at once.
    pub(super) async fn wait_for_retry(
        self,
        host: &mut IngestorHost,
        shutdown: &mut watch::Receiver<bool>,
        delay: Duration,
    ) -> bool {
        match self {
            Self::Returned => wait_for_retry(host, shutdown, delay).await,
            Self::Stopping => false,
            Self::Restarting => true,
        }
    }
}

/// What one broker read returned, and how it ended.
pub(super) struct BrokerRead<M> {
    pub(super) batch: IngestorResult<IngestorBatch<M>>,
    pub(super) end: ReadEnd,
}

/// What the loop does with a read once it returned.
pub(super) enum ReadOutcome<M> {
    /// The messages to take in: none when the ingestor returned an empty batch, a cancelled read
    /// had gathered nothing, or the ingestor dropped what it read for its size.
    TakeIn(Vec<M>),
    ResumeRequired,
    Closed,
    Failed(Report<IngestorError>),
}

impl<M> BrokerRead<M> {
    /// What the loop does with this read. A batch the ingestor contract forbids, an empty one or an
    /// interruption the host did not ask for, is reported, and what it carries is still taken in.
    /// Payloads the ingestor dropped for their size are counted, and the loop reads again.
    pub(super) fn into_outcome(self, host: &IngestorHost) -> ReadOutcome<M> {
        match self.batch {
            Ok(IngestorBatch::Messages(messages)) => {
                if messages.is_empty() {
                    host.report_error("source returned an empty message batch".to_string());
                }
                ReadOutcome::TakeIn(messages)
            }
            Ok(IngestorBatch::Interrupted { gathered }) => {
                if !self.end.cancelled() {
                    host.report_error(
                        "source interrupted a read the host did not cancel".to_string(),
                    );
                }
                ReadOutcome::TakeIn(gathered)
            }
            Ok(IngestorBatch::Oversized { payloads }) => {
                host.record_oversized_payloads(payloads);
                ReadOutcome::TakeIn(Vec::new())
            }
            Ok(IngestorBatch::ResumeRequired) => ReadOutcome::ResumeRequired,
            Ok(IngestorBatch::Closed) => ReadOutcome::Closed,
            Err(error) => ReadOutcome::Failed(error),
        }
    }
}

/// Reads the next batch from `ingestor` and awaits the read to completion.
///
/// A flush that falls due while the read waits needs nothing from the ingestor, so the host flushes
/// and keeps waiting. A quiesce change or a stop needs the loop back: the host cancels the read and
/// still awaits it, so every message the ingestor already took from its transport reaches the loop
/// rather than being dropped with the read.
pub(super) async fn read_batch<C>(
    source: &mut C,
    host: &mut IngestorHost,
    shutdown: &mut watch::Receiver<bool>,
    request: IngestorBatchRequest,
) -> BrokerRead<C::Message>
where
    C: BrokerIngestorConnector,
{
    let cancellation = IngestorReadCancellation::new();
    let mut read = pin!(source.next_batch(request, &cancellation));
    let end = loop {
        nervix_primitives::task::consume_budget().await;
        let next_flush = host.next_flush();
        nervix_primitives::select! {
            batch = &mut read => {
                return BrokerRead {
                    batch,
                    end: ReadEnd::Returned,
                };
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break ReadEnd::Stopping;
                }
                break ReadEnd::Restarting;
            }
            () = host.wait_for_quiesce_change() => break ReadEnd::Restarting,
            () = wait_for_flush(next_flush) => flush_for_lifecycle(host).await,
        }
    };
    cancellation.cancel();
    let batch = read.await;
    BrokerRead { batch, end }
}

async fn wait_for_flush(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => future::pending().await,
    }
}
