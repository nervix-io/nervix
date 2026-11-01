//! A paced poll's intake: how the messages one poll returned enter the graph in bounded batches.
//!
//! Layer: data plane.
//!
//! - **Owns.** Reporting the records a poll could not read, counting header values its connector
//!   dropped, splitting the messages it did read into intake batches no larger than the bound the
//!   ingestor's entity declares, and starting an ingest group of its own for each batch.
//! - **Depends on.** The connector ingestor contract's poll result and the ingestor host's
//!   unacknowledged intake, quiesce control and ingest group flush.
//! - **Must not know.** A connector driver, the domain cadence that schedules the poll, or the
//!   paced loop's readiness and retry decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use nervix_connector::IngestorPollMessage;

use super::*;

/// The intake batches one poll's messages enter the graph in: consecutive runs of at most `bound`
/// messages, in the order the ingestor returned them, each observed at the poll's instant.
pub(super) struct PollIntakeBatches {
    messages: std::vec::IntoIter<IngestorPollMessage>,
    bound: NonZeroUsize,
    observed_at: Timestamp,
}

impl PollIntakeBatches {
    pub(super) fn new(
        messages: Vec<IngestorPollMessage>,
        bound: NonZeroUsize,
        observed_at: Timestamp,
    ) -> Self {
        Self {
            messages: messages.into_iter(),
            bound,
            observed_at,
        }
    }
}

impl Iterator for PollIntakeBatches {
    type Item = BufferedIngestPayload;

    fn next(&mut self) -> Option<Self::Item> {
        let entries: Vec<_> = self
            .messages
            .by_ref()
            .take(self.bound.get())
            .map(|message| {
                let metadata = BufferedIngestMetadata::polled(message.metadata, message.headers);
                (message.payload, metadata)
            })
            .collect();
        if entries.is_empty() {
            return None;
        }
        Some(BufferedIngestPayload::batch(entries, self.observed_at))
    }
}

impl RuntimeIngestorHost {
    /// Takes in the messages `poll` returned, in intake batches of at most `intake_batch` messages.
    /// Each batch after the first starts an ingest group of its own: the group holding the batch
    /// before it flushes first, so a large poll is never one group of any size. Answers whether any
    /// message entered an ingest group; the caller flushes the group holding the last.
    pub(super) async fn intake_poll(
        &mut self,
        poll: IngestorPoll,
        intake_batch: NonZeroUsize,
    ) -> IngestorIntakeResult<bool> {
        if poll.dropped_header_values > 0 {
            self.header_values_dropped
                .inc_by(poll.dropped_header_values.arch_into());
        }
        for failure in poll.failures {
            nervix_primitives::task::consume_budget().await;
            self.report_error(format!(
                "ingestor poll could not materialize one message: {failure:?}"
            ));
        }
        let mut entered = false;
        for payload in PollIntakeBatches::new(poll.messages, intake_batch, poll.observed_at) {
            nervix_primitives::task::consume_budget().await;
            if entered {
                self.flush().await?;
            }
            if self.intake_unacknowledged_payload(payload).await? {
                entered = true;
            }
        }
        Ok(entered)
    }
}
