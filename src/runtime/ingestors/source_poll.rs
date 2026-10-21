//! A paced poll's intake: how the messages one poll returned enter the graph.
//!
//! Layer: data plane.
//!
//! - **Owns.** Reporting the records a poll could not read and taking the messages it did read in
//!   as one unacknowledged payload.
//! - **Depends on.** The connector source contract's poll result and the source host's
//!   unacknowledged intake.
//! - **Must not know.** A connector driver, the domain cadence that schedules the poll, or the
//!   paced loop's readiness and retry decisions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use super::*;

impl RuntimeSourceHost {
    pub(super) async fn intake_poll(&mut self, poll: SourcePoll) -> SourceIntakeResult<bool> {
        for failure in poll.failures {
            nervix_primitives::task::consume_budget().await;
            self.report_error(format!(
                "source poll could not materialize one message: {failure:?}"
            ));
        }
        if poll.messages.is_empty() {
            return Ok(false);
        }
        let entries = poll
            .messages
            .into_iter()
            .map(|message| {
                (
                    message.payload,
                    BufferedIngestMetadata::Headers(message.headers),
                )
            })
            .collect();
        let payload = BufferedIngestPayload::batch(entries, poll.observed_at);
        self.intake_unacknowledged_payload(payload).await
    }
}
