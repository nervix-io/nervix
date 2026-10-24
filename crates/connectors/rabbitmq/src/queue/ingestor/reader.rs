//! The RabbitMQ consumer's batch read owner.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Gathering a requested batch, its physical deadline, cancellation handover, and a
//!   transport failure deferred until every gathered delivery has reached the host.
//! - **Depends on.** The connector read contract, the primitive clock, and the consumer stream.
//! - **Must not know.** Graphs, acknowledgement trees, placement, or Plexus Models.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns bounded consumer batch reads")
)]

use error_stack::Report;
use lapin::{Error, message::Delivery};
use nervix_connector::{IngestorBatch, IngestorBatchRequest, IngestorReadCancellation};
use nervix_primitives::{
    stream::{Stream, StreamExt as _},
    time::{Instant, timeout_at},
};

use super::{
    RabbitMqQueueIngestorError, RabbitMqQueueIngestorMessage, RabbitMqQueueIngestorResult,
};

pub(super) struct RabbitMqBatchReader<S> {
    stream: S,
    failure: Option<Report<RabbitMqQueueIngestorError>>,
}

impl<S> RabbitMqBatchReader<S>
where
    S: Stream<Item = Result<Delivery, Error>> + Unpin,
{
    pub(super) fn new(stream: S) -> Self {
        Self {
            stream,
            failure: None,
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "polls the connector-owned consumer stream at its driver \
                                   boundary; the reader owns the batch, deadline and \
                                   cancellation handover")
    )]
    pub(super) async fn read(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> RabbitMqQueueIngestorResult<IngestorBatch<RabbitMqQueueIngestorMessage>> {
        if let Some(failure) = self.failure.take() {
            return Err(failure);
        }
        let Some(first) = cancellation.unless_cancelled(self.stream.next()).await else {
            return Ok(IngestorBatch::Interrupted {
                gathered: Vec::new(),
            });
        };
        let first = Self::delivery(first)?;
        let mut gathered = Vec::with_capacity(request.max_messages.get());
        gathered.push(RabbitMqQueueIngestorMessage::new(first));
        if cancellation.is_cancelled() {
            return Ok(IngestorBatch::Interrupted { gathered });
        }
        if request.max_messages.get() == 1 {
            return Ok(IngestorBatch::Messages(gathered));
        }
        let Some(batch_timeout) = request.batch_timeout else {
            return Ok(IngestorBatch::Messages(gathered));
        };
        let Some(deadline) = Instant::now().checked_add(batch_timeout) else {
            self.failure = Some(Report::new(RabbitMqQueueIngestorError::BatchDeadline));
            return Ok(IngestorBatch::Messages(gathered));
        };
        while gathered.len() < request.max_messages.get() {
            nervix_primitives::task::consume_budget().await;
            if cancellation.is_cancelled() {
                return Ok(IngestorBatch::Interrupted { gathered });
            }
            let next = cancellation
                .unless_cancelled(timeout_at(deadline, self.stream.next()))
                .await;
            let Some(received) = next else {
                return Ok(IngestorBatch::Interrupted { gathered });
            };
            let Ok(received) = received else {
                break;
            };
            match Self::delivery(received) {
                Ok(delivery) => gathered.push(RabbitMqQueueIngestorMessage::new(delivery)),
                Err(failure) => {
                    self.failure = Some(failure);
                    break;
                }
            }
        }
        Ok(IngestorBatch::Messages(gathered))
    }

    fn delivery(
        received: Option<Result<Delivery, Error>>,
    ) -> RabbitMqQueueIngestorResult<Delivery> {
        match received {
            Some(Ok(delivery)) => Ok(delivery),
            Some(Err(cause)) => {
                Err(Report::new(cause).change_context(RabbitMqQueueIngestorError::Receive))
            }
            None => Err(Report::new(RabbitMqQueueIngestorError::ConsumerClosed)),
        }
    }
}

#[cfg(all(test, not(feature = "shuttle")))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests the production batch reader")
)]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "explores production batch interleavings")
)]
mod shuttle_tests;
