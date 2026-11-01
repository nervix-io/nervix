//! The pull a NATS STREAM ingestor instance reads its durable consumer's deliveries through.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** One instance's connection, the inbox its pull requests deliver to, the pull request
//!   outstanding on it, gathering a requested batch with its physical deadline and cancellation
//!   handover, a transport failure deferred until every gathered delivery has reached the host,
//!   the acknowledgement and negative acknowledgement of a delivery, and returning what is in
//!   transit when the instance lets its pull go.
//! - **Depends on.** The connector read contract, the primitive clock and channels, and the
//!   `async-nats` core client and pull consumer.
//! - **Must not know.** Graphs, acknowledgement trees, placement, or Plexus Models.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns one instance's pull of its durable consumer")
)]

use std::{num::NonZeroUsize, time::Duration};

use async_nats::{
    Client as NatsClient, Message as NatsMessage, StatusCode, Subject, Subscriber,
    jetstream::consumer::{Config as ConsumerConfig, PullConsumer, pull::BatchConfig},
};
use error_stack::Report;
use futures_util::StreamExt as _;
use nervix_connector::{IngestorBatch, IngestorBatchRequest, IngestorReadCancellation};
use nervix_primitives::{
    sync::mpsc,
    time::{Instant, timeout_at},
};
use nervix_recovery::Reported as _;

use super::ingestor::{
    NatsPullRefusal, NatsStreamIngestorError, NatsStreamIngestorMessage,
    NatsStreamIngestorPosition, NatsStreamIngestorResult,
};

/// What a server answers an acknowledgement with: the delivery is processed.
const ACKNOWLEDGED: &[u8] = b"+ACK";
/// What a server answers a negative acknowledgement with: the delivery is to be delivered again
/// at once.
const NOT_ACKNOWLEDGED: &[u8] = b"-NAK";

/// What the consumer an instance pulls from lets one pull request ask for, as its operator
/// configured it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NatsPullLimits {
    /// The most deliveries one pull request may ask for, where the consumer sets a
    /// `MaxRequestBatch`.
    batch: Option<NonZeroUsize>,
    /// The longest a pull request may wait for deliveries, where the consumer sets a
    /// `MaxRequestExpires`. A pull request waits without expiry otherwise.
    expires: Option<Duration>,
}

impl NatsPullLimits {
    /// The limits `config` sets. JetStream reads a nonpositive `MaxRequestBatch` and a zero
    /// `MaxRequestExpires` as no limit.
    pub(super) fn of(config: &ConsumerConfig) -> Self {
        let batch = match usize::try_from(config.max_batch) {
            Ok(batch) => NonZeroUsize::new(batch),
            // A negative limit is JetStream's spelling of no limit, and one beyond the address
            // space limits nothing a request can ask for.
            Err(_) => None,
        };
        let expires = if config.max_expires.is_zero() {
            None
        } else {
            Some(config.max_expires)
        };
        Self { batch, expires }
    }

    /// The deliveries one pull request asks for when the host can take `wanted` of them.
    pub(super) fn batch_of(self, wanted: NonZeroUsize) -> NonZeroUsize {
        match self.batch {
            Some(limit) => wanted.min(limit),
            None => wanted,
        }
    }
}

/// What a message on the pull's inbox is.
#[derive(Debug, PartialEq, Eq)]
enum PullStatus {
    /// A delivery of the consumer.
    Delivery,
    /// The server ended the pull request: it expired, or it found nothing to deliver at once. A
    /// new pull request asks again.
    Ended,
    /// An idle heartbeat, which says only that the pull request is still waiting.
    Heartbeat,
    /// The server refused the pull request with this status, or ended it in a way a new pull
    /// request on the same connection does not repair, such as a deleted consumer or a leadership
    /// change.
    Refused(StatusCode),
}

impl PullStatus {
    fn of(message: &NatsMessage) -> Self {
        let Some(status) = message.status else {
            return Self::Delivery;
        };
        if status == StatusCode::OK {
            return Self::Delivery;
        }
        if status == StatusCode::IDLE_HEARTBEAT {
            return Self::Heartbeat;
        }
        if status == StatusCode::TIMEOUT || status == StatusCode::NOT_FOUND {
            return Self::Ended;
        }
        Self::Refused(status)
    }
}

/// What one wait on the pull's inbox ended with.
enum PullReceipt {
    /// A delivery of the consumer, as the client received it.
    Delivery(NatsMessage),
    /// The pull request ended without the deliveries it asked for, so the next one asks again.
    Ended,
}

/// One instance's pull of its durable consumer: the connection it pulls on, the consumer, the
/// inbox every pull request delivers to, and how many deliveries the pull request outstanding on
/// that inbox may still bring.
///
/// At most one pull request is outstanding, and a new one is sent only once the previous one has
/// brought everything it asked for or ended, so the instance never holds more deliveries ahead of
/// the host than one request asks for.
pub(super) struct NatsStreamPull {
    client: NatsClient,
    consumer: PullConsumer,
    inbox: Subject,
    deliveries: Subscriber,
    /// Receives a signal when the client loses its connection to the server, and ends once the
    /// client's connection task has ended.
    connection_lost: mpsc::Receiver<()>,
    limits: NatsPullLimits,
    /// The deliveries the outstanding pull request may still bring, zero when none is outstanding.
    outstanding: usize,
    /// A failure found while gathering a batch that already held deliveries, returned by the next
    /// read once those deliveries have reached the host.
    failure: Option<Report<NatsStreamIngestorError>>,
}

impl NatsStreamPull {
    pub(super) fn new(
        client: NatsClient,
        consumer: PullConsumer,
        deliveries: Subscriber,
        inbox: Subject,
        connection_lost: mpsc::Receiver<()>,
        limits: NatsPullLimits,
    ) -> Self {
        Self {
            client,
            consumer,
            inbox,
            deliveries,
            connection_lost,
            limits,
            outstanding: 0,
            failure: None,
        }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "polls the connector-owned inbox subscription at its driver \
                                   boundary; the pull owns the batch, deadline and cancellation \
                                   handover")
    )]
    pub(super) async fn read(
        &mut self,
        request: IngestorBatchRequest,
        cancellation: &IngestorReadCancellation,
    ) -> NatsStreamIngestorResult<IngestorBatch<NatsStreamIngestorMessage>> {
        if let Some(failure) = self.failure.take() {
            return Err(failure);
        }
        let wanted = self.limits.batch_of(request.max_messages);
        // The batch grows with what arrives: a statement's window bounds it, and reserving that
        // window up front would allocate for deliveries that may never come.
        let mut gathered = Vec::new();
        loop {
            nervix_primitives::task::consume_budget().await;
            self.request_deliveries(wanted).await?;
            let Some(receipt) = cancellation.unless_cancelled(self.next_receipt()).await else {
                return Ok(IngestorBatch::Interrupted { gathered });
            };
            match receipt? {
                PullReceipt::Delivery(message) => {
                    gathered.push(NatsStreamIngestorMessage::delivered(message)?);
                    break;
                }
                PullReceipt::Ended => {}
            }
        }
        if cancellation.is_cancelled() {
            return Ok(IngestorBatch::Interrupted { gathered });
        }
        let Some(batch_timeout) = request.batch_timeout else {
            return Ok(IngestorBatch::Messages(gathered));
        };
        let Some(deadline) = Instant::now().checked_add(batch_timeout) else {
            self.failure = Some(Report::new(NatsStreamIngestorError::BatchDeadline));
            return Ok(IngestorBatch::Messages(gathered));
        };
        // A batch gathers only what the pull request that brought its first delivery still brings,
        // so the instance never asks for more than one request ahead of the host.
        while gathered.len() < wanted.get() && self.outstanding > 0 {
            nervix_primitives::task::consume_budget().await;
            if cancellation.is_cancelled() {
                return Ok(IngestorBatch::Interrupted { gathered });
            }
            let next = cancellation
                .unless_cancelled(timeout_at(deadline, self.next_receipt()))
                .await;
            let Some(received) = next else {
                return Ok(IngestorBatch::Interrupted { gathered });
            };
            let Ok(receipt) = received else {
                break;
            };
            let message = match receipt {
                Ok(PullReceipt::Delivery(message)) => message,
                Ok(PullReceipt::Ended) => break,
                Err(failure) => {
                    self.failure = Some(failure);
                    break;
                }
            };
            match NatsStreamIngestorMessage::delivered(message) {
                Ok(delivered) => gathered.push(delivered),
                Err(failure) => {
                    self.failure = Some(failure);
                    break;
                }
            }
        }
        Ok(IngestorBatch::Messages(gathered))
    }

    /// Sends a pull request for `wanted` deliveries unless one is still outstanding.
    async fn request_deliveries(&mut self, wanted: NonZeroUsize) -> NatsStreamIngestorResult<()> {
        if self.outstanding > 0 {
            return Ok(());
        }
        let request = BatchConfig {
            batch: wanted.get(),
            expires: self.limits.expires,
            ..BatchConfig::default()
        };
        self.consumer
            .request_batch(request, self.inbox.clone())
            .await
            .map_err(|source| Report::new(source).change_context(NatsStreamIngestorError::Pull))?;
        self.outstanding = wanted.get();
        Ok(())
    }

    /// Waits for what the inbox carries next that the read acts on, or for the connection to end.
    ///
    /// A delivery the client already received is taken before a lost connection ends the pull, so
    /// a loss never discards what reached the client before it.
    async fn next_receipt(&mut self) -> NatsStreamIngestorResult<PullReceipt> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let received = nervix_primitives::select! {
                biased;
                message = self.deliveries.next() => message,
                _ = self.connection_lost.recv() => None,
            };
            // The client ends a subscription's stream only when its connection task ended, which
            // a lost connection the client did not restore does.
            let Some(message) = received else {
                return Err(Report::new(NatsStreamIngestorError::ConnectionLost));
            };
            match PullStatus::of(&message) {
                PullStatus::Delivery => {
                    let Some(remaining) = self.outstanding.checked_sub(1) else {
                        return Err(Report::new(NatsStreamIngestorError::UnrequestedDelivery));
                    };
                    self.outstanding = remaining;
                    return Ok(PullReceipt::Delivery(message));
                }
                PullStatus::Ended => {
                    self.outstanding = 0;
                    return Ok(PullReceipt::Ended);
                }
                PullStatus::Heartbeat => {}
                PullStatus::Refused(status) => {
                    self.outstanding = 0;
                    let refusal = NatsPullRefusal {
                        status: status.as_u16(),
                        description: message.description,
                    };
                    return Err(Report::new(NatsStreamIngestorError::PullRefused {
                        refusal,
                    }));
                }
            }
        }
    }

    /// Acknowledges the delivery at `position` and returns once the server answered.
    pub(super) async fn acknowledge(
        &self,
        position: &NatsStreamIngestorPosition,
    ) -> NatsStreamIngestorResult<()> {
        self.client
            .request(position.acknowledgement_subject(), ACKNOWLEDGED.into())
            .await
            .map_err(|source| {
                Report::new(source).change_context(NatsStreamIngestorError::Acknowledge)
            })?;
        Ok(())
    }

    /// Negatively acknowledges the delivery at `position`, so the consumer delivers it again at
    /// once, and returns once the server answered.
    pub(super) async fn reject(
        &self,
        position: &NatsStreamIngestorPosition,
    ) -> NatsStreamIngestorResult<()> {
        self.client
            .request(position.acknowledgement_subject(), NOT_ACKNOWLEDGED.into())
            .await
            .map_err(|source| {
                Report::new(source).change_context(NatsStreamIngestorError::NegativelyAcknowledge)
            })?;
        Ok(())
    }

    /// Lets the pull go when the instance suspends or closes, returning to the consumer every
    /// delivery the client already received for it.
    ///
    /// While a pull request is outstanding a delivery may wait in the inbox or travel toward it.
    /// Draining the inbox unsubscribes it, so the server delivers nothing more to it, and the client
    /// hands over every delivery it had received before it ended the subscription. Each is
    /// negatively acknowledged, so the consumer delivers it again at once rather than once its
    /// `AckWait` passes. The client ends the subscription as soon as it has written the
    /// unsubscription, so a delivery still travelling then is dropped by the client and delivered
    /// again once that `AckWait` passes.
    pub(super) async fn release(mut self) {
        if self.outstanding == 0 {
            return;
        }
        if self.deliveries.drain().await.is_err() {
            // The client's connection task has ended: the server delivers again what it sent once
            // the consumer's AckWait passes, and nothing more reaches this inbox.
            return;
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            let received = nervix_primitives::select! {
                biased;
                message = self.deliveries.next() => message,
                _ = self.connection_lost.recv() => None,
            };
            let Some(message) = received else {
                break;
            };
            if PullStatus::of(&message) != PullStatus::Delivery {
                continue;
            }
            let Some(reply) = message.reply else {
                continue;
            };
            self.client
                .publish(reply, NOT_ACKNOWLEDGED.into())
                .await
                .reported("returning a received NATS JetStream delivery to its consumer");
        }
        self.client
            .flush()
            .await
            .reported("writing the negative acknowledgements of a released NATS JetStream pull");
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use async_nats::Subject;
    use bytes::Bytes;
    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    fn limits(max_batch: i64, max_expires: Duration) -> NatsPullLimits {
        NatsPullLimits::of(&ConsumerConfig {
            max_batch,
            max_expires,
            ..ConsumerConfig::default()
        })
    }

    #[test]
    fn a_consumer_without_request_limits_lets_a_pull_ask_for_the_hosts_whole_window() {
        let window = NonZeroUsize::new(64).assured("a fixed window of 64 is positive");
        for unlimited in [limits(0, Duration::ZERO), limits(-1, Duration::ZERO)] {
            assert_eq!(unlimited.batch_of(window), window);
            assert_eq!(unlimited.expires, None);
        }
    }

    #[test]
    fn a_pull_asks_for_at_most_the_consumers_max_request_batch_and_waits_its_max_expires() {
        let limited = limits(8, Duration::from_secs(2));
        let window = NonZeroUsize::new(64).assured("a fixed window of 64 is positive");
        let below = NonZeroUsize::new(3).assured("a fixed window of 3 is positive");
        assert_eq!(limited.batch_of(window).get(), 8);
        assert_eq!(limited.batch_of(below), below);
        assert_eq!(limited.expires, Some(Duration::from_secs(2)));
    }

    fn message(status: Option<StatusCode>) -> NatsMessage {
        NatsMessage {
            subject: Subject::from("_INBOX.pull"),
            reply: None,
            payload: Bytes::new(),
            headers: None,
            status,
            description: None,
            length: 0,
        }
    }

    #[test]
    fn the_inbox_tells_a_delivery_from_the_pull_requests_statuses() {
        assert_eq!(PullStatus::of(&message(None)), PullStatus::Delivery);
        assert_eq!(
            PullStatus::of(&message(Some(StatusCode::OK))),
            PullStatus::Delivery
        );
        assert_eq!(
            PullStatus::of(&message(Some(StatusCode::IDLE_HEARTBEAT))),
            PullStatus::Heartbeat
        );
        assert_eq!(
            PullStatus::of(&message(Some(StatusCode::TIMEOUT))),
            PullStatus::Ended
        );
        assert_eq!(
            PullStatus::of(&message(Some(StatusCode::NOT_FOUND))),
            PullStatus::Ended
        );
        let bad_request =
            StatusCode::from_u16(400).assured("400 is a status code the protocol defines");
        for refused in [
            StatusCode::REQUEST_TERMINATED,
            StatusCode::NO_RESPONDERS,
            bad_request,
        ] {
            assert_eq!(
                PullStatus::of(&message(Some(refused))),
                PullStatus::Refused(refused)
            );
        }
    }

    #[test]
    fn a_delivery_is_acknowledged_on_the_subject_it_arrived_with() {
        let mut delivery = message(None);
        delivery.reply = Some(Subject::from("$JS.ACK.orders.nervix.1.7.3.0.0"));
        let delivered = NatsStreamIngestorMessage::delivered(delivery)
            .assured("a delivery with a reply subject can be acknowledged");
        assert_eq!(
            nervix_connector::IngestorMessage::position(&delivered)
                .acknowledgement_subject()
                .as_str(),
            "$JS.ACK.orders.nervix.1.7.3.0.0"
        );
        let Err(refused) = NatsStreamIngestorMessage::delivered(message(None)) else {
            panic!("a delivery without a reply subject cannot be acknowledged");
        };
        assert!(matches!(
            refused.current_context(),
            NatsStreamIngestorError::UnacknowledgeableDelivery
        ));
    }
}
