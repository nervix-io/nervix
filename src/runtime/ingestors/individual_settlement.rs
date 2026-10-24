//! Per-delivery settlement of one independently acknowledged source batch.
//!
//! Layer: data plane.
//! - **Owns.** Concurrent graph waits, each delivery's confirmation lifetime and unresolved
//!   positions retained after a transport failure.
//! - **Depends on.** The connector contract and the broker loop's typed settlement operations.
//! - **Must not know.** A broker driver, configuration Models, placement or wire formats.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns individual source settlement")
)]

use super::*;

enum DeliveryOutcome {
    Graph {
        outcome: IngestorAcknowledgementOutcome,
        // Kept through the transport operation, including after this root resolved.
        acknowledgement: IngestorBatchAcknowledgement,
    },
    IntakeRefused,
}

struct SettledDelivery {
    index: usize,
    outcome: DeliveryOutcome,
}

pub(super) struct IndividualBatch<P> {
    // The batch is a sequence. An index names exactly the position of its one graph wait;
    // taking it removes it from recovery before the next wait can settle.
    positions: Vec<Option<P>>,
    waits: FuturesUnordered<BoxFuture<'static, SettledDelivery>>,
}

impl<P> IndividualBatch<P>
where
    P: Clone + Send + Sync + 'static,
{
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "source messages lend payloads and metadata while positions \
                                   are cloned once into batch-owned recovery storage")
    )]
    pub(super) async fn intake<M>(
        host: &mut IngestorHost,
        messages: Vec<M>,
        timeout: Duration,
    ) -> Self
    where
        M: IngestorMessage<Position = P>,
    {
        let batch = Self {
            positions: messages
                .iter()
                .map(|message| Some(message.position().clone()))
                .collect(),
            waits: FuturesUnordered::new(),
        };
        for (index, message) in messages.into_iter().enumerate() {
            nervix_primitives::task::consume_budget().await;
            let intake = IngestorIntakeMessage {
                payload: message.payload(),
                metadata: message.metadata(),
            };
            match host.intake_acknowledged(vec![intake]).await {
                Ok(mut acknowledgement) => {
                    batch.waits.push(Box::pin(async move {
                        let outcome = acknowledgement.wait(timeout).await;
                        SettledDelivery {
                            index,
                            outcome: DeliveryOutcome::Graph {
                                outcome,
                                acknowledgement,
                            },
                        }
                    }));
                }
                Err(error) => {
                    host.report_error(format!("{error:#}"));
                    batch.waits.push(Box::pin(async move {
                        SettledDelivery {
                            index,
                            outcome: DeliveryOutcome::IntakeRefused,
                        }
                    }));
                }
            }
        }
        batch
    }

    pub(super) async fn settle<C>(
        mut self,
        source: &mut C,
        host: &mut IngestorHost,
        settlement: BatchSettlement,
    ) -> BatchDisposition<P>
    where
        C: BrokerIngestorConnector<Position = P>,
    {
        let mut retry = false;
        let mut retained: Option<Vec<P>> = None;
        while let Some(delivery) = self.waits.next().await {
            nervix_primitives::task::consume_budget().await;
            let position = self
                .positions
                .get_mut(delivery.index)
                .assured("every completed wait names its index in this batch")
                .take()
                .assured("one wait completes once for each batch position");
            if let Some(positions) = retained.as_mut() {
                positions.push(position);
                match delivery.outcome {
                    DeliveryOutcome::Graph {
                        outcome: IngestorAcknowledgementOutcome::Shutdown,
                        acknowledgement,
                    } => {
                        let outstanding = positions.len().checked_add(self.waits.len()).assured(
                            "retained positions and pending waits partition the original batch",
                        );
                        host.record_unconfirmed_stop(outstanding);
                        drop(acknowledgement);
                        return BatchDisposition::Shutdown;
                    }
                    DeliveryOutcome::Graph {
                        outcome: IngestorAcknowledgementOutcome::NoAck(reason),
                        acknowledgement,
                    } => {
                        host.handle_ack_failure(reason);
                        drop(acknowledgement);
                    }
                    DeliveryOutcome::Graph {
                        acknowledgement, ..
                    } => drop(acknowledgement),
                    DeliveryOutcome::IntakeRefused => {}
                }
                continue;
            }
            let disposition = match delivery.outcome {
                DeliveryOutcome::Graph {
                    outcome: IngestorAcknowledgementOutcome::Ack,
                    acknowledgement,
                } => {
                    let disposition = confirm_batch(source, host, settlement, vec![position]).await;
                    drop(acknowledgement);
                    disposition
                }
                DeliveryOutcome::Graph {
                    outcome: IngestorAcknowledgementOutcome::NoAck(reason),
                    acknowledgement,
                } => {
                    host.handle_ack_failure(reason);
                    let disposition = reject_batch(source, host, settlement, vec![position]).await;
                    drop(acknowledgement);
                    disposition
                }
                DeliveryOutcome::Graph {
                    outcome: IngestorAcknowledgementOutcome::Shutdown,
                    acknowledgement,
                } => {
                    let outstanding = self.waits.len().checked_add(1).assured(
                        "the completed wait plus pending waits fit the original batch length",
                    );
                    host.record_unconfirmed_stop(outstanding);
                    drop(acknowledgement);
                    return BatchDisposition::Shutdown;
                }
                DeliveryOutcome::IntakeRefused => {
                    reject_batch(source, host, settlement, vec![position]).await
                }
            };
            match disposition {
                BatchDisposition::Accepted => {}
                BatchDisposition::Retry => retry = true,
                // The transport cannot settle the remaining positions. Keep their graph waits
                // owned until they complete before opening a consumer that replays their work.
                BatchDisposition::RetryRejection(positions) => retained = Some(positions),
                BatchDisposition::Shutdown => return BatchDisposition::Shutdown,
            }
        }
        if let Some(positions) = retained {
            BatchDisposition::RetryRejection(positions)
        } else if retry {
            BatchDisposition::Retry
        } else {
            BatchDisposition::Accepted
        }
    }
}
