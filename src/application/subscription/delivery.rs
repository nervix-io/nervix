//! Delivering one subscription generation's rows to its session.
//!
//! Layer: control plane.
//!
//! - **Owns.** One generation's delivery from the moment it attaches: holding its rows back until
//!   the reply that announces it is queued, selecting and encoding the rows of each relay batch,
//!   queueing them as its delivery behavior asks, reporting the rows it skipped or dropped, ending
//!   it with a typed reason when its relay closes, and releasing the relay receiver and the
//!   interest lease it holds.
//! - **Depends on.** The runtime's relay receiver, subscription predicate and domain time, the Row
//!   encoder, the session's subscription lane, the interest lease, and the schedule it reads an
//!   end reason from.
//! - **Must not know.** Requests, replies, or how a transport writes frames.
//!
//! A generation stops in exactly one of three ways, and each releases the relay receiver and the
//! interest lease before anything waits on the client:
//!
//! - it is never announced, because the reply that opened it was not queued, and it sends nothing;
//! - it is withdrawn, because its client deleted it or its session ended, and it sends nothing
//!   more;
//! - its relay closes, because the relay was redefined or removed, and it sends a
//!   `SubscriptionEnded` naming why as the last frame of the generation.

use std::num::NonZeroU64;

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_client_wire::{
    EncodedFrame, ServerFrame, SessionLimits, SubscriptionDeliveryLost, SubscriptionEndReason,
    SubscriptionEnded, SubscriptionHandle, SubscriptionRowsSkipped, WireEncodeError,
};
use nervix_models::{DomainName, RelayName, SubscriptionDeliveryBehavior};
use nervix_recovery::NoReceiver as _;
use tokio::sync::oneshot;
use tracing::debug;

use super::{SkippedRows, interest::SubscriptionInterestLease, select_subscription_rows};
use crate::{
    application::{
        session::outbound::{LaneClosed, LaneRefusal, SubscriptionLane},
        session_service::SessionServiceImpl,
    },
    metrics::RuntimeMetrics,
    runtime::{CompiledSubscriptionPredicate, RelayRecordBatch, RelaySubscriptionReceiver},
    subscription_row::{SubscriptionRowEncoder, SubscriptionRowFrame, SubscriptionRowSelection},
};

/// Everything one generation's delivery owns.
pub(super) struct SubscriptionDelivery {
    pub(super) handle: SubscriptionHandle,
    pub(super) domain: DomainName,
    pub(super) relay: RelayName,
    pub(super) predicate: Option<CompiledSubscriptionPredicate>,
    pub(super) behavior: SubscriptionDeliveryBehavior,
    pub(super) batch_sample_rate: Option<f64>,
    pub(super) receiver: RelaySubscriptionReceiver<RelayRecordBatch>,
    pub(super) encoder: SubscriptionRowEncoder,
    pub(super) lane: SubscriptionLane,
    pub(super) limits: SessionLimits,
    pub(super) lease: SubscriptionInterestLease,
    /// The runtime the predicate reads domain time from, and the schedule an end reason is read
    /// from.
    pub(super) service: SessionServiceImpl,
}

/// How the wait for a generation's announcement ended.
enum Announcement {
    /// The reply that opens the generation is queued.
    Announced,
    /// The reply is queued, but the relay closed while it was on its way.
    AnnouncedClosed,
    /// The reply was not queued, or the generation was withdrawn before it was.
    Abandoned,
}

/// Why an announced generation stopped delivering.
enum DeliveryStop {
    /// The generation was withdrawn, or its session ended or stopped taking frames.
    Withdrawn,
    /// The relay closed the generation's receiver.
    RelayClosed,
}

/// The part of a generation's delivery that outlives its relay receiver and interest lease.
struct ActiveDelivery {
    domain: DomainName,
    relay: RelayName,
    predicate: Option<CompiledSubscriptionPredicate>,
    batch_sample_rate: Option<f64>,
    encoder: SubscriptionRowEncoder,
    service: SessionServiceImpl,
    sender: SubscriptionSender,
}

impl SubscriptionDelivery {
    /// Delivers the generation until it stops. `announced` fires once the reply that opens it is
    /// queued; dropping it instead abandons the generation.
    pub(super) async fn run(self, announced: oneshot::Receiver<()>) {
        let Self {
            handle,
            domain,
            relay,
            predicate,
            behavior,
            batch_sample_rate,
            mut receiver,
            encoder,
            lane,
            limits,
            lease,
            service,
        } = self;
        let announcement = wait_for_announcement(&lane, &mut receiver, announced).await;
        let losses = DeliveryLosses {
            metrics: service.inner.runtime.metrics(),
            domain: domain.clone(),
            relay: relay.clone(),
        };
        let mut delivery = ActiveDelivery {
            domain,
            relay,
            predicate,
            batch_sample_rate,
            encoder,
            service,
            sender: SubscriptionSender {
                handle,
                lane,
                limits,
                behavior,
                dropped_rows: 0,
                losses,
            },
        };
        let stop = match announcement {
            Announcement::Abandoned => DeliveryStop::Withdrawn,
            Announcement::AnnouncedClosed => DeliveryStop::RelayClosed,
            Announcement::Announced => delivery.deliver(&mut receiver).await,
        };
        drop(receiver);
        lease.release().await;
        if let DeliveryStop::RelayClosed = stop {
            delivery.report_end().await;
        }
    }
}

/// Waits until the reply that opens the generation is queued.
///
/// Rows published before then are not the generation's rows, since nothing has told the client
/// their schema yet. They are taken and dropped, so the relay never waits on a subscription that
/// does not deliver yet.
async fn wait_for_announcement(
    lane: &SubscriptionLane,
    receiver: &mut RelaySubscriptionReceiver<RelayRecordBatch>,
    mut announced: oneshot::Receiver<()>,
) -> Announcement {
    let mut relay_open = true;
    loop {
        tokio::task::consume_budget().await;
        tokio::select! {
            biased;
            _ = lane.withdrawn() => return Announcement::Abandoned,
            released = &mut announced => {
                if released.is_err() {
                    return Announcement::Abandoned;
                }
                if relay_open {
                    return Announcement::Announced;
                }
                return Announcement::AnnouncedClosed;
            }
            batch = receiver.recv(), if relay_open => {
                if batch.is_none() {
                    relay_open = false;
                }
            }
        }
    }
}

impl ActiveDelivery {
    /// Delivers every batch the relay publishes until the generation is withdrawn or the relay
    /// closes.
    async fn deliver(
        &mut self,
        receiver: &mut RelaySubscriptionReceiver<RelayRecordBatch>,
    ) -> DeliveryStop {
        loop {
            tokio::task::consume_budget().await;
            let batch = tokio::select! {
                biased;
                _ = self.sender.lane.withdrawn() => return DeliveryStop::Withdrawn,
                batch = receiver.recv() => batch,
            };
            let Some(batch) = batch else {
                return DeliveryStop::RelayClosed;
            };
            if self.deliver_batch(&batch).await.is_err() {
                return DeliveryStop::Withdrawn;
            }
        }
    }

    /// Selects, encodes and queues the rows of one batch. An error means the generation can no
    /// longer deliver.
    async fn deliver_batch(&mut self, batch: &RelayRecordBatch) -> Result<(), LaneClosed> {
        let selection = select_subscription_rows(
            batch,
            self.predicate.as_ref(),
            self.batch_sample_rate,
            &self.service.inner.runtime,
            &self.domain,
        )
        .await;
        if let Some(skipped) = selection.skipped {
            self.sender.report_skipped(skipped).await?;
        }
        if selection.rows.is_empty() {
            return Ok(());
        }
        let encoded = self.encoder.encode(
            batch.record_batch(),
            batch.branch_keys(),
            SubscriptionRowSelection::Rows(&selection.rows),
        );
        let frames = match encoded {
            Ok(frames) => frames,
            Err(error) => {
                let rows = NonZeroU64::new(
                    u64::try_from(selection.rows.len())
                        .assured("supported targets have a pointer width no larger than u64"),
                )
                .verified("the empty selection above already returned");
                let skipped = SkippedRows {
                    cause: nervix_client_wire::RowsSkippedCause::EncodingFailed,
                    rows,
                    message: format!(
                        "session subscription '{}' could not encode rows of relay '{}': {error}",
                        self.sender.handle.name, self.relay,
                    ),
                };
                return self.sender.report_skipped(skipped).await;
            }
        };
        for frame in frames {
            tokio::task::consume_budget().await;
            self.sender.send_rows(frame).await?;
        }
        Ok(())
    }

    /// Tells the client why the server ended the generation. It is the generation's last frame.
    async fn report_end(&self) {
        let reason = self.end_reason().await;
        let message = match reason {
            SubscriptionEndReason::RelayRemoved => format!(
                "session subscription '{}' ended because relay '{}' in domain '{}' no longer \
                 exists",
                self.sender.handle.name, self.relay, self.domain,
            ),
            SubscriptionEndReason::RelayChanged => format!(
                "session subscription '{}' ended because relay '{}' in domain '{}' was redefined; \
                 subscribe again to receive its rows under the current schema",
                self.sender.handle.name, self.relay, self.domain,
            ),
        };
        self.sender
            .report_end(reason, message)
            .await
            .means_peer_left("the session subscription's client");
    }

    /// Why the relay closed the generation's receiver. The runtime closes a subscriber only when
    /// the relay is withdrawn or declared with a different definition, so a relay the schedule
    /// still holds was redefined, or removed and declared again.
    async fn end_reason(&self) -> SubscriptionEndReason {
        let target = self
            .service
            .subscription_target_from_schedule(&self.domain, &self.relay)
            .await;
        match target {
            Ok(None) => SubscriptionEndReason::RelayRemoved,
            Ok(Some(_)) | Err(_) => SubscriptionEndReason::RelayChanged,
        }
    }
}

/// Queues one generation's frames on its session's subscription lane.
struct SubscriptionSender {
    handle: SubscriptionHandle,
    lane: SubscriptionLane,
    limits: SessionLimits,
    behavior: SubscriptionDeliveryBehavior,
    /// Rows a dropping subscription discarded that its client has not been told about yet.
    dropped_rows: u64,
    losses: DeliveryLosses,
}

/// Where a dropping subscription records the rows it discards, for the node's operators.
struct DeliveryLosses {
    metrics: RuntimeMetrics,
    domain: DomainName,
    relay: RelayName,
}

impl SubscriptionSender {
    /// Queues a notice about the generation, waiting for room until it is withdrawn.
    async fn send_notice(
        &self,
        frame: Result<EncodedFrame<ServerFrame>, Report<WireEncodeError>>,
    ) -> Result<(), LaneClosed> {
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                debug!(
                    subscription = %self.handle.name,
                    error = %error,
                    "a subscription notice does not fit a session frame"
                );
                return Ok(());
            }
        };
        self.lane.send(frame).await
    }

    async fn report_skipped(&self, skipped: SkippedRows) -> Result<(), LaneClosed> {
        let frame = SubscriptionRowsSkipped {
            subscription: self.handle.clone(),
            cause: skipped.cause,
            skipped_rows: skipped.rows,
            message: skipped.message,
        }
        .encode(&self.limits);
        self.send_notice(frame).await
    }

    async fn report_end(
        &self,
        reason: SubscriptionEndReason,
        message: String,
    ) -> Result<(), LaneClosed> {
        let frame = SubscriptionEnded {
            subscription: self.handle.clone(),
            reason,
            message,
        }
        .encode(&self.limits);
        self.send_notice(frame).await
    }

    /// Queues one frame of rows. A blocking subscription waits for room; a dropping one discards
    /// the frame when the lane is full and reports the loss before the next rows it delivers.
    async fn send_rows(&mut self, frame: SubscriptionRowFrame) -> Result<(), LaneClosed> {
        let SubscriptionRowFrame { frame, rows } = frame;
        let rows = u64::try_from(rows.get())
            .assured("supported targets have a pointer width no larger than u64");
        match self.behavior {
            SubscriptionDeliveryBehavior::Blocking => self.lane.send(frame).await,
            SubscriptionDeliveryBehavior::Dropping => {
                if let Some(dropped_rows) = NonZeroU64::new(self.dropped_rows) {
                    let lost = SubscriptionDeliveryLost {
                        subscription: self.handle.clone(),
                        dropped_rows,
                    }
                    .encode(&self.limits);
                    match lost {
                        Ok(lost) => match self.lane.try_send(lost) {
                            Ok(()) => self.dropped_rows = 0,
                            // The loss is still unreported, so these rows cannot go ahead of it.
                            Err(LaneRefusal::Full) => {
                                self.count_dropped(rows);
                                return Ok(());
                            }
                            Err(LaneRefusal::Closed) => return Err(LaneClosed),
                        },
                        Err(error) => {
                            debug!(
                                subscription = %self.handle.name,
                                error = %error,
                                "a subscription loss report does not fit a session frame"
                            );
                            self.dropped_rows = 0;
                        }
                    }
                }
                match self.lane.try_send(frame) {
                    Ok(()) => Ok(()),
                    Err(LaneRefusal::Full) => {
                        self.count_dropped(rows);
                        Ok(())
                    }
                    Err(LaneRefusal::Closed) => Err(LaneClosed),
                }
            }
        }
    }

    fn count_dropped(&mut self, rows: u64) {
        self.dropped_rows = self.dropped_rows.checked_add(rows).assured(
            "the count restarts at every report, and no session drops 2^64 rows between two: at a \
             billion rows a second that takes centuries",
        );
        self.losses
            .metrics
            .increment_session_subscription_dropped_rows(
                &self.losses.domain,
                &self.losses.relay,
                rows,
            );
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc as StdArc, time::Duration};

    use arrow_array::{RecordBatch, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};
    use nervix_client_wire::{
        RowSchema, RowsSkippedCause, ServerEvent, ServerMessage, SubscriptionHandle, VerifiedFrame,
    };
    use nervix_models::{DomainName, RelayName, SchemaField, SubscriptionName};
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        application::{
            session::outbound::{self, SESSION_SUBSCRIPTION_CAPACITY, SessionFrames},
            test_fixtures::named,
        },
        subscription_row::SubscriptionRowOpening,
    };

    /// Generously longer than any step here takes, so only a hang reaches it.
    const WAIT: Duration = Duration::from_secs(30);

    fn handle() -> SubscriptionHandle {
        SubscriptionHandle {
            name: named::<SubscriptionName>("sampled_events"),
            generation: NonZeroU64::MIN,
        }
    }

    fn encoder() -> SubscriptionRowEncoder {
        let schema = RowSchema {
            fields: vec![SchemaField {
                name: named("user_id"),
                ty: nervix_models::ParseAsType::U32,
                optional: false,
                sensitive: false,
            }],
            branch: None,
        };
        let (_, encoder) = SubscriptionRowOpening::new(
            handle(),
            schema,
            SessionLimits::DEFAULT,
            NonZeroUsize::new(256).assured("a literal row limit is non-zero"),
        )
        .assured("the test row limit fits the default collection limit")
        .open(named::<DomainName>("default"), named::<RelayName>("events"));
        encoder
    }

    /// One frame holding a row for each of `user_ids`, as the subscription's encoder writes it.
    fn row_frame(encoder: &SubscriptionRowEncoder, user_ids: &[u32]) -> SubscriptionRowFrame {
        let schema = Schema::new(vec![Field::new("user_id", DataType::UInt32, false)]);
        let column = UInt32Array::from(user_ids.to_vec());
        let batch = RecordBatch::try_new(StdArc::new(schema), vec![StdArc::new(column)])
            .assured("the column matches the one-field schema");
        let unbranched = vec![None; user_ids.len()];
        let frames = encoder
            .encode(&batch, &unbranched, SubscriptionRowSelection::All)
            .assured("the test rows match the subscription's row schema");
        let Ok([frame]) = <[SubscriptionRowFrame; 1]>::try_from(frames) else {
            panic!("a handful of rows fits one frame");
        };
        frame
    }

    fn sender(
        behavior: SubscriptionDeliveryBehavior,
    ) -> (SubscriptionSender, SessionFrames, outbound::SessionOutbound) {
        let (outbound, frames) = outbound::channel(CancellationToken::new());
        let sender = SubscriptionSender {
            handle: handle(),
            lane: outbound.subscription_lane(),
            limits: SessionLimits::DEFAULT,
            behavior,
            dropped_rows: 0,
            losses: DeliveryLosses {
                metrics: crate::runtime::Runtime::default().metrics(),
                domain: named::<DomainName>("default"),
                relay: named::<RelayName>("events"),
            },
        };
        (sender, frames, outbound)
    }

    /// The next frame the transport would write, decoded.
    async fn next_event(frames: &mut SessionFrames) -> ServerEvent {
        let frame = timeout(WAIT, frames.next())
            .await
            .assured("the sender queues its frames before the test reads them")
            .assured("the test holds the session's producers");
        let frame = VerifiedFrame::verify(frame.into_bytes(), &SessionLimits::DEFAULT)
            .assured("the server encodes frames the session limits admit");
        let ServerMessage::Event(event) = ServerMessage::decode(&frame).assured("a frame decodes")
        else {
            panic!("a subscription sends only events");
        };
        event
    }

    fn delivered_rows(event: ServerEvent) -> usize {
        let ServerEvent::SubscriptionRows(rows) = event else {
            panic!("expected subscription rows, found {event:?}");
        };
        rows.batch().len()
    }

    #[tokio::test]
    async fn a_dropping_subscription_reports_what_it_dropped_before_its_next_rows() {
        let encoder = encoder();
        let (mut sender, mut frames, _outbound) = sender(SubscriptionDeliveryBehavior::Dropping);
        for _ in 0..SESSION_SUBSCRIPTION_CAPACITY {
            sender
                .send_rows(row_frame(&encoder, &[1]))
                .await
                .assured("the lane has room until it is full");
        }
        sender
            .send_rows(row_frame(&encoder, &[2, 3]))
            .await
            .assured("a full lane drops rows rather than closing");
        assert_eq!(sender.dropped_rows, 2, "a full lane drops the frame");
        // The loss is still unreported and finds no room either, so these rows are dropped too.
        sender
            .send_rows(row_frame(&encoder, &[4]))
            .await
            .assured("a full lane drops rows rather than closing");
        assert_eq!(sender.dropped_rows, 3);

        // Room for the loss report and the rows that follow it.
        assert_eq!(delivered_rows(next_event(&mut frames).await), 1);
        assert_eq!(delivered_rows(next_event(&mut frames).await), 1);
        sender
            .send_rows(row_frame(&encoder, &[5]))
            .await
            .assured("the lane has room again");
        for _ in 2..SESSION_SUBSCRIPTION_CAPACITY {
            assert_eq!(delivered_rows(next_event(&mut frames).await), 1);
        }
        let ServerEvent::SubscriptionDeliveryLost(lost) = next_event(&mut frames).await else {
            panic!("the loss is reported before the rows that follow it");
        };
        assert_eq!(lost.subscription, handle());
        assert_eq!(lost.dropped_rows.get(), 3);
        assert_eq!(sender.dropped_rows, 0, "a reported loss starts a new count");
        assert_eq!(delivered_rows(next_event(&mut frames).await), 1);

        sender.lane.withdrawal().withdraw();
        assert_eq!(
            sender.send_rows(row_frame(&encoder, &[6])).await,
            Err(LaneClosed),
            "rows of a withdrawn subscription end its delivery"
        );
    }

    #[tokio::test]
    async fn skipped_rows_are_reported_with_their_cause_and_count() {
        let (sender, mut frames, _outbound) = sender(SubscriptionDeliveryBehavior::Blocking);
        let skipped = SkippedRows {
            cause: RowsSkippedCause::FilterFailed,
            rows: NonZeroU64::new(2).assured("two is non-zero"),
            message: "session subscription predicate failed: division by zero".to_string(),
        };

        sender
            .report_skipped(skipped)
            .await
            .assured("the lane has room");

        let ServerEvent::SubscriptionRowsSkipped(reported) = next_event(&mut frames).await else {
            panic!("skipped rows are reported as such");
        };
        assert_eq!(reported.subscription, handle());
        assert_eq!(reported.cause, RowsSkippedCause::FilterFailed);
        assert_eq!(reported.skipped_rows.get(), 2);
        assert_eq!(
            reported.message,
            "session subscription predicate failed: division by zero"
        );
        sender.lane.withdrawal().withdraw();
        let gone = SkippedRows {
            cause: RowsSkippedCause::EncodingFailed,
            rows: NonZeroU64::MIN,
            message: "rows could not be encoded".to_string(),
        };
        assert_eq!(
            sender.report_skipped(gone).await,
            Err(LaneClosed),
            "a report for a withdrawn subscription ends its delivery"
        );
    }
}
