//! The domain clocks one session follows.
//!
//! Layer: edges.
//!
//! - **Owns.** Attaching the session to a domain's clock and detaching it, the typed outcomes of
//!   both, and delivering installation changes and newest accepted ticks on the session's
//!   control lane until the session detaches, the domain leaves the serving node, or the session
//!   ends.
//! - **Depends on.** The runtime's installation of the committed domains and its observer of
//!   installed domain clocks, the session's control lane, and the client wire contract.
//! - **Must not know.** Relay subscriptions or their lane, the session's transaction binding, how a
//!   transport carries frames, or how a clock is installed or advanced.
//!
//! An attachment is keyed by its domain, so a session follows each domain clock at most once. Both
//! requests run on the ordered lane. An attach answers only once the serving node has installed
//! the committed domains since it started, so a domain it refuses as not found is one the committed
//! state lacks. The attach reply is queued before delivery starts, so the clock it carries precedes
//! every frame about the domain, and detach stops delivery before its reply is queued, so no frame
//! about the domain follows that reply. Delivery reads the installation the serving node publishes
//! each time it is replaced and sends a frame only when it differs from the one the client last
//! received. It then sends the newest accepted tick of that installation. State frames wait for
//! room; each tick holds one replaceable control slot.

use std::sync::atomic::{AtomicBool, Ordering};

use ahash::HashMap;
use meticulous::OptionExt as _;
use nervix_client_wire::{
    DomainClockAttachDisposition, DomainClockAttachOutcome, DomainClockAttachmentEndReason,
    DomainClockAttachmentEnded, DomainClockDetachDisposition, DomainClockDetachOutcome,
    DomainClockObserved, DomainClockTicked, EncodedFrame, ReplyBody, RequestId, ServerFrame,
};
use nervix_models::{DomainClockObservation, DomainClockTickObservation, DomainName};
use nervix_recovery::Discarded as _;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, warn};
use triomphe::Arc;

use super::{QueuedReply, SessionShared, outbound::ReplaceableControlFrame};
use crate::{
    runtime::DomainClockObserver,
    task_shutdown::{JoinOutputShutdown as _, JoinShutdown as _},
};

/// The domain clocks a session follows, by domain.
#[derive(Default)]
pub(super) struct ClockAttachments {
    attached: HashMap<DomainName, ClockAttachment>,
}

/// One followed domain clock and the delivery of its changes.
struct ClockAttachment {
    /// Stops delivery when it is dropped, whoever drops it.
    stop: DropGuard,
    /// Raised by the delivery before it queues the frame that ends the attachment, so a request
    /// the client sends after reading that frame always finds the attachment ending.
    ending: Arc<AtomicBool>,
    delivery: JoinHandle<DeliveryEnd>,
}

/// How the delivery of an attached clock ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryEnd {
    /// The session detached, ended, or stopped taking frames. Nothing told the client it ended.
    Stopped,
    /// The domain left the serving node, and the frame that says so was queued.
    DomainRemoved,
}

impl ClockAttachments {
    /// Attaches the session to the clock of `domain` and answers the request.
    ///
    /// The reply carries the clock as this node has it installed. A node that has not installed the
    /// committed domains since it started answers once it has, so it never refuses a domain the
    /// cluster has as not found. Delivery starts only once that reply is queued: a reply that was
    /// not queued, because the request was cancelled, the session ended or the reply did not fit,
    /// announced nothing, so nothing is delivered after it.
    pub(super) async fn attach(
        &mut self,
        shared: &Arc<SessionShared>,
        request_id: RequestId,
        domain: DomainName,
    ) {
        self.release_ending(&domain).await;
        if self.attached.contains_key(&domain) {
            let outcome = DomainClockAttachOutcome {
                message: format!(
                    "this session already follows the clock of domain '{}'",
                    domain.as_str()
                ),
                disposition: DomainClockAttachDisposition::AlreadyAttached(domain),
            };
            shared
                .finish_with(request_id, ReplyBody::DomainClockAttach(outcome))
                .await;
            return;
        }
        let runtime = &shared.service.inner.runtime;
        // A node holds no domain until it installs the committed ones after it starts, so before
        // that it cannot tell a domain the cluster lacks from one it has not installed yet, as
        // right after a restart. The answer waits for that installation, or for the session to end.
        tokio::select! {
            () = runtime.committed_domains_installed() => {}
            () = shared.ended() => return,
        }
        let observer = runtime.observe_domain_clock(&domain);
        let clock = match &observer {
            Some(observer) => observer.current(),
            None => None,
        };
        let (Some(observer), Some(clock)) = (observer, clock) else {
            let outcome = DomainClockAttachOutcome {
                message: format!("domain '{}' does not exist", domain.as_str()),
                disposition: DomainClockAttachDisposition::DomainNotFound(domain),
            };
            shared
                .finish_with(request_id, ReplyBody::DomainClockAttach(outcome))
                .await;
            return;
        };
        let outcome = DomainClockAttachOutcome {
            message: format!(
                "attached to the clock of domain '{}': {clock}",
                domain.as_str()
            ),
            disposition: DomainClockAttachDisposition::Attached {
                domain: domain.clone(),
                clock: clock.clone(),
            },
        };
        let queued = shared
            .finish_with(request_id, ReplyBody::DomainClockAttach(outcome))
            .await;
        let QueuedReply::Reply = queued else {
            debug!(
                domain = domain.as_str(),
                "a domain clock attachment was not announced and delivers nothing"
            );
            return;
        };
        debug!(
            domain = domain.as_str(),
            "a session attached to a domain clock"
        );
        let attachment = ClockAttachment::start(shared, domain.clone(), observer, clock);
        self.attached.insert(domain, attachment).discarded(
            "the release above removed an ending attachment, and a delivering one answered \
             already attached",
        );
    }

    /// Detaches the session from the clock of `domain` and answers the request.
    ///
    /// Delivery has stopped before the reply is queued, so no frame about the domain follows it.
    /// An attachment the server already ended is no longer followed, and says so.
    pub(super) async fn detach(
        &mut self,
        shared: &SessionShared,
        request_id: RequestId,
        domain: DomainName,
    ) {
        let end = match self.attached.remove(&domain) {
            Some(attachment) => Some(attachment.stop().await),
            None => None,
        };
        let outcome = match end {
            Some(DeliveryEnd::Stopped) => {
                debug!(
                    domain = domain.as_str(),
                    "a session detached from a domain clock"
                );
                DomainClockDetachOutcome {
                    message: format!("detached from the clock of domain '{}'", domain.as_str()),
                    disposition: DomainClockDetachDisposition::Detached(domain),
                }
            }
            Some(DeliveryEnd::DomainRemoved) | None => DomainClockDetachOutcome {
                message: format!(
                    "this session does not follow the clock of domain '{}'",
                    domain.as_str()
                ),
                disposition: DomainClockDetachDisposition::NotAttached(domain),
            },
        };
        shared
            .finish_with(request_id, ReplyBody::DomainClockDetach(outcome))
            .await;
    }

    /// Stops every attachment when the session ends, all of them together, and waits until each
    /// delivery has ended. The client is told nothing: its session is gone.
    pub(super) async fn stop_all(&mut self) {
        let attached = std::mem::take(&mut self.attached);
        let mut deliveries = Vec::with_capacity(attached.len());
        for (_, attachment) in attached {
            let ClockAttachment { stop, delivery, .. } = attachment;
            drop(stop);
            deliveries.push(delivery);
        }
        for delivery in deliveries {
            tokio::task::consume_budget().await;
            delivery.join_after_shutdown("domain clock delivery").await;
        }
    }

    /// Forgets the attachment of `domain` once the server is ending it, so the session can attach
    /// to the domain's clock again after the frame that ends it.
    async fn release_ending(&mut self, domain: &DomainName) {
        let ending = match self.attached.get(domain) {
            Some(attachment) => attachment.is_ending(),
            None => false,
        };
        if !ending {
            return;
        }
        let attachment = self
            .attached
            .remove(domain)
            .verified("the attachment found above is still held");
        attachment.stop().await;
    }
}

impl ClockAttachment {
    /// Starts delivering the changes of an attached clock, the client having received `delivered`.
    fn start(
        shared: &Arc<SessionShared>,
        domain: DomainName,
        observer: DomainClockObserver,
        delivered: DomainClockObservation,
    ) -> Self {
        let stop = CancellationToken::new();
        let ending = Arc::new(AtomicBool::new(false));
        let delivery = ClockDelivery {
            shared: shared.clone(),
            domain,
            observer,
            order: ClockDeliveryOrder::new(delivered),
            pending_tick: None,
            stop: stop.clone(),
            ending: ending.clone(),
        };
        Self {
            stop: stop.drop_guard(),
            ending,
            delivery: tokio::spawn(delivery.run()),
        }
    }

    /// Whether the delivery is ending the attachment, or has ended, on its own.
    fn is_ending(&self) -> bool {
        self.ending.load(Ordering::Acquire) || self.delivery.is_finished()
    }

    /// Ends delivery and waits until it has, so it queues nothing afterwards. A delivery that is
    /// ending the attachment on its own finishes queueing the frame that says so; any other stops
    /// at once.
    async fn stop(self) -> DeliveryEnd {
        let Self {
            stop,
            ending,
            delivery,
        } = self;
        let end = if ending.load(Ordering::Acquire) {
            let end = delivery
                .output_after_shutdown("domain clock delivery")
                .await;
            drop(stop);
            end
        } else {
            drop(stop);
            delivery
                .output_after_shutdown("domain clock delivery")
                .await
        };
        // A delivery that panicked queued nothing the client could take for an end.
        end.unwrap_or(DeliveryEnd::Stopped)
    }
}

/// The task that sends an attached clock's changes to its session.
struct ClockDelivery {
    shared: Arc<SessionShared>,
    domain: DomainName,
    observer: DomainClockObserver,
    order: ClockDeliveryOrder,
    pending_tick: Option<ReplaceableControlFrame>,
    stop: CancellationToken,
    /// Raised before the frame that ends the attachment is queued.
    ending: Arc<AtomicBool>,
}

/// The greatest tick queued by this delivery owner for one generation.
struct DeliveredTick {
    generation: u64,
    id: u64,
}

/// The production delivery owner's state and tick choice. It decides from the observer's latest
/// publication each time delivery has room to proceed, and records the newest tick selected for
/// its replaceable slot.
pub(crate) struct ClockDeliveryOrder {
    /// The clock the client last received, in the attach reply or a frame.
    delivered: DomainClockObservation,
    delivered_tick: Option<DeliveredTick>,
}

pub(crate) enum NextClockFrame {
    State(DomainClockObservation),
    Tick(DomainClockTickObservation),
    End,
    Wait,
}

impl ClockDeliveryOrder {
    pub(crate) fn new(delivered: DomainClockObservation) -> Self {
        Self {
            delivered,
            delivered_tick: None,
        }
    }

    pub(crate) fn next(&self, observer: &DomainClockObserver) -> NextClockFrame {
        let Some(clock) = observer.current() else {
            return NextClockFrame::End;
        };
        if clock != self.delivered {
            return NextClockFrame::State(clock);
        }
        if let Some(tick) = observer.current_tick()
            && self.newer_tick(&tick)
        {
            // Progress and installation have separate publications. A replacement can land
            // between the first state read and the tick snapshot, so check the state again
            // before selecting the tick for the control lane.
            match observer.current() {
                Some(current) if current != self.delivered => {
                    return NextClockFrame::State(current);
                }
                None => return NextClockFrame::End,
                Some(_) => {}
            }
            return NextClockFrame::Tick(tick);
        }
        NextClockFrame::Wait
    }

    pub(crate) fn state_queued(&mut self, clock: DomainClockObservation) {
        if self.delivered.generation != clock.generation {
            self.delivered_tick = None;
        }
        self.delivered = clock;
    }

    fn newer_tick(&self, tick: &DomainClockTickObservation) -> bool {
        tick.generation == self.delivered.generation
            && !self.delivered_tick.as_ref().is_some_and(|delivered| {
                delivered.generation == tick.generation && delivered.id >= tick.tick_id
            })
    }

    pub(crate) fn tick_queued(&mut self, tick: &DomainClockTickObservation) {
        self.delivered_tick = Some(DeliveredTick {
            generation: tick.generation,
            id: tick.tick_id,
        });
    }
}

impl ClockDelivery {
    async fn run(mut self) -> DeliveryEnd {
        loop {
            tokio::task::consume_budget().await;
            if self.stop.is_cancelled() {
                return DeliveryEnd::Stopped;
            }
            match self.order.next(&self.observer) {
                NextClockFrame::End => return self.end().await,
                NextClockFrame::State(clock) => {
                    if let Some(pending) = self.pending_tick.take() {
                        pending.withdraw();
                    }
                    let observed = DomainClockObserved {
                        domain: self.domain.clone(),
                        clock,
                    };
                    let frame = match observed.encode(self.shared.limits()) {
                        Ok(frame) => frame,
                        Err(error) => {
                            warn!(
                                domain = self.domain.as_str(),
                                error = %error,
                                "a domain clock observation does not fit a session frame"
                            );
                            if !self.wait_change().await {
                                return DeliveryEnd::Stopped;
                            }
                            continue;
                        }
                    };
                    if !self.send(frame).await {
                        return DeliveryEnd::Stopped;
                    }
                    self.order.state_queued(observed.clock);
                }
                NextClockFrame::Tick(tick) => {
                    if !self.send_tick(tick).await {
                        return DeliveryEnd::Stopped;
                    }
                }
                NextClockFrame::Wait => {
                    if !self.wait_change().await {
                        return DeliveryEnd::Stopped;
                    }
                }
            }
        }
    }

    async fn wait_change(&mut self) -> bool {
        tokio::select! {
            biased;
            () = self.stop.cancelled() => false,
            () = self.shared.ended() => false,
            () = self.observer.any_changed() => true,
        }
    }

    fn encode_tick(&self, tick: &DomainClockTickObservation) -> Option<EncodedFrame<ServerFrame>> {
        let event = DomainClockTicked {
            domain: self.domain.clone(),
            tick: tick.clone(),
        };
        match event.encode(self.shared.limits()) {
            Ok(frame) => Some(frame),
            Err(error) => {
                warn!(
                    domain = self.domain.as_str(),
                    error = %error,
                    "a domain clock tick does not fit a session frame"
                );
                None
            }
        }
    }

    /// Queues one replaceable tick slot. While the lane is full, new progress overwrites the
    /// slot's contents. Once queued, later progress still replaces it until transport takes it.
    async fn send_tick(&mut self, tick: DomainClockTickObservation) -> bool {
        let Some(frame) = self.encode_tick(&tick) else {
            return self.wait_change().await;
        };
        if let Some(slot) = &self.pending_tick {
            match slot.replace(frame) {
                Ok(()) => {
                    self.order.tick_queued(&tick);
                    return true;
                }
                Err(frame) => return self.queue_tick(frame, tick).await,
            }
        }
        self.queue_tick(frame, tick).await
    }

    async fn queue_tick(
        &mut self,
        frame: EncodedFrame<ServerFrame>,
        tick: DomainClockTickObservation,
    ) -> bool {
        let slot = ReplaceableControlFrame::new(frame);
        let outbound = self.shared.delivery.outbound.clone();
        let sending = outbound.send_replaceable(slot.clone());
        tokio::pin!(sending);
        self.order.tick_queued(&tick);
        loop {
            tokio::task::consume_budget().await;
            tokio::select! {
                biased;
                () = self.stop.cancelled() => return false,
                () = self.shared.ended() => return false,
                sent = &mut sending => {
                    if sent.is_err() {
                        return false;
                    }
                    self.pending_tick = Some(slot);
                    return true;
                }
                () = self.observer.any_changed() => {
                    match self.order.next(&self.observer) {
                        NextClockFrame::State(_) | NextClockFrame::End => {
                            slot.withdraw();
                            return true;
                        }
                        NextClockFrame::Tick(newer) => {
                            if let Some(frame) = self.encode_tick(&newer)
                                && slot.replace(frame).is_ok()
                            {
                                self.order.tick_queued(&newer);
                            }
                        }
                        NextClockFrame::Wait => {}
                    }
                }
            }
        }
    }

    /// Tells the client the domain left the serving node, which ends the attachment.
    async fn end(self) -> DeliveryEnd {
        // Raised before the frame is queued: a request the client sends after reading it then
        // finds the attachment ending on the ordered lane.
        self.ending.store(true, Ordering::Release);
        let ended = DomainClockAttachmentEnded {
            domain: self.domain.clone(),
            reason: DomainClockAttachmentEndReason::DomainRemoved,
        };
        match ended.encode(self.shared.limits()) {
            Ok(frame) => {
                if !self.send(frame).await {
                    return DeliveryEnd::Stopped;
                }
            }
            Err(error) => warn!(
                domain = self.domain.as_str(),
                error = %error,
                "the end of a domain clock attachment does not fit a session frame"
            ),
        }
        debug!(
            domain = self.domain.as_str(),
            "a domain clock attachment ended because the domain left the node"
        );
        DeliveryEnd::DomainRemoved
    }

    /// Queues a frame on the control lane unless delivery stops first. `false` means it stopped,
    /// and the frame was not queued.
    async fn send(&self, frame: EncodedFrame<ServerFrame>) -> bool {
        tokio::select! {
            biased;
            () = self.stop.cancelled() => false,
            sent = self.shared.send_frame(frame) => sent.is_ok(),
        }
    }
}
