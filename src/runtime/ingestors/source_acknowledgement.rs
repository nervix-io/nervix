//! The acknowledgement a broker source's host returns for one acknowledged batch, and the work it
//! keeps outstanding until the source has confirmed the batch on its transport.
//!
//! Layer: data plane.
//!
//! - **Owns.** The confirmed roots of one source batch's payloads, including the at-once resolved
//!   root of a payload the host consumed, waiting on them together, deciding the batch's outcome,
//!   and holding the batch's work outstanding for drains and ownership handoffs from the moment its
//!   last root resolves until the source loop drops the acknowledgement.
//! - **Depends on.** The ACK tree's confirmed roots, the source contract's batch acknowledgement
//!   and the stop the source host watches.
//! - **Must not know.** A broker driver, connector configuration, the source loop's retry policy,
//!   or how the batch's messages were dispatched.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use nervix_connector::SourceBatchAcknowledgementServices;

use super::*;

/// The confirmed roots of one acknowledged batch's payloads, which the batch's acknowledgement
/// waits on.
///
/// A dispatched payload's root resolves once the graph has acknowledged every message the payload
/// produced. A payload the host consumed without dispatching it, one its codec refused, has nothing
/// left to wait for, so its root resolves at once. It still takes a confirmed root, because the
/// payload's position is settled on the transport together with the batch's: a drain or an
/// ownership handoff has to wait for that settlement before another owner may read the payload's
/// position again, exactly as it waits for a dispatched payload's.
pub(super) struct BatchRoots<'t> {
    trackers: &'t IngestorAckRootTrackers,
    completions: Vec<AckCompletion>,
}

impl<'t> BatchRoots<'t> {
    /// The roots of a batch of `payloads` payloads, counted against `trackers`.
    pub(super) fn new(trackers: &'t IngestorAckRootTrackers, payloads: usize) -> Self {
        Self {
            trackers,
            completions: Vec::with_capacity(payloads),
        }
    }

    /// The root of a payload the host dispatches into the graph, which resolves it.
    pub(super) fn dispatched(&mut self) -> AckSet {
        let (root, completion) = self.trackers.confirmed_root();
        self.completions.push(completion);
        root
    }

    /// Resolves the root of a payload the host consumed without dispatching it.
    pub(super) fn consumed(&mut self) {
        let (root, completion) = self.trackers.confirmed_root();
        root.ack_success();
        self.completions.push(completion);
    }

    /// The acknowledgement of the batch, which waits on every root these hold.
    pub(super) fn acknowledgement(
        self,
        shutdown: watch::Receiver<bool>,
        confirmation_delay: Option<Duration>,
    ) -> RuntimeBatchAcknowledgement {
        RuntimeBatchAcknowledgement::new(self.completions, shutdown, confirmation_delay)
    }
}

/// One root of a batch whose wait ended: the outcome it ended with and the completion that
/// observed it.
struct SettledRoot {
    outcome: AckOutcome,
    /// Keeps a root that resolved counted for drains and ownership handoffs until it drops.
    completion: AckCompletion,
}

/// The acknowledgement of one acknowledged source batch, over the confirmed roots of the messages
/// the host dispatched.
///
/// A confirmed root keeps counting against its domain and ingestor after it resolves, for as long
/// as the completion that observed the resolution lives. This acknowledgement drops the completion
/// of every root that resolves while others are still unresolved, and keeps the completion of the
/// root that resolves last. Until then the unresolved roots hold the batch outstanding themselves,
/// unless they wait on what a drain does not wait for, such as `REQUIRED WAIT` state. From then on
/// the kept completion holds it, so a drain or an ownership handoff that waits for the batch keeps
/// waiting until the source loop drops this acknowledgement, which it does once the source's
/// transport has confirmed or rejected the batch's positions.
pub(super) struct RuntimeBatchAcknowledgement {
    /// The completions of the batch's roots, until the first wait starts waiting on them.
    unsettled: Vec<AckCompletion>,
    settling: FuturesUnordered<BoxFuture<'static, SettledRoot>>,
    shutdown: watch::Receiver<bool>,
    /// The completion of the root that settled last, held only so that it holds the batch
    /// outstanding until this acknowledgement drops.
    _confirming: Option<AckCompletion>,
    /// How long a scenario makes the host wait, once every root acknowledged, before it lets the
    /// source confirm the batch, as a slow transport would.
    confirmation_delay: Option<Duration>,
}

impl RuntimeBatchAcknowledgement {
    /// The acknowledgement of a batch whose dispatched messages resolve `roots`. A batch with no
    /// roots, whose every message a quiesce buffer or drop policy took over, acknowledges at once.
    pub(super) fn new(
        roots: Vec<AckCompletion>,
        shutdown: watch::Receiver<bool>,
        confirmation_delay: Option<Duration>,
    ) -> Self {
        Self {
            unsettled: roots,
            settling: FuturesUnordered::new(),
            shutdown,
            _confirming: None,
            confirmation_delay,
        }
    }

    /// Waits until `completion` reports its root's terminal outcome. Progress the root reports
    /// restarts the wait, so `timeout` bounds how long the root may stay silent.
    async fn settle(mut completion: AckCompletion, timeout: Duration) -> SettledRoot {
        let outcome = completion.wait_while_alive(timeout).await;
        SettledRoot {
            outcome,
            completion,
        }
    }
}

#[async_trait]
impl SourceBatchAcknowledgementServices for RuntimeBatchAcknowledgement {
    async fn wait(&mut self, timeout: Duration) -> SourceAcknowledgementOutcome {
        for completion in self.unsettled.drain(..) {
            self.settling
                .push(Box::pin(Self::settle(completion, timeout)));
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            if self.settling.is_empty() {
                break;
            }
            // A root that already settled is taken before a stop, so a batch whose roots resolved
            // is never abandoned to a stop that arrived at the same moment.
            let next: Option<SettledRoot> = nervix_primitives::select! {
                biased;
                next = self.settling.next() => next,
                _ = self.shutdown.changed() => return SourceAcknowledgementOutcome::Shutdown,
            };
            let settled = next.verified("the loop ends once no root is left to settle");
            match settled.outcome {
                AckOutcome::Ack => {
                    if self.settling.is_empty() {
                        self._confirming = Some(settled.completion);
                    }
                }
                AckOutcome::NoAck(reason) => {
                    // The source rejects the batch, and the drain waits for that as well.
                    self._confirming = Some(settled.completion);
                    return SourceAcknowledgementOutcome::NoAck(reason);
                }
            }
        }
        if let Some(delay) = self.confirmation_delay {
            sleep(delay).await;
        }
        SourceAcknowledgementOutcome::Ack
    }
}
