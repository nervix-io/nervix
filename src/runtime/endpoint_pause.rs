//! A client's claim on a request in an endpoint pause buffer.
//!
//! Layer: data plane.
//! - **Owns.** Exactly one admission or abandonment, the waiting client's reply and its deadline.
//! - **Depends on.** The ingestor's pause buffer, response dispositions and execution primitives.
//! - **Must not know.** Socket framing, statement Models, schedule installation or transactions.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the pending endpoint response")
)]

use std::{fmt, future::pending};

use meticulous::ResultExt as _;
use nervix_models::ClientProcessingFailure;
use nervix_primitives::{
    sync::{
        Arc, CancellationToken, Notify,
        atomic::{AtomicU8, Ordering},
        blocking::Mutex,
        oneshot, watch,
    },
    time::{Instant, sleep_until},
};
use nervix_recovery::NoReceiver;

use super::{
    AckCompletion, AckOutcome, AckProgress, IngestorQuiesceControl,
    endpoint_acknowledgement::{EndpointAdmissionRefusal, EndpointRequestOutcome},
};
use crate::metrics::{EndpointPauseRefusal, IngestorEndpointMetrics};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct EndpointPauseId(pub(super) u64);

/// A single transition coordinates the request future and the buffer's replay task. The atomic
/// selects the outcome alone. The buffer and response channel own all associated data.
#[derive(Debug)]
struct EndpointPauseClaim {
    disposition: AtomicU8,
    abandoned: CancellationToken,
    metrics: Option<IngestorEndpointMetrics>,
    bytes: i64,
}

impl EndpointPauseClaim {
    const WAITING: u8 = 0;
    const ADMITTED: u8 = 1;
    const ABANDONED: u8 = 2;

    fn new(metrics: Option<IngestorEndpointMetrics>, bytes: usize) -> Self {
        let bytes = i64::try_from(bytes).assured("a request occupies an addressable byte slice");
        if let Some(metrics) = &metrics {
            metrics.begin_pause_wait(bytes);
        }
        Self {
            disposition: AtomicU8::new(Self::WAITING),
            abandoned: CancellationToken::new(),
            metrics,
            bytes,
        }
    }

    fn admit(&self) -> bool {
        self.transition(Self::ADMITTED)
    }

    fn transition(&self, disposition: u8) -> bool {
        let changed = self
            .disposition
            .compare_exchange(
                Self::WAITING,
                disposition,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok();
        if changed && let Some(metrics) = &self.metrics {
            metrics.end_pause_wait(self.bytes);
        }
        changed
    }

    fn abandon(&self) -> bool {
        let changed = self.transition(Self::ABANDONED);
        if changed {
            self.abandoned.cancel();
        }
        changed
    }

    fn admitted(&self) -> bool {
        self.disposition.load(Ordering::Relaxed) == Self::ADMITTED
    }

    fn waiting(&self) -> bool {
        self.disposition.load(Ordering::Relaxed) == Self::WAITING
    }
}

/// The buffered bindings of one WebSocket frame. A route can have several ingestors, but the
/// connection only reads ahead while at least one binding still holds the frame.
#[derive(Default)]
pub(crate) struct EndpointFramePause {
    claims: Mutex<Vec<Arc<EndpointPauseClaim>>>,
    changed: Notify,
}

impl EndpointFramePause {
    pub(super) fn observe(&self, wait: &EndpointPauseWait<'_>) {
        self.claims.lock().push(wait.claim.clone());
        self.changed.notify_waiters();
    }

    pub(crate) async fn wait_until_buffered(&self) {
        loop {
            let changed = self.changed.notified();
            let mut changed = std::pin::pin!(changed);
            changed.as_mut().enable();
            if self.claims.lock().iter().any(|claim| claim.waiting()) {
                return;
            }
            changed.await;
        }
    }

    /// Returns true when at least one part of the frame was cancelled before admission. This
    /// distinguishes a second frame that arrived during a pause from one that raced its end.
    pub(crate) fn abandon_waiting(&self) -> bool {
        let mut abandoned = false;
        for claim in self.claims.lock().iter() {
            abandoned |= claim.abandon();
        }
        abandoned
    }
}
pub(super) enum EndpointPauseOutcome {
    Finished(EndpointRequestOutcome),
    Acknowledging {
        completion: AckCompletion,
        intake_ending: watch::Receiver<bool>,
    },
}

pub(super) struct EndpointPauseReply {
    claim: Arc<EndpointPauseClaim>,
    deadline: Option<Instant>,
    sender: oneshot::Sender<EndpointPauseOutcome>,
}

impl fmt::Debug for EndpointPauseReply {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EndpointPauseReply")
            .field("claim", &self.claim)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl EndpointPauseReply {
    pub(super) fn channel(
        control: &IngestorQuiesceControl,
        id: EndpointPauseId,
        bytes: usize,
        deadline: Option<Instant>,
    ) -> (Self, EndpointPauseWait<'_>) {
        let claim = Arc::new(EndpointPauseClaim::new(
            control.metric_labels.endpoint().cloned(),
            bytes,
        ));
        let (sender, receiver) = oneshot::channel();
        let reply = Self {
            claim: claim.clone(),
            deadline,
            sender,
        };
        let wait = EndpointPauseWait {
            control,
            id,
            claim,
            deadline,
            receiver,
        };
        (reply, wait)
    }

    pub(super) fn acknowledged(&self) -> bool {
        self.deadline.is_some()
    }

    pub(super) fn abandoned(&self) -> bool {
        self.claim.disposition.load(Ordering::Relaxed) == EndpointPauseClaim::ABANDONED
    }

    pub(super) async fn wait_for_abandonment(&self) {
        self.claim.abandoned.cancelled().await;
    }

    /// Transitions to graph admission before the graph can see a row. A deadline already reached
    /// refuses admission even when its receiver has not been polled yet.
    pub(super) fn admit(&self) -> bool {
        if let Some(deadline) = self.deadline
            && Instant::now() >= deadline
        {
            self.claim.abandon();
            return false;
        }
        self.claim.admit()
    }

    pub(super) fn answer(self, result: EndpointPauseOutcome) {
        self.sender.send(result).means_peer_left("endpoint request");
    }
}

pub(super) struct EndpointPauseWait<'a> {
    control: &'a IngestorQuiesceControl,
    id: EndpointPauseId,
    claim: Arc<EndpointPauseClaim>,
    deadline: Option<Instant>,
    receiver: oneshot::Receiver<EndpointPauseOutcome>,
}

impl EndpointPauseWait<'_> {
    pub(super) async fn finish(self, shutdown: &CancellationToken) -> EndpointRequestOutcome {
        let deadline = self.deadline;
        self.finish_until(shutdown, async {
            match deadline {
                Some(deadline) => sleep_until(deadline).await,
                None => pending::<()>().await,
            }
        })
        .await
    }

    async fn finish_until(
        mut self,
        shutdown: &CancellationToken,
        expiration: impl Future<Output = ()>,
    ) -> EndpointRequestOutcome {
        let work = async {
            match (&mut self.receiver).await {
                Ok(EndpointPauseOutcome::Finished(result)) => Some(result),
                Ok(EndpointPauseOutcome::Acknowledging { completion, mut intake_ending }) => Some(
                    nervix_primitives::select! {
                        biased;
                        outcome = await_acknowledgement(completion) => outcome,
                        _ = intake_ending.wait_for(|ending| *ending) => EndpointRequestOutcome::Interrupted,
                    }
                ),
                Err(_) => None,
            }
        };
        let stop_while_waiting = async {
            self.control.wait_until_intake_stops().await;
            if !self.claim.abandon() && self.claim.admitted() {
                pending::<()>().await;
            }
        };
        let outcome = nervix_primitives::select! {
            biased;
            outcome = work => {
                if let Some(outcome) = outcome {
                    outcome
                } else {
                    self.interruption()
                }
            }
            () = shutdown.cancelled() => self.interruption(),
            () = stop_while_waiting => self.interruption(),
            () = expiration => {
                if self.claim.abandon() || !self.claim.admitted() {
                    EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::PauseExpired)
                } else {
                    EndpointRequestOutcome::ProcessingFailed(ClientProcessingFailure::AckTimedOut)
                }
            }
        };
        if let Some(metrics) = &self.claim.metrics {
            match outcome {
                EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::PauseExpired) => {
                    metrics.record_pause_refusal(EndpointPauseRefusal::Deadline);
                }
                EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::IntakeEnded) => {
                    metrics.record_pause_refusal(EndpointPauseRefusal::IntakeEnded);
                }
                _ => {}
            }
        }
        outcome
    }

    fn interruption(&self) -> EndpointRequestOutcome {
        if self.claim.abandon() || !self.claim.admitted() {
            EndpointRequestOutcome::NotAdmitted(EndpointAdmissionRefusal::IntakeEnded)
        } else {
            EndpointRequestOutcome::Interrupted
        }
    }
}

impl Drop for EndpointPauseWait<'_> {
    fn drop(&mut self) {
        self.claim.abandon();
        if !self.claim.admitted() {
            self.control.remove_waiting_endpoint(self.id);
        }
    }
}

pub(super) async fn await_acknowledgement(mut completion: AckCompletion) -> EndpointRequestOutcome {
    loop {
        nervix_primitives::task::consume_budget().await;
        match completion.wait_for_progress().await {
            AckProgress::Alive => {}
            AckProgress::Complete(AckOutcome::Ack) => return EndpointRequestOutcome::Completed,
            AckProgress::Complete(AckOutcome::NoAck(_)) => {
                return EndpointRequestOutcome::ProcessingFailed(ClientProcessingFailure::Rejected);
            }
        }
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "endpoint_pause_shuttle_tests.rs"]
mod shuttle_tests;
