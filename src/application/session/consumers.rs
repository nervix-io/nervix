//! The native emitter consumers attached to one client session.
//!
//! Layer: edges.
//! - **Owns.** Consumer opens, session and node credit, one read stream per attachment,
//!   application settlement, and detachment when the session or an attachment ends.
//! - **Depends on.** The committed emitter schedule, node-local delivery owner and session wire.
//! - **Must not know.** Arrow batch encoding, graph execution, or transport framing.

use std::sync::{Arc as StdArc, Weak};

use ahash::HashMap;
use nervix_client_wire::{
    CloseEmitterOutcome, CloseEmitterRequest, ConsumerId, EmitterBatchDecision,
    EmitterBatchReceived, EmitterCloseDisposition, EmitterOpenRefusal, EmitterOpened,
    EmitterSettlement, OpenEmitterDisposition, OpenEmitterOutcome, OpenEmitterRequest,
    ReadEmitterBatchOutcome, ReadEmitterBatchRequest, ReadEmitterDisposition, ReplyBody, RequestId,
    SettleEmitterBatchOutcome, SettleEmitterBatchRequest,
};
use nervix_models::{
    CLIENT_CONSUMER_SESSION_BYTES, DomainStatus, EmitSink, MAX_CLIENT_CONSUMERS_PER_SESSION,
};
use nervix_primitives::sync::{
    Mutex as AsyncMutex,
    atomic::{AtomicBool, Ordering},
    blocking::Mutex,
    mpsc,
};
use triomphe::Arc;

use super::{QueuedReply, SessionShared};
use crate::{
    application::client_consumers::{ConsumerDelivery, ConsumerResponder},
    runtime::{ClientEmitterAnswer, ClientEmitterGrant, ClientEmitterRefusal},
};

#[derive(Default)]
pub(super) struct SessionConsumers {
    state: StdArc<Mutex<ConsumerState>>,
}

#[derive(Default)]
struct ConsumerState {
    open: HashMap<ConsumerId, Arc<AttachedConsumer>>,
    held_count: usize,
    held_bytes: u64,
}

struct AttachedConsumer {
    responder: ConsumerResponder,
    deliveries: AsyncMutex<mpsc::UnboundedReceiver<ConsumerDelivery>>,
    closed: AtomicBool,
    _capacity: ConsumerCapacityReservation,
    _grant: ClientEmitterGrant,
}

impl Drop for AttachedConsumer {
    fn drop(&mut self) {
        self.responder.close();
    }
}

struct ConsumerCapacityReservation {
    state: Weak<Mutex<ConsumerState>>,
    bytes: u64,
}

impl Drop for ConsumerCapacityReservation {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let mut state = state.lock();
            state.held_count -= 1;
            state.held_bytes -= self.bytes;
        }
    }
}

impl SessionConsumers {
    fn reserve(&self, bytes: u64) -> Result<ConsumerCapacityReservation, EmitterOpenRefusal> {
        let mut state = self.state.lock();
        if state.held_count >= MAX_CLIENT_CONSUMERS_PER_SESSION {
            return Err(EmitterOpenRefusal::TooManyConsumers);
        }
        if state
            .held_bytes
            .checked_add(bytes)
            .is_none_or(|total| total > CLIENT_CONSUMER_SESSION_BYTES)
        {
            return Err(EmitterOpenRefusal::SessionCapacityExhausted);
        }
        state.held_count += 1;
        state.held_bytes += bytes;
        Ok(ConsumerCapacityReservation {
            state: StdArc::downgrade(&self.state),
            bytes,
        })
    }

    pub(super) async fn open(
        &self,
        shared: &Arc<SessionShared>,
        request_id: RequestId,
        open: OpenEmitterRequest,
        in_transaction: bool,
    ) {
        let consumer = ConsumerId::opened_by(request_id);
        let outcome = match self.attach(shared, consumer, open, in_transaction).await {
            Ok(opened) => OpenEmitterOutcome {
                disposition: OpenEmitterDisposition::Opened(Box::new(opened)),
                message: "consumer attached".to_string(),
            },
            Err((refusal, message)) => OpenEmitterOutcome {
                disposition: OpenEmitterDisposition::Refused(refusal),
                message,
            },
        };
        let announced = shared
            .finish_with(request_id, ReplyBody::OpenEmitter(outcome))
            .await;
        if !matches!(announced, QueuedReply::Reply) {
            self.close_handle(consumer);
        }
    }

    async fn attach(
        &self,
        shared: &Arc<SessionShared>,
        consumer: ConsumerId,
        open: OpenEmitterRequest,
        in_transaction: bool,
    ) -> Result<EmitterOpened, (EmitterOpenRefusal, String)> {
        let refuse = |why, message: &str| (why, message.to_string());
        if in_transaction {
            return Err(refuse(
                EmitterOpenRefusal::InTransaction,
                "consumers are session scoped and cannot open in a transaction",
            ));
        }
        if !open.limits.is_within_bounds() {
            return Err(refuse(
                EmitterOpenRefusal::InvalidLimits,
                "consumer limits exceed the session maximum",
            ));
        }
        let service = &shared.service;
        let Some(domain_state) = service.inner.consensus.current_domain(&open.domain).await else {
            return Err(refuse(
                EmitterOpenRefusal::DomainNotFound,
                "domain does not exist",
            ));
        };
        if matches!(domain_state.status, DomainStatus::Stopped) {
            return Err(refuse(
                EmitterOpenRefusal::DomainStopped,
                "domain is stopped",
            ));
        }
        let Some((model, scheduled)) = service
            .emitter_target_from_schedule(&open.domain, &open.emitter)
            .await
            .ok()
            .flatten()
        else {
            return Err(refuse(
                EmitterOpenRefusal::EmitterNotFound,
                "emitter is not scheduled",
            ));
        };
        if !matches!(model.sink.as_ref(), EmitSink::Client { .. }) {
            return Err(refuse(
                EmitterOpenRefusal::NotClientEmitter,
                "emitter has an external sink",
            ));
        }
        let Some(owner) = scheduled.execution_node() else {
            return Err(refuse(
                EmitterOpenRefusal::EndpointUnavailable,
                "emitter has no execution owner",
            ));
        };
        let capacity = self
            .reserve(open.limits.bytes.get())
            .map_err(|why| (why, "session consumer budget is full".to_string()))?;
        let Some(grant) = service
            .inner
            .runtime
            .client_emitter_budget()
            .try_grant(open.limits.bytes)
        else {
            return Err(refuse(
                EmitterOpenRefusal::NodeCapacityExhausted,
                "node consumer budget is full",
            ));
        };
        let attached = service
            .inner
            .client_consumers
            .open(
                owner,
                open.domain.clone(),
                open.emitter.clone(),
                open.expected_fields.clone(),
                open.limits,
            )
            .await;
        let attached = match attached {
            Ok(value) => value,
            Err(why) => return Err((why, "consumer could not attach to this emitter".to_string())),
        };
        let crate::application::client_consumers::OpenedConsumerRoute {
            description,
            responder,
            deliveries,
        } = attached;
        let handle = Arc::new(AttachedConsumer {
            responder,
            deliveries: AsyncMutex::new(deliveries),
            closed: AtomicBool::new(false),
            _capacity: capacity,
            _grant: grant,
        });
        let replaced = self.state.lock().open.insert(consumer, handle);
        if let Some(previous) = replaced {
            previous.closed.store(true, Ordering::Release);
            previous.responder.close();
        }
        Ok(EmitterOpened {
            domain: open.domain,
            emitter: open.emitter,
            fields: description.fields,
            window: description.window,
            ack_timeout: description.ack_timeout,
            retry_backoff: description.retry_backoff,
            retry_max_backoff: description.retry_max_backoff,
            granted: open.limits,
            max_batch_bytes: description.maximum_payload_bytes,
            max_batch_rows: description.maximum_payload_rows,
        })
    }

    fn handle(&self, id: ConsumerId) -> Option<Arc<AttachedConsumer>> {
        self.state.lock().open.get(&id).cloned()
    }

    fn close_handle(&self, id: ConsumerId) -> bool {
        let Some(handle) = self.state.lock().open.remove(&id) else {
            return false;
        };
        handle.closed.store(true, Ordering::Release);
        handle.responder.close();
        true
    }

    pub(super) async fn read(&self, request: ReadEmitterBatchRequest) -> ReadEmitterBatchOutcome {
        let Some(handle) = self.handle(request.consumer) else {
            return ReadEmitterBatchOutcome {
                disposition: ReadEmitterDisposition::Ended,
                message: "consumer is not open".to_string(),
            };
        };
        let mut deliveries = handle.deliveries.lock().await;
        let received = deliveries.recv().await;
        if handle.closed.load(Ordering::Acquire) {
            return ReadEmitterBatchOutcome {
                disposition: ReadEmitterDisposition::Ended,
                message: "consumer was closed".to_string(),
            };
        }
        match received {
            Some(delivery) => ReadEmitterBatchOutcome {
                disposition: ReadEmitterDisposition::Batch(EmitterBatchReceived {
                    identity: delivery.identity,
                    reference: delivery.reference,
                    source_relay: delivery.source,
                    branch_fingerprint: delivery.branch_fingerprint,
                    batch: delivery.body,
                    members: delivery.members,
                    execution_now: delivery.execution_now,
                }),
                message: String::new(),
            },
            None => {
                self.close_handle(request.consumer);
                ReadEmitterBatchOutcome {
                    disposition: ReadEmitterDisposition::Ended,
                    message: "emitter endpoint ended".to_string(),
                }
            }
        }
    }

    pub(super) async fn settle(
        &self,
        request: SettleEmitterBatchRequest,
    ) -> SettleEmitterBatchOutcome {
        let disposition = match self.handle(request.consumer) {
            None => EmitterSettlement::ConsumerEnded,
            Some(handle) if handle.closed.load(Ordering::Acquire) => {
                EmitterSettlement::ConsumerEnded
            }
            Some(handle) => {
                let answer = match request.decision {
                    EmitterBatchDecision::Ack => ClientEmitterAnswer::Ack,
                    EmitterBatchDecision::Retry => ClientEmitterAnswer::Retry,
                    EmitterBatchDecision::Reject(reason) => ClientEmitterAnswer::Reject(reason),
                };
                match handle.responder.answer(request.reference, answer).await {
                    Ok(()) => EmitterSettlement::Confirmed,
                    Err(ClientEmitterRefusal::StaleReference) => EmitterSettlement::StaleReference,
                    Err(ClientEmitterRefusal::WrongConsumer) => EmitterSettlement::WrongConsumer,
                    Err(ClientEmitterRefusal::InvalidReason) => EmitterSettlement::InvalidReason,
                    Err(_) => EmitterSettlement::ConsumerEnded,
                }
            }
        };
        SettleEmitterBatchOutcome {
            disposition,
            message: String::new(),
        }
    }

    pub(super) fn close(&self, request: CloseEmitterRequest) -> CloseEmitterOutcome {
        let disposition = if self.close_handle(request.consumer) {
            EmitterCloseDisposition::Closed
        } else {
            EmitterCloseDisposition::NotOpen
        };
        CloseEmitterOutcome {
            disposition,
            message: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_interrupted_open_releases_its_session_reservation() {
        let consumers = SessionConsumers::default();
        let first = consumers
            .reserve(CLIENT_CONSUMER_SESSION_BYTES)
            .expect("first reservation fits");
        assert!(matches!(
            consumers.reserve(1),
            Err(EmitterOpenRefusal::SessionCapacityExhausted)
        ));
        drop(first);
        let replacement = consumers
            .reserve(CLIENT_CONSUMER_SESSION_BYTES)
            .expect("an interrupted open releases its reservation");
        drop(replacement);
        assert_eq!(consumers.state.lock().held_count, 0);
        assert_eq!(consumers.state.lock().held_bytes, 0);
    }
}
