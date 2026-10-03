//! Volatile application delivery from one executing client emitter.
//!
//! Layer: data plane.
//! - **Owns.** Competing consumer assignments, attempt revocation, physical retry deadlines,
//!   bounded retained Arrow bytes, and the application result that releases source work.
//! - **Depends on.** Typed emitter plans, Arrow schema contracts, the execution-sensitive
//!   primitive boundary, and physical time.
//! - **Must not know.** Sessions, their wire format, NSPL text, or a client's transport.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "client emitter attachment installation resolves the concrete consumer lifetime"
    )
)]

use std::{collections::VecDeque, num::NonZeroU64, time::Duration};

use ahash::HashMap;
use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    AckWindow, CLIENT_CONSUMER_NODE_BYTES, CLIENT_CONSUMER_SESSION_BYTES, DomainName, EmitterName,
    ModelKind, RelayName, SchemaField, Timestamp,
};
use nervix_primitives::{
    sync::{
        Arc, Notify,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, oneshot,
    },
    time::{Instant, sleep_until},
};
use nervix_recovery::NoReceiver as _;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use uuid::Uuid;

use super::{BranchKey, DomainNodeRef, Runtime};
use crate::metrics::ClientEmitterSeries;

const RETAINED_RESULTS: usize = 2048;

/// A reservation for Arrow bytes still awaiting application processing.
#[derive(Clone)]
pub(crate) struct ClientEmitterBudget {
    used: Arc<AtomicU64>,
    granted: Arc<AtomicU64>,
    changed: Arc<Notify>,
}

impl Default for ClientEmitterBudget {
    fn default() -> Self {
        Self {
            used: Arc::new(AtomicU64::new(0)),
            granted: Arc::new(AtomicU64::new(0)),
            changed: Arc::new(Notify::new()),
        }
    }
}

impl ClientEmitterBudget {
    pub(crate) fn try_grant(&self, bytes: NonZeroU64) -> Option<ClientEmitterGrant> {
        let mut current = self.granted.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes.get())?;
            if next > CLIENT_CONSUMER_NODE_BYTES {
                return None;
            }
            match self.granted.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(ClientEmitterGrant {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub(crate) async fn reserve(&self, bytes: NonZeroU64) -> Option<ClientEmitterReservation> {
        if bytes.get() > CLIENT_CONSUMER_NODE_BYTES {
            return None;
        }
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let mut current = self.used.load(Ordering::Acquire);
            loop {
                let next = current.checked_add(bytes.get())?;
                if next > CLIENT_CONSUMER_NODE_BYTES {
                    break;
                }
                match self.used.compare_exchange_weak(
                    current,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        return Some(ClientEmitterReservation {
                            budget: self.clone(),
                            bytes,
                        });
                    }
                    Err(actual) => current = actual,
                }
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
}

pub(crate) struct ClientEmitterGrant {
    budget: ClientEmitterBudget,
    bytes: NonZeroU64,
}

impl Drop for ClientEmitterGrant {
    fn drop(&mut self) {
        self.budget
            .granted
            .fetch_sub(self.bytes.get(), Ordering::AcqRel);
    }
}

pub(crate) struct ClientEmitterReservation {
    budget: ClientEmitterBudget,
    bytes: NonZeroU64,
}

impl Drop for ClientEmitterReservation {
    fn drop(&mut self) {
        self.budget
            .used
            .fetch_sub(self.bytes.get(), Ordering::AcqRel);
        self.budget.changed.notify_waiters();
    }
}

/// The source identity and exact Arrow IPC bytes prepared once for a delivery.
#[derive(Debug, Clone)]
pub(crate) struct ClientEmitterPayload {
    pub(crate) identity: Uuid,
    pub(crate) source: RelayName,
    pub(crate) branch: Option<BranchKey>,
    pub(crate) body: Bytes,
    pub(crate) members: usize,
    pub(crate) execution_now: Timestamp,
}

/// One attempt sent to a consumer. Its reference changes whenever it is reassigned.
#[derive(Debug, Clone)]
pub(crate) struct ClientEmitterDelivery {
    pub(crate) payload: ClientEmitterPayload,
    pub(crate) reference: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClientEmitterResult {
    Acknowledged,
    Rejected(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) enum ClientEmitterAnswer {
    Ack,
    Retry,
    Reject(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) enum ClientEmitterRefusal {
    Closed,
    SchemaMismatch,
    InvalidWindow,
    NodeBudgetFull,
    StaleReference,
    WrongConsumer,
    InvalidReason,
}

pub(crate) struct ClientEmitterEndpoint {
    commands: mpsc::UnboundedSender<Command>,
    description: ClientEmitterDescription,
    budget: ClientEmitterBudget,
    series: ClientEmitterSeries,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub(crate) struct ClientEmitterDescription {
    pub(crate) fields: Vec<SchemaField>,
    pub(crate) maximum_payload_bytes: u64,
    pub(crate) maximum_payload_rows: u32,
    pub(crate) window: AckWindow,
    pub(crate) ack_timeout: Duration,
    pub(crate) retry_backoff: Duration,
    pub(crate) retry_max_backoff: Duration,
}

pub(crate) struct ClientEmitterConsumer {
    responder: ClientEmitterResponder,
    pub(crate) deliveries: mpsc::UnboundedReceiver<ClientEmitterDelivery>,
}

#[derive(Clone)]
pub(crate) struct ClientEmitterResponder {
    inner: Arc<ConsumerResponderInner>,
}

struct ConsumerResponderInner {
    commands: mpsc::UnboundedSender<Command>,
    id: u64,
    closed: AtomicBool,
}

impl Drop for ConsumerResponderInner {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.commands
                .send(Command::Detach(self.id))
                .means_shutdown("client emitter delivery owner");
        }
    }
}

impl ClientEmitterResponder {
    pub(crate) async fn answer(
        &self,
        reference: Uuid,
        answer: ClientEmitterAnswer,
    ) -> Result<(), ClientEmitterRefusal> {
        let (reply, result) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Answer {
                consumer: self.inner.id,
                reference,
                answer,
                reply,
            })
            .map_err(|_| ClientEmitterRefusal::Closed)?;
        result.await.unwrap_or(Err(ClientEmitterRefusal::Closed))
    }

    pub(crate) fn close(&self) {
        if !self.inner.closed.swap(true, Ordering::AcqRel) {
            self.inner
                .commands
                .send(Command::Detach(self.inner.id))
                .means_shutdown("client emitter delivery owner");
        }
    }
}

impl ClientEmitterConsumer {
    #[cfg(test)]
    pub(crate) async fn answer(
        &self,
        reference: Uuid,
        answer: ClientEmitterAnswer,
    ) -> Result<(), ClientEmitterRefusal> {
        self.responder.answer(reference, answer).await
    }

    pub(crate) fn split(
        self,
    ) -> (
        ClientEmitterResponder,
        mpsc::UnboundedReceiver<ClientEmitterDelivery>,
    ) {
        (self.responder, self.deliveries)
    }
}

impl ClientEmitterEndpoint {
    pub(crate) fn new(
        description: ClientEmitterDescription,
        budget: ClientEmitterBudget,
        series: ClientEmitterSeries,
    ) -> Self {
        Self::new_with_references(description, budget, series, Uuid::now_v7, true)
    }

    fn new_with_references<R>(
        description: ClientEmitterDescription,
        budget: ClientEmitterBudget,
        series: ClientEmitterSeries,
        references: R,
        deadline_wakes: bool,
    ) -> Self
    where
        R: FnMut() -> Uuid + Send + 'static,
    {
        series.reset_gauges();
        let (commands, receiver) = mpsc::unbounded_channel();
        nervix_primitives::task::spawn(
            Owner {
                commands: receiver,
                deliveries: HashMap::default(),
                pending: VecDeque::new(),
                consumers: HashMap::default(),
                consumer_order: VecDeque::new(),
                attempts: HashMap::default(),
                attempt_order: VecDeque::new(),
                next_consumer: 1,
                next_sequence: 1,
                window: description.window,
                ack_timeout: description.ack_timeout,
                retry_backoff: description.retry_backoff,
                retry_max_backoff: description.retry_max_backoff,
                series: series.clone(),
                references,
                deadline_wakes,
            }
            .run(),
        );
        Self {
            commands,
            description,
            budget,
            series,
        }
    }

    pub(crate) fn description(&self) -> ClientEmitterDescription {
        self.description.clone()
    }

    pub(crate) async fn open(
        &self,
        expected_fields: &[SchemaField],
        max_batches: u32,
        max_bytes: NonZeroU64,
        forwarded: bool,
    ) -> Result<ClientEmitterConsumer, ClientEmitterRefusal> {
        if expected_fields != self.description.fields {
            return Err(ClientEmitterRefusal::SchemaMismatch);
        }
        if max_batches == 0
            || max_bytes.get() < self.description.maximum_payload_bytes
            || max_bytes.get() > CLIENT_CONSUMER_SESSION_BYTES
        {
            return Err(ClientEmitterRefusal::InvalidWindow);
        }
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command::Attach {
                max_batches,
                max_bytes: max_bytes.get(),
                forwarded,
                reply,
            })
            .map_err(|_| ClientEmitterRefusal::Closed)?;
        let (id, deliveries) = result.await.map_err(|_| ClientEmitterRefusal::Closed)?;
        Ok(ClientEmitterConsumer {
            responder: ClientEmitterResponder {
                inner: Arc::new(ConsumerResponderInner {
                    commands: self.commands.clone(),
                    id,
                    closed: AtomicBool::new(false),
                }),
            },
            deliveries,
        })
    }

    /// Keeps the exact prepared payload alive until the application ACKs or rejects it. Dropping
    /// this future cancels the delivery and revokes its current attempt.
    pub(crate) async fn publish(
        &self,
        payload: ClientEmitterPayload,
    ) -> Result<ClientEmitterResult, ClientEmitterRefusal> {
        let bytes = NonZeroU64::new(
            u64::try_from(payload.body.len())
                .assured("an in-memory IPC payload length fits in u64"),
        )
        .ok_or(ClientEmitterRefusal::InvalidWindow)?;
        if bytes.get() > self.description.maximum_payload_bytes {
            return Err(ClientEmitterRefusal::InvalidWindow);
        }
        let reservation = self
            .budget
            .reserve(bytes)
            .await
            .ok_or(ClientEmitterRefusal::InvalidWindow)?;
        let (reply, result) = oneshot::channel();
        let identity = payload.identity;
        let mut guard = PublishGuard {
            commands: self.commands.clone(),
            identity,
            armed: true,
        };
        self.commands
            .send(Command::Publish {
                payload,
                reservation,
                reply,
            })
            .map_err(|_| ClientEmitterRefusal::Closed)?;
        let outcome = result.await.map_err(|_| ClientEmitterRefusal::Closed)?;
        guard.armed = false;
        Ok(outcome)
    }

    pub(crate) fn end(&self) {
        self.commands
            .send(Command::End {
                reset_metrics: true,
            })
            .means_shutdown("client emitter delivery owner");
    }

    pub(crate) fn replace(&self) {
        self.commands
            .send(Command::End {
                reset_metrics: false,
            })
            .means_shutdown("replaced client emitter delivery owner");
    }

    pub(crate) fn metric_lines(&self) -> Vec<String> {
        self.series.describe_lines()
    }
}

impl Runtime {
    pub(crate) fn client_emitter_budget(&self) -> ClientEmitterBudget {
        self.inner.client_emitter_budget.clone()
    }

    pub(crate) async fn open_client_consumer(
        &self,
        domain: &DomainName,
        emitter: &EmitterName,
        expected_fields: &[SchemaField],
        max_batches: u32,
        max_bytes: NonZeroU64,
        forwarded: bool,
    ) -> Result<(ClientEmitterDescription, ClientEmitterConsumer), ClientEmitterRefusal> {
        let key = DomainNodeRef::node_in(domain.clone(), ModelKind::Emitter, emitter.clone());
        let Some(endpoint) = self.inner.client_emitters.get(&key) else {
            return Err(ClientEmitterRefusal::Closed);
        };
        let endpoint = endpoint.clone();
        let description = endpoint.description();
        let consumer = endpoint
            .open(expected_fields, max_batches, max_bytes, forwarded)
            .await?;
        Ok((description, consumer))
    }
}

struct PublishGuard {
    commands: mpsc::UnboundedSender<Command>,
    identity: Uuid,
    armed: bool,
}
impl Drop for PublishGuard {
    fn drop(&mut self) {
        if self.armed {
            self.commands
                .send(Command::Cancel(self.identity))
                .means_shutdown("client emitter delivery owner");
        }
    }
}

enum Command {
    Attach {
        max_batches: u32,
        max_bytes: u64,
        forwarded: bool,
        reply: oneshot::Sender<(u64, mpsc::UnboundedReceiver<ClientEmitterDelivery>)>,
    },
    Detach(u64),
    Publish {
        payload: ClientEmitterPayload,
        reservation: ClientEmitterReservation,
        reply: oneshot::Sender<ClientEmitterResult>,
    },
    Cancel(Uuid),
    Answer {
        consumer: u64,
        reference: Uuid,
        answer: ClientEmitterAnswer,
        reply: oneshot::Sender<Result<(), ClientEmitterRefusal>>,
    },
    End {
        reset_metrics: bool,
    },
}

struct Consumer {
    events: mpsc::UnboundedSender<ClientEmitterDelivery>,
    max_batches: u32,
    max_bytes: u64,
    forwarded: bool,
    used_batches: u32,
    used_bytes: u64,
}

enum DeliveryPhase {
    Pending {
        due: Instant,
    },
    Assigned {
        consumer: u64,
        reference: Uuid,
        deadline: Instant,
    },
}

struct RetainedDelivery {
    payload: ClientEmitterPayload,
    sequence: u64,
    _reservation: ClientEmitterReservation,
    reply: oneshot::Sender<ClientEmitterResult>,
    phase: DeliveryPhase,
    retries: u32,
}

#[derive(Clone, PartialEq, Eq)]
enum AttemptHistory {
    Live { identity: Uuid, consumer: u64 },
    Acknowledged { consumer: u64 },
    Retried { consumer: u64 },
    Rejected { consumer: u64 },
    Stale,
}

struct Owner<R> {
    commands: mpsc::UnboundedReceiver<Command>,
    deliveries: HashMap<Uuid, RetainedDelivery>,
    pending: VecDeque<Uuid>,
    consumers: HashMap<u64, Consumer>,
    consumer_order: VecDeque<u64>,
    attempts: HashMap<Uuid, AttemptHistory>,
    attempt_order: VecDeque<Uuid>,
    next_consumer: u64,
    next_sequence: u64,
    window: AckWindow,
    ack_timeout: Duration,
    retry_backoff: Duration,
    retry_max_backoff: Duration,
    series: ClientEmitterSeries,
    references: R,
    deadline_wakes: bool,
}

impl<R: FnMut() -> Uuid + Send + 'static> Owner<R> {
    async fn run(mut self) {
        loop {
            self.expire();
            self.dispatch();
            let wake = self.next_wake();
            let command = match wake {
                Some(wake) => nervix_primitives::select! {
                    command = self.commands.recv() => command,
                    _ = sleep_until(wake) => continue,
                },
                None => self.commands.recv().await,
            };
            let Some(command) = command else {
                self.series.reset_gauges();
                break;
            };
            if self.command(command) {
                break;
            }
        }
    }

    fn command(&mut self, command: Command) -> bool {
        match command {
            Command::Attach {
                max_batches,
                max_bytes,
                forwarded,
                reply,
            } => {
                let id = self.next_consumer;
                self.next_consumer = self
                    .next_consumer
                    .checked_add(1)
                    .verified("a node cannot attach u64::MAX consumers during its lifetime");
                let (events, receiver) = mpsc::unbounded_channel();
                self.consumers.insert(
                    id,
                    Consumer {
                        events,
                        max_batches,
                        max_bytes,
                        forwarded,
                        used_batches: 0,
                        used_bytes: 0,
                    },
                );
                self.consumer_order.push_back(id);
                self.series.attach(forwarded, max_bytes);
                if reply.send((id, receiver)).is_err() {
                    self.detach(id);
                }
            }
            Command::Detach(id) => self.detach(id),
            Command::Publish {
                payload,
                reservation,
                reply,
            } => {
                let identity = payload.identity;
                if self.deliveries.contains_key(&identity) {
                    return false;
                }
                self.series.retain(payload.body.len());
                let sequence = self.next_sequence;
                self.next_sequence = self
                    .next_sequence
                    .checked_add(1)
                    .verified("a node cannot retain u64::MAX client batches during its lifetime");
                self.deliveries.insert(
                    identity,
                    RetainedDelivery {
                        payload,
                        sequence,
                        _reservation: reservation,
                        reply,
                        phase: DeliveryPhase::Pending {
                            due: Instant::now(),
                        },
                        retries: 0,
                    },
                );
                self.pending.push_back(identity);
            }
            Command::Cancel(identity) => self.cancel(identity),
            Command::Answer {
                consumer,
                reference,
                answer,
                reply,
            } => {
                reply
                    .send(self.answer(consumer, reference, answer))
                    .means_peer_left("client emitter settlement requester");
            }
            Command::End { reset_metrics } => {
                if reset_metrics {
                    self.series.reset_gauges();
                }
                return true;
            }
        }
        false
    }

    fn answer(
        &mut self,
        consumer: u64,
        reference: Uuid,
        answer: ClientEmitterAnswer,
    ) -> Result<(), ClientEmitterRefusal> {
        if let ClientEmitterAnswer::Reject(reason) = &answer
            && (reason.is_empty() || reason.len() > 1024)
        {
            return Err(ClientEmitterRefusal::InvalidReason);
        }
        let history = self
            .attempts
            .get(&reference)
            .cloned()
            .ok_or(ClientEmitterRefusal::StaleReference)?;
        let AttemptHistory::Live {
            identity,
            consumer: owner,
        } = history
        else {
            return match (history, answer) {
                (AttemptHistory::Acknowledged { consumer: owner }, ClientEmitterAnswer::Ack)
                | (AttemptHistory::Retried { consumer: owner }, ClientEmitterAnswer::Retry)
                | (AttemptHistory::Rejected { consumer: owner }, ClientEmitterAnswer::Reject(_))
                    if owner == consumer =>
                {
                    Ok(())
                }
                _ => Err(ClientEmitterRefusal::StaleReference),
            };
        };
        if owner != consumer {
            return Err(ClientEmitterRefusal::WrongConsumer);
        }
        let Some(delivery) = self.deliveries.get(&identity) else {
            return Err(ClientEmitterRefusal::StaleReference);
        };
        if !matches!(delivery.phase, DeliveryPhase::Assigned { reference: active, .. } if active == reference)
        {
            return Err(ClientEmitterRefusal::StaleReference);
        }
        self.release_assignment(identity);
        let history = match answer {
            ClientEmitterAnswer::Ack => {
                self.series.ack();
                self.complete(identity, ClientEmitterResult::Acknowledged);
                AttemptHistory::Acknowledged { consumer }
            }
            ClientEmitterAnswer::Retry => {
                self.retry(identity);
                AttemptHistory::Retried { consumer }
            }
            ClientEmitterAnswer::Reject(reason) => {
                self.series.reject();
                self.complete(identity, ClientEmitterResult::Rejected(reason));
                AttemptHistory::Rejected { consumer }
            }
        };
        self.attempts.insert(reference, history);
        self.remember_result(reference);
        Ok(())
    }

    fn complete(&mut self, identity: Uuid, result: ClientEmitterResult) {
        if let Some(delivery) = self.deliveries.remove(&identity) {
            self.series.release(delivery.payload.body.len());
            delivery
                .reply
                .send(result)
                .means_peer_left("client emitter publisher");
        }
    }

    fn retry(&mut self, identity: Uuid) {
        if let Some(delivery) = self.deliveries.get_mut(&identity) {
            self.series.retry();
            if delivery.retries < 9 {
                delivery.retries += 1;
            }
            let multiplier = 1_u32 << (delivery.retries - 1);
            let delay = self
                .retry_backoff
                .checked_mul(multiplier)
                .unwrap_or(self.retry_max_backoff)
                .min(self.retry_max_backoff);
            delivery.phase = DeliveryPhase::Pending {
                due: Instant::now() + delay,
            };
            self.pending.push_back(identity);
        }
    }

    fn release_assignment(&mut self, identity: Uuid) {
        let Some(delivery) = self.deliveries.get(&identity) else {
            return;
        };
        let DeliveryPhase::Assigned { consumer, .. } = delivery.phase else {
            return;
        };
        if let Some(worker) = self.consumers.get_mut(&consumer) {
            worker.used_batches -= 1;
            worker.used_bytes -= u64::try_from(delivery.payload.body.len())
                .assured("an in-memory IPC payload length fits in u64");
            self.series
                .unassign(delivery.payload.body.len(), worker.forwarded);
        }
    }

    fn detach(&mut self, id: u64) {
        let assigned = self
            .deliveries
            .iter()
            .filter_map(|(identity, delivery)| {
                matches!(delivery.phase, DeliveryPhase::Assigned { consumer, .. } if consumer == id)
                    .then_some(*identity)
            })
            .collect::<Vec<_>>();
        for identity in assigned {
            self.revoke(identity);
        }
        if let Some(consumer) = self.consumers.remove(&id) {
            self.series.detach(consumer.forwarded, consumer.max_bytes);
        }
        self.consumer_order.retain(|candidate| *candidate != id);
    }

    fn cancel(&mut self, identity: Uuid) {
        if let Some(delivery) = self.deliveries.get(&identity)
            && let DeliveryPhase::Assigned { reference, .. } = delivery.phase
        {
            self.attempts.insert(reference, AttemptHistory::Stale);
            self.remember_result(reference);
            self.release_assignment(identity);
        }
        if let Some(delivery) = self.deliveries.remove(&identity) {
            self.series.release(delivery.payload.body.len());
        }
    }

    fn revoke(&mut self, identity: Uuid) {
        if let Some(delivery) = self.deliveries.get(&identity)
            && let DeliveryPhase::Assigned { reference, .. } = delivery.phase
        {
            self.attempts.insert(reference, AttemptHistory::Stale);
            self.remember_result(reference);
            self.release_assignment(identity);
        }
        self.retry(identity);
    }

    fn expire(&mut self) {
        let now = Instant::now();
        let expired = self.deliveries.iter().filter_map(|(identity, delivery)| {
            matches!(delivery.phase, DeliveryPhase::Assigned { deadline, .. } if deadline <= now)
                .then_some(*identity)
        }).collect::<Vec<_>>();
        for identity in expired {
            self.revoke(identity);
        }
    }

    fn next_wake(&self) -> Option<Instant> {
        if !self.deadline_wakes {
            return None;
        }
        self.deliveries
            .values()
            .filter_map(|delivery| match delivery.phase {
                DeliveryPhase::Assigned { deadline, .. } => Some(deadline),
                DeliveryPhase::Pending { due } if due > Instant::now() => Some(due),
                _ => None,
            })
            .min()
    }

    fn stream_capacity(&self, identity: Uuid) -> bool {
        let Some(delivery) = self.deliveries.get(&identity) else {
            return false;
        };
        if matches!(self.window, AckWindow::Sequential)
            && self.deliveries.values().any(|candidate| {
                candidate.sequence < delivery.sequence
                    && candidate.payload.source == delivery.payload.source
                    && candidate.payload.branch == delivery.payload.branch
            })
        {
            return false;
        }
        let active = u64::try_from(
            self.deliveries
                .values()
                .filter(|candidate| {
                    matches!(candidate.phase, DeliveryPhase::Assigned { .. })
                        && candidate.payload.source == delivery.payload.source
                        && candidate.payload.branch == delivery.payload.branch
                })
                .count(),
        )
        .assured("the in-memory number of active attempts fits in u64");
        let limit = match self.window {
            AckWindow::Sequential => 1,
            AckWindow::Parallel { max } => max.get(),
        };
        active < limit
    }

    fn available_consumer(&mut self, bytes: u64) -> Option<u64> {
        for _ in 0..self.consumer_order.len() {
            let id = self.consumer_order.pop_front()?;
            self.consumer_order.push_back(id);
            let Some(consumer) = self.consumers.get(&id) else {
                continue;
            };
            if consumer.used_batches < consumer.max_batches
                && consumer
                    .used_bytes
                    .checked_add(bytes)
                    .is_some_and(|used| used <= consumer.max_bytes)
            {
                return Some(id);
            }
        }
        None
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the retained delivery owner invokes its reference allocator; local \
                      allocator bodies remain checked"
        )
    )]
    fn dispatch(&mut self) {
        let rounds = self.pending.len();
        for _ in 0..rounds {
            let Some(identity) = self.pending.pop_front() else {
                break;
            };
            let Some(delivery) = self.deliveries.get(&identity) else {
                continue;
            };
            let DeliveryPhase::Pending { due } = delivery.phase else {
                continue;
            };
            let bytes = u64::try_from(delivery.payload.body.len())
                .assured("an in-memory IPC payload length fits in u64");
            if due > Instant::now() || !self.stream_capacity(identity) {
                self.pending.push_back(identity);
                continue;
            }
            let Some(consumer) = self.available_consumer(bytes) else {
                self.pending.push_back(identity);
                continue;
            };
            let reference = (self.references)();
            let delivery = self
                .deliveries
                .get_mut(&identity)
                .verified("the pending queue only contains retained delivery identities");
            delivery.phase = DeliveryPhase::Assigned {
                consumer,
                reference,
                deadline: Instant::now() + self.ack_timeout,
            };
            let event = ClientEmitterDelivery {
                payload: delivery.payload.clone(),
                reference,
            };
            let worker = self
                .consumers
                .get_mut(&consumer)
                .verified("the chosen consumer remains attached until this dispatch ends");
            worker.used_batches += 1;
            worker.used_bytes += bytes;
            self.series
                .assign(delivery.payload.body.len(), worker.forwarded);
            self.attempts
                .insert(reference, AttemptHistory::Live { identity, consumer });
            if worker.events.send(event).is_err() {
                self.detach(consumer);
            }
        }
    }

    fn remember_result(&mut self, reference: Uuid) {
        self.attempt_order.push_back(reference);
        while self.attempt_order.len() > RETAINED_RESULTS {
            if let Some(expired) = self.attempt_order.pop_front() {
                self.attempts.remove(&expired);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use nervix_models::{FieldName, ParseAsType};

    use super::*;
    use crate::runtime::RuntimeValue;

    fn fields() -> Vec<SchemaField> {
        vec![SchemaField {
            name: FieldName::parse("id").assured("literal field name"),
            ty: ParseAsType::String,
            optional: false,
            sensitive: false,
        }]
    }

    fn description(window: AckWindow, ack_timeout: Duration) -> ClientEmitterDescription {
        ClientEmitterDescription {
            fields: fields(),
            maximum_payload_bytes: 1024,
            maximum_payload_rows: 64,
            window,
            ack_timeout,
            retry_backoff: Duration::from_millis(1),
            retry_max_backoff: Duration::from_millis(8),
        }
    }

    fn series() -> ClientEmitterSeries {
        crate::metrics::RuntimeMetrics::default().client_emitter_series(
            &DomainName::parse("test").assured("literal domain name"),
            &EmitterName::parse("output").assured("literal emitter name"),
        )
    }

    #[nervix_primitives::test]
    async fn consumers_require_the_exact_output_contract_and_enough_credit() {
        let budget = ClientEmitterBudget::default();
        let endpoint = ClientEmitterEndpoint::new(
            description(AckWindow::Sequential, Duration::from_secs(2)),
            budget.clone(),
            series(),
        );
        let mut different = fields();
        different[0].sensitive = true;
        let credit = NonZeroU64::new(1024).assured("literal credit");
        assert!(matches!(
            endpoint.open(&different, 1, credit, false).await,
            Err(ClientEmitterRefusal::SchemaMismatch)
        ));
        assert!(matches!(
            endpoint.open(&fields(), 0, credit, false).await,
            Err(ClientEmitterRefusal::InvalidWindow)
        ));
        assert!(matches!(
            endpoint
                .open(
                    &fields(),
                    1,
                    NonZeroU64::new(1023).assured("literal credit"),
                    false,
                )
                .await,
            Err(ClientEmitterRefusal::InvalidWindow)
        ));
        let full = NonZeroU64::new(CLIENT_CONSUMER_NODE_BYTES).assured("node credit is nonzero");
        let grant = budget
            .try_grant(full)
            .expect("full node budget is available");
        assert!(budget.try_grant(credit).is_none());
        drop(grant);
        assert!(budget.try_grant(credit).is_some());
    }

    #[nervix_primitives::test]
    async fn rejecting_a_live_attempt_returns_the_application_reason_to_the_source() {
        let endpoint = ClientEmitterEndpoint::new(
            description(AckWindow::Sequential, Duration::from_secs(2)),
            ClientEmitterBudget::default(),
            series(),
        );
        let mut consumer = endpoint
            .open(
                &fields(),
                1,
                NonZeroU64::new(1024).assured("literal credit"),
                false,
            )
            .await
            .expect("consumer opens");
        let publisher = nervix_primitives::task::spawn(async move {
            endpoint
                .publish(ClientEmitterPayload {
                    identity: Uuid::now_v7(),
                    source: RelayName::parse("orders").assured("literal relay name"),
                    branch: None,
                    body: Bytes::from_static(b"arrow ipc test bytes"),
                    members: 1,
                    execution_now: Timestamp::now(),
                })
                .await
        });
        let attempt =
            nervix_primitives::time::timeout(Duration::from_secs(1), consumer.deliveries.recv())
                .await
                .expect("attempt arrives")
                .expect("consumer stays open");
        assert_eq!(
            consumer
                .answer(
                    attempt.reference,
                    ClientEmitterAnswer::Reject(String::new())
                )
                .await,
            Err(ClientEmitterRefusal::InvalidReason)
        );
        consumer
            .answer(
                attempt.reference,
                ClientEmitterAnswer::Reject("application rejected".into()),
            )
            .await
            .expect("rejection confirmed");
        assert_eq!(
            publisher.await.expect("publisher runs"),
            Ok(ClientEmitterResult::Rejected("application rejected".into()))
        );
        consumer
            .answer(
                attempt.reference,
                ClientEmitterAnswer::Reject("same result".into()),
            )
            .await
            .expect("duplicate rejection is confirmed while retained");
    }

    #[nervix_primitives::test]
    async fn a_retry_revokes_the_first_worker_and_keeps_the_payload_until_ack() {
        let budget = ClientEmitterBudget::default();
        let endpoint = ClientEmitterEndpoint::new(
            description(AckWindow::Sequential, Duration::from_secs(2)),
            budget.clone(),
            series(),
        );
        let credit = NonZeroU64::new(1024).assured("literal credit");
        let mut first = endpoint
            .open(&fields(), 1, credit, false)
            .await
            .expect("first consumer opens");
        let mut second = endpoint
            .open(&fields(), 1, credit, false)
            .await
            .expect("second consumer opens");
        let body = Bytes::from_static(b"arrow ipc test bytes");
        let identity = Uuid::now_v7();
        let producer = nervix_primitives::task::spawn(async move {
            endpoint
                .publish(ClientEmitterPayload {
                    identity,
                    source: RelayName::parse("orders").assured("literal relay name"),
                    branch: None,
                    body,
                    members: 1,
                    execution_now: Timestamp::now(),
                })
                .await
        });
        let attempt =
            nervix_primitives::time::timeout(Duration::from_secs(1), first.deliveries.recv())
                .await
                .expect("first attempt arrives")
                .expect("consumer stays open");
        assert_eq!(attempt.payload.identity, identity);
        assert_eq!(
            budget.used(),
            u64::try_from(attempt.payload.body.len()).assured("test payload length fits in u64")
        );
        first
            .answer(attempt.reference, ClientEmitterAnswer::Retry)
            .await
            .expect("retry accepted");
        let replacement =
            nervix_primitives::time::timeout(Duration::from_secs(1), second.deliveries.recv())
                .await
                .expect("replacement arrives")
                .expect("consumer stays open");
        assert_eq!(replacement.payload.identity, attempt.payload.identity);
        assert_eq!(replacement.payload.body, attempt.payload.body);
        assert_ne!(replacement.reference, attempt.reference);
        assert_eq!(
            first
                .answer(attempt.reference, ClientEmitterAnswer::Ack)
                .await,
            Err(ClientEmitterRefusal::StaleReference)
        );
        second
            .answer(replacement.reference, ClientEmitterAnswer::Ack)
            .await
            .expect("ACK accepted");
        assert_eq!(
            producer.await.expect("publisher runs"),
            Ok(ClientEmitterResult::Acknowledged)
        );
        assert_eq!(budget.used(), 0);
        second
            .answer(replacement.reference, ClientEmitterAnswer::Ack)
            .await
            .expect("duplicate ACK remains idempotent while retained");
    }

    #[nervix_primitives::test]
    async fn sequential_output_blocks_only_its_own_source_and_branch() {
        let endpoint = Arc::new(ClientEmitterEndpoint::new(
            description(AckWindow::Sequential, Duration::from_secs(2)),
            ClientEmitterBudget::default(),
            series(),
        ));
        let credit = NonZeroU64::new(1024).assured("literal credit");
        let mut first = endpoint
            .open(&fields(), 1, credit, false)
            .await
            .expect("first consumer opens");
        let mut second = endpoint
            .open(&fields(), 1, credit, false)
            .await
            .expect("second consumer opens");
        let publish = |source: &'static str| {
            let endpoint = endpoint.clone();
            nervix_primitives::task::spawn(async move {
                endpoint
                    .publish(ClientEmitterPayload {
                        identity: Uuid::now_v7(),
                        source: RelayName::parse(source).assured("literal relay name"),
                        branch: None,
                        body: Bytes::from_static(b"arrow ipc test bytes"),
                        members: 1,
                        execution_now: Timestamp::now(),
                    })
                    .await
            })
        };
        let first_publisher = publish("orders");
        let first_attempt =
            nervix_primitives::time::timeout(Duration::from_secs(1), first.deliveries.recv())
                .await
                .expect("first attempt arrives")
                .expect("first consumer stays open");
        let second_publisher = publish("orders");
        let independent_publisher = publish("invoices");
        let independent_attempt =
            nervix_primitives::time::timeout(Duration::from_secs(1), second.deliveries.recv())
                .await
                .expect("independent source arrives")
                .expect("second consumer stays open");
        assert_eq!(independent_attempt.payload.source.as_str(), "invoices");
        assert!(matches!(
            first.deliveries.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        first
            .answer(first_attempt.reference, ClientEmitterAnswer::Ack)
            .await
            .expect("first ACK");
        let second_attempt =
            nervix_primitives::time::timeout(Duration::from_secs(1), first.deliveries.recv())
                .await
                .expect("second source attempt arrives after ACK")
                .expect("first consumer stays open");
        assert_eq!(second_attempt.payload.source.as_str(), "orders");
        first
            .answer(second_attempt.reference, ClientEmitterAnswer::Ack)
            .await
            .expect("second ACK");
        second
            .answer(independent_attempt.reference, ClientEmitterAnswer::Ack)
            .await
            .expect("independent ACK");
        for publisher in [first_publisher, second_publisher, independent_publisher] {
            assert_eq!(
                publisher.await.expect("publisher runs"),
                Ok(ClientEmitterResult::Acknowledged)
            );
        }
    }

    #[nervix_primitives::test]
    async fn sequential_output_assigns_distinct_branches_independently() {
        let endpoint = Arc::new(ClientEmitterEndpoint::new(
            description(AckWindow::Sequential, Duration::from_secs(2)),
            ClientEmitterBudget::default(),
            series(),
        ));
        let credit = NonZeroU64::new(1024).assured("literal credit");
        let mut first = endpoint
            .open(&fields(), 1, credit, false)
            .await
            .expect("first consumer opens");
        let mut second = endpoint
            .open(&fields(), 1, credit, false)
            .await
            .expect("second consumer opens");
        let publish = |tenant: &'static str| {
            let endpoint = endpoint.clone();
            nervix_primitives::task::spawn(async move {
                endpoint
                    .publish(ClientEmitterPayload {
                        identity: Uuid::now_v7(),
                        source: RelayName::parse("orders").assured("literal relay name"),
                        branch: Some(
                            BranchKey::from_fields([(
                                FieldName::parse("tenant").assured("literal branch field"),
                                RuntimeValue::String(tenant.to_string()),
                            )])
                            .assured("concrete branch key"),
                        ),
                        body: Bytes::from_static(b"arrow ipc test bytes"),
                        members: 1,
                        execution_now: Timestamp::now(),
                    })
                    .await
            })
        };
        let first_publisher = publish("acme");
        let first_attempt =
            nervix_primitives::time::timeout(Duration::from_secs(1), first.deliveries.recv())
                .await
                .expect("first branch arrives")
                .expect("first consumer stays open");
        let second_publisher = publish("beta");
        let second_attempt =
            nervix_primitives::time::timeout(Duration::from_secs(1), second.deliveries.recv())
                .await
                .expect("other branch arrives before first ACK")
                .expect("second consumer stays open");
        assert_ne!(first_attempt.payload.branch, second_attempt.payload.branch);
        first
            .answer(first_attempt.reference, ClientEmitterAnswer::Ack)
            .await
            .expect("first ACK");
        second
            .answer(second_attempt.reference, ClientEmitterAnswer::Ack)
            .await
            .expect("second ACK");
        assert_eq!(
            first_publisher.await.expect("publisher runs"),
            Ok(ClientEmitterResult::Acknowledged)
        );
        assert_eq!(
            second_publisher.await.expect("publisher runs"),
            Ok(ClientEmitterResult::Acknowledged)
        );
    }

    #[nervix_primitives::test]
    async fn parallel_window_and_timeout_reassignment_keep_one_live_attempt_per_delivery() {
        let endpoint = Arc::new(ClientEmitterEndpoint::new(
            description(
                AckWindow::Parallel {
                    max: NonZeroU64::new(2).assured("literal window"),
                },
                Duration::from_millis(500),
            ),
            ClientEmitterBudget::default(),
            series(),
        ));
        let credit = NonZeroU64::new(1024).assured("literal credit");
        let mut first = endpoint
            .open(&fields(), 2, credit, false)
            .await
            .expect("first consumer opens");
        let mut second = endpoint
            .open(&fields(), 2, credit, false)
            .await
            .expect("second consumer opens");
        let publish = || {
            let endpoint = endpoint.clone();
            nervix_primitives::task::spawn(async move {
                endpoint
                    .publish(ClientEmitterPayload {
                        identity: Uuid::now_v7(),
                        source: RelayName::parse("orders").assured("literal relay name"),
                        branch: None,
                        body: Bytes::from_static(b"arrow ipc test bytes"),
                        members: 1,
                        execution_now: Timestamp::now(),
                    })
                    .await
            })
        };
        let first_publisher = publish();
        let first_attempt =
            nervix_primitives::time::timeout(Duration::from_secs(1), first.deliveries.recv())
                .await
                .expect("first attempt arrives")
                .expect("first consumer stays open");
        let second_publisher = publish();
        let second_attempt =
            nervix_primitives::time::timeout(Duration::from_secs(1), second.deliveries.recv())
                .await
                .expect("second attempt arrives")
                .expect("second consumer stays open");
        let third_publisher = publish();
        nervix_primitives::task::yield_now().await;
        assert!(matches!(
            first.deliveries.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            second.deliveries.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        first
            .answer(first_attempt.reference, ClientEmitterAnswer::Ack)
            .await
            .expect("first ACK");
        let third_attempt =
            nervix_primitives::time::timeout(Duration::from_secs(1), first.deliveries.recv())
                .await
                .expect("third attempt starts after the first ACK")
                .expect("first consumer stays open");
        assert_ne!(
            third_attempt.payload.identity,
            first_attempt.payload.identity
        );
        second
            .answer(second_attempt.reference, ClientEmitterAnswer::Ack)
            .await
            .expect("second ACK");
        first
            .answer(third_attempt.reference, ClientEmitterAnswer::Ack)
            .await
            .expect("third ACK");
        for publisher in [first_publisher, second_publisher, third_publisher] {
            assert_eq!(
                publisher.await.expect("publisher runs"),
                Ok(ClientEmitterResult::Acknowledged)
            );
        }

        let retry_publisher = publish();
        let timed_attempt =
            nervix_primitives::time::timeout(Duration::from_secs(1), second.deliveries.recv())
                .await
                .expect("timed attempt arrives")
                .expect("second consumer stays open");
        let replacement =
            nervix_primitives::time::timeout(Duration::from_secs(2), first.deliveries.recv())
                .await
                .expect("replacement arrives after timeout")
                .expect("first consumer stays open");
        assert_eq!(replacement.payload.identity, timed_attempt.payload.identity);
        assert_ne!(replacement.reference, timed_attempt.reference);
        assert_eq!(
            second
                .answer(timed_attempt.reference, ClientEmitterAnswer::Ack)
                .await,
            Err(ClientEmitterRefusal::StaleReference)
        );
        first
            .answer(replacement.reference, ClientEmitterAnswer::Ack)
            .await
            .expect("replacement ACK");
        assert_eq!(
            retry_publisher.await.expect("publisher runs"),
            Ok(ClientEmitterResult::Acknowledged)
        );
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "client_emitter_shuttle_tests.rs"]
mod shuttle_tests;
