//! Shared relay-consumer scheduling.
//!
//! This module owns the mechanics that are independent of a concrete runtime node: named relay
//! fan-in, source-local and branch-local collection, wake/force-flush arbitration, receiver-local
//! input draining, and quiesce work accounting. Nodes retain their processing and output behavior.
//!
//! The mode deliberately uses Tokio's wall-clock [`Instant`]. Emitters and reingestors have this
//! contract today, while materializers use it for their wall-clock expiration scan. Processor
//! supervisors use it for relay fan-in, but their branch workers retain paced domain timestamps
//! and operation-owned buffers behind that boundary. Connector ingestors consume external
//! transports rather than relays and remain outside this boundary.
//!
//! Force flush and watch-triggered shutdown capture a finite count from every receiver. The
//! interaction drains exactly that cut, releases every collection, and only then emits the control
//! event. This keeps a hot source from extending a flush forever. A drain-first command is the
//! graceful topology path: callers fence relay dispatch gates before sending it. Dropping the mode
//! terminally resolves any input left outside a finite cut instead of stranding attached ACKs.
//! Watch-triggered shutdown is therefore a terminal cut for one receiver, not a graph-wide drain:
//! an upstream node can still publish after a downstream receiver captured its cut. Domain
//! lifecycle code must stop graph sources and prove relay buffers, node work, and force-flush
//! obligations are quiescent before broadcasting the terminal watch value.

use std::{future::pending, task::Poll};

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use nervix_models::Identifier;
use thiserror::Error;
use tokio::{
    sync::{mpsc, watch},
    time::{Duration, Instant, sleep_until},
};
use triomphe::Arc;

use super::{
    BranchKey, DomainForceFlushCompletion, DomainForceFlushParticipant, NodeQuiesceCounters,
    NodeQuiesceWorkGuard, RelayRecordBatch, RelayRuntimeFanIn,
};
use crate::runtime_ack::AckSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RuntimeInputCollectPolicy {
    pub(super) interval: Duration,
    pub(super) max_batch_size: Option<u64>,
}

impl RuntimeInputCollectPolicy {
    pub(super) fn size_boundary_reached(self, pending_bytes: u64) -> bool {
        self.max_batch_size
            .is_some_and(|max_batch_size| pending_bytes >= max_batch_size)
    }
}

#[derive(Debug, Error)]
pub(super) enum RelayInteractionError {
    #[error("relay interaction requires at least one input")]
    NoInputs,
    #[error("relay interaction input relay '{relay}' is declared more than once")]
    DuplicateInput { relay: Identifier },
    #[error("failed to concatenate collected input from relay '{relay}': {reason}")]
    Concatenate {
        relay: Identifier,
        reason: String,
        acks: AckSet,
    },
}

impl RelayInteractionError {
    pub(super) fn acks(&self) -> Option<&AckSet> {
        if let Self::Concatenate { acks, .. } = self {
            Some(acks)
        } else {
            None
        }
    }
}

/// Commands classify whether already accepted relay input must be handled before the command.
pub(super) trait RelayInteractionCommand: Send {
    fn drain_inputs_before_handling(&self) -> bool {
        false
    }

    /// Whether an input accepted by the command's finite drain may still wait for external state.
    ///
    /// Terminal commands use this to prevent a ready batch from turning graceful shutdown into an
    /// unbounded wait. Non-terminal drain commands can retain their ordinary backpressure
    /// contract.
    fn cancels_external_waits_while_draining(&self) -> bool {
        false
    }
}

#[derive(Debug)]
pub(super) enum NoRelayInteractionCommand {}

impl RelayInteractionCommand for NoRelayInteractionCommand {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RelayInteractionStop {
    Shutdown,
    InputsClosed,
    CommandsClosed,
    ForceFlushClosed,
}

#[derive(Debug)]
pub(super) enum RelayInteractionEvent<C> {
    Batch {
        relay: Identifier,
        batch: RelayRecordBatch,
    },
    Wake,
    ForceFlush(DomainForceFlushCompletion),
    Command(C),
    Stopped(RelayInteractionStop),
}

pub(super) struct RelayInteractionWork<C> {
    event: RelayInteractionEvent<C>,
    work: Option<NodeQuiesceWorkGuard>,
}

impl<C> RelayInteractionWork<C> {
    pub(super) fn into_parts(self) -> (RelayInteractionEvent<C>, Option<NodeQuiesceWorkGuard>) {
        (self.event, self.work)
    }
}

pub(super) struct RelayInteractionInput {
    relay: Identifier,
    receiver: RelayRuntimeFanIn,
    collect_policy: Option<RuntimeInputCollectPolicy>,
}

impl RelayInteractionInput {
    pub(super) fn new(
        relay: Identifier,
        receiver: RelayRuntimeFanIn,
        collect_policy: Option<RuntimeInputCollectPolicy>,
    ) -> Self {
        Self {
            relay,
            receiver,
            collect_policy,
        }
    }
}

#[derive(Debug, Default)]
struct RelayInputBranchCollection {
    batches: Vec<RelayRecordBatch>,
    bytes: u64,
    deadline: Option<Instant>,
}

#[derive(Debug)]
struct RelayInputCollectionError {
    reason: String,
    acks: AckSet,
}

#[derive(Debug)]
struct RelayInputCollection {
    policy: Option<RuntimeInputCollectPolicy>,
    pending: HashMap<Option<BranchKey>, RelayInputBranchCollection>,
    branch_order: Vec<Option<BranchKey>>,
    quiesce_counters: Option<Arc<NodeQuiesceCounters>>,
    pending_batches: usize,
}

impl RelayInputCollection {
    fn new(
        policy: Option<RuntimeInputCollectPolicy>,
        quiesce_counters: Option<Arc<NodeQuiesceCounters>>,
    ) -> Self {
        Self {
            policy,
            pending: HashMap::new(),
            branch_order: Vec::new(),
            quiesce_counters,
            pending_batches: 0,
        }
    }

    fn push(
        &mut self,
        batch: RelayRecordBatch,
    ) -> Result<Option<RelayRecordBatch>, RelayInputCollectionError> {
        let Some(policy) = self.policy else {
            return Ok(Some(batch));
        };
        let key = batch.key.clone();
        if !self.pending.contains_key(&key) {
            self.branch_order.push(key.clone());
        }
        let collection = self.pending.entry(key.clone()).or_default();
        collection.bytes = collection.bytes.saturating_add(batch.estimated_bytes());
        collection.batches.push(batch);
        self.pending_batches = self.pending_batches.saturating_add(1);
        if let Some(counters) = &self.quiesce_counters {
            counters
                .collected_inputs
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        }
        collection
            .deadline
            .get_or_insert_with(|| Instant::now() + policy.interval);
        if policy.size_boundary_reached(collection.bytes) {
            return self.take(&key).map(Some);
        }
        Ok(None)
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.pending
            .values()
            .filter_map(|collection| collection.deadline)
            .min()
    }

    fn take_due(
        &mut self,
        now: Instant,
    ) -> Result<Option<RelayRecordBatch>, RelayInputCollectionError> {
        let key = self.branch_order.iter().find_map(|key| {
            self.pending
                .get(key)
                .and_then(|collection| collection.deadline)
                .is_some_and(|deadline| deadline <= now)
                .then_some(key.clone())
        });
        let Some(key) = key else {
            return Ok(None);
        };
        self.take(&key).map(Some)
    }

    fn take_any(&mut self) -> Result<Option<RelayRecordBatch>, RelayInputCollectionError> {
        let Some(key) = self.branch_order.first().cloned() else {
            return Ok(None);
        };
        self.take(&key).map(Some)
    }

    fn take(
        &mut self,
        key: &Option<BranchKey>,
    ) -> Result<RelayRecordBatch, RelayInputCollectionError> {
        let collection = self
            .pending
            .remove(key)
            .expect("ordered input collection key must exist");
        self.branch_order.retain(|candidate| candidate != key);
        self.pending_batches = self
            .pending_batches
            .saturating_sub(collection.batches.len());
        if let Some(counters) = &self.quiesce_counters {
            counters.collected_inputs.fetch_sub(
                collection.batches.len(),
                std::sync::atomic::Ordering::AcqRel,
            );
        }
        RelayRecordBatch::concat_preserving(collection.batches).map_err(|error| {
            let (reason, batches) = *error;
            let acks = AckSet::merged(batches.iter().map(RelayRecordBatch::merged_acks));
            RelayInputCollectionError { reason, acks }
        })
    }
}

impl Drop for RelayInputCollection {
    fn drop(&mut self) {
        if let Some(counters) = &self.quiesce_counters {
            counters
                .collected_inputs
                .fetch_sub(self.pending_batches, std::sync::atomic::Ordering::AcqRel);
        }
        for batch in self
            .pending
            .values()
            .flat_map(|collection| &collection.batches)
        {
            batch
                .merged_acks()
                .no_ack("relay interaction dropped collected input");
        }
    }
}

struct RelayInteractionSource {
    relay: Identifier,
    receiver: RelayRuntimeFanIn,
    collection: RelayInputCollection,
    closed: bool,
}

impl Drop for RelayInteractionSource {
    fn drop(&mut self) {
        loop {
            match self.receiver.try_recv() {
                Ok(batch) => batch
                    .merged_acks()
                    .no_ack("relay interaction dropped queued input"),
                Err(
                    async_broadcast::TryRecvError::Empty | async_broadcast::TryRecvError::Closed,
                ) => break,
                Err(async_broadcast::TryRecvError::Overflowed(_)) => {
                    unreachable!("relay broadcasts are backpressured and must not overflow")
                }
            }
        }
    }
}

struct RelayInteractionInputs {
    sources: Vec<RelayInteractionSource>,
    receive_cursor: usize,
    collection_cursor: usize,
    quiesce_counters: Option<Arc<NodeQuiesceCounters>>,
}

enum ReadyInput {
    Batch(usize, RelayRecordBatch, Option<NodeQuiesceWorkGuard>),
    Exhausted,
}

impl RelayInteractionInputs {
    fn new(
        inputs: Vec<RelayInteractionInput>,
        quiesce_counters: Option<Arc<NodeQuiesceCounters>>,
    ) -> Result<Self, RelayInteractionError> {
        if inputs.is_empty() {
            return Err(RelayInteractionError::NoInputs);
        }
        let mut declared = HashSet::new();
        let mut sources = Vec::with_capacity(inputs.len());
        for input in inputs {
            if !declared.insert(input.relay.clone()) {
                return Err(RelayInteractionError::DuplicateInput { relay: input.relay });
            }
            sources.push(RelayInteractionSource {
                relay: input.relay,
                receiver: input.receiver,
                collection: RelayInputCollection::new(
                    input.collect_policy,
                    quiesce_counters.clone(),
                ),
                closed: false,
            });
        }
        Ok(Self {
            sources,
            receive_cursor: 0,
            collection_cursor: 0,
            quiesce_counters,
        })
    }

    fn pending_snapshot(&self) -> Vec<usize> {
        self.sources
            .iter()
            .map(|source| {
                if source.closed {
                    0
                } else {
                    source.receiver.pending_len()
                }
            })
            .collect()
    }

    fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<(usize, RelayRecordBatch, Option<NodeQuiesceWorkGuard>)>> {
        let source_count = self.sources.len();
        let mut open = false;
        let quiesce_counters = self.quiesce_counters.clone();
        for offset in 0..source_count {
            let index = (self.receive_cursor + offset) % source_count;
            let source = &mut self.sources[index];
            if source.closed {
                continue;
            }
            open = true;
            match source
                .receiver
                .poll_recv_with_quiesce(cx, quiesce_counters.as_ref())
            {
                Poll::Ready(Some((batch, work))) => {
                    self.receive_cursor = (index + 1) % source_count;
                    return Poll::Ready(Some((index, batch, work)));
                }
                Poll::Ready(None) => source.closed = true,
                Poll::Pending => {}
            }
        }
        if open && self.sources.iter().any(|source| !source.closed) {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }

    fn try_recv_snapshot(&mut self, remaining: &mut [usize]) -> ReadyInput {
        let source_count = self.sources.len();
        let quiesce_counters = self.quiesce_counters.clone();
        for offset in 0..source_count {
            let index = (self.receive_cursor + offset) % source_count;
            if remaining[index] == 0 || self.sources[index].closed {
                continue;
            }
            match self.sources[index]
                .receiver
                .try_recv_with_quiesce(quiesce_counters.as_ref())
            {
                Ok((batch, work)) => {
                    remaining[index] = remaining[index].saturating_sub(1);
                    self.receive_cursor = (index + 1) % source_count;
                    return ReadyInput::Batch(index, batch, work);
                }
                Err(async_broadcast::TryRecvError::Empty) => {
                    // The finite cut only includes batches ready when it was captured. `len()` and
                    // `try_recv()` are observed by this interaction's sole receiver, so this is a
                    // defensive race fallback rather than a reason to extend the drain.
                    remaining[index] = 0;
                }
                Err(async_broadcast::TryRecvError::Closed) => {
                    self.sources[index].closed = true;
                    remaining[index] = 0;
                }
                Err(async_broadcast::TryRecvError::Overflowed(_)) => {
                    unreachable!("relay broadcasts are backpressured and must not overflow")
                }
            }
        }
        ReadyInput::Exhausted
    }

    fn accept(
        &mut self,
        source: usize,
        batch: RelayRecordBatch,
    ) -> Result<Option<(Identifier, RelayRecordBatch)>, RelayInteractionError> {
        let relay = self.sources[source].relay.clone();
        self.sources[source]
            .collection
            .push(batch)
            .map(|batch| batch.map(|batch| (relay.clone(), batch)))
            .map_err(|error| RelayInteractionError::Concatenate {
                relay,
                reason: error.reason,
                acks: error.acks,
            })
    }

    fn next_collection_deadline(&self) -> Option<Instant> {
        self.sources
            .iter()
            .filter_map(|source| source.collection.next_deadline())
            .min()
    }

    fn take_due(
        &mut self,
        now: Instant,
    ) -> Result<Option<(Identifier, RelayRecordBatch)>, RelayInteractionError> {
        self.take_collection(|collection| collection.take_due(now))
    }

    fn take_any(
        &mut self,
    ) -> Result<Option<(Identifier, RelayRecordBatch)>, RelayInteractionError> {
        self.take_collection(RelayInputCollection::take_any)
    }

    fn take_collection(
        &mut self,
        mut take: impl FnMut(
            &mut RelayInputCollection,
        ) -> Result<Option<RelayRecordBatch>, RelayInputCollectionError>,
    ) -> Result<Option<(Identifier, RelayRecordBatch)>, RelayInteractionError> {
        let source_count = self.sources.len();
        for offset in 0..source_count {
            let index = (self.collection_cursor + offset) % source_count;
            let relay = self.sources[index].relay.clone();
            let batch = take(&mut self.sources[index].collection).map_err(|error| {
                RelayInteractionError::Concatenate {
                    relay: relay.clone(),
                    reason: error.reason,
                    acks: error.acks,
                }
            })?;
            if let Some(batch) = batch {
                self.collection_cursor = (index + 1) % source_count;
                return Ok(Some((relay, batch)));
            }
        }
        Ok(None)
    }
}

enum DrainFinish<C> {
    ForceFlush(DomainForceFlushCompletion),
    Command(C),
    Stop(RelayInteractionStop),
}

struct RelayInteractionDrain<C> {
    remaining: Vec<usize>,
    finish: DrainFinish<C>,
}

enum ReadyCommand<C> {
    None,
    Command(C),
    Closed,
}

pub(super) struct RelayInteraction<C> {
    inputs: RelayInteractionInputs,
    shutdown_rx: watch::Receiver<bool>,
    _drain_shutdown_tx: watch::Sender<bool>,
    drain_shutdown_rx: watch::Receiver<bool>,
    force_flush: Option<DomainForceFlushParticipant>,
    commands: Option<mpsc::Receiver<C>>,
    quiesce_counters: Option<Arc<NodeQuiesceCounters>>,
    drain: Option<RelayInteractionDrain<C>>,
    suppress_shutdown: bool,
    terminal_drain: bool,
}

impl RelayInteraction<NoRelayInteractionCommand> {
    pub(super) fn new(
        inputs: Vec<RelayInteractionInput>,
        shutdown_rx: watch::Receiver<bool>,
        force_flush: Option<DomainForceFlushParticipant>,
        quiesce_counters: Option<Arc<NodeQuiesceCounters>>,
    ) -> Result<Self, RelayInteractionError> {
        Self::build(inputs, shutdown_rx, force_flush, quiesce_counters, None)
    }
}

impl<C: RelayInteractionCommand> RelayInteraction<C> {
    pub(super) fn with_commands(
        inputs: Vec<RelayInteractionInput>,
        shutdown_rx: watch::Receiver<bool>,
        force_flush: Option<DomainForceFlushParticipant>,
        quiesce_counters: Option<Arc<NodeQuiesceCounters>>,
        commands: mpsc::Receiver<C>,
    ) -> Result<Self, RelayInteractionError> {
        Self::build(
            inputs,
            shutdown_rx,
            force_flush,
            quiesce_counters,
            Some(commands),
        )
    }

    fn build(
        inputs: Vec<RelayInteractionInput>,
        shutdown_rx: watch::Receiver<bool>,
        force_flush: Option<DomainForceFlushParticipant>,
        quiesce_counters: Option<Arc<NodeQuiesceCounters>>,
        commands: Option<mpsc::Receiver<C>>,
    ) -> Result<Self, RelayInteractionError> {
        let (drain_shutdown_tx, drain_shutdown_rx) = watch::channel(false);
        Ok(Self {
            inputs: RelayInteractionInputs::new(inputs, quiesce_counters.clone())?,
            shutdown_rx,
            _drain_shutdown_tx: drain_shutdown_tx,
            drain_shutdown_rx,
            force_flush,
            commands,
            quiesce_counters,
            drain: None,
            suppress_shutdown: false,
            terminal_drain: false,
        })
    }

    /// Returns whether the current event belongs to a finite input cut.
    pub(super) fn is_draining(&self) -> bool {
        self.drain.is_some()
    }

    /// Returns whether external waits must be canceled while handling the current finite cut.
    pub(super) fn is_terminal_drain(&self) -> bool {
        self.terminal_drain
    }

    pub(super) fn shutdown_receiver(&mut self) -> &mut watch::Receiver<bool> {
        if self.suppress_shutdown {
            &mut self.drain_shutdown_rx
        } else {
            &mut self.shutdown_rx
        }
    }

    pub(super) async fn next(
        &mut self,
        wake_at: Option<Instant>,
    ) -> Result<RelayInteractionWork<C>, RelayInteractionError> {
        self.next_with_input(wake_at, true).await
    }

    /// Returns the next control, wake, collection, or relay event while optionally pausing new
    /// relay dequeues. Already-collected input and terminal drains remain deliverable, while a
    /// force flush respects the pause and operates only on work the node already owns. This lets a
    /// node preserve relay backpressure while an external dependency is unavailable without hiding
    /// force-flush or shutdown behind the dependency wait.
    pub(super) async fn next_with_input(
        &mut self,
        wake_at: Option<Instant>,
        receive_input: bool,
    ) -> Result<RelayInteractionWork<C>, RelayInteractionError> {
        if self.drain.is_none() {
            self.suppress_shutdown = false;
            self.terminal_drain = false;
        }
        loop {
            tokio::task::consume_budget().await;

            if self.drain.is_some() {
                return self.next_drain_work().await;
            }

            match self.ready_command() {
                ReadyCommand::Command(command) => {
                    if command.drain_inputs_before_handling() {
                        self.begin_drain(DrainFinish::Command(command), true);
                        continue;
                    }
                    return Ok(self.work(RelayInteractionEvent::Command(command)));
                }
                ReadyCommand::Closed => {
                    self.begin_drain(
                        DrainFinish::Stop(RelayInteractionStop::CommandsClosed),
                        true,
                    );
                    continue;
                }
                ReadyCommand::None => {}
            }

            if *self.shutdown_rx.borrow() {
                self.begin_drain(DrainFinish::Stop(RelayInteractionStop::Shutdown), true);
                continue;
            }

            if let Some(completion) = self.force_flush_changed()? {
                self.begin_drain(DrainFinish::ForceFlush(completion), receive_input);
                continue;
            }
            if self.drain.is_some() {
                continue;
            }

            let now = Instant::now();
            if wake_at.is_some_and(|deadline| deadline <= now) {
                return Ok(self.work(RelayInteractionEvent::Wake));
            }
            let work = self.begin_work();
            if let Some((relay, batch)) = self.inputs.take_due(now)? {
                return Ok(self.work_with(RelayInteractionEvent::Batch { relay, batch }, work));
            }
            drop(work);

            let collection_at = self.inputs.next_collection_deadline();
            let selected = tokio::select! {
                biased;
                command = recv_optional_command(&mut self.commands) => Selected::Command(command),
                changed = self.shutdown_rx.changed() => {
                    Selected::Shutdown(changed.map_err(|_| ()))
                }
                changed = changed_optional_force_flush(&mut self.force_flush) => {
                    Selected::ForceFlush(changed)
                }
                _ = wait_until(wake_at) => Selected::Wake,
                _ = wait_until(collection_at) => Selected::CollectionDue,
                input = std::future::poll_fn(|cx| self.inputs.poll_recv(cx)), if receive_input => {
                    Selected::Input(input)
                }
            };
            match selected {
                Selected::Command(Some(command)) => {
                    if command.drain_inputs_before_handling() {
                        self.begin_drain(DrainFinish::Command(command), true);
                    } else {
                        return Ok(self.work(RelayInteractionEvent::Command(command)));
                    }
                }
                Selected::Command(None) => self.begin_drain(
                    DrainFinish::Stop(RelayInteractionStop::CommandsClosed),
                    true,
                ),
                Selected::Shutdown(changed) => {
                    if changed.is_err() || *self.shutdown_rx.borrow() {
                        self.begin_drain(DrainFinish::Stop(RelayInteractionStop::Shutdown), true);
                    }
                }
                Selected::ForceFlush(Ok(completion)) => {
                    self.begin_drain(DrainFinish::ForceFlush(completion), receive_input);
                }
                Selected::ForceFlush(Err(())) => self.begin_drain(
                    DrainFinish::Stop(RelayInteractionStop::ForceFlushClosed),
                    true,
                ),
                Selected::Wake => return Ok(self.work(RelayInteractionEvent::Wake)),
                Selected::CollectionDue => {}
                Selected::Input(Some((source, batch, work))) => {
                    if let Some((relay, batch)) = self.inputs.accept(source, batch)? {
                        return Ok(
                            self.work_with(RelayInteractionEvent::Batch { relay, batch }, work)
                        );
                    }
                }
                Selected::Input(None) => {
                    self.begin_drain(DrainFinish::Stop(RelayInteractionStop::InputsClosed), true)
                }
            }
        }
    }

    fn work(&self, event: RelayInteractionEvent<C>) -> RelayInteractionWork<C> {
        self.work_with(event, self.begin_work())
    }

    fn begin_work(&self) -> Option<NodeQuiesceWorkGuard> {
        self.quiesce_counters
            .as_ref()
            .map(|counters| NodeQuiesceWorkGuard::begin(counters.clone()))
    }

    fn work_with(
        &self,
        event: RelayInteractionEvent<C>,
        work: Option<NodeQuiesceWorkGuard>,
    ) -> RelayInteractionWork<C> {
        RelayInteractionWork { event, work }
    }

    fn ready_command(&mut self) -> ReadyCommand<C> {
        let Some(commands) = &mut self.commands else {
            return ReadyCommand::None;
        };
        match commands.try_recv() {
            Ok(command) => ReadyCommand::Command(command),
            Err(mpsc::error::TryRecvError::Empty) => ReadyCommand::None,
            Err(mpsc::error::TryRecvError::Disconnected) => ReadyCommand::Closed,
        }
    }

    fn force_flush_changed(
        &mut self,
    ) -> Result<Option<DomainForceFlushCompletion>, RelayInteractionError> {
        let Some(force_flush) = &mut self.force_flush else {
            return Ok(None);
        };
        match force_flush.pending_completion() {
            Ok(completion) => Ok(completion),
            Err(()) => {
                self.begin_drain(
                    DrainFinish::Stop(RelayInteractionStop::ForceFlushClosed),
                    true,
                );
                Ok(None)
            }
        }
    }

    fn begin_drain(&mut self, finish: DrainFinish<C>, drain_ready_inputs: bool) {
        if self.drain.is_none() {
            self.suppress_shutdown = !matches!(&finish, DrainFinish::ForceFlush(_));
            self.terminal_drain = match &finish {
                DrainFinish::ForceFlush(_) => false,
                DrainFinish::Command(command) => command.cancels_external_waits_while_draining(),
                DrainFinish::Stop(_) => true,
            };
            self.drain = Some(RelayInteractionDrain {
                remaining: if drain_ready_inputs {
                    self.inputs.pending_snapshot()
                } else {
                    vec![0; self.inputs.sources.len()]
                },
                finish,
            });
        }
    }

    async fn next_drain_work(&mut self) -> Result<RelayInteractionWork<C>, RelayInteractionError> {
        loop {
            tokio::task::consume_budget().await;
            let ready = {
                let drain = self.drain.as_mut().expect("drain state must exist");
                self.inputs.try_recv_snapshot(&mut drain.remaining)
            };
            let input = match ready {
                ReadyInput::Batch(source, batch, work) => Some((source, batch, work)),
                ReadyInput::Exhausted => None,
            };
            if let Some((source, batch, work)) = input {
                if let Some((relay, batch)) = self.inputs.accept(source, batch)? {
                    return Ok(self.work_with(RelayInteractionEvent::Batch { relay, batch }, work));
                }
                continue;
            }
            let work = self.begin_work();
            if let Some((relay, batch)) = self.inputs.take_any()? {
                return Ok(self.work_with(RelayInteractionEvent::Batch { relay, batch }, work));
            }
            drop(work);
            let drain = self.drain.take().expect("drain state must exist");
            let event = match drain.finish {
                DrainFinish::ForceFlush(completion) => {
                    RelayInteractionEvent::ForceFlush(completion)
                }
                DrainFinish::Command(command) => RelayInteractionEvent::Command(command),
                DrainFinish::Stop(reason) => RelayInteractionEvent::Stopped(reason),
            };
            return Ok(self.work(event));
        }
    }
}

enum Selected<C> {
    Command(Option<C>),
    Shutdown(Result<(), ()>),
    ForceFlush(Result<DomainForceFlushCompletion, ()>),
    Wake,
    CollectionDue,
    Input(Option<(usize, RelayRecordBatch, Option<NodeQuiesceWorkGuard>)>),
}

async fn recv_optional_command<C>(commands: &mut Option<mpsc::Receiver<C>>) -> Option<C> {
    match commands {
        Some(commands) => commands.recv().await,
        None => pending().await,
    }
}

async fn changed_optional_force_flush(
    participant: &mut Option<DomainForceFlushParticipant>,
) -> Result<DomainForceFlushCompletion, ()> {
    match participant {
        Some(participant) => participant.changed().await,
        None => pending().await,
    }
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::OnceLock};

    use nervix_models::{CreateSchema, Identifier, ParseAsType, Timestamp};

    use super::*;
    use crate::{
        runtime::{
            BranchKey, NodeQuiesceCounters, RelayBroadcast, RelayRecordBatch, RelayRuntimeFanIn,
            RuntimeInputCollectPolicy, force_flush::DomainForceFlush,
        },
        runtime_ack::{AckOutcome, AckSet},
        runtime_schema::{CompiledSchema, RuntimeValue, compile_schema, test_runtime_row},
    };

    fn schema() -> triomphe::Arc<CompiledSchema> {
        static SCHEMA: OnceLock<triomphe::Arc<CompiledSchema>> = OnceLock::new();
        SCHEMA
            .get_or_init(|| {
                triomphe::Arc::new(compile_schema(&CreateSchema {
                    name: Identifier::parse("relay_interaction_test").expect("valid schema"),
                    fields: vec![nervix_models::SchemaField {
                        name: Identifier::parse("value").expect("valid field"),
                        ty: ParseAsType::I64,
                        optional: false,
                        sensitive: false,
                    }],
                }))
            })
            .clone()
    }

    fn batch(value: i64) -> RelayRecordBatch {
        batch_with(value, None, AckSet::empty())
    }

    fn batch_with(value: i64, key: Option<BranchKey>, acks: AckSet) -> RelayRecordBatch {
        RelayRecordBatch::single(
            schema(),
            key,
            test_runtime_row([("value".to_string(), RuntimeValue::I64(value))])
                .with_ingested_at_watermarks(Timestamp::from_unix_nanos(value)),
            acks,
        )
        .expect("test batch must build")
    }

    fn alternate_batch(acks: AckSet) -> RelayRecordBatch {
        let alternate_schema = triomphe::Arc::new(compile_schema(&CreateSchema {
            name: Identifier::parse("relay_interaction_alternate").expect("valid alternate schema"),
            fields: vec![nervix_models::SchemaField {
                name: Identifier::parse("value").expect("valid field"),
                ty: ParseAsType::String,
                optional: false,
                sensitive: false,
            }],
        }));
        RelayRecordBatch::single(
            alternate_schema,
            None,
            test_runtime_row([("value".to_string(), RuntimeValue::String("two".to_string()))])
                .with_ingested_at_watermarks(Timestamp::from_unix_nanos(2)),
            acks,
        )
        .expect("alternate test batch must build")
    }

    fn branch(value: &str) -> Option<BranchKey> {
        Some(
            BranchKey::from_fields([(
                Identifier::parse("tenant").expect("valid branch field"),
                RuntimeValue::String(value.to_string()),
            )])
            .expect("branch key must build"),
        )
    }

    fn value(batch: &RelayRecordBatch) -> i64 {
        let record = batch.runtime_row(0).expect("one row");
        let Ok(Some(RuntimeValue::I64(value))) = record.value("value") else {
            panic!("test value must be I64")
        };
        value
    }

    fn source(
        name: &str,
        capacity: usize,
        policy: Option<RuntimeInputCollectPolicy>,
    ) -> (RelayInteractionInput, RelayBroadcast<RelayRecordBatch>) {
        let relay = Identifier::parse(name).expect("valid relay");
        let broadcast = RelayBroadcast::with_capacity(
            NonZeroUsize::new(capacity).expect("nonzero test capacity"),
        );
        let receiver = RelayRuntimeFanIn::new(broadcast.new_receiver());
        (
            RelayInteractionInput::new(relay, receiver, policy),
            broadcast,
        )
    }

    fn force_flush_participant(
        counters: Option<triomphe::Arc<NodeQuiesceCounters>>,
    ) -> (triomphe::Arc<DomainForceFlush>, DomainForceFlushParticipant) {
        let coordinator = DomainForceFlush::new();
        let participant = DomainForceFlush::subscribe(&coordinator, counters);
        (coordinator, participant)
    }

    fn complete_force_flush<C>(event: RelayInteractionEvent<C>) {
        let RelayInteractionEvent::ForceFlush(completion) = event else {
            panic!("expected force-flush event")
        };
        assert!(completion.complete());
    }

    async fn event<C: RelayInteractionCommand>(
        interaction: &mut RelayInteraction<C>,
        wake_at: Option<tokio::time::Instant>,
    ) -> RelayInteractionEvent<C> {
        let (event, _work) = interaction
            .next(wake_at)
            .await
            .expect("interaction must advance")
            .into_parts();
        event
    }

    #[tokio::test]
    async fn consumes_every_batch_that_is_already_ready() {
        let (input, broadcast) = source("events", 3, None);
        for value in 1..=3 {
            tokio::task::consume_budget().await;
            broadcast
                .broadcast(batch(value))
                .await
                .expect("batch must queue");
        }
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");

        for expected in 1..=3 {
            tokio::task::consume_budget().await;
            let RelayInteractionEvent::Batch { batch, .. } = event(&mut interaction, None).await
            else {
                panic!("ready batch {expected} was not consumed")
            };
            assert_eq!(value(&batch), expected);
        }
    }

    #[tokio::test]
    async fn multiple_sources_remain_independent_and_make_progress() {
        let (left, left_broadcast) = source("left", 2, None);
        let (right, right_broadcast) = source("right", 2, None);
        left_broadcast
            .broadcast(batch(1))
            .await
            .expect("left must queue");
        right_broadcast
            .broadcast(batch(2))
            .await
            .expect("right must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![left, right], shutdown_rx, None, None)
            .expect("interaction must build");

        let mut observed = Vec::new();
        for _ in 0..2 {
            tokio::task::consume_budget().await;
            let RelayInteractionEvent::Batch { relay, batch } = event(&mut interaction, None).await
            else {
                panic!("both sources must produce")
            };
            observed.push((relay.as_str().to_string(), value(&batch)));
        }
        observed.sort();
        assert_eq!(
            observed,
            [("left".to_string(), 1), ("right".to_string(), 2)]
        );
    }

    #[tokio::test]
    async fn wake_precedes_ready_input_and_the_next_call_consumes_it() {
        let (input, broadcast) = source("events", 2, None);
        broadcast
            .broadcast(batch(1))
            .await
            .expect("first batch must queue");
        broadcast
            .broadcast(batch(2))
            .await
            .expect("second batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");

        assert!(matches!(
            event(&mut interaction, Some(tokio::time::Instant::now())).await,
            RelayInteractionEvent::Wake
        ));
        for expected in [1, 2] {
            tokio::task::consume_budget().await;
            let RelayInteractionEvent::Batch { batch, .. } = event(&mut interaction, None).await
            else {
                panic!("ready batch {expected} must follow the serviced wake")
            };
            assert_eq!(value(&batch), expected);
        }
    }

    #[tokio::test]
    async fn round_robin_mux_services_eight_ready_sources() {
        let mut inputs = Vec::new();
        let mut broadcasts = Vec::new();
        for index in 0..8 {
            tokio::task::consume_budget().await;
            let (input, broadcast) = source(&format!("source_{index}"), 1, None);
            broadcast
                .broadcast(batch(index))
                .await
                .expect("source batch must queue");
            inputs.push(input);
            broadcasts.push(broadcast);
        }
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction =
            RelayInteraction::new(inputs, shutdown_rx, None, None).expect("interaction must build");

        let mut observed = Vec::new();
        for _ in 0..8 {
            tokio::task::consume_budget().await;
            let RelayInteractionEvent::Batch { relay, batch } = event(&mut interaction, None).await
            else {
                panic!("every ready source must make progress")
            };
            observed.push((relay.as_str().to_string(), value(&batch)));
        }
        assert_eq!(
            observed,
            (0..8)
                .map(|index| (format!("source_{index}"), index))
                .collect::<Vec<_>>()
        );
        drop(broadcasts);
    }

    #[tokio::test]
    async fn ready_hot_source_cannot_starve_another_source() {
        let (hot, hot_broadcast) = source("hot", 3, None);
        let (other, other_broadcast) = source("other", 1, None);
        for value in 1..=3 {
            tokio::task::consume_budget().await;
            hot_broadcast
                .broadcast(batch(value))
                .await
                .expect("hot-source batch must queue");
        }
        other_broadcast
            .broadcast(batch(10))
            .await
            .expect("other-source batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![hot, other], shutdown_rx, None, None)
            .expect("interaction must build");

        let mut observed = Vec::new();
        for _ in 0..4 {
            tokio::task::consume_budget().await;
            let RelayInteractionEvent::Batch { relay, batch } = event(&mut interaction, None).await
            else {
                panic!("all ready batches must make progress")
            };
            observed.push((relay.as_str().to_string(), value(&batch)));
        }
        assert_eq!(
            observed,
            [
                ("hot".to_string(), 1),
                ("other".to_string(), 10),
                ("hot".to_string(), 2),
                ("hot".to_string(), 3),
            ]
        );
    }

    #[tokio::test]
    async fn output_wake_wins_without_discarding_a_due_collection() {
        let policy = RuntimeInputCollectPolicy {
            interval: tokio::time::Duration::from_millis(1),
            max_batch_size: None,
        };
        let (input, broadcast) = source("events", 1, Some(policy));
        broadcast
            .broadcast(batch(1))
            .await
            .expect("batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");

        {
            let receive = interaction.next(None);
            tokio::pin!(receive);
            assert!(futures_util::poll!(&mut receive).is_pending());
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;

        assert!(matches!(
            event(&mut interaction, Some(tokio::time::Instant::now())).await,
            RelayInteractionEvent::Wake
        ));
        let RelayInteractionEvent::Batch { batch, .. } = event(&mut interaction, None).await else {
            panic!("due collection must remain available after the wake")
        };
        assert_eq!(value(&batch), 1);
    }

    #[tokio::test]
    async fn paused_input_preserves_ready_relay_backpressure_until_resumed() {
        let (input, sender) = source("orders", 2, None);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");
        sender
            .broadcast(batch(1))
            .await
            .expect("batch must become ready");

        let (wake, _work) = interaction
            .next_with_input(Some(Instant::now()), false)
            .await
            .expect("wake must remain observable while input is paused")
            .into_parts();
        assert!(matches!(wake, RelayInteractionEvent::Wake));
        assert_eq!(interaction.inputs.sources[0].receiver.pending_len(), 1);

        let RelayInteractionEvent::Batch { batch, .. } = event(&mut interaction, None).await else {
            panic!("resuming input must deliver the ready batch")
        };
        assert_eq!(value(&batch), 1);
    }

    #[tokio::test]
    async fn force_flush_respects_a_normal_input_pause() {
        let (input, sender) = source("orders", 2, None);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (coordinator, participant) = force_flush_participant(None);
        let mut interaction =
            RelayInteraction::new(vec![input], shutdown_rx, Some(participant), None)
                .expect("interaction must build");
        sender
            .broadcast(batch(1))
            .await
            .expect("batch must become ready");
        coordinator.request();

        let (work, _guard) = interaction
            .next_with_input(None, false)
            .await
            .expect("force flush must remain observable")
            .into_parts();
        complete_force_flush(work);
        assert_eq!(interaction.inputs.sources[0].receiver.pending_len(), 1);

        let RelayInteractionEvent::Batch { batch, .. } = event(&mut interaction, None).await else {
            panic!("resuming input must deliver the batch retained across force flush")
        };
        assert_eq!(value(&batch), 1);
    }

    #[tokio::test]
    async fn collection_deadline_releases_every_row_for_its_branch() {
        let policy = RuntimeInputCollectPolicy {
            interval: tokio::time::Duration::from_millis(1),
            max_batch_size: None,
        };
        let (input, broadcast) = source("events", 2, Some(policy));
        broadcast
            .broadcast(batch(1))
            .await
            .expect("first batch must queue");
        broadcast
            .broadcast(batch(2))
            .await
            .expect("second batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");

        let RelayInteractionEvent::Batch { batch, .. } = event(&mut interaction, None).await else {
            panic!("collection timer must release input")
        };
        assert_eq!(batch.message_count(), 2);
    }

    #[tokio::test]
    async fn collection_size_boundary_releases_without_waiting_for_timer() {
        let policy = RuntimeInputCollectPolicy {
            interval: tokio::time::Duration::from_secs(60),
            max_batch_size: Some(0),
        };
        let (input, broadcast) = source("events", 2, Some(policy));
        broadcast
            .broadcast(batch(1))
            .await
            .expect("first batch must queue");
        broadcast
            .broadcast(batch(2))
            .await
            .expect("second batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");

        for expected in [1, 2] {
            tokio::task::consume_budget().await;
            let RelayInteractionEvent::Batch { batch, .. } = event(&mut interaction, None).await
            else {
                panic!("size boundary must release batch {expected}")
            };
            assert_eq!(value(&batch), expected);
        }
    }

    #[tokio::test]
    async fn force_flush_latches_and_drains_every_source_before_firing() {
        let policy = RuntimeInputCollectPolicy {
            interval: tokio::time::Duration::from_secs(60),
            max_batch_size: None,
        };
        let (left, left_broadcast) = source("left", 2, Some(policy));
        let (right, right_broadcast) = source("right", 2, Some(policy));
        for value in [1, 2] {
            tokio::task::consume_budget().await;
            left_broadcast
                .broadcast(batch(value))
                .await
                .expect("left must queue");
        }
        right_broadcast
            .broadcast(batch(3))
            .await
            .expect("right must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (force_flush, force_participant) = force_flush_participant(None);
        let mut interaction = RelayInteraction::new(
            vec![left, right],
            shutdown_rx,
            Some(force_participant),
            None,
        )
        .expect("interaction must build");
        force_flush.request();

        let mut rows = Vec::new();
        loop {
            tokio::task::consume_budget().await;
            match event(&mut interaction, None).await {
                RelayInteractionEvent::Batch { batch, .. } => {
                    for row in 0..batch.message_count() as usize {
                        let record = batch.runtime_row(row).expect("row must exist");
                        let Ok(Some(RuntimeValue::I64(value))) = record.value("value") else {
                            panic!("row value must be I64")
                        };
                        rows.push(value);
                    }
                }
                RelayInteractionEvent::ForceFlush(completion) => {
                    assert!(completion.complete());
                    break;
                }
                other => panic!("unexpected event while force flushing: {other:?}"),
            }
        }
        rows.sort_unstable();
        assert_eq!(rows, [1, 2, 3]);
    }

    #[tokio::test]
    async fn force_flush_preserves_source_and_branch_collection_boundaries() {
        let policy = RuntimeInputCollectPolicy {
            interval: tokio::time::Duration::from_secs(60),
            max_batch_size: None,
        };
        let (input, broadcast) = source("events", 4, Some(policy));
        for (value, tenant) in [(1, "alpha"), (2, "beta"), (3, "alpha"), (4, "beta")] {
            tokio::task::consume_budget().await;
            broadcast
                .broadcast(batch_with(value, branch(tenant), AckSet::empty()))
                .await
                .expect("branched batch must queue");
        }
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (force_flush, force_participant) = force_flush_participant(None);
        let mut interaction =
            RelayInteraction::new(vec![input], shutdown_rx, Some(force_participant), None)
                .expect("interaction must build");
        force_flush.request();

        let mut groups = Vec::new();
        loop {
            tokio::task::consume_budget().await;
            match event(&mut interaction, None).await {
                RelayInteractionEvent::Batch { relay, batch } => {
                    let tenant = batch
                        .key
                        .as_ref()
                        .and_then(|key| key.field_value("tenant"))
                        .and_then(|value| {
                            if let RuntimeValue::String(value) = value {
                                Some(value.clone())
                            } else {
                                None
                            }
                        })
                        .expect("collected branch must be retained");
                    groups.push((relay.as_str().to_string(), tenant, batch.message_count()));
                }
                RelayInteractionEvent::ForceFlush(completion) => {
                    assert!(completion.complete());
                    break;
                }
                other => panic!("unexpected event while draining branches: {other:?}"),
            }
        }
        groups.sort();
        assert_eq!(
            groups,
            [
                ("events".to_string(), "alpha".to_string(), 2),
                ("events".to_string(), "beta".to_string(), 2),
            ]
        );
    }

    #[tokio::test]
    async fn force_flush_uses_a_finite_ready_snapshot() {
        let (input, broadcast) = source("events", 2, None);
        broadcast
            .broadcast(batch(1))
            .await
            .expect("pre-cut batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (force_flush, force_participant) = force_flush_participant(None);
        let mut interaction =
            RelayInteraction::new(vec![input], shutdown_rx, Some(force_participant), None)
                .expect("interaction must build");
        force_flush.request();

        let RelayInteractionEvent::Batch {
            batch: pre_cut_batch,
            ..
        } = event(&mut interaction, None).await
        else {
            panic!("pre-cut batch must drain")
        };
        assert_eq!(value(&pre_cut_batch), 1);
        broadcast
            .broadcast(batch(2))
            .await
            .expect("post-cut batch must queue");
        complete_force_flush(event(&mut interaction, None).await);
        let RelayInteractionEvent::Batch { batch, .. } = event(&mut interaction, None).await else {
            panic!("post-cut batch must remain for normal processing")
        };
        assert_eq!(value(&batch), 2);
    }

    #[tokio::test]
    async fn force_flush_drain_remains_interruptible_by_later_shutdown() {
        let (input, broadcast) = source("events", 1, None);
        broadcast
            .broadcast(batch(1))
            .await
            .expect("pre-cut batch must queue");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (force_flush, force_participant) = force_flush_participant(None);
        let mut interaction =
            RelayInteraction::new(vec![input], shutdown_rx, Some(force_participant), None)
                .expect("interaction must build");
        force_flush.request();

        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Batch { .. }
        ));
        assert!(interaction.is_draining());
        assert!(!interaction.is_terminal_drain());
        shutdown_tx.send(true).expect("shutdown must send");
        tokio::time::timeout(
            tokio::time::Duration::from_millis(100),
            interaction.shutdown_receiver().changed(),
        )
        .await
        .expect("shutdown must interrupt work inside a force-flush drain")
        .expect("shutdown sender must remain open");

        complete_force_flush(event(&mut interaction, None).await);
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Stopped(RelayInteractionStop::Shutdown)
        ));
    }

    #[tokio::test]
    async fn shutdown_drains_queued_and_collected_batches_before_stopping() {
        let policy = RuntimeInputCollectPolicy {
            interval: tokio::time::Duration::from_secs(60),
            max_batch_size: None,
        };
        let (input, broadcast) = source("events", 3, Some(policy));
        for value in 1..=3 {
            tokio::task::consume_budget().await;
            broadcast
                .broadcast(batch(value))
                .await
                .expect("batch must queue");
        }
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");
        shutdown_tx.send(true).expect("shutdown must send");

        let RelayInteractionEvent::Batch { batch, .. } = event(&mut interaction, None).await else {
            panic!("shutdown must drain accepted input")
        };
        assert_eq!(batch.message_count(), 3);
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Stopped(RelayInteractionStop::Shutdown)
        ));
    }

    #[tokio::test]
    async fn closed_shutdown_channel_drains_then_stops() {
        let (input, broadcast) = source("events", 1, None);
        broadcast
            .broadcast(batch(1))
            .await
            .expect("batch must queue");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        drop(shutdown_tx);
        let mut interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");

        let first = tokio::time::timeout(
            tokio::time::Duration::from_millis(100),
            interaction.next(None),
        )
        .await
        .expect("closed shutdown channel must not spin")
        .expect("queued batch must drain")
        .into_parts()
        .0;
        assert!(matches!(first, RelayInteractionEvent::Batch { .. }));
        assert!(
            !*interaction.shutdown_receiver().borrow(),
            "work accepted by the finite drain must not inherit cancellation"
        );
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Stopped(RelayInteractionStop::Shutdown)
        ));
        assert!(
            !*interaction.shutdown_receiver().borrow(),
            "the node's final flush is part of the graceful drain"
        );
    }

    #[tokio::test]
    async fn one_closed_source_does_not_stop_other_sources() {
        let (left, left_broadcast) = source("left", 1, None);
        let (right, right_broadcast) = source("right", 1, None);
        drop(left_broadcast);
        right_broadcast
            .broadcast(batch(9))
            .await
            .expect("open source must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![left, right], shutdown_rx, None, None)
            .expect("interaction must build");

        let RelayInteractionEvent::Batch { relay, batch } = event(&mut interaction, None).await
        else {
            panic!("open source must continue after its sibling closes")
        };
        assert_eq!(relay.as_str(), "right");
        assert_eq!(value(&batch), 9);
        drop(right_broadcast);
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Stopped(RelayInteractionStop::InputsClosed)
        ));
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestCommand {
        Inspect,
        Stop,
    }

    impl RelayInteractionCommand for TestCommand {
        fn drain_inputs_before_handling(&self) -> bool {
            matches!(self, Self::Stop)
        }

        fn cancels_external_waits_while_draining(&self) -> bool {
            matches!(self, Self::Stop)
        }
    }

    #[derive(Debug)]
    struct DefaultCommand;

    impl RelayInteractionCommand for DefaultCommand {}

    #[test]
    fn commands_are_immediate_by_default() {
        assert!(!DefaultCommand.drain_inputs_before_handling());
        assert!(!DefaultCommand.cancels_external_waits_while_draining());
    }

    #[test]
    fn finite_snapshot_handles_empty_and_closed_receivers() {
        let (input, broadcast) = source("events", 1, None);
        let mut inputs =
            RelayInteractionInputs::new(vec![input], None).expect("interaction inputs must build");

        let mut remaining = vec![1];
        assert!(matches!(
            inputs.try_recv_snapshot(&mut remaining),
            ReadyInput::Exhausted
        ));
        assert_eq!(remaining, vec![0]);
        assert!(!inputs.sources[0].closed);

        drop(broadcast);
        let mut remaining = vec![1];
        assert!(matches!(
            inputs.try_recv_snapshot(&mut remaining),
            ReadyInput::Exhausted
        ));
        assert_eq!(remaining, vec![0]);
        assert!(inputs.sources[0].closed);
    }

    #[tokio::test]
    async fn first_terminal_drain_reason_is_latched() {
        let (input, _broadcast) = source("events", 1, None);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");

        interaction.begin_drain(DrainFinish::Stop(RelayInteractionStop::Shutdown), true);
        interaction.begin_drain(DrainFinish::Stop(RelayInteractionStop::InputsClosed), true);

        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Stopped(RelayInteractionStop::Shutdown)
        ));
    }

    #[tokio::test]
    async fn graceful_command_drains_but_regular_command_is_immediate() {
        let (input, broadcast) = source("events", 1, None);
        broadcast
            .broadcast(batch(1))
            .await
            .expect("batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(2);
        let mut interaction =
            RelayInteraction::with_commands(vec![input], shutdown_rx, None, None, command_rx)
                .expect("interaction must build");
        command_tx
            .send(TestCommand::Inspect)
            .await
            .expect("inspect must send");

        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Command(TestCommand::Inspect)
        ));
        command_tx
            .send(TestCommand::Stop)
            .await
            .expect("stop must send");
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Batch { .. }
        ));
        assert!(interaction.is_draining());
        assert!(interaction.is_terminal_drain());
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Command(TestCommand::Stop)
        ));
        assert!(!interaction.is_draining());
        assert!(interaction.is_terminal_drain());
    }

    #[tokio::test]
    async fn command_arriving_while_waiting_uses_the_same_drain_contract() {
        let (input, broadcast) = source("events", 1, None);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(2);
        let mut interaction =
            RelayInteraction::with_commands(vec![input], shutdown_rx, None, None, command_rx)
                .expect("interaction must build");

        {
            let waiting = interaction.next(None);
            tokio::pin!(waiting);
            assert!(futures_util::poll!(&mut waiting).is_pending());
            command_tx
                .send(TestCommand::Inspect)
                .await
                .expect("inspect must send");
            let (event, _work) = waiting
                .await
                .expect("command must wake interaction")
                .into_parts();
            assert!(matches!(
                event,
                RelayInteractionEvent::Command(TestCommand::Inspect)
            ));
        }

        let (received, _work) = {
            let waiting = interaction.next(None);
            tokio::pin!(waiting);
            assert!(futures_util::poll!(&mut waiting).is_pending());
            broadcast
                .broadcast(batch(1))
                .await
                .expect("pre-command batch must queue");
            command_tx
                .send(TestCommand::Stop)
                .await
                .expect("stop must send");
            waiting
                .await
                .expect("stop must wake interaction")
                .into_parts()
        };
        assert!(matches!(received, RelayInteractionEvent::Batch { .. }));
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Command(TestCommand::Stop)
        ));
    }

    #[tokio::test]
    async fn shutdown_arriving_while_waiting_drains_ready_input() {
        let (input, broadcast) = source("events", 1, None);
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");
        assert!(!*interaction.shutdown_receiver().borrow());
        let (received, _work) = {
            let waiting = interaction.next(None);
            tokio::pin!(waiting);
            assert!(futures_util::poll!(&mut waiting).is_pending());
            broadcast
                .broadcast(batch(1))
                .await
                .expect("pre-shutdown batch must queue");
            shutdown_tx.send(true).expect("shutdown must send");
            waiting
                .await
                .expect("shutdown must wake interaction")
                .into_parts()
        };
        assert!(matches!(received, RelayInteractionEvent::Batch { .. }));
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Stopped(RelayInteractionStop::Shutdown)
        ));
    }

    #[tokio::test]
    async fn force_flush_arriving_while_waiting_drains_ready_input() {
        let (input, broadcast) = source("events", 1, None);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (force_flush, force_participant) = force_flush_participant(None);
        let mut interaction =
            RelayInteraction::new(vec![input], shutdown_rx, Some(force_participant), None)
                .expect("interaction must build");
        let (received, _work) = {
            let waiting = interaction.next(None);
            tokio::pin!(waiting);
            assert!(futures_util::poll!(&mut waiting).is_pending());
            broadcast
                .broadcast(batch(1))
                .await
                .expect("pre-flush batch must queue");
            force_flush.request();
            waiting
                .await
                .expect("force flush must wake interaction")
                .into_parts()
        };
        assert!(matches!(received, RelayInteractionEvent::Batch { .. }));
        complete_force_flush(event(&mut interaction, None).await);
    }

    #[tokio::test]
    async fn future_wake_deadline_interrupts_an_idle_input_wait() {
        let (input, _broadcast) = source("events", 1, None);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");

        assert!(matches!(
            event(
                &mut interaction,
                Some(tokio::time::Instant::now() + tokio::time::Duration::from_millis(1)),
            )
            .await,
            RelayInteractionEvent::Wake
        ));
    }

    #[tokio::test]
    async fn closed_command_channel_drains_then_stops() {
        let (input, broadcast) = source("events", 1, None);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (command_tx, command_rx) = tokio::sync::mpsc::channel::<TestCommand>(1);
        let mut interaction =
            RelayInteraction::with_commands(vec![input], shutdown_rx, None, None, command_rx)
                .expect("interaction must build");

        let (received, _work) = {
            let waiting = interaction.next(None);
            tokio::pin!(waiting);
            assert!(futures_util::poll!(&mut waiting).is_pending());
            broadcast
                .broadcast(batch(1))
                .await
                .expect("batch must queue");
            drop(command_tx);
            waiting
                .await
                .expect("command closure must wake interaction")
                .into_parts()
        };
        assert!(matches!(received, RelayInteractionEvent::Batch { .. }));
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Stopped(RelayInteractionStop::CommandsClosed)
        ));
    }

    #[tokio::test]
    async fn command_channel_closed_before_poll_stops_after_the_ready_cut() {
        let (input, broadcast) = source("events", 1, None);
        broadcast
            .broadcast(batch(1))
            .await
            .expect("batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (command_tx, command_rx) = tokio::sync::mpsc::channel::<TestCommand>(1);
        drop(command_tx);
        let mut interaction =
            RelayInteraction::with_commands(vec![input], shutdown_rx, None, None, command_rx)
                .expect("interaction must build");

        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Batch { .. }
        ));
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Stopped(RelayInteractionStop::CommandsClosed)
        ));
    }

    #[tokio::test]
    async fn closed_force_flush_channel_stops_after_draining() {
        let (input, broadcast) = source("events", 1, None);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (force_flush, force_participant) = force_flush_participant(None);
        let mut interaction =
            RelayInteraction::new(vec![input], shutdown_rx, Some(force_participant), None)
                .expect("interaction must build");

        let (received, _work) = {
            let waiting = interaction.next(None);
            tokio::pin!(waiting);
            assert!(futures_util::poll!(&mut waiting).is_pending());
            broadcast
                .broadcast(batch(1))
                .await
                .expect("batch must queue");
            force_flush.close();
            waiting
                .await
                .expect("force channel closure must wake interaction")
                .into_parts()
        };
        assert!(matches!(received, RelayInteractionEvent::Batch { .. }));
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Stopped(RelayInteractionStop::ForceFlushClosed)
        ));
    }

    #[tokio::test]
    async fn force_flush_closed_before_poll_stops_after_the_ready_cut() {
        let (input, broadcast) = source("events", 1, None);
        broadcast
            .broadcast(batch(1))
            .await
            .expect("batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (force_flush, force_participant) = force_flush_participant(None);
        force_flush.close();
        let mut interaction =
            RelayInteraction::new(vec![input], shutdown_rx, Some(force_participant), None)
                .expect("interaction must build");

        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Batch { .. }
        ));
        assert!(matches!(
            event(&mut interaction, None).await,
            RelayInteractionEvent::Stopped(RelayInteractionStop::ForceFlushClosed)
        ));
    }

    #[tokio::test]
    async fn quiesce_counts_collected_and_in_flight_work() {
        let policy = RuntimeInputCollectPolicy {
            interval: tokio::time::Duration::from_secs(60),
            max_batch_size: None,
        };
        let (input, broadcast) = source("events", 1, Some(policy));
        broadcast
            .broadcast(batch(1))
            .await
            .expect("batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let counters = triomphe::Arc::new(NodeQuiesceCounters::default());
        let (force_flush, force_participant) = force_flush_participant(Some(counters.clone()));
        let mut interaction = RelayInteraction::new(
            vec![input],
            shutdown_rx,
            Some(force_participant),
            Some(counters.clone()),
        )
        .expect("interaction must build");

        {
            let pending = interaction.next(None);
            tokio::pin!(pending);
            assert!(futures_util::poll!(&mut pending).is_pending());
        }
        assert_eq!(counters.outstanding_work(), 1);
        force_flush.request();
        let work = interaction.next(None).await.expect("batch must release");
        assert_eq!(counters.outstanding_work(), 2);
        drop(work);
        assert_eq!(counters.outstanding_work(), 1);
        complete_force_flush(event(&mut interaction, None).await);
        assert_eq!(counters.outstanding_work(), 0);
    }

    #[tokio::test]
    async fn ready_dequeue_owns_work_before_receiver_becomes_empty() {
        let (input, broadcast) = source("events", 1, None);
        broadcast
            .broadcast(batch(1))
            .await
            .expect("batch must queue");
        let counters = triomphe::Arc::new(NodeQuiesceCounters::default());
        let mut inputs = RelayInteractionInputs::new(vec![input], Some(counters.clone()))
            .expect("inputs must build");

        let (_, _, work) = std::future::poll_fn(|cx| inputs.poll_recv(cx))
            .await
            .expect("ready input must dequeue");
        assert_eq!(inputs.pending_snapshot(), [0]);
        assert_eq!(counters.outstanding_work(), 1);
        drop(work);
        assert_eq!(counters.outstanding_work(), 0);
    }

    #[tokio::test]
    async fn dequeue_to_collection_has_overlapping_quiesce_accounting() {
        let policy = RuntimeInputCollectPolicy {
            interval: tokio::time::Duration::from_secs(60),
            max_batch_size: None,
        };
        let (input, broadcast) = source("events", 1, Some(policy));
        broadcast
            .broadcast(batch(1))
            .await
            .expect("batch must queue");
        let counters = triomphe::Arc::new(NodeQuiesceCounters::default());
        let mut inputs = RelayInteractionInputs::new(vec![input], Some(counters.clone()))
            .expect("inputs must build");

        let (source, batch, work) = std::future::poll_fn(|cx| inputs.poll_recv(cx))
            .await
            .expect("ready input must dequeue");
        assert!(
            inputs
                .accept(source, batch)
                .expect("collection must accept batch")
                .is_none()
        );
        assert_eq!(counters.outstanding_work(), 2);
        drop(work);
        assert_eq!(counters.outstanding_work(), 1);
    }

    #[tokio::test]
    async fn dropping_interaction_releases_collected_quiesce_work() {
        let policy = RuntimeInputCollectPolicy {
            interval: tokio::time::Duration::from_secs(60),
            max_batch_size: None,
        };
        let (input, broadcast) = source("events", 1, Some(policy));
        let (acks, completion) = AckSet::root();
        broadcast
            .broadcast(batch_with(1, None, acks))
            .await
            .expect("batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let counters = triomphe::Arc::new(NodeQuiesceCounters::default());
        let mut interaction =
            RelayInteraction::new(vec![input], shutdown_rx, None, Some(counters.clone()))
                .expect("interaction must build");
        {
            let pending = interaction.next(None);
            tokio::pin!(pending);
            assert!(futures_util::poll!(&mut pending).is_pending());
        }
        assert_eq!(counters.outstanding_work(), 1);
        drop(interaction);
        assert_eq!(counters.outstanding_work(), 0);
        assert_eq!(
            completion.wait().await,
            AckOutcome::NoAck("relay interaction dropped collected input".to_string())
        );
    }

    #[tokio::test]
    async fn dropping_interaction_terminally_resolves_queued_input() {
        let (input, broadcast) = source("events", 1, None);
        let (acks, completion) = AckSet::root();
        broadcast
            .broadcast(batch_with(1, None, acks))
            .await
            .expect("batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let interaction = RelayInteraction::new(vec![input], shutdown_rx, None, None)
            .expect("interaction must build");

        drop(interaction);

        assert_eq!(
            completion.wait().await,
            AckOutcome::NoAck("relay interaction dropped queued input".to_string())
        );
    }

    #[tokio::test]
    async fn concatenated_batches_preserve_and_complete_every_ack_root() {
        let policy = RuntimeInputCollectPolicy {
            interval: tokio::time::Duration::from_secs(60),
            max_batch_size: None,
        };
        let (input, broadcast) = source("events", 2, Some(policy));
        let (first_acks, first_completion) = AckSet::root();
        let (second_acks, second_completion) = AckSet::root();
        broadcast
            .broadcast(batch_with(1, None, first_acks))
            .await
            .expect("first batch must queue");
        broadcast
            .broadcast(batch_with(2, None, second_acks))
            .await
            .expect("second batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (force_flush, force_participant) = force_flush_participant(None);
        let mut interaction =
            RelayInteraction::new(vec![input], shutdown_rx, Some(force_participant), None)
                .expect("interaction must build");
        force_flush.request();

        let RelayInteractionEvent::Batch { batch, .. } = event(&mut interaction, None).await else {
            panic!("collected batch must be released")
        };
        assert_eq!(batch.message_count(), 2);
        batch.ack_success();
        assert_eq!(first_completion.wait().await, AckOutcome::Ack);
        assert_eq!(second_completion.wait().await, AckOutcome::Ack);
    }

    #[tokio::test]
    async fn concatenation_failure_preserves_every_ack_root_for_error_handling() {
        let policy = RuntimeInputCollectPolicy {
            interval: tokio::time::Duration::from_secs(60),
            max_batch_size: None,
        };
        let (input, broadcast) = source("events", 2, Some(policy));
        let (first_acks, first_completion) = AckSet::root();
        let (second_acks, second_completion) = AckSet::root();
        broadcast
            .broadcast(batch_with(1, None, first_acks))
            .await
            .expect("first batch must queue");
        let alternate = alternate_batch(second_acks);
        broadcast
            .broadcast(alternate)
            .await
            .expect("second batch must queue");
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (force_flush, force_participant) = force_flush_participant(None);
        let mut interaction =
            RelayInteraction::new(vec![input], shutdown_rx, Some(force_participant), None)
                .expect("interaction must build");
        force_flush.request();

        let error = match interaction.next(None).await {
            Ok(_) => panic!("incompatible collected schemas must fail concatenation"),
            Err(error) => error,
        };
        assert!(matches!(error, RelayInteractionError::Concatenate { .. }));
        let reason = error.to_string();
        error
            .acks()
            .expect("concatenation error must retain ACK ownership")
            .no_ack(reason.clone());
        assert_eq!(
            first_completion.wait().await,
            AckOutcome::NoAck(reason.clone())
        );
        assert_eq!(second_completion.wait().await, AckOutcome::NoAck(reason));
    }

    #[tokio::test]
    async fn size_triggered_concatenation_failure_preserves_ack_roots() {
        let (first_acks, first_completion) = AckSet::root();
        let (second_acks, second_completion) = AckSet::root();
        let first = batch_with(1, None, first_acks);
        let max_batch_size = first.estimated_bytes().saturating_add(1);
        let mut collection = RelayInputCollection::new(
            Some(RuntimeInputCollectPolicy {
                interval: tokio::time::Duration::from_secs(60),
                max_batch_size: Some(max_batch_size),
            }),
            None,
        );
        assert!(
            collection
                .push(first)
                .expect("first schema must collect")
                .is_none()
        );

        let error = match collection.push(alternate_batch(second_acks)) {
            Ok(_) => panic!("size-triggered incompatible schemas must fail concatenation"),
            Err(error) => error,
        };
        error.acks.no_ack(error.reason.clone());
        assert_eq!(
            first_completion.wait().await,
            AckOutcome::NoAck(error.reason.clone())
        );
        assert_eq!(
            second_completion.wait().await,
            AckOutcome::NoAck(error.reason)
        );
    }

    #[test]
    fn rejects_empty_and_duplicate_input_sets() {
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let no_inputs = match RelayInteraction::new(Vec::new(), shutdown_rx, None, None) {
            Ok(_) => panic!("empty interaction must fail"),
            Err(error) => error,
        };
        assert!(matches!(&no_inputs, RelayInteractionError::NoInputs));
        assert!(no_inputs.acks().is_none());

        let (first, _first_broadcast) = source("same", 1, None);
        let (second, _second_broadcast) = source("same", 1, None);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        assert!(matches!(
            RelayInteraction::new(vec![first, second], shutdown_rx, None, None),
            Err(RelayInteractionError::DuplicateInput { .. })
        ));
    }
}
