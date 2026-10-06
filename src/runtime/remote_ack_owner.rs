//! Delivery-scoped owners and bounded numeric routing for volatile remote correlations.
//!
//! Layer: data plane.
//! - **Owns.** Correlation capacity, exact generation identity, ACK progress and retirement.
//! - **Depends on.** The ACK tree, execution admission and execution-sensitive primitives.
//! - **Must not know.** Graph Models, placement, connectors or persistent storage.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        key = "one delivery slot and its exact registration generation",
        bound = "fixed delivery and admission positions; row state is charged to the relay \
                 budget; no guard crosses await",
        reason = "each delivery owns its acknowledgement rows independently of other deliveries"
    )
)]

use error_stack::ResultExt as _;
use nervix_execution::{MemoryClass, Reservation};
use nervix_primitives::{
    collections::{ConcurrentQueue, PushError},
    sync::blocking::Mutex,
};

use super::{
    remote_dispatch::{
        REMOTE_ACK_SILENCE_SWEEP_INTERVAL, REMOTE_ACK_SILENCE_TIMEOUT, REMOTE_ACK_SILENT_SWEEPS,
        REMOTE_RELAY_TOTAL_TIMEOUT, RelayAdmissionUpdate, RemoteDispatchError,
    },
    *,
};

const REMOTE_DELIVERY_CAPACITY: usize = 8192;
// A row's metadata alone exceeds two bytes, so the existing 32-MiB relay wire budget is
// stricter than this address space. Row positions never overlap delivery generations.
const ROW_POSITIONS: u64 = 1 << 24;
const ADMISSION_SWEEPS: u64 =
    REMOTE_RELAY_TOTAL_TIMEOUT.as_secs() / REMOTE_ACK_SILENCE_SWEEP_INTERVAL.as_secs();

/// The node supplies immutable routing positions. A position owns one delivery, with independent
/// record outcomes. Admission has its own free queue, so full record capacity cannot prevent a
/// registered delivery from requesting admission. A returned position retains its generation.
pub(super) struct RemoteDispatchRegistry {
    slots: Box<[Mutex<CorrelationSlot>]>,
    deliveries: ConcurrentQueue<usize>,
    admissions: ConcurrentQueue<usize>,
    delivery_capacity: usize,
    executor: Executor,
}

enum CorrelationSlot {
    Open {
        generation: u64,
        correlation: Option<RemoteCorrelation>,
    },
    Closed,
}

struct RemoteCorrelation {
    route: u64,
    state: RemoteCorrelationState,
}

enum RemoteCorrelationState {
    Records(DeliveryAcks),
    Admission(watch::Sender<RelayAdmissionUpdate>),
}

struct DeliveryAcks {
    rows: Vec<Option<PendingRemoteAck>>,
    remaining: usize,
    _memory: Reservation,
}

enum RetiredCorrelation {
    Record(PendingRemoteAck),
    Admission(watch::Sender<RelayAdmissionUpdate>),
}

struct PendingRemoteAck {
    receiver: ClusterNodeName,
    acks: AckSet,
    phase: RemoteAckPhase,
    required_wait: Option<AckParkGuard>,
    progress_sequence: Option<u64>,
}

enum RemoteAckPhase {
    AwaitingAdmission { sweeps: u64 },
    Admitted { silent_sweeps: u64 },
}

impl PendingRemoteAck {
    fn new(receiver: ClusterNodeName, acks: AckSet) -> Self {
        Self {
            receiver,
            acks,
            phase: RemoteAckPhase::AwaitingAdmission { sweeps: 0 },
            required_wait: None,
            progress_sequence: None,
        }
    }

    fn report(&mut self) {
        if let RemoteAckPhase::Admitted { silent_sweeps } = &mut self.phase {
            *silent_sweeps = 0;
        }
        self.acks.ack_alive();
    }

    fn progress(&mut self, sequence: u64, parked: bool) {
        if self.progress_sequence.is_some_and(|seen| sequence <= seen) {
            return;
        }
        self.progress_sequence = Some(sequence);
        if parked {
            if self.required_wait.is_none() {
                self.required_wait = Some(AckParkGuard::new([&self.acks]));
            }
        } else {
            self.required_wait = None;
        }
    }

    fn swept(&mut self) -> bool {
        match &mut self.phase {
            RemoteAckPhase::AwaitingAdmission { sweeps } => {
                *sweeps = sweeps
                    .checked_add(1)
                    .assured("admission sweeps end at the finite admission bound");
                *sweeps > ADMISSION_SWEEPS
            }
            RemoteAckPhase::Admitted { silent_sweeps } => {
                *silent_sweeps = silent_sweeps
                    .checked_add(1)
                    .assured("silence sweeps end at the finite silence bound");
                *silent_sweeps > REMOTE_ACK_SILENT_SWEEPS
            }
        }
    }

    fn expiration_reason(&self) -> String {
        match self.phase {
            RemoteAckPhase::AwaitingAdmission { .. } => format!(
                "node '{}' did not admit the forwarded record within {}",
                self.receiver,
                humantime::format_duration(REMOTE_RELAY_TOTAL_TIMEOUT)
            ),
            RemoteAckPhase::Admitted { .. } => format!(
                "node '{}' reported nothing about the forwarded record for {}",
                self.receiver,
                humantime::format_duration(REMOTE_ACK_SILENCE_TIMEOUT)
            ),
        }
    }
}

impl RemoteDispatchRegistry {
    pub(super) fn new(executor: Executor) -> Self {
        Self::build(executor, REMOTE_DELIVERY_CAPACITY)
    }

    #[cfg(test)]
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self::build(Executor::default(), capacity)
    }

    fn build(executor: Executor, capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "a correlation owner has at least one routing position"
        );
        let deliveries = ConcurrentQueue::bounded(capacity);
        let admissions = ConcurrentQueue::bounded(capacity);
        let total = capacity
            .checked_mul(2)
            .assured("the configured routing capacity fits in usize");
        let mut slots = Vec::with_capacity(total);
        for position in 0..total {
            slots.push(Mutex::new(CorrelationSlot::Open {
                generation: 0,
                correlation: None,
            }));
            let free = if position < capacity {
                &deliveries
            } else {
                &admissions
            };
            free.push(position)
                .assured("each empty position enters its free queue once");
        }
        Self {
            slots: slots.into_boxed_slice(),
            deliveries,
            admissions,
            delivery_capacity: capacity,
            executor,
        }
    }

    fn register(
        &self,
        state: RemoteCorrelationState,
    ) -> error_stack::Result<u64, RemoteDispatchError> {
        let free = match &state {
            RemoteCorrelationState::Records(_) => &self.deliveries,
            RemoteCorrelationState::Admission(_) => &self.admissions,
        };
        let position = free.pop().map_err(|_| {
            Report::new(RemoteDispatchError::CorrelationCapacity {
                capacity: self.delivery_capacity,
            })
        })?;
        let mut slot = self.slots[position].lock();
        let CorrelationSlot::Open {
            generation,
            correlation,
        } = &mut *slot
        else {
            return Err(Report::new(RemoteDispatchError::CorrelationCapacity {
                capacity: self.delivery_capacity,
            }));
        };
        assert!(
            correlation.is_none(),
            "only an empty position may enter the free queue"
        );
        let capacity = u64::try_from(self.slots.len()).assured("the routing capacity fits in u64");
        let position_number = u64::try_from(position).assured("a routing position fits in u64");
        let route = match generation.checked_add(1) {
            Some(next) => match next.checked_mul(capacity) {
                Some(base) => base.checked_add(position_number),
                None => None,
            },
            None => None,
        };
        let Some(route) = route.filter(|route| *route <= u64::MAX / ROW_POSITIONS) else {
            // Exhaustion irreversibly seals this position; recycling would reuse an identity.
            *slot = CorrelationSlot::Closed;
            return Err(Report::new(
                RemoteDispatchError::CorrelationIdentityExhausted,
            ));
        };
        *generation = generation
            .checked_add(1)
            .verified("the route computation checked its next generation");
        *correlation = Some(RemoteCorrelation { route, state });
        Ok(route * ROW_POSITIONS)
    }

    #[cfg(test)]
    pub(super) fn register_ack(
        &self,
        receiver: ClusterNodeName,
        acks: AckSet,
    ) -> error_stack::Result<u64, RemoteDispatchError> {
        let registrations = self.register_acks(receiver, vec![acks])?;
        Ok(registrations
            .into_iter()
            .next()
            .flatten()
            .assured("the fixture registers a nonempty record acknowledgement"))
    }

    pub(super) fn register_acks(
        &self,
        receiver: ClusterNodeName,
        acks: Vec<AckSet>,
    ) -> error_stack::Result<Vec<Option<u64>>, RemoteDispatchError> {
        let row_count = u64::try_from(acks.len()).assured("a row allocation length fits in u64");
        if row_count > ROW_POSITIONS {
            return Err(Report::new(
                RemoteDispatchError::CorrelationIdentityExhausted,
            ));
        }
        let remaining = acks.iter().filter(|acks| !acks.is_empty()).count();
        if remaining == 0 {
            return Ok(vec![None; acks.len()]);
        }
        let bytes_per_row = std::mem::size_of::<Option<PendingRemoteAck>>()
            .checked_add(receiver.as_str().len())
            .assured("a node name and a record owner fit in usize");
        let bytes = acks
            .len()
            .checked_mul(bytes_per_row)
            .assured("a valid relay frame's record owners fit in usize");
        let memory = self
            .executor
            .try_reserve(
                MemoryClass::Relay,
                u64::try_from(bytes).assured("a record owner allocation fits in u64"),
            )
            .change_context(RemoteDispatchError::CorrelationMemory)?;
        let mut rows = Vec::with_capacity(acks.len());
        let mut registrations = Vec::with_capacity(acks.len());
        for (row, acks) in acks.into_iter().enumerate() {
            if acks.is_empty() {
                registrations.push(None);
                rows.push(None);
            } else {
                // This is the row's relative position until registration supplies its generation.
                registrations.push(Some(u64::try_from(row).assured("a wire row fits in u64")));
                rows.push(Some(PendingRemoteAck::new(receiver.clone(), acks)));
            }
        }
        let base = self.register(RemoteCorrelationState::Records(DeliveryAcks {
            rows,
            remaining,
            _memory: memory,
        }))?;
        for id in registrations.iter_mut().flatten() {
            *id = base
                .checked_add(*id)
                .assured("the route reserves every row position without overflow");
        }
        Ok(registrations)
    }

    pub(super) fn register_admission(
        &self,
    ) -> error_stack::Result<(u64, watch::Receiver<RelayAdmissionUpdate>), RemoteDispatchError>
    {
        let (sender, receiver) = watch::channel(RelayAdmissionUpdate::Pending);
        let id = self.register(RemoteCorrelationState::Admission(sender))?;
        Ok((id, receiver))
    }

    fn position(&self, id: u64) -> usize {
        let capacity = u64::try_from(self.slots.len()).assured("the routing capacity fits in u64");
        usize::try_from((id / ROW_POSITIONS) % capacity)
            .assured("a modulo-capacity position fits in usize")
    }

    fn recycle(&self, position: usize) {
        let free = if position < self.delivery_capacity {
            &self.deliveries
        } else {
            &self.admissions
        };
        match free.push(position) {
            Ok(()) => {}
            // Shutdown closes both queues before ending their retained positions.
            Err(PushError::Closed(_)) => {}
            Err(PushError::Full(_)) => {
                panic!("each delivery returns its sole routing position exactly once")
            }
        }
    }

    fn record_mut(correlation: &mut RemoteCorrelation, id: u64) -> Option<&mut PendingRemoteAck> {
        if correlation.route != id / ROW_POSITIONS {
            return None;
        }
        let RemoteCorrelationState::Records(records) = &mut correlation.state else {
            return None;
        };
        let row = usize::try_from(id % ROW_POSITIONS).assured("a wire row fits in usize");
        records.rows.get_mut(row)?.as_mut()
    }

    pub(super) fn admit_ack(&self, id: u64) {
        let mut slot = self.slots[self.position(id)].lock();
        if let CorrelationSlot::Open {
            correlation: Some(correlation),
            ..
        } = &mut *slot
            && let Some(pending) = Self::record_mut(correlation, id)
            && matches!(pending.phase, RemoteAckPhase::AwaitingAdmission { .. })
        {
            pending.phase = RemoteAckPhase::Admitted { silent_sweeps: 0 };
        }
    }

    pub(super) fn admit_acks(&self, registrations: &[Option<RemoteAckRegistration>]) {
        for registration in registrations.iter().flatten() {
            self.admit_ack(registration.ack_id);
        }
    }

    pub(super) fn report_ack(&self, id: u64) -> bool {
        self.apply_progress(id, None)
    }
    pub(super) fn progress_ack(&self, id: u64, sequence: u64, parked: bool) -> bool {
        self.apply_progress(id, Some((sequence, parked)))
    }

    fn apply_progress(&self, id: u64, progress: Option<(u64, bool)>) -> bool {
        let position = self.position(id);
        let mut slot = self.slots[position].lock();
        let CorrelationSlot::Open {
            correlation: entry, ..
        } = &mut *slot
        else {
            return false;
        };
        let Some(correlation) = entry.as_mut() else {
            return false;
        };
        if correlation.route != id / ROW_POSITIONS {
            return false;
        }
        match &mut correlation.state {
            RemoteCorrelationState::Records(_) => {
                let Some(pending) = Self::record_mut(correlation, id) else {
                    return false;
                };
                if let Some((sequence, parked)) = progress {
                    pending.progress(sequence, parked);
                }
                pending.report();
            }
            RemoteCorrelationState::Admission(admission) => {
                if !id.is_multiple_of(ROW_POSITIONS) {
                    return false;
                }
                if admission.is_closed() {
                    let pending = entry.take();
                    drop(slot);
                    self.recycle(position);
                    drop(pending);
                    return true;
                }
                admission.send_replace(RelayAdmissionUpdate::Alive);
            }
        }
        true
    }

    fn take(&self, id: u64) -> Option<RetiredCorrelation> {
        let position = self.position(id);
        let mut slot = self.slots[position].lock();
        let CorrelationSlot::Open {
            correlation: entry, ..
        } = &mut *slot
        else {
            return None;
        };
        let correlation = entry.as_mut()?;
        if correlation.route != id / ROW_POSITIONS {
            return None;
        }
        match &mut correlation.state {
            RemoteCorrelationState::Records(records) => {
                let row = usize::try_from(id % ROW_POSITIONS).assured("a wire row fits in usize");
                let pending = records.rows.get_mut(row)?.take()?;
                records.remaining = records
                    .remaining
                    .checked_sub(1)
                    .assured("one live record is retired exactly once");
                if records.remaining == 0 {
                    let ended = entry.take();
                    drop(slot);
                    self.recycle(position);
                    drop(ended);
                }
                Some(RetiredCorrelation::Record(pending))
            }
            RemoteCorrelationState::Admission(_) => {
                if !id.is_multiple_of(ROW_POSITIONS) {
                    return None;
                }
                let ended = entry
                    .take()
                    .verified("the exact admission was found under its owner guard");
                drop(slot);
                self.recycle(position);
                let RemoteCorrelationState::Admission(admission) = ended.state else {
                    unreachable!("the admission kind was checked under the owner guard");
                };
                Some(RetiredCorrelation::Admission(admission))
            }
        }
    }

    pub(super) fn resolve_ack(&self, id: u64, outcome: AckOutcome) -> bool {
        let Some(pending) = self.take(id) else {
            return false;
        };
        match pending {
            RetiredCorrelation::Record(pending) => match outcome {
                AckOutcome::Ack => pending.acks.ack_success(),
                AckOutcome::NoAck(error) => pending.acks.no_ack(error),
            },
            RetiredCorrelation::Admission(admission) => {
                admission.send_replace(match outcome {
                    AckOutcome::Ack => RelayAdmissionUpdate::Admitted,
                    AckOutcome::NoAck(error) => RelayAdmissionUpdate::Rejected(error),
                });
            }
        }
        true
    }

    pub(super) fn clear_ack(&self, id: u64) {
        drop(self.take(id));
    }

    #[cfg(test)]
    pub(super) fn holds_ack(&self, id: u64) -> bool {
        let mut slot = self.slots[self.position(id)].lock();
        let CorrelationSlot::Open {
            correlation: Some(correlation),
            ..
        } = &mut *slot
        else {
            return false;
        };
        match correlation.state {
            RemoteCorrelationState::Records(_) => Self::record_mut(correlation, id).is_some(),
            RemoteCorrelationState::Admission(_) => {
                correlation.route == id / ROW_POSITIONS && id.is_multiple_of(ROW_POSITIONS)
            }
        }
    }

    pub(super) fn fail_silent_acks(&self) -> BTreeMap<ClusterNodeName, usize> {
        let mut failed = BTreeMap::new();
        for (position, slot) in self.slots.iter().enumerate() {
            let mut slot = slot.lock();
            let CorrelationSlot::Open {
                correlation: entry, ..
            } = &mut *slot
            else {
                continue;
            };
            let Some(correlation) = entry.as_mut() else {
                continue;
            };
            let mut expired = Vec::new();
            let reclaim = match &mut correlation.state {
                RemoteCorrelationState::Records(records) => {
                    for row in &mut records.rows {
                        if let Some(pending) = row
                            && pending.swept()
                        {
                            expired.push(
                                row.take()
                                    .verified("the swept row was present under the owner guard"),
                            );
                            records.remaining = records
                                .remaining
                                .checked_sub(1)
                                .assured("one live record expires exactly once");
                        }
                    }
                    records.remaining == 0
                }
                RemoteCorrelationState::Admission(admission) => admission.is_closed(),
            };
            let ended = if reclaim { entry.take() } else { None };
            drop(slot);
            if reclaim {
                self.recycle(position);
            }
            drop(ended);
            for pending in expired {
                pending.acks.no_ack(pending.expiration_reason());
                let count = failed.entry(pending.receiver).or_insert(0_usize);
                *count = count
                    .checked_add(1)
                    .assured("the charged record count bounds one sweep's outcomes");
            }
        }
        failed
    }

    pub(super) async fn sweep_silent_acks(&self) {
        let mut sweeps = nervix_primitives::time::interval(REMOTE_ACK_SILENCE_SWEEP_INTERVAL);
        sweeps.set_missed_tick_behavior(nervix_primitives::time::MissedTickBehavior::Skip);
        loop {
            nervix_primitives::task::consume_budget().await;
            sweeps.tick().await;
            for (receiver, acknowledgements) in self.fail_silent_acks() {
                warn!(target_node = %receiver, acknowledgements, "remote record acknowledgement owner expired");
            }
        }
    }

    pub(super) fn shutdown(&self) {
        self.deliveries.close();
        self.admissions.close();
        for slot in &self.slots {
            let state = std::mem::replace(&mut *slot.lock(), CorrelationSlot::Closed);
            if let CorrelationSlot::Open {
                correlation: Some(correlation),
                ..
            } = state
            {
                match correlation.state {
                    RemoteCorrelationState::Records(records) => {
                        for pending in records.rows.into_iter().flatten() {
                            pending.acks.no_ack("remote correlation owner ended");
                        }
                    }
                    RemoteCorrelationState::Admission(admission) => {
                        admission.send_replace(RelayAdmissionUpdate::Rejected(
                            "remote correlation owner ended".to_string(),
                        ));
                    }
                }
            }
        }
    }
}

impl Drop for RemoteDispatchRegistry {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(all(test, not(any(feature = "loom", feature = "shuttle"))))]
#[path = "remote_ack_owner_tests.rs"]
mod tests;
