//! Delivery-scoped owners and bounded numeric routing for volatile remote correlations.
//!
//! Layer: data plane.
//! - **Owns.** Correlation capacity, exact generation identity, ACK progress and retirement.
//! - **Depends on.** The ACK tree, execution admission and execution-sensitive primitives.
//! - **Must not know.** Graph Models, placement, connectors or persistent storage.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]
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
#[cfg(test)]
use nervix_execution::MemoryClass;
use nervix_execution::Reservation;
use nervix_models::RemoteAckTiming;
use nervix_primitives::{
    collections::{ConcurrentQueue, PopError, PushError},
    publication::ArcSwap,
    sync::{
        Notify,
        atomic::{AtomicUsize, Ordering},
        blocking::Mutex,
    },
};

use super::{
    relay_memory_hold::{RelayMemoryHold, RelayMemoryHolding},
    remote_dispatch::{RelayAdmissionUpdate, RemoteDispatchError},
    *,
};

// The checked two-sided capacity leaves the top bit free to seal first claims at shutdown.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the top claim bit seals first registrations at shutdown"
    )
)]
const CLAIMS_CLOSED: usize = 1_usize << (usize::BITS - 1);
// A row's metadata alone exceeds two bytes, so the existing 32-MiB relay wire budget is
// stricter than this address space. Row positions never overlap delivery generations.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "correlation route identity reserves twenty-four bits for row positions"
    )
)]
const ROW_POSITIONS: u64 = 1 << 24;
#[cfg(all(test, not(any(feature = "loom", feature = "shuttle"))))]
const ADMISSION_SWEEPS: u64 =
    REMOTE_RELAY_TOTAL_TIMEOUT.as_secs() / REMOTE_ACK_SILENCE_SWEEP_INTERVAL.as_secs();

/// The node supplies immutable routing positions. A position owns one delivery, with independent
/// record outcomes. Each side claims unused positions on demand and recycles returned positions
/// through its own free queue, so full record capacity cannot prevent admission. A returned
/// position retains its generation.
pub(super) struct RemoteDispatchRegistry {
    slots: Box<[Mutex<CorrelationSlot>]>,
    deliveries: ConcurrentQueue<usize>,
    admissions: ConcurrentQueue<usize>,
    next_delivery: AtomicUsize,
    next_admission: AtomicUsize,
    delivery_capacity: usize,
    executor: Executor,
    timing: ArcSwap<RemoteAckTiming>,
    changed: Notify,
    next_sweep: Mutex<Option<Instant>>,
}

/// How storing the acknowledgement records of one delivery ended.
#[derive(Debug)]
pub(super) enum RecordStorage {
    /// The records are stored, under one registration for each row that carries an
    /// acknowledgement.
    Stored {
        registrations: Vec<Option<u64>>,
        timing: RemoteAckTiming,
    },
    /// The delivery stopped needing them while it waited for room to store them.
    Cancelled,
}

enum CorrelationSlot {
    Open {
        generation: u64,
        correlation: Option<RemoteCorrelation>,
    },
    Closed,
}

/// How many positions each side has handed out for the first time. A side claims its positions in
/// order, so the positions below its count are the only ones a correlation has ever occupied.
struct FirstClaims {
    deliveries: usize,
    admissions: usize,
}

/// The positions a correlation has ever occupied, deliveries before admissions.
type ClaimedPositions = std::iter::Chain<std::ops::Range<usize>, std::ops::Range<usize>>;

struct RemoteCorrelation {
    route: u64,
    state: RemoteCorrelationState,
}

enum RemoteCorrelationState {
    Records(DeliveryAcks),
    Admission(watch::Sender<RelayAdmissionUpdate>),
}

struct DeliveryAcks {
    rows: DeliveryRows,
    remaining: usize,
    _memory: Reservation,
}

/// Keep cohort state in the delivery's existing row allocation. Empty correlation positions
/// retain no timing state, and each row keeps its existing width and allocation count.
struct DeliveryRows(Vec<DeliveryRow>);
enum DeliveryRow {
    Policy { timing: SweepPolicy, next: Instant },
    Record(Option<PendingRemoteAck>),
}
const _: () =
    assert!(std::mem::size_of::<DeliveryRow>() <= std::mem::size_of::<Option<PendingRemoteAck>>());

#[derive(Clone, Copy)]
struct SweepPolicy {
    interval_nanos: u64,
    silence_nanos: u64,
    admission_nanos: u64,
}
impl SweepPolicy {
    fn new(timing: RemoteAckTiming) -> Self {
        let nanos = |value: std::time::Duration| {
            u64::try_from(value.as_nanos())
                .assured("validated timing durations fit in u64 nanoseconds")
        };
        Self {
            interval_nanos: nanos(timing.remote_ack_sweep_interval),
            silence_nanos: nanos(timing.remote_ack_silence_timeout),
            admission_nanos: nanos(timing.remote_relay_total_timeout),
        }
    }
    fn interval(self) -> std::time::Duration {
        std::time::Duration::from_nanos(self.interval_nanos)
    }
    fn silent_sweeps(self) -> u64 {
        self.silence_nanos / self.interval_nanos
    }
    fn admission_sweeps(self) -> u64 {
        self.admission_nanos / self.interval_nanos
    }
}
impl DeliveryRows {
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies one immediate monotonic clock read for \
                                   the captured cohort deadline")
    )]
    fn new(capacity: usize, timing: RemoteAckTiming, now: impl FnOnce() -> Instant) -> Self {
        let mut rows = Vec::with_capacity(
            capacity
                .checked_add(1)
                .assured("wire row count leaves room for its cohort"),
        );
        rows.push(DeliveryRow::Policy {
            timing: SweepPolicy::new(timing),
            next: now() + timing.remote_ack_sweep_interval,
        });
        Self(rows)
    }
    fn push(&mut self, row: Option<PendingRemoteAck>) {
        self.0.push(DeliveryRow::Record(row));
    }
    fn get_mut(&mut self, index: usize) -> Option<&mut Option<PendingRemoteAck>> {
        match self.0.get_mut(index.checked_add(1)?)? {
            DeliveryRow::Record(row) => Some(row),
            DeliveryRow::Policy { .. } => None,
        }
    }
    fn policy(&mut self) -> (&SweepPolicy, &mut Instant) {
        match &mut self.0[0] {
            DeliveryRow::Policy { timing, next } => (timing, next),
            DeliveryRow::Record(_) => unreachable!("a delivery starts with its cohort"),
        }
    }
    fn iter_mut(&mut self) -> impl Iterator<Item = &mut Option<PendingRemoteAck>> {
        self.0.iter_mut().skip(1).map(|entry| match entry {
            DeliveryRow::Record(row) => row,
            DeliveryRow::Policy { .. } => unreachable!("cohort precedes every record"),
        })
    }
    fn into_records(self) -> impl Iterator<Item = PendingRemoteAck> {
        self.0.into_iter().filter_map(|entry| match entry {
            DeliveryRow::Record(row) => row,
            DeliveryRow::Policy { .. } => None,
        })
    }
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

    fn swept(&mut self, timing: SweepPolicy) -> bool {
        match &mut self.phase {
            RemoteAckPhase::AwaitingAdmission { sweeps } => {
                *sweeps = sweeps
                    .checked_add(1)
                    .assured("admission sweeps end at the finite admission bound");
                *sweeps > timing.admission_sweeps()
            }
            RemoteAckPhase::Admitted { silent_sweeps } => {
                *silent_sweeps = silent_sweeps
                    .checked_add(1)
                    .assured("silence sweeps end at the finite silence bound");
                *silent_sweeps > timing.silent_sweeps()
            }
        }
    }

    fn expiration_reason(&self, timing: SweepPolicy) -> String {
        match self.phase {
            RemoteAckPhase::AwaitingAdmission { .. } => format!(
                "node '{}' did not admit the forwarded record within {}",
                self.receiver,
                humantime::format_duration(std::time::Duration::from_nanos(timing.admission_nanos))
            ),
            RemoteAckPhase::Admitted { .. } => format!(
                "node '{}' reported nothing about the forwarded record for {}",
                self.receiver,
                humantime::format_duration(std::time::Duration::from_nanos(timing.silence_nanos))
            ),
        }
    }
}

impl RemoteDispatchRegistry {
    pub(super) fn timing(&self) -> RemoteAckTiming {
        **self.timing.load()
    }
    pub(super) fn install_timing(&self, timing: RemoteAckTiming) {
        self.timing.store(StdArc::new(timing));
    }

    pub(super) fn new(executor: Executor, capacity: usize) -> Self {
        Self::build(executor, capacity)
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
        for _ in 0..total {
            slots.push(Mutex::new(CorrelationSlot::Open {
                generation: 0,
                correlation: None,
            }));
        }
        Self {
            slots: slots.into_boxed_slice(),
            deliveries,
            admissions,
            next_delivery: AtomicUsize::new(0),
            next_admission: AtomicUsize::new(0),
            delivery_capacity: capacity,
            timing: ArcSwap::from_pointee(RemoteAckTiming::default()),
            changed: Notify::new(),
            next_sweep: Mutex::new(None),
            executor,
        }
    }

    fn register(
        &self,
        state: RemoteCorrelationState,
    ) -> error_stack::Result<u64, RemoteDispatchError> {
        let (free, next, offset) = match &state {
            RemoteCorrelationState::Records(_) => (&self.deliveries, &self.next_delivery, 0),
            RemoteCorrelationState::Admission(_) => (
                &self.admissions,
                &self.next_admission,
                self.delivery_capacity,
            ),
        };
        let position = match free.pop() {
            Ok(position) => position,
            Err(PopError::Empty) => self.claim_fresh(next, offset)?,
            Err(PopError::Closed) => {
                return Err(Report::new(RemoteDispatchError::CorrelationCapacity {
                    capacity: self.delivery_capacity,
                }));
            }
        };
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

    fn claim_fresh(
        &self,
        next: &AtomicUsize,
        offset: usize,
    ) -> error_stack::Result<usize, RemoteDispatchError> {
        // Atomic modification order arbitrates a first claim against shutdown; the slot guard
        // publishes the correlation itself.
        let mut position = next.load(Ordering::Relaxed);
        loop {
            if position >= self.delivery_capacity {
                return Err(Report::new(RemoteDispatchError::CorrelationCapacity {
                    capacity: self.delivery_capacity,
                }));
            }
            match next.compare_exchange(
                position,
                position + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(position + offset),
                Err(observed) => position = observed,
            }
        }
    }

    #[cfg(test)]
    pub(super) fn register_ack(
        &self,
        receiver: ClusterNodeName,
        acks: AckSet,
    ) -> error_stack::Result<u64, RemoteDispatchError> {
        self.register_ack_with_clock(receiver, acks, Instant::now)
    }

    #[cfg(test)]
    pub(super) fn register_ack_with_clock(
        &self,
        receiver: ClusterNodeName,
        acks: AckSet,
        now: impl FnOnce() -> Instant,
    ) -> error_stack::Result<u64, RemoteDispatchError> {
        let registrations = self.register_acks_with_clock(receiver, vec![acks], now)?;
        Ok(registrations
            .into_iter()
            .next()
            .flatten()
            .assured("the fixture registers a nonempty record acknowledgement"))
    }

    /// The relay memory the records of `acks` take while they wait for `receiver`, or `None` when
    /// no row carries an acknowledgement to store.
    fn record_storage_bytes(
        receiver: &ClusterNodeName,
        acks: &[AckSet],
    ) -> error_stack::Result<Option<u64>, RemoteDispatchError> {
        let row_count = u64::try_from(acks.len()).assured("a row allocation length fits in u64");
        if row_count > ROW_POSITIONS {
            return Err(Report::new(
                RemoteDispatchError::CorrelationIdentityExhausted,
            ));
        }
        if acks.iter().all(AckSet::is_empty) {
            return Ok(None);
        }
        let bytes_per_row = std::mem::size_of::<DeliveryRow>()
            .checked_add(receiver.as_str().len())
            .assured("a node name and a record owner fit in usize");
        let bytes = acks
            .len()
            .checked_mul(bytes_per_row)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<DeliveryRow>()))
            .assured("a valid relay frame's record owners fit in usize");
        Ok(Some(
            u64::try_from(bytes).assured("a record owner allocation fits in u64"),
        ))
    }

    /// Stores the records of `acks`, to be resolved by `receiver`, holding the delivery back while
    /// the relay budget has no room for them.
    ///
    /// The wait reads the budget rather than queueing in it, as a receiver's watch charge does, and
    /// ends without storing anything as soon as `cancelled` resolves. Records larger than the whole
    /// budget, or still without room at `deadline`, are refused with the budget's refusal beneath
    /// `CorrelationMemory`.
    pub(super) async fn store_acks(
        &self,
        receiver: ClusterNodeName,
        acks: Vec<AckSet>,
        deadline: Instant,
        timing: RemoteAckTiming,
        cancelled: impl std::future::Future<Output = ()>,
    ) -> error_stack::Result<RecordStorage, RemoteDispatchError> {
        let Some(bytes) = Self::record_storage_bytes(&receiver, &acks)? else {
            return Ok(RecordStorage::Stored {
                registrations: vec![None; acks.len()],
                timing,
            });
        };
        let holding = RelayMemoryHold::new(bytes, deadline)
            .charge(&self.executor, cancelled)
            .await
            .change_context(RemoteDispatchError::CorrelationMemory)?;
        let RelayMemoryHolding::Charged(memory) = holding else {
            return Ok(RecordStorage::Cancelled);
        };
        let registrations =
            self.register_charged_acks(receiver, acks, memory, timing, Instant::now)?;
        Ok(RecordStorage::Stored {
            registrations,
            timing,
        })
    }

    /// Stores the records of `acks` at once, or refuses them when the relay budget has no room.
    #[cfg(all(test, not(any(feature = "loom", feature = "shuttle"))))]
    pub(super) fn register_acks(
        &self,
        receiver: ClusterNodeName,
        acks: Vec<AckSet>,
    ) -> error_stack::Result<Vec<Option<u64>>, RemoteDispatchError> {
        self.register_acks_with_clock(receiver, acks, Instant::now)
    }

    #[cfg(test)]
    fn register_acks_with_clock(
        &self,
        receiver: ClusterNodeName,
        acks: Vec<AckSet>,
        now: impl FnOnce() -> Instant,
    ) -> error_stack::Result<Vec<Option<u64>>, RemoteDispatchError> {
        let Some(bytes) = Self::record_storage_bytes(&receiver, &acks)? else {
            return Ok(vec![None; acks.len()]);
        };
        let memory = self
            .executor
            .try_reserve(MemoryClass::Relay, bytes)
            .change_context(RemoteDispatchError::CorrelationMemory)?;
        self.register_charged_acks(receiver, acks, memory, self.timing(), now)
    }

    /// Registers the records of `acks`, which `memory` was charged for, to be resolved by
    /// `receiver`.
    fn register_charged_acks(
        &self,
        receiver: ClusterNodeName,
        acks: Vec<AckSet>,
        memory: Reservation,
        timing: RemoteAckTiming,
        now: impl FnOnce() -> Instant,
    ) -> error_stack::Result<Vec<Option<u64>>, RemoteDispatchError> {
        let remaining = acks.iter().filter(|acks| !acks.is_empty()).count();
        let mut rows = DeliveryRows::new(acks.len(), timing, now);
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
        let next_sweep = *rows.policy().1;
        let base = self.register(RemoteCorrelationState::Records(DeliveryAcks {
            rows,
            remaining,
            _memory: memory,
        }))?;
        SilenceSweeps::watching(&self.changed, &self.next_sweep).announce(next_sweep);
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
        let next_sweep = Instant::now() + self.timing().remote_ack_sweep_interval;
        let id = self.register(RemoteCorrelationState::Admission(sender))?;
        SilenceSweeps::watching(&self.changed, &self.next_sweep).announce(next_sweep);
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

    /// The positions a correlation has ever occupied: each side's first claims, deliveries before
    /// admissions. Every other position is still as it was built, open and empty.
    fn claimed_positions(&self, claims: &FirstClaims) -> ClaimedPositions {
        let admission_end = self
            .delivery_capacity
            .checked_add(claims.admissions)
            .assured(
                "a side's first claims never exceed the capacity its positions were built for",
            );
        (0..claims.deliveries).chain(self.delivery_capacity..admission_end)
    }

    /// The positions one silence sweep visits: those a side has claimed so far.
    ///
    /// A position no registrar has claimed has never held a correlation, so a sweep takes no guard
    /// for it: a node that forwards nothing sweeps nothing, however many positions it was built
    /// with. The counts bound the scan and order nothing: the slot guard publishes a correlation,
    /// and no reader relies on a sweep having observed a claim. A first claim these loads do not
    /// observe registers beside this sweep, and its silence is counted from the first sweep that
    /// finds it under its guard.
    fn swept_positions(&self) -> ClaimedPositions {
        let claims = FirstClaims {
            deliveries: self.next_delivery.load(Ordering::Relaxed) & !CLAIMS_CLOSED,
            admissions: self.next_admission.load(Ordering::Relaxed) & !CLAIMS_CLOSED,
        };
        self.claimed_positions(&claims)
    }

    /// Runs `during` while the guard of `position` is held, for a check of which guards a sweep
    /// may wait for.
    #[cfg(all(test, feature = "shuttle"))]
    pub(super) fn while_position_is_held<T>(
        &self,
        position: usize,
        during: impl FnOnce() -> T,
    ) -> T {
        let _held = self.slots[position].lock();
        during()
    }

    #[cfg(test)]
    pub(super) fn fail_silent_acks(&self) -> BTreeMap<ClusterNodeName, usize> {
        let mut failed = BTreeMap::new();
        SilenceSweeps::watching(&self.changed, &self.next_sweep).sweep_once(|| {
            let (outcomes, next) = self.sweep_acks(None);
            failed = outcomes;
            next
        });
        failed
    }

    /// Count at most one sweep per delivery, even when its timer runs late. Each delivery keeps
    /// the cadence and report window captured when it registered; a policy update cannot mix them.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the delivery owns the bounded row iterator; each step only \
                                   borrows its next acknowledgement record")
    )]
    fn sweep_acks(
        &self,
        now: Option<Instant>,
    ) -> (BTreeMap<ClusterNodeName, usize>, Option<Instant>) {
        let mut failed = BTreeMap::new();
        let mut next_sweep: Option<Instant> = None;
        let admission_interval = self.timing().remote_ack_sweep_interval;

        for position in self.swept_positions() {
            let mut slot = self.slots[position].lock();
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
                    let (timing, delivery_next) = records.rows.policy();
                    let timing = *timing;
                    if let Some(now) = now {
                        if now < *delivery_next {
                            next_sweep = Some(match next_sweep {
                                Some(next) => next.min(*delivery_next),
                                None => *delivery_next,
                            });
                            continue;
                        }
                        *delivery_next = now + timing.interval();
                    }
                    let delivery_next = *delivery_next;
                    for row in records.rows.iter_mut() {
                        if let Some(pending) = row
                            && pending.swept(timing)
                        {
                            let pending = row
                                .take()
                                .verified("the swept row was present under the owner guard");
                            let reason = pending.expiration_reason(timing);
                            expired.push((pending, reason));
                            records.remaining = records
                                .remaining
                                .checked_sub(1)
                                .assured("one live record expires exactly once");
                        }
                    }
                    if records.remaining > 0 {
                        next_sweep = Some(match next_sweep {
                            Some(next) => next.min(delivery_next),
                            None => delivery_next,
                        });
                    }
                    records.remaining == 0
                }
                RemoteCorrelationState::Admission(admission) => {
                    if admission.is_closed() {
                        true
                    } else {
                        if let Some(now) = now {
                            let next = now + admission_interval;
                            next_sweep = Some(match next_sweep {
                                Some(prior) => prior.min(next),
                                None => next,
                            });
                        }
                        false
                    }
                }
            };
            let ended = if reclaim { entry.take() } else { None };
            drop(slot);
            if reclaim {
                self.recycle(position);
            }
            drop(ended);
            for (pending, reason) in expired {
                pending.acks.no_ack(reason);
                let count = failed.entry(pending.receiver).or_insert(0_usize);
                *count = count
                    .checked_add(1)
                    .assured("the charged record count bounds one sweep's outcomes");
            }
        }
        (failed, next_sweep)
    }

    /// Sleep until the earliest retained delivery's next sweep. Register for new work before
    /// scanning, so an arrival cannot be missed between an empty scan and waiting. Idle registries
    /// require no periodic scan. Every completed scan yields before it can scan again.
    pub(super) async fn sweep_silent_acks(&self) {
        SilenceSweeps::watching(&self.changed, &self.next_sweep).run(|| {
            let (failed, next) = self.sweep_acks(Some(Instant::now()));
            for (receiver, acknowledgements) in failed {
                warn!(target_node = %receiver, acknowledgements, "remote record acknowledgement owner expired");
            }
            next
        }).await;
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "each delivery consumes its bounded owned row iterator before resolving \
                      those acknowledgement records"
        )
    )]
    pub(super) fn shutdown(&self) {
        // Seal first claims before choosing the scan bounds. A claim won before either seal is
        // included even when its registrar has not reached the slot yet: the slot guard then
        // decides whether registration or shutdown owns its final state.
        let deliveries =
            self.next_delivery.fetch_or(CLAIMS_CLOSED, Ordering::AcqRel) & !CLAIMS_CLOSED;
        let admissions = self
            .next_admission
            .fetch_or(CLAIMS_CLOSED, Ordering::AcqRel)
            & !CLAIMS_CLOSED;
        let claims = FirstClaims {
            deliveries,
            admissions,
        };
        self.deliveries.close();
        self.admissions.close();
        for position in self.claimed_positions(&claims) {
            let slot = &self.slots[position];
            let state = std::mem::replace(&mut *slot.lock(), CorrelationSlot::Closed);
            if let CorrelationSlot::Open {
                correlation: Some(correlation),
                ..
            } = state
            {
                match correlation.state {
                    RemoteCorrelationState::Records(records) => {
                        for pending in records.rows.into_records() {
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

/// The production sweep driver, shared by runtime and cancellation checks.
pub(super) struct SilenceSweeps<'a> {
    changed: &'a Notify,
    next: &'a Mutex<Option<Instant>>,
}
impl<'a> SilenceSweeps<'a> {
    pub(super) fn watching(changed: &'a Notify, next: &'a Mutex<Option<Instant>>) -> Self {
        Self { changed, next }
    }

    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "the registry publishes one earliest wake deadline",
            key = "node remote-correlation registry",
            bound = "one inline optional deadline per registry"
        )
    )]
    fn merge_deadline(&self, next: Option<Instant>) -> bool {
        let Some(next) = next else { return false };
        let mut published = self.next.lock();
        match *published {
            Some(current) if current <= next => false,
            _ => {
                *published = Some(next);
                true
            }
        }
    }

    fn announce(&self, next: Instant) {
        if self.merge_deadline(Some(next)) {
            self.changed.notify_one();
        }
    }

    /// Consume preceding announcements before scanning. Registrations racing the scan publish
    /// beside it, and merging its result retains their earlier deadlines. No deadline guard is
    /// held while visiting correlation positions or resolving their acknowledgements.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the owner supplies one bounded registry scan and its deadline")
    )]
    fn sweep_once(&self, sweep: impl FnOnce() -> Option<Instant>) {
        *self.next.lock() = None;
        self.merge_deadline(sweep());
    }

    /// Register before scanning and yield after every synchronous sweep. Even an overdue next
    /// deadline cannot prevent cancellation from running between two sweeps.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "the caller supplies one synchronous bounded registry sweep \
                                   and its next deadline; the driver yields between calls")
    )]
    pub(super) async fn run(self, mut sweep: impl FnMut() -> Option<Instant>) {
        loop {
            nervix_primitives::task::consume_budget().await;
            let mut changed = std::pin::pin!(self.changed.notified());
            changed.as_mut().enable();
            self.sweep_once(&mut sweep);
            nervix_primitives::task::yield_now().await;
            loop {
                let next = *self.next.lock();
                let notified = if let Some(next) = next {
                    nervix_primitives::select! {
                        _ = &mut changed => true,
                        _ = sleep_until(next) => false,
                    }
                } else {
                    changed.as_mut().await;
                    true
                };
                if !notified {
                    break;
                }
                changed.set(self.changed.notified());
                changed.as_mut().enable();
                // An announcement changes only the timer. In particular, ordinary later
                // arrivals must never turn a per-deadline scan into a per-arrival scan.
                nervix_primitives::task::yield_now().await;
            }
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
#[path = "remote_ack_owner_policy_tests.rs"]
pub(super) mod policy_tests;

#[cfg(all(test, not(any(feature = "loom", feature = "shuttle"))))]
#[path = "remote_ack_owner_model.rs"]
mod model;

#[cfg(all(test, not(any(feature = "loom", feature = "shuttle"))))]
#[path = "remote_ack_owner_properties.rs"]
mod properties;

#[cfg(all(test, not(any(feature = "loom", feature = "shuttle"))))]
#[path = "remote_ack_owner_tests.rs"]
mod tests;

#[cfg(all(test, not(any(feature = "loom", feature = "shuttle"))))]
use super::remote_dispatch::{
    REMOTE_ACK_SILENCE_SWEEP_INTERVAL, REMOTE_ACK_SILENCE_TIMEOUT, REMOTE_ACK_SILENT_SWEEPS,
    REMOTE_RELAY_TOTAL_TIMEOUT,
};
