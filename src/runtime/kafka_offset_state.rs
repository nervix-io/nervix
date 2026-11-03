//! Native Kafka offset state and assignment-qualified access.
//!
//! Layer: data plane.
//! - **Owns.** Partition offsets, conservative revisions, native
//!   capture and conversion, replica installation, persistence state and quorum observation.
//! - **Depends on.** Placement vocabulary, assignment capabilities and execution primitives.
//! - **Must not know.** Brokers, graph scheduling, Plexus or transfer framing.

#![cfg_attr(
    nervix_lint,
    nervix::layer(data_plane, reason = "owns the data plane boundary declared above")
)]

use std::{collections::BTreeMap, time::Duration};

use ahash::HashMap;
use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_checkpoint_replication::{CheckpointReplication, ReplicaProgress};
use nervix_connector_kafka::KafkaOffsetPosition;
use nervix_models::ClusterNodeName;
use nervix_primitives::{
    publication::ArcSwap,
    sync::{
        Arc, StdArc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Instant, timeout_at},
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use super::{
    BackupKafkaPartitionOffset, PersistedRuntimeStateEntry, RuntimePersistenceError,
    RuntimeStateKind, RuntimeStateOperationError, RuntimeStatePlacement, StateAssignmentAuthority,
    StateAssignmentToken, StateAuthorityError, StateCapability, StateReplicationRoles,
    lsm_sequence::LsmSequence, state_replication::StateReplicationError,
};

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct KafkaOffsetEntrySnapshot {
    topic: String,
    partition: i32,
    next_offset: i64,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct KafkaOffsetSnapshot {
    applied_start_generation: Option<u64>,
    offsets: Vec<KafkaOffsetEntrySnapshot>,
    format: [u8; 16],
}

/// The current native checkpoint's required format identity, verified before interpreting state.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "the native Kafka checkpoint format reserves exactly these sixteen signature \
                  bytes"
    )
)]
const KAFKA_OFFSET_FORMAT: [u8; 16] = *b"NervixKafkaState";

pub(super) fn encode_start_candidate(
    generation: u64,
    positions: Vec<KafkaOffsetPosition>,
) -> error_stack::Result<Vec<u8>, RuntimePersistenceError> {
    let offsets = positions
        .into_iter()
        .map(|position| KafkaOffsetEntrySnapshot {
            topic: position.topic,
            partition: position.partition,
            next_offset: position.offset,
        })
        .collect();
    rkyv::to_bytes::<rkyv::rancor::Error>(&KafkaOffsetSnapshot {
        applied_start_generation: Some(generation),
        offsets,
        format: KAFKA_OFFSET_FORMAT,
    })
    .map(|bytes| bytes.to_vec())
    .change_context(RuntimePersistenceError::EncodeState)
}

pub(super) fn decode_start_positions(
    payload: &[u8],
    generation: u64,
    cancellation: &nervix_execution::Cancellation,
) -> error_stack::Result<Vec<KafkaOffsetPosition>, RuntimePersistenceError> {
    let snapshot = access_offset_snapshot(payload)?;
    let observed = snapshot
        .applied_start_generation
        .as_ref()
        .map(|value| value.to_native());
    if observed != Some(generation) {
        return Err(Report::new(KafkaOffsetCheckpointError::StartGeneration {
            expected: generation,
            observed,
        })
        .change_context(RuntimePersistenceError::DecodeState));
    }
    let mut positions = Vec::with_capacity(snapshot.offsets.len());
    for entry in snapshot.offsets.iter() {
        cancellation
            .check()
            .change_context(RuntimePersistenceError::Cancelled)?;
        positions.push(KafkaOffsetPosition {
            topic: entry.topic.as_str().to_owned(),
            partition: entry.partition.to_native(),
            offset: entry.next_offset.to_native(),
        });
    }
    Ok(positions)
}

#[derive(Debug, thiserror::Error)]
enum KafkaOffsetCheckpointError {
    #[error(
        "Kafka offset checkpoint format signature does not match the current format; recreate the \
         runtime state"
    )]
    FormatSignature,
    #[error("the ingestor layout is not a concrete instance within its declared count")]
    InstanceSelection,
    #[error("Kafka start generation {expected} is no longer held (observed {observed:?})")]
    StartGeneration {
        expected: u64,
        observed: Option<u64>,
    },
    #[error(
        "topic '{topic}' partition {partition} is held outside instance {source_index}'s \
         ownership in a {count}-unit layout"
    )]
    ForeignPartition {
        source_index: u64,
        count: std::num::NonZeroU64,
        topic: String,
        partition: i32,
    },
}

pub(super) fn project_offset_snapshot(
    payload: &[u8],
    selection: nervix_interconnect::IngestorUnitSelection,
    cancellation: &nervix_execution::Cancellation,
) -> error_stack::Result<Vec<u8>, RuntimePersistenceError> {
    let nervix_models::Unit::Instance(index) = selection.unit else {
        return Err(Report::new(KafkaOffsetCheckpointError::InstanceSelection)
            .change_context(RuntimePersistenceError::DecodeState));
    };
    if index >= selection.count.get() {
        return Err(Report::new(KafkaOffsetCheckpointError::InstanceSelection)
            .change_context(RuntimePersistenceError::DecodeState));
    }
    let archived = access_offset_snapshot(payload)?;
    let mut offsets = Vec::new();
    for entry in archived.offsets.iter() {
        cancellation
            .check()
            .change_context(RuntimePersistenceError::Cancelled)?;
        let partition = entry.partition.to_native();
        // Kafka partitions are numbered consecutively from zero. Unsigned conversion also gives
        // every representable native key one destination in representation checks.
        if u64::from(partition.cast_unsigned()) % selection.count.get() == index {
            offsets.push(KafkaOffsetEntrySnapshot {
                topic: entry.topic.as_str().to_owned(),
                partition,
                next_offset: entry.next_offset.to_native(),
            });
        }
    }
    rkyv::to_bytes::<rkyv::rancor::Error>(&KafkaOffsetSnapshot {
        applied_start_generation: archived
            .applied_start_generation
            .as_ref()
            .map(|value| value.to_native()),
        offsets,
        format: KAFKA_OFFSET_FORMAT,
    })
    .map(|bytes| bytes.to_vec())
    .change_context(RuntimePersistenceError::EncodeState)
}

pub(super) struct IngestorOffsetCheckpoint {
    pub(super) unit: nervix_models::Unit,
    pub(super) payload: Vec<u8>,
}

/// Prefer the actual source unit's progress over its still-pending canonical seed on unit zero.
/// A source that has not adopted the newest start contributes no earlier-generation positions.
pub(super) fn merge_offset_snapshots(
    payloads: &[IngestorOffsetCheckpoint],
    source_count: std::num::NonZeroU64,
    cancellation: &nervix_execution::Cancellation,
) -> error_stack::Result<Vec<u8>, RuntimePersistenceError> {
    let generation = payloads
        .iter()
        .map(|checkpoint| {
            access_offset_snapshot(&checkpoint.payload).map(|snapshot| {
                snapshot
                    .applied_start_generation
                    .as_ref()
                    .map(|value| value.to_native())
            })
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .flatten();
    let mut table = KafkaOffsetTable {
        applied_start_generation: generation,
        ..KafkaOffsetTable::default()
    };
    enum SelectedPosition {
        Pending(i64),
        Owned(i64),
    }
    let mut selected = BTreeMap::new();
    for checkpoint in payloads {
        let nervix_models::Unit::Instance(source_index) = checkpoint.unit else {
            return Err(Report::new(KafkaOffsetCheckpointError::InstanceSelection)
                .change_context(RuntimePersistenceError::DecodeState));
        };
        if source_index >= source_count.get() {
            return Err(Report::new(KafkaOffsetCheckpointError::InstanceSelection)
                .change_context(RuntimePersistenceError::DecodeState));
        }
        let incoming = access_offset_snapshot(&checkpoint.payload)?;
        if incoming
            .applied_start_generation
            .as_ref()
            .map(|value| value.to_native())
            != generation
        {
            continue;
        }
        for entry in incoming.offsets.iter() {
            cancellation
                .check()
                .change_context(RuntimePersistenceError::Cancelled)?;
            let topic = entry.topic.as_str();
            let partition = entry.partition.to_native();
            let owner = u64::from(partition.cast_unsigned()) % source_count.get();
            let key = (topic.to_owned(), partition);
            let offset = entry.next_offset.to_native();
            if source_index == owner {
                selected.insert(key, SelectedPosition::Owned(offset));
            } else if source_index == 0 {
                let position = selected
                    .entry(key)
                    .or_insert(SelectedPosition::Pending(offset));
                if let SelectedPosition::Pending(value) = position {
                    *value = offset;
                }
            } else {
                return Err(Report::new(KafkaOffsetCheckpointError::ForeignPartition {
                    source_index,
                    count: source_count,
                    topic: topic.to_owned(),
                    partition,
                })
                .change_context(RuntimePersistenceError::DecodeState));
            }
        }
    }
    for ((topic, partition), position) in selected {
        let offset = match position {
            SelectedPosition::Pending(offset) | SelectedPosition::Owned(offset) => offset,
        };
        table.record_partition(&topic, partition, offset);
    }
    table.encode()
}

/// Where each recorded partition resumes.
///
/// A table is published whole and replaced only when the recorded partitions change.
/// Every partition's next offset lives in a slot of its own that later tables share, so a commit to
/// a partition the table already records moves that offset in place.
#[derive(Debug, Clone, Default)]
struct KafkaOffsetTable {
    /// The most recent explicit domain start applied to every partition in this table.
    applied_start_generation: Option<u64>,
    topics: HashMap<String, BTreeMap<i32, Arc<AtomicI64>>>,
}

#[derive(Debug)]
pub(super) struct ReplicatedKafkaOffsetState {
    placement: RuntimeStatePlacement,
    assignment: StateAssignmentAuthority,
    /// Published only under the assignment barrier; commits to recorded partitions move their
    /// slots without it.
    offsets: ArcSwap<KafkaOffsetTable>,
    current_lsm: LsmSequence,
    last_persisted_lsm: AtomicU64,
    /// What each replica reported holding and the offer of the newest offsets to them while this
    /// node originates the offsets, and the owner's announcements while it replicates them.
    replication: CheckpointReplication,
    /// Startup-only coordination; ordinary offset commits never reach this owner.
    start: super::kafka_start::KafkaStartCoordination,
}

/// Read-only access to one Kafka offset state. The handle exposes snapshots and offset lookup but
/// carries no assignment token, so it cannot originate or install state.
#[derive(Debug, Clone)]
pub struct KafkaOffsetStateRead {
    state: Arc<ReplicatedKafkaOffsetState>,
}

/// One topology and conservative revision retained while its offsets are streamed. Later commits
/// may move the shared slots forward; an offset included ahead of this revision is sent again by
/// the next capture, as it is for an ordinary checkpoint.
pub(super) struct CapturedKafkaOffsets {
    pub(super) lsm: u64,
    table: StdArc<KafkaOffsetTable>,
}

/// Authoritative access held by the Kafka ingestor for one concrete assignment generation.
#[derive(Debug, Clone)]
pub struct KafkaOffsetStateOriginator {
    read: KafkaOffsetStateRead,
    assignment: StateAssignmentToken,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum KafkaStartApplication {
    Canonical,
    Unit,
}

/// The originator one running Kafka ingestor commits through. A reassignment that keeps the
/// ingestor running on its node, such as one that replaces a replica of its offsets, rebinds the
/// offsets to a new assignment generation; schedule application then hands the ingestor the
/// originator that binding issued. An ingestor instance the schedule stopped keeps the originator
/// it last held, which every later binding refuses.
#[derive(Debug)]
pub(super) struct KafkaOffsetOriginatorSlot {
    /// Replaced only by schedule application, for as long as the ingestor instance runs.
    originator: ArcSwap<KafkaOffsetStateOriginator>,
}

impl KafkaOffsetOriginatorSlot {
    pub(super) fn new(originator: KafkaOffsetStateOriginator) -> Self {
        Self {
            originator: ArcSwap::from_pointee(originator),
        }
    }

    /// The originator the ingestor's next operation acts under.
    pub(super) fn current(&self) -> StdArc<KafkaOffsetStateOriginator> {
        self.originator.load_full()
    }

    /// Hands the running ingestor the originator a reassignment's binding issued.
    pub(super) fn replace(&self, originator: KafkaOffsetStateOriginator) {
        self.originator.store(StdArc::new(originator));
    }
}

/// Replica installation access held by one synchronization task for one assignment generation.
#[derive(Debug, Clone)]
pub struct KafkaOffsetSnapshotInstaller {
    read: KafkaOffsetStateRead,
    assignment: StateAssignmentToken,
}

/// Local persistence is allowed for owners and replicas and is independent of either logical-state
/// mutation capability.
#[derive(Debug, Clone)]
pub(super) struct KafkaOffsetStatePersistence {
    read: KafkaOffsetStateRead,
}

#[derive(Debug)]
pub(super) struct KafkaOffsetStateAssignment {
    pub(super) originator: Option<KafkaOffsetStateOriginator>,
    pub(super) installer: Option<KafkaOffsetSnapshotInstaller>,
    pub(super) persistence: KafkaOffsetStatePersistence,
}

impl ReplicatedKafkaOffsetState {
    pub(super) fn new(
        placement: RuntimeStatePlacement,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> error_stack::Result<Self, RuntimePersistenceError> {
        let mut offsets = KafkaOffsetTable::default();
        let mut current_lsm = 0;
        if let Some(initial) = initial {
            current_lsm = initial.lsm;
            offsets = KafkaOffsetTable::decode(&initial.payload)?;
        }
        Ok(Self {
            placement,
            assignment: StateAssignmentAuthority::default(),
            offsets: ArcSwap::from_pointee(offsets),
            current_lsm: LsmSequence::restored(current_lsm),
            last_persisted_lsm: AtomicU64::new(current_lsm),
            replication: CheckpointReplication::new(),
            start: super::kafka_start::KafkaStartCoordination::default(),
        })
    }

    pub(super) fn bind(
        state: &Arc<Self>,
        roles: StateReplicationRoles,
        local_node: Option<&ClusterNodeName>,
    ) -> KafkaOffsetStateAssignment {
        let binding = state.assignment.rebind(roles, local_node);
        let read = KafkaOffsetStateRead {
            state: state.clone(),
        };
        KafkaOffsetStateAssignment {
            originator: binding
                .token_for(StateCapability::Originate)
                .map(|assignment| KafkaOffsetStateOriginator {
                    read: read.clone(),
                    assignment,
                }),
            installer: binding
                .token_for(StateCapability::InstallSnapshot)
                .map(|assignment| KafkaOffsetSnapshotInstaller {
                    read: read.clone(),
                    assignment,
                }),
            persistence: KafkaOffsetStatePersistence { read: read.clone() },
        }
    }

    pub(super) fn read(state: &Arc<Self>) -> KafkaOffsetStateRead {
        KafkaOffsetStateRead {
            state: state.clone(),
        }
    }

    pub(super) fn current_originator(state: &Arc<Self>) -> Option<KafkaOffsetStateOriginator> {
        let assignment = state
            .assignment
            .current_binding()
            .token_for(StateCapability::Originate)?;
        Some(KafkaOffsetStateOriginator {
            read: Self::read(state),
            assignment,
        })
    }

    pub(super) fn replication(&self) -> &CheckpointReplication {
        &self.replication
    }

    /// Move the offset of a partition this state already records and stamp the change with the
    /// next revision, or report that the partition is not recorded.
    fn move_recorded_offset(&self, topic: &str, partition: i32, next_offset: i64) -> Option<u64> {
        let offsets = self.offsets.load();
        let slot = offsets.slot(topic, partition)?;
        slot.store(next_offset, Ordering::SeqCst);
        Some(self.current_lsm.advance())
    }

    /// Record a partition's first committed offset by publishing a table that includes it.
    ///
    /// Tables are only published under the barrier, so a partition this finds unrecorded stays
    /// unrecorded until the table published here records it.
    fn record_first_offset(&self, topic: &str, partition: i32, next_offset: i64) -> u64 {
        if let Some(lsm) = self.move_recorded_offset(topic, partition, next_offset) {
            return lsm;
        }
        let mut table = KafkaOffsetTable::clone(&self.offsets.load());
        table.record_partition(topic, partition, next_offset);
        self.offsets.store(StdArc::new(table));
        self.current_lsm.advance()
    }
}

impl KafkaOffsetStateRead {
    pub(super) fn authorize_start_offer(
        &self,
        assignment: StateAssignmentToken,
    ) -> error_stack::Result<(), StateAuthorityError> {
        self.state
            .assignment
            .authorize(assignment, StateCapability::Originate, || ())
    }
    pub(super) fn start_coordination(&self) -> &super::kafka_start::KafkaStartCoordination {
        &self.state.start
    }

    pub(super) fn applied_start_generation(&self) -> Option<u64> {
        self.state.offsets.load().applied_start_generation
    }

    pub(super) fn placement(&self) -> &RuntimeStatePlacement {
        &self.state.placement
    }

    pub(super) fn next_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.state.offsets.load().next_offset(topic, partition)
    }

    pub(super) fn current_lsm(&self) -> u64 {
        self.state.current_lsm.current()
    }

    /// Check the revision before retaining a topology, without encoding an unchanged checkpoint.
    pub(super) fn capture_after(&self, after_lsm: Option<u64>) -> Option<CapturedKafkaOffsets> {
        self.state.assignment.serialize(|| {
            let lsm = self.current_lsm();
            if after_lsm.is_some_and(|after| lsm <= after) {
                return None;
            }
            Some(CapturedKafkaOffsets {
                lsm,
                table: self.state.offsets.load_full(),
            })
        })
    }

    /// Encode the offsets this state records, stamped with its revision.
    ///
    /// The barrier keeps the recorded partitions fixed while they are read. The revision is read
    /// first, so a commit admitted while the offsets are read is either inside that revision or
    /// ahead of it, and a later snapshot carries it again.
    pub(super) fn latest_snapshot(
        &self,
    ) -> error_stack::Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        self.state.assignment.serialize(|| {
            let lsm = self.state.current_lsm.current();
            let payload = self.state.offsets.load().encode()?;
            Ok(PersistedRuntimeStateEntry { lsm, payload })
        })
    }

    pub(super) fn primary_node(&self) -> Option<ClusterNodeName> {
        self.state.assignment.roles().primary_node.clone()
    }

    pub(super) fn required_replica_acks(&self) -> usize {
        self.state.assignment.roles().required_replica_acks
    }

    pub(super) fn replication(&self) -> &CheckpointReplication {
        &self.state.replication
    }

    /// Whether enough of the assigned replicas reported holding revision `lsm`.
    pub(super) fn replica_quorum_holds(&self, lsm: u64) -> bool {
        self.state
            .replication
            .with_progress(|progress| self.quorum_holds(progress, lsm))
    }

    /// Whether `progress` has at least the required number of the replicas assigned now holding
    /// revision `lsm`.
    fn quorum_holds(&self, progress: &ReplicaProgress, lsm: u64) -> bool {
        let roles = self.state.assignment.roles();
        progress.holding(&roles.replica_nodes, lsm) >= roles.required_replica_acks
    }

    /// Wait until enough assigned replicas report holding revision `lsm`, within the caller's
    /// checkpoint-operation budget.
    ///
    /// The wait registers for the next replica report before it reads what the replicas hold, so a
    /// report that lands in between wakes it instead of leaving the commit to its deadline. The
    /// replicas are the ones assigned when each report arrives.
    pub(super) async fn wait_for_replica_quorum(
        &self,
        lsm: u64,
        wait: Duration,
    ) -> error_stack::Result<(), StateReplicationError> {
        let deadline = Instant::now()
            .checked_add(wait)
            .assured("the bounded checkpoint operation fits a monotonic deadline");
        let quorum = self
            .state
            .replication
            .wait_until(|progress| self.quorum_holds(progress, lsm));
        if timeout_at(deadline, quorum).await.is_ok() || self.replica_quorum_holds(lsm) {
            return Ok(());
        }
        Err(Report::new(StateReplicationError::ReplicaQuorum {
            placement: self.placement().clone(),
            lsm,
            required_acks: self.required_replica_acks(),
        }))
    }
}

impl KafkaOffsetStateOriginator {
    pub(super) fn same_assignment(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.read.state, &other.read.state) && self.assignment == other.assignment
    }
    pub(super) fn assignment_token(&self) -> StateAssignmentToken {
        self.assignment
    }

    /// The receiving unit has durably adopted these seeds. Repeated or stale acknowledgements
    /// cannot remove another unit's positions or the coordinator's committed progress.
    pub(super) fn acknowledge_start_seed(
        &self,
        generation: u64,
        selection: nervix_interconnect::IngestorUnitSelection,
    ) -> error_stack::Result<(), StateAuthorityError> {
        let state = &self.read.state;
        state
            .assignment
            .authorize_exclusive(self.assignment, StateCapability::Originate, || {
                let nervix_models::Unit::Instance(index) = selection.unit else {
                    return;
                };
                if index == 0 || index >= selection.count.get() {
                    return;
                }
                let current = state.offsets.load();
                if current.applied_start_generation != Some(generation) {
                    return;
                }
                let mut table = KafkaOffsetTable::clone(&current);
                let mut changed = false;
                for partitions in table.topics.values_mut() {
                    partitions.retain(|partition, _| {
                        let retain =
                            u64::from(partition.cast_unsigned()) % selection.count.get() != index;
                        changed |= !retain;
                        retain
                    });
                }
                if changed {
                    table.topics.retain(|_, partitions| !partitions.is_empty());
                    state.offsets.store(StdArc::new(table));
                    state.current_lsm.advance();
                }
            })
    }
    pub(super) fn placement(&self) -> &RuntimeStatePlacement {
        self.read.placement()
    }

    pub(super) fn read(&self) -> &KafkaOffsetStateRead {
        &self.read
    }

    pub(super) fn persistence(&self) -> KafkaOffsetStatePersistence {
        KafkaOffsetStatePersistence {
            read: self.read.clone(),
        }
    }

    /// Apply an explicit domain start once, preserving commits when another instance resolved
    /// that same start concurrently. A delayed earlier generation cannot replace a newer start.
    /// The returned complete checkpoint must be persisted before the instance starts consuming.
    pub(super) fn apply_start(
        &self,
        generation: u64,
        offsets: Vec<KafkaOffsetPosition>,
    ) -> error_stack::Result<(u64, Vec<u8>), RuntimeStateOperationError> {
        self.apply_start_for(KafkaStartApplication::Unit, generation, offsets)
    }

    pub(super) fn apply_canonical_start(
        &self,
        generation: u64,
        offsets: Vec<KafkaOffsetPosition>,
    ) -> error_stack::Result<(u64, Vec<u8>), RuntimeStateOperationError> {
        self.apply_start_for(KafkaStartApplication::Canonical, generation, offsets)
    }

    fn apply_start_for(
        &self,
        application: KafkaStartApplication,
        generation: u64,
        offsets: Vec<KafkaOffsetPosition>,
    ) -> error_stack::Result<(u64, Vec<u8>), RuntimeStateOperationError> {
        let state = &self.read.state;
        let encoded = state
            .assignment
            .authorize_exclusive(self.assignment, StateCapability::Originate, || {
                let current = state.offsets.load();
                let mut table;
                match current.applied_start_generation {
                    Some(applied) if applied > generation => {
                        let lsm = state.current_lsm.current();
                        return current.encode().map(|payload| (lsm, payload));
                    }
                    Some(applied) if applied == generation => {
                        if application == KafkaStartApplication::Canonical {
                            let lsm = state.current_lsm.current();
                            return current.encode().map(|payload| (lsm, payload));
                        }
                        table = KafkaOffsetTable::clone(&current);
                        if !table.record_unrecorded_starts(offsets) {
                            let lsm = state.current_lsm.current();
                            return current.encode().map(|payload| (lsm, payload));
                        }
                    }
                    _ => {
                        table = KafkaOffsetTable::from_offsets(offsets);
                        table.applied_start_generation = Some(generation);
                    }
                }
                let table = StdArc::new(table);
                state.offsets.store(table.clone());
                let lsm = state.current_lsm.advance();
                table.encode().map(|payload| (lsm, payload))
            })
            .change_context(RuntimeStateOperationError::Authority)?;
        encoded.change_context(RuntimeStateOperationError::Persistence)
    }

    /// Record `next_offset` as where `partition` of `topic` resumes, returning the revision the
    /// commit is stamped with.
    ///
    /// A commit to a partition the state already records moves that partition's offset without
    /// the barrier. A partition's first commit changes which partitions are recorded, so it
    /// publishes a new table under the barrier. Neither encodes nor persists anything: the
    /// snapshot task does, on its interval.
    pub(super) fn apply_committed_offset(
        &self,
        position: &KafkaOffsetPosition,
    ) -> Result<u64, Report<StateAuthorityError>> {
        let state = &self.read.state;
        let moved =
            state
                .assignment
                .authorize(self.assignment, StateCapability::Originate, || {
                    state.move_recorded_offset(&position.topic, position.partition, position.offset)
                })?;
        if let Some(lsm) = moved {
            return Ok(lsm);
        }
        state
            .assignment
            .authorize_exclusive(self.assignment, StateCapability::Originate, || {
                state.record_first_offset(&position.topic, position.partition, position.offset)
            })
    }
}

impl KafkaOffsetSnapshotInstaller {
    pub(super) fn read(&self) -> &KafkaOffsetStateRead {
        &self.read
    }

    /// Report the installed revision only while this assignment still grants replica authority.
    /// A poll reuses this report when no transfer is needed, so a lost acknowledgement is retried.
    pub(super) fn acknowledged_revision(&self) -> error_stack::Result<u64, StateReplicationError> {
        self.read
            .state
            .assignment
            .authorize(self.assignment, StateCapability::InstallSnapshot, || {
                self.read.current_lsm()
            })
            .change_context(StateReplicationError::Capture {
                placement: self.read.placement().clone(),
            })
    }

    pub(super) fn install_cancellable_snapshot(
        &self,
        lsm: u64,
        payload: &[u8],
        cancellation: &nervix_execution::Cancellation,
    ) -> error_stack::Result<(), StateReplicationError> {
        self.install_checked_snapshot(lsm, payload, || {
            cancellation
                .check()
                .change_context(RuntimePersistenceError::Cancelled)
        })
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies the cancellation check between native checkpoint entries"
        )
    )]
    fn install_checked_snapshot(
        &self,
        lsm: u64,
        payload: &[u8],
        mut check: impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<(), StateReplicationError> {
        let table = KafkaOffsetTable::decode_with(payload, &mut check).change_context(
            StateReplicationError::Capture {
                placement: self.read.placement().clone(),
            },
        )?;
        check().change_context(StateReplicationError::Capture {
            placement: self.read.placement().clone(),
        })?;
        self.install_table(lsm, table)
            .change_context(StateReplicationError::Capture {
                placement: self.read.placement().clone(),
            })
    }

    fn install_table(
        &self,
        lsm: u64,
        table: KafkaOffsetTable,
    ) -> error_stack::Result<(), RuntimeStateOperationError> {
        let state = &self.read.state;
        state
            .assignment
            .authorize_exclusive(self.assignment, StateCapability::InstallSnapshot, || {
                state.offsets.store(StdArc::new(table));
                state.current_lsm.adopt(lsm);
            })
            .change_context(RuntimeStateOperationError::Authority)
    }
}

impl KafkaOffsetStatePersistence {
    pub(super) fn read(&self) -> &KafkaOffsetStateRead {
        &self.read
    }

    /// Whether this state holds a revision it has not persisted yet.
    pub(super) fn is_dirty(&self) -> bool {
        self.read.state.current_lsm.current()
            > self.read.state.last_persisted_lsm.load(Ordering::SeqCst)
    }

    pub(super) fn last_persisted_lsm(&self) -> u64 {
        self.read.state.last_persisted_lsm.load(Ordering::SeqCst)
    }

    pub(super) fn record_persisted(&self, lsm: u64) {
        self.read
            .state
            .last_persisted_lsm
            .fetch_max(lsm, Ordering::SeqCst);
    }
}

impl KafkaOffsetTable {
    /// A later initialization may have observed additional partitions while resolving the start.
    /// Keep every recorded slot, and install only starts the current table does not yet contain.
    fn record_unrecorded_starts(&mut self, positions: Vec<KafkaOffsetPosition>) -> bool {
        let mut changed = false;
        for position in positions {
            if self.slot(&position.topic, position.partition).is_none() {
                self.record_partition(&position.topic, position.partition, position.offset);
                changed = true;
            }
        }
        changed
    }

    /// A table recording `offsets`, each partition in a slot of its own.
    fn from_offsets(offsets: Vec<KafkaOffsetPosition>) -> Self {
        let mut table = Self {
            applied_start_generation: None,
            topics: HashMap::default(),
        };
        for position in offsets {
            table.record_partition(&position.topic, position.partition, position.offset);
        }
        table
    }

    fn slot(&self, topic: &str, partition: i32) -> Option<&AtomicI64> {
        let partitions = self.topics.get(topic)?;
        let slot: &AtomicI64 = partitions.get(&partition)?;
        Some(slot)
    }

    fn next_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let slot = self.slot(topic, partition)?;
        Some(slot.load(Ordering::SeqCst))
    }

    /// Record `partition` of `topic` at `next_offset` in a new slot.
    fn record_partition(&mut self, topic: &str, partition: i32, next_offset: i64) {
        let slot = Arc::new(AtomicI64::new(next_offset));
        match self.topics.get_mut(topic) {
            Some(partitions) => {
                partitions.insert(partition, slot);
            }
            None => {
                self.topics
                    .insert(topic.to_string(), BTreeMap::from([(partition, slot)]));
            }
        }
    }

    fn encode(&self) -> error_stack::Result<Vec<u8>, RuntimePersistenceError> {
        self.encode_with(|| Ok(()))
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies cancellation between bounded native entries"
        )
    )]
    fn encode_with(
        &self,
        mut check: impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<Vec<u8>, RuntimePersistenceError> {
        check()?;
        let mut entries = Vec::new();
        for (topic, partitions) in &self.topics {
            for (partition, slot) in partitions {
                check()?;
                entries.push(KafkaOffsetEntrySnapshot {
                    topic: topic.clone(),
                    partition: *partition,
                    next_offset: slot.load(Ordering::SeqCst),
                });
            }
        }
        entries.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then(left.partition.cmp(&right.partition))
        });
        rkyv::to_bytes::<rkyv::rancor::Error>(&KafkaOffsetSnapshot {
            applied_start_generation: self.applied_start_generation,
            offsets: entries,
            format: KAFKA_OFFSET_FORMAT,
        })
        .map(|bytes| bytes.to_vec())
        .change_context(RuntimePersistenceError::EncodeState)
    }

    fn decode(payload: &[u8]) -> error_stack::Result<Self, RuntimePersistenceError> {
        Self::decode_with(payload, || Ok(()))
    }

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the caller supplies the cancellation check between bounded native entries"
        )
    )]
    fn decode_with(
        payload: &[u8],
        mut check: impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<Self, RuntimePersistenceError> {
        check()?;
        let snapshot = access_offset_snapshot(payload)?;
        let mut table = Self {
            applied_start_generation: snapshot
                .applied_start_generation
                .as_ref()
                .map(|value| value.to_native()),
            ..Self::default()
        };
        for entry in snapshot.offsets.iter() {
            check()?;
            table.record_partition(
                entry.topic.as_str(),
                entry.partition.to_native(),
                entry.next_offset.to_native(),
            );
        }
        Ok(table)
    }
}

/// Convert the internal offset checkpoint into typed partition positions, decoded whole for
/// inspection and properties that compare complete values. The scheduling cache is recomputed at
/// START and is not among them.
#[cfg(any(test, feature = "testing"))]
pub(in crate::runtime) fn backup_offset_positions(
    payload: &[u8],
) -> error_stack::Result<Vec<(String, i32, i64)>, RuntimePersistenceError> {
    let table = KafkaOffsetTable::decode(payload)?;
    let mut offsets = Vec::new();
    for (topic, partitions) in table.topics {
        for (partition, slot) in partitions {
            offsets.push((topic.clone(), partition, slot.load(Ordering::SeqCst)));
        }
    }
    offsets.sort_by(|left, right| (&left.0, left.1).cmp(&(&right.0, right.1)));
    Ok(offsets)
}

/// A stored Kafka offset checkpoint validated once in its aligned allocation, beside the order its
/// partitions are archived in. A partition the checkpoint records twice keeps its later record, as
/// decoding the checkpoint into a table keeps it.
pub(crate) struct NativeKafkaOffsets {
    payload: rkyv::util::AlignedVec<16>,
    /// One index into the archived offsets for every partition, in topic and partition order.
    order: Vec<u32>,
}

// The order holds at most one index per archived entry, and an archived entry is never smaller
// than an index, so the order never takes more memory than the payload it indexes.
const _: () =
    assert!(std::mem::size_of::<ArchivedKafkaOffsetEntrySnapshot>() >= std::mem::size_of::<u32>());

impl NativeKafkaOffsets {
    pub(super) fn applied_start_generation(&self) -> Option<u64> {
        let snapshot = access_offset_snapshot(&self.payload)
            .verified("the checkpoint was validated and its bytes stay immutable");
        snapshot
            .applied_start_generation
            .as_ref()
            .map(|value| value.to_native())
    }

    /// Validates the archived checkpoint and orders its partitions, running `check` before each.
    pub(super) fn validate(
        payload: rkyv::util::AlignedVec<16>,
        mut check: impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<Self, RuntimePersistenceError> {
        let snapshot = access_offset_snapshot(&payload)?;
        let offsets = &snapshot.offsets;
        let count = u32::try_from(offsets.len())
            .verified("an archived vector counts its entries in 32 bits");
        let mut order = Vec::with_capacity(offsets.len());
        for index in 0..count {
            check()?;
            order.push(index);
        }
        // A partition recorded twice sorts its later record first, and the deduplication below
        // keeps the first of each run.
        order.sort_unstable_by(|left, right| {
            let left_key = offsets[left.arch_into()].partition_key();
            let right_key = offsets[right.arch_into()].partition_key();
            left_key.cmp(&right_key).then(right.cmp(left))
        });
        order.dedup_by(|current, kept| {
            offsets[current.arch_into()].partition_key()
                == offsets[kept.arch_into()].partition_key()
        });
        Ok(Self { payload, order })
    }

    /// Hands `visit` every partition's position in topic and partition order, through an
    /// iterator it may clone and walk again.
    pub(crate) fn with_positions<R>(&self, visit: impl FnOnce(KafkaOffsetPositions<'_>) -> R) -> R {
        let snapshot = access_offset_snapshot(&self.payload)
            .verified("the offsets were validated when they were read and have not changed since");
        visit(KafkaOffsetPositions {
            offsets: &snapshot.offsets,
            order: self.order.iter(),
        })
    }
}

fn access_offset_snapshot(
    payload: &[u8],
) -> error_stack::Result<&ArchivedKafkaOffsetSnapshot, RuntimePersistenceError> {
    let snapshot = rkyv::access::<ArchivedKafkaOffsetSnapshot, rkyv::rancor::Error>(payload)
        .change_context(RuntimePersistenceError::DecodeState)?;
    if snapshot.format != KAFKA_OFFSET_FORMAT {
        return Err(Report::new(KafkaOffsetCheckpointError::FormatSignature)
            .change_context(RuntimePersistenceError::DecodeState));
    }
    Ok(snapshot)
}

impl ArchivedKafkaOffsetEntrySnapshot {
    /// The topic and partition the entry records, which order an archive's offsets.
    fn partition_key(&self) -> (&str, i32) {
        (self.topic.as_str(), self.partition.to_native())
    }
}

/// The partitions of a validated offset checkpoint in topic and partition order, each converted
/// when it is reached.
#[derive(Clone)]
pub(crate) struct KafkaOffsetPositions<'a> {
    offsets: &'a rkyv::vec::ArchivedVec<ArchivedKafkaOffsetEntrySnapshot>,
    order: std::slice::Iter<'a, u32>,
}

impl Iterator for KafkaOffsetPositions<'_> {
    type Item = BackupKafkaPartitionOffset;

    fn next(&mut self) -> Option<Self::Item> {
        let index = *self.order.next()?;
        let entry = &self.offsets[index.arch_into()];
        Some(BackupKafkaPartitionOffset {
            topic: entry.topic.as_str().to_owned(),
            partition: entry.partition.to_native(),
            next_offset: entry.next_offset.to_native(),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.order.size_hint()
    }
}

impl ExactSizeIterator for KafkaOffsetPositions<'_> {
    fn len(&self) -> usize {
        self.order.len()
    }
}

/// A serialization view of the current archived offset root. Restore carries positions only;
/// topic scheduling is established by the restored execution plan.
#[derive(Archive, RkyvSerialize)]
#[rkyv(as = ArchivedKafkaOffsetSnapshot)]
#[rkyv(serialize_bounds(__S: rkyv::ser::Writer + rkyv::ser::Allocator, __S::Error: rkyv::rancor::Source))]
struct StreamingKafkaOffsetSnapshot<
    'a,
    I: ExactSizeIterator<Item = KafkaOffsetEntrySnapshot> + Clone,
> {
    applied_start_generation: Option<u64>,
    #[rkyv(with = super::native_checkpoint_encoding::IteratorAsVec<super::native_checkpoint_encoding::CancellableEntry<'a, KafkaOffsetEntrySnapshot>>, omit_bounds)]
    offsets: super::native_checkpoint_encoding::CancellableIterator<'a, I>,
    format: [u8; 16],
}

pub(in crate::runtime) fn write_offset_payload(
    applied_start_generation: Option<u64>,
    offsets: impl ExactSizeIterator<Item = (String, i32, i64)> + Clone,
    writer: &mut dyn std::io::Write,
    cancellation: &nervix_execution::Cancellation,
) -> error_stack::Result<(), RuntimePersistenceError> {
    write_offset_snapshot(applied_start_generation, offsets, writer, cancellation)
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "the exact-size caller iterator supplies checkpoint positions; its local bodies \
                  remain checked and the serializer owns admitted scratch and cancellation"
    )
)]
fn write_offset_snapshot(
    applied_start_generation: Option<u64>,
    offsets: impl ExactSizeIterator<Item = (String, i32, i64)> + Clone,
    writer: &mut dyn std::io::Write,
    cancellation: &nervix_execution::Cancellation,
) -> error_stack::Result<(), RuntimePersistenceError> {
    let capacity = super::native_checkpoint_encoding::scratch_bytes::<KafkaOffsetEntrySnapshot>(
        offsets.len(),
        0,
    )
    .ok_or_else(|| {
        Report::new(RuntimePersistenceError::NativeEncoding {
            state: RuntimeStateKind::KafkaOffset,
        })
    })?;
    let mut scratch = vec![std::mem::MaybeUninit::uninit(); capacity];
    let snapshot = StreamingKafkaOffsetSnapshot {
        applied_start_generation,
        offsets: super::native_checkpoint_encoding::CancellableIterator {
            cancellation,
            entries: offsets.map(|(topic, partition, next_offset)| KafkaOffsetEntrySnapshot {
                topic,
                partition,
                next_offset,
            }),
        },
        format: KAFKA_OFFSET_FORMAT,
    };
    {
        let mapping_input = rkyv::api::low::to_bytes_in_with_alloc::<_, _, rkyv::rancor::Error>(
            &snapshot,
            rkyv::ser::writer::IoWriter::new(writer),
            rkyv::ser::allocator::SubAllocator::new(&mut scratch),
        )
        .map(|_| ());
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err(Report::new(error).change_context(
                RuntimePersistenceError::NativeEncoding {
                    state: RuntimeStateKind::KafkaOffset,
                },
            )),
        }
    }
}

impl CapturedKafkaOffsets {
    /// Encode exactly the retained partition topology used to calculate the allocation bound.
    pub(super) fn snapshot(
        &self,
        check: impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        Ok(PersistedRuntimeStateEntry {
            lsm: self.lsm,
            payload: self.table.encode_with(check)?,
        })
    }

    /// A pending unit's start positions are immutable while unit zero commits its own slice.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "the admitted encoder checks caller cancellation between retained entries"
        )
    )]
    pub(super) fn start_seed(
        &self,
        generation: u64,
        selection: nervix_interconnect::IngestorUnitSelection,
        mut check: impl FnMut() -> error_stack::Result<(), RuntimePersistenceError>,
    ) -> error_stack::Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        let nervix_models::Unit::Instance(index) = selection.unit else {
            return Err(Report::new(KafkaOffsetCheckpointError::InstanceSelection)
                .change_context(RuntimePersistenceError::DecodeState));
        };
        if index == 0 || index >= selection.count.get() {
            return Err(Report::new(KafkaOffsetCheckpointError::InstanceSelection)
                .change_context(RuntimePersistenceError::DecodeState));
        }
        let current = &self.table;
        if current.applied_start_generation != Some(generation) {
            return Err(Report::new(KafkaOffsetCheckpointError::StartGeneration {
                expected: generation,
                observed: current.applied_start_generation,
            })
            .change_context(RuntimePersistenceError::DecodeState));
        }
        let mut seed = KafkaOffsetTable {
            applied_start_generation: Some(generation),
            ..KafkaOffsetTable::default()
        };
        for (topic, partitions) in &current.topics {
            for (partition, offset) in partitions {
                check()?;
                if u64::from(partition.cast_unsigned()) % selection.count.get() == index {
                    seed.record_partition(topic, *partition, offset.load(Ordering::SeqCst));
                }
            }
        }
        Ok(PersistedRuntimeStateEntry {
            lsm: self.lsm,
            payload: seed.encode_with(check)?,
        })
    }

    /// Reserve a checked upper bound for the artifact and for native serializer scratch. Neither
    /// depends on materializing all partition positions or encoded bytes in memory.
    pub(super) fn bounds(&self) -> error_stack::Result<(u64, u64), RuntimePersistenceError> {
        let overflow = || {
            Report::new(RuntimePersistenceError::NativeEncoding {
                state: RuntimeStateKind::KafkaOffset,
            })
        };
        let mut encoded = 1024_u64;
        let mut scratch = 64 * 1024_u64;
        for (topic, partitions) in &self.table.topics {
            let count = u64::try_from(partitions.len()).map_err(|_| overflow())?;
            let topic_bytes = u64::try_from(topic.len()).map_err(|_| overflow())?;
            encoded = encoded
                .checked_add(
                    count
                        .checked_mul(topic_bytes.checked_add(128).ok_or_else(overflow)?)
                        .ok_or_else(overflow)?,
                )
                .ok_or_else(overflow)?;
            scratch = scratch
                .checked_add(count.checked_mul(128).ok_or_else(overflow)?)
                .ok_or_else(overflow)?;
        }
        Ok((encoded, scratch))
    }

    pub(super) fn write(
        &self,
        writer: &mut dyn std::io::Write,
        cancellation: &nervix_execution::Cancellation,
    ) -> error_stack::Result<(), RuntimePersistenceError> {
        let remaining = self.table.topics.values().map(BTreeMap::len).sum();
        let offsets = KafkaOffsetEntries {
            topics: self.table.topics.iter(),
            partitions: None,
            remaining,
        };
        write_offset_snapshot(
            self.table.applied_start_generation,
            offsets,
            writer,
            cancellation,
        )
    }
}

#[derive(Clone)]
struct KafkaOffsetEntries<'a> {
    topics: std::collections::hash_map::Iter<'a, String, BTreeMap<i32, Arc<AtomicI64>>>,
    partitions: Option<(
        &'a str,
        std::collections::btree_map::Iter<'a, i32, Arc<AtomicI64>>,
    )>,
    remaining: usize,
}

impl Iterator for KafkaOffsetEntries<'_> {
    type Item = (String, i32, i64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((topic, partitions)) = &mut self.partitions
                && let Some((partition, slot)) = partitions.next()
            {
                self.remaining -= 1;
                return Some(((*topic).to_owned(), *partition, slot.load(Ordering::SeqCst)));
            }
            let (topic, partitions) = self.topics.next()?;
            self.partitions = Some((topic, partitions.iter()));
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for KafkaOffsetEntries<'_> {}

/// What one Kafka offset table records, read out of its slots: every topic's partitions with
/// their next offsets, each in key order.
#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
struct KafkaOffsetView {
    applied_start_generation: Option<u64>,
    offsets: BTreeMap<String, BTreeMap<i32, i64>>,
}

#[cfg(test)]
impl KafkaOffsetTable {
    fn view(&self) -> KafkaOffsetView {
        let mut offsets = BTreeMap::new();
        for (topic, partitions) in &self.topics {
            let mut positions = BTreeMap::new();
            for (partition, slot) in partitions {
                positions.insert(*partition, slot.load(Ordering::SeqCst));
            }
            offsets.insert(topic.clone(), positions);
        }
        KafkaOffsetView {
            applied_start_generation: self.applied_start_generation,
            offsets,
        }
    }

    /// Up to three recorded partitions of up to three topics at any offset.
    fn generated(arbitrary: &mut nervix_arbitrary::Arbitrary<'_>) -> Self {
        let mut positions = Vec::new();
        for _ in 0..arbitrary.entropy().count(3) {
            let topic = arbitrary.string();
            for _ in 0..arbitrary.entropy().count(3) {
                positions.push(KafkaOffsetPosition {
                    topic: topic.clone(),
                    partition: generated_partition(arbitrary),
                    offset: arbitrary.entropy().any_i64(),
                });
            }
        }
        let mut table = Self::from_offsets(positions);
        if arbitrary.entropy().byte() % 2 == 0 {
            table.applied_start_generation = Some(arbitrary.entropy().any_u64());
        }
        table
    }
}

/// A Kafka partition number, landing on zero and the extremes as often as elsewhere.
#[cfg(test)]
fn generated_partition(arbitrary: &mut nervix_arbitrary::Arbitrary<'_>) -> i32 {
    use meticulous::ResultExt as _;

    match arbitrary.entropy().byte() % 4 {
        0 => 0,
        1 => i32::MIN,
        2 => i32::MAX,
        _ => {
            let bits = u32::try_from(arbitrary.entropy().up_to(u64::from(u32::MAX)))
                .verified("the draw ends at u32::MAX");
            bits.cast_signed()
        }
    }
}

/// A generated offset table stores through `stored`, the checkpoint envelope a node keeps it in,
/// and restores every partition offset and the applied start generation.
#[cfg(test)]
pub(in crate::runtime) fn assert_generated_offsets_survive(
    arbitrary: &mut nervix_arbitrary::Arbitrary<'_>,
    stored: impl FnOnce(Vec<u8>) -> Vec<u8>,
) {
    use meticulous::ResultExt as _;

    let table = KafkaOffsetTable::generated(arbitrary);
    let payload = table
        .encode()
        .assured("a bounded generated offset table encodes");
    let restored = KafkaOffsetTable::decode(&stored(payload))
        .assured("a stored offset table decodes from its own encoding");
    assert_eq!(restored.view(), table.view());
}

/// Arbitrary bytes read as a stored offset table either fail with the typed decode failure or
/// restore a table that stores back unchanged.
#[cfg(test)]
pub(in crate::runtime) fn assert_offset_payload_decodes_typed(payload: &[u8]) {
    use meticulous::ResultExt as _;

    match KafkaOffsetTable::decode(payload) {
        Ok(table) => {
            let encoded = table.encode().assured("a decoded offset table encodes");
            let again =
                KafkaOffsetTable::decode(&encoded).assured("a re-encoded offset table decodes");
            assert_eq!(again.view(), table.view());
        }
        Err(error) => assert!(
            matches!(
                error.current_context(),
                RuntimePersistenceError::DecodeState
            ),
            "{error:?}"
        ),
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_arbitrary::{Arbitrary, Domain};
    use nervix_models::{ClusterNodeName, DomainName, ModelKind, ModelName};
    use nervix_primitives::sync::{Arc, oneshot};

    use super::*;
    use crate::runtime::{RuntimeState, StateReplicationRoles, native_checkpoint_properties};

    fn aligned(payload: &[u8]) -> rkyv::util::AlignedVec<16> {
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
        aligned.extend_from_slice(payload);
        aligned
    }

    fn entry(topic: &str, partition: i32, next_offset: i64) -> KafkaOffsetEntrySnapshot {
        KafkaOffsetEntrySnapshot {
            topic: topic.to_string(),
            partition,
            next_offset,
        }
    }

    fn position(topic: &str, partition: i32, next_offset: i64) -> BackupKafkaPartitionOffset {
        BackupKafkaPartitionOffset {
            topic: topic.to_string(),
            partition,
            next_offset,
        }
    }

    #[test]
    fn native_offsets_walk_partitions_in_order_and_keep_a_partition_s_later_record() {
        let snapshot = KafkaOffsetSnapshot {
            applied_start_generation: Some(0),
            offsets: vec![
                entry("beta", 1, 10),
                entry("alpha", 2, 20),
                entry("beta", 0, 30),
                entry("alpha", 2, 40),
                entry("alpha", 10, 50),
            ],
            format: KAFKA_OFFSET_FORMAT,
        };
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot)
            .assured("the unordered current shape encodes")
            .to_vec();
        let offsets = NativeKafkaOffsets::validate(aligned(&payload), || Ok(()))
            .assured("a current offset checkpoint validates");
        let walked = offsets.with_positions(|positions| {
            assert_eq!(positions.len(), 4, "every partition is walked once");
            assert_eq!(positions.clone().count(), 4, "a clone walks them again");
            positions.collect::<Vec<_>>()
        });
        let expected = vec![
            position("alpha", 2, 40),
            position("alpha", 10, 50),
            position("beta", 0, 30),
            position("beta", 1, 10),
        ];
        assert_eq!(walked, expected);
        let decoded = backup_offset_positions(&payload)
            .assured("the table decoding reads the same checkpoint");
        let mut tabled = Vec::new();
        for (topic, partition, next_offset) in decoded {
            tabled.push(BackupKafkaPartitionOffset {
                topic,
                partition,
                next_offset,
            });
        }
        assert_eq!(tabled, expected, "the walk agrees with the table decoding");
    }

    #[test]
    fn validating_native_offsets_checks_between_entries_and_refuses_foreign_bytes() {
        let snapshot = KafkaOffsetSnapshot {
            applied_start_generation: Some(u64::MAX),
            offsets: vec![
                entry("alpha", 0, 1),
                entry("alpha", 1, 2),
                entry("alpha", 2, 3),
            ],
            format: KAFKA_OFFSET_FORMAT,
        };
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&snapshot)
            .assured("the current shape encodes")
            .to_vec();
        let mut checks = 0;
        let failure = NativeKafkaOffsets::validate(aligned(&payload), || {
            checks += 1;
            if checks > 2 {
                return Err(Report::new(RuntimePersistenceError::Cancelled));
            }
            Ok(())
        })
        .err()
        .assured("the third check stops validation");
        assert!(matches!(
            failure.current_context(),
            RuntimePersistenceError::Cancelled
        ));
        let failure = NativeKafkaOffsets::validate(aligned(b"not an offset checkpoint"), || Ok(()))
            .err()
            .assured("foreign bytes are refused");
        assert!(matches!(
            failure.current_context(),
            RuntimePersistenceError::DecodeState
        ));
    }

    #[test]
    fn a_checkpoint_requires_its_current_format_signature() {
        let mut format = KAFKA_OFFSET_FORMAT;
        format[0] ^= 1;
        let checkpoint = KafkaOffsetSnapshot {
            applied_start_generation: Some(5),
            offsets: vec![entry("events", 0, 9)],
            format,
        };
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
            .assured("the current checkpoint shape encodes");
        let failure = KafkaOffsetTable::decode(&payload)
            .err()
            .assured("a corrupt current format signature is refused");
        assert!(failure.contains::<KafkaOffsetCheckpointError>());
        assert!(matches!(
            failure.current_context(),
            RuntimePersistenceError::DecodeState
        ));
        let captured = NativeKafkaOffsets::validate(aligned(&payload), || Ok(()))
            .err()
            .assured("capture uses the same format validation");
        assert!(captured.contains::<KafkaOffsetCheckpointError>());
    }

    #[test]
    fn a_current_kafka_replica_recovers_a_lost_progress_report_without_another_snapshot() {
        let owner = ClusterNodeName::parse("node-1").assured("the owner name is valid");
        let replica = ClusterNodeName::parse("node-2").assured("the replica name is valid");
        let roles = StateReplicationRoles::new(Some(owner.clone()), vec![replica.clone()], 1);
        let primary = Arc::new(
            ReplicatedKafkaOffsetState::new(offset_placement(), None)
                .assured("an empty primary initializes"),
        );
        let originator = ReplicatedKafkaOffsetState::bind(&primary, roles.clone(), Some(&owner))
            .originator
            .assured("the primary may commit offsets");
        let revision = originator
            .apply_committed_offset(&KafkaOffsetPosition {
                topic: "events".into(),
                partition: 0,
                offset: 9,
            })
            .assured("the first offset is committed");
        let snapshot = originator
            .read()
            .latest_snapshot()
            .assured("the committed checkpoint captures");
        let secondary = Arc::new(
            ReplicatedKafkaOffsetState::new(offset_placement(), Some(snapshot))
                .assured("the replica already holds the checkpoint"),
        );
        let installer = ReplicatedKafkaOffsetState::bind(&secondary, roles, Some(&replica))
            .installer
            .assured("the secondary retains replica authority");
        let first_report = installer
            .acknowledged_revision()
            .assured("the installed replica can report progress");
        assert_eq!(first_report, revision);
        // The first report is lost before the primary receives it. A later poll asks for only
        // changes, so the primary has no checkpoint to send back.
        assert!(!originator.read().replica_quorum_holds(revision));
        assert!(
            originator
                .read()
                .capture_after(Some(installer.read().current_lsm()))
                .is_none()
        );
        let repeated_report = installer
            .acknowledged_revision()
            .assured("an unchanged replica repeats its held revision");
        originator
            .read()
            .replication()
            .record(&replica, repeated_report);
        assert!(originator.read().replica_quorum_holds(revision));
        assert_eq!(installer.read().next_offset("events", 0), Some(9));
    }

    #[test]
    fn a_reissued_originator_commits_after_a_replica_change_that_fences_the_one_it_replaced() {
        let owner = ClusterNodeName::parse("node-1").assured("the owner name is valid");
        let lost = ClusterNodeName::parse("node-2").assured("the lost replica name is valid");
        let promoted = ClusterNodeName::parse("node-3").assured("the new replica name is valid");
        let position = |offset| KafkaOffsetPosition {
            topic: "events".into(),
            partition: 0,
            offset,
        };
        let state = Arc::new(
            ReplicatedKafkaOffsetState::new(offset_placement(), None)
                .assured("an empty primary initializes"),
        );
        let started = ReplicatedKafkaOffsetState::bind(
            &state,
            StateReplicationRoles::new(Some(owner.clone()), vec![lost], 0),
            Some(&owner),
        )
        .originator
        .assured("the primary may commit offsets");
        let slot = KafkaOffsetOriginatorSlot::new(started.clone());
        slot.current()
            .apply_committed_offset(&position(4))
            .assured("the ingestor commits under the binding it started with");

        let reassigned = ReplicatedKafkaOffsetState::bind(
            &state,
            StateReplicationRoles::new(Some(owner.clone()), vec![promoted], 0),
            Some(&owner),
        )
        .originator
        .assured("the primary still originates its offsets");
        assert!(
            started.apply_committed_offset(&position(5)).is_err(),
            "the replica change superseded the binding the ingestor started with"
        );
        slot.replace(reassigned);
        slot.current()
            .apply_committed_offset(&position(6))
            .assured("the kept ingestor commits under the binding the reassignment issued");
        assert_eq!(
            ReplicatedKafkaOffsetState::read(&state).next_offset("events", 0),
            Some(6)
        );
    }

    #[test]
    fn cancelled_or_invalid_kafka_conversion_keeps_the_installed_checkpoint() {
        let payload = |positions| {
            KafkaOffsetTable::from_offsets(positions)
                .encode()
                .assured("current positions encode")
        };
        let position = |partition, offset| KafkaOffsetPosition {
            topic: "events".into(),
            partition,
            offset,
        };
        let state = Arc::new(
            ReplicatedKafkaOffsetState::new(
                offset_placement(),
                Some(PersistedRuntimeStateEntry {
                    lsm: 7,
                    payload: payload(vec![position(0, 9)]),
                }),
            )
            .assured("the initial replica checkpoint loads"),
        );
        let owner = ClusterNodeName::parse("node-1").assured("the owner name is valid");
        let replica = ClusterNodeName::parse("node-2").assured("the replica name is valid");
        let installer = ReplicatedKafkaOffsetState::bind(
            &state,
            StateReplicationRoles::new(Some(owner), vec![replica.clone()], 1),
            Some(&replica),
        )
        .installer
        .assured("the replica may install snapshots");
        let next = payload(vec![position(0, 10), position(1, 11)]);
        for cancel_at in [3, 4] {
            let mut checked = 0;
            let cancelled = installer
                .install_checked_snapshot(8, &next, || {
                    checked += 1;
                    if checked == cancel_at {
                        return Err(Report::new(RuntimePersistenceError::Cancelled));
                    }
                    Ok(())
                })
                .err()
                .assured("conversion is cancelled before publishing any positions");
            assert!(cancelled.contains::<RuntimePersistenceError>());
            assert_eq!(checked, cancel_at);
            assert_eq!(installer.read().current_lsm(), 7);
            assert_eq!(installer.read().next_offset("events", 0), Some(9));
            assert_eq!(installer.read().next_offset("events", 1), None);
        }
        let invalid = installer
            .install_checked_snapshot(8, &next[..next.len() - 1], || Ok(()))
            .err()
            .assured("a truncated current checkpoint is refused");
        assert!(matches!(
            invalid.downcast_ref::<RuntimePersistenceError>(),
            Some(RuntimePersistenceError::DecodeState)
        ));
        assert!(invalid.contains::<rkyv::rancor::Error>());
        let invalid_load = ReplicatedKafkaOffsetState::new(
            offset_placement(),
            Some(PersistedRuntimeStateEntry {
                lsm: 8,
                payload: next[..next.len() - 1].to_vec(),
            }),
        )
        .err()
        .assured("loading a truncated current checkpoint preserves its decode failure");
        assert!(matches!(
            invalid_load.current_context(),
            RuntimePersistenceError::DecodeState
        ));
        assert!(invalid_load.contains::<rkyv::rancor::Error>());
        assert_eq!(installer.read().current_lsm(), 7);
        assert_eq!(installer.read().next_offset("events", 0), Some(9));
        assert_eq!(installer.read().next_offset("events", 1), None);
    }

    #[nervix_primitives::test]
    async fn captured_kafka_offsets_stream_beyond_bulk_memory_with_every_position() {
        use nervix_execution::{ExecutionConfig, Executor, MemoryClass};

        use crate::runtime::snapshot_staging::{SnapshotStaging, SnapshotStagingLimits};

        let topic = format!("events-{}", "x".repeat(240));
        let positions = (0..4096)
            .map(|partition| KafkaOffsetPosition {
                topic: topic.clone(),
                partition,
                offset: i64::from(partition) + 17,
            })
            .chain([KafkaOffsetPosition {
                topic: "other".into(),
                partition: 7,
                offset: 99,
            }])
            .collect();
        let state = Arc::new(
            ReplicatedKafkaOffsetState::new(
                offset_placement(),
                Some(PersistedRuntimeStateEntry {
                    lsm: 7,
                    payload: KafkaOffsetTable::from_offsets(positions)
                        .encode()
                        .assured("the fixture encodes"),
                }),
            )
            .assured("the recorded positions load"),
        );
        let node = ClusterNodeName::parse("node-1").assured("the owner name is valid");
        let originator = ReplicatedKafkaOffsetState::bind(
            &state,
            StateReplicationRoles::new(Some(node.clone()), Vec::new(), 0),
            Some(&node),
        )
        .originator
        .assured("the owner can originate offsets");
        assert!(originator.read().capture_after(Some(7)).is_none());
        originator
            .apply_committed_offset(&KafkaOffsetPosition {
                topic: topic.clone(),
                partition: 0,
                offset: 71,
            })
            .assured("the owner commits a new position");
        let captured = originator
            .read()
            .capture_after(Some(7))
            .assured("the checkpoint advanced");
        let lsm = captured.lsm;
        let (maximum, scratch) = captured.bounds().assured("the native encoding is bounded");
        let mut config = ExecutionConfig::default();
        config.budgets.bulk = ubyte::ByteUnit::Mebibyte(1);
        config.limits.snapshot_section_bytes = ubyte::ByteUnit::Kibibyte(64);
        let executor = Executor::new(config).assured("the small bulk budget is valid");
        let directory = tempfile::tempdir().assured("the staging directory opens");
        let staging = SnapshotStaging::new(
            directory.path().to_path_buf(),
            executor.clone(),
            SnapshotStagingLimits::default(),
        );
        let metadata = executor
            .reserve(MemoryClass::RestoreMetadata, scratch)
            .await
            .assured("serializer scratch is admitted separately");
        let working = executor
            .reserve(MemoryClass::Bulk, 64 * 1024)
            .await
            .assured("bounded I/O is admitted");
        let artifact = staging
            .stage(maximum)
            .await
            .assured("the checkpoint has staging quota")
            .encode_artifact(working, move |output, cancellation| {
                let _metadata = metadata;
                captured
                    .write(output, cancellation)
                    .change_context(crate::runtime::SnapshotStagingError::Encode)
            })
            .await
            .assured("the complete native checkpoint streams to disk");
        assert!(artifact.length() > 1024 * 1024);
        let mut reader = artifact
            .open_reader()
            .await
            .assured("the staged checkpoint opens");
        let mut payload = Vec::new();
        while let Some(chunk) = reader
            .next_chunk(64 * 1024)
            .await
            .assured("one bounded chunk is admitted")
        {
            assert!(chunk.len() <= 64 * 1024);
            payload.extend_from_slice(chunk.as_ref());
        }
        assert_eq!(*blake3::hash(&payload).as_bytes(), artifact.digest());
        let restored = Arc::new(
            ReplicatedKafkaOffsetState::new(
                offset_placement(),
                Some(PersistedRuntimeStateEntry { lsm, payload }),
            )
            .assured("the streamed current checkpoint loads"),
        );
        let read = ReplicatedKafkaOffsetState::read(&restored);
        assert_eq!(read.next_offset("other", 7), Some(99));
        for partition in 0..4096 {
            assert_eq!(
                read.next_offset(&topic, partition),
                Some(if partition == 0 {
                    71
                } else {
                    i64::from(partition) + 17
                })
            );
        }
        assert_eq!(read.current_lsm(), lsm);
        assert!(read.capture_after(Some(lsm)).is_none());
    }

    #[nervix_primitives::test]
    async fn delayed_replica_snapshot_cannot_overwrite_promoted_state() {
        let topic = "events";
        let partition = 0;
        let offset = |next_offset| {
            vec![KafkaOffsetPosition {
                topic: topic.to_string(),
                partition,
                offset: next_offset,
            }]
        };
        let payload = |next_offset| {
            KafkaOffsetTable::from_offsets(offset(next_offset))
                .encode()
                .assured("the in-memory test snapshot contains encodable values")
        };
        let node_1 = ClusterNodeName::parse("node-1")
            .assured("the test node name satisfies the cluster-node grammar");
        let node_2 = ClusterNodeName::parse("node-2")
            .assured("the test node name satisfies the cluster-node grammar");
        let state = Arc::new(
            ReplicatedKafkaOffsetState::new(offset_placement(), None)
                .assured("the empty Kafka offset state is valid"),
        );
        let mut replica_assignment = ReplicatedKafkaOffsetState::bind(
            &state,
            StateReplicationRoles::new(Some(node_1), vec![node_2.clone()], 1),
            Some(&node_2),
        );
        let installer = replica_assignment
            .installer
            .take()
            .assured("node-2 is assigned as the replica");
        installer
            .install_checked_snapshot(1, &payload(2), || Ok(()))
            .assured("the initial replica snapshot is valid");
        assert_eq!(
            installer
                .acknowledged_revision()
                .assured("an unchanged replica reports the revision it holds"),
            1
        );
        assert_eq!(
            installer
                .acknowledged_revision()
                .assured("a lost acknowledgement can be repeated without a transfer"),
            1
        );

        let delayed_installer = installer.clone();
        let delayed_payload = payload(3);
        let (response_received_tx, response_received_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let delayed_install = nervix_primitives::task::spawn(async move {
            let _ = response_received_tx.send(());
            release_rx
                .await
                .assured("the test retains the delayed-response release sender");
            delayed_installer.install_checked_snapshot(2, &delayed_payload, || Ok(()))
        });
        response_received_rx
            .await
            .assured("the delayed replica response task remains alive");

        let mut owner_assignment = ReplicatedKafkaOffsetState::bind(
            &state,
            StateReplicationRoles::new(Some(node_2.clone()), Vec::new(), 0),
            Some(&node_2),
        );
        let originator = owner_assignment
            .originator
            .take()
            .assured("node-2 is assigned as the owner");
        originator
            .apply_committed_offset(&KafkaOffsetPosition {
                topic: topic.to_string(),
                partition,
                offset: 9,
            })
            .assured("the promoted owner assignment is current");
        let refused_acknowledgement = installer
            .acknowledged_revision()
            .err()
            .assured("promotion fences acknowledgements from the prior replica assignment");
        assert!(refused_acknowledgement.contains::<StateAuthorityError>());
        let _ = release_tx.send(());
        let delayed_result = delayed_install
            .await
            .assured("the delayed replica response task joins");

        let rejected = delayed_result
            .err()
            .assured("a superseded replica assignment cannot install its checkpoint");
        assert!(rejected.contains::<StateAuthorityError>());
        assert!(matches!(
            rejected.downcast_ref::<RuntimeStateOperationError>(),
            Some(RuntimeStateOperationError::Authority)
        ));
        assert!(matches!(
            rejected.current_context(),
            StateReplicationError::Capture { .. }
        ));
        assert_eq!(originator.read().next_offset(topic, partition), Some(9));
    }

    /// One check before a native offset checkpoint is read and one before each entry it holds:
    /// every recorded partition.
    fn native_entry_checks(view: &KafkaOffsetView) -> usize {
        let mut checks = 1_usize;
        for partitions in view.offsets.values() {
            checks = checks
                .checked_add(partitions.len())
                .assured("a generated table records at most nine partitions");
        }
        checks
    }

    /// A captured offset table writes a native checkpoint no longer than
    /// the bound its capture reserves. The replica's cancellable reader restores every partition
    /// offset and applied start generation, checks before every native entry, and stops at whichever
    /// check refuses.
    #[test]
    fn bolero_captured_offsets_write_within_their_bound_and_read_back_whole() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(2048)
            .for_each(|bytes: &[u8]| {
                let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
                let table = KafkaOffsetTable::generated(&mut arbitrary);
                let expected = table.view();
                let lsm = arbitrary.entropy().any_u64();
                let payload = table
                    .encode()
                    .assured("a bounded generated offset table encodes");
                let state = Arc::new(
                    ReplicatedKafkaOffsetState::new(
                        offset_placement(),
                        Some(PersistedRuntimeStateEntry { lsm, payload }),
                    )
                    .assured("a stored generated offset table loads"),
                );
                let captured = ReplicatedKafkaOffsetState::read(&state)
                    .capture_after(None)
                    .assured("a capture after no revision retains the table");
                assert_eq!(captured.lsm, lsm);
                let (maximum, _scratch) = captured
                    .bounds()
                    .assured("the checkpoint of a generated table is bounded");
                let snapshot = captured
                    .snapshot(|| Ok(()))
                    .assured("the retained topology encodes as one native snapshot");
                assert_eq!(snapshot.lsm, lsm);
                assert_eq!(
                    KafkaOffsetTable::decode(&snapshot.payload)
                        .assured("the retained snapshot decodes whole")
                        .view(),
                    expected
                );
                assert!(
                    u64::try_from(snapshot.payload.len()).assured("a checkpoint length fits u64")
                        <= maximum
                );
                let checks = native_entry_checks(&expected);
                let refusing = arbitrary.entropy().positive_count(
                    std::num::NonZeroUsize::new(checks)
                        .assured("the encoder checks even an empty table"),
                );
                let mut performed = 0_usize;
                let refused = captured
                    .snapshot(|| {
                        performed += 1;
                        if performed == refusing {
                            return Err(Report::new(RuntimePersistenceError::Cancelled));
                        }
                        Ok(())
                    })
                    .err()
                    .assured("cancellation stops the retained snapshot encoder");
                assert!(matches!(
                    refused.current_context(),
                    RuntimePersistenceError::Cancelled
                ));
                assert_eq!(performed, refusing);
                let written = native_checkpoint_properties::in_admitted_job(move |cancellation| {
                    let mut written = Vec::new();
                    captured
                        .write(&mut written, cancellation)
                        .assured("a captured generated table writes its native checkpoint");
                    written
                });
                let written_bytes =
                    u64::try_from(written.len()).assured("a checkpoint length fits u64");
                assert!(
                    written_bytes <= maximum,
                    "{written_bytes} written bytes exceed the reserved {maximum}"
                );

                let checks = native_entry_checks(&expected);
                let mut performed = 0_usize;
                let restored = KafkaOffsetTable::decode_with(&written, || {
                    performed += 1;
                    Ok(())
                })
                .assured("a written native checkpoint reads back");
                assert_eq!(restored.view(), expected);
                assert_eq!(performed, checks);

                let refusing = arbitrary.entropy().positive_count(
                    std::num::NonZeroUsize::new(checks)
                        .assured("a reader checks once before it reads any entry"),
                );
                let mut performed = 0_usize;
                let refused = KafkaOffsetTable::decode_with(&written, || {
                    performed += 1;
                    if performed == refusing {
                        return Err(Report::new(RuntimePersistenceError::Cancelled));
                    }
                    Ok(())
                })
                .err()
                .assured("a refused check stops the read");
                assert!(matches!(
                    refused.current_context(),
                    RuntimePersistenceError::Cancelled
                ));
                assert_eq!(performed, refusing);
            });
    }

    fn offset_placement() -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse("default")
                .assured("the test domain name satisfies the domain grammar"),
            state: RuntimeState::KafkaOffset,
            kind: ModelKind::Ingestor,
            identifier: ModelName::parse("source")
                .assured("the test model name satisfies the model-name grammar"),
            unit: nervix_models::Unit::Single,
            branch_key: None,
        }
    }

    #[test]
    fn bolero_unit_layouts_preserve_canonical_starts_and_committed_positions() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(512)
            .for_each(|bytes: &[u8]| {
                let bytes = bytes.to_vec();
                native_checkpoint_properties::in_admitted_job(move |cancellation| {
                    let mut arbitrary = Arbitrary::new(&bytes, Domain::Vocabulary);
                    let count =
                        std::num::NonZeroU64::new(arbitrary.entropy().boundary_biased(1..=5))
                            .assured("the count is positive");
                    let generation = arbitrary.entropy().boundary_biased(2..=u64::MAX);
                    let partition_count = arbitrary.entropy().boundary_biased(1..=12);
                    let mut canonical = Vec::new();
                    let mut expected = BTreeMap::new();
                    for topic in ["first", "second"] {
                        let mut positions = BTreeMap::new();
                        for partition in 0..partition_count {
                            let partition = i32::try_from(partition)
                                .assured("there are at most twelve partitions");
                            canonical.push(KafkaOffsetPosition {
                                topic: topic.to_owned(),
                                partition,
                                offset: 500,
                            });
                            positions.insert(partition, 500);
                        }
                        expected.insert(topic.to_owned(), positions);
                    }
                    let mut states = Vec::new();
                    for index in 0..count.get() {
                        let mut placement = offset_placement();
                        placement.unit = nervix_models::Unit::Instance(index);
                        let state = Arc::new(
                            ReplicatedKafkaOffsetState::new(placement, None)
                                .assured("an empty instance initializes"),
                        );
                        let originator = ReplicatedKafkaOffsetState::bind(
                            &state,
                            StateReplicationRoles::owned_by(None),
                            None,
                        )
                        .originator
                        .assured("the local owner may originate");
                        states.push(originator);
                    }
                    let candidate = encode_start_candidate(generation, canonical.clone())
                        .assured("a whole candidate encodes");
                    let decoded_candidate =
                        decode_start_positions(&candidate, generation, cancellation)
                            .assured("the candidate decodes whole");
                    assert_eq!(decoded_candidate, canonical);
                    states[0]
                        .apply_canonical_start(generation, decoded_candidate)
                        .assured("unit zero retains the canonical cut");
                    for index in 0..count.get() {
                        let unit =
                            usize::try_from(index).assured("the generated index is at most four");
                        let selection = nervix_interconnect::IngestorUnitSelection {
                            unit: nervix_models::Unit::Instance(index),
                            count,
                        };
                        let initialized = index == 0 || arbitrary.entropy().flag();
                        if index != 0 && initialized {
                            let seed = states[0]
                                .read()
                                .capture_after(None)
                                .assured("the seed retains its partition topology")
                                .start_seed(generation, selection, || {
                                    cancellation
                                        .check()
                                        .change_context(RuntimePersistenceError::Cancelled)
                                })
                                .assured("an unclaimed slice exists");
                            let positions =
                                decode_start_positions(&seed.payload, generation, cancellation)
                                    .assured("every seed position decodes");
                            states[unit]
                                .apply_start(generation, positions)
                                .assured("the unit adopts its whole slice");
                        } else if index != 0 {
                            let earlier = canonical
                                .iter()
                                .filter(|position| {
                                    u64::from(position.partition.cast_unsigned()) % count.get()
                                        == index
                                })
                                .cloned()
                                .collect();
                            states[unit]
                                .apply_start(
                                    generation
                                        .checked_sub(1)
                                        .assured("the generated start is at least two"),
                                    earlier,
                                )
                                .assured("a pending unit may still hold an earlier start");
                        }
                        if initialized {
                            for position in &canonical {
                                if u64::from(position.partition.cast_unsigned()) % count.get()
                                    != index
                                {
                                    continue;
                                }
                                let offset =
                                    i64::try_from(arbitrary.entropy().boundary_biased(0..=1000))
                                        .assured("the generated position is at most one thousand");
                                states[unit]
                                    .apply_committed_offset(&KafkaOffsetPosition {
                                        offset,
                                        ..position.clone()
                                    })
                                    .assured("the owning unit advances its position");
                                expected
                                    .get_mut(&position.topic)
                                    .assured("both generated topics are present")
                                    .insert(position.partition, offset);
                            }
                            if index != 0 && arbitrary.entropy().flag() {
                                states[0]
                                    .acknowledge_start_seed(generation, selection)
                                    .assured("an initialized unit releases only its seeds");
                            }
                        }
                    }
                    let mut cuts = states
                        .iter()
                        .map(|state| IngestorOffsetCheckpoint {
                            unit: state.placement().unit,
                            payload: state
                                .read()
                                .latest_snapshot()
                                .assured("a frozen source captures")
                                .payload,
                        })
                        .collect::<Vec<_>>();
                    let mut source_count = count;
                    for target_count in [arbitrary.entropy().boundary_biased(1..=5), 1, 5, 2] {
                        let target_count = std::num::NonZeroU64::new(target_count)
                            .assured("each generated layout is positive");
                        let mut prepared = Vec::new();
                        let mut complete = BTreeMap::new();
                        for index in 0..target_count.get() {
                            let unit = nervix_models::Unit::Instance(index);
                            let selection = nervix_interconnect::IngestorUnitSelection {
                                unit,
                                count: target_count,
                            };
                            let projected = cuts
                                .iter()
                                .map(|cut| IngestorOffsetCheckpoint {
                                    unit: cut.unit,
                                    payload: project_offset_snapshot(
                                        &cut.payload,
                                        selection,
                                        cancellation,
                                    )
                                    .assured("a frozen source projects"),
                                })
                                .collect::<Vec<_>>();
                            let payload =
                                merge_offset_snapshots(&projected, source_count, cancellation)
                                    .assured("pending seeds and owned progress join");
                            let actual = KafkaOffsetTable::decode(&payload)
                                .assured("the prepared target loads")
                                .view();
                            assert_eq!(actual.applied_start_generation, Some(generation));
                            for (topic, partitions) in actual.offsets {
                                let received = complete.entry(topic).or_insert_with(BTreeMap::new);
                                for (partition, offset) in partitions {
                                    assert_eq!(
                                        u64::from(partition.cast_unsigned()) % target_count.get(),
                                        index
                                    );
                                    assert!(
                                        received.insert(partition, offset).is_none(),
                                        "each position has exactly one target"
                                    );
                                }
                            }
                            prepared.push(IngestorOffsetCheckpoint { unit, payload });
                        }
                        assert_eq!(
                            complete, expected,
                            "all topics and positions survive each shrink and grow"
                        );
                        cuts = prepared;
                        source_count = target_count;
                    }
                });
            });
    }

    #[test]
    fn an_explicit_start_preserves_its_generations_commits_and_survives_checkpoint_loading() {
        let state = Arc::new(
            ReplicatedKafkaOffsetState::new(offset_placement(), None)
                .assured("empty offsets initialize"),
        );
        let originator =
            ReplicatedKafkaOffsetState::bind(&state, StateReplicationRoles::owned_by(None), None)
                .originator
                .assured("the local owner originates offsets");
        let starts = || {
            vec![KafkaOffsetPosition {
                topic: "events".into(),
                partition: 0,
                offset: 0,
            }]
        };
        originator
            .apply_start(3, starts())
            .assured("the first explicit start applies");
        let committed = originator
            .apply_committed_offset(&KafkaOffsetPosition {
                topic: "events".into(),
                partition: 0,
                offset: 9,
            })
            .assured("the owner acknowledges work after its start");
        for generation in [3, 2] {
            let (revision, payload) = originator
                .apply_start(generation, starts())
                .assured("an already applied start retains the complete checkpoint");
            assert_eq!(revision, committed);
            let decoded = KafkaOffsetTable::decode(&payload).assured("the checkpoint decodes");
            assert_eq!(decoded.applied_start_generation, Some(3));
            assert_eq!(decoded.next_offset("events", 0), Some(9));
        }
        let mut expanded = starts();
        expanded.push(KafkaOffsetPosition {
            topic: "events".into(),
            partition: 1,
            offset: 5,
        });
        originator
            .apply_start(3, expanded)
            .assured("a concurrent initializer may have observed a new partition");
        assert_eq!(originator.read().next_offset("events", 0), Some(9));
        assert_eq!(originator.read().next_offset("events", 1), Some(5));
        originator
            .apply_start(4, starts())
            .assured("the next explicit generation resets offsets");
        originator
            .apply_committed_offset(&KafkaOffsetPosition {
                topic: "events".into(),
                partition: 1,
                offset: 5,
            })
            .assured("a new partition can record its timestamp-resolved start");
        let checkpoint = originator
            .read()
            .latest_snapshot()
            .assured("the complete state encodes");
        let restored = Arc::new(
            ReplicatedKafkaOffsetState::new(offset_placement(), Some(checkpoint))
                .assured("the current checkpoint loads"),
        );
        let read = ReplicatedKafkaOffsetState::read(&restored);
        assert_eq!(read.applied_start_generation(), Some(4));
        assert_eq!(read.next_offset("events", 0), Some(0));
        assert_eq!(read.next_offset("events", 1), Some(5));
    }

    /// The memory-ordering claim of a Kafka offset snapshot, explored by Loom over the production
    /// offset state.
    ///
    /// Layer: test harness.
    ///
    /// - **Owns.** The invariant that a snapshot stamped with a commit's revision encodes that
    ///   commit's offset.
    /// - **Depends on.** The production offset state and its revision, and the Loom runner of
    ///   `nervix-model-harness`.
    /// - **Must not know.** Brokers, consumer groups, replicas, or how a snapshot is stored.
    ///
    /// One thread commits an offset of a recorded partition, which never takes the assignment
    /// barrier, while the main thread captures a snapshot under it, so the only synchronization
    /// between them is the offset slot and the revision. `just test-loom-qualification` shows that
    /// weakening the revision's advance makes the model fail.
    #[cfg(feature = "loom")]
    mod loom_models {
        use meticulous::{OptionExt as _, ResultExt as _};
        use nervix_model_harness::{
            InvariantId,
            loom::{explore, spawn},
        };
        use nervix_models::ClusterNodeName;
        use nervix_primitives::sync::Arc;

        use super::{
            super::{KafkaOffsetTable, ReplicatedKafkaOffsetState},
            KafkaOffsetPosition, StateReplicationRoles, offset_placement,
        };

        const SNAPSHOT_REVISION: InvariantId =
            InvariantId::new("runtime.kafka-offset-state.snapshot-revision");

        const TOPIC: &str = "events";

        fn position(offset: i64) -> KafkaOffsetPosition {
            KafkaOffsetPosition {
                topic: TOPIC.to_string(),
                partition: 0,
                offset,
            }
        }

        #[test]
        fn loom_a_snapshot_carrying_a_commits_revision_carries_its_offset() {
            explore(SNAPSHOT_REVISION, || {
                let node = ClusterNodeName::parse("node-1")
                    .assured("the test node name satisfies the cluster-node grammar");
                let state = Arc::new(
                    ReplicatedKafkaOffsetState::new(offset_placement(), None)
                        .assured("the empty Kafka offset state is valid"),
                );
                let mut assignment = ReplicatedKafkaOffsetState::bind(
                    &state,
                    StateReplicationRoles::new(Some(node.clone()), Vec::new(), 0),
                    Some(&node),
                );
                let originator = assignment
                    .originator
                    .take()
                    .assured("node-1 is assigned as the owner");
                originator
                    .apply_committed_offset(&position(1))
                    .assured("the owner assignment is current");
                let read = originator.read().clone();
                let committing = spawn(move || {
                    originator
                        .apply_committed_offset(&position(2))
                        .assured("the owner assignment is current")
                });
                let captured = read
                    .capture_after(None)
                    .assured("the recorded offset topology is retained");
                let snapshot_lsm = captured.lsm;
                let payload = captured
                    .table
                    .encode()
                    .assured("the retained offsets encode outside the assignment barrier");
                let revision = committing
                    .join()
                    .assured("the committing side only commits one offset");
                let encoded = KafkaOffsetTable::decode(&payload)
                    .assured("a snapshot of this state decodes")
                    .next_offset(TOPIC, 0);
                assert!(
                    snapshot_lsm < revision || encoded == Some(2),
                    "a snapshot carried the revision of a commit without its offset"
                );
            });
        }
    }

    #[cfg(feature = "shuttle")]
    mod shuttle_checks {
        #[test]
        fn shuttle_pending_start_seeds_survive_commits_acknowledgements_and_late_candidates() {
            check_interleavings(|| {
                let mut placement = offset_placement();
                placement.unit = nervix_models::Unit::Instance(0);
                let state = Arc::new(
                    ReplicatedKafkaOffsetState::new(placement, None)
                        .assured("an empty coordinator initializes"),
                );
                let root = ReplicatedKafkaOffsetState::bind(
                    &state,
                    StateReplicationRoles::owned_by(None),
                    None,
                )
                .originator
                .assured("the local coordinator originates");
                let positions = [0, 1, 2]
                    .into_iter()
                    .map(|partition| KafkaOffsetPosition {
                        topic: "events".to_owned(),
                        partition,
                        offset: 0,
                    })
                    .collect::<Vec<_>>();
                root.apply_canonical_start(7, positions.clone())
                    .assured("the first canonical cut is installed");
                let count = std::num::NonZeroU64::new(3).assured("the layout has three units");
                let committing = thread::spawn({
                    let root = root.clone();
                    move || {
                        root.apply_committed_offset(&KafkaOffsetPosition {
                            topic: "events".to_owned(),
                            partition: 0,
                            offset: 11,
                        })
                        .assured("unit zero commits its own partition");
                    }
                });
                let adopting = thread::spawn({
                    let root = root.clone();
                    move || {
                        let selection = nervix_interconnect::IngestorUnitSelection {
                            unit: nervix_models::Unit::Instance(1),
                            count,
                        };
                        let seed = root
                            .read()
                            .capture_after(None)
                            .assured("the seed retains its partition topology")
                            .start_seed(7, selection, || Ok(()))
                            .assured("unit one retains its pending cut");
                        let view = KafkaOffsetTable::decode(&seed.payload)
                            .assured("the seed loads")
                            .view();
                        assert_eq!(
                            view.offsets,
                            BTreeMap::from([("events".to_owned(), BTreeMap::from([(1, 0)]))])
                        );
                        root.acknowledge_start_seed(7, selection)
                            .assured("unit one releases only its own seed after adoption");
                    }
                });
                let retrying = thread::spawn({
                    let root = root.clone();
                    move || {
                        let later = positions
                            .into_iter()
                            .map(|position| KafkaOffsetPosition {
                                offset: 99,
                                ..position
                            })
                            .collect();
                        root.apply_canonical_start(7, later)
                            .assured("a late candidate observes the accepted cut");
                    }
                });
                let selection = nervix_interconnect::IngestorUnitSelection {
                    unit: nervix_models::Unit::Instance(2),
                    count,
                };
                let before = root
                    .read()
                    .capture_after(None)
                    .assured("the seed retains its partition topology")
                    .start_seed(7, selection, || Ok(()))
                    .assured("unit two's slice exists");
                thread::yield_now();
                let after = root
                    .read()
                    .capture_after(None)
                    .assured("the seed retains its partition topology")
                    .start_seed(7, selection, || Ok(()))
                    .assured("unit two's slice remains pending");
                assert_eq!(
                    before.payload, after.payload,
                    "unrelated progress and seed acknowledgements never change an unclaimed slice"
                );
                committing
                    .join()
                    .assured("a failing model participant fails its execution");
                adopting
                    .join()
                    .assured("a failing model participant fails its execution");
                retrying
                    .join()
                    .assured("a failing model participant fails its execution");
                assert_eq!(root.read().next_offset("events", 0), Some(11));
                assert_eq!(root.read().next_offset("events", 2), Some(0));
                assert_eq!(root.read().applied_start_generation(), Some(7));
            });
        }
        use nervix_model_harness::shuttle::check_interleavings;
        use nervix_primitives::{sync::blocking::mpsc, thread};

        use super::*;

        /// Concurrent instances may both resolve the first start, but neither can reset offsets
        /// another instance already acknowledged in that generation.
        #[test]
        fn shuttle_instances_apply_a_generation_start_once_without_rewinding_commits() {
            check_interleavings(|| {
                let state = Arc::new(
                    ReplicatedKafkaOffsetState::new(offset_placement(), None)
                        .assured("empty offsets initialize"),
                );
                let originator = ReplicatedKafkaOffsetState::bind(
                    &state,
                    StateReplicationRoles::owned_by(None),
                    None,
                )
                .originator
                .assured("the local owner originates offsets");
                let mut instances = Vec::new();
                for partition in [0, 1] {
                    let originator = originator.clone();
                    instances.push(thread::spawn(move || {
                        let mut starts = [0, 1]
                            .into_iter()
                            .map(|partition| KafkaOffsetPosition {
                                topic: "events".into(),
                                partition,
                                offset: 0,
                            })
                            .collect::<Vec<_>>();
                        if partition == 1 {
                            starts.push(KafkaOffsetPosition {
                                topic: "events".into(),
                                partition: 2,
                                offset: 0,
                            });
                        }
                        originator
                            .apply_start(7, starts)
                            .assured("the start is assignment-qualified");
                        originator
                            .apply_committed_offset(&KafkaOffsetPosition {
                                topic: "events".into(),
                                partition,
                                offset: i64::from(partition) + 1,
                            })
                            .assured("each instance commits only its own partition");
                    }));
                }
                for instance in instances {
                    instance
                        .join()
                        .assured("a failing model participant fails the entire execution");
                }
                let read = originator.read();
                assert_eq!(read.applied_start_generation(), Some(7));
                assert_eq!(read.next_offset("events", 0), Some(1));
                assert_eq!(read.next_offset("events", 1), Some(2));
                assert_eq!(read.next_offset("events", 2), Some(0));
            });
        }

        /// The owner commits a recorded partition's offset and checks its replica quorum while
        /// another thread holds the assignment barrier, which that thread releases only after both
        /// have returned.
        fn commit_under_a_held_barrier() {
            let node_1 = ClusterNodeName::parse("node-1")
                .assured("the test node name satisfies the cluster-node grammar");
            let node_2 = ClusterNodeName::parse("node-2")
                .assured("the test node name satisfies the cluster-node grammar");
            let state = Arc::new(
                ReplicatedKafkaOffsetState::new(offset_placement(), None)
                    .assured("the empty Kafka offset state is valid"),
            );
            let mut assignment = ReplicatedKafkaOffsetState::bind(
                &state,
                StateReplicationRoles::new(Some(node_1.clone()), vec![node_2], 1),
                Some(&node_1),
            );
            let originator = assignment
                .originator
                .take()
                .assured("node-1 is assigned as the owner");
            // A partition's first commit records the partition itself; the commit after it only
            // moves that partition's offset.
            originator
                .apply_committed_offset(&KafkaOffsetPosition {
                    topic: "events".to_string(),
                    partition: 0,
                    offset: 1,
                })
                .assured("the owner assignment is current");
            let (held_tx, held_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel::<()>();
            let barrier = thread::spawn({
                let state = state.clone();
                move || {
                    state.assignment.serialize(|| {
                        held_tx
                            .send(())
                            .assured("the model keeps the receiver until the barrier is held");
                        release_rx.recv().assured(
                            "the model releases the barrier once the commit and its quorum check \
                             returned",
                        );
                    });
                }
            });
            held_rx
                .recv()
                .assured("the barrier thread reports once it holds the barrier");

            // The barrier stays held until the commit and its quorum check return, so either one
            // waiting for it would leave every thread blocked, which Shuttle reports as a deadlock.
            let committed = originator.apply_committed_offset(&KafkaOffsetPosition {
                topic: "events".to_string(),
                partition: 0,
                offset: 2,
            });
            let quorum_satisfied = originator.read().replica_quorum_holds(1);
            release_tx
                .send(())
                .assured("the barrier thread waits for its release");
            barrier.join().assured(
                "Shuttle fails the whole execution when a model thread panics, so no join \
                 observes one",
            );

            assert!(
                committed.is_ok(),
                "the commit was refused while the assignment barrier was held"
            );
            assert!(
                !quorum_satisfied,
                "no replica has acknowledged the committed revision"
            );
        }

        /// Encoding a snapshot holds the assignment barrier. A commit, or the replica quorum check
        /// that follows it, that queued behind the barrier would hold the acknowledged partition
        /// for the whole encode.
        #[test]
        fn shuttle_a_committed_offset_proceeds_while_the_assignment_barrier_is_held() {
            check_interleavings(commit_under_a_held_barrier);
        }

        const MODEL_TASK_JOINS: &str =
            "Shuttle fails the whole execution when a model task panics, so no join observes one";

        /// The owner waits for its one replica to hold a committed revision while that replica
        /// reports holding it. Shuttle never lets the wait's deadline pass unless a check triggers
        /// it, so the wait completes only by learning of the report: a report that lands between
        /// the wait's read of the replicas' progress and its registration for the next report
        /// leaves every task blocked, which Shuttle reports as a deadlock.
        fn quorum_wait_racing_its_replica_acknowledgement() {
            shuttle::future::block_on(async {
                let node_1 = ClusterNodeName::parse("node-1")
                    .assured("the test node name satisfies the cluster-node grammar");
                let node_2 = ClusterNodeName::parse("node-2")
                    .assured("the test node name satisfies the cluster-node grammar");
                let state = Arc::new(
                    ReplicatedKafkaOffsetState::new(offset_placement(), None)
                        .assured("the empty Kafka offset state is valid"),
                );
                let mut assignment = ReplicatedKafkaOffsetState::bind(
                    &state,
                    StateReplicationRoles::new(Some(node_1.clone()), vec![node_2.clone()], 1),
                    Some(&node_1),
                );
                let originator = assignment
                    .originator
                    .take()
                    .assured("node-1 is assigned as the owner");
                let lsm = originator
                    .apply_committed_offset(&KafkaOffsetPosition {
                        topic: "events".to_string(),
                        partition: 0,
                        offset: 1,
                    })
                    .assured("the owner assignment is current");
                let read = originator.read().clone();
                let waiting = nervix_primitives::task::spawn(async move {
                    read.wait_for_replica_quorum(lsm, Duration::from_secs(30))
                        .await
                });
                let reporting = nervix_primitives::task::spawn(async move {
                    state.replication().record(&node_2, lsm);
                });
                reporting.await.assured(MODEL_TASK_JOINS);
                let waited = waiting.await.assured(MODEL_TASK_JOINS);
                assert!(
                    waited.is_ok(),
                    "the quorum wait ended at its deadline although its replica reported holding \
                     the revision"
                );
            });
        }

        /// A replica acknowledgement is never lost to a quorum wait, however it interleaves with
        /// the wait's read and registration.
        #[test]
        fn shuttle_a_replica_acknowledgement_racing_the_quorum_wait_is_never_missed() {
            check_interleavings(quorum_wait_racing_its_replica_acknowledgement);
        }
    }
}
