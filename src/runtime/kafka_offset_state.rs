//! Native Kafka offset state and assignment-qualified access.
//!
//! Layer: data plane.
//! - **Owns.** Partition offsets, topic scheduling checkpoints, conservative revisions, native
//!   capture and conversion, replica installation, persistence state and quorum observation.
//! - **Depends on.** Placement vocabulary, assignment capabilities and execution primitives.
//! - **Must not know.** Brokers, graph scheduling, NSPL or transfer framing.

use std::{collections::BTreeMap, num::NonZeroU64, time::Duration};

use ahash::HashMap;
use arch_into::ArchInto as _;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_checkpoint_replication::{CheckpointReplication, ReplicaProgress};
use nervix_connector_kafka::KafkaOffsetPosition;
use nervix_models::ClusterNodeName;
#[cfg(test)]
use nervix_models::KafkaPartitionSchedule;
use nervix_primitives::{
    publication::ArcSwap,
    sync::{
        Arc, StdArc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Instant, timeout_at},
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[cfg(test)]
use super::KafkaDomainOffsetDescribe;
#[cfg(test)]
use super::observability::kafka_domain_offset_describe_from_schedule;
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
struct KafkaPartitionAssignmentSnapshot {
    partition: i32,
    instance_idx: u64,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct KafkaTopicSchedulingSnapshot {
    topic: String,
    instances: NonZeroU64,
    rebalance_epoch: u64,
    observed_partitions: Vec<i32>,
    assignments: Vec<KafkaPartitionAssignmentSnapshot>,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct KafkaOffsetSnapshot {
    offsets: Vec<KafkaOffsetEntrySnapshot>,
    schedules: Vec<KafkaTopicSchedulingSnapshot>,
}

/// Where each recorded partition resumes, and the schedule each topic was last rebalanced onto.
///
/// A table is published whole and replaced only when the recorded partitions or schedules change.
/// Every partition's next offset lives in a slot of its own that later tables share, so a commit to
/// a partition the table already records moves that offset in place.
#[derive(Debug, Clone, Default)]
struct KafkaOffsetTable {
    topics: HashMap<String, BTreeMap<i32, Arc<AtomicI64>>>,
    schedules: HashMap<String, KafkaTopicSchedulingState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KafkaTopicSchedulingState {
    instances: NonZeroU64,
    rebalance_epoch: u64,
    observed_partitions: Vec<i32>,
    assignments: HashMap<i32, u64>,
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

    #[cfg(test)]
    pub(super) fn describe_topic(&self, topic: &str) -> Option<KafkaDomainOffsetDescribe> {
        let schedule = self.state.offsets.load().schedules.get(topic).cloned()?;
        Some(kafka_domain_offset_describe_from_schedule(
            topic,
            schedule.instances,
            &KafkaPartitionSchedule::new(
                schedule.instances,
                schedule.observed_partitions,
                schedule.rebalance_epoch,
            ),
        ))
    }
}

impl KafkaOffsetStateOriginator {
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

    /// Replace every recorded offset, as a new start point does, and encode the state that leaves.
    pub(super) fn replace_offsets(
        &self,
        offsets: Vec<KafkaOffsetPosition>,
    ) -> error_stack::Result<(u64, Vec<u8>), RuntimeStateOperationError> {
        let state = &self.read.state;
        let encoded = state
            .assignment
            .authorize_exclusive(self.assignment, StateCapability::Originate, || {
                let schedules = state.offsets.load().schedules.clone();
                let table = StdArc::new(KafkaOffsetTable::from_offsets(offsets, schedules));
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

    #[cfg(test)]
    pub(super) fn update_partition_schedule(
        &self,
        topic: &str,
        instances: NonZeroU64,
        observed_partitions: Vec<i32>,
    ) -> error_stack::Result<Option<(u64, Vec<u8>)>, RuntimeStateOperationError> {
        let state = &self.read.state;
        let encoded = state
            .assignment
            .authorize_exclusive(self.assignment, StateCapability::Originate, || {
                let current = state.offsets.load();
                let existing = current.schedules.get(topic);
                let rebalance_epoch = match existing {
                    Some(existing) => existing.rebalance_epoch,
                    None => 0,
                };
                let next =
                    KafkaPartitionSchedule::new(instances, observed_partitions, rebalance_epoch);
                let mut next_schedule = KafkaTopicSchedulingState {
                    instances,
                    rebalance_epoch: next.rebalance_epoch,
                    observed_partitions: next.observed_partitions.clone(),
                    assignments: next
                        .instance_assignments
                        .iter()
                        .enumerate()
                        .flat_map(|(instance_idx, partitions)| {
                            partitions
                                .iter()
                                .copied()
                                .map(move |partition| (partition, instance_idx.arch_into()))
                        })
                        .collect(),
                };
                match existing {
                    Some(existing)
                        if existing.instances == next_schedule.instances
                            && existing.observed_partitions
                                == next_schedule.observed_partitions
                            && existing.assignments == next_schedule.assignments =>
                    {
                        return Ok(None);
                    }
                    Some(existing) => {
                        next_schedule.rebalance_epoch = existing
                            .rebalance_epoch
                            .checked_add(1)
                            .assured("an ingestor cannot observe 2^64 partition rebalances");
                    }
                    None => {}
                }
                let mut table = KafkaOffsetTable::clone(&current);
                table.schedules.insert(topic.to_string(), next_schedule);
                let table = StdArc::new(table);
                state.offsets.store(table.clone());
                let lsm = state.current_lsm.advance();
                table.encode().map(|payload| Some((lsm, payload)))
            })
            .change_context(RuntimeStateOperationError::Authority)?;
        encoded.change_context(RuntimeStateOperationError::Persistence)
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
    /// A table recording `offsets`, each partition in a slot of its own, beside `schedules`.
    fn from_offsets(
        offsets: Vec<KafkaOffsetPosition>,
        schedules: HashMap<String, KafkaTopicSchedulingState>,
    ) -> Self {
        let mut table = Self {
            topics: HashMap::default(),
            schedules,
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
        let mut entries = Vec::new();
        for (topic, partitions) in &self.topics {
            for (partition, slot) in partitions {
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
        let mut schedule_entries = self
            .schedules
            .iter()
            .map(|(topic, schedule)| {
                let mut assignments = schedule
                    .assignments
                    .iter()
                    .map(
                        |(partition, instance_idx)| KafkaPartitionAssignmentSnapshot {
                            partition: *partition,
                            instance_idx: *instance_idx,
                        },
                    )
                    .collect::<Vec<_>>();
                assignments.sort_by_key(|left| left.partition);
                KafkaTopicSchedulingSnapshot {
                    topic: topic.clone(),
                    instances: schedule.instances,
                    rebalance_epoch: schedule.rebalance_epoch,
                    observed_partitions: schedule.observed_partitions.clone(),
                    assignments,
                }
            })
            .collect::<Vec<_>>();
        schedule_entries.sort_by(|left, right| left.topic.cmp(&right.topic));
        rkyv::to_bytes::<rkyv::rancor::Error>(&KafkaOffsetSnapshot {
            offsets: entries,
            schedules: schedule_entries,
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
        let snapshot = rkyv::access::<ArchivedKafkaOffsetSnapshot, rkyv::rancor::Error>(payload)
            .change_context(RuntimePersistenceError::DecodeState)?;
        let mut table = Self::default();
        for entry in snapshot.offsets.iter() {
            check()?;
            table.record_partition(
                entry.topic.as_str(),
                entry.partition.to_native(),
                entry.next_offset.to_native(),
            );
        }
        for schedule in snapshot.schedules.iter() {
            check()?;
            let mut assignments = HashMap::default();
            for assignment in schedule.assignments.iter() {
                check()?;
                assignments.insert(
                    assignment.partition.to_native(),
                    assignment.instance_idx.to_native(),
                );
            }
            let mut observed_partitions = Vec::with_capacity(schedule.observed_partitions.len());
            for partition in schedule.observed_partitions.iter() {
                check()?;
                observed_partitions.push(partition.to_native());
            }
            observed_partitions.sort_unstable();
            table.schedules.insert(
                schedule.topic.as_str().to_owned(),
                KafkaTopicSchedulingState {
                    instances: schedule.instances.to_native(),
                    rebalance_epoch: schedule.rebalance_epoch.to_native(),
                    observed_partitions,
                    assignments,
                },
            );
        }
        Ok(table)
    }
}

/// Convert the internal offset checkpoint into typed partition positions for an archive. The
/// scheduling cache is recomputed at START and therefore does not cross the archive boundary.
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
    rkyv::access::<ArchivedKafkaOffsetSnapshot, rkyv::rancor::Error>(payload)
        .change_context(RuntimePersistenceError::DecodeState)
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
    #[rkyv(with = super::native_checkpoint_encoding::IteratorAsVec<super::native_checkpoint_encoding::CancellableEntry<'a, KafkaOffsetEntrySnapshot>>, omit_bounds)]
    offsets: super::native_checkpoint_encoding::CancellableIterator<'a, I>,
    schedules: Vec<KafkaTopicSchedulingSnapshot>,
}

pub(in crate::runtime) fn write_offset_payload(
    offsets: impl ExactSizeIterator<Item = (String, i32, i64)> + Clone,
    writer: &mut dyn std::io::Write,
    cancellation: &nervix_execution::Cancellation,
) -> error_stack::Result<(), RuntimePersistenceError> {
    write_offset_snapshot(offsets, Vec::new(), writer, cancellation)
}

#[cfg_attr(
    nervix_lint,
    nervix::dispatch(
        reason = "the exact-size caller iterator supplies checkpoint positions; its local bodies \
                  remain checked and the serializer owns admitted scratch and cancellation"
    )
)]
fn write_offset_snapshot(
    offsets: impl ExactSizeIterator<Item = (String, i32, i64)> + Clone,
    schedules: Vec<KafkaTopicSchedulingSnapshot>,
    writer: &mut dyn std::io::Write,
    cancellation: &nervix_execution::Cancellation,
) -> error_stack::Result<(), RuntimePersistenceError> {
    let nested = schedules
        .iter()
        .try_fold(0_usize, |total, schedule| {
            total.checked_add(
                128_usize.checked_mul(
                    schedule
                        .assignments
                        .len()
                        .checked_add(schedule.observed_partitions.len())?
                        .checked_add(1)?,
                )?,
            )
        })
        .ok_or_else(|| {
            Report::new(RuntimePersistenceError::NativeEncoding {
                state: RuntimeStateKind::KafkaOffset,
            })
        })?;
    let capacity = super::native_checkpoint_encoding::scratch_bytes::<KafkaOffsetEntrySnapshot>(
        offsets.len(),
        nested,
    )
    .ok_or_else(|| {
        Report::new(RuntimePersistenceError::NativeEncoding {
            state: RuntimeStateKind::KafkaOffset,
        })
    })?;
    let mut scratch = vec![std::mem::MaybeUninit::uninit(); capacity];
    let snapshot = StreamingKafkaOffsetSnapshot {
        offsets: super::native_checkpoint_encoding::CancellableIterator {
            cancellation,
            entries: offsets.map(|(topic, partition, next_offset)| KafkaOffsetEntrySnapshot {
                topic,
                partition,
                next_offset,
            }),
        },
        schedules,
    };
    rkyv::api::low::to_bytes_in_with_alloc::<_, _, rkyv::rancor::Error>(
        &snapshot,
        rkyv::ser::writer::IoWriter::new(writer),
        rkyv::ser::allocator::SubAllocator::new(&mut scratch),
    )
    .map(|_| ())
    .map_err(|error| {
        Report::new(error).change_context(RuntimePersistenceError::NativeEncoding {
            state: RuntimeStateKind::KafkaOffset,
        })
    })
}

impl CapturedKafkaOffsets {
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
        for (topic, schedule) in &self.table.schedules {
            let entries = schedule
                .assignments
                .len()
                .checked_add(schedule.observed_partitions.len())
                .ok_or_else(overflow)?;
            let entries = entries.checked_add(1).ok_or_else(overflow)?;
            let entries = u64::try_from(entries).map_err(|_| overflow())?;
            let topic_bytes = u64::try_from(topic.len()).map_err(|_| overflow())?;
            let bytes = entries.checked_mul(256).ok_or_else(overflow)?;
            let bytes = bytes.checked_add(topic_bytes).ok_or_else(overflow)?;
            encoded = encoded.checked_add(bytes).ok_or_else(overflow)?;
            scratch = scratch.checked_add(bytes).ok_or_else(overflow)?;
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
        let schedules = self
            .table
            .schedules
            .iter()
            .map(|(topic, schedule)| KafkaTopicSchedulingSnapshot {
                topic: topic.clone(),
                instances: schedule.instances,
                rebalance_epoch: schedule.rebalance_epoch,
                observed_partitions: schedule.observed_partitions.clone(),
                assignments: schedule
                    .assignments
                    .iter()
                    .map(
                        |(partition, instance_idx)| KafkaPartitionAssignmentSnapshot {
                            partition: *partition,
                            instance_idx: *instance_idx,
                        },
                    )
                    .collect(),
            })
            .collect();
        write_offset_snapshot(offsets, schedules, writer, cancellation)
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
/// their next offsets, and every topic's schedule, each in key order.
#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
struct KafkaOffsetView {
    offsets: BTreeMap<String, BTreeMap<i32, i64>>,
    schedules: BTreeMap<String, KafkaScheduleView>,
}

/// One topic's recorded schedule with its assignments in partition order.
#[cfg(test)]
#[derive(Debug, PartialEq, Eq)]
struct KafkaScheduleView {
    instances: NonZeroU64,
    rebalance_epoch: u64,
    observed_partitions: Vec<i32>,
    assignments: BTreeMap<i32, u64>,
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
        let mut schedules = BTreeMap::new();
        for (topic, schedule) in &self.schedules {
            let assignments = schedule.assignments.iter().map(|(p, i)| (*p, *i)).collect();
            schedules.insert(
                topic.clone(),
                KafkaScheduleView {
                    instances: schedule.instances,
                    rebalance_epoch: schedule.rebalance_epoch,
                    observed_partitions: schedule.observed_partitions.clone(),
                    assignments,
                },
            );
        }
        KafkaOffsetView { offsets, schedules }
    }

    /// Up to three recorded partitions of up to three topics at any offset, and up to three topic
    /// schedules with their observed partitions in the sorted order a rebalance records them.
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
        let mut schedules = HashMap::default();
        for _ in 0..arbitrary.entropy().count(3) {
            let mut observed = std::collections::BTreeSet::new();
            for _ in 0..arbitrary.entropy().count(3) {
                observed.insert(generated_partition(arbitrary));
            }
            let mut assignments = HashMap::default();
            for partition in &observed {
                assignments.insert(*partition, arbitrary.entropy().any_u64());
            }
            schedules.insert(
                arbitrary.string(),
                KafkaTopicSchedulingState {
                    instances: arbitrary.positive_u64(),
                    rebalance_epoch: arbitrary.entropy().any_u64(),
                    observed_partitions: observed.into_iter().collect(),
                    assignments,
                },
            );
        }
        Self::from_offsets(positions, schedules)
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
/// and restores every partition offset and topic schedule.
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
mod tests {
    use ahash::HashMap;
    use nervix_models::{ClusterNodeName, DomainName, ModelKind, ModelName};
    use nervix_primitives::sync::{Arc, oneshot};

    use super::*;
    use crate::runtime::{RuntimeState, StateReplicationRoles};

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
            offsets: vec![
                entry("beta", 1, 10),
                entry("alpha", 2, 20),
                entry("beta", 0, 30),
                entry("alpha", 2, 40),
                entry("alpha", 10, 50),
            ],
            schedules: Vec::new(),
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
            offsets: vec![
                entry("alpha", 0, 1),
                entry("alpha", 1, 2),
                entry("alpha", 2, 3),
            ],
            schedules: Vec::new(),
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
    fn cancelled_or_invalid_kafka_conversion_keeps_the_installed_checkpoint() {
        let payload = |positions| {
            KafkaOffsetTable::from_offsets(positions, HashMap::default())
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
    async fn captured_kafka_offsets_stream_beyond_bulk_memory_with_every_position_and_schedule() {
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
                    payload: KafkaOffsetTable::from_offsets(positions, HashMap::default())
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
        originator
            .update_partition_schedule(&topic, nonzero_ext::nonzero!(2_u64), vec![0, 1, 2])
            .assured("the topic schedule is current");
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
        assert_eq!(
            read.describe_topic(&topic),
            originator.read().describe_topic(&topic)
        );
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
            KafkaOffsetTable::from_offsets(offset(next_offset), HashMap::default())
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

    fn offset_placement() -> RuntimeStatePlacement {
        RuntimeStatePlacement {
            domain: DomainName::parse("default")
                .assured("the test domain name satisfies the domain grammar"),
            state: RuntimeState::KafkaOffset,
            kind: ModelKind::Ingestor,
            identifier: ModelName::parse("source")
                .assured("the test model name satisfies the model-name grammar"),
            branch_key: None,
        }
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
        use nervix_model_harness::shuttle::check_interleavings;
        use nervix_primitives::{sync::blocking::mpsc, thread};

        use super::*;

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
