use std::{collections::BTreeMap, num::NonZeroU64, sync::Arc as StdArc, time::Duration};

use ahash::HashMap;
#[cfg(test)]
use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_checkpoint_replication::{CheckpointReplication, ReplicaProgress};
use nervix_connector_kafka::KafkaOffsetPosition;
use nervix_models::ClusterNodeName;
#[cfg(test)]
use nervix_models::KafkaPartitionSchedule;
use nervix_primitives::{
    publication::ArcSwap,
    sync::atomic::{AtomicI64, AtomicU64, Ordering},
    time::{Instant, timeout_at},
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use triomphe::Arc;

#[cfg(test)]
use super::KafkaDomainOffsetDescribe;
#[cfg(test)]
use super::observability::kafka_domain_offset_describe_from_schedule;
use super::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStateOperationError,
    RuntimeStatePlacement, StateAssignmentAuthority, StateAssignmentToken, StateAuthorityError,
    StateCapability, StateReplicationRoles, lsm_sequence::LsmSequence,
    state_replication::StateReplicationError,
};

/// How long a committed or replaced offset waits for the replicas the offsets are assigned to hold
/// it.
const REPLICA_QUORUM_WAIT: Duration = Duration::from_secs(5);

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
    ) -> Result<Self, RuntimePersistenceError> {
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

    /// Encode the offsets this state records, stamped with its revision.
    ///
    /// The barrier keeps the recorded partitions fixed while they are read. The revision is read
    /// first, so a commit admitted while the offsets are read is either inside that revision or
    /// ahead of it, and a later snapshot carries it again.
    pub(super) fn latest_snapshot(
        &self,
    ) -> Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
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

    /// Wait until enough of the assigned replicas report holding revision `lsm`, for at most
    /// [`REPLICA_QUORUM_WAIT`].
    ///
    /// The wait registers for the next replica report before it reads what the replicas hold, so a
    /// report that lands in between wakes it instead of leaving the commit to its deadline. The
    /// replicas are the ones assigned when each report arrives.
    pub(super) async fn wait_for_replica_quorum(
        &self,
        lsm: u64,
    ) -> error_stack::Result<(), StateReplicationError> {
        let deadline = Instant::now()
            .checked_add(REPLICA_QUORUM_WAIT)
            .assured("a wait of a few seconds stays within Instant");
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
    ) -> Result<(u64, Vec<u8>), RuntimeStateOperationError> {
        let state = &self.read.state;
        let result = state.assignment.authorize_exclusive(
            self.assignment,
            StateCapability::Originate,
            || {
                let schedules = state.offsets.load().schedules.clone();
                let table = StdArc::new(KafkaOffsetTable::from_offsets(offsets, schedules));
                state.offsets.store(table.clone());
                let lsm = state.current_lsm.advance();
                table.encode().map(|payload| (lsm, payload))
            },
        )?;
        Ok(result?)
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
    ) -> Result<Option<(u64, Vec<u8>)>, RuntimeStateOperationError> {
        let state = &self.read.state;
        let result = state.assignment.authorize_exclusive(
            self.assignment,
            StateCapability::Originate,
            || {
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
            },
        )?;
        Ok(result?)
    }
}

impl KafkaOffsetSnapshotInstaller {
    pub(super) fn read(&self) -> &KafkaOffsetStateRead {
        &self.read
    }

    pub(super) fn install_snapshot(
        &self,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), RuntimeStateOperationError> {
        let table = KafkaOffsetTable::decode(payload)?;
        let state = &self.read.state;
        state.assignment.authorize_exclusive(
            self.assignment,
            StateCapability::InstallSnapshot,
            || {
                state.offsets.store(StdArc::new(table));
                state.current_lsm.adopt(lsm);
            },
        )?;
        Ok(())
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

    fn encode(&self) -> Result<Vec<u8>, RuntimePersistenceError> {
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
        .map_err(|error| RuntimePersistenceError::EncodeState(error.to_string()))
    }

    fn decode(payload: &[u8]) -> Result<Self, RuntimePersistenceError> {
        let snapshot = rkyv::from_bytes::<KafkaOffsetSnapshot, rkyv::rancor::Error>(payload)
            .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
        let mut table = Self::default();
        for entry in snapshot.offsets {
            table.record_partition(&entry.topic, entry.partition, entry.next_offset);
        }
        for schedule in snapshot.schedules {
            let mut assignments = HashMap::default();
            for assignment in schedule.assignments {
                assignments.insert(assignment.partition, assignment.instance_idx);
            }
            let mut observed_partitions = schedule.observed_partitions;
            observed_partitions.sort_unstable();
            table.schedules.insert(
                schedule.topic,
                KafkaTopicSchedulingState {
                    instances: schedule.instances,
                    rebalance_epoch: schedule.rebalance_epoch,
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
    let table = KafkaOffsetTable::decode(payload).map_err(Report::new)?;
    let mut offsets = Vec::new();
    for (topic, partitions) in table.topics {
        for (partition, slot) in partitions {
            offsets.push((topic.clone(), partition, slot.load(Ordering::SeqCst)));
        }
    }
    offsets.sort_by(|left, right| (&left.0, left.1).cmp(&(&right.0, right.1)));
    Ok(offsets)
}

pub(in crate::runtime) fn restore_offset_payload(
    offsets: Vec<(String, i32, i64)>,
) -> error_stack::Result<Vec<u8>, RuntimePersistenceError> {
    let positions = offsets
        .into_iter()
        .map(|(topic, partition, offset)| KafkaOffsetPosition {
            topic,
            partition,
            offset,
        })
        .collect();
    KafkaOffsetTable::from_offsets(positions, HashMap::default())
        .encode()
        .map_err(Report::new)
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use meticulous::ResultExt as _;
    use nervix_models::{ClusterNodeName, DomainName, ModelKind, ModelName};
    use nervix_primitives::sync::oneshot;
    use triomphe::Arc;

    use super::*;
    use crate::runtime::{RuntimeState, StateReplicationRoles};

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
            .install_snapshot(1, &payload(2))
            .assured("the initial replica snapshot is valid");

        let delayed_installer = installer.clone();
        let delayed_payload = payload(3);
        let (response_received_tx, response_received_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let delayed_install = nervix_primitives::task::spawn(async move {
            let _ = response_received_tx.send(());
            release_rx
                .await
                .assured("the test retains the delayed-response release sender");
            delayed_installer.install_snapshot(2, &delayed_payload)
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
        let _ = release_tx.send(());
        let delayed_result = delayed_install
            .await
            .assured("the delayed replica response task joins");

        assert!(matches!(
            delayed_result,
            Err(RuntimeStateOperationError::Authority(_))
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

    #[cfg(feature = "shuttle")]
    mod shuttle_checks {
        use nervix_primitives::{sync::blocking::mpsc, thread};

        use super::*;
        use crate::shuttle_test::check_interleavings;

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
                    read.wait_for_replica_quorum(lsm).await
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
