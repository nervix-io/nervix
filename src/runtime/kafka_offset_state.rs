use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    sync::{
        Arc as StdArc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
};

use ahash::{HashMap, RandomState};
#[cfg(test)]
use arch_into::ArchInto as _;
use error_stack::Report;
#[cfg(test)]
use meticulous::OptionExt as _;
use nervix_execution::sync::{ArcSwap, DashMap};
use nervix_models::ClusterNodeName;
#[cfg(test)]
use nervix_models::KafkaPartitionSchedule;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tokio::sync::Notify;
use triomphe::Arc;

#[cfg(test)]
use super::KafkaDomainOffsetDescribe;
#[cfg(test)]
use super::observability::kafka_domain_offset_describe_from_schedule;
use super::{
    PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStateOperationError,
    RuntimeStatePlacement, StateAssignmentAuthority, StateAssignmentToken, StateAuthorityError,
    StateCapability, StateReplicationRoles, lsm_sequence::LsmSequence,
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

/// One partition of one topic, which is what a Kafka offset is recorded against.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::runtime) struct KafkaTopicPartition {
    pub(in crate::runtime) topic: String,
    pub(in crate::runtime) partition: i32,
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
    replica_progress: DashMap<String, u64, RandomState>,
    replication_notify: Notify,
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
            replica_progress: DashMap::default(),
            replication_notify: Notify::new(),
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

    pub(super) fn mark_replica_progress(&self, node_id: &ClusterNodeName, lsm: u64) {
        self.replica_progress.insert(node_id.to_string(), lsm);
        self.replication_notify.notify_waiters();
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
            Ok(PersistedRuntimeStateEntry {
                lsm,
                schema_fingerprint: self.state.placement.schema_fingerprint,
                payload,
            })
        })
    }

    pub(super) fn primary_node(&self) -> Option<ClusterNodeName> {
        self.state.assignment.roles().primary_node.clone()
    }

    pub(super) fn required_replica_acks(&self) -> usize {
        self.state.assignment.roles().required_replica_acks
    }

    pub(super) fn replica_quorum_satisfied(&self, lsm: u64) -> bool {
        let roles = self.state.assignment.roles();
        roles
            .replica_nodes
            .iter()
            .filter(|node_id| {
                self.state
                    .replica_progress
                    .get(node_id.as_str())
                    .is_some_and(|observed| *observed >= lsm)
            })
            .count()
            >= roles.required_replica_acks
    }

    pub(super) async fn wait_for_replication_progress(&self) {
        self.state.replication_notify.notified().await;
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
        offsets: HashMap<KafkaTopicPartition, i64>,
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
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<u64, Report<StateAuthorityError>> {
        let state = &self.read.state;
        let moved =
            state
                .assignment
                .authorize(self.assignment, StateCapability::Originate, || {
                    state.move_recorded_offset(topic, partition, next_offset)
                })?;
        if let Some(lsm) = moved {
            return Ok(lsm);
        }
        state
            .assignment
            .authorize_exclusive(self.assignment, StateCapability::Originate, || {
                state.record_first_offset(topic, partition, next_offset)
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
                state.replication_notify.notify_waiters();
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
        offsets: HashMap<KafkaTopicPartition, i64>,
        schedules: HashMap<String, KafkaTopicSchedulingState>,
    ) -> Self {
        let mut table = Self {
            topics: HashMap::default(),
            schedules,
        };
        for (key, next_offset) in offsets {
            table.record_partition(&key.topic, key.partition, next_offset);
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

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use meticulous::ResultExt as _;
    use nervix_models::{ClusterNodeName, DomainName, ModelKind, ModelName};
    use tokio::sync::oneshot;
    use triomphe::Arc;

    use super::*;
    use crate::runtime::{RuntimeStateKind, StateReplicationRoles};

    #[tokio::test]
    async fn delayed_replica_snapshot_cannot_overwrite_promoted_state() {
        let topic = "events";
        let partition = 0;
        let offset = |next_offset| {
            HashMap::from_iter([(
                KafkaTopicPartition {
                    topic: topic.to_string(),
                    partition,
                },
                next_offset,
            )])
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
        let delayed_install = tokio::spawn(async move {
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
            .apply_committed_offset(topic, partition, 9)
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
            state: RuntimeStateKind::KafkaOffset,
            kind: ModelKind::Ingestor,
            identifier: ModelName::parse("source")
                .assured("the test model name satisfies the model-name grammar"),
            schema_fingerprint: [0; 32],
            branch_key: None,
        }
    }

    #[cfg(feature = "shuttle")]
    mod shuttle_checks {
        use shuttle::{sync::mpsc, thread};

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
                .apply_committed_offset("events", 0, 1)
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
            let committed = originator.apply_committed_offset("events", 0, 2);
            let quorum_satisfied = originator.read().replica_quorum_satisfied(1);
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
    }
}
