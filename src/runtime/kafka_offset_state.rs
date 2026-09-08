use std::{
    num::NonZeroU64,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use ahash::{HashMap, RandomState};
#[cfg(test)]
use arch_into::ArchInto as _;
use dashmap::DashMap;
#[cfg(test)]
use meticulous::OptionExt as _;
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
    RuntimeStatePlacement, StateAssignmentAuthority, StateAssignmentToken, StateCapability,
    StateReplicationRoles, lsm_sequence::LsmSequence,
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

/// A decoded Kafka offset snapshot: where each assigned partition resumes, and the partition
/// schedule each topic was last rebalanced onto.
struct KafkaOffsetSnapshotState {
    offsets: HashMap<KafkaTopicPartition, i64>,
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
    offsets: parking_lot::Mutex<HashMap<KafkaTopicPartition, i64>>,
    schedules: parking_lot::Mutex<HashMap<String, KafkaTopicSchedulingState>>,
    current_lsm: LsmSequence,
    last_persisted_lsm: AtomicU64,
    dirty: AtomicBool,
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
    fn snapshot_components(&self) -> KafkaOffsetSnapshotState {
        KafkaOffsetSnapshotState {
            offsets: self.offsets.lock().clone(),
            schedules: self.schedules.lock().clone(),
        }
    }

    pub(super) fn new(
        placement: RuntimeStatePlacement,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Result<Self, RuntimePersistenceError> {
        let mut offsets = HashMap::default();
        let mut schedules = HashMap::default();
        let mut current_lsm = 0;
        let mut last_persisted_lsm = 0;
        if let Some(initial) = initial {
            current_lsm = initial.lsm;
            last_persisted_lsm = initial.lsm;
            let decoded = decode_kafka_offset_snapshot(&initial.payload)?;
            offsets = decoded.offsets;
            schedules = decoded.schedules;
        }
        Ok(Self {
            placement,
            assignment: StateAssignmentAuthority::default(),
            offsets: parking_lot::Mutex::new(offsets),
            schedules: parking_lot::Mutex::new(schedules),
            current_lsm: LsmSequence::restored(current_lsm),
            last_persisted_lsm: AtomicU64::new(last_persisted_lsm),
            dirty: AtomicBool::new(false),
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
}

impl KafkaOffsetStateRead {
    pub(super) fn placement(&self) -> &RuntimeStatePlacement {
        &self.state.placement
    }

    pub(super) fn next_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.state
            .offsets
            .lock()
            .get(&KafkaTopicPartition {
                topic: topic.to_string(),
                partition,
            })
            .copied()
    }

    pub(super) fn current_lsm(&self) -> u64 {
        self.state.current_lsm.current()
    }

    pub(super) fn latest_snapshot(
        &self,
    ) -> Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        self.state.assignment.serialize(|| {
            let components = self.state.snapshot_components();
            Ok(PersistedRuntimeStateEntry {
                lsm: self.state.current_lsm.current(),
                schema_fingerprint: self.state.placement.schema_fingerprint,
                payload: encode_kafka_offset_snapshot(&components.offsets, &components.schedules)?,
            })
        })
    }

    pub(super) fn primary_node(&self) -> Option<ClusterNodeName> {
        self.state.assignment.roles().primary_node
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
        let schedule = self.state.schedules.lock().get(topic).cloned()?;
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
    pub(super) fn read(&self) -> &KafkaOffsetStateRead {
        &self.read
    }

    pub(super) fn persistence(&self) -> KafkaOffsetStatePersistence {
        KafkaOffsetStatePersistence {
            read: self.read.clone(),
        }
    }

    pub(super) fn replace_offsets(
        &self,
        offsets: HashMap<KafkaTopicPartition, i64>,
    ) -> Result<(u64, Vec<u8>), RuntimeStateOperationError> {
        let result = self.read.state.assignment.authorize(
            self.assignment,
            StateCapability::Originate,
            || {
                *self.read.state.offsets.lock() = offsets.clone();
                let schedules = self.read.state.schedules.lock().clone();
                let lsm = self.read.state.current_lsm.advance();
                self.read.state.dirty.store(true, Ordering::SeqCst);
                encode_kafka_offset_snapshot(&offsets, &schedules).map(|payload| (lsm, payload))
            },
        )?;
        Ok(result?)
    }

    pub(super) fn apply_committed_offset(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(u64, Vec<u8>), RuntimeStateOperationError> {
        let result = self.read.state.assignment.authorize(
            self.assignment,
            StateCapability::Originate,
            || {
                let mut offsets = self.read.state.offsets.lock();
                offsets.insert(
                    KafkaTopicPartition {
                        topic: topic.to_string(),
                        partition,
                    },
                    next_offset,
                );
                let snapshot = offsets.clone();
                drop(offsets);
                let schedules = self.read.state.schedules.lock().clone();
                let lsm = self.read.state.current_lsm.advance();
                self.read.state.dirty.store(true, Ordering::SeqCst);
                encode_kafka_offset_snapshot(&snapshot, &schedules).map(|payload| (lsm, payload))
            },
        )?;
        Ok(result?)
    }

    #[cfg(test)]
    pub(super) fn update_partition_schedule(
        &self,
        topic: &str,
        instances: NonZeroU64,
        observed_partitions: Vec<i32>,
    ) -> Result<Option<(u64, Vec<u8>)>, RuntimeStateOperationError> {
        let result = self.read.state.assignment.authorize(
            self.assignment,
            StateCapability::Originate,
            || {
                let next_schedule = {
                    let schedules = self.read.state.schedules.lock();
                    let rebalance_epoch = schedules
                        .get(topic)
                        .map(|existing| existing.rebalance_epoch)
                        .unwrap_or(0);
                    let next = KafkaPartitionSchedule::new(
                        instances,
                        observed_partitions,
                        rebalance_epoch,
                    );
                    KafkaTopicSchedulingState {
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
                    }
                };
                let mut schedules = self.read.state.schedules.lock();
                let mut updated = false;
                match schedules.get(topic) {
                    Some(existing)
                        if existing.instances == next_schedule.instances
                            && existing.observed_partitions
                                == next_schedule.observed_partitions
                            && existing.assignments == next_schedule.assignments => {}
                    Some(existing) => {
                        let mut next_schedule = next_schedule;
                        next_schedule.rebalance_epoch = existing
                            .rebalance_epoch
                            .checked_add(1)
                            .assured("an ingestor cannot observe 2^64 partition rebalances");
                        schedules.insert(topic.to_string(), next_schedule);
                        updated = true;
                    }
                    None => {
                        schedules.insert(topic.to_string(), next_schedule);
                        updated = true;
                    }
                }
                let snapshot_schedules = schedules.clone();
                drop(schedules);
                if !updated {
                    return Ok(None);
                }
                let offsets = self.read.state.offsets.lock().clone();
                let lsm = self.read.state.current_lsm.advance();
                self.read.state.dirty.store(true, Ordering::SeqCst);
                encode_kafka_offset_snapshot(&offsets, &snapshot_schedules)
                    .map(|payload| Some((lsm, payload)))
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
        let decoded = decode_kafka_offset_snapshot(payload)?;
        self.read.state.assignment.authorize(
            self.assignment,
            StateCapability::InstallSnapshot,
            || {
                *self.read.state.offsets.lock() = decoded.offsets;
                *self.read.state.schedules.lock() = decoded.schedules;
                self.read.state.current_lsm.adopt(lsm);
                self.read.state.dirty.store(true, Ordering::SeqCst);
                self.read.state.replication_notify.notify_waiters();
            },
        )?;
        Ok(())
    }
}

impl KafkaOffsetStatePersistence {
    pub(super) fn read(&self) -> &KafkaOffsetStateRead {
        &self.read
    }

    pub(super) fn take_dirty(&self) -> bool {
        self.read.state.dirty.swap(false, Ordering::SeqCst)
    }

    pub(super) fn restore_dirty(&self) {
        self.read.state.dirty.store(true, Ordering::SeqCst);
    }

    pub(super) fn last_persisted_lsm(&self) -> u64 {
        self.read.state.last_persisted_lsm.load(Ordering::SeqCst)
    }

    pub(super) fn record_persisted(&self, lsm: u64) {
        self.read.state.assignment.serialize(|| {
            self.read
                .state
                .last_persisted_lsm
                .fetch_max(lsm, Ordering::SeqCst);
            if self.read.state.current_lsm.current() <= lsm {
                self.read.state.dirty.store(false, Ordering::SeqCst);
            }
        });
    }
}

fn encode_kafka_offset_snapshot(
    offsets: &HashMap<KafkaTopicPartition, i64>,
    schedules: &HashMap<String, KafkaTopicSchedulingState>,
) -> Result<Vec<u8>, RuntimePersistenceError> {
    let mut entries = offsets
        .iter()
        .map(|(key, next_offset)| KafkaOffsetEntrySnapshot {
            topic: key.topic.clone(),
            partition: key.partition,
            next_offset: *next_offset,
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.partition.cmp(&right.partition))
    });
    let mut schedule_entries = schedules
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

fn decode_kafka_offset_snapshot(
    payload: &[u8],
) -> Result<KafkaOffsetSnapshotState, RuntimePersistenceError> {
    let snapshot = rkyv::from_bytes::<KafkaOffsetSnapshot, rkyv::rancor::Error>(payload)
        .map_err(|error| RuntimePersistenceError::DecodeState(error.to_string()))?;
    let mut offsets = HashMap::default();
    for entry in snapshot.offsets {
        offsets.insert(
            KafkaTopicPartition {
                topic: entry.topic,
                partition: entry.partition,
            },
            entry.next_offset,
        );
    }
    let mut schedules = HashMap::default();
    for schedule in snapshot.schedules {
        let mut assignments = HashMap::default();
        for assignment in schedule.assignments {
            assignments.insert(assignment.partition, assignment.instance_idx);
        }
        let mut observed_partitions = schedule.observed_partitions;
        observed_partitions.sort_unstable();
        schedules.insert(
            schedule.topic,
            KafkaTopicSchedulingState {
                instances: schedule.instances,
                rebalance_epoch: schedule.rebalance_epoch,
                observed_partitions,
                assignments,
            },
        );
    }
    Ok(KafkaOffsetSnapshotState { offsets, schedules })
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
            encode_kafka_offset_snapshot(&offset(next_offset), &HashMap::default())
                .assured("the in-memory test snapshot contains encodable values")
        };
        let node_1 = ClusterNodeName::parse("node-1")
            .assured("the test node name satisfies the cluster-node grammar");
        let node_2 = ClusterNodeName::parse("node-2")
            .assured("the test node name satisfies the cluster-node grammar");
        let state = Arc::new(
            ReplicatedKafkaOffsetState::new(
                RuntimeStatePlacement {
                    domain: DomainName::parse("default")
                        .assured("the test domain name satisfies the domain grammar"),
                    state: RuntimeStateKind::KafkaOffset,
                    kind: ModelKind::Ingestor,
                    identifier: ModelName::parse("source")
                        .assured("the test model name satisfies the model-name grammar"),
                    schema_fingerprint: [0; 32],
                    branch_key: None,
                },
                None,
            )
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
            .assured("the promoted owner offset is encodable");
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
}
