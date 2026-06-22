use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use ahash::{HashMap, RandomState};
use dashmap::DashMap;
#[cfg(test)]
use nervix_models::KafkaPartitionSchedule;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tokio::sync::Notify;

#[cfg(test)]
use super::KafkaDomainOffsetDescribe;
use super::{PersistedRuntimeStateEntry, RuntimePersistenceError, RuntimeStatePlacement};

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
    instances: u64,
    rebalance_epoch: u64,
    observed_partitions: Vec<i32>,
    assignments: Vec<KafkaPartitionAssignmentSnapshot>,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct KafkaOffsetSnapshot {
    offsets: Vec<KafkaOffsetEntrySnapshot>,
    schedules: Vec<KafkaTopicSchedulingSnapshot>,
}

type KafkaOffsetSnapshotState = (
    HashMap<(String, i32), i64>,
    HashMap<String, KafkaTopicSchedulingState>,
);

#[derive(Debug, Clone, PartialEq, Eq)]
struct KafkaTopicSchedulingState {
    instances: u64,
    rebalance_epoch: u64,
    observed_partitions: Vec<i32>,
    assignments: HashMap<i32, u64>,
}

#[derive(Debug)]
pub(super) struct ReplicatedKafkaOffsetState {
    pub(super) placement: RuntimeStatePlacement,
    pub(super) required_replica_acks: usize,
    pub(super) primary_node: Option<String>,
    pub(super) replica_nodes: Vec<String>,
    offsets: parking_lot::Mutex<HashMap<(String, i32), i64>>,
    schedules: parking_lot::Mutex<HashMap<String, KafkaTopicSchedulingState>>,
    pub(super) current_lsm: AtomicU64,
    pub(super) last_persisted_lsm: AtomicU64,
    pub(super) dirty: AtomicBool,
    pub(super) replica_progress: DashMap<String, u64, RandomState>,
    pub(super) replication_notify: Notify,
}

impl ReplicatedKafkaOffsetState {
    fn snapshot_components(
        &self,
    ) -> (
        HashMap<(String, i32), i64>,
        HashMap<String, KafkaTopicSchedulingState>,
    ) {
        let offsets = self.offsets.lock().clone();
        let schedules = self.schedules.lock().clone();
        (offsets, schedules)
    }

    pub(super) fn new(
        placement: RuntimeStatePlacement,
        primary_node: Option<String>,
        replica_nodes: Vec<String>,
        required_replica_acks: usize,
        initial: Option<PersistedRuntimeStateEntry>,
    ) -> Result<Self, RuntimePersistenceError> {
        let mut offsets = HashMap::default();
        let mut schedules = HashMap::default();
        let mut current_lsm = 0;
        let mut last_persisted_lsm = 0;
        if let Some(initial) = initial {
            current_lsm = initial.lsm;
            last_persisted_lsm = initial.lsm;
            (offsets, schedules) = decode_kafka_offset_snapshot(&initial.payload)?;
        }
        Ok(Self {
            placement,
            required_replica_acks,
            primary_node,
            replica_nodes,
            offsets: parking_lot::Mutex::new(offsets),
            schedules: parking_lot::Mutex::new(schedules),
            current_lsm: AtomicU64::new(current_lsm),
            last_persisted_lsm: AtomicU64::new(last_persisted_lsm),
            dirty: AtomicBool::new(false),
            replica_progress: DashMap::default(),
            replication_notify: Notify::new(),
        })
    }

    pub(super) fn next_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.offsets
            .lock()
            .get(&(topic.to_string(), partition))
            .copied()
    }

    pub(super) fn replace_offsets(
        &self,
        offsets: HashMap<(String, i32), i64>,
    ) -> Result<(u64, Vec<u8>), RuntimePersistenceError> {
        *self.offsets.lock() = offsets.clone();
        let schedules = self.schedules.lock().clone();
        let lsm = self
            .current_lsm
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.dirty.store(true, Ordering::SeqCst);
        Ok((lsm, encode_kafka_offset_snapshot(&offsets, &schedules)?))
    }

    pub(super) fn apply_committed_offset(
        &self,
        topic: &str,
        partition: i32,
        next_offset: i64,
    ) -> Result<(u64, Vec<u8>), RuntimePersistenceError> {
        let mut offsets = self.offsets.lock();
        offsets.insert((topic.to_string(), partition), next_offset);
        let snapshot = offsets.clone();
        drop(offsets);
        let schedules = self.schedules.lock().clone();
        let lsm = self
            .current_lsm
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.dirty.store(true, Ordering::SeqCst);
        Ok((lsm, encode_kafka_offset_snapshot(&snapshot, &schedules)?))
    }

    #[cfg(test)]
    pub(super) fn update_partition_schedule(
        &self,
        topic: &str,
        instances: u64,
        observed_partitions: Vec<i32>,
    ) -> Result<Option<(u64, Vec<u8>)>, RuntimePersistenceError> {
        let next_schedule = {
            let schedules = self.schedules.lock();
            let rebalance_epoch = schedules
                .get(topic)
                .map(|existing| existing.rebalance_epoch)
                .unwrap_or(0);
            let next = KafkaPartitionSchedule::new(instances, observed_partitions, rebalance_epoch);
            KafkaTopicSchedulingState {
                instances,
                rebalance_epoch: next.rebalance_epoch,
                observed_partitions: next.observed_partitions.clone(),
                assignments: next
                    .instance_assignments
                    .iter()
                    .enumerate()
                    .flat_map(|(instance_idx, partitions)| {
                        partitions.iter().copied().map(move |partition| {
                            (partition, u64::try_from(instance_idx).unwrap_or_default())
                        })
                    })
                    .collect(),
            }
        };
        let mut schedules = self.schedules.lock();
        let mut updated = false;
        match schedules.get(topic) {
            Some(existing)
                if existing.instances == next_schedule.instances
                    && existing.observed_partitions == next_schedule.observed_partitions
                    && existing.assignments == next_schedule.assignments => {}
            Some(existing) => {
                let mut next_schedule = next_schedule;
                next_schedule.rebalance_epoch = existing.rebalance_epoch.saturating_add(1);
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
        let offsets = self.offsets.lock().clone();
        let lsm = self
            .current_lsm
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.dirty.store(true, Ordering::SeqCst);
        Ok(Some((
            lsm,
            encode_kafka_offset_snapshot(&offsets, &snapshot_schedules)?,
        )))
    }

    #[cfg(test)]
    pub(super) fn describe_topic(&self, topic: &str) -> Option<KafkaDomainOffsetDescribe> {
        let schedule = self.schedules.lock().get(topic).cloned()?;
        Some(super::kafka_domain_offset_describe_from_schedule(
            topic,
            schedule.instances,
            &KafkaPartitionSchedule::new(
                schedule.instances,
                schedule.observed_partitions,
                schedule.rebalance_epoch,
            ),
        ))
    }

    pub(super) fn apply_snapshot(
        &self,
        lsm: u64,
        payload: &[u8],
    ) -> Result<(), RuntimePersistenceError> {
        let (offsets, schedules) = decode_kafka_offset_snapshot(payload)?;
        *self.offsets.lock() = offsets;
        *self.schedules.lock() = schedules;
        self.current_lsm.store(lsm, Ordering::SeqCst);
        self.dirty.store(true, Ordering::SeqCst);
        self.replication_notify.notify_waiters();
        Ok(())
    }

    pub(super) fn latest_snapshot(
        &self,
    ) -> Result<PersistedRuntimeStateEntry, RuntimePersistenceError> {
        let (offsets, schedules) = self.snapshot_components();
        Ok(PersistedRuntimeStateEntry {
            lsm: self.current_lsm.load(Ordering::SeqCst),
            payload: encode_kafka_offset_snapshot(&offsets, &schedules)?,
        })
    }

    pub(super) fn mark_replica_progress(&self, node_id: &str, lsm: u64) {
        self.replica_progress.insert(node_id.to_string(), lsm);
        self.replication_notify.notify_waiters();
    }

    pub(super) fn replica_quorum_satisfied(&self, lsm: u64) -> bool {
        self.replica_nodes
            .iter()
            .filter(|node_id| {
                self.replica_progress
                    .get(node_id.as_str())
                    .is_some_and(|observed| *observed >= lsm)
            })
            .count()
            >= self.required_replica_acks
    }
}

fn encode_kafka_offset_snapshot(
    offsets: &HashMap<(String, i32), i64>,
    schedules: &HashMap<String, KafkaTopicSchedulingState>,
) -> Result<Vec<u8>, RuntimePersistenceError> {
    let mut entries = offsets
        .iter()
        .map(
            |((topic, partition), next_offset)| KafkaOffsetEntrySnapshot {
                topic: topic.clone(),
                partition: *partition,
                next_offset: *next_offset,
            },
        )
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
        offsets.insert((entry.topic, entry.partition), entry.next_offset);
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
    Ok((offsets, schedules))
}
