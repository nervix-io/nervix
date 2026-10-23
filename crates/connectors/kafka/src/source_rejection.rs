//! Where a consumer-group source starts a partition it rejected records of, whenever its group
//! assigns it that partition, until a commit passes the earliest of them.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The earliest rejected record of each partition no commit has passed yet, and the
//!   consumer context that starts each such partition at that record whenever the group assigns
//!   it, so a reassignment never resumes it from a committed offset past an unacknowledged record
//!   or from where the client's reset policy starts a partition without one.
//! - **Depends on.** `rust-rdkafka`'s client and consumer contexts, its assignment calls and
//!   topic partition lists, and the boundary's thread-blocking lock.
//! - **Must not know.** The host, domain-owned offsets, how a commit reaches its coordinator, or
//!   any other connector implementation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::collections::BTreeMap;

use nervix_primitives::sync::blocking::Mutex;
use rdkafka::{
    ClientContext,
    consumer::{BaseConsumer, Consumer as _, ConsumerContext, RebalanceProtocol, StreamConsumer},
    error::{KafkaError, RDKafkaErrorCode},
    topic_partition_list::{Offset, TopicPartitionList},
    types::RDKafkaRespErr,
};
use tracing::warn;

use super::KafkaOffsetPosition;

/// The consumer of every Kafka source and partition inspector.
pub(super) type KafkaConsumer = StreamConsumer<KafkaConsumerContext>;

/// One partition of a topic a source rejected records of.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RejectedPartition {
    topic: String,
    partition: i32,
}

/// The earliest rejected record of each partition whose rejection no commit has passed yet.
///
/// A consumer group resumes a partition it assigns from the group's committed offset, or, when the
/// group committed none, from where the client's `auto.offset.reset` starts a partition. Neither is
/// at a rejected record once the seek that rewound the partition is lost, which happens whenever
/// the group revokes the partition before its records are read again: after the source leaves and
/// rejoins the group, and after its group session lapses. A floor outlives every such loss, until
/// a commit at or past the record after it confirms that the rejected records were delivered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct RejectionFloors {
    /// The offset of each partition's earliest rejected record.
    partitions: BTreeMap<RejectedPartition, i64>,
}

impl RejectionFloors {
    /// Records each of `starts`, the offset of a partition's earliest rejected record. A partition
    /// that already has a floor keeps the earlier one, which an earlier rejection left unread.
    pub(super) fn record(&mut self, starts: &[KafkaOffsetPosition]) {
        for start in starts {
            let partition = RejectedPartition {
                topic: start.topic.clone(),
                partition: start.partition,
            };
            if let Some(floor) = self.partitions.get_mut(&partition) {
                *floor = (*floor).min(start.offset);
            } else {
                self.partitions.insert(partition, start.offset);
            }
        }
    }

    /// Forgets the floor of every partition that `committed`, the next offsets a commit stored,
    /// moved past its earliest rejected record.
    pub(super) fn confirm(&mut self, committed: &[KafkaOffsetPosition]) {
        for position in committed {
            let partition = RejectedPartition {
                topic: position.topic.clone(),
                partition: position.partition,
            };
            if let Some(floor) = self.partitions.get(&partition)
                && position.offset > *floor
            {
                self.partitions.remove(&partition);
            }
        }
    }

    /// Starts every partition of `assignment` that has a floor at its floor.
    ///
    /// Under the eager protocol an assignment names every partition the group gives this member,
    /// so a partition it omits went to another member, which resumes it from the group's committed
    /// offset, and its floor is forgotten. A cooperative assignment names only the partitions it
    /// adds, so it leaves the other floors in place.
    pub(super) fn stage(
        &mut self,
        assignment: &mut TopicPartitionList,
        protocol: &RebalanceProtocol,
    ) -> Result<(), KafkaError> {
        let mut assigned = BTreeMap::new();
        for element in assignment.elements() {
            let partition = RejectedPartition {
                topic: element.topic().to_string(),
                partition: element.partition(),
            };
            if let Some(floor) = self.partitions.get(&partition) {
                assigned.insert(partition, *floor);
            }
        }
        for (partition, floor) in &assigned {
            assignment.set_partition_offset(
                &partition.topic,
                partition.partition,
                Offset::Offset(*floor),
            )?;
        }
        if let RebalanceProtocol::Eager = protocol {
            self.partitions = assigned;
        }
        Ok(())
    }

    /// Whether every rejection has been passed by a commit.
    pub(super) fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }
}

/// The context of every Kafka source and partition inspector consumer.
///
/// Logging, errors and statistics keep rust-rdkafka's default handling. A rebalance assigns and
/// revokes partitions as rust-rdkafka's default handling does, after it started every assigned
/// partition that has a floor at that floor. librdkafka runs the rebalance while the source polls
/// for its next batch, and the source never polls while it rejects or acknowledges a batch, so the
/// lock is never contended; it orders the floors only against a rebalance a future version of the
/// driver might serve on another thread.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        bounded,
        reason = "a source records its rejected partitions once per rejection, confirms them once \
                  per acknowledged batch while a rejection is outstanding, and its consumer \
                  stages them once per rebalance",
        key = "one Kafka consumer's rejected partitions",
        bound = "one floor per partition of the source topic; no guard is held across an await or \
                 a driver call that waits on the broker"
    )
)]
#[derive(Default)]
pub(super) struct KafkaConsumerContext {
    floors: Mutex<RejectionFloors>,
}

impl KafkaConsumerContext {
    /// Records `starts`, the earliest rejected record of each partition of a rejected batch, before
    /// the source rewinds the partitions it is still assigned, so a partition the group assigns
    /// from then on starts at its floor.
    pub(super) fn record_rejection(&self, starts: &[KafkaOffsetPosition]) {
        self.floors.lock().record(starts);
    }

    /// Forgets the floors that `committed`, the next offsets a commit stored, passed, and returns
    /// whether any rejection is still outstanding.
    pub(super) fn confirm_commit(&self, committed: &[KafkaOffsetPosition]) -> bool {
        let mut floors = self.floors.lock();
        floors.confirm(committed);
        !floors.is_empty()
    }

    /// Starts every partition of `assignment` that has a floor at its floor, as
    /// [`RejectionFloors::stage`] describes.
    pub(super) fn stage_assignment(
        &self,
        assignment: &mut TopicPartitionList,
        protocol: &RebalanceProtocol,
    ) -> Result<(), KafkaError> {
        self.floors.lock().stage(assignment, protocol)
    }

    /// The partitions whose rejections no commit has passed yet.
    #[cfg(test)]
    pub(super) fn floors(&self) -> RejectionFloors {
        self.floors.lock().clone()
    }
}

impl ClientContext for KafkaConsumerContext {}

impl ConsumerContext for KafkaConsumerContext {
    fn rebalance(
        &self,
        base_consumer: &BaseConsumer<Self>,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        let protocol = base_consumer.rebalance_protocol();
        match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                if let Err(error) = self.stage_assignment(tpl, &protocol) {
                    warn!(
                        error = %error,
                        "failed to start rejected Kafka partitions at their earliest rejected \
                         record; the group's committed offsets start them"
                    );
                }
                let assigned = match protocol {
                    RebalanceProtocol::Cooperative => base_consumer.incremental_assign(tpl),
                    RebalanceProtocol::Eager | RebalanceProtocol::None => base_consumer.assign(tpl),
                };
                if let Err(error) = assigned {
                    warn!(error = %error, "failed to take up a Kafka consumer group assignment");
                }
            }
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                let revoked = match protocol {
                    RebalanceProtocol::Cooperative => base_consumer.incremental_unassign(tpl),
                    RebalanceProtocol::Eager | RebalanceProtocol::None => base_consumer.unassign(),
                };
                if let Err(error) = revoked {
                    warn!(error = %error, "failed to give up a revoked Kafka assignment");
                }
            }
            other => {
                warn!(
                    error = %RDKafkaErrorCode::from(other),
                    "Kafka consumer group rebalance failed; the consumer gives up its assignment"
                );
                let revoked = match protocol {
                    RebalanceProtocol::Cooperative => base_consumer.incremental_unassign(tpl),
                    RebalanceProtocol::Eager | RebalanceProtocol::None => base_consumer.unassign(),
                };
                if let Err(error) = revoked {
                    warn!(error = %error, "failed to give up a Kafka assignment after a failed rebalance");
                }
            }
        }
    }
}

#[cfg(all(test, feature = "shuttle"))]
#[path = "source_rejection_shuttle_tests.rs"]
mod shuttle_tests;
