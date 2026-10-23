//! The Kafka source's rejected-partition floors explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants the floors are held to while a consumer group decides revocations and
//!   assignments at any point of a rejection: the first record the source reads again from a
//!   rejected partition is the rejected one, however the group takes the partition away, and a
//!   commit past that record lets the next assignment resume from the committed offset.
//! - **Depends on.** The consumer context's floors, a fake consumer whose rebalance events a
//!   coordinator task queues and the source's polls serve, as librdkafka serves them, a channel as
//!   the consumer's event queue, and the model harness's Shuttle runner.
//! - **Must not know.** librdkafka's group protocol, a broker, or the source contract.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_primitives::sync::mpsc;
use rdkafka::{
    consumer::RebalanceProtocol,
    topic_partition_list::{Offset, TopicPartitionList},
};

use super::{super::KafkaOffsetPosition, KafkaConsumerContext};

const CHECK_TASK_JOINS: &str =
    "a check task that panics fails the execution before its join returns";

const TOPIC: &str = "orders";

/// The offset of the record the source rejects.
const REJECTED: i64 = 5;

/// Where the partition ends, which is where the client's `latest` reset starts a partition the
/// group committed nothing for.
const END: i64 = 9;

fn position(offset: i64) -> KafkaOffsetPosition {
    KafkaOffsetPosition {
        topic: TOPIC.to_string(),
        partition: 0,
        offset,
    }
}

/// A rebalance the group's coordinator decided, which the consumer's next poll serves.
enum RebalanceEvent {
    Revoke,
    Assign,
}

/// The source's consumer of one partition: where it fetches the partition from while the group
/// assigns it, and the next offset the group committed.
struct FakeConsumer {
    fetching: Option<i64>,
    committed: Option<i64>,
}

impl FakeConsumer {
    /// Serves `event` as librdkafka's poll serves a rebalance: a revocation stops the fetch, which
    /// loses any seek, and an assignment starts the partition where the context stages it, at the
    /// committed offset when it stages nothing, or at the partition's end when the group committed
    /// nothing.
    fn serve(&mut self, event: RebalanceEvent, context: &KafkaConsumerContext) {
        match event {
            RebalanceEvent::Revoke => self.fetching = None,
            RebalanceEvent::Assign => {
                let mut assignment = TopicPartitionList::new();
                assignment.add_partition(TOPIC, 0);
                context
                    .stage_assignment(&mut assignment, &RebalanceProtocol::Eager)
                    .assured("a floor is a valid start offset");
                let staged = assignment
                    .find_partition(TOPIC, 0)
                    .assured("the assignment names the partition it assigns")
                    .offset();
                let start = match staged {
                    Offset::Offset(floor) => floor,
                    Offset::Beginning
                    | Offset::End
                    | Offset::Stored
                    | Offset::Invalid
                    | Offset::OffsetTail(_) => self.committed.unwrap_or(END),
                };
                self.fetching = Some(start);
            }
        }
    }

    /// Serves every event already queued, then, while the partition is not assigned, waits for
    /// the next one, and returns the offset of the next record read once it is.
    async fn read(
        &mut self,
        events: &mut mpsc::UnboundedReceiver<RebalanceEvent>,
        context: &KafkaConsumerContext,
    ) -> i64 {
        while let Ok(event) = events.try_recv() {
            self.serve(event, context);
        }
        loop {
            nervix_primitives::task::consume_budget().await;
            if let Some(fetching) = self.fetching {
                let next = fetching
                    .checked_add(1)
                    .assured("a check partition holds only a few records");
                self.fetching = Some(next);
                return fetching;
            }
            let event = events
                .recv()
                .await
                .assured("the coordinator assigns the partition again before it stops");
            self.serve(event, context);
        }
    }

    /// Rejects the record at `REJECTED` as the source does: it records the floor, then seeks the
    /// partition if the consumer is still assigned it.
    fn reject(&mut self, context: &KafkaConsumerContext) {
        context.record_rejection(&[position(REJECTED)]);
        if self.fetching.is_some() {
            self.fetching = Some(REJECTED);
        }
    }
}

/// Takes the partition away from the source and assigns it back, as a rebalance, a rejoin or a
/// lapsed group session does.
async fn take_the_partition_away(events: mpsc::UnboundedSender<RebalanceEvent>) {
    for event in [RebalanceEvent::Revoke, RebalanceEvent::Assign] {
        nervix_primitives::task::consume_budget().await;
        if events.send(event).is_err() {
            panic!("the source serves rebalances until the check ends");
        }
    }
}

fn a_rejected_record_is_read_again_whenever_its_group_takes_the_partition_away() {
    shuttle::future::block_on(async {
        let context = KafkaConsumerContext::default();
        let mut consumer = FakeConsumer {
            fetching: Some(REJECTED),
            committed: None,
        };
        let (events, mut served) = mpsc::unbounded_channel();
        let coordinator = nervix_primitives::task::spawn(take_the_partition_away(events));
        // The source reads the record it then rejects, while the group may already be taking the
        // partition away.
        let (_idle, mut nothing) = mpsc::unbounded_channel();
        let read = consumer.read(&mut nothing, &context).await;
        assert_eq!(read, REJECTED);

        consumer.reject(&context);
        let read_again = consumer.read(&mut served, &context).await;

        assert_eq!(
            read_again, REJECTED,
            "the first record read after the rejection is the rejected one"
        );
        coordinator.await.assured(CHECK_TASK_JOINS);
    });
}

#[test]
fn shuttle_a_rejected_record_is_read_again_whenever_its_group_takes_the_partition_away() {
    check_interleavings(
        a_rejected_record_is_read_again_whenever_its_group_takes_the_partition_away,
    );
}

fn a_commit_past_the_rejected_record_lets_the_group_resume_from_the_committed_offset() {
    shuttle::future::block_on(async {
        let context = KafkaConsumerContext::default();
        let mut consumer = FakeConsumer {
            fetching: Some(REJECTED),
            committed: None,
        };
        let (_idle, mut nothing) = mpsc::unbounded_channel();
        let read = consumer.read(&mut nothing, &context).await;
        assert_eq!(read, REJECTED);
        consumer.reject(&context);
        let (events, mut served) = mpsc::unbounded_channel();
        let coordinator = nervix_primitives::task::spawn(take_the_partition_away(events));
        let read_again = consumer.read(&mut served, &context).await;
        assert_eq!(read_again, REJECTED);

        // The source acknowledges the record it read again, and the group commits past it.
        let committed = REJECTED
            .checked_add(1)
            .assured("a check partition holds only a few records");
        consumer.committed = Some(committed);
        let outstanding = context.confirm_commit(&[position(committed)]);
        assert!(!outstanding, "the commit passed the only rejected record");
        let next = consumer.read(&mut served, &context).await;

        assert_eq!(
            next, committed,
            "the record after the committed one is read next, whether the partition was assigned \
             again or not"
        );
        coordinator.await.assured(CHECK_TASK_JOINS);
    });
}

#[test]
fn shuttle_a_commit_past_the_rejected_record_lets_the_group_resume_from_the_committed_offset() {
    check_interleavings(
        a_commit_past_the_rejected_record_lets_the_group_resume_from_the_committed_offset,
    );
}
