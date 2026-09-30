//! A Kafka consumer group member that a scenario runs beside Nervix's own consumers.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One librdkafka consumer subscribed to one topic in a named group, the thread that
//!   polls it so the group can rebalance, the partitions the group currently assigns it, and
//!   leaving the group.
//! - **Depends on.** rust-rdkafka through the Kafka connector's testing re-export, and the watch
//!   channel, synchronous channel, blocking tasks and threads of the primitives boundary.
//! - **Must not know.** Nervix, its ingestors, or the offsets they commit.
//!
//! # Taking a partition
//!
//! librdkafka's default assignor is the range assignor, which hands a topic's partitions to the
//! group's members in the order of their member ids, and a member id begins with its client id. The
//! member's client id sorts before `rdkafka`, the id librdkafka gives a Nervix consumer whose client
//! configuration names none, so the group assigns this member the topic's first partitions. It
//! reads the messages of those partitions and never commits an offset, so the group's committed
//! position stays exactly where Nervix's consumers left it.

use std::{collections::BTreeSet, io, time::Duration};

use nervix_connector_kafka::testing_rdkafka::{
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer},
};
use nervix_primitives::{
    sync::{
        blocking::mpsc::{self, TryRecvError},
        watch,
    },
    task::spawn_blocking,
    thread::{self, JoinHandle},
};

/// Sorts before `rdkafka`, the client id of a Nervix consumer that sets none.
const EXTERNAL_MEMBER_CLIENT_ID: &str = "external-group-member";

/// How long one poll waits for the queue, and so how quickly the member answers a rebalance or a
/// request to leave.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) struct ExternalKafkaGroupMember {
    topic: String,
    /// Sending on it, or dropping it with the member, makes the polling thread leave the group.
    leave: mpsc::Sender<()>,
    /// The partitions of `topic` the group assigned the member at its latest poll.
    assignment: watch::Receiver<BTreeSet<i32>>,
    poller: JoinHandle<()>,
}

impl ExternalKafkaGroupMember {
    /// Subscribes a new member of `group` to `topic` and starts polling it.
    pub(crate) fn join(mut config: ClientConfig, group: &str, topic: &str) -> io::Result<Self> {
        let consumer: BaseConsumer = config
            .set("group.id", group)
            .set("client.id", EXTERNAL_MEMBER_CLIENT_ID)
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .create()
            .map_err(io::Error::other)?;
        consumer.subscribe(&[topic]).map_err(io::Error::other)?;
        let (leave, leave_requested) = mpsc::channel();
        let (assignment_updates, assignment) = watch::channel(BTreeSet::new());
        let poller_topic = topic.to_string();
        let poller = thread::Builder::new()
            .name("external-kafka-member".to_string())
            .spawn(move || {
                poll_until_left(
                    &consumer,
                    &poller_topic,
                    &leave_requested,
                    &assignment_updates,
                );
            })?;
        Ok(Self {
            topic: topic.to_string(),
            leave,
            assignment,
            poller,
        })
    }

    /// Waits until the group assigns the member `partition` of `topic`.
    pub(crate) async fn wait_until_assigned(
        &mut self,
        topic: &str,
        partition: i32,
        budget: Duration,
    ) -> io::Result<()> {
        if topic != self.topic {
            return Err(io::Error::other(format!(
                "the external member subscribes to topic '{}', not '{topic}'",
                self.topic
            )));
        }
        let assigned = self
            .assignment
            .wait_for(|partitions| partitions.contains(&partition));
        let elapsed = match nervix_primitives::time::timeout(budget, assigned).await {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(_)) => {
                return Err(io::Error::other(
                    "the external member stopped polling before it was assigned the partition",
                ));
            }
            Err(elapsed) => elapsed,
        };
        let held = self.assignment.borrow().clone();
        Err(io::Error::other(format!(
            "the external member was not assigned topic '{topic}' partition {partition} within \
             {budget:?} ({elapsed}); it holds partitions {held:?}"
        )))
    }

    /// Leaves the group and waits until the member's consumer has closed.
    pub(crate) async fn leave(self) -> io::Result<()> {
        self.leave
            .send(())
            .map_err(|_| io::Error::other("the external member stopped polling on its own"))?;
        let poller = self.poller;
        let joined = spawn_blocking(move || poller.join())
            .await
            .map_err(io::Error::other)?;
        joined.map_err(|_| io::Error::other("the external member's polling thread panicked"))
    }
}

/// Polls until the member is asked to leave, publishing its assignment after every poll. The
/// polling thread then drops the consumer, which closes it and leaves the group.
fn poll_until_left(
    consumer: &BaseConsumer,
    topic: &str,
    leave_requested: &mpsc::Receiver<()>,
    assignment_updates: &watch::Sender<BTreeSet<i32>>,
) {
    loop {
        match leave_requested.try_recv() {
            Err(TryRecvError::Empty) => {}
            Ok(()) | Err(TryRecvError::Disconnected) => return,
        }
        // Serving the queue is what answers the group's rebalances. A message the member reads is
        // dropped without committing its offset.
        if let Some(Err(error)) = consumer.poll(POLL_INTERVAL) {
            eprintln!("external Kafka group member poll failed: {error}");
        }
        match consumer.assignment() {
            Ok(current) => {
                let partitions = current
                    .elements_for_topic(topic)
                    .iter()
                    .map(|element| element.partition())
                    .collect::<BTreeSet<_>>();
                assignment_updates.send_replace(partitions);
            }
            Err(error) => eprintln!("external Kafka group member assignment read failed: {error}"),
        }
    }
}
