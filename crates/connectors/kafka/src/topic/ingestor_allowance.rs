//! Kafka consumer-group control while the host holds an ingestor's reads back: during its read
//! allowance hold, and while a payload it read waits for the node's extension workers.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The native fetch pauses taken during a hold and polling the same consumer's group
//!   events while its partitions are paused.
//! - **Depends on.** The connector contract, Kafka control execution, the primitive boundary,
//!   and rust-rdkafka's stream and partition assignment APIs.
//! - **Must not know.** Why the host holds the reads, how it calculates its allowance, graph
//!   acknowledgements, relays, or schedules.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "services one retained Kafka consumer during a read hold"
    )
)]

use std::{
    collections::BTreeSet,
    convert::Infallible,
    future::{Future, pending},
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use async_trait::async_trait;
use error_stack::{Report, ResultExt as _};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{IngestorError, IngestorReadHold};
use nervix_primitives::{
    sync::{Notify, StdArc},
    time::{Instant, sleep_until},
};
use rdkafka::{
    config::ClientConfig, consumer::Consumer as _, error::RDKafkaErrorCode,
    topic_partition_list::TopicPartitionList,
};

use super::{InstanceOffsetMode, KAFKA, KafkaTopicIngestor, KafkaTopicIngestorError};

/// rust-rdkafka's stream consumer wakes every reader waiting on its stream twice per
/// `max.poll.interval.ms`, so a reader waiting for records polls the driver at least every half
/// interval and keeps the other half for its own scheduling. A held consumer's service is due on
/// the same cadence, half an interval after the consumer's last poll, which leaves the other half,
/// as the driver leaves it to a woken reader, for the service job's executor admission and its
/// native poll.
#[cfg_attr(
    nervix_lint,
    nervix::constant(
        fixed,
        reason = "rust-rdkafka's StreamConsumer wakes a reader waiting on its stream at half of \
                  librdkafka's max.poll.interval.ms"
    )
)]
const STREAM_WAKES_PER_POLL_INTERVAL: u32 = 2;

pub(super) struct KafkaAllowanceHold {
    /// Half the consumer's poll interval: how long after its last poll its group service is due.
    service_delay: Duration,
    /// When the group service is due, counted from the last poll the ingestor observed; `None`
    /// before the consumer's first poll since it subscribed, when the service is due at once.
    next_service: Option<Instant>,
    /// Each topic partition the current hold may have paused, including revoked partitions
    /// whose native pause can survive a rebalance.
    paused: Option<BTreeSet<i32>>,
    /// An admitted poll can outlive the host dropping its future at the end of a hold.
    poll_in_flight: bool,
}

impl KafkaAllowanceHold {
    pub(super) fn new(config: &ClientConfig) -> Result<Self, Report<KafkaTopicIngestorError>> {
        let native = config
            .create_native_config()
            .change_context(KafkaTopicIngestorError::Initialize)?;
        let interval = native
            .get("max.poll.interval.ms")
            .change_context(KafkaTopicIngestorError::Initialize)?;
        let milliseconds = interval.parse::<u64>().assured(
            "librdkafka validates max.poll.interval.ms as an integer and renders decimal digits",
        );
        Ok(Self {
            service_delay: Duration::from_millis(milliseconds) / STREAM_WAKES_PER_POLL_INTERVAL,
            next_service: None,
            paused: None,
            poll_in_flight: false,
        })
    }

    pub(super) fn subscribed(&mut self) {
        self.next_service = None;
    }

    /// Records a driver poll that has just returned: one that handed a read its record, or this
    /// hold's own service poll. A read that waits out its batch deadline or ends at a failure
    /// records none, so the service stays due from the latest poll recorded, which is never later
    /// than the driver's actual last poll.
    pub(super) fn polled(&mut self) {
        self.next_service = Some(Instant::now().checked_add(self.service_delay).assured(
            "half of librdkafka's maximum one-day poll interval fits the monotonic clock",
        ));
    }

    pub(super) fn is_paused(&self) -> bool {
        self.paused.is_some()
    }
}

/// The driver's queue callback and half-interval wakeups ask for the next control poll. A wake
/// during the executor job stays pending until the owner can receive it.
#[derive(Default)]
struct GroupPollWake(Notify);

impl Wake for GroupPollWake {
    fn wake(self: StdArc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &StdArc<Self>) {
        self.0.notify_one();
    }
}

#[async_trait]
impl IngestorReadHold for KafkaTopicIngestor {
    async fn maintain(&mut self, until: Option<Instant>) -> Report<IngestorError> {
        match self.service_held_group(until).await {
            Err(error) => error.change_context(IngestorError::Read { connector: KAFKA }),
            Ok(never) => match never {},
        }
    }
}

impl KafkaTopicIngestor {
    /// Services the held consumer's group until the host stops polling. A hold with a deadline
    /// needs no service it would end before, and a hold without one, a payload waiting for the
    /// node's extension workers, is serviced for as long as it lasts.
    async fn service_held_group(
        &mut self,
        until: Option<Instant>,
    ) -> Result<Infallible, Report<KafkaTopicIngestorError>> {
        let hold = self
            .allowance_hold
            .as_mut()
            .verified("only a consumer with allowance maintenance offers the hook");
        if let Some(next_service) = hold.next_service {
            if let Some(until) = until
                && next_service >= until
            {
                return pending().await;
            }
            sleep_until(next_service).await;
        }
        if let Some(until) = until
            && Instant::now() >= until
        {
            return pending().await;
        }
        hold.paused.get_or_insert_with(BTreeSet::new);

        let topic = self.topic.clone();
        let mut assignment = self
            .control
            .run(&self.consumer, {
                let topic = topic.clone();
                move |consumer, _budget| {
                    consumer.assignment().change_context_lazy(|| {
                        KafkaTopicIngestorError::ReadAssignment {
                            topic: topic.as_str().to_owned(),
                        }
                    })
                }
            })
            .await?;
        let wake = StdArc::new(GroupPollWake::default());
        let waker = Waker::from(StdArc::clone(&wake));
        let reader = self.consumer.clone();
        let mut read = Box::pin(async move {
            reader.recv().await.err().assured(
                "the assignment is paused before each poll, and the pinned stream yields as soon \
                 as it handles a rebalance",
            )
        });

        loop {
            nervix_primitives::task::consume_budget().await;
            if let Some(until) = until
                && Instant::now() >= until
            {
                return pending().await;
            }
            hold.paused
                .as_mut()
                .verified("the hold recorded its native pause before admitting work")
                .extend(
                    assignment
                        .elements()
                        .iter()
                        .map(|partition| partition.partition()),
                );
            // The blocking work owns exactly one poll. A native pause retires prefetched copies
            // and rewinds fetching to the next unread offset. A rebalance callback can add a
            // partition during this poll, but the stream yields before its next attempt. Refresh
            // the assignment in the same control job, ready to pause it before that next poll.
            let poll_waker = waker.clone();
            let poll_topic = topic.clone();
            hold.poll_in_flight = true;
            (read, assignment) = self
                .control
                .run(&self.consumer, move |consumer, budget| {
                    if assignment.count() > 0 {
                        consumer.pause(&assignment).change_context_lazy(|| {
                            KafkaTopicIngestorError::PauseForAllowance {
                                topic: poll_topic.as_str().to_owned(),
                            }
                        })?;
                        for partition in assignment.elements() {
                            partition.error().change_context_lazy(|| {
                                KafkaTopicIngestorError::PauseForAllowance {
                                    topic: poll_topic.as_str().to_owned(),
                                }
                            })?;
                        }
                    }
                    budget.remaining()?;
                    let mut context = Context::from_waker(&poll_waker);
                    match read.as_mut().poll(&mut context) {
                        Poll::Pending => {
                            budget.remaining()?;
                            let assignment = consumer.assignment().change_context_lazy(|| {
                                KafkaTopicIngestorError::ReadAssignment {
                                    topic: poll_topic.as_str().to_owned(),
                                }
                            })?;
                            Ok((read, assignment))
                        }
                        Poll::Ready(error) => Err(Self::receive_failure(&poll_topic, error)),
                    }
                })
                .await?;
            hold.poll_in_flight = false;
            hold.polled();
            wake.0.notified().await;
        }
    }

    /// A settlement or lifecycle operation can follow a measured hold without another read.
    /// Complete any callback already admitted by the hold before touching the consumer.
    pub(super) async fn finish_allowance_poll(
        &mut self,
    ) -> Result<(), Report<KafkaTopicIngestorError>> {
        if let Some(hold) = &mut self.allowance_hold
            && hold.poll_in_flight
        {
            self.control.wait_for_turn().await?;
            hold.poll_in_flight = false;
        }
        Ok(())
    }

    /// Release only this hold's pauses and wait for its admitted control poll before reading.
    pub(super) async fn resume_after_allowance(
        &mut self,
    ) -> Result<(), Report<KafkaTopicIngestorError>> {
        let hold = self
            .allowance_hold
            .as_mut()
            .verified("a held consumer has allowance maintenance");
        let paused = hold
            .paused
            .as_ref()
            .verified("the caller found a possible native pause");
        let topology = match &self.offset_mode {
            InstanceOffsetMode::ConsumerGroup {
                committed_partitions: Some(read),
                ..
            } => read.paused_for_topology.as_ref(),
            _ => None,
        };
        let topic = self.topic.as_str().to_owned();
        let mut partitions = TopicPartitionList::new();
        for &partition in paused {
            if !topology.is_some_and(|holds| holds.contains(&partition)) {
                partitions.add_partition(&topic, partition);
            }
        }
        self.control
            .run(&self.consumer, move |consumer, _budget| {
                if partitions.count() == 0 {
                    return Ok(());
                }
                consumer.resume(&partitions).change_context_lazy(|| {
                    KafkaTopicIngestorError::ResumeAfterAllowance {
                        topic: topic.clone(),
                    }
                })?;
                for partition in partitions.elements() {
                    if let Err(error) = partition.error()
                        && error.rdkafka_error_code() != Some(RDKafkaErrorCode::UnknownPartition)
                    {
                        return Err(Report::new(error).change_context(
                            KafkaTopicIngestorError::ResumeAfterAllowance { topic },
                        ));
                    }
                }
                Ok(())
            })
            .await?;
        hold.poll_in_flight = false;
        hold.paused = None;
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
#[path = "ingestor_allowance_tests.rs"]
mod tests;
