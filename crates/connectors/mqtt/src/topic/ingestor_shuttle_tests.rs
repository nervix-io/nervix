//! How an MQTT read meets the host's cancellation, explored under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants a read holds while the host cancels it at any point of a batch that
//!   a broker task delivers publishes into: every publish the broker delivered reaches the host
//!   exactly once and in order, from the read or from the connection's next read, and a batch
//!   that is not full ends only because the host cancelled it, unless its batch timeout passed.
//! - **Depends on.** The ingestor's read and its publish seam, an unbounded channel as the
//!   connection's read-ahead, the model harness's Shuttle runner, and the task labels and timeout
//!   triggers of Shuttle's Tokio.
//! - **Must not know.** The MQTT client, the broker's protocol, or the host loop that cancels a
//!   read.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{num::NonZeroUsize, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    IngestorBatch, IngestorBatchRequest, IngestorMessage as _, IngestorReadCancellation,
};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_primitives::{
    sync::{Arc, mpsc},
    task::JoinHandle,
    time::{clear_triggers, trigger_timeouts},
};
use rumqttc::{Publish, QoS};

use super::{
    MqttTopicIngestor, MqttTopicIngestorError, MqttTopicIngestorMessage, MqttTopicIngestorResult,
    PublishRead,
};

const CHECK_TASK_JOINS: &str =
    "a check task that panics fails the execution before its join returns";

/// The publishes a batch of the checks holds.
const BATCH_MESSAGES: usize = 3;

/// The batch timeout of the checks. Shuttle does not measure it: it passes only when a check's
/// clock expires it.
const BATCH_TIMEOUT: Duration = Duration::from_secs(3_600);

/// A connection a broker task delivers publishes into through a channel, as the connection's
/// driver hands them to the ingestor through its read-ahead: a read that stops waiting for the next
/// publish loses nothing the connection's next read would have returned.
struct DeliveredPublishes {
    publishes: mpsc::UnboundedReceiver<Publish>,
}

impl PublishRead for DeliveredPublishes {
    async fn next_publish(&mut self) -> MqttTopicIngestorResult<Publish> {
        match self.publishes.recv().await {
            Some(publish) => Ok(publish),
            None => Err(Report::new(MqttTopicIngestorError::Receive)),
        }
    }
}

/// Marks the reading task, whose batch deadline a check's clock expires.
#[derive(Debug, Clone, Copy)]
struct ReadDeadline;

/// The ids of the publishes `messages` holds.
fn ids(messages: &[MqttTopicIngestorMessage]) -> Vec<u8> {
    let mut ids = Vec::with_capacity(messages.len());
    for message in messages {
        let [id] = message.payload() else {
            panic!("a check's payload is the one byte of its publish's id");
        };
        ids.push(*id);
    }
    ids
}

/// Delivers the publishes `1..=delivered` into the connection, one scheduling point apart, and
/// returns the delivering end, so the connection stays open after the last.
fn deliver(
    deliveries: mpsc::UnboundedSender<Publish>,
    delivered: u8,
) -> JoinHandle<mpsc::UnboundedSender<Publish>> {
    nervix_primitives::task::spawn(async move {
        for id in 1..=delivered {
            nervix_primitives::task::consume_budget().await;
            deliveries
                .send(Publish::new("events", QoS::AtLeastOnce, vec![id], None))
                .assured("the reading task holds the connection until the broker returned");
        }
        deliveries
    })
}

/// A read of a batch of up to three publishes, from a connection a broker task delivers
/// `delivered` publishes into, which the host cancels at any point: before the read takes its
/// first publish, while it waits for one, between two, or after it returned. When
/// `deadline_passes`, a clock also expires the batch timeout at any point.
fn a_cancelled_read_hands_over_every_publish_the_broker_delivered(
    delivered: u8,
    deadline_passes: bool,
) {
    // Timeout triggers are thread-local rather than per execution, so every execution starts
    // without the trigger an earlier one registered.
    clear_triggers();
    shuttle::future::block_on(async move {
        shuttle::current::set_label_for_task(shuttle::current::me(), ReadDeadline);
        let cancellation = Arc::new(IngestorReadCancellation::new());
        let (deliveries, publishes) = mpsc::unbounded_channel();
        let broker = deliver(deliveries, delivered);
        let host = nervix_primitives::task::spawn({
            let cancellation = Arc::clone(&cancellation);
            async move {
                cancellation.cancel();
            }
        });
        let mut clock = None;
        if deadline_passes {
            clock = Some(nervix_primitives::task::spawn(async {
                trigger_timeouts(|labels| labels.get::<ReadDeadline>().is_some());
            }));
        }
        let mut connection = DeliveredPublishes { publishes };
        let request = IngestorBatchRequest {
            max_messages: NonZeroUsize::new(BATCH_MESSAGES)
                .assured("a batch of the checks holds three publishes"),
            batch_timeout: Some(BATCH_TIMEOUT),
        };

        let batch = MqttTopicIngestor::read_batch(&mut connection, request, &cancellation)
            .await
            .assured("a connection the broker keeps open fails no read");

        host.await.assured(CHECK_TASK_JOINS);
        if let Some(clock) = clock {
            clock.await.assured(CHECK_TASK_JOINS);
        }
        drop(broker.await.assured(CHECK_TASK_JOINS));
        let (mut handed_over, interrupted) = match batch {
            IngestorBatch::Messages(messages) => (ids(&messages), false),
            IngestorBatch::Interrupted { gathered } => (ids(&gathered), true),
            IngestorBatch::Oversized { payloads } => {
                panic!("an MQTT read drops no publish for its size, yet it dropped {payloads}")
            }
            IngestorBatch::ResumeRequired | IngestorBatch::Closed => {
                panic!("a read from a connection neither resumes nor closes")
            }
        };
        let full = handed_over.len() == BATCH_MESSAGES;
        if !full && !deadline_passes {
            assert!(
                interrupted,
                "a batch of {handed_over:?} that is not full ended before the host cancelled it"
            );
        }
        // The broker has returned its end, so the connection now holds exactly the publishes the
        // read left for the next one.
        loop {
            nervix_primitives::task::consume_budget().await;
            let Some(publish) = connection.publishes.recv().await else {
                break;
            };
            let [id] = &publish.payload[..] else {
                panic!("a check's payload is the one byte of its publish's id");
            };
            handed_over.push(*id);
        }
        let mut expected = Vec::with_capacity(usize::from(delivered));
        expected.extend(1..=delivered);
        assert_eq!(
            handed_over, expected,
            "every publish the broker delivered reaches the host exactly once, in order, from the \
             read or from the connection's next read"
        );
    });
}

/// A read the host cancels at any point hands it every publish it took, whether the broker
/// delivers fewer publishes than the batch holds or enough to fill it, and a batch that is not
/// full ends only because the host cancelled it.
#[test]
fn shuttle_a_cancelled_read_hands_over_every_publish_the_broker_delivered() {
    check_interleavings(|| {
        a_cancelled_read_hands_over_every_publish_the_broker_delivered(2, false)
    });
    check_interleavings(|| {
        a_cancelled_read_hands_over_every_publish_the_broker_delivered(3, false)
    });
}

/// A read whose batch timeout passes at any point, racing the host's cancellation and the
/// broker's deliveries, still hands the host every publish it took, exactly once.
#[test]
fn shuttle_a_read_whose_batch_timeout_races_the_cancellation_hands_over_every_publish() {
    check_interleavings(|| a_cancelled_read_hands_over_every_publish_the_broker_delivered(2, true));
}
