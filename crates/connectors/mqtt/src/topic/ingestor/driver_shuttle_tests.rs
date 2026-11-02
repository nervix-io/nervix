//! How an MQTT ingestor connection's driver meets the ingestor's reads and acknowledgements, explored
//! under Shuttle.
//!
//! Layer: test harness.
//! - **Owns.** The invariants a connection holds at any interleaving of a broker task's deliveries,
//!   the connection's end and the ingestor's acknowledgement: an acknowledgement returns only once
//!   the event loop wrote every acknowledgement it requested, it never waits on a driver that
//!   stopped polling and fails only when the driver cannot poll before the ingestor reads again, and
//!   admitted publishes retain packet order, and a discarded publish's acknowledgement and size
//!   outcome survive cancellation exactly once without losing its ordinary neighbours.
//! - **Depends on.** The connection and its driver, a fake event loop that writes the
//!   acknowledgements the ingestor requested ahead of what it reads, as the client's does, and the
//!   model harness's Shuttle runner.
//! - **Must not know.** The MQTT client, the broker's protocol, or the host that acknowledges.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    io,
    num::{NonZeroU16, NonZeroU32, NonZeroUsize},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    IngestorBatch, IngestorBatchRequest, IngestorMessage as _, IngestorReadCancellation,
};
use nervix_model_harness::shuttle::check_interleavings;
use nervix_models::{ClientConfigEntry, MqttClientConfig};
use nervix_primitives::{
    sync::{Arc, mpsc},
    task::JoinHandle,
};
use rumqttc::{
    ClientError, ConnectionError, Event, Incoming, ManualAck, Outgoing, PubAck, Publish, QoS,
    Request,
};

use super::{AcknowledgementRequests, ConnectionEvents, MqttConnection};

/// The largest complete publish a client that leaves `max_packet_size` out admits, below the checks'
/// 20 KiB publishes, so those are discards.
fn default_admission() -> NonZeroU32 {
    let client = [ClientConfigEntry {
        key: "addr".to_string(),
        value: "mqtt://127.0.0.1:1883".to_string(),
    }];
    MqttClientConfig::parse(&client)
        .assured("an address alone is a valid MQTT client configuration")
        .max_packet_size()
}
use crate::topic::ingestor::{
    IngestorAcknowledgement, MqttTopicIngestor, MqttTopicIngestorError, MqttTopicIngestorPosition,
    PublishRead as _, PublishReadOutcome, metrics_fixture::IngestorMetricsFixture,
};

const CHECK_TASK_JOINS: &str =
    "a check task that panics fails the execution before its join returns";

/// A connection's event loop, which writes every acknowledgement the ingestor requested before it
/// reads what the broker delivered, as the client's event loop admits its requests ahead of the
/// network, and ends once the broker's end of the connection closes.
struct FakeEventLoop {
    requests: mpsc::UnboundedReceiver<u16>,
    deliveries: mpsc::UnboundedReceiver<Publish>,
    /// Every acknowledgement the loop wrote to the connection, in the order it wrote them.
    written: mpsc::UnboundedSender<u16>,
}

impl ConnectionEvents for FakeEventLoop {
    async fn next_event(&mut self) -> Result<Event, ConnectionError> {
        let closed = || {
            ConnectionError::Io(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "the broker closed the connection",
            ))
        };
        nervix_primitives::select! {
            biased;
            requested = self.requests.recv() => {
                let Some(packet_id) = requested else {
                    return Err(closed());
                };
                self.written
                    .send(packet_id)
                    .assured("the check keeps the record of writes until it ends");
                Ok(Event::Outgoing(Outgoing::PubAck(packet_id)))
            }
            delivered = self.deliveries.recv() => match delivered {
                Some(publish) => Ok(Event::Incoming(Incoming::Publish(publish))),
                None => Err(closed()),
            },
        }
    }
}

/// A client that hands every requested acknowledgement to the fake event loop, and fails once the
/// loop is gone, as the client's request channel ends with its event loop.
struct FakeClient {
    requests: mpsc::UnboundedSender<u16>,
}

impl AcknowledgementRequests for FakeClient {
    async fn request(&self, acknowledgement: &ManualAck) -> Result<(), ClientError> {
        let packet_id = match acknowledgement {
            ManualAck::PubAck(ack) => ack.pkid,
            ManualAck::PubRec(rec) => rec.pkid,
        };
        self.requests.send(packet_id).map_err(|_| {
            ClientError::RequestChannelDisconnected(Box::new(Request::PubAck(PubAck::new(
                packet_id, None,
            ))))
        })
    }
}

/// A publish whose payload and packet identifier are both `id`.
fn publish(id: u8, qos: QoS) -> Publish {
    let mut publish = Publish::new("events", qos, vec![id], None);
    if qos != QoS::AtMostOnce {
        publish.pkid = u16::from(id);
    }
    publish
}

/// The id a check's publish carries as its one byte of payload.
fn id(publish: &Publish) -> u8 {
    let [id] = &publish.payload[..] else {
        panic!("a check's payload is the one byte of its publish's id");
    };
    *id
}

/// A connection over a fake event loop with a read-ahead of `read_ahead` publishes, the broker's
/// end to deliver publishes through, and the record of the acknowledgements the loop wrote.
fn connection(
    read_ahead: usize,
) -> (
    MqttConnection<FakeClient>,
    mpsc::UnboundedSender<Publish>,
    mpsc::UnboundedReceiver<u16>,
) {
    let (requests, requested) = mpsc::unbounded_channel();
    let (deliveries, delivered) = mpsc::unbounded_channel();
    let (written, record) = mpsc::unbounded_channel();
    let events = FakeEventLoop {
        requests: requested,
        deliveries: delivered,
        written,
    };
    let read_ahead =
        u16::try_from(read_ahead).assured("a check's read-ahead fits MQTT's Receive Maximum");
    let read_ahead = NonZeroU16::new(read_ahead).assured("a check's read-ahead holds a publish");
    // The checks explore the driver's interleavings rather than its deadline, so the connection
    // keeps no keep-alive.
    let connection = MqttConnection::start(
        FakeClient { requests },
        events,
        IngestorAcknowledgement::Manual {
            receive_maximum: read_ahead,
        },
        None,
        default_admission(),
        IngestorMetricsFixture::new().counter(),
    );
    (connection, deliveries, record)
}

/// Delivers `publishes` one scheduling point apart, and returns the broker's end of the connection
/// so the connection stays open after the last.
fn deliver(
    deliveries: mpsc::UnboundedSender<Publish>,
    publishes: Vec<Publish>,
) -> JoinHandle<mpsc::UnboundedSender<Publish>> {
    nervix_primitives::task::spawn(async move {
        for publish in publishes {
            nervix_primitives::task::consume_budget().await;
            deliveries
                .send(publish)
                .assured("the fake event loop reads the connection until the check ends");
        }
        deliveries
    })
}

/// Every acknowledgement the loop has written so far.
fn written(record: &mut mpsc::UnboundedReceiver<u16>) -> Vec<u16> {
    let mut written = Vec::new();
    loop {
        let Ok(packet_id) = record.try_recv() else {
            return written;
        };
        written.push(packet_id);
    }
}

/// An ingestor reads three publishes and acknowledges them while the broker delivers two more.
fn an_acknowledgement_returns_once_the_loop_wrote_every_acknowledgement_it_requested() {
    shuttle::future::block_on(async {
        let (mut connection, deliveries, mut record) = connection(8);
        let mut publishes = Vec::new();
        for id in 1..=5 {
            publishes.push(publish(id, QoS::AtLeastOnce));
        }
        let broker = deliver(deliveries, publishes);

        let mut read = Vec::new();
        for _ in 0..3 {
            let publish = connection
                .next_publish()
                .await
                .assured("a connection the broker keeps open fails no read");
            let PublishReadOutcome::Message(publish) = publish else {
                panic!("the ordinary fixture yields an admitted publish");
            };
            let publish = *publish;
            read.push(MqttTopicIngestorPosition { publish });
        }
        connection
            .acknowledge(&read)
            .await
            .assured("a connection that keeps polling writes every acknowledgement");

        assert_eq!(
            written(&mut record),
            [1, 2, 3],
            "an acknowledgement returns only once the loop wrote every acknowledgement it \
             requested, and nothing more"
        );
        let mut ids = Vec::new();
        for position in &read {
            ids.push(id(position.publish()));
        }
        for _ in 0..2 {
            let publish = connection
                .next_publish()
                .await
                .assured("a connection the broker keeps open fails no read");
            let PublishReadOutcome::Message(publish) = publish else {
                panic!("the ordinary fixture yields an admitted publish");
            };
            let publish = *publish;
            ids.push(id(&publish));
        }
        assert_eq!(
            ids,
            [1, 2, 3, 4, 5],
            "every publish the broker delivered reaches the source exactly once, in order"
        );
        drop(broker.await.assured(CHECK_TASK_JOINS));
    });
}

/// A broker observation that the driver reached the following QoS 1 publish, after classifying
/// the complete QoS 0 flood. It cannot unblock a driver waiting for read-ahead room.
struct OverflowObservedEventLoop {
    events: FakeEventLoop,
    following: mpsc::Sender<()>,
}

impl ConnectionEvents for OverflowObservedEventLoop {
    async fn next_event(&mut self) -> Result<Event, ConnectionError> {
        let event = self.events.next_event().await?;
        if let Event::Incoming(Incoming::Publish(publish)) = &event
            && publish.pkid == 200
        {
            self.following
                .send(())
                .await
                .assured("the check waits for its delivery witness");
        }
        Ok(event)
    }
}

/// QoS 0 overflow cannot stop polling or occupy the capacity the following QoS 1 publish needs.
fn an_acknowledgement_stays_writable_during_qos_zero_overflow() {
    shuttle::future::block_on(async {
        let metrics = IngestorMetricsFixture::new();
        let (requests, requested) = mpsc::unbounded_channel();
        let (deliveries, delivered) = mpsc::unbounded_channel();
        let (written_to, mut record) = mpsc::unbounded_channel();
        let (following, mut reached) = mpsc::channel(1);
        let events = OverflowObservedEventLoop {
            events: FakeEventLoop {
                requests: requested,
                deliveries: delivered,
                written: written_to,
            },
            following,
        };
        let mut connection = MqttConnection::start(
            FakeClient { requests },
            events,
            IngestorAcknowledgement::Manual {
                receive_maximum: NonZeroU16::new(65)
                    .assured("the sequential window includes headroom"),
            },
            None,
            default_admission(),
            metrics.counter(),
        );
        let first = publish(1, QoS::AtLeastOnce);
        deliveries
            .send(first.clone())
            .assured("the driver keeps its receiver");
        let read = connection
            .next_publish()
            .await
            .assured("the first publish arrives");
        assert!(matches!(read, PublishReadOutcome::Message(read) if *read == first));

        let mut flood = Vec::new();
        for id in 2..=67 {
            flood.push(publish(id, QoS::AtMostOnce));
        }
        let following = publish(200, QoS::AtLeastOnce);
        flood.push(following.clone());
        let broker = deliver(deliveries, flood);
        reached
            .recv()
            .await
            .assured("the driver keeps polling after QoS 0 overflow");
        connection
            .acknowledge(&[MqttTopicIngestorPosition { publish: first }])
            .await
            .assured("the first QoS 1 acknowledgement stays writable");
        assert_eq!(written(&mut record), [1]);

        for id in 2..=65 {
            let read = connection
                .next_publish()
                .await
                .assured("the admitted prefix is retained");
            let expected = publish(id, QoS::AtMostOnce);
            assert!(matches!(read, PublishReadOutcome::Message(read) if *read == expected));
        }
        let read = connection
            .next_publish()
            .await
            .assured("QoS 0 cannot take the following QoS 1 slot");
        assert!(matches!(read, PublishReadOutcome::Message(read) if *read == following));
        connection
            .acknowledge(&[MqttTopicIngestorPosition { publish: following }])
            .await
            .assured("the following QoS 1 acknowledgement is written");
        assert_eq!(written(&mut record), [200]);
        assert_eq!(metrics.dropped(), 2);
        drop(broker.await.assured(CHECK_TASK_JOINS));
    });
}

/// Reading and acknowledging an admitted prefix preserves the order and full contents of the
/// remaining QoS 0 publishes.
fn admitted_publishes_keep_order_while_the_first_acknowledgement_is_written() {
    shuttle::future::block_on(async {
        let (mut connection, deliveries, mut record) = connection(1);
        let mut publishes = vec![publish(1, QoS::AtLeastOnce)];
        for id in 2..=3 {
            publishes.push(publish(id, QoS::AtMostOnce));
        }
        let broker = deliver(deliveries, publishes);
        let mut read = Vec::new();
        for _ in 0..2 {
            let publish = connection
                .next_publish()
                .await
                .assured("a connection the broker keeps open fails no read");
            let PublishReadOutcome::Message(publish) = publish else {
                panic!("the ordinary fixture yields an admitted publish");
            };
            let publish = *publish;
            read.push(MqttTopicIngestorPosition { publish });
        }

        connection.acknowledge(&read).await.assured(
            "a driver whose held publish the source made room for polls again and writes the \
             acknowledgement",
        );

        assert_eq!(
            written(&mut record),
            [1],
            "an acknowledgement returns only once the loop wrote the one acknowledgement it \
             requested"
        );
        let last = connection
            .next_publish()
            .await
            .assured("a connection the broker keeps open fails no read");
        let PublishReadOutcome::Message(last) = last else {
            panic!("the ordinary fixture yields an admitted publish");
        };
        let last = *last;
        let mut ids = Vec::new();
        for position in &read {
            ids.push(id(position.publish()));
        }
        ids.push(id(&last));
        assert_eq!(
            ids,
            [1, 2, 3],
            "every publish the broker delivered reaches the source exactly once, in order"
        );
        drop(broker.await.assured(CHECK_TASK_JOINS));
    });
}

/// An ingestor acknowledges a publish while the broker closes the connection at any point.
fn an_acknowledgement_whose_connection_ends_fails_with_the_loss() {
    shuttle::future::block_on(async {
        let (mut connection, deliveries, mut record) = connection(8);
        let broker = deliver(deliveries, vec![publish(1, QoS::AtLeastOnce)]);
        let first = connection
            .next_publish()
            .await
            .assured("the broker delivered the publish before it closed the connection");
        let PublishReadOutcome::Message(first) = first else {
            panic!("the ordinary fixture yields an admitted publish");
        };
        let first = *first;
        // The broker's end closes once the delivery task returns it, at any point of the
        // acknowledgement.
        let closing = nervix_primitives::task::spawn(async move {
            drop(broker.await.assured(CHECK_TASK_JOINS));
        });

        let acknowledged = connection
            .acknowledge(&[MqttTopicIngestorPosition { publish: first }])
            .await;

        match acknowledged {
            Ok(()) => {
                assert_eq!(
                    written(&mut record),
                    [1],
                    "an acknowledgement that returned was written"
                );
                let Err(error) = connection.next_publish().await else {
                    panic!("a connection the broker closed delivers nothing more");
                };
                assert!(
                    matches!(error.current_context(), MqttTopicIngestorError::Receive),
                    "the read after the end reports the lost connection: {error:?}"
                );
            }
            Err(error) => {
                assert!(
                    matches!(error.current_context(), MqttTopicIngestorError::Acknowledge),
                    "an acknowledgement whose connection ended fails as an acknowledgement: \
                     {error:?}"
                );
                assert!(
                    error.frames().any(|frame| matches!(
                        frame.downcast_ref::<MqttTopicIngestorError>(),
                        Some(MqttTopicIngestorError::Receive)
                    )),
                    "the lost connection is the cause beneath the acknowledgement: {error:?}"
                );
                assert!(
                    written(&mut record).is_empty(),
                    "an acknowledgement that failed was not written"
                );
            }
        }
        closing.await.assured(CHECK_TASK_JOINS);
    });
}

/// An ordinary prefix is acknowledged before its intervening discard starts. Cancellation of
/// either read must preserve packet order, one discard ACK and one size outcome.
fn a_cancelled_discard_preserves_acknowledgement_and_batch_order() {
    shuttle::future::block_on(async {
        let (mut connection, deliveries, mut record) = connection(8);
        let mut oversized = publish(2, QoS::AtLeastOnce);
        oversized.payload = vec![2; 20_480].into();
        let broker = deliver(
            deliveries,
            vec![
                publish(1, QoS::AtLeastOnce),
                oversized,
                publish(3, QoS::AtLeastOnce),
            ],
        );
        let cancellation = Arc::new(IngestorReadCancellation::new());
        let host = nervix_primitives::task::spawn({
            let cancellation = Arc::clone(&cancellation);
            async move {
                cancellation.cancel();
            }
        });
        let request = IngestorBatchRequest {
            max_messages: NonZeroUsize::new(3)
                .assured("the batch can gather the ordinary neighbours"),
            batch_timeout: Some(Duration::from_secs(3_600)),
        };
        let first = MqttTopicIngestor::read_batch(&mut connection, request, &cancellation)
            .await
            .assured("the broker keeps the connection open");
        host.await.assured(CHECK_TASK_JOINS);
        let prefix = match first {
            IngestorBatch::Messages(messages)
            | IngestorBatch::Interrupted { gathered: messages } => messages,
            _ => panic!("the first packet is an ordinary publish"),
        };
        let prefix = if prefix.is_empty() {
            match MqttTopicIngestor::read_batch(
                &mut connection,
                request,
                &IngestorReadCancellation::new(),
            )
            .await
            .assured("the uncancelled read takes the prefix")
            {
                IngestorBatch::Messages(messages) => messages,
                _ => panic!("the uncancelled batch ends at the discard"),
            }
        } else {
            prefix
        };
        assert_eq!(prefix.len(), 1);
        assert_eq!(prefix[0].payload(), &[1]);
        let positions = prefix
            .iter()
            .map(|message| message.position().clone())
            .collect::<Vec<_>>();
        connection
            .acknowledge(&positions)
            .await
            .assured("both queued acknowledgements can be written");

        let single = IngestorBatchRequest {
            max_messages: NonZeroUsize::MIN,
            batch_timeout: None,
        };
        let discard_cancellation = Arc::new(IngestorReadCancellation::new());
        let host = nervix_primitives::task::spawn({
            let cancellation = Arc::clone(&discard_cancellation);
            async move {
                cancellation.cancel();
            }
        });
        let size = MqttTopicIngestor::read_batch(&mut connection, single, &discard_cancellation)
            .await
            .assured("a discard read survives cancellation");
        host.await.assured(CHECK_TASK_JOINS);
        let size = match size {
            IngestorBatch::Interrupted { gathered } => {
                assert!(gathered.is_empty());
                MqttTopicIngestor::read_batch(
                    &mut connection,
                    single,
                    &IngestorReadCancellation::new(),
                )
                .await
                .assured("the next read completes the retained discard")
            }
            size => size,
        };
        assert!(matches!(size, IngestorBatch::Oversized { payloads } if payloads.get() == 1));
        let mut writes = written(&mut record);
        assert_eq!(
            writes,
            [1, 2],
            "the ordinary prefix is acknowledged before the discard, which is counted only after \
             its ACK is written"
        );
        let next = MqttTopicIngestor::read_batch(
            &mut connection,
            single,
            &IngestorReadCancellation::new(),
        )
        .await
        .assured("the following publish is retained");
        let IngestorBatch::Messages(messages) = next else {
            panic!("the discard notification is delivered once");
        };
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload(), &[3]);
        connection
            .acknowledge(&[messages[0].position().clone()])
            .await
            .assured("the following publish is acknowledged");
        writes.extend(written(&mut record));
        assert_eq!(writes, [1, 2, 3]);
        drop(broker.await.assured(CHECK_TASK_JOINS));
    });
}

#[test]
fn shuttle_a_cancelled_discard_preserves_acknowledgement_and_batch_order() {
    check_interleavings(a_cancelled_discard_preserves_acknowledgement_and_batch_order);
}

/// An acknowledgement returns only once the connection's event loop wrote every acknowledgement it
/// requested, while the driver keeps reading what the broker delivers for the ingestor's next reads.
#[test]
fn shuttle_an_acknowledgement_returns_once_the_loop_wrote_every_acknowledgement_it_requested() {
    check_interleavings(
        an_acknowledgement_returns_once_the_loop_wrote_every_acknowledgement_it_requested,
    );
}

/// An acknowledgement remains writable after the driver drops a QoS 0 flood, and the following
/// QoS 1 publish retains its reserved read-ahead capacity.
#[test]
fn shuttle_an_acknowledgement_stays_writable_during_qos_zero_overflow() {
    check_interleavings(an_acknowledgement_stays_writable_during_qos_zero_overflow);
}

/// Admitted publishes retain packet order while the first acknowledgement is written.
#[test]
fn shuttle_admitted_publishes_keep_order_while_the_first_acknowledgement_is_written() {
    check_interleavings(admitted_publishes_keep_order_while_the_first_acknowledgement_is_written);
}

/// An acknowledgement whose connection ends before the loop wrote it fails with the lost
/// connection beneath it, and one written first returns before the read after it reports the loss.
#[test]
fn shuttle_an_acknowledgement_whose_connection_ends_fails_with_the_loss() {
    check_interleavings(an_acknowledgement_whose_connection_ends_fails_with_the_loss);
}
