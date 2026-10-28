//! MQTT ingestor packet admission and discard acknowledgement checks.
//!
//! Layer: test harness.
//! - **Owns.** Complete-packet admission, independent admitted bytes, compact discard ACKs and
//!   their progress and original deadline across cancellation or a connection ending.
//! - **Depends on.** The ingestor connection and driver, controlled event loops and request queues,
//!   MQTT packet types, and the paused clock of the boundary's runtime.
//! - **Must not know.** A real MQTT client or broker, the host's graph, or its ACK trees.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    future::pending,
    num::{NonZeroU16, NonZeroUsize},
    time::Duration,
};

use bytes::Bytes;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    IngestorBatch, IngestorBatchRequest, IngestorMessage as _, IngestorReadCancellation,
};
use nervix_models::MQTT_SOURCE_MAXIMUM_PACKET_BYTES;
use nervix_primitives::{
    sync::mpsc,
    time::{Instant, sleep},
};
use rumqttc::{
    AckMode, ClientError, ConnectionError, Event, Incoming, ManualAck, Outgoing, PubAck, Publish,
    PublishProperties, QoS,
};

use super::{AcknowledgementRequests, ConnectionEvents, IngestorPacket, MqttConnection};
use crate::topic::ingestor::{
    IngestorAcknowledgement, MqttTopicIngestor, MqttTopicIngestorError, MqttTopicIngestorPosition,
    PublishRead as _, metrics_fixture::IngestorMetricsFixture,
};

/// The event loop of a connection whose broker stopped reading it: it writes nothing and yields
/// nothing.
struct StalledEventLoop;

impl ConnectionEvents for StalledEventLoop {
    async fn next_event(&mut self) -> Result<Event, ConnectionError> {
        pending().await
    }
}

/// A client that queues every acknowledgement the ingestor requests.
struct QueueingClient;

impl AcknowledgementRequests for QueueingClient {
    async fn request(&self, _acknowledgement: &ManualAck) -> Result<(), ClientError> {
        Ok(())
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn an_acknowledgement_its_connection_cannot_write_fails_after_one_keep_alive_interval() {
    let keep_alive = Duration::from_secs(10);
    let mut connection = MqttConnection::start(
        QueueingClient,
        StalledEventLoop,
        IngestorAcknowledgement::Manual {
            receive_maximum: NonZeroU16::MIN,
        },
        Some(keep_alive),
        IngestorMetricsFixture::new().counter(),
    );
    let mut publish = Publish::new("events", QoS::AtLeastOnce, vec![1], None);
    publish.pkid = 1;
    let started = Instant::now();

    let Err(error) = connection
        .acknowledge(&[MqttTopicIngestorPosition { publish }])
        .await
    else {
        panic!("a connection that writes nothing confirms no acknowledgement");
    };

    assert!(
        matches!(
            error.current_context(),
            MqttTopicIngestorError::AcknowledgementDeadline { keep_alive: bound } if *bound == keep_alive
        ),
        "an acknowledgement its connection cannot write fails at the keep-alive: {error:?}"
    );
    assert!(
        started.elapsed() >= keep_alive,
        "the acknowledgement waits one whole keep-alive interval before it fails"
    );
}

#[test]
fn admission_bounds_the_complete_packet_and_retains_only_a_discard_acknowledgement() {
    let maximum = usize::try_from(MQTT_SOURCE_MAXIMUM_PACKET_BYTES).assured("ten KiB fits usize");
    for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
        let mut publish = Publish::new("events", qos, vec![7; maximum], None);
        publish.pkid = if qos == QoS::AtMostOnce { 0 } else { 7 };
        let payload_bytes = maximum - (publish.size() - maximum);
        publish.payload = publish.payload.slice(..payload_bytes);
        assert_eq!(publish.size(), maximum);
        let manual = AckMode::Manual;
        let IngestorPacket::Message(admitted) = IngestorPacket::classify(publish.clone(), manual)
        else {
            panic!("a packet at the admission bound is delivered");
        };
        assert_eq!(admitted, publish);
        publish.payload = vec![7; payload_bytes + 1].into();
        assert_eq!(publish.size(), maximum + 1);
        let IngestorPacket::Oversized(acknowledgement) =
            IngestorPacket::classify(publish.clone(), manual)
        else {
            panic!("a complete packet above the bound is counted without its bytes");
        };
        match (qos, acknowledgement) {
            (QoS::AtMostOnce, None) => {}
            (QoS::AtLeastOnce, Some(ManualAck::PubAck(ack))) => assert_eq!(ack.pkid, 7),
            (QoS::ExactlyOnce, Some(ManualAck::PubRec(rec))) => assert_eq!(rec.pkid, 7),
            pair => panic!("the discard retains the acknowledgement its QoS needs: {pair:?}"),
        }
        assert!(matches!(
            IngestorPacket::classify(publish, AckMode::Automatic),
            IngestorPacket::Oversized(None)
        ));
    }
}

#[test]
fn an_admitted_publish_owns_its_topic_payload_and_correlation_bytes() {
    let frame = Bytes::from(vec![b'a'; 20_480]);
    let publish = Publish {
        topic: frame.slice(0..6),
        payload: frame.slice(6..7),
        properties: Some(PublishProperties {
            correlation_data: Some(frame.slice(7..8)),
            ..Default::default()
        }),
        ..Publish::new("events", QoS::AtMostOnce, Vec::new(), None)
    };
    let IngestorPacket::Message(admitted) =
        IngestorPacket::classify(publish.clone(), AckMode::Automatic)
    else {
        panic!("a small packet is admitted");
    };
    assert_eq!(admitted, publish);
    assert_ne!(admitted.topic.as_ptr(), publish.topic.as_ptr());
    assert_ne!(admitted.payload.as_ptr(), publish.payload.as_ptr());
    let admitted_data = admitted
        .properties
        .as_ref()
        .and_then(|p| p.correlation_data.as_ref())
        .assured("the fixture keeps correlation data");
    let frame_data = publish
        .properties
        .as_ref()
        .and_then(|p| p.correlation_data.as_ref())
        .assured("the fixture keeps correlation data");
    assert_ne!(admitted_data.as_ptr(), frame_data.as_ptr());
}

struct OnePublishEventLoop(Option<Publish>);

impl ConnectionEvents for OnePublishEventLoop {
    async fn next_event(&mut self) -> Result<Event, ConnectionError> {
        match self.0.take() {
            Some(publish) => Ok(Event::Incoming(Incoming::Publish(publish))),
            None => pending().await,
        }
    }
}

struct RecordingClient(mpsc::Sender<ManualAck>);

impl AcknowledgementRequests for RecordingClient {
    async fn request(&self, acknowledgement: &ManualAck) -> Result<(), ClientError> {
        self.0
            .send(acknowledgement.clone())
            .await
            .assured("the check retains its acknowledgement receiver");
        Ok(())
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn a_cancelled_discard_keeps_its_acknowledgement_progress_and_original_deadline() {
    for queued_before_cancel in [false, true] {
        let keep_alive = Duration::from_secs(10);
        let mut publish = Publish::new("events", QoS::AtLeastOnce, vec![7; 20_480], None);
        publish.pkid = 7;
        let (requests, mut queued) = mpsc::channel(1);
        if !queued_before_cancel {
            requests
                .send(ManualAck::PubAck(PubAck::new(99, None)))
                .await
                .assured("the fixture occupies the request slot");
        }
        let mut connection = MqttConnection::start(
            RecordingClient(requests),
            OnePublishEventLoop(Some(publish)),
            IngestorAcknowledgement::Manual {
                receive_maximum: NonZeroU16::MIN,
            },
            Some(keep_alive),
            IngestorMetricsFixture::new().counter(),
        );
        let cancellation = IngestorReadCancellation::new();
        let request = IngestorBatchRequest {
            max_messages: NonZeroUsize::MIN,
            batch_timeout: None,
        };
        let started = Instant::now();
        let host = async {
            if queued_before_cancel {
                assert!(
                    matches!(queued.recv().await, Some(ManualAck::PubAck(ack)) if ack.pkid == 7)
                );
            } else {
                sleep(Duration::from_secs(1)).await;
            }
            cancellation.cancel();
        };
        let (read, ()) = tokio::join!(
            MqttTopicIngestor::read_batch(&mut connection, request, &cancellation),
            host
        );
        assert!(
            matches!(read.assured("cancellation returns the empty batch"), IngestorBatch::Interrupted { gathered } if gathered.is_empty())
        );
        sleep(Duration::from_secs(3)).await;
        let Err(error) = MqttTopicIngestor::read_batch(
            &mut connection,
            request,
            &IngestorReadCancellation::new(),
        )
        .await
        else {
            panic!(
                "a discard whose acknowledgement is never written reaches its physical deadline"
            );
        };
        assert!(
            matches!(error.current_context(), MqttTopicIngestorError::AcknowledgementDeadline { keep_alive: bound } if *bound == keep_alive)
        );
        assert_eq!(started.elapsed(), keep_alive);
        if !queued_before_cancel {
            assert!(matches!(queued.try_recv(), Ok(ManualAck::PubAck(ack)) if ack.pkid == 99));
        }
        assert!(matches!(
            queued.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}

struct ClosingEventLoop(Option<Publish>);

impl ConnectionEvents for ClosingEventLoop {
    async fn next_event(&mut self) -> Result<Event, ConnectionError> {
        match self.0.take() {
            Some(publish) => Ok(Event::Incoming(Incoming::Publish(publish))),
            None => Err(ConnectionError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "the scripted connection ended",
            ))),
        }
    }
}

#[nervix_primitives::test]
async fn a_discard_whose_connection_ends_preserves_the_acknowledgement_failure_cause() {
    let mut publish = Publish::new("events", QoS::AtLeastOnce, vec![7; 20_480], None);
    publish.pkid = 7;
    let (requests, _queued) = mpsc::channel(1);
    let mut connection = MqttConnection::start(
        RecordingClient(requests),
        ClosingEventLoop(Some(publish)),
        IngestorAcknowledgement::Manual {
            receive_maximum: NonZeroU16::MIN,
        },
        None,
        IngestorMetricsFixture::new().counter(),
    );
    let request = IngestorBatchRequest {
        max_messages: NonZeroUsize::MIN,
        batch_timeout: None,
    };
    let Err(error) =
        MqttTopicIngestor::read_batch(&mut connection, request, &IngestorReadCancellation::new())
            .await
    else {
        panic!("a discard cannot confirm an acknowledgement after its connection ended");
    };
    assert!(matches!(
        error.current_context(),
        MqttTopicIngestorError::Acknowledge
    ));
    assert!(error.frames().any(|frame| matches!(
        frame.downcast_ref::<MqttTopicIngestorError>(),
        Some(MqttTopicIngestorError::Receive)
    )));
}

/// Reads its supplied packets, then confirms each queued acknowledgement as an outgoing event.
struct AcknowledgingEventLoop {
    publishes: std::collections::VecDeque<Publish>,
    requests: mpsc::Receiver<ManualAck>,
}

impl ConnectionEvents for AcknowledgingEventLoop {
    async fn next_event(&mut self) -> Result<Event, ConnectionError> {
        if let Some(publish) = self.publishes.pop_front() {
            return Ok(Event::Incoming(Incoming::Publish(publish)));
        }
        let acknowledgement = self
            .requests
            .recv()
            .await
            .assured("the connection keeps its client");
        let outgoing = match acknowledgement {
            ManualAck::PubAck(ack) => Outgoing::PubAck(ack.pkid),
            ManualAck::PubRec(rec) => Outgoing::PubRec(rec.pkid),
        };
        Ok(Event::Outgoing(outgoing))
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn a_discard_deadline_starts_after_the_host_settles_its_ordinary_prefix() {
    let keep_alive = Duration::from_secs(10);
    let mut first = Publish::new("events", QoS::AtLeastOnce, vec![1], None);
    first.pkid = 1;
    let mut oversized = Publish::new("events", QoS::AtLeastOnce, vec![2; 20_480], None);
    oversized.pkid = 2;
    let (requests, queued) = mpsc::channel(2);
    let events = AcknowledgingEventLoop {
        publishes: [first, oversized].into(),
        requests: queued,
    };
    let mut connection = MqttConnection::start(
        RecordingClient(requests),
        events,
        IngestorAcknowledgement::Manual {
            receive_maximum: NonZeroU16::new(2).assured("two is nonzero"),
        },
        Some(keep_alive),
        IngestorMetricsFixture::new().counter(),
    );
    let request = IngestorBatchRequest {
        max_messages: NonZeroUsize::new(3).assured("three is nonzero"),
        batch_timeout: Some(Duration::from_secs(3_600)),
    };
    let batch =
        MqttTopicIngestor::read_batch(&mut connection, request, &IngestorReadCancellation::new())
            .await
            .assured("the discard ends the ordinary prefix");
    let IngestorBatch::Messages(prefix) = batch else {
        panic!("the ordinary prefix is returned first");
    };
    assert_eq!(prefix.len(), 1);
    assert_eq!(prefix[0].payload(), &[1]);
    sleep(keep_alive + Duration::from_secs(5)).await;
    connection
        .acknowledge(&[prefix[0].position().clone()])
        .await
        .assured("the ordinary prefix is acknowledged first");
    let size =
        MqttTopicIngestor::read_batch(&mut connection, request, &IngestorReadCancellation::new())
            .await
            .assured("the discard has its own acknowledgement deadline after prefix processing");
    assert!(matches!(size, IngestorBatch::Oversized { payloads } if payloads.get() == 1));
}

/// Scripted incoming packets and acknowledgement writes share the production driver's event loop.
/// A ping event is a progress witness after the entire incoming prefix has been classified.
struct OverflowEventLoop {
    packets: mpsc::UnboundedReceiver<Event>,
    requests: mpsc::Receiver<ManualAck>,
    pinged: mpsc::Sender<()>,
}

impl ConnectionEvents for OverflowEventLoop {
    async fn next_event(&mut self) -> Result<Event, ConnectionError> {
        nervix_primitives::select! {
            biased;
            requested = self.requests.recv() => {
                let acknowledgement = requested.assured("the connection keeps its request sender");
                let outgoing = match acknowledgement {
                    ManualAck::PubAck(ack) => Outgoing::PubAck(ack.pkid),
                    ManualAck::PubRec(rec) => Outgoing::PubRec(rec.pkid),
                };
                Ok(Event::Outgoing(outgoing))
            }
            packet = self.packets.recv() => {
                let packet = packet.assured("the check keeps its packet sender");
                if let Event::Outgoing(Outgoing::PingReq) = packet {
                    self.pinged.send(()).await.assured("the check waits for its ping witness");
                }
                Ok(packet)
            }
        }
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn qos_zero_overflow_keeps_polling_and_preserves_acknowledged_publishes() {
    let metrics = IngestorMetricsFixture::new();
    let (packets, incoming) = mpsc::unbounded_channel();
    let (requests, queued) = mpsc::channel(65);
    let (pinged, mut pings) = mpsc::channel(1);
    let events = OverflowEventLoop {
        packets: incoming,
        requests: queued,
        pinged,
    };
    let mut connection = MqttConnection::start(
        RecordingClient(requests),
        events,
        IngestorAcknowledgement::Manual {
            receive_maximum: NonZeroU16::new(65).assured("the sequential window includes headroom"),
        },
        Some(Duration::from_secs(10)),
        metrics.counter(),
    );
    let mut first = Publish::new("events", QoS::AtLeastOnce, vec![1], None);
    first.pkid = 1;
    packets
        .send(Event::Incoming(Incoming::Publish(first.clone())))
        .assured("the driver owns its packet receiver");
    let first_read = connection
        .next_publish()
        .await
        .assured("the first publish is delivered");
    assert!(matches!(first_read, super::PublishReadOutcome::Message(publish) if *publish == first));

    for value in 0..128_u8 {
        packets
            .send(Event::Incoming(Incoming::Publish(Publish::new(
                "events",
                QoS::AtMostOnce,
                vec![value],
                None,
            ))))
            .assured("the driver keeps polling the incoming flood");
    }
    let mut following = Publish::new("events", QoS::AtLeastOnce, vec![200], None);
    following.pkid = 2;
    packets
        .send(Event::Incoming(Incoming::Publish(following.clone())))
        .assured("the following acknowledged publish fits its broker window");
    packets
        .send(Event::Outgoing(Outgoing::PingReq))
        .assured("the loop emits a ping after reading the flood");
    nervix_primitives::time::timeout(Duration::from_secs(10), pings.recv())
        .await
        .assured("QoS 0 overflow must not stop the connection's keep-alive polling")
        .assured("the driver sends its progress witness");
    assert_eq!(
        metrics.dropped(),
        64,
        "every overflow is counted while the host still waits"
    );
    connection
        .acknowledge(&[MqttTopicIngestorPosition { publish: first }])
        .await
        .assured("QoS 0 overflow leaves the first acknowledgement writable");

    for value in 0..64_u8 {
        let next = connection
            .next_publish()
            .await
            .assured("the admitted prefix is retained");
        let expected = Publish::new("events", QoS::AtMostOnce, vec![value], None);
        assert!(
            matches!(next, super::PublishReadOutcome::Message(publish) if *publish == expected)
        );
    }
    let next = connection
        .next_publish()
        .await
        .assured("the following acknowledged publish is retained");
    assert!(matches!(next, super::PublishReadOutcome::Message(publish) if *publish == following));
    connection
        .acknowledge(&[MqttTopicIngestorPosition { publish: following }])
        .await
        .assured("the following acknowledgement is written");
    let resumed = Publish::new("events", QoS::AtMostOnce, vec![201], None);
    packets
        .send(Event::Incoming(Incoming::Publish(resumed.clone())))
        .assured("taking the queued prefix releases its QoS 0 capacity");
    let next = connection
        .next_publish()
        .await
        .assured("QoS 0 admission resumes after draining");
    assert!(matches!(next, super::PublishReadOutcome::Message(publish) if *publish == resumed));
    assert_eq!(metrics.dropped(), 64);
}
