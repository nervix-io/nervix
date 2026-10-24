//! How an MQTT ingestor connection bounds an acknowledgement by its keep-alive, on a paused clock.
//!
//! Layer: test harness.
//! - **Owns.** The check that an acknowledgement its connection cannot write fails once one
//!   keep-alive interval has passed, and not before.
//! - **Depends on.** The connection and its driver, a fake event loop that never writes, and the
//!   paused clock of the boundary's runtime.
//! - **Must not know.** The MQTT client, the broker's protocol, or the host that acknowledges.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{future::pending, num::NonZeroUsize, time::Duration};

use nervix_primitives::time::Instant;
use rumqttc::{ClientError, ConnectionError, Event, Publish, QoS};

use super::{AcknowledgementRequests, ConnectionEvents, MqttConnection};
use crate::topic::ingestor::{MqttTopicIngestorError, MqttTopicIngestorPosition};

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
    async fn request(&self, _publish: &Publish) -> Result<(), ClientError> {
        Ok(())
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn an_acknowledgement_its_connection_cannot_write_fails_after_one_keep_alive_interval() {
    let keep_alive = Duration::from_secs(10);
    let mut connection = MqttConnection::start(
        QueueingClient,
        StalledEventLoop,
        NonZeroUsize::MIN,
        Some(keep_alive),
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
