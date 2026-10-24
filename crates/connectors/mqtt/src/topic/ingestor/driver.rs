//! The task that drives one MQTT ingestor connection once its subscription is established.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The event loop of one ingestor connection and the task that polls it without pause,
//!   so the client writes the acknowledgements a ingestor requests and its keep-alive pings while the
//!   host waits on acknowledgement trees; the bounded read-ahead it hands publishes over through;
//!   the progress it publishes to the ingestor, the count of acknowledgements written to the
//!   connection and whether it polls; and the report of how the connection ended.
//! - **Depends on.** The ingestor's typed errors and positions, `rumqttc`'s event loop, requests
//!   and events, and the boundary's task, channel, watch and timer primitives.
//! - **Must not know.** The host, its batches or acknowledgement trees, or why a publish is
//!   acknowledged.
//!
//! # One connection
//!
//! The driver is the only owner of the event loop. It hands every publish the loop reads to the
//! ingestor through the read-ahead, in the order it read them, and once the read-ahead is full it
//! holds the next publish and polls nothing until the ingestor reads one. Every acknowledgement the
//! event loop writes, a `PUBACK`, or a `PUBREC` for a publish at quality of service 2, raises the
//! count the driver publishes; the loop yields such a write only after it flushed the requests that
//! carried it to the connection. A failed poll ends the connection: the driver publishes that it
//! ended, drops the read-ahead, which the ingestor reads empty before it takes the driver's report,
//! and returns the report as its task's output.
//!
//! A ingestor requests an acknowledgement through the client and waits until the published count
//! covers every acknowledgement it requested on the connection. It never waits on a driver that
//! cannot poll before the ingestor reads again: one that ended, and one holding a publish for room
//! in a read-ahead the ingestor has taken nothing from since the driver published that it waits.
//! Such a wait ends at once with a failure. A driver whose held publish the ingestor has made room
//! for is about to poll again, so the wait goes on, for at most one keep-alive interval: a
//! connection whose broker stopped reading it holds the client's flush, which has no deadline of
//! its own.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::{future::Future, num::NonZeroUsize, time::Duration};

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_primitives::{
    sync::{
        mpsc::{self, error::TrySendError},
        watch,
    },
    task::AbortOnDropHandle,
    time::timeout,
};
use rumqttc::{
    AsyncClient, ClientError, ConnectionError, Event, EventLoop, Incoming, Outgoing, Publish, QoS,
};

use super::{
    MqttTopicIngestorError, MqttTopicIngestorPosition, MqttTopicIngestorResult, PublishRead,
};

/// The events one connection yields: the client's event loop, and a fake event loop in the checks
/// of how the driver meets the ingestor's reads and acknowledgements.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "the driver polls its connection for every packet the connection carries"
    )
)]
pub(super) trait ConnectionEvents: Send + 'static {
    /// Polls the connection until it yields its next event.
    fn next_event(&mut self) -> impl Future<Output = Result<Event, ConnectionError>> + Send;
}

impl ConnectionEvents for EventLoop {
    async fn next_event(&mut self) -> Result<Event, ConnectionError> {
        self.poll().await
    }
}

/// Where a ingestor requests the acknowledgement of a publish: the client, which queues it for the
/// event loop to write, and a fake client in the checks.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "a source requests the acknowledgement of every publish the host accepted"
    )
)]
pub(super) trait AcknowledgementRequests: Send + Sync + 'static {
    /// Queues the acknowledgement `publish`'s quality of service asks for: a `PUBACK` at quality
    /// of service 1, a `PUBREC` at 2. A publish at quality of service 0 asks for none, so a ingestor
    /// never requests one for it.
    fn request(&self, publish: &Publish) -> impl Future<Output = Result<(), ClientError>> + Send;
}

impl AcknowledgementRequests for AsyncClient {
    async fn request(&self, publish: &Publish) -> Result<(), ClientError> {
        self.ack(publish).await
    }
}

/// What a connection's driver is doing, as the ingestor that owns the connection observes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DriverActivity {
    /// Polling the event loop, which writes every acknowledgement the ingestor requested.
    Polling,
    /// Holding a publish it read, and polling nothing, until the read-ahead has room for it. It had
    /// handed `handed` publishes to the read-ahead before that one.
    WaitingForRoom { handed: u64 },
    /// The connection ended, and the driver's report says how.
    Ended,
}

/// What a connection's driver publishes to the ingestor that owns the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DriverProgress {
    /// How many acknowledgements the event loop has written to the connection.
    written: u64,
    activity: DriverActivity,
}

/// Why the acknowledgements a ingestor requested stay unwritten until it reads again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unwritable {
    /// The driver holds a publish for room in a read-ahead the ingestor has taken nothing from since.
    ReadAheadFull,
    /// The connection ended.
    Ended,
}

/// How a wait for an acknowledgement's writes ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteWait {
    /// Every acknowledgement the ingestor requested on the connection is written.
    Written,
    /// The driver writes them only after the ingestor reads again.
    Unwritable(Unwritable),
}

impl DriverProgress {
    /// Why the driver writes nothing until the ingestor reads again, if it does, for a ingestor that
    /// has taken `taken` publishes in all from a read-ahead of `capacity`.
    ///
    /// A driver holding a publish for room the ingestor has made since is about to poll again.
    fn unwritable(&self, taken: u64, capacity: NonZeroUsize) -> Option<Unwritable> {
        let handed = match self.activity {
            DriverActivity::Polling => return None,
            DriverActivity::Ended => return Some(Unwritable::Ended),
            DriverActivity::WaitingForRoom { handed } => handed,
        };
        // A ingestor that has taken more publishes than the driver had handed over before the hold
        // has taken the held one too, which the driver handed over before it published that it
        // polls again.
        let unread = handed.checked_sub(taken)?;
        let capacity: u64 = capacity.get().arch_into();
        if unread < capacity {
            return None;
        }
        Some(Unwritable::ReadAheadFull)
    }

    /// How a wait for `requested` written acknowledgements ends at this progress, if it ends, for
    /// a ingestor that has taken `taken` publishes in all from a read-ahead of `capacity`.
    fn write_wait(&self, requested: u64, taken: u64, capacity: NonZeroUsize) -> Option<WriteWait> {
        if self.written >= requested {
            return Some(WriteWait::Written);
        }
        let unwritable = self.unwritable(taken, capacity)?;
        Some(WriteWait::Unwritable(unwritable))
    }
}

/// The task that owns one connection's event loop.
struct ConnectionDriver<E> {
    events: E,
    read_ahead: mpsc::Sender<Publish>,
    /// How many publishes the driver has handed to the read-ahead.
    handed: u64,
    progress: watch::Sender<DriverProgress>,
}

impl<E: ConnectionEvents> ConnectionDriver<E> {
    /// Polls the connection until it ends, and returns how it ended.
    async fn run(mut self) -> Report<MqttTopicIngestorError> {
        loop {
            nervix_primitives::task::consume_budget().await;
            let event = match self.events.next_event().await {
                Ok(event) => event,
                Err(error) => return self.end(MqttTopicIngestorError::lost_connection(error)),
            };
            match event {
                Event::Incoming(Incoming::Publish(publish)) => {
                    if let Err(ended) = self.hand_over(publish).await {
                        return ended;
                    }
                }
                Event::Outgoing(Outgoing::PubAck(_) | Outgoing::PubRec(_)) => {
                    self.progress.send_modify(|progress| {
                        progress.written = progress.written.checked_add(1).assured(
                            "a connection cannot write 2^64 acknowledgements in the lifetime of a \
                             node",
                        );
                    });
                }
                Event::Incoming(_) | Event::Outgoing(_) | Event::Auth(_) => {}
            }
        }
    }

    /// Hands `publish` to the ingestor. While the read-ahead is full, it holds the publish and polls
    /// nothing.
    async fn hand_over(&mut self, publish: Publish) -> Result<(), Report<MqttTopicIngestorError>> {
        let publish = match self.read_ahead.try_send(publish) {
            Ok(()) => {
                self.count_handed();
                return Ok(());
            }
            Err(TrySendError::Full(publish)) => publish,
            // The ingestor dropped the connection, and with it the read-ahead.
            Err(TrySendError::Closed(_)) => {
                return Err(self.end(Report::new(MqttTopicIngestorError::Disconnected)));
            }
        };
        let handed = self.handed;
        self.progress.send_modify(|progress| {
            progress.activity = DriverActivity::WaitingForRoom { handed };
        });
        if self.read_ahead.send(publish).await.is_err() {
            return Err(self.end(Report::new(MqttTopicIngestorError::Disconnected)));
        }
        self.count_handed();
        self.progress
            .send_modify(|progress| progress.activity = DriverActivity::Polling);
        Ok(())
    }

    fn count_handed(&mut self) {
        self.handed = self
            .handed
            .checked_add(1)
            .assured("a connection cannot read 2^64 publishes in the lifetime of a node");
    }

    /// Publishes that the connection ended, before the driver returns `report`.
    fn end(&self, report: Report<MqttTopicIngestorError>) -> Report<MqttTopicIngestorError> {
        self.progress
            .send_modify(|progress| progress.activity = DriverActivity::Ended);
        report
    }
}

/// One ingestor connection: the client the ingestor requests acknowledgements through, the read-ahead
/// it reads publishes from, and the driver task that owns the connection's event loop.
///
/// Dropping the connection aborts its driver, which drops the event loop and with it the
/// connection to the broker.
pub(super) struct MqttConnection<R = AsyncClient> {
    requests: R,
    read_ahead: mpsc::Receiver<Publish>,
    /// How many publishes the read-ahead holds.
    read_ahead_capacity: NonZeroUsize,
    /// How many publishes the ingestor has taken from the read-ahead.
    taken: u64,
    progress: watch::Receiver<DriverProgress>,
    /// The driver task, until the ingestor took the report of how the connection ended.
    driver: Option<AbortOnDropHandle<Report<MqttTopicIngestorError>>>,
    /// How many acknowledgements the ingestor requested on this connection.
    requested: u64,
    /// The keep-alive the connection keeps to, which bounds every acknowledgement, unless the
    /// broker disabled it.
    keep_alive: Option<Duration>,
}

impl<R: AcknowledgementRequests> MqttConnection<R> {
    /// Starts the driver of a connection whose subscription `events` already established, with a
    /// read-ahead of `read_ahead_capacity` publishes, that keeps to `keep_alive`, if the broker
    /// keeps one.
    pub(super) fn start<E: ConnectionEvents>(
        requests: R,
        events: E,
        read_ahead_capacity: NonZeroUsize,
        keep_alive: Option<Duration>,
    ) -> Self {
        let (read_ahead, publishes) = mpsc::channel(read_ahead_capacity.get());
        let (progress, observed) = watch::channel(DriverProgress {
            written: 0,
            activity: DriverActivity::Polling,
        });
        let driver = ConnectionDriver {
            events,
            read_ahead,
            handed: 0,
            progress,
        };
        let driver = AbortOnDropHandle::new(nervix_primitives::task::spawn(driver.run()));
        Self {
            requests,
            read_ahead: publishes,
            read_ahead_capacity,
            taken: 0,
            progress: observed,
            driver: Some(driver),
            requested: 0,
            keep_alive,
        }
    }

    /// Whether the connection ended, so that nothing it delivered can be acknowledged on it.
    pub(super) fn has_ended(&self) -> bool {
        self.progress.borrow().activity == DriverActivity::Ended
    }

    /// Requests the acknowledgement of every publish of `positions` that asks for one, and returns
    /// once the event loop has written every acknowledgement requested on the connection.
    ///
    /// It fails as soon as the driver cannot poll before the ingestor reads again with
    /// acknowledgements unwritten: when the connection ended, and when the driver holds a publish
    /// for room in a read-ahead the ingestor has taken nothing from since, which only the ingestor's
    /// next read, after this returns, could change. It fails too once one keep-alive interval has
    /// passed without the writes: a connection that cannot write for that long carries none of its
    /// pings either, and its broker is about to close it.
    pub(super) async fn acknowledge(
        &mut self,
        positions: &[MqttTopicIngestorPosition],
    ) -> MqttTopicIngestorResult<()> {
        let Some(keep_alive) = self.keep_alive else {
            return self.write_acknowledgements(positions).await;
        };
        let acknowledged = timeout(keep_alive, self.write_acknowledgements(positions)).await;
        let Ok(acknowledged) = acknowledged else {
            return Err(Report::new(
                MqttTopicIngestorError::AcknowledgementDeadline { keep_alive },
            ));
        };
        acknowledged
    }

    /// Requests the acknowledgement of every publish of `positions` that asks for one, and waits
    /// until the event loop has written every acknowledgement requested on the connection, or the
    /// driver cannot poll before the ingestor reads again.
    async fn write_acknowledgements(
        &mut self,
        positions: &[MqttTopicIngestorPosition],
    ) -> MqttTopicIngestorResult<()> {
        for position in positions {
            nervix_primitives::task::consume_budget().await;
            let publish = position.publish();
            if publish.qos == QoS::AtMostOnce {
                continue;
            }
            let requested = self.requested.checked_add(1).assured(
                "a connection cannot request 2^64 acknowledgements in the lifetime of a node",
            );
            if let Some(unwritable) = self.request(publish).await? {
                return Err(self.unwritten(unwritable).await);
            }
            self.requested = requested;
        }
        let wait = self.write_wait().await;
        match wait {
            WriteWait::Written => Ok(()),
            WriteWait::Unwritable(unwritable) => Err(self.unwritten(unwritable).await),
        }
    }

    /// Waits until the event loop has written every acknowledgement requested on the connection,
    /// or the driver cannot poll before the ingestor reads again.
    async fn write_wait(&mut self) -> WriteWait {
        let requested = self.requested;
        let taken = self.taken;
        let capacity = self.read_ahead_capacity;
        let waited = self
            .progress
            .wait_for(|progress| progress.write_wait(requested, taken, capacity).is_some())
            .await;
        match waited {
            Ok(progress) => progress
                .write_wait(requested, taken, capacity)
                .verified("the wait returned the progress its predicate held for"),
            // The driver drops its end only once it ended.
            Err(_) => WriteWait::Unwritable(Unwritable::Ended),
        }
    }

    /// Queues the acknowledgement of `publish`, unless the driver cannot poll before the ingestor
    /// reads again, which would leave the request where nothing writes it, and returns why.
    async fn request(&mut self, publish: &Publish) -> MqttTopicIngestorResult<Option<Unwritable>> {
        let taken = self.taken;
        let capacity = self.read_ahead_capacity;
        let requests = &self.requests;
        let progress = &mut self.progress;
        nervix_primitives::select! {
            biased;
            requested = requests.request(publish) => match requested {
                Ok(()) => Ok(None),
                // The client's requests end with the event loop, which only a driver that ended
                // drops.
                Err(ClientError::RequestChannelDisconnected(_)) => Ok(Some(Unwritable::Ended)),
                Err(error) => Err(Report::new(error).change_context(MqttTopicIngestorError::Acknowledge)),
            },
            stopped = progress.wait_for(|progress| progress.unwritable(taken, capacity).is_some()) => {
                match stopped {
                    Ok(progress) => Ok(progress.unwritable(taken, capacity)),
                    // The driver drops its end only once it ended.
                    Err(_) => Ok(Some(Unwritable::Ended)),
                }
            }
        }
    }

    /// The failure of acknowledgements that stay unwritten because of `unwritable`.
    async fn unwritten(&mut self, unwritable: Unwritable) -> Report<MqttTopicIngestorError> {
        match unwritable {
            Unwritable::ReadAheadFull => Report::new(MqttTopicIngestorError::ReadAheadFull {
                publishes: self.read_ahead_capacity,
            }),
            Unwritable::Ended => self
                .ended()
                .await
                .change_context(MqttTopicIngestorError::Acknowledge),
        }
    }

    /// How the connection ended, which the driver reports once.
    ///
    /// The ingestor takes the report when it reads the read-ahead empty of a driver that ended, or
    /// when an acknowledgement finds the connection ended, and then drops the connection.
    async fn ended(&mut self) -> Report<MqttTopicIngestorError> {
        let Some(driver) = self.driver.as_mut() else {
            return Report::new(MqttTopicIngestorError::Disconnected);
        };
        let ended = driver.await;
        self.driver = None;
        match ended {
            Ok(report) => report,
            Err(failure) => Report::new(failure).change_context(MqttTopicIngestorError::Driver),
        }
    }
}

impl<R: AcknowledgementRequests> PublishRead for MqttConnection<R> {
    async fn next_publish(&mut self) -> MqttTopicIngestorResult<Publish> {
        let Some(publish) = self.read_ahead.recv().await else {
            // The driver drops the read-ahead only once it ended, after every publish it read.
            return Err(self.ended().await);
        };
        self.taken = self
            .taken
            .checked_add(1)
            .assured("a connection cannot read 2^64 publishes in the lifetime of a node");
        Ok(publish)
    }
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "driver_shuttle_tests.rs"]
mod shuttle_tests;
