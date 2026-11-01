//! The task that drives one MQTT ingestor connection once its subscription is established.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The event loop of one ingestor connection and the task that polls it without pause,
//!   so the client writes the acknowledgements an ingestor requests and its keep-alive pings while the
//!   host waits on acknowledgement trees; the bounded read-ahead it hands publishes over through;
//!   admission of each decoded publish before read-ahead, compact discard acknowledgements and
//!   their cancellation-stable confirmation; the progress it publishes to the ingestor, the count
//!   of acknowledgements written to the connection and whether it polls; and the report of how
//!   the connection ended.
//! - **Depends on.** The ingestor's typed errors and positions, `rumqttc`'s event loop, requests
//!   and events, and the boundary's task, channel, watch and timer primitives.
//! - **Must not know.** The host, its batches or acknowledgement trees, or the graph's delivery
//!   decisions.
//!
//! # One connection
//!
//! The driver is the only owner of the event loop. It classifies every publish before reading the
//! next, then hands admitted bytes or a compact discard acknowledgement to the ingestor in packet
//! order. In manual acknowledgement mode, queued QoS 0 outcomes own at most 64 permits alongside
//! the complete Receive Maximum reserved for acknowledged outcomes. Refusing a QoS 0 permit drops
//! and counts that publish without stopping polling. Automatic acknowledgement retains bounded
//! backpressure. Every acknowledgement the
//! event loop writes, a `PUBACK`, or a `PUBREC` for a publish at quality of service 2, raises the
//! count the driver publishes; the loop yields such a write only after it flushed the requests that
//! carried it to the connection. A failed poll ends the connection: the driver publishes that it
//! ended, drops the read-ahead, which the ingestor reads empty before it takes the driver's report,
//! and returns the report as its task's output.
//!
//! An ingestor requests an acknowledgement through the client and waits until the published count
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

use std::{
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use arch_into::ArchInto as _;
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::MQTT_SOURCE_READ_AHEAD_HEADROOM;
use nervix_primitives::{
    sync::{
        OwnedSemaphorePermit, Semaphore, StdArc, TryAcquireError,
        mpsc::{self, error::TrySendError},
        watch,
    },
    task::AbortOnDropHandle,
    time::{Instant, timeout, timeout_at},
};
use rumqttc::{
    AckMode, AsyncClient, ClientError, ConnectionError, Event, EventLoop, Incoming, ManualAck,
    Outgoing, Publish, QoS,
};

use super::{
    GatherPublishOutcome, IngestorAcknowledgement, MqttTopicIngestorError,
    MqttTopicIngestorPosition, MqttTopicIngestorResult, PublishRead, PublishReadOutcome,
    acknowledgement,
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

/// Where an ingestor requests the acknowledgement of a publish: the client, which queues it for the
/// event loop to write, and a fake client in the checks.
#[cfg_attr(
    nervix_lint,
    nervix::context(
        recurring,
        reason = "a source requests the acknowledgement of every publish the host accepted"
    )
)]
pub(super) trait AcknowledgementRequests: Send + Sync + 'static {
    /// Queues a prepared acknowledgement without retaining its publish's bytes.
    fn request(
        &self,
        acknowledgement: &ManualAck,
    ) -> impl Future<Output = Result<(), ClientError>> + Send;
}

impl AcknowledgementRequests for AsyncClient {
    async fn request(&self, acknowledgement: &ManualAck) -> Result<(), ClientError> {
        self.manual_ack(acknowledgement.clone()).await
    }
}

/// What the driver retains after classifying one decoded publish.
enum IngestorPacket {
    Message(Publish),
    Oversized(Option<ManualAck>),
}

impl IngestorPacket {
    /// Admits `publish` when its complete packet holds at most `admission` bytes, and otherwise
    /// keeps only the acknowledgement its quality of service needs under `mode`.
    fn classify(mut publish: Publish, mode: AckMode, admission: NonZeroU32) -> Self {
        let packet_bytes: u64 = publish.size().arch_into();
        if packet_bytes > u64::from(admission.get()) {
            let acknowledgement = match mode {
                AckMode::Automatic => None,
                AckMode::Manual => acknowledgement(&publish),
            };
            return Self::Oversized(acknowledgement);
        }
        // The decoder slices a shared frame allocation. Independent admitted bytes prevent a
        // small topic or payload from pinning an oversized neighbouring packet's allocation.
        publish.topic = publish.topic.to_vec().into();
        publish.payload = publish.payload.to_vec().into();
        if let Some(properties) = publish.properties.as_mut()
            && let Some(data) = properties.correlation_data.as_mut()
        {
            *data = data.to_vec().into();
        }
        Self::Message(publish)
    }
}

/// A queued QoS 0 outcome retains its capacity until the source takes it, including a compact
/// oversized outcome. The same ordered queue reserves separate capacity for acknowledged packets.
struct ReadAheadPacket {
    packet: IngestorPacket,
    _qos_zero_capacity: Option<OwnedSemaphorePermit>,
}

/// The running acknowledgement policy, with capacity for traffic the broker cannot hold back.
enum DriverAcknowledgement {
    OnReceipt,
    Manual {
        /// Owned permits require the standard shared-ownership type. Queued packets retain this
        /// semaphore independently of the driver's lifetime.
        qos_zero: StdArc<Semaphore>,
    },
}

enum ReadAheadAdmission {
    Accept(Option<OwnedSemaphorePermit>),
    Overflow,
}

impl From<IngestorAcknowledgement> for DriverAcknowledgement {
    fn from(acknowledgement: IngestorAcknowledgement) -> Self {
        match acknowledgement {
            IngestorAcknowledgement::OnReceipt => Self::OnReceipt,
            IngestorAcknowledgement::Manual { .. } => Self::Manual {
                qos_zero: StdArc::new(Semaphore::new(
                    usize::try_from(MQTT_SOURCE_READ_AHEAD_HEADROOM)
                        .assured("MQTT's read-ahead headroom fits every supported native target"),
                )),
            },
        }
    }
}

impl DriverAcknowledgement {
    #[cfg_attr(
        nervix_lint,
        nervix::context(
            bounded,
            reason = "QoS 0 cannot consume the capacity reserved for broker-controlled packets",
            key = "one MQTT connection's QoS 0 read-ahead",
            bound = "64 permits retained only by queued outcomes; acquisition never waits"
        )
    )]
    fn admit(&self, publish: &Publish) -> ReadAheadAdmission {
        let Self::Manual { qos_zero } = self else {
            return ReadAheadAdmission::Accept(None);
        };
        if publish.qos != QoS::AtMostOnce {
            return ReadAheadAdmission::Accept(None);
        }
        let capacity = qos_zero.clone().try_acquire_owned();
        if let Err(TryAcquireError::NoPermits) = capacity {
            return ReadAheadAdmission::Overflow;
        }
        let capacity = capacity.assured("the driver owns the QoS 0 semaphore and never closes it");
        ReadAheadAdmission::Accept(Some(capacity))
    }

    fn ack_mode(&self) -> AckMode {
        match self {
            Self::OnReceipt => AckMode::Automatic,
            Self::Manual { .. } => AckMode::Manual,
        }
    }
}

/// The original physical bound of a discard acknowledgement, retained across read cancellation.
#[derive(Clone, Copy)]
struct AcknowledgementDeadline {
    at: Instant,
    keep_alive: Duration,
}

/// One discard's progress. The connection changes each state before yielding after queue admission
/// or write confirmation, so cancelling a read never queues its acknowledgement twice.
#[derive(Clone)]
enum PendingOversized {
    Deferred(Option<ManualAck>),
    Unrequested {
        acknowledgement: ManualAck,
        deadline: Option<AcknowledgementDeadline>,
    },
    Requested {
        target: u64,
        deadline: Option<AcknowledgementDeadline>,
    },
}

impl PendingOversized {
    fn start(acknowledgement: ManualAck, keep_alive: Option<Duration>) -> Self {
        let deadline = keep_alive.map(|keep_alive| AcknowledgementDeadline {
            at: Instant::now()
                .checked_add(keep_alive)
                .assured("MQTT's sixteen-bit keep-alive seconds fit the monotonic clock range"),
            keep_alive,
        });
        Self::Unrequested {
            acknowledgement,
            deadline,
        }
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

/// Why the acknowledgements an ingestor requested stay unwritten until it reads again.
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
    /// Why the driver writes nothing until the ingestor reads again, if it does, for an ingestor that
    /// has taken `taken` publishes in all from a read-ahead of `capacity`.
    ///
    /// A driver holding a publish for room the ingestor has made since is about to poll again.
    fn unwritable(&self, taken: u64, capacity: NonZeroUsize) -> Option<Unwritable> {
        let handed = match self.activity {
            DriverActivity::Polling => return None,
            DriverActivity::Ended => return Some(Unwritable::Ended),
            DriverActivity::WaitingForRoom { handed } => handed,
        };
        // An ingestor that has taken more publishes than the driver had handed over before the hold
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
    /// an ingestor that has taken `taken` publishes in all from a read-ahead of `capacity`.
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
    read_ahead: mpsc::Sender<ReadAheadPacket>,
    acknowledgement: DriverAcknowledgement,
    /// The host resolves this counter once; recording an overflow reaches no metrics registry.
    read_ahead_dropped: prometheus::IntCounter,
    /// The largest complete publish the client's configuration admits.
    admission: NonZeroU32,
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
                    let qos_zero_capacity = match self.acknowledgement.admit(&publish) {
                        ReadAheadAdmission::Accept(capacity) => capacity,
                        ReadAheadAdmission::Overflow => {
                            self.read_ahead_dropped.inc();
                            continue;
                        }
                    };
                    let packet = ReadAheadPacket {
                        packet: IngestorPacket::classify(
                            publish,
                            self.acknowledgement.ack_mode(),
                            self.admission,
                        ),
                        _qos_zero_capacity: qos_zero_capacity,
                    };
                    if let Err(ended) = self.hand_over(packet).await {
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
    async fn hand_over(
        &mut self,
        publish: ReadAheadPacket,
    ) -> Result<(), Report<MqttTopicIngestorError>> {
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
    read_ahead: mpsc::Receiver<ReadAheadPacket>,
    pending_oversized: Option<PendingOversized>,
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
    /// Starts the driver of a subscribed connection, with the read-ahead and acknowledgement mode
    /// its plan declares, the keep-alive its broker keeps, if any, and the largest complete
    /// publish its client admits.
    pub(super) fn start<E: ConnectionEvents>(
        requests: R,
        events: E,
        acknowledgement: IngestorAcknowledgement,
        keep_alive: Option<Duration>,
        admission: NonZeroU32,
        read_ahead_dropped: prometheus::IntCounter,
    ) -> Self {
        let read_ahead_capacity = acknowledgement.read_ahead();
        let (read_ahead, publishes) = mpsc::channel(read_ahead_capacity.get());
        let (progress, observed) = watch::channel(DriverProgress {
            written: 0,
            activity: DriverActivity::Polling,
        });
        let driver = ConnectionDriver {
            events,
            read_ahead,
            acknowledgement: acknowledgement.into(),
            read_ahead_dropped,
            admission,
            handed: 0,
            progress,
        };
        let driver = AbortOnDropHandle::new(nervix_primitives::task::spawn(driver.run()));
        Self {
            requests,
            read_ahead: publishes,
            pending_oversized: None,
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
            let Some(acknowledgement) = acknowledgement(position.publish()) else {
                continue;
            };
            self.enqueue(&acknowledgement).await?;
        }
        let wait = self.write_wait(self.requested).await;
        match wait {
            WriteWait::Written => Ok(()),
            WriteWait::Unwritable(unwritable) => Err(self.unwritten(unwritable).await),
        }
    }

    /// Waits until the event loop has written every acknowledgement requested on the connection,
    /// or the driver cannot poll before the ingestor reads again.
    async fn write_wait(&mut self, requested: u64) -> WriteWait {
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

    /// Queues a prepared acknowledgement, unless the driver cannot poll before the ingestor
    /// reads again, which would leave the request where nothing writes it, and returns why.
    async fn request(
        &mut self,
        acknowledgement: &ManualAck,
    ) -> MqttTopicIngestorResult<Option<Unwritable>> {
        let taken = self.taken;
        let capacity = self.read_ahead_capacity;
        let requests = &self.requests;
        let progress = &mut self.progress;
        nervix_primitives::select! {
            biased;
            requested = requests.request(acknowledgement) => match requested {
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

    /// Advances the shared acknowledgement count in the same poll that admits the request.
    async fn enqueue(&mut self, acknowledgement: &ManualAck) -> MqttTopicIngestorResult<u64> {
        let requested = self
            .requested
            .checked_add(1)
            .assured("a connection cannot request 2^64 acknowledgements in the lifetime of a node");
        if let Some(unwritable) = self.request(acknowledgement).await? {
            return Err(self.unwritten(unwritable).await);
        }
        self.requested = requested;
        Ok(requested)
    }

    /// Completes the retained discard, using the same queue, write confirmation and deadline as
    /// ordinary acknowledgements. Only the state lives across a cancelled read, never its bytes.
    async fn finish_oversized(&mut self) -> MqttTopicIngestorResult<PublishReadOutcome> {
        loop {
            let pending = self
                .pending_oversized
                .clone()
                .verified("a discard is installed before its acknowledgement is driven");
            match pending {
                PendingOversized::Deferred(acknowledgement) => {
                    let Some(acknowledgement) = acknowledgement else {
                        self.pending_oversized = None;
                        return Ok(PublishReadOutcome::Oversized);
                    };
                    self.pending_oversized =
                        Some(PendingOversized::start(acknowledgement, self.keep_alive));
                }
                PendingOversized::Unrequested {
                    acknowledgement,
                    deadline,
                } => {
                    let enqueue = self.enqueue(&acknowledgement);
                    let target = match deadline {
                        Some(deadline) => match timeout_at(deadline.at, enqueue).await {
                            Ok(queued) => queued?,
                            Err(_) => {
                                return Err(Report::new(
                                    MqttTopicIngestorError::AcknowledgementDeadline {
                                        keep_alive: deadline.keep_alive,
                                    },
                                ));
                            }
                        },
                        None => enqueue.await?,
                    };
                    self.pending_oversized = Some(PendingOversized::Requested { target, deadline });
                }
                PendingOversized::Requested { target, deadline } => {
                    let wait = self.write_wait(target);
                    let written = match deadline {
                        Some(deadline) => match timeout_at(deadline.at, wait).await {
                            Ok(written) => written,
                            Err(_) => {
                                return Err(Report::new(
                                    MqttTopicIngestorError::AcknowledgementDeadline {
                                        keep_alive: deadline.keep_alive,
                                    },
                                ));
                            }
                        },
                        None => wait.await,
                    };
                    if let WriteWait::Unwritable(unwritable) = written {
                        return Err(self.unwritten(unwritable).await);
                    }
                    self.pending_oversized = None;
                    return Ok(PublishReadOutcome::Oversized);
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

    /// Takes the next classified outcome without advancing a discard's acknowledgement.
    async fn take_packet(&mut self) -> MqttTopicIngestorResult<IngestorPacket> {
        let Some(packet) = self.read_ahead.recv().await else {
            return Err(self.ended().await);
        };
        self.taken = self
            .taken
            .checked_add(1)
            .assured("a connection cannot read 2^64 publishes in the lifetime of a node");
        // Moving the packet out releases its QoS 0 permit before any following source wait.
        Ok(packet.packet)
    }
}

impl<R: AcknowledgementRequests> PublishRead for MqttConnection<R> {
    async fn next_publish(&mut self) -> MqttTopicIngestorResult<PublishReadOutcome> {
        if self.pending_oversized.is_some() {
            return self.finish_oversized().await;
        }
        match self.take_packet().await? {
            IngestorPacket::Message(publish) => Ok(PublishReadOutcome::Message(Box::new(publish))),
            IngestorPacket::Oversized(acknowledgement) => {
                self.pending_oversized = Some(PendingOversized::Deferred(acknowledgement));
                self.finish_oversized().await
            }
        }
    }

    async fn gather_publish(&mut self) -> MqttTopicIngestorResult<GatherPublishOutcome> {
        match self.take_packet().await? {
            IngestorPacket::Message(publish) => {
                Ok(GatherPublishOutcome::Message(Box::new(publish)))
            }
            IngestorPacket::Oversized(acknowledgement) => {
                // Return the ordinary prefix before requesting this ACK: MQTT requires ACKs in
                // receive order. Its physical deadline starts when the following read begins it.
                self.pending_oversized = Some(PendingOversized::Deferred(acknowledgement));
                Ok(GatherPublishOutcome::Boundary)
            }
        }
    }
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod tests;

#[cfg(all(test, feature = "shuttle"))]
#[path = "driver_shuttle_tests.rs"]
mod shuttle_tests;
