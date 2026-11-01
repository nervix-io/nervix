//! What a scenario observes on a core NATS subject.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One subscription per observed subject, the messages it delivers in the order the
//!   server delivered them, the count of messages the observing client received, and the connection
//!   events its driver reported for that subject.
//! - **Depends on.** The `async-nats` client and the binary's runtime.
//! - **Must not know.** What a step asserts about a message, or how another broker is observed.
//!
//! The NATS client reads what the server delivers into the subscription's own buffer on a task of
//! its own, so the observer takes messages straight from that buffer: one poll of its caller
//! takes as many buffered messages as that poll's budget allows. Bounded broker sequence
//! assertions own an independent observation task, so their drain does not wait for Cucumber to
//! finish polling its other scenarios. There is no second forwarding task or channel per message.

use std::io;

use async_nats::{ConnectOptions, Event, Message as NatsMessage, Statistics, Subscriber};
use nervix_primitives::{
    stream::StreamExt as _,
    sync::{
        Arc, StdArc,
        atomic::{AtomicU64, Ordering},
    },
};

/// A subscription to one observed subject.
#[derive(Debug)]
pub(crate) struct NatsObserver {
    subscription: Subscriber,
    client: async_nats::Client,
    /// The observing client's counters. The client shares them through the standard library's
    /// shared ownership, which is the type its API returns.
    client_statistics: StdArc<Statistics>,
    connection_events: Arc<ConnectionEvents>,
}

impl NatsObserver {
    /// Connects an observing client and subscribes while recording its driver's connection events.
    pub(crate) async fn connect(address: &str, subject: &str) -> io::Result<Self> {
        Self::connect_with_options(address, subject, ConnectOptions::new()).await
    }

    pub(super) async fn connect_with_options(
        address: &str,
        subject: &str,
        options: ConnectOptions,
    ) -> io::Result<Self> {
        let connection_events = Arc::new(ConnectionEvents::default());
        let events = Arc::clone(&connection_events);
        let observed_subject = subject.to_string();
        let client = options
            .event_callback(move |event| {
                events.record(&event);
                tracing::info!(
                    subject = %observed_subject,
                    event = %event,
                    "observing NATS client event"
                );
                std::future::ready(())
            })
            .connect(address)
            .await
            .map_err(io::Error::other)?;
        let subscription = client
            .subscribe(subject.to_string())
            .await
            .map_err(io::Error::other)?;
        Ok(Self {
            subscription,
            client_statistics: client.statistics(),
            client,
            connection_events,
        })
    }

    /// The next message the server delivered to the subscription, or `None` once the subscription
    /// has ended.
    pub(crate) async fn next(&mut self) -> Option<NatsMessage> {
        self.subscription.next().await
    }

    /// Answers one request on the reply subject the scenario expects the publisher to write.
    pub(crate) async fn answer_next(
        &mut self,
        expected_reply: &str,
        payload: &str,
    ) -> io::Result<()> {
        let message = self
            .next()
            .await
            .ok_or_else(|| io::Error::other("NATS request subscription ended"))?;
        let reply = message
            .reply
            .ok_or_else(|| io::Error::other("NATS request carries no reply subject"))?;
        if reply.as_str() != expected_reply {
            return Err(io::Error::other(format!(
                "NATS request reply subject is '{reply}', expected '{expected_reply}'"
            )));
        }
        self.client
            .publish(reply, payload.as_bytes().to_vec().into())
            .await
            .map_err(io::Error::other)?;
        self.client.flush().await.map_err(io::Error::other)
    }

    /// How many messages the observing client has received from the server.
    pub(crate) fn received(&self) -> u64 {
        self.client_statistics.in_messages.load(Ordering::Relaxed)
    }

    /// Receipt and driver-event counts at the time a sequence assertion fails. The driver's event
    /// channel is bounded, so event counts describe delivered notifications, not lost messages.
    pub(crate) fn describe_receipts(&self) -> String {
        let events = &self.connection_events;
        format!(
            "the observing NATS client had received {} messages from the server; connection \
             events: connected={}, disconnected={}, slow_consumer={}, closed={}, \
             server_errors={}, client_errors={}, lame_duck={}, draining={}",
            self.received(),
            events.connected.load(Ordering::Relaxed),
            events.disconnected.load(Ordering::Relaxed),
            events.slow_consumer.load(Ordering::Relaxed),
            events.closed.load(Ordering::Relaxed),
            events.server_errors.load(Ordering::Relaxed),
            events.client_errors.load(Ordering::Relaxed),
            events.lame_duck.load(Ordering::Relaxed),
            events.draining.load(Ordering::Relaxed),
        )
    }
}

/// Fixed-size diagnostic counters shared with the driver's event callback. They publish no
/// execution state: a relaxed load reads only how many notifications the callback has recorded.
#[derive(Debug, Default)]
struct ConnectionEvents {
    connected: AtomicU64,
    disconnected: AtomicU64,
    slow_consumer: AtomicU64,
    closed: AtomicU64,
    server_errors: AtomicU64,
    client_errors: AtomicU64,
    lame_duck: AtomicU64,
    draining: AtomicU64,
}

impl ConnectionEvents {
    fn record(&self, event: &Event) {
        let counter = match event {
            Event::Connected => &self.connected,
            Event::Disconnected => &self.disconnected,
            Event::SlowConsumer(_) => &self.slow_consumer,
            Event::Closed => &self.closed,
            Event::ServerError(_) => &self.server_errors,
            Event::ClientError(_) => &self.client_errors,
            Event::LameDuckMode => &self.lame_duck,
            Event::Draining => &self.draining,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}
