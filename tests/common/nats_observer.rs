//! What a scenario observes on a core NATS subject.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** One subscription per observed subject, the messages it delivers in the order the
//!   server delivered them, and the count of messages the observing client received.
//! - **Depends on.** The `async-nats` client and the binary's runtime.
//! - **Must not know.** What a step asserts about a message, or how another broker is observed.
//!
//! The NATS client reads what the server delivers into the subscription's own buffer on a task of
//! its own, so a step takes its messages straight from that buffer: one poll of the runner task
//! takes as many buffered messages as that poll's budget allows. Every running scenario shares the
//! runner task, so a step that waited on another task for each message would wait once per message
//! for the runner task to come back to it.

use std::io;

use async_nats::{Client as NatsClient, Message as NatsMessage, Statistics, Subscriber};
use nervix_primitives::{
    stream::StreamExt as _,
    sync::{StdArc, atomic::Ordering},
};

/// A subscription to one observed subject.
#[derive(Debug)]
pub(crate) struct NatsObserver {
    subscription: Subscriber,
    /// The observing client's counters. The client shares them through the standard library's
    /// shared ownership, which is the type its API returns.
    client_statistics: StdArc<Statistics>,
}

impl NatsObserver {
    /// Subscribes `client` to `subject`.
    pub(crate) async fn subscribe(client: &NatsClient, subject: &str) -> io::Result<Self> {
        let subscription = client
            .subscribe(subject.to_string())
            .await
            .map_err(io::Error::other)?;
        Ok(Self {
            subscription,
            client_statistics: client.statistics(),
        })
    }

    /// The next message the server delivered to the subscription, or `None` once the subscription
    /// has ended.
    pub(crate) async fn next(&mut self) -> Option<NatsMessage> {
        self.subscription.next().await
    }

    /// How many messages the observing client has received from the server.
    pub(crate) fn received(&self) -> u64 {
        self.client_statistics.in_messages.load(Ordering::Relaxed)
    }
}
