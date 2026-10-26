//! RabbitMQ management observations, outside the product layer order.
//!
//! Layer: test harness.
//! - **Owns.** Bounded observations of consumer acknowledgement, prefetch and unsettled deliveries.
//! - **Depends on.** The explicitly provisioned broker's management API and the harness status budget.
//! - **Must not know.** Nervix consumer internals or how its graph settles acknowledgements.

use std::{io, time::Duration};

use nervix_primitives::time::{Instant, sleep};
use reqwest::Client;
use serde::Deserialize;
use url::Url;

use super::{
    dependencies::{DependencyEndpoints, RABBITMQ_MANAGEMENT_ADDR},
    status_request::STATUS_WAIT_BUDGET,
};

#[derive(Debug, Deserialize)]
struct QueueState {
    // Management statistics are absent until the broker publishes the queue's first sample.
    // Absence is pending observation; it never stands for an empty queue.
    messages: Option<usize>,
    messages_unacknowledged: Option<usize>,
    consumer_details: Option<Vec<ConsumerState>>,
}

#[derive(Debug, Deserialize)]
struct ConsumerState {
    ack_required: bool,
    prefetch_count: u16,
}

#[derive(Debug)]
pub(crate) enum QueueExpectation {
    Consumer { ack_required: bool, prefetch: u16 },
    Unacknowledged(usize),
    Outstanding(usize),
}

impl QueueExpectation {
    fn satisfied(&self, state: &QueueState) -> bool {
        match self {
            Self::Consumer {
                ack_required,
                prefetch,
            } => state.consumer_details.as_ref().is_some_and(|consumers| {
                consumers.len() == 1
                    && consumers.iter().all(|consumer| {
                        consumer.ack_required == *ack_required
                            && consumer.prefetch_count == *prefetch
                    })
            }),
            Self::Unacknowledged(expected) => state.messages_unacknowledged == Some(*expected),
            Self::Outstanding(expected) => state.messages == Some(*expected),
        }
    }
}

pub(crate) struct RabbitMqQueueProbe {
    endpoint: Url,
    client: Client,
}

impl RabbitMqQueueProbe {
    pub(crate) fn new(dependencies: &DependencyEndpoints, queue: &str) -> io::Result<Self> {
        let mut endpoint =
            Url::parse(dependencies.get(RABBITMQ_MANAGEMENT_ADDR)?).map_err(io::Error::other)?;
        endpoint
            .path_segments_mut()
            .map_err(|_| {
                io::Error::other("the RabbitMQ management endpoint accepts path segments")
            })?
            .extend(["api", "queues", "/", queue]);
        let client = Client::builder()
            .timeout(STATUS_WAIT_BUDGET)
            .build()
            .map_err(io::Error::other)?;
        Ok(Self { endpoint, client })
    }

    pub(crate) async fn wait_for(&self, expected: QueueExpectation) -> io::Result<()> {
        let deadline = Instant::now() + STATUS_WAIT_BUDGET;
        loop {
            nervix_primitives::task::consume_budget().await;
            let remaining = deadline.saturating_duration_since(Instant::now());
            let response = self
                .client
                .get(self.endpoint.clone())
                .basic_auth("guest", Some("guest"))
                .timeout(remaining)
                .send()
                .await
                .map_err(io::Error::other)?
                .error_for_status()
                .map_err(io::Error::other)?;
            let state: QueueState = response.json().await.map_err(io::Error::other)?;
            if expected.satisfied(&state) {
                return Ok(());
            }
            if deadline.saturating_duration_since(Instant::now()) <= Duration::from_millis(200) {
                return Err(io::Error::other(format!(
                    "RabbitMQ queue did not reach {expected:?}; observed {state:?}"
                )));
            }
            sleep(Duration::from_millis(200)).await;
        }
    }
}
