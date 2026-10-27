//! A bounded subscription sample independent of how its caller is scheduled.
//!
//! Layer: test harness.
//! - **Owns.** Expected payload multiplicities, receipt instants and the sample's deadline.
//! - **Depends on.** A public subscription reader, the harness's monotonic clock and its owned
//!   runtime observation task.
//! - **Must not know.** Scenario state, graph placement or runtime buffering internals.

use std::{collections::BTreeMap, future::Future, io, time::Duration};

use meticulous::OptionExt as _;
use nervix_primitives::time::Instant;

use super::runner_observation::observe_off_the_runner;

/// A public subscription read, including the budget for this one read.
pub(crate) trait PayloadSubscription: Send {
    fn next_payload(
        &mut self,
        budget: Duration,
    ) -> impl Future<Output = io::Result<Option<String>>> + Send;
}

/// One row and the instant its reader received it.
#[derive(Debug)]
pub(crate) struct PayloadReceipt {
    pub(crate) payload: String,
    pub(crate) received_at: Instant,
}

/// A finished sample returns ownership of the same subscription to its caller.
pub(crate) struct ObservedPayloads<S> {
    pub(crate) subscription: S,
    pub(crate) receipts: Vec<PayloadReceipt>,
}

/// Matches every expected fragment as often as it was requested.
pub(crate) struct SubscriptionPayloads {
    remaining: BTreeMap<String, usize>,
    receipts: Vec<PayloadReceipt>,
}

impl SubscriptionPayloads {
    pub(crate) fn new(expected: &[String]) -> Self {
        assert!(!expected.is_empty(), "a payload sample needs expected rows");
        let mut remaining = BTreeMap::new();
        for fragment in expected {
            *remaining.entry(fragment.clone()).or_insert(0usize) += 1;
        }
        Self {
            remaining,
            receipts: Vec::with_capacity(expected.len()),
        }
    }

    pub(crate) fn observe<S: PayloadSubscription + 'static>(
        mut self,
        mut subscription: S,
        budget: Duration,
    ) -> impl Future<Output = ObservedPayloads<S>> + Send {
        let began_at = Instant::now();
        let deadline = began_at
            .checked_add(budget)
            .assured("the scenario's bounded receive budget fits a monotonic instant");
        observe_off_the_runner(async move {
            while !self.remaining.is_empty() {
                nervix_primitives::task::consume_budget().await;
                let now = Instant::now();
                assert!(
                    now < deadline,
                    "timed out waiting for subscription payloads. expected remaining {:?}, \
                     receipts {:?}; budget {budget:?}, began {began_at:?}, deadline {deadline:?}, \
                     checked {now:?}",
                    self.remaining,
                    self.receipts
                );
                let wait = deadline
                    .checked_duration_since(now)
                    .verified("the deadline comparison established remaining receive time");
                let payload = match subscription.next_payload(wait).await {
                    Ok(Some(payload)) => payload,
                    Ok(None) => {
                        panic!(
                            "timed out waiting for subscription payloads. expected remaining \
                             {:?}, receipts {:?}; budget {budget:?}, began {began_at:?}, deadline \
                             {deadline:?}",
                            self.remaining, self.receipts
                        )
                    }
                    Err(error) => panic!("failed while waiting for subscription payloads: {error}"),
                };
                let received_at = Instant::now();
                assert!(
                    received_at < deadline,
                    "timed out waiting for subscription payloads. expected remaining {:?}, \
                     receipts {:?}; received {payload:?} at {received_at:?}; budget {budget:?}, \
                     began {began_at:?}, deadline {deadline:?}",
                    self.remaining,
                    self.receipts
                );
                let fragment = self
                    .remaining
                    .keys()
                    .find(|fragment| payload.contains(fragment.as_str()))
                    .cloned();
                self.receipts.push(PayloadReceipt {
                    payload,
                    received_at,
                });
                if let Some(fragment) = fragment {
                    let count = self
                        .remaining
                        .get_mut(&fragment)
                        .verified("the matched fragment is still in the remaining sample");
                    *count = count
                        .checked_sub(1)
                        .verified("a remaining fragment still needs at least one occurrence");
                    if *count == 0 {
                        self.remaining.remove(&fragment);
                    }
                }
            }
            ObservedPayloads {
                subscription,
                receipts: self.receipts,
            }
        })
    }
}
