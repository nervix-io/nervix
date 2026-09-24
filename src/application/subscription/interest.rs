//! This node's interest in the relays its session subscriptions read.
//!
//! Layer: control plane.
//!
//! - **Owns.** The count of this node's subscriptions attached to each relay, the lease each
//!   subscription holds on that count from the moment it attaches until it closes, keeping the
//!   node's gossip advertisement in step with the count, and the series that exposes the count.
//! - **Depends on.** Cluster membership for the advertisement, and the runtime's metrics registry.
//! - **Must not know.** Sessions, requests, delivery, or which node owns a relay.
//!
//! A node advertises interest in a relay exactly while it holds at least one lease on it, and a
//! relay owner fans its batches out to every node that advertises interest. A lease is released
//! exactly once: when its subscription closes, or when it is dropped on a path that could not
//! close it. The advertisement therefore never outlives a node's last subscription to a relay, and
//! never lapses while one is open, however many subscriptions of however many sessions share the
//! relay.
//!
//! Changing a count and writing the advertisement are separate steps, and leases on one relay
//! change concurrently. Every write therefore reads the count it publishes while it holds the
//! membership lock that orders the writes, so the last write always matches the count, and a
//! release that finishes late cannot withdraw the interest of a subscription that attached after
//! it.

use std::num::NonZeroUsize;

use ahash::RandomState;
use dashmap::mapref::entry::Entry;
use meticulous::OptionExt as _;
use nervix_execution::sync::DashMap;
use nervix_models::{DomainName, RelayName};
use triomphe::Arc;

use crate::{cluster::ClusterHandle, metrics::RuntimeMetrics};

/// One relay this node's subscriptions read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SubscriptionInterestKey {
    domain: DomainName,
    relay: RelayName,
}

struct SubscriptionInterestsInner {
    /// The leases held on each relay. A relay has an entry exactly while a lease on it is held.
    leases: DashMap<SubscriptionInterestKey, NonZeroUsize, RandomState>,
    /// Also held by the application, which starts and stops cluster membership.
    cluster: Arc<ClusterHandle>,
    metrics: RuntimeMetrics,
}

/// This node's subscription interest. Cloning it is one reference count.
#[derive(Clone)]
pub(in crate::application) struct SubscriptionInterests {
    inner: Arc<SubscriptionInterestsInner>,
}

/// One subscription's hold on this node's interest in the relay it reads.
pub(in crate::application) struct SubscriptionInterestLease {
    interests: SubscriptionInterests,
    key: SubscriptionInterestKey,
    /// Cleared by [`Self::release`], so dropping a released lease releases nothing twice.
    held: bool,
}

impl SubscriptionInterests {
    pub(in crate::application) fn new(
        cluster: Arc<ClusterHandle>,
        metrics: RuntimeMetrics,
    ) -> Self {
        Self {
            inner: Arc::new(SubscriptionInterestsInner {
                leases: DashMap::with_hasher(RandomState::new()),
                cluster,
                metrics,
            }),
        }
    }

    /// Takes a lease on this node's interest in `relay` and advertises the interest before it
    /// returns. Making the advertisement visible to the other nodes is the caller's handshake.
    pub(in crate::application) async fn acquire(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> SubscriptionInterestLease {
        let key = SubscriptionInterestKey {
            domain: domain.clone(),
            relay: relay.clone(),
        };
        self.count_up(&key);
        let lease = SubscriptionInterestLease {
            interests: self.clone(),
            key,
            held: true,
        };
        self.publish(&lease.key).await;
        lease
    }

    /// How many leases this node holds on `relay`.
    #[cfg(test)]
    pub(in crate::application) fn leases(&self, domain: &DomainName, relay: &RelayName) -> usize {
        let key = SubscriptionInterestKey {
            domain: domain.clone(),
            relay: relay.clone(),
        };
        match self.inner.leases.get(&key) {
            Some(leases) => leases.get(),
            None => 0,
        }
    }

    fn count_up(&self, key: &SubscriptionInterestKey) {
        let leases = match self.inner.leases.entry(key.clone()) {
            Entry::Occupied(mut held) => {
                let leases = held
                    .get()
                    .checked_add(1)
                    .assured("every lease is a live subscription, and no node holds usize::MAX");
                held.insert(leases);
                leases
            }
            Entry::Vacant(vacant) => *vacant.insert(NonZeroUsize::MIN),
        };
        self.inner
            .metrics
            .set_session_subscriptions(&key.domain, &key.relay, leases.get());
    }

    fn count_down(&self, key: &SubscriptionInterestKey) {
        let occupied = match self.inner.leases.entry(key.clone()) {
            Entry::Occupied(held) => Some(held),
            Entry::Vacant(_) => None,
        };
        let mut held =
            occupied.assured("a lease is counted from its acquisition until its release");
        let remaining = held
            .get()
            .get()
            .checked_sub(1)
            .assured("a counted relay holds at least one lease");
        match NonZeroUsize::new(remaining) {
            Some(leases) => {
                held.insert(leases);
            }
            None => {
                held.remove();
            }
        }
        self.inner
            .metrics
            .set_session_subscriptions(&key.domain, &key.relay, remaining);
    }

    /// Makes the advertisement of `key` match its current count.
    async fn publish(&self, key: &SubscriptionInterestKey) {
        let leases = &self.inner.leases;
        self.inner
            .cluster
            .reconcile_local_subscription_interest(key.domain.as_str(), key.relay.as_str(), || {
                leases.contains_key(key)
            })
            .await;
    }
}

impl SubscriptionInterestLease {
    /// Releases the lease, and withdraws the node's interest in the relay when it was the last.
    pub(in crate::application) async fn release(mut self) {
        self.held = false;
        self.interests.count_down(&self.key);
        self.interests.publish(&self.key).await;
    }
}

impl Drop for SubscriptionInterestLease {
    /// A lease dropped without [`SubscriptionInterestLease::release`] belonged to work that was
    /// cancelled or panicked. Its count is returned at once, and the advertisement follows it as
    /// soon as the node schedules the write, so the interest cannot leak.
    fn drop(&mut self) {
        if !self.held {
            return;
        }
        self.interests.count_down(&self.key);
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let interests = self.interests.clone();
        let key = self.key.clone();
        drop(runtime.spawn(async move {
            interests.publish(&key).await;
        }));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use meticulous::ResultExt as _;
    use nervix_recovery::Discarded as _;
    use tokio::time::timeout;

    use super::*;
    use crate::application::test_fixtures::{TestService, build_test_service, named};

    async fn advertised(interests: &SubscriptionInterests) -> bool {
        interests
            .inner
            .cluster
            .advertises_subscription_interest("default", "events")
            .await
    }

    #[tokio::test]
    async fn the_advertisement_follows_the_lease_count_whatever_order_writes_finish_in() {
        let TestService { service, path, .. } = build_test_service(false).await;
        let interests = service.inner.subscription_interests.clone();
        let domain = named::<DomainName>("default");
        let relay = named::<RelayName>("events");

        let first = interests.acquire(&domain, &relay).await;
        let second = interests.acquire(&domain, &relay).await;
        assert!(advertised(&interests).await);
        first.release().await;
        assert!(
            advertised(&interests).await,
            "the relay's other lease keeps the node's interest"
        );

        // A release that returned its count before a new lease was taken, and writes the
        // advertisement only after that lease's own write.
        let mut late = second;
        late.held = false;
        interests.count_down(&late.key);
        let third = interests.acquire(&domain, &relay).await;
        interests.publish(&late.key).await;
        assert!(
            advertised(&interests).await,
            "a release that finishes late cannot withdraw a newer lease's interest"
        );
        drop(late);
        assert_eq!(interests.leases(&domain, &relay), 1);

        third.release().await;
        assert!(!advertised(&interests).await);
        assert_eq!(interests.leases(&domain, &relay), 0);

        let abandoned = interests.acquire(&domain, &relay).await;
        drop(abandoned);
        assert_eq!(
            interests.leases(&domain, &relay),
            0,
            "a lease dropped without a release returns its count at once"
        );
        timeout(Duration::from_secs(30), async {
            while advertised(&interests).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .assured("the advertisement of a dropped lease follows its count");
        std::fs::remove_dir_all(path).discarded("the test directory is disposable");
    }
}
