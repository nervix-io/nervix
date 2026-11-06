//! Failure quotas for every presented authentication name on one node.
//!
//! Layer: control plane.
//!
//! - **Owns.** Per-name quota debt, admission waits and retirement of fully recovered buckets.
//! - **Depends on.** Governor's quota decisions, the expiry map and the primitive boundary.
//! - **Must not know.** Passwords, whether a name names a user, or a client transport.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns per-name authentication pacing on one node"
    )
)]

use std::{cell::Cell, num::NonZeroU32, time::Duration};

use governor::{
    Quota, RateLimiter,
    clock::Clock,
    middleware::NoOpMiddleware,
    nanos::Nanos,
    state::{NotKeyed, StateStore},
};
use meticulous::OptionExt as _;
use nervix_expiry_map::ExpiryMap;
use nervix_primitives::{
    sync::{Arc, Mutex},
    task::consume_budget,
    time::{Instant, sleep},
};
use nervix_recovery::Discarded as _;

pub(in crate::application) struct AuthenticationPacing {
    quota: Quota,
    names: Mutex<ExpiryMap<String, AuthenticationBucket>>,
}

/// The bucket whose slot this attempt took before credentials work. The shared key keeps that
/// allocation distinct even if success or full recovery removes its bucket during the work.
#[derive(Debug)]
pub(super) enum CredentialPacing {
    Unpaced,
    Paced(Arc<String>),
}

impl AuthenticationPacing {
    pub(super) fn new(rate: NonZeroU32) -> Self {
        Self {
            quota: Quota::per_second(rate),
            names: Mutex::new(ExpiryMap::new()),
        }
    }

    pub(super) async fn before_verification(&self, name: &String) -> CredentialPacing {
        loop {
            consume_budget().await;
            let wait = {
                let mut names = self.names.lock().await;
                let now = Instant::now();
                Self::retire_recovered(&mut names, now);
                let Some((key, bucket)) = names.renew(name) else {
                    return CredentialPacing::Unpaced;
                };
                if bucket.theoretical_arrival <= now {
                    names.remove(name).discarded(
                        "a fully recovered bucket is indistinguishable from a fresh quota",
                    );
                    return CredentialPacing::Unpaced;
                }
                match bucket.take_slot(self.quota, now) {
                    Some(wait) => wait,
                    None => return CredentialPacing::Paced(key.clone()),
                }
            };
            sleep(wait).await;
        }
    }

    /// Only a judged refusal establishes failure pacing. An attempt that found no debt before
    /// its hash still pays a slot here, including when another such attempt finished first. A
    /// receipt pays only its own active bucket; success or full recovery starts a fresh quota.
    pub(super) async fn refused(&self, name: String, admission: CredentialPacing) {
        loop {
            consume_budget().await;
            let wait = {
                let mut names = self.names.lock().await;
                let now = Instant::now();
                Self::retire_recovered(&mut names, now);
                if let CredentialPacing::Paced(charged) = &admission
                    && names.get_shared(&name).is_some_and(|(key, bucket)| {
                        Arc::ptr_eq(charged, key) && bucket.theoretical_arrival > now
                    })
                {
                    return;
                }
                match names.renew(&name) {
                    Some((_, bucket)) if bucket.theoretical_arrival > now => {
                        bucket.take_slot(self.quota, now)
                    }
                    recovered => {
                        if recovered.is_some() {
                            names.remove(&name).discarded(
                                "a fully recovered bucket starts with a fresh receipt identity",
                            );
                        }
                        let mut bucket = AuthenticationBucket {
                            theoretical_arrival: now,
                        };
                        assert!(
                            bucket.take_slot(self.quota, now).is_none(),
                            "a fresh nonzero quota always admits its first slot"
                        );
                        assert!(
                            names.insert(name, bucket),
                            "the missing name stays absent under the exclusive map guard"
                        );
                        return;
                    }
                }
            };
            let Some(wait) = wait else {
                return;
            };
            sleep(wait).await;
        }
    }

    pub(super) async fn authenticated(&self, name: &String) {
        self.names.lock().await.remove(name).discarded(
            "an authenticated name retains no failure quota, whether or not one existed",
        );
    }

    fn retire_recovered(names: &mut ExpiryMap<String, AuthenticationBucket>, now: Instant) {
        if names
            .oldest()
            .is_some_and(|(_, bucket)| bucket.theoretical_arrival <= now)
        {
            names
                .remove_oldest()
                .discarded("the oldest bucket's full quota no longer carries any debt");
        }
    }

    #[cfg(all(test, feature = "testing"))]
    pub(super) async fn is_empty(&self) -> bool {
        self.names.lock().await.is_empty()
    }
}

struct AuthenticationBucket {
    theoretical_arrival: Instant,
}

impl AuthenticationBucket {
    /// Governor keeps the quota algorithm; this adapter only reanchors its arrival mark at the
    /// caller's selected instant. A per-second quota bounds the remaining mark by one full burst,
    /// so the conversion never depends on the node's uptime or a backend clock/atomic store.
    fn take_slot(&mut self, quota: Quota, now: Instant) -> Option<Duration> {
        let state = Cell::new(Some(Nanos::from(
            self.theoretical_arrival.saturating_duration_since(now),
        )));
        let limiter = RateLimiter::<NotKeyed, _, _, NoOpMiddleware<Nanos>>::new(
            quota,
            SnapshotState(&state),
            SnapshotClock,
        );
        let wait = limiter
            .check()
            .err()
            .map(|limited| limited.wait_time_from(Nanos::new(0)));
        self.theoretical_arrival = now
            + Duration::from(state.get().assured(
                "the bucket starts with a mark and governor replaces it only with another mark",
            ));
        wait
    }
}

/// A synchronous decision at the normalized instant zero; this reads no physical clock.
struct SnapshotClock;

impl Clock for SnapshotClock {
    type Instant = Nanos;

    fn now(&self) -> Nanos {
        Nanos::new(0)
    }
}

/// The map guard exclusively owns the bucket; this cell lives only for its synchronous decision.
struct SnapshotState<'a>(&'a Cell<Option<Nanos>>);

impl StateStore for SnapshotState<'_> {
    type Key = NotKeyed;

    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(
            reason = "governor supplies one synchronous quota update that neither awaits nor \
                      escapes the exclusively owned snapshot"
        )
    )]
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            library,
            reason = "governor's state-store callback preserves its exact typed quota outcome"
        )
    )]
    fn measure_and_replace<T, F, E>(&self, _key: &Self::Key, f: F) -> Result<T, E>
    where
        F: Fn(Option<Nanos>) -> Result<(T, Nanos), E>,
    {
        let (outcome, updated) = f(self.0.get())?;
        self.0.set(Some(updated));
        Ok(outcome)
    }
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]
mod tests;
