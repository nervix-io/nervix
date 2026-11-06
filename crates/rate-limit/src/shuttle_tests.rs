//! Keyed production leases against concurrent admission, eviction and observation.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use governor::{RateLimiter, clock::Clock, nanos::Nanos};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_model_harness::shuttle::check_random_and_pct;
use nervix_primitives::{sync::Arc, thread};

use crate::{
    KeyFingerprint, KeyedOptions, Lease, LeaseKind, Quota, cell::RelativeStore, keyed::KeyedStore,
};

struct Captured;
impl Clock for Captured {
    type Instant = Nanos;
    fn now(&self) -> Nanos {
        0.into()
    }
}

fn charge(lease: &Lease<'_>, quota: Quota, now: u64) {
    let relative = RelativeStore {
        cell: lease.cell(),
        now,
    };
    let limiter: RateLimiter<_, _, _, governor::middleware::NoOpMiddleware<Nanos>> =
        RateLimiter::new(quota.governor, relative, Captured);
    assert!(
        limiter
            .check_n(NonZeroU32::MIN)
            .assured("cost fits")
            .is_ok()
    );
}

#[test]
fn shuttle_key_leases_refund_exactly_and_never_exceed_the_global_capacity() {
    check_random_and_pct(|| {
        let options = KeyedOptions::new(
            Duration::from_nanos(10),
            2,
            NonZeroUsize::new(2).assured("two shards"),
        )
        .assured("valid options");
        let store = Arc::new(KeyedStore::new(options));
        let quota = Quota::new(1, Duration::from_nanos(10), 4).assured("valid quota");
        // The first modeled accesses race to expire the retained keys at their exact TTL.
        for identity in [8, 9] {
            let keyed = store
                .prepare(KeyFingerprint::from([identity; 16]), Duration::ZERO)
                .assured("visit range")
                .assured("positive capacity");
            let lease = Lease::new(LeaseKind::Keyed(keyed));
            charge(&lease, quota, 0);
            lease.commit();
        }
        let mut participants = Vec::new();
        for identity in 0..3u8 {
            let store = store.clone();
            participants.push(thread::spawn(move || {
                let key = KeyFingerprint::from([identity; 16]);
                for attempt in 0..2 {
                    let keyed = store
                        .prepare(key, Duration::from_nanos(10))
                        .assured("visit range")
                        .assured("positive capacity");
                    let mut lease = Lease::new(LeaseKind::Keyed(keyed));
                    let before = lease.cell().horizon();
                    charge(&lease, quota, 10);
                    if attempt == 0 {
                        lease.rollback();
                        assert_eq!(
                            lease.cell().horizon(),
                            before,
                            "a refunded key retained a charge"
                        );
                        drop(lease);
                    } else {
                        lease.commit();
                    }
                    assert!(
                        store.statistics(Duration::from_nanos(10)).active <= 2,
                        "key capacity exceeded"
                    );
                }
            }));
        }
        for participant in participants {
            participant
                .join()
                .assured("bounded keyed transactions finish");
        }
        assert_eq!(store.statistics(Duration::from_nanos(10)).active, 2);
        let key = KeyFingerprint::from([4; 16]);
        let keyed = store
            .prepare(key, Duration::from_nanos(20))
            .assured("visit range")
            .assured("positive capacity");
        Lease::new(LeaseKind::Keyed(keyed)).commit();
        assert_eq!(
            store.statistics(Duration::from_nanos(20)).active,
            1,
            "TTL did not release the prior keys"
        );
    });
}
