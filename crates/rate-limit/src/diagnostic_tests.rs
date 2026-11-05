//! Real tracked-lock execution of the production key leases and generation restoration.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use chrono::{TimeZone as _, Utc};
use governor::{
    nanos::Nanos,
    state::{NotKeyed, StateStore as _},
};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::{sync::Arc, thread, time::Instant};

use crate::{
    Admission, Charge, Clause, KeyFingerprint, KeyedOptions, Lease, LeaseKind, Limiter, Quota,
    keyed::KeyedStore,
};

#[test]
fn deloxide_keyed_transactions_expire_evict_refund_and_restore() {
    let directory =
        std::env::var_os("NERVIX_DEADLOCK_EVIDENCE").map(nervix_deadlock::EvidenceDirectory::new);
    nervix_deadlock::DiagnosticRun::start(directory)
        .assured("this process installs its detector before constructing a tracked lock");
    let options = KeyedOptions::new(
        Duration::from_nanos(10),
        2,
        NonZeroUsize::new(2).assured("two shards"),
    )
    .assured("valid keys");
    let store = Arc::new(KeyedStore::new(options));
    for identity in [8, 9] {
        let keyed = store
            .prepare(KeyFingerprint::from([identity; 16]), Duration::ZERO)
            .assured("visit range")
            .assured("positive capacity");
        Lease::new(LeaseKind::Keyed(keyed)).commit();
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
                lease
                    .cell()
                    .measure_and_replace(&NotKeyed::NonKey, |horizon| {
                        let horizon = horizon.assured("the store supplies its horizon").as_u64();
                        Ok::<_, ()>((
                            (),
                            Nanos::from(
                                horizon
                                    .checked_add(1)
                                    .assured("two bounded test charges fit"),
                            ),
                        ))
                    })
                    .assured("conforming store update");
                if attempt == 0 {
                    lease.rollback();
                    assert_eq!(lease.cell().horizon(), before);
                    drop(lease);
                } else {
                    lease.commit();
                }
                assert!(store.statistics(Duration::from_nanos(10)).active <= 2);
            }
        }));
    }
    for participant in participants {
        participant
            .join()
            .assured("bounded store contenders finish");
    }
    let keyed = store
        .prepare(KeyFingerprint::from([9; 16]), Duration::from_nanos(20))
        .assured("visit range")
        .assured("positive capacity");
    Lease::new(LeaseKind::Keyed(keyed)).commit();
    assert_eq!(store.statistics(Duration::from_nanos(20)).active, 1);

    let now = Instant::now();
    let quota = Quota::new(1, Duration::from_nanos(20), 2).assured("valid quota");
    let mut limiter = Limiter::new(vec![Clause::keyed(quota, options)], now);
    let charges = [Charge::Keyed {
        key: KeyFingerprint::from([1; 16]),
        cost: NonZeroU32::new(1).assured("one cell"),
    }];
    assert_eq!(
        limiter.admit_at(&charges, now).assured("valid charge"),
        Admission::Admitted
    );
    let generation = limiter.generation(now).assured("same clock");
    let utc = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .assured("valid UTC fixture");
    let generation = generation
        .to_utc(utc)
        .assured("UTC fits")
        .from_utc(utc)
        .assured("same anchor");
    let restored =
        Limiter::from_generation(generation.clone(), now).assured("validated fresh store");
    assert_eq!(restored.generation(now).assured("same clock"), generation);
}
