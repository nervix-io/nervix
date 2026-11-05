use std::{
    collections::BTreeSet,
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::time::Instant;

use super::{
    Admission, Charge, Clause, ClauseGeneration, KeyFingerprint, KeyedOptions, Limiter, Quota,
};

fn quota(count: u64, nanos: u64, burst: u64) -> Quota {
    Quota::new(count, Duration::from_nanos(nanos), burst).assured("valid test quota")
}

fn cost(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).assured("positive test cost")
}

#[test]
fn quota_refuses_unrepresentable_rates_bursts_and_costs() {
    assert!(Quota::new(0, Duration::from_secs(1), 1).is_err());
    assert!(Quota::new(1, Duration::ZERO, 1).is_err());
    assert!(Quota::new(11, Duration::from_nanos(10), 1).is_err());
    assert!(Quota::new(1, Duration::from_secs(1), 0).is_err());
    assert!(Quota::new(1, Duration::from_secs(1), u64::from(u32::MAX) + 1).is_err());
    assert!(Quota::cost(0).is_err());
    assert!(Quota::cost(u64::from(u32::MAX) + 1).is_err());
}

#[test]
fn a_later_refusal_refunds_every_earlier_clause_and_reports_the_latest_wait() {
    let now = Instant::now();
    let mut limiter = Limiter::new(
        vec![
            Clause::direct(quota(1, 10, 2)),
            Clause::direct(quota(1, 30, 1)),
            Clause::direct(quota(1, 50, 1)),
        ],
        now,
    );
    let charges = [
        Charge::Direct(cost(1)),
        Charge::Direct(cost(1)),
        Charge::Direct(cost(1)),
    ];
    assert_eq!(
        limiter.admit_at(&charges, now).assured("valid charges"),
        Admission::Admitted
    );
    let before = limiter.generation(now).assured("same clock");
    assert_eq!(
        limiter.admit_at(&charges, now).assured("valid charges"),
        Admission::WaitUntil(now + Duration::from_nanos(50))
    );
    assert_eq!(limiter.generation(now).assured("same clock"), before);
}

#[test]
fn a_cost_above_a_burst_never_charges_an_earlier_clause() {
    let now = Instant::now();
    let mut limiter = Limiter::new(
        vec![
            Clause::direct(quota(1, 10, 3)),
            Clause::direct(quota(1, 10, 1)),
        ],
        now,
    );
    let before = limiter.generation(now).assured("same clock");
    assert!(matches!(
        limiter
            .admit_at(&[Charge::Direct(cost(1)), Charge::Direct(cost(2))], now)
            .assured("valid charges"),
        Admission::NeverAdmissible { clause: 1, .. }
    ));
    assert_eq!(limiter.generation(now).assured("same clock"), before);
}

#[test]
fn key_capacity_is_global_across_shards_and_eviction_follows_last_use() {
    let now = Instant::now();
    let options = KeyedOptions::new(
        Duration::from_nanos(100),
        2,
        NonZeroUsize::new(2).assured("two shards"),
    )
    .assured("valid keys");
    let mut limiter = Limiter::new(vec![Clause::keyed(quota(1, 100, 2), options)], now);
    let a = KeyFingerprint::from([0; 16]);
    let b = KeyFingerprint::from([1; 16]);
    let c = KeyFingerprint::from([2; 16]);
    for key in [a, b, a, c] {
        assert_eq!(
            limiter
                .admit_at(&[Charge::Keyed { key, cost: cost(1) }], now)
                .assured("valid key"),
            Admission::Admitted
        );
    }
    let statistics = limiter.key_statistics(now).assured("same clock");
    assert_eq!(statistics.active, 2);
    assert_eq!(statistics.evicted, 1);
    let generation = limiter.generation(now).assured("same clock");
    let mut keys = BTreeSet::new();
    for clause in generation.clauses {
        if let ClauseGeneration::Keyed { keys: entries, .. } = clause {
            for entry in entries {
                keys.insert(entry.key);
            }
        }
    }
    assert_eq!(keys, BTreeSet::from([a, c]));
}

#[test]
fn utc_conversion_keeps_every_key_age_and_lru_ordinal() {
    use chrono::{TimeZone as _, Utc};
    let now = Instant::now();
    let options = KeyedOptions::new(
        Duration::from_nanos(100),
        3,
        NonZeroUsize::new(2).assured("two shards"),
    )
    .assured("valid keys");
    let mut limiter = Limiter::new(
        vec![
            Clause::direct(quota(1, 30, 1)),
            Clause::keyed(quota(1, 50, 2), options),
        ],
        now,
    );
    let key = KeyFingerprint::from([7; 16]);
    assert_eq!(
        limiter
            .admit_at(
                &[
                    Charge::Direct(cost(1)),
                    Charge::Keyed { key, cost: cost(1) }
                ],
                now
            )
            .assured("valid payload"),
        Admission::Admitted
    );
    let observed = now + Duration::from_nanos(10);
    let state = limiter.generation(observed).assured("clock advanced");
    let utc = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .assured("valid UTC fixture");
    assert_eq!(
        state
            .to_utc(utc)
            .assured("UTC fits")
            .from_utc(utc)
            .assured("same anchor"),
        state
    );
}

#[test]
fn utc_downtime_discards_past_horizons_and_expired_keys() {
    use chrono::{TimeDelta, TimeZone as _, Utc};
    let now = Instant::now();
    let options = KeyedOptions::new(
        Duration::from_nanos(10),
        1,
        NonZeroUsize::new(1).assured("one shard"),
    )
    .assured("valid keys");
    let mut limiter = Limiter::new(
        vec![
            Clause::direct(quota(1, 10, 1)),
            Clause::keyed(quota(1, 50, 2), options),
        ],
        now,
    );
    let key = KeyFingerprint::from([8; 16]);
    limiter
        .admit_at(
            &[
                Charge::Direct(cost(1)),
                Charge::Keyed { key, cost: cost(1) },
            ],
            now,
        )
        .assured("valid payload");
    let utc = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .assured("valid UTC fixture");
    let state = limiter
        .generation(now)
        .assured("same clock")
        .to_utc(utc)
        .assured("UTC fits");
    let elapsed = state
        .from_utc(utc + TimeDelta::nanoseconds(10))
        .assured("positive downtime");
    assert!(matches!(
        elapsed.clauses[0],
        ClauseGeneration::Direct {
            remaining: None,
            ..
        }
    ));
    let ClauseGeneration::Keyed { keys, .. } = &elapsed.clauses[1] else {
        unreachable!("keyed declaration");
    };
    assert!(keys.is_empty());
}

#[test]
fn a_refused_keyed_payload_neither_spends_nor_evicts_an_existing_key() {
    let now = Instant::now();
    let options = KeyedOptions::new(
        Duration::from_nanos(100),
        1,
        NonZeroUsize::new(2).assured("two shards"),
    )
    .assured("valid keys");
    let mut limiter = Limiter::new(
        vec![
            Clause::keyed(quota(1, 100, 2), options),
            Clause::direct(quota(1, 30, 1)),
        ],
        now,
    );
    let a = KeyFingerprint::from([0; 16]);
    let b = KeyFingerprint::from([1; 16]);
    limiter
        .admit_at(
            &[
                Charge::Keyed {
                    key: a,
                    cost: cost(1),
                },
                Charge::Direct(cost(1)),
            ],
            now,
        )
        .assured("first payload");
    let before = limiter.generation(now).assured("same clock");
    assert_eq!(
        limiter
            .admit_at(
                &[
                    Charge::Keyed {
                        key: b,
                        cost: cost(1)
                    },
                    Charge::Direct(cost(1))
                ],
                now
            )
            .assured("second payload"),
        Admission::WaitUntil(now + Duration::from_nanos(30))
    );
    assert_eq!(limiter.generation(now).assured("same clock"), before);
    assert_eq!(limiter.key_statistics(now).assured("same clock").evicted, 0);
}

#[test]
fn zero_key_budget_refunds_other_clauses() {
    let now = Instant::now();
    let options = KeyedOptions::new(
        Duration::from_nanos(100),
        0,
        NonZeroUsize::new(1).assured("one shard"),
    )
    .assured("zero capacity is an actual budget");
    let mut limiter = Limiter::new(
        vec![
            Clause::direct(quota(1, 10, 1)),
            Clause::keyed(quota(1, 100, 2), options),
        ],
        now,
    );
    let before = limiter.generation(now).assured("same clock");
    assert_eq!(
        limiter
            .admit_at(
                &[
                    Charge::Direct(cost(1)),
                    Charge::Keyed {
                        key: KeyFingerprint::from([0; 16]),
                        cost: cost(1)
                    }
                ],
                now
            )
            .assured("valid payload"),
        Admission::KeyBudgetExhausted {
            clause: 1,
            limit: 0
        }
    );
    assert_eq!(limiter.generation(now).assured("same clock"), before);
}

#[test]
fn statistics_releases_expired_keys_without_counting_them_as_evictions() {
    let now = Instant::now();
    let options =
        KeyedOptions::new(Duration::from_nanos(10), 1, NonZeroUsize::MIN).assured("bounded keys");
    let mut limiter = Limiter::new(vec![Clause::keyed(quota(1, 50, 1), options)], now);
    limiter
        .admit_at(
            &[Charge::Keyed {
                key: KeyFingerprint::from([1; 16]),
                cost: cost(1),
            }],
            now,
        )
        .assured("valid charge");
    assert_eq!(
        limiter
            .key_statistics(now + Duration::from_nanos(9))
            .assured("advanced clock")
            .active,
        1
    );
    assert_eq!(
        limiter
            .key_statistics(now + Duration::from_nanos(10))
            .assured("TTL boundary"),
        super::KeyStatistics::default()
    );
    assert!(limiter.key_statistics(now).is_err());
}

#[test]
fn rounded_rate_and_whole_burst_obey_the_exact_refill_boundary() {
    let now = Instant::now();
    let quota = quota(3, 10, 2);
    assert_eq!(quota.replenish_interval(), Duration::from_nanos(4));
    let mut limiter = Limiter::new(vec![Clause::direct(quota)], now);
    let charges = [Charge::Direct(cost(2))];
    assert_eq!(
        limiter.admit_at(&charges, now).assured("full burst"),
        Admission::Admitted
    );
    assert_eq!(
        limiter
            .admit_at(&charges, now + Duration::from_nanos(7))
            .assured("one nanosecond short"),
        Admission::WaitUntil(now + Duration::from_nanos(8))
    );
    assert_eq!(
        limiter
            .admit_at(&charges, now + Duration::from_nanos(8))
            .assured("exact refill"),
        Admission::Admitted
    );
    assert_eq!(
        Quota::cost(u64::from(u32::MAX))
            .assured("maximum cost")
            .get(),
        u32::MAX
    );
    assert!(Quota::new(1, Duration::from_nanos(u64::MAX), 1).is_err());
    assert!(KeyedOptions::new(Duration::ZERO, 1, NonZeroUsize::MIN).is_err());
}

#[test]
fn malformed_charges_and_regressing_clocks_change_no_allowance() {
    let now = Instant::now();
    let mut limiter = Limiter::new(vec![Clause::direct(quota(1, 10, 2))], now);
    let before = limiter.generation(now).assured("same clock");
    assert!(limiter.admit_at(&[], now).is_err());
    assert!(
        limiter
            .admit_at(
                &[Charge::Keyed {
                    key: KeyFingerprint::from([0; 16]),
                    cost: cost(1)
                }],
                now
            )
            .is_err()
    );
    assert!(
        limiter
            .admit_at(&[Charge::Direct(cost(1))], now - Duration::from_nanos(1))
            .is_err()
    );
    assert_eq!(limiter.generation(now).assured("same clock"), before);
}

#[test]
fn restored_key_ages_preserve_ttl_and_lru_with_the_same_allowance_debt() {
    let now = Instant::now();
    let options = KeyedOptions::new(
        Duration::from_nanos(100),
        2,
        NonZeroUsize::new(2).assured("two shards"),
    )
    .assured("bounded keys");
    let mut original = Limiter::new(vec![Clause::keyed(quota(1, 50, 2), options)], now);
    for (identity, elapsed) in [(0, 0), (1, 10), (0, 20)] {
        original
            .admit_at(
                &[Charge::Keyed {
                    key: KeyFingerprint::from([identity; 16]),
                    cost: cost(1),
                }],
                now + Duration::from_nanos(elapsed),
            )
            .assured("valid charge");
    }
    let observed = now + Duration::from_nanos(25);
    let state = original.generation(observed).assured("advanced clock");
    let mut restored =
        Limiter::from_generation(state.clone(), observed).assured("valid generation");
    assert_eq!(restored.generation(observed).assured("same clock"), state);
    let next = [Charge::Keyed {
        key: KeyFingerprint::from([2; 16]),
        cost: cost(1),
    }];
    assert_eq!(
        restored.admit_at(&next, observed).assured("new key"),
        original.admit_at(&next, observed).assured("new key")
    );
    assert_eq!(
        restored.generation(observed).assured("same clock"),
        original.generation(observed).assured("same clock")
    );
    assert_eq!(
        restored
            .key_statistics(now + Duration::from_nanos(120))
            .assured("TTL boundary")
            .active,
        1
    );
}

#[test]
fn invalid_generations_and_utc_overflow_are_typed_refusals() {
    use chrono::{DateTime, Utc};

    use super::{Generation, KeyGeneration};
    let quota = quota(1, 50, 2);
    let direct = Generation {
        clauses: vec![ClauseGeneration::Direct {
            quota,
            remaining: Some(Duration::from_nanos(10)),
        }],
    };
    assert_eq!(
        *direct
            .to_utc(DateTime::<Utc>::MAX_UTC)
            .err()
            .assured("UTC addition overflows")
            .current_context(),
        super::AdmissionError::ClockRange
    );
    let options =
        KeyedOptions::new(Duration::from_nanos(100), 2, NonZeroUsize::MIN).assured("bounded keys");
    let key = KeyGeneration {
        key: KeyFingerprint::from([1; 16]),
        remaining: Duration::from_nanos(10),
        age: Duration::ZERO,
        ordinal: 0,
    };
    let invalid = Generation {
        clauses: vec![ClauseGeneration::Keyed {
            quota,
            options,
            keys: vec![key.clone(), key],
        }],
    };
    assert_eq!(
        *Limiter::from_generation(invalid, Instant::now())
            .err()
            .assured("duplicate keys")
            .current_context(),
        super::AdmissionError::GenerationState
    );
}

#[test]
fn generation_refuses_a_key_whose_age_and_debt_exceed_its_burst_horizon() {
    use super::{AdmissionError, Generation, KeyGeneration};
    let now = Instant::now();
    let quota = quota(1, 50, 2);
    let options = KeyedOptions::new(Duration::from_nanos(u64::MAX), 1, NonZeroUsize::MIN)
        .assured("bounded TTL");
    for age in [
        Duration::from_nanos(91),
        Duration::from_nanos(u64::MAX - 1),
        Duration::MAX,
    ] {
        let generation = Generation {
            clauses: vec![ClauseGeneration::Keyed {
                quota,
                options,
                keys: vec![KeyGeneration {
                    key: KeyFingerprint::from([1; 16]),
                    remaining: Duration::from_nanos(10),
                    age,
                    ordinal: 0,
                }],
            }],
        };
        assert_eq!(
            *Limiter::from_generation(generation, now)
                .err()
                .assured("a charged horizon is at most a full burst after its last use")
                .current_context(),
            AdmissionError::GenerationState
        );
    }
}

#[test]
fn bolero_quota_bounds_match_wide_integer_arithmetic() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(32)
        .for_each(|bytes| {
            let mut input = [0; 24];
            let count = bytes.len().min(input.len());
            input[..count].copy_from_slice(&bytes[..count]);
            let count = u64::from_le_bytes(input[..8].try_into().assured("eight bytes"));
            let period = u64::from_le_bytes(input[8..16].try_into().assured("eight bytes"));
            let burst = u64::from_le_bytes(input[16..24].try_into().assured("eight bytes"));
            let bounded_count = count % 1_000_001;
            let bounded_period = period % 1_000_001;
            let bounded_burst = burst % (u64::from(u32::MAX) + 2);
            for (count, period, burst) in [
                (count, period, burst),
                (bounded_count, bounded_period, bounded_burst),
            ] {
                let interval = if count == 0 {
                    0
                } else {
                    u128::from(period).div_ceil(u128::from(count))
                };
                let expected = count > 0
                    && period > 0
                    && count <= period
                    && burst > 0
                    && burst <= u64::from(u32::MAX)
                    && interval * u128::from(burst) <= u128::from(u64::MAX / 2);
                let actual = Quota::new(count, Duration::from_nanos(period), burst);
                assert_eq!(
                    actual.is_ok(),
                    expected,
                    "count={count} period={period} burst={burst}"
                );
                if let Ok(quota) = actual {
                    assert_eq!(quota.replenish_interval().as_nanos(), interval);
                    assert_eq!(u64::from(quota.burst().get()), burst);
                }
            }
        });
}

#[test]
fn bolero_utc_generations_preserve_all_fields_at_the_same_anchor() {
    use chrono::{TimeZone as _, Utc};

    use super::{Generation, KeyGeneration};
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes| {
            let mut input = [0; 64];
            let length = bytes.len().min(input.len());
            input[..length].copy_from_slice(&bytes[..length]);
            let nanos =
                u32::from_le_bytes(input[..4].try_into().assured("four bytes")) % 1_000_000_000;
            let seconds = i64::from(u32::from_le_bytes(
                input[4..8].try_into().assured("four bytes"),
            ));
            let utc = Utc
                .timestamp_opt(seconds, nanos)
                .single()
                .assured("bounded UTC");
            let quota = quota(1, 1_000, 4);
            let options = KeyedOptions::new(
                Duration::from_nanos(10_000),
                3,
                NonZeroUsize::new(2).assured("two shards"),
            )
            .assured("bounded keys");
            let mut keys = Vec::new();
            for index in 0..3usize {
                let mut fingerprint: [u8; 16] = input[8 + index * 16..24 + index * 16]
                    .try_into()
                    .assured("sixteen bytes");
                fingerprint[0] = u8::try_from(index).assured("three keys");
                let remaining = 1 + u64::from(input[56 + index]) * 10;
                let age = u64::try_from(3 - index).assured("three keys") * 100
                    + u64::from(input[59 + index] % 100);
                keys.push(KeyGeneration {
                    key: KeyFingerprint::from(fingerprint),
                    remaining: Duration::from_nanos(remaining),
                    age: Duration::from_nanos(age),
                    ordinal: u64::try_from(index).assured("three keys") + u64::from(input[62]),
                });
            }
            let remaining = if input[63] % 2 == 0 {
                None
            } else {
                Some(Duration::from_nanos(1 + u64::from(input[63])))
            };
            let generation = Generation {
                clauses: vec![
                    ClauseGeneration::Direct { quota, remaining },
                    ClauseGeneration::Keyed {
                        quota,
                        options,
                        keys,
                    },
                ],
            };
            assert_eq!(
                generation
                    .to_utc(utc)
                    .assured("bounded UTC")
                    .from_utc(utc)
                    .assured("same anchor"),
                generation
            );
        });
}
