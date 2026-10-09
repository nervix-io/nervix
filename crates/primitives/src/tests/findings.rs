//! A deadlock finding keeps its bounds in every build and describes itself without the values its
//! locks protect.

use std::{num::NonZeroU64, panic::Location, time::UNIX_EPOCH};

use meticulous::{OptionExt as _, ResultExt as _};

use crate::deadlock::{
    Access, ActiveCycle, BlockedThread, BoundedText, CycleOutOfBounds, LockKind, MAX_CYCLE_THREADS,
    MAX_TEXT_BYTES, SourceSite, TextOutOfBounds, TrackedLockId, TrackedThreadId,
};

fn number(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).assured("the checks number from one")
}

fn thread(value: u64) -> BlockedThread {
    BlockedThread {
        thread: TrackedThreadId::new(number(value)),
        name: None,
        waits_for: None,
        attempt: None,
    }
}

#[test]
fn a_bounded_text_keeps_whole_characters_up_to_the_bound() {
    let short = BoundedText::new("worker");
    assert_eq!(short.as_str(), "worker");
    assert!(!short.is_truncated());
    assert_eq!(short.to_string(), "worker");

    // Every character is three bytes, so the bound falls inside one.
    let long = "✓".repeat(MAX_TEXT_BYTES);
    let kept = BoundedText::new(&long);
    assert!(kept.is_truncated());
    assert!(kept.as_str().len() <= MAX_TEXT_BYTES);
    assert!(kept.as_str().chars().all(|character| character == '✓'));
    assert_eq!(
        BoundedText::from_parts(kept.as_str().to_string(), kept.original_bytes()),
        Ok(kept.clone())
    );
    assert!(
        kept.to_string()
            .ends_with(&format!("... (cut from {} bytes)", long.len()))
    );
}

#[test]
fn a_kept_text_new_could_not_have_produced_is_refused() {
    let past = "a".repeat(MAX_TEXT_BYTES + 1);
    assert_eq!(
        BoundedText::from_parts(past, 600),
        Err(TextOutOfBounds::Kept { bytes: 513 })
    );
    assert_eq!(
        BoundedText::from_parts("abc".to_string(), 2),
        Err(TextOutOfBounds::LongerThanOriginal {
            kept: 3,
            original: 2
        })
    );
    assert_eq!(
        BoundedText::from_parts("abc".to_string(), 4),
        Err(TextOutOfBounds::CutShort {
            kept: 3,
            original: 4
        })
    );
    assert_eq!(
        TextOutOfBounds::Kept { bytes: 513 }.to_string(),
        "a kept text of 513 bytes is longer than 512"
    );
    assert_eq!(
        TextOutOfBounds::LongerThanOriginal {
            kept: 3,
            original: 2
        }
        .to_string(),
        "a kept text of 3 bytes is longer than the 2 bytes it was cut from"
    );
    assert_eq!(
        TextOutOfBounds::CutShort {
            kept: 3,
            original: 4
        }
        .to_string(),
        "a text of 4 bytes was cut to 3, further than the bound requires"
    );
}

#[test]
fn a_cycle_has_threads_within_its_bound_and_omits_only_past_it() {
    assert_eq!(
        ActiveCycle::new(UNIX_EPOCH, Vec::new(), 0),
        Err(CycleOutOfBounds::Empty)
    );
    let past = MAX_CYCLE_THREADS + 1;
    let too_many = (1..=past)
        .map(|index| thread(index.try_into().assured("small")))
        .collect();
    assert_eq!(
        ActiveCycle::new(UNIX_EPOCH, too_many, 0),
        Err(CycleOutOfBounds::TooManyThreads { threads: past })
    );
    assert_eq!(
        ActiveCycle::new(UNIX_EPOCH, vec![thread(1)], 2),
        Err(CycleOutOfBounds::OmittedBelowBound {
            threads: 1,
            omitted: 2
        })
    );
    let cycle = ActiveCycle::new(UNIX_EPOCH, vec![thread(1), thread(2)], 0)
        .assured("two threads are within the bound");
    assert_eq!(cycle.detected_at(), UNIX_EPOCH);
    assert_eq!(cycle.threads().len(), 2);
    assert_eq!(cycle.omitted_threads(), 0);
    assert_eq!(
        CycleOutOfBounds::Empty.to_string(),
        "a cycle has no threads"
    );
    assert_eq!(
        CycleOutOfBounds::TooManyThreads { threads: 65 }.to_string(),
        "a cycle describes 65 threads, more than 64"
    );
    assert_eq!(
        CycleOutOfBounds::OmittedBelowBound {
            threads: 1,
            omitted: 2
        }
        .to_string(),
        "a cycle omits 2 threads while describing only 1"
    );
}

#[test]
fn identities_sites_kinds_and_access_describe_themselves() {
    let caller = Location::caller();
    let site = SourceSite::from_location(caller);
    assert_eq!(site.file.as_str(), caller.file());
    assert_eq!(site.line, caller.line());
    assert_eq!(
        site.to_string(),
        format!("{}:{}:{}", caller.file(), caller.line(), caller.column())
    );
    assert_eq!(TrackedThreadId::new(number(3)).to_string(), "thread 3");
    assert_eq!(TrackedThreadId::new(number(3)).get(), number(3));
    assert_eq!(TrackedLockId::new(number(5)).to_string(), "lock 5");
    assert_eq!(TrackedLockId::new(number(5)).get(), number(5));
    assert_eq!(LockKind::Mutex.to_string(), "mutex");
    assert_eq!(LockKind::RwLock.to_string(), "read-write lock");
    assert_eq!(LockKind::CondvarState.to_string(), "condition variable");
    assert_eq!(Access::Exclusive.to_string(), "exclusive");
    assert_eq!(Access::Shared.to_string(), "shared");
}

#[test]
fn the_lane_stress_configuration_preempts_one_in_twenty_between_its_delays() {
    use std::time::Duration;

    use crate::deadlock::{DiagnosticSelection, PREEMPTION_SCALE, StressConfiguration};

    let lane = StressConfiguration::LANE;
    assert_eq!(lane.preemptions_per_million().get(), PREEMPTION_SCALE / 20);
    assert_eq!(lane.shortest_delay(), Duration::from_micros(20));
    assert_eq!(lane.longest_delay(), Duration::from_micros(200));
    assert!(!lane.yield_after_release());
    let stressed = DiagnosticSelection::StressedActiveOnly(lane);
    assert_eq!(stressed.stress(), Some(lane));
    assert!(!stressed.checks_order());
    assert_eq!(DiagnosticSelection::ActiveOnly.stress(), None);
    assert_eq!(
        stressed.is_available(),
        cfg!(feature = "deloxide-stress"),
        "only a stress build installs a stressed selection"
    );
    assert_eq!(
        DiagnosticSelection::ActiveOnly.is_available(),
        !cfg!(any(feature = "deloxide-order", feature = "deloxide-stress"))
    );
    if cfg!(feature = "deloxide-stress") {
        assert_eq!(DiagnosticSelection::for_build(false), stressed);
        assert_eq!(DiagnosticSelection::for_build(true), stressed);
    }
}

#[test]
fn a_stress_configuration_outside_its_bounds_is_refused() {
    use std::{num::NonZeroU32, time::Duration};

    use crate::deadlock::{
        MAX_STRESS_DELAY, PREEMPTION_SCALE, StressConfiguration, StressOutOfBounds,
    };

    let per_million = |value: u32| NonZeroU32::new(value).assured("the checks use nonzero values");
    let micro = Duration::from_micros(1);
    let whole = StressConfiguration::new(
        per_million(PREEMPTION_SCALE),
        micro,
        MAX_STRESS_DELAY,
        false,
    )
    .assured("a probability of one and the delay bounds themselves are within the bounds");
    assert_eq!(whole.preemptions_per_million().get(), PREEMPTION_SCALE);

    let above_one = PREEMPTION_SCALE
        .checked_add(1)
        .assured("the scale is a million");
    assert_eq!(
        StressConfiguration::new(per_million(above_one), micro, micro, false),
        Err(StressOutOfBounds::Probability {
            per_million: above_one
        })
    );
    let too_long = MAX_STRESS_DELAY
        .checked_add(micro)
        .assured("two milliseconds and one");
    assert_eq!(
        StressConfiguration::new(per_million(1), micro, too_long, false),
        Err(StressOutOfBounds::Delay { delay: too_long })
    );
    assert_eq!(
        StressConfiguration::new(per_million(1), Duration::ZERO, micro, false),
        Err(StressOutOfBounds::Delay {
            delay: Duration::ZERO
        })
    );
    let fractional = Duration::from_nanos(1_500);
    assert_eq!(
        StressConfiguration::new(per_million(1), fractional, MAX_STRESS_DELAY, false),
        Err(StressOutOfBounds::Delay { delay: fractional })
    );
    assert_eq!(
        StressConfiguration::new(per_million(1), MAX_STRESS_DELAY, micro, true),
        Err(StressOutOfBounds::Inverted {
            shortest: MAX_STRESS_DELAY,
            longest: micro
        })
    );
    assert!(
        StressOutOfBounds::Inverted {
            shortest: MAX_STRESS_DELAY,
            longest: micro
        }
        .to_string()
        .contains("longer than the longest")
    );
}
