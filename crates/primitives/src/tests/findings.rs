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
