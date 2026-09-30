//! The bytes a property receives, read as a sequence of bounded choices.

use std::{num::NonZeroUsize, ops::RangeInclusive};

use meticulous::{OptionExt as _, ResultExt as _};

/// The bytes a property received, read front to back as bounded choices.
///
/// Every read consumes the bytes it needs from the front. Once they run out, every choice takes its
/// first and smallest option, so a shorter input always describes a smaller value: truncating a
/// failing input, which is how libFuzzer minimizes one, simplifies the value it describes rather
/// than reshuffling it.
#[derive(Debug, Clone)]
pub struct Entropy<'bytes> {
    remaining: &'bytes [u8],
}

impl<'bytes> Entropy<'bytes> {
    pub fn new(bytes: &'bytes [u8]) -> Self {
        Self { remaining: bytes }
    }

    /// The next byte, or zero once every byte has been read.
    pub fn byte(&mut self) -> u8 {
        let Some((first, rest)) = self.remaining.split_first() else {
            return 0;
        };
        self.remaining = rest;
        *first
    }

    /// A yes-or-no choice, no once every byte has been read.
    pub fn flag(&mut self) -> bool {
        self.byte() & 1 == 1
    }

    /// A value from `0` through `max`, reading only as many bytes as `max` spans.
    pub fn up_to(&mut self, max: u64) -> u64 {
        let mut value = 0_u64;
        let mut span = max;
        while span > 0 {
            value = (value << 8) | u64::from(self.byte());
            span >>= 8;
        }
        match max.checked_add(1) {
            Some(modulus) => value % modulus,
            None => value,
        }
    }

    /// A value from the inclusive `range`, which callers write low end first.
    pub fn between(&mut self, range: RangeInclusive<u64>) -> u64 {
        let (low, high) = range.into_inner();
        let span = high
            .checked_sub(low)
            .assured("every range a generator reads is written low end first");
        let offset = self.up_to(span);
        low.checked_add(offset)
            .verified("the offset is at most the span between the two ends")
    }

    /// A value from the inclusive `range` that lands on an end, or next to one, as often as it
    /// lands anywhere else, because the ends are where a representation runs out of room.
    pub fn boundary_biased(&mut self, range: RangeInclusive<u64>) -> u64 {
        let (low, high) = range.into_inner();
        match self.byte() % 6 {
            0 => low,
            1 => high,
            2 => {
                let above = low.checked_add(1);
                match above {
                    Some(value) if value <= high => value,
                    Some(_) | None => low,
                }
            }
            3 => {
                let below = high.checked_sub(1);
                match below {
                    Some(value) if value >= low => value,
                    Some(_) | None => high,
                }
            }
            _ => self.between(low..=high),
        }
    }

    /// A count from `0` through `max`.
    pub fn count(&mut self, max: usize) -> usize {
        let max = u64::try_from(max).assured("supported targets address at most 64 bits");
        let chosen = self.up_to(max);
        usize::try_from(chosen).verified("the count is at most the usize it was widened from")
    }

    /// A count from `1` through `max`.
    pub fn positive_count(&mut self, max: NonZeroUsize) -> usize {
        let below = max
            .get()
            .checked_sub(1)
            .verified("a non-zero count is at least one");
        self.count(below)
            .checked_add(1)
            .verified("the count is below a usize it can be incremented back to")
    }

    /// An index into `options` many choices.
    pub fn index(&mut self, options: NonZeroUsize) -> usize {
        let last = options
            .get()
            .checked_sub(1)
            .verified("a non-zero number of options has a last index");
        self.count(last)
    }

    /// One of `options`, which is never empty.
    pub fn pick<T: Clone, const N: usize>(&mut self, options: [T; N]) -> T {
        const { assert!(N > 0, "a choice needs at least one option") };
        let count = NonZeroUsize::new(N).verified("the constant assertion above rules out zero");
        let chosen = self.index(count);
        options[chosen].clone()
    }

    /// Any `u64`, landing on the representable extremes as often as elsewhere.
    pub fn any_u64(&mut self) -> u64 {
        self.boundary_biased(0..=u64::MAX)
    }

    /// Any `i64`, landing on zero, the extremes and their neighbours as often as elsewhere.
    pub fn any_i64(&mut self) -> i64 {
        match self.byte() % 8 {
            0 => 0,
            1 => i64::MIN,
            2 => i64::MAX,
            3 => -1,
            4 => 1,
            5 => i64::MIN
                .checked_add(1)
                .assured("one above the minimum is representable"),
            _ => {
                let bits = self.up_to(u64::MAX);
                bits.cast_signed()
            }
        }
    }
}
