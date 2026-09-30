//! Constant integer divisors prepared once for a run.
//!
//! Layer: primitives.
//! - **Owns.** Exact reciprocal division, truncating and Euclidean signs, and checked overflow.
//! - **Depends on.** Integer arithmetic and portable SIMD selection.
//! - **Must not know.** Arrow, datetime units, validity or a caller's error classification.

use std::num::NonZeroU64;

use meticulous::{OptionExt as _, ResultExt as _};

mod lanes;
pub use lanes::{ConstantDivision, DivisionLane};

/// An unsigned divisor whose reciprocal is computed once, before any lane is read.
#[derive(Debug, Clone, Copy)]
pub struct UnsignedDivisor {
    divisor: NonZeroU64,
    reciprocal: Reciprocal,
}

#[derive(Debug, Clone, Copy)]
enum Reciprocal {
    Shift(u32),
    Multiply(u64),
}

impl UnsignedDivisor {
    #[inline]
    pub fn new(divisor: u64) -> Option<Self> {
        let divisor = NonZeroU64::new(divisor)?;
        let value = divisor.get();
        let reciprocal = if value.is_power_of_two() {
            Reciprocal::Shift(value.trailing_zeros())
        } else {
            let multiplier = (1_u128 << u64::BITS) / u128::from(value);
            Reciprocal::Multiply(u64::try_from(multiplier).assured(
                "a non-power-of-two divisor is at least three, so its reciprocal fits u64",
            ))
        };
        Some(Self {
            divisor,
            reciprocal,
        })
    }

    /// Exact quotient and remainder, with no division instruction after preparation.
    #[inline(always)]
    pub fn quotient_remainder(self, value: u64) -> (u64, u64) {
        match self.reciprocal {
            Reciprocal::Shift(shift) => (value >> shift, value & (self.divisor.get() - 1)),
            Reciprocal::Multiply(multiplier) => {
                // A u64 × u64 product is exact in u128. With m = floor(2^64 / d),
                // floor(n*m / 2^64) is either floor(n/d) or one below it for n < 2^64.
                let product = u128::from(value) * u128::from(multiplier);
                let quotient = u64::try_from(product >> u64::BITS)
                    .assured("the approximate quotient is no greater than the u64 dividend");
                let multiple = quotient
                    .checked_mul(self.divisor.get())
                    .assured("the approximate quotient is at most the exact quotient, so q*d <= n");
                let remainder = value - multiple;
                let correction = u64::from(remainder >= self.divisor.get());
                let quotient = quotient
                    .checked_add(correction)
                    .assured("the corrected quotient is at most the dividend");
                (quotient, remainder - correction * self.divisor.get())
            }
        }
    }

    #[inline]
    pub fn quotient(self, value: u64) -> u64 {
        self.quotient_remainder(value).0
    }

    #[inline]
    pub fn remainder(self, value: u64) -> u64 {
        self.quotient_remainder(value).1
    }
}

/// A signed constant with the exact checked truncating and Euclidean integer contracts.
#[derive(Debug, Clone, Copy)]
pub struct SignedDivisor {
    magnitude: UnsignedDivisor,
    negative: bool,
}

impl SignedDivisor {
    #[inline]
    pub fn new(divisor: i64) -> Option<Self> {
        Some(Self {
            magnitude: UnsignedDivisor::new(divisor.unsigned_abs())?,
            negative: divisor < 0,
        })
    }

    #[inline]
    fn overflows(self, value: i64) -> bool {
        value == i64::MIN && self.negative && self.magnitude.divisor.get() == 1
    }

    /// Applies a sign to an unsigned magnitude, including the magnitude of `i64::MIN`.
    #[inline]
    fn signed(magnitude: u64, negative: bool) -> i64 {
        if negative {
            // Two's complement negation is the representation of the negative magnitude.
            magnitude.overflowing_neg().0.cast_signed()
        } else {
            magnitude.cast_signed()
        }
    }

    #[inline]
    pub fn checked_div(self, value: i64) -> Option<i64> {
        if self.overflows(value) {
            return None;
        }
        let quotient = self.magnitude.quotient(value.unsigned_abs());
        Some(Self::signed(quotient, (value < 0) != self.negative))
    }

    #[inline]
    pub fn checked_rem(self, value: i64) -> Option<i64> {
        if self.overflows(value) {
            return None;
        }
        let remainder = self.magnitude.remainder(value.unsigned_abs());
        Some(Self::signed(remainder, value < 0))
    }

    #[inline]
    pub fn checked_div_euclid(self, value: i64) -> Option<i64> {
        if self.overflows(value) {
            return None;
        }
        let (quotient, remainder) = self.magnitude.quotient_remainder(value.unsigned_abs());
        let correction = u64::from(value < 0 && remainder != 0);
        // Only MIN / -1 exceeds the signed magnitude bound, and was rejected above.
        let quotient = quotient
            .checked_add(correction)
            .assured("a Euclidean quotient magnitude is at most 2^63");
        Some(Self::signed(quotient, (value < 0) != self.negative))
    }

    #[inline]
    pub fn checked_rem_euclid(self, value: i64) -> Option<i64> {
        if self.overflows(value) {
            return None;
        }
        let remainder = self.magnitude.remainder(value.unsigned_abs());
        let remainder = if value < 0 && remainder != 0 {
            self.magnitude.divisor.get() - remainder
        } else {
            remainder
        };
        Some(
            i64::try_from(remainder)
                .assured("a Euclidean remainder is below the divisor magnitude, at most 2^63"),
        )
    }
}

#[cfg(test)]
#[path = "division_tests.rs"]
mod tests;
