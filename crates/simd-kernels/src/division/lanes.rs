//! Constant-divisor lane execution.
//!
//! Layer: primitives.
//! - **Owns.** Portable reciprocal lane multiplication and checked division failure words.
//! - **Depends on.** Prepared divisors and portable SIMD registers.
//! - **Must not know.** Arrow, validity or a caller's rounding units.

use fearless_simd::{Level, dispatch, prelude::*};
use meticulous::{OptionExt as _, ResultExt as _};

use super::{Reciprocal, SignedDivisor, UnsignedDivisor};
use crate::{CheckedLanes, LEVEL, WORD_LANES, lane_mask};

#[derive(Debug, Clone, Copy)]
pub struct ConstantDivision {
    level: Level,
}

impl ConstantDivision {
    pub fn new() -> Self {
        Self {
            level: *LEVEL.get_or_init(Level::new),
        }
    }

    #[cfg(test)]
    pub(super) fn with_level(level: Level) -> Self {
        Self { level }
    }

    /// Truncating quotients. Zero divisors and signed MIN / -1 fail their lanes.
    pub fn quotients<N: DivisionLane>(self, values: &[N], divisor: N) -> CheckedLanes<N> {
        self.compute(values, divisor, Operation::Quotient)
    }

    /// Truncating remainders. Only zero divisors fail; signed MIN % -1 is zero.
    pub fn remainders<N: DivisionLane>(self, values: &[N], divisor: N) -> CheckedLanes<N> {
        self.compute(values, divisor, Operation::Remainder)
    }

    fn compute<N: DivisionLane>(
        self,
        values: &[N],
        divisor: N,
        operation: Operation,
    ) -> CheckedLanes<N> {
        N::compute(self, values, divisor, operation)
    }
}

impl Default for ConstantDivision {
    fn default() -> Self {
        Self::new()
    }
}

/// Integer widths whose column-by-constant division has one prepared reciprocal.
pub trait DivisionLane: registers::Divisible {}

impl<N: registers::Divisible> DivisionLane for N {}

#[derive(Clone, Copy)]
pub enum Operation {
    Quotient,
    Remainder,
}

pub struct RegisterLanes<V> {
    values: V,
    failed: u64,
}

mod registers {
    use super::*;

    pub trait Divisible: SimdIntElement {
        fn compute(
            kernel: ConstantDivision,
            values: &[Self],
            divisor: Self,
            operation: Operation,
        ) -> CheckedLanes<Self>;
    }

    pub trait RegisterDivision: SimdIntElement {
        fn prepare(divisor: Self) -> Option<UnsignedDivisor>;

        fn register<S: Simd>(
            simd: S,
            value: Self::Native<S>,
            divisor: Self,
            prepared: UnsignedDivisor,
            operation: Operation,
        ) -> RegisterLanes<Self::Native<S>>;
    }

    pub trait Unsigned: SimdIntElement {
        fn high_product<S: Simd>(
            simd: S,
            value: Self::Native<S>,
            multiplier: Self,
        ) -> Self::Native<S>;
    }
}

fn zero_divisor<N: Copy + Default>(length: usize) -> CheckedLanes<N> {
    let mut failed = vec![u64::MAX; length.div_ceil(WORD_LANES)];
    if let Some(tail) = failed.last_mut() {
        let remainder = length % WORD_LANES;
        if remainder != 0 {
            *tail = lane_mask(remainder);
        }
    }
    CheckedLanes {
        values: vec![N::default(); length],
        failed,
    }
}

fn compute_registers<N: registers::RegisterDivision>(
    kernel: ConstantDivision,
    values: &[N],
    divisor: N,
    operation: Operation,
) -> CheckedLanes<N> {
    match N::prepare(divisor) {
        Some(prepared) => dispatch!(kernel.level, simd => transform(simd, values, |value| {
            N::register(simd, value, divisor, prepared, operation)
        })),
        None => zero_divisor(values.len()),
    }
}

macro_rules! vector_division {
    ($($native:ty),+ $(,)?) => {$(
        impl registers::Divisible for $native {
            fn compute(kernel: ConstantDivision, values: &[Self], divisor: Self, operation: Operation) -> CheckedLanes<Self> {
                compute_registers(kernel, values, divisor, operation)
            }
        }
    )+};
}

vector_division!(i8, u8, i16, u16, i32, u32);

impl registers::Divisible for i64 {
    fn compute(
        kernel: ConstantDivision,
        values: &[Self],
        divisor: Self,
        operation: Operation,
    ) -> CheckedLanes<Self> {
        match operation {
            Operation::Quotient => compute_registers(kernel, values, divisor, Operation::Quotient),
            Operation::Remainder => {
                let Some(prepared) = SignedDivisor::new(divisor) else {
                    return zero_divisor(values.len());
                };
                // The scalar reciprocal remainder beats the vector form on x86-64-v3.
                // Only a zero divisor fails; checked MIN % -1 is the successful zero.
                let mut output = vec![0; values.len()];
                for (output, value) in output.iter_mut().zip(values) {
                    *output = prepared.checked_rem(*value).unwrap_or(0);
                }
                CheckedLanes {
                    values: output,
                    failed: vec![0; values.len().div_ceil(WORD_LANES)],
                }
            }
        }
    }
}

impl registers::Divisible for u64 {
    fn compute(
        _kernel: ConstantDivision,
        values: &[Self],
        divisor: Self,
        operation: Operation,
    ) -> CheckedLanes<Self> {
        let Some(prepared) = UnsignedDivisor::new(divisor) else {
            return zero_divisor(values.len());
        };
        // The scalar multiply-high loop measured faster than the u64 SIMD loop.
        // A nonzero unsigned divisor cannot fail, so it needs no failure packing.
        let mut output = vec![0; values.len()];
        match operation {
            Operation::Quotient => {
                for (output, value) in output.iter_mut().zip(values) {
                    *output = prepared.quotient(*value);
                }
            }
            Operation::Remainder => {
                for (output, value) in output.iter_mut().zip(values) {
                    *output = prepared.remainder(*value);
                }
            }
        }
        CheckedLanes {
            values: output,
            failed: vec![0; values.len().div_ceil(WORD_LANES)],
        }
    }
}

/// Widened products retain exactly the upper operand-width bits before narrowing.
macro_rules! narrow_products {
    ($($native:ty => $wide:ty),+ $(,)?) => {$(
        impl registers::Unsigned for $native {
            #[inline(always)]
            fn high_product<S: Simd>(simd: S, value: Self::Native<S>, multiplier: Self) -> Self::Native<S> {
                let multiplier = <$wide as SimdIntElement>::Native::<S>::splat(simd, <$wide>::from(multiplier));
                let (low, high) = value.widen();
                let low = (low * multiplier) >> <$native>::BITS;
                let high = (high * multiplier) >> <$native>::BITS;
                low.narrow(high)
            }
        }
    )+};
}

narrow_products!(u8 => u16, u16 => u32, u32 => u64);

impl registers::Unsigned for u64 {
    #[inline(always)]
    fn high_product<S: Simd>(simd: S, left: S::u64s, multiplier: u64) -> S::u64s {
        // Four exact 32 × 32 partial products. The middle sum is below 3*2^32,
        // and the high sum is the upper half of a u64 × u64 product, so none overflows.
        let mask = S::u64s::splat(simd, u64::from(u32::MAX));
        let left_low = left & mask;
        let left_high = left >> 32;
        let right_low = S::u64s::splat(simd, multiplier & u64::from(u32::MAX));
        let right_high = S::u64s::splat(simd, multiplier >> 32);
        let low = left_low * right_low;
        let cross_left = left_high * right_low;
        let cross_right = left_low * right_high;
        let high = left_high * right_high;
        let middle = (low >> 32) + (cross_left & mask) + (cross_right & mask);
        high + (cross_left >> 32) + (cross_right >> 32) + (middle >> 32)
    }
}

/// Reciprocal division in unsigned lanes. Its approximate quotient is at most one too small,
/// so one compare and correction produce the exact quotient and remainder.
#[inline(always)]
fn unsigned_register<S: Simd, N: registers::Unsigned>(
    simd: S,
    value: N::Native<S>,
    prepared: UnsignedDivisor,
) -> (N::Native<S>, N::Native<S>) {
    let divisor = N::try_from(prepared.divisor.get())
        .assured("the prepared magnitude fits its unsigned lane");
    let divisor = N::Native::<S>::splat(simd, divisor);
    match prepared.reciprocal {
        Reciprocal::Shift(shift) => {
            let one = N::Native::<S>::splat(
                simd,
                N::try_from(1_u8).assured("every unsigned lane holds one"),
            );
            (value >> shift, value & (divisor - one))
        }
        Reciprocal::Multiply(multiplier) => {
            // floor(floor(2^64 / d) / 2^(64-w)) = floor(2^w / d), so every
            // lane width uses the same prepared reciprocal with its low bits discarded.
            let discarded_bits = u64::BITS
                - u32::try_from(N::BITS)
                    .assured("the supported integer lane widths are at most 64");
            let multiplier = N::try_from(multiplier >> discarded_bits)
                .assured("the reciprocal of a divisor above one fits its lane width");
            let quotient = N::high_product(simd, value, multiplier);
            let remainder = value - quotient * divisor;
            let correction = remainder.simd_ge(divisor);
            let one = N::Native::<S>::splat(
                simd,
                N::try_from(1_u8).assured("every unsigned lane holds one"),
            );
            let zero = N::Native::<S>::splat(simd, N::default());
            (
                quotient + correction.select(one, zero),
                remainder - correction.select(divisor, zero),
            )
        }
    }
}

macro_rules! unsigned_lanes {
    ($($native:ty),+ $(,)?) => {$(
        impl registers::RegisterDivision for $native {
            fn prepare(divisor: Self) -> Option<UnsignedDivisor> {
                UnsignedDivisor::new(u64::from(divisor))
            }

            #[inline(always)]
            fn register<S: Simd>(simd: S, value: Self::Native<S>, _divisor: Self, prepared: UnsignedDivisor, operation: Operation) -> RegisterLanes<Self::Native<S>> {
                let (quotient, remainder) = unsigned_register::<S, Self>(simd, value, prepared);
                let values = match operation {
                    Operation::Quotient => quotient,
                    Operation::Remainder => remainder,
                };
                RegisterLanes { values, failed: 0 }
            }
        }
    )+};
}

unsigned_lanes!(u8, u16, u32);

macro_rules! signed_lanes {
    ($($native:ty => $unsigned:ty),+ $(,)?) => {$(
        impl registers::RegisterDivision for $native {
            fn prepare(divisor: Self) -> Option<UnsignedDivisor> {
                UnsignedDivisor::new(u64::from(divisor.unsigned_abs()))
            }

            #[inline(always)]
            fn register<S: Simd>(simd: S, value: Self::Native<S>, divisor: Self, prepared: UnsignedDivisor, operation: Operation) -> RegisterLanes<Self::Native<S>> {
                let zero = Self::Native::<S>::splat(simd, 0);
                let negative = value.simd_lt(zero);
                // SIMD subtraction is two's complement arithmetic: MIN's magnitude is its
                // unsigned high bit, so the negative magnitude is representable without widening.
                let magnitude: <$unsigned as SimdIntElement>::Native<S> = negative.select(zero - value, value).bitcast();
                let (quotient, remainder) = unsigned_register::<S, $unsigned>(simd, magnitude, prepared);
                match operation {
                    Operation::Quotient => {
                        let quotient: Self::Native<S> = quotient.bitcast();
                        let signed = negative.select(zero - quotient, quotient);
                        let values = if divisor < 0 { zero - signed } else { signed };
                        if divisor == -1 {
                            let overflow = value.simd_eq(Self::Native::<S>::splat(simd, Self::MIN));
                            RegisterLanes { values: overflow.select(zero, values), failed: overflow.to_bitmask() }
                        } else {
                            RegisterLanes { values, failed: 0 }
                        }
                    }
                    Operation::Remainder => {
                        let remainder: Self::Native<S> = remainder.bitcast();
                        RegisterLanes { values: negative.select(zero - remainder, remainder), failed: 0 }
                    }
                }
            }
        }
    )+};
}

signed_lanes!(i8 => u8, i16 => u16, i32 => u32, i64 => u64);

#[inline(always)]
fn transform<S: Simd, N: SimdIntElement>(
    simd: S,
    input: &[N],
    operation: impl Fn(N::Native<S>) -> RegisterLanes<N::Native<S>>,
) -> CheckedLanes<N> {
    let mut values = vec![N::default(); input.len()];
    let mut failed = vec![0_u64; input.len().div_ceil(WORD_LANES)];
    let (words, tail) = input.as_chunks::<WORD_LANES>();
    let (outputs, output_tail) = values.as_chunks_mut::<WORD_LANES>();
    for ((input, output), failed) in words.iter().zip(outputs).zip(failed.iter_mut()) {
        *failed = transform_word(simd, input, output, &operation);
    }
    if !tail.is_empty() {
        let mut padded = [N::default(); WORD_LANES];
        padded[..tail.len()].copy_from_slice(tail);
        let mut output = [N::default(); WORD_LANES];
        let tail_failed = transform_word(simd, &padded, &mut output, &operation);
        output_tail.copy_from_slice(&output[..tail.len()]);
        *failed
            .last_mut()
            .assured("a nonempty tail has a failure word") = tail_failed & lane_mask(tail.len());
    }
    CheckedLanes { values, failed }
}

#[inline(always)]
fn transform_word<S: Simd, N: SimdIntElement>(
    simd: S,
    input: &[N; WORD_LANES],
    output: &mut [N; WORD_LANES],
    operation: &impl Fn(N::Native<S>) -> RegisterLanes<N::Native<S>>,
) -> u64 {
    let width = N::Native::<S>::LEN;
    let mut failed = 0;
    for (index, (input, output)) in input
        .chunks_exact(width)
        .zip(output.chunks_exact_mut(width))
        .enumerate()
    {
        let lanes = operation(N::Native::<S>::from_slice(simd, input));
        lanes.values.store_slice(output);
        failed |= lanes.failed << (index * width);
    }
    failed
}
