//! Checked integer sums, differences and products computed in vector lanes.
//!
//! Layer: primitives.
//!
//! - **Owns.** Checked addition, subtraction and multiplication of integer lanes at the SIMD level
//!   the process selected: every lane's wrapped result, and a bitmask word per 64 lanes of the
//!   lanes whose exact result their type cannot hold.
//! - **Depends on.** Portable SIMD selection and the word layout of the flag packing.
//! - **Must not know.** Arrow, validity, or what a failed lane means to its caller.
//!
//! A sum or a difference wraps in its operands' own lanes, and the signs of the operands and the
//! wrapped result, or the carry out of unsigned lanes, decide exactly which lanes overflowed, so
//! it needs no wider lane at any width. A product has no such rule: an 8-, 16- or 32-bit register
//! widens into two registers of lanes twice as wide, where every product of two of its values is
//! exact. Comparing the exact products against the narrow type's bounds gives each lane's failure
//! bit, and narrowing them back by truncation gives each lane's value, the low bits of the exact
//! product. Every lane's value is the wrapped result that `overflowing_add`, `overflowing_sub` or
//! `overflowing_mul` returns. A 64-bit product has no wider lane to be exact in, so the kernels
//! leave it to their caller.
//!
//! Every level, the scalar fallback included, computes the same value and the same failure bit for
//! every lane.

use std::iter;

use fearless_simd::{Level, dispatch, prelude::*};

use self::registers::RegisterLanes;
use crate::{
    LEVEL,
    flags::{WORD_LANES, lane_mask},
};

/// Checked sums, differences and products of integer lanes at the SIMD level the process selected.
///
/// The level is resolved when the kernel is made, and one call computes a whole run of lanes, so a
/// caller pays the dispatch once for as many lanes as it hands over.
#[derive(Debug, Clone, Copy)]
pub struct CheckedArithmetic {
    level: Level,
}

impl CheckedArithmetic {
    pub fn new() -> Self {
        Self {
            level: *LEVEL.get_or_init(Level::new),
        }
    }

    /// Every lane's sum, failed where the exact sum does not fit the type.
    pub fn sums<N: CheckedLane>(self, operands: LaneOperands<'_, N>) -> CheckedLanes<N> {
        dispatch!(self.level, simd => compute(simd, operands, N::sum_lanes))
    }

    /// Every lane's difference, failed where the exact difference does not fit the type.
    pub fn differences<N: CheckedLane>(self, operands: LaneOperands<'_, N>) -> CheckedLanes<N> {
        dispatch!(self.level, simd => compute(simd, operands, N::difference_lanes))
    }

    /// Every lane's product, failed where the exact product does not fit the type.
    pub fn products<N: WidenedLane>(self, operands: LaneOperands<'_, N>) -> CheckedLanes<N> {
        dispatch!(self.level, simd => compute(simd, operands, N::product_lanes))
    }
}

impl Default for CheckedArithmetic {
    fn default() -> Self {
        Self::new()
    }
}

/// The operands of one binary lane operation.
#[derive(Debug, Clone, Copy)]
pub enum LaneOperands<'a, N> {
    /// A left and a right value for every lane. The two slices pair their values by index, as
    /// `zip` does, so there is a lane for every index both of them hold.
    Runs { left: &'a [N], right: &'a [N] },
    /// A left value every lane shares, and a right value for every lane.
    SharedLeft { left: N, right: &'a [N] },
    /// A left value for every lane, and a right value every lane shares.
    SharedRight { left: &'a [N], right: N },
}

/// The values a checked kernel computed for a run of lanes, and which of its lanes failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedLanes<N> {
    /// Every lane's result wrapped to the width of its type. A lane that did not fail holds its
    /// exact result.
    pub values: Vec<N>,
    /// One word for every [`WORD_LANES`] lanes and one for the lanes that remain, lane zero in the
    /// lowest bit, with a bit set for each lane whose exact result the type cannot hold. A word
    /// for fewer lanes has every bit past its last lane clear.
    pub failed: Vec<u64>,
}

/// An integer type whose checked sums and differences the kernels compute: every signed and
/// unsigned width from 8 to 64 bits.
pub trait CheckedLane: registers::Additive {}

impl<N: registers::Additive> CheckedLane for N {}

/// An integer type of 8, 16 or 32 bits, whose checked products the kernels compute in lanes of
/// twice its width, where every product of two of its values is exact.
pub trait WidenedLane: CheckedLane + registers::Multiplicative {}

impl<N: CheckedLane + registers::Multiplicative> WidenedLane for N {}

/// The register operations behind [`CheckedLane`] and [`WidenedLane`], which no caller outside
/// this crate can name or implement.
mod registers {
    use fearless_simd::{Simd, SimdIntElement};

    /// One register's results, and a bit for each of its lanes that failed, lane zero lowest.
    pub struct RegisterLanes<V> {
        pub values: V,
        pub failed: u64,
    }

    pub trait Additive: SimdIntElement {
        fn sum_lanes<S: Simd>(
            simd: S,
            left: Self::Native<S>,
            right: Self::Native<S>,
        ) -> RegisterLanes<Self::Native<S>>;

        fn difference_lanes<S: Simd>(
            simd: S,
            left: Self::Native<S>,
            right: Self::Native<S>,
        ) -> RegisterLanes<Self::Native<S>>;
    }

    pub trait Multiplicative: SimdIntElement {
        fn product_lanes<S: Simd>(
            simd: S,
            left: Self::Native<S>,
            right: Self::Native<S>,
        ) -> RegisterLanes<Self::Native<S>>;
    }
}

/// Computes one register of products in lanes twice as wide: `operation` over both halves of the
/// widened operands, a failure bit wherever `outside` finds the exact product out of the narrow
/// type's range, and the products narrowed back by truncation.
#[inline(always)]
fn widened<S: Simd, V: SimdWiden<S>>(
    left: V,
    right: V,
    operation: impl Fn(V::Widened, V::Widened) -> V::Widened,
    outside: impl Fn(V::Widened) -> u64,
) -> RegisterLanes<V> {
    let (left_low, left_high) = left.widen();
    let (right_low, right_high) = right.widen();
    let low = operation(left_low, right_low);
    let high = operation(left_high, right_high);
    let failed = outside(low) | (outside(high) << V::Widened::LEN);
    RegisterLanes {
        values: low.narrow(high),
        failed,
    }
}

/// The range of a signed narrow type, splat into the lanes of the type twice as wide.
struct SignedRange<W> {
    min: W,
    max: W,
}

impl<W> SignedRange<W> {
    /// A bit for each lane of `wide` below the minimum or above the maximum.
    #[inline(always)]
    fn outside<S: Simd>(&self, wide: W) -> u64
    where
        W: SimdBase<S>,
    {
        (wide.simd_lt(self.min) | wide.simd_gt(self.max)).to_bitmask()
    }
}

/// The largest value of an unsigned narrow type, splat into the lanes of the type twice as wide.
/// Every exact product of two unsigned values is at least zero, so a wide product fits the narrow
/// type exactly when it is at most this maximum.
struct UnsignedRange<W> {
    max: W,
}

impl<W> UnsignedRange<W> {
    /// A bit for each lane of `wide` above the maximum.
    #[inline(always)]
    fn outside<S: Simd>(&self, wide: W) -> u64
    where
        W: SimdBase<S>,
    {
        wide.simd_gt(self.max).to_bitmask()
    }
}

/// The products of signed narrow types, computed in lanes twice as wide.
macro_rules! widened_signed {
    ($($narrow:ty => $wide:ty),+ $(,)?) => {
        $(
            impl registers::Multiplicative for $narrow {
                #[inline(always)]
                fn product_lanes<S: Simd>(
                    simd: S,
                    left: Self::Native<S>,
                    right: Self::Native<S>,
                ) -> RegisterLanes<Self::Native<S>> {
                    let range = SignedRange {
                        min: <$wide as SimdIntElement>::Native::<S>::splat(
                            simd,
                            <$wide>::from(<$narrow>::MIN),
                        ),
                        max: <$wide as SimdIntElement>::Native::<S>::splat(
                            simd,
                            <$wide>::from(<$narrow>::MAX),
                        ),
                    };
                    widened(left, right, |left, right| left * right, |wide| {
                        range.outside::<S>(wide)
                    })
                }
            }
        )+
    };
}

widened_signed!(i8 => i16, i16 => i32, i32 => i64);

/// The products of unsigned narrow types, computed in lanes twice as wide.
macro_rules! widened_unsigned {
    ($($narrow:ty => $wide:ty),+ $(,)?) => {
        $(
            impl registers::Multiplicative for $narrow {
                #[inline(always)]
                fn product_lanes<S: Simd>(
                    simd: S,
                    left: Self::Native<S>,
                    right: Self::Native<S>,
                ) -> RegisterLanes<Self::Native<S>> {
                    let range = UnsignedRange {
                        max: <$wide as SimdIntElement>::Native::<S>::splat(
                            simd,
                            <$wide>::from(<$narrow>::MAX),
                        ),
                    };
                    widened(left, right, |left, right| left * right, |wide| {
                        range.outside::<S>(wide)
                    })
                }
            }
        )+
    };
}

widened_unsigned!(u8 => u16, u16 => u32, u32 => u64);

/// A signed sum computed at the operands' own width. It overflows exactly when both operands have
/// one sign and the wrapped sum has the other, which sets the sign bit of both exclusive ors of an
/// operand with the sum.
#[inline(always)]
fn signed_sum<S: Simd, V: SimdInt<S>>(simd: S, left: V, right: V) -> RegisterLanes<V> {
    let sum = left + right;
    let overflowed = (left ^ sum) & (right ^ sum);
    RegisterLanes {
        values: sum,
        failed: overflowed
            .simd_lt(V::splat(simd, V::Element::default()))
            .to_bitmask(),
    }
}

/// A signed difference computed at the operands' own width. It overflows exactly when the operands
/// have different signs and the wrapped difference has the sign of the right operand.
#[inline(always)]
fn signed_difference<S: Simd, V: SimdInt<S>>(simd: S, left: V, right: V) -> RegisterLanes<V> {
    let difference = left - right;
    let overflowed = (left ^ right) & (left ^ difference);
    RegisterLanes {
        values: difference,
        failed: overflowed
            .simd_lt(V::splat(simd, V::Element::default()))
            .to_bitmask(),
    }
}

/// An unsigned sum computed at the operands' own width. It carries out of the width exactly when
/// its wrapped value is below an operand.
#[inline(always)]
fn unsigned_sum<S: Simd, V: SimdInt<S>>(left: V, right: V) -> RegisterLanes<V> {
    let sum = left + right;
    RegisterLanes {
        values: sum,
        failed: sum.simd_lt(left).to_bitmask(),
    }
}

/// An unsigned difference computed at the operands' own width. It borrows exactly when the right
/// operand is the larger one.
#[inline(always)]
fn unsigned_difference<S: Simd, V: SimdInt<S>>(left: V, right: V) -> RegisterLanes<V> {
    RegisterLanes {
        values: left - right,
        failed: left.simd_lt(right).to_bitmask(),
    }
}

/// The sums and differences of signed types, computed at their own width.
macro_rules! signed_lanes {
    ($($native:ty),+ $(,)?) => {
        $(
            impl registers::Additive for $native {
                #[inline(always)]
                fn sum_lanes<S: Simd>(
                    simd: S,
                    left: Self::Native<S>,
                    right: Self::Native<S>,
                ) -> RegisterLanes<Self::Native<S>> {
                    signed_sum(simd, left, right)
                }

                #[inline(always)]
                fn difference_lanes<S: Simd>(
                    simd: S,
                    left: Self::Native<S>,
                    right: Self::Native<S>,
                ) -> RegisterLanes<Self::Native<S>> {
                    signed_difference(simd, left, right)
                }
            }
        )+
    };
}

signed_lanes!(i8, i16, i32, i64);

/// The sums and differences of unsigned types, computed at their own width.
macro_rules! unsigned_lanes {
    ($($native:ty),+ $(,)?) => {
        $(
            impl registers::Additive for $native {
                #[inline(always)]
                fn sum_lanes<S: Simd>(
                    _simd: S,
                    left: Self::Native<S>,
                    right: Self::Native<S>,
                ) -> RegisterLanes<Self::Native<S>> {
                    unsigned_sum(left, right)
                }

                #[inline(always)]
                fn difference_lanes<S: Simd>(
                    _simd: S,
                    left: Self::Native<S>,
                    right: Self::Native<S>,
                ) -> RegisterLanes<Self::Native<S>> {
                    unsigned_difference(left, right)
                }
            }
        )+
    };
}

unsigned_lanes!(u8, u16, u32, u64);

/// One side of a lane operation, read one word of lanes at a time.
trait Side<N> {
    /// The operands of every whole word, in order.
    fn words<'a>(&'a self) -> impl Iterator<Item = &'a [N; WORD_LANES]>
    where
        N: 'a;

    /// The operands of the lanes past the last whole word, padded to a word.
    fn tail(&self) -> &[N; WORD_LANES];
}

/// A side with a value of its own for every lane: its whole words, and its remaining lanes padded
/// with zeros to a word.
struct Run<'a, N> {
    words: &'a [[N; WORD_LANES]],
    tail: [N; WORD_LANES],
}

impl<'a, N: SimdIntElement> Run<'a, N> {
    fn new(values: &'a [N]) -> Self {
        let (words, remainder) = values.as_chunks::<WORD_LANES>();
        let mut tail = [N::default(); WORD_LANES];
        tail[..remainder.len()].copy_from_slice(remainder);
        Self { words, tail }
    }
}

impl<N> Side<N> for Run<'_, N> {
    fn words<'a>(&'a self) -> impl Iterator<Item = &'a [N; WORD_LANES]>
    where
        N: 'a,
    {
        self.words.iter()
    }

    fn tail(&self) -> &[N; WORD_LANES] {
        &self.tail
    }
}

/// A side whose one value every lane shares, repeated across a word.
struct Shared<N> {
    word: [N; WORD_LANES],
}

impl<N: SimdIntElement> Shared<N> {
    fn new(value: N) -> Self {
        Self {
            word: [value; WORD_LANES],
        }
    }
}

impl<N> Side<N> for Shared<N> {
    fn words<'a>(&'a self) -> impl Iterator<Item = &'a [N; WORD_LANES]>
    where
        N: 'a,
    {
        iter::repeat(&self.word)
    }

    fn tail(&self) -> &[N; WORD_LANES] {
        &self.word
    }
}

/// Computes `operation` for every lane of `operands`. Each shape of operands runs a loop of its
/// own, so no word decides which shape it has.
#[inline(always)]
fn compute<S: Simd, N: SimdIntElement>(
    simd: S,
    operands: LaneOperands<'_, N>,
    operation: impl Fn(S, N::Native<S>, N::Native<S>) -> RegisterLanes<N::Native<S>>,
) -> CheckedLanes<N> {
    match operands {
        LaneOperands::Runs { left, right } => {
            let lanes = left.len().min(right.len());
            let left = Run::new(&left[..lanes]);
            let right = Run::new(&right[..lanes]);
            compute_sides(simd, lanes, &left, &right, &operation)
        }
        LaneOperands::SharedLeft { left, right } => {
            let left = Shared::new(left);
            compute_sides(simd, right.len(), &left, &Run::new(right), &operation)
        }
        LaneOperands::SharedRight { left, right } => {
            let right = Shared::new(right);
            compute_sides(simd, left.len(), &Run::new(left), &right, &operation)
        }
    }
}

/// Computes `operation` for `lanes` lanes of two sides, one word of lanes at a time.
#[inline(always)]
fn compute_sides<S: Simd, N: SimdIntElement>(
    simd: S,
    lanes: usize,
    left: &impl Side<N>,
    right: &impl Side<N>,
    operation: &impl Fn(S, N::Native<S>, N::Native<S>) -> RegisterLanes<N::Native<S>>,
) -> CheckedLanes<N> {
    let mut values = vec![N::default(); lanes];
    let mut failed = vec![0_u64; lanes.div_ceil(WORD_LANES)];
    let (value_words, value_tail) = values.as_chunks_mut::<WORD_LANES>();
    let words = value_words
        .iter_mut()
        .zip(failed.iter_mut())
        .zip(left.words().zip(right.words()));
    for ((word_values, word_failed), (left, right)) in words {
        *word_failed = compute_word(simd, left, right, word_values, operation);
    }
    if let Some(tail_failed) = failed.last_mut()
        && !value_tail.is_empty()
    {
        // The padding lanes compute on zeros or on the shared value, and their failure bits are
        // cleared along with their values.
        let mut tail_values = [N::default(); WORD_LANES];
        let padded_failed =
            compute_word(simd, left.tail(), right.tail(), &mut tail_values, operation);
        value_tail.copy_from_slice(&tail_values[..value_tail.len()]);
        *tail_failed = padded_failed & lane_mask(value_tail.len());
    }
    CheckedLanes { values, failed }
}

/// Computes `operation` for the 64 lanes of one word, one register at a time, and packs the
/// registers' failure bits into the word.
#[inline(always)]
fn compute_word<S: Simd, N: SimdIntElement>(
    simd: S,
    left: &[N; WORD_LANES],
    right: &[N; WORD_LANES],
    values: &mut [N; WORD_LANES],
    operation: &impl Fn(S, N::Native<S>, N::Native<S>) -> RegisterLanes<N::Native<S>>,
) -> u64 {
    // Every native register holds 2, 4, 8, 16, 32 or 64 lanes, so the registers tile a word.
    let width = N::Native::<S>::LEN;
    let mut failed = 0_u64;
    let registers = values
        .chunks_exact_mut(width)
        .zip(left.chunks_exact(width))
        .zip(right.chunks_exact(width));
    for (register, ((register_values, left), right)) in registers.enumerate() {
        let left = N::Native::<S>::from_slice(simd, left);
        let right = N::Native::<S>::from_slice(simd, right);
        let lanes = operation(simd, left, right);
        lanes.values.store_slice(register_values);
        failed |= lanes.failed << (register * width);
    }
    failed
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use meticulous::ResultExt as _;

    use super::*;
    use crate::supported_levels;

    /// The scalar reference of each operation: its wrapped result and whether it overflowed.
    trait Overflowing: Copy + Debug + Default + PartialEq {
        fn reference_sum(self, right: Self) -> (Self, bool);

        fn reference_difference(self, right: Self) -> (Self, bool);

        fn reference_product(self, right: Self) -> (Self, bool);
    }

    macro_rules! overflowing {
        ($($native:ty),+ $(,)?) => {
            $(
                impl Overflowing for $native {
                    fn reference_sum(self, right: Self) -> (Self, bool) {
                        self.overflowing_add(right)
                    }

                    fn reference_difference(self, right: Self) -> (Self, bool) {
                        self.overflowing_sub(right)
                    }

                    fn reference_product(self, right: Self) -> (Self, bool) {
                        self.overflowing_mul(right)
                    }
                }
            )+
        };
    }

    overflowing!(i8, u8, i16, u16, i32, u32, i64, u64);

    #[derive(Debug, Clone, Copy)]
    enum Operation {
        Sum,
        Difference,
        Product,
    }

    impl Operation {
        fn reference<N: Overflowing>(self, left: N, right: N) -> (N, bool) {
            match self {
                Self::Sum => left.reference_sum(right),
                Self::Difference => left.reference_difference(right),
                Self::Product => left.reference_product(right),
            }
        }

        /// Every lane of `operands` through the scalar reference, each failure bit set by its own
        /// shift.
        fn expected<N: Overflowing>(self, operands: LaneOperands<'_, N>) -> CheckedLanes<N> {
            let mut lanes = Vec::new();
            match operands {
                LaneOperands::Runs { left, right } => {
                    lanes.extend(left.iter().copied().zip(right.iter().copied()));
                }
                LaneOperands::SharedLeft { left, right } => {
                    lanes.extend(right.iter().map(|right| (left, *right)));
                }
                LaneOperands::SharedRight { left, right } => {
                    lanes.extend(left.iter().map(|left| (*left, right)));
                }
            }
            let mut values = Vec::with_capacity(lanes.len());
            let mut failed = vec![0_u64; lanes.len().div_ceil(WORD_LANES)];
            for (lane, (left, right)) in lanes.into_iter().enumerate() {
                let (value, overflowed) = self.reference(left, right);
                values.push(value);
                if overflowed {
                    failed[lane / WORD_LANES] |= 1_u64 << (lane % WORD_LANES);
                }
            }
            CheckedLanes { values, failed }
        }
    }

    /// Compares a kernel's lanes with the reference's, naming the first lane that differs rather
    /// than printing whole runs, and then the words, so no bit past the last lane is set either.
    fn assert_lanes<N: Overflowing>(
        actual: &CheckedLanes<N>,
        expected: &CheckedLanes<N>,
        context: &str,
    ) {
        assert_eq!(actual.values.len(), expected.values.len(), "{context}");
        for (lane, (value, reference)) in actual.values.iter().zip(&expected.values).enumerate() {
            let bit = 1_u64 << (lane % WORD_LANES);
            let failed = actual.failed[lane / WORD_LANES] & bit != 0;
            let reference_failed = expected.failed[lane / WORD_LANES] & bit != 0;
            assert_eq!(
                (value, failed),
                (reference, reference_failed),
                "lane {lane}, {context}"
            );
        }
        assert_eq!(actual.failed, expected.failed, "{context}");
    }

    fn assert_sums_and_differences<N: CheckedLane + Overflowing>(
        operands: LaneOperands<'_, N>,
        context: &str,
    ) {
        for level in supported_levels() {
            let kernel = CheckedArithmetic { level };
            assert_lanes(
                &kernel.sums(operands),
                &Operation::Sum.expected(operands),
                &format!("sums at {level:?}, {context}"),
            );
            assert_lanes(
                &kernel.differences(operands),
                &Operation::Difference.expected(operands),
                &format!("differences at {level:?}, {context}"),
            );
        }
    }

    fn assert_every_operation<N: WidenedLane + Overflowing>(
        operands: LaneOperands<'_, N>,
        context: &str,
    ) {
        assert_sums_and_differences(operands, context);
        for level in supported_levels() {
            let kernel = CheckedArithmetic { level };
            assert_lanes(
                &kernel.products(operands),
                &Operation::Product.expected(operands),
                &format!("products at {level:?}, {context}"),
            );
        }
    }

    /// Checks every pair of `values` as two runs, and every value shared by one side against all
    /// of them on the other.
    fn check_every_pair<N: Overflowing>(values: &[N], check: impl Fn(LaneOperands<'_, N>, &str)) {
        let mut left = Vec::with_capacity(values.len() * values.len());
        let mut right = Vec::with_capacity(values.len() * values.len());
        for first in values {
            for second in values {
                left.push(*first);
                right.push(*second);
            }
        }
        check(
            LaneOperands::Runs {
                left: &left,
                right: &right,
            },
            "every pair as runs",
        );
        for shared in values {
            check(
                LaneOperands::SharedLeft {
                    left: *shared,
                    right: values,
                },
                &format!("shared left {shared:?}"),
            );
            check(
                LaneOperands::SharedRight {
                    left: values,
                    right: *shared,
                },
                &format!("shared right {shared:?}"),
            );
        }
    }

    #[test]
    fn every_i8_and_u8_operand_pair_matches_overflowing_arithmetic_at_every_level() {
        let signed = (i8::MIN..=i8::MAX).collect::<Vec<_>>();
        check_every_pair(&signed, assert_every_operation);
        let unsigned = (u8::MIN..=u8::MAX).collect::<Vec<_>>();
        check_every_pair(&unsigned, assert_every_operation);
    }

    /// A value drawn from the whole range of a type, or from the small values whose products
    /// stay near the type's bounds.
    trait RandomLane: Overflowing {
        fn any(random: &mut fastrand::Rng) -> Self;

        fn small(random: &mut fastrand::Rng) -> Self;
    }

    macro_rules! random_lane {
        ($($native:ty => $any:ident, $small:expr),+ $(,)?) => {
            $(
                impl RandomLane for $native {
                    fn any(random: &mut fastrand::Rng) -> Self {
                        random.$any(..)
                    }

                    fn small(random: &mut fastrand::Rng) -> Self {
                        random.$any($small)
                    }
                }
            )+
        };
    }

    random_lane!(
        i16 => i16, -400..400,
        u16 => u16, 0..800,
        i32 => i32, -100_000..100_000,
        u32 => u32, 0..200_000,
        i64 => i64, -(1_i64 << 40)..(1_i64 << 40),
        u64 => u64, 0..(1_u64 << 41),
    );

    /// Random runs whose lanes mix values from the whole range with small ones, and random values
    /// shared by either side.
    fn check_random<N: RandomLane>(seed: u64, check: impl Fn(LaneOperands<'_, N>, &str)) {
        let mut random = fastrand::Rng::with_seed(seed);
        let lane = |random: &mut fastrand::Rng| {
            if random.bool() {
                N::any(random)
            } else {
                N::small(random)
            }
        };
        let left = (0..4_099).map(|_| lane(&mut random)).collect::<Vec<_>>();
        let right = (0..4_099).map(|_| lane(&mut random)).collect::<Vec<_>>();
        check(
            LaneOperands::Runs {
                left: &left,
                right: &right,
            },
            &format!("random runs of seed {seed}"),
        );
        for _ in 0..16 {
            let shared = lane(&mut random);
            check(
                LaneOperands::SharedLeft {
                    left: shared,
                    right: &right,
                },
                &format!("shared left {shared:?} of seed {seed}"),
            );
            check(
                LaneOperands::SharedRight {
                    left: &left,
                    right: shared,
                },
                &format!("shared right {shared:?} of seed {seed}"),
            );
        }
    }

    #[test]
    fn sixteen_and_thirty_two_bit_lanes_match_overflowing_arithmetic_at_their_bounds() {
        // The products of the square roots of each bound, and of the powers of two whose product is
        // one past the maximum or exactly the minimum, sit on either side of the bounds.
        let signed_16 = [
            i16::MIN,
            i16::MIN + 1,
            -16_384,
            -256,
            -182,
            -181,
            -128,
            -2,
            -1,
            0,
            1,
            2,
            128,
            181,
            182,
            255,
            256,
            16_383,
            16_384,
            i16::MAX - 1,
            i16::MAX,
        ];
        check_every_pair(&signed_16, assert_every_operation);
        let unsigned_16 = [
            0,
            1,
            2,
            255,
            256,
            257,
            32_767,
            32_768,
            u16::MAX - 1,
            u16::MAX,
        ];
        check_every_pair(&unsigned_16, assert_every_operation);
        let signed_32 = [
            i32::MIN,
            i32::MIN + 1,
            -1_073_741_824,
            -65_536,
            -46_341,
            -46_340,
            -32_768,
            -2,
            -1,
            0,
            1,
            2,
            32_768,
            46_340,
            46_341,
            65_535,
            65_536,
            1_073_741_823,
            1_073_741_824,
            i32::MAX - 1,
            i32::MAX,
        ];
        check_every_pair(&signed_32, assert_every_operation);
        let unsigned_32 = [
            0,
            1,
            2,
            65_535,
            65_536,
            65_537,
            2_147_483_647,
            2_147_483_648,
            u32::MAX - 1,
            u32::MAX,
        ];
        check_every_pair(&unsigned_32, assert_every_operation);
    }

    #[test]
    fn sixteen_and_thirty_two_bit_lanes_match_overflowing_arithmetic_on_random_operands() {
        check_random::<i16>(0x0007_0016, assert_every_operation);
        check_random::<u16>(0x0007_F016, assert_every_operation);
        check_random::<i32>(0x0007_0032, assert_every_operation);
        check_random::<u32>(0x0007_F032, assert_every_operation);
    }

    #[test]
    fn sixty_four_bit_sums_and_differences_match_overflowing_arithmetic() {
        let signed = [
            i64::MIN,
            i64::MIN + 1,
            i64::MIN / 2,
            -2,
            -1,
            0,
            1,
            2,
            i64::MAX / 2,
            i64::MAX / 2 + 1,
            i64::MAX - 1,
            i64::MAX,
        ];
        check_every_pair(&signed, assert_sums_and_differences);
        let unsigned = [
            0,
            1,
            2,
            u64::MAX / 2,
            u64::MAX / 2 + 1,
            u64::MAX - 1,
            u64::MAX,
        ];
        check_every_pair(&unsigned, assert_sums_and_differences);
        check_random::<i64>(0x0007_0064, assert_sums_and_differences);
        check_random::<u64>(0x0007_F064, assert_sums_and_differences);
    }

    /// Runs of every length through three words and one lane, so every register and word tail is
    /// reached, alone and after whole words, with failures scattered across the lanes.
    fn check_run_lengths<N: RandomLane>(seed: u64, check: impl Fn(LaneOperands<'_, N>, &str)) {
        let mut random = fastrand::Rng::with_seed(seed);
        let lanes = 3 * WORD_LANES + 1;
        let left = (0..lanes).map(|_| N::any(&mut random)).collect::<Vec<_>>();
        let right = (0..lanes).map(|_| N::any(&mut random)).collect::<Vec<_>>();
        let shared = N::any(&mut random);
        for length in 0..=lanes {
            check(
                LaneOperands::Runs {
                    left: &left[..length],
                    right: &right[..length],
                },
                &format!("runs of {length} lanes"),
            );
            check(
                LaneOperands::SharedLeft {
                    left: shared,
                    right: &right[..length],
                },
                &format!("{length} lanes after a shared left"),
            );
            check(
                LaneOperands::SharedRight {
                    left: &left[..length],
                    right: shared,
                },
                &format!("{length} lanes before a shared right"),
            );
        }
    }

    #[test]
    fn every_run_length_computes_its_lanes_and_clears_the_bits_past_them() {
        check_run_lengths::<i16>(0x0007_1016, assert_every_operation);
        check_run_lengths::<u16>(0x0007_1F16, assert_every_operation);
        check_run_lengths::<i32>(0x0007_1032, assert_every_operation);
        check_run_lengths::<u32>(0x0007_1F32, assert_every_operation);
        check_run_lengths::<i64>(0x0007_1064, assert_sums_and_differences);
        check_run_lengths::<u64>(0x0007_1F64, assert_sums_and_differences);
        let bytes = (0..=u8::MAX).cycle().take(3 * WORD_LANES + 1);
        let unsigned = bytes.clone().collect::<Vec<_>>();
        let reversed = unsigned.iter().rev().copied().collect::<Vec<_>>();
        for length in 0..=unsigned.len() {
            assert_every_operation(
                LaneOperands::Runs {
                    left: &unsigned[..length],
                    right: &reversed[..length],
                },
                &format!("bytes over {length} lanes"),
            );
        }
    }

    #[test]
    fn runs_of_different_lengths_pair_their_values_by_index() {
        let left = [i32::MAX, 5, 7];
        let right = [1, 2];
        let kernel = CheckedArithmetic::new();

        let sums = kernel.sums(LaneOperands::Runs {
            left: &left,
            right: &right,
        });
        let products = CheckedArithmetic::default().products(LaneOperands::Runs {
            left: &right,
            right: &left,
        });

        assert_eq!(sums.values, [i32::MIN, 7]);
        assert_eq!(sums.failed, [0b01]);
        assert_eq!(products.values, [i32::MAX, 10]);
        assert_eq!(products.failed, [0b00]);
    }

    /// A lane type read from the low bits of a generated 64-bit pattern.
    trait RawLane: Overflowing {
        fn from_raw(raw: u64) -> Self;
    }

    macro_rules! raw_lane {
        ($($native:ty),+ $(,)?) => {
            $(
                impl RawLane for $native {
                    fn from_raw(raw: u64) -> Self {
                        let bytes = raw.to_le_bytes();
                        let (low, _) = bytes.split_at(size_of::<Self>());
                        let low = low
                            .try_into()
                            .assured("the split keeps exactly the bytes the type holds");
                        Self::from_le_bytes(low)
                    }
                }
            )+
        };
    }

    raw_lane!(i8, u8, i16, u16, i32, u32, i64, u64);

    /// One generated case: the operand shape, the run length, the shared value, and the pairs of
    /// lanes, which repeat until the run is as long as the case asks.
    struct GeneratedCase<'a> {
        shape: u8,
        length: usize,
        shared: u64,
        pairs: &'a [(u64, u64)],
    }

    impl GeneratedCase<'_> {
        fn check<N: RawLane>(&self, check: impl Fn(LaneOperands<'_, N>, &str)) {
            let mut left = Vec::with_capacity(self.length);
            let mut right = Vec::with_capacity(self.length);
            for (left_raw, right_raw) in self.pairs.iter().cycle().take(self.length) {
                left.push(N::from_raw(*left_raw));
                right.push(N::from_raw(*right_raw));
            }
            let shared = N::from_raw(self.shared);
            let operands = match self.shape % 3 {
                0 => LaneOperands::Runs {
                    left: &left,
                    right: &right,
                },
                1 => LaneOperands::SharedLeft {
                    left: shared,
                    right: &right,
                },
                _ => LaneOperands::SharedRight {
                    left: &left,
                    right: shared,
                },
            };
            check(operands, "a generated case");
        }
    }

    #[test]
    fn bolero_checked_lanes_match_overflowing_arithmetic_at_every_level() {
        bolero::check!()
            .with_iterations(256)
            .with_max_len(128)
            .with_type::<(u8, u8, u8, u64, Vec<(u64, u64)>)>()
            .for_each(|&(width, shape, length, shared, ref pairs)| {
                let case = GeneratedCase {
                    shape,
                    length: usize::from(length),
                    shared,
                    pairs,
                };
                match width % 8 {
                    0 => case.check::<i8>(assert_every_operation),
                    1 => case.check::<u8>(assert_every_operation),
                    2 => case.check::<i16>(assert_every_operation),
                    3 => case.check::<u16>(assert_every_operation),
                    4 => case.check::<i32>(assert_every_operation),
                    5 => case.check::<u32>(assert_every_operation),
                    6 => case.check::<i64>(assert_sums_and_differences),
                    _ => case.check::<u64>(assert_sums_and_differences),
                }
            });
    }

    #[test]
    fn an_empty_run_computes_no_lanes_and_no_words() {
        let kernel = CheckedArithmetic::new();
        let empty: [u16; 0] = [];

        let sums = kernel.sums(LaneOperands::SharedLeft {
            left: 3_u16,
            right: &empty,
        });

        assert!(sums.values.is_empty());
        assert!(sums.failed.is_empty());
    }
}
