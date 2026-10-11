//! Decimal rounding kernel tests.
//!
//! Layer: test harness.
//!
//! - **Owns.** Reference cases for `round(value, digits)` computed with exact decimal arithmetic,
//!   agreement between the significand arithmetic and the decimal expansion it falls back to, the
//!   integer overflow and tie boundaries, and how the kernels treat digit columns and nulls.
//! - **Depends on.** The decimal rounding kernels and Arrow arrays.
//! - **Must not know.** Programs, registers or how the runtime records a failed lane.

use arrow_array::{
    Array, Float32Array, Float64Array, Int8Array, Int64Array, UInt8Array, UInt64Array,
    types::{
        Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use nervix_approx_into::CheckedApproxInto as _;

use super::*;
use crate::batch::TypedArray;

/// `(value, digits, expected bits)`, where each expected value is the `F64` nearest to the exact
/// decimal rounding of the value's binary value, computed with Python's `decimal` module at 3000
/// digits of precision and `ROUND_HALF_UP`, which rounds halves away from zero.
const F64_REFERENCES: [(f64, i16, u64); 33] = [
    // 0.15 is stored below 0.15, so it rounds down although its decimal digits look like a tie.
    (0.15, 1, 0x3fb9_9999_9999_999a),
    (2.675, 2, 0x4005_5c28_f5c2_8f5c),
    (1.005, 2, 0x3ff0_0000_0000_0000),
    // 0.125 is exactly a tie at two places, and a tie rounds away from zero.
    (0.125, 2, 0x3fc0_a3d7_0a3d_70a4),
    (-0.125, 2, 0xbfc0_a3d7_0a3d_70a4),
    (2.5, 0, 0x4008_0000_0000_0000),
    (-2.5, 0, 0xc008_0000_0000_0000),
    (0.499_999_999_999_999_94, 0, 0x0000_0000_0000_0000),
    (1_234.567_8, -2, 0x4092_c000_0000_0000),
    (1_250.0, -2, 0x4094_5000_0000_0000),
    (-1_250.0, -2, 0xc094_5000_0000_0000),
    // A negative value that rounds to zero keeps its sign.
    (-0.004, 2, 0x8000_0000_0000_0000),
    (123_456_789.123_456_79, 4, 0x419d_6f34_547e_76c9),
    (5e-324, 323, 0x0000_0000_0000_0000),
    (5e-324, 324, 0x0000_0000_0000_0001),
    // Rounding the largest value to a multiple of 10^308 overflows to an infinity.
    (f64::MAX, -308, 0x7ff0_0000_0000_0000),
    (1e22, -22, 0x4480_f0cf_064d_d592),
    (4.5e22, -23, 0x0000_0000_0000_0000),
    // 5e22 is stored below 5e22, so it is below half of 10^23.
    (5e22, -23, 0x0000_0000_0000_0000),
    (5.5e22, -23, 0x44b5_2d02_c7e1_4af6),
    (1.234_567_890_123_456_8e-10, 25, 0x3de0_f7bf_e5e2_538c),
    (5.960_464_477_539_063e-8, 23, 0x3e70_0000_0000_0000),
    (-5.960_464_477_539_063e-8, 23, 0xbe70_0000_0000_0000),
    (1.2345e30, -25, 0x462f_29c4_eeb1_55e5),
    (1e23, -22, 0x44b5_2d02_c7e1_4af6),
    (0.1, 20, 0x3fb9_9999_9999_999a),
    (0.1, 17, 0x3fb9_9999_9999_999a),
    (1e-7, 7, 0x3e7a_d7f2_9abc_af48),
    (4.567_89, 3, 0x4012_45a1_cac0_8312),
    (-4.567_89, 3, 0xc012_45a1_cac0_8312),
    (6.022_140_76e23, -20, 0x44df_e154_f457_ea13),
    (1.5, -400, 0x0000_0000_0000_0000),
    (f64::MAX, 400, 0x7fef_ffff_ffff_ffff),
];

/// `(value bits, digits, expected bits)` for `F32`, where each expected value is the `F32` nearest
/// to the exact decimal rounding of the value's binary value.
const F32_REFERENCES: [(u32, i16, u32); 16] = [
    // The F32 nearest to 0.15 is above 0.15, so it rounds up.
    (0x3e19_999a, 1, 0x3e4c_cccd),
    (0x402b_3333, 2, 0x402a_e148),
    (0x3e00_0000, 2, 0x3e05_1eb8),
    (0xbe00_0000, 2, 0xbe05_1eb8),
    (0x4020_0000, 0, 0x4040_0000),
    (0x449a_522b, -2, 0x4496_0000),
    (0xbb83_126f, 2, 0x8000_0000),
    (0x0000_0001, 44, 0x0000_0000),
    (0x0000_0001, 45, 0x0000_0001),
    (0x7f7f_ffff, -38, 0x7f61_b1e6),
    (0x2f07_bdff, 15, 0x2f07_be0e),
    (0x7f7f_c99e, -10, 0x7f7f_c99e),
    (0x7f7f_c99e, -11, 0x7f7f_c99e),
    (0x4b80_0000, -1, 0x4b80_0002),
    (0x3dcc_cccd, 9, 0x3dcc_cccd),
    (0x3fc0_0000, -60, 0x0000_0000),
];

#[test]
fn floats_round_exactly_to_the_reference_decimal_rounding() {
    for (value, digits, expected) in F64_REFERENCES {
        let rounded = value.rounded_to_digits(digits);
        assert_eq!(
            rounded.to_bits(),
            expected,
            "round({value:e}, {digits}) gave {rounded:e}, expected {:e}",
            f64::from_bits(expected)
        );
    }
    for (bits, digits, expected) in F32_REFERENCES {
        let value = f32::from_bits(bits);
        let rounded = value.rounded_to_digits(digits);
        assert_eq!(
            rounded.to_bits(),
            expected,
            "round({value:e}, {digits}) gave {rounded:e}, expected {:e}",
            f32::from_bits(expected)
        );
    }
}

#[test]
fn the_decimal_expansion_reproduces_every_reference_including_ties() {
    for (value, digits, expected) in F64_REFERENCES {
        let rounded = value.rounded_by_decimal_expansion(value.binary_parts(), digits);
        assert_eq!(rounded.to_bits(), expected, "round({value:e}, {digits})");
    }
    for (bits, digits, expected) in F32_REFERENCES {
        let value = f32::from_bits(bits);
        let rounded = value.rounded_by_decimal_expansion(value.binary_parts(), digits);
        assert_eq!(rounded.to_bits(), expected, "round({value:e}, {digits})");
    }
}

/// A digit count as a column writes it. Most counts lie within forty places either side, around
/// every exactly representable power of ten; the rest reach the whole bound and beyond it, where a
/// count reads as the nearer end of the bound.
#[derive(Debug, bolero::TypeGenerator)]
enum DigitCount {
    Near(i8),
    Within(i16),
    Beyond(i64),
}

impl DigitCount {
    fn written(&self) -> i64 {
        match self {
            Self::Near(count) => i64::from(*count % 41),
            Self::Within(count) => i64::from(*count % (DIGITS_BOUND + 1)),
            Self::Beyond(count) => *count,
        }
    }
}

/// The count a written digit count rounds with: the count itself within the bound, and the nearer
/// end of the bound beyond it.
fn bounded_count(written: i128) -> i16 {
    let bound = i128::from(DIGITS_BOUND);
    i16::try_from(written.clamp(-bound, bound)).assured("the clamp keeps the count within ±400")
}

/// A generated float to round and the digit count it is rounded with.
#[derive(Debug, bolero::TypeGenerator)]
enum RoundedValue {
    /// Any bit pattern of the type: subnormals, extremes, signed zeros and non-finite values.
    Bits { bits: u64, digits: DigitCount },
    /// Any `F64` bit pattern, narrowed to `F32`, rounded at a place within its own significant
    /// digits, so the rounding moves the value.
    Scaled { bits: u64, place: i8 },
    /// `significand × 10^exponent`, read as the nearest float of each type, rounded at a place
    /// within its digits: decimal digits that look like a tie, which the binary value lies to one
    /// side of.
    Decimal {
        significand: i32,
        exponent: i8,
        place: i8,
    },
    /// `±(2 × odd + 1) / 2^places`, an exact binary tie whose last decimal digit is a 5, rounded to
    /// one place fewer.
    Tie {
        odd: u32,
        places: u8,
        negative: bool,
    },
}

impl RoundedValue {
    fn wide(&self) -> f64 {
        match self {
            Self::Bits { bits, .. } | Self::Scaled { bits, .. } => f64::from_bits(*bits),
            Self::Decimal {
                significand,
                exponent,
                ..
            } => Self::decimal_text(*significand, *exponent)
                .parse()
                .assured("a signed integer significand and exponent are valid float syntax"),
            Self::Tie {
                odd,
                places,
                negative,
            } => {
                let doubled = u64::from(*odd)
                    .checked_mul(2)
                    .assured("twice a u32 is below 2^33");
                let numerator = doubled
                    .checked_add(1)
                    .assured("twice a u32 plus one is below 2^33");
                let numerator: f64 = numerator.approx_into();
                let exponent = -i32::from(Self::tie_places(*places));
                let magnitude = numerator * 2_f64.powi(exponent);
                if *negative { -magnitude } else { magnitude }
            }
        }
    }

    fn narrow(&self) -> f32 {
        match self {
            Self::Bits { bits, .. } => {
                let high = u32::try_from(bits >> 32).assured("the high half of a u64 fits u32");
                f32::from_bits(high)
            }
            Self::Decimal {
                significand,
                exponent,
                ..
            } => Self::decimal_text(*significand, *exponent)
                .parse()
                .assured("a signed integer significand and exponent are valid float syntax"),
            Self::Scaled { .. } | Self::Tie { .. } => self.wide().approx_into(),
        }
    }

    /// The digit count as the column writes it.
    fn written_digits(&self) -> i64 {
        match self {
            Self::Bits { digits, .. } => digits.written(),
            Self::Scaled { place, .. } => {
                let wide = self.wide();
                let place = i64::from(*place % 20);
                if !wide.is_finite() || wide == 0.0 {
                    return place;
                }
                // The power of ten of the value's leading digit, so the place counts the digits
                // kept after it; an estimate one off still lands within the significant digits.
                let scale = wide.abs().log10().floor();
                let scale: i64 = scale
                    .checked_approx_into()
                    .assured("the leading power of ten of a finite F64 is within ±324");
                place - scale
            }
            Self::Decimal {
                exponent, place, ..
            } => i64::from(*place % 10) - i64::from(*exponent % 31),
            Self::Tie { places, .. } => i64::from(Self::tie_places(*places)) - 1,
        }
    }

    fn decimal_text(significand: i32, exponent: i8) -> String {
        format!("{}e{}", significand % 10_000_000, exponent % 31)
    }

    /// Between one and sixty fractional binary places, so the tie is a normal value of both types.
    fn tie_places(places: u8) -> u8 {
        places % 60 + 1
    }
}

/// One lane of a generated column pair: the value, its digit count and whether either is null.
#[derive(Debug, bolero::TypeGenerator)]
struct FloatLane {
    value: RoundedValue,
    null_value: bool,
    null_digits: bool,
}

impl FloatLane {
    /// The lane's value as `value` reads it for the column's type, or a null.
    fn value_cell<T>(&self, value: &impl Fn(&RoundedValue) -> T) -> Option<T> {
        if self.null_value {
            return None;
        }
        Some(value(&self.value))
    }

    /// The lane's digit count as its column writes it, or a null.
    fn digits_cell(&self) -> Option<i64> {
        if self.null_digits {
            return None;
        }
        Some(self.value.written_digits())
    }

    /// Whether the value or the digit count of the lane is null.
    fn has_null(&self) -> bool {
        self.null_value || self.null_digits
    }
}

/// The bits of a float, which compare NaN payloads and signed zeros exactly.
trait FloatBits: DecimalRounding + Copy + std::fmt::Debug {
    fn bits(self) -> u64;
}

impl FloatBits for f64 {
    fn bits(self) -> u64 {
        self.to_bits()
    }
}

impl FloatBits for f32 {
    fn bits(self) -> u64 {
        u64::from(self.to_bits())
    }
}

impl BinaryParts {
    /// The fractional decimal digits of the value's exact expansion: a lowest set bit of `2^-n`
    /// gives exactly `n` of them.
    fn fraction_digits(self) -> i64 {
        let lowest_set_bit =
            i64::from(self.exponent) + i64::from(self.significand.trailing_zeros());
        if lowest_set_bit < 0 {
            -lowest_set_bit
        } else {
            0
        }
    }
}

/// The scalar rounding of one value, checked against the exact decimal expansion: the value of
/// the type nearest to the exact rounding. A value with no more fractional digits than the count is
/// already its own rounding, and rounding is symmetric about zero.
fn assert_scalar_rounding<F: FloatBits>(value: F, digits: i16) -> F {
    let rounded = value.rounded_to_digits(digits);
    if value.is_nan_value() {
        assert!(
            rounded.is_nan_value(),
            "round(NaN, {digits}) gave {rounded:?}"
        );
        return rounded;
    }
    if !value.is_finite_value() || value == F::default() {
        assert_eq!(
            rounded.bits(),
            value.bits(),
            "round({value:?}, {digits}) changed an infinity or a zero to {rounded:?}"
        );
        return rounded;
    }
    let parts = value.binary_parts();
    let expected = value.rounded_by_decimal_expansion(parts, digits);
    assert_eq!(
        rounded.bits(),
        expected.bits(),
        "round({value:?}, {digits}) gave {rounded:?}, the decimal expansion {expected:?}"
    );
    if i64::from(digits) >= parts.fraction_digits() {
        assert_eq!(
            rounded.bits(),
            value.bits(),
            "round({value:?}, {digits}) holds every fractional digit of the value"
        );
    }
    let mirrored = (-value).rounded_to_digits(digits);
    assert_eq!(
        mirrored.bits(),
        (-rounded).bits(),
        "round({value:?}, {digits}) is not symmetric about zero"
    );
    rounded
}

/// Rounds a column pair through the batch kernel and compares every lane with the scalar rounding
/// of its value: a null value or count makes the lane null without failing it, and a lane whose
/// value or result is NaN or an infinity fails and is null.
fn assert_rounded_column<T>(lanes: &[FloatLane], value: impl Fn(&RoundedValue) -> T::Native)
where
    T: ArrowPrimitiveType,
    T::Native: FloatBits,
{
    let values = PrimitiveArray::<T>::from_iter(lanes.iter().map(|lane| lane.value_cell(&value)));
    let digits = Int64Array::from_iter(lanes.iter().map(FloatLane::digits_cell));
    let operand = RoundingDigits::from_typed(&TypedArray::Int64(digits))
        .assured("an Int64 column holds digit counts");
    let rounded = operand.round_floats(&values);
    let failed = rounded.failed.lanes().collect::<Vec<_>>();
    let mut expected_failed = Vec::new();
    for (index, lane) in lanes.iter().enumerate() {
        let digits = bounded_count(i128::from(lane.value.written_digits()));
        let scalar = assert_scalar_rounding(value(&lane.value), digits);
        if lane.has_null() {
            assert!(
                rounded.column.is_null(index),
                "lane {index} has a null operand"
            );
            continue;
        }
        if !scalar.is_finite_value() {
            expected_failed.push(index);
            assert!(rounded.column.is_null(index), "failed lane {index} is null");
            continue;
        }
        assert!(rounded.column.is_valid(index), "lane {index}");
        assert_eq!(
            rounded.column.value(index).bits(),
            scalar.bits(),
            "lane {index}"
        );
    }
    assert_eq!(failed, expected_failed);
}

#[test]
fn bolero_rounded_float_columns_match_the_exact_decimal_expansion() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<Vec<FloatLane>>()
        .for_each(|lanes| {
            assert_rounded_column::<Float64Type>(lanes, RoundedValue::wide);
            assert_rounded_column::<Float32Type>(lanes, RoundedValue::narrow);
        });
}

/// An integer type generated from raw bits, with the reference rounding computed in `i128`.
trait ReferenceInteger: IntegerRounding + Copy + std::fmt::Debug + PartialEq {
    type Arrow: ArrowPrimitiveType<Native = Self>;
    const MIN: Self;
    const MAX: Self;

    fn from_raw(raw: u64) -> Self;

    /// The value `wide` names, which lies within the type.
    fn from_wide(wide: i128) -> Self;

    fn typed(values: PrimitiveArray<Self::Arrow>) -> TypedArray;

    /// The type's value nearest to `value`, as a column of digit counts of this type writes it.
    fn digits_from(value: i64) -> Self;
}

macro_rules! reference_integer {
    ($($native:ty => $arrow:ty, $variant:ident);+ $(;)?) => {$(
        impl ReferenceInteger for $native {
            type Arrow = $arrow;
            const MIN: Self = <$native>::MIN;
            const MAX: Self = <$native>::MAX;

            fn from_raw(raw: u64) -> Self {
                let bytes = raw.to_le_bytes();
                let (low, _) = bytes.split_at(size_of::<Self>());
                Self::from_le_bytes(low.try_into().assured("the slice has the type's byte width"))
            }

            fn from_wide(wide: i128) -> Self {
                Self::try_from(wide).assured("the caller keeps the value within the type")
            }

            fn typed(values: PrimitiveArray<Self::Arrow>) -> TypedArray {
                TypedArray::$variant(values)
            }

            fn digits_from(value: i64) -> Self {
                let clamped = i128::from(value)
                    .clamp(i128::from(Self::MIN), i128::from(Self::MAX));
                Self::try_from(clamped).assured("the clamp keeps the value within the type")
            }
        }
    )+};
}

reference_integer!(
    u8 => UInt8Type, UInt8;
    i8 => Int8Type, Int8;
    u16 => UInt16Type, UInt16;
    i16 => Int16Type, Int16;
    u32 => UInt32Type, UInt32;
    i32 => Int32Type, Int32;
    u64 => UInt64Type, UInt64;
    i64 => Int64Type, Int64;
);

/// The multiple of `10^-digits` nearest to `value`, halves away from zero, computed directly in
/// 128-bit arithmetic, and whether it falls outside the type.
fn reference_integer_rounding<N: ReferenceInteger>(value: N, digits: i16) -> (N, bool) {
    if digits >= 0 {
        return (value, false);
    }
    let exponent = u32::from(digits.unsigned_abs());
    // Half of a power of ten above 10^38 exceeds every value of every type.
    if exponent > 38 {
        return (N::default(), false);
    }
    let power = 10_u128
        .checked_pow(exponent)
        .assured("every power of ten up to 10^38 is below 2^127");
    let wide: i128 = value.into();
    let half = power / 2;
    let raised = wide
        .unsigned_abs()
        .checked_add(half)
        .assured("a magnitude below 2^64 plus half of at most 10^38 is below 2^127");
    let magnitude = raised / power * power;
    let magnitude = i128::try_from(magnitude).assured("the rounded magnitude is below 2^127");
    let signed = if wide < 0 { -magnitude } else { magnitude };
    match N::try_from(signed) {
        Ok(rounded) => (rounded, false),
        Err(_) => (N::default(), true),
    }
}

/// How a generated integer lane builds its value.
#[derive(Debug, bolero::TypeGenerator)]
enum IntegerValue {
    /// Any value of the type.
    Raw(u64),
    /// Within a short distance of the type's largest value, where rounding up overflows.
    NearMax(u16),
    /// Within a short distance of the type's smallest value.
    NearMin(u16),
}

#[derive(Debug, bolero::TypeGenerator)]
struct IntegerLane {
    value: IntegerValue,
    digits: DigitCount,
    null_value: bool,
    null_digits: bool,
}

impl IntegerLane {
    /// The lane's value as a value of `N`, or a null.
    fn value_cell<N: ReferenceInteger>(&self) -> Option<N> {
        if self.null_value {
            return None;
        }
        Some(self.value.value::<N>())
    }

    /// The lane's digit count as its column writes it, or a null.
    fn digits_cell(&self) -> Option<i64> {
        if self.null_digits {
            return None;
        }
        Some(self.digits.written())
    }

    /// Whether the value or the digit count of the lane is null.
    fn has_null(&self) -> bool {
        self.null_value || self.null_digits
    }
}

#[derive(Debug, bolero::TypeGenerator)]
struct IntegerColumn {
    width: u8,
    digits_width: u8,
    lanes: Vec<IntegerLane>,
}

impl IntegerValue {
    fn value<N: ReferenceInteger>(&self) -> N {
        let minimum: i128 = N::MIN.into();
        let maximum: i128 = N::MAX.into();
        match self {
            Self::Raw(raw) => N::from_raw(*raw),
            Self::NearMax(distance) => {
                let wide = maximum - i128::from(*distance);
                N::from_wide(wide.max(minimum))
            }
            Self::NearMin(distance) => {
                let wide = minimum + i128::from(*distance);
                N::from_wide(wide.min(maximum))
            }
        }
    }
}

/// Rounds a generated integer column through the batch kernel and compares every lane with the
/// scalar rounding and its 128-bit reference: a lane whose multiple does not fit the type fails and
/// is null, and a null value or count makes the lane null without failing it.
fn assert_rounded_integers<N: ReferenceInteger>(column: &IntegerColumn) {
    let values = PrimitiveArray::<N::Arrow>::from_iter(
        column.lanes.iter().map(IntegerLane::value_cell::<N>),
    );
    let digits = Int64Array::from_iter(column.lanes.iter().map(IntegerLane::digits_cell));
    let operand = RoundingDigits::from_typed(&TypedArray::Int64(digits))
        .assured("an Int64 column holds digit counts");
    let rounded = operand.round_integers(&values);
    let failed = rounded.failed.lanes().collect::<Vec<_>>();
    let mut expected_failed = Vec::new();
    for (index, lane) in column.lanes.iter().enumerate() {
        let value = lane.value.value::<N>();
        let digits = bounded_count(i128::from(lane.digits.written()));
        let expected = reference_integer_rounding(value, digits);
        assert_eq!(
            value.lane_rounded_to_digits(digits),
            expected,
            "round({value:?}, {digits})"
        );
        if lane.has_null() {
            assert!(
                rounded.column.is_null(index),
                "lane {index} has a null operand"
            );
            continue;
        }
        let (expected_value, expected_failure) = expected;
        if expected_failure {
            expected_failed.push(index);
            assert!(rounded.column.is_null(index), "failed lane {index} is null");
            continue;
        }
        assert!(rounded.column.is_valid(index), "lane {index}");
        assert_eq!(rounded.column.value(index), expected_value, "lane {index}");
    }
    assert_eq!(failed, expected_failed);
}

/// Reads the generated digit counts through a column of type `N` and compares each lane with the
/// count it rounds with: the value itself within the bound and the nearer end beyond it, with the
/// column's nulls kept.
fn assert_digit_operand<N: ReferenceInteger>(column: &IntegerColumn) {
    let mut written = Vec::with_capacity(column.lanes.len());
    let mut nulls = Vec::with_capacity(column.lanes.len());
    let mut null_count = 0_usize;
    for lane in &column.lanes {
        written.push(N::digits_from(lane.digits.written()));
        nulls.push(!lane.null_digits);
        if lane.null_digits {
            null_count += 1;
        }
    }
    // A column without nulls carries no null buffer, as Arrow builds one.
    let null_buffer = if null_count == 0 {
        None
    } else {
        Some(NullBuffer::from(nulls.clone()))
    };
    let typed = PrimitiveArray::<N::Arrow>::new(written.iter().copied().collect(), null_buffer);
    let operand = RoundingDigits::from_typed(&N::typed(typed))
        .assured("an integer column holds digit counts");
    let expected = written
        .iter()
        .map(|count| bounded_count((*count).into()))
        .collect::<Vec<_>>();
    assert_eq!(operand.lanes, expected);
    for (index, valid) in nulls.into_iter().enumerate() {
        let kept = match &operand.nulls {
            Some(kept) => kept.is_valid(index),
            None => true,
        };
        assert_eq!(kept, valid, "lane {index}");
    }
}

#[test]
fn bolero_rounded_integer_columns_match_a_wide_reference() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<IntegerColumn>()
        .for_each(|column| {
            match column.width % 8 {
                0 => assert_rounded_integers::<u8>(column),
                1 => assert_rounded_integers::<i8>(column),
                2 => assert_rounded_integers::<u16>(column),
                3 => assert_rounded_integers::<i16>(column),
                4 => assert_rounded_integers::<u32>(column),
                5 => assert_rounded_integers::<i32>(column),
                6 => assert_rounded_integers::<u64>(column),
                _ => assert_rounded_integers::<i64>(column),
            }
            match column.digits_width % 8 {
                0 => assert_digit_operand::<u8>(column),
                1 => assert_digit_operand::<i8>(column),
                2 => assert_digit_operand::<u16>(column),
                3 => assert_digit_operand::<i16>(column),
                4 => assert_digit_operand::<u32>(column),
                5 => assert_digit_operand::<i32>(column),
                6 => assert_digit_operand::<u64>(column),
                _ => assert_digit_operand::<i64>(column),
            }
        });
}

#[test]
fn non_finite_values_and_zeros_are_returned_unchanged() {
    for digits in [-400, -3, 0, 2, 400] {
        assert!(f64::NAN.rounded_to_digits(digits).is_nan());
        assert_eq!(f64::INFINITY.rounded_to_digits(digits), f64::INFINITY);
        assert_eq!(
            (-0.0_f64).rounded_to_digits(digits).to_bits(),
            (-0.0_f64).to_bits()
        );
        assert_eq!(
            f32::NEG_INFINITY.rounded_to_digits(digits),
            f32::NEG_INFINITY
        );
    }
}

#[test]
fn the_power_of_two_exponent_of_each_power_of_ten_is_exact() {
    for exponent_of_ten in 0..=38_i32 {
        let power = 10_u128.pow(u32::try_from(exponent_of_ten).expect("nonnegative"));
        let expected = i32::try_from(power.ilog2()).expect("below 128");
        assert_eq!(
            binary_exponent_of_power_of_ten(exponent_of_ten),
            expected,
            "10^{exponent_of_ten}"
        );
    }
    for exponent_of_ten in 39..=i32::from(DIGITS_BOUND) {
        let expected = (f64::from(exponent_of_ten) * std::f64::consts::LOG2_10).floor();
        assert_eq!(
            f64::from(binary_exponent_of_power_of_ten(exponent_of_ten)),
            expected,
            "10^{exponent_of_ten}"
        );
    }
}

#[test]
fn integers_round_to_multiples_of_powers_of_ten_with_halves_away_from_zero() {
    let cases: [(i64, i16, Option<i64>); 12] = [
        (1_234, 0, Some(1_234)),
        (1_234, 3, Some(1_234)),
        (1_234, -1, Some(1_230)),
        (1_235, -1, Some(1_240)),
        (-1_235, -1, Some(-1_240)),
        (-1_234, -2, Some(-1_200)),
        (499, -3, Some(0)),
        (500, -3, Some(1_000)),
        (i64::MIN, -18, Some(-9_000_000_000_000_000_000)),
        (i64::MAX, -19, None),
        (i64::MAX, -20, Some(0)),
        (i64::MIN, -400, Some(0)),
    ];
    for (value, digits, expected) in cases {
        let (rounded, failed) = value.lane_rounded_to_digits(digits);
        match expected {
            Some(expected) => {
                assert!(!failed, "round({value}, {digits}) must not fail");
                assert_eq!(rounded, expected, "round({value}, {digits})");
            }
            None => assert!(failed, "round({value}, {digits}) must overflow"),
        }
    }

    assert_eq!(124_i8.lane_rounded_to_digits(-1), (120, false));
    assert!(125_i8.lane_rounded_to_digits(-1).1);
    assert!((-125_i8).lane_rounded_to_digits(-1).1);
    assert_eq!((-124_i8).lane_rounded_to_digits(-1), (-120, false));
    assert_eq!(254_u8.lane_rounded_to_digits(-1), (250, false));
    assert!(255_u8.lane_rounded_to_digits(-1).1);
    assert!(250_u8.lane_rounded_to_digits(-2).1);
    assert_eq!(249_u8.lane_rounded_to_digits(-2), (200, false));
    assert_eq!(
        14_999_999_999_999_999_999_u64.lane_rounded_to_digits(-19),
        (10_000_000_000_000_000_000, false)
    );
    assert!(u64::MAX.lane_rounded_to_digits(-19).1);
}

#[test]
fn digit_columns_of_every_integer_type_are_held_within_the_bound() {
    let wide = RoundingDigits::from_typed(&TypedArray::Int64(Int64Array::from(vec![
        i64::MIN,
        -3,
        2,
        i64::MAX,
    ])))
    .expect("an integer column holds digits");
    assert_eq!(wide.lanes, [-400, -3, 2, 400]);

    let unsigned =
        RoundingDigits::from_typed(&TypedArray::UInt64(UInt64Array::from(vec![0, u64::MAX])))
            .expect("an integer column holds digits");
    assert_eq!(unsigned.lanes, [0, 400]);

    let narrow = RoundingDigits::from_typed(&TypedArray::Int8(Int8Array::from(vec![-128, 127])))
        .expect("an integer column holds digits");
    assert_eq!(narrow.lanes, [-128, 127]);

    assert!(
        RoundingDigits::from_typed(&TypedArray::Float64(Float64Array::from(vec![1.0]))).is_none()
    );
}

#[test]
fn a_null_value_or_digit_count_nulls_the_lane_without_failing_it() {
    let values = Float64Array::from(vec![Some(2.675), None, Some(f64::NAN), Some(1.25)]);
    let digits = RoundingDigits::from_typed(&TypedArray::UInt8(UInt8Array::from(vec![
        Some(2),
        Some(2),
        Some(1),
        None,
    ])))
    .expect("an integer column holds digits");

    let rounded = digits.round_floats(&values);

    assert_eq!(rounded.column.value(0).to_bits(), 2.67_f64.to_bits());
    assert!(rounded.column.is_null(1));
    assert!(rounded.column.is_null(3));
    assert_eq!(rounded.failed.lanes().collect::<Vec<_>>(), [2]);

    let narrow = Float32Array::from(vec![Some(0.125_f32), Some(f32::MAX)]);
    let negative_digits =
        RoundingDigits::from_typed(&TypedArray::Int64(Int64Array::from(vec![2, -38])))
            .expect("an integer column holds digits");
    let rounded = negative_digits.round_floats(&narrow);
    assert_eq!(rounded.column.value(0).to_bits(), 0x3e05_1eb8);
    assert_eq!(rounded.column.value(1).to_bits(), 0x7f61_b1e6);
    assert_eq!(rounded.failed.lanes().count(), 0);

    let integers = Int8Array::from(vec![Some(125), Some(124), None]);
    let tens = RoundingDigits::from_typed(&TypedArray::Int8(Int8Array::from(vec![-1, -1, -1])))
        .expect("an integer column holds digits");
    let rounded = tens.round_integers(&integers);
    assert_eq!(rounded.failed.lanes().collect::<Vec<_>>(), [0]);
    assert!(rounded.column.is_null(0));
    assert_eq!(rounded.column.value(1), 120);
    assert!(rounded.column.is_null(2));
}
