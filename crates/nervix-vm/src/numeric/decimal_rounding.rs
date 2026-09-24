//! Rounding to a number of decimal digits over Arrow buffers.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The batch kernels behind `round(value, digits)`: exact decimal rounding of a
//!   floating-point value, and rounding of an integer to a multiple of a power of ten, which fails
//!   when that multiple does not fit the integer's type.
//! - **Depends on.** Arrow arrays and buffers, the checked lane loop of the numeric kernels, and the
//!   side-error reasons of the VM.
//! - **Must not know.** Registers, programs, spans, or how a failed lane is recorded as a row
//!   error.
//!
//! A float is rounded exactly. The result is the value of the float's own type nearest to the
//! float's binary value rounded to `digits` decimal places, with halves rounded away from zero, so
//! it does not depend on the platform. Exactness takes integer arithmetic on the float's significand
//! rather than a scaled floating-point product, which would round the scaled value before it is
//! rounded to an integer. A lane is therefore a scalar computation that no loop vectorizes, and the
//! kernels skip null lanes.

use std::{fmt::Display, str::FromStr};

use arrow_array::{Array, ArrowPrimitiveType, PrimitiveArray};
use arrow_buffer::{ArrowNativeType, NullBuffer};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto as _;

use super::{Checked, CheckedFloat, Lanes};
use crate::{batch::TypedArray, semantics::ClassifiedFloat};

/// The bound digits are held within. With more places than this, every `F64` and `F32` value is
/// already its own rounding, because no value has a unit in the last place below `2^-1074`, which
/// is above `10^-400`. With a power of ten above `10^400`, every value rounds to zero, because half
/// of it exceeds every `F64` and every integer. Every count beyond the bound therefore rounds as the
/// bound does.
const DIGITS_BOUND: i16 = 400;

/// Exponents `k` for which `10^k`, and so `5^k`, fits the 128-bit integer tables below.
const INTEGER_POWER_COUNT: usize = 23;

const POWERS_OF_FIVE: [u128; INTEGER_POWER_COUNT] = integer_powers(5);

const POWERS_OF_TEN: [u128; INTEGER_POWER_COUNT] = integer_powers(10);

const fn integer_powers(base: u128) -> [u128; INTEGER_POWER_COUNT] {
    let mut powers = [1_u128; INTEGER_POWER_COUNT];
    let mut exponent = 1;
    while exponent < INTEGER_POWER_COUNT {
        powers[exponent] = powers[exponent - 1] * base;
        exponent += 1;
    }
    powers
}

/// `floor(k × log2(10))`, the exponent of the largest power of two at most `10^k`, for
/// `0 <= k <= DIGITS_BOUND`. It is `k + floor(k × log2(5))`, and `(k × 1217359) >> 19` equals
/// `floor(k × log2(5))` for every `k` up to 3528.
fn binary_exponent_of_power_of_ten(k: i32) -> i32 {
    let scaled = k
        .checked_mul(1_217_359)
        .assured("k is at most DIGITS_BOUND, so the product is below 2^29");
    k.checked_add(scaled >> 19)
        .assured("k is at most DIGITS_BOUND, so the sum is below 2^11")
}

/// The digits operand of `round(value, digits)`: one count per lane, read from a column of any
/// integer type and held within [`DIGITS_BOUND`], with that column's validity.
pub(crate) struct RoundingDigits {
    lanes: Vec<i16>,
    nulls: Option<NullBuffer>,
}

/// An integer type a digit count is read from.
trait DigitsOperand: ArrowNativeType {
    fn bounded_digits(self) -> i16;
}

macro_rules! narrow_digits_operand {
    ($($native:ty),+ $(,)?) => {
        $(
            impl DigitsOperand for $native {
                fn bounded_digits(self) -> i16 {
                    i16::from(self).clamp(-DIGITS_BOUND, DIGITS_BOUND)
                }
            }
        )+
    };
}

narrow_digits_operand!(u8, i8);

macro_rules! signed_digits_operand {
    ($($native:ty),+ $(,)?) => {
        $(
            impl DigitsOperand for $native {
                fn bounded_digits(self) -> i16 {
                    match i16::try_from(self) {
                        Ok(digits) => digits.clamp(-DIGITS_BOUND, DIGITS_BOUND),
                        Err(_) => {
                            if self < 0 {
                                -DIGITS_BOUND
                            } else {
                                DIGITS_BOUND
                            }
                        }
                    }
                }
            }
        )+
    };
}

signed_digits_operand!(i16, i32, i64);

macro_rules! unsigned_digits_operand {
    ($($native:ty),+ $(,)?) => {
        $(
            impl DigitsOperand for $native {
                fn bounded_digits(self) -> i16 {
                    match i16::try_from(self) {
                        Ok(digits) => digits.min(DIGITS_BOUND),
                        Err(_) => DIGITS_BOUND,
                    }
                }
            }
        )+
    };
}

unsigned_digits_operand!(u16, u32, u64);

impl RoundingDigits {
    /// Reads the digit counts of an integer column, or answers `None` for a column that is not
    /// integral.
    pub(crate) fn from_typed(input: &TypedArray) -> Option<Self> {
        match input {
            TypedArray::UInt8(array) => Some(Self::new(array)),
            TypedArray::Int8(array) => Some(Self::new(array)),
            TypedArray::UInt16(array) => Some(Self::new(array)),
            TypedArray::Int16(array) => Some(Self::new(array)),
            TypedArray::UInt32(array) => Some(Self::new(array)),
            TypedArray::Int32(array) => Some(Self::new(array)),
            TypedArray::UInt64(array) => Some(Self::new(array)),
            TypedArray::Int64(array) => Some(Self::new(array)),
            TypedArray::Float32(_)
            | TypedArray::Float64(_)
            | TypedArray::Boolean(_)
            | TypedArray::Utf8(_)
            | TypedArray::Binary(_)
            | TypedArray::Datetime(_)
            | TypedArray::Generic(_)
            | TypedArray::Uninitialized { .. } => None,
        }
    }

    fn new<T>(input: &PrimitiveArray<T>) -> Self
    where
        T: ArrowPrimitiveType,
        T::Native: DigitsOperand,
    {
        Self {
            lanes: input
                .values()
                .iter()
                .map(|digits| digits.bounded_digits())
                .collect(),
            nulls: input.nulls().cloned(),
        }
    }

    /// Rounds every lane of a float column to the digits in the same lane. A lane fails when its
    /// value is NaN or an infinity, or when rounding a value to a multiple of a power of ten
    /// overflows the type.
    pub(crate) fn round_floats<T>(&self, values: &PrimitiveArray<T>) -> Checked<T>
    where
        T: ArrowPrimitiveType,
        T::Native: DecimalRounding,
    {
        let nulls = NullBuffer::union(values.nulls(), self.nulls.as_ref());
        let value_lanes: &[T::Native] = values.values();
        let lanes = match &nulls {
            Some(valid) => Lanes::binary_valid(
                value_lanes,
                &self.lanes,
                valid,
                |value: T::Native, digits| value.rounded_to_digits(digits).finite_lane(),
            ),
            None => Lanes::binary(value_lanes, &self.lanes, |value: T::Native, digits| {
                value.rounded_to_digits(digits).finite_lane()
            }),
        };
        Checked::from_lanes(lanes, nulls)
    }

    /// Rounds every lane of an integer column to the digits in the same lane. A lane fails when
    /// the multiple of a power of ten it rounds to does not fit the type.
    pub(crate) fn round_integers<T>(&self, values: &PrimitiveArray<T>) -> Checked<T>
    where
        T: ArrowPrimitiveType,
        T::Native: IntegerRounding,
    {
        let nulls = NullBuffer::union(values.nulls(), self.nulls.as_ref());
        let value_lanes: &[T::Native] = values.values();
        let lanes = match &nulls {
            Some(valid) => Lanes::binary_valid(
                value_lanes,
                &self.lanes,
                valid,
                T::Native::lane_rounded_to_digits,
            ),
            None => Lanes::binary(value_lanes, &self.lanes, T::Native::lane_rounded_to_digits),
        };
        Checked::from_lanes(lanes, nulls)
    }
}

/// An integer type `round(value, digits)` rounds to a multiple of a power of ten.
pub(crate) trait IntegerRounding:
    ArrowNativeType + Default + Into<i128> + TryFrom<i128>
{
    /// The multiple of `10^-digits` nearest to the value, with halves rounded away from zero. An
    /// integer has no fractional digits, so a count of zero or more returns it unchanged. Fails
    /// when the multiple does not fit the type.
    fn lane_rounded_to_digits(self, digits: i16) -> (Self, bool) {
        if digits >= 0 {
            return (self, false);
        }
        let exponent_of_ten = usize::from(digits.unsigned_abs());
        // Every supported integer is below 2^64 in magnitude, which is below half of 10^20, so a
        // power of ten above 10^19 rounds every value to zero.
        if exponent_of_ten > 19 {
            return (Self::default(), false);
        }
        let power = POWERS_OF_TEN[exponent_of_ten];
        let value: i128 = self.into();
        let magnitude = value.unsigned_abs();
        let remainder = magnitude
            .checked_rem(power)
            .assured("every power of ten is nonzero");
        let truncated = magnitude
            .checked_sub(remainder)
            .assured("a remainder is at most the value it was taken from");
        let distance_to_next = power
            .checked_sub(remainder)
            .assured("a remainder is below its divisor");
        // Half away from zero: the magnitude moves up when its remainder is at least as far from
        // the multiple below as the multiple above is from it.
        let rounded = if remainder >= distance_to_next {
            truncated.checked_add(power).assured(
                "the magnitude is below 2^64 and the power at most 10^19, so the sum is below 2^65",
            )
        } else {
            truncated
        };
        let rounded = i128::try_from(rounded)
            .assured("the rounded magnitude is below 2^65, which fits an i128");
        let signed = if value < 0 {
            rounded
                .checked_neg()
                .assured("the rounded magnitude is below 2^65, so its negation fits an i128")
        } else {
            rounded
        };
        match Self::try_from(signed) {
            Ok(rounded) => (rounded, false),
            Err(_) => (Self::default(), true),
        }
    }
}

impl IntegerRounding for u8 {}
impl IntegerRounding for i8 {}
impl IntegerRounding for u16 {}
impl IntegerRounding for i16 {}
impl IntegerRounding for u32 {}
impl IntegerRounding for i32 {}
impl IntegerRounding for u64 {}
impl IntegerRounding for i64 {}

/// A finite nonzero float as `±significand × 2^exponent`, where `2^exponent` is the float's unit
/// in the last place.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BinaryParts {
    negative: bool,
    significand: u64,
    exponent: i32,
}

/// How far integer arithmetic on the significand takes a rounding.
enum SignificandRounding<F> {
    /// The value is already its own rounding.
    Unchanged,
    /// The magnitude of the rounded value.
    Magnitude(F),
    /// The power of ten has no exact representation in the float's type, so only the value's
    /// decimal expansion rounds it exactly.
    NeedsDecimalExpansion,
}

/// A floating-point type `round(value, digits)` rounds exactly.
pub(crate) trait DecimalRounding:
    CheckedFloat + ClassifiedFloat + Display + FromStr
{
    /// The bits of the significand of every normal value.
    const SIGNIFICAND_BITS: i32;

    /// `10^k` for every `k` whose power of ten the type represents exactly.
    const EXACT_POWERS_OF_TEN: &'static [Self];

    /// The value's sign, significand and exponent. The value must be finite.
    fn binary_parts(self) -> BinaryParts;

    /// The integer `significand`, which is at most `2^SIGNIFICAND_BITS` and so exact.
    fn from_significand(significand: u64) -> Self;

    /// The float nearest to the decimal number `digits × 10^exponent`, where `digits` is a string of
    /// ASCII digits.
    fn from_decimal(digits: &str, exponent: i16) -> Self;

    /// The value rounded to `digits` decimal places, or for a negative count to a multiple of
    /// `10^-digits`, with halves rounded away from zero. NaN and the infinities are returned
    /// unchanged, and a zero keeps its sign.
    fn rounded_to_digits(self, digits: i16) -> Self {
        if !self.is_finite_value() || self == Self::default() {
            return self;
        }
        let parts = self.binary_parts();
        let rounding = if digits >= 0 {
            parts.rounded_to_places::<Self>(digits)
        } else {
            parts.rounded_to_power_of_ten::<Self>(digits.unsigned_abs())
        };
        let magnitude = match rounding {
            SignificandRounding::Unchanged => return self,
            SignificandRounding::Magnitude(magnitude) => magnitude,
            SignificandRounding::NeedsDecimalExpansion => {
                return self.rounded_by_decimal_expansion(parts, digits);
            }
        };
        if parts.negative {
            -magnitude
        } else {
            magnitude
        }
    }

    /// Rounds through the value's exact decimal expansion. Every finite float has a finite decimal
    /// expansion, and a rounding decision needs only its digits: the magnitude rounds up exactly
    /// when the first digit it drops is 5 or more.
    fn rounded_by_decimal_expansion(self, parts: BinaryParts, digits: i16) -> Self {
        let lowest_set_bit = parts
            .exponent
            .checked_add_unsigned(parts.significand.trailing_zeros())
            .assured("a float's exponent and its significand's trailing zeros both fit an i32");
        // A lowest set bit of 2^-n gives exactly n fractional decimal digits, so formatting that
        // many writes the exact expansion without rounding any digit.
        let fraction_digits = if lowest_set_bit < 0 {
            usize::try_from(lowest_set_bit.unsigned_abs())
                .assured("a float has at most 1074 fractional binary digits")
        } else {
            0
        };
        let magnitude = if parts.negative { -self } else { self };
        let expansion = format!("{magnitude:.fraction_digits$}");
        let (integer_digits, fraction) = match expansion.split_once('.') {
            Some((integer_digits, fraction)) => (integer_digits, fraction),
            None => (expansion.as_str(), ""),
        };
        let mut kept = Vec::with_capacity(expansion.len());
        kept.extend_from_slice(integer_digits.as_bytes());
        kept.extend_from_slice(fraction.as_bytes());
        let integer_length = i32::try_from(integer_digits.len())
            .assured("a float's integer part has at most 309 digits");
        let kept_length = integer_length
            .checked_add(i32::from(digits))
            .assured("both the integer length and the digits are bounded well below 2^31");
        let round_up = match usize::try_from(kept_length) {
            Ok(kept_length) if kept_length < kept.len() => {
                let first_dropped = kept[kept_length];
                kept.truncate(kept_length);
                first_dropped >= b'5'
            }
            Ok(_) => return self,
            // Every digit lies after the rounding position, so the magnitude is below half of the
            // smallest multiple it could round to.
            Err(_) => {
                kept.clear();
                false
            }
        };
        if round_up {
            increment_decimal_digits(&mut kept);
        }
        if kept.is_empty() {
            kept.push(b'0');
        }
        let kept = std::str::from_utf8(&kept).assured("the expansion holds only ASCII digits");
        let negative_digits = digits
            .checked_neg()
            .assured("digits are within DIGITS_BOUND, so their negation fits an i16");
        let rounded = Self::from_decimal(kept, negative_digits);
        if parts.negative { -rounded } else { rounded }
    }
}

/// Adds one to a string of ASCII digits, growing it by a leading `1` when every digit carries.
fn increment_decimal_digits(digits: &mut Vec<u8>) {
    for digit in digits.iter_mut().rev() {
        if *digit == b'9' {
            *digit = b'0';
        } else {
            *digit += 1;
            return;
        }
    }
    digits.insert(0, b'1');
}

impl BinaryParts {
    /// Rounds to `places` decimal places, `places >= 0`.
    fn rounded_to_places<F: DecimalRounding>(self, places: i16) -> SignificandRounding<F> {
        let places = i32::from(places);
        // When the unit in the last place exceeds 10^-places, the rounded decimal lies closer to
        // the value than half that unit, so the value itself is the nearest float.
        if self.exponent >= -binary_exponent_of_power_of_ten(places) {
            return SignificandRounding::Unchanged;
        }
        let places_index = usize::try_from(places).assured("places is nonnegative");
        let Some(power) = F::EXACT_POWERS_OF_TEN.get(places_index) else {
            return SignificandRounding::NeedsDecimalExpansion;
        };
        // The value times 10^places is significand × 5^places × 2^(exponent + places), and the
        // check above made exponent + places negative, so the product is divided by 2^shift.
        let exponent_after_scaling = self
            .exponent
            .checked_add(places)
            .assured("the exponent is at least -1074 and places at most 22");
        let shift = exponent_after_scaling.unsigned_abs();
        let scaled = u128::from(self.significand)
            .checked_mul(POWERS_OF_FIVE[places_index])
            .assured("a significand below 2^53 times 5^22, below 2^52, is below 2^105");
        let rounded = if shift > 105 {
            // The scaled value is below 2^105, so it is below half of 2^shift.
            0
        } else {
            let half_shift = shift
                .checked_sub(1)
                .assured("exponent + places is negative, so the shift is at least 1");
            let half = 1_u128
                .checked_shl(half_shift)
                .assured("the shift is at most 105");
            let raised = scaled
                .checked_add(half)
                .assured("the scaled value and half are both below 2^105");
            raised
                .checked_shr(shift)
                .assured("the shift is at most 105")
        };
        let rounded = u64::try_from(rounded).assured(
            "the value is below 2^(exponent + SIGNIFICAND_BITS) and 10^places below 2^-exponent, \
             so the rounded integer is at most 2^SIGNIFICAND_BITS",
        );
        SignificandRounding::Magnitude(F::from_significand(rounded) / *power)
    }

    /// Rounds to a multiple of `10^exponent_of_ten`, `exponent_of_ten >= 1`.
    fn rounded_to_power_of_ten<F: DecimalRounding>(
        self,
        exponent_of_ten: u16,
    ) -> SignificandRounding<F> {
        let exponent_of_ten = i32::from(exponent_of_ten);
        let binary_exponent = binary_exponent_of_power_of_ten(exponent_of_ten);
        // A unit in the last place above 10^exponent_of_ten keeps the rounded value within half a
        // unit of the value, so the value is its own rounding.
        if self.exponent > binary_exponent {
            return SignificandRounding::Unchanged;
        }
        // The magnitude is below 2^(exponent + SIGNIFICAND_BITS), and when that is at most
        // 2^(binary_exponent - 1) it is below half of 10^exponent_of_ten.
        let magnitude_bits = self
            .exponent
            .checked_add(F::SIGNIFICAND_BITS)
            .assured("a float's exponent is far inside the i32 range");
        if magnitude_bits < binary_exponent {
            return SignificandRounding::Magnitude(F::default());
        }
        let power_index = usize::try_from(exponent_of_ten).assured("exponent_of_ten is positive");
        let Some(power) = F::EXACT_POWERS_OF_TEN.get(power_index) else {
            return SignificandRounding::NeedsDecimalExpansion;
        };
        let integer_power = POWERS_OF_TEN[power_index];
        // The exponent lies between binary_exponent - SIGNIFICAND_BITS and binary_exponent, at most
        // 73, so both sides of the division stay below 2^127.
        let (numerator, denominator) = if self.exponent >= 0 {
            let numerator = u128::from(self.significand)
                .checked_shl(self.exponent.unsigned_abs())
                .assured("the exponent is at most 73");
            (numerator, integer_power)
        } else {
            let denominator = integer_power
                .checked_shl(self.exponent.unsigned_abs())
                .assured("the exponent is at least -50");
            (u128::from(self.significand), denominator)
        };
        let doubled_numerator = numerator
            .checked_mul(2)
            .assured("the numerator is below 2^126");
        let doubled_denominator = denominator
            .checked_mul(2)
            .assured("the denominator is below 2^124");
        // floor(numerator / denominator + 1/2): the quotient with halves rounded up.
        let raised = doubled_numerator
            .checked_add(denominator)
            .assured("both terms are below 2^127");
        let rounded = raised
            .checked_div(doubled_denominator)
            .assured("the denominator is a nonzero power of ten");
        let rounded = u64::try_from(rounded).assured(
            "the value is below 2^(binary_exponent + SIGNIFICAND_BITS), and 10^exponent_of_ten is \
             above 2^binary_exponent, so the rounded integer is at most 2^SIGNIFICAND_BITS",
        );
        SignificandRounding::Magnitude(F::from_significand(rounded) * *power)
    }
}

impl DecimalRounding for f64 {
    const SIGNIFICAND_BITS: i32 = 53;

    const EXACT_POWERS_OF_TEN: &'static [Self] = &[
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
        1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
    ];

    fn binary_parts(self) -> BinaryParts {
        let bits = self.to_bits();
        let negative = bits >> 63 == 1;
        let biased_exponent =
            i32::try_from((bits >> 52) & 0x7ff).assured("an eleven-bit field fits an i32");
        let fraction = bits & ((1_u64 << 52) - 1);
        if biased_exponent == 0 {
            BinaryParts {
                negative,
                significand: fraction,
                exponent: -1074,
            }
        } else {
            BinaryParts {
                negative,
                significand: fraction | (1_u64 << 52),
                exponent: biased_exponent
                    .checked_sub(1075)
                    .assured("an eleven-bit field minus its bias fits an i32"),
            }
        }
    }

    fn from_significand(significand: u64) -> Self {
        significand.approx_into()
    }

    fn from_decimal(digits: &str, exponent: i16) -> Self {
        format!("{digits}e{exponent}")
            .parse()
            .assured("ASCII digits followed by an exponent are valid float syntax")
    }
}

impl DecimalRounding for f32 {
    const SIGNIFICAND_BITS: i32 = 24;

    const EXACT_POWERS_OF_TEN: &'static [Self] =
        &[1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10];

    fn binary_parts(self) -> BinaryParts {
        let bits = self.to_bits();
        let negative = bits >> 31 == 1;
        let biased_exponent =
            i32::try_from((bits >> 23) & 0xff).assured("an eight-bit field fits an i32");
        let fraction = u64::from(bits & ((1_u32 << 23) - 1));
        if biased_exponent == 0 {
            BinaryParts {
                negative,
                significand: fraction,
                exponent: -149,
            }
        } else {
            BinaryParts {
                negative,
                significand: fraction | (1_u64 << 23),
                exponent: biased_exponent
                    .checked_sub(150)
                    .assured("an eight-bit field minus its bias fits an i32"),
            }
        }
    }

    fn from_significand(significand: u64) -> Self {
        significand.approx_into()
    }

    fn from_decimal(digits: &str, exponent: i16) -> Self {
        format!("{digits}e{exponent}")
            .parse()
            .assured("ASCII digits followed by an exponent are valid float syntax")
    }
}

#[cfg(test)]
#[path = "decimal_rounding_tests.rs"]
mod tests;
