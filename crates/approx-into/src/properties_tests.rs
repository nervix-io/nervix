//! Generated properties of the rounding and the failing conversions.
//!
//! Layer: test harness.
//!
//! - **Owns.** The exact domain on which every conversion is lossless, and a bit-level reference of
//!   IEEE 754 rounding and of truncation that the conversions are compared with everywhere else.
//! - **Depends on.** The conversions of this crate and the standard library's float bits.
//! - **Must not know.** Anything in Nervix, as the crate itself does not.

use meticulous::ResultExt as _;

use crate::{ApproxInto as _, CheckedApproxInto as _};

/// An IEEE 754 binary format, described by the numbers a reference rounding needs.
#[derive(Debug, Clone, Copy)]
struct Format {
    /// Significand bits, the implicit leading bit included.
    precision: u32,
    /// The exponent of the smallest subnormal: every value is a multiple of `2^minimum_quantum`.
    minimum_quantum: i32,
    /// The bias of the encoded exponent.
    bias: i32,
    /// The encoded exponent of the infinities, one above the largest finite one.
    infinite_exponent: u64,
    /// The position of the sign bit.
    sign_bit: u32,
}

const WIDE: Format = Format {
    precision: 53,
    minimum_quantum: -1074,
    bias: 1023,
    infinite_exponent: 2047,
    sign_bit: 63,
};

const NARROW: Format = Format {
    precision: 24,
    minimum_quantum: -149,
    bias: 127,
    infinite_exponent: 255,
    sign_bit: 31,
};

/// An exact value `±magnitude × 2^exponent`.
#[derive(Debug, Clone, Copy)]
struct Exact {
    negative: bool,
    magnitude: u128,
    exponent: i32,
}

impl Exact {
    fn integer(negative: bool, magnitude: u128) -> Self {
        Self {
            negative,
            magnitude,
            exponent: 0,
        }
    }

    fn signed(value: i128) -> Self {
        Self::integer(value < 0, value.unsigned_abs())
    }

    /// The exact value of a finite `F64`, read from its bits.
    fn of_wide(value: f64) -> Self {
        let bits = value.to_bits();
        let biased = i32::try_from((bits >> 52) & 0x7ff).assured("an eleven-bit field fits i32");
        let fraction = u128::from(bits & ((1_u64 << 52) - 1));
        Self::from_fields(bits >> 63 == 1, biased, fraction, WIDE)
    }

    /// The exact value of a finite `F32`, read from its bits.
    fn of_narrow(value: f32) -> Self {
        let bits = value.to_bits();
        let biased = i32::try_from((bits >> 23) & 0xff).assured("an eight-bit field fits i32");
        let fraction = u128::from(bits & ((1_u32 << 23) - 1));
        Self::from_fields(bits >> 31 == 1, biased, fraction, NARROW)
    }

    /// The exact value of an `F64`, or `None` for NaN and the infinities, which name no integer.
    fn of_finite_wide(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        Some(Self::of_wide(value))
    }

    /// The exact value of an `F32`, or `None` for NaN and the infinities, which name no integer.
    fn of_finite_narrow(value: f32) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        Some(Self::of_narrow(value))
    }

    fn from_fields(negative: bool, biased: i32, fraction: u128, format: Format) -> Self {
        let fraction_bits = format.precision - 1;
        if biased == 0 {
            return Self {
                negative,
                magnitude: fraction,
                exponent: format.minimum_quantum,
            };
        }
        let fraction_width = i32::try_from(fraction_bits).assured("a precision fits i32");
        Self {
            negative,
            magnitude: fraction | (1_u128 << fraction_bits),
            exponent: biased - format.bias - fraction_width,
        }
    }

    /// The bits of the value of `format` nearest to this one, halves to the even significand, and
    /// the infinity of its sign past the largest finite value.
    fn nearest(self, format: Format) -> u64 {
        let sign = if self.negative {
            1_u64 << format.sign_bit
        } else {
            0
        };
        if self.magnitude == 0 {
            return sign;
        }
        let precision = i32::try_from(format.precision).assured("a precision fits i32");
        let width = i32::try_from(u128::BITS - self.magnitude.leading_zeros())
            .assured("a bit count fits i32");
        let leading = self.exponent + width - 1;
        let quantum = (leading - (precision - 1)).max(format.minimum_quantum);
        let rounded = self.quantized(quantum);
        let (significand, quantum) = if rounded == 1_u128 << format.precision {
            // Rounding carried into a new leading bit: the next power of two.
            (rounded >> 1, quantum + 1)
        } else {
            (rounded, quantum)
        };
        let implicit = 1_u128 << (format.precision - 1);
        if significand < implicit {
            // A subnormal, or zero after rounding: the quantum is the smallest one.
            let fraction = u64::try_from(significand).assured("a subnormal significand fits u64");
            return sign | fraction;
        }
        let biased = quantum + precision - 1 + format.bias;
        let biased = u64::try_from(biased).assured("a normal value's exponent is positive");
        if biased >= format.infinite_exponent {
            return sign | (format.infinite_exponent << (format.precision - 1));
        }
        let fraction =
            u64::try_from(significand - implicit).assured("a fraction field fits in 52 bits");
        sign | (biased << (format.precision - 1)) | fraction
    }

    /// The magnitude in units of `2^quantum`, rounded to the nearest integer with halves to even.
    fn quantized(self, quantum: i32) -> u128 {
        let shift = quantum - self.exponent;
        if shift <= 0 {
            // The magnitude has no more significant bits than the format keeps.
            return self.magnitude << shift.unsigned_abs();
        }
        let shift = shift.unsigned_abs();
        if shift > u128::BITS {
            // The magnitude is below 2^128, which is at most half of 2^shift.
            return 0;
        }
        let (truncated, remainder) = if shift == u128::BITS {
            (0, self.magnitude)
        } else {
            (
                self.magnitude >> shift,
                self.magnitude & ((1_u128 << shift) - 1),
            )
        };
        let half = 1_u128 << (shift - 1);
        let round_up = remainder > half || (remainder == half && truncated & 1 == 1);
        if round_up { truncated + 1 } else { truncated }
    }

    /// The integer part, truncated toward zero, if it lies within `minimum..=maximum`.
    fn truncated_within(self, minimum: i128, maximum: i128) -> Option<i128> {
        let magnitude = if self.exponent >= 0 {
            let shift = self.exponent.unsigned_abs();
            if shift >= self.magnitude.leading_zeros() {
                // Shifting would lose bits: the integer part is at least 2^128.
                return None;
            }
            self.magnitude << shift
        } else {
            let shift = self.exponent.unsigned_abs();
            if shift >= u128::BITS {
                0
            } else {
                self.magnitude >> shift
            }
        };
        let magnitude = i128::try_from(magnitude).ok()?;
        let value = if self.negative { -magnitude } else { magnitude };
        if value < minimum || value > maximum {
            return None;
        }
        Some(value)
    }
}

/// An integer type the conversions read, built from the low bytes of generated bits.
trait SourceInteger: Copy + std::fmt::Debug {
    fn from_raw(raw: u128) -> Self;

    fn exact(self) -> Exact;
}

macro_rules! source_integer {
    ($($native:ty),+ $(,)?) => {$(
        impl SourceInteger for $native {
            fn from_raw(raw: u128) -> Self {
                let bytes = raw.to_le_bytes();
                let (low, _) = bytes.split_at(size_of::<Self>());
                Self::from_le_bytes(low.try_into().assured("the slice has the type's byte width"))
            }

            fn exact(self) -> Exact {
                Exact::signed(self.try_into().assured("every integer of at most 64 bits fits i128"))
            }
        }
    )+};
}

source_integer!(u32, i32, u64, i64, usize, isize);

/// Converts `$value` into each named float type and compares the bits with the reference rounding
/// of its exact value `$exact` in that type's format.
macro_rules! assert_rounds_to_nearest {
    ($value:expr, $exact:expr => $($float:ty: $format:expr),+ $(,)?) => {{
        let value = $value;
        let exact: Exact = $exact;
        $(
            let converted: $float = value.approx_into();
            assert_eq!(
                u64::from(converted.to_bits()),
                exact.nearest($format),
                "{value:?} as {} gave {converted:e}",
                stringify!($float)
            );
        )+
    }};
}

/// Truncates the float `$value`, whose exact value is `$exact` or `None` when it is NaN or an
/// infinity, into each named integer type and compares the result with the reference truncation.
macro_rules! assert_truncations {
    ($value:expr, $exact:expr => $($native:ty),+ $(,)?) => {{
        let value = $value;
        let exact: Option<Exact> = $exact;
        $(
            let minimum: i128 = <$native>::MIN.try_into().assured("every limit fits i128");
            let maximum: i128 = <$native>::MAX.try_into().assured("every limit fits i128");
            let expected = match exact {
                Some(exact) => exact.truncated_within(minimum, maximum),
                None => None,
            };
            let truncated: Option<$native> = value.checked_approx_into();
            let truncated: Option<i128> = match truncated {
                Some(integer) => Some(integer.try_into().assured("every integer fits i128")),
                None => None,
            };
            assert_eq!(truncated, expected, "{value:e} into {}", stringify!($native));
        )+
    }};
}

/// How a generated case builds the values it converts.
#[derive(Debug, bolero::TypeGenerator)]
enum Conversion {
    /// Any 128-bit pattern, read as an integer of every width the conversions take.
    Integer(u128),
    /// `±(2^power + offset)`: integers beside a power of two, where the spacing of floats doubles
    /// and halves land exactly on ties.
    NearPowerOfTwo {
        power: u8,
        offset: i16,
        negative: bool,
    },
    /// Any `F64` bit pattern, narrowed to `F32` and truncated to every integer type.
    Wide(u64),
    /// `±2^power × (1 + steps × 2^-52)`: floats beside the integer types' limits and the powers of
    /// two where narrowing changes its spacing.
    WideNearPowerOfTwo {
        power: i16,
        steps: i8,
        negative: bool,
    },
    /// Any `F32` bit pattern, truncated to every integer type.
    Narrow(u32),
}

impl Conversion {
    /// The bits of the integer this case converts into floats, which every width reads.
    fn integer_bits(&self) -> Option<u128> {
        match self {
            Self::Integer(raw) => Some(*raw),
            Self::NearPowerOfTwo {
                power,
                offset,
                negative,
            } => {
                let base = 1_i128 << (power % 127);
                let value = base + i128::from(*offset);
                let value = if *negative { -value } else { value };
                Some(value.cast_unsigned())
            }
            Self::Wide(_) | Self::WideNearPowerOfTwo { .. } | Self::Narrow(_) => None,
        }
    }

    /// The `F64` this case narrows and truncates.
    fn wide(&self) -> Option<f64> {
        match self {
            Self::Wide(bits) => Some(f64::from_bits(*bits)),
            Self::WideNearPowerOfTwo {
                power,
                steps,
                negative,
            } => {
                let unit = 2_f64.powi(i32::from(*power % 1024));
                let magnitude = unit + f64::from(*steps) * (f64::EPSILON * unit);
                Some(if *negative { -magnitude } else { magnitude })
            }
            Self::Integer(_) | Self::NearPowerOfTwo { .. } | Self::Narrow(_) => None,
        }
    }

    /// The `F32` this case truncates.
    fn narrow(&self) -> Option<f32> {
        match self {
            Self::Narrow(bits) => Some(f32::from_bits(*bits)),
            Self::Integer(_)
            | Self::NearPowerOfTwo { .. }
            | Self::Wide(_)
            | Self::WideNearPowerOfTwo { .. } => None,
        }
    }
}

/// Narrows an `F64` and compares the result with the reference rounding into `F32`: NaN stays NaN
/// and an infinity keeps its sign.
fn assert_narrowing(wide: f64) {
    let narrowed: f32 = wide.approx_into();
    if wide.is_nan() {
        assert!(narrowed.is_nan(), "NaN narrowed to {narrowed:e}");
        return;
    }
    if wide.is_infinite() {
        assert!(narrowed.is_infinite(), "{wide:e} narrowed to {narrowed:e}");
        assert_eq!(narrowed.is_sign_negative(), wide.is_sign_negative());
        return;
    }
    assert_eq!(
        u64::from(narrowed.to_bits()),
        Exact::of_wide(wide).nearest(NARROW),
        "{wide:e} narrowed to {narrowed:e}"
    );
}

#[test]
fn bolero_conversions_round_to_the_nearest_value_or_name_no_integer() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .with_type::<Conversion>()
        .for_each(|conversion| {
            if let Some(raw) = conversion.integer_bits() {
                let unsigned = u32::from_raw(raw);
                assert_rounds_to_nearest!(unsigned, unsigned.exact() => f32: NARROW);
                let signed = i32::from_raw(raw);
                assert_rounds_to_nearest!(signed, signed.exact() => f32: NARROW);
                let unsigned = u64::from_raw(raw);
                assert_rounds_to_nearest!(unsigned, unsigned.exact() => f64: WIDE, f32: NARROW);
                let signed = i64::from_raw(raw);
                assert_rounds_to_nearest!(signed, signed.exact() => f64: WIDE, f32: NARROW);
                let unsigned = usize::from_raw(raw);
                assert_rounds_to_nearest!(unsigned, unsigned.exact() => f64: WIDE, f32: NARROW);
                let signed = isize::from_raw(raw);
                assert_rounds_to_nearest!(signed, signed.exact() => f64: WIDE, f32: NARROW);
                assert_rounds_to_nearest!(
                    raw,
                    Exact::integer(false, raw) => f64: WIDE, f32: NARROW
                );
                let signed = raw.cast_signed();
                assert_rounds_to_nearest!(signed, Exact::signed(signed) => f64: WIDE, f32: NARROW);
            }
            if let Some(wide) = conversion.wide() {
                assert_narrowing(wide);
                assert_truncations!(
                    wide,
                    Exact::of_finite_wide(wide) => u8, u16, u32, u64, usize, i8, i16, i32, i64, isize
                );
            }
            if let Some(narrow) = conversion.narrow() {
                assert_truncations!(
                    narrow,
                    Exact::of_finite_narrow(narrow) => u8, u16, u32, u64, usize, i8, i16, i32, i64, isize
                );
            }
        });
}

/// A value inside the domain on which every conversion is exact.
#[derive(Debug, bolero::TypeGenerator)]
enum Representable {
    /// `±significand × 2^shift`, whose significant bits fit the float type the case converts it
    /// into, within the range of every integer type that holds it.
    Integer {
        significand: u64,
        shift: u8,
        negative: bool,
    },
    /// Any `F32` bit pattern but NaN, which widens to `F64` and narrows back unchanged.
    Narrow(u32),
}

/// Converts an integer of type `N` into a float and back, which loses nothing when its significant
/// bits fit the float's significand.
fn assert_round_trip<N>(value: i128)
where
    N: TryFrom<i128> + Copy + std::fmt::Debug + PartialEq + crate::ApproxInto,
    f64: crate::ApproxFrom<N>,
    N: crate::CheckedApproxFrom<f64>,
{
    let Ok(integer) = N::try_from(value) else {
        return;
    };
    let wide: f64 = integer.approx_into();
    assert_eq!(wide.fract(), 0.0, "{integer:?} as F64 has no fraction");
    assert_eq!(
        wide.checked_approx_into::<N>(),
        Some(integer),
        "{integer:?} through F64"
    );
}

/// The narrow counterpart of [`assert_round_trip`], for significands of at most 24 bits.
fn assert_narrow_round_trip<N>(value: i128)
where
    N: TryFrom<i128> + Copy + std::fmt::Debug + PartialEq + crate::ApproxInto,
    f32: crate::ApproxFrom<N>,
    N: crate::CheckedApproxFrom<f32>,
{
    let Ok(integer) = N::try_from(value) else {
        return;
    };
    let narrow: f32 = integer.approx_into();
    assert_eq!(
        narrow.checked_approx_into::<N>(),
        Some(integer),
        "{integer:?} through F32"
    );
}

#[test]
fn bolero_exactly_representable_values_convert_without_loss() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(32)
        .with_type::<Representable>()
        .for_each(|representable| match representable {
            Representable::Integer {
                significand,
                shift,
                negative,
            } => {
                let wide_significand = significand & ((1_u64 << f64::MANTISSA_DIGITS) - 1);
                let shift = u32::from(shift % 72);
                let magnitude = i128::from(wide_significand) << shift;
                let value = if *negative { -magnitude } else { magnitude };
                assert_round_trip::<u64>(value);
                assert_round_trip::<i64>(value);
                assert_round_trip::<usize>(value);
                assert_round_trip::<isize>(value);
                let narrow_significand = significand & ((1_u64 << f32::MANTISSA_DIGITS) - 1);
                let magnitude = i128::from(narrow_significand) << shift;
                let value = if *negative { -magnitude } else { magnitude };
                assert_narrow_round_trip::<u32>(value);
                assert_narrow_round_trip::<i32>(value);
                assert_narrow_round_trip::<u64>(value);
                assert_narrow_round_trip::<i64>(value);
            }
            Representable::Narrow(bits) => {
                let narrow = f32::from_bits(*bits);
                if narrow.is_nan() {
                    return;
                }
                let narrowed: f32 = f64::from(narrow).approx_into();
                assert_eq!(narrowed.to_bits(), narrow.to_bits(), "{narrow:e}");
            }
        });
}
