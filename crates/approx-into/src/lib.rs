//! Conversions between exact integers and floating point, for the pairs `From` and `TryFrom`
//! cannot express.
//!
//! Nervix converts between integers and floating point at three boundaries: NSPL numeric builtins
//! evaluate in `f64`, metric protocols carry every sample as `f64`, and layout arithmetic works in
//! `f64` before it addresses a bucket, a rank, or a pixel. The standard library covers the pairs
//! that are exact — `f64::from(u32)`, `f32::from(u16)` — and stops there, because the rest either
//! round or fail. Those are the pairs this crate names.
//!
//! [`ApproxInto`] is the rounding direction and is total: every integer has a nearest
//! representable float, and above the mantissa width the float keeps only the leading bits.
//! [`TryApproxInto`] is the failing direction: a float is not an integer until it is finite and
//! inside the target's range, so the conversion returns [`FromFloatError`] instead of saturating
//! the way `as` does.
//!
//! Use `From` and `TryFrom` wherever they apply. This crate deliberately implements only the pairs
//! they leave out, so reaching for `approx_into` is itself a statement that the conversion rounds.

use thiserror::Error;

/// A conversion into floating point that rounds to the nearest representable value.
pub trait ApproxFrom<T>: Sized {
    /// Converts from `T`, rounding to the nearest representable value.
    fn approx_from(value: T) -> Self;
}

/// A conversion from floating point into an integer, which fails unless the value is an integer
/// the target can hold.
pub trait TryApproxFrom<T>: Sized {
    /// Converts from `T`, discarding any fractional part.
    ///
    /// # Errors
    ///
    /// Returns [`FromFloatError`] when the value is not finite, or when its integer part is
    /// outside the range of `Self`.
    fn try_approx_from(value: T) -> Result<Self, FromFloatError>;
}

/// The calling side of [`ApproxFrom`], so a conversion can end a method chain.
pub trait ApproxInto: Sized {
    /// Converts into `T`, rounding to the nearest representable value.
    fn approx_into<T: ApproxFrom<Self>>(self) -> T;
}

/// The calling side of [`TryApproxFrom`], so a conversion can end a method chain.
pub trait TryApproxInto: Sized {
    /// Converts into `T`, discarding any fractional part.
    ///
    /// # Errors
    ///
    /// Returns [`FromFloatError`] when the value is not finite, or when its integer part is
    /// outside the range of `T`.
    fn try_approx_into<T: TryApproxFrom<Self>>(self) -> Result<T, FromFloatError>;
}

/// Why a floating point value has no value in the target integer type.
#[derive(Debug, Clone, Copy, PartialEq, Error)]
pub enum FromFloatError {
    /// The value is `NaN` or an infinity, so it names no integer at all.
    #[error("{value} is not a finite number, so it has no {target} value")]
    NotFinite {
        /// The rejected value.
        value: f64,
        /// The integer type the conversion targeted.
        target: &'static str,
    },
    /// The value is finite, but its integer part lies outside the target's range.
    #[error("{value} is outside the range of {target}")]
    OutOfRange {
        /// The rejected value.
        value: f64,
        /// The integer type the conversion targeted.
        target: &'static str,
    },
}

impl<T: Sized> ApproxInto for T {
    #[inline]
    fn approx_into<U: ApproxFrom<Self>>(self) -> U {
        U::approx_from(self)
    }
}

impl<T: Sized> TryApproxInto for T {
    #[inline]
    fn try_approx_into<U: TryApproxFrom<Self>>(self) -> Result<U, FromFloatError> {
        U::try_approx_from(self)
    }
}

/// Implements the rounding direction for one integer-to-float pair.
///
/// The cast is the operation itself: rounding to the nearest representable float is what the
/// caller asked for by naming `approx_into`, and it is the only way to spell it.
macro_rules! approx_from_integer {
    ($float:ty, $($integer:ty),+ $(,)?) => {
        $(
            impl ApproxFrom<$integer> for $float {
                #[inline]
                #[expect(
                    clippy::as_conversions,
                    reason = "rounding to the nearest representable float is the operation this \
                              crate exists to name"
                )]
                fn approx_from(value: $integer) -> Self {
                    value as Self
                }
            }
        )+
    };
}

approx_from_integer!(f64, u64, i64, u128, i128, usize, isize);
approx_from_integer!(f32, u32, i32, u64, i64, u128, i128, usize, isize);

impl ApproxFrom<f64> for f32 {
    /// Rounds to the nearest `f32`, which is an infinity when the magnitude exceeds `f32::MAX`.
    #[inline]
    #[expect(
        clippy::as_conversions,
        reason = "narrowing to the nearest representable float is the operation this crate exists \
                  to name"
    )]
    fn approx_from(value: f64) -> Self {
        value as Self
    }
}

/// Implements the failing direction for one float-to-integer pair.
///
/// The bounds are written as exact powers of two so the comparison itself never rounds: an
/// integer type's exclusive limit is `2^bits` unsigned and `2^(bits - 1)` signed, and every one of
/// those is representable in `f64`. `i64::MAX as f64` would not be, which is why the limit is
/// stated instead of derived.
macro_rules! try_approx_from_float {
    ($($integer:ty => $minimum:expr, $limit:expr;)+) => {
        $(
            impl TryApproxFrom<f64> for $integer {
                #[inline]
                #[expect(
                    clippy::as_conversions,
                    reason = "the bounds above have already excluded every value the cast would \
                              saturate or round"
                )]
                fn try_approx_from(value: f64) -> Result<Self, FromFloatError> {
                    if !value.is_finite() {
                        return Err(FromFloatError::NotFinite {
                            value,
                            target: stringify!($integer),
                        });
                    }
                    let integral = value.trunc();
                    if integral < $minimum || integral >= $limit {
                        return Err(FromFloatError::OutOfRange {
                            value,
                            target: stringify!($integer),
                        });
                    }
                    Ok(integral as Self)
                }
            }

            impl TryApproxFrom<f32> for $integer {
                #[inline]
                fn try_approx_from(value: f32) -> Result<Self, FromFloatError> {
                    Self::try_approx_from(f64::from(value))
                }
            }
        )+
    };
}

try_approx_from_float! {
    u8 => 0.0, 256.0;
    u16 => 0.0, 65_536.0;
    u32 => 0.0, 4_294_967_296.0;
    u64 => 0.0, 18_446_744_073_709_551_616.0;
    i8 => -128.0, 128.0;
    i16 => -32_768.0, 32_768.0;
    i32 => -2_147_483_648.0, 2_147_483_648.0;
    i64 => -9_223_372_036_854_775_808.0, 9_223_372_036_854_775_808.0;
}

/// Implements the failing direction for a pointer-width integer through its fixed-width twin.
///
/// A 64-bit host makes the second step total and a 32-bit host makes it the range check the
/// caller already asked for, so neither has to be special-cased here.
macro_rules! try_approx_from_float_pointer_width {
    ($($integer:ty => $fixed:ty;)+) => {
        $(
            impl<F> TryApproxFrom<F> for $integer
            where
                F: Copy,
                $fixed: TryApproxFrom<F>,
                f64: ApproxFrom<$fixed>,
            {
                #[inline]
                fn try_approx_from(value: F) -> Result<Self, FromFloatError> {
                    let fixed = <$fixed>::try_approx_from(value)?;
                    Self::try_from(fixed).map_err(|_| FromFloatError::OutOfRange {
                        value: f64::approx_from(fixed),
                        target: stringify!($integer),
                    })
                }
            }
        )+
    };
}

try_approx_from_float_pointer_width! {
    usize => u64;
    isize => i64;
}

#[cfg(test)]
mod tests {
    use super::{ApproxInto as _, FromFloatError, TryApproxInto as _};

    #[test]
    fn integers_inside_the_mantissa_convert_exactly() {
        let exact: f64 = (1_u64 << 53).approx_into();
        assert_eq!(exact, 9_007_199_254_740_992.0);

        let negative: f64 = i64::MIN.approx_into();
        assert_eq!(negative, -9_223_372_036_854_775_808.0);
    }

    #[test]
    fn integers_above_the_mantissa_keep_their_leading_bits() {
        let rounded: f64 = u64::MAX.approx_into();
        assert_eq!(rounded, 18_446_744_073_709_551_616.0);
    }

    #[test]
    fn floats_lose_their_fractional_part() {
        assert_eq!(3.9_f64.try_approx_into(), Ok(3_u64));
        assert_eq!((-3.9_f64).try_approx_into(), Ok(-3_i64));
        assert_eq!(0.5_f32.try_approx_into(), Ok(0_u8));
    }

    #[test]
    fn floats_outside_the_target_range_are_rejected() {
        assert_eq!(
            (-1.0_f64).try_approx_into::<u64>(),
            Err(FromFloatError::OutOfRange {
                value: -1.0,
                target: "u64",
            })
        );
        assert_eq!(
            256.0_f64.try_approx_into::<u8>(),
            Err(FromFloatError::OutOfRange {
                value: 256.0,
                target: "u8",
            })
        );
        assert_eq!(255.9_f64.try_approx_into(), Ok(255_u8));
    }

    #[test]
    fn the_signed_limit_is_exclusive_where_the_cast_would_saturate() {
        assert_eq!(
            9_223_372_036_854_775_808.0_f64.try_approx_into::<i64>(),
            Err(FromFloatError::OutOfRange {
                value: 9_223_372_036_854_775_808.0,
                target: "i64",
            })
        );
        let largest: f64 = 9_223_372_036_854_774_784_i64.approx_into();
        assert_eq!(largest.try_approx_into(), Ok(9_223_372_036_854_774_784_i64));
    }

    #[test]
    fn non_finite_floats_name_no_integer() {
        assert!(matches!(
            f64::NAN.try_approx_into::<i64>(),
            Err(FromFloatError::NotFinite { value, target: "i64" }) if value.is_nan()
        ));
        assert!(matches!(
            f64::INFINITY.try_approx_into::<usize>(),
            Err(FromFloatError::NotFinite { .. })
        ));
    }

    #[test]
    fn pointer_width_targets_reuse_the_fixed_width_range() {
        assert_eq!(12.75_f64.try_approx_into(), Ok(12_usize));
        assert_eq!((-12.75_f64).try_approx_into(), Ok(-12_isize));
        assert!(matches!(
            (-1.0_f64).try_approx_into::<usize>(),
            Err(FromFloatError::OutOfRange { .. })
        ));
    }

    #[test]
    fn narrowing_a_float_rounds_to_the_nearest_representable_value() {
        let narrowed: f32 = 0.1_f64.approx_into();
        assert_eq!(narrowed, 0.1_f32);

        let overflowed: f32 = f64::MAX.approx_into();
        assert!(overflowed.is_infinite());
    }
}
