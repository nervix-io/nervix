//! Exact constant-division qualification against the checked integer reference.
//!
//! Layer: test harness.
//! - **Owns.** Sign, range and reciprocal correction boundaries, and lane differential properties.
//! - **Depends on.** The production constant divisors and the supported SIMD levels.
//! - **Must not know.** Arrow, VM programs or error reporting.

use super::*;
use crate::{CheckedLanes, WORD_LANES, supported_levels};

#[test]
fn signed_constants_preserve_checked_euclidean_and_truncating_results() {
    let values = [i64::MIN, i64::MIN + 1, -7, -1, 0, 1, 7, i64::MAX];
    let divisors = [i64::MIN, -1_000_000_000, -7, -2, -1, 1, 2, 7, i64::MAX];
    for divisor in divisors {
        let prepared = SignedDivisor::new(divisor).assured("every test divisor is nonzero");
        for value in values {
            assert_eq!(prepared.checked_div(value), value.checked_div(divisor));
            assert_eq!(prepared.checked_rem(value), value.checked_rem(divisor));
            assert_eq!(
                prepared.checked_div_euclid(value),
                value.checked_div_euclid(divisor)
            );
            assert_eq!(
                prepared.checked_rem_euclid(value),
                value.checked_rem_euclid(divisor)
            );
        }
    }
    assert!(SignedDivisor::new(0).is_none());
}

trait Reference: DivisionLane + std::fmt::Debug {
    fn from_raw(raw: u64) -> Self;
    fn quotient(self, divisor: Self) -> Option<Self>;
    fn remainder(self, divisor: Self) -> Option<Self>;
}

macro_rules! reference {
    ($($native:ty),+ $(,)?) => {$(
        impl Reference for $native {
            fn from_raw(raw: u64) -> Self {
                let bytes = raw.to_le_bytes();
                let (low, _) = bytes.split_at(size_of::<Self>());
                Self::from_le_bytes(low.try_into().assured("the slice has exactly the type's byte width"))
            }

            fn quotient(self, divisor: Self) -> Option<Self> {
                self.checked_div(divisor)
            }

            fn remainder(self, divisor: Self) -> Option<Self> {
                if divisor == 0 { None } else { Some(self.checked_rem(divisor).unwrap_or(0)) }
            }
        }
    )+};
}

reference!(i8, u8, i16, u16, i32, u32, i64, u64);

fn check_lanes<N: Reference>(values: &[N], divisor: N) {
    for remainder in [false, true] {
        let mut expected = CheckedLanes {
            values: Vec::with_capacity(values.len()),
            failed: vec![0; values.len().div_ceil(WORD_LANES)],
        };
        for (index, value) in values.iter().enumerate() {
            let result = if remainder {
                value.remainder(divisor)
            } else {
                value.quotient(divisor)
            };
            match result {
                Some(value) => expected.values.push(value),
                None => {
                    expected.values.push(N::default());
                    expected.failed[index / WORD_LANES] |= 1 << (index % WORD_LANES);
                }
            }
        }
        for level in supported_levels() {
            let kernel = ConstantDivision::with_level(level);
            let actual = if remainder {
                kernel.remainders(values, divisor)
            } else {
                kernel.quotients(values, divisor)
            };
            assert_eq!(
                actual, expected,
                "{level:?}, divisor {divisor:?}, remainder {remainder}"
            );
        }
    }
}

#[test]
fn every_byte_constant_matches_all_byte_dividends_at_every_level() {
    let unsigned: Vec<u8> = (u8::MIN..=u8::MAX).collect();
    let signed: Vec<i8> = (i8::MIN..=i8::MAX).collect();
    for divisor in u8::MIN..=u8::MAX {
        check_lanes(&unsigned, divisor);
    }
    for divisor in i8::MIN..=i8::MAX {
        check_lanes(&signed, divisor);
    }
}

fn check_width<N: Reference>() {
    let raw = [
        0,
        1,
        2,
        3,
        7,
        10,
        1_000,
        1_000_000,
        1_000_000_000,
        60_000_000_000,
        3_600_000_000_000,
        86_400_000_000_000,
        604_800_000_000_000,
        u64::MAX / 2,
        (u64::MAX / 2) + 1,
        u64::MAX - 1,
        u64::MAX,
    ];
    let mut random = fastrand::Rng::with_seed(0x0008_D1D1);
    let mut values: Vec<N> = raw.iter().copied().map(N::from_raw).collect();
    for _ in 0..256 {
        values.push(N::from_raw(random.u64(..)));
    }
    for divisor in raw {
        let divisor = N::from_raw(divisor);
        check_lanes(&values, divisor);
        for length in [0, 1, 2, 3, 7, 15, 31, 63, 64, 65, 127, 128, 129, 193] {
            check_lanes(&values[..length], divisor);
        }
    }
    for _ in 0..32 {
        check_lanes(&values, N::from_raw(random.u64(..)));
    }
}

#[test]
fn reciprocal_lanes_match_full_width_values_and_word_tails() {
    check_width::<i16>();
    check_width::<u16>();
    check_width::<i32>();
    check_width::<u32>();
    check_width::<i64>();
    check_width::<u64>();
}

#[test]
fn bolero_constant_division_matches_checked_operations_at_every_level() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(128)
        .with_type::<(u8, u8, u64, Vec<u64>)>()
        .for_each(|(width, length, divisor, raw)| {
            macro_rules! width_case {
                ($native:ty) => {{
                    let mut values: Vec<$native> = raw
                        .iter()
                        .cycle()
                        .take(usize::from(*length))
                        .copied()
                        .map(<$native>::from_raw)
                        .collect();
                    values.extend([<$native>::MIN, <$native>::MAX, 0, 1]);
                    check_lanes(&values, <$native>::from_raw(*divisor));
                }};
            }
            match width % 8 {
                0 => width_case!(i8),
                1 => width_case!(u8),
                2 => width_case!(i16),
                3 => width_case!(u16),
                4 => width_case!(i32),
                5 => width_case!(u32),
                6 => width_case!(i64),
                _ => width_case!(u64),
            }
            let divisor = divisor.cast_signed();
            if let Some(prepared) = SignedDivisor::new(divisor) {
                for value in raw {
                    let value = value.cast_signed();
                    assert_eq!(prepared.checked_div(value), value.checked_div(divisor));
                    assert_eq!(prepared.checked_rem(value), value.checked_rem(divisor));
                    assert_eq!(
                        prepared.checked_div_euclid(value),
                        value.checked_div_euclid(divisor)
                    );
                    assert_eq!(
                        prepared.checked_rem_euclid(value),
                        value.checked_rem_euclid(divisor)
                    );
                }
            }
            if let Some(prepared) = UnsignedDivisor::new(divisor.cast_unsigned()) {
                for value in raw {
                    assert_eq!(
                        prepared.quotient_remainder(*value),
                        (
                            value / divisor.cast_unsigned(),
                            value % divisor.cast_unsigned()
                        )
                    );
                }
            }
        });
}

#[test]
fn every_fixed_unit_and_generated_bin_width_has_exact_euclidean_results() {
    let units = [
        1_i64,
        1_000,
        1_000_000,
        1_000_000_000,
        60_000_000_000,
        3_600_000_000_000,
        86_400_000_000_000,
        604_800_000_000_000,
    ];
    let mut random = fastrand::Rng::with_seed(0x0008_DA7E);
    for unit in units {
        for count in [1, 2, 3, 15, i64::MAX / unit] {
            let stride = count
                .checked_mul(unit)
                .assured("the count is bounded by MAX / unit");
            let prepared = SignedDivisor::new(stride).assured("every generated stride is positive");
            for value in [i64::MIN, i64::MIN + 1, -stride, -1, 0, 1, stride, i64::MAX] {
                assert_eq!(
                    prepared.checked_div_euclid(value),
                    value.checked_div_euclid(stride)
                );
                assert_eq!(
                    prepared.checked_rem_euclid(value),
                    value.checked_rem_euclid(stride)
                );
            }
        }
        for _ in 0..256 {
            let count = random.i64(1..=i64::MAX / unit);
            let stride = count
                .checked_mul(unit)
                .assured("the generated count is bounded by MAX / unit");
            let value = random.i64(..);
            let prepared = SignedDivisor::new(stride).assured("every generated stride is positive");
            assert_eq!(
                prepared.checked_div_euclid(value),
                value.checked_div_euclid(stride)
            );
            assert_eq!(
                prepared.checked_rem_euclid(value),
                value.checked_rem_euclid(stride)
            );
        }
    }
}
