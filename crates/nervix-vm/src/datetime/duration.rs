//! Checked DURATION execution over Arrow buffers.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The batch kernels behind the operators a DURATION takes: the sum and the difference
//!   of two durations, the negation of one, an instant moved forward or backward by a duration, and
//!   the duration from one instant to another. It also owns the kernels behind the conversions
//!   between a duration and a whole number of units, and the lane contracts constant folding shares
//!   with them, so a folded constant and an executed column agree by construction.
//! - **Depends on.** Arrow arrays, the checked lanes and integer arithmetic of the numeric kernels,
//!   the prepared signed divisor of the SIMD kernel crate, and the resolved duration units of a VM
//!   program.
//! - **Must not know.** Registers, programs, spans, clocks, or how a failed lane is recorded as a
//!   row error.
//!
//! A DURATION lane holds a signed count of nanoseconds, and a DATETIME lane signed nanoseconds since
//! the Unix epoch. Every operator is therefore exact `i64` arithmetic on the lanes' nanoseconds,
//! computed by the checked integer lanes the numeric operators use: the sums and differences of the
//! explicit SIMD kernels, and the negation's lane loop. An operand of one temporal type meets an
//! operand of the other in the same kernel through its nanoseconds, read in place from its value
//! and validity buffers. A result that leaves `i64` fails its lane instead of wrapping: an instant
//! outside the DATETIME range, or a duration longer than a DURATION holds.
//!
//! A conversion to whole units divides by one unit prepared once per call, so no lane executes a
//! division instruction, and rounds in the one direction its function names. A count of units is
//! never longer than the nanoseconds it counts, so no rounding fails. A conversion from a count
//! multiplies exactly in `i128` and fails a lane whose duration does not fit `i64`.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use arrow_array::{
    Array, ArrowPrimitiveType, DurationNanosecondArray, Int64Array, PrimitiveArray,
    TimestampNanosecondArray,
    types::{DurationNanosecondType, Int64Type, TimestampNanosecondType},
};
use meticulous::OptionExt as _;
use nervix_simd_kernels::SignedDivisor;

use super::{UnitCounts, datetime_column, i64_lane};
use crate::{
    numeric::{self, Arithmetic, Checked, Lanes},
    operand::Operand,
    program::{DurationUnit, UnitRounding},
};

/// Every lane's sum of two durations, failed where the sum does not fit a DURATION.
pub(crate) fn sums(
    left: Operand<'_, DurationNanosecondArray>,
    right: Operand<'_, DurationNanosecondArray>,
) -> Checked<DurationNanosecondType> {
    Arithmetic::Add.evaluate_integers(left, right)
}

/// Every lane's difference of two durations, failed where the difference does not fit a
/// DURATION.
pub(crate) fn differences(
    left: Operand<'_, DurationNanosecondArray>,
    right: Operand<'_, DurationNanosecondArray>,
) -> Checked<DurationNanosecondType> {
    Arithmetic::Sub.evaluate_integers(left, right)
}

/// Every lane's negated duration, failed for the shortest DURATION, whose negation is one
/// nanosecond longer than the longest.
pub(crate) fn negations(values: &DurationNanosecondArray) -> Checked<DurationNanosecondType> {
    numeric::integer_negation(values)
}

/// Which way a duration moves an instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    /// `DATETIME + DURATION`, and `DURATION + DATETIME`, which moves the instant the same way.
    Forward,
    /// `DATETIME - DURATION`.
    Backward,
}

impl Direction {
    /// The integer operator that moves an instant's nanoseconds by a duration's.
    const fn arithmetic(self) -> Arithmetic {
        match self {
            Self::Forward => Arithmetic::Add,
            Self::Backward => Arithmetic::Sub,
        }
    }
}

/// Every lane's instant moved by its duration, failed where the moved instant lies outside the
/// DATETIME range.
pub(crate) fn moved_instants(
    instants: Operand<'_, TimestampNanosecondArray>,
    durations: Operand<'_, DurationNanosecondArray>,
    direction: Direction,
) -> Checked<TimestampNanosecondType> {
    let instants = Nanoseconds::of(instants);
    let durations = Nanoseconds::of(durations);
    let moved = direction
        .arithmetic()
        .evaluate_integers(instants.operand(), durations.operand());
    datetime_column(moved.reinterpreted())
}

/// Every lane's duration from its `earlier` instant to its `later` one, negative where `later`
/// precedes `earlier`, and failed where the two instants lie further apart than a DURATION holds.
pub(crate) fn elapsed(
    later: Operand<'_, TimestampNanosecondArray>,
    earlier: Operand<'_, TimestampNanosecondArray>,
) -> Checked<DurationNanosecondType> {
    let later = Nanoseconds::of(later);
    let earlier = Nanoseconds::of(earlier);
    Arithmetic::Sub
        .evaluate_integers(later.operand(), earlier.operand())
        .reinterpreted()
}

/// One temporal operand read as its nanoseconds: an `I64` array sharing the operand's value and
/// validity buffers, in the operand's own shape, so operands of two temporal types meet in one
/// checked integer kernel without copying either.
struct Nanoseconds {
    lanes: Int64Array,
    shared: bool,
}

impl Nanoseconds {
    fn of<T: ArrowPrimitiveType<Native = i64>>(operand: Operand<'_, PrimitiveArray<T>>) -> Self {
        let array = operand.array();
        let lanes = Int64Array::new(array.values().clone(), array.nulls().cloned());
        let shared = match operand {
            Operand::Column(_) => false,
            Operand::Scalar(_) => true,
        };
        Self { lanes, shared }
    }

    fn operand(&self) -> Operand<'_, Int64Array> {
        if self.shared {
            Operand::Scalar(&self.lanes)
        } else {
            Operand::Column(&self.lanes)
        }
    }
}

impl Checked<Int64Type> {
    /// The same lanes and failures read as another temporal type, whose lanes are nanoseconds too.
    fn reinterpreted<T: ArrowPrimitiveType<Native = i64>>(self) -> Checked<T> {
        let (_, values, nulls) = self.column.into_parts();
        Checked {
            column: PrimitiveArray::new(values, nulls),
            failed: self.failed,
        }
    }
}

/// A unit prepared for the lanes of one call: its exact length in nanoseconds, and the divisor
/// that divides by that length without a division instruction.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UnitLength {
    nanoseconds: i64,
    divisor: SignedDivisor,
}

impl UnitLength {
    pub(crate) fn of(unit: DurationUnit) -> Self {
        let nanoseconds = unit.nanoseconds();
        let divisor = SignedDivisor::new(nanoseconds)
            .assured("a duration unit is a positive number of nanoseconds");
        Self {
            nanoseconds,
            divisor,
        }
    }

    /// The whole units in `value`, rounded toward negative infinity.
    pub(crate) fn floor(self, value: i64) -> i64 {
        self.divisor
            .checked_div_euclid(value)
            .assured("a positive unit divides every i64 value into a quotient that fits i64")
    }

    /// The whole units in `value`, rounded toward positive infinity.
    pub(crate) fn ceil(self, value: i64) -> i64 {
        let truncated = self.truncated(value);
        let step = if self.remainder(value) > 0 { 1 } else { 0 };
        truncated.checked_add(step).assured(
            "a positive remainder needs a unit of at least two nanoseconds, so the quotient it \
             rounds up is at most half of i64::MAX",
        )
    }

    /// The whole units nearest to `value`, with a value halfway between two counts rounded away
    /// from zero.
    pub(crate) fn round(self, value: i64) -> i64 {
        let truncated = self.truncated(value);
        let remainder = self.remainder(value).unsigned_abs();
        let unit = self.nanoseconds.unsigned_abs();
        // The remainder is below one unit, so the next whole unit lies the rest of the unit away.
        let to_next = unit
            .checked_sub(remainder)
            .assured("a remainder is smaller than the unit it is the remainder of");
        let step = if remainder >= to_next {
            value.signum()
        } else {
            0
        };
        truncated.checked_add(step).assured(
            "a remainder that rounds away from zero needs a unit of at least two nanoseconds, so \
             the quotient it rounds is at most half of i64::MAX from zero",
        )
    }

    /// The duration `count` units long, failed where it does not fit a DURATION.
    pub(crate) fn duration(self, count: impl Into<i128>) -> (i64, bool) {
        let nanoseconds = count
            .into()
            .checked_mul(i128::from(self.nanoseconds))
            .assured(
                "an integer below 2^64 in magnitude times a unit of at most a day, below 2^47 \
                 nanoseconds, fits i128",
            );
        i64_lane(nanoseconds)
    }

    /// The whole units in `value`, rounded toward zero.
    fn truncated(self, value: i64) -> i64 {
        self.divisor
            .checked_div(value)
            .assured("a positive unit divides every i64 value into a quotient that fits i64")
    }

    /// What `value` holds past its whole units rounded toward zero, with the sign of `value`.
    fn remainder(self, value: i64) -> i64 {
        self.divisor
            .checked_rem(value)
            .assured("a remainder by a positive unit exists for every i64 value")
    }
}

impl UnitRounding {
    /// The whole units of `length` in `value`, rounded this way: the lane contract its kernel
    /// applies to every lane, for a value constant folding computes once.
    pub(crate) fn whole_units(self, value: i64, length: UnitLength) -> i64 {
        match self {
            Self::Floor => length.floor(value),
            Self::Ceil => length.ceil(value),
            Self::Round => length.round(value),
        }
    }
}

/// Every lane's whole units of `unit`, rounded as `rounding` names. A lane with a null duration
/// is null, and no lane fails.
pub(crate) fn whole_units(
    values: &DurationNanosecondArray,
    unit: DurationUnit,
    rounding: UnitRounding,
) -> Int64Array {
    let length = UnitLength::of(unit);
    // Each arm hands its own lane function to the loop, so the loop inlines it rather than calling
    // one chosen function per lane.
    match rounding {
        UnitRounding::Floor => values.unary(|value| length.floor(value)),
        UnitRounding::Ceil => values.unary(|value| length.ceil(value)),
        UnitRounding::Round => values.unary(|value| length.round(value)),
    }
}

/// Every lane's duration of its count of `unit`s, failed where the duration does not fit a
/// DURATION.
pub(crate) fn durations(
    counts: &UnitCounts<'_>,
    unit: DurationUnit,
) -> Checked<DurationNanosecondType> {
    let length = UnitLength::of(unit);
    match counts {
        UnitCounts::UInt8(counts) => duration_lanes(counts, length),
        UnitCounts::Int8(counts) => duration_lanes(counts, length),
        UnitCounts::UInt16(counts) => duration_lanes(counts, length),
        UnitCounts::Int16(counts) => duration_lanes(counts, length),
        UnitCounts::UInt32(counts) => duration_lanes(counts, length),
        UnitCounts::Int32(counts) => duration_lanes(counts, length),
        UnitCounts::UInt64(counts) => duration_lanes(counts, length),
        UnitCounts::Int64(counts) => duration_lanes(counts, length),
    }
}

fn duration_lanes<T>(
    counts: &PrimitiveArray<T>,
    length: UnitLength,
) -> Checked<DurationNanosecondType>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i128>,
{
    let lanes = Lanes::unary(counts.values(), |count: T::Native| length.duration(count));
    Checked::from_lanes(lanes, counts.nulls().cloned())
}

#[cfg(test)]
#[path = "duration_tests.rs"]
mod tests;
