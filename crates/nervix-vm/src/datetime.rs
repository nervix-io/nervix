//! Checked datetime execution over Arrow buffers.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The batch kernels behind every datetime builtin: date-part extraction, truncation,
//!   binning, fixed-unit addition and difference, and conversion to and from Unix time. Each
//!   kernel is one pass over its operands' value buffers that yields the result column and the
//!   lanes whose result its type cannot hold.
//! - **Depends on.** Arrow arrays and Arrow's date-part kernel, the checked lanes of the numeric
//!   kernels, and the resolved datetime vocabulary of a VM program.
//! - **Must not know.** Registers, programs, spans, clocks, or how a failed lane is recorded as a
//!   row error.
//!
//! A DATETIME lane holds signed nanoseconds since the Unix epoch, which is how an Arrow timestamp
//! array carries a UTC instant. Every kernel computes exactly in nanoseconds. Truncation, binning
//! and conversion to a coarser Unix unit round toward negative infinity, a difference rounds toward
//! zero, and a result outside its type fails its lane instead of wrapping or saturating. No kernel
//! reads a clock: a kernel over the current time receives it as an operand, from the execution
//! context its caller supplies.

use arrow_arith::temporal::{DatePart as ArrowDatePart, date_part as arrow_date_part};
use arrow_array::{
    Array, ArrowPrimitiveType, Int8Array, Int16Array, Int32Array, Int64Array, PrimitiveArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    cast::AsArray as _,
    types::{Int32Type, Int64Type, TimestampNanosecondType},
};
use arrow_buffer::NullBuffer;
use meticulous::{OptionExt as _, ResultExt as _};

use crate::{
    batch::TypedArray,
    numeric::{Checked, Lanes},
    program::{DateBinWidth, DatePart, FixedTimeUnit},
};

/// How far the first Monday after the Unix epoch lies past it. Weeks start on Monday, and the
/// epoch itself was a Thursday.
const FIRST_MONDAY_AFTER_UNIX_EPOCH: i64 = 4 * FixedTimeUnit::Day.nanoseconds();

const _: () = assert!(
    FIRST_MONDAY_AFTER_UNIX_EPOCH < FixedTimeUnit::Week.nanoseconds(),
    "a week's start lies less than one week past the Unix epoch"
);

/// Extracts one UTC date part from every lane.
pub(crate) fn date_part(values: &TimestampNanosecondArray, part: DatePart) -> Int64Array {
    // Arrow moves every lane into the array's timezone before extracting a part from it. A
    // DATETIME already is a UTC instant, so its nanoseconds read as a zoneless timestamp yield the
    // UTC part without converting each lane through an offset.
    let zoneless = TimestampNanosecondArray::new(values.values().clone(), values.nulls().cloned());
    let arrow_part = match part {
        DatePart::Year => ArrowDatePart::Year,
        DatePart::Quarter => ArrowDatePart::Quarter,
        DatePart::Month => ArrowDatePart::Month,
        DatePart::Day => ArrowDatePart::Day,
        DatePart::Hour => ArrowDatePart::Hour,
        DatePart::Minute => ArrowDatePart::Minute,
        DatePart::Second => ArrowDatePart::Second,
        DatePart::Millisecond => ArrowDatePart::Millisecond,
        DatePart::Microsecond => ArrowDatePart::Microsecond,
        DatePart::Nanosecond => ArrowDatePart::Nanosecond,
        DatePart::DayOfWeek => ArrowDatePart::DayOfWeekSunday0,
        DatePart::DayOfYear => ArrowDatePart::DayOfYear,
        DatePart::IsoYear => ArrowDatePart::YearISO,
        DatePart::IsoWeek => ArrowDatePart::WeekISO,
        DatePart::IsoDayOfWeek => ArrowDatePart::DayOfWeekMonday0,
    };
    let extracted = arrow_date_part(&zoneless, arrow_part)
        .assured("Arrow extracts every date part from a zoneless nanosecond timestamp array");
    let extracted = extracted
        .as_primitive_opt::<Int32Type>()
        .assured("Arrow extracts a date part from a timestamp array as Int32 lanes");
    if let DatePart::IsoDayOfWeek = part {
        // ISO 8601 numbers the days from Monday as 1 to Sunday as 7, one past Arrow's count.
        return extracted.unary(|day| {
            i64::from(day)
                .checked_add(1)
                .assured("a day of the week counted from Monday is at most 6")
        });
    }
    extracted.unary(i64::from)
}

/// Truncates every lane to the start of the unit that holds it. A day, and every shorter unit,
/// starts at a whole number of units since the Unix epoch, and a week starts on Monday.
pub(crate) fn truncate(
    values: &TimestampNanosecondArray,
    unit: FixedTimeUnit,
) -> Checked<TimestampNanosecondType> {
    let stride = unit.nanoseconds();
    let phase = match unit {
        FixedTimeUnit::Week => FIRST_MONDAY_AFTER_UNIX_EPOCH,
        FixedTimeUnit::Nanosecond
        | FixedTimeUnit::Microsecond
        | FixedTimeUnit::Millisecond
        | FixedTimeUnit::Second
        | FixedTimeUnit::Minute
        | FixedTimeUnit::Hour
        | FixedTimeUnit::Day => 0,
    };
    let lanes = Lanes::unary(values.values(), |value: i64| {
        bin_start(value, phase, stride)
    });
    datetime_column(Checked::from_lanes(lanes, values.nulls().cloned()))
}

/// Places every lane in a bin `width` wide, where bins start at the lane's origin and at every
/// whole number of widths before and after it.
pub(crate) fn bin(
    values: &TimestampNanosecondArray,
    origins: &TimestampNanosecondArray,
    width: DateBinWidth,
) -> Checked<TimestampNanosecondType> {
    let stride = width.nanoseconds();
    let lanes = Lanes::binary(
        values.values(),
        origins.values(),
        |value: i64, origin: i64| {
            let phase = origin
                .checked_rem_euclid(stride)
                .assured("a bin width is a positive number of nanoseconds");
            bin_start(value, phase, stride)
        },
    );
    let nulls = NullBuffer::union(values.nulls(), origins.nulls());
    datetime_column(Checked::from_lanes(lanes, nulls))
}

/// The start of the bin that holds `value`, for bins `stride` nanoseconds wide that start `phase`
/// nanoseconds past every multiple of `stride`, with `phase` in `[0, stride)`.
///
/// A start is never after its value, so a value before the epoch or before its origin belongs to
/// the bin that starts at or before it. The lane fails when that start precedes the signed
/// Unix-nanosecond range.
fn bin_start(value: i64, phase: i64, stride: i64) -> (i64, bool) {
    let past_multiple = value
        .checked_rem_euclid(stride)
        .assured("a stride is a positive number of nanoseconds");
    let past_phase = past_multiple
        .checked_sub(phase)
        .assured("both offsets lie in [0, stride), so their difference lies in (-stride, stride)");
    let into_bin = past_phase
        .checked_rem_euclid(stride)
        .assured("a stride is a positive number of nanoseconds");
    value.overflowing_sub(into_bin)
}

/// An integer column a datetime builtin reads as a count of units, at the width its schema
/// declares.
pub(crate) enum UnitCounts<'a> {
    UInt8(&'a UInt8Array),
    Int8(&'a Int8Array),
    UInt16(&'a UInt16Array),
    Int16(&'a Int16Array),
    UInt32(&'a UInt32Array),
    Int32(&'a Int32Array),
    UInt64(&'a UInt64Array),
    Int64(&'a Int64Array),
}

impl<'a> UnitCounts<'a> {
    /// Reads an integer column as counts of units, or answers `None` for a column that is not
    /// integral.
    pub(crate) fn from_typed(input: &'a TypedArray) -> Option<Self> {
        match input {
            TypedArray::UInt8(array) => Some(Self::UInt8(array)),
            TypedArray::Int8(array) => Some(Self::Int8(array)),
            TypedArray::UInt16(array) => Some(Self::UInt16(array)),
            TypedArray::Int16(array) => Some(Self::Int16(array)),
            TypedArray::UInt32(array) => Some(Self::UInt32(array)),
            TypedArray::Int32(array) => Some(Self::Int32(array)),
            TypedArray::UInt64(array) => Some(Self::UInt64(array)),
            TypedArray::Int64(array) => Some(Self::Int64(array)),
            TypedArray::Float32(_)
            | TypedArray::Float64(_)
            | TypedArray::Boolean(_)
            | TypedArray::Utf8(_)
            | TypedArray::Datetime(_)
            | TypedArray::Generic(_)
            | TypedArray::Uninitialized { .. } => None,
        }
    }
}

/// Moves every lane by its amount of units, forward for a positive amount and backward for a
/// negative one.
pub(crate) fn add(
    amounts: &UnitCounts<'_>,
    values: &TimestampNanosecondArray,
    unit: FixedTimeUnit,
) -> Checked<TimestampNanosecondType> {
    let unit = i128::from(unit.nanoseconds());
    match amounts {
        UnitCounts::UInt8(amounts) => add_lanes(amounts, values, unit),
        UnitCounts::Int8(amounts) => add_lanes(amounts, values, unit),
        UnitCounts::UInt16(amounts) => add_lanes(amounts, values, unit),
        UnitCounts::Int16(amounts) => add_lanes(amounts, values, unit),
        UnitCounts::UInt32(amounts) => add_lanes(amounts, values, unit),
        UnitCounts::Int32(amounts) => add_lanes(amounts, values, unit),
        UnitCounts::UInt64(amounts) => add_lanes(amounts, values, unit),
        UnitCounts::Int64(amounts) => add_lanes(amounts, values, unit),
    }
}

/// Adds `amounts` of a unit `unit` nanoseconds long. The sum is exact in `i128`, so a lane fails
/// only when the instant it names lies outside the signed Unix-nanosecond range, even where the
/// offset alone would not fit `i64`.
fn add_lanes<T>(
    amounts: &PrimitiveArray<T>,
    values: &TimestampNanosecondArray,
    unit: i128,
) -> Checked<TimestampNanosecondType>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i128>,
{
    let lanes = Lanes::binary(
        amounts.values(),
        values.values(),
        |amount: T::Native, value: i64| {
            let offset = amount.into().checked_mul(unit).assured(
                "an integer operand is below 2^64 and a unit below 2^50 nanoseconds, so their \
                 product fits i128",
            );
            let instant = offset
                .checked_add(i128::from(value))
                .assured("an offset below 2^114 plus an i64 value fits i128");
            i64_lane(instant)
        },
    );
    let nulls = NullBuffer::union(amounts.nulls(), values.nulls());
    datetime_column(Checked::from_lanes(lanes, nulls))
}

/// Counts the whole units from every start lane to its end lane, rounding toward zero. The count
/// is negative when the end precedes the start, and a lane fails when the count does not fit
/// `i64`.
pub(crate) fn difference(
    starts: &TimestampNanosecondArray,
    ends: &TimestampNanosecondArray,
    unit: FixedTimeUnit,
) -> Checked<Int64Type> {
    let unit = i128::from(unit.nanoseconds());
    let lanes = Lanes::binary(starts.values(), ends.values(), |start: i64, end: i64| {
        let elapsed = i128::from(end)
            .checked_sub(i128::from(start))
            .assured("two i64 values differ by less than 2^64, which fits i128");
        let units = elapsed
            .checked_div(unit)
            .assured("a unit is at least one nanosecond, so the quotient exists");
        i64_lane(units)
    });
    Checked::from_lanes(lanes, NullBuffer::union(starts.nulls(), ends.nulls()))
}

/// Counts the whole units from the Unix epoch to every lane, rounding toward negative infinity, so
/// an instant before the epoch counts back to the unit that starts at or before it.
pub(crate) fn to_unix(values: &TimestampNanosecondArray, unit: FixedTimeUnit) -> Int64Array {
    let unit = unit.nanoseconds();
    values.unary(|value| {
        value
            .checked_div_euclid(unit)
            .assured("a unit is at least one nanosecond, so the quotient exists and fits i64")
    })
}

/// The instant every lane's count of units lies past the Unix epoch.
pub(crate) fn from_unix(
    counts: &UnitCounts<'_>,
    unit: FixedTimeUnit,
) -> Checked<TimestampNanosecondType> {
    let unit = i128::from(unit.nanoseconds());
    match counts {
        UnitCounts::UInt8(counts) => from_unix_lanes(counts, unit),
        UnitCounts::Int8(counts) => from_unix_lanes(counts, unit),
        UnitCounts::UInt16(counts) => from_unix_lanes(counts, unit),
        UnitCounts::Int16(counts) => from_unix_lanes(counts, unit),
        UnitCounts::UInt32(counts) => from_unix_lanes(counts, unit),
        UnitCounts::Int32(counts) => from_unix_lanes(counts, unit),
        UnitCounts::UInt64(counts) => from_unix_lanes(counts, unit),
        UnitCounts::Int64(counts) => from_unix_lanes(counts, unit),
    }
}

fn from_unix_lanes<T>(counts: &PrimitiveArray<T>, unit: i128) -> Checked<TimestampNanosecondType>
where
    T: ArrowPrimitiveType,
    T::Native: Into<i128>,
{
    let lanes = Lanes::unary(counts.values(), |count: T::Native| {
        let nanoseconds = count.into().checked_mul(unit).assured(
            "an integer operand is below 2^64 and a unit below 2^50 nanoseconds, so their product \
             fits i128",
        );
        i64_lane(nanoseconds)
    });
    datetime_column(Checked::from_lanes(lanes, counts.nulls().cloned()))
}

/// A lane holding `value`, failed when `value` does not fit `i64`.
fn i64_lane(value: i128) -> (i64, bool) {
    match i64::try_from(value) {
        Ok(value) => (value, false),
        Err(_) => (0, true),
    }
}

/// Marks a computed timestamp column as the UTC instants every DATETIME holds.
fn datetime_column(checked: Checked<TimestampNanosecondType>) -> Checked<TimestampNanosecondType> {
    Checked {
        column: checked.column.with_timezone_utc(),
        failed: checked.failed,
    }
}

#[cfg(test)]
#[path = "datetime_tests.rs"]
mod tests;
