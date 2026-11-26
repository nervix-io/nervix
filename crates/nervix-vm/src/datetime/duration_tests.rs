//! Tests of the DURATION kernels: the operators a duration takes and the conversions between a
//! duration and whole units, against exact `i128` references.
//!
//! Layer: test harness.
//!
//! - **Owns.** The value, validity and failure of every lane the duration kernels compute, for
//!   columns, shared scalars and sliced arrays, at the boundaries of DURATION and DATETIME.
//! - **Depends on.** The duration kernels, the checked lanes they return, and Arrow arrays.
//! - **Must not know.** Registers, programs, spans, or how a failed lane becomes a row error.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::collections::BTreeSet;

use arrow_array::{
    Array, ArrowPrimitiveType, DurationNanosecondArray, Int64Array, PrimitiveArray,
    TimestampNanosecondArray,
    types::{
        DurationNanosecondType, Int8Type, Int16Type, Int32Type, Int64Type, TimestampNanosecondType,
        UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::{DataType, TimeUnit};
use strum::{IntoEnumIterator as _, VariantNames as _};

use super::{
    Direction, UnitLength, differences, durations, elapsed, moved_instants, negations, sums,
    whole_units,
};
use crate::{
    batch::TypedArray,
    datetime::UnitCounts,
    numeric::Checked,
    operand::Operand,
    program::{DurationUnit, UnitRounding},
};

const SECOND: i64 = 1_000_000_000;

/// What one lane of a checked result holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lane {
    /// A computed value.
    Value(i64),
    /// No value, because an operand was null.
    Null,
    /// No value, because the operation failed.
    Failed,
}

/// Every lane of a checked result. A failed lane must be null in the column, so no failed result
/// survives in the buffer.
fn lanes<T>(checked: &Checked<T>) -> Vec<Lane>
where
    T: ArrowPrimitiveType<Native = i64>,
{
    let failed = checked.failed.lanes().collect::<BTreeSet<_>>();
    let mut lanes = Vec::with_capacity(checked.column.len());
    for lane in 0..checked.column.len() {
        let valid = checked.column.is_valid(lane);
        if failed.contains(&lane) {
            assert!(!valid, "failed lane {lane} must be null");
            lanes.push(Lane::Failed);
        } else if valid {
            lanes.push(Lane::Value(checked.column.value(lane)));
        } else {
            lanes.push(Lane::Null);
        }
    }
    lanes
}

/// The lane an exact result names: the value when it fits `i64`, and a failed lane when it does
/// not.
fn exact_lane(exact: i128) -> Lane {
    match i64::try_from(exact) {
        Ok(value) => Lane::Value(value),
        Err(_) => Lane::Failed,
    }
}

fn durations_of(values: &[Option<i64>]) -> DurationNanosecondArray {
    DurationNanosecondArray::from(values.to_vec())
}

fn instants_of(values: &[Option<i64>]) -> TimestampNanosecondArray {
    TimestampNanosecondArray::from(values.to_vec()).with_timezone_utc()
}

#[test]
fn durations_add_and_subtract_exactly_and_fail_where_the_result_leaves_a_duration() {
    let left = durations_of(&[
        Some(90 * SECOND),
        Some(i64::MAX),
        Some(i64::MIN),
        None,
        Some(-SECOND),
    ]);
    let right = durations_of(&[Some(-30 * SECOND), Some(1), Some(1), Some(SECOND), None]);
    let added = sums(Operand::Column(&left), Operand::Column(&right));
    assert_eq!(
        lanes(&added),
        [
            Lane::Value(60 * SECOND),
            Lane::Failed,
            Lane::Value(i64::MIN + 1),
            Lane::Null,
            Lane::Null,
        ]
    );
    let subtracted = differences(Operand::Column(&left), Operand::Column(&right));
    assert_eq!(
        lanes(&subtracted),
        [
            Lane::Value(120 * SECOND),
            Lane::Value(i64::MAX - 1),
            Lane::Failed,
            Lane::Null,
            Lane::Null,
        ]
    );
}

#[test]
fn a_negated_duration_fails_only_for_the_shortest_duration() {
    let values = durations_of(&[Some(SECOND), Some(i64::MAX), Some(i64::MIN), Some(0), None]);
    assert_eq!(
        lanes(&negations(&values)),
        [
            Lane::Value(-SECOND),
            Lane::Value(-i64::MAX),
            Lane::Failed,
            Lane::Value(0),
            Lane::Null,
        ]
    );
}

#[test]
fn a_duration_moves_an_instant_inside_the_datetime_range_only() {
    let instants = instants_of(&[Some(0), Some(i64::MAX - SECOND), Some(i64::MIN), None]);
    let durations = durations_of(&[Some(-SECOND), Some(2 * SECOND), Some(SECOND), Some(SECOND)]);
    let forward = moved_instants(
        Operand::Column(&instants),
        Operand::Column(&durations),
        Direction::Forward,
    );
    assert_eq!(
        lanes(&forward),
        [
            Lane::Value(-SECOND),
            Lane::Failed,
            Lane::Value(i64::MIN + SECOND),
            Lane::Null,
        ]
    );
    let backward = moved_instants(
        Operand::Column(&instants),
        Operand::Column(&durations),
        Direction::Backward,
    );
    assert_eq!(
        lanes(&backward),
        [
            Lane::Value(SECOND),
            Lane::Value(i64::MAX - 3 * SECOND),
            Lane::Failed,
            Lane::Null,
        ]
    );
    // A moved instant is a DATETIME, a UTC instant, whichever way it moved.
    let utc_instant = DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into()));
    for moved in [forward, backward] {
        assert_eq!(moved.column.data_type(), &utc_instant);
    }
}

#[test]
fn the_duration_between_two_instants_fails_only_where_they_lie_further_apart_than_a_duration_holds()
{
    let later = instants_of(&[Some(90 * SECOND), Some(i64::MAX), Some(0), Some(i64::MIN)]);
    let earlier = instants_of(&[
        Some(30 * SECOND),
        Some(i64::MIN),
        Some(90 * SECOND),
        Some(0),
    ]);
    assert_eq!(
        lanes(&elapsed(Operand::Column(&later), Operand::Column(&earlier))),
        [
            Lane::Value(60 * SECOND),
            Lane::Failed,
            Lane::Value(-90 * SECOND),
            Lane::Value(i64::MIN),
        ]
    );
}

#[test]
fn a_shared_scalar_on_either_side_applies_to_every_lane_and_a_null_scalar_nulls_them() {
    let column = durations_of(&[Some(SECOND), Some(i64::MAX), None]);
    let one = durations_of(&[Some(1)]);
    assert_eq!(
        lanes(&sums(Operand::Scalar(&one), Operand::Column(&column))),
        [Lane::Value(SECOND + 1), Lane::Failed, Lane::Null]
    );
    assert_eq!(
        lanes(&differences(
            Operand::Column(&column),
            Operand::Scalar(&one)
        )),
        [
            Lane::Value(SECOND - 1),
            Lane::Value(i64::MAX - 1),
            Lane::Null
        ]
    );
    let null = durations_of(&[None]);
    assert_eq!(
        lanes(&sums(Operand::Column(&column), Operand::Scalar(&null))),
        [Lane::Null; 3]
    );
    let instant = instants_of(&[Some(10 * SECOND)]);
    assert_eq!(
        lanes(&moved_instants(
            Operand::Scalar(&instant),
            Operand::Column(&column),
            Direction::Backward
        )),
        [
            Lane::Value(9 * SECOND),
            Lane::Value(10 * SECOND - i64::MAX),
            Lane::Null
        ]
    );
}

#[test]
fn a_sliced_column_computes_only_its_own_lanes() {
    let durations = durations_of(&[Some(i64::MAX), Some(SECOND), None, Some(2 * SECOND)]);
    let sliced = durations.slice(1, 3);
    // Read without its offset, the slice would add the longest duration to the first instant and
    // fail the second lane instead of the third.
    let instants = instants_of(&[Some(0), Some(i64::MAX), Some(i64::MAX)]);
    let moved = moved_instants(
        Operand::Column(&instants),
        Operand::Column(&sliced),
        Direction::Forward,
    );
    assert_eq!(
        lanes(&moved),
        [Lane::Value(SECOND), Lane::Null, Lane::Failed]
    );
    // A sliced instant column keeps its own offset too.
    let wide = instants_of(&[Some(i64::MIN), Some(3 * SECOND), Some(4 * SECOND)]);
    let narrow = wide.slice(1, 2);
    let earlier = instants_of(&[Some(SECOND), Some(i64::MIN)]);
    assert_eq!(
        lanes(&elapsed(
            Operand::Column(&narrow),
            Operand::Column(&earlier)
        )),
        [Lane::Value(2 * SECOND), Lane::Failed]
    );
}

#[test]
fn whole_units_round_in_the_direction_their_function_names() {
    let values = durations_of(&[
        Some(90_400_000_000),
        Some(-87_900_000_000),
        Some(500_000_000),
        Some(-500_000_000),
        Some(1_500_000_000),
        Some(-1_500_000_000),
        Some(0),
        Some(i64::MAX),
        Some(i64::MIN),
        None,
    ]);
    let collect = |rounding| -> Vec<Option<i64>> {
        whole_units(&values, DurationUnit::Second, rounding)
            .iter()
            .collect()
    };
    let max_seconds = i64::MAX / SECOND;
    let min_seconds = i64::MIN / SECOND;
    assert_eq!(
        collect(UnitRounding::Floor),
        [
            Some(90),
            Some(-88),
            Some(0),
            Some(-1),
            Some(1),
            Some(-2),
            Some(0),
            Some(max_seconds),
            Some(min_seconds - 1),
            None
        ]
    );
    assert_eq!(
        collect(UnitRounding::Ceil),
        [
            Some(91),
            Some(-87),
            Some(1),
            Some(0),
            Some(2),
            Some(-1),
            Some(0),
            Some(max_seconds + 1),
            Some(min_seconds),
            None
        ]
    );
    assert_eq!(
        collect(UnitRounding::Round),
        [
            Some(90),
            Some(-88),
            Some(1),
            Some(-1),
            Some(2),
            Some(-2),
            Some(0),
            Some(max_seconds + 1),
            Some(min_seconds - 1),
            None
        ]
    );
    // A nanosecond is the unit a duration is counted in, so every rounding returns it unchanged.
    for rounding in [UnitRounding::Floor, UnitRounding::Ceil, UnitRounding::Round] {
        let units = whole_units(&values, DurationUnit::Nanosecond, rounding);
        assert_eq!(
            units.iter().collect::<Vec<_>>(),
            values.iter().collect::<Vec<_>>()
        );
    }
}

#[test]
fn a_count_converts_to_a_duration_until_it_no_longer_fits() {
    let longest_days = i64::MAX / DurationUnit::Day.nanoseconds();
    let counts = Int64Array::from(vec![
        Some(longest_days),
        Some(longest_days + 1),
        Some(-longest_days),
        Some(-longest_days - 1),
        None,
    ]);
    let typed = TypedArray::Int64(counts);
    let counts = UnitCounts::from_typed(&typed).expect("an I64 column is a count of units");
    let day = DurationUnit::Day.nanoseconds();
    assert_eq!(
        lanes(&durations(&counts, DurationUnit::Day)),
        [
            Lane::Value(longest_days * day),
            Lane::Failed,
            Lane::Value(-longest_days * day),
            Lane::Failed,
            Lane::Null,
        ]
    );
    // Every U64 count of nanoseconds above i64::MAX is longer than a DURATION holds.
    let unsigned = TypedArray::UInt64(PrimitiveArray::<UInt64Type>::from(vec![
        Some(u64::try_from(i64::MAX).expect("i64::MAX is positive")),
        Some(u64::MAX),
    ]));
    let unsigned = UnitCounts::from_typed(&unsigned).expect("a U64 column is a count of units");
    assert_eq!(
        lanes(&durations(&unsigned, DurationUnit::Nanosecond)),
        [Lane::Value(i64::MAX), Lane::Failed]
    );
}

/// A generated count of nanoseconds, drawn anywhere in `i64`, beside either limit or near zero, so
/// the generated domain reaches every boundary an operator or a conversion fails at.
#[derive(Debug, Clone, Copy, bolero::TypeGenerator)]
enum Nanoseconds {
    Raw(i64),
    NearMax(u16),
    NearMin(u16),
    NearZero(i16),
}

impl Nanoseconds {
    fn value(self) -> i64 {
        match self {
            Self::Raw(value) => value,
            Self::NearMax(distance) => i64::MAX - i64::from(distance),
            Self::NearMin(distance) => i64::MIN + i64::from(distance),
            Self::NearZero(value) => i64::from(value),
        }
    }
}

/// How many leading lanes a generated slice hides, so a kernel must read its operands from their
/// own offsets.
fn skipped_lanes(skipped: u8) -> usize {
    usize::from(skipped % 4)
}

/// One operand side of a generated case, before its arrays are built: the value of every lane, or
/// `None` for a null lane.
struct Side {
    values: Vec<Option<i64>>,
}

impl Side {
    /// The side a one-lane scalar holds: this side's first lane, or no lane for an empty side.
    fn first_lane(&self) -> Self {
        let mut values = Vec::with_capacity(1);
        if let Some(first) = self.values.first() {
            values.push(*first);
        }
        Self { values }
    }

    /// The values `rows` lanes read from this one-lane scalar side.
    fn repeated(&self, rows: usize) -> Vec<Option<i64>> {
        let value = self
            .values
            .first()
            .expect("a scalar side replaces a side only when the case has a lane");
        vec![*value; rows]
    }

    /// The side's values as an array of `T`, with `skipped` leading lanes that the slice it returns
    /// hides.
    fn sliced<T>(&self, skipped: usize) -> PrimitiveArray<T>
    where
        T: ArrowPrimitiveType<Native = i64>,
    {
        let mut padded = vec![Some(i64::MIN); skipped];
        padded.extend(self.values.iter().copied());
        let array = padded.into_iter().collect::<PrimitiveArray<T>>();
        array.slice(skipped, self.values.len())
    }
}

#[derive(Debug, bolero::TypeGenerator)]
struct OperandLane {
    left: Nanoseconds,
    right: Nanoseconds,
    null_left: bool,
    null_right: bool,
}

impl OperandLane {
    fn left_value(&self) -> Option<i64> {
        if self.null_left {
            None
        } else {
            Some(self.left.value())
        }
    }

    fn right_value(&self) -> Option<i64> {
        if self.null_right {
            None
        } else {
            Some(self.right.value())
        }
    }
}

/// Operands of a binary operator: their lanes, which side a one-lane scalar replaces, and how many
/// leading lanes a slice skips.
#[derive(Debug, bolero::TypeGenerator)]
struct OperatorColumns {
    lanes: Vec<OperandLane>,
    shape: u8,
    skipped: u8,
}

/// How the two operands of a generated case are shaped.
#[derive(Debug, Clone, Copy)]
enum Shapes {
    Columns,
    ScalarLeft,
    ScalarRight,
}

impl OperatorColumns {
    fn shapes(&self) -> Shapes {
        match self.shape % 3 {
            0 => Shapes::Columns,
            1 => Shapes::ScalarLeft,
            _ => Shapes::ScalarRight,
        }
    }

    /// The values of the left and the right operand, before a scalar replaces a side.
    fn sides(&self) -> (Side, Side) {
        let mut left = Vec::with_capacity(self.lanes.len());
        let mut right = Vec::with_capacity(self.lanes.len());
        for lane in &self.lanes {
            left.push(lane.left_value());
            right.push(lane.right_value());
        }
        (Side { values: left }, Side { values: right })
    }
}

/// The lanes one operator computed over a generated case, and the values every lane read on each
/// side, after a scalar replaced its side.
struct OperatorRun {
    computed: Vec<Lane>,
    left: Vec<Option<i64>>,
    right: Vec<Option<i64>>,
}

/// Builds the operands of one generated case and runs `kernel` over them, handing it each side as
/// a column or as the scalar its shape names. A scalar side holds its first lane.
fn run_operator<L, R, O>(
    columns: &OperatorColumns,
    kernel: impl Fn(Operand<'_, PrimitiveArray<L>>, Operand<'_, PrimitiveArray<R>>) -> Checked<O>,
) -> OperatorRun
where
    L: ArrowPrimitiveType<Native = i64>,
    R: ArrowPrimitiveType<Native = i64>,
    O: ArrowPrimitiveType<Native = i64>,
{
    let skipped = skipped_lanes(columns.skipped);
    let (mut left, mut right) = columns.sides();
    let rows = left.values.len();
    // A scalar is a one-lane array, so a case without lanes has no value to share.
    let shapes = if rows == 0 {
        Shapes::Columns
    } else {
        columns.shapes()
    };
    let left_scalar = left.first_lane();
    let right_scalar = right.first_lane();
    let left_array = match shapes {
        Shapes::ScalarLeft => left_scalar.sliced::<L>(skipped),
        Shapes::Columns | Shapes::ScalarRight => left.sliced::<L>(skipped),
    };
    let right_array = match shapes {
        Shapes::ScalarRight => right_scalar.sliced::<R>(skipped),
        Shapes::Columns | Shapes::ScalarLeft => right.sliced::<R>(skipped),
    };
    let (left_operand, right_operand) = match shapes {
        Shapes::Columns => (Operand::Column(&left_array), Operand::Column(&right_array)),
        Shapes::ScalarLeft => (Operand::Scalar(&left_array), Operand::Column(&right_array)),
        Shapes::ScalarRight => (Operand::Column(&left_array), Operand::Scalar(&right_array)),
    };
    let computed = lanes(&kernel(left_operand, right_operand));
    // Every lane of a side a scalar replaced reads the scalar's one value.
    if let Shapes::ScalarLeft = shapes {
        left.values = left_scalar.repeated(rows);
    }
    if let Shapes::ScalarRight = shapes {
        right.values = right_scalar.repeated(rows);
    }
    OperatorRun {
        computed,
        left: left.values,
        right: right.values,
    }
}

/// The lanes an operator computes exactly as `exact` does in `i128`: a lane with a null operand is
/// null without failing, and a lane whose exact result leaves `i64` fails.
fn reference_lanes(
    left: &[Option<i64>],
    right: &[Option<i64>],
    exact: impl Fn(i128, i128) -> i128,
) -> Vec<Lane> {
    let mut lanes = Vec::with_capacity(left.len());
    for (left, right) in left.iter().zip(right) {
        let (Some(left), Some(right)) = (left, right) else {
            lanes.push(Lane::Null);
            continue;
        };
        lanes.push(exact_lane(exact(i128::from(*left), i128::from(*right))));
    }
    lanes
}

/// `+`: `DURATION + DURATION`, and `DATETIME + DURATION`, which `DURATION + DATETIME` runs too with
/// its operands exchanged.
fn assert_additions(columns: &OperatorColumns) {
    let run = run_operator::<DurationNanosecondType, DurationNanosecondType, _>(columns, sums);
    let expected = reference_lanes(&run.left, &run.right, |left, right| left + right);
    assert_eq!(run.computed, expected, "DURATION + DURATION");
    let run = run_operator::<TimestampNanosecondType, DurationNanosecondType, _>(
        columns,
        |instants, durations| moved_instants(instants, durations, Direction::Forward),
    );
    let expected = reference_lanes(&run.left, &run.right, |left, right| left + right);
    assert_eq!(run.computed, expected, "DATETIME + DURATION");
}

/// `-`: `DURATION - DURATION`, `DATETIME - DURATION` and `DATETIME - DATETIME`.
fn assert_subtractions(columns: &OperatorColumns) {
    let run =
        run_operator::<DurationNanosecondType, DurationNanosecondType, _>(columns, differences);
    let expected = reference_lanes(&run.left, &run.right, |left, right| left - right);
    assert_eq!(run.computed, expected, "DURATION - DURATION");
    let run = run_operator::<TimestampNanosecondType, DurationNanosecondType, _>(
        columns,
        |instants, durations| moved_instants(instants, durations, Direction::Backward),
    );
    let expected = reference_lanes(&run.left, &run.right, |left, right| left - right);
    assert_eq!(run.computed, expected, "DATETIME - DURATION");
    let run = run_operator::<TimestampNanosecondType, TimestampNanosecondType, _>(columns, elapsed);
    let expected = reference_lanes(&run.left, &run.right, |left, right| left - right);
    assert_eq!(run.computed, expected, "DATETIME - DATETIME");
}

#[test]
fn bolero_duration_addition_matches_an_exact_reference() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<OperatorColumns>()
        .for_each(assert_additions);
}

#[test]
fn bolero_duration_subtraction_matches_an_exact_reference() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<OperatorColumns>()
        .for_each(assert_subtractions);
}

#[derive(Debug, bolero::TypeGenerator)]
struct DurationLane {
    value: Nanoseconds,
    null: bool,
}

/// A generated `DURATION` column: its lanes, and how many leading lanes a slice skips.
#[derive(Debug, bolero::TypeGenerator)]
struct DurationColumn {
    lanes: Vec<DurationLane>,
    skipped: u8,
}

impl DurationColumn {
    /// The value of every lane, or `None` for a null lane.
    fn values(&self) -> Vec<Option<i64>> {
        let mut values = Vec::with_capacity(self.lanes.len());
        for lane in &self.lanes {
            if lane.null {
                values.push(None);
            } else {
                values.push(Some(lane.value.value()));
            }
        }
        values
    }

    /// The column as a sliced array, whose leading hidden lanes the kernel must not read.
    fn sliced(&self) -> DurationNanosecondArray {
        let side = Side {
            values: self.values(),
        };
        side.sliced::<DurationNanosecondType>(skipped_lanes(self.skipped))
    }
}

/// Unary `-` over every lane of a generated column.
fn assert_negations(column: &DurationColumn) {
    let mut expected = Vec::with_capacity(column.lanes.len());
    for value in column.values() {
        match value {
            Some(value) => expected.push(exact_lane(-i128::from(value))),
            None => expected.push(Lane::Null),
        }
    }
    assert_eq!(lanes(&negations(&column.sliced())), expected, "-DURATION");
}

#[test]
fn bolero_duration_negation_matches_an_exact_reference() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<DurationColumn>()
        .for_each(assert_negations);
}

/// The unit a generated index names, among every unit a duration conversion counts in.
fn unit_of(index: u8) -> DurationUnit {
    let position = usize::from(index) % DurationUnit::VARIANTS.len();
    DurationUnit::iter()
        .nth(position)
        .expect("the position is below the number of units")
}

/// A generated `DURATION` column and the unit a conversion counts it in.
#[derive(Debug, bolero::TypeGenerator)]
struct UnitDurations {
    unit: u8,
    column: DurationColumn,
}

/// The whole units of `unit` an exact `i128` rounding names for `value`.
fn reference_units(value: i64, unit: i64, rounding: UnitRounding) -> i64 {
    let value = i128::from(value);
    let unit = i128::from(unit);
    let units = match rounding {
        UnitRounding::Floor => value.div_euclid(unit),
        UnitRounding::Ceil => -(-value).div_euclid(unit),
        UnitRounding::Round => {
            let truncated = value / unit;
            let remainder = value % unit;
            if 2 * remainder.abs() >= unit {
                truncated + value.signum()
            } else {
                truncated
            }
        }
    };
    i64::try_from(units).expect("a count of units of at least a nanosecond fits i64")
}

/// One rounding over every lane of a generated column, and the folding contract over every value
/// of it, against the exact reference. No lane fails.
fn assert_whole_units(case: &UnitDurations, rounding: UnitRounding) {
    let unit = unit_of(case.unit);
    let length = UnitLength::of(unit);
    let mut expected = Vec::with_capacity(case.column.lanes.len());
    for value in case.column.values() {
        let Some(value) = value else {
            expected.push(None);
            continue;
        };
        let units = reference_units(value, unit.nanoseconds(), rounding);
        // Folding computes one constant through the lane contract the kernel applies to columns.
        assert_eq!(
            rounding.whole_units(value, length),
            units,
            "folded {rounding:?} {unit}"
        );
        expected.push(Some(units));
    }
    let computed = whole_units(&case.column.sliced(), unit, rounding);
    assert_eq!(
        computed.iter().collect::<Vec<_>>(),
        expected,
        "{rounding:?} {unit}"
    );
}

#[test]
fn bolero_duration_floor_matches_an_exact_reference() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<UnitDurations>()
        .for_each(|case| assert_whole_units(case, UnitRounding::Floor));
}

#[test]
fn bolero_duration_ceil_matches_an_exact_reference() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<UnitDurations>()
        .for_each(|case| assert_whole_units(case, UnitRounding::Ceil));
}

#[test]
fn bolero_duration_round_matches_an_exact_reference() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<UnitDurations>()
        .for_each(|case| assert_whole_units(case, UnitRounding::Round));
}

/// A count of units of any integer width, built from the low bytes of its generated bits so every
/// width reaches both of its limits.
trait CountType: ArrowPrimitiveType + Sized
where
    Self::Native: Into<i128>,
{
    fn from_bits(bits: u64) -> Self::Native;

    fn typed(array: PrimitiveArray<Self>) -> TypedArray;
}

macro_rules! count_types {
    ($($Type:ty => $Native:ty, $width:literal, $Variant:ident;)+) => {
        $(
            impl CountType for $Type {
                fn from_bits(bits: u64) -> $Native {
                    let bytes = bits.to_le_bytes();
                    let mut narrow = [0_u8; $width];
                    narrow.copy_from_slice(&bytes[..$width]);
                    <$Native>::from_le_bytes(narrow)
                }

                fn typed(array: PrimitiveArray<Self>) -> TypedArray {
                    TypedArray::$Variant(array)
                }
            }
        )+
    };
}

count_types! {
    UInt8Type => u8, 1, UInt8;
    Int8Type => i8, 1, Int8;
    UInt16Type => u16, 2, UInt16;
    Int16Type => i16, 2, Int16;
    UInt32Type => u32, 4, UInt32;
    Int32Type => i32, 4, Int32;
    UInt64Type => u64, 8, UInt64;
    Int64Type => i64, 8, Int64;
}

#[derive(Debug, bolero::TypeGenerator)]
struct CountLane {
    bits: u64,
    null: bool,
}

/// A generated count column of one integer width, the unit `to_duration` counts it in, and how
/// many leading lanes a slice skips.
#[derive(Debug, bolero::TypeGenerator)]
struct UnitCountColumn {
    unit: u8,
    width: u8,
    lanes: Vec<CountLane>,
    skipped: u8,
}

fn assert_counts<C>(case: &UnitCountColumn)
where
    C: CountType,
    C::Native: Into<i128>,
{
    let unit = unit_of(case.unit);
    let skipped = skipped_lanes(case.skipped);
    let mut counts = Vec::with_capacity(case.lanes.len());
    for lane in &case.lanes {
        if lane.null {
            counts.push(None);
        } else {
            counts.push(Some(C::from_bits(lane.bits)));
        }
    }
    let mut padded = vec![None; skipped];
    padded.extend(counts.iter().copied());
    let column = padded.into_iter().collect::<PrimitiveArray<C>>();
    let typed = C::typed(column.slice(skipped, counts.len()));
    let operand = UnitCounts::from_typed(&typed).expect("an integer column is a count of units");
    let mut expected = Vec::with_capacity(counts.len());
    for count in counts {
        let Some(count) = count else {
            expected.push(Lane::Null);
            continue;
        };
        let exact = count.into() * i128::from(unit.nanoseconds());
        expected.push(exact_lane(exact));
    }
    assert_eq!(
        lanes(&durations(&operand, unit)),
        expected,
        "to_duration {unit}"
    );
}

/// `to_duration` over a generated count column of the width the case names.
fn assert_to_duration(case: &UnitCountColumn) {
    match case.width % 8 {
        0 => assert_counts::<UInt8Type>(case),
        1 => assert_counts::<Int8Type>(case),
        2 => assert_counts::<UInt16Type>(case),
        3 => assert_counts::<Int16Type>(case),
        4 => assert_counts::<UInt32Type>(case),
        5 => assert_counts::<Int32Type>(case),
        6 => assert_counts::<UInt64Type>(case),
        _ => assert_counts::<Int64Type>(case),
    }
}

#[test]
fn bolero_to_duration_matches_an_exact_reference() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(1024)
        .with_type::<UnitCountColumn>()
        .for_each(assert_to_duration);
}
