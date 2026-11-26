//! The arithmetic a `DURATION` and a `DATETIME` take.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The six binary forms of duration and datetime arithmetic, each with its operand
//!   types and result type, and whether an arithmetic operator meets a temporal operand. Typing in
//!   the compiler and dispatch in the runtime both classify through these forms.
//! - **Depends on.** VM operators and register types.
//! - **Must not know.** Kernels, registers, payload values, graph execution or scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the classification of duration and datetime arithmetic"
    )
)]

use arrow_schema::DataType;

use crate::{RegisterType, program::BinaryOp};

/// The binary arithmetic a `DURATION` and a `DATETIME` take. With unary minus on a `DURATION`,
/// these are the only operators either type takes besides comparison.
///
/// Typing and execution classify an operator and its operand types through
/// [`TemporalArithmetic::of`] alike, so every form the compiler accepts is a form the runtime
/// executes, with the result type the compiler gave it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalArithmetic {
    /// `DURATION + DURATION`.
    DurationSum,
    /// `DURATION - DURATION`.
    DurationDifference,
    /// `DATETIME + DURATION`: the instant the duration lies after the datetime, or before it for
    /// a negative duration.
    InstantPlusDuration,
    /// `DURATION + DATETIME`: the same instant, with the duration written first.
    DurationPlusInstant,
    /// `DATETIME - DURATION`: the instant the duration lies before the datetime.
    InstantMinusDuration,
    /// `DATETIME - DATETIME`: the duration from the right instant to the left one, negative when
    /// the left one is earlier.
    Elapsed,
}

impl TemporalArithmetic {
    /// The form `op` takes over operands of these register types, or `None` when no form
    /// combines them: an operand is not temporal, or the operator does not apply to the pair.
    pub const fn of(op: BinaryOp, left: RegisterType, right: RegisterType) -> Option<Self> {
        match (op, left, right) {
            (BinaryOp::Add, RegisterType::Duration, RegisterType::Duration) => {
                Some(Self::DurationSum)
            }
            (BinaryOp::Sub, RegisterType::Duration, RegisterType::Duration) => {
                Some(Self::DurationDifference)
            }
            (BinaryOp::Add, RegisterType::Datetime, RegisterType::Duration) => {
                Some(Self::InstantPlusDuration)
            }
            (BinaryOp::Add, RegisterType::Duration, RegisterType::Datetime) => {
                Some(Self::DurationPlusInstant)
            }
            (BinaryOp::Sub, RegisterType::Datetime, RegisterType::Duration) => {
                Some(Self::InstantMinusDuration)
            }
            (BinaryOp::Sub, RegisterType::Datetime, RegisterType::Datetime) => Some(Self::Elapsed),
            _ => None,
        }
    }

    /// The form `op` takes over operands of these types, as [`TemporalArithmetic::of`] classifies
    /// their registers.
    pub fn of_types(op: BinaryOp, left: &DataType, right: &DataType) -> Option<Self> {
        let left = RegisterType::from_data_type(left)?;
        let right = RegisterType::from_data_type(right)?;
        Self::of(op, left, right)
    }

    /// The register type of the result.
    pub const fn output(self) -> RegisterType {
        match self {
            Self::DurationSum | Self::DurationDifference | Self::Elapsed => RegisterType::Duration,
            Self::InstantPlusDuration | Self::DurationPlusInstant | Self::InstantMinusDuration => {
                RegisterType::Datetime
            }
        }
    }
}

/// Whether `op` is an arithmetic operator with a `DURATION` or a `DATETIME` operand. Such an
/// operator combines operands of different types in some forms, so a pair it refuses is refused as
/// a pair rather than for its types differing.
pub fn is_temporal_arithmetic(op: BinaryOp, left: &DataType, right: &DataType) -> bool {
    let arithmetic = matches!(
        op,
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Rem
    );
    arithmetic && (is_temporal_type(left) || is_temporal_type(right))
}

fn is_temporal_type(data_type: &DataType) -> bool {
    matches!(
        RegisterType::from_data_type(data_type),
        Some(RegisterType::Datetime | RegisterType::Duration)
    )
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use arrow_schema::{DataType, TimeUnit};

    use super::is_temporal_arithmetic;
    use crate::{
        RegisterType,
        program::{BinaryOp, UnaryOp},
        semantics::{binary_output_type, unary_descriptor},
    };

    #[test]
    fn duration_and_datetime_operators_type_exactly_their_forms() {
        let duration = DataType::Duration(TimeUnit::Nanosecond);
        let datetime = RegisterType::Datetime.data_type();
        let forms = [
            (BinaryOp::Add, &duration, &duration, Some(&duration)),
            (BinaryOp::Sub, &duration, &duration, Some(&duration)),
            (BinaryOp::Add, &datetime, &duration, Some(&datetime)),
            (BinaryOp::Add, &duration, &datetime, Some(&datetime)),
            (BinaryOp::Sub, &datetime, &duration, Some(&datetime)),
            (BinaryOp::Sub, &datetime, &datetime, Some(&duration)),
            (BinaryOp::Sub, &duration, &datetime, None),
            (BinaryOp::Add, &datetime, &datetime, None),
            (BinaryOp::Mul, &duration, &duration, None),
            (BinaryOp::Div, &duration, &duration, None),
            (BinaryOp::Rem, &duration, &duration, None),
            (BinaryOp::Mul, &duration, &DataType::Int64, None),
            (BinaryOp::Add, &DataType::Int64, &datetime, None),
        ];
        for (op, left, right, output) in forms {
            assert_eq!(
                binary_output_type(op, left, right).as_ref(),
                output,
                "{op:?} over {left:?} and {right:?}"
            );
            assert!(is_temporal_arithmetic(op, left, right), "{op:?}");
        }
        // Comparison still requires one type, and arithmetic over numbers alone is not temporal.
        assert!(!is_temporal_arithmetic(BinaryOp::Lt, &duration, &datetime));
        assert!(!is_temporal_arithmetic(
            BinaryOp::Add,
            &DataType::Int64,
            &DataType::Int64
        ));
        assert_eq!(
            unary_descriptor(UnaryOp::Neg).output_type(&duration),
            Some(duration.clone())
        );
        assert_eq!(unary_descriptor(UnaryOp::Neg).output_type(&datetime), None);
    }
}
