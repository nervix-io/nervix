//! Exact signatures for explicit arithmetic over list elements.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Arithmetic function identities, numeric leaf types and fixed shape preservation.
//! - **Depends on.** The VM semantic catalog and Arrow type vocabulary.
//! - **Must not know.** Registers, payload values, graph execution or scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns list arithmetic signatures")
)]

use super::*;

/// An explicit arithmetic operation over the numeric leaves of an ARRAY or VEC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListArithmetic {
    Add,
    Subtract,
    Multiply,
    Divide,
    AddScalar,
    SubtractScalar,
    MultiplyScalar,
    DivideScalar,
    AbsoluteValue,
}

impl ListArithmetic {
    pub(crate) const fn has_scalar(self) -> bool {
        matches!(
            self,
            Self::AddScalar | Self::SubtractScalar | Self::MultiplyScalar | Self::DivideScalar
        )
    }

    pub(crate) fn arithmetic(self) -> Option<crate::numeric::Arithmetic> {
        use crate::numeric::Arithmetic;
        match self {
            Self::Add | Self::AddScalar => Some(Arithmetic::Add),
            Self::Subtract | Self::SubtractScalar => Some(Arithmetic::Sub),
            Self::Multiply | Self::MultiplyScalar => Some(Arithmetic::Mul),
            Self::Divide | Self::DivideScalar => Some(Arithmetic::Div),
            Self::AbsoluteValue => None,
        }
    }

    pub(super) fn output_type(
        self,
        function: &FunctionName,
        arguments: &[DataType],
        span: std::ops::Range<usize>,
    ) -> error_stack::Result<DataType, CompileError> {
        let arity = if let Self::AbsoluteValue = self { 1 } else { 2 };
        require_builtin_arity_exact(function, arguments, arity, span.clone())?;
        let element = list_element_type(function, &arguments[0], ListElements::Any, span.clone())?;
        let leaf = self.numeric_leaf(function, &arguments[0], span.clone())?;
        if let Self::AbsoluteValue = self {
            return Ok(arguments[0].clone());
        }
        if self.has_scalar() {
            self.require_equal(function, leaf, &arguments[1], span)?;
            return Ok(arguments[0].clone());
        }
        let other = list_element_type(function, &arguments[1], ListElements::Any, span.clone())?;
        self.numeric_leaf(function, &arguments[1], span.clone())?;
        self.require_equal(function, &element, &other, span.clone())?;
        match (&arguments[0], &arguments[1]) {
            (DataType::FixedSizeList(_, left), DataType::FixedSizeList(_, right)) => {
                if left != right {
                    return Err(Report::new(CompileError {
                        code: CompileErrorCode::TypeMismatch,
                        message: format!(
                            "function '{}' requires equal ARRAY widths, found {left} and {right}",
                            function.as_str()
                        ),
                        span: span.into(),
                    }));
                }
                Ok(arguments[0].clone())
            }
            (DataType::FixedSizeList(field, _), _) => Ok(DataType::List(field.clone())),
            _ => Ok(arguments[0].clone()),
        }
    }

    fn numeric_leaf<'a>(
        self,
        function: &FunctionName,
        list: &'a DataType,
        span: std::ops::Range<usize>,
    ) -> error_stack::Result<&'a DataType, CompileError> {
        let (DataType::List(field) | DataType::FixedSizeList(field, _)) = list else {
            unreachable!("list_element_type validated the outer list above");
        };
        let mut field = field;
        let leaf = loop {
            if field.is_nullable() {
                return Err(Report::new(CompileError {
                    code: CompileErrorCode::UnsupportedFunction,
                    message: format!(
                        "function '{}' requires non-null list elements",
                        function.as_str()
                    ),
                    span: span.into(),
                }));
            }
            let leaf = field.data_type();
            let DataType::FixedSizeList(inner, _) = leaf else {
                break leaf;
            };
            field = inner;
        };
        let register = require_supported_register_type(function, leaf, span.clone())?;
        require_numeric_arg(function, register, span)?;
        Ok(leaf)
    }

    fn require_equal(
        self,
        function: &FunctionName,
        expected: &DataType,
        actual: &DataType,
        span: std::ops::Range<usize>,
    ) -> error_stack::Result<(), CompileError> {
        if expected == actual {
            return Ok(());
        }
        Err(Report::new(CompileError {
            code: CompileErrorCode::TypeMismatch,
            message: format!(
                "function '{}' requires exact matching element types, found {expected:?} and \
                 {actual:?}",
                function.as_str()
            ),
            span: span.into(),
        }))
    }
}
