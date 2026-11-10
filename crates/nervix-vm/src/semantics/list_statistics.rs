//! The element types the statistics over one list read, and the types they return.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Which elements the list statistics accept and what they return, and the order check
//!   `min`, `max`, `list_argmin` and `list_argmax` share.
//! - **Depends on.** The catalog's list element rule and its argument checks.
//! - **Must not know.** Registers, instructions, kernels, or how a call was written.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::ops::Range;

use arrow_schema::DataType;
use error_stack::Report;

use super::{
    ListElements, list_element_type, require_builtin_arity_exact, require_numeric_arg,
    require_supported_register_type,
};
use crate::{
    CompileError, CompileErrorCode, RegisterType, list_statistics::ListStatistic,
    program::FunctionName,
};

impl ListStatistic {
    /// The type of the statistic over its one list argument. A variance, standard deviation or
    /// quantile reads numeric elements and returns an `F64`; an index reads any ordered scalar
    /// element and returns an `I64`. Neither reads a nested `ARRAY` or `VEC` element.
    pub(super) fn output_type(
        self,
        function: &FunctionName,
        arg_types: &[DataType],
        span: Range<usize>,
    ) -> error_stack::Result<DataType, CompileError> {
        require_builtin_arity_exact(function, arg_types, 1, span.clone())?;
        let element =
            list_element_type(function, &arg_types[0], ListElements::Scalar, span.clone())?;
        let input = require_supported_register_type(function, &element, span.clone())?;
        match self {
            Self::Dispersion(_) | Self::Quantile(_) => {
                require_numeric_arg(function, input, span)?;
                Ok(DataType::Float64)
            }
            Self::ExtremeIndex(_) => {
                require_ordered_elements(function, &element, input, span)?;
                Ok(DataType::Int64)
            }
        }
    }
}

/// Requires elements `min`, `max`, `list_argmin` and `list_argmax` can order: numbers, `BOOL`,
/// `STRING` and `DATETIME`.
pub(super) fn require_ordered_elements(
    function: &FunctionName,
    element: &DataType,
    input: RegisterType,
    span: Range<usize>,
) -> error_stack::Result<(), CompileError> {
    if input.is_ordered() || input == RegisterType::Boolean {
        return Ok(());
    }
    Err(Report::new(CompileError {
        code: CompileErrorCode::UnsupportedFunction,
        message: format!(
            "function '{}' requires ordered scalar elements, found {element:?}",
            function.as_str()
        ),
        span: span.into(),
    }))
}
