//! Fixed dimensions and result signatures for list shape functions.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Validated literal dimensions and exact reshape, flatten and transpose signatures.
//! - **Depends on.** Arrow types and the VM's typed compilation diagnostics.
//! - **Must not know.** Models, parser state, graph ownership or execution scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns list shape signatures and validated dimensions")
)]

use arrow_schema::{DataType, Field};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::sync::StdArc;
use strum::{AsRefStr, Display, EnumIter, EnumString, IntoStaticStr};
use thiserror::Error;

use crate::{CompileError, CompileErrorCode, program::Span};

/// Spellings whose parameters the frontend resolves before type inference.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, EnumIter, EnumString, AsRefStr, IntoStaticStr, Display,
)]
#[strum(ascii_case_insensitive)]
pub enum ListShapeFunctionName {
    #[strum(serialize = "list_reshape")]
    Reshape,
    #[strum(serialize = "list_flatten")]
    Flatten,
    #[strum(serialize = "list_transpose")]
    Transpose,
}

/// A shape operation with its literal parameters resolved once.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ListShapeFunction {
    Reshape(ArrayDimensions),
    Flatten,
    Transpose,
}

/// A nonempty sequence of positive Arrow fixed-list widths whose product is addressable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArrayDimensions {
    widths: Vec<i32>,
}

/// Why literal dimensions cannot describe a fixed array.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ListShapeArgumentError {
    #[error("requires an input list and at least one dimension")]
    MissingDimensions,
    #[error("dimension {position} must be a positive integer literal")]
    NonLiteralDimension { position: usize },
    #[error("dimension {position} must be in 1..=2147483647, found {value}")]
    InvalidDimension { position: usize, value: i64 },
    #[error("the dimension product exceeds the addressable array length")]
    ElementCountOverflow,
}

impl TryFrom<Vec<i64>> for ArrayDimensions {
    type Error = Report<ListShapeArgumentError>;

    fn try_from(values: Vec<i64>) -> Result<Self, Self::Error> {
        if values.is_empty() {
            return Err(Report::new(ListShapeArgumentError::MissingDimensions));
        }
        let mut widths = Vec::with_capacity(values.len());
        let mut count = 1_usize;
        for (index, value) in values.into_iter().enumerate() {
            let position = index
                .checked_add(1)
                .assured("the dimension sequence fits in addressable memory");
            let width = i32::try_from(value).map_err(|_| {
                Report::new(ListShapeArgumentError::InvalidDimension { position, value })
            })?;
            if width <= 0 {
                return Err(Report::new(ListShapeArgumentError::InvalidDimension {
                    position,
                    value,
                }));
            }
            let size = usize::try_from(width).assured("the dimension is positive");
            count = count
                .checked_mul(size)
                .ok_or_else(|| Report::new(ListShapeArgumentError::ElementCountOverflow))?;
            widths.push(width);
        }
        Ok(Self { widths })
    }
}

impl ArrayDimensions {
    pub(crate) fn widths(&self) -> &[i32] {
        &self.widths
    }

    pub(crate) fn element_count(&self) -> usize {
        let mut count = 1_usize;
        for width in &self.widths {
            let width = usize::try_from(*width).assured("validated dimensions are positive");
            count = count
                .checked_mul(width)
                .assured("dimension construction checked the complete product");
        }
        count
    }

    fn data_type(&self, element: &DataType) -> DataType {
        let mut result = element.clone();
        for width in self.widths.iter().rev() {
            result =
                DataType::FixedSizeList(StdArc::new(Field::new("item", result, false)), *width);
        }
        result
    }
}

impl ListShapeFunction {
    pub const fn name(&self) -> ListShapeFunctionName {
        match self {
            Self::Reshape(_) => ListShapeFunctionName::Reshape,
            Self::Flatten => ListShapeFunctionName::Flatten,
            Self::Transpose => ListShapeFunctionName::Transpose,
        }
    }

    pub(crate) fn output_type(
        &self,
        arguments: &[DataType],
        span: Span,
    ) -> error_stack::Result<DataType, CompileError> {
        let [input] = arguments else {
            return Err(self.refusal("requires exactly one list input", span));
        };
        match self {
            Self::Reshape(dimensions) => {
                let shape = FixedShape::read(self, input, span)?;
                if shape.element_count() != dimensions.element_count() {
                    return Err(self.refusal(
                        "dimension product must equal the input's innermost element count",
                        span,
                    ));
                }
                Ok(dimensions.data_type(shape.element))
            }
            Self::Flatten => self.flatten_type(input, span),
            Self::Transpose => {
                let shape = FixedShape::read(self, input, span)?;
                let mut widths = shape.widths;
                if widths.len() < 2 {
                    return Err(self.refusal("requires a fixed ARRAY of rank two or more", span));
                }
                widths.swap(0, 1);
                Ok(ArrayDimensions { widths }.data_type(shape.element))
            }
        }
    }

    fn flatten_type(
        &self,
        input: &DataType,
        span: Span,
    ) -> error_stack::Result<DataType, CompileError> {
        let outer = match input {
            DataType::List(field) | DataType::FixedSizeList(field, _) => field,
            _ => return Err(self.refusal("requires an ARRAY or VEC of lists", span)),
        };
        match (input, outer.data_type()) {
            (DataType::FixedSizeList(_, left), DataType::FixedSizeList(field, right)) => {
                let width = left.checked_mul(*right).ok_or_else(|| {
                    self.refusal("flattened fixed width exceeds the Arrow ARRAY width", span)
                })?;
                Ok(DataType::FixedSizeList(field.clone(), width))
            }
            (_, DataType::List(field) | DataType::FixedSizeList(field, _)) => {
                Ok(DataType::List(field.clone()))
            }
            _ => Err(self.refusal("requires an ARRAY or VEC of lists", span)),
        }
    }

    fn refusal(&self, requirement: &str, span: Span) -> Report<CompileError> {
        Report::new(CompileError {
            code: CompileErrorCode::UnsupportedFunction,
            message: format!("function '{}' {requirement}", self.name()),
            span,
        })
    }
}

/// The fixed levels of one declared input, ending at its innermost element type.
struct FixedShape<'a> {
    widths: Vec<i32>,
    element: &'a DataType,
}

impl<'a> FixedShape<'a> {
    fn read(
        function: &ListShapeFunction,
        input: &'a DataType,
        span: Span,
    ) -> error_stack::Result<Self, CompileError> {
        let mut element = input;
        let mut widths = Vec::new();
        let mut element_count = 1_usize;
        while let DataType::FixedSizeList(field, width) = element {
            let Ok(size) = usize::try_from(*width) else {
                return Err(function.refusal("requires positive fixed dimensions", span));
            };
            if size == 0 {
                return Err(function.refusal("requires positive fixed dimensions", span));
            }
            element_count = element_count.checked_mul(size).ok_or_else(|| {
                function.refusal("input shape exceeds the addressable array length", span)
            })?;
            widths.push(*width);
            element = field.data_type();
        }
        if widths.is_empty() || matches!(element, DataType::List(_)) {
            return Err(function.refusal("requires a fixed ARRAY with no VEC dimension", span));
        }
        Ok(Self { widths, element })
    }

    fn element_count(&self) -> usize {
        let mut count = 1_usize;
        for width in &self.widths {
            let width = usize::try_from(*width).assured("the input shape has positive dimensions");
            count = count
                .checked_mul(width)
                .assured("reading the fixed shape checked the complete product");
        }
        count
    }
}
