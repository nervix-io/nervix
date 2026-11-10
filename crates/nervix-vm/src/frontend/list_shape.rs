//! Resolves literal list dimensions at the Model-to-program boundary.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Turning literal shape parameters into validated program vocabulary.
//! - **Depends on.** The VM frontend's structured expressions and shape vocabulary.
//! - **Must not know.** Parser tokens, runtime arrays or graph ownership.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "resolves shape literals before VM compilation")
)]

use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;

use super::{FrontendError, FrontendErrorKind, FrontendResult};
use crate::program::{
    ArrayDimensions, Expr, FunctionName, ListShapeArgumentError, ListShapeFunction,
    ListShapeFunctionName, Literal, Span, SpannedExpr,
};

impl ListShapeFunctionName {
    pub(super) fn lower_call(self, args: Vec<SpannedExpr>, span: Span) -> FrontendResult<Expr> {
        let function = match self {
            Self::Reshape => {
                if args.len() < 2 {
                    return Err(Report::new(ListShapeArgumentError::MissingDimensions)
                        .change_context(self.argument_context(span)));
                }
                let mut arguments = args.into_iter();
                let input = arguments
                    .next()
                    .verified("the argument count above includes an input");
                let mut widths = Vec::new();
                for (index, argument) in arguments.enumerate() {
                    let Expr::Literal(Literal::Int64(width)) = argument.inner else {
                        return Err(Report::new(ListShapeArgumentError::NonLiteralDimension {
                            position: index
                                .checked_add(1)
                                .assured("the argument sequence fits addressable memory"),
                        })
                        .change_context(self.argument_context(span)));
                    };
                    widths.push(width);
                }
                let dimensions = ArrayDimensions::try_from(widths)
                    .change_context(self.argument_context(span))?;
                return Ok(Expr::Call {
                    function: FunctionName::ListShape(ListShapeFunction::Reshape(dimensions)),
                    args: vec![input],
                });
            }
            Self::Flatten => ListShapeFunction::Flatten,
            Self::Transpose => ListShapeFunction::Transpose,
        };
        Ok(Expr::Call {
            function: FunctionName::ListShape(function),
            args,
        })
    }

    fn argument_context(self, span: Span) -> FrontendError {
        FrontendError::at(
            span,
            FrontendErrorKind::ListShapeArguments { function: self },
        )
    }
}
