//! Lowering a call of a statistic over one list, with a quantile's percentile read from its
//! literal.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** How many arguments each list statistic is written with, the statistic each name
//!   lowers to, and reading `list_quantile`'s percentile from its numeric literal once, when the
//!   call is lowered.
//! - **Depends on.** The list statistics' vocabulary and the frontend's errors.
//! - **Must not know.** Types, registers, kernels, or the rows a call reads.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use meticulous::OptionExt as _;
use nervix_approx_into::ApproxInto as _;

use super::{ArgumentCount, FrontendError, FrontendErrorKind, FrontendResult};
use crate::{
    list_statistics::{
        ListDispersion, ListExtremum, ListPercentile, ListQuantile, ListStatistic,
        ListStatisticName,
    },
    program::{Expr, FunctionName, Literal, Span, SpannedExpr},
};

impl ListStatisticName {
    /// Lowers a call of this statistic. `list_quantile` reads its percentile here, once, from its
    /// literal, so the lowered call keeps only the list it reads on every row.
    pub(super) fn lower_call(self, args: Vec<SpannedExpr>, span: Span) -> FrontendResult<Expr> {
        let expected = self.argument_count();
        let found = args.len();
        if found != expected {
            return Err(FrontendError::report(
                span,
                FrontendErrorKind::ListStatisticArity {
                    function: self,
                    expected: ArgumentCount {
                        fewest: expected,
                        most: expected,
                    },
                    found,
                },
            ));
        }
        let mut args = args.into_iter();
        let list = args
            .next()
            .verified("the call was checked above to have its list argument");
        let statistic = match self {
            Self::ListVarPop => ListStatistic::Dispersion(ListDispersion::VarPop),
            Self::ListVarSamp => ListStatistic::Dispersion(ListDispersion::VarSamp),
            Self::ListStddevPop => ListStatistic::Dispersion(ListDispersion::StddevPop),
            Self::ListStddevSamp => ListStatistic::Dispersion(ListDispersion::StddevSamp),
            Self::ListMedian => ListStatistic::Quantile(ListQuantile::Median),
            Self::ListQuantile => {
                let written = args
                    .next()
                    .verified("the call was checked above to have its percentile argument");
                let percentile = literal_percentile(&written, span)?;
                ListStatistic::Quantile(ListQuantile::Percentile(percentile))
            }
            Self::ListArgmin => ListStatistic::ExtremeIndex(ListExtremum::Least),
            Self::ListArgmax => ListStatistic::ExtremeIndex(ListExtremum::Greatest),
        };
        Ok(Expr::Call {
            function: FunctionName::ListStatistic(statistic),
            args: vec![list],
        })
    }

    /// How many arguments a call of this statistic is written with: the list, and for
    /// `list_quantile` its percentile.
    const fn argument_count(self) -> usize {
        match self {
            Self::ListQuantile => 2,
            Self::ListVarPop
            | Self::ListVarSamp
            | Self::ListStddevPop
            | Self::ListStddevSamp
            | Self::ListMedian
            | Self::ListArgmin
            | Self::ListArgmax => 1,
        }
    }
}

/// The percentile a numeric literal names, as the window percentile aggregates read theirs: an
/// integer or a decimal literal from 0 to 100.
fn literal_percentile(written: &SpannedExpr, span: Span) -> FrontendResult<ListPercentile> {
    let value = match &written.inner {
        Expr::Literal(Literal::Int64(value)) => (*value).approx_into(),
        Expr::Literal(Literal::Float64(value)) => *value,
        _ => {
            return Err(FrontendError::report(
                span,
                FrontendErrorKind::NonLiteralListPercentile,
            ));
        }
    };
    ListPercentile::new(value).ok_or_else(|| {
        FrontendError::report(
            span,
            FrontendErrorKind::ListPercentileOutOfRange { percentile: value },
        )
    })
}
