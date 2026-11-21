//! What a window has to declare before its routes may call a collection aggregate.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The bound each collection aggregate needs from its window, whether a declared window
//!   has it, and the aggregates the value a window route assigns may call where it is written.
//! - **Depends on.** The vocabulary's window bounds, builtin names and completion scope, and the
//!   window aggregate declaration.
//! - **Must not know.** Plexus tokens or diagnostics, registry state, sessions or runtime tasks.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns the collection bounds declared above")
)]

use nervix_models::{BuiltinFunctionName, WindowBound, WindowRouteValue, WindowStateLimit};
use strum::IntoEnumIterator as _;

use crate::program::{FunctionName, WindowAggregateFunction};

/// What a window has to declare before its routes may call an aggregate that returns the values
/// the window retains, rather than one value computed from them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowCollectionBound {
    /// `WIDTH <n> MESSAGES` with no duration, whose count is the length of every collection.
    ExactMessageCount,
    /// A message `WIDTH` or a `MAX STATE SIZE`, which bounds the rows a collection returns.
    RetainedRows,
}

impl WindowCollectionBound {
    /// Whether a window declared with `width` and `state_limit` has this bound.
    pub fn is_declared_by(self, width: &WindowBound, state_limit: WindowStateLimit) -> bool {
        match self {
            Self::ExactMessageCount => width.fixed_message_width().is_some(),
            Self::RetainedRows => {
                if width.messages.is_some() {
                    return true;
                }
                match state_limit {
                    WindowStateLimit::MaxBytes(_) => true,
                    WindowStateLimit::Unbounded => false,
                }
            }
        }
    }

    /// The first of `functions` that needs this bound, when a window declared with `width` and
    /// `state_limit` does not have it.
    pub fn first_unmet<'a>(
        self,
        functions: impl IntoIterator<Item = &'a WindowAggregateFunction>,
        width: &WindowBound,
        state_limit: WindowStateLimit,
    ) -> Option<WindowAggregateFunction> {
        if self.is_declared_by(width, state_limit) {
            return None;
        }
        for function in functions {
            if function.collection_bound() == Some(self) {
                return Some(*function);
            }
        }
        None
    }
}

impl WindowAggregateFunction {
    /// The bound a window has to declare before its routes may call this aggregate, when the
    /// aggregate returns the values the window retains.
    pub const fn collection_bound(self) -> Option<WindowCollectionBound> {
        match self {
            Self::CollectArray => Some(WindowCollectionBound::ExactMessageCount),
            Self::CollectVec => Some(WindowCollectionBound::RetainedRows),
            Self::ApproxCountDistinct
            | Self::ApproxQuantile
            | Self::ApproxTopK
            | Self::ArgMax
            | Self::ArgMin
            | Self::Avg
            | Self::BoolAnd
            | Self::BoolOr
            | Self::Corr
            | Self::Count
            | Self::CountIf
            | Self::CovarPop
            | Self::CovarSamp
            | Self::First
            | Self::Last
            | Self::Max
            | Self::Min
            | Self::PercentileLinearHistogram
            | Self::StddevPop
            | Self::StddevSamp
            | Self::Sum
            | Self::VarPop
            | Self::VarSamp => None,
        }
    }

    /// The aggregate a window route reads a call of `name` as, the way its lowering reads the call.
    pub fn called(name: &BuiltinFunctionName) -> Option<Self> {
        Self::parse_name(&FunctionName::parse(name.as_str()))
    }

    /// Spellings of the aggregates the value a window route assigns may call at `route`: each
    /// collection aggregate whose bound the window declares, and none inside an aggregate's
    /// argument, since aggregates do not nest. The ordinary functions stay offered beside them.
    pub fn route_completion_names(route: &WindowRouteValue) -> Vec<String> {
        for call in &route.enclosing_calls {
            if Self::called(call).is_some() {
                return Vec::new();
            }
        }
        let mut names = Vec::new();
        for function in Self::iter() {
            let Some(bound) = function.collection_bound() else {
                continue;
            };
            if bound.is_declared_by(&route.width, route.state_limit) {
                names.push(function.plexus_name().to_string());
            }
        }
        names
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "collection bound and completion witnesses")
)]
mod tests {
    //! Layer: harness.
    //! - **Owns.** The collection bounds each window shape declares, and the aggregates a window
    //!   route's value offers under each bound and nesting.
    //! - **Depends on.** The production declaration and completion owner.
    //! - **Must not know.** Graph acceptance or session resolution.

    use std::num::NonZeroU64;

    use meticulous::{OptionExt as _, ResultExt as _};

    use super::*;

    fn count() -> WindowBound {
        WindowBound::of_messages(3)
    }

    fn duration() -> WindowBound {
        WindowBound::of_duration("10s".to_string())
    }

    fn combined() -> WindowBound {
        WindowBound {
            messages: Some(3),
            duration: Some("10s".to_string()),
        }
    }

    fn limited() -> WindowStateLimit {
        WindowStateLimit::MaxBytes(NonZeroU64::new(1024).assured("1024 is positive"))
    }

    fn call(name: &str) -> BuiltinFunctionName {
        BuiltinFunctionName::parse(name).assured("the test names a valid builtin")
    }

    fn route(
        width: WindowBound,
        state_limit: WindowStateLimit,
        enclosing: &[&str],
    ) -> WindowRouteValue {
        WindowRouteValue {
            width,
            state_limit,
            enclosing_calls: enclosing.iter().map(|name| call(name)).collect(),
        }
    }

    #[test]
    fn only_the_collection_aggregates_need_a_collection_bound() {
        let mut bound = Vec::new();
        for function in WindowAggregateFunction::iter() {
            if let Some(needed) = function.collection_bound() {
                bound.push((function, needed));
            }
        }
        assert_eq!(
            bound,
            [
                (
                    WindowAggregateFunction::CollectArray,
                    WindowCollectionBound::ExactMessageCount
                ),
                (
                    WindowAggregateFunction::CollectVec,
                    WindowCollectionBound::RetainedRows
                ),
            ]
        );
    }

    #[test]
    fn each_window_shape_declares_the_bounds_its_rule_names() {
        let unbounded = WindowStateLimit::Unbounded;
        let exact = WindowCollectionBound::ExactMessageCount;
        let retained = WindowCollectionBound::RetainedRows;
        assert!(exact.is_declared_by(&count(), unbounded));
        assert!(exact.is_declared_by(&count(), limited()));
        assert!(!exact.is_declared_by(&WindowBound::of_messages(0), limited()));
        assert!(!exact.is_declared_by(&duration(), limited()));
        assert!(!exact.is_declared_by(&combined(), limited()));
        assert!(retained.is_declared_by(&count(), unbounded));
        assert!(retained.is_declared_by(&combined(), unbounded));
        assert!(retained.is_declared_by(&duration(), limited()));
        assert!(!retained.is_declared_by(&duration(), unbounded));
    }

    #[test]
    fn the_first_unmet_collection_is_named_only_when_its_bound_is_missing() {
        let functions = [
            WindowAggregateFunction::Count,
            WindowAggregateFunction::CollectVec,
            WindowAggregateFunction::CollectArray,
        ];
        let retained = WindowCollectionBound::RetainedRows;
        let exact = WindowCollectionBound::ExactMessageCount;
        assert_eq!(
            retained.first_unmet(&functions, &duration(), WindowStateLimit::Unbounded),
            Some(WindowAggregateFunction::CollectVec)
        );
        assert_eq!(
            retained.first_unmet(&functions, &duration(), limited()),
            None
        );
        assert_eq!(
            exact.first_unmet(&functions, &combined(), limited()),
            Some(WindowAggregateFunction::CollectArray)
        );
        assert_eq!(
            exact.first_unmet(&functions, &count(), WindowStateLimit::Unbounded),
            None
        );
        assert_eq!(
            exact.first_unmet(
                &[WindowAggregateFunction::Count],
                &duration(),
                WindowStateLimit::Unbounded
            ),
            None
        );
    }

    #[test]
    fn a_window_route_value_offers_the_collections_its_window_declares() {
        struct Case {
            width: WindowBound,
            state_limit: WindowStateLimit,
            offered: &'static [&'static str],
        }
        let cases = [
            Case {
                width: count(),
                state_limit: WindowStateLimit::Unbounded,
                offered: &["COLLECT_ARRAY", "COLLECT_VEC"],
            },
            Case {
                width: count(),
                state_limit: limited(),
                offered: &["COLLECT_ARRAY", "COLLECT_VEC"],
            },
            Case {
                width: duration(),
                state_limit: WindowStateLimit::Unbounded,
                offered: &[],
            },
            Case {
                width: duration(),
                state_limit: limited(),
                offered: &["COLLECT_VEC"],
            },
            Case {
                width: combined(),
                state_limit: WindowStateLimit::Unbounded,
                offered: &["COLLECT_VEC"],
            },
        ];
        for case in cases {
            let described = format!("{:?} with {:?}", case.width, case.state_limit);
            let offered = WindowAggregateFunction::route_completion_names(&route(
                case.width,
                case.state_limit,
                &[],
            ));
            assert_eq!(offered, case.offered, "{described}");
        }
    }

    #[test]
    fn an_aggregate_argument_offers_no_aggregate() {
        let window = |enclosing: &[&str]| {
            WindowAggregateFunction::route_completion_names(&route(
                count(),
                WindowStateLimit::Unbounded,
                enclosing,
            ))
        };
        assert_eq!(window(&["list_flatten"]), ["COLLECT_ARRAY", "COLLECT_VEC"]);
        assert_eq!(
            window(&["list_flatten", "abs"]),
            ["COLLECT_ARRAY", "COLLECT_VEC"]
        );
        assert!(window(&["COLLECT_VEC"]).is_empty());
        assert!(window(&["collect_array"]).is_empty());
        assert!(window(&["list_flatten", "count"]).is_empty());
        assert!(window(&["avg", "abs"]).is_empty());
    }

    #[test]
    fn a_call_is_an_aggregate_exactly_where_a_window_route_reads_one() {
        assert_eq!(
            WindowAggregateFunction::called(&call("collect_vec")),
            Some(WindowAggregateFunction::CollectVec)
        );
        assert_eq!(
            WindowAggregateFunction::called(&call("COUNT")),
            Some(WindowAggregateFunction::Count)
        );
        assert_eq!(
            WindowAggregateFunction::called(&call("Percentile_Linear_Histogram")),
            Some(WindowAggregateFunction::PercentileLinearHistogram)
        );
        for scalar in ["list_median", "list_flatten", "coalesce", "abs", "date_bin"] {
            assert_eq!(
                WindowAggregateFunction::called(&call(scalar)),
                None,
                "{scalar}"
            );
        }
    }
}
