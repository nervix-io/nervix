//! Layer: engines and infrastructure.
//!
//! - **Owns.** Lowering semantic window assignments into VM expressions and aggregate demands.
//! - **Depends on.** The vocabulary and the VM's program frontend.
//! - **Must not know.** NSPL tokens or diagnostics, registry state, or runtime tasks.

use std::{num::NonZeroUsize, time::Duration};

use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::ApproxInto as _;
use nervix_models::{AssignmentTargetScope, Expression, RouteConstruction};
use sorted_vec::SortedSet;
use thiserror::Error;

pub use crate::program::WindowAggregateFunction;
use crate::{
    frontend::lower_expression,
    program::{
        Expr, FieldRef, FunctionName, Literal, Span, SpannedExpr, SpannedNode,
        WindowAggregateInvocation, spanned,
    },
};

#[derive(Debug, Clone, PartialEq)]
pub struct WindowAggregateProgram {
    pub assignments: Vec<WindowAggregateAssignment>,
    pub demands: Vec<WindowAggregateDemand>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowAggregateAssignment {
    pub target: FieldRef,
    pub value: SpannedNode<WindowAggregateExpr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowAggregateExpr {
    Scalar(SpannedExpr),
    Array(Vec<SpannedNode<WindowAggregateExpr>>),
}

pub type WindowAggregateDemandId = usize;

#[derive(Debug, Clone, PartialEq)]
pub struct WindowLinearHistogramConfig {
    pub buckets: NonZeroUsize,
    pub min: f64,
    pub max: f64,
    pub delay: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WindowAggregateStorageKind {
    Counter,
    Histogram,
    Sequence,
    SortedMap,
    Sum,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowAggregateDemand {
    pub id: WindowAggregateDemandId,
    pub functions: SortedSet<WindowAggregateFunction>,
    pub storage: WindowAggregateStorageKind,
    pub input: Option<Expr>,
    pub linear_histogram: Option<WindowLinearHistogramConfig>,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct WindowAggregateError {
    message: String,
}

pub type WindowAggregateResult<T> = Result<T, Report<WindowAggregateError>>;

fn invalid_window_aggregate(message: impl Into<String>) -> Report<WindowAggregateError> {
    Report::new(WindowAggregateError {
        message: message.into(),
    })
}

impl WindowAggregateFunction {
    fn parse_name(function: &FunctionName) -> Option<Self> {
        if let FunctionName::Udf(_) = function {
            None
        } else {
            function.as_str().parse().ok()
        }
    }

    pub fn storage(self) -> WindowAggregateStorageKind {
        match self {
            Self::Count => WindowAggregateStorageKind::Counter,
            Self::First | Self::Last => WindowAggregateStorageKind::Sequence,
            Self::Max | Self::Min => WindowAggregateStorageKind::SortedMap,
            Self::PercentileLinearHistogram => WindowAggregateStorageKind::Histogram,
            Self::Sum => WindowAggregateStorageKind::Sum,
        }
    }
}

impl WindowAggregateStorageKind {
    pub fn nspl_name(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Histogram => "linear_histogram",
            Self::Sequence => "sequence",
            Self::SortedMap => "sorted_map",
            Self::Sum => "sum",
        }
    }
}

impl WindowAggregateProgram {
    pub fn demands(&self) -> &[WindowAggregateDemand] {
        &self.demands
    }

    pub fn demand_reference_counts(&self) -> Vec<usize> {
        let mut counts = vec![0; self.demands.len()];
        for assignment in &self.assignments {
            assignment
                .value
                .inner
                .collect_demand_references(&mut counts);
        }
        counts
    }

    /// Combines route-local aggregate programs into the single accumulator plan used by a
    /// window processor. Each route keeps its own compiled output program; this combined plan
    /// owns the shared window state and therefore uses globally unique demand identifiers.
    pub fn combine_route_programs(programs: &[Self]) -> Self {
        let mut combined = Self {
            assignments: Vec::new(),
            demands: Vec::new(),
        };
        for program in programs {
            let demand_offset = combined.demands.len();
            combined
                .assignments
                .extend(program.assignments.iter().cloned().map(|mut assignment| {
                    assignment
                        .value
                        .inner
                        .offset_demand_references(demand_offset);
                    assignment
                }));
            combined
                .demands
                .extend(program.demands.iter().cloned().map(|mut demand| {
                    demand.id += demand_offset;
                    demand
                }));
        }
        combined
    }
}

pub fn lower_window_assignments(
    construction: &RouteConstruction,
) -> WindowAggregateResult<SpannedNode<WindowAggregateProgram>> {
    if construction.inherit.is_some() {
        return Err(invalid_window_aggregate(
            "window routes do not support INHERIT",
        ));
    }
    if !construction.invocations.is_empty() {
        return Err(invalid_window_aggregate(
            "window routes do not support INVOKE",
        ));
    }
    let span: Span = (0..0).into();
    let assignments = construction
        .assignments
        .iter()
        .map(|assignment| {
            if !matches!(
                assignment.target.scope,
                AssignmentTargetScope::Bare | AssignmentTargetScope::Output
            ) {
                return Err(invalid_window_aggregate(
                    "window SET targets must be bare or output.<field>",
                ));
            }
            Ok(WindowAggregateAssignment {
                target: FieldRef {
                    relay: "output".to_string(),
                    field: assignment.target.field.as_str().to_string(),
                },
                value: lower_window_expression(&assignment.value, span)?,
            })
        })
        .collect::<WindowAggregateResult<Vec<_>>>()?;
    let mut program = WindowAggregateProgram {
        assignments,
        demands: Vec::new(),
    };
    assign_aggregate_demands(&mut program);
    Ok(spanned(program, span))
}

fn lower_window_expression(
    expression: &Expression,
    span: Span,
) -> WindowAggregateResult<SpannedNode<WindowAggregateExpr>> {
    match expression {
        Expression::Array(items) => {
            if items.is_empty() {
                return Err(invalid_window_aggregate(
                    "window array expressions must not be empty",
                ));
            }
            Ok(spanned(
                WindowAggregateExpr::Array(
                    items
                        .iter()
                        .map(|item| lower_window_expression(item, span))
                        .collect::<WindowAggregateResult<Vec<_>>>()?,
                ),
                span,
            ))
        }
        _ => {
            let expression =
                lower_expression(expression, "output").map_err(invalid_window_aggregate)?;
            validate_aggregate_expr(&expression)?;
            validate_window_input_scope(&expression.inner, false)?;
            Ok(spanned(WindowAggregateExpr::Scalar(expression), span))
        }
    }
}

fn validate_window_input_scope(
    expression: &Expr,
    inside_aggregate: bool,
) -> WindowAggregateResult<()> {
    match expression {
        Expr::FieldRef(field) if inside_aggregate && field.relay != "input" => {
            Err(invalid_window_aggregate(format!(
                "window aggregate arguments may read only input fields, found '{}.{}'",
                field.relay, field.field
            )))
        }
        Expr::FieldRef(field) if field.relay == "input" && !inside_aggregate => {
            Err(invalid_window_aggregate(format!(
                "input.{} is available only inside a window aggregate argument",
                field.field
            )))
        }
        Expr::FieldRef(_) | Expr::InternalFieldRef(_) | Expr::Literal(_) => Ok(()),
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            validate_window_input_scope(&expr.inner, inside_aggregate)
        }
        Expr::Binary { left, right, .. } => {
            validate_window_input_scope(&left.inner, inside_aggregate)?;
            validate_window_input_scope(&right.inner, inside_aggregate)
        }
        Expr::Call { function, args } => {
            let inside_aggregate =
                inside_aggregate || WindowAggregateFunction::parse_name(function).is_some();
            for argument in args {
                validate_window_input_scope(&argument.inner, inside_aggregate)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                validate_window_input_scope(&operand.inner, inside_aggregate)?;
            }
            for branch in branches {
                validate_window_input_scope(&branch.when.inner, inside_aggregate)?;
                validate_window_input_scope(&branch.result.inner, inside_aggregate)?;
            }
            if let Some(else_result) = else_result {
                validate_window_input_scope(&else_result.inner, inside_aggregate)?;
            }
            Ok(())
        }
    }
}

impl WindowAggregateExpr {
    fn collect_demand_references(&self, counts: &mut [usize]) {
        match self {
            Self::Scalar(expr) => collect_expr_demand_references(&expr.inner, counts),
            Self::Array(items) => {
                for item in items {
                    item.inner.collect_demand_references(counts);
                }
            }
        }
    }

    fn offset_demand_references(&mut self, offset: usize) {
        match self {
            Self::Scalar(expr) => offset_expr_demand_references(&mut expr.inner, offset),
            Self::Array(items) => {
                for item in items {
                    item.inner.offset_demand_references(offset);
                }
            }
        }
    }
}

fn offset_expr_demand_references(expr: &mut Expr, offset: usize) {
    match expr {
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            offset_expr_demand_references(&mut expr.inner, offset);
        }
        Expr::Binary { left, right, .. } => {
            offset_expr_demand_references(&mut left.inner, offset);
            offset_expr_demand_references(&mut right.inner, offset);
        }
        Expr::Call { function, args } => {
            if let FunctionName::WindowAggregate(invocation) = function {
                invocation.demand_id += offset;
                return;
            }
            for arg in args {
                offset_expr_demand_references(&mut arg.inner, offset);
            }
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                offset_expr_demand_references(&mut operand.inner, offset);
            }
            for branch in branches {
                offset_expr_demand_references(&mut branch.when.inner, offset);
                offset_expr_demand_references(&mut branch.result.inner, offset);
            }
            if let Some(else_result) = else_result {
                offset_expr_demand_references(&mut else_result.inner, offset);
            }
        }
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => {}
    }
}

fn collect_expr_demand_references(expr: &Expr, counts: &mut [usize]) {
    match expr {
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            collect_expr_demand_references(&expr.inner, counts);
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_demand_references(&left.inner, counts);
            collect_expr_demand_references(&right.inner, counts);
        }
        Expr::Call { function, args } => {
            if let FunctionName::WindowAggregate(invocation) = function {
                if let Some(count) = counts.get_mut(invocation.demand_id) {
                    *count += 1;
                }
                return;
            }
            for arg in args {
                collect_expr_demand_references(&arg.inner, counts);
            }
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_expr_demand_references(&operand.inner, counts);
            }
            for branch in branches {
                collect_expr_demand_references(&branch.when.inner, counts);
                collect_expr_demand_references(&branch.result.inner, counts);
            }
            if let Some(else_result) = else_result {
                collect_expr_demand_references(&else_result.inner, counts);
            }
        }
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => {}
    }
}

fn validate_aggregate_expr(expr: &SpannedExpr) -> WindowAggregateResult<()> {
    match &expr.inner {
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => validate_aggregate_expr(expr),
        Expr::Binary { left, right, .. } => {
            validate_aggregate_expr(left)?;
            validate_aggregate_expr(right)
        }
        Expr::Call { function, args } => {
            if let Some(function) = WindowAggregateFunction::parse_name(function) {
                return validate_aggregate_call(function, args);
            }
            for arg in args {
                validate_aggregate_expr(arg)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                validate_aggregate_expr(operand)?;
            }
            for branch in branches {
                validate_aggregate_expr(&branch.when)?;
                validate_aggregate_expr(&branch.result)?;
            }
            if let Some(else_result) = else_result {
                validate_aggregate_expr(else_result)?;
            }
            Ok(())
        }
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => Ok(()),
    }
}

fn validate_aggregate_call(
    function: WindowAggregateFunction,
    args: &[SpannedExpr],
) -> WindowAggregateResult<()> {
    if args.len() != function.expected_arity() {
        return Err(invalid_window_aggregate(format!(
            "{function:?} expects {} argument(s), found {}",
            function.expected_arity(),
            args.len()
        )));
    }
    if args.iter().any(|arg| contains_aggregate_call(&arg.inner)) {
        return Err(invalid_window_aggregate(
            "aggregate functions must not be nested inside aggregate arguments",
        ));
    }
    if function == WindowAggregateFunction::PercentileLinearHistogram {
        percentile_arg(&args[1])?;
    }
    if function == WindowAggregateFunction::PercentileLinearHistogram {
        linear_histogram_config(args)?;
    }
    Ok(())
}

fn percentile_arg(expr: &SpannedExpr) -> WindowAggregateResult<f64> {
    let value = match &expr.inner {
        Expr::Literal(Literal::Int64(value)) => (*value).approx_into(),
        Expr::Literal(Literal::Float64(value)) => *value,
        _ => {
            return Err(invalid_window_aggregate(
                "PERCENTILE_LINEAR_HISTOGRAM percentile argument must be a numeric constant",
            ));
        }
    };
    if !(0.0..=100.0).contains(&value) {
        return Err(invalid_window_aggregate(
            "PERCENTILE_LINEAR_HISTOGRAM percentile argument must be between 0 and 100",
        ));
    }
    Ok(value)
}

fn linear_histogram_config(
    args: &[SpannedExpr],
) -> WindowAggregateResult<WindowLinearHistogramConfig> {
    let bucket_count = int_arg(&args[2], "bucket count")?;
    let buckets = match usize::try_from(bucket_count) {
        Ok(bucket_count) => NonZeroUsize::new(bucket_count),
        Err(_) => None,
    };
    let Some(buckets) = buckets else {
        return Err(invalid_window_aggregate(
            "PERCENTILE_LINEAR_HISTOGRAM bucket count must be greater than zero",
        ));
    };
    let min = numeric_arg(&args[3], "minimum")?;
    let max = numeric_arg(&args[4], "maximum")?;
    if min >= max {
        return Err(invalid_window_aggregate(
            "PERCENTILE_LINEAR_HISTOGRAM minimum must be less than maximum",
        ));
    }
    let delay = match &args[5].inner {
        Expr::Literal(Literal::String(value)) => value.clone(),
        _ => {
            return Err(invalid_window_aggregate(
                "PERCENTILE_LINEAR_HISTOGRAM delay argument must be a duration string constant",
            ));
        }
    };
    let delay = humantime::parse_duration(&delay).map_err(|error| {
        invalid_window_aggregate(format!(
            "invalid PERCENTILE_LINEAR_HISTOGRAM delay duration '{delay}': {error}"
        ))
    })?;
    Ok(WindowLinearHistogramConfig {
        buckets,
        min,
        max,
        delay,
    })
}

fn int_arg(expr: &SpannedExpr, name: &str) -> WindowAggregateResult<i64> {
    match &expr.inner {
        Expr::Literal(Literal::Int64(value)) => Ok(*value),
        _ => Err(invalid_window_aggregate(format!(
            "PERCENTILE_LINEAR_HISTOGRAM {name} argument must be an integer constant"
        ))),
    }
}

fn numeric_arg(expr: &SpannedExpr, name: &str) -> WindowAggregateResult<f64> {
    match &expr.inner {
        Expr::Literal(Literal::Int64(value)) => Ok((*value).approx_into()),
        Expr::Literal(Literal::Float64(value)) => Ok(*value),
        _ => Err(invalid_window_aggregate(format!(
            "PERCENTILE_LINEAR_HISTOGRAM {name} argument must be a numeric constant"
        ))),
    }
}

fn contains_aggregate_call(expr: &Expr) -> bool {
    match expr {
        Expr::Call { function, args } => {
            WindowAggregateFunction::parse_name(function).is_some()
                || args.iter().any(|arg| contains_aggregate_call(&arg.inner))
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => contains_aggregate_call(&expr.inner),
        Expr::Binary { left, right, .. } => {
            contains_aggregate_call(&left.inner) || contains_aggregate_call(&right.inner)
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            operand
                .as_ref()
                .is_some_and(|operand| contains_aggregate_call(&operand.inner))
                || branches.iter().any(|branch| {
                    contains_aggregate_call(&branch.when.inner)
                        || contains_aggregate_call(&branch.result.inner)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|result| contains_aggregate_call(&result.inner))
        }
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => false,
    }
}

fn assign_aggregate_demands(program: &mut WindowAggregateProgram) {
    program.demands.clear();
    for assignment in &mut program.assignments {
        assign_expr_demands(&mut assignment.value.inner, &mut program.demands);
    }
}

fn assign_expr_demands(expr: &mut WindowAggregateExpr, demands: &mut Vec<WindowAggregateDemand>) {
    match expr {
        WindowAggregateExpr::Scalar(expr) => assign_vm_expr_demands(expr, demands),
        WindowAggregateExpr::Array(items) => {
            for item in items {
                assign_expr_demands(&mut item.inner, demands);
            }
        }
    }
}

fn assign_vm_expr_demands(expr: &mut SpannedExpr, demands: &mut Vec<WindowAggregateDemand>) {
    match &mut expr.inner {
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            assign_vm_expr_demands(expr, demands);
        }
        Expr::Binary { left, right, .. } => {
            assign_vm_expr_demands(left, demands);
            assign_vm_expr_demands(right, demands);
        }
        Expr::Call { function, args } => {
            let Some(aggregate_function) = WindowAggregateFunction::parse_name(function) else {
                for arg in args {
                    assign_vm_expr_demands(arg, demands);
                }
                return;
            };
            let percentile =
                if aggregate_function == WindowAggregateFunction::PercentileLinearHistogram {
                    Some(percentile_arg(&args[1]).verified(
                        "aggregate validation checked these same arguments before the demand pass",
                    ))
                } else {
                    None
                };
            let linear_histogram =
                if aggregate_function == WindowAggregateFunction::PercentileLinearHistogram {
                    Some(linear_histogram_config(args).verified(
                        "aggregate validation checked these same arguments before the demand pass",
                    ))
                } else {
                    None
                };
            let demand = aggregate_demand_for_call(
                aggregate_function,
                args,
                linear_histogram,
                demands.len(),
            );
            let demand_id = if let Some(existing) = demands
                .iter_mut()
                .find(|candidate| demand_matches(candidate, &demand))
            {
                existing.functions.find_or_insert(aggregate_function);
                existing.id
            } else {
                let id = demands.len();
                demands.push(WindowAggregateDemand { id, ..demand });
                id
            };
            *function = FunctionName::WindowAggregate(WindowAggregateInvocation {
                demand_id,
                function: aggregate_function,
                percentile,
            });
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                assign_vm_expr_demands(operand, demands);
            }
            for branch in branches {
                assign_vm_expr_demands(&mut branch.when, demands);
                assign_vm_expr_demands(&mut branch.result, demands);
            }
            if let Some(else_result) = else_result {
                assign_vm_expr_demands(else_result, demands);
            }
        }
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => {}
    }
}

fn aggregate_demand_for_call(
    function: WindowAggregateFunction,
    args: &[SpannedExpr],
    linear_histogram: Option<WindowLinearHistogramConfig>,
    id: WindowAggregateDemandId,
) -> WindowAggregateDemand {
    let input = Some(
        args.first()
            .verified("aggregate validation above enforces the function's nonzero arity")
            .inner
            .clone(),
    );
    WindowAggregateDemand {
        id,
        functions: SortedSet::from_unsorted(vec![function]),
        storage: function.storage(),
        input,
        linear_histogram,
    }
}

fn demand_matches(left: &WindowAggregateDemand, right: &WindowAggregateDemand) -> bool {
    left.storage == right.storage
        && left.input == right.input
        && left.linear_histogram == right.linear_histogram
}

pub fn referenced_field_refs(expr: &WindowAggregateExpr) -> Vec<&FieldRef> {
    let mut refs = Vec::new();
    collect_referenced_field_refs(expr, &mut refs);
    refs
}

fn collect_referenced_field_refs<'a>(expr: &'a WindowAggregateExpr, refs: &mut Vec<&'a FieldRef>) {
    match expr {
        WindowAggregateExpr::Scalar(expr) => collect_expr_field_refs(&expr.inner, refs),
        WindowAggregateExpr::Array(items) => {
            for item in items {
                collect_referenced_field_refs(&item.inner, refs);
            }
        }
    }
}

fn collect_expr_field_refs<'a>(expr: &'a Expr, refs: &mut Vec<&'a FieldRef>) {
    match expr {
        Expr::FieldRef(field_ref) => refs.push(field_ref),
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => {
            collect_expr_field_refs(&expr.inner, refs);
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_field_refs(&left.inner, refs);
            collect_expr_field_refs(&right.inner, refs);
        }
        Expr::Call { args, .. } => {
            for arg in args {
                collect_expr_field_refs(&arg.inner, refs);
            }
        }
        Expr::Case {
            operand,
            branches,
            else_result,
        } => {
            if let Some(operand) = operand {
                collect_expr_field_refs(&operand.inner, refs);
            }
            for branch in branches {
                collect_expr_field_refs(&branch.when.inner, refs);
                collect_expr_field_refs(&branch.result.inner, refs);
            }
            if let Some(else_result) = else_result {
                collect_expr_field_refs(&else_result.inner, refs);
            }
        }
        Expr::Literal(_) | Expr::InternalFieldRef(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use nonzero_ext::nonzero;

    use super::*;

    fn lower_aggregate_program(
        assignments: &str,
    ) -> Result<SpannedNode<WindowAggregateProgram>, String> {
        let construction = nervix_nspl::parse_route_construction(&format!("SET {assignments}"))
            .map_err(|error| error.to_string())?;
        lower_window_assignments(&construction).map_err(|error| error.to_string())
    }

    #[test]
    fn parses_aggregate_program_and_demands() {
        let parsed = lower_aggregate_program(
            "latency_p99 = PERCENTILE_LINEAR_HISTOGRAM(abs(input.latency), 99, 2048, 0, 10000, \
             '2s'), time = MAX(input.timestamp), started_at = FIRST(input.timestamp), latencies = \
             [PERCENTILE_LINEAR_HISTOGRAM(input.latency, 90, 2048, 0, 10000, '2s'), \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 95, 2048, 0, 10000, '2s')]",
        )
        .expect("aggregate program should parse");

        assert_eq!(parsed.assignments.len(), 4);
        let demands = parsed.demands();
        assert_eq!(demands.len(), 4);
        assert_eq!(demands[0].storage, WindowAggregateStorageKind::Histogram);
        assert_eq!(demands[0].id, 0);
        assert_eq!(demands[1].storage, WindowAggregateStorageKind::SortedMap);
        assert_eq!(demands[2].storage, WindowAggregateStorageKind::Sequence);
        assert_eq!(demands[3].storage, WindowAggregateStorageKind::Histogram);
    }

    #[test]
    fn conditional_window_results_collect_all_aggregate_demands() {
        let parsed = lower_aggregate_program(
            "result = CASE WHEN COUNT(input.value) > 0 THEN SUM(input.value) ELSE 0 END",
        )
        .expect("conditional aggregate expression must parse");

        assert_eq!(parsed.demands().len(), 2);
        assert_eq!(parsed.demand_reference_counts(), vec![1, 1]);
    }

    #[test]
    fn route_aggregate_arguments_reject_output_fields() {
        let construction = nervix_nspl::parse_route_construction("SET total = COUNT(output.total)")
            .expect("route construction should parse");

        let error = lower_window_assignments(&construction)
            .expect_err("aggregate arguments must read the original input");

        assert!(
            error
                .to_string()
                .contains("window aggregate arguments may read only input fields")
        );
    }

    #[test]
    fn route_array_values_preserve_ordered_aggregate_expressions() {
        let construction = nervix_nspl::parse_route_construction(
            "SET percentiles = [MIN(input.value), MAX(input.value)]",
        )
        .expect("route array construction should parse");

        let lowered = lower_window_assignments(&construction)
            .expect("window array construction should lower");

        assert_eq!(lowered.demands().len(), 1);
        let WindowAggregateExpr::Array(items) = &lowered.assignments[0].value.inner else {
            panic!("window array must remain structurally represented");
        };
        assert_eq!(items.len(), 2);
        assert_eq!(
            aggregate_invocation(&items[0].inner).function,
            WindowAggregateFunction::Min
        );
        assert_eq!(
            aggregate_invocation(&items[1].inner).function,
            WindowAggregateFunction::Max
        );
    }

    #[test]
    fn deduplicates_demands_and_assigns_call_demand_ids() {
        let parsed = lower_aggregate_program(
            "p50 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 2048, 0, 10000, '2s'), p90 = \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 90, 2048, 0, 10000, '2s')",
        )
        .expect("aggregate program should parse");

        assert_eq!(parsed.demands().len(), 1);
        let first = aggregate_invocation(&parsed.assignments[0].value.inner);
        let second = aggregate_invocation(&parsed.assignments[1].value.inner);
        assert_eq!(first.demand_id, 0);
        assert_eq!(second.demand_id, 0);
        assert_eq!(parsed.demand_reference_counts(), vec![2]);
    }

    #[test]
    fn counts_references_for_nested_array_aggregate_demands() {
        let parsed = lower_aggregate_program(
            "latencies = [PERCENTILE_LINEAR_HISTOGRAM(input.latency, 50, 2048, 0, 10000, '2s'), \
             PERCENTILE_LINEAR_HISTOGRAM(input.latency, 90, 2048, 0, 10000, '2s')], count = \
             COUNT(input.latency)",
        )
        .expect("aggregate program should parse");

        assert_eq!(parsed.demands().len(), 2);
        assert_eq!(parsed.demand_reference_counts(), vec![2, 1]);
    }

    #[test]
    fn minimizes_structures_across_compatible_aggregate_functions() {
        let parsed = lower_aggregate_program(
            "first = FIRST(input.value), last = LAST(input.value), min = MIN(input.value), max = \
             MAX(input.value)",
        )
        .expect("compatible aggregate functions should share online structures");

        assert_eq!(parsed.demands().len(), 2);
        assert_eq!(
            parsed.demands()[0].functions.as_slice(),
            &[
                WindowAggregateFunction::First,
                WindowAggregateFunction::Last,
            ]
        );
        assert_eq!(
            parsed.demands()[1].functions.as_slice(),
            &[WindowAggregateFunction::Max, WindowAggregateFunction::Min,]
        );
        assert_eq!(parsed.demand_reference_counts(), vec![2, 2]);
    }

    #[test]
    fn rejects_non_constant_percentile() {
        lower_aggregate_program(
            "p = PERCENTILE_LINEAR_HISTOGRAM(input.latency, input.rank, 2048, 0, 10000, '2s')",
        )
        .expect_err("percentile must be constant");
    }

    #[test]
    fn parses_linear_histogram_percentile_config() {
        let parsed = lower_aggregate_program(
            "p99 = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 99, 2048, 0, 10000, '2s')",
        )
        .expect("aggregate program should parse");

        let call = aggregate_invocation(&parsed.assignments[0].value.inner);
        assert_eq!(
            call.function,
            WindowAggregateFunction::PercentileLinearHistogram
        );
        assert_eq!(call.percentile, Some(99.0));
        assert_eq!(
            parsed.demands()[0].linear_histogram,
            Some(WindowLinearHistogramConfig {
                buckets: nonzero!(2048usize),
                min: 0.0,
                max: 10000.0,
                delay: Duration::from_secs(2),
            })
        );
    }

    #[test]
    fn rejects_invalid_linear_histogram_config() {
        lower_aggregate_program(
            "p = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 99, 0, 0, 10000, '2s')",
        )
        .expect_err("bucket count must be positive");
        lower_aggregate_program(
            "p = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 99, 2048, 10000, 0, '2s')",
        )
        .expect_err("range must be ordered");
        lower_aggregate_program(
            "p = PERCENTILE_LINEAR_HISTOGRAM(input.latency, 99, 2048, 0, 10000, input.delay)",
        )
        .expect_err("delay must be constant");
    }

    #[test]
    fn rejects_nested_aggregate_calls() {
        lower_aggregate_program("p = SUM(COUNT(input.latency))")
            .expect_err("aggregate calls must not be nested");
    }

    #[test]
    fn parses_aggregate_calls_inside_vm_expressions() {
        let parsed = lower_aggregate_program(
            "adjusted_count = COUNT(input.latency) + 2, adjusted_p99 = \
             ABS(PERCENTILE_LINEAR_HISTOGRAM(input.latency, 99, 2048, 0, 10000, '2s'))",
        )
        .expect("aggregate calls should be valid inside VM expressions");

        assert_eq!(parsed.demands().len(), 2);
        assert_eq!(parsed.demand_reference_counts(), vec![1, 1]);
    }

    #[test]
    fn exposes_referenced_field_refs() {
        let parsed = lower_aggregate_program(
            "p = PERCENTILE_LINEAR_HISTOGRAM(abs(input.latency), 99, 2048, 0, 10000, '2s')",
        )
        .expect("aggregate program should parse");
        let refs = referenced_field_refs(&parsed.assignments[0].value.inner);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].relay, "input");
        assert_eq!(refs[0].field, "latency");
    }

    #[test]
    fn combines_route_programs_with_globally_unique_demands() {
        let first = lower_aggregate_program("output.count = COUNT(input.value)")
            .expect("first route aggregate should parse");
        let second = lower_aggregate_program(
            "output.minimum = MIN(input.value), output.maximum = MAX(input.value)",
        )
        .expect("second route aggregate should parse");

        let combined = WindowAggregateProgram::combine_route_programs(&[
            first.inner.clone(),
            second.inner.clone(),
        ]);

        assert_eq!(combined.demands().len(), 2);
        assert_eq!(combined.demands()[0].id, 0);
        assert_eq!(combined.demands()[1].id, 1);
        assert_eq!(combined.demand_reference_counts(), vec![1, 2]);
        assert_eq!(
            aggregate_invocation(&combined.assignments[1].value.inner).demand_id,
            1
        );
        assert_eq!(
            aggregate_invocation(&combined.assignments[2].value.inner).demand_id,
            1
        );
    }

    fn aggregate_invocation(expr: &WindowAggregateExpr) -> &WindowAggregateInvocation {
        let WindowAggregateExpr::Scalar(expr) = expr else {
            panic!("expected scalar aggregate expression");
        };
        let Expr::Call {
            function: FunctionName::WindowAggregate(invocation),
            ..
        } = &expr.inner
        else {
            panic!("expected aggregate invocation");
        };
        invocation
    }
}
