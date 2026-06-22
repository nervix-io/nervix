use std::{ops::Range, time::Duration};

use chumsky::{
    input::{Stream, ValueInput},
    prelude::*,
};

use crate::vm_program::{
    Diagnostic, Expr, FieldRef, FunctionName, Literal, ParseError, ParseFromSourceError, Span,
    SpannedExpr, SpannedNode, SpannedToken, Token, expr_parser, field_ref_parser, lex,
};

fn spanned<T>(inner: T, span: Span) -> SpannedNode<T> {
    chumsky::span::Spanned { inner, span }
}

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
    AggregateCall(WindowAggregateCall),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowAggregateCall {
    pub function: WindowAggregateFunction,
    pub args: Vec<SpannedExpr>,
    pub demand_id: WindowAggregateDemandId,
    pub percentile: Option<f64>,
    pub linear_histogram: Option<WindowLinearHistogramConfig>,
}

pub type WindowAggregateDemandId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WindowAggregateFunction {
    Count,
    First,
    Last,
    Max,
    Min,
    PercentileLinearHistogram,
    Sum,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowLinearHistogramConfig {
    pub buckets: usize,
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
    pub function: WindowAggregateFunction,
    pub storage: WindowAggregateStorageKind,
    pub input: Option<Expr>,
    pub linear_histogram: Option<WindowLinearHistogramConfig>,
}

impl WindowAggregateFunction {
    pub fn nspl_name(self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::First => "FIRST",
            Self::Last => "LAST",
            Self::Max => "MAX",
            Self::Min => "MIN",
            Self::PercentileLinearHistogram => "PERCENTILE_LINEAR_HISTOGRAM",
            Self::Sum => "SUM",
        }
    }

    fn parse_name(function: &FunctionName) -> Option<Self> {
        match function.as_str().to_ascii_lowercase().as_str() {
            "count" => Some(Self::Count),
            "first" => Some(Self::First),
            "last" => Some(Self::Last),
            "max" => Some(Self::Max),
            "min" => Some(Self::Min),
            "percentile_linear_histogram" => Some(Self::PercentileLinearHistogram),
            "sum" => Some(Self::Sum),
            _ => None,
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

    fn expected_arity(self) -> usize {
        match self {
            Self::PercentileLinearHistogram => 6,
            Self::Count | Self::First | Self::Last | Self::Max | Self::Min | Self::Sum => 1,
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
}

impl WindowAggregateExpr {
    fn collect_demand_references(&self, counts: &mut [usize]) {
        match self {
            Self::Scalar(_) => {}
            Self::Array(items) => {
                for item in items {
                    item.inner.collect_demand_references(counts);
                }
            }
            Self::AggregateCall(call) => {
                if let Some(count) = counts.get_mut(call.demand_id) {
                    *count += 1;
                }
            }
        }
    }
}

pub fn parse_aggregate_program(
    input: &str,
) -> Result<SpannedNode<WindowAggregateProgram>, ParseFromSourceError> {
    let source = input.to_string();
    let tokens = lex(input).map_err(|errors| ParseFromSourceError::Lex {
        source: source.clone(),
        diagnostics: errors
            .into_iter()
            .map(|error| Diagnostic {
                message: format!("{error:?}"),
                span: error.span().into_range(),
            })
            .collect(),
    })?;

    parse_aggregate_tokens(&tokens).map_err(|errors| ParseFromSourceError::Parse {
        source,
        diagnostics: errors
            .into_iter()
            .map(|error| Diagnostic {
                message: format!("{error:?}"),
                span: error.span().into_range(),
            })
            .collect(),
    })
}

pub fn parse_aggregate_tokens(
    tokens: &[SpannedToken],
) -> Result<SpannedNode<WindowAggregateProgram>, Vec<ParseError<'_>>> {
    let end_span = tokens
        .last()
        .map(|token| token.span.end..token.span.end)
        .unwrap_or(0..0);
    let relay = Stream::from_iter(
        tokens
            .iter()
            .cloned()
            .map(|token| (token.token, token.span)),
    )
    .map(end_span.into(), |(token, span)| (token, span));

    let parsed = aggregate_parser().then_ignore(end()).parse(relay);
    if parsed.has_errors() {
        Err(parsed.into_errors())
    } else {
        let mut program = parsed
            .into_output()
            .expect("successful parse must contain an aggregate program");
        assign_aggregate_demands(&mut program.inner);
        Ok(program)
    }
}

fn aggregate_parser<'src, I>()
-> impl Parser<'src, I, SpannedNode<WindowAggregateProgram>, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    aggregate_assignment()
        .separated_by(just(Token::Comma))
        .at_least(1)
        .allow_trailing()
        .collect::<Vec<_>>()
        .then_ignore(just(Token::Semicolon).repeated())
        .map_with(|assignments, e| {
            spanned(
                WindowAggregateProgram {
                    assignments,
                    demands: Vec::new(),
                },
                e.span(),
            )
        })
}

fn aggregate_assignment<'src, I>()
-> impl Parser<'src, I, WindowAggregateAssignment, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    field_ref_parser()
        .map(|target| target.inner)
        .then_ignore(just(Token::Eq))
        .then(aggregate_expr())
        .map(|(target, value)| WindowAggregateAssignment { target, value })
}

fn aggregate_expr<'src, I>()
-> impl Parser<'src, I, SpannedNode<WindowAggregateExpr>, extra::Err<ParseError<'src>>> + Clone
where
    I: ValueInput<'src, Token = Token, Span = Span>,
{
    recursive(|aggregate_expr| {
        let array = aggregate_expr
            .clone()
            .separated_by(just(Token::Comma))
            .allow_trailing()
            .collect::<Vec<_>>()
            .delimited_by(just(Token::LBracket), just(Token::RBracket))
            .try_map(|items, span: Span| {
                if items.is_empty() {
                    return Err(Rich::custom(span, "aggregate arrays must not be empty"));
                }
                Ok(spanned(WindowAggregateExpr::Array(items), span))
            });

        choice((array, expr_parser().try_map(aggregate_expr_from_vm_expr)))
    })
}

fn aggregate_expr_from_vm_expr<'src>(
    expr: SpannedExpr,
    span: Span,
) -> Result<SpannedNode<WindowAggregateExpr>, Rich<'src, Token>> {
    if let Expr::Call { function, args: _ } = &expr.inner
        && legacy_percentile_name(function)
    {
        return Err(Rich::custom(
            span,
            "PERCENTILE is not supported; use PERCENTILE_LINEAR_HISTOGRAM",
        ));
    }
    if let Expr::Call { function, args } = &expr.inner
        && let Some(function) = WindowAggregateFunction::parse_name(function)
    {
        validate_aggregate_call(function, args, span)?;
        let percentile = if function == WindowAggregateFunction::PercentileLinearHistogram {
            Some(percentile_arg(&args[1], span)?)
        } else {
            None
        };
        let linear_histogram = if function == WindowAggregateFunction::PercentileLinearHistogram {
            Some(linear_histogram_config(args, span)?)
        } else {
            None
        };
        return Ok(spanned(
            WindowAggregateExpr::AggregateCall(WindowAggregateCall {
                function,
                args: args.clone(),
                demand_id: 0,
                percentile,
                linear_histogram,
            }),
            expr.span,
        ));
    }

    if contains_aggregate_call(&expr.inner) {
        return Err(Rich::custom(
            span,
            "aggregate functions must be top-level aggregate values or array elements",
        ));
    }
    Ok(spanned(WindowAggregateExpr::Scalar(expr), span))
}

fn validate_aggregate_call<'src>(
    function: WindowAggregateFunction,
    args: &[SpannedExpr],
    span: Span,
) -> Result<(), Rich<'src, Token>> {
    if args.len() != function.expected_arity() {
        return Err(Rich::custom(
            span,
            format!(
                "{function:?} expects {} argument(s), found {}",
                function.expected_arity(),
                args.len()
            ),
        ));
    }
    if args.iter().any(|arg| contains_aggregate_call(&arg.inner)) {
        return Err(Rich::custom(
            span,
            "aggregate functions must not be nested inside aggregate arguments",
        ));
    }
    if function == WindowAggregateFunction::PercentileLinearHistogram {
        percentile_arg(&args[1], span)?;
    }
    if function == WindowAggregateFunction::PercentileLinearHistogram {
        linear_histogram_config(args, span)?;
    }
    Ok(())
}

fn percentile_arg<'src>(expr: &SpannedExpr, span: Span) -> Result<f64, Rich<'src, Token>> {
    let value = match &expr.inner {
        Expr::Literal(Literal::Int64(value)) => *value as f64,
        Expr::Literal(Literal::Float64(value)) => *value,
        _ => {
            return Err(Rich::custom(
                span,
                "PERCENTILE_LINEAR_HISTOGRAM percentile argument must be a numeric constant",
            ));
        }
    };
    if !(0.0..=100.0).contains(&value) {
        return Err(Rich::custom(
            span,
            "PERCENTILE_LINEAR_HISTOGRAM percentile argument must be between 0 and 100",
        ));
    }
    Ok(value)
}

fn linear_histogram_config<'src>(
    args: &[SpannedExpr],
    span: Span,
) -> Result<WindowLinearHistogramConfig, Rich<'src, Token>> {
    let buckets = int_arg(&args[2], span, "bucket count")?;
    if buckets <= 0 {
        return Err(Rich::custom(
            span,
            "PERCENTILE_LINEAR_HISTOGRAM bucket count must be greater than zero",
        ));
    }
    let min = numeric_arg(&args[3], span, "minimum")?;
    let max = numeric_arg(&args[4], span, "maximum")?;
    if min >= max {
        return Err(Rich::custom(
            span,
            "PERCENTILE_LINEAR_HISTOGRAM minimum must be less than maximum",
        ));
    }
    let delay = match &args[5].inner {
        Expr::Literal(Literal::String(value)) => value.clone(),
        _ => {
            return Err(Rich::custom(
                span,
                "PERCENTILE_LINEAR_HISTOGRAM delay argument must be a duration string constant",
            ));
        }
    };
    let delay = humantime::parse_duration(&delay).map_err(|error| {
        Rich::custom(
            span,
            format!("invalid PERCENTILE_LINEAR_HISTOGRAM delay duration '{delay}': {error}"),
        )
    })?;
    Ok(WindowLinearHistogramConfig {
        buckets: buckets as usize,
        min,
        max,
        delay,
    })
}

fn int_arg<'src>(expr: &SpannedExpr, span: Span, name: &str) -> Result<i64, Rich<'src, Token>> {
    match &expr.inner {
        Expr::Literal(Literal::Int64(value)) => Ok(*value),
        _ => Err(Rich::custom(
            span,
            format!("PERCENTILE_LINEAR_HISTOGRAM {name} argument must be an integer constant"),
        )),
    }
}

fn numeric_arg<'src>(expr: &SpannedExpr, span: Span, name: &str) -> Result<f64, Rich<'src, Token>> {
    match &expr.inner {
        Expr::Literal(Literal::Int64(value)) => Ok(*value as f64),
        Expr::Literal(Literal::Float64(value)) => Ok(*value),
        _ => Err(Rich::custom(
            span,
            format!("PERCENTILE_LINEAR_HISTOGRAM {name} argument must be a numeric constant"),
        )),
    }
}

fn contains_aggregate_call(expr: &Expr) -> bool {
    match expr {
        Expr::Call { function, args } => {
            WindowAggregateFunction::parse_name(function).is_some()
                || legacy_percentile_name(function)
                || args.iter().any(|arg| contains_aggregate_call(&arg.inner))
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } => contains_aggregate_call(&expr.inner),
        Expr::Binary { left, right, .. } => {
            contains_aggregate_call(&left.inner) || contains_aggregate_call(&right.inner)
        }
        Expr::Literal(_) | Expr::FieldRef(_) | Expr::InternalFieldRef(_) => false,
    }
}

fn legacy_percentile_name(function: &FunctionName) -> bool {
    function.as_str().eq_ignore_ascii_case("percentile")
}

fn assign_aggregate_demands(program: &mut WindowAggregateProgram) {
    program.demands.clear();
    for assignment in &mut program.assignments {
        assign_expr_demands(&mut assignment.value.inner, &mut program.demands);
    }
}

fn assign_expr_demands(expr: &mut WindowAggregateExpr, demands: &mut Vec<WindowAggregateDemand>) {
    match expr {
        WindowAggregateExpr::Scalar(_) => {}
        WindowAggregateExpr::Array(items) => {
            for item in items {
                assign_expr_demands(&mut item.inner, demands);
            }
        }
        WindowAggregateExpr::AggregateCall(call) => {
            let demand = aggregate_demand_for_call(call, demands.len());
            let demand_id = demands
                .iter()
                .position(|candidate| demand_matches(candidate, &demand))
                .unwrap_or_else(|| {
                    let id = demands.len();
                    demands.push(WindowAggregateDemand { id, ..demand });
                    id
                });
            call.demand_id = demand_id;
        }
    }
}

fn aggregate_demand_for_call(
    call: &WindowAggregateCall,
    id: WindowAggregateDemandId,
) -> WindowAggregateDemand {
    let input = if call.function == WindowAggregateFunction::Count {
        None
    } else {
        Some(
            call.args
                .first()
                .expect("aggregate call must carry its validated input argument")
                .inner
                .clone(),
        )
    };
    WindowAggregateDemand {
        id,
        function: call.function,
        storage: call.function.storage(),
        input,
        linear_histogram: call.linear_histogram.clone(),
    }
}

fn demand_matches(left: &WindowAggregateDemand, right: &WindowAggregateDemand) -> bool {
    left.function == right.function
        && left.storage == right.storage
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
        WindowAggregateExpr::AggregateCall(call) => {
            for arg in &call.args {
                collect_expr_field_refs(&arg.inner, refs);
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
        Expr::Literal(_) | Expr::InternalFieldRef(_) => {}
    }
}

pub fn span_range(span: Span) -> Range<usize> {
    span.into_range()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_aggregate_program_and_demands() {
        let parsed = parse_aggregate_program(
            "s2.latency_p99 = PERCENTILE_LINEAR_HISTOGRAM(abs(s1.latency), 99, 2048, 0, 10000, \
             '2s'), s2.time = MAX(s1.timestamp), s2.started_at = FIRST(s1.timestamp), \
             s2.latencies = [PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 90, 2048, 0, 10000, '2s'), \
             PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 95, 2048, 0, 10000, '2s')]",
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
    fn deduplicates_demands_and_assigns_call_demand_ids() {
        let parsed = parse_aggregate_program(
            "s2.p50 = PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 50, 2048, 0, 10000, '2s'), s2.p90 = \
             PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 90, 2048, 0, 10000, '2s')",
        )
        .expect("aggregate program should parse");

        assert_eq!(parsed.demands().len(), 1);
        let WindowAggregateExpr::AggregateCall(first) = &parsed.assignments[0].value.inner else {
            panic!("expected first aggregate call");
        };
        let WindowAggregateExpr::AggregateCall(second) = &parsed.assignments[1].value.inner else {
            panic!("expected second aggregate call");
        };
        assert_eq!(first.demand_id, 0);
        assert_eq!(second.demand_id, 0);
        assert_eq!(parsed.demand_reference_counts(), vec![2]);
    }

    #[test]
    fn counts_references_for_nested_array_aggregate_demands() {
        let parsed = parse_aggregate_program(
            "s2.latencies = [PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 50, 2048, 0, 10000, '2s'), \
             PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 90, 2048, 0, 10000, '2s')], s2.count = \
             COUNT(s1.latency)",
        )
        .expect("aggregate program should parse");

        assert_eq!(parsed.demands().len(), 2);
        assert_eq!(parsed.demand_reference_counts(), vec![2, 1]);
    }

    #[test]
    fn rejects_non_constant_percentile() {
        parse_aggregate_program(
            "s2.p = PERCENTILE_LINEAR_HISTOGRAM(s1.latency, s1.rank, 2048, 0, 10000, '2s')",
        )
        .expect_err("percentile must be constant");
    }

    #[test]
    fn rejects_legacy_percentile_function() {
        parse_aggregate_program("s2.p = PERCENTILE(s1.latency, 99)")
            .expect_err("legacy percentile must be rejected");
    }

    #[test]
    fn parses_linear_histogram_percentile_config() {
        let parsed = parse_aggregate_program(
            "s2.p99 = PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 99, 2048, 0, 10000, '2s')",
        )
        .expect("aggregate program should parse");

        let WindowAggregateExpr::AggregateCall(call) = &parsed.assignments[0].value.inner else {
            panic!("expected aggregate call");
        };
        assert_eq!(
            call.function,
            WindowAggregateFunction::PercentileLinearHistogram
        );
        assert_eq!(call.percentile, Some(99.0));
        assert_eq!(
            call.linear_histogram,
            Some(WindowLinearHistogramConfig {
                buckets: 2048,
                min: 0.0,
                max: 10000.0,
                delay: Duration::from_secs(2),
            })
        );
    }

    #[test]
    fn rejects_invalid_linear_histogram_config() {
        parse_aggregate_program(
            "s2.p = PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 99, 0, 0, 10000, '2s')",
        )
        .expect_err("bucket count must be positive");
        parse_aggregate_program(
            "s2.p = PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 99, 2048, 10000, 0, '2s')",
        )
        .expect_err("range must be ordered");
        parse_aggregate_program(
            "s2.p = PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 99, 2048, 0, 10000, s1.delay)",
        )
        .expect_err("delay must be constant");
    }

    #[test]
    fn rejects_nested_aggregate_calls() {
        parse_aggregate_program(
            "s2.p = lower(PERCENTILE_LINEAR_HISTOGRAM(s1.latency, 99, 2048, 0, 10000, '2s'))",
        )
        .expect_err("aggregate calls must not be nested");
    }

    #[test]
    fn exposes_referenced_field_refs() {
        let parsed = parse_aggregate_program(
            "s2.p = PERCENTILE_LINEAR_HISTOGRAM(abs(s1.latency), 99, 2048, 0, 10000, '2s')",
        )
        .expect("aggregate program should parse");
        let refs = referenced_field_refs(&parsed.assignments[0].value.inner);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].relay, "s1");
        assert_eq!(refs[0].field, "latency");
    }
}
