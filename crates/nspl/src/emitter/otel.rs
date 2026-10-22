//! OTEL emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and connector-specific clauses of OTEL emitters.
//! - **Depends on.** Shared emitter grammar, lexical primitives and the connector vocabulary.
//! - **Must not know.** Connector drivers, graph planning or runtime publication.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the language boundary declared above")
)]

use super::*;

pub(super) fn otel_temporality_parser<'src>()
-> impl Parser<'src, &'src [Token], OtelAggregationTemporality, extra::Err<ParseError<'src>>> + Clone
{
    choice((
        kw(Identifier::Delta).to(OtelAggregationTemporality::Delta),
        kw(Identifier::Cumulative).to(OtelAggregationTemporality::Cumulative),
    ))
}

pub(super) fn otel_metric_kind_parser<'src>()
-> impl Parser<'src, &'src [Token], OtelMetricKind, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Gauge).to(OtelMetricKind::Gauge),
        kw(Identifier::Sum)
            .ignore_then(kw(Identifier::Monotonic).or_not())
            .then(otel_temporality_parser())
            .map(|(monotonic, temporality)| OtelMetricKind::Sum {
                monotonic: monotonic.is_some(),
                temporality,
            }),
        kw(Identifier::Histogram)
            .ignore_then(otel_temporality_parser())
            .map(|temporality| OtelMetricKind::Histogram { temporality }),
    ))
}

/// The signal an OTEL sink body spells after its client, which decides the entity the sink is.
#[derive(Debug, Clone)]
pub(super) enum OtelSignal {
    Logs,
    Traces,
    Metric(OtelMetric),
}

pub(super) fn otel_signal_parser<'src>()
-> impl Parser<'src, &'src [Token], OtelSignal, extra::Err<ParseError<'src>>> + Clone {
    choice((
        kw(Identifier::Logs).to(OtelSignal::Logs),
        kw(Identifier::Traces).to(OtelSignal::Traces),
        kw(Identifier::Metric)
            .ignore_then(string_lit().labelled("otel_metric_name"))
            .then_ignore(kw(Identifier::Unit))
            .then(string_lit().labelled("otel_metric_unit"))
            .then(
                kw(Identifier::Description)
                    .ignore_then(string_lit().labelled("otel_metric_description"))
                    .or_not(),
            )
            .then(otel_metric_kind_parser())
            .map(|(((name, unit), description), kind)| {
                OtelSignal::Metric(OtelMetric {
                    name,
                    unit,
                    description,
                    kind,
                })
            }),
    ))
}

pub(super) fn otel_literal_expression(expression: &nervix_models::Expression) -> bool {
    match expression {
        nervix_models::Expression::Literal(_) => true,
        nervix_models::Expression::Array(items) => items.iter().all(otel_literal_expression),
        nervix_models::Expression::Field(_)
        | nervix_models::Expression::Unary { .. }
        | nervix_models::Expression::Binary { .. }
        | nervix_models::Expression::Cast { .. }
        | nervix_models::Expression::Call { .. }
        | nervix_models::Expression::UdfCall { .. }
        | nervix_models::Expression::If { .. }
        | nervix_models::Expression::Case { .. }
        | nervix_models::Expression::Membership { .. }
        | nervix_models::Expression::Range { .. }
        | nervix_models::Expression::TryCast { .. }
        | nervix_models::Expression::JsonValue { .. }
        | nervix_models::Expression::TryJsonValue { .. }
        | nervix_models::Expression::JsonExists { .. } => false,
    }
}

pub(super) fn otel_resource_values<'src>()
-> impl Parser<'src, &'src [Token], Vec<ClickHouseValueMapping>, extra::Err<ParseError<'src>>> + Clone
{
    clickhouse_values().try_map(|values, span| {
        if values
            .iter()
            .all(|mapping| otel_literal_expression(&mapping.expression))
        {
            Ok(values)
        } else {
            Err(Rich::custom(
                span,
                "OTEL RESOURCE values must be literal expressions",
            ))
        }
    })
}

pub(super) fn otel_scope_parser<'src>()
-> impl Parser<'src, &'src [Token], OtelScope, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Scope)
        .ignore_then(string_lit().labelled("otel_scope_name"))
        .then(
            kw(Identifier::Version)
                .ignore_then(string_lit().labelled("otel_scope_version"))
                .or_not(),
        )
        .map(|(name, version)| OtelScope { name, version })
}

pub(super) fn otel_emit_sink_parser<'src>()
-> impl Parser<'src, &'src [Token], EmitSink, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Otel)
        .ignore_then(client_ref())
        .then(otel_signal_parser())
        .then_ignore(kw(Identifier::Values))
        .then(clickhouse_values())
        .then(
            kw(Identifier::Attributes)
                .ignore_then(clickhouse_values())
                .or_not(),
        )
        .then(
            kw(Identifier::Resource)
                .ignore_then(otel_resource_values())
                .or_not(),
        )
        .then(otel_scope_parser().or_not())
        .map(
            |(((((client, signal), values), attributes), resource), scope)| {
                let mappings = OtelMappings {
                    values,
                    attributes: attributes.unwrap_or_default(),
                    resource: resource.unwrap_or_default(),
                    scope,
                };
                match signal {
                    OtelSignal::Logs => EmitSink::from(OtelLogsSink { client, mappings }),
                    OtelSignal::Traces => EmitSink::from(OtelTracesSink { client, mappings }),
                    OtelSignal::Metric(metric) => EmitSink::from(OtelMetricsSink {
                        client,
                        metric,
                        mappings,
                    }),
                }
            },
        )
}
