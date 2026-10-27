//! OTEL emitter grammar in Plexus.
//!
//! Layer: language.
//! - **Owns.** The destination body and typed publication grammar of OTEL emitters.
//! - **Depends on.** Shared emitter clauses and this connector's declared entity Models.
//! - **Must not know.** Connector drivers, execution planning or runtime publication.

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
fn metric_locator<'src>()
-> impl Parser<'src, &'src [Token], OtelMetric, extra::Err<ParseError<'src>>> + Clone {
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
        .map(|(((name, unit), description), kind)| OtelMetric {
            name,
            unit,
            description,
            kind,
        })
        .boxed()
}
fn mappings<'src>()
-> impl Parser<'src, &'src [Token], OtelMappings, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Values)
        .ignore_then(clickhouse_values())
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
        .map(|(((values, attributes), resource), scope)| OtelMappings {
            values,
            attributes: attributes.unwrap_or_default(),
            resource: resource.unwrap_or_default(),
            scope,
        })
        .boxed()
}
pub(super) fn logs_mode<'src>()
-> impl Parser<'src, &'src [Token], OtelLogsPublishingMode, extra::Err<ParseError<'src>>> + Clone {
    request_mode!(OtelLogsPublishingMode)
}
pub(super) fn logs_body<'src>()
-> impl Parser<'src, &'src [Token], ParsedDestination<OtelLogsEmitter>, extra::Err<ParseError<'src>>>
+ Clone {
    values_destination(
        client_ref()
            .then_ignore(kw(Identifier::Mode))
            .then(logs_mode())
            .then(mappings())
            .map(|((client, mode), mappings)| OtelLogsEmitter {
                client,
                mode,
                mappings,
            }),
    )
}
pub(super) fn traces_mode<'src>()
-> impl Parser<'src, &'src [Token], OtelTracesPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    request_mode!(OtelTracesPublishingMode)
}
pub(super) fn traces_body<'src>()
-> impl Parser<'src, &'src [Token], ParsedDestination<OtelTracesEmitter>, extra::Err<ParseError<'src>>>
+ Clone {
    values_destination(
        client_ref()
            .then_ignore(kw(Identifier::Mode))
            .then(traces_mode())
            .then(mappings())
            .map(|((client, mode), mappings)| OtelTracesEmitter {
                client,
                mode,
                mappings,
            }),
    )
}
pub(super) fn metrics_mode<'src>()
-> impl Parser<'src, &'src [Token], OtelMetricsPublishingMode, extra::Err<ParseError<'src>>> + Clone
{
    request_mode!(OtelMetricsPublishingMode)
}
pub(super) fn metrics_body<'src>()
-> impl Parser<'src, &'src [Token], ParsedDestination<OtelMetricsEmitter>, extra::Err<ParseError<'src>>>
+ Clone {
    values_destination(
        client_ref()
            .then(metric_locator())
            .then_ignore(kw(Identifier::Mode))
            .then(metrics_mode())
            .then(mappings())
            .map(|(((client, metric), mode), mappings)| OtelMetricsEmitter {
                client,
                metric,
                mode,
                mappings,
            }),
    )
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "checks the OTEL declaration grammar")
)]
mod tests {
    //! Layer: test harness.
    //! - **Owns.** Parsing and mapping order for every declared OTEL signal field.
    //! - **Depends on.** The composed emitter parser and public field declarations.
    //! - **Must not know.** Registry decisions, OTLP encoding or runtime publication.

    use meticulous::ResultExt as _;
    use nervix_models::{
        OTEL_HISTOGRAM_VALUE_FIELDS, OTEL_LOG_VALUE_FIELDS, OTEL_NUMBER_VALUE_FIELDS,
        OTEL_TRACE_VALUE_FIELDS, OtelValueField,
    };

    #[test]
    fn every_declared_otel_value_key_parses_and_keeps_its_written_order() {
        struct Shape {
            signal: &'static str,
            metric: &'static str,
            fields: &'static [OtelValueField],
        }
        let shapes = [
            Shape {
                signal: "LOGS",
                metric: "",
                fields: OTEL_LOG_VALUE_FIELDS,
            },
            Shape {
                signal: "TRACES",
                metric: "",
                fields: OTEL_TRACE_VALUE_FIELDS,
            },
            Shape {
                signal: "METRICS",
                metric: "METRIC 'events' UNIT '1' GAUGE",
                fields: OTEL_NUMBER_VALUE_FIELDS,
            },
            Shape {
                signal: "METRICS",
                metric: "METRIC 'events' UNIT '1' SUM MONOTONIC DELTA",
                fields: OTEL_NUMBER_VALUE_FIELDS,
            },
            Shape {
                signal: "METRICS",
                metric: "METRIC 'events' UNIT '1' HISTOGRAM CUMULATIVE",
                fields: OTEL_HISTOGRAM_VALUE_FIELDS,
            },
        ];
        for Shape {
            signal,
            metric,
            fields,
        } in shapes
        {
            let values: Vec<_> = fields
                .iter()
                .map(|field| format!("'{}' = input.{}", field.key, field.key))
                .collect();
            let text = format!(
                "CREATE OTEL {signal} EMITTER outgoing FROM events TO collector {metric} MODE ACK \
                 RETRY POLICY BACKOFF 1s MAX 30s VALUES {{ {} }} ATTRIBUTES {{'nested' = [true, \
                 false]}} RESOURCE {{'service.name' = 'nervix'}} SCOPE 'nervix' VERSION '1' FLUSH \
                 IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
                values.join(", ")
            );
            let emitter = crate::emitter::parse_create_emitter(&text)
                .assured("every declared signal field belongs to its VALUES grammar");
            let nervix_models::EmitterEntity::Otel(connector) = emitter.connector.as_ref() else {
                panic!("the selected OTEL grammar produces its connector");
            };
            let keys: Vec<_> = connector
                .mappings()
                .values
                .iter()
                .map(|mapping| mapping.column.as_str())
                .collect();
            let expected: Vec<_> = fields.iter().map(|field| field.key).collect();
            assert_eq!(keys, expected);
        }
    }
}
