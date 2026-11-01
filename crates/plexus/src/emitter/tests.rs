//! Existing emitter parsing and completion checks.
//!
//! Layer: test harness.
//! - **Owns.** Examples that exercise the composed emitter grammar and its semantic output.
//! - **Depends on.** The emitter grammar, shared parser support and public vocabulary.
//! - **Must not know.** Connector drivers or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_models::{
    ClickHouseEmitter, ClickHouseTableEmitter, FlushPolicy, HttpEmitter, HttpRequestEmitter,
    IcebergEmitter, IcebergTableEmitter, KafkaEmitter, MongoDbCollectionEmitter, MongoDbEmitter,
    MqttEmitter, MySqlEmitter, MySqlTableEmitter, NatsEmitter, OtelEmitter, OtelLogsEmitter,
    OtelMetricsEmitter, PostgresEmitter, PostgresTableEmitter, PulsarEmitter, RabbitMqEmitter,
    RedisEmitter, SqsEmitter, SqsQueueEmitter, SyslogEmitter, ZeroMqEmitter,
};

use super::*;
use crate::lexer::lex;

#[test]
fn http_emitter_body_modes_roundtrip_with_qualified_request_fields() {
    for body in [
        "ENCODE USING body_codec INHERIT ALL",
        "WITHOUT BODY INVOKE write_header('X-Request', input.method)",
    ] {
        let source = format!(
            "CREATE HTTP REQUEST EMITTER send FROM source TO api METHOD input.method PATH \
             input.path MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s {body} FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
        let parsed = parse_create_emitter(&source).expect("HTTP emitter should parse");
        let canonical = parsed
            .to_canonical_plexus()
            .expect("HTTP emitter should format");
        let reparsed = parse_create_emitter(&canonical).expect("canonical HTTP should parse");
        assert_eq!(parsed, reparsed);
    }
}

#[test]
fn fields_named_like_the_request_clauses_stay_in_the_http_request() {
    let parsed = parse_create_emitter(
        "CREATE HTTP REQUEST EMITTER send FROM source TO api METHOD method PATH path MODE ACK \
         RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY INVOKE write_header('X-Mode', mode) \
         FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
    )
    .expect("bare method and path fields complete their expressions");

    let EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter { method, path, .. })) =
        parsed.connector.as_ref()
    else {
        panic!("the emitter must be HTTP, got {:?}", parsed.connector);
    };
    assert_eq!(
        method,
        &crate::parse_expression("method").expect("valid expression")
    );
    assert_eq!(
        path,
        &crate::parse_expression("path").expect("valid expression")
    );

    let input = "CREATE HTTP REQUEST EMITTER send FROM source TO api METHOD method ";
    let suggestions = suggest_create_emitter(input, input.len());
    assert!(suggestions.contains(&"PATH".to_string()), "{suggestions:?}");
    let input = "CREATE HTTP REQUEST EMITTER send FROM source TO api METHOD method PATH path ";
    let suggestions = suggest_create_emitter(input, input.len());
    assert!(suggestions.contains(&"MODE".to_string()), "{suggestions:?}");
}

#[test]
fn fields_named_like_keywords_stay_in_a_fifo_group_and_values() {
    let parsed = parse_create_emitter(&complete_codec_emitter(
        "SQS QUEUE EMITTER emit FROM p99 TO broker QUEUE 'events.fifo' FIFO GROUP \
         concat(input.mode, mode) SEND SINGLE MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
    ))
    .expect("a bare mode completes the group before MODE");
    let EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
        fifo_group: Some(nervix_models::SqsFifoGroup::Expression(group)),
        ..
    })) = parsed.connector.as_ref()
    else {
        panic!(
            "the emitter must be SQS with a group expression, got {:?}",
            parsed.connector
        );
    };
    assert_eq!(
        group,
        &crate::parse_expression("concat(input.mode, mode)").expect("valid expression")
    );

    let parsed = parse_create_emitter(
        "CREATE CLICKHOUSE TABLE EMITTER emit FROM p99 TO db TABLE 'events' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s VALUES { 'to' = to, 'end' = input.end, 'from' = `from` } BATCH MAX \
         MESSAGES 100 MAX SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
    )
    .expect("values named like keywords stay in their entries");
    let EmitterEntity::ClickHouse(ClickHouseEmitter::Table(ClickHouseTableEmitter {
        values, ..
    })) = parsed.connector.as_ref()
    else {
        panic!("the emitter must be ClickHouse, got {:?}", parsed.connector);
    };
    let expressions = values
        .iter()
        .map(|mapping| mapping.expression.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        expressions,
        crate::parse_expression_list("to, input.end, `from`").expect("valid expressions")
    );
}

#[test]
fn a_path_opening_with_a_parenthesis_still_begins_after_the_method() {
    let parsed = parse_create_emitter(
        "CREATE HTTP REQUEST EMITTER send FROM source TO api METHOD input.method PATH \
         (input.prefix + input.tenant) + input.path MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s \
         WITHOUT BODY FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
    )
    .expect("PATH takes an expression, so the parenthesis after it opens that expression");

    let EmitterEntity::Http(HttpEmitter::Request(HttpRequestEmitter { method, path, .. })) =
        parsed.connector.as_ref()
    else {
        panic!("the emitter must be HTTP, got {:?}", parsed.connector);
    };
    assert_eq!(
        method,
        &crate::parse_expression("input.method").expect("valid expression")
    );
    assert_eq!(
        path,
        &crate::parse_expression("(input.prefix + input.tenant) + input.path")
            .expect("valid expression")
    );
}

#[test]
fn http_emitter_requires_ordered_request_and_body_clauses() {
    for connector in [
        "HTTP REQUEST EMITTER emit FROM p99 TO api PATH '/' MODE ACK RETRY POLICY BACKOFF 250ms \
         MAX 30s WITHOUT BODY",
        "HTTP REQUEST EMITTER emit FROM p99 TO api METHOD 'GET' MODE ACK RETRY POLICY BACKOFF \
         250ms MAX 30s WITHOUT BODY",
        "HTTP REQUEST EMITTER emit FROM p99 TO api METHOD 'GET' PATH '/' MODE NO ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s WITHOUT BODY",
        "HTTP REQUEST EMITTER emit FROM p99 TO api METHOD 'GET' PATH '/' MODE ACK SEQUENTIAL ACK \
         TIMEOUT 1s RETRY POLICY BACKOFF 250ms MAX 30s WITHOUT BODY",
        "HTTP REQUEST EMITTER emit FROM p99 TO api METHOD 'GET' PATH '/' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s",
        "HTTP REQUEST EMITTER emit FROM p99 TO api METHOD 'GET' PATH '/' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s WITHOUT BODY INHERIT ALL",
        "HTTP REQUEST EMITTER emit FROM p99 TO api METHOD 'GET' PATH '/' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s WITHOUT BODY SET id = input.id",
        "HTTP REQUEST EMITTER emit FROM p99 TO api METHOD 'GET' PATH '/' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s WITHOUT BODY VALUES { 'id' = input.id }",
        "HTTP REQUEST EMITTER emit FROM p99 TO api METHOD 'GET' PATH '/' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s ENCODE USING body_codec WITHOUT BODY",
        "HTTP REQUEST EMITTER emit FROM p99 TO api METHOD 'GET' PATH '/' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s WITHOUT BODY BATCH MAX MESSAGES 2 MAX SIZE 1MiB",
    ] {
        let source = format!(
            "CREATE {connector} FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
        assert!(parse_create_emitter(&source).is_err(), "{source}");
    }
}

#[test]
fn http_emitter_completion_follows_clause_order() {
    let prefix = "CREATE HTTP REQUEST EMITTER send FROM source TO api ";
    assert_eq!(suggest_create_emitter(prefix, prefix.len()), vec!["METHOD"]);
    let mode = format!("{prefix}METHOD 'POST' PATH input.path MODE ");
    assert_eq!(suggest_create_emitter(&mode, mode.len()), vec!["ACK"]);
    let body = format!("{mode}ACK RETRY POLICY BACKOFF 250ms MAX 30s ");
    let suggestions = suggest_create_emitter(&body, body.len());
    assert!(suggestions.contains(&"ENCODE USING".to_string()));
    assert!(suggestions.contains(&"WITHOUT BODY".to_string()));
    assert!(!suggestions.contains(&"BATCH".to_string()));
}

#[test]
fn http_alter_emitter_roundtrips_both_explicit_body_selections() {
    for body in ["ENCODE USING body_codec", "WITHOUT BODY"] {
        let source = format!(
            "ALTER HTTP REQUEST EMITTER send SET TO api METHOD input.method PATH input.path MODE \
             ACK RETRY POLICY BACKOFF 250ms MAX 30s {body};"
        );
        let parsed = parse_alter_emitter(&source).expect("HTTP replacement should parse");
        let canonical = parsed
            .to_canonical_plexus()
            .expect("HTTP replacement should format");
        let reparsed = parse_alter_emitter(&canonical).expect("canonical replacement should parse");
        assert_eq!(parsed, reparsed);
    }
}

#[test]
fn http_request_expressions_keep_clause_words_inside_qualified_fields_and_groups() {
    let source = "CREATE HTTP REQUEST EMITTER send FROM source TO api METHOD input.path PATH \
                  coalesce(input.mode, 'MODE ACK') MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s \
                  WITHOUT BODY WHERE input.path = '/ready' INVOKE write_header('X-Method', \
                  input.method) FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;";
    let parsed = parse_create_emitter(source).expect("qualified clause words should parse");
    let canonical = parsed
        .to_canonical_plexus()
        .expect("request expressions should format");
    assert_eq!(parse_create_emitter(&canonical).expect("roundtrip"), parsed);
}

fn to_tokens(input: &str) -> Vec<Token> {
    lex(input)
        .expect("lexer should succeed")
        .into_iter()
        .map(|t| t.token)
        .collect()
}

fn otel_emitter(signal: &str, values: &str, tail: &str) -> String {
    let (entity, option) = match signal {
        "LOGS" | "TRACES" => (signal, ""),
        _ => ("METRICS", signal),
    };
    format!(
        "CREATE OTEL {entity} EMITTER telemetry FROM telemetry_relay TO otel_main {option} MODE \
         ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {{{values}}} {tail} FLUSH IMMEDIATE ON \
         MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
    )
}

#[test]
fn parses_otel_logs_traces_and_metric_shapes() {
    let logs = parse_create_emitter(&otel_emitter(
        "LOGS",
        "'time' = input.event_ts, 'body' = input.message",
        "ATTRIBUTES {'service.instance.id' = input.instance_id} RESOURCE {'service.name' = \
         'checkout'} SCOPE 'nervix/audit' VERSION '1.0'",
    ))
    .expect("OTEL logs emitter should parse");
    let EmitterEntity::Otel(OtelEmitter::Logs(OtelLogsEmitter { mappings, .. })) =
        logs.connector.as_ref()
    else {
        panic!("expected OTEL logs emitter");
    };
    assert_eq!(mappings.attributes.len(), 1);
    assert_eq!(mappings.resource.len(), 1);
    assert_eq!(
        mappings.scope.as_ref().map(|scope| scope.name.as_str()),
        Some("nervix/audit")
    );

    let traces = parse_create_emitter(&otel_emitter(
        "TRACES",
        "'trace_id' = input.trace_id, 'span_id' = input.span_id, 'name' = input.name, \
         'start_time' = input.started_at, 'end_time' = input.finished_at",
        "",
    ))
    .expect("OTEL traces emitter should parse");
    assert!(matches!(
        traces.connector.as_ref(),
        EmitterEntity::Otel(OtelEmitter::Traces(_))
    ));

    let gauge = parse_create_emitter(&otel_emitter(
        "METRIC 'queue.depth' UNIT '1' DESCRIPTION 'Pending work' GAUGE",
        "'time' = input.observed_at, 'value' = input.depth",
        "",
    ))
    .expect("OTEL gauge emitter should parse");
    assert!(matches!(
        gauge.connector.as_ref(),
        EmitterEntity::Otel(OtelEmitter::Metrics(OtelMetricsEmitter {
            metric: OtelMetric {
                kind: OtelMetricKind::Gauge,
                ..
            },
            ..
        }))
    ));

    let sum = parse_create_emitter(&otel_emitter(
        "METRIC 'http.requests' UNIT '1' SUM MONOTONIC DELTA",
        "'time' = input.finished_at, 'start_time' = input.started_at, 'value' = input.count",
        "",
    ))
    .expect("OTEL sum emitter should parse");
    assert!(matches!(
        sum.connector.as_ref(),
        EmitterEntity::Otel(OtelEmitter::Metrics(OtelMetricsEmitter {
            metric: OtelMetric {
                kind: OtelMetricKind::Sum {
                    monotonic: true,
                    temporality: OtelAggregationTemporality::Delta,
                },
                ..
            },
            ..
        }))
    ));

    let histogram = parse_create_emitter(&otel_emitter(
        "METRIC 'http.duration' UNIT 'ms' HISTOGRAM CUMULATIVE",
        "'time' = input.observed_at, 'count' = input.count, 'bucket_counts' = \
         input.bucket_counts, 'explicit_bounds' = input.bounds",
        "",
    ))
    .expect("OTEL histogram emitter should parse");
    assert!(matches!(
        histogram.connector.as_ref(),
        EmitterEntity::Otel(OtelEmitter::Metrics(OtelMetricsEmitter {
            metric: OtelMetric {
                kind: OtelMetricKind::Histogram {
                    temporality: OtelAggregationTemporality::Cumulative,
                },
                ..
            },
            ..
        }))
    ));
}

#[test]
fn rejects_invalid_otel_emitter_clause_surfaces() {
    assert!(
        parse_create_emitter(
            "CREATE OTEL LOGS EMITTER telemetry FROM telemetry_relay TO otel_main  MODE ACK RETRY \
             POLICY BACKOFF 250ms MAX 30s FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR \
             LOG;"
        )
        .is_err()
    );
    assert!(
        parse_create_emitter(&otel_emitter(
            "LOGS",
            "'time' = input.event_ts, 'body' = input.message",
            "RESOURCE {'service.name' = input.service_name}",
        ))
        .is_err()
    );
    assert!(
        parse_create_emitter(
            "CREATE OTEL LOGS EMITTER telemetry FROM telemetry_relay TO otel_main  MODE NO ACK \
             RETRY POLICY BACKOFF 250ms MAX 30s VALUES {'time' = input.event_ts, 'body' = \
             input.message} FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
        .is_err()
    );
    assert!(
        parse_create_emitter(
            "CREATE OTEL LOGS EMITTER telemetry FROM telemetry_relay TO otel_main  MODE ACK RETRY \
             POLICY BACKOFF 250ms MAX 30s VALUES {'time' = input.event_ts, 'body' = \
             input.message} ENCODE USING telemetry_codec FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
             GENERAL ERROR LOG;"
        )
        .is_err()
    );
}

#[test]
fn otel_completion_stays_within_the_selected_signal_grammar() {
    let logs = "CREATE OTEL LOGS EMITTER telemetry FROM events TO collector ";
    assert_eq!(suggest_create_emitter(logs, logs.len()), vec!["MODE"]);
    let metric = "CREATE OTEL METRICS EMITTER telemetry FROM events TO collector ";
    assert_eq!(suggest_create_emitter(metric, metric.len()), vec!["METRIC"]);
    let values = "CREATE OTEL LOGS EMITTER telemetry FROM events TO collector MODE ACK RETRY \
                  POLICY BACKOFF 1s MAX 2s VALUES {'body' = input.body} ";
    let suggestions = suggest_create_emitter(values, values.len());
    for clause in ["ATTRIBUTES", "RESOURCE", "SCOPE"] {
        assert!(suggestions.contains(&clause.to_string()), "{suggestions:?}");
    }
}

#[test]
fn parses_alter_emitter_operations_in_written_order() {
    let parsed = parse_alter_emitter(
        "ALTER ZEROMQ PUSH EMITTER event_emitter ADD FROM backup WHERE input.kind = 'backup', \
         ALTER FROM backup SET WHERE input.kind = 'current', ALTER FROM backup DROP WHERE, DROP \
         FROM backup, SET TO emitter_b MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE \
         USING event_codec, SET CLIENT emitter_c, SET ENCODE USING event_codec, DROP ENCODE, SET \
         COLLECT FOR 10ms MAX BATCH SIZE 1MiB, DROP COLLECT, SET DETACHED, SET FLUSH IMMEDIATE;",
    )
    .expect("kinded ALTER should parse");

    assert_eq!(parsed.operations.len(), 12);
    assert!(matches!(
        parsed.operations[0],
        AlterEmitterOperation::AddFrom { .. }
    ));
    assert!(matches!(
        parsed.operations[1],
        AlterEmitterOperation::AlterFromSetWhere { .. }
    ));
    assert!(matches!(
        parsed.operations[2],
        AlterEmitterOperation::AlterFromDropWhere { .. }
    ));
    assert!(matches!(
        parsed.operations[3],
        AlterEmitterOperation::DropFrom { .. }
    ));
    assert!(matches!(
        parsed.operations[4],
        AlterEmitterOperation::SetTo {
            ref destination,
            ..
        } if matches!(destination.as_ref(), EmitterEntity::ZeroMq(_))
    ));
    assert!(matches!(
        parsed.operations[5],
        AlterEmitterOperation::SetClient { .. }
    ));
    assert!(matches!(
        parsed.operations[6],
        AlterEmitterOperation::SetEncodeUsing { .. }
    ));
    assert_eq!(parsed.operations[7], AlterEmitterOperation::DropEncode);
    assert!(matches!(
        parsed.operations[8],
        AlterEmitterOperation::SetCollect { .. }
    ));
    assert_eq!(parsed.operations[9], AlterEmitterOperation::DropCollect);
    assert_eq!(
        parsed.operations[10],
        AlterEmitterOperation::SetAttachment {
            mode: AckMode::Detached
        }
    );
    assert_eq!(
        parsed.operations[11],
        AlterEmitterOperation::SetFlush {
            flush_policy: FlushPolicy::Immediate,
        }
    );
}

#[test]
fn parses_alter_emitter_direct_emitter_values_and_iceberg_commit_policy() {
    let direct = parse_alter_emitter(
        "ALTER POSTGRES TABLE EMITTER event_emitter SET TO postgres_main TABLE 'events' MODE ACK \
         RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'seq' = concat(input.kind, ','), 'value' = \
         input.value } , SET BATCH MAX MESSAGES 100 MAX SIZE 1MiB, SET FLUSH EACH 1s MAX BATCH \
         SIZE 1MiB;",
    )
    .expect("direct emitter expressions should preserve internal commas");
    assert_eq!(direct.operations.len(), 3);

    let iceberg = parse_alter_emitter(
        "ALTER ICEBERG TABLE EMITTER event_emitter SET COMMIT EACH 30s MAX SIZE 64MiB;",
    )
    .expect("Iceberg commit policy should parse");
    assert_eq!(
        iceberg.operations,
        vec![AlterEmitterOperation::SetCommit {
            commit_each: "30s".to_string(),
            max_commit_size: "64MiB".parse().expect("valid commit size"),
        }]
    );

    let replace_iceberg = parse_alter_emitter(
        "ALTER ICEBERG TABLE EMITTER event_emitter SET TO s3_main TABLE 'events' NAMESPACE \
         'Analytics.Events' LOCATION 's3://warehouse/events' CATALOG iceberg_catalog COMMIT EACH \
         1m MAX SIZE 512MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'value' = \
         input.value } ;",
    )
    .expect("SET TO Iceberg should require and retain its complete commit contract");
    assert!(matches!(
        replace_iceberg.operations.as_slice(),
        [AlterEmitterOperation::SetTo {
            destination,
            ..
        }] if matches!(
            destination.as_ref(),
            EmitterEntity::Iceberg(IcebergEmitter::Table(IcebergTableEmitter {
                commit_each,
                max_commit_size,
                ..
            })) if commit_each == "1m" && max_commit_size.to_string() == "512MiB"
        )
    ));
}

#[test]
fn rejects_alter_emitter_without_operations_or_complete_flush_policy() {
    assert!(parse_alter_emitter("ALTER ZEROMQ PUSH EMITTER event_emitter;").is_err());
    assert!(
        parse_alter_emitter("ALTER ZEROMQ PUSH EMITTER event_emitter SET FLUSH EACH 1s;").is_err()
    );
}

#[test]
fn alter_emitter_completion_comes_from_its_operation_grammar() {
    let suggestions = suggest_alter_emitter("ALTER ZEROMQ PUSH EMITTER event_emitter ", usize::MAX);
    for expected in ["ADD FROM", "ALTER FROM", "SET TO", "DROP FROM"] {
        assert!(
            suggestions.contains(&expected.to_string()),
            "missing {expected}: {suggestions:?}"
        );
    }
    assert!(!suggestions.contains(&"SCHEMA".to_string()));
}

fn expression(source: &str) -> nervix_models::Expression {
    crate::parse_expression(source).expect("valid structured expression")
}

fn batch_policy(max_messages: u64, max_size: &str) -> EmitterBatchPolicy {
    EmitterBatchPolicy {
        max_messages: BatchMessageLimit::try_from(max_messages)
            .expect("the fixture message limit is within range"),
        max_size: max_size
            .parse()
            .expect("the fixture size is a whole number of bytes"),
    }
}

fn complete_codec_emitter(connector: &str) -> String {
    format!(
        "CREATE {connector} ENCODE USING my_codec FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL \
         ERROR LOG;"
    )
}

fn assert_canonical_emitter_roundtrip(source: &str, mode: &str) {
    let parsed = parse_create_emitter(source)
        .unwrap_or_else(|error| panic!("publishing mode must parse for `{mode}`: {error:?}"));
    let canonical = parsed
        .to_canonical_plexus()
        .unwrap_or_else(|error| panic!("publishing mode must render for `{mode}`: {error:?}"));
    let reparsed = parse_create_emitter(&canonical).unwrap_or_else(|error| {
        panic!("canonical publishing mode must reparse for `{mode}`: {error:?}\n{canonical}")
    });
    assert_eq!(reparsed, parsed, "canonical round-trip changed `{mode}`");
}

#[test]
fn native_client_emitter_constructs_output_and_round_trips() {
    let source = "CREATE CLIENT CONSUMER EMITTER app_output FROM orders TO SCHEMA outgoing MODE \
                  ACK PARALLEL MAX 4 ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s INHERIT id \
                  SET cents = input.amount * 100 WHERE output.cents > 0 BATCH MAX MESSAGES 16 MAX \
                  SIZE 1MiB FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;";
    assert_canonical_emitter_roundtrip(source, "CLIENT ACK");
    let parsed = parse_create_emitter(source).expect("native client output parses");
    assert!(matches!(
        parsed.connector.as_ref(),
        EmitterEntity::Client(_)
    ));
    assert!(matches!(parsed.body.body, EmitterBody::Client));
    assert!(parsed.batch.is_some());
}

#[test]
fn native_client_emitter_requires_ack_mode_and_batch_limits() {
    let prefix = "CREATE CLIENT CONSUMER EMITTER app_output FROM orders TO SCHEMA outgoing ";
    let tail = " FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;";
    for body in [
        "MODE NO ACK RETRY POLICY BACKOFF 100ms MAX 1s BATCH MAX MESSAGES 16 MAX SIZE 1MiB",
        "MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s",
        "MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s BATCH MAX MESSAGES \
         0 MAX SIZE 1MiB",
        "MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 1s ENCODE USING codec \
         BATCH MAX MESSAGES 16 MAX SIZE 1MiB",
    ] {
        assert!(
            parse_create_emitter(&format!("{prefix}{body}{tail}")).is_err(),
            "invalid native CLIENT emitter parsed: {body}"
        );
    }
}

#[test]
fn native_client_emitter_completion_stays_on_its_grammar_branch() {
    let source = "CREATE CLIENT CONSUMER EMITTER app_output FROM orders TO ";
    let suggestions = suggest_create_emitter(source, source.len());
    assert!(suggestions.contains(&"SCHEMA".to_string()));
    assert!(!suggestions.contains(&"TOPIC".to_string()));
    let source =
        "CREATE CLIENT CONSUMER EMITTER app_output FROM orders TO SCHEMA outgoing MODE ACK ";
    let suggestions = suggest_create_emitter(source, source.len());
    assert!(suggestions.contains(&"SEQUENTIAL".to_string()));
    assert!(suggestions.contains(&"PARALLEL".to_string()));
    assert!(!suggestions.contains(&"NO ACK".to_string()));
}

#[test]
fn every_publishing_mode_body_parses_and_canonical_round_trips() {
    for connector in [
        "KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE NO ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s",
        "KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE ACK SEQUENTIAL ACK \
         TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s",
        "PULSAR TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE ACK PARALLEL MAX 16 ACK \
         TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s",
        "RABBITMQ QUEUE EMITTER emit FROM p99 TO broker QUEUE 'events' MODE NO ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s",
        "MQTT TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE QOS 0 RETRY POLICY \
         BACKOFF 250ms MAX 30s",
        "MQTT TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE QOS 1 ACK SEQUENTIAL ACK \
         TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s",
        "MQTT TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE QOS 2 ACK PARALLEL MAX 8 \
         ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s",
        "NATS SUBJECT EMITTER emit FROM p99 TO broker SUBJECT 'events' MODE NO ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s",
        "NATS SUBJECT EMITTER emit FROM p99 TO broker SUBJECT 'events' MODE JETSTREAM ACK \
         SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s",
        "REDIS PUBSUB EMITTER emit FROM p99 TO broker CHANNEL 'events' MODE NO ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s",
        "ZEROMQ PUSH EMITTER emit FROM p99 TO broker MODE NO ACK RETRY POLICY BACKOFF 250ms MAX \
         30s",
        "SQS QUEUE EMITTER emit FROM p99 TO broker QUEUE 'events' SEND SINGLE MODE ACK RETRY \
         POLICY BACKOFF 250ms MAX 30s",
        "SQS QUEUE EMITTER emit FROM p99 TO broker QUEUE 'events' SEND BATCH MODE ACK RETRY \
         POLICY BACKOFF 250ms MAX 30s",
        "SQS QUEUE EMITTER emit FROM p99 TO broker QUEUE 'events.fifo' FIFO GROUP FROM BRANCH \
         SEND BATCH MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
        "SENTRY EVENT EMITTER emit FROM p99 TO broker MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
    ] {
        assert_canonical_emitter_roundtrip(&complete_codec_emitter(connector), connector);
    }

    for connector in [
        "CLICKHOUSE TABLE EMITTER emit FROM p99 TO db TABLE 'events' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s VALUES { 'id' = input.id } BATCH MAX MESSAGES 100 MAX SIZE 1MiB",
        "POSTGRES TABLE EMITTER emit FROM p99 TO db TABLE 'events' MODE ACK RETRY POLICY BACKOFF \
         250ms MAX 30s VALUES { 'id' = input.id } BATCH MAX MESSAGES 100 MAX SIZE 1MiB",
        "MYSQL TABLE EMITTER emit FROM p99 TO db TABLE 'events' MODE ACK RETRY POLICY BACKOFF \
         250ms MAX 30s VALUES { 'id' = input.id } BATCH MAX MESSAGES 100 MAX SIZE 1MiB",
        "MONGODB COLLECTION EMITTER emit FROM p99 TO db COLLECTION 'events' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s VALUES { 'id' = input.id } BATCH MAX MESSAGES 100 MAX SIZE 1MiB",
        "ICEBERG TABLE EMITTER emit FROM p99 TO store TABLE 'events' NAMESPACE 'Analytics.Events' \
         LOCATION 's3://bucket/events' CATALOG catalog COMMIT EACH 1m MAX SIZE 64MiB MODE ACK \
         RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'id' = input.id } ",
        "OTEL LOGS EMITTER emit FROM p99 TO otel_main  MODE ACK RETRY POLICY BACKOFF 250ms MAX \
         30s VALUES { 'time' = input.time, 'body' = input.body } ATTRIBUTES { \
         'service.instance.id' = input.instance_id } RESOURCE { 'service.name' = 'checkout' } \
         SCOPE 'nervix/logs' VERSION '1.0' ",
        "OTEL TRACES EMITTER emit FROM p99 TO otel_main  MODE ACK RETRY POLICY BACKOFF 250ms MAX \
         30s VALUES { 'trace_id' = input.trace_id, 'span_id' = input.span_id, 'name' = \
         input.name, 'start_time' = input.start_time, 'end_time' = input.end_time } ",
        "OTEL METRICS EMITTER emit FROM p99 TO otel_main METRIC 'request.duration' UNIT 'ms' \
         DESCRIPTION 'Request duration' HISTOGRAM CUMULATIVE MODE ACK RETRY POLICY BACKOFF 250ms \
         MAX 30s VALUES { 'time' = input.time, 'count' = input.count, 'bucket_counts' = \
         input.bucket_counts, 'explicit_bounds' = input.explicit_bounds } ",
    ] {
        let source = format!(
            "CREATE {connector} FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
        assert_canonical_emitter_roundtrip(&source, connector);
    }
}

#[test]
fn rejects_missing_incomplete_or_foreign_publishing_modes() {
    for source in [
        complete_codec_emitter("KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events'"),
        complete_codec_emitter(
            "KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE NO ACK",
        ),
        complete_codec_emitter(
            "KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE ACK SEQUENTIAL RETRY \
             POLICY BACKOFF 250ms MAX 30s",
        ),
        complete_codec_emitter(
            "KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE ACK PARALLEL MAX 0 \
             ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s",
        ),
        complete_codec_emitter(
            "KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE QOS 1 ACK SEQUENTIAL \
             ACK TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s",
        ),
        "CREATE CLICKHOUSE TABLE EMITTER emit FROM p99 TO db TABLE 'events' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s VALUES { 'id' = input.id } FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON \
         GENERAL ERROR LOG;"
            .to_string(),
    ] {
        assert!(
            parse_create_emitter(&source).is_err(),
            "invalid emitter unexpectedly parsed: {source}"
        );
    }
}

#[test]
fn publishing_mode_completion_stays_within_the_selected_emitter() {
    let before_mode = "CREATE KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' ";
    let suggestions = suggest_create_emitter(before_mode, before_mode.len());
    assert_eq!(suggestions, vec!["MODE".to_string()]);

    let kafka_mode = format!("{before_mode}MODE ");
    let suggestions = suggest_create_emitter(&kafka_mode, kafka_mode.len());
    assert!(suggestions.contains(&"ACK".to_string()));
    assert!(suggestions.contains(&"NO ACK".to_string()));
    assert!(!suggestions.contains(&"QOS".to_string()));
    assert!(!suggestions.contains(&"JETSTREAM".to_string()));
    assert!(!suggestions.contains(&"SINGLE".to_string()));
}

#[test]
fn nats_jetstream_completion_uses_the_complete_mode_phrase() {
    let input = "CREATE NATS SUBJECT EMITTER emit FROM p99 TO broker SUBJECT 'events' MODE ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert!(suggestions.contains(&"NO ACK".to_string()));
    assert!(suggestions.contains(&"JETSTREAM ACK".to_string()));
    assert!(!suggestions.contains(&"JETSTREAM".to_string()));
    assert!(!suggestions.contains(&"QOS".to_string()));
    assert!(!suggestions.contains(&"SINGLE".to_string()));
}

#[test]
fn mqtt_mode_completion_requires_qos_before_the_level() {
    let input = "CREATE MQTT TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert!(suggestions.contains(&"QOS".to_string()));
    assert!(!suggestions.contains(&"mqtt_qos".to_string()));

    let input = "CREATE MQTT TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE QOS ";
    let suggestions = suggest_create_emitter(input, input.len());
    assert_eq!(suggestions, vec!["mqtt_qos".to_string()]);
}

#[test]
fn sqs_quoted_fifo_queue_completes_its_group_and_policy() {
    let input = "ALTER SQS QUEUE EMITTER outgoing SET TO broker QUEUE 'orders.fifo' ";
    let suggestions = suggest_alter_emitter(input, input.len());
    assert!(
        suggestions.contains(&"FIFO GROUP".to_string()),
        "{suggestions:?}"
    );
    assert!(suggestions.contains(&"SEND".to_string()), "{suggestions:?}");
    assert!(
        !suggestions.contains(&"MODE".to_string()),
        "{suggestions:?}"
    );
}

#[test]
fn direct_emitter_completion_only_offers_where_construction() {
    let input = "CREATE CLICKHOUSE TABLE EMITTER emit FROM p99 TO db TABLE 'events' MODE ACK \
                 RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'id' = input.id } ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert!(suggestions.contains(&"WHERE".to_string()));
    // A database write has no unbounded form, so its batching clause comes before any flush.
    assert!(suggestions.contains(&"BATCH MAX MESSAGES".to_string()));
    assert!(!suggestions.contains(&"FLUSH EACH".to_string()));
    assert!(!suggestions.contains(&"FLUSH IMMEDIATE".to_string()));
    assert!(!suggestions.contains(&"INHERIT".to_string()));
    assert!(!suggestions.contains(&"SET".to_string()));
    assert!(!suggestions.contains(&"INVOKE".to_string()));
}

#[test]
fn parses_sqs_fifo_group_forms() {
    let from_branch = parse_create_emitter(&complete_codec_emitter(
        "SQS QUEUE EMITTER emit FROM p99 TO broker QUEUE 'events.fifo' FIFO GROUP FROM BRANCH \
         SEND SINGLE MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
    ))
    .expect("SQS FIFO FROM BRANCH mode should parse");
    assert!(matches!(
        from_branch.connector.as_ref(),
        EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
            queue,
            fifo_group: Some(nervix_models::SqsFifoGroup::FromBranch),
            ..
        })) if queue.as_str() == "events.fifo"
    ));
    let canonical = from_branch
        .to_canonical_plexus()
        .expect("SQS FIFO emitter should render canonically");
    let round_tripped =
        parse_create_emitter(&canonical).expect("canonical SQS FIFO emitter should parse again");
    assert_eq!(round_tripped, from_branch);

    let expression = parse_create_emitter(&complete_codec_emitter(
        "SQS QUEUE EMITTER emit FROM p99 TO broker QUEUE 'events.fifo' FIFO GROUP \
         concat(input.tenant, '-', input.region) SEND BATCH MODE ACK RETRY POLICY BACKOFF 250ms \
         MAX 30s",
    ))
    .expect("SQS FIFO expression mode should parse");
    assert!(matches!(
        expression.connector.as_ref(),
        EmitterEntity::Sqs(SqsEmitter::Queue(SqsQueueEmitter {
            fifo_group: Some(nervix_models::SqsFifoGroup::Expression(_)),
            ..
        }))
    ));
}

#[test]
fn rejects_fifo_group_on_non_sqs_emitter() {
    let source = complete_codec_emitter(
        "KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' FIFO GROUP FROM BRANCH MODE \
         NO ACK RETRY POLICY BACKOFF 250ms MAX 30s",
    );
    assert!(parse_create_emitter(&source).is_err());
}

#[test]
fn parses_alter_emitter_publishing_and_attachment_modes() {
    let parsed = parse_alter_emitter(
        "ALTER KAFKA TOPIC EMITTER event_emitter SET MODE ACK PARALLEL MAX 32 ACK TIMEOUT 10s \
         RETRY POLICY BACKOFF 250ms MAX 30s, SET DETACHED, SET TO emitter_b TOPIC 'events' MODE \
         NO ACK RETRY POLICY BACKOFF 1s MAX 1m ENCODE USING event_codec;",
    )
    .expect("kinded ALTER mode operations should parse");

    assert!(matches!(
        parsed.operations[0],
        AlterEmitterOperation::SetPublishingMode {
            mode: EmitterModeChange::Kafka(KafkaTopicEmitterMode::Topic(
                KafkaTopicPublishingMode::Ack { .. }
            ))
        }
    ));
    assert!(matches!(
        parsed.operations[1],
        AlterEmitterOperation::SetAttachment {
            mode: AckMode::Detached
        }
    ));
    assert!(matches!(
        parsed.operations[2],
        AlterEmitterOperation::SetTo {
            ref destination,
            ..
        } if matches!(destination.publishing(), EmitterPublishing::NoAck { .. })
    ));

    let request_ack = parse_alter_emitter(
        "ALTER POSTGRES TABLE EMITTER database_emitter SET MODE ACK RETRY POLICY BACKOFF 250ms \
         MAX 30s;",
    )
    .expect("request/response ACK mode should parse without a confirmation window");
    assert!(matches!(
        request_ack.operations.as_slice(),
        [AlterEmitterOperation::SetPublishingMode {
            mode: EmitterModeChange::Postgres(PostgresTableEmitterMode::Table(
                PostgresTablePublishingMode::Ack { .. }
            ))
        }]
    ));
}

#[test]
fn direct_database_emitters_reject_codecs() {
    let source = complete_codec_emitter(
        "POSTGRES TABLE EMITTER emit FROM p99 TO database TABLE 'events' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s VALUES { 'value' = input.value } ",
    );
    assert!(parse_create_emitter(&source).is_err());
}

#[test]
fn parses_create_emitter_kafka() {
    let input = r#"
        CREATE KAFKA TOPIC EMITTER emit
            FROM p99
            TO broker1 TOPIC 'topic' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    assert_eq!(parsed.name.as_str(), "emit");
    assert_eq!(
        parsed
            .from
            .first()
            .expect("emitter must have an input")
            .as_str(),
        "p99"
    );
    assert_eq!(
        parsed.body.body.codec().map(|codec| codec.as_str()),
        Some("my_codec")
    );
    let EmitterEntity::Kafka(KafkaEmitter::Topic(destination)) = parsed.connector.as_ref() else {
        panic!("expected Kafka Topic");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("broker1").expect("valid client identifier")
    );
    assert_eq!(
        destination.topic,
        nervix_models::KafkaTopicName::try_from("topic").expect("valid topic identifier")
    );
    assert_eq!(parsed.mode, AckMode::Attached);
}

#[test]
fn parses_emitter_input_collection() {
    let parsed = parse_create_emitter(
        "CREATE KAFKA TOPIC EMITTER emit FROM p99 COLLECT FOR 1s MAX BATCH SIZE 10MiB TO broker1 \
         TOPIC 'topic' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec FLUSH \
         IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
    )
    .expect("emitter input collection must parse");
    let policy = parsed
        .from
        .collect_policy
        .as_ref()
        .expect("emitter collection policy must be structured");
    assert_eq!(policy.collect_for, "1s");
    assert_eq!(policy.max_batch_size.as_deref(), Some("10MiB"));
}

#[test]
fn suggests_input_collection_after_emitter_source() {
    let input = "CREATE KAFKA TOPIC EMITTER emit FROM p99 COL";
    let suggestions = suggest_create_emitter(input, input.len());
    assert!(suggestions.contains(&"COLLECT FOR".to_string()));
}

#[test]
fn parses_multiple_emitter_inputs_with_source_where() {
    let parsed = parse_create_emitter(
        "CREATE ZEROMQ PUSH EMITTER emit FROM source_a WHERE input.kind = 'a', source_b WHERE \
         input.kind = 'b' COLLECT FOR 10ms MAX BATCH SIZE 1MiB TO emitter MODE NO ACK RETRY \
         POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec FLUSH IMMEDIATE ON MESSAGE ERROR LOG \
         ON GENERAL ERROR LOG;",
    )
    .expect("multiple emitter inputs should parse");

    assert_eq!(
        parsed
            .from
            .relays()
            .iter()
            .map(|relay| relay.as_str())
            .collect::<Vec<_>>(),
        vec!["source_a", "source_b"]
    );
    assert_eq!(parsed.from.where_clauses().len(), 2);
    assert!(parsed.from.collect_policy.is_some());
}

#[test]
fn rejects_incomplete_multiple_emitter_inputs() {
    assert!(
        parse_create_emitter(
            "CREATE ZEROMQ PUSH EMITTER emit FROM source_a, TO emitter ENCODE USING my_codec \
             FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
        .is_err()
    );
    assert!(
        parse_create_emitter(
            "CREATE ZEROMQ PUSH EMITTER emit FROM source_a WHERE, source_b TO emitter ENCODE \
             USING my_codec FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        )
        .is_err()
    );
}

#[test]
fn emitter_input_completion_does_not_leak_unrelated_grammar() {
    let input = "CREATE KAFKA TOPIC EMITTER emit FROM source_a WHE";
    let suggestions = suggest_create_emitter(input, input.len());
    assert!(suggestions.contains(&"WHERE".to_string()));
    assert!(!suggestions.contains(&"SCHEMA".to_string()));
}

#[test]
fn parses_create_emitter_sentry() {
    let input = r#"
        CREATE SENTRY EVENT EMITTER emit
            FROM errors
            TO sentry_main MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING error_event_codec
            INHERIT ALL
            FLUSH EACH 100ms MAX BATCH SIZE 1MiB
            ON MESSAGE ERROR LOG
            ON GENERAL ERROR LOG;
    "#;

    let parsed = parse_create_emitter(input).expect("parse should succeed");

    assert_eq!(parsed.connector.transport_label(), "SENTRY EVENT");
    assert_eq!(
        parsed.connector.client().map(|client| client.as_str()),
        Some("sentry_main")
    );
}

#[test]
fn rejects_sentry_emitter_without_codec() {
    let input = r#"
        CREATE SENTRY EVENT EMITTER emit
            FROM errors
            TO sentry_main MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
            INHERIT ALL
            FLUSH EACH 100ms MAX BATCH SIZE 1MiB
            ON MESSAGE ERROR LOG
            ON GENERAL ERROR LOG;
    "#;

    let error = parse_create_emitter(input).expect_err("parse must fail");
    let ParseFromSourceError::Parse { diagnostics, .. } = error.current_context() else {
        panic!("expected parse error, got {error:?}");
    };
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("ENCODE USING")),
        "expected codec diagnostic, got {diagnostics:?}"
    );
}

#[test]
fn parses_create_emitter_clickhouse() {
    let input = r#"
        CREATE CLICKHOUSE TABLE EMITTER to_ch
            FROM notifications
            TO clickhouse_client TABLE 'sales.Orders'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "clickhouse_user_id" = input.user_id,
                "clickhouse_now" = NOW(),
                "clickhouse_action" = LOWER(input.action)
            } BATCH MAX MESSAGES 100 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    assert_eq!(parsed.body.body, EmitterBody::Values);
    let EmitterEntity::ClickHouse(ClickHouseEmitter::Table(destination)) =
        parsed.connector.as_ref()
    else {
        panic!("expected ClickHouse Table");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("clickhouse_client").expect("valid client identifier")
    );
    assert_eq!(
        destination.table,
        nervix_models::ClickHouseTableName::try_from("sales.Orders")
            .expect("valid table identifier")
    );
    assert_eq!(
        destination.values,
        vec![
            ClickHouseValueMapping {
                column: "clickhouse_user_id".to_string(),
                expression: expression("input.user_id"),
            },
            ClickHouseValueMapping {
                column: "clickhouse_now".to_string(),
                expression: expression("NOW ( )"),
            },
            ClickHouseValueMapping {
                column: "clickhouse_action".to_string(),
                expression: expression("LOWER ( input.action )"),
            },
        ]
    );
    assert_eq!(parsed.batch, Some(batch_policy(100, "1MiB")));
}

#[test]
fn clickhouse_mode_completion_offers_only_ack() {
    let input = "CREATE CLICKHOUSE TABLE EMITTER to_ch FROM notifications TO clickhouse_client \
                 TABLE 'sales.Orders' MODE ";
    let suggestions = suggest_create_emitter(input, input.len());
    assert!(suggestions.contains(&"ACK".to_string()));
    assert!(!suggestions.contains(&"NO ACK".to_string()));
}

#[test]
fn rejects_clickhouse_emitter_without_flush_policy() {
    let tokens = to_tokens(
        r#"
        CREATE CLICKHOUSE TABLE EMITTER to_ch FROM notifications
        TO clickhouse_client TABLE 'my_table'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "user_id" = input.user_id } BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter().any(|err| format!("{err:?}").contains("FLUSH")),
        "expected ClickHouse flush diagnostic, got {errs:?}"
    );
}

#[test]
fn clickhouse_values_keep_an_array_value_whole() {
    let tokens = to_tokens(
        r#"
        CREATE CLICKHOUSE TABLE EMITTER to_ch FROM notifications
        TO clickhouse_client TABLE 'my_table'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "tags" = [input.first_tag, input.second_tag], "id" = input.id } BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    let EmitterEntity::ClickHouse(ClickHouseEmitter::Table(ClickHouseTableEmitter {
        values, ..
    })) = parsed.connector.as_ref()
    else {
        panic!("expected a ClickHouse emitter, got {:?}", parsed.connector);
    };
    assert_eq!(
        values,
        &vec![
            ClickHouseValueMapping {
                column: "tags".to_string(),
                expression: expression("[input.first_tag, input.second_tag]"),
            },
            ClickHouseValueMapping {
                column: "id".to_string(),
                expression: expression("input.id"),
            },
        ]
    );
}

#[test]
fn rejects_clickhouse_values_with_an_unclosed_array() {
    let tokens = to_tokens(
        r#"
        CREATE CLICKHOUSE TABLE EMITTER to_ch FROM notifications
        TO clickhouse_client TABLE 'my_table'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "tags" = [input.first_tag, input.second_tag } BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    parse_create_emitter_tokens(&tokens).expect_err("an unclosed array must not parse");
}

#[test]
fn clickhouse_completion_after_values_holding_an_array_offers_the_mode() {
    let input = "CREATE CLICKHOUSE TABLE EMITTER to_ch FROM notifications TO clickhouse_client \
                 TABLE 'my_table' VALUES { 'tags' = [input.first_tag, input.second_tag], 'id' = \
                 input.id } ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert!(suggestions.contains(&"MODE".to_string()), "{suggestions:?}");
    assert!(!suggestions.contains(&"]".to_string()), "{suggestions:?}");
    assert!(
        !suggestions.contains(&"VALUES".to_string()),
        "{suggestions:?}"
    );
}

#[test]
fn parses_create_emitter_iceberg() {
    let input = r#"
        CREATE DETACHED ICEBERG TABLE EMITTER to_iceberg
            FROM notifications
            TO s3_client TABLE 'notifications' NAMESPACE 'Analytics.Events'
            LOCATION 's3://nervix-iceberg/tables/notifications'
            CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "user_id" = input.user_id,
                "action" = input.action
            } FLUSH EACH 10s MAX BATCH SIZE 64MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    assert_eq!(parsed.body.body, EmitterBody::Values);
    assert_eq!(parsed.mode, AckMode::Detached);
    let EmitterEntity::Iceberg(IcebergEmitter::Table(destination)) = parsed.connector.as_ref()
    else {
        panic!("expected Iceberg Table");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("s3_client").expect("valid client identifier")
    );
    assert_eq!(
        destination.table,
        nervix_models::IcebergTableName::try_from("notifications").expect("valid table identifier")
    );
    assert_eq!(
        destination.values,
        vec![
            ClickHouseValueMapping {
                column: "user_id".to_string(),
                expression: expression("input.user_id"),
            },
            ClickHouseValueMapping {
                column: "action".to_string(),
                expression: expression("input.action"),
            },
        ]
    );
    assert_eq!(
        destination.location,
        "s3://nervix-iceberg/tables/notifications".to_string()
    );
    assert_eq!(
        destination.catalog,
        IcebergCatalog::Rest {
            client: nervix_models::ClientName::try_from("iceberg_catalog")
                .expect("valid catalog client identifier"),
        }
    );
    assert_eq!(destination.commit_each, "1m".to_string());
    assert_eq!(
        destination.max_commit_size,
        "512MiB".parse().expect("valid commit size")
    );
}

#[test]
fn parses_create_emitter_iceberg_gcs() {
    let input = r#"
        CREATE ICEBERG TABLE EMITTER to_iceberg
            FROM notifications
            TO gcs_client TABLE 'notifications' NAMESPACE 'Analytics.Events'
            LOCATION 'gs://nervix-iceberg/tables/notifications'
            CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "user_id" = input.user_id,
                "action" = input.action
            } FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    let EmitterEntity::Iceberg(IcebergEmitter::Table(destination)) = parsed.connector.as_ref()
    else {
        panic!("expected Iceberg Table");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("gcs_client").expect("valid client identifier")
    );
    assert_eq!(
        destination.table,
        nervix_models::IcebergTableName::try_from("notifications").expect("valid table identifier")
    );
    assert_eq!(
        destination.values,
        vec![
            ClickHouseValueMapping {
                column: "user_id".to_string(),
                expression: expression("input.user_id"),
            },
            ClickHouseValueMapping {
                column: "action".to_string(),
                expression: expression("input.action"),
            },
        ]
    );
    assert_eq!(
        destination.location,
        "gs://nervix-iceberg/tables/notifications".to_string()
    );
    assert_eq!(
        destination.catalog,
        IcebergCatalog::Rest {
            client: nervix_models::ClientName::try_from("iceberg_catalog")
                .expect("valid catalog client identifier"),
        }
    );
    assert_eq!(destination.commit_each, "1m".to_string());
    assert_eq!(
        destination.max_commit_size,
        "512MiB".parse().expect("valid commit size")
    );
}

#[test]
fn parses_create_emitter_iceberg_azure_blob() {
    let input = r#"
        CREATE ICEBERG TABLE EMITTER to_iceberg
            FROM notifications
            TO azure_client TABLE 'notifications' NAMESPACE 'Analytics.Events'
            LOCATION 'wasb://nervix-iceberg@devstoreaccount1.blob.core.windows.net/tables/notifications'
            CATALOG iceberg_catalog COMMIT EACH 1m MAX SIZE 512MiB MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "user_id" = input.user_id,
                "action" = input.action
            } FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    let EmitterEntity::Iceberg(IcebergEmitter::Table(destination)) = parsed.connector.as_ref()
    else {
        panic!("expected Iceberg Table");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("azure_client").expect("valid client identifier")
    );
    assert_eq!(
        destination.table,
        nervix_models::IcebergTableName::try_from("notifications").expect("valid table identifier")
    );
    assert_eq!(
        destination.values,
        vec![
            ClickHouseValueMapping {
                column: "user_id".to_string(),
                expression: expression("input.user_id"),
            },
            ClickHouseValueMapping {
                column: "action".to_string(),
                expression: expression("input.action"),
            },
        ]
    );
    assert_eq!(
        destination.location,
        "wasb://nervix-iceberg@devstoreaccount1.blob.core.windows.net/tables/notifications"
            .to_string()
    );
    assert_eq!(
        destination.catalog,
        IcebergCatalog::Rest {
            client: nervix_models::ClientName::try_from("iceberg_catalog")
                .expect("valid catalog client identifier"),
        }
    );
    assert_eq!(destination.commit_each, "1m".to_string());
    assert_eq!(
        destination.max_commit_size,
        "512MiB".parse().expect("valid commit size")
    );
}

#[test]
fn rejects_iceberg_emitter_without_flush_policy() {
    let tokens = to_tokens(
        r#"
        CREATE ICEBERG TABLE EMITTER to_iceberg FROM notifications
        TO s3_client TABLE 'notifications' NAMESPACE 'Analytics.Events'
        LOCATION 's3://nervix-iceberg/tables/notifications'
        CATALOG iceberg_catalog
        COMMIT EACH 1m MAX SIZE 64MiB MODE ACK RETRY POLICY BACKOFF 1s MAX 2s
        VALUES { "user_id" = input.user_id }
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter().any(|err| format!("{err:?}").contains("FLUSH")),
        "Iceberg requires a flush policy: {errs:?}"
    );
}

#[test]
fn rejects_iceberg_emitter_with_encode_using() {
    let tokens = to_tokens(
        r#"
        CREATE ICEBERG TABLE EMITTER to_iceberg FROM notifications ENCODE USING json_codec
        TO s3_client TABLE 'notifications' NAMESPACE 'Analytics.Events'
        VALUES { "user_id" = input.user_id }
        LOCATION 's3://nervix-iceberg/tables/notifications'
        CATALOG iceberg_catalog
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter()
            .any(|err| format!("{err:?}").contains("iden: Encode")),
        "Iceberg takes no codec, so ENCODE USING must not be accepted before TO: {errs:?}"
    );
}

#[test]
fn rejects_iceberg_emitter_without_values_mapping() {
    let tokens = to_tokens(
        r#"
        CREATE ICEBERG TABLE EMITTER to_iceberg FROM notifications
        TO s3_client TABLE 'notifications' NAMESPACE 'Analytics.Events'
        LOCATION 's3://nervix-iceberg/tables/notifications'
        CATALOG iceberg_catalog
        COMMIT EACH 1m MAX SIZE 64MiB MODE ACK RETRY POLICY BACKOFF 1s MAX 2s
        FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter().any(|err| format!("{err:?}").contains("VALUES")),
        "expected Iceberg VALUES diagnostic, got {errs:?}"
    );
}

#[test]
fn rejects_iceberg_emitter_without_commit_policy() {
    let tokens = to_tokens(
        r#"
        CREATE ICEBERG TABLE EMITTER to_iceberg FROM notifications
        TO s3_client TABLE 'notifications' NAMESPACE 'Analytics.Events'
        LOCATION 's3://nervix-iceberg/tables/notifications'
        CATALOG iceberg_catalog
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter()
            .any(|err| format!("{err:?}").contains("COMMIT EACH")),
        "expected Iceberg COMMIT EACH diagnostic, got {errs:?}"
    );
}

#[test]
fn rejects_non_iceberg_emitter_with_commit_policy() {
    let tokens = to_tokens(
        r#"
        CREATE CLICKHOUSE TABLE EMITTER to_ch FROM notifications
        TO clickhouse_client TABLE 'my_table'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "user_id" = input.user_id } BATCH MAX MESSAGES 100 MAX SIZE 1MiB
        FLUSH EACH 100ms MAX BATCH SIZE 1MiB COMMIT EACH 1m MAX SIZE 512MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter()
            .any(|err| format!("{err:?}").contains("iden: Commit")),
        "COMMIT EACH belongs to the Iceberg emitter and must not be accepted here: {errs:?}"
    );
}

#[test]
fn iceberg_catalog_context_suggestions_do_not_leak_emitter_keywords() {
    let input = "CREATE ICEBERG TABLE EMITTER to_iceberg FROM notifications TO s3_client TABLE \
                 'notifications' NAMESPACE 'Analytics.Events' LOCATION 's3://bucket/table' \
                 CATALOG ";
    let suggestions = suggest_create_emitter(input, input.len());
    assert_eq!(suggestions, vec!["ref:client".to_string()]);
}

#[test]
fn iceberg_options_precede_the_policy_and_values() {
    let input = "CREATE ICEBERG TABLE EMITTER lake FROM events TO store TABLE 'Orders' NAMESPACE \
                 'Analytics.Events' ";
    assert_eq!(suggest_create_emitter(input, input.len()), vec!["LOCATION"]);
}

#[test]
fn parses_create_emitter_postgres() {
    let input = r#"
        CREATE POSTGRES TABLE EMITTER to_pg
            FROM notifications
            TO postgres_client TABLE 'my_table'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "postgres_user_id" = input.user_id,
                "postgres_now" = NOW() AS STRING,
                "postgres_action" = LOWER(input.action)
            } BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    assert_eq!(parsed.body.body, EmitterBody::Values);
    let EmitterEntity::Postgres(PostgresEmitter::Table(destination)) = parsed.connector.as_ref()
    else {
        panic!("expected Postgres Table");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("postgres_client").expect("valid client identifier")
    );
    assert_eq!(
        destination.table,
        nervix_models::PostgresTableName::try_from("my_table").expect("valid table identifier")
    );
    assert_eq!(
        destination.values,
        vec![
            ClickHouseValueMapping {
                column: "postgres_user_id".to_string(),
                expression: expression("input.user_id"),
            },
            ClickHouseValueMapping {
                column: "postgres_now".to_string(),
                expression: expression("NOW ( ) AS STRING"),
            },
            ClickHouseValueMapping {
                column: "postgres_action".to_string(),
                expression: expression("LOWER ( input.action )"),
            },
        ]
    );
    assert_eq!(destination.conflict_action, PostgresConflictAction::None);
    assert_eq!(parsed.batch, Some(batch_policy(25, "1MiB")));
}

#[test]
fn parses_postgres_emitter_on_conflict_do_update() {
    let input = r#"
        CREATE POSTGRES TABLE EMITTER to_pg
            FROM notifications
            TO postgres_client TABLE 'my_table'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "postgres_user_id" = input.user_id,
                "postgres_action" = LOWER(input.action)
            }
            ON CONFLICT ("postgres_user_id") DO UPDATE BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let parsed = parse_create_emitter(input).expect("parse should succeed");
    let EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter {
        conflict_action,
        ..
    })) = parsed.connector.as_ref()
    else {
        panic!("expected postgres emitter emitter");
    };
    assert_eq!(
        conflict_action,
        &PostgresConflictAction::DoUpdate {
            target: vec!["postgres_user_id".to_string()]
        }
    );
}

#[test]
fn parses_postgres_emitter_on_conflict_do_nothing_without_target() {
    let input = r#"
        CREATE POSTGRES TABLE EMITTER to_pg
            FROM notifications
            TO postgres_client TABLE 'my_table'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "postgres_user_id" = input.user_id,
                "postgres_action" = LOWER(input.action)
            }
            ON CONFLICT DO NOTHING BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let parsed = parse_create_emitter(input).expect("parse should succeed");
    let EmitterEntity::Postgres(PostgresEmitter::Table(PostgresTableEmitter {
        conflict_action,
        ..
    })) = parsed.connector.as_ref()
    else {
        panic!("expected postgres emitter emitter");
    };
    assert_eq!(
        conflict_action,
        &PostgresConflictAction::DoNothing { target: Vec::new() }
    );
}

#[test]
fn rejects_postgres_emitter_on_conflict_do_update_without_target() {
    let input = r#"
        CREATE POSTGRES TABLE EMITTER to_pg
            FROM notifications
            TO postgres_client TABLE 'my_table'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "postgres_user_id" = input.user_id,
                "postgres_action" = LOWER(input.action)
            }
            ON CONFLICT DO UPDATE
            BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let error = parse_create_emitter(input).expect_err("parse must fail");
    match error.current_context() {
        ParseFromSourceError::Parse { diagnostics, .. } => {
            assert!(
                diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.message.contains("requires a conflict target")),
                "expected conflict target diagnostic, got {diagnostics:?}"
            );
        }
        other => panic!("expected parse error, got {other:?}"),
    }
}

#[test]
fn suggests_postgres_conflict_clause_after_values() {
    let input = "CREATE POSTGRES TABLE EMITTER to_pg FROM notifications TO postgres_client TABLE \
                 'my_table' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { \
                 \"postgres_user_id\" = input.user_id } ";
    let suggestions = suggest_create_emitter(input, input.len());

    // The whole phrase, not a bare `ON`: `ON` alone cannot be continued from here.
    assert!(suggestions.contains(&"ON CONFLICT".to_string()));
    assert!(!suggestions.contains(&"ON".to_string()));
    assert!(suggestions.contains(&"BATCH MAX MESSAGES".to_string()));
}

#[test]
fn suggests_postgres_conflict_actions_after_do() {
    let input = "CREATE POSTGRES TABLE EMITTER to_pg FROM notifications TO postgres_client TABLE \
                 'my_table' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { \
                 \"postgres_user_id\" = input.user_id, \"postgres_action\" = input.action } ON \
                 CONFLICT (\"postgres_user_id\") DO ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert!(suggestions.contains(&"UPDATE".to_string()));
    assert!(suggestions.contains(&"NOTHING".to_string()));
}

#[test]
fn rejects_postgres_emitter_without_batch_clause() {
    let tokens = to_tokens(
        r#"
        CREATE POSTGRES TABLE EMITTER to_pg FROM notifications
        TO postgres_client TABLE 'my_table'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "user_id" = input.user_id } FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter()
            .any(|err| format!("{err:?}").contains("BATCH MAX MESSAGES")),
        "expected a BATCH MAX MESSAGES diagnostic, got {errs:?}"
    );
}

#[test]
fn rejects_postgres_emitter_without_flush_policy() {
    let tokens = to_tokens(
        r#"
        CREATE POSTGRES TABLE EMITTER to_pg FROM notifications
        TO postgres_client TABLE 'my_table'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "user_id" = input.user_id } BATCH MAX MESSAGES 25 MAX SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter().any(|err| format!("{err:?}").contains("FLUSH")),
        "expected Postgres flush diagnostic, got {errs:?}"
    );
}

#[test]
fn rejects_batch_limits_outside_their_range() {
    for (clause, diagnostic) in [
        (
            "BATCH MAX MESSAGES 0 MAX SIZE 1MiB",
            "BATCH MAX MESSAGES must be between 1 and 4294967295, found 0",
        ),
        (
            "BATCH MAX MESSAGES 10 MAX SIZE 0B",
            "BATCH MAX SIZE must be greater than zero",
        ),
        (
            "BATCH MAX MESSAGES 10 MAX SIZE 1.5MiB",
            "BATCH MAX SIZE '1.5MiB' must be a whole number",
        ),
        (
            "BATCH MAX MESSAGES 10 MAX SIZE 16777216TiB",
            "BATCH MAX SIZE '16777216TiB' exceeds the largest size",
        ),
    ] {
        let source = format!(
            "CREATE POSTGRES TABLE EMITTER to_pg FROM notifications TO postgres_client TABLE \
             'my_table' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {{ 'user_id' = \
             input.user_id }} {clause} FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON \
             GENERAL ERROR LOG;"
        );
        let error = parse_create_emitter(&source).expect_err("the limit must be rejected");

        assert!(
            format!("{error:?}").contains(diagnostic),
            "expected `{diagnostic}` for `{clause}`, got {error:?}"
        );
    }
}

#[test]
fn parses_create_emitter_mysql() {
    let input = r#"
        CREATE MYSQL TABLE EMITTER to_mysql
            FROM notifications
            TO mysql_client TABLE 'my_table'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "mysql_user_id" = input.user_id,
                "mysql_now" = NOW() AS STRING,
                "mysql_action" = LOWER(input.action)
            } BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    assert_eq!(parsed.body.body, EmitterBody::Values);
    let EmitterEntity::MySql(MySqlEmitter::Table(destination)) = parsed.connector.as_ref() else {
        panic!("expected MySql Table");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("mysql_client").expect("valid client identifier")
    );
    assert_eq!(
        destination.table,
        nervix_models::MySqlTableName::try_from("my_table").expect("valid table identifier")
    );
    assert_eq!(
        destination.values,
        vec![
            ClickHouseValueMapping {
                column: "mysql_user_id".to_string(),
                expression: expression("input.user_id"),
            },
            ClickHouseValueMapping {
                column: "mysql_now".to_string(),
                expression: expression("NOW ( ) AS STRING"),
            },
            ClickHouseValueMapping {
                column: "mysql_action".to_string(),
                expression: expression("LOWER ( input.action )"),
            },
        ]
    );
    assert_eq!(destination.conflict_action, MySqlConflictAction::None);
    assert_eq!(parsed.batch, Some(batch_policy(25, "1MiB")));
}

#[test]
fn parses_mysql_emitter_on_conflict_do_update() {
    let input = r#"
        CREATE MYSQL TABLE EMITTER to_mysql
            FROM notifications
            TO mysql_client TABLE 'my_table'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "mysql_user_id" = input.user_id,
                "mysql_action" = LOWER(input.action)
            }
            ON CONFLICT DO UPDATE BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let parsed = parse_create_emitter(input).expect("parse should succeed");
    let EmitterEntity::MySql(MySqlEmitter::Table(MySqlTableEmitter {
        conflict_action, ..
    })) = parsed.connector.as_ref()
    else {
        panic!("expected mysql emitter emitter");
    };
    assert_eq!(conflict_action, &MySqlConflictAction::DoUpdate);
}

#[test]
fn parses_mysql_emitter_on_conflict_do_nothing() {
    let input = r#"
        CREATE MYSQL TABLE EMITTER to_mysql
            FROM notifications
            TO mysql_client TABLE 'my_table'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "mysql_user_id" = input.user_id,
                "mysql_action" = LOWER(input.action)
            }
            ON CONFLICT DO NOTHING BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let parsed = parse_create_emitter(input).expect("parse should succeed");
    let EmitterEntity::MySql(MySqlEmitter::Table(MySqlTableEmitter {
        conflict_action, ..
    })) = parsed.connector.as_ref()
    else {
        panic!("expected mysql emitter emitter");
    };
    assert_eq!(conflict_action, &MySqlConflictAction::DoNothing);
}

#[test]
fn rejects_mysql_emitter_on_conflict_target() {
    let input = r#"
        CREATE MYSQL TABLE EMITTER to_mysql
            FROM notifications
            TO mysql_client TABLE 'my_table'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "mysql_user_id" = input.user_id }
            ON CONFLICT ("mysql_user_id") DO UPDATE
            BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    assert!(
        parse_create_emitter(input).is_err(),
        "mysql conflict target must fail"
    );
}

#[test]
fn suggests_mysql_conflict_clause_after_values() {
    let input = "CREATE MYSQL TABLE EMITTER to_mysql FROM notifications TO mysql_client TABLE \
                 'my_table' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { \
                 \"mysql_user_id\" = input.user_id } ";
    let suggestions = suggest_create_emitter(input, input.len());

    // The whole phrase, not a bare `ON`: `ON` alone cannot be continued from here.
    assert!(suggestions.contains(&"ON CONFLICT".to_string()));
    assert!(!suggestions.contains(&"ON".to_string()));
    assert!(suggestions.contains(&"BATCH MAX MESSAGES".to_string()));
}

#[test]
fn suggests_mysql_conflict_actions_after_do() {
    let input = "CREATE MYSQL TABLE EMITTER to_mysql FROM notifications TO mysql_client TABLE \
                 'my_table' MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { \
                 \"mysql_user_id\" = input.user_id } ON CONFLICT DO ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert!(suggestions.contains(&"UPDATE".to_string()));
    assert!(suggestions.contains(&"NOTHING".to_string()));
}

#[test]
fn rejects_mysql_emitter_without_batch_clause() {
    let tokens = to_tokens(
        r#"
        CREATE MYSQL TABLE EMITTER to_mysql FROM notifications
        TO mysql_client TABLE 'my_table'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "user_id" = input.user_id } FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter()
            .any(|err| format!("{err:?}").contains("BATCH MAX MESSAGES")),
        "expected a BATCH MAX MESSAGES diagnostic, got {errs:?}"
    );
}

#[test]
fn rejects_mysql_emitter_without_flush_policy() {
    let tokens = to_tokens(
        r#"
        CREATE MYSQL TABLE EMITTER to_mysql FROM notifications
        TO mysql_client TABLE 'my_table'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "user_id" = input.user_id } BATCH MAX MESSAGES 25 MAX SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter().any(|err| format!("{err:?}").contains("FLUSH")),
        "expected MySQL flush diagnostic, got {errs:?}"
    );
}

#[test]
fn parses_create_emitter_mongodb() {
    let input = r#"
        CREATE MONGODB COLLECTION EMITTER to_mongodb
            FROM notifications
            TO mongodb_client COLLECTION 'my_collection'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "mongodb_user_id" = input.user_id,
                "mongodb_now" = NOW() AS STRING,
                "mongodb_action" = LOWER(input.action)
            } BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    assert_eq!(parsed.body.body, EmitterBody::Values);
    let EmitterEntity::MongoDb(MongoDbEmitter::Collection(destination)) = parsed.connector.as_ref()
    else {
        panic!("expected MongoDb Collection");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("mongodb_client").expect("valid client identifier")
    );
    assert_eq!(
        destination.collection,
        nervix_models::MongoDbCollectionName::try_from("my_collection")
            .expect("valid collection identifier")
    );
    assert_eq!(
        destination.values,
        vec![
            ClickHouseValueMapping {
                column: "mongodb_user_id".to_string(),
                expression: expression("input.user_id"),
            },
            ClickHouseValueMapping {
                column: "mongodb_now".to_string(),
                expression: expression("NOW ( ) AS STRING"),
            },
            ClickHouseValueMapping {
                column: "mongodb_action".to_string(),
                expression: expression("LOWER ( input.action )"),
            },
        ]
    );
    assert_eq!(destination.conflict_action, MongoDbConflictAction::None);
    assert_eq!(parsed.batch, Some(batch_policy(25, "1MiB")));
}

#[test]
fn parses_mongodb_emitter_on_conflict_do_update() {
    let input = r#"
        CREATE MONGODB COLLECTION EMITTER to_mongodb
            FROM notifications
            TO mongodb_client COLLECTION 'my_collection'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "mongodb_user_id" = input.user_id,
                "mongodb_action" = LOWER(input.action)
            }
            ON CONFLICT ("mongodb_user_id") DO UPDATE BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let parsed = parse_create_emitter(input).expect("parse should succeed");
    let EmitterEntity::MongoDb(MongoDbEmitter::Collection(MongoDbCollectionEmitter {
        conflict_action,
        ..
    })) = parsed.connector.as_ref()
    else {
        panic!("expected mongodb emitter emitter");
    };
    assert_eq!(
        conflict_action,
        &MongoDbConflictAction::DoUpdate {
            target: vec!["mongodb_user_id".to_string()]
        }
    );
}

#[test]
fn parses_mongodb_emitter_on_conflict_do_nothing() {
    let input = r#"
        CREATE MONGODB COLLECTION EMITTER to_mongodb
            FROM notifications
            TO mongodb_client COLLECTION 'my_collection'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "mongodb_user_id" = input.user_id,
                "mongodb_action" = LOWER(input.action)
            }
            ON CONFLICT ("mongodb_user_id") DO NOTHING BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let parsed = parse_create_emitter(input).expect("parse should succeed");
    let EmitterEntity::MongoDb(MongoDbEmitter::Collection(MongoDbCollectionEmitter {
        conflict_action,
        ..
    })) = parsed.connector.as_ref()
    else {
        panic!("expected mongodb emitter emitter");
    };
    assert_eq!(
        conflict_action,
        &MongoDbConflictAction::DoNothing {
            target: vec!["mongodb_user_id".to_string()]
        }
    );
}

#[test]
fn rejects_mongodb_emitter_on_conflict_do_update_without_target() {
    let input = r#"
        CREATE MONGODB COLLECTION EMITTER to_mongodb
            FROM notifications
            TO mongodb_client COLLECTION 'my_collection'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "mongodb_user_id" = input.user_id,
                "mongodb_action" = LOWER(input.action)
            }
            ON CONFLICT DO UPDATE
            BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    assert!(
        parse_create_emitter(input).is_err(),
        "mongodb conflict target must fail"
    );
}

#[test]
fn rejects_mongodb_emitter_on_conflict_target_not_mapped() {
    let input = r#"
        CREATE MONGODB COLLECTION EMITTER to_mongodb
            FROM notifications
            TO mongodb_client COLLECTION 'my_collection'
            MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES {
                "mongodb_action" = LOWER(input.action)
            }
            ON CONFLICT ("mongodb_user_id") DO UPDATE
            BATCH MAX MESSAGES 25 MAX SIZE 1MiB
            FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let error = parse_create_emitter(input).expect_err("parse must fail");
    match error.current_context() {
        ParseFromSourceError::Parse { diagnostics, .. } => {
            assert!(
                diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.message.contains("is not mapped in VALUES")),
                "expected unmapped target diagnostic, got {diagnostics:?}"
            );
        }
        other => panic!("expected parse error, got {other:?}"),
    }
}

#[test]
fn suggests_mongodb_conflict_clause_after_values() {
    let input = "CREATE MONGODB COLLECTION EMITTER to_mongodb FROM notifications TO \
                 mongodb_client COLLECTION 'my_collection' MODE ACK RETRY POLICY BACKOFF 250ms \
                 MAX 30s VALUES { \"mongodb_user_id\" = input.user_id } ";
    let suggestions = suggest_create_emitter(input, input.len());

    // The whole phrase, not a bare `ON`: `ON` alone cannot be continued from here.
    assert!(suggestions.contains(&"ON CONFLICT".to_string()));
    assert!(!suggestions.contains(&"ON".to_string()));
    assert!(suggestions.contains(&"BATCH MAX MESSAGES".to_string()));
}

#[test]
fn suggests_mongodb_conflict_actions_after_do() {
    let input = "CREATE MONGODB COLLECTION EMITTER to_mongodb FROM notifications TO \
                 mongodb_client COLLECTION 'my_collection' MODE ACK RETRY POLICY BACKOFF 250ms \
                 MAX 30s VALUES { \"mongodb_user_id\" = input.user_id, \"mongodb_action\" = \
                 input.action } ON CONFLICT (\"mongodb_user_id\") DO ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert!(suggestions.contains(&"UPDATE".to_string()));
    assert!(suggestions.contains(&"NOTHING".to_string()));
}

#[test]
fn rejects_mongodb_emitter_without_flush_policy() {
    let tokens = to_tokens(
        r#"
        CREATE MONGODB COLLECTION EMITTER to_mongodb FROM notifications
        TO mongodb_client COLLECTION 'my_collection'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "user_id" = input.user_id } BATCH MAX MESSAGES 25 MAX SIZE 1MiB
        ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter().any(|err| format!("{err:?}").contains("FLUSH")),
        "expected MongoDB flush diagnostic, got {errs:?}"
    );
}

#[test]
fn rejects_mongodb_emitter_without_batch_clause() {
    let tokens = to_tokens(
        r#"
        CREATE MONGODB COLLECTION EMITTER to_mongodb FROM notifications
        TO mongodb_client COLLECTION 'my_collection'
        MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s VALUES { "user_id" = input.user_id } FLUSH EACH 10s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );

    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter()
            .any(|err| format!("{err:?}").contains("BATCH MAX MESSAGES")),
        "expected a BATCH MAX MESSAGES diagnostic, got {errs:?}"
    );
}

#[test]
fn emitter_heads_complete_the_declared_connector_and_entity() {
    let input = "CREATE ";
    let suggestions = suggest_create_emitter(input, input.len());
    for kind in EmitterEntityKind::all() {
        assert!(
            suggestions.contains(&format!("{} EMITTER", kind.phrase())),
            "{kind}: {suggestions:?}"
        );
    }
}

#[test]
fn a_table_emitter_completes_its_locator_after_the_client() {
    let input = "CREATE POSTGRES TABLE EMITTER to_pg FROM events TO database ";
    assert_eq!(suggest_create_emitter(input, input.len()), vec!["TABLE"]);
}

#[test]
fn parses_create_emitter_pulsar() {
    let input = r#"
        CREATE PULSAR TOPIC EMITTER emit
            FROM p99
            TO pulsar1 TOPIC 'topic' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    let EmitterEntity::Pulsar(PulsarEmitter::Topic(destination)) = parsed.connector.as_ref() else {
        panic!("expected Pulsar Topic");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("pulsar1").expect("valid client identifier")
    );
    assert_eq!(
        destination.topic,
        nervix_models::PulsarTopicName::try_from("topic").expect("valid topic identifier")
    );
}

#[test]
fn parses_create_emitter_detached() {
    let input = r#"
        CREATE DETACHED KAFKA TOPIC EMITTER emit
            FROM p99
            TO broker1 TOPIC 'topic' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    assert_eq!(parsed.mode, AckMode::Detached);
}

#[test]
fn fails_without_to_clause() {
    let tokens =
        to_tokens("CREATE ATTACHED KAFKA TOPIC EMITTER emit FROM p99 ENCODE USING my_codec;");
    let errs = parse_create_emitter_tokens(&tokens).expect_err("must fail");
    assert!(!errs.is_empty());
}

#[test]
fn rejects_pulsar_emitter_without_topic() {
    let tokens = to_tokens("CREATE ATTACHED PULSAR TOPIC EMITTER emit FROM p99 TO pulsar1;");
    let errs = parse_create_emitter_tokens(&tokens).expect_err("must fail");
    assert!(!errs.is_empty());
}

#[test]
fn rejects_encoded_emitter_without_codec() {
    let tokens = to_tokens(
        r#"
        CREATE KAFKA TOPIC EMITTER emit
            FROM p99
            TO broker1 TOPIC 'topic'
            MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
    );
    let errs = parse_create_emitter_tokens(&tokens).expect_err("parse must fail");
    assert!(
        errs.iter()
            .any(|err| format!("{err:?}").contains("ENCODE USING")),
        "expected codec diagnostic, got {errs:?}"
    );
}

#[test]
fn suggests_mode_after_create() {
    let input = "CREATE ";
    let suggestions = suggest_create_emitter(input, input.len());
    assert!(suggestions.contains(&"ATTACHED".to_string()));
    assert!(suggestions.contains(&"DETACHED".to_string()));
    assert!(!suggestions.contains(&"FROM".to_string()));
}

#[test]
fn suggests_encode_using_as_compound_keyword() {
    // The codec follows the emitter it encodes for, so it is offered once the emitter is named.
    let input = "CREATE ATTACHED KAFKA TOPIC EMITTER emit FROM p99 TO broker1 TOPIC 'topic' MODE \
                 NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ";
    let suggestions = suggest_create_emitter(input, input.len());
    assert!(suggestions.contains(&"ENCODE USING".to_string()));

    let before_emitter = "CREATE ATTACHED KAFKA TOPIC EMITTER emit FROM p99 ";
    let suggestions = suggest_create_emitter(before_emitter, before_emitter.len());
    assert!(!suggestions.contains(&"ENCODE USING".to_string()));
}

#[test]
fn a_kinded_emitter_completes_its_client_reference() {
    let input = "CREATE ATTACHED KAFKA TOPIC EMITTER emit FROM events TO ";
    assert_eq!(
        suggest_create_emitter(input, input.len()),
        vec!["ref:client"]
    );
}

#[test]
fn sentry_emitter_completion_context_does_not_leak_transport_qualifiers() {
    let client_input = "CREATE SENTRY EVENT EMITTER emit FROM errors TO ";
    let client_suggestions = suggest_create_emitter(client_input, client_input.len());
    assert!(client_suggestions.contains(&"ref:client".to_string()));
    assert!(!client_suggestions.contains(&"TOPIC".to_string()));
    assert!(!client_suggestions.contains(&"QUEUE".to_string()));

    let route_input = "CREATE SENTRY EVENT EMITTER emit FROM errors TO sentry_main MODE ACK RETRY \
                       POLICY BACKOFF 250ms MAX 30s ENCODE USING error_event_codec ";
    let route_suggestions = suggest_create_emitter(route_input, route_input.len());
    assert!(route_suggestions.contains(&"INHERIT".to_string()));
    assert!(route_suggestions.contains(&"FLUSH EACH".to_string()));
    assert!(!route_suggestions.contains(&"TOPIC".to_string()));
    assert!(!route_suggestions.contains(&"QUEUE".to_string()));
    assert!(!route_suggestions.contains(&"SUBJECT".to_string()));
}

#[test]
fn suggests_flush_after_emitter_error_policies() {
    let input = "CREATE ATTACHED KAFKA TOPIC EMITTER emit FROM p99 TO broker1 TOPIC 'topic' MODE \
                 NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec ON MESSAGE ERROR \
                 LOG ON GENERAL ERROR LOG ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert!(suggestions.contains(&"FLUSH EACH".to_string()));
    assert!(suggestions.contains(&"FLUSH IMMEDIATE".to_string()));
    assert!(!suggestions.contains(&"COMMIT EACH".to_string()));
}

#[test]
fn parses_create_emitter_mqtt() {
    let input = r#"
        CREATE ATTACHED MQTT TOPIC EMITTER emit
            FROM p99
            TO broker1 TOPIC 'topic' MODE QOS 0 RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    let EmitterEntity::Mqtt(MqttEmitter::Topic(destination)) = parsed.connector.as_ref() else {
        panic!("expected Mqtt Topic");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("broker1").expect("valid client identifier")
    );
    assert_eq!(
        destination.topic,
        nervix_models::MqttTopicName::try_from("topic").expect("valid topic identifier")
    );
}

#[test]
fn parses_create_emitter_nats() {
    let input = r#"
        CREATE ATTACHED NATS SUBJECT EMITTER emit
            FROM p99
            TO nats_main SUBJECT 'notifications' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    let EmitterEntity::Nats(NatsEmitter::Subject(destination)) = parsed.connector.as_ref() else {
        panic!("expected Nats Subject");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("nats_main").expect("valid client identifier")
    );
    assert_eq!(
        destination.subject,
        nervix_models::NatsPublishSubjectName::try_from("notifications")
            .expect("valid subject identifier")
    );
}

#[test]
fn parses_create_emitter_rabbitmq() {
    let input = r#"
        CREATE ATTACHED RABBITMQ QUEUE EMITTER emit
            FROM p99
            TO broker1 QUEUE 'queue1' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    let EmitterEntity::RabbitMq(RabbitMqEmitter::Queue(destination)) = parsed.connector.as_ref()
    else {
        panic!("expected RabbitMq Queue");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("broker1").expect("valid client identifier")
    );
    assert_eq!(
        destination.queue,
        nervix_models::RabbitMqQueueName::try_from("queue1").expect("valid queue identifier")
    );
}

#[test]
fn parses_create_emitter_redis() {
    let input = r#"
        CREATE ATTACHED REDIS PUBSUB EMITTER emit
            FROM p99
            TO broker1 CHANNEL 'out' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    let EmitterEntity::Redis(RedisEmitter::PubSub(destination)) = parsed.connector.as_ref() else {
        panic!("expected Redis PubSub");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("broker1").expect("valid client identifier")
    );
    assert_eq!(
        destination.channel,
        nervix_models::RedisChannelName::try_from("out").expect("valid channel identifier")
    );
}

#[test]
fn parses_create_emitter_zeromq() {
    let input = r#"
        CREATE ATTACHED ZEROMQ PUSH EMITTER emit
            FROM p99
            TO zmq_out MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    let EmitterEntity::ZeroMq(ZeroMqEmitter::Push(destination)) = parsed.connector.as_ref() else {
        panic!("expected ZeroMq Push");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("zmq_out").expect("valid client identifier")
    );
}

#[test]
fn parses_create_emitter_syslog_with_only_no_ack() {
    let input = r#"
        CREATE SYSLOG COLLECTOR EMITTER forward_syslog
            FROM events
            TO syslog_out MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING syslog_codec INHERIT ALL
            FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let parsed = parse_create_emitter(input).expect("parse should succeed");
    let EmitterEntity::Syslog(SyslogEmitter::Collector(destination)) = parsed.connector.as_ref()
    else {
        panic!("expected Syslog Collector");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("syslog_out").expect("valid client identifier")
    );
    assert!(matches!(
        parsed.connector.publishing(),
        EmitterPublishing::NoAck { .. }
    ));
}

#[test]
fn rejects_syslog_ack_mode_and_header_invocations() {
    assert!(
        parse_create_emitter(
            "CREATE SYSLOG COLLECTOR EMITTER e FROM events TO c MODE ACK SEQUENTIAL ACK TIMEOUT \
             1s RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING codec INHERIT ALL FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
        )
        .is_err()
    );

    // Header capability is rejected semantically because route construction is shared by all
    // encoded emitters; the parser still preserves the structured invocation for validation.
    let parsed = parse_create_emitter(
        "CREATE SYSLOG COLLECTOR EMITTER e FROM events TO c MODE NO ACK RETRY POLICY BACKOFF 1s \
         MAX 30s ENCODE USING codec INHERIT ALL INVOKE write_header('x', 'y') FLUSH IMMEDIATE ON \
         MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
    )
    .expect("shared route syntax should parse for semantic capability validation");
    assert_eq!(parsed.construction.invocations.len(), 1);
}

#[test]
fn syslog_completion_stays_on_no_ack_mode() {
    let input = "CREATE SYSLOG COLLECTOR EMITTER e FROM events TO c MODE ";
    let suggestions = suggest_create_emitter(input, input.len());
    assert!(suggestions.contains(&"NO ACK".to_string()));
    assert!(!suggestions.contains(&"ACK".to_string()));
    assert!(!suggestions.contains(&"QOS".to_string()));
}

#[test]
fn parses_create_emitter_sqs() {
    let input = r#"
        CREATE ATTACHED SQS QUEUE EMITTER emit
            FROM p99
            TO sqs_main QUEUE 'queue1' SEND SINGLE MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let tokens = to_tokens(input);
    let parsed = parse_create_emitter_tokens(&tokens).expect("parse should succeed");

    let EmitterEntity::Sqs(SqsEmitter::Queue(destination)) = parsed.connector.as_ref() else {
        panic!("expected Sqs Queue");
    };
    assert_eq!(
        destination.client,
        nervix_models::ClientName::try_from("sqs_main").expect("valid client identifier")
    );
    assert_eq!(destination.queue.as_str(), "queue1");
    assert_eq!(destination.fifo_group, None);
    assert_eq!(destination.send, nervix_models::SqsSend::Single);
}

#[test]
fn parses_codec_emitter_route_construction() {
    let input = r#"
        CREATE KAFKA TOPIC EMITTER emit
            FROM p99
            TO broker1 TOPIC 'topic' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec
            INHERIT ALL EXCEPT raw
            SET normalized = lower(input.name), score = input.score AS FLOAT64
            WHERE output.active
            INVOKE write_header(lower("TENANT"), input.tenant), write_header("route", output.normalized)
            FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let parsed = parse_create_emitter(input).expect("parse should succeed");

    assert!(matches!(
        parsed.construction.inherit,
        Some(nervix_models::Inheritance::AllExcept(ref fields)) if fields.len() == 1
    ));
    assert_eq!(parsed.construction.assignments.len(), 2);
    assert_eq!(parsed.construction.invocations.len(), 2);
}

#[test]
fn parses_emitter_with_invoke_only_route() {
    let input = r#"
        CREATE KAFKA TOPIC EMITTER emit
            FROM p99
            TO broker1 TOPIC 'topic' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec
            INVOKE write_header("route", input.route)
            FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let parsed = parse_create_emitter(input).expect("parse should succeed");
    assert_eq!(parsed.construction.invocations.len(), 1);
}

#[test]
fn rejects_invalid_emitter_route_construction() {
    let input = r#"
        CREATE KAFKA TOPIC EMITTER emit
            FROM p99
            TO broker1 TOPIC 'topic' MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s
            ENCODE USING my_codec
            SET normalized = FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
    "#;

    let error = parse_create_emitter(input).expect_err("parse should fail");
    match error.current_context() {
        ParseFromSourceError::Parse { diagnostics, .. } => {
            assert!(!diagnostics.is_empty());
        }
        other => panic!("expected parse error, got {other:?}"),
    }
}

#[test]
fn does_not_leak_emitter_suggestions_inside_filter_map_program() {
    let input = "CREATE ATTACHED KAFKA TOPIC EMITTER emit FROM p99 TO broker1 TOPIC 'topic' MODE \
                 NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec WHERE ";
    let suggestions = suggest_create_emitter(input, input.len());
    assert!(!suggestions.contains(&"MQTT".to_string()));
    assert!(!suggestions.contains(&"NATS".to_string()));
}

#[test]
fn emitter_filter_map_context_suggests_invoke_without_emitter_leakage() {
    let input = "CREATE ATTACHED KAFKA TOPIC EMITTER emit FROM p99 TO broker1 TOPIC 'topic' MODE \
                 NO ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert!(suggestions.contains(&"INVOKE".to_string()));
    assert!(!suggestions.contains(&"SUBJECT".to_string()));
    assert!(!suggestions.contains(&"QUEUE".to_string()));
}

#[test]
fn pulsar_emitter_context_does_not_offer_other_transport_keywords() {
    let input = "CREATE ATTACHED PULSAR TOPIC EMITTER emit FROM p99 TO pulsar1 ";
    let suggestions = suggest_create_emitter(input, input.len());
    assert!(suggestions.contains(&"TOPIC".to_string()));
    assert!(!suggestions.contains(&"QUEUE".to_string()));
    assert!(!suggestions.contains(&"SUBJECT".to_string()));
    assert!(!suggestions.contains(&"CHANNEL".to_string()));
}

#[test]
fn every_emitter_family_accepts_an_optional_batch_clause_and_round_trips_it() {
    let codec_emitters = [
        "KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE NO ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s",
        "PULSAR TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE NO ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s",
        "RABBITMQ QUEUE EMITTER emit FROM p99 TO broker QUEUE 'events' MODE NO ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s",
        "REDIS PUBSUB EMITTER emit FROM p99 TO broker CHANNEL 'events' MODE NO ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s",
        "MQTT TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE QOS 1 ACK SEQUENTIAL ACK \
         TIMEOUT 30s RETRY POLICY BACKOFF 250ms MAX 30s",
        "NATS SUBJECT EMITTER emit FROM p99 TO broker SUBJECT 'events' MODE NO ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s",
        "ZEROMQ PUSH EMITTER emit FROM p99 TO broker MODE NO ACK RETRY POLICY BACKOFF 250ms MAX \
         30s",
        "SYSLOG COLLECTOR EMITTER emit FROM p99 TO broker MODE NO ACK RETRY POLICY BACKOFF 250ms \
         MAX 30s",
        "SQS QUEUE EMITTER emit FROM p99 TO broker QUEUE 'events' SEND BATCH MODE ACK RETRY \
         POLICY BACKOFF 250ms MAX 30s",
        "SENTRY EVENT EMITTER emit FROM p99 TO broker MODE ACK RETRY POLICY BACKOFF 250ms MAX 30s",
    ];
    for connector in codec_emitters {
        let omitted = complete_codec_emitter(connector);
        let parsed = parse_create_emitter(&omitted).expect("an emitter without BATCH parses");
        assert_eq!(parsed.batch, None, "`{connector}` without BATCH");
        assert_canonical_emitter_roundtrip(&omitted, connector);

        let batched = format!(
            "CREATE {connector} ENCODE USING my_codec BATCH MAX MESSAGES 500 MAX SIZE 1MiB FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );
        let parsed = parse_create_emitter(&batched).expect("an emitter with BATCH parses");
        assert_eq!(
            parsed.batch,
            Some(batch_policy(500, "1MiB")),
            "`{connector}` with BATCH"
        );
        assert_canonical_emitter_roundtrip(&batched, connector);
    }

    let direct_emitters = [
        "OTEL LOGS EMITTER emit FROM p99 TO otel_main  MODE ACK RETRY POLICY BACKOFF 250ms MAX \
         30s VALUES { 'time' = input.time, 'body' = input.body } ",
        "ICEBERG TABLE EMITTER emit FROM p99 TO store TABLE 'events' NAMESPACE 'Analytics.Events' \
         LOCATION 's3://bucket/events' CATALOG catalog COMMIT EACH 1m MAX SIZE 64MiB MODE ACK \
         RETRY POLICY BACKOFF 250ms MAX 30s VALUES { 'id' = input.id } ",
    ];
    for connector in direct_emitters {
        for batch in ["", " BATCH MAX MESSAGES 1000 MAX SIZE 3MiB"] {
            let source = format!(
                "CREATE {connector}{batch} FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR \
                 LOG;"
            );
            let parsed = parse_create_emitter(&source).expect("the direct emitter parses");
            assert_eq!(parsed.batch.is_some(), !batch.is_empty(), "`{source}`");
            assert_canonical_emitter_roundtrip(&source, connector);
        }
    }
}

#[test]
fn batch_clause_follows_route_construction() {
    let encoded = parse_create_emitter(
        "CREATE KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE NO ACK RETRY \
         POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec INHERIT ALL WHERE input.id > 0 BATCH \
         MAX MESSAGES 10 MAX SIZE 64KiB FLUSH EACH 1s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON \
         GENERAL ERROR LOG;",
    )
    .expect("BATCH follows the route construction of an encoded emitter");
    assert_eq!(encoded.batch, Some(batch_policy(10, "64KiB")));
    assert!(encoded.construction.where_clause.is_some());

    let direct = parse_create_emitter(
        "CREATE POSTGRES TABLE EMITTER emit FROM p99 TO db TABLE 'events' MODE ACK RETRY POLICY \
         BACKOFF 250ms MAX 30s VALUES { 'id' = input.id } WHERE input.id > 0 BATCH MAX MESSAGES \
         500 MAX SIZE 8MiB FLUSH EACH 1s MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL \
         ERROR LOG;",
    )
    .expect("BATCH follows the WHERE clause of a direct emitter");
    assert_eq!(direct.batch, Some(batch_policy(500, "8MiB")));
    assert!(direct.construction.where_clause.is_some());
}

#[test]
fn batch_clause_is_written_before_the_flush_policy() {
    let error = parse_create_emitter(
        "CREATE KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE NO ACK RETRY \
         POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec FLUSH IMMEDIATE BATCH MAX MESSAGES 10 \
         MAX SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
    );

    assert!(error.is_err(), "BATCH after FLUSH must not parse");
}

#[test]
fn batch_clause_requires_both_limits() {
    for clause in [
        "BATCH MAX MESSAGES 10",
        "BATCH MAX SIZE 1MiB",
        "BATCH MAX SIZE 1MiB MAX MESSAGES 10",
    ] {
        let source = format!(
            "CREATE KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE NO ACK RETRY \
             POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec {clause} FLUSH IMMEDIATE ON \
             MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
        );

        assert!(
            parse_create_emitter(&source).is_err(),
            "`{clause}` must not parse"
        );
    }
}

#[test]
fn parses_and_renders_alter_emitter_batch_operations() {
    let alter = parse_alter_emitter(
        "ALTER ZEROMQ PUSH EMITTER event_emitter SET BATCH MAX MESSAGES 250 MAX SIZE 512KiB, DROP \
         BATCH;",
    )
    .expect("batch operations parse");

    assert_eq!(
        alter.operations,
        vec![
            AlterEmitterOperation::SetBatch {
                policy: batch_policy(250, "512KiB"),
            },
            AlterEmitterOperation::DropBatch,
        ]
    );
    let canonical = alter
        .to_canonical_plexus()
        .expect("batch operations render");
    assert_eq!(
        canonical,
        "ALTER ZEROMQ PUSH EMITTER event_emitter SET BATCH MAX MESSAGES 250 MAX SIZE 512KiB, DROP \
         BATCH;"
    );
    assert_eq!(
        parse_alter_emitter(&canonical).expect("the rendering reparses"),
        alter
    );
}

#[test]
fn rejects_malformed_alter_emitter_batch_operations() {
    for source in [
        "ALTER ZEROMQ PUSH EMITTER event_emitter SET BATCH;",
        "ALTER ZEROMQ PUSH EMITTER event_emitter SET BATCH MAX MESSAGES 0 MAX SIZE 1MiB;",
        "ALTER ZEROMQ PUSH EMITTER event_emitter SET BATCH MAX MESSAGES 10;",
        "ALTER ZEROMQ PUSH EMITTER event_emitter DROP BATCH MAX MESSAGES 10 MAX SIZE 1MiB;",
    ] {
        assert!(
            parse_alter_emitter(source).is_err(),
            "`{source}` must not parse"
        );
    }
}

#[test]
fn completes_the_optional_batch_clause_beside_the_flush_policy() {
    let input = "CREATE KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE NO ACK \
                 RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert!(suggestions.contains(&"BATCH MAX MESSAGES".to_string()));
    assert!(suggestions.contains(&"FLUSH EACH".to_string()));
    assert!(suggestions.contains(&"FLUSH IMMEDIATE".to_string()));
    assert!(!suggestions.contains(&"BATCH".to_string()));
}

#[test]
fn completes_the_batch_size_after_the_message_limit() {
    let input = "CREATE KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE NO ACK \
                 RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec BATCH MAX MESSAGES 10 ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert_eq!(suggestions, vec!["MAX SIZE".to_string()]);
}

#[test]
fn completes_the_flush_policy_after_the_batch_clause() {
    let input = "CREATE KAFKA TOPIC EMITTER emit FROM p99 TO broker TOPIC 'events' MODE NO ACK \
                 RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec BATCH MAX MESSAGES 10 \
                 MAX SIZE 1MiB ";
    let suggestions = suggest_create_emitter(input, input.len());

    assert!(suggestions.contains(&"FLUSH EACH".to_string()));
    assert!(suggestions.contains(&"FLUSH IMMEDIATE".to_string()));
    assert!(!suggestions.contains(&"BATCH MAX MESSAGES".to_string()));
    assert!(!suggestions.contains(&"WHERE".to_string()));
}

#[test]
fn completes_alter_emitter_batch_operations() {
    let set = "ALTER ZEROMQ PUSH EMITTER event_emitter SET ";
    let set_suggestions = suggest_alter_emitter(set, set.len());
    assert!(set_suggestions.contains(&"BATCH MAX MESSAGES".to_string()));
    assert!(set_suggestions.contains(&"FLUSH EACH".to_string()));

    let drop = "ALTER ZEROMQ PUSH EMITTER event_emitter DROP ";
    let drop_suggestions = suggest_alter_emitter(drop, drop.len());
    assert!(drop_suggestions.contains(&"BATCH".to_string()));
    assert!(drop_suggestions.contains(&"ENCODE".to_string()));
    assert!(!drop_suggestions.contains(&"BATCH MAX MESSAGES".to_string()));
}
