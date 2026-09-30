//! Parsing and completion of the composed statement grammar.
//!
//! Layer: test harness.
//!
//! - **Owns.** Positive and negative parses of every statement family through the composed grammar,
//!   canonical round trips, and completion expectations that guard against grammar-branch leakage.
//! - **Depends on.** The composed statement grammar and the vocabulary Models.
//! - **Must not know.** Validation, persistence or runtime execution.

use nervix_models::{
    AckMode, AlterRelay, AlterRelayOperation, ClusterNodeName, CordonNode, CorrelatorName,
    DeduplicatorName, DescribeRelay, DrainNode, DropModel, DropNode, EmitterName, EndpointName,
    FieldName, FlushPolicy, IngestorName, JunctionName, Model, ModelKind, ModelName,
    ReingestorName, RelayName, ReordererName, RequestedResourceVersion, ResourceName, SchemaName,
    ShowIngestors, Statement, SubscriptionBinding, SubscriptionLiteral, UncordonNode,
    WasmProcessorName, WindowProcessorName,
};
use nonzero_ext::nonzero;
use rstest::rstest;

use super::*;

#[test]
fn client_context_suggestions_do_not_leak_schema_keywords() {
    let input = "CREATE CLIENT kafka_main TYPE ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"KAFKA".to_string()));
    assert!(suggestions.contains(&"PULSAR".to_string()));
    assert!(suggestions.contains(&"HTTP".to_string()));
    assert!(suggestions.contains(&"SENTRY".to_string()));
    assert!(suggestions.contains(&"PROMETHEUS".to_string()));
    assert!(suggestions.contains(&"RABBITMQ".to_string()));
    assert!(suggestions.contains(&"REDIS".to_string()));
    assert!(suggestions.contains(&"MQTT".to_string()));
    assert!(suggestions.contains(&"NATS".to_string()));
    assert!(suggestions.contains(&"ZEROMQ".to_string()));
    assert!(suggestions.contains(&"SQS".to_string()));
    assert!(suggestions.contains(&"S3".to_string()));
    assert!(suggestions.contains(&"GCS".to_string()));
    assert!(suggestions.contains(&"AZURE_BLOB".to_string()));
    assert!(suggestions.contains(&"ICEBERG_REST".to_string()));
    assert!(suggestions.contains(&"WEBSOCKETS".to_string()));
    assert!(suggestions.contains(&"CLICKHOUSE".to_string()));
    assert!(suggestions.contains(&"POSTGRES".to_string()));
    assert!(suggestions.contains(&"MYSQL".to_string()));
    assert!(suggestions.contains(&"MONGODB".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

/// Every numeric slot in the language names itself.
///
/// An unlabelled `select!` reports `SomethingElse`, which the suggestion mapping discards, so
/// completion goes silent at a position that is otherwise perfectly valid — the user is left
/// with a half-written statement and nothing to accept.
#[test]
fn numeric_slots_offer_a_named_placeholder_rather_than_nothing() {
    for (input, expected) in [
        ("CREATE SCHEMA s ( f ARRAY < U8 , ", "array_length"),
        ("DESCRIBE RESOURCE r VERSION ", "resource_version"),
        (
            "CREATE INFERENCER i FROM r USING RESOURCE res VERSION ",
            "completed_resource_version",
        ),
        (
            "CREATE INGESTOR i FROM KAFKA c TOPIC t OFFSET BY DOMAIN INSTANCES ",
            "instance_count",
        ),
        (
            "CREATE INGESTOR i FROM MQTT c TOPIC t MODE ACK PARALLEL MAX ",
            "max_in_flight",
        ),
        ("CREATE INGESTOR i FROM MQTT c TOPIC t QOS ", "mqtt_qos"),
        (
            "CREATE BRANCH b SCHEMA s TTL 1s MAX INSTANCES ",
            "max_instances",
        ),
        (
            "CREATE RELAY r SCHEMA s UNBRANCHED CAPACITY ",
            "relay_capacity",
        ),
        (
            "CREATE PLACEMENT p FROM a TO b REQUIRE COLOCATION RANK ",
            "placement_rank",
        ),
        (
            "CREATE WASM PROCESSOR w FROM r USING RESOURCE res VERSION 1 FILE 'g.wasm' MAX FUEL ",
            "max_fuel",
        ),
    ] {
        let suggestions = suggest_statement(input, input.len());
        assert!(
            suggestions.contains(&expected.to_string()),
            "{input:?} should offer {expected}, got {suggestions:?}"
        );
    }
}

/// A modifier that is already present must stop being offered, not be accepted and then
/// rejected by a validation pass.
#[test]
fn schema_field_modifiers_are_offered_at_most_once() {
    let input = "CREATE SCHEMA s ( f BOOL OPTIONAL ";
    let suggestions = suggest_statement(input, input.len());
    assert!(!suggestions.contains(&"OPTIONAL".to_string()));
    assert!(suggestions.contains(&"SENSITIVE".to_string()));

    let input = "CREATE SCHEMA s ( f BOOL SENSITIVE ";
    let suggestions = suggest_statement(input, input.len());
    assert!(!suggestions.contains(&"SENSITIVE".to_string()));
    assert!(suggestions.contains(&"OPTIONAL".to_string()));

    assert!(parse_statement("CREATE SCHEMA s (f BOOL OPTIONAL OPTIONAL)").is_err());
}

/// A window bound is one message count and one duration at most.
#[test]
fn a_window_bound_offers_each_kind_at_most_once() {
    let input = "CREATE WINDOW PROCESSOR w FROM r WIDTH 1s DURATION ";
    let suggestions = suggest_statement(input, input.len());
    assert!(!suggestions.contains(&"duration_literal".to_string()));
    assert!(suggestions.contains(&"message_count".to_string()));
}

/// The clause head is offered first, then the body it introduces — not the body in its place.
#[test]
fn route_construction_offers_its_head_then_names_the_body() {
    let input = "CREATE JUNCTION j FROM r UNBRANCHED TO o ";
    let suggestions = suggest_statement(input, input.len());
    for head in ["INHERIT", "SET", "WHERE", "INVOKE"] {
        assert!(
            suggestions.contains(&head.to_string()),
            "{head} should be offered, got {suggestions:?}"
        );
    }
    assert!(!suggestions.contains(&"set_assignments".to_string()));

    let input = "CREATE JUNCTION j FROM r UNBRANCHED TO o SET ";
    assert_eq!(
        suggest_statement(input, input.len()),
        vec!["set_assignments".to_string()]
    );
}

/// Contexts that accept only `SET` must not offer the clauses they will reject.
#[test]
fn set_only_route_contexts_offer_only_set() {
    let input = "CREATE INGESTOR i FROM ENDPOINT e MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX \
                 SIZE 1MiB DECODE USING c TO s BRANCHED BY b ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"SET".to_string()));
    for rejected in ["INHERIT", "INVOKE", "WHERE"] {
        assert!(
            !suggestions.contains(&rejected.to_string()),
            "{rejected} is rejected by branch construction and must not be offered, got \
             {suggestions:?}"
        );
    }
}

/// Typing the first characters of a suggestion must not make it disappear.
///
/// The partial word is removed before parsing: left in place it is swallowed by whichever
/// free-form expression region it lands in, and the expression grammar then fails with a
/// custom error that carries no expectations at all.
#[test]
fn typing_a_prefix_after_an_expression_region_keeps_the_suggestion() {
    let base = "CREATE JUNCTION j FROM r UNBRANCHED TO o SET x = 1";
    let offered = suggest_statement(&format!("{base} "), base.len() + 1);
    assert!(offered.contains(&"FLUSH EACH".to_string()));

    let typed = format!("{base} FL");
    let still_offered = suggest_statement(&typed, typed.len());
    assert!(
        still_offered.contains(&"FLUSH EACH".to_string()),
        "typing FL must keep FLUSH EACH, got {still_offered:?}"
    );
}

/// Once the partial word is removed the statement may be complete, and a complete statement has
/// no expectations, so the grammar has to be asked again with the word in place.
#[test]
fn a_prefix_typed_after_a_complete_statement_still_completes() {
    let input = "DESCRIBE RESOURCE r VE";
    let suggestions = suggest_statement(input, input.len());
    assert!(
        suggestions.contains(&"VERSION".to_string()),
        "got {suggestions:?}"
    );

    let input = "START A";
    let suggestions = suggest_statement(input, input.len());
    assert_eq!(suggestions, vec!["AT".to_string()]);
}

/// Nothing follows a terminated statement.
#[test]
fn a_terminated_statement_offers_no_continuation() {
    let input = "START AT NOW ; ";
    assert!(suggest_statement(input, input.len()).is_empty());
}

#[test]
fn show_transactions_completion_stays_in_its_statement_branch() {
    let input = "SHOW T";
    assert_eq!(
        suggest_statement(input, input.len()),
        vec!["TRANSACTIONS".to_string()]
    );

    let complete = "SHOW TRANSACTIONS ";
    assert!(suggest_statement(complete, complete.len()).is_empty());
}

/// A slot with a real constraint should say what it wants.
#[test]
fn constrained_slots_do_not_hide_behind_a_generic_literal_label() {
    let input = "CREATE ENDPOINT e ON v PATH ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"endpoint_path".to_string()));
    assert!(!suggestions.contains(&"string_literal".to_string()));
}

/// A keyword is never a duration, so it cannot be swallowed as one.
///
/// Whether a value is a *valid* duration is left to whatever consumes it: an ordinary
/// identifier still parses here and is rejected by the runtime, which has to check it anyway
/// because models also arrive from persisted state.
#[test]
fn a_bare_keyword_is_not_accepted_as_a_duration() {
    let error = parse_statement(
        "CREATE JUNCTION j FROM r UNBRANCHED TO o SET x = 1 FLUSH EACH UNBRANCHED MAX BATCH SIZE \
         1MiB ON MESSAGE ERROR LOG",
    )
    .expect_err("a keyword is not a duration");
    assert!(
        format!("{error:?}").contains("duration_literal"),
        "expected the duration slot to be named, got {error:?}"
    );

    parse_statement(
        "CREATE JUNCTION j FROM r UNBRANCHED TO o SET x = 1 FLUSH EACH oops MAX BATCH SIZE 1MiB \
         ON MESSAGE ERROR LOG",
    )
    .expect("an identifier parses and is validated where it is consumed");
}

/// A codec is required by the sink, so the grammar asks for it right after the sink.
///
/// Written before `TO`, the requirement could only be checked once the whole statement was
/// read, and a `try_map` over the whole statement reports its failure at the statement's first
/// token — where every other `CREATE` alternative outranks it and the user is told
/// `expected ... found EMITTER`.
#[test]
fn a_cross_clause_constraint_is_reported_where_it_was_broken() {
    let error = parse_statement(
        "CREATE EMITTER e FROM r TO KAFKA c TOPIC t MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX \
         30s FLUSH IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG",
    )
    .expect_err("an encoded sink needs a codec");
    assert!(
        format!("{error:?}").contains("expected ENCODE USING"),
        "the codec is required by the sink, so the grammar should ask for it: {error:?}"
    );
}

#[test]
fn create_if_not_exists_completion_suggests_compound_keyword_without_leakage() {
    let input = "CREATE ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"IF NOT EXISTS".to_string()));
    assert!(!suggestions.contains(&"IF_NOT_EXISTS".to_string()));
}

#[test]
fn ingestor_mode_context_suggestions_do_not_leak_schema_keywords() {
    let input = "CREATE INGESTOR i FROM KAFKA t TOPIC top OFFSET BY CONSUMER GROUP g MODE ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"ACK".to_string()));
    assert!(suggestions.contains(&"NO_ACK".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

#[test]
fn ingestor_branch_context_suggests_only_branch_selection_keywords() {
    let input = "CREATE INGESTOR i FROM ENDPOINT ep MODE NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX \
                 SIZE 1MiB DECODE USING sch TO s ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"UNBRANCHED".to_string()));
    assert!(suggestions.contains(&"BRANCHED BY".to_string()));
    assert!(!suggestions.contains(&"BY".to_string()));
}

#[test]
fn ingestor_bare_by_is_rejected() {
    let input = "CREATE INGESTOR i TO s FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG \
                 DECODE USING sch BY u_branch FROM ENDPOINT ep MODE NO_ACK SEQUENTIAL ON GENERAL \
                 ERROR LOG;";

    let error = parse_statement(input).expect_err("bare BY is not a branch selection mode");
    assert!(!error.current_context().diagnostics().is_empty());
}

#[test]
fn attached_stream_context_suggestions_do_not_leak_schema_keywords() {
    let input = "CREATE RELAY p ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"SCHEMA".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

#[test]
fn show_create_context_suggestions_do_not_leak_format_keywords() {
    let input = "SHOW CREATE ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"SCHEMA".to_string()));
    assert!(suggestions.contains(&"CODEC".to_string()));
    assert!(suggestions.contains(&"CLIENT".to_string()));
    assert!(suggestions.contains(&"VHOST".to_string()));
    assert!(suggestions.contains(&"ENDPOINT".to_string()));
    assert!(suggestions.contains(&"INGESTOR".to_string()));
    assert!(suggestions.contains(&"RELAY".to_string()));
    assert!(suggestions.contains(&"JUNCTION".to_string()));
    assert!(suggestions.contains(&"JUNCTION".to_string()));
    assert!(suggestions.contains(&"DEDUPLICATOR".to_string()));
    assert!(suggestions.contains(&"SIGNALING PROTOCOL".to_string()));
    assert!(suggestions.contains(&"WASM PROCESSOR".to_string()));
    assert!(suggestions.contains(&"EMITTER".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

#[test]
fn show_context_suggestions_include_cluster_without_entity_leakage() {
    let input = "SHOW ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"CREATE".to_string()));
    assert!(suggestions.contains(&"CLUSTER".to_string()));
    assert!(suggestions.contains(&"INGESTORS".to_string()));
    assert!(!suggestions.contains(&"INGESTOR".to_string()));
    assert!(!suggestions.contains(&"SCHEMA".to_string()));
    assert!(!suggestions.contains(&"CLIENT".to_string()));
}

#[test]
fn show_ingestors_parses_alone_and_refuses_trailing_words() {
    assert_eq!(
        parse_statement("SHOW INGESTORS;").ok(),
        Some(Statement::ShowIngestors(ShowIngestors))
    );
    assert_eq!(
        parse_statement("show ingestors").ok(),
        Some(Statement::ShowIngestors(ShowIngestors))
    );
    assert!(parse_statement("SHOW INGESTORS orders;").is_err());
    assert!(parse_statement("SHOW INGESTOR;").is_err());
    let input = "SHOW INGESTORS ";
    let suggestions = suggest_statement(input, input.len());
    assert!(
        !suggestions.contains(&"FROM".to_string()),
        "the listing takes no clause, so no ingestor grammar leaks after it"
    );
}

#[test]
fn emitter_context_suggestions_do_not_leak_schema_keywords() {
    let input = "CREATE EMITTER emit FROM p99 TO ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"KAFKA".to_string()));
    assert!(suggestions.contains(&"RABBITMQ".to_string()));
    assert!(suggestions.contains(&"REDIS".to_string()));
    assert!(suggestions.contains(&"MQTT".to_string()));
    assert!(suggestions.contains(&"NATS".to_string()));
    assert!(suggestions.contains(&"ZEROMQ".to_string()));
    assert!(suggestions.contains(&"SQS".to_string()));
    assert!(suggestions.contains(&"CLICKHOUSE".to_string()));
    assert!(suggestions.contains(&"POSTGRES".to_string()));
    assert!(suggestions.contains(&"MYSQL".to_string()));
    assert!(suggestions.contains(&"MONGODB".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

#[test]
fn deduplicator_context_suggestions_do_not_leak_schema_keywords() {
    let input = "CREATE DEDUPLICATOR dedup FROM ss1 DEDUPLICATE ON input.transaction_id MAX ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"TIME".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

#[test]
fn from_relay_context_suggests_where_without_schema_keyword_leakage() {
    let input = "CREATE DEDUPLICATOR dedup FROM ss1 ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"WHERE".to_string()));
    assert!(suggestions.contains(&"DEDUPLICATE ON".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

/// A call or a field scope named like the keyword that begins the next clause leaves completion
/// after its region as it is: the clauses that may follow are offered exactly as after any other
/// expression.
#[test]
fn calls_and_scopes_named_like_clause_keywords_keep_the_following_clauses_offered() {
    for (plain, spelled, following) in [
        (
            "CREATE JUNCTION peaks FROM sensors FILTER WHERE input.total > 10 ",
            "CREATE JUNCTION peaks FROM sensors FILTER WHERE max(input.readings) > output.total ",
            "UNBRANCHED",
        ),
        (
            "CREATE JUNCTION peaks FROM sensors WHERE input.total > 10 ",
            "CREATE JUNCTION peaks FROM sensors WHERE max(input.readings) > output.total ",
            "FILTER",
        ),
        (
            "CREATE DEDUPLICATOR distinct_peaks FROM sensors DEDUPLICATE ON input.id ",
            "CREATE DEDUPLICATOR distinct_peaks FROM sensors DEDUPLICATE ON max(input.readings) ",
            "MAX",
        ),
        (
            "CREATE REORDERER peaks_in_order FROM sensors BY input.id ",
            "CREATE REORDERER peaks_in_order FROM sensors BY max(input.readings) ",
            "MAX",
        ),
        (
            "CREATE CORRELATOR suffix_matches LEFT FROM sensors WHERE left.id > 0 ",
            "CREATE CORRELATOR suffix_matches LEFT FROM sensors WHERE right(left.name, 2) = \
             right.suffix ",
            "RIGHT",
        ),
        (
            "ALTER JUNCTION peaks SET FILTER WHERE concat(input.name, input.kind) != '', SET ",
            "ALTER JUNCTION peaks SET FILTER WHERE concat(input.name, replace(input.kind, 'a', \
             'b')) != '', SET ",
            "FILTER",
        ),
    ] {
        let offered = suggest_statement(plain, plain.len());
        assert!(
            offered.contains(&following.to_string()),
            "{plain} must offer {following}, got {offered:?}"
        );
        assert_eq!(
            suggest_statement(spelled, spelled.len()),
            offered,
            "completion changed after {spelled}"
        );
    }
}

#[test]
fn junction_context_suggestions_do_not_leak_schema_keywords() {
    let input = "CREATE JUNCTION merge FROM orders_a, orders_b ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"BRANCHED BY".to_string()));
    assert!(suggestions.contains(&"UNBRANCHED".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

#[test]
fn conditional_expression_body_does_not_change_route_completion_context() {
    let literal = "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = 1 ";
    let conditional = "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = CASE \
                       WHEN input.active THEN 1 ELSE 0 END ";

    assert_eq!(
        suggest_statement(literal, literal.len()),
        suggest_statement(conditional, conditional.len())
    );
}

#[test]
fn collection_expression_body_preserves_route_completion_context() {
    let literal = "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = 1 ";
    for expression in [
        "[input.first, input.second]",
        "array(input.first, input.second)",
        "vec(input.first, input.second)",
        "slice(input.items, 0, 2)",
    ] {
        let input = format!(
            "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = {expression} "
        );
        assert_eq!(
            suggest_statement(literal, literal.len()),
            suggest_statement(&input, input.len()),
            "completion changed after {expression}",
        );
    }
}

#[test]
fn tolerant_conversion_body_does_not_change_route_completion_context() {
    let literal = "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = 1 ";
    let converted = "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = \
                     TRY_CAST(input.raw AS I64) ";
    assert_eq!(
        suggest_statement(literal, literal.len()),
        suggest_statement(converted, converted.len())
    );

    let unfinished_conditional =
        "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET result = CASE WHEN ";
    let unfinished_conversion = "CREATE JUNCTION merge FROM orders UNBRANCHED TO routed SET \
                                 result = TRY_CAST(input.raw AS ";
    assert_eq!(
        suggest_statement(unfinished_conditional, unfinished_conditional.len()),
        suggest_statement(unfinished_conversion, unfinished_conversion.len()),
        "an unfinished conversion offers what any unfinished expression offers"
    );
}

#[test]
fn reingestor_output_context_suggestions_do_not_leak_schema_keywords() {
    let input = "CREATE REINGESTOR log_splitter FROM incoming_logs TO errors_ss FLUSH IMMEDIATE \
                 ON MESSAGE ERROR LOG ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"UNBRANCHED".to_string()));
    assert!(suggestions.contains(&"BRANCHED BY".to_string()));
    assert!(!suggestions.contains(&"BY".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

#[test]
fn alter_context_suggestions_include_all_supported_model_families() {
    let input = "ALTER ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"RELAY".to_string()));
    assert!(suggestions.contains(&"JUNCTION".to_string()));
    assert!(suggestions.contains(&"DEDUPLICATOR".to_string()));
    assert!(suggestions.contains(&"REORDERER".to_string()));
    assert!(suggestions.contains(&"EMITTER".to_string()));
    assert!(suggestions.contains(&"INGESTOR".to_string()));
    assert!(suggestions.contains(&"REINGESTOR".to_string()));
    assert!(suggestions.contains(&"GENERATOR".to_string()));
    assert!(suggestions.contains(&"SCHEMA".to_string()));
    assert!(suggestions.contains(&"WIRE".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

#[test]
fn alter_relay_capacity_context_suggestions_do_not_leak_schema_keywords() {
    let input = "ALTER RELAY notifications SET CAPACITY ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"relay_capacity".to_string()));
    assert!(!suggestions.contains(&"SCHEMA".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

#[test]
fn parses_drop_statement() {
    let parsed = parse_statement("DROP SCHEMA event_schema;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::Drop(DropModel {
            kind: ModelKind::Schema,
            name: ModelName::from(
                &SchemaName::try_from("event_schema").expect("valid schema name")
            ),
        })
    );
}

#[test]
fn parses_alter_relay_set_capacity_statement() {
    let parsed = parse_statement("ALTER RELAY notifications SET CAPACITY 32;")
        .expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::AlterRelay(AlterRelay {
            relay: RelayName::try_from("notifications").expect("valid relay name"),
            operations: vec![AlterRelayOperation::SetCapacity {
                capacity: nonzero!(32usize)
            }],
        })
    );
}

#[test]
fn rejects_alter_relay_zero_capacity_statement() {
    let error = parse_statement("ALTER RELAY notifications SET CAPACITY 0;")
        .expect_err("parse should fail");

    let ParseFromSourceError::Parse { diagnostics, .. } = error.current_context() else {
        panic!("expected parse error");
    };
    assert!(!diagnostics.is_empty());
}

#[test]
fn parses_drop_node_statement() {
    let parsed = parse_statement("DROP NODE node-2;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DropNode(DropNode {
            node_id: ClusterNodeName::parse("node-2").expect("valid name"),
        })
    );
}

#[test]
fn parses_cordon_node_statement() {
    let parsed = parse_statement("CORDON NODE node-2;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::CordonNode(CordonNode {
            node_id: ClusterNodeName::parse("node-2").expect("valid name"),
        })
    );
}

#[test]
fn parses_uncordon_node_statement() {
    let parsed = parse_statement("UNCORDON NODE node-2;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::UncordonNode(UncordonNode {
            node_id: ClusterNodeName::parse("node-2").expect("valid name"),
        })
    );
}

#[test]
fn parses_drain_node_statement() {
    let parsed = parse_statement("DRAIN NODE node-2;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DrainNode(DrainNode {
            node_id: ClusterNodeName::parse("node-2").expect("valid name"),
        })
    );
}

#[test]
fn parses_describe_resource_statement() {
    let parsed =
        parse_statement("DESCRIBE RESOURCE fraud_model VERSION 7;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeResource(nervix_models::DescribeResource {
            identifier: ResourceName::parse("fraud_model").expect("valid resource name"),
            version: Some(7),
        })
    );
}

#[test]
fn parses_describe_resource_summary_statement() {
    let parsed = parse_statement("DESCRIBE RESOURCE fraud_model;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeResource(nervix_models::DescribeResource {
            identifier: ResourceName::parse("fraud_model").expect("valid resource name"),
            version: None,
        })
    );
}

#[test]
fn describe_context_suggestions_include_resource_and_stream() {
    let input = "DESCRIBE ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"DEDUPLICATOR".to_string()));
    assert!(suggestions.contains(&"DOMAIN".to_string()));
    assert!(suggestions.contains(&"EMITTER".to_string()));
    assert!(suggestions.contains(&"INGESTOR".to_string()));
    assert!(suggestions.contains(&"JUNCTION".to_string()));
    assert!(suggestions.contains(&"REINGESTOR".to_string()));
    assert!(suggestions.contains(&"RESOURCE".to_string()));
    assert!(suggestions.contains(&"RELAY".to_string()));
    assert!(suggestions.contains(&"WINDOW".to_string()));
}

#[test]
fn parses_describe_domain_statement() {
    let parsed = parse_statement("DESCRIBE DOMAIN;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeDomain(nervix_models::DescribeDomain)
    );
}

#[test]
fn parses_describe_ingestor_statement() {
    let parsed =
        parse_statement("DESCRIBE INGESTOR kafka_notifications;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeIngestor(nervix_models::DescribeIngestor {
            ingestor: IngestorName::parse("kafka_notifications").expect("valid name"),
        })
    );
}

#[test]
fn parses_describe_endpoint_statement() {
    let parsed = parse_statement("DESCRIBE ENDPOINT http_notifications_endpoint;")
        .expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeEndpoint(nervix_models::DescribeEndpoint {
            name: EndpointName::parse("http_notifications_endpoint").expect("valid endpoint name"),
        })
    );
}

#[test]
fn parses_describe_relay_statement() {
    let parsed = parse_statement("DESCRIBE RELAY notifications WHERE (user_id = 42);")
        .expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeRelay(DescribeRelay {
            relay: RelayName::parse("notifications").expect("valid name"),
            bindings: vec![SubscriptionBinding {
                field: FieldName::parse("user_id").expect("valid name"),
                value: SubscriptionLiteral::Number("42".to_string()),
            }],
        })
    );
}

#[test]
fn parses_describe_deduplicator_statement() {
    let parsed =
        parse_statement("DESCRIBE DEDUPLICATOR dedup_txns;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeDeduplicator(nervix_models::DescribeDeduplicator {
            name: DeduplicatorName::parse("dedup_txns").expect("valid name"),
        })
    );
}

#[test]
fn parses_describe_junction_statement() {
    let parsed =
        parse_statement("DESCRIBE JUNCTION route_notifications;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeJunction(nervix_models::DescribeJunction {
            name: JunctionName::parse("route_notifications").expect("valid name"),
        })
    );
}

#[test]
fn parses_describe_reingestor_statement() {
    let parsed = parse_statement("DESCRIBE REINGESTOR repartition;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeReingestor(nervix_models::DescribeReingestor {
            name: ReingestorName::parse("repartition").expect("valid name"),
        })
    );
}

#[test]
fn parses_describe_correlator_statement() {
    let parsed =
        parse_statement("DESCRIBE CORRELATOR correlate_profiles;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeCorrelator(nervix_models::DescribeCorrelator {
            name: CorrelatorName::parse("correlate_profiles").expect("valid name"),
        })
    );
}

#[test]
fn parses_describe_reorderer_statement() {
    let parsed =
        parse_statement("DESCRIBE REORDERER order_notifications;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeReorderer(nervix_models::DescribeReorderer {
            name: ReordererName::parse("order_notifications").expect("valid name"),
        })
    );
}

#[test]
fn parses_describe_emitter_statement() {
    let parsed = parse_statement("DESCRIBE EMITTER kafka_out;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeEmitter(nervix_models::DescribeEmitter {
            name: EmitterName::parse("kafka_out").expect("valid name"),
        })
    );
}

#[test]
fn parses_describe_window_processor_statement() {
    let parsed =
        parse_statement("DESCRIBE WINDOW PROCESSOR latency_window;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeWindowProcessor(nervix_models::DescribeWindowProcessor {
            name: WindowProcessorName::parse("latency_window").expect("valid name"),
        })
    );
}

#[test]
fn parses_describe_wasm_processor_statement() {
    let parsed =
        parse_statement("DESCRIBE WASM PROCESSOR filter_even;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::DescribeWasmProcessor(nervix_models::DescribeWasmProcessor {
            name: WasmProcessorName::parse("filter_even").expect("valid name"),
            format: nervix_models::InspectionFormat::Text,
        })
    );
}

#[test]
fn describe_resource_summary_context_suggests_version_keyword() {
    let input = "DESCRIBE RESOURCE fraud_model ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"VERSION".to_string()));
}

#[test]
fn wasm_description_offers_the_shared_inspection_format() {
    let input = "DESCRIBE WASM PROCESSOR filter_even ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"FORMAT".to_string()));
    let input = "DESCRIBE WASM PROCESSOR filter_even FORMAT ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"JSON".to_string()));
    assert!(suggestions.contains(&"TEXT".to_string()));
}

#[test]
fn parses_create_resource_statement() {
    let parsed = parse_statement("CREATE RESOURCE fraud_model;").expect("parse should succeed");
    assert_eq!(
        parsed,
        Statement::CreateResource(nervix_models::CreateStatement::new(
            nervix_models::CreateResource {
                identifier: ResourceName::parse("fraud_model").expect("valid resource name"),
            },
            false,
        ))
    );
}

#[test]
fn rejects_client_only_upload_resource_statement() {
    let error = parse_statement("UPLOAD RESOURCE fraud_model VERSION '/tmp/model';")
        .expect_err("UPLOAD RESOURCE is a client-side command");
    assert!(!error.current_context().diagnostics().is_empty());
}

#[test]
fn parses_junction_statement_with_implicit_attached_mode() {
    let parsed = parse_statement(
        "CREATE JUNCTION join_streams FROM notifications_a, notifications_b BRANCHED BY tenant TO \
         merged_notifications INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR \
         LOG;",
    )
    .expect("parse should succeed");

    let Statement::Create(parsed) = parsed else {
        panic!("expected junction statement");
    };
    let Model::Junction(junction) = parsed.body.as_ref() else {
        panic!("expected junction statement");
    };
    assert_eq!(junction.mode, AckMode::Attached);
    assert_eq!(junction.from.from.len(), 2);
}

#[test]
fn parses_deduplicator_statement_with_implicit_attached_mode() {
    let parsed = parse_statement(
        "CREATE DEDUPLICATOR dedup_txns FROM ss1 DEDUPLICATE ON input.transaction_id MAX TIME 10m \
         BRANCHED BY tenant TO ss2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE \
         ERROR LOG;",
    )
    .expect("parse should succeed");

    let Statement::Create(parsed) = parsed else {
        panic!("expected deduplicator statement");
    };
    let Model::Deduplicator(deduplicator) = parsed.body.as_ref() else {
        panic!("expected deduplicator statement");
    };
    assert_eq!(deduplicator.mode, AckMode::Attached);
    assert_eq!(deduplicator.max_time, "10m");
}

#[test]
fn parses_inferencer_statement() {
    let parsed = parse_statement(
            r#"CREATE INFERENCER score_model FROM features USING RESOURCE fraud_model VERSION 3 FILE 'models/fraud.onnx' INPUTS { "features" DENSE TENSOR<F32>[2] = input.vector } OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] } UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG;"#,
        )
        .expect("parse should succeed");

    let Statement::Create(parsed) = parsed else {
        panic!("expected inferencer statement");
    };
    let Model::Inferencer(processor) = parsed.body.as_ref() else {
        panic!("expected inferencer statement");
    };
    assert_eq!(processor.mode, AckMode::Attached);
    assert_eq!(processor.resource.as_str(), "fraud_model");
    assert_eq!(
        processor.resource_version,
        RequestedResourceVersion::Number(3)
    );
    assert_eq!(processor.inputs.len(), 1);
    assert_eq!(processor.output_schema.len(), 1);
    assert_eq!(
        processor.output_routes.routes[0]
            .flush_policy
            .as_ref()
            .expect("output flush policy should parse"),
        &FlushPolicy::Immediate
    );
    let canonical = processor
        .to_canonical_nspl()
        .expect("inferencer should render canonically");
    assert!(canonical.contains("TO scored\n    SET score = score\n    FLUSH IMMEDIATE"));
    assert!(canonical.contains("OUTPUT SCHEMA {\n    'score' DENSE TENSOR<F32>[1]\n  }"));
}

#[test]
fn parses_reingestor_statement_with_flush_each() {
    let parsed = parse_statement(
        "CREATE REINGESTOR repartition FROM notifications TO tenant_notifications BRANCHED BY \
         tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
         MESSAGE ERROR LOG;",
    )
    .expect("parse should succeed");

    let Statement::Create(parsed) = parsed else {
        panic!("expected reingestor statement");
    };
    let Model::Reingestor(reingestor) = parsed.body.as_ref() else {
        panic!("expected reingestor statement");
    };
    assert_eq!(
        reingestor.output_routes.routes[0]
            .flush_policy
            .as_ref()
            .expect("output flush policy should parse"),
        &FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string()
        }
    );
}

#[test]
fn parses_reingestor_statement_with_multiple_output_routes() {
    let parsed = parse_statement(
            r#"CREATE REINGESTOR log_splitter FROM incoming_logs FILTER WHERE input.active TO errors_ss SET severity = lower(input.level) WHERE output.severity = "error" BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG TO warnings_ss WHERE input.level = "warn" BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG TO info_ss INHERIT ALL BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;"#,
        )
        .expect("parse should succeed");

    let Statement::Create(parsed) = parsed else {
        panic!("expected reingestor statement");
    };
    let Model::Reingestor(reingestor) = parsed.body.as_ref() else {
        panic!("expected reingestor statement");
    };
    assert_eq!(reingestor.mode, AckMode::Attached);
    assert_eq!(reingestor.output_routes.routes.len(), 3);
    assert_eq!(
        reingestor
            .output_routes
            .routes
            .get(2)
            .map(|output| output.relay.as_str()),
        Some("info_ss")
    );
    assert_eq!(
        reingestor.output_routes.routes[0]
            .flush_policy
            .as_ref()
            .expect("output flush policy should parse"),
        &FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string()
        }
    );
    assert_eq!(
        reingestor.filter_where,
        Some(crate::parse_expression("input.active").expect("valid expression"))
    );
}

#[test]
fn parses_single_reingestor_output_route_with_filter_map() {
    let parsed = parse_statement(
        "CREATE REINGESTOR fw1 FROM ss1 TO ss3 SET normalized = lower(input.raw) WHERE \
         output.normalized != '' BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH EACH \
         100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
    )
    .expect("parse should succeed");

    let Statement::Create(parsed) = parsed else {
        panic!("expected reingestor statement");
    };
    let Model::Reingestor(reingestor) = parsed.body.as_ref() else {
        panic!("expected reingestor statement");
    };
    assert_eq!(reingestor.mode, AckMode::Attached);
    assert_eq!(reingestor.from.from[0].as_str(), "ss1");
    assert_eq!(reingestor.output_routes.routes.len(), 1);
    let output = reingestor
        .output_routes
        .routes
        .first()
        .expect("output route should parse");
    assert_eq!(output.relay.as_str(), "ss3");
    assert_eq!(
        reingestor.output_routes.routes[0]
            .flush_policy
            .as_ref()
            .expect("output flush policy should parse"),
        &FlushPolicy::Each {
            interval: "100ms".to_string(),
            max_batch_size: "1MiB".to_string()
        }
    );
    assert!(!output.construction.assignments.is_empty());
    assert!(output.construction.where_clause.is_some());
}

#[test]
fn parses_emitter_statement_with_implicit_attached_mode() {
    let parsed = parse_statement(
        "CREATE EMITTER emit FROM notifications TO KAFKA kafka_main TOPIC notifications_out MODE \
         NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING notification_codec FLUSH EACH \
         100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;",
    )
    .expect("parse should succeed");

    let Statement::Create(parsed) = parsed else {
        panic!("expected emitter statement");
    };
    let Model::Emitter(emitter) = parsed.body.as_ref() else {
        panic!("expected emitter statement");
    };
    assert_eq!(emitter.mode, AckMode::Attached);
}

#[test]
fn runtime_nodes_require_supported_error_policy_blocks() {
    let external_cases = [
        (
            "ingestor",
            "CREATE INGESTOR http_notifications FROM ENDPOINT http_notifications_endpoint MODE \
             NO_ACK SEQUENTIAL ON QUIESCE BUFFER MAX SIZE 1MiB DECODE USING notification_codec TO \
             notifications BRANCHED BY user_id_branch SET user_id = message.user_id FLUSH EACH \
             100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG",
            " ON GENERAL ERROR LOG;",
        ),
        (
            "emitter",
            "CREATE EMITTER kafka_emit FROM notifications TO KAFKA kafka_main TOPIC \
             notifications_out MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING \
             notification_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG",
            " ON GENERAL ERROR LOG;",
        ),
    ];

    for (node, prefix, suffix) in external_cases {
        let full = format!("{prefix}{suffix}");
        parse_statement(&full).unwrap_or_else(|error| {
            panic!("{node} should parse with message and general error policies: {error:?}")
        });

        let missing_both = format!("{};", prefix.replace(" ON MESSAGE ERROR LOG", ""));
        assert!(
            parse_statement(&missing_both).is_err(),
            "{node} should reject missing error policies"
        );

        let missing_general = format!("{prefix};");
        assert!(
            parse_statement(&missing_general).is_err(),
            "{node} should reject missing general error policy"
        );
    }

    let processor_cases = [
        (
            "reingestor",
            "CREATE REINGESTOR repartition FROM notifications TO tenant_notifications BRANCHED BY \
             tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
             MESSAGE ERROR LOG",
        ),
        (
            "junction",
            "CREATE JUNCTION join_streams FROM notifications_a, notifications_b BRANCHED BY \
             tenant_branch TO notifications_all INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB \
             ON MESSAGE ERROR LOG",
        ),
        (
            "deduplicator",
            "CREATE DEDUPLICATOR dedup_txns FROM inbound DEDUPLICATE ON input.transaction_id MAX \
             TIME 10m BRANCHED BY tenant_branch TO deduped INHERIT ALL FLUSH EACH 100ms MAX BATCH \
             SIZE 1MiB ON MESSAGE ERROR LOG",
        ),
        (
            "window processor",
            "CREATE WINDOW PROCESSOR latency_window FROM metrics WIDTH 10s DURATION STEP 5s \
             DURATION BRANCHED BY tenant_branch TO metric_summaries SET total_latency = \
             SUM(input.latency) ON MESSAGE ERROR LOG",
        ),
        (
            "generator",
            "CREATE GENERATOR synth USING MATERIALIZED STATE notifications EACH 100ms BRANCHED BY \
             tenant_branch TO alerts SET user_id = relay_state.notifications.user_id FLUSH EACH \
             100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG",
        ),
        (
            "inferencer",
            r#"CREATE INFERENCER score_model FROM features USING RESOURCE fraud_model VERSION 3 FILE 'models/fraud.onnx' INPUTS { "features" DENSE TENSOR<F32>[2] = input.vector } OUTPUT SCHEMA { "score" DENSE TENSOR<F32>[1] } UNBRANCHED TO scored SET score = score FLUSH IMMEDIATE ON MESSAGE ERROR LOG"#,
        ),
    ];

    for (node, prefix) in processor_cases {
        let full = format!("{prefix};");
        parse_statement(&full).unwrap_or_else(|error| {
            panic!("{node} should parse with message error policy: {error:?}")
        });

        let missing_both = format!("{};", prefix.replace(" ON MESSAGE ERROR LOG", ""));
        assert!(
            parse_statement(&missing_both).is_err(),
            "{node} should reject missing error policies"
        );

        let with_general =
            format!("{prefix} ON GENERAL ERROR LOG FLUSH EACH 100ms MAX BATCH SIZE 1MiB;");
        assert!(
            parse_statement(&with_general).is_err(),
            "{node} should reject general error policy"
        );
    }
}

#[test]
fn message_error_policy_accepts_send_to_and_rejects_legacy_dlq() {
    let parsed = parse_statement(
        "CREATE REINGESTOR pass_through FROM notifications TO forwarded_notifications BRANCHED BY \
         tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON \
         MESSAGE ERROR SEND TO error_stream SET error_message = error.message;",
    )
    .expect("message error SEND TO policy should parse");
    let Statement::Create(parsed) = parsed else {
        panic!("expected create statement");
    };
    let canonical = parsed.to_canonical_nspl().expect("policy should render");
    assert!(
        canonical
            .contains("ON MESSAGE ERROR SEND TO error_stream SET error_message = error.message")
    );
    assert!(!canonical.contains("ON MESSAGE ERROR DLQ"));

    assert!(
        parse_statement(
            "CREATE REINGESTOR pass_through FROM notifications TO forwarded_notifications \
             BRANCHED BY tenant_branch SET tenant = message.tenant FLUSH EACH 100ms MAX BATCH \
             SIZE 1MiB ON MESSAGE ERROR DLQ error_stream SET error_message = error.message;",
        )
        .is_err(),
        "legacy DLQ syntax must be rejected"
    );
}

#[test]
fn message_error_policy_completion_suggests_send_to_without_branch_leakage() {
    let input = "CREATE REINGESTOR pass_through FROM notifications TO forwarded_notifications \
                 UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR ";
    let suggestions = suggest_statement(input, input.len());

    assert!(suggestions.contains(&"SEND TO".to_string()));
    assert!(!suggestions.contains(&"DLQ".to_string()));
    assert!(!suggestions.contains(&"BRANCHED BY".to_string()));
    assert!(!suggestions.contains(&"UNBRANCHED".to_string()));
}

#[test]
fn branch_preserving_processors_accept_unbranched() {
    for statement in [
        "CREATE RELAY raw SCHEMA metric UNBRANCHED;",
        "CREATE DEDUPLICATOR dedup FROM raw DEDUPLICATE ON input.value MAX TIME 10m UNBRANCHED TO \
         projected INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        "CREATE REORDERER reorder FROM raw BY input.value MAX TIME 10s UNBRANCHED TO projected \
         INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        "CREATE JUNCTION join_streams FROM left, right UNBRANCHED TO joined INHERIT ALL FLUSH \
         EACH 100ms MAX BATCH SIZE 1MiB ON MESSAGE ERROR LOG;",
        "CREATE WINDOW PROCESSOR window_metrics FROM raw WIDTH 2 MESSAGES STEP 2 MESSAGES \
         UNBRANCHED TO projected SET value = COUNT(input.value) ON MESSAGE ERROR LOG;",
        "CREATE INFERENCER score FROM features USING RESOURCE fraud_model VERSION 1 FILE \
         'models/simple_score.onnx' INPUTS { \"features\" DENSE TENSOR<F32>[2] = input.vector } \
         OUTPUT SCHEMA { \"score\" DENSE TENSOR<F32>[1] } UNBRANCHED TO scored SET score = score \
         FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        "CREATE WASM PROCESSOR filter_even FROM raw USING RESOURCE wasm_filter VERSION 1 FILE \
         'processors/filter_even.wasm' MAX FUEL 1000000000 MAX MEMORY 64MiB UNBRANCHED TO \
         projected ON MESSAGE ERROR LOG ON GLOBAL ERROR LOG;",
    ] {
        parse_statement(statement).unwrap_or_else(|error| {
            panic!("statement should parse: {statement}\n{error:?}");
        });
    }
}

#[test]
fn drop_context_suggestions_do_not_leak_format_keywords() {
    let input = "DROP ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"SCHEMA".to_string()));
    assert!(suggestions.contains(&"WIRE".to_string()));
    assert!(suggestions.contains(&"CODEC".to_string()));
    assert!(suggestions.contains(&"CLIENT".to_string()));
    assert!(suggestions.contains(&"VHOST".to_string()));
    assert!(suggestions.contains(&"ENDPOINT".to_string()));
    assert!(suggestions.contains(&"NODE".to_string()));
    assert!(!suggestions.contains(&"JSON".to_string()));
    assert!(!suggestions.contains(&"AVRO".to_string()));
}

#[test]
fn create_client_name_completion_is_not_semantic_reference_lookup() {
    let input = "CREATE CLIENT ";
    let suggestions = suggest_statement(input, input.len());
    assert!(suggestions.contains(&"client_name".to_string()));
    assert!(!suggestions.contains(&"ref:client".to_string()));
}

#[rstest]
#[case::schema(
        r#"
            CREATE SCHEMA notification (
                user_id U32,
                created_at DATETIME,
                payload STRING
            );
        "#,
        None,
        &[]
    )]
#[case::rebind_resource(
        "REBIND RESOURCE bundle TO VERSION LATEST FOR CLIENT mounted, HASH MAP countries;",
        None,
        &["REBIND RESOURCE bundle TO VERSION LATEST FOR"]
    )]
#[case::wire_schema(
        r#"
            CREATE WIRE JSON SCHEMA notification_wire MODE STRICT (
                user_id integer,
                created_at string,
                payload object
            );
        "#,
        None,
        &[]
    )]
#[case::alter_schema(
        "ALTER SCHEMA events ADD FIELD note STRING OPTIONAL SENSITIVE, RENAME FIELD id TO \
             event_id, ALTER FIELD event_id SET TYPE I64, ALTER FIELD event_id DROP OPTIONAL, \
             ALTER FIELD note DROP SENSITIVE;",
        None,
        &[]
    )]
#[case::alter_wire_schema(
        "ALTER WIRE AVRO SCHEMA payload ADD FIELD note STRING OPTIONAL, ALTER FIELD id SET \
             TYPE LONG, ALTER FIELD note DROP OPTIONAL;",
        None,
        &[]
    )]
#[case::alter_wire_schema_mode(
        "ALTER WIRE JSON SCHEMA payload MODE LOOSE;",
        Some("ALTER WIRE JSON SCHEMA payload MODE LOOSE;"),
        &[]
    )]
#[case::alter_relay(
        "ALTER RELAY notifications SET CAPACITY 8, SET SCHEMA event_v2, SET BRANCHED BY \
             by_tenant, SET MATERIALIZED STATE LAST BY TIMESTAMP;",
        None,
        &[]
    )]
#[case::alter_junction(
        "ALTER JUNCTION route_events ADD FROM incoming_b WHERE input.kind = 'event', SET \
             COLLECT FOR 10ms MAX BATCH SIZE 1MiB, SET FILTER WHERE input.kind != '', ADD \
             MATERIALIZED STATE profiles REQUIRED WAIT, ADD ROUTE TO projected INHERIT ALL FLUSH \
             IMMEDIATE ON MESSAGE ERROR SEND TO errors SET code = error.code, SET DETACHED;",
        None,
        &[]
    )]
#[case::alter_deduplicator(
        "ALTER DEDUPLICATOR dedup_events ADD FROM incoming_b WHERE input.active, SET \
             DEDUPLICATE ON concat(input.tenant, ','), input.id, SET MAX TIME 20m, ADD ROUTE TO \
             audit INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG, SET DETACHED;",
        None,
        &[]
    )]
#[case::alter_reorderer(
        "ALTER REORDERER order_events ADD FROM incoming_b WHERE input.active, SET BY \
             concat(input.tenant, ','), input.id, SET MAX TIME 20m, ADD ROUTE TO audit INHERIT \
             ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG, SET DETACHED;",
        None,
        &[]
    )]
#[case::alter_emitter(
        "ALTER EMITTER event_sink SET TO ZEROMQ sink_b MODE NO_ACK RETRY POLICY BACKOFF 250ms \
             MAX 30s, SET CLIENT sink_c, SET ENCODE USING event_codec, SET COLLECT FOR 10ms MAX \
             BATCH SIZE 1MiB, SET DETACHED, SET FLUSH IMMEDIATE;",
        None,
        &[]
    )]
#[case::alter_ingestor(
        "ALTER INGESTOR event_source SET FROM ENDPOINT ingress_b MODE NO_ACK SEQUENTIAL ON \
             QUIESCE BUFFER MAX SIZE 1MiB, SET DECODE USING event_codec_v2, SET TIMESTAMP AT \
             occurred_at, SET FILTER WHERE input.active, REPLACE ROUTE TO events INHERIT ALL \
             UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG, SET GENERAL ERROR IGNORE;",
        None,
        &[]
    )]
#[case::alter_client_ingestor(
        "ALTER INGESTOR submit_events SET FROM CLIENT SCHEMA event MODE ACK PARALLEL MAX 8 ACK \
             TIMEOUT 20s RETRY POLICY BACKOFF 50ms MAX 2s ON QUIESCE SUSPEND, SET QUIESCE SUSPEND;",
        None,
        &[]
    )]
#[case::alter_reingestor(
        "ALTER REINGESTOR repartition ADD FROM incoming_b WHERE input.active, SET FILTER \
             WHERE concat(input.tenant, ',') != '', SET DETACHED, REPLACE ROUTE TO outgoing \
             INHERIT ALL UNBRANCHED FLUSH IMMEDIATE ON MESSAGE ERROR LOG;",
        None,
        &[]
    )]
#[case::alter_generator(
        "ALTER GENERATOR synth SET MATERIALIZED STATE state_v2, SET EACH 250ms, SET \
             UNBRANCHED, REPLACE ROUTE TO outgoing SET value = relay_state.state_v2.value FLUSH \
             IMMEDIATE ON MESSAGE ERROR LOG;",
        None,
        &[]
    )]
#[case::transport(
        r#"
            CREATE CLIENT kafka_main
              TYPE KAFKA
              CONFIG {
                'bootstrap.servers' = 'host1:9092,host2:9092',
                'enable.auto.commit' = true
              };
        "#,
        None,
        &[]
    )]
#[case::http_client(
        r#"
            CREATE CLIENT http_main
              TYPE HTTP
              CONFIG {
                'endpoint' = 'https://api.example.com/events',
                'method' = 'POST'
              };
        "#,
        None,
        &[]
    )]
#[case::otel_client(
        r#"
            CREATE CLIENT otel_main
              TYPE OTEL
              CONFIG {
                'endpoint' = 'https://collector.example.com:4317',
                'protocol' = 'grpc',
                'headers' = 'authorization=Bearer token',
                'compression' = 'gzip',
                'timeout_ms' = 5000
              };
        "#,
        None,
        &[]
    )]
#[case::rabbitmq_transport(
        r#"
            CREATE CLIENT rabbit_main
              TYPE RABBITMQ
              CONFIG {
                'addr' = 'amqp://guest:guest@localhost:5672/%2f',
                'connection_name' = 'nervix-rabbit'
              };
        "#,
        None,
        &[]
    )]
#[case::websockets_client(
        r#"
            CREATE CLIENT ws_main
              TYPE WEBSOCKETS
              CONFIG {
                'endpoint' = 'wss://api.example.com/ws',
                'subprotocol' = 'notifications'
              };
        "#,
        None,
        &[]
    )]
#[case::redis_transport(
        r#"
            CREATE CLIENT redis_main
              TYPE REDIS
              POOL SIZE MIN 1 MAX 4
              CONFIG {
                'addr' = 'redis://127.0.0.1:6379/',
                'read_timeout_ms' = 5000
              };
        "#,
        None,
        &[]
    )]
#[case::mqtt_transport(
        r#"
            CREATE CLIENT mqtt_main
              TYPE MQTT
              CONFIG {
                'addr' = 'mqtt://127.0.0.1:1883',
                'client_id' = 'nervix-mqtt'
              };
        "#,
        None,
        &[]
    )]
#[case::prometheus_transport(
        r#"
            CREATE CLIENT prom_main
              TYPE PROMETHEUS
              CONFIG {
                'addr' = 'http://127.0.0.1:9090'
              };
        "#,
        None,
        &[]
    )]
#[case::vhost(
        r#"
            CREATE VHOST my_vhost api.example.com, foo-bar.localhost WITH TLS tls_bundle VERSION 3;
        "#,
        None,
        &[]
    )]
#[case::endpoint(
        r#"
            CREATE ENDPOINT my_ws_endpoint
                ON edge
                PATH '/ws'
                TYPE WEBSOCKETS;
        "#,
        None,
        &[]
    )]
#[case::http_endpoint(
        r#"
            CREATE ENDPOINT my_http_endpoint
                ON edge
                PATH '/ingest'
                TYPE HTTP;
        "#,
        None,
        &[]
    )]
#[case::client_ingestor(
        r#"
            CREATE INGESTOR submit_events
                FROM CLIENT SCHEMA event
                    MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX 5s
                    ON QUIESCE SUSPEND
                TIMESTAMP NOW
                TO events
                    INHERIT ALL
                    BRANCHED BY by_customer SET customer_id = message.customer_id
                    FLUSH EACH 10ms MAX BATCH SIZE 1MiB
                    ON MESSAGE ERROR LOG
                ON GENERAL ERROR LOG;
        "#,
        None,
        &[]
    )]
#[case::ingestor(
        r#"
            CREATE INGESTOR kafka_notifications
                FROM
                    KAFKA kafka_main
                    TOPIC notifications
                    OFFSET BY CONSUMER GROUP nervix_consumer
                    MODE ACK PARALLEL MAX 10 BATCH TIMEOUT 500ms ACK TIMEOUT 30s RETRY POLICY BACKOFF 200ms MAX 5s
                ON QUIESCE SUSPEND DECODE USING notification_kafka_message
                TO notifications
                    BRANCHED BY user_id_kind_branch
                    SET user_id = message.user_id, kind = message.kind
                    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                    ON MESSAGE ERROR LOG
                ON GENERAL ERROR LOG;
        "#,
        None,
        &[]
    )]
#[case::prometheus_ingestor(
        r#"
            CREATE INGESTOR prom_samples
                FROM PROMETHEUS prom_main
                QUERY 'label_replace(vector(42.5), "source", "local", "", "")'
                EVERY 15s
                ON QUIESCE SUSPEND DECODE USING sample_codec
                TO samples
                    BRANCHED BY source_branch SET source = message.source
                    FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                    ON MESSAGE ERROR LOG
                ON GENERAL ERROR LOG;
        "#,
        None,
        &[]
    )]
#[case::stream(
        r#"
            CREATE RELAY p99_latency SCHEMA notification_schema UNBRANCHED;
        "#,
        None,
        &[]
    )]
#[case::wasm_processor_preserves_exact_limits(
        r#"
            CREATE WASM PROCESSOR normalize_events
                FROM events
                USING RESOURCE normalizer VERSION 1
                FILE "processor.wasm"
                MAX FUEL 1000000
                MAX MEMORY 64MiB
                UNBRANCHED
                TO normalized_events ON MESSAGE ERROR LOG
                ON GLOBAL ERROR LOG;
        "#,
        None,
        &["MAX FUEL 1000000\n  MAX MEMORY 64MiB"]
    )]
#[case::junction(
        r#"
            CREATE JUNCTION join_streams
                FROM ss1, ss2, ss3
                COLLECT FOR 25ms MAX BATCH SIZE 2MiB
                BRANCHED BY tenant
                TO ss10 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG;
        "#,
        None,
        &[]
    )]
#[case::junction_membership_ranges_and_extrema(
        r#"
            CREATE JUNCTION route_parcels
                FROM parcels WHERE input.status NOT IN ('void', 'test'),
                    archive WHERE input.id IN () OR lower(input.id) IN ('a', 'b')
                UNBRANCHED
                TO routed
                    SET within = input.weight BETWEEN 1.0 AND 50.0,
                        moved = input.carrier IS DISTINCT FROM input.preferred,
                        capped = clamp(greatest(input.weight, 0.0), 1.0, least(input.limit, 50.0))
                    WHERE input.weight NOT BETWEEN 60.0 AND 70.0
                        AND input.carrier IS NOT DISTINCT FROM 'dhl'
                    FLUSH IMMEDIATE
                    ON MESSAGE ERROR LOG;
        "#,
        None,
        &[
            "input.status NOT IN ('void', 'test')",
            "input.id IN () OR lower(input.id) IN ('a', 'b')",
            "within = input.weight BETWEEN 1.0 AND 50.0",
            "moved = input.carrier IS DISTINCT FROM input.preferred",
            "capped = clamp(greatest(input.weight, 0.0), 1.0, least(input.limit, 50.0))",
            "input.weight NOT BETWEEN 60.0 AND 70.0 AND input.carrier IS NOT DISTINCT FROM 'dhl'",
        ]
    )]
#[case::correlator_input_collection(
        r#"
            CREATE CORRELATOR correlate
                LEFT FROM left_current, left_archive
                COLLECT FOR 10ms
                RIGHT FROM right_current
                COLLECT FOR 20ms MAX BATCH SIZE 1MiB
                CORRELATE WHERE left.id = right.id
                MATCH EARLIEST
                MAX TIME 5s
                ON CORRELATION TIMEOUT DROP, DROP
                UNBRANCHED
                TO matched
                    SET id = left.id
                    FLUSH IMMEDIATE
                    ON MESSAGE ERROR LOG;
        "#,
        None,
        &[]
    )]
#[case::preserves_conditional_surface_forms(
        r#"
            CREATE JUNCTION conditional
                FROM source
                UNBRANCHED
                TO projected
                    INHERIT ALL
                    SET if_result = IF input.active THEN 1 ELSE 0 END,
                        simple_result = CASE input.kind
                            WHEN "primary" THEN 1
                            WHEN "secondary" THEN 2
                            ELSE 0
                        END,
                        searched_result = CASE
                            WHEN input.active THEN 1
                        END
                    FLUSH IMMEDIATE
                    ON MESSAGE ERROR LOG;
        "#,
        None,
        &[
            "IF input.active THEN 1 ELSE 0 END",
            "CASE input.kind WHEN",
            "CASE WHEN input.active THEN 1 END",
        ]
    )]
#[case::deduplicator(
        r#"
            CREATE DEDUPLICATOR dedup_txns
                FROM ss1
                DEDUPLICATE ON input.transaction_id
                MAX TIME 10m
                BRANCHED BY tenant
                TO ss2 INHERIT ALL FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG;
        "#,
        None,
        &[]
    )]
#[case::expression_string_spanning_lines(
        "
            CREATE DEDUPLICATOR dedup_txns
                FROM ss1
                DEDUPLICATE ON input.transaction_id
                MAX TIME 10m
                BRANCHED BY tenant
                TO ss2 INHERIT ALL WHERE note = $s$line\nbreak$s$
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG;
        ",
        None,
        &[]
    )]
#[case::codec_with_multiline_jaq_program(
        r#"
            CREATE CODEC binance_ws_event_codec
                FROM JSON
                TO SCHEMA binance_ws_event
                WITH JAQ TRANSFORMATIONS ON INGESTION $jaq${
                    event_type: .e,
                    price: (if .e == "aggTrade" then .p else null end)
                }$jaq$;
        "#,
        None,
        &[]
    )]
#[case::preserves_float_literal_types(
        r#"
            CREATE DEDUPLICATOR dedup_txns
                FROM ss1
                DEDUPLICATE ON input.transaction_id
                MAX TIME 10m
                BRANCHED BY tenant
                TO ss2 INHERIT ALL WHERE battery_pct < 15.0 AND score >= 80.0
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG;
        "#,
        None,
        &[]
    )]
#[case::emitter(
        r#"
            CREATE EMITTER emit
                FROM p99
                COLLECT FOR 50ms
                TO KAFKA broker1 TOPIC topic MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
        None,
        &[]
    )]
#[case::pulsar_emitter(
        r#"
            CREATE EMITTER emit
                FROM p99
                TO PULSAR pulsar_main TOPIC topic MODE ACK PARALLEL MAX 16 ACK TIMEOUT 30s
                RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
        None,
        &[]
    )]
#[case::rabbitmq_emitter(
        r#"
            CREATE EMITTER emit
                FROM p99
                TO RABBITMQ broker1 QUEUE outbox MODE ACK SEQUENTIAL ACK TIMEOUT 30s
                RETRY POLICY BACKOFF 250ms MAX 30s ENCODE USING my_codec
                FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
        None,
        &[]
    )]
#[case::redis_emitter(
        r#"
            CREATE EMITTER emit
                FROM p99
                TO REDIS PUBSUB broker1 CHANNEL outbox MODE NO_ACK RETRY POLICY BACKOFF 250ms MAX 30s
                ENCODE USING my_codec FLUSH EACH 100ms MAX BATCH SIZE 1MiB
                ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;
        "#,
        None,
        &[]
    )]
fn canonical_statements_roundtrip(
    #[case] input: &str,
    #[case] expected_canonical: Option<&str>,
    #[case] expected_fragments: &[&str],
) {
    let parsed = parse_statement(input).expect("parse should succeed");
    let canonical = parsed.to_canonical_nspl().expect("must render canonical");
    if let Some(expected_canonical) = expected_canonical {
        assert_eq!(canonical, expected_canonical);
    }
    for expected_fragment in expected_fragments {
        assert!(canonical.contains(expected_fragment));
    }
    let reparsed = parse_statement(&canonical).expect("canonical parse should succeed");
    assert_eq!(parsed, reparsed);
}
