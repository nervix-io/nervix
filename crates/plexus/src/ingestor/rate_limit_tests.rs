//! The `RATE LIMIT` clause through the composed statement grammar.
//!
//! Layer: test harness.
//!
//! - **Owns.** What a written clause becomes, what the grammar refuses inside one, the alterations
//!   that replace or drop the clauses, and completion inside a clause.
//! - **Depends on.** The composed statement grammar, its completion and canonical Plexus.
//! - **Must not know.** Which entities declare which modes; the generated conformance tests read
//!   that from the declarations.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_models::{
    AlterIngestorOperation, AlterTransportIngestorOperation, Expression, FieldScope,
    IngestorAlteration, IngestorEntity, IngestorInput, KafkaIngestor, KafkaTopicIngestor,
    KafkaTopicIngestorAlteration, Model, RateLimits, Statement,
};

use crate::{
    conformance_support::{assert_offers_exactly, assert_reads_back_canonically, created_model},
    parser_support::ParseFromSourceError,
    statement::parse_statement,
};

/// A `KAFKA TOPIC` ingestor whose body writes `clauses` after its delivery mode.
fn kafka(clauses: &str) -> String {
    format!(
        "CREATE KAFKA TOPIC INGESTOR limited FROM broker TOPIC 'orders' OFFSET BY DOMAIN MODE NO \
         ACK {clauses} ON QUIESCE SUSPEND DECODE USING codec TO events UNBRANCHED FLUSH IMMEDIATE \
         ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
    )
}

fn rate_limits(statement: &str) -> RateLimits {
    let Some(Model::Ingestor(ingestor)) = created_model(statement) else {
        panic!("the grammar refuses {statement}");
    };
    ingestor.input.rate_limits().clone()
}

fn refusal_messages(statement: &str) -> Vec<String> {
    let Err(error) = parse_statement(statement) else {
        panic!("the grammar accepts {statement}");
    };
    let ParseFromSourceError::Parse { diagnostics, .. } = error.current_context() else {
        panic!("expected a parse error for {statement}, got {error:?}");
    };
    diagnostics
        .iter()
        .map(|diagnostic| diagnostic.message.clone())
        .collect()
}

#[test]
fn a_clause_keeps_its_counts_and_period_as_written() {
    let limits = rate_limits(&kafka(
        "RATE LIMIT 16MiB OF payload.bytes PER 1s BURST 64MiB",
    ));
    let [clause] = limits.clauses() else {
        panic!("one clause: {limits:?}");
    };
    assert_eq!(clause.count.as_str(), "16MiB");
    assert_eq!(clause.period, "1s");
    assert_eq!(clause.burst.as_str(), "64MiB");
    assert_eq!(
        clause.count.units().map(|units| units.get()).ok(),
        Some(16 * 1024 * 1024)
    );
    let Some(Expression::Field(field)) = &clause.measure else {
        panic!("payload.bytes is a field read: {:?}", clause.measure);
    };
    assert_eq!(field.scope, FieldScope::Payload);
    assert_eq!(field.field.as_str(), "bytes");
}

#[test]
fn clauses_keep_their_written_order_and_read_back_canonically() {
    let statement = kafka(
        "RATE LIMIT 4 PER 1s BURST 1 RATE LIMIT 60 PER 1m BURST 3 RATE LIMIT 1KiB OF \
         payload.bytes - (10 AS U64) PER 10s BURST 2KiB",
    );
    let limits = rate_limits(&statement);
    let counts: Vec<_> = limits
        .clauses()
        .iter()
        .map(|clause| clause.count.as_str())
        .collect();
    assert_eq!(counts, ["4", "60", "1KiB"]);
    let parsed = parse_statement(&statement).expect("the statement parses");
    assert_reads_back_canonically(&parsed);
    let canonical = parsed.to_canonical_plexus().expect("the statement renders");
    assert!(
        canonical.contains(
            "MODE NO ACK RATE LIMIT 4 PER 1s BURST 1 RATE LIMIT 60 PER 1m BURST 3 RATE LIMIT 1KiB \
             OF payload.bytes - 10 AS U64 PER 10s BURST 2KiB ON QUIESCE SUSPEND"
        ),
        "{canonical}"
    );
}

#[test]
fn a_count_or_burst_that_counts_nothing_is_refused_where_it_is_written() {
    for clauses in [
        "RATE LIMIT 0 PER 1s BURST 10",
        "RATE LIMIT 10 PER 1s BURST 0",
        "RATE LIMIT 0KiB PER 1s BURST 10",
    ] {
        let messages = refusal_messages(&kafka(clauses));
        assert!(
            messages
                .iter()
                .any(|message| message == "a RATE LIMIT count must be greater than 0"),
            "{clauses}: {messages:?}"
        );
    }
}

#[test]
fn a_clause_needs_its_period_and_its_burst() {
    for clauses in [
        "RATE LIMIT 10 BURST 10",
        "RATE LIMIT 10 PER 1s",
        "RATE LIMIT PER 1s BURST 10",
        "RATE LIMIT 10 OF PER 1s BURST 10",
    ] {
        assert!(
            parse_statement(&kafka(clauses)).is_err(),
            "{clauses} is refused"
        );
    }
}

#[test]
fn completion_walks_a_clause_keyword_by_keyword() {
    let head = "CREATE KAFKA TOPIC INGESTOR limited FROM broker TOPIC 'orders' OFFSET BY DOMAIN \
                MODE NO ACK ";
    assert_offers_exactly(head, &["RATE LIMIT", "ON QUIESCE"]);
    assert_offers_exactly(&format!("{head}RATE LIMIT "), &["rate_limit_count"]);
    assert_offers_exactly(&format!("{head}RATE LIMIT 10 "), &["OF", "PER"]);
    assert_offers_exactly(&format!("{head}RATE LIMIT 10 PER "), &["duration_literal"]);
    assert_offers_exactly(&format!("{head}RATE LIMIT 10 PER 1s "), &["BURST"]);
    assert_offers_exactly(
        &format!("{head}RATE LIMIT 10 PER 1s BURST "),
        &["rate_limit_burst"],
    );
    assert_offers_exactly(
        &format!("{head}RATE LIMIT 10 PER 1s BURST 10 "),
        &["RATE LIMIT", "ON QUIESCE"],
    );
}

/// The operations of `ALTER KAFKA TOPIC INGESTOR limited <operations>`.
fn kafka_operations(operations: &str) -> Vec<AlterTransportIngestorOperation<KafkaTopicIngestor>> {
    let statement = format!("ALTER KAFKA TOPIC INGESTOR limited {operations};");
    let Ok(Statement::AlterIngestor(alter)) = parse_statement(&statement) else {
        panic!("the grammar refuses {statement}");
    };
    let IngestorAlteration::Kafka(KafkaTopicIngestorAlteration::Topic(operations)) =
        alter.alteration
    else {
        panic!("{statement} alters a KAFKA TOPIC ingestor");
    };
    operations
}

#[test]
fn set_rate_limit_replaces_every_clause_and_drop_rate_limit_removes_them() {
    let operations = kafka_operations(
        "SET RATE LIMIT 2 PER 1s BURST 2 RATE LIMIT 30 PER 1m BURST 5, DROP RATE LIMIT",
    );
    let [set, drop] = operations.as_slice() else {
        panic!("two operations: {operations:?}");
    };
    let AlterTransportIngestorOperation::Ingestor(set) = set else {
        panic!("SET RATE LIMIT is an operation every held ingestor accepts: {set:?}");
    };
    let AlterIngestorOperation::SetRateLimit { limits } = set.as_ref() else {
        panic!("SET RATE LIMIT sets the clauses: {set:?}");
    };
    assert_eq!(limits.clauses().len(), 2);
    assert!(matches!(
        drop,
        AlterTransportIngestorOperation::Ingestor(operation)
            if matches!(operation.as_ref(), AlterIngestorOperation::DropRateLimit)
    ));

    let created = kafka("RATE LIMIT 1 PER 1h BURST 1");
    let Some(Model::Ingestor(mut ingestor)) = created_model(&created) else {
        panic!("the grammar refuses {created}");
    };
    let alter = "ALTER KAFKA TOPIC INGESTOR limited SET RATE LIMIT 2 PER 1s BURST 2 RATE LIMIT 30 \
                 PER 1m BURST 5;";
    let Ok(Statement::AlterIngestor(alter)) = parse_statement(alter) else {
        panic!("the grammar refuses {alter}");
    };
    ingestor
        .apply_alter(&alter)
        .expect("SET RATE LIMIT applies to a held ingestor");
    let counts: Vec<_> = ingestor
        .input
        .rate_limits()
        .clauses()
        .iter()
        .map(|clause| clause.count.as_str().to_owned())
        .collect();
    assert_eq!(counts, ["2", "30"]);
    let drop = "ALTER KAFKA TOPIC INGESTOR limited DROP RATE LIMIT;";
    let Ok(Statement::AlterIngestor(drop)) = parse_statement(drop) else {
        panic!("the grammar refuses {drop}");
    };
    ingestor
        .apply_alter(&drop)
        .expect("DROP RATE LIMIT applies to a held ingestor");
    assert!(ingestor.input.rate_limits().is_empty());
    let IngestorInput::Transport(input) = &ingestor.input else {
        panic!("a Kafka ingestor reads a transport");
    };
    let IngestorEntity::Kafka(KafkaIngestor::Topic(_)) = &input.source else {
        panic!("the alteration kept the Kafka topic body");
    };
}

#[test]
fn the_alterations_read_back_canonically() {
    for operations in [
        "SET RATE LIMIT 16MiB OF payload.bytes PER 1s BURST 64MiB",
        "SET RATE LIMIT 2 PER 1s BURST 2 RATE LIMIT 30 PER 1m BURST 5",
        "DROP RATE LIMIT",
    ] {
        let statement = format!("ALTER KAFKA TOPIC INGESTOR limited {operations};");
        let parsed = parse_statement(&statement).expect("the alteration parses");
        assert_reads_back_canonically(&parsed);
    }
}

#[test]
fn an_entity_that_cannot_hold_its_source_back_refuses_the_alterations_by_name() {
    for operation in ["SET RATE LIMIT 10 PER 1s BURST 10", "DROP RATE LIMIT"] {
        let statement = format!("ALTER NATS SUBJECT INGESTOR intake {operation};");
        let messages = refusal_messages(&statement);
        assert!(
            messages.iter().any(|message| message
                == "NATS SUBJECT ingestors cannot hold their source back, so they take no RATE \
                    LIMIT"),
            "{statement}: {messages:?}"
        );
        let statement = format!("ALTER PROMETHEUS QUERY INGESTOR intake {operation};");
        let messages = refusal_messages(&statement);
        assert!(
            messages.iter().any(|message| message
                == "PROMETHEUS QUERY ingestors read on their EVERY cadence, so they take no RATE \
                    LIMIT"),
            "{statement}: {messages:?}"
        );
    }
}

#[test]
fn completion_offers_the_alterations_only_where_the_entity_holds_its_source_back() {
    let held = crate::statement::suggest_statement(
        "ALTER RABBITMQ QUEUE INGESTOR intake SET ",
        "ALTER RABBITMQ QUEUE INGESTOR intake SET ".len(),
    );
    assert!(held.iter().any(|item| item == "RATE LIMIT"), "{held:?}");
    let unheld = crate::statement::suggest_statement(
        "ALTER NATS SUBJECT INGESTOR intake SET ",
        "ALTER NATS SUBJECT INGESTOR intake SET ".len(),
    );
    assert!(
        !unheld.iter().any(|item| item == "RATE LIMIT"),
        "{unheld:?}"
    );
}
