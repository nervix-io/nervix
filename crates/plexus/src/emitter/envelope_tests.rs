//! The envelope section read against a declaration that declares every clause kind.
//!
//! Layer: test harness.
//! - **Owns.** Positive, negative and completion-context cases of the shared envelope grammar.
//! - **Depends on.** The envelope section parser, the lexer and completion.
//! - **Must not know.** Which entities declare clauses; generated conformance tests cover those.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use nervix_models::{Literal, PropertyType, expression_to_plexus};

use super::*;
use crate::semantic_program::parse_expression;

/// Every clause kind, which no entity declares together; the section reads each declared one.
const EVERY_CLAUSE: &[EnvelopeClauseKind] = &[
    EnvelopeClauseKind::Persistent,
    EnvelopeClauseKind::Mandatory,
    EnvelopeClauseKind::Retain,
    EnvelopeClauseKind::Timestamp,
    EnvelopeClauseKind::DeliverAfter,
    EnvelopeClauseKind::Delay,
    EnvelopeClauseKind::ReplyTo,
    EnvelopeClauseKind::MessageId,
    EnvelopeClauseKind::DeduplicationId,
];
const PROPERTIES: &[PropertyDeclaration] = &[
    PropertyDeclaration {
        name: "content_type",
        ty: PropertyType::String,
    },
    PropertyDeclaration {
        name: "priority",
        ty: PropertyType::U8,
    },
];

/// The envelope `text` writes, read as an entity that declares `clauses` and [`PROPERTIES`], or
/// the diagnostics refusing it.
fn read(clauses: &'static [EnvelopeClauseKind], text: &str) -> Result<EmitterEnvelope, String> {
    read_declared(clauses, PROPERTIES, text)
}

/// The envelope `text` writes, read as an entity that declares `clauses` and `properties`.
fn read_declared(
    clauses: &'static [EnvelopeClauseKind],
    properties: &'static [PropertyDeclaration],
    text: &str,
) -> Result<EmitterEnvelope, String> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(text).expect("every case lexes");
    let parsed = envelope_section(clauses, properties)
        .then_ignore(end())
        .parse(tokens.as_slice())
        .into_result();
    match parsed {
        Ok(envelope) => Ok(envelope),
        Err(errors) => {
            let refused = into_parse_error(source, &spanned_tokens, text.len(), errors);
            Err(format!("{refused:#}"))
        }
    }
}

/// What completion offers after `text`, where the section of an entity that declares `clauses`
/// and [`PROPERTIES`] is followed by `FLUSH`.
fn offered(clauses: &'static [EnvelopeClauseKind], text: &str) -> Vec<String> {
    offered_declared(clauses, PROPERTIES, text)
}

/// What completion offers after `text`, where the section of an entity that declares `clauses`
/// and `properties` is followed by `FLUSH`.
fn offered_declared(
    clauses: &'static [EnvelopeClauseKind],
    properties: &'static [PropertyDeclaration],
    text: &str,
) -> Vec<String> {
    suggest_from!(
        text,
        text.len(),
        envelope_section(clauses, properties).then_ignore(kw(Identifier::Flush))
    )
}

fn expression(text: &str) -> Expression {
    parse_expression(text).expect("every expression case parses")
}

#[test]
fn every_declared_clause_and_property_is_read_in_written_order() {
    let envelope = read(
        EVERY_CLAUSE,
        "PERSISTENT MANDATORY RETAIN TIMESTAMP input.occurred_at DELIVER AFTER 5s DELAY 250ms \
         REPLY TO 'replies' MESSAGE ID input.id DEDUPLICATION ID concat(input.id, '-1') \
         PROPERTIES {'priority' = 9, 'content_type' = 'application/json'}",
    )
    .expect("every declared clause is read");

    assert_eq!(
        envelope,
        EmitterEnvelope {
            persistent: true,
            mandatory: true,
            retain: true,
            timestamp: Some(expression("input.occurred_at")),
            deliver_after: Some("5s".to_string()),
            delay: Some("250ms".to_string()),
            reply_to: Some(Expression::Literal(Literal::String("replies".to_string()))),
            message_id: Some(expression("input.id")),
            deduplication_id: Some(expression("concat(input.id, '-1')")),
            properties: vec![
                EnvelopeProperty {
                    name: "priority".to_string(),
                    expression: expression("9"),
                },
                EnvelopeProperty {
                    name: "content_type".to_string(),
                    expression: expression("'application/json'"),
                },
            ],
        }
    );
}

#[test]
fn an_entity_that_declares_nothing_reads_no_token() {
    assert_eq!(read_declared(&[], &[], ""), Ok(EmitterEnvelope::default()));
    for clause in EnvelopeClauseKind::iter() {
        assert!(
            read_declared(&[], &[], clause.example()).is_err(),
            "{clause:?}"
        );
    }
    assert!(read_declared(&[], &[], "PROPERTIES {'priority' = 1}").is_err());
    assert_eq!(offered_declared(&[], &[], ""), ["FLUSH"]);
}

#[test]
fn each_declared_clause_may_be_left_out() {
    assert_eq!(read(EVERY_CLAUSE, ""), Ok(EmitterEnvelope::default()));
    let envelope = read(EVERY_CLAUSE, "RETAIN MESSAGE ID input.id").expect("two clauses read");
    assert_eq!(
        envelope,
        EmitterEnvelope {
            retain: true,
            message_id: Some(expression("input.id")),
            ..EmitterEnvelope::default()
        }
    );
}

#[test]
fn a_clause_its_entity_does_not_declare_is_refused() {
    const WITHOUT_DELAY: &[EnvelopeClauseKind] = &[
        EnvelopeClauseKind::Persistent,
        EnvelopeClauseKind::Timestamp,
    ];
    read(WITHOUT_DELAY, "PERSISTENT TIMESTAMP input.at").expect("declared clauses read");
    let refused = read(WITHOUT_DELAY, "PERSISTENT DELAY 5s").expect_err("DELAY is not declared");
    assert!(refused.contains("DELAY"), "{refused}");
}

#[test]
fn clauses_written_out_of_order_or_twice_are_refused() {
    assert!(read(EVERY_CLAUSE, "TIMESTAMP input.at PERSISTENT").is_err());
    assert!(read(EVERY_CLAUSE, "DELAY 5s DELIVER AFTER 5s").is_err());
    assert!(read(EVERY_CLAUSE, "PERSISTENT PERSISTENT").is_err());
    assert!(read(EVERY_CLAUSE, "MESSAGE ID a MESSAGE ID b").is_err());
    assert!(read(EVERY_CLAUSE, "PROPERTIES {'priority' = 1} RETAIN").is_err());
}

#[test]
fn a_property_outside_the_declared_set_or_set_twice_is_refused() {
    let undeclared = read(EVERY_CLAUSE, "PROPERTIES {'expiration' = '60000'}")
        .expect_err("an undeclared property is refused");
    assert!(undeclared.contains("expiration"), "{undeclared}");

    let twice = read(EVERY_CLAUSE, "PROPERTIES {'priority' = 1, 'priority' = 2}")
        .expect_err("a property set twice is refused");
    assert!(
        twice.contains("PROPERTIES sets key 'priority' more than once"),
        "{twice}"
    );

    assert!(read(EVERY_CLAUSE, "PROPERTIES {}").is_err());
}

#[test]
fn a_duration_clause_takes_a_valid_duration_literal() {
    assert!(read(EVERY_CLAUSE, "DELIVER AFTER 5").is_err());
    assert!(read(EVERY_CLAUSE, "DELAY input.delay").is_err());
    assert!(read(EVERY_CLAUSE, "DELAY 5parsecs").is_err());
}

#[test]
fn completion_offers_each_declared_clause_as_one_item_in_written_order() {
    let mut expected = vec![
        "DEDUPLICATION ID",
        "DELAY",
        "DELIVER AFTER",
        "FLUSH",
        "MANDATORY",
        "MESSAGE ID",
        "PERSISTENT",
        "PROPERTIES",
        "REPLY TO",
        "RETAIN",
        "TIMESTAMP",
    ];
    assert_eq!(offered(EVERY_CLAUSE, ""), expected);

    // Only the clauses after the one written last are still offered.
    expected = vec![
        "DEDUPLICATION ID",
        "FLUSH",
        "MESSAGE ID",
        "PROPERTIES",
        "REPLY TO",
    ];
    assert_eq!(offered(EVERY_CLAUSE, "PERSISTENT DELAY 5s "), expected);
}

#[test]
fn completion_offers_only_the_declared_clauses() {
    const DECLARED: &[EnvelopeClauseKind] =
        &[EnvelopeClauseKind::Mandatory, EnvelopeClauseKind::ReplyTo];
    assert_eq!(
        offered(DECLARED, ""),
        ["FLUSH", "MANDATORY", "PROPERTIES", "REPLY TO"]
    );
    assert_eq!(
        offered_declared(DECLARED, &[], ""),
        ["FLUSH", "MANDATORY", "REPLY TO"]
    );
}

#[test]
fn completion_inside_properties_offers_exactly_the_declared_keys() {
    assert_eq!(
        offered(EVERY_CLAUSE, "PROPERTIES {"),
        ["\"content_type\"", "\"priority\""]
    );
}

#[test]
fn the_canonical_section_reads_back_as_the_same_envelope() {
    let envelope = read(
        EVERY_CLAUSE,
        "MANDATORY TIMESTAMP input.at DELAY 1m REPLY TO input.reply PROPERTIES {'content_type' = \
         'text/plain'}",
    )
    .expect("the case reads");

    let canonical = envelope
        .to_canonical_plexus()
        .expect("the envelope renders");

    assert_eq!(
        canonical,
        format!(
            "MANDATORY TIMESTAMP {} DELAY 1m REPLY TO {} PROPERTIES {{'content_type' = \
             'text/plain'}}",
            expression_to_plexus(&expression("input.at")).expect("renders"),
            expression_to_plexus(&expression("input.reply")).expect("renders"),
        )
    );
    assert_eq!(read(EVERY_CLAUSE, &canonical), Ok(envelope));
}

/// Every envelope an entity that declares every clause and property could write renders to
/// canonical text that the shared envelope grammar reads back as the same envelope.
#[test]
fn bolero_envelope_sections_read_back_as_the_envelopes_they_render() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(2048)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary =
                nervix_arbitrary::Arbitrary::new(bytes, nervix_arbitrary::Domain::Plexus);
            let envelope = arbitrary.emitter_envelope(EVERY_CLAUSE, PROPERTIES);
            let canonical = envelope
                .to_canonical_plexus()
                .expect("a generated envelope renders");
            assert_eq!(read(EVERY_CLAUSE, &canonical), Ok(envelope), "{canonical}");
        });
}
