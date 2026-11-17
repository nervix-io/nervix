//! `RATE LIMIT` admission through the production registry validators.
//!
//! Layer: test harness.
//! - **Owns.** The refusals of a clause written where its entity cannot hold its source back, of a
//!   clause that declares no allowance, and of a measure the host cannot run before decoding.
//! - **Depends on.** Typed Models and pure registry validation inputs.
//! - **Must not know.** Connector drivers, runtime execution, storage or scheduling.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "rate-limit registry admission checks")
)]

use nervix_arbitrary::{Arbitrary, Domain};
use nervix_models::{
    BinaryOperator, BuiltinFunctionName, FieldReference, FieldScope, IngestorEntityKind,
    KafkaIngestor, Literal, RabbitMqIngestMode, RabbitMqIngestor, RabbitMqIngestorEntity,
    RateLimit, RateLimits, RateQuantity,
};

use super::*;
use crate::registry::{
    RateLimitClauseIssue,
    test_fixtures::{ingestor_statement, named},
};

fn quantity(text: &str) -> RateQuantity {
    RateQuantity::parse(text).assured("the test writes a positive count")
}

fn clause(count: &str, measure: Option<Expression>, period: &str) -> RateLimit {
    RateLimit {
        count: quantity(count),
        measure,
        period: period.to_owned(),
        burst: quantity("10"),
    }
}

fn payload(field: &str) -> Expression {
    Expression::Field(FieldReference::scoped(FieldScope::Payload, named(field)))
}

/// The Kafka fixture ingestor, which holds its source back in every mode, writing `limits`.
fn kafka(limits: Vec<RateLimit>) -> CreateIngestor {
    let mut ingestor = ingestor_statement("intake", "events", "codec", "broker", &[]);
    let IngestorInput::Transport(input) = &mut ingestor.input else {
        panic!("the fixture ingestor reads a transport");
    };
    let IngestorEntity::Kafka(KafkaIngestor::Topic(entity)) = &mut input.source else {
        panic!("the fixture ingestor reads a Kafka topic");
    };
    entity.rate_limits = RateLimits::from(limits);
    ingestor
}

fn measures(ingestor: &CreateIngestor) -> Result<(), Report<RegistryError>> {
    validate_ingestor_rate_limit_measures(
        &named("rate_domain"),
        &named("intake"),
        &ModelIndex::default(),
        &ingestor.input,
    )
}

fn clauses(ingestor: &CreateIngestor) -> Result<(), Report<RegistryError>> {
    validate_ingestor_rate_limit_clauses(&named("rate_domain"), &named("intake"), &ingestor.input)
}

/// The issue a measure refusal names for the clause at `clause`, from 1.
fn measure_issue(
    result: Result<(), Report<RegistryError>>,
    clause: usize,
) -> RateLimitMeasureIssue {
    let Err(error) = result else {
        panic!("the measure was admitted");
    };
    let RegistryError::InvalidConnectorDeclaration {
        issue:
            ConnectorDeclarationIssue::RateLimitMeasure {
                clause: refused,
                issue,
            },
        ..
    } = error.current_context()
    else {
        panic!("a measure refusal names its clause: {error:?}");
    };
    assert_eq!(*refused, clause, "{error:?}");
    *issue
}

#[test]
fn a_measure_over_payload_facts_is_admitted() {
    let measure = Expression::Binary {
        operator: BinaryOperator::Subtract,
        left: Box::new(payload("bytes")),
        right: Box::new(Expression::Cast {
            expression: Box::new(Expression::Literal(Literal::I64(10))),
            target: ParseAsType::U64,
        }),
    };
    let ingestor = kafka(vec![
        clause("10", None, "1s"),
        clause("1KiB", Some(measure), "1s"),
    ]);
    measures(&ingestor).assured("an exact U64 over payload.bytes costs a payload before decoding");
    clauses(&ingestor).assured("both clauses declare an allowance");
}

#[test]
fn a_measure_reading_what_only_decoding_knows_is_refused() {
    let decoded = Expression::Field(FieldReference::scoped(FieldScope::Message, named("seq")));
    let ingestor = kafka(vec![
        clause("10", None, "1s"),
        clause("10", Some(decoded), "1s"),
    ]);
    let issue = measure_issue(measures(&ingestor), 2);
    assert_eq!(issue, RateLimitMeasureIssue::Scope);
}

#[test]
fn a_measure_calling_a_function_the_payload_does_not_determine_is_refused() {
    let now = Expression::Call {
        function: BuiltinFunctionName::parse("now").assured("now is a builtin name"),
        arguments: Vec::new(),
    };
    let measure = Expression::Cast {
        expression: Box::new(now),
        target: ParseAsType::U64,
    };
    let issue = measure_issue(measures(&kafka(vec![clause("10", Some(measure), "1s")])), 1);
    assert_eq!(issue, RateLimitMeasureIssue::Nondeterministic);
}

#[test]
fn a_measure_that_is_not_an_exact_unsigned_count_is_refused() {
    let signed = Expression::Literal(Literal::I64(5));
    let issue = measure_issue(measures(&kafka(vec![clause("10", Some(signed), "1s")])), 1);
    assert_eq!(issue, RateLimitMeasureIssue::Program);
}

#[test]
fn a_measure_reading_a_payload_fact_its_entity_does_not_offer_is_refused() {
    let error = measures(&kafka(vec![clause("10", Some(payload("rows")), "1s")]))
        .expect_err("only a client producer offers payload.rows");
    assert!(
        matches!(
            error.current_context(),
            RegistryError::UndeclaredIngestorPayload { field, .. } if field.as_str() == "rows"
        ),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains("the measure of its RATE LIMIT clause 1"),
        "{error}"
    );
}

#[test]
fn a_clause_that_declares_no_allowance_is_refused_by_its_position() {
    for (limit, expected) in [
        (clause("10", None, "0s"), RateLimitClauseIssue::Period),
        (
            clause("2", None, "1ns"),
            RateLimitClauseIssue::Quota(nervix_rate_limit::QuotaError::CountExceedsPeriod),
        ),
    ] {
        let ingestor = kafka(vec![clause("10", None, "1s"), limit]);
        let error = clauses(&ingestor).expect_err("the second clause declares no allowance");
        let RegistryError::InvalidConnectorDeclaration {
            issue: ConnectorDeclarationIssue::RateLimitClause { clause, issue },
            ..
        } = error.current_context()
        else {
            panic!("a clause refusal names its clause: {error:?}");
        };
        assert_eq!((*clause, *issue), (2, expected), "{error:?}");
    }
}

#[test]
fn a_clause_under_a_mode_that_cannot_hold_its_source_back_is_refused_naming_the_mode() {
    let bytes = vec![3; 1024];
    let kind = IngestorEntityKind::RabbitMq(RabbitMqIngestorEntity::Queue);
    let mut ingestor = Arbitrary::new(&bytes, Domain::Plexus).create_ingestor_from(kind);
    let IngestorInput::Transport(input) = &mut ingestor.input else {
        panic!("a RabbitMQ ingestor reads a transport");
    };
    let IngestorEntity::RabbitMq(RabbitMqIngestor::Queue(entity)) = &mut input.source else {
        panic!("the generated ingestor reads a RabbitMQ queue");
    };
    entity.mode = RabbitMqIngestMode::NoAck;
    entity.rate_limits = RateLimits::from(vec![clause("10", None, "1s")]);
    let error = clauses(&ingestor).expect_err("RABBITMQ QUEUE pushes every delivery under NO ACK");
    assert!(
        matches!(
            error.current_context(),
            RegistryError::InvalidConnectorDeclaration {
                issue: ConnectorDeclarationIssue::RateLimit(_),
                ..
            }
        ),
        "{error:?}"
    );
    assert!(
        error.to_string().contains(
            "RABBITMQ QUEUE ingestors in MODE NO ACK cannot hold their source back, so they take \
             no RATE LIMIT"
        ),
        "{error}"
    );
}
