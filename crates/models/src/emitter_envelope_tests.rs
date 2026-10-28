//! Envelope sections checked against the clauses and properties their entity declares.
//!
//! Layer: test harness.
//! - **Owns.** The order an envelope lists its clauses in, and which envelopes a declaration
//!   admits.
//! - **Depends on.** The envelope Model and the emitter entity declarations.
//! - **Must not know.** The grammar, the registry or the host.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]

use strum::IntoEnumIterator as _;

use super::*;
use crate::{EnvelopeClauseForm, KafkaEmitterEntity, Literal, PropertyDeclaration, PropertyType};

/// The entity whose declaration the synthetic ones below extend.
const ENTITY: EmitterEntityKind = EmitterEntityKind::Kafka(KafkaEmitterEntity::Topic);

/// A declaration of three clauses and two properties, which no real entity makes yet.
const CLAUSES: &[EnvelopeClauseKind] = &[
    EnvelopeClauseKind::Persistent,
    EnvelopeClauseKind::Timestamp,
    EnvelopeClauseKind::MessageId,
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

fn declared() -> EmitterEntityCapabilities {
    EmitterEntityCapabilities {
        envelope: CLAUSES,
        properties: PROPERTIES,
        ..ENTITY.capabilities()
    }
}

fn text(value: &str) -> Expression {
    Expression::Literal(Literal::String(value.to_string()))
}

fn property(name: &str) -> EnvelopeProperty {
    EnvelopeProperty {
        name: name.to_string(),
        expression: text("application/json"),
    }
}

/// An envelope that writes every clause kind once.
fn every_clause() -> EmitterEnvelope {
    EmitterEnvelope {
        persistent: true,
        mandatory: true,
        retain: true,
        timestamp: Some(text("timestamp")),
        deliver_after: Some("5s".to_string()),
        delay: Some("1s".to_string()),
        reply_to: Some(text("replies")),
        message_id: Some(text("id")),
        deduplication_id: Some(text("dedup")),
        properties: Vec::new(),
    }
}

#[test]
fn the_empty_envelope_writes_no_clause_and_sets_no_property() {
    let envelope = EmitterEnvelope::default();

    assert!(envelope.is_empty());
    assert!(envelope.clauses().is_empty());
}

#[test]
fn an_envelope_lists_its_clauses_in_the_order_a_statement_writes_them() {
    let envelope = every_clause();

    let kinds = envelope
        .clauses()
        .into_iter()
        .map(EnvelopeClause::kind)
        .collect::<Vec<_>>();

    assert_eq!(kinds, EnvelopeClauseKind::iter().collect::<Vec<_>>());
    assert!(!envelope.is_empty());
}

#[test]
fn an_envelope_that_only_sets_properties_is_not_empty() {
    let envelope = EmitterEnvelope {
        properties: vec![property("content_type")],
        ..EmitterEnvelope::default()
    };

    assert!(envelope.clauses().is_empty());
    assert!(!envelope.is_empty());
}

#[test]
fn every_clause_and_property_its_entity_declares_is_admitted() {
    let envelope = EmitterEnvelope {
        persistent: true,
        timestamp: Some(text("timestamp")),
        message_id: Some(text("id")),
        properties: vec![property("priority"), property("content_type")],
        ..EmitterEnvelope::default()
    };

    assert_eq!(envelope.violation_against(ENTITY, &declared()), None);
}

/// An envelope that writes `kind` and nothing else.
fn only(kind: EnvelopeClauseKind) -> EmitterEnvelope {
    let mut envelope = EmitterEnvelope::default();
    match kind {
        EnvelopeClauseKind::Persistent => envelope.persistent = true,
        EnvelopeClauseKind::Mandatory => envelope.mandatory = true,
        EnvelopeClauseKind::Retain => envelope.retain = true,
        EnvelopeClauseKind::Timestamp => envelope.timestamp = Some(text("timestamp")),
        EnvelopeClauseKind::DeliverAfter => envelope.deliver_after = Some("5s".to_string()),
        EnvelopeClauseKind::Delay => envelope.delay = Some("1s".to_string()),
        EnvelopeClauseKind::ReplyTo => envelope.reply_to = Some(text("replies")),
        EnvelopeClauseKind::MessageId => envelope.message_id = Some(text("id")),
        EnvelopeClauseKind::DeduplicationId => {
            envelope.deduplication_id = Some(text("dedup"));
        }
    }
    envelope
}

#[test]
fn a_clause_its_entity_does_not_declare_is_refused_by_name() {
    for kind in EnvelopeClauseKind::iter() {
        if CLAUSES.contains(&kind) {
            continue;
        }

        assert_eq!(
            only(kind).violation_against(ENTITY, &declared()),
            Some(EnvelopeDeclarationError::UndeclaredClause {
                entity: ENTITY,
                clause: kind,
            })
        );
    }
}

#[test]
fn a_property_its_entity_does_not_declare_is_refused_by_name() {
    let envelope = EmitterEnvelope {
        properties: vec![property("content_type"), property("expiration")],
        ..EmitterEnvelope::default()
    };

    assert_eq!(
        envelope.violation_against(ENTITY, &declared()),
        Some(EnvelopeDeclarationError::UndeclaredProperty {
            entity: ENTITY,
            name: "expiration".to_string(),
        })
    );
}

#[test]
fn a_property_set_twice_is_refused() {
    let envelope = EmitterEnvelope {
        properties: vec![property("priority"), property("priority")],
        ..EmitterEnvelope::default()
    };

    assert_eq!(
        envelope.violation_against(ENTITY, &declared()),
        Some(EnvelopeDeclarationError::DuplicateProperty {
            name: "priority".to_string(),
        })
    );
}

#[test]
fn every_entity_refuses_each_clause_and_property_it_does_not_declare() {
    for entity in EmitterEntityKind::all() {
        EmitterEnvelope::default()
            .check_declared(entity)
            .expect("an emitter that writes no clause and sets no property declares nothing");
        let declared = entity.capabilities();
        for kind in EnvelopeClauseKind::iter() {
            if declared.declares_envelope_clause(kind) {
                continue;
            }
            let refused = only(kind)
                .check_declared(entity)
                .expect_err("an undeclared clause is refused");
            assert_eq!(
                refused.current_context(),
                &EnvelopeDeclarationError::UndeclaredClause {
                    entity,
                    clause: kind,
                },
                "{entity}"
            );
        }
        if declared.property("content_type").is_none() {
            let envelope = EmitterEnvelope {
                properties: vec![property("content_type")],
                ..EmitterEnvelope::default()
            };
            let refused = envelope
                .check_declared(entity)
                .expect_err("an undeclared property is refused");
            assert_eq!(
                refused.current_context(),
                &EnvelopeDeclarationError::UndeclaredProperty {
                    entity,
                    name: "content_type".to_string(),
                },
                "{entity}"
            );
        }
    }
}

#[test]
fn a_clause_holds_an_expression_exactly_where_its_form_is_one() {
    for clause in every_clause().clauses() {
        let computed = matches!(clause.kind().form(), EnvelopeClauseForm::Expression(_));
        assert_eq!(
            clause.expression().is_some(),
            computed,
            "{:?}",
            clause.kind()
        );
    }
}

#[test]
fn an_emitter_presents_the_envelope_it_writes_among_its_inspection_rows() {
    let entity =
        crate::EmitterEntity::ZeroMq(crate::ZeroMqEmitter::Push(crate::ZeroMqPushEmitter {
            mode: crate::ZeroMqPushPublishingMode::NoAck {
                retry_policy: crate::RetryPolicy {
                    backoff: "250ms".to_string(),
                    max_backoff: "30s".to_string(),
                },
            },
            client: crate::ClientName::parse("zeromq_main").expect("valid client name"),
        }));
    let envelope = EmitterEnvelope {
        persistent: true,
        delay: Some("1s".to_string()),
        message_id: Some(text("id")),
        properties: vec![property("content_type")],
        ..EmitterEnvelope::default()
    };

    let plain = entity.entity_presentation().expect("the entity renders");
    let written = entity
        .entity_presentation()
        .expect("the entity renders")
        .with_envelope(&envelope)
        .expect("the envelope renders");

    assert_eq!(
        plain.describe_lines().last().map(String::as_str),
        Some("envelope: none")
    );
    assert_eq!(
        written.describe_lines().last().map(String::as_str),
        Some(
            "envelope: PERSISTENT DELAY 1s MESSAGE ID 'id' PROPERTIES {'content_type' = \
             'application/json'}"
        )
    );
    assert_eq!(
        envelope
            .to_canonical_plexus()
            .expect("the envelope renders"),
        "PERSISTENT DELAY 1s MESSAGE ID 'id' PROPERTIES {'content_type' = 'application/json'}"
    );
    assert_eq!(
        EmitterEnvelope::default()
            .to_canonical_plexus()
            .expect("the empty envelope renders"),
        ""
    );
}

#[test]
fn an_emitter_statement_writes_its_envelope_after_its_payload() {
    let emitter = crate::CreateEmitter {
        name: crate::EmitterName::try_from("publish_events").expect("valid emitter name"),
        from: crate::ProcessorInputs::single(
            crate::RelayName::try_from("events").expect("valid relay name"),
        ),
        body: crate::EmitterBody::Codec {
            codec: crate::CodecName::try_from("events_codec").expect("valid codec name"),
        },
        envelope: Box::new(EmitterEnvelope {
            persistent: true,
            message_id: Some(text("id")),
            properties: vec![
                property("content_type"),
                EnvelopeProperty {
                    name: "priority".to_string(),
                    expression: Expression::Literal(Literal::I64(5)),
                },
            ],
            ..EmitterEnvelope::default()
        }),
        connector: Box::new(crate::EmitterEntity::ZeroMq(crate::ZeroMqEmitter::Push(
            crate::ZeroMqPushEmitter {
                mode: crate::ZeroMqPushPublishingMode::NoAck {
                    retry_policy: crate::RetryPolicy {
                        backoff: "250ms".to_string(),
                        max_backoff: "30s".to_string(),
                    },
                },
                client: crate::ClientName::parse("zeromq_main").expect("valid client name"),
            },
        ))),
        batch: None,
        flush_policy: crate::FlushPolicy::Immediate,
        mode: crate::AckMode::Attached,
        error_policies: crate::ErrorPolicies::handled_by_log(),
        construction: crate::RouteConstruction::default(),
        materialized_state: Vec::new(),
    };

    // Each clause takes its own line beside the payload, and the PROPERTIES block follows them,
    // before the construction a statement would write and the emitter's batching and flush.
    assert_eq!(
        emitter.to_canonical_plexus().expect("the emitter renders"),
        "CREATE ATTACHED ZEROMQ PUSH EMITTER publish_events\n  FROM events\n  TO zeromq_main\n    \
         MODE NO ACK RETRY POLICY BACKOFF 250ms MAX 30s\n    ENCODE USING events_codec\n    \
         PERSISTENT\n    MESSAGE ID 'id'\n    PROPERTIES {\n      'content_type' = \
         'application/json',\n      'priority' = 5\n    }\n  FLUSH IMMEDIATE\n  ON MESSAGE ERROR \
         LOG\n  ON GENERAL ERROR LOG;"
    );
}
