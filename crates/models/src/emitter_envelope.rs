//! What an emitter's transport carries beside each payload, as its statement writes it.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The envelope section of an emitter statement: the envelope clauses it writes and
//!   the properties it sets, each at most once, what each is written with, and whether its entity
//!   declares each of them.
//! - **Depends on.** Expressions, duration text, and the envelope clauses and properties an
//!   emitter entity declares.
//! - **Must not know.** How the grammar spells a clause, how the registry checks its types, or how
//!   a host evaluates an envelope and carries it to a connector.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use std::collections::BTreeSet;

use error_stack::Report;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{EmitterEntityCapabilities, EmitterEntityKind, EnvelopeClauseKind, Expression};

/// One key an envelope's `PROPERTIES { … }` sets, and the expression that computes its value for
/// every record.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct EnvelopeProperty {
    pub name: String,
    pub expression: Expression,
}

/// The envelope section of one emitter: what its statement writes after the payload, and before
/// its construction, to set what the transport carries beside each payload.
///
/// A statement writes each clause at most once, so each clause has one field, and a flag clause
/// is written or not. A statement writes the clauses in the order [`EnvelopeClauseKind`] lists
/// them, then `PROPERTIES`. The empty envelope is an emitter that writes none of them.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct EmitterEnvelope {
    /// `PERSISTENT`.
    pub persistent: bool,
    /// `MANDATORY`.
    pub mandatory: bool,
    /// `RETAIN`.
    pub retain: bool,
    /// `TIMESTAMP <expression>`.
    pub timestamp: Option<Expression>,
    /// `DELIVER AFTER <duration>`, as the statement writes the duration.
    pub deliver_after: Option<String>,
    /// `DELAY <duration>`, as the statement writes the duration.
    pub delay: Option<String>,
    /// `REPLY TO <expression>`.
    pub reply_to: Option<Expression>,
    /// `MESSAGE ID <expression>`.
    pub message_id: Option<Expression>,
    /// `DEDUPLICATION ID <expression>`.
    pub deduplication_id: Option<Expression>,
    /// `PROPERTIES { '<key>' = <expression>, … }`, in the order the statement writes its keys.
    pub properties: Vec<EnvelopeProperty>,
}

/// One clause an envelope writes, with what the statement writes after its keywords.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeClause<'a> {
    Persistent,
    Mandatory,
    Retain,
    Timestamp(&'a Expression),
    DeliverAfter(&'a str),
    Delay(&'a str),
    ReplyTo(&'a Expression),
    MessageId(&'a Expression),
    DeduplicationId(&'a Expression),
}

impl<'a> EnvelopeClause<'a> {
    /// The expression the clause evaluates for every record, for a clause whose form is one.
    pub fn expression(self) -> Option<&'a Expression> {
        match self {
            Self::Timestamp(expression)
            | Self::ReplyTo(expression)
            | Self::MessageId(expression)
            | Self::DeduplicationId(expression) => Some(expression),
            Self::Persistent
            | Self::Mandatory
            | Self::Retain
            | Self::DeliverAfter(_)
            | Self::Delay(_) => None,
        }
    }

    pub fn kind(self) -> EnvelopeClauseKind {
        match self {
            Self::Persistent => EnvelopeClauseKind::Persistent,
            Self::Mandatory => EnvelopeClauseKind::Mandatory,
            Self::Retain => EnvelopeClauseKind::Retain,
            Self::Timestamp(_) => EnvelopeClauseKind::Timestamp,
            Self::DeliverAfter(_) => EnvelopeClauseKind::DeliverAfter,
            Self::Delay(_) => EnvelopeClauseKind::Delay,
            Self::ReplyTo(_) => EnvelopeClauseKind::ReplyTo,
            Self::MessageId(_) => EnvelopeClauseKind::MessageId,
            Self::DeduplicationId(_) => EnvelopeClauseKind::DeduplicationId,
        }
    }
}

/// Why an emitter's envelope cannot be written for its entity.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[cfg_attr(
    nervix_lint,
    nervix::error_boundary(
        outcome,
        reason = "checking an envelope against its entity's declaration is a pure refusal"
    )
)]
pub enum EnvelopeDeclarationError {
    #[error("{entity} emitters declare no {} clause", .clause.keywords())]
    UndeclaredClause {
        entity: EmitterEntityKind,
        clause: EnvelopeClauseKind,
    },
    #[error("{entity} emitters declare no PROPERTIES key '{name}'")]
    UndeclaredProperty {
        entity: EmitterEntityKind,
        name: String,
    },
    #[error("PROPERTIES sets key '{name}' more than once")]
    DuplicateProperty { name: String },
}

impl EmitterEnvelope {
    /// Whether the statement writes no envelope clause and sets no property.
    pub fn is_empty(&self) -> bool {
        self.clauses().is_empty() && self.properties.is_empty()
    }

    /// Every clause this envelope writes, in the order a statement writes them.
    pub fn clauses(&self) -> Vec<EnvelopeClause<'_>> {
        let mut clauses = Vec::new();
        if self.persistent {
            clauses.push(EnvelopeClause::Persistent);
        }
        if self.mandatory {
            clauses.push(EnvelopeClause::Mandatory);
        }
        if self.retain {
            clauses.push(EnvelopeClause::Retain);
        }
        if let Some(expression) = &self.timestamp {
            clauses.push(EnvelopeClause::Timestamp(expression));
        }
        if let Some(duration) = &self.deliver_after {
            clauses.push(EnvelopeClause::DeliverAfter(duration));
        }
        if let Some(duration) = &self.delay {
            clauses.push(EnvelopeClause::Delay(duration));
        }
        if let Some(expression) = &self.reply_to {
            clauses.push(EnvelopeClause::ReplyTo(expression));
        }
        if let Some(expression) = &self.message_id {
            clauses.push(EnvelopeClause::MessageId(expression));
        }
        if let Some(expression) = &self.deduplication_id {
            clauses.push(EnvelopeClause::DeduplicationId(expression));
        }
        clauses
    }

    /// Checks that `entity` declares every clause this envelope writes and every property it sets,
    /// and that it sets each property once.
    pub fn check_declared(
        &self,
        entity: EmitterEntityKind,
    ) -> error_stack::Result<(), EnvelopeDeclarationError> {
        match self.declaration_violation(entity) {
            Some(violation) => Err(Report::new(violation)),
            None => Ok(()),
        }
    }

    /// The first clause or property this envelope writes that `entity` does not declare, or the
    /// first property it sets twice.
    ///
    /// The registry reports a violation in its own error and the vocabulary in its own, so neither
    /// has to rebuild the other's report.
    pub fn declaration_violation(
        &self,
        entity: EmitterEntityKind,
    ) -> Option<EnvelopeDeclarationError> {
        self.violation_against(entity, &entity.capabilities())
    }

    /// The first violation of `declared`, the declaration of `entity`, this envelope makes.
    fn violation_against(
        &self,
        entity: EmitterEntityKind,
        declared: &EmitterEntityCapabilities,
    ) -> Option<EnvelopeDeclarationError> {
        for clause in self.clauses() {
            let kind = clause.kind();
            if !declared.declares_envelope_clause(kind) {
                return Some(EnvelopeDeclarationError::UndeclaredClause {
                    entity,
                    clause: kind,
                });
            }
        }
        let mut set = BTreeSet::new();
        for property in &self.properties {
            if declared.property(&property.name).is_none() {
                return Some(EnvelopeDeclarationError::UndeclaredProperty {
                    entity,
                    name: property.name.clone(),
                });
            }
            if !set.insert(property.name.as_str()) {
                return Some(EnvelopeDeclarationError::DuplicateProperty {
                    name: property.name.clone(),
                });
            }
        }
        None
    }
}

#[cfg(test)]
#[path = "emitter_envelope_tests.rs"]
mod tests;
