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

/// One change an `ALTER` makes to an emitter's envelope where it stands. `SET PROPERTIES`
/// replaces the complete property set; every other change preserves each envelope field it does
/// not name.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum EmitterEnvelopeAlteration {
    /// `SET <clause>`: the clause, in place of what the envelope wrote for it.
    SetClause { clause: EnvelopeClauseValue },
    /// `DROP <clause keywords>`: stops writing a clause the envelope writes.
    DropClause { clause: EnvelopeClauseKind },
    /// `SET PROPERTIES { … }`: the complete, nonempty property set.
    SetProperties { properties: Vec<EnvelopeProperty> },
    /// `DROP PROPERTIES`: stops setting every property the envelope sets.
    DropProperties,
}

impl EmitterEnvelopeAlteration {
    /// The clause the change names, or `None` for a change to the properties.
    pub fn clause(&self) -> Option<EnvelopeClauseKind> {
        match self {
            Self::SetClause { clause } => Some(clause.kind()),
            Self::DropClause { clause } => Some(*clause),
            Self::SetProperties { .. } | Self::DropProperties => None,
        }
    }
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

/// One envelope clause as an `ALTER` sets it: the clause, with what the statement writes after its
/// keywords.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum EnvelopeClauseValue {
    Persistent,
    Mandatory,
    Retain,
    Timestamp(Expression),
    /// The duration as the statement writes it.
    DeliverAfter(String),
    /// The duration as the statement writes it.
    Delay(String),
    ReplyTo(Expression),
    MessageId(Expression),
    DeduplicationId(Expression),
}

impl EnvelopeClauseValue {
    /// The clause as an envelope writes it.
    pub fn clause(&self) -> EnvelopeClause<'_> {
        match self {
            Self::Persistent => EnvelopeClause::Persistent,
            Self::Mandatory => EnvelopeClause::Mandatory,
            Self::Retain => EnvelopeClause::Retain,
            Self::Timestamp(expression) => EnvelopeClause::Timestamp(expression),
            Self::DeliverAfter(duration) => EnvelopeClause::DeliverAfter(duration),
            Self::Delay(duration) => EnvelopeClause::Delay(duration),
            Self::ReplyTo(expression) => EnvelopeClause::ReplyTo(expression),
            Self::MessageId(expression) => EnvelopeClause::MessageId(expression),
            Self::DeduplicationId(expression) => EnvelopeClause::DeduplicationId(expression),
        }
    }

    pub fn kind(&self) -> EnvelopeClauseKind {
        self.clause().kind()
    }
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

/// Why an emitter's envelope cannot be written for its entity, or why a change an `ALTER` makes
/// to it does not apply.
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
    #[error("{entity} emitters declare no PROPERTIES")]
    UndeclaredProperties { entity: EmitterEntityKind },
    #[error("SET PROPERTIES requires at least one key")]
    EmptyProperties,
    #[error(
        "the emitter's envelope writes no {keywords}, so there is no {keywords} to drop",
        keywords = .clause.keywords()
    )]
    ClauseNotWritten { clause: EnvelopeClauseKind },
    #[error("the emitter's envelope sets no PROPERTIES, so there are none to drop")]
    PropertiesNotWritten,
}

impl EmitterEnvelope {
    /// Applies `alteration` to this envelope of an emitter of `entity`, atomically. The change must
    /// name a clause or the properties `entity` declares, a `DROP` must name something this
    /// envelope writes, and the result must fit the declaration, its closed property keys
    /// included. A refused change leaves the envelope as it was.
    pub(crate) fn apply_alteration(
        &mut self,
        entity: EmitterEntityKind,
        alteration: &EmitterEnvelopeAlteration,
    ) -> error_stack::Result<(), EnvelopeDeclarationError> {
        let declared = entity.capabilities();
        match alteration.clause() {
            Some(clause) => {
                if !declared.declares_envelope_clause(clause) {
                    return Err(Report::new(EnvelopeDeclarationError::UndeclaredClause {
                        entity,
                        clause,
                    }));
                }
            }
            None => {
                if declared.properties.is_empty() {
                    return Err(Report::new(
                        EnvelopeDeclarationError::UndeclaredProperties { entity },
                    ));
                }
            }
        }
        let mut candidate = self.clone();
        match alteration {
            EmitterEnvelopeAlteration::SetClause { clause } => candidate.set_clause(clause),
            EmitterEnvelopeAlteration::DropClause { clause } => {
                if !candidate.drop_clause(*clause) {
                    return Err(Report::new(EnvelopeDeclarationError::ClauseNotWritten {
                        clause: *clause,
                    }));
                }
            }
            EmitterEnvelopeAlteration::SetProperties { properties } => {
                if properties.is_empty() {
                    return Err(Report::new(EnvelopeDeclarationError::EmptyProperties));
                }
                candidate.properties = properties.clone();
            }
            EmitterEnvelopeAlteration::DropProperties => {
                if candidate.properties.is_empty() {
                    return Err(Report::new(EnvelopeDeclarationError::PropertiesNotWritten));
                }
                candidate.properties.clear();
            }
        }
        candidate.check_declared(entity)?;
        *self = candidate;
        Ok(())
    }

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

    /// Writes `value`, in place of what this envelope wrote for its clause.
    pub fn set_clause(&mut self, value: &EnvelopeClauseValue) {
        match value {
            EnvelopeClauseValue::Persistent => self.persistent = true,
            EnvelopeClauseValue::Mandatory => self.mandatory = true,
            EnvelopeClauseValue::Retain => self.retain = true,
            EnvelopeClauseValue::Timestamp(expression) => {
                self.timestamp = Some(expression.clone());
            }
            EnvelopeClauseValue::DeliverAfter(duration) => {
                self.deliver_after = Some(duration.clone());
            }
            EnvelopeClauseValue::Delay(duration) => self.delay = Some(duration.clone()),
            EnvelopeClauseValue::ReplyTo(expression) => self.reply_to = Some(expression.clone()),
            EnvelopeClauseValue::MessageId(expression) => {
                self.message_id = Some(expression.clone());
            }
            EnvelopeClauseValue::DeduplicationId(expression) => {
                self.deduplication_id = Some(expression.clone());
            }
        }
    }

    /// Stops writing the clause `kind`, and says whether this envelope wrote it.
    pub fn drop_clause(&mut self, kind: EnvelopeClauseKind) -> bool {
        match kind {
            EnvelopeClauseKind::Persistent => std::mem::take(&mut self.persistent),
            EnvelopeClauseKind::Mandatory => std::mem::take(&mut self.mandatory),
            EnvelopeClauseKind::Retain => std::mem::take(&mut self.retain),
            EnvelopeClauseKind::Timestamp => self.timestamp.take().is_some(),
            EnvelopeClauseKind::DeliverAfter => self.deliver_after.take().is_some(),
            EnvelopeClauseKind::Delay => self.delay.take().is_some(),
            EnvelopeClauseKind::ReplyTo => self.reply_to.take().is_some(),
            EnvelopeClauseKind::MessageId => self.message_id.take().is_some(),
            EnvelopeClauseKind::DeduplicationId => self.deduplication_id.take().is_some(),
        }
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
