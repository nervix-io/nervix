//! Incomplete declared envelope fields and ordered external value mappings.
//!
//! Layer: edges.
//! - **Owns.** Browser text for declared envelope values, properties and direct mappings.
//! - **Depends on.** Envelope declarations, semantic expressions and their parser.
//! - **Must not know.** External transports or how a connector encodes a property.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns emitter envelope drafts")
)]

use error_stack::{Report, ResultExt as _};
use indexmap::IndexMap;
use nervix_models::{
    EmitterEntityKind, EmitterEnvelope, EmitterValueMapping, EnvelopeClauseForm,
    EnvelopeClauseKind, EnvelopeClauseValue, EnvelopeProperty,
};
use nervix_plexus::parse_expression;

use super::emitter_draft::{EmitterDraftError, required};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct ValueDraft {
    pub(super) name: String,
    pub(super) expression: String,
}

impl ValueDraft {
    pub(super) fn build(&self) -> error_stack::Result<EmitterValueMapping, EmitterDraftError> {
        Ok(EmitterValueMapping {
            column: self.name.clone(),
            expression: parse_expression(&self.expression)
                .change_context(EmitterDraftError::Invalid("mapped value expression"))?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum EnvelopeValueDraft {
    Flag,
    Duration(String),
    Expression(String),
}

impl EnvelopeValueDraft {
    pub(super) fn text(&self) -> Option<&str> {
        match self {
            Self::Flag => None,
            Self::Duration(value) | Self::Expression(value) => Some(value),
        }
    }

    pub(super) fn set_text(&mut self, text: String) {
        match self {
            Self::Flag => {}
            Self::Duration(value) | Self::Expression(value) => *value = text,
        }
    }

    fn build(
        &self,
        kind: EnvelopeClauseKind,
    ) -> error_stack::Result<EnvelopeClauseValue, EmitterDraftError> {
        match (kind, self) {
            (EnvelopeClauseKind::PartitionFromBranch, Self::Flag) => {
                Ok(EnvelopeClauseValue::PartitionFromBranch)
            }
            (EnvelopeClauseKind::Persistent, Self::Flag) => Ok(EnvelopeClauseValue::Persistent),
            (EnvelopeClauseKind::Mandatory, Self::Flag) => Ok(EnvelopeClauseValue::Mandatory),
            (EnvelopeClauseKind::Retain, Self::Flag) => Ok(EnvelopeClauseValue::Retain),
            (EnvelopeClauseKind::DeliverAfter, Self::Duration(text)) => Ok(
                EnvelopeClauseValue::DeliverAfter(required(text, kind.keywords())?),
            ),
            (EnvelopeClauseKind::Delay, Self::Duration(text)) => {
                Ok(EnvelopeClauseValue::Delay(required(text, kind.keywords())?))
            }
            (_, Self::Expression(text)) => {
                let expression = parse_expression(text)
                    .change_context(EmitterDraftError::Invalid(kind.keywords()))?;
                match kind {
                    EnvelopeClauseKind::Key => Ok(EnvelopeClauseValue::Key(expression)),
                    EnvelopeClauseKind::Timestamp => Ok(EnvelopeClauseValue::Timestamp(expression)),
                    EnvelopeClauseKind::ReplyTo => Ok(EnvelopeClauseValue::ReplyTo(expression)),
                    EnvelopeClauseKind::MessageId => Ok(EnvelopeClauseValue::MessageId(expression)),
                    EnvelopeClauseKind::DeduplicationId => {
                        Ok(EnvelopeClauseValue::DeduplicationId(expression))
                    }
                    _ => Err(Report::new(EmitterDraftError::Invalid(kind.keywords()))),
                }
            }
            _ => Err(Report::new(EmitterDraftError::Invalid(kind.keywords()))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct EnvelopeDraft {
    pub(super) clauses: IndexMap<EnvelopeClauseKind, EnvelopeValueDraft>,
    pub(super) properties: Vec<ValueDraft>,
}

impl EnvelopeDraft {
    pub(super) fn toggle(&mut self, kind: EnvelopeClauseKind, enabled: bool) {
        if !enabled {
            self.clauses.shift_remove(&kind);
            return;
        }
        self.clauses
            .entry(kind)
            .or_insert_with(|| match kind.form() {
                EnvelopeClauseForm::Flag => EnvelopeValueDraft::Flag,
                EnvelopeClauseForm::Duration => EnvelopeValueDraft::Duration(String::new()),
                EnvelopeClauseForm::Expression(_) => EnvelopeValueDraft::Expression(String::new()),
            });
    }

    pub(super) fn build(
        &self,
        kind: EmitterEntityKind,
    ) -> error_stack::Result<EmitterEnvelope, EmitterDraftError> {
        let mut envelope = EmitterEnvelope::default();
        for (kind, value) in &self.clauses {
            envelope.set_clause(&value.build(*kind)?);
        }
        for property in &self.properties {
            let value = property.build()?;
            envelope.properties.push(EnvelopeProperty {
                name: value.column,
                expression: value.expression,
            });
        }
        envelope
            .check_declared(kind)
            .change_context(EmitterDraftError::Invalid("declared envelope"))?;
        Ok(envelope)
    }
}
