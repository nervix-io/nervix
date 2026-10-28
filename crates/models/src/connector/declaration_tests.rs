//! Owner checks for the types an entity's declaration gives its envelope values.
//!
//! Layer: test harness.
//! - **Owns.** Evidence that each property type and each envelope clause names the schema type its
//!   value has.
//! - **Depends on.** The declaration shapes and the schema types.
//! - **Must not know.** Any connector, the grammar, or how a value is checked or evaluated.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns declaration type evidence")
)]

use strum::IntoEnumIterator as _;

use super::{EnvelopeClauseForm, EnvelopeClauseKind, PropertyType};
use crate::ParseAsType;

#[test]
fn a_property_value_has_exactly_the_schema_type_its_property_type_names() {
    let expected = [
        (PropertyType::U8, ParseAsType::U8),
        (PropertyType::I8, ParseAsType::I8),
        (PropertyType::U16, ParseAsType::U16),
        (PropertyType::I16, ParseAsType::I16),
        (PropertyType::U32, ParseAsType::U32),
        (PropertyType::I32, ParseAsType::I32),
        (PropertyType::U64, ParseAsType::U64),
        (PropertyType::I64, ParseAsType::I64),
        (PropertyType::Bool, ParseAsType::Bool),
        (PropertyType::String, ParseAsType::String),
        (PropertyType::Datetime, ParseAsType::Datetime),
        (PropertyType::Bytes, ParseAsType::Bytes),
    ];
    assert_eq!(
        PropertyType::iter().count(),
        expected.len(),
        "every property type has its schema type"
    );
    for (property_type, schema_type) in expected {
        assert_eq!(ParseAsType::from(property_type), schema_type);
        assert_eq!(
            property_type.to_string(),
            schema_type.to_string(),
            "a property type is written as the schema type of its value"
        );
    }
}

#[test]
fn each_envelope_clause_takes_the_form_its_value_has() {
    for kind in EnvelopeClauseKind::iter() {
        let expected = match kind {
            EnvelopeClauseKind::Persistent
            | EnvelopeClauseKind::Mandatory
            | EnvelopeClauseKind::Retain => EnvelopeClauseForm::Flag,
            EnvelopeClauseKind::DeliverAfter | EnvelopeClauseKind::Delay => {
                EnvelopeClauseForm::Duration
            }
            EnvelopeClauseKind::Timestamp => EnvelopeClauseForm::Expression(ParseAsType::Datetime),
            EnvelopeClauseKind::ReplyTo
            | EnvelopeClauseKind::MessageId
            | EnvelopeClauseKind::DeduplicationId => {
                EnvelopeClauseForm::Expression(ParseAsType::String)
            }
        };
        assert_eq!(kind.form(), expected, "{}", kind.keywords());
    }
}
