//! Typed references a field picker inserts into the expression text of a visual draft.
//!
//! Layer: edges.
//!
//! - **Owns.** The shapes a picker inserts, a scoped field or one call over a scoped field, and
//!   appending their canonical spelling to editable expression text.
//! - **Depends on.** The expression vocabulary and its canonical renderer.
//! - **Must not know.** Which scopes an expression may read, function semantics, or validation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

use meticulous::ResultExt as _;
use nervix_models::{BuiltinFunctionName, Expression, FieldReference, expression_to_plexus};

/// A reference a picker inserts where the operator is writing an expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum InsertedReference {
    /// A field read in its scope, such as `left.order_id`.
    Field(FieldReference),
    /// A call whose one argument is a field read in its scope, such as `count(input.latency)`.
    Call {
        function: BuiltinFunctionName,
        argument: FieldReference,
    },
}

impl InsertedReference {
    fn expression(&self) -> Expression {
        match self {
            Self::Field(field) => Expression::Field(field.clone()),
            Self::Call { function, argument } => Expression::Call {
                function: function.clone(),
                arguments: vec![Expression::Field(argument.clone())],
            },
        }
    }

    /// Appends this reference, spelled as the canonical renderer spells it, to `text`, separated
    /// from what is already typed by a space.
    pub(super) fn append_to(&self, text: &mut String) {
        let rendered = expression_to_plexus(&self.expression()).assured(
            "a field reference, or a call over one, holds no float literal, the only expression \
             that fails to render",
        );
        if !text.trim().is_empty() && !text.ends_with(char::is_whitespace) {
            text.push(' ');
        }
        text.push_str(&rendered);
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_models::{FieldName, FieldScope};

    use super::*;

    fn field(scope: FieldScope, name: &str) -> FieldReference {
        FieldReference::scoped(scope, FieldName::parse(name).assured("test field is valid"))
    }

    #[test]
    fn references_append_in_canonical_spelling_after_a_separating_space() {
        let mut text = String::new();
        InsertedReference::Field(field(FieldScope::Left, "order_id")).append_to(&mut text);
        assert_eq!(text, "left.order_id");
        text.push_str(" = ");
        InsertedReference::Field(field(FieldScope::Right, "order_id")).append_to(&mut text);
        assert_eq!(text, "left.order_id = right.order_id");

        let mut text = "max(input.latency) -".to_string();
        InsertedReference::Call {
            function: BuiltinFunctionName::parse("MIN").assured("valid function"),
            argument: field(FieldScope::Input, "latency"),
        }
        .append_to(&mut text);
        assert_eq!(text, "max(input.latency) - min(input.latency)");
    }
}
