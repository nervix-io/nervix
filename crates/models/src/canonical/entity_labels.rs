//! Canonical rendering of shared entity metadata.
//!
//! Layer: vocabulary.
//! - **Owns.** One trailing label clause and the common metadata command forms.
//! - **Depends on.** Exact labels, existing typed references and the shared string renderer.
//! - **Must not know.** Parsing, registry state or execution identity.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns metadata canonical text")
)]

use meticulous::OptionExt as _;

use super::{Clause, INDENT, string_literal};
use crate::{EntityLabelMutation, EntityLabels, MutateEntityLabels, ShowLabels};

pub(super) fn append_labels(mut creation: String, labels: &EntityLabels) -> String {
    if labels.is_empty() {
        return creation;
    }
    creation
        .pop()
        .assured("every canonical creation renderer ends its statement with a semicolon");
    let mut lines = vec![creation];
    Clause::braced("LABELS", assignments(labels)).append_to(INDENT, &mut lines);
    let mut rendered = lines.join("\n");
    rendered.push(';');
    rendered
}

fn assignments(labels: &EntityLabels) -> Vec<String> {
    labels
        .iter()
        .map(|(key, value)| {
            format!(
                "{} = {}",
                string_literal(key.as_str()),
                string_literal(value)
            )
        })
        .collect()
}

impl MutateEntityLabels {
    pub fn to_canonical_plexus(&self) -> String {
        let target = format!(
            "{} {}",
            self.entity.kind.keyword_phrase(),
            self.entity.identifier
        );
        match &self.mutation {
            EntityLabelMutation::Set(labels) => format!(
                "SET LABELS ON {target} {{ {} }};",
                assignments(labels).join(", ")
            ),
            EntityLabelMutation::Drop(keys) => {
                let keys = keys
                    .iter()
                    .map(|key| string_literal(key.as_str()))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("DROP LABELS ON {target} {{ {keys} }};")
            }
        }
    }
}

impl ShowLabels {
    pub fn to_canonical_plexus(&self) -> String {
        format!(
            "SHOW LABELS ON {} {} FORMAT {};",
            self.entity.kind.keyword_phrase(),
            self.entity.identifier,
            self.format.as_ref()
        )
    }
}
