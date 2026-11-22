//! Canonical rendering of shared entity metadata.
//!
//! Layer: vocabulary.
//! - **Owns.** One trailing label clause and the common metadata command forms: one `SET LABELS`
//!   edit and its inspection.
//! - **Depends on.** Exact labels, existing typed references and the shared string renderer.
//! - **Must not know.** Parsing, registry state or execution identity.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns metadata canonical text")
)]

use meticulous::OptionExt as _;

use super::{Clause, INDENT, string_literal};
use crate::{EntityLabels, LabelChange, MutateEntityLabels, ShowLabels};

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
    /// One `SET LABELS` statement: every entry in key order, a set value as an assignment and a
    /// removal as `DROP`.
    pub fn to_canonical_plexus(&self) -> String {
        let mut entries = Vec::with_capacity(self.mutation.len());
        for (key, change) in self.mutation.iter() {
            let key = string_literal(key.as_str());
            let entry = match change {
                LabelChange::Set(value) => format!("{key} = {}", string_literal(value)),
                LabelChange::Drop => format!("{key} DROP"),
            };
            entries.push(entry);
        }
        format!(
            "SET LABELS ON {} {{ {} }};",
            self.entity,
            entries.join(", ")
        )
    }
}

impl ShowLabels {
    pub fn to_canonical_plexus(&self) -> String {
        format!(
            "SHOW LABELS ON {} FORMAT {};",
            self.entity,
            self.format.as_ref()
        )
    }
}
