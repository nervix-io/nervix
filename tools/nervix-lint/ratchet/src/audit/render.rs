//! Repository tooling, outside the product layer order.
//! Owns: the audit's human-readable Markdown and summary messages.
//! Depends on: complete typed reports and the report vocabulary's serialized labels.
//! Must not know: Git processes or enforcement policy.

use std::{collections::BTreeSet, fmt::Write as _};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_lint_report::audit::AuditPattern;
use serde::Serialize;

use super::{
    candidates::{Candidate, Pattern, Relocated},
    report::{Attribution, Report},
};

fn label(value: &impl Serialize) -> String {
    let value = serde_json::to_value(value)
        .assured("the report vocabulary has infallible scalar and context encodings");
    let label = if value.is_string() {
        &value
    } else {
        &value["context"]
    };
    label
        .as_str()
        .verified("the enum label is serialized as a string")
        .into()
}

pub(super) fn detail(candidate: &Candidate) -> String {
    match &candidate.pattern {
        Pattern::Resolved(AuditPattern::RepeatedLookup {
            receiver,
            key,
            operations,
            guarded,
            ..
        }) => {
            let operations: Vec<_> = operations
                .iter()
                .map(|operation| operation.operation.as_str())
                .collect();
            let guarded = if *guarded { "guarded" } else { "sequential" };
            format!(
                "`{receiver}` key `{key}`: {} ({guarded})",
                operations.join(" -> ")
            )
        }
        Pattern::Resolved(AuditPattern::KeyAllocation {
            operation,
            allocation,
            key_type,
            ..
        }) => format!("{operation} with `{allocation}` ({key_type})"),
        Pattern::Resolved(AuditPattern::BorrowedCopy {
            operation,
            value_type,
        }) => format!("borrowed `{operation}` of {value_type}"),
        Pattern::Resolved(AuditPattern::DuplicatedBranch {
            construct,
            statement,
            occurrences,
        }) => format!(
            "{}: `{statement}` in {} branches",
            label(construct),
            occurrences.len()
        ),
        Pattern::Resolved(AuditPattern::ForwardingWrapper {
            callee,
            callers,
            contract,
            implementation,
            relocated,
        }) => {
            let mut flags = Vec::new();
            for (enabled, name) in [
                (*contract, "contract"),
                (*implementation, "implementation"),
                (*relocated, "relocated"),
            ] {
                if enabled {
                    flags.push(name);
                }
            }
            format!("to `{callee}`, {callers} callers {}", flags.join(" "))
                .trim()
                .into()
        }
        Pattern::Resolved(AuditPattern::DiscardedError {
            operation,
            error_type,
            information,
        }) => format!("{operation} drops {error_type} ({})", label(information)),
        Pattern::Resolved(AuditPattern::DisguisedClamp {
            operation,
            fallback,
        }) => format!("{operation} falls back to `{fallback}`"),
        Pattern::Relocated(Relocated::RelocatedAcquisition {
            receiver,
            operation,
            acquisition,
        }) => format!("{receiver}::{operation} ({})", label(acquisition)),
    }
}

pub(super) fn evidence_text(attribution: &Attribution) -> String {
    let Some(data) = attribution.data() else {
        return "—".into();
    };
    let rules = match &data.origin {
        Some(origin) => &origin.rules,
        None => &data.rules,
    };
    let mut facts = Vec::new();
    for rule in rules {
        let mut found = Vec::new();
        if let Some(event) = &rule.event
            && !event.is_empty()
        {
            found.push(event.clone());
        }
        if rule.spelling_removed {
            found.push("hunk removed spelling".into());
        } else if rule.file_spelling_removed {
            found.push("file removed spelling".into());
        } else if rule.commit_spelling_removed {
            found.push("commit removed spelling".into());
        }
        if let Some(rewrite) = &rule.pull_request_rewrite {
            found.push(format!("folded rewrite `{}`", prefix(&rewrite.sha, 8)));
        }
        if rule.written_while_counted {
            found.push("written while counted".into());
        }
        if rule.predates_rule {
            found.push("predates".into());
        }
        if !found.is_empty() {
            facts.push(format!("{}: {}", rule.rule, found.join(", ")));
        }
    }
    if facts.is_empty() {
        "—".into()
    } else {
        facts.join("; ")
    }
}

pub(super) fn prefix(text: &str, count: usize) -> String {
    text.chars().take(count).collect()
}

pub(super) fn markdown(report: &Report) -> String {
    let mut text = format!(
        "# Lint workaround audit\n\nRevision `{}`; source identity `{}`.\nConfigurations: \
         {}.\n\nCandidates are review input. A suggestion summarizes attribution evidence; it is \
         not a\nclassification.\n\n| Kind | Candidates | Recurring | Suspected | Possible | \
         Unattributed | Pre-existing |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n",
        report.revision,
        report.source_identity,
        report.configurations.join(", ")
    );
    for (kind, counts) in &report.summary.0 {
        writeln!(
            text,
            "| {kind} | {} | {} | {} | {} | {} | {} |",
            counts.total,
            counts.recurring,
            counts.suspected,
            counts.possible,
            counts.unattributed,
            counts.pre_existing
        )
        .assured("writing to a String cannot fail");
    }
    for kind in report.summary.0.keys() {
        writeln!(
            text,
            "\n## {kind}\n\n| ID | Location | Owner | Context | Detail | Suggestion | Commit | \
             Evidence |\n| --- | --- | --- | --- | --- | --- | --- | --- |"
        )
        .assured("writing to a String cannot fail");
        for record in &report.candidates {
            let candidate = &record.candidate;
            if candidate.kind() != kind {
                continue;
            }
            let mut introduced = match record.attribution.data() {
                Some(data) => format!("`{}` {}", prefix(&data.commit.sha, 8), data.commit.date),
                None => "—".into(),
            };
            if let Some(data) = record.attribution.data()
                && let Some(origin) = &data.origin
            {
                write!(
                    introduced,
                    ", origin `{}` {}",
                    prefix(&origin.sha, 8),
                    origin.date
                )
                .assured("writing to a String cannot fail");
            }
            let mut contexts = BTreeSet::new();
            for context in &candidate.contexts {
                let name = match context {
                    Some(context) => label(context),
                    None => "none".into(),
                };
                contexts.insert(name);
            }
            let contexts: Vec<_> = contexts.into_iter().collect();
            writeln!(
                text,
                "| `{}` | `{}:{}` | `{}` | {} | {} | {} | {introduced} | {} |",
                candidate.id,
                candidate.path(),
                candidate.span.line,
                candidate.owner.replace('|', "\\|"),
                contexts.join("/"),
                detail(candidate).replace('|', "\\|"),
                record.attribution.name(),
                evidence_text(&record.attribution).replace('|', "\\|")
            )
            .assured("writing to a String cannot fail");
        }
    }
    text
}
