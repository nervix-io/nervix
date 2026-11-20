//! How a stop's answer, and a stopping or stopped domain's description, present the stop.
//!
//! Layer: control plane.
//! - **Owns.** The text that says how a stop ended, which request enforced it, what its teardown
//!   cut per node and entity, and what a stopping domain waits for.
//! - **Depends on.** The committed stop records and the owner's waits.
//! - **Must not know.** How the stop drained, or the session protocol's encoding.

use nervix_interconnect::domain_stop::StopWaitEntry;
use nervix_models::{
    DomainName, DomainStopCause, DomainStopEnding, DomainStopReport, DomainStopping,
    NodeStopReport, StopCut, StopCutReason, StopEnding, StopRequestId,
};

use super::{NodeStopWaits, StopWaits};

fn entity_text(kind: nervix_models::ModelKind, name: &nervix_models::ModelName) -> String {
    format!("{} '{}'", kind.as_str(), name.as_str())
}

fn cut_text(cut: &StopCut) -> String {
    format!(
        "{}: {} ({})",
        entity_text(cut.entity_kind, &cut.entity),
        cut.kind.describe(),
        cut.items
    )
}

fn wait_text(entry: &StopWaitEntry) -> String {
    match &entry.entity {
        Some(entity) => format!(
            "{}: {} ({})",
            entity_text(entity.kind, &entity.identifier),
            entry.kind.describe(),
            entry.items
        ),
        None => format!("{} ({})", entry.kind.describe(), entry.items),
    }
}

/// The cuts of `report` that `select` keeps, node by node: `node: cut, cut; node: cut`.
/// The cuts of every reporting node that the teardown made for `reason`, per node.
fn report_text(report: &DomainStopReport, reason: StopCutReason) -> Option<String> {
    let mut nodes = Vec::new();
    for (node, node_report) in &report.nodes {
        let NodeStopReport::Reported { cuts } = node_report else {
            continue;
        };
        let selected = cuts
            .iter()
            .filter(|cut| cut.kind.reason() == reason)
            .map(cut_text)
            .collect::<Vec<_>>();
        if !selected.is_empty() {
            nodes.push(format!("{node}: {}", selected.join(", ")));
        }
    }
    (!nodes.is_empty()).then(|| nodes.join("; "))
}

fn unreported_text(report: &DomainStopReport) -> Option<String> {
    let unreported = report
        .nodes
        .iter()
        .filter(|(_, node_report)| matches!(node_report, NodeStopReport::Unreported))
        .map(|(node, _)| node.to_string())
        .collect::<Vec<_>>();
    (!unreported.is_empty()).then(|| unreported.join(", "))
}

/// What the stop's teardown cut, as one clause per kind of cut.
pub(in crate::application) fn cut_clauses(report: Option<&DomainStopReport>) -> Vec<String> {
    let Some(report) = report else {
        return vec!["what its teardown cut is not reported yet".to_string()];
    };
    let mut clauses = Vec::new();
    if let Some(enforced) = report_text(report, StopCutReason::Enforcement) {
        clauses.push(format!("cut by enforcement: {enforced}"));
    }
    if let Some(held) = report_text(report, StopCutReason::NotSavedYet) {
        clauses.push(format!("cut because it cannot be saved yet: {held}"));
    }
    if let Some(retained) = report_text(report, StopCutReason::SessionEnded) {
        clauses.push(format!("discarded with the ingestor session: {retained}"));
    }
    if let Some(unreported) = unreported_text(report) {
        clauses.push(format!("not reported by {unreported}"));
    }
    clauses
}

fn requester_text(id: &StopRequestId, own: Option<&StopRequestId>) -> String {
    match (id, own) {
        (StopRequestId::Licensing, _) => "the licensing stop".to_string(),
        (id, Some(own)) if id == own => format!("this request '{id}'"),
        (id, _) => format!("request '{id}'"),
    }
}

/// How the stop ended, as the answer of request `own` and the description of the stopped domain
/// put it.
pub(in crate::application) fn ending_text(
    ending: &DomainStopEnding,
    own: Option<&StopRequestId>,
) -> String {
    let how = match &ending.ending {
        StopEnding::Completed => "every message in motion finished".to_string(),
        StopEnding::Enforced { by } => format!(
            "{} enforced the stop and ended its tasks at once",
            requester_text(by, own)
        ),
    };
    let mut clauses = vec![how];
    clauses.extend(cut_clauses(ending.report.as_ref()));
    clauses.join("; ")
}

/// The answer of a request whose stop ended.
pub(in crate::application) fn stopped_answer(
    domain: &DomainName,
    ending: &DomainStopEnding,
    own: &StopRequestId,
) -> String {
    format!(
        "stopped domain '{}': {}",
        domain.as_str(),
        ending_text(ending, Some(own))
    )
}

/// What the stop waits for, in motion, node by node.
pub(in crate::application) fn waits_text(waits: &StopWaits) -> String {
    let mut nodes = Vec::new();
    for (node, node_waits) in &waits.nodes {
        match node_waits {
            NodeStopWaits::Observed(entries) => {
                let moving = entries
                    .iter()
                    .filter(|entry| entry.kind.in_motion())
                    .map(wait_text)
                    .collect::<Vec<_>>();
                if !moving.is_empty() {
                    nodes.push(format!("{node}: {}", moving.join(", ")));
                }
            }
            NodeStopWaits::Unanswered => nodes.push(format!("{node}: no answer")),
        }
    }
    if nodes.is_empty() {
        return "the round that confirms nothing is still moving".to_string();
    }
    nodes.join("; ")
}

/// The held work the stop will cut, node by node, if any.
pub(in crate::application) fn held_text(waits: &StopWaits) -> Option<String> {
    let mut nodes = Vec::new();
    for (node, node_waits) in &waits.nodes {
        let NodeStopWaits::Observed(entries) = node_waits else {
            continue;
        };
        let held = entries
            .iter()
            .filter(|entry| !entry.kind.in_motion())
            .map(wait_text)
            .collect::<Vec<_>>();
        if !held.is_empty() {
            nodes.push(format!("{node}: {}", held.join(", ")));
        }
    }
    (!nodes.is_empty()).then(|| nodes.join("; "))
}

/// The answer of a request whose `CANCEL` withdrew it.
pub(in crate::application) fn cancelled_answer(
    domain: &DomainName,
    after: std::time::Duration,
    waits: Option<&StopWaits>,
) -> String {
    let waiting = match waits {
        Some(waits) => waits_text(waits),
        None => "the work in motion".to_string(),
    };
    format!(
        "the stop of domain '{}' was cancelled after {}: the request was withdrawn while the stop \
         was waiting for {waiting}",
        domain.as_str(),
        humantime::format_duration(after)
    )
}

/// A stopping domain's requests, as its description lists them.
pub(in crate::application) fn requests_text(stopping: &DomainStopping) -> String {
    let mut requests = Vec::new();
    if let Some(reason) = stopping.licensing {
        requests.push(format!("licensing ({reason})"));
    }
    for (reference, request) in &stopping.operators {
        let clause = match request.limit {
            Some(limit) => format!(" {limit}"),
            None => String::new(),
        };
        requests.push(format!("'{}'{clause}", reference.as_str()));
    }
    requests.join(", ")
}

/// Why the domain stops or stopped, as its description names it.
pub(in crate::application) fn cause_text(cause: DomainStopCause) -> Option<String> {
    match cause {
        DomainStopCause::Operator => None,
        DomainStopCause::Licensing(reason) => Some(format!("stopped by licensing: {reason}")),
    }
}
