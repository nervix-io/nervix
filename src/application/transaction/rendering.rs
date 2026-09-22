//! The TEXT and JSON renderings of one transaction inspection.
//!
//! Layer: edges.
//!
//! - **Owns.** The lines a client displays for an inspected transaction and the JSON document
//!   `FORMAT JSON` prints, both rendered from the one typed inspection so that they carry the same
//!   meaning through every session transport.
//! - **Depends on.** The inspection envelope and the impact report vocabulary.
//! - **Must not know.** How the report was read, planned or transported, or which session asked.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ActualExecutionStepImpact, ConcreteBranchCoverage, ExecutionStepImpactReport,
    ExecutionStepOutcome, ImpactAttribution, ImpactDiagnostic, ImpactEffects, ImpactNodeCoverage,
    ImpactTopology, NodeRef, OperationImpactReason, OperationImpactReport, PauseRequirement,
    QuiescenceOutcome, TransactionInspection, TransactionLifecycle, TransactionOperation,
    TransactionOperationRange, TransactionReportFormat,
};

/// Whether an operation's own contribution is rendered beside its identity and reasons.
///
/// Execution steps already render the effects every step applies, so a whole-transaction view
/// shows each contribution once, through its step. The operation a reader selected leads with its
/// own contribution, because that is what the reader asked about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Contribution {
    Shown,
    Omitted,
}

/// One inspection, rendered in the format its statement asked for.
pub(super) struct InspectionRendering<'a> {
    inspection: &'a TransactionInspection,
}

impl<'a> InspectionRendering<'a> {
    pub(super) const fn new(inspection: &'a TransactionInspection) -> Self {
        Self { inspection }
    }

    pub(super) fn render(&self, format: TransactionReportFormat) -> String {
        match format {
            TransactionReportFormat::Text => self.text(),
            TransactionReportFormat::Json => self.json(),
        }
    }

    /// The inspection's own JSON representation: the status with its lifecycle state inline, the
    /// selected operation or `null`, and the whole report.
    fn json(&self) -> String {
        serde_json::to_string_pretty(self.inspection).assured(
            "an inspection holds only strings, numbers, sequences and tagged enums, all of which \
             have a JSON representation",
        )
    }

    /// The lines a reader scans: who and where the transaction is, whether its report is complete,
    /// what it requires, and what each operation and execution step contributes.
    ///
    /// Selecting an operation reorders the report rather than narrowing it: the operation and the
    /// execution step containing it come first, followed by the rest of the transaction.
    fn text(&self) -> String {
        let report = &self.inspection.report;
        let mut lines = self.status_lines();
        lines.extend(self.completeness_lines());
        match self.inspection.operation {
            None => {
                lines.extend(self.summary_lines());
                for operation in report.operations() {
                    lines.extend(operation_lines(operation, Contribution::Omitted));
                }
                for step in report.execution_steps() {
                    lines.extend(step_lines(step));
                }
            }
            Some(selected) => {
                let operation = report.operations().get(selected.index()).verified(
                    "the inspection service rejects an operation past the operations the report \
                     numbers, and the report numbers its operations by position",
                );
                let step = self.step_containing(operation);
                lines.push(format!("inspected operation: {selected}"));
                lines.extend(operation_lines(operation, Contribution::Shown));
                lines.extend(step_lines(step));
                lines.extend(self.summary_lines());
                for other in report.operations() {
                    if other.number != selected {
                        lines.extend(operation_lines(other, Contribution::Omitted));
                    }
                }
                for other in report.execution_steps() {
                    if other.operations() != step.operations() {
                        lines.extend(step_lines(other));
                    }
                }
            }
        }
        lines.join("\n")
    }

    /// The execution step `operation` belongs to.
    ///
    /// Steps partition the operations in order, so the step is found by its first operation
    /// rather than by scanning every step.
    fn step_containing(&self, operation: &OperationImpactReport) -> &'a ExecutionStepImpactReport {
        let steps = self.inspection.report.execution_steps();
        let first = operation.execution_step.first();
        let index = steps
            .binary_search_by_key(&first, |step| step.operations().first())
            .assured(
                "a report is validated so that every operation names the consecutive step that \
                 contains it, and the steps are ordered by their first operation",
            );
        &steps[index]
    }

    fn status_lines(&self) -> Vec<String> {
        let status = &self.inspection.transaction;
        let mut lines = vec![
            format!("transaction: {}", status.transaction_id()),
            format!("domain: {}", status.domain().as_str()),
            format!("state: {}", status.lifecycle().as_ref()),
        ];
        if let TransactionLifecycle::Failed {
            failing_operation,
            error,
        } = status.lifecycle()
        {
            lines.push(format!("failing operation: {failing_operation}"));
            lines.push(format!("error: {error}"));
        }
        lines.push(format!(
            "operations: {} accepted, {} applied, {} pending",
            status.accepted_operations().accepted_operations(),
            status.applied_operations(),
            status.pending_operations()
        ));
        lines
    }

    fn completeness_lines(&self) -> Vec<String> {
        let report = &self.inspection.report;
        let mut lines = vec![format!("report: {}", report.completeness().as_ref())];
        for diagnostic in report.completeness().diagnostics() {
            lines.push(format!("- {}", diagnostic_text(diagnostic)));
        }
        lines.push(format!("planning basis: {}", report.planning_basis()));
        lines
    }

    /// The pause the whole transaction requires: the maximum over its execution steps.
    fn summary_lines(&self) -> Vec<String> {
        let pause = self.inspection.report.summary().pause();
        let mut lines = vec![format!("quiesce level: {}", pause.level().as_str())];
        lines.push(format!("pause: {}", pause_text(pause)));
        lines.extend(pause_scope_lines(pause, "  "));
        lines
    }
}

fn operation_lines(operation: &OperationImpactReport, contribution: Contribution) -> Vec<String> {
    let mut lines = vec![
        format!(
            "operation {}: {}",
            operation.number,
            transaction_operation_text(&operation.operation)
        ),
        format!(
            "  execution step: {}",
            operation_range_text(operation.execution_step)
        ),
    ];
    for diagnostic in operation.completeness.diagnostics() {
        lines.push(format!("  incomplete: {}", diagnostic_text(diagnostic)));
    }
    for reason in &operation.reasons {
        lines.push(format!("  reason: {}", reason_text(reason)));
    }
    if contribution == Contribution::Shown {
        for effect in effect_lines(&operation.contribution) {
            lines.push(format!("  contribution: {effect}"));
        }
    }
    lines
}

/// One execution step: what it requires, what it changes, and how far applying it has come.
fn step_lines(step: &ExecutionStepImpactReport) -> Vec<String> {
    let planned = step.planned();
    let actual = step.actual();
    let mut lines = vec![
        format!(
            "execution step {}: planned {}, actual {}, outcome {}",
            operation_range_text(step.operations()),
            planned.pause.level().as_str(),
            step.actual_quiesce_level().as_str(),
            actual.outcome.as_ref()
        ),
        format!("  pause: {}", pause_text(&planned.pause)),
    ];
    lines.extend(pause_scope_lines(&planned.pause, "    "));
    for diagnostic in planned.completeness.diagnostics() {
        lines.push(format!("  incomplete: {}", diagnostic_text(diagnostic)));
    }
    for effect in effect_lines(&planned.effects) {
        lines.push(format!("  effect: {effect}"));
    }
    lines.extend(actual_lines(actual));
    lines
}

/// What applying a step actually engaged, and why it failed when it did.
fn actual_lines(actual: &ActualExecutionStepImpact) -> Vec<String> {
    let mut lines = Vec::new();
    for engagement in &actual.quiescence {
        let outcomes = engagement
            .outcomes
            .iter()
            .map(quiescence_outcome_text)
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!(
            "  quiescence: {} {outcomes}",
            pause_text(&engagement.requirement)
        ));
    }
    if let ExecutionStepOutcome::Failed { diagnostic } = &actual.outcome {
        lines.push(format!("  failure: {}", diagnostic_text(diagnostic)));
    }
    lines
}

fn transaction_operation_text(operation: &TransactionOperation) -> String {
    let action = operation.as_ref();
    match operation {
        TransactionOperation::CreateConfiguration { node, .. }
        | TransactionOperation::AlterConfiguration { node, .. }
        | TransactionOperation::DropConfiguration { node, .. } => {
            format!("{action} {}", node_text(node))
        }
        TransactionOperation::AlterDomain { domain }
        | TransactionOperation::StartDomain { domain }
        | TransactionOperation::StopDomain { domain } => {
            format!("{action} domain={}", domain.as_str())
        }
        TransactionOperation::CreateResource { resource, .. } => {
            format!("{action} resource={}", resource.as_str())
        }
        TransactionOperation::RebindResource {
            resource,
            requested,
            version,
            ..
        } => format!(
            "{action} resource={} version={version} requested={requested}",
            resource.as_str()
        ),
    }
}

fn reason_text(reason: &OperationImpactReason) -> String {
    let kind = reason.as_ref();
    match reason {
        OperationImpactReason::Configuration { node, aspect } => {
            format!("{kind} {} aspect={}", node_text(node), aspect.as_ref())
        }
        OperationImpactReason::DomainPlacement
        | OperationImpactReason::DomainStart
        | OperationImpactReason::DomainStop => kind.to_string(),
        OperationImpactReason::ResourceCatalog { resource } => {
            format!("{kind} resource={}", resource.as_str())
        }
        OperationImpactReason::ResourceRebinding {
            node,
            resource,
            from_version,
            to_version,
        } => format!(
            "{kind} {} resource={} from={from_version} to={to_version}",
            node_text(node),
            resource.as_str()
        ),
    }
}

/// Every effect in `effects`, one line each, with the operations each is attributed to.
fn effect_lines(effects: &ImpactEffects) -> Vec<String> {
    let mut lines = Vec::new();
    for change in &effects.changed_configuration {
        lines.push(format!(
            "configuration {} {} {}",
            change.transition.as_ref(),
            node_text(change.transition.node()),
            attribution_text(&change.attribution)
        ));
    }
    if !effects.topology.before.is_empty() || !effects.topology.after.is_empty() {
        lines.push(format!(
            "topology before {} after {}",
            topology_text(&effects.topology.before),
            topology_text(&effects.topology.after)
        ));
    }
    for moved in &effects.ownership_moves {
        lines.push(format!(
            "ownership move {} from={} to={} {}",
            coverage_text(&moved.node),
            moved.source,
            moved.destination,
            attribution_text(&moved.attribution)
        ));
    }
    for lifecycle in &effects.lifecycle {
        lines.push(format!(
            "lifecycle {} domain={} {}",
            lifecycle.action.as_ref(),
            lifecycle.domain.as_str(),
            attribution_text(&lifecycle.attribution)
        ));
    }
    for activation in &effects.activations {
        lines.push(format!(
            "activation {} {} {}",
            activation.action.as_ref(),
            coverage_text(&activation.node),
            attribution_text(&activation.attribution)
        ));
    }
    for rebuild in &effects.rebuilds {
        lines.push(format!(
            "rebuild {} {} {}",
            rebuild.reason.as_ref(),
            coverage_text(&rebuild.node),
            attribution_text(&rebuild.attribution)
        ));
    }
    for reset in &effects.state_resets {
        lines.push(format!(
            "state reset {} {} {}",
            reset.state.as_ref(),
            coverage_text(&reset.node),
            attribution_text(&reset.attribution)
        ));
    }
    for flush in &effects.force_flushes {
        lines.push(format!(
            "force flush {} {}",
            coverage_text(&flush.node),
            attribution_text(&flush.attribution)
        ));
    }
    for catalog in &effects.resource_catalog {
        lines.push(format!(
            "resource {} resource={} {}",
            catalog.action.as_ref(),
            catalog.resource.as_str(),
            attribution_text(&catalog.attribution)
        ));
    }
    for binding in &effects.resource_bindings {
        lines.push(format!(
            "resource binding {} resource={} version={} requested={} {}",
            node_text(&binding.node),
            binding.resource.as_str(),
            binding.version,
            binding.requested,
            attribution_text(&binding.attribution)
        ));
    }
    lines
}

/// The requirement itself, without the members of a paused subgraph.
fn pause_text(pause: &PauseRequirement) -> String {
    let kind = pause.as_ref();
    match pause {
        PauseRequirement::NoPause => kind.to_string(),
        PauseRequirement::Subgraph { scope } => format!(
            "{kind} nodes={} gates={}",
            scope.nodes().len(),
            scope.gate_boundaries().len()
        ),
        PauseRequirement::Domain { domain } => format!("{kind} domain={}", domain.as_str()),
    }
}

/// The nodes and admission gates of a paused subgraph, one line each, indented by `indent`.
fn pause_scope_lines(pause: &PauseRequirement, indent: &str) -> Vec<String> {
    let PauseRequirement::Subgraph { scope } = pause else {
        return Vec::new();
    };
    let mut lines = Vec::new();
    for node in scope.nodes() {
        lines.push(format!(
            "{indent}node {} {}",
            coverage_text(&node.coverage),
            attribution_text(&node.attribution)
        ));
    }
    for gate in scope.gate_boundaries() {
        lines.push(format!(
            "{indent}gate relay={} branches={} {}",
            gate.boundary.relay.as_str(),
            branches_text(&gate.boundary.branches),
            attribution_text(&gate.attribution)
        ));
    }
    lines
}

fn quiescence_outcome_text(outcome: &QuiescenceOutcome) -> String {
    let kind = outcome.as_ref();
    match outcome {
        QuiescenceOutcome::Requested
        | QuiescenceOutcome::Confirmed
        | QuiescenceOutcome::Released => kind.to_string(),
        QuiescenceOutcome::Failed { diagnostic } | QuiescenceOutcome::Uncertain { diagnostic } => {
            format!("{kind} ({})", diagnostic.message)
        }
    }
}

fn diagnostic_text(diagnostic: &ImpactDiagnostic) -> String {
    match diagnostic.operation {
        Some(operation) => format!(
            "kind={} operation={operation} {}",
            diagnostic.kind.as_ref(),
            diagnostic.message
        ),
        None => format!("kind={} {}", diagnostic.kind.as_ref(), diagnostic.message),
    }
}

fn node_text(node: &NodeRef) -> String {
    format!(
        "kind={} name={}",
        node.kind.as_str(),
        node.identifier.as_str()
    )
}

/// A node with the concrete executions an effect covers. A configuration-only node has no
/// executions, so it names no branches.
fn coverage_text(coverage: &ImpactNodeCoverage) -> String {
    match &coverage.branches {
        Some(branches) => format!(
            "{} branches={}",
            node_text(&coverage.node),
            branches_text(branches)
        ),
        None => node_text(&coverage.node),
    }
}

/// Which concrete executions are covered. Selected branch keys are fingerprints of possibly
/// sensitive values, so text reports how many there are rather than what they are.
fn branches_text(branches: &ConcreteBranchCoverage) -> String {
    let kind = branches.as_ref();
    match branches {
        ConcreteBranchCoverage::All | ConcreteBranchCoverage::Unbranched => kind.to_string(),
        ConcreteBranchCoverage::AllOfBranch { branch } => {
            format!("{kind} branch={}", branch.as_str())
        }
        ConcreteBranchCoverage::Selected { branch, keys } => {
            format!("{kind} branch={} keys={}", branch.as_str(), keys.len())
        }
    }
}

fn attribution_text(attribution: &ImpactAttribution) -> String {
    let operations = attribution
        .operations()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!("operations={operations}")
}

fn topology_text(topology: &ImpactTopology) -> String {
    format!(
        "nodes={} edges={}",
        topology.nodes.len(),
        topology.edges.len()
    )
}

/// An execution step's operations: one number for a single operation, otherwise `first-last`.
fn operation_range_text(range: TransactionOperationRange) -> String {
    if range.first() == range.last() {
        return range.first().to_string();
    }
    format!("{}-{}", range.first(), range.last())
}

#[cfg(test)]
#[path = "rendering_tests.rs"]
mod tests;
