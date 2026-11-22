//! How each domain's committed schedule stands against its node rules, for inspection and metrics.
//!
//! Layer: control plane.
//!
//! - **Owns.** Capturing the live nodes' committed records and the placement candidates once per
//!   domain, judging the committed schedule with the registry, and the per-unit annotations
//!   `SHOW CLUSTER STATUS` and the placement inspections print.
//! - **Depends on.** The schedule planning snapshot, the consensus observer and the registry's
//!   node rule judgement.
//! - **Must not know.** How the scheduler ranks candidates or how a refusal is decided.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use std::collections::BTreeMap;

use error_stack::Report;
use nervix_models::{CreatePlacement, DescribePlacement, DomainName, NodeUnitRef, PlacementName};

use super::{
    command_result::CommandResult,
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
};
use crate::registry::{NodeRuleJudgement, PlacementPlan, RegistryError, UnitRuleState};

/// The units of every domain that break a node rule, as `SHOW CLUSTER STATUS` annotates them.
#[derive(Debug, Default)]
pub(in crate::application) struct NodeRuleAnnotations {
    domains: BTreeMap<DomainName, BTreeMap<NodeUnitRef, UnitRuleState>>,
}

impl NodeRuleAnnotations {
    /// The fields the schedule line of `unit` in `domain` gains: ` violates=<rules>` for a live
    /// owner a rule does not admit, ` waiting_for=<rules>` for a unit no candidate satisfies.
    pub(in crate::application) fn fields(&self, domain: &DomainName, unit: &NodeUnitRef) -> String {
        let Some(state) = self.domains.get(domain).and_then(|units| units.get(unit)) else {
            return String::new();
        };
        match state {
            UnitRuleState::Violates(rules) => format!(" violates={}", rule_list(rules)),
            UnitRuleState::WaitingFor(rules) => format!(" waiting_for={}", rule_list(rules)),
            UnitRuleState::Satisfied | UnitRuleState::Unplaced => String::new(),
        }
    }
}

/// Rule names joined for one status field.
pub(in crate::application) fn rule_list(rules: &[PlacementName]) -> String {
    rules
        .iter()
        .map(PlacementName::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

impl SessionServiceImpl {
    /// `domain`'s committed schedule judged against its node rules, by the committed records of
    /// the live nodes this node observes. `None` for a domain with no schedule, no graph or no
    /// node rule. It reads no cluster parameter, so a node that still initializes reports it.
    pub(in crate::application) async fn node_rule_judgement(
        &self,
        domain: &DomainName,
    ) -> Option<Result<NodeRuleJudgement, Report<RegistryError>>> {
        let graph = self.inner.registry.active_graph(domain)?;
        if !graph.has_node_rules() {
            return None;
        }
        let inputs = self.inner.consensus.domain_planning_inputs(domain).await;
        let schedule = inputs.schedule()?.clone();
        let (records, candidates) = self.capture_node_rule_inputs(&inputs).await;
        let activity = match inputs.state() {
            Some(state) => crate::registry::ScheduleActivity::of(&state.status),
            None => crate::registry::ScheduleActivity::Stopped,
        };
        Some(graph.judge_node_rules(domain, &schedule, &records, &candidates, activity))
    }

    /// Describes the node rule `describe` names: its form and labels, then what it declares,
    /// matches and covers, and how each unit of its members stands against it.
    pub(in crate::application) async fn describe_node_rule(
        &self,
        domain: &DomainName,
        describe: &DescribePlacement,
    ) -> CommandResult {
        let not_found =
            || command_error(format!("placement '{}' not found", describe.name.as_str()));
        let judgement = match self.node_rule_judgement(domain).await {
            Some(Ok(judgement)) => judgement,
            Some(Err(error)) => return command_error(format!("{error:#}")),
            None => return not_found(),
        };
        let Ok(index) = judgement
            .plans()
            .binary_search_by(|rule| rule.name.as_str().cmp(describe.name.as_str()))
        else {
            return not_found();
        };
        let rule = &judgement.plans()[index];
        let mut lines = vec![format!("placement: {}", rule.name.as_str())];
        match self
            .inner
            .registry
            .get::<CreatePlacement>(domain, &describe.name)
        {
            Ok(Some(placement)) => {
                match placement.to_canonical_plexus() {
                    Ok(form) => lines.push(format!("form: {form}")),
                    Err(error) => return command_error(format!("{error:#}")),
                }
                super::describe_output::append_entity_label_lines(&mut lines, &placement.labels);
            }
            Ok(None) => {}
            Err(error) => return command_error(format!("{error:#}")),
        }
        let schedule = self.inner.consensus.current_schedule().await;
        lines.extend(describe_node_rule_lines(
            &judgement,
            rule,
            schedule.domain(domain),
        ));
        command_ok(lines.join("\n"))
    }

    /// The `DESCRIBE DOMAIN` lines of `domain`'s node rules and of the units breaking them; none
    /// for a domain without node rules.
    pub(in crate::application) async fn describe_domain_node_rules(
        &self,
        domain: &DomainName,
    ) -> Vec<String> {
        match self.node_rule_judgement(domain).await {
            Some(Ok(judgement)) if !judgement.plans().is_empty() => {
                describe_domain_node_rule_lines(&judgement)
            }
            Some(Ok(_)) | None => Vec::new(),
            Some(Err(error)) => vec![format!("  node rules: {error:#}")],
        }
    }

    /// The `SHOW PLACEMENTS` lines of the node rules in `domain`'s placement `plan`; none for a
    /// plan without node rules or a domain with no schedule or graph.
    pub(in crate::application) async fn show_domain_node_rules(
        &self,
        domain: &DomainName,
        plan: &PlacementPlan,
    ) -> Result<Vec<String>, Report<RegistryError>> {
        if plan.node_rules.is_empty() {
            return Ok(Vec::new());
        }
        let Some(judgement) = self.node_rule_judgement(domain).await else {
            return Ok(Vec::new());
        };
        Ok(show_node_rule_lines(&judgement?))
    }

    /// The breaking units of every scheduled domain. A domain whose rules cannot be judged is left
    /// without annotations, and the failure is logged.
    pub(in crate::application) async fn node_rule_annotations(&self) -> NodeRuleAnnotations {
        let schedule = self.inner.consensus.current_schedule().await;
        let mut annotations = NodeRuleAnnotations::default();
        for domain in schedule.domains.keys() {
            nervix_primitives::task::consume_budget().await;
            let Some(judged) = self.node_rule_judgement(domain).await else {
                continue;
            };
            let judgement = match judged {
                Ok(judgement) => judgement,
                Err(error) => {
                    tracing::warn!(
                        domain = domain.as_str(),
                        error = %format_args!("{error:#}"),
                        "could not judge the domain's schedule against its node rules"
                    );
                    continue;
                }
            };
            let mut units = BTreeMap::new();
            for report in judgement.units {
                units.insert(report.unit, report.state);
            }
            annotations.domains.insert(domain.clone(), units);
        }
        annotations
    }
}

/// A unit as placement inspection names it: its kind and name, then its partition or instance.
pub(in crate::application) fn unit_label(unit: &NodeUnitRef) -> String {
    format!(
        "{} {}{}",
        unit.node.kind.as_str(),
        unit.node.identifier.as_str(),
        unit.unit.label_suffix()
    )
}

/// The coverage a node rule writes in its scope field.
pub(in crate::application) fn node_rule_scope(
    rule: &crate::registry::NodeRulePlan,
) -> &'static str {
    match rule.scope {
        nervix_models::NodeRuleScope::Members(_) => "members",
        nervix_models::NodeRuleScope::DomainWide => "domain-wide",
    }
}

fn node_rule_rank(rule: &crate::registry::NodeRulePlan) -> String {
    match rule.rank {
        Some(rank) => rank.to_string(),
        None => "unranked".to_owned(),
    }
}

fn node_rule_coverage(
    judgement: &NodeRuleJudgement,
    rule: &crate::registry::NodeRulePlan,
) -> &'static str {
    rule.coverage(judgement.matching_nodes(rule).as_ref())
        .into()
}

/// The canonical text of a rule's selector, absent for `ANY NODE`.
fn selector_text(rule: &crate::registry::NodeRulePlan) -> Option<String> {
    let selector = rule.policy.selector()?;
    Some(match nervix_models::expression_to_plexus(selector) {
        Ok(text) => text,
        Err(error) => format!("(cannot render the selector: {error:#})"),
    })
}

/// `SHOW PLACEMENTS` lines for the node rules: one summary per rule, then one line per member
/// naming the policy that wins there or the rules that override it.
fn show_node_rule_lines(judgement: &NodeRuleJudgement) -> Vec<String> {
    let mut lines = Vec::new();
    for rule in judgement.plans() {
        lines.push(format!(
            "{} scope={} policy={} rank={} coverage={}",
            rule.name.as_str(),
            node_rule_scope(rule),
            rule.policy.label(),
            node_rule_rank(rule),
            node_rule_coverage(judgement, rule),
        ));
    }
    for rule in judgement.plans() {
        for member in &rule.members {
            if member.overridden_by.is_empty() {
                lines.push(format!(
                    "{} member={} effective={}",
                    rule.name.as_str(),
                    member.node.identifier.as_str(),
                    rule.policy.label()
                ));
            } else {
                lines.push(format!(
                    "{} member={} overridden_by={}",
                    rule.name.as_str(),
                    member.node.identifier.as_str(),
                    rule_list(&member.overridden_by)
                ));
            }
        }
    }
    lines
}

/// `DESCRIBE PLACEMENT` lines for one node rule after its form and labels: what it declares,
/// the nodes it matches now, where it wins, and how every unit of its members stands.
fn describe_node_rule_lines(
    judgement: &NodeRuleJudgement,
    rule: &crate::registry::NodeRulePlan,
    schedule: Option<&nervix_models::DomainSchedule>,
) -> Vec<String> {
    let mut lines = vec![
        format!("scope: {}", node_rule_scope(rule)),
        format!("policy: {}", rule.policy.label()),
    ];
    if let Some(selector) = selector_text(rule) {
        lines.push(format!("selector: {selector}"));
    }
    lines.push(format!("rank: {}", node_rule_rank(rule)));
    let members = rule
        .members
        .iter()
        .map(|member| member.node.identifier.as_str())
        .collect::<Vec<_>>();
    lines.push(format!(
        "members: {}",
        if members.is_empty() {
            "(none)".to_owned()
        } else {
            members.join(", ")
        }
    ));
    match judgement.matching_nodes(rule) {
        Some(nodes) if nodes.is_empty() => lines.push("matching nodes: (none)".to_owned()),
        Some(nodes) => lines.push(format!(
            "matching nodes: {}",
            nodes
                .iter()
                .map(|node| node.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )),
        None => lines.push("matching nodes: every node".to_owned()),
    }
    lines.push(format!("coverage: {}", node_rule_coverage(judgement, rule)));
    for member in &rule.members {
        if member.overridden_by.is_empty() {
            lines.push(format!(
                "member: {} effective",
                member.node.identifier.as_str()
            ));
        } else {
            lines.push(format!(
                "member: {} overridden by {}",
                member.node.identifier.as_str(),
                rule_list(&member.overridden_by)
            ));
        }
        let Some(scheduled) = schedule.and_then(|schedule| schedule.nodes.get(&member.node)) else {
            continue;
        };
        for (unit, assignment) in &scheduled.units {
            let unit = NodeUnitRef::new(member.node.clone(), *unit);
            let owner = assignment.execution_node();
            let satisfaction = unit_satisfaction(judgement, rule, member, &unit, owner);
            lines.push(format!(
                "unit: {} owner={} satisfaction={satisfaction}",
                unit_label(&unit),
                match owner {
                    Some(owner) => owner.as_str(),
                    None => "-",
                }
            ));
        }
    }
    for failure in judgement.failures() {
        if failure.rule != rule.name {
            continue;
        }
        for error in &failure.failure.errors {
            lines.push(format!(
                "selector failed on '{}': {}",
                failure.failure.node, error.reason
            ));
        }
    }
    lines
}

/// How one unit of a rule's member stands against that rule.
fn unit_satisfaction(
    judgement: &NodeRuleJudgement,
    rule: &crate::registry::NodeRulePlan,
    member: &crate::registry::NodeRuleMemberPlan,
    unit: &NodeUnitRef,
    owner: Option<&nervix_models::ClusterNodeName>,
) -> &'static str {
    if !member.overridden_by.is_empty() {
        return "overridden";
    }
    match &rule.policy {
        nervix_models::NodePolicy::Require(_) => match judgement.state_of(unit) {
            Some(UnitRuleState::Satisfied) => "satisfied",
            Some(UnitRuleState::Unplaced) => "unplaced",
            // The winning requirements of one runtime node: the rules at its strongest rank.
            Some(UnitRuleState::Violates(rules)) if rules.contains(&rule.name) => "violates",
            Some(UnitRuleState::Violates(_)) => "satisfied",
            Some(UnitRuleState::WaitingFor(_)) => "waiting",
            None => "satisfied",
        },
        nervix_models::NodePolicy::Prefer(_) => {
            let Some(owner) = owner else {
                return "unplaced";
            };
            match judgement.rule_admits(rule, owner) {
                Some(true) => "preferred",
                Some(false) | None => "not preferred",
            }
        }
        nervix_models::NodePolicy::Any => match owner {
            Some(_) => "satisfied",
            None => "unplaced",
        },
    }
}

/// `DESCRIBE DOMAIN` lines for the node rules: one per rule, then the units breaking one.
fn describe_domain_node_rule_lines(judgement: &NodeRuleJudgement) -> Vec<String> {
    let mut lines = Vec::new();
    for rule in judgement.plans() {
        lines.push(format!(
            "  {} scope={} policy={}",
            rule.name.as_str(),
            node_rule_scope(rule),
            rule.policy.label()
        ));
    }
    let mut violating = Vec::new();
    let mut waiting = Vec::new();
    for report in &judgement.units {
        match &report.state {
            UnitRuleState::Violates(rules) => violating.push(format!(
                "{} on {} ({})",
                unit_label(&report.unit),
                match &report.owner {
                    Some(owner) => owner.as_str(),
                    None => "-",
                },
                rule_list(rules)
            )),
            UnitRuleState::WaitingFor(rules) => waiting.push(format!(
                "{} ({})",
                unit_label(&report.unit),
                rule_list(rules)
            )),
            UnitRuleState::Satisfied | UnitRuleState::Unplaced => {}
        }
    }
    let list = |entries: Vec<String>| {
        if entries.is_empty() {
            "none".to_owned()
        } else {
            entries.join(", ")
        }
    };
    lines.push(format!("  units violating node rules: {}", list(violating)));
    lines.push(format!("  units waiting for node rules: {}", list(waiting)));
    lines
}

/// What the previous node rule observation saw, so a unit entering or leaving a breach is logged
/// once and a domain's gauges are removed once it no longer has node rules.
#[derive(Debug, Default)]
pub(in crate::application) struct NodeRuleObservation {
    breaking: BTreeMap<DomainName, BTreeMap<NodeUnitRef, UnitRuleState>>,
    exported: std::collections::BTreeSet<DomainName>,
}

impl SessionServiceImpl {
    /// Exports the placement gauges of every domain with node rules, from this node's applied
    /// state and observed liveness, and, on the leader, logs each unit that begins or stops
    /// breaking a node rule.
    pub(in crate::application) async fn observe_node_rule_units(
        &self,
        observation: &mut NodeRuleObservation,
    ) {
        let schedule = self.inner.consensus.current_schedule().await;
        let leads = self.inner.consensus.current_leader().await.as_ref()
            == Some(self.inner.consensus.local_node_id());
        let metrics = self.inner.runtime.metrics();
        let mut exported = std::collections::BTreeSet::new();
        for domain in schedule.domains.keys() {
            nervix_primitives::task::consume_budget().await;
            let Some(Ok(judgement)) = self.node_rule_judgement(domain).await else {
                continue;
            };
            if judgement.plans().is_empty() {
                continue;
            }
            metrics.set_placement_node_rule_units(domain, judgement.count());
            exported.insert(domain.clone());
            let mut breaking = BTreeMap::new();
            for report in judgement.units {
                if let UnitRuleState::Violates(_) | UnitRuleState::WaitingFor(_) = report.state {
                    breaking.insert(report.unit, report.state);
                }
            }
            let previous = observation.breaking.remove(domain).unwrap_or_default();
            if leads {
                log_node_rule_transitions(domain, &previous, &breaking);
            }
            observation.breaking.insert(domain.clone(), breaking);
        }
        for domain in observation.exported.difference(&exported) {
            metrics.remove_placement_node_rule_units(domain);
        }
        observation
            .breaking
            .retain(|domain, _| exported.contains(domain));
        observation.exported = exported;
    }
}

/// Logs, at `info`, each unit whose breach of a node rule began, changed or ended.
fn log_node_rule_transitions(
    domain: &DomainName,
    previous: &BTreeMap<NodeUnitRef, UnitRuleState>,
    current: &BTreeMap<NodeUnitRef, UnitRuleState>,
) {
    for (unit, state) in current {
        if previous.get(unit) == Some(state) {
            continue;
        }
        match state {
            UnitRuleState::Violates(rules) => tracing::info!(
                domain = domain.as_str(),
                unit = unit_label(unit),
                rules = rule_list(rules),
                "unit runs on a node its node rules do not admit"
            ),
            UnitRuleState::WaitingFor(rules) => tracing::info!(
                domain = domain.as_str(),
                unit = unit_label(unit),
                rules = rule_list(rules),
                "unit waits for a node its node rules admit"
            ),
            UnitRuleState::Satisfied | UnitRuleState::Unplaced => {}
        }
    }
    for unit in previous.keys() {
        if !current.contains_key(unit) {
            tracing::info!(
                domain = domain.as_str(),
                unit = unit_label(unit),
                "unit no longer breaks a node rule"
            );
        }
    }
}
