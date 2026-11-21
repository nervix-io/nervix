//! Which cluster nodes a domain's node rules admit, or prefer, for each runtime node they cover.
//!
//! Layer: decisions.
//!
//! - **Owns.** The requirements and preferences ladders of every runtime node a node rule covers,
//!   the conflicts between them, evaluating each rule's selector over committed node records,
//!   judging a schedule's units against the result, and the rule plans inspection reads.
//! - **Depends on.** The node selector decision, placement Models and the placement members a
//!   corridor rule resolves the same way.
//! - **Must not know.** How liveness or records are captured, the order the scheduler ranks
//!   candidates in, or how a refusal or a report reaches a client.

#![cfg_attr(
    nervix_lint,
    nervix::layer(decisions, reason = "owns the decisions boundary declared above")
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
};

use ahash::HashMap;
use error_stack::{Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_models::{
    ClusterNodeName, CommittedNodeRecord, CreatePlacement, DomainName, DomainSchedule,
    DomainStatus, ModelIndex, ModelKind, NodePolicy, NodeRef, NodeRuleScope, NodeUnitRef,
    PlacementName, PlacementRule,
};
use petgraph::{graph::DiGraph, prelude::NodeIndex};

use crate::registry::{
    error::RegistryError,
    graph::{ActiveNode, EdgeKind},
    node_selector::{NodeSelectionFailure, NodeSelector},
    placement::{is_placement_runtime_model, placement_rank_key, resolve_placement_members},
};

/// One node rule, with its members resolved and its selector compiled.
#[derive(Debug, Clone)]
struct NodeRule {
    name: PlacementName,
    scope: NodeRuleScope,
    policy: NodePolicy,
    rank: Option<NonZeroU64>,
    /// The runtime nodes the rule covers, in their order.
    members: Vec<NodeRef>,
    /// Absent for `ANY NODE`, which admits every node.
    selector: Option<NodeSelector>,
}

/// The rules that win on one runtime node, as indices into the rules in name order.
#[derive(Debug, Clone, Default)]
struct NodeLadders {
    /// The rules at the strongest rank of the requirements ladder: all `REQUIRE`, or all
    /// `ANY NODE`.
    requirement: Vec<usize>,
    /// Whether the winning requirement rules are `ANY NODE`, which lifts every weaker one.
    requirement_lifted: bool,
    /// The `PREFER` rules at the strongest rank of the preferences ladder.
    preference: Vec<usize>,
}

/// A domain's node rules and the ladders they resolve to on every runtime node they cover.
#[derive(Debug, Clone, Default)]
pub(in crate::registry) struct NodeRules {
    /// In name order, so every decision visits rules the same way.
    rules: Vec<NodeRule>,
    ladders: HashMap<NodeRef, NodeLadders>,
}

/// Each node rule's selector, evaluated once over one set of committed node records.
#[derive(Debug, Clone, Default)]
pub(crate) struct NodeRuleMatches {
    /// One entry per rule, in the rules' order: the nodes its selector admits, absent for
    /// `ANY NODE`.
    matches: Vec<Option<BTreeSet<ClusterNodeName>>>,
    /// Every node whose record was evaluated.
    nodes: BTreeSet<ClusterNodeName>,
    /// The nodes on which a rule's selector failed to evaluate, which it therefore does not admit.
    pub(crate) failures: Vec<NodeRuleSelectorFailure>,
}

#[derive(Debug, Clone)]
pub(crate) struct NodeRuleSelectorFailure {
    pub(crate) rule: PlacementName,
    pub(crate) failure: NodeSelectionFailure,
}

/// Whether the units of a judged schedule execute on the nodes it assigns them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScheduleActivity {
    /// The domain runs, is paused or is stopping: an assigned unit executes on its owner.
    Executing,
    /// The domain is stopped: nothing executes until START places every unit.
    Stopped,
}

impl ScheduleActivity {
    pub(crate) fn of(status: &DomainStatus) -> Self {
        match status {
            DomainStatus::Stopped => Self::Stopped,
            DomainStatus::Running | DomainStatus::Paused | DomainStatus::Stopping => {
                Self::Executing
            }
        }
    }
}

/// How a schedule's unit stands against the node rules that require something of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnitRuleState {
    /// Its owner is live and every winning requirement admits it.
    Satisfied,
    /// It has no live owner yet, and a node its rules admit can take it.
    Unplaced,
    /// Its live owner is a node these winning `REQUIRE` rules do not admit, and it runs there, or,
    /// in a stopped domain, a candidate node its rules admit can take it.
    Violates(Vec<PlacementName>),
    /// It runs on no node its rules admit, and no candidate node satisfies these rules.
    WaitingFor(Vec<PlacementName>),
}

/// One unit of a runtime node a winning `REQUIRE` rule constrains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnitRuleReport {
    pub(crate) unit: NodeUnitRef,
    #[cfg_attr(
        nervix_lint,
        nervix::identity(node, reason = "the cluster node the unit executes on")
    )]
    pub(crate) owner: Option<ClusterNodeName>,
    pub(crate) state: UnitRuleState,
}

/// What one node rule declares and where it wins, for inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeRulePlan {
    pub(crate) name: PlacementName,
    pub(crate) scope: NodeRuleScope,
    pub(crate) policy: NodePolicy,
    pub(crate) rank: Option<NonZeroU64>,
    pub(crate) members: Vec<NodeRuleMemberPlan>,
}

/// One runtime node a node rule covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeRuleMemberPlan {
    pub(crate) node: NodeRef,
    /// Empty where the rule wins on this runtime node; otherwise the stronger rules that do.
    pub(crate) overridden_by: Vec<PlacementName>,
}

/// How much of a node rule takes effect, the coverage `SHOW PLACEMENTS` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum NodeRuleCoverage {
    /// The rule covers no runtime node.
    Empty,
    /// The rule wins on every runtime node it covers.
    Effective,
    /// The rule wins on some of the runtime nodes it covers.
    Partial,
    /// Stronger rules win on every runtime node it covers.
    Overridden,
    /// The rule wins somewhere, but no live candidate node satisfies its selector now.
    Unmatched,
}

impl NodeRules {
    /// Resolves `placements`, the domain's node rules, against the graph: each member is pinned
    /// like a corridor endpoint, each selector is compiled against the fixed node schema, and an
    /// `ANY NODE` rule beside a `REQUIRE` rule at a runtime node's winning rank is refused.
    pub(in crate::registry) fn build(
        domain: &DomainName,
        mut placements: Vec<CreatePlacement>,
        models: &ModelIndex,
        indices: &HashMap<NodeRef, NodeIndex>,
        graph: &mut DiGraph<ActiveNode, EdgeKind>,
    ) -> Result<Self, Report<RegistryError>> {
        placements.sort_by(|left, right| left.name.as_str().cmp(right.name.as_str()));
        let mut runtime_nodes = graph
            .node_weights()
            .filter(|node| is_placement_runtime_model(node.config.as_ref()))
            .map(ActiveNode::node_ref)
            .collect::<Vec<_>>();
        runtime_nodes.sort();

        let mut rules = Vec::with_capacity(placements.len());
        for placement in placements {
            placement.validate().map_err(|rejection| {
                RegistryError::invalid_model(domain, placement.name.as_str(), rejection)
            })?;
            let PlacementRule::Nodes { scope, policy } = &placement.rule else {
                continue;
            };
            let members = match scope {
                NodeRuleScope::Members(names) => {
                    let resolved = resolve_placement_members(domain, &placement, names, models)?;
                    let placement_index = indices
                        .get(&NodeRef::new(ModelKind::Placement, placement.name.clone()))
                        .copied()
                        .verified("the graph holds a node for every placement Model");
                    let mut members = Vec::with_capacity(resolved.len());
                    for member in resolved {
                        let pin_index = indices
                            .get(&member.pin)
                            .copied()
                            .verified("resolved placement pins belong to this graph");
                        graph.add_edge(pin_index, placement_index, EdgeKind::RequiredBy);
                        members.push(member.runtime);
                    }
                    members
                }
                NodeRuleScope::DomainWide => runtime_nodes.clone(),
            };
            let selector = match policy.selector() {
                Some(expression) => Some(NodeSelector::compile(expression).change_context(
                    RegistryError::PlacementNodeSelector {
                        domain: domain.clone(),
                        placement: placement.name.clone(),
                    },
                )?),
                None => None,
            };
            rules.push(NodeRule {
                name: placement.name.clone(),
                scope: scope.clone(),
                policy: policy.clone(),
                rank: placement.rank,
                members,
                selector,
            });
        }

        let mut requirement_claims = BTreeMap::<NodeRef, Vec<usize>>::new();
        let mut preference_claims = BTreeMap::<NodeRef, Vec<usize>>::new();
        for (index, rule) in rules.iter().enumerate() {
            let claims = match rule.policy {
                NodePolicy::Require(_) | NodePolicy::Any => &mut requirement_claims,
                NodePolicy::Prefer(_) => &mut preference_claims,
            };
            for member in &rule.members {
                claims.entry(member.clone()).or_default().push(index);
            }
        }

        let mut ladders = HashMap::<NodeRef, NodeLadders>::default();
        for (node, claims) in requirement_claims {
            let winners = strongest_claims(&rules, &claims);
            let lifting = winners
                .iter()
                .find(|index| matches!(rules[**index].policy, NodePolicy::Any));
            let requiring = winners
                .iter()
                .find(|index| matches!(rules[**index].policy, NodePolicy::Require(_)));
            if let (Some(lifting), Some(requiring)) = (lifting, requiring) {
                let mut names = [rules[*lifting].name.clone(), rules[*requiring].name.clone()];
                names.sort_by(|left, right| left.as_str().cmp(right.as_str()));
                let [first_rule, second_rule] = names;
                return Err(Report::new(RegistryError::PlacementNodeRuleConflict {
                    domain: domain.clone(),
                    first_rule,
                    second_rule,
                    kind: node.kind.as_str(),
                    identifier: node.identifier.clone(),
                }));
            }
            let ladder = ladders.entry(node).or_default();
            ladder.requirement_lifted = lifting.is_some();
            ladder.requirement = winners;
        }
        for (node, claims) in preference_claims {
            ladders.entry(node).or_default().preference = strongest_claims(&rules, &claims);
        }
        Ok(Self { rules, ladders })
    }

    pub(in crate::registry) fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Evaluates every rule's selector once over `records`. A node on which a selector fails to
    /// evaluate is one that selector does not admit, and the failure is kept for inspection.
    pub(in crate::registry) fn evaluate(
        &self,
        domain: &DomainName,
        records: &BTreeMap<ClusterNodeName, CommittedNodeRecord>,
    ) -> Result<NodeRuleMatches, Report<RegistryError>> {
        let mut matches = Vec::with_capacity(self.rules.len());
        let mut failures = Vec::new();
        for rule in &self.rules {
            let Some(selector) = &rule.selector else {
                matches.push(None);
                continue;
            };
            let selection = selector.evaluate(records).change_context(
                RegistryError::PlacementNodeSelectorEvaluation {
                    domain: domain.clone(),
                    placement: rule.name.clone(),
                },
            )?;
            for failure in selection.failures {
                failures.push(NodeRuleSelectorFailure {
                    rule: rule.name.clone(),
                    failure,
                });
            }
            matches.push(Some(selection.matches));
        }
        Ok(NodeRuleMatches {
            matches,
            nodes: records.keys().cloned().collect(),
            failures,
        })
    }

    /// The evaluated nodes every winning requirement on `node` admits, or `None` when no
    /// requirement constrains it: no rule requires anything of it, or `ANY NODE` wins.
    pub(in crate::registry) fn admitted(
        &self,
        matches: &NodeRuleMatches,
        node: &NodeRef,
    ) -> Option<BTreeSet<ClusterNodeName>> {
        let ladder = self.ladders.get(node)?;
        if ladder.requirement.is_empty() || ladder.requirement_lifted {
            return None;
        }
        let mut admitted = matches.nodes.clone();
        for index in &ladder.requirement {
            let rule_matches = matches.matches[*index]
                .as_ref()
                .verified("a winning requirement that is not ANY NODE is a REQUIRE rule");
            admitted.retain(|candidate| rule_matches.contains(candidate));
        }
        Some(admitted)
    }

    /// The nodes every winning requirement of every runtime node in `nodes` admits, or `None`
    /// when no requirement constrains any of them, so a hard colocation group lands only where
    /// each of its members may run.
    pub(in crate::registry) fn admitted_by_all<'a>(
        &self,
        matches: &NodeRuleMatches,
        nodes: impl IntoIterator<Item = &'a NodeRef>,
    ) -> Option<BTreeSet<ClusterNodeName>> {
        let mut admitted: Option<BTreeSet<ClusterNodeName>> = None;
        for node in nodes {
            let Some(own) = self.admitted(matches, node) else {
                continue;
            };
            admitted = Some(match admitted {
                Some(mut so_far) => {
                    so_far.retain(|candidate| own.contains(candidate));
                    so_far
                }
                None => own,
            });
        }
        admitted
    }

    /// How many winning `PREFER` selectors on `node` admit `candidate`.
    pub(in crate::registry) fn preference_score(
        &self,
        matches: &NodeRuleMatches,
        node: &NodeRef,
        candidate: &ClusterNodeName,
    ) -> usize {
        let Some(ladder) = self.ladders.get(node) else {
            return 0;
        };
        let mut score = 0usize;
        for index in &ladder.preference {
            let admits = matches.matches[*index]
                .as_ref()
                .is_some_and(|rule_matches| rule_matches.contains(candidate));
            if admits {
                score = score
                    .checked_add(1)
                    .assured("a score counts rules held in memory");
            }
        }
        score
    }

    /// The winning `REQUIRE` rules on `node`, in name order; empty when none constrains it.
    pub(in crate::registry) fn requirement_rules(&self, node: &NodeRef) -> Vec<PlacementName> {
        let Some(ladder) = self.ladders.get(node) else {
            return Vec::new();
        };
        if ladder.requirement_lifted {
            return Vec::new();
        }
        ladder
            .requirement
            .iter()
            .map(|index| self.rules[*index].name.clone())
            .collect()
    }

    /// The winning `REQUIRE` rules on `node` whose selector does not admit `candidate`.
    fn unmet_requirements(
        &self,
        matches: &NodeRuleMatches,
        node: &NodeRef,
        candidate: &ClusterNodeName,
    ) -> Vec<PlacementName> {
        let Some(ladder) = self.ladders.get(node) else {
            return Vec::new();
        };
        if ladder.requirement_lifted {
            return Vec::new();
        }
        let mut unmet = Vec::new();
        for index in &ladder.requirement {
            let admits = matches.matches[*index]
                .as_ref()
                .is_some_and(|rule_matches| rule_matches.contains(candidate));
            if !admits {
                unmet.push(self.rules[*index].name.clone());
            }
        }
        unmet
    }

    /// Judges every unit of `schedule` that a winning `REQUIRE` rule constrains. `matches` are the
    /// rules evaluated over the records of the live nodes, and `candidates` are the nodes that can
    /// take new work now. A unit of a hard colocation group waits while no candidate admits every
    /// member of its group. A stopped domain's unit runs nowhere, so one stored on a live node its
    /// rules do not admit violates them only while a candidate could take it, and waits otherwise.
    pub(in crate::registry) fn judge(
        &self,
        schedule: &DomainSchedule,
        matches: &NodeRuleMatches,
        candidates: &BTreeSet<ClusterNodeName>,
        activity: ScheduleActivity,
    ) -> Vec<UnitRuleReport> {
        let mut groups = HashMap::<NodeUnitRef, usize>::default();
        for (index, group) in schedule.placement_groups.iter().enumerate() {
            for member in &group.members {
                groups.insert(member.clone(), index);
            }
        }
        let mut reports = Vec::new();
        for node in schedule.nodes.values() {
            if node.config.executes_on_every_cluster_node() {
                continue;
            }
            let identity = node.identity();
            let Some(admitted) = self.admitted(matches, &identity) else {
                continue;
            };
            for (unit, scheduled) in &node.units {
                let member = NodeUnitRef::new(identity.clone(), *unit);
                let owner = scheduled.execution_node().cloned();
                let live_owner = owner
                    .as_ref()
                    .filter(|owner| matches.nodes.contains(*owner));
                let placeable = || {
                    self.has_admitted_candidate(
                        schedule,
                        groups.get(&member).copied(),
                        matches,
                        &admitted,
                        candidates,
                    )
                };
                let state = match (live_owner, activity) {
                    (Some(owner), _) if admitted.contains(owner) => UnitRuleState::Satisfied,
                    (Some(owner), ScheduleActivity::Executing) => {
                        UnitRuleState::Violates(self.unmet_requirements(matches, &identity, owner))
                    }
                    (Some(owner), ScheduleActivity::Stopped) => {
                        if placeable() {
                            UnitRuleState::Violates(
                                self.unmet_requirements(matches, &identity, owner),
                            )
                        } else {
                            UnitRuleState::WaitingFor(self.requirement_rules(&identity))
                        }
                    }
                    (None, _) => {
                        if placeable() {
                            UnitRuleState::Unplaced
                        } else {
                            UnitRuleState::WaitingFor(self.requirement_rules(&identity))
                        }
                    }
                };
                reports.push(UnitRuleReport {
                    unit: member,
                    owner,
                    state,
                });
            }
        }
        reports
    }

    /// Whether a candidate node can take `member`: one its winning requirements admit, and, for a
    /// member of the hard colocation group at `group`, one every member's requirements admit.
    fn has_admitted_candidate(
        &self,
        schedule: &DomainSchedule,
        group: Option<usize>,
        matches: &NodeRuleMatches,
        admitted: &BTreeSet<ClusterNodeName>,
        candidates: &BTreeSet<ClusterNodeName>,
    ) -> bool {
        let group_admitted = match group {
            Some(group) => self
                .admitted_by_all(
                    matches,
                    schedule.placement_groups[group]
                        .members
                        .iter()
                        .map(|member| &member.node),
                )
                .verified("this group holds a constrained member"),
            None => admitted.clone(),
        };
        group_admitted
            .iter()
            .any(|candidate| candidates.contains(candidate))
    }

    /// Every rule's plan, in name order.
    pub(in crate::registry) fn plans(&self) -> Vec<NodeRulePlan> {
        self.rules
            .iter()
            .enumerate()
            .map(|(index, rule)| NodeRulePlan {
                name: rule.name.clone(),
                scope: rule.scope.clone(),
                policy: rule.policy.clone(),
                rank: rule.rank,
                members: rule
                    .members
                    .iter()
                    .map(|member| NodeRuleMemberPlan {
                        node: member.clone(),
                        overridden_by: self.overriding_rules(index, member),
                    })
                    .collect(),
            })
            .collect()
    }

    /// The rules that win on `member` instead of the rule at `index`, empty when it wins itself.
    fn overriding_rules(&self, index: usize, member: &NodeRef) -> Vec<PlacementName> {
        let ladder = self
            .ladders
            .get(member)
            .verified("every member of a rule has a ladder that rule claims");
        let winners = match self.rules[index].policy {
            NodePolicy::Require(_) | NodePolicy::Any => &ladder.requirement,
            NodePolicy::Prefer(_) => &ladder.preference,
        };
        // Winners are claim indices in ascending order, as `strongest_claims` keeps them.
        if winners.binary_search(&index).is_ok() {
            return Vec::new();
        }
        winners
            .iter()
            .map(|winner| self.rules[*winner].name.clone())
            .collect()
    }
}

impl NodeRulePlan {
    /// How much of the rule takes effect, given the live candidate nodes its selector admits now
    /// (absent for `ANY NODE`, which admits every node).
    pub(crate) fn coverage(
        &self,
        admitted: Option<&BTreeSet<ClusterNodeName>>,
    ) -> NodeRuleCoverage {
        if self.members.is_empty() {
            return NodeRuleCoverage::Empty;
        }
        let winning = self
            .members
            .iter()
            .filter(|member| member.overridden_by.is_empty())
            .count();
        if winning == 0 {
            return NodeRuleCoverage::Overridden;
        }
        if admitted.is_some_and(BTreeSet::is_empty) {
            return NodeRuleCoverage::Unmatched;
        }
        if winning == self.members.len() {
            NodeRuleCoverage::Effective
        } else {
            NodeRuleCoverage::Partial
        }
    }
}

/// The claims among `claims` at the strongest rank: explicit ranks ascending, then the unranked
/// tier. `claims` are indices in name order, and so are the winners.
fn strongest_claims(rules: &[NodeRule], claims: &[usize]) -> Vec<usize> {
    let strongest = claims
        .iter()
        .map(|index| placement_rank_key(rules[*index].rank))
        .min()
        .verified("a runtime node enters the claims together with its first claim");
    claims
        .iter()
        .copied()
        .filter(|index| placement_rank_key(rules[*index].rank) == strongest)
        .collect()
}

/// A schedule judged against the node rules of the graph it is planned for.
#[derive(Debug, Clone, Default)]
pub(crate) struct NodeRuleJudgement {
    /// Every unit a winning `REQUIRE` rule constrains, in schedule order.
    pub(crate) units: Vec<UnitRuleReport>,
    /// The position of each unit in `units`.
    by_unit: BTreeMap<NodeUnitRef, usize>,
    plans: Vec<NodeRulePlan>,
    matches: NodeRuleMatches,
    candidates: BTreeSet<ClusterNodeName>,
}

/// The units a judgement finds breaking a node rule, so that a later plan refuses only a breach
/// it introduces itself, never one a cluster change left behind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct NodeRuleBreaches {
    violating: BTreeSet<NodeUnitRef>,
    waiting: BTreeSet<NodeUnitRef>,
}

impl NodeRuleJudgement {
    /// Each rule's plan, in name order.
    pub(crate) fn plans(&self) -> &[NodeRulePlan] {
        &self.plans
    }

    /// The live candidate nodes the rule's selector admits now; absent for `ANY NODE`.
    pub(crate) fn matching_nodes(&self, rule: &NodeRulePlan) -> Option<BTreeSet<ClusterNodeName>> {
        let index = self
            .plans
            .binary_search_by(|plan| plan.name.as_str().cmp(rule.name.as_str()))
            .ok()?;
        let matches = self.matches.matches.get(index)?.as_ref()?;
        Some(
            matches
                .iter()
                .filter(|node| self.candidates.contains(*node))
                .cloned()
                .collect(),
        )
    }

    /// The nodes on which a rule's selector failed to evaluate.
    pub(crate) fn failures(&self) -> &[NodeRuleSelectorFailure] {
        &self.matches.failures
    }

    /// How `unit` stands against the rules that constrain it; absent for a unit none constrains.
    pub(crate) fn state_of(&self, unit: &NodeUnitRef) -> Option<&UnitRuleState> {
        let index = self.by_unit.get(unit)?;
        Some(&self.units[*index].state)
    }

    /// Whether the rule's selector admits the live node `node`; absent for `ANY NODE`, which
    /// admits every node.
    pub(crate) fn rule_admits(&self, rule: &NodeRulePlan, node: &ClusterNodeName) -> Option<bool> {
        let index = self
            .plans
            .binary_search_by(|plan| plan.name.as_str().cmp(rule.name.as_str()))
            .ok()?;
        let matches = self.matches.matches.get(index)?.as_ref()?;
        Some(matches.contains(node))
    }

    /// The units breaking a rule now.
    pub(crate) fn breaches(&self) -> NodeRuleBreaches {
        let mut breaches = NodeRuleBreaches::default();
        for report in &self.units {
            match &report.state {
                UnitRuleState::Violates(_) => {
                    breaches.violating.insert(report.unit.clone());
                }
                UnitRuleState::WaitingFor(_) => {
                    breaches.waiting.insert(report.unit.clone());
                }
                UnitRuleState::Satisfied | UnitRuleState::Unplaced => {}
            }
        }
        breaches
    }

    /// The first breach of this judgement that `standing` does not already hold, as the refusal
    /// a running domain answers a change with.
    pub(crate) fn new_breach(
        &self,
        domain: &DomainName,
        standing: &NodeRuleBreaches,
    ) -> Option<Report<RegistryError>> {
        for report in &self.units {
            match &report.state {
                UnitRuleState::Violates(rules) if !standing.violating.contains(&report.unit) => {
                    let placement = rules.first()?.clone();
                    let node = report.owner.clone()?;
                    return Some(Report::new(RegistryError::PlacementNodeRuleViolation {
                        domain: domain.clone(),
                        placement,
                        node,
                        kind: report.unit.node.kind.as_str(),
                        identifier: report.unit.node.identifier.clone(),
                    }));
                }
                UnitRuleState::WaitingFor(rules) if !standing.waiting.contains(&report.unit) => {
                    let placement = rules.first()?.clone();
                    return Some(Report::new(RegistryError::PlacementNodeRuleUnsatisfiable {
                        domain: domain.clone(),
                        placement,
                        kind: report.unit.node.kind.as_str(),
                        identifier: report.unit.node.identifier.clone(),
                    }));
                }
                UnitRuleState::Satisfied
                | UnitRuleState::Unplaced
                | UnitRuleState::Violates(_)
                | UnitRuleState::WaitingFor(_) => {}
            }
        }
        None
    }

    /// The units a rule's live owner breaks, and the units waiting for a node.
    pub(crate) fn count(&self) -> NodeRuleUnitCounts {
        let mut counts = NodeRuleUnitCounts::default();
        for report in &self.units {
            match report.state {
                UnitRuleState::Violates(_) => {
                    counts.violating = counts
                        .violating
                        .checked_add(1)
                        .assured("a count of scheduled units fits in memory");
                }
                UnitRuleState::WaitingFor(_) => {
                    counts.waiting = counts
                        .waiting
                        .checked_add(1)
                        .assured("a count of scheduled units fits in memory");
                }
                UnitRuleState::Satisfied | UnitRuleState::Unplaced => {}
            }
        }
        counts
    }
}

/// How many of a domain's units break a node rule, the values the two placement gauges export.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct NodeRuleUnitCounts {
    pub(crate) violating: u64,
    pub(crate) waiting: u64,
}

impl crate::registry::graph::ActiveGraph {
    /// Judges `schedule` against this graph's node rules, evaluated over `live_records`, the
    /// committed records of the live nodes; `candidates` are the nodes that can take new work, and
    /// `activity` whether the schedule's units execute where it assigns them.
    pub(crate) fn judge_node_rules(
        &self,
        domain: &DomainName,
        schedule: &DomainSchedule,
        live_records: &BTreeMap<ClusterNodeName, CommittedNodeRecord>,
        candidates: &BTreeSet<ClusterNodeName>,
        activity: ScheduleActivity,
    ) -> Result<NodeRuleJudgement, Report<RegistryError>> {
        let rules = self.placement.node_rules();
        if rules.is_empty() {
            return Ok(NodeRuleJudgement::default());
        }
        let matches = rules.evaluate(domain, live_records)?;
        let units = rules.judge(schedule, &matches, candidates, activity);
        let mut by_unit = BTreeMap::new();
        for (index, report) in units.iter().enumerate() {
            by_unit.insert(report.unit.clone(), index);
        }
        Ok(NodeRuleJudgement {
            units,
            by_unit,
            plans: rules.plans(),
            matches,
            candidates: candidates.clone(),
        })
    }
}

impl crate::registry::graph::ActiveGraph {
    /// The nodes among `schedulable` each runtime node a winning `REQUIRE` rule constrains may
    /// move to, judged by `records`. A runtime node absent from the map may move to any of them.
    pub(crate) fn node_rule_targets(
        &self,
        domain: &DomainName,
        records: &BTreeMap<ClusterNodeName, CommittedNodeRecord>,
        schedulable: &BTreeSet<ClusterNodeName>,
    ) -> Result<BTreeMap<NodeRef, BTreeSet<ClusterNodeName>>, Report<RegistryError>> {
        let rules = self.placement.node_rules();
        if rules.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut candidates = BTreeMap::new();
        for node in schedulable {
            if let Some(record) = records.get(node) {
                candidates.insert(node.clone(), record.clone());
            }
        }
        let matches = rules.evaluate(domain, &candidates)?;
        let mut targets = BTreeMap::new();
        for node in rules.ladders.keys() {
            if let Some(admitted) = rules.admitted(&matches, node) {
                targets.insert(node.clone(), admitted);
            }
        }
        Ok(targets)
    }
}

#[cfg(test)]
mod tests;
