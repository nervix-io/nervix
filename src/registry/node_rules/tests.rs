//! Node rule decisions exercised from Plexus sources over committed node records.
//!
//! Layer: test harness.
//! - **Owns.** The requirements and preferences ladders, their conflicts, coverage, schedule
//!   judgement, and the production and random planners' candidate restriction and order.
//! - **Depends on.** The production registry decisions, the Plexus parser and record fixtures.
//! - **Must not know.** Liveness capture, consensus or the control plane that reports a refusal.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
};

use meticulous::ResultExt as _;
use nervix_models::{
    ClusterNodeName, CommittedNodeRecord, DomainName, DomainSchedule, ModelKind, ModelName,
    NodePolicy, NodeRef, NodeRuleScope, NodeUnitRef, Unit,
};

use super::*;
use crate::registry::{
    graph::ActiveGraph,
    scheduler::{committed_record, uniform_capabilities},
    storage::Registry,
    test_fixtures::{example_graph_models, named, scheduled_node, temp_db_path},
};

/// Two junctions in a line, `scorer` then `archiver`, joined through `rule_middle`.
const GRAPH: &str = "
CREATE UNPACED DOMAIN rules;
CREATE SCHEMA rule_event ( id I64 );
CREATE RELAY rule_input SCHEMA rule_event UNBRANCHED;
CREATE RELAY rule_middle SCHEMA rule_event UNBRANCHED;
CREATE RELAY rule_output SCHEMA rule_event UNBRANCHED;
CREATE JUNCTION scorer FROM rule_input UNBRANCHED
  TO rule_middle INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
CREATE JUNCTION archiver FROM rule_middle UNBRANCHED
  TO rule_output INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG;
";

/// A registry holding [`GRAPH`] and `rules`, removed when dropped.
struct Fixture {
    path: PathBuf,
    registry: Option<Registry>,
    domain: DomainName,
}

impl Fixture {
    fn new(rules: &str) -> Self {
        let fixture = Self::empty();
        let (domain, models) = example_graph_models("node rules", &format!("{GRAPH}{rules}"));
        fixture
            .registry()
            .apply_batch(&domain, models)
            .assured("the fixture's node rules validate");
        fixture
    }

    fn empty() -> Self {
        let path = temp_db_path();
        let registry = Registry::open(&path).assured("a fresh registry opens");
        Self {
            path,
            registry: Some(registry),
            domain: named("rules"),
        }
    }

    fn registry(&self) -> &Registry {
        self.registry
            .as_ref()
            .verified("the registry stays open until the fixture drops")
    }

    fn graph(&self) -> ActiveGraph {
        self.registry()
            .active_graph(&self.domain)
            .verified("the fixture installs its graph")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.registry.take());
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn node(name: &str) -> ClusterNodeName {
    named(name)
}

fn nodes() -> Vec<ClusterNodeName> {
    vec![node("node-1"), node("node-2"), node("node-3")]
}

fn junction(name: &str) -> NodeRef {
    NodeRef::new(ModelKind::Junction, named::<ModelName>(name))
}

fn single(name: &str) -> NodeUnitRef {
    NodeUnitRef::new(junction(name), Unit::SINGLE)
}

/// The committed records of node-1 to node-3 carrying `tags`, in order.
fn records(tags: [&[&str]; 3]) -> BTreeMap<ClusterNodeName, CommittedNodeRecord> {
    nodes()
        .into_iter()
        .zip(tags)
        .map(|(name, tags)| {
            let record = committed_record(&name, uniform_capabilities(), tags);
            (name, record)
        })
        .collect()
}

fn owner(schedule: &DomainSchedule, name: &str) -> ClusterNodeName {
    scheduled_node(schedule, ModelKind::Junction, name)
        .primary_node()
        .cloned()
        .verified("the fixture places every junction")
}

fn schedule(
    graph: &ActiveGraph,
    domain: &DomainName,
    replicas: usize,
    existing: Option<&DomainSchedule>,
    records: &BTreeMap<ClusterNodeName, CommittedNodeRecord>,
) -> DomainSchedule {
    graph
        .schedule_for_domain(domain, &nodes(), replicas, existing, records)
        .assured("the fixture topology schedules")
}

fn judge(
    graph: &ActiveGraph,
    domain: &DomainName,
    schedule: &DomainSchedule,
    records: &BTreeMap<ClusterNodeName, CommittedNodeRecord>,
) -> NodeRuleJudgement {
    judge_as(
        graph,
        domain,
        schedule,
        records,
        ScheduleActivity::Executing,
    )
}

fn judge_as(
    graph: &ActiveGraph,
    domain: &DomainName,
    schedule: &DomainSchedule,
    records: &BTreeMap<ClusterNodeName, CommittedNodeRecord>,
    activity: ScheduleActivity,
) -> NodeRuleJudgement {
    let candidates = records.keys().cloned().collect::<BTreeSet<_>>();
    graph
        .judge_node_rules(domain, schedule, records, &candidates, activity)
        .assured("the fixture's selectors evaluate")
}

fn plan<'a>(judgement: &'a NodeRuleJudgement, name: &str) -> &'a NodeRulePlan {
    judgement
        .plans()
        .iter()
        .find(|plan| plan.name.as_str() == name)
        .verified("the fixture declares the rule")
}

fn coverage(judgement: &NodeRuleJudgement, name: &str) -> NodeRuleCoverage {
    let rule = plan(judgement, name);
    rule.coverage(judgement.matching_nodes(rule).as_ref())
}

fn names(rules: &[PlacementName]) -> Vec<&str> {
    rules.iter().map(PlacementName::as_str).collect()
}

#[test]
fn the_requirements_ladder_intersects_its_strongest_rank_and_overrides_weaker_rules() {
    let fixture = Fixture::new(
        "
        CREATE PLACEMENT eu FOR scorer REQUIRE NODES WHERE tag('zone') = 'eu' RANK 1;
        CREATE PLACEMENT fast FOR scorer REQUIRE NODES WHERE tag('pool') = 'fast' RANK 1;
        CREATE PLACEMENT us FOR scorer REQUIRE NODES WHERE tag('zone') = 'us' RANK 2;
        ",
    );
    let graph = fixture.graph();
    let records = records([
        &["zone=eu"],
        &["zone=eu", "pool=fast"],
        &["zone=us", "pool=fast"],
    ]);
    let targets = graph
        .node_rule_targets(&fixture.domain, &records, &nodes().into_iter().collect())
        .assured("the fixture's selectors evaluate");
    assert_eq!(
        targets.get(&junction("scorer")),
        Some(&BTreeSet::from([node("node-2")])),
        "both rank-1 selectors must admit a node; the rank-2 rule does not count"
    );
    assert_eq!(
        targets.get(&junction("archiver")),
        None,
        "no rule covers the archiver"
    );

    let schedule = schedule(&graph, &fixture.domain, 0, None, &records);
    assert_eq!(owner(&schedule, "scorer"), node("node-2"));
    let judgement = judge(&graph, &fixture.domain, &schedule, &records);
    assert_eq!(
        judgement.state_of(&single("scorer")),
        Some(&UnitRuleState::Satisfied)
    );
    assert_eq!(coverage(&judgement, "eu"), NodeRuleCoverage::Effective);
    assert_eq!(coverage(&judgement, "us"), NodeRuleCoverage::Overridden);
    assert_eq!(
        names(&plan(&judgement, "us").members[0].overridden_by),
        ["eu", "fast"]
    );
    assert_eq!(
        judgement.matching_nodes(plan(&judgement, "eu")),
        Some(BTreeSet::from([node("node-1"), node("node-2")]))
    );
}

#[test]
fn any_node_lifts_weaker_requirements_and_conflicts_with_a_requirement_at_its_rank() {
    let fixture = Fixture::new(
        "
        CREATE PLACEMENT eu DOMAIN WIDE REQUIRE NODES WHERE tag('zone') = 'eu' RANK 2;
        CREATE PLACEMENT free FOR archiver ANY NODE RANK 1;
        ",
    );
    let graph = fixture.graph();
    let records = records([&[], &["zone=eu"], &[]]);
    let targets = graph
        .node_rule_targets(&fixture.domain, &records, &nodes().into_iter().collect())
        .assured("the fixture's selectors evaluate");
    assert_eq!(
        targets.get(&junction("scorer")),
        Some(&BTreeSet::from([node("node-2")]))
    );
    assert_eq!(
        targets.get(&NodeRef::new(
            ModelKind::Relay,
            named::<ModelName>("rule_middle")
        )),
        Some(&BTreeSet::from([node("node-2")])),
        "a domain-wide rule covers every runtime node, relays included"
    );
    assert_eq!(
        targets.get(&junction("archiver")),
        None,
        "the stronger ANY NODE lifts the domain-wide requirement"
    );
    let schedule = schedule(&graph, &fixture.domain, 0, None, &records);
    let judgement = judge(&graph, &fixture.domain, &schedule, &records);
    assert_eq!(coverage(&judgement, "eu"), NodeRuleCoverage::Partial);
    assert_eq!(coverage(&judgement, "free"), NodeRuleCoverage::Effective);
    assert_eq!(judgement.state_of(&single("archiver")), None);

    let conflicting = Fixture::empty();
    let (domain, models) = example_graph_models(
        "conflicting node rules",
        &format!(
            "{GRAPH}
            CREATE PLACEMENT required FOR scorer REQUIRE NODES WHERE tag('zone') = 'eu';
            CREATE PLACEMENT anywhere FOR scorer ANY NODE;
            "
        ),
    );
    let error = conflicting
        .registry()
        .apply_batch(&domain, models)
        .expect_err("ANY NODE beside REQUIRE NODES at the same rank conflicts");
    assert!(
        format!("{error:?}").contains(
            "placements 'anywhere' and 'required' conflict on runtime node junction 'scorer'"
        ),
        "{error:?}"
    );
}

#[test]
fn the_preferences_ladder_counts_each_satisfied_selector_at_its_strongest_rank() {
    let fixture = Fixture::new(
        "
        CREATE PLACEMENT prefer_eu FOR scorer PREFER NODES WHERE tag('zone') = 'eu' RANK 1;
        CREATE PLACEMENT prefer_fast FOR scorer PREFER NODES WHERE tag('pool') = 'fast' RANK 1;
        CREATE PLACEMENT prefer_us FOR scorer PREFER NODES WHERE tag('zone') = 'us' RANK 2;
        ",
    );
    let graph = fixture.graph();
    let records = records([&["zone=eu"], &["zone=eu", "pool=fast"], &["zone=us"]]);
    let rules = graph.placement.node_rules();
    let matches = rules
        .evaluate(&fixture.domain, &records)
        .assured("the fixture's selectors evaluate");
    let scores = nodes()
        .iter()
        .map(|candidate| rules.preference_score(&matches, &junction("scorer"), candidate))
        .collect::<Vec<_>>();
    assert_eq!(scores, [1, 2, 0]);
    assert_eq!(
        rules.admitted(&matches, &junction("scorer")),
        None,
        "a preference never restricts the candidates"
    );

    let schedule = schedule(&graph, &fixture.domain, 0, None, &records);
    assert_eq!(owner(&schedule, "scorer"), node("node-2"));
    let judgement = judge(&graph, &fixture.domain, &schedule, &records);
    assert_eq!(
        judgement.state_of(&single("scorer")),
        None,
        "only a requirement constrains a unit"
    );
    assert_eq!(
        coverage(&judgement, "prefer_us"),
        NodeRuleCoverage::Overridden
    );
}

#[test]
fn a_hard_colocation_group_runs_where_every_member_is_admitted_and_sums_its_preferences() {
    let required = Fixture::new(
        "
        CREATE PLACEMENT local FROM scorer TO archiver REQUIRE COLOCATION RANK 1;
        CREATE PLACEMENT scorer_eu FOR scorer REQUIRE NODES WHERE tag('zone') = 'eu';
        CREATE PLACEMENT archiver_fast FOR archiver REQUIRE NODES WHERE tag('pool') = 'fast';
        ",
    );
    let graph = required.graph();
    let required_records = records([&["zone=eu"], &["zone=eu", "pool=fast"], &["pool=fast"]]);
    let placed = schedule(&graph, &required.domain, 0, None, &required_records);
    assert_eq!(owner(&placed, "scorer"), node("node-2"));
    assert_eq!(owner(&placed, "archiver"), node("node-2"));

    let preferred = Fixture::new(
        "
        CREATE PLACEMENT local FROM scorer TO archiver REQUIRE COLOCATION RANK 1;
        CREATE PLACEMENT scorer_eu FOR scorer PREFER NODES WHERE tag('zone') = 'eu';
        CREATE PLACEMENT archiver_fast FOR archiver PREFER NODES WHERE tag('pool') = 'fast';
        CREATE PLACEMENT archiver_gpu FOR archiver PREFER NODES WHERE tag('gpu') = 'yes';
        ",
    );
    let graph = preferred.graph();
    let preferred_records = records([&["zone=eu"], &["pool=fast", "gpu=yes"], &[]]);
    let placed = schedule(&graph, &preferred.domain, 0, None, &preferred_records);
    assert_eq!(
        owner(&placed, "scorer"),
        node("node-2"),
        "the group's score is its members' sum: 0 + 2 on node-2 beats 1 + 0 on node-1"
    );
    assert_eq!(owner(&placed, "archiver"), node("node-2"));
}

#[test]
fn a_requirement_restricts_every_assignment_slot_and_ranks_ahead_of_load() {
    let fixture = Fixture::new(
        "
        CREATE PLACEMENT two_zones DOMAIN WIDE REQUIRE NODES WHERE tag('zone') IN ('eu', 'uk');
        CREATE PLACEMENT prefer_uk FOR scorer, archiver PREFER NODES WHERE tag('zone') = 'uk';
        ",
    );
    let graph = fixture.graph();
    let records = records([&["zone=eu"], &["zone=uk"], &["zone=us"]]);
    let placed = schedule(&graph, &fixture.domain, 2, None, &records);
    for scheduled in placed.nodes.values() {
        for unit in scheduled.units.values() {
            assert!(
                unit.assigned_nodes
                    .iter()
                    .all(|assigned| *assigned != node("node-3")),
                "{} {:?}",
                scheduled.identity().identifier,
                unit.assigned_nodes
            );
        }
    }
    assert_eq!(
        owner(&placed, "scorer"),
        node("node-2"),
        "the preference ranks ahead of the load node-2 already carries"
    );
    assert_eq!(owner(&placed, "archiver"), node("node-2"));
}

#[test]
fn a_unit_no_node_admits_keeps_its_last_assignment_and_waits() {
    let fixture = Fixture::new(
        "
        CREATE PLACEMENT eu_only FOR scorer REQUIRE NODES WHERE tag('zone') = 'eu';
        ",
    );
    let graph = fixture.graph();
    let tagged = records([&[], &["zone=eu"], &[]]);
    let placed = schedule(&graph, &fixture.domain, 0, None, &tagged);
    assert_eq!(owner(&placed, "scorer"), node("node-2"));

    // node-2 restarts without its tag: it stays live, so its unit violates the rule.
    let untagged = records([&[], &[], &[]]);
    let judgement = judge(&graph, &fixture.domain, &placed, &untagged);
    assert_eq!(
        judgement.state_of(&single("scorer")),
        Some(&UnitRuleState::Violates(vec![named("eu_only")]))
    );
    assert_eq!(judgement.count().violating, 1);
    let standing = judgement.breaches();
    assert!(
        judgement.new_breach(&fixture.domain, &standing).is_none(),
        "a breach the cluster left behind does not refuse the next change"
    );
    let fresh = judgement
        .new_breach(&fixture.domain, &NodeRuleBreaches::default())
        .verified("a breach no earlier judgement held is new");
    assert!(
        fresh.to_string().contains(
            "placement 'eu_only' does not admit node 'node-2', which runs junction 'scorer'"
        ),
        "{fresh:?}"
    );

    // Stopped, nothing runs there: with no node to take it the unit waits, and once a tagged
    // node can take it the stored assignment violates the rule until START moves it.
    let stopped = judge_as(
        &graph,
        &fixture.domain,
        &placed,
        &untagged,
        ScheduleActivity::Stopped,
    );
    assert_eq!(
        stopped.state_of(&single("scorer")),
        Some(&UnitRuleState::WaitingFor(vec![named("eu_only")]))
    );
    let movable = judge_as(
        &graph,
        &fixture.domain,
        &placed,
        &records([&["zone=eu"], &[], &[]]),
        ScheduleActivity::Stopped,
    );
    assert_eq!(
        movable.state_of(&single("scorer")),
        Some(&UnitRuleState::Violates(vec![named("eu_only")]))
    );

    // node-2 leaves: no live node admits the scorer, so it keeps node-2 and waits.
    let mut departed = untagged.clone();
    departed.remove(&node("node-2"));
    let replanned = graph
        .schedule_for_domain(
            &fixture.domain,
            &[node("node-1"), node("node-3")],
            0,
            Some(&placed),
            &departed,
        )
        .assured("the fixture topology schedules");
    assert_eq!(owner(&replanned, "scorer"), node("node-2"));
    let judgement = judge(&graph, &fixture.domain, &replanned, &departed);
    assert_eq!(
        judgement.state_of(&single("scorer")),
        Some(&UnitRuleState::WaitingFor(vec![named("eu_only")]))
    );
    assert_eq!(judgement.count().waiting, 1);
    assert_eq!(coverage(&judgement, "eu_only"), NodeRuleCoverage::Unmatched);

    // A tagged node joins: the unit is placed there, its waiting is over.
    let returned = records([&["zone=eu"], &[], &[]]);
    let mut joined = returned.clone();
    joined.remove(&node("node-2"));
    let recovered = graph
        .schedule_for_domain(
            &fixture.domain,
            &[node("node-1"), node("node-3")],
            0,
            Some(&replanned),
            &joined,
        )
        .assured("the fixture topology schedules");
    assert_eq!(owner(&recovered, "scorer"), node("node-1"));
    let judgement = judge(&graph, &fixture.domain, &recovered, &joined);
    assert_eq!(
        judgement.state_of(&single("scorer")),
        Some(&UnitRuleState::Satisfied)
    );
}

#[test]
fn coverage_names_how_much_of_a_rule_takes_effect() {
    let member = |name: &str, overridden_by: &[&str]| NodeRuleMemberPlan {
        node: junction(name),
        overridden_by: overridden_by.iter().map(|rule| named(rule)).collect(),
    };
    let rule = |members: Vec<NodeRuleMemberPlan>| NodeRulePlan {
        name: named("rule"),
        scope: NodeRuleScope::DomainWide,
        policy: NodePolicy::Any,
        rank: None,
        members,
    };
    let some = BTreeSet::from([node("node-1")]);
    let none = BTreeSet::new();
    assert_eq!(
        rule(Vec::new()).coverage(Some(&some)),
        NodeRuleCoverage::Empty
    );
    assert_eq!(
        rule(vec![member("scorer", &[])]).coverage(Some(&some)),
        NodeRuleCoverage::Effective
    );
    assert_eq!(
        rule(vec![member("scorer", &[])]).coverage(None),
        NodeRuleCoverage::Effective,
        "ANY NODE admits every node"
    );
    assert_eq!(
        rule(vec![
            member("scorer", &[]),
            member("archiver", &["stronger"])
        ])
        .coverage(Some(&some)),
        NodeRuleCoverage::Partial
    );
    assert_eq!(
        rule(vec![member("scorer", &["stronger"])]).coverage(Some(&some)),
        NodeRuleCoverage::Overridden
    );
    assert_eq!(
        rule(vec![member("scorer", &[])]).coverage(Some(&none)),
        NodeRuleCoverage::Unmatched
    );
    assert_eq!(
        rule(vec![member("scorer", &["stronger"])]).coverage(Some(&none)),
        NodeRuleCoverage::Overridden,
        "a rule that wins nowhere is overridden, matched or not"
    );
}

#[cfg(feature = "testing")]
#[test]
fn the_random_test_scheduler_draws_only_admitted_nodes_and_ranks_preferred_ones_first() {
    let fixture = Fixture::new(
        "
        CREATE PLACEMENT eu_only FOR scorer REQUIRE NODES WHERE tag('zone') = 'eu';
        CREATE PLACEMENT prefer_fast FOR archiver PREFER NODES WHERE tag('pool') = 'fast';
        ",
    );
    let graph = fixture.graph();
    let records = records([&["pool=fast"], &[], &["zone=eu"]]);
    let placed = graph
        .schedule_for_domain_with_mode(
            &fixture.domain,
            &nodes(),
            0,
            None,
            super::super::scheduler::SchedulerMode::Random,
            &records,
        )
        .assured("the fixture topology schedules");
    assert_eq!(owner(&placed, "scorer"), node("node-3"));
    assert_eq!(owner(&placed, "archiver"), node("node-1"));
}
