//! Regressions for the division of the scenario suite between the shards of a sharded run.
//!
//! Layer: test harness.
//! - **Owns.** Stand-in features with plain scenarios, outline examples and rules, and the
//!   division every shard count makes of them.
//! - **Depends on.** The shard option, the shard's selection and Cucumber's Gherkin parser.
//! - **Must not know.** What the suite's scenarios do or which CI job runs a shard.

use std::{collections::BTreeSet, convert::Infallible, num::NonZeroUsize, path::PathBuf};

use clap::Parser as _;
use cucumber::{
    feature::Ext as _,
    gherkin::{Feature, GherkinEnv},
};
use meticulous::{OptionExt as _, ResultExt as _};

use crate::scenario_shard::{
    SCENARIO_SHARD_ENV, ScenarioShard, ScenarioShardArgs, ScenarioShardError,
};

/// A feature of plain scenarios, one of them before an outline and one after it, with the
/// outline's rows split over two example blocks.
const OUTLINE_FEATURE: &str = "\
Feature: Outline
  Background:
    Given a cluster

  Scenario: First
    Given a step

  Scenario Outline: Rows of <case>
    Given the case <case>

    Examples:
      | case |
      | one  |
      | two  |
      | three |

    @tagged
    Examples:
      | case |
      | four |
      | five |

  Scenario: Last
    Given a step
";

/// A feature whose scenarios sit outside a rule and inside two rules.
const RULE_FEATURE: &str = "\
Feature: Rules
  Scenario: Outside
    Given a step

  Rule: The first rule
    Scenario: First in a rule
      Given a step

    Scenario: Second in a rule
      Given a step

  Rule: The second rule
    Scenario: Third in a rule
      Given a step
";

/// A feature of six plain scenarios, whose runs follow each other.
const SIX_FEATURE: &str = "\
Feature: Six
  Scenario: One
    Given a step
  Scenario: Two
    Given a step
  Scenario: Three
    Given a step
  Scenario: Four
    Given a step
  Scenario: Five
    Given a step
  Scenario: Six
    Given a step
";

/// The runs a stand-in suite holds: two plain scenarios and five outline rows, four rule
/// scenarios, and six plain scenarios.
const SUITE_RUNS: usize = 17;

fn feature(path: &str, text: &str) -> Feature {
    let mut feature = Feature::parse(text, GherkinEnv::default())
        .assured("the stand-in feature is valid Gherkin");
    feature.path = Some(PathBuf::from(path));
    feature
        .expand_examples()
        .assured("the stand-in outline names only the columns of its examples")
}

/// The stand-in suite as the parser hands it over, in an order other than its paths' order.
fn suite() -> Vec<Result<Feature, Infallible>> {
    vec![
        Ok(feature("tests/features/rules.feature", RULE_FEATURE)),
        Ok(feature("tests/features/outline.feature", OUTLINE_FEATURE)),
        Ok(feature("tests/features/six.feature", SIX_FEATURE)),
    ]
}

/// One kept run: its feature file and its scenario's name.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct KeptRun {
    path: PathBuf,
    name: String,
}

/// The runs `features` still hold, each with its feature's path and its scenario's name.
fn kept_runs(features: &[Result<Feature, Infallible>]) -> Vec<KeptRun> {
    let mut runs = Vec::new();
    for feature in features.iter().flatten() {
        let path = feature
            .path
            .clone()
            .assured("every stand-in feature has a path");
        let in_rules = feature.rules.iter().flat_map(|rule| &rule.scenarios);
        for scenario in feature.scenarios.iter().chain(in_rules) {
            runs.push(KeptRun {
                path: path.clone(),
                name: scenario.name.clone(),
            });
        }
    }
    runs
}

fn shard(index: usize, count: usize) -> ScenarioShard {
    let index = NonZeroUsize::new(index).assured("the test names shards from one");
    let count = NonZeroUsize::new(count).assured("the test names at least one shard");
    ScenarioShard::new(index, count).assured("the test names a shard of its count")
}

#[test]
fn a_shard_is_written_as_its_index_over_the_count() {
    let parsed: ScenarioShard = "2/3"
        .parse()
        .assured("2/3 names the second of three shards");
    assert_eq!(parsed, shard(2, 3));
    assert_eq!(parsed.to_string(), "2 of 3");
}

#[test]
fn a_shard_text_must_name_one_existing_shard() {
    let refused = [
        ("3", "form"),
        ("1-3", "form"),
        ("x/3", "number"),
        ("1/-3", "number"),
        ("1/3/4", "number"),
        ("0/3", "zero"),
        ("1/0", "zero"),
        ("4/3", "beyond"),
    ];
    for (text, expected) in refused {
        let Err(error) = text.parse::<ScenarioShard>() else {
            panic!("{text:?} must name no shard");
        };
        let kind = match error {
            ScenarioShardError::Form => "form",
            ScenarioShardError::Number { .. } => "number",
            ScenarioShardError::Zero => "zero",
            ScenarioShardError::BeyondCount { .. } => "beyond",
        };
        assert_eq!(kind, expected, "{text:?} was refused as {error}");
    }
}

#[test]
fn every_run_belongs_to_exactly_one_shard_of_each_count() {
    let mut all = kept_runs(&suite());
    all.sort();
    assert_eq!(all.len(), SUITE_RUNS, "the stand-in suite holds {all:?}");
    for count in 1..=SUITE_RUNS + 1 {
        let mut divided = Vec::new();
        for index in 1..=count {
            let mut features = suite();
            let selection = shard(index, count).keep_own_runs(&mut features);
            let kept = kept_runs(&features);
            assert_eq!(selection.read, SUITE_RUNS);
            assert_eq!(selection.kept, kept.len());
            assert!(
                kept.len().abs_diff(SUITE_RUNS / count) <= 1,
                "shard {index} of {count} kept {} runs: {kept:?}",
                kept.len()
            );
            divided.extend(kept);
        }
        divided.sort();
        assert_eq!(
            divided, all,
            "the shards of {count} must run every run once"
        );
    }
}

#[test]
fn consecutive_runs_of_one_feature_go_to_different_shards() {
    let mut names = Vec::new();
    for index in 1..=3 {
        let mut features = vec![Ok(feature("tests/features/six.feature", SIX_FEATURE))];
        shard(index, 3).keep_own_runs(&mut features);
        let kept: Vec<String> = kept_runs(&features)
            .into_iter()
            .map(|run| run.name)
            .collect();
        names.push(kept);
    }
    assert_eq!(names, [["One", "Four"], ["Two", "Five"], ["Three", "Six"]]);
}

#[test]
fn every_outline_row_and_rule_scenario_is_its_own_run() {
    let mut names = BTreeSet::new();
    for index in 1..=2 {
        let mut features = suite();
        shard(index, 2).keep_own_runs(&mut features);
        let mut kept = BTreeSet::new();
        for run in kept_runs(&features) {
            kept.insert(run.name);
        }
        assert!(
            names.is_disjoint(&kept),
            "two shards both kept {:?}",
            names.intersection(&kept).collect::<Vec<_>>()
        );
        names.extend(kept);
    }
    for expected in [
        "Rows of one",
        "Rows of five",
        "Outside",
        "Third in a rule",
        "Last",
    ] {
        assert!(
            names.contains(expected),
            "no shard ran {expected}: {names:?}"
        );
    }
}

/// A command line that carries only the shard option, so it can be parsed on its own.
#[derive(clap::Parser)]
struct StandInShardCli {
    #[command(flatten)]
    shard: ScenarioShardArgs,
}

#[test]
fn the_shard_option_reads_its_text_and_otherwise_the_environment() {
    let configured = StandInShardCli::try_parse_from(["scenarios", "--shard", "2/3"])
        .assured("2/3 names an existing shard");
    assert_eq!(configured.shard.shard(), Some(shard(2, 3)));
    assert!(
        StandInShardCli::try_parse_from(["scenarios", "--shard", "4/3"]).is_err(),
        "a shard beyond its count must be refused"
    );

    // The option also reads `NERVIX_TEST_SCENARIO_SHARD`, so only a run that sets neither executes
    // every scenario. This process may be either kind of run.
    let expected = match std::env::var(SCENARIO_SHARD_ENV) {
        Ok(given) => Some(
            given
                .parse::<ScenarioShard>()
                .assured("the environment of a test run names an existing shard"),
        ),
        Err(_) => None,
    };
    let default = StandInShardCli::try_parse_from(["scenarios"]).assured("the option is optional");
    assert_eq!(default.shard.shard(), expected);
}
