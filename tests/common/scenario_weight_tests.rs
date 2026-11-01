//! Regressions for declared scenario load and the run slots each attempt reserves.
//!
//! Layer: test harness.
//! - **Owns.** Gherkin declaration fixtures and deterministic weighted-admission regressions.
//! - **Depends on.** The production harness's declaration and admission owners and Gherkin parser.
//! - **Must not know.** Product deadlines, scenario steps or the machine running the suite.

use std::{collections::BTreeMap, num::NonZeroU32};

use cucumber::{
    feature::Ext as _,
    gherkin::{Feature, GherkinEnv},
};
use meticulous::{OptionExt as _, ResultExt as _};

use crate::scenario_schedule::{
    FeatureLimit, ScenarioLength, ScenarioRun, ScenarioRunSlots, ScenarioWeight,
};

fn weight(tags: &[&str]) -> ScenarioWeight {
    ScenarioWeight::declared_by(tags.iter().copied())
        .assured("the fixture declares positive run-slot weights")
}

fn run(number: u32) -> ScenarioRun {
    ScenarioRun::new(NonZeroU32::new(number).assured("the fixture numbers attempts from one"))
}

#[test]
fn scenario_weight_reads_feature_rule_scenario_and_expanded_example_tags() {
    let feature = Feature::parse(
        "\
@slots_2
Feature: Weighted declarations
  Scenario: Feature weight
    Given a step

  @slots_3
  Scenario: Scenario weight
    Given a step

  Rule: Inherited feature weight
    Scenario: Feature weight inside a rule
      Given a step

  @slots_4
  Rule: Weighted rule
    Scenario: Rule weight
      Given a step

    @slots_5
    Scenario Outline: Example <case>
      Given a step

      Examples:
        | case     |
        | scenario |

      @slots_7
      Examples:
        | case    |
        | example |
",
        GherkinEnv::default(),
    )
    .assured("the declaration fixture is valid Gherkin")
    .expand_examples()
    .assured("the outline references its declared example column");
    let mut observed = BTreeMap::new();
    for scenario in &feature.scenarios {
        let declared = ScenarioWeight::for_scenario(&feature, None, scenario)
            .assured("the feature and scenario tags declare positive weights");
        observed.insert(
            scenario.name.as_str(),
            ScenarioRun::FIRST.run_slots(48, declared),
        );
    }
    for rule in &feature.rules {
        for scenario in &rule.scenarios {
            let declared = ScenarioWeight::for_scenario(&feature, Some(rule), scenario)
                .assured("the rule and expanded example tags declare positive weights");
            observed.insert(
                scenario.name.as_str(),
                ScenarioRun::FIRST.run_slots(48, declared),
            );
        }
    }
    assert_eq!(
        observed,
        BTreeMap::from([
            ("Feature weight", 2),
            ("Scenario weight", 3),
            ("Feature weight inside a rule", 2),
            ("Rule weight", 4),
            ("Example scenario", 5),
            ("Example example", 7),
        ])
    );
}

#[test]
fn scenario_weight_uses_the_largest_declaration_without_changing_length() {
    for tags in [
        vec!["slots_4", "slots_2", "long_running"],
        vec!["slots_2", "long_running", "slots_4"],
        vec!["slots_4", "slots_4", "long_running"],
    ] {
        let declared = weight(&tags);
        assert_eq!(ScenarioRun::FIRST.run_slots(48, declared), 4);
        assert_eq!(
            ScenarioLength::declared_by(tags.iter().copied()),
            ScenarioLength::LongRunning
        );
    }
    assert_eq!(weight(&[]), ScenarioWeight::ONE);
    assert_eq!(
        weight(&["long_running", "deloxide_stress"]),
        ScenarioWeight::ONE
    );
}

#[test]
fn scenario_weight_refuses_invalid_numeric_declarations_with_their_tag() {
    for tag in [
        "slots_0",
        "slots_",
        "slots_four",
        "slots_-1",
        "slots_999999999999999999999999999999999999",
    ] {
        let Err(error) = ScenarioWeight::declared_by([tag]) else {
            panic!("@{tag} must be refused as an invalid run-slot declaration");
        };
        assert!(error.to_string().contains(tag), "{error}");
        assert!(error.to_string().contains("positive"), "{error}");
    }
}

#[test]
fn scenario_weight_combines_with_retries_by_maximum_and_caps_every_attempt() {
    let four = weight(&["slots_4"]);
    assert_eq!(run(1).run_slots(48, four), 4);
    assert_eq!(run(2).run_slots(48, four), 4);
    assert_eq!(run(3).run_slots(48, four), 6);
    assert_eq!(run(9).run_slots(48, four), 6);
    let unbounded_declaration = format!("slots_{}", usize::MAX);
    let largest = weight(&[&unbounded_declaration]);
    for pool in 1..=512 {
        for number in 1..=9 {
            let held = run(number).run_slots(pool, largest);
            assert_eq!(held, (pool / 4).max(1));
            assert!(held <= pool);
            assert!(
                run(number).run_slots(pool, four)
                    >= run(number).run_slots(pool, ScenarioWeight::ONE)
            );
        }
    }
}

#[nervix_primitives::test]
async fn scenario_weight_holds_its_slots_through_the_admissions_lifetime() {
    let slots = ScenarioRunSlots::new(16);
    let weighted = slots
        .admit_with(
            FeatureLimit::Unlimited,
            ScenarioLength::Ordinary,
            weight(&["slots_4"]),
            "weighted first attempt",
            ScenarioRun::FIRST,
            |_| {},
        )
        .await;
    assert_eq!(weighted.slots(), 4);
    let mut running = Vec::new();
    for _ in 0..12 {
        nervix_primitives::task::consume_budget().await;
        running.push(
            slots
                .admit_with(
                    FeatureLimit::Unlimited,
                    ScenarioLength::Ordinary,
                    ScenarioWeight::ONE,
                    "ordinary beside the weighted attempt",
                    ScenarioRun::FIRST,
                    |_| {},
                )
                .await,
        );
    }
    let mut next = Box::pin(slots.admit_with(
        FeatureLimit::Unlimited,
        ScenarioLength::Ordinary,
        weight(&["slots_4"]),
        "next weighted attempt",
        ScenarioRun::FIRST,
        |_| {},
    ));
    assert!(futures_util::poll!(next.as_mut()).is_pending());
    drop(weighted);
    let std::task::Poll::Ready(next) = futures_util::poll!(next.as_mut()) else {
        panic!("ending the weighted admission must return all four slots together");
    };
    assert_eq!(next.slots(), 4);
}

#[nervix_primitives::test]
async fn scenario_weight_gathers_free_slots_before_smaller_first_attempts() {
    let slots = ScenarioRunSlots::new(8);
    let mut running = Vec::new();
    for _ in 0..8 {
        nervix_primitives::task::consume_budget().await;
        running.push(
            slots
                .admit_with(
                    FeatureLimit::Unlimited,
                    ScenarioLength::Ordinary,
                    ScenarioWeight::ONE,
                    "running ordinary attempt",
                    ScenarioRun::FIRST,
                    |_| {},
                )
                .await,
        );
    }
    let mut weighted = Box::pin(slots.admit_with(
        FeatureLimit::Unlimited,
        ScenarioLength::Ordinary,
        weight(&["slots_2"]),
        "weighted first attempt at the head",
        ScenarioRun::FIRST,
        |_| {},
    ));
    assert!(futures_util::poll!(weighted.as_mut()).is_pending());
    let mut behind = Box::pin(slots.admit_with(
        FeatureLimit::Unlimited,
        ScenarioLength::Ordinary,
        ScenarioWeight::ONE,
        "smaller first attempt behind it",
        ScenarioRun::FIRST,
        |_| {},
    ));
    assert!(futures_util::poll!(behind.as_mut()).is_pending());
    drop(running.pop());
    assert!(futures_util::poll!(weighted.as_mut()).is_pending());
    assert!(futures_util::poll!(behind.as_mut()).is_pending());
    drop(running.pop());
    let std::task::Poll::Ready(weighted) = futures_util::poll!(weighted.as_mut()) else {
        panic!("the two freed slots must admit the first queued weighted attempt");
    };
    assert_eq!(weighted.slots(), 2);
    assert!(futures_util::poll!(behind.as_mut()).is_pending());
    drop(weighted);
    assert!(futures_util::poll!(behind.as_mut()).is_ready());
}

#[nervix_primitives::test]
async fn scenario_weight_still_gives_retries_priority_and_respects_feature_limits() {
    let slots = ScenarioRunSlots::new(16);
    let mut running = Vec::new();
    for _ in 0..16 {
        nervix_primitives::task::consume_budget().await;
        running.push(
            slots
                .admit_with(
                    FeatureLimit::Unlimited,
                    ScenarioLength::Ordinary,
                    ScenarioWeight::ONE,
                    "running ordinary attempt",
                    ScenarioRun::FIRST,
                    |_| {},
                )
                .await,
        );
    }
    let mut first = Box::pin(slots.admit_with(
        FeatureLimit::WasmStateReset,
        ScenarioLength::LongRunning,
        weight(&["slots_4"]),
        "weighted limited first attempt",
        ScenarioRun::FIRST,
        |_| {},
    ));
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    let mut retry = Box::pin(slots.admit_with(
        FeatureLimit::Unlimited,
        ScenarioLength::Ordinary,
        ScenarioWeight::ONE,
        "retry behind the weighted first attempt",
        run(3),
        |_| {},
    ));
    assert!(futures_util::poll!(retry.as_mut()).is_pending());
    drop(running.pop());
    drop(running.pop());
    let std::task::Poll::Ready(retry) = futures_util::poll!(retry.as_mut()) else {
        panic!("an eligible retry must precede a weighted limited first attempt");
    };
    assert_eq!(retry.slots(), 2);
    assert!(futures_util::poll!(first.as_mut()).is_pending());
    drop(retry);
    drop(running.pop());
    drop(running.pop());
    let std::task::Poll::Ready(first) = futures_util::poll!(first.as_mut()) else {
        panic!("the weighted limited attempt must take all four free slots");
    };
    assert_eq!(first.slots(), 4);
    // Leave enough slots free so only the feature permit can keep the next attempt queued.
    for _ in 0..4 {
        drop(running.pop());
    }
    let mut same_feature = Box::pin(slots.admit_with(
        FeatureLimit::WasmStateReset,
        ScenarioLength::Ordinary,
        weight(&["slots_4"]),
        "another attempt of the limited feature",
        ScenarioRun::FIRST,
        |_| {},
    ));
    assert!(futures_util::poll!(same_feature.as_mut()).is_pending());
    drop(first);
    assert!(futures_util::poll!(same_feature.as_mut()).is_ready());
}
