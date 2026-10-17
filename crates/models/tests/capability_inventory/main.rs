//! Every capability a connector entity declares is observed at run time by a registered scenario.
//!
//! Layer: test harness.
//!
//! - **Owns.** The check that holds `tests/connector-capability-inventory.toml` to the entities'
//!   declarations and to the scenario suite: every capability an entity declares names at least
//!   one scenario, every entry names a declared capability of a declared entity once, and every
//!   scenario an entry names exists.
//! - **Depends on.** The vocabulary's declarations, the inventory and the feature files.
//! - **Must not know.** What a scenario does beyond its feature file and its name.
//!
//! `just validate-connector-capabilities` runs this check, and `just validate` runs it with the
//! other validations.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use nervix_models::{EmitSinkKind, IngestSourceKind};
use serde::Deserialize;

/// The inventory, as `tests/connector-capability-inventory.toml` writes it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    capability: Vec<Entry>,
}

/// The scenarios that observe one capability of one entity.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    /// The entity, as its statement's head names it: `KAFKA TOPIC INGESTOR`.
    entity: String,
    /// The capability, as its statement writes it: `MODE NO ACK`, `OFFSET BY DOMAIN`.
    capability: String,
    scenarios: Vec<ScenarioReference>,
}

/// One scenario, by the feature file that holds it and its name.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScenarioReference {
    feature: String,
    scenario: String,
}

/// One capability of one entity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Declared {
    entity: String,
    capability: String,
}

fn workspace() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let Some(workspace) = manifest.parent().and_then(Path::parent) else {
        panic!("the vocabulary crate lives two levels below the workspace root");
    };
    workspace.to_path_buf()
}

fn inventory() -> Inventory {
    let path = workspace().join("tests/connector-capability-inventory.toml");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
    toml::from_str(&text)
        .unwrap_or_else(|error| panic!("{} is not a valid inventory: {error}", path.display()))
}

/// Every capability every entity declares.
fn declared() -> BTreeSet<Declared> {
    let mut declared = BTreeSet::new();
    for kind in IngestSourceKind::all() {
        for capability in kind.capabilities().runtime_capabilities() {
            declared.insert(Declared {
                entity: format!("{} INGESTOR", kind.phrase()),
                capability: capability.to_string(),
            });
        }
    }
    for kind in EmitSinkKind::all() {
        for capability in kind.capabilities().runtime_capabilities() {
            declared.insert(Declared {
                entity: format!("{} EMITTER", kind.phrase()),
                capability: capability.to_string(),
            });
        }
    }
    declared
}

/// The names of the scenarios the feature file at `feature`, relative to the workspace, holds,
/// or `None` where there is no such file.
fn feature_scenarios(feature: &str) -> Option<BTreeSet<String>> {
    match fs::read_to_string(workspace().join(feature)) {
        Ok(text) => Some(scenario_names(&text)),
        Err(_) => None,
    }
}

/// The names of the scenarios a feature file holds.
fn scenario_names(text: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for line in text.lines() {
        let line = line.trim();
        let name = line
            .strip_prefix("Scenario Outline:")
            .or_else(|| line.strip_prefix("Scenario:"));
        if let Some(name) = name {
            names.insert(name.trim().to_owned());
        }
    }
    names
}

#[test]
fn every_declared_capability_is_observed_by_a_registered_scenario_that_exists() {
    let inventory = inventory();
    let declared = declared();
    let mut problems = Vec::new();
    let mut registered = BTreeSet::new();
    let mut features: BTreeMap<String, Option<BTreeSet<String>>> = BTreeMap::new();
    for entry in &inventory.capability {
        let key = Declared {
            entity: entry.entity.clone(),
            capability: entry.capability.clone(),
        };
        if !declared.contains(&key) {
            problems.push(format!(
                "{} `{}` is registered, but no entity declares it",
                entry.entity, entry.capability
            ));
        }
        if entry.scenarios.is_empty() {
            problems.push(format!(
                "{} `{}` names no scenario",
                entry.entity, entry.capability
            ));
        }
        for scenario in &entry.scenarios {
            let names = features
                .entry(scenario.feature.clone())
                .or_insert_with(|| feature_scenarios(&scenario.feature));
            let exists = match names {
                Some(names) => names.contains(&scenario.scenario),
                None => false,
            };
            if !exists {
                problems.push(format!(
                    "{} `{}` names `{}` in {}, which does not exist",
                    entry.entity, entry.capability, scenario.scenario, scenario.feature
                ));
            }
        }
        if !registered.insert(key) {
            problems.push(format!(
                "{} `{}` is registered twice",
                entry.entity, entry.capability
            ));
        }
    }
    for capability in declared.difference(&registered) {
        problems.push(format!(
            "{} declares `{}`, but no scenario that observes it is registered",
            capability.entity, capability.capability
        ));
    }
    assert!(
        problems.is_empty(),
        "tests/connector-capability-inventory.toml does not match the declarations and the \
         scenario suite:\n{}",
        problems.join("\n")
    );
}
