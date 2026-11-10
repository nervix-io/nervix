//! Public inspection at the two controlled boundaries of a processor key's claim.
//!
//! Layer: test harness.
//! - **Owns.** Claim phase controls and exact per-partition inspection assertions.
//! - **Depends on.** The scenario fault injector and public typed inspection outcomes.
//! - **Must not know.** Runtime publications or checkpoint storage.

use cucumber::given;
use nervix_models::{DomainName, DomainNodeRef, ModelKind, ModelName};

use super::*;

fn processor(world: &ScenarioWorld, kind: &str) -> DomainNodeRef {
    let kind = match kind {
        "deduplicator" => ModelKind::Deduplicator,
        "window" => ModelKind::WindowProcessor,
        _ => panic!("the fixture names a supported processor"),
    };
    let domain = world
        .domain
        .parse::<DomainName>()
        .assured("the scenario creates a valid domain");
    let name = "remembered"
        .parse::<ModelName>()
        .assured("a valid fixture processor name");
    DomainNodeRef::node_in(domain, kind, name)
}

#[given(expr = "the {word} key claim is paused before install and before forget")]
fn arm(world: &mut ScenarioWorld, kind: String) {
    let processor = processor(world, &kind);
    world
        .fault_injection
        .pause_processor_partition_install(processor.clone());
    world
        .fault_injection
        .pause_processor_partition_forget(processor);
}

#[then(expr = "the {word} key claim reaches the {word} pause")]
async fn reached(world: &mut ScenarioWorld, kind: String, phase: String) {
    let processor = processor(world, &kind);
    let wait = async {
        match phase.as_str() {
            "install" => {
                world
                    .fault_injection
                    .wait_for_processor_partition_install(&processor)
                    .await
            }
            "forget" => {
                world
                    .fault_injection
                    .wait_for_processor_partition_forget(&processor)
                    .await
            }
            _ => panic!("the fixture names a supported claim phase"),
        }
    };
    nervix_primitives::time::timeout(Duration::from_secs(60), wait)
        .await
        .assured("the claim reaches its armed boundary");
}

#[when(expr = "the {word} key claim's {word} pause is released")]
fn release(world: &mut ScenarioWorld, kind: String, phase: String) {
    let processor = processor(world, &kind);
    match phase.as_str() {
        "install" => world
            .fault_injection
            .release_processor_partition_install(&processor),
        "forget" => world
            .fault_injection
            .release_processor_partition_forget(&processor),
        _ => panic!("the fixture names a supported claim phase"),
    }
}

#[then(expr = "the last processor state lists the moving branch under partition {int}")]
fn one_partition(world: &mut ScenarioWorld, partition: u32) {
    let StateInspectionResult::Page(page) = &last_state(world).result else {
        panic!("the inspection succeeded");
    };
    assert_eq!(
        page.counts.branches, 2,
        "the released checkpoint is not another branch"
    );
    let mut moving = std::collections::BTreeSet::new();
    for unit in &page.units {
        if unit.counts.branches == 0 {
            assert_eq!(unit.counts.rows, 0);
            assert_eq!(unit.counts.bytes, 0);
            continue;
        }
        assert_eq!(unit.counts.branches, 1);
        for section in &unit.sections {
            for row in displayed_rows(section) {
                if row.branch["tenant"] == "beta" {
                    moving.insert(unit.unit);
                }
            }
        }
    }
    assert_eq!(
        moving,
        std::collections::BTreeSet::from([nervix_models::Unit::partition(partition)])
    );
}

#[then(expr = "the last processor state preserves both tenants' input values")]
fn values(world: &mut ScenarioWorld) {
    let state = last_state(world);
    let (section, value_field) = match processor_kind(state) {
        ModelKind::Deduplicator => ("keys", "key_1"),
        ModelKind::WindowProcessor => ("rows", "seq"),
        _ => panic!("the fixture inspects deduplicator keys or retained windows"),
    };
    let StateInspectionResult::Page(page) = &state.result else {
        panic!("the inspection succeeded");
    };
    let mut observed = BTreeMap::<String, std::collections::BTreeSet<i64>>::new();
    let mut window_bytes = 0_u64;
    for unit in &page.units {
        for part in &unit.sections {
            if part.name == section {
                for row in displayed_rows(part) {
                    let tenant = row.branch["tenant"]
                        .as_str()
                        .assured("the fixture's tenant is a string")
                        .to_string();
                    let value = row.fields[value_field]
                        .as_i64()
                        .assured("the fixture's values are I64");
                    observed.entry(tenant).or_default().insert(value);
                }
            }
            if part.name == "header" {
                for row in displayed_rows(part) {
                    let bytes = row.fields["state_bytes"]
                        .as_u64()
                        .assured("window headers report their production byte charge");
                    window_bytes = window_bytes
                        .checked_add(bytes)
                        .assured("three fixture rows fit u64");
                }
            }
        }
    }
    assert_eq!(
        observed,
        BTreeMap::from([
            ("beta".to_string(), std::collections::BTreeSet::from([1, 2])),
            ("eta".to_string(), std::collections::BTreeSet::from([10])),
        ])
    );
    if let ModelKind::WindowProcessor = processor_kind(state) {
        assert_eq!(
            page.counts.bytes, window_bytes,
            "the retained holder copy adds no byte charge"
        );
    }
}
