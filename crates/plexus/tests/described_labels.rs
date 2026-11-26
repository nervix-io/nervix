//! Every Model description is held to its entity's labels by one public scenario.
//!
//! Layer: test harness, outside the product layer order.
//! - **Owns.** The check that the label description scenario lists exactly the Model kinds the
//!   grammar can describe.
//! - **Depends on.** The production Plexus grammar, the Model kinds and the scenario's feature
//!   file.
//! - **Must not know.** How a description is rendered, or what the scenario asserts beyond the
//!   statements its table lists.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test harness stays outside product execution")
)]

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use nervix_models::ModelKind;
use nervix_plexus::statement::parse_statement;
use strum::IntoEnumIterator as _;

/// The feature that proves descriptions show labels, relative to the workspace.
const FEATURE: &str = "tests/features/runtime/entity_labels.feature";

/// The step whose table lists one description per row.
const STEP: &str = "Then every one of these descriptions shows its entity's labels";

fn workspace() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let Some(workspace) = manifest.parent().and_then(Path::parent) else {
        panic!("the language crate lives two levels below the workspace root");
    };
    workspace.to_path_buf()
}

/// The description heads the grammar accepts for a Model kind, such as `DESCRIBE HASH MAP`. The
/// grammar decides which kinds have a description, so no list of kinds is kept here.
fn described_heads() -> BTreeSet<String> {
    let mut heads = BTreeSet::new();
    for kind in ModelKind::iter() {
        let head = format!("DESCRIBE {}", kind.keyword_phrase());
        if parse_statement(&format!("{head} described_entity;")).is_ok() {
            heads.insert(head);
        }
    }
    heads
}

/// The description head of every row of the scenario's table: each statement without the name
/// of the entity it describes.
fn scenario_heads(feature: &str) -> BTreeSet<String> {
    let mut heads = BTreeSet::new();
    let mut lines = feature.lines();
    for line in lines.by_ref() {
        if line.trim() == STEP {
            break;
        }
    }
    // The first row names the table's columns.
    let mut rows = lines.skip(1);
    for row in rows.by_ref() {
        let Some(cells) = row.trim().strip_prefix('|') else {
            break;
        };
        let Some((statement, _)) = cells.split_once('|') else {
            panic!("a row of the description table has two cells: {row}");
        };
        let Some((head, _)) = statement.trim().rsplit_once(' ') else {
            panic!("a description names its kind and its entity: {row}");
        };
        heads.insert(head.to_owned());
    }
    heads
}

#[test]
fn the_label_description_scenario_lists_every_model_kind_the_grammar_describes() {
    let path = workspace().join(FEATURE);
    let feature = match fs::read_to_string(&path) {
        Ok(feature) => feature,
        Err(error) => panic!("failed to read {}: {error}", path.display()),
    };
    let described = described_heads();
    assert!(
        described.contains("DESCRIBE RELAY"),
        "the probe statement no longer parses for a kind that has a description: {described:?}"
    );
    assert_eq!(
        scenario_heads(&feature),
        described,
        "the table under {STEP:?} in {FEATURE} lists one description for every Model kind the \
         grammar can describe, and nothing else"
    );
}
