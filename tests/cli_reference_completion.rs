//! The completions the public CLI offers at the end of each of several statement prefixes.
//!
//! Layer: test harness.
//! - **Owns.** Completing a table of statement prefixes through the public CLI and reporting every
//!   prefix whose offers differ, so one run shows each position that disagrees.
//! - **Depends on.** The scenario's CLI suggestion request, its placeholders and its cluster.
//! - **Must not know.** The grammar, the completion resolvers or the forms' choices.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests own public completion witnesses")
)]

use super::*;

#[then(expr = "the CLI completion after each input on node {string} offers exactly")]
async fn completion_after_each_input_offers_exactly(
    world: &mut ScenarioWorld,
    node: String,
    #[step] step: &Step,
) {
    let node = expand_placeholders(world, &node);
    let table = step
        .table
        .as_ref()
        .assured("the step lists its inputs and their offers in a table");
    let mut differences = Vec::new();
    for row in table.rows.iter().skip(1) {
        let [input, offers] = row.as_slice() else {
            panic!("each completion row names an input and its offers: {row:?}");
        };
        // A table trims its cells, so the space that ends the input's last word is added back and
        // completion is asked where the next word starts.
        let input = format!("{} ", expand_placeholders(world, input));
        let output = cli_suggestions(world, &input, input.len(), &node).await;
        let offered = offered_suggestion_values(&output);
        let mut expected = BTreeSet::new();
        for offer in offers.split(", ") {
            if !offer.is_empty() {
                expected.insert(offer.to_string());
            }
        }
        if offered != expected {
            differences.push(format!(
                "{input}| offered {offered:?}, expected {expected:?}"
            ));
        }
    }
    assert!(
        differences.is_empty(),
        "completion offered other candidates:\n{}",
        differences.join("\n")
    );
}
