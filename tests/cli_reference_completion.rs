//! The completions the public CLI offers at the end of each of several statement prefixes.
//!
//! Layer: test harness.
//! - **Owns.** Completing a table of statement prefixes through the public CLI, where the next word
//!   starts or within the word that ends each prefix, and reporting every prefix whose offers
//!   differ, so one run shows each position that disagrees.
//! - **Depends on.** The scenario's CLI suggestion request, its placeholders and its cluster.
//! - **Must not know.** The grammar, the completion resolvers or the forms' choices.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "tests own public completion witnesses")
)]

use super::*;

/// Where completion is asked in each input of a table.
#[derive(Clone, Copy)]
enum CompletionPosition {
    /// After the input's last word, where the next word starts.
    NextWord,
    /// Within the word that ends the input, which completion replaces.
    LastWord,
}

impl CompletionPosition {
    /// The text whose end completion is asked at, for a row's `input`.
    ///
    /// A table trims its cells, so the space that ends the input's last word is added back where
    /// completion is asked for the next word.
    fn text(self, input: String) -> String {
        match self {
            Self::NextWord => format!("{input} "),
            Self::LastWord => input,
        }
    }
}

#[then(expr = "the CLI completion after each input on node {string} offers exactly")]
async fn completion_after_each_input_offers_exactly(
    world: &mut ScenarioWorld,
    node: String,
    #[step] step: &Step,
) {
    each_input_offers_exactly(world, node, step, CompletionPosition::NextWord).await;
}

#[then(expr = "the CLI completion of the word ending each input on node {string} offers exactly")]
async fn completion_of_the_word_ending_each_input_offers_exactly(
    world: &mut ScenarioWorld,
    node: String,
    #[step] step: &Step,
) {
    each_input_offers_exactly(world, node, step, CompletionPosition::LastWord).await;
}

/// Asks the CLI for the completions at `position` in each input of the step's table, and fails
/// with every input whose offers differ from the row's.
async fn each_input_offers_exactly(
    world: &ScenarioWorld,
    node: String,
    step: &Step,
    position: CompletionPosition,
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
        let input = position.text(expand_placeholders(world, input));
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
