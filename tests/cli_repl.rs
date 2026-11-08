//! Public interactive CLI output while the operator leaves a line unfinished.
//!
//! Layer: test harness.
//! - **Owns.** Typing unsubmitted text and editing keys into the scenario's terminal.
//! - **Depends on.** The public CLI terminal fixture and scenario placeholders.
//! - **Must not know.** The line editor's buffer or event queue implementation.

use cucumber::when;

use super::*;

#[when(expr = "the CLI REPL types {string} without pressing Enter")]
async fn type_without_submitting(world: &mut ScenarioWorld, text: String) {
    let text = expand_placeholders(world, &text);
    let terminal = world
        .cli_terminal
        .as_ref()
        .verified("a preceding step started the CLI REPL");
    if let Err(error) = terminal.type_text(&text).await {
        panic!(
            "the CLI REPL cannot type {text:?}: {error}; it displayed:{}",
            terminal.transcript()
        );
    }
}

#[when(expr = "the CLI REPL presses {string}")]
async fn press_editing_key(world: &mut ScenarioWorld, key: String) {
    let keys = match key.as_str() {
        "Tab" => "\t",
        "Enter" => "\r",
        "Ctrl-R" => "\u{12}",
        _ => panic!("the scenario names an unsupported editing key: {key}"),
    };
    let terminal = world
        .cli_terminal
        .as_ref()
        .verified("a preceding step started the CLI REPL");
    if let Err(error) = terminal.type_text(keys).await {
        panic!(
            "the CLI REPL cannot press {key}: {error}; it displayed:{}",
            terminal.transcript()
        );
    }
}
