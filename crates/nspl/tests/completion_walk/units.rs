//! Tests for the completion walker's own machinery.
//!
//! The walker itself is a `harness = false` target so it can print a report and take CLI
//! arguments, and cargo does not collect `#[test]` functions in such a target. These tests
//! therefore live in a second, harnessed target over the same modules: if the oracle, the label
//! tables or the deduplication are wrong, the report they produce is worthless.

#[path = "grammar.rs"]
mod grammar;
#[path = "report.rs"]
mod report;
#[path = "suggestion.rs"]
mod suggestion;
#[path = "walker.rs"]
mod walker;

use nervix_nspl::{Token, Word, lex};

use crate::{
    grammar::{Grammar, ParseOutcome},
    report::{Baseline, Finding, FindingKind, Report},
    suggestion::{MaterializeError, Materializer, SuggestionClass},
    walker::{Budget, WalkState, Walker},
};

fn budget() -> Budget {
    Budget {
        max_depth: 6,
        max_label_repeats: 2,
        max_states: 400,
        max_frontier: 400,
        signature_window: 2,
    }
}

fn finding(kind: FindingKind, label: &str, path: &[&str]) -> Finding {
    Finding::new(
        kind,
        Some(label.to_string()),
        "CREATE CODEC nx_codec VERSION".to_string(),
        path.iter().map(|label| (*label).to_string()).collect(),
        Vec::new(),
        String::new(),
    )
}

// -- the parse oracle --

#[test]
fn a_complete_statement_is_accepted() {
    assert_eq!(
        Grammar::Server.parse("CREATE DOMAIN nx_domain"),
        ParseOutcome::Accepted
    );
}

#[test]
fn a_bare_leading_keyword_is_incomplete_not_rejected() {
    // Alternatives that never matched CREATE fail at token 0, but the ones that consumed it fail at
    // end of input, and only the furthest error decides the outcome.
    assert!(matches!(
        Grammar::Server.parse("CREATE"),
        ParseOutcome::Incomplete { .. }
    ));
}

#[test]
fn empty_input_is_incomplete() {
    assert!(matches!(
        Grammar::Server.parse(""),
        ParseOutcome::Incomplete { .. }
    ));
}

#[test]
fn text_the_parser_cannot_consume_is_rejected_at_the_insertion_point() {
    // A comma cannot be a domain name, whereas a keyword can: `word_raw` accepts known words too,
    // so `CREATE DOMAIN BY` parses and would prove nothing here.
    let base = "CREATE DOMAIN";
    let input = format!("{base} ,");
    let outcome = Grammar::Server.parse(&input);
    assert!(
        matches!(outcome, ParseOutcome::Rejected { at, .. } if at == base.len() + 1),
        "expected rejection at the inserted token, got {outcome:?}"
    );
}

#[test]
fn every_diagnostic_is_available_for_triage_not_just_the_furthest() {
    // `--inspect` shows all of them: the furthest decides the verdict, but the rest are what
    // explain why a position offers what it offers.
    let diagnostics = Grammar::Server.diagnostics("CREATE DOMAIN ,");
    assert!(!diagnostics.is_empty());
    assert!(Grammar::Server.diagnostics("CREATE DOMAIN d").is_empty());
}

#[test]
fn the_top_level_suggestion_set_is_probed_not_empty() {
    assert!(!Grammar::Server.suggest("").is_empty());
    assert!(!Grammar::Client.suggest("").is_empty());
}

// -- label classification and materialization --

#[test]
fn classify_covers_every_label_shape() {
    assert_eq!(
        SuggestionClass::classify("CREATE"),
        Some(SuggestionClass::Keyword)
    );
    assert_eq!(
        SuggestionClass::classify("BRANCHED BY"),
        Some(SuggestionClass::Keyword)
    );
    assert_eq!(
        SuggestionClass::classify("ROTO_0_11"),
        Some(SuggestionClass::Keyword)
    );
    assert_eq!(
        SuggestionClass::classify(";"),
        Some(SuggestionClass::Punctuation)
    );
    assert_eq!(
        SuggestionClass::classify(">="),
        Some(SuggestionClass::Punctuation)
    );
    assert_eq!(
        SuggestionClass::classify("ref:relay"),
        Some(SuggestionClass::Reference)
    );
    assert_eq!(
        SuggestionClass::classify("field_name"),
        Some(SuggestionClass::Placeholder)
    );
    assert_eq!(SuggestionClass::classify(""), None);
}

#[test]
fn repeated_names_are_distinct_so_a_statement_does_not_name_two_things_alike() {
    let materializer = Materializer::new();
    assert_eq!(
        materializer
            .materialize("column_name", 0)
            .expect("first")
            .text,
        "'nx_column'"
    );
    assert_eq!(
        materializer
            .materialize("field_name", 0)
            .expect("first")
            .text,
        "nx_field"
    );
    assert_eq!(
        materializer
            .materialize("field_name", 1)
            .expect("second")
            .text,
        "nx_field_2"
    );
}

#[test]
fn references_and_names_become_synthetic_identifiers() {
    let materializer = Materializer::default();
    assert_eq!(
        materializer
            .materialize("ref:relay", 0)
            .expect("reference materializes")
            .text,
        "nx_relay"
    );
    assert_eq!(
        materializer
            .materialize("session_subscription_name", 0)
            .expect("name materializes")
            .text,
        "nx_session_subscription"
    );
}

#[test]
fn an_unregistered_placeholder_is_reported_not_guessed() {
    assert_eq!(
        Materializer::new().materialize("some_new_label", 0),
        Err(MaterializeError::UnfilledPlaceholder)
    );
}

#[test]
fn expression_regions_are_ordinary_labelled_placeholders() {
    // Every token-swallowing region names itself, so the walker fills it like any other
    // placeholder rather than guessing when a keyword opened one.
    let materializer = Materializer::new();

    let assignments = materializer
        .materialize("set_assignments", 0)
        .expect("set_assignments materializes");
    assert_eq!(assignments.text, "nx_out = 1");
    assert!(assignments.free_form);

    let set = materializer
        .materialize("SET", 0)
        .expect("SET materializes");
    assert_eq!(set.text, "SET");
    assert!(!set.free_form);
}

#[test]
fn synthetic_names_never_lex_as_keywords() {
    // A synthesized name that happened to be a keyword would silently steer the parse down another
    // branch, so every name the walker can produce must lex as a single unknown word.
    let materializer = Materializer::new();

    for stem in [
        "relay",
        "schema",
        "field",
        "session_subscription",
        "udf",
        "consumer_group",
        "queue_group",
        "mqtt_topic_filter",
    ] {
        let label = if stem.contains("group") || stem.contains("filter") {
            stem.to_string()
        } else {
            format!("{stem}_name")
        };
        let text = materializer
            .materialize(&label, 0)
            .unwrap_or_else(|error| panic!("{label} must materialize: {error}"))
            .text;
        let tokens = lex(&text).expect("synthetic name must lex");
        assert!(
            matches!(
                tokens.as_slice(),
                [only] if matches!(&only.token, Token::Word(Word::UnknownWord(_)))
            ),
            "synthetic name {text} must lex as one unknown word, got {tokens:?}"
        );
    }
}

#[test]
fn every_registered_filler_lexes() {
    let materializer = Materializer::new();
    for label in [
        "alter_operation_separator",
        "batch_size",
        "byte_size_literal",
        "column_name",
        "config_key",
        "config_value",
        "duration_literal",
        "hostname",
        "iceberg_location",
        "message_count",
        "node_id",
        "number_literal",
        "relay_capacity",
        "string_literal",
        "time_rate",
        "timestamp",
        "deduplicate_on",
        "reorder_by",
        "value_expression",
    ] {
        let text = materializer
            .materialize(label, 0)
            .unwrap_or_else(|error| panic!("{label} must materialize: {error}"))
            .text;
        lex(&text).unwrap_or_else(|error| panic!("filler for {label} must lex, got {error:?}"));
    }
}

// -- walk state --

#[test]
fn extending_joins_with_one_space() {
    let materializer = Materializer::new();
    let root = WalkState::seed(String::new());

    let first = root.extend(
        "CREATE",
        &materializer.materialize("CREATE", 0).expect("keyword"),
    );
    assert_eq!(first.input, "CREATE");

    let second = first.extend(
        "ref:schema",
        &materializer
            .materialize("ref:schema", 0)
            .expect("reference"),
    );
    assert_eq!(second.input, "CREATE nx_schema");
    assert_eq!(
        second.path,
        vec!["CREATE".to_string(), "ref:schema".to_string()]
    );
}

fn state_with_path(path: &[&str]) -> WalkState {
    let mut state = WalkState::seed(String::new());
    state.path = path.iter().map(|label| (*label).to_string()).collect();
    state
}

#[test]
fn the_position_signature_ignores_history_beyond_the_window() {
    let suggestions = vec!["ALPHA".to_string(), "BETA".to_string()];
    let one = state_with_path(&["CREATE", "RELAY", "P", "C", "D"]);
    let other = state_with_path(&["CREATE", "RELAY", "Q", "C", "D"]);

    assert_eq!(
        one.position_signature(&suggestions, 2),
        other.position_signature(&suggestions, 2)
    );
    assert_ne!(
        one.position_signature(&suggestions, 4),
        other.position_signature(&suggestions, 4)
    );
}

#[test]
fn the_position_signature_separates_statement_shapes() {
    // Without this, `TO ref:relay` in a junction and in an emitter collapse together and only one
    // family gets explored.
    let suggestions = vec!["ALPHA".to_string()];
    let junction = state_with_path(&["CREATE", "JUNCTION", "TO", "ref:relay"]);
    let emitter = state_with_path(&["CREATE", "EMITTER", "TO", "ref:relay"]);

    assert_ne!(
        junction.position_signature(&suggestions, 2),
        emitter.position_signature(&suggestions, 2)
    );
}

#[test]
fn the_position_signature_separates_paths_of_different_length() {
    // The walk is breadth-first, so without depth in the key the shorter path claims the position
    // and the longer one — the one carrying the clause needed to finish — is never expanded.
    let suggestions = vec!["ALPHA".to_string()];
    let short = state_with_path(&["CREATE", "EMITTER", "TO", "ref:client"]);
    let long = state_with_path(&[
        "CREATE",
        "EMITTER",
        "ENCODE USING",
        "ref:codec",
        "TO",
        "ref:client",
    ]);

    assert_ne!(
        short.position_signature(&suggestions, 2),
        long.position_signature(&suggestions, 2)
    );
}

// -- findings, deduplication and baseline --

#[test]
fn signature_ignores_the_reproduction_text() {
    let first = finding(
        FindingKind::DeadEnd,
        "VERSION",
        &["CREATE", "CODEC", "VERSION"],
    );
    let mut second = finding(
        FindingKind::DeadEnd,
        "VERSION",
        &["CREATE", "CODEC", "VERSION"],
    );
    second.statement = "CREATE WASM PROCESSOR nx_wasm VERSION".to_string();

    assert_eq!(first.signature(), second.signature());
}

#[test]
fn context_excludes_the_offending_label_itself() {
    let finding = finding(
        FindingKind::DeadEnd,
        "VERSION",
        &["CREATE", "CODEC", "VERSION"],
    );
    assert_eq!(
        finding.context,
        vec!["CREATE".to_string(), "CODEC".to_string()]
    );
}

#[test]
fn duplicates_collapse_to_one_unique_finding() {
    let mut report = Report::default();
    report.push(finding(
        FindingKind::DeadEnd,
        "VERSION",
        &["CREATE", "CODEC", "VERSION"],
    ));
    report.push(finding(
        FindingKind::DeadEnd,
        "VERSION",
        &["CREATE", "CODEC", "VERSION"],
    ));
    report.push(finding(
        FindingKind::DeadEnd,
        "WIDTH",
        &["CREATE", "WINDOW", "WIDTH"],
    ));

    assert_eq!(report.unique_findings().len(), 2);
}

#[test]
fn a_missing_baseline_reports_everything_and_a_stored_one_reports_nothing() {
    let mut report = Report::default();
    report.push(finding(
        FindingKind::DeadEnd,
        "VERSION",
        &["CREATE", "CODEC", "VERSION"],
    ));
    report.push(finding(
        FindingKind::DeadEnd,
        "WIDTH",
        &["CREATE", "WINDOW", "WIDTH"],
    ));

    let path = std::env::temp_dir().join(format!(
        "nspl-completion-walk-baseline-{}.txt",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);

    let missing = Baseline::load(&path).expect("a missing baseline loads as empty");
    assert_eq!(missing.new_findings(&report).len(), 2);

    Baseline::store(&path, &report).expect("baseline stores");
    let stored = Baseline::load(&path).expect("stored baseline loads");
    assert!(stored.new_findings(&report).is_empty());
    assert!(stored.stale(&report).is_empty());

    // A baseline entry the walk stopped producing is surfaced rather than silently kept.
    assert_eq!(stored.stale(&Report::default()).len(), 2);

    std::fs::remove_file(&path).expect("temporary baseline is removed");
}

// -- the walk end to end --

#[test]
fn the_report_does_not_depend_on_how_many_workers_ran() {
    let single = Walker::new(Grammar::Server, budget(), 1).run("");
    let many = Walker::new(Grammar::Server, budget(), 8).run("");

    assert_eq!(single.render(), many.render());
    assert!(single.stats.states_evaluated > 1);
}

#[test]
fn the_walk_reaches_complete_statements() {
    let report = Walker::new(Grammar::Server, budget(), 4).run("");
    assert!(
        report.stats.completed_statements > 0,
        "expected the walk to complete at least one statement:\n{}",
        report.render()
    );
}

#[test]
fn completion_never_goes_silent_on_a_reachable_branch() {
    // A dead end is a position that parses as an incomplete statement yet offers nothing: the user
    // has typed something valid and completion has abandoned them. This is the walk's whole point,
    // so it is asserted directly rather than left to the baseline.
    for seed in [
        "",
        "CREATE CODEC nx_codec",
        "CREATE SCHEMA nx_schema (",
        "CREATE INGESTOR nx_ingestor FROM MQTT nx_client",
    ] {
        let report = Walker::new(Grammar::Server, budget(), 4).run(seed);
        let dead_ends = report
            .unique_findings()
            .into_iter()
            .filter(|finding| finding.kind == FindingKind::DeadEnd)
            .collect::<Vec<_>>();
        assert!(
            dead_ends.is_empty(),
            "completion went silent below {seed:?}: {:?}",
            dead_ends
                .iter()
                .map(|finding| &finding.statement)
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn scratch_probe() {
    let input = "CREATE INFERENCER nx_i FROM nx_r USING RESOURCE nx_res VERSION ";
    println!(
        "family: {:?}",
        nervix_nspl::inferencer::suggest_create_inferencer(input, input.len())
    );
    let wasm = "CREATE WASM PROCESSOR nx_w FROM nx_r USING RESOURCE nx_res VERSION ";
    println!(
        "wasm family: {:?}",
        nervix_nspl::wasm_processor::suggest_create_wasm_processor(wasm, wasm.len())
    );
}
