//! Declaration-backed coverage of domain entity metadata grammar and canonical text.
//!
//! Layer: test harness.
//! - **Owns.** Complete current Model metadata grammar and completion conformance.
//! - **Depends on.** The composed grammar, canonical rendering and exhaustive Model generator.
//! - **Must not know.** Registry storage, schedules, network drivers or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns metadata language conformance")
)]

use std::collections::BTreeSet;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_arbitrary::{Arbitrary, Domain, ModelVariant};
use nervix_models::{EntityLabels, ModelKind, Statement};
use strum::IntoEnumIterator as _;

use crate::{
    conformance_support::{assert_reads_back_canonically, created_model},
    statement::{parse_statement, suggest_statement},
};

fn metadata() -> EntityLabels {
    serde_json::from_str(r#"{"empty":"","owner":"Mixed Case / Δ","team/path":"'quoted'\nvalue"}"#)
        .assured("the fixture has valid unique keys and exact UTF-8 values")
}

#[test]
fn metadata_inspection_reads_beside_the_transaction_queue() {
    let statement = parse_statement("SHOW LABELS ON SCHEMA events;")
        .assured("the metadata inspection is valid");
    let statement = crate::client_statement::ClientStatement::Server(statement);
    assert!(statement.reads_transaction_state());
}

#[test]
fn every_current_model_has_the_same_trailing_metadata_and_typed_address() {
    let mut seen = BTreeSet::new();
    for variant in ModelVariant::iter() {
        // The existing exhaustive Model generator selects the shape. There is no label-specific
        // table of expected kinds to become stale when the vocabulary adds an entity.
        let bytes = [17; 4096];
        let mut model = Arbitrary::new(&bytes, Domain::Plexus).model_of(variant);
        *model.labels_mut() = metadata();
        let source = model
            .to_canonical_plexus()
            .assured("the Plexus domain is renderable");
        assert_eq!(
            created_model(&source),
            Some(model.clone()),
            "{variant:?}: {source}"
        );
        seen.insert(model.kind().as_str());
        let target = format!("{} {}", model.kind().keyword_phrase(), model.name());
        for source in [
            format!("SET LABELS ON {target} {{ 'owner' = 'Platform', 'empty' = '' }};"),
            format!("SET LABELS ON {target} {{ 'region' = 'east', 'owner' DROP, 'absent' DROP }};"),
            format!("SHOW LABELS ON {target};"),
            format!("SHOW LABELS ON {target} FORMAT JSON;"),
        ] {
            let statement = parse_statement(&source)
                .unwrap_or_else(|error| panic!("{variant:?}: {source}\n{error:?}"));
            assert_reads_back_canonically(&statement);
        }
        let mut without_labels = model.clone();
        *without_labels.labels_mut() = EntityLabels::default();
        let prefix = without_labels
            .to_canonical_plexus()
            .assured("empty labels preserve a complete creation statement");
        let prefix = format!("{} ", prefix.trim_end_matches(';'));
        assert!(
            suggest_statement(&prefix, prefix.len())
                .iter()
                .any(|item| item == "LABELS"),
            "{variant:?}: {prefix}"
        );
        assert!(
            parse_statement(&format!(
                "{source} LABELS {{ 'extra' = 'after semicolon' }};"
            ))
            .is_err()
        );
    }
    assert_eq!(seen, ModelKind::iter().map(ModelKind::as_str).collect());
}

#[test]
fn metadata_clause_has_one_position_and_duplicates_are_never_overwritten() {
    for source in [
        "CREATE SCHEMA event ( value U32 ) LABELS { 'owner' = 'one', 'owner' = 'one' };",
        "CREATE SCHEMA event ( value U32 ) LABELS { 'Uppercase' = 'one' };",
        "CREATE SCHEMA event ( value U32 ); LABELS { 'owner' = 'one' };",
        "CREATE SCHEMA event ( value U32 ) LABELS {} LABELS {};",
        "SET LABELS ON SCHEMA event { 'owner' = 'one', 'owner' = 'two' };",
        "SET LABELS ON SCHEMA event { 'owner' DROP, 'owner' DROP };",
        "SET LABELS ON SCHEMA event { 'owner' = 'one', 'owner' DROP };",
        "SET LABELS ON SCHEMA event { 'owner' };",
        "SET LABELS ON SCHEMA event { 'owner' DROP = 'one' };",
        "SET LABELS ON SCHEMA event { DROP 'owner' };",
        "SET LABELS ON SCHEMA event { 'Owner' DROP };",
        "SHOW LABELS ON DOMAIN;",
        "SHOW LABELS ON RESOURCE event VERSION 1;",
        "SHOW LABELS ON USER event LABELS {};",
    ] {
        assert!(parse_statement(source).is_err(), "{source}");
    }
    let parsed =
        parse_statement("create schema event ( value U32 ) labels { 'team/path' = ' Δ ' };")
            .assured("keywords ignore case while metadata spelling is exact");
    let Statement::Create(create) = parsed else {
        panic!("the source is a creation statement");
    };
    assert_eq!(create.body.labels().get("team/path"), Some(" Δ "));
}

#[test]
fn administrative_creations_and_catalog_targets_use_the_shared_grammar() {
    for (creation, target, name_label) in [
        ("CREATE DOMAIN sales", "DOMAIN sales", "ref:domain"),
        (
            "CREATE USER customer WITH PASSWORD 'test-password'",
            "USER customer",
            "user_name",
        ),
        (
            "CREATE RESOURCE catalog",
            "RESOURCE catalog",
            "ref:resource",
        ),
    ] {
        for source in [
            format!("{creation} LABELS {{ 'team/path' = ' Δ ' }};"),
            format!("SET LABELS ON {target} {{ 'team/path' = ' Δ ' }};"),
            format!("SET LABELS ON {target} {{ 'team/path' DROP }};"),
            format!("SHOW LABELS ON {target} FORMAT JSON;"),
        ] {
            let statement =
                parse_statement(&source).assured("the shared metadata grammar accepts this owner");
            assert_reads_back_canonically(&statement);
        }
        let creation_prefix = format!("{creation} ");
        assert!(
            suggest_statement(&creation_prefix, creation_prefix.len())
                .iter()
                .any(|item| item == "LABELS")
        );
        let kind = target
            .split_once(' ')
            .assured("every example has a typed name")
            .0;
        let prefix = format!("SHOW LABELS ON {kind} ");
        let suggestions = suggest_statement(&prefix, prefix.len());
        assert!(
            suggestions.iter().any(|item| item == name_label),
            "{prefix}: {suggestions:?}"
        );
    }
}

#[test]
fn label_commands_complete_composed_heads_and_existing_reference_kinds() {
    for (prefix, expected) in [("SET ", "LABELS ON"), ("SHOW ", "LABELS ON")] {
        let suggestions = suggest_statement(prefix, prefix.len());
        assert!(
            suggestions.iter().any(|item| item == expected),
            "{prefix:?} must offer the composed phrase {expected:?}: {suggestions:?}"
        );
    }
    for kind in ModelKind::iter() {
        let prefix = format!("SHOW LABELS ON {} ", kind.keyword_phrase());
        let suggestions = suggest_statement(&prefix, prefix.len());
        assert!(
            suggestions
                .iter()
                .any(|item| item == kind.completion_label()),
            "{prefix:?}: {suggestions:?}"
        );
    }
}

#[test]
fn server_and_client_completion_derive_metadata_tails_without_consuming_open_groups() {
    let client = crate::client_statement::ClientStatementGrammar::new();
    for (prefix, expected) in [
        ("CREATE SCHEMA event (value U32) ", "LABELS"),
        ("SHOW LABELS ON SCHEMA event ", "FORMAT"),
        ("CREATE SCHEMA event (value ", "U32"),
    ] {
        let server = suggest_statement(prefix, prefix.len());
        assert!(
            server.iter().any(|label| label == expected),
            "{prefix:?}: {server:?}"
        );
        assert_eq!(client.suggest_statement(prefix, prefix.len()), server);
    }
    let complete = "CREATE SCHEMA event (value U32) LABELS {} ";
    let suggestions = client.suggest_statement(complete, complete.len());
    assert!(
        suggestions.iter().all(|label| label != "LABELS"),
        "{suggestions:?}"
    );
    let terminated = "CREATE SCHEMA event (value U32); ";
    assert!(
        client
            .suggest_statement(terminated, terminated.len())
            .is_empty()
    );
}

#[test]
fn one_edit_sets_and_removes_keys_and_reads_back_in_key_order() {
    let statement = parse_statement("SET LABELS ON SCHEMA event { 'team' DROP, 'owner' = '' , };")
        .assured("an assignment and a removal form one edit");
    let Statement::MutateEntityLabels(edit) = &statement else {
        panic!("the source is a label edit: {statement:?}");
    };
    let mut changes = Vec::new();
    for (key, change) in edit.mutation.iter() {
        changes.push((key.as_str().to_owned(), change.clone()));
    }
    assert_eq!(
        changes,
        vec![
            (
                "owner".to_owned(),
                nervix_models::LabelChange::Set(String::new())
            ),
            ("team".to_owned(), nervix_models::LabelChange::Drop),
        ]
    );
    assert_eq!(
        statement
            .to_canonical_plexus()
            .assured("a label edit renders"),
        "SET LABELS ON SCHEMA event { 'owner' = '', 'team' DROP };"
    );
    assert_reads_back_canonically(&statement);
    let empty = parse_statement("SET LABELS ON SCHEMA event {};")
        .assured("an empty edit is a valid statement");
    assert_reads_back_canonically(&empty);
}

#[test]
fn an_edit_entry_completes_an_assignment_or_a_removal() {
    let client = crate::client_statement::ClientStatementGrammar::new();
    for (prefix, expected) in [
        ("SET LABELS ON SCHEMA event { 'owner' ", "="),
        ("SET LABELS ON SCHEMA event { 'owner' ", "DROP"),
        ("SET LABELS ON SCHEMA event { 'owner' DROP, 'team' ", "DROP"),
    ] {
        let server = suggest_statement(prefix, prefix.len());
        assert!(
            server.iter().any(|label| label == expected),
            "{prefix:?} must offer {expected:?}: {server:?}"
        );
        assert_eq!(client.suggest_statement(prefix, prefix.len()), server);
    }
    let removal = "SET LABELS ON SCHEMA event { 'owner' DROP ";
    let suggestions = suggest_statement(removal, removal.len());
    assert!(
        suggestions
            .iter()
            .all(|label| label != "=" && label != "DROP"),
        "a removal takes no value: {suggestions:?}"
    );
    let creation = "CREATE SCHEMA event (value U32) LABELS { 'owner' ";
    let suggestions = suggest_statement(creation, creation.len());
    assert!(
        suggestions.iter().all(|label| label != "DROP"),
        "a creation sets every label it names: {suggestions:?}"
    );
}
