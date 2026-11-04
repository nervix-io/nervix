//! Parsing, canonical round trips and grammar-local completion of cluster administration.

use std::time::Duration;

use meticulous::ResultExt as _;
use nervix_models::{ClusterParameterKey, ClusterParameterValueKind, Statement};
use strum::IntoEnumIterator as _;

use crate::statement::{parse_statement, suggest_statement};

#[test]
fn every_parameter_parses_and_renders_its_semantic_literal_family() {
    for parameter in ClusterParameterKey::iter() {
        let value = match parameter.value_kind() {
            ClusterParameterValueKind::Duration => "250ms",
            ClusterParameterValueKind::Count => "7",
            ClusterParameterValueKind::Bytes => "1KiB",
        };
        let input = format!("ALTER CLUSTER SET {} = {value};", parameter.keyword());
        let statement =
            parse_statement(&input).assured("every declared parameter has its own grammar");
        let rendered = statement
            .to_canonical_plexus()
            .assured("cluster parameters have canonical Plexus");
        assert_eq!(
            parse_statement(&rendered).assured("canonical Plexus parses"),
            statement
        );
    }
}

#[test]
fn several_parameters_keep_order_and_lower_duration_values_once() {
    let statement =
        parse_statement("aLtEr cLuStEr SeT node_unavailability_timeout = 15s, replica_count = 2;")
            .assured("language keywords are case insensitive");
    let Statement::AlterCluster(alter) = statement else {
        panic!("the grammar must return cluster administration");
    };
    let expected = vec![
        nervix_models::ClusterParameterChange::NodeUnavailabilityTimeout(Duration::from_secs(15)),
        nervix_models::ClusterParameterChange::ReplicaCount(2),
    ];
    assert_eq!(
        alter.parameters.iter().cloned().collect::<Vec<_>>(),
        expected
    );
}

#[test]
fn malformed_assignments_do_not_escape_into_another_statement_family() {
    for input in [
        "ALTER CLUSTER SET;",
        "ALTER CLUSTER SET REPLICA_COUNT =;",
        "ALTER CLUSTER SET REPLICA_COUNT = -1;",
        "ALTER CLUSTER SET REPLICA_COUNT = 18446744073709551616;",
        "ALTER CLUSTER SET STATE_SNAPSHOT_INTERVAL = 4messages;",
        "ALTER CLUSTER SET UDF_CODE_BYTES = 5s;",
        "ALTER CLUSTER SET RAFT_HEARTBEAT_INTERVAL = 1s;",
        "ALTER CLUSTER SET made_up_parameter = 1;",
        "ALTER DOMAIN SET REPLICA_COUNT = 2;",
    ] {
        assert!(parse_statement(input).is_err(), "{input}");
    }
}

#[test]
fn completion_offers_the_composed_command_then_only_parameter_keys() {
    let root = suggest_statement("", 0);
    assert!(root.contains(&"ALTER CLUSTER SET".to_string()), "{root:?}");
    let prefix = "ALTER CLUSTER SET ";
    let suggestions = suggest_statement(prefix, prefix.len());
    for parameter in ClusterParameterKey::iter() {
        assert!(
            suggestions.contains(&parameter.keyword()),
            "{parameter}: {suggestions:?}"
        );
    }
    assert!(!suggestions.contains(&"SCHEMA".to_string()));
    let prefix = "ALTER CLUSTER SET STATE_SNAPSHOT_INTERVAL = ";
    let suggestions = suggest_statement(prefix, prefix.len());
    assert!(
        suggestions.contains(&"duration_literal".to_string()),
        "{suggestions:?}"
    );
    assert!(!suggestions.contains(&"SCHEMA".to_string()));
}
