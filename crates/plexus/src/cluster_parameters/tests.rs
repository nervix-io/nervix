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
            ClusterParameterValueKind::Duration => "13m 49s 1ns",
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

#[rstest::rstest]
#[case("13m 49s", Duration::from_secs(829))]
#[case("1h 2m 3s 4ms 5us 6ns", Duration::new(3_723, 4_005_006))]
#[case("1.5s 250ms", Duration::from_millis(1_750))]
#[case("1 min 30s", Duration::from_secs(90))]
#[case("0s 1ns", Duration::from_nanos(1))]
#[case(
    "4294967295s 999ms 999us 999ns",
    Duration::new(u64::from(u32::MAX), 999_999_999)
)]
fn multi_span_cluster_durations_parse_and_reparse_canonically(
    #[case] literal: &str,
    #[case] expected: Duration,
) {
    let input = format!("ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = {literal};");
    let statement = parse_statement(&input).assured("the duration has valid spans");
    let Statement::AlterCluster(alter) = &statement else {
        panic!("the statement must be cluster administration");
    };
    assert_eq!(
        alter.parameters.iter().cloned().collect::<Vec<_>>(),
        vec![nervix_models::ClusterParameterChange::DomainDrainTimeout(
            expected
        )]
    );
    let canonical = statement
        .to_canonical_plexus()
        .assured("the semantic duration has canonical Plexus");
    assert_eq!(
        parse_statement(&canonical).assured("canonical multi-span durations parse"),
        statement,
        "{canonical}"
    );
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
        "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m 49;",
        "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m 49messages;",
        "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m -49s;",
        "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m 49s trailing;",
        "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m 18446744073709551615years;",
        "ALTER CLUSTER SET UDF_CODE_BYTES = 5s;",
        "ALTER CLUSTER SET RAFT_HEARTBEAT_INTERVAL = 1s;",
        "ALTER CLUSTER SET made_up_parameter = 1;",
        "ALTER DOMAIN SET REPLICA_COUNT = 2;",
    ] {
        assert!(parse_statement(input).is_err(), "{input}");
    }
}

#[test]
fn completion_after_multi_span_durations_stays_in_cluster_assignments() {
    let prefix = "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m 49s, ";
    let suggestions = suggest_statement(prefix, prefix.len());
    for parameter in ClusterParameterKey::iter()
        .filter(|parameter| *parameter != ClusterParameterKey::DomainDrainTimeout)
    {
        assert!(
            suggestions.contains(&parameter.keyword()),
            "{suggestions:?}"
        );
    }
    for unrelated in ["SCHEMA", "FROM", "CREATE RELAY"] {
        assert!(
            !suggestions.contains(&unrelated.to_string()),
            "{suggestions:?}"
        );
    }
    let statement = parse_statement(&format!("{prefix}REPLICA_COUNT = 2;"))
        .assured("a distinct assignment follows the complete duration");
    let Statement::AlterCluster(alter) = statement else {
        panic!("the grammar must return cluster administration");
    };
    assert_eq!(
        alter.parameters.iter().cloned().collect::<Vec<_>>(),
        vec![
            nervix_models::ClusterParameterChange::DomainDrainTimeout(Duration::from_secs(829)),
            nervix_models::ClusterParameterChange::ReplicaCount(2),
        ]
    );
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
