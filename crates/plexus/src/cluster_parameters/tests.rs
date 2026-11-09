//! Parsing, canonical round trips and grammar-local completion of cluster administration.

use std::{collections::BTreeSet, time::Duration};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::{
    ClusterParameterChange, ClusterParameterError, ClusterParameterKey, ClusterParameterValueKind,
    Statement,
};
use strum::IntoEnumIterator as _;

use crate::{
    parser_support::ParseFromSourceError,
    statement::{parse_statement, suggest_statement},
};

/// A literal of the family `parameter` accepts.
fn literal(parameter: ClusterParameterKey) -> &'static str {
    match parameter.value_kind() {
        ClusterParameterValueKind::Duration => "13m 49s 1ns",
        ClusterParameterValueKind::Count => "7",
        ClusterParameterValueKind::Bytes => "1KiB",
        ClusterParameterValueKind::AlgorithmNames => "('fips')",
    }
}

#[test]
fn algorithm_lists_resolve_aliases_and_keep_first_occurrence_preference() {
    let statement = parse_statement(
        "ALTER CLUSTER SET TLS_CIPHER_SUITES = ('TLS13_CHACHA20_POLY1305_SHA256', 'FiPs', \
         'tls_aes_128_gcm_sha256'), TLS_KEY_EXCHANGE_GROUPS = ('X25519', 'secp256r1', 'x25519');",
    )
    .assured("declared names and aliases are case insensitive");
    let Statement::AlterCluster(alter) = &statement else {
        panic!("the parser returns cluster administration");
    };
    let changes: Vec<_> = alter.parameters.iter().collect();
    let ClusterParameterChange::TlsCipherSuites(suites) = changes[0] else {
        panic!("the first assignment names suites");
    };
    assert_eq!(
        suites
            .iter()
            .next()
            .assured("the selection is nonempty")
            .name(),
        "TLS13_CHACHA20_POLY1305_SHA256"
    );
    let ClusterParameterChange::TlsKeyExchangeGroups(groups) = changes[1] else {
        panic!("the second assignment names groups");
    };
    assert_eq!(groups.to_string(), "X25519,secp256r1");
    let canonical = statement
        .to_canonical_plexus()
        .assured("the semantic assignments render canonically");
    assert_eq!(
        parse_statement(&canonical).assured("complete canonical lists parse"),
        statement
    );
}

#[test]
fn unknown_algorithm_diagnostics_name_the_parameter_and_the_offending_item() {
    let error = parse_statement("ALTER CLUSTER SET TLS_CIPHER_SUITES = ('unknown_algorithm');")
        .expect_err("the catalog names no such algorithm");
    let diagnostics = error.current_context().diagnostics();
    assert_eq!(diagnostics.len(), 1);
    assert!(
        diagnostics[0].message.contains("tls-cipher-suites"),
        "{diagnostics:?}"
    );
    assert!(
        diagnostics[0].message.contains("unknown_algorithm"),
        "{diagnostics:?}"
    );
}

#[test]
fn algorithm_completion_offers_valid_values_in_only_the_selected_parameter() {
    let prefix = "ALTER CLUSTER SET TLS_CIPHER_SUITES = (";
    let offered = suggest_statement(prefix, prefix.len());
    assert!(
        offered
            .iter()
            .any(|value| value.contains("TLS13_AES_128_GCM_SHA256")),
        "{offered:?}"
    );
    assert!(
        offered.iter().any(|value| value.contains("fips")),
        "{offered:?}"
    );
    for value in offered {
        let complete = format!("{prefix}{value});");
        assert!(
            parse_statement(&complete).is_ok(),
            "completion must finish its own grammar: {complete}"
        );
    }
    let prefix = "ALTER CLUSTER SET TLS_KEY_EXCHANGE_GROUPS = ('secp256r1', ";
    let offered = suggest_statement(prefix, prefix.len());
    assert!(
        offered.iter().any(|value| value.contains("X25519")),
        "{offered:?}"
    );
    for value in offered {
        let complete = format!("{prefix}{value});");
        assert!(
            parse_statement(&complete).is_ok(),
            "completion must finish its own grammar: {complete}"
        );
    }
}

/// `KEY = <literal>` for each of `parameters`, in the order given, separated by commas.
fn assignments(parameters: impl IntoIterator<Item = ClusterParameterKey>) -> String {
    let written: Vec<String> = parameters
        .into_iter()
        .map(|parameter| format!("{} = {}", parameter.keyword(), literal(parameter)))
        .collect();
    written.join(", ")
}

/// The parameters `statement` assigns, in the order it assigns them.
fn assigned_parameters(statement: &Statement) -> Vec<ClusterParameterKey> {
    let Statement::AlterCluster(alter) = statement else {
        panic!("the statement must be cluster administration, got {statement:?}");
    };
    alter
        .parameters
        .iter()
        .map(ClusterParameterChange::parameter)
        .collect()
}

/// The keywords of the parameters `assigned` leaves unassigned.
fn unassigned_keywords(assigned: &BTreeSet<ClusterParameterKey>) -> BTreeSet<String> {
    ClusterParameterKey::iter()
        .filter(|parameter| !assigned.contains(parameter))
        .map(ClusterParameterKey::keyword)
        .collect()
}

#[test]
fn every_parameter_parses_and_renders_its_semantic_literal_family() {
    for parameter in ClusterParameterKey::iter() {
        let input = format!(
            "ALTER CLUSTER SET {} = {};",
            parameter.keyword(),
            literal(parameter)
        );
        let statement =
            parse_statement(&input).assured("every declared parameter has its own grammar");
        assert_eq!(assigned_parameters(&statement), vec![parameter], "{input}");
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
fn one_change_assigns_every_parameter_once_in_the_order_written() {
    let written: Vec<ClusterParameterKey> = ClusterParameterKey::iter().rev().collect();
    let input = format!(
        "ALTER CLUSTER SET {};",
        assignments(written.iter().copied())
    );
    let statement = parse_statement(&input).assured("the change assigns each parameter once");
    assert_eq!(assigned_parameters(&statement), written);
    let canonical = statement
        .to_canonical_plexus()
        .assured("cluster parameters have canonical Plexus");
    assert_eq!(
        parse_statement(&canonical).assured("canonical Plexus parses"),
        statement,
        "{canonical}"
    );
}

#[rstest::rstest]
#[case::multi_span_duration(
    "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m 49s, ",
    &[ClusterParameterKey::DomainDrainTimeout]
)]
#[case::single_span_duration(
    "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m, ",
    &[ClusterParameterKey::DomainDrainTimeout]
)]
#[case::lower_case_key(
    "alter cluster set domain_drain_timeout = 13m 49s, ",
    &[ClusterParameterKey::DomainDrainTimeout]
)]
#[case::mixed_case_key(
    "ALTER CLUSTER SET Replica_Count = 2, ",
    &[ClusterParameterKey::ReplicaCount]
)]
#[case::mixed_literal_families(
    "ALTER CLUSTER SET REPLICA_COUNT = 2, UDF_CODE_BYTES = 1KiB, STATE_SNAPSHOT_INTERVAL = 1h 2m, ",
    &[
        ClusterParameterKey::ReplicaCount,
        ClusterParameterKey::UdfCodeBytes,
        ClusterParameterKey::StateSnapshotInterval,
    ]
)]
fn completion_after_assignments_offers_only_the_parameters_they_leave(
    #[case] prefix: &str,
    #[case] assigned: &[ClusterParameterKey],
) {
    let assigned: BTreeSet<ClusterParameterKey> = assigned.iter().copied().collect();
    let offered: BTreeSet<String> = suggest_statement(prefix, prefix.len())
        .into_iter()
        .collect();
    assert_eq!(offered, unassigned_keywords(&assigned), "{prefix}");
    for parameter in ClusterParameterKey::iter() {
        if assigned.contains(&parameter) {
            continue;
        }
        let completed = format!("{prefix}{} = {};", parameter.keyword(), literal(parameter));
        let statement =
            parse_statement(&completed).assured("every offered parameter completes the change");
        let mut parameters = assigned_parameters(&statement);
        assert_eq!(parameters.pop(), Some(parameter), "{completed}");
    }
}

#[rstest::rstest]
#[case::sibling_keyword(
    "ALTER CLUSTER SET HTTP_HEADER_BYTES = 8KiB, HTTP_HEADER_",
    &["HTTP_HEADER_FIELDS"]
)]
#[case::shared_prefix(
    "ALTER CLUSTER SET TRANSACTION_IDLE_TIMEOUT = 30s, transaction_",
    &[
        "TRANSACTION_MAX_OPEN",
        "TRANSACTION_MAX_SOURCE_BYTES",
        "TRANSACTION_MAX_STATEMENTS",
        "TRANSACTION_TOMBSTONE_RETENTION",
    ]
)]
#[case::only_the_assigned_keyword_begins_so(
    "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m 49s, DOMAIN",
    &[]
)]
fn completion_of_a_partial_parameter_offers_the_unassigned_keywords_it_begins(
    #[case] input: &str,
    #[case] expected: &[&str],
) {
    let offered: BTreeSet<String> = suggest_statement(input, input.len()).into_iter().collect();
    let expected: BTreeSet<String> = expected.iter().map(|keyword| keyword.to_string()).collect();
    assert_eq!(offered, expected, "{input}");
}

#[test]
fn completion_offers_the_one_parameter_the_others_leave() {
    for remaining in ClusterParameterKey::iter() {
        let others = ClusterParameterKey::iter().filter(|parameter| *parameter != remaining);
        let prefix = format!("ALTER CLUSTER SET {}, ", assignments(others));
        assert_eq!(
            suggest_statement(&prefix, prefix.len()),
            vec![remaining.keyword()],
            "{prefix}"
        );
    }
}

#[rstest::rstest]
#[case::single_span_duration(
    "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m, DOMAIN_DRAIN_TIMEOUT = 100ms;",
    "DOMAIN_DRAIN_TIMEOUT",
    ClusterParameterKey::DomainDrainTimeout
)]
#[case::multi_span_duration_repeated_in_lower_case(
    "ALTER CLUSTER SET DOMAIN_DRAIN_TIMEOUT = 13m 49s, REPLICA_COUNT = 2, domain_drain_timeout = \
     100ms;",
    "domain_drain_timeout",
    ClusterParameterKey::DomainDrainTimeout
)]
#[case::mixed_literal_families(
    "alter cluster set replica_count = 2, UDF_CODE_BYTES = 1KiB, STATE_SNAPSHOT_INTERVAL = 1h 2m, \
     Replica_Count = 3;",
    "Replica_Count",
    ClusterParameterKey::ReplicaCount
)]
#[case::repeated_value_unwritten(
    "ALTER CLUSTER SET UDF_CODE_BYTES = 1KiB, UDF_CODE_BYTES",
    "UDF_CODE_BYTES",
    ClusterParameterKey::UdfCodeBytes
)]
fn a_repeated_parameter_is_refused_where_it_is_written(
    #[case] input: &str,
    #[case] repeat: &str,
    #[case] parameter: ClusterParameterKey,
) {
    let Err(error) = parse_statement(input) else {
        panic!("{input} assigns {parameter} twice");
    };
    let ParseFromSourceError::Parse { diagnostics, .. } = error.current_context() else {
        panic!("{input} must be refused by the grammar, got {error:?}");
    };
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    let diagnostic = &diagnostics[0];
    assert_eq!(
        diagnostic.message,
        ClusterParameterError::Duplicate { parameter }.to_string()
    );
    let start = input.rfind(repeat).assured("each case writes its repeat");
    assert_eq!(diagnostic.span.start, start, "{input}");
    assert_eq!(input.get(diagnostic.span.clone()), Some(repeat), "{input}");
}

#[test]
fn a_comma_after_every_parameter_is_reported_where_it_is_written() {
    let assigned = format!(
        "ALTER CLUSTER SET {}",
        assignments(ClusterParameterKey::iter())
    );
    let input = format!("{assigned}, made_up = 1;");
    let Err(error) = parse_statement(&input) else {
        panic!("{input} continues after every parameter is assigned");
    };
    let diagnostics = error.current_context().diagnostics();
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(diagnostics[0].message, "expected ; | end of input, found ,");
    assert_eq!(diagnostics[0].span.start, assigned.len(), "{input}");
    assert_eq!(input.get(diagnostics[0].span.clone()), Some(","), "{input}");
}

#[test]
fn a_parameter_repeated_after_every_parameter_is_refused_where_it_is_written() {
    let input = format!(
        "ALTER CLUSTER SET {}, REPLICA_COUNT = 3;",
        assignments(ClusterParameterKey::iter())
    );
    let Err(error) = parse_statement(&input) else {
        panic!("{input} assigns replica-count twice");
    };
    let diagnostics = error.current_context().diagnostics();
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert_eq!(
        diagnostics[0].message,
        ClusterParameterError::Duplicate {
            parameter: ClusterParameterKey::ReplicaCount
        }
        .to_string()
    );
    let start = input
        .rfind("REPLICA_COUNT")
        .assured("the input repeats REPLICA_COUNT last");
    assert_eq!(diagnostics[0].span.start, start, "{input}");
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
