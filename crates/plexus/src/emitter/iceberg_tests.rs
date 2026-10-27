//! Iceberg parsing and completion evidence.
//!
//! Layer: test harness.
//! - **Owns.** Current namespace and commit-limit grammar checks.
//! - **Depends on.** The composed emitter grammar and semantic vocabulary.
//! - **Must not know.** Connector drivers or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        harness,
        reason = "tests current Iceberg grammar through its public entry points"
    )
)]

use nervix_models::{EmitterEntity, IcebergEmitter};

use super::super::{parse_create_emitter, suggest_create_emitter};

fn statement(namespace: &str, size: &str) -> String {
    format!(
        "CREATE ICEBERG TABLE EMITTER output FROM events TO storage TABLE 'Orders' NAMESPACE \
         '{namespace}' LOCATION 's3://warehouse/orders' CATALOG catalog COMMIT EACH 1s MAX SIZE \
         {size} MODE ACK RETRY POLICY BACKOFF 100ms MAX 1s VALUES {{ 'id' = input.id }} FLUSH \
         IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
    )
}

#[test]
fn iceberg_namespace_and_whole_byte_commit_limit_round_trip() {
    for namespace in ["Analytics.Events", "Mixed_CASE", "a.b.C"] {
        for size in ["1B", "64MiB", "18446744073709551615B"] {
            let parsed =
                parse_create_emitter(&statement(namespace, size)).expect("current statement");
            let EmitterEntity::Iceberg(IcebergEmitter::Table(table)) = parsed.connector.as_ref()
            else {
                panic!("expected Iceberg table");
            };
            assert_eq!(table.namespace.as_str(), namespace);
            assert_eq!(table.max_commit_size.to_string(), size);
            let canonical = parsed.to_canonical_plexus().expect("canonical statement");
            assert_eq!(parse_create_emitter(&canonical).expect("reparse"), parsed);
        }
    }
}

#[test]
fn iceberg_namespace_levels_and_commit_sizes_are_validated_by_their_owners() {
    for namespace in ["", ".Events", "Analytics.", "Analytics..Events", "a/b"] {
        assert!(
            parse_create_emitter(&statement(namespace, "1MiB")).is_err(),
            "{namespace:?}"
        );
    }
    for size in ["0B", "0MiB", "0.5MiB", "18446744073709551616B", "1pebble"] {
        let error = parse_create_emitter(&statement("Analytics.Events", size))
            .expect_err("invalid whole-byte limit");
        assert!(
            format!("{error:?}").contains("COMMIT MAX SIZE"),
            "{size}: {error:?}"
        );
    }
}

#[test]
fn iceberg_namespace_completion_follows_the_composed_grammar() {
    let prefix = "CREATE ICEBERG TABLE EMITTER output FROM events TO storage TABLE 'Orders' ";
    assert_eq!(suggest_create_emitter(prefix, prefix.len()), ["NAMESPACE"]);
    let prefix = format!("{prefix}nAmEsPaCe ");
    assert_eq!(
        suggest_create_emitter(&prefix, prefix.len()),
        ["iceberg_namespace"]
    );
    let prefix = format!("{prefix}'Analytics.Events' ");
    assert_eq!(suggest_create_emitter(&prefix, prefix.len()), ["LOCATION"]);
}
