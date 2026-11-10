//! The `INSTANCES` clause of an emitter and its `SET INSTANCES` alteration.
//!
//! Layer: test harness.
//! - **Owns.** Parsing, canonical rendering, completion and declared refusals of emitter instances.
//! - **Depends on.** The composed emitter grammar and the vocabulary's instancing capabilities.
//! - **Must not know.** Placement, runtime execution or connector drivers.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use meticulous::ResultExt as _;

use super::*;

fn instanced_kafka_emitter(instances: &str) -> String {
    format!(
        "CREATE KAFKA TOPIC EMITTER spread FROM orders {instances} TO kafka_main TOPIC 'orders' \
         MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE USING my_codec INHERIT ALL FLUSH \
         IMMEDIATE ON MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
    )
}

#[test]
fn an_instances_clause_follows_the_inputs_and_reads_back_canonically() {
    for (clause, expected) in [
        ("", EmitterInstances::ONE),
        ("INSTANCES 1", EmitterInstances::ONE),
        (
            "INSTANCES 3",
            EmitterInstances::Count(std::num::NonZeroU64::new(3).assured("three is positive")),
        ),
        (
            "INSTANCES 18446744073709551615",
            EmitterInstances::Count(std::num::NonZeroU64::MAX),
        ),
        ("INSTANCES PER PARTITION", EmitterInstances::PerPartition),
        (
            "COLLECT FOR 10ms INSTANCES PER PARTITION",
            EmitterInstances::PerPartition,
        ),
    ] {
        let source = instanced_kafka_emitter(clause);
        let parsed = parse_create_emitter(&source)
            .unwrap_or_else(|error| panic!("`{clause}` must parse: {error:?}"));
        assert_eq!(parsed.body.instances, expected, "{source}");
        let canonical = parsed
            .to_canonical_plexus()
            .assured("an instanced emitter renders canonically");
        let lines: Vec<_> = canonical.lines().map(str::trim).collect();
        if expected.is_one() {
            assert!(!canonical.contains("INSTANCES"), "{canonical}");
        } else {
            assert!(
                lines.windows(3).any(|window| window[0].starts_with("FROM ")
                    && window[1] == format!("INSTANCES {expected}")
                    && window[2] == "ATTACHED"),
                "{canonical}\nmust write INSTANCES between the inputs and the attachment"
            );
        }
        assert_eq!(
            parse_create_emitter(&canonical).assured("the canonical form parses"),
            parsed
        );
    }
    for refused in [
        "INSTANCES",
        "INSTANCES 0",
        "INSTANCES 18446744073709551616",
        "INSTANCES PER",
        "INSTANCES PARTITION",
        "ATTACHED INSTANCES 2",
    ] {
        let source = instanced_kafka_emitter(refused);
        assert!(parse_create_emitter(&source).is_err(), "{source}");
    }
}

#[test]
fn instanced_emitters_keep_exact_labels_through_the_composed_creation_grammar() {
    for (clause, expected) in [
        (
            "INSTANCES 3",
            EmitterInstances::Count(std::num::NonZeroU64::new(3).assured("three is positive")),
        ),
        ("INSTANCES PER PARTITION", EmitterInstances::PerPartition),
    ] {
        let definition = instanced_kafka_emitter(clause);
        let definition = definition
            .strip_suffix(';')
            .assured("the fixture ends its statement");
        let source =
            format!("{definition} LABELS {{ 'owner' = '  Platform / Δ  ', 'empty' = '' }};");
        let parsed = parse_create_emitter(&source).assured("instances and labels compose");
        assert_eq!(parsed.body.instances, expected);
        assert_eq!(parsed.body.labels.get("owner"), Some("  Platform / Δ  "));
        assert_eq!(parsed.body.labels.get("empty"), Some(""));
        let canonical = parsed
            .to_canonical_plexus()
            .assured("the complete emitter renders");
        assert_eq!(
            parse_create_emitter(&canonical).assured("the complete canonical emitter reparses"),
            parsed,
        );
    }
}

#[test]
fn completion_offers_instances_only_where_the_entity_runs_several() {
    let prefix = "CREATE KAFKA TOPIC EMITTER spread FROM orders ";
    let suggestions = suggest_create_emitter(prefix, prefix.len());
    for expected in ["INSTANCES", "ATTACHED", "DETACHED", "TO"] {
        assert!(
            suggestions.contains(&expected.to_string()),
            "missing {expected}: {suggestions:?}"
        );
    }
    let prefix = "CREATE KAFKA TOPIC EMITTER spread FROM orders INSTANCES ";
    assert_eq!(
        suggest_create_emitter(prefix, prefix.len()),
        vec!["PER PARTITION", "instance_count"]
    );
    let prefix = "CREATE KAFKA TOPIC EMITTER spread FROM orders INSTANCES 2 ";
    let suggestions = suggest_create_emitter(prefix, prefix.len());
    assert!(
        !suggestions.contains(&"INSTANCES".to_string()),
        "{suggestions:?}"
    );
    for prefix in [
        "CREATE CLIENT CONSUMER EMITTER single FROM orders ",
        "CREATE MQTT TOPIC EMITTER single FROM orders ",
        "CREATE ICEBERG TABLE EMITTER single FROM orders ",
    ] {
        let suggestions = suggest_create_emitter(prefix, prefix.len());
        assert!(
            !suggestions.contains(&"INSTANCES".to_string()),
            "{suggestions:?}"
        );
        assert!(
            suggestions.contains(&"ATTACHED".to_string()),
            "{suggestions:?}"
        );
    }
}

#[test]
fn a_single_instance_entity_refuses_instances_with_its_declared_reason() {
    for (head, tail, message) in [
        (
            "CLIENT CONSUMER",
            "TO SCHEMA event MODE ACK SEQUENTIAL ACK TIMEOUT 30s RETRY POLICY BACKOFF 100ms MAX \
             1s INHERIT ALL BATCH MAX MESSAGES 16 MAX SIZE 1MiB",
            "CLIENT CONSUMER emitters deliver through one consumer stream, so they refuse \
             INSTANCES",
        ),
        (
            "MQTT TOPIC",
            "TO mqtt_main TOPIC 'orders' QOS 1 MODE NO ACK RETRY POLICY BACKOFF 1s MAX 30s ENCODE \
             USING my_codec INHERIT ALL",
            "MQTT TOPIC emitters publish through one client identifier, so they refuse INSTANCES",
        ),
        (
            "ICEBERG TABLE",
            "TO lake NAMESPACE 'analytics' TABLE 'orders' MODE ACK",
            "ICEBERG TABLE emitters commit through one table writer, so they refuse INSTANCES",
        ),
    ] {
        for instances in ["INSTANCES 2", "INSTANCES PER PARTITION", "INSTANCES 1"] {
            let source = format!(
                "CREATE {head} EMITTER single FROM orders {instances} {tail} FLUSH IMMEDIATE ON \
                 MESSAGE ERROR LOG ON GENERAL ERROR LOG;"
            );
            let error = parse_create_emitter(&source)
                .err()
                .unwrap_or_else(|| panic!("{source} must be refused"));
            assert!(format!("{error:?}").contains(message), "{error:?}");
        }
        let alter = format!("ALTER {head} EMITTER single SET INSTANCES 2;");
        let error = parse_alter_emitter(&alter)
            .err()
            .unwrap_or_else(|| panic!("{alter} must be refused"));
        assert!(format!("{error:?}").contains(message), "{error:?}");
        let prefix = format!("ALTER {head} EMITTER single SET ");
        let suggestions = suggest_alter_emitter(&prefix, prefix.len());
        assert!(
            !suggestions.contains(&"INSTANCES".to_string()),
            "{suggestions:?}"
        );
    }
}

#[test]
fn set_instances_replaces_the_declaration_of_an_entity_that_runs_several() {
    for (operation, expected) in [
        (
            "SET INSTANCES 2",
            EmitterInstances::Count(std::num::NonZeroU64::new(2).assured("two is positive")),
        ),
        (
            "SET INSTANCES PER PARTITION",
            EmitterInstances::PerPartition,
        ),
    ] {
        let source = format!("ALTER KAFKA TOPIC EMITTER spread {operation};");
        let parsed = parse_alter_emitter(&source)
            .unwrap_or_else(|error| panic!("{source} must parse: {error:?}"));
        assert_eq!(
            parsed.operations,
            vec![AlterEmitterOperation::SetInstances {
                instances: expected
            }]
        );
        let statement = crate::statement::parse_statement(&source).assured("the alteration parses");
        crate::conformance_support::assert_reads_back_canonically(&statement);
    }
    let prefix = "ALTER KAFKA TOPIC EMITTER spread SET ";
    let suggestions = suggest_alter_emitter(prefix, prefix.len());
    assert!(
        suggestions.contains(&"INSTANCES".to_string()),
        "{suggestions:?}"
    );
    assert!(parse_alter_emitter("ALTER KAFKA TOPIC EMITTER spread SET INSTANCES 0;").is_err());
}
