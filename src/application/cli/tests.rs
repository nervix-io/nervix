//! Server command-line groups, the settings they convert into and the forms they render.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions about the options groups, their names, headings and defaults, the
//!   rendering of settings back into options, and that every node setting a connector declaration
//!   names is one of those options.
//! - **Depends on.** The command-line module under test, the application test fixtures and the
//!   connector declarations' bounds.
//! - **Must not know.** Production ownership beyond the module under test.

use std::{collections::BTreeSet, time::Duration};

use clap::CommandFactory as _;
use nervix_consensus::RaftTimingError;
use ubyte::ByteUnit;

use super::*;
use crate::application::{
    cli_values::{HumanBytes, HumanDuration},
    test_fixtures::test_args,
};

/// The headings of the settings areas, in the order help lists them.
const SETTINGS_HEADINGS: [&str; 22] = [
    "Databases",
    "Consensus capacities",
    "DNS",
    "Interconnect",
    "Connector host",
    "Expression engine",
    "First-bootstrap cluster parameters",
    "Consensus",
    "Membership",
    "Scheduling",
    "Runtime state",
    "Data-plane runtime capabilities",
    "Transactions and commands",
    "Memory pressure",
    "Shutdown",
    "Resources",
    "Capacity",
    "Execution",
    "Operational node capabilities",
    "Listeners",
    "Trace export",
    "Web console pacing",
];

/// Every setting the settings groups declare, by the name its definition gives it.
const SETTING_NAMES: [&str; 270] = [
    "runtime-delivery-capacity",
    "runtime-ingest-group-max-rows",
    "runtime-ingest-group-idle-flush",
    "runtime-unacknowledged-poll-batch",
    "runtime-ingestor-error-retry",
    "runtime-reconnect-initial",
    "runtime-reconnect-max",
    "runtime-branch-expiration-scan",
    "runtime-replication-poll",
    "runtime-catch-up-concurrency",
    "runtime-catch-up-pages",
    "runtime-retained-removals",
    "runtime-snapshot-staging-bytes",
    "runtime-snapshot-bytes",
    "runtime-snapshot-describe-timeout",
    "runtime-snapshot-fetch-timeout",
    "runtime-wasm-checkpoint-timeout",
    "runtime-event-capacity",
    "runtime-wasm-recovery-capacity",
    "runtime-immediate-flush-minimum",
    "runtime-checkpoint-bytes",
    "runtime-wasm-epoch-tick-interval",
    "runtime-wasm-epoch-deadline-ticks",
    "runtime-wasm-guest-buffer-bytes",
    "runtime-wasm-optimize",
    "connector-retry-multiplier",
    "connector-retry-jitter-percent",
    "kafka-domain-assignment-retry",
    "partition-topology-poll-interval",
    "syslog-intake-queue-capacity",
    "syslog-read-chunk-bytes",
    "mqtt-read-ahead-headroom",
    "wasm-stack-bytes",
    "wasm-memory-reservation-bytes",
    "wasm-memory-guard-bytes",
    "wasm-memory-growth-reservation-bytes",
    "vm-inline-row-limit",
    "vm-regex-search-cache-bytes",
    "vm-dynamic-pattern-cache-capacity",
    "vm-pattern-sets-per-batch",
    "udf-watchdog",
    "database-cache-bytes",
    "database-journal-bytes",
    "database-workers",
    "database-max-open-files",
    "raft-database-cache-bytes",
    "raft-database-journal-bytes",
    "raft-database-workers",
    "raft-database-max-open-files",
    "raft-event-capacity",
    "raft-append-batch-entries",
    "raft-outstanding-append-batches",
    "raft-outstanding-append-bytes",
    "raft-append-idle-bound",
    "raft-snapshot-transfer-timeout",
    "raft-membership-mutation-timeout",
    "raft-retention-interval",
    "raft-retention-admission-timeout",
    "raft-purge-batch-entries",
    "raft-api-channel-capacity",
    "raft-notification-channel-capacity",
    "raft-state-machine-channel-capacity",
    "raft-api-batch-capacity",
    "raft-replication-backoff",
    "raft-replication-backoff-maximum",
    "raft-membership-step-down-delay",
    "dns-cache-entries",
    "dns-positive-max-ttl",
    "dns-negative-max-ttl",
    "dns-hook-lookup-budget",
    "dns-address-family-policy",
    "dns-trust-negative-answers",
    "interconnect-reconnect-jitter-permille",
    "interconnect-max-peers",
    "interconnect-concurrent-handshakes",
    "interconnect-incoming-queue-capacity",
    "interconnect-request-quota",
    "interconnect-stream-receive-window",
    "interconnect-connection-receive-window",
    "interconnect-connection-setup-timeout",
    "interconnect-request-timeout",
    "interconnect-progress-timeout",
    "interconnect-reconnect-backoff",
    "interconnect-max-reconnect-backoff",
    "raft-heartbeat-interval",
    "raft-snapshot-entry-threshold",
    "raft-snapshot-byte-threshold",
    "raft-covered-log-entries-retained",
    "raft-covered-log-bytes-retained",
    "raft-retained-log-cap",
    "node-unavailability-timeout",
    "gossip-interval",
    "clock-skew-tolerance",
    "gossip-receive-queue-capacity",
    "gossip-tombstone-grace-period",
    "replica-count",
    "state-snapshot-interval",
    "inspection-max-rows",
    "inspection-max-sections",
    "inspection-read-timeout",
    "restore-staging-max-bytes",
    "transaction-idle-timeout",
    "transaction-tombstone-retention",
    "command-retry-validity",
    "command-execution-capacity",
    "transaction-max-statements",
    "transaction-max-source-bytes",
    "transaction-max-open",
    "memory-high-watermark",
    "memory-low-watermark",
    "memory-pressure-check-interval",
    "memory-pressure-resume-jitter",
    "shutdown-timeout",
    "task-grace-period",
    "resource-max-archive-bytes",
    "resource-max-extracted-bytes",
    "resource-max-file-count",
    "interconnect-relay-batch-bytes",
    "interconnect-relay-decoded-bytes",
    "interconnect-bulk-chunk-bytes",
    "interconnect-bulk-receive-window",
    "cpu-count",
    "memory-limit",
    "node-detection-timeout",
    "execution-control-cpu-workers",
    "execution-control-cpu-pending-jobs",
    "execution-credentials-cpu-workers",
    "execution-credentials-cpu-pending-jobs",
    "execution-data-cpu-workers",
    "execution-data-cpu-pending-jobs",
    "execution-extension-cpu-workers",
    "execution-extension-cpu-pending-jobs",
    "execution-bulk-cpu-workers",
    "execution-bulk-cpu-pending-jobs",
    "execution-consensus-storage-workers",
    "execution-consensus-storage-pending-jobs",
    "execution-filesystem-storage-workers",
    "execution-filesystem-storage-pending-jobs",
    "execution-connector-storage-workers",
    "execution-connector-storage-pending-jobs",
    "execution-management-memory-bytes",
    "execution-commands-memory-bytes",
    "execution-relay-memory-bytes",
    "execution-state-read-memory-bytes",
    "execution-state-read-cache-memory-bytes",
    "execution-bulk-memory-bytes",
    "execution-bulk-window-memory-bytes",
    "execution-restore-metadata-memory-bytes",
    "execution-intake-memory-bytes",
    "execution-credentials-memory-bytes",
    "execution-relay-encoded-bytes",
    "execution-relay-decoded-bytes",
    "execution-relay-scratch-bytes",
    "execution-resident-replication-batches",
    "execution-bulk-chunk-bytes",
    "runtime-worker-threads",
    "runtime-max-blocking-threads",
    "runtime-thread-stack-size",
    "client-producer-node-bytes",
    "client-consumer-node-bytes",
    "listener-max-connections",
    "listener-header-bytes",
    "listener-max-headers",
    "listener-buffer-bytes",
    "listener-header-timeout",
    "listener-tls-timeout",
    "listener-idle-timeout",
    "listener-accept-retry-delay",
    "listener-http-keepalive",
    "http-body-bytes",
    "http-body-copy-bytes",
    "http-body-timeout",
    "http-refusal-drain-timeout",
    "websocket-message-bytes",
    "websocket-frame-bytes",
    "websocket-write-buffer-bytes",
    "grpc-header-bytes",
    "grpc-concurrency",
    "grpc-request-timeout",
    "grpc-max-streams",
    "grpc-stream-window-bytes",
    "grpc-connection-window-bytes",
    "grpc-send-buffer-bytes",
    "grpc-max-pending-reset-streams",
    "grpc-max-error-reset-streams",
    "grpc-keepalive-interval",
    "grpc-keepalive-timeout",
    "listener-tcp-keepalive-time",
    "listener-tcp-keepalive-interval",
    "listener-tcp-keepalive-retries",
    "payload-unfold-limit",
    "forced-recovery-budget",
    "ownership-handoff-poll-interval",
    "entity-gate-poll-interval",
    "domain-drain-poll-interval",
    "runtime-readiness-propagation-bound",
    "tls-cipher-suites",
    "tls-key-exchange-groups",
    "password-iterations",
    "licensing-request-timeout",
    "licensing-retry-backoff-max",
    "licensing-stream-idle-timeout",
    "domain-drain-timeout",
    "entity-gate-deadline",
    "remote-ack-alive-interval",
    "remote-ack-dispatch-timeout",
    "remote-ack-silence-timeout",
    "remote-ack-sweep-interval",
    "remote-relay-total-timeout",
    "remote-relay-instantiation-wait",
    "max-batch-messages",
    "client-producer-session-bytes",
    "client-consumer-session-bytes",
    "udf-code-bytes",
    "udf-compile-budget",
    "entity-labels-max-count",
    "entity-labels-max-key-bytes",
    "entity-labels-max-value-bytes",
    "entity-labels-max-bytes",
    "http-header-bytes",
    "http-header-fields",
    "client-batch-rows",
    "control-plane-membership-interval",
    "control-plane-scheduling-interval",
    "control-plane-resource-replication-interval",
    "control-plane-health-probe-interval",
    "control-plane-leadership-sync-retry",
    "control-plane-runtime-state-retry-interval",
    "control-plane-transaction-sweep-interval",
    "control-plane-restore-staging-sweep-interval",
    "control-plane-transaction-recovery-concurrency",
    "control-plane-resource-replication-concurrency",
    "control-plane-entity-gate-release-retry-interval",
    "control-plane-entity-gate-engagement-retry-interval",
    "control-plane-transaction-application-retry-interval",
    "control-plane-domain-clock-progress-retry-interval",
    "control-plane-backup-retry-interval",
    "control-plane-restore-admission-wait",
    "control-plane-restore-admission-retry-interval",
    "control-plane-model-inspection-timeout",
    "control-plane-kafka-partition-watch-interval",
    "interconnect-tls-reload-interval",
    "authentication-attempts-per-second",
    "session-frame-bytes",
    "session-transfer-bytes",
    "session-nesting-depth",
    "session-collection-entries",
    "session-string-bytes",
    "session-queue-memory-bytes",
    "session-subscription-rows-per-frame",
    "session-notice-capacity",
    "session-leadership-check-interval",
    "session-snapshot-interval",
    "client-link-heartbeat-interval",
    "client-link-producer-open-timeout",
    "client-link-consumer-open-timeout",
    "client-link-producer-answer-capacity",
    "client-link-consumer-answer-capacity",
    "client-link-consumer-writer-capacity",
    "otel-trace-sample-ratio",
    "otel-trace-export-timeout",
    "otel-trace-export-interval",
    "otel-trace-queue-capacity",
    "otel-trace-export-batch-spans",
    "web-console-request-timeout",
    "web-console-connect-timeout",
    "web-console-retry-backoff",
    "web-console-retry-max-backoff",
    "web-console-restoration-backoff",
    "web-console-restore-frame-capacity",
];

#[test]
fn the_complete_server_command_has_distinct_argument_identities() {
    Args::command().debug_assert();
}

#[test]
fn durations_render_in_the_largest_unit_that_holds_them_exactly() {
    let rendered = |duration: Duration| HumanDuration(duration).to_string();

    assert_eq!(rendered(Duration::from_millis(250)), "250ms");
    assert_eq!(rendered(Duration::from_millis(1500)), "1500ms");
    assert_eq!(rendered(Duration::from_secs(3)), "3s");
    assert_eq!(rendered(Duration::from_secs(15 * 60)), "15m");
    assert_eq!(rendered(Duration::from_secs(2 * 86_400)), "2d");
    assert_eq!(rendered(Duration::from_micros(7)), "7us");
    assert_eq!(rendered(Duration::new(1, 1)), "1000000001ns");
    assert_eq!(rendered(Duration::ZERO), "0s");
    // Nanoseconds this many do not fit one 64-bit count, so the seconds are written apart.
    assert_eq!(
        rendered(Duration::new(u64::MAX, 1)),
        "18446744073709551615s 1ns"
    );
}

#[test]
fn large_subsecond_durations_render_as_exact_parseable_spans() {
    struct DurationCase {
        duration: Duration,
        text: &'static str,
    }

    let cases = [
        DurationCase {
            duration: Duration::from_micros(u64::MAX / 1_000),
            text: "18446744073709551us",
        },
        DurationCase {
            duration: Duration::from_micros(u64::MAX / 1_000 + 1),
            text: "18446744073s 709552000ns",
        },
        DurationCase {
            duration: Duration::from_millis(u64::MAX / 1_000_000),
            text: "18446744073709ms",
        },
        DurationCase {
            duration: Duration::from_millis(u64::MAX / 1_000_000 + 1),
            text: "18446744073s 710000000ns",
        },
        DurationCase {
            duration: Duration::new(169_112_806_400, 601_538_000),
            text: "169112806400s 601538000ns",
        },
        DurationCase {
            duration: Duration::new(169_112_806_400, 601_000_000),
            text: "169112806400s 601000000ns",
        },
        DurationCase {
            duration: Duration::from_nanos(u64::MAX),
            text: "18446744073709551615ns",
        },
        DurationCase {
            duration: Duration::from_secs(60_000_000_000),
            text: "1000000000m",
        },
    ];

    for case in cases {
        let text = HumanDuration(case.duration).to_string();
        assert_eq!(text, case.text);
        let read = text
            .parse::<HumanDuration>()
            .assured("the rendered spans preserve every nanosecond");
        assert_eq!(read.0, case.duration);
    }
}

#[test]
fn byte_quantities_render_in_the_largest_binary_unit_that_holds_them_exactly() {
    let rendered = |bytes: ByteUnit| HumanBytes(bytes).to_string();

    assert_eq!(rendered(ByteUnit::Mebibyte(64)), "64MiB");
    assert_eq!(rendered(ByteUnit::Gibibyte(128)), "128GiB");
    assert_eq!(rendered(ByteUnit::Kibibyte(1536)), "1536KiB");
    assert_eq!(rendered(ByteUnit::Byte(1000)), "1000B");
    assert_eq!(rendered(ByteUnit::Byte(0)), "0B");
}

#[test]
fn every_rendered_duration_and_byte_quantity_reads_back_unchanged() {
    for duration in [
        Duration::from_millis(250),
        Duration::from_millis(1500),
        Duration::from_secs(15 * 60),
        Duration::new(1, 1),
        Duration::ZERO,
        Duration::new(4_000_000_000, 999_999_999),
    ] {
        let read = HumanDuration(duration)
            .to_string()
            .parse::<HumanDuration>()
            .assured("a rendered duration is duration text");
        assert_eq!(read, HumanDuration(duration));
    }
    for bytes in [
        ByteUnit::Mebibyte(64),
        ByteUnit::Kibibyte(1536),
        ByteUnit::Byte(1000),
        ByteUnit::Byte(0),
        ByteUnit::Byte(u64::MAX),
    ] {
        let read = HumanBytes(bytes)
            .to_string()
            .parse::<HumanBytes>()
            .assured("a rendered byte quantity is byte text");
        assert_eq!(read, HumanBytes(bytes));
    }
}

#[test]
fn a_command_line_without_settings_runs_every_definition_default() {
    let application =
        Application::try_from(test_args(&[])).assured("the fixture's arguments are valid");

    assert_eq!(application.settings, NodeSettings::default());
}

#[test]
fn tls_algorithm_seeds_resolve_aliases_and_round_trip_as_explicit_selections() {
    for name in ["tls-cipher-suites", "tls-key-exchange-groups"] {
        let command = Args::command();
        let argument = command
            .get_arguments()
            .find(|argument| argument.get_long() == Some(name))
            .assured("the TLS seed has a long option");
        assert_eq!(
            argument.get_env().and_then(|value| value.to_str()),
            Some(format!("NERVIX_{}", name.to_uppercase().replace('-', "_")).as_str())
        );
    }
    let settings = NodeSettings::default()
        .with_overrides(&[
            override_of("tls-cipher-suites", "FiPs,TLS13_CHACHA20_POLY1305_SHA256"),
            override_of("tls-key-exchange-groups", "fips,X25519,X25519"),
        ])
        .assured("both explicit TLS selections form a valid policy");
    let values = nervix_models::ClusterParameterValues::resolve(settings.parameter_seeds)
        .assured("the CLI retains a valid complete seed pair");
    assert!(values.tls_cipher_suites().to_string().contains("CHACHA20"));
    assert!(
        values
            .tls_key_exchange_groups()
            .to_string()
            .ends_with(",X25519")
    );
    assert_eq!(
        settings
            .with_overrides(&[])
            .assured("resolved names read back"),
        settings
    );
}

#[test]
fn tls_seed_pairs_are_validated_before_the_node_starts() {
    for overrides in [
        vec![override_of("tls-key-exchange-groups", "MLKEM768")],
        vec![override_of(
            "tls-cipher-suites",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        )],
    ] {
        let refused = NodeSettings::default()
            .with_overrides(&overrides)
            .expect_err("the complete TLS seed pair is unusable");
        assert!(matches!(
            refused.current_context(),
            AppError::InvalidClusterParameters
        ));
        assert!(refused.contains::<nervix_models::ClusterParameterError>());
    }
    for value in ["", "TLS13_NOT_A_SUITE"] {
        let refused = NodeSettings::default()
            .with_overrides(&[override_of("tls-cipher-suites", value)])
            .expect_err("an empty or unknown algorithm selection is refused");
        assert!(matches!(refused.current_context(), AppError::ReadSettings));
    }
}

#[test]
fn every_settings_option_is_named_by_its_definition_and_read_from_its_environment_twin() {
    let command = Args::command();
    let mut names = BTreeSet::new();
    for argument in command.get_arguments() {
        let Some(heading) = argument.get_help_heading() else {
            continue;
        };
        if !SETTINGS_HEADINGS.contains(&heading) {
            continue;
        }
        let name = argument
            .get_long()
            .assured("every settings option has a long name");
        let environment = argument
            .get_env()
            .assured("every settings option has an environment twin");
        let expected = format!("NERVIX_{}", name.to_uppercase().replace('-', "_"));
        assert_eq!(environment.to_str(), Some(expected.as_str()), "{name}");
        names.insert(name);
    }

    assert_eq!(names, SETTING_NAMES.into_iter().collect::<BTreeSet<_>>());
}

#[test]
fn every_node_setting_a_connector_declaration_names_is_a_settings_option() {
    let mut named = BTreeSet::new();
    for kind in nervix_models::IngestorEntityKind::all() {
        named.extend(kind.capabilities().bounds.node_settings());
    }
    for kind in nervix_models::EmitterEntityKind::all() {
        named.extend(kind.capabilities().bounds.node_settings());
    }
    assert!(
        !named.is_empty(),
        "connector declarations name node settings, so this check is not vacuous"
    );

    let options: BTreeSet<&str> = SETTING_NAMES.into_iter().collect();
    let undeclared: Vec<&str> = named.difference(&options).copied().collect();
    assert!(
        undeclared.is_empty(),
        "connector declarations name node settings the node does not declare: {undeclared:?}"
    );
}

#[test]
fn help_lists_each_settings_area_under_its_own_heading() {
    let help = Args::command().render_long_help().to_string();
    let mut positions = Vec::new();
    for heading in SETTINGS_HEADINGS {
        let position = help
            .find(&format!("\n{heading}:\n"))
            .unwrap_or_else(|| panic!("help lists no {heading} heading"));
        positions.push(position);
    }
    let identity = help
        .find("\nIdentity, addresses and paths:\n")
        .assured("help lists the identity heading");

    assert!(positions.is_sorted(), "{help}");
    assert!(identity < positions[0], "{help}");
}

#[test]
fn infrastructure_overrides_preserve_derived_inputs_and_refuse_invalid_bounds() {
    let settings = NodeSettings::default()
        .with_overrides(&[
            override_of("interconnect-max-peers", "16"),
            override_of("interconnect-request-quota", "4"),
            override_of("interconnect-request-timeout", "20s"),
            override_of("database-workers", "2"),
            override_of("raft-database-max-open-files", "64"),
            override_of("dns-address-family-policy", "ipv4"),
            override_of("dns-trust-negative-answers", "false"),
            override_of("raft-append-idle-bound", "3s"),
        ])
        .assured("consistent local infrastructure bounds");
    let rendered = settings.with_overrides(&[]).assured("settings read back");
    assert_eq!(rendered, settings);
    assert_eq!(settings.interconnect.request_quota(), 4);
    assert_eq!(*settings.dns.concurrent_lookups.value(), 16);
    assert_eq!(*settings.node_database.workers.value(), Some(2));
    assert_eq!(*settings.consensus_database.workers.value(), None);
    assert_eq!(settings.consensus_database.max_open_files.value().get(), 64);
    assert_eq!(
        settings.node_database.max_open_files,
        NodeSettings::default().node_database.max_open_files,
        "an omitted open-file limit keeps the platform default"
    );
    assert_eq!(
        NodeSettings::default().interconnect.values().request_quota,
        None
    );

    for (name, value) in [
        ("interconnect-max-peers", "0"),
        ("interconnect-request-quota", "0"),
        ("interconnect-request-quota", "1025"),
        ("interconnect-request-timeout", "0s"),
        ("interconnect-stream-receive-window", "2GiB"),
        ("interconnect-reconnect-jitter-permille", "1001"),
        ("database-journal-bytes", "1MiB"),
        ("raft-database-workers", "0"),
        ("database-max-open-files", "9"),
        ("raft-database-max-open-files", "0"),
        ("dns-cache-entries", "0"),
        ("dns-hook-lookup-budget", "0s"),
        ("raft-outstanding-append-batches", "0"),
        ("raft-retention-interval", "0s"),
        ("raft-replication-backoff-maximum", "100ms"),
    ] {
        assert!(
            NodeSettings::default()
                .with_overrides(&[override_of(name, value)])
                .is_err(),
            "{name}={value} must be refused before startup"
        );
    }
}

#[test]
fn rendered_settings_read_back_as_the_same_settings() {
    let settings = NodeSettings::default()
        .with_overrides(&[
            override_of("raft-heartbeat-interval", "100ms"),
            override_of("raft-covered-log-bytes-retained", "3MiB"),
            override_of("replica-count", "2"),
            override_of("restore-staging-max-bytes", "5GiB"),
            override_of("transaction-max-source-bytes", "77"),
            override_of("memory-high-watermark", "2MiB"),
            override_of("memory-low-watermark", "1MiB"),
            override_of("resource-max-file-count", "9"),
            override_of("inspection-max-rows", "7"),
            override_of("inspection-max-sections", "2"),
            override_of("inspection-read-timeout", "3s"),
        ])
        .assured("the overrides are valid settings");

    assert_eq!(settings.runtime.inspection.max_rows.value().get(), 7);
    assert_eq!(settings.runtime.inspection.max_sections.value().get(), 2);
    assert_eq!(
        *settings.runtime.inspection.read_timeout.value(),
        Duration::from_secs(3)
    );

    let read = settings
        .with_overrides(&[])
        .assured("rendered settings read back");

    assert_eq!(read, settings);
}

#[test]
fn a_named_setting_replaces_only_its_own_value() {
    let defaults = NodeSettings::default();
    let settings = defaults
        .with_overrides(&[override_of("replica-count", "2")])
        .assured("two replicas are a valid replica count");

    assert_eq!(settings.scheduling.replica_count(), 2);
    assert_eq!(
        NodeSettings {
            scheduling: defaults.scheduling,
            parameter_seeds: defaults.parameter_seeds,
            ..settings
        },
        defaults
    );
}

#[test]
fn a_later_setting_of_the_same_name_replaces_an_earlier_one() {
    let settings = NodeSettings::default()
        .with_overrides(&[
            override_of("replica-count", "2"),
            override_of("replica-count", "3"),
        ])
        .assured("three replicas are a valid replica count");

    assert_eq!(settings.scheduling.replica_count(), 3);
}

#[test]
fn a_setting_the_command_line_does_not_know_is_refused() {
    let refused = NodeSettings::default()
        .with_overrides(&[override_of("no-such-setting", "1")])
        .err()
        .assured("an unknown option is refused");

    assert!(matches!(refused.current_context(), AppError::ReadSettings));
    assert!(refused.contains::<clap::Error>());
}

#[test]
fn a_heartbeat_below_one_millisecond_refuses_the_node() {
    let refused = Application::try_from(test_args(&["--raft-heartbeat-interval", "500us"]))
        .err()
        .assured("half a millisecond truncates to no consensus milliseconds");
    assert!(matches!(
        refused.current_context(),
        AppError::InvalidRaftTimings
    ));
    assert_eq!(
        refused.downcast_ref::<RaftTimingError>(),
        Some(&RaftTimingError::HeartbeatBelowOneMillisecond {
            heartbeat_interval: Duration::from_micros(500),
        })
    );
}

#[test]
fn election_timeouts_are_six_and_twelve_heartbeats() {
    let application = Application::try_from(test_args(&["--raft-heartbeat-interval", "200ms"]))
        .assured("a 200ms heartbeat is a valid heartbeat");
    let raft = application.settings.raft;
    assert_eq!(raft.election_timeout_min(), Duration::from_millis(1200));
    assert_eq!(raft.election_timeout_max(), Duration::from_millis(2400));
}

#[test]
fn a_gossip_interval_below_one_millisecond_refuses_the_node() {
    let refused = Application::try_from(test_args(&["--gossip-interval", "0s"]))
        .err()
        .assured("no gossip interval is no cadence");
    assert!(matches!(
        refused.current_context(),
        AppError::InvalidMembershipSettings
    ));
}

#[test]
fn gossip_and_mqtt_read_ahead_capabilities_read_back_as_configured() {
    let settings = NodeSettings::default()
        .with_overrides(&[
            override_of("gossip-receive-queue-capacity", "512"),
            override_of("gossip-tombstone-grace-period", "90s"),
            override_of("mqtt-read-ahead-headroom", "32"),
        ])
        .assured("positive capacities within their channel bounds and a positive grace are valid");

    let read = settings
        .with_overrides(&[])
        .assured("rendered settings read back");
    assert_eq!(read, settings);
    assert_ne!(settings, NodeSettings::default());
    assert_eq!(
        settings.membership.gossip_receive_queue_capacity().get(),
        512
    );
    assert_eq!(
        settings.membership.gossip_tombstone_grace_period(),
        Duration::from_secs(90)
    );
    assert_eq!(settings.runtime.connector_host.mqtt.headroom().get(), 32);
    let defaults = NodeSettings::default();
    assert_eq!(
        defaults.membership.gossip_receive_queue_capacity().get(),
        1024
    );
    assert_eq!(
        defaults.membership.gossip_tombstone_grace_period(),
        Duration::from_secs(60)
    );
    assert_eq!(defaults.runtime.connector_host.mqtt.headroom().get(), 64);
}

#[test]
fn gossip_and_mqtt_read_ahead_capabilities_outside_their_bounds_refuse_the_node() {
    struct Refusal {
        name: &'static str,
        value: String,
        membership: bool,
    }

    let beyond_channel = nervix_primitives::sync::Semaphore::MAX_PERMITS
        .checked_add(1)
        .assured("the channel bound leaves its three high bits free")
        .to_string();
    let refusals = [
        Refusal {
            name: "gossip-receive-queue-capacity",
            value: beyond_channel.clone(),
            membership: true,
        },
        Refusal {
            name: "gossip-tombstone-grace-period",
            value: "0s".to_string(),
            membership: true,
        },
        Refusal {
            name: "gossip-tombstone-grace-period",
            value: "4294967296s".to_string(),
            membership: true,
        },
        Refusal {
            name: "mqtt-read-ahead-headroom",
            value: beyond_channel,
            membership: false,
        },
    ];
    for refusal in refusals {
        let refused = NodeSettings::default()
            .with_overrides(&[override_of(refusal.name, &refusal.value)])
            .err()
            .assured("a value outside the capability's bounds is refused before startup");
        if refusal.membership {
            assert!(
                matches!(
                    refused.current_context(),
                    AppError::InvalidMembershipSettings
                ),
                "{}={}: {refused:#}",
                refusal.name,
                refusal.value
            );
            assert!(refused.contains::<crate::cluster::MembershipSettingsError>());
        } else {
            assert!(
                matches!(
                    refused.current_context(),
                    AppError::InvalidConnectorHostSettings
                ),
                "{}={}: {refused:#}",
                refusal.name,
                refusal.value
            );
            assert!(refused.contains::<nervix_connector_mqtt::MqttReadAheadSettingsError>());
        }
        assert!(
            format!("{refused:#}").contains(refusal.name),
            "the refusal names {}: {refused:#}",
            refusal.name
        );
    }
}

#[test]
fn a_zero_gossip_queue_or_mqtt_headroom_cannot_be_read() {
    for name in ["gossip-receive-queue-capacity", "mqtt-read-ahead-headroom"] {
        let refused = NodeSettings::default()
            .with_overrides(&[override_of(name, "0")])
            .err()
            .assured("a capacity of no items is refused before startup");
        assert!(matches!(refused.current_context(), AppError::ReadSettings));
        let kind = refused
            .downcast_ref::<clap::Error>()
            .assured("the command line refuses the value it cannot read")
            .kind();
        assert_eq!(kind, clap::error::ErrorKind::ValueValidation, "{name}");
    }
}

#[test]
fn a_named_setting_that_breaks_a_relationship_is_refused_like_an_option() {
    let refused = NodeSettings::default()
        .with_overrides(&[override_of("raft-heartbeat-interval", "500us")])
        .err()
        .assured("half a millisecond truncates to no consensus milliseconds");

    assert!(matches!(
        refused.current_context(),
        AppError::InvalidRaftTimings
    ));
}

#[test]
fn a_udf_watchdog_reaches_the_runtime_and_its_capability_claim() {
    assert_eq!(
        NodeSettings::default().runtime.udf.watchdog(),
        Duration::from_secs(5)
    );
    let settings = NodeSettings::default()
        .with_overrides(&[override_of("udf-watchdog", "250ms")])
        .assured("a positive watchdog is a valid capability");
    assert_eq!(settings.runtime.udf.watchdog(), Duration::from_millis(250));
    assert_eq!(
        settings.runtime.engine_capabilities().udf_watchdog,
        Duration::from_millis(250)
    );
    let read = settings
        .with_overrides(&[])
        .assured("rendered settings read back");
    assert_eq!(read, settings);
}

#[test]
fn a_zero_udf_watchdog_refuses_the_node_before_startup() {
    let refused = NodeSettings::default()
        .with_overrides(&[override_of("udf-watchdog", "0s")])
        .err()
        .assured("no call returns within a zero watchdog");
    assert!(matches!(
        refused.current_context(),
        AppError::InvalidUdfSettings
    ));
    assert!(
        format!("{refused:#}").contains("udf-watchdog must be positive"),
        "{refused:#}"
    );
}

#[test]
fn a_udf_compile_budget_seed_reads_back_and_refuses_a_zero_budget() {
    let settings = NodeSettings::default()
        .with_overrides(&[override_of("udf-compile-budget", "30s")])
        .assured("a positive budget is a valid first-bootstrap seed");
    assert_eq!(
        settings.parameter_seeds.udf_compile_budget,
        Some(Duration::from_secs(30))
    );
    let read = settings
        .with_overrides(&[])
        .assured("rendered seeds read back");
    assert_eq!(read, settings);
    assert_eq!(
        NodeSettings::default().parameter_seeds.udf_compile_budget,
        None
    );

    let refused = NodeSettings::default()
        .with_overrides(&[override_of("udf-compile-budget", "0s")])
        .err()
        .assured("a zero budget would refuse every UDF");
    assert!(matches!(
        refused.current_context(),
        AppError::InvalidClusterParameters
    ));
    assert!(
        format!("{refused:#}").contains("udf-compile-budget must be greater than zero"),
        "{refused:#}"
    );
}

#[test]
fn trace_export_capabilities_render_and_read_back_and_refuse_a_batch_beyond_the_queue() {
    let settings = NodeSettings::default()
        .with_overrides(&[
            override_of("otel-trace-sample-ratio", "0.125"),
            override_of("otel-trace-export-timeout", "7s"),
            override_of("otel-trace-export-interval", "250ms"),
            override_of("otel-trace-queue-capacity", "4096"),
            override_of("otel-trace-export-batch-spans", "4096"),
        ])
        .assured("the overrides are valid trace export capabilities");
    let traces = settings.operational.traces;
    assert_eq!(
        traces.sample_ratio(),
        nervix_models::SampleRatio::new(0.125)
    );
    assert_eq!(traces.export_timeout(), Duration::from_secs(7));
    assert_eq!(traces.export_interval(), Duration::from_millis(250));
    assert_eq!(traces.queued_spans(), 4096);
    assert_eq!(traces.batched_spans(), 4096);
    let read = settings
        .with_overrides(&[])
        .assured("rendered settings read back");
    assert_eq!(read, settings);

    let refused = settings
        .with_overrides(&[override_of("otel-trace-queue-capacity", "2048")])
        .err()
        .assured("a 4096-span batch does not fit a 2048-span queue");
    assert!(matches!(
        refused.current_context(),
        AppError::InvalidTraceExportSettings
    ));
    assert_eq!(
        refused.downcast_ref::<nervix_models::NodeOperationalError>(),
        Some(&nervix_models::NodeOperationalError::TraceExportBatch {
            batch_setting: "otel-trace-export-batch-spans",
            batch: 4096,
            queue_setting: "otel-trace-queue-capacity",
            queue: 2048,
        })
    );
}

#[test]
fn web_console_pacing_renders_and_reads_back_and_refuses_a_ceiling_below_a_first_wait() {
    let settings = NodeSettings::default()
        .with_overrides(&[
            override_of("web-console-request-timeout", "45s"),
            override_of("web-console-connect-timeout", "4s"),
            override_of("web-console-retry-backoff", "300ms"),
            override_of("web-console-retry-max-backoff", "6s"),
            override_of("web-console-restoration-backoff", "2s"),
            override_of("web-console-restore-frame-capacity", "8"),
        ])
        .assured("the overrides are valid web console pacing");
    let web_console = settings.operational.web_console;
    assert_eq!(web_console.request_timeout(), Duration::from_secs(45));
    assert_eq!(web_console.connect_timeout(), Duration::from_secs(4));
    assert_eq!(web_console.retry_backoff(), Duration::from_millis(300));
    assert_eq!(web_console.retry_max_backoff(), Duration::from_secs(6));
    assert_eq!(web_console.restoration_backoff(), Duration::from_secs(2));
    assert_eq!(web_console.restore_frame_capacity().get(), 8);
    let read = settings
        .with_overrides(&[])
        .assured("rendered settings read back");
    assert_eq!(read, settings);

    let refused = NodeSettings::default()
        .with_overrides(&[override_of("web-console-retry-max-backoff", "100ms")])
        .err()
        .assured("a 100 ms ceiling is below the 250 ms first retry wait");
    assert!(matches!(
        refused.current_context(),
        AppError::InvalidWebConsoleSettings
    ));
    assert_eq!(
        refused.downcast_ref::<nervix_models::NodeOperationalError>(),
        Some(
            &nervix_models::NodeOperationalError::WebConsoleBackoffCeiling {
                setting: "web-console-retry-max-backoff",
                value: Duration::from_millis(100),
                first_setting: "web-console-retry-backoff",
                first: Duration::from_millis(250),
            }
        )
    );

    let refused = NodeSettings::default()
        .with_overrides(&[override_of("web-console-request-timeout", "1500us")])
        .err()
        .assured("a browser timer cannot keep part of a millisecond");
    assert!(matches!(
        refused.downcast_ref::<nervix_models::NodeOperationalError>(),
        Some(nervix_models::NodeOperationalError::WebConsoleWait {
            setting: "web-console-request-timeout",
            ..
        })
    ));
}

fn override_of(name: &str, value: &str) -> SettingOverride {
    SettingOverride {
        name: name.to_string(),
        value: value.to_string(),
    }
}
