//! Server command-line groups, the settings they convert into and the forms they render.
//!
//! Test harness outside the product layer order.
//! - **Owns.** Assertions about the options groups, their names, headings and defaults, and the
//!   rendering of settings back into options.
//! - **Depends on.** The command-line module under test and the application test fixtures.
//! - **Must not know.** Production ownership beyond the module under test.

use std::{collections::BTreeSet, time::Duration};

use clap::CommandFactory as _;
use nervix_consensus::RaftTimingError;
use ubyte::ByteUnit;

use super::*;
use crate::application::{
    ShutdownSettingsError,
    cli_values::{HumanBytes, HumanDuration},
    test_fixtures::test_args,
};

/// The headings of the settings areas, in the order help lists them.
const SETTINGS_HEADINGS: [&str; 17] = [
    "Databases",
    "Consensus capacities",
    "DNS",
    "Interconnect",
    "Connector host",
    "Expression engine",
    "Consensus",
    "Membership",
    "Scheduling",
    "Runtime state",
    "Transactions and commands",
    "Memory pressure",
    "Shutdown",
    "Resources",
    "Capacity",
    "Execution",
    "Listeners",
];

/// Every setting the settings groups declare, by the name its definition gives it.
const SETTING_NAMES: [&str; 150] = [
    "connector-retry-multiplier",
    "connector-retry-jitter-percent",
    "kafka-domain-assignment-retry",
    "kafka-partition-watch-interval",
    "syslog-intake-queue-capacity",
    "syslog-read-chunk-bytes",
    "wasm-stack-bytes",
    "wasm-memory-reservation-bytes",
    "vm-inline-row-limit",
    "vm-regex-search-cache-bytes",
    "vm-dynamic-pattern-cache-capacity",
    "vm-pattern-sets-per-batch",
    "database-cache-bytes",
    "database-journal-bytes",
    "database-workers",
    "raft-database-cache-bytes",
    "raft-database-journal-bytes",
    "raft-database-workers",
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
    "drain-timeout",
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
    "grpc-header-bytes",
    "grpc-concurrency",
    "grpc-request-timeout",
    "grpc-max-streams",
    "grpc-keepalive-interval",
    "grpc-keepalive-timeout",
    "listener-tcp-keepalive-time",
    "listener-tcp-keepalive-interval",
    "listener-tcp-keepalive-retries",
];

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
fn a_drain_timeout_above_the_shutdown_timeout_refuses_the_node() {
    let refused = Application::try_from(test_args(&["--drain-timeout", "1m"]))
        .err()
        .assured("a one-minute drain does not fit a fifty-second shutdown");

    assert!(matches!(
        refused.current_context(),
        AppError::InvalidShutdownTimeouts
    ));
    assert_eq!(
        refused.downcast_ref::<ShutdownSettingsError>(),
        Some(&ShutdownSettingsError::DrainExceedsShutdown {
            drain_timeout: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(50),
        })
    );
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
fn a_named_setting_that_breaks_a_relationship_is_refused_like_an_option() {
    let refused = NodeSettings::default()
        .with_overrides(&[override_of("drain-timeout", "2m")])
        .err()
        .assured("a two-minute drain does not fit a fifty-second shutdown");

    assert!(matches!(
        refused.current_context(),
        AppError::InvalidShutdownTimeouts
    ));
}

fn override_of(name: &str, value: &str) -> SettingOverride {
    SettingOverride {
        name: name.to_string(),
        value: value.to_string(),
    }
}
