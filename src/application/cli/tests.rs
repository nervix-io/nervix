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
const SETTINGS_HEADINGS: [&str; 9] = [
    "Consensus",
    "Membership",
    "Scheduling",
    "Runtime state",
    "Transactions and commands",
    "Memory pressure",
    "Shutdown",
    "Resources",
    "Capacity",
];

/// Every setting the settings groups declare, by the name its definition gives it.
const SETTING_NAMES: [&str; 29] = [
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
    "resource-max-archive-bytes",
    "resource-max-extracted-bytes",
    "resource-max-file-count",
    "memory-limit",
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
        ])
        .assured("the overrides are valid settings");

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
