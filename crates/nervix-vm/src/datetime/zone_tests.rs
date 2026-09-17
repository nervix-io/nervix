//! Tests for time zone resolution, offset lookups and local time resolution.
//!
//! Layer: test harness.
//!
//! - **Owns.** Checks that every zone of the bundled database resolves and keeps the guarantees the
//!   calendar kernels rely on, that written zones resolve exactly as documented, and that offsets,
//!   abbreviations and the instants of skipped and repeated local times agree with jiff's own
//!   lookups.
//! - **Depends on.** The zone module and jiff as the reference for offsets and transitions.
//! - **Must not know.** Arrow arrays, formats, or row errors.

use std::str::FromStr as _;

use jiff::{Timestamp, civil::date, tz::Offset};
use nervix_models::Timestamp as NervixTimestamp;

use super::{LocalInstants, OffsetStyle, ShortText, UnresolvedLocalTime, Zone, ZoneOffsets};
use crate::program::Disambiguation;

/// The IANA release the documentation names. A dependency update that bundles another release
/// changes local times for some zones, so it must update the Time Zones section of the expression
/// function reference together with this constant.
const DOCUMENTED_DATABASE_VERSION: &str = "2026c";

fn nanoseconds(instant: &str) -> i64 {
    NervixTimestamp::from_str(instant)
        .expect("test instants are RFC 3339 values inside the DATETIME range")
        .unix_nanos()
}

fn zone(written: &str) -> Zone {
    Zone::resolve(written).unwrap_or_else(|| panic!("{written} is a zone"))
}

#[test]
fn the_bundled_database_is_the_documented_release() {
    assert_eq!(jiff_tzdb::VERSION, Some(DOCUMENTED_DATABASE_VERSION));
}

#[test]
fn every_bundled_zone_resolves_with_bounded_transitions_and_abbreviations() {
    let first = Timestamp::from_nanosecond(i128::from(i64::MIN)).expect("in jiff's range");
    let last = Timestamp::from_nanosecond(i128::from(i64::MAX)).expect("in jiff's range");
    let two_days = 2 * 86_400;
    let mut zones = 0;
    for name in jiff_tzdb::available() {
        let resolved = Zone::resolve(name).unwrap_or_else(|| panic!("{name} resolves"));
        if name.eq_ignore_ascii_case("UTC") {
            assert_eq!(resolved, Zone::UTC);
            continue;
        }
        assert_eq!(resolved.to_string(), name);
        let super::ZoneRules::Named { rules, .. } = &resolved.0 else {
            panic!("{name} resolves to IANA rules");
        };
        let mut previous = rules.to_offset(first);
        let mut longest_abbreviation = rules.to_offset_info(first).abbreviation().len();
        for transition in rules.following(first) {
            if transition.timestamp() > last {
                break;
            }
            let shift = (transition.offset().seconds() - previous.seconds()).abs();
            assert!(
                shift < two_days,
                "{name} shifts local time by {shift} seconds at {}",
                transition.timestamp()
            );
            previous = transition.offset();
            longest_abbreviation = longest_abbreviation.max(transition.abbreviation().len());
        }
        assert_eq!(
            resolved.longest_abbreviation(),
            longest_abbreviation,
            "{name}"
        );
        zones += 1;
    }
    assert!(zones > 500, "the bundled database holds every IANA zone");
}

#[test]
fn written_zones_resolve_as_documented() {
    assert_eq!(zone("UTC"), Zone::UTC);
    assert_eq!(zone("utc"), Zone::UTC);
    assert_eq!(zone("UTC").to_string(), "UTC");

    assert_eq!(zone("+05:30").to_string(), "+05:30");
    assert_eq!(zone("-08:00").to_string(), "-08:00");
    assert_eq!(zone("+23:59").to_string(), "+23:59");
    assert_eq!(zone("+00:00").to_string(), "+00:00");
    assert_ne!(zone("+00:00"), Zone::UTC);

    assert_eq!(zone("america/new_york").to_string(), "America/New_York");
    assert_eq!(zone("America/New_York"), zone("AMERICA/NEW_YORK"));
    // A link keeps its own name, so formats that write the name write it as the call named it.
    assert_eq!(zone("US/Eastern").to_string(), "US/Eastern");
    assert_ne!(zone("US/Eastern"), zone("America/New_York"));
    assert_eq!(zone("Etc/UTC").to_string(), "Etc/UTC");

    for unknown in [
        "",
        "Mars/Olympus_Mons",
        "America/NewYork",
        "+24:00",
        "+05:60",
        "+5:30",
        "05:30",
        "+0530",
        "+05:30:00",
        "Z",
        " UTC",
        "local",
    ] {
        assert!(Zone::resolve(unknown).is_none(), "{unknown:?}");
    }
}

#[test]
fn rule_offsets_agree_with_jiff_at_and_around_every_transition() {
    for name in [
        "America/New_York",
        "Europe/Berlin",
        "Australia/Lord_Howe",
        "Pacific/Apia",
        "America/Havana",
        "Asia/Kolkata",
    ] {
        let resolved = zone(name);
        let super::ZoneRules::Named { rules, .. } = &resolved.0 else {
            panic!("{name} resolves to IANA rules");
        };
        let mut offsets = resolved.offsets();
        let start = Timestamp::from_nanosecond(i128::from(nanoseconds("1880-01-01T00:00:00Z")))
            .expect("in jiff's range");
        let end = Timestamp::from_nanosecond(i128::from(nanoseconds("2040-01-01T00:00:00Z")))
            .expect("in jiff's range");
        for transition in rules.following(start) {
            if transition.timestamp() > end {
                break;
            }
            let at = i64::try_from(transition.timestamp().as_nanosecond()).expect("fits i64");
            for instant in [
                at - 3_600_000_000_000,
                at - 1,
                at,
                at + 1,
                at + 86_400_000_000_000,
            ] {
                // Jiff reads a timestamp's seconds truncated toward zero, so before the epoch it is
                // asked about the whole second at or before the instant, which transitions never
                // split.
                let second = Timestamp::from_second(instant.div_euclid(1_000_000_000))
                    .expect("in jiff's range");
                assert_eq!(
                    offsets.offset_at(instant),
                    rules.to_offset(second),
                    "{name} at {instant}"
                );
                let abbreviation =
                    offsets.read_offset_and_abbreviation(instant, |_, text| text.to_string());
                assert_eq!(
                    abbreviation,
                    rules.to_offset_info(second).abbreviation(),
                    "{name} at {instant}"
                );
            }
            let ZoneOffsets::Rules(rule_offsets) = &mut offsets else {
                panic!("{name} looks offsets up in its rules");
            };
            let span = rule_offsets.span_at(at);
            assert_eq!(
                span.start,
                Some(i128::from(at)),
                "{name} span starts at its transition"
            );
            let span_before = rule_offsets.span_at(at - 1);
            assert!(
                span_before.start < Some(i128::from(at)),
                "{name} one nanosecond before a transition lies in the span before it"
            );
        }
    }
}

#[test]
fn constant_zones_show_one_offset_and_abbreviation() {
    for (written, seconds, abbreviation) in [
        ("UTC", 0, "UTC"),
        ("+05:30", 19_800, "+05:30"),
        ("-00:30", -1_800, "-00:30"),
    ] {
        let resolved = zone(written);
        assert!(matches!(resolved.offsets(), ZoneOffsets::Constant(_)));
        assert_eq!(resolved.longest_abbreviation(), abbreviation.len());
        let mut offsets = resolved.offsets();
        for instant in [i64::MIN, 0, i64::MAX] {
            let (offset, text) = offsets
                .read_offset_and_abbreviation(instant, |offset, text| (offset, text.to_string()));
            assert_eq!(offset.seconds(), seconds);
            assert_eq!(text, abbreviation);
        }
    }
    assert!(matches!(
        zone("Europe/Paris").offsets(),
        ZoneOffsets::Rules(_)
    ));
}

#[test]
fn local_times_resolve_to_their_instants_in_gaps_and_folds() {
    let new_york = zone("America/New_York");
    let unique = new_york.instants_of(date(2024, 7, 4).at(12, 30, 0, 0));
    assert_eq!(
        unique,
        LocalInstants::Unique(i128::from(nanoseconds("2024-07-04T16:30:00Z")))
    );

    let skipped = new_york.instants_of(date(2024, 3, 10).at(2, 30, 0, 0));
    assert_eq!(
        skipped,
        LocalInstants::Skipped {
            earlier: i128::from(nanoseconds("2024-03-10T06:30:00Z")),
            later: i128::from(nanoseconds("2024-03-10T07:30:00Z")),
        }
    );
    let repeated = new_york.instants_of(date(2024, 11, 3).at(1, 30, 0, 0));
    assert_eq!(
        repeated,
        LocalInstants::Repeated {
            earlier: i128::from(nanoseconds("2024-11-03T05:30:00Z")),
            later: i128::from(nanoseconds("2024-11-03T06:30:00Z")),
        }
    );

    let resolutions = [
        (
            skipped,
            Disambiguation::Compatible,
            Ok("2024-03-10T07:30:00Z"),
        ),
        (skipped, Disambiguation::Earlier, Ok("2024-03-10T06:30:00Z")),
        (skipped, Disambiguation::Later, Ok("2024-03-10T07:30:00Z")),
        (
            skipped,
            Disambiguation::Reject,
            Err(UnresolvedLocalTime::Skipped),
        ),
        (
            repeated,
            Disambiguation::Compatible,
            Ok("2024-11-03T05:30:00Z"),
        ),
        (
            repeated,
            Disambiguation::Earlier,
            Ok("2024-11-03T05:30:00Z"),
        ),
        (repeated, Disambiguation::Later, Ok("2024-11-03T06:30:00Z")),
        (
            repeated,
            Disambiguation::Reject,
            Err(UnresolvedLocalTime::Repeated),
        ),
        (unique, Disambiguation::Reject, Ok("2024-07-04T16:30:00Z")),
    ];
    for (instants, disambiguation, expected) in resolutions {
        let expected = expected.map(|instant| i128::from(nanoseconds(instant)));
        assert_eq!(
            instants.resolve(disambiguation),
            expected,
            "{instants:?} {disambiguation}"
        );
    }

    let fixed = zone("-08:00");
    assert_eq!(
        fixed.instants_of(date(2024, 3, 10).at(2, 30, 0, 0)),
        LocalInstants::Unique(i128::from(nanoseconds("2024-03-10T10:30:00Z")))
    );
}

#[test]
fn offsets_are_written_in_every_style() {
    let cases = [
        (19_800, OffsetStyle::Compact, "+0530"),
        (19_800, OffsetStyle::Colon, "+05:30"),
        (19_800, OffsetStyle::Seconds, "+05:30:00"),
        (0, OffsetStyle::Compact, "+0000"),
        (-28_800, OffsetStyle::Colon, "-08:00"),
        // New York's local mean time, 4 hours, 56 minutes and 2 seconds behind UTC.
        (-17_762, OffsetStyle::Compact, "-045602"),
        (-17_762, OffsetStyle::Colon, "-04:56:02"),
        (-17_762, OffsetStyle::Seconds, "-04:56:02"),
        (Offset::MAX.seconds(), OffsetStyle::Seconds, "+25:59:59"),
    ];
    for (seconds, style, expected) in cases {
        let text = ShortText::offset(seconds, style);
        assert_eq!(text.as_str(), expected);
        assert!(text.as_bytes().len() <= style.longest());
    }
}
