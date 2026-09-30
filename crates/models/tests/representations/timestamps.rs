//! A timestamp keeps its instant through every representation, and RFC 3339 text reads as the
//! instant it names or fails with a typed error.
//!
//! The valid domain is every signed Unix nanosecond. The nanosecond integer, the serde form, the
//! archived form, chrono's `DateTime<Utc>` and the RFC 3339 text `Timestamp::to_rfc3339` writes
//! each hold the whole domain exactly. Parsing RFC 3339 text is a projection: an offset is
//! converted to UTC, fractional digits past the ninth are truncated, and a leap second reads as
//! the instant one second past the second before it.

use chrono::{DateTime, Utc};
use nervix_arbitrary::{Arbitrary, Domain, Entropy};
use nervix_models::{Timestamp, TimestampError};

const NANOS_PER_SECOND: i128 = 1_000_000_000;

#[test]
fn bolero_timestamps_round_trip_through_every_representation() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let nanos = arbitrary.entropy().any_i64();
            let timestamp = Timestamp::from_unix_nanos(nanos);
            assert_eq!(timestamp.unix_nanos(), nanos);

            let text = timestamp.to_rfc3339();
            assert_eq!(text.parse::<Timestamp>().ok(), Some(timestamp), "{text}");

            let json = serde_json::to_string(&timestamp).expect("a timestamp has a JSON form");
            assert_eq!(json, nanos.to_string());
            let decoded: Timestamp = serde_json::from_str(&json).expect("the JSON reads back");
            assert_eq!(decoded, timestamp);

            assert_eq!(crate::archive_round_trip!(&timestamp, Timestamp), timestamp);

            let datetime = DateTime::<Utc>::from(timestamp);
            assert_eq!(Timestamp::try_from(datetime).ok(), Some(timestamp));
        });
}

/// The pieces of one RFC 3339 timestamp, as generated before they are written out.
#[derive(Debug)]
struct WrittenTimestamp {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    fraction: String,
    offset_minutes: Option<i64>,
    separator: char,
}

impl WrittenTimestamp {
    fn generate(entropy: &mut Entropy<'_>) -> Self {
        let year = i64::try_from(entropy.boundary_biased(1600..=2300)).expect("a year fits in i64");
        let month = i64::try_from(entropy.between(1..=12)).expect("a month fits in i64");
        // Up to 31 in every month, so an invalid date is generated as often as a month ends.
        let day = i64::try_from(entropy.boundary_biased(1..=31)).expect("a day fits in i64");
        let hour = i64::try_from(entropy.boundary_biased(0..=23)).expect("an hour fits in i64");
        let minute = i64::try_from(entropy.boundary_biased(0..=59)).expect("a minute fits in i64");
        // Sixty is a leap second.
        let second = i64::try_from(entropy.boundary_biased(0..=60)).expect("a second fits in i64");
        let digits = entropy.boundary_biased(0..=12);
        let mut fraction = String::new();
        for _ in 0..digits {
            fraction.push(char::from(
                b'0' + u8::try_from(entropy.up_to(9)).expect("a digit fits in u8"),
            ));
        }
        let offset_minutes = if entropy.flag() {
            None
        } else {
            let magnitude = i64::try_from(entropy.boundary_biased(0..=(23 * 60 + 59)))
                .expect("an offset fits in i64");
            Some(if entropy.flag() {
                magnitude
            } else {
                -magnitude
            })
        };
        let separator = entropy.pick(['T', 't']);
        Self {
            year,
            month,
            day,
            hour,
            minute,
            second,
            fraction,
            offset_minutes,
            separator,
        }
    }

    fn text(&self) -> String {
        let fraction = if self.fraction.is_empty() {
            String::new()
        } else {
            format!(".{}", self.fraction)
        };
        let offset = match self.offset_minutes {
            None => "Z".to_string(),
            Some(minutes) => {
                let sign = if minutes < 0 { '-' } else { '+' };
                let magnitude = minutes.abs();
                format!("{sign}{:02}:{:02}", magnitude / 60, magnitude % 60)
            }
        };
        format!(
            "{:04}-{:02}-{:02}{}{:02}:{:02}:{:02}{fraction}{offset}",
            self.year, self.month, self.day, self.separator, self.hour, self.minute, self.second
        )
    }

    fn is_valid_date(&self) -> bool {
        let leap = (self.year % 4 == 0 && self.year % 100 != 0) || self.year % 400 == 0;
        let days = match self.month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ if leap => 29,
            _ => 28,
        };
        self.day <= days
    }

    /// The instant this text names, in Unix nanoseconds, with the fraction truncated to nine
    /// digits and a leap second counted as the second after the one before it.
    fn instant_nanos(&self) -> i128 {
        let days = i128::from(days_from_civil(self.year, self.month, self.day));
        let seconds = days * 86_400
            + i128::from(self.hour) * 3_600
            + i128::from(self.minute) * 60
            + i128::from(self.second)
            - i128::from(self.offset_minutes.unwrap_or_default()) * 60;
        let mut nanos = String::from(&self.fraction[..self.fraction.len().min(9)]);
        while nanos.len() < 9 {
            nanos.push('0');
        }
        seconds * NANOS_PER_SECOND + nanos.parse::<i128>().expect("nine digits read as a number")
    }
}

/// Days from 1970-01-01 to the proleptic Gregorian date, by Howard Hinnant's civil-date algorithm.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_from_march = (month + 9) % 12;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[test]
fn bolero_timestamp_text_reads_as_its_instant_or_a_typed_error() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .for_each(|bytes: &[u8]| {
            let mut arbitrary = Arbitrary::new(bytes, Domain::Vocabulary);
            let written = WrittenTimestamp::generate(arbitrary.entropy());
            let text = written.text();
            let parsed = text.parse::<Timestamp>();
            if !written.is_valid_date() {
                assert!(
                    matches!(parsed, Err(TimestampError::InvalidRfc3339(_))),
                    "{text} names no date, yet read as {parsed:?}"
                );
                return;
            }
            let nanos = written.instant_nanos();
            match i64::try_from(nanos) {
                Ok(nanos) => {
                    let timestamp = parsed.unwrap_or_else(|error| {
                        panic!("{text} names an instant in range: {error}")
                    });
                    assert_eq!(timestamp, Timestamp::from_unix_nanos(nanos), "{text}");
                    let canonical = timestamp.to_rfc3339();
                    assert_eq!(canonical.parse::<Timestamp>().ok(), Some(timestamp));
                }
                Err(_) => assert!(
                    matches!(
                        parsed,
                        Err(TimestampError::OutsideUnixNanosecondRange { .. })
                    ),
                    "{text} is outside the nanosecond range, yet read as {parsed:?}"
                ),
            }
        });
}
