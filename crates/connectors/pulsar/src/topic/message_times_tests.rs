//! The times a Pulsar message carries, read from the record that sets them.
//!
//! Layer: test harness.
//! - **Owns.** The contract of a message's times: the millisecond that contains each, the Unix
//!   epoch's first millisecond as the earliest event time, the batch's execution time as what a
//!   delay is measured from, the exact round trip of every whole millisecond, and the refusal of
//!   a time a message cannot carry.
//! - **Depends on.** The Pulsar emitter's message times, records and Nervix timestamps.
//! - **Must not know.** The Pulsar driver, brokers or how a message is published.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use std::time::Duration;

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{EmitterRecord, EmitterRecordId, RecordEnvelope};
use nervix_models::Timestamp;

use super::message_times::{PulsarEventTime, PulsarMessageTimeError, PulsarMessageTimes};

/// Nanoseconds in one millisecond, the unit a Pulsar message counts its times in.
const NANOS_PER_MILLI: i64 = 1_000_000;

fn at(rfc3339: &str) -> Timestamp {
    rfc3339
        .parse()
        .assured("every case writes an RFC 3339 instant")
}

/// A record of a batch that executed at `execution_time`, which writes `timestamp` and
/// `deliver_after`.
fn record(
    execution_time: Timestamp,
    timestamp: Option<Timestamp>,
    deliver_after: Option<Duration>,
) -> EmitterRecord {
    let record = EmitterRecord::new(
        EmitterRecordId::new(0),
        None,
        Vec::new(),
        Vec::new(),
        execution_time,
    );
    record.with_envelope(RecordEnvelope {
        timestamp,
        deliver_after,
        ..RecordEnvelope::default()
    })
}

#[test]
fn a_timestamp_is_written_as_the_millisecond_that_contains_it() {
    for (written, unix_millis) in [
        ("2001-02-03T04:05:06.789Z", 981_173_106_789),
        ("2001-02-03T04:05:06.789000001Z", 981_173_106_789),
        ("2001-02-03T04:05:06.789999999Z", 981_173_106_789),
        ("1970-01-01T00:00:00.001Z", 1),
        ("1970-01-01T00:00:00.001999999Z", 1),
    ] {
        let times = PulsarMessageTimes::of(&record(at(written), Some(at(written)), None))
            .assured("a time after the epoch's first millisecond is an event time");
        assert_eq!(times.event_time, Some(unix_millis), "{written}");
        assert_eq!(times.deliver_at_time, None, "{written}");
    }
}

#[test]
fn a_timestamp_whose_millisecond_is_not_after_the_epoch_is_refused() {
    for written in [
        "1970-01-01T00:00:00Z",
        "1970-01-01T00:00:00.000999999Z",
        "1969-12-31T23:59:59.999Z",
        "1677-09-21T00:12:43.145224192Z",
    ] {
        let timestamp = at(written);
        assert_eq!(
            PulsarEventTime::try_from(timestamp),
            Err(PulsarMessageTimeError::EventTimeBeforeFirstMillisecond { timestamp }),
            "{written}"
        );
    }
}

#[test]
fn a_refused_timestamp_names_itself_and_the_earliest_event_time() {
    let refusal = PulsarEventTime::try_from(at("1969-12-31T23:59:59.999Z"))
        .expect_err("a time before the epoch is no event time");

    assert_eq!(
        refusal.to_string(),
        "message TIMESTAMP 1969-12-31T23:59:59.999Z precedes 1970-01-01T00:00:00.001Z, the \
         earliest event time a PULSAR message carries"
    );
}

#[test]
fn the_latest_timestamp_is_an_event_time() {
    let latest = Timestamp::from_unix_nanos(i64::MAX);
    let times = PulsarMessageTimes::of(&record(latest, Some(latest), None))
        .assured("the latest timestamp is long after the epoch");

    assert_eq!(times.event_time, Some(9_223_372_036_854));
}

#[test]
fn a_message_is_due_the_delay_after_its_batchs_execution_time() {
    let execution_time = at("2001-02-03T04:05:06.789Z");
    let times = PulsarMessageTimes::of(&record(execution_time, None, Some(Duration::from_secs(5))))
        .assured("the delay leaves the timestamp range nowhere near here");

    assert_eq!(times.event_time, None);
    assert_eq!(times.deliver_at_time, Some(981_173_111_789));
}

#[test]
fn a_delivery_time_is_the_millisecond_that_contains_it() {
    let execution_time = at("2001-02-03T04:05:06.789000400Z");
    let times = PulsarMessageTimes::of(&record(
        execution_time,
        None,
        Some(Duration::from_micros(1_500)),
    ))
    .assured("the delay leaves the timestamp range nowhere near here");

    assert_eq!(times.deliver_at_time, Some(981_173_106_790));
}

/// A paced domain's logical time may precede the Unix epoch. Such a message's time has passed by
/// any broker's clock, which is a time the field holds rather than a refusal.
#[test]
fn a_delivery_time_before_the_epoch_is_written_as_a_time_that_has_passed() {
    let times = PulsarMessageTimes::of(&record(
        at("1969-12-31T23:59:59.000Z"),
        None,
        Some(Duration::from_millis(1)),
    ))
    .assured("the delay leaves the timestamp range nowhere near here");

    assert_eq!(times.deliver_at_time, Some(-999));
}

#[test]
fn a_delivery_time_past_the_last_timestamp_is_refused_with_its_execution_time_and_delay() {
    let execution_time = Timestamp::from_unix_nanos(i64::MAX);
    let deliver_after = Duration::from_nanos(1);
    let refusal = PulsarMessageTimes::of(&record(execution_time, None, Some(deliver_after)))
        .expect_err("no timestamp follows the last one");

    assert_eq!(
        refusal,
        PulsarMessageTimeError::DeliveryTimeOutOfRange {
            execution_time,
            deliver_after,
        }
    );
    assert_eq!(
        refusal.to_string(),
        "DELIVER AFTER 1ns past the execution time 2262-04-11T23:47:16.854775807Z is later than \
         any timestamp"
    );
}

#[test]
fn a_record_without_either_clause_carries_neither_time() {
    let times = PulsarMessageTimes::of(&record(Timestamp::from_unix_nanos(i64::MIN), None, None))
        .assured("a record that sets no time asks for none");

    assert_eq!(times, PulsarMessageTimes::default());
}

/// A generated timestamp: near the Unix epoch, where the earliest event time lies, near the last
/// timestamp, where a delay leaves the range, or anywhere in the Unix-nanosecond range.
#[derive(Debug, bolero::TypeGenerator)]
enum GeneratedTimestamp {
    /// Less than three milliseconds from the Unix epoch, on either side of it.
    NearEpoch(i32),
    /// At most `u32::MAX` nanoseconds before the last timestamp.
    NearLast(u32),
    /// Any signed Unix nanosecond, both ends of the range included.
    Anywhere(i64),
}

impl GeneratedTimestamp {
    fn unix_nanos(&self) -> i64 {
        match self {
            Self::NearEpoch(offset) => i64::from(*offset) % (3 * NANOS_PER_MILLI),
            Self::NearLast(offset) => i64::MAX - i64::from(*offset),
            Self::Anywhere(unix_nanos) => *unix_nanos,
        }
    }
}

/// What a generated record sets, and the execution time of its batch.
#[derive(Debug, bolero::TypeGenerator)]
struct GeneratedRecord {
    execution_time: GeneratedTimestamp,
    timestamp: Option<GeneratedTimestamp>,
    deliver_after_nanos: Option<u64>,
}

impl GeneratedRecord {
    fn record(&self) -> EmitterRecord {
        let timestamp = self
            .timestamp
            .as_ref()
            .map(|timestamp| Timestamp::from_unix_nanos(timestamp.unix_nanos()));
        record(
            Timestamp::from_unix_nanos(self.execution_time.unix_nanos()),
            timestamp,
            self.deliver_after_nanos.map(Duration::from_nanos),
        )
    }

    /// The Unix millisecond that contains `unix_nanos`, measured without the conversion under
    /// test.
    fn containing_millisecond(unix_nanos: i128) -> i128 {
        unix_nanos.div_euclid(i128::from(NANOS_PER_MILLI))
    }

    /// The event time this record's message carries, or the refusal of its timestamp.
    fn expected_event_time(&self) -> Result<Option<u64>, PulsarMessageTimeError> {
        let Some(timestamp) = &self.timestamp else {
            return Ok(None);
        };
        let unix_nanos = timestamp.unix_nanos();
        let millisecond = Self::containing_millisecond(i128::from(unix_nanos));
        if millisecond < 1 {
            return Err(PulsarMessageTimeError::EventTimeBeforeFirstMillisecond {
                timestamp: Timestamp::from_unix_nanos(unix_nanos),
            });
        }
        let event_time = u64::try_from(millisecond).assured("the millisecond is positive");
        Ok(Some(event_time))
    }

    /// The delivery time this record's message carries, or the refusal of its delay.
    fn expected_deliver_at_time(&self) -> Result<Option<i64>, PulsarMessageTimeError> {
        let Some(deliver_after_nanos) = self.deliver_after_nanos else {
            return Ok(None);
        };
        let execution_time = self.execution_time.unix_nanos();
        let due = i128::from(execution_time)
            .checked_add(i128::from(deliver_after_nanos))
            .assured("two values of 64 bits fit 128");
        if due > i128::from(i64::MAX) {
            return Err(PulsarMessageTimeError::DeliveryTimeOutOfRange {
                execution_time: Timestamp::from_unix_nanos(execution_time),
                deliver_after: Duration::from_nanos(deliver_after_nanos),
            });
        }
        let deliver_at = i64::try_from(Self::containing_millisecond(due))
            .assured("a millisecond of the timestamp range fits 64 bits");
        Ok(Some(deliver_at))
    }

    fn expected(&self) -> Result<PulsarMessageTimes, PulsarMessageTimeError> {
        Ok(PulsarMessageTimes {
            event_time: self.expected_event_time()?,
            deliver_at_time: self.expected_deliver_at_time()?,
        })
    }
}

/// A message carries exactly the milliseconds that contain what its record sets: an event time
/// after the Unix epoch's first millisecond, and a delivery time measured from its batch's
/// execution time. Every other record is refused, naming what it set, and a resent record carries
/// the times of its first attempt. An event time reads back as the millisecond it was written
/// from, the exact round trip of a whole-millisecond timestamp.
#[test]
fn bolero_message_times_are_the_milliseconds_containing_what_their_record_sets() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .with_type::<GeneratedRecord>()
        .for_each(|generated| {
            let record = generated.record();
            let times = PulsarMessageTimes::of(&record);

            assert_eq!(times, generated.expected());
            assert_eq!(
                PulsarMessageTimes::of(&record),
                times,
                "every attempt at a record writes the same times"
            );
            let Ok(PulsarMessageTimes {
                event_time: Some(event_time),
                ..
            }) = times
            else {
                return;
            };
            let unix_nanos = i64::try_from(event_time)
                .assured("an event time is a millisecond of a signed Unix-nanosecond timestamp")
                .checked_mul(NANOS_PER_MILLI)
                .assured("an event time is a millisecond of a signed Unix-nanosecond timestamp");
            let whole_millisecond = Timestamp::from_unix_nanos(unix_nanos);
            let read_back = PulsarMessageTimes::of(&self::record(
                whole_millisecond,
                Some(whole_millisecond),
                None,
            ));
            assert_eq!(
                read_back,
                Ok(PulsarMessageTimes {
                    event_time: Some(event_time),
                    deliver_at_time: None,
                })
            );
        });
}
