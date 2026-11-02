//! The time a Kafka record carries, read from the `TIMESTAMP` its record's envelope sets.
//!
//! Layer: test harness.
//! - **Owns.** The create-time conversion's contract: the millisecond that contains a timestamp,
//!   the Unix epoch's first millisecond as the earliest record time, the exact round trip of every
//!   later millisecond, and the refusal of every earlier timestamp.
//! - **Depends on.** The Kafka emitter's create time and Nervix timestamps.
//! - **Must not know.** librdkafka, brokers or how a record is published.

use nervix_models::Timestamp;

use super::{KafkaCreateTime, KafkaCreateTimeError};

/// Nanoseconds in one millisecond, the unit a Kafka record time counts in.
const NANOS_PER_MILLI: i64 = 1_000_000;

fn at(rfc3339: &str) -> Timestamp {
    rfc3339
        .parse()
        .expect("every case writes an RFC 3339 instant")
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
        let create_time = KafkaCreateTime::try_from(at(written))
            .expect("a time after the epoch's first millisecond is a record time");
        assert_eq!(create_time.unix_millis, unix_millis, "{written}");
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
            KafkaCreateTime::try_from(timestamp),
            Err(KafkaCreateTimeError::BeforeFirstMillisecond { timestamp }),
            "{written}"
        );
    }
}

#[test]
fn the_refusal_names_the_timestamp_and_the_earliest_record_time() {
    let refusal = KafkaCreateTime::try_from(at("1969-12-31T23:59:59.999Z"))
        .expect_err("a time before the epoch is no record time");

    assert_eq!(
        refusal.to_string(),
        "kafka record timestamp 1969-12-31T23:59:59.999Z precedes 1970-01-01T00:00:00.001Z, the \
         earliest time a kafka record carries"
    );
}

#[test]
fn the_latest_timestamp_is_a_record_time() {
    let create_time = KafkaCreateTime::try_from(Timestamp::from_unix_nanos(i64::MAX))
        .expect("the latest timestamp is long after the epoch");

    assert_eq!(create_time.unix_millis, 9_223_372_036_854);
    assert_eq!(
        create_time.timestamp(),
        at("2262-04-11T23:47:16.854Z"),
        "the latest record time is the millisecond that contains the latest timestamp"
    );
}

/// A generated timestamp: near the Unix epoch, where the earliest record time lies, or anywhere in
/// the Unix-nanosecond range.
#[derive(Debug, bolero::TypeGenerator)]
enum GeneratedTimestamp {
    /// Less than three milliseconds from the Unix epoch, on either side of it.
    NearEpoch(i32),
    /// Any signed Unix nanosecond, both ends of the range included.
    Anywhere(i64),
}

impl GeneratedTimestamp {
    fn unix_nanos(&self) -> i64 {
        match self {
            Self::NearEpoch(offset) => i64::from(*offset) % (3 * NANOS_PER_MILLI),
            Self::Anywhere(unix_nanos) => *unix_nanos,
        }
    }
}

/// A timestamp whose millisecond is after the Unix epoch becomes the create time of exactly that
/// millisecond, which reads back as the timestamp floored to it, and the record time of every such
/// millisecond round-trips unchanged; every earlier timestamp is refused, naming that timestamp.
#[test]
fn bolero_create_times_are_the_millisecond_containing_their_timestamp() {
    bolero::check!()
        .with_iterations(256)
        .with_max_len(64)
        .with_type::<GeneratedTimestamp>()
        .for_each(|generated| {
            let unix_nanos = generated.unix_nanos();
            let timestamp = Timestamp::from_unix_nanos(unix_nanos);
            let containing_millis = unix_nanos.div_euclid(NANOS_PER_MILLI);
            match KafkaCreateTime::try_from(timestamp) {
                Ok(create_time) => {
                    assert!(
                        containing_millis >= 1,
                        "{timestamp:?} is before the epoch's first millisecond"
                    );
                    assert_eq!(create_time.unix_millis, containing_millis);
                    let floored = create_time.timestamp();
                    let finer_part = unix_nanos
                        .checked_sub(floored.unix_nanos())
                        .expect("both instants are after the epoch");
                    assert!(
                        (0..NANOS_PER_MILLI).contains(&finer_part),
                        "{floored:?} is not the millisecond that contains {timestamp:?}"
                    );
                    assert_eq!(KafkaCreateTime::try_from(floored), Ok(create_time));
                }
                Err(KafkaCreateTimeError::BeforeFirstMillisecond { timestamp: refused }) => {
                    assert!(
                        containing_millis < 1,
                        "{timestamp:?} is a record time but was refused"
                    );
                    assert_eq!(refused, timestamp);
                }
            }
        });
}
