//! A timestamp whose complete value space is exactly signed Unix nanoseconds.
//!
//! Layer: vocabulary.
//!
//! - **Owns.** The validated internal timestamp and its boundary conversions.
//! - **Depends on.** Chrono and serialization primitives.
//! - **Must not know.** Domain clocks, scheduling, runtime state or transport behavior.

use std::{fmt, str::FromStr, time::Duration};

use chrono::{DateTime, Utc};
use error_stack::Report;
use meticulous::{OptionExt as _, ResultExt as _};
use rkyv::{
    Archive, Archived, Deserialize as RkyvDeserialize, Place, Serialize as RkyvSerialize,
    rancor::Fallible,
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::Error as _};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TimestampError {
    #[error("invalid RFC 3339 timestamp: {0}")]
    InvalidRfc3339(#[source] chrono::ParseError),
    #[error("timestamp '{timestamp}' is outside the signed Unix-nanosecond range")]
    OutsideUnixNanosecondRange { timestamp: DateTime<Utc> },
    #[error("timestamp arithmetic leaves the signed Unix-nanosecond range")]
    ArithmeticOverflow,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
pub struct Timestamp(#[rkyv(with = UnixNanoseconds)] DateTime<Utc>);

impl Timestamp {
    pub fn now() -> Self {
        Self::try_from(Utc::now()).assured(
            "supported Nervix hosts keep their system clock inside the signed Unix-nanosecond \
             range",
        )
    }

    pub const fn as_datetime(&self) -> &DateTime<Utc> {
        &self.0
    }

    pub const fn into_datetime(self) -> DateTime<Utc> {
        self.0
    }

    pub fn from_unix_nanos(unix_nanos: i64) -> Self {
        Self(DateTime::from_timestamp_nanos(unix_nanos))
    }

    pub fn unix_nanos(self) -> i64 {
        self.0.timestamp_nanos_opt().assured(
            "Timestamp construction proves the value is inside the signed Unix-nanosecond range",
        )
    }

    pub fn checked_add(self, duration: Duration) -> Result<Self, Report<TimestampError>> {
        let available = i128::from(i64::MAX)
            .checked_sub(i128::from(self.unix_nanos()))
            .assured("subtracting two i64 values cannot overflow i128");
        let duration_nanos = duration.as_nanos();
        if duration_nanos
            > u128::try_from(available)
                .assured("i64::MAX is not below a valid signed Unix-nanosecond timestamp")
        {
            return Err(Report::new(TimestampError::ArithmeticOverflow));
        }
        let duration_nanos = i128::try_from(duration_nanos)
            .assured("the comparison above bounded the duration by u64::MAX");
        let unix_nanos = i128::from(self.unix_nanos())
            .checked_add(duration_nanos)
            .assured("the available-range comparison above proved the addition fits");
        let unix_nanos = i64::try_from(unix_nanos)
            .verified("the available-range comparison above bounded the sum by i64::MAX");
        Ok(Self::from_unix_nanos(unix_nanos))
    }

    pub fn checked_sub(self, duration: Duration) -> Result<Self, Report<TimestampError>> {
        let available = i128::from(self.unix_nanos())
            .checked_sub(i128::from(i64::MIN))
            .assured("subtracting two i64 values cannot overflow i128");
        let duration_nanos = duration.as_nanos();
        if duration_nanos
            > u128::try_from(available)
                .assured("a valid signed Unix-nanosecond timestamp is not below i64::MIN")
        {
            return Err(Report::new(TimestampError::ArithmeticOverflow));
        }
        let duration_nanos = i128::try_from(duration_nanos)
            .assured("the comparison above bounded the duration by u64::MAX");
        let unix_nanos = i128::from(self.unix_nanos())
            .checked_sub(duration_nanos)
            .assured("the available-range comparison above proved the subtraction fits");
        let unix_nanos = i64::try_from(unix_nanos)
            .verified("the available-range comparison above bounded the difference by i64::MIN");
        Ok(Self::from_unix_nanos(unix_nanos))
    }

    pub fn duration_since(self, earlier: Self) -> Option<Duration> {
        let nanos = i128::from(self.unix_nanos())
            .checked_sub(i128::from(earlier.unix_nanos()))
            .assured("subtracting two i64 values cannot overflow i128");
        if nanos < 0 {
            return None;
        }
        Some(Duration::from_nanos(u64::try_from(nanos).assured(
            "the difference between two i64 values is at most u64::MAX",
        )))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<DateTime<Utc>> for Timestamp {
    type Error = TimestampError;

    fn try_from(value: DateTime<Utc>) -> Result<Self, Self::Error> {
        if value.timestamp_nanos_opt().is_none() {
            return Err(TimestampError::OutsideUnixNanosecondRange { timestamp: value });
        }
        Ok(Self(value))
    }
}

impl FromStr for Timestamp {
    type Err = TimestampError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = DateTime::parse_from_rfc3339(value)
            .map_err(TimestampError::InvalidRfc3339)?
            .to_utc();
        Self::try_from(parsed)
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(value: Timestamp) -> Self {
        value.into_datetime()
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.0.timestamp_nanos_opt().ok_or_else(|| {
            S::Error::custom("Timestamp construction guarantees the signed Unix-nanosecond range")
        })?)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self::from_unix_nanos(i64::deserialize(deserializer)?))
    }
}

struct UnixNanoseconds;

impl ArchiveWith<DateTime<Utc>> for UnixNanoseconds {
    type Archived = Archived<i64>;
    type Resolver = <i64 as Archive>::Resolver;

    fn resolve_with(field: &DateTime<Utc>, resolver: Self::Resolver, out: Place<Self::Archived>) {
        field
            .timestamp_nanos_opt()
            .assured(
                "Timestamp construction proves the value is inside the signed Unix-nanosecond \
                 range",
            )
            .resolve(resolver, out);
    }
}

impl<S> SerializeWith<DateTime<Utc>, S> for UnixNanoseconds
where
    S: Fallible + ?Sized,
    i64: RkyvSerialize<S>,
{
    fn serialize_with(
        field: &DateTime<Utc>,
        serializer: &mut S,
    ) -> Result<Self::Resolver, S::Error> {
        let nanos = field.timestamp_nanos_opt().assured(
            "Timestamp construction proves the value is inside the signed Unix-nanosecond range",
        );
        RkyvSerialize::serialize(&nanos, serializer)
    }
}

impl<D> DeserializeWith<Archived<i64>, DateTime<Utc>, D> for UnixNanoseconds
where
    D: Fallible + ?Sized,
    Archived<i64>: RkyvDeserialize<i64, D>,
{
    fn deserialize_with(
        field: &Archived<i64>,
        deserializer: &mut D,
    ) -> Result<DateTime<Utc>, D::Error> {
        Ok(DateTime::from_timestamp_nanos(
            field.deserialize(deserializer)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{DateTime, TimeZone, Utc};
    use rkyv::{from_bytes, rancor::Error, to_bytes};

    use super::{Timestamp, TimestampError};

    #[test]
    fn serde_roundtrips_as_unix_nanoseconds() {
        let timestamp = Timestamp::try_from(Utc.with_ymd_and_hms(2026, 4, 21, 12, 34, 56).unwrap())
            .expect("fixture is inside the signed Unix-nanosecond range");

        let encoded = serde_json::to_string(&timestamp).expect("timestamp must serialize");
        let decoded: Timestamp =
            serde_json::from_str(&encoded).expect("timestamp must deserialize");

        assert_eq!(decoded, timestamp);
        assert_eq!(encoded, timestamp.unix_nanos().to_string());
    }

    #[test]
    fn rkyv_roundtrips_as_unix_nanoseconds() {
        let timestamp = Timestamp::try_from(Utc.with_ymd_and_hms(2026, 4, 21, 12, 34, 56).unwrap())
            .expect("fixture is inside the signed Unix-nanosecond range");

        let bytes = to_bytes::<Error>(&timestamp).expect("timestamp must archive");
        let decoded: Timestamp =
            from_bytes::<Timestamp, Error>(&bytes[..]).expect("timestamp must deserialize");

        assert_eq!(decoded, timestamp);
    }

    #[test]
    fn accepts_both_signed_unix_nanosecond_endpoints() {
        for (text, unix_nanos) in [
            ("1677-09-21T00:12:43.145224192Z", i64::MIN),
            ("2262-04-11T23:47:16.854775807Z", i64::MAX),
        ] {
            assert_eq!(
                Timestamp::from_unix_nanos(unix_nanos).unix_nanos(),
                unix_nanos
            );
            assert_eq!(
                text.parse::<Timestamp>()
                    .expect("endpoint is valid RFC 3339 and signed Unix nanoseconds")
                    .unix_nanos(),
                unix_nanos
            );
        }
    }

    #[test]
    fn rejects_datetime_immediately_outside_each_endpoint() {
        for value in [
            "1677-09-21T00:12:43.145224191Z",
            "2262-04-11T23:47:16.854775808Z",
        ] {
            let parsed = DateTime::parse_from_rfc3339(value)
                .expect("fixture is valid RFC 3339")
                .to_utc();
            assert!(matches!(
                Timestamp::try_from(parsed),
                Err(TimestampError::OutsideUnixNanosecondRange { .. })
            ));
        }
    }

    #[test]
    fn checked_arithmetic_rejects_both_range_overflows() {
        assert!(
            Timestamp::from_unix_nanos(i64::MAX)
                .checked_add(Duration::from_nanos(1))
                .is_err()
        );
        assert!(
            Timestamp::from_unix_nanos(i64::MIN)
                .checked_sub(Duration::from_nanos(1))
                .is_err()
        );
    }
}
