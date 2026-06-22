use std::fmt;

use chrono::{DateTime, Utc};
use rkyv::{
    Archive, Archived, Deserialize as RkyvDeserialize, Place, Serialize as RkyvSerialize,
    rancor::Fallible,
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::Error as _};

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
        Self(Utc::now())
    }

    pub const fn from_datetime(datetime: DateTime<Utc>) -> Self {
        Self(datetime)
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
        self.0
            .timestamp_nanos_opt()
            .expect("Timestamp must always be representable as unix nanoseconds")
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Self::from_datetime(value)
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
        serializer.serialize_i64(
            self.0
                .timestamp_nanos_opt()
                .ok_or_else(|| S::Error::custom("timestamp is out of unix nanosecond range"))?,
        )
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
            .expect("timestamp must always be representable as unix nanoseconds")
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
        let nanos = field
            .timestamp_nanos_opt()
            .expect("timestamp must always be representable as unix nanoseconds");
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
    use chrono::{TimeZone, Utc};
    use rkyv::{from_bytes, rancor::Error, to_bytes};

    use super::Timestamp;

    #[test]
    fn serde_roundtrips_as_unix_nanoseconds() {
        let timestamp =
            Timestamp::from_datetime(Utc.with_ymd_and_hms(2026, 4, 21, 12, 34, 56).unwrap());

        let encoded = serde_json::to_string(&timestamp).expect("timestamp must serialize");
        let decoded: Timestamp =
            serde_json::from_str(&encoded).expect("timestamp must deserialize");

        assert_eq!(decoded, timestamp);
        assert_eq!(encoded, timestamp.unix_nanos().to_string());
    }

    #[test]
    fn rkyv_roundtrips_as_unix_nanoseconds() {
        let timestamp =
            Timestamp::from_datetime(Utc.with_ymd_and_hms(2026, 4, 21, 12, 34, 56).unwrap());

        let bytes = to_bytes::<Error>(&timestamp).expect("timestamp must archive");
        let decoded: Timestamp =
            from_bytes::<Timestamp, Error>(&bytes[..]).expect("timestamp must deserialize");

        assert_eq!(decoded, timestamp);
    }
}
