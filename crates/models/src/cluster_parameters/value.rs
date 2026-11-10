//! Compact validated storage for complete cluster parameter values.
//!
//! Layer: vocabulary.
//! - **Owns.** Lossless inline duration storage under the parameter's existing validated range.
//! - **Depends on.** Setting definitions, semantic values and current representation codecs.
//! - **Must not know.** Parsing, consensus commands, clocks or runtime installation.

use std::{fmt, marker::PhantomData, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use rkyv::{Archive, Deserialize, Serialize};

use crate::SettingDefinition;

pub trait ParameterValue: Copy {
    type Stored: Copy + fmt::Debug + PartialEq + Eq;
    fn store(self) -> Self::Stored;
    fn read(stored: Self::Stored) -> Self;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize)]
pub struct DurationParts {
    seconds: u32,
    nanos: u32,
}

impl ParameterValue for Duration {
    type Stored = DurationParts;
    fn store(self) -> DurationParts {
        DurationParts {
            seconds: u32::try_from(self.as_secs())
                .assured("the definition validates the u32 seconds range"),
            nanos: self.subsec_nanos(),
        }
    }
    fn read(stored: DurationParts) -> Self {
        Duration::new(u64::from(stored.seconds), stored.nanos)
    }
}

macro_rules! unchanged {
    ($($value:ty),+ $(,)?) => {$ (
        impl ParameterValue for $value {
            type Stored = Self;
            fn store(self) -> Self { self }
            fn read(stored: Self) -> Self { stored }
        }
    )+};
}
unchanged!(
    u64,
    nervix_crypto_policy::TlsCipherSuites,
    nervix_crypto_policy::TlsKeyExchangeGroups
);

pub struct ParameterSetting<T: SettingDefinition>
where
    T::Value: ParameterValue,
{
    stored: <T::Value as ParameterValue>::Stored,
    marker: PhantomData<T>,
}
impl<T: SettingDefinition> Copy for ParameterSetting<T> where T::Value: ParameterValue {}
impl<T: SettingDefinition> Clone for ParameterSetting<T>
where
    T::Value: ParameterValue,
{
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: SettingDefinition> PartialEq for ParameterSetting<T>
where
    T::Value: ParameterValue,
{
    fn eq(&self, other: &Self) -> bool {
        self.stored == other.stored
    }
}
impl<T: SettingDefinition> Eq for ParameterSetting<T> where T::Value: ParameterValue {}
impl<T: SettingDefinition> fmt::Debug for ParameterSetting<T>
where
    T::Value: ParameterValue,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Setting")
            .field("name", &T::NAME)
            .finish_non_exhaustive()
    }
}

impl<T: SettingDefinition<Context = ()>> ParameterSetting<T>
where
    T::Value: ParameterValue,
{
    pub(super) fn new(value: T::Value, context: &()) -> Result<Self, Report<T::Error>> {
        T::validate(&value, context)?;
        Ok(Self {
            stored: value.store(),
            marker: PhantomData,
        })
    }
    pub(super) fn resolve(value: Option<T::Value>, context: &()) -> Result<Self, Report<T::Error>> {
        Self::new(value.unwrap_or_else(|| T::default_value(context)), context)
    }
    pub(crate) fn value(&self) -> T::Value {
        T::Value::read(self.stored)
    }
    pub(super) fn stored(&self) -> &<T::Value as ParameterValue>::Stored {
        &self.stored
    }
    pub(super) fn from_stored(
        stored: <T::Value as ParameterValue>::Stored,
    ) -> Result<Self, Report<T::Error>> {
        Self::new(T::Value::read(stored), &())
    }
}

impl<D> Deserialize<DurationParts, D> for ArchivedDurationParts
where
    D: rkyv::rancor::Fallible + ?Sized,
    D::Error: rkyv::rancor::Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<DurationParts, D::Error> {
        use rkyv::rancor::Source as _;
        let seconds = self.seconds.deserialize(deserializer)?;
        let nanos = self.nanos.deserialize(deserializer)?;
        if nanos >= 1_000_000_000 {
            return Err(D::Error::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "duration nanoseconds exceed their coordinate",
            )));
        }
        Ok(DurationParts { seconds, nanos })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClusterParameterSeedValues, ClusterParameterValues,
        cluster_parameters::LicensingRequestTimeout,
    };

    #[test]
    fn durations_keep_subsecond_precision_and_the_entire_validated_range_in_eight_bytes() {
        assert_eq!(
            std::mem::size_of::<ParameterSetting<LicensingRequestTimeout>>(),
            8
        );
        for value in [
            Duration::from_nanos(1),
            Duration::new(100, 999_999_999),
            Duration::from_secs(u64::from(u32::MAX)),
        ] {
            let setting = ParameterSetting::<LicensingRequestTimeout>::new(value, &())
                .assured("current validated range");
            assert_eq!(setting.value(), value);
        }
        assert!(ParameterSetting::<LicensingRequestTimeout>::new(Duration::ZERO, &()).is_err());
        assert!(
            ParameterSetting::<LicensingRequestTimeout>::new(
                Duration::new(u64::from(u32::MAX), 1),
                &()
            )
            .is_err()
        );
    }
    #[test]
    fn fractional_and_maximum_durations_keep_public_values_and_complete_encodings() {
        assert_eq!(std::mem::size_of::<DurationParts>(), 8);
        assert_eq!(std::mem::size_of::<rkyv::Archived<DurationParts>>(), 8);
        for duration in [
            Duration::from_nanos(1),
            Duration::new(1, 999_999_999),
            Duration::new(u64::from(u32::MAX) - 1, 999_999_999),
            super::super::MAX_PARAMETER_DURATION,
        ] {
            let values = ClusterParameterValues::resolve(ClusterParameterSeedValues {
                licensing_request_timeout: Some(duration),
                licensing_retry_backoff_max: Some(duration),
                licensing_stream_idle_timeout: Some(duration),
                node_unavailability_timeout: Some(duration),
                domain_drain_timeout: Some(duration),
                entity_gate_deadline: Some(duration),
                transaction_idle_timeout: Some(duration),
                transaction_tombstone_retention: Some(duration),
                command_retry_validity: Some(duration),
                state_snapshot_interval: Some(duration),
                ..Default::default()
            })
            .assured("each duration is positive and within the declared bound");
            for actual in [
                values.licensing_request_timeout(),
                values.licensing_retry_backoff_max(),
                values.licensing_stream_idle_timeout(),
                values.node_unavailability_timeout(),
                values.domain_drain_timeout(),
                values.entity_gate_deadline(),
                values.transaction_idle_timeout(),
                values.transaction_tombstone_retention(),
                values.command_retry_validity(),
                values.state_snapshot_interval(),
            ] {
                assert_eq!(actual, duration);
            }
            let json = serde_json::to_vec(&values).assured("the full policy serializes");
            assert_eq!(
                serde_json::from_slice::<ClusterParameterValues>(&json)
                    .assured("the full policy decodes"),
                values
            );
            let archived =
                rkyv::to_bytes::<rkyv::rancor::Error>(&values).assured("the full policy archives");
            assert_eq!(
                rkyv::from_bytes::<ClusterParameterValues, rkyv::rancor::Error>(&archived)
                    .assured("the full current policy unarchives"),
                values
            );
        }
    }

    #[test]
    fn declaration_bounds_still_refuse_zero_and_every_duration_past_the_maximum() {
        for duration in [
            Duration::ZERO,
            super::super::MAX_PARAMETER_DURATION + Duration::from_nanos(1),
            Duration::from_secs(u64::from(u32::MAX) + 1),
        ] {
            assert!(
                ClusterParameterValues::resolve(ClusterParameterSeedValues {
                    node_unavailability_timeout: Some(duration),
                    ..Default::default()
                })
                .is_err()
            );
        }
    }
    #[test]
    fn current_archives_refuse_a_noncanonical_nanosecond_coordinate() {
        let value = DurationParts {
            seconds: 1,
            nanos: 1_000_000_000,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value)
            .assured("the structurally current malformed coordinate archives");
        assert!(rkyv::from_bytes::<DurationParts, rkyv::rancor::Error>(&bytes).is_err());
    }
}
