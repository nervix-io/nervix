//! Lossless inline storage of the bounded cluster-parameter durations.
//!
//! Layer: vocabulary.
//! - **Owns.** Compact seconds/nanoseconds storage and its declaration-preserving setting adapter.
//! - **Depends on.** Duration values, the setting carrier and cluster parameter validation.
//! - **Must not know.** Consensus records, parsing, service timers or runtime installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns bounded parameter representation")
)]

use std::{marker::PhantomData, time::Duration};

use error_stack::{Report, ResultExt as _};
use meticulous::ResultExt as _;
use rkyv::{
    Archive, Deserialize, Serialize,
    rancor::{Fallible, Source},
};

use super::{ClusterParameterError, ClusterParameterKey};
use crate::{ClusterParameters, Setting, SettingDefinition};

/// Every duration declaration already bounds seconds by u32; nanoseconds remain exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize)]
pub struct PackedDuration {
    seconds: u32,
    nanoseconds: u32,
}

#[derive(Debug, thiserror::Error)]
#[error("cluster parameter duration has invalid seconds or nanoseconds")]
struct InvalidPackedDuration;

impl PackedDuration {
    fn pack(value: Duration) -> Result<Self, Report<InvalidPackedDuration>> {
        Ok(Self {
            seconds: u32::try_from(value.as_secs()).change_context(InvalidPackedDuration)?,
            nanoseconds: value.subsec_nanos(),
        })
    }
    pub(super) fn get(self) -> Duration {
        Duration::new(u64::from(self.seconds), self.nanoseconds)
    }
}

impl serde::Serialize for PackedDuration {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.get(), serializer)
    }
}
impl<'de> serde::Deserialize<'de> for PackedDuration {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = <Duration as serde::Deserialize>::deserialize(deserializer)?;
        Self::pack(value).map_err(|report| serde::de::Error::custom(format_args!("{report:#}")))
    }
}
impl<D: Fallible + ?Sized> Deserialize<PackedDuration, D> for ArchivedPackedDuration
where
    D::Error: Source,
{
    fn deserialize(&self, _: &mut D) -> Result<PackedDuration, D::Error> {
        let nanoseconds = self.nanoseconds.to_native();
        if nanoseconds >= 1_000_000_000 {
            return Err(D::Error::new(InvalidPackedDuration));
        }
        Ok(PackedDuration {
            seconds: self.seconds.to_native(),
            nanoseconds,
        })
    }
}

/// Keeps the original declaration as the sole class, name, default and validation owner.
pub struct DurationDefinition<D>(PhantomData<fn() -> D>);
impl<D> SettingDefinition for DurationDefinition<D>
where
    D: SettingDefinition<
            Class = ClusterParameters,
            Value = Duration,
            Context = (),
            Error = ClusterParameterError,
        >,
{
    type Class = ClusterParameters;
    type Value = PackedDuration;
    type Context = ();
    type Error = ClusterParameterError;
    const NAME: &'static str = D::NAME;
    const DERIVATION: Option<&'static str> = D::DERIVATION;
    fn default_value(context: &()) -> PackedDuration {
        PackedDuration::pack(D::default_value(context))
            .assured("cluster duration defaults fit their declared seconds bound")
    }
    fn validate(value: &PackedDuration, context: &()) -> Result<(), Report<ClusterParameterError>> {
        D::validate(&value.get(), context)
    }
}

pub(super) fn resolve<D>(
    value: Option<Duration>,
    parameter: ClusterParameterKey,
) -> Result<Setting<DurationDefinition<D>>, Report<ClusterParameterError>>
where
    D: SettingDefinition<
            Class = ClusterParameters,
            Value = Duration,
            Context = (),
            Error = ClusterParameterError,
        >,
{
    let value = match value {
        Some(value) => Some(PackedDuration::pack(value).change_context(
            ClusterParameterError::DurationTooLong {
                parameter,
                maximum: super::MAX_PARAMETER_DURATION,
            },
        )?),
        None => None,
    };
    Setting::resolve(value, &())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterParameterSeedValues, ClusterParameterValues};

    #[test]
    fn fractional_and_maximum_durations_keep_public_values_and_complete_encodings() {
        assert_eq!(std::mem::size_of::<PackedDuration>(), 8);
        assert_eq!(std::mem::size_of::<rkyv::Archived<PackedDuration>>(), 8);
        for duration in [
            Duration::from_nanos(1),
            Duration::new(1, 999_999_999),
            Duration::new(u64::from(u32::MAX) - 1, 999_999_999),
            super::super::MAX_PARAMETER_DURATION,
        ] {
            let values = ClusterParameterValues::resolve(ClusterParameterSeedValues {
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
        let value = PackedDuration {
            seconds: 1,
            nanoseconds: 1_000_000_000,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value)
            .assured("the structurally current malformed coordinate archives");
        assert!(rkyv::from_bytes::<PackedDuration, rkyv::rancor::Error>(&bytes).is_err());
    }
}
