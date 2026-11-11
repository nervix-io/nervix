//! Compact validated storage for complete cluster parameter values.
//!
//! Layer: vocabulary.
//! - **Owns.** Lossless inline parameter storage under each definition's existing validated range.
//! - **Depends on.** Setting definitions, semantic values and current representation codecs.
//! - **Must not know.** Parsing, consensus commands, clocks or runtime installation.

use std::{fmt, marker::PhantomData, time::Duration};

use error_stack::Report;
use meticulous::ResultExt as _;
use rkyv::{Archive, Deserialize, Serialize};

use crate::SettingDefinition;

pub trait ParameterValue: Copy {
    type Stored: ParameterStorage<Self>;
}

/// An exact projection used only after its owning definition has validated the semantic value.
pub trait ParameterStorage<Value>: Copy + fmt::Debug + PartialEq + Eq {
    fn store(value: Value) -> Self;
    fn read(self) -> Value;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize)]
pub struct DurationParts {
    seconds: u32,
    nanos: u32,
}

impl ParameterValue for Duration {
    type Stored = DurationParts;
}

impl ParameterStorage<Duration> for DurationParts {
    fn store(value: Duration) -> Self {
        DurationParts {
            seconds: u32::try_from(value.as_secs())
                .assured("the definition validates the u32 seconds range"),
            nanos: value.subsec_nanos(),
        }
    }
    fn read(self) -> Duration {
        Duration::new(u64::from(self.seconds), self.nanos)
    }
}

macro_rules! unchanged {
    ($($value:ty),+ $(,)?) => {$ (
        impl ParameterValue for $value {
            type Stored = Self;
        }
        impl ParameterStorage<$value> for $value {
            fn store(value: $value) -> Self { value }
            fn read(self) -> $value { self }
        }
    )+};
}
unchanged!(
    u64,
    nervix_crypto_policy::TlsCipherSuites,
    nervix_crypto_policy::TlsKeyExchangeGroups
);

impl ParameterStorage<u64> for u32 {
    fn store(value: u64) -> Self {
        u32::try_from(value).assured("the parameter definition validates the u32 count range")
    }
    fn read(self) -> u64 {
        u64::from(self)
    }
}

pub struct ParameterSetting<T, Stored = <<T as SettingDefinition>::Value as ParameterValue>::Stored>
where
    T: SettingDefinition,
    T::Value: ParameterValue,
    Stored: ParameterStorage<T::Value>,
{
    stored: Stored,
    marker: PhantomData<T>,
}
impl<T, Stored> Copy for ParameterSetting<T, Stored>
where
    T: SettingDefinition,
    T::Value: ParameterValue,
    Stored: ParameterStorage<T::Value>,
{
}
impl<T, Stored> Clone for ParameterSetting<T, Stored>
where
    T: SettingDefinition,
    T::Value: ParameterValue,
    Stored: ParameterStorage<T::Value>,
{
    fn clone(&self) -> Self {
        *self
    }
}
impl<T, Stored> PartialEq for ParameterSetting<T, Stored>
where
    T: SettingDefinition,
    T::Value: ParameterValue,
    Stored: ParameterStorage<T::Value>,
{
    fn eq(&self, other: &Self) -> bool {
        self.stored == other.stored
    }
}
impl<T, Stored> Eq for ParameterSetting<T, Stored>
where
    T: SettingDefinition,
    T::Value: ParameterValue,
    Stored: ParameterStorage<T::Value>,
{
}
impl<T, Stored> fmt::Debug for ParameterSetting<T, Stored>
where
    T: SettingDefinition,
    T::Value: ParameterValue,
    Stored: ParameterStorage<T::Value>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Setting")
            .field("name", &T::NAME)
            .finish_non_exhaustive()
    }
}

impl<T, Stored> ParameterSetting<T, Stored>
where
    T: SettingDefinition<Context = ()>,
    T::Value: ParameterValue,
    Stored: ParameterStorage<T::Value>,
{
    pub(super) fn new(value: T::Value, context: &()) -> Result<Self, Report<T::Error>> {
        T::validate(&value, context)?;
        Ok(Self {
            stored: Stored::store(value),
            marker: PhantomData,
        })
    }
    pub(super) fn resolve(value: Option<T::Value>, context: &()) -> Result<Self, Report<T::Error>> {
        Self::new(value.unwrap_or_else(|| T::default_value(context)), context)
    }
    pub(super) fn value(&self) -> T::Value {
        self.stored.read()
    }
    pub(super) fn stored(&self) -> &Stored {
        &self.stored
    }
    pub(super) fn from_stored(stored: Stored) -> Result<Self, Report<T::Error>> {
        Self::new(stored.read(), &())
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
    use crate::cluster_parameters::LicensingRequestTimeout;

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
}
