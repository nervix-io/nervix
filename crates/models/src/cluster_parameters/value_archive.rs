//! Validated archive and JSON projections of context-free settings.
//!
//! Layer: vocabulary.
//! - **Owns.** Validated archive and JSON projections of context-free settings.
//! - **Depends on.** Setting classes, semantic values and typed validation reports.
//! - **Must not know.** Parsing, consensus, command-line options or running services.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns the vocabulary boundary declared above")
)]

use error_stack::Report;
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Place, Serialize as RkyvSerialize,
    rancor::{Fallible, Source},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};
use serde::{Deserialize as _, Deserializer, Serializer, de::Error as _};

use super::value::{ParameterSetting, ParameterStorage, ParameterValue};
use crate::SettingDefinition;

pub struct ValidatedSetting;

impl<T, Stored> ArchiveWith<ParameterSetting<T, Stored>> for ValidatedSetting
where
    T: SettingDefinition<Context = ()>,
    T::Value: ParameterValue,
    Stored: ParameterStorage<T::Value> + Archive,
{
    type Archived = Stored::Archived;
    type Resolver = Stored::Resolver;
    fn resolve_with(
        field: &ParameterSetting<T, Stored>,
        resolver: Self::Resolver,
        out: Place<Self::Archived>,
    ) {
        field.stored().resolve(resolver, out);
    }
}

impl<T, Stored, S> SerializeWith<ParameterSetting<T, Stored>, S> for ValidatedSetting
where
    T: SettingDefinition<Context = ()>,
    T::Value: ParameterValue,
    Stored: ParameterStorage<T::Value> + Archive + RkyvSerialize<S>,
    S: Fallible + ?Sized,
{
    fn serialize_with(
        field: &ParameterSetting<T, Stored>,
        serializer: &mut S,
    ) -> Result<Self::Resolver, S::Error> {
        RkyvSerialize::serialize(field.stored(), serializer)
    }
}

impl<T, Stored, D> DeserializeWith<Stored::Archived, ParameterSetting<T, Stored>, D>
    for ValidatedSetting
where
    T: SettingDefinition<Context = ()>,
    T::Value: ParameterValue,
    Stored: ParameterStorage<T::Value> + Archive,
    D: Fallible + ?Sized,
    D::Error: Source,
    Stored::Archived: RkyvDeserialize<Stored, D>,
{
    fn deserialize_with(
        field: &Stored::Archived,
        deserializer: &mut D,
    ) -> Result<ParameterSetting<T, Stored>, D::Error> {
        let value = field.deserialize(deserializer)?;
        ParameterSetting::from_stored(value).map_err(|report| D::Error::new(report.into_error()))
    }
}

pub(crate) fn serialize<T, Stored, S>(
    field: &ParameterSetting<T, Stored>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    T: SettingDefinition<Context = ()>,
    T::Value: ParameterValue + serde::Serialize,
    Stored: ParameterStorage<T::Value>,
    S: Serializer,
{
    serde::Serialize::serialize(&field.value(), serializer)
}

pub(crate) fn deserialize<'de, T, Stored, D>(
    deserializer: D,
) -> Result<ParameterSetting<T, Stored>, D::Error>
where
    T: SettingDefinition<Context = ()>,
    T::Value: ParameterValue + serde::Deserialize<'de>,
    Stored: ParameterStorage<T::Value>,
    D: Deserializer<'de>,
{
    let value = T::Value::deserialize(deserializer)?;
    ParameterSetting::new(value, &())
        .map_err(|report: Report<T::Error>| D::Error::custom(format_args!("{report:#}")))
}
