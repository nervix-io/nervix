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

use crate::{Setting, SettingDefinition};

pub struct ValidatedSetting;

impl<T: SettingDefinition> ArchiveWith<Setting<T>> for ValidatedSetting
where
    T::Value: Archive,
{
    type Archived = <T::Value as Archive>::Archived;
    type Resolver = <T::Value as Archive>::Resolver;

    fn resolve_with(field: &Setting<T>, resolver: Self::Resolver, out: Place<Self::Archived>) {
        field.value().resolve(resolver, out);
    }
}

impl<T, S> SerializeWith<Setting<T>, S> for ValidatedSetting
where
    T: SettingDefinition,
    S: Fallible + ?Sized,
    T::Value: RkyvSerialize<S>,
{
    fn serialize_with(field: &Setting<T>, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        RkyvSerialize::serialize(field.value(), serializer)
    }
}

impl<T, D> DeserializeWith<<T::Value as Archive>::Archived, Setting<T>, D> for ValidatedSetting
where
    T: SettingDefinition<Context = ()>,
    D: Fallible + ?Sized,
    D::Error: Source,
    T::Value: Archive,
    <T::Value as Archive>::Archived: RkyvDeserialize<T::Value, D>,
{
    fn deserialize_with(
        field: &<T::Value as Archive>::Archived,
        deserializer: &mut D,
    ) -> Result<Setting<T>, D::Error> {
        let value = field.deserialize(deserializer)?;
        Setting::new(value, &()).map_err(|report| D::Error::new(report.into_error()))
    }
}

pub(super) fn serialize<T: SettingDefinition, S: Serializer>(
    field: &Setting<T>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    T::Value: serde::Serialize,
{
    serde::Serialize::serialize(field.value(), serializer)
}

pub(super) fn deserialize<'de, T, D>(deserializer: D) -> Result<Setting<T>, D::Error>
where
    T: SettingDefinition<Context = ()>,
    T::Value: serde::Deserialize<'de>,
    D: Deserializer<'de>,
{
    let value = T::Value::deserialize(deserializer)?;
    Setting::new(value, &())
        .map_err(|report: Report<T::Error>| D::Error::custom(format_args!("{report:#}")))
}
