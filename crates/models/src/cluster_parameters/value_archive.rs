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

use super::value::{ParameterDefinition, ParameterSetting};

pub struct ValidatedSetting;

impl<T: ParameterDefinition<Context = ()>> ArchiveWith<ParameterSetting<T>> for ValidatedSetting
where
    T::Stored: Archive,
{
    type Archived = <T::Stored as Archive>::Archived;
    type Resolver = <T::Stored as Archive>::Resolver;

    fn resolve_with(
        field: &ParameterSetting<T>,
        resolver: Self::Resolver,
        out: Place<Self::Archived>,
    ) {
        field.stored().resolve(resolver, out);
    }
}

impl<T, S> SerializeWith<ParameterSetting<T>, S> for ValidatedSetting
where
    T: ParameterDefinition<Context = ()>,
    S: Fallible + ?Sized,
    T::Stored: RkyvSerialize<S>,
{
    fn serialize_with(
        field: &ParameterSetting<T>,
        serializer: &mut S,
    ) -> Result<Self::Resolver, S::Error> {
        RkyvSerialize::serialize(field.stored(), serializer)
    }
}

impl<T, D> DeserializeWith<<T::Stored as Archive>::Archived, ParameterSetting<T>, D>
    for ValidatedSetting
where
    T: ParameterDefinition<Context = ()>,
    D: Fallible + ?Sized,
    D::Error: Source,
    T::Stored: Archive,
    <T::Stored as Archive>::Archived: RkyvDeserialize<T::Stored, D>,
{
    fn deserialize_with(
        field: &<T::Stored as Archive>::Archived,
        deserializer: &mut D,
    ) -> Result<ParameterSetting<T>, D::Error> {
        let value = field.deserialize(deserializer)?;
        ParameterSetting::from_stored(value).map_err(|report| D::Error::new(report.into_error()))
    }
}

pub(crate) fn serialize<T: ParameterDefinition<Context = ()>, S: Serializer>(
    field: &ParameterSetting<T>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    T::Value: serde::Serialize,
{
    serde::Serialize::serialize(&field.value(), serializer)
}

pub(crate) fn deserialize<'de, T, D>(deserializer: D) -> Result<ParameterSetting<T>, D::Error>
where
    T: ParameterDefinition<Context = ()>,
    T::Value: serde::Deserialize<'de>,
    D: Deserializer<'de>,
{
    let value = T::Value::deserialize(deserializer)?;
    ParameterSetting::new(value, &())
        .map_err(|report: Report<T::Error>| D::Error::custom(format_args!("{report:#}")))
}
