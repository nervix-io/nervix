//! Validated current metadata decoders.
//!
//! Layer: vocabulary.
//! - **Owns.** Key and uniqueness validation at serde and archive boundaries.
//! - **Depends on.** The metadata value constructors and serialization traits.
//! - **Must not know.** Committed policy, registry state or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(vocabulary, reason = "owns metadata decoder invariants")
)]

use std::fmt;

use rkyv::{
    Deserialize as RkyvDeserialize,
    rancor::{Fallible, Source},
};
use serde::{
    Deserialize, Deserializer,
    de::{Error as _, MapAccess, Visitor},
};

use super::{
    ArchivedEntityLabelKeys, ArchivedEntityLabels, ArchivedLabelKey, EntityLabelError,
    EntityLabelKeys, EntityLabels, LabelKey,
};

impl<'de> Deserialize<'de> for LabelKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from(String::deserialize(deserializer)?)
            .map_err(|error| D::Error::custom(format_args!("{error:#}")))
    }
}

impl<D: Fallible + ?Sized> RkyvDeserialize<LabelKey, D> for ArchivedLabelKey
where
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<LabelKey, D::Error> {
        let value: String = self.0.deserialize(deserializer)?;
        LabelKey::try_from(value).map_err(|error| D::Error::new(error.into_error()))
    }
}

impl<'de> Deserialize<'de> for EntityLabels {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LabelVisitor;
        impl<'de> Visitor<'de> for LabelVisitor {
            type Value = EntityLabels;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map of distinct label keys to exact string values")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut labels = EntityLabels::default();
                while let Some((key, value)) = access.next_entry::<LabelKey, String>()? {
                    labels
                        .insert_unique(key, value)
                        .map_err(|error| A::Error::custom(format_args!("{error:#}")))?;
                }
                Ok(labels)
            }
        }
        deserializer.deserialize_map(LabelVisitor)
    }
}

impl<D: Fallible + ?Sized> RkyvDeserialize<EntityLabels, D> for ArchivedEntityLabels
where
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<EntityLabels, D::Error> {
        let mut labels = EntityLabels::default();
        let mut previous: Option<LabelKey> = None;
        for (key, value) in self.0.iter() {
            let key: LabelKey = key.deserialize(deserializer)?;
            if previous.as_ref().is_some_and(|previous| previous >= &key) {
                return Err(D::Error::new(EntityLabelError::NonCanonicalKeys));
            }
            previous = Some(key.clone());
            labels
                .insert_unique(key, value.deserialize(deserializer)?)
                .map_err(|error| D::Error::new(error.into_error()))?;
        }
        Ok(labels)
    }
}

impl<'de> Deserialize<'de> for EntityLabelKeys {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from(Vec::<LabelKey>::deserialize(deserializer)?)
            .map_err(|error| D::Error::custom(format_args!("{error:#}")))
    }
}

impl<D: Fallible + ?Sized> RkyvDeserialize<EntityLabelKeys, D> for ArchivedEntityLabelKeys
where
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<EntityLabelKeys, D::Error> {
        let mut keys = Vec::with_capacity(self.0.len());
        for key in self.0.iter() {
            let key: LabelKey = key.deserialize(deserializer)?;
            if keys.last().is_some_and(|previous| previous >= &key) {
                return Err(D::Error::new(EntityLabelError::NonCanonicalKeys));
            }
            keys.push(key);
        }
        EntityLabelKeys::try_from(keys).map_err(|error| D::Error::new(error.into_error()))
    }
}
