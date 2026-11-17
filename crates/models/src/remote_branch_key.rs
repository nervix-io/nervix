//! Concrete key fields and external partition provenance at storage and transport boundaries.
//!
//! Layer: vocabulary.
//! - **Owns.** The one current representation of unbranched, direct and inherited key scopes.
//! - **Depends on.** Remote field values and external partition coordinates.
//! - **Must not know.** Routing, state ownership, codecs or runtime branch actors.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, MapAccess, SeqAccess, Visitor},
};

use crate::{ExternalPartition, InheritedPartitionOrigin, RemoteRuntimeField};

pub type RemoteBranchKey = BranchKeyFields<RemoteRuntimeField>;
pub type InheritedRemoteBranchKey = InheritedBranchKeyFields<RemoteRuntimeField>;

/// Direct scopes keep their compact field-only representation; inherited scopes retain the
/// original external coordinate so regrouping can compute ownership without hashing key fields.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum BranchKeyFields<T> {
    Unbranched,
    Direct(Box<[T]>),
    Inherited(Box<InheritedBranchKeyFields<T>>),
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[serde(deny_unknown_fields)]
pub struct InheritedBranchKeyFields<T> {
    pub fields: Box<[T]>,
    pub origin: InheritedPartitionOrigin,
}

impl<T> BranchKeyFields<T> {
    pub fn branched(fields: Box<[T]>, origin: Option<InheritedPartitionOrigin>) -> Self {
        match origin {
            Some(origin) => Self::Inherited(Box::new(InheritedBranchKeyFields { fields, origin })),
            None => Self::Direct(fields),
        }
    }

    pub fn is_branched(&self) -> bool {
        !matches!(self, Self::Unbranched)
    }

    pub fn map_fields<U>(self, mut map: impl FnMut(T) -> U) -> BranchKeyFields<U> {
        match self {
            Self::Unbranched => BranchKeyFields::Unbranched,
            Self::Direct(fields) => BranchKeyFields::Direct(fields.into_iter().map(map).collect()),
            Self::Inherited(key) => {
                BranchKeyFields::Inherited(Box::new(InheritedBranchKeyFields {
                    fields: key.fields.into_iter().map(&mut map).collect(),
                    origin: key.origin,
                }))
            }
        }
    }

    pub fn fields(&self) -> Option<&[T]> {
        match self {
            Self::Unbranched => None,
            Self::Direct(fields) => Some(fields),
            Self::Inherited(key) => Some(&key.fields),
        }
    }

    pub fn fields_mut(&mut self) -> Option<&mut [T]> {
        match self {
            Self::Unbranched => None,
            Self::Direct(fields) => Some(fields),
            Self::Inherited(key) => Some(&mut key.fields),
        }
    }

    pub fn external_partition(&self) -> Option<ExternalPartition> {
        match self {
            Self::Inherited(key) => Some(key.origin.partition),
            _ => None,
        }
    }

    pub fn inherited_origin(&self) -> Option<InheritedPartitionOrigin> {
        match self {
            Self::Inherited(key) => Some(key.origin),
            _ => None,
        }
    }

    pub fn into_parts(self) -> Option<(Box<[T]>, Option<InheritedPartitionOrigin>)> {
        match self {
            Self::Unbranched => None,
            Self::Direct(fields) => Some((fields, None)),
            Self::Inherited(key) => Some((key.fields, Some(key.origin))),
        }
    }
}

impl<T: Serialize> Serialize for BranchKeyFields<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Unbranched => serializer.serialize_none(),
            Self::Direct(fields) => fields.serialize(serializer),
            Self::Inherited(key) => key.serialize(serializer),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for BranchKeyFields<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for KeyVisitor<T> {
            type Value = BranchKeyFields<T>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("null, concrete key fields, or fields with an external partition")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(BranchKeyFields::Unbranched)
            }
            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(BranchKeyFields::Unbranched)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
                Vec::<T>::deserialize(de::value::SeqAccessDeserializer::new(access))
                    .map(|fields| BranchKeyFields::Direct(fields.into_boxed_slice()))
            }
            fn visit_map<A: MapAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
                InheritedBranchKeyFields::deserialize(de::value::MapAccessDeserializer::new(access))
                    .map(|key| BranchKeyFields::Inherited(Box::new(key)))
            }
        }
        deserializer.deserialize_any(KeyVisitor(std::marker::PhantomData))
    }
}
