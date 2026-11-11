//! Lossless archival of shared document kinds and semantic transitions.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Archive projections of protocol document kinds and complete transition values.
//! - **Depends on.** rkyv, maintained JSON encoding and the protocol's semantic vocabulary.
//! - **Must not know.** Trust verification, policy, storage layout or clocks.

use nervix_licensing_protocol::document::DocumentType;
use rkyv::{
    Place, SerializeUnsized,
    rancor::{Fallible, Source},
    string::{ArchivedString, StringResolver},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};

/// Maintained serialization for the keyed committed grants and their authorization bounds.
pub(super) struct Json;

impl<T> ArchiveWith<T> for Json {
    type Archived = ArchivedString;
    type Resolver = TransitionsResolver;

    fn resolve_with(_: &T, resolver: Self::Resolver, out: Place<Self::Archived>) {
        ArchivedString::resolve_from_str(&resolver.value, resolver.resolver, out);
    }
}

impl<T, S> SerializeWith<T, S> for Json
where
    T: serde::Serialize,
    S: Fallible + ?Sized,
    S::Error: Source,
    str: SerializeUnsized<S>,
{
    fn serialize_with(field: &T, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let value = serde_json::to_string(field).map_err(S::Error::new)?;
        let resolver = ArchivedString::serialize_from_str(&value, serializer)?;
        Ok(TransitionsResolver { value, resolver })
    }
}

impl<T, D> DeserializeWith<ArchivedString, T, D> for Json
where
    T: serde::de::DeserializeOwned,
    D: Fallible + ?Sized,
    D::Error: Source,
{
    fn deserialize_with(field: &ArchivedString, _: &mut D) -> Result<T, D::Error> {
        serde_json::from_str(field.as_str()).map_err(D::Error::new)
    }
}

pub(super) struct DocumentKind;

/// Archive the protocol's semantic transitions as one maintained JSON value, decoded once on load.
/// Policy and observations borrow the resulting typed slice and never parse storage bytes.
pub(super) struct Transitions;

pub(super) struct TransitionsResolver {
    value: String,
    resolver: StringResolver,
}

impl ArchiveWith<Vec<nervix_licensing_protocol::documents::Transition>> for Transitions {
    type Archived = ArchivedString;
    type Resolver = TransitionsResolver;

    fn resolve_with(
        _: &Vec<nervix_licensing_protocol::documents::Transition>,
        resolver: Self::Resolver,
        out: Place<Self::Archived>,
    ) {
        ArchivedString::resolve_from_str(&resolver.value, resolver.resolver, out);
    }
}

impl<S> SerializeWith<Vec<nervix_licensing_protocol::documents::Transition>, S> for Transitions
where
    S: Fallible + ?Sized,
    S::Error: Source,
    str: SerializeUnsized<S>,
{
    fn serialize_with(
        field: &Vec<nervix_licensing_protocol::documents::Transition>,
        serializer: &mut S,
    ) -> Result<Self::Resolver, S::Error> {
        let value = serde_json::to_string(field).map_err(S::Error::new)?;
        let resolver = ArchivedString::serialize_from_str(&value, serializer)?;
        Ok(TransitionsResolver { value, resolver })
    }
}

impl<D> DeserializeWith<ArchivedString, Vec<nervix_licensing_protocol::documents::Transition>, D>
    for Transitions
where
    D: Fallible + ?Sized,
    D::Error: Source,
{
    fn deserialize_with(
        field: &ArchivedString,
        _: &mut D,
    ) -> Result<Vec<nervix_licensing_protocol::documents::Transition>, D::Error> {
        serde_json::from_str(field.as_str()).map_err(D::Error::new)
    }
}

impl ArchiveWith<DocumentType> for DocumentKind {
    type Archived = ArchivedString;
    type Resolver = StringResolver;
    fn resolve_with(field: &DocumentType, resolver: Self::Resolver, out: Place<Self::Archived>) {
        ArchivedString::resolve_from_str(field.as_str(), resolver, out);
    }
}

impl<S> SerializeWith<DocumentType, S> for DocumentKind
where
    S: Fallible + ?Sized,
    S::Error: Source,
    str: SerializeUnsized<S>,
{
    fn serialize_with(
        field: &DocumentType,
        serializer: &mut S,
    ) -> Result<Self::Resolver, S::Error> {
        ArchivedString::serialize_from_str(field.as_str(), serializer)
    }
}

impl<D> DeserializeWith<ArchivedString, DocumentType, D> for DocumentKind
where
    D: Fallible + ?Sized,
    D::Error: Source,
{
    fn deserialize_with(field: &ArchivedString, _: &mut D) -> Result<DocumentType, D::Error> {
        serde::Deserialize::deserialize(
            serde::de::value::StrDeserializer::<serde::de::value::Error>::new(field.as_str()),
        )
        .map_err(D::Error::new)
    }
}
