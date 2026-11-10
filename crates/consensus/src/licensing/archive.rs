//! Lossless archival of the shared signed-document kind.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** The archive projection of the protocol's document-kind vocabulary.
//! - **Depends on.** rkyv and the protocol's current serialized names.
//! - **Must not know.** Trust verification, policy, storage layout or clocks.

use nervix_licensing_protocol::document::DocumentType;
use rkyv::{
    Place, SerializeUnsized,
    rancor::{Fallible, Source},
    string::{ArchivedString, StringResolver},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};

pub(super) struct DocumentKind;

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
