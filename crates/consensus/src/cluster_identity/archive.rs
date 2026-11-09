//! Validated current archives of cluster identity and shared public-key vocabulary.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Lossless archive projections and complete identity validation during recovery.
//! - **Depends on.** rkyv, UUIDs, shared protocol keys and the identity record's validation.
//! - **Must not know.** Database layout, bootstrap, graph execution or licensing policy.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns identity archive validation")
)]

use nervix_crypto::ecdsa::P256PublicCoordinates;
use nervix_licensing_protocol::keys::{KeyId, PublicKey};
use rkyv::{
    Archive, Deserialize, Place, Serialize, SerializeUnsized,
    rancor::{Fallible, Source},
    string::{ArchivedString, StringResolver},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};
use uuid::Uuid;

use super::{ArchivedClusterIdentityRecord, ClusterIdentityRecord};

pub(super) struct UuidBytes;

impl ArchiveWith<Uuid> for UuidBytes {
    type Archived = <[u8; 16] as Archive>::Archived;
    type Resolver = <[u8; 16] as Archive>::Resolver;

    fn resolve_with(field: &Uuid, resolver: Self::Resolver, out: Place<Self::Archived>) {
        field.as_bytes().resolve(resolver, out);
    }
}

impl<S: Fallible + ?Sized> SerializeWith<Uuid, S> for UuidBytes {
    fn serialize_with(field: &Uuid, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        field.as_bytes().serialize(serializer)
    }
}

impl<D: Fallible + ?Sized> DeserializeWith<<[u8; 16] as Archive>::Archived, Uuid, D> for UuidBytes {
    fn deserialize_with(
        field: &<[u8; 16] as Archive>::Archived,
        deserializer: &mut D,
    ) -> Result<Uuid, D::Error> {
        Ok(Uuid::from_bytes(field.deserialize(deserializer)?))
    }
}

pub(super) struct KeyIdentity;

impl ArchiveWith<KeyId> for KeyIdentity {
    type Archived = ArchivedString;
    type Resolver = StringResolver;

    fn resolve_with(field: &KeyId, resolver: Self::Resolver, out: Place<Self::Archived>) {
        ArchivedString::resolve_from_str(field.as_ref(), resolver, out);
    }
}

impl<S> SerializeWith<KeyId, S> for KeyIdentity
where
    S: Fallible + ?Sized,
    S::Error: Source,
    str: SerializeUnsized<S>,
{
    fn serialize_with(field: &KeyId, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        ArchivedString::serialize_from_str(field.as_ref(), serializer)
    }
}

impl<D> DeserializeWith<ArchivedString, KeyId, D> for KeyIdentity
where
    D: Fallible + ?Sized,
    D::Error: Source,
{
    fn deserialize_with(field: &ArchivedString, _: &mut D) -> Result<KeyId, D::Error> {
        field
            .as_str()
            .to_owned()
            .try_into()
            .map_err(|report: error_stack::Report<_>| D::Error::new(report.into_error()))
    }
}

pub(super) struct PublicCoordinates;

impl ArchiveWith<PublicKey> for PublicCoordinates {
    type Archived = <[[u8; 32]; 2] as Archive>::Archived;
    type Resolver = <[[u8; 32]; 2] as Archive>::Resolver;

    fn resolve_with(field: &PublicKey, resolver: Self::Resolver, out: Place<Self::Archived>) {
        let coordinates = field.coordinates();
        [coordinates.x, coordinates.y].resolve(resolver, out);
    }
}

impl<S: Fallible + ?Sized> SerializeWith<PublicKey, S> for PublicCoordinates {
    fn serialize_with(field: &PublicKey, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let coordinates = field.coordinates();
        [coordinates.x, coordinates.y].serialize(serializer)
    }
}

impl<D: Fallible + ?Sized> DeserializeWith<<[[u8; 32]; 2] as Archive>::Archived, PublicKey, D>
    for PublicCoordinates
{
    fn deserialize_with(
        field: &<[[u8; 32]; 2] as Archive>::Archived,
        deserializer: &mut D,
    ) -> Result<PublicKey, D::Error> {
        let [x, y] = field.deserialize(deserializer)?;
        Ok(PublicKey::from_coordinates(P256PublicCoordinates { x, y }))
    }
}

impl<D> Deserialize<ClusterIdentityRecord, D> for ArchivedClusterIdentityRecord
where
    D: Fallible + ?Sized,
    D::Error: Source,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<ClusterIdentityRecord, D::Error> {
        let record = ClusterIdentityRecord {
            cluster_uid: UuidBytes::deserialize_with(&self.cluster_uid, deserializer)?,
            created_at: self.created_at.deserialize(deserializer)?,
            key_generation: self.key_generation.deserialize(deserializer)?,
            current_key: self.current_key.deserialize(deserializer)?,
            retired_key: self.retired_key.deserialize(deserializer)?,
            pending_key: self.pending_key.deserialize(deserializer)?,
        };
        record
            .validate()
            .map_err(|report| D::Error::new(report.into_error()))?;
        Ok(record)
    }
}
