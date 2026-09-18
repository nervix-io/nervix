//! Semantic Model for atomic resource rebinding.
//!
//! Layer: vocabulary.
//! - **Owns.** The requested resource version and exact/all usage selection.
//! - **Depends on.** Resource names, Model references and resource-version vocabulary.
//! - **Must not know.** NSPL parsing, resource catalogs, transaction planning or runtime changes.

use meticulous::OptionExt as _;
use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Place, Serialize as RkyvSerialize,
    rancor::Fallible,
    ser::{Allocator, Writer},
    vec::{ArchivedVec, VecResolver},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use sorted_vec::SortedSet;

use crate::{NodeRef, RequestedResourceVersion, ResourceName};

/// Moves selected Models that bind one resource to one completed version.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub struct RebindResource {
    pub resource: ResourceName,
    pub version: RequestedResourceVersion,
    pub selection: RebindResourceSelection,
}

/// Which usages of a resource one rebinding selects.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Archive, RkyvSerialize, RkyvDeserialize,
)]
pub enum RebindResourceSelection {
    /// Every Model in the domain that binds the resource.
    All,
    /// Exactly the named kind-qualified Models.
    Members(RebindResourceMembers),
}

/// A non-empty, sorted and duplicate-free exact resource-usage selection.
#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct RebindResourceMembers {
    #[rkyv(with = RebindResourceMembersAsVec)]
    remaining: SortedSet<NodeRef>,
    last: NodeRef,
}

impl RebindResourceMembers {
    pub fn new(first: NodeRef, remaining: impl IntoIterator<Item = NodeRef>) -> Self {
        let mut members = SortedSet::new();
        members.find_or_insert(first);
        members.extend(remaining);
        let last = members
            .pop()
            .assured("inserting the required first member makes the set non-empty");
        Self {
            remaining: members,
            last,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &NodeRef> {
        self.remaining.iter().chain(std::iter::once(&self.last))
    }
}

impl Serialize for RebindResourceMembers {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for RebindResourceMembers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut members = Vec::<NodeRef>::deserialize(deserializer)?;
        let Some(first) = members.pop() else {
            return Err(D::Error::custom(
                "an exact resource rebind selection requires at least one member",
            ));
        };
        Ok(Self::new(first, members))
    }
}

#[derive(Debug)]
struct RebindResourceMembersAsVec;

impl ArchiveWith<SortedSet<NodeRef>> for RebindResourceMembersAsVec {
    type Archived = ArchivedVec<<NodeRef as Archive>::Archived>;
    type Resolver = VecResolver;

    fn resolve_with(
        field: &SortedSet<NodeRef>,
        resolver: Self::Resolver,
        out: Place<Self::Archived>,
    ) {
        ArchivedVec::resolve_from_len(field.len(), resolver, out);
    }
}

impl<S> SerializeWith<SortedSet<NodeRef>, S> for RebindResourceMembersAsVec
where
    NodeRef: RkyvSerialize<S>,
    S: Fallible + Allocator + Writer + ?Sized,
{
    fn serialize_with(
        field: &SortedSet<NodeRef>,
        serializer: &mut S,
    ) -> Result<Self::Resolver, S::Error> {
        ArchivedVec::<<NodeRef as Archive>::Archived>::serialize_from_iter::<NodeRef, _, _>(
            field.iter(),
            serializer,
        )
    }
}

impl<D> DeserializeWith<ArchivedVec<<NodeRef as Archive>::Archived>, SortedSet<NodeRef>, D>
    for RebindResourceMembersAsVec
where
    <NodeRef as Archive>::Archived: RkyvDeserialize<NodeRef, D>,
    D: Fallible + ?Sized,
{
    fn deserialize_with(
        field: &ArchivedVec<<NodeRef as Archive>::Archived>,
        deserializer: &mut D,
    ) -> Result<SortedSet<NodeRef>, D::Error> {
        let mut members = SortedSet::new();
        for archived in field.iter() {
            members.find_or_insert(archived.deserialize(deserializer)?);
        }
        Ok(members)
    }
}

#[cfg(test)]
mod tests {
    use meticulous::ResultExt as _;

    use super::*;
    use crate::{ModelKind, ModelName};

    fn member(kind: ModelKind, name: &str) -> NodeRef {
        let name = ModelName::try_from(name).assured("the test uses a valid model name");
        NodeRef::new(kind, name)
    }

    #[test]
    fn exact_members_round_trip_as_a_non_empty_sorted_set() {
        let members = RebindResourceMembers::new(
            member(ModelKind::Vhost, "api"),
            [
                member(ModelKind::Client, "mounted"),
                member(ModelKind::Vhost, "api"),
            ],
        );
        let json = serde_json::to_string(&members)
            .assured("an exact resource rebind selection has a JSON representation");
        let restored = serde_json::from_str::<RebindResourceMembers>(&json)
            .assured("the JSON was produced from this selection type");

        assert_eq!(restored, members);
        assert_eq!(restored.iter().count(), 2);
        assert!(serde_json::from_str::<RebindResourceMembers>("[]").is_err());
    }
}
