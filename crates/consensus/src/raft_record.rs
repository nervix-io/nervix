//! Nervix-owned rkyv representations of OpenRaft values shared by transport and storage.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Lossless conversion between OpenRaft identities, entries and memberships and their
//!   current rkyv records.
//! - **Depends on.** OpenRaft, the replicated command vocabulary, and node identity vocabulary.
//! - **Must not know.** Interconnect requests, HTTP/2, the storage engine, or control-plane policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

use nervix_models::ClusterNodeName;
use openraft::{BasicNode, Entry, LogId, Membership, StoredMembership, Vote, entry::EntryPayload};
use rkyv::{Archive, Deserialize, Serialize};

use crate::{ConsensusCommand, EntryOf, LogIdOf, StoredMembershipOf, TypeConfig, VoteOf};

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct VoteRecord {
    term: u64,
    node_id: ClusterNodeName,
    committed: bool,
}

impl From<VoteOf> for VoteRecord {
    fn from(value: VoteOf) -> Self {
        Self {
            term: value.leader_id.term,
            node_id: value.leader_id.node_id,
            committed: value.committed,
        }
    }
}

impl From<VoteRecord> for VoteOf {
    fn from(value: VoteRecord) -> Self {
        if value.committed {
            Vote::new_committed(value.term, value.node_id)
        } else {
            Vote::new(value.term, value.node_id)
        }
    }
}

impl VoteRecord {
    pub(crate) fn node_id(&self) -> &ClusterNodeName {
        &self.node_id
    }

    pub(crate) fn into_vote(self) -> VoteOf {
        self.into()
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct LogIdRecord {
    term: u64,
    node_id: ClusterNodeName,
    index: u64,
}

impl From<LogIdOf> for LogIdRecord {
    fn from(value: LogIdOf) -> Self {
        Self {
            term: value.leader_id.term,
            node_id: value.leader_id.node_id,
            index: value.index,
        }
    }
}

impl From<LogIdRecord> for LogIdOf {
    fn from(value: LogIdRecord) -> Self {
        LogId::new(
            openraft::impls::leader_id_adv::LeaderId {
                term: value.term,
                node_id: value.node_id,
            },
            value.index,
        )
    }
}

impl LogIdRecord {
    pub(crate) fn into_log_id(self) -> LogIdOf {
        self.into()
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct MembershipNodeRecord {
    node_id: ClusterNodeName,
    address: String,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
struct MembershipRecord {
    configurations: Vec<Vec<ClusterNodeName>>,
    nodes: Vec<MembershipNodeRecord>,
}

impl MembershipRecord {
    fn from_membership(value: &Membership<ClusterNodeName, BasicNode>) -> Self {
        Self {
            configurations: value
                .get_joint_config()
                .iter()
                .map(|configuration| configuration.iter().cloned().collect())
                .collect(),
            nodes: value
                .nodes()
                .map(|(node_id, node)| MembershipNodeRecord {
                    node_id: node_id.clone(),
                    address: node.addr.clone(),
                })
                .collect(),
        }
    }

    fn into_membership(self) -> io::Result<Membership<ClusterNodeName, BasicNode>> {
        let configurations = self
            .configurations
            .into_iter()
            .map(|configuration| configuration.into_iter().collect::<BTreeSet<_>>())
            .collect();
        let nodes = self
            .nodes
            .into_iter()
            .map(|node| (node.node_id, BasicNode::new(node.address)))
            .collect::<BTreeMap<_, _>>();
        Membership::new(configurations, nodes).map_err(io::Error::other)
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
enum EntryPayloadRecord {
    Blank,
    Normal(ConsensusCommand),
    Membership(MembershipRecord),
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct EntryRecord {
    log_id: LogIdRecord,
    payload: EntryPayloadRecord,
}

impl From<EntryOf<TypeConfig>> for EntryRecord {
    fn from(value: EntryOf<TypeConfig>) -> Self {
        let payload = match value.payload {
            EntryPayload::Blank => EntryPayloadRecord::Blank,
            EntryPayload::Normal(command) => EntryPayloadRecord::Normal(command),
            EntryPayload::Membership(membership) => {
                EntryPayloadRecord::Membership(MembershipRecord::from_membership(&membership))
            }
        };
        Self {
            log_id: value.log_id.into(),
            payload,
        }
    }
}

impl EntryRecord {
    pub(crate) fn into_entry(self) -> io::Result<EntryOf<TypeConfig>> {
        let payload = match self.payload {
            EntryPayloadRecord::Blank => EntryPayload::Blank,
            EntryPayloadRecord::Normal(command) => EntryPayload::Normal(command),
            EntryPayloadRecord::Membership(membership) => {
                EntryPayload::Membership(membership.into_membership()?)
            }
        };
        Ok(Entry {
            log_id: self.log_id.into(),
            payload,
        })
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StoredMembershipRecord {
    log_id: Option<LogIdRecord>,
    membership: MembershipRecord,
}

impl From<&StoredMembershipOf> for StoredMembershipRecord {
    fn from(value: &StoredMembershipOf) -> Self {
        Self {
            log_id: value.log_id().clone().map(Into::into),
            membership: MembershipRecord::from_membership(value.membership()),
        }
    }
}

impl From<StoredMembershipOf> for StoredMembershipRecord {
    fn from(value: StoredMembershipOf) -> Self {
        Self::from(&value)
    }
}

impl TryFrom<StoredMembershipRecord> for StoredMembershipOf {
    type Error = io::Error;

    fn try_from(value: StoredMembershipRecord) -> Result<Self, Self::Error> {
        Ok(StoredMembership::new(
            value.log_id.map(Into::into),
            value.membership.into_membership()?,
        ))
    }
}

impl StoredMembershipRecord {
    pub(crate) fn from_stored(value: &StoredMembershipOf) -> Self {
        value.into()
    }

    pub(crate) fn into_stored(self) -> io::Result<StoredMembershipOf> {
        self.try_into()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        error::Error,
    };

    use super::*;

    fn log_id(term: u64, node_id: ClusterNodeName, index: u64) -> LogIdOf {
        LogId::new(
            openraft::impls::leader_id_adv::LeaderId { term, node_id },
            index,
        )
    }

    #[test]
    fn vote_and_log_id_records_preserve_raft_identity() -> Result<(), Box<dyn Error>> {
        let node_id = ClusterNodeName::parse("node-1")?;
        let votes: [VoteOf; 2] = [
            Vote::new(7, node_id.clone()),
            Vote::new_committed(7, node_id.clone()),
        ];
        for vote in votes {
            let expected = vote.clone();
            let restored: VoteOf = VoteRecord::from(vote).into();
            assert_eq!(restored, expected);
        }

        let log_id = log_id(7, node_id, 42);
        let restored: LogIdOf = LogIdRecord::from(log_id.clone()).into();
        assert_eq!(restored, log_id);
        Ok(())
    }

    #[test]
    fn entry_and_membership_records_preserve_every_payload_kind() -> Result<(), Box<dyn Error>> {
        let node_id = ClusterNodeName::parse("node-1")?;
        let membership = Membership::new(
            vec![BTreeSet::from([node_id.clone()])],
            BTreeMap::from([(node_id.clone(), BasicNode::new("https://node-1.invalid"))]),
        )?;
        let entries: [EntryOf<TypeConfig>; 3] = [
            Entry {
                log_id: log_id(7, node_id.clone(), 40),
                payload: EntryPayload::Blank,
            },
            Entry {
                log_id: log_id(7, node_id.clone(), 41),
                payload: EntryPayload::Normal(ConsensusCommand::SetNodeCordoned {
                    node_id: node_id.clone(),
                    cordoned: true,
                }),
            },
            Entry {
                log_id: log_id(7, node_id.clone(), 42),
                payload: EntryPayload::Membership(membership.clone()),
            },
        ];
        for entry in entries {
            let expected = entry.clone();
            let restored = EntryRecord::from(entry).into_entry()?;
            assert_eq!(restored, expected);
        }

        let stored = StoredMembership::new(Some(log_id(7, node_id, 42)), membership);
        let restored = StoredMembershipOf::try_from(StoredMembershipRecord::from(&stored))?;
        assert_eq!(restored, stored);
        Ok(())
    }
}
