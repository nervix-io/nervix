//! Pure selection of one committed domain-clock authority from live voter incarnations.
//!
//! Layer: decisions.
//!
//! - **Owns.** Deterministic authority selection for a named domain.
//! - **Depends on.** Vocabulary identities and ordered collections.
//! - **Must not know.** Gossip, consensus, transport, Tokio or clock-production tasks.

use std::collections::BTreeMap;

use meticulous::ResultExt as _;
use nervix_models::{ClusterNodeIdentity, ClusterNodeName, DomainName};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DomainClockAuthorityCandidates {
    identities: Vec<ClusterNodeIdentity>,
}

impl DomainClockAuthorityCandidates {
    pub(crate) fn new(identities: impl IntoIterator<Item = ClusterNodeIdentity>) -> Self {
        let mut current = BTreeMap::<ClusterNodeName, ClusterNodeIdentity>::new();
        for identity in identities {
            let node_id = identity.node_id().clone();
            if current
                .get(&node_id)
                .is_none_or(|observed| observed.incarnation() < identity.incarnation())
            {
                current.insert(node_id, identity);
            }
        }
        Self {
            identities: current.into_values().collect(),
        }
    }

    pub(crate) fn owner_for(&self, domain: &DomainName) -> Option<ClusterNodeIdentity> {
        if self.identities.is_empty() {
            return None;
        }

        // Wrapping is the definition of this stable byte mixer. It gives every node the same
        // bounded value independently of the domain-name length.
        let mut hash = 0_u64;
        for byte in domain.as_str().bytes() {
            hash = hash.wrapping_mul(131).wrapping_add(u64::from(byte));
        }
        let candidate_count = u64::try_from(self.identities.len())
            .assured("a collection length always fits in u64 on supported targets");
        let index = usize::try_from(hash % candidate_count)
            .assured("the remainder is below a collection length represented by usize");
        self.identities.get(index).cloned()
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{ClusterNodeIncarnation, ClusterNodeName};

    use super::*;

    fn identity(name: &str, incarnation: u64) -> ClusterNodeIdentity {
        ClusterNodeIdentity::new(
            ClusterNodeName::parse(name).expect("fixture node names are valid"),
            ClusterNodeIncarnation::new(incarnation),
        )
    }

    #[test]
    fn membership_change_selects_one_deterministic_authority() {
        let domain = DomainName::parse("authority_domain").expect("fixture domain name is valid");
        let two =
            DomainClockAuthorityCandidates::new([identity("node-1", 1), identity("node-2", 1)]);
        let three = DomainClockAuthorityCandidates::new([
            identity("node-1", 1),
            identity("node-2", 1),
            identity("node-3", 1),
        ]);

        assert_eq!(two.owner_for(&domain), Some(identity("node-1", 1)));
        assert_eq!(three.owner_for(&domain), Some(identity("node-2", 1)));
    }

    #[test]
    fn restarted_owner_is_a_distinct_authority_candidate() {
        let domain = DomainName::parse("authority_domain").expect("fixture domain name is valid");
        let before =
            DomainClockAuthorityCandidates::new([identity("node-1", 10), identity("node-2", 20)]);
        let after =
            DomainClockAuthorityCandidates::new([identity("node-1", 11), identity("node-2", 20)]);

        assert_ne!(before.owner_for(&domain), after.owner_for(&domain));
    }

    #[test]
    fn duplicate_node_observations_keep_only_the_newest_incarnation() {
        let candidates = DomainClockAuthorityCandidates::new([
            identity("node-1", 10),
            identity("node-1", 11),
            identity("node-2", 20),
        ]);

        assert_eq!(candidates.identities.len(), 2);
        assert!(candidates.identities.contains(&identity("node-1", 11)));
        assert!(!candidates.identities.contains(&identity("node-1", 10)));
    }
}
