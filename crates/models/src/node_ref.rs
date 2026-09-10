//! How one entity of a domain is addressed, in one type.
//!
//! An entity is named by two things that only mean something together: the kind that says what it
//! is and the identifier that names it within its kind. Two entities of different kinds may share
//! an identifier, so neither half addresses one on its own. [`NodeRef`] carries the pair, and
//! [`DomainNodeRef`] adds the domain that owns it, because every entity is domain-owned and the
//! same name in two domains names two independent entities.
//!
//! Most holders of a [`NodeRef`] address a node of an execution graph, which is where the name
//! comes from; the registry addresses everything it stores the same way, so a schema or a codec is
//! referenced by the same pair. These are the only shapes that carry that pair. Storage keys,
//! schedules, placements, relocations, quiesce plans, the interconnect's control envelopes, and
//! the runtime's per-node maps all key on one of them rather than spelling the pair again beside
//! each collection.

use std::cmp::Ordering;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

use crate::{DomainName, ModelKind, ModelName};

/// One node of an execution graph, addressed by its kind and its identifier.
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
pub struct NodeRef {
    pub kind: ModelKind,
    pub identifier: ModelName,
}

impl NodeRef {
    /// A reference to the node of `kind` named `identifier`. The name of any model widens into the
    /// kind-erased [`ModelName`], so a caller holding a `RelayName` names the relay directly.
    pub fn new(kind: ModelKind, identifier: impl Into<ModelName>) -> Self {
        Self {
            kind,
            identifier: identifier.into(),
        }
    }

    /// This node as it is addressed inside `domain`.
    pub fn in_domain(&self, domain: &DomainName) -> DomainNodeRef {
        DomainNodeRef::new(domain.clone(), self.clone())
    }
}

impl Ord for NodeRef {
    /// Orders nodes by kind name and then identifier. The order is derived from the names
    /// themselves rather than from the declaration order of [`ModelKind`], so every cluster node
    /// sorts a set of nodes identically and applies the same change in the same order.
    fn cmp(&self, other: &Self) -> Ordering {
        self.kind
            .as_str()
            .cmp(other.kind.as_str())
            .then_with(|| self.identifier.cmp(&other.identifier))
    }
}

impl PartialOrd for NodeRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// One node as the domain that owns it addresses it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DomainNodeRef {
    pub domain: DomainName,
    pub node: NodeRef,
}

impl DomainNodeRef {
    pub fn new(domain: DomainName, node: NodeRef) -> Self {
        Self { domain, node }
    }

    /// A reference to the node of `kind` named `identifier` in `domain`.
    pub fn node_in(domain: DomainName, kind: ModelKind, identifier: impl Into<ModelName>) -> Self {
        Self::new(domain, NodeRef::new(kind, identifier))
    }

    pub fn kind(&self) -> ModelKind {
        self.node.kind
    }

    pub fn identifier(&self) -> &ModelName {
        &self.node.identifier
    }
}

#[cfg(test)]
mod tests {
    use super::NodeRef;
    use crate::{DomainName, ModelKind, ModelName};

    fn named<N>(raw: &str) -> N
    where
        N: for<'a> TryFrom<&'a str>,
        for<'a> <N as TryFrom<&'a str>>::Error: std::fmt::Debug,
    {
        N::try_from(raw).expect("valid name")
    }

    #[test]
    fn kind_distinguishes_nodes_that_share_an_identifier() {
        let relay = NodeRef::new(ModelKind::Relay, named::<ModelName>("orders"));
        let emitter = NodeRef::new(ModelKind::Emitter, named::<ModelName>("orders"));

        assert_ne!(relay, emitter);
    }

    #[test]
    fn nodes_order_by_kind_name_then_identifier() {
        let mut nodes = vec![
            NodeRef::new(ModelKind::Relay, named::<ModelName>("orders")),
            NodeRef::new(ModelKind::Emitter, named::<ModelName>("shipments")),
            NodeRef::new(ModelKind::Emitter, named::<ModelName>("orders")),
        ];
        nodes.sort();

        assert_eq!(
            nodes,
            vec![
                NodeRef::new(ModelKind::Emitter, named::<ModelName>("orders")),
                NodeRef::new(ModelKind::Emitter, named::<ModelName>("shipments")),
                NodeRef::new(ModelKind::Relay, named::<ModelName>("orders")),
            ]
        );
    }

    #[test]
    fn the_same_node_name_in_two_domains_is_two_nodes() {
        let node = NodeRef::new(ModelKind::Relay, named::<ModelName>("orders"));
        let staging = node.in_domain(&named::<DomainName>("staging"));
        let production = node.in_domain(&named::<DomainName>("production"));

        assert_ne!(staging, production);
        assert_eq!(staging.identifier(), production.identifier());
        assert_eq!(staging.kind(), ModelKind::Relay);
    }
}
