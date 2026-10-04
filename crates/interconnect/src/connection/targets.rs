//! Outbound peer target registration and exact retirement.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** Endpoint selection, topology admission and retirement of target-owned pool slots.
//! - **Depends on.** The transport owner, vocabulary identities and primitive publications.
//! - **Must not know.** Runtime graphs, scheduling or connector behavior.

use super::*;

impl TransportState {
    pub(crate) fn replace_outbound_targets(
        &self,
        endpoints: &BTreeMap<ClusterNodeName, NodeEndpoint>,
    ) {
        let accepted = endpoints
            .iter()
            .take(self.options.max_peers)
            .map(|(node, endpoint)| (node.clone(), endpoint.clone()))
            .collect::<BTreeMap<_, _>>();

        // Retire in node order, not map order, so the cancellations a replacement causes happen in
        // the same sequence in every process.
        let removed = self
            .targets
            .iter()
            .filter(|entry| accepted.get(entry.key()) != Some(&entry.value().endpoint))
            .map(|entry| entry.key().clone())
            .collect::<BTreeSet<_>>();
        for node in removed {
            self.targets.remove(&node);
            self.cancel_slots_for_node(&node);
        }

        for (node, endpoint) in accepted {
            let outbound = self.install_outbound_target(node, endpoint, OutboundDial::Advertised);
            self.ensure_preconnected_slots(&outbound);
        }
    }

    pub(crate) fn register_outbound_target(
        &self,
        node_id: ClusterNodeName,
        endpoint: NodeEndpoint,
    ) -> Result<(), Report<TransportError>> {
        if !self.has_room_for(&node_id) {
            return Err(Report::new(TransportError::PoolExhausted));
        }
        let outbound = self.install_outbound_target(node_id, endpoint, OutboundDial::Advertised);
        self.ensure_preconnected_slots(&outbound);
        Ok(())
    }

    /// Install the address a bootstrap exchange authenticated as `node_id`. An endpoint the node
    /// already has keeps how it is dialled, so a later bootstrap never narrows an advertised
    /// endpoint down to one of its addresses.
    pub(super) fn install_authenticated_target(
        &self,
        node_id: ClusterNodeName,
        target: PeerTarget,
    ) -> Arc<OutboundTarget> {
        let endpoint = target.endpoint();
        let current = self
            .targets
            .get(&node_id)
            .map(|current| Arc::clone(current.value()));
        if let Some(current) = current
            && current.endpoint == endpoint
        {
            return current;
        }
        self.install_outbound_target(node_id, endpoint, OutboundDial::Authenticated(target.addr))
    }

    /// Whether `node_id` fits the topology limit: a node that already has a target always does.
    pub(super) fn has_room_for(&self, node_id: &ClusterNodeName) -> bool {
        self.targets.contains_key(node_id) || self.targets.len() < self.options.max_peers
    }

    /// Make `endpoint` the endpoint `node_id` is reached at, dialled through `dial`, and return its
    /// registration. A different endpoint replaces the registered one and cancels the slots that
    /// belonged to it. The same endpoint keeps its slots and connections, and only a different
    /// `dial` changes where the next connection goes.
    pub(super) fn install_outbound_target(
        &self,
        node_id: ClusterNodeName,
        endpoint: NodeEndpoint,
        dial: OutboundDial,
    ) -> Arc<OutboundTarget> {
        let current = self
            .targets
            .get(&node_id)
            .map(|current| Arc::clone(current.value()));
        if let Some(current) = current
            && current.endpoint == endpoint
        {
            if current.dial == dial {
                return current;
            }
            let redialled = Arc::new(current.with_dial(dial));
            self.targets.insert(node_id, Arc::clone(&redialled));
            return redialled;
        }
        self.cancel_slots_for_node(&node_id);
        let outbound = Arc::new(OutboundTarget::new(&node_id, endpoint, dial));
        self.targets.insert(node_id, Arc::clone(&outbound));
        outbound
    }

}
