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
            .load()
            .iter()
            .filter(|entry| accepted.get(entry.0) != Some(&entry.1.endpoint))
            .map(|entry| entry.0.clone())
            .collect::<BTreeSet<_>>();
        for node in removed {
            self.remove_target(&node);
        }

        for (node, endpoint) in accepted {
            if let Ok(outbound) =
                self.install_outbound_target(node, endpoint, OutboundDial::Advertised)
            {
                self.ensure_preconnected_slots(&outbound);
            }
        }
    }

    pub(crate) fn register_outbound_target(
        &self,
        node_id: ClusterNodeName,
        endpoint: NodeEndpoint,
    ) -> Result<(), Report<TransportError>> {
        let outbound = self.install_outbound_target(node_id, endpoint, OutboundDial::Advertised)?;
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
    ) -> Result<Arc<OutboundTarget>, Report<TransportError>> {
        let endpoint = target.endpoint();
        let current = self.targets.load().get(&node_id).cloned();
        if let Some(current) = current
            && current.endpoint == endpoint
        {
            return Ok(current);
        }
        self.install_outbound_target(node_id, endpoint, OutboundDial::Authenticated(target.addr))
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
    ) -> Result<Arc<OutboundTarget>, Report<TransportError>> {
        loop {
            let current = self.targets.load_full();
            let registered = current.get(&node_id);
            if registered.is_none() && current.len() >= self.options.max_peers {
                return Err(Report::new(TransportError::PoolExhausted));
            }
            let outbound = if let Some(registered) = registered
                && registered.endpoint == endpoint
            {
                if registered.dial == dial {
                    return Ok(registered.clone());
                }
                Arc::new(registered.with_dial(dial))
            } else {
                if let Some(registered) = registered {
                    self.retire_target(registered);
                }
                Arc::new(OutboundTarget::new(&node_id, endpoint.clone(), dial))
            };
            let mut next = (*current).clone();
            next.insert(node_id.clone(), outbound.clone());
            let observed = self.targets.compare_and_swap(&current, StdArc::new(next));
            if StdArc::ptr_eq(&current, &observed) {
                self.connection_changed.notify_waiters();
                return Ok(outbound);
            }
        }
    }

    pub(super) fn remove_target(&self, node_id: &ClusterNodeName) {
        let current = self.targets.load_full();
        let Some(target) = current.get(node_id).cloned() else {
            return;
        };
        self.retire_target(&target);
        self.withdraw_target(node_id, &target);
    }

    pub(super) fn withdraw_target(&self, node_id: &ClusterNodeName, target: &Arc<OutboundTarget>) {
        let mut current = self.targets.load_full();
        loop {
            let Some(published) = current.get(node_id) else {
                return;
            };
            // A dial-policy publication retains the exact pool lifetime under a new wrapper.
            // Only a newly created pool may survive withdrawal of this retained owner.
            if !Arc::ptr_eq(&published.pool, &target.pool) {
                return;
            }
            let mut next = (*current).clone();
            next.remove(node_id);
            let observed = self.targets.compare_and_swap(&current, StdArc::new(next));
            if StdArc::ptr_eq(&current, &observed) {
                return;
            }
            current = StdArc::clone(&observed);
        }
    }

    pub(super) fn retire_target(&self, target: &OutboundTarget) {
        for slots in target.pool.slots.iter() {
            for slot in slots.iter() {
                self.retire_slot(slot);
            }
        }
    }
}
