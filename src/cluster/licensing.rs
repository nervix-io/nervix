//! Membership-scoped publication of the current licensing leader's connectivity.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** One bounded connectivity advertisement per live incarnation and its withdrawal.
//! - **Depends on.** Authenticated gossip, node identities and typed observation vocabulary.
//! - **Must not know.** Decisions, grants, request bodies or policy evaluation.

use meticulous::ResultExt as _;
use nervix_models::{LicensingConnectivity, LicensingConnectivityPublication};

use super::ClusterHandle;

pub(super) const KEY_LICENSING_CONNECTIVITY: &str = "licensing_connectivity";

impl ClusterHandle {
    pub(crate) async fn publish_licensing_connectivity(
        &self,
        term: Option<u64>,
        observation: LicensingConnectivity,
    ) {
        let leader = self.local_node_identity().await;
        let mut gossip = self.chitchat.lock().await;
        let state = gossip.self_node_state();
        match term {
            Some(term) => state.set(
                KEY_LICENSING_CONNECTIVITY,
                serde_json::to_string(&LicensingConnectivityPublication {
                    leader,
                    term,
                    observation,
                })
                .assured("bounded scalar connectivity serializes"),
            ),
            None => state.delete(KEY_LICENSING_CONNECTIVITY),
        }
    }

    pub(crate) async fn licensing_connectivity(
        &self,
        leader: &nervix_models::ClusterNodeName,
        term: u64,
    ) -> Option<LicensingConnectivityPublication> {
        let node = self.live_node(leader).await?;
        let publication = node.licensing?;
        if publication.leader.node_id() != leader
            || publication.leader.incarnation() != node.incarnation
            || publication.term != term
        {
            return None;
        }
        Some(publication)
    }
}
