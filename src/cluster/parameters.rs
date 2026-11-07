//! Gossip publication and discovery of committed and running cluster parameters.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Coherent parameter publication and discovery through authenticated gossip.
//! - **Depends on.** The parent cluster handle and the vocabulary's parameter records.
//! - **Must not know.** Parameter admission, command execution, graphs or runtime installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]
#![cfg_attr(
    nervix_lint,
    nervix::context(
        outside,
        reason = "gossip membership and management health coordinate topology outside record \
                  processing"
    )
)]

use std::time::Duration;

use meticulous::ResultExt as _;
use tracing::warn;

use super::{
    ClusterHandle, KEY_CLUSTER_PARAMETERS, peer_health_watcher::install_observation_freshness,
};

impl ClusterHandle {
    /// Publish a coherent committed/running pair; a joining node reads this live revision before
    /// activating its stored graph. Gossip authentication supplies the existing cluster boundary.
    pub(crate) async fn publish_cluster_parameters(
        &self,
        record: nervix_models::ClusterParameterRecord,
        running: bool,
    ) {
        let mut chitchat = self.chitchat.lock().await;
        let state = chitchat.self_node_state();
        let previous: Option<nervix_models::ClusterParameterPublication> = state
            .get(KEY_CLUSTER_PARAMETERS)
            .and_then(|text| serde_json::from_str(text).ok());
        let publication =
            previous.unwrap_or_else(|| nervix_models::ClusterParameterPublication::new(record));
        let next = if running {
            publication.install(record)
        } else {
            publication.observe(record)
        };
        match next {
            Ok(next) => {
                if let Some(applied) = next.running() {
                    install_observation_freshness(
                        &self.node_unavailability_timeout,
                        &self.peer_health_state,
                        applied.values.node_unavailability_timeout(),
                    );
                }
                state.set(
                    KEY_CLUSTER_PARAMETERS,
                    serde_json::to_string(&next)
                        .assured("parameter publications contain only bounded scalar values"),
                );
            }
            Err(error) => warn!(error = %error, "refused a contradictory parameter publication"),
        }
    }

    pub(crate) async fn live_cluster_parameters(
        &self,
    ) -> Option<nervix_models::ClusterParameterRecord> {
        let local = self.local_node_identity().await;
        self.gossip_state()
            .await
            .live_nodes
            .iter()
            .filter(|node| &node.node_id != local.node_id())
            .filter_map(|node| node.parameters.map(|publication| publication.committed()))
            .max_by_key(|record| record.revision)
    }

    pub(crate) fn node_unavailability_timeout(&self) -> Duration {
        **self.node_unavailability_timeout.load()
    }
}
