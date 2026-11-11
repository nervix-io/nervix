//! Coherent publication of committed peer receive claims by the control-plane owner.
//!
//! Layer: engines and infrastructure.
//! - **Owns.** A retained publication read through an already leased peer pool.
//! - **Depends on.** Typed committed records and the execution primitive publication boundary.
//! - **Must not know.** Consensus, membership policy, relay rows or acknowledgement shares.

#![cfg_attr(
    nervix_lint,
    nervix::layer(engine, reason = "owns retained peer capability publication")
)]

use nervix_models::{NodeCapabilityRecord, PeerReceiveBounds};
use nervix_primitives::{publication::ArcSwapOption, sync::StdArc};

#[derive(Default)]
pub(crate) struct CapabilityPublication {
    record: ArcSwapOption<NodeCapabilityRecord>,
}

impl CapabilityPublication {
    pub(crate) fn record(&self) -> Option<NodeCapabilityRecord> {
        self.record.load().as_deref().cloned()
    }
    /// The control plane supplies a complete committed view. Equal publications allocate nothing.
    pub(crate) fn publish(&self, record: Option<&NodeCapabilityRecord>) {
        let current = self.record.load();
        if current.as_deref() == record {
            return;
        }
        self.record
            .store(record.map(|record| StdArc::new(record.clone())));
    }

    pub(crate) fn receive_bounds(&self) -> Option<PeerReceiveBounds> {
        self.record
            .load()
            .as_ref()
            .map(|record| record.capabilities.peer_receive_bounds())
    }
}

#[cfg(all(test, feature = "shuttle"))]
mod tests {
    use std::num::NonZeroU64;

    use nervix_models::{
        ClusterNodeIdentity, ClusterNodeIncarnation, ClusterNodeName, NodeCapabilities,
    };
    use nervix_primitives::{sync::Arc, thread};

    use super::*;

    #[test]
    fn shuttle_publication_racing_re_slicing_preserves_every_admitted_row_once() {
        nervix_model_harness::shuttle::check_dfs(
            || {
                let publication = Arc::new(CapabilityPublication::default());
                let mut initial = NodeCapabilityRecord {
                    identity: ClusterNodeIdentity::new(
                        ClusterNodeName::parse("receiver").expect("node name"),
                        ClusterNodeIncarnation::new(1),
                    ),
                    capabilities: NodeCapabilities {
                        runtime: nervix_models::DataPlaneSettings::default().capabilities(),
                        shutdown: nervix_models::ShutdownSettings::default().capabilities(),
                        interconnect: crate::TransportSettings::default().capabilities(),
                        dns: nervix_arbitrary::Arbitrary::new(
                            &[],
                            nervix_arbitrary::Domain::Vocabulary,
                        )
                        .node_capability_record()
                        .capabilities
                        .dns,
                        node_database: nervix_arbitrary::Arbitrary::new(
                            &[],
                            nervix_arbitrary::Domain::Vocabulary,
                        )
                        .node_capability_record()
                        .capabilities
                        .node_database,
                        consensus_database: nervix_arbitrary::Arbitrary::new(
                            &[],
                            nervix_arbitrary::Domain::Vocabulary,
                        )
                        .node_capability_record()
                        .capabilities
                        .consensus_database,
                        consensus: nervix_arbitrary::Arbitrary::new(
                            &[],
                            nervix_arbitrary::Domain::Vocabulary,
                        )
                        .node_capability_record()
                        .capabilities
                        .consensus,
                        engines: nervix_arbitrary::Arbitrary::new(
                            &[],
                            nervix_arbitrary::Domain::Vocabulary,
                        )
                        .node_engine_capabilities(),
                        connector_host: nervix_arbitrary::Arbitrary::new(
                            &[],
                            nervix_arbitrary::Domain::Vocabulary,
                        )
                        .node_connector_host_capabilities(),
                        cpu_count: NonZeroU64::new(2),
                        memory_bytes: NonZeroU64::new(8 << 30),
                        receive: crate::PeerReceiveSettings::default().bounds(),
                    },
                };
                initial.capabilities.receive.relay_batch_bytes =
                    NonZeroU64::new(8).expect("positive");
                publication.publish(Some(&initial));
                let mut reduced = initial.clone();
                reduced.identity = ClusterNodeIdentity::new(
                    initial.identity.node_id().clone(),
                    ClusterNodeIncarnation::new(2),
                );
                reduced.capabilities.receive.relay_batch_bytes =
                    NonZeroU64::new(2).expect("positive");
                let writer = Arc::clone(&publication);
                let publish = thread::spawn(move || writer.publish(Some(&reduced)));
                let reader = Arc::clone(&publication);
                let send = thread::spawn(move || {
                    let mut cursor = crate::RelaySliceCursor::new(8);
                    let mut seen = [false; 8];
                    while cursor.admitted_rows() < cursor.total_rows() {
                        let observed = reader.receive_bounds().expect("published claim");
                        cursor.refuse(observed);
                        while u64::try_from(cursor.part_rows()).expect("eight rows")
                            > observed.relay_batch_bytes.get()
                        {
                            assert!(cursor.shrink());
                        }
                        let actual = reader.receive_bounds().expect("published claim");
                        if u64::try_from(cursor.part_rows()).expect("eight rows")
                            > actual.relay_batch_bytes.get()
                        {
                            cursor.refuse(actual);
                            continue;
                        }
                        let end = cursor
                            .admitted_rows()
                            .checked_add(cursor.part_rows())
                            .expect("eight rows");
                        for row in &mut seen[cursor.admitted_rows()..end] {
                            assert!(!*row, "a confirmed prefix is never sent twice");
                            *row = true;
                        }
                        cursor.admit();
                    }
                    assert!(seen.into_iter().all(|seen| seen));
                });
                publish.join().expect("publication thread");
                send.join().expect("slice owner thread");
                assert_eq!(
                    publication
                        .receive_bounds()
                        .expect("new claim")
                        .relay_batch_bytes
                        .get(),
                    2
                );
            },
            Some(1000),
        );
    }
}
