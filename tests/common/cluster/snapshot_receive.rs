//! Test harness for snapshot receive admission through authenticated public interconnect APIs.
//! Owns: the probe transport, complete checkpoint oracle and scenario-owned memory hold.
//! Depends on: cluster credentials, typed snapshot operations, execution faults and public metrics.
//! Must not know: runtime maps, native checkpoint layouts or state installation.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "owns the test harness boundary declared above")
)]

use nervix_interconnect::{
    FetchStateCheckpoint, RequestError, StateCheckpointRead, StatePlacementEnvelope,
    StateSnapshotEnvelope, StateSyncRequest, TransportError,
};
use nervix_primitives::{sync::oneshot, task::JoinHandle};
use nervix_recovery::Discarded as _;

use super::*;

struct SnapshotProbe {
    transport: Transport,
    end: oneshot::Sender<()>,
    cleanup: JoinHandle<()>,
}

struct BulkHold {
    faults: FaultInjection,
    node: ClusterNodeName,
}

impl Drop for BulkHold {
    fn drop(&mut self) {
        self.faults.release_bulk_memory_on(&self.node);
    }
}

impl Cluster {
    async fn snapshot_probe(&self, node: &str) -> io::Result<SnapshotProbe> {
        let target = self
            .nodes
            .get(node)
            .ok_or_else(|| io::Error::other("unknown snapshot owner"))?;
        let directory = tempfile::tempdir_in(self.roots.storage.path())?;
        let probe = node_name("snapshot-probe");
        let (certificate, key) =
            self.interconnect_ca
                .issue_node(TEST_CLUSTER_ID, probe.as_ref(), directory.path())?;
        let tls = TlsConfigBundle::from_pem_files(
            &self.interconnect_ca.path,
            certificate,
            key,
            TransportClock::system(),
        )
        .map_err(io::Error::other)?;
        let dns = DnsResolver::load(DnsConfiguration::system())
            .await
            .map_err(|error| io::Error::other(format!("{error:#}")))?;
        let (transport, _incoming) = Transport::bind(
            "127.0.0.1:0"
                .parse()
                .assured("the probe uses a literal loopback address"),
            TransportIdentity {
                cluster_id: TEST_CLUSTER_ID.to_owned(),
                node_id: probe,
                advertised_host: "localhost".to_owned(),
                join_terms: target.config.node_settings(node)?.join_terms(),
            },
            tls,
            TransportOptions::default(),
            Executor::default(),
            PeerResolver::new(dns),
        )
        .await
        .map_err(io::Error::other)?;
        let (end, ended) = oneshot::channel();
        let closing = transport.clone();
        let cleanup = nervix_primitives::task::spawn(async move {
            ended
                .await
                .discarded("ending the probe's owner releases its transport");
            closing.shutdown().await;
            drop(directory);
        });
        let probe = SnapshotProbe {
            transport,
            end,
            cleanup,
        };
        probe
            .transport
            .register_outbound_target(
                node_name(node),
                PeerTarget::new(target.spec.interconnect_listen_addr(), "localhost").endpoint(),
            )
            .map_err(io::Error::other)?;
        Ok(probe)
    }

    pub(crate) async fn verify_snapshot_receive_admission(
        &self,
        node: &str,
        placements: Vec<StatePlacementEnvelope>,
    ) -> io::Result<()> {
        let probe = self.snapshot_probe(node).await?;
        let result = self
            .verify_snapshot_placements(&probe, node, placements)
            .await;
        probe.shutdown().await;
        result
    }

    async fn verify_snapshot_placements(
        &self,
        probe: &SnapshotProbe,
        node: &str,
        placements: Vec<StatePlacementEnvelope>,
    ) -> io::Result<()> {
        for placement in placements {
            nervix_primitives::task::consume_budget().await;
            let described = probe
                .transport
                .request(
                    &node_name(node),
                    StateSyncRequest {
                        placement: placement.clone(),
                        after_lsm: None,
                    },
                )
                .await
                .map_err(io::Error::other)?;
            let snapshot = described
                .result
                .map_err(io::Error::other)?
                .ok_or_else(|| io::Error::other("the populated branch has no checkpoint"))?;
            let request = FetchStateCheckpoint {
                placement,
                lsm: snapshot.lsm,
                read: StateCheckpointRead::Published,
            };
            let baseline = probe.fetch(node, request.clone(), &snapshot).await?;
            self.fault_injection
                .hold_bulk_memory_leaving_room_on(&node_name(node), 4096);
            let hold = BulkHold {
                faults: self.fault_injection.clone(),
                node: node_name(node),
            };
            let refused = probe
                .transport
                .request_stream(&node_name(node), request.clone())
                .await;
            match refused {
                Err(error) => {
                    assert!(
                        matches!(
                            error.downcast_ref::<TransportError>(),
                            Some(TransportError::RemoteRejected { status: 503, .. })
                        ),
                        "snapshot receive admission must answer 503 before the request deadline: \
                         {error:#}"
                    );
                    assert!(error.contains::<RequestError>());
                }
                Ok(_) => panic!("the receive allowance must refuse the overlapping decode"),
            }
            drop(hold);
            let recovered = probe.fetch(node, request, &snapshot).await?;
            assert_eq!(
                recovered, baseline,
                "the exact complete checkpoint survives receive refusal"
            );
        }
        Ok(())
    }
}

impl SnapshotProbe {
    async fn shutdown(self) {
        drop(self.end);
        self.cleanup
            .await
            .assured("the probe cleanup task completes bounded transport shutdown");
    }

    async fn fetch(
        &self,
        node: &str,
        request: FetchStateCheckpoint,
        snapshot: &StateSnapshotEnvelope,
    ) -> io::Result<Vec<u8>> {
        assert!(
            snapshot.length <= 4 * 1024 * 1024,
            "the fixture checkpoint is bounded"
        );
        let mut stream = self
            .transport
            .request_stream(&node_name(node), request)
            .await
            .map_err(io::Error::other)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.map_err(io::Error::other)? {
            nervix_primitives::task::consume_budget().await;
            bytes.extend_from_slice(&chunk);
        }
        assert_eq!(
            u64::try_from(bytes.len()).assured("the fixture length fits"),
            snapshot.length
        );
        assert_eq!(*blake3::hash(&bytes).as_bytes(), snapshot.digest);
        Ok(bytes)
    }
}
