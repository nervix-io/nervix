//! Append-stream admission at the boundary with the caller's request stream.
//!
//! Layer: test harness.
//!
//! - **Owns.** Retry-gate and ended-input regressions against the actual network client.
//! - **Depends on.** Consensus, interconnect, execution primitives and explicit test fixtures.
//! - **Must not know.** Runtime graph placement or the commands a replicated entry contains.

use std::{collections::BTreeSet, time::Duration};

use futures_util::{StreamExt as _, stream};
use nervix_dns::{DnsConfiguration, DnsResolver, NameServers};
use nervix_interconnect::{
    JoinTerms, PeerResolver, TlsConfigBundle, Transport, TransportClock, TransportIdentity,
    TransportOptions,
};
use nervix_models::{ClusterNodeName, PrerequisiteTimings};
use nervix_primitives::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, sleep},
};
use openraft::{
    Vote,
    network::{RPCOption, RaftNetworkV2 as _},
};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};

use super::AppendPath;
use crate::{ConnectivityFault, NetworkClient, NetworkFactory, SnapshotTransferIds, TypeConfig};

async fn client_with_departed_target() -> NetworkClient<()> {
    let local = ClusterNodeName::parse("node-1").expect("valid local node");
    let target = ClusterNodeName::parse("node-2").expect("valid target node");
    let cluster = "append-admission";
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().expect("test CA key");
    let ca = ca_params.self_signed(&ca_key).expect("test CA certificate");
    let mut params =
        CertificateParams::new(vec!["localhost".to_string()]).expect("valid endpoint SAN");
    params.subject_alt_names.push(SanType::URI(
        format!("nervix://cluster/{cluster}/node/{local}")
            .try_into()
            .expect("valid identity URI"),
    ));
    let key = KeyPair::generate().expect("test node key");
    let cert = params
        .signed_by(&key, &ca, &ca_key)
        .expect("test node certificate");
    let tls = TlsConfigBundle::from_pem(
        ca.pem().as_bytes(),
        cert.pem().as_bytes(),
        key.serialize_pem().as_bytes(),
        TransportClock::system(),
    )
    .expect("test TLS configuration");
    let directory = tempfile::tempdir().expect("test resolver directory");
    let resolver_configuration = directory.path().join("resolv.conf");
    let hosts_file = directory.path().join("hosts");
    std::fs::write(&resolver_configuration, "nameserver 127.0.0.1\n")
        .expect("test resolver configuration");
    std::fs::write(&hosts_file, "127.0.0.1 localhost\n").expect("test hosts file");
    let dns = DnsResolver::load(DnsConfiguration {
        resolver_configuration,
        hosts_file,
        name_servers: NameServers::Explicit(vec![
            "127.0.0.1:5353".parse().expect("test nameserver"),
        ]),
    })
    .await
    .expect("test resolver loads its explicit fixtures");
    let executor = nervix_execution::Executor::default();
    let (transport, _incoming) = Transport::bind(
        "127.0.0.1:0".parse().expect("test listen address"),
        TransportIdentity {
            cluster_id: cluster.to_string(),
            node_id: local.clone(),
            advertised_host: "localhost".to_string(),
            join_terms: test_join_terms(),
        },
        tls,
        TransportOptions::default(),
        executor.clone(),
        PeerResolver::new(dns),
    )
    .await
    .expect("test transport binds");
    transport.replace_live_nodes(&BTreeSet::from([local.clone()]));
    NetworkFactory {
        local_node_id: local,
        interconnect: transport,
        executor,
        connectivity: ConnectivityFault::default(),
        append_stream_open_recorder: (),
        snapshot_transfer_ids: Arc::new(SnapshotTransferIds::new()),
    }
    .client(target, AppendPath::Replication)
}

#[nervix_primitives::test]
async fn failed_append_setup_observes_the_callers_retry_gate() {
    let mut client = client_with_departed_target().await;
    let transport = client.interconnect.clone();
    let passed_gate = Arc::new(AtomicBool::new(false));
    let observed_gate = passed_gate.clone();
    let vote = Vote::new_committed(1, client.local_node_id.clone());
    let delay = Duration::from_millis(200);
    let input = stream::once(async move {
        sleep(delay).await;
        passed_gate.store(true, Ordering::Release);
        openraft::raft::AppendEntriesRequest::<TypeConfig> {
            vote,
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: None,
        }
    })
    .boxed();
    let started = Instant::now();
    let result = client
        .stream_append(input, RPCOption::new(Duration::from_millis(250)))
        .await;
    let elapsed = started.elapsed();
    transport.shutdown().await;
    assert!(
        result.is_err(),
        "the departed peer rejects stream admission"
    );
    assert!(
        observed_gate.load(Ordering::Acquire),
        "failed stream setup must first pass the caller's retry gate"
    );
    assert!(
        elapsed >= delay,
        "the caller's delay bounds immediate admission failures"
    );
}

#[nervix_primitives::test]
async fn an_ended_append_input_finishes_without_opening_a_transport() {
    let mut client = client_with_departed_target().await;
    let transport = client.interconnect.clone();
    let result = client
        .stream_append(stream::empty(), RPCOption::new(Duration::from_millis(250)))
        .await;
    transport.shutdown().await;
    let mut answers = match result {
        Ok(answers) => answers,
        Err(error) => {
            panic!("ended input must finish without contacting the departed peer: {error}")
        }
    };
    assert!(
        answers.next().await.is_none(),
        "ended input has no append acknowledgement"
    );
}

/// The join terms every test transport brings, so their handshakes accept each other.
fn test_join_terms() -> JoinTerms {
    JoinTerms {
        durability: crate::RAFT_LOG_DURABILITY,
        timings: PrerequisiteTimings {
            raft_heartbeat_interval: crate::RaftTimings::default().heartbeat_interval(),
            gossip_interval: Duration::from_millis(500),
            clock_skew_tolerance: Duration::from_millis(500),
        },
    }
}
