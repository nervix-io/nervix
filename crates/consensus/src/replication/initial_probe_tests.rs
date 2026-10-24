//! Initial replication probes at the management and append-stream boundary.
//!
//! Layer: test harness.
//! - **Owns.** Real transport evidence for initial empty probes followed by ordered data batches.
//! - **Depends on.** Consensus, interconnect, execution primitives and explicit TLS/DNS fixtures.
//! - **Must not know.** Runtime graph placement or the commands a replicated entry contains.

use std::{collections::BTreeSet, time::Duration};

use futures_util::{StreamExt as _, stream};
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_dns::{DnsConfiguration, DnsResolver, NameServers};
use nervix_interconnect::{
    DuplexResponses, JoinTerms, PeerResolver, TlsConfigBundle, Transport, TransportClock,
    TransportIdentity, TransportOptions,
};
use nervix_models::{ClusterNodeName, PrerequisiteTimings};
use nervix_primitives::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use openraft::{
    Entry, EntryPayload, Vote,
    network::{RPCOption, RaftNetworkV2 as _},
    vote::RaftLeaderId as _,
};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};

use super::AppendPath;
use crate::{
    CommittedLeaderIdOf, ConnectivityFault, LogIdOf, NetworkClient, NetworkFactory,
    SnapshotTransferIds, TypeConfig, wire,
};

struct ProbePeer {
    client: NetworkClient<()>,
    follower: Transport,
    management_calls: Arc<AtomicUsize>,
    append_streams: Arc<AtomicUsize>,
}

async fn probe_peer(probe_response: wire::AppendEntriesResponseRecord) -> ProbePeer {
    let leader = ClusterNodeName::parse("node-1").assured("valid leader");
    let follower = ClusterNodeName::parse("node-2").assured("valid follower");
    let cluster = "initial-append-probes";
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate().assured("test CA key");
    let ca = ca_params
        .self_signed(&ca_key)
        .assured("test CA certificate");
    let tls = |node: &ClusterNodeName| {
        let mut params =
            CertificateParams::new(vec!["localhost".to_string()]).assured("valid endpoint SAN");
        params.subject_alt_names.push(SanType::URI(
            format!("nervix://cluster/{cluster}/node/{node}")
                .try_into()
                .assured("valid identity URI"),
        ));
        let key = KeyPair::generate().assured("test node key");
        let cert = params
            .signed_by(&key, &ca, &ca_key)
            .assured("test certificate");
        TlsConfigBundle::from_pem(
            ca.pem().as_bytes(),
            cert.pem().as_bytes(),
            key.serialize_pem().as_bytes(),
            TransportClock::system(),
        )
        .assured("test TLS configuration")
    };
    let directory = tempfile::tempdir().assured("test resolver directory");
    let configuration = directory.path().join("resolv.conf");
    let hosts = directory.path().join("hosts");
    std::fs::write(&configuration, "nameserver 127.0.0.1\n").assured("test resolver configuration");
    std::fs::write(&hosts, "127.0.0.1 localhost\n").assured("test hosts file");
    let dns = DnsResolver::load(DnsConfiguration {
        resolver_configuration: configuration,
        hosts_file: hosts,
        name_servers: NameServers::Explicit(vec![
            "127.0.0.1:5353".parse().assured("test nameserver"),
        ]),
    })
    .await
    .assured("test resolver loads");
    let executor = nervix_execution::Executor::default();
    let bind = |node: ClusterNodeName| {
        let tls = tls(&node);
        let executor = executor.clone();
        let resolver = PeerResolver::new(dns.clone());
        async move {
            Transport::bind(
                "127.0.0.1:0".parse().assured("test listen address"),
                TransportIdentity {
                    cluster_id: cluster.to_string(),
                    node_id: node,
                    advertised_host: "localhost".to_string(),
                    join_terms: test_join_terms(),
                },
                tls,
                TransportOptions::default(),
                executor,
                resolver,
            )
            .await
            .assured("test transport binds")
            .0
        }
    };
    let leader_transport = bind(leader.clone()).await;
    let follower_transport = bind(follower.clone()).await;
    leader_transport
        .register_outbound_target(
            follower.clone(),
            format!("localhost:{}", follower_transport.local_addr().port())
                .parse()
                .assured("valid follower endpoint"),
        )
        .assured("test follower registers");
    let live = BTreeSet::from([leader.clone(), follower.clone()]);
    leader_transport.replace_live_nodes(&live);
    follower_transport.replace_live_nodes(&live);
    let management_calls = Arc::new(AtomicUsize::new(0));
    follower_transport
        .register_handler::<wire::HeartbeatRequest, _, _>({
            let calls = management_calls.clone();
            move |_context, request| {
                calls.fetch_add(1, Ordering::Relaxed);
                let response = probe_response.clone();
                async move {
                    let request = request.0.into_request().assured("valid probe");
                    assert!(
                        request.entries.is_empty(),
                        "management carries no data batch"
                    );
                    Ok(response)
                }
            }
        })
        .assured("test management handler registers");
    let append_streams = Arc::new(AtomicUsize::new(0));
    follower_transport
        .register_duplex_handler::<wire::OpenAppendStream, _, _>({
            let opened = append_streams.clone();
            move |_context, _opening, items| {
                opened.fetch_add(1, Ordering::Relaxed);
                async move {
                    let answers = stream::unfold(items, |mut items| async move {
                        let charged = match items.next().await.assured("test frame decodes") {
                            Some(charged) => charged,
                            None => return None,
                        };
                        let request = charged.item.into_request().assured("valid append request");
                        let matching = request
                            .entries
                            .last()
                            .map(|entry| entry.log_id.clone())
                            .or(request.prev_log_id);
                        let result: openraft::raft::StreamAppendResult<TypeConfig> = Ok(matching);
                        Some((Ok(Ok(wire::StreamAppendResultRecord::from(result))), items))
                    });
                    Ok(DuplexResponses::new(answers))
                }
            }
        })
        .assured("test append handler registers");
    let client = NetworkFactory {
        local_node_id: leader,
        interconnect: leader_transport,
        executor,
        connectivity: ConnectivityFault::default(),
        append_stream_open_recorder: (),
        snapshot_transfer_ids: Arc::new(SnapshotTransferIds::new()),
    }
    .client(follower, AppendPath::Replication);
    ProbePeer {
        client,
        follower: follower_transport,
        management_calls,
        append_streams,
    }
}

#[nervix_primitives::test]
async fn initial_empty_append_probes_use_management_before_one_data_stream() {
    let mut peer = probe_peer(wire::AppendEntriesResponseRecord::Success).await;
    let leader = peer.client.local_node_id.clone();
    let committed = CommittedLeaderIdOf::<TypeConfig>::new(1, leader.clone());
    let vote = Vote::new_committed(1, leader);
    let boundary = LogIdOf::new(committed.clone(), 11);
    let option = RPCOption::new(Duration::from_secs(5));
    for _ in 0..2 {
        nervix_primitives::task::consume_budget().await;
        let input = stream::iter([openraft::raft::AppendEntriesRequest::<TypeConfig> {
            vote: vote.clone(),
            prev_log_id: Some(boundary.clone()),
            entries: Vec::new(),
            leader_commit: Some(boundary.clone()),
        }]);
        let mut answers = peer
            .client
            .stream_append(input, option.clone())
            .await
            .assured("probe opens");
        assert_eq!(
            answers
                .next()
                .await
                .assured("probe answer")
                .assured("probe succeeds"),
            Ok(Some(boundary.clone()))
        );
        assert!(
            answers.next().await.is_none(),
            "the finite probe input ends"
        );
    }
    let next = LogIdOf::new(committed.clone(), 12);
    let input = stream::iter([openraft::raft::AppendEntriesRequest::<TypeConfig> {
        vote,
        prev_log_id: Some(boundary),
        entries: vec![Entry {
            log_id: next.clone(),
            payload: EntryPayload::Blank,
        }],
        leader_commit: Some(next.clone()),
    }]);
    let mut answers = peer
        .client
        .stream_append(input, option)
        .await
        .assured("data stream opens");
    assert_eq!(
        answers
            .next()
            .await
            .assured("data answer")
            .assured("data succeeds"),
        Ok(Some(next))
    );
    assert!(answers.next().await.is_none(), "the finite data input ends");
    drop(answers);
    peer.client.interconnect.shutdown().await;
    peer.follower.shutdown().await;
    assert_eq!(
        peer.management_calls.load(Ordering::Relaxed),
        2,
        "initial empty probes belong to the management pool"
    );
    assert_eq!(
        peer.append_streams.load(Ordering::Relaxed),
        1,
        "only the data input opens a replication stream"
    );
}

impl ProbePeer {
    fn log_id(&self, index: u64) -> LogIdOf {
        let leader = CommittedLeaderIdOf::<TypeConfig>::new(1, self.client.local_node_id.clone());
        LogIdOf::new(leader, index)
    }

    fn empty_request(
        &self,
        boundary: Option<LogIdOf>,
    ) -> openraft::raft::AppendEntriesRequest<TypeConfig> {
        openraft::raft::AppendEntriesRequest {
            vote: Vote::new_committed(1, self.client.local_node_id.clone()),
            prev_log_id: boundary.clone(),
            entries: Vec::new(),
            leader_commit: boundary,
        }
    }

    fn data_request(
        &self,
        boundary: LogIdOf,
        next: LogIdOf,
    ) -> openraft::raft::AppendEntriesRequest<TypeConfig> {
        openraft::raft::AppendEntriesRequest {
            vote: Vote::new_committed(1, self.client.local_node_id.clone()),
            prev_log_id: Some(boundary),
            entries: vec![Entry {
                log_id: next.clone(),
                payload: EntryPayload::Blank,
            }],
            leader_commit: Some(next),
        }
    }

    async fn close(&self) {
        self.client.interconnect.shutdown().await;
        self.follower.shutdown().await;
    }
}

#[nervix_primitives::test]
async fn an_initial_probe_then_data_keeps_empty_commit_updates_on_the_data_stream() {
    let mut peer = probe_peer(wire::AppendEntriesResponseRecord::Success).await;
    let boundary = peer.log_id(11);
    let first = peer.log_id(12);
    let last = peer.log_id(13);
    let input = stream::iter([
        peer.empty_request(Some(boundary.clone())),
        peer.data_request(boundary.clone(), first.clone()),
        peer.empty_request(Some(first.clone())),
        peer.data_request(first.clone(), last.clone()),
    ]);
    let mut answers = peer
        .client
        .stream_append(input, RPCOption::new(Duration::from_secs(5)))
        .await
        .assured("the test peer accepts its initial probe");
    let mut matches = Vec::new();
    while let Some(answer) = answers.next().await {
        nervix_primitives::task::consume_budget().await;
        matches.push(
            answer
                .assured("the test transport answers")
                .assured("the fixture accepts the ordered input"),
        );
    }
    drop(answers);
    peer.close().await;
    assert_eq!(
        matches,
        vec![Some(boundary), Some(first.clone()), Some(first), Some(last)]
    );
    assert_eq!(
        peer.management_calls.load(Ordering::Relaxed),
        1,
        "only the leading probe uses management"
    );
    assert_eq!(
        peer.append_streams.load(Ordering::Relaxed),
        1,
        "data and its commit update share one ordered stream"
    );
}

#[nervix_primitives::test]
async fn rejected_initial_probes_end_before_data_stream_admission() {
    use openraft::raft::{AppendEntriesResponse, StreamAppendError};
    let leader = ClusterNodeName::parse("node-1").assured("the fixture uses a valid node name");
    let boundary = LogIdOf::new(CommittedLeaderIdOf::<TypeConfig>::new(1, leader), 11);
    let higher = Vote::new_committed(
        2,
        ClusterNodeName::parse("node-2").assured("the fixture uses a valid node name"),
    );
    let cases = [
        (
            AppendEntriesResponse::Conflict,
            StreamAppendError::Conflict(boundary),
        ),
        (
            AppendEntriesResponse::HigherVote(higher.clone()),
            StreamAppendError::HigherVote(higher),
        ),
    ];
    for (response, expected) in cases {
        nervix_primitives::task::consume_budget().await;
        let mut peer = probe_peer(response.into()).await;
        let boundary = peer.log_id(11);
        let input = stream::iter([
            peer.empty_request(Some(boundary.clone())),
            peer.data_request(boundary, peer.log_id(12)),
        ]);
        let mut answers = peer
            .client
            .stream_append(input, RPCOption::new(Duration::from_secs(5)))
            .await
            .assured("the test peer answers its initial probe");
        assert_eq!(
            answers
                .next()
                .await
                .assured("the probe has an answer")
                .assured("the transport succeeded"),
            Err(expected)
        );
        assert!(
            answers.next().await.is_none(),
            "the rejected probe ends this input"
        );
        drop(answers);
        peer.close().await;
        assert_eq!(peer.management_calls.load(Ordering::Relaxed), 1);
        assert_eq!(peer.append_streams.load(Ordering::Relaxed), 0);
    }
}

#[nervix_primitives::test]
async fn an_empty_beginning_probe_accepts_a_complete_partial_answer() {
    let mut peer = probe_peer(wire::AppendEntriesResponseRecord::PartialSuccess(None)).await;
    let input = stream::iter([
        peer.empty_request(None),
        peer.empty_request(None),
        openraft::raft::AppendEntriesRequest {
            vote: Vote::new_committed(1, peer.client.local_node_id.clone()),
            prev_log_id: None,
            entries: vec![Entry {
                log_id: peer.log_id(0),
                payload: EntryPayload::Blank,
            }],
            leader_commit: Some(peer.log_id(0)),
        },
    ]);
    let mut answers = peer
        .client
        .stream_append(input, RPCOption::new(Duration::from_secs(5)))
        .await
        .assured("the test peer confirms the beginning boundary");
    let mut matches = Vec::new();
    while let Some(answer) = answers.next().await {
        nervix_primitives::task::consume_budget().await;
        matches.push(
            answer
                .assured("the test transport answers")
                .assured("the fixture accepts its input"),
        );
    }
    drop(answers);
    peer.close().await;
    assert_eq!(matches, vec![None, None, Some(peer.log_id(0))]);
    assert_eq!(peer.management_calls.load(Ordering::Relaxed), 2);
    assert_eq!(peer.append_streams.load(Ordering::Relaxed), 1);
}

#[nervix_primitives::test]
async fn a_failed_following_probe_ends_before_data_stream_admission() {
    let mut peer = probe_peer(wire::AppendEntriesResponseRecord::Success).await;
    let boundary = peer.log_id(11);
    let input = stream::iter([
        peer.empty_request(Some(boundary.clone())),
        peer.empty_request(Some(boundary.clone())),
        peer.data_request(boundary.clone(), peer.log_id(12)),
    ]);
    let mut answers = peer
        .client
        .stream_append(input, RPCOption::new(Duration::from_secs(5)))
        .await
        .assured("the test peer confirms the first boundary");
    assert_eq!(
        answers
            .next()
            .await
            .assured("the first probe answers")
            .assured("the transport succeeds"),
        Ok(Some(boundary))
    );
    peer.follower.shutdown().await;
    assert!(
        answers
            .next()
            .await
            .assured("the failed probe has an outcome")
            .is_err(),
        "the closed peer refuses the following management request"
    );
    assert!(
        answers.next().await.is_none(),
        "the failed probe ends the input"
    );
    drop(answers);
    peer.client.interconnect.shutdown().await;
    assert_eq!(peer.management_calls.load(Ordering::Relaxed), 1);
    assert_eq!(peer.append_streams.load(Ordering::Relaxed), 0);
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
