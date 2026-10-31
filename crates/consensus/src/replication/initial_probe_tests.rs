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
    mpsc,
};
use openraft::{
    Entry, EntryPayload, Vote,
    network::{RPCOption, RaftNetworkV2 as _},
    vote::RaftLeaderId as _,
};
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};

use super::AppendPath;
use crate::{
    AppendStreamOpenRecorder, CommittedLeaderIdOf, ConnectivityFault, LeaseRefusal, LogIdOf,
    NetworkClient, NetworkFactory, SnapshotTransferIds, TypeConfig, wire,
};

#[derive(Clone, Default)]
struct ProbeRecorder {
    attempts: Arc<AtomicUsize>,
    opened: Arc<AtomicUsize>,
}

impl AppendStreamOpenRecorder for ProbeRecorder {
    fn record_append_stream_attempt(&self, _target: &ClusterNodeName) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_append_stream_open(&self, _target: &ClusterNodeName) {
        self.opened.fetch_add(1, Ordering::Relaxed);
    }

    fn connectivity_fault(&self) -> ConnectivityFault {
        ConnectivityFault::default()
    }

    fn lease_refusal(&self) -> LeaseRefusal {
        LeaseRefusal::default()
    }
}

struct ProbePeer {
    client: NetworkClient<ProbeRecorder>,
    follower: Transport,
    management_calls: Arc<AtomicUsize>,
    append_streams: Arc<AtomicUsize>,
    probes: mpsc::Receiver<wire::AppendEntriesRecord>,
    batches: mpsc::Receiver<wire::AppendEntriesRecord>,
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
    let (probe_tx, probes) = mpsc::channel(16);
    follower_transport
        .register_handler::<wire::HeartbeatRequest, _, _>({
            let calls = management_calls.clone();
            let probe_tx = probe_tx.clone();
            move |_context, request| {
                calls.fetch_add(1, Ordering::Relaxed);
                let response = probe_response.clone();
                let probe_tx = probe_tx.clone();
                async move {
                    probe_tx
                        .send(request.0.clone())
                        .await
                        .assured("the fixture retains its probe observer");
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
    let (batch_tx, batches) = mpsc::channel(16);
    follower_transport
        .register_duplex_handler::<wire::OpenAppendStream, _, _>({
            let opened = append_streams.clone();
            let batch_tx = batch_tx.clone();
            move |_context, _opening, items| {
                opened.fetch_add(1, Ordering::Relaxed);
                let batch_tx = batch_tx.clone();
                async move {
                    let answers =
                        stream::unfold((items, batch_tx), |(mut items, batch_tx)| async move {
                            let charged = match items.next().await.assured("test frame decodes") {
                                Some(charged) => charged,
                                None => return None,
                            };
                            batch_tx
                                .send(charged.item.clone())
                                .await
                                .assured("the fixture retains its batch observer");
                            let request =
                                charged.item.into_request().assured("valid append request");
                            let matching = request
                                .entries
                                .last()
                                .map(|entry| entry.log_id.clone())
                                .or(request.prev_log_id);
                            let result: openraft::raft::StreamAppendResult<TypeConfig> =
                                Ok(matching);
                            Some((
                                Ok(Ok(wire::StreamAppendResultRecord::from(result))),
                                (items, batch_tx),
                            ))
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
        append_stream_open_recorder: ProbeRecorder::default(),
        snapshot_transfer_ids: Arc::new(SnapshotTransferIds::new()),
    }
    .client(follower, AppendPath::Replication);
    ProbePeer {
        client,
        follower: follower_transport,
        management_calls,
        append_streams,
        probes,
        batches,
    }
}

#[nervix_primitives::test]
async fn a_first_data_batch_checks_its_unconfirmed_prefix_before_sending_entries() {
    let leader = ClusterNodeName::parse("node-1").assured("valid fixture leader");
    let boundary = LogIdOf::new(CommittedLeaderIdOf::<TypeConfig>::new(1, leader), 11);
    let responses = [
        wire::AppendEntriesResponseRecord::Success,
        wire::AppendEntriesResponseRecord::PartialSuccess(Some(boundary.into())),
    ];
    for response in responses {
        nervix_primitives::task::consume_budget().await;
        // The probe cannot advance the original commit position or cross its checked prefix.
        for (committed, probe_committed) in [(None, None), (Some(7), Some(7)), (Some(13), Some(11))]
        {
            nervix_primitives::task::consume_budget().await;
            let mut peer = probe_peer(response.clone()).await;
            let boundary = peer.log_id(11);
            let mut request = peer.data_request(boundary.clone(), peer.log_id(12));
            request.entries.push(Entry {
                log_id: peer.log_id(13),
                payload: EntryPayload::Blank,
            });
            request.leader_commit = committed.map(|index| peer.log_id(index));
            let expected = wire::AppendEntriesRecord::from_request(request.clone());
            let mut prefix = peer.empty_request(Some(boundary));
            prefix.leader_commit = probe_committed.map(|index| peer.log_id(index));
            let prefix = wire::AppendEntriesRecord::from_request(prefix);
            let mut answers = peer
                .client
                .stream_append(stream::iter([request]), RPCOption::new(Duration::ZERO))
                .await
                .assured("the peer accepts its prefix and data");
            let answer = answers
                .next()
                .await
                .assured("the data has an answer")
                .assured("the transport answers");
            assert!(
                answers.next().await.is_none(),
                "one data request has one answer"
            );
            drop(answers);
            peer.close().await;
            assert_eq!(answer, Ok(Some(peer.log_id(13))));
            assert_eq!(peer.management_calls.load(Ordering::Relaxed), 1);
            assert_eq!(peer.append_streams.load(Ordering::Relaxed), 1);
            assert_eq!(
                peer.probes.try_recv().assured("the prefix was observed"),
                prefix
            );
            assert_eq!(
                peer.batches
                    .try_recv()
                    .assured("the exact data was observed"),
                expected
            );
        }
    }
}

#[nervix_primitives::test]
async fn a_first_data_batch_at_the_log_beginning_transfers_its_complete_request() {
    let mut peer = probe_peer(wire::AppendEntriesResponseRecord::Conflict).await;
    let mut request = peer.empty_request(None);
    request.entries = vec![Entry {
        log_id: peer.log_id(0),
        payload: EntryPayload::Blank,
    }];
    request.leader_commit = Some(peer.log_id(0));
    let expected = wire::AppendEntriesRecord::from_request(request.clone());
    let mut answers = peer
        .client
        .stream_append(stream::iter([request]), RPCOption::new(Duration::ZERO))
        .await
        .assured("the beginning needs no preceding-entry check");
    let answer = answers
        .next()
        .await
        .assured("the data has an answer")
        .assured("the transport answers");
    assert!(
        answers.next().await.is_none(),
        "the single request has one answer"
    );
    drop(answers);
    peer.close().await;
    assert_eq!(answer, Ok(Some(peer.log_id(0))));
    assert_eq!(peer.management_calls.load(Ordering::Relaxed), 0);
    assert_eq!(peer.append_streams.load(Ordering::Relaxed), 1);
    assert_eq!(
        peer.batches
            .try_recv()
            .assured("the exact data was observed"),
        expected
    );
}

#[nervix_primitives::test]
async fn finite_root_data_and_following_input_keep_distinct_stream_lifetimes() {
    let mut peer = probe_peer(wire::AppendEntriesResponseRecord::Success).await;
    let mut root = peer.empty_request(None);
    root.entries = [0, 1]
        .into_iter()
        .map(|index| Entry {
            log_id: peer.log_id(index),
            payload: EntryPayload::Blank,
        })
        .collect();
    root.leader_commit = Some(peer.log_id(1));
    let generations = [
        vec![root],
        vec![
            peer.data_request(peer.log_id(1), peer.log_id(2)),
            peer.data_request(peer.log_id(2), peer.log_id(3)),
            peer.empty_request(Some(peer.log_id(3))),
        ],
    ];
    let prefix = wire::AppendEntriesRecord::from_request(peer.empty_request(Some(peer.log_id(1))));
    for requests in generations {
        nervix_primitives::task::consume_budget().await;
        let expected: Vec<_> = requests
            .iter()
            .cloned()
            .map(wire::AppendEntriesRecord::from_request)
            .collect();
        let expected_answers: Vec<_> = requests
            .iter()
            .map(|request| {
                request
                    .entries
                    .last()
                    .map(|entry| entry.log_id.clone())
                    .or(request.prev_log_id.clone())
            })
            .collect();
        let mut answers = peer
            .client
            .stream_append(stream::iter(requests), RPCOption::new(Duration::ZERO))
            .await
            .assured("the next caller input opens its own data stream");
        for matching in expected_answers {
            nervix_primitives::task::consume_budget().await;
            assert_eq!(
                answers
                    .next()
                    .await
                    .assured("one answer per request")
                    .assured("the transport answers"),
                Ok(matching)
            );
        }
        assert!(
            answers.next().await.is_none(),
            "a completed input ends its stream"
        );
        drop(answers);
        for record in expected {
            assert_eq!(
                peer.batches
                    .try_recv()
                    .assured("every complete request was observed"),
                record
            );
        }
    }
    peer.close().await;
    assert_eq!(
        peer.probes
            .try_recv()
            .assured("the second input verifies its prefix"),
        prefix
    );
    assert_eq!(peer.management_calls.load(Ordering::Relaxed), 1);
    assert_eq!(peer.append_streams.load(Ordering::Relaxed), 2);
    assert_eq!(
        peer.client
            .append_stream_open_recorder
            .attempts
            .load(Ordering::Relaxed),
        2
    );
    assert_eq!(
        peer.client
            .append_stream_open_recorder
            .opened
            .load(Ordering::Relaxed),
        2
    );
}

#[nervix_primitives::test]
async fn a_data_prefix_different_from_the_confirmed_probe_is_checked_before_streaming() {
    let mut peer = probe_peer(wire::AppendEntriesResponseRecord::Success).await;
    let confirmed = peer.log_id(11);
    let data_prefix = peer.log_id(15);
    let data_end = peer.log_id(16);
    let probe = peer.empty_request(Some(confirmed.clone()));
    let expected_confirmed = wire::AppendEntriesRecord::from_request(probe.clone());
    let data = peer.data_request(data_prefix.clone(), data_end.clone());
    let expected_probe =
        wire::AppendEntriesRecord::from_request(peer.empty_request(Some(data_prefix)));
    let expected_data = wire::AppendEntriesRecord::from_request(data.clone());
    let mut answers = peer
        .client
        .stream_append(
            stream::iter([probe, data]),
            RPCOption::new(Duration::from_secs(5)),
        )
        .await
        .assured("the peer accepts the initial probe");
    let mut matches = Vec::new();
    while let Some(answer) = answers.next().await {
        nervix_primitives::task::consume_budget().await;
        matches.push(
            answer
                .assured("the transport answers")
                .assured("the peer accepts its prefix and data"),
        );
    }
    drop(answers);
    peer.close().await;
    assert_eq!(matches, vec![Some(confirmed), Some(data_end)]);
    assert_eq!(peer.management_calls.load(Ordering::Relaxed), 2);
    assert_eq!(peer.append_streams.load(Ordering::Relaxed), 1);
    assert_eq!(
        peer.probes
            .try_recv()
            .assured("the initial probe was observed"),
        expected_confirmed
    );
    assert_eq!(
        peer.probes
            .try_recv()
            .assured("the data prefix was observed"),
        expected_probe
    );
    assert_eq!(
        peer.batches
            .try_recv()
            .assured("the exact data was observed"),
        expected_data
    );
}

#[nervix_primitives::test]
async fn an_unavailable_data_prefix_ends_its_input_before_stream_admission() {
    let mut peer = probe_peer(wire::AppendEntriesResponseRecord::Success).await;
    let boundary = peer.log_id(11);
    let input = stream::iter([
        peer.empty_request(Some(boundary.clone())),
        peer.data_request(peer.log_id(15), peer.log_id(16)),
        peer.data_request(peer.log_id(16), peer.log_id(17)),
    ]);
    let mut answers = peer
        .client
        .stream_append(input, RPCOption::new(Duration::from_secs(5)))
        .await
        .assured("the peer accepts the initial probe");
    let first = answers
        .next()
        .await
        .assured("the initial probe has an answer")
        .assured("the transport answers");
    peer.follower.shutdown().await;
    let failed = answers
        .next()
        .await
        .assured("the unavailable prefix has an outcome");
    assert!(
        answers.next().await.is_none(),
        "the failed prefix ends its input"
    );
    drop(answers);
    peer.close().await;
    assert_eq!(first, Ok(Some(boundary)));
    let Err(openraft::error::RPCError::Unreachable(_)) = failed else {
        panic!("the unavailable prefix must report an unreachable peer: {failed:?}");
    };
    assert_eq!(peer.management_calls.load(Ordering::Relaxed), 1);
    assert_eq!(peer.append_streams.load(Ordering::Relaxed), 0);
    assert_eq!(
        peer.client
            .append_stream_open_recorder
            .attempts
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        peer.client
            .append_stream_open_recorder
            .opened
            .load(Ordering::Relaxed),
        0
    );
}

#[nervix_primitives::test]
async fn a_rejected_data_prefix_returns_its_result_before_transferring_entries() {
    use openraft::raft::{AppendEntriesResponse, StreamAppendError};
    let leader = ClusterNodeName::parse("node-1").assured("valid fixture leader");
    let boundary = LogIdOf::new(CommittedLeaderIdOf::<TypeConfig>::new(1, leader), 11);
    let higher = Vote::new_committed(
        2,
        ClusterNodeName::parse("node-2").assured("valid fixture follower"),
    );
    let cases = [
        (
            AppendEntriesResponse::Conflict,
            Err(StreamAppendError::Conflict(boundary)),
        ),
        (
            AppendEntriesResponse::HigherVote(higher.clone()),
            Err(StreamAppendError::HigherVote(higher)),
        ),
        (AppendEntriesResponse::PartialSuccess(None), Ok(None)),
    ];
    for (response, expected) in cases {
        nervix_primitives::task::consume_budget().await;
        let mut peer = probe_peer(response.into()).await;
        let input = stream::iter([peer.data_request(peer.log_id(11), peer.log_id(12))]);
        let mut answers = peer
            .client
            .stream_append(input, RPCOption::new(Duration::from_secs(5)))
            .await
            .assured("the peer answers the prefix");
        let answer = answers
            .next()
            .await
            .assured("the prefix has a result")
            .assured("the transport answers");
        assert!(
            answers.next().await.is_none(),
            "the refused prefix ends this input"
        );
        drop(answers);
        peer.close().await;
        assert_eq!(answer, expected);
        assert_eq!(peer.management_calls.load(Ordering::Relaxed), 1);
        assert_eq!(peer.append_streams.load(Ordering::Relaxed), 0);
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
        3,
        "finite empty probes and an unconfirmed data prefix use management"
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
