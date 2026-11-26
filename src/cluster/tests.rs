//! Harness checks for cluster topology and application-health observation.
//! Layer: harness.
//! Owns: tests of gossip errors, discovery and exact peer-health lifetimes.
//! Depends on: the cluster owner, interconnect contracts and primitive test adapters.
//! Must not know: domain scheduling or runtime graph execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "cluster topology and health checks")
)]

use super::*;

#[test]
fn gossip_errors_keep_their_typed_interconnect_report() {
    let transport = anyhow::Error::new(GossipExchangeFailure::Transport(Report::new(
        TransportError::ShuttingDown,
    )));
    let GossipExchangeFailure::Transport(report) = transport
        .downcast_ref::<GossipExchangeFailure>()
        .assured("the gossip transport error was wrapped in GossipExchangeFailure")
    else {
        panic!("the transport failure must retain its transport report");
    };
    assert!(matches!(
        report.current_context(),
        TransportError::ShuttingDown
    ));
    assert_eq!(transport.to_string(), "transport is shutting down");

    let node = ClusterNodeName::parse("node-2").assured("the fixture node name is valid");
    let request = anyhow::Error::new(GossipExchangeFailure::Request(Report::new(
        RequestError::TargetLeft {
            node: node.clone(),
            request: GossipExchange::NAME,
        },
    )));
    let GossipExchangeFailure::Request(report) = request
        .downcast_ref::<GossipExchangeFailure>()
        .assured("the gossip request error was wrapped in GossipExchangeFailure")
    else {
        panic!("the request failure must retain its request report");
    };
    assert!(matches!(
        report.current_context(),
        RequestError::TargetLeft { node: target, request }
            if target == &node && *request == GossipExchange::NAME
    ));
    assert!(request.to_string().contains("node-2"));
    assert!(request.to_string().contains(GossipExchange::NAME));
    assert!(request.to_string().contains("left the cluster"));
}

#[nervix_primitives::test]
async fn gossip_reconnects_a_known_route_after_its_outbound_target_is_retired() {
    let node = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
    let interconnect = crate::application::test_fixtures::test_interconnect("test", &node).await;
    let address = interconnect.local_addr();
    let peer = PeerTarget::new(address, address.ip().to_string());
    let seed = GossipRoute {
        peer: GossipRoutePeer::Expected(BootstrapExpectation::AnyMember),
        target: peer,
    };
    let gossip = InterconnectGossipTransport::build(
        interconnect.clone(),
        address,
        BTreeMap::from([(address, seed)]),
        node.clone(),
        crate::ConfiguredFaultInjection::default(),
        MembershipSettings::default().gossip_receive_queue_capacity(),
    )
    .assured("the gossip handler is registered once");

    gossip
        .exchange(address, vec![1])
        .await
        .assured("the initial authenticated exchange establishes the route");
    interconnect.replace_live_nodes(&BTreeSet::new());
    gossip
        .exchange(address, vec![2])
        .await
        .assured("a retained gossip route reconnects after its peer left the live set");

    interconnect
        .shutdown(crate::application::test_fixtures::test_teardown_ceiling())
        .await;
}

/// A node that restarted is live under both its runs until its earlier run is declared dead; the
/// routes follow its newest run alone, so they never move its target back to the earlier one.
#[nervix_primitives::test]
async fn gossip_routes_follow_only_the_newest_run_of_a_restarted_node() {
    let node = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
    let interconnect = crate::application::test_fixtures::test_interconnect("test", &node).await;
    let address = interconnect.local_addr();
    let gossip = InterconnectGossipTransport::build(
        interconnect.clone(),
        address,
        BTreeMap::new(),
        node.clone(),
        crate::ConfiguredFaultInjection::default(),
        MembershipSettings::default().gossip_receive_queue_capacity(),
    )
    .assured("the gossip handler is registered once");
    let local = ChitchatId::new("node-1".to_owned(), 1, address);
    let earlier = ChitchatId::new(
        "node-2".to_owned(),
        1,
        SocketAddr::from(([127, 0, 0, 1], 7001)),
    );
    let newer = ChitchatId::new(
        "node-2".to_owned(),
        2,
        SocketAddr::from(([127, 0, 0, 1], 7002)),
    );
    let other = ChitchatId::new(
        "node-3".to_owned(),
        5,
        SocketAddr::from(([127, 0, 0, 1], 7003)),
    );

    let newest = gossip.newest_runs([&newer, &earlier, &local, &other]);

    let restarted = ClusterNodeName::parse("node-2").assured("the fixture node name is valid");
    let unrestarted = ClusterNodeName::parse("node-3").assured("the fixture node name is valid");
    assert_eq!(newest.len(), 2, "this node is never one of its own routes");
    assert_eq!(newest.get(&restarted), Some(&&newer));
    assert_eq!(newest.get(&unrestarted), Some(&&other));

    interconnect
        .shutdown(crate::application::test_fixtures::test_teardown_ceiling())
        .await;
}

/// Chitchat probes the address a node's earlier run advertised until it forgets that run. Such a
/// probe restores a missing target only, and never moves the node's target from the endpoint the
/// node is reached at now back to the run that is gone.
#[nervix_primitives::test]
async fn a_gossip_probe_of_an_earlier_run_never_moves_a_node_that_runs_elsewhere_now() {
    let node = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
    let interconnect = crate::application::test_fixtures::test_interconnect("test", &node).await;
    let address = interconnect.local_addr();
    let peer = PeerTarget::new(address, address.ip().to_string());
    let probed = peer.endpoint();
    let seed = GossipRoute {
        peer: GossipRoutePeer::Expected(BootstrapExpectation::AnyMember),
        target: peer,
    };
    let gossip = InterconnectGossipTransport::build(
        interconnect.clone(),
        address,
        BTreeMap::from([(address, seed)]),
        node.clone(),
        crate::ConfiguredFaultInjection::default(),
        MembershipSettings::default().gossip_receive_queue_capacity(),
    )
    .assured("the gossip handler is registered once");
    gossip
        .exchange(address, vec![1])
        .await
        .assured("the initial authenticated exchange establishes the route");
    let elsewhere = NodeEndpoint::new("localhost", 9001);
    interconnect.register_outbound_target(node.clone(), elsewhere.clone());

    // The probe restores its route before it waits for a stream to the node, so one poll covers
    // everything it does to the node's target.
    let probe = gossip.exchange(address, vec![2]);
    let mut probe = std::pin::pin!(probe);
    assert!(
        futures_util::poll!(probe.as_mut()).is_pending(),
        "the probe waits for the endpoint the node is reached at, where nothing listens"
    );
    drop(probe);

    assert_eq!(
        interconnect.restore_outbound_target(node.clone(), probed),
        elsewhere,
        "the probe left the node's target where the node runs now"
    );

    interconnect
        .shutdown(crate::application::test_fixtures::test_teardown_ceiling())
        .await;
}

#[nervix_primitives::test]
async fn the_configured_receive_queue_capacity_bounds_incoming_gossip() {
    let node = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
    let interconnect = crate::application::test_fixtures::test_interconnect("test", &node).await;
    let address = interconnect.local_addr();
    let membership = MembershipSettings::resolve(MembershipValues {
        gossip_receive_queue_capacity: NonZeroUsize::new(3),
        ..MembershipValues::default()
    })
    .assured("three queued datagrams are a valid gossip receive queue");
    let gossip = InterconnectGossipTransport::build(
        interconnect.clone(),
        address,
        BTreeMap::new(),
        node,
        crate::ConfiguredFaultInjection::default(),
        membership.gossip_receive_queue_capacity(),
    )
    .assured("the gossip handler is registered once");

    assert_eq!(gossip.inner.incoming_tx.max_capacity(), 3);
    assert_eq!(
        MembershipSettings::default()
            .gossip_receive_queue_capacity()
            .get(),
        1024,
        "a node given no capacity keeps the queue it ran with"
    );

    interconnect
        .shutdown(crate::application::test_fixtures::test_teardown_ceiling())
        .await;
}

#[test]
fn the_membership_event_bus_keeps_the_session_notice_backlog() {
    let default = nervix_models::SessionCapabilities::default().notice_capacity();
    assert_eq!(
        ClusterEvents::capacity_for(default),
        256,
        "the bus keeps the 256 changes it ran with"
    );
    let configured = NonZeroU64::new(3).assured("three is nonzero");
    assert_eq!(ClusterEvents::capacity_for(configured), 3);
}

#[nervix_primitives::test]
async fn the_largest_gossip_exchange_fits_one_management_request_and_a_wider_one_does_not() {
    let node = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
    let interconnect = crate::application::test_fixtures::test_interconnect("test", &node).await;
    let management_event_bytes = interconnect
        .local_prerequisites()
        .consensus_floors
        .largest_management_event_bytes;
    assert_eq!(management_event_bytes, 68 << 10);

    GossipExchange::check_fits(&interconnect, management_event_bytes)
        .assured("Chitchat's largest message and its typed request fields fit the 68 KiB bound");

    let from = GossipExchange::largest().from;
    let exchange_of = |payload_bytes: usize| GossipExchange {
        from,
        payload: vec![0; payload_bytes],
    };
    let refused = interconnect
        .check_request_fits(&exchange_of(68 << 10))
        .err()
        .assured("a payload of the whole bound leaves no room for its typed request fields");
    assert!(matches!(
        refused.current_context(),
        nervix_interconnect::RequestFitError::Payload { request, limit, .. }
            if *request == GossipExchange::NAME && *limit == management_event_bytes
    ));

    // Just below the whole bound the exchange's own archive fits, and only the request envelope
    // around it no longer does: the check counts the envelope too.
    let mut envelope_refusals = 0_u32;
    for payload_bytes in (68 << 10) - 128..68 << 10 {
        let Err(refused) = interconnect.check_request_fits(&exchange_of(payload_bytes)) else {
            continue;
        };
        if let nervix_interconnect::RequestFitError::Body { .. } = refused.current_context() {
            envelope_refusals += 1;
        }
    }
    assert!(
        envelope_refusals > 0,
        "an archive that fits can still need a control body above the bound"
    );

    interconnect
        .shutdown(crate::application::test_fixtures::test_teardown_ceiling())
        .await;
}

#[test]
fn a_gossip_exchange_larger_than_one_message_is_refused() {
    let from = SocketAddr::from(([127, 0, 0, 1], 7946));
    let largest = GossipExchange {
        from,
        payload: vec![0; MAX_GOSSIP_MESSAGE_BYTES],
    };
    assert_eq!(largest.check_size(), Ok(()));

    let oversized = GossipExchange {
        from,
        payload: vec![0; MAX_GOSSIP_MESSAGE_BYTES + 1],
    };
    let refusal = oversized
        .check_size()
        .expect_err("a message past the bound is refused");
    assert_eq!(
        refusal,
        GossipExchangeRefusal::Oversized {
            size: 65_511,
            limit: 65_510,
        }
    );
    assert_eq!(
        refusal.to_string(),
        "a gossip message of 65511 bytes exceeds the 65510-byte bound"
    );
}

/// Chitchat's budget counts a message from its one-byte type on, but not the header Chitchat writes
/// before the type, so the largest message Chitchat builds is that header longer than its budget.
#[test]
fn the_gossip_bound_adds_the_header_chitchat_writes_outside_its_budget() {
    // A BadCluster message carries nothing after its type: it is the header and the type alone.
    let bad_cluster = chitchat::Serializable::serialize_to_vec(&ChitchatMessage::BadCluster);
    assert_eq!(bad_cluster.len(), CHITCHAT_MESSAGE_HEADER_BYTES + 1);
}

/// A Syn of exactly `length` encoded bytes: Chitchat's header, the Syn type, an empty digest and a
/// cluster identifier filling the rest. Chitchat budgets the messages that carry state, never a
/// Syn, so a Syn can stand at whatever length a check of the gossip bound needs.
fn syn_of_length(length: usize) -> ChitchatMessage {
    const SYN_MESSAGE_TAG: u8 = 0;
    // Every Chitchat message opens with the same header, its magic number and protocol version,
    // before its one-byte type. A Syn then carries its digest, empty here, and the cluster
    // identifier as one Chitchat string: a u16 length and its bytes.
    let bad_cluster = chitchat::Serializable::serialize_to_vec(&ChitchatMessage::BadCluster);
    let (_bad_cluster_type, header) = bad_cluster
        .split_last()
        .assured("a BadCluster message encodes its header and its type");
    let mut encoded = header.to_vec();
    encoded.push(SYN_MESSAGE_TAG);
    chitchat::Serializable::serialize(&0_u16, &mut encoded);
    let fixed_fields = encoded
        .len()
        .checked_add(chitchat::Serializable::serialized_len(&String::new()))
        .assured("a Syn's fixed fields take a few bytes");
    let identifier_length = length
        .checked_sub(fixed_fields)
        .assured("the fixture's lengths leave room for a Syn's fixed fields");
    chitchat::Serializable::serialize(&"x".repeat(identifier_length), &mut encoded);
    assert_eq!(encoded.len(), length);
    let mut remaining = encoded.as_slice();
    <ChitchatMessage as chitchat::Deserializable>::deserialize(&mut remaining)
        .assured("the fixture encodes a Syn with an empty digest")
}

/// The gossip transport of the member `node_id`, gossiping at the address of its `interconnect`.
fn gossip_transport_on(
    interconnect: &InterconnectTransport,
    node_id: ClusterNodeName,
) -> InterconnectGossipTransport {
    InterconnectGossipTransport::build(
        interconnect.clone(),
        interconnect.local_addr(),
        BTreeMap::new(),
        node_id,
        crate::ConfiguredFaultInjection::default(),
        MembershipSettings::default().gossip_receive_queue_capacity(),
    )
    .assured("the gossip handler is registered once")
}

/// A message of the whole gossip bound, the size of the largest SynAck or Ack Chitchat builds,
/// crosses the real interconnect to the peer's socket intact as one gossip exchange. Chitchat
/// discards the error of a reply it cannot send, so the sender itself refuses a message one byte
/// larger, logs it and queues nothing for the peer.
#[nervix_primitives::test]
async fn the_whole_gossip_bound_reaches_the_peer_and_one_byte_more_stays_with_the_sender() {
    let authority = crate::application::test_fixtures::TestCertificateAuthority::generate();
    let sender = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
    let receiver = ClusterNodeName::parse("node-2").assured("the fixture node name is valid");
    let sender_interconnect = authority.interconnect("test", &sender).await;
    let receiver_interconnect = authority.interconnect("test", &receiver).await;
    let sender_address = sender_interconnect.local_addr();
    let receiver_address = receiver_interconnect.local_addr();
    let sender_transport = gossip_transport_on(&sender_interconnect, sender);
    let receiver_transport = gossip_transport_on(&receiver_interconnect, receiver);
    let mut sender_socket = GossipTransport::open(&sender_transport, sender_address)
        .await
        .assured("the transport opens its one socket");
    let mut receiver_socket = GossipTransport::open(&receiver_transport, receiver_address)
        .await
        .assured("the transport opens its one socket");

    let refused = sender_socket
        .send(
            receiver_address,
            syn_of_length(MAX_GOSSIP_MESSAGE_BYTES + 1),
        )
        .await
        .expect_err("a message above the gossip bound is refused at the sender");
    assert_eq!(refused.to_string(), "gossip message exceeds 65510 bytes");
    assert!(
        sender_transport.inner.outgoing.is_empty(),
        "nothing was queued for the peer"
    );

    let queued = sender_socket
        .send(receiver_address, syn_of_length(MAX_GOSSIP_MESSAGE_BYTES))
        .await;
    assert!(
        queued.is_ok(),
        "a message of the whole bound is queued for the peer: {queued:?}"
    );
    let (from, received) =
        nervix_primitives::time::timeout(Duration::from_secs(30), receiver_socket.recv())
            .await
            .assured("the peer receives the message of the whole bound within 30s")
            .assured("the peer's socket decodes the message of the whole bound");
    assert_eq!(from, sender_address);
    assert_eq!(received, syn_of_length(MAX_GOSSIP_MESSAGE_BYTES));

    sender_interconnect
        .shutdown(crate::application::test_fixtures::test_teardown_ceiling())
        .await;
    receiver_interconnect
        .shutdown(crate::application::test_fixtures::test_teardown_ceiling())
        .await;
}

/// The settings of the member `node_id` of the cluster `test`, reached on `interconnect`. It joins
/// through `bootstrap` when that names a member.
fn loopback_member_settings(
    node_id: &ClusterNodeName,
    interconnect: &InterconnectTransport,
    bootstrap: Option<SocketAddr>,
) -> ClusterSettings {
    let address = interconnect.local_addr();
    ClusterSettings {
        cluster_id: "test".to_string(),
        node_id: node_id.clone(),
        grpc_listen_addr: address,
        client_advertise_url: crate::application::test_fixtures::test_service_url(address),
        console_advertise_url: crate::application::test_fixtures::test_service_url(address),
        interconnect_advertise_addr: address.into(),
        bootstrap_host: bootstrap.map(|seed| seed.to_string()),
        recovered_members: BTreeMap::new(),
        interconnect: interconnect.clone(),
        membership: MembershipSettings::default(),
        session_notice_capacity: nervix_models::SessionCapabilities::default().notice_capacity(),
        prerequisites: interconnect.local_prerequisites(),
        fault_injection: crate::ConfiguredFaultInjection::default(),
    }
}

/// Waits until `cluster` sees `peer` live, and fails the test once `bound` passes first.
async fn wait_until_live(cluster: &ClusterHandle, peer: &ClusterNodeName, bound: Duration) {
    let seen = nervix_primitives::time::timeout(bound, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            if cluster.live_node(peer).await.is_some() {
                return;
            }
            nervix_primitives::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(seen.is_ok(), "'{peer}' was not seen live within {bound:?}");
}

/// Chitchat fills every message it gossips up to its own message budget and splits larger state
/// over later rounds, and the gossip transport must carry whatever it builds. A member whose state
/// a peer lacks by more than one exchange carries must still reach that peer whole, in as many
/// exchanges as it takes. Subscription interests are the state that grows with the work a node
/// serves. Chitchat compresses a delta in blocks but counts the block it is still filling at its
/// uncompressed size, so a full message ends up below its budget by what its last block compressed
/// away. Long random relay names compress too little for that to keep a full message within a
/// transport bound much smaller than the budget.
#[nervix_primitives::test]
async fn a_peer_learns_gossip_state_larger_than_one_exchange_carries() {
    const DOMAIN: &str = "gossip";
    const INTERESTS: usize = 400;
    const RELAY_NAME_CHARACTERS: usize = 500;
    const MEMBERSHIP_BOUND: Duration = Duration::from_secs(30);
    const CONVERGENCE_BOUND: Duration = Duration::from_secs(30);

    let first = ClusterNodeName::parse("node-1").assured("the fixture node name is valid");
    let second = ClusterNodeName::parse("node-2").assured("the fixture node name is valid");
    let authority = crate::application::test_fixtures::TestCertificateAuthority::generate();
    let first_interconnect = authority.interconnect("test", &first).await;
    let second_interconnect = authority.interconnect("test", &second).await;
    let first_cluster = start_cluster(loopback_member_settings(&first, &first_interconnect, None))
        .await
        .assured("the first member starts on its loopback interconnect");
    let second_cluster = start_cluster(loopback_member_settings(
        &second,
        &second_interconnect,
        Some(first_interconnect.local_addr()),
    ))
    .await
    .assured("the second member starts with the first as its bootstrap host");
    wait_until_live(&second_cluster, &first, MEMBERSHIP_BOUND).await;
    wait_until_live(&first_cluster, &second, MEMBERSHIP_BOUND).await;

    let mut random = fastrand::Rng::with_seed(61_440);
    let mut relays = Vec::with_capacity(INTERESTS);
    for _ in 0..INTERESTS {
        let mut relay = String::with_capacity(RELAY_NAME_CHARACTERS);
        for _ in 0..RELAY_NAME_CHARACTERS {
            relay.push(random.alphanumeric());
        }
        relays.push(relay);
    }
    for relay in &relays {
        first_cluster
            .reconcile_local_subscription_interest(DOMAIN, relay, || true)
            .await;
    }

    // Chitchat sends a member's keys in the order of their versions, so the last interest
    // arriving means every earlier one arrived too.
    let first_identity = first_cluster.local_node_identity().await;
    let last_relay = relays
        .last()
        .assured("the fixture advertises at least one interest");
    let last_version = first_cluster
        .local_subscription_interest_version(DOMAIN, last_relay)
        .await
        .assured("the first member advertises the interest it reconciled above");
    let learned_last = nervix_primitives::time::timeout(
        CONVERGENCE_BOUND,
        second_cluster.wait_for_subscription_interest(
            &first_identity,
            DOMAIN,
            last_relay,
            last_version,
        ),
    )
    .await;
    let index = second_cluster.subscription_interest_index();
    let mut missing = Vec::new();
    for relay in &relays {
        if !index.contains(&first_identity, DOMAIN, relay, 1) {
            missing.push(relay.as_str());
        }
    }
    assert!(
        learned_last.is_ok() && missing.is_empty(),
        "node-2 still lacks {} of the {} subscription interests node-1 advertised, \
         {CONVERGENCE_BOUND:?} after node-1 advertised them",
        missing.len(),
        relays.len(),
    );

    second_cluster
        .shutdown()
        .await
        .assured("the second member's gossip stops");
    first_cluster
        .shutdown()
        .await
        .assured("the first member's gossip stops");
    second_interconnect
        .shutdown(crate::application::test_fixtures::test_teardown_ceiling())
        .await;
    first_interconnect
        .shutdown(crate::application::test_fixtures::test_teardown_ceiling())
        .await;
}

fn health_identity(node: &str, incarnation: u64) -> ClusterNodeIdentity {
    ClusterNodeIdentity::new(
        ClusterNodeName::parse(node).assured("the test node name is valid"),
        ClusterNodeIncarnation::new(incarnation),
    )
}

fn health_endpoint(node: &str, incarnation: u64, address: &str) -> PeerHealthEndpoint {
    PeerHealthEndpoint::new(
        health_identity(node, incarnation),
        address
            .parse::<NodeEndpoint>()
            .expect("the test endpoint is a host and port"),
    )
}

fn subscription_state(
    node: &str,
    incarnation: u64,
    port: u16,
    interests: &[(&str, &str)],
) -> (ChitchatId, NodeState) {
    let id = ChitchatId {
        node_id: node.to_string(),
        generation_id: incarnation,
        gossip_advertise_addr: SocketAddr::from(([127, 0, 0, 1], port)),
    };
    let mut state = NodeState::for_test();
    for (domain, relay) in interests {
        state.set(subscription_interest_key(domain, relay), "1");
    }
    (id, state)
}

fn gossip_id(node: &str, incarnation: u64) -> ChitchatId {
    ChitchatId {
        node_id: node.to_string(),
        generation_id: incarnation,
        gossip_advertise_addr: SocketAddr::from(([127, 0, 0, 1], 47392)),
    }
}

#[test]
fn established_health_target_survives_gossip_loss_until_continuous_failure() {
    let timeout = Duration::from_secs(10);
    let started_at = Instant::now();
    let almost_unavailable = started_at
        .checked_add(Duration::from_secs(9))
        .assured("the test observation time fits in the monotonic clock range");
    let unavailable_at = started_at
        .checked_add(timeout)
        .assured("the test observation time fits in the monotonic clock range");
    let endpoint = "node-2.example:7001";
    let mut health = PeerHealthStateSnapshot::default();
    let target = health
        .replace_endpoints(
            [health_endpoint("node-2", 7, endpoint)],
            &BTreeSet::new(),
            started_at,
            timeout,
        )
        .into_iter()
        .next()
        .assured("one endpoint produces one health target");
    health.record_result(
        PeerHealthProbeResult::new(target.clone(), PeerHealthProbeOutcome::Failure, started_at),
        timeout,
    );
    health.record_result(
        PeerHealthProbeResult::new(target, PeerHealthProbeOutcome::Failure, almost_unavailable),
        timeout,
    );

    let mut gossip_state = NodeState::for_test();
    gossip_state.set(KEY_INTERCONNECT_ADVERTISE_ADDR, endpoint);
    let node = to_gossip_node(&gossip_id("node-2", 7), &gossip_state)
        .assured("the test gossip identity is valid");
    let node_id = node.node_id.clone();
    let known_nodes = BTreeMap::from([(node_id.clone(), node.clone())]);
    let gossip_miss = || GossipState {
        live_nodes: Vec::new(),
        dead_node_ids: BTreeSet::from([node_id.clone()]),
        dead_node_identities: BTreeSet::from([node.identity()]),
    };

    let mut available = gossip_miss();
    health
        .effective_snapshot(almost_unavailable, timeout)
        .retain_known_nodes(&mut available, &known_nodes);
    assert_eq!(available.live_node_ids(), BTreeSet::from([node_id.clone()]));
    assert!(available.dead_node_ids.is_empty());

    let mut failed = gossip_miss();
    health
        .effective_snapshot(unavailable_at, timeout)
        .retain_known_nodes(&mut failed, &known_nodes);
    assert!(failed.live_node_ids().is_empty());
    assert_eq!(failed.dead_node_ids, BTreeSet::from([node_id.clone()]));

    let replacement = to_gossip_node(&gossip_id("node-2", 8), &gossip_state)
        .assured("the test replacement gossip identity is valid");
    assert!(
        !health
            .effective_snapshot(almost_unavailable, timeout)
            .retains_known_node(&replacement)
    );
    let mut changed_endpoint_state = NodeState::for_test();
    changed_endpoint_state.set(KEY_INTERCONNECT_ADVERTISE_ADDR, "node-2.example:7002");
    let changed_endpoint = to_gossip_node(&gossip_id("node-2", 7), &changed_endpoint_state)
        .assured("the test changed gossip endpoint is valid");
    assert!(
        !health
            .effective_snapshot(almost_unavailable, timeout)
            .retains_known_node(&changed_endpoint)
    );

    let unavailable = health.effective_snapshot(unavailable_at, timeout);
    let mut still_live = health.clone();
    still_live.replace_endpoints(
        [],
        &BTreeSet::from([node_id.clone()]),
        unavailable_at,
        timeout,
    );
    assert!(still_live.scheduling_revision() > unavailable.scheduling_revision());
    health.replace_endpoints([], &BTreeSet::new(), unavailable_at, timeout);
    let retired = health.effective_snapshot(unavailable_at, timeout);
    let mut after_retirement = gossip_miss();
    retired.retain_known_nodes(&mut after_retirement, &known_nodes);
    assert_eq!(after_retirement, failed);
    assert!(retired.revision() > unavailable.revision());
    assert_eq!(
        retired.scheduling_revision(),
        unavailable.scheduling_revision()
    );
}

#[test]
fn a_gossip_lapse_is_bridged_only_while_health_was_observed_within_the_interval() {
    let timeout = Duration::from_secs(10);
    let healthy_at = Instant::now();
    let capacity_at = healthy_at
        .checked_add(Duration::from_secs(9))
        .assured("the test observation time fits in the monotonic clock range");
    let healthy_stale_at = healthy_at
        .checked_add(timeout)
        .assured("the test observation time fits in the monotonic clock range");
    let capacity_stale_at = capacity_at
        .checked_add(timeout)
        .assured("the test observation time fits in the monotonic clock range");
    let endpoint = "node-2.example:7001";
    let mut health = PeerHealthStateSnapshot::default();
    let target = health
        .replace_endpoints(
            [health_endpoint("node-2", 7, endpoint)],
            &BTreeSet::new(),
            healthy_at,
            timeout,
        )
        .into_iter()
        .next()
        .assured("one endpoint produces one health target");
    health.record_result(
        PeerHealthProbeResult::new(
            target.clone(),
            PeerHealthProbeOutcome::Healthy(health_identity("node-2", 7)),
            healthy_at,
        ),
        timeout,
    );

    let mut gossip_state = NodeState::for_test();
    gossip_state.set(KEY_INTERCONNECT_ADVERTISE_ADDR, endpoint);
    let node = to_gossip_node(&gossip_id("node-2", 7), &gossip_state)
        .assured("the test gossip identity is valid");
    let node_id = node.node_id.clone();
    let known_nodes = BTreeMap::from([(node_id.clone(), node.clone())]);
    let gossip_miss = || GossipState {
        live_nodes: Vec::new(),
        dead_node_ids: BTreeSet::from([node_id.clone()]),
        dead_node_identities: BTreeSet::from([node.identity()]),
    };

    let mut recently_healthy = gossip_miss();
    health
        .effective_snapshot(capacity_at, timeout)
        .retain_known_nodes(&mut recently_healthy, &known_nodes);
    assert_eq!(
        recently_healthy.live_node_ids(),
        BTreeSet::from([node_id.clone()])
    );

    // No probe completed after the healthy one, as when no probe to a stopped peer finishes:
    // the observation ages out and gossip's verdict applies.
    let mut stale = gossip_miss();
    health
        .effective_snapshot(healthy_stale_at, timeout)
        .retain_known_nodes(&mut stale, &known_nodes);
    assert!(stale.live_node_ids().is_empty());
    assert_eq!(stale.dead_node_ids, BTreeSet::from([node_id.clone()]));

    health.record_result(
        PeerHealthProbeResult::new(
            target,
            PeerHealthProbeOutcome::CapacityExhausted,
            capacity_at,
        ),
        timeout,
    );
    let mut inconclusive = gossip_miss();
    health
        .effective_snapshot(healthy_stale_at, timeout)
        .retain_known_nodes(&mut inconclusive, &known_nodes);
    assert_eq!(
        inconclusive.live_node_ids(),
        BTreeSet::from([node_id.clone()])
    );

    let mut inconclusive_stale = gossip_miss();
    health
        .effective_snapshot(capacity_stale_at, timeout)
        .retain_known_nodes(&mut inconclusive_stale, &known_nodes);
    assert!(inconclusive_stale.live_node_ids().is_empty());
    assert_eq!(inconclusive_stale.dead_node_ids, BTreeSet::from([node_id]));
}

#[test]
fn gossip_endpoints_arrive_field_by_field() {
    let id = gossip_id("node-2", 7);
    let mut state = NodeState::for_test();

    let discovered = to_gossip_node(&id, &state).expect("a named node is discovered");
    assert_eq!(discovered.client_url, None);
    assert_eq!(discovered.console_url, None);
    assert_eq!(discovered.interconnect_endpoint, None);

    state.set(KEY_INTERCONNECT_ADVERTISE_ADDR, "node-2.test:47392");
    let discovered = to_gossip_node(&id, &state).expect("a named node is discovered");
    assert_eq!(
        discovered.interconnect_endpoint,
        Some(NodeEndpoint::new("node-2.test", 47392))
    );
    assert_eq!(discovered.client_url, None);
    assert_eq!(discovered.console_url, None);

    state.set(KEY_GRPC_ADVERTISE_ADDR, "http://node-2.test:47391");
    state.set(KEY_WEB_CONSOLE_ADVERTISE_ADDR, "https://node-2.test:8443");
    let discovered = to_gossip_node(&id, &state).expect("a named node is discovered");
    assert_eq!(
        discovered.client_url,
        Some(
            "http://node-2.test:47391"
                .parse::<NodeServiceUrl>()
                .expect("the test url has a scheme and a host")
        )
    );
    assert_eq!(
        discovered.console_url,
        Some(
            "https://node-2.test:8443"
                .parse::<NodeServiceUrl>()
                .expect("the test url has a scheme and a host")
        )
    );
}

#[test]
fn malformed_gossip_endpoints_stay_unavailable() {
    let id = gossip_id("node-2", 7);
    let mut state = NodeState::for_test();
    state.set(KEY_INTERCONNECT_ADVERTISE_ADDR, "node-2.test");
    state.set(KEY_GRPC_ADVERTISE_ADDR, "node-2.test:47391");
    state.set(KEY_WEB_CONSOLE_ADVERTISE_ADDR, "");

    let discovered = to_gossip_node(&id, &state).expect("a named node is discovered");
    assert_eq!(discovered.interconnect_endpoint, None);
    assert_eq!(discovered.client_url, None);
    assert_eq!(discovered.console_url, None);
}

#[test]
fn a_node_whose_name_this_build_rejects_is_not_discovered() {
    let id = gossip_id("Node 2", 7);
    let mut state = NodeState::for_test();
    state.set(KEY_INTERCONNECT_ADVERTISE_ADDR, "node-2.test:47392");

    assert!(to_gossip_node(&id, &state).is_none());
}

#[test]
fn derive_peer_addr_increments_port() {
    let grpc_addr: SocketAddr = "127.0.0.1:47391".parse().expect("valid socket addr");
    let gossip_addr = derive_peer_addr(grpc_addr).expect("must derive");
    let expected: SocketAddr = "127.0.0.1:47392".parse().unwrap();
    assert_eq!(gossip_addr, expected);
}

#[test]
fn derive_peer_addr_returns_none_on_port_overflow() {
    let grpc_addr: SocketAddr = "127.0.0.1:65535".parse().expect("valid socket addr");
    assert!(derive_peer_addr(grpc_addr).is_none());
}

#[test]
fn subscription_interest_index_tracks_live_advertisements_and_withdrawals() {
    let (node_one_id, node_one_state) =
        subscription_state("node-1", 7, 7101, &[("sales", "events")]);
    let (node_two_id, mut node_two_state) = subscription_state(
        "node-2",
        9,
        7102,
        &[("sales", "events"), ("sales", "audits")],
    );
    node_two_state.set("subscription_interest:incomplete", "1");
    let mut live_nodes = BTreeMap::from([
        (node_one_id.clone(), node_one_state),
        (node_two_id.clone(), node_two_state),
    ]);

    let index = subscription_interests_from_live_nodes(&live_nodes);
    let expected = BTreeMap::from([
        (
            ClusterNodeName::parse("node-1").assured("the test node name is valid"),
            AdvertisedSubscriptionInterest {
                incarnation: ClusterNodeIncarnation::new(7),
                version: 1,
            },
        ),
        (
            ClusterNodeName::parse("node-2").assured("the test node name is valid"),
            AdvertisedSubscriptionInterest {
                incarnation: ClusterNodeIncarnation::new(9),
                version: 1,
            },
        ),
    ]);
    assert_eq!(
        index
            .nodes("sales", "events")
            .assured("both test nodes advertise sales events"),
        &expected
    );
    assert!(index.contains(&health_identity("node-2", 9), "sales", "events", 1));
    assert!(index.nodes("incomplete", "").is_none());

    live_nodes
        .get_mut(&node_two_id)
        .assured("the second test node is live")
        .delete(&subscription_interest_key("sales", "events"));
    let withdrawn = subscription_interests_from_live_nodes(&live_nodes);
    let remaining = withdrawn
        .nodes("sales", "events")
        .assured("the first test node remains interested");
    assert_eq!(remaining.len(), 1);
    assert!(
        remaining
            .contains_key(&ClusterNodeName::parse("node-1").assured("the test node name is valid"))
    );
    assert!(!withdrawn.contains(&health_identity("node-2", 9), "sales", "events", 1));
}

#[nervix_primitives::test]
async fn subscription_interest_visibility_requires_the_current_advertisement() {
    let (node_id, mut state) = subscription_state("node-1", 7, 7101, &[("sales", "events")]);
    let subscriber = health_identity("node-1", 7);
    let key = subscription_interest_key("sales", "events");
    let previous = BTreeMap::from([(node_id.clone(), state.clone())]);
    let publication = SubscriptionInterestPublication::new();
    publication.publish(&previous);

    state.delete(&key);
    let withdrawn = BTreeMap::from([(node_id.clone(), state.clone())]);
    state.set(&key, "1");
    let minimum_version = state
        .get_versioned(&key)
        .assured("the interest was just advertised again")
        .version;
    let mut visible =
        std::pin::pin!(publication.wait_for(&subscriber, "sales", "events", minimum_version,));

    assert!(
        !publication
            .load()
            .contains(&subscriber, "sales", "events", minimum_version),
        "reopening must not accept the advertisement that preceded withdrawal"
    );
    assert!(futures_util::poll!(&mut visible).is_pending());
    publication.publish(&withdrawn);
    assert!(
        !publication
            .load()
            .contains(&subscriber, "sales", "events", minimum_version)
    );
    assert!(futures_util::poll!(&mut visible).is_pending());
    publication.publish(&BTreeMap::from([(node_id, state)]));
    assert!(
        publication
            .load()
            .contains(&subscriber, "sales", "events", minimum_version)
    );
    nervix_primitives::time::timeout(Duration::from_secs(30), visible)
        .await
        .assured("publishing the renewed interest releases its visibility wait");
    assert!(!publication.load().contains(
        &health_identity("node-1", 8),
        "sales",
        "events",
        minimum_version,
    ));
}

#[test]
fn membership_publication_replaces_the_process_identity_for_remote_ack_watchers() {
    let publication = SubscriptionInterestPublication::new();
    let (node, state) = subscription_state("node-1", 7, 7101, &[]);
    let name = ClusterNodeName::parse("node-1").assured("the fixture node is valid");
    publication.publish(&BTreeMap::from([(node, state)]));
    assert_eq!(
        publication.live_runs.load().get(&name),
        Some(&ClusterNodeIncarnation::new(7))
    );
    let (node, state) = subscription_state("node-1", 8, 7101, &[]);
    publication.publish(&BTreeMap::from([(node, state)]));
    assert_eq!(
        publication.live_runs.load().get(&name),
        Some(&ClusterNodeIncarnation::new(8))
    );
    publication.publish(&BTreeMap::new());
    assert!(publication.live_runs.load().is_empty());
}

#[test]
fn subscription_interest_snapshot_load_and_lookup_do_not_allocate() {
    let (node_id, node_state) = subscription_state("node-1", 7, 7101, &[("sales", "events")]);
    let live_nodes = BTreeMap::from([(node_id, node_state)]);
    let publication = SubscriptionInterestPublication::new();
    publication.publish(&live_nodes);

    let (allocations, node_count) = alloc_count::alloc_count!({
        let index = publication.load();
        std::hint::black_box(
            index
                .nodes("sales", "events")
                .assured("the test node advertises sales events")
                .len(),
        )
    });

    assert_eq!(node_count, 1);
    assert_eq!(
        (allocations.alloc_calls, allocations.realloc_calls),
        (0, 0),
        "loading and querying the published subscription-interest index must not allocate"
    );
}

#[test]
fn peer_health_ignores_results_superseded_by_an_endpoint_change() {
    let timeout = Duration::from_secs(10);
    let started_at = Instant::now();
    let mut state = PeerHealthStateSnapshot::default();
    let superseded_target = state
        .replace_endpoints(
            vec![health_endpoint("node-2", 7, "node-2.example:7001")],
            &BTreeSet::new(),
            started_at,
            timeout,
        )
        .into_iter()
        .next()
        .assured("one endpoint produces one probe target");
    let current_target = state
        .replace_endpoints(
            vec![health_endpoint("node-2", 8, "node-2.example:7002")],
            &BTreeSet::new(),
            started_at,
            timeout,
        )
        .into_iter()
        .next()
        .assured("the replacement endpoint produces one probe target");
    assert!(current_target.endpoint_generation() > superseded_target.endpoint_generation());
    let revision_after_replacement = state.revision();

    assert_eq!(
        state.record_result(
            PeerHealthProbeResult::new(
                superseded_target,
                PeerHealthProbeOutcome::Healthy(health_identity("node-2", 7)),
                started_at,
            ),
            timeout,
        ),
        PeerHealthResultDisposition::Superseded
    );
    assert_eq!(state.revision(), revision_after_replacement);
    assert_eq!(
        state
            .effective_snapshot(started_at, timeout)
            .status(&ClusterNodeName::parse("node-2").assured("the test node name is valid")),
        Some(PeerHealthStatus::Unobserved)
    );

    assert!(matches!(
        state.record_result(
            PeerHealthProbeResult::new(
                current_target,
                PeerHealthProbeOutcome::Healthy(health_identity("node-2", 8)),
                started_at,
            ),
            timeout,
        ),
        PeerHealthResultDisposition::Published { .. }
    ));
    let healthy = state.effective_snapshot(started_at, timeout);
    let node = ClusterNodeName::parse("node-2").assured("the test node name is valid");
    assert_eq!(healthy.status(&node), Some(PeerHealthStatus::Healthy));
    assert_eq!(healthy.observed_at(&node), Some(started_at));
}

#[test]
fn repeated_equivalent_observations_do_not_invalidate_scheduling() {
    let freshness = Duration::from_secs(10);
    let started_at = Instant::now();
    let repeated_at = started_at
        .checked_add(Duration::from_secs(1))
        .assured("the test observation interval fits in the monotonic clock range");
    let mut state = PeerHealthStateSnapshot::default();
    let target = state
        .replace_endpoints(
            [health_endpoint("node-2", 7, "node-2.example:7001")],
            &BTreeSet::new(),
            started_at,
            freshness,
        )
        .into_iter()
        .next()
        .assured("one endpoint produces one probe target");
    state.record_result(
        PeerHealthProbeResult::new(
            target.clone(),
            PeerHealthProbeOutcome::Healthy(health_identity("node-2", 7)),
            started_at,
        ),
        freshness,
    );
    let first = state.effective_snapshot(started_at, freshness);
    state.record_result(
        PeerHealthProbeResult::new(
            target,
            PeerHealthProbeOutcome::Healthy(health_identity("node-2", 7)),
            repeated_at,
        ),
        freshness,
    );
    let repeated = state.effective_snapshot(repeated_at, freshness);

    assert!(repeated.revision() > first.revision());
    assert_eq!(
        repeated.scheduling_revision(),
        first.scheduling_revision(),
        "a fresher observation with the same effective availability cannot stale a schedule"
    );
}

#[test]
fn only_fresh_continuous_failures_become_unavailable() {
    let timeout = Duration::from_secs(10);
    let started_at = Instant::now();
    let second_observation_at = started_at
        .checked_add(Duration::from_secs(9))
        .assured("the test observation time fits in the monotonic clock range");
    let unavailable_at = started_at
        .checked_add(timeout)
        .assured("the test unavailability time fits in the monotonic clock range");
    let stale_at = second_observation_at
        .checked_add(timeout)
        .assured("the test stale time fits in the monotonic clock range");
    let mut state = PeerHealthStateSnapshot::default();
    let target = state
        .replace_endpoints(
            vec![health_endpoint("node-2", 7, "node-2.example:7001")],
            &BTreeSet::new(),
            started_at,
            timeout,
        )
        .into_iter()
        .next()
        .assured("one endpoint produces one probe target");
    state.record_result(
        PeerHealthProbeResult::new(target.clone(), PeerHealthProbeOutcome::Failure, started_at),
        timeout,
    );
    state.record_result(
        PeerHealthProbeResult::new(
            target,
            PeerHealthProbeOutcome::Failure,
            second_observation_at,
        ),
        timeout,
    );

    let failed = state.effective_snapshot(second_observation_at, timeout);
    assert_eq!(
        failed.status(&ClusterNodeName::parse("node-2").assured("the test node name is valid")),
        Some(PeerHealthStatus::Failure)
    );
    let revision_before_deadline = failed.revision();
    let unavailable = state.effective_snapshot(unavailable_at, timeout);
    assert_eq!(
        unavailable
            .status(&ClusterNodeName::parse("node-2").assured("the test node name is valid")),
        Some(PeerHealthStatus::Unavailable)
    );
    assert!(unavailable.revision() > revision_before_deadline);
    assert!(
        unavailable
            .unavailable_nodes()
            .contains(&ClusterNodeName::parse("node-2").assured("the test node name is valid"))
    );

    let stale = state.effective_snapshot(stale_at, timeout);
    assert_eq!(
        stale.status(&ClusterNodeName::parse("node-2").assured("the test node name is valid")),
        Some(PeerHealthStatus::Stale)
    );
    assert!(stale.unavailable_nodes().is_empty());
}

#[test]
fn capacity_results_break_a_failure_run_without_marking_the_peer_dead() {
    let timeout = Duration::from_secs(10);
    let started_at = Instant::now();
    let mut state = PeerHealthStateSnapshot::default();
    let target = state
        .replace_endpoints(
            vec![health_endpoint("node-2", 7, "node-2.example:7001")],
            &BTreeSet::new(),
            started_at,
            timeout,
        )
        .into_iter()
        .next()
        .assured("one endpoint produces one probe target");
    state.record_result(
        PeerHealthProbeResult::new(target.clone(), PeerHealthProbeOutcome::Failure, started_at),
        timeout,
    );
    let capacity_at = started_at
        .checked_add(Duration::from_secs(9))
        .assured("the test capacity observation fits in the monotonic clock range");
    state.record_result(
        PeerHealthProbeResult::new(
            target.clone(),
            PeerHealthProbeOutcome::CapacityExhausted,
            capacity_at,
        ),
        timeout,
    );
    let original_failure_deadline = started_at
        .checked_add(timeout)
        .assured("the test original failure deadline fits in the monotonic clock range");
    let capacity = state.effective_snapshot(original_failure_deadline, timeout);
    assert_eq!(
        capacity.status(&ClusterNodeName::parse("node-2").assured("the test node name is valid")),
        Some(PeerHealthStatus::Inconclusive)
    );
    assert_eq!(
        capacity.latest_outcome(
            &ClusterNodeName::parse("node-2").assured("the test node name is valid")
        ),
        Some(PeerHealthObservationKind::CapacityExhausted)
    );
    assert!(capacity.unavailable_nodes().is_empty());
}

/// Chitchat keeps one receiver of its live set and never marks what it published as seen, and
/// hands out clones of it. A subscription taken after the set was published must still wait for
/// the next change rather than report the publications before it.
#[nervix_primitives::test]
async fn a_live_set_subscription_waits_for_a_change_made_after_it() {
    let (live_set, chitchat_receiver) =
        nervix_primitives::unmodeled::sync::watch::channel(BTreeMap::new());
    live_set.send_replace(BTreeMap::new());
    let (_peer_health_state, peer_health_state_receiver) =
        watch::channel(PeerHealthStateSnapshot::default());
    let observation_freshness = ArcSwap::from_pointee(Duration::from_secs(10));
    let mut watcher = ClusterStateWatcher {
        live_node_states: ClusterHandle::live_node_changes_after(chitchat_receiver.clone()),
        peer_health_state: PeerHealthStateWatcher::new(
            peer_health_state_receiver,
            &observation_freshness,
        ),
    };
    let mut waiting = Box::pin(watcher.wait_for_change_or_next_unavailability());

    nervix_primitives::select! {
        biased;
        _ = &mut waiting => {
            panic!("a fresh subscription to the live set reported a change made before it")
        }
        () = nervix_primitives::task::yield_now() => {}
    }
}

/// A caller subscribes, reads the cluster state, and only then prepares its wait. A health
/// change published between its read and its wait must still wake that wait.
#[nervix_primitives::test]
async fn cluster_state_watcher_observes_a_health_target_change_before_wait_preparation() {
    let observation_freshness = Duration::from_secs(10);
    let (_live_state, live_state_receiver) =
        nervix_primitives::unmodeled::sync::watch::channel(BTreeMap::new());
    let (peer_health_state, peer_health_state_receiver) =
        watch::channel(PeerHealthStateSnapshot::default());
    let freshness_policy = ArcSwap::from_pointee(observation_freshness);
    let mut watcher = ClusterStateWatcher {
        live_node_states: live_state_receiver,
        peer_health_state: PeerHealthStateWatcher::new(
            peer_health_state_receiver,
            &freshness_policy,
        ),
    };
    peer_health_state.send_modify(|snapshot| {
        snapshot.replace_endpoints(
            [health_endpoint("node-2", 7, "node-2.example:7001")],
            &BTreeSet::new(),
            Instant::now(),
            observation_freshness,
        );
    });
    let mut waiting = Box::pin(watcher.wait_for_change_or_next_unavailability());

    nervix_primitives::select! {
        biased;
        _ = &mut waiting => {}
        () = nervix_primitives::task::yield_now() => {
            panic!("a health target update before wait preparation must wake the waiter")
        }
    }
}

#[nervix_primitives::test]
async fn cluster_state_watcher_observes_a_health_target_change_after_wait_preparation() {
    let observation_freshness = Duration::from_secs(10);
    let (_live_state, live_state_receiver) =
        nervix_primitives::unmodeled::sync::watch::channel(BTreeMap::new());
    let (peer_health_state, peer_health_state_receiver) =
        watch::channel(PeerHealthStateSnapshot::default());
    let freshness_policy = ArcSwap::from_pointee(observation_freshness);
    let mut watcher = ClusterStateWatcher {
        live_node_states: live_state_receiver,
        peer_health_state: PeerHealthStateWatcher::new(
            peer_health_state_receiver,
            &freshness_policy,
        ),
    };
    let mut waiting = Box::pin(watcher.wait_for_change_or_next_unavailability());
    peer_health_state.send_modify(|snapshot| {
        snapshot.replace_endpoints(
            [health_endpoint("node-2", 7, "node-2.example:7001")],
            &BTreeSet::new(),
            Instant::now(),
            observation_freshness,
        );
    });

    nervix_primitives::select! {
        biased;
        _ = &mut waiting => {}
        () = nervix_primitives::task::yield_now() => {
            panic!("a health target update after wait preparation must wake the waiter")
        }
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn a_terminating_advertisement_is_held_only_until_its_propagation_has_passed() {
    let propagation = Duration::from_secs(1);
    let advertisement = TerminatingAdvertisement {
        advertised_at: Instant::now(),
    };
    assert_eq!(
        advertisement.propagation_remaining(propagation),
        Some(propagation)
    );

    nervix_primitives::time::advance(Duration::from_millis(400)).await;
    assert_eq!(
        advertisement.propagation_remaining(propagation),
        Some(Duration::from_millis(600))
    );

    nervix_primitives::time::advance(Duration::from_millis(600)).await;
    assert_eq!(advertisement.propagation_remaining(propagation), None);
}
