//! How an MQTT client reaches its broker, observed against a local DNS authority and TCP listeners
//! that stand in for the broker's addresses. The real broker is the Cucumber suite's.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_primitives::net::{TcpListener, TcpSocket};
use nervix_test_environment::dns_authority::DnsAnswer;
use rumqttc::{AsyncClient, Event, Incoming};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;
use crate::test_fixtures::DnsFixture;

/// The fixture name every check resolves.
const BROKER: &str = "mqtt.nervix.test";
/// A budget no check expects to exhaust.
const GENEROUS_BUDGET: Duration = Duration::from_secs(30);
/// An MQTT 5 CONNACK accepting a clean session, with no properties.
const CONNACK: [u8; 5] = [0x20, 0x03, 0x00, 0x00, 0x00];

fn loopback(last: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 8, last))
}

fn dialer(fixture: &DnsFixture, budget: Duration) -> MqttDialer {
    MqttDialer {
        dns: fixture.dns.clone(),
        budget,
    }
}

async fn loopback_listener(address: IpAddr) -> TcpListener {
    TcpListener::bind(SocketAddr::new(address, 0))
        .await
        .assured("a loopback port is available")
}

fn port_of(listener: &TcpListener) -> u16 {
    listener
        .local_addr()
        .assured("a bound listener has an address")
        .port()
}

/// A listener on `address` whose accept queue is full, so a connection to it is never answered:
/// the kernel drops the SYN of every further client instead.
async fn silent_listener(address: SocketAddr) -> (TcpListener, Vec<TcpStream>) {
    let socket = TcpSocket::new_v4().assured("a TCP socket can be created");
    socket
        .bind(address)
        .assured("the loopback address is free on this port");
    let listener = socket.listen(1).assured("a bound socket can listen");
    let mut queued = Vec::new();
    for _ in 0..2 {
        let stream = TcpStream::connect(address)
            .await
            .assured("the accept queue has room for its first connections");
        queued.push(stream);
    }
    (listener, queued)
}

/// The dial `authority` ends in, which a check expects to fail.
async fn failed_dial(dialer: &MqttDialer, authority: &str) -> io::Error {
    let dialled = dialer.dial(authority, NetworkOptions::new()).await;
    let Err(error) = dialled else {
        panic!("dialling '{authority}' was expected to fail");
    };
    error
}

#[nervix_primitives::test]
async fn a_connection_dials_the_answers_in_order_until_one_accepts() {
    let fixture = DnsFixture::start().await;
    let refusing = loopback_listener(loopback(1)).await;
    let port = port_of(&refusing);
    drop(refusing);
    let listener = TcpListener::bind(SocketAddr::new(loopback(2), port))
        .await
        .assured("the loopback address is free on this port");
    fixture.answer(BROKER, vec![loopback(1), loopback(2)]);

    let stream = dialer(&fixture, GENEROUS_BUDGET)
        .dial(&format!("{BROKER}:{port}"), NetworkOptions::new())
        .await
        .assured("the second answer accepts the connection");

    let (_accepted, _) = listener
        .accept()
        .await
        .assured("the accepting answer received the connection");
    let peer = stream.peer_addr().assured("a connected stream has a peer");
    assert_eq!(peer, SocketAddr::new(loopback(2), port));
    assert!(fixture.authority.questions_for(BROKER) > 0);
}

#[nervix_primitives::test]
async fn a_name_with_both_address_families_reaches_its_ipv6_answer() {
    let fixture = DnsFixture::start().await;
    let listener = loopback_listener(IpAddr::V6(Ipv6Addr::LOCALHOST)).await;
    let port = port_of(&listener);
    // Nothing listens on the IPv4 answer, which the resolver lists first.
    fixture.answer(BROKER, vec![IpAddr::V6(Ipv6Addr::LOCALHOST), loopback(11)]);

    let stream = dialer(&fixture, GENEROUS_BUDGET)
        .dial(&format!("{BROKER}:{port}"), NetworkOptions::new())
        .await
        .assured("the IPv6 answer accepts the connection");

    let peer = stream.peer_addr().assured("a connected stream has a peer");
    assert_eq!(peer, SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port));
}

#[nervix_primitives::test]
async fn an_address_that_never_answers_leaves_time_for_the_next() {
    let fixture = DnsFixture::start().await;
    let listener = loopback_listener(loopback(3)).await;
    let port = port_of(&listener);
    let (_silent, _queued) = silent_listener(SocketAddr::new(loopback(4), port)).await;
    fixture.answer(BROKER, vec![loopback(4), loopback(3)]);

    let stream = dialer(&fixture, Duration::from_secs(4))
        .dial(&format!("{BROKER}:{port}"), NetworkOptions::new())
        .await
        .assured("the second answer accepts within what the first left");

    let peer = stream.peer_addr().assured("a connected stream has a peer");
    assert_eq!(peer, SocketAddr::new(loopback(3), port));
}

#[nervix_primitives::test]
async fn no_answer_that_accepts_ends_within_the_budget_with_the_last_failure() {
    let fixture = DnsFixture::start().await;
    let refusing = loopback_listener(loopback(5)).await;
    let port = port_of(&refusing);
    drop(refusing);
    let (_silent, _queued) = silent_listener(SocketAddr::new(loopback(6), port)).await;
    fixture.answer(BROKER, vec![loopback(5), loopback(6)]);

    let dialer = dialer(&fixture, Duration::from_secs(1));
    let authority = format!("{BROKER}:{port}");
    let dialling = failed_dial(&dialer, &authority);
    let error = timeout(GENEROUS_BUDGET, dialling)
        .await
        .assured("the budget ends the last attempt");

    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    let described = error.to_string();
    let last = format!("{}: ", SocketAddr::new(loopback(6), port));
    assert!(described.starts_with(&last), "{described}");
}

#[nervix_primitives::test]
async fn names_that_do_not_resolve_keep_their_typed_lookup_failure() {
    let fixture = DnsFixture::start().await;
    for (answer, expected) in [
        (
            DnsAnswer::NameNotFound {
                negative_ttl: Duration::ZERO,
            },
            DnsLookupFailure::NameNotFound,
        ),
        (
            DnsAnswer::NoAddresses {
                negative_ttl: Duration::ZERO,
            },
            DnsLookupFailure::NoAddresses,
        ),
        (DnsAnswer::Silent, DnsLookupFailure::Timeout),
    ] {
        fixture.authority.set(BROKER, answer);
        let error = failed_dial(
            &dialer(&fixture, GENEROUS_BUDGET),
            &format!("{BROKER}:1883"),
        )
        .await;

        let lookup = DnsLookupError::find_in(&error)
            .assured("a failed lookup is the cause the driver receives");
        assert_eq!(lookup.name(), BROKER);
        assert_eq!(lookup.failure(), expected);
    }
}

#[nervix_primitives::test]
async fn literal_addresses_are_dialled_without_a_lookup() {
    let fixture = DnsFixture::start().await;
    let ipv4 = loopback_listener(loopback(7)).await;
    let ipv6 = loopback_listener(IpAddr::V6(Ipv6Addr::LOCALHOST)).await;
    // The driver writes an IPv6 literal without brackets; the bracketed form names the same host.
    let authorities = [
        format!("{}:{}", loopback(7), port_of(&ipv4)),
        format!("::1:{}", port_of(&ipv6)),
        format!("[::1]:{}", port_of(&ipv6)),
    ];
    for authority in authorities {
        let stream = dialer(&fixture, GENEROUS_BUDGET)
            .dial(&authority, NetworkOptions::new())
            .await
            .assured("a literal address that listens accepts the connection");
        let peer = stream.peer_addr().assured("a connected stream has a peer");
        assert_eq!(
            peer.to_string().replace(['[', ']'], ""),
            authority.replace(['[', ']'], "")
        );
    }
    assert_eq!(fixture.authority.total_questions(), 0);
}

#[nervix_primitives::test]
async fn every_attempt_applies_the_drivers_network_options() {
    let fixture = DnsFixture::start().await;
    let listener = loopback_listener(loopback(8)).await;
    let port = port_of(&listener);
    fixture.answer(BROKER, vec![loopback(8)]);
    let mut network = NetworkOptions::new();
    network.set_tcp_nodelay(true);
    network.set_bind_addr(SocketAddr::new(loopback(9), 0));

    let stream = dialer(&fixture, GENEROUS_BUDGET)
        .dial(&format!("{BROKER}:{port}"), network)
        .await
        .assured("the answer accepts the connection");

    assert!(
        stream
            .nodelay()
            .assured("a connected stream reports its options")
    );
    let local = stream
        .local_addr()
        .assured("a connected stream has a local address");
    assert_eq!(local.ip(), loopback(9));
}

#[nervix_primitives::test]
async fn an_authority_without_a_port_is_invalid_input() {
    let fixture = DnsFixture::start().await;
    for authority in [BROKER.to_string(), format!("{BROKER}:mqtt")] {
        let error = failed_dial(&dialer(&fixture, GENEROUS_BUDGET), &authority).await;
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{authority}");
    }
    assert_eq!(fixture.authority.total_questions(), 0);
}

#[nervix_primitives::test]
async fn the_event_loop_connects_through_the_node_resolver() {
    let fixture = DnsFixture::start().await;
    let listener = loopback_listener(loopback(10)).await;
    let port = port_of(&listener);
    fixture.answer(BROKER, vec![loopback(10)]);
    let mut options = MqttOptions::new("nervix-mqtt-dial-check", (BROKER, port));
    MqttDialer::install(&mut options, fixture.dns.clone());
    assert!(options.has_socket_connector());
    let (_client, mut eventloop) = AsyncClient::builder(options)
        .try_build()
        .assured("the check's MQTT options are valid");
    let broker = nervix_primitives::task::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .assured("the client under test dials this listener");
        let mut connect = [0_u8; 256];
        let read = stream
            .read(&mut connect)
            .await
            .assured("the client sends its CONNECT packet");
        stream
            .write_all(&CONNACK)
            .await
            .assured("the client waits for the CONNACK");
        (stream, read)
    });

    let event = timeout(GENEROUS_BUDGET, eventloop.poll())
        .await
        .assured("the stand-in broker answers within the budget")
        .assured("the stand-in broker accepts the connection");

    assert!(
        matches!(event, Event::Incoming(Incoming::ConnAck(_))),
        "{event:?}"
    );
    let (_stream, read) = broker.await.assured("the stand-in broker does not panic");
    assert!(read > 0, "the client sent its CONNECT packet");
    assert!(fixture.authority.questions_for(BROKER) > 0);
}

#[nervix_primitives::test]
async fn an_event_loop_whose_broker_does_not_resolve_reports_the_lookup() {
    let fixture = DnsFixture::start().await;
    fixture.deny(BROKER);
    let mut options = MqttOptions::new("nervix-mqtt-dial-check", (BROKER, 1883));
    MqttDialer::install(&mut options, fixture.dns.clone());
    let (_client, mut eventloop) = AsyncClient::builder(options)
        .try_build()
        .assured("the check's MQTT options are valid");

    let polled = timeout(GENEROUS_BUDGET, eventloop.poll())
        .await
        .assured("the lookup ends within the connect timeout");
    let Err(error) = polled else {
        panic!("a broker whose name does not exist cannot be reached");
    };
    let report = MqttConnectionError::report(error);

    assert!(
        matches!(
            report.current_context(),
            MqttConnectionError::Resolve {
                host,
                failure: DnsLookupFailure::NameNotFound,
            } if host == BROKER
        ),
        "{report:?}"
    );
    assert_eq!(
        report.current_context().to_string(),
        "resolving MQTT host 'mqtt.nervix.test' failed: the name does not exist"
    );
    let lookup = report
        .downcast_ref::<DnsLookupError>()
        .assured("the resolver's own error stays beneath the connection failure");
    assert_eq!(lookup.name(), BROKER);
}

#[test]
fn other_connection_failures_keep_the_drivers_description() {
    let error = ConnectionError::Io(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        "127.0.8.11:1883: connection refused",
    ));

    let report = MqttConnectionError::report(error);

    assert!(matches!(
        report.current_context(),
        MqttConnectionError::Driver(ConnectionError::Io(_))
    ));
    assert_eq!(
        report.current_context().to_string(),
        "I/O: 127.0.8.11:1883: connection refused"
    );
}
