//! How a RabbitMQ client reaches its broker, observed against local DNS authorities and TCP
//! listeners that stand in for the broker's addresses.
//!
//! A listener reads the AMQP protocol header Lapin sends over the transport it was handed, and
//! answers with bytes that are not AMQP, so each connection ends in the AMQP handshake once the
//! path to the broker has been proven. The real broker is the Cucumber suite's.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use meticulous::ResultExt as _;
use nervix_connector::DeadlineElapsed;
use nervix_dns::{DnsConfiguration, DnsLookupFailure, DnsResolver, NameServers};
use nervix_models::{ClientConfigEntry, DeadlineKey};
use nervix_primitives::{
    net::{TcpListener, TcpSocket, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::Instant,
};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::*;

/// The fixture name every check resolves.
const BROKER: &str = "rabbitmq.nervix.test";
/// The first bytes of every AMQP 0-9-1 connection.
const AMQP_PROTOCOL_HEADER: [u8; 8] = *b"AMQP\x00\x00\x09\x01";
/// A budget no check expects to exhaust.
const GENEROUS_BUDGET: Duration = Duration::from_secs(30);
/// How long a listener waits for the client to close its side after the handshake failed.
const CLOSE_DEADLINE: Duration = Duration::from_secs(10);

/// The connection deadline a client whose `connect_timeout_ms` is `budget` takes.
fn connect_deadline(budget: Duration) -> ConfiguredDeadline {
    let millis = budget.as_millis().to_string();
    ConfiguredDeadline::connect(&[ClientConfigEntry {
        key: "connect_timeout_ms".to_string(),
        value: millis,
    }])
    .assured("each check's budget is a positive whole number of milliseconds")
}

/// A resolver asking one local authority, with the files it was loaded from.
struct Fixture {
    authority: DnsAuthority,
    dns: DnsResolver,
    _files: TempDir,
}

impl Fixture {
    /// A resolver whose hosts file names `localhost` as `localhost`, so a host that silently
    /// became `localhost` would dial there rather than where the address said.
    async fn start(localhost: IpAddr) -> Self {
        let authority = DnsAuthority::start_on_loopback()
            .await
            .assured("a loopback UDP port is available");
        let files = tempfile::tempdir().assured("a temporary directory can be created");
        let resolver_configuration = files.path().join("resolv.conf");
        let hosts_file = files.path().join("hosts");
        std::fs::write(
            &resolver_configuration,
            "search nervix.test\noptions ndots:1 timeout:1 attempts:1\n",
        )
        .assured("the resolver configuration can be written");
        std::fs::write(&hosts_file, format!("{localhost} localhost\n"))
            .assured("the hosts file can be written");
        let dns = DnsResolver::load(DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: NameServers::Explicit(vec![authority.address()]),
        })
        .await
        .assured("the fixture configuration is valid");
        Self {
            authority,
            dns,
            _files: files,
        }
    }

    fn answer(&self, addresses: Vec<IpAddr>) {
        self.answer_name(BROKER, addresses);
    }

    fn answer_name(&self, name: &str, addresses: Vec<IpAddr>) {
        self.authority.set(
            name,
            DnsAnswer::Addresses {
                addresses,
                ttl: Duration::from_secs(1),
            },
        );
    }

    fn broker(&self, addr: &str) -> RabbitMqBroker {
        RabbitMqBroker::from_config(&entries(addr), self.dns.clone())
            .assured("the fixture address is a valid AMQP URI")
    }
}

fn entries(addr: &str) -> Vec<ClientConfigEntry> {
    vec![ClientConfigEntry {
        key: "addr".to_string(),
        value: addr.to_string(),
    }]
}

fn loopback(last: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 7, last))
}

/// A listener standing in for a broker address: it reads the AMQP protocol header, answers with
/// bytes that are not AMQP, and then waits for the client to close the connection.
fn stand_in_broker(listener: TcpListener) -> JoinHandle<[u8; 8]> {
    nervix_primitives::task::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .assured("the client under test dials this listener");
        let mut header = [0_u8; 8];
        stream
            .read_exact(&mut header)
            .await
            .assured("Lapin sends its protocol header before anything else");
        stream
            .write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            .await
            .assured("the client is still connected while it waits for Connection.Start");
        let mut rest = Vec::new();
        timeout(CLOSE_DEADLINE, stream.read_to_end(&mut rest))
            .await
            .assured("a failed handshake closes the client's socket")
            .assured("reading until the client closes cannot fail on loopback");
        header
    })
}

/// A listener standing in for a broker that never answers: it reads the AMQP protocol header,
/// tells `header_read` it did, answers nothing, and then waits for the client to close the
/// connection.
fn silent_broker(
    listener: TcpListener,
    header_read: oneshot::Sender<()>,
    closes: &'static str,
) -> JoinHandle<[u8; 8]> {
    nervix_primitives::task::spawn(async move {
        let (mut stream, _) = listener
            .accept()
            .await
            .assured("the client under test dials this listener");
        let mut header = [0_u8; 8];
        stream
            .read_exact(&mut header)
            .await
            .assured("Lapin sends its protocol header before anything else");
        header_read
            .send(())
            .assured("the check waits for the header until the broker has read it");
        let mut rest = Vec::new();
        timeout(CLOSE_DEADLINE, stream.read_to_end(&mut rest))
            .await
            .assured(closes)
            .assured("reading until the client closes cannot fail on loopback");
        header
    })
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

async fn loopback_listener(address: IpAddr) -> TcpListener {
    TcpListener::bind(SocketAddr::new(address, 0))
        .await
        .assured("a loopback port is available")
}

#[nervix_primitives::test]
async fn invalid_addresses_and_ca_files_are_configuration_failures() {
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let root = tempfile::tempdir().assured("a temporary directory can be created");
    let not_utf8 = root.path().join("ca.pem");
    std::fs::write(&not_utf8, [0xff]).assured("the CA fixture can be written");
    let missing = root.path().join("missing.pem");
    let tls = |ca_file: &std::path::Path| {
        vec![
            ClientConfigEntry {
                key: "addr".to_string(),
                value: "amqps://guest:guest@127.0.0.1:5671/%2f".to_string(),
            },
            ClientConfigEntry {
                key: "tls_ca_file".to_string(),
                value: ca_file.display().to_string(),
            },
        ]
    };
    let cases = [
        (Vec::new(), "missing addr"),
        (entries("not a url"), "not a URL"),
        (entries("http://127.0.0.1:5672/%2f"), "not an AMQP scheme"),
        (
            entries("amqp:guest:secret@127.0.0.1:5672"),
            "an address without an authority",
        ),
        (
            entries("amqp://guest:secret@127.0.0.1:99999/%2f"),
            "an invalid port",
        ),
        (entries("amqp:/%2f"), "an address that names no host"),
        (
            entries("amqp://guest:secret@127.0.0.1:0/%2f"),
            "an address whose port is zero",
        ),
        (tls(&not_utf8), "a CA file that is not UTF-8"),
        (tls(&missing), "a CA file that cannot be read"),
    ];
    for (entries, case) in cases {
        let Err(report) = RabbitMqBroker::from_config(&entries, fixture.dns.clone()) else {
            panic!("{case} must not describe a broker");
        };
        assert!(report.current_context().is_configuration(), "{case}");
        let reported = format!("{report:?}");
        assert!(
            !reported.contains("secret"),
            "{case} repeats its credentials: {reported}"
        );
    }
}

#[nervix_primitives::test]
async fn connections_dial_the_first_answer_that_accepts_and_hand_lapin_the_transport() {
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let listener = loopback_listener(loopback(1)).await;
    let port = listener
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    fixture.answer(vec![loopback(2), loopback(1)]);
    let broker_side = stand_in_broker(listener);

    let broker = fixture.broker(&format!("amqp://guest:guest@{BROKER}:{port}/%2f"));
    let Err(report) = broker
        .connect_within(connect_deadline(GENEROUS_BUDGET))
        .await
    else {
        panic!("a listener that does not speak AMQP cannot complete the handshake");
    };

    assert!(matches!(
        report.current_context(),
        RabbitMqConnectError::AmqpHandshake { host } if host == BROKER
    ));
    let header = broker_side
        .await
        .assured("the stand-in broker does not panic");
    assert_eq!(header, AMQP_PROTOCOL_HEADER);
    assert!(fixture.authority.questions_for(BROKER) > 0);
}

#[nervix_primitives::test]
async fn an_address_that_never_answers_leaves_time_for_the_next() {
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let listener = loopback_listener(loopback(3)).await;
    let port = listener
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    let (_silent, _queued) = silent_listener(SocketAddr::new(loopback(4), port)).await;
    fixture.answer(vec![loopback(4), loopback(3)]);
    let broker_side = stand_in_broker(listener);

    let broker = fixture.broker(&format!("amqp://guest:guest@{BROKER}:{port}/%2f"));
    let Err(report) = broker
        .connect_within(connect_deadline(Duration::from_secs(4)))
        .await
    else {
        panic!("a listener that does not speak AMQP cannot complete the handshake");
    };

    assert!(matches!(
        report.current_context(),
        RabbitMqConnectError::AmqpHandshake { .. }
    ));
    let header = broker_side
        .await
        .assured("the stand-in broker does not panic");
    assert_eq!(header, AMQP_PROTOCOL_HEADER);
}

#[nervix_primitives::test]
async fn no_answer_that_accepts_ends_within_the_budget() {
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let refusing = loopback_listener(loopback(5)).await;
    let port = refusing
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    drop(refusing);
    let (_silent, _queued) = silent_listener(SocketAddr::new(loopback(6), port)).await;
    fixture.answer(vec![loopback(5), loopback(6)]);

    let broker = fixture.broker(&format!("amqp://guest:guest@{BROKER}:{port}/%2f"));
    let budget = Duration::from_secs(1);
    let started = Instant::now();
    let Err(report) = broker.connect_within(connect_deadline(budget)).await else {
        panic!("neither answer accepts a connection");
    };

    assert!(matches!(
        report.current_context(),
        RabbitMqConnectError::Unreachable { host } if host == BROKER
    ));
    let elapsed = report.downcast_ref::<DeadlineElapsed>().copied();
    assert_eq!(
        elapsed.map(|elapsed| (elapsed.key, elapsed.bound)),
        Some((DeadlineKey::ConnectTimeout, budget)),
        "the silent answer ran the connection out of its deadline: {report:?}"
    );
    let attempts: Vec<&String> = report
        .frames()
        .filter_map(|frame| frame.downcast_ref::<String>())
        .collect();
    assert_eq!(attempts.len(), 2, "every answer is reported: {report:?}");
    for address in [loopback(5), loopback(6)] {
        let dialled = format!("{}: ", SocketAddr::new(address, port));
        assert!(
            attempts.iter().any(|attempt| attempt.starts_with(&dialled)),
            "{dialled} is reported: {report:?}"
        );
    }
    assert!(started.elapsed() < GENEROUS_BUDGET);
}

#[nervix_primitives::test]
async fn an_amqp_handshake_the_broker_never_answers_ends_within_the_budget_and_closes_its_socket() {
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let listener = loopback_listener(loopback(13)).await;
    let port = listener
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    fixture.answer(vec![loopback(13)]);
    let (header_read, _header_was_read) = oneshot::channel();
    let broker_side = silent_broker(
        listener,
        header_read,
        "a handshake that runs out of its budget closes the client's socket",
    );

    let broker = fixture.broker(&format!("amqp://guest:guest@{BROKER}:{port}/%2f"));
    let budget = Duration::from_millis(500);
    let attempt = timeout(
        GENEROUS_BUDGET,
        broker.connect_within(connect_deadline(budget)),
    )
    .await
    .assured("a handshake the broker never answers ends within its budget");
    let Err(report) = attempt else {
        panic!("a broker that never answers cannot complete the handshake");
    };

    assert!(matches!(
        report.current_context(),
        RabbitMqConnectError::AmqpHandshake { host } if host == BROKER
    ));
    let elapsed = report.downcast_ref::<DeadlineElapsed>().copied();
    assert_eq!(
        elapsed.map(|elapsed| (elapsed.key, elapsed.bound)),
        Some((DeadlineKey::ConnectTimeout, budget)),
        "the handshake ran the connection out of its deadline: {report:?}"
    );
    let header = broker_side
        .await
        .assured("the silent broker does not panic");
    assert_eq!(header, AMQP_PROTOCOL_HEADER);
}

#[nervix_primitives::test]
async fn a_connection_its_caller_stops_waiting_for_closes_its_socket() {
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let listener = loopback_listener(loopback(14)).await;
    let port = listener
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    fixture.answer(vec![loopback(14)]);
    let (header_read, header_was_read) = oneshot::channel();
    let broker_side = silent_broker(
        listener,
        header_read,
        "a connection whose caller stopped waiting closes its socket",
    );

    let broker = fixture.broker(&format!("amqp://guest:guest@{BROKER}:{port}/%2f"));
    let connecting = broker.connect_within(connect_deadline(GENEROUS_BUDGET));
    nervix_primitives::select! {
        _ = connecting => panic!("a broker that never answers cannot complete the handshake"),
        read = header_was_read => read.assured("the silent broker reads the header it is sent"),
    }

    // The connection is dropped in the middle of its AMQP handshake.
    let header = broker_side
        .await
        .assured("the silent broker does not panic");
    assert_eq!(header, AMQP_PROTOCOL_HEADER);
}

#[nervix_primitives::test]
async fn opening_steps_end_within_what_remains_of_the_budget() {
    let budget = Duration::from_millis(50);
    let opening = OpeningBudget {
        deadline: OperationBudget::start(budget, LookupShare::Whole),
        connect_timeout: connect_deadline(budget),
    };

    let answered = opening
        .within("answering at once", async { Ok::<_, lapin::Error>(7_u8) })
        .await;
    assert!(matches!(answered, Ok(7)));
    let refused = opening
        .within("refusing at once", async {
            Err::<(), _>(lapin::Error::from(io::Error::other(
                "the broker closed the connection",
            )))
        })
        .await;
    assert!(matches!(refused, Err(OpeningStepFailure::Failed(_))));
    let stalled = opening
        .within(
            "opening the test channel",
            std::future::pending::<lapin::Result<()>>(),
        )
        .await;
    let Err(OpeningStepFailure::Elapsed(elapsed)) = stalled else {
        panic!("a step the broker never answers runs out of the budget");
    };
    assert_eq!(
        (elapsed.operation, elapsed.key, elapsed.bound),
        (
            "opening the test channel",
            DeadlineKey::ConnectTimeout,
            budget
        )
    );
}

#[test]
fn an_opening_step_failure_leads_its_report_beneath_its_owners_context() {
    let context = || RabbitMqConnectError::AmqpHandshake {
        host: BROKER.to_string(),
    };
    let elapsed = connect_deadline(Duration::from_secs(1)).elapsed("opening the test channel");
    let overdue = OpeningStepFailure::Elapsed(elapsed).beneath(context());
    assert!(matches!(
        overdue.current_context(),
        RabbitMqConnectError::AmqpHandshake { host } if host == BROKER
    ));
    assert_eq!(
        overdue.downcast_ref::<DeadlineElapsed>().copied(),
        Some(elapsed)
    );
    let leading = overdue
        .frames()
        .find_map(|frame| frame.downcast_ref::<String>());
    assert_eq!(
        leading.map(String::as_str),
        Some("opening the test channel did not finish within connect_timeout_ms 1s")
    );

    let refused = OpeningStepFailure::Failed(lapin::Error::from(io::Error::other(
        "the broker closed the connection",
    )))
    .beneath(context());
    assert!(refused.downcast_ref::<DeadlineElapsed>().is_none());
    let leading = refused
        .frames()
        .find_map(|frame| frame.downcast_ref::<String>());
    let Some(leading) = leading else {
        panic!("Lapin's failure leads the report: {refused:?}");
    };
    assert!(
        leading.contains("the broker closed the connection"),
        "{refused:?}"
    );
}

#[nervix_primitives::test]
async fn an_opening_socket_shuts_its_socket_down_unless_its_connection_opened() {
    for opened in [false, true] {
        let listener = loopback_listener(loopback(15)).await;
        let address = listener
            .local_addr()
            .assured("a bound listener has an address");
        let runtime = lapin::runtime::default_runtime().assured("a test runs inside Tokio");
        let mut client = runtime
            .tcp_connect_addr(address)
            .await
            .assured("the loopback listener accepts a connection");
        let (mut accepted, _) = listener
            .accept()
            .await
            .assured("the client dialled this listener");
        let socket =
            OpeningSocket::hold(&client, BROKER).assured("a connected socket has a second handle");

        if opened {
            socket.release();
            client
                .get_mut()
                .write_all(b"x")
                .await
                .assured("a released handle leaves the connection open");
            let mut byte = [0_u8; 1];
            accepted
                .read_exact(&mut byte)
                .await
                .assured("the byte the client wrote arrives");
            assert_eq!(&byte, b"x");
        } else {
            drop(socket);
            let mut rest = Vec::new();
            timeout(CLOSE_DEADLINE, accepted.read_to_end(&mut rest))
                .await
                .assured("a dropped handle shuts the socket down")
                .assured("reading until the client closes cannot fail on loopback");
            assert!(rest.is_empty());
            assert!(
                client.get_mut().write_all(b"x").await.is_err(),
                "a socket that was shut down takes no more writes"
            );
        }
    }
}

#[nervix_primitives::test]
async fn names_that_do_not_resolve_keep_their_dns_failure() {
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    for (answer, expected) in [
        (
            DnsAnswer::NameNotFound {
                negative_ttl: Duration::ZERO,
            },
            DnsLookupFailure::NameNotFound,
        ),
        (DnsAnswer::Silent, DnsLookupFailure::Timeout),
    ] {
        fixture.authority.set(BROKER, answer);
        let broker = fixture.broker(&format!("amqp://guest:guest@{BROKER}.:5672/%2f"));
        let Err(report) = broker
            .connect_within(connect_deadline(GENEROUS_BUDGET))
            .await
        else {
            panic!("a name that does not resolve cannot be dialled");
        };
        let RabbitMqConnectError::Resolve { host, failure } = report.current_context() else {
            panic!("expected a resolution failure, got {report:?}");
        };
        assert_eq!(host, &format!("{BROKER}."));
        assert_eq!(*failure, expected);
        assert!(!report.current_context().is_configuration());
    }
}

#[nervix_primitives::test]
async fn ipv6_literals_are_dialled_as_written() {
    // `localhost` names an address nothing listens on, so dialling it instead of the literal fails
    // before the AMQP handshake.
    let fixture = Fixture::start(loopback(9)).await;
    let listener = TcpListener::bind("[::1]:0")
        .await
        .assured("the IPv6 loopback address is available");
    let port = listener
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    let broker_side = stand_in_broker(listener);

    let broker = fixture.broker(&format!("amqp://guest:guest@[::1]:{port}/%2f"));
    let Err(report) = broker
        .connect_within(connect_deadline(GENEROUS_BUDGET))
        .await
    else {
        panic!("a listener that does not speak AMQP cannot complete the handshake");
    };

    assert!(matches!(
        report.current_context(),
        RabbitMqConnectError::AmqpHandshake { host } if host == "::1"
    ));
    let header = broker_side
        .await
        .assured("the stand-in broker does not panic");
    assert_eq!(header, AMQP_PROTOCOL_HEADER);
    assert_eq!(fixture.authority.total_questions(), 0);
}

#[nervix_primitives::test]
async fn amqps_handshakes_that_fail_or_stall_are_tls_failures() {
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let closing = loopback_listener(loopback(10)).await;
    let port = closing
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    let stalling = TcpListener::bind(SocketAddr::new(loopback(11), port))
        .await
        .assured("the loopback address is free on this port");
    let closes = nervix_primitives::task::spawn(async move {
        let (stream, _) = closing
            .accept()
            .await
            .assured("the client under test dials this listener");
        drop(stream);
    });
    let stalls = nervix_primitives::task::spawn(async move {
        let (mut stream, _) = stalling
            .accept()
            .await
            .assured("the client under test dials this listener");
        let mut hello = Vec::new();
        timeout(CLOSE_DEADLINE, stream.read_to_end(&mut hello))
            .await
            .assured("a stalled handshake closes the client's socket at its deadline")
            .assured("reading until the client closes cannot fail on loopback");
    });

    for (name, address, budget) in [
        ("closing.nervix.test", loopback(10), GENEROUS_BUDGET),
        (
            "stalling.nervix.test",
            loopback(11),
            Duration::from_millis(500),
        ),
    ] {
        fixture.answer_name(name, vec![address]);
        let broker = fixture.broker(&format!("amqps://guest:guest@{name}:{port}/%2f"));
        let Err(report) = broker.connect_within(connect_deadline(budget)).await else {
            panic!("a listener that does not speak TLS cannot complete the handshake");
        };
        assert!(
            matches!(
                report.current_context(),
                RabbitMqConnectError::TlsHandshake { host } if host == name
            ),
            "{name}: {report:?}"
        );
        // Only the handshake that stalled ran out of the deadline, and its failure names the key.
        let elapsed = report.downcast_ref::<DeadlineElapsed>().copied();
        if budget == GENEROUS_BUDGET {
            assert_eq!(elapsed, None, "{name}: {report:?}");
        } else {
            assert_eq!(
                elapsed.map(|elapsed| (elapsed.key, elapsed.bound)),
                Some((DeadlineKey::ConnectTimeout, budget)),
                "{name}: {report:?}"
            );
        }
    }
    closes.await.assured("the closing listener does not panic");
    stalls.await.assured("the stalling listener does not panic");
}

#[test]
fn every_runtime_connects_through_the_resolver_it_was_given() {
    for round in 0..2 {
        let runtime = nervix_primitives::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .assured("a Tokio runtime can be built");
        runtime.block_on(async {
            let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
            let listener = loopback_listener(loopback(12)).await;
            let port = listener
                .local_addr()
                .assured("a bound listener has an address")
                .port();
            fixture.answer(vec![loopback(12)]);
            let broker_side = stand_in_broker(listener);
            let broker = fixture.broker(&format!("amqp://guest:guest@{BROKER}:{port}/%2f"));
            let Err(report) = broker
                .connect_within(connect_deadline(GENEROUS_BUDGET))
                .await
            else {
                panic!("a listener that does not speak AMQP cannot complete the handshake");
            };
            assert!(
                matches!(
                    report.current_context(),
                    RabbitMqConnectError::AmqpHandshake { .. }
                ),
                "round {round}: {report:?}"
            );
            let header = broker_side
                .await
                .assured("the stand-in broker does not panic");
            assert_eq!(header, AMQP_PROTOCOL_HEADER);
            assert!(fixture.authority.questions_for(BROKER) > 0, "round {round}");
        });
    }
}

#[test]
fn a_zero_connection_budget_leaves_nothing_for_address_attempts() {
    let deadline = OperationBudget::start(Duration::ZERO, LookupShare::Whole);
    let addresses = [
        SocketAddr::new(loopback(1), 5672),
        SocketAddr::new(loopback(2), 5672),
    ];
    assert_eq!(deadline.remaining(), Duration::ZERO);
    assert_eq!(
        deadline
            .attempts(&addresses)
            .map(|attempt| attempt.budget)
            .collect::<Vec<_>>(),
        vec![Duration::ZERO, Duration::ZERO]
    );
}

#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tls;
