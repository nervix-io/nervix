//! Certificate validity, session deadlines and replay under the Turmoil clock.
//!
//! Layer: test harness outside the product layer order.
//!
//! - **Owns.** Fixed-validity certificate fixtures and the authenticated-session scenarios that
//!   prove the transport judges certificates and deadlines by simulated time on both peers.
//! - **Depends on.** The Turmoil runner, its simulated clock, entropy and semantic trace.
//! - **Must not know.** HTTP/2 pools, relays, or persisted cluster state.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    path::PathBuf,
    process::Command,
    sync::Arc as StdArc,
    time::{Duration, SystemTime},
};

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_models::ClusterNodeName;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType, date_time_ymd,
};
use tokio::time::{Instant, sleep_until};

use crate::{
    TlsConfigBundle, TransportClock, TransportEntropy, TransportError,
    simulation_runner::{
        ClockSkew, HostSupervisor, NetworkParameters, SemanticTrace, SimulatedEntropy,
        SimulatedUtc, SimulationBounds, SimulationConfig, Topology,
    },
};

const CLUSTER: &str = "simulated";
const PORT: u16 = 7443;
const SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const TICK: Duration = Duration::from_millis(1);
const REPLAY_TRACE_VARIABLE: &str = "NERVIX_SIMULATION_TRACE";

/// Seconds from the Unix epoch to 2027-01-15T00:00:00Z, the fixtures' validity origin. It lies
/// after any host wall clock these tests run on, so a certificate check that consulted the host
/// clock instead of the simulation would find every fixture not yet valid.
const EPOCH_UNIX_SECONDS: u64 = 1_799_971_200;

/// A certificate's validity window, in seconds after the simulation epoch.
#[derive(Debug, Clone, Copy)]
struct Validity {
    not_before: u64,
    not_after: u64,
}

struct ClusterAuthority {
    certificate: rcgen::Certificate,
    key: KeyPair,
}

impl ClusterAuthority {
    fn new() -> Self {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        params.not_before = date_time_ymd(2027, 1, 1);
        params.not_after = date_time_ymd(2028, 1, 1);
        let key = KeyPair::generate().assured("the test CA key is generated");
        let certificate = params
            .self_signed(&key)
            .assured("the test CA certificate is self-signed");
        Self { certificate, key }
    }

    /// Issue a bundle for `node`, reachable as the DNS name `host`, judged by `clock`.
    fn issue(
        &self,
        node: &str,
        host: &str,
        validity: Validity,
        skew: ClockSkew,
    ) -> TlsConfigBundle {
        let mut params = CertificateParams::new(vec![host.to_string()])
            .assured("the fixture host name is a valid DNS SAN");
        params.subject_alt_names.push(SanType::URI(
            format!("nervix://cluster/{CLUSTER}/node/{node}")
                .try_into()
                .assured("the fixture identity URI is valid"),
        ));
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let origin = date_time_ymd(2027, 1, 15);
        params.not_before = origin + Duration::from_secs(validity.not_before);
        params.not_after = origin + Duration::from_secs(validity.not_after);
        let key = KeyPair::generate().assured("the test node key is generated");
        let certificate = params
            .signed_by(&key, &self.certificate, &self.key)
            .assured("the test CA signs the node certificate");
        TlsConfigBundle::from_pem(
            self.certificate.pem().as_bytes(),
            certificate.pem().as_bytes(),
            key.serialize_pem().as_bytes(),
            TransportClock::from_provider(StdArc::new(SimulatedUtc::new(skew))),
        )
        .assured("the fixture bundle parses")
    }
}

fn simulation(seed: u64, simulated_duration: Duration) -> SimulationConfig {
    let max_steps = simulated_duration
        .as_millis()
        .checked_add(1_000)
        .assured("the scenario durations are a few minutes");
    SimulationConfig {
        seed,
        epoch: SystemTime::UNIX_EPOCH + Duration::from_secs(EPOCH_UNIX_SECONDS),
        topology: Topology::Ipv4,
        network: NetworkParameters::LOSSLESS,
        bounds: SimulationBounds {
            simulated_duration,
            tick: TICK,
            max_steps: NonZeroUsize::new(
                usize::try_from(max_steps).assured("a few minutes of milliseconds fit usize"),
            )
            .assured("the step bound is at least one thousand"),
            wall_duration: Duration::from_secs(120),
        },
    }
}

/// The stable, secret-free description of a failed session.
fn outcome(error: &TransportError) -> String {
    let TransportError::Io(io_error) = error else {
        return format!("rejected: {error}");
    };
    match io_error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
    {
        Some(tls) => format!("tls: {tls:?}"),
        None => format!("io: {:?}", io_error.kind()),
    }
}

/// Serve authenticated sessions until the simulation ends, recording each decision and holding
/// every accepted session until its certificate deadline drains it.
async fn serve(tls: TlsConfigBundle, trace: SemanticTrace) -> io::Result<()> {
    let listener =
        turmoil::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, PORT))).await?;
    loop {
        tokio::task::consume_budget().await;
        let (tcp, peer_addr) = listener.accept().await?;
        let tls = tls.clone();
        let trace = trace.clone();
        tokio::spawn(async move {
            match tls.accept(tcp, peer_addr, SETUP_TIMEOUT, CLUSTER).await {
                Ok(session) => {
                    trace.record("server", format!("accepted {}", session.peer.node_id));
                    sleep_until(session.expires_at).await;
                    trace.record("server", "certificate deadline drained the session");
                    drop(session.stream);
                }
                Err(error) => trace.record("server", outcome(error.current_context())),
            }
        });
    }
}

/// Open one authenticated session to the server, recording the decision. An accepted session is
/// held until its certificate deadline drains it.
async fn dial(tls: &TlsConfigBundle, trace: &SemanticTrace) -> Option<TransportError> {
    dial_expecting(tls, trace, "server").await
}

/// [`dial`], expecting the server's certificate to name `expected_node`.
async fn dial_expecting(
    tls: &TlsConfigBundle,
    trace: &SemanticTrace,
    expected_node: &str,
) -> Option<TransportError> {
    let tcp = match turmoil::net::TcpStream::connect(("server", PORT)).await {
        Ok(tcp) => tcp,
        Err(error) => {
            trace.record("client", format!("connect failed: {:?}", error.kind()));
            return None;
        }
    };
    let expected = ClusterNodeName::parse(expected_node).assured("the fixture node name is valid");
    match tls.connect(tcp, "server", CLUSTER, Some(&expected)).await {
        Ok(session) => {
            trace.record("client", format!("accepted {}", session.peer.node_id));
            let trace = trace.clone();
            tokio::spawn(async move {
                sleep_until(session.expires_at).await;
                trace.record("client", "certificate deadline drained the session");
                drop(session.stream);
            });
            None
        }
        Err(error) => {
            trace.record("client", outcome(&error));
            Some(error)
        }
    }
}

/// Dial at 5 s, 30 s and 90 s against a server certificate valid from 10 s to 60 s and a client
/// certificate valid from 0 s to 120 s, recording every decision and both drain deadlines.
fn validity_window_trace(seed: u64) -> SemanticTrace {
    let authority = StdArc::new(ClusterAuthority::new());
    let trace = SemanticTrace::default();
    let server_trace = trace.clone();
    let client_trace = trace.clone();
    let server_authority = StdArc::clone(&authority);
    let result = simulation(seed, Duration::from_secs(120)).run("validity windows", move |sim| {
        sim.host("server", move || {
            let tls = server_authority.issue(
                "server",
                "server",
                Validity {
                    not_before: 10,
                    not_after: 60,
                },
                ClockSkew::Exact,
            );
            let trace = server_trace.clone();
            async move { HostSupervisor::run(serve(tls, trace)).await }
        });
        sim.client("client", async move {
            let entropy = SimulatedEntropy::new(seed, "client");
            let entropy = TransportEntropy::from_source(move || entropy.next_u64());
            client_trace.record(
                "client",
                format!("process epoch {:#018x}", entropy.next_u64()),
            );
            let tls = authority.issue(
                "client",
                "client",
                Validity {
                    not_before: 0,
                    not_after: 120,
                },
                ClockSkew::Exact,
            );
            let origin = Instant::now();
            for offset in [5, 30, 90] {
                sleep_until(origin + Duration::from_secs(offset)).await;
                client_trace.record("client", format!("dialing at {offset}s"));
                dial(&tls, &client_trace).await;
            }
            sleep_until(origin + Duration::from_secs(100)).await;
            Ok(())
        });
    });
    assert!(result.is_ok(), "seed {seed}: {result:?}");
    trace
}

fn find(trace: &SemanticTrace, host: &str, event: &str) -> Duration {
    let events = trace.events();
    let Some(found) = events
        .iter()
        .find(|candidate| candidate.host == host && candidate.event == event)
    else {
        panic!("{host} never recorded {event:?}:\n{}", trace.render());
    };
    found.at
}

#[test]
fn certificate_validity_and_expiry_deadlines_follow_simulated_time_on_both_peers() {
    let trace = validity_window_trace(7);
    let rendered = trace.render();
    // Link latency decides how the two hosts interleave, so each host's own decisions are compared
    // in order, apart from the other's.
    let decisions = |host: &str| {
        trace
            .events()
            .into_iter()
            .filter(|event| event.host == host)
            .map(|event| event.event)
            .filter(|event| !event.starts_with("dialing") && !event.starts_with("process"))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        decisions("server"),
        [
            // At 5 s the server's own certificate is not valid yet, so it refuses before TLS.
            "rejected: peer handshake is invalid: certificate is not valid yet",
            // At 30 s both certificates are current and the server authenticates the client.
            "accepted client",
            // The session drains at the earlier expiry, 60 s, in simulated time.
            "certificate deadline drained the session",
            // At 90 s the server's own certificate has expired.
            "rejected: peer handshake is invalid: certificate has expired",
        ]
        .map(str::to_string),
        "{rendered}"
    );
    assert_eq!(
        decisions("client"),
        [
            "io: UnexpectedEof",
            "accepted server",
            "certificate deadline drained the session",
            "io: UnexpectedEof",
        ]
        .map(str::to_string),
        "{rendered}"
    );
    for host in ["server", "client"] {
        let drained = find(&trace, host, "certificate deadline drained the session");
        assert!(
            drained >= Duration::from_secs(60) && drained < Duration::from_secs(61),
            "{host} drained at {drained:?}:\n{rendered}"
        );
    }
}

/// Run one handshake at 50 s between a server certificate valid from 0 s to 100 s and a client
/// certificate valid from 40 s to 1000 s, with each peer's clock skewed as given.
fn skewed_handshake(server_skew: ClockSkew, client_skew: ClockSkew) -> SemanticTrace {
    let authority = StdArc::new(ClusterAuthority::new());
    let trace = SemanticTrace::default();
    let server_trace = trace.clone();
    let client_trace = trace.clone();
    let server_authority = StdArc::clone(&authority);
    let result = simulation(11, Duration::from_secs(60)).run("skewed handshake", move |sim| {
        sim.host("server", move || {
            let tls = server_authority.issue(
                "server",
                "server",
                Validity {
                    not_before: 0,
                    not_after: 100,
                },
                server_skew,
            );
            let trace = server_trace.clone();
            async move { HostSupervisor::run(serve(tls, trace)).await }
        });
        sim.client("client", async move {
            let tls = authority.issue(
                "client",
                "client",
                Validity {
                    not_before: 40,
                    not_after: 1000,
                },
                client_skew,
            );
            sleep_until(Instant::now() + Duration::from_secs(50)).await;
            dial(&tls, &client_trace).await;
            sleep_until(Instant::now() + Duration::from_secs(1)).await;
            Ok(())
        });
    });
    assert!(result.is_ok(), "{result:?}");
    trace
}

#[test]
fn client_rustls_verification_reads_the_bundle_clock() {
    // The client reads 130 s: its own certificate is current, the server's expired at 100 s.
    let trace = skewed_handshake(ClockSkew::Exact, ClockSkew::Ahead(Duration::from_secs(80)));
    let rendered = trace.render();
    let events = trace.events();
    let [client, server] = events.as_slice() else {
        panic!("expected one decision per peer:\n{rendered}");
    };
    assert_eq!(client.host, "client", "{rendered}");
    assert!(
        client
            .event
            .starts_with("tls: InvalidCertificate(ExpiredContext"),
        "{rendered}"
    );
    let expected_time = EPOCH_UNIX_SECONDS + 130;
    assert!(
        client
            .event
            .contains(&format!("time: UnixTime({expected_time})")),
        "the client judged expiry at simulated UTC plus its skew:\n{rendered}"
    );
    assert_eq!(server.host, "server", "{rendered}");
    assert_eq!(
        server.event, "tls: AlertReceived(CertificateExpired)",
        "{rendered}"
    );
}

#[test]
fn server_rustls_verification_reads_the_bundle_clock() {
    // The server reads 30 s: its own certificate is current, the client's starts at 40 s.
    let trace = skewed_handshake(ClockSkew::Behind(Duration::from_secs(20)), ClockSkew::Exact);
    let rendered = trace.render();
    let events = trace.events();
    let Some(server) = events.iter().find(|event| event.host == "server") else {
        panic!("the server recorded no decision:\n{rendered}");
    };
    assert!(
        server
            .event
            .starts_with("tls: InvalidCertificate(NotValidYetContext"),
        "{rendered}"
    );
    let expected_time = EPOCH_UNIX_SECONDS + 30;
    assert!(
        server
            .event
            .contains(&format!("time: UnixTime({expected_time})")),
        "the server judged validity at simulated UTC minus its skew:\n{rendered}"
    );
    // In TLS 1.3 the client completes its side before the server verifies the client certificate,
    // so the client accepts the server and learns of the rejection only from the closed session.
    let Some(client) = events.iter().find(|event| event.host == "client") else {
        panic!("the client recorded no decision:\n{rendered}");
    };
    assert_eq!(client.event, "accepted server", "{rendered}");
}

#[test]
fn skewed_clocks_accept_current_certificates() {
    // Both clocks are skewed but still read inside both windows, so the session is accepted.
    let trace = skewed_handshake(
        ClockSkew::Ahead(Duration::from_secs(10)),
        ClockSkew::Behind(Duration::from_secs(5)),
    );
    let rendered = trace.render();
    let decisions = trace
        .events()
        .into_iter()
        .map(|event| format!("{} {}", event.host, event.event))
        .collect::<Vec<_>>();
    assert!(
        decisions.contains(&"server accepted client".to_string()),
        "{rendered}"
    );
    assert!(
        decisions.contains(&"client accepted server".to_string()),
        "{rendered}"
    );
}

#[test]
fn stalled_handshake_times_out_after_the_simulated_setup_deadline() {
    let authority = StdArc::new(ClusterAuthority::new());
    let trace = SemanticTrace::default();
    let server_trace = trace.clone();
    let result = simulation(13, Duration::from_secs(30)).run("stalled handshake", move |sim| {
        sim.host("server", move || {
            let tls = authority.issue(
                "server",
                "server",
                Validity {
                    not_before: 0,
                    not_after: 100,
                },
                ClockSkew::Exact,
            );
            let trace = server_trace.clone();
            async move {
                HostSupervisor::run(async move {
                    let listener = turmoil::net::TcpListener::bind(SocketAddr::from((
                        Ipv4Addr::UNSPECIFIED,
                        PORT,
                    )))
                    .await?;
                    let (tcp, peer_addr) = listener.accept().await?;
                    let started = Instant::now();
                    let result = tls.accept(tcp, peer_addr, SETUP_TIMEOUT, CLUSTER).await;
                    let waited = started.elapsed();
                    let Err(error) = result else {
                        panic!("a silent client cannot complete a handshake");
                    };
                    assert!(
                        matches!(
                            error.current_context(),
                            TransportError::ConnectionSetupTimeout { timeout, .. }
                                if *timeout == SETUP_TIMEOUT
                        ),
                        "{error:?}"
                    );
                    assert!(
                        waited >= SETUP_TIMEOUT && waited < SETUP_TIMEOUT + TICK,
                        "waited {waited:?}"
                    );
                    trace.record("server", outcome(error.current_context()));
                    Ok::<(), io::Error>(())
                })
                .await
            }
        });
        sim.client("client", async move {
            // Connect, then say nothing, holding the connection past the server's deadline.
            let tcp = turmoil::net::TcpStream::connect(("server", PORT)).await?;
            sleep_until(Instant::now() + Duration::from_secs(10)).await;
            drop(tcp);
            Ok(())
        });
    });
    assert!(result.is_ok(), "{result:?}");
    let events = trace.events();
    let [timed_out] = events.as_slice() else {
        panic!("expected one timeout:\n{}", trace.render());
    };
    assert!(
        timed_out.at >= SETUP_TIMEOUT && timed_out.at < SETUP_TIMEOUT + Duration::from_secs(1),
        "{}",
        trace.render()
    );
}

#[test]
fn certificate_clock_without_a_simulated_host_has_no_time() {
    // Outside a simulated host the simulated clock has no reading, and the transport refuses to
    // judge a certificate rather than consult the host's wall clock.
    let authority = ClusterAuthority::new();
    let tls = authority.issue(
        "server",
        "server",
        Validity {
            not_before: 0,
            not_after: 100,
        },
        ClockSkew::Exact,
    );
    let result = tls.clock.ensure_current(&tls.certificate);
    let Err(error) = result else {
        panic!("a clock without a reading cannot judge a certificate");
    };
    assert!(
        matches!(
            error.current_context(),
            crate::TlsConfigError::ClockUnavailable
        ),
        "{error:?}"
    );
}

#[test]
fn a_server_certificate_for_another_node_is_rejected() {
    let authority = StdArc::new(ClusterAuthority::new());
    let trace = SemanticTrace::default();
    let server_trace = trace.clone();
    let client_trace = trace.clone();
    let server_authority = StdArc::clone(&authority);
    let result = simulation(17, Duration::from_secs(5)).run("unexpected node", move |sim| {
        sim.host("server", move || {
            let tls = server_authority.issue(
                "server",
                "server",
                Validity {
                    not_before: 0,
                    not_after: 100,
                },
                ClockSkew::Exact,
            );
            let trace = server_trace.clone();
            async move { HostSupervisor::run(serve(tls, trace)).await }
        });
        sim.client("client", async move {
            let tls = authority.issue(
                "client",
                "client",
                Validity {
                    not_before: 0,
                    not_after: 100,
                },
                ClockSkew::Exact,
            );
            let error = dial_expecting(&tls, &client_trace, "elsewhere").await;
            assert!(
                matches!(error, Some(TransportError::InvalidHandshake(_))),
                "{error:?}"
            );
            Ok(())
        });
    });
    assert!(result.is_ok(), "{result:?}");
    let rendered = trace.render();
    let client_decisions = trace
        .events()
        .into_iter()
        .filter(|event| event.host == "client")
        .map(|event| event.event)
        .collect::<Vec<_>>();
    assert_eq!(
        client_decisions,
        [
            "rejected: peer handshake is invalid: peer certificate identifies node 'server', \
             expected 'elsewhere'"
        ]
        .map(str::to_string),
        "{rendered}"
    );
}

#[test]
fn outcome_names_the_failure_without_payload() {
    let eof = TransportError::Io(io::ErrorKind::UnexpectedEof.into());
    assert_eq!(outcome(&eof), "io: UnexpectedEof");
    let shutdown = TransportError::ShuttingDown;
    assert_eq!(outcome(&shutdown), "rejected: transport is shutting down");
}

#[test]
fn semantic_trace_replays_within_one_process() {
    let first = validity_window_trace(21).render();
    let second = validity_window_trace(21).render();
    assert_eq!(first, second);
    let other_seed = validity_window_trace(22).render();
    assert_ne!(
        first, other_seed,
        "the seed must reach the process epoch and link latency"
    );
}

/// Records one replay trace for [`semantic_trace_replays_in_fresh_processes`]. It runs only when
/// that test starts it in a fresh process with the output path in the environment.
#[test]
#[ignore = "started in a fresh process by semantic_trace_replays_in_fresh_processes"]
fn replay_trace_in_fresh_process() {
    let path = std::env::var_os(REPLAY_TRACE_VARIABLE)
        .verified("the parent test sets the trace path before starting this process");
    std::fs::write(path, validity_window_trace(21).render())
        .assured("the parent test created a writable trace directory");
}

#[test]
fn semantic_trace_replays_in_fresh_processes() {
    let executable = std::env::current_exe().assured("the running test binary has a path");
    let directory = tempfile::tempdir().assured("the test creates a trace directory");
    let mut traces = Vec::new();
    for run in 0..2 {
        let path: PathBuf = directory.path().join(format!("trace-{run}.txt"));
        let status = Command::new(&executable)
            .args([
                "authentication::simulation_tests::replay_trace_in_fresh_process",
                "--exact",
                "--ignored",
                "--test-threads=1",
            ])
            .env(REPLAY_TRACE_VARIABLE, &path)
            .status()
            .assured("the test binary starts again");
        assert!(status.success(), "fresh replay process {run} failed");
        traces.push(std::fs::read_to_string(&path).assured("the fresh process wrote its trace"));
    }
    assert!(!traces[0].is_empty());
    assert_eq!(traces[0], traces[1]);
    assert_eq!(traces[0], validity_window_trace(21).render());
}

#[test]
fn clock_skew_behind_the_unix_epoch_has_no_time() {
    // A clock skewed before the Unix epoch has no representable reading.
    let result = simulation(3, Duration::from_secs(1)).run("skew underflow", |sim| {
        sim.client("client", async {
            let clock = SimulatedUtc::new(ClockSkew::Behind(Duration::from_secs(
                EPOCH_UNIX_SECONDS + 1,
            )));
            assert!(rustls::time_provider::TimeProvider::current_time(&clock).is_none());
            Ok(())
        });
    });
    assert!(result.is_ok(), "{result:?}");
}
