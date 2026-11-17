//! Public listener admission and deadline regressions.
//!
//! Outside the layer order: a harness module.
//! - **Owns.** Requests that exhaust a listener's declared capacity or leave its input incomplete.
//! - **Depends on.** Public sockets, HTTP clients, the scenario cluster and the accept-failure seam.
//! - **Must not know.** The listener's admission implementation or executor internals.

#![cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "drives listener bounds through public connections")
)]

use std::{io, time::Duration};

use cucumber::then;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_approx_into::CheckedApproxInto as _;
use nervix_models::parse_duration_text;
use nervix_primitives::{
    net::TcpStream,
    sync::{mpsc, watch},
    task::AbortOnDropHandle,
    time::{Instant, timeout},
};
use nervix_recovery::Discarded as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use crate::{ScenarioWorld, common::cluster::NodeListener, expand_placeholders};

const WAIT: Duration = Duration::from_secs(30);

struct ProxyConnector {
    client: Option<tokio::io::DuplexStream>,
}

impl tower_service::Service<http::Uri> for ProxyConnector {
    type Response = hyper_util::rt::TokioIo<tokio::io::DuplexStream>;
    type Error = io::Error;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Uri) -> Self::Future {
        let client = self
            .client
            .take()
            .ok_or_else(|| io::Error::other("the fixture opens one connection"));
        std::future::ready(client.map(hyper_util::rt::TokioIo::new))
    }
}

#[then(expr = "node {string} releases a gRPC session whose peer stops reading within {string}")]
async fn half_open_grpc(world: &mut ScenarioWorld, node: String, wait: String) {
    let wait = parse_duration_text(&wait).assured("a valid duration");
    let labels = ["transport=\"grpc\"".to_string()];
    let count: i64 = world
        .cluster()
        .read_observability_metric(&node, "nervix_sessions", &labels)
        .await
        .assured("the node reports its sessions")
        .checked_approx_into()
        .assured("a session gauge is an integer");
    let server = world
        .cluster()
        .grpc_uri(&node)
        .assured("the node has a gRPC endpoint");
    let socket = TcpStream::connect(address(world, &node, "grpc"))
        .await
        .assured("the gRPC listener accepts TCP");
    let (mut server_read, mut server_write) = socket.into_split();
    let (client, proxy) = tokio::io::duplex(65536);
    let (mut client_read, mut client_write) = tokio::io::split(proxy);
    let (paused, mut pause) = watch::channel(false);
    let writer = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
        tokio::io::copy(&mut client_read, &mut server_write)
            .await
            .discarded("the proxy ends with its test client");
    }));
    let reader = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
        let mut bytes = [0; 8192];
        loop {
            nervix_primitives::task::consume_budget().await;
            if *pause.borrow_and_update() {
                if pause.changed().await.is_err() {
                    return;
                }
                continue;
            }
            let read = nervix_primitives::select! {
                biased;
                change = pause.changed() => { if change.is_err() { return; } continue; },
                read = server_read.read(&mut bytes) => read,
            };
            let read = match read {
                Ok(0) | Err(_) => return,
                Ok(read) => read,
            };
            if client_write.write_all(&bytes[..read]).await.is_err() {
                return;
            }
        }
    }));
    let connector = ProxyConnector {
        client: Some(client),
    };
    let channel = tonic::transport::Endpoint::from_shared(server)
        .assured("a valid gRPC URI")
        .connect_with_connector(connector)
        .await
        .assured("the proxy connects its HTTP/2 client");
    let session = crate::common::raw_session::open_session_on_channel(
        channel,
        &world.domain,
        &crate::common::cluster::test_basic_authorization(),
    )
    .await
    .assured("the public exchange opens")
    .assured("the fixture credentials authenticate");
    world
        .cluster()
        .wait_for_observability_metric_value(
            &node,
            "nervix_sessions",
            &labels,
            count.checked_add(1).assured("one fixture session fits i64"),
            Some(wait),
        )
        .await
        .assured("the new session is observable");
    // Responsive transports survive several keepalive periods before the proxy withholds input.
    nervix_primitives::time::sleep(Duration::from_secs(3)).await;
    let healthy = world
        .cluster()
        .read_observability_metric(&node, "nervix_sessions", &labels)
        .await
        .assured("the responsive session remains established");
    assert_eq!(
        healthy,
        f64::from(
            i32::try_from(count.checked_add(1).assured("one session fits"))
                .assured("fixture session count fits i32")
        )
    );
    let start = Instant::now();
    paused
        .send(true)
        .assured("the proxy retains its pause receiver");
    world
        .cluster()
        .wait_for_observability_metric_value(&node, "nervix_sessions", &labels, count, Some(wait))
        .await
        .assured("the server reaps the peer without a TCP close or client cancellation");
    // A ping may already be in flight when the proxy pauses. The configured 500 ms ping
    // interval and 2 s acknowledgement budget allow 3 s of observation/scheduling jitter.
    // The connection and client remain alive throughout this measured transport expiry.
    let maximum = Duration::from_millis(5500).min(wait);
    assert!(
        start.elapsed() <= maximum,
        "the keepalive bounds must release the session within {maximum:?}, observed {:?}",
        start.elapsed()
    );
    drop(session);
    drop(reader);
    drop(writer);
}

fn address(world: &ScenarioWorld, node: &str, listener: &str) -> std::net::SocketAddr {
    world
        .cluster()
        .public_listener_addr(
            node,
            listener.parse().assured("the scenario names a listener"),
        )
        .assured("the node exposes its listener")
}

#[then(
    expr = "the {word} listener on node {string} waits at least {string} after an accept failure"
)]
async fn accept_pause(world: &mut ScenarioWorld, listener: String, node: String, pause: String) {
    let address = address(world, &node, &listener);
    let pause = parse_duration_text(&pause).assured("a valid duration");
    let client = reqwest::Client::new();
    world.fault_injection.fail_next_listener_accept(address);
    let start = timeout(WAIT, world.fault_injection.listener_accept_failure(address))
        .await
        .assured("the armed accept failure fires");
    let response = client
        .get(format!("http://{address}/"))
        .timeout(WAIT)
        .send()
        .await
        .assured("the listener resumes and responds");
    assert_ne!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        start.elapsed() >= pause,
        "the listener resumed after {:?}, before its {pause:?} pause",
        start.elapsed()
    );
}

#[then(expr = "the {word} listener on node {string} refuses unfinished headers after {string}")]
async fn unfinished_headers(
    world: &mut ScenarioWorld,
    listener: String,
    node: String,
    delay: String,
) {
    let address = address(world, &node, &listener);
    let delay = parse_duration_text(&delay).assured("a valid duration");
    let start = Instant::now();
    let mut stream = TcpStream::connect(address)
        .await
        .assured("the listener accepts TCP");
    // An intentionally unfinished HTTP fixture: Hyper must own the timeout and response.
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: ")
        .await
        .assured("the peer receives the partial headers");
    let mut bytes = [0; 1024];
    let read = timeout(WAIT, stream.read(&mut bytes))
        .await
        .assured("unfinished headers have a bounded lifetime");
    assert!(start.elapsed() >= delay, "the header timeout fired early");
    assert!(
        start.elapsed() < delay.saturating_add(Duration::from_secs(3)),
        "the configured header deadline was ignored"
    );
    match read {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
            ) => {}
        other => {
            panic!("Hyper must close the incomplete request at its header deadline: {other:?}")
        }
    }
}

#[then(
    expr = "the {word} listener on node {string} admits two connections and refuses a third with \
            status 503"
)]
async fn connection_capacity(world: &mut ScenarioWorld, listener: String, node: String) {
    let address = address(world, &node, &listener);
    let path = match listener.parse::<NodeListener>().assured("a valid listener") {
        NodeListener::Observability => "/livez",
        NodeListener::Console => "/console/console.css",
        _ => panic!("this scenario selects an HTTP information listener"),
    };
    let url = format!("http://{address}{path}");
    let first = reqwest::Client::new();
    let second = reqwest::Client::new();
    for client in [&first, &second] {
        let response = client
            .get(&url)
            .timeout(WAIT)
            .send()
            .await
            .assured("a connection below capacity responds");
        assert_eq!(response.status(), 200);
        response
            .bytes()
            .await
            .assured("the response returns its socket to its pool");
    }
    let response = reqwest::Client::new()
        .get(&url)
        .timeout(WAIT)
        .send()
        .await
        .assured("a full listener returns its refusal");
    assert_eq!(response.status(), 503);
    assert_eq!(
        response
            .text()
            .await
            .assured("the typed refusal is readable"),
        "listener_connection_capacity\n"
    );
    // Keep the refusal parser waiting for a head while admitted connections return capacity.
    // A bounded refusal must not serialize the listener's next healthy admission behind it.
    let mut excess = TcpStream::connect(address)
        .await
        .assured("the excess peer reaches the listener's backlog");
    excess
        .write_all(b"GET / HTTP/1.1\r\nHost: ")
        .await
        .assured("the excess peer leaves its HTTP head unfinished");
    let mut overflow = TcpStream::connect(address)
        .await
        .assured("a further excess peer reaches the listener");
    let mut bytes = [0; 1];
    match timeout(Duration::from_secs(2), overflow.read(&mut bytes))
        .await
        .assured("an occupied refusal slot never queues another excess connection")
    {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
            ) => {}
        result => {
            panic!("a further excess connection must close before protocol admission: {result:?}")
        }
    }
    drop(overflow);
    drop(first);
    drop(second);
    timeout(Duration::from_secs(2), async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let response = reqwest::Client::new().get(&url).timeout(WAIT).send().await;
            match response {
                Ok(response) => {
                    if response.status() == 200 {
                        break;
                    }
                    assert_eq!(response.status(), 503);
                }
                Err(error) => {
                    assert!(
                        error.is_connect() || error.is_request(),
                        "only an excess socket's close is retryable: {error:#}"
                    );
                }
            }
            nervix_primitives::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .assured("capacity returns promptly even while an excess peer stalls its refusal");
    drop(excess);
}

#[then(
    expr = "node {string} refuses a {int} byte HTTP payload for host {string} with status {int} \
            using {word} framing"
)]
async fn oversize_body(
    world: &mut ScenarioWorld,
    node: String,
    size: usize,
    host: String,
    status: u16,
    framing: String,
) {
    let host = expand_placeholders(world, &host);
    let address = address(world, &node, "http");
    let payload = vec![b'x'; size];
    let body = match framing.as_str() {
        "length" => reqwest::Body::from(payload),
        "chunked" => {
            reqwest::Body::wrap_stream(futures_util::stream::iter([Ok::<_, io::Error>(payload)]))
        }
        "stalled" => reqwest::Body::wrap_stream(futures_util::StreamExt::chain(
            futures_util::stream::iter([Ok::<_, io::Error>(vec![b'{'])]),
            futures_util::stream::pending(),
        )),
        _ => panic!("the scenario declares its framing"),
    };
    let metric = "nervix_execution_memory_reserved_bytes";
    let labels = ["class=\"relay\"".to_string()];
    let before = world
        .cluster()
        .read_observability_metric(&node, metric, &labels)
        .await
        .assured("the node reports its retained relay memory");
    let start = Instant::now();
    let response = reqwest::Client::new()
        .post(format!("http://{address}/ingest"))
        .header("host", host)
        .body(body)
        .timeout(WAIT)
        .send()
        .await
        .assured("the listener answers the oversize request");
    assert_eq!(response.status(), status);
    if framing == "stalled" {
        assert!(
            start.elapsed() >= Duration::from_millis(500),
            "the body deadline fired early"
        );
    }
    let after = world
        .cluster()
        .read_observability_metric(&node, metric, &labels)
        .await
        .assured("the node reports its retained relay memory after refusal");
    assert_eq!(before, after, "refused HTTP input retains no body memory");
    let intake = world
        .cluster()
        .read_observability_metric(&node, metric, &["class=\"intake\"".to_string()])
        .await
        .assured("the node reports retained input memory");
    assert_eq!(
        intake, 0.0,
        "refused input returns its independent reservation"
    );
}

async fn closed_after(mut stream: TcpStream, delay: Duration, start: Instant) {
    let mut byte = [0];
    let result = timeout(WAIT, stream.read(&mut byte))
        .await
        .assured("the peer's idle lifetime is bounded");
    assert!(
        start.elapsed() >= delay,
        "the peer closed before its declared timeout"
    );
    assert!(
        start.elapsed() < delay.saturating_add(Duration::from_secs(3)),
        "the configured idle or TLS deadline was ignored"
    );
    match result {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
            ) => {}
        other => panic!("the peer must close without receiving input: {other:?}"),
    }
}

#[then(expr = "the {word} listener on node {string} closes an idle connection after {string}")]
async fn idle_connection(world: &mut ScenarioWorld, listener: String, node: String, delay: String) {
    let start = Instant::now();
    let stream = TcpStream::connect(address(world, &node, &listener))
        .await
        .assured("a fresh connection is accepted");
    closed_after(
        stream,
        parse_duration_text(&delay).assured("a valid duration"),
        start,
    )
    .await;
}

#[then(expr = "node {string} closes an unfinished console TLS handshake after {string}")]
async fn tls_handshake(world: &mut ScenarioWorld, node: String, delay: String) {
    let url = world
        .cluster()
        .web_console_url(&node)
        .assured("the node has a console URL");
    let url = url::Url::parse(&url).assured("the console URL is valid");
    assert_eq!(url.scheme(), "https");
    let host = url.host_str().assured("the fixture URL has a host");
    let port = url.port_or_known_default().assured("HTTPS has a port");
    let address = std::net::SocketAddr::new(
        host.parse()
            .assured("the fixture host is a numeric address"),
        port,
    );
    let start = Instant::now();
    let stream = TcpStream::connect(address)
        .await
        .assured("the TLS listener accepts TCP");
    closed_after(
        stream,
        parse_duration_text(&delay).assured("a valid duration"),
        start,
    )
    .await;
}

#[then(expr = "the {word} listener on node {string} refuses {int} header bytes with status 431")]
async fn oversize_headers(world: &mut ScenarioWorld, listener: String, node: String, bytes: usize) {
    let address = address(world, &node, &listener);
    let response = reqwest::Client::new()
        .get(format!("http://{address}/"))
        .header("x-large", "x".repeat(bytes))
        .timeout(WAIT)
        .send()
        .await
        .assured("the listener answers excessive header input");
    assert_eq!(response.status(), 431);
}

#[then(expr = "node {string} closes an unfinished gRPC TLS handshake after {string}")]
async fn grpc_tls_handshake(world: &mut ScenarioWorld, node: String, delay: String) {
    let start = Instant::now();
    let stream = TcpStream::connect(address(world, &node, "grpc"))
        .await
        .assured("the native TLS listener accepts TCP");
    closed_after(
        stream,
        parse_duration_text(&delay).assured("a valid duration"),
        start,
    )
    .await;
}

async fn input_memory_above(world: &ScenarioWorld, node: &str, threshold: f64) {
    timeout(WAIT, async {
        loop {
            nervix_primitives::task::consume_budget().await;
            let retained = world
                .cluster()
                .read_observability_metric(
                    node,
                    "nervix_execution_memory_reserved_bytes",
                    &["class=\"intake\"".to_string()],
                )
                .await
                .assured("the input memory gauge is public");
            if retained > threshold {
                return;
            }
        }
    })
    .await
    .assured("the open request retains its declared input charge");
}

#[then(expr = "node {string} refuses retained HTTP input growth for host {string} with status 503")]
async fn refused_input_growth(world: &mut ScenarioWorld, node: String, host: String) {
    let host = expand_placeholders(world, &host);
    let address = address(world, &node, "http");
    let mut held = TcpStream::connect(address)
        .await
        .assured("the first request connects");
    held.write_all(
        format!("POST /ingest HTTP/1.1\r\nHost: {host}\r\nContent-Length: 1\r\n\r\n").as_bytes(),
    )
    .await
    .assured("the first body stays incomplete");
    input_memory_above(world, &node, 0.0).await;
    let (payloads, body) = mpsc::channel::<Result<Vec<u8>, io::Error>>(1);
    let client = reqwest::Client::new();
    let request = AbortOnDropHandle::new(nervix_primitives::task::spawn(async move {
        client
            .post(format!("http://{address}/ingest"))
            .header("host", host)
            .body(reqwest::Body::wrap_stream(
                nervix_primitives::stream::wrappers::ReceiverStream::new(body),
            ))
            .timeout(WAIT)
            .send()
            .await
    }));
    // In the configured 4 MiB pool, two incomplete requests retain more than 3 MiB.
    // The payload is below its 1 MiB cap but exceeds the remaining memory, independently
    // of packetization. Keep its stream open so assembly and peer timeout cannot answer it.
    input_memory_above(world, &node, 3_145_728.0).await;
    payloads
        .send(Ok(vec![b'x'; 1_048_575]))
        .await
        .assured("the bounded fixture publishes one payload");
    let response = timeout(WAIT, request)
        .await
        .assured("memory refusal ends the request")
        .assured("the fixture request task completes")
        .assured("HTTP preserves the refusal response");
    assert_eq!(response.status(), 503);
    assert_eq!(
        response
            .text()
            .await
            .assured("the typed reason is readable"),
        "http_input_memory_capacity"
    );
    drop(payloads);
    drop(held);
    world
        .cluster()
        .wait_for_observability_metric_value(
            &node,
            "nervix_execution_memory_reserved_bytes",
            &["class=\"intake\"".to_string()],
            0,
            Some(WAIT),
        )
        .await
        .assured("both cancelled inputs return their entire reservation");
}

#[then(expr = "node {string} admits an ordinary fragmented HTTP body for host {string}")]
async fn admitted_fragmented_body(world: &mut ScenarioWorld, node: String, host: String) {
    use futures_util::StreamExt as _;

    let address = address(world, &node, "http");
    let host = expand_placeholders(world, &host);
    let payload = serde_json::to_vec(&serde_json::json!({"value": "x".repeat(768 * 1024)}))
        .assured("the bounded JSON fixture encodes");
    let chunks: Vec<_> = payload.chunks(4096).map(<[u8]>::to_vec).collect();
    let body = futures_util::stream::iter(chunks).then(|chunk| async move {
        nervix_primitives::time::sleep(Duration::from_millis(5)).await;
        Ok::<_, io::Error>(chunk)
    });
    let response = reqwest::Client::new()
        .post(format!("http://{address}/ingest"))
        .header("host", host)
        .body(reqwest::Body::wrap_stream(body))
        .timeout(WAIT)
        .send()
        .await
        .assured("ordinary fragmented input receives a response");
    let status = response.status();
    let reason = response.text().await.assured("the outcome is readable");
    assert_eq!(
        status, 202,
        "a valid body within the declared bounds: {reason}"
    );
    world
        .cluster()
        .wait_for_observability_metric_value(
            &node,
            "nervix_execution_memory_reserved_bytes",
            &["class=\"intake\"".to_string()],
            0,
            Some(WAIT),
        )
        .await
        .assured("successful input returns its entire reservation");
}

#[then(expr = "node {string} refuses an oversized chunked trailer for host {string}")]
async fn oversized_trailer(world: &mut ScenarioWorld, node: String, host: String) {
    let host = expand_placeholders(world, &host);
    let mut stream = TcpStream::connect(address(world, &node, "http"))
        .await
        .assured("the HTTP listener accepts the framing fixture");
    let payload = br#"{"value":"framed"}"#;
    let request = format!(
        "POST /ingest HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nTransfer-Encoding: \
         chunked\r\n\r\n{:x}\r\n{}\r\n0\r\nX-Bound: {}\r\n\r\n",
        payload.len(),
        std::str::from_utf8(payload).assured("the JSON fixture is UTF-8"),
        "x".repeat(32 * 1024),
    );
    let sent = timeout(WAIT, stream.write_all(request.as_bytes()))
        .await
        .assured("sending the bounded trailer finishes");
    if let Err(error) = sent {
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
            ),
            "sending the refused trailer: {error}"
        );
    }
    let mut response = Vec::new();
    let read = timeout(WAIT, stream.read_to_end(&mut response))
        .await
        .assured("invalid framing closes the connection");
    if let Err(error) = read {
        assert!(
            matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
            ),
            "reading the refusal: {error}"
        );
    }
    let response = std::str::from_utf8(&response).assured("the refusal is textual HTTP");
    assert!(response.starts_with("HTTP/1.1 400 "), "{response}");
    assert!(response.ends_with("http_body_invalid"), "{response}");
    world
        .cluster()
        .wait_for_observability_metric_value(
            &node,
            "nervix_execution_memory_reserved_bytes",
            &["class=\"intake\"".to_string()],
            0,
            Some(WAIT),
        )
        .await
        .assured("rejected trailers return the body reservation");
}
