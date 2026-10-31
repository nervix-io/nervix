//! Steps for the HTTP client transport contract: a forward proxy a client may declare or the
//! environment may name, and a real-process cluster whose environment names one.
//!
//! Layer: test harness.
//! - **Owns.** Starting named HTTP proxies, waiting for what each carried, asserting that one
//!   carried nothing, and starting a real-process cluster whose environment names a proxy.
//! - **Depends on.** The HTTP proxy fixture, the real-process cluster fixture and the scenario's
//!   placeholders.
//! - **Must not know.** What a node does with its proxy settings; scenarios observe that through
//!   the proxies and the receivers.

use cucumber::{given, then};

use super::*;
use crate::common::http_proxy::{ProxiedRequest, ProxyCredentials};

/// How long a step waits for a proxy to carry what a node sends through it. Generous, because a
/// wait for something to happen ends as soon as it does.
const HTTP_PROXY_WAIT: Duration = Duration::from_secs(60);

/// Every spelling of the proxy variables an HTTP library reads from the environment.
const PROXY_ENVIRONMENT_VARIABLES: [&str; 6] = [
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
];

/// Every spelling of the variable that exempts hosts from those proxies. Set empty, so a value the
/// scenario runner itself inherited cannot exempt the loopback receivers a scenario uses.
const PROXY_EXEMPTION_VARIABLES: [&str; 2] = ["NO_PROXY", "no_proxy"];

async fn start_http_proxy(
    world: &mut ScenarioWorld,
    name: String,
    credentials: Option<ProxyCredentials>,
) {
    initialize_scenario_identity(world);
    let name = expand_placeholders(world, &name);
    assert!(
        !world.http_proxies.contains_key(&name),
        "HTTP proxy '{name}' is already running"
    );
    let port = draw_scenario_port(world, "HTTP proxy");
    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let proxy = match HttpProxy::start(address, credentials).await {
        Ok(proxy) => proxy,
        Err(error) => panic!("HTTP proxy '{name}' failed to start: {error:?}"),
    };
    world
        .placeholders
        .insert(format!("http_proxy.{name}"), format!("http://{address}"));
    world
        .placeholders
        .insert(format!("http_proxy_port.{name}"), proxy.port().to_string());
    world.http_proxies.insert(name, proxy);
}

fn http_proxy<'world>(world: &'world ScenarioWorld, name: &str) -> &'world HttpProxy {
    let name = expand_placeholders(world, name);
    match world.http_proxies.get(&name) {
        Some(proxy) => proxy,
        None => panic!("HTTP proxy '{name}' is not running"),
    }
}

#[given(expr = "HTTP proxy {string} is running")]
async fn given_http_proxy_is_running(world: &mut ScenarioWorld, name: String) {
    start_http_proxy(world, name, None).await;
}

/// A proxy that answers `407` to every request whose `Proxy-Authorization` does not carry these
/// `Basic` credentials.
#[given(expr = "HTTP proxy {string} is running and requires user {string} with password {string}")]
async fn given_http_proxy_requiring_credentials_is_running(
    world: &mut ScenarioWorld,
    name: String,
    user: String,
    password: String,
) {
    let credentials = ProxyCredentials::basic(&user, &password);
    start_http_proxy(world, name, Some(credentials)).await;
}

/// The proxy forwarded a request in absolute form whose request line is exactly this one.
#[then(expr = "HTTP proxy {string} eventually forwards {string}")]
async fn then_http_proxy_eventually_forwards(
    world: &mut ScenarioWorld,
    name: String,
    request_line: String,
) {
    let request_line = expand_placeholders(world, &request_line);
    let expected = ProxiedRequest::Forward { request_line };
    if let Err(error) = http_proxy(world, &name)
        .wait_for(expected, HTTP_PROXY_WAIT)
        .await
    {
        panic!("HTTP proxy '{name}': {error}");
    }
}

/// The proxy opened a `CONNECT` tunnel to exactly this authority.
#[then(expr = "HTTP proxy {string} eventually tunnels to {string}")]
async fn then_http_proxy_eventually_tunnels_to(
    world: &mut ScenarioWorld,
    name: String,
    authority: String,
) {
    let authority = expand_placeholders(world, &authority);
    let expected = ProxiedRequest::Tunnel { authority };
    if let Err(error) = http_proxy(world, &name)
        .wait_for(expected, HTTP_PROXY_WAIT)
        .await
    {
        panic!("HTTP proxy '{name}': {error}");
    }
}

/// Nothing reached the proxy: no request, tunnel or refusal. Asserted after the requests a scenario
/// waits for have reached their receiver, so every one of them had its chance to go through it.
#[then(expr = "HTTP proxy {string} has carried no request")]
async fn then_http_proxy_has_carried_no_request(world: &mut ScenarioWorld, name: String) {
    let recorded = http_proxy(world, &name).recorded();
    assert!(
        recorded.is_empty(),
        "HTTP proxy '{name}' carried requests it should never have seen: {recorded:?}"
    );
}

/// A real-process cluster whose every member has each proxy variable set to the named proxy, as
/// an operator's shell or service unit might set them. A client that read them would send its
/// requests through that proxy, which records each one.
#[given(
    expr = "a {int} node nervix-server process cluster is started with proxy environment \
            variables naming HTTP proxy {string}"
)]
async fn given_server_process_cluster_with_proxy_environment_is_started(
    world: &mut ScenarioWorld,
    node_count: usize,
    proxy: String,
) {
    assert!(
        world.server_process_cluster.is_none(),
        "a scenario starts at most one real-process cluster"
    );
    initialize_scenario_identity(world);
    let proxy_url = expand_placeholders(world, &format!("{{{{http_proxy.{proxy}}}}}"));
    let mut options = Vec::new();
    for variable in PROXY_ENVIRONMENT_VARIABLES {
        options.push(ServerProcessOption::Environment {
            variable: variable.to_string(),
            value: proxy_url.clone(),
        });
    }
    for variable in PROXY_EXEMPTION_VARIABLES {
        options.push(ServerProcessOption::Environment {
            variable: variable.to_string(),
            value: String::new(),
        });
    }
    let started = ServerProcessCluster::start_sized(node_count, &options).await;
    world.server_process_cluster = Some(
        started.unwrap_or_else(|error| panic!("failed to start the real-process cluster: {error}")),
    );
}

/// An HTTPS receiver whose TLS offers HTTP/2 ahead of HTTP/1.1, so a client that offers HTTP/2
/// speaks it to the receiver.
#[given(
    regex = r#"^HTTPS receiver "([^"]*)" is running with a certificate for "([^"]*)" and offers HTTP/2$"#
)]
async fn given_https_receiver_offering_http2_is_running(
    world: &mut ScenarioWorld,
    name: String,
    hosts: String,
) {
    // The certificate's names may name the scenario, so its identity exists before they expand.
    initialize_scenario_identity(world);
    let options = ReceiverTlsOptions {
        certificate_hosts: certificate_hosts(world, &hosts),
        client_certificate: ClientCertificatePolicy::NotRequested,
        protocols: ReceiverProtocols::Http2AndHttp1,
    };
    start_http_receiver(
        world,
        name,
        ReceiverTransport::Tls(options),
        LengthPolicy::Lenient,
    )
    .await;
}

/// The receiver captured a request with exactly this request line, `<METHOD> <target>`, and its
/// client sent it over HTTP/2. Every request of one client on one route speaks the same version,
/// so the first such request settles it.
#[then(
    regex = r#"^HTTP receiver "([^"]*)" eventually captures a request with request line "([^"]*)" over HTTP/2$"#
)]
async fn then_http_receiver_eventually_captures_request_line_over_http2(
    world: &mut ScenarioWorld,
    name: String,
    request_line: String,
) {
    let request_line = expand_placeholders(world, &request_line);
    let waited = http_receiver(world, &name)
        .wait_for_request_line(&request_line, HTTP_PROXY_WAIT)
        .await;
    let captured = match waited {
        Ok(captured) => captured,
        Err(error) => panic!("HTTP receiver '{name}': {error}"),
    };
    let mut versions = Vec::new();
    for request in &captured {
        if request.request_line() == request_line {
            versions.push(request.version);
        }
    }
    assert!(
        versions.contains(&http::Version::HTTP_2),
        "HTTP receiver '{name}' captured '{request_line}' only over {versions:?}"
    );
}

/// The receiver captured at least one request with exactly this request line, `<METHOD> <target>`.
/// A scenario whose source keeps polling uses it where the number of such requests is not part of
/// the contract.
#[then(expr = "HTTP receiver {string} eventually captures a request with request line {string}")]
async fn then_http_receiver_eventually_captures_request_line(
    world: &mut ScenarioWorld,
    name: String,
    request_line: String,
) {
    let request_line = expand_placeholders(world, &request_line);
    let waited = http_receiver(world, &name)
        .wait_for_request_line(&request_line, HTTP_PROXY_WAIT)
        .await;
    if let Err(error) = waited {
        panic!("HTTP receiver '{name}': {error}");
    }
}
