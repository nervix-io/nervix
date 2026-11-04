//! Public scenarios for provisioned listener TLS material and advertised console schemes.
//!
//! Outside the product layer order.
//! - **Owns.** Configuring TLS fixtures and observing the real native and browser boundaries.
//! - **Depends on.** The shared cluster fixture, CLI, native client and browser harness.
//! - **Must not know.** How the server loads or publishes its TLS configuration.

use std::{io, net::SocketAddr, time::Duration};

use cucumber::{given, then, when};
use nervix_primitives::{net::TcpStream, sync::StdArc, time::timeout};
use nervix_server::application::InternalTransportMode;
use playwright_rs::Playwright;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use rustls_pki_types::pem::PemObject;
use tokio_rustls::TlsConnector;

use super::{
    ScenarioWorld, chromium_launch_options, close_browser, expand_placeholders, workspace_root,
};

/// Observe the actual presented leaf, with CA and endpoint-name verification still enabled.
async fn native_peer_certificate(endpoint: &str) -> io::Result<Vec<u8>> {
    let endpoint = url::Url::parse(endpoint).map_err(io::Error::other)?;
    let host = endpoint
        .host_str()
        .ok_or_else(|| io::Error::other("fixture endpoint has no host"))?;
    let port = endpoint
        .port()
        .ok_or_else(|| io::Error::other("fixture endpoint has no port"))?;
    let address = SocketAddr::new(host.parse().map_err(io::Error::other)?, port);
    let server_name = ServerName::try_from(host.to_owned()).map_err(io::Error::other)?;
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(workspace_root().join("tls/dev/ca.pem"))
        .map_err(io::Error::other)?
    {
        roots
            .add(certificate.map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
    }
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let stream = timeout(Duration::from_secs(30), async {
        let tcp = TcpStream::connect(address).await?;
        TlsConnector::from(StdArc::new(config))
            .connect(server_name, tcp)
            .await
    })
    .await
    .map_err(io::Error::other)??;
    let chain = stream
        .get_ref()
        .1
        .peer_certificates()
        .ok_or_else(|| io::Error::other("native listener presented no certificate chain"))?;
    let certificate = chain
        .first()
        .ok_or_else(|| io::Error::other("native listener presented an empty certificate chain"))?;
    Ok(certificate.as_ref().to_vec())
}

async fn assert_operator_certificate(
    world: &ScenarioWorld,
    node: &str,
    certificate: &str,
) -> io::Result<()> {
    let expected = CertificateDer::from_pem_file(
        workspace_root()
            .join("tls/dev")
            .join(format!("{certificate}.pem")),
    )
    .map_err(io::Error::other)?;
    let endpoint = world.cluster().grpc_uri(node)?;
    let actual = native_peer_certificate(&endpoint).await?;
    assert_eq!(
        actual.as_slice(),
        expected.as_ref(),
        "{node} must present the supplied operator certificate"
    );
    Ok(())
}

#[then(expr = "every native listener presents the operator certificate {string}")]
async fn then_operator_certificate_is_presented(
    world: &mut ScenarioWorld,
    certificate: String,
) -> io::Result<()> {
    for node in world.cluster().node_ids() {
        assert_operator_certificate(world, &node, &certificate).await?;
    }
    Ok(())
}

#[then(expr = "the native listener on node {string} presents the operator certificate {string}")]
async fn then_node_presents_operator_certificate(
    world: &mut ScenarioWorld,
    node: String,
    certificate: String,
) -> io::Result<()> {
    let node = expand_placeholders(world, &node);
    assert_operator_certificate(world, &node, &certificate).await
}

#[when(expr = "the native TLS files on node {string} are replaced with certificate {string}")]
fn when_native_tls_files_are_replaced(
    world: &mut ScenarioWorld,
    node: String,
    certificate: String,
) -> io::Result<()> {
    let node = expand_placeholders(world, &node);
    let directory = workspace_root().join("tls/dev");
    let chain = std::fs::read(directory.join(format!("{certificate}.pem")))?;
    let key = std::fs::read(directory.join(format!("{certificate}-key.pem")))?;
    world
        .cluster()
        .replace_native_tls_material(&node, &chain, &key)
}

#[then(expr = "the Rust client lists domain {string} through native listener {string}")]
async fn then_rust_client_lists_domain(
    world: &mut ScenarioWorld,
    domain: String,
    node: String,
) -> io::Result<()> {
    let domain = expand_placeholders(world, &domain);
    let node = expand_placeholders(world, &node);
    let output = world
        .cluster()
        .run_command(&node, &domain, "LIST DOMAINS;")
        .await?;
    assert!(
        output.contains(&domain),
        "{node} did not list {domain}: {output}"
    );
    Ok(())
}

#[given("the web console listener is configured for HTTPS")]
fn given_https_console(world: &mut ScenarioWorld) {
    world.cluster_config.web_console_mode = InternalTransportMode::Https;
}

#[when(expr = "the HTTPS web console is opened on node {string}")]
async fn when_https_console_opens(world: &mut ScenarioWorld, node: String) -> io::Result<()> {
    close_browser(world).await;
    let node = expand_placeholders(world, &node);
    let url = world.cluster().web_console_url(&node)?;
    assert!(
        url.starts_with("https://"),
        "the fixture must exercise HTTPS"
    );
    let playwright = Playwright::launch().await.map_err(io::Error::other)?;
    // This fresh browser observes redirect schemes with a per-cluster test CA. Keep certificate
    // verification in the native and CLI scenarios; disable it only for this browser process.
    let mut launch = chromium_launch_options();
    launch
        .args
        .get_or_insert_with(Vec::new)
        .push("--ignore-certificate-errors".to_string());
    let browser = playwright
        .chromium()
        .launch_with_options(launch)
        .await
        .map_err(io::Error::other)?;
    let context = browser.new_context().await.map_err(io::Error::other)?;
    let page = context.new_page().await.map_err(io::Error::other)?;
    page.set_default_timeout(10_000.0).await;
    super::web_console_connections::record_console_connections(
        &page,
        super::web_console_connections::InitialSession::Deliver,
    )
    .await
    .map_err(io::Error::other)?;
    world.browser_page = Some(page.clone());
    world.browser_context = Some(context);
    world.browser = Some(browser);
    world.playwright = Some(playwright);
    page.goto(&url, None).await.map_err(io::Error::other)?;
    Ok(())
}

#[then(expr = "the console session reaches node {string} over WSS")]
async fn then_console_uses_wss(world: &mut ScenarioWorld, node: String) -> io::Result<()> {
    let node = expand_placeholders(world, &node);
    let mut endpoint =
        url::Url::parse(&world.cluster().web_console_url(&node)?).map_err(io::Error::other)?;
    assert_eq!(
        endpoint.scheme(),
        "https",
        "this scenario uses an HTTPS console"
    );
    endpoint
        .set_scheme("wss")
        .map_err(|()| io::Error::other("the HTTPS endpoint must admit WSS"))?;
    let origin = serde_json::to_string(&endpoint.origin().ascii_serialization())
        .map_err(io::Error::other)?;
    let script = r#"() => {
        const expected = __EXPECTED_ORIGIN__;
        const connections = window.__nervixConsoleConnections.record().connections;
        return connections.some(connection =>
            connection.address === expected && connection.path === '/console/ws' &&
            connection.connected !== null) ? 'OK' :
            JSON.stringify(connections.map(({address, path, connected}) => ({address, path, connected})));
    }"#.replace("__EXPECTED_ORIGIN__", &origin);
    let page = world
        .browser_page
        .as_ref()
        .ok_or_else(|| io::Error::other("the HTTPS console page is not open"))?;
    super::assert_graph_probe(page, &script, "the leader's session connection over WSS").await;
    Ok(())
}
