//! Public observations of committed outward algorithms and retained TLS connections.
//!
//! Outside the layer order: scenario harness.
//! - **Owns.** A restricted independent peer and post-change application traffic.
//! - **Depends on.** Public listener URLs, real rustls handshakes and cluster commands.
//! - **Must not know.** Runtime policy publication, caches or connector internals.

use std::{io, net::SocketAddr, time::Duration};

use cucumber::{given, then};
use nervix_primitives::{net::TcpStream, sync::StdArc, time::timeout};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use rustls_pki_types::pem::PemObject as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio_rustls::{TlsConnector, client::TlsStream};

use super::ScenarioWorld;

const PEER_BUDGET: Duration = Duration::from_secs(10);

pub(super) struct RetainedConnection {
    stream: TlsStream<TcpStream>,
    native: bool,
}

fn restricted_client(native: bool, ca_pem: &[u8]) -> io::Result<ClientConfig> {
    let mut provider = nervix_crypto::tls::outward_tls_provider().rustls();
    provider
        .cipher_suites
        .retain(|suite| suite.suite() == rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256);
    provider
        .kx_groups
        .retain(|group| group.name() == rustls::NamedGroup::X25519);
    let mut roots = RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(ca_pem) {
        roots
            .add(certificate.map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
    }
    let mut config = ClientConfig::builder_with_provider(StdArc::new(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![if native {
        b"h2".to_vec()
    } else {
        b"http/1.1".to_vec()
    }];
    Ok(config)
}

async fn connect(
    endpoint: &str,
    native: bool,
    ca_pem: &[u8],
) -> io::Result<Result<TlsStream<TcpStream>, io::Error>> {
    let url = url::Url::parse(endpoint).map_err(io::Error::other)?;
    let host = url
        .host_str()
        .ok_or_else(|| io::Error::other("the fixture URL needs a host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| io::Error::other("the fixture URL needs a port"))?;
    let address = SocketAddr::new(host.parse().map_err(io::Error::other)?, port);
    connect_at(address, host, native, ca_pem).await
}

async fn connect_at(
    address: SocketAddr,
    host: &str,
    native: bool,
    ca_pem: &[u8],
) -> io::Result<Result<TlsStream<TcpStream>, io::Error>> {
    // A refused TCP connection or elapsed budget is a fixture failure, never a TLS-policy verdict.
    let tcp = timeout(PEER_BUDGET, TcpStream::connect(address))
        .await
        .map_err(io::Error::other)??;
    let name = ServerName::try_from(host.to_owned()).map_err(io::Error::other)?;
    timeout(
        PEER_BUDGET,
        TlsConnector::from(StdArc::new(restricted_client(native, ca_pem)?)).connect(name, tcp),
    )
    .await
    .map_err(io::Error::other)
}

fn listeners(world: &ScenarioWorld) -> io::Result<Vec<(String, bool, Vec<u8>)>> {
    let mut urls = Vec::new();
    for node in world.cluster().node_ids() {
        urls.push((
            world.cluster().grpc_uri(&node)?,
            true,
            super::common::cluster::dev_tls_ca_pem()?,
        ));
        urls.push((
            world.cluster().web_console_url(&node)?,
            false,
            world.cluster().web_console_ca_pem(&node)?,
        ));
    }
    Ok(urls)
}

#[then("every native and console listener refuses the ChaCha20 and X25519 peer")]
async fn restricted_peer_is_refused(world: &mut ScenarioWorld) -> io::Result<()> {
    for (url, native, ca) in listeners(world)? {
        let error = connect(&url, native, &ca)
            .await?
            .expect_err("fips must refuse the restricted algorithms");
        let source = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<rustls::Error>());
        assert!(
            matches!(
                source,
                Some(rustls::Error::AlertReceived(
                    rustls::AlertDescription::HandshakeFailure
                ))
            ),
            "{url}: expected the TLS handshake-failure alert, got {error:?}"
        );
    }
    Ok(())
}

#[then("every native and console listener accepts and retains the ChaCha20 and X25519 peer")]
async fn restricted_peer_is_accepted(world: &mut ScenarioWorld) -> io::Result<()> {
    assert!(world.outward_tls_retained.is_empty());
    for (url, native, ca) in listeners(world)? {
        let stream = connect(&url, native, &ca).await??;
        let session = stream.get_ref().1;
        assert_eq!(
            session.negotiated_cipher_suite().map(|suite| suite.suite()),
            Some(rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256)
        );
        assert_eq!(
            session
                .negotiated_key_exchange_group()
                .map(|group| group.name()),
            Some(rustls::NamedGroup::X25519)
        );
        world
            .outward_tls_retained
            .push(RetainedConnection { stream, native });
    }
    Ok(())
}

async fn post_change_traffic(connection: &mut RetainedConnection) -> io::Result<()> {
    if !connection.native {
        connection
            .stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;
        let mut response = [0; 12];
        connection.stream.read_exact(&mut response).await?;
        assert!(
            response.starts_with(b"HTTP/1.1 "),
            "the retained console connection carries a response"
        );
        return Ok(());
    }
    // A PING acknowledgement proves the server processed traffic sent after publication; an
    // already-buffered SETTINGS frame alone would not establish that property.
    connection.stream.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x06\x00\x00\x00\x00\x00policy!!").await?;
    for _ in 0..16 {
        let mut header = [0; 9];
        connection.stream.read_exact(&mut header).await?;
        let length =
            usize::from(header[0]) * 65536 + usize::from(header[1]) * 256 + usize::from(header[2]);
        assert!(
            length <= 65536,
            "the fixture bounds its received frame memory"
        );
        let mut payload = vec![0; length];
        connection.stream.read_exact(&mut payload).await?;
        if header[3] == 6 && header[4] & 1 == 1 {
            assert_eq!(payload, b"policy!!");
            return Ok(());
        }
    }
    Err(io::Error::other(
        "no PING acknowledgement in the bounded frame prefix",
    ))
}

#[then("the retained restricted TLS connections still carry application traffic")]
async fn retained_connections_keep_working(world: &mut ScenarioWorld) -> io::Result<()> {
    assert!(!world.outward_tls_retained.is_empty());
    for mut connection in world.outward_tls_retained.drain(..) {
        timeout(PEER_BUDGET, post_change_traffic(&mut connection))
            .await
            .map_err(io::Error::other)??;
    }
    Ok(())
}

#[given(expr = "HTTPS receiver {string} offers only ChaCha20 and X25519")]
async fn restricted_receiver(world: &mut ScenarioWorld, name: String) {
    use super::common::http_receiver::{
        ClientCertificatePolicy, LengthPolicy, ReceiverProtocols, ReceiverTlsAlgorithms,
        ReceiverTlsOptions, ReceiverTransport,
    };
    let options = ReceiverTlsOptions {
        algorithms: ReceiverTlsAlgorithms::ChaCha20AndX25519,
        certificate_hosts: vec!["127.0.0.1".to_owned()],
        client_certificate: ClientCertificatePolicy::NotRequested,
        protocols: ReceiverProtocols::Http1,
    };
    super::start_http_receiver(
        world,
        name,
        ReceiverTransport::Tls(options),
        LengthPolicy::Lenient,
    )
    .await;
}

#[then(expr = "HTTPS receiver {string} records a TLS algorithm refusal")]
async fn receiver_refuses_policy(world: &mut ScenarioWorld, name: String) {
    use super::common::http_receiver::ReceiverFault;
    super::http_receiver(world, &name)
        .wait_for_fault(
            "no cipher suite or key-exchange group in common",
            |fault| {
                let ReceiverFault::TlsHandshake { source, .. } = fault else {
                    return false;
                };
                matches!(
                    source
                        .get_ref()
                        .and_then(|e| e.downcast_ref::<rustls::Error>()),
                    Some(rustls::Error::PeerIncompatible(
                        rustls::PeerIncompatible::NoCipherSuitesInCommon
                            | rustls::PeerIncompatible::NoKxGroupsInCommon
                    ))
                )
            },
            super::HTTP_RECEIVER_WAIT,
        )
        .await
        .expect("the restricted peer must reject the retained connector under fips");
}

#[then(
    expr = "every HTTPS listener for host {string} {word} the restricted peer trusting resource \
            directory {string}"
)]
async fn https_policy(
    world: &mut ScenarioWorld,
    host: String,
    verdict: String,
    bundle: String,
) -> io::Result<()> {
    let host = super::expand_placeholders(world, &host);
    let ca = super::resource_directory_ca_pem(world, &bundle);
    for node in world.cluster().node_ids() {
        let result = connect_at(
            world.cluster().https_address(&node)?,
            &host,
            false,
            ca.as_bytes(),
        )
        .await?;
        match verdict.as_str() {
            "accepts" => {
                let mut stream = result?;
                assert_eq!(
                    stream
                        .get_ref()
                        .1
                        .negotiated_cipher_suite()
                        .map(|s| s.suite()),
                    Some(rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256)
                );
                let request =
                    format!("GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
                timeout(PEER_BUDGET, async {
                    stream.write_all(request.as_bytes()).await?;
                    let mut response = [0; 12];
                    stream.read_exact(&mut response).await?;
                    assert!(response.starts_with(b"HTTP/1.1 "));
                    Ok::<_, io::Error>(())
                })
                .await
                .map_err(io::Error::other)??;
            }
            "refuses" => {
                let error =
                    result.expect_err("the committed fips policy refuses the restricted peer");
                let cause = error
                    .get_ref()
                    .and_then(|e| e.downcast_ref::<rustls::Error>());
                assert!(
                    matches!(
                        cause,
                        Some(rustls::Error::AlertReceived(
                            rustls::AlertDescription::HandshakeFailure
                        ))
                    ),
                    "{node}: {error:?}"
                );
            }
            _ => panic!("the verdict is accepts or refuses"),
        }
    }
    Ok(())
}
