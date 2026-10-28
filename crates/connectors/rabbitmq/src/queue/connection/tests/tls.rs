//! The client identity an `amqps` connection presents, observed by local TLS listeners that stand
//! in for a broker: one requires a certificate its own authority issued, the other requests none.
//!
//! Each listener completes the TLS handshake, reads the AMQP protocol header Lapin sends and
//! answers with bytes that are not AMQP, so each connection ends in the AMQP handshake once the
//! listener has seen which certificates the client presented.

use nervix_primitives::sync::StdArc;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use rustls::{
    AlertDescription, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    server::WebPkiClientVerifier,
};
use tokio_rustls::TlsAcceptor;

use super::*;

/// The loopback address every stand-in TLS broker listens on, and the one its certificate names.
fn broker_address() -> IpAddr {
    loopback(20)
}

/// A certificate one authority issued, with its private key.
struct Issued {
    certificate: rcgen::Certificate,
    key: KeyPair,
}

impl Issued {
    /// A certificate for `name` that `authority` signs, usable for `purpose`.
    fn by(authority: &Issued, name: &str, purpose: ExtendedKeyUsagePurpose) -> Self {
        let mut params = CertificateParams::new(vec![name.to_string()])
            .assured("the subject is a valid DNS name or IP address");
        params.extended_key_usages = vec![purpose];
        let key = KeyPair::generate().assured("AWS-LC generates a key pair");
        let certificate = params
            .signed_by(&key, &authority.certificate, &authority.key)
            .assured("the authority signs a leaf it issued");
        Self { certificate, key }
    }

    fn der(&self) -> CertificateDer<'static> {
        self.certificate.der().clone()
    }
}

/// The files a client names as its identity, by their names in the authority's directory.
#[derive(Clone, Copy)]
struct IdentityFiles {
    certificate: &'static str,
    key: &'static str,
}

/// The identity [`Authority::issue`] writes for its client.
const CLIENT_IDENTITY: IdentityFiles = IdentityFiles {
    certificate: "client.pem",
    key: "client-key.pem",
};

/// Whether a stand-in broker asks the client for a certificate.
#[derive(Clone, Copy)]
enum ClientCertificates {
    /// The handshake fails unless the client presents a certificate the authority issued.
    Required,
    /// As `Required`, over TLS 1.2 only, where the broker refuses before the handshake completes.
    RequiredOverTls12,
    /// The broker asks for none, as a broker that authenticates its clients by password only.
    NotRequested,
}

/// A certificate authority, the broker certificate it issued for [`broker_address`] and a client
/// identity it issued, with the authority, the client certificate and its key written as the PEM
/// files a client names.
struct Authority {
    files: TempDir,
    authority: Issued,
    broker: Issued,
    client: Issued,
}

impl Authority {
    fn issue() -> Self {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let key = KeyPair::generate().assured("AWS-LC generates a key pair");
        let certificate = params
            .self_signed(&key)
            .assured("the authority's parameters are valid");
        let authority = Issued { certificate, key };
        let broker = Issued::by(
            &authority,
            &broker_address().to_string(),
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let client = Issued::by(
            &authority,
            "nervix-client",
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let files = tempfile::tempdir().assured("a temporary directory can be created");
        for (name, pem) in [
            ("ca.pem", authority.certificate.pem()),
            ("client.pem", client.certificate.pem()),
            ("client-key.pem", client.key.serialize_pem()),
        ] {
            std::fs::write(files.path().join(name), pem).assured("the PEM fixture can be written");
        }
        Self {
            files,
            authority,
            broker,
            client,
        }
    }

    fn file(&self, name: &str) -> String {
        self.files.path().join(name).display().to_string()
    }

    /// The client configuration of `addr` trusting this authority, presenting `identity` when it
    /// names one.
    fn entries(&self, addr: &str, identity: Option<IdentityFiles>) -> Vec<ClientConfigEntry> {
        let entry = |key: &str, value: String| ClientConfigEntry {
            key: key.to_string(),
            value,
        };
        let mut entries = vec![
            entry("addr", addr.to_string()),
            entry("tls_ca_file", self.file("ca.pem")),
        ];
        if let Some(identity) = identity {
            entries.push(entry("tls_cert_file", self.file(identity.certificate)));
            entries.push(entry("tls_key_file", self.file(identity.key)));
        }
        entries
    }

    /// The TLS configuration of a broker presenting the certificate this authority issued it.
    fn broker(&self, clients: ClientCertificates) -> StdArc<ServerConfig> {
        let builder = match clients {
            ClientCertificates::RequiredOverTls12 => {
                ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
            }
            ClientCertificates::Required | ClientCertificates::NotRequested => {
                ServerConfig::builder()
            }
        };
        let builder = match clients {
            ClientCertificates::Required | ClientCertificates::RequiredOverTls12 => {
                let mut roots = RootCertStore::empty();
                roots
                    .add(self.authority.der())
                    .assured("the authority's certificate is a valid trust anchor");
                let verifier = WebPkiClientVerifier::builder(StdArc::new(roots))
                    .build()
                    .assured("a verifier over one trust anchor builds");
                builder.with_client_cert_verifier(verifier)
            }
            ClientCertificates::NotRequested => builder.with_no_client_auth(),
        };
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.broker.key.serialize_der()));
        let config = builder
            .with_single_cert(vec![self.broker.der()], key)
            .assured("the broker key matches its certificate");
        StdArc::new(config)
    }
}

/// What a stand-in TLS broker saw of the one connection it accepted.
#[derive(Debug, PartialEq)]
enum BrokerSaw {
    /// The TLS handshake completed, with the certificates the client presented in it: none when the
    /// broker requested none.
    Session {
        client_certificates: Vec<CertificateDer<'static>>,
    },
    /// The TLS handshake did not complete.
    NoSession,
}

/// A listener standing in for an `amqps` broker: it completes a TLS handshake under `config`,
/// reads the AMQP protocol header, answers with bytes that are not AMQP, and then waits for the
/// client to close the connection.
fn tls_broker(listener: TcpListener, config: StdArc<ServerConfig>) -> JoinHandle<BrokerSaw> {
    nervix_primitives::task::spawn(async move {
        let (stream, _) = listener
            .accept()
            .await
            .assured("the client under test dials this listener");
        let accepted = TlsAcceptor::from(config).accept(stream).await;
        let Ok(mut stream) = accepted else {
            return BrokerSaw::NoSession;
        };
        let client_certificates = match stream.get_ref().1.peer_certificates() {
            Some(certificates) => certificates.to_vec(),
            None => Vec::new(),
        };
        let mut header = [0_u8; 8];
        stream
            .read_exact(&mut header)
            .await
            .assured("Lapin sends its protocol header before anything else");
        assert_eq!(header, AMQP_PROTOCOL_HEADER);
        stream
            .write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            .await
            .assured("the client is still connected while it waits for Connection.Start");
        let mut rest = Vec::new();
        timeout(CLOSE_DEADLINE, stream.read_to_end(&mut rest))
            .await
            .assured("a failed handshake closes the client's socket")
            .discarded(
                "the client shuts its socket down without a TLS close_notify, which ends the read \
                 either way",
            );
        BrokerSaw::Session {
            client_certificates,
        }
    })
}

/// The address of the stand-in broker on `port`.
fn amqps_address(port: u16) -> String {
    format!("amqps://guest:guest@{}:{port}/%2f", broker_address())
}

#[nervix_primitives::test]
async fn amqps_connections_present_the_client_identity_their_broker_requires() {
    nervix_connector::install_rustls_crypto_provider();
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let authority = Authority::issue();
    let listener = loopback_listener(broker_address()).await;
    let port = listener
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    let broker_side = tls_broker(listener, authority.broker(ClientCertificates::Required));

    let entries = authority.entries(&amqps_address(port), Some(CLIENT_IDENTITY));
    let broker = RabbitMqBroker::from_config(&entries, fixture.dns.clone())
        .assured("the client's identity files hold a certificate and its key");
    let connected = broker
        .connect_within(connect_deadline(GENEROUS_BUDGET))
        .await;

    let saw = broker_side
        .await
        .assured("the stand-in broker does not panic");
    assert_eq!(
        saw,
        BrokerSaw::Session {
            client_certificates: vec![authority.client.der()],
        },
        "the broker must see the client certificate the client configuration names"
    );
    let Err(report) = connected else {
        panic!("a listener that does not speak AMQP cannot complete the handshake");
    };
    assert!(
        matches!(
            report.current_context(),
            RabbitMqConnectError::AmqpHandshake { host } if *host == broker_address().to_string()
        ),
        "{report:?}"
    );
}

#[nervix_primitives::test]
async fn amqps_connections_without_an_identity_present_no_certificate() {
    nervix_connector::install_rustls_crypto_provider();
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let authority = Authority::issue();
    let listener = loopback_listener(broker_address()).await;
    let port = listener
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    let broker_side = tls_broker(listener, authority.broker(ClientCertificates::NotRequested));

    let broker = RabbitMqBroker::from_config(
        &authority.entries(&amqps_address(port), None),
        fixture.dns.clone(),
    )
    .assured("a client that names no identity describes its broker");
    let connected = broker
        .connect_within(connect_deadline(GENEROUS_BUDGET))
        .await;

    let saw = broker_side
        .await
        .assured("the stand-in broker does not panic");
    assert_eq!(
        saw,
        BrokerSaw::Session {
            client_certificates: Vec::new(),
        }
    );
    let Err(report) = connected else {
        panic!("a listener that does not speak AMQP cannot complete the handshake");
    };
    assert!(
        matches!(
            report.current_context(),
            RabbitMqConnectError::AmqpHandshake { .. }
        ),
        "{report:?}"
    );
}

#[nervix_primitives::test]
async fn client_identity_files_that_cannot_be_presented_are_configuration_failures() {
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let authority = Authority::issue();
    // A key file holding a certificate, and a certificate file holding only a key.
    std::fs::write(
        authority.files.path().join("certificate-as-key.pem"),
        authority.client.certificate.pem(),
    )
    .assured("the PEM fixture can be written");
    std::fs::write(
        authority.files.path().join("key-as-certificate.pem"),
        authority.client.key.serialize_pem(),
    )
    .assured("the PEM fixture can be written");
    std::fs::write(authority.files.path().join("not-pem.pem"), [0xff, 0x00])
        .assured("the fixture can be written");
    // PEM sections whose bodies are not base64.
    std::fs::write(
        authority.files.path().join("malformed-certificate.pem"),
        "-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n",
    )
    .assured("the PEM fixture can be written");
    std::fs::write(
        authority.files.path().join("malformed-key.pem"),
        "-----BEGIN PRIVATE KEY-----\n!!!\n-----END PRIVATE KEY-----\n",
    )
    .assured("the PEM fixture can be written");
    /// An identity the client cannot present, and what is wrong with it.
    struct Case {
        identity: IdentityFiles,
        what: &'static str,
    }
    let unreadable = |certificate, key, what| Case {
        identity: IdentityFiles { certificate, key },
        what,
    };
    let address = amqps_address(5671);
    let cases = [
        unreadable(
            "missing.pem",
            CLIENT_IDENTITY.key,
            "a certificate file that cannot be read",
        ),
        unreadable(
            CLIENT_IDENTITY.certificate,
            "missing.pem",
            "a key file that cannot be read",
        ),
        unreadable(
            "key-as-certificate.pem",
            CLIENT_IDENTITY.key,
            "a certificate file holding no certificate",
        ),
        unreadable(
            CLIENT_IDENTITY.certificate,
            "certificate-as-key.pem",
            "a key file holding no private key",
        ),
        unreadable(
            "not-pem.pem",
            CLIENT_IDENTITY.key,
            "a certificate file that is not PEM",
        ),
        unreadable(
            CLIENT_IDENTITY.certificate,
            "not-pem.pem",
            "a key file that is not PEM",
        ),
        unreadable(
            "malformed-certificate.pem",
            CLIENT_IDENTITY.key,
            "a certificate section that is not base64",
        ),
        unreadable(
            CLIENT_IDENTITY.certificate,
            "malformed-key.pem",
            "a key section that is not base64",
        ),
    ];
    for case in cases {
        let what = case.what;
        let entries = authority.entries(&address, Some(case.identity));
        let Err(report) = RabbitMqBroker::from_config(&entries, fixture.dns.clone()) else {
            panic!("{what} must not describe a broker");
        };
        assert!(
            report.current_context().is_configuration(),
            "{what}: {report:?}"
        );
        let reported = format!("{report:?}");
        assert!(
            !reported.contains("PRIVATE KEY"),
            "{what} repeats key material: {reported}"
        );
    }
}

#[nervix_primitives::test]
async fn a_broker_that_requires_a_client_certificate_refuses_a_connection_that_presents_none() {
    nervix_connector::install_rustls_crypto_provider();
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let authority = Authority::issue();
    let listener = loopback_listener(broker_address()).await;
    let port = listener
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    let broker_side = tls_broker(listener, authority.broker(ClientCertificates::Required));

    let broker = RabbitMqBroker::from_config(
        &authority.entries(&amqps_address(port), None),
        fixture.dns.clone(),
    )
    .assured("a client that names no identity describes its broker");
    let connected = broker
        .connect_within(connect_deadline(GENEROUS_BUDGET))
        .await;

    let saw = broker_side
        .await
        .assured("the stand-in broker does not panic");
    assert_eq!(saw, BrokerSaw::NoSession);
    let Err(report) = connected else {
        panic!("a broker that refused the TLS session cannot complete the AMQP handshake");
    };
    // TLS 1.3 completes the client's side of the handshake before the broker checks the
    // client's certificate, so the broker's alert arrives on the first read of the AMQP handshake.
    assert!(
        matches!(
            report.current_context(),
            RabbitMqConnectError::TlsRefused { host, alert: AlertDescription::CertificateRequired }
                if *host == broker_address().to_string()
        ),
        "{report:?}"
    );
    assert!(!report.current_context().is_configuration());
}

#[nervix_primitives::test]
async fn a_client_identity_missing_its_certificate_or_key_is_a_configuration_failure() {
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let authority = Authority::issue();
    for (key, file) in [
        ("tls_cert_file", CLIENT_IDENTITY.certificate),
        ("tls_key_file", CLIENT_IDENTITY.key),
    ] {
        let mut entries = authority.entries(&amqps_address(5671), None);
        entries.push(ClientConfigEntry {
            key: key.to_string(),
            value: authority.file(file),
        });
        let Err(report) = RabbitMqBroker::from_config(&entries, fixture.dns.clone()) else {
            panic!("a client naming only '{key}' must not describe a broker");
        };
        assert!(
            matches!(
                report.current_context(),
                RabbitMqConnectError::IncompleteClientIdentity
            ),
            "{key}: {report:?}"
        );
        assert!(report.current_context().is_configuration(), "{key}");
    }
}

#[nervix_primitives::test]
async fn a_broker_certificate_the_client_does_not_trust_fails_the_handshake_rather_than_a_refusal()
{
    nervix_connector::install_rustls_crypto_provider();
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let broker_authority = Authority::issue();
    let client_authority = Authority::issue();
    let listener = loopback_listener(broker_address()).await;
    let port = listener
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    let broker_side = tls_broker(
        listener,
        broker_authority.broker(ClientCertificates::NotRequested),
    );

    // The client trusts only an authority that did not issue the broker's certificate.
    let broker = RabbitMqBroker::from_config(
        &client_authority.entries(&amqps_address(port), Some(CLIENT_IDENTITY)),
        fixture.dns.clone(),
    )
    .assured("the client's identity files hold a certificate and its key");
    let connected = broker
        .connect_within(connect_deadline(GENEROUS_BUDGET))
        .await;

    let saw = broker_side
        .await
        .assured("the stand-in broker does not panic");
    assert_eq!(saw, BrokerSaw::NoSession);
    let Err(report) = connected else {
        panic!("a client that does not trust the broker cannot complete the handshake");
    };
    assert!(
        matches!(
            report.current_context(),
            RabbitMqConnectError::TlsHandshake { host } if *host == broker_address().to_string()
        ),
        "{report:?}"
    );
}

#[nervix_primitives::test]
async fn a_tls12_broker_that_requires_a_client_certificate_refuses_the_handshake() {
    nervix_connector::install_rustls_crypto_provider();
    let fixture = Fixture::start(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
    let authority = Authority::issue();
    let listener = loopback_listener(broker_address()).await;
    let port = listener
        .local_addr()
        .assured("a bound listener has an address")
        .port();
    let broker_side = tls_broker(
        listener,
        authority.broker(ClientCertificates::RequiredOverTls12),
    );

    let broker = RabbitMqBroker::from_config(
        &authority.entries(&amqps_address(port), None),
        fixture.dns.clone(),
    )
    .assured("a client that names no identity describes its broker");
    let connected = broker
        .connect_within(connect_deadline(GENEROUS_BUDGET))
        .await;

    let saw = broker_side
        .await
        .assured("the stand-in broker does not panic");
    assert_eq!(saw, BrokerSaw::NoSession);
    let Err(report) = connected else {
        panic!("a broker that refused the TLS session cannot complete the AMQP handshake");
    };
    // Under TLS 1.2 the broker's alert ends the handshake itself.
    assert!(
        matches!(
            report.current_context(),
            RabbitMqConnectError::TlsRefused { host, .. } if *host == broker_address().to_string()
        ),
        "{report:?}"
    );
    assert!(!report.current_context().is_configuration());
}
