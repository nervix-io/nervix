//! The interconnect's TLS algorithm policy through the production bundle and real rustls peers.
//!
//! Layer: test harness.
//! - **Owns.** ClientHello refusal assertions for the interconnect's fixed algorithm set.
//! - **Depends on.** The production TLS bundle, test certificates and rustls connections.
//! - **Must not know.** Graph execution, request routing or connector policy.

use std::io::Cursor;

use meticulous::ResultExt as _;
use nervix_primitives::sync::StdArc;
use rustls::{
    ClientConfig, ClientConnection, PeerIncompatible, RootCertStore, ServerConnection,
    crypto::{CryptoProvider, aws_lc_rs},
};

use super::TestCertificateAuthority;

struct TlsPeer {
    client: ClientConnection,
    server: ServerConnection,
}

impl TlsPeer {
    fn new(provider: CryptoProvider) -> Self {
        let authority = TestCertificateAuthority::new();
        let bundle = authority.issue("policy", &"node".parse().assured("node is a valid name"));
        let mut roots = RootCertStore::empty();
        roots
            .add(authority.certificate.der().clone())
            .assured("the generated authority is a valid trust anchor");
        let config = ClientConfig::builder_with_provider(StdArc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .assured("the fixture provider supplies TLS 1.3 suites")
            .with_root_certificates(roots)
            .with_client_cert_resolver(StdArc::clone(
                &bundle.client_config.client_auth_cert_resolver,
            ));
        Self {
            client: ClientConnection::new(
                StdArc::new(config),
                "localhost"
                    .try_into()
                    .assured("localhost is a valid server name"),
            )
            .assured("the fixture supplies a complete TLS client config"),
            server: ServerConnection::new(bundle.server_config)
                .assured("the production bundle supplies a complete server config"),
        }
    }

    fn assert_refused(&mut self, reason: PeerIncompatible) {
        let mut hello = Vec::new();
        self.client
            .write_tls(&mut hello)
            .assured("writing the client hello into a Vec cannot fail");
        self.server
            .read_tls(&mut Cursor::new(hello))
            .assured("the complete client hello is readable from memory");
        let result = self.server.process_new_packets();
        assert!(
            matches!(&result, Err(rustls::Error::PeerIncompatible(actual)) if actual == &reason),
            "the interconnect must refuse the peer with {reason:?}: {result:?}"
        );
    }

    fn assert_handshake(&mut self) {
        for _ in 0..8 {
            let mut outbound = Vec::new();
            self.client
                .write_tls(&mut outbound)
                .assured("writing TLS records into memory cannot fail");
            self.server
                .read_tls(&mut Cursor::new(outbound))
                .assured("the client's TLS records are readable from memory");
            self.server
                .process_new_packets()
                .assured("the production server accepts the approved peer");
            let mut outbound = Vec::new();
            self.server
                .write_tls(&mut outbound)
                .assured("writing TLS records into memory cannot fail");
            self.client
                .read_tls(&mut Cursor::new(outbound))
                .assured("the server's TLS records are readable from memory");
            self.client
                .process_new_packets()
                .assured("the peer verifies the production server");
            if !self.client.is_handshaking() && !self.server.is_handshaking() {
                assert_eq!(
                    self.client.protocol_version(),
                    Some(rustls::ProtocolVersion::TLSv1_3)
                );
                assert_eq!(
                    self.server.protocol_version(),
                    Some(rustls::ProtocolVersion::TLSv1_3)
                );
                return;
            }
        }
        panic!("the in-memory mutual TLS handshake did not complete in eight exchanges");
    }
}

#[test]
fn interconnect_refuses_a_peer_offering_only_chacha20() {
    let mut provider = nervix_crypto::tls::outward_tls_provider().rustls();
    provider.cipher_suites = vec![aws_lc_rs::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256];
    let mut peer = TlsPeer::new(provider);
    peer.assert_refused(PeerIncompatible::NoCipherSuitesInCommon);
}

#[test]
fn interconnect_refuses_a_peer_offering_only_x25519() {
    let mut provider = nervix_crypto::tls::outward_tls_provider().rustls();
    provider.kx_groups = vec![aws_lc_rs::kx_group::X25519];
    let mut peer = TlsPeer::new(provider);
    peer.assert_refused(PeerIncompatible::NoKxGroupsInCommon);
}

#[test]
fn interconnect_accepts_every_approved_cipher_and_group() {
    for suite in [
        aws_lc_rs::cipher_suite::TLS13_AES_256_GCM_SHA384,
        aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256,
    ] {
        for group in [
            aws_lc_rs::kx_group::SECP256R1,
            aws_lc_rs::kx_group::SECP384R1,
            aws_lc_rs::kx_group::X25519MLKEM768,
        ] {
            let mut provider = nervix_crypto::tls::outward_tls_provider().rustls();
            provider.cipher_suites = vec![suite];
            provider.kx_groups = vec![group];
            TlsPeer::new(provider).assert_handshake();
        }
    }
}
