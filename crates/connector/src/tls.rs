//! The rustls client configuration a connector's TLS transport is built from.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Building a rustls client configuration from a client's TLS entries: the trust
//!   anchors, the optional CA file, the optional client identity, and how much of a server's
//!   certificate the client confirms.
//! - **Depends on.** The client configuration's TLS paths and PEM files, rustls with its AWS-LC
//!   provider, and the bundled and host trust stores.
//! - **Must not know.** Which connector or driver the configuration is built for.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use error_stack::{Report, ResultExt as _};
use nervix_primitives::sync::StdArc;
use nervix_recovery::Discarded as _;
use rustls::{
    CertificateError, ClientConfig as RustlsClientConfig, DigitallySignedStruct,
    Error as RustlsError, RootCertStore, SignatureScheme,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject};
use thiserror::Error;
use tracing::warn;

use crate::client_config::{ClientTlsPaths, client_identity_pem, client_tls_paths, read_tls_file};

#[derive(Debug, Error)]
pub enum TlsClientConfigError {
    #[error("failed to read TLS CA certificate '{path}'")]
    ReadCaCertificate { path: std::path::PathBuf },
    #[error("failed to read TLS client identity")]
    ReadClientIdentity,
    #[error("failed to parse TLS CA certificate '{path}'")]
    ParseCaCertificate { path: std::path::PathBuf },
    #[error("failed to add TLS CA certificate '{path}'")]
    AddCaCertificate { path: std::path::PathBuf },
    #[error("failed to parse TLS client certificate chain")]
    ParseClientCertificate,
    #[error("failed to parse TLS client private key")]
    ParseClientKey,
    #[error("failed to configure TLS client certificate")]
    ConfigureClientCertificate,
    #[error("failed to build the verifier of TLS server certificates")]
    BuildServerVerifier,
}

/// How much of a server's certificate a client confirms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerCertificateTrust {
    /// It chains to a trust anchor and names the server the client dialled.
    NamedAndAnchored,
    /// It chains to a trust anchor, whatever server it names.
    Anchored,
    /// Nothing about it: the connection is encrypted and says nothing about who holds it.
    Unconfirmed,
}

/// Confirms less of a server's certificate than a default configuration does, as a client's
/// [`ServerCertificateTrust`] asks. The handshake's signatures are verified as usual, so the server
/// still proves it holds the key of the certificate it presented.
#[derive(Debug)]
struct RelaxedServerVerifier {
    verifier: StdArc<WebPkiServerVerifier>,
    trust: ServerCertificateTrust,
}

impl ServerCertVerifier for RelaxedServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        let verified = self.verifier.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        );
        match (self.trust, verified) {
            (ServerCertificateTrust::Unconfirmed, _) => Ok(ServerCertVerified::assertion()),
            // The verifier compares the name last, so this certificate chained to an anchor.
            (
                ServerCertificateTrust::Anchored,
                Err(RustlsError::InvalidCertificate(
                    CertificateError::NotValidForName
                    | CertificateError::NotValidForNameContext { .. },
                )),
            ) => Ok(ServerCertVerified::assertion()),
            (_, verified) => verified,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.verifier
            .verify_tls12_signature(message, certificate, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.verifier
            .verify_tls13_signature(message, certificate, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.verifier.supported_verify_schemes()
    }
}

pub struct RustlsClientConfigProvider<'a> {
    entries: &'a [nervix_models::ClientConfigEntry],
}

#[derive(Clone, Copy)]
enum CaPlacement {
    AddToDefaults,
    ReplaceDefaults,
}

/// Installs the rustls provider connector transports use unless the host already chose one.
pub fn install_rustls_crypto_provider() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .discarded(
            "a provider the host installed first is the one this connector would have installed",
        );
}

impl<'a> RustlsClientConfigProvider<'a> {
    pub fn new(entries: &'a [nervix_models::ClientConfigEntry]) -> Self {
        Self { entries }
    }

    pub fn build(
        &self,
    ) -> Result<Option<StdArc<RustlsClientConfig>>, Report<TlsClientConfigError>> {
        let tls = client_tls_paths(self.entries);
        if tls.is_empty() {
            return Ok(None);
        }

        self.build_config(
            tls,
            CaPlacement::AddToDefaults,
            ServerCertificateTrust::NamedAndAnchored,
        )
        .map(Some)
    }

    pub fn build_with_default_roots(
        &self,
    ) -> Result<StdArc<RustlsClientConfig>, Report<TlsClientConfigError>> {
        self.build_with_server_trust(ServerCertificateTrust::NamedAndAnchored)
    }

    /// Build a TLS client from the default roots that confirms as much of a server's certificate
    /// as `trust` says.
    pub fn build_with_server_trust(
        &self,
        trust: ServerCertificateTrust,
    ) -> Result<StdArc<RustlsClientConfig>, Report<TlsClientConfigError>> {
        self.build_config(
            client_tls_paths(self.entries),
            CaPlacement::AddToDefaults,
            trust,
        )
    }

    /// Build a TLS client from native roots, replacing them when a CA file is configured.
    pub fn build_with_replacement_ca(
        &self,
    ) -> Result<StdArc<RustlsClientConfig>, Report<TlsClientConfigError>> {
        self.build_config(
            client_tls_paths(self.entries),
            CaPlacement::ReplaceDefaults,
            ServerCertificateTrust::NamedAndAnchored,
        )
    }

    fn build_config(
        &self,
        tls: ClientTlsPaths,
        ca_placement: CaPlacement,
        trust: ServerCertificateTrust,
    ) -> Result<StdArc<RustlsClientConfig>, Report<TlsClientConfigError>> {
        install_rustls_crypto_provider();

        let mut roots = match (ca_placement, tls.ca_file.as_ref()) {
            (CaPlacement::ReplaceDefaults, Some(_)) => RootCertStore::empty(),
            (CaPlacement::ReplaceDefaults, None) => Self::root_store_with_native_roots(),
            (CaPlacement::AddToDefaults, _) => Self::root_store_with_default_roots(),
        };
        if let Some(ca_file) = tls.ca_file.as_ref() {
            let ca_pem = read_tls_file(ca_file, "TLS CA certificate").change_context(
                TlsClientConfigError::ReadCaCertificate {
                    path: ca_file.clone(),
                },
            )?;
            for cert in CertificateDer::pem_slice_iter(&ca_pem) {
                let cert = cert.map_err(|source| {
                    Report::new(TlsClientConfigError::ParseCaCertificate {
                        path: ca_file.clone(),
                    })
                    .attach_printable(source.to_string())
                })?;
                roots.add(cert).map_err(|source| {
                    Report::new(TlsClientConfigError::AddCaCertificate {
                        path: ca_file.clone(),
                    })
                    .attach_printable(source.to_string())
                })?;
            }
        }

        let builder = RustlsClientConfig::builder();
        let builder = match trust {
            ServerCertificateTrust::NamedAndAnchored => builder.with_root_certificates(roots),
            ServerCertificateTrust::Anchored | ServerCertificateTrust::Unconfirmed => {
                let verifier = WebPkiServerVerifier::builder(StdArc::new(roots))
                    .build()
                    .change_context(TlsClientConfigError::BuildServerVerifier)?;
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(StdArc::new(RelaxedServerVerifier {
                        verifier,
                        trust,
                    }))
            }
        };
        let client_config = if let Some(identity_pem) =
            client_identity_pem(&tls).change_context(TlsClientConfigError::ReadClientIdentity)?
        {
            let certs = CertificateDer::pem_slice_iter(&identity_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|source| {
                    Report::new(TlsClientConfigError::ParseClientCertificate)
                        .attach_printable(source.to_string())
                })?;
            let key = PrivateKeyDer::from_pem_slice(&identity_pem).map_err(|source| {
                Report::new(TlsClientConfigError::ParseClientKey)
                    .attach_printable(source.to_string())
            })?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|source| {
                    Report::new(TlsClientConfigError::ConfigureClientCertificate)
                        .attach_printable(source.to_string())
                })?
        } else {
            builder.with_no_client_auth()
        };
        Ok(StdArc::new(client_config))
    }

    /// The trust anchors a connector validates a server against: the bundled public roots, plus
    /// whatever the host's own trust store adds to them.
    ///
    /// The host store is the part that can fail, and failing it is not fatal — the bundled roots
    /// still verify every public certificate authority. It is also the part an operator installs a
    /// private CA into, so a store that failed to load and a store that was empty produce exactly
    /// the same handshake failure later, with nothing to tell them apart. Both failures are
    /// therefore reported here rather than absorbed into the fallback.
    fn root_store_with_default_roots() -> RootCertStore {
        let mut roots = RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        Self::add_native_roots(&mut roots);
        roots
    }

    /// The native roots used when a caller requests CA replacement but supplies no CA file.
    fn root_store_with_native_roots() -> RootCertStore {
        let mut roots = RootCertStore::empty();
        Self::add_native_roots(&mut roots);
        roots
    }

    fn add_native_roots(roots: &mut RootCertStore) {
        let native_roots = rustls_native_certs::load_native_certs();
        for error in &native_roots.errors {
            warn!(%error, "failed to read part of the host TLS trust store");
        }
        for cert in native_roots.certs {
            if let Err(error) = roots.add(cert) {
                warn!(%error, "rejected a certificate from the host TLS trust store");
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::ResultExt as _;
    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};

    use super::*;

    fn entry(key: &str, value: &std::path::Path) -> nervix_models::ClientConfigEntry {
        nervix_models::ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string_lossy().into_owned(),
        }
    }

    /// A certificate authority and a server whose certificate it signed for `broker.example`.
    struct TlsFixture {
        directory: tempfile::TempDir,
        server: StdArc<rustls::ServerConfig>,
    }

    impl TlsFixture {
        fn generate() -> Self {
            install_rustls_crypto_provider();
            let authority_key = KeyPair::generate().assured("the key generator has entropy");
            let mut authority = CertificateParams::new(Vec::new())
                .assured("a certificate without names has no invalid name");
            authority.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let authority = authority
                .self_signed(&authority_key)
                .assured("an authority signs its own certificate");
            let server_key = KeyPair::generate().assured("the key generator has entropy");
            let server = CertificateParams::new(vec!["broker.example".to_string()])
                .assured("the fixture name is a valid DNS name")
                .signed_by(&server_key, &authority, &authority_key)
                .assured("the authority signs the server's certificate");
            let directory = tempfile::tempdir().expect("temporary TLS directory should open");
            std::fs::write(directory.path().join("ca.pem"), authority.pem())
                .expect("the authority fixture should be writable");
            let server = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![server.der().clone()],
                    PrivateKeyDer::Pkcs8(rustls_pki_types::PrivatePkcs8KeyDer::from(
                        server_key.serialize_der(),
                    )),
                )
                .assured("the generated key matches the generated certificate");
            Self {
                directory,
                server: StdArc::new(server),
            }
        }

        fn client(
            &self,
            trust_authority: bool,
            trust: ServerCertificateTrust,
        ) -> StdArc<RustlsClientConfig> {
            let mut entries = Vec::new();
            if trust_authority {
                entries.push(entry("tls_ca_file", &self.directory.path().join("ca.pem")));
            }
            RustlsClientConfigProvider::new(&entries)
                .build_with_server_trust(trust)
                .expect("the fixture's client configuration should build")
        }

        /// Runs a handshake with this server in memory, as a client that dialled `name`, and
        /// returns what the client decided.
        fn dial(&self, client: StdArc<RustlsClientConfig>, name: &str) -> Result<(), RustlsError> {
            let name = ServerName::try_from(name.to_string()).expect("a valid fixture name");
            let mut client = rustls::ClientConnection::new(client, name)?;
            let mut server = rustls::ServerConnection::new(self.server.clone())?;
            while client.is_handshaking() {
                let mut sent = Vec::new();
                client.write_tls(&mut sent).expect("a Vec takes every byte");
                server
                    .read_tls(&mut sent.as_slice())
                    .expect("a slice gives every byte");
                server.process_new_packets()?;
                let mut answered = Vec::new();
                server
                    .write_tls(&mut answered)
                    .expect("a Vec takes every byte");
                client
                    .read_tls(&mut answered.as_slice())
                    .expect("a slice gives every byte");
                client.process_new_packets()?;
            }
            Ok(())
        }
    }

    #[test]
    fn a_client_confirms_as_much_of_a_server_certificate_as_its_trust_says() {
        let fixture = TlsFixture::generate();
        let confirmed = fixture.client(true, ServerCertificateTrust::NamedAndAnchored);
        fixture
            .dial(confirmed.clone(), "broker.example")
            .expect("the certificate names the server and chains to a trusted authority");
        assert!(matches!(
            fixture.dial(confirmed, "other.example"),
            Err(RustlsError::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. }
            ))
        ));

        let anchored = fixture.client(true, ServerCertificateTrust::Anchored);
        fixture
            .dial(anchored, "other.example")
            .expect("a client that compares no name accepts another server's certificate");
        let unanchored = fixture.client(false, ServerCertificateTrust::Anchored);
        assert!(matches!(
            fixture.dial(unanchored, "broker.example"),
            Err(RustlsError::InvalidCertificate(
                CertificateError::UnknownIssuer
            ))
        ));

        let unconfirmed = fixture.client(false, ServerCertificateTrust::Unconfirmed);
        fixture
            .dial(unconfirmed, "other.example")
            .expect("a client that confirms nothing accepts any certificate");
    }

    #[test]
    fn default_root_store_includes_public_webpki_roots() {
        let roots = RustlsClientConfigProvider::root_store_with_default_roots();
        assert!(roots.roots.len() >= webpki_roots::TLS_SERVER_ROOTS.len());
    }

    #[test]
    fn required_client_config_builds_without_client_tls_entries() {
        let entries = Vec::new();
        RustlsClientConfigProvider::new(&entries)
            .build_with_default_roots()
            .expect("default-root TLS config should build");
        RustlsClientConfigProvider::new(&entries)
            .build_with_replacement_ca()
            .expect("Redis's native-root TLS config should build");
    }

    #[test]
    fn rustls_client_config_classifies_invalid_certificate_material() {
        let root = tempfile::tempdir().expect("temporary TLS directory should open");
        let malformed_certificate = root.path().join("malformed-certificate.pem");
        std::fs::write(
            &malformed_certificate,
            b"-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n",
        )
        .expect("the malformed certificate fixture should be writable");
        let malformed_ca = [entry("tls_ca_file", &malformed_certificate)];
        let error = RustlsClientConfigProvider::new(&malformed_ca)
            .build()
            .expect_err("malformed CA PEM must fail parsing");
        assert!(matches!(
            error.current_context(),
            TlsClientConfigError::ParseCaCertificate { .. }
        ));

        let invalid_certificate = root.path().join("invalid-certificate.pem");
        std::fs::write(
            &invalid_certificate,
            b"-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n",
        )
        .expect("the invalid certificate fixture should be writable");
        let invalid_ca = [entry("tls_ca_file", &invalid_certificate)];
        let error = RustlsClientConfigProvider::new(&invalid_ca)
            .build()
            .expect_err("invalid CA DER must fail trust-store insertion");
        assert!(matches!(
            error.current_context(),
            TlsClientConfigError::AddCaCertificate { .. }
        ));

        let empty_certificate = root.path().join("empty-certificate.pem");
        let invalid_key = root.path().join("invalid-key.pem");
        std::fs::write(&empty_certificate, b"")
            .expect("the empty certificate fixture should be writable");
        std::fs::write(&invalid_key, b"not a private key")
            .expect("the invalid key fixture should be writable");
        let identity = [
            entry("tls_cert_file", &empty_certificate),
            entry("tls_key_file", &invalid_key),
        ];
        let error = RustlsClientConfigProvider::new(&identity)
            .build()
            .expect_err("invalid private-key PEM must fail parsing");
        assert!(matches!(
            error.current_context(),
            TlsClientConfigError::ParseClientKey
        ));

        let malformed_identity = [
            entry("tls_cert_file", &malformed_certificate),
            entry("tls_key_file", &invalid_key),
        ];
        let error = RustlsClientConfigProvider::new(&malformed_identity)
            .build()
            .expect_err("malformed client certificate PEM must fail parsing");
        assert!(matches!(
            error.current_context(),
            TlsClientConfigError::ParseClientCertificate
        ));
    }
}
