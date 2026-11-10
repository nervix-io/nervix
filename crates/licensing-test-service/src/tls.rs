//! TLS identity for one in-process licensing fixture.
//!
//! Layer: test harness.
//! - **Owns.** A per-run CA, leaf certificate and the temporary CA file.
//! - **Depends on.** Maintained rcgen/rustls encodings and the AWS-LC provider.
//! - **Must not know.** Signed licensing documents or server policy.

use std::path::PathBuf;

use error_stack::ResultExt as _;
use nervix_primitives::sync::StdArc;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
use rustls_pki_types::{PrivateKeyDer, pem::PemObject as _};
use tokio_rustls::TlsAcceptor;

use crate::ServiceError;

pub(super) struct Identity {
    pub acceptor: TlsAcceptor,
    pub ca_file: PathBuf,
    _directory: tempfile::TempDir,
}

impl Identity {
    pub fn generate() -> error_stack::Result<Self, ServiceError> {
        let ca_key = KeyPair::generate().change_context(ServiceError::Tls)?;
        let mut ca_params = CertificateParams::new(Vec::new()).change_context(ServiceError::Tls)?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = ca_params
            .self_signed(&ca_key)
            .change_context(ServiceError::Tls)?;
        let key = KeyPair::generate().change_context(ServiceError::Tls)?;
        let params = CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .change_context(ServiceError::Tls)?;
        let leaf = params
            .signed_by(&key, &ca, &ca_key)
            .change_context(ServiceError::Tls)?;
        let key = PrivateKeyDer::from_pem_slice(key.serialize_pem().as_bytes())
            .change_context(ServiceError::Tls)?;
        let provider = nervix_crypto::tls::outward_tls_provider().rustls();
        let mut config = rustls::ServerConfig::builder_with_provider(StdArc::new(provider))
            .with_safe_default_protocol_versions()
            .change_context(ServiceError::Tls)?
            .with_no_client_auth()
            .with_single_cert(vec![leaf.der().clone()], key)
            .change_context(ServiceError::Tls)?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let directory = tempfile::tempdir().change_context(ServiceError::Tls)?;
        let ca_file = directory.path().join("licensing-ca.pem");
        std::fs::write(&ca_file, ca.pem()).change_context(ServiceError::Tls)?;
        Ok(Self {
            acceptor: TlsAcceptor::from(StdArc::new(config)),
            ca_file,
            _directory: directory,
        })
    }
}
