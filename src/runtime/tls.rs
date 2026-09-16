use std::sync::Arc;

use error_stack::{Report, ResultExt as _};
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use thiserror::Error;
use tracing::warn;

use super::client_config::{client_identity_pem, client_tls_paths, read_tls_file};

#[derive(Debug, Error)]
pub(in crate::runtime) enum TlsClientConfigError {
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
}

pub(in crate::runtime) struct RustlsClientConfigSource<'a> {
    entries: &'a [nervix_models::ClientConfigEntry],
}

impl<'a> RustlsClientConfigSource<'a> {
    pub(in crate::runtime) fn new(entries: &'a [nervix_models::ClientConfigEntry]) -> Self {
        Self { entries }
    }

    pub(in crate::runtime) fn build(
        &self,
    ) -> Result<Option<Arc<RustlsClientConfig>>, Report<TlsClientConfigError>> {
        let tls = client_tls_paths(self.entries);
        if tls.is_empty() {
            return Ok(None);
        }

        self.build_config(tls).map(Some)
    }

    pub(in crate::runtime) fn build_with_default_roots(
        &self,
    ) -> Result<Arc<RustlsClientConfig>, Report<TlsClientConfigError>> {
        self.build_config(client_tls_paths(self.entries))
    }

    fn build_config(
        &self,
        tls: super::client_config::ClientTlsPaths,
    ) -> Result<Arc<RustlsClientConfig>, Report<TlsClientConfigError>> {
        nervix_interconnect::install_rustls_crypto_provider();

        let mut roots = Self::root_store_with_default_roots();
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

        let builder = RustlsClientConfig::builder().with_root_certificates(roots);
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
        Ok(Arc::new(client_config))
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
        let native_roots = rustls_native_certs::load_native_certs();
        for error in &native_roots.errors {
            warn!(%error, "failed to read part of the host TLS trust store");
        }
        for cert in native_roots.certs {
            if let Err(error) = roots.add(cert) {
                warn!(%error, "rejected a certificate from the host TLS trust store");
            }
        }
        roots
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_root_store_includes_public_webpki_roots() {
        let roots = RustlsClientConfigSource::root_store_with_default_roots();
        assert!(roots.roots.len() >= webpki_roots::TLS_SERVER_ROOTS.len());
    }

    #[test]
    fn required_client_config_builds_without_client_tls_entries() {
        let entries = Vec::new();
        RustlsClientConfigSource::new(&entries)
            .build_with_default_roots()
            .expect("default-root TLS config should build");
    }
}
