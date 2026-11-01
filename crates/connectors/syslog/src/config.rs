//! Syslog client configuration at the connector boundary.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** How a client configuration fails at this connector, and loading the TLS files a
//!   client names into rustls configurations.
//! - **Depends on.** Connector TLS primitives, the typed clients the vocabulary reads, and rustls.
//! - **Must not know.** Which keys a side reads, runtime tasks, relays, branches, schedules, or
//!   registry state.

#![cfg_attr(
    nervix_lint,
    nervix::input(plans, reason = "connector host supplies validated plans")
)]
#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use error_stack::Report;
use nervix_connector::{RustlsClientConfigProvider, install_rustls_crypto_provider, read_tls_file};
use nervix_models::{ClientConfigEntry, SyslogListenerTls};
use nervix_primitives::sync::StdArc;
use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SyslogConfigError {
    /// The client breaks what its declaration or its side demands, which the frames beneath name.
    #[error("invalid Syslog client configuration")]
    Client,
    /// A deadline key's value breaks the deadline rule, which the frames beneath name with the key.
    #[error("invalid Syslog client deadline")]
    InvalidDeadline,
    #[error("invalid Syslog TLS configuration: {reason}")]
    TlsMaterial { reason: String },
}

/// The rustls configuration of a collector's client: its `tls_ca_file` trust root and optional
/// client identity, which the shared provider reads from the rendered entries.
pub(crate) fn tls_client_config(
    entries: &[ClientConfigEntry],
) -> error_stack::Result<StdArc<rustls::ClientConfig>, SyslogConfigError> {
    let mapping_input = RustlsClientConfigProvider::new(entries).build_with_default_roots();
    match mapping_input {
        Ok(mapped_value) => Ok(mapped_value),
        Err(error) => Err({
            let reason = error.to_string();
            error.change_context(SyslogConfigError::TlsMaterial { reason })
        }),
    }
}

/// The rustls configuration of a TLS listener: its server identity, and the client verifier its
/// `tls_client_ca_file` turns on.
pub(crate) fn tls_server_config(
    tls: &SyslogListenerTls,
) -> error_stack::Result<StdArc<ServerConfig>, SyslogConfigError> {
    install_rustls_crypto_provider();
    let cert_file = &tls.cert_file;
    let key_file = &tls.key_file;
    let cert_pem = {
        let mapping_input = read_tls_file(cert_file, "Syslog TLS server certificate");
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let reason = error.to_string();
                error.change_context(SyslogConfigError::TlsMaterial { reason })
            }),
        }
    }?;
    let key_pem = {
        let mapping_input = read_tls_file(key_file, "Syslog TLS server private key");
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let reason = error.to_string();
                error.change_context(SyslogConfigError::TlsMaterial { reason })
            }),
        }
    }?;
    let certs = {
        let mapping_input =
            CertificateDer::pem_slice_iter(&cert_pem).collect::<Result<Vec<_>, _>>();
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let reason = format!(
                    "invalid Syslog client config key 'tls_cert_file' '{}': {error}",
                    cert_file.display()
                );
                Report::new(error).change_context(SyslogConfigError::TlsMaterial { reason })
            }),
        }
    }?;
    if certs.is_empty() {
        return Err(Report::new(SyslogConfigError::TlsMaterial {
            reason: format!(
                "invalid Syslog client config key 'tls_cert_file' '{}': no certificates found",
                cert_file.display()
            ),
        }));
    }
    let key = {
        let mapping_input = PrivateKeyDer::from_pem_slice(&key_pem);
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let reason = format!(
                    "invalid Syslog client config key 'tls_key_file' '{}': {error}",
                    key_file.display()
                );
                Report::new(error).change_context(SyslogConfigError::TlsMaterial { reason })
            }),
        }
    }?;
    let builder = ServerConfig::builder();
    let config = {
        let mapping_input = if let Some(ca_file) = tls.client_ca_file.as_ref() {
            let ca_pem = {
                let mapping_input = read_tls_file(ca_file, "Syslog TLS client CA certificate");
                match mapping_input {
                    Ok(mapped_value) => Ok(mapped_value),
                    Err(error) => Err({
                        let reason = error.to_string();
                        error.change_context(SyslogConfigError::TlsMaterial { reason })
                    }),
                }
            }?;
            let mut roots = RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(&ca_pem) {
                let cert = {
                    let mapping_input = cert;
                    match mapping_input {
                        Ok(mapped_value) => Ok(mapped_value),
                        Err(error) => Err({
                            let reason = format!(
                                "invalid Syslog client config key 'tls_client_ca_file' '{}': \
                                 {error}",
                                ca_file.display()
                            );
                            Report::new(error)
                                .change_context(SyslogConfigError::TlsMaterial { reason })
                        }),
                    }
                }?;
                {
                    let mapping_input = roots.add(cert);
                    match mapping_input {
                        Ok(mapped_value) => Ok(mapped_value),
                        Err(error) => Err({
                            let reason = format!(
                                "invalid Syslog client config key 'tls_client_ca_file' '{}': \
                                 {error}",
                                ca_file.display()
                            );
                            Report::new(error)
                                .change_context(SyslogConfigError::TlsMaterial { reason })
                        }),
                    }
                }?;
            }
            let verifier = {
                let mapping_input = WebPkiClientVerifier::builder(StdArc::new(roots)).build();
                match mapping_input {
                    Ok(mapped_value) => Ok(mapped_value),
                    Err(error) => Err({
                        let reason = format!(
                            "invalid Syslog client config key 'tls_client_ca_file' '{}': {error}",
                            ca_file.display()
                        );
                        Report::new(error).change_context(SyslogConfigError::TlsMaterial { reason })
                    }),
                }
            }?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
        } else {
            builder.with_no_client_auth().with_single_cert(certs, key)
        };
        match mapping_input {
            Ok(mapped_value) => Ok(mapped_value),
            Err(error) => Err({
                let reason = format!("invalid Syslog TLS server identity: {error}");
                Report::new(error).change_context(SyslogConfigError::TlsMaterial { reason })
            }),
        }
    }?;
    Ok(StdArc::new(config))
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn syslog_tls_material_read_failures_keep_their_context() {
        let missing = "/definitely/missing/nervix-syslog-tls.pem";
        let collector = [ClientConfigEntry {
            key: "tls_ca_file".to_string(),
            value: missing.to_string(),
        }];
        let error =
            tls_client_config(&collector).expect_err("a missing collector CA file must fail");
        assert!(matches!(
            error.current_context(),
            SyslogConfigError::TlsMaterial { .. }
        ));
        assert!(error.contains::<nervix_connector::ClientConfigError>());

        let error = tls_server_config(&SyslogListenerTls {
            cert_file: PathBuf::from(missing),
            key_file: PathBuf::from(missing),
            client_ca_file: None,
        })
        .expect_err("a missing server certificate must fail");
        assert!(matches!(
            error.current_context(),
            SyslogConfigError::TlsMaterial { .. }
        ));
        assert!(error.contains::<nervix_connector::ClientConfigError>());

        let root = tempfile::tempdir().expect("temporary Syslog TLS directory should open");
        let certificate = root.path().join("certificate.pem");
        std::fs::write(&certificate, b"certificate fixture")
            .expect("the Syslog certificate fixture should be writable");
        let error = tls_server_config(&SyslogListenerTls {
            cert_file: certificate,
            key_file: PathBuf::from(missing),
            client_ca_file: None,
        })
        .expect_err("a missing server private key must fail");
        assert!(matches!(
            error.current_context(),
            SyslogConfigError::TlsMaterial { .. }
        ));
        assert!(error.contains::<nervix_connector::ClientConfigError>());
    }
}
