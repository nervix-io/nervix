//! The shared settings an HTTP-speaking connector builds its client from.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Building an HTTP client from a client's entries: the request timeout, the optional
//!   CA file and the optional client identity.
//! - **Depends on.** The client configuration's entries, TLS paths and PEM files, and `reqwest`.
//! - **Must not know.** Which connector sends requests through the client, or what it sends.

use std::time::Duration;

use error_stack::{Report, ResultExt as _};
use reqwest::{Certificate as HttpCertificate, Client as HttpClient, Identity as HttpIdentity};
use thiserror::Error;

use crate::client_config::{
    client_identity_pem, client_tls_paths, optional_client_config_value, read_tls_file,
};

#[derive(Debug, Error)]
pub enum HttpClientConfigError {
    #[error("invalid {label} timeout_ms")]
    InvalidTimeout { label: &'static str },
    #[error("failed to read {label} TLS configuration")]
    ReadTls { label: &'static str },
    #[error("failed to parse {label} TLS CA certificate")]
    ParseCaCertificate { label: &'static str },
    #[error("failed to parse {label} TLS client identity")]
    ParseClientIdentity { label: &'static str },
    #[error("failed to build {label} HTTP client")]
    Build { label: &'static str },
}

pub struct HttpClientConfig<'a> {
    entries: &'a [nervix_models::ClientConfigEntry],
    label: &'static str,
}

impl<'a> HttpClientConfig<'a> {
    pub fn new(entries: &'a [nervix_models::ClientConfigEntry], label: &'static str) -> Self {
        Self { entries, label }
    }

    pub fn build(&self) -> Result<HttpClient, Report<HttpClientConfigError>> {
        self.builder()?.build().map_err(|source| {
            Report::new(HttpClientConfigError::Build { label: self.label })
                .attach_printable(source.to_string())
        })
    }

    fn builder(&self) -> Result<reqwest::ClientBuilder, Report<HttpClientConfigError>> {
        let mut builder = HttpClient::builder();
        if let Some(timeout_ms) = optional_client_config_value(self.entries, "timeout_ms") {
            let timeout_ms = timeout_ms.parse::<u64>().map_err(|source| {
                Report::new(HttpClientConfigError::InvalidTimeout { label: self.label })
                    .attach_printable(source.to_string())
            })?;
            builder = builder.timeout(Duration::from_millis(timeout_ms));
        }

        let tls = client_tls_paths(self.entries);
        if let Some(ca_file) = tls.ca_file.as_ref() {
            let ca_pem = read_tls_file(ca_file, "TLS CA certificate")
                .change_context(HttpClientConfigError::ReadTls { label: self.label })?;
            builder = builder.add_root_certificate(HttpCertificate::from_pem(&ca_pem).map_err(
                |source| {
                    Report::new(HttpClientConfigError::ParseCaCertificate { label: self.label })
                        .attach_printable(source.to_string())
                },
            )?);
        }
        if let Some(identity_pem) = client_identity_pem(&tls)
            .change_context(HttpClientConfigError::ReadTls { label: self.label })?
        {
            builder =
                builder.identity(HttpIdentity::from_pem(&identity_pem).map_err(|source| {
                    Report::new(HttpClientConfigError::ParseClientIdentity { label: self.label })
                        .attach_printable(source.to_string())
                })?);
        }
        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::ClientConfigEntry;

    use super::*;

    #[test]
    fn http_client_validates_timeout_configuration() {
        let client = HttpClientConfig::new(
            &[ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "250".to_string(),
            }],
            "HTTP",
        )
        .build();
        assert!(client.is_ok());

        let err = HttpClientConfig::new(
            &[ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "oops".to_string(),
            }],
            "HTTP",
        )
        .build()
        .expect_err("invalid timeout");
        assert!(err.to_string().contains("invalid HTTP timeout_ms"));
    }

    #[test]
    fn http_client_classifies_invalid_tls_material() {
        let root = tempfile::tempdir().expect("temporary TLS directory should open");
        let invalid = root.path().join("invalid.pem");
        std::fs::write(
            &invalid,
            b"-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n",
        )
        .expect("the invalid TLS fixture should be writable");
        let invalid = invalid.to_string_lossy().into_owned();

        let ca_entries = [ClientConfigEntry {
            key: "tls_ca_file".to_string(),
            value: invalid.clone(),
        }];
        let ca = HttpClientConfig::new(&ca_entries, "HTTP")
            .build()
            .expect_err("an invalid CA certificate must fail");
        assert!(matches!(
            ca.current_context(),
            HttpClientConfigError::ParseCaCertificate { label: "HTTP" }
                | HttpClientConfigError::Build { label: "HTTP" }
        ));

        let identity_entries = [
            ClientConfigEntry {
                key: "tls_cert_file".to_string(),
                value: invalid.clone(),
            },
            ClientConfigEntry {
                key: "tls_key_file".to_string(),
                value: invalid,
            },
        ];
        let identity = HttpClientConfig::new(&identity_entries, "HTTP")
            .build()
            .expect_err("an invalid client identity must fail");
        assert!(matches!(
            identity.current_context(),
            HttpClientConfigError::ParseClientIdentity { label: "HTTP" }
        ));
    }
}
