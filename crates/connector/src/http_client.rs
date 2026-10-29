//! The shared settings an HTTP-speaking connector builds its client from.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Building an HTTP client from a client's entries: the node DNS resolver, the
//!   request deadline its `timeout_ms` sets, optional CA file and optional client identity, and
//!   whether the client follows redirects or uses a declared proxy, and refusing proxy environment
//!   settings for HTTP REQUEST polling.
//! - **Depends on.** The client configuration's entries, TLS paths and PEM files, `nervix-dns`,
//!   and `reqwest`.
//! - **Must not know.** Which connector sends requests through the client, or what it sends.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use error_stack::{Report, ResultExt as _};
use nervix_dns::DnsResolver;
use reqwest::{
    Certificate as HttpCertificate, Client as HttpClient, Identity as HttpIdentity, Proxy, redirect,
};
use thiserror::Error;

use crate::{
    client_config::{client_identity_pem, client_tls_paths, read_tls_file},
    client_deadline::ConfiguredDeadline,
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
    dns: &'a DnsResolver,
}

impl<'a> HttpClientConfig<'a> {
    pub fn new(
        entries: &'a [nervix_models::ClientConfigEntry],
        label: &'static str,
        dns: &'a DnsResolver,
    ) -> Self {
        Self {
            entries,
            label,
            dns,
        }
    }

    pub fn build(&self) -> Result<HttpClient, Report<HttpClientConfigError>> {
        self.finish(self.builder()?)
    }

    /// The client, refusing every redirect: an endpoint's `3xx` answer is the response, and no
    /// request is ever sent to its `Location`.
    pub fn build_without_redirects(&self) -> Result<HttpClient, Report<HttpClientConfigError>> {
        self.finish(self.builder()?.redirect(redirect::Policy::none()))
    }

    /// Apply the HTTP client's explicit redirect and proxy settings. The environment cannot
    /// silently change where a request goes.
    pub fn build_http_request(&self) -> Result<HttpClient, Report<HttpClientConfigError>> {
        let contract = nervix_models::CreateClientHttp::TRANSPORT;
        let follow_redirects = self.entries.iter().any(|entry| {
            entry.key == "follow_redirects" && entry.value.eq_ignore_ascii_case("true")
        });
        let redirects = if follow_redirects {
            redirect::Policy::limited(contract.redirect_limit_with_opt_in)
        } else {
            redirect::Policy::none()
        };
        let builder = self.builder()?;
        let mut builder = if contract.reads_environment_proxy {
            builder
        } else {
            builder.no_proxy()
        }
        .redirect(redirects);
        if let Some(proxy) = self.entries.iter().find(|entry| entry.key == "proxy") {
            let proxy = Proxy::all(&proxy.value).map_err(|source| {
                Report::new(source.without_url())
                    .change_context(HttpClientConfigError::Build { label: self.label })
            })?;
            builder = builder.proxy(proxy);
        }
        self.finish(builder)
    }

    fn finish(
        &self,
        builder: reqwest::ClientBuilder,
    ) -> Result<HttpClient, Report<HttpClientConfigError>> {
        builder.build().map_err(|source| {
            Report::new(source.without_url())
                .change_context(HttpClientConfigError::Build { label: self.label })
        })
    }

    fn builder(&self) -> Result<reqwest::ClientBuilder, Report<HttpClientConfigError>> {
        let mut builder = HttpClient::builder().dns_resolver(self.dns.clone());
        let timeout = ConfiguredDeadline::timeout(self.entries)
            .change_context(HttpClientConfigError::InvalidTimeout { label: self.label })?;
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout.bound());
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
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use nervix_dns::{DnsConfiguration, DnsConfigurationError};
    use nervix_models::ClientConfigEntry;

    use super::*;

    #[nervix_primitives::test]
    async fn http_client_validates_timeout_configuration()
    -> Result<(), Report<DnsConfigurationError>> {
        let dns = DnsResolver::load(DnsConfiguration::system()).await?;
        let client = HttpClientConfig::new(
            &[ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "250".to_string(),
            }],
            "HTTP",
            &dns,
        )
        .build();
        assert!(client.is_ok());
        let refusing_redirects = HttpClientConfig::new(
            &[ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "250".to_string(),
            }],
            "HTTP",
            &dns,
        )
        .build_without_redirects();
        assert!(refusing_redirects.is_ok());

        let err = HttpClientConfig::new(
            &[ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "oops".to_string(),
            }],
            "HTTP",
            &dns,
        )
        .build()
        .expect_err("invalid timeout");
        assert!(err.to_string().contains("invalid HTTP timeout_ms"));
        Ok(())
    }

    #[nervix_primitives::test]
    async fn http_client_classifies_invalid_tls_material()
    -> Result<(), Report<DnsConfigurationError>> {
        let dns = DnsResolver::load(DnsConfiguration::system()).await?;
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
        let ca = HttpClientConfig::new(&ca_entries, "HTTP", &dns)
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
        let identity = HttpClientConfig::new(&identity_entries, "HTTP", &dns)
            .build()
            .expect_err("an invalid client identity must fail");
        assert!(matches!(
            identity.current_context(),
            HttpClientConfigError::ParseClientIdentity { label: "HTTP" }
        ));
        Ok(())
    }
}
