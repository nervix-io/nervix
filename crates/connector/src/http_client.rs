//! The shared settings an HTTP-speaking connector builds its client from.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Building an HTTP client from a client's entries: the node DNS resolver, the
//!   request deadline its `timeout_ms` sets, optional CA file and optional client identity, the
//!   one proxy the client declares and the credentials it carries, refusing every redirect so the
//!   connector that sends decides what a redirect means, and never reading a proxy from the
//!   environment.
//! - **Depends on.** The client configuration's entries, TLS paths and PEM files, `nervix-dns`,
//!   URLs, Base64 and percent decoding, and `reqwest`.
//! - **Must not know.** Which connector sends requests through the client, or what it sends.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        engine,
        reason = "owns the engines and infrastructure boundary declared above"
    )
)]

use std::fmt;

use base64::Engine as _;
use error_stack::{Report, ResultExt as _};
use nervix_dns::DnsResolver;
use percent_encoding::percent_decode_str;
use reqwest::{
    Certificate as HttpCertificate, Client as HttpClient, Identity as HttpIdentity, Proxy, redirect,
};
use thiserror::Error;
use url::Url;

use crate::{
    client_config::{
        client_identity_pem, client_tls_paths, optional_client_config_value, read_tls_file,
    },
    client_deadline::ConfiguredDeadline,
};

/// The configuration key that names a client's proxy.
const PROXY: &str = "proxy";

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
    /// The `proxy` value is not an `http` or `https` URL with a host. The value itself is never
    /// quoted, because its user information is a credential.
    #[error("invalid {label} proxy")]
    InvalidProxy { label: &'static str },
    #[error("failed to build {label} HTTP client")]
    Build { label: &'static str },
}

pub struct HttpClientConfig<'a> {
    entries: &'a [nervix_models::ClientConfigEntry],
    label: &'static str,
    dns: &'a DnsResolver,
}

/// The one proxy a client declares with its `proxy` key. Every request the client sends travels
/// through it; nothing in the environment names another.
///
/// Its `Debug` names only the proxy's scheme, host and port, never its credentials.
pub struct DeclaredProxy {
    /// The proxy's scheme, host and port, without its user information.
    address: Url,
    /// The `Proxy-Authorization` value the declared URL's user information asks for.
    authorization: Option<String>,
    /// The declared URL as written, which Reqwest reads its credentials from itself.
    declared: Url,
}

impl fmt::Debug for DeclaredProxy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeclaredProxy")
            .field("address", &self.address.as_str())
            .field("authenticates", &self.authorization.is_some())
            .finish()
    }
}

impl DeclaredProxy {
    /// The proxy's scheme, host and port, which a connection dials; it carries no credentials.
    pub fn address(&self) -> &Url {
        &self.address
    }

    /// The `Proxy-Authorization` value that carries the declared credentials, `Basic` and the
    /// Base64 of the percent-decoded user, a colon and the percent-decoded password, when the
    /// declared URL has user information. It is written only to the proxy.
    pub fn authorization(&self) -> Option<&str> {
        self.authorization.as_deref()
    }

    fn parse(value: &str, label: &'static str) -> Result<Self, Report<HttpClientConfigError>> {
        let invalid = || HttpClientConfigError::InvalidProxy { label };
        let declared = Url::parse(value).change_context_lazy(invalid)?;
        if !matches!(declared.scheme(), "http" | "https") || declared.host().is_none() {
            return Err(Report::new(invalid()));
        }
        let authorization = Self::basic_authorization(&declared);
        let mut address = declared.clone();
        // Both setters fail only for a URL that cannot have a host, which the check above refused.
        let cleared_user = address.set_username("");
        let cleared_password = address.set_password(None);
        if cleared_user.is_err() || cleared_password.is_err() {
            return Err(Report::new(invalid()));
        }
        Ok(Self {
            address,
            authorization,
            declared,
        })
    }

    /// `Basic` credentials of the URL's user information, decoded as an HTTP client decodes a
    /// proxy URL's: each part percent-decoded, and invalid UTF-8 replaced.
    fn basic_authorization(declared: &Url) -> Option<String> {
        if declared.username().is_empty() && declared.password().is_none() {
            return None;
        }
        let user = percent_decode_str(declared.username()).decode_utf8_lossy();
        let password = match declared.password() {
            Some(password) => percent_decode_str(password).decode_utf8_lossy(),
            None => "".into(),
        };
        let credentials =
            base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
        Some(format!("Basic {credentials}"))
    }
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

    /// The client every request is sent through. It resolves names through the node's resolver,
    /// bounds each request by the client's `timeout_ms` where the client sets one, trusts and
    /// identifies itself with the client's TLS files, and sends through the declared proxy alone:
    /// a proxy named by the environment is never read. It follows no redirect: a `3xx` answer is
    /// the response, and a connector that follows redirects sends the next request itself.
    pub fn build(&self) -> Result<HttpClient, Report<HttpClientConfigError>> {
        let mut builder = self
            .builder()?
            .no_proxy()
            .redirect(redirect::Policy::none());
        if let Some(proxy) = self.proxy()? {
            let proxy = Proxy::all(proxy.declared).map_err(|source| {
                Report::new(source.without_url())
                    .change_context(HttpClientConfigError::InvalidProxy { label: self.label })
            })?;
            builder = builder.proxy(proxy);
        }
        builder.build().map_err(|source| {
            Report::new(source.without_url())
                .change_context(HttpClientConfigError::Build { label: self.label })
        })
    }

    /// The proxy the client declares with its `proxy` key, where it declares one.
    pub fn proxy(&self) -> Result<Option<DeclaredProxy>, Report<HttpClientConfigError>> {
        let Some(value) = optional_client_config_value(self.entries, PROXY) else {
            return Ok(None);
        };
        let proxy = DeclaredProxy::parse(value, self.label)?;
        Ok(Some(proxy))
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
    use nervix_primitives::net::TcpListener;
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

    use super::*;

    fn entry(key: &str, value: &str) -> ClientConfigEntry {
        ClientConfigEntry {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    /// Serves one connection on a loopback listener: reads one request head, answers it with
    /// `response`, and returns the head's lines, without their line endings.
    async fn answer_one_request(listener: TcpListener, response: &'static [u8]) -> Vec<String> {
        let (mut stream, _) = listener.accept().await.expect("one client connects");
        let mut request = tokio::io::BufReader::new(&mut stream);
        let mut lines = Vec::new();
        loop {
            let mut line = String::new();
            let read = request
                .read_line(&mut line)
                .await
                .expect("the request head is readable");
            assert!(read > 0, "the request ends with a complete head");
            if line == "\r\n" {
                break;
            }
            lines.push(line.trim_end_matches("\r\n").to_string());
        }
        drop(request);
        stream
            .write_all(response)
            .await
            .expect("the answer is writable");
        lines
    }

    #[nervix_primitives::test]
    async fn a_built_client_takes_a_redirect_as_its_answer_and_sends_nothing_to_its_location()
    -> Result<(), Report<DnsConfigurationError>> {
        let dns = DnsResolver::load(DnsConfiguration::system()).await?;
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("a loopback listener binds");
        let origin = listener.local_addr().expect("the listener has an address");
        let server = nervix_primitives::task::spawn(answer_one_request(
            listener,
            b"HTTP/1.1 302 Found\r\nLocation: /moved\r\nContent-Length: 0\r\nConnection: \
              close\r\n\r\n",
        ));
        let entries = [entry("timeout_ms", "5000")];
        let client = HttpClientConfig::new(&entries, "HTTP", &dns)
            .build()
            .expect("a client with a timeout builds");
        let response = client
            .get(format!("http://{origin}/events"))
            .send()
            .await
            .expect("the redirect is an answer");
        assert_eq!(response.status(), reqwest::StatusCode::FOUND);
        let request = server.await.expect("the origin served its one request");
        assert_eq!(request[0], "GET /events HTTP/1.1");
        Ok(())
    }

    #[nervix_primitives::test]
    async fn a_declared_proxy_carries_every_request_with_the_credentials_its_url_holds()
    -> Result<(), Report<DnsConfigurationError>> {
        let dns = DnsResolver::load(DnsConfiguration::system()).await?;
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("a loopback listener binds");
        let proxy = listener.local_addr().expect("the listener has an address");
        let server = nervix_primitives::task::spawn(answer_one_request(
            listener,
            b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n",
        ));
        let entries = [
            entry("timeout_ms", "5000"),
            entry("proxy", &format!("http://scenario:p%40ss@{proxy}")),
        ];
        let client = HttpClientConfig::new(&entries, "HTTP", &dns)
            .build()
            .expect("a client with a declared proxy builds");
        // Only the proxy can resolve the target's name, so the answer proves the request went
        // through it.
        let response = client
            .get("http://origin.invalid:8080/events?tenant=north")
            .send()
            .await
            .expect("the proxy answers");
        assert_eq!(response.status(), reqwest::StatusCode::NO_CONTENT);
        let request = server.await.expect("the proxy served its one request");
        assert_eq!(
            request[0],
            "GET http://origin.invalid:8080/events?tenant=north HTTP/1.1"
        );
        let expected = format!(
            "proxy-authorization: Basic {}",
            base64::engine::general_purpose::STANDARD.encode("scenario:p@ss")
        );
        assert!(
            request
                .iter()
                .any(|line| line.eq_ignore_ascii_case(&expected)),
            "the proxy receives the decoded credentials: {request:?}"
        );
        Ok(())
    }

    #[nervix_primitives::test]
    async fn a_declared_proxy_is_dialled_without_its_credentials_and_never_shows_them()
    -> Result<(), Report<DnsConfigurationError>> {
        let dns = DnsResolver::load(DnsConfiguration::system()).await?;
        let declared = [entry(
            "proxy",
            "https://egress%20user:se%3Acret@proxy.internal:3128",
        )];
        let proxy = HttpClientConfig::new(&declared, "HTTP", &dns)
            .proxy()
            .expect("an https proxy URL is valid")
            .expect("the client declares a proxy");
        assert_eq!(proxy.address().as_str(), "https://proxy.internal:3128/");
        assert_eq!(
            proxy.authorization(),
            Some(
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode("egress user:se:cret")
                )
                .as_str()
            )
        );
        let shown = format!("{proxy:?}");
        assert!(
            !shown.contains("se%3Acret") && !shown.contains("egress"),
            "{shown}"
        );

        let user_only = [entry("proxy", "http://reader@proxy.internal")];
        let user_only = HttpClientConfig::new(&user_only, "HTTP", &dns)
            .proxy()
            .expect("a proxy URL with a user is valid")
            .expect("the client declares a proxy");
        assert_eq!(
            user_only.authorization(),
            Some(
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode("reader:")
                )
                .as_str()
            )
        );

        let anonymous = [entry("proxy", "http://proxy.internal:3128")];
        let anonymous = HttpClientConfig::new(&anonymous, "HTTP", &dns)
            .proxy()
            .expect("a proxy URL without credentials is valid")
            .expect("the client declares a proxy");
        assert_eq!(anonymous.authorization(), None);

        assert!(
            HttpClientConfig::new(&[], "HTTP", &dns)
                .proxy()
                .expect("no proxy is valid")
                .is_none()
        );
        for invalid in [
            "socks5://proxy.internal:1080",
            "proxy.internal:3128",
            "file:///proxy",
        ] {
            let declared = [entry("proxy", invalid)];
            let refusal = HttpClientConfig::new(&declared, "HTTP", &dns)
                .proxy()
                .expect_err("only an http or https URL with a host names a proxy");
            assert!(matches!(
                refusal.current_context(),
                HttpClientConfigError::InvalidProxy { label: "HTTP" }
            ));
            assert!(!format!("{refusal:?}").contains(invalid), "{refusal:?}");
        }
        Ok(())
    }

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
