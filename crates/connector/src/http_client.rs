//! The shared settings an HTTP-speaking connector builds its client from.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Building an HTTP client from a client's entries: the node DNS resolver, the
//!   request deadline its `timeout_ms` sets, the shared TLS trust and client identity, the
//!   one proxy the client declares and the credentials it carries, refusing every redirect so the
//!   connector that sends decides what a redirect means, sending no request again after a refusal
//!   so the connector's retry policy is the only retry, and never reading a proxy from the
//!   environment.
//! - **Depends on.** The client configuration's entries, the shared rustls configuration owner,
//!   `nervix-dns`, URLs, Base64 and percent decoding, and `reqwest`.
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
use reqwest::{Client as HttpClient, Proxy, redirect};
use thiserror::Error;
use url::Url;

use crate::{
    client_config::optional_client_config_value, client_deadline::ConfiguredDeadline,
    tls::RustlsClientConfigProvider,
};

/// The configuration key that names a client's proxy.
const PROXY: &str = "proxy";

#[derive(Debug, Error)]
pub enum HttpClientConfigError {
    #[error("invalid {label} timeout_ms")]
    InvalidTimeout { label: &'static str },
    #[error("failed to configure {label} TLS")]
    Tls { label: &'static str },
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
    policy: &'a nervix_tls::OutwardTlsPolicy,
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
        policy: &'a nervix_tls::OutwardTlsPolicy,
    ) -> Self {
        Self {
            entries,
            label,
            dns,
            policy,
        }
    }

    /// The client every request is sent through. It resolves names through the node's resolver,
    /// bounds each request by the client's `timeout_ms` where the client sets one, trusts and
    /// identifies itself with the client's TLS files, and sends through the declared proxy alone:
    /// a proxy named by the environment is never read. It follows no redirect: a `3xx` answer is
    /// the response, and a connector that follows redirects sends the next request itself.
    ///
    /// It sends each request once. A request the server refuses, whatever the protocol says about
    /// sending it again, fails, and the connector's retry policy decides about the next attempt.
    /// The connection pool beneath still writes a request on a fresh connection when the reused
    /// connection it chose closed before any of the request was written; that request never
    /// reached the server, and Reqwest offers no way to turn the pool's choice off.
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
        // Reqwest's default policy sends a request again after an HTTP/2 refusal, `REFUSED_STREAM`
        // or a graceful `GOAWAY`, on a schedule the connector never sees. The refusal is the
        // attempt's failure instead, which the connector's own retry policy paces.
        let mut builder = HttpClient::builder()
            .dns_resolver(self.dns.clone())
            .retry(reqwest::retry::never());
        let timeout = ConfiguredDeadline::timeout(self.entries)
            .change_context(HttpClientConfigError::InvalidTimeout { label: self.label })?;
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout.bound());
        }

        let tls = RustlsClientConfigProvider::new(self.entries, self.policy)
            .build_with_default_roots()
            .change_context(HttpClientConfigError::Tls { label: self.label })?;
        let mut tls = (*tls).clone();
        // Reqwest preserves a preconfigured client's ALPN instead of setting its HTTP defaults.
        tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(builder.tls_backend_preconfigured(tls))
    }
}

#[cfg(test)]
#[cfg_attr(
    nervix_lint,
    nervix::layer(harness, reason = "test module remains outside product execution")
)]
mod tests {
    use meticulous::{OptionExt as _, ResultExt as _};
    use nervix_dns::{DnsConfiguration, DnsConfigurationError};
    use nervix_models::ClientConfigEntry;
    use nervix_primitives::{net::TcpListener, sync::StdArc};
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
        let dns = DnsResolver::load(
            DnsConfiguration::system(),
            nervix_dns::DnsSettings::default(),
        )
        .await?;
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
        let client = HttpClientConfig::new(&entries, "HTTP", &dns, &Default::default())
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
        let dns = DnsResolver::load(
            DnsConfiguration::system(),
            nervix_dns::DnsSettings::default(),
        )
        .await?;
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
        let client = HttpClientConfig::new(&entries, "HTTP", &dns, &Default::default())
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
        let dns = DnsResolver::load(
            DnsConfiguration::system(),
            nervix_dns::DnsSettings::default(),
        )
        .await?;
        let declared = [entry(
            "proxy",
            "https://egress%20user:se%3Acret@proxy.internal:3128",
        )];
        let proxy = HttpClientConfig::new(&declared, "HTTP", &dns, &Default::default())
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
        let user_only = HttpClientConfig::new(&user_only, "HTTP", &dns, &Default::default())
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
        let anonymous = HttpClientConfig::new(&anonymous, "HTTP", &dns, &Default::default())
            .proxy()
            .expect("a proxy URL without credentials is valid")
            .expect("the client declares a proxy");
        assert_eq!(anonymous.authorization(), None);

        assert!(
            HttpClientConfig::new(&[], "HTTP", &dns, &Default::default())
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
            let refusal = HttpClientConfig::new(&declared, "HTTP", &dns, &Default::default())
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

    /// A certificate authority, written to a file for a client's `tls_ca_file`, and the TLS
    /// configuration of an endpoint whose certificate it issued for `127.0.0.1`. The endpoint offers
    /// HTTP/2 alone, so a client that connects speaks HTTP/2.
    struct Http2Endpoint {
        _files: tempfile::TempDir,
        ca: std::path::PathBuf,
        tls: StdArc<rustls::ServerConfig>,
    }

    impl Http2Endpoint {
        fn issue() -> Self {
            let authority_key = rcgen::KeyPair::generate().assured("rcgen generates keys");
            let mut authority = rcgen::CertificateParams::new(Vec::<String>::new())
                .assured("an authority needs no name");
            authority.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let authority = authority
                .self_signed(&authority_key)
                .assured("an authority signs itself");
            let server_key = rcgen::KeyPair::generate().assured("rcgen generates keys");
            let server = rcgen::CertificateParams::new(vec!["127.0.0.1".to_string()])
                .assured("a loopback address is a valid subject name")
                .signed_by(&server_key, &authority, &authority_key)
                .assured("the authority issues the server certificate");
            let files = tempfile::tempdir().assured("the test host has a temporary directory");
            let ca = files.path().join("ca.pem");
            std::fs::write(&ca, authority.pem()).assured("the temporary directory is writable");
            let chain = vec![server.der().clone()];
            let key = rustls_pki_types::PrivateKeyDer::Pkcs8(
                rustls_pki_types::PrivatePkcs8KeyDer::from(server_key.serialize_der()),
            );
            nervix_crypto::tls::install_default_provider();
            let mut tls = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(chain, key)
                .assured("the issued chain matches its key");
            tls.alpn_protocols = vec![b"h2".to_vec()];
            Self {
                _files: files,
                ca,
                tls: StdArc::new(tls),
            }
        }
    }

    /// Serves one HTTPS connection over HTTP/2: refuses its first stream with `REFUSED_STREAM`,
    /// the refusal RFC 9113 section 8.7 lets a client send again because the endpoint processed
    /// nothing, answers every later stream with `204 No Content`, and returns how many streams the
    /// client opened once it closes the connection.
    async fn refuse_the_first_stream(
        listener: TcpListener,
        tls: StdArc<rustls::ServerConfig>,
    ) -> usize {
        let (stream, _) = listener.accept().await.assured("the client connects");
        let stream = tokio_rustls::TlsAcceptor::from(tls)
            .accept(stream)
            .await
            .assured("the client completes TLS");
        let mut connection = h2::server::handshake(stream)
            .await
            .assured("the client speaks HTTP/2");
        let mut streams = 0_usize;
        while let Some(accepted) = connection.accept().await {
            nervix_primitives::task::consume_budget().await;
            let Ok((_request, mut respond)) = accepted else {
                break;
            };
            streams = streams
                .checked_add(1)
                .assured("one connection opens far fewer streams than usize counts");
            if streams == 1 {
                respond.send_reset(h2::Reason::REFUSED_STREAM);
                continue;
            }
            let answer = http::Response::builder()
                .status(http::StatusCode::NO_CONTENT)
                .body(())
                .assured("a status alone is a valid response head");
            respond
                .send_response(answer, true)
                .assured("the client waits for the answer to the stream it opened");
        }
        streams
    }

    #[nervix_primitives::test]
    async fn a_built_client_hands_a_refused_http2_stream_to_its_caller_without_sending_it_again()
    -> Result<(), Report<DnsConfigurationError>> {
        let dns = DnsResolver::load(
            DnsConfiguration::system(),
            nervix_dns::DnsSettings::default(),
        )
        .await?;
        let endpoint = Http2Endpoint::issue();
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .assured("a loopback listener binds");
        let address = listener.local_addr().assured("the listener has an address");
        let server =
            nervix_primitives::task::spawn(refuse_the_first_stream(listener, endpoint.tls.clone()));
        let entries = [
            entry("timeout_ms", "5000"),
            entry("tls_ca_file", &endpoint.ca.to_string_lossy()),
        ];
        let client = HttpClientConfig::new(&entries, "HTTP", &dns, &Default::default())
            .build()
            .assured("a client that trusts the endpoint's authority builds");

        let outcome = client.get(format!("https://{address}/events")).send().await;
        let refused = outcome.is_err();
        let described = format!("{outcome:?}");
        drop(outcome);
        drop(client);
        let streams = server
            .await
            .assured("the endpoint served its one connection");

        assert!(
            refused,
            "the refused request is its caller's to send again: {described}"
        );
        assert_eq!(streams, 1, "the client sends a refused request once");
        Ok(())
    }

    #[nervix_primitives::test]
    async fn http_client_validates_timeout_configuration()
    -> Result<(), Report<DnsConfigurationError>> {
        let dns = DnsResolver::load(
            DnsConfiguration::system(),
            nervix_dns::DnsSettings::default(),
        )
        .await?;
        let client = HttpClientConfig::new(
            &[ClientConfigEntry {
                key: "timeout_ms".to_string(),
                value: "250".to_string(),
            }],
            "HTTP",
            &dns,
            &Default::default(),
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
            &Default::default(),
        )
        .build()
        .expect_err("invalid timeout");
        assert!(err.to_string().contains("invalid HTTP timeout_ms"));
        Ok(())
    }

    #[nervix_primitives::test]
    async fn http_client_builds_without_native_trust_anchors()
    -> Result<(), Report<DnsConfigurationError>> {
        const CHILD: &str = "NERVIX_TEST_HTTP_EMPTY_TRUST_STORE";
        if std::env::var_os(CHILD).is_some() {
            let dns = DnsResolver::load(
                DnsConfiguration::system(),
                nervix_dns::DnsSettings::default(),
            )
            .await?;
            let entries = [entry("timeout_ms", "5000")];
            HttpClientConfig::new(&entries, "HTTP", &dns, &Default::default())
                .build()
                .expect("the bundled roots suffice when native roots are empty");
            return Ok(());
        }

        // Root-store discovery reads process environment. Isolate those paths in this one child,
        // never in the test process that runs alongside the other connector tests.
        let directory = tempfile::tempdir().expect("the trust-store fixture is writable");
        let certificate_file = directory.path().join("empty.pem");
        let certificate_directory = directory.path().join("empty-roots");
        std::fs::write(&certificate_file, b"").expect("the empty certificate file is writable");
        std::fs::create_dir(&certificate_directory).expect("the empty root directory is writable");
        let executable = std::env::current_exe().expect("the running test has an executable");
        let mut child = tokio::process::Command::new(executable);
        child
            .args([
                "--exact",
                "http_client::tests::http_client_builds_without_native_trust_anchors",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("SSL_CERT_FILE", certificate_file)
            .env("SSL_CERT_DIR", certificate_directory)
            .kill_on_drop(true);
        let output =
            nervix_primitives::time::timeout(std::time::Duration::from_secs(60), child.output())
                .await
                .expect("the isolated trust-store test finishes within its supervision bound")
                .expect("the child test starts");
        assert!(
            output.status.success(),
            "empty native roots must not prevent HTTP client construction: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    #[nervix_primitives::test]
    async fn http_client_keeps_the_shared_tls_configuration_cause()
    -> Result<(), Report<DnsConfigurationError>> {
        let dns = DnsResolver::load(
            DnsConfiguration::system(),
            nervix_dns::DnsSettings::default(),
        )
        .await?;
        let directory = tempfile::tempdir().expect("the TLS fixture directory is writable");
        let ca = directory.path().join("invalid-ca.pem");
        std::fs::write(
            &ca,
            b"-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n",
        )
        .expect("the malformed CA fixture is writable");
        let entries = [entry("tls_ca_file", &ca.to_string_lossy())];
        let error = HttpClientConfig::new(&entries, "HTTP", &dns, &Default::default())
            .build()
            .expect_err("a malformed trust anchor must refuse the client");
        let cause = error
            .downcast_ref::<crate::TlsClientConfigError>()
            .expect("HTTP clients retain the shared TLS owner's typed failure");
        assert!(matches!(
            cause,
            crate::TlsClientConfigError::ParseCaCertificate { path } if path == &ca
        ));
        Ok(())
    }

    #[nervix_primitives::test]
    async fn http_client_classifies_invalid_tls_material()
    -> Result<(), Report<DnsConfigurationError>> {
        let dns = DnsResolver::load(
            DnsConfiguration::system(),
            nervix_dns::DnsSettings::default(),
        )
        .await?;
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
        let ca = HttpClientConfig::new(&ca_entries, "HTTP", &dns, &Default::default())
            .build()
            .expect_err("an invalid CA certificate must fail");
        assert!(matches!(
            ca.current_context(),
            HttpClientConfigError::Tls { label: "HTTP" }
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
        let identity = HttpClientConfig::new(&identity_entries, "HTTP", &dns, &Default::default())
            .build()
            .expect_err("an invalid client identity must fail");
        assert!(matches!(
            identity.current_context(),
            HttpClientConfigError::Tls { label: "HTTP" }
        ));
        Ok(())
    }
}
