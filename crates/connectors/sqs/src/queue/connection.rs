//! The one client an SQS client's ingestors and emitters reach the service through, and what a
//! request that failed on the way reports.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** Reading the keys an SQS client declares into the one SDK client both directions
//!   build: its endpoint, region and static credentials, the `timeout_ms` that bounds each request,
//!   the TLS trust of its connections, and the node resolver each new connection resolves the
//!   endpoint host through. Also checking a CA file before Smithy reads it, and the description,
//!   and the typed lookup and deadline failures, of a request that failed.
//! - **Depends on.** The node resolver, the connector contract's client keys, deadlines and TLS
//!   failures, rustls's certificate parser, the AWS SDK's configuration loader and its Smithy HTTP
//!   client, and the SDK's request errors.
//! - **Must not know.** Queues, messages, request signing, or the host's retry policy.
//!
//! # Connections
//!
//! The client is the Smithy HTTP client the SDK itself builds, with the node resolver installed as
//! its connector's `ResolveDns` hook: one connector, and so one connection pool, for each timeout
//! setting, TLS with AWS-LC, and a new lookup for each new connection. It takes no proxy, from the
//! environment or anywhere else, because an SQS client declares none. The SDK signs a request for
//! the configured endpoint before the connector resolves its host, so the signature, the `Host`
//! header and the certificate check all name that host, whichever address accepted the connection.
//!
//! # Attempts and deadlines
//!
//! The SDK makes each request once: the host retries a failed one on the entity's `RETRY POLICY`,
//! so SDK retries would only multiply that policy. `timeout_ms` bounds the whole of each request,
//! its lookup and connection included, as both the operation and its one attempt. A receive asks
//! the service to hold it for a long poll before answering empty, so its deadline starts after
//! that wait.

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

use std::{error::Error, fmt::Debug, path::PathBuf, time::Duration};

use aws_config::{BehaviorVersion, retry::RetryConfig, timeout::TimeoutConfig};
use aws_credential_types::Credentials;
use aws_sdk_sqs::{
    Client as SqsClient,
    config::{Builder as SqsConfigBuilder, Region, SharedHttpClient},
    error::SdkError,
};
use aws_smithy_http_client::{
    Builder,
    tls::{Provider, TlsContext, TrustStore, rustls_provider::CryptoMode},
};
use error_stack::{Context, Report, ResultExt as _};
use meticulous::OptionExt as _;
use nervix_connector::{
    ConfiguredDeadline, DeadlineElapsed, TlsClientConfigError, client_config_value,
    client_tls_paths, optional_client_config_value, read_tls_file,
};
use nervix_dns::{DnsLookupError, DnsResolver};
use nervix_models::{ClientConfigEntry, ConnectorKind};
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, pem::PemObject as _};
use thiserror::Error;

/// The key that names a client's CA file.
const TLS_CA_FILE: &str = "tls_ca_file";

/// A key of an SQS client whose value its connections cannot use. The cause beneath says why.
#[derive(Debug, Error)]
#[error("invalid SQS client config key '{key}'")]
pub struct InvalidSqsClientKey {
    pub key: &'static str,
}

/// A CA file without a certificate. A client that trusts it alone could complete no handshake, so
/// it is a configuration failure rather than a trust store that every service fails.
#[derive(Debug, Error)]
#[error("TLS CA certificate '{path}' holds no certificate")]
pub struct EmptyCaFile {
    pub path: PathBuf,
}

/// The keys an SQS client's connections read, each checked once, before any client is built.
pub(crate) struct SqsClientConfig {
    endpoint: String,
    region: String,
    credentials: Credentials,
    timeout: Option<ConfiguredDeadline>,
    trust: SqsTrust,
}

impl SqsClientConfig {
    /// Reads `config` as every connection of the client reads it: its required endpoint, region and
    /// access key, the optional session token, `timeout_ms`, and the CA file it trusts, whose every
    /// certificate is parsed here.
    pub(crate) fn parse(config: &[ClientConfigEntry]) -> Result<Self, Report<InvalidSqsClientKey>> {
        let endpoint = Self::required(config, "endpoint")?;
        let region = Self::required(config, "region")?;
        let access_key_id = Self::required(config, "access_key_id")?;
        let secret_access_key = Self::required(config, "secret_access_key")?;
        let session_token = optional_client_config_value(config, "session_token");
        let timeout = ConfiguredDeadline::timeout(config)
            .change_context(InvalidSqsClientKey { key: "timeout_ms" })?;
        let trust = match client_tls_paths(config).ca_file {
            Some(ca_file) => SqsTrust::ca(ca_file)?,
            None => SqsTrust::Platform,
        };
        let credentials = Credentials::new(
            access_key_id,
            secret_access_key,
            session_token.map(ToOwned::to_owned),
            None,
            "nervix-sqs",
        );
        Ok(Self {
            endpoint,
            region,
            credentials,
            timeout,
            trust,
        })
    }

    fn required(
        config: &[ClientConfigEntry],
        key: &'static str,
    ) -> Result<String, Report<InvalidSqsClientKey>> {
        client_config_value(config, key, ConnectorKind::Sqs.keyword())
            .change_context(InvalidSqsClientKey { key })
    }

    /// The client every request of this configuration travels over, whose every new connection
    /// resolves the endpoint host through `dns`.
    ///
    /// The loader is handed the region, the credentials, the endpoint, the timeouts and the HTTP
    /// client, so it consults none of the default chains that could reach IMDS, ECS, STS or SSO
    /// over HTTP.
    pub(crate) async fn client(self, dns: DnsResolver) -> SqsServiceClient {
        let Self {
            endpoint,
            region,
            credentials,
            timeout,
            trust,
        } = self;
        // The SDK's own timeouts, its connect timeout among them, which `timeout_ms` adds the
        // operation's and its one attempt's to.
        let mut timeouts = aws_config::default_provider::timeout_config::default_provider()
            .timeout_config()
            .await;
        if let Some(timeout) = timeout {
            let bound = timeout.bound();
            timeouts = timeouts
                .to_builder()
                .operation_timeout(bound)
                .operation_attempt_timeout(bound)
                .build();
        }
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region))
            .endpoint_url(endpoint)
            .retry_config(RetryConfig::disabled())
            .timeout_config(timeouts.clone())
            .credentials_provider(credentials)
            .http_client(trust.http_client(dns))
            .load()
            .await;
        SqsServiceClient {
            client: SqsClient::new(&sdk_config),
            timeouts,
            timeout,
        }
    }
}

/// The SDK client every request of one SQS client travels over, with the timeouts it was built
/// with and the deadline its `timeout_ms` sets, if it sets one.
#[derive(Clone, Debug)]
pub(crate) struct SqsServiceClient {
    client: SqsClient,
    /// What bounds each request of the client, which a receive extends by its long poll.
    timeouts: TimeoutConfig,
    timeout: Option<ConfiguredDeadline>,
}

impl SqsServiceClient {
    pub(crate) fn sdk(&self) -> &SqsClient {
        &self.client
    }

    /// The configuration a receive overrides its client's with: the client's own timeouts, with
    /// `timeout_ms` counted from the end of the `long_poll` the receive asks the service to hold it
    /// for. A client without `timeout_ms` overrides nothing.
    pub(crate) fn receive_override(&self, long_poll: Duration) -> SqsConfigBuilder {
        let mut config = SqsConfigBuilder::default();
        let Some(timeout) = self.timeout else {
            return config;
        };
        let bound = long_poll.checked_add(timeout.bound()).assured(
            "a long poll of seconds and a timeout_ms the deadline rule bounds to about 146 years \
             fit a duration",
        );
        let timeouts = self
            .timeouts
            .to_builder()
            .operation_timeout(bound)
            .operation_attempt_timeout(bound)
            .build();
        config.set_timeout_config(Some(timeouts));
        config
    }

    /// `error`, which ended `request`, as its report describes it.
    pub(crate) fn failed<'a, E, R>(
        &self,
        request: SqsRequest,
        error: &'a SdkError<E, R>,
    ) -> FailedRequest<'a>
    where
        E: Error + 'static,
        R: Debug + 'static,
    {
        let elapsed = match error {
            SdkError::TimeoutError(_) => self
                .timeout
                .map(|timeout| timeout.elapsed(request.waited())),
            _ => None,
        };
        FailedRequest::new(request, error, elapsed)
    }
}

/// What an SQS client's connections trust.
pub(crate) enum SqsTrust {
    /// A client without `tls_ca_file`: the platform's native roots, as the SDK's own default
    /// client.
    Platform,
    /// A client with `tls_ca_file`: that CA chain alone.
    Ca(TlsContext),
}

impl SqsTrust {
    /// Trust in the CA chain of the file at `ca_file` alone.
    ///
    /// Smithy's TLS context parses a CA chain only when it builds a connector, and panics on a
    /// section it cannot parse or a certificate rustls does not take as a trust anchor. Every
    /// certificate is therefore parsed here first, with the same parser and root store, so a file
    /// Smithy would refuse fails the configuration instead.
    pub(crate) fn ca(ca_file: PathBuf) -> Result<Self, Report<InvalidSqsClientKey>> {
        let invalid = || InvalidSqsClientKey { key: TLS_CA_FILE };
        let ca_pem = read_tls_file(&ca_file, "TLS CA certificate").change_context_lazy(invalid)?;
        let mut anchors = RootCertStore::empty();
        for certificate in CertificateDer::pem_slice_iter(&ca_pem) {
            let certificate = certificate
                .map_err(|source| {
                    Report::new(TlsClientConfigError::ParseCaCertificate {
                        path: ca_file.clone(),
                    })
                    .attach_printable(source.to_string())
                })
                .change_context_lazy(invalid)?;
            anchors
                .add(certificate)
                .map_err(|source| {
                    Report::new(TlsClientConfigError::AddCaCertificate {
                        path: ca_file.clone(),
                    })
                    .attach_printable(source.to_string())
                })
                .change_context_lazy(invalid)?;
        }
        if anchors.is_empty() {
            return Err(Report::new(EmptyCaFile { path: ca_file }).change_context(invalid()));
        }
        let trust_store = TrustStore::empty().with_pem_certificate(ca_pem);
        let context = TlsContext::builder()
            .with_trust_store(trust_store)
            .build()
            .change_context_lazy(invalid)?;
        Ok(Self::Ca(context))
    }

    /// The HTTP client every request of the SQS client travels over, whose every new connection
    /// resolves the endpoint host through `dns` and takes no proxy.
    fn http_client(self, dns: DnsResolver) -> SharedHttpClient {
        let builder = Builder::new().tls_provider(Provider::Rustls(CryptoMode::AwsLc));
        match self {
            Self::Platform => builder.build_with_resolver(dns),
            Self::Ca(tls_context) => builder.tls_context(tls_context).build_with_resolver(dns),
        }
    }
}

/// A request an SQS client sends, as the report of its failure names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqsRequest {
    QueueLookup,
    Receive,
    DeleteBatch,
    ChangeVisibilityBatch,
    Send,
    SendBatch,
}

impl SqsRequest {
    /// The operation a failure's description leads with.
    fn failure(self) -> &'static str {
        match self {
            Self::QueueLookup => "SQS GetQueueUrl failed",
            Self::Receive => "SQS ReceiveMessage failed",
            Self::DeleteBatch => "SQS DeleteMessageBatch failed",
            Self::ChangeVisibilityBatch => "SQS ChangeMessageVisibilityBatch failed",
            Self::Send => "SQS SendMessage failed",
            Self::SendBatch => "SQS SendMessageBatch failed",
        }
    }

    /// The wait the client's `timeout_ms` bounds, as a deadline that ran out names it.
    fn waited(self) -> &'static str {
        match self {
            Self::QueueLookup => "the SQS queue lookup",
            Self::Receive => "an SQS receive after its long poll",
            Self::DeleteBatch => "an SQS batch delete",
            Self::ChangeVisibilityBatch => "an SQS visibility change",
            Self::Send => "an SQS send",
            Self::SendBatch => "an SQS batch send",
        }
    }
}

/// One SQS request that failed, as its report describes it.
pub(crate) struct FailedRequest<'a> {
    request: SqsRequest,
    error: &'a (dyn Error + 'static),
    /// Whether the request failed before any response of the service arrived: connecting to it,
    /// resolving its host included, or a timeout.
    unanswered: bool,
    /// The client's `timeout_ms`, when running out of it is what ended the request.
    elapsed: Option<DeadlineElapsed>,
}

impl<'a> FailedRequest<'a> {
    fn new<E, R>(
        request: SqsRequest,
        error: &'a SdkError<E, R>,
        elapsed: Option<DeadlineElapsed>,
    ) -> Self
    where
        E: Error + 'static,
        R: Debug + 'static,
    {
        let unanswered = matches!(
            error,
            SdkError::DispatchFailure(_) | SdkError::TimeoutError(_)
        );
        Self {
            request,
            error,
            unanswered,
            elapsed,
        }
    }

    /// The failed lookup of the endpoint host, when resolving it is what failed the request.
    pub(crate) fn lookup_failure(&self) -> Option<&'a DnsLookupError> {
        DnsLookupError::find_in(self.error)
    }

    /// The failure described for an operator.
    ///
    /// A request no response answered is described by the error and every cause under it, which
    /// describe the connection, never a message, and carry no credentials. A response from the
    /// service keeps the SDK's own short description, because the service's message can repeat
    /// what the request carried.
    #[cfg_attr(
        nervix_lint,
        nervix::dispatch(reason = "formats the typed external AWS SDK request failure")
    )]
    pub(crate) fn description(&self) -> String {
        let mut description = self.error.to_string();
        if !self.unanswered {
            return description;
        }
        let mut cause = self.error.source();
        while let Some(current) = cause {
            description.push_str(": ");
            description.push_str(&current.to_string());
            cause = current.source();
        }
        description
    }

    /// A report of `context` for this failure, whose message leads with the request. A failed
    /// lookup of the endpoint host, and a `timeout_ms` the request ran out of, stay the report's
    /// typed cause, and its message names the host or the deadline.
    pub(crate) fn report<C: Context>(&self, context: C) -> Report<C> {
        let operation = self.request.failure();
        if let Some(lookup) = self.lookup_failure() {
            return Report::new(lookup.clone())
                .change_context(context)
                .attach_printable(format!("{operation}: {lookup}"));
        }
        if let Some(elapsed) = self.elapsed {
            return Report::new(elapsed)
                .change_context(context)
                .attach_printable(format!("{operation}: {elapsed}"));
        }
        Report::new(context).attach_printable(format!("{operation}: {}", self.description()))
    }
}
