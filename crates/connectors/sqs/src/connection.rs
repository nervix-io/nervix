//! How an SQS client reaches the service, and what a request that failed on the way reports.
//!
//! Layer: engines and infrastructure.
//!
//! - **Owns.** The HTTP client every request of one SQS client travels over: the TLS trust and
//!   proxy of its connections, and the node resolver each new connection resolves the endpoint
//!   host through. Also the description, and the typed lookup failure, of a request that failed.
//! - **Depends on.** The node resolver, the Smithy HTTP client, and the SDK's request errors.
//! - **Must not know.** Queues, messages, request signing, or the host's retry policy.
//!
//! # Connections
//!
//! The client is the Smithy HTTP client the SDK itself builds, with the node resolver installed as
//! its connector's `ResolveDns` hook: one connector, and so one connection pool, for each timeout
//! setting, TLS with AWS-LC, and a new lookup for each new connection. The SDK signs a request for
//! the configured endpoint before the connector resolves its host, so the signature, the `Host`
//! header and the certificate check all name that host, whichever address accepted the connection.

use std::{error::Error, fmt::Debug};

use aws_sdk_sqs::{config::SharedHttpClient, error::SdkError};
use aws_smithy_http_client::{
    Builder, Connector, HttpClientError,
    proxy::ProxyConfig,
    tls::{Provider, TlsContext, TrustStore, rustls_provider::CryptoMode},
};
use error_stack::{Context, Report};
use nervix_dns::{DnsLookupError, DnsResolver};

/// What an SQS client's connections trust, and the proxy they take.
pub(crate) enum SqsTrust {
    /// A client without `tls_ca_file`: the platform's native roots, and the proxy the
    /// `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` environment variables name, exactly as the SDK's
    /// own default client.
    Platform,
    /// A client with `tls_ca_file`: that CA chain alone, and no proxy.
    Ca(TlsContext),
}

impl SqsTrust {
    /// Trust in the PEM CA chain `ca_pem` alone.
    pub(crate) fn ca(ca_pem: Vec<u8>) -> Result<Self, HttpClientError> {
        let trust_store = TrustStore::empty().with_pem_certificate(ca_pem);
        let context = TlsContext::builder()
            .with_trust_store(trust_store)
            .build()?;
        Ok(Self::Ca(context))
    }

    /// The HTTP client every request of the SQS client travels over, whose every new connection
    /// resolves the endpoint host through `dns`.
    pub(crate) fn http_client(self, dns: DnsResolver) -> SharedHttpClient {
        let (tls_context, proxy) = match self {
            Self::Platform => (TlsContext::default(), ProxyConfig::from_env()),
            Self::Ca(tls_context) => (tls_context, ProxyConfig::disabled()),
        };
        // The SDK builds its own default client the same way, which is also the only way to set a
        // proxy: the client keeps the connector built for each timeout setting and reuses its pool.
        Builder::new().build_with_connector_fn(move |settings, components| {
            let mut connector = Connector::builder()
                .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
                .tls_context(tls_context.clone());
            connector.set_connector_settings(settings.cloned());
            if let Some(components) = components {
                connector.set_sleep_impl(components.sleep_impl());
            }
            connector.set_proxy_config(Some(proxy.clone()));
            connector.build_with_resolver(dns.clone())
        })
    }
}

/// One SQS request that failed, as its report describes it.
pub(crate) struct FailedRequest<'a> {
    error: &'a (dyn Error + 'static),
    /// Whether the request failed before any response of the service arrived: connecting to it,
    /// resolving its host included, or a timeout.
    unanswered: bool,
}

impl<'a> FailedRequest<'a> {
    pub(crate) fn new<E, R>(error: &'a SdkError<E, R>) -> Self
    where
        E: Error + 'static,
        R: Debug + 'static,
    {
        let unanswered = matches!(
            error,
            SdkError::DispatchFailure(_) | SdkError::TimeoutError(_)
        );
        Self { error, unanswered }
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

    /// A report of `context` for this failure, whose message leads with `operation`. A failed
    /// lookup of the endpoint host stays the report's typed cause, and its message names the host.
    pub(crate) fn report<C: Context>(&self, context: C, operation: &str) -> Report<C> {
        match self.lookup_failure() {
            Some(lookup) => Report::new(lookup.clone())
                .change_context(context)
                .attach_printable(format!("{operation}: {lookup}")),
            None => Report::new(context)
                .attach_printable(format!("{operation}: {}", self.description())),
        }
    }
}
