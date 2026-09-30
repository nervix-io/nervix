//! Where this node's traces and logs go.
//!
//! Layer: edges.
//!
//! - **Owns.** The subscriber the process installs, the OTLP exporter it may add, and the guard
//!   that flushes them on shutdown.
//! - **Depends on.** The command-line arguments, the node resolver and telemetry transport hooks.
//! - **Must not know.** What any other module records.

use std::{
    fs::OpenOptions,
    future::Future,
    io,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use error_stack::{Report, ResultExt};
use hyper_util::client::legacy::connect::HttpConnector;
use meticulous::OptionExt as _;
use nervix_dns::DnsResolver;
use nervix_primitives::{
    sync::{blocking::Mutex as ParkingMutex, watch},
    time::timeout,
};
use nervix_recovery::{Discarded, Reported};
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    trace::{Sampler, SdkTracerProvider},
};
use tower_service::Service;
use tracing_subscriber::{
    EnvFilter, fmt, fmt::writer::BoxMakeWriter, prelude::__tracing_subscriber_SubscriberExt,
    util::SubscriberInitExt,
};
use triomphe::Arc;

use super::{AppError, Args};

const DEFAULT_TRACE_FILTER: &str =
    "info,nervix=info,registry=info,openraft::core::heartbeat::worker=error,\
     openraft::replication=error,openraft::engine::handler::replication_handler=error";

pub struct TracingGuard {
    traces: Option<TraceExportGuard>,
}

struct TraceExportGuard {
    tracer_provider: SdkTracerProvider,
    dns: watch::Sender<Option<DnsResolver>>,
}

impl TracingGuard {
    pub(super) fn install_dns(&self, dns: &DnsResolver) {
        if let Some(traces) = &self.traces {
            traces.dns.send_replace(Some(dns.clone()));
        }
    }
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if let Some(traces) = self.traces.take() {
            // Startup may fail before publishing DNS. Release that wait before flushing the SDK.
            drop(traces.dns);
            traces
                .tracer_provider
                .shutdown()
                .reported("flushing the tracer provider on shutdown");
        }
    }
}

/// A lazy collector connection waits for the one resolver loaded by node startup. Its connection
/// budget begins after installation, so a slow startup does not discard its early queued spans.
#[derive(Clone)]
struct NodeTraceConnector {
    receiver: watch::Receiver<Option<DnsResolver>>,
    timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
enum TraceConnectError {
    #[error("node startup ended before installing the trace export resolver")]
    Unavailable,
    #[error("trace export connection exceeded its {timeout:?} timeout")]
    Timeout { timeout: Duration },
    #[error("trace export connection failed")]
    Connect(#[source] <HttpConnector<DnsResolver> as Service<http::Uri>>::Error),
}

impl Service<http::Uri> for NodeTraceConnector {
    type Response = <HttpConnector<DnsResolver> as Service<http::Uri>>::Response;
    type Error = TraceConnectError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let mut receiver = self.receiver.clone();
        let connection_timeout = self.timeout;
        Box::pin(async move {
            let dns = {
                let installed = receiver
                    .wait_for(Option::is_some)
                    .await
                    .map_err(|_| TraceConnectError::Unavailable)?;
                installed
                    .as_ref()
                    .verified("wait_for returns only an installed resolver")
                    .clone()
            };
            let mut connector = HttpConnector::new_with_resolver(dns);
            connector.enforce_http(false);
            connector.set_nodelay(true);
            connector.set_connect_timeout(Some(connection_timeout));
            match timeout(connection_timeout, connector.call(uri)).await {
                Ok(connection) => connection.map_err(TraceConnectError::Connect),
                Err(_) => Err(TraceConnectError::Timeout {
                    timeout: connection_timeout,
                }),
            }
        })
    }
}

struct TraceExporter {
    exporter: opentelemetry_otlp::SpanExporter,
    dns: watch::Sender<Option<DnsResolver>>,
}

impl TraceExporter {
    fn new(args: &Args) -> Result<Self, Report<AppError>> {
        let traces_endpoint =
            std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_TRACES_ENDPOINT).ok();
        let general_endpoint = std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_ENDPOINT).ok();
        let endpoint = Self::endpoint_from(
            &args.otel_otlp_endpoint,
            [traces_endpoint.as_deref(), general_endpoint.as_deref()],
        );
        let traces_timeout =
            std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_TRACES_TIMEOUT).ok();
        let general_timeout = std::env::var(opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT).ok();
        let timeout = Self::timeout_from([traces_timeout.as_deref(), general_timeout.as_deref()]);
        Self::from_endpoint(endpoint, timeout)
    }

    fn endpoint_from(configured: &str, variables: [Option<&str>; 2]) -> String {
        // Preserve the SDK's endpoint precedence at its external configuration boundary.
        if !configured.is_empty() {
            return configured.to_string();
        }
        for variable in variables {
            let Some(endpoint) = variable else {
                continue;
            };
            return endpoint.to_string();
        }
        "http://localhost:4317".to_string()
    }

    fn timeout_from(variables: [Option<&str>; 2]) -> Duration {
        for variable in variables {
            if let Some(value) = variable
                && let Ok(milliseconds) = value.parse::<u64>()
            {
                return Duration::from_millis(milliseconds);
            }
        }
        opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT
    }

    fn from_endpoint(endpoint: String, timeout: Duration) -> Result<Self, Report<AppError>> {
        let endpoint = otel_tonic::transport::Endpoint::from_shared(endpoint)
            .change_context(AppError::InitTracing)?
            .timeout(timeout);
        let (dns, receiver) = watch::channel(None);
        let channel =
            endpoint.connect_with_connector_lazy(NodeTraceConnector { receiver, timeout });
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_channel(channel)
            .with_timeout(timeout)
            .build()
            .change_context(AppError::InitTracing)?;
        Ok(Self { exporter, dns })
    }
}

pub fn init_tracing(args: &Args) -> Result<TracingGuard, Report<AppError>> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_TRACE_FILTER));
    let fmt_layer = fmt::layer().with_ansi(false);

    if args.otel_enabled {
        let trace = TraceExporter::new(args)?;
        let resource = Resource::builder()
            .with_service_name(args.otel_service_name.clone())
            .build();
        let tracer_provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                args.otel_trace_sample_ratio,
            ))))
            .with_resource(resource)
            .with_batch_exporter(trace.exporter)
            .build();
        let tracer = tracer_provider.tracer("nervix");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .with(otel_layer)
            .try_init()
            .change_context(AppError::InitTracing)?;

        Ok(TracingGuard {
            traces: Some(TraceExportGuard {
                tracer_provider,
                dns: trace.dns,
            }),
        })
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .try_init()
            .change_context(AppError::InitTracing)?;

        Ok(TracingGuard { traces: None })
    }
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]
#[path = "tracing_setup_tests.rs"]
mod tests;

#[derive(Clone)]
struct SharedFileWriter(Arc<ParkingMutex<std::fs::File>>);

impl io::Write for SharedFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().flush()
    }
}

/// What a scenario run records beyond the node's own default.
///
/// The interconnect reports why a connection attempt failed at `debug`: a refused dial, a setup
/// deadline, a rejected handshake. A scenario that fails on "peer never became connected" is
/// undiagnosable without those lines — the cluster status only says the peer is unavailable, not
/// what went wrong reaching it — and the failures that need them appear under whole-suite load,
/// where re-running the feature alone does not reproduce them. They are per connection event
/// rather than per message, so keeping them on costs a handful of lines per scenario.
const TEST_TRACE_FILTER: &str = "nervix_interconnect=debug";

pub fn init_tracing_to_file(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let file = Arc::new(ParkingMutex::new(file));
    let make_writer = BoxMakeWriter::new(move || SharedFileWriter(file.clone()));
    fmt()
        .with_ansi(false)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            EnvFilter::new(format!("{DEFAULT_TRACE_FILTER},{TEST_TRACE_FILTER}"))
        }))
        .with_writer(make_writer)
        .try_init()
        .discarded(
            "the first call in this process installed the subscriber this one would replace",
        );
    Ok(())
}
