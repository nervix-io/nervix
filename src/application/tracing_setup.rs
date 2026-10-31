//! Where this node's traces and logs go.
//!
//! Layer: edges.
//!
//! - **Owns.** The subscriber the process installs and the filter that decides what it records,
//!   including the dependency events no filter may select, the OTLP exporter it may add, the guard
//!   that flushes them on shutdown, and the trace sample ratio setting the exporter samples with.
//! - **Depends on.** The tracing command-line arguments, the node resolver and telemetry transport
//!   hooks.
//! - **Must not know.** What any other module records.

#![cfg_attr(
    nervix_lint,
    nervix::layer(edge, reason = "owns the edges boundary declared above")
)]

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
use nervix_models::{NodeCapabilities, Setting, SettingDefinition};
use nervix_primitives::{
    sync::{Arc, blocking::Mutex as ParkingMutex, watch},
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
use tracing::{Level, Metadata, Subscriber};
use tracing_subscriber::{
    EnvFilter, Layer, filter::filter_fn, fmt, fmt::writer::BoxMakeWriter,
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt,
};

use super::{AppError, TracingArgs};

const DEFAULT_TRACE_FILTER: &str =
    "info,nervix=info,registry=info,openraft::core::heartbeat::worker=error,\
     openraft::replication=error,openraft::engine::handler::replication_handler=error";

/// What the node's log records: what its trace filter selects, except a dependency event that
/// carries a record's payload, which no filter can select.
///
/// The trace filter is the operator's `RUST_LOG`, or the default when it is unset or does not
/// parse. The payload rule is a filter of its own beneath it, not a directive added to it: a
/// field-qualified directive outranks a plain one for the same target, and a span-scoped directive
/// enables everything inside its span, but neither reaches an event the layer beneath refused.
///
/// Lapin logs every message a broker returns at `warn`, with the whole message, its body and
/// properties included, inside its `io_loop` span. A RabbitMQ emitter that publishes mandatory
/// messages gets back every record its broker cannot route, and reports each return itself,
/// naming the broker's reply without the record. Lapin's `debug` and `trace` events about returned
/// messages carry no message and stay with the filter, as every other target does.
struct LogFilter {
    configured: EnvFilter,
}

impl LogFilter {
    /// The module Lapin logs a returned message from, which is the target of its events.
    const LAPIN_RETURNED_MESSAGES: &'static str = "lapin::returned_messages";

    /// The operator's `RUST_LOG`, or `default` when it is unset or does not parse.
    fn from_environment(default: &str) -> Self {
        let configured =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
        Self { configured }
    }

    /// The layer a subscriber decides what it records with: the payload rule, then the filter.
    fn into_layer<S: Subscriber>(self) -> impl Layer<S> {
        self.configured.and_then(filter_fn(Self::admits))
    }

    /// Whether the log may record what `metadata` describes, whatever the filter selects.
    fn admits(metadata: &Metadata<'_>) -> bool {
        let returned_message =
            metadata.target() == Self::LAPIN_RETURNED_MESSAGES && *metadata.level() <= Level::INFO;
        !returned_message
    }
}

/// The share of root traces the node exports, between nothing and everything.
pub struct TraceSampleRatio;

impl SettingDefinition for TraceSampleRatio {
    type Class = NodeCapabilities;
    type Value = f64;
    type Context = ();
    type Error = TraceSampleRatioError;

    const NAME: &'static str = "otel-trace-sample-ratio";
    const DERIVATION: Option<&'static str> = None;

    fn default_value(_: &()) -> f64 {
        1.0
    }

    fn validate(ratio: &f64, _: &()) -> Result<(), Report<TraceSampleRatioError>> {
        if (0.0..=1.0).contains(ratio) {
            Ok(())
        } else {
            Err(Report::new(TraceSampleRatioError::OutOfRange {
                ratio: *ratio,
            }))
        }
    }
}

/// Why a trace sample ratio is refused.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum TraceSampleRatioError {
    #[error("trace sample ratio {ratio} must be between 0.0 and 1.0")]
    OutOfRange { ratio: f64 },
}

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
    Connect,
}

/// Tonic requires a standard error; retain the complete connection report through that hook.
#[derive(Debug)]
struct TraceConnectFailure(Report<TraceConnectError>);

impl std::fmt::Display for TraceConnectFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, formatter)
    }
}

impl std::error::Error for TraceConnectFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        let cause = self
            .0
            .downcast_ref::<<HttpConnector<DnsResolver> as Service<http::Uri>>::Error>()?;
        Some(cause)
    }
}

impl Service<http::Uri> for NodeTraceConnector {
    type Response = <HttpConnector<DnsResolver> as Service<http::Uri>>::Response;
    type Error = TraceConnectFailure;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let mut receiver = self.receiver.clone();
        let connection_timeout = self.timeout;
        let connect = async move {
            let dns = {
                let installed = receiver
                    .wait_for(Option::is_some)
                    .await
                    .change_context(TraceConnectError::Unavailable)?;
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
                Ok(connection) => connection.change_context(TraceConnectError::Connect),
                Err(_) => Err(Report::new(TraceConnectError::Timeout {
                    timeout: connection_timeout,
                })),
            }
        };
        Box::pin(async move { connect.await.map_err(TraceConnectFailure) })
    }
}

struct TraceExporter {
    exporter: opentelemetry_otlp::SpanExporter,
    dns: watch::Sender<Option<DnsResolver>>,
}

impl TraceExporter {
    fn new(args: &TracingArgs) -> Result<Self, Report<AppError>> {
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

pub fn init_tracing(args: &TracingArgs) -> Result<TracingGuard, Report<AppError>> {
    let sample_ratio = Setting::<TraceSampleRatio>::new(args.otel_trace_sample_ratio, &())
        .change_context(AppError::InitTracing)?;
    let log_filter = LogFilter::from_environment(DEFAULT_TRACE_FILTER).into_layer();
    let fmt_layer = fmt::layer().with_ansi(false);

    if args.otel_enabled {
        let trace = TraceExporter::new(args)?;
        let resource = Resource::builder()
            .with_service_name(args.otel_service_name.clone())
            .build();
        let tracer_provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                *sample_ratio.value(),
            ))))
            .with_resource(resource)
            .with_batch_exporter(trace.exporter)
            .build();
        let tracer = tracer_provider.tracer("nervix");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(log_filter)
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
            .with(log_filter)
            .with(fmt_layer)
            .try_init()
            .change_context(AppError::InitTracing)?;

        Ok(TracingGuard { traces: None })
    }
}

#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]
#[path = "tracing_setup_tests.rs"]
mod tests;

#[cfg(all(test, not(any(feature = "shuttle", feature = "loom"))))]
#[path = "tracing_setup_filter_tests.rs"]
mod filter_tests;

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
///
/// Native producers and consumers are restored the same way: a read that waits out its deadline
/// after a restart is undiagnosable unless the log says which side stalled, whether the client
/// waited out a refused restoration, the serving node refused or forwarded the open, or the
/// executing endpoint never assigned the batch. Their decisions are logged at `debug`, once per
/// open, refusal, attachment and attempt, so a scenario with endpoints costs a few dozen lines.
const TEST_TRACE_FILTER: &str =
    "nervix_interconnect=debug,nervix_consensus::replication=debug,\
     nervix_client_core::restoration=debug,nervix_server::application=debug,\
     nervix_client_core::consumer=debug,nervix_client_core::producer=debug,\
     nervix_server::application::session::consumers=debug,\
     nervix_server::application::session::producers=debug,\
     nervix_server::application::client_consumers=debug,\
     nervix_server::application::client_producers=debug,\
     nervix_server::runtime::client_emitter=debug,nervix_server::runtime::client_ingestor=debug";

pub fn init_tracing_to_file(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let file = Arc::new(ParkingMutex::new(file));
    let make_writer = BoxMakeWriter::new(move || SharedFileWriter(file.clone()));
    let log_filter =
        LogFilter::from_environment(&format!("{DEFAULT_TRACE_FILTER},{TEST_TRACE_FILTER}"))
            .into_layer();
    tracing_subscriber::registry()
        .with(log_filter)
        .with(fmt::layer().with_ansi(false).with_writer(make_writer))
        .try_init()
        .discarded(
            "the first call in this process installed the subscriber this one would replace",
        );
    Ok(())
}
