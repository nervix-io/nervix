//! Where this node's traces and logs go.
//!
//! Layer: edges.
//!
//! - **Owns.** The subscriber the process installs and the filter that decides what it records,
//!   including the dependency events no filter may select, the provisioned trace collector, the
//!   OTLP span exporter the node adds for it, configured only by that collector and the trace
//!   export capabilities, and the guard that flushes them on shutdown.
//! - **Depends on.** The node's provisioned collector and trace export capabilities, the node
//!   resolver, the telemetry SDK and the OpenTelemetry protocol crate's generated client.
//! - **Must not know.** What any other module records, or any `OTEL_*` environment variable.

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
use nervix_models::TraceExportCapabilities;
use nervix_primitives::{
    sync::{Arc, blocking::Mutex as ParkingMutex, watch},
    time::timeout,
};
use nervix_recovery::{Discarded, Reported};
use opentelemetry::trace::TracerProvider;
use opentelemetry_proto::{
    tonic::collector::trace::v1::{
        ExportTraceServiceRequest, trace_service_client::TraceServiceClient,
    },
    transform::{
        common::tonic::ResourceAttributesWithSchema,
        trace::tonic::group_spans_by_resource_and_scope,
    },
};
use opentelemetry_sdk::{
    Resource,
    error::{OTelSdkError, OTelSdkResult},
    resource::TelemetryResourceDetector,
    trace::{
        BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider, SpanData, SpanExporter,
        SpanLimits,
    },
};
use tower_service::Service;
use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    EnvFilter, Layer, filter::filter_fn, fmt, fmt::writer::BoxMakeWriter,
    layer::Context as LayerContext, prelude::__tracing_subscriber_SubscriberExt,
    registry::LookupSpan, util::SubscriberInitExt,
};

use super::AppError;

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

    /// The directives a scenario run records with when `RUST_LOG` is unset: the node's default and
    /// what a scenario needs beyond it.
    fn scenario_default() -> String {
        format!("{DEFAULT_TRACE_FILTER},{TEST_TRACE_FILTER}")
    }

    /// The registry the process records through, which its writers are layered on: the payload rule
    /// as the layer beneath and the filter above it. The filter answers first, so the subscriber's
    /// level hint is the filter's, a callsite the filter refuses is never asked about again, and the
    /// rule is asked only about what the filter selects.
    ///
    /// Composing the two with `Layer::and_then` would not keep that. A layer composed over the
    /// registry takes its inner layer for the registry itself, so the composition would report the
    /// rule's absent level hint, which raises the subscriber's maximum level to `trace`, and would
    /// evaluate both filters on every event of a callsite the filter refuses.
    fn into_subscriber(
        self,
    ) -> impl Subscriber + for<'span> LookupSpan<'span> + Send + Sync + 'static {
        tracing_subscriber::registry()
            .with(filter_fn(Self::admits))
            .with(MqttPayloadRule)
            .with(self.configured)
    }

    /// Whether the log may record what `metadata` describes, whatever the filter selects.
    fn admits(metadata: &Metadata<'_>) -> bool {
        let returned_message =
            metadata.target() == Self::LAPIN_RETURNED_MESSAGES && *metadata.level() <= Level::INFO;
        !returned_message
    }
}

/// The pinned MQTT driver's retained-publish refusal embeds the whole request in its message.
/// Its target also owns ordinary connection warnings, so this rule visits only that message's
/// fixed prefix and stops formatting before reaching the request.
struct MqttPayloadRule;

impl<S: Subscriber> Layer<S> for MqttPayloadRule {
    fn event_enabled(&self, event: &Event<'_>, _: LayerContext<'_, S>) -> bool {
        let metadata = event.metadata();
        if metadata.target() != "rumqttc::eventloop" || *metadata.level() > Level::INFO {
            return true;
        }
        let mut prefix = MqttWarningPrefix::Matching(
            b"rejecting retained publish because the broker does not support retained messages:",
        );
        event.record(&mut prefix);
        !prefix.matched()
    }
}

/// A bounded comparison, with no allocation or retained event data. Matching either ends at the
/// fixed prefix or at its first differing byte; no request value is rendered to decide it.
enum MqttWarningPrefix {
    Matching(&'static [u8]),
    Matched,
    Different,
}

impl MqttWarningPrefix {
    fn matched(&self) -> bool {
        matches!(self, Self::Matched)
    }

    fn record_message(&mut self, field: &Field, arguments: std::fmt::Arguments<'_>) {
        if field.name() == "message" {
            std::fmt::write(self, arguments).discarded(
                "the bounded prefix writer ends formatting as soon as its comparison is decided",
            );
        }
    }
}

impl Visit for MqttWarningPrefix {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.record_message(field, format_args!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_message(field, format_args!("{value}"));
    }
}

impl std::fmt::Write for MqttWarningPrefix {
    #[cfg_attr(
        nervix_lint,
        nervix::error_boundary(
            outcome,
            reason = "fmt::Error stops the foreign formatter after the fixed prefix comparison is \
                      decided"
        )
    )]
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let Self::Matching(remaining) = self else {
            return Err(std::fmt::Error);
        };
        let remaining = *remaining;
        let compared = remaining.len().min(value.len());
        let (expected, rest) = remaining.split_at(compared);
        if &value.as_bytes()[..compared] != expected {
            *self = Self::Different;
            return Err(std::fmt::Error);
        }
        if rest.is_empty() {
            *self = Self::Matched;
            return Err(std::fmt::Error);
        }
        *self = Self::Matching(rest);
        Ok(())
    }
}

/// The collector a node is provisioned to export its own traces to, and the service name its
/// spans carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceCollector {
    endpoint: http::Uri,
    service_name: String,
}

impl TraceCollector {
    /// Reads the collector's endpoint before the node does any I/O; text that is not a URI, the
    /// empty text included, refuses the node.
    pub(super) fn new(endpoint: &str, service_name: String) -> Result<Self, Report<AppError>> {
        let endpoint = endpoint
            .parse::<http::Uri>()
            .change_context(AppError::ParseTraceCollectorEndpoint)?;
        Ok(Self {
            endpoint,
            service_name,
        })
    }
}

/// The node's own trace export: the collector it is provisioned with and the capabilities it
/// exports with.
#[derive(Debug, Clone, Copy)]
pub struct NodeTraceExport<'a> {
    pub collector: &'a TraceCollector,
    pub capabilities: &'a TraceExportCapabilities,
}

impl super::Application {
    /// The node's own trace export, when the node is provisioned with a collector. Only the
    /// process entry point installs it, and a Shuttle build has no process entry point.
    #[cfg(not(feature = "shuttle"))]
    pub(in crate::application) fn trace_export(&self) -> Option<NodeTraceExport<'_>> {
        let collector = self.identity.trace_collector.as_ref()?;
        Some(NodeTraceExport {
            collector,
            capabilities: &self.settings.operational.traces,
        })
    }
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

/// Exports the node's spans to its collector with the OpenTelemetry protocol crate's generated
/// gRPC client and span transform. Only the node's provisioning and capabilities configure it:
/// it sends no request metadata, compresses nothing and reads no environment variable, which
/// the OTLP exporter library's builder always does.
struct NodeSpanExporter {
    client: Option<TraceServiceClient<otel_tonic::transport::Channel>>,
    resource: ResourceAttributesWithSchema,
}

impl NodeSpanExporter {
    fn new(channel: otel_tonic::transport::Channel) -> Self {
        Self {
            client: Some(TraceServiceClient::new(channel)),
            resource: ResourceAttributesWithSchema::default(),
        }
    }
}

impl std::fmt::Debug for NodeSpanExporter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeSpanExporter")
            .field("shut_down", &self.client.is_none())
            .finish_non_exhaustive()
    }
}

impl SpanExporter for NodeSpanExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let Some(client) = &self.client else {
            return Err(OTelSdkError::AlreadyShutdown);
        };
        let mut client = client.clone();
        let resource_spans = group_spans_by_resource_and_scope(batch, &self.resource);
        let request = ExportTraceServiceRequest { resource_spans };
        match client.export(request).await {
            Ok(_) => Ok(()),
            Err(status) => Err(OTelSdkError::InternalFailure(status.to_string())),
        }
    }

    fn shutdown(&mut self) -> OTelSdkResult {
        match self.client.take() {
            Some(_) => Ok(()),
            None => Err(OTelSdkError::AlreadyShutdown),
        }
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.resource = resource.into();
    }
}

struct TraceExporter {
    exporter: NodeSpanExporter,
    dns: watch::Sender<Option<DnsResolver>>,
}

impl TraceExporter {
    /// A lazy channel to the collector: creating it performs no lookup or dial. The export
    /// timeout bounds reaching the collector and each export request.
    fn new(endpoint: http::Uri, timeout: Duration) -> Self {
        let endpoint = otel_tonic::transport::Endpoint::from(endpoint).timeout(timeout);
        let (dns, receiver) = watch::channel(None);
        let channel =
            endpoint.connect_with_connector_lazy(NodeTraceConnector { receiver, timeout });
        Self {
            exporter: NodeSpanExporter::new(channel),
            dns,
        }
    }
}

/// Installs the process's subscriber, and the node's own trace export when it has a collector.
///
/// Every value the export runs with comes from the collector's provisioning and the trace export
/// capabilities, never from an `OTEL_*` variable: the SDK still reads its environment while it
/// builds its defaults, and each such default is replaced here.
pub fn init_tracing(export: Option<NodeTraceExport<'_>>) -> Result<TracingGuard, Report<AppError>> {
    let subscriber = LogFilter::from_environment(DEFAULT_TRACE_FILTER).into_subscriber();
    let fmt_layer = fmt::layer().with_ansi(false);
    let Some(export) = export else {
        subscriber
            .with(fmt_layer)
            .try_init()
            .change_context(AppError::InitTracing)?;
        return Ok(TracingGuard { traces: None });
    };
    let capabilities = export.capabilities;
    let trace = TraceExporter::new(
        export.collector.endpoint.clone(),
        capabilities.export_timeout(),
    );
    // The thread-based batch processor uses these three values; the export timeout it reads from
    // OTEL_BSP_EXPORT_TIMEOUT and the concurrency it reads from OTEL_BSP_MAX_CONCURRENT_EXPORTS
    // belong to the SDK's experimental asynchronous processor, which the node does not run.
    let batch = BatchConfigBuilder::default()
        .with_max_queue_size(capabilities.queued_spans())
        .with_scheduled_delay(capabilities.export_interval())
        .with_max_export_batch_size(capabilities.batched_spans())
        .build();
    let processor = BatchSpanProcessor::builder(trace.exporter)
        .with_batch_config(batch)
        .build();
    // No environment detector: OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME add nothing.
    let resource = Resource::builder_empty()
        .with_detector(Box::new(TelemetryResourceDetector))
        .with_service_name(export.collector.service_name.clone())
        .build();
    // The sampler and the specification's default span limits replace those the SDK reads from
    // OTEL_TRACES_SAMPLER and OTEL_SPAN_*_COUNT_LIMIT.
    let tracer_provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            capabilities.sample_ratio().value(),
        ))))
        .with_span_limits(SpanLimits::default())
        .with_resource(resource)
        .with_span_processor(processor)
        .build();
    let tracer = tracer_provider.tracer("nervix");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    subscriber
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
///
/// A native resource upload says at `debug` why it opens another call for the same identity: the
/// status of the call that ended before its reply arrived, the leader a reply redirected it to, or
/// that no leader was known. Without those lines an upload that recovered the version its own
/// earlier call installed reads like one that found another upload's, and an upload that repeats
/// nothing costs nothing.
///
/// Consensus receiver contexts identify the concrete node even when parallel clusters use the
/// same names. Its vote and pre-vote clients record send and completion timing at `debug`, so the
/// failing attempt shows whether an election waited before the Raft API, in its core, or for its
/// response. These events are per election request, not per heartbeat. OpenRaft keeps its existing
/// filter; collecting these boundaries does not enable its per-tick and per-append debug stream.
///
/// Nothing here selects `trace`. The process records through one subscriber, and the `log` bridge
/// hands it every record a dependency writes through the `log` crate, up to the most verbose level
/// any directive selects, to be judged one record at a time. Cranelift writes `trace` records
/// throughout every module it compiles, and a node compiles the WASM module it installs before it
/// reports the revision prepared: one `trace` directive made each compile of the scenario guest
/// about six times as long, and the installations after a restart, a restore or a peer's
/// validation outlasted the budget a revision has to become prepared and ready.
const TEST_TRACE_FILTER: &str =
    "nervix_interconnect=debug,nervix_consensus=debug,nervix_consensus::replication=debug,\
     nervix_client_core::restoration=debug,nervix_client_core::upload=debug,\
     nervix_server::application=debug,nervix_client_core::consumer=debug,\
     nervix_client_core::producer=debug,nervix_server::application::session::consumers=debug,\
     nervix_server::application::session::producers=debug,\
     nervix_server::application::client_consumers=debug,\
     nervix_server::application::client_producers=debug,\
     nervix_server::runtime::client_emitter=debug,nervix_server::runtime::client_ingestor=debug";

pub fn init_tracing_to_file(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let file = Arc::new(ParkingMutex::new(file));
    let make_writer = BoxMakeWriter::new(move || SharedFileWriter(file.clone()));
    LogFilter::from_environment(&LogFilter::scenario_default())
        .into_subscriber()
        .with(fmt::layer().with_ansi(false).with_writer(make_writer))
        .try_init()
        .discarded(
            "the first call in this process installed the subscriber this one would replace",
        );
    Ok(())
}
