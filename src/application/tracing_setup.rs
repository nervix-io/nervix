//! Where this node's traces and logs go.
//!
//! Layer: edges.
//!
//! - **Owns.** The subscriber the process installs, the OTLP exporter it may add, and the guard
//!   that flushes them on shutdown.
//! - **Depends on.** The command-line arguments that configure them.
//! - **Must not know.** What any other module records.

use std::{fs::OpenOptions, io, path::Path};

use error_stack::{Report, ResultExt};
use nervix_recovery::{Discarded, Reported};
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    trace::{Sampler, SdkTracerProvider},
};
use parking_lot::Mutex as ParkingMutex;
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
    tracer_provider: Option<SdkTracerProvider>,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if let Some(tracer_provider) = self.tracer_provider.take() {
            tracer_provider
                .shutdown()
                .reported("flushing the tracer provider on shutdown");
        }
    }
}

pub fn init_tracing(args: &Args) -> Result<TracingGuard, Report<AppError>> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_TRACE_FILTER));
    let fmt_layer = fmt::layer().with_ansi(false);

    if args.otel_enabled {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(args.otel_otlp_endpoint.clone())
            .build()
            .change_context(AppError::InitTracing)?;
        let resource = Resource::builder()
            .with_service_name(args.otel_service_name.clone())
            .build();
        let tracer_provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                args.otel_trace_sample_ratio,
            ))))
            .with_resource(resource)
            .with_batch_exporter(exporter)
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
            tracer_provider: Some(tracer_provider),
        })
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .try_init()
            .change_context(AppError::InitTracing)?;

        Ok(TracingGuard {
            tracer_provider: None,
        })
    }
}

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
