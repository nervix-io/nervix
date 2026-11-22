//! Ordinary-mode evidence for the node's trace-export DNS boundary.
//!
//! Outside the layer order: tests may name any layer and product code may not name them.
//!
//! - **Owns.** Local DNS fixtures and assertions over the actual exporter and resolver service.
//! - **Depends on.** The tracing owner, the DNS authority and the telemetry SDK's exporter API.
//! - **Must not know.** A graph, a connector's retries or acknowledgement state.

use std::{net::SocketAddr, time::Duration};

use futures_util::poll;
use meticulous::{OptionExt as _, ResultExt as _};
use nervix_dns::{DnsConfiguration, DnsLookupError, DnsLookupFailure, DnsResolver, NameServers};
use nervix_primitives::{
    net::TcpListener,
    sync::watch,
    time::{Instant, timeout},
};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use opentelemetry_sdk::{error::OTelSdkError, trace::SpanExporter as _};
use tempfile::TempDir;
use tokio::io::AsyncReadExt as _;
use tower_service::Service as _;

use super::{
    super::AppError, NodeTraceConnector, TraceCollector, TraceConnectError, TraceExportGuard,
    TraceExporter, TracingGuard,
};

const NAME: &str = "node-traces.nervix.test";
const ENDPOINT: &str = "http://node-traces.nervix.test:4317";
const TEST_WAIT: Duration = Duration::from_secs(5);

struct Fixture {
    authority: DnsAuthority,
    resolver: DnsResolver,
    _root: TempDir,
}

impl Fixture {
    async fn new(answer: DnsAnswer) -> Self {
        let authority = DnsAuthority::start_on_loopback()
            .await
            .assured("a loopback DNS fixture can bind");
        authority.set(NAME, answer);
        let root = tempfile::tempdir().assured("a fixture directory can be created");
        let resolver_configuration = root.path().join("resolv.conf");
        let hosts_file = root.path().join("hosts");
        // The fixture's DNS deadline outlasts TEST_WAIT so the exporter can time out first.
        std::fs::write(
            &resolver_configuration,
            "options ndots:1 timeout:30 attempts:1\n",
        )
        .assured("the fixture resolver configuration can be written");
        std::fs::write(&hosts_file, "").assured("the fixture hosts file can be written");
        let resolver = DnsResolver::load(
            DnsConfiguration {
                resolver_configuration,
                hosts_file,
                name_servers: NameServers::Explicit(vec![authority.address()]),
            },
            nervix_dns::DnsSettings::default(),
        )
        .await
        .assured("the fixture configuration names its DNS authority");
        Self {
            authority,
            resolver,
            _root: root,
        }
    }

    async fn name_not_found() -> Self {
        Self::new(DnsAnswer::NameNotFound {
            negative_ttl: Duration::from_secs(1),
        })
        .await
    }
}

#[nervix_primitives::test]
async fn node_trace_channel_is_lazy_and_resolves_through_the_installed_node_resolver() {
    let fixture = Fixture::name_not_found().await;
    let trace = TraceExporter::new(http::Uri::from_static(ENDPOINT), TEST_WAIT);
    nervix_primitives::task::yield_now().await;
    assert_eq!(fixture.authority.questions_for(NAME), 0);
    trace.dns.send_replace(Some(fixture.resolver.clone()));
    nervix_primitives::task::yield_now().await;
    assert_eq!(fixture.authority.questions_for(NAME), 0);
    let result = trace.exporter.export(Vec::new()).await;
    assert!(result.is_err());
    assert!(fixture.authority.questions_for(NAME) > 0);
}

#[nervix_primitives::test]
async fn node_trace_resolution_waits_for_startup_and_retains_the_lookup_failure() {
    let fixture = Fixture::name_not_found().await;
    let (publisher, receiver) = watch::channel(None);
    let mut connector = NodeTraceConnector {
        receiver,
        timeout: TEST_WAIT,
    };
    let mut lookup = connector.call(http::Uri::from_static(ENDPOINT));
    assert!(poll!(&mut lookup).is_pending());
    assert_eq!(fixture.authority.questions_for(NAME), 0);
    let guard = TracingGuard {
        traces: Some(TraceExportGuard {
            tracer_provider: opentelemetry_sdk::trace::SdkTracerProvider::builder().build(),
            dns: publisher,
        }),
    };
    guard.install_dns(&fixture.resolver);
    match lookup.await {
        Err(failure) if matches!(failure.0.current_context(), TraceConnectError::Connect) => {
            let error =
                DnsLookupError::find_in(&failure).verified("Hyper retains its resolver error");
            assert!(failure.0.contains::<<hyper_util::client::legacy::connect::HttpConnector<DnsResolver> as tower_service::Service<http::Uri>>::Error>());
            assert_eq!(error.failure(), DnsLookupFailure::NameNotFound);
            assert_eq!(error.name(), NAME);
        }
        outcome => panic!("expected the fixture's lookup failure, got {outcome:?}"),
    }
}

#[nervix_primitives::test(start_paused = true)]
async fn startup_wait_retains_early_exports_and_closes_if_startup_ends() {
    let (publisher, receiver) = watch::channel(None);
    let guard = TracingGuard {
        traces: Some(TraceExportGuard {
            tracer_provider: opentelemetry_sdk::trace::SdkTracerProvider::builder().build(),
            dns: publisher,
        }),
    };
    let mut connector = NodeTraceConnector {
        receiver,
        timeout: TEST_WAIT,
    };
    let mut lookup = connector.call(http::Uri::from_static(ENDPOINT));
    assert!(poll!(&mut lookup).is_pending());
    nervix_primitives::time::advance(Duration::from_secs(60)).await;
    assert!(poll!(&mut lookup).is_pending());
    drop(guard);
    let failure = lookup
        .await
        .err()
        .assured("startup ended before installing DNS");
    assert!(matches!(
        failure.0.current_context(),
        TraceConnectError::Unavailable
    ));
}

#[nervix_primitives::test]
async fn the_trace_export_timeout_bounds_a_silent_node_dns_lookup() {
    // Give the resolver and authority time to run alongside the rest of the suite.
    const EXPORT_TIMEOUT: Duration = Duration::from_secs(2);
    const _: () = assert!(EXPORT_TIMEOUT.as_nanos() < TEST_WAIT.as_nanos());

    let fixture = Fixture::new(DnsAnswer::Silent).await;
    let trace = TraceExporter::new(http::Uri::from_static(ENDPOINT), EXPORT_TIMEOUT);
    trace.dns.send_replace(Some(fixture.resolver.clone()));
    let started = Instant::now();
    let result = timeout(TEST_WAIT, trace.exporter.export(Vec::new()))
        .await
        .assured("the export ends within the test guard, before the DNS hook's deadline");
    assert!(
        fixture.authority.questions_for(NAME) > 0,
        "the node resolver must ask the authority before the export ends"
    );
    assert!(
        result.is_err(),
        "silent DNS leaves the collector unreachable"
    );
    assert!(
        started.elapsed() >= EXPORT_TIMEOUT,
        "the export ended before its configured timeout"
    );
}

#[nervix_primitives::test]
async fn https_trace_export_requires_configured_tonic_tls() {
    let fixture = Fixture::name_not_found().await;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .assured("a loopback collector listener can bind");
    let address = listener
        .local_addr()
        .assured("a bound listener has an address");
    let endpoint = format!("https://{address}")
        .parse::<http::Uri>()
        .assured("the HTTPS collector URI is valid");
    let trace = TraceExporter::new(endpoint, TEST_WAIT);
    trace.dns.send_replace(Some(fixture.resolver.clone()));
    let exporting =
        nervix_primitives::task::spawn(async move { trace.exporter.export(Vec::new()).await });
    let (mut connection, _) = timeout(TEST_WAIT, listener.accept())
        .await
        .assured("the literal endpoint needs no lookup")
        .assured("the exporter reaches the listening collector");
    let mut byte = [0];
    let count = timeout(TEST_WAIT, connection.read(&mut byte))
        .await
        .assured("Tonic rejects the unconfigured TLS transport")
        .assured("the collector observes the client closing");
    assert_eq!(count, 0);
    assert!(
        exporting
            .await
            .assured("the exporting task completes")
            .is_err()
    );
}

#[nervix_primitives::test]
async fn a_shut_down_exporter_refuses_further_exports_and_a_second_shutdown() {
    let mut trace = TraceExporter::new(http::Uri::from_static(ENDPOINT), TEST_WAIT);
    assert_eq!(
        format!("{:?}", trace.exporter),
        "NodeSpanExporter { shut_down: false, .. }"
    );
    assert!(trace.exporter.shutdown().is_ok());
    assert_eq!(
        format!("{:?}", trace.exporter),
        "NodeSpanExporter { shut_down: true, .. }"
    );
    assert!(matches!(
        trace.exporter.export(Vec::new()).await,
        Err(OTelSdkError::AlreadyShutdown)
    ));
    assert!(matches!(
        trace.exporter.shutdown(),
        Err(OTelSdkError::AlreadyShutdown)
    ));
}

#[test]
fn a_collector_endpoint_that_is_not_a_uri_refuses_the_node_before_it_starts() {
    for endpoint in ["http://bad\nhost", ""] {
        let failure = TraceCollector::new(endpoint, "nervix".to_owned())
            .err()
            .assured("a newline or nothing at all is no collector URI");
        assert!(matches!(
            failure.current_context(),
            AppError::ParseTraceCollectorEndpoint
        ));
    }
    let collector = TraceCollector::new(ENDPOINT, "nervix".to_owned())
        .assured("an HTTP collector URI names a collector");
    assert_eq!(collector.endpoint, http::Uri::from_static(ENDPOINT));
    assert_eq!(collector.service_name, "nervix");
}
