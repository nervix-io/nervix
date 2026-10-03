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
use nervix_primitives::{net::TcpListener, sync::watch, time::timeout};
use nervix_test_environment::dns_authority::{DnsAnswer, DnsAuthority};
use opentelemetry_sdk::trace::SpanExporter as _;
use tempfile::TempDir;
use tokio::io::AsyncReadExt as _;
use tower_service::Service as _;

use super::{
    super::AppError, NodeTraceConnector, TraceConnectError, TraceExportGuard, TraceExporter,
    TracingGuard,
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
        // Silent queries outlast TEST_WAIT; only the exporter's connection deadline can end them.
        std::fs::write(
            &resolver_configuration,
            "options ndots:1 timeout:30 attempts:1\n",
        )
        .assured("the fixture resolver configuration can be written");
        std::fs::write(&hosts_file, "").assured("the fixture hosts file can be written");
        let resolver = DnsResolver::load(DnsConfiguration {
            resolver_configuration,
            hosts_file,
            name_servers: NameServers::Explicit(vec![authority.address()]),
        })
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
    let trace = TraceExporter::from_endpoint(ENDPOINT.to_string(), TEST_WAIT)
        .assured("the configured HTTP collector endpoint is valid");
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
        Err(TraceConnectError::Connect(source)) => {
            let error =
                DnsLookupError::find_in(&source).verified("Hyper retains its resolver error");
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
    assert!(matches!(lookup.await, Err(TraceConnectError::Unavailable)));
}

#[nervix_primitives::test]
async fn the_trace_export_timeout_bounds_a_silent_node_dns_lookup() {
    let fixture = Fixture::new(DnsAnswer::Silent).await;
    let trace = TraceExporter::from_endpoint(ENDPOINT.to_string(), Duration::from_millis(100))
        .assured("the configured HTTP collector endpoint is valid");
    trace.dns.send_replace(Some(fixture.resolver.clone()));
    let result = timeout(TEST_WAIT, trace.exporter.export(Vec::new()))
        .await
        .assured("the export timeout ends the connection before the DNS hook's deadline");
    assert!(result.is_err());
    assert!(fixture.authority.questions_for(NAME) > 0);
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
    let trace = TraceExporter::from_endpoint(format!("https://{address}"), TEST_WAIT)
        .assured("the HTTPS collector URI is valid");
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

#[test]
fn trace_endpoint_validation_reports_the_tracing_owner() {
    let failure = TraceExporter::from_endpoint("http://bad\nhost".to_string(), TEST_WAIT)
        .err()
        .assured("a newline is invalid in a collector URI");
    assert!(matches!(failure.current_context(), AppError::InitTracing));
}

#[test]
fn trace_export_timeout_keeps_signal_precedence_and_ignores_invalid_values() {
    for (variables, expected) in [
        ([None, None], Duration::from_secs(10)),
        ([Some("17"), Some("25")], Duration::from_millis(17)),
        ([Some("invalid"), Some("25")], Duration::from_millis(25)),
        ([Some(""), Some("invalid")], Duration::from_secs(10)),
        ([Some("0"), None], Duration::ZERO),
    ] {
        assert_eq!(TraceExporter::timeout_from(variables), expected);
    }
}

#[test]
fn trace_endpoint_keeps_the_sdk_configuration_precedence() {
    struct Case {
        configured: &'static str,
        variables: [Option<&'static str>; 2],
        expected: &'static str,
    }
    for case in [
        Case {
            configured: ENDPOINT,
            variables: [Some("http://trace:4317"), Some("http://general:4317")],
            expected: ENDPOINT,
        },
        Case {
            configured: "",
            variables: [Some("http://trace:4317"), Some("http://general:4317")],
            expected: "http://trace:4317",
        },
        Case {
            configured: "",
            variables: [None, Some("http://general:4317")],
            expected: "http://general:4317",
        },
        Case {
            configured: "",
            variables: [Some(""), Some("http://general:4317")],
            expected: "",
        },
        Case {
            configured: "",
            variables: [None, None],
            expected: "http://localhost:4317",
        },
    ] {
        assert_eq!(
            TraceExporter::endpoint_from(case.configured, case.variables),
            case.expected
        );
    }
}
