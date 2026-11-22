//! Public evidence for the node's own OTLP trace exporter.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** A real-process cluster, the DNS fixture its trace-export endpoint names, and the
//!   OpenTelemetry environment a scenario gives its processes.
//! - **Depends on.** The process cluster, the local OTLP receiver and its current protobuf schema.
//! - **Must not know.** The node's subscriber, exporter or resolver implementation.

use std::{collections::BTreeSet, io, net::IpAddr, time::Duration};

use meticulous::OptionExt as _;
use nix::sys::signal::Signal;
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest, common::v1::any_value::Value,
};
use otel_prost::Message as _;
use tempfile::TempDir;

use super::{
    grpc_receiver::GrpcReceiver,
    peer_addressing::{ClusterDns, PeerAddressing},
    phase_deadline::PhaseDeadline,
    server_process::{ServerProcessOption, TraceCollectorEndpoint},
    server_process_cluster::ServerProcessCluster,
};

const COLLECTOR_NAME: &str = "node-traces.nervix.test";

/// The request header names the OpenTelemetry environment asks the exporter to send.
const ENVIRONMENT_HEADERS: [&str; 2] = [
    "x-nervix-environment-general",
    "x-nervix-environment-traces",
];

/// The resource attribute the OpenTelemetry environment asks the SDK to add to every span.
const ENVIRONMENT_RESOURCE_ATTRIBUTE: &str = "nervix.environment";

/// Every variable the OpenTelemetry SDK and its OTLP exporter read, each set to a value that would
/// visibly change the export if it reached it: the endpoint names another collector, the headers
/// and resource attribute are named above, compression is on, the timeouts are one millisecond, the
/// batch processor holds spans for an hour, span limits drop every attribute, event and link, the
/// sampler samples nothing and the service name is another node's.
fn opentelemetry_environment(collector: &str) -> Vec<(&'static str, String)> {
    vec![
        ("OTEL_EXPORTER_OTLP_ENDPOINT", collector.to_owned()),
        ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.to_owned()),
        (
            "OTEL_EXPORTER_OTLP_HEADERS",
            format!("{}=general", ENVIRONMENT_HEADERS[0]),
        ),
        (
            "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
            format!("{}=traces", ENVIRONMENT_HEADERS[1]),
        ),
        ("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip".to_owned()),
        ("OTEL_EXPORTER_OTLP_TRACES_COMPRESSION", "gzip".to_owned()),
        ("OTEL_EXPORTER_OTLP_TIMEOUT", "1".to_owned()),
        ("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", "1".to_owned()),
        ("OTEL_BSP_SCHEDULE_DELAY", "3600000".to_owned()),
        ("OTEL_BSP_MAX_QUEUE_SIZE", "100000".to_owned()),
        ("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "100000".to_owned()),
        ("OTEL_BSP_EXPORT_TIMEOUT", "1".to_owned()),
        ("OTEL_BSP_MAX_CONCURRENT_EXPORTS", "8".to_owned()),
        ("OTEL_SPAN_ATTRIBUTE_COUNT_LIMIT", "0".to_owned()),
        ("OTEL_SPAN_EVENT_COUNT_LIMIT", "0".to_owned()),
        ("OTEL_SPAN_LINK_COUNT_LIMIT", "0".to_owned()),
        ("OTEL_TRACES_SAMPLER", "always_off".to_owned()),
        ("OTEL_TRACES_SAMPLER_ARG", "0".to_owned()),
        (
            "OTEL_RESOURCE_ATTRIBUTES",
            format!("{ENVIRONMENT_RESOURCE_ATTRIBUTE}=present"),
        ),
        ("OTEL_SERVICE_NAME", "nervix-environment".to_owned()),
    ]
}

/// How a scenario names the collector its processes export to.
#[derive(Clone, Copy, Debug)]
pub(crate) enum CollectorProvisioning<'a> {
    /// `--otel-otlp-endpoint` names the collector. A `twin` collector, when one is given, is
    /// named by the option's environment twin beside it.
    Option { twin: Option<&'a GrpcReceiver> },
    /// Only the option's environment twin, `NERVIX_OTEL_OTLP_ENDPOINT`, names the collector.
    EnvironmentTwin,
}

/// The processes' trace export: the collector they are provisioned with and how, and another
/// collector every OpenTelemetry environment variable names, with every other OpenTelemetry
/// variable set as well.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TraceExportLaunch<'a> {
    pub(crate) collector: &'a GrpcReceiver,
    pub(crate) provisioning: CollectorProvisioning<'a>,
    pub(crate) opentelemetry: Option<&'a GrpcReceiver>,
}

impl<'a> TraceExportLaunch<'a> {
    /// The collector `--otel-otlp-endpoint` names, with nothing else in the environment.
    pub(crate) fn option(collector: &'a GrpcReceiver) -> Self {
        Self {
            collector,
            provisioning: CollectorProvisioning::Option { twin: None },
            opentelemetry: None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct NodeTraceExport {
    // Processes stop before the resolver files and authority disappear.
    cluster: ServerProcessCluster,
    dns: ClusterDns,
    service: String,
    _root: TempDir,
}

impl NodeTraceExport {
    pub(crate) async fn start(
        node_count: usize,
        launch: TraceExportLaunch<'_>,
        service: String,
    ) -> io::Result<Self> {
        let root = tempfile::tempdir()?;
        let dns = ClusterDns::start(PeerAddressing::DnsNames, root.path(), &[])
            .await?
            .assured("DNS addressing constructs a fixture");
        dns.publish_service(COLLECTOR_NAME, vec![IpAddr::from([127, 0, 0, 1])]);
        let collector = Self::endpoint(launch.collector);
        let endpoint = match launch.provisioning {
            CollectorProvisioning::Option { .. } => TraceCollectorEndpoint::Option(collector),
            CollectorProvisioning::EnvironmentTwin => {
                TraceCollectorEndpoint::EnvironmentTwin(collector)
            }
        };
        let mut options = vec![
            ServerProcessOption::Dns(dns.configuration()),
            ServerProcessOption::TraceExport {
                endpoint,
                service: service.clone(),
            },
        ];
        if let CollectorProvisioning::Option { twin: Some(twin) } = launch.provisioning {
            options.push(ServerProcessOption::Environment {
                variable: "NERVIX_OTEL_OTLP_ENDPOINT".to_owned(),
                value: Self::endpoint(twin),
            });
        }
        if let Some(opentelemetry) = launch.opentelemetry {
            let named = Self::endpoint(opentelemetry);
            for (variable, value) in opentelemetry_environment(&named) {
                options.push(ServerProcessOption::Environment {
                    variable: variable.to_owned(),
                    value,
                });
            }
        }
        let cluster = ServerProcessCluster::start_sized(node_count, &options).await?;
        Ok(Self {
            cluster,
            dns,
            service,
            _root: root,
        })
    }

    /// The URI a process names `receiver` by, through the fixture's DNS name.
    fn endpoint(receiver: &GrpcReceiver) -> String {
        format!("http://{COLLECTOR_NAME}:{}", receiver.port())
    }

    /// The service name each process exports its spans under.
    fn services(&self) -> BTreeSet<String> {
        let mut services = BTreeSet::new();
        for node in self.cluster.node_ids() {
            services.insert(format!("{}-{node}", self.service));
        }
        services
    }

    pub(crate) async fn assert_exports(&self, receiver: &GrpcReceiver) -> io::Result<()> {
        let mut missing = self.services();
        let deadline = PhaseDeadline::after(Duration::from_secs(120));
        loop {
            nervix_primitives::task::consume_budget().await;
            for call in receiver.captured() {
                assert_eq!(
                    call.path,
                    "/opentelemetry.proto.collector.trace.v1.TraceService/Export"
                );
                let export = ExportTraceServiceRequest::decode(call.message.as_slice())
                    .map_err(io::Error::other)?;
                for resource_spans in export.resource_spans {
                    let has_spans = resource_spans.scope_spans.iter().any(|scope| {
                        scope
                            .spans
                            .iter()
                            .any(|span| span.name == "load_dns_configuration")
                    });
                    if !has_spans {
                        continue;
                    }
                    if let Some(resource) = resource_spans.resource {
                        for attribute in resource.attributes {
                            if attribute.key == "service.name"
                                && let Some(value) = attribute.value
                                && let Some(Value::StringValue(service)) = value.value
                            {
                                missing.remove(&service);
                            }
                        }
                    }
                }
            }
            if missing.is_empty() {
                assert!(self.dns.questions_for_name(COLLECTOR_NAME) > 0);
                return Ok(());
            }
            if deadline.has_passed() {
                return Err(io::Error::other(format!(
                    "no exported startup spans from {missing:?}; collector DNS questions: {}, \
                     captured exports: {}",
                    self.dns.questions_for_name(COLLECTOR_NAME),
                    receiver.captured().len()
                )));
            }
            deadline.pause(Duration::from_millis(100)).await;
        }
    }

    /// Requires that nothing an OpenTelemetry environment variable asks for reached `receiver`:
    /// no request header it names and no compression, no resource attribute or service name it
    /// names, and no span limit it sets, so every exported span keeps its attributes, events and
    /// links. At least one exported span carries attributes, so the limit check is not vacuous.
    pub(crate) fn assert_declared_export_only(&self, receiver: &GrpcReceiver) -> io::Result<()> {
        let services = self.services();
        let mut spans_with_attributes = 0_usize;
        for call in receiver.captured() {
            for header in ENVIRONMENT_HEADERS {
                if !call.header_values(header).is_empty() {
                    return Err(io::Error::other(format!(
                        "an export carried the environment's {header} header"
                    )));
                }
            }
            if call.compressed {
                return Err(io::Error::other(
                    "an export was compressed as the environment asked",
                ));
            }
            let export = ExportTraceServiceRequest::decode(call.message.as_slice())
                .map_err(io::Error::other)?;
            for resource_spans in export.resource_spans {
                if let Some(resource) = &resource_spans.resource {
                    for attribute in &resource.attributes {
                        if attribute.key == ENVIRONMENT_RESOURCE_ATTRIBUTE {
                            return Err(io::Error::other(format!(
                                "an exported resource carries the environment's {} attribute",
                                attribute.key
                            )));
                        }
                        if attribute.key != "service.name" {
                            continue;
                        }
                        let Some(value) = &attribute.value else {
                            return Err(io::Error::other(
                                "an export names a service without a value",
                            ));
                        };
                        let Some(Value::StringValue(service)) = &value.value else {
                            return Err(io::Error::other(format!(
                                "an export names service {value:?}, which is not text"
                            )));
                        };
                        if !services.contains(service) {
                            return Err(io::Error::other(format!(
                                "an export names service {service}, not one of {services:?}"
                            )));
                        }
                    }
                }
                for scope in resource_spans.scope_spans {
                    for span in scope.spans {
                        let dropped = span.dropped_attributes_count != 0
                            || span.dropped_events_count != 0
                            || span.dropped_links_count != 0;
                        if dropped {
                            return Err(io::Error::other(format!(
                                "span {} dropped {} attributes, {} events and {} links under the \
                                 environment's span limits",
                                span.name,
                                span.dropped_attributes_count,
                                span.dropped_events_count,
                                span.dropped_links_count
                            )));
                        }
                        if !span.attributes.is_empty() {
                            spans_with_attributes += 1;
                        }
                    }
                }
            }
        }
        if spans_with_attributes == 0 {
            return Err(io::Error::other(
                "no exported span carries an attribute, so the span limits went unchecked",
            ));
        }
        Ok(())
    }

    pub(crate) fn signal_shutdown(&mut self) -> io::Result<()> {
        for node in self.cluster.node_ids() {
            self.cluster.signal(&node, Signal::SIGTERM)?;
        }
        Ok(())
    }

    pub(crate) async fn assert_shutdown(&mut self) -> io::Result<()> {
        for node in self.cluster.node_ids() {
            nervix_primitives::task::consume_budget().await;
            let status = self.cluster.wait_for_exit(&node).await?;
            if !status.success() {
                return Err(io::Error::other(format!("{node} exited with {status}")));
            }
        }
        Ok(())
    }
}
