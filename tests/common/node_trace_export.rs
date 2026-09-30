//! Public evidence for the node's own OTLP trace exporter.
//!
//! Outside the layer order: a harness. It may name any layer, and no product code may name it.
//!
//! - **Owns.** A real-process cluster and the DNS fixture its trace-export endpoint names.
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
    server_process::ServerProcessOption,
    server_process_cluster::ServerProcessCluster,
};

const COLLECTOR_NAME: &str = "node-traces.nervix.test";

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
        receiver: &GrpcReceiver,
        service: String,
    ) -> io::Result<Self> {
        let root = tempfile::tempdir()?;
        let dns = ClusterDns::start(PeerAddressing::DnsNames, root.path(), &[])
            .await?
            .assured("DNS addressing constructs a fixture");
        dns.publish_service(COLLECTOR_NAME, vec![IpAddr::from([127, 0, 0, 1])]);
        let options = [
            ServerProcessOption::Dns(dns.configuration()),
            ServerProcessOption::TraceExport {
                endpoint: format!("http://{COLLECTOR_NAME}:{}", receiver.port()),
                service: service.clone(),
            },
        ];
        let cluster = ServerProcessCluster::start_sized(node_count, &options).await?;
        Ok(Self {
            cluster,
            dns,
            service,
            _root: root,
        })
    }

    pub(crate) async fn assert_exports(&self, receiver: &GrpcReceiver) -> io::Result<()> {
        let mut missing = BTreeSet::new();
        for node in self.cluster.node_ids() {
            missing.insert(format!("{}-{node}", self.service));
        }
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
