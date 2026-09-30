//! HTTP endpoint ingestion boundary: the request path of the endpoint source.
//!
//! Layer: data plane.
//! - **Owns.** Resolving a request's host and path to the endpoint routes and the ingestors bound
//!   to them, request admission, the endpoint buffer, rejection with a retry delay, per-request
//!   dispatch, and response disposition.
//! - **Depends on.** The request intakes endpoint sources bind, Arrow decoding and actual-UTC
//!   observation.
//! - **Must not know.** When a route is bound or unbound, which the endpoint source owns; NSPL
//!   parsing, placement policy or consensus storage.

use std::borrow::Cow;

use nervix_connector::{
    IngestMessageHeaders, RetainedIngestHeaders, physical_time::actual_utc_now,
};
use nervix_connector_websockets::CompiledSignalingProtocol;

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct HttpRouteKey {
    pub(super) host: String,
    pub(super) path: String,
}

#[derive(Debug, Clone)]
pub(super) struct EndpointRoute {
    pub(super) path: String,
    pub(super) hostnames: Vec<String>,
    pub(super) endpoint_type: EndpointType,
    pub(super) signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
}

/// One instantiated endpoint route as inbound HTTP and WebSocket routing sees it, without the
/// configuration a request never consults.
#[derive(Debug, Clone)]
pub(super) struct RoutedEndpoint {
    pub(super) endpoint_type: EndpointType,
    pub(super) signaling_protocol: Option<Arc<CompiledSignalingProtocol>>,
}

/// The intake one ingestor admits an endpoint's requests through, bound to every route the
/// endpoint publishes while the ingestor's endpoint source runs.
pub(crate) struct EndpointIngestBinding {
    pub(super) quiesce: Arc<IngestorQuiesceControl>,
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    pub(super) timestamp_source: Option<IngestTimestampSource>,
    pub(super) output_routes: Arc<BoundIngestorRoutes>,
    pub(super) filter_where: Option<CompiledProgramWithMaterializedInterest>,
    pub(super) codec: Arc<CompiledCodec>,
    pub(super) metrics: MessageMetricsHandle,
    pub(super) branched_senders: HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct EndpointDispatchOutcome {
    pub(in crate::runtime) accepted: usize,
    pub(in crate::runtime) rejected: usize,
    pub(crate) retry_after: Option<Duration>,
}

impl EndpointDispatchOutcome {
    pub(crate) fn is_accepted(self) -> bool {
        self.accepted > 0
    }
}

pub(crate) type ResolvedEndpointRoute = Arc<EndpointIntakeRoute<EndpointIngestBinding>>;

pub(super) fn normalize_http_host(host: &str) -> Cow<'_, str> {
    let host = host.split(':').next().unwrap_or(host).trim();
    if host.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Cow::Owned(host.to_ascii_lowercase())
    } else {
        Cow::Borrowed(host)
    }
}

impl Runtime {
    pub(crate) fn resolve_endpoint(&self, host: &str, path: &str) -> Option<ResolvedEndpointRoute> {
        self.inner.endpoint_intake_routes.resolve(host, path)
    }

    pub(in crate::runtime) async fn signaling_protocol(
        &self,
        domain: &DomainName,
        signaling_protocol: &SignalingProtocolName,
    ) -> Option<Arc<CompiledSignalingProtocol>> {
        let routing = self.domain_routing(domain)?;
        routing
            .load()
            .signaling_protocols
            .get(signaling_protocol)
            .cloned()
    }
}

impl EndpointIntakeRoute<EndpointIngestBinding> {
    pub(crate) fn admission(&self) -> EndpointDispatchOutcome {
        let mut outcome = EndpointDispatchOutcome::default();
        let mut retry_after = Vec::new();
        for lifetime in self.bindings() {
            let lease = lifetime.intake();
            let Some(binding) = lease.as_deref() else {
                outcome.rejected = outcome
                    .rejected
                    .checked_add(1)
                    .assured("the bindings counted here are endpoint routes held in memory");
                retry_after.push(None);
                continue;
            };
            match binding.quiesce.endpoint_admission() {
                Ok(()) => {
                    outcome.accepted = outcome
                        .accepted
                        .checked_add(1)
                        .assured("the bindings counted here are endpoint routes held in memory");
                }
                Err(duration) => {
                    outcome.rejected = outcome
                        .rejected
                        .checked_add(1)
                        .assured("the bindings counted here are endpoint routes held in memory");
                    retry_after.push(duration);
                }
            }
        }
        if outcome.accepted == 0
            && !retry_after.is_empty()
            && retry_after.iter().all(Option::is_some)
        {
            outcome.retry_after = retry_after.into_iter().flatten().max();
        }
        outcome
    }

    pub(crate) async fn dispatch(
        &self,
        runtime: &Runtime,
        payload: &[u8],
        headers: &dyn IngestMessageHeaders,
    ) -> EndpointDispatchOutcome {
        let protocol = match self.endpoint_type() {
            EndpointType::Http => "http",
            EndpointType::Websockets => "websocket",
        };
        let mut outcome = EndpointDispatchOutcome::default();
        let mut retry_after = Vec::new();
        for lifetime in self.bindings() {
            let lease = lifetime.intake();
            let Some(binding) = lease.as_deref() else {
                outcome.rejected = outcome
                    .rejected
                    .checked_add(1)
                    .assured("the bindings counted here are endpoint routes held in memory");
                retry_after.push(None);
                continue;
            };
            // A binding may buffer this request until it resumes, so its copy of the
            // request headers is taken here and only here.
            let payload = BufferedIngestPayload::new(
                payload,
                BufferedIngestMetadata::Headers(RetainedIngestHeaders::capture(headers)),
                actual_utc_now(),
            );
            match binding.quiesce.intake(0, payload, true) {
                IngestorQuiesceIntake::Dispatch(payload) => {
                    outcome.accepted = outcome
                        .accepted
                        .checked_add(1)
                        .assured("the bindings counted here are endpoint routes held in memory");
                    runtime
                        .dispatch_endpoint_binding(binding, payload, protocol)
                        .await;
                }
                IngestorQuiesceIntake::Buffered => {
                    outcome.accepted = outcome
                        .accepted
                        .checked_add(1)
                        .assured("the bindings counted here are endpoint routes held in memory");
                }
                IngestorQuiesceIntake::Dropped => {
                    outcome.rejected = outcome
                        .rejected
                        .checked_add(1)
                        .assured("the bindings counted here are endpoint routes held in memory");
                    retry_after.push(None);
                }
                IngestorQuiesceIntake::Rejected {
                    retry_after: binding_retry_after,
                } => {
                    outcome.rejected = outcome
                        .rejected
                        .checked_add(1)
                        .assured("the bindings counted here are endpoint routes held in memory");
                    retry_after.push(binding_retry_after);
                }
            }
        }
        if outcome.accepted == 0
            && !retry_after.is_empty()
            && retry_after.iter().all(Option::is_some)
        {
            outcome.retry_after = retry_after.into_iter().flatten().max();
        }
        outcome
    }
}

impl Runtime {
    async fn dispatch_endpoint_binding(
        &self,
        binding: &EndpointIngestBinding,
        payload: BufferedIngestPayload,
        protocol: &str,
    ) {
        // One request is one group, so its builders are sized for the single payload this binding
        // decodes; a payload that unfolds into several messages grows them.
        let mut collector =
            IngestRouteCollector::new(IngestMetadataKind::Headers, 1, binding.metrics.clone());
        match collector
            .decode_payload(&binding.codec, payload.payload())
            .await
        {
            Ok(()) => {
                let metadata = [payload.first_metadata_row()];
                let dispatch_result = self
                    .dispatch_ingested_records(IngestGroupDispatch {
                        collector: &mut collector,
                        domain: &binding.domain,
                        ingestor: &binding.ingestor,
                        timestamp_source: binding.timestamp_source.as_ref(),
                        output_routes: &binding.output_routes,
                        filter_where: binding.filter_where.as_ref(),
                        metadata: &metadata,
                        ingested_at: payload.observed_at(),
                        acks: vec![AckSet::empty()],
                    })
                    .await;
                let flush_result = self
                    .flush_ingest_collector(
                        &binding.domain,
                        &binding.ingestor,
                        &binding.branched_senders,
                        &mut collector,
                    )
                    .await;
                if let Err(error) = dispatch_result.and(flush_result) {
                    self.inner.events.report_error(format!(
                        "failed to dispatch {protocol} message for ingestor '{}' in domain '{}': \
                         {error:?}",
                        binding.ingestor.as_str(),
                        binding.domain.as_str(),
                    ));
                    warn!(
                        domain = binding.domain.as_str(),
                        ingestor = binding.ingestor.as_str(),
                        error = ?error,
                        protocol,
                        "failed to dispatch endpoint message"
                    );
                }
            }
            Err(error) => {
                // The codec's context names the codec and field; the causes beneath it say why.
                let reason = format!("{error:#}");
                self.inner.events.report_error(format!(
                    "failed to decode {protocol} message for ingestor '{}' in domain '{}': \
                     {reason}",
                    binding.ingestor.as_str(),
                    binding.domain.as_str(),
                ));
                warn!(
                    domain = binding.domain.as_str(),
                    ingestor = binding.ingestor.as_str(),
                    error = %reason,
                    protocol,
                    "failed to decode endpoint message"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_http_host_strips_port_and_normalizes_case() {
        assert_eq!(normalize_http_host(" Example.COM:8080 "), "example.com");
        assert_eq!(normalize_http_host("api.example.com"), "api.example.com");
        assert!(matches!(
            normalize_http_host(" api.example.com:8080 "),
            Cow::Borrowed("api.example.com")
        ));
    }
}
