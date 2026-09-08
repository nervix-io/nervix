use std::borrow::Cow;

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

/// Every domain publishing one exact host and path. A request resolves the host and path by key
/// and then reads this map, which holds one entry per domain that claims that exact pair.
pub(super) type RoutedEndpointsByDomain = HashMap<DomainName, RoutedEndpoint>;

#[derive(Clone)]
pub(super) struct EndpointIngestBinding {
    pub(super) runtime_key: DomainNodeRef,
    pub(super) quiesce: Arc<IngestorQuiesceControl>,
    pub(super) domain: DomainName,
    pub(super) ingestor: IngestorName,
    pub(super) timestamp_source: Option<IngestTimestampSource>,
    pub(super) output_routes: RelayProcessorOutputsNode,
    pub(super) filter_where: Option<CompiledProgramWithMaterializedInterest>,
    pub(super) codec: Arc<CompiledCodec>,
    pub(super) branched_senders: HashMap<RelayName, mpsc::Sender<BranchedEntrypointInput>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EndpointDispatchOutcome {
    pub accepted: usize,
    pub rejected: usize,
    pub retry_after: Option<Duration>,
}

impl EndpointDispatchOutcome {
    pub fn is_accepted(self) -> bool {
        self.accepted > 0
    }
}

pub(super) fn normalize_http_host(host: &str) -> String {
    host.split(':')
        .next()
        .unwrap_or(host)
        .trim()
        .to_ascii_lowercase()
}

impl Runtime {
    pub async fn has_websocket_endpoint(&self, host: &str, path: &str) -> bool {
        self.has_endpoint(host, path, EndpointType::Websockets)
            .await
    }

    pub async fn websocket_endpoint_signaling_protocol(
        &self,
        host: &str,
        path: &str,
    ) -> Option<Arc<CompiledSignalingProtocol>> {
        let key = HttpRouteKey {
            host: normalize_http_host(host),
            path: path.to_string(),
        };
        let domains = self.inner.routed_endpoints.get(&key)?;
        domains
            .values()
            .find(|endpoint| endpoint.endpoint_type == EndpointType::Websockets)
            .and_then(|endpoint| endpoint.signaling_protocol.clone())
    }

    pub(in crate::runtime) async fn signaling_protocol(
        &self,
        domain: &DomainName,
        signaling_protocol: &SignalingProtocolName,
    ) -> Option<Arc<CompiledSignalingProtocol>> {
        let execution = self.inner.executions.get(domain)?;
        execution
            .signaling_protocols
            .get(signaling_protocol)
            .cloned()
    }

    pub async fn has_http_endpoint(&self, host: &str, path: &str) -> bool {
        self.has_endpoint(host, path, EndpointType::Http).await
    }

    pub(in crate::runtime) async fn has_endpoint(
        &self,
        host: &str,
        path: &str,
        endpoint_type: EndpointType,
    ) -> bool {
        let key = HttpRouteKey {
            host: normalize_http_host(host),
            path: path.to_string(),
        };
        self.inner
            .routed_endpoints
            .get(&key)
            .is_some_and(|domains| {
                domains
                    .values()
                    .any(|endpoint| endpoint.endpoint_type == endpoint_type)
            })
    }

    pub(crate) async fn dispatch_websocket_payload(
        &self,
        host: &str,
        path: &str,
        payload: &[u8],
        headers: &dyn IngestMessageHeaders,
    ) -> EndpointDispatchOutcome {
        self.dispatch_endpoint_payload(host, path, payload, headers, "websocket")
            .await
    }

    pub async fn websocket_endpoint_admission(
        &self,
        host: &str,
        path: &str,
    ) -> EndpointDispatchOutcome {
        let route_key = HttpRouteKey {
            host: normalize_http_host(host),
            path: path.to_string(),
        };
        let bindings = self
            .inner
            .endpoint_bindings
            .get(&route_key)
            .map(|bindings| bindings.clone())
            .unwrap_or_default();
        let mut outcome = EndpointDispatchOutcome::default();
        let mut retry_after = Vec::new();
        for binding in &bindings {
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

    pub(crate) async fn dispatch_http_payload(
        &self,
        host: &str,
        path: &str,
        payload: &[u8],
        headers: &dyn IngestMessageHeaders,
    ) -> EndpointDispatchOutcome {
        self.dispatch_endpoint_payload(host, path, payload, headers, "http")
            .await
    }

    pub(in crate::runtime) async fn dispatch_endpoint_payload(
        &self,
        host: &str,
        path: &str,
        payload: &[u8],
        headers: &dyn IngestMessageHeaders,
        protocol: &str,
    ) -> EndpointDispatchOutcome {
        let route_key = HttpRouteKey {
            host: normalize_http_host(host),
            path: path.to_string(),
        };
        let bindings = {
            self.inner
                .endpoint_bindings
                .get(&route_key)
                .map(|bindings| bindings.clone())
                .unwrap_or_default()
        };

        let mut outcome = EndpointDispatchOutcome::default();
        let mut retry_after = Vec::new();
        for binding in &bindings {
            // A binding may buffer this request until it resumes, so its copy of the
            // request headers is taken here and only here.
            let payload = BufferedIngestPayload::new(
                payload,
                BufferedIngestMetadata::Headers(RetainedIngestHeaders::capture(headers)),
            );
            match binding.quiesce.intake(0, payload, true) {
                IngestorQuiesceIntake::Dispatch(payload) => {
                    outcome.accepted = outcome
                        .accepted
                        .checked_add(1)
                        .assured("the bindings counted here are endpoint routes held in memory");
                    self.dispatch_endpoint_binding(binding, payload, protocol)
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

    pub(in crate::runtime) async fn dispatch_endpoint_binding(
        &self,
        binding: &EndpointIngestBinding,
        payload: BufferedIngestPayload,
        protocol: &str,
    ) {
        // One request is one group, so its builders are sized for the single row this binding
        // decodes.
        let mut collector = IngestRouteCollector::new(IngestMetadataKind::Headers, 1);
        match collector
            .decode_payload(&binding.codec, Cow::Borrowed(payload.payload()))
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
                        ingested_at: current_timestamp(),
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
                         {}",
                        binding.ingestor.as_str(),
                        binding.domain.as_str(),
                        error
                    ));
                    warn!(
                        domain = binding.domain.as_str(),
                        ingestor = binding.ingestor.as_str(),
                        error = %error,
                        protocol,
                        "failed to dispatch endpoint message"
                    );
                }
            }
            Err(error) => {
                self.inner.events.report_error(format!(
                    "failed to decode {protocol} message for ingestor '{}' in domain '{}': {}",
                    binding.ingestor.as_str(),
                    binding.domain.as_str(),
                    error
                ));
                warn!(
                    domain = binding.domain.as_str(),
                    ingestor = binding.ingestor.as_str(),
                    error = %error,
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
    }
}
