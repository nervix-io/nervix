use super::super::*;

pub(in crate::runtime) struct EndpointIngestor;

impl EndpointIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        domain: &Domain,
        endpoint: CreateEndpoint,
        ingestor: CreateIngestor,
    ) -> Result<(), RuntimeError> {
        let key = RuntimeKey::new(domain.clone(), ingestor.name.clone());
        if runtime.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let route = {
            let Some(execution) = runtime.executions.get(domain) else {
                return Err(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: "domain execution is not instantiated".to_string(),
                });
            };
            execution
                .endpoint_routes
                .get(&endpoint.name)
                .cloned()
                .ok_or_else(|| RuntimeError::StartIngestor {
                    domain: domain.as_str().to_string(),
                    ingestor: ingestor.name.as_str().to_string(),
                    reason: format!("endpoint '{}' is not instantiated", endpoint.name.as_str()),
                })?
        };

        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
        let branched_runtime = runtime.start_branched_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.branched_templates,
        );
        let binding = EndpointIngestBinding {
            runtime_key: key.clone(),
            domain: domain.clone(),
            ingestor: ingestor.name.clone(),
            timestamp_source: ingestor.timestamp_source.clone(),
            output_routes: dependencies.output_routes,
            filter_where: dependencies.filter_where,
            codec: dependencies.codec,
            branching: dependencies.branching,
            branch_value_mappings: dependencies.branch_value_mappings.clone(),
            branched_senders: branched_runtime.senders.clone(),
        };

        let route_keys = route
            .hostnames
            .iter()
            .map(|host| HttpRouteKey {
                host: host.clone(),
                path: route.path.clone(),
            })
            .collect::<Vec<_>>();

        for route_key in &route_keys {
            runtime
                .endpoint_bindings
                .entry(route_key.clone())
                .or_default()
                .push(binding.clone());
        }

        runtime.ingestors.insert(
            key,
            IngestorRuntime::Endpoint {
                route_keys,
                branched: branched_runtime.runtimes,
            },
        );

        Ok(())
    }
}
