//! HTTP paced-source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the HTTP connector with host-owned cadence, intake, quiescence and
//!   readiness.
//! - **Depends on.** The connector source contract, typed HTTP plans and pre-resolved runtime
//!   handles.
//! - **Must not know.** HTTP request or response details, NSPL parsing, registry validation, or
//!   placement computation.

use nervix_connector::{SourceAckPolicy, SourceCapabilities, SourceConnector, SourcePlan};

use super::{
    super::*,
    http_source::{HttpSource, HttpSourcePlan},
    source::{RuntimeSourceHost, RuntimeSourceHostSpec, run_paced_source},
};

pub(in crate::runtime) struct HttpIngestor;

impl HttpIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: HttpIngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let HttpIngestorStartPlan {
            ingestor,
            client,
            every,
        } = plan;
        let domain = &ingestor.domain;
        let key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.name.clone());
        if runtime.inner.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }

        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
        let resolved_client = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
        let acknowledgement = SourceAckPolicy::None;
        let source_plan = SourcePlan {
            connector: HttpSourcePlan {
                config: resolved_client.entries,
            },
            capabilities: SourceCapabilities::new(
                ingestor.allow_header_reads,
                ingestor.metadata_kind.source_scope(),
                ingestor.quiesce.supports(ingestor.quiesce.mode()),
                NonZeroU64::MIN,
                acknowledgement.support(),
            ),
            acknowledgement,
        };
        let source = HttpSource::open(&source_plan.connector, 0)
            .await
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
        let cadence = runtime
            .bind_domain_cadence(domain, every, DomainCadenceStart::Immediate)
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
        let branched_runtime = runtime.start_branched_ingestor_runtime(
            domain,
            &ingestor.name,
            dependencies.branched_templates,
        );
        let quiesce = runtime
            .ingestor_quiesce_control(domain, &ingestor.name)
            .verified(
                "the runtime registers quiesce control for an ingestor before it starts the task",
            );
        runtime.prepare_ingestor_readiness(
            domain,
            &ingestor.name,
            source_plan.capabilities.instances(),
        );

        let (shutdown_tx, _) = watch::channel(false);
        let host = RuntimeSourceHost::new(RuntimeSourceHostSpec {
            runtime: runtime.clone(),
            domain: domain.clone(),
            ingestor: ingestor.name.clone(),
            timestamp_source: ingestor.timestamp_source,
            output_routes: dependencies.output_routes,
            filter_where: dependencies.filter_where,
            codec: dependencies.codec,
            metrics: dependencies.metrics,
            branched_senders: branched_runtime.senders.clone(),
            quiesce,
            shutdown: shutdown_tx.subscribe(),
            instance_index: 0,
            metadata_kind: ingestor.metadata_kind,
        });
        let shutdown = shutdown_tx.subscribe();
        let task_domain = domain.clone();
        let task_ingestor = ingestor.name.clone();
        let client_mounts = resolved_client.mounts;
        let task = tokio::spawn(async move {
            let _client_mounts = client_mounts;
            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                every = %every,
                "started HTTP ingestor"
            );
            run_paced_source(source, host, cadence, shutdown).await;
            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                "stopped HTTP ingestor"
            );
        });

        runtime.inner.ingestors.insert(
            key,
            IngestorRuntime::Background {
                shutdown: shutdown_tx,
                branched: branched_runtime.runtimes,
                tasks: vec![task],
            },
        );
        Ok(())
    }
}
