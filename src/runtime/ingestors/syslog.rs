//! Syslog source runtime composition.
//!
//! Layer: data plane.
//!
//! - **Owns.** Composing the Syslog connector plan with host-owned intake and task lifecycle.
//! - **Depends on.** The connector source contract, the Syslog connector, and pre-resolved runtime
//!   execution handles.
//! - **Must not know.** Syslog socket drivers, framing, TLS configuration, NSPL parsing, registry
//!   validation, or placement computation.

use nervix_connector::{
    ParsedRetryPolicy, SourceAckPolicy, SourceCapabilities, SourceConnector, SourcePlan,
};
use nervix_connector_syslog::{SyslogSource, SyslogSourcePlan};

use super::{
    super::*,
    source::{BrokerSourceHost, BrokerSourceHostSpec, run_source_instance_with_retry},
};

const SYSLOG_RETRY_POLICY: ParsedRetryPolicy = ParsedRetryPolicy {
    backoff: Duration::from_millis(250),
    max_backoff: Duration::from_secs(30),
};

pub(in crate::runtime) struct SyslogIngestor;

impl SyslogIngestor {
    pub(in crate::runtime) async fn start(
        runtime: &Runtime,
        plan: SyslogIngestorStartPlan,
    ) -> Result<(), RuntimeError> {
        let SyslogIngestorStartPlan { ingestor, client } = plan;
        let domain = &ingestor.domain;
        let key =
            DomainNodeRef::node_in(domain.clone(), ModelKind::Ingestor, ingestor.name.clone());
        if runtime.inner.ingestors.contains_key(&key) {
            return Err(RuntimeError::IngestorAlreadyRunning {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
            });
        }
        let resolved = runtime
            .resolve_client_config(domain, client.mount.as_ref(), &client.config)
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
        let connector = SyslogSourcePlan::new(resolved.entries, |configured| {
            runtime.syslog_ingestor_bind_addr(configured)
        })
        .map_err(|error| RuntimeError::StartIngestor {
            domain: domain.as_str().to_string(),
            ingestor: ingestor.name.as_str().to_string(),
            reason: error.to_string(),
        })?;
        let acknowledgement = SourceAckPolicy::None;
        let source_plan = SourcePlan {
            connector,
            capabilities: SourceCapabilities::new(
                ingestor.allow_header_reads,
                ingestor.metadata_kind.source_scope(),
                ingestor.quiesce.supports(ingestor.quiesce.mode()),
                NonZeroU64::MIN,
                acknowledgement.support(),
            ),
            acknowledgement,
        };
        let source = SyslogSource::open(&source_plan.connector, 0)
            .await
            .map_err(|error| RuntimeError::StartIngestor {
                domain: domain.as_str().to_string(),
                ingestor: ingestor.name.as_str().to_string(),
                reason: error.to_string(),
            })?;
        let dependencies = runtime.ingestor_dependencies(domain, &ingestor).await?;
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
        let (shutdown_tx, _) = watch::channel(false);
        runtime.prepare_ingestor_readiness(
            domain,
            &ingestor.name,
            source_plan.capabilities.instances(),
        );
        let host = BrokerSourceHost::build(BrokerSourceHostSpec {
            runtime: runtime.clone(),
            domain: domain.clone(),
            ingestor: ingestor.name.clone(),
            timestamp_source: ingestor.timestamp_source.clone(),
            output_routes: dependencies.output_routes,
            filter_where: dependencies.filter_where,
            codec: dependencies.codec,
            metrics: dependencies.metrics,
            branched_senders: branched_runtime.senders.clone(),
            quiesce,
            shutdown: shutdown_tx.subscribe(),
            instance_index: 0,
            metadata_kind: ingestor.metadata_kind,
            buffered_intake: true,
        });
        let task_domain = domain.clone();
        let task_ingestor = ingestor.name.clone();
        let shutdown = shutdown_tx.subscribe();
        let acknowledgement = source_plan.acknowledgement;
        let client_mounts = resolved.mounts;
        let task = tokio::spawn(async move {
            let _client_mounts = client_mounts;
            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                "started syslog ingestor"
            );
            run_source_instance_with_retry(
                source,
                host,
                acknowledgement,
                SYSLOG_RETRY_POLICY,
                shutdown,
            )
            .await;
            info!(
                domain = task_domain.as_str(),
                ingestor = task_ingestor.as_str(),
                "stopped syslog ingestor"
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
