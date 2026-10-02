//! Node-local binding of scheduled processor plans.
//!
//! Layer: data plane.
//! - **Owns.** Preparing the complete processor-plan map for one installed domain revision.
//! - **Depends on.** Decision-layer processor specifications and installed runtime capabilities.
//! - **Must not know.** NSPL text, parser state or control-plane transaction mechanics.

#![cfg_attr(
    nervix_lint,
    nervix::context(
        lifecycle,
        reason = "processor plans are prepared and published once per installed domain revision"
    )
)]

use error_stack::{Report, ResultExt as _};

use super::*;

impl Runtime {
    pub(in crate::runtime) async fn bind_installed_processor_plans(
        &self,
        domain: &DomainName,
        revision: &ExecutionRevision,
    ) -> error_stack::Result<HashMap<NodeRef, StdArc<PublishedProcessorPlan>>, RuntimeError> {
        let dispatcher = self.inner.remote_dispatcher.load_full();
        let local_node_id = dispatcher.as_deref().map(RemoteDispatcher::local_node_id);
        let specs = revision
            .processors
            .processors
            .iter()
            .filter(|spec| {
                revision
                    .nodes
                    .get(&NodeRef::new(spec.spec.kind, spec.spec.processor.clone()))
                    .is_some_and(|node| Self::scheduled_node_executes_locally(node, local_node_id))
            })
            .cloned()
            .collect::<Vec<_>>();
        let routing = {
            let Some(execution) = self.inner.executions.get(domain) else {
                return Err(Report::new(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: "domain execution is unavailable while binding processor plans"
                        .to_string(),
                }));
            };
            (*execution.routing).clone()
        };
        bind_published_processor_plans(
            &specs,
            ProcessorPlanBindingContext {
                runtime: self,
                domain,
                relay_schemas: &routing.relay_schemas,
                relay_services: &routing.relay_services,
                relay_branchings: &routing.relay_branchings,
                materialized_stream_specs: &routing.materialized_stream_specs,
                lookups: &routing.lookups,
                udfs: Some(&routing.udfs),
                previous: &routing.processor_plans,
            },
        )
        .await
        .change_context(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: "failed to bind published processor plans".to_string(),
        })
    }

    pub(in crate::runtime) async fn bind_installed_processor_plan(
        &self,
        domain: &DomainName,
        spec: &BranchedProcessorNodeSpec,
    ) -> error_stack::Result<StdArc<PublishedProcessorPlan>, RuntimeError> {
        let routing = {
            let Some(execution) = self.inner.executions.get(domain) else {
                return Err(Report::new(RuntimeError::BuildDomainExecution {
                    domain: domain.as_str().to_string(),
                    reason: "domain execution is unavailable while binding a processor plan"
                        .to_string(),
                }));
            };
            (*execution.routing).clone()
        };
        let mut plans = bind_published_processor_plans(
            std::slice::from_ref(spec),
            ProcessorPlanBindingContext {
                runtime: self,
                domain,
                relay_schemas: &routing.relay_schemas,
                relay_services: &routing.relay_services,
                relay_branchings: &routing.relay_branchings,
                materialized_stream_specs: &routing.materialized_stream_specs,
                lookups: &routing.lookups,
                udfs: Some(&routing.udfs),
                previous: &routing.processor_plans,
            },
        )
        .await
        .change_context(RuntimeError::BuildDomainExecution {
            domain: domain.as_str().to_string(),
            reason: "failed to bind published processor plan".to_string(),
        })?;
        let identity = NodeRef::new(spec.spec.kind, spec.spec.processor.clone());
        let Some(plan) = plans.remove(&identity) else {
            return Err(Report::new(RuntimeError::BuildDomainExecution {
                domain: domain.as_str().to_string(),
                reason: format!(
                    "processor plan binder omitted {} '{}'",
                    identity.kind.as_str(),
                    identity.identifier.as_str()
                ),
            }));
        };
        Ok(plan)
    }
}
