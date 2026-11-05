//! Public restore qualification for committed model-cap policy.
//!
//! Layer: test harness.
//! - **Owns.** Observing strong model publication beside the restore's typed cap refusal.
//! - **Depends on.** Existing restore/runtime barriers and authoritative consensus observation.
//! - **Must not know.** Registry storage, publication implementation or a second model form.

use cucumber::then;
use meticulous::ResultExt as _;
use nervix_consensus::{CommandExecutionDisposition, CommandExecutionState};
use nervix_models::{
    ClusterNodeName, ClusterSchedule, DomainName, ModelKind, ModelName, NodeRef, RestoreStep,
};

use super::{
    RESTORE_PAUSE_TIMEOUT, ScenarioWorld, append_cucumber_log_line, expand_placeholders,
    scenario_domain,
};
use crate::common::cluster::node_name;

/// A held installer keeps a successful wrong publication waiting before activation; the
/// restore owns the target's only mutation lease. The bad model consequently remains present
/// until the barrier is released. This oracle does not require every watch revision to arrive.
fn publication_problem(
    schedule: &ClusterSchedule,
    domain: &DomainName,
    ingestor: &NodeRef,
    revision: u64,
) -> Option<String> {
    schedule
        .domains
        .get(domain)
        .is_some_and(|domain| domain.nodes.contains_key(ingestor))
        .then(|| {
            format!(
                "over-cap ingestor '{}' was strongly published in domain '{}' while committed \
                 http-header-fields=1 revision={revision} and runtime installation is held",
                ingestor.identifier, domain,
            )
        })
}

/// Unblock both existing barriers before either a successful return or a failed assertion.
fn release_restore_parameter_barriers(world: &mut ScenarioWorld, node: &ClusterNodeName) {
    if let Some(pause) = world.restore_step_pause.take() {
        world
            .fault_injection
            .release_restore_step_pause(&node_name(&pause.node), &pause.step);
    }
    world
        .fault_injection
        .release_runtime_installation_pause(node);
}

#[then(
    expr = "node {string} records the restore's lower header cap refusal before publishing \
            ingestor {string} in domain {string}"
)]
async fn then_restore_lower_header_cap_refuses_before_publication(
    world: &mut ScenarioWorld,
    node: String,
    ingestor: String,
    domain: String,
) {
    let node = node_name(&expand_placeholders(world, &node));
    let domain = scenario_domain(world, &domain);
    let ingestor = expand_placeholders(world, &ingestor);
    let model = NodeRef::new(
        ModelKind::Ingestor,
        ModelName::try_from(ingestor.as_str()).assured("the scenario names an ingestor"),
    );
    let reference = world
        .background_restore
        .as_ref()
        .map(|restore| restore.reference.clone());
    let observer = world.fault_injection.consensus_observer(&node);
    let mut applied = observer.subscribe_applied();
    let expected_step = RestoreStep::ApplyModels(domain.clone());
    let observation = nervix_primitives::time::timeout(RESTORE_PAUSE_TIMEOUT, async {
        let reference = reference
            .as_ref()
            .ok_or_else(|| "the restore must be running at its held model step".to_owned())?;
        loop {
            nervix_primitives::task::consume_budget().await;
            if observer.current_leader_now() != Some(node.clone()) {
                return Err("the held node stopped coordinating the controlled restore".to_owned());
            }
            let parameters = observer
                .cluster_parameters()
                .ok_or_else(|| "the live cluster has no committed parameter record".to_owned())?;
            if parameters.values.http_header_fields() != 1 {
                return Err("the controlled lower header cap is not committed".to_owned());
            }
            let schedule = observer.current_schedule().await;
            if let Some(problem) =
                publication_problem(&schedule, &domain, &model, parameters.revision.get())
            {
                return Err(problem);
            }
            let execution = observer
                .current_command_execution(reference)
                .await
                .ok_or_else(|| "the admitted restore lost its execution identity".to_owned())?;
            match execution.state {
                CommandExecutionState::Finished { result, .. } => {
                    if !matches!(&result.disposition, CommandExecutionDisposition::Failed) {
                        return Err(format!("the restore recorded no typed failure: {result:?}"));
                    }
                    let report = result
                        .restore
                        .as_ref()
                        .ok_or_else(|| "the restore cap refusal retained no report".to_owned())?;
                    if report.failed_step() != Some(&expected_step) {
                        return Err(format!("the restore failed at another step: {report:?}"));
                    }
                    if !result.message.contains("http-header-fields")
                        || !result.message.contains(&ingestor)
                    {
                        return Err(format!(
                            "the refusal must name the header cap and owning ingestor: {}",
                            result.message,
                        ));
                    }
                    let schedule = observer.current_schedule().await;
                    if let Some(problem) =
                        publication_problem(&schedule, &domain, &model, parameters.revision.get())
                    {
                        return Err(problem);
                    }
                    if schedule
                        .domains
                        .get(&domain)
                        .is_some_and(|domain| !domain.nodes.is_empty())
                    {
                        return Err("the refused atomic model run left target models".to_owned());
                    }
                    return Ok(format!(
                        "restore parameter refusal before model publication: \
                         reference={reference} domain={domain} parameters_revision={} \
                         failed_step={expected_step} reason={}",
                        parameters.revision, result.message,
                    ));
                }
                CommandExecutionState::Applying { .. } => {}
                CommandExecutionState::Expired => {
                    return Err(
                        "the controlled restore expired instead of refusing its model".to_owned(),
                    );
                }
            }
            applied.changed().await.map_err(|_| {
                "the held coordinator ended before its barrier was released".to_owned()
            })?;
        }
    })
    .await;
    release_restore_parameter_barriers(world, &node);
    match observation {
        Ok(Ok(message)) => append_cucumber_log_line(&message),
        Ok(Err(problem)) => {
            append_cucumber_log_line(&format!(
                "restore parameter publication observation: {problem}"
            ));
            panic!("{problem}");
        }
        Err(_) => panic!(
            "the restore neither refused its header cap nor published the model within the \
             existing restore-pause budget"
        ),
    }
}
