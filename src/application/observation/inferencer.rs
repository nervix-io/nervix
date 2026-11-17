//! Layer: control plane.
//! Owns: inference descriptions composed from configuration, placement and typed owner reports.
//! Depends on: validated Models, pure state identity/size decisions, runtime observation and interconnect.
//! Must not know: tensor values, publication mutation or inference execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "composes typed inference descriptions")
)]

use error_stack::{Report, ResultExt as _};
use nervix_interconnect::{
    DescribeInferencerRequest, RemoteOperationFailure, RemoteOperationSubject, RuntimeState,
};
use nervix_models::{
    ClusterNodeName, CreateInferencer, DescribeInferencer, DomainName, DomainNodeRef,
    DomainNodeUnitRef, InferencerExecutionMode, InferencerStateSummary, Model, ModelKind, NodeRef,
    ScheduledNode, Unit,
};
use nervix_primitives::task::consume_budget;

use super::{CommandResult, ObservationError, SessionServiceImpl, command_error, command_ok};
use crate::registry::{InferencerStatePlan, inferencer_model_fingerprint};

/// One typed envelope owns every rendered fact; tensor declarations remain semantic Models.
struct InferencerDescription {
    definition: CreateInferencer,
    execution: InferencerExecutionMode,
    bytes_per_branch: u64,
    units: Vec<InferencerUnitDescription>,
}

struct InferencerUnitDescription {
    unit: Unit,
    owner: Option<ClusterNodeName>,
    replicas: Vec<ClusterNodeName>,
    state: Option<InferencerStateSummary>,
}

fn state_identity(node: &ScheduledNode) -> Option<RuntimeState> {
    inferencer_model_fingerprint(node).map(|model| RuntimeState::Inferencer {
        schema: node.schema_fingerprint,
        model,
    })
}

impl SessionServiceImpl {
    pub(in crate::application) async fn describe_inferencer(
        &self,
        domain: &DomainName,
        describe: DescribeInferencer,
    ) -> CommandResult {
        let described = match self
            .described_model::<CreateInferencer>(domain, &describe.name)
            .await
        {
            Ok(Some(model)) => model,
            Ok(None) => {
                return command_error(format!(
                    "inferencer '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read inferencer '{}': {error:#}",
                    describe.name.as_str()
                ));
            }
        };
        let execution = match described.config.execution_mode() {
            Ok(mode) => mode,
            Err(error) => {
                return command_error(format!("failed to describe inference execution: {error:#}"));
            }
        };
        if let Some(node) = &described.scheduled {
            let consistent = matches!(node.config.as_ref(), Model::Inferencer(model) if model == &described.config);
            if !consistent {
                return command_error(format!(
                    "{:#}",
                    Report::new(ObservationError::InferencerConfigurationChanged {
                        domain: domain.clone(),
                        name: describe.name.clone(),
                    })
                ));
            }
        }
        let bytes_per_branch = InferencerStatePlan::bytes_from_validated(&described.config.state);
        let mut units = Vec::new();
        if let Some(node) = &described.scheduled {
            let expected = state_identity(node);
            for (unit, placement) in &node.units {
                consume_budget().await;
                let owner = placement.execution_node().cloned();
                let replicas = placement.replica_nodes().into_iter().cloned().collect();
                let state = if let Some(owner) = &owner {
                    let request = DescribeInferencerRequest {
                        domain: domain.clone(),
                        name: describe.name.clone(),
                        unit: *unit,
                        state: expected,
                    };
                    let result = if owner == self.inner.consensus.local_node_id() {
                        self.handle_describe_inferencer_request(request)
                            .await
                            .map_err(|failure| {
                                Report::new(ObservationError::RemoteDescribeFailure { failure })
                            })
                    } else {
                        match self
                            .inner
                            .interconnect
                            .request(owner, request)
                            .await
                            .change_context(ObservationError::RemoteDescribeRequest {
                                domain: domain.clone(),
                                entity: NodeRef::new(ModelKind::Inferencer, describe.name.clone()),
                                node: owner.clone(),
                            }) {
                            Ok(result) => result.map_err(|failure| {
                                Report::new(ObservationError::RemoteDescribeFailure { failure })
                            }),
                            Err(error) => Err(error),
                        }
                    };
                    match result {
                        Ok(summary) => Some(summary),
                        Err(error) => return command_error(format!("{error:#}")),
                    }
                } else {
                    None
                };
                units.push(InferencerUnitDescription {
                    unit: *unit,
                    owner,
                    replicas,
                    state,
                });
            }
        }
        let description = InferencerDescription {
            definition: described.config,
            execution,
            bytes_per_branch,
            units,
        };
        match description.render() {
            Ok(text) => command_ok(text),
            Err(error) => command_error(format!(
                "failed to render inferencer description: {error:#}"
            )),
        }
    }

    pub(in crate::application) async fn handle_describe_inferencer_request(
        &self,
        request: DescribeInferencerRequest,
    ) -> Result<InferencerStateSummary, RemoteOperationFailure> {
        let node = self
            .prepare_owner_control_request(&request.domain, ModelKind::Inferencer, &request.name)
            .await?;
        let subject = RemoteOperationSubject::entity(
            &request.domain,
            ModelKind::Inferencer,
            request.name.clone(),
        );
        if !node.unit_executes_on(request.unit, self.inner.consensus.local_node_id())
            || state_identity(&node) != request.state
        {
            return Err(RemoteOperationFailure::Rejected { subject });
        }
        let Model::Inferencer(model) = node.config.as_ref() else {
            return Err(RemoteOperationFailure::Rejected { subject });
        };
        let Some(unit) = node.units.get(&request.unit) else {
            return Err(RemoteOperationFailure::Rejected { subject });
        };
        let bytes = InferencerStatePlan::bytes_from_validated(&model.state);
        let replicas = unit.replica_nodes().into_iter().cloned().collect();
        self.inner
            .runtime
            .describe_inferencer_unit_state(
                DomainNodeUnitRef::new(
                    DomainNodeRef::node_in(request.domain, ModelKind::Inferencer, request.name),
                    request.unit,
                ),
                request.state,
                bytes,
                replicas,
            )
            .await
            .map_err(|error| RemoteOperationFailure::failed(subject, format!("{error:#}")))
    }
}

impl InferencerDescription {
    fn render(&self) -> Result<String, Report<nervix_models::CanonicalPlexusError>> {
        let execution = match self.execution {
            InferencerExecutionMode::PerMessage => "per message",
            InferencerExecutionMode::Batched => "batched",
        };
        let mut lines = vec![
            format!("inferencer: {}", self.definition.name.as_str()),
            format!(
                "resource: {}@{}",
                self.definition.resource.as_str(),
                self.definition.resource_version
            ),
            format!("file: {}", self.definition.file),
            format!("execution: {execution}"),
            format!("state bytes per branch: {}", self.bytes_per_branch),
            "definition:".to_owned(),
        ];
        lines.extend(
            self.definition
                .to_canonical_plexus()?
                .lines()
                .map(|line| format!("  {line}")),
        );
        for unit in &self.units {
            lines.push(format!("unit: {:?}", unit.unit));
            let owner = match &unit.owner {
                Some(node) => node.as_str(),
                None => "unavailable",
            };
            lines.push(format!("  owner: {owner}"));
            lines.push(format!(
                "  replicas: {}",
                unit.replicas
                    .iter()
                    .map(|node| node.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            if let Some(state) = &unit.state {
                lines.push(format!("  branches: {}", state.branches));
                lines.push(format!("  state bytes: {}", state.state_bytes));
                lines.push(format!(
                    "  published revision: {}",
                    revision(state.published_revision)
                ));
                lines.push(format!(
                    "  persisted revision: {}",
                    revision(state.persisted_revision)
                ));
                for replica in &state.replicas {
                    lines.push(format!(
                        "  replica {} replicated revision: {}",
                        replica.node.as_str(),
                        revision(replica.replicated_revision)
                    ));
                }
            } else {
                lines.push("  state: unavailable".to_owned());
            }
        }
        Ok(lines.join("\n"))
    }
}

fn revision(value: Option<u64>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "unavailable".to_owned(),
    }
}
