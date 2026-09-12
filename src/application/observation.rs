//! Answering DESCRIBE and SHOW, wherever the node that knows the answer is.
//!
//! Layer: control plane.
//!
//! - **Owns.** The describe and show commands, the remote requests they fan out, and the handlers
//!   that answer those requests on the owning node.
//! - **Depends on.** The registry for models and schedules, the runtime for what is running, and
//!   the interconnect to reach the node that owns each answer.
//! - **Must not know.** How the reported state came to be.

use std::collections::BTreeSet;

use arch_into::ArchInto;
use error_stack::Report;
use meticulous::OptionExt as _;
use nervix_dataflow_graph::DataflowNodeHealth;
use nervix_execution::MemoryClass;
use nervix_interconnect::{
    DataflowNodeStatusEnvelope, DataflowNodeStatusRequest as RemoteDataflowNodeStatusRequest,
    DataflowNodeStatusResponse as RemoteDataflowNodeStatusResponse,
    DescribeIngestorRequest as RemoteDescribeIngestorRequest,
    DescribeLookupRequest as RemoteDescribeLookupRequest,
    DescribeMetricsEnvelope as RemoteDescribeMetricsEnvelope,
    DescribeMetricsRequest as RemoteDescribeMetricsRequest,
    DescribeRelayRequest as RemoteDescribeRelayRequest,
    DescribeRelayResponse as RemoteDescribeRelayResponse, LookupDescribeEnvelope,
    LookupRequest as RemoteLookupRequest, LookupResponse as RemoteLookupResponse,
};
use nervix_models::{
    ClusterNodeName, CreateCorrelator, CreateDeduplicator, CreateEmitter, CreateEndpoint,
    CreateIngestor, CreateJunction, CreateLookup, CreatePlacement, CreateReingestor,
    CreateReorderer, CreateUdf, CreateWasmProcessor, CreateWindowProcessor, DescribeCorrelator,
    DescribeDeduplicator, DescribeDomain, DescribeEmitter, DescribeEndpoint, DescribeIngestor,
    DescribeJunction, DescribeLookup, DescribePlacement, DescribeReingestor, DescribeRelay,
    DescribeReorderer, DescribeResource, DescribeUdf, DescribeWasmProcessor,
    DescribeWindowProcessor, DomainName, DomainStatus, LookupName, LookupQuery, Model, ModelKind,
    ModelName, NodeRef, ParseAsType, RelayName, ResourceId, ScheduledNode,
    ShowRelayMaterializedState, UniquelyKindedModel,
};
use nervix_vm::window::{WindowAggregateProgram, lower_window_assignments};
use tokio::time::Duration;
use tracing::warn;

use super::{
    describe_output::{
        append_metrics_lines, dataflow_node_status_from_envelope, dataflow_node_status_to_envelope,
        format_correlator_describe_output, format_deduplicator_describe_output,
        format_emitter_describe_output, format_endpoint_describe_output,
        format_ingestor_describe_output, format_junction_describe_output,
        format_lookup_describe_output, format_materialized_stream_state_output,
        format_placement_runtime_node, format_placement_runtime_nodes,
        format_placement_runtime_nodes_in_context, format_reingestor_describe_output,
        format_relay_describe_output, format_reorderer_describe_output,
        format_wasm_processor_describe_output, format_window_processor_describe_output,
        ordered_placement_corridor, placement_claim_owner, placement_group_host,
        placement_groups_claimed_by_rule, placement_rule_coverage_status,
        placement_rule_endpoint_nodes, placement_rule_runtime_nodes,
        runtime_ingestor_describe_from_envelope,
    },
    model_mutation::{command_error, command_ok},
    session_service::SessionServiceImpl,
    subscription::{
        SubscriptionTarget, branch_key_from_filter, parse_subscription_literal,
        render_subscription_literal, validate_subscription_bindings,
    },
};
use crate::{
    proto::{CommandResult, CommandResultKind, Diagnostic},
    registry::RegistryError,
    resource::{ResourceEntryContent, ResourceManifestEntry},
    runtime::IngestorDescribe as RuntimeIngestorDescribe,
    runtime_schema,
};
const REMOTE_DESCRIBE_RELAY_TIMEOUT: Duration = Duration::from_secs(1);

/// A model as `DESCRIBE` reads it: the configuration it was created with, and the schedule entry
/// that places it while its domain is running.
struct DescribedModel<M> {
    config: M,
    scheduled: Option<ScheduledNode>,
}

/// The hash map a lookup query reaches, as the cluster schedule describes it: the lookup model,
/// the scheduled node that owns it, and the declared type of its key field.
struct LookupTarget {
    lookup: CreateLookup,
    node: ScheduledNode,
    key_ty: ParseAsType,
}

/// The kind and model a dataflow metric identifier addresses, or `None` when `id` is not one.
pub(in crate::application) fn dataflow_metric_target(id: &str) -> Option<(String, ModelName)> {
    let (kind, identifier) = id.split_once(':')?;
    Some((
        kind.to_ascii_uppercase(),
        ModelName::parse(identifier).ok()?,
    ))
}

impl SessionServiceImpl {
    pub(in crate::application) async fn describe_stream(
        &self,
        domain: &DomainName,
        describe: DescribeRelay,
    ) -> CommandResult {
        let SubscriptionTarget {
            relay: ack_model,
            schema,
            branching,
        } = match self
            .subscription_target_from_schedule(domain, &describe.relay)
            .await
        {
            Ok(Some(target)) => target,
            Ok(None) => {
                return CommandResult {
                    success: false,
                    message: format!(
                        "stream '{}' does not exist in domain '{}'",
                        describe.relay.as_str(),
                        domain.as_str()
                    ),
                    diagnostics: vec![Diagnostic {
                        message: format!("stream '{}' not found", describe.relay.as_str()),
                        span_start: 0,
                        span_end: 0,
                    }],
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
            Err(message) => {
                return CommandResult {
                    success: false,
                    diagnostics: vec![Diagnostic {
                        message: message.clone(),
                        span_start: 0,
                        span_end: 0,
                    }],
                    message,
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };

        let schedule = self.inner.consensus.current_schedule().await;
        let scheduled_relay = if let Some(domain_schedule) = schedule.domain(domain) {
            domain_schedule.nodes.get(&NodeRef::new(
                ModelKind::Relay,
                ModelName::from(&describe.relay),
            ))
        } else {
            None
        };
        if describe.bindings.is_empty() {
            let metrics = match self
                .describe_metrics_for_scheduled_node(
                    domain,
                    ModelKind::Relay,
                    &describe.relay,
                    scheduled_relay,
                )
                .await
            {
                Ok(metrics) => metrics,
                Err(message) => return command_error(message),
            };
            return command_ok(append_metrics_lines(
                format_relay_describe_output(&ack_model, &branching, scheduled_relay),
                metrics,
            ));
        }

        let filter = match validate_subscription_bindings(
            &ack_model.name,
            &branching,
            &schema,
            &describe.bindings,
        ) {
            Ok(filter) => filter,
            Err(message) => {
                return CommandResult {
                    success: false,
                    diagnostics: vec![Diagnostic {
                        message: message.clone(),
                        span_start: 0,
                        span_end: 0,
                    }],
                    message,
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };
        let key = match branch_key_from_filter(&branching, &filter) {
            Ok(key) => key,
            Err(message) => {
                return CommandResult {
                    success: false,
                    diagnostics: vec![Diagnostic {
                        message: message.clone(),
                        span_start: 0,
                        span_end: 0,
                    }],
                    message,
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };

        if let Some(domain_state) = self.inner.consensus.current_domain(domain).await
            && let DomainStatus::Stopped = domain_state.status
        {
            return command_ok("not exists".to_string());
        }

        let owner_nodes = match self
            .scheduled_stream_owner_nodes(domain, &describe.relay)
            .await
        {
            Ok(owner_nodes) => owner_nodes,
            Err(message) => {
                return CommandResult {
                    success: false,
                    diagnostics: vec![Diagnostic {
                        message: message.clone(),
                        span_start: 0,
                        span_end: 0,
                    }],
                    message,
                    kind: i32::from(CommandResultKind::Error),
                    ..Default::default()
                };
            }
        };

        let local_node_id = self.inner.consensus.local_node_id().clone();
        let mut exists = false;
        if owner_nodes.is_empty() || owner_nodes.iter().any(|owner| owner == &local_node_id) {
            match self
                .inner
                .runtime
                .describe_local_stream_exists(domain, &describe.relay, &key)
            {
                Ok(local_exists) => exists |= local_exists,
                Err(error) => {
                    return CommandResult {
                        success: false,
                        diagnostics: vec![Diagnostic {
                            message: error.to_string(),
                            span_start: 0,
                            span_end: 0,
                        }],
                        message: error.to_string(),
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
            }
        }
        for owner in owner_nodes {
            if owner == local_node_id {
                continue;
            }
            let response = self
                .inner
                .interconnect
                .request_with_timeout(
                    &owner,
                    RemoteDescribeRelayRequest {
                        domain: domain.clone(),
                        relay: describe.relay.clone(),
                        bindings: describe.bindings.clone(),
                    },
                    REMOTE_DESCRIBE_RELAY_TIMEOUT,
                )
                .await;
            match response {
                Ok(RemoteDescribeRelayResponse {
                    result: Ok(remote_exists),
                }) => exists |= remote_exists,
                Ok(RemoteDescribeRelayResponse {
                    result: Err(message),
                }) => {
                    return CommandResult {
                        success: false,
                        diagnostics: vec![Diagnostic {
                            message: message.clone(),
                            span_start: 0,
                            span_end: 0,
                        }],
                        message,
                        kind: i32::from(CommandResultKind::Error),
                        ..Default::default()
                    };
                }
                Err(error) => {
                    warn!(
                        %owner,
                        domain = domain.as_str(),
                        relay = describe.relay.as_str(),
                        error = %error,
                        "timed out waiting for remote DESCRIBE RELAY response"
                    );
                    continue;
                }
            }
        }

        let mut lines = vec![if exists {
            "exists".to_string()
        } else {
            "not exists".to_string()
        }];
        if exists {
            lines.push(format!("capacity: {}", ack_model.buffer));
        }
        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Relay,
                &describe.relay,
                scheduled_relay,
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        lines.extend(metrics);

        CommandResult {
            success: true,
            message: lines.join("\n"),
            diagnostics: Vec::new(),
            kind: i32::from(CommandResultKind::Ok),
            ..Default::default()
        }
    }

    pub(in crate::application) async fn handle_describe_stream_request(
        &self,
        request: RemoteDescribeRelayRequest,
    ) -> Result<bool, String> {
        self.prepare_stream_owner_control_request(&request.domain, &request.relay)
            .await?;
        let Some(SubscriptionTarget {
            relay: ack_model,
            schema,
            branching,
        }) = self
            .subscription_target_from_schedule(&request.domain, &request.relay)
            .await?
        else {
            return Err(format!(
                "stream '{}' does not exist in domain '{}'",
                request.relay.as_str(),
                request.domain.as_str()
            ));
        };

        let filter = validate_subscription_bindings(
            &ack_model.name,
            &branching,
            &schema,
            &request.bindings,
        )?;
        let key = branch_key_from_filter(&branching, &filter)?;
        match self
            .inner
            .runtime
            .describe_local_stream_exists(&request.domain, &request.relay, &key)
        {
            Ok(exists) => Ok(exists),
            Err(crate::runtime::RuntimeError::RelayNotInstantiated { .. }) => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    }

    pub(in crate::application) async fn describe_domain(
        &self,
        domain: &DomainName,
        _describe: DescribeDomain,
    ) -> CommandResult {
        let Some(domain_state) = self.inner.consensus.current_domain(domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        let mut lines = vec![
            format!("domain: {}", domain.as_str()),
            format!("status: {:?}", domain_state.status).to_ascii_lowercase(),
        ];
        lines.extend(self.inner.runtime.describe_domain_statistics(domain));
        lines.push("placement:".to_string());
        lines.push(format!(
            "  default policy: {}",
            domain_state.config.placement.as_ref()
        ));
        let placement_plan = self
            .inner
            .registry
            .placement_plan(domain, domain_state.config.placement);
        let rule_count = match placement_plan.as_ref() {
            Some(plan) => plan.rules.len(),
            None => 0,
        };
        lines.push(format!("  rule count: {rule_count}"));
        if let Some(plan) = placement_plan {
            let schedule = self.inner.consensus.current_schedule().await;
            let domain_schedule = schedule.domain(domain);
            for (index, group) in plan.require_groups.iter().enumerate() {
                lines.push(format!("  group {}:", index + 1));
                lines.push(format!(
                    "    members: {}",
                    format_placement_runtime_nodes(&group.members)
                ));
                let host = match placement_group_host(domain_schedule, &group.members) {
                    Some(host) => host.as_str(),
                    None => "(unassigned)",
                };
                lines.push(format!("    host: {host}"));
                for bond in &group.bonds {
                    lines.push(format!(
                        "    bond: {} <-> {} ({})",
                        format_placement_runtime_node(&bond.left, &group.members),
                        format_placement_runtime_node(&bond.right, &group.members),
                        placement_claim_owner(&bond.winning_rules),
                    ));
                }
            }
        }
        command_ok(lines.join("\n"))
    }

    pub(in crate::application) async fn show_placements(
        &self,
        domain: &DomainName,
    ) -> CommandResult {
        let Some(domain_state) = self.inner.consensus.current_domain(domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        let Some(plan) = self
            .inner
            .registry
            .placement_plan(domain, domain_state.config.placement)
        else {
            return command_ok("no placements".to_string());
        };
        if plan.rules.is_empty() {
            return command_ok("no placements".to_string());
        }
        let lines = plan
            .rules
            .iter()
            .map(|rule| {
                let coverage = placement_rule_coverage_status(rule);
                let rank = match rule.rank {
                    Some(rank) => rank.to_string(),
                    None => "unranked".to_string(),
                };
                format!(
                    "{} policy={} rank={} coverage={coverage}",
                    rule.name.as_str(),
                    rule.policy.as_ref(),
                    rank,
                )
            })
            .collect::<Vec<_>>();
        command_ok(lines.join("\n"))
    }

    pub(in crate::application) async fn describe_placement(
        &self,
        domain: &DomainName,
        describe: DescribePlacement,
    ) -> CommandResult {
        let Some(domain_state) = self.inner.consensus.current_domain(domain).await else {
            return command_error(format!("domain '{}' does not exist", domain.as_str()));
        };
        let Some(plan) = self
            .inner
            .registry
            .placement_plan(domain, domain_state.config.placement)
        else {
            return command_error(format!("placement '{}' not found", describe.name.as_str()));
        };
        // The plan was just built for this describe and is walked once for the single named rule,
        // so an index over it would cost the walk it replaces.
        let Some(rule) = plan
            .rules
            .iter()
            .find(|rule| rule.name == ModelName::from(&describe.name))
        else {
            return command_error(format!("placement '{}' not found", describe.name.as_str()));
        };
        let form = match self
            .inner
            .registry
            .get::<CreatePlacement>(domain, &describe.name)
        {
            Ok(Some(placement)) => placement.to_canonical_nspl().ok(),
            Ok(None) => None,
            Err(error) => return command_error(error.to_string()),
        };
        let mut lines = vec![format!("placement: {}", rule.name.as_str())];
        if let Some(form) = form {
            lines.push(format!("form: {form}"));
        }
        lines.push(format!("policy: {}", rule.policy.as_ref()));
        let rank = match rule.rank {
            Some(rank) => rank.to_string(),
            None => "unranked".to_string(),
        };
        lines.push(format!("rank: {rank}"));
        let rule_runtime_nodes = placement_rule_runtime_nodes(rule);
        let from_runtime_nodes = placement_rule_endpoint_nodes(rule, true);
        let to_runtime_nodes = placement_rule_endpoint_nodes(rule, false);
        lines.push(format!(
            "from: {}",
            format_placement_runtime_nodes_in_context(&from_runtime_nodes, &rule_runtime_nodes)
        ));
        lines.push(format!(
            "to: {}",
            format_placement_runtime_nodes_in_context(&to_runtime_nodes, &rule_runtime_nodes)
        ));
        for endpoint in &rule.endpoint_pairs {
            lines.push(format!(
                "pair: {} -> {}",
                format_placement_runtime_node(&endpoint.source, &rule_runtime_nodes),
                format_placement_runtime_node(&endpoint.destination, &rule_runtime_nodes),
            ));
            lines.push(format!("connected: {}", endpoint.connected));
            if endpoint.connected {
                lines.push(format!(
                    "covered: {}",
                    format_placement_runtime_nodes(&ordered_placement_corridor(endpoint))
                ));
            } else {
                lines.push("covered: (none)".to_string());
            }
            for witness in &endpoint.witnesses {
                lines.push(format!(
                    "witness: {}",
                    witness
                        .path
                        .iter()
                        .map(|node| format_placement_runtime_node(node, &rule_runtime_nodes))
                        .collect::<Vec<_>>()
                        .join(" -> ")
                ));
            }
        }
        for claim in &rule.claims {
            lines.push(format!(
                "effective pair: {} <-> {}",
                format_placement_runtime_node(&claim.left, &rule_runtime_nodes),
                format_placement_runtime_node(&claim.right, &rule_runtime_nodes),
            ));
            lines.push(format!(
                "effective policy: {}",
                claim.effective_policy.as_ref()
            ));
            if claim.effective {
                lines.push(format!(
                    "winning claim: {}",
                    placement_claim_owner(&claim.winning_rules)
                ));
            } else {
                lines.push(format!(
                    "overridden by: {}",
                    placement_claim_owner(&claim.winning_rules)
                ));
            }
        }
        let schedule = self.inner.consensus.current_schedule().await;
        let domain_schedule = schedule.domain(domain);
        for group in placement_groups_claimed_by_rule(&plan, rule) {
            lines.push(format!(
                "group members: {}",
                format_placement_runtime_nodes(&group.members)
            ));
            let host = match placement_group_host(domain_schedule, &group.members) {
                Some(host) => host.as_str(),
                None => "(unassigned)",
            };
            lines.push(format!("group host: {host}"));
            for bond in &group.bonds {
                lines.push(format!(
                    "bond: {} <-> {} ({})",
                    format_placement_runtime_node(&bond.left, &group.members),
                    format_placement_runtime_node(&bond.right, &group.members),
                    placement_claim_owner(&bond.winning_rules),
                ));
            }
        }
        command_ok(lines.join("\n"))
    }

    pub(in crate::application) async fn describe_endpoint(
        &self,
        domain: &DomainName,
        describe: DescribeEndpoint,
    ) -> CommandResult {
        match self
            .inner
            .registry
            .get::<CreateEndpoint>(domain, &describe.name)
        {
            Ok(Some(endpoint)) => command_ok(format_endpoint_describe_output(
                &ModelName::from(&describe.name),
                &endpoint,
            )),
            Ok(None) => command_error(format!("endpoint '{}' not found", describe.name.as_str())),
            Err(error) => command_error(error.to_string()),
        }
    }

    pub(in crate::application) async fn describe_ingestor(
        &self,
        domain: &DomainName,
        describe: DescribeIngestor,
    ) -> CommandResult {
        let (ingestor, ingestor_node) = match self
            .ingestor_target_from_schedule(domain, &describe.ingestor)
            .await
        {
            Ok(Some(target)) => target,
            Ok(None) => {
                return command_error(format!(
                    "ingestor '{}' does not exist in domain '{}'",
                    describe.ingestor.as_str(),
                    domain.as_str()
                ));
            }
            Err(message) => return command_error(message),
        };

        let local_node_id = self.inner.consensus.local_node_id();
        let summary = if ingestor_node.executes_on(local_node_id) {
            self.inner
                .runtime
                .describe_local_ingestor(domain, &describe.ingestor)
                .map(|summary| {
                    (
                        summary,
                        self.inner.runtime.describe_metrics_for(
                            domain,
                            "INGESTOR",
                            &describe.ingestor,
                        ),
                    )
                })
        } else if let Some(owner) = ingestor_node.execution_node() {
            match self
                .inner
                .interconnect
                .request(
                    owner,
                    RemoteDescribeIngestorRequest {
                        domain: domain.clone(),
                        name: describe.ingestor.clone(),
                    },
                )
                .await
            {
                Ok(Ok(summary)) => Ok(runtime_ingestor_describe_from_envelope(summary)),
                Ok(Err(message)) => Err(message),
                Err(error) => Err(error.to_string()),
            }
        } else {
            Ok((
                RuntimeIngestorDescribe {
                    running: false,
                    ready: false,
                    quiesce_state: None,
                    quiesce_counters: Default::default(),
                    memory_backpressure_paused: self
                        .inner
                        .runtime
                        .ingestors_paused_for_memory_pressure(),
                    transient_error: None,
                    reconnect_backoff: None,
                    reconnect_wait_millis: None,
                    kafka_domain_offsets: None,
                },
                self.inner
                    .runtime
                    .describe_metrics_for(domain, "INGESTOR", &describe.ingestor),
            ))
        };

        match summary {
            Ok((summary, metrics)) => command_ok(append_metrics_lines(
                format_ingestor_describe_output(
                    &describe.ingestor,
                    &ingestor,
                    &ingestor_node,
                    &summary,
                ),
                metrics,
            )),
            Err(message) => command_error(message),
        }
    }

    pub(in crate::application) async fn dataflow_node_status_for_graph(
        &self,
        domain: &DomainName,
        kind: &str,
        identifier: &ModelName,
    ) -> DataflowNodeHealth {
        dataflow_node_status_from_envelope(
            self.dataflow_node_status_envelope_for_graph(domain, kind, identifier)
                .await,
        )
    }

    async fn dataflow_node_status_envelope_for_graph(
        &self,
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
    ) -> DataflowNodeStatusEnvelope {
        let identifier = identifier.into();
        let Ok(model_kind) = kind.to_ascii_lowercase().parse::<ModelKind>() else {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier.clone());
        };
        if model_kind != ModelKind::Ingestor && model_kind != ModelKind::Emitter {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier.clone());
        }
        let Some(node) = self
            .scheduled_model_node(domain, model_kind, identifier.clone())
            .await
        else {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier.clone());
        };
        let local_node_id = self.inner.consensus.local_node_id();
        if node.executes_on(local_node_id) {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier.clone());
        }
        let Some(owner) = node.execution_node() else {
            return self.local_dataflow_node_status_envelope(domain, kind, identifier.clone());
        };
        let response = self
            .inner
            .interconnect
            .request_with_timeout(
                owner,
                RemoteDataflowNodeStatusRequest {
                    domain: domain.clone(),
                    kind: model_kind,
                    name: identifier.clone(),
                },
                Duration::from_secs(2),
            )
            .await;
        match response {
            Ok(RemoteDataflowNodeStatusResponse { result: Ok(status) }) => status,
            _ => self.local_dataflow_node_status_envelope(domain, kind, identifier),
        }
    }

    fn local_dataflow_node_status_envelope(
        &self,
        domain: &DomainName,
        kind: &str,
        identifier: impl Into<ModelName>,
    ) -> DataflowNodeStatusEnvelope {
        let identifier = identifier.into();
        let health = self
            .inner
            .runtime
            .dataflow_node_status(domain, kind, identifier.clone());
        let transient = self
            .inner
            .runtime
            .dataflow_node_transient_state(domain, kind, identifier);
        dataflow_node_status_to_envelope(
            health.status,
            health.detail,
            transient.error,
            transient.reconnect_backoff,
            health
                .reconnect_wait_millis
                .or(transient.reconnect_wait_millis),
        )
    }

    pub(in crate::application) async fn handle_dataflow_node_status_request(
        &self,
        request: RemoteDataflowNodeStatusRequest,
    ) -> Result<DataflowNodeStatusEnvelope, String> {
        self.prepare_owner_control_request(&request.domain, request.kind, &request.name)
            .await?;
        let health = self.inner.runtime.dataflow_node_status(
            &request.domain,
            request.kind.as_str(),
            &request.name,
        );
        let transient = self.inner.runtime.dataflow_node_transient_state(
            &request.domain,
            request.kind.as_str(),
            &request.name,
        );
        Ok(dataflow_node_status_to_envelope(
            health.status,
            health.detail,
            transient.error,
            transient.reconnect_backoff,
            health
                .reconnect_wait_millis
                .or(transient.reconnect_wait_millis),
        ))
    }

    async fn describe_metrics_for_scheduled_node(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        scheduled_node: Option<&ScheduledNode>,
    ) -> Result<Vec<String>, String> {
        let identifier = identifier.into();
        self.describe_runtime_for_scheduled_node(domain, kind, identifier, scheduled_node)
            .await
            .map(|details| details.metrics)
    }

    async fn describe_runtime_for_scheduled_node(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        scheduled_node: Option<&ScheduledNode>,
    ) -> Result<RemoteDescribeMetricsEnvelope, String> {
        let identifier = identifier.into();
        let metric_kind = kind.as_str().to_ascii_uppercase();
        let Some(node) = scheduled_node else {
            return Ok(self.local_runtime_describe(domain, kind, identifier, &metric_kind));
        };
        let local_node_id = self.inner.consensus.local_node_id();
        if node.executes_on(local_node_id) {
            return Ok(self.local_runtime_describe(domain, kind, identifier, &metric_kind));
        }
        let Some(owner) = node.execution_node() else {
            return Ok(self.local_runtime_describe(domain, kind, identifier, &metric_kind));
        };

        self.inner
            .interconnect
            .request(
                owner,
                RemoteDescribeMetricsRequest {
                    domain: domain.clone(),
                    kind,
                    name: identifier.clone(),
                },
            )
            .await
            .map_err(|error| error.to_string())?
            .result
    }

    fn local_runtime_describe(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
        metric_kind: &str,
    ) -> RemoteDescribeMetricsEnvelope {
        let identifier = identifier.into();
        let state = if let ModelKind::WasmProcessor = kind {
            self.inner
                .runtime
                .describe_wasm_processor_state_for(domain, identifier.clone())
        } else {
            Vec::new()
        };
        RemoteDescribeMetricsEnvelope {
            metrics: self
                .inner
                .runtime
                .describe_metrics_for(domain, metric_kind, identifier),
            state,
        }
    }

    pub(in crate::application) async fn handle_describe_metrics_request(
        &self,
        request: RemoteDescribeMetricsRequest,
    ) -> Result<RemoteDescribeMetricsEnvelope, String> {
        self.prepare_owner_control_request(&request.domain, request.kind, &request.name)
            .await?;
        let metric_kind = request.kind.as_str().to_ascii_uppercase();
        Ok(self.local_runtime_describe(&request.domain, request.kind, &request.name, &metric_kind))
    }

    pub(in crate::application) async fn describe_lookup(
        &self,
        domain: &DomainName,
        describe: DescribeLookup,
    ) -> CommandResult {
        let lookup_target = match self
            .lookup_target_from_schedule(domain, &describe.name)
            .await
        {
            Ok(target) => target,
            Err(message) => return command_error(message),
        };
        let Some(LookupTarget {
            lookup,
            node: lookup_node,
            ..
        }) = lookup_target
        else {
            return command_error(format!(
                "hash map '{}' does not exist in domain '{}'",
                describe.name.as_str(),
                domain.as_str()
            ));
        };

        let local_node_id = self.inner.consensus.local_node_id();
        let summary = if lookup_node.executes_on(local_node_id) {
            match self
                .inner
                .runtime
                .describe_local_lookup(domain, &describe.name)
            {
                Ok(description) => Ok(LookupDescribeEnvelope {
                    resource: lookup.resource.clone(),
                    resource_version: description.resource_version,
                    path: lookup.path.clone(),
                    decode_using_codec: lookup.decode_using_codec.clone(),
                    key_field: lookup.key_field.clone(),
                    entry_count: description.entry_count.arch_into(),
                }),
                Err(message) => Err(message),
            }
        } else if let Some(owner) = lookup_node.execution_node() {
            match self
                .inner
                .interconnect
                .request(
                    owner,
                    RemoteDescribeLookupRequest {
                        domain: domain.clone(),
                        name: describe.name.clone(),
                    },
                )
                .await
            {
                Ok(response) => response.result,
                Err(error) => Err(error.to_string()),
            }
        } else {
            Err(format!(
                "hash map '{}' in domain '{}' has no execution node",
                describe.name.as_str(),
                domain.as_str()
            ))
        };

        match summary {
            Ok(summary) => {
                let metrics = match self
                    .describe_metrics_for_scheduled_node(
                        domain,
                        ModelKind::Lookup,
                        &describe.name,
                        Some(&lookup_node),
                    )
                    .await
                {
                    Ok(metrics) => metrics,
                    Err(message) => return command_error(message),
                };
                command_ok(append_metrics_lines(
                    format_lookup_describe_output(&describe.name, &lookup_node, &summary),
                    metrics,
                ))
            }
            Err(message) => command_error(message),
        }
    }

    pub(in crate::application) async fn handle_describe_lookup_request(
        &self,
        request: RemoteDescribeLookupRequest,
    ) -> Result<LookupDescribeEnvelope, String> {
        self.prepare_owner_control_request(&request.domain, ModelKind::Lookup, &request.name)
            .await?;
        let description = self
            .inner
            .runtime
            .describe_local_lookup(&request.domain, &request.name)?;
        Ok(LookupDescribeEnvelope {
            resource: description.model.resource,
            resource_version: description.resource_version,
            path: description.model.path,
            decode_using_codec: description.model.decode_using_codec,
            key_field: description.model.key_field,
            entry_count: description.entry_count.arch_into(),
        })
    }

    pub(in crate::application) async fn describe_deduplicator(
        &self,
        domain: &DomainName,
        describe: DescribeDeduplicator,
    ) -> CommandResult {
        let DescribedModel {
            config: deduplicator,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateDeduplicator>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "deduplicator '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read deduplicator '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Deduplicator,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_deduplicator_describe_output(
                &describe.name,
                &deduplicator,
                scheduled_node.as_ref(),
            ),
            metrics,
        ))
    }

    pub(in crate::application) async fn describe_junction(
        &self,
        domain: &DomainName,
        describe: DescribeJunction,
    ) -> CommandResult {
        let DescribedModel {
            config: junction,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateJunction>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "junction '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read junction '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Junction,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_junction_describe_output(&describe.name, &junction, scheduled_node.as_ref()),
            metrics,
        ))
    }

    pub(in crate::application) async fn describe_reingestor(
        &self,
        domain: &DomainName,
        describe: DescribeReingestor,
    ) -> CommandResult {
        let DescribedModel {
            config: reingestor,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateReingestor>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "reingestor '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read reingestor '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Reingestor,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_reingestor_describe_output(&describe.name, &reingestor, scheduled_node.as_ref()),
            metrics,
        ))
    }

    pub(in crate::application) async fn describe_correlator(
        &self,
        domain: &DomainName,
        describe: DescribeCorrelator,
    ) -> CommandResult {
        let DescribedModel {
            config: correlator,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateCorrelator>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "correlator '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read correlator '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Correlator,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_correlator_describe_output(&describe.name, &correlator, scheduled_node.as_ref()),
            metrics,
        ))
    }

    pub(in crate::application) async fn describe_reorderer(
        &self,
        domain: &DomainName,
        describe: DescribeReorderer,
    ) -> CommandResult {
        let DescribedModel {
            config: reorderer,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateReorderer>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "reorderer '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read reorderer '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Reorderer,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_reorderer_describe_output(&describe.name, &reorderer, scheduled_node.as_ref()),
            metrics,
        ))
    }

    pub(in crate::application) async fn describe_emitter(
        &self,
        domain: &DomainName,
        describe: DescribeEmitter,
    ) -> CommandResult {
        let DescribedModel {
            config: emitter,
            scheduled: scheduled_node,
        } = match self
            .described_model::<CreateEmitter>(domain, &describe.name)
            .await
        {
            Ok(Some(described)) => described,
            Ok(None) => {
                return command_error(format!(
                    "emitter '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read emitter '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };
        let status = self
            .dataflow_node_status_envelope_for_graph(
                domain,
                ModelKind::Emitter.as_str(),
                &describe.name,
            )
            .await;

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::Emitter,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_emitter_describe_output(
                &describe.name,
                &emitter,
                scheduled_node.as_ref(),
                Some(&status),
            ),
            metrics,
        ))
    }

    pub(in crate::application) async fn describe_window_processor(
        &self,
        domain: &DomainName,
        describe: DescribeWindowProcessor,
    ) -> CommandResult {
        let processor = match self
            .inner
            .registry
            .get::<CreateWindowProcessor>(domain, &describe.name)
        {
            Ok(Some(processor)) => processor,
            Ok(None) => {
                return command_error(format!(
                    "window processor '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read window processor '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };
        let aggregate = match processor
            .output_routes
            .routes
            .iter()
            .map(|output| {
                lower_window_assignments(&output.construction).map(|program| program.inner)
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(programs) if !programs.is_empty() => {
                WindowAggregateProgram::combine_route_programs(&programs)
            }
            Ok(_) => {
                return command_error("window processor has no output routes".to_string());
            }
            Err(error) => {
                return command_error(format!(
                    "failed to lower aggregate outputs for window processor '{}': {error}",
                    describe.name.as_str()
                ));
            }
        };

        let scheduled_node = self
            .scheduled_model_node(domain, ModelKind::WindowProcessor, &describe.name)
            .await;

        let metrics = match self
            .describe_metrics_for_scheduled_node(
                domain,
                ModelKind::WindowProcessor,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_window_processor_describe_output(
                &describe.name,
                &processor,
                &aggregate,
                scheduled_node.as_ref(),
            ),
            metrics,
        ))
    }

    pub(in crate::application) async fn describe_wasm_processor(
        &self,
        domain: &DomainName,
        describe: DescribeWasmProcessor,
    ) -> CommandResult {
        let processor = match self
            .inner
            .registry
            .get::<CreateWasmProcessor>(domain, &describe.name)
        {
            Ok(Some(processor)) => processor,
            Ok(None) => {
                return command_error(format!(
                    "wasm processor '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!(
                    "failed to read wasm processor '{}' in domain '{}': {error:?}",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
        };
        let scheduled_node = self
            .scheduled_model_node(domain, ModelKind::WasmProcessor, &describe.name)
            .await;

        let runtime_details = match self
            .describe_runtime_for_scheduled_node(
                domain,
                ModelKind::WasmProcessor,
                &describe.name,
                scheduled_node.as_ref(),
            )
            .await
        {
            Ok(metrics) => metrics,
            Err(message) => return command_error(message),
        };
        command_ok(append_metrics_lines(
            format_wasm_processor_describe_output(
                &describe.name,
                &processor,
                scheduled_node.as_ref(),
                runtime_details.state,
            ),
            runtime_details.metrics,
        ))
    }

    /// The `M` named `identifier` in `domain`, with the schedule entry that places it.
    ///
    /// `DESCRIBE` answers on any node, and a node that has not stored the domain's models still
    /// holds the schedule it was given, so the schedule is the second source for the same
    /// configuration. Both sources are keyed by `M`'s kind and hand back an `M`, so a description
    /// either has the model or does not.
    async fn described_model<M: UniquelyKindedModel>(
        &self,
        domain: &DomainName,
        identifier: impl Into<ModelName>,
    ) -> Result<Option<DescribedModel<M>>, Report<RegistryError>> {
        let identifier = identifier.into();
        let scheduled = self
            .scheduled_model_node(domain, M::KIND, identifier.clone())
            .await;
        if let Some(config) = self.inner.registry.get::<M>(domain, identifier)? {
            return Ok(Some(DescribedModel { config, scheduled }));
        }
        let Some(scheduled) = scheduled else {
            return Ok(None);
        };
        let config = M::from_model((*scheduled.config).clone()).assured(
            "a schedule keys every entry by the kind of the configuration it carries, and this \
             entry was resolved under this model's kind",
        );
        Ok(Some(DescribedModel {
            config,
            scheduled: Some(scheduled),
        }))
    }

    pub(in crate::application) async fn scheduled_model_node(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) -> Option<ScheduledNode> {
        let identifier = identifier.into();
        let schedule = self.inner.consensus.current_schedule().await;
        let domain_schedule = schedule.domain(domain)?;
        domain_schedule
            .nodes
            .get(&NodeRef::new(kind, identifier))
            .cloned()
    }

    pub(in crate::application) async fn prepare_owner_control_request(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) -> Result<ScheduledNode, String> {
        let identifier = identifier.into();
        self.prepare_control_request_domain(domain).await?;
        let node = self
            .scheduled_model_node(domain, kind, identifier.clone())
            .await
            .ok_or_else(|| {
                format!(
                    "{} '{}' does not exist in domain '{}'",
                    kind.as_str().to_ascii_lowercase(),
                    identifier.as_str(),
                    domain.as_str()
                )
            })?;
        let local_node_id = self.inner.consensus.local_node_id();
        if !node.executes_on(local_node_id) {
            let owner = match node.execution_node() {
                Some(owner) => owner.as_str(),
                None => "-",
            };
            return Err(format!(
                "{} '{}' in domain '{}' is owned by '{owner}' but request reached '{}'",
                kind.as_str().to_ascii_lowercase(),
                identifier.as_str(),
                domain.as_str(),
                local_node_id
            ));
        }
        Ok(node)
    }

    async fn prepare_assigned_control_request(
        &self,
        domain: &DomainName,
        kind: ModelKind,
        identifier: impl Into<ModelName>,
    ) -> Result<ScheduledNode, String> {
        let identifier = identifier.into();
        self.prepare_control_request_domain(domain).await?;
        let node = self
            .scheduled_model_node(domain, kind, identifier.clone())
            .await
            .ok_or_else(|| {
                format!(
                    "{} '{}' does not exist in domain '{}'",
                    kind.as_str().to_ascii_lowercase(),
                    identifier.as_str(),
                    domain.as_str()
                )
            })?;
        let local_node_id = self.inner.consensus.local_node_id();
        if !node.is_assigned_to(local_node_id) {
            return Err(format!(
                "{} '{}' in domain '{}' is not assigned to '{}'",
                kind.as_str().to_ascii_lowercase(),
                identifier.as_str(),
                domain.as_str(),
                local_node_id
            ));
        }
        Ok(node)
    }

    async fn prepare_stream_owner_control_request(
        &self,
        domain: &DomainName,
        relay: &RelayName,
    ) -> Result<(), String> {
        self.prepare_control_request_domain(domain).await?;
        let owner_nodes = self.scheduled_stream_owner_nodes(domain, relay).await?;
        let local_node_id = self.inner.consensus.local_node_id();
        if !owner_nodes.iter().any(|owner| owner == local_node_id) {
            return Err(format!(
                "stream '{}' in domain '{}' is not owned by '{}'",
                relay.as_str(),
                domain.as_str(),
                local_node_id
            ));
        }
        Ok(())
    }

    pub(in crate::application) async fn prepare_control_request_domain(
        &self,
        domain: &DomainName,
    ) -> Result<(), String> {
        if self.inner.consensus.current_domain(domain).await.is_none() {
            return Err(format!("domain '{}' does not exist", domain.as_str()));
        }
        self.reconcile_running_domain_runtime(domain).await
    }

    pub(in crate::application) async fn lookup_query(
        &self,
        domain: &DomainName,
        query: LookupQuery,
    ) -> CommandResult {
        let lookup_target = match self.lookup_target_from_schedule(domain, &query.name).await {
            Ok(target) => target,
            Err(message) => return command_error(message),
        };
        let Some(LookupTarget {
            lookup,
            node: lookup_node,
            key_ty,
        }) = lookup_target
        else {
            return command_error(format!(
                "hash map '{}' does not exist in domain '{}'",
                query.name.as_str(),
                domain.as_str()
            ));
        };

        let parsed = match parse_subscription_literal(&lookup.key_field, &key_ty, &query.key) {
            Ok(value) => value,
            Err(message) => return command_error(message),
        };
        let key = parsed.to_key_fragment();
        let local_node_id = self.inner.consensus.local_node_id();
        let local_record = if lookup_node.is_assigned_to(local_node_id) {
            Some(
                self.inner
                    .runtime
                    .query_local_lookup(domain, &query.name, &key),
            )
        } else {
            None
        };
        let record = match local_record {
            Some(Ok(record)) => Ok(record),
            Some(Err(local_error)) => {
                let mut targets = Vec::new();
                if let Some(owner) = lookup_node.execution_node()
                    && owner != local_node_id
                {
                    targets.push(owner.clone());
                }
                for assigned in &lookup_node.assigned_nodes {
                    if assigned != local_node_id && !targets.contains(assigned) {
                        targets.push(assigned.clone());
                    }
                }
                if targets.is_empty() {
                    Err(local_error)
                } else {
                    self.lookup_query_remote_candidates(domain, &query.name, &key, targets)
                        .await
                }
            }
            None => {
                let mut targets = Vec::new();
                if let Some(owner) = lookup_node.execution_node() {
                    targets.push(owner.clone());
                }
                for assigned in &lookup_node.assigned_nodes {
                    if !targets.contains(assigned) {
                        targets.push(assigned.clone());
                    }
                }
                if targets.is_empty() {
                    Err(format!(
                        "hash map '{}' in domain '{}' has no execution node",
                        query.name.as_str(),
                        domain.as_str()
                    ))
                } else {
                    self.lookup_query_remote_candidates(domain, &query.name, &key, targets)
                        .await
                }
            }
        };

        match record {
            Ok(Some(record)) => match record.row_to_json_string(0) {
                Ok(json) => command_ok(json),
                Err(message) => command_error(message),
            },
            Ok(None) => command_error(format!(
                "hash map '{}' has no entry for key {}",
                query.name.as_str(),
                render_subscription_literal(&query.key)
            )),
            Err(message) => command_error(message),
        }
    }

    async fn lookup_query_remote_candidates(
        &self,
        domain: &DomainName,
        name: impl Into<ModelName>,
        key: &str,
        targets: Vec<ClusterNodeName>,
    ) -> Result<Option<runtime_schema::RuntimeRecordBatch>, String> {
        let name = name.into();
        let mut errors = Vec::new();
        for target in targets {
            let response = self
                .inner
                .interconnect
                .request(
                    &target,
                    RemoteLookupRequest {
                        domain: domain.clone(),
                        name: LookupName::from(&name),
                        key: key.to_string(),
                    },
                )
                .await;
            match response {
                Ok(RemoteLookupResponse { result }) => match result {
                    Ok(None) => return Ok(None),
                    Ok(Some(bytes)) => return self.decode_lookup_record(bytes).await.map(Some),
                    Err(message) => errors.push(message),
                },
                Err(error) => errors.push(error.to_string()),
            }
        }
        Err(errors
            .into_iter()
            .next()
            .unwrap_or_else(|| "lookup has no remote execution node".to_string()))
    }

    pub(in crate::application) async fn handle_lookup_request(
        &self,
        request: RemoteLookupRequest,
    ) -> Result<Option<runtime_schema::RuntimeRecordBatch>, String> {
        self.prepare_assigned_control_request(&request.domain, ModelKind::Lookup, &request.name)
            .await?;
        self.inner
            .runtime
            .query_local_lookup(&request.domain, &request.name, &request.key)
    }

    /// Decode one remote lookup answer through the node's admission, so a large answer is charged
    /// and runs off the async workers like every other body.
    async fn decode_lookup_record(
        &self,
        bytes: Vec<u8>,
    ) -> Result<runtime_schema::RuntimeRecordBatch, String> {
        let executor = self.inner.runtime.executor();
        let body = executor
            .charge_owned(MemoryClass::Commands, bytes)
            .await
            .map_err(|error| error.to_string())?;
        runtime_schema::RuntimeRecordBatch::decode_arrow_ipc(executor, body)
            .await
            .map_err(|error| error.to_string())
    }

    pub(in crate::application) async fn show_stream_materialized_state(
        &self,
        domain: &DomainName,
        show: ShowRelayMaterializedState,
    ) -> CommandResult {
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return command_error(format!(
                "domain '{}' has no active schedule",
                domain.as_str()
            ));
        };
        let Some(relay_node) = domain_schedule
            .nodes
            .get(&NodeRef::new(
                ModelKind::Relay,
                ModelName::from(&show.relay),
            ))
            .filter(|node| {
                matches!(node.config.as_ref(), Model::Relay(relay) if relay.materialized_state.is_some())
            })
        else {
            return command_error(format!(
                "stream '{}' in domain '{}' is not materialized",
                show.relay.as_str(),
                domain.as_str()
            ));
        };

        let entries = match self
            .inner
            .runtime
            .local_materialized_stream_state(domain, &show.relay)
            .await
        {
            Ok(entries) if !entries.is_empty() => entries,
            Ok(_) if !relay_node.executes_on(self.inner.consensus.local_node_id()) => {
                if let Some(primary_node) = relay_node.primary_node() {
                    match self
                        .inner
                        .runtime
                        .remote_materialized_stream_state(primary_node, domain, &show.relay)
                        .await
                    {
                        Ok(entries) => entries,
                        Err(message) => return command_error(message),
                    }
                } else {
                    Vec::new()
                }
            }
            Ok(entries) => entries,
            Err(message) => return command_error(message),
        };

        let message = if entries.is_empty() {
            format_materialized_stream_state_output(&show.relay, relay_node, Vec::new())
        } else {
            let entry_lines = entries
                .into_iter()
                .map(|record| {
                    format!(
                        "key={} payload={} low={} high={}",
                        if record.branch.is_empty() {
                            "(root)"
                        } else {
                            record.branch.as_str()
                        },
                        record.payload,
                        record.ingested_at_low_watermark,
                        record.ingested_at_high_watermark
                    )
                })
                .collect::<Vec<_>>();
            format_materialized_stream_state_output(&show.relay, relay_node, entry_lines)
        };
        command_ok(message)
    }

    pub(in crate::application) fn describe_udf(
        &self,
        domain: &DomainName,
        describe: DescribeUdf,
    ) -> CommandResult {
        let model = match self.inner.registry.get::<CreateUdf>(domain, &describe.name) {
            Ok(Some(udf)) => udf,
            Ok(None) => {
                return command_error(format!(
                    "UDF '{}' does not exist in domain '{}'",
                    describe.name.as_str(),
                    domain.as_str()
                ));
            }
            Err(error) => {
                return command_error(format!("failed to read UDF: {error}"));
            }
        };
        let arguments = model
            .arguments
            .iter()
            .map(|argument| {
                format!(
                    "{} {}{}",
                    argument.name.as_str(),
                    argument.ty,
                    if argument.optional { " OPTIONAL" } else { "" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let references = if let Some(graph) = self.inner.registry.active_graph(domain) {
            let mut references = Vec::new();
            for edge in graph.edges() {
                if edge.kind == crate::registry::EdgeKind::RequiredBy
                    && edge.from == ModelName::from(&model.name)
                {
                    references.push(edge.to);
                }
            }
            references.sort();
            references.dedup();
            if references.is_empty() {
                "(none)".to_string()
            } else {
                references
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        } else {
            "(none)".to_string()
        };
        command_ok(format!(
            "name: {}\nlanguage: {}\nsignature: ({arguments}) -> {}{}\nvolatile: {}\ncode_hash: \
             {}\nreferencing_nodes: {references}",
            model.name.as_str(),
            model.language.as_ref(),
            model.returns.ty,
            if model.returns.optional {
                " OPTIONAL"
            } else {
                ""
            },
            model.volatile,
            model.code_hash
        ))
    }

    pub(in crate::application) fn show_udfs(&self, domain: &DomainName) -> CommandResult {
        match self
            .inner
            .registry
            .list_identifiers(domain, ModelKind::Udf, "")
        {
            Ok(identifiers) if identifiers.is_empty() => command_ok("(none)".to_string()),
            Ok(identifiers) => command_ok(
                identifiers
                    .iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
            Err(error) => command_error(format!("failed to list UDFs: {error}")),
        }
    }

    pub(in crate::application) async fn describe_resource(
        &self,
        domain: &DomainName,
        describe: DescribeResource,
    ) -> CommandResult {
        if describe.version.is_none() {
            let resources = self.inner.consensus.current_resources().await;
            if !resources.is_declared(domain, &describe.identifier) {
                return command_error(format!(
                    "resource '{}' does not exist",
                    describe.identifier.as_str()
                ));
            }
            let versions = resources
                .versions
                .iter()
                .filter(|resource| {
                    resource.id.domain == *domain && resource.id.identifier == describe.identifier
                })
                .cloned()
                .collect::<Vec<_>>();
            let version_numbers = if versions.is_empty() {
                "(none)".to_string()
            } else {
                versions
                    .iter()
                    .map(|resource| resource.id.version.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            };
            let mut lines = vec![
                format!("resource: {}", describe.identifier.as_str()),
                format!("versions: {version_numbers}"),
            ];
            lines.push("version_details:".to_string());
            if versions.is_empty() {
                lines.push("- none".to_string());
            } else {
                for resource in &versions {
                    tokio::task::consume_budget().await;
                    lines.push(SessionServiceImpl::format_resource_version_summary(
                        resource,
                    ));
                    lines.push("  entries:".to_string());
                    lines.extend(self.resource_version_entry_lines(resource).await);
                }
            }
            return command_ok(lines.join("\n"));
        }

        let version = describe
            .version
            .verified("the branch above returned for the absent case");
        let id = ResourceId::new(domain.clone(), describe.identifier.clone(), version);
        let resources = self.inner.consensus.current_resources().await;
        let Some(resource) = resources.version(&id).cloned() else {
            return command_error(format!(
                "resource '{}@{}' does not exist",
                describe.identifier.as_str(),
                version
            ));
        };

        let replicas = resources
            .replicas
            .iter()
            .filter(|replica| replica.key.version_key().resource_id() == id)
            .cloned()
            .collect::<Vec<_>>();
        let gossip = self.inner.cluster.availability_state().await;
        let live_node_ids = gossip
            .live_identities()
            .into_iter()
            .map(|identity| identity.node_id().clone())
            .collect::<BTreeSet<_>>();
        let mut live_node_ids = live_node_ids;
        if live_node_ids.is_empty() {
            live_node_ids.insert(self.inner.consensus.local_node_id().clone());
        }
        let dead_node_ids = gossip.dead_node_ids;
        let node_ids = live_node_ids
            .iter()
            .cloned()
            .chain(dead_node_ids.iter().cloned())
            .chain(replicas.iter().map(|replica| replica.key.node_id.clone()))
            .collect::<BTreeSet<_>>();
        let ready_live_nodes = live_node_ids
            .iter()
            .filter(|node_id| {
                replicas.iter().any(|replica| {
                    &replica.key.node_id == *node_id
                        && replica.state.as_ref() == "ready"
                        && replica.root_checksum.as_deref() == Some(resource.root_checksum.as_str())
                })
            })
            .count();
        let cluster_ready = !live_node_ids.is_empty() && ready_live_nodes == live_node_ids.len();

        let mut lines = vec![
            format!(
                "resource: {}@{}",
                resource.id.identifier.as_str(),
                resource.id.version
            ),
            format!("root_checksum: {}", resource.root_checksum),
            format!("manifest_checksum: {}", resource.manifest_checksum),
            format!("file_count: {}", resource.file_count),
            format!("total_bytes: {}", resource.total_bytes),
            format!("created_by_node: {}", resource.created_by_node),
            format!("created_at: {}", resource.created_at),
            format!(
                "cluster_ready: {}",
                if cluster_ready { "true" } else { "false" }
            ),
            "entries:".to_string(),
        ];
        lines.extend(self.resource_version_entry_lines(&resource).await);
        lines.extend([
            format!(
                "alive_nodes: {}",
                if live_node_ids.is_empty() {
                    "(none)".to_string()
                } else {
                    live_node_ids.iter().cloned().collect::<Vec<_>>().join(",")
                }
            ),
            format!(
                "dead_nodes: {}",
                if dead_node_ids.is_empty() {
                    "(none)".to_string()
                } else {
                    dead_node_ids.iter().cloned().collect::<Vec<_>>().join(",")
                }
            ),
            "nodes:".to_string(),
        ]);

        if node_ids.is_empty() {
            lines.push("- none".to_string());
        } else {
            for node_id in node_ids {
                let topology = if live_node_ids.contains(&node_id) {
                    "alive"
                } else if dead_node_ids.contains(&node_id) {
                    "dead"
                } else {
                    "unknown"
                };
                let replica = replicas
                    .iter()
                    .find(|replica| replica.key.node_id == node_id);
                let state = match replica {
                    Some(replica) => replica.state.as_ref(),
                    None if live_node_ids.contains(&node_id) => "pending",
                    None => "untracked",
                };
                let checksum = if let Some(replica) = replica {
                    replica.root_checksum.as_deref().unwrap_or("-")
                } else {
                    "-"
                };
                let verified_at = if let Some(replica) = replica
                    && let Some(value) = replica.last_verified_at
                {
                    value.to_string()
                } else {
                    "-".to_string()
                };
                let source = match replica.and_then(|replica| replica.source_node_id.as_ref()) {
                    Some(source) => source.as_str(),
                    None => "-",
                };
                let error = if let Some(replica) = replica {
                    replica.error.as_deref().unwrap_or("-")
                } else {
                    "-"
                };
                lines.push(format!(
                    "- {} topology={} state={} checksum={} verified_at={} source={} error={}",
                    node_id, topology, state, checksum, verified_at, source, error,
                ));
            }
        }

        command_ok(lines.join("\n"))
    }

    fn format_resource_version_summary(resource: &nervix_models::ResourceVersion) -> String {
        format!(
            "- version={} root_checksum={} manifest_checksum={} file_count={} total_bytes={} \
             created_by_node={} created_at={}",
            resource.id.version,
            resource.root_checksum,
            resource.manifest_checksum,
            resource.file_count,
            resource.total_bytes,
            resource.created_by_node,
            resource.created_at
        )
    }

    async fn resource_version_entry_lines(
        &self,
        resource: &nervix_models::ResourceVersion,
    ) -> Vec<String> {
        match self.inner.resource_store.read_manifest(&resource.id).await {
            Ok(manifest) if manifest.entries.is_empty() => vec!["  - none".to_string()],
            Ok(manifest) => manifest
                .entries
                .iter()
                .map(SessionServiceImpl::format_resource_manifest_entry)
                .collect(),
            Err(error) => vec![format!("  - unavailable error={error}")],
        }
    }

    fn format_resource_manifest_entry(entry: &ResourceManifestEntry) -> String {
        let (entry_type, size, checksum) = match &entry.content {
            ResourceEntryContent::File { size, checksum } => ("file", *size, checksum.as_str()),
            ResourceEntryContent::Directory => ("directory", 0, "-"),
        };
        format!(
            "  - type={} path={} size={} checksum={}",
            entry_type, entry.path, size, checksum
        )
    }
}

impl SessionServiceImpl {
    async fn lookup_target_from_schedule(
        &self,
        domain: &DomainName,
        name: impl Into<ModelName>,
    ) -> Result<Option<LookupTarget>, String> {
        let name = name.into();
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(lookup_node) = domain_schedule
            .nodes
            .get(&NodeRef::new(ModelKind::Lookup, name.clone()))
        else {
            return Ok(None);
        };
        let Model::Lookup(lookup) = lookup_node.config.as_ref() else {
            return Err("scheduled lookup node has invalid model kind".to_string());
        };
        let Some(codec_node) = domain_schedule.nodes.get(&NodeRef::new(
            ModelKind::Codec,
            ModelName::from(&lookup.decode_using_codec),
        )) else {
            return Err(format!(
                "lookup '{}' references missing scheduled codec '{}'",
                name.as_str(),
                lookup.decode_using_codec.as_str()
            ));
        };
        let Model::Codec(codec) = codec_node.config.as_ref() else {
            return Err("scheduled codec node has invalid model kind".to_string());
        };
        let Some(schema_node) = domain_schedule.nodes.get(&NodeRef::new(
            ModelKind::Schema,
            ModelName::from(&codec.schema),
        )) else {
            return Err(format!(
                "lookup '{}' references missing scheduled schema '{}'",
                name.as_str(),
                codec.schema.as_str()
            ));
        };
        let Model::Schema(schema) = schema_node.config.as_ref() else {
            return Err("scheduled schema node has invalid model kind".to_string());
        };
        let Some(field) = schema
            .fields
            .iter()
            .find(|field| field.name == lookup.key_field)
        else {
            return Err(format!(
                "lookup '{}' key field '{}' is missing from schema '{}'",
                name.as_str(),
                lookup.key_field.as_str(),
                schema.name.as_str()
            ));
        };
        Ok(Some(LookupTarget {
            lookup: lookup.clone(),
            node: lookup_node.clone(),
            key_ty: field.ty.clone(),
        }))
    }

    async fn ingestor_target_from_schedule(
        &self,
        domain: &DomainName,
        name: impl Into<ModelName>,
    ) -> Result<Option<(CreateIngestor, ScheduledNode)>, String> {
        let name = name.into();
        let schedule = self.inner.consensus.current_schedule().await;
        let Some(domain_schedule) = schedule.domain(domain) else {
            return Ok(None);
        };
        let Some(ingestor_node) = domain_schedule
            .nodes
            .get(&NodeRef::new(ModelKind::Ingestor, name))
        else {
            return Ok(None);
        };
        let Model::Ingestor(ingestor) = ingestor_node.config.as_ref() else {
            return Err("scheduled ingestor node has invalid model kind".to_string());
        };
        Ok(Some((ingestor.clone(), ingestor_node.clone())))
    }
}

#[cfg(test)]
mod tests {
    use nervix_models::{
        DomainConfig, DomainName, DomainPace, DomainState, DomainStatus, ResourceUploadIdentity,
        ResourceUploadKey, Timestamp, UserName,
    };
    use tokio::sync::mpsc;

    use super::super::{
        subscription::SessionSubscriptions,
        test_fixtures::{TestService, build_test_service, named},
    };
    use crate::proto::CommandRequest;

    #[tokio::test]
    async fn show_placements_reports_fully_overridden_effective_coverage() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();
        let configured = service
            .process_command(
                CommandRequest {
                    query: "BEGIN; CREATE SCHEMA placement_event ( id I64 ); CREATE RELAY \
                            placement_input SCHEMA placement_event UNBRANCHED; CREATE RELAY \
                            placement_middle SCHEMA placement_event UNBRANCHED; CREATE RELAY \
                            placement_output SCHEMA placement_event UNBRANCHED; CREATE JUNCTION \
                            corridor_source FROM placement_input UNBRANCHED TO placement_middle \
                            INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG; CREATE JUNCTION \
                            corridor_sink FROM placement_middle UNBRANCHED TO placement_output \
                            INHERIT ALL FLUSH IMMEDIATE ON MESSAGE ERROR LOG; CREATE PLACEMENT \
                            weak_glue FROM corridor_source TO corridor_sink REQUIRE COLOCATION \
                            RANK 2; CREATE PLACEMENT strong_cut FROM corridor_source TO \
                            corridor_sink NEUTRAL RANK 1; COMMIT;"
                        .to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;
        assert!(
            configured.success,
            "placement coverage fixture should configure: {configured:?}"
        );

        let output = service
            .show_placements(&DomainName::parse("default").expect("valid domain"))
            .await;
        assert!(output.success, "SHOW PLACEMENTS should succeed: {output:?}");
        assert!(
            output
                .message
                .lines()
                .any(|line| line.starts_with("weak_glue ") && line.ends_with("coverage=overridden")),
            "unexpected SHOW PLACEMENTS output: {}",
            output.message
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }

    #[tokio::test]
    async fn process_command_describes_resource_metadata() {
        let TestService {
            service,
            registry: _registry,
            path,
        } = build_test_service(true).await;
        let expected_leader = service.inner.consensus.local_node_id().clone();
        let resource_store = service.inner.resource_store.clone();
        let proposer = service.inner.consensus.clone();
        let source_v1 = path.join("resource-source-v1");
        std::fs::create_dir_all(source_v1.join("nested"))
            .expect("test resource directory should exist");
        std::fs::write(source_v1.join("alpha.txt"), "alpha")
            .expect("test resource file should write");
        std::fs::write(source_v1.join("nested").join("beta.txt"), "beta")
            .expect("test resource file should write");
        let resource_domain = DomainName::parse("default").expect("valid domain");
        let manifest_v1 = resource_store
            .install_from_directory(
                nervix_models::ResourceId::new(resource_domain.clone(), named("fraud_model"), 1),
                &source_v1,
                expected_leader.clone(),
                Timestamp::from_unix_nanos(77),
            )
            .await
            .expect("resource version should install");
        let source_v2 = path.join("resource-source-v2");
        std::fs::create_dir_all(&source_v2).expect("test resource directory should exist");
        std::fs::write(source_v2.join("model.onnx"), "model")
            .expect("test resource file should write");
        let manifest_v2 = resource_store
            .install_from_directory(
                nervix_models::ResourceId::new(resource_domain.clone(), named("fraud_model"), 2),
                &source_v2,
                expected_leader.clone(),
                Timestamp::from_unix_nanos(79),
            )
            .await
            .expect("resource version should install");
        proposer
            .create_resource_catalog(&resource_domain, &named("fraud_model"))
            .await
            .expect("resource catalog should persist");
        let upload_v1 = ResourceUploadKey::new(
            UserName::parse("admin").expect("valid user name"),
            resource_domain.clone(),
            named("fraud_model"),
            ResourceUploadIdentity::parse("describe-v1").expect("valid upload identity"),
        );
        proposer
            .begin_resource_upload(upload_v1.clone())
            .await
            .expect("resource upload should begin");
        let replica_v1 = nervix_models::ResourceNodeStatus {
            key: nervix_models::ResourceReplicaKey::new(
                resource_domain.clone(),
                named("fraud_model"),
                1,
                expected_leader.clone(),
            ),
            state: nervix_models::ResourceNodeState::Ready,
            root_checksum: Some(manifest_v1.resource.root_checksum.clone()),
            last_verified_at: Some(Timestamp::from_unix_nanos(78)),
            source_node_id: Some(expected_leader.clone()),
            error: None,
        };
        proposer
            .publish_resource_upload(upload_v1, manifest_v1.resource.clone(), replica_v1)
            .await
            .expect("resource version should persist");
        let upload_v2 = ResourceUploadKey::new(
            UserName::parse("admin").expect("valid user name"),
            resource_domain.clone(),
            named("fraud_model"),
            ResourceUploadIdentity::parse("describe-v2").expect("valid upload identity"),
        );
        proposer
            .begin_resource_upload(upload_v2.clone())
            .await
            .expect("resource upload should begin");
        proposer
            .publish_resource_upload(
                upload_v2,
                manifest_v2.resource.clone(),
                nervix_models::ResourceNodeStatus {
                    key: nervix_models::ResourceReplicaKey::new(
                        resource_domain.clone(),
                        named("fraud_model"),
                        2,
                        expected_leader.clone(),
                    ),
                    state: nervix_models::ResourceNodeState::Ready,
                    root_checksum: Some(manifest_v2.resource.root_checksum.clone()),
                    last_verified_at: Some(Timestamp::from_unix_nanos(80)),
                    source_node_id: Some(expected_leader.clone()),
                    error: None,
                },
            )
            .await
            .expect("resource version should persist");
        proposer
            .put_domain(DomainState {
                id: DomainName::parse("default").expect("valid domain"),
                config: DomainConfig {
                    pace: DomainPace::Unpaced,
                    period: "0ms".to_string(),
                    skew: "0ms".to_string(),
                    placement: nervix_models::PlacementPolicy::Neutral,
                },
                status: DomainStatus::Stopped,
                start_version: 0,
                last_start: nervix_models::DomainStartPoint::Resume,
                clock: None,
            })
            .await
            .expect("domain should persist");
        let (tx, _rx) = mpsc::channel(16);
        let mut subscriptions = SessionSubscriptions::new();

        let result = service
            .process_command(
                CommandRequest {
                    query: "DESCRIBE RESOURCE fraud_model VERSION 1;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(result.success, "command must succeed: {}", result.message);
        assert!(result.message.contains("resource: fraud_model@1"));
        assert!(result.message.contains("cluster_ready: true"));
        assert!(result.message.contains(&format!(
            "- {} topology=alive state=ready checksum={}",
            expected_leader, manifest_v1.resource.root_checksum
        )));
        assert!(result.message.contains("entries:"));
        assert!(
            result
                .message
                .contains("- type=directory path=nested size=0 checksum=-")
        );
        assert!(
            result
                .message
                .contains("- type=file path=nested/beta.txt size=4 checksum=")
        );

        let result = service
            .process_command(
                CommandRequest {
                    query: "DESCRIBE RESOURCE fraud_model;".to_string(),
                    domain: "default".to_string(),
                },
                &tx,
                &mut subscriptions,
            )
            .await;

        assert!(result.success, "command must succeed: {}", result.message);
        assert!(result.message.contains("resource: fraud_model"));
        assert!(result.message.contains("versions: 1,2"));
        assert!(result.message.contains("version_details:"));
        assert!(result.message.contains("- version=1 root_checksum="));
        assert!(result.message.contains("manifest_checksum="));
        assert!(result.message.contains("file_count=2 total_bytes=9"));
        assert!(result.message.contains("- version=2 root_checksum="));
        assert!(result.message.contains("file_count=1 total_bytes=5"));
        assert!(result.message.contains("  entries:"));
        assert!(
            result
                .message
                .contains("- type=file path=alpha.txt size=5 checksum=")
        );
        assert!(
            result
                .message
                .contains("- type=file path=model.onnx size=5 checksum=")
        );

        subscriptions.stop_all(&service).await;
        let _ = std::fs::remove_dir_all(&path);
    }
}
