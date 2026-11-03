//! Materialized relay state inspection through its captured schedule and sensitivity.
//!
//! Layer: control plane.
//!
//! - **Owns.** The materialized-state command, its required scheduled metadata, payload and branch
//!   sensitivity, local or owner reads, and the text presented to the session.
//! - **Depends on.** The committed schedule for relay/schema/branch declarations, the runtime for
//!   materialized records, and the command and presentation owners for the result boundary.
//! - **Must not know.** How materialized records are executed, persisted or replicated.

#![cfg_attr(
    nervix_lint,
    nervix::layer(
        control_plane,
        reason = "owns the control plane boundary declared above"
    )
)]

use error_stack::Report;
use nervix_models::{
    CreateRelay, CreateSchema, DomainName, RelayName, ResolvedBranching, ScheduledModel,
    SchemaName, ShowRelayMaterializedState,
};

use crate::{
    application::{
        command_result::CommandResult,
        describe_output::format_materialized_stream_state_output,
        model_mutation::{command_error, command_ok},
        session_service::SessionServiceImpl,
    },
    runtime_schema,
};

#[derive(Debug, thiserror::Error)]
enum MaterializedStateInspectionError {
    #[error(
        "materialized relay '{relay}' references missing scheduled schema '{schema}' in domain \
         '{domain}'"
    )]
    MissingSchema {
        domain: DomainName,
        relay: RelayName,
        schema: SchemaName,
    },
    #[error("materialized relay '{relay}' has no scheduled branch metadata in domain '{domain}'")]
    MissingBranching {
        domain: DomainName,
        relay: RelayName,
    },
}

impl SessionServiceImpl {
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
        let Some(ScheduledModel {
            config: relay,
            node: relay_node,
        }) = domain_schedule
            .scheduled::<CreateRelay>(&show.relay)
            .filter(|scheduled| scheduled.config.materialized_state.is_some())
        else {
            return command_error(format!(
                "stream '{}' in domain '{}' is not materialized",
                show.relay.as_str(),
                domain.as_str()
            ));
        };

        let Some(schema) = domain_schedule.configured::<CreateSchema>(&relay.schema) else {
            let error = Report::new(MaterializedStateInspectionError::MissingSchema {
                domain: domain.clone(),
                relay: show.relay.clone(),
                schema: relay.schema.clone(),
            });
            return command_error(format!("{error:#}"));
        };
        let Some(branching) = relay_node.resolved_branching.as_ref() else {
            let error = Report::new(MaterializedStateInspectionError::MissingBranching {
                domain: domain.clone(),
                relay: show.relay.clone(),
            });
            return command_error(format!("{error:#}"));
        };
        let payload_sensitivity = runtime_schema::compile_schema(schema).vm_sensitivity();
        let branch_sensitivity = match branching {
            ResolvedBranching::Unbranched => nervix_vm::SchemaSensitivity::default(),
            ResolvedBranching::Branched { schema, .. } => {
                runtime_schema::compile_schema(schema).vm_sensitivity()
            }
        };

        let entries = match self
            .inner
            .runtime
            .local_materialized_stream_state(domain, &show.relay, &payload_sensitivity)
            .await
        {
            Ok(entries) if !entries.is_empty() => entries,
            Ok(_) if !relay_node.executes_on(self.inner.consensus.local_node_id()) => {
                if let Some(primary_node) = relay_node.primary_node() {
                    match self
                        .inner
                        .runtime
                        .remote_materialized_stream_state(
                            primary_node,
                            domain,
                            &show.relay,
                            &payload_sensitivity,
                        )
                        .await
                    {
                        Ok(entries) => entries,
                        Err(error) => return command_error(format!("{error:#}")),
                    }
                } else {
                    Vec::new()
                }
            }
            Ok(entries) => entries,
            Err(error) => return command_error(format!("{error:#}")),
        };

        let mut entry_lines = Vec::with_capacity(entries.len());
        for record in entries {
            let key = match record.branch.as_ref() {
                Some(branch) => branch.to_json_string_masking(&branch_sensitivity),
                None => std::borrow::Cow::Borrowed("(root)"),
            };
            entry_lines.push(format!(
                "key={key} payload={} low={} high={}",
                record.payload, record.ingested_at_low_watermark, record.ingested_at_high_watermark
            ));
        }
        let message = format_materialized_stream_state_output(&show.relay, relay_node, entry_lines);
        command_ok(message)
    }
}
