//! Summaries and per-unit state counts for deduplicators and window processors.
//!
//! Layer: control plane.
//! - **Owns.** The configuration, state-count and metric lines of these processor descriptions.
//! - **Depends on.** Registry Models and schedules, owner inspection and processor metrics.
//! - **Must not know.** How the reported state is executed, mutated or persisted.

#![cfg_attr(
    nervix_lint,
    nervix::layer(control_plane, reason = "owns processor description assembly")
)]

use super::*;

impl SessionServiceImpl {
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
            Err(message) => return command_error(format!("{message:#}")),
        };
        let mut lines = match scheduled_node.as_ref() {
            Some(node) => {
                let processor = NodeRef::new(ModelKind::Deduplicator, describe.name.clone());
                self.processor_state_lines(domain, processor, node).await
            }
            None => Vec::new(),
        };
        lines.extend(metrics);
        command_ok(append_metrics_lines(
            format_deduplicator_describe_output(
                &describe.name,
                &deduplicator,
                scheduled_node.as_ref(),
            ),
            lines,
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
            Err(message) => return command_error(format!("{message:#}")),
        };
        let mut lines = match scheduled_node.as_ref() {
            Some(node) => {
                let processor = NodeRef::new(ModelKind::WindowProcessor, describe.name.clone());
                self.processor_state_lines(domain, processor, node).await
            }
            None => Vec::new(),
        };
        lines.extend(metrics);
        command_ok(append_metrics_lines(
            format_window_processor_describe_output(
                &describe.name,
                &processor,
                &aggregate,
                scheduled_node.as_ref(),
            ),
            lines,
        ))
    }
}
